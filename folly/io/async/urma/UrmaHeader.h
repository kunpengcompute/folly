/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaHeader: headroom 路由的 64B 消息头 + SocketId 管理。
 *
 *   设计文档 §5 headroom 路由:
 *     - UrmaMsgHeader 固定 64B,嵌入 IOBuf block headroom(data 指针前 64B)
 *     - 发送方 IOBuf::prepend(64) → 在 headroom 写 header,单 SGE 覆盖 header+data
 *     - 接收方从 recv buffer 前 64B 解析 header → Socket::Address 路由
 *
 *   SocketId: 64 位全局唯一标识(version 32bit + index 32bit)
 *     - version: 连接复用时递增,防止 socket_id 被复用导致错配
 *     - index: 全局递增索引
 *     - Socket::Address(socket_id) 通过版本号校验后返回 UrmaAsyncSocket*
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <cstdint>
#include <cstring>

#include <folly/io/IOBuf.h>

namespace folly {

// ── 魔数常量 ──
constexpr uint32_t URMA_MAGIC = 0x55524D41;    // "URMA"
constexpr uint32_t INLINE_MAGIC = 0x494E4C41;  // "INLA" (inline 路径预留,Phase 6)

// ── 消息类型 ──
enum UrmaMsgType : uint8_t {
  MSG_DATA = 0,       // 普通数据 SEND(≤ peer_rx_buf_size)
  MSG_PULL_MEM = 1,   // 大包控制包(通知接收方发起 READ)
  MSG_FREE = 2,       // 大包 FREE 通知(接收方 → 发送方,释放源 buffer)
};

// ── 64B 固定消息头(设计 §5.3) ──
struct __attribute__((packed)) UrmaMsgHeader {
  uint32_t magic;           // URMA_MAGIC
  uint64_t src_socket_id;   // 发送方 SocketId(接收方据此路由)
  uint16_t seq;             // 连接级发送序列号(per-连接单调递增,保序用)
  uint8_t  type;            // UrmaMsgType
  uint8_t  msg_count;       // 合并的消息数(1=单消息,>1=合并包)
  uint32_t data_size;       // data 字节数(不含 header)
  uint32_t data_sge_count;  // data SGE 个数(校验用)
  uint16_t sub_len[15];     // 子消息长度表(合并用,msg_count=1 时 sub_len[0]=data_size)
  uint8_t  reserved[10];    // 凑齐 64B
};
static_assert(sizeof(UrmaMsgHeader) == 64, "UrmaMsgHeader must be 64 bytes");

// ── SocketId: 64 位全局唯一标识 ──
struct SocketId {
  uint32_t version;    // 版本号,连接复用时递增防错配
  uint32_t index;      // 全局递增索引

  uint64_t toUint64() const {
    return (static_cast<uint64_t>(version) << 32) | index;
  }
  static SocketId fromUint64(uint64_t v) {
    return {static_cast<uint32_t>(v >> 32), static_cast<uint32_t>(v)};
  }
  bool operator==(const SocketId& other) const {
    return version == other.version && index == other.index;
  }
  bool operator!=(const SocketId& other) const { return !(*this == other); }
};

// ── SocketId 分配器(全局原子递增) ──
class SocketIdAllocator {
 public:
  static SocketIdAllocator& get() {
    static SocketIdAllocator inst;
    return inst;
  }

  SocketId allocate() {
    uint32_t idx = next_index_.fetch_add(1, std::memory_order_relaxed);
    return {1, idx};  // version=1(首次分配),后续连接复用时递增
  }

 private:
  SocketIdAllocator() = default;
  std::atomic<uint32_t> next_index_{1};
};

// ── header 写入/解析工具函数 ──

// 在 IOBuf 的 headroom 区域写入 UrmaMsgHeader
// 调用前: IOBuf 的 data 指针已在 data 区起始(headroom 64B 空闲)
// 调用后: IOBuf::prepend(64) → data 指针前移到 header 起始,length 增加 64
//
// buf:     要发送的 IOBuf(第一个 block,有 64B headroom)
// socket_id: 发送方 SocketId
// seq:     连接级发送序列号
// type:    消息类型(MSG_DATA / MSG_PULL_MEM / MSG_FREE)
// msg_count: 合并的消息数(1=单消息)
// sub_len:  子消息长度表指针(msg_count=1 时可为 nullptr,自动填 data_size)
// data_size: data 字节数(不含 header)
inline bool writeHeader(IOBuf* buf, uint64_t socket_id, uint16_t seq,
                        uint8_t type, uint8_t msg_count,
                        const uint16_t* sub_len, uint32_t data_size) {
  if (!buf) return false;
  // prepend(64): data 指针前移 64B 到 headroom 区
  if (buf->headroom() < sizeof(UrmaMsgHeader)) {
    return false;
  }
  buf->prepend(sizeof(UrmaMsgHeader));

  auto* hdr = reinterpret_cast<UrmaMsgHeader*>(buf->writableData());
  hdr->magic = URMA_MAGIC;
  hdr->src_socket_id = socket_id;
  hdr->seq = seq;
  hdr->type = type;
  hdr->msg_count = msg_count;
  hdr->data_size = data_size;
  hdr->data_sge_count = 0;  // 填充时不知道 SGE 数,由调用方后续设置
  std::memset(hdr->sub_len, 0, sizeof(hdr->sub_len));
  if (msg_count == 1) {
    hdr->sub_len[0] = static_cast<uint16_t>(data_size);
  } else if (sub_len) {
    for (uint8_t i = 0; i < msg_count && i < 15; ++i) {
      hdr->sub_len[i] = sub_len[i];
    }
  }
  hdr->reserved[0] = 0;
  hdr->reserved[1] = 0;
  return true;
}

// 从 recv buffer 前 64B 解析 UrmaMsgHeader
// recv_buf: 接收到的 buffer 起始地址
// 返回: UrmaMsgHeader 指针(magic 校验失败返回 nullptr)
inline const UrmaMsgHeader* parseHeader(const void* recv_buf) {
  if (!recv_buf) return nullptr;
  auto* hdr = static_cast<const UrmaMsgHeader*>(recv_buf);
  if (hdr->magic != URMA_MAGIC) return nullptr;
  return hdr;
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
