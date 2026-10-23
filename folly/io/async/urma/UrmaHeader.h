/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaHeader: 独立 buffer 方案的 24B 消息头 + SocketId 管理。
 *
 *   设计文档 docs/urma-header-redesign.md:
 *     - UrmaMsgHeader 固定 24B,始终作为独立 IOBuf 节点(SGE[0])
 *     - DATA 包走普通 SEND,header + data 投影为 SGE 列表
 *     - PULL_MEM / FREE 控制包走 SEND_IMM,imm_data 携带控制元数据
 *     - 接收方从 recv buffer 前 24B 解析 header → Socket::Address 路由
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
constexpr uint8_t  URMA_VERSION = 1;           // 协议版本(兼容性预留)

// ── 消息类型 ──
enum UrmaMsgType : uint8_t {
  MSG_DATA = 0,       // 普通数据 SEND(≤ peer_rx_buf_size - 24)
  MSG_PULL_MEM = 1,   // 大包控制包(SEND_IMM + trailing data_size + imm remote_va)
  MSG_FREE = 2,       // 大包 FREE 通知(SEND_IMM + imm msg_id)
};

// ── 24B 固定消息头(独立 buffer 方案) ──
//
// 字段语义:
//   magic          URMA_MAGIC,防误判
//   version        URMA_VERSION(=1),协议演进兼容性预留
//   flags          预留标志位(当前恒 0)
//   type           UrmaMsgType:MSG_DATA / MSG_PULL_MEM / MSG_FREE
//   src_socket_id  发送方 SocketId(池化 JFR 路由)
//   seq            连接级发送序列号(每 WR 递增,保序用,与 msg_id 独立)
//   msg_id         大包消息 ID(PULL/FREE 匹配;DATA 包填 0)
//                  —— 仅 sendBigMsgByRead 分配,PULL 和对应 FREE 复用
//
// seq 与 msg_id 关系(两者语义完全独立,不可复用):
//   seq    = send_seq_.fetch_add(1),每 post 一个 WR 递增(DATA/PULL/FREE 都分配)
//            接收侧 expected_seq_ 校验,不一致仅 WARN(保序诊断,不丢弃)
//   msg_id = next_msg_id_++,仅大包分配
//            FREE 时查 pending_big_msg_[msg_id] 释放源 buffer
struct __attribute__((packed)) UrmaMsgHeader {
  uint32_t magic;           // 0:  URMA_MAGIC
  uint8_t  version;         // 4:  URMA_VERSION(=1)
  uint8_t  flags;           // 5:  预留标志位
  uint8_t  type;            // 6:  UrmaMsgType
  uint8_t  reserved1;       // 7:  对齐
  uint64_t src_socket_id;   // 8:  发送方 SocketId(池化 JFR 路由)
  uint16_t seq;             // 16: 连接级发送序列号(保序,每 WR 递增)
  uint16_t msg_id;          // 18: 大包 msg_id(PULL/FREE 匹配;DATA 填 0)
  uint32_t reserved2;       // 20: 对齐 + 预留
};
static_assert(sizeof(UrmaMsgHeader) == 24, "UrmaMsgHeader must be 24 bytes");

// ── PULL_MEM 控制包 SGE[0] 完整布局(24B header + 8B trailing = 32B) ──
//
// SEND_IMM WR 的 SGE[0] 指向此结构(32B ≤ max_inline_size,NIC 内联拷贝):
//   header          UrmaMsgHeader(type=MSG_PULL_MEM, msg_id=大包 msg_id)
//   data_size       远端数据长度(接收方 READ 长度)
//   padding         对齐到 32B
//
// 同时 imm_data = remote_va(u64),随 CQE 通告给接收方
struct __attribute__((packed)) PullMemControlPacket {
  UrmaMsgHeader header;     // 24B
  uint32_t data_size;       // 28B: 远端数据长度(READ 长度)
  uint32_t padding;         // 32B: 对齐到 32B
};
static_assert(sizeof(PullMemControlPacket) == 32,
              "PullMemControlPacket must be 32 bytes");

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

// 在独立 IOBuf buffer 中写入 UrmaMsgHeader
// 调用前: IOBuf 已分配 sizeof(UrmaMsgHeader) 空间并 append(24)
// 调用后: IOBuf 的 data 区写入完整 header
//
// buf:       独立 header IOBuf(已 append 24B)
// socket_id: 发送方 SocketId
// seq:       连接级发送序列号(每 WR 递增,保序用)
// type:      消息类型(MSG_DATA / MSG_PULL_MEM / MSG_FREE)
// msg_id:    大包 msg_id(PULL/FREE 匹配;DATA 填 0)
inline bool writeHeader(IOBuf* buf, uint64_t socket_id, uint16_t seq,
                        uint8_t type, uint16_t msg_id) {
  if (!buf) return false;
  if (buf->length() < sizeof(UrmaMsgHeader)) return false;

  auto* hdr = reinterpret_cast<UrmaMsgHeader*>(buf->writableData());
  hdr->magic = URMA_MAGIC;
  hdr->version = URMA_VERSION;
  hdr->flags = 0;
  hdr->type = type;
  hdr->reserved1 = 0;
  hdr->src_socket_id = socket_id;
  hdr->seq = seq;
  hdr->msg_id = msg_id;
  hdr->reserved2 = 0;
  return true;
}

// 直接写入 UrmaMsgHeader 结构体(用于控制包 SGE[0] 内联构造)
// hdr:       目标 UrmaMsgHeader 指针(调用方保证已分配 24B 空间)
// socket_id: 发送方 SocketId
// seq:       连接级发送序列号(每 WR 递增,保序用)
// type:      消息类型(MSG_DATA / MSG_PULL_MEM / MSG_FREE)
// msg_id:    大包 msg_id(PULL/FREE 匹配;DATA 填 0)
inline bool writeHeader(UrmaMsgHeader* hdr, uint64_t socket_id, uint16_t seq,
                        uint8_t type, uint16_t msg_id) {
  if (!hdr) return false;
  hdr->magic = URMA_MAGIC;
  hdr->version = URMA_VERSION;
  hdr->flags = 0;
  hdr->type = type;
  hdr->reserved1 = 0;
  hdr->src_socket_id = socket_id;
  hdr->seq = seq;
  hdr->msg_id = msg_id;
  hdr->reserved2 = 0;
  return true;
}

// 从 recv buffer 前 24B 解析 UrmaMsgHeader
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
