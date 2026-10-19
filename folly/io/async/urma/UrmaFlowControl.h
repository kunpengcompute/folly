/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaFlowControl: FC 通道流控协议(Phase 6)。
 *
 *   设计文档 §4.6 FC 通道:
 *     - FC 物理隔离:独立的 FC JFR/JFS 池,IO 通道故障不影响 FC
 *     - FC payload 前 8B = SocketId(无 64B headroom header,开销更低)
 *     - 4 种消息:REQ(申请 credit) / REP(授予) / RETURN(归还) / ACK(确认归还)
 *
 *   credit 流控(设计 §3.3):
 *     - 发送方维护 peer_credit_(对端授予的剩余 credit)
 *     - 接收方维护 credit_idle(空闲) + credit_allocated(已分配)
 *     - 不变式: credit_idle + credit_allocated ≤ rx_depth
 *     - 自适应: credit_multiple=2.0(充足 ×2,不足 /2)
 *
 *   FC 消息格式(16B payload):
 *     ┌──────────────┬──────────┬────────────┬──────────┬──────────┐
 *     │ src_socket_id│ msg_type │ credit_cnt │ fc_seq   │ reserved │
 *     │   8 bytes    │ 1 byte   │  2 bytes   │ 2 bytes  │ 3 bytes  │
 *     └──────────────┴──────────┴────────────┴──────────┴──────────┘
 *
 *   路由:接收方从 payload 前 8B 取 SocketId → fc_socket_map 查 UrmaAsyncSocket
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <cstdint>
#include <cstring>

namespace folly {

// ── FC 消息类型 ──
enum UrmaFcMsgType : uint8_t {
  FC_REQ = 1,      // 发送方 → 接收方:申请 credit(credit_cnt=请求数量)
  FC_REP = 2,      // 接收方 → 发送方:授予 credit(credit_cnt=授予数量)
  FC_RETURN = 3,   // 发送方 → 接收方:归还 credit(credit_cnt=归还数量)
  FC_ACK = 4,      // 接收方 → 发送方:确认归还(credit_cnt=确认数量)
};

// ── FC 消息头(跟在 8B SocketId 后,8B 对齐) ──
struct __attribute__((packed)) FcHeader {
  uint8_t  msg_type;     // UrmaFcMsgType
  uint16_t credit_cnt;   // credit 数量(REQ/REP/RETURN/ACK 携带)
  uint16_t fc_seq;       // FC 消息序列号(去重/匹配 REQ-REP 用)
  uint8_t  reserved[3];  // 凑齐 8B
};
static_assert(sizeof(FcHeader) == 8, "FcHeader must be 8 bytes");

// FC 消息总长度:SocketId(8B) + FcHeader(8B) = 16B
constexpr uint32_t FC_MSG_LEN = 16;

// ── FC 消息序列化/反序列化 ──

// 序列化 FC 消息到 16B buffer
// buf: 16B 缓冲区(调用方保证足够大)
// socket_id: 发送方 SocketId(路由用)
// type: FC 消息类型
// credit_cnt: credit 数量
// fc_seq: FC 序列号
inline void serializeFcMsg(void* buf, uint64_t socket_id, UrmaFcMsgType type,
                            uint16_t credit_cnt, uint16_t fc_seq) {
  auto* p = static_cast<uint8_t*>(buf);
  std::memcpy(p, &socket_id, sizeof(socket_id));
  p += sizeof(socket_id);
  auto* hdr = reinterpret_cast<FcHeader*>(p);
  hdr->msg_type = static_cast<uint8_t>(type);
  hdr->credit_cnt = credit_cnt;
  hdr->fc_seq = fc_seq;
  hdr->reserved[0] = 0;
  hdr->reserved[1] = 0;
  hdr->reserved[2] = 0;
}

// 从 16B buffer 反序列化 FC 消息
// buf: 接收到的 buffer 起始(至少 16B)
// socket_id: 输出发送方 SocketId
// type: 输出 FC 消息类型
// credit_cnt: 输出 credit 数量
// fc_seq: 输出 FC 序列号
// 返回: true 表示解析成功
inline bool deserializeFcMsg(const void* buf, uint64_t& socket_id,
                              UrmaFcMsgType& type, uint16_t& credit_cnt,
                              uint16_t& fc_seq) {
  if (!buf) return false;
  auto* p = static_cast<const uint8_t*>(buf);
  std::memcpy(&socket_id, p, sizeof(socket_id));
  p += sizeof(socket_id);
  const auto* hdr = reinterpret_cast<const FcHeader*>(p);
  type = static_cast<UrmaFcMsgType>(hdr->msg_type);
  credit_cnt = hdr->credit_cnt;
  fc_seq = hdr->fc_seq;
  return true;
}

// ── per-连接 FC 状态(嵌入 UrmaAsyncSocket) ──
// peer_credit: 发送方视角,对端授予的剩余 credit(可发送的消息数)
// peer_granted: 接收方视角,已授予对端的 credit 总数
// fc_seq_next: FC 消息发送序列号(去重用)
// fc_last_req_seq: 最近收到的 REQ 序列号(去重,防止重发 REQ 重复授信)
struct UrmaFcState {
  // 发送方状态
  std::atomic<int32_t> peer_credit{0};      // 对端授予的剩余 credit(<0 表示超额)
  std::atomic<uint16_t> fc_seq_next{0};     // FC 发送序列号

  // 接收方状态
  std::atomic<int32_t> peer_granted{0};     // 已授予对端的 credit
  std::atomic<uint16_t> fc_last_req_seq{0}; // 最近处理的 REQ 序列号

  // 自适应 credit_multiple(设计 §4.6.4)
  // 充足时 ×2,不足时 /2,范围 [1, 8]
  std::atomic<uint32_t> credit_multiple{2};

  // 初始化授予的 credit(握手后第一次 REP 授予的数量)
  // 设计 §4.6:ACK 时 FC REP 授予 INITIAL_CREDIT=rx_depth/16
  static constexpr uint32_t INITIAL_CREDIT_DIVISOR = 16;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
