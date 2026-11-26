/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaFlowControl: receiver-driven credit 流控协议。
 *
 *   核心原则:发送方不主动申请 credit,接收方按收割 CQE 的速率主动设定
 *   每个连接的发送窗口。详见《fbthrift URMA credit 流控重设计》。
 *
 *   FC 通道物理隔离:独立 FC JFR/JFS,IO 通道故障不影响 FC。
 *   FC payload 前 8B = SocketId(无 64B headroom header,开销更低)。
 *
 *   两种消息:
 *     GRANT   接收方 → 发送方,携带 delta(可负,负值表示回收窗口)
 *     BLOCKED 发送方 → 接收方,纯通知(撞底,不带请求量)
 *
 *   记账(per-JFR,见 UrmaTransportContext::EvbJfr):
 *     credit_idle + credit_allocated == recv_posted
 *
 *   FC 消息格式(16B payload):
 *     ┌──────────────┬──────────┬────────────┬──────────┬──────────┐
 *     │ dst_socket_id│ msg_type │   delta    │ fc_seq   │ reserved │
 *     │   8 bytes    │ 1 byte   │  4 bytes   │ 2 bytes  │ 1 byte   │
 *     └──────────────┴──────────┴────────────┴──────────┴──────────┘
 *
 *   路由:接收方从 payload 前 8B 取 SocketId → fc 路由表查 UrmaAsyncSocket
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <cstdint>
#include <cstring>

namespace folly {

// ── FC 消息类型 ──
enum UrmaFcMsgType : uint8_t {
  FC_GRANT = 1,    // 接收方 → 发送方:调整窗口(delta 可负)
  FC_BLOCKED = 2,  // 发送方 → 接收方:撞底通知(纯通知,delta 忽略)
};

// ── FC 消息头(跟在 8B SocketId 后,8B 对齐) ──
struct __attribute__((packed)) FcHeader {
  uint8_t msg_type;   // UrmaFcMsgType
  int32_t delta;      // GRANT:窗口增量(正授出/负回收);BLOCKED 忽略
  uint16_t fc_seq;    // FC 消息序列号
  uint8_t reserved;   // 凑齐 8B
};
static_assert(sizeof(FcHeader) == 8, "FcHeader must be 8 bytes");

// FC 消息总长度:SocketId(8B) + FcHeader(8B) = 16B
constexpr uint32_t FC_MSG_LEN = 16;

// ── FC 消息序列化/反序列化 ──

// 序列化 FC 消息到 16B buffer
// buf: 16B 缓冲区(调用方保证足够大)
// socket_id: 目标方 SocketId(发送方的对端 = 接收方的本端,路由用)
// type: FC 消息类型
// delta: 窗口增量(GRANT 用;BLOCKED 传 0)
// fc_seq: FC 序列号
inline void serializeFcMsg(void* buf, uint64_t socket_id, UrmaFcMsgType type,
                           int32_t delta, uint16_t fc_seq) {
  auto* p = static_cast<uint8_t*>(buf);
  std::memcpy(p, &socket_id, sizeof(socket_id));
  p += sizeof(socket_id);
  auto* hdr = reinterpret_cast<FcHeader*>(p);
  hdr->msg_type = static_cast<uint8_t>(type);
  hdr->delta = delta;
  hdr->fc_seq = fc_seq;
  hdr->reserved = 0;
}

// 从 16B buffer 反序列化 FC 消息
// 返回: true 表示解析成功
inline bool deserializeFcMsg(const void* buf, uint64_t& socket_id,
                             UrmaFcMsgType& type, int32_t& delta,
                             uint16_t& fc_seq) {
  if (!buf) return false;
  auto* p = static_cast<const uint8_t*>(buf);
  std::memcpy(&socket_id, p, sizeof(socket_id));
  p += sizeof(socket_id);
  const auto* hdr = reinterpret_cast<const FcHeader*>(p);
  type = static_cast<UrmaFcMsgType>(hdr->msg_type);
  delta = hdr->delta;
  fc_seq = hdr->fc_seq;
  return true;
}

// ── 接收方 per-连接状态(嵌入 UrmaAsyncSocket) ──
// 接收方视角:控制"对端发给我多少"。
// 全部字段仅在连接属主 EVB 线程访问(不变量 I1),无需 atomic。
struct ConnRxState {
  uint32_t limit_conn{0};   // 未消费额度(授出 +,消费 -)
  uint32_t last_grant{0};   // 上次补货量(撞底补发用)
  double   rate_conn{0.0};  // EWMA 平滑后的接收速率(消息/秒)
  bool     rate_inited{false}; // rate 是否已初始化(首次采样直接赋值)
  uint64_t last_recv_ts{0};    // 最后收到该连接 CQE 的时刻(纳秒)
  uint32_t cnt_this_round{0};  // 本轮 harvest 累计收到的 CQE 数(采样用)
};

// ── 发送方 per-连接状态(嵌入 UrmaAsyncSocket) ──
// 发送方视角:受"对端让我发多少"控制。
struct UrmaFcState {
  int32_t peer_credit{0};   // 剩余额度(收到 GRANT 加 delta,每发一个消息减 1)
  bool    blocked_sent{false}; // BLOCKED 去重标记(撞底只发一次,收到 GRANT 复位)
};

// ── 流控参数(默认值,见设计 §9) ──
constexpr uint32_t kUrmaFcInitialWindow = 16;   // 握手初始窗口
constexpr uint32_t kUrmaFcMinWindow = 8;        // 水位目标保底
constexpr uint32_t kUrmaFcGrantPeriodMs = 1;    // 水位调整定时器周期
constexpr uint32_t kUrmaFcIdleTimeoutMs = 100;  // 空闲超时回收阈值
constexpr uint32_t kUrmaFcReclaimUsedPercent = 80; // 池子占用率阈值
constexpr double   kUrmaFcAlphaMin = 1.0 / 16;  // EWMA 下限(稳态平滑)
constexpr double   kUrmaFcAlphaMax = 1.0 / 2;   // EWMA 上限(突变跟随)

} // namespace folly

#endif // FOLLY_HAVE_URMA
