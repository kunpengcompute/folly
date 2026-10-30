/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaHandshake: v2 二进制握手协议。
 *
 *   90 字节 HelloMessage (magic "URMA" + 86 字节 body), 通过 TCP fd 交换。
 *   两轮: Hello 交换 + ACK 确认。
 *   异步驱动: 通过 TCP fd 的 writeChain/readDataAvailable 驱动。
 *   5s 超时 (folly::AsyncTimeout)。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>

#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

class UrmaSharedJfr;

namespace v2_wire {

constexpr size_t MAGIC_STR_LEN = 4;
constexpr size_t HELLO_BODY_LEN = 94;
constexpr size_t HELLO_PACKET_LEN = MAGIC_STR_LEN + HELLO_BODY_LEN;  // 98
constexpr uint16_t HELLO_V2_VERSION = 2;
constexpr uint16_t IMPL_V2_VERSION = 1;

constexpr uint32_t HELLO_ACK_VALUE = 0x00000001;

// Phase 8: capability flags(复用 pad[3] 空间,HelloMessage 大小不变)
// 协商池化能力:对端不支持时降级 TCP
constexpr uint8_t CAP_SHARED_JFR       = 0x01;  // JFR 池化(headroom 路由)
constexpr uint8_t CAP_HEADROOM_ROUTING = 0x02;  // headroom 路由(64B header in recv buffer)

struct __attribute__((packed)) HelloMessage {
  uint16_t msg_len{HELLO_PACKET_LEN};
  uint16_t hello_ver{HELLO_V2_VERSION};
  uint16_t impl_ver{IMPL_V2_VERSION};
  uint32_t buffer_size{4096};
  uint32_t recv_buffer_cnt{1024};
  uint32_t jfr_id{0};        // 任务 A: 去 Jetty —— 通告 JFR 级共享 jfr_id(替代 jetty_id)
  uint8_t eid[16] = {};
  uint32_t uasid{0};
  uint8_t tp_type{URMA_CTP};
  uint8_t capability_flags[3] = {CAP_SHARED_JFR | CAP_HEADROOM_ROUTING, 0, 0};
  uint8_t seg_eid[16] = {};
  uint32_t seg_uasid{0};
  uint64_t seg_va{0};
  uint64_t seg_len{0};
  uint32_t seg_token_id{0};
  uint32_t fc_jfr_id{0};    // 任务 A: FC JFR 级共享 jfr_id(替代 fc_jetty_id)
  uint64_t socket_id{0};  // 本端 SocketId(握手通告,对端写入 header.dst_socket_id,接收方据此路由)

  void serialize(void* buf) const;
  void deserialize(const void* buf);
};

static_assert(sizeof(HelloMessage) == HELLO_BODY_LEN,
              "HelloMessage must be 94 bytes");

} // namespace v2_wire

class UrmaAsyncSocket;

class UrmaHandshake : private AsyncTimeout {
  friend class UrmaAsyncSocket;
 public:
  using ReadyCallback = std::function<void()>;
  using ErrorCallback = std::function<void(const std::string&)>;

  UrmaHandshake(EventBase* evb, int tcp_fd, bool is_client,
                UrmaSharedJfr* shared, const UrmaConfig& config,
                class UrmaAsyncSocket* socket);

  // 设置握手超时(毫秒),默认 5000
  void setHandshakeTimeoutMs(uint32_t ms) { handshake_timeout_ms_ = ms; }

  ~UrmaHandshake() override;

  // 启动握手(客户端发 Hello, 服务端等 Hello)
  void start(ReadyCallback ready_cb, ErrorCallback error_cb);

  // TCP fd 收到数据时调用(由 UrmaAsyncSocket 的 read 回调驱动)
  void onDataAvailable(const uint8_t* data, size_t len);

 private:
  enum class Phase : uint8_t {
    kIdle,
    kSendHello,
    kWaitHello,
    kSendAck,
    kWaitAck,
    kDone,
  };

  void sendHello();
  void processRemoteHello(const v2_wire::HelloMessage& hello);
  void allocateResources();
  void importPeer(const v2_wire::HelloMessage& hello);
  void sendAck();
  void onHandshakeReady();
  void onHandshakeError(const std::string& reason);
  void timeoutExpired() noexcept override;

  bool validateHello(const v2_wire::HelloMessage& hello);

  int tcp_fd_;
  bool is_client_;
  UrmaSharedJfr* shared_;
  const UrmaConfig& config_;
  class UrmaAsyncSocket* socket_;  // 直接操作 socket->res_

  Phase phase_{Phase::kIdle};
  v2_wire::HelloMessage local_hello_;

  // peer_rx_buf_size_ 在握手完成后被 UrmaAsyncSocket::onHandshakeReady 读取
  uint32_t peer_rx_buf_size_{4096};

  ReadyCallback ready_cb_;
  ErrorCallback error_cb_;

  // 握手缓冲区(收到的 TCP 数据累积)
  std::vector<uint8_t> recv_buf_;

  uint32_t handshake_timeout_ms_{5000};  // 握手超时(可配置)
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
