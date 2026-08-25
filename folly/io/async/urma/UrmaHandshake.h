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
#include <folly/io/async/urma/stub/urma_api.h>

namespace folly {

class UrmaSharedJfr;

namespace v2_wire {

constexpr size_t MAGIC_STR_LEN = 4;
constexpr size_t HELLO_BODY_LEN = 86;
constexpr size_t HELLO_PACKET_LEN = MAGIC_STR_LEN + HELLO_BODY_LEN;  // 90
constexpr uint16_t HELLO_V2_VERSION = 2;
constexpr uint16_t IMPL_V2_VERSION = 1;

constexpr uint32_t HELLO_ACK_VALUE = 0x00000001;

struct HelloMessage {
  uint16_t msg_len{HELLO_PACKET_LEN};
  uint16_t hello_ver{HELLO_V2_VERSION};
  uint16_t impl_ver{IMPL_V2_VERSION};
  uint32_t buffer_size{4096};
  uint32_t recv_buffer_cnt{1024};
  uint32_t jetty_id{0};
  uint8_t eid[16] = {};
  uint32_t uasid{0};
  uint8_t tp_type{URMA_CTP};
  uint8_t pad[3] = {};
  uint8_t seg_eid[16] = {};
  uint32_t seg_uasid{0};
  uint64_t seg_va{0};
  uint64_t seg_len{0};
  uint32_t seg_token_id{0};
  uint32_t fc_jetty_id{0};

  void serialize(void* buf) const;
  void deserialize(const void* buf);
};

static_assert(sizeof(HelloMessage) == HELLO_BODY_LEN,
              "HelloMessage must be 86 bytes");

} // namespace v2_wire

class UrmaAsyncSocket;

class UrmaHandshake : private AsyncTimeout {
 public:
  using ReadyCallback = std::function<void()>;
  using ErrorCallback = std::function<void(const std::string&)>;

  UrmaHandshake(EventBase* evb, int tcp_fd, bool is_client,
                UrmaSharedJfr* shared, const UrmaConfig& config,
                class UrmaAsyncSocket* socket);
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

  uint32_t peer_rx_buf_size_{4096};

  ReadyCallback ready_cb_;
  ErrorCallback error_cb_;

  // 握手缓冲区(收到的 TCP 数据累积)
  std::vector<uint8_t> recv_buf_;

  static constexpr uint32_t HANDSHAKE_TIMEOUT_MS = 5000;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
