/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaAsyncSocket: AsyncTransport 实现,基于 URMA SEND/RECV。
 *
 *   客户端: TCP connect + 异步握手 → URMA_ON
 *   服务端: accept fd + 异步握手 → URMA_ON
 *
 *   writeChain: pollTx → 多 SGE 合并 → post SEND → EAGAIN 挂起
 *   onRxBuf: IOBuf::takeOwnership 零拷贝 → readCB
 *   pollTx: 回收 SEND completion → 释放 sbuf_ → retryWrite
 *   EAGAIN: pending_write_ 挂起 → TX JFCE 中断 → retryWrite
 *
 *   Phase 2 简化: 无 credit 流控(remote_rq_window = tx_depth); >4KB writeError
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <memory>
#include <vector>

#include <folly/SocketAddress.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/EventBase.h>
#include <folly/net/NetworkSocket.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaResource.h>
#include <folly/io/async/urma/stub/urma_api.h>

namespace folly {

class UrmaSharedJfr;
class UrmaHandshake;
class UrmaCqHandler;

class UrmaAsyncSocket : public AsyncTransport {
 public:
  // 客户端构造
  UrmaAsyncSocket(EventBase* evb, const SocketAddress& addr);
  // 服务端构造(fd 已 accept)
  UrmaAsyncSocket(EventBase* evb, int fd);
  // 握手完成后构造(接管 handshake 资源)
  UrmaAsyncSocket(EventBase* evb, int fd, UrmaHandshake* handshake);

  ~UrmaAsyncSocket() override;

  // ---------- AsyncReader ----------
  void setReadCB(ReadCallback* callback) noexcept override;
  ReadCallback* getReadCallback() const noexcept override { return read_cb_; }

  // ---------- AsyncWriter ----------
  void write(WriteCallback* callback, const void* buf, size_t bytes,
             WriteFlags flags = WriteFlags::NONE) noexcept override;
  void writev(WriteCallback* callback, const iovec* vec, size_t count,
              WriteFlags flags = WriteFlags::NONE) noexcept override;
  void writeChain(WriteCallback* callback, std::unique_ptr<IOBuf>&& buf,
                  WriteFlags flags = WriteFlags::NONE) noexcept override;

  // ---------- AsyncTransport ----------
  void close() override;
  void closeNow() override;
  void shutdownWrite() override;
  void shutdownWriteNow() override;
  bool good() const override { return state_ == State::kOpen; }
  bool readable() const override { return state_ == State::kOpen; }
  bool connecting() const override { return state_ == State::kConnecting; }
  bool error() const override { return state_ == State::kError; }
  void attachEventBase(EventBase* eventBase) override;
  void detachEventBase() override;
  bool isDetachable() const override { return false; }
  void setSendTimeout(uint32_t milliseconds) override;
  uint32_t getSendTimeout() const override { return send_timeout_ms_; }
  void getLocalAddress(SocketAddress* address) const override;
  void getPeerAddress(SocketAddress* address) const override;
  bool isEorTrackingEnabled() const override { return eor_tracking_; }
  void setEorTracking(bool track) override { eor_tracking_ = track; }
  size_t getAppBytesWritten() const override { return bytes_written_; }
  size_t getRawBytesWritten() const override { return bytes_written_; }
  size_t getAppBytesReceived() const override { return bytes_received_; }
  size_t getRawBytesReceived() const override { return bytes_received_; }

  // ---------- URMA 专属 ----------
  // 接收数据(由 UrmaSharedJfr::pollAndDispatch 调用)
  void onRxBuf(std::unique_ptr<IOBuf> data) noexcept;

  // TX completion(由 UrmaCqHandler TX 模式调用)
  void pollTx() noexcept;
  void retryWrite() noexcept;

  // 握手数据透传(TCP fd 收到的数据转发给 UrmaHandshake)
  void onHandshakeData(const uint8_t* data, size_t len);

  // TCP connect 回调
  struct ConnectCallback : AsyncSocket::ConnectCallback {
    UrmaAsyncSocket* self;
    void connectSuccess() noexcept override;
    void connectErr(const AsyncSocketException&) noexcept override;
  };

 private:
  enum class State : uint8_t {
    kIdle,
    kConnecting,    // TCP connecting
    kHandshaking,   // URMA handshake
    kOpen,          // URMA_ON
    kClosing,
    kClosed,
    kError,
  };

  void startHandshake(bool is_client);
  void onHandshakeReady();
  void onHandshakeError(const std::string& reason);
  void destroyUrmaResources();

  // 握手期间监听 TCP fd 可读事件的内部 EventHandler
  class TcpHandshakeHandler : public EventHandler {
   public:
    TcpHandshakeHandler(EventBase* evb, int fd, UrmaAsyncSocket* socket)
        : EventHandler(evb, NetworkSocket(fd)),
          socket_(socket) {
      registerHandler(READ | PERSIST);
    }
    ~TcpHandshakeHandler() override { unregisterHandler(); }
    void handlerReady(uint16_t /*events*/) noexcept override;
   private:
    UrmaAsyncSocket* socket_;
  };

  EventBase* evb_;
  int tcp_fd_{-1};
  State state_{State::kIdle};
  ReadCallback* read_cb_{nullptr};

  // TCP connect
  std::unique_ptr<ConnectCallback> connect_cb_;
  std::unique_ptr<AsyncSocket> tcp_socket_;  // 用于 TCP connect + 握手数据传输

  // URMA 资源(握手完成后填充)
  UrmaSharedJfr* shared_{nullptr};
  UrmaHandshake* handshake_{nullptr};
  UrmaResource res_;
  uint32_t peer_rx_buf_size_{4096};

  // 握手期间监听 TCP fd 的 EventHandler
  std::unique_ptr<TcpHandshakeHandler> tcp_handshake_handler_;

  // 发送状态
  // sbuf_: per-WR 引用(SGE 数组 + IOBuf 链),持有直到 SEND completion
  struct WrRefs {
    std::shared_ptr<std::vector<urma_sge_t>> sge;
    std::shared_ptr<IOBuf> bufs;
  };
  std::vector<WrRefs> sbuf_;
  std::unique_ptr<IOBuf> pending_write_;
  WriteCallback* pending_write_cb_{nullptr};

  // TX JFCE handler (EAGAIN 中断: SQ 满时 urma_rearm_jfc 武装, CQE 到达唤醒 retryWrite)
  UrmaCqHandler* tx_cq_handler_{nullptr};

  uint32_t send_timeout_ms_{0};
  size_t bytes_written_{0};
  size_t bytes_received_{0};
  bool eor_tracking_{false};

  static constexpr int POLL_BATCH_MAX = 64;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
