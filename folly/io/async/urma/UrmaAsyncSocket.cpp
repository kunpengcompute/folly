/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaAsyncSocket.h>

#ifdef FOLLY_HAVE_URMA

#include <algorithm>
#include <cstring>
#include <utility>

#include <folly/io/async/urma/UrmaCqHandler.h>
#include <folly/io/async/urma/UrmaHandshake.h>
#include <folly/io/async/urma/UrmaSharedJfr.h>
#include <folly/logging/xlog.h>

#include <unistd.h>

namespace folly {

// ── TcpHandshakeHandler ──

void UrmaAsyncSocket::TcpHandshakeHandler::handlerReady(
    uint16_t /*events*/) noexcept {
  uint8_t buf[4096];
  ssize_t n = ::read(socket_->tcp_fd_, buf, sizeof(buf));
  if (n > 0) {
    socket_->onHandshakeData(buf, static_cast<size_t>(n));
  } else if (n == 0) {
    socket_->onHandshakeError("TCP connection closed during handshake");
  } else {
    socket_->onHandshakeError("TCP read error during handshake");
  }
}

// ── ConnectCallback ──

void UrmaAsyncSocket::ConnectCallback::connectSuccess() noexcept {
  self->tcp_fd_ = self->tcp_socket_->getNetworkSocket().toFd();
  self->tcp_socket_.reset();  // 释放 AsyncSocket,保留 fd
  self->startHandshake(true);
}

void UrmaAsyncSocket::ConnectCallback::connectErr(
    const AsyncSocketException& ex) noexcept {
  XLOG(ERR) << "UrmaAsyncSocket connect error: " << ex.what();
  self->state_ = State::kError;
}

// ── 构造函数 ──

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* evb, const SocketAddress& addr)
    : evb_(evb), state_(State::kConnecting) {
  tcp_socket_ = AsyncSocket::newSocket(evb, addr);
  connect_cb_ = std::make_unique<ConnectCallback>();
  connect_cb_->self = this;
  tcp_socket_->connect(connect_cb_.get(), addr, 0);
}

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* evb, int fd)
    : evb_(evb), tcp_fd_(fd), state_(State::kIdle) {
  startHandshake(false);
}

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* evb, int fd,
                                  UrmaHandshake* handshake)
    : evb_(evb),
      tcp_fd_(fd),
      state_(State::kHandshaking),
      handshake_(handshake) {
  // 握手由外部启动
}

UrmaAsyncSocket::~UrmaAsyncSocket() {
  destroyUrmaResources();
}

// ── 握手 ──

void UrmaAsyncSocket::startHandshake(bool is_client) {
  shared_ = UrmaSharedJfr::getOrCreate(evb_);
  if (!shared_) {
    onHandshakeError("UrmaSharedJfr::getOrCreate failed");
    return;
  }
  handshake_ = new UrmaHandshake(evb_, tcp_fd_, is_client, shared_,
                                  UrmaInit::getConfig(), this);
  state_ = State::kHandshaking;

  // 注册 TCP fd 到 EventBase epoll,握手数据到达时触发 handlerReady
  tcp_handshake_handler_ = std::make_unique<TcpHandshakeHandler>(evb_, tcp_fd_, this);

  handshake_->start(
      [this]() { onHandshakeReady(); },
      [this](const std::string& reason) { onHandshakeError(reason); });
}

void UrmaAsyncSocket::onHandshakeReady() {
  // 握手完成,注销 TCP fd 的 epoll 监听(URMA 数据走 JFR/JFCE)
  tcp_handshake_handler_.reset();

  // URMA 资源已由 handshake 直接写入 res_,无需接管拷贝
  res_.shared_io_jfr = shared_->ioJfr();
  res_.shared_io_jfc = shared_->ioJfc();
  res_.shared_fc_jfr = shared_->fcJfr();
  res_.shared_fc_jfc = shared_->fcJfc();
  peer_rx_buf_size_ = handshake_->peer_rx_buf_size_;

  // TX JFCE handler: EAGAIN(SQ 满)时 urma_rearm_jfc 武装, CQE 到达唤醒 pollTx + retryWrite
  if (res_.tx_jfce) {
    tx_cq_handler_ = new UrmaCqHandler(evb_, urma_get_jfce_fd(res_.tx_jfce), this);
  }

  // 注册 socket 到共享层路由表
  if (res_.io_jetty) {
    shared_->registerSocket(res_.io_jetty->jetty_id.id, this);
  }
  if (res_.fc_jetty) {
    shared_->registerFcSocket(res_.fc_jetty->jetty_id.id, this);
  }

  // TX JFCE handler: EAGAIN(SQ 满)时 urma_rearm_jfc 武装, CQE 到达唤醒 pollTx + retryWrite

  // handshake 已完成,资源在 res_ 中,删除 handshake 对象
  delete handshake_;
  handshake_ = nullptr;

  state_ = State::kOpen;
  XLOG(INFO) << "UrmaAsyncSocket: URMA_ON, peer_rx_buf_size=" << peer_rx_buf_size_;
}

void UrmaAsyncSocket::onHandshakeError(const std::string& reason) {
  XLOG(ERR) << "UrmaAsyncSocket: handshake error: " << reason;
  tcp_handshake_handler_.reset();
  state_ = State::kError;
  if (handshake_) {
    delete handshake_;
    handshake_ = nullptr;
  }
}

void UrmaAsyncSocket::onHandshakeData(const uint8_t* data, size_t len) {
  if (handshake_) {
    handshake_->onDataAvailable(data, len);
  }
}

// ── AsyncReader ──

void UrmaAsyncSocket::setReadCB(ReadCallback* callback) noexcept {
  read_cb_ = callback;
  // 如果有 preReceivedData,立即触发 readDataAvailable
  // (Phase 4 TCP 降级时 setPreReceivedData 使用)
}

// ── AsyncWriter ──

void UrmaAsyncSocket::write(WriteCallback* cb, const void* buf, size_t bytes,
                             WriteFlags flags) noexcept {
  auto iobuf = IOBuf::copyBuffer(buf, bytes);
  writeChain(cb, std::move(iobuf), flags);
}

void UrmaAsyncSocket::writev(WriteCallback* cb, const iovec* vec, size_t count,
                              WriteFlags flags) noexcept {
  auto iobuf = IOBuf::wrapIov(vec, count);
  writeChain(cb, std::move(iobuf), flags);
}

void UrmaAsyncSocket::writeChain(WriteCallback* cb, std::unique_ptr<IOBuf>&& chain,
                                  WriteFlags flags) noexcept {
  if (state_ != State::kOpen) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "socket not open"));
    return;
  }

  pollTx();  // 发送前先回收 SEND completion

  // 检查 pending_write_(上次 EAGAIN 挂起的)
  if (pending_write_) {
    // 将新 chain 追加到 pending_write_ 末尾
    pending_write_->prependChain(std::move(chain));
    // 不回调,等 retryWrite 全部完成
    return;
  }

  uint32_t total_posted = 0;
  while (chain) {
    // 多 SGE 合并:累积 IOBuf 直到总大小 ≤ peer_rx_buf_size_
    uint32_t sge_total = 0;
    int num_sge = 0;

    uint8_t hw_max_sge = UrmaInit::getMaxJfsSge();
    size_t chain_remaining = chain->countChainElements();
    uint8_t sge_capacity = static_cast<uint8_t>(
        std::min(chain_remaining, static_cast<size_t>(hw_max_sge)));

    // 堆分配 SGE 数组:post 后 NIC 异步 DMA,栈数组会失效
    auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(sge_capacity);
    // 用 shared_ptr 持有 IOBuf 链头,链头的 next_ 持有后续 IOBuf
    // 初始为空 IOBuf(prependChain 到空 IOBuf 上创建链)
    auto wr_chain_head = std::shared_ptr<IOBuf>(IOBuf::create(0).release());

    while (chain && num_sge < sge_capacity) {
      size_t iobuf_len = chain->length();
      if (sge_total + iobuf_len > peer_rx_buf_size_ && num_sge > 0) {
        break;  // 超 peer_rx_buf_size_,新开 WR
      }
      if (iobuf_len > peer_rx_buf_size_) {
        if (cb) cb->writeErr(total_posted, AsyncSocketException(
            AsyncSocketException::UNKNOWN, "IOBuf > peer_rx_buf_size not supported in Phase 2"));
        return;
      }
      (*sge_arr)[num_sge].addr = chain->writableData();
      (*sge_arr)[num_sge].len = static_cast<uint32_t>(iobuf_len);
      (*sge_arr)[num_sge].tseg = UrmaInit::getPoolTseg();
      wr_chain_head->prependChain(chain->cloneCoalesced());
      sge_total += iobuf_len;
      num_sge++;
      chain = chain->pop();
    }

    if (num_sge == 0) break;

    // 构造 SEND WR
    urma_jfs_wr_t wr = {};
    wr.opcode = URMA_OPC_SEND;
    wr.tjetty = res_.remote_jetty;
    wr.flag.bs.complete_enable = 1;
    wr.send.src.sge = sge_arr->data();
    wr.send.src.num_sge = static_cast<uint32_t>(num_sge);
    wr.user_ctx = sbuf_.size();
    wr.next = nullptr;

    // 持有 SGE 数组 + IOBuf 链引用直到 SEND completion
    sbuf_.push_back(WrRefs{std::move(sge_arr), std::move(wr_chain_head)});

    total_posted += sge_total;
    urma_status_t ret = urma_post_jetty_send_wr(res_.io_jetty, &wr);
    if (ret == URMA_SUCCESS) {
      continue;  // post 成功,处理下一个 IOBuf
    }
    // post 失败:回滚刚 push 的 sbuf_ 条目
    sbuf_.pop_back();
    if (ret == URMA_EAGAIN) {
      // SQ 满:挂起剩余 IOBuf 链,武装 TX JFCE 中断等 CQE 回收
      pending_write_ = std::move(chain);
      pending_write_cb_ = cb;
      if (res_.tx_jfc) {
        urma_rearm_jfc(res_.tx_jfc, false);
      }
      return;  // 不回调 writeSuccess
    }
    // 其他错误(URMA_FAIL/ENOMEM/EINVAL 等):回调 writeErr
    XLOG(ERR) << "UrmaAsyncSocket: post failed, status=" << ret;
    if (cb) cb->writeErr(total_posted, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "urma_post_jetty_send_wr failed"));
    return;
  }

  if (cb) cb->writeSuccess();
  bytes_written_ += total_posted;
}

// ── TX completion ──

void UrmaAsyncSocket::pollTx() noexcept {
  if (!res_.tx_jfc) return;
  urma_cr_t cr[POLL_BATCH_MAX];
  int n;
  // 循环 poll 直到 CQE 全部回收
  while ((n = urma_poll_jfc(res_.tx_jfc, POLL_BATCH_MAX, cr)) > 0) {
    for (int i = 0; i < n; ++i) {
      // 按 user_ctx 精确释放对应 WR 的 SGE 数组 + IOBuf 链
      uint64_t idx = cr[i].user_ctx;
      if (idx < sbuf_.size()) {
        sbuf_[idx] = {};  // 释放 shared_ptr(NIC DMA 已完成)
      }
    }
  }
}

void UrmaAsyncSocket::retryWrite() noexcept {
  if (!pending_write_) return;
  auto chain = std::move(pending_write_);
  auto cb = pending_write_cb_;
  pending_write_cb_ = nullptr;
  pending_write_.reset();
  writeChain(cb, std::move(chain), WriteFlags::NONE);
}

// ── 接收 ──

void UrmaAsyncSocket::onRxBuf(std::unique_ptr<IOBuf> data) noexcept {
  bytes_received_ += data->length();
  if (read_cb_) {
    // URMA 数据已是零拷贝 IOBuf,直接走 readBufferAvailable
    read_cb_->readBufferAvailable(std::move(data));
  }
}

// ── 关闭 ──

void UrmaAsyncSocket::close() {
  shutdownWrite();
  closeNow();
}

void UrmaAsyncSocket::closeNow() {
  if (state_ == State::kClosed) return;
  state_ = State::kClosed;
  destroyUrmaResources();
}

void UrmaAsyncSocket::shutdownWrite() {
  // 等待 in-flight SEND completion(SQ 中所有 WR 完成)
  if (res_.tx_jfc) {
    pollTx();
  }
}

void UrmaAsyncSocket::shutdownWriteNow() {
  // 立即关闭,不等 completion
}

void UrmaAsyncSocket::destroyUrmaResources() {
  if (tx_cq_handler_) {
    delete tx_cq_handler_;
    tx_cq_handler_ = nullptr;
  }
  if (shared_) {
    if (res_.io_jetty) {
      shared_->unregisterSocket(res_.io_jetty->jetty_id.id);
    }
    if (res_.fc_jetty) {
      shared_->unregisterFcSocket(res_.fc_jetty->jetty_id.id);
    }
  }
  if (res_.remote_fc_jetty) {
    urma_unimport_jetty(res_.remote_fc_jetty);
    res_.remote_fc_jetty = nullptr;
  }
  if (res_.remote_jetty) {
    urma_unimport_jetty(res_.remote_jetty);
    res_.remote_jetty = nullptr;
  }
  if (res_.remote_seg) {
    urma_unimport_seg(res_.remote_seg);
    res_.remote_seg = nullptr;
  }
  if (res_.fc_jetty) {
    urma_delete_jetty(res_.fc_jetty);
    res_.fc_jetty = nullptr;
  }
  if (res_.io_jetty) {
    urma_delete_jetty(res_.io_jetty);
    res_.io_jetty = nullptr;
  }
  if (res_.fc_tx_jfc) {
    urma_delete_jfc(res_.fc_tx_jfc);
    res_.fc_tx_jfc = nullptr;
  }
  if (res_.tx_jfc) {
    urma_delete_jfc(res_.tx_jfc);
    res_.tx_jfc = nullptr;
  }
  if (res_.tx_jfce) {
    urma_delete_jfce(res_.tx_jfce);
    res_.tx_jfce = nullptr;
  }
  sbuf_.clear();
  pending_write_.reset();
  pending_write_cb_ = nullptr;
}

// ── 其他 ──

void UrmaAsyncSocket::attachEventBase(EventBase* /*eventBase*/) {
  // Phase 4 实现
}

void UrmaAsyncSocket::detachEventBase() {
  // Phase 4 实现
}

void UrmaAsyncSocket::setSendTimeout(uint32_t milliseconds) {
  send_timeout_ms_ = milliseconds;
}

void UrmaAsyncSocket::getLocalAddress(SocketAddress* /*address*/) const {
  // Phase 4 从 tcp_fd_ 获取
}

void UrmaAsyncSocket::getPeerAddress(SocketAddress* /*address*/) const {
  // Phase 4 从 tcp_fd_ 获取
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
