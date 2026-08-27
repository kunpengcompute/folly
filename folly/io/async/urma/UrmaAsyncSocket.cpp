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
  // detachNetworkSocket 转移 fd 所有权,之后 reset 不会关闭 fd
  self->tcp_fd_ = self->tcp_socket_->detachNetworkSocket().toFd();
  self->tcp_socket_.reset();
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
  tcp_socket_ = AsyncSocket::newSocket(evb);
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
  handshake_->setHandshakeTimeoutMs(UrmaInit::getConfig().handshake_timeout_ms);
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
    tx_cq_handler_ = new UrmaCqHandler(evb_, res_.tx_jfce->fd, this);
  }

  // 注册 socket 到共享层路由表
  // key = 远端 jetty_id.id(= CQE.remote_id.id),pollAndDispatch 按此路由
  if (res_.remote_jetty) {
    shared_->registerSocket(res_.remote_jetty->id.id, this);
  }
  if (res_.remote_fc_jetty) {
    shared_->registerFcSocket(res_.remote_fc_jetty->id.id, this);
  }

  // TX JFCE handler: EAGAIN(SQ 满)时 urma_rearm_jfc 武装, CQE 到达唤醒 pollTx + retryWrite

  // handshake 已完成,资源在 res_ 中,删除 handshake 对象
  delete handshake_;
  handshake_ = nullptr;

  state_ = State::kOpen;
  XLOG(INFO) << "UrmaAsyncSocket: URMA_ON, peer_rx_buf_size=" << peer_rx_buf_size_;

  // flush 握手期间排队的写请求
  // pending_write_head_ 已包含排队节点,直接调用 handlePendingWrites 逐个发送
  handlePendingWrites();
}

void UrmaAsyncSocket::onHandshakeError(const std::string& reason) {
  XLOG(ERR) << "UrmaAsyncSocket: handshake error: " << reason;
  tcp_handshake_handler_.reset();
  state_ = State::kError;
  if (handshake_) {
    delete handshake_;
    handshake_ = nullptr;
  }
  // 回调握手期间排队的写请求
  while (pending_write_head_) {
    auto pw = std::move(pending_write_head_);
    pending_write_head_ = std::move(pw->next);
    if (!pending_write_head_) {
      pending_write_tail_ = nullptr;
    }
    if (pw->cb) {
      pw->cb->writeErr(0, AsyncSocketException(
          AsyncSocketException::UNKNOWN, reason));
    }
  }
}

void UrmaAsyncSocket::onHandshakeData(const uint8_t* data, size_t len) {
  if (handshake_) {
    handshake_->onDataAvailable(data, len);
  }
}

// ── AsyncReader ──

void UrmaAsyncSocket::setReadCB(ReadCallback* callback) noexcept {
  // 握手期间拒绝设置(避免干扰 TcpHandshakeHandler)
  // 上层会在握手完成后重新设置
  if (state_ == State::kConnecting || state_ == State::kHandshaking) {
    read_cb_ = callback;  // 暂存,握手完成后上层会重新设置
    return;
  }
  read_cb_ = callback;
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
  if (state_ == State::kError || state_ == State::kClosed) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "socket not open"));
    return;
  }

  // 有挂起请求时(连接中/握手中/EAGAIN),新请求追加到链表尾部
  if (pending_write_head_) {
    auto pw = std::make_unique<PendingWrite>();
    pw->cb = cb;
    pw->buf = std::move(chain);
    pw->next = nullptr;
    pending_write_tail_->next = std::move(pw);
    pending_write_tail_ = pending_write_tail_->next.get();
    return;
  }

  pollTx();

  // 没有挂起请求,直接发送
  uint32_t total_posted = 0;
  urma_status_t ret = sendChain(cb, chain, total_posted);
  if (ret == URMA_EAGAIN) {
    // SQ 满:剩余 chain 挂到链表头,武装 TX JFCE 中断
    auto pw = std::make_unique<PendingWrite>();
    pw->cb = cb;
    pw->buf = std::move(chain);
    pw->next = nullptr;
    pending_write_head_ = std::move(pw);
    pending_write_tail_ = pending_write_head_.get();
    if (res_.tx_jfc) {
      urma_rearm_jfc(res_.tx_jfc, false);
    }
  } else if (ret == URMA_SUCCESS) {
    if (cb) cb->writeSuccess();
    bytes_written_ += total_posted;
  }
  // 其他错误: sendChain 内部已回调 writeErr
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
  handlePendingWrites();
}

// 发送单个 IOBuf chain(多 SGE 合并 → post SEND)
// 返回值: URMA_SUCCESS(全部发完) / URMA_EAGAIN(SQ 满,chain 中剩余部分通过 chain 引用返回) / URMA_FAIL(已回调 writeErr)
urma_status_t UrmaAsyncSocket::sendChain(WriteCallback* cb,
                                          std::unique_ptr<IOBuf>& chain,
                                          uint32_t& total_posted) noexcept {
  total_posted = 0;
  while (chain) {
    uint32_t sge_total = 0;
    int num_sge = 0;

    uint8_t hw_max_sge = UrmaInit::getMaxJfsSge();
    size_t chain_remaining = chain->countChainElements();
    uint8_t sge_capacity = static_cast<uint8_t>(
        std::min(chain_remaining, static_cast<size_t>(hw_max_sge)));

    auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(sge_capacity);
    auto wr_chain_head = std::shared_ptr<IOBuf>(IOBuf::create(0).release());

    while (chain && num_sge < sge_capacity) {
      size_t iobuf_len = chain->length();
      if (sge_total + iobuf_len > peer_rx_buf_size_ && num_sge > 0) {
        break;
      }
      if (iobuf_len > peer_rx_buf_size_) {
        if (cb) cb->writeErr(total_posted, AsyncSocketException(
            AsyncSocketException::UNKNOWN,
            "IOBuf > peer_rx_buf_size not supported in Phase 2"));
        return URMA_FAIL;
      }
      (*sge_arr)[num_sge].addr = reinterpret_cast<uint64_t>(chain->writableData());
      (*sge_arr)[num_sge].len = static_cast<uint32_t>(iobuf_len);
      (*sge_arr)[num_sge].tseg = UrmaInit::getPoolTseg();
      wr_chain_head->prependChain(chain->cloneCoalesced());
      sge_total += iobuf_len;
      num_sge++;
      chain = chain->pop();
    }

    if (num_sge == 0) break;

    urma_jfs_wr_t wr = {};
    wr.opcode = URMA_OPC_SEND;
    wr.tjetty = res_.remote_jetty;
    wr.flag.bs.complete_enable = 1;
    wr.send.src.sge = sge_arr->data();
    wr.send.src.num_sge = static_cast<uint32_t>(num_sge);
    wr.user_ctx = sbuf_.size();
    wr.next = nullptr;

    sbuf_.push_back(WrRefs{std::move(sge_arr), std::move(wr_chain_head)});

    total_posted += sge_total;
    urma_jfs_wr_t* bad_wr = nullptr;
    urma_status_t ret = urma_post_jetty_send_wr(res_.io_jetty, &wr, &bad_wr);
    if (ret == URMA_SUCCESS) {
      continue;
    }
    sbuf_.pop_back();
    if (ret == URMA_EAGAIN) {
      // SQ 满:剩余 chain 通过引用返回给调用方
      return URMA_EAGAIN;
    }
    XLOG(ERR) << "UrmaAsyncSocket: post failed, status=" << ret;
    if (cb) cb->writeErr(total_posted, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "urma_post_jetty_send_wr failed"));
    return URMA_FAIL;
  }
  return URMA_SUCCESS;
}

// 遍历 pending_write_head_ 链表逐个发送(参照 AsyncSocket::handleWrite)
// 用于握手完成后 flush / EAGAIN 恢复后重试
void UrmaAsyncSocket::handlePendingWrites() noexcept {
  pollTx();

  while (pending_write_head_) {
    auto* pw = pending_write_head_.get();
    std::unique_ptr<IOBuf> chain = std::move(pw->buf);
    uint32_t total_posted = 0;

    urma_status_t ret = sendChain(pw->cb, chain, total_posted);
    if (ret == URMA_EAGAIN) {
      // SQ 满:剩余 chain 放回当前节点,武装 TX JFCE 中断
      pw->buf = std::move(chain);
      if (res_.tx_jfc) {
        urma_rearm_jfc(res_.tx_jfc, false);
      }
      return;
    }
    if (ret == URMA_SUCCESS) {
      // 当前节点全部发完,从链表移除,回调 writeSuccess
      WriteCallback* cb = pw->cb;
      pending_write_head_ = std::move(pending_write_head_->next);
      if (!pending_write_head_) {
        pending_write_tail_ = nullptr;
      }
      if (cb) cb->writeSuccess();
      bytes_written_ += total_posted;
    }
    // URMA_FAIL: sendChain 内部已回调 writeErr,从链表移除当前节点
    if (ret == URMA_FAIL) {
      pending_write_head_ = std::move(pending_write_head_->next);
      if (!pending_write_head_) {
        pending_write_tail_ = nullptr;
      }
    }
  }
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
    if (res_.remote_jetty) {
      shared_->unregisterSocket(res_.remote_jetty->id.id);
    }
    if (res_.remote_fc_jetty) {
      shared_->unregisterFcSocket(res_.remote_fc_jetty->id.id);
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
  pending_write_head_.reset();
  pending_write_tail_ = nullptr;
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
