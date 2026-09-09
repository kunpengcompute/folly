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
#include <folly/io/async/urma/UrmaInit.h>
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
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    socket_->onHandshakeError("TCP read error during handshake");
  }
}

// ── ConnectCallback ──

void UrmaAsyncSocket::ConnectCallback::connectSuccess() noexcept {
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

  tcp_handshake_handler_ = std::make_unique<TcpHandshakeHandler>(evb_, tcp_fd_, this);

  handshake_->start(
      [this]() { onHandshakeReady(); },
      [this](const std::string& reason) { onHandshakeError(reason); });
}

void UrmaAsyncSocket::onHandshakeReady() {
  tcp_handshake_handler_.reset();

  res_.shared_io_jfr = shared_->ioJfr();
  res_.shared_io_jfc = shared_->ioJfc();
  res_.shared_fc_jfr = shared_->fcJfr();
  res_.shared_fc_jfc = shared_->fcJfc();
  peer_rx_buf_size_ = handshake_->peer_rx_buf_size_;

  // TX JFCE handler + 初始 arm(READ completion 也能异步唤醒 pollTx)
  if (res_.tx_jfce) {
    tx_cq_handler_ = new UrmaCqHandler(evb_, res_.tx_jfce->fd, this);
    urma_rearm_jfc(res_.tx_jfc, false);
  }

  if (res_.remote_jetty) {
    shared_->registerSocket(res_.remote_jetty->id.id, this);
  }
  if (res_.remote_fc_jetty) {
    shared_->registerFcSocket(res_.remote_fc_jetty->id.id, this);
  }

  delete handshake_;
  handshake_ = nullptr;

  state_ = State::kOpen;

  XLOG(INFO) << "[URMA] handshakeReady state->kOpen";

  handlePendingWrites();

  // 启动大包超时扫描(100ms 周期,设计 §4.7.8)
  if (evb_) {
    evb_->runAfterDelay([this] { scanBigMsgTimeout(); }, 100);
  }
}

void UrmaAsyncSocket::onHandshakeError(const std::string& reason) {
  XLOG(ERR) << "UrmaAsyncSocket: handshake error: " << reason;
  tcp_handshake_handler_.reset();
  state_ = State::kError;
  if (handshake_) {
    delete handshake_;
    handshake_ = nullptr;
  }
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
  read_cb_ = callback;
  if (read_cb_ && !read_queue_.empty()) {
    deliverReadData();
  }
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

  if (pending_write_head_) {
    auto pw = std::make_unique<PendingWrite>();
    pw->cb = cb;
    pw->buf = std::move(chain);
    pw->next = nullptr;
    pending_write_tail_->next = std::move(pw);
    pending_write_tail_ = pending_write_tail_->next.get();
    return;
  }

  if (state_ != State::kOpen) {
    auto pw = std::make_unique<PendingWrite>();
    pw->cb = cb;
    pw->buf = std::move(chain);
    pw->next = nullptr;
    pending_write_head_ = std::move(pw);
    pending_write_tail_ = pending_write_head_.get();
    return;
  }

  pollTx();

  uint32_t total_posted = 0;
  bool big_deferred = false;
  urma_status_t ret = sendChain(cb, chain, total_posted, big_deferred);
  if (ret == URMA_EAGAIN) {
    auto pw = std::make_unique<PendingWrite>();
    pw->cb = cb;
    pw->buf = std::move(chain);
    pw->next = nullptr;
    pending_write_head_ = std::move(pw);
    pending_write_tail_ = pending_write_head_.get();
    if (res_.tx_jfc) {
      urma_status_t rearm_ret = urma_rearm_jfc(res_.tx_jfc, false);
      if (rearm_ret != URMA_SUCCESS) {
        XLOG(ERR) << "UrmaAsyncSocket: urma_rearm_jfc(tx) failed, status=" << rearm_ret;
      }
    }
  } else if (ret == URMA_SUCCESS) {
    if (cb && !big_deferred) cb->writeSuccess();
    bytes_written_ += total_posted;
  }
}

// ── TX completion ──

void UrmaAsyncSocket::pollTx() noexcept {
  if (!res_.tx_jfc) return;
  if (state_ != State::kOpen) return;
  urma_cr_t cr[POLL_BATCH_MAX];
  int n;
  while ((n = urma_poll_jfc(res_.tx_jfc, POLL_BATCH_MAX, cr)) > 0) {
    for (int i = 0; i < n; ++i) {
      uint64_t ctx = cr[i].user_ctx;
      if (ctx == kCtrlUserCtx) {
        // 控制包/FREE 的 TX completion:释放 sge 引用
        if (!ctrl_sge_refs_.empty()) {
          ctrl_sge_refs_.pop_front();
        }
        continue;
      }
      if (ctx & kReadUserCtxBase) {
        // B-4: 校验 CQE msg_id,跳过旧 READ 的重复 CQE
        uint16_t cqe_msg_id = static_cast<uint16_t>(ctx & 0xFFFF);
        if (cqe_msg_id != pending_read_msg_id_) {
          XLOG(WARN) << "[URMA] pollTx: stale READ cqe msg_id=" << cqe_msg_id
                     << " expect=" << pending_read_msg_id_;
          continue;
        }
        if (cr[i].status == URMA_CR_SUCCESS) {
          onReadComplete();
        } else {
          onReadFailed(cr[i].status);
        }
        continue;
      }
      // SEND completion:释放 sbuf_
      uint64_t idx = ctx;
      if (idx < sbuf_.size()) {
        sbuf_[idx] = {};
      }
    }
  }
  urma_rearm_jfc(res_.tx_jfc, false);
}

void UrmaAsyncSocket::retryWrite() noexcept {
  handlePendingWrites();
}

urma_status_t UrmaAsyncSocket::sendChain(WriteCallback* cb,
                                          std::unique_ptr<IOBuf>& chain,
                                          uint32_t& total_posted,
                                          bool& big_deferred) noexcept {
  total_posted = 0;
  big_deferred = false;
  if (state_ != State::kOpen) {
    return URMA_EAGAIN;
  }
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
        // §4.7 大包:走单边 READ(控制包 SEND_IMM + 对端 READ 拉取 + FREE)
        auto big = std::move(chain);
        size_t big_len = big->length();
        chain = big->pop();  // 剩余 chain 返回
        if (sendBigMsgByRead(cb, std::move(big))) {
          big_deferred = true;  // writeSuccess 延迟到对端 FREE
          total_posted += static_cast<uint32_t>(big_len);
          continue;
        }
        if (cb) cb->writeErr(total_posted, AsyncSocketException(
            AsyncSocketException::UNKNOWN, "sendBigMsgByRead failed"));
        return URMA_FAIL;
      }
      (*sge_arr)[num_sge].addr = reinterpret_cast<uint64_t>(chain->writableData());
      (*sge_arr)[num_sge].len = static_cast<uint32_t>(iobuf_len);
      (*sge_arr)[num_sge].tseg = UrmaInit::getPoolTseg();
      wr_chain_head->prependChain(chain->cloneOne());
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
      return URMA_EAGAIN;
    }
    XLOG(ERR) << "UrmaAsyncSocket: post failed, status=" << ret;
    if (cb) cb->writeErr(total_posted, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "urma_post_jetty_send_wr failed"));
    return URMA_FAIL;
  }
  return URMA_SUCCESS;
}

void UrmaAsyncSocket::handlePendingWrites() noexcept {
  pollTx();

  while (pending_write_head_) {
    auto* pw = pending_write_head_.get();
    std::unique_ptr<IOBuf> chain = std::move(pw->buf);
    uint32_t total_posted = 0;
    bool big_deferred = false;

    urma_status_t ret = sendChain(pw->cb, chain, total_posted, big_deferred);
    if (ret == URMA_EAGAIN) {
      pw->buf = std::move(chain);
      if (res_.tx_jfc) {
        urma_status_t rearm_ret = urma_rearm_jfc(res_.tx_jfc, false);
        if (rearm_ret != URMA_SUCCESS) {
          XLOG(ERR) << "UrmaAsyncSocket: urma_rearm_jfc(tx) failed, status=" << rearm_ret;
        }
      }
      return;
    }
    if (ret == URMA_SUCCESS) {
      WriteCallback* cb = pw->cb;
      pending_write_head_ = std::move(pending_write_head_->next);
      if (!pending_write_head_) {
        pending_write_tail_ = nullptr;
      }
      if (cb && !big_deferred) cb->writeSuccess();
      bytes_written_ += total_posted;
    }
    if (ret == URMA_FAIL) {
      pending_write_head_ = std::move(pending_write_head_->next);
      if (!pending_write_head_) {
        pending_write_tail_ = nullptr;
      }
    }
  }
}

// ── 单边 READ:发送方 ──

bool UrmaAsyncSocket::sendBigMsgByRead(WriteCallback* cb,
                                       std::unique_ptr<IOBuf> node) noexcept {
  uint32_t data_size = static_cast<uint32_t>(node->length());

  // A-1: 非 MR 内存先 memcpy 一次到 MR buffer 兜底
  if (!UrmaInit::isPoolBuffer(node->data())) {
    void* mr_buf = UrmaInit::poolAlloc(data_size);
    if (!mr_buf) {
      XLOG(ERR) << "[URMA] sendBig: poolAlloc fallback failed size=" << data_size;
      return false;
    }
    std::memcpy(mr_buf, node->data(), data_size);
    node = IOBuf::takeOwnership(
        mr_buf, data_size,
        [](void* ptr, void*) { UrmaInit::poolFree(ptr); });
  }

  // C-3: msg_id 回绕后跳过仍在途的
  uint16_t msg_id = next_msg_id_;
  while (pending_big_msg_.count(msg_id)) {
    ++msg_id;
  }
  next_msg_id_ = msg_id + 1;

  uint64_t remote_va = reinterpret_cast<uint64_t>(node->data());
  XLOG(INFO) << "[URMA] sendBig msg_id=" << msg_id << " size=" << data_size
             << " remote_va=" << remote_va;

  PendingBigMsg p;
  p.iobuf = std::shared_ptr<IOBuf>(std::move(node));
  p.post_time = std::chrono::steady_clock::now();
  p.retries = 0;
  p.is_free = false;
  p.cb = cb;
  pending_big_msg_.emplace(msg_id, std::move(p));

  if (!postControlPacket(msg_id, remote_va, data_size)) {
    pending_big_msg_.erase(msg_id);
    return false;
  }
  return true;
}

bool UrmaAsyncSocket::postControlPacket(uint16_t msg_id, uint64_t remote_va,
                                        uint32_t data_size) noexcept {
  if (!res_.remote_jetty) return false;
  // B-6: 控制包 8 字节 remote_va 用每连接固定的 MR buffer(DMA 可读,复用不动态申请)
  if (!ctrl_meta_mr_) {
    ctrl_meta_mr_ = UrmaInit::poolAlloc(sizeof(uint64_t));
    if (!ctrl_meta_mr_) {
      XLOG(ERR) << "[URMA] ctrlPullPost: poolAlloc ctrl_meta failed";
      return false;
    }
  }
  *reinterpret_cast<uint64_t*>(ctrl_meta_mr_) = remote_va;
  // sge 必须在堆上(URMA 驱动异步 DMA 读 sge,栈变量 post 后悬垂)
  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(1);
  (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(ctrl_meta_mr_);
  (*sge_arr)[0].len = sizeof(uint64_t);
  (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_SEND_IMM;
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.flag.bs.inline_flag = 0;
  wr.send.src.sge = sge_arr->data();
  wr.send.src.num_sge = 1;
  wr.send.imm_data = encodeBigMsgImm(msg_id, kCtrlTypePull, data_size);
  wr.user_ctx = kCtrlUserCtx;
  wr.next = nullptr;

  // 持有 sge 到 SEND completion(pollTx 里 kCtrlUserCtx 跳过释放,
  // 这里用 ctrl_sge_refs_ 存)
  ctrl_sge_refs_.push_back(sge_arr);

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t st = urma_post_jetty_send_wr(res_.io_jetty, &wr, &bad_wr);
  if (st != URMA_SUCCESS) {
    ctrl_sge_refs_.pop_back();
  }
  XLOG(INFO) << "[URMA] ctrlPullPost msg_id=" << msg_id << " size=" << data_size
             << " st=" << static_cast<int>(st);
  return st == URMA_SUCCESS;
}

// ── 单边 READ:接收方 ──

void UrmaAsyncSocket::onControlMsg(uint64_t imm, const void* meta) noexcept {
  uint16_t msg_id;
  uint8_t is_ctl;
  uint32_t data_size;
  decodeBigMsgImm(imm, msg_id, is_ctl, data_size);

  if (is_ctl == kCtrlTypeFree) {
    // 发送方收到 FREE:释放大 buffer 并回调 writeSuccess(幂等,重复 FREE 忽略)
    auto it = pending_big_msg_.find(msg_id);
    XLOG(INFO) << "[URMA] ctrlFreeRecv msg_id=" << msg_id
               << " found=" << (it != pending_big_msg_.end());
    if (it != pending_big_msg_.end() && !it->second.is_free) {
      WriteCallback* cb = it->second.cb;
      pending_big_msg_.erase(it);
      if (cb) cb->writeSuccess();
    }
    return;
  }

  if (is_ctl != kCtrlTypePull) {
    return;
  }

  // PULL:解码元数据(8 字节 remote_va)
  uint64_t remote_va = 0;
  if (meta) {
    std::memcpy(&remote_va, meta, sizeof(remote_va));
  }
  XLOG(INFO) << "[URMA] ctrlPullRecv msg_id=" << msg_id << " size=" << data_size
             << " remote_va=" << remote_va << " pending=" << has_pending_read_
             << " is_ctl=" << static_cast<int>(is_ctl);

  if (has_pending_read_) {
    // 有 READ 在途:控制包进保序队列(等当前 READ 完成)
    RxBufItem item;
    item.is_control = true;
    item.msg_id = msg_id;
    item.remote_va = remote_va;
    item.data_size = data_size;
    rx_pending_queue_.push(std::move(item));
    return;
  }

  has_pending_read_ = startRead(msg_id, remote_va, data_size);
}

bool UrmaAsyncSocket::startRead(uint16_t msg_id, uint64_t remote_va,
                                uint32_t data_size) noexcept {
  void* buf = UrmaInit::poolAlloc(data_size);
  if (!buf) {
    XLOG(ERR) << "startRead: poolAlloc failed for " << data_size << " msg_id=" << msg_id;
    return false;
  }
  auto iobuf = IOBuf::takeOwnership(
      buf, data_size,
      [](void* ptr, void*) { UrmaInit::poolFree(ptr); });

  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(2);
  (*sge_arr)[0].addr = remote_va;
  (*sge_arr)[0].len = data_size;
  (*sge_arr)[0].tseg = res_.remote_seg;
  (*sge_arr)[1].addr = reinterpret_cast<uint64_t>(iobuf->writableData());
  (*sge_arr)[1].len = data_size;
  (*sge_arr)[1].tseg = UrmaInit::getPoolTseg();

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_READ;
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.rw.src.sge = &(*sge_arr)[0];
  wr.rw.src.num_sge = 1;
  wr.rw.dst.sge = &(*sge_arr)[1];
  wr.rw.dst.num_sge = 1;
  wr.user_ctx = kReadUserCtxBase | msg_id;
  wr.next = nullptr;

  pending_read_msg_id_ = msg_id;
  pending_read_buf_ = std::move(iobuf);
  pending_read_sge_ = sge_arr;

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t st = urma_post_jetty_send_wr(res_.io_jetty, &wr, &bad_wr);
  if (st != URMA_SUCCESS) {
    pending_read_sge_.reset();
    pending_read_buf_.reset();
    XLOG(ERR) << "startRead: post READ failed, status=" << st << " msg_id=" << msg_id;
    return false;
  }
  XLOG(INFO) << "[URMA] readPost msg_id=" << msg_id << " size=" << data_size
             << " remote_va=" << remote_va << " st=" << static_cast<int>(st);
  return true;
}

void UrmaAsyncSocket::onReadComplete() noexcept {
  if (!pending_read_buf_) {
    XLOG(WARN) << "[URMA] readDone: pending_read_buf_ already reset, skip";
    return;
  }
  auto data = std::move(pending_read_buf_);
  pending_read_sge_.reset();
  has_pending_read_ = false;
  bytes_received_ += data->length();
  XLOG(INFO) << "[URMA] readDone msg_id=" << pending_read_msg_id_
             << " len=" << data->length();

  read_queue_.append(std::move(data));
  if (read_cb_) {
    deliverReadData();
  }

  // 连接已关闭时不 post FREE(remote_jetty 可能已 null)
  if (state_ != State::kOpen) {
    return;
  }

  if (!postFreeNotify(pending_read_msg_id_)) {
    XLOG(WARN) << "UrmaAsyncSocket: postFreeNotify failed for msg_id="
               << pending_read_msg_id_;
  }

  // 串行保序:从队列按序取出(控制包→发起新 READ 后停止,小包→交付)
  drainRxPendingQueue();
}

void UrmaAsyncSocket::onReadFailed(urma_status_t status) noexcept {
  XLOG(ERR) << "UrmaAsyncSocket: READ failed, msg_id=" << pending_read_msg_id_
            << " status=" << status;
  pending_read_buf_.reset();
  pending_read_sge_.reset();
  has_pending_read_ = false;
  drainRxPendingQueue();
}

bool UrmaAsyncSocket::postFreeNotify(uint16_t msg_id) noexcept {
  if (!res_.remote_jetty) return false;
  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_SEND_IMM;
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.flag.bs.inline_flag = 0;  // ctrl packet inline
  wr.send.src.num_sge = 0;
  wr.send.imm_data = encodeBigMsgImm(msg_id, kCtrlTypeFree, 0);
  wr.user_ctx = kCtrlUserCtx;
  wr.next = nullptr;

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t st = urma_post_jetty_send_wr(res_.io_jetty, &wr, &bad_wr);
  XLOG(INFO) << "[URMA] freePost msg_id=" << msg_id << " st=" << static_cast<int>(st);
  return st == URMA_SUCCESS;
}

void UrmaAsyncSocket::drainRxPendingQueue() noexcept {
  while (!rx_pending_queue_.empty()) {
    RxBufItem item = std::move(rx_pending_queue_.front());
    rx_pending_queue_.pop();
    if (item.is_control) {
      // 串行保序:遇到控制包,发起新 READ 后停止取出
      XLOG(INFO) << "[URMA] drainCtrl msg_id=" << item.msg_id
                 << " size=" << item.data_size << " qsize=" << rx_pending_queue_.size();
      has_pending_read_ = startRead(item.msg_id, item.remote_va, item.data_size);
      return;
    }
    XLOG(INFO) << "[URMA] drainData len=" << item.data->length();
    read_queue_.append(std::move(item.data));
    if (read_cb_) {
      deliverReadData();
    }
  }
  has_pending_read_ = false;
}

// ── 单边 READ:超时重发 ──

void UrmaAsyncSocket::scanBigMsgTimeout() noexcept {
  if (state_ != State::kOpen) return;

  auto now = std::chrono::steady_clock::now();
  auto timeout = std::chrono::milliseconds(7000);  // 7s 超时(设计 §4.7.8)

  for (auto it = pending_big_msg_.begin(); it != pending_big_msg_.end();) {
    auto& p = it->second;
    if (now - p.post_time < timeout) {
      ++it;
      continue;
    }

    if (p.retries >= kCtrlMsgMaxRetries) {
      XLOG(ERR) << "[URMA] sendBig: msg " << it->first
                << " timeout, giving up (is_free=" << p.is_free << ")";
      WriteCallback* cb = p.cb;
      it = pending_big_msg_.erase(it);
      // B-3: 回调 writeErr,上层可感知大包失败
      if (cb) {
        cb->writeErr(0, AsyncSocketException(
            AsyncSocketException::UNKNOWN, "big msg timeout after retries"));
      }
      continue;
    }

    if (p.is_free) {
      // FREE 重发(发送方未确认释放)
      if (postFreeNotify(it->first)) {
        p.retries++;
        p.post_time = now;
        XLOG(WARN) << "[URMA] sendBig: resend FREE msg_id=" << it->first
                   << " retries=" << (int)p.retries;
      } else {
        p.retries = kCtrlMsgMaxRetries;
      }
    } else {
      // 控制包重发
      uint64_t remote_va = reinterpret_cast<uint64_t>(p.iobuf->data());
      uint32_t data_size = static_cast<uint32_t>(p.iobuf->length());
      if (postControlPacket(static_cast<uint16_t>(it->first), remote_va,
                            data_size)) {
        p.retries++;
        p.post_time = now;
        XLOG(WARN) << "UrmaAsyncSocket: resend control msg_id=" << it->first
                   << " retries=" << (int)p.retries << " size=" << data_size;
      } else {
        p.retries = kCtrlMsgMaxRetries;
      }
    }
    ++it;
  }

  // 重新调度(100ms 周期,设计 §4.7.8 UrmaIdleMonitor 量级)
  if (evb_ && state_ == State::kOpen) {
    evb_->runAfterDelay([this] { scanBigMsgTimeout(); }, 100);
  }
}

// ── 接收(普通 SEND 数据) ──

void UrmaAsyncSocket::onRxBuf(std::unique_ptr<IOBuf> data) noexcept {
  bytes_received_ += data->length();
  XLOG(INFO) << "[URMA] rxSmall len=" << data->length()
             << " pending=" << has_pending_read_;
  if (has_pending_read_) {
    RxBufItem item;
    item.is_control = false;
    item.data = std::move(data);
    rx_pending_queue_.push(std::move(item));
    return;
  }
  read_queue_.append(std::move(data));
  if (read_cb_) {
    deliverReadData();
  }
}

void UrmaAsyncSocket::deliverReadData() noexcept {
  if (!read_cb_ || read_queue_.empty()) return;
  if (read_cb_->isBufferMovable()) {
    auto buf = read_queue_.move();
    read_cb_->readBufferAvailable(std::move(buf));
  } else {
    void* buf = nullptr;
    size_t buf_len = 0;
    read_cb_->getReadBuffer(&buf, &buf_len);
    if (!buf || buf_len == 0) return;

    auto front = read_queue_.front();
    if (!front) return;
    size_t copy_len = std::min(front->length(), buf_len);
    memcpy(buf, front->data(), copy_len);
    read_queue_.trimStart(copy_len);
    read_cb_->readDataAvailable(copy_len);
  }

  if (read_cb_ && !read_queue_.empty()) {
    deliverReadData();
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
  if (res_.tx_jfc) {
    pollTx();
  }
}

void UrmaAsyncSocket::shutdownWriteNow() {
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
  pending_big_msg_.clear();
  pending_read_buf_.reset();
  pending_read_sge_.reset();
  if (ctrl_meta_mr_) {
    UrmaInit::poolFree(ctrl_meta_mr_);
    ctrl_meta_mr_ = nullptr;
  }
  // 清空保序队列
  while (!rx_pending_queue_.empty()) {
    rx_pending_queue_.pop();
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
  sbuf_.clear();
  pending_write_head_.reset();
  pending_write_tail_ = nullptr;
  read_queue_.move();
}

// ── 其他 ──

void UrmaAsyncSocket::attachEventBase(EventBase* /*eventBase*/) {
}

void UrmaAsyncSocket::detachEventBase() {
}

void UrmaAsyncSocket::setSendTimeout(uint32_t milliseconds) {
  send_timeout_ms_ = milliseconds;
}

void UrmaAsyncSocket::getLocalAddress(SocketAddress* /*address*/) const {
}

void UrmaAsyncSocket::getPeerAddress(SocketAddress* /*address*/) const {
}

} // namespace folly

#endif // FOLLY_HAVE_URMA