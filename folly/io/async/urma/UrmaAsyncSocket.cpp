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
#include <cerrno>
#include <cstring>
#include <utility>

#include <folly/io/async/urma/UrmaCqHandler.h>
#include <folly/io/async/urma/UrmaHandshake.h>
#include <folly/io/async/urma/UrmaSharedJfr.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaJfsPool.h>
#include <folly/io/async/urma/UrmaJfrPool.h>
#include <folly/io/async/urma/UrmaImportCache.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <folly/logging/xlog.h>

namespace folly {

// 单连接 pin JFS:首次发送时 round-robin 选定一个 JFS,后续所有发送复用
// 保证同连接的 SEND 操作走同一 JFS,硬件保证同 JFS 内 FIFO 顺序
SharedJfs* UrmaAsyncSocket::acquirePinnedJfs(UrmaJfsPool* pool,
                                              uint32_t* out_index) {
  if (pinned_jfs_index_ != kInvalidJfsIndex) {
    // 优先用 pin 的 JFS(保证单连接所有 WR 走同一 JFS,硬件保序)
    SharedJfs* sj = pool->tryAcquireIoJfsByIndex(pinned_jfs_index_);
    if (sj) {
      *out_index = pinned_jfs_index_;
      return sj;
    }
    // pin 的 JFS 的 SQ 已满 → 返回 nullptr(EAGAIN),让上层挂起 pending_write_,
    // 等 TX CQE 释放 SQ 槽位后重试。不再 fallback round-robin(避免乱序)。
    return nullptr;
  }
  SharedJfs* sj = pool->acquireIoJfs(out_index);
  if (sj) {
    pinned_jfs_index_ = *out_index;  // pin 到首次获取的 JFS
  }
  return sj;
}

// 统计 IOBuf chain 中 >peer_rx_buf_size_ 的大包数量(用于 writeChain 完成组计数)
static uint32_t countBigMsgs(const IOBuf* head, uint32_t peer_rx_buf_size) {
  uint32_t n = 0;
  if (!head) return 0;
  // Phase 3: headroom 路径下大包判断:data_len + 64B header > peer_rx_buf_size
  const IOBuf* b = head;
  do {
    if (b->length() + sizeof(UrmaMsgHeader) > peer_rx_buf_size) {
      ++n;
    }
    b = b->next();
  } while (b != head);
  return n;
}

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
  socket_id_ = SocketIdAllocator::get().allocate();
  tcp_socket_ = AsyncSocket::newSocket(evb);
  connect_cb_ = std::make_unique<ConnectCallback>();
  connect_cb_->self = this;
  tcp_socket_->connect(connect_cb_.get(), addr, 0);
}

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* evb, int fd)
    : evb_(evb), tcp_fd_(fd), state_(State::kIdle) {
  socket_id_ = SocketIdAllocator::get().allocate();
  startHandshake(false);
}

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* evb, int fd,
                                  UrmaHandshake* handshake)
    : evb_(evb),
      tcp_fd_(fd),
      state_(State::kHandshaking),
      handshake_(handshake) {
  socket_id_ = SocketIdAllocator::get().allocate();
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

  // Phase 2: TX CQE 由全局 JFS 池的 CqHandler 处理,不再 per-连接创建 tx_cq_handler_

  if (res_.remote_jetty) {
    XLOG(DBG) << "[URMA] onHandshakeReady: registerSocket socket_id="
               << socket_id_.toUint64() << " peer_io_jetty_id=" << res_.peer_io_jetty_id
               << " this=" << this;
    // Phase 3: 用本端 SocketId 注册(接收方按 header.dst_socket_id 查表路由)
    // 发送方写入 header.dst_socket_id = peer_socket_id(对端 = 接收方本端)
    // TX CQE 路由:harvestTxAndDispatch 用 tx_ctx->socket_id(本端)查表,同 key 命中
    shared_->registerSocket(socket_id_.toUint64(), this);
  }
  if (res_.remote_fc_jetty) {
    XLOG(DBG) << "[URMA] onHandshakeReady: registerFcSocket socket_id="
               << socket_id_.toUint64() << " peer_fc_jetty_id=" << res_.peer_fc_jetty_id
               << " this=" << this;
    shared_->registerFcSocket(socket_id_.toUint64(), this);
  }

  delete handshake_;
  handshake_ = nullptr;

  state_ = State::kOpen;

  // 休眠管理:握手成功,本连接激活,唤醒可能休眠的 CqHandler
  shared_->incActiveConn();

  handlePendingWrites();

  // Phase 7: 接收方授予初始 credit(FC_REP),让对端可以开始发送
  // 初始 credit = rx_depth / INITIAL_CREDIT_DIVISOR
  if (res_.remote_fc_jetty) {
    uint32_t initial_credit = 0;
    if (auto* pool = UrmaJfrPool::get()) {
      if (auto* jfr = pool->getIoJfrByIndex(0)) {
        initial_credit = jfr->rx_depth / UrmaFcState::INITIAL_CREDIT_DIVISOR;
        if (initial_credit > 0) {
          jfr->credit_idle.fetch_sub(initial_credit, std::memory_order_acq_rel);
          jfr->credit_allocated.fetch_add(initial_credit, std::memory_order_acq_rel);
        }
      }
    }
    if (initial_credit > 0) {
      fc_state_.peer_granted.fetch_add(initial_credit, std::memory_order_acq_rel);
      uint16_t rep_seq = fc_state_.fc_seq_next.fetch_add(1, std::memory_order_relaxed);
      bool sent = sendFcMsg(FC_REP, static_cast<uint16_t>(initial_credit), rep_seq);
      XLOG(DBG) << "[URMA-FC] onHandshakeReady: granted initial credit=" << initial_credit
                << " rep_seq=" << rep_seq << " sent=" << sent
                << " socket_id=0x" << std::hex << socket_id_.toUint64() << std::dec;
    }
  }

  // 启动大包超时扫描(100ms 周期,设计 §4.7.8)
  if (evb_) {
    evb_->runAfterDelay([this] { scanBigMsgTimeout(); }, 100);
  }
}

void UrmaAsyncSocket::onHandshakeError(const std::string& reason) {
  XLOG(ERR) << "UrmaAsyncSocket: handshake error: " << reason;
  // tcp_handshake_handler_ 和 handshake_ 都不能同步删除 —
  // onHandshakeError 可能从 TcpHandshakeHandler::handlerReady →
  // UrmaHandshake::onDataAvailable/timeoutExpired → error_cb_ 调用链进入,
  // 同步删除会导致当前栈帧 use-after-free (SIGSEGV/SIGBUS)。
  // 先 unregisterHandler(从 epoll 摘除),再用 runInLoop 异步 delete。
  if (tcp_handshake_handler_) {
    auto* handler = tcp_handshake_handler_.release();
    tcp_handshake_handler_ = nullptr;
    handler->unregisterHandler();  // 立即从 epoll 摘除,防止再次触发 handlerReady
    if (evb_) {
      evb_->runInLoop([handler] { delete handler; });
    } else {
      delete handler;
    }
  }
  if (handshake_) {
    auto* hs = handshake_;
    handshake_ = nullptr;
    if (evb_) {
      evb_->runInLoop([hs] { delete hs; });
    } else {
      delete hs;
    }
  }

  // Phase 8: 降级 TCP — 握手失败时用已有 tcp_fd_ 创建 AsyncSocket 作为 delegate
  // 对上层透明:后续 write/read/close 全部委托给 tcp_delegate_
  if (tcp_fd_ >= 0) {
    XLOG(WARN) << "UrmaAsyncSocket: falling back to TCP, reason=" << reason;
    fallbackToTcp();
    return;
  }

  // 无 tcp_fd_(如 connect 失败),无法降级,直接报错
  state_ = State::kError;
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

void UrmaAsyncSocket::fallbackToTcp() {
  // 用已有 tcp_fd_ 创建 AsyncSocket,接管 fd 所有权
  tcp_delegate_ = AsyncSocket::newSocket(evb_, NetworkSocket(tcp_fd_));
  // tcp_fd_ 所有权移交给 delegate,本对象不再持有
  tcp_fd_ = -1;

  // 检查 delegate 是否已断开(对端可能已关闭 TCP fd)
  if (!tcp_delegate_ || !tcp_delegate_->good()) {
    XLOG(WARN) << "UrmaAsyncSocket: TCP fallback delegate not good (peer closed), "
               << "fd state invalid, transitioning to kError";
    state_ = State::kError;
    // 回调所有 pending write 为错误
    while (pending_write_head_) {
      auto pw = std::move(pending_write_head_);
      pending_write_head_ = std::move(pw->next);
      if (!pending_write_head_) {
        pending_write_tail_ = nullptr;
      }
      if (pw->cb) {
        pw->cb->writeErr(0, AsyncSocketException(
            AsyncSocketException::END_OF_FILE,
            "TCP fallback delegate not good"));
      }
    }
    return;
  }

  XLOG(WARN) << "UrmaAsyncSocket: TCP fallback active, delegate fd good, "
             << "replaying pending writes and delegating read/write";
  state_ = State::kOpen;  // 降级后视为 open,操作委托给 delegate

  // 将 pending_write_ 队列中的写入通过 delegate 重放
  while (pending_write_head_) {
    auto pw = std::move(pending_write_head_);
    pending_write_head_ = std::move(pw->next);
    if (!pending_write_head_) {
      pending_write_tail_ = nullptr;
    }
    if (pw->cb) {
      tcp_delegate_->writeChain(pw->cb, std::move(pw->buf), WriteFlags::NONE);
    }
  }

  // 如果已有 read_cb_,设置到 delegate
  if (read_cb_) {
    tcp_delegate_->setReadCB(read_cb_);
  }
}

void UrmaAsyncSocket::onHandshakeData(const uint8_t* data, size_t len) {
  if (handshake_) {
    handshake_->onDataAvailable(data, len);
  }
}

// ── AsyncReader ──

void UrmaAsyncSocket::setReadCB(ReadCallback* callback) noexcept {
  // Phase 8: 降级 TCP 模式下委托给 delegate
  if (tcp_delegate_) {
    tcp_delegate_->setReadCB(callback);
    return;
  }
  read_cb_ = callback;
  if (read_cb_ && !read_queue_.empty()) {
    deliverReadData();
  }
}

// ── AsyncWriter ──

void UrmaAsyncSocket::write(WriteCallback* cb, const void* buf, size_t bytes,
                             WriteFlags flags) noexcept {
  if (tcp_delegate_) {
    tcp_delegate_->write(cb, buf, bytes, flags);
    return;
  }
  auto iobuf = IOBuf::copyBuffer(buf, bytes);
  writeChain(cb, std::move(iobuf), flags);
}

void UrmaAsyncSocket::writev(WriteCallback* cb, const iovec* vec, size_t count,
                              WriteFlags flags) noexcept {
  if (tcp_delegate_) {
    tcp_delegate_->writev(cb, vec, count, flags);
    return;
  }
  auto iobuf = IOBuf::wrapIov(vec, count);
  writeChain(cb, std::move(iobuf), flags);
}

void UrmaAsyncSocket::writeChain(WriteCallback* cb, std::unique_ptr<IOBuf>&& chain,
                                  WriteFlags flags) noexcept {
  if (tcp_delegate_) {
    tcp_delegate_->writeChain(cb, std::move(chain), flags);
    return;
  }
  if (state_ == State::kError || state_ == State::kClosed) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "socket not open"));
    return;
  }

  // 本 writeChain 的完成组:组内所有大包收到对端 FREE 后 writeSuccess 只回调一次
  auto group = std::make_shared<BigMsgGroup>();
  group->remaining = countBigMsgs(chain.get(), peer_rx_buf_size_);

  if (pending_write_head_) {
    auto pw = std::make_unique<PendingWrite>();
    pw->cb = cb;
    pw->buf = std::move(chain);
    pw->big_group = group;
    pw->next = nullptr;
    pending_write_tail_->next = std::move(pw);
    pending_write_tail_ = pending_write_tail_->next.get();
    return;
  }

  if (state_ != State::kOpen) {
    auto pw = std::make_unique<PendingWrite>();
    pw->cb = cb;
    pw->buf = std::move(chain);
    pw->big_group = group;
    pw->next = nullptr;
    pending_write_head_ = std::move(pw);
    pending_write_tail_ = pending_write_head_.get();
    return;
  }

  pollTx();

  uint32_t total_posted = 0;
  bool big_deferred = false;
  urma_status_t ret = sendChain(cb, chain, total_posted, big_deferred, group);
  if (ret == URMA_EAGAIN) {
    auto pw = std::make_unique<PendingWrite>();
    pw->cb = cb;
    pw->buf = std::move(chain);
    pw->big_group = group;
    pw->next = nullptr;
    pending_write_head_ = std::move(pw);
    pending_write_tail_ = pending_write_head_.get();
    // Phase 2: 武装全局 JFS 池的 TX JFCE 中断,等待 CQE 唤醒
    if (auto* pool = UrmaJfsPool::get()) {
      pool->rearmAllTxJfce();
    }
  } else if (ret == URMA_SUCCESS) {
//                << " big_deferred=" << big_deferred
//                << " pending_big=" << pending_big_msg_.size();
    // 组内大包未全部 FREE 时延迟 writeSuccess(跨 EAGAIN 拆分的多次 sendChain 也成立)
    if (cb && group->remaining == 0) cb->writeSuccess();
    bytes_written_ += total_posted;
  }
}

// ── TX completion ──

void UrmaAsyncSocket::pollTx() noexcept {
  if (state_ != State::kOpen) return;
  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) return;
  // 性能优化:只扫本连接活跃 JFS 位图(避免遍历全部 64 个 JFS)
  // 位图为 0 时直接返回,零系统调用
  // Phase 5: 跨连接的 TX CQE 收割由 UrmaJfrPool::harvestTxAndDispatch 负责
  // (在 pollAndDispatch 中顺带收割所有 JFS 的 TX CQE 并路由)。
  // pollTx 只负责收割本连接 mask 置位的 JFS 上的 CQE(本连接 post 的 SEND/CTRL)。
  if (active_jfs_mask_ == 0) return;

  urma_cr_t cr[POLL_BATCH_MAX];
  uint64_t mask = active_jfs_mask_;
  while (mask) {
    uint32_t ji = __builtin_ctzll(mask);
    mask &= (mask - 1);
    SharedJfs* sj = pool->getIoJfsByIndex(ji);
    if (!sj || !sj->jfc) continue;
    int n;
    while ((n = urma_poll_jfc(sj->jfc, POLL_BATCH_MAX, cr)) > 0) {
      for (int i = 0; i < n; ++i) {
        auto* tx_ctx = reinterpret_cast<TxCompletionCtx*>(cr[i].user_ctx);
        if (!tx_ctx) continue;

        if (tx_ctx->type == TxCompletionCtx::MSG_READ) {
          // READ CQE:本连接 post 的 READ(handleRemoteReadCqe 路由来的)
          // 或其他连接 post 的 READ(本连接 mask 恰好置位此 JFS)
          if (tx_ctx->socket_id == socket_id_.toUint64()) {
            if (tx_ctx->msg_id != pending_read_msg_id_) {
              releaseActiveJfsSlot(tx_ctx->jfs_index);
              pool->releaseIoJfs(tx_ctx->jfs_index);
              delete tx_ctx;
              continue;
            }
            if (cr[i].status == URMA_CR_SUCCESS) {
              onReadComplete();
            } else {
              onReadFailed(cr[i].status);
            }
            releaseActiveJfsSlot(tx_ctx->jfs_index);
            pool->releaseIoJfs(tx_ctx->jfs_index);
            delete tx_ctx;
          } else {
            // 其他连接的 READ CQE:路由到目标连接
            UrmaAsyncSocket* peer = nullptr;
            if (auto* jfr_pool = UrmaJfrPool::get()) {
              peer = jfr_pool->findSocket(tx_ctx->socket_id);
            }
            if (peer) {
              peer->handleRemoteReadCqe(tx_ctx->msg_id, cr[i].status);
            }
            pool->releaseIoJfs(tx_ctx->jfs_index);
            delete tx_ctx;
          }
        } else {
          // MSG_DATA / MSG_CTRL_PULL / MSG_CTRL_FREE
          releaseActiveJfsSlot(tx_ctx->jfs_index);
          pool->releaseIoJfs(tx_ctx->jfs_index);
          delete tx_ctx;
        }
      }
    }
    // edge-triggered rearm: pollTx 收割 CQE 后必须 rearm,
    // 否则后续新 CQE 到达时 edge-triggered epoll 不触发(harvestTxAndDispatch 饿死)
    urma_rearm_jfc(sj->jfc, false);
  }
}

void UrmaAsyncSocket::retryWrite() noexcept {
  handlePendingWrites();
}

urma_status_t UrmaAsyncSocket::sendChain(WriteCallback* cb,
                                          std::unique_ptr<IOBuf>& chain,
                                          uint32_t& total_posted,
                                          bool& big_deferred,
                                          const std::shared_ptr<BigMsgGroup>& group) noexcept {
  total_posted = 0;
  big_deferred = false;
  if (state_ != State::kOpen) {
    return URMA_EAGAIN;
  }

  // Phase 7: credit 流控(软限制)— peer_credit <= 0 时发 FC_REQ 申请额度
  // 但不阻塞发送(EAGAIN 会串行化 close-loop 请求,导致 QPS 暴跌 350 倍)
  // credit 机制保留用于统计和诊断,实际防溢出由水位线 + recv buffer 池保证
  if (fc_state_.peer_credit.load(std::memory_order_acquire) <= 0) {
    uint32_t multiple = fc_state_.credit_multiple.load(std::memory_order_relaxed);
    auto* jfr_pool = UrmaJfrPool::get();
    uint16_t req_cnt = 1;
    if (jfr_pool) {
      if (auto* jfr = jfr_pool->getIoJfrByIndex(0)) {
        req_cnt = static_cast<uint16_t>(
            std::max(1u, multiple * jfr->rx_depth /
                          UrmaFcState::INITIAL_CREDIT_DIVISOR));
      }
    }
    uint16_t req_seq = fc_state_.fc_seq_next.fetch_add(1, std::memory_order_relaxed);
    sendFcMsg(FC_REQ, req_cnt, req_seq);
    XLOG(DBG) << "[URMA-FC] sendChain credit=0, req_cnt=" << req_cnt
              << " (soft limit, not blocking) socket_id=0x"
              << std::hex << socket_id_.toUint64() << std::dec;
    // 不返回 EAGAIN,继续发送(软限制)
  }

  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "JFS pool not initialized"));
    return URMA_FAIL;
  }

  while (chain) {
    uint32_t sge_total = 0;
    int num_sge = 0;

    uint8_t hw_max_sge = UrmaInit::getMaxJfsSge();
    size_t chain_remaining = chain->countChainElements();
    // 独立 buffer 方案:SGE[0] 固定给 header(24B),data SGE 最多 hw_max_sge-1
    uint8_t max_data_sge = static_cast<uint8_t>(
        std::min(chain_remaining, static_cast<size_t>(hw_max_sge - 1)));
    uint8_t sge_capacity = static_cast<uint8_t>(max_data_sge + 1);

    auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(sge_capacity);
    // 持有本次 WR 引用的 IOBuf chain(防止 post 期间被释放)
    auto wr_chain_head = std::make_shared<IOBuf>();

    while (chain && num_sge < max_data_sge) {
      size_t iobuf_len = chain->length();
      // 总长 = header(24B) + sge_total,不能超过 peer_rx_buf_size_(recv buffer 大小)
      if (sge_total + iobuf_len + sizeof(UrmaMsgHeader) > peer_rx_buf_size_ &&
          num_sge > 0) {
        break;
      }
      // 大包判断条件:data_len + 24 > peer_rx_buf_size_ → 走单边 READ
      if (iobuf_len + sizeof(UrmaMsgHeader) > peer_rx_buf_size_) {
        // §4.7 大包:走单边 READ(控制包 SEND_IMM + 对端 READ 拉取 + FREE)
        auto big = std::move(chain);
        size_t big_len = big->length();
        chain = big->pop();  // 剩余 chain 返回
        if (sendBigMsgByRead(cb, std::move(big), group)) {
          big_deferred = true;  // writeSuccess 延迟到组内所有大包 FREE
          total_posted += static_cast<uint32_t>(big_len);
          continue;
        }
        if (cb) cb->writeErr(total_posted, AsyncSocketException(
            AsyncSocketException::UNKNOWN, "sendBigMsgByRead failed"));
        if (group) group->done = true;
        return URMA_FAIL;
      }
      // clone IOBuf 节点,加入发送链
      std::unique_ptr<IOBuf> cloned = chain->cloneOne();
      // data SGE 从 SGE[1] 开始(SGE[0] 留给 header,稍后填入)
      (*sge_arr)[num_sge + 1].addr = reinterpret_cast<uint64_t>(cloned->writableData());
      (*sge_arr)[num_sge + 1].len = static_cast<uint32_t>(iobuf_len);
      (*sge_arr)[num_sge + 1].tseg = UrmaInit::getPoolTseg();
      wr_chain_head->appendChain(std::move(cloned));
      sge_total += iobuf_len;
      num_sge++;
      chain = chain->pop();
    }

    if (num_sge == 0) break;

    // 独立 buffer 方案:始终创建独立 header IOBuf 作为 SGE[0]
    // 不再判断 headroom,不再 prepend,不再区分路径 A/B
    uint16_t seq = send_seq_.fetch_add(1, std::memory_order_relaxed);
    auto hdr_buf = IOBuf::create(sizeof(UrmaMsgHeader));
    hdr_buf->append(sizeof(UrmaMsgHeader));
    writeHeader(hdr_buf.get(), res_.peer_socket_id, seq, MSG_DATA, 0);

    // SGE[0] = header(24B)
    (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(hdr_buf->writableData());
    (*sge_arr)[0].len = sizeof(UrmaMsgHeader);
    (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();
    num_sge += 1;
    sge_total += sizeof(UrmaMsgHeader);
    // hdr_buf 加入发送链,保持存活到 TX CQE
    wr_chain_head->appendChain(std::move(hdr_buf));

    // Phase 2: 从 JFS 池获取一个空闲 JFS(pin 到同连接固定 JFS,保证顺序)
    uint32_t jfs_index = 0;
    SharedJfs* sj = acquirePinnedJfs(pool, &jfs_index);
    if (!sj) {
      // 所有 JFS 的 SQ 都满 → EAGAIN
      return URMA_EAGAIN;
    }

    // Phase 2: 创建 TxCompletionCtx,move IOBuf chain 到 ctx
    // post 前 move-in,TX CQE 后 delete ctx → send_data 析构 → block 回收
    auto* tx_ctx = new TxCompletionCtx();
    tx_ctx->socket_id = socket_id_.toUint64();
    tx_ctx->seq = seq;
    tx_ctx->type = TxCompletionCtx::MSG_DATA;
    tx_ctx->jfs_index = jfs_index;
    tx_ctx->msg_id = 0;
    // 将 wr_chain_head move 到 ctx 的 send_data(持有 block 引用)
    tx_ctx->send_data = std::move(*wr_chain_head);
    // SGE 数组也由 ctx 管理(保持存活到硬件处理完毕)
    tx_ctx->sge_arr = sge_arr;

    // 性能优化:置位本连接活跃 JFS 位图(pollTx 只扫位图置位的 JFS)
    markActiveJfs(jfs_index);

    urma_jfs_wr_t wr = {};
    wr.opcode = URMA_OPC_SEND;
    wr.tjetty = res_.remote_jetty;
    wr.flag.bs.complete_enable = 1;
    wr.send.src.sge = sge_arr->data();
    wr.send.src.num_sge = static_cast<uint32_t>(num_sge);
    wr.user_ctx = reinterpret_cast<uint64_t>(tx_ctx);
    wr.next = nullptr;

    total_posted += sge_total;
    urma_jfs_wr_t* bad_wr = nullptr;
    urma_status_t ret = urma_post_jfs_wr(sj->jfs, &wr, &bad_wr);
    if (ret == URMA_SUCCESS) {
      // tx_ctx 持有 send_data(IOBuf chain) + sge_arr,TX CQE 后 delete 回收
      // Phase 7: 消耗一个 credit(软限制,允许负数,用于统计)
      fc_state_.peer_credit.fetch_sub(1, std::memory_order_acq_rel);
      continue;
    }
    // post 失败:回收 ctx + 归还 sq_inflight
    pool->releaseIoJfs(jfs_index);
    delete tx_ctx;
    if (ret == URMA_EAGAIN) {
      return URMA_EAGAIN;
    }
    XLOG(ERR) << "UrmaAsyncSocket: post failed, status=" << ret;
    if (cb) cb->writeErr(total_posted, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "urma_post_jfs_wr failed"));
    if (group) group->done = true;
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

    // 防御:老路径可能没建完成组,补一个(计数仅覆盖剩余 chain)
    if (!pw->big_group) {
      pw->big_group = std::make_shared<BigMsgGroup>();
      pw->big_group->remaining = countBigMsgs(chain.get(), peer_rx_buf_size_);
    }

    urma_status_t ret = sendChain(pw->cb, chain, total_posted, big_deferred,
                                  pw->big_group);
    if (ret == URMA_EAGAIN) {
      pw->buf = std::move(chain);
      // Phase 2: 武装全局 JFS 池的 TX JFCE 中断
      if (auto* pool = UrmaJfsPool::get()) {
        pool->rearmAllTxJfce();
      }
      return;
    }
    if (ret == URMA_SUCCESS) {
      WriteCallback* cb = pw->cb;
      auto big_group = pw->big_group;  // pw 即将销毁,先保存完成组
      pending_write_head_ = std::move(pending_write_head_->next);
      if (!pending_write_head_) {
        pending_write_tail_ = nullptr;
      }
//                  << " big_deferred=" << big_deferred
//                  << " pending_big=" << pending_big_msg_.size();
      if (cb && big_group->remaining == 0) cb->writeSuccess();
      bytes_written_ += total_posted;
    }
    if (ret == URMA_FAIL) {
      XLOG(ERR) << "[URMA] pendingWrite FAIL total=" << total_posted;
      pending_write_head_ = std::move(pending_write_head_->next);
      if (!pending_write_head_) {
        pending_write_tail_ = nullptr;
      }
    }
  }
}

// ── 单边 READ:发送方 ──

bool UrmaAsyncSocket::sendBigMsgByRead(WriteCallback* cb,
                                       std::unique_ptr<IOBuf> node,
                                       const std::shared_ptr<BigMsgGroup>& group) noexcept {
  uint32_t data_size = static_cast<uint32_t>(node->length());

  // C-3: msg_id 回绕后跳过仍在途的
  uint16_t msg_id = next_msg_id_;
  while (pending_big_msg_.count(msg_id)) {
    ++msg_id;
  }
  next_msg_id_ = msg_id + 1;

  uint64_t remote_va = reinterpret_cast<uint64_t>(node->data());

  PendingBigMsg p;
  p.iobuf = std::shared_ptr<IOBuf>(std::move(node));
  p.post_time = std::chrono::steady_clock::now();
  p.retries = 0;
  p.is_free = false;
  p.cb = cb;
  p.group = group;
  pending_big_msg_.emplace(msg_id, std::move(p));

//              << " remote_va=" << remote_va
//              << " inflight=" << pending_big_msg_.size();

  // 控制消息(PULL_MEM)走 SEND_IMM → onControlMsgImm,不经过 onIoMessageReceived seq 校验。
  // 不递增 send_seq_,避免大包场景下 expected_seq_ 与 send_seq_ 不同步。
  if (!postControlPacket(msg_id, 0, remote_va, data_size)) {
    pending_big_msg_.erase(msg_id);
    return false;
  }
  return true;
}

bool UrmaAsyncSocket::postControlPacket(uint16_t msg_id, uint16_t seq,
                                        uint64_t remote_va,
                                        uint32_t data_size) noexcept {
  if (!res_.remote_jetty) return false;

  // 从 JFS 池获取 IO JFS
  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) return false;
  uint32_t jfs_index = 0;
  SharedJfs* sj = acquirePinnedJfs(pool, &jfs_index);
  if (!sj) {
    XLOG(WARN) << "[URMA] postControlPacket: no available JFS";
    return false;
  }
  markActiveJfs(jfs_index);

  // PULL_MEM 控制包:SEND_IMM + PullMemControlPacket(32B) + imm_data(remote_va)
  // SGE[0] = PullMemControlPacket(24B header + 4B data_size + 4B padding)
  // imm_data = remote_va(随 CQE 通告给接收方)
  // inline_flag=1:32B ≤ max_inline_size,NIC 内联拷贝,无需 MR 注册
  auto ctrl_buf = IOBuf::create(sizeof(PullMemControlPacket));
  ctrl_buf->append(sizeof(PullMemControlPacket));
  auto* pkt = reinterpret_cast<PullMemControlPacket*>(ctrl_buf->writableData());
  writeHeader(&pkt->header, res_.peer_socket_id, seq, MSG_PULL_MEM, msg_id);
  pkt->data_size = data_size;
  pkt->padding = 0;

  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(1);
  (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(ctrl_buf->writableData());
  (*sge_arr)[0].len = sizeof(PullMemControlPacket);
  (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();

  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->type = TxCompletionCtx::MSG_CTRL_PULL;
  tx_ctx->socket_id = socket_id_.toUint64();
  tx_ctx->seq = seq;
  tx_ctx->jfs_index = jfs_index;
  tx_ctx->msg_id = msg_id;
  tx_ctx->sge_arr = sge_arr;
  tx_ctx->send_data = std::move(*ctrl_buf);  // 持有 buffer 到 TX CQE

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_SEND_IMM;  // SEND_IMM:imm_data 携带 remote_va
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.flag.bs.inline_flag = 1;  // NIC 内联拷贝 32B,不 DMA
  wr.send.src.sge = sge_arr->data();
  wr.send.src.num_sge = 1;
  wr.send.imm_data = remote_va;  // 随 CQE 通告给接收方
  wr.user_ctx = reinterpret_cast<uint64_t>(tx_ctx);
  wr.next = nullptr;

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t st = urma_post_jfs_wr(sj->jfs, &wr, &bad_wr);
  if (st != URMA_SUCCESS) {
    pool->releaseIoJfs(jfs_index);
    delete tx_ctx;
    XLOG(ERR) << "[URMA] ctrlPullPost FAIL msg_id=" << msg_id
              << " size=" << data_size << " st=" << static_cast<int>(st);
    return false;
  }
  return true;
}

// ── 单边 READ:接收方 ──

void UrmaAsyncSocket::onControlMsgImm(uint8_t type, uint16_t msg_id,
                                       uint64_t remote_va,
                                       uint32_t data_size) noexcept {
  XLOG(DBG) << "[URMA] onControlMsgImm type=" << static_cast<int>(type)
             << " msg_id=" << msg_id
             << " data_size=" << data_size
             << " has_pending_read=" << has_pending_read_;

  if (type == MSG_FREE) {
    // 发送方收到 FREE:释放大 buffer。writeSuccess 只在组内全部大包 FREE 后回调一次
    auto it = pending_big_msg_.find(msg_id);
    if (it != pending_big_msg_.end() && !it->second.is_free) {
      auto group = it->second.group;
      WriteCallback* cb = it->second.cb;
      pending_big_msg_.erase(it);
      if (group) {
        if (!group->done && --group->remaining == 0) {
          group->done = true;
          if (cb) cb->writeSuccess();
        }
      } else if (cb) {
        cb->writeSuccess();
      }
    }
    return;
  }

  if (type != MSG_PULL_MEM) {
    return;
  }

  // PULL_MEM:remote_va 和 data_size 已由调用方从 imm_data 和 recv_buf trailing 解析
  if (has_pending_read_) {
    // 有 READ 在途:控制包进保序队列(等当前 READ 完成)
    XLOG(DBG) << "[URMA] PULL_MEM queued: msg_id=" << msg_id
               << " has_pending_read=true pending_read_msg_id=" << pending_read_msg_id_
               << " qsize=" << rx_pending_queue_.size();
    RxBufItem item;
    item.is_control = true;
    item.msg_id = msg_id;
    item.remote_va = remote_va;
    item.data_size = data_size;
    rx_pending_queue_.push(std::move(item));
    return;
  }

  XLOG(DBG) << "[URMA] PULL_MEM startRead: msg_id=" << msg_id
             << " data_size=" << data_size;
  has_pending_read_ = startRead(msg_id, remote_va, data_size);
  if (!has_pending_read_) {
    XLOG(ERR) << "[URMA] PULL_MEM startRead FAILED: msg_id=" << msg_id;
  }
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

  // Phase 2: 从 JFS 池获取 IO JFS
  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) {
    XLOG(ERR) << "startRead: JFS pool not available";
    return false;
  }
  uint32_t jfs_index = 0;
  SharedJfs* sj = acquirePinnedJfs(pool, &jfs_index);
  if (!sj) {
    XLOG(WARN) << "startRead: no available JFS";
    return false;
  }
  markActiveJfs(jfs_index);

  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(2);
  (*sge_arr)[0].addr = remote_va;
  (*sge_arr)[0].len = data_size;
  (*sge_arr)[0].tseg = res_.remote_seg;
  (*sge_arr)[1].addr = reinterpret_cast<uint64_t>(iobuf->writableData());
  (*sge_arr)[1].len = data_size;
  (*sge_arr)[1].tseg = UrmaInit::getPoolTseg();

  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->type = TxCompletionCtx::MSG_READ;
  tx_ctx->socket_id = socket_id_.toUint64();  // Phase 5: 跨连接 pollTx 路由用
  tx_ctx->jfs_index = jfs_index;
  tx_ctx->msg_id = msg_id;
  tx_ctx->sge_arr = sge_arr;
  // READ 的 dst buffer 通过 pending_read_buf_ 持有,不放到 send_data
  // tx_ctx->send_data 保持空

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_READ;
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.rw.src.sge = &(*sge_arr)[0];
  wr.rw.src.num_sge = 1;
  wr.rw.dst.sge = &(*sge_arr)[1];
  wr.rw.dst.num_sge = 1;
  wr.user_ctx = reinterpret_cast<uint64_t>(tx_ctx);
  wr.next = nullptr;

  pending_read_msg_id_ = msg_id;
  pending_read_buf_ = std::move(iobuf);
  pending_read_sge_ = sge_arr;

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t st = urma_post_jfs_wr(sj->jfs, &wr, &bad_wr);
  if (st != URMA_SUCCESS) {
    pool->releaseIoJfs(jfs_index);
    delete tx_ctx;
    pending_read_sge_.reset();
    pending_read_buf_.reset();
    XLOG(ERR) << "startRead: post READ failed, status=" << st << " msg_id=" << msg_id;
    return false;
  }
  XLOG(DBG) << "[URMA] startRead posted msg_id=" << msg_id
             << " jfs_idx=" << jfs_index << " size=" << data_size;
  // 调度 pollTx 确保 READ CQE 被收割。
  // startRead 可能从 drainRxPendingQueue 调用(不在 writeChain/onIoMessageReceived 上下文中),
  // 此时没有后续的 pollTx 调用,READ CQE 会饿死。
  if (evb_) {
    evb_->runInLoop([this] { pollTx(); });
  }
  return true;
}

void UrmaAsyncSocket::onReadComplete() noexcept {
  if (!pending_read_buf_) {
    XLOG(WARN) << "[URMA] readDone: pending_read_buf_ already reset, skip";
    return;
  }
  XLOG(DBG) << "[URMA] onReadComplete msg_id=" << pending_read_msg_id_
             << " len=" << pending_read_buf_->length();
  auto data = std::move(pending_read_buf_);
  pending_read_sge_.reset();
  has_pending_read_ = false;
  bytes_received_ += data->length();

  read_queue_.append(std::move(data));
  if (read_cb_) {
    deliverReadData();
  }

  // 连接已关闭时不 post FREE(remote_jetty 可能已 null)
  if (state_ != State::kOpen) {
    return;
  }

  // FREE 通知走 SEND_IMM → onControlMsgImm,不经过 seq 校验,不递增 send_seq_
  if (!postFreeNotify(pending_read_msg_id_, 0)) {
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

void UrmaAsyncSocket::handleRemoteReadCqe(uint16_t msg_id,
                                          urma_status_t status) noexcept {
  if (msg_id != pending_read_msg_id_) {
    XLOG(DBG) << "[URMA] handleRemoteReadCqe MISMATCH: msg_id=" << msg_id
              << " expected=" << pending_read_msg_id_ << " status=" << status;
    return;  // 不是当前等待的 READ,忽略(重复或过时 CQE)
  }
  // 同步处理:与 pollTx 中本连接 READ CQE 处理保持一致。
  // 之前用 runInLoop 异步调度,高 QPS 时 EventBase 事件队列积压导致
  // onReadComplete 延迟执行,串行 READ 累积延迟超过 RPC timeout。
  // 串行化保证同时只有 1 个 READ 在途,同步调用不会递归超过 1 层。
  if (status == URMA_CR_SUCCESS) {
    onReadComplete();
  } else {
    onReadFailed(status);
  }
}

bool UrmaAsyncSocket::postFreeNotify(uint16_t msg_id, uint16_t seq) noexcept {
  if (!res_.remote_jetty) return false;

  // 从 JFS 池获取 IO JFS
  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) return false;
  uint32_t jfs_index = 0;
  SharedJfs* sj = acquirePinnedJfs(pool, &jfs_index);
  if (!sj) return false;
  markActiveJfs(jfs_index);

  // FREE 通知:SEND_IMM + UrmaMsgHeader(24B) + imm_data(msg_id)
  // inline_flag=1:24B ≤ max_inline_size,NIC 内联拷贝,无需 MR 注册
  auto hdr_buf = IOBuf::create(sizeof(UrmaMsgHeader));
  hdr_buf->append(sizeof(UrmaMsgHeader));
  writeHeader(hdr_buf.get(), res_.peer_socket_id, seq, MSG_FREE, msg_id);

  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(1);
  (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(hdr_buf->writableData());
  (*sge_arr)[0].len = sizeof(UrmaMsgHeader);
  (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();

  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->type = TxCompletionCtx::MSG_CTRL_FREE;
  tx_ctx->socket_id = socket_id_.toUint64();
  tx_ctx->seq = seq;
  tx_ctx->jfs_index = jfs_index;
  tx_ctx->msg_id = msg_id;
  tx_ctx->sge_arr = sge_arr;
  tx_ctx->send_data = std::move(*hdr_buf);  // 持有 buffer 到 TX CQE

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_SEND_IMM;  // SEND_IMM:imm_data 携带 msg_id
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.flag.bs.inline_flag = 1;  // NIC 内联拷贝 24B,不 DMA
  wr.send.src.sge = sge_arr->data();
  wr.send.src.num_sge = 1;
  wr.send.imm_data = static_cast<uint64_t>(msg_id);  // 低 16 位有效
  wr.user_ctx = reinterpret_cast<uint64_t>(tx_ctx);
  wr.next = nullptr;

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t st = urma_post_jfs_wr(sj->jfs, &wr, &bad_wr);
  if (st != URMA_SUCCESS) {
    pool->releaseIoJfs(jfs_index);
    delete tx_ctx;
    return false;
  }
  return true;
}

void UrmaAsyncSocket::drainRxPendingQueue() noexcept {
  while (!rx_pending_queue_.empty()) {
    RxBufItem item = std::move(rx_pending_queue_.front());
    rx_pending_queue_.pop();
    if (item.is_control) {
      // 串行保序:遇到控制包,发起新 READ 后停止取出
//                  << " size=" << item.data_size << " qsize=" << rx_pending_queue_.size();
      has_pending_read_ = startRead(item.msg_id, item.remote_va, item.data_size);
      if (!has_pending_read_) {
        XLOG(ERR) << "[URMA] drainCtrl startRead failed msg_id=" << item.msg_id;
      }
      return;
    }
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
      auto group = p.group;
      WriteCallback* cb = p.cb;
      it = pending_big_msg_.erase(it);
      // B-3: 组内任一重发耗尽,整个 writeChain 只回调一次 writeErr
      if (group) {
        if (!group->done) {
          group->done = true;
          if (cb) {
            cb->writeErr(0, AsyncSocketException(
                AsyncSocketException::UNKNOWN, "big msg timeout after retries"));
          }
        }
      } else if (cb) {
        cb->writeErr(0, AsyncSocketException(
            AsyncSocketException::UNKNOWN, "big msg timeout after retries"));
      }
      continue;
    }

    if (p.is_free) {
      // FREE 重发(发送方未确认释放)
      // FREE 重发:控制消息不递增 send_seq_
      if (postFreeNotify(it->first, 0)) {
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
      // PULL_MEM 重发:控制消息不递增 send_seq_
      if (postControlPacket(static_cast<uint16_t>(it->first), 0,
                            remote_va, data_size)) {
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

// ── Phase 3: 独立 buffer 路由 ──
// 从 recv buffer 前 24B 解析 UrmaMsgHeader,仅处理 MSG_DATA
// MSG_PULL_MEM / MSG_FREE 走 SEND_IMM 路径,由 UrmaJfrPool::harvestAndDispatch
// 解析 imm_data + PullMemControlPacket 后直接调 onControlMsgImm,不进入此方法
// recv_buf: 接收到的 buffer 起始(header 24B + data)
// recv_len: 实际接收字节数
// slot:     rx_bufs_ 的 slot 索引(补投 recv WR 用)
void UrmaAsyncSocket::onIoMessageReceived(void* recv_buf, uint32_t recv_len,
                                           uint32_t slot) noexcept {
  if (!recv_buf || recv_len < sizeof(UrmaMsgHeader)) {
    return;
  }

  auto* hdr = parseHeader(recv_buf);
  if (!hdr) {
    XLOG(WARN) << "[URMA] onIoMessageReceived: magic mismatch, recv_len="
               << recv_len;
    return;
  }

  // version 校验(不匹配仅 WARN,不丢弃,向前兼容)
  if (hdr->version != URMA_VERSION) {
    XLOG(WARN) << "[URMA] onIoMessageReceived: version mismatch, got="
               << static_cast<int>(hdr->version)
               << " expected=" << static_cast<int>(URMA_VERSION)
               << " type=" << static_cast<int>(hdr->type);
  }

  // seq 保序校验(可选,不一致仅 WARN 不丢弃)
  uint16_t expected = expected_seq_.load(std::memory_order_relaxed);
  if (hdr->seq != expected) {
    XLOG(WARN) << "[URMA] seq mismatch: expected=" << expected
               << " got=" << hdr->seq
               << " type=" << static_cast<int>(hdr->type);
  }
  expected_seq_.store(static_cast<uint16_t>(hdr->seq + 1),
                      std::memory_order_relaxed);

  if (hdr->type == MSG_DATA) {
    // 普通 SEND 数据:跳过 24B header,取 data 部分交付
    uint32_t data_len = recv_len - sizeof(UrmaMsgHeader);
    void* data_ptr = static_cast<uint8_t*>(recv_buf) + sizeof(UrmaMsgHeader);
    auto iobuf = IOBuf::wrapBuffer(data_ptr, data_len);
    onRxBuf(std::move(iobuf));
  } else {
    // MSG_PULL_MEM / MSG_FREE 不应出现在 SEND 路径(应走 SEND_IMM)
    XLOG(WARN) << "[URMA] onIoMessageReceived: unexpected type="
               << static_cast<int>(hdr->type)
               << " in SEND path (expected SEND_IMM), msg_id=" << hdr->msg_id;
  }

  // Phase 5: RX 处理后异步调度 pollTx,收割可能在途的 READ/SEND TX CQE。
  // 大包流程中 startRead post 的 READ WR,其 CQE 需要在 pollTx 中收割。
  // 用 runInLoop 异步调度,避免在 harvestAndDispatch 调用栈中同步 pollTx
  // 导致递归(pollTx → onReadComplete → deliverReadData → write → pollTx)。
  if (evb_) {
    evb_->runInLoop([this] { pollTx(); });
  }
}

// ── Phase 6: FC 消息接收 ──
//
// FC 消息路由通路:UrmaJfrPool::harvestFcAndDispatch → fc_socket_map → 此方法
// payload 前 8B = 发送方 SocketId,后续 8B = FcHeader(msg_type/credit_cnt/fc_seq)
// 按 msg_type 分发到 handleFcReq/handleFcRep/handleFcReturn/handleFcAck

void UrmaAsyncSocket::onFcMessageReceived(void* recv_buf,
                                            uint32_t recv_len) noexcept {
  if (!recv_buf || recv_len < FC_MSG_LEN) {
    XLOG(WARN) << "[URMA-FC] recv too short: recv_len=" << recv_len;
    return;
  }

  uint64_t dst_socket_id = 0;
  UrmaFcMsgType type;
  uint16_t credit_cnt = 0;
  uint16_t fc_seq = 0;
  if (!deserializeFcMsg(recv_buf, dst_socket_id, type, credit_cnt, fc_seq)) {
    return;
  }

  XLOG(DBG) << "[URMA-FC] onFcMessageReceived: type=" << static_cast<int>(type)
            << " credit_cnt=" << credit_cnt << " fc_seq=" << fc_seq
            << " dst_socket_id=0x" << std::hex << dst_socket_id << std::dec;

  switch (type) {
    case FC_REQ:
      handleFcReq(dst_socket_id, credit_cnt, fc_seq);
      break;
    case FC_REP:
      handleFcRep(credit_cnt, fc_seq);
      break;
    case FC_RETURN:
      handleFcReturn(dst_socket_id, credit_cnt, fc_seq);
      break;
    case FC_ACK:
      handleFcAck(credit_cnt, fc_seq);
      break;
    default:
      XLOG(WARN) << "[URMA-FC] unknown msg_type=" << static_cast<int>(type);
      break;
  }
}

// ── Phase 6: FC REQ 处理(接收方) ──
// 对端申请 credit,从 credit_idle 扣减后回复 REP 授予
// 去重:fc_seq 与 fc_last_req_seq 相同则跳过(防止 REQ 重发重复授信)

void UrmaAsyncSocket::handleFcReq(uint64_t dst_socket_id, uint16_t credit_cnt,
                                    uint16_t fc_seq) noexcept {
  // 去重:同一 REQ 序列号只处理一次
  uint16_t last_seq = fc_state_.fc_last_req_seq.load(std::memory_order_acquire);
  if (fc_seq == last_seq && last_seq != 0) {
    XLOG(DBG) << "[URMA-FC] dup REQ fc_seq=" << fc_seq << ", skip";
    return;
  }
  fc_state_.fc_last_req_seq.store(fc_seq, std::memory_order_release);

  // 从 per-JFR credit_idle 扣减(通过 UrmaJfrPool)
  // 授予数量 = min(请求量, credit_idle)
  uint32_t granted = 0;
  if (auto* pool = UrmaJfrPool::get()) {
    if (auto* jfr = pool->getIoJfrByIndex(0)) {  // 简化:用第一个 IO JFR
      uint32_t idle = jfr->credit_idle.load(std::memory_order_acquire);
      uint32_t to_grant = std::min(static_cast<uint32_t>(credit_cnt), idle);
      if (to_grant > 0) {
        // 原子扣减 credit_idle
        uint32_t cur = idle;
        while (cur >= to_grant) {
          if (jfr->credit_idle.compare_exchange_weak(
                  cur, cur - to_grant,
                  std::memory_order_acq_rel, std::memory_order_acquire)) {
            granted = to_grant;
            break;
          }
        }
      }
      // credit_allocated += granted
      if (granted > 0) {
        jfr->credit_allocated.fetch_add(granted, std::memory_order_release);
      }
    }
  }

  fc_state_.peer_granted.fetch_add(granted, std::memory_order_acq_rel);

  XLOG(DBG) << "[URMA-FC] REQ: requested=" << credit_cnt
            << " granted=" << granted;

  // 回复 REP(授予 credit)
  if (granted > 0) {
    uint16_t rep_seq = fc_state_.fc_seq_next.fetch_add(1, std::memory_order_relaxed);
    sendFcMsg(FC_REP, static_cast<uint16_t>(granted), rep_seq);
  }
}

// ── Phase 6: FC REP 处理(发送方) ──
// 对端授予 credit,增加 peer_credit,唤醒 pending_write_

void UrmaAsyncSocket::handleFcRep(uint16_t credit_cnt, uint16_t fc_seq) noexcept {
  fc_state_.peer_credit.fetch_add(credit_cnt, std::memory_order_acq_rel);

  XLOG(DBG) << "[URMA-FC] REP received: credit_cnt=" << credit_cnt
            << " fc_seq=" << fc_seq
            << " peer_credit=" << fc_state_.peer_credit.load()
            << " socket_id=0x" << std::hex << socket_id_.toUint64() << std::dec;

  // Phase 7: credit_multiple 自适应
  // credit 充足(> 请求量) → ×2 扩大下次请求,不足 → /2 缩减,范围 [1, 8]
  uint32_t cur_mult = fc_state_.credit_multiple.load(std::memory_order_relaxed);
  if (credit_cnt > 0 && fc_state_.peer_credit.load(std::memory_order_relaxed) > 0) {
    // 充足:扩大
    uint32_t new_mult = std::min(cur_mult * 2, 8u);
    fc_state_.credit_multiple.store(new_mult, std::memory_order_relaxed);
  } else if (credit_cnt == 0) {
    // 不足:缩减
    uint32_t new_mult = std::max(cur_mult / 2, 1u);
    fc_state_.credit_multiple.store(new_mult, std::memory_order_relaxed);
  }

  XLOG(DBG) << "[URMA-FC] REP: granted=" << credit_cnt
            << " peer_credit=" << fc_state_.peer_credit.load()
            << " credit_multiple=" << fc_state_.credit_multiple.load();

  // 唤醒 pending_write_(credit 到位后可继续发送)
  if (evb_) {
    evb_->runInLoop([this] { handlePendingWrites(); });
  }
}

// ── Phase 6: FC RETURN 处理(接收方) ──
// 对端归还 credit,credit_idle += N,credit_allocated -= N

void UrmaAsyncSocket::handleFcReturn(uint64_t dst_socket_id,
                                       uint16_t credit_cnt,
                                       uint16_t fc_seq) noexcept {
  if (auto* pool = UrmaJfrPool::get()) {
    if (auto* jfr = pool->getIoJfrByIndex(0)) {
      jfr->credit_idle.fetch_add(credit_cnt, std::memory_order_release);
      jfr->credit_allocated.fetch_sub(credit_cnt, std::memory_order_acq_rel);
    }
  }
  fc_state_.peer_granted.fetch_sub(credit_cnt, std::memory_order_acq_rel);

  XLOG(DBG) << "[URMA-FC] RETURN: returned=" << credit_cnt;

  // 回复 ACK(确认归还)
  uint16_t ack_seq = fc_state_.fc_seq_next.fetch_add(1, std::memory_order_relaxed);
  sendFcMsg(FC_ACK, credit_cnt, ack_seq);
}

// ── Phase 6: FC ACK 处理(发送方) ──
// 对端确认归还(可选,用于可靠性,当前仅日志)

void UrmaAsyncSocket::handleFcAck(uint16_t credit_cnt,
                                    uint16_t fc_seq) noexcept {
  XLOG(DBG) << "[URMA-FC] ACK: confirmed=" << credit_cnt;
}

// ── Phase 6: FC 消息发送(通过 FC JFS 池) ──
// 序列化 16B FC 消息 → acquireFcJfs → post SEND
// 目标 jetty:res_.remote_fc_jetty(握手时 importPeer 创建)

bool UrmaAsyncSocket::sendFcMsg(UrmaFcMsgType type, uint16_t credit_cnt,
                                  uint16_t fc_seq) noexcept {
  if (state_ != State::kOpen) return false;
  if (!res_.remote_fc_jetty) {
    XLOG(WARN) << "[URMA-FC] sendFcMsg: no remote_fc_jetty";
    return false;
  }

  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) return false;

  uint32_t fc_jfs_index = 0;
  SharedJfs* fc_jfs = pool->acquireFcJfs(&fc_jfs_index);
  if (!fc_jfs) {
    XLOG(DBG) << "[URMA-FC] acquireFcJfs failed (SQ full)";
    return false;
  }

  // Phase 7: 每次分配独立 16B IOBuf 作为 FC buffer,避免复用 fc_send_buf_ 导致 DMA 数据竞争
  // (onHandshakeReady 中连续两次 sendFcMsg 会覆盖同一 buffer,硬件 DMA 异步读取时
  //  第二次序列化覆盖了第一次的内容,对端收到损坏的 FC 消息)
  auto fc_iobuf = IOBuf::create(FC_MSG_LEN);
  fc_iobuf->append(FC_MSG_LEN);
  serializeFcMsg(fc_iobuf->writableData(), res_.peer_socket_id, type, credit_cnt, fc_seq);

  // 构造 SGE + WR
  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(1);
  (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(fc_iobuf->writableData());
  (*sge_arr)[0].len = FC_MSG_LEN;
  (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();

  // per-WR context:send_data move 持有 fc_iobuf,TX CQE 后 delete ctx → send_data 析构 → IOBuf 释放
  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->socket_id = socket_id_.toUint64();
  tx_ctx->seq = fc_seq;
  tx_ctx->type = TxCompletionCtx::MSG_CTRL_PULL;  // 复用控制包类型(FC CQE 回收路径相同)
  tx_ctx->jfs_index = fc_jfs_index;
  tx_ctx->msg_id = 0;
  tx_ctx->sge_arr = sge_arr;
  tx_ctx->send_data = std::move(*fc_iobuf);

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_SEND;
  wr.tjetty = res_.remote_fc_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.send.src.sge = sge_arr->data();
  wr.send.src.num_sge = 1;
  wr.user_ctx = reinterpret_cast<uint64_t>(tx_ctx);
  wr.next = nullptr;

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfs_wr(fc_jfs->jfs, &wr, &bad_wr);
  if (ret != URMA_SUCCESS) {
    XLOG(WARN) << "[URMA-FC] post_jfs_wr failed: " << ret;
    pool->releaseFcJfs(fc_jfs_index);
    delete tx_ctx;
    return false;
  }

  // 武装 FC TX JFCE 中断(SQ 可能满,等待 CQE 唤醒)
  pool->rearmAllFcTxFce();

  return true;
}

// ── 接收(普通 SEND 数据) ──

void UrmaAsyncSocket::onRxBuf(std::unique_ptr<IOBuf> data) noexcept {
  bytes_received_ += data->length();
//              << " pending=" << has_pending_read_;
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
  if (tcp_delegate_) {
    tcp_delegate_->close();
    return;
  }
  shutdownWrite();
  closeNow();
}

void UrmaAsyncSocket::closeNow() {
  if (tcp_delegate_) {
    tcp_delegate_->closeNow();
    return;
  }
  if (state_ == State::kClosed) return;
  state_ = State::kClosed;
  destroyUrmaResources();
}

void UrmaAsyncSocket::shutdownWrite() {
  if (tcp_delegate_) {
    tcp_delegate_->shutdownWrite();
    return;
  }
  // Phase 2: poll JFS 池的 TX CQE 回收资源
  pollTx();
}

void UrmaAsyncSocket::shutdownWriteNow() {
  if (tcp_delegate_) {
    tcp_delegate_->shutdownWriteNow();
    return;
  }
}

void UrmaAsyncSocket::destroyUrmaResources() {
  // Phase 4: 路由表注销直接调 UrmaJfrPool(不依赖 shared_,避免 EventBase 析构后悬空)
  if (res_.remote_jetty) {
    if (auto* pool = UrmaJfrPool::get()) {
      pool->unregisterSocket(socket_id_.toUint64());
    }
  }
  if (res_.remote_fc_jetty) {
    if (auto* pool = UrmaJfrPool::get()) {
      pool->unregisterFcSocket(socket_id_.toUint64());
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
  // Phase 3: import 句柄通过 ImportedJettyCache 引用计数管理
  if (res_.remote_fc_jetty) {
    ImportedJettyCache::get()->release(res_.remote_fc_jetty);
    res_.remote_fc_jetty = nullptr;
  }
  if (res_.remote_jetty) {
    ImportedJettyCache::get()->release(res_.remote_jetty);
    res_.remote_jetty = nullptr;
  }
  if (res_.remote_seg) {
    urma_unimport_seg(res_.remote_seg);
    res_.remote_seg = nullptr;
  }
  // Phase 2: 不再有 sbuf_ 环形数组(TxCompletionCtx 在 pollTx 中 delete)
  ctrl_sge_refs_.clear();
  pending_write_head_.reset();
  pending_write_tail_ = nullptr;
  read_queue_.move();

  // 休眠管理:本连接断开,递减活跃连接计数,归零则休眠 CqHandler
  // (从 epoll 摘除,避免 LT 模式下 JFCE fd 持续 spin 占 CPU)
  if (shared_) {
    shared_->decActiveConn();
  }
}

// ── 其他 ──

void UrmaAsyncSocket::attachEventBase(EventBase* /*eventBase*/) {
}

void UrmaAsyncSocket::detachEventBase() {
}

void UrmaAsyncSocket::setSendTimeout(uint32_t milliseconds) {
  send_timeout_ms_ = milliseconds;
  if (tcp_delegate_) {
    tcp_delegate_->setSendTimeout(milliseconds);
  }
}

void UrmaAsyncSocket::getLocalAddress(SocketAddress* address) const {
  if (tcp_delegate_) {
    tcp_delegate_->getLocalAddress(address);
    return;
  }
}

void UrmaAsyncSocket::getPeerAddress(SocketAddress* address) const {
  if (tcp_delegate_) {
    tcp_delegate_->getPeerAddress(address);
    return;
  }
}

} // namespace folly

#endif // FOLLY_HAVE_URMA