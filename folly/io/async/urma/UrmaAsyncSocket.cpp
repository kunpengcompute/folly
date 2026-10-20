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
#include <folly/io/async/urma/UrmaJfsPool.h>
#include <folly/io/async/urma/UrmaJfrPool.h>
#include <folly/io/async/urma/UrmaImportCache.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <folly/logging/xlog.h>

#include <unistd.h>

namespace folly {

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
    XLOG(INFO) << "[URMA-DIAG] onHandshakeReady: registerSocket peer_socket_id="
               << res_.peer_socket_id << " peer_io_jetty_id=" << res_.peer_io_jetty_id
               << " this=" << this;
    // Phase 3: 用对端 SocketId 注册(headroom 路由,接收方按 header.src_socket_id 查表)
    shared_->registerSocket(res_.peer_socket_id, this);
  }
  if (res_.remote_fc_jetty) {
    XLOG(INFO) << "[URMA-DIAG] onHandshakeReady: registerFcSocket peer_socket_id="
               << res_.peer_socket_id << " peer_fc_jetty_id=" << res_.peer_fc_jetty_id
               << " this=" << this;
    shared_->registerFcSocket(res_.peer_socket_id, this);
  }

  delete handshake_;
  handshake_ = nullptr;

  state_ = State::kOpen;


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

  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "JFS pool not initialized"));
    return URMA_FAIL;
  }

  // Phase 6: inline 路径预留(设计 §4.5 inline 快速路径)
  // 当 payload ≤ max_inline_len - 10(header 开销)时,走 inline SEND(零拷贝,无需 SGE)
  // 当前硬件 max_inline_len=8,条件不满足,自动走 headroom 路径
  // 未来硬件支持更大 inline 时,此条件自动启用
  uint32_t max_inline_len = UrmaInit::getMaxJfsInlineLen();
  if (chain && !chain->isChained() &&  // 单 IOBuf block
      max_inline_len > 10 + sizeof(UrmaMsgHeader) &&
      chain->length() <= max_inline_len - 10) {
    // inline 快速路径(当前不可达,预留)
    // 未来实现:post inline SEND,无需 acquireIoJfs/SGE
    // 暂走 headroom 路径
  }

  while (chain) {
    uint32_t sge_total = 0;
    int num_sge = 0;

    uint8_t hw_max_sge = UrmaInit::getMaxJfsSge();
    size_t chain_remaining = chain->countChainElements();
    // Phase 3: 预留 1 个 SGE 给 header(路径 B:headroom 不足时插入独立 header IOBuf)
    // 路径 A 复用 headroom 时不会额外占用 SGE,但预留一位保证路径 B 不越界
    uint8_t max_data_sge = static_cast<uint8_t>(
        std::min(chain_remaining, static_cast<size_t>(hw_max_sge - 1)));
    uint8_t sge_capacity = static_cast<uint8_t>(max_data_sge + 1);

    auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(sge_capacity);
    // 持有本次 WR 引用的 IOBuf chain(防止 post 期间被释放)
    auto wr_chain_head = std::make_shared<IOBuf>();

    // Phase 3: 记录第一个 IOBuf 指针(用于后续 prepend 写 header)
    IOBuf* first_buf = nullptr;
    uint32_t first_data_len = 0;

    while (chain && num_sge < max_data_sge) {
      size_t iobuf_len = chain->length();
      // Phase 3: headroom 路径下总长 = header(64B) + sge_total,
      // 不能超过 peer_rx_buf_size_(recv buffer 大小)
      if (sge_total + iobuf_len + sizeof(UrmaMsgHeader) > peer_rx_buf_size_ &&
          num_sge > 0) {
        break;
      }
      // Phase 3: headroom 路径下 SGE[0] = header(64B) + data,
      // data 不能超过 peer_rx_buf_size_ - sizeof(UrmaMsgHeader),
      // 否则接收端 recv buffer(4096B) 装不下 header+data,header 被截断。
      // 大包判断条件:data_len + 64 > peer_rx_buf_size_ → 走单边 READ
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
      // Phase 3: 先 clone(保留 headroom 引用),把 clone 加入发送链
      // 关键:必须保留 clone 的裸指针,因为 chain->pop() 会销毁原 head IOBuf
      // 若保存 chain.get() 作 first_buf,后续 prepend 会访问悬空指针
      std::unique_ptr<IOBuf> cloned = chain->cloneOne();
      (*sge_arr)[num_sge].addr = reinterpret_cast<uint64_t>(cloned->writableData());
      (*sge_arr)[num_sge].len = static_cast<uint32_t>(iobuf_len);
      (*sge_arr)[num_sge].tseg = UrmaInit::getPoolTseg();
      // Phase 3: 记录第一个 clone block(用于 headroom header 写入)
      if (num_sge == 0) {
        first_buf = cloned.get();
        first_data_len = static_cast<uint32_t>(iobuf_len);
      }
      wr_chain_head->appendChain(std::move(cloned));
      sge_total += iobuf_len;
      num_sge++;
      chain = chain->pop();
    }

    if (num_sge == 0) break;

    // Phase 3: headroom 路由 — 在第一个 IOBuf block 的 headroom 写 UrmaMsgHeader
    // 优先复用 first_buf 的 headroom(内存池 IOBuf 有 64B headroom);
    // headroom 不足时(rocket transport 默认 IOBuf 只有 ~16B headroom)
    // 在 chain 前面插入独立 64B header IOBuf 作为 SGE[0],原 SGE 顺延
    uint16_t seq = send_seq_.fetch_add(1, std::memory_order_relaxed);
    if (first_buf && first_buf->headroom() >= sizeof(UrmaMsgHeader)) {
      // 路径 A:复用 headroom,SGE[0] 覆盖 header+data(单 SGE 合并)
      first_buf->prepend(sizeof(UrmaMsgHeader));
      auto* hdr = reinterpret_cast<UrmaMsgHeader*>(first_buf->writableData());
      hdr->magic = URMA_MAGIC;
      hdr->src_socket_id = socket_id_.toUint64();
      hdr->seq = seq;
      hdr->type = MSG_DATA;
      hdr->msg_count = static_cast<uint8_t>(num_sge);
      hdr->data_size = sge_total;
      hdr->data_sge_count = static_cast<uint32_t>(num_sge);
      std::memset(hdr->sub_len, 0, sizeof(hdr->sub_len));
      hdr->sub_len[0] = static_cast<uint16_t>(first_data_len);
      hdr->reserved[0] = 0;
      hdr->reserved[1] = 0;
      // SGE[0] 扩展覆盖 header+data
      (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(first_buf->writableData());
      (*sge_arr)[0].len = sizeof(UrmaMsgHeader) + first_data_len;
      sge_total += sizeof(UrmaMsgHeader);
    } else {
      // 路径 B:headroom 不足,插入独立 64B header IOBuf 作为 SGE[0]
      // 原 SGE[0..n-1] 顺延为 SGE[1..n],需检查 max_sge 上限
      if (num_sge + 1 > hw_max_sge) {
        // 超限:无法插入 header,报错(理论上 max_sge=13 不会触发)
        if (cb) cb->writeErr(total_posted, AsyncSocketException(
            AsyncSocketException::UNKNOWN, "SGE overflow with header"));
        if (group) group->done = true;
        return URMA_FAIL;
      }
      auto hdr_buf = IOBuf::create(sizeof(UrmaMsgHeader));
      hdr_buf->append(sizeof(UrmaMsgHeader));
      auto* hdr = reinterpret_cast<UrmaMsgHeader*>(hdr_buf->writableData());
      hdr->magic = URMA_MAGIC;
      hdr->src_socket_id = socket_id_.toUint64();
      hdr->seq = seq;
      hdr->type = MSG_DATA;
      hdr->msg_count = static_cast<uint8_t>(num_sge);
      hdr->data_size = sge_total;
      hdr->data_sge_count = static_cast<uint32_t>(num_sge);
      std::memset(hdr->sub_len, 0, sizeof(hdr->sub_len));
      hdr->sub_len[0] = static_cast<uint16_t>(first_data_len);
      hdr->reserved[0] = 0;
      hdr->reserved[1] = 0;
      // SGE 数组后移一位,腾出 SGE[0] 给 header
      for (int k = num_sge; k > 0; --k) {
        (*sge_arr)[k] = (*sge_arr)[k - 1];
      }
      (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(hdr_buf->writableData());
      (*sge_arr)[0].len = sizeof(UrmaMsgHeader);
      (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();
      num_sge += 1;
      sge_total += sizeof(UrmaMsgHeader);
      // hdr_buf 加入发送链,保持存活到 TX CQE
      wr_chain_head->appendChain(std::move(hdr_buf));
    }

    // Phase 2: 从 JFS 池获取一个空闲 JFS
    uint32_t jfs_index = 0;
    SharedJfs* sj = pool->acquireIoJfs(&jfs_index);
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

  if (!postControlPacket(msg_id, remote_va, data_size)) {
    pending_big_msg_.erase(msg_id);
    return false;
  }
  return true;
}

bool UrmaAsyncSocket::postControlPacket(uint16_t msg_id, uint64_t remote_va,
                                        uint32_t data_size) noexcept {
  if (!res_.remote_jetty) return false;

  // Phase 2: 从 JFS 池获取 IO JFS
  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) return false;
  uint32_t jfs_index = 0;
  SharedJfs* sj = pool->acquireIoJfs(&jfs_index);
  if (!sj) {
    XLOG(WARN) << "[URMA] postControlPacket: no available JFS";
    return false;
  }
  markActiveJfs(jfs_index);

  // Phase 5: 控制包改用 headroom 小 SEND(替代 SEND_IMM)
  // 构造 64B header(MSG_PULL_MEM) + 8B remote_va data
  // header.seq = msg_id(接收侧 onIoMessageReceived 用 seq 作为 msg_id 匹配 READ/FREE)
  auto hdr_buf = IOBuf::create(sizeof(UrmaMsgHeader) + sizeof(uint64_t));
  hdr_buf->append(sizeof(UrmaMsgHeader) + sizeof(uint64_t));
  auto* hdr = reinterpret_cast<UrmaMsgHeader*>(hdr_buf->writableData());
  hdr->magic = URMA_MAGIC;
  hdr->src_socket_id = socket_id_.toUint64();
  hdr->seq = msg_id;  // 控制包用 msg_id 作为 seq(接收侧据此匹配)
  hdr->type = MSG_PULL_MEM;
  hdr->msg_count = 1;
  hdr->data_size = data_size;
  hdr->data_sge_count = 0;
  std::memset(hdr->sub_len, 0, sizeof(hdr->sub_len));
  std::memset(hdr->reserved, 0, sizeof(hdr->reserved));
  // data 部分:8 字节 remote_va
  std::memcpy(hdr_buf->writableData() + sizeof(UrmaMsgHeader),
              &remote_va, sizeof(remote_va));

  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(1);
  (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(hdr_buf->writableData());
  (*sge_arr)[0].len = sizeof(UrmaMsgHeader) + sizeof(uint64_t);
  (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();

  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->type = TxCompletionCtx::MSG_CTRL_PULL;
  tx_ctx->socket_id = socket_id_.toUint64();
  tx_ctx->jfs_index = jfs_index;
  tx_ctx->msg_id = msg_id;
  tx_ctx->sge_arr = sge_arr;
  tx_ctx->send_data = std::move(*hdr_buf);  // 持有 buffer 到 TX CQE

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_SEND;
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.send.src.sge = sge_arr->data();
  wr.send.src.num_sge = 1;
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

void UrmaAsyncSocket::onControlMsg(uint64_t imm, const void* meta) noexcept {
  uint16_t msg_id;
  uint8_t is_ctl;
  uint32_t data_size;
  decodeBigMsgImm(imm, msg_id, is_ctl, data_size);
  XLOG(DBG) << "[URMA-DIAG] onControlMsg msg_id=" << msg_id
             << " is_ctl=" << static_cast<int>(is_ctl)
             << " data_size=" << data_size
             << " has_pending_read=" << has_pending_read_;

  if (is_ctl == kCtrlTypeFree) {
    // 发送方收到 FREE:释放大 buffer。writeSuccess 只在组内全部大包 FREE 后回调一次
    auto it = pending_big_msg_.find(msg_id);
//                << " found=" << (it != pending_big_msg_.end())
//                << " inflight=" << pending_big_msg_.size();
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

  if (is_ctl != kCtrlTypePull) {
    return;
  }

  // PULL:解码元数据(8 字节 remote_va)
  uint64_t remote_va = 0;
  if (meta) {
    std::memcpy(&remote_va, meta, sizeof(remote_va));
  }
//              << " remote_va=" << remote_va << " pending=" << has_pending_read_
//              << " is_ctl=" << static_cast<int>(is_ctl);

  if (has_pending_read_) {
    // 有 READ 在途:控制包进保序队列(等当前 READ 完成)
    RxBufItem item;
    item.is_control = true;
    item.msg_id = msg_id;
    item.remote_va = remote_va;
    item.data_size = data_size;
    rx_pending_queue_.push(std::move(item));
//                << " size=" << data_size << " qsize=" << rx_pending_queue_.size();
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

  // Phase 2: 从 JFS 池获取 IO JFS
  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) {
    XLOG(ERR) << "startRead: JFS pool not available";
    return false;
  }
  uint32_t jfs_index = 0;
  SharedJfs* sj = pool->acquireIoJfs(&jfs_index);
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
  XLOG(DBG) << "[URMA-DIAG] startRead posted msg_id=" << msg_id
             << " jfs_idx=" << jfs_index << " remote_va=0x" << std::hex
             << remote_va << std::dec << " size=" << data_size;
  return true;
}

void UrmaAsyncSocket::onReadComplete() noexcept {
  if (!pending_read_buf_) {
    XLOG(WARN) << "[URMA] readDone: pending_read_buf_ already reset, skip";
    return;
  }
  XLOG(DBG) << "[URMA-DIAG] onReadComplete msg_id=" << pending_read_msg_id_
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

void UrmaAsyncSocket::handleRemoteReadCqe(uint16_t msg_id,
                                          urma_status_t status) noexcept {
  // Phase 5: 处理其他连接 pollTx/harvestTxAndDispatch 收割到的本连接 READ CQE。
  // JFS 池化后多连接共享 JFS,READ CQE 可能被其他连接的 poll 收割。
  // 通过 tx_ctx->socket_id 路由到本连接调用此方法。
  //
  // 用 runInLoop 异步执行,避免在 harvestTxAndDispatch 调用栈中同步处理
  // 导致递归(onReadComplete → deliverReadData → write → sendChain → pollTx)。
  if (msg_id != pending_read_msg_id_) {
    return;  // 不是当前等待的 READ,忽略(重复或过时 CQE)
  }
  if (!evb_) {
    // 无 EventBase(不应发生),直接处理
    if (status == URMA_CR_SUCCESS) {
      onReadComplete();
    } else {
      onReadFailed(status);
    }
    return;
  }
  evb_->runInLoop([this, status] {
    if (status == URMA_CR_SUCCESS) {
      onReadComplete();
    } else {
      onReadFailed(status);
    }
  });
}

bool UrmaAsyncSocket::postFreeNotify(uint16_t msg_id) noexcept {
  if (!res_.remote_jetty) return false;

  // Phase 2: 从 JFS 池获取 IO JFS
  UrmaJfsPool* pool = UrmaJfsPool::get();
  if (!pool) return false;
  uint32_t jfs_index = 0;
  SharedJfs* sj = pool->acquireIoJfs(&jfs_index);
  if (!sj) return false;
  markActiveJfs(jfs_index);

  // Phase 5: FREE 通知改用 headroom 小 SEND(替代 SEND_IMM)
  // 构造 64B header(MSG_FREE),无 data
  // header.seq = msg_id(复用对应 PULL_MEM 的 msg_id,发送方据此匹配释放)
  auto hdr_buf = IOBuf::create(sizeof(UrmaMsgHeader));
  hdr_buf->append(sizeof(UrmaMsgHeader));
  auto* hdr = reinterpret_cast<UrmaMsgHeader*>(hdr_buf->writableData());
  hdr->magic = URMA_MAGIC;
  hdr->src_socket_id = socket_id_.toUint64();
  hdr->seq = msg_id;  // 复用 PULL_MEM 的 msg_id
  hdr->type = MSG_FREE;
  hdr->msg_count = 1;
  hdr->data_size = 0;
  hdr->data_sge_count = 0;
  std::memset(hdr->sub_len, 0, sizeof(hdr->sub_len));
  std::memset(hdr->reserved, 0, sizeof(hdr->reserved));

  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(1);
  (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(hdr_buf->writableData());
  (*sge_arr)[0].len = sizeof(UrmaMsgHeader);
  (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();

  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->type = TxCompletionCtx::MSG_CTRL_FREE;
  tx_ctx->socket_id = socket_id_.toUint64();
  tx_ctx->jfs_index = jfs_index;
  tx_ctx->msg_id = msg_id;
  tx_ctx->sge_arr = sge_arr;
  tx_ctx->send_data = std::move(*hdr_buf);  // 持有 buffer 到 TX CQE

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_SEND;
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.send.src.sge = sge_arr->data();
  wr.send.src.num_sge = 1;
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

// ── Phase 3: headroom 路由 ──
// 从 recv buffer 前 64B 解析 UrmaMsgHeader,按 type 分发
// recv_buf: 接收到的 buffer 起始(header 64B + data)
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

  uint32_t data_len = recv_len - sizeof(UrmaMsgHeader);
  void* data_ptr = static_cast<uint8_t*>(recv_buf) + sizeof(UrmaMsgHeader);

  if (hdr->type == MSG_DATA) {
    // 普通 SEND 数据:跳过 64B header,取 data 部分交付
    auto iobuf = IOBuf::wrapBuffer(data_ptr, data_len);
    onRxBuf(std::move(iobuf));
  } else if (hdr->type == MSG_PULL_MEM) {
    // 大包控制包(PULL):meta 在 data 部分(8 字节 remote_va)
    // 复用 onControlMsg,is_ctl=kCtrlTypePull,imm 编码 msg_id|is_ctl|data_size
    // msg_id 从 header seq 字段获取(Phase 5 保序后改用独立 msg_id)
    XLOG(DBG) << "[URMA-DIAG] onIoMessageReceived MSG_PULL_MEM msg_id="
               << hdr->seq << " data_size=" << hdr->data_size
               << " data_len=" << data_len;
    uint64_t imm = encodeBigMsgImm(hdr->seq, kCtrlTypePull,
                                    hdr->data_size);
    onControlMsg(imm, data_ptr);
  } else if (hdr->type == MSG_FREE) {
    // 大包 FREE 通知:无 meta
    XLOG(DBG) << "[URMA-DIAG] onIoMessageReceived MSG_FREE msg_id="
               << hdr->seq;
    uint64_t imm = encodeBigMsgImm(hdr->seq, kCtrlTypeFree, 0);
    onControlMsg(imm, nullptr);
  } else {
    XLOG(WARN) << "[URMA] onIoMessageReceived: unknown type="
               << static_cast<int>(hdr->type);
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

  uint64_t peer_socket_id = 0;
  UrmaFcMsgType type;
  uint16_t credit_cnt = 0;
  uint16_t fc_seq = 0;
  if (!deserializeFcMsg(recv_buf, peer_socket_id, type, credit_cnt, fc_seq)) {
    return;
  }

  XLOG(DBG) << "[URMA-FC] onFcMessageReceived: type=" << static_cast<int>(type)
            << " credit_cnt=" << credit_cnt << " fc_seq=" << fc_seq
            << " peer_socket_id=0x" << std::hex << peer_socket_id << std::dec;

  switch (type) {
    case FC_REQ:
      handleFcReq(peer_socket_id, credit_cnt, fc_seq);
      break;
    case FC_REP:
      handleFcRep(credit_cnt, fc_seq);
      break;
    case FC_RETURN:
      handleFcReturn(peer_socket_id, credit_cnt, fc_seq);
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

void UrmaAsyncSocket::handleFcReq(uint64_t peer_socket_id, uint16_t credit_cnt,
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
  XLOG(DBG) << "[URMA-FC] REP: granted=" << credit_cnt
            << " peer_credit=" << fc_state_.peer_credit.load();

  // 唤醒 pending_write_(credit 到位后可继续发送)
  if (evb_) {
    evb_->runInLoop([this] { handlePendingWrites(); });
  }
}

// ── Phase 6: FC RETURN 处理(接收方) ──
// 对端归还 credit,credit_idle += N,credit_allocated -= N

void UrmaAsyncSocket::handleFcReturn(uint64_t peer_socket_id,
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

  // 序列化 FC 消息到 fc_send_buf_
  serializeFcMsg(fc_send_buf_, socket_id_.toUint64(), type, credit_cnt, fc_seq);

  // 构造 SGE + WR
  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(1);
  (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(fc_send_buf_);
  (*sge_arr)[0].len = FC_MSG_LEN;
  (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();

  // per-WR context(FC 消息无数据 block 需持有,send_data 留空)
  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->socket_id = socket_id_.toUint64();
  tx_ctx->seq = fc_seq;
  tx_ctx->type = TxCompletionCtx::MSG_CTRL_PULL;  // 复用控制包类型(FC CQE 回收路径相同)
  tx_ctx->jfs_index = fc_jfs_index;
  tx_ctx->msg_id = 0;
  tx_ctx->sge_arr = sge_arr;

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
  shutdownWrite();
  closeNow();
}

void UrmaAsyncSocket::closeNow() {
  if (state_ == State::kClosed) return;
  state_ = State::kClosed;
  destroyUrmaResources();
}

void UrmaAsyncSocket::shutdownWrite() {
  // Phase 2: poll JFS 池的 TX CQE 回收资源
  pollTx();
}

void UrmaAsyncSocket::shutdownWriteNow() {
}

void UrmaAsyncSocket::destroyUrmaResources() {
  // Phase 4: 路由表注销直接调 UrmaJfrPool(不依赖 shared_,避免 EventBase 析构后悬空)
  if (res_.remote_jetty) {
    if (auto* pool = UrmaJfrPool::get()) {
      pool->unregisterSocket(res_.peer_socket_id);
    }
  }
  if (res_.remote_fc_jetty) {
    if (auto* pool = UrmaJfrPool::get()) {
      pool->unregisterFcSocket(res_.peer_socket_id);
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