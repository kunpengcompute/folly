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

#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <utility>

#include <folly/io/async/urma/UrmaCqHandler.h>
#include <folly/io/async/urma/UrmaHandshake.h>
#include <folly/io/async/urma/UrmaTransportContext.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <folly/logging/xlog.h>

namespace folly {

// SDD-URMA-EVBEXCLUSIVE-001: acquirePinnedJfs 删除
// (I1:每 EVB 只有一个 JFS,sq_inflight 由 ctx_->tryAcquireIoSq() 管理)

// 统计 IOBuf chain 中 >peer_rx_buf_size_ 的大包数量(用于 writeChain 完成组计数)
static uint32_t countBigMsgs(const IOBuf* head, uint32_t peer_rx_buf_size) {
  uint32_t n = 0;
  if (!head) return 0;
  // 任务:1M 分片 —— 首个超限节点起整条剩余链聚合为一次大包 READ,
  // 组计数 = 1(与 sendChain/sendBigMsgByRead 的整链移交语义一致)
  const IOBuf* b = head;
  do {
    if (b->length() + sizeof(UrmaMsgHeader) > peer_rx_buf_size) {
      return 1;
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
  // 任务 D(H-5):幂等销毁(closeNow 已调过则直接返回;路由 erase 为内部第一步)
  destroyUrmaResources();
  // 任务 B(H-2):析构 drain —— erase 后 pending 不再增长,自旋等待在途
  // 跨线程转发归零(上限 10ms,超时 WARN 放行,风险 R3 安全阀)
  drainPendingForwards();
}

// ── 握手 ──

void UrmaAsyncSocket::startHandshake(bool is_client) {
  shared_ = UrmaTransportContext::getOrCreate(evb_);
  if (!shared_) {
    onHandshakeError("UrmaTransportContext::getOrCreate failed");
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
  // TcpHandshakeHandler 不能同步析构 —— 它是当前调用栈的所有者
  // (handlerReady → onHandshakeData → onDataAvailable → ACK → 本函数);
  // 同样 handshake_ 也不能同步 delete(onDataAvailable 栈帧还会访问 recv_buf_)。
  // 统一走 onHandshakeError 的成熟模式:先从 epoll 摘除,再 runInLoop 异步 delete。
  // (ASAN 实测修复:heap-use-after-free at UrmaHandshake.cpp:391)
  if (tcp_handshake_handler_) {
    auto* handler = tcp_handshake_handler_.release();
    handler->unregisterHandler();  // 立即摘除,防止再次触发 handlerReady
    if (evb_) {
      evb_->runInLoop([handler] { delete handler; });
    } else {
      delete handler;
    }
  }

  res_.shared_io_jfr = shared_->ioJfr();
  res_.shared_io_jfc = shared_->ioJfc();
  res_.shared_fc_jfr = shared_->fcJfr();
  res_.shared_fc_jfc = shared_->fcJfc();
  peer_rx_buf_size_ = handshake_->peer_rx_buf_size_;

  // Phase 2: TX CQE 由全局 JFS 池的 CqHandler 处理,不再 per-连接创建 tx_cq_handler_

  // 任务 D(H-5):路由注册 + 活跃连接记账同点绑定(幂等;
  // 服务端 processRemoteHello 预注册过的同 socket_id 返回既有 entry,不双计)
  registerRoutes();

  if (handshake_) {
    auto* hs = handshake_;
    handshake_ = nullptr;
    if (evb_) {
      evb_->runInLoop([hs] { delete hs; });  // 异步:调用栈仍持有
    } else {
      delete hs;
    }
  }

  state_ = State::kOpen;

  handlePendingWrites();

  // Phase 7: 接收方授予初始 credit(FC_REP)— 仅 fc_enable 时执行
  if (UrmaInit::getConfig().fc_enable && res_.remote_fc_jetty) {
    uint32_t initial_credit = 0;
    if (shared_) {
      initial_credit = shared_->creditIdle() / UrmaFcState::INITIAL_CREDIT_DIVISOR;
      if (initial_credit > 0) {
        for (uint32_t i = 0; i < initial_credit; ++i) {
          shared_->tryAllocCredit();
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

  // 任务 E(H-6):大包超时扫描定时器(可取消 AsyncTimeout 成员,替代
  // 不可取消的 runAfterDelay([this], 100),消除析构后 100ms UAF 窗口)
  scan_timer_ = std::make_shared<ScanTimer>(this);
  scan_timer_->arm();
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

// ── 任务 D(H-5):路由注册 + 活跃连接记账同点绑定(幂等) ──
//
// 绑定不变式(消除检视报告 H-5 (b) 降级连接只减不增):
//   首次路由注册 → incActiveConn;destroyUrmaResources 注销 → decActiveConn。
//   与 res_.remote_jetty 是否持有解耦:注册即绑定,注销即解绑,天然对称。
// 幂等性:UrmaRouteTable::insert 对同 socket_id 返回既有 entry(服务端预注册 +
//   onHandshakeReady 重注册不双计);active_conn_bound_ 标志保证 inc/dec 恰好一次。
// EVB 亲和:全部在属主 EVB 调用(握手线程/连接回调),DORMANT/wakeup 状态机
//   依赖此串行性(UrmaSharedJfr::active_conn_count_ 为普通 uint32_t + 断言)。
void UrmaAsyncSocket::registerRoutes() noexcept {
  if (!shared_) return;

  XLOG(DBG) << "[URMA] registerRoutes: socket_id=" << socket_id_.toUint64()
            << " peer_io_jetty_id=" << res_.peer_io_jetty_id
            << " active_conn_bound=" << active_conn_bound_ << " this=" << this;

  // Phase 3: 用本端 SocketId 注册(接收方按 header.dst_socket_id 查表路由)
  shared_->registerSocket(socket_id_.toUint64(), this);
  if (!route_entry_) {
    route_entry_ = shared_->findRouteEntry(socket_id_.toUint64());
  }
  if (res_.remote_fc_jetty) {
    shared_->registerFcSocket(socket_id_.toUint64(), this);
  }

  // 记账绑定:首次注册时恰好一次 inc(预注册与 onHandshakeReady 幂等)
  if (!active_conn_bound_ && route_entry_) {
    active_conn_bound_ = true;
    if (shared_) {
      shared_->incActiveConn();
    }
  }
}

// ── 任务 B(H-2):析构收尾(sock 置空协议) ──
// destroyUrmaResources 已 erase 路由并将 entry->sock 置空(置空协议):
// 在途转发 lambda(恒在本 EVB 队列)判空跳过回调,不再触碰 socket ——
// 本函数无需自旋等待 pending(原方案的自旋会阻塞本 EVB 队列,
// lambda 永不执行 → 10ms 超时放行 → UAF,已结构性消除,原风险 R3)。
// pending 计数仅保护 entry 本身的生命周期(guard shared_ptr 引用)。
void UrmaAsyncSocket::drainPendingForwards() noexcept {
  route_entry_.reset();
}

// ── 任务 F(H-7):关闭路径回调补全 ──
// pending_write_ 与 pending_big_msg_ 两阶段遍历,BigMsgGroup done 语义
// 复用 scanBigMsgTimeout 放弃路径:组内恰好一次 writeErr(跨队列去重)。
void UrmaAsyncSocket::failPendingWrites() noexcept {
  // 阶段 1: 挂起的 writeChain 队列
  while (pending_write_head_) {
    auto pw = std::move(pending_write_head_);
    pending_write_head_ = std::move(pw->next);
    if (!pending_write_head_) {
      pending_write_tail_ = nullptr;
    }
    if (pw->cb) {
      if (pw->big_group) {
        if (pw->big_group->done) {
          continue;  // 已经由大包放弃路径回调过,恰好一次
        }
        pw->big_group->done = true;
      }
      pw->cb->writeErr(0, AsyncSocketException(
          AsyncSocketException::END_OF_FILE, "socket closed"));
    }
  }
  pending_write_tail_ = nullptr;

  // 阶段 2: 挂起的大包(等对端 FREE 的 PULL_MEM)
  for (auto it = pending_big_msg_.begin(); it != pending_big_msg_.end(); ++it) {
    auto& p = it->second;
    WriteCallback* cb = p.cb;
    if (!cb || !p.group || p.group->done) {
      continue;  // 无回调或已恰好一次回调
    }
    p.group->done = true;
    cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::END_OF_FILE, "socket closed"));
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

  // 跨线程转发:确保 sendChain 在 EVB 线程执行(I2 same-thread 不变量)
  // close_loop 回调通过 .via(cpu_executor) 在 CPU 线程调用 writeChain,
  // 非属主 EVB 线程访问 per-EVB UrmaTransportContext 四件套(sq_inflight 等非原子变量)会 data race
  if (evb_ && !evb_->inRunningEventBaseThread()) {
    evb_->runInEventBaseThread(
        [this, cb, chain = std::move(chain), flags]() mutable {
          writeChain(cb, std::move(chain), flags);
        });
    return;
  }

  // 本 writeChain 的完成组:组内所有大包收到对端 FREE 后 writeSuccess 只回调一次
  auto group = std::make_shared<BigMsgGroup>();
  // 大包完成计数移交 sendBigMsgByRead(按 1M 分片块逐块计数,离散块独立 PULL)
  group->remaining = 0;

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
    // SDD-URMA-EVBEXCLUSIVE-001: TX CQE 由 pollAndDispatch 统一收割(I3)
    // 无需 rearmAllTxJfce —— 共享 JFC 的 JFCE 已由 CqHandler 注册 epoll
  } else if (ret == URMA_SUCCESS) {
//                << " big_deferred=" << big_deferred
//                << " pending_big=" << pending_big_msg_.size();
    // 组内大包未全部 FREE 时延迟 writeSuccess(跨 EAGAIN 拆分的多次 sendChain 也成立)
    if (cb && group->remaining == 0) cb->writeSuccess();
    bytes_written_ += total_posted;
  }
}

// ── TX completion ──
// SDD-URMA-EVBEXCLUSIVE-001: pollTx 整体删除
// (I3:TX CQE 由 pollAndDispatch(is_tx=true) 独立收割 —— WRITE/READ CQE 在
//  pollAndDispatchTxOnly 中处理,sq_inflight 归还 + socket 路由均在收割分派器完成)
// pollTx 保留为空函数,供现有调用点(startRead/deliverDataFrame 等)无编译断裂
void UrmaAsyncSocket::pollTx() noexcept {
  // no-op: TX CQE 由 UrmaTransportContext::pollAndDispatch(is_tx=true) 独立收割(I3)
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

  // Phase 7: credit 流控(软限制)— 仅 fc_enable 时触发 FC_REQ
  // close_loop 场景下 FC 闭环为净开销(per-EVB 独占 + 水位线已防溢出),
  // 默认禁用,消除跨线程 FC 消息通知导致的 P99 尾部延迟雪崩。
  if (UrmaInit::getConfig().fc_enable &&
      fc_state_.peer_credit.load(std::memory_order_acquire) <= 0) {
    uint32_t multiple = fc_state_.credit_multiple.load(std::memory_order_relaxed);
    uint16_t req_cnt = 1;
    if (shared_) {
      req_cnt = static_cast<uint16_t>(
          std::max(1u, multiple * shared_->creditIdle() /
                        UrmaFcState::INITIAL_CREDIT_DIVISOR));
    }
    uint16_t req_seq = fc_state_.fc_seq_next.fetch_add(1, std::memory_order_relaxed);
    sendFcMsg(FC_REQ, req_cnt, req_seq);
    // 不返回 EAGAIN,继续发送(软限制)
  }

  if (!shared_) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "TransportContext not initialized"));
    return URMA_FAIL;
  }
  EvbJfs* io_jfs = shared_->ioJfs();
  if (!io_jfs || !io_jfs->jfs) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::UNKNOWN, "IO JFS not available"));
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
        // 任务:1M tier 分片 —— 摘出【连续大节点子链】(遇小节点停止,
        // 上层 close_loop 会将多个 RPC 帧合入一条 writeChain,帧边界未知;
        // 大帧走离散分片 PULL,小帧继续 SEND,帧间乱序 thrift 按 seqid 容忍)
        auto big = std::move(chain);
        size_t big_len = big->length();
        auto rest = big->pop();  // big 单节点化,rest=剩余链
        IOBuf* bt = big.get();
        while (rest &&
               rest->length() + sizeof(UrmaMsgHeader) > peer_rx_buf_size_) {
          big_len += rest->length();
          auto node = std::move(rest);
          rest = node->pop();  // node 单节点化
          bt->appendChain(std::move(node));
          bt = bt->next();
        }
        chain = std::move(rest);  // 小节点(或空)回主链
        if (sendBigMsgByRead(cb, std::move(big), group)) {
          big_deferred = true;  // writeSuccess 延迟到组内所有大包 FREE
          total_posted += static_cast<uint32_t>(big_len);
          continue;  // 继续处理剩余(小节点攒 SGE)
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

    // SDD-URMA-EVBEXCLUSIVE-001: 独占 JFS,sq_inflight 由 ctx_->tryAcquireIoSq() 管理
    if (!shared_->tryAcquireIoSq()) {
      // SQ 满 → EAGAIN
      return URMA_EAGAIN;
    }

    // Phase 2: 创建 TxCompletionCtx,move IOBuf chain 到 ctx
    auto* tx_ctx = new TxCompletionCtx();
    tx_ctx->socket_id = socket_id_.toUint64();
    tx_ctx->seq = seq;
    tx_ctx->type = TxCompletionCtx::MSG_DATA;
    tx_ctx->jfs_index = 0;  // 独占只有一个 JFS,index 无意义(保留字段)
    tx_ctx->msg_id = 0;
    tx_ctx->send_data = std::move(*wr_chain_head);
    tx_ctx->sge_arr = sge_arr;

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
    urma_status_t ret = urma_post_jfs_wr(io_jfs->jfs, &wr, &bad_wr);
    if (ret == URMA_SUCCESS) {
      if (UrmaInit::getConfig().fc_enable) {
        fc_state_.peer_credit.fetch_sub(1, std::memory_order_acq_rel);
      }
      continue;
    }
    // post 失败:归还 sq_inflight + 回收 ctx
    shared_->releaseIoSq();
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
  while (pending_write_head_) {
    auto* pw = pending_write_head_.get();
    std::unique_ptr<IOBuf> chain = std::move(pw->buf);
    uint32_t total_posted = 0;
    bool big_deferred = false;

    // 防御:老路径可能没建完成组,补一个(计数仅覆盖剩余 chain)
    if (!pw->big_group) {
      pw->big_group = std::make_shared<BigMsgGroup>();
      pw->big_group->remaining = 0;
    }

    urma_status_t ret = sendChain(pw->cb, chain, total_posted, big_deferred,
                                  pw->big_group);
    if (ret == URMA_EAGAIN) {
      pw->buf = std::move(chain);
      // SDD-URMA-EVBEXCLUSIVE-001: TX CQE 由 pollAndDispatch 统一收割(I3)
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

bool UrmaAsyncSocket::sendBigMsgByRead(
    WriteCallback* cb, std::unique_ptr<IOBuf> chain,
    const std::shared_ptr<BigMsgGroup>& group) noexcept {
  if (!chain) {
    return false;
  }
  size_t total = chain->computeChainDataLength();
  if (total == 0) {
    return false;
  }
  constexpr size_t kMaxBigMsgTotal = 256ull * 1024 * 1024;  // 巨量防御(256M)
  if (total > kMaxBigMsgTotal) {
    XLOG(ERR) << "[URMA] sendBigMsgByRead: total=" << total
              << " exceeds hard limit 256M, reject";
    return false;
  }

  // ── 段表免拷贝协议(任务:去 9M tier + 打包分片免拷贝)──
  // 序列化输出已是 1M 块链(IOBufQueue 大块 1M 档分配),本函数零拷贝:
  //   - clone() 整链(引用计数共享数据,无 memcpy)
  //   - 遍历物理块生成段表 (va=块数据址, len=块长)
  //   - 段表 > kMaxPullFrags 时按批拆分(每批独立 msg_id 串行保序)
  // 接收方对每段独立 READ(WR 链),DMA 直写本地块 —— 全程无二次拷贝。
  auto kept = chain->clone();  // 零拷贝保活(writeChain 原链即释放)
  std::vector<PullMemFragDesc> all_frags;
  all_frags.reserve(16);
  for (IOBuf* cur = kept.get();; cur = cur->next()) {
    if (cur->length() > 0) {
      PullMemFragDesc d;
      d.va = reinterpret_cast<uint64_t>(cur->data());
      d.len = static_cast<uint32_t>(cur->length());
      d.rsvd = 0;
      all_frags.push_back(d);
    }
    if (cur->next() == kept.get() || cur->next() == nullptr) {
      break;  // 尾节点(folly 循环链)
    }
  }

  size_t nposted = 0;
  size_t off = 0;
  while (off < all_frags.size()) {
    size_t cnt = std::min(all_frags.size() - off, size_t(kMaxPullFrags));
    uint32_t batch_total = 0;
    for (size_t i = 0; i < cnt; ++i) {
      batch_total += all_frags[off + i].len;
    }

    // C-3: msg_id 回绕后跳过仍在途的
    uint16_t msg_id = next_msg_id_;
    while (pending_big_msg_.count(msg_id)) {
      ++msg_id;
    }
    next_msg_id_ = msg_id + 1;

    PendingBigMsg p;
    // 每批独立 clone 引用(零拷贝;批间独立生命周期,FREE 各批各自释放)
    p.iobuf = std::shared_ptr<IOBuf>(kept->clone());
    p.frags.assign(all_frags.begin() + off, all_frags.begin() + off + cnt);
    p.total_size = batch_total;
    p.post_time = std::chrono::steady_clock::now();
    p.retries = 0;
    p.is_free = false;
    p.cb = cb;
    p.group = group;
    pending_big_msg_.emplace(msg_id, std::move(p));
    if (group) {
      group->remaining++;
    }

    // 段表 post(签名见 postControlPacketFrags)
    if (!postControlPacketFrags(msg_id, 0,
                                all_frags.data() + off,
                                static_cast<uint16_t>(cnt), batch_total)) {
      pending_big_msg_.erase(msg_id);
      if (group && group->remaining > 0) {
        group->remaining--;
      }
      return nposted > 0;
    }
    ++nposted;
    off += cnt;
  }
  return true;
}

// 段表式 PULL_MEM 控制包(免拷贝):SGE[0] = PullMemFragPacket(变长段表),
// imm_data = total_size(诊断用);控制包走 4K recv buffer(≤ kMaxPullFrags 段)
bool UrmaAsyncSocket::postControlPacketFrags(
    uint16_t msg_id, uint16_t seq, const PullMemFragDesc* frags,
    uint16_t frag_count, uint32_t total_size) noexcept {
  if (!res_.remote_jetty) return false;
  if (!frags || frag_count == 0 || frag_count > kMaxPullFrags) return false;

  // SDD-URMA-EVBEXCLUSIVE-001: 独占 IO JFS
  if (!shared_) return false;
  EvbJfs* io_jfs = shared_->ioJfs();
  if (!io_jfs || !io_jfs->jfs) return false;
  if (!shared_->tryAcquireIoSq()) {
    XLOG(WARN) << "[URMA] postControlPacketFrags: SQ full";
    return false;
  }

  size_t pkt_len = sizeof(PullMemFragPacket) -  // 头 40B
                  sizeof(PullMemFragDesc) * (kMaxPullFrags - frag_count);
  auto ctrl_buf = IOBuf::create(pkt_len);
  ctrl_buf->append(pkt_len);
  auto* pkt = reinterpret_cast<PullMemFragPacket*>(ctrl_buf->writableData());
  writeHeader(&pkt->header, res_.peer_socket_id, seq, MSG_PULL_MEM, msg_id);
  pkt->frag_count = frag_count;
  pkt->rsvd0 = 0;
  pkt->total_size = total_size;
  std::memcpy(pkt->frags, frags, sizeof(PullMemFragDesc) * frag_count);

  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(1);
  (*sge_arr)[0].addr = reinterpret_cast<uint64_t>(ctrl_buf->writableData());
  (*sge_arr)[0].len = static_cast<uint32_t>(pkt_len);
  (*sge_arr)[0].tseg = UrmaInit::getPoolTseg();

  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->type = TxCompletionCtx::MSG_CTRL_PULL;
  tx_ctx->socket_id = socket_id_.toUint64();
  tx_ctx->seq = seq;
  tx_ctx->jfs_index = 0;
  tx_ctx->msg_id = msg_id;
  tx_ctx->sge_arr = sge_arr;
  tx_ctx->send_data = std::move(*ctrl_buf);  // 持有段表 buffer 到 TX CQE

  urma_jfs_wr_t wr = {};
  wr.opcode = URMA_OPC_SEND_IMM;
  wr.tjetty = res_.remote_jetty;
  wr.flag.bs.complete_enable = 1;
  wr.send.src.sge = sge_arr->data();
  wr.send.src.num_sge = 1;
  wr.send.imm_data = total_size;  // 随 CQE 通告(诊断)
  wr.user_ctx = reinterpret_cast<uint64_t>(tx_ctx);
  wr.next = nullptr;

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t st = urma_post_jfs_wr(io_jfs->jfs, &wr, &bad_wr);
  if (st != URMA_SUCCESS) {
    shared_->releaseIoSq();
    delete tx_ctx;
    XLOG(ERR) << "[URMA] ctrlPullFrags FAIL msg_id=" << msg_id
              << " frags=" << frag_count << " total=" << total_size
              << " st=" << static_cast<int>(st);
    return false;
  }
  return true;
}

// ── 单边 READ:接收方 ──

void UrmaAsyncSocket::onControlMsgImm(uint8_t type, uint16_t msg_id,
                                       uint64_t remote_va,
                                       uint32_t data_size) noexcept {
  if (type == MSG_FREE) {
    // 发送方收到 FREE:释放源链引用(零拷贝 clone)。
    // writeSuccess 只在组内全部大包 FREE 后回调一次
    auto it = pending_big_msg_.find(msg_id);
    if (it != pending_big_msg_.end() && !it->second.is_free) {
      auto group = it->second.group;
      WriteCallback* cb = it->second.cb;
      pending_big_msg_.erase(it);  // 释放 clone 引用
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
  // PULL_MEM 已由 onPullMemFrags(段表版)接管,旧 va/size 路径不再使用
}

// ── 段表式 PULL_MEM(免拷贝):接收方入口 ──
// 由 UrmaJfrPool::harvestAndDispatch 解析 PullMemFragPacket 后调用
// (段表已按值拷出 recv buffer,slot 立即回收)
void UrmaAsyncSocket::onPullMemFrags(uint16_t msg_id,
                                     const PullMemFragDesc* frags,
                                     uint16_t frag_count,
                                     uint32_t total_size) noexcept {
  XLOG(DBG) << "[URMA] onPullMemFrags msg_id=" << msg_id
            << " frags=" << frag_count << " total=" << total_size
            << " has_pending_read=" << has_pending_read_;

  if (has_pending_read_) {
    // 有 READ 在途:控制包进保序队列(等当前 READ 完成,串行保序)
    RxBufItem item;
    item.is_control = true;
    item.msg_id = msg_id;
    item.frags.assign(frags, frags + frag_count);
    item.total_size = total_size;
    rx_pending_queue_.push(std::move(item));
    return;
  }

  has_pending_read_ = startRead(msg_id, frags, frag_count, total_size);
  if (!has_pending_read_) {
    XLOG(ERR) << "[URMA] PULL_FRAGS startRead FAILED: msg_id=" << msg_id;
  }
}

// ── 接收侧段表 READ(免拷贝,WR 链串行 post)──
// 每段 (src=发送方块 va/len, dst=本地 poolAlloc 块),wr.next 串联成链;
// 仅尾 WR complete_enable + user_ctx,硬件作为单一事务串行执行,
// 尾 WR CQE 触发 onReadComplete。段块链 DMA 直写,零拷贝交付 read_queue_。
bool UrmaAsyncSocket::startRead(uint16_t msg_id, const PullMemFragDesc* frags,
                                uint16_t frag_count,
                                uint32_t total_size) noexcept {
  if (!frags || frag_count == 0 || total_size == 0) {
    return false;
  }
  if (frag_count > kMaxPullFrags) {
    XLOG(ERR) << "startRead: frag_count=" << frag_count
              << " > kMaxPullFrags, msg_id=" << msg_id;
    return false;
  }

  // 每段分配本地 dst 块(段长 ≤ 1M,按段长请求池档)
  std::unique_ptr<IOBuf> head;
  IOBuf* tail = nullptr;
  for (uint16_t i = 0; i < frag_count; ++i) {
    size_t frag = frags[i].len;
    if (frag == 0 || frag > URMA_LARGE_BUF_SIZE) {
      XLOG(ERR) << "startRead: bad frag len=" << frag << " idx=" << i;
      return false;  // 已分配块随 head 析构释放
    }
    void* buf = UrmaInit::poolAlloc(URMA_LARGE_BUF_SIZE);
    if (!buf) {
      XLOG(ERR) << "startRead: poolAlloc failed at frag " << i
                << " msg_id=" << msg_id;
      return false;
    }
    auto frag_iobuf = IOBuf::takeOwnership(
        buf, URMA_LARGE_BUF_SIZE,
        [](void* p, void*) { UrmaInit::poolFree(p); });
    // 5 参重载 length=capacity,clear 归零再 append 实际段长(防虚增)
    frag_iobuf->clear();
    frag_iobuf->append(frag);
    if (!head) {
      head = std::move(frag_iobuf);
      tail = head.get();
    } else {
      tail->appendChain(std::move(frag_iobuf));
      tail = tail->next();
    }
  }

  // SDD-URMA-EVBEXCLUSIVE-001: 独占 IO JFS
  if (!shared_) {
    XLOG(ERR) << "startRead: no TransportContext";
    return false;
  }
  EvbJfs* io_jfs = shared_->ioJfs();
  if (!io_jfs || !io_jfs->jfs) {
    XLOG(ERR) << "startRead: IO JFS not available";
    return false;
  }
  if (!shared_->tryAcquireIoSq()) {
    XLOG(WARN) << "startRead: SQ full, msg_id=" << msg_id;
    return false;
  }

  // SGE 布局:每段 2 项(src 段 i / dst 本地块 i)
  auto sge_arr = std::make_shared<std::vector<urma_sge_t>>(2 * size_t(frag_count));
  {
    size_t idx = 0;
    for (IOBuf* cur = head.get();; cur = cur->next()) {
      (*sge_arr)[idx].addr = frags[idx / 2].va;  // src:发送方块地址(段表)
      (*sge_arr)[idx].len = static_cast<uint32_t>(cur->length());
      (*sge_arr)[idx].tseg = res_.remote_seg;
      ++idx;
      (*sge_arr)[idx].addr = reinterpret_cast<uint64_t>(cur->writableData());
      (*sge_arr)[idx].len = static_cast<uint32_t>(cur->length());
      (*sge_arr)[idx].tseg = UrmaInit::getPoolTseg();
      ++idx;
      if (cur == tail) {
        break;  // 尾节点(folly 循环链)
      }
    }
  }

  // 构建 WR 链:wr.next 串联 N 段,仅尾 WR complete_enable + user_ctx
  auto wr_chain = std::make_unique<urma_jfs_wr_t[]>(frag_count);
  for (uint16_t i = 0; i < frag_count; ++i) {
    wr_chain[i] = {};
    wr_chain[i].opcode = URMA_OPC_READ;
    wr_chain[i].tjetty = res_.remote_jetty;
    wr_chain[i].rw.src.sge = &(*sge_arr)[2 * size_t(i)];
    wr_chain[i].rw.src.num_sge = 1;
    wr_chain[i].rw.dst.sge = &(*sge_arr)[2 * size_t(i) + 1];
    wr_chain[i].rw.dst.num_sge = 1;
    wr_chain[i].next = (i + 1 < frag_count) ? &wr_chain[i + 1] : nullptr;
  }
  // 仅尾 WR 产生 CQE + 携带 user_ctx
  wr_chain[frag_count - 1].flag.bs.complete_enable = 1;

  auto* tx_ctx = new TxCompletionCtx();
  tx_ctx->type = TxCompletionCtx::MSG_READ;
  tx_ctx->socket_id = socket_id_.toUint64();
  tx_ctx->jfs_index = 0;
  tx_ctx->msg_id = msg_id;
  tx_ctx->sge_arr = sge_arr;
  tx_ctx->read_wr_cnt = frag_count;

  // 尾 WR 携带 user_ctx(必须在 move 前设置)
  wr_chain[frag_count - 1].user_ctx = reinterpret_cast<uint64_t>(tx_ctx);
  tx_ctx->read_wr_chain = std::move(wr_chain);
  wr_chain = nullptr;  // 已 move

  urma_jfs_wr_t* bad_wr = nullptr;
  urma_status_t st = urma_post_jfs_wr(io_jfs->jfs, tx_ctx->read_wr_chain.get(), &bad_wr);
  if (st != URMA_SUCCESS) {
    XLOG(ERR) << "startRead: post WR chain failed status=" << st
              << " msg_id=" << msg_id;
    shared_->releaseIoSq();
    delete tx_ctx;
    return false;
  }

  // 赋值在途状态
  pending_read_msg_id_ = msg_id;
  pending_read_buf_ = std::move(head);
  pending_read_sge_ = sge_arr;

  XLOG(DBG) << "[URMA] startRead posted msg_id=" << msg_id
            << " frags=" << frag_count << " total=" << total_size;
  // TX CQE 由 CqHandler 事件驱动收割,pollTx 已删除,无需调度
  return true;
}

void UrmaAsyncSocket::onReadComplete() noexcept {
  if (!pending_read_buf_) {
    XLOG(WARN) << "[URMA] readDone: buf already reset, skip";
    return;
  }
  XLOG(DBG) << "[URMA] onReadComplete msg_id=" << pending_read_msg_id_
             << " len=" << pending_read_buf_->computeChainDataLength();
  auto data = std::move(pending_read_buf_);
  pending_read_sge_.reset();
  has_pending_read_ = false;
  bytes_received_ += data->computeChainDataLength();

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
  // WR 链串行 post:仅尾 WR 产生 1 个 CQE,直接 finalize
  if (pending_read_msg_id_ != msg_id) {
    XLOG(DBG) << "[URMA] handleRemoteReadCqe MISMATCH/stale: msg_id=" << msg_id
             << " status=" << status;
    return;  // 过时 CQE
  }

  // 任务 B(H-2):本方法只在属主 EVB 线程执行 ——
  // 跨 EVB 收割的 READ CQE 由 UrmaJfrPool::dispatchReadCqe 统一转发
  // (同 EVB 短路同步调用,零延迟;跨 EVB 经 runInLoop 转发,不变量 I1)。
  if (status != URMA_CR_SUCCESS) {
    onReadFailed(status);
  } else {
    onReadComplete();
  }
}

bool UrmaAsyncSocket::postFreeNotify(uint16_t msg_id, uint16_t seq) noexcept {
  if (!res_.remote_jetty) return false;

  // SDD-URMA-EVBEXCLUSIVE-001: 独占 IO JFS
  if (!shared_) return false;
  EvbJfs* io_jfs = shared_->ioJfs();
  if (!io_jfs || !io_jfs->jfs) return false;
  if (!shared_->tryAcquireIoSq()) return false;

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
  tx_ctx->jfs_index = 0;
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
  urma_status_t st = urma_post_jfs_wr(io_jfs->jfs, &wr, &bad_wr);
  if (st != URMA_SUCCESS) {
    shared_->releaseIoSq();
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
      // 串行保序:遇到控制包,发起新段表 READ 后停止取出
      has_pending_read_ = startRead(item.msg_id, item.frags.data(),
                                    static_cast<uint16_t>(item.frags.size()),
                                    item.total_size);
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
      // PULL_MEM 段表重发:块地址不变(源链 clone 保活),重发同一批段表;
      // 控制消息不递增 send_seq_
      if (postControlPacketFrags(static_cast<uint16_t>(it->first), 0,
                                 p.frags.data(),
                                 static_cast<uint16_t>(p.frags.size()),
                                 p.total_size)) {
        p.retries++;
        p.post_time = now;
        XLOG(WARN) << "UrmaAsyncSocket: resend control msg_id=" << it->first
                   << " retries=" << (int)p.retries
                   << " total=" << p.total_size;
      } else {
        p.retries = kCtrlMsgMaxRetries;
      }
    }
    ++it;
  }
  // 任务 E(H-6):自续调度移至 ScanTimer::timeoutExpired(可取消),
  // 本函数仅执行扫描;close/析构时 scan_timer_.reset() 即阻断后续触发
}

// ── 任务 E(H-6):ScanTimer(AsyncTimeout 成员,替代 runAfterDelay 自循环) ──
// timeoutExpired 在 socket 属主 EVB 执行;socket 关闭/析构后定时器已 cancel,
// 消除旧 runAfterDelay([this]) 析构后 100ms 窗口 UAF(检视报告 H-6)。
// 除大包扫描外,同时驱动:保序重排超时检查 + FC credit 周期归还。
void UrmaAsyncSocket::ScanTimer::timeoutExpired() noexcept {
  // alive_ 标志:destroyUrmaResources 置 false 后不再访问 sock_
  // (libevent 2.x event_del 无法撤回已就绪事件,timer 可能在
  // scan_timer_.reset() 后仍触发,此时 alive_==false 短路返回)
  if (!alive_.load(std::memory_order_acquire)) {
    return;
  }
  if (sock_->state_ != State::kOpen) {
    return;  // socket 已关闭:停止自续
  }
  sock_->scanBigMsgTimeout();
  sock_->checkReorderTimeout();
  sock_->maybeReturnCredit();
  scheduleTimeout(100);  // 100ms 周期自续(设计 §4.7.8)
}

// ── 保序:重排缓冲超时兜底(任务:保序功能增强) ──
// 空洞在 reorder_timeout_ms 内未补齐 → 放行缓冲全部(seq 升序交付)+
// expected 重同步 + WARN 计数。正常流量下缓冲恒空,零开销。
void UrmaAsyncSocket::checkReorderTimeout() noexcept {
  if (!UrmaInit::getConfig().rx_reorder_enable) return;
  if (rx_reorder_.empty()) {
    return;
  }
  uint16_t expected = expected_seq_.load(std::memory_order_relaxed);
  std::vector<std::unique_ptr<IOBuf>> released;
  uint32_t timeout_ms = UrmaInit::getConfig().reorder_timeout_ms;
  if (rx_reorder_.releaseOnTimeout(expected, timeout_ms, released,
                                   std::chrono::steady_clock::now())) {
    ++reorder_released_;
    expected_seq_.store(expected, std::memory_order_relaxed);
    XLOG(WARN) << "[URMA] reorder hole timeout(" << timeout_ms
               << "ms), released=" << released.size()
               << " resync expected->" << expected
               << " socket_id=0x" << std::hex << socket_id_.toUint64()
               << std::dec
               << " (reliable-SEND seq gap = connection anomaly)";
    for (auto& buf : released) {
      onRxBuf(std::move(buf));
    }
  }
}

// ── FC:周期 credit 归还(任务:流控闭环) ──
// 旧实现只借不还:发送方消耗 credit 后从不 RETURN → 接收方 credit_idle
// 单调下降、REQ 授予枯竭、peer_credit 单调下探(统计失效,M-2 遗留)。
// 归还策略(ScanTimer 100ms 周期):peer_credit 超过保留水位(初始授予量
// rx_depth/DIVISOR)时,归还多余部分 → 接收方 idle 回升 → 下一轮 REQ 可再授予。
// 闭环:REQ(借) → REP(授) → 消耗 → RETURN(还) → idle 回升 → ...
void UrmaAsyncSocket::maybeReturnCredit() noexcept {
  if (!UrmaInit::getConfig().fc_enable) return;
  if (state_ != State::kOpen || !res_.remote_fc_jetty) {
    return;
  }
  int32_t pc = fc_state_.peer_credit.load(std::memory_order_acquire);
  if (pc <= 0) {
    return;  // 无可归还(软限制下允许负)
  }
  // 保留水位 = 初始授予量(覆盖 close-loop 并发窗口)
  int32_t keep = 0;
  if (shared_) {
    keep = static_cast<int32_t>(
        shared_->rxDepth() / UrmaFcState::INITIAL_CREDIT_DIVISOR);
  }
  if (pc <= keep) {
    return;  // 未超保留水位,无需归还
  }
  uint16_t ret = static_cast<uint16_t>(pc - keep);
  uint16_t fc_seq = fc_state_.fc_seq_next.fetch_add(1, std::memory_order_relaxed);
  if (sendFcMsg(FC_RETURN, ret, fc_seq)) {
    fc_state_.peer_credit.fetch_sub(ret, std::memory_order_acq_rel);
    XLOG(INFO) << "[URMA-FC] RETURN sent: cnt=" << ret
              << " peer_credit " << pc << "->" << (pc - ret);
  }
}

// ── Phase 3: 独立 buffer 路由 ──
// 从 recv buffer 前 24B 解析 UrmaMsgHeader,仅处理 MSG_DATA
// MSG_PULL_MEM / MSG_FREE 走 SEND_IMM 路径,由 UrmaJfrPool::harvestAndDispatch
// 解析 imm_data + PullMemControlPacket 后直接调 onControlMsgImm,不进入此方法
//
// 任务 C(H-3)零拷贝所有权交付(docs/urma-threading.md 不变量 I4):
//   DATA slot 经 IOBuf::takeOwnership 交付,deleter 将 slot 送回 per-JFR
//   无锁回收栈 —— 修复旧"wrapBuffer 非拥有交付 + 消费即重投"的数据竞争
//   (上层异步持有期间 DMA 覆盖同一 buffer)。
//   held 预算(安全阀,防 RQ 枯竭):movable 仅当 held < max_water - min_water;
//   超预算降级 memcpy(等价旧行为,RQ 永远保有 ≥ min_water,杜绝 RNR 风暴)。
//
// recv_buf: 接收到的 buffer 起始(header 24B + data)
// recv_len: 实际接收字节数
// ctx:      常驻 RxSlotCtx(零拷贝交付后 deleter 重投凭据)
// 返回:     true = 所有权已随 IOBuf 转移(收割线程不再回收该 slot)
bool UrmaAsyncSocket::onIoMessageReceived(void* recv_buf, uint32_t recv_len,
                                           RxSlotCtx* ctx) noexcept {
  if (!recv_buf || recv_len < sizeof(UrmaMsgHeader) || !ctx) {
    return false;  // 收割线程回收 slot
  }

  auto* hdr = parseHeader(recv_buf);
  if (!hdr) {
    XLOG(WARN) << "[URMA] onIoMessageReceived: magic mismatch, recv_len="
               << recv_len;
    return false;
  }

  // version 校验(不匹配仅 WARN,不丢弃,向前兼容)
  if (hdr->version != URMA_VERSION) {
    XLOG(WARN) << "[URMA] onIoMessageReceived: version mismatch, got="
               << static_cast<int>(hdr->version)
               << " expected=" << static_cast<int>(URMA_VERSION)
               << " type=" << static_cast<int>(hdr->type);
  }

  if (hdr->type != MSG_DATA) {
    // MSG_PULL_MEM / MSG_FREE 不应出现在 SEND 路径(应走 SEND_IMM)
    XLOG(WARN) << "[URMA] onIoMessageReceived: unexpected type="
               << static_cast<int>(hdr->type)
               << " in SEND path (expected SEND_IMM), msg_id=" << hdr->msg_id;
    return false;
  }

  // ── seq 保序(默认关:per-EVB 独占 JFR + TX/RX 独立 JFC 下
  //   URMA SEND 可靠有序,seq 严格递增,重排器恒空)──
  //   rx_reorder_enable=true 时走完整重排逻辑(多 JFR 共享场景);
  //   默认 false:直接交付,零 seq 校验开销。
  if (!UrmaInit::getConfig().rx_reorder_enable) {
    bool owned = deliverDataFrame(recv_buf, recv_len, ctx);
    expected_seq_.store(static_cast<uint16_t>(hdr->seq + 1),
                        std::memory_order_relaxed);
    return owned;
  }

  uint16_t expected = expected_seq_.load(std::memory_order_relaxed);
  if (hdr->seq == expected) {
    bool owned = deliverDataFrame(recv_buf, recv_len, ctx);
    expected_seq_.store(static_cast<uint16_t>(expected + 1),
                        std::memory_order_relaxed);
    if (!rx_reorder_.empty()) {
      std::vector<std::unique_ptr<IOBuf>> drained;
      rx_reorder_.drainInOrder(expected, drained);
      for (auto& buf : drained) {
        onRxBuf(std::move(buf));
      }
    }
    return owned;
  }

  int32_t dist = UrmaSeqReorder::seqDist(hdr->seq, expected);
  if (dist > 0) {
      auto held_buf = buildOwnedDataFrame(recv_buf, recv_len, ctx);
      if (held_buf && rx_reorder_.push(hdr->seq, std::move(held_buf),
                                        std::chrono::steady_clock::now())) {
        ++reorder_buffered_;
        return true;
      }
      if (held_buf) {
        XLOG(WARN) << "[URMA] reorder buffer full, flush+resync: got="
                   << hdr->seq << " expected=" << expected
                   << " depth=" << rx_reorder_.size();
        std::vector<std::unique_ptr<IOBuf>> flushed;
        flushReorderBuffer(expected, flushed);
        for (auto& buf : flushed) {
          onRxBuf(std::move(buf));
        }
        bool owned = deliverDataFrame(recv_buf, recv_len, ctx);
        expected_seq_.store(static_cast<uint16_t>(hdr->seq + 1),
                            std::memory_order_relaxed);
        return owned;
      }
  } else {
    ++reorder_dup_dropped_;
    XLOG(WARN) << "[URMA] dup/stale seq dropped: got=" << hdr->seq
               << " expected=" << expected;
    return false;
  }

  bool owned = deliverDataFrame(recv_buf, recv_len, ctx);
  expected_seq_.store(static_cast<uint16_t>(hdr->seq + 1),
                      std::memory_order_relaxed);
  return owned;
}

// 冲刷重排缓冲:timeout=0 强制放行全部(帧数据按 seq 升序交付),
// expected 重同步为最大 seq+1。仅属主 EVB 调用
void UrmaAsyncSocket::flushReorderBuffer(
    uint16_t& expected,
    std::vector<std::unique_ptr<IOBuf>>& out) noexcept {
  rx_reorder_.releaseOnTimeout(expected, 0, out,
                               std::chrono::steady_clock::now());
}

// MSG_DATA 帧 movable/拷贝构造(不入 read_queue_,交付/入缓冲由调用方决定)
// 返回空 = 无法 movable(拷贝路径由调用方另行处理);held++/transit-- 由本函数完成
std::unique_ptr<IOBuf> UrmaAsyncSocket::buildOwnedDataFrame(
    void* recv_buf, uint32_t recv_len, RxSlotCtx* ctx) noexcept {
  EvbJfr* jfr = ctx->jfr;
  if (!jfr || !jfr->recycle) {
    return nullptr;
  }
  uint32_t data_len = recv_len - sizeof(UrmaMsgHeader);
  void* data_ptr = static_cast<uint8_t*>(recv_buf) + sizeof(UrmaMsgHeader);
  uint32_t held_budget =
      jfr->watermark.max_water - jfr->watermark.min_water;
  uint32_t held = jfr->held_count.load(std::memory_order_acquire);
  static const bool force_copy_rx = []() {
    const char* e = getenv("URMA_RX_FORCE_COPY");
    return e && e[0] == '1';
  }();
  if (force_copy_rx || data_len == 0 || held >= held_budget) {
    return nullptr;  // 走拷贝路径
  }
  jfr->held_count.fetch_add(1, std::memory_order_acq_rel);
  jfr->transit_cnt.fetch_sub(1, std::memory_order_acq_rel);
  return IOBuf::takeOwnership(
      data_ptr, data_len,
      [](void*, void* ud) {
        auto* rc = static_cast<RxSlotCtx*>(ud);
        UrmaTransportContext::releaseHeldSlot(rc->ctx, rc->jfr, rc->slot);
      },
      ctx);
}

// MSG_DATA 帧交付:movable 优先(零拷贝),降级 memcpy;append read_queue_ +
// deliverReadData。返回 true = slot 已随 movable 转移
// TX CQE 由 UrmaTransportContext::pollAndDispatch(is_tx=true) 独立收割,
// 不再在 RX 路径调度 pollTx(原 schedulePollTx 是 no-op 残留,纯 runInLoop 开销)。
bool UrmaAsyncSocket::deliverDataFrame(void* recv_buf, uint32_t recv_len,
                                        RxSlotCtx* ctx) noexcept {
  uint32_t data_len = recv_len - sizeof(UrmaMsgHeader);
  void* data_ptr = static_cast<uint8_t*>(recv_buf) + sizeof(UrmaMsgHeader);

  if (auto held_buf = buildOwnedDataFrame(recv_buf, recv_len, ctx)) {
    onRxBuf(std::move(held_buf));
    return true;  // 所有权已转移,收割线程不再回收
  }

  // 降级 memcpy 路径(超 held 预算 / data_len==0 / FORCE_COPY)
  auto iobuf = IOBuf::copyBuffer(data_ptr, data_len);
  onRxBuf(std::move(iobuf));
  return false;  // 数据已拷出,收割线程立即回收 slot
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

  // SDD-URMA-EVBEXCLUSIVE-001: credit 从 ctx_->credit_idle 扣减(same-thread I2)
  uint32_t granted = 0;
  if (shared_) {
    uint32_t idle = shared_->creditIdle();
    uint32_t to_grant = std::min(static_cast<uint32_t>(credit_cnt), idle);
    for (uint32_t i = 0; i < to_grant; ++i) {
      if (shared_->tryAllocCredit()) {
        ++granted;
      } else {
        break;
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
  // 线程安全修正:FC 收割线程可能非本 socket 属主 EVB(FC JFR 归属其他 EVB),
  // runInLoop(Func) 非线程安全 —— 统一走 runInEventBaseThread
  if (evb_) {
    evb_->runInEventBaseThread([this] { handlePendingWrites(); });
  }
}

// ── Phase 6: FC RETURN 处理(接收方) ──
// 对端归还 credit,credit_idle += N,credit_allocated -= N

void UrmaAsyncSocket::handleFcReturn(uint64_t dst_socket_id,
                                       uint16_t credit_cnt,
                                       uint16_t fc_seq) noexcept {
  if (shared_) {
    shared_->releaseCredit(credit_cnt);  // allocated -= cnt; idle += cnt
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

  // SDD-URMA-EVBEXCLUSIVE-001: 独占 FC JFS
  if (!shared_) return false;
  EvbJfs* fc_jfs = shared_->fcJfs();
  if (!fc_jfs || !fc_jfs->jfs) return false;
  if (!shared_->tryAcquireFcSq()) {
    XLOG(DBG) << "[URMA-FC] FC SQ full";
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
  tx_ctx->jfs_index = 0;
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
    shared_->releaseFcSq();
    delete tx_ctx;
    return false;
  }

  // SDD-URMA-EVBEXCLUSIVE-001: 无需 rearmAllFcTxFce(I3:TX JFC 独立收割)
  return true;
}

// ── 接收(普通 SEND 数据) ──

void UrmaAsyncSocket::onRxBuf(std::unique_ptr<IOBuf> data) noexcept {
  bytes_received_ += data->length();
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
}void UrmaAsyncSocket::shutdownWrite() {
  if (tcp_delegate_) {
    tcp_delegate_->shutdownWrite();
    return;
  }
  // TX CQE 由 CqHandler 事件驱动收割,pollTx 已删除
}

void UrmaAsyncSocket::shutdownWriteNow() {
  if (tcp_delegate_) {
    tcp_delegate_->shutdownWriteNow();
    return;
  }
}

void UrmaAsyncSocket::destroyUrmaResources() {
  // 任务 D(H-5):幂等化 —— closeNow 与析构双调防护(resources_destroyed_)
  // 消除旧代码 decActiveConn 双调下溢(检视报告 H-5(a))。
  // 隐式同 EVB 串行依赖显式化(检视报告 H-5(c) 结论修正:无跨线程竞态,
  // 但以 DCHECK 固化前提,CI debug 构建常开)。
  if (resources_destroyed_) {
    return;
  }
  resources_destroyed_ = true;
  DCHECK(!evb_ || evb_->inRunningEventBaseThread());

  // SDD-URMA-EVBEXCLUSIVE-001: 路由摘表 + sock 置空(置空协议)
  if (shared_) {
    shared_->unregisterSocket(socket_id_.toUint64());
    shared_->unregisterFcSocket(socket_id_.toUint64());
  }
  if (route_entry_) {
    route_entry_->sock.store(nullptr, std::memory_order_release);
  }
  // 任务 E(H-6):取消大包扫描定时器
  // libevent 2.x event_del 无法撤回已就绪事件:如果 timer 事件已在 active
  // 队列中,直接 reset() 析构后 vtable 破坏,event_base_loop 处理该事件时
  // 调用 timeoutExpired() → SIGSEGV。改为:
  //   1. kill() 设置 alive_=false(原子,timeoutExpired 中短路返回)
  //   2. runInLoop 延迟 reset()(下一轮 loop 当前事件已处理完毕,event_del 安全)
  if (scan_timer_) {
    scan_timer_->kill();
    auto timer = std::move(scan_timer_);
    if (evb_) {
      evb_->runInLoop([timer = std::move(timer)] {});
    }
  }
  // 任务 D(H-5):记账解绑(dec 绑定在路由注销处,与注册同点幂等对称;
  // 与 res_.remote_jetty 解耦,降级连接天然对称)
  if (active_conn_bound_) {
    active_conn_bound_ = false;
    if (shared_) {
      shared_->decActiveConn();
    }
  }

  // 任务 F(H-7):关闭路径回调补全 —— reset 前遍历 pending 队列,
  // 逐项 writeErr(END_OF_FILE),BigMsgGroup done 语义保证组内恰好一次;
  // 与 fallbackToTcp 重放路径互斥(降级时不销毁资源,现有逻辑已保证)
  failPendingWrites();

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
   // 任务 A(SDD-URMA-SHARED-JFS-JFR-002): 去 Jetty —— 远端 import 句柄
   // 由进程级 UrmaJfrPool cache 持有,连接断开仅置空借用指针(I1/I4:
   // 连接零 URMA 对象创建/释放)
   res_.remote_fc_jetty = nullptr;
   res_.remote_jetty = nullptr;
   res_.remote_seg = nullptr;
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