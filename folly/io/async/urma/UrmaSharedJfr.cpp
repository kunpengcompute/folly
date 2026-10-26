/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaSharedJfr.h>

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/EventBase.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaCqHandler.h>
#include <folly/io/async/urma/UrmaJfsPool.h>
#include <folly/logging/xlog.h>

namespace folly {

EventBaseLocal<std::unique_ptr<UrmaSharedJfr>>& UrmaSharedJfr::registry() {
  static EventBaseLocal<std::unique_ptr<UrmaSharedJfr>> r;
  return r;
}

UrmaSharedJfr* UrmaSharedJfr::getOrCreate(EventBase* evb) {
  auto* existing = get(evb);
  if (existing && existing->isValid()) {
    // 休眠态唤醒:新连接建立前先重新武装 CqHandler
    if (existing->dormant_.load(std::memory_order_acquire)) {
      existing->wakeup();
    }
    return existing;
  }
  if (existing) {
    registry().erase(*evb);
  }
  auto& up = registry().try_emplace_with(*evb, [evb]() {
    return std::make_unique<UrmaSharedJfr>(evb, UrmaInit::getConfig());
  });
  return up.get();
}

UrmaSharedJfr* UrmaSharedJfr::get(EventBase* evb) {
  auto* up = registry().get(*evb);
  return up ? up->get() : nullptr;
}

void UrmaSharedJfr::releaseIfEmpty(EventBase* evb) {
  if (!evb) return;
  auto* pool = UrmaJfrPool::get();
  if (!pool || !pool->socketMapEmpty()) {
    return;  // 路由表非空,仍有活跃连接
  }
  // 路由表为空:所有连接已断开,erase UrmaSharedJfr 触发析构
  // → 释放 CqHandler(epoll 注销),防止 JFCE fd 在 LT 模式下持续 spin 占 CPU
  XLOG(INFO) << "UrmaSharedJfr: releaseIfEmpty, socket_map empty, erasing from EventBaseLocal";
  registry().erase(*evb);
}

// ── 休眠管理 ──

void UrmaSharedJfr::incActiveConn() noexcept {
  // 休眠态唤醒:新连接建立前先重新武装 CqHandler
  if (dormant_.load(std::memory_order_acquire)) {
    wakeup();
  }
  active_conn_count_.fetch_add(1, std::memory_order_acq_rel);
}

void UrmaSharedJfr::decActiveConn() noexcept {
  uint32_t prev = active_conn_count_.fetch_sub(1, std::memory_order_acq_rel);
  if (prev == 1) {
    // 计数归零:本 EventBase 无活跃连接,休眠 CqHandler
    dormant();
  }
}

void UrmaSharedJfr::dormant() noexcept {
  // 先 drain 残留 CQE,减少 unregister 后内核 CQ 积压
  if (io_shared_jfr_) {
    UrmaJfrPool::get()->pollAndDispatch(io_shared_jfr_);
  }
  if (fc_shared_jfr_) {
    UrmaJfrPool::get()->pollFcAndDispatch(fc_shared_jfr_);
  }
  // drain TX CQE(含 READ CQE)
  if (auto* jfs_pool = UrmaJfsPool::get()) {
    uint32_t io_cnt = jfs_pool->ioJfsCount();
    for (uint32_t ji = 0; ji < io_cnt; ++ji) {
      harvestTx(ji);
    }
    uint32_t fc_cnt = jfs_pool->fcJfsCount();
    for (uint32_t fi = 0; fi < fc_cnt; ++fi) {
      harvestFcTx(fi);
    }
  }

  // 从 epoll 摘除所有 CqHandler(不 delete 对象,保留供 wakeup 复用)
  if (io_cq_handler_) {
    io_cq_handler_->unregisterHandler();
  }
  if (fc_cq_handler_) {
    fc_cq_handler_->unregisterHandler();
  }
  for (auto& h : tx_cq_handlers_) {
    if (h) h->unregisterHandler();
  }

  dormant_.store(true, std::memory_order_release);
  XLOG(INFO) << "UrmaSharedJfr: dormant (CqHandler unregistered), active_conn=0";
}

void UrmaSharedJfr::wakeup() noexcept {
  // 重新武装所有 CqHandler(从 epoll 摘除后重新注册)
  // registerHandler 内部先 event_del 再 event_add,可安全重复调用
  if (io_cq_handler_) {
    io_cq_handler_->registerHandler(EventHandler::READ | EventHandler::PERSIST);
    // rearm JFC 让 LT 模式下残留 CQE 立即触发
    if (io_shared_jfr_ && io_shared_jfr_->jfc) {
      urma_rearm_jfc(io_shared_jfr_->jfc, false);
    }
  }
  if (fc_cq_handler_) {
    fc_cq_handler_->registerHandler(EventHandler::READ | EventHandler::PERSIST);
    if (fc_shared_jfr_ && fc_shared_jfr_->jfc) {
      urma_rearm_jfc(fc_shared_jfr_->jfc, false);
    }
  }
  for (auto& h : tx_cq_handlers_) {
    if (h) {
      h->registerHandler(EventHandler::READ | EventHandler::PERSIST);
    }
  }

  dormant_.store(false, std::memory_order_release);
  XLOG(INFO) << "UrmaSharedJfr: wakeup (CqHandler re-registered)";
}

UrmaSharedJfr::UrmaSharedJfr(EventBase* evb, const UrmaConfig& /*config*/)
    : evb_(evb) {
  // Phase 4: 从进程级 UrmaJfrPool 选定 JFR(不再自己创建)
  UrmaJfrPool* pool = UrmaJfrPool::get();
  if (!pool) {
    XLOG(ERR) << "UrmaSharedJfr: UrmaJfrPool not initialized";
    return;
  }

  io_shared_jfr_ = pool->selectIoJfr(&io_jfr_index_);
  if (!io_shared_jfr_) {
    XLOG(ERR) << "UrmaSharedJfr: selectIoJfr failed";
    return;
  }

  fc_shared_jfr_ = pool->selectFcJfr(&fc_jfr_index_);
  if (!fc_shared_jfr_) {
    XLOG(ERR) << "UrmaSharedJfr: selectFcJfr failed";
    return;
  }

  // CqHandler: 注册选定的 JFR 的 JFCE fd 到 EventBase epoll
  io_cq_handler_ = new UrmaCqHandler(evb_, io_shared_jfr_->jfce_fd,
                                      this, false);
  urma_rearm_jfc(io_shared_jfr_->jfc, false);

  fc_cq_handler_ = new UrmaCqHandler(evb_, fc_shared_jfr_->jfce_fd,
                                      this, true);
  urma_rearm_jfc(fc_shared_jfr_->jfc, false);

  // credit_pool 从选定的 JFR 获取(非拥有,仅展示用)
  credit_idle_.store(io_shared_jfr_->credit_idle.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
  credit_allocated_.store(io_shared_jfr_->credit_allocated.load(std::memory_order_relaxed),
                          std::memory_order_relaxed);

  // TX CqHandler: 为所有 IO JFS 的 TX JFCE fd 注册 epoll
  // 让 READ CQE 有独立的事件驱动收割,不依赖 RX 事件
  // (多连接大包并发时,所有连接等 READ 完成无 RX 事件,
  //  harvestTxAndDispatch 不被触发,READ CQE 饿死)
  if (auto* jfs_pool = UrmaJfsPool::get()) {
    auto fds = jfs_pool->getAllJfceFds();
    tx_cq_handlers_.reserve(fds.size());
    int registered = 0;
    for (const auto& entry : fds) {
      // Phase 7: 注册所有 JFS(含 FC JFS)的 TX JFCE fd 到 epoll
      // FC JFS 的 TX CQE 也需要事件驱动收割,否则 FC 消息的 TX CQE 积压
      if (entry.fd < 0) continue;
      // edge-triggered: 先 rearm 再注册,避免错过已有 CQE
      SharedJfs* sj = entry.is_fc
                          ? jfs_pool->getFcJfsByIndex(entry.jfs_index)
                          : jfs_pool->getIoJfsByIndex(entry.jfs_index);
      if (sj && sj->jfc) {
        urma_rearm_jfc(sj->jfc, false);
      }
      tx_cq_handlers_.push_back(
          std::make_unique<UrmaCqHandler>(evb_, entry.fd, this,
                                          true, entry.is_fc, entry.jfs_index));
      ++registered;
    }
    XLOG(INFO) << "UrmaSharedJfr: registered " << registered
               << " TX JFCE fds to epoll";
  }

  valid_ = true;
  XLOG(INFO) << "UrmaSharedJfr: bound to io_jfr[" << io_jfr_index_
             << "] fc_jfr[" << fc_jfr_index_ << "]";
}

UrmaSharedJfr::~UrmaSharedJfr() {
  // 先清理 TX CqHandler(释放 epoll 注册)
  tx_cq_handlers_.clear();
  if (io_cq_handler_) {
    delete io_cq_handler_;
    io_cq_handler_ = nullptr;
  }
  if (fc_cq_handler_) {
    delete fc_cq_handler_;
    fc_cq_handler_ = nullptr;
  }
  // JFR/JFC/JFCE 由 UrmaJfrPool 管理,不在此删除
}

void UrmaSharedJfr::pollAndDispatch() noexcept {
  // 委托给 UrmaJfrPool::pollAndDispatch(selected io jfr)
  if (io_shared_jfr_) {
    UrmaJfrPool::get()->pollAndDispatch(io_shared_jfr_);
  }
}

void UrmaSharedJfr::pollFcAndDispatch() noexcept {
  if (fc_shared_jfr_) {
    UrmaJfrPool::get()->pollFcAndDispatch(fc_shared_jfr_);
  }
}

void UrmaSharedJfr::harvestTx(uint32_t jfs_index) noexcept {
  // 单点收割: 只 poll 指定 JFS 的 TX CQE(含 READ CQE)
  if (auto* pool = UrmaJfrPool::get()) {
    pool->harvestTxAndDispatch(jfs_index);
  }
}

void UrmaSharedJfr::harvestFcTx(uint32_t jfs_index) noexcept {
  // FC JFS TX CQE 收割(Phase 7)
  if (auto* pool = UrmaJfrPool::get()) {
    pool->harvestFcTxAndDispatch();
  }
}

void UrmaSharedJfr::recharge() noexcept {
  if (io_shared_jfr_) {
    UrmaJfrPool::get()->recharge(io_shared_jfr_);
  }
}

void UrmaSharedJfr::registerSocket(uint64_t socket_id, UrmaAsyncSocket* socket) {
  if (auto* pool = UrmaJfrPool::get()) {
    pool->registerSocket(socket_id, socket);
  }
}

void UrmaSharedJfr::unregisterSocket(uint64_t socket_id) {
  if (auto* pool = UrmaJfrPool::get()) {
    pool->unregisterSocket(socket_id);
  }
}

void UrmaSharedJfr::registerFcSocket(uint64_t socket_id, UrmaAsyncSocket* socket) {
  if (auto* pool = UrmaJfrPool::get()) {
    pool->registerFcSocket(socket_id, socket);
  }
}

void UrmaSharedJfr::unregisterFcSocket(uint64_t socket_id) {
  if (auto* pool = UrmaJfrPool::get()) {
    pool->unregisterFcSocket(socket_id);
  }
}

urma_jfr_t* UrmaSharedJfr::ioJfr() const noexcept {
  return io_shared_jfr_ ? io_shared_jfr_->jfr : nullptr;
}

urma_jfc_t* UrmaSharedJfr::ioJfc() const noexcept {
  return io_shared_jfr_ ? io_shared_jfr_->jfc : nullptr;
}

urma_jfr_t* UrmaSharedJfr::fcJfr() const noexcept {
  return fc_shared_jfr_ ? fc_shared_jfr_->jfr : nullptr;
}

urma_jfc_t* UrmaSharedJfr::fcJfc() const noexcept {
  return fc_shared_jfr_ ? fc_shared_jfr_->jfc : nullptr;
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
