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
      if (entry.is_fc) continue;  // 只注册 IO JFS 的 TX JFCE
      if (entry.fd < 0) continue;
      // edge-triggered: 先 rearm 再注册,避免错过已有 CQE
      auto* sj = jfs_pool->getIoJfsByIndex(entry.jfs_index);
      if (sj && sj->jfc) {
        urma_rearm_jfc(sj->jfc, false);
      }
      tx_cq_handlers_.push_back(
          std::make_unique<UrmaCqHandler>(evb_, entry.fd, this, true, false));
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

void UrmaSharedJfr::harvestTx() noexcept {
  // 委托给 UrmaJfrPool::harvestTxAndDispatch
  // 遍历所有 IO/FC JFS 的 TX JFCE,poll TX CQE(含 READ CQE)并派发
  if (auto* pool = UrmaJfrPool::get()) {
    pool->harvestTxAndDispatch();
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
