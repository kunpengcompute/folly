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

  valid_ = true;
  XLOG(INFO) << "UrmaSharedJfr: bound to io_jfr[" << io_jfr_index_
             << "] fc_jfr[" << fc_jfr_index_ << "]";
}

UrmaSharedJfr::~UrmaSharedJfr() {
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

void UrmaSharedJfr::recharge() noexcept {
  if (io_shared_jfr_) {
    UrmaJfrPool::get()->recharge(io_shared_jfr_);
  }
}

void UrmaSharedJfr::registerSocket(uint32_t jetty_id, UrmaAsyncSocket* socket) {
  if (auto* pool = UrmaJfrPool::get()) {
    pool->registerSocket(jetty_id, socket);
  }
}

void UrmaSharedJfr::unregisterSocket(uint32_t jetty_id) {
  if (auto* pool = UrmaJfrPool::get()) {
    pool->unregisterSocket(jetty_id);
  }
}

void UrmaSharedJfr::registerFcSocket(uint32_t fc_jetty_id, UrmaAsyncSocket* socket) {
  if (auto* pool = UrmaJfrPool::get()) {
    pool->registerFcSocket(fc_jetty_id, socket);
  }
}

void UrmaSharedJfr::unregisterFcSocket(uint32_t fc_jetty_id) {
  if (auto* pool = UrmaJfrPool::get()) {
    pool->unregisterFcSocket(fc_jetty_id);
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
