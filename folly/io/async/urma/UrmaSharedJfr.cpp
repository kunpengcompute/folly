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
  // 旧的无效或不存在,删除后重新创建
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

UrmaSharedJfr::UrmaSharedJfr(EventBase* evb, const UrmaConfig& config)
    : evb_(evb), config_(config) {
  urma_context_t* ctx = UrmaInit::getContext();
  if (!ctx) {
    XLOG(ERR) << "UrmaSharedJfr: UrmaInit not initialized";
    return;
  }

  // ── IO 组 ──
  // IO JFCE (先创建,JFC 创建时指定)
  io_jfce_ = urma_create_jfce(ctx);
  if (!io_jfce_) {
    XLOG(ERR) << "UrmaSharedJfr: create io_jfce failed";
    return;
  }

  // IO JFC (指定 jfce)
  urma_jfc_cfg_t io_jfc_cfg = {};
  io_jfc_cfg.depth = config_.rx_depth;
  io_jfc_cfg.jfce = io_jfce_;
  io_jfc_ = urma_create_jfc(ctx, &io_jfc_cfg);
  if (!io_jfc_) {
    XLOG(ERR) << "UrmaSharedJfr: create io_jfc failed";
    return;  // 析构清理 io_jfce_
  }

  // IO JFR
  urma_jfr_cfg_t io_jfr_cfg = {};
  io_jfr_cfg.depth = config_.rx_depth;
  io_jfr_cfg.trans_mode = URMA_TM_RM;
  io_jfr_cfg.max_sge = 1;  // 每个 recv WR 用 1 个 SGE(参照 brpc)
  io_jfr_cfg.min_rnr_timer = URMA_DEFAULT_MIN_RNR_TIMER;
  io_jfr_cfg.jfc = io_jfc_;
  io_jfr_ = urma_create_jfr(ctx, &io_jfr_cfg);
  if (!io_jfr_) {
    XLOG(ERR) << "UrmaSharedJfr: create io_jfr failed";
    return;  // 析构清理 io_jfc_ + io_jfce_
  }

  // IO RX buffer 池: rx_depth 个 4KB buffer
  rx_bufs_.resize(config_.rx_depth);
  for (uint32_t i = 0; i < config_.rx_depth; ++i) {
    rx_bufs_[i].data = UrmaInit::poolAlloc(URMA_SMALL_BUF_SIZE);
    if (!rx_bufs_[i].data) {
      XLOG(ERR) << "UrmaSharedJfr: rx_buf alloc failed at " << i;
      rx_bufs_.resize(i);  // 截断到已分配的部分,析构清理
      return;  // 析构清理 rx_bufs_[0..i-1] + io_jfr_ + io_jfc_ + io_jfce_
    }

    urma_jfr_wr_t* wr = &rx_bufs_[i].wr;
    rx_bufs_[i].sge.addr = reinterpret_cast<uint64_t>(rx_bufs_[i].data);
    rx_bufs_[i].sge.len = URMA_SMALL_BUF_SIZE;
    rx_bufs_[i].sge.tseg = UrmaInit::getPoolTseg();
    wr->src.sge = &rx_bufs_[i].sge;
    wr->src.num_sge = 1;
    wr->user_ctx = i;  // slot 索引
    wr->next = nullptr;
    urma_jfr_wr_t* bad_wr = nullptr;
    urma_status_t ret = urma_post_jfr_wr(io_jfr_, wr, &bad_wr);
    if (ret != URMA_SUCCESS) {
      XLOG(ERR) << "UrmaSharedJfr: post_jfr_wr failed at slot " << i
                << ", status=" << ret;
      UrmaInit::poolFree(rx_bufs_[i].data);
      rx_bufs_[i].data = nullptr;
      rx_bufs_.resize(i);  // 截断,析构清理已分配的
      urma_delete_jfr(io_jfr_);
      io_jfr_ = nullptr;
      urma_delete_jfc(io_jfc_);
      io_jfc_ = nullptr;
      urma_delete_jfce(io_jfce_);
      io_jfce_ = nullptr;
      return;
    }
  }

  // IO CqHandler
  io_cq_handler_ = new UrmaCqHandler(evb_, io_jfce_->fd,
                                      this, false);
  urma_rearm_jfc(io_jfc_, false);

  // ── FC 组 ──
  // FC JFCE (先创建)
  fc_jfce_ = urma_create_jfce(ctx);
  if (!fc_jfce_) {
    XLOG(ERR) << "UrmaSharedJfr: create fc_jfce failed";
    return;  // 析构清理 IO 组
  }

  // FC JFC (指定 jfce)
  urma_jfc_cfg_t fc_jfc_cfg = {};
  fc_jfc_cfg.depth = config_.fc_rx_depth;
  fc_jfc_cfg.jfce = fc_jfce_;
  fc_jfc_ = urma_create_jfc(ctx, &fc_jfc_cfg);
  if (!fc_jfc_) {
    XLOG(ERR) << "UrmaSharedJfr: create fc_jfc failed";
    return;  // 析构清理 fc_jfce_ + IO 组
  }

  urma_jfr_cfg_t fc_jfr_cfg = {};
  fc_jfr_cfg.depth = config_.fc_rx_depth;
  fc_jfr_cfg.trans_mode = URMA_TM_RM;
  fc_jfr_cfg.max_sge = 1;  // FC recv WR 用 1 个 SGE
  fc_jfr_cfg.min_rnr_timer = URMA_DEFAULT_MIN_RNR_TIMER;
  fc_jfr_cfg.jfc = fc_jfc_;
  fc_jfr_ = urma_create_jfr(ctx, &fc_jfr_cfg);
  if (!fc_jfr_) {
    XLOG(ERR) << "UrmaSharedJfr: create fc_jfr failed";
    return;  // 析构清理 fc_jfc_ + fc_jfce_ + IO 组
  }

  // FC RX buffer: Phase 2 空实现(Phase 4 填充)
  fc_cq_handler_ = new UrmaCqHandler(evb_, fc_jfce_->fd,
                                      this, true);
  urma_rearm_jfc(fc_jfc_, false);

  // credit_pool 桩初始化
  credit_idle_.store(config_.rx_depth);
  credit_allocated_.store(0);

  valid_ = true;  // 全部资源创建成功
  XLOG(INFO) << "UrmaSharedJfr: initialized, rx_depth=" << config_.rx_depth
             << " fc_rx_depth=" << config_.fc_rx_depth;
}

UrmaSharedJfr::~UrmaSharedJfr() {
  // 参照 brpc urma_helper.cpp:624-628:
  // 不在析构中删除 URMA 硬件资源(JFR/JFC/JFCE/jetty)
  // 原因:
  // 1. jetty 可能仍引用 JFR(share_jfr=1),urma_delete_jfr 会阻塞等待解绑
  // 2. urma_delete_jfc 会阻塞等待所有 CQE 被消费
  // 3. 进程退出时内核自动回收所有 URMA 资源
  // 仅清理 CqHandler(从 EventBase epoll 注销)
  if (io_cq_handler_) {
    delete io_cq_handler_;
    io_cq_handler_ = nullptr;
  }
  if (fc_cq_handler_) {
    delete fc_cq_handler_;
    fc_cq_handler_ = nullptr;
  }
  // JFR/JFC/JFCE 不删除——依赖进程退出时内核回收
  // RX buffer 也不释放——MR 内存池在进程退出时由 munmap 回收
}

void UrmaSharedJfr::replenishRxBuf(uint32_t slot) noexcept {
  if (slot >= rx_bufs_.size()) return;
  urma_jfr_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfr_wr(io_jfr_, &rx_bufs_[slot].wr, &bad_wr);
  if (ret != URMA_SUCCESS) {
    XLOG(WARN) << "UrmaSharedJfr: replenishRxBuf post failed at slot "
               << slot << ", status=" << ret;
  }
}

void UrmaSharedJfr::fillFcRxBuf() noexcept {
  // Phase 4: 补充 FC RX buffer
}

void UrmaSharedJfr::pollAndDispatch() noexcept {
  // handlerReady 被 epoll 唤醒后调用,无需 wait
  urma_jfc_t* jfcs[1] = {io_jfc_};
  uint32_t nevents[1] = {1};
  urma_ack_jfc(jfcs, nevents, 1);
  urma_cr_t cr[POLL_BATCH_MAX];
  int n;
  // 循环 poll 直到 CQE 全部收割
  while ((n = urma_poll_jfc(io_jfc_, POLL_BATCH_MAX, cr)) > 0) {
    for (int i = 0; i < n; ++i) {
      uint32_t slot = static_cast<uint32_t>(cr[i].user_ctx);
      auto it = socket_map_.find(cr[i].remote_id.id);
      if (it == socket_map_.end()) {
        replenishRxBuf(slot);
        continue;
      }
      UrmaAsyncSocket* socket = it->second;
      auto data = IOBuf::takeOwnership(
          rx_bufs_[slot].data, cr[i].completion_len,
          [](void* /*buf*/, void* userData) {
            auto* self = static_cast<UrmaSharedJfr*>(
                reinterpret_cast<void*>(
                    reinterpret_cast<uintptr_t>(userData) >> 16));
            uint32_t s = static_cast<uint32_t>(
                reinterpret_cast<uintptr_t>(userData) & 0xFFFF);
            self->replenishRxBuf(s);
            self->recharge();
          },
          reinterpret_cast<void*>(
              (reinterpret_cast<uintptr_t>(this) << 16) | slot));
      socket->onRxBuf(std::move(data));
    }
  }
  urma_rearm_jfc(io_jfc_, false);
}

void UrmaSharedJfr::pollFcAndDispatch() noexcept {
  // Phase 4: 完整 FC 消息处理
  urma_jfc_t* jfcs[1] = {fc_jfc_};
  uint32_t nevents[1] = {1};
  urma_ack_jfc(jfcs, nevents, 1);
  urma_cr_t cr[POLL_BATCH_MAX];
  int n;
  while ((n = urma_poll_jfc(fc_jfc_, POLL_BATCH_MAX, cr)) > 0) {
    (void)n;  // Phase 4 处理 FC CQE
  }
  urma_rearm_jfc(fc_jfc_, false);
}

void UrmaSharedJfr::registerSocket(uint32_t jetty_id, UrmaAsyncSocket* socket) {
  socket_map_[jetty_id] = socket;
}

void UrmaSharedJfr::unregisterSocket(uint32_t jetty_id) {
  socket_map_.erase(jetty_id);
}

void UrmaSharedJfr::registerFcSocket(uint32_t fc_jetty_id, UrmaAsyncSocket* socket) {
  fc_socket_map_[fc_jetty_id] = socket;
}

void UrmaSharedJfr::unregisterFcSocket(uint32_t fc_jetty_id) {
  fc_socket_map_.erase(fc_jetty_id);
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
