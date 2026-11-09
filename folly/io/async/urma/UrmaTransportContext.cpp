/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaTransportContext.h>

#ifdef FOLLY_HAVE_URMA

#include <algorithm>
#include <cstring>

#include <glog/logging.h>

#include <folly/io/async/EventBase.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaCqHandler.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <folly/logging/xlog.h>

namespace folly {

// 任务 C:回收栈单轮重投批量上限
static constexpr uint32_t kRepostBatchMax = 64;

// 诊断打点:单调微秒时钟
static uint64_t diag_now_us() noexcept {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL + ts.tv_nsec / 1000;
}

void UrmaTransportContext::logDiagIfDue() noexcept {
  uint64_t now = diag_now_us();
  uint64_t last = diag_.last_log_us.load(std::memory_order_relaxed);
  if (now - last < 1000000) return;  // 1s 最多打一次
  if (!diag_.last_log_us.compare_exchange_strong(last, now)) return;
  uint64_t rx_ready = diag_.handler_ready_io_rx.load(std::memory_order_relaxed);
  uint64_t tx_ready = diag_.handler_ready_io_tx.load(std::memory_order_relaxed);
  uint64_t d_rx = rx_ready - diag_.last_handler_rx;
  uint64_t d_tx = tx_ready - diag_.last_handler_tx;
  diag_.last_handler_rx = rx_ready;
  diag_.last_handler_tx = tx_ready;
  XLOG(INFO) << "[URMA-DIAG] io_rx_ready=" << rx_ready << "(+" << d_rx << ")"
             << " io_tx_ready=" << tx_ready << "(+" << d_tx << ")"
             << " wait_le0=" << diag_.wait_le0_skip.load()
             << " rx_cqe=" << diag_.poll_rx_cqe.load()
             << " tx_cqe=" << diag_.poll_tx_cqe.load()
             << " rearm_fail=" << diag_.rearm_fail.load()
             << " repost_tick=" << diag_.repost_timer_tick.load();
}

// ── per-EVB registry ──

EventBaseLocal<std::unique_ptr<UrmaTransportContext>>&
UrmaTransportContext::registry() {
  static EventBaseLocal<std::unique_ptr<UrmaTransportContext>> r;
  return r;
}

UrmaTransportContext* UrmaTransportContext::getOrCreate(EventBase* evb) {
  auto* existing = get(evb);
  if (existing && existing->isValid()) {
    if (existing->dormant_.load(std::memory_order_acquire)) {
      existing->wakeup();
    }
    return existing;
  }
  if (existing) {
    registry().erase(*evb);
  }
  auto& up = registry().try_emplace_with(*evb, [evb]() {
    return std::make_unique<UrmaTransportContext>(evb, UrmaInit::getConfig());
  });
  return up.get();
}

UrmaTransportContext* UrmaTransportContext::get(EventBase* evb) {
  auto* up = registry().get(*evb);
  return up ? up->get() : nullptr;
}

// ── 四件套创建 ──

bool UrmaTransportContext::createIoJfs(urma_context_t* ctx,
                                        const UrmaConfig& config) {
  // TX 独立 JFCE
  urma_jfce_t* tx_jfce = urma_create_jfce(ctx);
  if (!tx_jfce) { XLOG(ERR) << "UrmaTransportContext: create io tx jfce failed"; return false; }
  io_jfs_.tx_jfce = tx_jfce;
  io_jfs_.tx_jfce_fd = tx_jfce->fd;

  // TX 独立 JFC(depth = tx_depth + 1 reserved for flush_done)
  urma_jfc_cfg_t jfc_cfg = {};
  jfc_cfg.depth = config.tx_depth + kUrmaTxJfcReservedSlots;
  jfc_cfg.jfce = tx_jfce;
  io_jfs_.tx_jfc = urma_create_jfc(ctx, &jfc_cfg);
  if (!io_jfs_.tx_jfc) {
    XLOG(ERR) << "UrmaTransportContext: create io tx jfc failed";
    urma_delete_jfce(tx_jfce); io_jfs_.tx_jfce = nullptr;
    return false;
  }

  urma_jfs_cfg_t jfs_cfg = {};
  jfs_cfg.depth = config.tx_depth;
  jfs_cfg.trans_mode = URMA_TM_RM;
  jfs_cfg.max_sge = UrmaInit::getMaxJfsSge();
  jfs_cfg.max_inline_data = UrmaInit::getMaxJfsInlineLen();
  jfs_cfg.rnr_retry = URMA_DEFAULT_RNR_RETRY;
  jfs_cfg.err_timeout = URMA_DEFAULT_ERR_TIMEOUT;
  jfs_cfg.jfc = io_jfs_.tx_jfc;
  io_jfs_.jfs = urma_create_jfs(ctx, &jfs_cfg);
  if (!io_jfs_.jfs) {
    XLOG(ERR) << "UrmaTransportContext: create io jfs failed";
    urma_delete_jfc(io_jfs_.tx_jfc); io_jfs_.tx_jfc = nullptr;
    urma_delete_jfce(tx_jfce); io_jfs_.tx_jfce = nullptr;
    return false;
  }
  io_jfs_.sq_depth = config.tx_depth;
  io_jfs_.sq_inflight = 0;
  return true;
}

bool UrmaTransportContext::createFcJfs(urma_context_t* ctx,
                                        const UrmaConfig& config) {
  // TX 独立 JFCE
  urma_jfce_t* tx_jfce = urma_create_jfce(ctx);
  if (!tx_jfce) { XLOG(ERR) << "UrmaTransportContext: create fc tx jfce failed"; return false; }
  fc_jfs_.tx_jfce = tx_jfce;
  fc_jfs_.tx_jfce_fd = tx_jfce->fd;

  // TX 独立 JFC
  urma_jfc_cfg_t jfc_cfg = {};
  jfc_cfg.depth = config.fc_tx_depth + kUrmaTxJfcReservedSlots;
  jfc_cfg.jfce = tx_jfce;
  fc_jfs_.tx_jfc = urma_create_jfc(ctx, &jfc_cfg);
  if (!fc_jfs_.tx_jfc) {
    XLOG(ERR) << "UrmaTransportContext: create fc tx jfc failed";
    urma_delete_jfce(tx_jfce); fc_jfs_.tx_jfce = nullptr;
    return false;
  }

  urma_jfs_cfg_t jfs_cfg = {};
  jfs_cfg.depth = config.fc_tx_depth;
  jfs_cfg.trans_mode = URMA_TM_RM;
  jfs_cfg.max_sge = 1;
  jfs_cfg.max_inline_data = UrmaInit::getMaxJfsInlineLen();
  jfs_cfg.rnr_retry = URMA_DEFAULT_RNR_RETRY;
  jfs_cfg.err_timeout = URMA_DEFAULT_ERR_TIMEOUT;
  jfs_cfg.jfc = fc_jfs_.tx_jfc;
  fc_jfs_.jfs = urma_create_jfs(ctx, &jfs_cfg);
  if (!fc_jfs_.jfs) {
    XLOG(ERR) << "UrmaTransportContext: create fc jfs failed";
    urma_delete_jfc(fc_jfs_.tx_jfc); fc_jfs_.tx_jfc = nullptr;
    urma_delete_jfce(tx_jfce); fc_jfs_.tx_jfce = nullptr;
    return false;
  }
  fc_jfs_.sq_depth = config.fc_tx_depth;
  fc_jfs_.sq_inflight = 0;
  return true;
}

bool UrmaTransportContext::createIoJfr(urma_context_t* ctx,
                                        const UrmaConfig& config) {
  io_jfr_.is_fc = false;
  io_jfr_.rx_depth = config.rx_depth;
  io_jfr_.watermark.min_water = config.min_water;
  io_jfr_.watermark.max_water = config.max_water;
  io_jfr_.watermark.batch_post = config.batch_post;

  // RX 独立 JFCE
  urma_jfce_t* rx_jfce = urma_create_jfce(ctx);
  if (!rx_jfce) { XLOG(ERR) << "UrmaTransportContext: create io rx jfce failed"; return false; }
  io_jfr_.rx_jfce = rx_jfce;
  io_jfr_.rx_jfce_fd = rx_jfce->fd;

  // RX 独立 JFC(depth = rx_depth + kRepostBatchMax 余量)
  urma_jfc_cfg_t jfc_cfg = {};
  jfc_cfg.depth = config.rx_depth + kRepostBatchMax;
  jfc_cfg.jfce = rx_jfce;
  io_jfr_.rx_jfc = urma_create_jfc(ctx, &jfc_cfg);
  if (!io_jfr_.rx_jfc) {
    XLOG(ERR) << "UrmaTransportContext: create io rx jfc failed";
    urma_delete_jfce(rx_jfce); io_jfr_.rx_jfce = nullptr;
    return false;
  }

  // JFR 绑定 RX 独立 JFC
  urma_jfr_cfg_t jfr_cfg = {};
  jfr_cfg.depth = config.rx_depth;
  jfr_cfg.trans_mode = URMA_TM_RM;
  jfr_cfg.max_sge = 1;
  jfr_cfg.min_rnr_timer = URMA_DEFAULT_MIN_RNR_TIMER;
  jfr_cfg.jfc = io_jfr_.rx_jfc;
  io_jfr_.jfr = urma_create_jfr(ctx, &jfr_cfg);
  if (!io_jfr_.jfr) {
    XLOG(ERR) << "UrmaTransportContext: create io jfr failed";
    urma_delete_jfc(io_jfr_.rx_jfc); io_jfr_.rx_jfc = nullptr;
    urma_delete_jfce(rx_jfce); io_jfr_.rx_jfce = nullptr;
    return false;
  }

  if (!initRxBufPool(&io_jfr_, io_jfr_.watermark.max_water)) {
    XLOG(ERR) << "UrmaTransportContext: init io rx buf pool failed";
    return false;
  }

  io_jfr_.credit_idle = config.rx_depth;
  io_jfr_.credit_allocated = 0;
  return true;
}

bool UrmaTransportContext::createFcJfr(urma_context_t* ctx,
                                        const UrmaConfig& config) {
  fc_jfr_.is_fc = true;
  fc_jfr_.rx_depth = config.fc_rx_depth;
  // FC 水位:SDD §2 fc_min=8, fc_max=32
  fc_jfr_.watermark.min_water = 8;
  fc_jfr_.watermark.max_water = 32;
  fc_jfr_.watermark.batch_post = 8;

  // RX 独立 JFCE
  urma_jfce_t* rx_jfce = urma_create_jfce(ctx);
  if (!rx_jfce) { XLOG(ERR) << "UrmaTransportContext: create fc rx jfce failed"; return false; }
  fc_jfr_.rx_jfce = rx_jfce;
  fc_jfr_.rx_jfce_fd = rx_jfce->fd;

  // RX 独立 JFC
  urma_jfc_cfg_t jfc_cfg = {};
  jfc_cfg.depth = config.fc_rx_depth + kRepostBatchMax;
  jfc_cfg.jfce = rx_jfce;
  fc_jfr_.rx_jfc = urma_create_jfc(ctx, &jfc_cfg);
  if (!fc_jfr_.rx_jfc) {
    XLOG(ERR) << "UrmaTransportContext: create fc rx jfc failed";
    urma_delete_jfce(rx_jfce); fc_jfr_.rx_jfce = nullptr;
    return false;
  }

  urma_jfr_cfg_t jfr_cfg = {};
  jfr_cfg.depth = config.fc_rx_depth;
  jfr_cfg.trans_mode = URMA_TM_RM;
  jfr_cfg.max_sge = 1;
  jfr_cfg.min_rnr_timer = URMA_DEFAULT_MIN_RNR_TIMER;
  jfr_cfg.jfc = fc_jfr_.rx_jfc;
  fc_jfr_.jfr = urma_create_jfr(ctx, &jfr_cfg);
  if (!fc_jfr_.jfr) {
    XLOG(ERR) << "UrmaTransportContext: create fc jfr failed";
    urma_delete_jfc(fc_jfr_.rx_jfc); fc_jfr_.rx_jfc = nullptr;
    urma_delete_jfce(rx_jfce); fc_jfr_.rx_jfce = nullptr;
    return false;
  }

  if (!initRxBufPool(&fc_jfr_, fc_jfr_.watermark.max_water)) {
    XLOG(ERR) << "UrmaTransportContext: init fc rx buf pool failed";
    return false;
  }
  return true;
}

// ── recv buffer 池初始化 ──
bool UrmaTransportContext::initRxBufPool(EvbJfr* jfr, uint32_t max_bufs) {
  jfr->rx_bufs.resize(max_bufs);
  jfr->recycle = std::make_unique<UrmaRecycleStack>();
  jfr->recycle->init(max_bufs);
  jfr->held_count.store(0, std::memory_order_relaxed);
  jfr->transit_cnt.store(0, std::memory_order_relaxed);
  for (uint32_t i = 0; i < max_bufs; ++i) {
    jfr->rx_bufs[i].data = UrmaInit::poolAlloc(URMA_SMALL_BUF_SIZE);
    if (!jfr->rx_bufs[i].data) {
      XLOG(ERR) << "UrmaTransportContext: rx_buf alloc failed at " << i;
      for (uint32_t j = 0; j < i; ++j) {
        UrmaInit::poolFree(jfr->rx_bufs[j].data);
        jfr->rx_bufs[j].data = nullptr;
      }
      jfr->rx_bufs.clear();
      jfr->recycle.reset();
      return false;
    }
    jfr->rx_bufs[i].sge.addr = reinterpret_cast<uint64_t>(jfr->rx_bufs[i].data);
    jfr->rx_bufs[i].sge.len = URMA_SMALL_BUF_SIZE;
    jfr->rx_bufs[i].sge.tseg = UrmaInit::getPoolTseg();
    jfr->rx_bufs[i].wr.src.sge = &jfr->rx_bufs[i].sge;
    jfr->rx_bufs[i].wr.src.num_sge = 1;
    jfr->rx_bufs[i].wr.user_ctx = i;
    jfr->rx_bufs[i].wr.next = nullptr;
    jfr->rx_bufs[i].ctx.ctx = this;
    jfr->rx_bufs[i].ctx.jfr = jfr;
    jfr->rx_bufs[i].ctx.slot = i;
  }
  uint32_t to_post = jfr->watermark.min_water;
  for (uint32_t i = 0; i < to_post; ++i) {
    if (!postRecvWr(jfr, i)) {
      XLOG(ERR) << "UrmaTransportContext: initial postRecvWr failed at " << i;
      return false;
    }
  }
  jfr->recv_posted = to_post;
  jfr->next_free_slot = to_post;
  XLOG(INFO) << "UrmaTransportContext: rx buf pool init, max=" << max_bufs
             << " pre_posted=" << to_post << " is_fc=" << jfr->is_fc;
  return true;
}

// ── post recv WR ──
bool UrmaTransportContext::postRecvWr(EvbJfr* jfr, uint32_t slot) {
  if (slot >= jfr->rx_bufs.size()) return false;
  jfr->rx_bufs[slot].wr.next = nullptr;
  urma_jfr_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfr_wr(jfr->jfr, &jfr->rx_bufs[slot].wr, &bad_wr);
  if (ret != URMA_SUCCESS) {
    XLOG(ERR) << "UrmaTransportContext: postRecvWr failed slot=" << slot
              << " status=" << ret
              << " jfr_depth=" << jfr->rx_depth
              << " recv_posted=" << jfr->recv_posted;
    return false;
  }
  return true;
}

bool UrmaTransportContext::postRecvWrBatch(EvbJfr* jfr, const uint32_t* slots,
                                            uint32_t count) {
  if (count == 0) return true;
  urma_jfr_wr_t* head = nullptr;
  urma_jfr_wr_t* tail = nullptr;
  for (uint32_t i = 0; i < count; ++i) {
    uint32_t slot = slots[i];
    if (slot >= jfr->rx_bufs.size()) continue;
    urma_jfr_wr_t* wr = &jfr->rx_bufs[slot].wr;
    wr->next = nullptr;
    if (!head) { head = wr; tail = wr; }
    else { tail->next = wr; tail = wr; }
  }
  if (!head) return true;
  urma_jfr_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfr_wr(jfr->jfr, head, &bad_wr);
  return ret == URMA_SUCCESS;
}

uint32_t UrmaTransportContext::postRecvWrBatchCount(EvbJfr* jfr,
                                                     const uint32_t* slots,
                                                     uint32_t count) noexcept {
  if (count == 0) return 0;
  urma_jfr_wr_t* head = nullptr;
  urma_jfr_wr_t* tail = nullptr;
  urma_jfr_wr_t* nodes[kRepostBatchMax];
  uint32_t node_cnt = 0;
  for (uint32_t i = 0; i < count && node_cnt < kRepostBatchMax; ++i) {
    uint32_t slot = slots[i];
    if (slot >= jfr->rx_bufs.size()) continue;
    urma_jfr_wr_t* wr = &jfr->rx_bufs[slot].wr;
    wr->next = nullptr;
    nodes[node_cnt++] = wr;
    if (!head) { head = wr; tail = wr; }
    else { tail->next = wr; tail = wr; }
  }
  if (!head) return 0;
  urma_jfr_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfr_wr(jfr->jfr, head, &bad_wr);
  if (ret == URMA_SUCCESS) return node_cnt;
  if (!bad_wr) return 0;
  for (uint32_t i = 0; i < node_cnt; ++i) {
    if (nodes[i] == bad_wr) return i;
  }
  return 0;
}

// ── RX slot 回收(I2 豁免:deleter 任意线程) ──
void UrmaTransportContext::releaseHeldSlot(UrmaTransportContext* /*ctx*/,
                                            EvbJfr* jfr,
                                            uint32_t slot) noexcept {
  if (!jfr || !jfr->recycle) return;
  jfr->recycle->push(slot);
  jfr->held_count.fetch_sub(1, std::memory_order_acq_rel);
}

void UrmaTransportContext::recycleRxSlot(EvbJfr* jfr, uint32_t slot) noexcept {
  if (!jfr || !jfr->recycle) return;
  jfr->recycle->push(slot);
  jfr->transit_cnt.fetch_sub(1, std::memory_order_acq_rel);
}

// ── repostRecycled(same-thread,I2 无锁,简单批量补投) ──
void UrmaTransportContext::repostRecycled(EvbJfr* jfr) noexcept {
  if (!jfr || !jfr->recycle || !jfr->jfr) return;
  uint32_t slots[kRepostBatchMax];

  // 路径 1:回收栈有 slot → 直接批量补投
  while (jfr->recv_posted < jfr->watermark.max_water) {
    uint32_t room = jfr->watermark.max_water - jfr->recv_posted;
    uint32_t want = std::min(room, kRepostBatchMax);
    uint32_t cnt = jfr->recycle->drain(slots, want);
    if (cnt == 0) break;  // 回收栈空,转路径 2
    // same-thread:直接加减,无 CAS(I2)
    jfr->recv_posted += cnt;
    uint32_t ok = postRecvWrBatchCount(jfr, slots, cnt);
    if (ok < cnt) {
      jfr->recv_posted -= (cnt - ok);
      for (uint32_t i = ok; i < cnt; ++i) jfr->recycle->push(slots[i]);
      break;
    }
  }

  // 路径 2:回收栈空,游标取新 slot 补充水位
  if (jfr->recv_posted >= jfr->watermark.min_water) return;
  uint32_t room = jfr->watermark.max_water - jfr->recv_posted;
  // in_flight 统计(transit_cnt/held_count 仍需 atomic load,deleter 任意线程 → I2 豁免)
  uint32_t in_flight = jfr->recv_posted +
                       jfr->transit_cnt.load(std::memory_order_acquire) +
                       jfr->held_count.load(std::memory_order_acquire) +
                       jfr->recycle->size();
  uint32_t budget = jfr->watermark.max_water > in_flight
                        ? jfr->watermark.max_water - in_flight : 0;
  uint32_t supply = std::min({room, budget, kRepostBatchMax});
  if (supply == 0) return;
  uint32_t n = 0;
  uint32_t cursor = jfr->next_free_slot;
  while (n < supply && cursor < jfr->rx_bufs.size()) {
    slots[n++] = cursor++;
  }
  if (n == 0) return;
  jfr->next_free_slot = cursor;  // same-thread:直接赋值(I2)
  jfr->recv_posted += n;         // same-thread:直接加(I2)
  uint32_t ok = postRecvWrBatchCount(jfr, slots, n);
  if (ok < n) {
    jfr->recv_posted -= (n - ok);
    for (uint32_t i = ok; i < n; ++i) jfr->recycle->push(slots[i]);
  }
}

// ── checkRxAccounting(same-thread) ──
void UrmaTransportContext::checkRxAccounting(EvbJfr* jfr) noexcept {
  if (!jfr || !jfr->recycle) return;
  uint32_t circulating = std::max(jfr->watermark.min_water, jfr->next_free_slot);
  constexpr uint32_t kSlack = kRepostBatchMax + 16;
  uint32_t sum = jfr->recv_posted +
                 jfr->transit_cnt.load(std::memory_order_acquire) +
                 jfr->held_count.load(std::memory_order_acquire) +
                 jfr->recycle->size();
  int64_t delta = static_cast<int64_t>(sum) - static_cast<int64_t>(circulating);
  if (delta == 0) return;
  int64_t abs_delta = std::abs(delta);
  if (abs_delta <= static_cast<int64_t>(kSlack)) return;
  static thread_local uint32_t bad_streak = 0;
  if (++bad_streak >= 5) {
    bad_streak = 0;
    XLOG(ERR) << "[URMA] RX accounting invariant VIOLATED: "
              << "recv_posted=" << jfr->recv_posted
              << " transit=" << jfr->transit_cnt.load()
              << " held=" << jfr->held_count.load()
              << " recycle=" << jfr->recycle->size()
              << " sum=" << sum << " circulating=" << circulating
              << " is_fc=" << jfr->is_fc;
  }
}

// ── 统一收割分派器(任务 B,I3) ──
// wait→poll→ack→rearm→poll→ack 五步(URMA 官方中断模式)
// LT 模式(libevent 2.x setEdgeTriggered 静默失败)下必须保留 wait/ack,
// 否则 JFCE fd 恒可读导致 epoll 空轮询 CPU 100%。
// 第⑤步二轮 poll 防 rearm 窗口期 CQE 漏收割。
// is_fc: IO/FC 选择; is_tx: TX/RX JFC 选择
void UrmaTransportContext::pollAndDispatch(bool is_fc, bool is_tx) noexcept {
  // dormant 后跳过(dormant_.store 在 runInLoop 摘除 CqHandler 之前,
  // 此窗口期 epoll 可能仍有残留触发)
  if (dormant_.load(std::memory_order_acquire)) return;

  // [DIAG] 记录 handlerReady → pollAndDispatch 入口
  if (!is_fc) {
    if (is_tx) diag_.handler_ready_io_tx.fetch_add(1, std::memory_order_relaxed);
    else       diag_.handler_ready_io_rx.fetch_add(1, std::memory_order_relaxed);
  }

  urma_jfc_t* jfc = nullptr;
  urma_jfce_t* jfce = nullptr;
  if (is_fc) {
    if (is_tx) {
      jfc = fc_jfs_.tx_jfc;
      jfce = fc_jfs_.tx_jfce;
    } else {
      jfc = fc_jfr_.rx_jfc;
      jfce = fc_jfr_.rx_jfce;
    }
  } else {
    if (is_tx) {
      jfc = io_jfs_.tx_jfc;
      jfce = io_jfs_.tx_jfce;
    } else {
      jfc = io_jfr_.rx_jfc;
      jfce = io_jfr_.rx_jfce;
    }
  }
  if (!jfc || !jfce) return;

  // ① wait:消费 JFCE 事件通知(epoll 已触发,非阻塞立即返回)
  urma_jfc_t* ev = nullptr;
  int wait_ret = urma_wait_jfc(jfce, 1, kWaitNoBlock, &ev);
  if (wait_ret <= 0) {
    // [DIAG] wait 返回 ≤0,事件被消费或异常,提前退出
    if (!is_fc) diag_.wait_le0_skip.fetch_add(1, std::memory_order_relaxed);
    logDiagIfDue();
    return;  // 事件被消费/异常,防御返回
  }

  // ② poll:收割(TX/RX 独立 JFC,无需 user_ctx 区分)
  uint64_t cqe_before_rx = diag_.poll_rx_cqe.load(std::memory_order_relaxed);
  uint64_t cqe_before_tx = diag_.poll_tx_cqe.load(std::memory_order_relaxed);
  if (is_tx) {
    pollAndDispatchTxOnly(jfc, is_fc);
  } else {
    pollAndDispatchRxOnly(jfc, is_fc);
  }

  // ③ ack:确认收割(清通知计数)
  uint32_t ack_cnt = 1;
  urma_ack_jfc(&jfc, &ack_cnt, 1);

  // ④ rearm:重新武装下一次事件通知
  if (urma_rearm_jfc(jfc, false) != URMA_SUCCESS) {
    if (!is_fc) diag_.rearm_fail.fetch_add(1, std::memory_order_relaxed);
    logDiagIfDue();
    return;
  }

  // ⑤ rearm 窗口期补收割(防漏)
  if (is_tx) {
    pollAndDispatchTxOnly(jfc, is_fc);
  } else {
    pollAndDispatchRxOnly(jfc, is_fc);
  }
  ack_cnt = 1;
  urma_ack_jfc(&jfc, &ack_cnt, 1);

  // [DIAG] 统计本轮收割的 CQE 数
  if (!is_fc) {
    if (is_tx) {
      uint64_t n = diag_.poll_tx_cqe.load(std::memory_order_relaxed) - cqe_before_tx;
      (void)n;
    } else {
      uint64_t n = diag_.poll_rx_cqe.load(std::memory_order_relaxed) - cqe_before_rx;
      (void)n;
    }
  }
  logDiagIfDue();
}

void UrmaTransportContext::pollAndDispatchTxOnly(urma_jfc_t* tx_jfc,
                                                   bool is_fc) {
  urma_cr_t cr[POLL_BATCH_MAX];
  int n;
  while ((n = urma_poll_jfc(tx_jfc, POLL_BATCH_MAX, cr)) > 0) {
    if (!is_fc) diag_.poll_tx_cqe.fetch_add(n, std::memory_order_relaxed);
    for (int i = 0; i < n; ++i) {
      // TX JFC:所有 CQE 都是 TxCompletionCtx*(无 RX CQE 混入)
      auto* tx_ctx = reinterpret_cast<TxCompletionCtx*>(cr[i].user_ctx);
      if (!tx_ctx) continue;
      if (tx_ctx->type == TxCompletionCtx::MSG_READ) {
        uint64_t socket_id = tx_ctx->socket_id;
        uint16_t msg_id = tx_ctx->msg_id;
        urma_status_t status = cr[i].status;
        if (is_fc) {
          releaseFcSq();
        } else {
          releaseIoSq();
        }
        delete tx_ctx;
        // 路由表查 socket(same-thread 直调,置空协议判空)
        auto entry = route_table_.find(socket_id);
        if (entry) {
          if (auto* sock = entry->sock.load(std::memory_order_acquire)) {
            sock->handleRemoteReadCqe(msg_id, status);
          }
        }
      } else {
        // MSG_DATA / MSG_CTRL_PULL / MSG_CTRL_FREE:
        // 归还 sq_inflight + delete ctx → IoBuf chain 析构 → block 回收
        if (is_fc) {
          releaseFcSq();
        } else {
          releaseIoSq();
        }
        delete tx_ctx;
      }
    }
  }
}

void UrmaTransportContext::pollAndDispatchRxOnly(urma_jfc_t* rx_jfc,
                                                   bool is_fc) {
  EvbJfr* jfr = is_fc ? &fc_jfr_ : &io_jfr_;
  urma_cr_t cr[POLL_BATCH_MAX];
  int n;
  while ((n = urma_poll_jfc(rx_jfc, POLL_BATCH_MAX, cr)) > 0) {
    if (!is_fc) diag_.poll_rx_cqe.fetch_add(n, std::memory_order_relaxed);
    for (int i = 0; i < n; ++i) {
      // RX JFC:所有 CQE 都是 slot index(无 TX CQE 混入)
      handleRecvCqe(is_fc, cr[i]);
    }
  }
  // 批量收割后统一补投一次(而非每个 CQE 调用,减少 atomic load 水位检查)
  repostRecycled(jfr);
}

void UrmaTransportContext::handleRecvCqe(bool is_fc, const urma_cr_t& cr) {
  EvbJfr* jfr = is_fc ? &fc_jfr_ : &io_jfr_;
  uint32_t slot = static_cast<uint32_t>(cr.user_ctx);
  if (slot >= jfr->rx_bufs.size()) return;
  void* recv_buf = jfr->rx_bufs[slot].data;
  uint32_t recv_len = cr.completion_len;

  // 记账(先加后减序)
  jfr->transit_cnt.fetch_add(1, std::memory_order_acq_rel);
  jfr->recv_posted -= 1;  // same-thread(I2)

  if (is_fc) {
    // FC 消息:payload 前 8B 携带 SocketId
    if (recv_len < sizeof(uint64_t)) {
      XLOG(WARN) << "[URMA-FC] recv too short, slot=" << slot;
      recycleRxSlot(jfr, slot);
      return;
    }
    uint64_t dst_socket_id = 0;
    std::memcpy(&dst_socket_id, recv_buf, sizeof(uint64_t));
    {
      auto entry = fc_route_table_.find(dst_socket_id);
      if (!entry) {
        recycleRxSlot(jfr, slot);
        return;
      }
      if (auto* sock = entry->sock.load(std::memory_order_acquire)) {
        sock->onFcMessageReceived(recv_buf, recv_len);
      }
      recycleRxSlot(jfr, slot);
    }
  } else {
    // IO 消息:SEND_IMM 控制包 或 SEND 数据包
    if (cr.opcode == URMA_CR_OPC_SEND_WITH_IMM) {
      auto* ctrl_hdr = parseHeader(recv_buf);
      if (!ctrl_hdr) {
        recycleRxSlot(jfr, slot);
        return;
      }
      auto entry = route_table_.find(ctrl_hdr->dst_socket_id);
      if (!entry) {
        recycleRxSlot(jfr, slot);
        return;
      }
      auto* sock = entry->sock.load(std::memory_order_acquire);
      if (!sock) {
        recycleRxSlot(jfr, slot);
        return;
      }
      if (ctrl_hdr->type == MSG_PULL_MEM) {
        if (recv_len < offsetof(PullMemFragPacket, frags)) {
          recycleRxSlot(jfr, slot);
          return;
        }
        auto* pkt = static_cast<const PullMemFragPacket*>(recv_buf);
        uint16_t frag_count = pkt->frag_count;
        if (frag_count == 0 || frag_count > 16 ||
            recv_len < offsetof(PullMemFragPacket, frags) +
                           sizeof(PullMemFragDesc) * frag_count) {
          recycleRxSlot(jfr, slot);
          return;
        }
        sock->onPullMemFrags(ctrl_hdr->msg_id, pkt->frags, frag_count,
                             pkt->total_size);
        recycleRxSlot(jfr, slot);
      } else if (ctrl_hdr->type == MSG_FREE) {
        uint16_t msg_id = static_cast<uint16_t>(cr.imm_data);
        sock->onControlMsgImm(MSG_FREE, msg_id, 0, 0);
        recycleRxSlot(jfr, slot);
      } else {
        recycleRxSlot(jfr, slot);
      }
    } else {
      // MSG_DATA:独立 buffer 路由
      auto* hdr = parseHeader(recv_buf);
      if (!hdr) {
        recycleRxSlot(jfr, slot);
        return;
      }
      auto entry = route_table_.find(hdr->dst_socket_id);
      if (!entry) {
        recycleRxSlot(jfr, slot);
        return;
      }
      auto* sock = entry->sock.load(std::memory_order_acquire);
      if (!sock) {
        recycleRxSlot(jfr, slot);
        return;
      }
      bool owned = sock->onIoMessageReceived(recv_buf, recv_len,
                                              &jfr->rx_bufs[slot].ctx);
      if (!owned) {
        recycleRxSlot(jfr, slot);
      }
    }
  }
}

// ── 休眠管理(I4) ──
void UrmaTransportContext::incActiveConn() noexcept {
  DCHECK(!evb_ || evb_->inRunningEventBaseThread());
  if (dormant_.load(std::memory_order_acquire)) {
    wakeup();
  }
  ++active_conn_count_;
}

void UrmaTransportContext::decActiveConn() noexcept {
  DCHECK(!evb_ || evb_->inRunningEventBaseThread());
  if (active_conn_count_ == 0) return;
  if (--active_conn_count_ == 0) {
    dormant();
  }
}

void UrmaTransportContext::dormant() noexcept {
  if (active_conn_count_ > 0) return;
  if (dormant_.load(std::memory_order_acquire)) return;
  // 排空回收栈(仅 repost slot,不触发 socket callback,安全)
  repostRecycled(&io_jfr_);
  repostRecycled(&fc_jfr_);
  // 标记休眠(先于延迟摘除,阻止新事件进入 pollAndDispatch)
  dormant_.store(true, std::memory_order_release);
  // 延迟摘除 CqHandler + RepostTimer:
  // dormant() 可能从 handlerReady→pollAndDispatch→handleRecvCqe→close→decActiveConn
  // 调用栈中触发。在栈上直接 event_del 当前正处理的事件会导致 libevent 2.x
  // event_del_nolock_ use-after-free(SIGSEGV)。runInLoop 延迟到下一轮
  // event_base_loop 迭代执行,当前事件已脱离激活队列,event_del 安全。
  // 不 drain 残留 CQE:drain 可能触发连锁 closeNow→scan_timer_.reset()→event_del
  // 已就绪 timer 事件 → vtable 破坏 → event_base_loop 处理就绪事件时 SIGSEGV。
  evb_->runInLoop([this]() {
    if (io_rx_cq_handler_) io_rx_cq_handler_->unregisterHandler();
    if (io_tx_cq_handler_) io_tx_cq_handler_->unregisterHandler();
    if (fc_rx_cq_handler_) fc_rx_cq_handler_->unregisterHandler();
    if (fc_tx_cq_handler_) fc_tx_cq_handler_->unregisterHandler();
    if (repost_timer_) repost_timer_->disarm();
    XLOG(INFO) << "UrmaTransportContext: dormant (CqHandler unregistered)";
  });
}

void UrmaTransportContext::wakeup() noexcept {
  if (io_rx_cq_handler_) {
    io_rx_cq_handler_->registerHandler(EventHandler::READ | EventHandler::PERSIST);
    if (io_jfr_.rx_jfc) urma_rearm_jfc(io_jfr_.rx_jfc, false);
  }
  if (io_tx_cq_handler_) {
    io_tx_cq_handler_->registerHandler(EventHandler::READ | EventHandler::PERSIST);
    if (io_jfs_.tx_jfc) urma_rearm_jfc(io_jfs_.tx_jfc, false);
  }
  if (fc_rx_cq_handler_) {
    fc_rx_cq_handler_->registerHandler(EventHandler::READ | EventHandler::PERSIST);
    if (fc_jfr_.rx_jfc) urma_rearm_jfc(fc_jfr_.rx_jfc, false);
  }
  if (fc_tx_cq_handler_) {
    fc_tx_cq_handler_->registerHandler(EventHandler::READ | EventHandler::PERSIST);
    if (fc_jfs_.tx_jfc) urma_rearm_jfc(fc_jfs_.tx_jfc, false);
  }
  repostRecycled(&io_jfr_);
  repostRecycled(&fc_jfr_);
  if (repost_timer_) repost_timer_->arm();
  dormant_.store(false, std::memory_order_release);
  XLOG(INFO) << "UrmaTransportContext: wakeup (CqHandler re-registered)";
}

// ── RepostTimer ──
void UrmaTransportContext::RepostTimer::timeoutExpired() noexcept {
  owner_->diag_.repost_timer_tick.fetch_add(1, std::memory_order_relaxed);

  // [DIAG] 每 10 个 tick(100ms)打一次详细日志
  // 如果 io_rx_ready 不增长但 repost_tick 持续增长,说明 epoll 事件丢失
  if (owner_->diag_.repost_timer_tick.load(std::memory_order_relaxed) % 10 == 0) {
    XLOG(INFO) << "[URMA-DIAG] RepostTimer 100ms checkpoint"
               << " tick=" << owner_->diag_.repost_timer_tick.load()
               << " io_rx_ready=" << owner_->diag_.handler_ready_io_rx.load()
               << " io_tx_ready=" << owner_->diag_.handler_ready_io_tx.load()
               << " wait_le0=" << owner_->diag_.wait_le0_skip.load()
               << " rx_cqe=" << owner_->diag_.poll_rx_cqe.load()
               << " tx_cqe=" << owner_->diag_.poll_tx_cqe.load()
               << " rearm_fail=" << owner_->diag_.rearm_fail.load();
  }

  owner_->logDiagIfDue();
  owner_->repostRecycled(&owner_->io_jfr_);
  owner_->checkRxAccounting(&owner_->io_jfr_);
  owner_->repostRecycled(&owner_->fc_jfr_);
  owner_->checkRxAccounting(&owner_->fc_jfr_);
  scheduleTimeout(10);
}

// ── 构造/析构 ──
UrmaTransportContext::UrmaTransportContext(EventBase* evb,
                                            const UrmaConfig& config)
    : evb_(evb) {
  urma_context_t* ctx = UrmaInit::getContext();
  if (!ctx) {
    XLOG(ERR) << "UrmaTransportContext: UrmaInit not initialized";
    return;
  }

  // 创建顺序:IO TX JFC+JFS → IO RX JFC+JFR
  //           FC TX JFC+JFS → FC RX JFC+JFR
  if (!createIoJfs(ctx, config)) return;
  if (!createIoJfr(ctx, config)) return;
  if (!createFcJfs(ctx, config)) return;
  if (!createFcJfr(ctx, config)) return;

  // CqHandler:4 个(io_rx + io_tx + fc_rx + fc_tx)
  io_rx_cq_handler_ = new UrmaCqHandler(evb_, io_jfr_.rx_jfce_fd, this, false, false);
  urma_rearm_jfc(io_jfr_.rx_jfc, false);

  io_tx_cq_handler_ = new UrmaCqHandler(evb_, io_jfs_.tx_jfce_fd, this, false, true);
  urma_rearm_jfc(io_jfs_.tx_jfc, false);

  fc_rx_cq_handler_ = new UrmaCqHandler(evb_, fc_jfr_.rx_jfce_fd, this, true, false);
  urma_rearm_jfc(fc_jfr_.rx_jfc, false);

  fc_tx_cq_handler_ = new UrmaCqHandler(evb_, fc_jfs_.tx_jfce_fd, this, true, true);
  urma_rearm_jfc(fc_jfs_.tx_jfc, false);

  // RepostTimer
  repost_timer_ = std::make_unique<RepostTimer>(this);
  repost_timer_->arm();

  valid_ = true;
  XLOG(INFO) << "UrmaTransportContext: initialized on evb="
             << reinterpret_cast<void*>(evb)
             << " io_jfr_id=" << ioJfrId()
             << " fc_jfr_id=" << fcJfrId();
}

UrmaTransportContext::~UrmaTransportContext() {
  if (repost_timer_) {
    repost_timer_->disarm();
    repost_timer_.reset();
  }
  if (io_rx_cq_handler_) { delete io_rx_cq_handler_; io_rx_cq_handler_ = nullptr; }
  if (io_tx_cq_handler_) { delete io_tx_cq_handler_; io_tx_cq_handler_ = nullptr; }
  if (fc_rx_cq_handler_) { delete fc_rx_cq_handler_; fc_rx_cq_handler_ = nullptr; }
  if (fc_tx_cq_handler_) { delete fc_tx_cq_handler_; fc_tx_cq_handler_ = nullptr; }
  // URMA 硬件资源不显式删除(内核自动回收)
}

// ── 路由表委托 ──
void UrmaTransportContext::registerSocket(uint64_t socket_id,
                                           UrmaAsyncSocket* socket) {
  route_table_.insert(socket_id, socket, evb_);
}
void UrmaTransportContext::unregisterSocket(uint64_t socket_id) {
  route_table_.erase(socket_id);
}
void UrmaTransportContext::registerFcSocket(uint64_t socket_id,
                                             UrmaAsyncSocket* socket) {
  fc_route_table_.insert(socket_id, socket, evb_);
}
void UrmaTransportContext::unregisterFcSocket(uint64_t socket_id) {
  fc_route_table_.erase(socket_id);
}
SafeRouteEntryPtr UrmaTransportContext::findRouteEntry(uint64_t socket_id) {
  return route_table_.find(socket_id);
}
SafeRouteEntryPtr UrmaTransportContext::findFcRouteEntry(uint64_t socket_id) {
  return fc_route_table_.find(socket_id);
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
