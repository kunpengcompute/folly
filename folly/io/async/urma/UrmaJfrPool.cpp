/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaJfrPool.h>

#ifdef FOLLY_HAVE_URMA

#include <algorithm>
#include <cstring>

#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/logging/xlog.h>

namespace folly {

// ── 进程级单例 ──

static UrmaJfrPool* g_jfr_pool = nullptr;

UrmaJfrPool* UrmaJfrPool::get() {
  return g_jfr_pool;
}

// ── 创建单个 JFR(含 JFCE + JFC + JFR + recv buffer 预投) ──

bool UrmaJfrPool::createJfr(urma_context_t* ctx, SharedJfr* jfr,
                              uint32_t rx_depth, bool is_fc,
                              const RecvWaterMark& wm) {
  jfr->is_fc = is_fc;
  jfr->rx_depth = rx_depth;
  jfr->watermark = wm;

  // JFCE
  jfr->jfce = urma_create_jfce(ctx);
  if (!jfr->jfce) {
    XLOG(ERR) << "UrmaJfrPool: create jfce failed";
    return false;
  }
  jfr->jfce_fd = jfr->jfce->fd;

  // JFC(绑定 jfce)
  urma_jfc_cfg_t jfc_cfg = {};
  jfc_cfg.depth = rx_depth;
  jfc_cfg.jfce = jfr->jfce;
  jfr->jfc = urma_create_jfc(ctx, &jfc_cfg);
  if (!jfr->jfc) {
    XLOG(ERR) << "UrmaJfrPool: create jfc failed";
    urma_delete_jfce(jfr->jfce);
    jfr->jfce = nullptr;
    return false;
  }

  // JFR
  urma_jfr_cfg_t jfr_cfg = {};
  jfr_cfg.depth = rx_depth;
  jfr_cfg.trans_mode = URMA_TM_RM;
  jfr_cfg.max_sge = 1;  // 每个 recv WR 用 1 个 SGE
  jfr_cfg.min_rnr_timer = URMA_DEFAULT_MIN_RNR_TIMER;
  jfr_cfg.jfc = jfr->jfc;
  jfr->jfr = urma_create_jfr(ctx, &jfr_cfg);
  if (!jfr->jfr) {
    XLOG(ERR) << "UrmaJfrPool: create jfr failed";
    urma_delete_jfc(jfr->jfc);
    jfr->jfc = nullptr;
    urma_delete_jfce(jfr->jfce);
    jfr->jfce = nullptr;
    return false;
  }

  // jetty_id 从 JFR 获取(urma_create_jfr 分配全局唯一 jetty_id)
  // 注意:jfr 本身不持有 jetty_id,jetty 在 UrmaHandshake::allocateResources 中创建
  // 这里记录 jfr 的 jetty_id 供调试
  // jfr->jetty_id 在握手时由 jetty->jetty_id.id 填充

  // recv buffer 池:按 max_water 预分配 buffer,启动时只预投 min_water 个 recv WR
  if (!initRxBufPool(jfr, wm.max_water)) {
    XLOG(ERR) << "UrmaJfrPool: init rx buf pool failed";
    urma_delete_jfr(jfr->jfr);
    jfr->jfr = nullptr;
    urma_delete_jfc(jfr->jfc);
    jfr->jfc = nullptr;
    urma_delete_jfce(jfr->jfce);
    jfr->jfce = nullptr;
    return false;
  }

  // credit_pool 初始化(IO JFR)
  if (!is_fc) {
    jfr->credit_idle.store(rx_depth, std::memory_order_relaxed);
    jfr->credit_allocated.store(0, std::memory_order_relaxed);
  }

  return true;
}

// ── 初始化 recv buffer 池 ──
// 预分配 max_water 个 4KB buffer,但启动时只预投 min_water 个 recv WR
bool UrmaJfrPool::initRxBufPool(SharedJfr* jfr, uint32_t max_bufs) {
  jfr->rx_bufs.resize(max_bufs);
  for (uint32_t i = 0; i < max_bufs; ++i) {
    jfr->rx_bufs[i].data = UrmaInit::poolAlloc(URMA_SMALL_BUF_SIZE);
    if (!jfr->rx_bufs[i].data) {
      XLOG(ERR) << "UrmaJfrPool: rx_buf alloc failed at " << i;
      // 回收已分配的
      for (uint32_t j = 0; j < i; ++j) {
        UrmaInit::poolFree(jfr->rx_bufs[j].data);
        jfr->rx_bufs[j].data = nullptr;
      }
      jfr->rx_bufs.clear();
      return false;
    }
    // 预填充 SGE 和 WR 模板
    jfr->rx_bufs[i].sge.addr = reinterpret_cast<uint64_t>(jfr->rx_bufs[i].data);
    jfr->rx_bufs[i].sge.len = URMA_SMALL_BUF_SIZE;
    jfr->rx_bufs[i].sge.tseg = UrmaInit::getPoolTseg();
    jfr->rx_bufs[i].wr.src.sge = &jfr->rx_bufs[i].sge;
    jfr->rx_bufs[i].wr.src.num_sge = 1;
    jfr->rx_bufs[i].wr.user_ctx = i;  // slot 索引
    jfr->rx_bufs[i].wr.next = nullptr;
  }

  // 启动时只预投 min_water 个 recv WR(按需投递,设计 §7.3)
  uint32_t to_post = jfr->watermark.min_water;
  for (uint32_t i = 0; i < to_post; ++i) {
    if (!postRecvWr(jfr, i)) {
      XLOG(ERR) << "UrmaJfrPool: initial postRecvWr failed at slot " << i;
      return false;
    }
  }
  jfr->recv_posted.store(to_post, std::memory_order_relaxed);

  XLOG(INFO) << "UrmaJfrPool: rx buf pool initialized, max_bufs=" << max_bufs
             << " pre_posted=" << to_post
             << " is_fc=" << jfr->is_fc;
  return true;
}

// ── post 单个 recv WR ──
bool UrmaJfrPool::postRecvWr(SharedJfr* jfr, uint32_t slot) {
  if (slot >= jfr->rx_bufs.size()) return false;
  urma_jfr_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfr_wr(jfr->jfr, &jfr->rx_bufs[slot].wr, &bad_wr);
  if (ret != URMA_SUCCESS) {
    XLOG(WARN) << "UrmaJfrPool: postRecvWr failed at slot " << slot
               << ", status=" << ret;
    return false;
  }
  return true;
}

// ── init ──

bool UrmaJfrPool::init(urma_context_t* ctx, const UrmaConfig& config) {
  if (inited_) return true;

  RecvWaterMark wm;
  wm.min_water = config.min_water;
  wm.max_water = config.max_water;
  wm.batch_post = config.batch_post;

  // IO JFR 池
  io_jfr_list_.resize(config.io_jfr_count);
  for (uint32_t i = 0; i < config.io_jfr_count; ++i) {
    if (!createJfr(ctx, &io_jfr_list_[i], config.rx_depth, false, wm)) {
      XLOG(ERR) << "UrmaJfrPool: create io jfr " << i << " failed";
      return false;
    }
  }

  // FC JFR 池
  fc_jfr_list_.resize(config.fc_jfr_count);
  for (uint32_t i = 0; i < config.fc_jfr_count; ++i) {
    if (!createJfr(ctx, &fc_jfr_list_[i], config.fc_rx_depth, true, wm)) {
      XLOG(ERR) << "UrmaJfrPool: create fc jfr " << i << " failed";
      return false;
    }
  }

  g_jfr_pool = this;
  inited_ = true;
  XLOG(INFO) << "UrmaJfrPool: initialized, io_jfr=" << config.io_jfr_count
             << " fc_jfr=" << config.fc_jfr_count
             << " rx_depth=" << config.rx_depth
             << " min_water=" << wm.min_water
             << " max_water=" << wm.max_water;
  return true;
}

// ── fini ──

void UrmaJfrPool::fini() {
  // 参照 UrmaSharedJfr:不在 fini 中删除 URMA 硬件资源
  // 进程退出时内核自动回收
  io_jfr_list_.clear();
  fc_jfr_list_.clear();
  socket_map_.clear();
  fc_socket_map_.clear();
  g_jfr_pool = nullptr;
  inited_ = false;
}

// ── round-robin 选 JFR ──

SharedJfr* UrmaJfrPool::selectIoJfr(uint32_t* out_index) {
  if (io_jfr_list_.empty()) return nullptr;
  uint32_t idx = io_rr_.fetch_add(1, std::memory_order_relaxed) %
                 static_cast<uint32_t>(io_jfr_list_.size());
  if (out_index) *out_index = idx;
  return &io_jfr_list_[idx];
}

SharedJfr* UrmaJfrPool::selectFcJfr(uint32_t* out_index) {
  if (fc_jfr_list_.empty()) return nullptr;
  uint32_t idx = fc_rr_.fetch_add(1, std::memory_order_relaxed) %
                 static_cast<uint32_t>(fc_jfr_list_.size());
  if (out_index) *out_index = idx;
  return &fc_jfr_list_[idx];
}

// ── 补投 recv WR(单个 slot) ──

void UrmaJfrPool::replenishRxBuf(SharedJfr* jfr, uint32_t slot) noexcept {
  // 单个 slot 补投:CQE 消费后直接重投同一 slot
  // recv_posted 在 pollAndDispatch 中统一管理(批量扣减 consumed 后按水位线补投)
  postRecvWr(jfr, slot);
}

// ── 按水位线批量补投 recv WR ──

void UrmaJfrPool::replenishByWaterMark(SharedJfr* jfr) noexcept {
  uint32_t posted = jfr->recv_posted.load(std::memory_order_relaxed);
  if (posted >= jfr->watermark.min_water) {
    return;  // 水位足够,不补投
  }

  // 补投到 max_water
  uint32_t to_post = jfr->watermark.max_water - posted;
  uint32_t max_slots = static_cast<uint32_t>(jfr->rx_bufs.size());
  for (uint32_t i = 0; i < to_post; ++i) {
    // 从 next_free_slot 游标取下一个 slot
    uint32_t slot = jfr->next_free_slot.fetch_add(1, std::memory_order_relaxed) %
                    max_slots;
    if (postRecvWr(jfr, slot)) {
      jfr->recv_posted.fetch_add(1, std::memory_order_relaxed);
    } else {
      // post 失败(slot 可能仍在 RQ 中),回退游标
      jfr->next_free_slot.fetch_sub(1, std::memory_order_relaxed);
      break;
    }
  }
}

// ── pollAndDispatch: IO JFR 的 CQE 处理 ──

void UrmaJfrPool::pollAndDispatch(SharedJfr* jfr) noexcept {
  if (!jfr || !jfr->jfc) return;

  urma_jfc_t* wait_jfcs[1] = {jfr->jfc};
  int wait_ret = urma_wait_jfc(jfr->jfce, 1, 0, wait_jfcs);
  if (wait_ret < 0) {
    urma_rearm_jfc(jfr->jfc, false);
    return;
  }

  constexpr int POLL_BATCH_MAX = 64;
  urma_cr_t cr[POLL_BATCH_MAX];
  int n;
  while ((n = urma_poll_jfc(jfr->jfc, POLL_BATCH_MAX, cr)) > 0) {
    for (int i = 0; i < n; ++i) {
      uint32_t slot = static_cast<uint32_t>(cr[i].user_ctx);
      void* recv_buf = jfr->rx_bufs[slot].data;
      uint32_t recv_len = cr[i].completion_len;

      // Phase 3: 兼容 SEND_IMM 控制包 + headroom 路由双路径
      if (cr[i].opcode == URMA_CR_OPC_SEND_WITH_IMM) {
        // 旧版 SEND_IMM 控制包
        auto it = socket_map_.find(cr[i].remote_id.id);
        if (it != socket_map_.end()) {
          it->second->onControlMsg(cr[i].imm_data, recv_buf);
        }
        postRecvWr(jfr, slot);  // 立即重投同一 slot
        recharge(jfr);
        continue;
      }

      // headroom 路由:从 recv buffer 前 64B 解析 header
      auto* hdr = parseHeader(recv_buf);
      if (!hdr) {
        XLOG(WARN) << "[URMA] JfrPool poll: magic mismatch, slot=" << slot;
        postRecvWr(jfr, slot);
        recharge(jfr);
        continue;
      }

      // 路由到 UrmaAsyncSocket(过渡:仍用 socket_map_)
      auto it = socket_map_.find(cr[i].remote_id.id);
      if (it == socket_map_.end()) {
        postRecvWr(jfr, slot);
        recharge(jfr);
        continue;
      }

      // 统一通过 onIoMessageReceived 分发
      it->second->onIoMessageReceived(recv_buf, recv_len, slot);
      postRecvWr(jfr, slot);  // 立即重投同一 slot
      recharge(jfr);
    }
  }

  // ack + rearm
  urma_jfc_t* ack_jfcs[1] = {jfr->jfc};
  uint32_t nevents[1] = {static_cast<uint32_t>(wait_ret > 0 ? wait_ret : 0)};
  if (nevents[0] > 0) {
    urma_ack_jfc(ack_jfcs, nevents, 1);
  }
  urma_rearm_jfc(jfr->jfc, false);

  // recv_posted 不变(CQE 处理后立即重投同一 slot)
  // 水位线机制预留(Phase 4 后续优化:改为延迟批量补投)
}

// ── pollFcAndDispatch: FC JFR 的 CQE 处理(Phase 6 完整实现) ──

void UrmaJfrPool::pollFcAndDispatch(SharedJfr* jfr) noexcept {
  if (!jfr || !jfr->jfc) return;

  urma_jfc_t* wait_jfcs[1] = {jfr->jfc};
  int wait_ret = urma_wait_jfc(jfr->jfce, 1, 0, wait_jfcs);
  if (wait_ret < 0) {
    urma_rearm_jfc(jfr->jfc, false);
    return;
  }

  constexpr int POLL_BATCH_MAX = 64;
  urma_cr_t cr[POLL_BATCH_MAX];
  int n;
  while ((n = urma_poll_jfc(jfr->jfc, POLL_BATCH_MAX, cr)) > 0) {
    // Phase 6: FC 消息处理(REQ/REP/RETURN/ACK)
    (void)n;
  }

  urma_jfc_t* ack_jfcs[1] = {jfr->jfc};
  uint32_t nevents[1] = {static_cast<uint32_t>(wait_ret > 0 ? wait_ret : 0)};
  if (nevents[0] > 0) {
    urma_ack_jfc(ack_jfcs, nevents, 1);
  }
  urma_rearm_jfc(jfr->jfc, false);
}

// ── 路由表 ──

void UrmaJfrPool::registerSocket(uint32_t jetty_id, UrmaAsyncSocket* socket) {
  socket_map_[jetty_id] = socket;
}

void UrmaJfrPool::unregisterSocket(uint32_t jetty_id) {
  socket_map_.erase(jetty_id);
}

UrmaAsyncSocket* UrmaJfrPool::findSocket(uint32_t jetty_id) {
  auto it = socket_map_.find(jetty_id);
  return it != socket_map_.end() ? it->second : nullptr;
}

void UrmaJfrPool::registerFcSocket(uint32_t fc_jetty_id, UrmaAsyncSocket* socket) {
  fc_socket_map_[fc_jetty_id] = socket;
}

void UrmaJfrPool::unregisterFcSocket(uint32_t fc_jetty_id) {
  fc_socket_map_.erase(fc_jetty_id);
}

UrmaAsyncSocket* UrmaJfrPool::findFcSocket(uint32_t fc_jetty_id) {
  auto it = fc_socket_map_.find(fc_jetty_id);
  return it != fc_socket_map_.end() ? it->second : nullptr;
}

// ── JFCE fd 列表(供 UrmaCqHandler 注册) ──

std::vector<UrmaJfrPool::JfrFdEntry> UrmaJfrPool::getAllJfrFds() const {
  std::vector<JfrFdEntry> fds;
  for (uint32_t i = 0; i < io_jfr_list_.size(); ++i) {
    if (io_jfr_list_[i].jfce_fd >= 0) {
      fds.push_back({io_jfr_list_[i].jfce_fd, i, false});
    }
  }
  for (uint32_t i = 0; i < fc_jfr_list_.size(); ++i) {
    if (fc_jfr_list_[i].jfce_fd >= 0) {
      fds.push_back({fc_jfr_list_[i].jfce_fd, i, true});
    }
  }
  return fds;
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
