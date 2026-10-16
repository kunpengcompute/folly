/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaJfsPool.h>

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <cstring>

namespace folly {

// ── 进程级单例 ──

static UrmaJfsPool* g_jfs_pool = nullptr;

UrmaJfsPool* UrmaJfsPool::get() {
  return g_jfs_pool;
}

// ── JFS 创建 ──

SharedJfs UrmaJfsPool::createJfs(urma_context_t* ctx, uint32_t depth,
                                   uint8_t max_sge, uint32_t max_inline_len) {
  SharedJfs sj;

  // 1. JFCE(完成事件文件,供 epoll 注册)
  sj.jfce = urma_create_jfce(ctx);
  if (!sj.jfce) {
    XLOG(ERR) << "UrmaJfsPool: urma_create_jfce failed";
    return sj;
  }
  sj.jfce_fd = sj.jfce->fd;

  // 2. JFC(完成队列,depth + 1 预留 flush_done slot)
  urma_jfc_cfg_t jfc_cfg = {};
  jfc_cfg.depth = depth + 1;
  jfc_cfg.jfce = sj.jfce;
  sj.jfc = urma_create_jfc(ctx, &jfc_cfg);
  if (!sj.jfc) {
    XLOG(ERR) << "UrmaJfsPool: urma_create_jfc failed";
    urma_delete_jfce(sj.jfce);
    sj.jfce = nullptr;
    return sj;
  }

  // 3. JFS(独立创建,不绑定 jetty)
  urma_jfs_cfg_t jfs_cfg = {};
  jfs_cfg.depth = depth;
  jfs_cfg.trans_mode = URMA_TM_RM;
  jfs_cfg.max_sge = max_sge;
  jfs_cfg.max_inline_data = max_inline_len;
  jfs_cfg.rnr_retry = URMA_DEFAULT_RNR_RETRY;
  jfs_cfg.err_timeout = URMA_DEFAULT_ERR_TIMEOUT;
  jfs_cfg.jfc = sj.jfc;
  sj.jfs = urma_create_jfs(ctx, &jfs_cfg);
  if (!sj.jfs) {
    XLOG(ERR) << "UrmaJfsPool: urma_create_jfs failed";
    urma_delete_jfc(sj.jfc);
    sj.jfc = nullptr;
    urma_delete_jfce(sj.jfce);
    sj.jfce = nullptr;
    sj.jfce_fd = -1;
    return sj;
  }

  sj.sq_depth = depth;
  sj.sq_inflight.store(0, std::memory_order_relaxed);
  return sj;
}

// ── 初始化 ──

bool UrmaJfsPool::init(urma_context_t* ctx, const UrmaConfig& config,
                        uint8_t max_sge, uint32_t max_inline_len) {
  if (inited_) {
    XLOG(WARN) << "UrmaJfsPool: already initialized, skipping";
    return true;
  }
  // 设置全局单例指针(供 UrmaJfsPool::get() 访问)
  g_jfs_pool = this;
  if (!ctx) {
    XLOG(ERR) << "UrmaJfsPool: null context";
    return false;
  }

  uint32_t io_count = config.io_jfs_count;
  uint32_t fc_count = config.fc_jfs_count;
  if (io_count == 0) io_count = 1;
  if (fc_count == 0) fc_count = 1;

  // 创建 IO JFS 池
  io_jfs_list_.reserve(io_count);
  uint32_t io_ok = 0;
  for (uint32_t i = 0; i < io_count; ++i) {
    SharedJfs sj = createJfs(ctx, config.tx_depth, max_sge, max_inline_len);
    if (!sj.jfs) {
      XLOG(ERR) << "UrmaJfsPool: create IO JFS[" << i << "] failed";
      break;
    }
    io_jfs_list_.push_back(std::move(sj));
    ++io_ok;
  }
  if (io_ok == 0) {
    XLOG(ERR) << "UrmaJfsPool: no IO JFS created, init failed";
    return false;
  }
  if (io_ok < io_count) {
    XLOG(WARN) << "UrmaJfsPool: created " << io_ok << "/" << io_count << " IO JFS";
  }

  // 创建 FC JFS 池(max_sge=1, depth=fc_tx_depth)
  fc_jfs_list_.reserve(fc_count);
  uint32_t fc_ok = 0;
  for (uint32_t i = 0; i < fc_count; ++i) {
    SharedJfs sj = createJfs(ctx, config.fc_tx_depth, 1, max_inline_len);
    if (!sj.jfs) {
      XLOG(ERR) << "UrmaJfsPool: create FC JFS[" << i << "] failed";
      break;
    }
    fc_jfs_list_.push_back(std::move(sj));
    ++fc_ok;
  }
  if (fc_ok == 0) {
    XLOG(WARN) << "UrmaJfsPool: no FC JFS created (FC channel disabled)";
  }

  io_rr_.store(0, std::memory_order_relaxed);
  fc_rr_.store(0, std::memory_order_relaxed);
  inited_ = true;

  XLOG(INFO) << "UrmaJfsPool: initialized, io_jfs=" << io_ok
             << " fc_jfs=" << fc_ok
             << " tx_depth=" << config.tx_depth
             << " fc_tx_depth=" << config.fc_tx_depth
             << " max_sge=" << static_cast<unsigned>(max_sge);
  return true;
}

// ── 销毁 ──

void UrmaJfsPool::fini() {
  if (!inited_) return;

  // 参照 brpc: 不在 fini 中删除 URMA 硬件资源
  // 进程退出时由内核自动回收,用户态清理可能导致阻塞
  // 仅清空列表引用(不调用 urma_delete_jfs/jfc/jfce)
  io_jfs_list_.clear();
  fc_jfs_list_.clear();
  inited_ = false;
  g_jfs_pool = nullptr;
}

// ── IO JFS acquire/release ──

SharedJfs* UrmaJfsPool::acquireIoJfs(uint32_t* out_index) {
  if (io_jfs_list_.empty()) return nullptr;
  uint32_t count = static_cast<uint32_t>(io_jfs_list_.size());
  // round-robin 起始 + 扫描所有 JFS(最多 2*count 次覆盖回绕)
  uint32_t start = io_rr_.fetch_add(1, std::memory_order_relaxed) % count;
  for (uint32_t i = 0; i < count * 2; ++i) {
    uint32_t idx = (start + i) % count;
    auto& sj = io_jfs_list_[idx];
    // CAS: sq_inflight < sq_depth 时 +1
    uint32_t cur = sj.sq_inflight.load(std::memory_order_acquire);
    while (cur < sj.sq_depth) {
      if (sj.sq_inflight.compare_exchange_weak(cur, cur + 1,
              std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (out_index) *out_index = idx;
        return &sj;
      }
      // CAS 失败,cur 已更新,继续重试
    }
  }
  // 全满
  return nullptr;
}

void UrmaJfsPool::releaseIoJfs(uint32_t index) {
  if (index >= io_jfs_list_.size()) return;
  io_jfs_list_[index].sq_inflight.fetch_sub(1, std::memory_order_acq_rel);
}

// ── FC JFS acquire/release ──

SharedJfs* UrmaJfsPool::acquireFcJfs(uint32_t* out_index) {
  if (fc_jfs_list_.empty()) return nullptr;
  uint32_t count = static_cast<uint32_t>(fc_jfs_list_.size());
  uint32_t start = fc_rr_.fetch_add(1, std::memory_order_relaxed) % count;
  for (uint32_t i = 0; i < count * 2; ++i) {
    uint32_t idx = (start + i) % count;
    auto& sj = fc_jfs_list_[idx];
    uint32_t cur = sj.sq_inflight.load(std::memory_order_acquire);
    while (cur < sj.sq_depth) {
      if (sj.sq_inflight.compare_exchange_weak(cur, cur + 1,
              std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (out_index) *out_index = idx;
        return &sj;
      }
    }
  }
  return nullptr;
}

void UrmaJfsPool::releaseFcJfs(uint32_t index) {
  if (index >= fc_jfs_list_.size()) return;
  fc_jfs_list_[index].sq_inflight.fetch_sub(1, std::memory_order_acq_rel);
}

// ── TX JFCE 中断管理 ──

void UrmaJfsPool::rearmAllTxJfce() {
  for (auto& sj : io_jfs_list_) {
    if (sj.jfc) {
      urma_rearm_jfc(sj.jfc, false);
    }
  }
}

void UrmaJfsPool::rearmAllFcTxFce() {
  for (auto& sj : fc_jfs_list_) {
    if (sj.jfc) {
      urma_rearm_jfc(sj.jfc, false);
    }
  }
}

std::vector<UrmaJfsPool::JfceFdEntry> UrmaJfsPool::getAllJfceFds() const {
  std::vector<JfceFdEntry> result;
  result.reserve(io_jfs_list_.size() + fc_jfs_list_.size());
  for (uint32_t i = 0; i < io_jfs_list_.size(); ++i) {
    if (io_jfs_list_[i].jfce_fd >= 0) {
      result.push_back({io_jfs_list_[i].jfce_fd, i, false});
    }
  }
  for (uint32_t i = 0; i < fc_jfs_list_.size(); ++i) {
    if (fc_jfs_list_[i].jfce_fd >= 0) {
      result.push_back({fc_jfs_list_[i].jfce_fd, i, true});
    }
  }
  return result;
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
