/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaImportCache.h>

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/urma/UrmaInit.h>
#include <folly/logging/xlog.h>

#include <cstdio>
#include <cstring>

namespace folly {

// ── 进程级单例 ──

static ImportedJettyCache* g_import_cache = nullptr;

ImportedJettyCache* ImportedJettyCache::get() {
  if (!g_import_cache) {
    g_import_cache = new ImportedJettyCache();
  }
  return g_import_cache;
}

// ── eid 转 hex ──

std::string eidToHex(const uint8_t eid[16]) {
  char hex[33];
  std::snprintf(hex, sizeof(hex),
                "%02x%02x%02x%02x%02x%02x%02x%02x"
                "%02x%02x%02x%02x%02x%02x%02x%02x",
                eid[0], eid[1], eid[2], eid[3],
                eid[4], eid[5], eid[6], eid[7],
                eid[8], eid[9], eid[10], eid[11],
                eid[12], eid[13], eid[14], eid[15]);
  return std::string(hex);
}

// ── acquire ──

urma_target_jetty_t* ImportedJettyCache::acquire(
    const urma_rjetty_t& rjetty, urma_token_t* token) {
  auto key = std::make_pair(
      eidToHex(rjetty.jetty_id.eid.raw), rjetty.jetty_id.id);

  // 读锁:查缓存命中
  {
    folly::SharedMutex::ReadHolder rlock(mutex_);
    auto it = entries_.find(key);
    if (it != entries_.end()) {
      it->second->ref_count.fetch_add(1, std::memory_order_relaxed);
      return it->second->target;
    }
  }

  // 写锁:首次 import
  folly::SharedMutex::WriteHolder wlock(mutex_);
  // double-check:可能在等写锁期间已被其他线程 import
  auto it2 = entries_.find(key);
  if (it2 != entries_.end()) {
    it2->second->ref_count.fetch_add(1, std::memory_order_relaxed);
    return it2->second->target;
  }

  urma_context_t* ctx = UrmaInit::getContext();
  if (!ctx) {
    XLOG(ERR) << "ImportedJettyCache: UrmaInit not initialized";
    return nullptr;
  }

  urma_target_jetty_t* target = urma_import_jetty(ctx, &rjetty, token);
  if (!target) {
    XLOG(ERR) << "ImportedJettyCache: urma_import_jetty failed for jetty_id="
              << rjetty.jetty_id.id;
    return nullptr;
  }

  auto* entry = new ImportedJettyEntry();
  entry->target = target;
  entry->ref_count.store(1, std::memory_order_relaxed);
  entries_[key] = entry;
  reverse_index_[target] = key;
  return target;
}

// ── release ──

void ImportedJettyCache::release(urma_target_jetty_t* target) {
  if (!target) return;

  folly::SharedMutex::WriteHolder wlock(mutex_);
  auto rit = reverse_index_.find(target);
  if (rit == reverse_index_.end()) return;

  auto eit = entries_.find(rit->second);
  if (eit == entries_.end()) return;

  if (eit->second->ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    // 最后一个引用,unimport + 清理
    urma_unimport_jetty(target);
    delete eit->second;
    entries_.erase(eit);
    reverse_index_.erase(rit);
  }
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
