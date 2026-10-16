/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * ImportedJettyCache: 全局 import_jetty 句柄缓存。
 *
 *   设计文档 §10.5.2:
 *     - 全局缓存 import 句柄,(eid, jetty_id) 做 key + 引用计数
 *     - 第一次 import 时缓存 urma_target_jetty_t* + ref_count=1
 *     - 后续连接复用,ref_count++
 *     - 最后一个引用断开时 urma_delete_jetty + 删除条目
 *     - 线程安全: folly::SharedMutex(读多写少优化)
 *
 *   IO 通道和 FC 通道各自独立缓存(jetty_id 不同,key 天然不冲突)
 *   import_seg 不缓存(per-连接独占,设计 §10.5.5)
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include <folly/SharedMutex.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

// import 句柄缓存条目
struct ImportedJettyEntry {
  urma_target_jetty_t* target{nullptr};
  std::atomic<uint32_t> ref_count{0};
};

class ImportedJettyCache {
 public:
  static ImportedJettyCache* get();

  // 获取或创建 import 句柄,ref_count++
  // rjetty: 从对端 HelloMessage 构造的 rjetty
  // token:  urma_import_jetty 需要的 token
  // 返回 target_jetty 指针;首次调用时执行 urma_import_jetty;失败返回 nullptr
  urma_target_jetty_t* acquire(const urma_rjetty_t& rjetty,
                                urma_token_t* token);

  // ref_count--,归零时 urma_delete_jetty + 删除条目
  void release(urma_target_jetty_t* target);

 private:
  ImportedJettyCache() = default;
  ~ImportedJettyCache() = default;

  folly::SharedMutex mutex_;
  // key = (eid_hex_string, jetty_id)
  using CacheKey = std::pair<std::string, uint32_t>;
  std::map<CacheKey, ImportedJettyEntry*> entries_;
  // 反向索引: target → key(Release 时 O(log N) 查找)
  std::map<urma_target_jetty_t*, CacheKey> reverse_index_;
};

// eid 转 32 字符 hex string
std::string eidToHex(const uint8_t eid[16]);

} // namespace folly

#endif // FOLLY_HAVE_URMA
