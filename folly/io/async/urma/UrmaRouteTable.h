/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaRouteTable: 进程级 SocketId → UrmaAsyncSocket 路由表(SDD-URMA-HFIX-001 任务 A)。
 *
 *   修复 H-1:原 socket_map_/fc_socket_map_ 为普通 unordered_map,任意 EVB 的
 *   收割线程读 + 任意握手线程写 → 多线程 UB / rehash 崩溃。
 *
 *   设计(docs/urma-threading.md 不变量 I2/I3):
 *     - 16 分片,每分片 folly::Synchronized<unordered_map, SharedMutex>(读写锁)
 *     - SafeRouteEntry 独立分配,shared_ptr 保活:find 持分片读锁拷出句柄后立即放锁,
 *       后续无锁使用;entry 生命周期 = map 内引用 + 已拷出引用,
 *       最后一个释放时析构(消除 find 返回裸指针后 socket 被销毁的 UAF 窗口)
 *     - 跨线程仅传递 {socket_id, evb, SafeRouteEntry}(禁止裸指针跨线程逃逸)
 *     - SafeRouteEntry::pending 为在途跨线程转发计数(任务 B 析构 drain 用)
 *
 *   本文件不依赖 urma_api.h,可独立单测(urma/test/UrmaRouteTableTest.cpp)。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>

#include <folly/SharedMutex.h>
#include <folly/Synchronized.h>

namespace folly {

class EventBase;
class UrmaAsyncSocket;

// ── 安全路由句柄(不变量 I3:跨线程安全传递的最小单元) ──
//
// sock 为原子指针(任务 B 修复升级:sock 置空协议)——
// erase 后属主 EVB 将 sock 置 nullptr,在途转发 lambda(恒在属主 EVB 执行)
// 判空跳过回调,结构性消除"析构 drain 自旋阻塞本 EVB 队列"的死锁-UAF 链
// (原 10ms 超时放行方案在 close_loop 高压下会被真实触发,风险 R3)。
// evb 在 insert 后不可变(仅同 socket_id 幂等重注册返回同一 entry,
// 保证句柄身份稳定 → 在途 pending 计数不丢失)。
struct SafeRouteEntry {
  std::atomic<UrmaAsyncSocket*> sock{nullptr};  // 属主 socket(置空协议核心)
  EventBase* evb{nullptr};          // 属主 EventBase
  std::atomic<uint32_t> pending{0}; // 在途跨线程转发数(仅保活 entry 计数)

  SafeRouteEntry() = default;
  ~SafeRouteEntry() = default;
  SafeRouteEntry(const SafeRouteEntry&) = delete;
  SafeRouteEntry& operator=(const SafeRouteEntry&) = delete;
};

using SafeRouteEntryPtr = std::shared_ptr<SafeRouteEntry>;

// ── 16 分片路由表(读多写少,分片 SharedMutex,冲突概率 ~1/16) ──
class UrmaRouteTable {
 public:
  static constexpr size_t kShardCount = 16;
  static_assert((kShardCount & (kShardCount - 1)) == 0,
                "kShardCount must be power of two");

  // 命中返回安全句柄(持读锁拷出 shared_ptr 后放锁);未命中返回 nullptr
  SafeRouteEntryPtr find(uint64_t socket_id) const;

  // 插入;socket_id 已存在时幂等返回既有 entry(不覆盖,保持句柄身份稳定)
  SafeRouteEntryPtr insert(uint64_t socket_id, UrmaAsyncSocket* sock,
                           EventBase* evb);

  // 摘除(只摘表,不等待在途引用);返回是否实际摘除
  bool erase(uint64_t socket_id);

  bool empty() const;
  size_t size() const;
  void clear();

 private:
  using ShardMap = std::unordered_map<uint64_t, SafeRouteEntryPtr>;
  using Shard = folly::Synchronized<ShardMap, folly::SharedMutex>;

  static size_t shardOf(uint64_t socket_id) {
    return std::hash<uint64_t>{}(socket_id) & (kShardCount - 1);
  }

  // 分片读写锁保护,无数据竞争;mutable 支持常量读路径
  mutable std::array<Shard, kShardCount> shards_{};
};

inline SafeRouteEntryPtr UrmaRouteTable::find(uint64_t socket_id) const {
  const Shard& shard = shards_[shardOf(socket_id)];
  return shard.withRLock([&](const ShardMap& m) -> SafeRouteEntryPtr {
    auto it = m.find(socket_id);
    return it != m.end() ? it->second : nullptr;
  });
}

inline SafeRouteEntryPtr UrmaRouteTable::insert(uint64_t socket_id,
                                                UrmaAsyncSocket* sock,
                                                EventBase* evb) {
  Shard& shard = shards_[shardOf(socket_id)];
  return shard.withWLock([&](ShardMap& m) -> SafeRouteEntryPtr {
    auto it = m.find(socket_id);
    if (it != m.end()) {
      // 幂等重注册:返回既有 entry,保持句柄身份稳定
      // (在途 lambda 持有的句柄与 socket 侧 drain 的句柄必须是同一对象)
      return it->second;
    }
    auto entry = std::make_shared<SafeRouteEntry>();
    entry->sock.store(sock, std::memory_order_release);
    entry->evb = evb;
    m.emplace(socket_id, entry);
    return entry;
  });
}

inline bool UrmaRouteTable::erase(uint64_t socket_id) {
  Shard& shard = shards_[shardOf(socket_id)];
  return shard.withWLock(
      [&](ShardMap& m) { return m.erase(socket_id) > 0; });
}

inline bool UrmaRouteTable::empty() const {
  for (const auto& shard : shards_) {
    // 任一分片非空 → 表非空
    if (shard.withRLock([](const ShardMap& m) { return !m.empty(); })) {
      return false;
    }
  }
  return true;
}

inline size_t UrmaRouteTable::size() const {
  size_t total = 0;
  for (const auto& shard : shards_) {
    total += shard.withRLock(
        [](const ShardMap& m) { return m.size(); });
  }
  return total;
}

inline void UrmaRouteTable::clear() {
  for (auto& shard : shards_) {
    shard.withWLock([](ShardMap& m) { m.clear(); });
  }
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
