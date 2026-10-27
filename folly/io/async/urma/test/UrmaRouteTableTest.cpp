/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaRouteTable 单测(SDD-URMA-HFIX-001 任务 A 验收):
 *   - 基本语义:insert/find/erase/empty/size
 *   - 幂等重注册:同 socket_id 返回同一 entry(句柄身份稳定)
 *   - 安全句柄:erase 后已拷出句柄仍存活(引用计数保活)
 *   - 并发:SDD §3.1 验收 —— 4 写线程 × 16 读线程 × 10 万次增删查,零错误
 *
 * 不依赖 urma_api/硬件,仅需 FOLLY_HAVE_URMA 宏与 folly 头。
 */
#include <folly/io/async/urma/UrmaRouteTable.h>

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <folly/portability/GTest.h>

using namespace folly;

namespace {

// 测试替身:用编码指针模拟 UrmaAsyncSocket*
// 高 56 位 = socket_id,低 8 位 = 代数(insert 计数)
uintptr_t encodeSock(uint64_t key, uint8_t gen) {
  return (key << 8) | gen;
}
uint64_t sockKey(uintptr_t p) { return p >> 8; }

} // namespace

TEST(UrmaRouteTableTest, BasicInsertFindErase) {
  UrmaRouteTable t;
  EXPECT_TRUE(t.empty());
  EXPECT_EQ(0, t.size());

  auto e0 = t.insert(0x1000, reinterpret_cast<UrmaAsyncSocket*>(0x1000),
                     nullptr);
  ASSERT_NE(nullptr, e0);
  EXPECT_EQ(reinterpret_cast<UrmaAsyncSocket*>(0x1000),
            e0->sock.load());
  EXPECT_EQ(nullptr, e0->evb);
  EXPECT_EQ(0, e0->pending.load());
  EXPECT_EQ(1, t.size());
  EXPECT_FALSE(t.empty());

  // 未命中
  EXPECT_EQ(nullptr, t.find(0x2000));

  // 命中:find 返回同一 entry
  auto f = t.find(0x1000);
  ASSERT_NE(nullptr, f);
  EXPECT_EQ(e0.get(), f.get());

  // 摘除后再查未命中
  EXPECT_TRUE(t.erase(0x1000));
  EXPECT_FALSE(t.erase(0x1000));  // 幂等
  EXPECT_EQ(nullptr, t.find(0x1000));
  EXPECT_TRUE(t.empty());
}

TEST(UrmaRouteTableTest, IdempotentReregisterSameEntry) {
  // 任务 A 关键语义:同 socket_id 重注册返回既有 entry(句柄身份稳定,
  // 在途 pending 计数不丢失 —— 服务端预注册 + onHandshakeReady 重注册场景)
  UrmaRouteTable t;
  auto e1 = t.insert(0x1000, reinterpret_cast<UrmaAsyncSocket*>(0x11),
                     nullptr);
  auto e2 = t.insert(0x1000, reinterpret_cast<UrmaAsyncSocket*>(0x12),
                     nullptr);
  EXPECT_EQ(e1.get(), e2.get());  // 不覆盖,返回既有 entry
  EXPECT_EQ(reinterpret_cast<UrmaAsyncSocket*>(0x11), e1->sock.load());
  EXPECT_EQ(1, t.size());
}

TEST(UrmaRouteTableTest, HandleOutlivesErase) {
  // H-1 隐藏子问题:find 拷出的安全句柄在 erase 后保活
  UrmaRouteTable t;
  auto entry = t.insert(0x1000, reinterpret_cast<UrmaAsyncSocket*>(0x1000),
                        nullptr);
  auto held = t.find(0x1000);
  ASSERT_NE(nullptr, held);
  t.erase(0x1000);
  EXPECT_EQ(nullptr, t.find(0x1000));
  // 持有副本仍然有效(shared_ptr 引用计数)
  held->pending.fetch_add(1, std::memory_order_relaxed);
  EXPECT_EQ(1, entry->pending.load());
  held.reset();
  EXPECT_NE(nullptr, entry);  // 原始引用仍在
}

TEST(UrmaRouteTableTest, ShardDistribution) {
  // 16 分片:不同 key 应分散;跨分片无干扰
  UrmaRouteTable t;
  for (uint64_t k = 1; k <= 1000; ++k) {
    t.insert(k, reinterpret_cast<UrmaAsyncSocket*>(encodeSock(k, 0)),
             nullptr);
  }
  EXPECT_EQ(1000, t.size());
  for (uint64_t k = 1; k <= 1000; ++k) {
    auto e = t.find(k);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(k, sockKey(reinterpret_cast<uintptr_t>(e->sock)));
  }
  EXPECT_TRUE(t.erase(500));
  EXPECT_EQ(nullptr, t.find(500));
  EXPECT_EQ(999, t.size());
}

TEST(UrmaRouteTableTest, ConcurrentInsertEraseFind) {
  // SDD §3.1 验收:4 写线程 × 16 读线程 × 10 万次增删查
  constexpr int kWriters = 4;
  constexpr int kReaders = 16;
  constexpr int kKeysPerWriter = 1000;
  // 每写线程 25 pass × 1000 key × 增+删 = 5 万次操作,4 线程共 20 万次(≥ SDD 量级)
  constexpr int kPasses = 25;

  UrmaRouteTable t;
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> errors{0};
  std::atomic<uint64_t> finds{0};

  std::vector<std::thread> readers;
  for (int r = 0; r < kReaders; ++r) {
    readers.emplace_back([&]() {
      uint64_t seed = 0x9E3779B97F4A7C15ULL;
      while (!stop.load(std::memory_order_relaxed)) {
        for (int i = 0; i < 64; ++i) {
          seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
          uint64_t k = 1 + (seed % (kWriters * kKeysPerWriter));
          auto e = t.find(k);
          finds.fetch_add(1, std::memory_order_relaxed);
          if (e) {
            // 一致性:entry 的 sock 必属于 key k 的名字空间
            if (sockKey(reinterpret_cast<uintptr_t>(e->sock.load())) != k) {
              errors.fetch_add(1, std::memory_order_relaxed);
            }
          }
        }
        std::this_thread::yield();
      }
    });
  }

  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w]() {
      uint64_t base = w * kKeysPerWriter + 1;
      for (int pass = 0; pass < kPasses; ++pass) {
        // 增删混合:每写线程独占 key 空间,erase 必须成功,失败即记账缺陷
        for (uint64_t k = base; k < base + kKeysPerWriter; ++k) {
          t.insert(k, reinterpret_cast<UrmaAsyncSocket*>(encodeSock(k, 0)),
                   nullptr);
        }
        for (uint64_t k = base; k < base + kKeysPerWriter; ++k) {
          if (!t.erase(k)) {
            errors.fetch_add(1, std::memory_order_relaxed);
          }
        }
      }
    });
  }

  for (auto& w : writers) {
    w.join();
  }
  stop.store(true);
  for (auto& r : readers) {
    r.join();
  }

  EXPECT_EQ(0, errors.load());
  EXPECT_EQ(0, t.size());  // 所有 key 最终被各写线程 erase
  EXPECT_GT(finds.load(), 100000u);
}

TEST(UrmaRouteTableTest, Clear) {
  UrmaRouteTable t;
  for (uint64_t k = 0; k < 100; ++k) {
    t.insert(k, reinterpret_cast<UrmaAsyncSocket*>(encodeSock(k, 0)),
             nullptr);
  }
  t.clear();
  EXPECT_TRUE(t.empty());
  EXPECT_EQ(0, t.size());
  EXPECT_EQ(nullptr, t.find(42));
}

#endif // FOLLY_HAVE_URMA
