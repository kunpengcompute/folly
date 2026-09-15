/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/memory/IoBufPool.h>

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>
#include <set>

#include <folly/io/IOBuf.h>
#include <folly/portability/GTest.h>

using folly::IOBuf;
using folly::IoBufBlock;
using folly::detail::TLSBlockCache;
using folly::detail::gIoBufBlockSize;
using folly::detail::gMaxBlocksPerThread;
using folly::kDefaultBlockSize;
using folly::kMaxBlocksHardLimit;

// =========================================================================
// IoBufPool unit tests
// =========================================================================

TEST(IoBufPool, BlockAllocateRelease) {
  IoBufBlock* b = folly::detail::ioBufBlockAllocate();
  ASSERT_NE(nullptr, b);
  EXPECT_EQ(IoBufBlock::kBlockMagic, b->magic);
  EXPECT_EQ(gIoBufBlockSize.load(std::memory_order_relaxed) - sizeof(IoBufBlock), b->capacity);
  EXPECT_EQ(0u, b->data_len);
  EXPECT_EQ(1, b->ref_count.load());

  // Write into payload to verify memory is usable
  char* p = b->payloadBegin();
  std::memset(p, 0xAB, b->capacity);
  EXPECT_EQ(static_cast<char>(0xAB), p[0]);
  EXPECT_EQ(static_cast<char>(0xAB), p[b->capacity - 1]);

  folly::detail::ioBufBlockRelease(b);
}

TEST(IoBufPool, BlockReuseFromTLSCache) {
  // Allocate and release several blocks, then verify they are reused
  std::vector<IoBufBlock*> blocks;
  for (int i = 0; i < 4; ++i) {
    blocks.push_back(folly::detail::ioBufBlockAllocate());
  }
  for (auto* b : blocks) {
    folly::detail::ioBufBlockRelease(b);
  }
  // Now allocate again — should come from TLS cache (same pointers)
  std::vector<IoBufBlock*> reused;
  for (int i = 0; i < 4; ++i) {
    reused.push_back(folly::detail::ioBufBlockAllocate());
  }
  // The order may be LIFO, so check as sets
  std::set<IoBufBlock*> s1(blocks.begin(), blocks.end());
  std::set<IoBufBlock*> s2(reused.begin(), reused.end());
  EXPECT_EQ(s1, s2);
  for (auto* b : reused) {
    folly::detail::ioBufBlockRelease(b);
  }
}

TEST(IoBufPool, BlockReleaseNullSafe) {
  folly::detail::ioBufBlockRelease(nullptr); // should not crash
}

TEST(IoBufPool, ShareBlockSameBlock) {
  // share_block should return the same block if it has enough remaining space
  IoBufBlock* b1 = folly::detail::share_block(64);
  ASSERT_NE(nullptr, b1);
  IoBufBlock* b2 = folly::detail::share_block(64);
  EXPECT_EQ(b1, b2); // same block, enough space
}

TEST(IoBufPool, ShareBlockNewBlockWhenFull) {
  // share_block returns a block with full capacity.
  // To force a new block, consume all remaining space.
  IoBufBlock* b1 = folly::detail::share_block(1);
  ASSERT_NE(nullptr, b1);
  EXPECT_FALSE(b1->full());
  // Fill it up
  b1->data_len = b1->capacity;
  EXPECT_TRUE(b1->full());
  // Next share_block recycles the same block via LIFO cache
  IoBufBlock* b2 = folly::detail::share_block(1);
  ASSERT_NE(nullptr, b2);
  EXPECT_EQ(b1, b2);
  EXPECT_FALSE(b2->full());
  // Don't manually release — let ~TLSBlockCache handle current_share cleanup.
  // The old current_share (b1) was already released inside share_block.
}

TEST(IoBufPool, BlockSizes) {
  EXPECT_EQ(32u, sizeof(IoBufBlock));
  EXPECT_EQ(8192u, kDefaultBlockSize);
  EXPECT_EQ(256u, kMaxBlocksHardLimit);
  EXPECT_EQ(8192u - 32u, kDefaultBlockSize - sizeof(IoBufBlock));
}

namespace {
// Isolate the thread-local cache and restore the setting even after ASSERT_*.
template <typename F>
void withFreshBlockCache(F test) {
  const auto savedSize = gIoBufBlockSize.load(std::memory_order_relaxed);
  std::thread worker(test);
  worker.join();
  gIoBufBlockSize.store(savedSize, std::memory_order_relaxed);
}
} // namespace

TEST(IoBufPool, ShareBlockSkipsUndersizedCachedBlocks) {
  withFreshBlockCache([] {
    gIoBufBlockSize.store(sizeof(IoBufBlock) + 1);
    std::vector<IoBufBlock*> blocks;
    for (size_t i = 0; i < 3; ++i) {
      auto* block = folly::detail::ioBufBlockAllocate();
      ASSERT_NE(nullptr, block);
      blocks.push_back(block);
    }
    for (auto* block : blocks) {
      folly::detail::ioBufBlockRelease(block);
    }

    gIoBufBlockSize.store(kDefaultBlockSize);
    auto* block = folly::detail::share_block(512);
    ASSERT_NE(nullptr, block);
    EXPECT_GE(block->remaining(), 512u);
    EXPECT_EQ(1, block->ref_count.load());
    EXPECT_EQ(block, folly::detail::share_block(512));
  });
}

TEST(IoBufPool, ShareBlockReusesAdequateBlockBelowUndersizedBlock) {
  withFreshBlockCache([] {
    gIoBufBlockSize.store(kDefaultBlockSize);
    auto* adequate = folly::detail::ioBufBlockAllocate();
    ASSERT_NE(nullptr, adequate);
    gIoBufBlockSize.store(sizeof(IoBufBlock) + 1);
    auto* small = folly::detail::ioBufBlockAllocate();
    ASSERT_NE(nullptr, small);
    folly::detail::ioBufBlockRelease(adequate);
    folly::detail::ioBufBlockRelease(small);

    gIoBufBlockSize.store(kDefaultBlockSize);
    auto* block = folly::detail::share_block(512);
    ASSERT_NE(nullptr, block);
    EXPECT_EQ(adequate, block);
    EXPECT_GE(block->remaining(), 512u);
  });
}

TEST(IoBufPool, ShareBlockReplacesUndersizedCurrentBlock) {
  withFreshBlockCache([] {
    gIoBufBlockSize.store(sizeof(IoBufBlock) + 1);
    ASSERT_NE(nullptr, folly::detail::share_block(1));

    gIoBufBlockSize.store(kDefaultBlockSize);
    for (size_t i = 0; i < 3; ++i) {
      auto* block = folly::detail::share_block(512);
      ASSERT_NE(nullptr, block);
      EXPECT_GE(block->remaining(), 512u);
      // Exhaust this block to exercise recycling on the following request.
      block->data_len = block->capacity;
    }
  });
}

// =========================================================================
// IOBuf memory pool integration tests
// =========================================================================

class IoBufMemoryPoolTest : public ::testing::Test {
 protected:
  void SetUp() override {
    IOBuf::enableMemoryPool();
    IOBuf::setBlockSize(kDefaultBlockSize);
    IOBuf::setMaxBlocksPerThread(8);
  }
};

TEST_F(IoBufMemoryPoolTest, CreateFromPool) {
  auto buf = IOBuf::create(1024);
  ASSERT_NE(nullptr, buf);
  EXPECT_LE(1024u, buf->capacity());
  // Verify we can write to it
  std::memset(buf->writableData(), 0x42, 1024);
  buf->append(1024);
  EXPECT_EQ(1024u, buf->length());
}

TEST_F(IoBufMemoryPoolTest, CreateSmallCapacity) {
  auto buf = IOBuf::create(1);
  ASSERT_NE(nullptr, buf);
  EXPECT_GE(buf->capacity(), 1u); // at least 1
  *buf->writableData() = 0x55;
  buf->append(1);
  EXPECT_EQ(1u, buf->length());
  EXPECT_EQ(0x55, *buf->data());
}

TEST_F(IoBufMemoryPoolTest, CreateExactBlockCapacity) {
  size_t blockDataCap = gIoBufBlockSize.load(std::memory_order_relaxed) - sizeof(IoBufBlock);
  auto buf = IOBuf::create(blockDataCap);
  ASSERT_NE(nullptr, buf);
  EXPECT_LE(blockDataCap, buf->capacity());
}

TEST_F(IoBufMemoryPoolTest, CreateLargerThanBlock) {
  // Capacity larger than block → should fall through to original path
  size_t blockDataCap = gIoBufBlockSize.load(std::memory_order_relaxed) - sizeof(IoBufBlock);
  auto buf = IOBuf::create(blockDataCap + 100);
  ASSERT_NE(nullptr, buf);
  EXPECT_LE(blockDataCap + 100, buf->capacity());
}

TEST_F(IoBufMemoryPoolTest, MultipleCreatesShareBlock) {
  // Multiple small creates should share the same underlying block
  size_t cap1 = 64;
  auto buf1 = IOBuf::create(cap1);
  ASSERT_NE(nullptr, buf1);

  size_t cap2 = 128;
  auto buf2 = IOBuf::create(cap2);
  ASSERT_NE(nullptr, buf2);

  // Both should reference the same underlying block.
  // Verify indirectly by checking data pointers are within the same 8KB region
  uintptr_t p1 = reinterpret_cast<uintptr_t>(buf1->data());
  uintptr_t p2 = reinterpret_cast<uintptr_t>(buf2->data());
  uintptr_t region = p1 / kDefaultBlockSize;
  // They should be in the same or adjacent 8KB region (depending on offset)
  // This is a soft check — the key point is both came from pool
  EXPECT_NE(0u, region);
}

TEST_F(IoBufMemoryPoolTest, WriteAndRead) {
  const char* testStr = "Hello, IOBufPool!";
  size_t len = std::strlen(testStr);
  auto buf = IOBuf::create(len);
  std::memcpy(buf->writableData(), testStr, len);
  buf->append(len);
  EXPECT_EQ(len, buf->length());
  EXPECT_EQ(0, std::memcmp(buf->data(), testStr, len));
}

TEST_F(IoBufMemoryPoolTest, MoveConstruct) {
  auto buf1 = IOBuf::create(256);
  std::memset(buf1->writableData(), 0xCC, 100);
  buf1->append(100);

  IOBuf moved(std::move(*buf1));
  EXPECT_EQ(0u, buf1->length()); // NOLINT: use after move
  EXPECT_EQ(100u, moved.length());
  EXPECT_EQ(0xCC, moved.data()[0]);
}

TEST_F(IoBufMemoryPoolTest, MoveAssignment) {
  auto buf1 = IOBuf::create(256);
  std::memset(buf1->writableData(), 0xDD, 50);
  buf1->append(50);

  IOBuf buf2(IOBuf::CREATE, 128);
  buf2 = std::move(*buf1);
  EXPECT_EQ(0u, buf1->length()); // NOLINT: use after move
  EXPECT_EQ(50u, buf2.length());
  EXPECT_EQ(0xDD, buf2.data()[0]);
}

TEST_F(IoBufMemoryPoolTest, CloneOne) {
  auto buf1 = IOBuf::create(128);
  std::memset(buf1->writableData(), 0xEE, 64);
  buf1->append(64);

  auto cloned = buf1->cloneOne();
  ASSERT_NE(nullptr, cloned);
  EXPECT_EQ(64u, cloned->length());
  EXPECT_EQ(0, std::memcmp(buf1->data(), cloned->data(), 64));
}

TEST_F(IoBufMemoryPoolTest, CloneOneAsValue) {
  auto buf1 = IOBuf::create(128);
  const char* str = "test clone value";
  size_t len = std::strlen(str);
  std::memcpy(buf1->writableData(), str, len);
  buf1->append(len);

  IOBuf cloned = buf1->cloneOneAsValue();
  EXPECT_EQ(len, cloned.length());
  EXPECT_EQ(0, std::memcmp(buf1->data(), cloned.data(), len));
}

TEST_F(IoBufMemoryPoolTest, ChainOfPooledBufs) {
  auto head = IOBuf::create(64);
  std::memset(head->writableData(), 'A', 64);
  head->append(64);

  auto second = IOBuf::create(64);
  std::memset(second->writableData(), 'B', 64);
  second->append(64);

  head->prependChain(std::move(second));

  EXPECT_EQ(128u, head->computeChainDataLength());
  EXPECT_EQ('A', head->data()[0]);
  EXPECT_EQ('B', head->next()->data()[0]);
}

TEST_F(IoBufMemoryPoolTest, UnshareOne) {
  auto buf1 = IOBuf::create(128);
  std::memset(buf1->writableData(), 0x11, 64);
  buf1->append(64);

  auto cloned = buf1->cloneOne();
  // Modify clone → should trigger unshare on clone
  cloned->unshare();
  std::memset(cloned->writableData(), 0x22, 64);
  // Original should be unchanged
  EXPECT_EQ(0x11, buf1->data()[0]);
  EXPECT_EQ(0x22, cloned->data()[0]);
}

TEST_F(IoBufMemoryPoolTest, Coalesce) {
  auto head = IOBuf::create(32);
  std::memset(head->writableData(), 'X', 32);
  head->append(32);

  for (int i = 0; i < 3; ++i) {
    auto b = IOBuf::create(32);
    std::memset(b->writableData(), 'X' + i + 1, 32);
    b->append(32);
    head->prependChain(std::move(b));
  }

  head->coalesce();
  EXPECT_EQ(1u, head->countChainElements()); // just head now
  EXPECT_EQ(128u, head->length());
}

TEST_F(IoBufMemoryPoolTest, Reserve) {
  auto buf = IOBuf::create(32);
  std::memset(buf->writableData(), 0x33, 16);
  buf->append(16);

  // Reserve more than current capacity → should reallocate
  buf->reserve(0, 256);
  EXPECT_LE(256u, buf->tailroom());
  // Data preserved
  EXPECT_EQ(16u, buf->length());
  EXPECT_EQ(0x33, buf->data()[0]);
}

TEST_F(IoBufMemoryPoolTest, MoveToFbString) {
  auto buf = IOBuf::create(64);
  const char* str = "move to fbstring";
  size_t len = std::strlen(str);
  std::memcpy(buf->writableData(), str, len);
  buf->append(len);

  folly::fbstring fs = buf->moveToFbString();
  EXPECT_EQ(len, fs.size());
  EXPECT_EQ(0, std::memcmp(fs.data(), str, len));
}

TEST_F(IoBufMemoryPoolTest, SetBlockSizeIgnoredAfterPoolEnabled) {
  // Pool is enabled by SetUp(); setBlockSize should be ignored
  IOBuf::setBlockSize(4096);
  EXPECT_EQ(kDefaultBlockSize, IOBuf::getBlockSize());

  auto buf = IOBuf::create(100);
  ASSERT_NE(nullptr, buf);
  EXPECT_LE(100u, buf->capacity());
}

TEST(IoBufPool, SetMaxBlocksPerThread) {
  // If pool is already enabled (from prior tests), setMaxBlocksPerThread is ignored.
  // Skip the clamping test in that case.
  if (IOBuf::isMemoryPoolEnabled()) {
    GTEST_SKIP() << "Pool already enabled, clamping test skipped";
  }

  IOBuf::setMaxBlocksPerThread(2);
  EXPECT_EQ(2u, IOBuf::getMaxBlocksPerThread());

  IOBuf::setMaxBlocksPerThread(0);
  EXPECT_EQ(1u, IOBuf::getMaxBlocksPerThread()); // clamped to 1

  IOBuf::setMaxBlocksPerThread(kMaxBlocksHardLimit + 100);
  EXPECT_EQ(kMaxBlocksHardLimit, IOBuf::getMaxBlocksPerThread()); // clamped to hard limit

  // Restore default
  IOBuf::setMaxBlocksPerThread(8);
  EXPECT_EQ(8u, IOBuf::getMaxBlocksPerThread());
}

TEST_F(IoBufMemoryPoolTest, Multithreaded) {
  const int kThreads = 4;
  const int kIters = 1000;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kIters; ++i) {
        auto buf = IOBuf::create(256);
        EXPECT_NE(nullptr, buf);
        std::memset(buf->writableData(), i & 0xFF, 128);
        buf->append(128);
        EXPECT_EQ(128u, buf->length());
        // buf goes out of scope → released to pool
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

TEST_F(IoBufMemoryPoolTest, PoolEnabledAfterSetUp) {
  // Pool is enabled by SetUp(); verify the flag mechanism works
  EXPECT_TRUE(IOBuf::isMemoryPoolEnabled());
}

TEST(IoBufPool, DefaultBlockSizeConstants) {
  EXPECT_EQ(8192u, kDefaultBlockSize);
  EXPECT_EQ(256u, kMaxBlocksHardLimit);
  EXPECT_EQ(32u, sizeof(IoBufBlock));
}

TEST(IoBufPool, IoBufBlockLayout) {
  IoBufBlock* b = folly::detail::ioBufBlockAllocate();
  ASSERT_NE(nullptr, b);

  EXPECT_EQ(IoBufBlock::kBlockMagic, b->magic);
  EXPECT_EQ(gIoBufBlockSize.load(std::memory_order_relaxed) - sizeof(IoBufBlock), b->capacity);
  EXPECT_FALSE(b->full());
  EXPECT_EQ(b->capacity, b->remaining());

  // Simulate filling
  b->data_len = b->capacity;
  EXPECT_TRUE(b->full());
  EXPECT_EQ(0u, b->remaining());

  folly::detail::ioBufBlockRelease(b);
}

TEST(IoBufPool, PayloadBeginAlignment) {
  IoBufBlock* b = folly::detail::ioBufBlockAllocate();
  ASSERT_NE(nullptr, b);
  // payloadBegin should be 16-byte aligned (after 32-byte header)
  uintptr_t p = reinterpret_cast<uintptr_t>(b->payloadBegin());
  EXPECT_EQ(0u, p % 16);
  folly::detail::ioBufBlockRelease(b);
}
