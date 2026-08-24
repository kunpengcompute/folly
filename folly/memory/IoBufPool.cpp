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

#include <glog/logging.h>
#include <cstdlib>

namespace folly {
namespace detail {

std::atomic<size_t> gIoBufBlockSize{kDefaultBlockSize};
std::atomic<size_t> gMaxBlocksPerThread{8};

namespace {
thread_local TLSBlockCache g_tls_cache;
} // namespace

static IoBufBlock* allocateFromSystem() {
  const size_t blockSize = gIoBufBlockSize.load(std::memory_order_relaxed);
  void* mem = std::malloc(blockSize);
  if (!mem) return nullptr;
  auto* b = static_cast<IoBufBlock*>(mem);
  b->magic = IoBufBlock::kBlockMagic;
  b->capacity = blockSize - sizeof(IoBufBlock);
  b->ref_count.store(1, std::memory_order_relaxed);
  b->data_len = 0;
  return b;
}

IoBufBlock* ioBufBlockAllocate() {
  TLSBlockCache& c = g_tls_cache;
  if (c.count > 0) {
    IoBufBlock* b = c.blocks[--c.count];
    DCHECK_EQ(b->magic, IoBufBlock::kBlockMagic);
    b->ref_count.store(1, std::memory_order_relaxed);
    b->data_len = 0;
    return b;
  }
  return allocateFromSystem();
}

void ioBufBlockRelease(IoBufBlock* b) {
  if (!b) return;
  DCHECK_EQ(b->magic, IoBufBlock::kBlockMagic);
  TLSBlockCache& c = g_tls_cache;
  if (c.count < gMaxBlocksPerThread.load(std::memory_order_relaxed)) {
    c.blocks[c.count++] = b;
  } else {
    std::free(b);
  }
}

IoBufBlock* share_block(size_t min_capacity) {
  TLSBlockCache& c = g_tls_cache;
  if (c.current_share != nullptr &&
      c.current_share->remaining() >= min_capacity) {
    return c.current_share;
  }
  if (c.current_share != nullptr) {
    if (c.current_share->ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      ioBufBlockRelease(c.current_share);
    }
    c.current_share = nullptr;
  }
  c.current_share = ioBufBlockAllocate();
  return c.current_share;
}

TLSBlockCache::~TLSBlockCache() {
  if (current_share != nullptr) {
    if (current_share->ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      std::free(current_share);
    }
    current_share = nullptr;
  }
  for (size_t i = 0; i < count; ++i) {
    std::free(blocks[i]);
  }
  count = 0;
}

} // namespace detail
} // namespace folly
