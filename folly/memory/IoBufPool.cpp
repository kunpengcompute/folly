#include <folly/memory/IoBufPool.h>

#include <cassert>
#include <cstdlib>

namespace folly {

size_t gIoBufBlockSize = kDefaultBlockSize;
size_t gMaxBlocksPerThread = 8;

namespace {
thread_local TLSBlockCache g_tls_cache;
} // namespace

static IoBufBlock* allocateFromSystem() {
  void* mem = std::malloc(gIoBufBlockSize);
  if (!mem) return nullptr;
  auto* b = static_cast<IoBufBlock*>(mem);
  b->magic = IoBufBlock::kBlockMagic;
  b->capacity = static_cast<uint32_t>(gIoBufBlockSize - sizeof(IoBufBlock));
  b->ref_count.store(1, std::memory_order_relaxed);
  b->data_len = 0;
  return b;
}

IoBufBlock* ioBufBlockAllocate() {
  TLSBlockCache& c = g_tls_cache;
  if (c.count > 0) {
    IoBufBlock* b = c.blocks[--c.count];
    assert(b->magic == IoBufBlock::kBlockMagic);
    b->ref_count.store(1, std::memory_order_relaxed);
    b->data_len = 0;
    return b;
  }
  return allocateFromSystem();
}

void ioBufBlockRelease(IoBufBlock* b) {
  if (!b) return;
  assert(b->magic == IoBufBlock::kBlockMagic);
  TLSBlockCache& c = g_tls_cache;
  if (c.count < gMaxBlocksPerThread) {
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

} // namespace folly
