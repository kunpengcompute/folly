#include <folly/memory/IoBufPool.h>

#include <cassert>
#include <cstdlib>

namespace folly {

size_t gIoBufBlockSize = kDefaultBlockSize;
size_t gMaxBlocksPerThread = 8;

namespace {
thread_local TLSBlockCache g_tls_cache;
} // namespace

std::atomic<IoBufBlockSizeFn> g_size_fn{nullptr};

void setIoBufBlockAllocFree(IoBufBlockAllocFn alloc_fn,
                            IoBufBlockFreeFn free_fn,
                            IoBufBlockSizeFn size_fn) {
  // 允许覆盖,但实际应避免在 IOBuf::create 已使用后再切换
  g_alloc_fn.store(alloc_fn, std::memory_order_release);
  g_free_fn.store(free_fn, std::memory_order_release);
  g_size_fn.store(size_fn, std::memory_order_release);
}

/**
 * 直接走注入函数,默认为 std::malloc / std::free
 *
 * 注意:alloc/free 必须配对使用,即注入了 alloc 必须同时注入 free,
 * 否则释放路径与申请路径不匹配,导致 heap 错误。
 */
static inline void* blockAlloc(size_t size) {
  return g_alloc_fn.load(std::memory_order_acquire)(size);
}

static inline void blockFree(void* ptr) {
  g_free_fn.load(std::memory_order_acquire)(ptr);
}

static IoBufBlock* allocateFromSystem(size_t size = gIoBufBlockSize) {
  void* mem = blockAlloc(size);
  if (!mem) return nullptr;
  auto* b = static_cast<IoBufBlock*>(mem);
  b->magic = IoBufBlock::kBlockMagic;
  IoBufBlockSizeFn size_fn = g_size_fn.load(std::memory_order_acquire);
  size_t actual_size = size_fn ? size_fn(size) : size;
  b->capacity = static_cast<uint32_t>(actual_size - sizeof(IoBufBlock));
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
  size_t blockDataCap = gIoBufBlockSize - sizeof(IoBufBlock);
  if (b->capacity == blockDataCap) {
    if (c.count < gMaxBlocksPerThread) {
      c.blocks[c.count++] = b;
    } else {
      blockFree(b);
    }
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
  size_t blockDataCap = gIoBufBlockSize - sizeof(IoBufBlock);
  if (min_capacity <= blockDataCap) {
    c.current_share = ioBufBlockAllocate();
  } else {
    size_t alloc_size = min_capacity + sizeof(IoBufBlock);
    c.current_share = allocateFromSystem(alloc_size);
  }
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
