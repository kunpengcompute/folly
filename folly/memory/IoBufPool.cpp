#include <folly/memory/IoBufPool.h>

#include <cstdlib>

#include <folly/lang/SafeAssert.h>

namespace folly {

size_t gIoBufBlockSize = kDefaultBlockSize;
size_t gMaxBlocksPerThread = 8;

namespace {
thread_local TLSBlockCache g_tls_cache;
} // namespace

std::atomic<IoBufBlockAllocFn> g_alloc_fn{&std::malloc};
std::atomic<IoBufBlockFreeFn>  g_free_fn{&std::free};
std::atomic<IoBufBlockSizeFn> g_size_fn{nullptr};

// URMA headroom 预留大小（默认 0=禁用，UrmaInit::init 时设为 64）
std::atomic<size_t> g_urma_headroom{0};

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

// URMA headroom 预留：每个 block 的 data 区前 64B 作为 headroom，
// 供 IOBuf::prepend(64) 写入 UrmaMsgHeader（DPDK rte_mbuf 风格）。
// 外部可通过 g_urma_headroom 设置（0=禁用，64=启用）。
// 启用时：capacity 减 64，data_len 初始 = 64，IOBuf 的 data_ 从 payloadBegin()+64 开始。

static IoBufBlock* allocateFromSystem(size_t size = gIoBufBlockSize) {
  void* mem = blockAlloc(size);
  if (!mem) return nullptr;
  auto* b = static_cast<IoBufBlock*>(mem);
  b->magic = IoBufBlock::kBlockMagic;
  IoBufBlockSizeFn size_fn = g_size_fn.load(std::memory_order_acquire);
  size_t actual_size = size_fn ? size_fn(size) : size;
  size_t headroom = g_urma_headroom.load(std::memory_order_acquire);
  b->capacity = static_cast<uint32_t>(actual_size - sizeof(IoBufBlock) - headroom);
  b->ref_count.store(1, std::memory_order_relaxed);
  b->data_len = static_cast<uint32_t>(headroom);  // 预留 headroom 区域
  return b;
}

IoBufBlock* ioBufBlockAllocate() {
  TLSBlockCache& c = g_tls_cache;
  if (c.count > 0) {
    IoBufBlock* b = c.blocks[--c.count];
    FOLLY_SAFE_CHECK(
        b->magic == IoBufBlock::kBlockMagic, "corrupt IOBuf block");
    b->ref_count.store(1, std::memory_order_relaxed);
    size_t headroom = g_urma_headroom.load(std::memory_order_acquire);
    b->data_len = static_cast<uint32_t>(headroom);
    return b;
  }
  return allocateFromSystem();
}

void ioBufBlockRelease(IoBufBlock* b) {
  if (!b) return;
  FOLLY_SAFE_CHECK(
      b->magic == IoBufBlock::kBlockMagic, "corrupt IOBuf block");
  TLSBlockCache& c = g_tls_cache;
  size_t headroom = g_urma_headroom.load(std::memory_order_acquire);
  size_t blockDataCap = gIoBufBlockSize - sizeof(IoBufBlock) - headroom;
  if (b->capacity == blockDataCap) {
    if (c.count < gMaxBlocksPerThread) {
      c.blocks[c.count++] = b;
    } else {
      blockFree(b);
    }
  } else {
    // 大 block 直接走注入的 free 函数(避免 std::free 与 poolAlloc 不匹配)
    blockFree(b);
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
  size_t headroom = g_urma_headroom.load(std::memory_order_acquire);
  size_t blockDataCap = gIoBufBlockSize - sizeof(IoBufBlock) - headroom;
  if (min_capacity <= blockDataCap) {
    c.current_share = ioBufBlockAllocate();
  } else {
    size_t alloc_size = min_capacity + sizeof(IoBufBlock) + headroom;
    // Pool tier cap: 分级池最大档位为 1M,超出即 poolAlloc 返回 null。
    // 此处将请求截断到最大档位 —— 上层序列化框架(fbthrift
    // preprocessSendT/serializeResponse)保持原始调用形式
    // IOBuf::create(kHeadroomBytes + bufSize) 不感知池限制,
    // 剩余数据由 IOBufQueue::pushAtMost → preallocateSlow 自动多块链化
    // (preallocateSlow 已有 kUrmaMaxBlockAlloc = 1M-256 上限)。
    size_t pool_tier_max = 1024 * 1024;  // URMA_LARGE_BUF_SIZE
    if (alloc_size > pool_tier_max) {
      alloc_size = pool_tier_max;
    }
    c.current_share = allocateFromSystem(alloc_size);
  }
  return c.current_share;
}

TLSBlockCache::~TLSBlockCache() {
  if (current_share != nullptr) {
    if (current_share->ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      blockFree(current_share);
    }
    current_share = nullptr;
  }
  for (size_t i = 0; i < count; ++i) {
    blockFree(blocks[i]);
  }
  count = 0;
}

} // namespace folly
