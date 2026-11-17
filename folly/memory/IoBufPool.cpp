/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/memory/IoBufPool.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/Indestructible.h>
#include <folly/io/async/urma/UrmaMemoryPool.h>

#include <algorithm>
#include <array>
#include <limits>
#include <mutex>
#include <vector>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
namespace folly {
namespace {
// These bytes are local metadata, never a transport prefix. All offsets below
// are measured from payload(), so capacity/used/remaining share one coordinate.
struct alignas(std::max_align_t) BlockLayout {
  size_t capacity{0}, used{0};
  size_t padding() const {
    constexpr size_t alignment = alignof(std::max_align_t);
    return (alignment - used % alignment) % alignment;
  }
  bool fits(size_t bytes) const {
    const size_t remaining = capacity - used;
    const size_t pad = padding();
    return pad <= remaining && bytes <= remaining - pad;
  }
};
struct IoBufBlock : boost::intrusive_ref_counter<IoBufBlock> {
  std::shared_ptr<urma_detail::MemoryPool> pool;
  UrmaPoolBuffer storage;
  BlockLayout* layout;
  IoBufBlock(std::shared_ptr<urma_detail::MemoryPool> p, UrmaPoolBuffer b)
      : pool(std::move(p)),
        storage(std::move(b)),
        layout(new(storage.data())
                   BlockLayout{storage.capacity() - sizeof(BlockLayout), 0}) {}
  uint8_t* payload() const {
    return static_cast<uint8_t*>(storage.data()) + sizeof(BlockLayout);
  }
};
void freeSlice(void*, void* cookie) noexcept {
  intrusive_ptr_release(static_cast<IoBufBlock*>(cookie));
}
struct TLSBlockCache {
  std::mutex mutex;
  std::shared_ptr<urma_detail::MemoryPool> pool;
  std::array<boost::intrusive_ptr<IoBufBlock>, 16> blocks{};
  size_t current{0};
  explicit TLSBlockCache(std::shared_ptr<urma_detail::MemoryPool> p)
      : pool(std::move(p)) {}
  void drop(size_t i) noexcept {
    if (blocks[i]) {
      pool->setCached(blocks[i]->storage, false);
      blocks[i].reset();
    }
  }
  ~TLSBlockCache() {
    for (size_t i = 0; i < blocks.size(); ++i) drop(i);
  }
};
struct Registry {
  std::mutex mutex;
  std::vector<std::weak_ptr<TLSBlockCache>> caches;
};
Registry& registry() {
  static Indestructible<Registry> r;
  return *r;
}
thread_local std::shared_ptr<TLSBlockCache> localCache;
}  // namespace
std::unique_ptr<IOBuf> IoBufPool::createFromPoolShared(
    const std::shared_ptr<urma_detail::MemoryPool>& pool, size_t capacity,
    UrmaPoolSignal* waiter) {
  if (!capacity ||
      capacity > std::numeric_limits<size_t>::max() - sizeof(BlockLayout))
    return nullptr;
  if (!localCache || localCache->pool != pool) {
    auto cache = std::make_shared<TLSBlockCache>(pool);
    auto& r = registry();
    std::lock_guard<std::mutex> lock(r.mutex);
    r.caches.erase(std::remove_if(r.caches.begin(), r.caches.end(),
                                  [](const auto& p) { return p.expired(); }),
                   r.caches.end());
    r.caches.push_back(cache);
    localCache = std::move(cache);
  }
  auto& cache = *localCache;
  std::lock_guard<std::mutex> lock(cache.mutex);
  const size_t limit = pool->config().tls_cache_blocks;
  // Only small requests share TLS blocks. Larger registered allocations must
  // return to the public pool after their slices/DMA/export pins are released.
  const bool cacheable =
      capacity <= pool->config().tls_block_size - sizeof(BlockLayout);
  auto block = cacheable ? cache.blocks[cache.current] : nullptr;
  if (block && (!pool->reusable(block->storage) ||
                !block->layout->fits(capacity)))
    block.reset();
  if (!block && cacheable) {
    for (size_t i = 0; i < limit; ++i) {
      auto& b = cache.blocks[i];
      if (b && b->use_count() == 1 && b->layout->capacity >= capacity &&
          pool->reusable(b->storage, true)) {
        b->layout->used = 0;
        block = b;
        cache.current = i;
        break;
      }
    }
  }
  if (!block) {
    const auto bytes =
        std::max(pool->config().tls_block_size, capacity + sizeof(BlockLayout));
    auto storage = pool->allocate(bytes, waiter);
    if (!storage) {
      // A miss is not proof of physical exhaustion: release reusable TLS first.
      for (size_t i = 0; i < limit; ++i)
        if (cache.blocks[i] && cache.blocks[i]->use_count() == 1) cache.drop(i);
      reclaim(pool->identity(), false);  // current cache is locked and skipped
      storage = pool->allocate(bytes, waiter);
    }
    if (!storage) return nullptr;
    block = boost::intrusive_ptr<IoBufBlock>(new IoBufBlock(pool, std::move(storage)));
    if (limit && cacheable) {
      cache.current = (cache.current + 1) % limit;
      cache.drop(cache.current);
      cache.blocks[cache.current] = block;
      pool->setCached(block->storage, true);
    }
  }
  const auto offset = block->layout->used + block->layout->padding();
  // Reserve this non-overlapping slice even if IOBuf metadata allocation fails.
  block->layout->used = offset + capacity;
  // takeOwnership invokes freeSlice on failure as well as on final release.
  intrusive_ptr_add_ref(block.get());
  return IOBuf::takeOwnership(block->payload() + offset, capacity, 0, freeSlice,
                              block.get());
}
bool IoBufPool::owns(const IOBuf& b) noexcept {
  return b.isManagedOne() && b.getFreeFn() == freeSlice;
}
urma_target_seg_t* IoBufPool::segment(const IOBuf& b) noexcept {
  if (!owns(b)) return nullptr;
  return static_cast<IoBufBlock*>(b.getUserData())->storage.segment();
}
void IoBufPool::reclaim(const void* identity, bool all) noexcept {
  auto& r = registry();
  std::unique_lock<std::mutex> lock(r.mutex, std::try_to_lock);
  if (!lock) return;
  for (const auto& item : r.caches) {
    auto cache = item.lock();
    if (!cache || cache->pool->identity() != identity) continue;
    std::unique_lock<std::mutex> held(cache->mutex, std::try_to_lock);
    if (!held) continue;
    for (size_t i = 0; i < cache->blocks.size(); ++i)
      if (cache->blocks[i] && (all || cache->blocks[i]->use_count() == 1))
        cache->drop(i);
  }
}
}  // namespace folly
#endif
