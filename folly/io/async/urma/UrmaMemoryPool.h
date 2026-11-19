/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <urma_api.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace folly {
struct UrmaPoolClass {
  size_t bytes;
  bool operator==(const UrmaPoolClass& other) const { return bytes == other.bytes; }
};
struct UrmaPoolConfig {
  bool enabled{true};
  // Process-wide IOBuf allocation mode after UrmaInit; misses throw bad_alloc.
  bool zero_copy{true};
  // Initial process-wide capacity, not the size of each region.
  size_t pool_size{256 * 1024 * 1024};
  // Zero means pool_size. Set elastic=false to keep a fixed registered pool.
  size_t pool_max_size{512 * 1024 * 1024};
  bool elastic{true};
  // Requests including slice metadata must fit this size to enter TLS.
  // Retained capacity per thread is at most tls_cache_blocks times the pool's
  // rounded allocation size for tls_block_size; live uncached users are extra.
  size_t tls_block_size{16384};
  size_t tls_cache_blocks{2};
  // Optional growth unit; pool_size remains the initial total capacity.
  size_t region_bytes{4 * 1024 * 1024};
  std::vector<UrmaPoolClass> classes{
      {4096}, {16384}, {65536}, {262144}, {1048576}, {2097152}, {4194304},
      {8388608}, {8388608 + 65536}, {16777216}};
  uint32_t idle_ms{1000};
  uint32_t retry_ms{100};
  bool operator==(const UrmaPoolConfig& other) const;
  bool operator!=(const UrmaPoolConfig& other) const {
    return !(*this == other);
  }
};
struct UrmaPoolStats {
  size_t soft_bytes{0}, hard_bytes{0};
  size_t registered_bytes{0}, registering_bytes{0}, allocated_bytes{0};
  size_t dma_bytes{0}, exported_bytes{0}, free_bytes{0}, retiring_bytes{0};
  size_t regions{0}, allocation_failures{0}, registration_failures{0};
  size_t unregister_failures{0};
  size_t fast_allocations{0};
  size_t tls_retained_bytes{0};
  uint64_t register_calls{0}, unregister_calls{0};
  uint64_t register_nanoseconds{0}, unregister_nanoseconds{0};
};
struct UrmaPoolRegionInfo {
  uint64_t id;
  size_t bytes, allocated_blocks, dma_refs, exported_read_refs;
  bool retiring;
  int last_error;
};
namespace urma_detail {
class MemoryPool;
struct MemoryPin;
}  // namespace urma_detail
// One eventfd per owner, not per release. The handle may outlive the owner;
// stop() disables notifications while still accepting late block returns.
class UrmaPoolSignal {
 public:
  UrmaPoolSignal();
  ~UrmaPoolSignal();
  int fd() const noexcept { return fd_; }
  // Arm before checking a resource again, or while holding its pool lock.
  // Resource returns wake one pending attempt; control events use signal().
  void arm() noexcept { waiting_.store(true); }
  void notify() noexcept {
    if (waiting_.load() && waiting_.exchange(false)) signal();
  }
  void signal() noexcept;
  void consume() noexcept;
  void stop() noexcept { enabled_.store(false, std::memory_order_release); }

 private:
  const int fd_;
  std::atomic<bool> enabled_{true}, pending_{false};
  std::atomic<bool> waiting_{false};
};
// Copying a handle preserves its original allocator and registration lifetime.
// A DMA/export pin remains valid after the allocation handle is released.
class UrmaPoolBuffer {
 public:
  UrmaPoolBuffer() = default;
  void* data() const;
  size_t capacity() const;
  urma_target_seg_t* segment() const;
  uint64_t regionId() const;
  explicit operator bool() const { return bool(pin_); }

 private:
  friend class urma_detail::MemoryPool;
  explicit UrmaPoolBuffer(std::shared_ptr<urma_detail::MemoryPin> pin)
      : pin_(std::move(pin)) {}
  std::shared_ptr<urma_detail::MemoryPin> pin_;
};
namespace urma_detail {
// Internal implementation owned by UrmaInit, never a second URMA context.
class MemoryPool {
 public:
  static void validate(const UrmaPoolConfig& config);
  MemoryPool(urma_context_t* context, const UrmaPoolConfig& config);
  ~MemoryPool();
  UrmaPoolBuffer allocate(size_t bytes, UrmaPoolSignal* waiter = nullptr);
  UrmaPoolBuffer lookup(const void* address, size_t length, bool exported);
  // Region leases prevent unregister while a peer can still reference a segment.
  UrmaPoolBuffer initialSegment();
  UrmaPoolBuffer holdSegment(uint64_t regionId);
  // Cold-path snapshot: no new allocation/export, and all block users drained.
  std::vector<uint64_t> revocableSegments() const;
  size_t bufferSize(size_t requested) const noexcept;
  void setCached(const UrmaPoolBuffer&, bool cached);
  bool reusable(const UrmaPoolBuffer&, bool recycle = false) const;
  const UrmaPoolConfig& config() const noexcept;
  const void* identity() const noexcept;
  void subscribe(const std::shared_ptr<UrmaPoolSignal>&);
  void retire(const UrmaPoolBuffer& buffer);
  void trim();
  bool warm(std::chrono::milliseconds timeout);
  bool shutdown(std::chrono::milliseconds timeout);
  UrmaPoolStats stats() const;
  std::vector<UrmaPoolRegionInfo> regions() const;

 private:
  struct State;
  std::shared_ptr<State> state_;
};
}  // namespace urma_detail
}  // namespace folly
#endif
