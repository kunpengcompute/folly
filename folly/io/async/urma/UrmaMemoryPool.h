/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <urma_api.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace folly {
enum class UrmaMemoryPurpose : size_t {
  Rx,
  Tx,
  Control,
  ReadSource,
  ReadTarget,
  ReadControl,  // READ completion messages have a separate progress reserve.
  Count
};
constexpr size_t kUrmaMemoryPurposes = size_t(UrmaMemoryPurpose::Count);
struct UrmaPoolClass {
  size_t bytes;
  std::array<size_t, kUrmaMemoryPurposes> reserve;
  bool operator==(const UrmaPoolClass& other) const {
    return bytes == other.bytes && reserve == other.reserve;
  }
};
struct UrmaPoolConfig {
  bool enabled{true};
  // Process-wide IOBuf allocation mode after UrmaInit; misses use normal heap.
  bool zero_copy{true};
  // Initial process-wide capacity, not the size of each region.
  size_t pool_size{256 * 1024 * 1024};
  // Zero means pool_size. Set elastic=false to keep a fixed registered pool.
  size_t pool_max_size{512 * 1024 * 1024};
  bool elastic{true};
  size_t tls_block_size{16384};
  size_t tls_cache_blocks{2};
  // Optional growth unit; pool_size remains the initial total capacity.
  size_t region_bytes{4 * 1024 * 1024};
  // Maximum held physical bytes per purpose. Class reserves protect progress.
  std::array<size_t, kUrmaMemoryPurposes> budgets{
      {32 * 1024 * 1024, 16 * 1024 * 1024, 4 * 1024 * 1024, 102 * 1024 * 1024,
       102 * 1024 * 1024, 4 * 1024 * 1024}};
  std::vector<UrmaPoolClass> classes{
      {4096, {{0, 4, 4, 2, 2}}},
      {16384, {{0, 0, 0, 2, 2}}},
      {65536, {{0, 0, 0, 1, 1}}},
      {262144, {{4, 0, 0, 1, 1}}},
      {1048576, {{0, 0, 0, 1, 1}}},
      // Grow intermediate large classes on demand; a protocol header must not
      // turn a 1 MiB payload into a 16 MiB allocation/clear on every request.
      {2097152, {{0, 0, 0, 0, 0}}},
      {4194304, {{0, 0, 0, 0, 0}}},
      {8388608, {{0, 0, 0, 0, 0}}},
      // An 8 MiB body also needs serializer/TLS metadata. Keep those small
      // additions from doubling each source and target's physical charge.
      {8388608 + 65536, {{0, 0, 0, 0, 0}}},
      {16777216, {{0, 0, 0, 1, 1}}}};
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
  std::array<size_t, kUrmaMemoryPurposes> held_by_purpose{};
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
  void signal() noexcept;
  void consume() noexcept;
  void stop() noexcept { enabled_.store(false, std::memory_order_release); }

 private:
  const int fd_;
  std::atomic<bool> enabled_{true}, pending_{false};
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
  UrmaPoolBuffer allocate(size_t bytes, UrmaMemoryPurpose purpose);
  UrmaPoolBuffer lookup(const void* address, size_t length, bool exported);
  // Connection hold for the only segment advertised in HELLO.
  UrmaPoolBuffer initialSegment();
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
