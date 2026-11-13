/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/io/IOBuf.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <urma_api.h>

#include <cstddef>
namespace folly {
class UrmaInit {
 public:
  // Compatibility view used by the shared owner; this is a lease from the
  // same PR22 allocator, never a second registered pool.
  struct Buffer {
    void* memory{nullptr};
    urma_target_seg_t* segment{nullptr};
    size_t size{0};
    UrmaPoolBuffer hold;
  };
  static std::unique_ptr<IOBuf> takeBuffer(Buffer buffer, size_t length);
  static Buffer allocateBuffer(size_t bytes,
      UrmaPoolSignal* waiter = nullptr);
  // Acquires the process SDK context and registered pool. Queue resources are
  // prepared by each EventBase's shared owner when first needed.
  static void init(const UrmaConfig& config);
  // Requests shutdown and rejects new transports. Existing owners must still
  // be released by the caller; cleanup then retries automatically, including
  // failed SDK teardown. Waits briefly, without force-freeing live DMA memory.
  // isInitialized() stays true until deferred shutdown has completed.
  static void fini();
  static bool isInitialized();
  // Includes tasks currently retrying SDK teardown after their socket closed.
  static size_t pendingCleanupCount();
  static const UrmaConfig& getConfig();
  static urma_eid_t getLocalEid();
  static urma_context_t* acquireContext();
  static uint32_t getMaxReadSize();
  static void releaseContext();
  // A miss returns empty; background growth is requested only in elastic mode.
  static UrmaPoolBuffer poolAlloc(size_t size,
                                  UrmaPoolSignal* waiter = nullptr);
  static UrmaPoolBuffer lookupSegment(const void* addr, size_t length);
  static UrmaPoolBuffer holdReadExport(const void* addr, size_t length);
  static UrmaPoolBuffer initialSegment();
  static UrmaPoolBuffer holdSegment(uint64_t regionId);
  static std::vector<uint64_t> revocableSegments();
  static size_t poolBufSize(size_t requested);
  static void poolFree(UrmaPoolBuffer& block) noexcept { block = {}; }
  static void subscribePool(const std::shared_ptr<UrmaPoolSignal>& signal);
  static void retirePoolBuffer(const UrmaPoolBuffer& buffer);
  static void trimPool();
  static UrmaPoolStats poolStats();
  static std::vector<UrmaPoolRegionInfo> poolRegions();
  // Explicit opt-in allocator. No hidden prefix; length=0, capacity>=size.
  static std::unique_ptr<IOBuf> createPoolBuffer(
      size_t size);
  // Used by IOBuf::create only in explicitly enabled zero_copy mode.
  // Returns null only when interception is disabled; a pool miss throws.
  static std::unique_ptr<IOBuf> tryCreateIOBuf(size_t size);
  static bool poolIOBufAllocationEnabled();
  static void freePoolBuffer(void*, void*);
  static bool ownsPoolBuffer(const IOBuf& buffer);
  // Retains the allocation until the last IOBuf/clone is destroyed. Capacity
  // is the whole block; length alone identifies the initialized bytes.
  static std::unique_ptr<IOBuf> takePoolBuffer(UrmaPoolBuffer buffer,
                                               size_t length);
};
}  // namespace folly
#endif
