/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/io/IOBuf.h>
#include <urma_api.h>

#include <memory>
namespace folly {
class UrmaPoolSignal;
namespace urma_detail {
class MemoryPool;
}
// Serialization and control TLS slices. RX/READ targets use complete blocks.
class IoBufPool {
 public:
  static std::unique_ptr<IOBuf> createFromPoolShared(
      const std::shared_ptr<urma_detail::MemoryPool>& pool, size_t capacity,
      UrmaPoolSignal* waiter = nullptr);
  static bool owns(const IOBuf&) noexcept;
  // Borrowed registration; the caller must retain the slice through DMA.
  static urma_target_seg_t* segment(const IOBuf&) noexcept;
  // No waiting for a busy allocating thread. Repeated shutdown passes drain
  // the remainder; live slices always retain their original allocator.
  static void reclaim(const void* poolIdentity, bool all) noexcept;
};
}  // namespace folly
#endif
