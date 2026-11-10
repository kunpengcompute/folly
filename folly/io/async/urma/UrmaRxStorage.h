/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/io/IOBuf.h>
#include <folly/io/async/urma/UrmaMemoryPool.h>
namespace folly::urma_detail {
struct RxBudget {
  std::atomic<size_t> retained{0};
  size_t limit;
  explicit RxBudget(size_t bytes) : limit(bytes) {}
  bool available(size_t bytes, UrmaPoolSignal& waiter) const {
    auto fits = [&] {
      const auto held = retained.load();
      return held <= limit && bytes <= limit - held;
    };
    if (fits()) return true;
    waiter.arm();
    // Sequentially consistent with the return and notification below. A
    // concurrent return either satisfies this check or observes the waiter.
    return fits();
  }
};
// Allocation handle retains the original pool/region; budgets and notification
// are independent of the socket and slot. Last IOBuf reference owns this lease.
struct RxStorageLease {
  UrmaPoolBuffer block, dma;
  std::shared_ptr<RxBudget> connectionBudget, ownerBudget;
  std::shared_ptr<UrmaPoolSignal> wakeup;
  bool accounted{false};
  ~RxStorageLease() {
    const auto bytes = block.capacity();
    if (accounted) {
      connectionBudget->retained.fetch_sub(bytes);
      ownerBudget->retained.fetch_sub(bytes);
    }
    dma = {};
    block = {};  // return physical storage before retrying delivery/refill
    if (wakeup) wakeup->notify();
  }
  static std::unique_ptr<IOBuf> take(std::unique_ptr<RxStorageLease> lease,
                                     size_t offset, size_t length) {
    if (!lease || !lease->block || offset > lease->block.capacity() ||
        length > lease->block.capacity() - offset || lease->dma)
      throw std::invalid_argument("RX lease still DMA-owned or invalid view");
    auto* base = lease->block.data();
    const auto capacity = lease->block.capacity();
    // freeOnError=false keeps the unique_ptr as the sole owner on failure.
    auto result = IOBuf::takeOwnership(
        base, capacity, offset, length,
        [](void*, void* cookie) noexcept {
          delete static_cast<RxStorageLease*>(cookie);
        },
        lease.get(), false);
    lease.release();
    return result;
  }
};
}  // namespace folly::urma_detail
#endif
