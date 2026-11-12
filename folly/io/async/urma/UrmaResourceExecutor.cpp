/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaResourceExecutor.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/io/async/urma/UrmaCleanup.h>

#include <atomic>
namespace folly::urma_detail {
namespace {
struct PreparationExecutor {
  std::atomic<size_t> pending{0};
  CPUThreadPoolExecutor workers{std::make_pair(size_t(4), size_t(4))};
  PreparationExecutor() { warmCleanupExecutor(); }
  ~PreparationExecutor() { workers.join(); }
};
PreparationExecutor& executor() {
  static PreparationExecutor e;
  return e;
}
}  // namespace
void warmResourceExecutor() { (void)executor(); }
bool submitPreparation(std::function<void()> task) {
  auto& e = executor();
  if (e.pending.fetch_add(1, std::memory_order_relaxed) >= 4096) {
    e.pending.fetch_sub(1, std::memory_order_relaxed);
    return false;
  }
  try {
    e.workers.add([task = std::move(task), &e] {
      auto done =
          makeGuard([&] { e.pending.fetch_sub(1, std::memory_order_relaxed); });
      task();
    });
  } catch (...) {
    e.pending.fetch_sub(1, std::memory_order_relaxed);
    throw;
  }
  return true;
}
}  // namespace folly::urma_detail
#endif
