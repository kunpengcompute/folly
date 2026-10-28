/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <urma_api.h>
#include <cstddef>
#include <folly/io/async/urma/UrmaConfig.h>
namespace folly {
class UrmaInit {
 public:
  static void init(const UrmaConfig& config);
  // Call after every transport has been destroyed. Waits up to 250 ms for
  // deferred cleanup; live/pending owners prevent context destruction. If
  // isInitialized() remains true, call fini() again after cleanup completes.
  static void fini();
  static bool isInitialized();
  // Includes tasks currently retrying SDK teardown after their socket closed.
  static size_t pendingCleanupCount();
  static const UrmaConfig& getConfig();
  static urma_eid_t getLocalEid();
  static urma_context_t* acquireContext();
  static uint32_t getMaxReadSize();
  static void releaseContext();
};
} // namespace folly
#endif
