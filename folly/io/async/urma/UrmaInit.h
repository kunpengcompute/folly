/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <urma_api.h>
#include <folly/io/async/urma/UrmaConfig.h>
namespace folly {
class UrmaInit {
 public:
  static void init(const UrmaConfig& config);
  // Call after every transport has been destroyed. A live owner prevents fini.
  static void fini();
  static bool isInitialized();
  static const UrmaConfig& getConfig();
  static urma_eid_t getLocalEid();
  static urma_context_t* acquireContext();
  static void releaseContext();
};
} // namespace folly
#endif
