/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace folly::urma_detail {
// A task owns every resource that a failed SDK teardown may still access.
// The intrusive queue needs no allocation when a socket hands ownership over.
struct CleanupTask {
  virtual ~CleanupTask() = default;
  virtual bool tryCleanup() noexcept = 0;
  bool failed(const char* operation, int result) noexcept {
    stage = operation;
    status = result;
    return false;
  }
  const char* stage{"not-started"};
  int status{0};
  uint64_t id{0}, attempts{0};
  CleanupTask* next{nullptr};
  std::chrono::steady_clock::time_point retryAt{};
};

void cleanupOrDefer(std::unique_ptr<CleanupTask> task) noexcept;
size_t pendingCleanups();
bool waitForCleanups(std::chrono::milliseconds timeout);
} // namespace folly::urma_detail
#endif
