/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
#include <folly/io/async/urma/UrmaCleanup.h>
#ifdef FOLLY_HAVE_URMA
#include <algorithm>
#include <condition_variable>
#include <cstdio>
#include <exception>
#include <mutex>
#include <thread>
#include <folly/logging/xlog.h>

namespace folly::urma_detail {
namespace {
using Clock = std::chrono::steady_clock;
class CleanupQueue {
 public:
  ~CleanupQueue() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
      changed_.notify_all();
    }
    if (worker_.joinable())
      worker_.join();
    // An SDK failure is not evidence that DMA stopped. Never force-free the
    // remaining tasks on library/process shutdown, either.
    // Logger singletons may already have shut down by the time this static
    // queue is destroyed. Keep the final retained-resource record observable.
    for (auto* task = head_; task; task = task->next)
      std::fprintf(
          stderr,
          "URMA cleanup retained at shutdown id=%llu stage=%s status=%d attempts=%llu\n",
          static_cast<unsigned long long>(task->id), task->stage, task->status,
          static_cast<unsigned long long>(task->attempts));
  }
  void add(CleanupTask* task) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    task->id = ++nextId_;
    task->attempts = 1; // The immediate teardown already failed.
    task->retryAt = Clock::now() + std::chrono::milliseconds(10);
    append(task);
    ++pending_;
    XLOG(ERR) << "URMA cleanup deferred id=" << task->id
              << " stage=" << task->stage << " status=" << task->status
              << " pending=" << pending_;
    startWorker();
    changed_.notify_all();
  }
  size_t pending() {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_;
  }
  bool wait(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (pending_)
      startWorker(); // Retry thread creation if an earlier attempt failed.
    return changed_.wait_for(lock, timeout, [this] { return pending_ == 0; });
  }

 private:
  void append(CleanupTask* task) noexcept {
    task->next = head_;
    head_ = task;
  }
  void startWorker() noexcept {
    if (worker_.joinable() || stopping_)
      return;
    try {
      worker_ = std::thread([this] { run(); });
    } catch (const std::exception& e) {
      // Tasks remain tracked and retain their context references. A subsequent
      // enqueue/fini can retry; never discard ownership on thread exhaustion.
      XLOG(ERR) << "URMA cleanup worker could not start: " << e.what();
    }
  }
  void run() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
      if (!head_) {
        changed_.wait(lock, [this] { return stopping_ || head_; });
        continue;
      }
      auto** selected = &head_;
      for (auto** p = &head_; *p; p = &(*p)->next)
        if ((*p)->retryAt < (*selected)->retryAt)
          selected = p;
      auto* task = *selected;
      if (Clock::now() < task->retryAt) {
        changed_.wait_until(lock, task->retryAt);
        continue;
      }
      *selected = task->next;
      task->next = nullptr;
      lock.unlock();
      ++task->attempts;
      const bool done = task->tryCleanup();
      if (done) {
        XLOG(INFO) << "URMA cleanup recovered id=" << task->id
                   << " attempts=" << task->attempts;
        delete task;
      } else {
        // Log powers of two, and back off to at most one attempt per second.
        // A permanently failing resource must not spin or flood the log.
        if (!(task->attempts & (task->attempts - 1)))
          XLOG(ERR) << "URMA cleanup still pending id=" << task->id
                    << " stage=" << task->stage << " status=" << task->status
                    << " attempts=" << task->attempts;
        const auto delay = std::min<uint64_t>(
            1000, uint64_t(10) << std::min<uint64_t>(task->attempts - 1, 7));
        task->retryAt = Clock::now() + std::chrono::milliseconds(delay);
      }
      lock.lock();
      if (done)
        --pending_;
      else
        append(task);
      changed_.notify_all();
    }
  }
  std::mutex mutex_;
  std::condition_variable changed_;
  std::thread worker_;
  CleanupTask* head_{nullptr};
  size_t pending_{0}; // Includes the task currently being retried.
  uint64_t nextId_{0};
  bool stopping_{false};
};
CleanupQueue& queue() {
  static CleanupQueue value;
  return value;
}
} // namespace

void cleanupOrDefer(std::unique_ptr<CleanupTask> task) noexcept {
  if (task && !task->tryCleanup())
    queue().add(task.release());
}
size_t pendingCleanups() {
  return queue().pending();
}
bool waitForCleanups(std::chrono::milliseconds timeout) {
  return queue().wait(timeout);
}
} // namespace folly::urma_detail
#endif
