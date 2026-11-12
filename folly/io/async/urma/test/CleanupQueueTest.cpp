/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaResourceExecutor.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>
using namespace folly::urma_detail;
namespace {
void require(bool value) {
  if (!value) std::abort();
}
struct Task : CleanupTask {
  std::atomic<int>& destroyed;
  std::atomic<bool>& allow;
  unsigned step{0};
  explicit Task(std::atomic<int>& d, std::atomic<bool>& a)
      : destroyed(d), allow(a) {}
  ~Task() override {
    require(step == 3);
    ++destroyed;
  }
  bool tryCleanup() noexcept override {
    if (!allow) return failed("blocked", -1);
    if (++step < 3) return failed("transient", -2);
    return true;
  }
};
}  // namespace
int main() {
  warmResourceExecutor();
  std::atomic<unsigned> started{0}, completed{0};
  std::atomic<bool> release{false};
  for (unsigned i = 0; i < 4096; ++i)
    require(submitPreparation([&] {
      ++started;
      while (!release) std::this_thread::yield();
      ++completed;
    }));
  require(!submitPreparation([] {}));
  release = true;
  for (int i = 0; i < 500 && completed != 4096; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  require(started == 4096 && completed == 4096);
  std::atomic<int> done{0};
  std::atomic<bool> blocked{false}, allowed{true};
  cleanupOrDefer(std::make_unique<Task>(done, blocked));
  std::vector<std::thread> producers;
  for (int i = 0; i < 8; ++i)
    producers.emplace_back([&] {
      for (int j = 0; j < 16; ++j)
        cleanupOrDefer(std::make_unique<Task>(done, allowed));
    });
  for (auto& thread : producers) thread.join();
  require(!waitForCleanups(std::chrono::milliseconds(250)));
  for (int i = 0; i < 100 && done != 128; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  require(done == 128 && pendingCleanups() == 1);
  blocked = true;
  require(waitForCleanups(std::chrono::seconds(3)));
  require(done == 129 && pendingCleanups() == 0);
  std::puts(
      "PASS bounded preparation (4096 accepted/next rejected) and cleanup (129 "
      "tasks, concurrent enqueue, retries, ownership)");
}
