/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/urma/UrmaInit.h>

#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <thread>

namespace folly::urma_test {
inline void require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}
inline const char* device() {
  const char* name = std::getenv("FBTHRIFT_URMA_TEST_DEVICE");
  return name ? name : "udmac0d1e2";
}
template <class Predicate>
void await(Predicate done, const char* message,
           std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto end = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < end) {
    if (done()) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  require(false, message);
}
inline void finalized() {
  await(
      [] {
        return !UrmaInit::isInitialized() && !UrmaInit::pendingCleanupCount();
      },
      "URMA shutdown did not converge after the final owner release");
}
inline void finalize() {
  UrmaInit::fini();
  finalized();
}
inline void stopListener(AsyncServerSocket& listener,
                         AsyncServerSocket::AcceptCallback& callback,
                         EventBase& evb) {
  listener.removeAcceptCallback(&callback, &evb);
  listener.stopAccepting();
  // acceptStopped is queued on the EventBase. Drain it while the non-owning
  // callback is still alive, rather than during EventBase destruction later.
  evb.loopOnce(EVLOOP_NONBLOCK);
}
}  // namespace folly::urma_test
