/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/io/async/urma/UrmaAsyncSocket.h>
namespace folly {
struct UrmaTransportWrapper {
  // Returns a transport whose handshake is asynchronous; writes queue until
  // ready.
  static AsyncTransport::UniquePtr createAndOffload(EventBase* evb, int fd) {
    return AsyncTransport::UniquePtr(new UrmaAsyncSocket(evb, fd));
  }
};
} // namespace folly
#endif
