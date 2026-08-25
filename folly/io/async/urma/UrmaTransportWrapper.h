/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaTransportWrapper: 服务端 accept 后创建 UrmaAsyncSocket + 启动握手。
 *
 *   createAndOffload(evb, fd):
 *     accept TCP fd → 创建 UrmaAsyncSocket(fd) → 启动服务端握手
 *     握手成功后返回 AsyncTransport::UniquePtr
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>

namespace folly {

class UrmaTransportWrapper {
 public:
  /**
   * 服务端:accept fd 后调用,创建 UrmaAsyncSocket 并启动服务端握手。
   * 返回 AsyncTransport::UniquePtr(握手成功后使用)。
   */
  static AsyncTransport::UniquePtr createAndOffload(EventBase* evb, int fd) {
    // UrmaAsyncSocket(fd) 内部调 startHandshake(false) → 服务端握手流程
    auto* socket = new UrmaAsyncSocket(evb, fd);
    return AsyncTransport::UniquePtr(socket);
  }
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
