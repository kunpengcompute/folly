/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaCqHandler: EventHandler 子类,将 JFCE fd 注册到 EventBase epoll。
 *
 *   IO JFCE → pollAndDispatch(数据)
 *   FC JFCE → pollFcAndDispatch(流控消息)
 *   TX JFCE → pollTx + retryWrite(Phase 4 EAGAIN 中断,Phase 2 仅 IO/FC)
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/EventHandler.h>
#include <folly/net/NetworkSocket.h>

namespace folly {

class UrmaSharedJfr;
class UrmaAsyncSocket;

class UrmaCqHandler : public EventHandler {
 public:
  // IO/FC 模式: is_tx_=false, shared_ 指向 UrmaSharedJfr
  // TX 模式: is_tx_=true, socket_ 指向 UrmaAsyncSocket
  UrmaCqHandler(EventBase* evb, int fd,
                UrmaSharedJfr* shared, bool is_fc)
      : EventHandler(evb, NetworkSocket(fd)),
        shared_(shared),
        socket_(nullptr),
        is_tx_(false),
        is_fc_(is_fc) {
    setEdgeTriggered();  // ET 模式:fd 状态变化时只触发一次(参照 brpc/UMQ EPOLLET)
    registerHandler(READ | PERSIST);
  }

  // TX 模式构造
  UrmaCqHandler(EventBase* evb, int fd, UrmaAsyncSocket* socket)
      : EventHandler(evb, NetworkSocket(fd)),
        shared_(nullptr),
        socket_(socket),
        is_tx_(true),
        is_fc_(false) {
    setEdgeTriggered();
    registerHandler(READ | PERSIST);
  }

  ~UrmaCqHandler() override {
    unregisterHandler();
  }

  void handlerReady(uint16_t /*events*/) noexcept override;

 private:
  UrmaSharedJfr* shared_;
  UrmaAsyncSocket* socket_;
  bool is_tx_;
  bool is_fc_;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
