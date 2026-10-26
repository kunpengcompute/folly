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
 *   TX JFCE → harvestTx(TX CQE 收割,含 READ CQE)
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
  // epoll 触发时调用 shared_->pollAndDispatch() 或 pollFcAndDispatch()
  UrmaCqHandler(EventBase* evb, int fd,
                UrmaSharedJfr* shared, bool is_fc)
      : EventHandler(evb, NetworkSocket(fd)),
        shared_(shared),
        socket_(nullptr),
        is_tx_(false),
        is_fc_(is_fc),
        is_fc_tx_(false),
        jfs_index_(0) {
    registerHandler(READ | PERSIST);
  }

  // TX 池模式: is_tx_=true, shared_ 指向 UrmaSharedJfr
  // epoll 触发时调用 shared_->harvestTx(jfs_index_) 单点收割绑定的 JFS
  // is_fc_tx=true 表示 FC JFS 的 TX CQE,调 harvestFcTx 而非 harvestTx
  UrmaCqHandler(EventBase* evb, int fd,
                UrmaSharedJfr* shared, bool is_tx, bool is_fc_tx,
                uint32_t jfs_index)
      : EventHandler(evb, NetworkSocket(fd)),
        shared_(shared),
        socket_(nullptr),
        is_tx_(is_tx),
        is_fc_(false),
        is_fc_tx_(is_fc_tx),
        jfs_index_(jfs_index) {
    registerHandler(READ | PERSIST);
  }

  // TX per-socket 模式(旧设计,保留兼容)
  UrmaCqHandler(EventBase* evb, int fd, UrmaAsyncSocket* socket)
      : EventHandler(evb, NetworkSocket(fd)),
        shared_(nullptr),
        socket_(socket),
        is_tx_(true),
        is_fc_(false),
        is_fc_tx_(false),
        jfs_index_(0) {
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
  bool is_fc_tx_;       // TX 模式下区分 FC JFS vs IO JFS
  uint32_t jfs_index_;  // TX 池模式绑定的 JFS index(单点收割)
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
