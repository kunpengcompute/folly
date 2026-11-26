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
 *   SDD-URMA-EVBEXCLUSIVE-001 任务 B:TX/RX JFC 分离后 4 实例
 *   io_rx_cq_handler → ctx->pollAndDispatch(false, false)(IO RX JFC)
 *   io_tx_cq_handler → ctx->pollAndDispatch(false, true) (IO TX JFC)
 *   fc_rx_cq_handler → ctx->pollAndDispatch(true,  false)(FC RX JFC)
 *   fc_tx_cq_handler → ctx->pollAndDispatch(true,  true) (FC TX JFC)
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/EventHandler.h>
#include <folly/net/NetworkSocket.h>

namespace folly {

class UrmaTransportContext;

class UrmaCqHandler : public EventHandler {
 public:
  // 统一构造:ctx 指向 UrmaTransportContext,is_fc 区分 IO/FC,is_tx 区分 TX/RX
  // epoll 触发时调用 ctx->pollAndDispatch(is_fc, is_tx)
  UrmaCqHandler(EventBase* evb, int fd,
                UrmaTransportContext* ctx, bool is_fc, bool is_tx)
      : EventHandler(evb, NetworkSocket(fd)),
        ctx_(ctx),
        is_fc_(is_fc),
        is_tx_(is_tx) {
    registerHandler(READ | PERSIST);
  }

  ~UrmaCqHandler() override {
    unregisterHandler();
  }

  void handlerReady(uint16_t /*events*/) noexcept override;

 private:
  UrmaTransportContext* ctx_;
  bool is_fc_;
  bool is_tx_;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
