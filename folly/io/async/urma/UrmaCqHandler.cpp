/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaCqHandler.h>

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaSharedJfr.h>

namespace folly {

void UrmaCqHandler::handlerReady(uint16_t /*events*/) noexcept {
  if (is_tx_) {
    // TX JFCE: CQE 到达(READ completion 或 SQ 解除 EAGAIN) → ack + pollTx + retryWrite
    if (socket_) {
      urma_jfc_t* jfcs[1] = {socket_->txJfc()};
      uint32_t nevents[1] = {1};
      urma_ack_jfc(jfcs, nevents, 1);
      socket_->pollTx();
      socket_->retryWrite();
    }
  } else if (is_fc_) {
    if (shared_) {
      shared_->pollFcAndDispatch();
    }
  } else {
    if (shared_) {
      shared_->pollAndDispatch();
    }
  }
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
