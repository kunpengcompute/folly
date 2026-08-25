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
    // TX JFCE: SQ 满(EAGAIN)后 CQE 到达 → pollTx 回收 sbuf_ + retryWrite
    if (socket_) {
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
