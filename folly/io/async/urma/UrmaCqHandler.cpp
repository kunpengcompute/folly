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

#include <folly/io/async/urma/UrmaTransportContext.h>

namespace folly {

void UrmaCqHandler::handlerReady(uint16_t /*events*/) noexcept {
  if (ctx_) {
    ctx_->pollAndDispatch(is_fc_, is_tx_);
  }
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
