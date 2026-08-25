/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaResource: per-连接 URMA 句柄 POD 结构体。
 *
 *   每条连接一个,包含本端发送/接收句柄 + 远端导入句柄 + 共享层指针。
 *   连接建立时按序创建,连接关闭时按逆序销毁。
 *   共享层指针(shared_*)非 owned,由 UrmaSharedJfr 持有。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/urma/stub/urma_api.h>

namespace folly {

struct UrmaResource {
  // 本端发送/接收句柄
  urma_jetty_t* io_jetty = nullptr;
  urma_jetty_t* fc_jetty = nullptr;
  urma_jfc_t* tx_jfc = nullptr;
  urma_jfce_t* tx_jfce = nullptr;
  urma_jfc_t* fc_tx_jfc = nullptr;
  // FC JFS JFC 绑定同一个 tx_jfce(不单独创建 fc_tx_jfce)

  // 远端句柄
  urma_target_jetty_t* remote_jetty = nullptr;
  urma_target_jetty_t* remote_fc_jetty = nullptr;
  urma_target_seg_t* remote_seg = nullptr;

  // 共享层指针(非 owned,由 UrmaSharedJfr 持有)
  urma_jfr_t* shared_io_jfr = nullptr;
  urma_jfc_t* shared_io_jfc = nullptr;
  urma_jfr_t* shared_fc_jfr = nullptr;
  urma_jfc_t* shared_fc_jfc = nullptr;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
