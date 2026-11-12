/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <functional>
namespace folly::urma_detail {
// Internal, process-wide setup workers. No SDK/context is acquired here.
void warmResourceExecutor();
bool submitPreparation(std::function<void()>);
}  // namespace folly::urma_detail
#endif
