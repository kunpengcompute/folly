/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaConfig: URMA 传输配置 POD 结构体。
 *
 *   由上层(fbthrift ThriftServer / 客户端)在启动前设置,传入 UrmaInit::init。
 *   所有参数有默认值,不设置时按默认值工作。
 *
 *   硬件限制校验由 validateAndClamp 完成,超限 clamp 到硬件上限并日志告警(不 crash)。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <algorithm>
#include <cstdint>

#include <folly/io/async/urma/stub/urma_api.h>
#include <folly/logging/xlog.h>

namespace folly {

// ── 默认值常量 ──
constexpr uint32_t kUrmaDefaultRxDepth = 1024;
constexpr uint32_t kUrmaDefaultFcRxDepth = 1024;
constexpr uint32_t kUrmaDefaultTxDepth = 1024;
constexpr uint32_t kUrmaDefaultIdleTimeoutMs = 10;
constexpr uint64_t kUrmaDefaultPoolSize = 512ULL * 1024 * 1024; // 512MB

// ── 硬件限制校验常量 ──
constexpr uint32_t kUrmaJfrPerIoThread = 2;    // IO + FC,每 IO 线程 2 个 JFR
constexpr uint32_t kUrmaJettyPerConnection = 2; // IO + FC,每连接 2 个 jetty
constexpr uint32_t kUrmaTxJfcReservedSlots = 1; // flush_done CQE 预留 1 个 slot

struct UrmaConfig {
  // ── 共享 JFR 接收侧 ──
  uint32_t rx_depth = kUrmaDefaultRxDepth;
  uint32_t fc_rx_depth = kUrmaDefaultFcRxDepth;

  // ── 发送侧 ──
  enum JfsMode : uint8_t { JFS_SINGLE = 0, JFS_SHARED = 1 };
  JfsMode jfs_mode = JFS_SINGLE;
  uint32_t tx_depth = kUrmaDefaultTxDepth;

  // ── 流控 ──
  uint32_t idle_timeout_ms = kUrmaDefaultIdleTimeoutMs;

  // ── MR 内存池 ──
  uint64_t pool_size = kUrmaDefaultPoolSize;

  // ── 硬件限制校验 ──
  // 超限 clamp 到硬件上限,日志告警(不 crash,不阻止启动)。
  // io_thread_count / max_connections 由调用方提供(总量校验用)。
  void validateAndClamp(const urma_device_cap_t& cap,
                        uint32_t io_thread_count = 0,
                        uint32_t max_connections = 0) {
    uint32_t max_jfr_depth = std::min(cap.max_jfr_depth, cap.max_jfc_depth);
    if (rx_depth > max_jfr_depth) {
      XLOG(WARN) << "rx_depth clamp " << rx_depth << " -> " << max_jfr_depth;
      rx_depth = max_jfr_depth;
    }
    if (fc_rx_depth > max_jfr_depth) {
      XLOG(WARN) << "fc_rx_depth clamp " << fc_rx_depth << " -> " << max_jfr_depth;
      fc_rx_depth = max_jfr_depth;
    }
    uint32_t max_jfs_depth = std::min(
        cap.max_jfs_depth, cap.max_jfc_depth - kUrmaTxJfcReservedSlots);
    if (tx_depth > max_jfs_depth) {
      XLOG(WARN) << "tx_depth clamp " << tx_depth << " -> " << max_jfs_depth;
      tx_depth = max_jfs_depth;
    }
    if (io_thread_count > 0) {
      uint32_t total_jfr = kUrmaJfrPerIoThread * io_thread_count;
      if (total_jfr > cap.max_jfr) {
        XLOG(WARN) << "JFR total " << total_jfr << " exceed max_jfr " << cap.max_jfr;
      }
    }
    if (max_connections > 0) {
      uint32_t total_jetty = kUrmaJettyPerConnection * max_connections;
      if (total_jetty > cap.max_jetty) {
        XLOG(WARN) << "jetty total " << total_jetty << " exceed max_jetty "
                   << cap.max_jetty << ", consider JFS_SHARED mode";
      }
    }
  }
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
