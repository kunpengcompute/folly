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
#include <string>

#include <ub/umdk/urma/urma_api.h>
#include <folly/logging/xlog.h>

namespace folly {

// ── 默认值常量 ──
constexpr uint32_t kUrmaDefaultRxDepth = 4096;
constexpr uint32_t kUrmaDefaultFcRxDepth = 1024;
constexpr uint32_t kUrmaDefaultTxDepth = 1024;
constexpr uint32_t kUrmaDefaultFcTxDepth = 64;
constexpr uint32_t kUrmaDefaultIdleTimeoutMs = 10;
constexpr uint64_t kUrmaDefaultPoolSize = 64ULL * 1024 * 1024 * 1024; // 64GB

// ── 池化默认值(设计 §12) ──
constexpr uint32_t kUrmaDefaultIoJfsCount = 64;
constexpr uint32_t kUrmaDefaultFcJfsCount = 16;
constexpr uint32_t kUrmaDefaultIoJfrCount = 16;
constexpr uint32_t kUrmaDefaultFcJfrCount = 4;
constexpr uint32_t kUrmaDefaultMinWater = 64;
constexpr uint32_t kUrmaDefaultMaxWater = 256;
constexpr uint32_t kUrmaDefaultBatchPost = 32;

// ── 硬件限制校验常量 ──
constexpr uint32_t kUrmaTxJfcReservedSlots = 1; // flush_done CQE 预留 1 个 slot

struct UrmaConfig {
  // ── URMA 设备选择 ──
  // 空字符串表示使用第一个可用设备(参照 brpc FLAGS_urma_device)
  std::string dev_name;

  // ── JFS 池(发送侧,设计 §12) ──
  uint32_t io_jfs_count = kUrmaDefaultIoJfsCount;  // IO JFS 数量(进程级共享)
  uint32_t fc_jfs_count = kUrmaDefaultFcJfsCount;  // FC JFS 数量
  uint32_t tx_depth = kUrmaDefaultTxDepth;          // 每个 IO JFS 的 SQ 深度
  uint32_t fc_tx_depth = kUrmaDefaultFcTxDepth;     // 每个 FC JFS 的 SQ 深度

  // ── JFR 池(接收侧,设计 §12) ──
  uint32_t io_jfr_count = kUrmaDefaultIoJfrCount;  // IO JFR 数量(进程级共享)
  uint32_t fc_jfr_count = kUrmaDefaultFcJfrCount;  // FC JFR 数量
  uint32_t rx_depth = kUrmaDefaultRxDepth;          // 每个 JFR 的 RQ 深度
  uint32_t fc_rx_depth = kUrmaDefaultFcRxDepth;     // 每个 FC JFR 的 RQ 深度

  // ── recv buffer 按需投递(设计 §7,DPDK 水位线) ──
  uint32_t min_water = kUrmaDefaultMinWater;  // 最低水位(低于此值补投)
  uint32_t max_water = kUrmaDefaultMaxWater;  // 最高水位(补投到此值)
  uint32_t batch_post = kUrmaDefaultBatchPost; // 每批补投数量

  // ── 流控 ──
  uint32_t idle_timeout_ms = kUrmaDefaultIdleTimeoutMs;

  // ── 握手 ──
  uint32_t handshake_timeout_ms = 5000;  // URMA 握手超时(毫秒)

  // ── MR 内存池 ──
  uint64_t pool_size = kUrmaDefaultPoolSize;

  // ── 硬件限制校验 ──
  // 超限 clamp 到硬件上限,日志告警(不 crash,不阻止启动)。
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
    if (fc_tx_depth > max_jfs_depth) {
      XLOG(WARN) << "fc_tx_depth clamp " << fc_tx_depth << " -> " << max_jfs_depth;
      fc_tx_depth = max_jfs_depth;
    }
    // JFS 池总量校验
    uint32_t total_jfs = io_jfs_count + fc_jfs_count;
    if (total_jfs > cap.max_jfs) {
      XLOG(WARN) << "JFS total " << total_jfs << " exceed max_jfs " << cap.max_jfs
                 << ", clamping io_jfs_count";
      io_jfs_count = std::max(1u, cap.max_jfs - fc_jfs_count);
    }
    // JFR 池总量校验
    uint32_t total_jfr = io_jfr_count + fc_jfr_count;
    if (total_jfr > cap.max_jfr) {
      XLOG(WARN) << "JFR total " << total_jfr << " exceed max_jfr " << cap.max_jfr
                 << ", clamping io_jfr_count";
      io_jfr_count = std::max(1u, cap.max_jfr - fc_jfr_count);
    }
  }
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
