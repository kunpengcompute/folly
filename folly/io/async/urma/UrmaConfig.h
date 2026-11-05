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
#include <cstdlib>
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

// ── 水位线默认值(设计 §12) ──
constexpr uint32_t kUrmaDefaultMinWater = 64;
constexpr uint32_t kUrmaDefaultMaxWater = 256;
constexpr uint32_t kUrmaDefaultBatchPost = 32;

// ── 硬件限制校验常量 ──
constexpr uint32_t kUrmaTxJfcReservedSlots = 1; // flush_done CQE 预留 1 个 slot

struct UrmaConfig {
  // ── URMA 设备选择 ──
  // 空字符串表示使用第一个可用设备(参照 brpc FLAGS_urma_device)
  std::string dev_name;

  // ── IO 线程数(信息字段,由上层 ThriftServer 设置) ──
  // SDD-URMA-EVBEXCLUSIVE-001: per-EVB 独占四件套后无需池对齐,
  // 保留字段向后兼容(ThriftServer.cpp 仍设置此值)
  uint32_t num_io_threads = 0;

  // ── JFS(发送侧,per-EVB 独占,SDD-URMA-EVBEXCLUSIVE-001) ──
  uint32_t tx_depth = kUrmaDefaultTxDepth;          // IO JFS 的 SQ 深度
  uint32_t fc_tx_depth = kUrmaDefaultFcTxDepth;     // FC JFS 的 SQ 深度

  // ── JFR(接收侧,per-EVB 独占,SDD-URMA-EVBEXCLUSIVE-001) ──
  uint32_t rx_depth = kUrmaDefaultRxDepth;          // IO JFR 的 RQ 深度
  uint32_t fc_rx_depth = kUrmaDefaultFcRxDepth;     // FC JFR 的 RQ 深度

  // ── recv buffer 按需投递(设计 §7,DPDK 水位线) ──
  uint32_t min_water = kUrmaDefaultMinWater;  // 最低水位(低于此值补投)
  uint32_t max_water = kUrmaDefaultMaxWater;  // 最高水位(补投到此值)
  uint32_t batch_post = kUrmaDefaultBatchPost; // 每批补投数量

  // ── 流控 ──
  uint32_t idle_timeout_ms = kUrmaDefaultIdleTimeoutMs;

  // FC 流控闭环开关(默认关:close_loop 场景 credit 软限制为净开销,
  // per-EVB 独占 + 水位线 + recv buffer 池已防溢出)。
  // env URMA_FC_ENABLE=1 启用 REQ/REP/RETURN/ACK 完整闭环。
  bool fc_enable = false;

  // ── 保序(任务:保序功能增强) ──
  // RX seq 级重排(默认关:per-EVB 独占 JFR + TX/RX 独立 JFC 下
  // URMA SEND 可靠有序,seq 严格递增,重排器恒空)。
  // env URMA_RX_REORDER=1 启用(多 JFR 共享收割等瞬态乱序场景)。
  bool rx_reorder_enable = false;
  uint32_t reorder_timeout_ms = 200;  // 空洞等待上限

  // ── 握手 ──
  uint32_t handshake_timeout_ms = 30000;  // URMA 握手超时(毫秒)

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
    // SDD-URMA-EVBEXCLUSIVE-001: 池数量字段已移除
    // per-EVB 独占四件套,每 EVB 恰 1 IO JFS + 1 FC JFS + 1 IO JFR + 1 FC JFR
    // 硬件限制由 UrmaTransportContext 构造时的 urma_create_jfs/jfr 错误码检查
  }
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
