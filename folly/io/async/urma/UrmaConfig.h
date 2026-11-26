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
constexpr uint32_t kUrmaDefaultFcRxDepth = 4096;
constexpr uint32_t kUrmaDefaultTxDepth = 1024;
// FC TX 深度必须 >= 单 EVB 连接数:1ms tick 会对每条连接各发一个 GRANT,
// 64 深度在 100 连接/EVB 时整轮打满 → 大量 GRANT 发不出去 → 窗口饿死。
constexpr uint32_t kUrmaDefaultFcTxDepth = 2048;
constexpr uint32_t kUrmaDefaultIdleTimeoutMs = 10;
constexpr uint64_t kUrmaDefaultPoolSize = 64ULL * 1024 * 1024 * 1024; // 64GB

// ── 水位线默认值(设计 §12) ──
constexpr uint32_t kUrmaDefaultMinWater = 64;
constexpr uint32_t kUrmaDefaultMaxWater = 256;
constexpr uint32_t kUrmaDefaultBatchPost = 32;

// ── RX buffer 按需扩容(阶梯式增长) ──
// 初始只分配/投递 initial 个,使用率达到 high_watermark 时按 step 追加,
// 直到 rx_depth 上限。这样低频场景不必为满配付出内存与初始投递开销。
constexpr uint32_t kUrmaDefaultRxInitial = 1024;      // 初始分配/投递量
constexpr uint32_t kUrmaDefaultRxGrowStep = 512;      // 每次追加量
constexpr uint32_t kUrmaDefaultRxHighWaterPct = 80;   // 使用率达到此百分比触发扩容

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

  // ── 流控(receiver-driven credit,见《fbthrift URMA credit 流控重设计》) ──
  uint32_t idle_timeout_ms = kUrmaDefaultIdleTimeoutMs;

  // FC 流控开关(默认开:receiver-driven credit 是硬限流,从源头消除 RNR)。
  // env URMA_FC_ENABLE=0 可关闭(仅用于 A/B 对比或故障排查)。
  bool fc_enable = true;

  // 流控参数(设计 §9)
  uint32_t fc_initial_window = 16;      // 握手初始窗口(充足,不全给)
  uint32_t fc_min_window = 8;           // 水位目标保底(每连接最低额度)
  uint32_t fc_grant_period_ms = 1;      // 水位调整定时器周期
  uint32_t fc_idle_timeout_ms = 100;    // 空闲超时回收阈值
  uint32_t fc_reclaim_used_percent = 80; // 池子占用率阈值(占用 <此值不回收)

  // ── 保序(任务:保序功能增强) ──
  // RX seq 级重排(默认关:per-EVB 独占 JFR + TX/RX 独立 JFC 下
  // URMA SEND 可靠有序,seq 严格递增,重排器恒空)。
  // env URMA_RX_REORDER=1 启用(多 JFR 共享收割等瞬态乱序场景)。
  bool rx_reorder_enable = false;
  uint32_t reorder_timeout_ms = 200;  // 空洞等待上限

  // ── 轮询模式(默认关:中断模式 wait→poll→ack→rearm→poll) ──
  // env URMA_POLL_MODE=1 启用:pollAndDispatch 只调用 poll→rearm→poll,
  // 跳过 wait/ack。适用于 libevent LT 模式下 JFCE fd 恒可读场景,
  // 利用 epoll LT 持续触发 handlerReady 代替 urma_wait_jfc 事件消费。
  // 注意:轮询模式下 rearm 仍保留(部分驱动 rearm 清 fd 可读状态需要)。
  bool poll_mode = false;

  // ── 握手 ──
  uint32_t handshake_timeout_ms = 30000;  // URMA 握手超时(毫秒)

  // ── MR 内存池 ──
  uint64_t pool_size = kUrmaDefaultPoolSize;

  // ── RX buffer 按需扩容(阶梯式增长) ──
  // 初始只分配/投递 rx_initial 个;使用率(已占用/已分配)达到
  // rx_high_water_pct 时按 rx_grow_step 追加,直到 rx_depth 上限。
  //
  // 注意:新字段必须追加在结构体末尾。UrmaConfig 被预编译的
  // fbthrift 库(libthriftcpp2.a 的 ThriftServer.cpp.o,调用
  // UrmaInit::init(UrmaConfig const&))按值传递,在中间插入字段会
  // 改变后续字段偏移 → ABI 错位 → pool_size 被当作指针解引用 SIGSEGV。
  uint32_t rx_initial = kUrmaDefaultRxInitial;             // 初始分配/投递量
  uint32_t rx_grow_step = kUrmaDefaultRxGrowStep;          // 每次追加量
  uint32_t rx_high_water_pct = kUrmaDefaultRxHighWaterPct; // 扩容触发百分比

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
