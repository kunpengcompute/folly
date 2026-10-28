/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaSeqReorder: RX seq 级保序重排器(SDD 任务:保序功能增强)。
 *
 *   URMA SEND 为可靠有序传输,正常情况下 seq 严格递增;重排器为防御机制,
 *   容忍异常场景(多 JFR 共享收割、硬件异常)下的瞬态乱序:
 *     - 收到 expected seq     → 立即交付,并连带交付后续连续项
 *     - 收到未来 seq(空洞)    → 入重排缓冲(上限 kMaxDepth 防洪泛)
 *     - 收到过时/重复 seq     → 丢弃(返回 false,调用方回收 slot)
 *     - 空洞超时未补齐        → 放行缓冲全部并重同步(WARN,真丢包 = 连接异常)
 *
 *   "重试"语义说明:SEND 语义无重传通道(区别于大包 PULL_MEM 已有的
 *   scanBigMsgTimeout 7s×3 重发);本机制的重试 = 重排缓冲等待 + 超时兜底放行。
 *
 *   独立功能开关:UrmaConfig::rx_reorder_enable(默认关,env URMA_RX_REORDER=1)。
 *   本文件不依赖 urma_api,可独立单测(urma/test/UrmaSeqReorderTest.cpp)。
 *   仅在 socket 属主 EVB 线程使用,无锁。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <utility>
#include <vector>

#include <folly/io/IOBuf.h>

namespace folly {

class UrmaSeqReorder {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;
  // 重排缓冲深度上限(防洪泛;超限时调用方应直接放行重同步)
  static constexpr size_t kMaxDepth = 64;

  UrmaSeqReorder() = default;
  ~UrmaSeqReorder() = default;
  UrmaSeqReorder(const UrmaSeqReorder&) = delete;
  UrmaSeqReorder& operator=(const UrmaSeqReorder&) = delete;

  // seq 距离:a 相对 b 的有向距离(处理 uint16 回绕)
  //   > 0: a 在 b 之后(未来)   < 0: a 在 b 之前(过时)   0: 相等
  static int32_t seqDist(uint16_t a, uint16_t b) {
    return static_cast<int32_t>(static_cast<int16_t>(a - b));
  }

  // 入缓冲(seq 必须为未来帧,由调用方判定)。缓冲满时返回 false(调用方
  // 应放行重同步,防异常洪泛)
  bool push(uint16_t seq, std::unique_ptr<IOBuf> buf, TimePoint now) {
    if (pending_.size() >= kMaxDepth || !buf) {
      return false;
    }
    pending_.emplace(seq, std::make_pair(std::move(buf), now));
    return true;
  }

  // 按 expected 弹出连续帧:弹出 seq == expected 的项并推进 expected,
  // 连带弹出后续连续项,按序追加到 out。
  // 返回弹出数量(0 = expected 仍缺失)
  size_t drainInOrder(uint16_t& expected,
                      std::vector<std::unique_ptr<IOBuf>>& out) {
    size_t popped = 0;
    while (true) {
      auto it = pending_.find(expected);
      if (it == pending_.end()) {
        break;
      }
      out.push_back(std::move(it->second.first));
      pending_.erase(it);
      ++popped;
      expected = static_cast<uint16_t>(expected + 1);
    }
    return popped;
  }

  // 超时兜底:缓冲非空且最早项年龄 > timeout_ms → 放行全部(按 seq 升序),
  // expected 重同步为最大 seq+1,返回 true(调用方打 WARN + 计数)
  bool releaseOnTimeout(uint16_t& expected, uint32_t timeout_ms,
                        std::vector<std::unique_ptr<IOBuf>>& out,
                        TimePoint now) {
    if (pending_.empty()) {
      return false;
    }
    auto oldest = pending_.begin()->second.second;
    if (now - oldest < std::chrono::milliseconds(timeout_ms)) {
      return false;
    }
    for (auto& kv : pending_) {
      out.push_back(std::move(kv.second.first));
      expected = static_cast<uint16_t>(kv.first + 1);
    }
    pending_.clear();
    return true;
  }

  size_t size() const { return pending_.size(); }
  bool empty() const { return pending_.empty(); }

  // 缓冲中最小/最大 seq(诊断打点用);空时返回 false
  bool range(uint16_t* lo, uint16_t* hi) const {
    if (pending_.empty()) return false;
    if (lo) *lo = pending_.begin()->first;
    if (hi) *hi = pending_.rbegin()->first;
    return true;
  }

 private:
  // seq → (帧数据, 入缓冲时刻)
  // 注:精确 find(expected) 匹配,不依赖跨回绕的全序
  std::map<uint16_t, std::pair<std::unique_ptr<IOBuf>, TimePoint>> pending_;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
