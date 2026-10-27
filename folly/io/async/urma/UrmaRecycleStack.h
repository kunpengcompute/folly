/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaRecycleStack: per-JFR RX slot 无锁回收栈(SDD-URMA-HFIX-001 任务 C)。
 *
 *   修复 H-3/H-4:RX buffer 所有权随 IOBuf 交付上层(零拷贝),deleter 可能
 *   在任意线程执行 → 不能直接 post;slot 入本栈,由收割线程批量重投。
 *
 *   实现:Treiber 无锁栈,head 压缩 (tag:32|slot:32) 防 ABA。
 *     - push: 任意线程(IOBuf deleter / 收割线程),wait-free CAS 循环
 *     - pop:  收割线程(多 EVB 并发安全),CAS 循环
 *     - 容量天然有界(≤ max_water):per-slot next 槽位数组,零堆分配
 *
 *   不可拷贝/移动(atomic 成员);SharedJfr 经 unique_ptr 持有以满足
 *   vector<SharedJfr> 的移动要求。
 *   本文件不依赖 urma_api.h,可独立单测(urma/test/UrmaRecycleStackTest.cpp)。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <cstdint>
#include <memory>

namespace folly {

class UrmaRecycleStack {
 public:
  static constexpr uint32_t kEmptySlot = 0xFFFFFFFFu;

  UrmaRecycleStack() = default;
  ~UrmaRecycleStack() = default;
  UrmaRecycleStack(const UrmaRecycleStack&) = delete;
  UrmaRecycleStack& operator=(const UrmaRecycleStack&) = delete;

  // 按 JFR 的 max_water 分配 per-slot next 数组(此后不再扩容)
  void init(uint32_t capacity) {
    next_ = std::make_unique<std::atomic<uint32_t>[]>(capacity);
    for (uint32_t i = 0; i < capacity; ++i) {
      next_[i].store(kEmptySlot, std::memory_order_relaxed);
    }
    capacity_ = capacity;
    head_.store(kNull, std::memory_order_relaxed);
    count_.store(0, std::memory_order_relaxed);
  }

  bool inited() const { return next_ != nullptr; }

  // 任意线程:slot 入栈(wait-free)
  void push(uint32_t slot) {
    if (slot >= capacity_) return;  // 越界防护(next_ 数组防 OOB)
    uint64_t h = head_.load(std::memory_order_acquire);
    while (true) {
      next_[slot].store(slotOf(h), std::memory_order_relaxed);
      // tag 单调递增(清空态也不归零):ABA 复现需 2^32 次操作,工程上不可达
      uint64_t nh = pack(tagOf(h) + 1, slot);
      if (head_.compare_exchange_weak(h, nh, std::memory_order_release,
                                       std::memory_order_acquire)) {
        break;
      }
    }
    count_.fetch_add(1, std::memory_order_release);
  }

  // 收割线程:弹出栈顶(空返回 false)
  bool pop(uint32_t* out_slot) {
    uint64_t h = head_.load(std::memory_order_acquire);
    while (true) {
      uint32_t slot = slotOf(h);
      if (slot == kEmptySlot) {
        return false;
      }
      uint32_t next_slot = next_[slot].load(std::memory_order_acquire);
      uint64_t nh = pack(tagOf(h) + 1, next_slot);
      if (head_.compare_exchange_weak(h, nh, std::memory_order_release,
                                      std::memory_order_acquire)) {
        *out_slot = slot;
        count_.fetch_sub(1, std::memory_order_release);
        return true;
      }
    }
  }

  // 收割线程:批量弹出(摊薄系统调用)
  uint32_t drain(uint32_t* slots, uint32_t max_cnt) {
    uint32_t n = 0;
    while (n < max_cnt && pop(&slots[n])) {
      ++n;
    }
    return n;
  }

  // 近似深度(记账不变式断言用,并发下瞬态不精确)
  uint32_t size() const {
    return count_.load(std::memory_order_acquire);
  }

  bool empty() const { return size() == 0; }

 private:
  // head 编码:高 32 位 tag(ABA 防护),低 32 位 slot 索引
  static constexpr uint64_t kNull = 0xFFFFFFFFULL;  // tag=0, slot=kEmptySlot

  static uint64_t pack(uint32_t tag, uint32_t slot) {
    return (static_cast<uint64_t>(tag) << 32) | slot;
  }
  static uint32_t tagOf(uint64_t h) {
    return static_cast<uint32_t>(h >> 32);
  }
  static uint32_t slotOf(uint64_t h) {
    return static_cast<uint32_t>(h & 0xFFFFFFFFu);
  }

  std::unique_ptr<std::atomic<uint32_t>[]> next_;  // per-slot next 指针
  uint32_t capacity_{0};
  std::atomic<uint64_t> head_{kNull};
  std::atomic<uint32_t> count_{0};
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
