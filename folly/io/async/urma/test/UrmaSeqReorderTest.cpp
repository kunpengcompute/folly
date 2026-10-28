/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaSeqReorder 单测(SDD 任务:保序功能增强):
 *   - seqDist 回绕距离
 *   - 按序命中 + 连带交付
 *   - 乱序入缓冲按序补齐
 *   - 重复/过时帧语义(由调用方判定,重排器仅存未来帧)
 *   - 空洞超时放行 + expected 重同步
 *   - 深度上限防洪泛
 *
 * 不依赖 urma_api/硬件,仅需 FOLLY_HAVE_URMA 宏。
 */
#include <folly/io/async/urma/UrmaSeqReorder.h>

#ifdef FOLLY_HAVE_URMA

#include <folly/io/IOBuf.h>
#include <folly/portability/GTest.h>

#include <chrono>
#include <vector>

using namespace folly;
using TP = UrmaSeqReorder::TimePoint;

namespace {
std::unique_ptr<IOBuf> mkBuf(uint8_t v) {
  auto b = IOBuf::create(16);
  memset(b->writableData(), v, 16);
  b->append(16);
  return b;
}
TP now() { return std::chrono::steady_clock::now(); }
} // namespace

TEST(UrmaSeqReorderTest, SeqDistWraparound) {
  EXPECT_EQ(0, UrmaSeqReorder::seqDist(100, 100));
  EXPECT_EQ(1, UrmaSeqReorder::seqDist(101, 100));
  EXPECT_EQ(-1, UrmaSeqReorder::seqDist(99, 100));
  // 回绕:65535 在 65534 之后(未来)
  EXPECT_EQ(1, UrmaSeqReorder::seqDist(65535, 65534));
  // 0 在 65535 之后 1(回绕)
  EXPECT_EQ(1, UrmaSeqReorder::seqDist(0, 65535));
  // 65535 在 0 之前 1(过时)
  EXPECT_EQ(-1, UrmaSeqReorder::seqDist(65535, 0));
  // 大距离
  EXPECT_EQ(100, UrmaSeqReorder::seqDist(200, 100));
  EXPECT_EQ(-100, UrmaSeqReorder::seqDist(100, 200));
}

TEST(UrmaSeqReorderTest, DrainInOrderWithGap) {
  UrmaSeqReorder r;
  // expected=10 缺失,先到 11/12/13 → 入缓冲
  EXPECT_TRUE(r.push(11, mkBuf(11), now()));
  EXPECT_TRUE(r.push(12, mkBuf(12), now()));
  EXPECT_TRUE(r.push(13, mkBuf(13), now()));
  EXPECT_EQ(3, r.size());

  uint16_t expected = 10;
  std::vector<std::unique_ptr<IOBuf>> out;
  // 10 未到:drain 无输出
  EXPECT_EQ(0, r.drainInOrder(expected, out));
  EXPECT_EQ(10, expected);

  // 10 到达:连带 11/12/13 全部按序交付
  EXPECT_TRUE(r.push(10, mkBuf(10), now()));
  EXPECT_EQ(4, r.drainInOrder(expected, out));
  EXPECT_EQ(14, expected);
  ASSERT_EQ(4, out.size());
  EXPECT_EQ(10, out[0]->data()[0]);
  EXPECT_EQ(11, out[1]->data()[0]);
  EXPECT_EQ(12, out[2]->data()[0]);
  EXPECT_EQ(13, out[3]->data()[0]);
  EXPECT_TRUE(r.empty());
}

TEST(UrmaSeqReorderTest, PartialDrain) {
  UrmaSeqReorder r;
  // 5 缺失:缓冲的 6 必须等待(不能先于 5 交付)
  EXPECT_TRUE(r.push(6, mkBuf(6), now()));
  EXPECT_TRUE(r.push(8, mkBuf(8), now()));
  std::vector<std::unique_ptr<IOBuf>> out;
  uint16_t e = 5;
  EXPECT_EQ(0, r.drainInOrder(e, out));
  EXPECT_EQ(5, e);
  // 5 已交付(调用方直接交付后 expected 推进为 6):6 连带弹出,8 等待 7
  e = 6;
  EXPECT_EQ(1, r.drainInOrder(e, out));
  EXPECT_EQ(7, e);
  EXPECT_EQ(1, r.size());
}

TEST(UrmaSeqReorderTest, WraparoundDrain) {
  UrmaSeqReorder r;
  uint16_t expected = 65535;
  // expected=65535 由调用方直接交付后 expected 回绕为 0;0/1 在缓冲
  EXPECT_TRUE(r.push(0, mkBuf(0), now()));
  EXPECT_TRUE(r.push(1, mkBuf(1), now()));
  uint16_t e = 0;
  std::vector<std::unique_ptr<IOBuf>> out;
  EXPECT_EQ(2, r.drainInOrder(e, out));
  EXPECT_EQ(2, e);
  EXPECT_TRUE(r.empty());
}

TEST(UrmaSeqReorderTest, ReleaseOnTimeout) {
  UrmaSeqReorder r;
  auto t0 = now();
  // 空洞 expected=20,缓冲 21/22,t0 时刻入
  EXPECT_TRUE(r.push(21, mkBuf(21), t0));
  EXPECT_TRUE(r.push(22, mkBuf(22), t0));

  uint16_t expected = 20;
  std::vector<std::unique_ptr<IOBuf>> out;
  // 未超时(100ms < 200ms):不释放
  auto t1 = t0 + std::chrono::milliseconds(100);
  EXPECT_FALSE(r.releaseOnTimeout(expected, 200, out, t1));
  EXPECT_TRUE(out.empty());

  // 超时(300ms > 200ms):放行全部 + expected 重同步 = 23
  auto t2 = t0 + std::chrono::milliseconds(300);
  EXPECT_TRUE(r.releaseOnTimeout(expected, 200, out, t2));
  EXPECT_EQ(23, expected);
  ASSERT_EQ(2, out.size());
  EXPECT_EQ(21, out[0]->data()[0]);
  EXPECT_EQ(22, out[1]->data()[0]);
  EXPECT_TRUE(r.empty());
}

TEST(UrmaSeqReorderTest, TimeoutZeroForcesRelease) {
  // timeout=0 = 立即放行(缓冲满冲刷路径)
  UrmaSeqReorder r;
  EXPECT_TRUE(r.push(30, mkBuf(30), now()));
  EXPECT_TRUE(r.push(31, mkBuf(31), now()));
  uint16_t expected = 25;
  std::vector<std::unique_ptr<IOBuf>> out;
  EXPECT_TRUE(r.releaseOnTimeout(expected, 0, out, now()));
  EXPECT_EQ(32, expected);
  EXPECT_EQ(2, out.size());
}

TEST(UrmaSeqReorderTest, DepthLimit) {
  UrmaSeqReorder r;
  // 填满 kMaxDepth
  for (size_t i = 0; i < UrmaSeqReorder::kMaxDepth; ++i) {
    EXPECT_TRUE(r.push(static_cast<uint16_t>(1000 + i), mkBuf(1), now()));
  }
  EXPECT_EQ(UrmaSeqReorder::kMaxDepth, r.size());
  // 超限拒绝(调用方走冲刷/放行路径)
  EXPECT_FALSE(r.push(static_cast<uint16_t>(1000 + UrmaSeqReorder::kMaxDepth),
                      mkBuf(2), now()));
  // null buf 拒绝
  EXPECT_FALSE(r.push(1, nullptr, now()));
}

TEST(UrmaSeqReorderTest, Range) {
  UrmaSeqReorder r;
  uint16_t lo = 0, hi = 0;
  EXPECT_FALSE(r.range(&lo, &hi));
  EXPECT_TRUE(r.push(7, mkBuf(7), now()));
  EXPECT_TRUE(r.push(3, mkBuf(3), now()));
  EXPECT_TRUE(r.push(9, mkBuf(9), now()));
  EXPECT_TRUE(r.range(&lo, &hi));
  EXPECT_EQ(3, lo);
  EXPECT_EQ(9, hi);
}

#endif // FOLLY_HAVE_URMA
