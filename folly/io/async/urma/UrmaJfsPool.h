/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaJfsPool: 进程级 JFS 池(发送侧)。
 *
 *   替代 per-连接/per-IO 线程 JFS,用进程级 io_jfs_count 个共享 IO JFS +
 *   fc_jfs_count 个共享 FC JFS,round-robin + CAS sq_inflight 原子计数分配。
 *
 *   设计文档 §4 JFS 池化:
 *     - SharedJfs: urma_jfs_t* + sq_inflight(atomic) + sq_depth + JFC/JFCE/fd
 *     - acquireIoJfs(): round-robin + CAS sq_inflight < sq_depth
 *     - releaseIoJfs(index): sq_inflight.fetch_sub(1)
 *     - rearmAllTxJfce(): SQ 满时武装 TX JFCE 中断
 *
 *   JFS 独立创建(urma_create_jfs),不绑定 jetty,urma_post_jfs_wr 线程安全无需加锁。
 *
 *   必须在 UrmaInit::init 中创建,所有 UrmaAsyncSocket 共享。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <cstdint>
#include <vector>

#include <ub/umdk/urma/urma_api.h>

namespace folly {

class UrmaConfig;

// 单个共享 JFS 的元数据
struct SharedJfs {
  urma_jfs_t* jfs = nullptr;
  urma_jfc_t* jfc = nullptr;
  urma_jfce_t* jfce = nullptr;
  int jfce_fd = -1;          // JFCE 的 fd,供 EventHandler 注册 epoll

  std::atomic<uint32_t> sq_inflight{0};  // 当前 SQ 在途 WR 数(CAS 分配/释放)
  uint32_t sq_depth = 0;                 // SQ 深度(= config.tx_depth)

  // std::atomic 不可拷贝,需显式定义移动构造/赋值以支持 vector::resize
  SharedJfs() = default;
  SharedJfs(SharedJfs&& o) noexcept
      : jfs(o.jfs), jfc(o.jfc), jfce(o.jfce), jfce_fd(o.jfce_fd),
        sq_inflight(o.sq_inflight.load(std::memory_order_relaxed)),
        sq_depth(o.sq_depth) {
    o.jfs = nullptr; o.jfc = nullptr; o.jfce = nullptr; o.jfce_fd = -1;
  }
  SharedJfs& operator=(SharedJfs&& o) noexcept {
    if (this != &o) {
      jfs = o.jfs; jfc = o.jfc; jfce = o.jfce; jfce_fd = o.jfce_fd;
      sq_inflight.store(o.sq_inflight.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
      sq_depth = o.sq_depth;
      o.jfs = nullptr; o.jfc = nullptr; o.jfce = nullptr; o.jfce_fd = -1;
    }
    return *this;
  }
  SharedJfs(const SharedJfs&) = delete;
  SharedJfs& operator=(const SharedJfs&) = delete;
};

class UrmaJfsPool {
 public:
  // 获取进程级单例(UrmaInit::init 中创建)
  static UrmaJfsPool* get();

  // 初始化 JFS 池(由 UrmaInit::init 调用)
  // 返回 true 表示成功
  bool init(urma_context_t* ctx, const UrmaConfig& config,
            uint8_t max_sge, uint32_t max_inline_len);

  // 销毁(由 UrmaInit::fini 调用,可选——进程退出时内核回收)
  void fini();

  // ── IO JFS 池 ──

  // 获取一个空闲 IO JFS(round-robin + CAS)
  // 返回 SharedJfs* 和 index(out 参数,供 releaseIoJfs 使用)
  // 全满时返回 nullptr
  SharedJfs* acquireIoJfs(uint32_t* out_index = nullptr);

  // 按指定 index 获取 IO JFS(CAS sq_inflight,同连接 pin 用)
  // 指定 JFS 满时返回 nullptr(调用方可 fallback 到 acquireIoJfs)
  SharedJfs* tryAcquireIoJfsByIndex(uint32_t index) {
    if (index >= io_jfs_list_.size()) return nullptr;
    auto& sj = io_jfs_list_[index];
    uint32_t cur = sj.sq_inflight.load(std::memory_order_acquire);
    while (cur < sj.sq_depth) {
      if (sj.sq_inflight.compare_exchange_weak(cur, cur + 1,
              std::memory_order_acq_rel, std::memory_order_acquire)) {
        return &sj;
      }
    }
    return nullptr;
  }

  // 归还 IO JFS 的 sq_inflight(TX CQE 完成后调用)
  void releaseIoJfs(uint32_t index);

  // 按 index 获取 IO JFS(供 poll TX CQE 使用)
  SharedJfs* getIoJfsByIndex(uint32_t index) {
    if (index < io_jfs_list_.size()) return &io_jfs_list_[index];
    return nullptr;
  }

  uint32_t ioJfsCount() const { return static_cast<uint32_t>(io_jfs_list_.size()); }

  // ── FC JFS 池 ──

  SharedJfs* acquireFcJfs(uint32_t* out_index = nullptr);
  void releaseFcJfs(uint32_t index);

  SharedJfs* getFcJfsByIndex(uint32_t index) {
    if (index < fc_jfs_list_.size()) return &fc_jfs_list_[index];
    return nullptr;
  }

  uint32_t fcJfsCount() const { return static_cast<uint32_t>(fc_jfs_list_.size()); }

  // ── TX JFCE 中断管理 ──

  // 武装所有 IO JFS 的 TX JFCE 中断(SQ 满时调用,等待 CQE 唤醒)
  void rearmAllTxJfce();

  // 武装所有 FC JFS 的 TX JFCE 中断
  void rearmAllFcTxFce();

  // 获取所有 IO JFS 的 JFCE fd 列表(供 UrmaCqHandler 注册)
  // 返回 (fd, jfs_index) 列表
  struct JfceFdEntry {
    int fd;
    uint32_t jfs_index;
    bool is_fc;
  };
  std::vector<JfceFdEntry> getAllJfceFds() const;

  UrmaJfsPool() = default;
  ~UrmaJfsPool() = default;

  // 创建单个 JFS(含 JFCE + JFC + JFS)
  // is_fc: true=FC JFS(max_sge=1, depth=fc_tx_depth), false=IO JFS
  static SharedJfs createJfs(urma_context_t* ctx, uint32_t depth,
                              uint8_t max_sge, uint32_t max_inline_len);

  std::vector<SharedJfs> io_jfs_list_;
  std::vector<SharedJfs> fc_jfs_list_;

  // round-robin 游标(原子,多线程安全)
  std::atomic<uint32_t> io_rr_{0};
  std::atomic<uint32_t> fc_rr_{0};

  bool inited_{false};
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
