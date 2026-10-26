/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaJfrPool: 进程级 JFR 池(接收侧)。
 *
 *   替代 per-EventBase UrmaSharedJfr,用进程级 io_jfr_count 个共享 IO JFR +
 *   fc_jfr_count 个共享 FC JFR。建链时 round-robin 选一个 JFR,通告 jetty_id 给对端。
 *
 *   设计文档 §3 JFR 池化:
 *     - SharedJfr: urma_jfr_t* + jfc + jfce + jetty_id + recv buffer 池 + credit_pool
 *     - selectIoJfr(): round-robin 选一个 IO JFR(建链时调用)
 *     - selectFcJfr(): round-robin 选一个 FC JFR
 *     - pollAndDispatch(jfr): batch poll CQE + headroom 路由
 *     - replenishRxBuf(jfr): 水位线管理,batch_post 摊薄 post 开销
 *     - recharge(jfr): RX CQE 消费后 credit 回池
 *
 *   JFR 池是进程级的,所有 EventBase 共享同一组 JFR。
 *   urma_poll_jfc 线程安全,多线程并发 poll 同一 JFC 不冲突(每个 CQE 只被一个线程收割)。
 *
 *   必须在 UrmaInit::init 中创建,所有 UrmaAsyncSocket 共享。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include <ub/umdk/urma/urma_api.h>

namespace folly {

class UrmaConfig;
class UrmaAsyncSocket;

// recv buffer 水位线参数(DPDK mempool 风格)
struct RecvWaterMark {
  uint32_t min_water = 64;    // 最低水位(低于此值触发补投)
  uint32_t max_water = 256;   // 最高水位(补投到此值)
  uint32_t batch_post = 32;   // 每批补投数量
};

// 单个共享 JFR 的元数据 + recv buffer 池 + credit_pool
struct SharedJfr {
  // URMA 对象
  urma_jfr_t* jfr = nullptr;
  urma_jfc_t* jfc = nullptr;
  urma_jfce_t* jfce = nullptr;
  int jfce_fd = -1;             // JFCE 的 fd,供 EventHandler 注册 epoll
  uint32_t jetty_id = 0;        // 全局唯一,握手时通告给对端

  // RX buffer 池(per-JFR 独立)
  // 每个 recv WR 用 1 个 SGE,指向 4KB buffer
  struct RxBuf {
    void* data = nullptr;
    urma_sge_t sge = {};
    urma_jfr_wr_t wr = {};
  };
  std::vector<RxBuf> rx_bufs;   // recv buffer 数组(max_water 大小)
  std::atomic<uint32_t> recv_posted{0};  // 当前已 post 的 recv WR 数
  std::atomic<uint32_t> next_free_slot{0};  // 下一个空闲 slot 游标(补投用)
  RecvWaterMark watermark;

  // credit_pool(仅 IO JFR 需要,设计 §3.3)
  std::atomic<uint32_t> credit_idle{0};      // 空闲 credit
  std::atomic<uint32_t> credit_allocated{0}; // 已分配 credit
  uint32_t rx_depth = 0;                      // credit_pool 逻辑总容量

  bool is_fc = false;  // FC JFR 标记

  // std::atomic 不可拷贝,需显式定义移动构造/赋值以支持 vector::resize
  SharedJfr() = default;
  SharedJfr(SharedJfr&& o) noexcept
      : jfr(o.jfr), jfc(o.jfc), jfce(o.jfce), jfce_fd(o.jfce_fd),
        jetty_id(o.jetty_id),
        rx_bufs(std::move(o.rx_bufs)),
        recv_posted(o.recv_posted.load(std::memory_order_relaxed)),
        next_free_slot(o.next_free_slot.load(std::memory_order_relaxed)),
        watermark(o.watermark),
        credit_idle(o.credit_idle.load(std::memory_order_relaxed)),
        credit_allocated(o.credit_allocated.load(std::memory_order_relaxed)),
        rx_depth(o.rx_depth), is_fc(o.is_fc) {
    o.jfr = nullptr; o.jfc = nullptr; o.jfce = nullptr; o.jfce_fd = -1;
  }
  SharedJfr& operator=(SharedJfr&& o) noexcept {
    if (this != &o) {
      jfr = o.jfr; jfc = o.jfc; jfce = o.jfce; jfce_fd = o.jfce_fd;
      jetty_id = o.jetty_id;
      rx_bufs = std::move(o.rx_bufs);
      recv_posted.store(o.recv_posted.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
      next_free_slot.store(o.next_free_slot.load(std::memory_order_relaxed),
                           std::memory_order_relaxed);
      watermark = o.watermark;
      credit_idle.store(o.credit_idle.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
      credit_allocated.store(o.credit_allocated.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
      rx_depth = o.rx_depth; is_fc = o.is_fc;
      o.jfr = nullptr; o.jfc = nullptr; o.jfce = nullptr; o.jfce_fd = -1;
    }
    return *this;
  }
  SharedJfr(const SharedJfr&) = delete;
  SharedJfr& operator=(const SharedJfr&) = delete;
};

class UrmaJfrPool {
 public:
  static UrmaJfrPool* get();

  // 初始化 JFR 池(由 UrmaInit::init 调用)
  bool init(urma_context_t* ctx, const UrmaConfig& config);

  // 销毁(由 UrmaInit::fini 调用)
  void fini();

  // ── 建链时 round-robin 选 JFR ──
  // 返回 SharedJfr* 和 index(out 参数)
  SharedJfr* selectIoJfr(uint32_t* out_index = nullptr);
  SharedJfr* selectFcJfr(uint32_t* out_index = nullptr);

  // 按 index 获取 JFR
  SharedJfr* getIoJfrByIndex(uint32_t index) {
    if (index < io_jfr_list_.size()) return &io_jfr_list_[index];
    return nullptr;
  }
  SharedJfr* getFcJfrByIndex(uint32_t index) {
    if (index < fc_jfr_list_.size()) return &fc_jfr_list_[index];
    return nullptr;
  }

  uint32_t ioJfrCount() const { return static_cast<uint32_t>(io_jfr_list_.size()); }
  uint32_t fcJfrCount() const { return static_cast<uint32_t>(fc_jfr_list_.size()); }

  // ── 接收侧 CQE 处理 ──
  // pollAndDispatch: 从指定 JFR poll CQE → headroom 路由 → 分发到 UrmaAsyncSocket
  void pollAndDispatch(SharedJfr* jfr) noexcept;
  void pollFcAndDispatch(SharedJfr* jfr) noexcept;

  // TX CQE 单点收割: 只 poll 指定 JFS 的 TX CQE
  // 由 TX JFCE fd 的 epoll 事件驱动(UrmaCqHandler is_tx 模式 → harvestTx)
  void harvestTxAndDispatch(uint32_t jfs_index) noexcept;

  // FC JFS TX CQE 全量收割(由 RX 事件顺带驱动,FC JFCE fd 未注册 epoll)
  void harvestFcTxAndDispatch() noexcept;

  // ── recv buffer 按需投递 ──
  // 补投 recv WR(水位线触发)
  void replenishRxBuf(SharedJfr* jfr, uint32_t slot) noexcept;
  // 按水位线批量补投:优先补投 free_slots(刚回收的 slot),不足再从游标取
  void replenishByWaterMark(SharedJfr* jfr, const uint32_t* free_slots = nullptr,
                            uint32_t free_cnt = 0) noexcept;

  // ── credit_pool ──
  // RX CQE 消费后 credit 回池
  void recharge(SharedJfr* jfr) noexcept {
    jfr->credit_idle.fetch_add(1, std::memory_order_release);
  }

  // ── 路由表(SocketId → UrmaAsyncSocket*) ──
  // Phase 3: 按 header.dst_socket_id 路由(对端发送时写入本端 SocketId,接收方据此查表)
  void registerSocket(uint64_t socket_id, UrmaAsyncSocket* socket);
  void unregisterSocket(uint64_t socket_id);
  UrmaAsyncSocket* findSocket(uint64_t socket_id);

  void registerFcSocket(uint64_t socket_id, UrmaAsyncSocket* socket);
  void unregisterFcSocket(uint64_t socket_id);
  UrmaAsyncSocket* findFcSocket(uint64_t socket_id);

  // 连接全断时判断:路由表为空表示该 EventBase 上无活跃连接
  bool socketMapEmpty() const {
    return socket_map_.empty() && fc_socket_map_.empty();
  }

  // ── JFCE fd 注册(供 UrmaCqHandler 使用) ──
  struct JfrFdEntry {
    int fd;
    uint32_t jfr_index;
    bool is_fc;
  };
  std::vector<JfrFdEntry> getAllJfrFds() const;

  UrmaJfrPool() = default;
  ~UrmaJfrPool() = default;

  // 创建单个 JFR(含 JFCE + JFC + JFR + recv buffer 预投 + jetty)
  static bool createJfr(urma_context_t* ctx, SharedJfr* jfr,
                        uint32_t rx_depth, bool is_fc,
                        const RecvWaterMark& wm);

  // 初始化 recv buffer 池(预投 min_water 个 recv WR)
  static bool initRxBufPool(SharedJfr* jfr, uint32_t max_bufs);

  // post 单个 recv WR
  static bool postRecvWr(SharedJfr* jfr, uint32_t slot);
  // 批量 post recv WR(用 WR 链表一次 urma_post_jfr_wr,摊薄系统调用)
  static bool postRecvWrBatch(SharedJfr* jfr, const uint32_t* slots, uint32_t count);

  // 内部:收割 CQE 并 headroom 路由分发(返回收割的 CQE 数)
  // 被 pollAndDispatch 调用,rearm 后的二次 poll 也复用此函数
  // (edge-triggered rearm 后必须再 poll 一次,防止漏 CQE)
  uint32_t harvestAndDispatch(SharedJfr* jfr) noexcept;

  // 内部:收割 FC JFR 的 CQE 并按 payload 前 8B SocketId 路由(返回收割的 CQE 数)
  // 被 pollFcAndDispatch 调用,与 harvestAndDispatch 同构但路由表为 fc_socket_map_
  uint32_t harvestFcAndDispatch(SharedJfr* jfr) noexcept;

  std::vector<SharedJfr> io_jfr_list_;
  std::vector<SharedJfr> fc_jfr_list_;

  // round-robin 游标(原子,多线程安全)
  std::atomic<uint32_t> io_rr_{0};
  std::atomic<uint32_t> fc_rr_{0};

  // 路由表(Phase 3: SocketId 全局路由)
  std::unordered_map<uint64_t, UrmaAsyncSocket*> socket_map_;
  std::unordered_map<uint64_t, UrmaAsyncSocket*> fc_socket_map_;

  bool inited_{false};
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
