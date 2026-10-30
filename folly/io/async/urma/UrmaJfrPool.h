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
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include <folly/io/async/urma/UrmaRecycleStack.h>
#include <folly/io/async/urma/UrmaRouteTable.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

class UrmaConfig;
class UrmaAsyncSocket;
class UrmaJfrPool;
struct SharedJfr;

// recv buffer 水位线参数(DPDK mempool 风格)
struct RecvWaterMark {
  uint32_t min_water = 64;    // 最低水位(低于此值触发补投)
  uint32_t max_water = 256;   // 最高水位(补投到此值)
  uint32_t batch_post = 32;   // 每批补投数量
};

// 任务 C(H-3):RX slot 回收上下文 —— 常驻于 RxBuf 的稳定存储
// (rx_bufs 在 init 后不再扩容,生命周期长于一切 IOBuf,评审要点②:
//  禁止指向 stack/临时对象)。deleter 在任意线程执行,凭此上下文
// 将 slot 送回 per-JFR 无锁回收栈,由收割线程批量重投。
struct RxSlotCtx {
  UrmaJfrPool* pool{nullptr};  // 所属池(回收入口)
  SharedJfr* jfr{nullptr};      // 所属 JFR(prepare 期回填)
  uint32_t slot{0};             // rx_bufs 索引
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
    RxSlotCtx ctx;              // 任务 C:零拷贝交付的 deleter 上下文(常驻稳定存储)
  };
  std::vector<RxBuf> rx_bufs;   // recv buffer 数组(max_water 大小,init 后不扩容)
  std::atomic<uint32_t> recv_posted{0};  // 当前已 post 的 recv WR 数(= RQ 深)
  std::atomic<uint32_t> next_free_slot{0};  // 下一个空闲 slot 游标(补投用)
  RecvWaterMark watermark;

  // ── 任务 C(H-3/H-4):RX 所有权记账(docs/urma-threading.md 不变量 I4) ──
  // 不变式:recv_posted + transit_cnt + held_count + 回收栈深 == max_water
  // (转移采用"先加后减"序,瞬态偏差有界;系统级泄漏由 checkRxAccounting 诊断)
  std::atomic<uint32_t> held_count{0};    // movable 交付未释放数(安全阀上限 = max-min)
  std::atomic<uint32_t> transit_cnt{0};   // CQE 收割→交付转移中(不变式断言用)
  // 无锁回收栈(IOBuf deleter 任意线程入栈,收割线程批量弹出重投)
  std::unique_ptr<UrmaRecycleStack> recycle;
  // checkRxAccounting 连续越界采样计数(多 EVB 定时器可能并发访问)
  std::atomic<uint32_t> accounting_bad_streak{0};

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
        held_count(o.held_count.load(std::memory_order_relaxed)),
        transit_cnt(o.transit_cnt.load(std::memory_order_relaxed)),
        recycle(std::move(o.recycle)),
        accounting_bad_streak(
            o.accounting_bad_streak.load(std::memory_order_relaxed)),
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
      held_count.store(o.held_count.load(std::memory_order_relaxed),
                       std::memory_order_relaxed);
      transit_cnt.store(o.transit_cnt.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
      recycle = std::move(o.recycle);
      accounting_bad_streak.store(
          o.accounting_bad_streak.load(std::memory_order_relaxed),
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

  // ── 任务 A(SDD-URMA-SHARED-JFS-JFR-002): 进程级远端 import cache ──
  // 对端通告 JFR 级共享 jfr_id 后,通过 urma_import_jfr 导入。
  // 同一远端 JFR 的所有连接共享同一 imported handle(cache 命中,零额外 syscall)。
  // 进程退出时 fini 统一 unimport,连接断开不触发(不变量 I1/I3)。
  static urma_target_jetty_t* importRemoteJfr(const urma_rjfr_t* rjfr);
  static urma_target_seg_t* importRemoteSeg(const urma_seg_t* seg);

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

  // ── 任务 B(H-2):TX CQE 属主转发 ──
  // READ CQE 单点转发:同 EVB 短路(零延迟),跨 EVB runInLoop 转发回属主线程。
  // 收割线程(harvestTxAndDispatch / UrmaAsyncSocket::pollTx)统一走此协议,
  // 禁止跨线程直接调用 sock 方法(不变量 I1/I3)。
  void dispatchReadCqe(uint64_t socket_id, uint16_t msg_id,
                       urma_status_t status) noexcept;

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

  // ── 任务 C(H-3/H-4):RX 所有权交付与记账(docs/urma-threading.md 不变量 I4) ──
  // IOBuf deleter 回收入口(任意线程):held-- + slot 入回收栈,不直接 post
  static void releaseHeldSlot(SharedJfr* jfr, uint32_t slot) noexcept;
  // 收割线程立即回收(数据已按值消费:控制包/FC/解析失败/降级拷贝):
  // transit-- + slot 入回收栈
  static void recycleRxSlot(SharedJfr* jfr, uint32_t slot) noexcept;
  // 收割线程:批量弹出回收栈并重投(账实严格一致,batch 失败按 bad_wr 精确记账)
  void repostRecycled(SharedJfr* jfr) noexcept;
  // 记账不变式诊断(10ms idle timer 周期调用):
  // recv_posted + transit + held + 回收栈深 ≈ max_water,持续偏差告警
  void checkRxAccounting(SharedJfr* jfr) noexcept;

  // ── credit_pool ──
  // RX CQE 消费后 credit 回池
  void recharge(SharedJfr* jfr) noexcept {
    jfr->credit_idle.fetch_add(1, std::memory_order_release);
  }

  // ── 路由表(SocketId → UrmaAsyncSocket*) ──
  // Phase 3: 按 header.dst_socket_id 路由(对端发送时写入本端 SocketId,接收方据此查表)
  // H-1 修复:分片读写锁 + SafeRouteEntry 安全句柄(接口签名不变,内部委托 route_table_)
  void registerSocket(uint64_t socket_id, UrmaAsyncSocket* socket);
  void unregisterSocket(uint64_t socket_id);
  UrmaAsyncSocket* findSocket(uint64_t socket_id);
  // H-2 修复:TX CQE 属主转发用,返回安全句柄(持分片读锁拷出后放锁)
  SafeRouteEntryPtr findRouteEntry(uint64_t socket_id);

  void registerFcSocket(uint64_t socket_id, UrmaAsyncSocket* socket);
  void unregisterFcSocket(uint64_t socket_id);
  UrmaAsyncSocket* findFcSocket(uint64_t socket_id);

  // 连接全断时判断:路由表为空表示该 EventBase 上无活跃连接
  bool socketMapEmpty() const {
    return route_table_.empty() && fc_route_table_.empty();
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
  // pool: 所属池指针(回填 RxSlotCtx.pool,deleter 回收入口用)
  static bool createJfr(urma_context_t* ctx, UrmaJfrPool* pool, SharedJfr* jfr,
                        uint32_t rx_depth, bool is_fc,
                        const RecvWaterMark& wm);

  // 初始化 recv buffer 池(预投 min_water 个 recv WR + 任务 C 回收栈)
  static bool initRxBufPool(UrmaJfrPool* pool, SharedJfr* jfr,
                            uint32_t max_bufs);

  // post 单个 recv WR
  static bool postRecvWr(SharedJfr* jfr, uint32_t slot);
  // 批量 post recv WR(用 WR 链表一次 urma_post_jfr_wr,摊薄系统调用)
  static bool postRecvWrBatch(SharedJfr* jfr, const uint32_t* slots, uint32_t count);
  // 任务 C:批量 post 并返回实际成功数(失败时 bad_wr 之前的 WR 已入 RQ,
  // 据此精确记账,保证 recv_posted 账实一致)
  static uint32_t postRecvWrBatchCount(SharedJfr* jfr, const uint32_t* slots,
                                       uint32_t count) noexcept;

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
  // H-1 修复:16 分片 SharedMutex + SafeRouteEntry 安全句柄(替代普通 unordered_map)
  UrmaRouteTable route_table_;
  UrmaRouteTable fc_route_table_;

  bool inited_{false};

  // ── 任务 A: 进程级远端 import cache(I2) ──
  // key = jfr_id(远端 JFR 级共享,≤ 远端 JFR 数),条目极少
  static std::shared_mutex s_import_mtx;
  static std::unordered_map<uint32_t, urma_target_jetty_t*> s_remote_jfr_cache;
  static std::unordered_map<uint64_t, urma_target_seg_t*> s_remote_seg_cache;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
