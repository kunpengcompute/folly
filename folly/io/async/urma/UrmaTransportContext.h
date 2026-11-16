/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaTransportContext: per-EventBase 独占四件套(SDD-URMA-EVBEXCLUSIVE-001)。
 *
 *   每个 IO EVB 恰一个 UrmaTransportContext,首连接时惰性创建、EVB 生命周期内常驻。
 *   持有 1 IO JFS + 1 IO JFR + 1 FC JFS + 1 FC JFR(独占,无池/无领用/无共享降级)。
 *   IO 侧 TX/RX 各 1 个独立 JFC(JFCE),FC 侧同构 → 每 EVB 4 个 epoll fd。
 *
 *   不变量 I1(独占):创建即独占,EVB 生命周期内不释放,无 evb_selects/owner_claimed
 *   不变量 I2(同线程):四件套一切收割/发送/水位/Timer 均在该 EVB 线程 → same-thread
 *   不变量 I3(单点收割):每 EVB 4 CqHandler(io_rx/io_tx/fc_rx/fc_tx),各自 poll 独立 JFC
 *   不变量 I4(动态创建):首连接惰性创建,连接全断不销毁(EVB 常驻),进程 fini 统一销毁
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBaseLocal.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaRecycleStack.h>
#include <folly/io/async/urma/UrmaRouteTable.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

class UrmaAsyncSocket;
class UrmaCqHandler;
class UrmaTransportContext;
class EvbJfr;

// ── recv buffer 水位线参数 ──
struct RecvWaterMark {
  uint32_t min_water = 64;
  uint32_t max_water = 256;
  uint32_t batch_post = 32;
};

// ── RX slot 回收上下文(全局结构体) ──
// deleter 在任意线程执行,凭此上下文将 slot 送回 per-JFR 无锁回收栈
struct RxSlotCtx {
  UrmaTransportContext* ctx{nullptr};
  EvbJfr* jfr{nullptr};
  uint32_t slot{0};
  // 问题 5.1:UAF 防护 —— IOBuf deleter 可能在 EventBase 析构后执行,
  // 此时 ctx/jfr 已悬垂。alive 标志由 shared_ptr 保活,deleter 检查后短路;
  // recycle/held_count 是裸指针,alive=true 时才安全访问
  std::shared_ptr<std::atomic<bool>> alive;
  UrmaRecycleStack* recycle{nullptr};
  std::atomic<uint32_t>* held_count{nullptr};
};

// 问题 01:SQ 配额等待者(same-thread,I2 无锁)
// sendChain 抢不到配额时登记,配额归还时 wakeOneIoSqWaiter 唤醒重试
struct SqWaiter {
  UrmaAsyncSocket* sock{nullptr};
  bool active{false};
};

// ── 四件套元数据结构(去原子,same-thread 访问) ──

// JFS 元料(I2:sq_inflight 退化为普通 uint32,same-thread CAS 不需要)
struct EvbJfs {
  urma_jfs_t* jfs = nullptr;
  urma_jfc_t* tx_jfc = nullptr;    // TX 独立 JFC(不再和 RX 共享)
  urma_jfce_t* tx_jfce = nullptr;  // TX JFCE(独立 fd)
  int tx_jfce_fd = -1;
  uint32_t sq_inflight = 0;    // same-thread,无锁(I2)
  uint32_t sq_depth = 0;

  EvbJfs() = default;
  EvbJfs(EvbJfs&& o) noexcept
      : jfs(o.jfs), tx_jfc(o.tx_jfc), tx_jfce(o.tx_jfce), tx_jfce_fd(o.tx_jfce_fd),
        sq_inflight(o.sq_inflight), sq_depth(o.sq_depth) {
    o.jfs = nullptr; o.tx_jfc = nullptr; o.tx_jfce = nullptr; o.tx_jfce_fd = -1;
  }
  EvbJfs& operator=(EvbJfs&& o) noexcept {
    if (this != &o) {
      jfs = o.jfs; tx_jfc = o.tx_jfc; tx_jfce = o.tx_jfce; tx_jfce_fd = o.tx_jfce_fd;
      sq_inflight = o.sq_inflight; sq_depth = o.sq_depth;
      o.jfs = nullptr; o.tx_jfc = nullptr; o.tx_jfce = nullptr; o.tx_jfce_fd = -1;
    }
    return *this;
  }
  EvbJfs(const EvbJfs&) = delete;
  EvbJfs& operator=(const EvbJfs&) = delete;
};

// JFR 元料(I2:recv_posted/next_free_slot/credit 退化为普通 uint32)
struct EvbJfr {
  urma_jfr_t* jfr = nullptr;
  urma_jfc_t* rx_jfc = nullptr;    // RX 独立 JFC(不再和 TX 共享)
  urma_jfce_t* rx_jfce = nullptr;  // RX JFCE(独立 fd)
  int rx_jfce_fd = -1;

  // RX buffer 池
  struct RxBuf {
    void* data = nullptr;
    urma_sge_t sge = {};
    urma_jfr_wr_t wr = {};
    // 问题 5.1:shared_ptr 保活 RxSlotCtx,独立于 jfr 析构
    std::shared_ptr<RxSlotCtx> ctx;
  };
  std::vector<RxBuf> rx_bufs;
  uint32_t recv_posted = 0;       // same-thread(I2)
  uint32_t next_free_slot = 0;    // same-thread
  RecvWaterMark watermark;

  // RX 所有权记账(I2 豁免:IOBuf deleter 任意线程 → 保留 atomic)
  std::atomic<uint32_t> held_count{0};
  std::atomic<uint32_t> transit_cnt{0};
  std::unique_ptr<UrmaRecycleStack> recycle;

  // credit_pool(仅 IO JFR,same-thread)
  uint32_t credit_idle = 0;
  uint32_t credit_allocated = 0;
  uint32_t rx_depth = 0;

  bool is_fc = false;

  EvbJfr() = default;
  EvbJfr(EvbJfr&& o) noexcept
      : jfr(o.jfr), rx_jfc(o.rx_jfc), rx_jfce(o.rx_jfce), rx_jfce_fd(o.rx_jfce_fd),
        rx_bufs(std::move(o.rx_bufs)),
        recv_posted(o.recv_posted), next_free_slot(o.next_free_slot),
        held_count(o.held_count.load(std::memory_order_relaxed)),
        transit_cnt(o.transit_cnt.load(std::memory_order_relaxed)),
        recycle(std::move(o.recycle)),
        watermark(o.watermark),
        credit_idle(o.credit_idle), credit_allocated(o.credit_allocated),
        rx_depth(o.rx_depth), is_fc(o.is_fc) {
    o.jfr = nullptr; o.rx_jfc = nullptr; o.rx_jfce = nullptr; o.rx_jfce_fd = -1;
  }
  EvbJfr& operator=(EvbJfr&& o) noexcept {
    if (this != &o) {
      jfr = o.jfr; rx_jfc = o.rx_jfc; rx_jfce = o.rx_jfce; rx_jfce_fd = o.rx_jfce_fd;
      rx_bufs = std::move(o.rx_bufs);
      recv_posted = o.recv_posted; next_free_slot = o.next_free_slot;
      held_count.store(o.held_count.load(std::memory_order_relaxed),
                       std::memory_order_relaxed);
      transit_cnt.store(o.transit_cnt.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
      recycle = std::move(o.recycle);
      watermark = o.watermark;
      credit_idle = o.credit_idle; credit_allocated = o.credit_allocated;
      rx_depth = o.rx_depth; is_fc = o.is_fc;
      o.jfr = nullptr; o.rx_jfc = nullptr; o.rx_jfce = nullptr; o.rx_jfce_fd = -1;
    }
    return *this;
  }
  EvbJfr(const EvbJfr&) = delete;
  EvbJfr& operator=(const EvbJfr&) = delete;
};

class UrmaTransportContext {
 public:
  static constexpr int POLL_BATCH_MAX = 64;
  static constexpr int kWaitNoBlock = 0;  // urma_wait_jfc 非阻塞 timeout

  // per-EVB 单例:首连接惰性创建,连接全断不销毁(I4)
  static UrmaTransportContext* getOrCreate(EventBase* evb);
  static UrmaTransportContext* get(EventBase* evb);

  ~UrmaTransportContext();

  bool isValid() const noexcept { return valid_; }

  // 休眠管理(I4:连接全断仅 dormant,不销毁)
  void incActiveConn() noexcept;
  void decActiveConn() noexcept;

  // ── 统一收割分派器(任务 B,I3) ──
  // wait→poll→ack→rearm→poll 五步(URMA 官方中断模式标准)
  // is_fc: IO/FC 选择; is_tx: TX/RX JFC 选择
  void pollAndDispatch(bool is_fc, bool is_tx) noexcept;

  // ── 路由表(进程级,16 分片,自身带锁) ──
  void registerSocket(uint64_t socket_id, UrmaAsyncSocket* socket);
  void unregisterSocket(uint64_t socket_id);
  void registerFcSocket(uint64_t socket_id, UrmaAsyncSocket* socket);
  void unregisterFcSocket(uint64_t socket_id);
  SafeRouteEntryPtr findRouteEntry(uint64_t socket_id);
  SafeRouteEntryPtr findFcRouteEntry(uint64_t socket_id);
  bool socketMapEmpty() const {
    return route_table_.empty() && fc_route_table_.empty();
  }

  // ── IO/FC JFR/JFC 访问器(供 UrmaHandshake 创建 jetty) ──
  urma_jfr_t* ioJfr() const noexcept { return io_jfr_.jfr; }
  urma_jfc_t* ioJfc() const noexcept { return io_jfr_.rx_jfc; }
  urma_jfr_t* fcJfr() const noexcept { return fc_jfr_.jfr; }
  urma_jfc_t* fcJfc() const noexcept { return fc_jfr_.rx_jfc; }
  uint32_t ioJfrId() const noexcept {
    return io_jfr_.jfr ? io_jfr_.jfr->jfr_id.id : 0;
  }
  uint32_t fcJfrId() const noexcept {
    return fc_jfr_.jfr ? fc_jfr_.jfr->jfr_id.id : 0;
  }

  // ── JFS 访问(供 UrmaAsyncSocket 发送路径) ──
  EvbJfs* ioJfs() noexcept { return &io_jfs_; }
  EvbJfs* fcJfs() noexcept { return &fc_jfs_; }

  // sq_inflight 管理(same-thread,I2 无锁)
  bool tryAcquireIoSq() noexcept {
    if (io_jfs_.sq_inflight < io_jfs_.sq_depth) {
      ++io_jfs_.sq_inflight;
      return true;
    }
    return false;
  }
  void releaseIoSq() noexcept { --io_jfs_.sq_inflight; }
  // 问题 06:WR 链按 frag_count 批量申请/归还(原只记 1 个 → SQ 真实溢出 → SIGSEGV)
  bool tryAcquireIoSqN(uint32_t n) noexcept {
    if (io_jfs_.sq_inflight + n <= io_jfs_.sq_depth) {
      io_jfs_.sq_inflight += n;
      return true;
    }
    return false;
  }
  void releaseIoSqN(uint32_t n) noexcept { io_jfs_.sq_inflight -= n; }
  bool tryAcquireFcSq() noexcept {
    if (fc_jfs_.sq_inflight < fc_jfs_.sq_depth) {
      ++fc_jfs_.sq_inflight;
      return true;
    }
    return false;
  }
  void releaseFcSq() noexcept { --fc_jfs_.sq_inflight; }
  // 问题 06:FC WR 链批量申请/归还
  bool tryAcquireFcSqN(uint32_t n) noexcept {
    if (fc_jfs_.sq_inflight + n <= fc_jfs_.sq_depth) {
      fc_jfs_.sq_inflight += n;
      return true;
    }
    return false;
  }
  void releaseFcSqN(uint32_t n) noexcept { fc_jfs_.sq_inflight -= n; }

  // 问题 01:SqWaiter 等待队列(same-thread,I2)
  void addIoSqWaiter(SqWaiter* w) noexcept;
  void removeIoSqWaiter(SqWaiter* w) noexcept;
  void wakeOneIoSqWaiter() noexcept;
  uint32_t ioSqWaiterCount() const noexcept { return io_waiters_.size(); }

  // ── credit_pool(same-thread,I2) ──
  void recharge() noexcept { ++io_jfr_.credit_idle; }
  uint32_t creditIdle() const noexcept { return io_jfr_.credit_idle; }
  uint32_t creditAllocated() const noexcept { return io_jfr_.credit_allocated; }
  uint32_t rxDepth() const noexcept { return io_jfr_.rx_depth; }
  bool tryAllocCredit() noexcept {
    if (io_jfr_.credit_idle > 0) {
      --io_jfr_.credit_idle;
      ++io_jfr_.credit_allocated;
      return true;
    }
    return false;
  }
  void releaseCredit(uint32_t cnt) noexcept {
    io_jfr_.credit_allocated -= cnt;
    io_jfr_.credit_idle += cnt;
  }

  // ── RX slot 回收(I2 豁免:deleter 任意线程) ──
  static void releaseHeldSlot(UrmaTransportContext* ctx, EvbJfr* jfr,
                               uint32_t slot) noexcept;
  static void recycleRxSlot(EvbJfr* jfr, uint32_t slot) noexcept;
  void repostRecycled(EvbJfr* jfr) noexcept;
  void checkRxAccounting(EvbJfr* jfr) noexcept;

  // post recv WR
  static bool postRecvWr(EvbJfr* jfr, uint32_t slot);
  static bool postRecvWrBatch(EvbJfr* jfr, const uint32_t* slots, uint32_t count);
  static uint32_t postRecvWrBatchCount(EvbJfr* jfr, const uint32_t* slots,
                                        uint32_t count) noexcept;

  EventBase* getEventBase() const noexcept { return evb_; }

  // 构造(EventBaseLocal::try_emplace_with 需访问)
  UrmaTransportContext(EventBase* evb, const UrmaConfig& config);

 private:
  // 四件套创建
  bool createIoJfs(urma_context_t* ctx, const UrmaConfig& config);
  bool createFcJfs(urma_context_t* ctx, const UrmaConfig& config);
  bool createIoJfr(urma_context_t* ctx, const UrmaConfig& config);
  bool createFcJfr(urma_context_t* ctx, const UrmaConfig& config);
  bool initRxBufPool(EvbJfr* jfr, uint32_t max_bufs);

  // 统一收割分派器内部(TX/RX 分离)
  void pollAndDispatchTxOnly(urma_jfc_t* tx_jfc, bool is_fc);
  void pollAndDispatchRxOnly(urma_jfc_t* rx_jfc, bool is_fc);
  void handleRecvCqe(bool is_fc, const urma_cr_t& cr);

  void dormant() noexcept;
  void wakeup() noexcept;

  EventBase* evb_;
  bool valid_{false};

  // 问题 5.1:UAF 防护 —— IOBuf deleter 可能在 EventBase 析构后执行,
  // shared_ptr<atomic<bool>> 独立存活,析构时置 false,deleter 短路返回
  using AliveFlag = std::shared_ptr<std::atomic<bool>>;
  AliveFlag alive_{std::make_shared<std::atomic<bool>>(true)};

  // 四件套(独占,I1)
  EvbJfs io_jfs_;
  EvbJfs fc_jfs_;
  EvbJfr io_jfr_;
  EvbJfr fc_jfr_;

  // CqHandler(per-EVB 4 个:io_rx + io_tx + fc_rx + fc_tx)
  UrmaCqHandler* io_rx_cq_handler_{nullptr};
  UrmaCqHandler* io_tx_cq_handler_{nullptr};
  UrmaCqHandler* fc_rx_cq_handler_{nullptr};
  UrmaCqHandler* fc_tx_cq_handler_{nullptr};

  // 休眠状态(I4:连接全断时 dormant,不销毁)
  uint32_t active_conn_count_{0};        // same-thread(I2)
  std::atomic<bool> dormant_{false};

  // 回收栈重投兜底定时器(可配置间隔,默认 10ms 自续)
  // 间隔可通过环境变量 URMA_REPOST_TIMER_MS 覆盖(决定性实验用)
  static uint32_t repostTimerIntervalMs() {
    if (kRepostIntervalMs == 0) {
      const char* env = getenv("URMA_REPOST_TIMER_MS");
      kRepostIntervalMs = env ? static_cast<uint32_t>(atoi(env)) : 10u;
      if (kRepostIntervalMs == 0) kRepostIntervalMs = 10u;
    }
    return kRepostIntervalMs;
  }
  class RepostTimer : private AsyncTimeout {
   public:
    explicit RepostTimer(UrmaTransportContext* owner)
        : AsyncTimeout(owner->evb_), owner_(owner) {}
    void arm() { scheduleTimeout(repostTimerIntervalMs()); }
    void disarm() { cancelTimeout(); }
   private:
    void timeoutExpired() noexcept override;
    UrmaTransportContext* owner_;
  };
  static std::atomic<uint32_t> kRepostIntervalMs;
  std::unique_ptr<RepostTimer> repost_timer_;

  // 路由表(进程级,16 分片)
  UrmaRouteTable route_table_;
  UrmaRouteTable fc_route_table_;

  // 问题 01:SqWaiter 等待队列(same-thread,I2)
  std::deque<SqWaiter*> io_waiters_;

  // per-EVB registry
  static EventBaseLocal<std::unique_ptr<UrmaTransportContext>>& registry();

  // ── 诊断打点(CQE 收割事件链路追踪)──
  // 目的:定位中断模式下 1K payload P99~4300us 根因
  struct DiagCounters {
    std::atomic<uint64_t> handler_ready_io_rx{0};   // handlerReady 调用次数(IO RX)
    std::atomic<uint64_t> handler_ready_io_tx{0};   // handlerReady 调用次数(IO TX)
    std::atomic<uint64_t> wait_le0_skip{0};         // wait_jfc<=0 提前退出次数
    std::atomic<uint64_t> wait_ret_0{0};            // wait_jfc 返回 0(无事件)
    std::atomic<uint64_t> wait_ret_1{0};            // wait_jfc 返回 1(有事件)
    std::atomic<uint64_t> wait_ret_neg{0};          // wait_jfc 返回 -1(错误)
    std::atomic<uint64_t> poll_rx_cqe{0};           // RX CQE 收割总数
    std::atomic<uint64_t> poll_tx_cqe{0};           // TX CQE 收割总数
    std::atomic<uint64_t> rearm_fail{0};            // rearm 失败次数
    std::atomic<uint64_t> repost_timer_tick{0};     // RepostTimer 触发次数
    std::atomic<uint64_t> last_log_us{0};           // 上次打点日志时间
    uint64_t last_handler_rx{0};                     // 上次打点时 handler_ready_io_rx 快照
    uint64_t last_handler_tx{0};                     // 上次打点时 handler_ready_io_tx 快照

    // ── 精细打点:handlerReady 间隔直方图(IO RX)──
    // 间隔桶:<100us / 100-500us / 500-1000us / 1-2ms / 2-4ms / 4-5ms / 5-10ms / >10ms
    std::atomic<uint64_t> rx_interval_lt100{0};
    std::atomic<uint64_t> rx_interval_100_500{0};
    std::atomic<uint64_t> rx_interval_500_1k{0};
    std::atomic<uint64_t> rx_interval_1k_2k{0};
    std::atomic<uint64_t> rx_interval_2k_4k{0};
    std::atomic<uint64_t> rx_interval_4k_5k{0};
    std::atomic<uint64_t> rx_interval_5k_10k{0};
    std::atomic<uint64_t> rx_interval_gt10k{0};
    uint64_t last_rx_handler_us{0};  // 上次 IO RX handlerReady 时间戳(us)

    // ── 精细打点:poll CQE 数量分布(每次 poll 返回的 CQE 数)──
    std::atomic<uint64_t> poll_rx_0{0};  // poll 返回 0 个 CQE
    std::atomic<uint64_t> poll_rx_1{0};  // poll 返回 1 个 CQE
    std::atomic<uint64_t> poll_rx_2_4{0};
    std::atomic<uint64_t> poll_rx_5_10{0};
    std::atomic<uint64_t> poll_rx_gt10{0};

    // ── 精细打点:pollAndDispatch 单次耗时(IO RX)──
    std::atomic<uint64_t> pad_rx_lt10{0};     // <10us
    std::atomic<uint64_t> pad_rx_10_50{0};    // 10-50us
    std::atomic<uint64_t> pad_rx_50_100{0};   // 50-100us
    std::atomic<uint64_t> pad_rx_100_500{0};  // 100-500us
    std::atomic<uint64_t> pad_rx_gt500{0};    // >500us
  };
  DiagCounters diag_{};
  void logDiagIfDue() noexcept;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
