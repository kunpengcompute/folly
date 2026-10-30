/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaSharedJfr: per-EventBase 接收层薄包装(Phase 4 重构)。
 *
 *   Phase 4 改造:JFR 池化后,JFR 由进程级 UrmaJfrPool 管理。
 *   UrmaSharedJfr 不再创建自己的 JFR/JFC/JFCE,而是从 UrmaJfrPool 选定
 *   一个 IO JFR + 一个 FC JFR,作为 per-EventBase 的 CqHandler 管理器。
 *
 *   pollAndDispatch: 委托给 UrmaJfrPool::pollAndDispatch(selected io jfr)
 *   pollFcAndDispatch: 委托给 UrmaJfrPool::pollFcAndDispatch(selected fc jfr)
 *   registerSocket/unregisterSocket: 委托给 UrmaJfrPool(全局路由表)
 *   ioJfr()/ioJfc(): 返回选定的 JFR/JFC(供 UrmaHandshake 创建 jetty)
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <memory>
#include <vector>

#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBaseLocal.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaJfrPool.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

class UrmaAsyncSocket;
class UrmaCqHandler;

class UrmaSharedJfr {
 public:
  static constexpr int POLL_BATCH_MAX = 64;

  static UrmaSharedJfr* getOrCreate(EventBase* evb);
  static UrmaSharedJfr* get(EventBase* evb);
  // 连接全断时调用:若路由表为空,从 EventBaseLocal erase 自身,
  // 释放 CqHandler 和 epoll 注册,防止残留 JFCE fd 在 LT 模式下持续 spin
  static void releaseIfEmpty(EventBase* evb);

  ~UrmaSharedJfr();

  bool isValid() const noexcept { return valid_; }

  // 休眠管理:连接全断时休眠 CqHandler(从 epoll 摘除),重连时唤醒(重新武装)
  // 避免 LT 模式下 JFCE fd 持续 spin 导致 CPU 100%
  // 任务 D(H-5):inc/dec 均在属主 EVB 线程调用(与路由表注册/注销同点绑定,
  // 见 UrmaAsyncSocket::registerRoutes/destroyUrmaResources),
  // 计数为普通 uint32_t + EVB 断言显式化(原 std::atomic 隐式依赖显式化)
  void incActiveConn() noexcept;  // 路由注册(握手就绪/服务端预注册)时调用
  void decActiveConn() noexcept;  // 路由注销(destroyUrmaResources)时调用,归零则休眠

  void pollAndDispatch() noexcept;
  void pollFcAndDispatch() noexcept;

  // TX CQE 单点收割(epoll 驱动,由 UrmaCqHandler::handlerReady 调用)
  // 只 poll 指定 jfs_index 的 JFS,而非全量遍历
  void harvestTx(uint32_t jfs_index) noexcept;
  // FC JFS TX CQE 收割(Phase 7)
  void harvestFcTx(uint32_t jfs_index) noexcept;

  // 路由表委托给 UrmaJfrPool(全局共享)
  void registerSocket(uint64_t socket_id, UrmaAsyncSocket* socket);
  void unregisterSocket(uint64_t socket_id);

  void registerFcSocket(uint64_t socket_id, UrmaAsyncSocket* socket);
  void unregisterFcSocket(uint64_t socket_id);

  EventBase* getEventBase() const noexcept { return evb_; }

  // credit_pool (Phase 4 委托给 UrmaJfrPool)
  void recharge() noexcept;
  std::atomic<uint32_t> credit_idle_{0};
  std::atomic<uint32_t> credit_allocated_{0};

  // IO/FC JFR/JFC 访问器(供 UrmaHandshake 创建 jetty 时使用)
  // Phase 4: 返回从 UrmaJfrPool 选定的 JFR 的指针
  urma_jfr_t* ioJfr() const noexcept;
  urma_jfc_t* ioJfc() const noexcept;
  urma_jfr_t* fcJfr() const noexcept;
  urma_jfc_t* fcJfc() const noexcept;

  // 任务 A(SDD-URMA-SHARED-JFS-JFR-002): 去 Jetty —— JFR 自带 jfr_id
  // (urma_jfr_t.jfr_id.id, 类型 = urma_jetty_id_t),可直接通告对端 import。
  // 同一 UrmaSharedJfr(同 EVB)选定同一 JFR → 所有连接通告相同 jfr_id。
  uint32_t ioJfrId() const noexcept;
  uint32_t fcJfrId() const noexcept;

  // 构造函数(EventBaseLocal::try_emplace_with 需要访问)
  UrmaSharedJfr(EventBase* evb, const UrmaConfig& config);

 private:
  EventBase* evb_;
  bool valid_{false};

  // Phase 4: 从 UrmaJfrPool 选定的 JFR(per-EventBase 绑定)
  SharedJfr* io_shared_jfr_{nullptr};
  SharedJfr* fc_shared_jfr_{nullptr};
  uint32_t io_jfr_index_{0};
  uint32_t fc_jfr_index_{0};

  // CqHandler(per-EventBase,绑定选定的 JFR 的 JFCE fd)
  UrmaCqHandler* io_cq_handler_{nullptr};
  UrmaCqHandler* fc_cq_handler_{nullptr};

  // TX CqHandler 列表:每个 IO JFS 的 TX JFCE fd 一个
  // 让 READ CQE 有独立的事件驱动收割,不依赖 RX 事件
  std::vector<std::unique_ptr<UrmaCqHandler>> tx_cq_handlers_;

  // 休眠状态:连接全断时 CqHandler 从 epoll 摘除,避免 LT spin
  // 任务 D(H-5):active_conn_count_ 由 atomic 降级为普通 uint32_t +
  // EVB 断言(同 EVB 串行的隐式依赖显式化);dormant_ 保留原子(getOrCreate 跨线程读)
  uint32_t active_conn_count_{0};        // per-EventBase 活跃连接数(仅属主 EVB 访问)
  std::atomic<bool> dormant_{false};     // true=CqHandler 已从 epoll 摘除

  void dormant() noexcept;  // 休眠:drain 残留 CQE + unregister 所有 CqHandler
  void wakeup() noexcept;   // 唤醒:rearm JFC + register 所有 CqHandler

  // ── 任务 C(H-3/H-4):RX 回收栈重投兜底定时器 ──
  // 10ms 周期:① 排空本 EVB 选定 IO/FC JFR 的回收栈(防"无新事件导致回收栈滞留",
  // 上层异步持有 buffer 释放后无 RX/TX 事件驱动的场景);② 记账不变式诊断。
  // JFR 为进程级共享,多个 UrmaSharedJfr 可能选定同一 JFR,重复排空幂等无害。
  class RepostTimer : private AsyncTimeout {
   public:
    explicit RepostTimer(UrmaSharedJfr* owner)
        : AsyncTimeout(owner->evb_), owner_(owner) {}
    void arm() { scheduleTimeout(10); }    // 10ms 自续(任务 C 设计)
    void disarm() { cancelTimeout(); }   // 私有继承下对外暴露取消
   private:
    void timeoutExpired() noexcept override;
    UrmaSharedJfr* owner_;
  };
  std::unique_ptr<RepostTimer> repost_timer_;

  static EventBaseLocal<std::unique_ptr<UrmaSharedJfr>>& registry();
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
