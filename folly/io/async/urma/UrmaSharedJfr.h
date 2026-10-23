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

  ~UrmaSharedJfr();

  bool isValid() const noexcept { return valid_; }

  void pollAndDispatch() noexcept;
  void pollFcAndDispatch() noexcept;

  // TX CQE 单点收割(epoll 驱动,由 UrmaCqHandler::handlerReady 调用)
  // 只 poll 指定 jfs_index 的 JFS,而非全量遍历
  void harvestTx(uint32_t jfs_index) noexcept;

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

  static EventBaseLocal<std::unique_ptr<UrmaSharedJfr>>& registry();
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
