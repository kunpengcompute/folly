/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaSharedJfr: per-EventBase 共享接收层。
 *
 *   每个 IO 线程一个,创建共享 IO JFR + FC JFR + RX buffer 池 + 路由表。
 *   通过 EventBaseLocal 单例管理(参照 DtoeEventBasePoller)。
 *
 *   pollAndDispatch: 收割 IO JFR CQE → 按 remote_id 路由到 UrmaAsyncSocket
 *   pollFcAndDispatch: 收割 FC JFR CQE(Phase 4 实现)
 *   credit_pool: 原子 IDLE/ALLOCATED(Phase 2 桩,Phase 4 完整实现)
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBaseLocal.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/stub/urma_api.h>

namespace folly {

class UrmaAsyncSocket;
class UrmaCqHandler;

struct UrmaRxBuf {
  void* data;
  urma_sge_t sge;    // SGE 供 wr.src.sge 指向
  urma_jfr_wr_t wr;
};

class UrmaSharedJfr {
 public:
  static constexpr int POLL_BATCH_MAX = 64;

  static UrmaSharedJfr* getOrCreate(EventBase* evb);
  static UrmaSharedJfr* get(EventBase* evb);

  ~UrmaSharedJfr();

  void pollAndDispatch() noexcept;
  void pollFcAndDispatch() noexcept;

  void registerSocket(uint32_t jetty_id, UrmaAsyncSocket* socket);
  void unregisterSocket(uint32_t jetty_id);

  void registerFcSocket(uint32_t fc_jetty_id, UrmaAsyncSocket* socket);
  void unregisterFcSocket(uint32_t fc_jetty_id);

  EventBase* getEventBase() const noexcept { return evb_; }

  // credit_pool (Phase 2 桩, Phase 4 完整实现)
  void recharge() noexcept {
    // Phase 4: credit_idle_.fetch_add(1, std::memory_order_relaxed);
  }
  std::atomic<uint32_t> credit_idle_{0};
  std::atomic<uint32_t> credit_allocated_{0};

  // IO/FC JFR/JFC/JFCE 访问器(供 UrmaHandshake 创建 jetty 时使用)
  urma_jfr_t* ioJfr() const noexcept { return io_jfr_; }
  urma_jfc_t* ioJfc() const noexcept { return io_jfc_; }
  urma_jfr_t* fcJfr() const noexcept { return fc_jfr_; }
  urma_jfc_t* fcJfc() const noexcept { return fc_jfc_; }

  // 构造函数(EventBaseLocal::try_emplace_with 需要访问)
  UrmaSharedJfr(EventBase* evb, const UrmaConfig& config);

 private:
  void replenishRxBuf(uint32_t slot) noexcept;
  void fillFcRxBuf() noexcept;  // Phase 2 空实现

  EventBase* evb_;
  UrmaConfig config_;

  // IO 组
  urma_jfr_t* io_jfr_{nullptr};
  urma_jfc_t* io_jfc_{nullptr};
  urma_jfce_t* io_jfce_{nullptr};
  UrmaCqHandler* io_cq_handler_{nullptr};

  // FC 组
  urma_jfr_t* fc_jfr_{nullptr};
  urma_jfc_t* fc_jfc_{nullptr};
  urma_jfce_t* fc_jfce_{nullptr};
  UrmaCqHandler* fc_cq_handler_{nullptr};

  // RX buffer 池
  std::vector<UrmaRxBuf> rx_bufs_;
  std::vector<urma_jfr_wr_t> fc_rx_wrs_;

  // 路由表
  std::unordered_map<uint32_t, UrmaAsyncSocket*> socket_map_;
  std::unordered_map<uint32_t, UrmaAsyncSocket*> fc_socket_map_;

  static EventBaseLocal<std::unique_ptr<UrmaSharedJfr>>& registry();
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
