/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaResource: per-连接 URMA 句柄 + per-WR context 结构体。
 *
 *   Phase 2 改造:
 *     - 移除 per-连接 io_jetty / fc_jetty / tx_jfc / tx_jfce / fc_tx_jfc
 *       (JFS 池化后发送侧由 UrmaJfsPool 管理)
 *     - 新增 TxCompletionCtx: per-WR context,内嵌 folly::IOBuf send_data,
 *       post 前 move-in,TX CQE 后 RAII 析构回收 block
 *     - 保留远端导入句柄(remote_jetty / remote_fc_jetty / remote_seg)
 *     - 保留共享层指针(Phase 4 重构为 UrmaJfrPool 后更新)
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <cstdint>
#include <memory>
#include <vector>

#include <folly/io/IOBuf.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

// per-WR 发送 context(设计 §5 TxCompletionCtx)
// post 前 new TxCompletionCtx{..., std::move(io_buf)}
// TX CQE 后 delete ctx → send_data 析构 → block 回收到对应池
struct TxCompletionCtx {
  uint64_t socket_id;          // 本端 SocketId(TX CQE 路由用,harvestTxAndDispatch 据此查 socket_map)
  uint16_t seq;                // 消息序列号(Phase 5 保序用,当前未使用)
  uint8_t  type;               // MSG_DATA / MSG_CTRL_PULL / MSG_CTRL_FREE / MSG_READ
  uint8_t  padding;
  uint32_t jfs_index;          // 归还 sq_inflight 用(UrmaJfsPool::releaseIoJfs)
  uint16_t msg_id;             // 大包 msg_id(READ/控制包匹配用)
  uint16_t padding2;
  folly::IOBuf send_data;      // 持有发送 IOBuf block,RAII 析构回收
  std::shared_ptr<std::vector<urma_sge_t>> sge_arr;  // SGE 数组(保持存活到 post 完成)
  // 任务:1M 分片 —— READ WR 链(N 个单段 READ,urma rw 仅支持单 src/dst SGE;
  // 链式 post,仅尾 WR complete_enable=1 产生单 CQE),保活到 CQE
  std::unique_ptr<urma_jfs_wr_t[]> read_wr_chain;
  uint32_t read_wr_cnt = 0;

  // 消息类型常量
  static constexpr uint8_t MSG_DATA = 0;       // 普通小包 SEND
  static constexpr uint8_t MSG_CTRL_PULL = 1;  // 大包控制包 PULL(SEND_IMM)
  static constexpr uint8_t MSG_CTRL_FREE = 2;  // 大包 FREE 通知(SEND_IMM)
  static constexpr uint8_t MSG_READ = 3;       // 大包单边 READ(分片 WR 链)
};

// per-WR 接收 context(设计 §7 RxRecvCtx)
// Phase 4 引入,当前预定义
struct RxRecvCtx {
  void*    recv_buf;           // recv buffer(4KB,从 MR 池分配)
  uint32_t jfr_index;          // 补投时定位 JFR
  uint32_t padding;
};

struct UrmaResource {
  // 任务 A(SDD-URMA-SHARED-JFS-JFR-002): 去 Jetty —— io_jetty/fc_jetty 已删除。
  // JFR 自带 jfr_id(urma_jfr_t.jfr_id.id),对端通过 urma_import_jfr 导入。
  // 本端通告 shared_->ioJfrId()(JFR 级共享,同 EVB 所有连接相同)。

  // ── 远端导入句柄(借用指针,由进程级 UrmaJfrPool import cache 持有) ──
  // 连接断开不 unimport(I1/I4:进程级生命周期)
  urma_target_jetty_t* remote_jetty = nullptr;
  urma_target_jetty_t* remote_fc_jetty = nullptr;
  urma_target_seg_t* remote_seg = nullptr;

  // ── 对端通告的 jfr_id(HelloMessage 里的值,CQE remote_id 匹配用) ──
  uint32_t peer_io_jetty_id = 0;  // 语义:对端 IO JFR 的共享 jfr_id
  uint32_t peer_fc_jetty_id = 0;  // 语义:对端 FC JFR 的共享 jfr_id

  // ── 对端 SocketId(发送方写入 header.dst_socket_id 的值) ──
  // 发送方写入 header.dst_socket_id = peer_socket_id(对端 = 接收方本端)
  // 接收方收到数据后用 header.dst_socket_id 查 socket_map 路由到本 socket
  // 注册时 key = 本端 socket_id_.toUint64()(接收方查表命中)
  uint64_t peer_socket_id = 0;

  // ── 共享层指针(非 owned,Phase 4 由 UrmaJfrPool 持有) ──
  urma_jfr_t* shared_io_jfr = nullptr;
  urma_jfc_t* shared_io_jfc = nullptr;
  urma_jfr_t* shared_fc_jfr = nullptr;
  urma_jfc_t* shared_fc_jfc = nullptr;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA
