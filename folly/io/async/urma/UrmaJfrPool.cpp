/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaJfrPool.h>

#ifdef FOLLY_HAVE_URMA

#include <algorithm>
#include <cstring>

#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaJfsPool.h>
#include <folly/io/async/urma/UrmaResource.h>
#include <folly/logging/xlog.h>

namespace folly {

// ── 进程级单例 ──

static UrmaJfrPool* g_jfr_pool = nullptr;

UrmaJfrPool* UrmaJfrPool::get() {
  return g_jfr_pool;
}

// ── 创建单个 JFR(含 JFCE + JFC + JFR + recv buffer 预投) ──

bool UrmaJfrPool::createJfr(urma_context_t* ctx, SharedJfr* jfr,
                              uint32_t rx_depth, bool is_fc,
                              const RecvWaterMark& wm) {
  jfr->is_fc = is_fc;
  jfr->rx_depth = rx_depth;
  jfr->watermark = wm;

  // JFCE
  jfr->jfce = urma_create_jfce(ctx);
  if (!jfr->jfce) {
    XLOG(ERR) << "UrmaJfrPool: create jfce failed";
    return false;
  }
  jfr->jfce_fd = jfr->jfce->fd;

  // JFC(绑定 jfce)
  urma_jfc_cfg_t jfc_cfg = {};
  jfc_cfg.depth = rx_depth;
  jfc_cfg.jfce = jfr->jfce;
  jfr->jfc = urma_create_jfc(ctx, &jfc_cfg);
  if (!jfr->jfc) {
    XLOG(ERR) << "UrmaJfrPool: create jfc failed";
    urma_delete_jfce(jfr->jfce);
    jfr->jfce = nullptr;
    return false;
  }

  // JFR
  urma_jfr_cfg_t jfr_cfg = {};
  jfr_cfg.depth = rx_depth;
  jfr_cfg.trans_mode = URMA_TM_RM;
  jfr_cfg.max_sge = 1;  // 每个 recv WR 用 1 个 SGE
  jfr_cfg.min_rnr_timer = URMA_DEFAULT_MIN_RNR_TIMER;
  jfr_cfg.jfc = jfr->jfc;
  jfr->jfr = urma_create_jfr(ctx, &jfr_cfg);
  if (!jfr->jfr) {
    XLOG(ERR) << "UrmaJfrPool: create jfr failed";
    urma_delete_jfc(jfr->jfc);
    jfr->jfc = nullptr;
    urma_delete_jfce(jfr->jfce);
    jfr->jfce = nullptr;
    return false;
  }

  // jetty_id 从 JFR 获取(urma_create_jfr 分配全局唯一 jetty_id)
  // 注意:jfr 本身不持有 jetty_id,jetty 在 UrmaHandshake::allocateResources 中创建
  // 这里记录 jfr 的 jetty_id 供调试
  // jfr->jetty_id 在握手时由 jetty->jetty_id.id 填充

  // recv buffer 池:按 max_water 预分配 buffer,启动时只预投 min_water 个 recv WR
  if (!initRxBufPool(jfr, wm.max_water)) {
    XLOG(ERR) << "UrmaJfrPool: init rx buf pool failed";
    urma_delete_jfr(jfr->jfr);
    jfr->jfr = nullptr;
    urma_delete_jfc(jfr->jfc);
    jfr->jfc = nullptr;
    urma_delete_jfce(jfr->jfce);
    jfr->jfce = nullptr;
    return false;
  }

  // credit_pool 初始化(IO JFR)
  if (!is_fc) {
    jfr->credit_idle.store(rx_depth, std::memory_order_relaxed);
    jfr->credit_allocated.store(0, std::memory_order_relaxed);
  }

  return true;
}

// ── 初始化 recv buffer 池 ──
// 预分配 max_water 个 4KB buffer,但启动时只预投 min_water 个 recv WR
bool UrmaJfrPool::initRxBufPool(SharedJfr* jfr, uint32_t max_bufs) {
  jfr->rx_bufs.resize(max_bufs);
  for (uint32_t i = 0; i < max_bufs; ++i) {
    jfr->rx_bufs[i].data = UrmaInit::poolAlloc(URMA_SMALL_BUF_SIZE);
    if (!jfr->rx_bufs[i].data) {
      XLOG(ERR) << "UrmaJfrPool: rx_buf alloc failed at " << i;
      // 回收已分配的
      for (uint32_t j = 0; j < i; ++j) {
        UrmaInit::poolFree(jfr->rx_bufs[j].data);
        jfr->rx_bufs[j].data = nullptr;
      }
      jfr->rx_bufs.clear();
      return false;
    }
    // 预填充 SGE 和 WR 模板
    jfr->rx_bufs[i].sge.addr = reinterpret_cast<uint64_t>(jfr->rx_bufs[i].data);
    jfr->rx_bufs[i].sge.len = URMA_SMALL_BUF_SIZE;
    jfr->rx_bufs[i].sge.tseg = UrmaInit::getPoolTseg();
    jfr->rx_bufs[i].wr.src.sge = &jfr->rx_bufs[i].sge;
    jfr->rx_bufs[i].wr.src.num_sge = 1;
    jfr->rx_bufs[i].wr.user_ctx = i;  // slot 索引
    jfr->rx_bufs[i].wr.next = nullptr;
  }

  // 启动时只预投 min_water 个 recv WR(按需投递,设计 §7.3)
  uint32_t to_post = jfr->watermark.min_water;
  for (uint32_t i = 0; i < to_post; ++i) {
    if (!postRecvWr(jfr, i)) {
      XLOG(ERR) << "UrmaJfrPool: initial postRecvWr failed at slot " << i;
      return false;
    }
  }
  jfr->recv_posted.store(to_post, std::memory_order_relaxed);

  XLOG(INFO) << "UrmaJfrPool: rx buf pool initialized, max_bufs=" << max_bufs
             << " pre_posted=" << to_post
             << " is_fc=" << jfr->is_fc;
  return true;
}

// ── post 单个 recv WR ──
bool UrmaJfrPool::postRecvWr(SharedJfr* jfr, uint32_t slot) {
  if (slot >= jfr->rx_bufs.size()) return false;
  urma_jfr_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfr_wr(jfr->jfr, &jfr->rx_bufs[slot].wr, &bad_wr);
  if (ret != URMA_SUCCESS) {
    XLOG(WARN) << "UrmaJfrPool: postRecvWr failed at slot " << slot
               << ", status=" << ret;
    return false;
  }
  return true;
}

// ── 批量 post recv WR(性能优化:WR 链表一次提交,摊薄 urma_post_jfr_wr 系统调用) ──
bool UrmaJfrPool::postRecvWrBatch(SharedJfr* jfr, const uint32_t* slots, uint32_t count) {
  if (count == 0) return true;
  // 用 wr.next 字段把多个 WR 串成链表,一次 urma_post_jfr_wr 提交全部
  // 注意:链表期间 wr.next 会被 urma_post_jfr_wr 修改,但 post 返回后硬件已拷贝,可重置
  urma_jfr_wr_t* head = nullptr;
  urma_jfr_wr_t* tail = nullptr;
  for (uint32_t i = 0; i < count; ++i) {
    uint32_t slot = slots[i];
    if (slot >= jfr->rx_bufs.size()) continue;
    urma_jfr_wr_t* wr = &jfr->rx_bufs[slot].wr;
    wr->next = nullptr;
    if (!head) {
      head = wr;
      tail = wr;
    } else {
      tail->next = wr;
      tail = wr;
    }
  }
  if (!head) return true;
  urma_jfr_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfr_wr(jfr->jfr, head, &bad_wr);
  if (ret != URMA_SUCCESS) {
    XLOG(WARN) << "UrmaJfrPool: postRecvWrBatch failed, count=" << count
               << ", status=" << ret;
    return false;
  }
  return true;
}

// ── init ──

bool UrmaJfrPool::init(urma_context_t* ctx, const UrmaConfig& config) {
  if (inited_) return true;

  RecvWaterMark wm;
  wm.min_water = config.min_water;
  wm.max_water = config.max_water;
  wm.batch_post = config.batch_post;

  // IO JFR 池
  io_jfr_list_.resize(config.io_jfr_count);
  for (uint32_t i = 0; i < config.io_jfr_count; ++i) {
    if (!createJfr(ctx, &io_jfr_list_[i], config.rx_depth, false, wm)) {
      XLOG(ERR) << "UrmaJfrPool: create io jfr " << i << " failed";
      return false;
    }
  }

  // FC JFR 池
  fc_jfr_list_.resize(config.fc_jfr_count);
  for (uint32_t i = 0; i < config.fc_jfr_count; ++i) {
    if (!createJfr(ctx, &fc_jfr_list_[i], config.fc_rx_depth, true, wm)) {
      XLOG(ERR) << "UrmaJfrPool: create fc jfr " << i << " failed";
      return false;
    }
  }

  g_jfr_pool = this;
  inited_ = true;
  XLOG(INFO) << "UrmaJfrPool: initialized, io_jfr=" << config.io_jfr_count
             << " fc_jfr=" << config.fc_jfr_count
             << " rx_depth=" << config.rx_depth
             << " min_water=" << wm.min_water
             << " max_water=" << wm.max_water;
  return true;
}

// ── fini ──

void UrmaJfrPool::fini() {
  // 参照 UrmaSharedJfr:不在 fini 中删除 URMA 硬件资源
  // 进程退出时内核自动回收
  io_jfr_list_.clear();
  fc_jfr_list_.clear();
  socket_map_.clear();
  fc_socket_map_.clear();
  g_jfr_pool = nullptr;
  inited_ = false;
}

// ── round-robin 选 JFR ──

SharedJfr* UrmaJfrPool::selectIoJfr(uint32_t* out_index) {
  if (io_jfr_list_.empty()) return nullptr;
  uint32_t idx = io_rr_.fetch_add(1, std::memory_order_relaxed) %
                 static_cast<uint32_t>(io_jfr_list_.size());
  if (out_index) *out_index = idx;
  return &io_jfr_list_[idx];
}

SharedJfr* UrmaJfrPool::selectFcJfr(uint32_t* out_index) {
  if (fc_jfr_list_.empty()) return nullptr;
  uint32_t idx = fc_rr_.fetch_add(1, std::memory_order_relaxed) %
                 static_cast<uint32_t>(fc_jfr_list_.size());
  if (out_index) *out_index = idx;
  return &fc_jfr_list_[idx];
}

// ── 补投 recv WR(单个 slot) ──

void UrmaJfrPool::replenishRxBuf(SharedJfr* jfr, uint32_t slot) noexcept {
  // 单个 slot 补投:CQE 消费后直接重投同一 slot
  // recv_posted 在 pollAndDispatch 中统一管理(批量扣减 consumed 后按水位线补投)
  postRecvWr(jfr, slot);
}

// ── 按水位线批量补投 recv WR ──
// 补投刚回收的 slot(已知空闲),补投到 max_water 水位

void UrmaJfrPool::replenishByWaterMark(SharedJfr* jfr, const uint32_t* free_slots,
                                        uint32_t free_cnt) noexcept {
  uint32_t posted = jfr->recv_posted.load(std::memory_order_relaxed);
  if (posted >= jfr->watermark.max_water || !free_slots || free_cnt == 0) {
    return;  // 水位已满或无待补投 slot
  }

  // 计算需补投数量:补投到 max_water,但不超过刚回收的 slot 数
  uint32_t to_post = jfr->watermark.max_water - posted;
  uint32_t batch_cnt = std::min(free_cnt, to_post);

  // 批量补投:用 WR 链表一次提交(free_slots 中的 slot 已从 RQ 消费,可安全重投)
  if (batch_cnt > 0) {
    postRecvWrBatch(jfr, free_slots, batch_cnt);
    jfr->recv_posted.fetch_add(batch_cnt, std::memory_order_relaxed);
  }
}

// ── pollAndDispatch: IO JFR 的 CQE 处理 ──

// 内部:收割 CQE 并分发(返回收割的 CQE 数)
// 被 pollAndDispatch 调用,rearm 后的二次 poll 也复用此函数
// 任务5:延迟批量补投 — 收割时递减 recv_posted,循环外按水位线批量补投
uint32_t UrmaJfrPool::harvestAndDispatch(SharedJfr* jfr) noexcept {
  constexpr int POLL_BATCH_MAX = 64;
  urma_cr_t cr[POLL_BATCH_MAX];
  // 累计所有 batch 的回收 slot,用于循环外水位线补投
  // 单次 harvestAndDispatch 最多回收的 CQE 数受 CQ 深度限制,
  // 实际中一次 poll 循环回收数 << POLL_BATCH_MAX,用 4 倍容量保守容纳
  static constexpr int REPOST_SLOTS_CAP = POLL_BATCH_MAX * 4;
  uint32_t repost_slots[REPOST_SLOTS_CAP];
  uint32_t repost_cnt_total = 0;
  uint32_t total_cqes = 0;
  int n;
  while ((n = urma_poll_jfc(jfr->jfc, POLL_BATCH_MAX, cr)) > 0) {
    total_cqes += static_cast<uint32_t>(n);

    for (int i = 0; i < n; ++i) {
      uint32_t slot = static_cast<uint32_t>(cr[i].user_ctx);
      void* recv_buf = jfr->rx_bufs[slot].data;
      uint32_t recv_len = cr[i].completion_len;

      // Phase 3: SEND_IMM 控制包路径(MSG_PULL_MEM / MSG_FREE)
      // 控制包仍写 header 到 recv buffer(用于 socket 路由),imm_data 携带控制元数据
      // 从 recv buffer 前 24B 解析 header → socket 路由 → 从 imm_data 和 trailing field 取控制元数据
      if (cr[i].opcode == URMA_CR_OPC_SEND_WITH_IMM) {
        auto* ctrl_hdr = parseHeader(recv_buf);
        if (!ctrl_hdr) {
          XLOG(WARN) << "[URMA] SEND_IMM magic mismatch, slot=" << slot
                     << " recv_len=" << recv_len;
          if (repost_cnt_total < REPOST_SLOTS_CAP) {
            repost_slots[repost_cnt_total++] = slot;
          }
          continue;
        }
        // version 校验(不匹配仅 WARN,不丢弃,向前兼容)
        if (ctrl_hdr->version != URMA_VERSION) {
          XLOG(WARN) << "[URMA] SEND_IMM version mismatch: got="
                     << static_cast<int>(ctrl_hdr->version)
                     << " expected=" << static_cast<int>(URMA_VERSION)
                     << " type=" << static_cast<int>(ctrl_hdr->type);
        }
        auto it = socket_map_.find(ctrl_hdr->dst_socket_id);
        if (it == socket_map_.end()) {
          XLOG(WARN) << "[URMA] SEND_IMM socket_map miss: dst_socket_id="
                     << ctrl_hdr->dst_socket_id;
          if (repost_cnt_total < REPOST_SLOTS_CAP) {
            repost_slots[repost_cnt_total++] = slot;
          }
          continue;
        }

        if (ctrl_hdr->type == MSG_PULL_MEM) {
          // PULL_MEM:SGE[0] = PullMemControlPacket(32B)
          // imm_data = remote_va(u64),trailing data_size 从 recv_buf offset 24 读取
          if (recv_len < sizeof(PullMemControlPacket)) {
            XLOG(WARN) << "[URMA] PULL_MEM too short: recv_len=" << recv_len;
            if (repost_cnt_total < REPOST_SLOTS_CAP) {
              repost_slots[repost_cnt_total++] = slot;
            }
            continue;
          }
          auto* pkt = static_cast<const PullMemControlPacket*>(recv_buf);
          uint64_t remote_va = cr[i].imm_data;
          uint32_t data_size = pkt->data_size;
          it->second->onControlMsgImm(MSG_PULL_MEM, ctrl_hdr->msg_id,
                                      remote_va, data_size);
        } else if (ctrl_hdr->type == MSG_FREE) {
          // FREE:SGE[0] = UrmaMsgHeader(24B),imm_data = msg_id(低 16 位有效)
          uint16_t msg_id = static_cast<uint16_t>(cr[i].imm_data);
          it->second->onControlMsgImm(MSG_FREE, msg_id, 0, 0);
        } else {
          XLOG(WARN) << "[URMA] SEND_IMM unexpected type="
                     << static_cast<int>(ctrl_hdr->type);
        }
        if (repost_cnt_total < REPOST_SLOTS_CAP) {
          repost_slots[repost_cnt_total++] = slot;
        }
        continue;
      }

      // 独立 buffer 路由:从 recv buffer 前 24B 解析 header(MSG_DATA 走此路径)
      auto* hdr = parseHeader(recv_buf);
      if (!hdr) {
        XLOG(WARN) << "[URMA] magic mismatch, slot=" << slot
                   << " recv_len=" << recv_len
                   << " first4=" << std::hex
                   << (recv_buf ? *static_cast<const uint32_t*>(recv_buf) : 0)
                   << std::dec;
        if (repost_cnt_total < REPOST_SLOTS_CAP) {
          repost_slots[repost_cnt_total++] = slot;
        }
        continue;
      }

      // Phase 3: headroom 路由 — 用 header.dst_socket_id 查 socket_map
      // 发送方写入对端 socket_id(= 接收方本端 socket_id),接收方据此路由
      auto it = socket_map_.find(hdr->dst_socket_id);
      if (it == socket_map_.end()) {
        XLOG(WARN) << "[URMA] socket_map miss: dst_socket_id=" << hdr->dst_socket_id
                   << " remote_id=" << cr[i].remote_id.id
                   << " map_size=" << socket_map_.size();
        if (repost_cnt_total < REPOST_SLOTS_CAP) {
          repost_slots[repost_cnt_total++] = slot;
        }
        continue;
      }

      // 统一通过 onIoMessageReceived 分发
      it->second->onIoMessageReceived(recv_buf, recv_len, slot);
      if (repost_cnt_total < REPOST_SLOTS_CAP) {
        repost_slots[repost_cnt_total++] = slot;
      }
    }
  }

  // 延迟批量补投(任务5):本批共回收 repost_cnt_total 个 posted slot,先递减 recv_posted
  if (repost_cnt_total > 0) {
    jfr->recv_posted.fetch_sub(repost_cnt_total, std::memory_order_acq_rel);
  }

  // 水位线补投:recv_posted 低于 min_water 时批量补投到 max_water
  // 只补投本批回收的 slot(已知空闲);不足部分下次收割继续补,recv_posted 不会持续下降
  if (repost_cnt_total > 0) {
    uint32_t posted = jfr->recv_posted.load(std::memory_order_acquire);
    if (posted < jfr->watermark.min_water) {
      uint32_t to_post = jfr->watermark.max_water - posted;
      uint32_t batch = std::min(repost_cnt_total, to_post);
      if (batch > 0) {
        postRecvWrBatch(jfr, repost_slots, batch);
        jfr->recv_posted.fetch_add(batch, std::memory_order_acq_rel);
      }
    }
  }
  return total_cqes;
}

void UrmaJfrPool::pollAndDispatch(SharedJfr* jfr) noexcept {
  if (!jfr || !jfr->jfc) return;

  // epoll 事件驱动模式:epoll 已通知 JFCE fd 可读,直接 poll_jfc 收割 CQE。
  // 不调用 urma_wait_jfc/urma_ack_jfc —— 它们在 epoll 驱动模式下多余且有害:
  // wait/ack 会消费并确认 JFCE 事件,与 edge-triggered rearm 语义冲突,
  // 导致 rearm 后到达的 CQE 事件丢失,pollAndDispatch 触发频率骤降,
  // 表现为 P50 劣化 ~40 倍(4312us vs 110us)。
  // 修复后 P50=49us,优于池化前 110us 基线。

  // 第一次收割
  (void)harvestAndDispatch(jfr);

  // rearm(为下次 epoll 事件武装)
  urma_rearm_jfc(jfr->jfc, false);

  // edge-triggered 模式下 rearm 后再 poll 一次,防止 rearm 窗口期间到达的 CQE 漏掉
  (void)harvestAndDispatch(jfr);

  // TX CQE 收割由 TX JFCE fd 的 epoll 事件独立驱动(UrmaCqHandler is_tx 模式),
  // 不在 RX 事件中耦合收割。RX 和 TX 各有独立的 epoll 事件源。
  // FC JFS TX CQE 不在此处收割:FC 消息量极小(仅建链/流控),
  // fc_tx_depth=64 的 CQ 足以缓冲,避免每次 RX 事件遍历 FC JFS 产生空轮询自旋锁开销。
}

void UrmaJfrPool::harvestTxAndDispatch(uint32_t jfs_index) noexcept {
  UrmaJfsPool* jfs_pool = UrmaJfsPool::get();
  if (!jfs_pool) return;

  SharedJfs* sj = jfs_pool->getIoJfsByIndex(jfs_index);
  if (!sj || !sj->jfc) return;

  constexpr int TX_POLL_BATCH = 32;
  urma_cr_t cr[TX_POLL_BATCH];

  int n;
  while ((n = urma_poll_jfc(sj->jfc, TX_POLL_BATCH, cr)) > 0) {
    for (int i = 0; i < n; ++i) {
      auto* tx_ctx = reinterpret_cast<TxCompletionCtx*>(cr[i].user_ctx);
      if (!tx_ctx) continue;

      if (tx_ctx->type == TxCompletionCtx::MSG_READ) {
        // READ CQE:通过 socket_id 路由到对应连接
        UrmaAsyncSocket* peer = findSocket(tx_ctx->socket_id);
        if (peer) {
          peer->handleRemoteReadCqe(tx_ctx->msg_id, cr[i].status);
        } else {
          XLOG(WARN) << "[URMA] harvestTx: READ cqe for disconnected socket_id=0x"
                     << std::hex << tx_ctx->socket_id << std::dec
                     << " msg_id=" << tx_ctx->msg_id;
        }
        jfs_pool->releaseIoJfs(tx_ctx->jfs_index);
        delete tx_ctx;
      } else {
        // MSG_DATA / MSG_CTRL_PULL / MSG_CTRL_FREE:
        // 归还 sq_inflight + delete ctx → send_data 析构 → block 回收
        jfs_pool->releaseIoJfs(tx_ctx->jfs_index);
        delete tx_ctx;
      }
    }
  }
  // edge-triggered rearm
  urma_rearm_jfc(sj->jfc, false);
}

void UrmaJfrPool::harvestFcTxAndDispatch() noexcept {
  UrmaJfsPool* jfs_pool = UrmaJfsPool::get();
  if (!jfs_pool) return;

  constexpr int TX_POLL_BATCH = 32;
  urma_cr_t cr[TX_POLL_BATCH];

  uint32_t fc_jfs_count = jfs_pool->fcJfsCount();
  for (uint32_t ji = 0; ji < fc_jfs_count; ++ji) {
    SharedJfs* sj = jfs_pool->getFcJfsByIndex(ji);
    if (!sj || !sj->jfc) continue;
    int n;
    while ((n = urma_poll_jfc(sj->jfc, TX_POLL_BATCH, cr)) > 0) {
      for (int i = 0; i < n; ++i) {
        auto* tx_ctx = reinterpret_cast<TxCompletionCtx*>(cr[i].user_ctx);
        if (!tx_ctx) continue;
        jfs_pool->releaseFcJfs(tx_ctx->jfs_index);
        delete tx_ctx;
      }
    }
    urma_rearm_jfc(sj->jfc, false);
  }
}

// ── pollFcAndDispatch: FC JFR 的 CQE 处理 ──
//
// 与 pollAndDispatch 一致采用 epoll 事件驱动模式:
// 不调用 urma_wait_jfc/urma_ack_jfc —— 它们在 epoll 驱动模式下多余且有害
// (会消费并确认 JFCE 事件,与 edge-triggered rearm 语义冲突,导致 CQE 事件丢失,
//  参见 IO JFR 的同类修复:4312us → 49us 的 P50 优化)。
//
// FC 消息路由(Phase 6):payload 前 8B 携带 SocketId(无 64B headroom header),
// 用 fc_socket_map 路由到 UrmaAsyncSocket::onFcMessageReceived。
//
// 当前 FC 消息处理为占位实现(REQ/REP/RETURN/ACK 完整逻辑在 Phase 6 后续补全),
// 仅回收 recv buffer slot,保证 FC JFR 的 RQ 不溢出。

void UrmaJfrPool::pollFcAndDispatch(SharedJfr* jfr) noexcept {
  if (!jfr || !jfr->jfc) return;

  // 第一次收割
  (void)harvestFcAndDispatch(jfr);

  // rearm(为下次 epoll 事件武装)
  urma_rearm_jfc(jfr->jfc, false);

  // edge-triggered 模式下 rearm 后再 poll 一次,防止 rearm 窗口期间到达的 CQE 漏掉
  (void)harvestFcAndDispatch(jfr);
}

// 内部:收割 FC JFR 的 CQE 并路由(返回收割的 CQE 数)
// 任务5:延迟批量补投 — 收割时递减 recv_posted,循环外按水位线批量补投
uint32_t UrmaJfrPool::harvestFcAndDispatch(SharedJfr* jfr) noexcept {
  constexpr int POLL_BATCH_MAX = 64;
  urma_cr_t cr[POLL_BATCH_MAX];
  // 累计所有 batch 的回收 slot,用于循环外水位线补投
  static constexpr int REPOST_SLOTS_CAP = POLL_BATCH_MAX * 4;
  uint32_t repost_slots[REPOST_SLOTS_CAP];
  uint32_t repost_cnt_total = 0;
  uint32_t total_cqes = 0;
  int n;
  while ((n = urma_poll_jfc(jfr->jfc, POLL_BATCH_MAX, cr)) > 0) {
    total_cqes += static_cast<uint32_t>(n);
    for (int i = 0; i < n; ++i) {
      uint32_t slot = static_cast<uint32_t>(cr[i].user_ctx);
      void* recv_buf = jfr->rx_bufs[slot].data;
      uint32_t recv_len = cr[i].completion_len;

      // FC 消息:payload 前 8B 携带 SocketId(无 64B headroom header)
      if (recv_len < sizeof(uint64_t)) {
        XLOG(WARN) << "[URMA-FC] recv too short, slot=" << slot
                   << " recv_len=" << recv_len;
        if (repost_cnt_total < REPOST_SLOTS_CAP) {
          repost_slots[repost_cnt_total++] = slot;
        }
        continue;
      }

      uint64_t dst_socket_id = 0;
      std::memcpy(&dst_socket_id, recv_buf, sizeof(uint64_t));

      auto it = fc_socket_map_.find(dst_socket_id);
      if (it == fc_socket_map_.end()) {
        XLOG(DBG) << "[URMA-FC] fc_socket_map miss: dst_socket_id=0x"
                   << std::hex << dst_socket_id << std::dec
                   << " map_size=" << fc_socket_map_.size();
        if (repost_cnt_total < REPOST_SLOTS_CAP) {
          repost_slots[repost_cnt_total++] = slot;
        }
        continue;
      }

      // 分发到目标 UrmaAsyncSocket(Phase 6: REQ/REP/RETURN/ACK 完整处理)
      it->second->onFcMessageReceived(recv_buf, recv_len);
      if (repost_cnt_total < REPOST_SLOTS_CAP) {
        repost_slots[repost_cnt_total++] = slot;
      }
    }
  }

  // 延迟批量补投(任务5):本批共回收 repost_cnt_total 个 posted slot,先递减 recv_posted
  if (repost_cnt_total > 0) {
    jfr->recv_posted.fetch_sub(repost_cnt_total, std::memory_order_acq_rel);
  }

  // 水位线补投:recv_posted 低于 min_water 时批量补投到 max_water
  if (repost_cnt_total > 0) {
    uint32_t posted = jfr->recv_posted.load(std::memory_order_acquire);
    if (posted < jfr->watermark.min_water) {
      uint32_t to_post = jfr->watermark.max_water - posted;
      uint32_t batch = std::min(repost_cnt_total, to_post);
      if (batch > 0) {
        postRecvWrBatch(jfr, repost_slots, batch);
        jfr->recv_posted.fetch_add(batch, std::memory_order_acq_rel);
      }
    }
  }
  return total_cqes;
}

// ── 路由表 ──

void UrmaJfrPool::registerSocket(uint64_t socket_id, UrmaAsyncSocket* socket) {
  socket_map_[socket_id] = socket;
}

void UrmaJfrPool::unregisterSocket(uint64_t socket_id) {
  socket_map_.erase(socket_id);
}

UrmaAsyncSocket* UrmaJfrPool::findSocket(uint64_t socket_id) {
  auto it = socket_map_.find(socket_id);
  return it != socket_map_.end() ? it->second : nullptr;
}

void UrmaJfrPool::registerFcSocket(uint64_t socket_id, UrmaAsyncSocket* socket) {
  fc_socket_map_[socket_id] = socket;
}

void UrmaJfrPool::unregisterFcSocket(uint64_t socket_id) {
  fc_socket_map_.erase(socket_id);
}

UrmaAsyncSocket* UrmaJfrPool::findFcSocket(uint64_t socket_id) {
  auto it = fc_socket_map_.find(socket_id);
  return it != fc_socket_map_.end() ? it->second : nullptr;
}

// ── JFCE fd 列表(供 UrmaCqHandler 注册) ──

std::vector<UrmaJfrPool::JfrFdEntry> UrmaJfrPool::getAllJfrFds() const {
  std::vector<JfrFdEntry> fds;
  for (uint32_t i = 0; i < io_jfr_list_.size(); ++i) {
    if (io_jfr_list_[i].jfce_fd >= 0) {
      fds.push_back({io_jfr_list_[i].jfce_fd, i, false});
    }
  }
  for (uint32_t i = 0; i < fc_jfr_list_.size(); ++i) {
    if (fc_jfr_list_[i].jfce_fd >= 0) {
      fds.push_back({fc_jfr_list_[i].jfce_fd, i, true});
    }
  }
  return fds;
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
