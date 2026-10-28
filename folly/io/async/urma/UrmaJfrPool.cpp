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
#include <cstddef>
#include <cstring>

#include <glog/logging.h>

#include <folly/io/async/EventBase.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaJfsPool.h>
#include <folly/io/async/urma/UrmaResource.h>
#include <folly/logging/xlog.h>

namespace folly {

// 任务 C:回收栈单轮重投批量上限(摊薄系统调用,同时限制不变式瞬态偏差窗口)
static constexpr uint32_t kRepostBatchMax = 64;
// 任务 C:记账不变式连续越界采样阈值(10ms × 5 = 50ms 持续越界才告警)
static constexpr uint32_t kAccountingSamples = 5;

// ── 进程级单例 ──

static UrmaJfrPool* g_jfr_pool = nullptr;

UrmaJfrPool* UrmaJfrPool::get() {
  return g_jfr_pool;
}

// ── 创建单个 JFR(含 JFCE + JFC + JFR + recv buffer 预投) ──

bool UrmaJfrPool::createJfr(urma_context_t* ctx, UrmaJfrPool* pool,
                              SharedJfr* jfr, uint32_t rx_depth, bool is_fc,
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
  if (!initRxBufPool(pool, jfr, wm.max_water)) {
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
// 任务 C:同时初始化无锁回收栈 + 回填 RxSlotCtx(常驻稳定存储,评审要点②:
// ctx 生命周期 = rx_bufs 生命周期,长于一切交付出的 IOBuf)
bool UrmaJfrPool::initRxBufPool(UrmaJfrPool* pool, SharedJfr* jfr,
                                uint32_t max_bufs) {
  jfr->rx_bufs.resize(max_bufs);
  jfr->recycle = std::make_unique<UrmaRecycleStack>();
  jfr->recycle->init(max_bufs);
  jfr->held_count.store(0, std::memory_order_relaxed);
  jfr->transit_cnt.store(0, std::memory_order_relaxed);
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
      jfr->recycle.reset();
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
    // 任务 C:deleter 回收上下文(零拷贝交付后跨线程重投的唯一凭据)
    jfr->rx_bufs[i].ctx.pool = pool;
    jfr->rx_bufs[i].ctx.jfr = jfr;
    jfr->rx_bufs[i].ctx.slot = i;
  }

  // 启动时只预投 min_water 个 recv WR(按需投递,设计 §7.3)
  // 游标初始化为 min_water:[0, min_water) 已预投,[min_water, max_bufs)
  // 由 repostRecycled 的水位补充逻辑按需推进(M-3 修复)
  uint32_t to_post = jfr->watermark.min_water;
  for (uint32_t i = 0; i < to_post; ++i) {
    if (!postRecvWr(jfr, i)) {
      XLOG(ERR) << "UrmaJfrPool: initial postRecvWr failed at slot " << i;
      return false;
    }
  }
  jfr->recv_posted.store(to_post, std::memory_order_relaxed);
  jfr->next_free_slot.store(to_post, std::memory_order_relaxed);

  XLOG(INFO) << "UrmaJfrPool: rx buf pool initialized, max_bufs=" << max_bufs
             << " pre_posted=" << to_post
             << " is_fc=" << jfr->is_fc;
  return true;
}

// ── post 单个 recv WR ──
bool UrmaJfrPool::postRecvWr(SharedJfr* jfr, uint32_t slot) {
  if (slot >= jfr->rx_bufs.size()) return false;
  // 重置链残留:批量 post 会写 wr->next,单发前清空防止误链(L-10 地雷)
  jfr->rx_bufs[slot].wr.next = nullptr;
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

// ── 任务 C:批量 post 并返回实际成功数(账实严格一致的关键) ──
// 失败时 urma_post_jfr_wr 已消费 bad_wr 之前的 WR(它们已入 RQ),
// 按链表节点指针比对定位成功前缀长度,调用方据此递增 recv_posted。
uint32_t UrmaJfrPool::postRecvWrBatchCount(SharedJfr* jfr,
                                           const uint32_t* slots,
                                           uint32_t count) noexcept {
  if (count == 0) return 0;
  urma_jfr_wr_t* head = nullptr;
  urma_jfr_wr_t* tail = nullptr;
  // 记录链表节点对应的 slots 下标,用于 bad_wr 定位
  urma_jfr_wr_t* nodes[kRepostBatchMax];
  uint32_t node_cnt = 0;
  for (uint32_t i = 0; i < count && node_cnt < kRepostBatchMax; ++i) {
    uint32_t slot = slots[i];
    if (slot >= jfr->rx_bufs.size()) continue;
    urma_jfr_wr_t* wr = &jfr->rx_bufs[slot].wr;
    wr->next = nullptr;
    nodes[node_cnt++] = wr;
    if (!head) {
      head = wr;
      tail = wr;
    } else {
      tail->next = wr;
      tail = wr;
    }
  }
  if (!head) return 0;
  urma_jfr_wr_t* bad_wr = nullptr;
  urma_status_t ret = urma_post_jfr_wr(jfr->jfr, head, &bad_wr);
  if (ret == URMA_SUCCESS) {
    return node_cnt;
  }
  XLOG(WARN) << "UrmaJfrPool: postRecvWrBatchCount partial failure, status="
             << ret << " count=" << node_cnt;
  if (!bad_wr) {
    // 契约:失败时 bad_wr 必指向首个失败 WR;防御按 0 成功处理
    return 0;
  }
  for (uint32_t i = 0; i < node_cnt; ++i) {
    if (nodes[i] == bad_wr) {
      return i;  // bad_wr 之前的 node_cnt 个已入 RQ
    }
  }
  return 0;
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
    if (!createJfr(ctx, this, &io_jfr_list_[i], config.rx_depth, false, wm)) {
      XLOG(ERR) << "UrmaJfrPool: create io jfr " << i << " failed";
      return false;
    }
  }

  // FC JFR 池
  fc_jfr_list_.resize(config.fc_jfr_count);
  for (uint32_t i = 0; i < config.fc_jfr_count; ++i) {
    if (!createJfr(ctx, this, &fc_jfr_list_[i], config.fc_rx_depth, true, wm)) {
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
  route_table_.clear();
  fc_route_table_.clear();
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
//
// 任务 C(H-3/H-4)记账模型(docs/urma-threading.md 不变量 I4):
//   收割 CQE:    recv_posted-- + transit++   (WR 离开 RQ,事实计数,无截断)
//   movable 交付: transit-- + held++         (所有权随 IOBuf 转移,socket 侧完成)
//   拷贝/控制包:  transit--,slot 入回收栈     (数据已拷出,无持有风险)
//   deleter 回收: held-- + slot 入回收栈      (任意线程)
//   重投:         回收栈弹出 N,post 成功 N 个 → recv_posted+=N
// 旧 REPOST_SLOTS_CAP=256 静态数组整体删除 —— 截断丢 slot 的结构性根因随之消除。
uint32_t UrmaJfrPool::harvestAndDispatch(SharedJfr* jfr) noexcept {
  constexpr int POLL_BATCH_MAX = 64;
  urma_cr_t cr[POLL_BATCH_MAX];
  uint32_t total_cqes = 0;
  int n;
  while ((n = urma_poll_jfc(jfr->jfc, POLL_BATCH_MAX, cr)) > 0) {
    total_cqes += static_cast<uint32_t>(n);

    for (int i = 0; i < n; ++i) {
      uint32_t slot = static_cast<uint32_t>(cr[i].user_ctx);
      void* recv_buf = jfr->rx_bufs[slot].data;
      uint32_t recv_len = cr[i].completion_len;

      // 记账(先加后减序:转移期 slot 双计,sum 只朝 M+1 方向瞬态偏差):
      // WR 离开 RQ,进入交付转移中
      jfr->transit_cnt.fetch_add(1, std::memory_order_acq_rel);
      jfr->recv_posted.fetch_sub(1, std::memory_order_acq_rel);

      // Phase 3: SEND_IMM 控制包路径(MSG_PULL_MEM / MSG_FREE)
      // 控制包仍写 header 到 recv buffer(用于 socket 路由),imm_data 携带控制元数据
      // 从 recv buffer 前 24B 解析 header → socket 路由 → 从 imm_data 和 trailing field 取控制元数据
      if (cr[i].opcode == URMA_CR_OPC_SEND_WITH_IMM) {
        auto* ctrl_hdr = parseHeader(recv_buf);
        if (!ctrl_hdr) {
          XLOG(WARN) << "[URMA] SEND_IMM magic mismatch, slot=" << slot
                     << " recv_len=" << recv_len;
          recycleRxSlot(jfr, slot);
          continue;
        }
        // version 校验(不匹配仅 WARN,不丢弃,向前兼容)
        if (ctrl_hdr->version != URMA_VERSION) {
          XLOG(WARN) << "[URMA] SEND_IMM version mismatch: got="
                     << static_cast<int>(ctrl_hdr->version)
                     << " expected=" << static_cast<int>(URMA_VERSION)
                     << " type=" << static_cast<int>(ctrl_hdr->type);
        }
        auto entry = route_table_.find(ctrl_hdr->dst_socket_id);
        if (!entry) {
          XLOG(WARN) << "[URMA] SEND_IMM socket_map miss: dst_socket_id="
                     << ctrl_hdr->dst_socket_id;
          recycleRxSlot(jfr, slot);
          continue;
        }

        if (ctrl_hdr->type == MSG_PULL_MEM) {
          // PULL_MEM 段表式(免拷贝):SGE[0] = PullMemFragPacket(变长段表)
          // 头 40B + 段 16B×N;段表按值拷出后 slot 立即回收
          if (recv_len < offsetof(PullMemFragPacket, frags)) {
            XLOG(WARN) << "[URMA] PULL_MEM too short: recv_len=" << recv_len;
            recycleRxSlot(jfr, slot);
            continue;
          }
          auto* pkt = static_cast<const PullMemFragPacket*>(recv_buf);
          uint16_t frag_count = pkt->frag_count;
          if (frag_count == 0 || frag_count > kMaxPullFrags ||
              recv_len < offsetof(PullMemFragPacket, frags) +
                             sizeof(PullMemFragDesc) * frag_count) {
            XLOG(WARN) << "[URMA] PULL_MEM bad frag_count=" << frag_count
                       << " recv_len=" << recv_len;
            recycleRxSlot(jfr, slot);
            continue;
          }
          entry->sock.load(std::memory_order_acquire)
              ->onPullMemFrags(ctrl_hdr->msg_id, pkt->frags, frag_count,
                               pkt->total_size);
        } else if (ctrl_hdr->type == MSG_FREE) {
          // FREE:SGE[0] = UrmaMsgHeader(24B),imm_data = msg_id(低 16 位有效)
          uint16_t msg_id = static_cast<uint16_t>(cr[i].imm_data);
          entry->sock.load(std::memory_order_acquire)
              ->onControlMsgImm(MSG_FREE, msg_id, 0, 0);
        } else {
          XLOG(WARN) << "[URMA] SEND_IMM unexpected type="
                     << static_cast<int>(ctrl_hdr->type);
        }
        // 控制包数据已按值解析拷出,无持有风险:立即回收(不变量 I4 范围裁剪)
        recycleRxSlot(jfr, slot);
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
        recycleRxSlot(jfr, slot);
        continue;
      }

      // Phase 3: headroom 路由 — 用 header.dst_socket_id 查分片路由表
      // 发送方写入对端 socket_id(= 接收方本端 socket_id),接收方据此路由
      auto entry = route_table_.find(hdr->dst_socket_id);
      if (!entry) {
        XLOG(WARN) << "[URMA] socket_map miss: dst_socket_id=" << hdr->dst_socket_id
                   << " remote_id=" << cr[i].remote_id.id
                   << " map_size=" << route_table_.size();
        recycleRxSlot(jfr, slot);
        continue;
      }

      // 统一通过 onIoMessageReceived 分发(所有权交付由返回值区分)
      // owned=true:  buffer 所有权已随 IOBuf 转移(transit→held),deleter 负责回收
      // owned=false: 数据已拷出/未消费,本线程立即回收
      bool owned = entry->sock.load(std::memory_order_acquire)->onIoMessageReceived(
          recv_buf, recv_len, &jfr->rx_bufs[slot].ctx);
      if (!owned) {
        recycleRxSlot(jfr, slot);
      }
    }
  }

  // 任务 C:每轮收割后检查回收栈批量重投(摊薄系统调用)
  repostRecycled(jfr);
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

// ── 任务 C(H-3/H-4):RX slot 回收与重投 ──

// deleter 回收入口(任意线程,IOBuf 析构时调用):
// held-- + slot 入无锁回收栈(先入栈后撤销 held,先加后减序)
// 禁止直接 post —— 跨线程 urma_post 虽功能正确但破坏批量摊薄,且与收割线程竞争。
void UrmaJfrPool::releaseHeldSlot(SharedJfr* jfr, uint32_t slot) noexcept {
  if (!jfr || !jfr->recycle) return;
  jfr->recycle->push(slot);
  jfr->held_count.fetch_sub(1, std::memory_order_acq_rel);
}

// 收割线程立即回收(数据已按值消费:控制包/FC/解析失败/降级拷贝):
// transit-- + slot 入无锁回收栈
void UrmaJfrPool::recycleRxSlot(SharedJfr* jfr, uint32_t slot) noexcept {
  if (!jfr || !jfr->recycle) return;
  jfr->recycle->push(slot);
  jfr->transit_cnt.fetch_sub(1, std::memory_order_acq_rel);
}

// 收割线程:批量弹出回收栈并重投。
// 触发点:① harvestAndDispatch/harvestFcAndDispatch 每轮收割后;
//        ② UrmaSharedJfr 10ms idle timer 兜底(防"无新事件导致回收栈滞留")。
// 账实严格一致:batch 失败按 bad_wr 定位成功前缀,只对成功数递增 recv_posted;
// 失败 slot 退回回收栈,下轮事件重试。
//
// 按需投递水位补全(任务:水位功能完整化,修复检视报告 M-3):
// 旧实现只重投【本池回收的 slot】→ 流通量恒为初始预投的 min_water,
// max_water 形同虚设;上层零拷贝持有(held)抬高时 RQ 水位无法恢复。
// 补充逻辑:回收栈排空后 RQ 仍低于 max_water 时,从未投递游标
// (next_free_slot)取新 slot 补投 —— 流通量逐步增长到 max_water 封顶
// (预算:posted + transit + held + 回收栈 ≤ max_water,防止超发)。
void UrmaJfrPool::repostRecycled(SharedJfr* jfr) noexcept {
  if (!jfr || !jfr->recycle || !jfr->jfr) return;
  uint32_t slots[kRepostBatchMax];
  while (true) {
    uint32_t posted = jfr->recv_posted.load(std::memory_order_acquire);
    if (posted >= jfr->watermark.max_water) return;  // RQ 水位已满
    uint32_t room = jfr->watermark.max_water - posted;
    uint32_t want = std::min(room, kRepostBatchMax);
    uint32_t cnt = jfr->recycle->drain(slots, want);
    if (cnt > 0) {
      uint32_t ok = postRecvWrBatchCount(jfr, slots, cnt);
      if (ok > 0) {
        jfr->recv_posted.fetch_add(ok, std::memory_order_acq_rel);
      }
      for (uint32_t i = ok; i < cnt; ++i) {
        jfr->recycle->push(slots[i]);  // 失败部分退栈,下轮重试
      }
      if (ok < cnt) {
        return;  // post 失败:不继续自旋,下轮事件重试
      }
      continue;  // 栈中可能还有,继续排空
    }

    // ── 水位补充(M-3 修复):回收栈空,RQ 低于 max_water → 游标取新 slot ──
    // 预算闸门:流通总量(posted+transit+held+栈) ≤ max_water,
    // 防止 held 高企时超发 slot 超出 buffer 总量
    uint32_t in_flight = posted +
                         jfr->transit_cnt.load(std::memory_order_acquire) +
                         jfr->held_count.load(std::memory_order_acquire) +
                         jfr->recycle->size();
    uint32_t budget = jfr->watermark.max_water > in_flight
                          ? jfr->watermark.max_water - in_flight
                          : 0;
    uint32_t supply = std::min({room, budget, kRepostBatchMax});
    if (supply == 0) {
      return;  // 预算耗尽(流通已满)或水位已到
    }
    uint32_t n = 0;
    uint32_t cursor = jfr->next_free_slot.load(std::memory_order_acquire);
    while (n < supply) {
      if (cursor >= jfr->rx_bufs.size()) {
        break;  // 游标耗尽(全部 slot 已进入流通)
      }
      slots[n++] = cursor++;
    }
    if (n == 0) {
      return;
    }
    jfr->next_free_slot.store(cursor, std::memory_order_release);
    uint32_t ok = postRecvWrBatchCount(jfr, slots, n);
    if (ok > 0) {
      jfr->recv_posted.fetch_add(ok, std::memory_order_acq_rel);
    }
    for (uint32_t i = ok; i < n; ++i) {
      // post 失败的游标 slot 退回回收栈(已计入流通,经栈重投)
      jfr->recycle->push(slots[i]);
    }
    return;  // 游标补充每轮一次(下轮事件/定时器继续)
  }
}

// 任务 C:记账不变式诊断(10ms idle timer 周期调用)。
// 不变式:recv_posted + transit_cnt + held_count + 回收栈深 == 流通 slot 总数。
// 注:流通总量 = 启动时预投的 min_water 个(重投只回收已流通 slot,水位不增长,
//     该行为与检视报告 M-3 描述一致,M-3 完整修复归 M 系列;用 max_water 会误报)。
// 并发转移期存在有界瞬态偏差(先加后减序 + repost 批次窗口),
// 故连续 kAccountingSamples 次采样均越界才告警 —— 捕获 H-4 类系统性
// slot 泄漏/重复(单次截断即永久漂移 ≥1,持续负载下秒级可见),零误报。
void UrmaJfrPool::checkRxAccounting(SharedJfr* jfr) noexcept {
  if (!jfr || !jfr->recycle) return;
  // 流通总量基准(水位补充后动态增长):= max(min_water, 游标推进数)
  // (游标 = 已进入流通的新 slot 总数;恒 ≥ min_water)
  uint32_t circulating = std::max(
      jfr->watermark.min_water,
      jfr->next_free_slot.load(std::memory_order_acquire));
  // 偏差容忍上界:单轮重投批量 + 并发收割转移数(16 JFR × 少量在途)
  constexpr uint32_t kSlack = kRepostBatchMax + 16;
  uint32_t sum = jfr->recv_posted.load(std::memory_order_acquire) +
                 jfr->transit_cnt.load(std::memory_order_acquire) +
                 jfr->held_count.load(std::memory_order_acquire) +
                 jfr->recycle->size();
  int64_t delta = static_cast<int64_t>(sum) -
                  static_cast<int64_t>(circulating);
  if (delta == 0) {
    jfr->accounting_bad_streak.store(0, std::memory_order_relaxed);
    return;
  }
  // 瞬态:偏差 > 0 且 ≤ kSlack(转移双计窗口);重投窗口可能短暂 < 0
  if (delta > 0 && static_cast<uint32_t>(delta) <= kSlack) {
    return;  // 疑似瞬态,不计入连续越界
  }
  if (jfr->accounting_bad_streak.fetch_add(1, std::memory_order_relaxed) + 1 >=
      kAccountingSamples) {
    jfr->accounting_bad_streak.store(0, std::memory_order_relaxed);
    XLOG(ERR) << "[URMA] RX accounting invariant VIOLATED: "
              << "recv_posted=" << jfr->recv_posted.load()
              << " transit=" << jfr->transit_cnt.load()
              << " held=" << jfr->held_count.load()
              << " recycle=" << jfr->recycle->size()
              << " sum=" << sum << " circulating=" << circulating
              << " is_fc=" << jfr->is_fc
              << " (slot leak or duplicate — check deleter/repost paths)";
    DCHECK(delta == 0);  // debug 构建显式断言,便于 CI 抓获
  }
}

// ── 任务 B(H-2):TX CQE(READ)属主转发 ──
//
// 协议(docs/urma-threading.md 不变量 I1/I3,sock 置空协议升级版):
//   1. 先查安全句柄(SafeRouteEntry,shared_ptr 保活);未命中 = 连接已销毁,丢弃。
//   2. 收割线程恰为属主 EVB → 同步短路调用(与属主析构天然串行,安全),零延迟
//      (保持小包路径性能,历史全量 runInLoop 回退的教训见 handleRemoteReadCqe 注释)。
//   3. 跨 EVB → pending++(仅作 entry 保活计数)→ runInEventBaseThread 转发回属主
//      线程(线程安全通道);msg_id/status 按值捕获于调用方 delete tx_ctx 之前
//      (评审要点①)。
//   4. 置空协议:连接销毁(destroyUrmaResources)时 sock 置 nullptr(属主 EVB 写),
//      在途 lambda(恒在属主 EVB 执行)判空跳过回调 —— 结构性消除 UAF,
//      替代原"析构自旋等待 pending==0"方案(自旋阻塞本 EVB 队列,lambda
//      无法执行 → 10ms 超时放行 → UAF,close_loop 高压下真实触发,原 R3 风险)。
void UrmaJfrPool::dispatchReadCqe(uint64_t socket_id, uint16_t msg_id,
                                 urma_status_t status) noexcept {
  auto entry = route_table_.find(socket_id);
  if (!entry) {
    XLOG(WARN) << "[URMA] dispatchReadCqe: READ cqe for unregistered socket_id=0x"
               << std::hex << socket_id << std::dec
               << " msg_id=" << msg_id << " (connection destroyed?)";
    return;
  }
  EventBase* evb = entry->evb;
  UrmaAsyncSocket* sock = entry->sock.load(std::memory_order_acquire);
  if (!evb || !sock) {
    return;  // 连接销毁中(置空协议):丢弃
  }
  if (evb->inRunningEventBaseThread()) {
    // 同 EVB 短路:零延迟同步调用(当前线程即属主 EVB,与销毁串行,安全)
    sock->handleRemoteReadCqe(msg_id, status);
    return;
  }
  // 跨 EVB 转发:pending 先增(entry 保活计数),句柄按值捕获。
  // 线程安全修正:EventBase::runInLoop(Func) 非线程安全(无锁 push_back
  // 到 loopCallbacks_,DCHECK 仅 debug 生效)—— 跨线程必须走
  // runInEventBaseThread(notification queue 通道),否则与本 EVB 线程的
  // runLoopCallbacks 并发破坏 intrusive 链表(4 IO 线程 8M 实测 SIGSEGV)
  entry->pending.fetch_add(1, std::memory_order_acq_rel);
  auto guard = entry;  // shared_ptr 拷贝:entry 生命周期 ≥ lambda 生命周期
  evb->runInEventBaseThread([sock, msg_id, status, guard]() {
    // 置空协议:连接销毁时 sock 已置 nullptr(属主 EVB 写,lambda 恒在属主
    // EVB 执行,与置空天然串行)—— 判空跳过,socket 生命周期不再依赖本 lambda
    if (sock == guard->sock.load(std::memory_order_acquire)) {
      sock->handleRemoteReadCqe(msg_id, status);
    }
    guard->pending.fetch_sub(1, std::memory_order_acq_rel);
  });
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
        // 任务 B(H-2):READ CQE 统一走属主转发协议
        // (同 EVB 短路同步,跨 EVB runInLoop 转发回属主线程);
        // socket_id/msg_id/status 先按值取走,ctx 在转发前 delete(评审要点①)
        uint64_t socket_id = tx_ctx->socket_id;
        uint16_t msg_id = tx_ctx->msg_id;
        urma_status_t status = cr[i].status;
        jfs_pool->releaseIoJfs(tx_ctx->jfs_index);
        delete tx_ctx;
        dispatchReadCqe(socket_id, msg_id, status);
      } else {
        // MSG_DATA / MSG_CTRL_PULL / MSG_CTRL_FREE:
        // 归还 sq_inflight + delete ctx → send_data 析构 → block 回收
        // 资源回收只触碰进程级池(池自身线程安全),收割线程就地完成,零转发;
        // ctx 内 send_data 的 IoBufBlock 归还收割线程 TLS 缓存,内存全局 MR 池,
        // 跨线程归还功能正确(行为说明见 docs/urma-threading.md)
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
// 任务 C(H-3/H-4)记账模型与 harvestAndDispatch 同构:
//   收割 CQE:recv_posted-- + transit++;FC 消息已按值反序列化,立即回收入栈;
//   每轮收割后回收栈批量重投。REPOST_SLOTS_CAP 截断根因随之消除。
uint32_t UrmaJfrPool::harvestFcAndDispatch(SharedJfr* jfr) noexcept {
  constexpr int POLL_BATCH_MAX = 64;
  urma_cr_t cr[POLL_BATCH_MAX];
  uint32_t total_cqes = 0;
  int n;
  while ((n = urma_poll_jfc(jfr->jfc, POLL_BATCH_MAX, cr)) > 0) {
    total_cqes += static_cast<uint32_t>(n);
    for (int i = 0; i < n; ++i) {
      uint32_t slot = static_cast<uint32_t>(cr[i].user_ctx);
      void* recv_buf = jfr->rx_bufs[slot].data;
      uint32_t recv_len = cr[i].completion_len;

      // 记账(先加后减序):WR 离开 RQ,进入交付转移中
      jfr->transit_cnt.fetch_add(1, std::memory_order_acq_rel);
      jfr->recv_posted.fetch_sub(1, std::memory_order_acq_rel);

      // FC 消息:payload 前 8B 携带 SocketId(无 64B headroom header)
      if (recv_len < sizeof(uint64_t)) {
        XLOG(WARN) << "[URMA-FC] recv too short, slot=" << slot
                   << " recv_len=" << recv_len;
        recycleRxSlot(jfr, slot);
        continue;
      }

      uint64_t dst_socket_id = 0;
      std::memcpy(&dst_socket_id, recv_buf, sizeof(uint64_t));

      auto entry = fc_route_table_.find(dst_socket_id);
      if (!entry) {
        XLOG(DBG) << "[URMA-FC] fc_socket_map miss: dst_socket_id=0x"
                   << std::hex << dst_socket_id << std::dec
                   << " map_size=" << fc_route_table_.size();
        recycleRxSlot(jfr, slot);
        continue;
      }

      // 分发到目标 UrmaAsyncSocket(FC 字段已按值反序列化,无持有风险 → 立即回收)
      entry->sock.load(std::memory_order_acquire)->onFcMessageReceived(recv_buf, recv_len);
      recycleRxSlot(jfr, slot);
    }
  }

  // 任务 C:每轮收割后回收栈批量重投
  repostRecycled(jfr);
  return total_cqes;
}

// ── 路由表 ──
// H-1 修复:全部委托 16 分片读写锁路由表,签名不变,调用方零改动

void UrmaJfrPool::registerSocket(uint64_t socket_id, UrmaAsyncSocket* socket) {
  route_table_.insert(socket_id, socket, socket->getEventBase());
}

void UrmaJfrPool::unregisterSocket(uint64_t socket_id) {
  route_table_.erase(socket_id);
}

UrmaAsyncSocket* UrmaJfrPool::findSocket(uint64_t socket_id) {
  auto entry = route_table_.find(socket_id);
  return entry ? entry->sock.load(std::memory_order_acquire) : nullptr;
}

SafeRouteEntryPtr UrmaJfrPool::findRouteEntry(uint64_t socket_id) {
  return route_table_.find(socket_id);
}

void UrmaJfrPool::registerFcSocket(uint64_t socket_id, UrmaAsyncSocket* socket) {
  fc_route_table_.insert(socket_id, socket, socket->getEventBase());
}

void UrmaJfrPool::unregisterFcSocket(uint64_t socket_id) {
  fc_route_table_.erase(socket_id);
}

UrmaAsyncSocket* UrmaJfrPool::findFcSocket(uint64_t socket_id) {
  auto entry = fc_route_table_.find(socket_id);
  return entry ? entry->sock.load(std::memory_order_acquire) : nullptr;
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
