/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaInit: URMA 全局初始化静态工具类。
 *
 *   封装 urma_init + 设备/EID 发现 + urma_create_context + MR 内存池(mmap + register_seg
 *   + 4 级分级 + 64 shard)+ setIoBufBlockAllocFree 注入 + enableMemoryPool/UnifiedPoolMode。
 *
 *   init()  : 进程级一次性调用,完成 URMA 库初始化 + MR 内存池创建 + folly IOBuf 注入
 *   fini()  : 反初始化(munmap + unregister_seg + urma_uninit)
 *
 *   MR 内存池通过 folly IOBuf::setIoBufBlockAllocFree 三参数注入(alloc + free + size_fn),
 *   使所有 IOBuf::create 分配都从 MR 内存池分配,全部可用于 URMA DMA。
 *
 *   必须在任何 UrmaAsyncSocket / UrmaSharedJfr 之前调用。
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <folly/io/async/urma/UrmaConfig.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

class UrmaInit {
 public:
  static void init(const UrmaConfig& config);
  static void fini();
  static bool isInitialized() { return s_inited_; }

  // 全局 context(MR 池 + URMA 对象创建都基于此)
  static urma_context_t* getContext() { return s_ctx; }
  static urma_target_seg_t* getPoolSeg() { return s_pool_seg; }
  // 全局内存池的 target_seg(供 SGE.tseg 使用,NIC DMA 需要此句柄定位 MR 段)
  // register_seg 返回的就是 target_seg_t*,直接用于 SGE.tseg
  static urma_target_seg_t* getPoolTseg() { return s_pool_seg; }

  // 硬件能力(init 时查询,供 UrmaAsyncSocket 等使用)
  static uint8_t getMaxJfsSge() { return s_max_jfs_sge; }
  static uint32_t getMaxJfsInlineLen() { return s_max_jfs_inline_len; }
  static const UrmaConfig& getConfig() { return s_config; }

  // 本地 EID(init 时从 urma_get_eid_list 获取,供 UrmaHandshake 填充 HelloMessage)
  static urma_eid_t getLocalEid() { return s_local_eid; }

  // MR 内存池分配/释放(由 setIoBufBlockAllocFree 注入到 IoBufPool)
  static void* poolAlloc(size_t size);
  static void poolFree(void* ptr);
  static size_t poolBufSize(size_t requested_size);

  // SDD-URMA-EVBEXCLUSIVE-001: 进程级远端 import cache(从 UrmaJfrPool 迁移)
  // 同一远端 JFR/seg 只 import 一次,double-checked locking 共享
  static urma_target_jetty_t* importRemoteJfr(const urma_rjfr_t* rjfr);
  static urma_target_seg_t* importRemoteSeg(const urma_seg_t* seg);

 private:
  UrmaInit() = delete;

  static void initMrPool();
  static void finiMrPool();

  static bool s_inited_;
  static urma_context_t* s_ctx;
  static urma_target_seg_t* s_pool_seg;
  static UrmaConfig s_config;
  static uint8_t s_max_jfs_sge;
  static uint32_t s_max_jfs_inline_len;
  static urma_eid_t s_local_eid;  // 硬件 max_jfs_sge(init 时查询)
};

// MR 内存池常量(public,供 UrmaInit.cpp 内部函数使用)
// 任务:1M tier 支持分片,9M tier 缩容 ——
//   传输层大消息路径(sendBigMsgByRead/startRead)已改 1M 分片逐块 PULL,
//   不再依赖 9M 单块;但 fbthrift 序列化(RequestChannel/AsyncProcessor)
//   对大帧整块预分配(框架 writer 空间假设,实测改小导致 memcpy 越界),
//   保留缩容 9M tier(12/16 → 1/16,约 4GB)作为序列化大帧工作缓冲。
constexpr size_t URMA_SMALL_BUF_SIZE = 4096;
constexpr size_t URMA_MEDIUM_BUF_SIZE = 16384;
constexpr size_t URMA_BIG_BUF_SIZE = 65536;
constexpr size_t URMA_LARGE_BUF_SIZE = 1048576;  // 1M:分片/序列化块基准
// 任务:彻底去除 9M tier —— 序列化侧经 IOBufQueue 大块分配直接产出
// 1M 块链(见 IOBufQueue.cpp 的 urma 适配),传输层零拷贝段表 PULL,
// 全链路无 >1M 单块需求;>1M 的 poolAlloc 直接拒绝(显性失败)
constexpr size_t URMA_NUM_TIERS = 4;
constexpr size_t URMA_NUM_SHARDS = 64;

// IOBuf block headroom 预留大小(DPDK rte_mbuf 风格)
// 启用后每个 block 的 data 区前 64B 作为 headroom,供 IOBuf::prepend(64)
// 写入 UrmaMsgHeader,实现单 SGE 覆盖 header+data 的 headroom 路由。
// init() 时通过 g_urma_headroom.store(kUrmaHeadroom) 生效。
static constexpr size_t kUrmaHeadroom = 64;

// JFS/JFR RNR 重试参数(参照 UMQ umq_ub.c:21-23,内部固定不暴露给用户)
constexpr uint8_t URMA_DEFAULT_RNR_RETRY = 6;       // 硬件自动重试次数
constexpr uint8_t URMA_DEFAULT_MIN_RNR_TIMER = 19;   // 每次重试间隔 2μs×2^19 ≈ 1.05s
// 8MB READ 在 1000 连接高并发下 DMA 传输/排队可超 16.4ms(原值=2),导致 URMA_CR_ACK_TIMEOUT_ERR
// (status=9)。err_timeout=10 -> 4.096x2^10 ~= 4.2s:大于并发排队时间(<1s),小于客户端
// scanBigMsgTimeout 7s 重发周期,保证重发 PULL 前服务端 READ 已失败并进入 drainRxPendingQueue。
constexpr uint8_t URMA_DEFAULT_ERR_TIMEOUT = 10;     // jetty 错误超时 4.096x2^10 ~= 4.2s

} // namespace folly

#endif // FOLLY_HAVE_URMA
