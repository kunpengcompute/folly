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
#include <folly/io/async/urma/stub/urma_api.h>

namespace folly {

class UrmaInit {
 public:
  static void init(const UrmaConfig& config);
  static void fini();
  static bool isInitialized() { return s_inited_; }

  // 全局 context(MR 池 + URMA 对象创建都基于此)
  static urma_context_t* getContext() { return s_ctx; }
  static urma_seg_t* getPoolSeg() { return s_pool_seg; }

  // MR 内存池分配/释放(由 setIoBufBlockAllocFree 注入到 IoBufPool)
  static void* poolAlloc(size_t size);
  static void poolFree(void* ptr);
  static size_t poolBufSize(size_t requested_size);

 private:
  UrmaInit() = delete;

  static void initMrPool();
  static void finiMrPool();

  static bool s_inited_;
  static urma_context_t* s_ctx;
  static urma_seg_t* s_pool_seg;
  static UrmaConfig s_config;
};

// MR 内存池常量(public,供 UrmaInit.cpp 内部函数使用)
constexpr size_t URMA_SMALL_BUF_SIZE = 4096;
constexpr size_t URMA_MEDIUM_BUF_SIZE = 16384;
constexpr size_t URMA_BIG_BUF_SIZE = 65536;
constexpr size_t URMA_LARGE_BUF_SIZE = 1048576;
constexpr size_t URMA_NUM_SHARDS = 64;

} // namespace folly

#endif // FOLLY_HAVE_URMA
