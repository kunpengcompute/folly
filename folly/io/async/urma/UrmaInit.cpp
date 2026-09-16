/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaInit.h>

#ifdef FOLLY_HAVE_URMA

#include <folly/io/IOBuf.h>
#include <folly/memory/IoBufPool.h>
#include <folly/logging/xlog.h>

#include <cstring>
#include <mutex>
#include <thread>
#include <sys/mman.h>
#include <unistd.h>

namespace folly {

bool UrmaInit::s_inited_ = false;
urma_context_t* UrmaInit::s_ctx = nullptr;
urma_target_seg_t* UrmaInit::s_pool_seg = nullptr;
UrmaConfig UrmaInit::s_config;
uint8_t UrmaInit::s_max_jfs_sge = 6;
uint32_t UrmaInit::s_max_jfs_inline_len = 8;
urma_eid_t UrmaInit::s_local_eid = {};

static void* s_mmap_base = nullptr;
static uint64_t s_mmap_size = 0;

// ── 4 级分级内存池 ──
// mmap 区域按设计文档 §4.2.2 比例切分:
//   Small(4KB)  75%   → 98304 个 (384MB)
//   Medium(16KB) 6.25% → 2048 个  (32MB)
//   Big(64KB)   6.25% → 512 个   (32MB)
//   Large(1MB)  12.5% → 64 个    (64MB)
//
// buffer 布局: 空闲时前 8 字节 overlay 为 next 指针(复用 IoBufBlock.magic+capacity);
// 分配后 allocateFromSystem 覆写 magic/capacity,next 自然失效。
// 无需额外 PoolHeader 开销,每个 buffer 节省 8 字节。
//
// 空闲链表: 4 级 × 64 shard,每 shard 一个 mutex

struct FreeNode {
  FreeNode* next;
};

struct ShardPool {
  FreeNode* head = nullptr;
  std::mutex mtx;
};

static ShardPool g_pools[URMA_NUM_TIERS][URMA_NUM_SHARDS];

// 各级 buffer 大小(= IoBufBlock payload 大小,IoBufBlock header 24B 在 buffer 内)
static constexpr size_t TIER_SIZES[URMA_NUM_TIERS] = {
    URMA_SMALL_BUF_SIZE, URMA_MEDIUM_BUF_SIZE, URMA_BIG_BUF_SIZE,
    URMA_LARGE_BUF_SIZE, URMA_HUGE_BUF_SIZE};
// 各级 stride = buffer 大小(无额外 header 开销)
static constexpr size_t TIER_STRIDES[URMA_NUM_TIERS] = {
    URMA_SMALL_BUF_SIZE, URMA_MEDIUM_BUF_SIZE, URMA_BIG_BUF_SIZE,
    URMA_LARGE_BUF_SIZE, URMA_HUGE_BUF_SIZE};

// 各级占比,用公分母 16 直接表示:
//   Small=1/16, Medium=1/16, Big=1/16, Large(1MB)=1/16, Huge(9MB)=12/16
static constexpr uint32_t TIER_NUMERATOR[URMA_NUM_TIERS] = {1, 1, 1, 1, 12};
static constexpr uint32_t TIER_DENOMINATOR = 16;

// 各级在 mmap 中的起始地址和 buffer 数量(运行时由 initMrPool 填充)
static char* g_tier_base[URMA_NUM_TIERS] = {};
static size_t g_tier_count[URMA_NUM_TIERS] = {};

static int poolTier(size_t size) {
  if (size <= URMA_SMALL_BUF_SIZE) return 0;
  if (size <= URMA_MEDIUM_BUF_SIZE) return 1;
  if (size <= URMA_BIG_BUF_SIZE) return 2;
  if (size <= URMA_LARGE_BUF_SIZE) return 3;
  if (size <= URMA_HUGE_BUF_SIZE) return 4;
  return -1;
}

// 按 tier stride 移位,使连续 buffer 均匀落到不同 shard
// shift = log2(stride): Small=12(4KB), Medium=14(16KB), Big=16(64KB),
//                        Large=20(1MB), Huge=23(9MB)
// 移位后连续 buffer 恰好差 1,% 64 得到 0,1,2,...,63 均匀循环
static constexpr int TIER_SHIFTS[URMA_NUM_TIERS] = {12, 14, 16, 20, 23};

static size_t shardIndex(void* ptr, int tier) {
  return (reinterpret_cast<uintptr_t>(ptr) >> TIER_SHIFTS[tier]) % URMA_NUM_SHARDS;
}

// 分配时用线程 ID 选 shard:同一线程始终用同一 shard,锁不竞争
static size_t allocShardIndex() {
  thread_local size_t tid = 
      std::hash<std::thread::id>{}(std::this_thread::get_id()) % URMA_NUM_SHARDS;
  return tid;
}

// 从 buffer 指针反查 tier(按地址范围)
static int tierFromPtr(void* ptr) {
  auto p = reinterpret_cast<char*>(ptr);
  for (int t = URMA_NUM_TIERS - 1; t >= 0; --t) {
    if (p >= g_tier_base[t]) return t;
  }
  return 0;
}

void UrmaInit::initMrPool() {
  s_mmap_size = s_config.pool_size;
  s_mmap_base = mmap(nullptr, s_mmap_size, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (s_mmap_base == MAP_FAILED) {
    XLOG(ERR) << "UrmaInit: mmap failed for MR pool size=" << s_mmap_size;
    s_mmap_base = nullptr;
    return;
  }

  // 注册 MR 段(参照 brpc urma_helper.cpp:369-379)
  urma_reg_seg_flag_t seg_flag{};
  seg_flag.bs.token_policy = URMA_TOKEN_NONE;
  seg_flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;

  urma_seg_cfg_t seg_cfg{};
  seg_cfg.va = reinterpret_cast<uint64_t>(s_mmap_base);
  seg_cfg.len = s_mmap_size;
  seg_cfg.token_id = nullptr;
  seg_cfg.flag = seg_flag;
  seg_cfg.user_ctx = reinterpret_cast<uint64_t>(s_mmap_base);
  seg_cfg.iova = 0;

  s_pool_seg = urma_register_seg(s_ctx, &seg_cfg);
  if (!s_pool_seg) {
    XLOG(ERR) << "UrmaInit: urma_register_seg failed";
    munmap(s_mmap_base, s_mmap_size);
    s_mmap_base = nullptr;
    return;
  }

  // 按比例切分 mmap 区域为分级子池
  char* base = static_cast<char*>(s_mmap_base);
  size_t offset = 0;
  for (int t = 0; t < URMA_NUM_TIERS; ++t) {
    size_t tier_total = (s_mmap_size * TIER_NUMERATOR[t]) / TIER_DENOMINATOR;
    size_t count = tier_total / TIER_STRIDES[t];
    g_tier_base[t] = base + offset;
    g_tier_count[t] = count;

    // 逐个 buffer 放入对应 shard 空闲链表
    // 空闲时 buffer 起始 8 字节 overlay 为 FreeNode.next
    for (size_t i = 0; i < count; ++i) {
      char* buf = g_tier_base[t] + i * TIER_STRIDES[t];
      auto* node = reinterpret_cast<FreeNode*>(buf);
      size_t s = shardIndex(node, t);
      node->next = g_pools[t][s].head;
      g_pools[t][s].head = node;
    }

    offset += count * TIER_STRIDES[t];
    XLOG(INFO) << "UrmaInit: tier=" << t << " buf_size=" << TIER_SIZES[t]
               << " count=" << count << " offset=" << offset;
  }

  XLOG(INFO) << "UrmaInit: MR pool initialized, total=" << s_mmap_size;
}

void UrmaInit::finiMrPool() {
  if (s_pool_seg) {
    urma_unregister_seg(s_pool_seg);
    s_pool_seg = nullptr;
  }
  if (s_mmap_base) {
    munmap(s_mmap_base, s_mmap_size);
    s_mmap_base = nullptr;
  }
  for (int t = 0; t < URMA_NUM_TIERS; ++t) {
    g_tier_base[t] = nullptr;
    g_tier_count[t] = 0;
    for (size_t s = 0; s < URMA_NUM_SHARDS; ++s) {
      g_pools[t][s].head = nullptr;
    }
  }
}

void* UrmaInit::poolAlloc(size_t size) {
  int tier = poolTier(size);
  if (tier < 0) {
    XLOG(WARN) << "UrmaInit: poolAlloc size=" << size << " exceeds max tier (9MB), rejected";
    return nullptr;
  }
  // 用线程 ID 选起始 shard,优先查自己的 shard,miss 后 steal 其余 shard
  size_t start = allocShardIndex();
  for (size_t i = 0; i < URMA_NUM_SHARDS; ++i) {
    size_t s = (start + i) % URMA_NUM_SHARDS;
    auto& pool = g_pools[tier][s];
    std::lock_guard<std::mutex> lock(pool.mtx);
    if (pool.head) {
      void* buf = pool.head;
      pool.head = pool.head->next;
      return buf;
    }
  }
  XLOG(WARN) << "UrmaInit: poolAlloc tier=" << tier << " size=" << size << " exhausted";
  return nullptr;
}

void UrmaInit::poolFree(void* ptr) {
  if (!ptr) return;
  // 校验指针在 mmap 范围内,防止非池指针静默腐败
  if (!s_mmap_base ||
      ptr < s_mmap_base ||
      ptr >= static_cast<char*>(s_mmap_base) + s_mmap_size) {
    return;
  }
  int tier = tierFromPtr(ptr);
  // 空闲时在 buffer 起始 overlay FreeNode(magic/capacity 已不再需要)
  auto* node = reinterpret_cast<FreeNode*>(ptr);
  size_t s = shardIndex(node, tier);
  auto& pool = g_pools[tier][s];
  std::lock_guard<std::mutex> lock(pool.mtx);
  node->next = pool.head;
  pool.head = node;
}

size_t UrmaInit::poolBufSize(size_t requested_size) {
  int tier = poolTier(requested_size);
  if (tier < 0) return 0;
  return TIER_SIZES[tier];
}

void UrmaInit::init(const UrmaConfig& config) {
  if (s_inited_) {
    XLOG(WARN) << "UrmaInit::init already called, skipping";
    return;
  }
  s_config = config;

  // 1. URMA 库初始化
  urma_init_attr_t init_attr = {};
  urma_status_t status = urma_init(&init_attr);
  if (status != URMA_SUCCESS && status != URMA_EEXIST) {
    XLOG(ERR) << "UrmaInit: urma_init failed, status=" << status;
    return;
  }

  // 2. 设备发现 + 按名称选择(参照 brpc FLAGS_urma_device)
  int ndev = 0;
  urma_device_t** devs = urma_get_device_list(&ndev);
  if (!devs || ndev == 0) {
    XLOG(ERR) << "UrmaInit: no URMA device found";
    urma_uninit();
    return;
  }
  urma_device_t* found = nullptr;
  for (int i = 0; i < ndev; ++i) {
    if (s_config.dev_name.empty() ||
        std::string(devs[i]->name) == s_config.dev_name) {
      found = devs[i];
      break;
    }
  }
  if (!found) {
    XLOG(ERR) << "UrmaInit: URMA device not found: " << s_config.dev_name;
    for (int i = 0; i < ndev; ++i) {
      XLOG(ERR) << "UrmaInit: available device[" << i << "]: " << devs[i]->name;
    }
    urma_free_device_list(devs);
    urma_uninit();
    return;
  }
  std::string device_name = found->name;

  // 3. 设备能力查询
  urma_device_attr_t dev_attr;
  if (urma_query_device(found, &dev_attr) != URMA_SUCCESS) {
    XLOG(ERR) << "UrmaInit: urma_query_device failed";
    urma_free_device_list(devs);
    urma_uninit();
    return;
  }
  s_config.validateAndClamp(dev_attr.dev_cap, 0, 0);
  s_max_jfs_sge = dev_attr.dev_cap.max_jfs_sge > 0 ? dev_attr.dev_cap.max_jfs_sge : 6;
  s_max_jfs_inline_len = dev_attr.dev_cap.max_jfs_inline_len > 0 ? dev_attr.dev_cap.max_jfs_inline_len : 8;

  // 4. EID 发现(参照 brpc urma_get_eid_list)
  uint32_t eid_cnt = 0;
  urma_eid_info_t* eids = urma_get_eid_list(found, &eid_cnt);
  if (!eids || eid_cnt == 0) {
    XLOG(ERR) << "UrmaInit: urma_get_eid_list failed";
    urma_free_eid_list(eids);
    urma_free_device_list(devs);
    urma_uninit();
    return;
  }
  // 保存本地 EID(供 HelloMessage 握手通告)
  s_local_eid = eids[0].eid;

  // 5. 创建 context(使用 eid_index,非硬编码 0)
  s_ctx = urma_create_context(found, eids[0].eid_index);
  urma_free_eid_list(eids);
  urma_free_device_list(devs);
  if (!s_ctx) {
    XLOG(ERR) << "UrmaInit: urma_create_context failed";
    urma_uninit();
    return;
  }

  // 6. MR 内存池
  initMrPool();
  if (!s_pool_seg) {
    XLOG(ERR) << "UrmaInit: MR pool init failed, URMA disabled";
    urma_delete_context(s_ctx);
    s_ctx = nullptr;
    urma_uninit();
    return;
  }

  // 7. folly IOBuf 注入(三步)
  setIoBufBlockAllocFree(
      &UrmaInit::poolAlloc, &UrmaInit::poolFree, &UrmaInit::poolBufSize);
  IOBuf::enableMemoryPool();
  IOBuf::setBlockSize(URMA_SMALL_BUF_SIZE);
  IOBuf::enableUnifiedPoolMode();

  s_inited_ = true;
  XLOG(INFO) << "UrmaInit: initialized, device=" << device_name
             << " max_sge=" << static_cast<unsigned>(s_max_jfs_sge)
             << " pool_size=" << s_mmap_size;
}

void UrmaInit::fini() {
  if (!s_inited_) return;

  // 参照 brpc: 不在 fini 中删除 URMA 硬件资源(context/seg/JFR/JFC/JFCE/jetty)
  // 原因:硬件资源在进程退出时由内核自动回收
  // 用户态清理可能导致阻塞(jetty 引用 JFR/JFC 时删除会等待解绑)
  // 仅恢复 IOBuf allocator 到默认值
  setIoBufBlockAllocFree(&std::malloc, &std::free, nullptr);

  // MR 内存池(mmap + register_seg)不 munmap——依赖进程退出时内核回收
  // URMA context 不删除——同上
  urma_uninit();
  s_inited_ = false;
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
