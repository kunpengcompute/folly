#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace folly {

constexpr size_t kDefaultBlockSize = 8192;
constexpr size_t kMaxBlocksHardLimit = 256;  // 数组硬上限（指针数组 2KB/线程）

extern size_t gIoBufBlockSize;
extern size_t gMaxBlocksPerThread;           // 运行时可调（默认 8，上限 kMaxBlocksHardLimit）

/**
 * IoBufPool 底层 block 内存申请/释放函数签名
 *
 * 默认走 std::malloc / std::free,保持原 folly 行为不变。
 * 业务可通过 setIoBufBlockAllocFree() 注入自定义实现(如 DTOE MR 内存池),
 * 注入后所有 IoBufBlock 分配/回收都走注入函数,使 IOBuf TLS 缓存复用的
 * block 全部来自业务指定的内存区(已 MR 注册等)。
 *
 * 约束:
 *   - 必须在首次 IOBuf::create 之前调用(启动期单次设置)
 *   - alloc 返回的内存必须满足 sizeof(IoBufBlock) + payload 连续可用
 *   - free 必须能处理 alloc 返回的指针
 *   - 设置后不可改回 nullptr(避免 TLS 中已缓存的 block 释放路径错乱)
 */
using IoBufBlockAllocFn = void* (*)(size_t size);
using IoBufBlockFreeFn  = void  (*)(void* ptr);
using IoBufBlockSizeFn  = size_t (*)(size_t requested_size);

extern std::atomic<IoBufBlockSizeFn> g_size_fn;  // size 查询函数(默认 nullptr)

// URMA headroom 预留大小(默认 0=禁用,UrmaInit::init 时设为 64)
// 启用后每个 block 的 data 区前 headroom 字节作为预留空间,
// 供 IOBuf::prepend(headroom) 写入 UrmaMsgHeader(DPDK rte_mbuf 风格)。
extern std::atomic<size_t> g_urma_headroom;

/**
 * 注入自定义 block alloc/free 函数
 *
 * 默认 nullptr,走 std::malloc / std::free。
 * 业务(如 folly::DtoeInit)在启动期调用此函数注入 kbdtoe_mempool_alloc /
 * kbdtoe_mempool_free,使 folly IOBuf 内置线程缓存复用的 block 全部 MR 注册。
 *
 * size_fn: 可选(默认 nullptr),返回 alloc 实际返回的 buffer 大小(可能 ≥ requested_size,
 * 如分级内存池返回固定档位 4KB/16KB/64KB/1MB)。nullptr 时按 requested_size 计算 capacity。
 * URMA 分级内存池注入此函数以使 capacity 认实际 buffer 大小,消除碎片浪费。
 *
 * 必须在第一个 IOBuf::create 之前调用。
 */
void setIoBufBlockAllocFree(IoBufBlockAllocFn alloc_fn,
                            IoBufBlockFreeFn free_fn,
                            IoBufBlockSizeFn size_fn = nullptr);

struct IoBufBlock {
  static constexpr uint32_t kBlockMagic = 0x12345678;

  uint32_t magic;
  uint32_t capacity;
  std::atomic<int> ref_count;    // 生命周期：TLS(1) + IOBuf slice 数量
  uint32_t data_len;
  uint64_t padding_;             // 保证 sizeof=24，数据区 8 字节对齐

  char* payloadBegin() {
    return reinterpret_cast<char*>(this) + sizeof(IoBufBlock);
  }
  const char* payloadBegin() const {
    return reinterpret_cast<const char*>(this) + sizeof(IoBufBlock);
  }
  bool full() const { return data_len >= capacity; }
  size_t remaining() const { return capacity - data_len; }
};
static_assert(sizeof(IoBufBlock) == 24, "IoBufBlock must be 24 bytes");

struct TLSBlockCache {
  IoBufBlock* blocks[kMaxBlocksHardLimit];
  size_t count;
  IoBufBlock* current_share;

  TLSBlockCache() : blocks{}, count(0), current_share(nullptr) {}
  ~TLSBlockCache();
};

IoBufBlock* ioBufBlockAllocate();
void ioBufBlockRelease(IoBufBlock* b);
IoBufBlock* share_block(size_t min_capacity = 0);

} // namespace folly
