#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace folly {

constexpr size_t kDefaultBlockSize = 8192;
constexpr size_t kMaxBlocksHardLimit = 256;  // 数组硬上限（指针数组 2KB/线程）

extern size_t gIoBufBlockSize;
extern size_t gMaxBlocksPerThread;           // 运行时可调（默认 8，上限 kMaxBlocksHardLimit）

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
