/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace folly {

constexpr size_t kDefaultBlockSize = 8192;
constexpr size_t kMaxBlocksHardLimit = 256;

struct alignas(16) IoBufBlock {
  static constexpr uint32_t kBlockMagic = 0x12345678;

  uint32_t magic;
  std::atomic<int> ref_count;
  size_t capacity;
  size_t data_len;

  char* payloadBegin() {
    return reinterpret_cast<char*>(this) + sizeof(IoBufBlock);
  }
  const char* payloadBegin() const {
    return reinterpret_cast<const char*>(this) + sizeof(IoBufBlock);
  }
  bool full() const { return data_len >= capacity; }
  size_t remaining() const { return capacity - data_len; }
};
static_assert(sizeof(IoBufBlock) == 32, "IoBufBlock must be 32 bytes for 16-byte alignment");

namespace detail {

extern std::atomic<size_t> gIoBufBlockSize;
extern std::atomic<size_t> gMaxBlocksPerThread;

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

} // namespace detail

} // namespace folly
