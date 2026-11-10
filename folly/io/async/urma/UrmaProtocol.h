/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <urma_api.h>

#include <array>
#include <atomic>
#include <cstring>
#include <stdexcept>
namespace folly::urma_detail {
constexpr uint32_t kMagic = 0x55524d41;
constexpr size_t kHeaderBytes = 24, kHelloBytes = 98, kPullBytes = 32;
enum class Message : uint8_t { Data = 0, Pull = 1, Free = 2 };
inline uint64_t getInteger(const uint8_t* p, size_t n) {
  uint64_t result = 0;
  for (size_t i = 0; i < n; ++i) result = (result << 8) | p[i];
  return result;
}
inline void putInteger(uint8_t* p, uint64_t v, size_t n) {
  while (n) {
    p[--n] = uint8_t(v);
    v >>= 8;
  }
}
struct RecordHeader {
  Message type;
  uint64_t destination, sequence;
};
inline void encodeHeader(uint8_t* p, const RecordHeader& h) {
  std::memset(p, 0, kHeaderBytes);
  putInteger(p, kMagic, 4);
  p[4] = 2;
  p[6] = uint8_t(h.type);
  putInteger(p + 8, h.destination, 8);
  putInteger(p + 16, h.sequence, 8);
}
inline bool decodeHeader(const uint8_t* p, size_t n, RecordHeader& h) {
  if (n < 24 || getInteger(p, 4) != kMagic || p[4] != 2 || p[5] || p[7] ||
      p[6] > 2)
    return false;
  h = {Message(p[6]), getInteger(p + 8, 8), getInteger(p + 16, 8)};
  return h.destination && h.sequence;
}
struct Hello {
  uint32_t bufferSize{0}, receiveCount{0}, ioJetty{0}, uasid{0},
      maxRead{0};
  urma_eid_t eid{};
  urma_seg_t segment{};
  uint64_t socketId{0};
  bool read{false};
};
inline std::array<uint8_t, kHelloBytes> encodeHello(const Hello& h) {
  std::array<uint8_t, kHelloBytes> p{};
  putInteger(p.data(), kMagic, 4);
  putInteger(p.data() + 4, kHelloBytes, 2);
  putInteger(p.data() + 6, 4, 2);
  putInteger(p.data() + 8, 2, 2);
  putInteger(p.data() + 10, h.bufferSize, 4);
  putInteger(p.data() + 14, h.receiveCount, 4);
  putInteger(p.data() + 18, h.ioJetty, 4);
  std::memcpy(p.data() + 22, h.eid.raw, 16);
  putInteger(p.data() + 38, h.uasid, 4);
  p[42] = URMA_CTP;
  p[43] = h.read ? 4 : 0;
  std::memcpy(p.data() + 46, h.segment.ubva.eid.raw, 16);
  putInteger(p.data() + 62, h.segment.ubva.uasid, 4);
  putInteger(p.data() + 66, h.segment.ubva.va, 8);
  putInteger(p.data() + 74, h.segment.len, 8);
  putInteger(p.data() + 82, h.segment.token_id, 4);
  putInteger(p.data() + 86, h.socketId, 8);
  putInteger(p.data() + 94, h.maxRead, 4);
  return p;
}
inline bool decodeHello(const uint8_t* p, size_t n, Hello& h) {
  if (n != kHelloBytes || getInteger(p, 4) != kMagic ||
      getInteger(p + 4, 2) != kHelloBytes || getInteger(p + 6, 2) != 4 ||
      getInteger(p + 8, 2) != 2 || p[42] != URMA_CTP || (p[43] & ~4) || p[44] ||
      p[45])
    return false;
  h.bufferSize = getInteger(p + 10, 4);
  h.receiveCount = getInteger(p + 14, 4);
  h.ioJetty = getInteger(p + 18, 4);
  std::memcpy(h.eid.raw, p + 22, 16);
  h.uasid = getInteger(p + 38, 4);
  h.segment = {};
  std::memcpy(h.segment.ubva.eid.raw, p + 46, 16);
  h.segment.ubva.uasid = getInteger(p + 62, 4);
  h.segment.ubva.va = getInteger(p + 66, 8);
  h.segment.len = getInteger(p + 74, 8);
  h.segment.token_id = getInteger(p + 82, 4);
  h.segment.attr.bs.access = URMA_ACCESS_READ;
  h.segment.attr.bs.token_policy = URMA_TOKEN_NONE;
  h.socketId = getInteger(p + 86, 8);
  h.maxRead = getInteger(p + 94, 4);
  h.read = p[43] == 4;
  return h.bufferSize >= 32 && h.receiveCount && h.ioJetty &&
         h.socketId &&
         (h.read ? h.maxRead && h.segment.ubva.va && h.segment.len
                 : !h.maxRead);
}
inline bool contains(const urma_seg_t& s, uint64_t address, size_t length) {
  return length && address >= s.ubva.va && address - s.ubva.va < s.len &&
         length <= s.len - (address - s.ubva.va);
}
inline uint64_t allocateSocketId() {
  static std::atomic<uint64_t> last{0};
  auto value = last.load(std::memory_order_relaxed);
  while (value != UINT64_MAX)
    if (last.compare_exchange_weak(value, value + 1, std::memory_order_relaxed))
      return value + 1;
  throw std::runtime_error("URMA socket ID exhausted");
}
}  // namespace folly::urma_detail
#endif
