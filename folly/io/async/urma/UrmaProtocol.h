/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace folly::urma_detail {
constexpr size_t kHeaderSize = 24;
constexpr size_t kPullSize = 40;
constexpr size_t kHelloSize = 102;
constexpr size_t kSegmentSize = 40;
constexpr uint16_t kHelloVersion = 8;
constexpr uint8_t kWireVersion = 5;
constexpr uint32_t kMagic = 0x55524d41;
enum class PacketKind : uint8_t {
  Data = 0, Pull = 1, Free = 2, Segment = 3, SegmentReady = 4,
  SegmentRevoke = 5, SegmentRevoked = 6
};
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
inline void store16(uint8_t* p, uint16_t n) { putInteger(p, n, 2); }
inline uint16_t load16(const uint8_t* p) { return getInteger(p, 2); }
inline void store32(uint8_t* p, uint32_t n) { putInteger(p, n, 4); }
inline uint32_t load32(const uint8_t* p) { return getInteger(p, 4); }
inline void store64(uint8_t* p, uint64_t n) { putInteger(p, n, 8); }
inline uint64_t load64(const uint8_t* p) { return getInteger(p, 8); }
// TCP write-side boundary: the first business sequence not sent by the peer.
constexpr size_t kEndSize = 12;
inline std::array<uint8_t, kEndSize> encodeEnd(uint64_t nextSequence) {
  std::array<uint8_t, kEndSize> result{};
  store32(result.data(), kMagic);
  store64(result.data() + 4, nextSequence);
  return result;
}
inline bool decodeEnd(const uint8_t* p, size_t n, uint64_t& nextSequence) {
  if (n != kEndSize || load32(p) != kMagic || !load64(p + 4)) return false;
  nextSequence = load64(p + 4);
  return true;
}
struct PacketHeader {
  PacketKind kind;
  uint64_t destination;
  uint64_t sequence;
};
inline void encodeHeader(uint8_t* p, const PacketHeader& h) {
  store32(p, kMagic);
  p[4] = kWireVersion;
  p[5] = 0;
  p[6] = uint8_t(h.kind);
  p[7] = 0;
  store64(p + 8, h.destination);
  store64(p + 16, h.sequence);
}
inline bool decodeHeader(const uint8_t* p, size_t n, PacketHeader& h) {
  if (n < kHeaderSize || load32(p) != kMagic || p[4] != kWireVersion || p[5] || p[7] ||
      p[6] > uint8_t(PacketKind::SegmentRevoked) || !load64(p + 8) || !load64(p + 16))
    return false;
  h = {PacketKind(p[6]), load64(p + 8), load64(p + 16)};
  return h.kind == PacketKind::Data ? n > kHeaderSize
      : h.kind == PacketKind::Pull
      ? n == kPullSize && load32(p + 24) && !load32(p + 28) && load64(p + 32)
      : h.kind == PacketKind::Segment ? n == kHeaderSize + kSegmentSize
      : n == kHeaderSize;
}
struct Hello {
  uint32_t bufferSize{0}, posted{0}, jfr{0};
  std::array<uint8_t, 16> eid{};
  uint32_t uasid{0};
  uint8_t tpType{0};
  bool read{false};
  std::array<uint8_t, 16> segmentEid{};
  uint32_t segmentUasid{0};
  uint64_t segmentAddress{0}, segmentLength{0};
  uint32_t segmentToken{0}, fcJfr{0};
  uint64_t socket{0};
  uint32_t maxRead{0};
};
inline void encodeHello(uint8_t* p, const Hello& h) {
  std::memset(p, 0, kHelloSize);
  store32(p, kMagic);
  store16(p + 4, kHelloSize);
  store16(p + 6, kHelloVersion);
  store16(p + 8, kWireVersion);
  store32(p + 10, h.bufferSize);
  store32(p + 14, h.posted);
  store32(p + 18, h.jfr);
  std::memcpy(p + 22, h.eid.data(), 16);
  store32(p + 38, h.uasid);
  p[42] = h.tpType;
  p[43] = h.read ? 4 : 0;
  std::memcpy(p + 46, h.segmentEid.data(), 16);
  store32(p + 62, h.segmentUasid);
  store64(p + 66, h.segmentAddress);
  store64(p + 74, h.segmentLength);
  store32(p + 82, h.segmentToken);
  store32(p + 86, h.fcJfr);
  store64(p + 90, h.socket);
  store32(p + 98, h.maxRead);
}
inline bool decodeHello(const uint8_t* p, size_t n, Hello& h) {
  if (n != kHelloSize || load32(p) != kMagic || load16(p + 4) != n ||
      load16(p + 6) != kHelloVersion || load16(p + 8) != kWireVersion || (p[43] & ~4) || p[44] ||
      p[45] || !load64(p + 90))
    return false;
  h.bufferSize = load32(p + 10);
  h.posted = load32(p + 14);
  h.jfr = load32(p + 18);
  std::memcpy(h.eid.data(), p + 22, 16);
  h.uasid = load32(p + 38);
  h.tpType = p[42];
  h.read = p[43] != 0;
  std::memcpy(h.segmentEid.data(), p + 46, 16);
  h.segmentUasid = load32(p + 62);
  h.segmentAddress = load64(p + 66);
  h.segmentLength = load64(p + 74);
  h.segmentToken = load32(p + 82);
  h.fcJfr = load32(p + 86);
  h.socket = load64(p + 90);
  h.maxRead = load32(p + 98);
  if (h.read)
    return h.maxRead && h.segmentAddress && h.segmentLength &&
        h.segmentLength <= UINT64_MAX - h.segmentAddress;
  for (size_t i = 46; i < 86; ++i)
    if (p[i])
      return false;
  return !h.maxRead;
}
inline bool containsRead(const Hello& h, uint64_t address, uint32_t length) {
  return h.read && length && length <= h.maxRead &&
      address >= h.segmentAddress && length <= h.segmentLength &&
      address - h.segmentAddress <= h.segmentLength - length;
}
} // namespace folly::urma_detail
