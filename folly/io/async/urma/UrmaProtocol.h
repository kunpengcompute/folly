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
constexpr size_t kSegmentSize = 40;
struct Hello;
inline void encodeHelloV1(uint8_t*, const Hello&);
inline bool decodeHelloV1(const uint8_t*, Hello&);
struct HelloLayout {
  uint16_t version, size;
  void (*encode)(uint8_t*, const Hello&);
  bool (*decode)(const uint8_t*, Hello&);
};
struct Protocol {
  const HelloLayout& hello;
  uint8_t wire;
};
// Released combinations, newest first. Keep old entries and their codecs.
inline constexpr HelloLayout kHelloV1{1, 75, encodeHelloV1, decodeHelloV1};
inline constexpr Protocol kProtocols[] = {{kHelloV1, 1}};
inline constexpr auto& kCurrentProtocol = kProtocols[0];
// Bound reads and peeking even when the offered layout is still unknown.
inline constexpr size_t kHelloSize = [] {
  size_t size = 0;
  for (const auto& p : kProtocols)
    if (p.hello.size > size) size = p.hello.size;
  return size;
}();
constexpr size_t kHelloPrefixSize = 10;
template <size_t N>
constexpr const Protocol* selectProtocol(
    uint16_t hello, uint16_t wire, const Protocol (&supported)[N]) {
  for (const auto& p : supported)
    if (p.hello.version == hello && p.wire == wire) return &p;
  return nullptr;
}
constexpr bool supportsWire(uint8_t wire) {
  for (const auto& p : kProtocols)
    if (p.wire == wire) return true;
  return false;
}
// decodeHeader already rejects unknown versions. Only mixed-wire builds
// need a second check against the destination connection.
constexpr bool kSingleWire = [] {
  for (const auto& p : kProtocols)
    if (p.wire != kCurrentProtocol.wire) return false;
  return true;
}();
inline uint8_t sendWire(const Protocol* protocol) {
  return kSingleWire ? kCurrentProtocol.wire : protocol->wire;
}
inline bool acceptsDecodedWire(const Protocol* protocol, uint8_t wire) {
  return kSingleWire || protocol->wire == wire;
}
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
  PacketKind kind{};
  uint8_t wire{kCurrentProtocol.wire};
  uint64_t destination{}, sequence{};
  PacketHeader() = default;
  PacketHeader(PacketKind k, uint64_t dest, uint64_t seq,
               uint8_t version = kCurrentProtocol.wire)
      : kind(k), wire(version), destination(dest), sequence(seq) {}
};
inline void encodeHeader(uint8_t* p, const PacketHeader& h) {
  store32(p, kMagic);
  p[4] = h.wire;
  p[5] = 0;
  p[6] = uint8_t(h.kind);
  p[7] = 0;
  store64(p + 8, h.destination);
  store64(p + 16, h.sequence);
}
inline bool decodeHeader(const uint8_t* p, size_t n, PacketHeader& h) {
  if (n < kHeaderSize || load32(p) != kMagic || !supportsWire(p[4]) || p[5] || p[7] ||
      p[6] > uint8_t(PacketKind::SegmentRevoked) || !load64(p + 8) || !load64(p + 16))
    return false;
  h = {PacketKind(p[6]), load64(p + 8), load64(p + 16), p[4]};
  return h.kind == PacketKind::Data ? n > kHeaderSize
      : h.kind == PacketKind::Pull
      ? n == kPullSize && load32(p + 24) && !load32(p + 28) && load64(p + 32)
      : h.kind == PacketKind::Segment ? n == kHeaderSize + kSegmentSize
      : n == kHeaderSize;
}
struct Hello {
  uint32_t bufferSize{0}, jfr{0};
  std::array<uint8_t, 16> eid{};
  uint32_t uasid{0};
  uint8_t tpType{0};
  uint64_t segmentAddress{0}, segmentLength{0};
  uint32_t segmentToken{0}, fcJfr{0};
  uint64_t socket{0};
  uint32_t maxRead{0};
};
inline void encodeHello(uint8_t* p, const Hello& h,
                        const Protocol& protocol = kCurrentProtocol) {
  store32(p, kMagic);
  store16(p + 4, protocol.hello.size);
  store16(p + 6, protocol.hello.version);
  store16(p + 8, protocol.wire);
  protocol.hello.encode(p, h);
}
inline bool decodeHello(const uint8_t* p, size_t n, Hello& h,
                        const Protocol& protocol = kCurrentProtocol) {
  return n == protocol.hello.size && load32(p) == kMagic && load16(p + 4) == n &&
      load16(p + 6) == protocol.hello.version && load16(p + 8) == protocol.wire &&
      protocol.hello.decode(p, h);
}
inline void encodeHelloV1(uint8_t* p, const Hello& h) {
  store32(p + 10, h.bufferSize);
  store32(p + 14, h.jfr);
  std::memcpy(p + 18, h.eid.data(), 16);
  store32(p + 34, h.uasid);
  p[38] = h.tpType;
  store64(p + 39, h.segmentAddress);
  store64(p + 47, h.segmentLength);
  store32(p + 55, h.segmentToken);
  store32(p + 59, h.fcJfr);
  store64(p + 63, h.socket);
  store32(p + 71, h.maxRead);
}
inline bool decodeHelloV1(const uint8_t* p, Hello& h) {
  h.bufferSize = load32(p + 10);
  h.jfr = load32(p + 14);
  std::memcpy(h.eid.data(), p + 18, 16);
  h.uasid = load32(p + 34);
  h.tpType = p[38];
  h.segmentAddress = load64(p + 39);
  h.segmentLength = load64(p + 47);
  h.segmentToken = load32(p + 55);
  h.fcJfr = load32(p + 59);
  h.socket = load64(p + 63);
  h.maxRead = load32(p + 71);
  if (!h.socket) return false;
  if (h.maxRead)
    return h.segmentAddress && h.segmentLength &&
        h.segmentLength <= UINT64_MAX - h.segmentAddress;
  return !h.segmentAddress && !h.segmentLength && !h.segmentToken;
}
inline bool containsRead(const Hello& h, uint64_t address, uint32_t length) {
  return length && length <= h.maxRead &&
      address >= h.segmentAddress && length <= h.segmentLength &&
      address - h.segmentAddress <= h.segmentLength - length;
}
} // namespace folly::urma_detail
