/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <array>
#include <iostream>
#include <stdexcept>
#include <folly/io/async/urma/UrmaProtocol.h>
using namespace folly::urma_detail;
void require(bool ok) {
  if (!ok)
    throw std::runtime_error("URMA wire assertion");
}
int main() {
  std::array<uint8_t, 65> packet{};
  PacketHeader h{};
  for (auto kind : {PacketKind::Data, PacketKind::Pull, PacketKind::Free,
                    PacketKind::Segment, PacketKind::SegmentReady,
                    PacketKind::SegmentRevoke, PacketKind::SegmentRevoked}) {
    size_t size = kind == PacketKind::Data ? 25
        : kind == PacketKind::Pull         ? 40
        : kind == PacketKind::Segment      ? 64
                                           : 24;
    encodeHeader(packet.data(), {kind, UINT64_MAX, (uint64_t(1) << 48) + 7});
    store32(packet.data() + 24, 8192);
    store64(packet.data() + 32, 0xffffae398028ULL);
    require(decodeHeader(packet.data(), size, h));
    require(
        h.kind == kind && h.destination == UINT64_MAX &&
        h.sequence == (uint64_t(1) << 48) + 7);
    require(load64(packet.data() + 32) == 0xffffae398028ULL);
    require(load32(packet.data()) == 0x55524d41 && packet[6] == uint8_t(kind));
    for (size_t n = 0; n < size; ++n)
      require(!decodeHeader(packet.data(), n, h));
    if (kind != PacketKind::Data)
      require(!decodeHeader(packet.data(), size + 1, h));
    for (size_t byte : {size_t(0), size_t(4), size_t(5), size_t(7)}) {
      packet[byte] ^= 1;
      require(!decodeHeader(packet.data(), size, h));
      packet[byte] ^= 1;
    }
  }
  encodeHeader(packet.data(), {PacketKind::Pull, 1, 1});
  store32(packet.data() + 24, 0);
  require(!decodeHeader(packet.data(), 40, h));
  store32(packet.data() + 24, 1);
  store32(packet.data() + 28, 1);
  require(!decodeHeader(packet.data(), 40, h));
  store32(packet.data() + 28, 0);
  store64(packet.data() + 32, 0);
  require(!decodeHeader(packet.data(), 40, h));
  encodeHeader(packet.data(), {PacketKind::Data, 0, 1});
  require(!decodeHeader(packet.data(), 25, h));
  encodeHeader(packet.data(), {PacketKind::Data, 1, 0});
  require(!decodeHeader(packet.data(), 25, h));
  for (uint8_t kind : {7, 8, 255}) {
    encodeHeader(packet.data(), {PacketKind::Data, 1, 1});
    packet[6] = kind;
    require(!decodeHeader(packet.data(), 25, h));
  }
  encodeHeader(packet.data(), {PacketKind::Data, 1, 1});
  store32(packet.data(), 0x55425331); // Reject UBS1, not a compatibility path.
  require(!decodeHeader(packet.data(), 25, h));

  Hello local;
  local.bufferSize = 4096;
  local.jfr = 37;
  local.eid[0] = 12;
  local.uasid = 123;
  local.tpType = 1;
  local.segmentAddress = uint64_t(1) << 48;
  local.segmentLength = 128 * 1024 * 1024;
  local.segmentToken = 9;
  local.socket = UINT64_MAX;
  local.maxRead = 8 * 1024 * 1024;
  std::array<uint8_t, kHelloSize> hello{};
  encodeHello(hello.data(), local);
  Hello decoded;
  require(decodeHello(hello.data(), hello.size(), decoded));
  // Independent v1 fixture, including every offset and the network byte order.
  const std::array<uint8_t, 75> expected = {{
      'U','R','M','A', 0,75, 0,1, 0,1, 0,0,16,0, 0,0,0,37,
      12,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,123, 1,
      0,1,0,0,0,0,0,0, 0,0,0,0,8,0,0,0, 0,0,0,9, 0,0,0,0,
      255,255,255,255,255,255,255,255, 0,128,0,0}};
  require(hello == expected);
  // A newer server policy must select the exact pair offered by a v1 client.
  static constexpr HelloLayout future{2, 80, nullptr, nullptr};
  constexpr Protocol supported[] = {{future, 2}, {future, 1}, {kHelloV1, 1}};
  auto selected = selectProtocol(load16(expected.data() + 6),
                                 load16(expected.data() + 8), supported);
  require(selected == &supported[2]);
  encodeHello(hello.data(), local, *selected);
  require(hello == expected && !selectProtocol(1, 2, supported) &&
          !selectProtocol(1, 257, supported) && selectProtocol(2, 1, supported) == &supported[1]);
  require(
      decoded.socket == UINT64_MAX && decoded.eid == local.eid &&
      decoded.uasid == local.uasid && decoded.segmentToken == 9 &&
      decoded.segmentAddress == local.segmentAddress && decoded.fcJfr == 0);
  for (size_t n = 0; n < hello.size(); ++n)
    require(!decodeHello(hello.data(), n, decoded));
  for (size_t byte :
       {size_t(0), size_t(4), size_t(6), size_t(8)}) {
    hello[byte] ^= 1;
    require(!decodeHello(hello.data(), hello.size(), decoded));
    hello[byte] ^= 1;
  }
  require(containsRead(local, local.segmentAddress, local.maxRead));
  require(
      containsRead(local, local.segmentAddress + local.segmentLength - 1, 1));
  require(!containsRead(local, local.segmentAddress - 1, 1));
  require(!containsRead(local, local.segmentAddress + local.segmentLength, 1));
  require(!containsRead(local, UINT64_MAX - 1, 8));
  require(!containsRead(local, local.segmentAddress, 0));
  require(!containsRead(local, local.segmentAddress, local.maxRead + 1));
  local.maxRead = 0;
  encodeHello(hello.data(), local);
  require(!decodeHello(hello.data(), hello.size(), decoded));
  local = {};
  local.socket = 1;
  encodeHello(hello.data(), local);
  require(decodeHello(hello.data(), hello.size(), decoded));
  require(!decoded.maxRead && !decoded.segmentAddress);
  uint64_t boundary = 0;
  auto end = encodeEnd(0x123456789abcdefULL);
  require(decodeEnd(end.data(), end.size(), boundary) &&
          boundary == 0x123456789abcdefULL);
  require(!decodeEnd(end.data(), end.size() - 1, boundary));
  end = encodeEnd(0);
  require(!decodeEnd(end.data(), end.size(), boundary));
  encodeHello(hello.data(), local);
  store16(hello.data() + 6, 0);
  require(!decodeHello(hello.data(), hello.size(), decoded));
  std::cout
      << "PASS H24/PULL40/FREE24/SEGMENT64/READY24 and HELLO75: exact wire, full uint64 IDs, versions, READ ranges\n";
}
