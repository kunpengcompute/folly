/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
#include <folly/io/async/urma/UrmaProtocol.h>
#include <folly/io/async/urma/UrmaSegmentState.h>
#include <atomic>
#include <iostream>
#include <stdexcept>

using namespace folly::urma_detail;
namespace {
std::atomic<bool> failUnimport{false};
std::atomic<unsigned> unimports{0};
void check(bool ok, const char* why) {
  if (!ok) throw std::runtime_error(why);
}
}
extern "C" urma_status_t urma_unimport_seg(urma_target_seg_t* segment) {
  if (failUnimport) return URMA_EAGAIN;
  delete segment;
  ++unimports;
  return URMA_SUCCESS;
}
int main() {
  using Type = SegmentState::Type;
  urma_seg_t initial{};
  initial.ubva.eid.raw[0] = 7;
  initial.ubva.uasid = 13;
  initial.ubva.va = 4096;
  initial.len = 4096;
  std::array<uint8_t, 40> descriptor{};
  std::memcpy(descriptor.data(), initial.ubva.eid.raw, 16);
  putInteger(descriptor.data() + 16, initial.ubva.uasid, 4);
  putInteger(descriptor.data() + 20, 16384, 8);
  putInteger(descriptor.data() + 28, 8192, 8);
  putInteger(descriptor.data() + 36, 19, 4);
  unsigned imports = 0;
  auto import = [&](const urma_seg_t& s) {
    ++imports;
    auto result = std::shared_ptr<ImportedSegment>(new ImportedSegment,
        [](ImportedSegment* p) { cleanupOrDefer(std::unique_ptr<ImportedSegment>(p)); });
    result->segment = new urma_target_seg_t{};
    result->segment->seg = s;
    return result;
  };
  for (bool close : {false, true}) {
    SegmentState segments;
    const uint64_t id = uint64_t(1) << 40;
    auto receive = [&](Type type, uint64_t token, size_t size = 0) {
      segments.receive(type, token, descriptor.data(), size, initial, import);
    };
    auto reject = [&](Type type, uint64_t token, size_t size) {
      bool rejected = false;
      try { receive(type, token, size); }
      catch (const std::runtime_error&) { rejected = true; }
      check(rejected, "invalid descriptor/control accepted");
    };
    reject(Type::Announce, id, 39);
    descriptor[0] ^= 1;
    reject(Type::Announce, id, 40);
    descriptor[0] ^= 1;
    for (uint64_t address : {uint64_t(0), initial.ubva.va, UINT64_MAX - 1}) {
      putInteger(descriptor.data() + 20, address, 8);
      reject(Type::Announce, id, 40);
    }
    putInteger(descriptor.data() + 20, 16384, 8);
    receive(Type::Announce, id, 40);
    check(segments.pending() && segments.pending()->type == Type::Ready &&
              segments.pending()->id == id, "missing READY/full-width identity");
    segments.pop();
    auto heldRead = segments.find(16384, 8192);
    check(heldRead && heldRead->segment->seg.token_id == 19 &&
              !segments.find(16383, 1) && !segments.find(24575, 2),
          "import range/descriptor changed");
    const auto imported = imports;
    receive(Type::Announce, id, 40);
    segments.pop();
    check(imports == imported, "duplicate announcement imported again");
    reject(Type::Announce, id + 1, 40);
    reject(Type::Ready, id, 0);
    if (close) segments.close(true);
    else receive(Type::Revoke, id);
    check(!segments.find(16384, 1) && !segments.pending() && segments.waiting(),
          "revocation acknowledged while a READ still holds the import");
    failUnimport = true;
    auto released = heldRead->released;
    heldRead.reset();
    segments.progress();
    check(!released->load() && !segments.pending() && !segments.empty(),
          "failed unimport acknowledged or discarded its cleanup responsibility");
    failUnimport = false;
    check(waitForCleanups(std::chrono::seconds(3)), "unimport did not recover");
    segments.progress();
    check(released->load() && segments.pending() &&
              segments.pending()->type == Type::Revoked,
          "missing REVOKED after successful hardware cleanup");
    segments.pop();
    receive(Type::Revoke, id);
    check(segments.pending() && segments.pending()->type == Type::Revoked,
          "duplicate REVOKE lost acknowledgement");
    segments.pop();
    reject(Type::Announce, id, 40);
    check(segments.empty() && !segments.waiting(), "segment state retained after cleanup");
  }
  check(unimports == 2, "unexpected import cleanup count");
  std::cout << "PASS segment validation, READ lifetime, revoke/close, failed unimport and replay\n";
}
