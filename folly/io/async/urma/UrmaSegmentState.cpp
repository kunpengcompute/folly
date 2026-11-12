/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
#include <folly/io/async/urma/UrmaSegmentState.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaProtocol.h>
#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <utility>

namespace folly::urma_detail {
namespace {
void require(bool value, const char* why) {
  if (!value) throw std::runtime_error(why);
}
bool containsRange(const urma_seg_t& s, uint64_t address, size_t n) {
  return n && address >= s.ubva.va && n <= s.len &&
      address - s.ubva.va <= s.len - n;
}
void remember(std::set<uint64_t>& ids, uint64_t id) {
  ids.insert(id);
  if (ids.size() > 64) ids.erase(ids.begin());
}
}
std::shared_ptr<ImportedSegment> ImportedSegment::create(const urma_seg_t& s) {
  auto result = std::shared_ptr<ImportedSegment>(new ImportedSegment,
      [](ImportedSegment* p) { cleanupOrDefer(std::unique_ptr<ImportedSegment>(p)); });
  result->context = UrmaInit::acquireContext();
  if (!result->context) return {};
  auto description = s;
  urma_import_seg_flag_t flags{};
  flags.bs.access = URMA_ACCESS_READ;
  urma_token_t token{};
  result->segment = urma_import_seg(result->context, &description, &token, 0, flags);
  return result->segment ? result : nullptr;
}
bool ImportedSegment::tryCleanup() noexcept {
  if (segment) {
    const auto st = urma_unimport_seg(segment);
    if (st != URMA_SUCCESS) return failed("unimport_read_segment", st);
    segment = nullptr;
  }
  if (context) { context = nullptr; UrmaInit::releaseContext(); }
  released->store(true, std::memory_order_release);
  return true;
}
bool SegmentState::Lease::tryCleanup() noexcept {
  if (announced && !released) return failed("segment_revoke_unconfirmed", URMA_EAGAIN);
  block = {};
  return true;
}
bool SegmentState::prepare(const UrmaPoolBuffer& exported, uint64_t initialAddress) {
  const auto& s = exported.segment()->seg;
  if (s.ubva.va == initialAddress) return true;
  require(!closing_ && !revokedLocal_.count(exported.regionId()),
          "URMA peer has released this segment");
  auto it = local_.find(exported.regionId());
  if (it != local_.end()) return it->second.ready && !it->second.revoking;
  urma_seg_attr_t supported{};
  supported.bs.access = URMA_ACCESS_READ;
  supported.bs.token_policy = URMA_TOKEN_NONE;
  supported.bs.cacheable = s.attr.bs.cacheable;
  supported.bs.non_pin = s.attr.bs.non_pin;
  supported.bs.user_token_id = s.attr.bs.user_token_id;
  require(s.attr.value == supported.value && local_.size() < 1024,
          "URMA segment attributes or announcement limit");
  auto lease = std::shared_ptr<Lease>(new Lease, [](Lease* p) {
    if (p->announced && !p->released) cleanupOrDefer(std::unique_ptr<Lease>(p));
    else delete p;
  });
  lease->block = UrmaInit::holdSegment(exported.regionId());
  if (!lease->block) return false;
  Control c{Type::Announce, exported.regionId()};
  std::memcpy(c.data.data(), s.ubva.eid.raw, 16);
  putInteger(c.data.data() + 16, s.ubva.uasid, 4);
  putInteger(c.data.data() + 20, s.ubva.va, 8);
  putInteger(c.data.data() + 28, s.len, 8);
  putInteger(c.data.data() + 36, s.token_id, 4);
  local_.emplace(c.id, Local{std::move(lease)});
  try { pending_.push_back(c); }
  catch (...) { local_.erase(c.id); throw; }
  return false;
}
bool SegmentState::announced(uint64_t id, bool value) {
  auto it = local_.find(id);
  return it == local_.end() ? false : std::exchange(it->second.lease->announced, value);
}
std::shared_ptr<ImportedSegment> SegmentState::find(uint64_t address, size_t n) const {
  auto it = remote_.upper_bound(address);
  if (it == remote_.begin()) return {};
  --it;
  return containsRange(it->second.range, address, n) ? it->second.imported : nullptr;
}
void SegmentState::receive(Type type, uint64_t id, const uint8_t* data,
    size_t size, const urma_seg_t& initialPeer, Import import) {
  require(id && (type == Type::Announce ? size == 40 : size == 0),
          "invalid URMA segment control");
  if (type == Type::Ready || type == Type::Revoked) {
    auto it = local_.find(id);
    if (it == local_.end() && (closing_ || revokedLocal_.count(id))) return;
    require(it != local_.end(), "unexpected URMA segment acknowledgement");
    if (type == Type::Ready) {
      if (!it->second.revoking) it->second.ready = true;
    } else {
      it->second.lease->released = true;
      remember(revokedLocal_, id);
      local_.erase(it);
    }
    return;
  }
  if (type == Type::Revoke) {
    auto it = std::find_if(remote_.begin(), remote_.end(),
                          [&](const auto& e) { return e.second.id == id; });
    if (it == remote_.end()) {
      if (closing_ && !revokedRemote_.count(id)) return;
      require(revokedRemote_.count(id), "URMA revoke for unknown segment");
      queue(Type::Revoked, id);
    } else if (!it->second.released) {
      it->second.released = it->second.imported->released;
      it->second.imported.reset();
    }
    progress();
    return;
  }
  require(type == Type::Announce && !closing_, "invalid URMA segment announcement");
  urma_seg_t s{};
  std::memcpy(s.ubva.eid.raw, data, 16);
  s.ubva.uasid = getInteger(data + 16, 4);
  s.ubva.va = getInteger(data + 20, 8);
  s.len = getInteger(data + 28, 8);
  s.token_id = getInteger(data + 36, 4);
  s.attr.bs.access = URMA_ACCESS_READ;
  require(s.ubva.va && s.len && s.len <= UINT64_MAX - s.ubva.va &&
              !std::memcmp(s.ubva.eid.raw, initialPeer.ubva.eid.raw, 16) &&
              s.ubva.uasid == initialPeer.ubva.uasid,
          "invalid URMA segment range or peer");
  auto it = remote_.find(s.ubva.va);
  if (it != remote_.end()) {
    require(!it->second.released && it->second.id == id &&
                it->second.range.len == s.len && it->second.range.token_id == s.token_id,
            "conflicting URMA segment announcement");
  } else {
    auto overlap = [&](const urma_seg_t& other) {
      return s.ubva.va < other.ubva.va + other.len && other.ubva.va < s.ubva.va + s.len;
    };
    require(!revokedRemote_.count(id) && remote_.size() < 1024 && !overlap(initialPeer),
            "URMA segment range overlap or limit");
    for (const auto& e : remote_)
      require(e.second.id != id && !overlap(e.second.range), "URMA segment identity/range reused");
    auto imported = import(s);
    require(bool(imported), "URMA announced segment import failed");
    remote_.emplace(s.ubva.va, Remote{id, s, std::move(imported), {}});
  }
  queue(Type::Ready, id);
}
void SegmentState::revoke(const std::vector<uint64_t>& ids) {
  for (auto id : ids) {
    auto it = local_.find(id);
    if (it == local_.end() || it->second.revoking) continue;
    queue(Type::Revoke, id);
    it->second.revoking = true;
    it->second.since = Clock::now();
  }
}
void SegmentState::progress() {
  for (auto it = remote_.begin(); it != remote_.end();) {
    if (!it->second.released || !it->second.released->load(std::memory_order_acquire)) {
      ++it;
      continue;
    }
    queue(Type::Revoked, it->second.id);
    remember(revokedRemote_, it->second.id);
    it = remote_.erase(it);
  }
}
void SegmentState::close(bool sourcesDrained) {
  if (!closing_) {
    closing_ = true;
    pending_.erase(std::remove_if(pending_.begin(), pending_.end(),
        [](const Control& c) { return c.type != Type::Revoked; }), pending_.end());
    for (auto it = local_.begin(); it != local_.end();) {
      if (!it->second.lease->announced) it = local_.erase(it);
      else { it->second.revoking = false; ++it; }
    }
    for (auto& e : remote_) {
      if (!e.second.released) e.second.released = e.second.imported->released;
      e.second.imported.reset();
    }
  }
  if (sourcesDrained) {
    std::vector<uint64_t> ids;
    for (const auto& e : local_) if (!e.second.revoking) ids.push_back(e.first);
    revoke(ids);
  }
  progress();
}
bool SegmentState::waiting() const {
  if (!pending_.empty()) return true;
  for (const auto& e : local_) if (!e.second.ready || e.second.revoking) return true;
  for (const auto& e : remote_) if (e.second.released) return true;
  return false;
}
SegmentState::Clock::time_point SegmentState::deadline() const {
  auto result = Clock::time_point::max();
  for (const auto& e : local_)
    if (!e.second.ready || e.second.revoking)
      result = std::min(result, e.second.since + std::chrono::seconds(30));
  return result;
}
} // namespace folly::urma_detail
#endif
