/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/Function.h>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaMemoryPool.h>
#include <array>
#include <deque>
#include <map>
#include <set>

namespace folly::urma_detail {
struct ImportedSegment final : CleanupTask {
  urma_context_t* context{nullptr};
  urma_target_seg_t* segment{nullptr};
  std::shared_ptr<std::atomic<bool>> released{new std::atomic<bool>(false)};
  static std::shared_ptr<ImportedSegment> create(const urma_seg_t&);
  bool tryCleanup() noexcept override;
};

// Connection-local segment protocol; queue ownership belongs to the transport.
class SegmentState {
 public:
  using Clock = std::chrono::steady_clock;
  enum class Type : uint8_t { Announce = 3, Ready, Revoke, Revoked };
  struct Control {
    Type type;
    uint64_t id;
    std::array<uint8_t, 40> data{};
    size_t size() const { return type == Type::Announce ? data.size() : 0; }
  };
  using Import = FunctionRef<std::shared_ptr<ImportedSegment>(const urma_seg_t&)>;
  bool prepare(const UrmaPoolBuffer&, uint64_t initialAddress);
  std::shared_ptr<ImportedSegment> find(uint64_t address, size_t length) const;
  void receive(Type, uint64_t, const uint8_t*, size_t,
               const urma_seg_t& initialPeer, Import);
  void revoke(const std::vector<uint64_t>&);
  void progress();
  void close(bool sourcesDrained);
  bool empty() const { return local_.empty() && remote_.empty() && pending_.empty(); }
  bool waiting() const;
  Clock::time_point deadline() const;
  const Control* pending() const { return pending_.empty() ? nullptr : &pending_.front(); }
  void pop() { pending_.pop_front(); }
  void queue(Type type, uint64_t id) { pending_.push_back({type, id}); }
  // Mark before post; restore only when the SDK proves non-acceptance.
  bool announced(uint64_t id, bool value);

 private:
  struct Lease : CleanupTask {
    UrmaPoolBuffer block;
    bool announced{false}, released{false};
    bool tryCleanup() noexcept override;
  };
  struct Local {
    std::shared_ptr<Lease> lease;
    bool ready{false}, revoking{false};
    Clock::time_point since{Clock::now()};
  };
  struct Remote {
    uint64_t id;
    urma_seg_t range;
    std::shared_ptr<ImportedSegment> imported;
    std::shared_ptr<std::atomic<bool>> released;
  };
  std::map<uint64_t, Local> local_;
  std::map<uint64_t, Remote> remote_;
  std::set<uint64_t> revokedLocal_, revokedRemote_;
  std::deque<Control> pending_;
  bool closing_{false};
};
} // namespace folly::urma_detail
#endif
