/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <urma_api.h>
#include <cstdint>
#include <memory>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaProtocol.h>
#include <folly/io/async/urma/UrmaSegmentState.h>

namespace folly {
class EventBase;
// One instance per EventBase, independent of the number of logical sockets.
// All methods except counters() must run on that EventBase's thread.
class UrmaIoContext {
 public:
  struct Listener {
    virtual ~Listener() = default;
    // False retains the completed buffer; its JFR slot is replenished.
    virtual bool receiveReady(const urma_detail::PacketHeader&, const urma_cr_t&,
                              const uint8_t*, size_t) { return true; }
    virtual void receive(
        const urma_detail::PacketHeader&,
        const urma_cr_t&,
        std::unique_ptr<IOBuf>) = 0;
    virtual void completed(const urma_cr_t&, bool read) = 0;
    virtual void writable() = 0;
    virtual void revokeSegments(const std::vector<uint64_t>&) {}
    virtual void failed(const char*) = 0;
  };
  // Cold close path. No application callbacks; accepted WRs and late protocol
  // obligations can outlive the socket, but never the owner's DMA barrier.
  struct Retired : Listener {
    // Transfer a closing socket's WR. Known-unsubmitted READs are cancelled;
    // return true to receive the terminal CQE of an accepted WR instead.
    virtual bool adopt(uint64_t user, bool read, bool submitted) = 0;
    // Validated, deferred packets never reached the socket's record queue.
    virtual void discardReceive(const urma_detail::PacketHeader&) {}
    virtual bool progress() = 0; // true once all obligations are discharged
  };
  struct Counters {
    uint64_t createAttempts, created, destroyed, live, peak;
  };
  struct Snapshot {
    uint32_t jfs, jfr, rxDepth, txDepth, rxTarget, posted;
    size_t connections, imports, inflight, waiting;
    uint64_t sends, receives, reads, completions;
    uint64_t readConnections, dataConnections;
    size_t retired;
  };
  // Import handles have context and WR lifetimes, not socket lifetimes.
  struct Peer;
  using ImportedSegment = urma_detail::ImportedSegment;
  using ReadBuffer = UrmaInit::Buffer;
  static UrmaIoContext& get(EventBase*);
  // Nonblocking preparation: false means transient memory/RQ pressure.
  bool prepare();
  // Shutdown only: no attached sockets and no owner callback on the stack.
  static void release(EventBase*);
  static Counters counters();
  ~UrmaIoContext();
  static uint64_t allocateSocketId();
  uint64_t attach(Listener*, uint64_t id = 0);
  void detach(uint64_t);
  // Closed connections may still owe a remote READ source. Only a matching
  // terminal notification can discharge that obligation; a timer cannot.
  void retire(uint64_t, std::shared_ptr<Retired>);
  // Available only after prepare() succeeds.
  const urma_jfr_id_t& endpoint() const;
  std::shared_ptr<Peer> importPeer(const urma_jfr_id_t&);
  static urma_target_jetty_t* handle(const std::shared_ptr<Peer>&);
  // The process initial segment, already registered by UrmaInit::init().
  // Buffers retain it through local DMA, remote FREE, and application IOBuf
  // lifetimes.
  const urma_seg_t* initialReadSegment();
  ReadBuffer takeReadBuffer(size_t);
  // Bounds active READs and unsent/inflight FREE responsibilities. PULLs
  // waiting for admission remain in the connection's bounded record queue.
  std::shared_ptr<void> retainRead();
  std::shared_ptr<void> retainReadSource();
  std::shared_ptr<ImportedSegment> importReadSegment(const urma_seg_t&);
  static urma_target_seg_t* handle(const std::shared_ptr<ImportedSegment>&);
  // Reserve before writing the DMA buffer. A slot is one WR, also for READ.
  static constexpr uint32_t kNoSlot = UINT32_MAX;
  uint32_t reserve(uint64_t, bool freeNotification = false);
  // Retry on owner progress, with a timer for resources returned by another
  // thread/owner. Registration is deduplicated and detach cancels the wait.
  void waitForResources(uint64_t);
  // Allocated only for this WR; null means retry after pool/owner progress.
  uint8_t* buffer(uint32_t, size_t bytes, bool control = false);
  urma_target_seg_t* segment(uint32_t) const;
  void cancel(uint32_t);
  // Build a SEND directly in its reserved slot. The returned WR has room for
  // max_send_sge source entries and remains owned by the slot until completion.
  urma_jfs_wr_t& prepareSend(uint32_t);
  // READs admitted during CQ dispatch are chained at the next poll boundary,
  // with one completion and one slot per WR. Success transfers ownership;
  // the owner retries a known-unaccepted suffix without duplicating its prefix.
  // Skip the owner poll timer only when the caller provides its own watchdog.
  urma_status_t post(uint32_t, uint64_t, urma_jfs_wr_t&, std::shared_ptr<void>,
                     bool schedulePoll = true);
  void poll();
  // During a CQ event, deliver its ready prefix after polling, before rearm.
  // The callback's existing intrusive hook also cancels delivery on close.
  void scheduleRead(EventBase::LoopCallback*);
  Snapshot snapshot() const;
  bool hasDeferredReceives(uint64_t socket) const;

 private:
  explicit UrmaIoContext(EventBase*);
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace folly
#endif
