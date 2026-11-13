/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/ScopeGuard.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaIoContext.h>
#include <folly/io/async/urma/UrmaSegmentState.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <stdexcept>
#include <utility>
namespace folly {
namespace {
using Clock = std::chrono::steady_clock;
constexpr size_t kHelloSize = urma_detail::kHelloSize;
constexpr uint32_t kHeadroom = 128;
constexpr uint32_t kReady = 1;
constexpr auto kReadTimeout = std::chrono::seconds(30);
constexpr auto kRnrRetryDelay = std::chrono::milliseconds(1);
constexpr auto kRnrTimeout = std::chrono::seconds(5);
using urma_detail::store32;
using urma_detail::load32;
bool supportedReadSegmentAttr(urma_seg_attr_t attr) {
  // The fixed READ descriptor carries no extension data or alternate address.
  // Reconstruct only supported attributes using fields shared by the SDKs.
  // In older SDKs has_user_info is part of reserved; comparing the whole value
  // rejects it (and any future extension) without naming an optional field.
  urma_seg_attr_t supported{};
  supported.bs.access = URMA_ACCESS_READ;
  supported.bs.token_policy = URMA_TOKEN_NONE;
  supported.bs.cacheable = attr.bs.cacheable;
  supported.bs.non_pin = attr.bs.non_pin;
  supported.bs.user_token_id = attr.bs.user_token_id;
  return attr.value == supported.value;
}
}  // namespace

struct UrmaAsyncSocket::Impl final : AsyncSocket::ConnectCallback,
                                     AsyncTransport::ReadCallback,
                                     AsyncTransport::WriteCallback,
                                     UrmaIoContext::Listener,
                                     AsyncTimeout {
  enum class State { Connecting, Handshake, Open, Closed, Error };
  using SendKind = urma_detail::PacketKind;
  using RegisteredBuffer = UrmaIoContext::ReadBuffer;
  using Segments = urma_detail::SegmentState;
  struct Resources : urma_detail::CleanupTask {
    urma_context_t* ctx{nullptr};
    std::shared_ptr<UrmaIoContext::Peer> peer;
    RegisteredBuffer source, destination;
    std::shared_ptr<void> readBudget, sourceResponsibility;
    std::shared_ptr<UrmaIoContext::ImportedSegment> imported;
    std::shared_ptr<void> parent;
    UrmaPoolBuffer exported;
    std::atomic<bool> sourceExposed{false};
    void releaseExposure() {
      exported = {};
      sourceResponsibility.reset();
      sourceExposed.store(false, std::memory_order_release);
    }
    bool tryCleanup() noexcept override {
      // Every accepted local WR retains this object through its terminal CQE
      // or successful shared-JFS destruction. Socket closure alone cannot
      // release a READ target or its imported source segment.
      if (sourceExposed.load(std::memory_order_acquire))
        return failed("remote_READ_unconfirmed", URMA_EAGAIN);
      releaseExposure();
      imported.reset();
      source = {};
      destination = {};
      readBudget.reset();
      peer.reset();
      parent.reset();
      if (ctx) {
        ctx = nullptr;
        UrmaInit::releaseContext();
      }
      return true;
    }
  };
  struct Range {
    size_t remaining;
    bool read;
  };
  struct Write {
    WriteCallback* callback;
    std::unique_ptr<IOBuf> data;
    std::vector<size_t> recordLengths;
    std::deque<Range> ranges{};
    bool planned{false};
    size_t completed{0};
    size_t submitted{0}, bytes{0}, capacity{0};
    std::map<size_t, size_t> finished;
  };
  struct Send : std::enable_shared_from_this<Send> {
    SendKind kind{SendKind::Data};
    uint64_t sequence{0};
    size_t offset{0}, bytes{0};
    std::shared_ptr<Write> write;
    std::shared_ptr<Resources> resources;
    std::shared_ptr<void> budget;
    std::array<UrmaPoolBuffer, 3> pins;
    std::array<uint8_t, urma_detail::kSegmentSize> control{};
    bool localDone{false}, freed{false};
    bool accepted{false};
    Clock::time_point since{Clock::now()}, rnrSince{}, retryAt{};
  };
  struct Read {
    uint64_t sequence;
    size_t bytes;
    std::shared_ptr<Resources> resources;
  };
  UrmaAsyncSocket* owner;
  EventBase* evb;
  UrmaConfig config;
  AsyncSocket::UniquePtr tcp;
  std::shared_ptr<Resources> res;
  UrmaIoContext* io{nullptr};
  uint64_t socketId{0}, peerSocketId{0}, sendSequence{0}, receiveSequence{0};
  uint64_t sendCompletions{0}, receiveCompletions{0}, readCompletions{0};
  uint64_t directReadBytes{0}, copiedReadBytes{0}, directSendBytes{0},
      copiedSendBytes{0};
  uint64_t maxReadsInFlight{0};
  State state{State::Connecting};
  AsyncTransport::ReadCallback* reader{nullptr};
  std::array<uint8_t, 128> tcpBuffer{};
  std::vector<uint8_t> handshake;
  unsigned handshakeWrites{0};
  bool haveHello{false}, haveReady{false}, peerRead{false}, client{false};
  urma_detail::Hello peerHello;
  bool sending{false}, delivering{false};
  bool writeShutdown{false}, closeAfterWrites{false};
  size_t txActive{0}, readActive{0};
  bool peerEof{false}, endSent{false}, tcpWriteClosed{false};
  uint64_t receiveEnd{0};
  Clock::time_point eofSince{};
  uint32_t peerBuffer{0}, sendTimeout{0};
  uint32_t timerInterval{0};
  std::map<uint64_t, std::shared_ptr<Send>> sources;
  std::deque<std::shared_ptr<Send>> retries;
  Segments segments;
  // A closed socket cannot receive application callbacks, but its owner can
  // still finish FREE/revocation controls. Abandonment retains unconfirmed
  // source pins and segment leases through the existing cleanup quarantine.
  struct Retirement final : UrmaIoContext::Retired {
    UrmaIoContext* io;
    uint64_t socket, destination;
    urma_jetty_id_t peerId;
    std::shared_ptr<Resources> res;
    std::map<uint64_t, std::shared_ptr<Resources>> sources;
    Segments segments;
    std::deque<std::shared_ptr<Send>> pending;
    size_t inflight{0};
    bool broken{false}, warned{false};
    Clock::time_point since{Clock::now()};
    Retirement(UrmaIoContext* i, uint64_t s, uint64_t d,
               urma_jetty_id_t p, std::shared_ptr<Resources> r)
        : io(i), socket(s), destination(d), peerId(p), res(std::move(r)) {}
    void queue(SendKind kind, uint64_t id, std::shared_ptr<void> budget = {}) {
      auto work = std::make_shared<Send>();
      work->kind = kind;
      work->sequence = id;
      work->resources = res;
      work->budget = std::move(budget);
      pending.push_back(std::move(work));
    }
    bool adopt(uint64_t user, bool read, bool submitted) override {
      if (read && !submitted) {
        auto& work = *reinterpret_cast<Read*>(user);
        // No DMA was accepted, and closing cancels this READ permanently.
        queue(SendKind::Free, work.sequence, std::move(work.resources->readBudget));
        return false;
      }
      if (!read) {
        auto kind = reinterpret_cast<Send*>(user)->kind;
        if (kind != SendKind::Free && kind != SendKind::SegmentRevoke &&
            kind != SendKind::SegmentRevoked) return false;
      }
      ++inflight;
      return true;
    }
    void writable() override {}
    void failed(const char*) override { broken = true; }
    void discardReceive(const urma_detail::PacketHeader& h) override {
      // No READ was submitted for this validated deferred PULL.
      if (h.kind == SendKind::Pull) queue(SendKind::Free, h.sequence);
    }
    void receive(const urma_detail::PacketHeader& h, const urma_cr_t& c,
                 std::unique_ptr<IOBuf> data) override {
      if (c.opcode != URMA_CR_OPC_SEND_WITH_IMM || data->length() ||
          std::memcmp(c.remote_id.eid.raw, peerId.eid.raw, 16)) return;
      if (h.kind == SendKind::Free) {
        auto it = sources.find(h.sequence);
        if (it != sources.end()) {
          it->second->releaseExposure();
          sources.erase(it);
        }
      } else if (h.kind == SendKind::SegmentRevoke ||
                 h.kind == SendKind::SegmentRevoked || h.kind == SendKind::SegmentReady) {
        segments.receive(Segments::Type(h.kind), h.sequence, nullptr, 0, {},
                         urma_detail::ImportedSegment::create);
      }
    }
    void completed(const urma_cr_t& c, bool read) override {
      --inflight;
      if (read && c.status == URMA_CR_SUCCESS) {
        auto& work = *reinterpret_cast<Read*>(c.user_ctx);
        queue(SendKind::Free, work.sequence, std::move(work.resources->readBudget));
      } else if (!read && c.status == URMA_CR_RNR_RETRY_CNT_EXC_ERR) {
        auto work = reinterpret_cast<Send*>(c.user_ctx)->shared_from_this();
        work->retryAt = Clock::now() + kRnrRetryDelay;
        pending.push_back(std::move(work));
      } else if (c.status != URMA_CR_SUCCESS) {
        XLOG(ERR) << "URMA retired completion failed: " << c.status;
        broken = true;
      }
    }
    bool progress() override {
      if (broken) return !inflight;
      if (!warned && Clock::now() - since >= kReadTimeout) {
        XLOG(ERR) << "URMA closed connection still awaiting FREE/segment release";
        warned = true; // keep routing late acknowledgements, never force release
      }
      segments.close(sources.empty());
      while (const auto* control = segments.pending()) {
        queue(SendKind(control->type), control->id);
        segments.pop();
      }
      while (!pending.empty() && !broken) {
        auto work = pending.front();
        if (Clock::now() < work->retryAt) break;
        auto slot = io->reserve(socket, true);
        if (slot == UrmaIoContext::kNoSlot) break;
        auto* packet = io->buffer(slot, urma_detail::kHeaderSize, true);
        if (!packet) {
          io->cancel(slot);
          io->waitForResources(socket);
          break;
        }
        urma_detail::encodeHeader(packet, {work->kind, destination, work->sequence});
        urma_sge_t sge{};
        sge.addr = reinterpret_cast<uintptr_t>(packet);
        sge.len = urma_detail::kHeaderSize;
        sge.tseg = io->segment(slot);
        urma_jfs_wr_t wr{};
        wr.opcode = URMA_OPC_SEND_IMM;
        wr.tjetty = UrmaIoContext::handle(res->peer);
        wr.flag.bs.place_order = URMA_STRONG_ORDER;
        wr.flag.bs.comp_order = wr.flag.bs.complete_enable = 1;
        wr.send.src.sge = &sge;
        wr.send.src.num_sge = 1;
        wr.user_ctx = reinterpret_cast<uintptr_t>(work.get());
        auto st = io->post(slot, socket, wr, work);
        if (st == URMA_EAGAIN) break;
        if (st != URMA_SUCCESS) { broken = true; break; }
        ++inflight;
        pending.pop_front();
      }
      return sources.empty() && segments.empty() &&
          pending.empty() && !inflight;
    }
  };
  bool resourcesReady{false};
  uint64_t admittedSequence{0};
  bool sourceWaiting{false};
  Clock::time_point outgoingSince{};
  Clock::time_point freeSince{};
  struct FreeNotification {
    uint64_t sequence;
    std::shared_ptr<void> budget;
  };
  std::deque<FreeNotification> pendingFree;
  Clock::time_point started{Clock::now()};
  Clock::time_point lastWriteProgress{};
  std::deque<std::shared_ptr<Write>> writes;
  IOBufQueue reads{IOBufQueue::cacheChainLength()};
  struct Record {
    SendKind kind;
    uint64_t address;
    size_t length;
    std::unique_ptr<IOBuf> data;
    bool active{false}, ready{false};
  };
  std::map<uint64_t, Record> records;
  size_t recordBytes{0};
  Clock::time_point recordSince{};
  size_t pendingBytes{0}, bytesWritten{0}, bytesRead{0};
  size_t queuedCapacity{0}, reportedBytes{0}, reportedCapacity{0};
  AsyncTransport::BufferCallback* bufferCallback{nullptr};
  bool notifyingBuffer{false};
  urma_jetty_id_t peerId{};

  Impl(UrmaAsyncSocket* o, EventBase* e)
      : AsyncTimeout(e),
        owner(o),
        evb(e),
        config(UrmaInit::getConfig()),
        socketId(UrmaIoContext::allocateSocketId()) {}
  ~Impl() override {
    cancelTimeout();
    cleanup();
    if (std::getenv("FOLLY_URMA_LOG_STATS"))
      std::fprintf(
          stderr,
          "URMA_COPY_STATS direct_read_source_bytes=%llu copied_read_source_bytes=%llu "
          "direct_send_bytes=%llu copied_send_bytes=%llu max_read_inflight=%llu\n",
          static_cast<unsigned long long>(directReadBytes),
          static_cast<unsigned long long>(copiedReadBytes),
          static_cast<unsigned long long>(directSendBytes),
          static_cast<unsigned long long>(copiedSendBytes),
          static_cast<unsigned long long>(maxReadsInFlight));
  }
  bool live() const { return state != State::Closed && state != State::Error; }
  size_t allocatedBytesBuffered() const {
    // Retained write storage plus the active READ copy; idle registered caches
    // and receive buffers are not pending egress data.
    size_t source = 0;
    for (const auto& entry : sources)
      if (entry.second->resources->source.hold)
        source += entry.second->resources->source.size;
    return queuedCapacity +
        std::min(source, std::numeric_limits<size_t>::max() - queuedCapacity);
  }
  void notifyBuffer() {
    if (notifyingBuffer)
      return;
    notifyingBuffer = true;
    // Callbacks may enqueue, close, unregister, or destroy the socket. Publish
    // the snapshot before calling them, then recheck the resulting state.
    while (bufferCallback && !owner->getDestroyPending()) {
      const auto capacity = allocatedBytesBuffered();
      if (reportedBytes == pendingBytes && reportedCapacity == capacity)
        break;
      reportedBytes = pendingBytes;
      reportedCapacity = capacity;
      auto* cb = bufferCallback;
      if (pendingBytes || capacity)
        cb->onEgressBuffered();
      else
        cb->onEgressBufferCleared();
    }
    notifyingBuffer = false;
  }
  void armTimer() {
    if (live() &&
        (state != State::Open || !writes.empty() || readActive ||
         !pendingFree.empty() || !records.empty() || !retries.empty() ||
         txActive || segments.waiting() ||
         (peerEof && receiveEnd && receiveSequence != receiveEnd - 1))) {
      const uint32_t interval = !retries.empty() ? 1 : 10;
      // Frequent owner wakeups must not postpone deadline checks forever.
      if (!isScheduled() || interval < timerInterval) {
        scheduleTimeout(interval);
        timerInterval = interval;
      }
    } else
      cancelTimeout();
  }
  bool createResources() {
    try {
      if (!io) {
        io = &UrmaIoContext::get(evb);
        res = std::shared_ptr<Resources>(new Resources, [](Resources* value) {
          urma_detail::cleanupOrDefer(std::unique_ptr<Resources>(value));
        });
        res->ctx = UrmaInit::acquireContext();
        io->attach(this, socketId);
      }
      return res->ctx && io->prepare();
    } catch (const std::exception& e) {
      fail(std::string("URMA owner setup: ") + e.what());
      return false;
    }
  }
  void start() {
    DestructorGuard guard(owner);
    state = State::Handshake;
    started = Clock::now();
    prepareHandshake();
    armTimer();
  }
  void prepareHandshake() {
    if (resourcesReady || !live() || !createResources()) return;
    resourcesReady = true;
    tcp->setReadCB(this);
    if (client) sendHello();
  }
  void sendHello() {
    urma_detail::Hello hello;
    const auto& id = io->endpoint();
    hello.bufferSize = config.buffer_size;
    hello.posted = io->snapshot().posted;
    hello.jfr = id.id;
    std::memcpy(hello.eid.data(), id.eid.raw, 16);
    hello.uasid = id.uasid;
    hello.tpType = URMA_CTP;
    hello.socket = socketId;
    if (config.enable_read && UrmaInit::getMaxReadSize()) {
      const auto* segment = io->initialReadSegment();
      if (!segment || !supportedReadSegmentAttr(segment->attr)) {
        fail("URMA initial READ segment setup failed");
        return;
      }
      hello.read = true;
      hello.maxRead =
          std::min(config.max_pending_bytes, UrmaInit::getMaxReadSize());
      std::memcpy(hello.segmentEid.data(), segment->ubva.eid.raw, 16);
      hello.segmentUasid = segment->ubva.uasid;
      hello.segmentAddress = segment->ubva.va;
      hello.segmentLength = segment->len;
      hello.segmentToken = segment->token_id;
    }
    std::array<uint8_t, kHelloSize> h{};
    urma_detail::encodeHello(h.data(), hello);
    sendHandshake(h.data(), h.size());
  }
  static std::unique_ptr<IOBuf> copyHandshake(const void* p, size_t n) {
    // TCP control storage is independent of registered DMA capacity, so pool
    // pressure cannot prevent HELLO or the final stream boundary from draining.
    auto bytes = std::make_unique<uint8_t[]>(n);
    std::memcpy(bytes.get(), p, n);
    return IOBuf::takeOwnership(bytes.release(), n,
        [](void* data, void*) { delete[] static_cast<uint8_t*>(data); });
  }
  void sendHandshake(const void* p, size_t n) {
    ++handshakeWrites;
    tcp->writeChain(this, copyHandshake(p, n));
  }
  void connectSuccess() noexcept override { start(); }
  void connectErr(const AsyncSocketException& e) noexcept override {
    fail(e.what());
  }
  void writeSuccess() noexcept override {
    DestructorGuard guard(owner);
    if (handshakeWrites)
      --handshakeWrites;
    if (state == State::Handshake) becomeReady();
    else if (state == State::Open) flush();
  }
  void writeErr(size_t, const AsyncSocketException& e) noexcept override {
    fail(e.what());
  }
  void getReadBuffer(void** p, size_t* n) override {
    *p = tcpBuffer.data();
    *n = tcpBuffer.size();
  }
  void readDataAvailable(size_t n) noexcept override {
    DestructorGuard guard(owner);
    if (!live())
      return;
    if (handshake.size() + n > kHelloSize + 4 + urma_detail::kEndSize) {
      fail("unexpected data on URMA handshake channel");
      return;
    }
    handshake.insert(handshake.end(), tcpBuffer.begin(), tcpBuffer.begin() + n);
    if (!haveHello && handshake.size() >= 10 &&
        (load32(handshake.data()) != urma_detail::kMagic ||
         urma_detail::load16(handshake.data() + 4) != kHelloSize ||
         urma_detail::load16(handshake.data() + 6) != urma_detail::kHelloVersion ||
         urma_detail::load16(handshake.data() + 8) != urma_detail::kWireVersion)) {
      fail("incompatible URMA handshake version");
      return;
    }
    if (!haveHello && handshake.size() >= kHelloSize) {
      if (!urma_detail::decodeHello(handshake.data(), kHelloSize, peerHello) ||
          peerHello.tpType != URMA_CTP || peerHello.fcJfr ||
          peerHello.bufferSize <= kHeadroom || peerHello.bufferSize > 4096 ||
          !peerHello.jfr || !peerHello.posted) {
        fail("incompatible URMA HELLO102 or flow-control mode");
        return;
      }
      std::memcpy(peerId.eid.raw, peerHello.eid.data(), 16);
      peerId.uasid = peerHello.uasid;
      peerId.id = peerHello.jfr;
      peerBuffer = std::min(config.buffer_size, peerHello.bufferSize);
      peerRead =
          peerHello.read && config.enable_read && UrmaInit::getMaxReadSize();
      peerSocketId = peerHello.socket;
      if (!client) {
        // Validate the peer before publishing our routing identity, but send
        // HELLO before import: bonding may wait for the peer's import too.
        sendHello();
        if (!live())
          return;
      }
      res->peer = io->importPeer(peerId);
      if (!res->peer) {
        fail("URMA peer import failed errno=" + std::to_string(errno));
        return;
      }
      if (peerRead) {
        urma_seg_t segment{};
        std::memcpy(segment.ubva.eid.raw, peerHello.segmentEid.data(), 16);
        segment.ubva.uasid = peerHello.segmentUasid;
        segment.ubva.va = peerHello.segmentAddress;
        segment.len = peerHello.segmentLength;
        segment.token_id = peerHello.segmentToken;
        segment.attr.bs.access = URMA_ACCESS_READ;
        res->imported = io->importReadSegment(segment);
        if (!res->imported) {
          fail("URMA initial READ segment import failed");
          return;
        }
      }
      haveHello = true;
      handshake.erase(handshake.begin(), handshake.begin() + kHelloSize);
      if (client) {
        uint8_t ready[4];
        store32(ready, kReady);
        // Mark before writeChain: synchronous completion may call becomeReady.
        haveReady = true;
        sendHandshake(ready, sizeof(ready));
      }
      if (!live())
        return;
    }
    if (!client && haveHello && !haveReady && handshake.size() >= 4) {
      if (load32(handshake.data()) != kReady) {
        fail("invalid URMA ready message");
        return;
      }
      handshake.erase(handshake.begin(), handshake.begin() + 4);
      haveReady = true;
      becomeReady();
    }
    if (!live() || !haveReady || handshake.empty()) return;
    if (receiveEnd || handshake.size() > urma_detail::kEndSize) {
      fail("unexpected data after URMA stream boundary");
      return;
    }
    if (handshake.size() == urma_detail::kEndSize) {
      uint64_t end = 0;
      if (!urma_detail::decodeEnd(handshake.data(), handshake.size(), end) ||
          end <= receiveSequence ||
          (!records.empty() && records.rbegin()->first >= end)) {
        fail("invalid URMA stream boundary");
        return;
      }
      receiveEnd = end;
      handshake.clear();
    }
  }
  void becomeReady() {
    if (state != State::Handshake || !haveReady || handshakeWrites)
      return;
    state = State::Open;
    armTimer();
    poll();
    drainRecords();
    flush();
    deliver();
  }
  void readEOF() noexcept override {
    DestructorGuard guard(owner);
    if (!live())
      return;
    if (state != State::Open) {
      fail("peer closed during URMA handshake");
      return;
    }
    if (!receiveEnd || !handshake.empty()) {
      fail("URMA peer closed without a complete stream boundary");
      return;
    }
    poll();
    if (!live())
      return;
    // TCP FIN closes only the peer's write direction. Its URMA FREEs for our
    // outgoing writes are still allowed, so do not cancel those writes here.
    peerEof = true;
    eofSince = Clock::now();
    // Deferred legal receives still belong to this stream. Deliver EOF only
    // after they and outstanding READs have drained, including a paused reader.
    deliver();
    armTimer();
  }
  void readErr(const AsyncSocketException& e) noexcept override {
    fail(e.what());
  }
  size_t smallWriteLimit() const {
    // Subtract instead of adding to the payload, so the test cannot overflow.
    return peerBuffer > kHeadroom ? peerBuffer - kHeadroom : 0;
  }
  void planWrite(Write& write) {
    if (write.planned)
      return;
    for (size_t length : write.recordLengths) {
      if (!length)
        continue;
      const bool read = peerRead &&
          UrmaInit::getMaxReadSize() && length > config.read_threshold &&
          length <= peerHello.maxRead;
      // Decide per record first. Adjacent eligible READ records can then share
      // one PULL, without pulling a small record into a READ or crossing the
      // peer's holding limit. This never waits for another independent write.
      if (!write.ranges.empty() && write.ranges.back().read == read &&
          (!read || length <= peerHello.maxRead - write.ranges.back().remaining))
        write.ranges.back().remaining += length;
      else
        write.ranges.push_back({length, read});
    }
    write.recordLengths.clear();
    write.planned = true;
  }
  std::shared_ptr<Resources> operationResources() {
    auto value = std::shared_ptr<Resources>(new Resources, [](Resources* p) {
      if (p->sourceExposed.load(std::memory_order_acquire))
        urma_detail::cleanupOrDefer(std::unique_ptr<Resources>(p));
      else {
        p->releaseExposure();
        delete p; // Only pool slices/shared handles; no SDK teardown here.
      }
    });
    value->parent = res;
    value->peer = res->peer;
    return value;
  }
  void consumeWrite(const std::shared_ptr<Send>& work) {
    auto& write = *work->write;
    if (work->offset != write.completed) {
      write.finished.emplace(work->offset, work->bytes);
      return;
    }
    auto advance = [&](size_t bytes) {
      pendingBytes -= bytes;
      bytesWritten += bytes;
      write.completed += bytes;
      lastWriteProgress = Clock::now();
    };
    // In-order completion needs no allocation or insertion into a map.
    advance(work->bytes);
    // A later CQE/FREE may arrive first. Only a contiguous prefix counts as
    // completed bytes, including the byte count reported by writeErr().
    while (true) {
      auto it = write.finished.find(write.completed);
      if (it == write.finished.end())
        break;
      advance(it->second);
      write.finished.erase(it);
    }
    finishWrite();
  }
  std::shared_ptr<UrmaIoContext::ImportedSegment> findReadSegment(
      uint64_t address, uint32_t length) {
    if (!peerRead || length > std::min(config.max_pending_bytes, UrmaInit::getMaxReadSize()))
      return {};
    auto range = peerHello;
    range.maxRead = std::min(config.max_pending_bytes, UrmaInit::getMaxReadSize());
    if (urma_detail::containsRead(range, address, length)) return res->imported;
    return segments.find(address, length);
  }
  bool announceSegment(const UrmaPoolBuffer& exported) {
    try { return segments.prepare(exported, io->initialReadSegment()->ubva.va); }
    catch (const std::exception& e) { fail(e.what()); return false; }
  }
  void revokeSegments(const std::vector<uint64_t>& ids) override {
    DestructorGuard guard(owner);
    if (!live()) return;
    segments.revoke(ids);
    flush();
    armTimer();
  }
  void receiveSegment(const urma_detail::PacketHeader& h,
                      const urma_cr_t& c, const IOBuf& data) {
    if (!peerRead || (h.kind == SendKind::Segment
        ? c.opcode != URMA_CR_OPC_SEND
        : c.opcode != URMA_CR_OPC_SEND_WITH_IMM)) {
      fail("invalid URMA segment control"); return;
    }
    try {
      segments.receive(Segments::Type(h.kind), h.sequence, data.data(), data.length(),
          UrmaIoContext::handle(res->imported)->seg,
          [&](const urma_seg_t& s) { return io->importReadSegment(s); });
    } catch (const std::exception& e) { fail(e.what()); }
  }
  bool preparePull(const std::shared_ptr<Send>& work) {
    if (!sourceWaiting)
      outgoingSince = Clock::now();
    auto resources = operationResources();
    resources->sourceResponsibility = io->retainReadSource();
    if (!resources->sourceResponsibility) {
      sourceWaiting = true;
      io->waitForResources(socketId);
      return false;
    }
    auto* node = work->write->data.get();
    size_t skip = work->offset;
    while (skip >= node->length()) {
      skip -= node->length();
      node = node->next();
    }
    const size_t directBytes = std::min(work->bytes, node->length() - skip);
    if (UrmaInit::ownsPoolBuffer(*node)) {
      resources->exported =
          UrmaInit::holdReadExport(node->data() + skip, directBytes);
      if (resources->exported) {
        work->bytes = directBytes;
        resources->source = {
            const_cast<uint8_t*>(node->data() + skip),
            resources->exported.segment(),
            directBytes,
            {}};
      }
    }
    if (!resources->source.memory) {
      // Compatibility for external buffers. Normal serialized IOBufs
      // already belong to the registered pool and never take this copy path.
      resources->source =
          UrmaInit::allocateBuffer(work->bytes);
      if (resources->source.memory)
        resources->exported =
            UrmaInit::holdReadExport(resources->source.memory, work->bytes);
      if (!resources->source.memory || !resources->exported) {
        sourceWaiting = true;
        io->waitForResources(socketId);
        return false; // Keep READ selected; never downgrade to SEND on
                      // pressure.
      }
      io::Cursor cursor(work->write->data.get());
      cursor.skip(work->offset);
      cursor.pull(resources->source.memory, work->bytes);
    }
    if (!announceSegment(resources->exported)) {
      if (live()) {
        sourceWaiting = true;
        io->waitForResources(socketId);
      }
      return false;
    }
    sourceWaiting = false;
    work->resources = std::move(resources);
    return true;
  }
  void finishLargeWrite(const std::shared_ptr<Send>& work) {
    if (!work->localDone || !work->freed)
      return;
    sources.erase(work->sequence);
    work->resources->source = {};
    consumeWrite(work);
  }
  void receiveFree(uint64_t sequence) {
    auto it = sources.find(sequence);
    if (it == sources.end() || it->second->freed)
      return;
    auto work = it->second;
    work->freed = true;
    work->resources->releaseExposure();
    finishLargeWrite(work);
  }
  bool postRead(uint64_t sequence, Record& record) {
    if (readActive >= config.read_window)
      return false;
    auto budget = io->retainRead();
    if (!budget) {
      io->waitForResources(socketId);
      return false;
    }
    auto resources = operationResources();
    resources->imported = findReadSegment(record.address, record.length);
    if (!resources->imported) { fail("URMA READ lost imported segment"); return false; }
    resources->destination = io->takeReadBuffer(record.length);
    if (!resources->destination.memory) {
      io->waitForResources(socketId);
      return false;
    }
    resources->readBudget = std::move(budget);
    auto work =
        std::make_shared<Read>(Read{sequence, record.length, resources});
    auto slot = io->reserve(socketId);
    if (slot == UrmaIoContext::kNoSlot)
      return false;
    urma_sge_t source{}, destination{};
    source.addr = record.address;
    source.len = record.length;
    source.tseg = UrmaIoContext::handle(resources->imported);
    destination.addr =
        reinterpret_cast<uint64_t>(resources->destination.memory);
    destination.len = record.length;
    destination.tseg = resources->destination.segment;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_READ;
    wr.tjetty = UrmaIoContext::handle(resources->peer);
    wr.flag.bs.complete_enable = 1;
    wr.rw.src.sge = &source;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &destination;
    wr.rw.dst.num_sge = 1;
    wr.user_ctx = reinterpret_cast<uintptr_t>(work.get());
    // post() may synchronously fail the owner with acceptance unknown. Close
    // must then leave this source to the WR's completion/barrier, not free it.
    record.active = true;
    ++readActive;
    auto status = io->post(slot, socketId, wr, work);
    if (!live()) return false;
    if (status == URMA_EAGAIN) {
      record.active = false;
      --readActive;
      return false;
    }
    if (status != URMA_SUCCESS) {
      fail("URMA READ submit failed: " + std::to_string(status));
      return false;
    }
    maxReadsInFlight = std::max<uint64_t>(maxReadsInFlight, readActive);
    return true;
  }
  void drainRecords() {
    // Submit every contiguous known record without waiting for an earlier
    // READ CQE. Admission remains bounded by the owner's SQ and READ budget.
    uint64_t next = receiveSequence;
    for (auto& entry : records) {
      if (entry.first != ++next)
        break;
      auto& record = entry.second;
      if (record.kind == SendKind::Pull && !record.active && !record.ready &&
          !postRead(entry.first, record))
        break;
    }
    while (state == State::Open && !owner->getDestroyPending()) {
      auto it = records.find(receiveSequence + 1);
      if (it == records.end() || !it->second.ready)
        break;
      auto data = std::move(it->second.data);
      recordBytes -= it->second.length;
      records.erase(it);
      ++receiveSequence;
      recordSince = Clock::now();
      bytesRead += data->length();
      reads.append(std::move(data));
      deliver();
    }
  }
  void completeRead(Read& work) {
    auto it = records.find(work.sequence);
    if (it == records.end() || !it->second.active || it->second.ready) {
      fail("invalid URMA READ completion");
      return;
    }
    --readActive;
    auto& resources = *work.resources;
    auto data =
        UrmaInit::takeBuffer(std::move(resources.destination), work.bytes);
    resources.destination = {};
    // FREE releases the peer's source, not our application-owned destination.
    // It can be sent before an older record is ready for ordered delivery.
    if (pendingFree.empty())
      freeSince = Clock::now();
    pendingFree.push_back({work.sequence, std::move(resources.readBudget)});
    it->second.data = std::move(data);
    it->second.ready = true;
    drainRecords();
  }
  void poll() {
    if (io && live())
      io->poll();
  }
  void failed(const char* why) override { fail(why); }
  void writable() override {
    DestructorGuard guard(owner);
    if (live()) {
      drainRecords();
      flush();
    }
  }
  bool receiveReady(const urma_detail::PacketHeader& h, const urma_cr_t& c,
                    const uint8_t* payload, size_t bytes) override {
    if (!live() || !haveHello ||
        std::memcmp(c.remote_id.eid.raw, peerId.eid.raw, 16) ||
        (h.kind != SendKind::Data && h.kind != SendKind::Pull) ||
        h.sequence <= receiveSequence || records.count(h.sequence) ||
        (receiveEnd && h.sequence >= receiveEnd)) return true;
    size_t length = bytes;
    if (h.kind == SendKind::Pull) {
      if (c.opcode != URMA_CR_OPC_SEND_WITH_IMM || !peerRead || bytes != 16 ||
          load32(payload + 4)) return true;
      length = load32(payload);
      if (!findReadSegment(urma_detail::load64(payload + 8), length)) return true;
    } else if (c.opcode != URMA_CR_OPC_SEND || !length ||
               length > config.buffer_size - urma_detail::kHeaderSize) return true;
    const size_t held = reads.chainLength() + recordBytes;
    // Admit DATA/PULL in wire sequence so later records cannot consume the
    // budget needed by a missing earlier one. READs still execute concurrently
    // and their CQEs are reordered separately by records.
    return h.sequence == admittedSequence + 1 && records.size() < 1024 &&
        held <= config.max_pending_bytes && length <= config.max_pending_bytes - held;
  }
  void receive(
      const urma_detail::PacketHeader& h,
      const urma_cr_t& c,
      std::unique_ptr<IOBuf> data) override {
    DestructorGuard guard(owner);
    if (!live())
      return;
    if (!haveHello || std::memcmp(c.remote_id.eid.raw, peerId.eid.raw, 16)) {
      fail("invalid URMA shared connection routing");
      return;
    }
    ++receiveCompletions;
    using Kind = urma_detail::PacketKind;
    if (h.kind == Kind::Segment || h.kind == Kind::SegmentReady ||
        h.kind == Kind::SegmentRevoke || h.kind == Kind::SegmentRevoked) {
      receiveSegment(h, c, *data);
    } else if (h.kind == Kind::Free) {
      if (c.opcode != URMA_CR_OPC_SEND_WITH_IMM ||
          data->length()) {
        fail("invalid URMA FREE24");
        return;
      }
      receiveFree(h.sequence);
    } else {
      size_t length = data->length();
      if (h.kind == Kind::Pull) {
        if (c.opcode != URMA_CR_OPC_SEND_WITH_IMM || !peerRead || length != 16 ||
            load32(data->data() + 4)) {
          fail("invalid URMA PULL40");
          return;
        }
        length = load32(data->data());
        if (!findReadSegment(urma_detail::load64(data->data() + 8), length)) {
          fail("URMA PULL outside acknowledged segment or READ limit");
          return;
        }
      } else if (
          c.opcode != URMA_CR_OPC_SEND || !length ||
          length > config.buffer_size - urma_detail::kHeaderSize) {
        fail("invalid URMA DATA");
        return;
      }
      if (h.sequence <= receiveSequence || records.count(h.sequence) ||
          (receiveEnd && h.sequence >= receiveEnd)) {
        fail("invalid URMA record sequence");
        return;
      }
      admittedSequence = h.sequence;
      if (records.empty())
        recordSince = Clock::now();
      records.emplace(
          h.sequence,
          Record{
              h.kind,
              h.kind == Kind::Pull ? urma_detail::load64(data->data() + 8) : 0,
              length,
              std::move(data),
              false,
              h.kind == SendKind::Data});
      recordBytes += length;
      drainRecords();
    }
    if (live() && !owner->getDestroyPending())
      flush();
  }
  void completed(const urma_cr_t& c, bool readCompletion) override {
    DestructorGuard guard(owner);
    if (!live())
      return;
    // The owner validates its fixed completion slot and retains the operation
    // through this callback. No token lookup or cross-thread table is needed.
    if (readCompletion) {
      if (c.status != URMA_CR_SUCCESS) {
        fail("URMA READ completion failed: " + std::to_string(c.status));
        return;
      }
      ++readCompletions;
      completeRead(*reinterpret_cast<Read*>(c.user_ctx));
    } else {
      auto work = reinterpret_cast<Send*>(c.user_ctx)->shared_from_this();
      --txActive;
      if (c.status == URMA_CR_RNR_RETRY_CNT_EXC_ERR) {
        if (work->kind == SendKind::Segment)
          segments.announced(work->sequence, false);
        if (work->kind == SendKind::Pull)
          work->resources->sourceExposed.store(false);
        auto now = Clock::now();
        if (work->rnrSince == Clock::time_point{})
          work->rnrSince = now;
        work->retryAt = now + kRnrRetryDelay;
        retries.push_back(std::move(work));
        armTimer();
        return;
      }
      if (c.status != URMA_CR_SUCCESS) {
        fail("URMA completion failed: " + std::to_string(c.status));
        return;
      }
      ++sendCompletions;
      work->localDone = true;
      if (work->kind == SendKind::Pull)
        finishLargeWrite(work);
      else if (work->kind == SendKind::Data)
        consumeWrite(work);
      else
        work->budget.reset();
    }
    if (live() && !owner->getDestroyPending())
      flush();
  }
  void finishWrite() {
    while (!writes.empty() &&
           writes.front()->completed == writes.front()->bytes && live() &&
           !owner->getDestroyPending()) {
      auto write = std::move(writes.front());
      writes.pop_front();
      queuedCapacity -= write->capacity;
      auto* cb = write->callback;
      write->callback = nullptr;
      if (cb)
        cb->writeSuccess();
    }
  }
  bool postSend(const std::shared_ptr<Send>& work) {
    const bool control = work->kind != SendKind::Data && work->kind != SendKind::Pull;
    auto slot = io->reserve(socketId, control);
    if (slot == UrmaIoContext::kNoSlot) return false;
    auto cancelSlot = makeGuard([&] { io->cancel(slot); });
    uint8_t* packet = nullptr;
    const size_t payload = work->kind == SendKind::Pull ? 16 : work->bytes;
    std::array<urma_sge_t, 4> sges{};
    sges[0].len = payload + urma_detail::kHeaderSize;
    uint32_t count = 1;
    work->pins = {};
    bool direct = false;
    if (work->kind == SendKind::Data) {
      const auto* node = work->write->data.get();
      if (!node->isChained() && !node->isSharedOne() && !work->offset &&
          work->bytes == work->write->bytes &&
          node->headroom() >= urma_detail::kHeaderSize) {
        auto* header = const_cast<uint8_t*>(node->data()) - urma_detail::kHeaderSize;
        auto pin = UrmaInit::lookupSegment(header, payload + urma_detail::kHeaderSize);
        if (pin) {
          packet = header;
          sges[0].addr = reinterpret_cast<uint64_t>(packet);
          sges[0].tseg = pin.segment();
          work->pins[0] = std::move(pin);
          direct = true;
        }
      }
      if (!direct) {
        io::Cursor cursor(node);
        cursor.skip(work->offset);
        size_t remaining = payload;
        while (remaining && count < config.max_send_sge) {
          auto bytes = cursor.peekBytes();
          const size_t n = std::min(remaining, bytes.size());
          auto pin = UrmaInit::lookupSegment(bytes.data(), n);
          if (!pin) break;
          work->pins[count - 1] = std::move(pin);
          sges[count].addr = reinterpret_cast<uint64_t>(bytes.data());
          sges[count].len = n;
          sges[count].tseg = work->pins[count - 1].segment();
          ++count;
          cursor.skip(n);
          remaining -= n;
        }
        if (remaining) {
          work->pins = {};
          count = 1;
        } else {
          sges[0].len = urma_detail::kHeaderSize;
          direct = true;
        }
      }
    }
    if (!packet) {
      packet = io->buffer(slot, sges[0].len, control);
      if (!packet) {
        io->waitForResources(socketId);
        return false;
      }
      sges[0].addr = reinterpret_cast<uint64_t>(packet);
      sges[0].tseg = io->segment(slot);
    }
    if (work->kind == SendKind::Pull) {
      store32(packet + urma_detail::kHeaderSize, work->bytes);
      store32(packet + urma_detail::kHeaderSize + 4, 0);
      urma_detail::store64(packet + urma_detail::kHeaderSize + 8,
          reinterpret_cast<uint64_t>(work->resources->source.memory));
    } else if (work->kind == SendKind::Segment) {
      std::memcpy(packet + urma_detail::kHeaderSize, work->control.data(), payload);
    } else if (work->kind == SendKind::Data && !direct) {
      // Preserve the planned coalescing boundary for heap/excess fragments.
      io::Cursor cursor(work->write->data.get());
      cursor.skip(work->offset);
      cursor.pull(packet + urma_detail::kHeaderSize, payload);
    }
    urma_detail::encodeHeader(packet, {work->kind, peerSocketId, work->sequence});
    urma_jfs_wr_t wr{};
    wr.opcode =
        (work->kind == SendKind::Data || work->kind == SendKind::Segment)
            ? URMA_OPC_SEND : URMA_OPC_SEND_IMM;
    wr.tjetty = UrmaIoContext::handle(work->resources->peer);
    wr.flag.bs.place_order = URMA_STRONG_ORDER;
    wr.flag.bs.comp_order = 1;
    wr.flag.bs.complete_enable = 1;
    wr.send.src.sge = sges.data();
    wr.send.src.num_sge = count;
    wr.send.imm_data = 0;
    wr.user_ctx = reinterpret_cast<uintptr_t>(work.get());
    bool announcedBefore = false;
    if (work->kind == SendKind::Segment)
      announcedBefore = segments.announced(work->sequence, true);
    if (work->kind == SendKind::Pull)
      work->resources->sourceExposed.store(true);
    // post() owns the slot once submission starts, including unknown
    // acceptance.
    cancelSlot.dismiss();
    auto status = io->post(slot, socketId, wr, work);
    if (status == URMA_EAGAIN) {
      if (work->kind == SendKind::Segment)
        segments.announced(work->sequence, announcedBefore);
      if (work->kind == SendKind::Pull)
        work->resources->sourceExposed.store(false);
      return false;
    }
    if (status != URMA_SUCCESS) {
      fail("URMA SEND submit failed: " + std::to_string(status));
      return false;
    }
    ++txActive;
    if (!work->accepted) {
      work->accepted = true;
      if (work->kind == SendKind::Pull)
        (work->resources->source.hold ? copiedReadBytes : directReadBytes) +=
            work->bytes;
      else if (work->kind == SendKind::Data)
        (direct ? directSendBytes : copiedSendBytes) += work->bytes;
    }
    return true;
  }
  void flush() {
    if (sending || state != State::Open || owner->getDestroyPending())
      return;
    sending = true;
    segments.progress();
    while (state == State::Open && !owner->getDestroyPending()) {
      if (!pendingFree.empty()) {
        auto work = std::make_shared<Send>();
        work->kind = SendKind::Free;
        work->sequence = pendingFree.front().sequence;
        work->budget = pendingFree.front().budget;
        work->resources = res;
        if (!postSend(work))
          break;
        pendingFree.pop_front();
        freeSince = Clock::now();
        continue;
      }
      if (!retries.empty()) {
        auto work = retries.front();
        if (Clock::now() < work->retryAt || !postSend(work))
          break;
        retries.pop_front();
        continue;
      }
      if (const auto* control = segments.pending()) {
        auto work = std::make_shared<Send>();
        work->kind = SendKind(control->type);
        work->sequence = control->id;
        work->control = control->data;
        work->bytes = control->size();
        work->resources = res;
        if (!postSend(work)) break;
        segments.pop();
        continue;
      }
      auto it =
          std::find_if(writes.begin(), writes.end(), [](const auto& write) {
            return write->submitted < write->bytes;
          });
      if (it == writes.end())
        break;
      auto write = *it;
      planWrite(*write);
      auto work = std::make_shared<Send>();
      work->write = write;
      work->offset = write->submitted;
      work->kind = write->ranges.front().read ? SendKind::Pull : SendKind::Data;
      work->bytes = work->kind == SendKind::Pull
          ? write->ranges.front().remaining
          : std::min(write->ranges.front().remaining, smallWriteLimit());
      if (sendSequence == UINT64_MAX) {
        fail("URMA wire sequence exhausted");
        break;
      }
      work->sequence = sendSequence + 1;
      if (work->kind == SendKind::Pull) {
        if (sources.size() >= config.offer_window)
          break;
        if (!preparePull(work)) {
          if (live() && segments.pending() != nullptr) continue;
          break;
        }
        // Publish before post: unknown acceptance may synchronously close us.
        sources.emplace(work->sequence, work);
      } else
        work->resources = res;
      if (!postSend(work)) {
        if (live())
          sources.erase(
              work->sequence); // Known unaccepted; retry original bytes.
        break;
      }
      ++sendSequence;
      write->submitted += work->bytes;
      write->ranges.front().remaining -= work->bytes;
      if (!write->ranges.front().remaining)
        write->ranges.pop_front();
    }
    if (state == State::Open && !owner->getDestroyPending())
      drainRecords();
    sending = false;
    notifyBuffer();
    if (state == State::Open && writes.empty() && !txActive &&
        retries.empty() && !segments.pending() && pendingFree.empty() && !readActive) {
      if ((closeAfterWrites || writeShutdown) && !endSent) {
        if (sendSequence == UINT64_MAX) {
          fail("URMA stream boundary sequence exhausted");
          return;
        }
        endSent = true; // TCP completion can synchronously reenter flush().
        auto end = urma_detail::encodeEnd(sendSequence + 1);
        sendHandshake(end.data(), end.size());
      }
      if (!live() || handshakeWrites) return;
      if (closeAfterWrites) {
        stop(false);
        return;
      }
      if (writeShutdown && !tcpWriteClosed) {
        tcpWriteClosed = true;
        tcp->shutdownWrite();
      }
    }
    armTimer();
  }
  void deliver() {
    if (delivering || state != State::Open || owner->getDestroyPending())
      return;
    delivering = true;
    while (reader && !reads.empty() && state == State::Open &&
           !owner->getDestroyPending()) {
      auto* cb = reader;
      if (cb->isBufferMovable()) {
        cb->readBufferAvailable(reads.move());
        continue;
      }
      void* p = nullptr;
      size_t length = 0;
      try {
        cb->getReadBuffer(&p, &length);
      } catch (const std::exception& e) {
        fail(e.what());
        break;
      } catch (...) {
        fail("read buffer callback threw");
        break;
      }
      if (state != State::Open || owner->getDestroyPending()) break;
      if (reader != cb) continue;
      if (!p || !length) {
        fail("empty application read buffer");
        break;
      }
      length = std::min(length, reads.front()->length());
      std::memcpy(p, reads.front()->data(), length);
      reads.trimStart(length);
      cb->readDataAvailable(length);
    }
    delivering = false;
    if (state == State::Open && peerEof && receiveEnd &&
        receiveSequence == receiveEnd - 1 && reads.empty() && records.empty() &&
        !readActive && !io->hasDeferredReceives(socketId) && reader &&
        !owner->getDestroyPending()) {
      auto* cb = std::exchange(reader, nullptr);
      cb->readEOF();
    }
  }
  void enqueue(
      WriteCallback* cb,
      std::unique_ptr<IOBuf> data,
      WriteFlags flags,
      std::vector<size_t> recordLengths = {}) {
    if (!live() || writeShutdown || closeAfterWrites ||
        flags != WriteFlags::NONE) {
      if (cb)
        cb->writeErr(
            0,
            AsyncSocketException(
                AsyncSocketException::NOT_OPEN,
                "URMA write unavailable or unsupported flags"));
      return;
    }
    const size_t n = data ? data->computeChainDataLength() : 0;
    if (n > config.max_pending_bytes - pendingBytes) {
      if (cb)
        cb->writeErr(
            0,
            AsyncSocketException(
                AsyncSocketException::BAD_ARGS,
                "URMA pending write limit reached"));
      return;
    }
    if (recordLengths.empty())
      recordLengths.push_back(n);
    size_t remaining = n;
    bool valid = true;
    for (size_t length : recordLengths) {
      if (length > remaining) {
        valid = false;
        break;
      }
      remaining -= length;
    }
    if (!valid || remaining) {
      if (cb)
        cb->writeErr(
            0,
            AsyncSocketException(
                AsyncSocketException::BAD_ARGS,
                "URMA record lengths do not cover write"));
      return;
    }
    size_t capacity = 0;
    if (data) {
      auto* node = data.get();
      do {
        if (node->capacity() >
            std::numeric_limits<size_t>::max() - queuedCapacity - capacity) {
          if (cb)
            cb->writeErr(
                0,
                AsyncSocketException(
                    AsyncSocketException::BAD_ARGS,
                    "URMA write capacity overflow"));
          return;
        }
        capacity += node->capacity();
        node = node->next();
      } while (node != data.get());
    }
    pendingBytes += n;
    queuedCapacity += capacity;
    if (writes.empty())
      lastWriteProgress = Clock::now();
    auto write = std::make_shared<Write>();
    write->callback = cb;
    write->data = std::move(data);
    write->recordLengths = std::move(recordLengths);
    write->bytes = n;
    write->capacity = capacity;
    writes.push_back(std::move(write));
    notifyBuffer();
    if (!live() || owner->getDestroyPending())
      return;
    flush();
    armTimer();
  }
  void timeoutExpired() noexcept override {
    DestructorGuard guard(owner);
    if (!live())
      return;
    const auto now = Clock::now();
    if (state != State::Open &&
        now - started >=
            std::chrono::milliseconds(config.handshake_timeout_ms)) {
      fail("URMA handshake timeout");
      return;
    }
    if (state == State::Handshake && !resourcesReady) {
      prepareHandshake();
      if (!live()) return;
    }
    if (sendTimeout && !writes.empty() &&
        now - lastWriteProgress >= std::chrono::milliseconds(sendTimeout)) {
      fail("URMA send timeout");
      return;
    }
    for (const auto& work : retries) {
      if (now - work->rnrSince >= kRnrTimeout) {
        fail("URMA receive-not-ready timeout");
        return;
      }
    }
    for (const auto& entry : sources) {
      if (now - entry.second->since >= kReadTimeout) {
        fail("URMA READ source/FREE timeout");
        return;
      }
    }
    if (now >= segments.deadline()) {
      fail("URMA segment acknowledgement timeout"); return;
    }
    if (peerEof && receiveEnd && receiveSequence != receiveEnd - 1 &&
        now - eofSince >= kReadTimeout) {
      fail("URMA stream boundary timeout");
      return;
    }
    if ((sourceWaiting && now - outgoingSince >= kReadTimeout) ||
        (!pendingFree.empty() && now - freeSince >= kReadTimeout) ||
        (!records.empty() && now - recordSince >= kReadTimeout)) {
      fail("URMA READ/FREE or receive sequence timeout");
      return;
    }
    if (state == State::Open) {
      poll();
      flush();
    }
    armTimer();
  }
  void fail(const std::string& message) {
    DestructorGuard guard(owner);
    if (!live())
      return;
    XLOG(ERR) << message;
    stop(true, message);
  }
  void stop(bool error, const std::string& message = "URMA connection closed") {
    if (!live())
      return;
    const bool graceful = !error && state == State::Open && writes.empty() &&
        sendSequence != UINT64_MAX;
    state = error ? State::Error : State::Closed;
    cancelTimeout();
    tcp->setReadCB(nullptr);
    if (graceful) {
      if (!endSent) {
        endSent = true;
        auto end = urma_detail::encodeEnd(sendSequence + 1);
        // No callback may retain Impl after closeNow/destruction. A normal
        // close() waits for the boundary in flush() before reaching stop().
        tcp->writeChain(nullptr, copyHandshake(end.data(), end.size()));
      }
      tcp->close();
    } else {
      tcp->closeNow();
    }
    cleanup();
    txActive = readActive = 0;
    retries.clear();
    segments = {};
    pendingBytes = 0;
    queuedCapacity = 0;
    sources.clear();
    reads.move();
    records.clear();
    pendingFree.clear();
    recordBytes = 0;
    auto failed = std::move(writes);
    writes.clear();
    AsyncSocketException ex(AsyncSocketException::NOT_OPEN, message);
    for (auto& w : failed)
      if (w->callback)
        w->callback->writeErr(w->completed, ex);
    notifyBuffer();
    if (auto* cb = std::exchange(reader, nullptr)) {
      if (error)
        cb->readErr(ex);
      else
        cb->readEOF();
    }
  }
  void cleanup() {
    if (io && socketId && res &&
        (!sources.empty() || !segments.empty() || !retries.empty() ||
         !pendingFree.empty() || !records.empty() || readActive || txActive ||
         io->hasDeferredReceives(socketId))) {
      try {
        auto retired = std::make_shared<Retirement>(
            io, socketId, peerSocketId, peerId, res);
        for (const auto& entry : sources)
          if (entry.second->resources->sourceExposed.load())
            retired->sources.emplace(entry.first, entry.second->resources);
        retired->segments = std::move(segments);
        retired->segments.close(retired->sources.empty());
        for (auto& free : pendingFree)
          retired->queue(SendKind::Free, free.sequence, std::move(free.budget));
        for (const auto& entry : records)
          if (entry.second.kind == SendKind::Pull && !entry.second.active)
            retired->queue(SendKind::Free, entry.first);
        for (const auto& work : retries)
          if (work->kind == SendKind::Free)
            retired->pending.push_back(work);
          else if (work->kind == SendKind::SegmentRevoked)
            retired->segments.queue(Segments::Type::Revoked, work->sequence);
        io->retire(socketId, std::move(retired));
      } catch (const std::bad_alloc&) {
        // Lease/source deleters quarantine unconfirmed exports on allocation
        // failure. Neither closing TCP nor a timer proves remote DMA stopped.
      }
    }
    if (io && socketId) {
      io->detach(socketId);
      socketId = 0;
    }
    res.reset();
  }
};

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* e, const SocketAddress& address)
    : impl_(std::make_unique<Impl>(this, e)) {
  impl_->client = true;
  impl_->tcp = AsyncSocket::newSocket(e);
  impl_->armTimer();
  impl_->tcp->connect(impl_.get(), address, impl_->config.handshake_timeout_ms);
}
UrmaAsyncSocket::UrmaAsyncSocket(EventBase* e, int fd)
    : UrmaAsyncSocket(AsyncSocket::newSocket(e, NetworkSocket(fd))) {}
UrmaAsyncSocket::UrmaAsyncSocket(AsyncSocket::UniquePtr acceptedSocket)
    : impl_(std::make_unique<Impl>(this, acceptedSocket->getEventBase())) {
  impl_->tcp = std::move(acceptedSocket);
  impl_->start();
}
UrmaAsyncSocket::~UrmaAsyncSocket() {
  impl_->reader = nullptr;
  impl_->stop(false);
}
UrmaAsyncSocket::Stats UrmaAsyncSocket::getUrmaStats() const {
  return {
      impl_->sendCompletions,
      impl_->receiveCompletions,
      impl_->readCompletions,
      impl_->directReadBytes,
      impl_->copiedReadBytes,
      impl_->directSendBytes,
      impl_->copiedSendBytes,
      impl_->maxReadsInFlight};
}
void UrmaAsyncSocket::setReadCB(ReadCallback* cb) noexcept {
  DestructorGuard guard(this);
  impl_->reader = cb;
  if (!cb)
    return;
  if (!impl_->live()) {
    impl_->reader = nullptr;
    if (error())
      cb->readErr(
          AsyncSocketException(AsyncSocketException::NOT_OPEN, "URMA failed"));
    else
      cb->readEOF();
  } else
    impl_->deliver();
}
AsyncTransport::ReadCallback* UrmaAsyncSocket::getReadCallback()
    const noexcept {
  return impl_->reader;
}
void UrmaAsyncSocket::write(WriteCallback* cb, const void* p, size_t n,
                            WriteFlags flags) noexcept {
  // Like writev(), retain the caller's bytes until its completion callback.
  // Registered staging (when needed) is acquired asynchronously by postSend.
  try { writeChain(cb, IOBuf::wrapBuffer(p, n), flags); }
  catch (const std::bad_alloc&) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::INTERNAL_ERROR, "URMA write metadata allocation failed"));
  }
}
void UrmaAsyncSocket::writev(
    WriteCallback* cb, const iovec* vec, size_t n, WriteFlags flags) noexcept {
  try { writeChain(cb, IOBuf::wrapIov(vec, n), flags); }
  catch (const std::bad_alloc&) {
    if (cb) cb->writeErr(0, AsyncSocketException(
        AsyncSocketException::INTERNAL_ERROR, "URMA write metadata allocation failed"));
  }
}
void UrmaAsyncSocket::writeChain(
    WriteCallback* cb, std::unique_ptr<IOBuf>&& p, WriteFlags flags) noexcept {
  DestructorGuard guard(this);
  impl_->enqueue(cb, std::move(p), flags);
}
void UrmaAsyncSocket::writeChainWithRecords(WriteCallback* cb,
                                            std::unique_ptr<IOBuf>&& data,
                                            std::vector<size_t> recordLengths,
                                            WriteFlags flags) noexcept {
  DestructorGuard guard(this);
  impl_->enqueue(cb, std::move(data), flags, std::move(recordLengths));
}
EventBase* UrmaAsyncSocket::getEventBase() const { return impl_->evb; }
void UrmaAsyncSocket::close() {
  DestructorGuard guard(this);
  impl_->closeAfterWrites = true;
  impl_->flush();
}
void UrmaAsyncSocket::closeNow() {
  DestructorGuard guard(this);
  impl_->stop(false);
}
void UrmaAsyncSocket::shutdownWrite() {
  DestructorGuard guard(this);
  impl_->writeShutdown = true;
  impl_->flush();
}
void UrmaAsyncSocket::shutdownWriteNow() { closeNow(); }
bool UrmaAsyncSocket::good() const {
  return impl_->live() && !impl_->peerEof && !impl_->writeShutdown &&
      !impl_->closeAfterWrites;
}
bool UrmaAsyncSocket::writable() const {
  return impl_->live() && !impl_->writeShutdown && !impl_->closeAfterWrites;
}
bool UrmaAsyncSocket::readable() const {
  return impl_->live() && (!impl_->peerEof || !impl_->reads.empty());
}
bool UrmaAsyncSocket::connecting() const {
  return impl_->state == Impl::State::Connecting ||
         impl_->state == Impl::State::Handshake;
}
bool UrmaAsyncSocket::error() const {
  return impl_->state == Impl::State::Error;
}
void UrmaAsyncSocket::attachEventBase(EventBase* e) {
  if (e != impl_->evb)
    throw std::logic_error("basic URMA transport cannot migrate EventBase");
}
void UrmaAsyncSocket::detachEventBase() {
  throw std::logic_error("basic URMA transport cannot detach EventBase");
}
void UrmaAsyncSocket::setSendTimeout(uint32_t ms) {
  impl_->sendTimeout = ms;
  // AsyncTransport specifies an idle-progress timeout; changing it restarts
  // the interval even when a write is already pending.
  impl_->lastWriteProgress = Clock::now();
  impl_->armTimer();
}
uint32_t UrmaAsyncSocket::getSendTimeout() const { return impl_->sendTimeout; }
void UrmaAsyncSocket::setBufferCallback(BufferCallback* callback) {
  DestructorGuard guard(this);
  impl_->bufferCallback = callback;
  impl_->reportedBytes = impl_->pendingBytes;
  impl_->reportedCapacity = impl_->allocatedBytesBuffered();
  // Like AsyncSocket, registration itself does not invoke user callbacks.
}
size_t UrmaAsyncSocket::getAppBytesBuffered() const {
  return impl_->pendingBytes;
}
size_t UrmaAsyncSocket::getRawBytesBuffered() const {
  return impl_->pendingBytes;
}
size_t UrmaAsyncSocket::getAllocatedBytesBuffered() const {
  return impl_->allocatedBytesBuffered();
}
void UrmaAsyncSocket::getLocalAddress(SocketAddress* a) const {
  impl_->tcp->getLocalAddress(a);
}
void UrmaAsyncSocket::getPeerAddress(SocketAddress* a) const {
  impl_->tcp->getPeerAddress(a);
}
void UrmaAsyncSocket::setEorTracking(bool enabled) {
  if (enabled)
    throw std::logic_error("basic URMA transport does not support EOR");
}
size_t UrmaAsyncSocket::getAppBytesWritten() const {
  return impl_->bytesWritten;
}
size_t UrmaAsyncSocket::getRawBytesWritten() const {
  return impl_->bytesWritten;
}
size_t UrmaAsyncSocket::getAppBytesReceived() const { return impl_->bytesRead; }
size_t UrmaAsyncSocket::getRawBytesReceived() const {
  return impl_->bytesRead;
}
bool UrmaAsyncSocket::isUrmaReady() const {
  return impl_->state == Impl::State::Open;
}
}  // namespace folly
#endif
