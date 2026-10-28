/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#ifdef FOLLY_HAVE_URMA
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <stdexcept>
#include <utility>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/logging/xlog.h>

namespace folly {
namespace {
// This deliberately has a different wire identity from the experimental V2/V3.
constexpr size_t kHelloSize = 52;
constexpr uint32_t kHeadroom = 128;
constexpr uint64_t kOffer = 0x5542523200000001; // UBR2, READ offer
constexpr uint64_t kReadDone = 0x5542523200000002;
constexpr size_t kOfferSize = 64;
constexpr uint32_t kReady = 0x52445931; // RDY1
constexpr uint64_t kRx = uint64_t(1) << 63;
constexpr uint64_t kRead = uint64_t(1) << 62;
constexpr uint64_t kWorkMask = kRead - 1;
constexpr auto kReadTimeout = std::chrono::seconds(30);
constexpr auto kRnrRetryDelay = std::chrono::milliseconds(1);
constexpr auto kRnrTimeout = std::chrono::seconds(5);
void put32(uint8_t* p, uint32_t n) {
  for (int i = 3; i >= 0; --i) {
    p[i] = uint8_t(n);
    n >>= 8;
  }
}
uint32_t get32(const uint8_t* p) {
  uint32_t n = 0;
  for (int i = 0; i < 4; ++i)
    n = (n << 8) | p[i];
  return n;
}
void put64(uint8_t* p, uint64_t n) {
  put32(p, uint32_t(n >> 32));
  put32(p + 4, uint32_t(n));
}
uint64_t get64(const uint8_t* p) {
  return (uint64_t(get32(p)) << 32) | get32(p + 4);
}
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
using Clock = std::chrono::steady_clock;
} // namespace

struct UrmaAsyncSocket::Impl final : AsyncSocket::ConnectCallback,
                                     AsyncTransport::ReadCallback,
                                     AsyncTransport::WriteCallback,
                                     EventHandler,
                                     AsyncTimeout {
  enum class State { Connecting, Handshake, Open, Closed, Error };
  enum class SendKind { Data, Offer, Ack };
  struct RegisteredBuffer {
    void* memory{nullptr};
    urma_target_seg_t* segment{nullptr};
    size_t size{0};
  };
  struct Resources : urma_detail::CleanupTask {
    urma_context_t* ctx{nullptr};
    urma_jfce_t* event{nullptr};
    urma_jfc_t* cq{nullptr};
    urma_jfr_t* rx{nullptr};
    urma_jetty_t* jetty{nullptr};
    urma_target_jetty_t* peer{nullptr};
    urma_target_seg_t* segment{nullptr};
    void* memory{nullptr};
    // Keep all DMA memory together, including on failed hardware teardown.
    RegisteredBuffer source, destination;
    urma_target_seg_t* imported{nullptr};
    urma_seg_t importedDescription{};
    void ackEvents() noexcept {
      if (!event)
        return;
      urma_jfc_t* queues[1];
      while (urma_wait_jfc(event, 1, 0, queues) > 0) {
        uint32_t counts[1] = {1};
        urma_ack_jfc(queues, counts, 1);
      }
    }
    bool tryCleanup() noexcept override {
      // The socket has detached its event handler and will never touch these
      // handles again. This state can therefore outlive both socket and evb.
      ackEvents();
      urma_status_t result;
      if (jetty) {
        result = urma_delete_jetty(jetty);
        if (result != URMA_SUCCESS)
          return failed("delete_jetty", result);
        jetty = nullptr;
      }
      if (peer) {
        result = urma_unimport_jetty(peer);
        if (result != URMA_SUCCESS)
          return failed("unimport_jetty", result);
        peer = nullptr;
      }
      if (imported) {
        result = urma_unimport_seg(imported);
        if (result != URMA_SUCCESS)
          return failed("unimport_seg", result);
        imported = nullptr;
      }
      // Successful local-jetty deletion and memory deregistration are the SDK
      // teardown barriers. A timeout or an OFFER CQE alone is never a barrier.
      for (auto* buffer : {&source, &destination}) {
        if (buffer->segment) {
          result = urma_unregister_seg(buffer->segment);
          if (result != URMA_SUCCESS)
            return failed(buffer == &source ? "unregister_source"
                                            : "unregister_destination", result);
          buffer->segment = nullptr;
        }
        std::free(buffer->memory);
        *buffer = {};
      }
      if (rx) {
        result = urma_delete_jfr(rx);
        if (result != URMA_SUCCESS)
          return failed("delete_jfr", result);
        rx = nullptr;
      }
      if (cq) {
        ackEvents();
        urma_cr_t completions[32];
        while (urma_poll_jfc(cq, 32, completions) > 0) {}
        result = urma_delete_jfc(cq);
        if (result != URMA_SUCCESS)
          return failed("delete_jfc", result);
        cq = nullptr;
      }
      if (event) {
        result = urma_delete_jfce(event);
        if (result != URMA_SUCCESS)
          return failed("delete_jfce", result);
        event = nullptr;
      }
      if (segment) {
        result = urma_unregister_seg(segment);
        if (result != URMA_SUCCESS)
          return failed("unregister_io", result);
        segment = nullptr;
      }
      std::free(memory);
      memory = nullptr;
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
  };
  UrmaAsyncSocket* owner;
  EventBase* evb;
  UrmaConfig config;
  AsyncSocket::UniquePtr tcp;
  std::unique_ptr<Resources> res;
  State state{State::Connecting};
  AsyncTransport::ReadCallback* reader{nullptr};
  std::array<uint8_t, 128> tcpBuffer{};
  std::vector<uint8_t> handshake;
  unsigned handshakeWrites{0};
  bool haveHello{false}, haveReady{false}, peerRead{false};
  bool sending{false}, delivering{false}, polling{false};
  bool writeShutdown{false}, closeAfterWrites{false}, txActive{false};
  bool peerEof{false};
  uint32_t peerBuffer{0}, sendTimeout{0};
  uint32_t peerLimit{0};
  SendKind txKind{SendKind::Data};
  uint64_t nextMessage{0}, outgoing{0}, incoming{0}, lastIncoming{0};
  uint64_t readId{0};
  size_t readOffset{0}, readBytes{0};
  size_t outgoingBytes{0}, incomingBytes{0};
  bool offerPosted{false}, offerCompleted{false}, readAck{false}, readActive{false};
  Clock::time_point outgoingSince{}, incomingSince{};
  Clock::time_point ackSince{};
  std::deque<uint64_t> acknowledgements;
  uint64_t txId{0};
  Clock::time_point started{Clock::now()};
  Clock::time_point rnrSince{}, retryAt{};
  Clock::time_point lastWriteProgress{};
  std::deque<Write> writes;
  IOBufQueue reads{IOBufQueue::cacheChainLength()};
  size_t pendingBytes{0}, txBytes{0}, bytesWritten{0}, bytesRead{0};
  size_t queuedCapacity{0}, reportedBytes{0}, reportedCapacity{0};
  AsyncTransport::BufferCallback* bufferCallback{nullptr};
  bool notifyingBuffer{false};
  urma_jetty_id_t peerId{};

  Impl(UrmaAsyncSocket* o, EventBase* e)
      : EventHandler(e),
        AsyncTimeout(e),
        owner(o),
        evb(e),
        config(UrmaInit::getConfig()) {}
  ~Impl() override {
    cancelTimeout();
    unregisterHandler();
    cleanup();
  }
  bool live() const { return state != State::Closed && state != State::Error; }
  size_t allocatedBytesBuffered() const {
    // Retained write storage plus the active READ copy; idle registered caches
    // and receive buffers are not pending egress data.
    const size_t source = outgoing && res ? res->source.size : 0;
    return queuedCapacity + std::min(
        source, std::numeric_limits<size_t>::max() - queuedCapacity);
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
  void popWriteBuffer(Write& write) {
    queuedCapacity -= write.data->capacity();
    write.data = write.data->pop();
  }
  void armTimer() {
    if (live() && (state != State::Open || !writes.empty() || incoming ||
                   !acknowledgements.empty() || txActive))
      scheduleTimeout(retryAt != Clock::time_point{} ? 1 : 10);
    else
      cancelTimeout();
  }
  bool createResources() {
    res = std::make_unique<Resources>();
    res->ctx = UrmaInit::acquireContext();
    if (!res->ctx)
      return false;
    const size_t bytes = size_t(config.rx_depth + 1) * config.buffer_size;
    if (posix_memalign(&res->memory, 4096, bytes))
      return false;
    urma_seg_cfg_t sc{};
    sc.va = reinterpret_cast<uint64_t>(res->memory);
    sc.len = bytes;
    sc.flag.bs.access = URMA_ACCESS_LOCAL_ONLY;
    res->segment = urma_register_seg(res->ctx, &sc);
    res->event = urma_create_jfce(res->ctx);
    if (!res->segment || !res->event)
      return false;
    urma_jfc_cfg_t cc{};
    cc.depth = config.rx_depth + config.tx_depth + 1;
    cc.jfce = res->event;
    res->cq = urma_create_jfc(res->ctx, &cc);
    if (!res->cq)
      return false;
    urma_jfr_cfg_t rc{};
    rc.depth = config.rx_depth;
    rc.max_sge = 1;
    rc.jfc = res->cq;
    rc.trans_mode = URMA_TM_RM;
    rc.flag.bs.order_type = URMA_OI;
    rc.min_rnr_timer = 12;
    res->rx = urma_create_jfr(res->ctx, &rc);
    if (!res->rx)
      return false;
    urma_jetty_cfg_t jc{};
    // SDK calls this 'share_jfr', but this JFR belongs to this connection only.
    jc.flag.bs.share_jfr = 1;
    jc.shared.jfr = res->rx;
    jc.shared.jfc = res->cq;
    jc.jfs_cfg.depth = config.tx_depth;
    jc.jfs_cfg.max_sge = 1;
    jc.jfs_cfg.jfc = res->cq;
    jc.jfs_cfg.trans_mode = URMA_TM_RM;
    jc.jfs_cfg.flag.bs.order_type = URMA_OI;
    jc.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jc.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    res->jetty = urma_create_jetty(res->ctx, &jc);
    if (!res->jetty)
      return false;
    for (uint32_t i = 0; i < config.rx_depth; ++i)
      if (!postReceive(i))
        return false;
    changeHandlerFD(NetworkSocket(res->event->fd));
    return registerHandler(READ | PERSIST) &&
        urma_rearm_jfc(res->cq, false) == URMA_SUCCESS;
  }
  bool postReceive(uint32_t slot) {
    urma_sge_t sge{};
    sge.addr = reinterpret_cast<uint64_t>(res->memory) +
        size_t(slot) * config.buffer_size;
    sge.len = config.buffer_size;
    sge.tseg = res->segment;
    urma_jfr_wr_t wr{};
    wr.src.sge = &sge;
    wr.src.num_sge = 1;
    wr.user_ctx = kRx | slot;
    urma_jfr_wr_t* bad = nullptr;
    return urma_post_jfr_wr(res->rx, &wr, &bad) == URMA_SUCCESS;
  }
  void start() {
    DestructorGuard guard(owner);
    state = State::Handshake;
    started = Clock::now();
    if (!createResources()) {
      fail("URMA resource creation failed");
      return;
    }
    std::array<uint8_t, kHelloSize> h{};
    std::memcpy(h.data(), "UBR2", 4);
    put32(h.data() + 4, kHelloSize);
    const auto& id = res->jetty->jetty_id;
    std::memcpy(h.data() + 8, id.eid.raw, 16);
    put32(h.data() + 24, id.uasid);
    put32(h.data() + 28, id.id);
    put32(h.data() + 32, config.buffer_size);
    put32(h.data() + 36, URMA_TM_RM);
    put32(h.data() + 40, kHeadroom);
    put32(h.data() + 44, config.max_pending_bytes);
    put32(h.data() + 48, config.enable_read && UrmaInit::getMaxReadSize() ? 1 : 0);
    tcp->setReadCB(this);
    sendHandshake(h.data(), h.size());
    armTimer();
  }
  void sendHandshake(const void* p, size_t n) {
    ++handshakeWrites;
    tcp->writeChain(this, IOBuf::copyBuffer(p, n));
  }
  void connectSuccess() noexcept override { start(); }
  void connectErr(const AsyncSocketException& e) noexcept override {
    fail(e.what());
  }
  void writeSuccess() noexcept override {
    DestructorGuard guard(owner);
    if (handshakeWrites)
      --handshakeWrites;
    becomeReady();
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
    if (state == State::Open || handshake.size() + n > kHelloSize + 4) {
      fail("unexpected data on URMA handshake channel");
      return;
    }
    handshake.insert(handshake.end(), tcpBuffer.begin(), tcpBuffer.begin() + n);
    if (!haveHello && handshake.size() >= kHelloSize) {
      auto* h = handshake.data();
      if (std::memcmp(h, "UBR2", 4) || get32(h + 4) != kHelloSize ||
          get32(h + 36) != URMA_TM_RM || get32(h + 32) <= kHeadroom ||
          get32(h + 32) > 4096 || !get32(h + 28) ||
          get32(h + 40) != kHeadroom || get32(h + 44) < get32(h + 32) ||
          get32(h + 48) > 1) {
        fail("incompatible basic URMA Hello");
        return;
      }
      std::memcpy(peerId.eid.raw, h + 8, 16);
      peerId.uasid = get32(h + 24);
      peerId.id = get32(h + 28);
      peerBuffer = std::min(config.buffer_size, get32(h + 32));
      peerLimit = get32(h + 44);
      peerRead = get32(h + 48) != 0;
      urma_rjetty_t remote{};
      remote.jetty_id = peerId;
      remote.trans_mode = URMA_TM_RM;
      remote.type = URMA_JETTY;
      remote.tp_type = URMA_CTP;
      remote.flag.bs.order_type = URMA_OI;
      urma_token_t token{};
      res->peer = urma_import_jetty(res->ctx, &remote, &token);
      if (!res->peer) {
        fail("URMA peer import failed errno=" + std::to_string(errno));
        return;
      }
      haveHello = true;
      handshake.erase(handshake.begin(), handshake.begin() + kHelloSize);
      uint8_t ready[4];
      put32(ready, kReady);
      sendHandshake(ready, sizeof(ready));
      if (!live())
        return;
    }
    if (haveHello && handshake.size() == 4) {
      if (get32(handshake.data()) != kReady) {
        fail("invalid URMA ready message");
        return;
      }
      handshake.clear();
      haveReady = true;
      becomeReady();
    }
  }
  void becomeReady() {
    if (state != State::Handshake || !haveReady || handshakeWrites)
      return;
    state = State::Open;
    armTimer();
    poll();
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
    // Local SEND completion precedes the peer's TCP FIN. Drain placed receives.
    poll();
    if (!live())
      return;
    // TCP FIN closes only the peer's write direction. Its URMA ACKs for our
    // outgoing writes are still allowed, so do not cancel those writes here.
    if (incoming) {
      fail("peer closed with an unfinished URMA READ");
      return;
    }
    peerEof = true;
    deliver();
    if (reads.empty() && reader) {
      auto* cb = std::exchange(reader, nullptr);
      cb->readEOF();
    }
  }
  void readErr(const AsyncSocketException& e) noexcept override {
    fail(e.what());
  }
  void handlerReady(uint16_t) noexcept override {
    DestructorGuard guard(owner);
    if (!live() || !res)
      return;
    ackEvents();
    poll();
    if (live() && res) {
      if (urma_rearm_jfc(res->cq, false) != URMA_SUCCESS) {
        fail("URMA CQ rearm failed");
        return;
      }
      poll(); // close the poll/rearm race
    }
  }
  void ackEvents() {
    if (res)
      res->ackEvents();
  }
  size_t smallWriteLimit() const {
    // Subtract instead of adding to the payload, so the test cannot overflow.
    return peerBuffer > kHeadroom ? peerBuffer - kHeadroom : 0;
  }
  bool registerBuffer(RegisteredBuffer& b, size_t size, uint32_t access) {
    b.size = size;
    if (posix_memalign(&b.memory, 4096, size))
      return false;
    urma_seg_cfg_t sc{};
    sc.va = reinterpret_cast<uint64_t>(b.memory);
    sc.len = size;
    sc.flag.bs.access = access;
    b.segment = urma_register_seg(res->ctx, &sc);
    return b.segment != nullptr;
  }
  static bool releaseBuffer(RegisteredBuffer& b) {
    if (b.segment && urma_unregister_seg(b.segment) != URMA_SUCCESS)
      return false;
    b.segment = nullptr;
    std::free(b.memory);
    b = {};
    return true;
  }
  bool ensureBuffer(
      RegisteredBuffer& b, size_t size, size_t limit, uint32_t access) {
    if (b.segment && b.size >= size)
      return true;
    // This is called only after the previous transfer's completion/ACK.
    // Reuse per-connection storage; registering every message blocks the IO
    // thread on page pinning and kernel/device bookkeeping.
    if (!releaseBuffer(b))
      return false;
    size_t capacity = std::min<size_t>(4096, limit);
    while (capacity < size)
      capacity = std::min(capacity * 2, limit);
    if (!registerBuffer(b, capacity, access))
      return false;
    std::memset(b.memory, 0, capacity);
    return true;
  }
  void planWrite(Write& write) {
    if (write.planned)
      return;
    for (size_t length : write.recordLengths) {
      if (!length)
        continue;
      const bool read = config.enable_read && peerRead &&
          UrmaInit::getMaxReadSize() && length > config.read_threshold &&
          length <= peerLimit;
      // Decide per record first. Adjacent eligible READ records can then share
      // one offer, without pulling a small record into a READ or crossing the
      // peer's holding limit. This never waits for another independent write.
      if (!write.ranges.empty() && write.ranges.back().read == read &&
          (!read || length <= peerLimit - write.ranges.back().remaining))
        write.ranges.back().remaining += length;
      else
        write.ranges.push_back({length, read});
    }
    write.recordLengths.clear();
    write.planned = true;
  }
  void consumeWrite(size_t bytes) {
    if (bytes)
      lastWriteProgress = Clock::now();
    auto& write = writes.front();
    pendingBytes -= bytes;
    bytesWritten += bytes;
    write.completed += bytes;
    write.ranges.front().remaining -= bytes;
    if (!write.ranges.front().remaining)
      write.ranges.pop_front();
    while (write.data && write.data->length() <= bytes) {
      bytes -= write.data->length();
      popWriteBuffer(write);
    }
    if (write.data)
      write.data->trimStart(bytes);
  }
  bool prepareOffer() {
    const auto size = writes.front().ranges.front().remaining;
    if (size > peerLimit || nextMessage == std::numeric_limits<uint64_t>::max()) {
      fail("URMA READ write exceeds peer limit or message IDs exhausted");
      return false;
    }
    if (!ensureBuffer(res->source, size,
                      std::min(peerLimit, config.max_pending_bytes), URMA_ACCESS_READ)) {
      // No offer was posted. The original bytes are still owned by the write;
      // retry this range through bounded SENDs instead of failing the stream.
      writes.front().ranges.front().read = false;
      return false;
    }
    // Snapshot user storage: even an abort callback may release the original
    // writev buffers while the peer is still finishing DMA.
    io::Cursor(writes.front().data.get()).pull(res->source.memory, size);
    if (!supportedReadSegmentAttr(res->source.segment->seg.attr)) {
      writes.front().ranges.front().read = false;
      return false;
    }
    outgoing = ++nextMessage;
    outgoingBytes = size;
    outgoingSince = Clock::now();
    offerPosted = offerCompleted = readAck = false;
    notifyBuffer();
    return live() && !owner->getDestroyPending();
  }
  void encodeOffer(uint8_t* p) const {
    const auto& seg = res->source.segment->seg;
    put64(p, outgoing);
    put64(p + 8, outgoingBytes);
    put64(p + 16, seg.ubva.va);
    put64(p + 24, seg.len);
    put32(p + 32, seg.token_id);
    put32(p + 36, seg.attr.value);
    put32(p + 40, seg.ubva.uasid);
    put32(p + 44, 0); // Reserved, never a raw process pointer.
    std::memcpy(p + 48, seg.ubva.eid.raw, 16);
  }
  void finishLargeWrite() {
    if (!outgoing || !offerCompleted || !readAck)
      return;
    const auto size = outgoingBytes;
    outgoing = 0;
    consumeWrite(size);
    finishWrite();
  }
  void receiveControl(uint64_t type, const uint8_t* p, size_t size) {
    if (type == kReadDone && size == 8) {
      if (!outgoing || !offerPosted || readAck || get64(p) != outgoing) {
        fail("invalid URMA READ acknowledgement");
        return;
      }
      readAck = true;
      finishLargeWrite(); // ACK may arrive before the local offer CQE.
      return;
    }
    if (type != kOffer || size != kOfferSize || incoming ||
        !config.enable_read || !peerRead || !UrmaInit::getMaxReadSize() ||
        acknowledgements.size() >= 2) {
      fail("invalid URMA READ control message");
      return;
    }
    const auto id = get64(p);
    const auto length = get64(p + 8);
    urma_seg_t seg{};
    seg.ubva.va = get64(p + 16);
    seg.len = get64(p + 24);
    seg.token_id = get32(p + 32);
    seg.attr.value = get32(p + 36);
    seg.ubva.uasid = get32(p + 40);
    std::memcpy(seg.ubva.eid.raw, p + 48, 16);
    // Only accept the exact, bounded segment offered by this connection's
    // peer. No remote address arithmetic is done until these checks pass.
    // READ selection is the peer's policy, independent of SEND capacity and
    // our local threshold. Negotiated capabilities and bounds still apply.
    if (id <= lastIncoming || !length ||
        length > config.max_pending_bytes - reads.chainLength() ||
        seg.len < length || seg.len > config.max_pending_bytes || !seg.ubva.va ||
        seg.ubva.va > std::numeric_limits<uint64_t>::max() - seg.len ||
        !supportedReadSegmentAttr(seg.attr) || get32(p + 44) ||
        seg.ubva.uasid != peerId.uasid ||
        std::memcmp(seg.ubva.eid.raw, peerId.eid.raw, 16)) {
      fail("invalid URMA READ descriptor");
      return;
    }
    const auto& previous = res->importedDescription;
    const bool sameSegment = res->imported &&
        seg.ubva.va == previous.ubva.va && seg.len == previous.len &&
        seg.token_id == previous.token_id && seg.attr.value == previous.attr.value &&
        seg.ubva.uasid == previous.ubva.uasid &&
        !std::memcmp(seg.ubva.eid.raw, previous.ubva.eid.raw, 16);
    if (!sameSegment) {
      if (res->imported && urma_unimport_seg(res->imported) != URMA_SUCCESS) {
        fail("URMA READ segment replacement failed");
        return;
      }
      res->imported = nullptr;
      urma_import_seg_flag_t flags{};
      flags.bs.access = URMA_ACCESS_READ;
      urma_token_t token{};
      res->imported = urma_import_seg(res->ctx, &seg, &token, 0, flags);
      res->importedDescription = seg;
    }
    if (!res->imported || !ensureBuffer(
        res->destination, length, config.max_pending_bytes, URMA_ACCESS_LOCAL_ONLY)) {
      fail("URMA READ destination setup failed");
      return;
    }
    incoming = lastIncoming = id;
    incomingBytes = length;
    incomingSince = Clock::now();
    readOffset = 0;
  }
  void postRead() {
    if (!incoming || readActive)
      return;
    const size_t bytes = std::min<size_t>(
        incomingBytes - readOffset, UrmaInit::getMaxReadSize());
    urma_sge_t source{}, destination{};
    source.addr = res->imported->seg.ubva.va + readOffset;
    source.len = bytes;
    source.tseg = res->imported;
    destination.addr = reinterpret_cast<uint64_t>(res->destination.memory) + readOffset;
    destination.len = bytes;
    destination.tseg = res->destination.segment;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_READ;
    wr.tjetty = res->peer;
    wr.flag.bs.complete_enable = 1;
    wr.rw.src.sge = &source;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &destination;
    wr.rw.dst.num_sge = 1;
    wr.user_ctx = kRead | ((readId + 1) & kWorkMask);
    urma_jfs_wr_t* bad = nullptr;
    const auto status = urma_post_jetty_send_wr(res->jetty, &wr, &bad);
    if (status == URMA_EAGAIN)
      return;
    if (status != URMA_SUCCESS) {
      fail("URMA READ submit failed: " + std::to_string(status));
      return;
    }
    readId = wr.user_ctx;
    readBytes = bytes;
    readActive = true;
  }
  void completeRead() {
    readActive = false;
    readOffset += readBytes;
    if (readOffset != incomingBytes)
      return;
    // Application IOBufs may outlive the next READ; give them independent
    // storage before acknowledging and reusing this registered destination.
    auto data = IOBuf::copyBuffer(res->destination.memory, incomingBytes);
    bytesRead += readOffset;
    // The ACK is durable in our queue before any application callback runs.
    // It has priority over local writes, including a simultaneous READ offer.
    if (acknowledgements.empty())
      ackSince = Clock::now();
    acknowledgements.push_back(incoming);
    incoming = 0;
    reads.append(std::move(data));
    flush();
    deliver();
  }
  void poll() {
    if (polling || !res || !res->cq || !live())
      return;
    polling = true;
    int n = 0;
    while (live() && !owner->getDestroyPending()) {
      // Providers do not populate every field of every completion type.
      urma_cr_t cr[32]{};
      n = urma_poll_jfc(res->cq, 32, cr);
      if (n <= 0)
        break;
      for (int i = 0; i < n && live() && !owner->getDestroyPending(); ++i) {
        const auto& c = cr[i];
        if (!c.flag.bs.s_r &&
            c.status == URMA_CR_RNR_RETRY_CNT_EXC_ERR && txActive &&
            c.user_ctx == txId) {
          // RM reports RNR only when the peer could not receive this SEND.
          // Keep its bytes queued and retry after yielding to the EventBase.
          // Never retry ambiguous errors such as ACK timeout.
          txActive = false;
          if (txKind == SendKind::Offer)
            offerPosted = false;
          const auto now = Clock::now();
          if (rnrSince == Clock::time_point{})
            rnrSince = now;
          retryAt = now + kRnrRetryDelay;
          continue;
        }
        if (c.status != URMA_CR_SUCCESS) {
          fail("URMA completion failed: " + std::to_string(c.status));
          break;
        }
        if (c.flag.bs.s_r) {
          const uint64_t slot = c.user_ctx & ~kRx;
          if (!(c.user_ctx & kRx) || slot >= config.rx_depth ||
              !c.completion_len || c.completion_len > config.buffer_size ||
              (c.opcode != URMA_CR_OPC_SEND &&
               c.opcode != URMA_CR_OPC_SEND_WITH_IMM) || !haveHello ||
              c.remote_id.id != peerId.id ||
              // UDMA receive CQEs report the remote EID and jetty ID, but
              // not UASID. Do not compare an unreported field with the hello.
              std::memcmp(c.remote_id.eid.raw, peerId.eid.raw, 16)) {
            fail("invalid URMA receive completion");
            break;
          }
          auto data = IOBuf::copyBuffer(
              static_cast<char*>(res->memory) + slot * config.buffer_size,
              c.completion_len);
          if (!postReceive(slot)) {
            fail("URMA receive repost failed");
            break;
          }
          if (c.opcode == URMA_CR_OPC_SEND_WITH_IMM) {
            receiveControl(c.imm_data, data->data(), data->length());
          } else {
            if (incoming || c.completion_len > smallWriteLimit() ||
                reads.chainLength() + c.completion_len > config.max_pending_bytes) {
              fail("invalid or excessive URMA stream data");
              break;
            }
            bytesRead += c.completion_len;
            reads.append(std::move(data));
            deliver();
          }
        } else if (c.user_ctx & kRead) {
          if (!readActive || c.user_ctx != readId) {
            fail("invalid URMA READ completion");
            break;
          }
          completeRead();
        } else {
          if (c.user_ctx != txId || !txActive) {
            fail("invalid URMA send completion");
            break;
          }
          txActive = false;
          rnrSince = retryAt = Clock::time_point{};
          if (txKind == SendKind::Ack) {
            acknowledgements.pop_front();
            ackSince = Clock::now();
            continue;
          }
          if (txKind == SendKind::Offer) {
            lastWriteProgress = Clock::now();
            offerCompleted = true;
            finishLargeWrite();
            continue;
          }
          // Consume only after CQ success; retries retain both bytes and ranges.
          consumeWrite(txBytes);
          txBytes = 0;
          finishWrite();
        }
      }
    }
    polling = false;
    if (n < 0 && live()) {
      fail("URMA CQ polling failed");
      return;
    }
    if (live() && !owner->getDestroyPending())
      flush();
  }
  void finishWrite() {
    if (txActive || writes.empty() || writes.front().data)
      return;
    auto* cb = writes.front().callback;
    writes.pop_front();
    if (cb)
      cb->writeSuccess();
  }
  void flush() {
    if (sending || state != State::Open || owner->getDestroyPending())
      return;
    sending = true;
    if (state == State::Open)
      postRead();
    while (state == State::Open && !txActive &&
           !owner->getDestroyPending()) {
      if (Clock::now() < retryAt)
        break;
      auto* target = static_cast<uint8_t*>(res->memory) +
          size_t(config.rx_depth) * config.buffer_size;
      size_t bytes = 0;
      SendKind kind;
      uint64_t control = 0;
      if (!acknowledgements.empty()) {
        kind = SendKind::Ack;
        control = kReadDone;
        bytes = 8;
        put64(target, acknowledgements.front());
      } else {
        if (writes.empty())
          break;
        auto& request = writes.front();
        while (request.data && request.data->length() == 0)
          popWriteBuffer(request);
        if (!request.data) {
          finishWrite();
          continue;
        }
        planWrite(request);
        if (request.ranges.front().read) {
          if (!outgoing && !prepareOffer()) {
            if (state != State::Open)
              break;
            continue; // Registration failed before posting: SEND fallback.
          }
          if (offerPosted)
            break; // Keep ownership until both local CQE and remote ACK.
          kind = SendKind::Offer;
          control = kOffer;
          bytes = kOfferSize;
          encodeOffer(target);
        } else {
          kind = SendKind::Data;
          bytes = std::min(request.ranges.front().remaining, smallWriteLimit());
          io::Cursor(request.data.get()).pull(target, bytes);
        }
      }
      urma_sge_t sge{};
      sge.addr = reinterpret_cast<uint64_t>(target);
      sge.len = bytes;
      sge.tseg = res->segment;
      urma_jfs_wr_t wr{};
      wr.opcode = control ? URMA_OPC_SEND_IMM : URMA_OPC_SEND;
      wr.tjetty = res->peer;
      // One outstanding RM SEND at a time: its successful completion confirms
      // remote placement before we submit the next portion of the byte stream.
      wr.flag.bs.place_order = URMA_STRONG_ORDER;
      wr.flag.bs.comp_order = 1;
      wr.flag.bs.complete_enable = 1;
      wr.send.src.sge = &sge;
      wr.send.src.num_sge = 1;
      wr.send.imm_data = control;
      wr.user_ctx = (txId + 1) & kWorkMask;
      urma_jfs_wr_t* bad = nullptr;
      const auto st = urma_post_jetty_send_wr(res->jetty, &wr, &bad);
      if (st == URMA_EAGAIN)
        break;
      if (st != URMA_SUCCESS) {
        fail("URMA SEND submit failed: " + std::to_string(st));
        break;
      }
      txId = wr.user_ctx;
      txKind = kind;
      if (kind == SendKind::Offer)
        offerPosted = true;
      txActive = true;
      txBytes = bytes;
      retryAt = Clock::time_point{};
    }
    sending = false;
    notifyBuffer();
    if (state == State::Open && writes.empty() && !txActive &&
        acknowledgements.empty() && !incoming) {
      if (closeAfterWrites) {
        stop(false);
        return;
      }
      if (writeShutdown)
        tcp->shutdownWrite();
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
      if (state != State::Open || owner->getDestroyPending() || reader != cb)
        break;
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
    if (state == State::Open && peerEof && reads.empty() && reader &&
        !owner->getDestroyPending()) {
      auto* cb = std::exchange(reader, nullptr);
      cb->readEOF();
    }
  }
  void enqueue(
      WriteCallback* cb, std::unique_ptr<IOBuf> data, WriteFlags flags,
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
        cb->writeErr(0, AsyncSocketException(
            AsyncSocketException::BAD_ARGS, "URMA record lengths do not cover write"));
      return;
    }
    size_t capacity = 0;
    if (data) {
      auto* node = data.get();
      do {
        if (node->capacity() > std::numeric_limits<size_t>::max() -
                queuedCapacity - capacity) {
          if (cb)
            cb->writeErr(0, AsyncSocketException(
                AsyncSocketException::BAD_ARGS, "URMA write capacity overflow"));
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
    writes.push_back(Write{cb, std::move(data), std::move(recordLengths)});
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
    if (sendTimeout && !writes.empty() &&
        now - lastWriteProgress >= std::chrono::milliseconds(sendTimeout)) {
      fail("URMA send timeout");
      return;
    }
    if (rnrSince != Clock::time_point{} && now - rnrSince >= kRnrTimeout) {
      fail("URMA receive-not-ready timeout");
      return;
    }
    if ((outgoing && now - outgoingSince >= kReadTimeout) ||
        (incoming && now - incomingSince >= kReadTimeout) ||
        (!acknowledgements.empty() && now - ackSince >= kReadTimeout)) {
      fail("URMA READ or acknowledgement timeout");
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
    state = error ? State::Error : State::Closed;
    cancelTimeout();
    unregisterHandler();
    // Detach the TCP callbacks before aborting handshake writes.
    tcp->setReadCB(nullptr);
    tcp->closeNow();
    cleanup();
    txActive = false;
    txBytes = 0;
    pendingBytes = 0;
    queuedCapacity = 0;
    outgoing = 0;
    reads.move();
    auto failed = std::move(writes);
    writes.clear();
    AsyncSocketException ex(AsyncSocketException::NOT_OPEN, message);
    for (auto& w : failed)
      if (w.callback)
        w.callback->writeErr(w.completed, ex);
    notifyBuffer();
    if (auto* cb = std::exchange(reader, nullptr)) {
      if (error)
        cb->readErr(ex);
      else
        cb->readEOF();
    }
  }
  void cleanup() {
    urma_detail::cleanupOrDefer(std::move(res));
  }
};

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* e, const SocketAddress& address)
    : impl_(std::make_unique<Impl>(this, e)) {
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
void UrmaAsyncSocket::write(
    WriteCallback* cb, const void* p, size_t n, WriteFlags flags) noexcept {
  writeChain(cb, IOBuf::copyBuffer(p, n), flags);
}
void UrmaAsyncSocket::writev(
    WriteCallback* cb, const iovec* vec, size_t n, WriteFlags flags) noexcept {
  writeChain(cb, IOBuf::wrapIov(vec, n), flags);
}
void UrmaAsyncSocket::writeChain(
    WriteCallback* cb, std::unique_ptr<IOBuf>&& p, WriteFlags flags) noexcept {
  DestructorGuard guard(this);
  impl_->enqueue(cb, std::move(p), flags);
}
void UrmaAsyncSocket::writeChainWithRecords(
    WriteCallback* cb, std::unique_ptr<IOBuf>&& data,
    std::vector<size_t> recordLengths, WriteFlags flags) noexcept {
  DestructorGuard guard(this);
  impl_->enqueue(cb, std::move(data), flags, std::move(recordLengths));
}
EventBase* UrmaAsyncSocket::getEventBase() const {
  return impl_->evb;
}
void UrmaAsyncSocket::close() {
  DestructorGuard guard(this);
  impl_->closeAfterWrites = true;
  if (impl_->writes.empty() && !impl_->txActive &&
      impl_->acknowledgements.empty() && !impl_->incoming)
    impl_->stop(false);
  else
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
void UrmaAsyncSocket::shutdownWriteNow() {
  closeNow();
}
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
uint32_t UrmaAsyncSocket::getSendTimeout() const {
  return impl_->sendTimeout;
}
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
size_t UrmaAsyncSocket::getAppBytesReceived() const {
  return impl_->bytesRead;
}
size_t UrmaAsyncSocket::getRawBytesReceived() const {
  return impl_->bytesRead;
}
bool UrmaAsyncSocket::isUrmaReady() const {
  return impl_->state == Impl::State::Open;
}
} // namespace folly
#endif
