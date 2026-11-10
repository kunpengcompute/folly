/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/ScopeGuard.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaIoContext.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
namespace folly {
using namespace urma_detail;
namespace {
using Clock = std::chrono::steady_clock;
struct PeerState {
  urma_context_t* context{nullptr};
  urma_target_jetty_t* io{nullptr};
  urma_target_seg_t* segment{nullptr};
  Hello hello;
  std::atomic<bool> done{true}, cancelled{false};
  bool ok{false};
};
struct PeerCleanup final : CleanupTask {
  std::shared_ptr<PeerState> value{std::make_shared<PeerState>()};
  bool tryCleanup() noexcept override {
    value->cancelled = true;
    if (!value->done.load(std::memory_order_acquire) || value.use_count() != 1)
      return failed("wait-peer-import-or-WR", 0);
    if (value->io) {
      auto rc = urma_unimport_jetty(value->io);
      if (rc != URMA_SUCCESS) return failed("unimport-peer-io", rc);
      value->io = nullptr;
    }
    if (value->segment) {
      auto rc = urma_unimport_seg(value->segment);
      if (rc != URMA_SUCCESS) return failed("unimport-hello-segment", rc);
      value->segment = nullptr;
    }
    if (value->context) {
      UrmaInit::releaseContext();
      value->context = nullptr;
    }
    return true;
  }
};
void importPeer(const std::shared_ptr<PeerState>& state,
                const std::shared_ptr<UrmaPoolSignal>& signal,
                bool read) noexcept {
  auto done = makeGuard([&] {
    state->done.store(true, std::memory_order_release);
    signal->signal();
  });
  if (state->cancelled) return;
  urma_rjetty_t remote{};
  remote.jetty_id.eid = state->hello.eid;
  remote.jetty_id.uasid = state->hello.uasid;
  remote.type = URMA_JETTY;
  remote.trans_mode = URMA_TM_RM;
  remote.tp_type = URMA_CTP;
  remote.flag.bs.order_type = URMA_OI;
  urma_token_t token{};
  remote.jetty_id.id = state->hello.ioJetty;
  state->io = urma_import_jetty(state->context, &remote, &token);
  if (!state->io || state->cancelled) return;
  if (read && state->hello.read) {
    urma_import_seg_flag_t flag{};
    flag.bs.access = URMA_ACCESS_READ;
    state->segment =
        urma_import_seg(state->context, &state->hello.segment, &token, 0, flag);
    if (!state->segment) return;
  }
  state->ok = true;
}
}  // namespace
struct UrmaAsyncSocket::Impl final : UrmaRoute,
                                     AsyncSocket::ConnectCallback,
                                     AsyncTransport::ReadCallback,
                                     AsyncTransport::WriteCallback,
                                     AsyncTimeout {
  enum class State { Connecting, Handshake, Open, Draining, Closed };
  struct PendingCharge {
    std::shared_ptr<RxBudget> budget;
    size_t bytes{0};
    void reserve(const std::shared_ptr<RxBudget>& b, size_t n) {
      const auto held = b->retained.load(std::memory_order_relaxed);
      if (held > b->limit || n > b->limit - held)
        throw std::runtime_error("URMA connection retained storage limit exceeded");
      b->retained.fetch_add(n, std::memory_order_acq_rel);
      budget = b;
      bytes = n;
    }
    ~PendingCharge() {
      if (budget) budget->retained.fetch_sub(bytes, std::memory_order_acq_rel);
    }
    void release(size_t n) {
      if (n > bytes) throw std::runtime_error("URMA retained charge underflow");
      bytes -= n;
      if (budget) budget->retained.fetch_sub(n, std::memory_order_acq_rel);
    }
  };
  struct Write {
    PendingCharge charge;
    WriteCallback* callback;
    std::unique_ptr<IOBuf> data;
    size_t bytes{0}, capacity{0}, submitted{0}, completed{0}, sends{0},
        frees{0}, released{0};
    size_t metadataCharge{0};
    bool callbackDone{false};
  };
  struct Source {
    std::shared_ptr<Write> write;
    UrmaPoolBuffer exported;
    size_t bytes, writeOffset;
    bool localDone{false}, freed{false};
  };
  struct FreeReply {
    uint64_t sequence;
    UrmaPoolBuffer storage;
    bool ready{false};
  };
  struct Received {
    PendingCharge charge;
    uint64_t sequence{0}, address{0};
    size_t length{0};
    bool pull{false};
    std::unique_ptr<IOBuf> data;
    std::shared_ptr<FreeReply> reply;
  };
  UrmaAsyncSocket* owner;
  EventBase* evb;
  UrmaConfig config;
  AsyncSocket::UniquePtr tcp;
  std::shared_ptr<UrmaIoContext> connection;
  std::unique_ptr<PeerCleanup> peer;
  UrmaPoolBuffer initial;
  State state{State::Connecting};
  bool haveHello{false}, receivedAck{false}, ackSent{false};
  bool helloSent{false};
  bool registered{false}, failedFlag{false};
  bool running{false}, delivering{false}, closeAfterWrites{false},
      writeShutdown{false};
  size_t handshakeWrites{0}, activeWr{0};
  AsyncTransport::ReadCallback* reader{nullptr};
  AsyncTransport::BufferCallback* bufferCallback{nullptr};
  std::array<uint8_t, 128> tcpBuffer{};
  std::vector<uint8_t> handshake;
  std::deque<std::shared_ptr<Write>> writes;
  std::map<uint64_t, std::shared_ptr<Source>> sources;
  std::map<uint64_t, std::shared_ptr<Received>> received;
  std::deque<std::shared_ptr<FreeReply>> freeReplies;
  IOBufQueue reads{IOBufQueue::cacheChainLength()};
  std::shared_ptr<RxBudget> rxBudget, readBudget;
  uint64_t nextRecord{1}, nextReceive{1};
  uint32_t sendTimeout{0};
  size_t pendingBytes{0}, queuedCapacity{0}, bytesWritten{0}, bytesRead{0};
  size_t rawWritten{0}, rawRead{0};
  size_t reportedBytes{0}, reportedCapacity{0}, receiveOffers{0};
  bool notifyingBuffer{false}, readActive{false}, timedOut{false};
  Clock::time_point started{Clock::now()}, lastProgress{started};
  Clock::time_point receiveSince{};
  Clock::time_point sendRetry{}, receiveRetry{};
  Executor::KeepAlive<EventBase> drainKeepAlive;
  Clock::time_point drainDeadline{};
  CopyStats copyStats;
  const AsyncSocketException closedError{AsyncSocketException::NOT_OPEN,
                                         "URMA connection closed"};
  const AsyncSocketException allocationError{
      AsyncSocketException::INTERNAL_ERROR, "URMA allocation failed", ENOMEM};

  Impl(UrmaAsyncSocket* o, EventBase* e)
      : AsyncTimeout(e),
        owner(o),
        evb(e),
        config(UrmaInit::getConfig()),
        rxBudget(std::make_shared<RxBudget>(config.max_pending_bytes)),
        readBudget(std::make_shared<RxBudget>(config.max_pending_bytes)) {}
  ~Impl() override {
    cancelTimeout();
    if (tcp) {
      tcp->setReadCB(nullptr);
      tcp->closeNow();
    }
    cleanupOrDefer(std::move(peer));
  }
  bool live() const {
    return state == State::Connecting || state == State::Handshake ||
           state == State::Open;
  }
  bool callbacksAllowed() const { return owner && !owner->getDestroyPending(); }
  template <class F>
  void guarded(F&& f) noexcept {
    auto keep = shared_from_this();
    DestructorGuard guard(owner);
    try {
      f();
    } catch (const std::bad_alloc&) {
      drain(true, allocationError);
    } catch (const std::exception& ex) {
      failed(ex.what());
    }
  }
  template <class F>
  void guardedWrite(WriteCallback* cb, F&& f) noexcept {
    auto keep = shared_from_this();
    DestructorGuard guard(owner);
    bool accepted = false;
    try {
      f(accepted);
    } catch (const std::bad_alloc&) {
      drain(true, allocationError);
      if (!accepted && cb) cb->writeErr(0, allocationError);
    } catch (const std::exception& ex) {
      failed(ex.what());
      if (!accepted && cb) cb->writeErr(0, closedError);
    }
  }
  void connectSuccess() noexcept override {
    guarded([&] { start(); });
  }
  void connectErr(const AsyncSocketException& ex) noexcept override {
    guarded([&] { drain(true, ex); });
  }
  void writeSuccess() noexcept override {
    guarded([&] {
      if (handshakeWrites) --handshakeWrites;
      progress();
    });
  }
  void writeErr(size_t, const AsyncSocketException& ex) noexcept override {
    guarded([&] { drain(true, ex); });
  }
  void getReadBuffer(void** p, size_t* n) override {
    *p = tcpBuffer.data();
    *n = tcpBuffer.size();
  }
  void readDataAvailable(size_t n) noexcept override {
    guarded([&] { consumeHandshake(n); });
  }
  void readEOF() noexcept override {
    guarded([&] { drain(false, closedError); });
  }
  void readErr(const AsyncSocketException& ex) noexcept override {
    guarded([&] { drain(true, ex); });
  }
  std::shared_ptr<RxBudget> receiveBudget() override { return rxBudget; }
  void timeoutExpired() noexcept override {
    guarded([&] { onTimer(); });
  }
  void start();
  void sendHello();
  void consumeHandshake(size_t);
  void finishHandshake();
  void sendHandshake(const void*, size_t);
  void enqueue(WriteCallback*, bool&, std::unique_ptr<IOBuf>, WriteFlags,
               std::vector<size_t> = {});
  void onRecord(const RecordHeader&, std::unique_ptr<IOBuf>, uint64_t) override;
  void onTx(TxCompletionCtx&, bool) override;
  void progress() noexcept override;
  void failed(const char*) noexcept override;
  void ownerStopped() noexcept override {
    timedOut = true;
    drainKeepAlive.reset();
    failed("URMA EventBase stopped; unresolved references retained");
    cancelTimeout();
    detachEventBase();
  }
  void drain(bool, const AsyncSocketException&) noexcept;
  void maybeClosed();
  void onTimer();
  void armTimer();
  void sendProgress();
  bool sendData(const std::shared_ptr<Write>&, const IOBuf*, size_t, size_t);
  bool sendPull(const std::shared_ptr<Write>&, const IOBuf*, size_t, size_t);
  void receiveProgress();
  void freeProgress();
  void finishSource(uint64_t);
  void reclaimWrite(const std::shared_ptr<Write>&);
  void finishWrites();
  void deliver();
  size_t allocatedBytesBuffered() const { return queuedCapacity; }
  void notifyBuffer();
  PostResult submit(std::unique_ptr<TxCompletionCtx>& work) {
    ++activeWr;
    auto result = connection->post(work);
    if (result == PostResult::Full || result == PostResult::Unaccepted)
      --activeWr;
    return result;
  }
};

void UrmaAsyncSocket::Impl::start() {
  if (!live()) return;
  state = State::Handshake;
  peer = std::make_unique<PeerCleanup>();
  peer->value->context = UrmaInit::acquireContext();
  if (!peer->value->context)
    throw std::runtime_error("URMA context unavailable");
  connection = UrmaIoContext::get(evb);
  auto rollback = makeGuard([&] { connection->stopOwner(); });
  for (const auto& write : writes)
    write->charge.reserve(connection->pendingBudget(),
                          write->capacity + write->metadataCharge);
  initial = UrmaInit::initialSegment();
  if (!initial)
    throw std::runtime_error("URMA HELLO initial segment unavailable");
  connection->add(shared_from_this());
  registered = true;
  rollback.dismiss();
  tcp->setReadCB(this);
  progress();
}
void UrmaAsyncSocket::Impl::sendHello() {
  if (helloSent || !connection->ready()) return;
  helloSent = true;
  Hello hello;
  const auto io = connection->endpoint();
  hello.eid = io.eid;
  hello.uasid = io.uasid;
  hello.ioJetty = io.id;
  hello.bufferSize = config.buffer_size;
  hello.receiveCount = connection->io().target;
  hello.socketId = socketId;
  hello.segment = initial.segment()->seg;
  hello.read = config.enable_read && UrmaInit::getMaxReadSize();
  hello.maxRead =
      hello.read
          ? std::min<uint32_t>(
                UrmaInit::getMaxReadSize(),
                std::min<uint64_t>(config.max_pending_bytes,
                                   config.registered_pool.classes.back().bytes))
          : 0;
  auto wire = encodeHello(hello);
  tcp->setReadCB(this);
  sendHandshake(wire.data(), wire.size());
  armTimer();
}
void UrmaAsyncSocket::Impl::sendHandshake(const void* data, size_t bytes) {
  ++handshakeWrites;
  tcp->writeChain(this, IOBuf::copyBuffer(data, bytes));
}
void UrmaAsyncSocket::Impl::consumeHandshake(size_t n) {
  if (state != State::Handshake || handshake.size() + n > kHelloBytes + 4)
    throw std::runtime_error("unexpected URMA TCP handshake bytes");
  handshake.insert(handshake.end(), tcpBuffer.begin(), tcpBuffer.begin() + n);
  if (!haveHello && handshake.size() >= kHelloBytes) {
    Hello hello;
    if (!decodeHello(handshake.data(), kHelloBytes, hello) ||
        hello.bufferSize > 4096)
      throw std::runtime_error("incompatible URMA HELLO 4 / H24 protocol");
    peer->value->hello = hello;
    haveHello = true;
    handshake.erase(handshake.begin(), handshake.begin() + kHelloBytes);
    peer->value->done.store(false, std::memory_order_release);
    try {
      const bool queued = submitPreparation(
          [value = peer->value, signal = connection->signal(),
           read = config.enable_read] { importPeer(value, signal, read); });
      if (!queued) {
        peer->value->done = true;
        throw std::runtime_error("URMA preparation queue full");
      }
    } catch (...) {
      peer->value->done = true;
      throw;
    }
    connection->wait(shared_from_this());
  }
  if (haveHello && !handshake.empty()) {
    if (handshake.size() > 4)
      throw std::runtime_error("unexpected handshake ACK");
    if (handshake.size() == 4) {
      if (receivedAck || getInteger(handshake.data(), 4) != 1)
        throw std::runtime_error("invalid handshake ACK");
      handshake.clear();
      receivedAck = true;
    }
  }
  progress();
}
void UrmaAsyncSocket::Impl::finishHandshake() {
  if (state != State::Handshake || !helloSent || !haveHello ||
      !peer->value->done.load(std::memory_order_acquire))
    return;
  if (!peer->value->ok) throw std::runtime_error("URMA peer import failed");
  // Both peers confirm that HELLO is parsed and imports are ready. Sending
  // a TCP HELLO locally does not ensure the remote has consumed it: URMA
  // DATA/PULL can overtake TCP on the other device path.
  if (!ackSent) {
    ackSent = true;
    uint8_t ack[4];
    putInteger(ack, 1, 4);
    sendHandshake(ack, sizeof(ack));
  }
  if (!handshakeWrites && receivedAck && ackSent) state = State::Open;
}
void UrmaAsyncSocket::Impl::deliver() {
  if (delivering || state != State::Open) return;
  delivering = true;
  auto done = makeGuard([&] { delivering = false; });
  size_t calls = 0;
  while (reader && !reads.empty() && state == State::Open &&
         callbacksAllowed() && calls++ < 32) {
    auto* cb = reader;
    if (cb->isBufferMovable()) {
      cb->readBufferAvailable(reads.move());
    } else {
      void* destination = nullptr;
      size_t capacity = 0;
      cb->getReadBuffer(&destination, &capacity);
      if (!destination || !capacity)
        throw std::runtime_error("empty application read buffer");
      const size_t length = std::min(capacity, reads.front()->length());
      std::memcpy(destination, reads.front()->data(), length);
      reads.trimStart(length);
      // Compatibility copies are separate from registered target transfer.
      copyStats.copied_read_target_bytes += length;
      cb->readDataAvailable(length);
    }
  }
  if (reader && !reads.empty()) {
    connection->wait(shared_from_this());
    connection->wake();
  }
}

void UrmaAsyncSocket::Impl::notifyBuffer() {
  if (notifyingBuffer) return;
  notifyingBuffer = true;
  auto reset = makeGuard([&] { notifyingBuffer = false; });
  while (
      bufferCallback && callbacksAllowed() &&
      (reportedBytes != pendingBytes || reportedCapacity != queuedCapacity)) {
    reportedBytes = pendingBytes;
    reportedCapacity = queuedCapacity;
    auto* cb = bufferCallback;
    if (pendingBytes || queuedCapacity)
      cb->onEgressBuffered();
    else
      cb->onEgressBufferCleared();
  }
}
void UrmaAsyncSocket::Impl::progress() noexcept {
  if (running || state == State::Closed) return;
  auto keep = shared_from_this();
  DestructorGuard guard(owner);
  running = true;
  auto reset = makeGuard([&] { running = false; });
  try {
    if (state == State::Handshake && connection) sendHello();
    finishHandshake();
    if ((state == State::Open || state == State::Draining) && peer &&
        peer->value->done.load(std::memory_order_acquire) && peer->value->ok) {
      freeProgress();
      if (state == State::Open) {
        receiveProgress();
        sendProgress();
      }
      finishWrites();
      deliver();
      if (state == State::Open && writes.empty() &&
          (closeAfterWrites || writeShutdown))
        drain(false, closedError);
    }
    maybeClosed();
    if (connection && state != State::Closed &&
        (state == State::Handshake || state == State::Draining ||
         !writes.empty() || !received.empty() || !freeReplies.empty()))
      connection->wait(shared_from_this());
    armTimer();
  } catch (const std::bad_alloc&) {
    drain(true, allocationError);
  } catch (const std::exception& ex) {
    failed(ex.what());
  }
}
void UrmaAsyncSocket::Impl::failed(const char* message) noexcept {
  auto keep = shared_from_this();
  DestructorGuard guard(owner);
  try {
    const AsyncSocketException ex(AsyncSocketException::INTERNAL_ERROR,
                                  message);
    drain(true, ex);
  } catch (...) {
    drain(true, allocationError);
  }
}
void UrmaAsyncSocket::Impl::drain(bool error,
                                  const AsyncSocketException& ex) noexcept {
  failedFlag |= error;
  if (state == State::Closed || state == State::Draining) return;
  state = State::Draining;
  if (!timedOut) {
    // Public socket destruction can be immediately followed by IO executor
    // stop. Keep the owning EventBase processing READ confirmations and CQEs during
    // its destructor, but never indefinitely on a failed/missing peer.
    drainKeepAlive = getKeepAliveToken(evb);
    drainDeadline = Clock::now() + std::chrono::seconds(5);
  }
  const bool wasRunning = running;
  running = true;
  auto reset = makeGuard([&] { running = wasRunning; });
  if (tcp) {
    tcp->setReadCB(nullptr);
    tcp->closeNow();
  }
  reads.move();
  for (const auto& write : writes) {
    auto* cb = write->callback;
    const bool call = !write->callbackDone;
    write->callbackDone = true;
    write->callback = nullptr;
    if (call && cb) cb->writeErr(write->completed, ex);
  }
  if (auto* cb = std::exchange(reader, nullptr)) {
    if (error)
      cb->readErr(ex);
    else
      cb->readEOF();
  }
  try {
    finishWrites();
    maybeClosed();
    if (connection && state != State::Closed) {
      connection->wait(shared_from_this());
      connection->wake();
    }
    armTimer();
  } catch (...) {
    // Preserve the route and physical pins if even shutdown bookkeeping fails.
    timedOut = true;
    drainKeepAlive.reset();
    cancelTimeout();
  }
}
void UrmaAsyncSocket::Impl::maybeClosed() {
  if (state != State::Draining) return;
  for (auto it = received.begin(); it != received.end();) {
    if (!it->second->pull)
      it = received.erase(it);
    else
      ++it;  // unresolved remote source responsibility cannot be discarded
  }
  if (activeWr || !sources.empty() || readActive || !freeReplies.empty() ||
      !received.empty())
    return;
  finishWrites();
  if (registered) {
    connection->remove(socketId, this);
    registered = false;
  }
  state = State::Closed;
  cancelTimeout();
  drainKeepAlive.reset();
  initial = {};
  if (peer) peer->value->cancelled = true;
  cleanupOrDefer(std::move(peer));
  notifyBuffer();
}
void UrmaAsyncSocket::Impl::armTimer() {
  if (state == State::Closed || timedOut) {
    cancelTimeout();
    return;
  }
  auto deadline = Clock::time_point::max();
  if (state == State::Draining && drainKeepAlive)
    deadline = drainDeadline;
  if (state == State::Connecting || state == State::Handshake)
    deadline = started + std::chrono::milliseconds(config.handshake_timeout_ms);
  if (sendTimeout && !writes.empty())
    deadline = std::min(deadline,
                        lastProgress + std::chrono::milliseconds(sendTimeout));
  if (state == State::Open && !writes.empty() && Clock::now() < sendRetry)
    deadline = std::min(deadline, sendRetry);
  if (state == State::Open && !received.empty() && !readActive &&
      Clock::now() < receiveRetry)
    deadline = std::min(deadline, receiveRetry);
  if (!received.empty())
    deadline = std::min(deadline, receiveSince + std::chrono::seconds(30));
  if (!sources.empty())
    deadline = std::min(deadline, lastProgress + std::chrono::seconds(30));
  if (deadline == Clock::time_point::max())
    cancelTimeout();
  else if (!scheduleTimeout(std::max<int64_t>(
               1, std::chrono::duration_cast<std::chrono::milliseconds>(
                      deadline - Clock::now())
                      .count())))
    throw std::runtime_error("URMA deadline scheduling failed");
}
void UrmaAsyncSocket::Impl::onTimer() {
  const auto now = Clock::now();
  if (state == State::Draining && drainKeepAlive && now >= drainDeadline) {
    // Stop retaining the EventBase, not the physical resources. stopOwner()
    // detaches callbacks; unresolved routes/WRs remain in bounded quarantine.
    timedOut = true;
    drainKeepAlive.reset();
    cancelTimeout();
    return;
  }
  const bool sendExpired = sendTimeout && !writes.empty() &&
      now - lastProgress >= std::chrono::milliseconds(sendTimeout);
  if (((state == State::Connecting || state == State::Handshake) &&
       now - started >=
           std::chrono::milliseconds(config.handshake_timeout_ms)) ||
      sendExpired ||
      (!received.empty() && now - receiveSince >= std::chrono::seconds(30)) ||
      (!sources.empty() && now - lastProgress >= std::chrono::seconds(30))) {
    timedOut = true;
    drainKeepAlive.reset();
    const AsyncSocketException timeout(AsyncSocketException::TIMED_OUT,
        sendExpired ? "URMA send timeout; unresolved device references retained"
                    : "URMA operation deadline exceeded; unresolved device references retained");
    drain(true, timeout);
    return;
  }
  progress();
}

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* e, const SocketAddress& address)
    : impl_(std::make_shared<Impl>(this, e)) {
  impl_->tcp = AsyncSocket::newSocket(e);
  impl_->armTimer();
  impl_->tcp->connect(impl_.get(), address, impl_->config.handshake_timeout_ms);
}
UrmaAsyncSocket::UrmaAsyncSocket(EventBase* e, int fd)
    : UrmaAsyncSocket(AsyncSocket::newSocket(e, NetworkSocket(fd))) {}
UrmaAsyncSocket::UrmaAsyncSocket(AsyncSocket::UniquePtr accepted)
    : impl_(std::make_shared<Impl>(this, accepted->getEventBase())) {
  impl_->tcp = std::move(accepted);
  impl_->guarded([&] { impl_->start(); });
}
UrmaAsyncSocket::~UrmaAsyncSocket() {
  if (const char* enabled = std::getenv("FOLLY_URMA_LOG_STATS");
      enabled && std::strcmp(enabled, "1") == 0) {
    const auto& s = impl_->copyStats;
    std::fprintf(stderr,
        "URMA_COPY_STATS socket_id=%llu direct_send_bytes=%llu "
        "copied_send_bytes=%llu send_wr_submitted=%llu send_sges=%llu "
        "direct_read_source_bytes=%llu transferred_read_target_bytes=%llu "
        "copied_read_target_bytes=%llu read_wr_submitted=%llu "
        "read_submit_calls=%llu max_read_inflight=%llu offers_submitted=%llu "
        "max_offers_inflight=%llu max_receive_offers=%llu "
        "pending_wr=%zu exported_sources=%zu\n",
        static_cast<unsigned long long>(impl_->socketId),
        static_cast<unsigned long long>(s.direct_send_bytes),
        static_cast<unsigned long long>(s.copied_send_bytes),
        static_cast<unsigned long long>(s.send_wr_submitted),
        static_cast<unsigned long long>(s.send_sges),
        static_cast<unsigned long long>(s.direct_read_source_bytes),
        static_cast<unsigned long long>(s.transferred_read_target_bytes),
        static_cast<unsigned long long>(s.copied_read_target_bytes),
        static_cast<unsigned long long>(s.read_wr_submitted),
        static_cast<unsigned long long>(s.read_submit_calls),
        static_cast<unsigned long long>(s.max_read_inflight),
        static_cast<unsigned long long>(s.offers_submitted),
        static_cast<unsigned long long>(s.max_offers_inflight),
        static_cast<unsigned long long>(s.max_receive_offers),
        impl_->activeWr, impl_->sources.size());
  }
  impl_->owner = nullptr;
  impl_->reader = nullptr;
  impl_->bufferCallback = nullptr;
  impl_->drain(false, impl_->closedError);
}
UrmaAsyncSocket::CopyStats UrmaAsyncSocket::getCopyStats() const {
  return impl_->copyStats;
}
void UrmaAsyncSocket::setReadCB(ReadCallback* cb) noexcept {
  impl_->guarded([&] {
    impl_->reader = cb;
    if (!cb) return;
    if (!impl_->live()) {
      impl_->reader = nullptr;
      if (impl_->failedFlag)
        cb->readErr(impl_->closedError);
      else
        cb->readEOF();
    } else
      impl_->deliver();
  });
}
AsyncTransport::ReadCallback* UrmaAsyncSocket::getReadCallback()
    const noexcept {
  return impl_->reader;
}
void UrmaAsyncSocket::write(WriteCallback* cb, const void* p, size_t n,
                            WriteFlags flags) noexcept {
  impl_->guardedWrite(cb, [&](bool& accepted) {
    impl_->enqueue(cb, accepted, IOBuf::copyBuffer(p, n), flags);
  });
}
void UrmaAsyncSocket::writev(WriteCallback* cb, const iovec* v, size_t n,
                             WriteFlags flags) noexcept {
  impl_->guardedWrite(cb, [&](bool& accepted) {
    impl_->enqueue(cb, accepted, IOBuf::wrapIov(v, n), flags);
  });
}
void UrmaAsyncSocket::writeChain(WriteCallback* cb,
                                 std::unique_ptr<IOBuf>&& data,
                                 WriteFlags flags) noexcept {
  impl_->guardedWrite(cb, [&](bool& accepted) {
    impl_->enqueue(cb, accepted, std::move(data), flags);
  });
}
void UrmaAsyncSocket::writeChainWithRecords(WriteCallback* cb,
                                            std::unique_ptr<IOBuf>&& data,
                                            std::vector<size_t> records,
                                            WriteFlags flags) noexcept {
  impl_->guardedWrite(cb, [&](bool& accepted) {
    impl_->enqueue(cb, accepted, std::move(data), flags, std::move(records));
  });
}
EventBase* UrmaAsyncSocket::getEventBase() const { return impl_->evb; }
void UrmaAsyncSocket::close() {
  impl_->guarded([&] {
    impl_->closeAfterWrites = true;
    impl_->progress();
  });
}
void UrmaAsyncSocket::closeNow() {
  impl_->guarded([&] { impl_->drain(false, impl_->closedError); });
}
void UrmaAsyncSocket::shutdownWrite() {
  impl_->guarded([&] {
    impl_->writeShutdown = true;
    impl_->progress();
  });
}
void UrmaAsyncSocket::shutdownWriteNow() { closeNow(); }
bool UrmaAsyncSocket::good() const {
  return impl_->live() && !impl_->closeAfterWrites && !impl_->writeShutdown;
}
bool UrmaAsyncSocket::readable() const { return impl_->live(); }
bool UrmaAsyncSocket::writable() const { return good(); }
bool UrmaAsyncSocket::connecting() const {
  return impl_->state == Impl::State::Connecting ||
         impl_->state == Impl::State::Handshake;
}
bool UrmaAsyncSocket::error() const { return impl_->failedFlag; }
void UrmaAsyncSocket::attachEventBase(EventBase* e) {
  if (e != impl_->evb)
    throw std::logic_error("URMA has a fixed EventBase owner");
}
void UrmaAsyncSocket::detachEventBase() {
  throw std::logic_error("URMA cannot detach EventBase");
}
void UrmaAsyncSocket::setSendTimeout(uint32_t ms) {
  impl_->sendTimeout = ms;
  impl_->lastProgress = Clock::now();
  impl_->armTimer();
}
uint32_t UrmaAsyncSocket::getSendTimeout() const { return impl_->sendTimeout; }
void UrmaAsyncSocket::setBufferCallback(BufferCallback* cb) {
  impl_->bufferCallback = cb;
  impl_->reportedBytes = impl_->pendingBytes;
  impl_->reportedCapacity = impl_->queuedCapacity;
}
size_t UrmaAsyncSocket::getAppBytesBuffered() const {
  return impl_->pendingBytes;
}
size_t UrmaAsyncSocket::getRawBytesBuffered() const {
  return impl_->pendingBytes;
}
size_t UrmaAsyncSocket::getAllocatedBytesBuffered() const {
  return impl_->queuedCapacity;
}
void UrmaAsyncSocket::getLocalAddress(SocketAddress* a) const {
  impl_->tcp->getLocalAddress(a);
}
void UrmaAsyncSocket::getPeerAddress(SocketAddress* a) const {
  impl_->tcp->getPeerAddress(a);
}
void UrmaAsyncSocket::setEorTracking(bool enable) {
  if (enable) throw std::logic_error("URMA EOR unsupported");
}
size_t UrmaAsyncSocket::getAppBytesWritten() const {
  return impl_->bytesWritten;
}
size_t UrmaAsyncSocket::getRawBytesWritten() const { return impl_->rawWritten; }
size_t UrmaAsyncSocket::getAppBytesReceived() const { return impl_->bytesRead; }
size_t UrmaAsyncSocket::getRawBytesReceived() const { return impl_->rawRead; }
bool UrmaAsyncSocket::isUrmaReady() const {
  return impl_->state == Impl::State::Open;
}
}  // namespace folly
#endif
