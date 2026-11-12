/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <dlfcn.h>
#include <fcntl.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaIoContext.h>
#include <folly/memory/IoBufPool.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <new>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "TestUtil.h"
namespace folly::urma_detail {
class UrmaIoContextTestPeer {
 public:
  static void completionReady(UrmaIoContext& owner) {
    // The synthetic CQE has no kernel fd event. Mark real CQ work explicitly;
    // a resource/progress wake alone must no longer poll an unrelated CQ.
    owner.cqPending_ = true;
    owner.wake();
  }
};
}  // namespace folly::urma_detail
using namespace folly;
using namespace folly::urma_detail;
using namespace folly::urma_test;
using namespace std::chrono_literals;
namespace {
std::atomic<unsigned> rejectTx{0}, rejectRx{0}, txRejected{0}, rxRejected{0};
std::atomic<bool> pauseImport{false};
std::atomic<unsigned> importsPaused{0};
std::atomic<unsigned> importsSeen{0}, importsRejected{0};
bool failImport{false};
bool injectUnknownPull{false};
unsigned unknownPullHits{0};
std::atomic<unsigned> jfsCreated{0}, jfrCreated{0}, nonRecordSends{0};
std::atomic<bool> failQueue{false};
std::atomic<unsigned> queuesRejected{0};
std::mutex rnrMutex;
std::map<urma_jfc_t*, std::deque<urma_cr_t>> rnrCompletions;
int rnrType{-1};
unsigned rnrHits{0};
bool rnrForever{false};
bool rnrFanout{false};
bool reverseCompletions{false};
size_t reversedCompletionBatches{0};
std::map<uint64_t, unsigned> rnrAttempts;
size_t maxConcurrentRetries{0};
urma_jfs_t* rnrQueue{nullptr};
uint64_t rnrSequence{0};
std::weak_ptr<UrmaIoContext> rnrOwner;
bool captureRoute{false}, holdReceive{false}, holdRead{false};
bool rejectRead{false}, unknownRead{false}, failReadCompletion{false};
unsigned readAttempts{0};
std::weak_ptr<UrmaRoute> lastPostedRoute;
urma_jfc_t* heldCq{nullptr};
std::set<uint64_t> readIds;
std::deque<urma_cr_t> heldCompletions;
constexpr uint64_t rxSlotBit = uint64_t(1) << 63;
TxCompletionCtx* contextOf(urma_jfs_wr_t* wr) {
  // These fixture WRs are members of owning contexts, not standard-layout
  // objects for offsetof(). Compute the offset from a live instance instead.
  static const auto offset = [] {
    TxCompletionCtx example;
    return reinterpret_cast<uintptr_t>(&example.wr) - reinterpret_cast<uintptr_t>(&example);
  }();
  return reinterpret_cast<TxCompletionCtx*>(reinterpret_cast<uintptr_t>(wr) - offset);
}
uint8_t pattern(size_t i, unsigned seed) {
  return uint8_t((i * 31 + seed) % 251);
}
struct Endpoint : AsyncTransport::ReadCallback,
                  AsyncTransport::WriteCallback,
                  AsyncTransport::BufferCallback {
  struct Completion : AsyncTransport::WriteCallback {
    Endpoint& endpoint;
    size_t index, calls{0};
    Completion(Endpoint& e, size_t i) : endpoint(e), index(i) {}
    void checkOrder() noexcept {
      if (calls++ || index != endpoint.callbacks + endpoint.errors)
        ++endpoint.errors;
    }
    void writeSuccess() noexcept override {
      checkOrder();
      endpoint.writeSuccess();
    }
    void writeErr(size_t n, const AsyncSocketException& e) noexcept override {
      checkOrder();
      endpoint.writeErr(n, e);
    }
  };
  AsyncTransport::UniquePtr socket;
  AsyncSocket* control{nullptr};  // accepted TCP control stream, owned by socket
  std::deque<Completion> completions;
  std::vector<std::unique_ptr<IOBuf>> held;
  std::vector<uint8_t> external;
  std::array<uint8_t, 16384> buffer{};
  size_t sent{0}, received{0}, callbacks{0}, errors{0};
  size_t buffered{0}, cleared{0};
  size_t eofs{0}, bytesAtEof{0};
  unsigned seed{0};
  const uint8_t* lastSource{nullptr};
  bool hold{false}, movable{true}, closing{false};
  std::function<void()> afterRead, afterWrite, onGetBuffer;
  explicit Endpoint(AsyncTransport::UniquePtr s) : socket(std::move(s)) {
    socket->setReadCB(this);
    socket->setSendTimeout(5000);
    transport().setBufferCallback(this);
  }
  ~Endpoint() {
    transport().setBufferCallback(nullptr);
    socket->setReadCB(nullptr);
    socket->closeNow();
  }
  UrmaAsyncSocket& transport() {
    return *static_cast<UrmaAsyncSocket*>(socket.get());
  }
  void writeSuccess() noexcept override {
    ++callbacks;
    if (auto action = std::exchange(afterWrite, {})) action();
  }
  void onEgressBuffered() override {
    ++buffered;
    if (socket->getAllocatedBytesBuffered() < socket->getAppBytesBuffered())
      ++errors;
  }
  void onEgressBufferCleared() override {
    ++cleared;
    if (socket->getAllocatedBytesBuffered() || socket->getAppBytesBuffered())
      ++errors;
  }
  void writeErr(size_t, const AsyncSocketException& e) noexcept override {
    ++errors;
    std::cerr << "write: " << e.what() << '\n';
  }
  void getReadBuffer(void** p, size_t* n) override {
    if (auto action = std::exchange(onGetBuffer, {})) action();
    *p = buffer.data();
    *n = buffer.size();
  }
  bool isBufferMovable() noexcept override { return movable; }
  void checkBytes(const uint8_t* p, size_t n) noexcept {
    for (size_t i = 0; i < n; ++i)
      if (p[i] != pattern(received + i, seed)) {
        std::cerr << "payload/order mismatch at " << received + i << '\n';
        ++errors;
        break;
      }
    received += n;
  }
  void readDataAvailable(size_t n) noexcept override {
    checkBytes(buffer.data(), n);
    if (auto action = std::exchange(afterRead, {})) action();
  }
  void readBufferAvailable(std::unique_ptr<IOBuf> b) noexcept override {
    auto* p = b.get();
    do {
      checkBytes(p->data(), p->length());
      p = p->next();
    } while (p != b.get());
    if (hold)
      held.push_back(b->clone());  // exercise last shared reference, not first
    if (auto action = std::exchange(afterRead, {})) action();
  }
  void readEOF() noexcept override {
    ++eofs;
    bytesAtEof = received;
    if (!closing) ++errors;
  }
  void readErr(const AsyncSocketException& e) noexcept override {
    ++errors;
    std::cerr << "read: " << e.what() << '\n';
  }
  void send(size_t n, unsigned value, bool pool, bool headroom = false) {
    std::unique_ptr<IOBuf> b;
    if (pool) {
      b = UrmaInit::createPoolBuffer(n + (headroom ? 24 : 0),
                                     UrmaMemoryPurpose::ReadSource);
      require(bool(b), "test source allocation");
      if (headroom) b->advance(24);
      for (size_t i = 0; i < n; ++i)
        b->writableData()[i] = pattern(sent + i, value);
      b->append(n);
    } else {
      require(external.empty(), "external test source already in use");
      external.resize(n);
      for (size_t i = 0; i < n; ++i) external[i] = pattern(sent + i, value);
      b = IOBuf::wrapBuffer(external.data(), external.size());
    }
    sent += n;
    lastSource = b->data();
    completions.emplace_back(*this, completions.size());
    socket->writeChain(&completions.back(), std::move(b));
  }
  void sendParts(bool mixed, size_t count = 3, unsigned value = 19,
                 size_t partBytes = 300) {
    auto chain = IOBuf::create(0);
    for (size_t i = 0; i < count; ++i) {
      auto p = UrmaInit::createPoolBuffer(partBytes, UrmaMemoryPurpose::ReadSource);
      require(bool(p), "chain test source");
      if (mixed && i == 2) {
        external.resize(partBytes);
        p = IOBuf::wrapBuffer(external.data(), external.size());
      } else
        p->append(partBytes);
      for (size_t n = 0; n < partBytes; ++n)
        p->writableData()[n] = pattern(sent + n, value);
      sent += partBytes;
      chain->prependChain(std::move(p));
      chain->prependChain(
          IOBuf::create(0));  // empty nodes consume no SGE/bytes
    }
    completions.emplace_back(*this, completions.size());
    transport().writeChainWithRecords(&completions.back(), std::move(chain),
                                      std::vector<size_t>(count, partBytes));
  }
  void sendReadThenData() {
    auto chain = IOBuf::create(0);
    for (size_t n : {size_t(16384), size_t(1024)}) {
      auto b = UrmaInit::createPoolBuffer(n, UrmaMemoryPurpose::ReadSource);
      require(bool(b), "batch prefix fixture allocation");
      for (size_t i = 0; i < n; ++i) b->writableData()[i] = pattern(sent + i, 19);
      b->append(n);
      sent += n;
      chain->prependChain(std::move(b));
    }
    completions.emplace_back(*this, completions.size());
    socket->writeChain(&completions.back(), std::move(chain));
  }
};
struct Listener : AsyncServerSocket::AcceptCallback {
  EventBase& evb;
  std::vector<std::unique_ptr<Endpoint>> peers;
  bool error{false};
  size_t automaticSend{0};
  explicit Listener(EventBase& e) : evb(e) {}
  void connectionAccepted(NetworkSocket fd, const SocketAddress&,
                          AcceptInfo) noexcept override {
    try {
      auto tcp = AsyncSocket::newSocket(&evb, fd);
      auto* control = tcp.get();
      auto p = std::make_unique<Endpoint>(
          AsyncTransport::UniquePtr(new UrmaAsyncSocket(std::move(tcp))));
      p->control = control;
      p->seed = 19;
      if (automaticSend) {
        p->closing = true;
        p->send(automaticSend, 73, true);
      }
      peers.push_back(std::move(p));
    } catch (const std::exception& e) {
      std::cerr << e.what() << '\n';
      error = true;
    }
  }
  void acceptError(const std::exception&) noexcept override { error = true; }
};
template <class F>
void pump(EventBase& evb, F predicate, const char* why) {
  auto end = std::chrono::steady_clock::now() + 6s;
  do {
    evb.loopOnce(EVLOOP_NONBLOCK);
    if (predicate()) return;
    std::this_thread::sleep_for(100us);
  } while (std::chrono::steady_clock::now() < end);
  throw std::runtime_error(why);
}
void shutdownRegression(EventBase& evb, Endpoint& client, Endpoint& server,
                        const std::string& mode) {
  client.socket->setSendTimeout(0);
  server.socket->setSendTimeout(0);
  client.closing = server.closing = true;
  client.seed = 73;
  server.seed = 19;
  captureRoute = true;
  server.send(1, 73, true);
  auto route = lastPostedRoute.lock();
  captureRoute = false;
  require(bool(route), "shutdown fixture did not capture its own route");
  auto owner = route->ioOwner->shared_from_this();
  pump(evb, [&] { return server.callbacks == 1 && client.received == 1; },
       "shutdown fixture warmup");
  heldCq = owner->io().cq;

  if (mode == "eof_tail" || mode == "eof_movable" || mode == "eof_paused") {
    holdReceive = true;
    server.movable = mode == "eof_movable";
    if (mode == "eof_paused") server.socket->setReadCB(nullptr);
    for (size_t i = 0; i < 80; ++i) client.send(300, 19, true);
    pump(evb, [&] { return client.callbacks == 80; }, "tail SEND completions");
    client.socket->closeNow();
    pump(evb, [&] {
      return !server.control->getReadCallback() && heldCompletions.size() == 80;
    }, "TCP FIN was not dispatched ahead of RX CQEs");
    require(!server.eofs && !server.errors && !server.received,
            "EOF overtook the held tail DATA");
    holdReceive = false;
    UrmaIoContextTestPeer::completionReady(*owner);
    if (mode == "eof_paused") {
      pump(evb, [&] { return heldCompletions.empty(); }, "paused tail CQ drain");
      require(!server.eofs && !server.received, "paused reader lost its tail");
      server.socket->setReadCB(&server);
    }
    pump(evb, [&] { return server.eofs == 1; }, "tail EOF never completed");
    require(server.received == 24000 && server.bytesAtEof == 24000 &&
                !server.errors && !client.errors,
            "normal EOF lost or corrupted tail DATA");
  } else if (mode == "eof_read_budget" || mode == "eof_read_reject") {
    auto budget = owner->readBudget();
    const auto limit = budget->limit;
    if (mode == "eof_read_budget") budget->limit = 0;
    else rejectRead = true;
    client.send(16384, 19, true);
    pump(evb, [&] {
      return server.transport().getCopyStats().max_receive_offers == 1 &&
          (mode == "eof_read_budget" || readAttempts != 0);
    }, "PULL did not wait for READ resources");
    // Deliver the TCP boundary/EOF callbacks before any successful READ.
    // Fragment the frame to cover incremental TCP control parsing as well.
    std::array<uint8_t, 12> end{};
    putInteger(end.data(), kMagic, 4);
    putInteger(end.data() + 4, 2, 8);
    auto* cb = server.control->getReadCallback();
    for (size_t offset : {size_t(0), size_t(3)}) {
      void* p; size_t capacity;
      cb->getReadBuffer(&p, &capacity);
      const size_t n = offset ? end.size() - offset : 3;
      require(capacity >= n, "TCP control buffer too small");
      std::memcpy(p, end.data() + offset, n);
      cb->readDataAvailable(n);
    }
    cb->readEOF();
    require(!server.eofs && !server.errors, "EOF cancelled an unsubmitted tail READ");
    budget->limit = limit;
    rejectRead = false;
    owner->wake();
    pump(evb, [&] { return server.eofs == 1 && client.callbacks == 1; },
         "tail READ did not resume after EOF");
    require(server.bytesAtEof == 16384 && !server.errors && !client.errors &&
                server.transport().getCopyStats().read_wr_submitted == 1,
            "tail READ/FREE/EOF ordering failed");
  } else if (mode == "eof_truncated") {
    // A FIN with no boundary cannot be labelled successful stream completion.
    auto* cb = server.control->getReadCallback();
    cb->readEOF();
    require(server.errors == 1 && !server.eofs,
            "unframed TCP EOF was reported as a normal EOF");
  } else {
    const bool budgetBlocked = mode == "drain_read_budget";
    const bool sqBlocked = mode == "drain_read_sq";
    rejectRead = mode == "drain_read_reject";
    unknownRead = mode == "drain_read_unknown";
    failReadCompletion = mode == "drain_read_failed";
    holdRead = !budgetBlocked && !sqBlocked && !rejectRead;
    auto budget = owner->readBudget();
    const auto limit = budget->limit;
    if (budgetBlocked) budget->limit = 0;
    if (sqBlocked) owner->io().inflight = owner->io().sendDepth;
    // The fixture owns the source outright: no remote socket export is
    // cancelled or falsely acknowledged to make local cleanup pass.
    auto source = UrmaInit::createPoolBuffer(16384, UrmaMemoryPurpose::ReadSource);
    require(bool(source), "shutdown READ source");
    auto descriptor = IOBuf::create(4);
    putInteger(descriptor->writableData(), 16384, 4);
    descriptor->append(4);
    route->onRecord({Message::Pull, route->socketId, 1}, std::move(descriptor),
                    reinterpret_cast<uint64_t>(source->data()));
    route->progress();
    if (holdRead) {
      pump(evb, [&] { return !heldCompletions.empty(); }, "READ CQE was not held");
      require(owner->io().inflight == 1, "READ lost its in-flight owner");
      auto& flight = *std::find_if(owner->io().flights.begin(), owner->io().flights.end(),
                                  [](const auto& p) { return bool(p); });
      require(flight && flight->destination && flight->destination->dma,
              "accepted/unknown READ dropped its DMA pin before CQE");
    } else {
      require(!server.transport().getCopyStats().read_wr_submitted &&
                  (rejectRead ? readAttempts == 1 : readAttempts == 0),
              "unsubmitted READ fixture submitted work");
    }
    server.socket->closeNow();
    if (sqBlocked) owner->io().inflight = 0;
    budget->limit = limit;
    rejectRead = false;
    if (holdRead) {
      require(owner->io().inflight == 1 && UrmaIoContext::activeRoutes() != 0,
              "close freed a READ before its CQE");
      holdRead = false;
      UrmaIoContextTestPeer::completionReady(*owner);
    }
    client.socket->closeNow();
    pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; },
         "closed READ retained a route with no future CQE");
    require(!server.received && readIds.empty(), "closed READ was delivered or not settled");
    require(!UrmaInit::poolStats().exported_bytes,
            "shutdown fixture accidentally created a remote source responsibility");
  }
  client.socket->closeNow();
  server.socket->closeNow();
  pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; }, "shutdown cleanup");
  require(heldCompletions.empty(), "shutdown lost removed CQEs");
  heldCq = nullptr;
  readIds.clear();
}
void callbackReentry(EventBase& evb, Endpoint& client, Endpoint& server,
                     const std::string& mode) {
  // Let handshake completions settle before the single triggering write.
  // No transport send timeout, reply timer, or extra traffic may rescue it.
  client.socket->setSendTimeout(0);
  server.socket->setSendTimeout(0);
  const auto settled = std::chrono::steady_clock::now() + 50ms;
  while (std::chrono::steady_clock::now() < settled) {
    evb.loopOnce(EVLOOP_NONBLOCK);
    std::this_thread::sleep_for(100us);
  }
  client.seed = 73;
  server.seed = 19;
  if (mode == "callback_write") {
    client.afterWrite = [&] { client.send(32, 19, true); };
    client.send(32, 19, true);
    pump(evb, [&] { return client.callbacks == 2 && server.received == 64; },
         "writeSuccess continuation stalled without unrelated traffic");
  } else if (mode == "callback_read" || mode == "callback_read_movable") {
    server.movable = mode == "callback_read_movable";
    server.afterRead = [&] { server.send(32, 73, true); };
    client.send(32, 19, true);
    pump(evb, [&] { return client.received == 32 && server.callbacks == 1; },
         "read callback reply stalled without unrelated traffic");
  } else {
    struct Reader : AsyncTransport::ReadCallback {
      std::array<uint8_t, 64> buffer{};
      size_t received{0}, errors{0};
      void getReadBuffer(void** p, size_t* n) override {
        *p = buffer.data();
        *n = buffer.size();
      }
      void readDataAvailable(size_t n) noexcept override {
        for (size_t i = 0; i < n; ++i)
          if (buffer[i] != pattern(received + i, 19)) ++errors;
        received += n;
      }
      void readEOF() noexcept override {}
      void readErr(const AsyncSocketException&) noexcept override { ++errors; }
    } replacement;
    server.movable = false;
    bool called = false;
    server.onGetBuffer = [&] {
      called = true;
      if (mode == "callback_close") {
        client.closing = server.closing = true;
        server.socket->closeNow();
      } else {
        server.socket->setReadCB(
            mode == "callback_replace" ? &replacement : nullptr);
      }
      // Endpoint::getReadBuffer still returns a valid buffer after reentry.
    };
    client.send(32, 19, true);
    pump(evb, [&] { return called; }, "getReadBuffer was not invoked");
    require(!server.received, "obsolete read callback received data");
    if (mode != "callback_close") {
      if (mode == "callback_unregister") {
        for (int i = 0; i < 32; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
        require(!server.received && !replacement.received &&
                    !server.socket->getReadCallback(),
                "unregistered callback still received data");
        server.socket->setReadCB(&replacement);
      }
      pump(evb, [&] { return replacement.received == 32; },
           "replacement reader did not receive retained data");
      require(!server.received && !replacement.errors,
              "reader replacement lost bytes or notified old callback");
    }
    server.socket->setReadCB(nullptr);
  }
  require(!client.errors && !server.errors, "callback reentry failed");
}
void processPeer(bool server, int incoming, int outgoing,
                 const UrmaConfig& config) {
  UrmaInit::init(config);
  {
    EventBase evb;
    Listener accepted(evb);
    std::shared_ptr<AsyncServerSocket> listener;
    std::unique_ptr<Endpoint> client;
    uint16_t port = 0;
    if (server) {
      listener = AsyncServerSocket::newSocket(&evb);
      listener->bind(SocketAddress("127.0.0.1", 0));
      listener->listen(4);
      accepted.automaticSend = 32768;
      listener->addAcceptCallback(&accepted, &evb);
      listener->startAccepting();
      SocketAddress address;
      listener->getAddress(&address);
      port = address.getPort();
      require(::write(outgoing, &port, sizeof(port)) == sizeof(port),
              "peer port pipe write");
    } else {
      pollfd p{incoming, POLLIN, 0};
      require(::poll(&p, 1, 10000) == 1 &&
                  ::read(incoming, &port, sizeof(port)) == sizeof(port),
              "peer port pipe read");
      client = std::make_unique<Endpoint>(
          UrmaAsyncSocket::newSocket(&evb, SocketAddress("127.0.0.1", port)));
      client->seed = 73;
      client->closing = true;
      client->send(16384, 19, true);  // queued before HELLO completes
    }
    Endpoint* peer = client.get();
    pump(
        evb,
        [&] {
          require(!accepted.error, "process accept failed");
          if (server) {
            if (accepted.peers.empty()) return false;
            peer = accepted.peers[0].get();
          }
          require(!peer->errors, "cross-process transport failed");
          return peer->callbacks == 1 &&
                 peer->received == (server ? 16384 : 32768);
        },
        "cross-process registered READ did not finish");
    const auto stats = peer->transport().getCopyStats();
    require(stats.read_wr_submitted == 1 &&
                stats.direct_read_source_bytes == (server ? 32768 : 16384),
            "separate processes did not use their independently imported "
            "initial segments");
    const char done = 1;
    require(::write(outgoing, &done, 1) == 1, "peer completion pipe write");
    require(fcntl(incoming, F_SETFL, O_NONBLOCK) == 0, "pipe nonblocking");
    pump(
        evb,
        [&] {
          char value;
          return ::read(incoming, &value, 1) == 1;
        },
        "peer completion barrier");
    peer->socket->closeNow();
    pump(
        evb, [] { return UrmaIoContext::activeRoutes() == 0; },
        "cross-process drain");
    if (server) stopListener(*listener, accepted, evb);
    client.reset();
    accepted.peers.clear();
  }
  finalize();
}
int separateProcesses(const UrmaConfig& config) {
  // Fork before UrmaInit creates any SDK context or worker. Each child then
  // has its own registered VA, HELLO segment and process-local route registry.
  int a[2], b[2];
  require(pipe(a) == 0 && pipe(b) == 0, "process test pipes");
  const auto pid = fork();
  require(pid >= 0, "fork");
  if (!pid) {
    close(a[0]);
    close(b[1]);
    try {
      processPeer(true, b[0], a[1], config);
      close(b[0]);
      close(a[1]);
      return 0;
    } catch (const std::exception& e) {
      std::cerr << "server: " << e.what() << '\n';
      std::_Exit(1);
    }
  }
  close(a[1]);
  close(b[0]);
  try {
    processPeer(false, a[0], b[1], config);
  } catch (...) {
    kill(pid, SIGTERM);
    waitpid(pid, nullptr, 0);
    throw;
  }
  int status = 0;
  require(waitpid(pid, &status, 0) == pid && WIFEXITED(status) &&
              !WEXITSTATUS(status),
          "server process failed");
  close(a[0]);
  close(b[1]);
  std::cout << "PASS Socket.process two independent processes: HELLO, "
               "bidirectional READ, FREE and shutdown\n";
  return 0;
}
void multipleOwners() {
  std::atomic<uint16_t> port{0};
  std::atomic<unsigned> clientsReady{0}, failures{0};
  std::atomic<bool> serverReady{false};
  auto run = [&](bool server) {
    try {
      EventBase evb;
      Listener accepted(evb);
      std::shared_ptr<AsyncServerSocket> listener;
      std::vector<std::unique_ptr<Endpoint>> clients;
      if (server) {
        listener = AsyncServerSocket::newSocket(&evb);
        listener->bind(SocketAddress("127.0.0.1", 0));
        listener->listen(32);
        accepted.automaticSend = 16384;
        listener->addAcceptCallback(&accepted, &evb);
        listener->startAccepting();
        SocketAddress address;
        listener->getAddress(&address);
        port = address.getPort();
      } else {
        await([&] { return port != 0; }, "threaded server port");
        for (size_t i = 0; i < 4; ++i) {
          auto c = std::make_unique<Endpoint>(UrmaAsyncSocket::newSocket(
              &evb, SocketAddress("127.0.0.1", port)));
          c->seed = 73;
          c->closing = true;
          c->send(32768, 19, true);
          clients.push_back(std::move(c));
        }
      }
      auto& peers = server ? accepted.peers : clients;
      pump(
          evb,
          [&] {
            if (accepted.error || failures)
              throw std::runtime_error("threaded peer failed");
            if (peers.size() != (server ? 12 : 4)) return false;
            for (auto& p : peers) {
              require(!p->errors, "threaded transfer error");
              if (p->callbacks != 1 || p->received != (server ? 32768 : 16384))
                return false;
            }
            return true;
          },
          "four-owner READ/routing");
      if (server)
        serverReady = true;
      else
        ++clientsReady;
      pump(
          evb, [&] { return serverReady && clientsReady == 3; },
          "threaded completion barrier");
      for (auto& p : peers) p->socket->closeNow();
      pump(
          evb, [] { return UrmaIoContext::activeRoutes() == 0; },
          "threaded DRAINING");
      if (server) stopListener(*listener, accepted, evb);
      peers.clear();
    } catch (const std::exception& e) {
      ++failures;
      std::cerr << "owner: " << e.what() << '\n';
    }
  };
  std::vector<std::thread> threads;
  threads.emplace_back(run, true);
  for (int i = 0; i < 3; ++i) threads.emplace_back(run, false);
  for (auto& t : threads) t.join();
  require(!failures, "multi-owner threads failed");
}
}  // namespace
extern "C" urma_jfs_t* urma_create_jfs(urma_context_t* ctx, urma_jfs_cfg_t* cfg) {
  static auto real = reinterpret_cast<decltype(&urma_create_jfs)>(dlsym(RTLD_NEXT, "urma_create_jfs"));
  require(!cfg->flag.bs.outorder_comp,
          "ordered SEND is incompatible with out-of-order completion mode");
  auto* queue = real(ctx, cfg);
  if (queue) ++jfsCreated;
  return queue;
}
extern "C" urma_jfr_t* urma_create_jfr(urma_context_t* ctx, urma_jfr_cfg_t* cfg) {
  static auto real = reinterpret_cast<decltype(&urma_create_jfr)>(dlsym(RTLD_NEXT, "urma_create_jfr"));
  if (failQueue.exchange(false)) {
    ++queuesRejected;
    return nullptr;
  }
  auto* queue = real(ctx, cfg);
  if (queue) ++jfrCreated;
  return queue;
}
extern "C" urma_status_t urma_post_jfs_wr(urma_jfs_t* queue, urma_jfs_wr_t* wr,
                                          urma_jfs_wr_t** bad) {
  static auto real = reinterpret_cast<decltype(&urma_post_jfs_wr)>(
      dlsym(RTLD_NEXT, "urma_post_jfs_wr"));
  if (captureRoute) lastPostedRoute = contextOf(wr)->route;
  if (wr->opcode == URMA_OPC_READ && queue->jfs_cfg.jfc == heldCq) {
    ++readAttempts;
    if (rejectRead) {
      *bad = wr;
      return static_cast<urma_status_t>(-1);
    }
    readIds.insert(wr->user_ctx);
    const auto rc = real(queue, wr, bad);
    if (unknownRead && rc == URMA_SUCCESS) {
      *bad = nullptr;
      return static_cast<urma_status_t>(-1);
    }
    return rc;
  }
  if (wr->opcode == URMA_OPC_SEND || wr->opcode == URMA_OPC_SEND_IMM)
    require(wr->flag.bs.place_order == URMA_STRONG_ORDER &&
                wr->flag.bs.comp_order == 1,
            "DATA/PULL/FREE lost per-connection SEND ordering");
  if (rnrFanout) {
    auto* current = contextOf(wr);
    if (current->rnrSince != std::chrono::steady_clock::time_point{}) {
      size_t active = 1;
      for (const auto& f : current->route->ioOwner->io().flights)
        if (f && f.get() != current && !f->retryPending &&
            f->rnrSince != std::chrono::steady_clock::time_point{}) ++active;
      maxConcurrentRetries = std::max(maxConcurrentRetries, active);
    }
  }
  if (rnrType >= 0 &&
      (wr->opcode == URMA_OPC_SEND || wr->opcode == URMA_OPC_SEND_IMM)) {
    RecordHeader h{};
    if (wr->send.src.num_sge &&
        decodeHeader(reinterpret_cast<const uint8_t*>(wr->send.src.sge[0].addr),
                     wr->send.src.sge[0].len, h) && int(h.type) == rnrType &&
        (!rnrQueue || (queue == rnrQueue && (rnrFanout || h.sequence == rnrSequence))) &&
        (rnrFanout ? rnrAttempts[h.sequence]++ < 2 : (rnrForever || rnrHits < 2))) {
      // Return a real-format RNR CQE without posting to hardware: the peer
      // must not see this attempt. The retry eventually uses the real SDK.
      rnrQueue = queue;
      rnrSequence = h.sequence;
      urma_cr_t c{};
      c.user_ctx = wr->user_ctx;
      c.status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
      std::lock_guard<std::mutex> lock(rnrMutex);
      rnrCompletions[queue->jfs_cfg.jfc].push_back(c);
      ++rnrHits;
      // A synthetic CQE has no kernel interrupt. Schedule the real owner
      // entry explicitly so the fixture models that wakeup as well.
      auto* work = contextOf(wr);
      rnrOwner = work->route->ioOwner->shared_from_this();
      UrmaIoContextTestPeer::completionReady(*work->route->ioOwner);
      return URMA_SUCCESS;
    }
  }
  if (rejectTx && wr->opcode == URMA_OPC_SEND && wr->send.src.num_sge &&
      wr->send.src.sge[0].len >= 24 &&
      getInteger(reinterpret_cast<const uint8_t*>(wr->send.src.sge[0].addr),
                 4) == kMagic &&
      rejectTx.exchange(0)) {
    *bad = wr;
    ++txRejected;
    return static_cast<urma_status_t>(-1);
  }
  if ((wr->opcode == URMA_OPC_SEND || wr->opcode == URMA_OPC_SEND_IMM) &&
      wr->send.src.num_sge && wr->send.src.sge[0].len >= 24 &&
      getInteger(reinterpret_cast<const uint8_t*>(wr->send.src.sge[0].addr), 4) != kMagic)
    ++nonRecordSends;
  const auto result = real(queue, wr, bad);
  if (injectUnknownPull && result == URMA_SUCCESS &&
      wr->opcode == URMA_OPC_SEND_IMM &&
      wr->send.src.sge[0].len == kPullBytes) {
    injectUnknownPull = false;
    ++unknownPullHits;
    *bad = nullptr;
    return static_cast<urma_status_t>(
        -1);  // accepted, but caller cannot prove it
  }
  return result;
}
extern "C" int urma_poll_jfc(urma_jfc_t* cq, int count, urma_cr_t* out) {
  static auto real = reinterpret_cast<decltype(&urma_poll_jfc)>(
      dlsym(RTLD_NEXT, "urma_poll_jfc"));
  int n = 0;
  if (cq == heldCq) {
    while (!holdReceive && !holdRead && n < count && !heldCompletions.empty()) {
      out[n++] = heldCompletions.front();
      heldCompletions.pop_front();
    }
  }
  const int polled = n == count ? 0 : real(cq, count - n, out + n);
  if (polled < 0) return polled;
  n += polled;
  if (cq == heldCq) {
    int delivered = 0;
    for (int i = 0; i < n; ++i) {
      auto c = out[i];
      const bool read = !(c.user_ctx & rxSlotBit) && readIds.count(c.user_ctx);
      if (read && failReadCompletion) c.status = URMA_CR_LOC_ACCESS_ERR;
      if ((holdReceive && (c.user_ctx & rxSlotBit)) || (holdRead && read))
        heldCompletions.push_back(c);
      else {
        if (read) readIds.erase(c.user_ctx);
        out[delivered++] = c;
      }
    }
    n = delivered;
  }
  if (n < 0) return n;
  if (reverseCompletions && n > 1) {
    std::reverse(out, out + n);
    ++reversedCompletionBatches;
  }
  std::lock_guard<std::mutex> lock(rnrMutex);
  auto it = rnrCompletions.find(cq);
  if (it != rnrCompletions.end()) {
    while (n < count && !it->second.empty()) {
      if (rnrFanout) {
        out[n++] = it->second.back();
        it->second.pop_back();
      } else {
        out[n++] = it->second.front();
        it->second.pop_front();
      }
    }
    if (it->second.empty()) rnrCompletions.erase(it);
  }
  return n;
}
extern "C" urma_status_t urma_post_jfr_wr(urma_jfr_t* queue, urma_jfr_wr_t* wr,
                                          urma_jfr_wr_t** bad) {
  static auto real = reinterpret_cast<decltype(&urma_post_jfr_wr)>(
      dlsym(RTLD_NEXT, "urma_post_jfr_wr"));
  if (rejectRx && wr->src.sge[0].len > 24 && rejectRx.exchange(0)) {
    *bad = wr;
    ++rxRejected;
    return static_cast<urma_status_t>(-1);
  }
  return real(queue, wr, bad);
}
extern "C" urma_target_jetty_t* urma_import_jetty(urma_context_t* context,
                                                  urma_rjetty_t* remote,
                                                  urma_token_t* token) {
  static auto real = reinterpret_cast<decltype(&urma_import_jetty)>(
      dlsym(RTLD_NEXT, "urma_import_jetty"));
  if (failImport && importsSeen.fetch_add(1) == 1) {
    ++importsRejected;  // fail after at least one successful endpoint import
    return nullptr;
  }
  if (pauseImport) {
    ++importsPaused;
    while (pauseImport) std::this_thread::sleep_for(1ms);
  }
  return real(context, remote, token);
}
int main(int argc, char** argv) {
  if (!std::getenv("FBTHRIFT_URMA_TEST_DEVICE")) {
    std::cout << "SKIP: FBTHRIFT_URMA_TEST_DEVICE required\n";
    return 77;
  }
  const std::string mode = argc > 1 ? argv[1] : "send";
  const bool shutdownMode = mode.compare(0, 4, "eof_") == 0 ||
      mode.compare(0, 11, "drain_read_") == 0;
  const bool chainMode = mode == "chain" || mode == "chain_single_sge";
  try {
    UrmaConfig config;
    config.dev_name = device();
    config.registered_pool.enabled = config.registered_pool.zero_copy = true;
    config.enable_read = shutdownMode || mode == "read" || mode == "retry" ||
                         mode == "process" || mode == "threads" ||
                         mode == "uncertain" || mode == "new_region" ||
                         mode == "rnr_read" || mode == "rnr_free" ||
                         mode == "rnr_pull_timeout" || mode == "rnr_unknown" ||
                         mode == "rnr_cancel" || mode == "batch_reclaim" ||
                         mode == "rnr_full_duplex";
    config.offer_window = 2;
    if (shutdownMode) config.rx_depth = config.tx_depth = 128;
    if (mode == "chain_single_sge") config.max_send_sge = 1;
    if (mode == "read") config.tx_depth = 2;  // one data/READ slot + FREE reserve
    if (mode == "rnr_full_duplex") config.tx_depth = 2;
    config.max_pending_bytes = mode == "pressure" ? 65536 : 262144;
    if (shutdownMode) config.max_pending_bytes = 2 * 1024 * 1024;
    if (mode == "batch_reclaim") config.max_pending_bytes = 32768;
    if (mode == "new_region") {
      auto& p = config.registered_pool;
      p.pool_size = p.region_bytes = 1024 * 1024;
      p.pool_max_size = 2 * p.pool_size;
      p.budgets.fill(p.pool_max_size);
      p.classes = {{4096, {}}, {16384, {}}, {65536, {}}};
      p.elastic = true;
    }
    if (mode == "process") return separateProcesses(config);
    UrmaInit::init(config);
    if (mode == "tls_alignment") {
      std::vector<std::unique_ptr<IOBuf>> slices;
      for (size_t n : {1, 8, 3, 17, 5, 31, 127}) {
        auto b = IOBuf::create(n);
        require(IoBufPool::owns(*b), "IOBuf::create did not use registered TLS");
        require(reinterpret_cast<uintptr_t>(b->data()) %
                        alignof(std::max_align_t) == 0,
                "IOBuf::create returned a misaligned TLS slice");
        if (n == sizeof(uint64_t)) {
          auto* value = new (b->writableData()) uint64_t(0x123456789abcdef0ULL);
          require(*value == 0x123456789abcdef0ULL, "typed IOBuf access failed");
        }
        slices.push_back(std::move(b));
      }
      slices.clear();
      finalize();
      std::cout << "PASS Socket.tls_alignment IOBuf::create registered TLS alignment\n";
      return 0;
    }
    if (mode == "threads") {
      multipleOwners();
      finalize();
      std::cout << "PASS Socket.threads four EventBases / 12 connections "
                   "bidirectional READ and cleanup\n";
      return 0;
    }
    {
      EventBase evb;
      Listener accepted(evb);
      auto listener = AsyncServerSocket::newSocket(&evb);
      listener->bind(SocketAddress("127.0.0.1", 0));
      listener->listen(128);
      listener->addAcceptCallback(&accepted, &evb);
      listener->startAccepting();
      SocketAddress address;
      listener->getAddress(&address);
      const size_t connections = mode == "multi" ? 12 : 1;
      std::vector<std::unique_ptr<Endpoint>> clients;
      if (mode == "cancel") pauseImport = true;
      if (mode == "prepare_fail") failImport = true;
      if (mode == "queue_fail") failQueue = true;
      for (size_t i = 0; i < connections; ++i)
        clients.push_back(std::make_unique<Endpoint>(
            UrmaAsyncSocket::newSocket(&evb, address)));
      if (mode == "prepare_fail" || mode == "queue_fail") {
        clients[0]->send(32768, 19, true);
        pump(evb, [&] { return (importsRejected || queuesRejected) && clients[0]->errors; },
             "partial import failure was not reported");
        require(clients[0]->completions[0].calls == 1 && !clients[0]->callbacks,
                "preparation failure lost or duplicated the write callback");
        clients.clear();
        accepted.peers.clear();
        pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; },
             "partial import failure leaked routes");
        stopListener(*listener, accepted, evb);
        finalize();
        std::cout << "PASS Socket." << mode << " partial preparation, one write error, "
                     "route removal and context cleanup\n";
        return 0;
      }
      if (mode == "cancel") {
        pump(
            evb, [] { return importsPaused >= 2; },
            "background import never paused");
        clients[0]->closing = accepted.peers[0]->closing = true;
        clients[0]->socket->closeNow();
        accepted.peers[0]->socket->closeNow();
        pump(
            evb, [] { return UrmaIoContext::activeRoutes() == 0; },
            "cancelled handshake routes leaked");
        clients.clear();
        accepted.peers.clear();
        pauseImport = false;
        stopListener(*listener, accepted, evb);
        finalize();
        std::cout << "PASS Socket.cancel paused import, socket destruction and "
                     "late worker cleanup\n";
        return 0;
      }
      pump(
          evb,
          [&] {
            if (accepted.error) throw std::runtime_error("accept failed");
            if (accepted.peers.size() != connections) return false;
            for (size_t i = 0; i < connections; ++i)
              if (!clients[i]->transport().isUrmaReady() ||
                  !accepted.peers[i]->transport().isUrmaReady())
                return false;
            return true;
          },
          "HELLO/import/ACK handshake timed out");
      require(jfsCreated >= 2 * connections && jfrCreated >= 2 * connections,
              "sockets reused a shared JFS/JFR instead of independent queues");
      if (mode == "retry") rejectTx = rejectRx = 1;
      if (shutdownMode) {
        shutdownRegression(evb, *clients[0], *accepted.peers[0], mode);
        stopListener(*listener, accepted, evb);
        clients.clear(); accepted.peers.clear();
        finalize();
        std::cout << "PASS Socket." << mode << " shutdown ownership, tail delivery and cleanup\n";
        return 0;
      }
      if (mode.compare(0, 9, "callback_") == 0) {
        callbackReentry(evb, *clients[0], *accepted.peers[0], mode);
        clients[0]->closing = accepted.peers[0]->closing = true;
        clients[0]->socket->closeNow();
        accepted.peers[0]->socket->closeNow();
        pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; },
             "callback reentry cleanup");
        stopListener(*listener, accepted, evb);
        clients.clear();
        accepted.peers.clear();
        finalize();
        std::cout << "PASS Socket." << mode
                  << " timeout-free idle callback reentry and cleanup\n";
        return 0;
      }
      if (mode == "completion_order") {
        auto& client = *clients[0];
        auto& server = *accepted.peers[0];
        server.seed = 19;
        reverseCompletions = true;
        for (int i = 0; i < 8; ++i) client.send(300, 19, true);
        // Accumulate real hardware completions before presenting the batch
        // backwards. Both bytes and whole-write callbacks must stay ordered.
        std::this_thread::sleep_for(20ms);
        pump(evb, [&] {
          require(!client.errors && !server.errors, "out-of-order CQE handling failed");
          return client.callbacks == 8 && server.received == 2400;
        }, "reversed completion batch did not drain");
        require(reversedCompletionBatches != 0, "fixture did not reverse a real CQE batch");
        client.closing = server.closing = true;
        client.socket->closeNow(); server.socket->closeNow();
        pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; }, "reversed completion cleanup");
        clients.clear(); accepted.peers.clear();
        stopListener(*listener, accepted, evb); finalize();
        std::cout << "PASS Socket.completion_order reversed real CQEs, ordered bytes/callbacks\n";
        return 0;
      }
      if (mode == "rnr_fanout") {
        rnrFanout = true;
        rnrType = int(Message::Data);
        auto& client = *clients[0];
        auto& server = *accepted.peers[0];
        server.seed = 19;
        // Pause owner dispatch until eight distinct WRs have all been rejected.
        for (int i = 0; i < 8; ++i) client.send(300, 19, true);
        pump(evb, [&] {
          require(!client.errors && !server.errors, "RNR burst recovery failed");
          return client.callbacks == 8 && server.received == 2400;
        }, "rejected SEND burst did not recover");
        require(rnrHits == 16 && maxConcurrentRetries == 1,
                "RNR recovery replayed a burst or skipped injected rejections");
        client.closing = server.closing = true;
        client.socket->closeNow(); server.socket->closeNow();
        pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; }, "RNR burst cleanup");
        clients.clear(); accepted.peers.clear();
        stopListener(*listener, accepted, evb); finalize();
        std::cout << "PASS Socket.rnr_fanout eight rejected records, serial retries, ordered completion\n";
        return 0;
      }
      if (mode == "rnr_full_duplex") {
        auto& client = *clients[0];
        auto& server = *accepted.peers[0];
        client.seed = 73;
        server.seed = 19;
        rnrType = int(Message::Data);
        rnrForever = true;
        client.send(1024, 19, true);
        pump(evb, [&] { return rnrHits >= 2; }, "outgoing SEND did not reach RNR");
        server.send(16384, 73, true);
        const auto deadline = std::chrono::steady_clock::now() + 1s;
        pump(evb, [&] {
          require(std::chrono::steady_clock::now() < deadline,
                  "outgoing SEND RNR blocked incoming READ/FREE progress");
          require(!client.errors && !server.errors, "full duplex RNR failed");
          return client.received == 16384 && server.callbacks == 1;
        }, "reverse READ did not complete while SEND was rejected");
        require(!client.callbacks && !server.received,
                "rejected SEND was prematurely delivered/completed");
        rnrForever = false;
        pump(evb, [&] { return client.callbacks == 1 && server.received == 1024; },
             "SEND retry did not recover after reverse READ");
        client.closing = server.closing = true;
        client.socket->closeNow();
        server.socket->closeNow();
        pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; }, "duplex cleanup");
        clients.clear();
        accepted.peers.clear();
        stopListener(*listener, accepted, evb);
        finalize();
        std::cout << "PASS Socket.rnr_full_duplex READ/FREE progress with rejected outbound SEND\n";
        return 0;
      }
      if (mode == "batch_reclaim") {
        auto& client = *clients[0];
        auto& server = *accepted.peers[0];
        rnrType = int(Message::Data);
        rnrForever = true;
        client.sendReadThenData();
        pump(evb, [&] {
          return server.received == 16384 &&
              client.socket->getAppBytesBuffered() == 1024 && rnrHits >= 2;
        }, "completed READ prefix remained charged behind pending SEND");
        require(!client.callbacks && client.socket->getAllocatedBytesBuffered() == 4096 &&
                    !UrmaInit::poolStats().exported_bytes,
                "prefix callback fired early or completed pool storage stayed held");
        // This new write cannot fit under the same 32KiB cap if the completed
        // 16KiB prefix remains charged until the whole original write ends.
        client.send(16384, 19, true);
        rnrForever = false;
        pump(evb, [&] {
          require(!client.errors && !server.errors, "batch reclamation failed");
          return client.callbacks == 2 && server.received == 33792;
        }, "reclaimed batch did not preserve byte/callback order");
        client.closing = server.closing = true;
        client.socket->closeNow();
        server.socket->closeNow();
        pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; }, "batch cleanup");
        clients.clear();
        accepted.peers.clear();
        stopListener(*listener, accepted, evb);
        finalize();
        std::cout << "PASS Socket.batch_reclaim completed prefix returned before whole-write callback\n";
        return 0;
      }
      if (mode.compare(0, 4, "rnr_") == 0) {
        rnrType = int(mode == "rnr_free" ? Message::Free :
                      config.enable_read ? Message::Pull : Message::Data);
        rnrForever = mode == "rnr_timeout" || mode == "rnr_pull_timeout";
      }
      if (mode == "rnr_cancel") {
        auto& client = *clients[0];
        client.closing = accepted.peers[0]->closing = true;
        client.send(32768, 19, true);
        pump(evb, [] {
          if (auto owner = rnrOwner.lock())
            for (const auto& flight : owner->io().flights)
              if (flight && flight->retryPending) return true;
          return false;
        }, "no rejected WR pending at cancellation");
        rnrOwner.lock()->stopOwner();
        require(client.completions[0].calls == 1 && !client.callbacks &&
                    !UrmaInit::poolStats().exported_bytes,
                "owner shutdown duplicated callback or retained rejected source");
        accepted.peers[0]->socket->closeNow();
        pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; },
             "rejected WR cancellation left routes");
        clients.clear();
        accepted.peers.clear();
        stopListener(*listener, accepted, evb);
        finalize();
        std::cout << "PASS Socket.rnr_cancel rejected PULL settled on owner shutdown\n";
        return 0;
      }
      if (rnrForever) {
        auto& client = *clients[0];
        auto& server = *accepted.peers[0];
        client.socket->setSendTimeout(0); // exercise the transport retry bound
        client.closing = server.closing = true;
        const auto retryStarted = std::chrono::steady_clock::now();
        client.send(32768, 19, true);
        client.send(300, 19, true); // later CQE wakes the interrupted queue
        pump(evb, [&] { return client.errors != 0; }, "RNR retry did not time out");
        require(std::chrono::steady_clock::now() - retryStarted >= 5s &&
                    rnrHits >= 2 && !client.callbacks && !server.received &&
                    client.completions.size() == 2 &&
                    client.completions[0].calls == 1 && client.completions[1].calls == 1 &&
                    !UrmaInit::poolStats().exported_bytes,
                "RNR timeout reordered bytes, duplicated callback or retained rejected PULL");
        client.socket->closeNow();
        server.socket->closeNow();
        pump(evb, [] { return UrmaIoContext::activeRoutes() == 0; },
             "definite RNR timeout left unresolved routes");
        clients.clear();
        accepted.peers.clear();
        stopListener(*listener, accepted, evb);
        finalize();
        std::cout << "PASS Socket." << mode << " bounded retry, one error per write, "
                     "no unordered delivery, rejected source reclaimed\n";
        return 0;
      }
      if (mode == "uncertain" || mode == "rnr_unknown") {
        clients[0]->closing = accepted.peers[0]->closing = true;
        injectUnknownPull = true;
        clients[0]->send(32768, 19, true);
        pump(
            evb, [&] { return unknownPullHits && clients[0]->errors; },
            "ambiguous PULL hook not exercised");
        const auto* source = clients[0]->lastSource;
        clients[0]->socket->closeNow();
        accepted.peers[0]->socket->closeNow();
        const auto until = std::chrono::steady_clock::now() + 100ms;
        pump(
            evb, [&] { return std::chrono::steady_clock::now() >= until; },
            "ambiguous acceptance settlement");
        require(clients[0]->completions[0].calls == 1 &&
                    clients[0]->callbacks == 0 &&
                    UrmaInit::poolStats().exported_bytes >= 32768 &&
                    UrmaIoContext::activeRoutes(),
                "uncertain exported source was reported successful or "
                "prematurely released");
        if (mode == "rnr_unknown")
          require(rnrHits == 2, "ambiguous retry replayed or skipped RNR fixture");
        clients.clear();
        accepted.peers.clear();
        stopListener(*listener, accepted, evb);
        require(
            source[0] == pattern(0, 19) && source[32767] == pattern(32767, 19),
            "quarantined source lost storage");
        UrmaInit::fini();
        require(UrmaInit::isInitialized() && UrmaInit::pendingCleanupCount(),
                "unknown remote READ was force-freed");
        std::cout
            << "PASS Socket." << mode << ": one error callback, exported "
               "source/context deliberately retained; no forced cleanup\n";
        return 0;
      }
      std::vector<UrmaPoolBuffer> initialPressure;
      if (mode == "new_region") {
        while (auto b = UrmaInit::poolAlloc(65536, UrmaMemoryPurpose::ReadSource))
          initialPressure.push_back(std::move(b));
        pump(evb, [&] { return UrmaInit::poolStats().registered_bytes > config.registered_pool.pool_size; },
             "real SDK growth did not publish");
      }
      for (size_t i = 0; i < connections; ++i) {
        clients[i]->seed = unsigned(73 + i);
        accepted.peers[i]->seed = unsigned(19 + i);
        accepted.peers[i]->hold = mode == "pressure" || mode == "read";
        clients[i]->movable =
            false;  // compatibility copy in the reverse direction
        if (mode == "new_region") {
          clients[i]->send(32768, unsigned(19 + i), true);
          auto segment = UrmaInit::lookupSegment(clients[i]->lastSource, 32768);
          require(segment && segment.segment() != UrmaInit::initialSegment().segment() &&
                      !UrmaInit::holdReadExport(clients[i]->lastSource, 32768),
                  "new-region source fixture used HELLO segment or allowed export");
        } else if (chainMode) {
          clients[i]->sendParts(false);
          clients[i]->sendParts(true);
          // SGE overflow copies into one registered SEND, in both directions.
          clients[i]->sendParts(false, 7);
          accepted.peers[i]->sendParts(false, 7, unsigned(73 + i));
          // Packet-size fragmentation still applies: 1 KiB needs one WR;
          // 4 KiB needs two with a 24-byte DATA header in each packet.
          clients[i]->sendParts(false, 8, unsigned(19 + i), 128);
          clients[i]->sendParts(false, 16, unsigned(19 + i), 256);
        } else if (mode == "pressure")
          clients[i]->send(65536, unsigned(19 + i), false);
        else {
          clients[i]->send(1024, unsigned(19 + i), true, true);
          clients[i]->send(mode == "read" ? 32768 : 12288, unsigned(19 + i),
                           true);
          clients[i]->send(300, unsigned(19 + i), false);
          if (mode != "read")
            accepted.peers[i]->send(16384, unsigned(73 + i), true);
        }
      }
      if (mode == "pressure") {
        auto& server = *accepted.peers[0];
        pump(
            evb, [&] { return server.received >= 60000; },
            "pressure stage never reached retained limit");
        auto until = std::chrono::steady_clock::now() + 100ms;
        pump(
            evb, [&] { return std::chrono::steady_clock::now() >= until; },
            "pressure observation");
        require(server.received < 65536,
                "retained receive budget was exceeded");
        pump(evb, [&] { return clients[0]->callbacks == 1; },
             "first SEND completion missing under held receive pressure");
        clients[0]->send(65536, 19, true);
        // Reuse/wrap RX slots while returning only part of the held data.
        // Later completed slots must not starve an earlier stream record.
        const auto untilWrap = std::chrono::steady_clock::now() + 2s;
        while (server.received < 131072 && std::chrono::steady_clock::now() < untilWrap) {
          if (!server.held.empty()) server.held.erase(server.held.begin());
          evb.loopOnce(EVLOOP_NONBLOCK);
          std::this_thread::sleep_for(1ms);
        }
        require(!server.errors && server.received == 131072,
                "RX slot wrap/partial budget return stalled or reordered data");
        server.hold = false;
        std::thread release(
            [old = std::move(server.held)]() mutable { old.clear(); });
        release.join();
      }
      pump(
          evb,
          [&] {
            bool done = true;
            for (size_t i = 0; i < connections; ++i) {
              auto& client = *clients[i];
              auto& server = *accepted.peers[i];
              require(!client.errors && !server.errors,
                      "socket reported an error");
              done &= client.received == server.sent &&
                      server.received == client.sent &&
                      client.callbacks == (mode == "pressure" ? 2 : mode == "new_region" ? 1
                                           : chainMode        ? 5
                                                              : 3) &&
                      server.callbacks ==
                          (mode == "pressure" || mode == "new_region" || mode == "read" ? 0 : 1);
            }
            return done;
          },
          "ordered bytes/callbacks did not complete");
      if (mode == "retry")
        require(txRejected == 1 && rxRejected == 1,
                "explicitly unaccepted TX/RX hooks were not both exercised");
      if (rnrType >= 0)
        require(rnrHits == 2, "RNR fault/retry path was not exercised twice");
      for (size_t i = 0; i < connections; ++i) {
        auto a = clients[i]->transport().getCopyStats();
        auto b = accepted.peers[i]->transport().getCopyStats();
        if (mode == "new_region")
          require(a.direct_send_bytes == 32768 && !a.direct_read_source_bytes && !b.read_wr_submitted,
                  "new-region input did not fall back to registered SEND");
        if (mode == "read")
          require(a.direct_read_source_bytes == 32768 &&
                      b.read_wr_submitted == 1 &&
                      b.transferred_read_target_bytes == 32768 &&
                      b.copied_read_target_bytes == 0 &&
                      !accepted.peers[i]->held.empty() &&
                      UrmaInit::poolStats().exported_bytes == 0,
                  "one-way READ/FREE must "
                  "release source before the retained target");
        if (mode == "send")
          require(a.direct_send_bytes == 13312 && a.copied_send_bytes == 300,
                  "SEND direct/copy selection was not exercised");
        if (mode == "chain") {
          require(a.send_wr_submitted == 6 && a.send_sges == 10 &&
                      a.direct_send_bytes == 924 && a.copied_send_bytes == 8096,
                  "small chains were split at the SGE limit instead of coalesced");
          require(b.send_wr_submitted == 1 && b.send_sges == 1 &&
                      b.direct_send_bytes == 0 && b.copied_send_bytes == 2100,
                  "registered reply chain was not coalesced into one SEND");
        }
        if (mode == "chain_single_sge")
          require(a.send_wr_submitted == 6 && a.send_sges == 6 &&
                      a.direct_send_bytes == 0 && a.copied_send_bytes == 9020 &&
                      b.send_wr_submitted == 1 && b.send_sges == 1 &&
                      b.direct_send_bytes == 0 && b.copied_send_bytes == 2100,
                  "single-SGE compatibility fallback stalled or lost bytes");
        require(clients[i]->socket->getAppBytesBuffered() == 0 &&
                    clients[i]->buffered && clients[i]->cleared,
                "write accounting/observer contract");
        clients[i]->closing = accepted.peers[i]->closing = true;
        clients[i]->socket->closeNow();
        accepted.peers[i]->socket->closeNow();
      }
      if (mode != "owner_exit")
        pump(
            evb, [] { return UrmaIoContext::activeRoutes() == 0; },
            "DRAINING connection operations did not settle");
      stopListener(*listener, accepted, evb);
      clients.clear();
      accepted.peers.clear();
      initialPressure.clear();
    }
    require(nonRecordSends == 0, "unexpected credit/control SEND on wire");
    finalize();
    std::cout << "PASS Socket." << mode
              << " bytes, callbacks, path counters, drain and final cleanup\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "FAIL Socket." << mode << ": " << e.what() << '\n';
    std::_Exit(1);
  }
}
