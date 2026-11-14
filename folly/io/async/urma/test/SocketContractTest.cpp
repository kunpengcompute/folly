/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include "TestUtil.h"
#include <functional>
#include <utility>
using namespace folly::urma_detail;
using namespace std::chrono_literals;
namespace {
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
  std::array<uint8_t, 16384> buffer{};
  size_t sent{0}, received{0}, callbacks{0}, errors{0};
  size_t eofs{0}, bytesAtEof{0}, readCallbacks{0};
  unsigned seed{0};
  bool movable{true}, closing{false};
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
    if (socket->getAllocatedBytesBuffered() < socket->getAppBytesBuffered())
      ++errors;
  }
  void onEgressBufferCleared() override {
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
    ++readCallbacks;
    checkBytes(buffer.data(), n);
    if (auto action = std::exchange(afterRead, {})) action();
  }
  void readBufferAvailable(std::unique_ptr<IOBuf> b) noexcept override {
    ++readCallbacks;
    auto* p = b.get();
    do {
      checkBytes(p->data(), p->length());
      p = p->next();
    } while (p != b.get());
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
  void send(size_t n, unsigned value) {
    auto b = UrmaInit::createPoolBuffer(n);
    require(bool(b), "test source allocation");
    for (size_t i = 0; i < n; ++i)
      b->writableData()[i] = pattern(sent + i, value);
    b->append(n);
    sent += n;
    completions.emplace_back(*this, completions.size());
    socket->writeChain(&completions.back(), std::move(b));
  }
};
struct Listener : AsyncServerSocket::AcceptCallback {
  EventBase& evb;
  std::vector<std::unique_ptr<Endpoint>> peers;
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
      peers.push_back(std::move(p));
    } catch (const std::exception& e) {
      std::cerr << e.what() << '\n';
    }
  }
  void acceptError(const std::exception& e) noexcept override {
    std::cerr << e.what() << '\n';
  }
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
    client.afterWrite = [&] { client.send(32, 19); };
    client.send(32, 19);
    pump(evb, [&] { return client.callbacks == 2 && server.received == 64; },
         "writeSuccess continuation stalled without unrelated traffic");
  } else if (mode == "callback_read" || mode == "callback_read_movable") {
    server.movable = mode == "callback_read_movable";
    server.afterRead = [&] { server.send(32, 73); };
    client.send(32, 19);
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
    client.send(32, 19);
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

void feedEnd(Endpoint& server, uint64_t sequence, bool eof = true) {
  auto end = encodeEnd(sequence);
  auto* cb = server.control->getReadCallback();
  require(cb, "missing TCP control reader");
  for (size_t offset : {size_t(0), size_t(3)}) {
    void* p; size_t capacity;
    cb->getReadBuffer(&p, &capacity);
    const size_t n = offset ? end.size() - offset : 3;
    require(capacity >= n, "TCP control buffer too small");
    std::memcpy(p, end.data() + offset, n);
    cb->readDataAvailable(n);
  }
  if (eof) cb->readEOF();
}
void boundaryContract(EventBase& evb, Endpoint& client, Endpoint& server,
                      const std::string& mode) {
  client.closing = server.closing = true;
  auto& owner = UrmaIoContext::get(&evb);
  if (mode == "eof_missing_tail") {
    feedEnd(server, 2);
    const auto deadline = std::chrono::steady_clock::now() + 32s;
    while (!server.errors && std::chrono::steady_clock::now() < deadline) {
      evb.loopOnce(EVLOOP_NONBLOCK);
      std::this_thread::sleep_for(1ms);
    }
    require(server.errors == 1 && !server.eofs,
            "missing final DATA waited forever or produced a clean EOF");
    return;
  }
  if (mode == "eof_truncated") {
    server.control->getReadCallback()->readEOF();
    require(server.errors == 1 && !server.eofs,
            "unframed TCP EOF was reported as successful");
    return;
  }
  if (mode == "eof_duplicate") {
    feedEnd(server, 1, false);
    feedEnd(server, 1, false);
    require(server.errors == 1 && !server.eofs, "duplicate END was accepted");
    return;
  }
  if (mode == "eof_read_budget" || mode == "eof_read_reject") {
    std::vector<std::shared_ptr<void>> budget;
    if (mode == "eof_read_budget") {
      budget = holdReadSlots(owner);
    } else blockReadPosts = true;
    const auto pulls = checkedPulls;
    const auto attempts = readPostCalls;
    client.send(16384, 19);
    pump(evb, [&] { return checkedPulls > pulls &&
        (!budget.empty() || readPostCalls > attempts); }, "tail PULL did not wait");
    feedEnd(server, 2);
    require(!server.eofs && !server.errors && !server.received,
            "EOF cancelled an unsubmitted tail READ");
    budget.clear(); blockReadPosts = false;
    owner.poll();
    pump(evb, [&] { return server.eofs == 1 && client.callbacks == 1; },
         "tail READ did not resume after EOF");
    require(server.bytesAtEof == 16384 && !server.errors,
            "tail READ/FREE/EOF ordering failed");
    return;
  }
  holdDataCompletions = true;
  server.movable = mode == "eof_movable" || mode == "delivery_batch";
  if (mode == "eof_paused") server.socket->setReadCB(nullptr);
  for (size_t i = 0; i < 80; ++i) client.send(300, 19);
  pump(evb, [&] { return client.callbacks == 80 && heldData.size() == 80; },
       "tail DATA SEND completions");
  client.socket->shutdownWrite();
  pump(evb, [&] { return !server.control->getReadCallback(); },
       "FIN did not precede held RX completions");
  require(!server.eofs && !server.errors && !server.received,
          "EOF overtook the held tail DATA");
  holdDataCompletions = false;
  if (mode == "delivery_batch") completeHeld(heldData);
  else completeHeldReverse(heldData);
  owner.poll();
  if (mode == "delivery_close") {
    require(!server.received && !server.eofs,
            "application delivery ran inside CQ dispatch");
    server.socket->closeNow();
    for (int i = 0; i < 32; ++i) evb.loopOnce(EVLOOP_NONBLOCK);
    require(!server.readCallbacks && server.eofs == 1 && !server.errors,
            "queued delivery survived close or duplicated EOF");
    return;
  }
  if (mode == "eof_paused") {
    require(!server.eofs && !server.received, "paused reader lost its tail");
    server.socket->setReadCB(&server);
  }
  pump(evb, [&] { return server.eofs == 1; }, "tail EOF never completed");
  require(server.received == 24000 && server.bytesAtEof == 24000 &&
              !server.errors && !client.errors,
          "normal EOF lost or corrupted tail DATA");
  if (mode == "delivery_batch")
    require(server.readCallbacks == 1,
            "ready DATA prefix was delivered one CQE at a time");
  // Receiving FIN must not close our own write side.
  client.seed = 73;
  server.send(32, 73);
  pump(evb, [&] { return client.received == 32 && server.callbacks == 1; },
       "peer half-close disabled the reverse write direction");
}
} // namespace
int main(int argc, char** argv) {
  if (argc != 2) return 2;
  const std::string mode = argv[1];
  try {
    UrmaConfig c;
    c.rx_depth = 128; c.rx_target = 96; c.tx_depth = 32;
    c.max_pending_bytes = 2 * 1024 * 1024;
    UrmaInit::init(c);
    {
      EventBase evb;
      Listener accepted(evb);
      SocketAddress address;
      auto listener = listenOnLoopback(evb, accepted, address, 4);
      {
        Endpoint client(UrmaAsyncSocket::newSocket(&evb, address));
        pump(evb, [&] { return !accepted.peers.empty() &&
            client.transport().isUrmaReady() &&
            accepted.peers[0]->transport().isUrmaReady(); }, "handshake");
        auto& server = *accepted.peers[0];
        client.seed = 73; server.seed = 19;
        if (mode.rfind("callback_", 0) == 0)
          callbackReentry(evb, client, server, mode);
        else boundaryContract(evb, client, server, mode);
        client.closing = server.closing = true;
        client.socket->closeNow(); server.socket->closeNow();
        accepted.peers.clear();
      }
      listener->stopAccepting();
      listener.reset();
      UrmaIoContext::release(&evb);
    }
    UrmaInit::fini();
    require(urma_detail::waitForCleanups(2s) && !UrmaInit::isInitialized() &&
                !registrations && jfsCreated == jfsDeleted &&
                jfrCreated == jfrDeleted && importsCreated == importsDeleted,
            "socket contract leaked owner resources");
    std::cout << "PASS Socket." << mode << "\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << '\n';
    return 1;
  }
}
