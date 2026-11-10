/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <dlfcn.h>
#include <fcntl.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaIoContext.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "TestUtil.h"
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
  std::deque<Completion> completions;
  std::vector<std::unique_ptr<IOBuf>> held;
  std::vector<uint8_t> external;
  std::array<uint8_t, 16384> buffer{};
  size_t sent{0}, received{0}, callbacks{0}, errors{0};
  size_t buffered{0}, cleared{0};
  unsigned seed{0};
  const uint8_t* lastSource{nullptr};
  bool hold{false}, movable{true}, closing{false};
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
  void writeSuccess() noexcept override { ++callbacks; }
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
  }
  void readBufferAvailable(std::unique_ptr<IOBuf> b) noexcept override {
    auto* p = b.get();
    do {
      checkBytes(p->data(), p->length());
      p = p->next();
    } while (p != b.get());
    if (hold)
      held.push_back(b->clone());  // exercise last shared reference, not first
  }
  void readEOF() noexcept override {
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
  void sendParts(bool mixed, size_t count = 3, unsigned value = 19) {
    auto chain = IOBuf::create(0);
    for (size_t i = 0; i < count; ++i) {
      auto p = UrmaInit::createPoolBuffer(300, UrmaMemoryPurpose::ReadSource);
      require(bool(p), "chain test source");
      if (mixed && i == 2) {
        external.resize(300);
        p = IOBuf::wrapBuffer(external.data(), external.size());
      } else
        p->append(300);
      for (size_t n = 0; n < 300; ++n)
        p->writableData()[n] = pattern(sent + n, value);
      sent += 300;
      chain->prependChain(std::move(p));
      chain->prependChain(
          IOBuf::create(0));  // empty nodes consume no SGE/bytes
    }
    completions.emplace_back(*this, completions.size());
    transport().writeChainWithRecords(&completions.back(), std::move(chain),
                                      std::vector<size_t>(count, 300));
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
      auto p = std::make_unique<Endpoint>(
          AsyncTransport::UniquePtr(new UrmaAsyncSocket(&evb, fd.toFd())));
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
      work->route->ioOwner->wake();
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
  int n = real(cq, count, out);
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
  const bool chainMode = mode == "chain" || mode == "chain_single_sge";
  try {
    UrmaConfig config;
    config.dev_name = device();
    config.registered_pool.enabled = config.registered_pool.zero_copy = true;
    config.enable_read = mode == "read" || mode == "retry" ||
                         mode == "process" || mode == "threads" ||
                         mode == "uncertain" || mode == "new_region" ||
                         mode == "rnr_read" || mode == "rnr_free" ||
                         mode == "rnr_pull_timeout" || mode == "rnr_unknown" ||
                         mode == "rnr_cancel" || mode == "batch_reclaim" ||
                         mode == "rnr_full_duplex";
    config.offer_window = 2;
    if (mode == "chain_single_sge") config.max_send_sge = 1;
    if (mode == "read") config.tx_depth = 2;  // one data/READ slot + FREE reserve
    if (mode == "rnr_full_duplex") config.tx_depth = 2;
    config.max_pending_bytes = mode == "pressure" ? 65536 : 262144;
    if (mode == "batch_reclaim") config.max_pending_bytes = 32768;
    if (mode == "new_region") {
      auto& p = config.registered_pool;
      p.pool_size = p.region_bytes = 1024 * 1024;
      p.pool_max_size = 2 * p.pool_size;
      p.budgets.fill(p.pool_max_size);
      p.classes = {{4096, {}}, {16384, {}}, {65536, {}}};
      p.elastic = true;
    }
    UrmaInit::init(config);
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
          // Coalesced replies can have more registered nodes than one WR
          // can describe. Preserve the original storage in both directions.
          clients[i]->sendParts(false, 7);
          accepted.peers[i]->sendParts(false, 7, unsigned(73 + i));
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
                                           : chainMode        ? 3
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
          require(a.send_wr_submitted == 5 && a.send_sges == 15 &&
                      a.direct_send_bytes == 3000 && a.copied_send_bytes == 900,
                  "registered chain exceeded SGE limit and copied instead of splitting");
          require(b.send_wr_submitted == 3 && b.send_sges == 10 &&
                      b.direct_send_bytes == 2100 && b.copied_send_bytes == 0,
                  "registered reply chain copied instead of splitting");
        }
        if (mode == "chain_single_sge")
          require(a.send_wr_submitted == 3 && a.send_sges == 3 &&
                      a.direct_send_bytes == 0 && a.copied_send_bytes == 3900 &&
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
