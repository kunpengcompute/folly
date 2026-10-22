/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
// Hardware regression test. SDK observation lives in this executable only.
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <urma_api.h>
#include <dlfcn.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <numeric>
#include <string>
#include <vector>

using namespace folly;
namespace {
using Clock = std::chrono::steady_clock;
struct Sample {
  size_t length{0}, posted{0}, wrs{0};
  Clock::time_point queued{}, first{}, completed{};
};
std::map<unsigned char, Sample> samples;
bool observe = false;
size_t capacity = 4096;
void require(bool ok, const char* message) {
  if (!ok) {
    std::cerr << "FAIL " << message << std::endl;
    std::abort();
  }
}
double us(Clock::duration value) {
  return std::chrono::duration<double, std::micro>(value).count();
}
const std::vector<std::vector<size_t>> layouts{
    {}, {300, 300, 300}, {0, 7, 13, 0, 113, 891, 0}, {},
    {40, 80, 200, 3648}, {17}, {19}, {3968}};
size_t totalBytes() {
  size_t result = 0;
  for (const auto& layout : layouts)
    result += std::accumulate(layout.begin(), layout.end(), size_t(0));
  return result;
}
} // namespace

extern "C" urma_status_t urma_post_jetty_send_wr(
    urma_jetty_t* jetty, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
  static auto real = reinterpret_cast<decltype(&urma_post_jetty_send_wr)>(
      dlsym(RTLD_NEXT, "urma_post_jetty_send_wr"));
  require(real != nullptr, "SDK symbol unavailable (shared Folly required)");
  auto result = real(jetty, wr, bad);
  if (!observe || result != URMA_SUCCESS)
    return result;
  for (auto* p = wr; p; p = p->next) {
    require(p->opcode == URMA_OPC_SEND, "expected the SEND coalescing path");
    require(p->send.src.num_sge == 1, "expected one registered SGE");
    const auto& sge = p->send.src.sge[0];
    require(sge.len && sge.len <= capacity && sge.tseg, "invalid SEND range");
    const auto* bytes = reinterpret_cast<const unsigned char*>(sge.addr);
    const auto label = bytes[0];
    auto it = samples.find(label);
    require(it != samples.end(), "unknown write label");
    // Different writes/connections have different labels. A single WR must
    // never consume bytes from two of them, even when both are already queued.
    require(std::all_of(bytes, bytes + sge.len, [label](auto c) {
      return c == label;
    }), "cross-write or cross-connection merge");
    auto& sample = it->second;
    if (!sample.wrs)
      sample.first = Clock::now();
    ++sample.wrs;
    sample.posted += sge.len;
    std::cout << "WR write=" << unsigned(label) << " bytes=" << sge.len
              << " sges=1 transport_header_bytes=0\n";
  }
  return result;
}

struct Run {
  EventBase evb;
  size_t expected, finished{0};
  explicit Run(size_t count) : expected(count) {}
  void finish() {
    if (++finished == expected)
      evb.terminateLoopSoon();
  }
};
struct Connection : AsyncTransport::ReadCallback {
  struct Completion : AsyncTransport::WriteCallback {
    Connection* connection;
    size_t index;
    Completion(Connection* c, size_t i) : connection(c), index(i) {}
    void writeSuccess() noexcept override { connection->completed(index); }
    void writeErr(size_t, const AsyncSocketException&) noexcept override {
      require(false, "unexpected write failure");
    }
  };
  Run& run;
  bool server, done{false}, submitted{false};
  size_t id, received{0}, callbacks{0};
  AsyncTransport::UniquePtr socket;
  std::array<char, 65536> buffer{};
  std::string expected;
  std::vector<std::string> storage;
  std::vector<std::unique_ptr<IOBuf>> retained;
  std::vector<std::unique_ptr<Completion>> completions;
  Connection(Run& r, bool s, size_t i) : run(r), server(s), id(i) {}
  ~Connection() {
    if (socket) {
      socket->setReadCB(nullptr);
      socket->closeNow();
    }
  }
  unsigned char label(size_t index) const { return 1 + id * 16 + index; }
  void attach(AsyncTransport::UniquePtr p) {
    socket = std::move(p);
    socket->setSendTimeout(10000);
    socket->setReadCB(this);
  }
  void send() {
    storage.reserve(layouts.size());
    for (size_t i = 0; i < layouts.size(); ++i) {
      size_t length = std::accumulate(
          layouts[i].begin(), layouts[i].end(), size_t(0));
      storage.emplace_back(length, char(label(i)));
      expected += storage.back();
    }
    for (size_t i = 0; i < layouts.size(); ++i) {
      auto chain = IOBuf::create(0);
      size_t offset = 0;
      for (auto length : layouts[i]) {
        // Exercise shared clones, ordinary unregistered storage, and empty
        // nodes. The transport must neither write headroom nor alter payload.
        auto node = i % 2 ? IOBuf::copyBuffer(storage[i].data() + offset, length)
                          : IOBuf::wrapBuffer(storage[i].data() + offset, length);
        retained.push_back(node->clone());
        chain->prependChain(std::move(node));
        offset += length;
      }
      completions.emplace_back(std::make_unique<Completion>(this, i));
      samples.emplace(label(i), Sample{storage[i].size(), 0, 0, Clock::now()});
      socket->writeChain(completions.back().get(), std::move(chain));
    }
    submitted = true;
    progress();
  }
  void completed(size_t index) {
    require(index == callbacks++, "write callbacks duplicated or out of order");
    auto& sample = samples.at(label(index));
    sample.completed = Clock::now();
    require(sample.posted == sample.length, "callback before all bytes posted");
    require(sample.wrs == (sample.length + capacity - 1) / capacity,
            "incorrect WR count for a write");
    progress();
  }
  void progress() {
    if (done || !submitted || callbacks != layouts.size() ||
        received != expected.size())
      return;
    size_t index = 0;
    for (size_t i = 0; i < layouts.size(); ++i) {
      require(storage[i] == std::string(storage[i].size(), char(label(i))),
              "wrapped input modified");
      for (auto length : layouts[i]) {
        auto& node = retained[index++];
        require(node->length() == length, "shared clone length modified");
        require(std::all_of(node->data(), node->data() + length,
                           [&](auto c) { return c == label(i); }),
                "shared clone data modified");
      }
    }
    require(socket->getAppBytesWritten() == expected.size(), "TX accounting");
    done = true;
    socket->setReadCB(nullptr);
    socket->close();
    run.finish();
  }
  void getReadBuffer(void** p, size_t* n) override {
    *p = buffer.data();
    *n = buffer.size();
  }
  void readDataAvailable(size_t n) noexcept override {
    if (server) {
      received += n;
      socket->write(nullptr, buffer.data(), n);
    } else {
      require(received + n <= expected.size(), "extra response bytes");
      require(expected.compare(received, n, buffer.data(), n) == 0,
              "echo byte order or contents changed");
      received += n;
      progress();
    }
  }
  void readEOF() noexcept override {
    require(server && !done && received == totalBytes(), "unexpected EOF");
    done = true;
    socket->closeNow();
    run.finish();
  }
  void readErr(const AsyncSocketException&) noexcept override {
    require(false, "unexpected read failure");
  }
};
struct Acceptor : AsyncServerSocket::AcceptCallback {
  Run& run;
  std::vector<std::unique_ptr<Connection>>& connections;
  Acceptor(Run& r, decltype(connections) c) : run(r), connections(c) {}
  void connectionAccepted(NetworkSocket fd, const SocketAddress&, AcceptInfo)
      noexcept override {
    auto c = std::make_unique<Connection>(run, true, connections.size());
    c->attach(AsyncTransport::UniquePtr(new UrmaAsyncSocket(&run.evb, fd.toFd())));
    connections.push_back(std::move(c));
  }
  void acceptError(exception_wrapper) noexcept override {
    require(false, "accept failed");
  }
  void acceptStarted() noexcept override {}
  void acceptStopped() noexcept override {}
};
int main(int argc, char** argv) {
  if (argc != 7) {
    std::cerr << "server|client address port device buffer-size connections\n";
    return 2;
  }
  std::cout.setf(std::ios::unitbuf);
  bool server = std::string(argv[1]) == "server";
  require(server || std::string(argv[1]) == "client", "invalid role");
  capacity = std::stoul(argv[5]);
  size_t count = std::stoul(argv[6]);
  require(capacity >= 64 && capacity <= 4096 && count && count <= 8,
          "invalid capacity or connection count");
  UrmaConfig config;
  config.dev_name = argv[4];
  config.buffer_size = capacity;
  UrmaInit::init(config);
  {
    Run run(count);
    std::vector<std::unique_ptr<Connection>> connections;
    auto listener = AsyncServerSocket::newSocket(&run.evb);
    Acceptor acceptor(run, connections);
    SocketAddress address(argv[2], uint16_t(std::stoul(argv[3])));
    observe = !server;
    if (server) {
      listener->bind(address);
      listener->listen(16);
      listener->addAcceptCallback(&acceptor, &run.evb);
      listener->startAccepting();
      std::cout << "LISTEN\n";
    } else {
      for (size_t i = 0; i < count; ++i) {
        auto c = std::make_unique<Connection>(run, false, i);
        c->attach(AsyncTransport::UniquePtr(new UrmaAsyncSocket(&run.evb, address)));
        connections.push_back(std::move(c));
        connections.back()->send();
      }
    }
    run.evb.runAfterDelay([] { require(false, "overall timeout"); }, 15000);
    run.evb.loop();
    if (server)
      listener->stopAccepting();
    require(run.finished == count, "missing connection completion");
  }
  UrmaInit::fini();
  require(!UrmaInit::isInitialized(), "context retained after test");
  if (!server) {
    std::cout << "write,bytes,wrs,sges_per_wr,header_bytes,queue_us,completion_us\n";
    for (const auto& item : samples) {
      const auto& s = item.second;
      std::cout << unsigned(item.first) << ',' << s.length << ',' << s.wrs
                << ",1,0," << (s.wrs ? us(s.first - s.queued) : 0)
                << ',' << us(s.completed - s.queued) << '\n';
    }
  }
  std::cout << "PASS coalescing role=" << argv[1] << '\n';
}
