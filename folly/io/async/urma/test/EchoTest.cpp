/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0. */
#include <folly/Conv.h>
#include <folly/ScopeGuard.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaInit.h>

#include <cstring>
#include <iostream>

using namespace folly;

namespace {
constexpr char message[] = "ping";
constexpr size_t length = sizeof(message) - 1;

struct Echo : AsyncTransport::ReadCallback, AsyncTransport::WriteCallback {
  EventBase evb;
  AsyncTransport::UniquePtr socket;
  const bool server;
  char received[length]{};
  size_t used{0};
  bool ok{false};

  explicit Echo(bool isServer) : server(isServer) {}
  ~Echo() {
    if (socket) {
      socket->setReadCB(nullptr);
      socket->closeNow();
    }
  }
  void attach(AsyncTransport::UniquePtr transport) {
    socket = std::move(transport);
    socket->setReadCB(this);
    if (!server) {
      socket->write(this, message, length);
    }
  }
  void getReadBuffer(void** data, size_t* size) override {
    // Keep accepting reads after the message to detect EOF or extra bytes.
    *data = received;
    *size = sizeof(received);
  }
  void readDataAvailable(size_t n) noexcept override {
    if (n > length - used || std::memcmp(received, message + used, n) != 0) {
      evb.terminateLoopSoon();
      return;
    }
    used += n;
    if (used == length) {
      if (server) {
        socket->write(this, message, length);
      } else {
        ok = true;
        socket->setReadCB(nullptr);
        socket->close();
        evb.terminateLoopSoon();
      }
    }
  }
  void readEOF() noexcept override {
    ok = server && used == length;
    evb.terminateLoopSoon();
  }
  void readErr(const AsyncSocketException& ex) noexcept override { fail(ex); }
  void writeErr(size_t, const AsyncSocketException& ex) noexcept override { fail(ex); }
  void writeSuccess() noexcept override {}
  void fail(const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    ok = false;
    evb.terminateLoopSoon();
  }
};

struct Acceptor : AsyncServerSocket::AcceptCallback {
  Echo& echo;
  explicit Acceptor(Echo& e) : echo(e) {}
  void connectionAccepted(NetworkSocket fd, const SocketAddress&, AcceptInfo)
      noexcept override {
    try {
      auto tcp = AsyncSocket::newSocket(&echo.evb, fd);
      if (!echo.socket) {
        echo.attach(AsyncTransport::UniquePtr(new UrmaAsyncSocket(std::move(tcp))));
      }
    } catch (const std::exception& ex) {
      echo.fail(ex);
    }
  }
  void acceptError(const std::exception& ex) noexcept override { echo.fail(ex); }
};
} // namespace

int main(int argc, char** argv) try {
  if (argc != 5 || (std::strcmp(argv[1], "server") && std::strcmp(argv[1], "client"))) {
    std::cerr << "Usage: UrmaEchoTest server|client server-ip port local-device\n";
    return 2;
  }
  UrmaConfig config;
  config.dev_name = argv[4];
  UrmaInit::init(config);
  auto cleanup = makeGuard([] { UrmaInit::fini(); });
  Echo echo(std::strcmp(argv[1], "server") == 0);
  SocketAddress address(argv[2], to<uint16_t>(argv[3]));
  Acceptor acceptor(echo);
  auto listener = AsyncServerSocket::newSocket(&echo.evb);
  if (echo.server) {
    listener->bind(address);
    listener->listen(1);
    listener->addAcceptCallback(&acceptor, &echo.evb);
    listener->startAccepting();
    std::cout << "LISTEN" << std::endl;
  } else {
    echo.attach(UrmaAsyncSocket::newSocket(&echo.evb, address));
  }
  echo.evb.runAfterDelay([&] { echo.evb.terminateLoopSoon(); }, 30000);
  echo.evb.loop();
  std::cout << (echo.ok ? "PASS " : "FAIL ") << argv[1] << std::endl;
  return echo.ok ? 0 : 1;
} catch (const std::exception& ex) {
  std::cerr << ex.what() << '\n';
  return 1;
}
