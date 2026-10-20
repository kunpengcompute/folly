/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
// One connection, one echo, SEND/RECV only. Both processes exit after the echo.
#include <array>
#include <folly/SocketAddress.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/portability/Sockets.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

using namespace folly;
namespace {
const std::string message = "hello from basic URMA\n";

struct Echo : AsyncTransport::ReadCallback, AsyncTransport::WriteCallback {
  EventBase &evb;
  bool server;
  AsyncTransport::UniquePtr socket;
  std::array<char, 4096> buffer;
  std::string received;
  bool done{false}, ok{false}, sent{false};
  explicit Echo(EventBase &e, bool s) : evb(e), server(s) {}
  ~Echo() override {
    if (socket) {
      socket->setReadCB(nullptr);
      socket->closeNow();
    }
  }
  void finish(bool success) {
    if (done)
      return;
    done = true;
    ok = success;
    socket->setReadCB(nullptr);
    if (success)
      socket->close();
    else
      socket->closeNow();
    evb.terminateLoopSoon();
  }
  void attach(AsyncTransport::UniquePtr transport) {
    socket = std::move(transport);
    socket->setSendTimeout(10000);
    socket->setReadCB(this);
    if (!server && !done)
      socket->write(this, message.data(), message.size());
  }
  void getReadBuffer(void **p, size_t *n) override {
    *p = buffer.data();
    *n = buffer.size();
  }
  void readDataAvailable(size_t n) noexcept override {
    if (done)
      return;
    received.append(buffer.data(), n);
    if (received.size() > message.size() ||
        message.compare(0, received.size(), received)) {
      std::cerr << "Echo content mismatch\n";
      finish(false);
      return;
    }
    if (server)
      socket->write(this, buffer.data(), n);
    else if (sent && received == message)
      finish(true);
  }
  void readEOF() noexcept override {
    finish(server && received == message && sent);
  }
  void readErr(const AsyncSocketException &e) noexcept override {
    std::cerr << e.what() << '\n';
    finish(false);
  }
  void writeSuccess() noexcept override {
    sent = true;
    if (!server && received == message)
      finish(true);
  }
  void writeErr(size_t, const AsyncSocketException &e) noexcept override {
    readErr(e);
  }
};

struct Accept : AsyncServerSocket::AcceptCallback {
  EventBase &evb;
  std::unique_ptr<Echo> echo;
  explicit Accept(EventBase &e) : evb(e) {}
  void connectionAccepted(NetworkSocket fd, const SocketAddress &,
                          AcceptInfo) noexcept override {
    if (echo) {
      netops::close(fd);
      return;
    }
    echo = std::make_unique<Echo>(evb, true);
    echo->attach(
        AsyncTransport::UniquePtr(new UrmaAsyncSocket(&evb, fd.toFd())));
  }
  void acceptError(exception_wrapper e) noexcept override {
    std::cerr << e.what() << '\n';
    evb.terminateLoopSoon();
  }
  void acceptStarted() noexcept override {}
  void acceptStopped() noexcept override {}
};

int run(bool server, const char *address, uint16_t port) {
  EventBase evb;
  auto listener = AsyncServerSocket::newSocket(&evb);
  Accept accept(evb);
  std::unique_ptr<Echo> client;
  if (server) {
    listener->bind(port);
    listener->listen(16);
    listener->addAcceptCallback(&accept, &evb);
    listener->startAccepting();
    std::cout << "Listening on " << port << " (URMA RM/CTP SEND/RECV)"
              << std::endl;
  } else {
    client = std::make_unique<Echo>(evb, false);
    client->attach(AsyncTransport::UniquePtr(
        new UrmaAsyncSocket(&evb, SocketAddress(address, port))));
  }
  evb.runAfterDelay(
      [&] {
        std::cerr << "Echo timeout\n";
        evb.terminateLoopSoon();
      },
      30000);
  if (!client || !client->done)
    evb.loop();
  if (server)
    listener->stopAccepting();
  auto *result = server ? accept.echo.get() : client.get();
  const bool ok = result && result->done && result->ok;
  std::cout << (ok ? "PASS" : "FAIL") << " URMA echo" << std::endl;
  return ok ? 0 : 1;
}
} // namespace

int main(int argc, char **argv) {
  if (argc < 2 ||
      (std::string(argv[1]) != "server" && std::string(argv[1]) != "client")) {
    std::cerr
        << "server <port> <device> | client <server_ip> <port> <device>\n";
    return 2;
  }
  const bool server = std::string(argv[1]) == "server";
  if (argc != (server ? 4 : 5)) {
    std::cerr << "Invalid arguments\n";
    return 2;
  }
  try {
    const auto port = std::stoul(argv[server ? 2 : 3]);
    if (!port || port > 65535)
      throw std::invalid_argument("invalid port");
    UrmaConfig config;
    config.dev_name = argv[server ? 3 : 4];
    UrmaInit::init(config);
    const int result =
        run(server, server ? nullptr : argv[2], static_cast<uint16_t>(port));
    UrmaInit::fini();
    return result;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    UrmaInit::fini();
    return 1;
  }
}
