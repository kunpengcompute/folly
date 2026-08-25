/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * URMA echo demo (server + client) — folly 层端到端收发示例。
 *
 * 编译宏隔离:
 *   - 定义 FOLLY_HAVE_URMA 时:走 URMA 路径(UrmaAsyncSocket / UrmaTransportWrapper)
 *   - 未定义时:回退到 folly 默认 AsyncSocket
 *   两条路径对外接口一致(均为 AsyncTransport),EchoConn/ClientConn 逻辑共用。
 *
 * 用法:
 *   ./UrmaEchoDemo server <port>
 *   ./UrmaEchoDemo client <server_ip> <port>
 *
 * 独立项目构建(WSL,需先构建 folly):
 *   g++ -std=gnu++17 -O2 -fcoroutines \
 *     -I<folly_src> -I<folly_build> \
 *     -DFOLLY_HAVE_URMA \
 *     -DFMT_SHARED -DGFLAGS_IS_A_DLL=0 -D_GNU_SOURCE -D_REENTRANT \
 *     <此文件> <folly_build>/libfolly.a \
 *     -lglog -lgflags -ldouble-conversion -lfmt \
 *     -lboost_context -lboost_system -lboost_filesystem -lboost_thread \
 *     -lssl -lcrypto -lzstd -lz -lbz2 -llz4 -lsnappy -llzma \
 *     -lunwind -lsodium -ldwarf -liberty -levent \
 *     -lpthread -ldl -lrt \
 *     -o UrmaEchoDemo
 */

#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>

#include <folly/SocketAddress.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>

#ifdef FOLLY_HAVE_URMA
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaTransportWrapper.h>
#endif

using namespace folly;

namespace {

#ifdef FOLLY_HAVE_URMA
constexpr const char* kMode = "URMA";
#else
constexpr const char* kMode = "AsyncSocket";
#endif

// ============================================================
// 服务端:一条连接 = EchoConn(读回调 + 写回调)
// 使用 readBufferAvailable 零拷贝路径(URMA 数据已是 IOBuf)
// ============================================================
class EchoConn : public AsyncReader::ReadCallback,
                 public AsyncWriter::WriteCallback {
 public:
  EchoConn(EventBase* evb, AsyncTransport::UniquePtr transport)
      : evb_(evb), transport_(std::move(transport)) {
    transport_->setReadCB(this);
  }

  // 零拷贝路径:URMA onRxBuf 直接调 readBufferAvailable
  bool isBufferMovable() noexcept override { return true; }

  void readBufferAvailable(std::unique_ptr<IOBuf> buf) noexcept override {
    // echo:直接回发收到的 IOBuf(零拷贝)
    transport_->writeChain(this, std::move(buf), WriteFlags::NONE);
  }

  // fallback 路径(非 URMA 模式)
  void getReadBuffer(void** bufReturn, size_t* lenReturn) noexcept override {
    *bufReturn = read_buf_;
    *lenReturn = sizeof(read_buf_);
  }

  void readDataAvailable(size_t len) noexcept override {
    auto out = IOBuf::create(len);
    std::memcpy(out->writableTail(), read_buf_, len);
    out->append(len);
    transport_->writeChain(this, std::move(out), WriteFlags::NONE);
  }

  void readEOF() noexcept override {
    std::cout << "[server] peer closed\n";
    evb_->runInEventBaseThread([this] { delete this; });
  }

  void readErr(const AsyncSocketException& ex) noexcept override {
    std::cerr << "[server] readErr: " << ex.what() << "\n";
    evb_->runInEventBaseThread([this] { delete this; });
  }

  void writeSuccess() noexcept override {}
  void writeErr(size_t, const AsyncSocketException& ex) noexcept override {
    std::cerr << "[server] writeErr: " << ex.what() << "\n";
  }

 private:
  ~EchoConn() override = default;
  EventBase* evb_;
  AsyncTransport::UniquePtr transport_;
  char read_buf_[4096];
};

// AcceptCallback:URMA 则 createAndOffload,否则直接 AsyncSocket 接管 fd
class EchoAcceptCb : public AsyncServerSocket::AcceptCallback {
 public:
  explicit EchoAcceptCb(EventBase* evb) : evb_(evb) {}

  void connectionAccepted(NetworkSocket fd,
                           const SocketAddress& addr,
                           AcceptInfo) noexcept override {
    AsyncTransport::UniquePtr transport;
#ifdef FOLLY_HAVE_URMA
    transport = UrmaTransportWrapper::createAndOffload(evb_, fd.toFd());
    if (!transport) {
      std::cerr << "[server] offload failed, fd=" << fd.toFd() << "\n";
      ::close(fd.toFd());
      return;
    }
#else
    AsyncSocket::UniquePtr sock = AsyncSocket::newSocket(evb_, fd);
    transport = AsyncTransport::UniquePtr(sock.release());
#endif
    std::cout << "[server] accepted from " << addr.describe()
              << " [" << kMode << "]\n";
    new EchoConn(evb_, std::move(transport));
  }

  void acceptError(exception_wrapper ew) noexcept override {
    std::cerr << "[server] acceptError: " << ew.what() << "\n";
  }
  void acceptStarted() noexcept override {
    std::cout << "[server] accept started\n";
  }
  void acceptStopped() noexcept override {
    std::cout << "[server] accept stopped\n";
  }

 private:
  EventBase* evb_;
};

int runServer(uint16_t port) {
#ifdef FOLLY_HAVE_URMA
  UrmaConfig config;
  config.pool_size = 64 * 1024 * 1024;  // 64MB(测试用小池)
  UrmaInit::init(config);
#endif

  EventBase evb;
  auto server = AsyncServerSocket::newSocket(&evb);
  server->bind(port);
  server->listen(128);
  std::cout << "[server] listening on port " << server->getAddress().getPort()
            << " [" << kMode << "]\n";

  EchoAcceptCb cb(&evb);
  server->addAcceptCallback(&cb, &evb);
  server->startAccepting();

  evb.loop();
#ifdef FOLLY_HAVE_URMA
  UrmaInit::fini();
#endif
  return 0;
}

// ============================================================
// 客户端:连接后发一条消息,收到回显后退出
// ============================================================
class ClientConn : public AsyncReader::ReadCallback,
                   public AsyncWriter::WriteCallback {
 public:
  ClientConn(EventBase* evb, AsyncTransport::UniquePtr transport)
      : evb_(evb), transport_(std::move(transport)) {
    transport_->setReadCB(this);
    std::string msg = "hello from urma client\n";
    auto out = IOBuf::create(msg.size());
    std::memcpy(out->writableTail(), msg.data(), msg.size());
    out->append(msg.size());
    transport_->writeChain(this, std::move(out), WriteFlags::NONE);
  }

  bool isBufferMovable() noexcept override { return true; }

  void readBufferAvailable(std::unique_ptr<IOBuf> buf) noexcept override {
    std::cout << "[client] echo: " << std::string(
        reinterpret_cast<const char*>(buf->data()), buf->length());
    evb_->terminateLoopSoon();
  }

  void getReadBuffer(void** bufReturn, size_t* lenReturn) noexcept override {
    *bufReturn = read_buf_;
    *lenReturn = sizeof(read_buf_);
  }

  void readDataAvailable(size_t len) noexcept override {
    std::cout << "[client] echo: " << std::string(read_buf_, len);
    evb_->terminateLoopSoon();
  }

  void readEOF() noexcept override {
    std::cerr << "[client] unexpected EOF\n";
    evb_->terminateLoopSoon();
  }

  void readErr(const AsyncSocketException& ex) noexcept override {
    std::cerr << "[client] readErr: " << ex.what() << "\n";
    evb_->terminateLoopSoon();
  }

  void writeSuccess() noexcept override {}
  void writeErr(size_t, const AsyncSocketException& ex) noexcept override {
    std::cerr << "[client] writeErr: " << ex.what() << "\n";
    evb_->terminateLoopSoon();
  }

 private:
  ~ClientConn() override = default;
  EventBase* evb_;
  AsyncTransport::UniquePtr transport_;
  char read_buf_[4096];
};

int runClient(const std::string& serverIp, uint16_t port) {
#ifdef FOLLY_HAVE_URMA
  UrmaConfig config;
  config.pool_size = 64 * 1024 * 1024;  // 64MB(测试用小池)
  UrmaInit::init(config);
#endif

  EventBase evb;
  SocketAddress addr(serverIp, port);
  AsyncTransport::UniquePtr transport;
#ifdef FOLLY_HAVE_URMA
  transport.reset(new UrmaAsyncSocket(&evb, addr));
#else
  transport.reset(new AsyncSocket(&evb, addr));
#endif

  // 驱动 evb 直到连接/握手完成(good)或失败(error)
  int waits = 0;
  while (!transport->good() && !transport->error() && waits < 40000) {
    evb.loopOnce();
    ++waits;
  }
  if (!transport->good()) {
    std::cerr << "[client] connect failed after " << waits
              << " iterations [" << kMode << "]\n";
    return 1;
  }
  std::cout << "[client] connected [" << kMode << "]\n";

  new ClientConn(&evb, std::move(transport));
  evb.loop();
#ifdef FOLLY_HAVE_URMA
  UrmaInit::fini();
#endif
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage:\n"
              << "  " << argv[0] << " server <port>\n"
              << "  " << argv[0] << " client <server_ip> <port>\n"
              << "  (mode: " << kMode << ")\n";
    return 1;
  }
  std::string mode = argv[1];
  if (mode == "server") {
    if (argc < 3) {
      std::cerr << "server needs: <port>\n";
      return 1;
    }
    return runServer(static_cast<uint16_t>(std::stoi(argv[2])));
  } else if (mode == "client") {
    if (argc < 4) {
      std::cerr << "client needs: <server_ip> <port>\n";
      return 1;
    }
    return runClient(argv[2], static_cast<uint16_t>(std::stoi(argv[3])));
  }
  std::cerr << "unknown mode: " << mode << "\n";
  return 1;
}
