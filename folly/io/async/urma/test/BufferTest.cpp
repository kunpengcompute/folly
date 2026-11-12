/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>

#include <iostream>
#include <stdexcept>

using namespace folly;
void check(bool condition, int line) {
  if (!condition)
    throw std::runtime_error("buffer callback assertion failed at " +
                             std::to_string(line));
}
#define require(condition) check((condition), __LINE__)
struct Write : AsyncTransport::WriteCallback {
  unsigned success{0}, errors{0};
  void writeSuccess() noexcept override { ++success; }
  void writeErr(size_t, const AsyncSocketException&) noexcept override {
    ++errors;
  }
};
struct Observer : AsyncTransport::BufferCallback {
  UrmaAsyncSocket* socket;
  AsyncTransport::UniquePtr* owner;
  unsigned changed{0}, cleared{0};
  int action{0};
  void onEgressBuffered() override {
    ++changed;
    require(socket->getAppBytesBuffered() > 0);
    require(socket->getAllocatedBytesBuffered() >=
            socket->getAppBytesBuffered());
    if (action == 1) socket->closeNow();
    if (action == 2) socket->setBufferCallback(nullptr);
    if (action == 3) owner->reset();
    if (action == 4) {
      action = 0;
      socket->write(nullptr, "extra", 5);
    }
  }
  void onEgressBufferCleared() override {
    ++cleared;
    require(socket->getAppBytesBuffered() == 0);
    require(socket->getAllocatedBytesBuffered() == 0);
  }
};
int main() {
  try {
    EventBase evb;
    auto listener = AsyncServerSocket::newSocket(&evb);
    listener->bind(SocketAddress("127.0.0.1", 0));
    listener->listen(10);
    SocketAddress address;
    listener->getAddress(&address);
    for (int action = 0; action <= 4; ++action) {
      std::cout << "ACTION " << action << std::endl;
      AsyncTransport::UniquePtr owner(new UrmaAsyncSocket(&evb, address));
      auto* socket = static_cast<UrmaAsyncSocket*>(owner.get());
      Observer observer;
      observer.socket = socket;
      observer.owner = &owner;
      observer.action = action;
      socket->setBufferCallback(&observer);
      require(observer.changed == 0);
      Write callback;
      auto data = IOBuf::create(4096);
      data->append(1024);
      const auto capacity = data->capacity();
      socket->writeChain(&callback, std::move(data));
      require(observer.changed >= 1);
      if (action == 0) {
        require(socket->getAppBytesBuffered() == 1024);
        require(socket->getAllocatedBytesBuffered() == capacity);
      }
      if (action == 4) {
        require(socket->getAppBytesBuffered() == 1029);
        require(observer.changed == 2);
      }
      if (owner) {
        socket->closeNow();
        socket->closeNow();
        require(socket->getRawBytesBuffered() == 0);
        socket->setBufferCallback(nullptr);
      }
      require(callback.errors == 1 && callback.success == 0);
      require(observer.cleared == (action == 2 ? 0 : 1));
    }
    {
      AsyncTransport::UniquePtr owner(new UrmaAsyncSocket(&evb, address));
      auto* socket = static_cast<UrmaAsyncSocket*>(owner.get());
      Write callback;
      socket->write(&callback, "queued before registration", 26);
      Observer observer;
      observer.socket = socket;
      observer.owner = &owner;
      socket->setBufferCallback(&observer);
      require(observer.changed == 0);
      socket->closeNow();
      require(observer.cleared == 1 && callback.errors == 1);
      socket->setBufferCallback(nullptr);
    }
    std::cout << "PASS buffer accounting, close, unregister, destroy, "
                 "reentrant enqueue, late registration\n";
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
