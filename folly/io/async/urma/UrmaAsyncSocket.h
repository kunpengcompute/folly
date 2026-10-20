/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <memory>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/AsyncSocket.h>
namespace folly {
class UrmaAsyncSocket : public AsyncTransport {
 public:
  UrmaAsyncSocket(EventBase* evb, const SocketAddress& address);
  UrmaAsyncSocket(EventBase* evb, int acceptedFd);
  explicit UrmaAsyncSocket(AsyncSocket::UniquePtr acceptedSocket);
  // Keep the legacy factory usable in a conditional with AsyncSocket::newSocket.
  // Both branches can convert to the AsyncTransport owning pointer.
  static AsyncTransport::UniquePtr newSocket(
      EventBase* evb, const SocketAddress& address) {
    return AsyncTransport::UniquePtr(new UrmaAsyncSocket(evb, address));
  }
  ~UrmaAsyncSocket() override;
  void setReadCB(ReadCallback*) noexcept override;
  ReadCallback* getReadCallback() const noexcept override;
  void write(
      WriteCallback*,
      const void*,
      size_t,
      WriteFlags = WriteFlags::NONE) noexcept override;
  void writev(
      WriteCallback*,
      const iovec*,
      size_t,
      WriteFlags = WriteFlags::NONE) noexcept override;
  void writeChain(
      WriteCallback*,
      std::unique_ptr<IOBuf>&&,
      WriteFlags = WriteFlags::NONE) noexcept override;
  EventBase* getEventBase() const override;
  void close() override;
  void closeNow() override;
  void shutdownWrite() override;
  void shutdownWriteNow() override;
  bool good() const override;
  bool readable() const override;
  bool writable() const override;
  bool connecting() const override;
  bool error() const override;
  bool isDetachable() const override { return false; }
  void attachEventBase(EventBase*) override;
  void detachEventBase() override;
  void setSendTimeout(uint32_t) override;
  uint32_t getSendTimeout() const override;
  void getLocalAddress(SocketAddress*) const override;
  void getPeerAddress(SocketAddress*) const override;
  bool isEorTrackingEnabled() const override { return false; }
  void setEorTracking(bool) override;
  size_t getAppBytesWritten() const override;
  size_t getRawBytesWritten() const override;
  size_t getAppBytesReceived() const override;
  size_t getRawBytesReceived() const override;
  bool isUrmaReady() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace folly
#endif
