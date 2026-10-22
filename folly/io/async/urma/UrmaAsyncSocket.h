/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <memory>
#include <vector>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/urma/UrmaRecordWriter.h>
namespace folly {
class UrmaAsyncSocket : public AsyncTransport, public UrmaRecordWriter {
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
  // recordLengths describe complete serialized records BEFORE upper-layer
  // batching, not IOBuf nodes. Their sum must equal the chain length. Adjacent
  // small records can share SENDs; large records use READ independently. One
  // callback still covers the entire write. Empty metadata means one record.
  bool supportsUrmaRecords() const override { return true; }
  void writeChainWithRecords(
      WriteCallback*,
      std::unique_ptr<IOBuf>&&,
      std::vector<size_t> recordLengths,
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
  // Called on the owning EventBase when queued bytes or retained capacity
  // change. Registration is passive; the caller must keep the callback alive
  // until it is unregistered. Idle registered caches are not pending egress.
  void setBufferCallback(BufferCallback*);
  size_t getAppBytesBuffered() const override;
  size_t getRawBytesBuffered() const override;
  size_t getAllocatedBytesBuffered() const override;
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
