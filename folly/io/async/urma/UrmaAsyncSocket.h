/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/urma/UrmaRecordWriter.h>
#include <folly/io/async/urma/UrmaResourceExecutor.h>

#include <memory>
#include <vector>
namespace folly {
class UrmaAsyncSocket : public AsyncTransport, public UrmaRecordWriter {
 public:
  struct CopyStats {
    uint64_t direct_send_bytes{0}, copied_send_bytes{0};
    uint64_t send_wr_submitted{0}, send_sges{0};
    uint64_t direct_read_source_bytes{0}, copied_read_source_bytes{0};
    uint64_t transferred_read_target_bytes{0}, copied_read_target_bytes{0};
    uint64_t read_wr_submitted{0}, read_submit_calls{0}, max_read_inflight{0};
    // Accepted PULL records; SDK-internal retries are not additional records.
    uint64_t offers_submitted{0}, max_offers_inflight{0}, max_receive_offers{0};
  };
  // Read on the owning EventBase; byte counters describe transport copies,
  // not serialization or an application's subsequent use of received data.
  CopyStats getCopyStats() const;
  UrmaAsyncSocket(EventBase* evb, const SocketAddress& address);
  UrmaAsyncSocket(EventBase* evb, int acceptedFd);
  explicit UrmaAsyncSocket(AsyncSocket::UniquePtr acceptedSocket);
  // Keep the legacy factory usable in a conditional with
  // AsyncSocket::newSocket. Both branches can convert to the AsyncTransport
  // owning pointer.
  static AsyncTransport::UniquePtr newSocket(EventBase* evb,
                                             const SocketAddress& address) {
    return AsyncTransport::UniquePtr(new UrmaAsyncSocket(evb, address));
  }
  ~UrmaAsyncSocket() override;
  void setReadCB(ReadCallback*) noexcept override;
  ReadCallback* getReadCallback() const noexcept override;
  void write(WriteCallback*, const void*, size_t,
             WriteFlags = WriteFlags::NONE) noexcept override;
  void writev(WriteCallback*, const iovec*, size_t,
              WriteFlags = WriteFlags::NONE) noexcept override;
  void writeChain(WriteCallback*, std::unique_ptr<IOBuf>&&,
                  WriteFlags = WriteFlags::NONE) noexcept override;
  // Compatibility entry point: validates the upper-layer record lengths, then
  // preserves their concatenated byte stream. Each PULL describes only one
  // actual continuous registered range. One callback covers the entire write.
  bool supportsUrmaRecords() const override { return true; }
  void writeChainWithRecords(WriteCallback*, std::unique_ptr<IOBuf>&&,
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
  friend void urma_detail::warmResourceExecutor();
  struct Impl;
  // Route/WR holders may outlive the public socket while DRAINING. Their
  // callbacks are detached before the public socket is destroyed.
  std::shared_ptr<Impl> impl_;
};
}  // namespace folly
#endif
