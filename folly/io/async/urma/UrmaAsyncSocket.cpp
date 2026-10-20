/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#ifdef FOLLY_HAVE_URMA
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <utility>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/logging/xlog.h>

namespace folly {
namespace {
// This deliberately has a different wire identity from the experimental V2/V3.
constexpr size_t kHelloSize = 40;
constexpr uint32_t kReady = 0x52445931; // RDY1
constexpr uint64_t kRx = uint64_t(1) << 63;
constexpr auto kRnrRetryDelay = std::chrono::milliseconds(1);
constexpr auto kRnrTimeout = std::chrono::seconds(5);
void put32(uint8_t* p, uint32_t n) {
  for (int i = 3; i >= 0; --i) {
    p[i] = uint8_t(n);
    n >>= 8;
  }
}
uint32_t get32(const uint8_t* p) {
  uint32_t n = 0;
  for (int i = 0; i < 4; ++i)
    n = (n << 8) | p[i];
  return n;
}
using Clock = std::chrono::steady_clock;
} // namespace

struct UrmaAsyncSocket::Impl final : AsyncSocket::ConnectCallback,
                                     AsyncTransport::ReadCallback,
                                     AsyncTransport::WriteCallback,
                                     EventHandler,
                                     AsyncTimeout {
  enum class State { Connecting, Handshake, Open, Closed, Error };
  struct Resources {
    urma_context_t* ctx{nullptr};
    urma_jfce_t* event{nullptr};
    urma_jfc_t* cq{nullptr};
    urma_jfr_t* rx{nullptr};
    urma_jetty_t* jetty{nullptr};
    urma_target_jetty_t* peer{nullptr};
    urma_target_seg_t* segment{nullptr};
    void* memory{nullptr};
  };
  struct Write {
    WriteCallback* callback;
    std::unique_ptr<IOBuf> data;
    size_t completed{0};
    Clock::time_point since{Clock::now()};
  };
  UrmaAsyncSocket* owner;
  EventBase* evb;
  UrmaConfig config;
  AsyncSocket::UniquePtr tcp;
  std::unique_ptr<Resources> res;
  State state{State::Connecting};
  AsyncTransport::ReadCallback* reader{nullptr};
  std::array<uint8_t, 128> tcpBuffer{};
  std::vector<uint8_t> handshake;
  unsigned handshakeWrites{0};
  bool haveHello{false}, haveReady{false};
  bool sending{false}, delivering{false}, polling{false};
  bool writeShutdown{false}, closeAfterWrites{false}, txActive{false};
  bool peerEof{false};
  uint32_t peerBuffer{0}, sendTimeout{0};
  uint64_t txId{0};
  Clock::time_point started{Clock::now()};
  Clock::time_point rnrSince{}, retryAt{};
  std::deque<Write> writes;
  IOBufQueue reads{IOBufQueue::cacheChainLength()};
  size_t pendingBytes{0}, txBytes{0}, bytesWritten{0}, bytesRead{0};
  urma_jetty_id_t peerId{};

  Impl(UrmaAsyncSocket* o, EventBase* e)
      : EventHandler(e),
        AsyncTimeout(e),
        owner(o),
        evb(e),
        config(UrmaInit::getConfig()) {}
  ~Impl() override {
    cancelTimeout();
    unregisterHandler();
    cleanup();
  }
  bool live() const { return state != State::Closed && state != State::Error; }
  void armTimer() {
    if (live() && (state != State::Open || !writes.empty()))
      scheduleTimeout(retryAt != Clock::time_point{} ? 1 : 10);
    else
      cancelTimeout();
  }
  bool createResources() {
    res = std::make_unique<Resources>();
    res->ctx = UrmaInit::acquireContext();
    if (!res->ctx)
      return false;
    const size_t bytes = size_t(config.rx_depth + 1) * config.buffer_size;
    if (posix_memalign(&res->memory, 4096, bytes))
      return false;
    urma_seg_cfg_t sc{};
    sc.va = reinterpret_cast<uint64_t>(res->memory);
    sc.len = bytes;
    sc.flag.bs.access = URMA_ACCESS_LOCAL_ONLY;
    res->segment = urma_register_seg(res->ctx, &sc);
    res->event = urma_create_jfce(res->ctx);
    if (!res->segment || !res->event)
      return false;
    urma_jfc_cfg_t cc{};
    cc.depth = config.rx_depth + config.tx_depth + 1;
    cc.jfce = res->event;
    res->cq = urma_create_jfc(res->ctx, &cc);
    if (!res->cq)
      return false;
    urma_jfr_cfg_t rc{};
    rc.depth = config.rx_depth;
    rc.max_sge = 1;
    rc.jfc = res->cq;
    rc.trans_mode = URMA_TM_RM;
    rc.flag.bs.order_type = URMA_OI;
    rc.min_rnr_timer = 12;
    res->rx = urma_create_jfr(res->ctx, &rc);
    if (!res->rx)
      return false;
    urma_jetty_cfg_t jc{};
    // SDK calls this 'share_jfr', but this JFR belongs to this connection only.
    jc.flag.bs.share_jfr = 1;
    jc.shared.jfr = res->rx;
    jc.shared.jfc = res->cq;
    jc.jfs_cfg.depth = config.tx_depth;
    jc.jfs_cfg.max_sge = 1;
    jc.jfs_cfg.jfc = res->cq;
    jc.jfs_cfg.trans_mode = URMA_TM_RM;
    jc.jfs_cfg.flag.bs.order_type = URMA_OI;
    jc.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jc.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    res->jetty = urma_create_jetty(res->ctx, &jc);
    if (!res->jetty)
      return false;
    for (uint32_t i = 0; i < config.rx_depth; ++i)
      if (!postReceive(i))
        return false;
    changeHandlerFD(NetworkSocket(res->event->fd));
    return registerHandler(READ | PERSIST) &&
        urma_rearm_jfc(res->cq, false) == URMA_SUCCESS;
  }
  bool postReceive(uint32_t slot) {
    urma_sge_t sge{};
    sge.addr = reinterpret_cast<uint64_t>(res->memory) +
        size_t(slot) * config.buffer_size;
    sge.len = config.buffer_size;
    sge.tseg = res->segment;
    urma_jfr_wr_t wr{};
    wr.src.sge = &sge;
    wr.src.num_sge = 1;
    wr.user_ctx = kRx | slot;
    urma_jfr_wr_t* bad = nullptr;
    return urma_post_jfr_wr(res->rx, &wr, &bad) == URMA_SUCCESS;
  }
  void start() {
    DestructorGuard guard(owner);
    state = State::Handshake;
    started = Clock::now();
    if (!createResources()) {
      fail("URMA resource creation failed");
      return;
    }
    std::array<uint8_t, kHelloSize> h{};
    std::memcpy(h.data(), "UBS2", 4);
    put32(h.data() + 4, kHelloSize);
    const auto& id = res->jetty->jetty_id;
    std::memcpy(h.data() + 8, id.eid.raw, 16);
    put32(h.data() + 24, id.uasid);
    put32(h.data() + 28, id.id);
    put32(h.data() + 32, config.buffer_size);
    put32(h.data() + 36, URMA_TM_RM);
    tcp->setReadCB(this);
    sendHandshake(h.data(), h.size());
    armTimer();
  }
  void sendHandshake(const void* p, size_t n) {
    ++handshakeWrites;
    tcp->writeChain(this, IOBuf::copyBuffer(p, n));
  }
  void connectSuccess() noexcept override { start(); }
  void connectErr(const AsyncSocketException& e) noexcept override {
    fail(e.what());
  }
  void writeSuccess() noexcept override {
    DestructorGuard guard(owner);
    if (handshakeWrites)
      --handshakeWrites;
    becomeReady();
  }
  void writeErr(size_t, const AsyncSocketException& e) noexcept override {
    fail(e.what());
  }
  void getReadBuffer(void** p, size_t* n) override {
    *p = tcpBuffer.data();
    *n = tcpBuffer.size();
  }
  void readDataAvailable(size_t n) noexcept override {
    DestructorGuard guard(owner);
    if (!live())
      return;
    if (state == State::Open || handshake.size() + n > kHelloSize + 4) {
      fail("unexpected data on URMA handshake channel");
      return;
    }
    handshake.insert(handshake.end(), tcpBuffer.begin(), tcpBuffer.begin() + n);
    if (!haveHello && handshake.size() >= kHelloSize) {
      auto* h = handshake.data();
      if (std::memcmp(h, "UBS2", 4) || get32(h + 4) != kHelloSize ||
          get32(h + 36) != URMA_TM_RM || get32(h + 32) < 64 ||
          get32(h + 32) > 4096 || !get32(h + 28)) {
        fail("incompatible basic URMA Hello");
        return;
      }
      std::memcpy(peerId.eid.raw, h + 8, 16);
      peerId.uasid = get32(h + 24);
      peerId.id = get32(h + 28);
      peerBuffer = std::min(config.buffer_size, get32(h + 32));
      urma_rjetty_t remote{};
      remote.jetty_id = peerId;
      remote.trans_mode = URMA_TM_RM;
      remote.type = URMA_JETTY;
      remote.tp_type = URMA_CTP;
      remote.flag.bs.order_type = URMA_OI;
      urma_token_t token{};
      res->peer = urma_import_jetty(res->ctx, &remote, &token);
      if (!res->peer) {
        fail("URMA peer import failed errno=" + std::to_string(errno));
        return;
      }
      haveHello = true;
      handshake.erase(handshake.begin(), handshake.begin() + kHelloSize);
      uint8_t ready[4];
      put32(ready, kReady);
      sendHandshake(ready, sizeof(ready));
      if (!live())
        return;
    }
    if (haveHello && handshake.size() == 4) {
      if (get32(handshake.data()) != kReady) {
        fail("invalid URMA ready message");
        return;
      }
      handshake.clear();
      haveReady = true;
      becomeReady();
    }
  }
  void becomeReady() {
    if (state != State::Handshake || !haveReady || handshakeWrites)
      return;
    state = State::Open;
    armTimer();
    poll();
    flush();
    deliver();
  }
  void readEOF() noexcept override {
    DestructorGuard guard(owner);
    if (!live())
      return;
    if (state != State::Open) {
      fail("peer closed during URMA handshake");
      return;
    }
    // Local SEND completion precedes the peer's TCP FIN. Drain placed receives.
    poll();
    if (!live())
      return;
    peerEof = true;
    deliver();
    if (reads.empty() && reader) {
      auto* cb = std::exchange(reader, nullptr);
      cb->readEOF();
    }
  }
  void readErr(const AsyncSocketException& e) noexcept override {
    fail(e.what());
  }
  void handlerReady(uint16_t) noexcept override {
    DestructorGuard guard(owner);
    if (!live() || !res)
      return;
    ackEvents();
    poll();
    if (live() && res) {
      if (urma_rearm_jfc(res->cq, false) != URMA_SUCCESS) {
        fail("URMA CQ rearm failed");
        return;
      }
      poll(); // close the poll/rearm race
    }
  }
  void ackEvents() {
    if (!res || !res->event)
      return;
    urma_jfc_t* cq[1];
    int n;
    while ((n = urma_wait_jfc(res->event, 1, 0, cq)) > 0) {
      uint32_t counts[1] = {1};
      urma_ack_jfc(cq, counts, 1);
    }
  }
  void poll() {
    if (polling || !res || !res->cq || !live())
      return;
    polling = true;
    int n = 0;
    while (live() && !owner->getDestroyPending()) {
      // Providers do not populate every field of every completion type.
      urma_cr_t cr[32]{};
      n = urma_poll_jfc(res->cq, 32, cr);
      if (n <= 0)
        break;
      for (int i = 0; i < n && live() && !owner->getDestroyPending(); ++i) {
        const auto& c = cr[i];
        if (!c.flag.bs.s_r &&
            c.status == URMA_CR_RNR_RETRY_CNT_EXC_ERR && txActive &&
            c.user_ctx == txId && !writes.empty()) {
          // RM reports RNR only when the peer could not receive this SEND.
          // Keep its bytes queued and retry after yielding to the EventBase.
          // Never retry ambiguous errors such as ACK timeout.
          txActive = false;
          const auto now = Clock::now();
          if (rnrSince == Clock::time_point{})
            rnrSince = now;
          retryAt = now + kRnrRetryDelay;
          continue;
        }
        if (c.status != URMA_CR_SUCCESS) {
          fail("URMA completion failed: " + std::to_string(c.status));
          break;
        }
        if (c.flag.bs.s_r) {
          const uint64_t slot = c.user_ctx & ~kRx;
          if (!(c.user_ctx & kRx) || slot >= config.rx_depth ||
              !c.completion_len || c.completion_len > config.buffer_size ||
              c.opcode != URMA_CR_OPC_SEND || !haveHello ||
              c.remote_id.id != peerId.id ||
              // UDMA receive CQEs report the remote EID and jetty ID, but
              // not UASID. Do not compare an unreported field with the hello.
              std::memcmp(c.remote_id.eid.raw, peerId.eid.raw, 16)) {
            fail("invalid URMA receive completion");
            break;
          }
          if (reads.chainLength() + c.completion_len >
              config.max_pending_bytes) {
            fail("URMA pending receive limit reached");
            break;
          }
          auto data = IOBuf::copyBuffer(
              static_cast<char*>(res->memory) + slot * config.buffer_size,
              c.completion_len);
          if (!postReceive(slot)) {
            fail("URMA receive repost failed");
            break;
          }
          bytesRead += c.completion_len;
          reads.append(std::move(data));
          deliver();
        } else {
          if (c.user_ctx != txId || !txActive || writes.empty()) {
            fail("invalid URMA send completion");
            break;
          }
          txActive = false;
          rnrSince = retryAt = Clock::time_point{};
          pendingBytes -= txBytes;
          writes.front().completed += txBytes;
          bytesWritten += txBytes;
          auto& data = writes.front().data;
          data->trimStart(txBytes);
          while (data && data->length() == 0)
            data = data->pop();
          txBytes = 0;
          finishWrite();
        }
      }
    }
    polling = false;
    if (n < 0 && live()) {
      fail("URMA CQ polling failed");
      return;
    }
    if (live() && !owner->getDestroyPending())
      flush();
  }
  void finishWrite() {
    if (txActive || writes.empty() || writes.front().data)
      return;
    auto* cb = writes.front().callback;
    writes.pop_front();
    if (cb)
      cb->writeSuccess();
  }
  void flush() {
    if (sending || state != State::Open || owner->getDestroyPending())
      return;
    sending = true;
    while (state == State::Open && !txActive && !writes.empty() &&
           !owner->getDestroyPending()) {
      if (Clock::now() < retryAt)
        break;
      auto& request = writes.front();
      while (request.data && request.data->length() == 0)
        request.data = request.data->pop();
      if (!request.data) {
        finishWrite();
        continue;
      }
      const size_t bytes = std::min(request.data->length(), size_t(peerBuffer));
      auto* target = static_cast<char*>(res->memory) +
          size_t(config.rx_depth) * config.buffer_size;
      std::memcpy(target, request.data->data(), bytes);
      urma_sge_t sge{};
      sge.addr = reinterpret_cast<uint64_t>(target);
      sge.len = bytes;
      sge.tseg = res->segment;
      urma_jfs_wr_t wr{};
      wr.opcode = URMA_OPC_SEND;
      wr.tjetty = res->peer;
      // One outstanding RM SEND at a time: its successful completion confirms
      // remote placement before we submit the next portion of the byte stream.
      wr.flag.bs.place_order = URMA_STRONG_ORDER;
      wr.flag.bs.comp_order = 1;
      wr.flag.bs.complete_enable = 1;
      wr.send.src.sge = &sge;
      wr.send.src.num_sge = 1;
      wr.user_ctx = (txId + 1) & ~kRx;
      urma_jfs_wr_t* bad = nullptr;
      const auto st = urma_post_jetty_send_wr(res->jetty, &wr, &bad);
      if (st == URMA_EAGAIN)
        break;
      if (st != URMA_SUCCESS) {
        fail("URMA SEND submit failed: " + std::to_string(st));
        break;
      }
      txId = wr.user_ctx;
      txActive = true;
      txBytes = bytes;
      retryAt = Clock::time_point{};
    }
    sending = false;
    if (state == State::Open && writes.empty()) {
      if (closeAfterWrites) {
        stop(false);
        return;
      }
      if (writeShutdown)
        tcp->shutdownWrite();
    }
    armTimer();
  }
  void deliver() {
    if (delivering || state != State::Open || owner->getDestroyPending())
      return;
    delivering = true;
    while (reader && !reads.empty() && state == State::Open &&
           !owner->getDestroyPending()) {
      auto* cb = reader;
      if (cb->isBufferMovable()) {
        cb->readBufferAvailable(reads.move());
        continue;
      }
      void* p = nullptr;
      size_t length = 0;
      try {
        cb->getReadBuffer(&p, &length);
      } catch (const std::exception& e) {
        fail(e.what());
        break;
      } catch (...) {
        fail("read buffer callback threw");
        break;
      }
      if (state != State::Open || owner->getDestroyPending() || reader != cb)
        break;
      if (!p || !length) {
        fail("empty application read buffer");
        break;
      }
      length = std::min(length, reads.front()->length());
      std::memcpy(p, reads.front()->data(), length);
      reads.trimStart(length);
      cb->readDataAvailable(length);
    }
    delivering = false;
    if (state == State::Open && peerEof && reads.empty() && reader &&
        !owner->getDestroyPending()) {
      auto* cb = std::exchange(reader, nullptr);
      cb->readEOF();
    }
  }
  void enqueue(
      WriteCallback* cb, std::unique_ptr<IOBuf> data, WriteFlags flags) {
    if (!live() || writeShutdown || closeAfterWrites ||
        flags != WriteFlags::NONE) {
      if (cb)
        cb->writeErr(
            0,
            AsyncSocketException(
                AsyncSocketException::NOT_OPEN,
                "URMA write unavailable or unsupported flags"));
      return;
    }
    const size_t n = data ? data->computeChainDataLength() : 0;
    if (n > config.max_pending_bytes - pendingBytes) {
      if (cb)
        cb->writeErr(
            0,
            AsyncSocketException(
                AsyncSocketException::BAD_ARGS,
                "URMA pending write limit reached"));
      return;
    }
    pendingBytes += n;
    writes.push_back(Write{cb, std::move(data)});
    flush();
    armTimer();
  }
  void timeoutExpired() noexcept override {
    DestructorGuard guard(owner);
    if (!live())
      return;
    const auto now = Clock::now();
    if (state != State::Open &&
        now - started >=
            std::chrono::milliseconds(config.handshake_timeout_ms)) {
      fail("URMA handshake timeout");
      return;
    }
    if (sendTimeout && !writes.empty() &&
        now - writes.front().since >= std::chrono::milliseconds(sendTimeout)) {
      fail("URMA send timeout");
      return;
    }
    if (rnrSince != Clock::time_point{} && now - rnrSince >= kRnrTimeout) {
      fail("URMA receive-not-ready timeout");
      return;
    }
    if (state == State::Open) {
      poll();
      flush();
    }
    armTimer();
  }
  void fail(const std::string& message) {
    DestructorGuard guard(owner);
    if (!live())
      return;
    XLOG(ERR) << message;
    stop(true, message);
  }
  void stop(bool error, const std::string& message = "URMA connection closed") {
    if (!live())
      return;
    state = error ? State::Error : State::Closed;
    cancelTimeout();
    unregisterHandler();
    // Detach the TCP callbacks before aborting handshake writes.
    tcp->setReadCB(nullptr);
    tcp->closeNow();
    cleanup();
    txActive = false;
    txBytes = 0;
    pendingBytes = 0;
    reads.move();
    auto failed = std::move(writes);
    writes.clear();
    AsyncSocketException ex(AsyncSocketException::NOT_OPEN, message);
    for (auto& w : failed)
      if (w.callback)
        w.callback->writeErr(w.completed, ex);
    if (auto* cb = std::exchange(reader, nullptr)) {
      if (error)
        cb->readErr(ex);
      else
        cb->readEOF();
    }
  }
  void cleanup() {
    if (!res)
      return;
    ackEvents();
    bool ok = true;
    if (ok && res->jetty) {
      if (urma_delete_jetty(res->jetty) == URMA_SUCCESS)
        res->jetty = nullptr;
      else
        ok = false;
    }
    if (ok && res->peer) {
      if (urma_unimport_jetty(res->peer) == URMA_SUCCESS)
        res->peer = nullptr;
      else
        ok = false;
    }
    if (ok && res->rx) {
      if (urma_delete_jfr(res->rx) == URMA_SUCCESS)
        res->rx = nullptr;
      else
        ok = false;
    }
    if (ok && res->cq) {
      ackEvents();
      urma_cr_t cr[32];
      while (urma_poll_jfc(res->cq, 32, cr) > 0) {
      }
      if (urma_delete_jfc(res->cq) == URMA_SUCCESS)
        res->cq = nullptr;
      else
        ok = false;
    }
    if (ok && res->event) {
      if (urma_delete_jfce(res->event) == URMA_SUCCESS)
        res->event = nullptr;
      else
        ok = false;
    }
    if (ok && res->segment) {
      if (urma_unregister_seg(res->segment) == URMA_SUCCESS)
        res->segment = nullptr;
      else
        ok = false;
    }
    if (!ok) {
      XLOG(ERR)
          << "URMA resource teardown failed; preserving registered memory and context owner";
      (void)res.release();
      return;
    }
    std::free(res->memory);
    if (res->ctx)
      UrmaInit::releaseContext();
    res.reset();
  }
};

UrmaAsyncSocket::UrmaAsyncSocket(EventBase* e, const SocketAddress& address)
    : impl_(std::make_unique<Impl>(this, e)) {
  impl_->tcp = AsyncSocket::newSocket(e);
  impl_->armTimer();
  impl_->tcp->connect(impl_.get(), address, impl_->config.handshake_timeout_ms);
}
UrmaAsyncSocket::UrmaAsyncSocket(EventBase* e, int fd)
    : UrmaAsyncSocket(AsyncSocket::newSocket(e, NetworkSocket(fd))) {}
UrmaAsyncSocket::UrmaAsyncSocket(AsyncSocket::UniquePtr acceptedSocket)
    : impl_(std::make_unique<Impl>(this, acceptedSocket->getEventBase())) {
  impl_->tcp = std::move(acceptedSocket);
  impl_->start();
}
UrmaAsyncSocket::~UrmaAsyncSocket() {
  impl_->reader = nullptr;
  impl_->stop(false);
}
void UrmaAsyncSocket::setReadCB(ReadCallback* cb) noexcept {
  DestructorGuard guard(this);
  impl_->reader = cb;
  if (!cb)
    return;
  if (!impl_->live()) {
    impl_->reader = nullptr;
    if (error())
      cb->readErr(
          AsyncSocketException(AsyncSocketException::NOT_OPEN, "URMA failed"));
    else
      cb->readEOF();
  } else
    impl_->deliver();
}
AsyncTransport::ReadCallback* UrmaAsyncSocket::getReadCallback()
    const noexcept {
  return impl_->reader;
}
void UrmaAsyncSocket::write(
    WriteCallback* cb, const void* p, size_t n, WriteFlags flags) noexcept {
  writeChain(cb, IOBuf::copyBuffer(p, n), flags);
}
void UrmaAsyncSocket::writev(
    WriteCallback* cb, const iovec* vec, size_t n, WriteFlags flags) noexcept {
  writeChain(cb, IOBuf::wrapIov(vec, n), flags);
}
void UrmaAsyncSocket::writeChain(
    WriteCallback* cb, std::unique_ptr<IOBuf>&& p, WriteFlags flags) noexcept {
  DestructorGuard guard(this);
  impl_->enqueue(cb, std::move(p), flags);
}
EventBase* UrmaAsyncSocket::getEventBase() const {
  return impl_->evb;
}
void UrmaAsyncSocket::close() {
  DestructorGuard guard(this);
  impl_->closeAfterWrites = true;
  if (impl_->writes.empty())
    impl_->stop(false);
  else
    impl_->flush();
}
void UrmaAsyncSocket::closeNow() {
  DestructorGuard guard(this);
  impl_->stop(false);
}
void UrmaAsyncSocket::shutdownWrite() {
  DestructorGuard guard(this);
  impl_->writeShutdown = true;
  impl_->flush();
}
void UrmaAsyncSocket::shutdownWriteNow() {
  closeNow();
}
bool UrmaAsyncSocket::good() const {
  return impl_->live() && !impl_->peerEof && !impl_->writeShutdown &&
      !impl_->closeAfterWrites;
}
bool UrmaAsyncSocket::writable() const {
  return impl_->live() && !impl_->writeShutdown && !impl_->closeAfterWrites;
}
bool UrmaAsyncSocket::readable() const {
  return impl_->live() && (!impl_->peerEof || !impl_->reads.empty());
}
bool UrmaAsyncSocket::connecting() const {
  return impl_->state == Impl::State::Connecting ||
      impl_->state == Impl::State::Handshake;
}
bool UrmaAsyncSocket::error() const {
  return impl_->state == Impl::State::Error;
}
void UrmaAsyncSocket::attachEventBase(EventBase* e) {
  if (e != impl_->evb)
    throw std::logic_error("basic URMA transport cannot migrate EventBase");
}
void UrmaAsyncSocket::detachEventBase() {
  throw std::logic_error("basic URMA transport cannot detach EventBase");
}
void UrmaAsyncSocket::setSendTimeout(uint32_t ms) {
  impl_->sendTimeout = ms;
  impl_->armTimer();
}
uint32_t UrmaAsyncSocket::getSendTimeout() const {
  return impl_->sendTimeout;
}
void UrmaAsyncSocket::getLocalAddress(SocketAddress* a) const {
  impl_->tcp->getLocalAddress(a);
}
void UrmaAsyncSocket::getPeerAddress(SocketAddress* a) const {
  impl_->tcp->getPeerAddress(a);
}
void UrmaAsyncSocket::setEorTracking(bool enabled) {
  if (enabled)
    throw std::logic_error("basic URMA transport does not support EOR");
}
size_t UrmaAsyncSocket::getAppBytesWritten() const {
  return impl_->bytesWritten;
}
size_t UrmaAsyncSocket::getRawBytesWritten() const {
  return impl_->bytesWritten;
}
size_t UrmaAsyncSocket::getAppBytesReceived() const {
  return impl_->bytesRead;
}
size_t UrmaAsyncSocket::getRawBytesReceived() const {
  return impl_->bytesRead;
}
bool UrmaAsyncSocket::isUrmaReady() const {
  return impl_->state == Impl::State::Open;
}
} // namespace folly
#endif
