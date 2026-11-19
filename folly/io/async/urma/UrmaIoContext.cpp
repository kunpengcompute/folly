/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaIoContext.h>
#ifdef FOLLY_HAVE_URMA
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iterator>
#include <list>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <folly/ScopeGuard.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBaseLocal.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/memory/IoBufPool.h>

namespace folly {
namespace {
constexpr uint64_t kRx = uint64_t(1) << 63;
std::atomic<size_t> exposedReads{0};
std::atomic<uint64_t> nextSocket{1}, attempts{0}, created{0}, destroyed{0},
    live{0}, peak{0};
EventBaseLocal<std::unique_ptr<UrmaIoContext>> contexts;
uint64_t allocateSocket() {
  auto n = nextSocket.load(std::memory_order_relaxed);
  while (n && n != UINT64_MAX) {
    if (nextSocket.compare_exchange_weak(n, n + 1, std::memory_order_relaxed))
      return n;
  }
  throw std::overflow_error("URMA socket IDs exhausted");
}
} // namespace

struct UrmaIoContext::Peer : urma_detail::CleanupTask {
  urma_context_t* context{nullptr};
  urma_target_jetty_t* target{nullptr};
  std::shared_ptr<std::atomic<bool>> released{new std::atomic<bool>(false)};
  bool tryCleanup() noexcept override {
    if (target) {
      const auto st = urma_unimport_jfr(target);
      if (st != URMA_SUCCESS)
        return failed("unimport_jfr", st);
      target = nullptr;
    }
    if (context) {
      context = nullptr;
      UrmaInit::releaseContext();
    }
    released->store(true, std::memory_order_release);
    return true;
  }
};

struct UrmaIoContext::Impl : EventHandler, AsyncTimeout {
  struct ReadBudget {
    std::atomic<size_t> pending{0};
  };
  struct TxCompletionCtx {
    uint64_t socket{0}, user{0};
    uint32_t index{0};
    Listener* listener{nullptr};
    bool read{false};
    bool* readObserved{nullptr};
    urma_jfs_wr_t work{};
    std::vector<urma_sge_t> source;
    urma_sge_t destination{};
    std::shared_ptr<void> hold;
  };
  struct Slot {
    // Fixed owner storage: user_ctx points here until the terminal CQE.
    TxCompletionCtx completion;
    UrmaInit::Buffer buffer;
    std::unique_ptr<IOBuf> controlBuffer;
    bool priority{false};
    bool reserved{false};
    bool submitted{false};
  };
  struct Hardware : urma_detail::CleanupTask {
    urma_context_t* ctx{nullptr};
    urma_jfce_t* event{nullptr};
    urma_jfc_t* cq{nullptr};
    urma_jfs_t* tx{nullptr};
    urma_jfr_t* rx{nullptr};
    std::vector<UrmaInit::Buffer> receiveBuffers;
    std::vector<Slot> slots;
    bool counted{false};
    bool ack(bool drain = true) {
      if (!event) return true;
      urma_jfc_t* queues[1];
      while (true) {
        const int n = urma_wait_jfc(event, 1, 0, queues);
        if (n < 0) return failed("wait_jfc", n);
        if (!n) return true;
        uint32_t count[1] = {1};
        urma_ack_jfc(queues, count, 1);
        // Remaining notifications keep the level-triggered event readable.
        if (!drain) return true;
      }
    }
    bool tryCleanup() noexcept override {
      if (!ack()) return false;
      urma_status_t st;
      if (tx) {
        st = urma_delete_jfs(tx);
        if (st != URMA_SUCCESS)
          return failed("delete_jfs", st);
        tx = nullptr;
      }
      // Successful JFS destruction is the local WR teardown barrier. In
      // particular, do not release READ destinations on a socket timeout.
      slots.clear();
      if (rx) {
        st = urma_delete_jfr(rx);
        if (st != URMA_SUCCESS)
          return failed("delete_jfr", st);
        rx = nullptr;
      }
      receiveBuffers.clear();
      if (cq) {
        if (!ack()) return false;
        urma_cr_t cr[32];
        int n;
        do { n = urma_poll_jfc(cq, 32, cr); } while (n > 0);
        if (n < 0) return failed("poll_jfc", n);
        st = urma_delete_jfc(cq);
        if (st != URMA_SUCCESS)
          return failed("delete_jfc", st);
        cq = nullptr;
      }
      if (event) {
        st = urma_delete_jfce(event);
        if (st != URMA_SUCCESS)
          return failed("delete_jfce", st);
        event = nullptr;
      }
      if (ctx) {
        ctx = nullptr;
        UrmaInit::releaseContext();
      }
      if (counted) {
        counted = false;
        ++destroyed;
        --live;
      }
      return true;
    }
  };
  struct Connection {
    Listener* listener;
    bool waiting{false}, read{false}, data{false}, freeWaiting{false};
    size_t deferred{0};
    std::list<uint64_t>::iterator waiter;
  };
  EventBase* evb;
  UrmaConfig config;
  uint32_t rxTarget;
  std::unique_ptr<Hardware> hw{new Hardware};
  std::unordered_map<uint64_t, Connection> sockets;
  std::unordered_map<uint64_t, std::shared_ptr<Retired>> retired;
  std::unordered_map<std::string, std::weak_ptr<Peer>> imports;
  std::list<uint64_t> waiters, freeWaiters;
  std::vector<uint32_t> freeSlots;
  std::vector<uint32_t> pendingRx;
  struct DeferredReceive {
    urma_detail::PacketHeader header;
    urma_cr_t completion{};
    std::chrono::steady_clock::time_point since;
    std::unique_ptr<IOBuf> data;
  };
  std::list<DeferredReceive> deferredRx;
  uint32_t initialRx{0};
  bool initialized{false};
  std::shared_ptr<UrmaPoolSignal> poolSignal{new UrmaPoolSignal};
  struct PoolHandler : EventHandler {
    Impl& owner;
    PoolHandler(Impl& o) : EventHandler(o.evb, NetworkSocket(o.poolSignal->fd())), owner(o) {}
    void handlerReady(uint16_t) noexcept override try {
      owner.poolSignal->consume();
      const auto ids = UrmaInit::revocableSegments();
      if (!ids.empty()) {
        owner.notifyingFailure = true; // prohibit owner teardown in callbacks
        auto reset = makeGuard([&] { owner.notifyingFailure = false; });
        std::vector<uint64_t> sockets;
        for (const auto& entry : owner.sockets) sockets.push_back(entry.first);
        for (auto id : sockets) {
          auto it = owner.sockets.find(id);
          if (it != owner.sockets.end()) it->second.listener->revokeSegments(ids);
        }
      }
      owner.timeoutExpired();
    } catch (const std::bad_alloc&) {
      owner.fail("URMA pool notification allocation failed");
    }
  };
  std::unique_ptr<PoolHandler> poolHandler;
  // Each entry owns one SQ slot and one READ context, including while a
  // known-unaccepted suffix waits for retry. Never wait to fill a batch.
  std::deque<uint32_t> pendingReads;
  bool readBlocked{false};
  std::shared_ptr<ReadBudget> readBudget{new ReadBudget};
  struct SegmentEntry {
    std::weak_ptr<ImportedSegment> segment;
    std::shared_ptr<std::atomic<bool>> released;
  };
  std::unordered_map<std::string, SegmentEntry> readImports;
  uint64_t sends{0}, receives{0}, reads{0}, completions{0};
  uint64_t readConnections{0}, dataConnections{0};
  bool polling{false}, waking{false}, notifyingFailure{false}, broken{false};
  bool handlingEvent{false};
  using ReadyReads = boost::intrusive::list<
      EventBase::LoopCallback, boost::intrusive::constant_time_size<false>>;
  ReadyReads readyReads;
  uint32_t timerInterval{0};

  explicit Impl(EventBase* e)
      : EventHandler(e),
        AsyncTimeout(e),
        evb(e),
        config(UrmaInit::getConfig()),
        rxTarget(config.rx_target ? config.rx_target : config.rx_depth) {}
  ~Impl() override {
    cancelTimeout();
    unregisterHandler();
    poolSignal->stop();
    poolHandler.reset();
    urma_detail::cleanupOrDefer(std::move(hw));
  }
  void armTimer(uint32_t interval) {
    // Keep the earlier poll/retry deadline instead of rearming for each WR.
    if (!isScheduled() || interval < timerInterval) {
      scheduleTimeout(interval);
      timerInterval = interval;
    }
  }
  void checkThread() const {
    if (!evb->isInEventBaseThread())
      throw std::logic_error("URMA owner thread mismatch");
  }
  bool init() {
    checkThread();
    if (broken) throw std::runtime_error("URMA shared owner failed");
    if (initialized) return true;
    if (!hw->ctx) {
      ++attempts;
      hw->ctx = UrmaInit::acquireContext();
      if (!hw->ctx) throw std::runtime_error("URMA context unavailable");
      hw->slots.resize(config.tx_depth);
      for (auto& slot : hw->slots)
        slot.completion.source.resize(config.max_send_sge);
      for (uint32_t i = config.tx_depth; i; --i) freeSlots.push_back(i - 1);
      hw->receiveBuffers.resize(rxTarget);
      if (!poolHandler) {
        poolHandler.reset(new PoolHandler(*this));
        if (!poolHandler->registerHandler(READ | PERSIST))
          throw std::runtime_error("URMA pool event registration failed");
        UrmaInit::subscribePool(poolSignal);
      }
    }
    if (!hw->event) {
      hw->event = urma_create_jfce(hw->ctx);
      if (!hw->event) throw std::runtime_error("URMA shared event creation failed");
    }
    if (!hw->cq) {
      urma_jfc_cfg_t cq{};
      cq.depth = config.rx_depth + config.tx_depth + 1;
      cq.jfce = hw->event;
      hw->cq = urma_create_jfc(hw->ctx, &cq);
      if (!hw->cq) throw std::runtime_error("URMA shared CQ creation failed");
    }
    if (!hw->rx) {
      urma_jfr_cfg_t rx{};
      rx.depth = config.rx_depth;
      rx.max_sge = 1;
      rx.jfc = hw->cq;
      rx.trans_mode = URMA_TM_RM;
      rx.flag.bs.order_type = URMA_OI;
      rx.min_rnr_timer = 12;
      hw->rx = urma_create_jfr(hw->ctx, &rx);
      if (!hw->rx) throw std::runtime_error("URMA shared JFR creation failed");
    }
    if (!hw->tx) {
      urma_jfs_cfg_t tx{};
      tx.depth = config.tx_depth;
      tx.max_sge = config.max_send_sge;
      tx.jfc = hw->cq;
      tx.trans_mode = URMA_TM_RM;
      tx.flag.bs.order_type = URMA_OI;
      tx.rnr_retry = URMA_TYPICAL_RNR_RETRY;
      tx.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
      hw->tx = urma_create_jfs(hw->ctx, &tx);
      if (!hw->tx) throw std::runtime_error("URMA shared JFS creation failed");
    }
    while (initialRx < rxTarget) {
      auto st = postRx(initialRx);
      if (st == URMA_EAGAIN) return false;
      if (st != URMA_SUCCESS)
        throw std::runtime_error("URMA initial receive post failed");
      ++initialRx;
    }
    changeHandlerFD(NetworkSocket(hw->event->fd));
    if (!registerHandler(READ | PERSIST) ||
        urma_rearm_jfc(hw->cq, false) != URMA_SUCCESS)
      throw std::runtime_error("URMA shared CQ registration failed");
    initialized = hw->counted = true;
    ++created;
    auto count = ++live;
    auto old = peak.load();
    while (count > old && !peak.compare_exchange_weak(old, count)) {}
    return true;
  }
  void cancelPreparation() {
    if (initialized || !sockets.empty()) return;
    unregisterHandler();
    cancelTimeout();
    urma_detail::cleanupOrDefer(std::move(hw));
    hw.reset(new Hardware);
    freeSlots.clear();
    deferredRx.clear();
    initialRx = 0;
  }
  urma_status_t postRx(uint32_t i) {
    auto& buffer = hw->receiveBuffers[i];
    if (!buffer.memory)
      buffer =
          UrmaInit::allocateBuffer(config.buffer_size, poolSignal.get());
    if (!buffer.memory)
      return URMA_EAGAIN;
    urma_sge_t sg{};
    sg.addr = reinterpret_cast<uint64_t>(buffer.memory);
    sg.len = config.buffer_size;
    sg.tseg = buffer.segment;
    urma_jfr_wr_t wr{};
    wr.src.sge = &sg;
    wr.src.num_sge = 1;
    wr.user_ctx = kRx | i;
    urma_jfr_wr_t* bad = nullptr;
    auto st = urma_post_jfr_wr(hw->rx, &wr, &bad);
    // Only an explicitly unaccepted WR can be retried.
    if (st == URMA_EAGAIN && bad != &wr)
      return URMA_EINVAL;
    return st;
  }
  void fail(const char* why) {
    if (broken)
      return;
    broken = true;
    notifyingFailure = true;
    unregisterHandler();
    cancelTimeout();
    std::vector<uint64_t> ids;
    for (const auto& item : sockets)
      ids.push_back(item.first);
    for (auto id : ids) {
      auto found = sockets.find(id);
      if (found != sockets.end())
        found->second.listener->failed(why);
    }
    notifyingFailure = false;
  }
  void wait(uint64_t id, bool freeNotification = false) {
    auto found = sockets.find(id);
    if (found == sockets.end())
      return;
    auto& c = found->second;
    if (c.waiting) {
      if (!freeNotification || c.freeWaiting)
        return;
      waiters.erase(c.waiter);
    }
    auto& queue = freeNotification ? freeWaiters : waiters;
    queue.push_back(id);
    c.waiter = std::prev(queue.end());
    c.waiting = true;
    c.freeWaiting = freeNotification;
  }
  void wake() {
    if (waking || broken)
      return;
    waking = true;
    size_t budget = std::min<size_t>(64, waiters.size() + freeWaiters.size());
    while (budget-- && !freeSlots.empty() &&
           (!freeWaiters.empty() || !waiters.empty()) && !broken) {
      auto& queue = freeWaiters.empty() ? waiters : freeWaiters;
      auto id = queue.front();
      queue.pop_front();
      auto found = sockets.find(id);
      if (found == sockets.end())
        continue;
      found->second.waiting = false;
      found->second.freeWaiting = false;
      found->second.listener->writable();
    }
    waking = false;
  }
  void flushReads() {
    while (!pendingReads.empty() && !broken) {
      std::array<uint32_t, 32> batch;
      const size_t count = std::min(batch.size(), pendingReads.size());
      for (size_t i = 0; i < count; ++i) {
        batch[i] = pendingReads.front();
        pendingReads.pop_front();
        auto& slot = hw->slots[batch[i]];
        slot.submitted = true;
        slot.completion.work.next = nullptr;
        if (i)
          hw->slots[batch[i - 1]].completion.work.next = &slot.completion.work;
      }
      urma_jfs_wr_t* bad = nullptr;
      auto status =
          urma_post_jfs_wr(hw->tx, &hw->slots[batch[0]].completion.work, &bad);
      size_t accepted = count;
      if (status != URMA_SUCCESS) {
        for (size_t i = 0; i < count; ++i)
          if (bad == &hw->slots[batch[i]].completion.work) {
            accepted = i;
            break;
          }
      }
      // The SDK consumes the descriptor chain during post. CQEs still refer
      // to individual stable slots; later slot reuse must not retain links.
      for (size_t i = 0; i < count; ++i)
        hw->slots[batch[i]].completion.work.next = nullptr;
      if (status != URMA_SUCCESS && accepted == count) {
        // No trustworthy failed-WR boundary. Any entry may be in DMA: retain
        // every hold until its CQE or successful hardware teardown.
        fail("URMA READ batch acceptance unknown");
        return;
      }
      reads += accepted;
      for (size_t i = count; i > accepted; --i) {
        hw->slots[batch[i - 1]].submitted = false;
        pendingReads.push_front(batch[i - 1]);
      }
      if (status == URMA_EAGAIN) {
        readBlocked = true;
        return; // Keep the suffix; CQ progress or the existing timer retries.
      }
      if (status != URMA_SUCCESS) {
        fail(("URMA READ batch submit failed: " + std::to_string(status))
                 .c_str());
        return;
      }
    }
  }
  void receive(const urma_cr_t& c) {
    const auto slot = uint32_t(c.user_ctx & ~kRx);
    auto& buffer = hw->receiveBuffers[slot];
    auto* bytes = static_cast<uint8_t*>(buffer.memory);
    urma_detail::PacketHeader header{};
    if (!urma_detail::decodeHeader(bytes, c.completion_len, header)) {
      fail("invalid URMA shared packet header"); return;
    }
    auto data = UrmaInit::takeBuffer(std::move(buffer), c.completion_len);
    buffer = {};
    data->trimStart(urma_detail::kHeaderSize);
    auto st = postRx(slot);
    if (st == URMA_EAGAIN) pendingRx.push_back(slot);
    else if (st != URMA_SUCCESS) {
      fail("URMA shared receive repost failed"); return;
    }
    auto found = sockets.find(header.destination);
    if (found != sockets.end() &&
        !urma_detail::acceptsDecodedWire(found->second.listener->protocol, header.wire)) {
      found->second.listener->failed("unexpected URMA wire version");
      return;
    }
    if (found != sockets.end() &&
        !found->second.listener->receiveReady(header, c, data->data(), data->length())) {
      // Keep the completed buffer, not its JFR slot. Otherwise later records
      // can occupy every slot while an earlier RNR retry needs one to arrive.
      // These leases remain charged to the registered pool's hard limit;
      // postRx() waits for memory if no replacement buffer is available.
      deferredRx.push_back({header, c, std::chrono::steady_clock::now(), std::move(data)});
      ++found->second.deferred;
      return;
    }
    dispatchReceive(header, c, std::move(data),
                    found == sockets.end() ? nullptr : &found->second);
  }
  void dispatchReceive(const urma_detail::PacketHeader& header,
                       const urma_cr_t& c, std::unique_ptr<IOBuf> data,
                       Connection* connection) {
    ++receives;
    if (connection) {
      if (header.kind == urma_detail::PacketKind::Data && !connection->data) {
        connection->data = true;
        ++dataConnections;
      }
      connection->listener->receive(header, c, std::move(data));
    } else {
      auto old = retired.find(header.destination);
      if (old != retired.end() && urma_detail::acceptsDecodedWire(old->second->protocol, header.wire))
        old->second->receive(header, c, std::move(data));
    }
  }
  void retryReceives() {
    // One bounded pass; a later sequence can become ready on the next CQ/timer
    // turn. Keep its original deadline even while other records make progress.
    for (auto it = deferredRx.begin(); it != deferredRx.end() && !broken;) {
      auto& d = *it;
      auto found = sockets.find(d.header.destination);
      if (found != sockets.end()) {
        if (std::chrono::steady_clock::now() - d.since >= std::chrono::seconds(30)) {
          found->second.listener->failed("URMA receive pressure timeout");
          found = sockets.find(d.header.destination);
        } else if (!found->second.listener->receiveReady(
                       d.header, d.completion, d.data->data(), d.data->length())) {
          ++it;
          continue;
        }
      }
      if (found != sockets.end()) --found->second.deferred;
      auto ready = std::move(d);
      it = deferredRx.erase(it);
      dispatchReceive(ready.header, ready.completion, std::move(ready.data),
                      found == sockets.end() ? nullptr : &found->second);
    }
  }
  void poll() try {
    checkThread();
    if (polling || broken || !initialized)
      return;
    polling = true;
    progressRetired();
    retryReceives();
    while (!broken) {
      // Flush the remaining READs without retrying an early EAGAIN before
      // polling for new completions. The timer also retries an idle suffix.
      if (!readBlocked)
        flushReads();
      readBlocked = false;
      if (broken)
        break;
      urma_cr_t cr[32]{};
      int n = urma_poll_jfc(hw->cq, 32, cr);
      if (n < 0) {
        fail("URMA shared CQ polling failed");
        break;
      }
      if (!n)
        break;
      for (int i = 0; i < n && !broken; ++i) {
        // Overlap READ DMA with further CQ dispatch, after callbacks finish.
        if (!readBlocked && pendingReads.size() >= 2) {
          flushReads();
          if (broken) break;
        }
        auto c = cr[i];
        ++completions;
        if (c.flag.bs.s_r) {
          auto slot = c.user_ctx & ~kRx;
          if (c.status != URMA_CR_SUCCESS || !(c.user_ctx & kRx) ||
              slot >= rxTarget || c.completion_len > config.buffer_size ||
              (c.opcode != URMA_CR_OPC_SEND &&
               c.opcode != URMA_CR_OPC_SEND_WITH_IMM)) {
            fail("invalid URMA shared receive completion");
            break;
          }
          receive(c);
        } else {
          // Validate the fixed storage address before dereferencing user_ctx.
          const auto base =
              reinterpret_cast<uintptr_t>(&hw->slots.front().completion);
          const auto offset = c.user_ctx - base;
          if (c.user_ctx < base || offset % sizeof(Slot) ||
              offset / sizeof(Slot) >= hw->slots.size()) {
            fail("invalid URMA shared send completion");
            break;
          }
          auto& slot = hw->slots[offset / sizeof(Slot)];
          if (!slot.reserved || !slot.submitted) {
            fail("inactive URMA shared send completion");
            break;
          }
          auto* ctx = &slot.completion;
          // Callbacks may close the socket or reuse this slot immediately.
          auto hold = std::move(ctx->hold);
          auto* listener = ctx->listener;
          const bool read = ctx->read;
          slot.buffer = {};
          slot.controlBuffer.reset();
          slot.reserved = slot.submitted = false;
          freeSlots.push_back(ctx->index);
          c.user_ctx = ctx->user;
          if (listener) {
            // Detach transfers or clears this pointer. Completion routing
            // does not consult the RX socket map or decode bits from user_ctx.
            if (ctx->readObserved && ctx->read && c.status == URMA_CR_SUCCESS &&
                !*ctx->readObserved) {
              *ctx->readObserved = true;
              ++readConnections;
            }
            listener->completed(c, read);
          }
          // hold survives callbacks, including close/destroy and slot reuse.
        }
      }
      wake();
      retryReceives();
    }
    progressRetired();
    polling = false;
    wake();
    if (!broken) {
      if (!deferredRx.empty() || !pendingReads.empty() || !pendingRx.empty() || !waiters.empty() ||
          !freeWaiters.empty())
        armTimer(1);
      else if (freeSlots.size() != hw->slots.size() || !retired.empty())
        armTimer(10);
      else
        cancelTimeout();
    }
  } catch (const std::bad_alloc&) {
    polling = waking = false;
    fail("URMA completion metadata allocation failed");
  }
  void progressRetired() {
    if (retired.empty() || broken) return;
    std::vector<uint64_t> ids;
    for (const auto& entry : retired) ids.push_back(entry.first);
    for (auto id : ids) {
      auto it = retired.find(id);
      if (it == retired.end()) continue;
      auto hold = it->second;
      if (hold->progress()) retired.erase(id);
    }
  }
  void deliverReadyReads() {
    // Callbacks may close another queued socket or poll reentrantly. Unlink
    // before calling, and leave newly queued work for the following pass.
    ReadyReads ready;
    ready.swap(readyReads);
    while (!ready.empty()) {
      auto& callback = ready.front();
      ready.pop_front();
      callback.runLoopCallback();
    }
  }
  void handlerReady(uint16_t) noexcept override {
    handlingEvent = true;
    auto reset = makeGuard([&] { handlingEvent = false; });
    if (!hw->ack(false)) {
      fail("URMA shared CQ event wait failed");
      return;
    }
    poll();
    deliverReadyReads();
    if (!broken) {
      if (urma_rearm_jfc(hw->cq, false) != URMA_SUCCESS) {
        fail("URMA shared CQ rearm failed");
        return;
      }
      poll();
      deliverReadyReads();
    }
    // Reentrant application work must not indefinitely extend this handler.
    while (!readyReads.empty()) {
      auto& callback = readyReads.front();
      readyReads.pop_front();
      evb->runInLoop(&callback, false, nullptr);
    }
  }
  void timeoutExpired() noexcept override try {
    if (!initialized || broken) return;
    auto pending = std::move(pendingRx);
    pendingRx.clear();
    for (auto i : pending) {
      auto st = postRx(i);
      if (st == URMA_EAGAIN)
        pendingRx.push_back(i);
      else if (st != URMA_SUCCESS) {
        fail("URMA shared receive retry failed");
        return;
      }
    }
    poll();
  } catch (const std::bad_alloc&) {
    fail("URMA receive metadata allocation failed");
  }
};

UrmaIoContext::UrmaIoContext(EventBase* evb) : impl_(new Impl(evb)) {
  impl_->init();
}
UrmaIoContext::~UrmaIoContext() = default;
bool UrmaIoContext::prepare() {
  try { return impl_->init(); }
  catch (...) { impl_->fail("URMA shared owner preparation failed"); throw; }
}
UrmaIoContext& UrmaIoContext::get(EventBase* evb) {
  if (!evb->isInEventBaseThread())
    throw std::logic_error("URMA owner thread mismatch");
  return *contexts.try_emplace_with(*evb, [&] {
    if (UrmaInit::pendingCleanupCount() >= 256)
      throw std::runtime_error("URMA failed-resource retention limit reached");
    return std::unique_ptr<UrmaIoContext>(new UrmaIoContext(evb));
  });
}
void UrmaIoContext::release(EventBase* evb) {
  if (!evb->isInEventBaseThread())
    throw std::logic_error("URMA owner thread mismatch");
  auto* ctx = contexts.get(*evb);
  if (ctx && !(*ctx)->impl_->sockets.empty())
    throw std::logic_error("URMA owner still has sockets");
  if (ctx &&
      ((*ctx)->impl_->polling || (*ctx)->impl_->waking ||
       (*ctx)->impl_->handlingEvent ||
       (*ctx)->impl_->notifyingFailure))
    throw std::logic_error("URMA owner is dispatching callbacks");
  contexts.erase(*evb);
}
UrmaIoContext::Counters UrmaIoContext::counters() {
  return {
      attempts.load(),
      created.load(),
      destroyed.load(),
      live.load(),
      peak.load()};
}
uint64_t UrmaIoContext::allocateSocketId() {
  return allocateSocket();
}
uint64_t UrmaIoContext::attach(Listener* listener, uint64_t id) {
  impl_->checkThread();
  if (impl_->broken)
    throw std::runtime_error("URMA owner failed");
  if (UrmaInit::pendingCleanupCount() >= 256 || impl_->retired.size() >= 1024)
    throw std::runtime_error("URMA failed-resource retention limit reached");
  if (!id)
    id = allocateSocket();
  if (!impl_->sockets.emplace(id, Impl::Connection{listener}).second)
    throw std::logic_error("duplicate URMA socket ID");
  return id;
}
void UrmaIoContext::detach(uint64_t id) {
  impl_->checkThread();
  auto found = impl_->sockets.find(id);
  if (found == impl_->sockets.end())
    return;
  auto retired = impl_->retired.find(id);
  auto* receiver = retired == impl_->retired.end() ? nullptr : retired->second.get();
  if (receiver)
    for (const auto& packet : impl_->deferredRx)
      if (packet.header.destination == id)
        receiver->discardReceive(packet.header);
  if (found->second.waiting)
    (found->second.freeWaiting ? impl_->freeWaiters : impl_->waiters)
        .erase(found->second.waiter);
  for (auto it = impl_->pendingReads.begin();
       it != impl_->pendingReads.end();) {
    auto& slot = impl_->hw->slots[*it];
    if (slot.completion.socket != id) {
      ++it;
      continue;
    }
    auto index = *it;
    it = impl_->pendingReads.erase(it);
    if (receiver)
      receiver->adopt(slot.completion.user, true, false);
    cancel(index); // Not accepted by the SDK: no DMA can reference this hold.
  }
  bool submitted = false;
  for (auto& slot : impl_->hw->slots)
    if (slot.submitted && slot.completion.socket == id) {
      submitted = true;
      auto& c = slot.completion;
      c.listener = receiver && receiver->adopt(c.user, c.read, true)
          ? receiver : nullptr;
      c.readObserved = nullptr;
    }
  impl_->sockets.erase(found);
  impl_->cancelPreparation();
  // The detached socket can no longer poll on timeout. Keep that fallback
  // while its submitted WRs still retain DMA resources.
  if ((submitted || !impl_->deferredRx.empty()) && !impl_->broken)
    impl_->armTimer(1);
}
void UrmaIoContext::retire(
    uint64_t id, std::shared_ptr<Retired> callback) {
  impl_->checkThread();
  impl_->retired.emplace(id, std::move(callback));
  if (!impl_->broken) impl_->armTimer(1);
}
const urma_jfr_id_t& UrmaIoContext::endpoint() const {
  return impl_->hw->rx->jfr_id;
}
std::shared_ptr<UrmaIoContext::Peer> UrmaIoContext::importPeer(
    const urma_jfr_id_t& id) {
  impl_->checkThread();
  // All imports use RM/CTP/OI, no token, and URMA_JFR. Scope the key by the
  // actual SDK context as well as EID/UASID/ID, never by a numeric ID alone.
  static std::mutex mutex;
  struct Entry {
    std::weak_ptr<Peer> peer;
    std::shared_ptr<std::atomic<bool>> released;
  };
  static std::unordered_map<std::string, Entry> peers;
  std::string key(
      reinterpret_cast<const char*>(&impl_->hw->ctx), sizeof(impl_->hw->ctx));
  key.append(reinterpret_cast<const char*>(id.eid.raw), 16);
  uint8_t tail[8];
  urma_detail::store32(tail, id.uasid);
  urma_detail::store32(tail + 4, id.id);
  key.append(reinterpret_cast<const char*>(tail), sizeof(tail));
  std::lock_guard<std::mutex> lock(mutex);
  auto found = peers.find(key);
  if (found != peers.end()) {
    if (auto p = found->second.peer.lock()) {
      impl_->imports[key] = p;
      return p;
    }
    // A weak reference expires before a deferred SDK unimport completes.
    // Never create a second handle for that identity during retirement.
    if (!found->second.released->load(std::memory_order_acquire))
      return {};
  }
  if (peers.size() >= 256) {
    for (auto it = peers.begin(); it != peers.end();)
      if (it->second.released->load(std::memory_order_acquire))
        it = peers.erase(it);
      else
        ++it;
  }
  if (impl_->imports.size() >= 256)
    for (auto it = impl_->imports.begin(); it != impl_->imports.end();)
      if (it->second.expired())
        it = impl_->imports.erase(it);
      else
        ++it;
  if (peers.size() >= 65536)
    return {};
  auto p = std::shared_ptr<Peer>(new Peer, [](Peer* value) {
    urma_detail::cleanupOrDefer(std::unique_ptr<Peer>(value));
  });
  p->context = UrmaInit::acquireContext();
  if (!p->context)
    return {};
  urma_rjfr_t remote{};
  remote.jfr_id = id;
  remote.trans_mode = URMA_TM_RM;
  remote.tp_type = URMA_CTP;
  remote.flag.bs.order_type = URMA_OI;
  urma_token_t token{};
  p->target = urma_import_jfr(p->context, &remote, &token);
  if (!p->target)
    return {};
  peers[key] = {p, p->released};
  impl_->imports[key] = p;
  return p;
}
urma_target_jetty_t* UrmaIoContext::handle(const std::shared_ptr<Peer>& p) {
  return p ? p->target : nullptr;
}
std::shared_ptr<UrmaIoContext::ImportedSegment>
UrmaIoContext::importReadSegment(const urma_seg_t& description) {
  impl_->checkThread();
  // HELLO/SEGMENT publish full identities; PULL can only use an existing import.
  std::string key(reinterpret_cast<const char*>(description.ubva.eid.raw), 16);
  uint8_t fields[24];
  urma_detail::store32(fields, description.ubva.uasid);
  urma_detail::store64(fields + 4, description.ubva.va);
  urma_detail::store64(fields + 12, description.len);
  urma_detail::store32(fields + 20, description.token_id);
  key.append(reinterpret_cast<const char*>(fields), sizeof(fields));
  auto found = impl_->readImports.find(key);
  if (found != impl_->readImports.end()) {
    if (auto segment = found->second.segment.lock())
      return segment;
    if (!found->second.released->load(std::memory_order_acquire))
      return {};
  }
  if (impl_->readImports.size() >= 256) {
    for (auto it = impl_->readImports.begin();
         it != impl_->readImports.end();) {
      if (it->second.released->load(std::memory_order_acquire))
        it = impl_->readImports.erase(it);
      else
        ++it;
    }
  }
  if (impl_->readImports.size() >= 65536)
    return {};
  auto value = ImportedSegment::create(description);
  if (!value) return {};
  impl_->readImports[key] = {value, value->released};
  return value;
}
urma_target_seg_t* UrmaIoContext::handle(
    const std::shared_ptr<ImportedSegment>& segment) {
  return segment ? segment->segment : nullptr;
}

const urma_seg_t* UrmaIoContext::initialReadSegment() {
  impl_->checkThread();
  auto initial = UrmaInit::initialSegment();
  return initial ? &initial.segment()->seg : nullptr;
}
UrmaIoContext::ReadBuffer UrmaIoContext::takeReadBuffer(size_t size) {
  impl_->checkThread();
  return UrmaInit::allocateBuffer(size, impl_->poolSignal.get());
}
std::shared_ptr<void> UrmaIoContext::retainReadSource() {
  // Many PULLs can pin the same block. The physical pool cap alone cannot
  // bound their metadata, particularly after closing an unconfirmed peer.
  if (exposedReads.fetch_add(1) >= 65536) {
    exposedReads.fetch_sub(1);
    return {};
  }
  return std::shared_ptr<void>(&exposedReads, [](void*) { exposedReads.fetch_sub(1); });
}
std::shared_ptr<void> UrmaIoContext::retainRead() {
  impl_->checkThread();
  auto pool = impl_->readBudget;
  if (pool->pending.fetch_add(1) >= 1024) {
    pool->pending.fetch_sub(1);
    return {};
  }
  return std::shared_ptr<void>(pool.get(), [pool](void*) {
    pool->pending.fetch_sub(1);
  });
}
uint32_t UrmaIoContext::reserve(uint64_t id, bool freeNotification) {
  impl_->checkThread();
  if (impl_->broken || (!impl_->sockets.count(id) && !impl_->retired.count(id)))
    return kNoSlot;
  if (impl_->freeSlots.empty() ||
      (!freeNotification && !impl_->freeWaiters.empty()) ||
      (!impl_->waking &&
       (freeNotification ? !impl_->freeWaiters.empty()
                         : !impl_->waiters.empty()))) {
    impl_->wait(id, freeNotification);
    return kNoSlot;
  }
  auto slot = impl_->freeSlots.back();
  impl_->freeSlots.pop_back();
  impl_->hw->slots[slot].reserved = true;
  impl_->hw->slots[slot].priority = freeNotification;
  return slot;
}
uint8_t* UrmaIoContext::buffer(uint32_t slot, size_t bytes, bool control) {
  auto& hw = *impl_->hw;
  auto& entry = hw.slots.at(slot);
  if (!entry.reserved || entry.submitted || !bytes || bytes > impl_->config.buffer_size)
    throw std::logic_error("invalid URMA send buffer request");
  if (control) {
    if (!entry.controlBuffer || entry.controlBuffer->capacity() < bytes)
      entry.controlBuffer = UrmaInit::createPoolSlice(bytes, impl_->poolSignal.get());
    return entry.controlBuffer ? entry.controlBuffer->writableData() : nullptr;
  }
  if (!entry.buffer.memory)
    entry.buffer = UrmaInit::allocateBuffer(bytes, impl_->poolSignal.get());
  return static_cast<uint8_t*>(entry.buffer.memory);
}
void UrmaIoContext::waitForResources(uint64_t id) {
  impl_->checkThread();
  impl_->poolSignal->arm();
  impl_->wait(id);
  impl_->armTimer(1);
}
urma_target_seg_t* UrmaIoContext::segment(uint32_t slot) const {
  const auto& entry = impl_->hw->slots.at(slot);
  return entry.controlBuffer ? IoBufPool::segment(*entry.controlBuffer)
                             : entry.buffer.segment;
}
void UrmaIoContext::cancel(uint32_t slot) {
  impl_->checkThread();
  auto& s = impl_->hw->slots.at(slot);
  if (!s.reserved || s.submitted)
    throw std::logic_error("invalid URMA slot cancellation");
  auto source = std::move(s.completion.source);
  s = {};
  s.completion.source = std::move(source);
  impl_->freeSlots.push_back(slot);
}
urma_jfs_wr_t& UrmaIoContext::prepareSend(uint32_t index) {
  impl_->checkThread();
  auto& slot = impl_->hw->slots.at(index);
  if (!slot.reserved || slot.submitted)
    throw std::logic_error("invalid URMA send preparation");
  auto& ctx = slot.completion;
  ctx.work = {};
  std::fill(ctx.source.begin(), ctx.source.end(), urma_sge_t{});
  ctx.work.send.src.sge = ctx.source.data();
  return ctx.work;
}
urma_status_t UrmaIoContext::post(
    uint32_t index,
    uint64_t id,
    urma_jfs_wr_t& wr,
    std::shared_ptr<void> hold,
    bool schedulePoll) {
  impl_->checkThread();
  auto& slot = impl_->hw->slots.at(index);
  auto found = impl_->sockets.find(id);
  auto retired = impl_->retired.find(id);
  if (!slot.reserved || slot.submitted ||
      (found == impl_->sockets.end() && retired == impl_->retired.end()))
    throw std::logic_error("invalid URMA slot submission");
  auto* ctx = &slot.completion;
  // Every live field is replaced below; completion has already moved out hold.
  ctx->socket = id;
  ctx->user = wr.user_ctx;
  ctx->index = index;
  ctx->listener = found != impl_->sockets.end() ? found->second.listener
                                              : retired->second.get();
  ctx->read = wr.opcode == URMA_OPC_READ;
  ctx->readObserved = found != impl_->sockets.end() ? &found->second.read : nullptr;
  if (!ctx->read && wr.send.src.num_sge > ctx->source.size())
    throw std::logic_error("URMA SEND SGE count exceeds stable context");
  if (&wr != &ctx->work)
    ctx->work = wr;
  if (ctx->read) {
    ctx->source[0] = wr.rw.src.sge[0];
    ctx->destination = wr.rw.dst.sge[0];
    ctx->work.rw.src.sge = ctx->source.data();
    ctx->work.rw.dst.sge = &ctx->destination;
  } else if (wr.send.src.sge && wr.send.src.sge != ctx->source.data()) {
    std::copy_n(wr.send.src.sge, wr.send.src.num_sge, ctx->source.begin());
    ctx->work.send.src.sge = ctx->source.data();
  }
  ctx->hold = std::move(hold);
  ctx->work.user_ctx = reinterpret_cast<uintptr_t>(ctx);
  if (ctx->read) {
    ctx->work.flag.bs.complete_enable = 1;
    if (impl_->polling || !impl_->pendingReads.empty()) {
      impl_->pendingReads.push_back(index);
      if (!impl_->polling)
        impl_->armTimer(1);
      // Ownership has transferred to the owner, even before hardware accepts
      // this READ. It retries known-unaccepted WRs and cancels them on detach.
      return URMA_SUCCESS;
    }
  }
  slot.submitted = true;
  urma_jfs_wr_t* bad = nullptr;
  auto st = urma_post_jfs_wr(impl_->hw->tx, &ctx->work, &bad);
  if (st == URMA_EAGAIN && bad == &ctx->work) {
    slot.submitted = false;
    const bool priority = slot.priority;
    cancel(index);
    impl_->wait(id, priority);
    impl_->armTimer(1);
  } else if (st != URMA_SUCCESS) {
    // Acceptance is unknown. Retain all DMA references until owner teardown.
    impl_->fail("URMA shared submit failed or acceptance unknown");
  } else {
    if (wr.opcode == URMA_OPC_READ)
      ++impl_->reads;
    else
      ++impl_->sends;
    if (schedulePoll) impl_->armTimer(10);
  }
  return st;
}
void UrmaIoContext::poll() {
  impl_->poll();
}
void UrmaIoContext::scheduleRead(EventBase::LoopCallback* callback) {
  impl_->checkThread();
  if (impl_->handlingEvent)
    impl_->readyReads.push_back(*callback);
  else
    impl_->evb->runInLoop(callback, false, nullptr);
}
bool UrmaIoContext::hasDeferredReceives(uint64_t id) const {
  impl_->checkThread();
  auto it = impl_->sockets.find(id);
  return it != impl_->sockets.end() && it->second.deferred;
}
UrmaIoContext::Snapshot UrmaIoContext::snapshot() const {
  impl_->checkThread();
  size_t imports = 0;
  for (const auto& item : impl_->imports)
    imports += !item.second.expired();
  return {
      impl_->hw->tx ? impl_->hw->tx->jfs_id.id : 0,
      impl_->hw->rx ? impl_->hw->rx->jfr_id.id : 0,
      impl_->config.rx_depth,
      impl_->config.tx_depth,
      impl_->rxTarget,
      uint32_t(impl_->initialRx - impl_->pendingRx.size()),
      impl_->sockets.size(),
      imports,
      impl_->hw->slots.size() - impl_->freeSlots.size(),
      impl_->waiters.size() + impl_->freeWaiters.size(),
      impl_->sends,
      impl_->receives,
      impl_->reads,
      impl_->completions,
      impl_->readConnections,
      impl_->dataConnections,
      impl_->retired.size()};
}
} // namespace folly
#endif
