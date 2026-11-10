/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaIoContext.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/Indestructible.h>
#include <folly/ScopeGuard.h>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaResourceExecutor.h>
#include <folly/io/async/EventBaseLocal.h>
#include <folly/io/async/urma/UrmaInit.h>

#include <cstdio>
#include <algorithm>
namespace folly::urma_detail {
namespace {
constexpr uint64_t kRxSlot = uint64_t(1) << 63;
constexpr int kCompletionBatch = 32;
constexpr int kDrainBatches = 32;
constexpr auto kRnrRetryDelay = std::chrono::milliseconds(1);
constexpr auto kRnrTimeout = std::chrono::seconds(5);
std::atomic<size_t> liveConnections{0}, liveRoutes{0};
struct OwnerLifetime {
  std::vector<std::weak_ptr<UrmaIoContext>> values;
  ~OwnerLifetime() {
    for (const auto& weak : values)
      if (auto value = weak.lock()) value->stopOwner();
  }
};
EventBaseLocal<OwnerLifetime>& owners() {
  static Indestructible<EventBaseLocal<OwnerLifetime>> value;
  return *value;
}
void require(bool ok, const char* why) {
  if (!ok) throw std::runtime_error(why);
}
}  // namespace
struct UrmaIoContext::Handler final : EventHandler {
  UrmaIoContext* owner;
  ConnectionQueue* group;
  Handler(UrmaIoContext* o, int fd, ConnectionQueue* g)
      : EventHandler(o->evb_, NetworkSocket(fd)), owner(o), group(g) {
    require(registerHandler(READ | PERSIST),
            "URMA owner event registration failed");
  }
  void handlerReady(uint16_t) noexcept override {
    auto keep = owner->shared_from_this();
    if (owner->stopping_) {
      owner->stopOwner();
      return;
    }
    try {
      if (group)
        owner->poll(*group);
      else
        owner->signal_->consume();
      owner->wake();
    } catch (...) {
      owner->fail("URMA completion processing failed");
    }
  }
};
UrmaIoContext::UrmaIoContext(urma_context_t* ctx, const UrmaConfig& c)
    : AsyncTimeout(static_cast<EventBase*>(nullptr)),
      context_(ctx),
      config_(c),
      signal_(std::make_shared<UrmaPoolSignal>()),
      budget_(std::make_shared<RxBudget>(
          c.registered_pool.budgets[size_t(UrmaMemoryPurpose::Rx)])),
      readBudget_(std::make_shared<RxBudget>(
          c.registered_pool.budgets[size_t(UrmaMemoryPurpose::ReadTarget)])),
      pendingBudget_(std::make_shared<RxBudget>(c.max_retained_bytes)) {}
struct UrmaIoContext::RetiredConnection final : CleanupTask {
  std::shared_ptr<UrmaIoContext> value;
  bool tryCleanup() noexcept override {
    if (!value->prepared_.load(std::memory_order_acquire))
      return failed("wait-connection-preparation", 0);
    if (value->route_)
      return failed("wait-unresolved-connection-operations", 0);
    if (!value->cleanup(value->io_))
      return failed("delete-connection-queues", 0);
    if (value->counted_) {
      --liveConnections;
      value->counted_ = false;
    }
    if (value->ownsContext_) {
      value->ownsContext_ = false;
      value->context_ = nullptr;
      UrmaInit::releaseContext();
    }
    return true;
  }
};
UrmaIoContext::~UrmaIoContext() = default;
uint32_t UrmaIoContext::validateCapacity(UrmaConfig& c) {
  require(c.max_connections && c.max_pending_records &&
              c.max_pending_records <= 65536 &&
              c.max_retained_bytes >= c.max_pending_bytes,
          "invalid connection or pending record limit");
  require(!c.enable_read ||
              (c.io_free_sq_reserve && c.io_free_sq_reserve < c.tx_depth),
          "READ has no local completion SEND reserve");
  if (!c.enable_read) c.io_free_sq_reserve = 0;
  // Protect the minimum resources for one connection. Further connections
  // are admitted only when their independent RX buffers can be allocated.
  auto reserve = [&](size_t bytes, size_t count, UrmaMemoryPurpose purpose) {
    for (auto& tier : c.registered_pool.classes)
      if (tier.bytes >= bytes) {
        tier.reserve[size_t(purpose)] =
            std::max(tier.reserve[size_t(purpose)], count);
        return;
      }
    throw std::invalid_argument("connection storage has no pool class");
  };
  reserve(c.buffer_size, c.rx_depth + 1, UrmaMemoryPurpose::Rx);
  reserve(c.buffer_size, 1, UrmaMemoryPurpose::Tx);
  reserve(kPullBytes, c.offer_window, UrmaMemoryPurpose::Control);
  uint32_t readLimit = 0;
  if (c.enable_read) {
    const auto available = std::min<size_t>(
        c.max_pending_bytes,
        c.registered_pool.budgets[size_t(UrmaMemoryPurpose::ReadTarget)]);
    for (const auto& tier : c.registered_pool.classes)
      if (tier.bytes <= available) readLimit = tier.bytes;
    require(readLimit > c.read_threshold,
            "READ budget cannot reserve one target");
    reserve(readLimit, 1, UrmaMemoryPurpose::ReadTarget);
    reserve(24, c.tx_depth, UrmaMemoryPurpose::ReadControl);
  }
  MemoryPool::validate(c.registered_pool);
  return readLimit;
}
void UrmaIoContext::prepare(ConnectionQueue& g) {
  g.depth = config_.rx_depth;
  g.target = config_.rx_depth;
  g.sendDepth = config_.tx_depth;
  g.slots.resize(g.target);
  g.deferredOrder.resize(g.target);
  g.flights.resize(g.sendDepth);
  g.event = urma_create_jfce(context_);
  require(g.event, "create connection JFCE failed");
  urma_jfc_cfg_t cq{};
  cq.depth = g.depth + g.sendDepth + 1;
  cq.jfce = g.event;
  g.cq = urma_create_jfc(context_, &cq);
  require(g.cq, "create connection CQ failed");
  urma_jfs_cfg_t tx{};
  tx.depth = g.sendDepth;
  tx.max_sge = config_.max_send_sge;
  tx.max_rsge = 1;
  tx.jfc = g.cq;
  tx.trans_mode = URMA_TM_RM;
  tx.flag.bs.order_type = URMA_OI;
  // SEND uses ordered execution/remote completion below. The SDK forbids
  // those WR flags when outorder_comp is enabled.
  tx.flag.bs.outorder_comp = 0;
  tx.rnr_retry = URMA_TYPICAL_RNR_RETRY;
  tx.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
  g.tx = urma_create_jfs(context_, &tx);
  require(g.tx, "create connection JFS failed");
  urma_jfr_cfg_t rx{};
  rx.depth = g.depth;
  rx.max_sge = 1;
  rx.jfc = g.cq;
  rx.trans_mode = URMA_TM_RM;
  rx.flag.bs.order_type = URMA_OI;
  rx.min_rnr_timer = 12;
  g.rx = urma_create_jfr(context_, &rx);
  require(g.rx, "create connection JFR failed");
  urma_jetty_cfg_t jetty{};
  jetty.flag.bs.share_jfr = 1;
  jetty.shared.jfr = g.rx;
  jetty.shared.jfc = g.cq;
  jetty.jfs_cfg = tx;
  jetty.jfs_cfg.depth = 1;
  g.endpoint = urma_create_jetty(context_, &jetty);
  require(g.endpoint, "create connection receive endpoint failed");
  for (size_t i = 0; i < g.slots.size(); ++i)
    require(refill(g, i), "initial soft RX target could not be posted");
  // The owner arms and checks the CQ after installing its event handlers.
}
std::shared_ptr<UrmaIoContext> UrmaIoContext::get(EventBase* evb) {
  auto* ctx = UrmaInit::acquireContext();
  require(ctx, "URMA context unavailable");
  auto release = makeGuard([&] { UrmaInit::releaseContext(); });
  auto chosen = std::shared_ptr<UrmaIoContext>(
      new UrmaIoContext(ctx, UrmaInit::getConfig()));
  chosen->cleanupTask_ = std::make_unique<RetiredConnection>();
  chosen->ownsContext_ = true;
  release.dismiss();
  // The cleanup node exists before the first SDK resource is created.
  try {
    auto count = liveConnections.load();
    do {
      require(count < chosen->config_.max_connections,
              "URMA connection limit exceeded");
    } while (!liveConnections.compare_exchange_weak(count, count + 1));
    chosen->counted_ = true;
    chosen->evb_ = evb;
    chosen->attachEventBase(evb);
    chosen->detached_ = false;
    chosen->poolHandler_ = std::make_unique<Handler>(
        chosen.get(), chosen->signal_->fd(), nullptr);
    auto* lifetime = owners().get(*evb);
    if (!lifetime) lifetime = &owners().emplace(*evb);
    auto& entries = lifetime->values;
    entries.erase(std::remove_if(entries.begin(), entries.end(),
                                 [](const auto& w) { return w.expired(); }),
                  entries.end());
    entries.push_back(chosen);
    UrmaInit::subscribePool(chosen->signal_);
    const bool queued = submitPreparation([chosen] {
      try {
        if (!chosen->stopping_.load()) {
          chosen->prepare(chosen->io_);
          chosen->preparedOK_ = true;
        }
      } catch (...) {
        // The cleanup worker owns all partially acquired resources.
      }
      chosen->prepared_.store(true, std::memory_order_release);
      chosen->signal_->signal();
    });
    require(queued, "URMA connection preparation queue full");
  } catch (...) {
    chosen->prepared_.store(true, std::memory_order_release);
    chosen->stopOwner();
    throw;
  }
  return chosen;
}
bool UrmaIoContext::ready() {
  if (!prepared_.load(std::memory_order_acquire)) return false;
  require(preparedOK_ && !stopping_, "URMA connection preparation failed");
  if (!ioHandler_) {
    ioHandler_ = std::make_unique<Handler>(this, io_.event->fd, &io_);
    poll(io_);
  }
  return true;
}
void UrmaIoContext::add(const std::shared_ptr<UrmaRoute>& route) {
  require(!route_ && !stopping_, "connection already has a socket");
  route_ = route;
  route->ioOwner = this;
  ++liveRoutes;
}
void UrmaIoContext::remove(uint64_t id, const UrmaRoute* expected) {
  if (!route_ || route_->socketId != id || route_.get() != expected) return;
  route_->ioOwner = nullptr;
  route_.reset();
  --liveRoutes;
  stopOwner();
}
size_t UrmaIoContext::activeRoutes() { return liveRoutes.load(); }
urma_jetty_id_t UrmaIoContext::endpoint() const {
  return io_.endpoint->jetty_id;
}
bool UrmaIoContext::refill(ConnectionQueue& g, size_t index) {
  auto& slot = g.slots[index];
  if (slot.state == RxSlotState::Posted) return true;
  if (g.broken || stopping_) return false;
  if (!slot.storage) {
    auto block = UrmaInit::poolAlloc(
        config_.buffer_size, UrmaMemoryPurpose::Rx);
    if (!block) {
      ++g.replacementFailures;
      g.refillPending = true;
      return false;
    }
    auto lease = std::make_unique<RxStorageLease>();
    lease->block = std::move(block);
    lease->wakeup = signal_;
    slot.storage = std::move(lease);
    slot.state = RxSlotState::Ready;
  }
  if (slot.state == RxSlotState::Completed) slot.state = RxSlotState::Ready;
  require(slot.state == RxSlotState::Ready, "RX slot state cannot be reposted");
  auto& lease = *slot.storage;
  lease.dma =
      UrmaInit::lookupSegment(lease.block.data(), lease.block.capacity());
  require(bool(lease.dma), "RX replacement is not registered");
  slot.sge = {};
  slot.sge.addr = reinterpret_cast<uint64_t>(lease.block.data());
  slot.sge.len = config_.buffer_size;
  slot.sge.tseg = lease.block.segment();
  slot.wr = {};
  slot.wr.src.sge = &slot.sge;
  slot.wr.src.num_sge = 1;
  slot.wr.user_ctx = kRxSlot | index;
  slot.wr.next = nullptr;
  urma_jfr_wr_t* bad = nullptr;
  const auto result = urma_post_jfr_wr(g.rx, &slot.wr, &bad);
  if (result == URMA_SUCCESS) {
    slot.state = RxSlotState::Posted;
    ++g.posted;
    return true;
  }
  g.refillPending = true;
  if (bad != &slot.wr) {
    // An unknown acceptance is potentially posted. Keep its DMA pin and allow
    // a later CQE to settle it, but admit no new traffic on this group.
    slot.state = RxSlotState::Posted;
    ++g.posted;
    g.broken = true;
    if (prepared_.load(std::memory_order_acquire))
      fail("ambiguous RX post acceptance");
  } else
    lease.dma = {};  // definitely unaccepted, keep READY and its block
  retryLater();
  return false;
}
PostResult UrmaIoContext::post(std::unique_ptr<TxCompletionCtx>& work) {
  auto& g = io_;
  if (g.broken || stopping_) return PostResult::Unaccepted;  // no SDK call
  // RNR of our outgoing SEND must not block an incoming PULL's READ/FREE.
  // READ consumes no remote receive slot. Its CQE returns this local slot
  // before the corresponding FREE needs it.
  const bool progress = work->kind == Operation::Read || work->kind == Operation::Free;
  if (g.retrying && !progress) return PostResult::Full;
  const auto reserve = progress ? 0 : config_.io_free_sq_reserve;
  if (g.inflight >= g.sendDepth - reserve) return PostResult::Full;
  size_t i = 0;
  while (i < g.flights.size() && g.flights[i]) ++i;
  require(i < g.flights.size(), "connection SQ accounting mismatch");
  work->wr.next = nullptr;
  work->wr.user_ctx = i;
  work->wr.flag.bs.complete_enable = 1;
  work->wr.flag.bs.solicited_enable = 1;
  if (work->kind != Operation::Read) {
    // Preserve the original per-connection SEND ordering for DATA/PULL/FREE.
    // Software sequences still handle retries and whole-write callbacks.
    work->wr.flag.bs.place_order = URMA_STRONG_ORDER;
    work->wr.flag.bs.comp_order = 1;
  }
  auto* wr = &work->wr;
  g.flights[i] = std::move(work);
  ++g.inflight;
  urma_jfs_wr_t* bad = nullptr;
  const auto result = urma_post_jfs_wr(g.tx, wr, &bad);
  if (result == URMA_SUCCESS) return PostResult::Accepted;
  if (bad == wr) {
    work = std::move(g.flights[i]);
    --g.inflight;
    retryLater();
    return PostResult::Unaccepted;
  }
  g.broken = true;
  return PostResult::Unknown;  // ctx stays pinned, never retry this record
}
bool UrmaIoContext::canPost(Operation kind) const {
  if (!prepared_.load(std::memory_order_acquire) || !preparedOK_) return false;
  const auto& g = io_;
  const bool progress = kind == Operation::Read || kind == Operation::Free;
  const auto reserve = progress ? 0 : config_.io_free_sq_reserve;
  return !g.broken && !stopping_ &&
         (!g.retrying || progress) &&
         g.inflight < g.sendDepth - reserve;
}
void UrmaIoContext::receive(ConnectionQueue& g, const urma_cr_t& c) {
  const size_t index = c.user_ctx & ~kRxSlot;
  require(index < g.slots.size(), "RX CQE slot outside configured soft target");
  auto& slot = g.slots[index];
  require((slot.state == RxSlotState::Posted || slot.deferred) && slot.storage,
          "invalid RX completion or slot lifetime");
  const bool retrying = slot.deferred;
  if (retrying) --g.deferredCount;
  if (slot.state == RxSlotState::Posted) --g.posted;
  slot.deferred = false;
  slot.state = RxSlotState::Completed;
  slot.storage->dma = {};
  if (c.status != URMA_CR_SUCCESS || c.completion_len > slot.sge.len) {
    g.broken = true;
    fail("failed or oversized RX completion");
    return;
  }
  auto* p = static_cast<const uint8_t*>(slot.storage->block.data());
  RecordHeader h{};
  require(decodeHeader(p, c.completion_len, h), "invalid H24");
  const bool data = h.type == Message::Data;
  require(data ? (c.opcode == URMA_CR_OPC_SEND && c.completion_len > 24)
               : (c.opcode == URMA_CR_OPC_SEND_WITH_IMM &&
                  (h.type == Message::Pull
                       ? c.completion_len == 32 && getInteger(p + 24, 4) &&
                             !getInteger(p + 28, 4)
                       : c.completion_len == 24 && c.imm_data == 0)),
          "invalid DATA/PULL/FREE record");
  auto route = route_;
  require(route && route->socketId == h.destination,
          "record destination does not match connection");
  if (h.type == Message::Free) {
    route->onRecord(h, nullptr, 0);
  } else if (data) {
    auto connectionBudget = route->receiveBudget();
    const auto capacity = slot.storage->block.capacity();
    const auto held = connectionBudget->retained.load();
    if ((!retrying && g.deferredCount) || held > connectionBudget->limit ||
        capacity > connectionBudget->limit - held) {
      // Keep the completed slot intact until an upper-layer buffer is returned.
      // This is local bounded storage; no credit packet is exchanged.
      slot.completion = c;
      slot.deferred = true;
      ++g.deferredCount;
      g.refillPending = true;
      return;
    }
    auto lease = std::move(slot.storage);
    slot.state = RxSlotState::Empty;
    lease->connectionBudget = std::move(connectionBudget);
    lease->ownerBudget = budget_;
    lease->connectionBudget->retained.fetch_add(capacity);
    lease->ownerBudget->retained.fetch_add(capacity);
    lease->accounted = true;
    auto buffer = RxStorageLease::take(std::move(lease), 24,
                                        c.completion_len - 24);
    route->onRecord(h, std::move(buffer), 0);
  } else {
    auto description = IOBuf::copyBuffer(p + 24, 8);
    route->onRecord(h, std::move(description), c.imm_data);
  }
  wait(route);
  // A continuation poll also schedules delivery; no credit timer is needed.
  wake();
  if (slot.storage) slot.state = RxSlotState::Ready;
  (void)refill(g, index);
}
void UrmaIoContext::poll(ConnectionQueue& g) {
  if (polling_ || stopping_) return;
  polling_ = true;
  auto finish = makeGuard([&] {
    polling_ = false;
    // A callback can close the owner. Settle the entire removed batch first.
    if (stopping_) stopOwner();
  });
  try {
    // A notification is not a CQE. Acknowledge exactly the retrieved events;
    // completions remain in the CQ until poll removes them.
    if (!g.eventCount) {
      const int n =
          urma_wait_jfc(g.event, ConnectionQueue::kEventBatch, 0, g.events.data());
      require(n >= 0, "wait connection JFC failed");
      g.eventCount = n;
    }
    require(g.eventCount <= ConnectionQueue::kEventBatch, "invalid JFC event count");
    for (int i = 0; i < g.eventCount; ++i)
      require(g.events[i] == g.cq, "unexpected JFC event");
    if (g.eventCount) {
      std::array<uint32_t, ConnectionQueue::kEventBatch> counts;
      counts.fill(1);
      urma_ack_jfc(g.events.data(), counts.data(), g.eventCount);
      g.eventCount = 0;
    }

    // Two bounded poll/rearm passes: the second poll catches the first arm's
    // race window. Full batches continue in the loop; later notifications are
    // handled by the persistent event handler. Both entries use this path.
    for (int pass = 0; pass < 2; ++pass) {
      urma_cr_t completions[kCompletionBatch]{};
      const int n = urma_poll_jfc(g.cq, kCompletionBatch, completions);
      require(n >= 0 && n <= kCompletionBatch, "poll connection JFC failed");
      for (int i = 0; i < n; ++i) complete(g, completions[i]);
      if (stopping_) return;
      if (n == kCompletionBatch) evb_->runInLoop(this);
      require(urma_rearm_jfc(g.cq, false) == URMA_SUCCESS,
              "connection CQ rearm failed");
    }

    // Still inside the callback lifetime guard: a terminal retry may close the
    // socket. stopOwner() must not start queue deletion until this turn ends.
    retryRnr(g);

  } catch (...) {
    g.broken = true;
    throw;
  }
}
void UrmaIoContext::complete(ConnectionQueue& g, const urma_cr_t& c) noexcept {
  try {
    if (c.status == URMA_CR_WR_SUSPEND_DONE ||
        c.status == URMA_CR_WR_FLUSH_ERR_DONE) {
      // SDK synthetic notifications explicitly have no valid user_ctx.
      // They cannot settle whichever flight happens to occupy that index.
      g.broken = true;
      fail("URMA synthetic queue-state completion");
      return;
    }
    if (c.user_ctx & kRxSlot)
      receive(g, c);
    else {
      require(c.user_ctx < g.flights.size() && g.flights[c.user_ctx],
              "unknown TX completion");
      auto& flight = g.flights[c.user_ctx];
      require(!flight->retryPending, "completion for an unposted RNR retry");
      flight->remoteRejected = flight->kind != Operation::Read &&
          c.status == URMA_CR_RNR_RETRY_CNT_EXC_ERR;
      if (!g.broken && !stopping_ && flight->kind != Operation::Read &&
          c.status == URMA_CR_RNR_RETRY_CNT_EXC_ERR) {
        const auto now = std::chrono::steady_clock::now();
        if (flight->rnrSince == std::chrono::steady_clock::time_point{}) {
          flight->rnrSince = now;
          ++g.retrying;
        }
        flight->retryPending = true;
        flight->retryAt = now + kRnrRetryDelay;
        // No completion callback or new sequence: the peer did not receive
        // this SEND. ACK timeout and other ambiguous errors never come here.
        return;
      }
      auto work = std::move(g.flights[c.user_ctx]);
      --g.inflight;
      if (work->rnrSince != std::chrono::steady_clock::time_point{}) --g.retrying;
      const bool ok = c.status == URMA_CR_SUCCESS;
      if (!ok)
        std::fprintf(stderr,
                     "URMA CQ failure: status=%u kind=%u queue=%s "
                     "local=%u tpn=%u sequence=%llu\n",
                     unsigned(c.status), unsigned(work->kind),
                     "IO", c.local_id, c.tpn,
                     static_cast<unsigned long long>(work->sequence));
      work->route->onTx(*work, ok);
      if (!ok) {
        g.broken = true;
        fail("URMA work completion failed");
      }
    }
  } catch (const std::exception& e) {
    // CQEs already removed from the device must all be settled, even if
    // one record is malformed or its software bookkeeping allocation
    // fails.
    g.broken = true;
    fail(e.what());
  } catch (...) {
    g.broken = true;
    fail("URMA completion processing failed with unknown exception");
  }
}
void UrmaIoContext::retryRnr(ConnectionQueue& g) {
  bool waiting = false;
  bool posted = false;
  size_t selected = g.flights.size();
  const auto now = std::chrono::steady_clock::now();
  for (size_t i = 0; i < g.flights.size(); ++i) {
    auto& flight = g.flights[i];
    if (!flight || flight->rnrSince == std::chrono::steady_clock::time_point{})
      continue;
    if (!flight->retryPending) {
      posted = true;
      continue;
    }
    if (g.broken || stopping_ || now - flight->rnrSince >= kRnrTimeout) {
      // This WR has a definite RNR completion, not an in-flight DMA. Other
      // accepted WRs remain pinned until their own completions are observed.
      g.broken = true;
      std::fprintf(stderr,
                   "URMA RNR retry ended: kind=%u sequence=%llu elapsed_ms=%lld\n",
                   unsigned(flight->kind),
                   static_cast<unsigned long long>(flight->sequence),
                   static_cast<long long>(std::chrono::duration_cast<
                       std::chrono::milliseconds>(now - flight->rnrSince).count()));
      finishRejected(g, i);
      fail("URMA receive-not-ready retry ended");
      continue;
    }
    waiting = true;
    if (now < flight->retryAt) continue;
    // Recover one rejected SEND at a time. Replaying an entire rejected burst
    // immediately recreates the receiver shortage. FREE has priority; DATA
    // and PULL retry in stream order, independent of recycled flight indices.
    if (selected == g.flights.size() ||
        (flight->kind == Operation::Free &&
         g.flights[selected]->kind != Operation::Free) ||
        ((flight->kind == Operation::Free) ==
             (g.flights[selected]->kind == Operation::Free) &&
         flight->sequence < g.flights[selected]->sequence))
      selected = i;
  }
  if (!g.broken && !stopping_ && !posted && selected != g.flights.size()) {
    auto& flight = g.flights[selected];
    urma_jfs_wr_t* bad = nullptr;
    auto* wr = &flight->wr;
    const auto rc = urma_post_jfs_wr(g.tx, wr, &bad);
    if (rc == URMA_SUCCESS) {
      flight->retryPending = false;
      flight->remoteRejected = false;
    } else if (bad == wr) {
      flight->retryAt = now + kRnrRetryDelay;
    } else {
      flight->retryPending = false;  // possibly posted: never submit it again
      flight->remoteRejected = false;
      g.broken = true;
      fail("URMA RNR retry acceptance unknown");
    }
  }
  if (waiting && !stopping_ && !scheduleTimeout(1))
    throw std::runtime_error("URMA RNR retry timer failed");
}
void UrmaIoContext::finishRejected(ConnectionQueue& g, size_t index) noexcept {
  auto work = std::move(g.flights[index]);
  --g.inflight;
  --g.retrying;
  try {
    work->route->onTx(*work, false);
  } catch (...) {
    fail("URMA rejected SEND completion callback failed");
  }
}
void UrmaIoContext::wait(const std::shared_ptr<UrmaRoute>& route) {
  if (route_ == route && !stopping_) pendingProgress_ = true;
}
void UrmaIoContext::wake() noexcept {
  if (!evb_ || detached_ || stopping_) return;
  readyProgress_ |= pendingProgress_;
  evb_->runInLoop(this);
}
void UrmaIoContext::retryLater() noexcept {
  if (!prepared_.load(std::memory_order_acquire)) return;
  if (evb_ && !stopping_ && !isScheduled() && !scheduleTimeout(5)) {
    io_.broken = true;
    wake();
  }
}
void UrmaIoContext::runLoopCallback() noexcept {
  auto keep = shared_from_this();
  if (stopping_) { stopOwner(); return; }
  try {
    if (prepared_.load(std::memory_order_acquire) && preparedOK_) {
      poll(io_);
      if (stopping_) return;
      if (io_.broken && !failed_) fail("URMA queue acceptance unknown");
      if (io_.refillPending) {
        io_.refillPending = false;
        bool retry = false;
        size_t count = 0;
        for (size_t i = 0; i < io_.slots.size(); ++i) {
          if (io_.slots[i].deferred) {
            io_.deferredOrder[count++] = i;
          } else if (io_.slots[i].state != RxSlotState::Posted) {
            retry |= !refill(io_, i);
          }
        }
        // Slot indices wrap independently of stream order. Admit the oldest
        // deferred record first so later data cannot consume its freed budget.
        auto sequence = [&](size_t i) {
          const auto* p = static_cast<const uint8_t*>(io_.slots[i].storage->block.data());
          return getInteger(p + 16, 8);
        };
        std::sort(io_.deferredOrder.begin(), io_.deferredOrder.begin() + count,
                  [&](size_t a, size_t b) { return sequence(a) < sequence(b); });
        for (size_t n = 0; n < count && !stopping_; ++n) {
          auto& slot = io_.slots[io_.deferredOrder[n]];
          const auto completion = slot.completion;
          receive(io_, completion);
          if (slot.deferred) break;
        }
        io_.refillPending |= io_.deferredCount != 0;
        if (retry && !io_.broken) retryLater();
      }
    }
    if (readyProgress_ && route_) {
      readyProgress_ = pendingProgress_ = false;
      auto route = route_;
      route->progress();
    }
  } catch (...) { fail("URMA connection progress failed"); }
}
void UrmaIoContext::fail(const char* message) noexcept {
  failed_ = true;
  if (auto route = route_) {
    if (stopping_) route->ownerStopped();
    else route->failed(message);
  }
}
void UrmaIoContext::stopOwner() noexcept {
  stopping_ = true;
  if (polling_) return;
  // Definitively rejected SENDs have no device access outstanding. Settle
  // them on the owner thread, under the same reentrancy guard as CQ callbacks.
  // Accepted/ambiguous WRs retain their pins; shutdown cannot invent a CQE.
  polling_ = true;
  if (prepared_.load(std::memory_order_acquire))
    for (size_t i = 0; i < io_.flights.size(); ++i)
      if (io_.flights[i] && io_.flights[i]->retryPending)
        finishRejected(io_, i);
  polling_ = false;
  if (!detached_) {
    cancelLoopCallback();
    cancelTimeout();
    if (ioHandler_) ioHandler_->unregisterHandler();
    if (poolHandler_) poolHandler_->unregisterHandler();
    signal_->stop();
    detachEventBase();
    detached_ = true;
    fail("URMA EventBase stopped");
  }
  if (cleanupTask_) {
    cleanupTask_->value = shared_from_this();
    cleanupOrDefer(std::move(cleanupTask_));
  }
}
bool UrmaIoContext::cleanup(ConnectionQueue& g) noexcept {
  if (g.tx) {
    if (urma_delete_jfs(g.tx) != URMA_SUCCESS) return false;
    g.tx = nullptr;
  }
  if (g.endpoint) {
    if (urma_delete_jetty(g.endpoint) != URMA_SUCCESS) return false;
    g.endpoint = nullptr;
  }
  if (g.rx) {
    if (urma_delete_jfr(g.rx) != URMA_SUCCESS) return false;
    g.rx = nullptr;
  }
  g.slots.clear();
  g.flights.clear();
  if (g.cq) {
    // Producers are stopped. Drain wait -> poll -> ACK without rearming.
    // Failed attempts retain fetched notifications and both handles for retry.
    for (int pass = 0; pass < kDrainBatches; ++pass) {
      if (!g.eventCount) {
        const int n =
            urma_wait_jfc(g.event, ConnectionQueue::kEventBatch, 0, g.events.data());
        if (n < 0) return false;
        g.eventCount = n;
      }
      if (g.eventCount > ConnectionQueue::kEventBatch) return false;
      for (int i = 0; i < g.eventCount; ++i)
        if (g.events[i] != g.cq) return false;
      const int events = g.eventCount;
      urma_cr_t completions[kCompletionBatch];
      const int n = urma_poll_jfc(g.cq, kCompletionBatch, completions);
      if (n < 0 || n > kCompletionBatch) return false;
      if (g.eventCount) {
        std::array<uint32_t, ConnectionQueue::kEventBatch> counts;
        counts.fill(1);
        urma_ack_jfc(g.events.data(), counts.data(), g.eventCount);
        g.eventCount = 0;
      }
      if (!events && !n) break;
      if (pass + 1 == kDrainBatches) return false;
    }
    if (urma_delete_jfc(g.cq) != URMA_SUCCESS) return false;
    g.cq = nullptr;
  }
  if (g.event) {
    if (urma_delete_jfce(g.event) != URMA_SUCCESS) return false;
    g.event = nullptr;
  }
  return true;
}
}  // namespace folly::urma_detail
#endif
