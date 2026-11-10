/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaIoContext.h>
#include <folly/io/async/urma/UrmaMemoryPool.h>
#include <folly/memory/IoBufPool.h>
#include <folly/ScopeGuard.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <fstream>
#include <poll.h>
#include <unistd.h>
#include <thread>
#include <vector>
using namespace folly;
using namespace std::chrono_literals;
namespace {
std::atomic<size_t> registered{0}, calls{0};
std::atomic<bool> failRegistration{false}, failUnregister{false};
std::atomic<bool> pauseRegistration{false};
// Track notification control blocks, including expired weak references.
template <class T>
struct SignalAllocator {
  using value_type = T;
  std::atomic<size_t>* outstanding;
  explicit SignalAllocator(std::atomic<size_t>& n) : outstanding(&n) {}
  template <class U>
  SignalAllocator(const SignalAllocator<U>& other) : outstanding(other.outstanding) {}
  T* allocate(size_t n) {
    auto* result = std::allocator<T>{}.allocate(n);
    outstanding->fetch_add(1);
    return result;
  }
  void deallocate(T* p, size_t n) noexcept {
    std::allocator<T>{}.deallocate(p, n);
    outstanding->fetch_sub(1);
  }
};
void check(bool b, const char* why) {
  if (!b) {
    std::cerr << "FAIL " << why << '\n';
    std::_Exit(1);
  }
}
UrmaPoolConfig config() {
  UrmaPoolConfig c;
  c.enabled = true;
  c.elastic = false;  // Fixed mode remains supported even when hard > soft.
  c.pool_size = 65536;
  c.pool_max_size = 131072;
  c.region_bytes = 65536;
  c.tls_block_size = 16384;
  c.budgets.fill(65536);
  c.classes = {{4096, {{0, 0, 1, 0, 0}}}, {12288, {}}, {16384, {}}};
  c.retry_ms = 2;
  c.idle_ms = 10;
  return c;
}
void largeBodiesStayRegistered() {
  constexpr size_t kBodyBytes = 8 * 1024 * 1024;
  constexpr size_t kSerializationBytes = 64;
  constexpr size_t kRequests = 10;
  constexpr auto kSource = size_t(UrmaMemoryPurpose::ReadSource);
  UrmaPoolConfig c;
  check(c.enabled && c.zero_copy && c.elastic && c.tls_cache_blocks &&
            c.pool_max_size > c.pool_size,
        "default policy must enable registered TLS and elastic capacity");
  auto p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
  check(p->warm(0ms), "default pool maintenance did not start");
  const auto registrations = calls.load();
  std::vector<std::unique_ptr<IOBuf>> bodies;
  for (size_t i = 0; i < kRequests; ++i) {
    auto b = IoBufPool::createFromPoolShared(p, kBodyBytes + kSerializationBytes);
    check(bool(b), "default source budget cannot retain ten large RPC bodies");
    std::memset(b->writableData(), int(i + 1), b->capacity());
    b->append(kBodyBytes + kSerializationBytes);
    check(IoBufPool::owns(*b) && bool(p->lookup(b->data(), b->length(), true)),
          "large body lost its exportable registered range");
    bodies.push_back(std::move(b));
  }
  auto clone = bodies.front()->cloneOne();
  auto dma = p->lookup(clone->data(), clone->length(), false);
  auto exported = p->lookup(clone->data(), clone->length(), true);
  check(dma && exported, "large source lifetime pins missing");
  // Fill the same bounded purpose; improving packing must not bypass its cap.
  while (auto b =
             IoBufPool::createFromPoolShared(p, kBodyBytes + kSerializationBytes))
    bodies.push_back(std::move(b));
  auto full = p->stats();
  check(full.held_by_purpose[kSource] <= c.budgets[kSource] &&
            full.registered_bytes == c.pool_size && calls == registrations,
        "large-body allocation exceeded its budget or registered per request");
  auto control = p->allocate(24, UrmaMemoryPurpose::ReadControl);
  check(bool(control), "large sources consumed READ completion storage");
  control = {};
  bodies.clear();
  p->trim();
  check(p->stats().held_by_purpose[kSource] != 0 &&
            clone->data()[0] == 1 && clone->data()[clone->length() - 1] == 1,
        "large clone/source contents or accounting released early");
  clone.reset();
  p->trim();
  const auto held = p->stats().held_by_purpose[kSource];
  check(held != 0 && !p->shutdown(0ms), "DMA/export-pinned large block released");
  dma = {};
  check(p->stats().held_by_purpose[kSource] == held,
        "large exported source lost its physical charge");
  exported = {};
  check(p->shutdown(1000ms) && registered == 0,
        "large-body final return failed to reclaim registration");
  std::cout << "PASS ten concurrent 8 MiB bodies, purpose cap, control reserve, "
               "clone/DMA/export lifetime and single registration\n";
}
void resourceNotifications() {
  using namespace urma_detail;
  auto c = config();
  c.budgets[size_t(UrmaMemoryPurpose::Tx)] = 4096;
  auto p = std::make_shared<MemoryPool>(nullptr, c);
  auto signal = std::make_shared<UrmaPoolSignal>();
  p->subscribe(signal);
  pollfd event{signal->fd(), POLLIN, 0};
  auto block = p->allocate(4096, UrmaMemoryPurpose::Tx, signal.get());
  check(bool(block), "notification allocation setup");
  block = {};
  check(::poll(&event, 1, 0) == 0, "normal block return woke an idle owner");
  block = p->allocate(4096, UrmaMemoryPurpose::Tx);
  check(!p->allocate(4096, UrmaMemoryPurpose::Tx, signal.get()),
        "notification budget shortage setup");
  std::thread returner([b = std::move(block)]() mutable { b = {}; });
  returner.join();
  check(::poll(&event, 1, 0) == 1, "failed allocation missed cross-thread return");
  signal->consume();
  block = p->allocate(4096, UrmaMemoryPurpose::Tx, signal.get());
  block = {};
  check(::poll(&event, 1, 0) == 0, "satisfied allocation kept waking owner");

  auto budget = std::make_shared<RxBudget>(4096);
  auto lease = std::make_unique<RxStorageLease>();
  lease->block = p->allocate(4096, UrmaMemoryPurpose::Rx);
  lease->connectionBudget = budget;
  lease->ownerBudget = std::make_shared<RxBudget>(4096);
  lease->connectionBudget->retained = lease->ownerBudget->retained = 4096;
  lease->accounted = true;
  lease->wakeup = signal;
  auto data = RxStorageLease::take(std::move(lease), 24, 1024);
  auto clone = data->cloneOne();
  auto retainedDma = p->lookup(data->data(), data->length(), false);
  check(bool(retainedDma), "retained-budget DMA hold setup");
  check(!budget->available(4096, *signal), "retained budget shortage setup");
  data.reset();
  check(::poll(&event, 1, 0) == 0, "live receive clone returned budget early");
  std::thread release([b = std::move(clone)]() mutable { b.reset(); });
  release.join();
  check(::poll(&event, 1, 0) == 1 && budget->available(4096, *signal),
        "last receive clone did not wake retained-budget waiter");
  signal->consume();
  // Budget can return before the backing block: its DMA pin must neither
  // suppress that notification nor produce another one for an idle owner.
  retainedDma = {};
  lease = std::make_unique<RxStorageLease>();
  lease->block = p->allocate(4096, UrmaMemoryPurpose::Rx);
  lease->wakeup = signal;
  lease.reset();
  check(::poll(&event, 1, 0) == 0, "idle receive returns still woke owner");

  // Race the first availability check, arming and recheck against a return.
  // Either the caller can proceed immediately or an event must be pending.
  std::atomic<unsigned> requested{0}, finished{0};
  std::thread concurrent([&] {
    for (unsigned i = 1; i <= 1000; ++i) {
      while (requested.load() < i) std::this_thread::yield();
      budget->retained.fetch_sub(4096);
      signal->notify();
      finished.store(i);
    }
  });
  for (unsigned i = 1; i <= 1000; ++i) {
    budget->retained = 4096;
    requested.store(i);
    const bool available = budget->available(4096, *signal);
    while (finished.load() < i) std::this_thread::yield();
    check(available || ::poll(&event, 1, 0) == 1,
          "concurrent retained-budget return lost its wakeup");
    signal->consume();
  }
  concurrent.join();
  signal->signal();
  check(::poll(&event, 1, 0) == 1, "control notification incorrectly gated");
  signal->consume();
  check(p->shutdown(0ms) && registered == 0, "notification cleanup");
  std::cout << "PASS quiet resource returns, allocation/clone waiters and "
               "1000 concurrent budget-return races\n";
}
}  // namespace
extern "C" urma_target_seg_t* urma_register_seg(urma_context_t*,
                                                urma_seg_cfg_t* c) {
  ++calls;
  while (pauseRegistration.load()) std::this_thread::sleep_for(1ms);
  if (failRegistration) return nullptr;
  auto* s = new urma_target_seg_t{};
  s->seg.ubva.va = c->va;
  s->seg.len = c->len;
  registered += c->len;
  return s;
}
extern "C" urma_status_t urma_unregister_seg(urma_target_seg_t* s) {
  if (failUnregister) return static_cast<urma_status_t>(-1);
  registered -= s->seg.len;
  delete s;
  return URMA_SUCCESS;
}
int main(int argc, char** argv) {
  try {
    largeBodiesStayRegistered();
    resourceNotifications();
    check(argc == 2, "shipped config fixture argument missing");
    UrmaConfig base;
    base.dev_name = "device-supplied-by-caller";
    auto example = loadUrmaConfigFile(argv[1], base);
    check(example.dev_name == base.dev_name &&
              example.registered_pool.enabled &&
              example.registered_pool.elastic &&
              example.registered_pool.zero_copy &&
              example.registered_pool == base.registered_pool &&
              example.registered_pool.pool_max_size >
                  example.registered_pool.pool_size &&
              urma_detail::UrmaIoContext::validateCapacity(example) >
                  example.read_threshold,
          "shipped config overlay / startup reserves");
    char path[] = "/tmp/urma-config-test-XXXXXX";
    int fd = ::mkstemp(path);
    check(fd >= 0, "temporary config creation failed");
    ::close(fd);
    auto removeFile = makeGuard([&] { ::unlink(path); });
    for (const char* key : {"io_owners", "max_water", "fc_rx_depth",
                            "fc_max_water", "rx_spare_blocks",
                            "io_free_rx_reserve", "credit_target",
                            "credit_refill_low", "credit_grant_cap",
                            "idle_timeout_ms", "owner_pending_bytes"}) {
      std::ofstream out(path);
      out << "{\"version\":3,\"" << key << "\":1}";
      out.close();
      bool rejected = false;
      try { (void)loadUrmaConfigFile(path, base); }
      catch (const std::invalid_argument&) { rejected = true; }
      check(rejected, "removed shared/credit configuration was accepted");
    }
    using namespace urma_detail;
    Hello hello;
    hello.bufferSize = 4096;
    hello.receiveCount = 24;
    hello.ioJetty = 7;
    hello.socketId = 0x123456789abcdefULL;
    hello.read = true;
    hello.maxRead = 65536;
    hello.segment.ubva.va = 0x123456000;
    hello.segment.len = 65536;
    auto wire = encodeHello(hello);
    Hello decoded;
    check(wire.size() == 98 &&
              decodeHello(wire.data(), wire.size(), decoded) &&
              decoded.ioJetty == hello.ioJetty &&
              decoded.maxRead == hello.maxRead &&
              decoded.socketId == hello.socketId &&
              decoded.segment.ubva.va == hello.segment.ubva.va,
          "HELLO98 byte order / independent endpoint / initial segment");
    putInteger(wire.data() + 6, 3, 2);
    check(!decodeHello(wire.data(), wire.size(), decoded),
          "shared-queue protocol version accepted");
    wire = encodeHello(hello);
    wire[44] = 1;
    check(!decodeHello(wire.data(), wire.size(), decoded),
          "HELLO accepted reserved bits");
    uint8_t header[24];
    RecordHeader h{};
    encodeHeader(header, {Message::Data, 9, 3});
    check(decodeHeader(header, 24, h) && h.destination == 9 && h.sequence == 3,
          "H24 round trip");
    header[4] = 1;
    check(!decodeHeader(header, 24, h), "old H24 version accepted");
    auto c = config();
    auto bad = c;
    bad.pool_size = 4096;
    bad.classes[0].reserve[0] = 2;
    bool rejected = false;
    try {
      urma_detail::MemoryPool::validate(bad);
    } catch (...) {
      rejected = true;
    }
    check(rejected, "startup borrowed the hard limit for soft reserves");
    failRegistration = true;
    rejected = false;
    try {
      auto p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
    } catch (...) {
      rejected = true;
    }
    failRegistration = false;
    check(rejected && registered == 0,
          "initial registration failure not atomic");
    auto p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
    check(p->warm(0ms) && registered == c.pool_size, "fixed soft registration");
    std::atomic<size_t> notifications{0};
    for (unsigned i = 0; i < 1000; ++i) {
      auto signal = std::allocate_shared<UrmaPoolSignal>(
          SignalAllocator<UrmaPoolSignal>(notifications));
      p->subscribe(signal);
      check(notifications == 1, "expired owner notifications accumulated");
    }
    // Prune the final expired reference before its test allocator goes away.
    p->subscribe(std::make_shared<UrmaPoolSignal>());
    check(notifications == 0, "expired notification control block retained");
    std::cout << "PASS notification storage remains bounded across 1000 owner lifetimes\n";
    auto tx = p->allocate(12000, UrmaMemoryPurpose::Tx);
    auto rx = p->allocate(4096, UrmaMemoryPurpose::Rx);
    check(tx && rx && tx.capacity() == 12288 && tx.segment() == rx.segment(),
          "shared region / exact class");
    check(!p->lookup(static_cast<char*>(tx.data()) + 12287, 2, false),
          "cross block lookup");
    check(!p->lookup(tx.data(), size_t(-1), false), "overflow lookup");
    auto dma = p->lookup(tx.data(), 12000, false);
    auto exported = p->lookup(tx.data(), 12000, true);
    auto wakeup = std::make_shared<UrmaPoolSignal>();
    p->subscribe(wakeup);
    wakeup->arm();
    pollfd wakeEvent{wakeup->fd(), POLLIN, 0};
    tx = {};
    check(p->stats().held_by_purpose[size_t(UrmaMemoryPurpose::Tx)] == 12288,
          "DMA/export hold lost budget");
    dma = {};
    check(::poll(&wakeEvent, 1, 0) == 0,
          "partial pin release woke owners without returning physical storage");
    exported = {};
    check(::poll(&wakeEvent, 1, 0) == 1,
          "last physical block return did not wake allocation waiters");
    wakeup->consume();
    rx = {};
    const auto before = calls.load();
    std::vector<UrmaPoolBuffer> held;
    while (auto b = p->allocate(4096, UrmaMemoryPurpose::Tx)) held.push_back(b);
    check(bool(p->allocate(4096, UrmaMemoryPurpose::Control)),
          "data stole control reserve");
    check(calls == before && p->stats().registering_bytes == 0 &&
              registered == c.pool_size,
          "fixed pool grew because hard > soft");
    held.clear();
    std::cout << "PASS fixed soft/hard, class reserves, mixed purposes, 12 "
                 "KiB, DMA/export lifetime\n";
    auto a = IoBufPool::createFromPoolShared(p, 100);
    auto b = IoBufPool::createFromPoolShared(p, 100);
    check(a && b && a->buffer() == a->data() && a->length() == 0 &&
              a->capacity() == 100,
          "slice create contract");
    check(b->data() == a->data() + 100, "small slices do not share a block");
    std::memset(a->writableData(), 0x51, 100);
    a->append(100);
    auto clone = a->cloneOne();
    a.reset();
    b.reset();
    p->trim();
    check(clone->data()[99] == 0x51, "TLS trim overwrote a live clone");
    std::thread release(
        [value = std::move(clone)]() mutable { value.reset(); });
    release.join();
    p->trim();
    check(p->stats().allocated_bytes == 0 && p->stats().tls_retained_bytes == 0,
          "cross-thread slice release leaked physical accounting");
    auto pinned = IoBufPool::createFromPoolShared(p, 15000);
    check(bool(pinned), "DMA recycle setup");
    auto* pinnedAddress = pinned->writableData();
    std::memset(pinnedAddress, 0x7c, 15000);
    dma = p->lookup(pinnedAddress, 15000, false);
    exported = p->lookup(pinnedAddress, 15000, true);
    pinned.reset();
    auto different = IoBufPool::createFromPoolShared(p, 15000);
    check(different && different->data() != pinnedAddress,
          "TLS recycled a DMA/export pinned slice");
    std::memset(different->writableData(), 0x11, 15000);
    check(pinnedAddress[14999] == 0x7c,
          "DMA source was overwritten after IOBuf release");
    different.reset();
    dma = {};
    exported = {};
    p->trim();
    auto original = IoBufPool::createFromPoolShared(p, 100);
    std::memset(original->writableData(), 0x42, 100);
    original->append(100);
    auto separated = original->cloneOne();
    separated->unshare();
    separated->writableData()[0] = 0x21;
    check(original->data()[0] == 0x42 && separated->data()[99] == 0x42,
          "IOBuf unshare changed the original slice");
    original.reset();
    separated.reset();
    p->trim();
    std::unique_ptr<IOBuf> late;
    std::thread producer(
        [&] { late = IoBufPool::createFromPoolShared(p, 100); });
    producer.join();
    check(late && !p->shutdown(0ms),
          "thread exit / shutdown released a live slice");
    late.reset();
    failUnregister = true;
    check(!p->shutdown(0ms) && registered == c.pool_size,
          "failed unregister lost R");
    failUnregister = false;
    std::this_thread::sleep_for(3ms);
    check(p->shutdown(0ms) && registered == 0,
          "late return / cleanup did not finish");
    std::cout << "PASS TLS slices, clone, cross-thread return, thread exit, "
                 "shutdown and unregister retry\n";
    c.elastic = true;
    p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
    check(p->warm(0ms), "elastic warm");
    auto initial = p->initialSegment();
    // Independent purpose budgets may share physical capacity, never regions.
    for (int i = 0; i < 3; ++i)
      held.push_back(p->allocate(16384, UrmaMemoryPurpose::Tx));
    auto extra = p->allocate(16384, UrmaMemoryPurpose::ReadTarget);
    auto end = std::chrono::steady_clock::now() + 2s;
    while (!extra && std::chrono::steady_clock::now() < end) {
      std::this_thread::sleep_for(2ms);
      extra = p->allocate(16384, UrmaMemoryPurpose::ReadTarget);
    }
    check(bool(extra), "elastic growth did not publish");
    check(extra.segment() != initial.segment() &&
              !p->lookup(extra.data(), 1, true),
          "new region was exported without HELLO publication");
    check(p->stats().registered_bytes + p->stats().registering_bytes <=
              c.pool_max_size,
          "R+E exceeded hard");
    auto retiringDma = p->lookup(extra.data(), extra.capacity(), false);
    const auto retiringId = extra.regionId();
    check(bool(retiringDma), "retirement DMA hold setup");
    p->retire(extra);
    check(!p->lookup(extra.data(), 1, false),
          "retiring region accepted a new DMA hold");
    extra = {};
    held.clear();
    std::this_thread::sleep_for(std::chrono::milliseconds(c.idle_ms * 3));
    bool stillPinned = false;
    for (const auto& region : p->regions())
      if (region.id == retiringId)
        stillPinned = region.retiring && region.allocated_blocks == 1 &&
                      region.dma_refs == 1;
    check(stillPinned && p->stats().registered_bytes == c.pool_max_size &&
              p->stats().retiring_bytes == c.region_bytes &&
              p->stats().registering_bytes == 0,
          "retirement released DMA-held storage or deducted R early");
    auto retiredWake = std::make_shared<UrmaPoolSignal>();
    p->subscribe(retiredWake);
    retiredWake->arm();
    retiringDma = {};
    pollfd retiredEvent{retiredWake->fd(), POLLIN, 0};
    check(::poll(&retiredEvent, 1, 0) == 1,
          "retired block returned purpose budget without waking waiters");
    end = std::chrono::steady_clock::now() + 2s;
    while (p->stats().registered_bytes != c.pool_size &&
           std::chrono::steady_clock::now() < end)
      std::this_thread::sleep_for(2ms);
    check(p->stats().registered_bytes == c.pool_size,
          "idle shrink below/above soft");
    check(!p->shutdown(0ms), "HELLO connection hold ignored");
    initial = {};
    check(p->shutdown(2s) && registered == 0, "elastic cleanup");
    std::cout << "PASS optional growth/shrink, DMA-held retirement, R+E, "
                 "HELLO segment hold and new-region READ exclusion\n";
    // Reserve is physically free but unavailable to TX: it must not suppress
    // growth. This uses a tier exactly matching the protected block.
    c.pool_size = 32768;
    c.pool_max_size = 65536;
    c.region_bytes = 32768;
    c.budgets.fill(65536);
    c.classes = {{4096, {}}, {16384, {{0, 0, 1, 0, 0, 0}}}};
    p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
    p->warm(0ms);
    tx = p->allocate(16384, UrmaMemoryPurpose::Tx);
    check(bool(tx), "protected growth setup");
    pauseRegistration = true;
    extra = p->allocate(16384, UrmaMemoryPurpose::Tx);
    end = std::chrono::steady_clock::now() + 2s;
    while (!p->stats().registering_bytes &&
           std::chrono::steady_clock::now() < end)
      std::this_thread::sleep_for(1ms);
    check(p->stats().registered_bytes == 32768 &&
              p->stats().registering_bytes == 32768,
          "in-flight registration did not reserve E before SDK call");
    std::vector<std::thread> requesters;
    for (unsigned i = 0; i < 8; ++i)
      requesters.emplace_back([&] {
        for (int j = 0; j < 20; ++j) {
          check(!p->allocate(16384, UrmaMemoryPurpose::ReadSource),
                "unpublished region escaped");
          auto s = p->stats();
          check(s.registered_bytes + s.registering_bytes <= c.pool_max_size,
                "concurrent growth bypassed E");
        }
      });
    for (auto& t : requesters) t.join();
    failRegistration = true;
    pauseRegistration = false;
    end = std::chrono::steady_clock::now() + 2s;
    while (!p->stats().registration_failures &&
           std::chrono::steady_clock::now() < end)
      std::this_thread::sleep_for(1ms);
    check(p->stats().registration_failures && registered == 32768,
          "failed growth published or lost budget");
    failRegistration = false;
    end = std::chrono::steady_clock::now() + 2s;
    while (!extra && std::chrono::steady_clock::now() < end) {
      std::this_thread::sleep_for(2ms);
      extra = p->allocate(16384, UrmaMemoryPurpose::Tx);
    }
    check(extra && p->stats().registered_bytes == 65536,
          "protected free bytes suppressed needed growth");
    check(bool(p->allocate(16384, UrmaMemoryPurpose::Control)),
          "growth consumed protected reserve");
    auto cached = IoBufPool::createFromPoolShared(p, 100);
    check(bool(cached), "elastic TLS cache setup");
    cached.reset();
    extra = {};
    tx = {};
    end = std::chrono::steady_clock::now() + 2s;
    while (p->stats().registered_bytes != c.pool_size &&
           std::chrono::steady_clock::now() < end)
      std::this_thread::sleep_for(2ms);
    check(p->stats().registered_bytes == c.pool_size &&
              !p->stats().tls_retained_bytes,
          "automatic shrink did not reclaim idle TLS");
    check(p->shutdown(2s) && !registered, "protected growth cleanup");
    {
      // Repartition free initial storage while reserved-class allocations
      // live in a growth region. Idle shrink must rebuild those reserves in
      // retained storage, not keep the otherwise unused region forever.
      auto policy = config();
      policy.elastic = true;
      policy.pool_max_size = 98304;
      policy.region_bytes = 32768;
      policy.budgets.fill(policy.pool_max_size);
      policy.classes = {{16384, {{0, 0, 0, 1, 1, 0}}}, {32768, {}}};
      auto pool = std::make_shared<urma_detail::MemoryPool>(nullptr, policy);
      check(pool->warm(0ms), "reserve relocation warm");
      auto retained = pool->initialSegment();
      std::vector<UrmaPoolBuffer> pressure;
      for (unsigned n = 0; n < 4; ++n) {
        auto b = pool->allocate(16384, n < 3 ? UrmaMemoryPurpose::ReadSource
                                            : UrmaMemoryPurpose::ReadTarget);
        check(b && b.segment() == retained.segment(), "initial pressure fixture");
        pressure.push_back(std::move(b));
      }
      UrmaPoolBuffer source;
      auto deadline = std::chrono::steady_clock::now() + 2s;
      while (!source && std::chrono::steady_clock::now() < deadline) {
        source = pool->allocate(16384, UrmaMemoryPurpose::ReadSource);
        if (!source) std::this_thread::sleep_for(2ms);
      }
      auto target = pool->allocate(16384, UrmaMemoryPurpose::ReadTarget);
      check(source && target && source.segment() != retained.segment() &&
                target.segment() == source.segment(), "growth reserve fixture");
      pressure.clear();
      auto repartition = pool->allocate(32768, UrmaMemoryPurpose::Tx);
      check(repartition && repartition.segment() == retained.segment(),
            "initial free storage was not repartitioned");
      repartition = {};
      source = {};
      target = {};
      deadline = std::chrono::steady_clock::now() + 2s;
      while (pool->stats().registered_bytes != policy.pool_size &&
             std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(2ms);
      check(pool->stats().registered_bytes == policy.pool_size,
            "free reserves in growth region prevented idle shrink");
      source = pool->allocate(16384, UrmaMemoryPurpose::ReadSource);
      target = pool->allocate(16384, UrmaMemoryPurpose::ReadTarget);
      check(source && target && source.segment() == retained.segment() &&
                target.segment() == retained.segment(),
            "shrinking did not preserve usable source/target reserves");
      source = {}; target = {}; retained = {};
      check(pool->shutdown(2s) && !registered, "reserve relocation cleanup");
      std::cout << "PASS idle shrink restores reserved classes in retained storage\n";
    }
    // Contended allocation and TLS cache reclamation share the same public
    // pool.
    c = config();
    c.pool_size = 1024 * 1024;
    c.pool_max_size = 0;
    c.budgets.fill(c.pool_size);
    p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
    std::vector<std::thread> threads;
    for (unsigned n = 0; n < 8; ++n)
      threads.emplace_back([&, n] {
        for (size_t i = 0; i < 1000; ++i) {
          auto value = IoBufPool::createFromPoolShared(p, 128);
          check(bool(value), "concurrent slice allocation");
          std::memset(value->writableData(), int(n), 128);
          value->append(128);
          auto copy = value->cloneOne();
          value.reset();
          check(copy->data()[127] == n, "concurrent clone overwritten");
          auto block = p->allocate(4096, UrmaMemoryPurpose::Rx);
          check(bool(block), "concurrent whole block allocation");
          auto pin = p->lookup(block.data(), 4096, false);
          block = {};
          if (i % 50 == 0) p->trim();
        }
      });
    for (auto& t : threads) t.join();
    p->trim();
    check(!p->stats().allocated_bytes && p->shutdown(0ms),
          "concurrent pool leaked physical holds");
    std::cout << "PASS protected-class growth and 8-thread shared "
                 "pool/TLS/clone lifetime\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << '\n';
    // Avoid hiding the failed assertion behind a pool destructor terminate.
    std::_Exit(1);
  }
}
