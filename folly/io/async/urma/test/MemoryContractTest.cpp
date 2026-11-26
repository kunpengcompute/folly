/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaMemoryPool.h>
#include <folly/memory/IoBufPool.h>
#include <folly/ScopeGuard.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <fstream>
#include <limits>
#include <new>
#include <poll.h>
#include <stdexcept>
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

  c.classes = {{4096}, {12288}, {16384}};
  c.retry_ms = 2;
  c.idle_ms = 10;
  return c;
}
void rxUsesPoolHardLimit() {
  auto c = config();
  c.elastic = true;

  auto p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
  check(p->warm(0ms), "RX hard-limit pool warm");
  std::vector<UrmaPoolBuffer> buffers;
  const size_t target = c.pool_max_size / 4096;
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (buffers.size() < target && std::chrono::steady_clock::now() < deadline) {
    if (auto b = p->allocate(4096))
      buffers.push_back(std::move(b));
    else
      std::this_thread::sleep_for(2ms);
  }
  check(buffers.size() == target && buffers.size() * 4096 > c.pool_size,
        "shared allocations did not follow the expanded pool capacity");
  check(!p->allocate(4096) &&
            p->stats().registered_bytes <= c.pool_max_size,
        "allocation bypassed hard capacity");
  buffers.clear();
  check(p->shutdown(2s) && registered == 0, "RX hard-limit pool cleanup");
  std::cout << "PASS allocations share all expanded hard capacity\n";
}
void alignedTlsSlices() {
  auto c = config();
  auto p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
  check(p->warm(0ms), "alignment pool warm");
  std::vector<std::unique_ptr<IOBuf>> slices;
  const auto allocate = [&](size_t bytes) {
    auto b = IoBufPool::createFromPoolShared(p, bytes);
    check(bool(b), "alignment slice allocation");
    check(reinterpret_cast<uintptr_t>(b->data()) % alignof(std::max_align_t) == 0,
          "TLS slice is not max_align_t aligned");
    std::memset(b->writableData(), 0x5a, bytes);
    auto* address = b->writableData();
    slices.push_back(std::move(b));
    return address;
  };
  allocate(1);
  auto* word = new (allocate(sizeof(uint64_t))) uint64_t(0x123456789abcdef0ULL);
  check(*word == 0x123456789abcdef0ULL, "aligned typed slice write");
  for (size_t n : {3, 5, 17, 31, 127}) allocate(n);
  slices.clear();
  p->trim();

  // Leave only one byte after the preceding odd slice. Padding must prevent
  // using that tail even though the requested byte alone would fit.
  constexpr size_t alignment = alignof(std::max_align_t);
  const auto* first = allocate(1);
  auto pin = p->lookup(first, 1, false);
  const size_t usable = pin.capacity() -
      (reinterpret_cast<uintptr_t>(first) -
       reinterpret_cast<uintptr_t>(pin.data()));
  pin = {};
  const auto* sameBlock = allocate(usable - alignment - 1);
  check(sameBlock == first + alignment, "test did not fill the same TLS block");
  auto tail = IoBufPool::createFromPoolShared(p, 1);
  check(tail && reinterpret_cast<uintptr_t>(tail->data()) % alignment == 0 &&
            tail->data() != first + usable - 1,
        "alignment padding overran the remaining TLS capacity");
  for (const auto& b : slices)
    for (size_t n = 0; n < b->capacity(); ++n)
      check(b->data()[n] == 0x5a, "aligned slices overlap");
  check(!IoBufPool::createFromPoolShared(p, std::numeric_limits<size_t>::max()),
        "oversized TLS slice overflowed");
  tail.reset();
  slices.clear();
  check(p->shutdown(1000ms) && registered == 0, "alignment pool cleanup");
  std::cout << "PASS TLS odd slices, max_align_t/typed access, padding boundary "
               "and overflow\n";
}
void largeBodiesStayRegistered() {
  constexpr size_t kBodyBytes = 8 * 1024 * 1024;
  constexpr size_t kSerializationBytes = 64;
  constexpr size_t kRequests = 10;
  UrmaPoolConfig c;
  check(c.enabled && c.zero_copy && c.elastic && c.tls_cache_blocks &&
            c.pool_max_size > c.pool_size,
        "default policy must enable registered TLS and elastic capacity");
  c.elastic = false;
  auto p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
  check(p->warm(0ms), "default pool maintenance did not start");
  const auto registrations = calls.load();
  std::vector<std::unique_ptr<IOBuf>> bodies;
  for (size_t i = 0; i < kRequests; ++i) {
    auto b = IoBufPool::createFromPoolShared(p, kBodyBytes + kSerializationBytes);
    check(bool(b), "default pool cannot retain ten large RPC bodies");
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
  // Fill the shared capacity; lifetime pins still prevent reuse.
  while (auto b =
             IoBufPool::createFromPoolShared(p, kBodyBytes + kSerializationBytes))
    bodies.push_back(std::move(b));
  auto full = p->stats();
  check(full.allocated_bytes <= c.pool_size &&
            full.registered_bytes == c.pool_size && calls == registrations,
        "large-body allocation exceeded total capacity or registered per request");
  bodies.clear();
  check(p->stats().tls_retained_bytes == 0,
        "large bodies entered the small-block TLS cache");
  check(p->stats().allocated_bytes != 0 &&
            clone->data()[0] == 1 && clone->data()[clone->length() - 1] == 1,
        "large clone/source contents or accounting released early");
  clone.reset();
  const auto held = p->stats().allocated_bytes;
  check(held != 0 && !p->shutdown(0ms), "DMA/export-pinned large block released");
  dma = {};
  check(p->stats().allocated_bytes == held,
        "large exported source lost its physical charge");
  exported = {};
  check(p->shutdown(1000ms) && registered == 0,
        "large-body final return failed to reclaim registration");
  std::cout << "PASS ten concurrent 8 MiB bodies, total capacity, "
               "clone/DMA/export lifetime and single registration\n";
}
void tlsCacheBoundaries() {
  auto c = config();
  c.pool_size = c.pool_max_size = 256 * 1024;
  // The configured size need not be a class: account the rounded 16 KiB.
  c.tls_block_size = 13000;
  auto p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
  check(p->warm(0ms), "TLS boundary warm");
  auto small = IoBufPool::createFromPoolShared(p, 100);
  auto pin = p->lookup(small->data(), 100, false);
  const auto metadata = reinterpret_cast<uintptr_t>(small->data()) -
      reinterpret_cast<uintptr_t>(pin.data());
  const auto rounded = pin.capacity();
  pin = {}; small.reset();
  auto atLimit = IoBufPool::createFromPoolShared(p, c.tls_block_size - metadata);
  check(atLimit && p->stats().tls_retained_bytes <= c.tls_cache_blocks * rounded,
        "small TLS boundary exceeded rounded retained capacity");
  atLimit.reset();
  const auto retained = p->stats().tls_retained_bytes;
  const auto allocated = p->stats().allocated_bytes;
  for (size_t size : {c.tls_block_size - metadata + 1, size_t(64 * 1024)}) {
    auto large = IoBufPool::createFromPoolShared(p, size);
    check(large && IoBufPool::owns(*large) &&
              p->stats().tls_retained_bytes == retained,
          "large request entered or evicted small TLS cache");
    large.reset();
    check(p->stats().allocated_bytes == allocated,
          "uncached large block needed trim or another allocation to return");
  }
  // Evict cache ownership while a clone, DMA and remote READ still use it.
  small = IoBufPool::createFromPoolShared(p, 100);
  std::memset(small->writableData(), 0x7a, 100);
  small->append(100);
  auto clone = small->cloneOne();
  auto* address = small->data();
  auto dma = p->lookup(address, 100, false);
  auto remote = p->lookup(address, 100, true);
  small.reset();
  std::thread evict([&] { IoBufPool::reclaim(p->identity(), true); });
  evict.join();
  check(!p->stats().tls_retained_bytes && clone->data()[99] == 0x7a,
        "cross-thread eviction invalidated live slices");
  clone.reset(); dma = {};
  check(p->stats().allocated_bytes && address[99] == 0x7a,
        "eviction released remote READ storage before FREE");
  remote = {};
  check(!p->stats().allocated_bytes, "evicted block did not return after final pin");
  // A different allocating thread can reclaim an idle cache while its owner
  // thread remains alive. No dependence on trim, shutdown or thread exit.
  auto cached = IoBufPool::createFromPoolShared(p, 100);
  cached.reset();
  std::vector<UrmaPoolBuffer> held;
  while (auto b = p->allocate(4096)) held.push_back(std::move(b));
  bool reused = false;
  std::thread borrower([&] {
    auto b = IoBufPool::createFromPoolShared(p, c.tls_block_size);
    reused = bool(b);
  });
  borrower.join();
  check(reused && !p->stats().tls_retained_bytes,
        "cross-thread pressure did not reclaim idle small TLS storage");
  held.clear();
  std::unique_ptr<IOBuf> late;
  std::thread producer([&] { late = IoBufPool::createFromPoolShared(p, 64000); });
  producer.join();
  check(late && p->stats().allocated_bytes && !p->stats().tls_retained_bytes,
        "thread exit released live uncached large storage");
  late.reset();
  check(!p->stats().allocated_bytes && p->shutdown(2s) && registered == 0,
        "late large return did not release pool");
  std::cout << "PASS bounded small TLS, uncached large return, cross-thread reclaim and pinned eviction\n";
}
void resourceNotifications() {
  using namespace urma_detail;
  auto c = config();

  auto p = std::make_shared<MemoryPool>(nullptr, c);
  auto signal = std::make_shared<UrmaPoolSignal>();
  p->subscribe(signal);
  pollfd event{signal->fd(), POLLIN, 0};
  std::vector<UrmaPoolBuffer> pressure;
  for (size_t n = 4096; n < c.pool_size; n += 4096)
    pressure.push_back(p->allocate(4096));
  auto block = p->allocate(4096, signal.get());
  check(bool(block), "notification allocation setup");
  block = {};
  check(::poll(&event, 1, 0) == 0, "normal block return woke an idle owner");
  block = p->allocate(4096);
  check(!p->allocate(4096, signal.get()),
        "notification capacity shortage setup");
  std::thread returner([b = std::move(block)]() mutable { b = {}; });
  returner.join();
  check(::poll(&event, 1, 0) == 1, "failed allocation missed cross-thread return");
  signal->consume();
  block = p->allocate(4096, signal.get());
  block = {};
  check(::poll(&event, 1, 0) == 0, "satisfied allocation kept waking owner");

  // Exercise the shared pool's actual allocation/wakeup race, rather than
  // the removed per-connection RX budget. The worker is the last block owner.
  UrmaPoolBuffer held;
  std::atomic<unsigned> requested{0}, finished{0};
  std::thread concurrent([&] {
    for (unsigned i = 1; i <= 1000; ++i) {
      while (requested.load() < i) std::this_thread::yield();
      held = {};
      finished.store(i);
    }
  });
  for (unsigned i = 1; i <= 1000; ++i) {
    held = p->allocate(4096);
    check(bool(held), "concurrent return fixture allocation");
    requested.store(i);
    auto available = p->allocate(4096, signal.get());
    while (finished.load() < i) std::this_thread::yield();
    check(available || ::poll(&event, 1, 0) == 1,
          "concurrent physical-block return lost its wakeup");
    signal->consume();
  }
  concurrent.join();
  signal->signal();
  check(::poll(&event, 1, 0) == 1, "control notification incorrectly gated");
  signal->consume();
  pressure.clear();
  check(p->shutdown(0ms) && registered == 0, "notification cleanup");
  std::cout << "PASS quiet resource returns, allocation/clone waiters and "
               "1000 concurrent block-return races\n";
}
void largeExtents() {
  const long pageSize = sysconf(_SC_PAGESIZE);
  if (pageSize <= 0) {
    throw std::runtime_error("could not determine system page size");
  }
  const auto page = static_cast<size_t>(pageSize);
  constexpr size_t mib = 1024 * 1024;
  UrmaPoolConfig c;
  c.elastic = false;
  c.pool_size = c.pool_max_size = 64 * mib;
  auto p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
  auto prefix = p->allocate(4096);
  const size_t request = 16 * mib + 13;
  const size_t capacity = request + (page - request % page) % page;
  auto large = p->allocate(request);
  check(large && large.capacity() == capacity &&
            reinterpret_cast<uintptr_t>(large.data()) % page == 0 &&
            large.segment() == prefix.segment(),
        "large request did not carve a page-aligned registered extent");
  auto* address = large.data();
  auto pin = p->lookup(address, request, true);
  auto other = p->allocate(32 * mib);
  check(other && !p->allocate(20 * mib), "large fragmentation fixture");
  large = {};
  check(!p->allocate(request), "exported large extent was reused before FREE");
  pin = {};
  large = p->allocate(request);
  check(large && large.data() == address, "returned large extent was not reused");
  large = {}; other = {}; prefix = {};
  large = p->allocate(48 * mib + 1);
  check(large && p->stats().register_calls == 1 &&
            p->stats().allocated_bytes == large.capacity(),
        "returned extents failed to coalesce or physical charge was incorrect");
  check(!p->allocate(size_t(-1)) && !p->bufferSize(size_t(-1)) &&
            !p->allocate(c.pool_size + 1), "large capacity/alignment overflow");
  large = {};
  check(p->shutdown(0ms) && registered == 0, "large fixed pool cleanup");

  c.elastic = true;
  c.pool_size = c.region_bytes = 16 * mib;
  c.pool_max_size = 48 * mib;
  c.retry_ms = 2;
  p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
  p->warm(0ms);
  auto full = p->allocate(c.pool_size);
  failRegistration = true;
  check(!p->allocate(20 * mib + 1), "large growth unexpectedly synchronous");
  auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!p->stats().registration_failures && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(2ms);
  check(p->stats().registration_failures && registered == c.pool_size &&
            p->stats().registered_bytes + p->stats().registering_bytes <= c.pool_max_size,
        "failed large registration published storage or exceeded hard capacity");
  failRegistration = false;
  deadline = std::chrono::steady_clock::now() + 2s;
  while (!large && std::chrono::steady_clock::now() < deadline) {
    large = p->allocate(20 * mib + 1);
    if (!large) std::this_thread::sleep_for(2ms);
  }
  check(large && large.capacity() == 20 * mib + page &&
            large.segment() != full.segment() &&
            p->stats().registered_bytes == c.pool_size + large.capacity(),
        "large growth recovery did not use aligned physical capacity");
  large = {}; full = {};
  check(p->shutdown(2s) && registered == 0, "large growth cleanup");
  std::cout << "PASS >16 MiB alignment, reuse/coalescing, export pins, overflow and growth rollback\n";
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
    rxUsesPoolHardLimit();
    alignedTlsSlices();
    largeBodiesStayRegistered();
    tlsCacheBoundaries();
    resourceNotifications();
    largeExtents();
    check(argc == 3, "expected paths to urma.json and urma.example.json");
    UrmaConfig base;
    base.dev_name = "device-supplied-by-caller";
    base.eid_index = 3;
    const auto defaults = loadUrmaConfigFile(argv[1], base);
    check(defaults.dev_name == base.dev_name &&
              defaults.eid_index == base.eid_index,
          "urma.json changed the caller's device or EID index");
    check(defaults.registered_pool.enabled &&
              defaults.registered_pool.elastic &&
              defaults.registered_pool.zero_copy &&
              defaults.registered_pool == base.registered_pool &&
              defaults.registered_pool.pool_max_size >
                  defaults.registered_pool.pool_size,
          "urma.json changed the registered pool defaults");
    const auto example = loadUrmaConfigFile(argv[2], base);
    check(example.dev_name == "udmac0d1e2" && example.eid_index == 0u,
          "urma.example.json did not select udmac0d1e2 and EID index 0");
    char path[] = "/tmp/urma-config-test-XXXXXX";
    int fd = ::mkstemp(path);
    check(fd >= 0, "temporary config creation failed");
    ::close(fd);
    auto removeFile = makeGuard([&] { ::unlink(path); });
    for (uint32_t index : {uint32_t(0), uint32_t(7), UINT32_MAX}) {
      std::ofstream(path) << "{\"eid_index\":" << index << "}";
      check(loadUrmaConfigFile(path, base).eid_index == index,
            "JSON EID index did not override the caller's selection");
    }
    for (const char* key : {"io_owners", "max_water", "fc_rx_depth",
                            "fc_max_water", "rx_spare_blocks",
                            "io_free_rx_reserve", "credit_target",
                            "credit_refill_low", "credit_grant_cap",
                            "idle_timeout_ms", "owner_pending_bytes", "version",
                            "buffer_size", "enable_read", "read_threshold",
                            "read_window", "offer_window", "max_send_sge"}) {
      std::ofstream out(path);
      out << "{\"" << key << "\":1}";
      out.close();
      bool rejected = false;
      try { (void)loadUrmaConfigFile(path, base); }
      catch (const std::invalid_argument&) { rejected = true; }
      check(rejected, "removed shared/credit configuration was accepted");
    }
    for (const char* pool : {"{\"budgets\":{}}", "{\"tls_block_size\":4096}",
                             "{\"tls_cache_blocks\":0}", "{\"zero_copy\":false}",
                             "{\"classes\":[{\"bytes\":4096,\"reserve\":{}}]}"}) {
      std::ofstream out(path);
      out << "{\"registered_pool\":" << pool << "}";
      out.close();
      bool rejected = false;
      try { (void)loadUrmaConfigFile(path, base); }
      catch (const std::invalid_argument&) { rejected = true; }
      check(rejected, "obsolete purpose isolation accepted");
    }
    struct BadNumber { const char* name; uint32_t UrmaConfig::*member; uint32_t value; };
    for (auto bad : {BadNumber{"rx_depth", &UrmaConfig::rx_depth, 65537},
                     BadNumber{"rx_target", &UrmaConfig::rx_target, 0},
                     BadNumber{"tx_depth", &UrmaConfig::tx_depth, 0},
                     BadNumber{"tx_depth", &UrmaConfig::tx_depth, 4097},
                     BadNumber{"handshake_timeout_ms", &UrmaConfig::handshake_timeout_ms, 0},
                     BadNumber{"handshake_timeout_ms", &UrmaConfig::handshake_timeout_ms, 60001},
                     BadNumber{"max_pending_bytes", &UrmaConfig::max_pending_bytes, 4095}}) {
      auto direct = base;
      direct.*bad.member = bad.value;
      auto rejects = [&](auto action) {
        try { action(); } catch (const std::invalid_argument& e) {
          return std::string(e.what()).find(bad.name) != std::string::npos;
        }
        return false;
      };
      check(rejects([&] { validateUrmaConfig(direct); }), "C++ invalid field not diagnosed");
      std::ofstream out(path);
      out << "{\"" << bad.name << "\":" << bad.value << "}";
      out.close();
      check(rejects([&] { loadUrmaConfigFile(path, base); }), "JSON/C++ validation differs");
    }
    for (const char* json : {"{\"rx_depth\":4,\"rx_target\":5}",
                             "{\"eid_index\":-1}", "{\"eid_index\":2.5}",
                             "{\"eid_index\":4294967296}", "{\"eid_index\":true}",
                             "{\"eid_index\":\"7\"}", "{\"eid_index\":null}",
                             "{\"tx_depth\":1,\"tx_depth\":2}",
                             "{\"tx_depth\":1.5}", "{\"tx_depth\":true}",
                             "{\"tx_depth\":-1}", "{\"tx_depth\":4294967296}",
                             "{\"registered_pool\":{\"pool_size\":0}}",
                             "{\"registered_pool\":{\"pool_size\":65537}}",
                             "{\"registered_pool\":{\"pool_max_size\":65536}}"}) {
      std::ofstream out(path); out << json; out.close();
      bool rejected = false;
      try { loadUrmaConfigFile(path, base); }
      catch (const std::invalid_argument&) { rejected = true; }
      check(rejected, "invalid type, duplicate, or dependent limits accepted");
    }
    { std::ofstream out(path); out << "{}"; }
    check(!loadUrmaConfigFile(path).eid_index, "omitted EID index selected zero");
    const auto inherited = loadUrmaConfigFile(path, base);
    check(inherited.dev_name == base.dev_name &&
              inherited.eid_index == base.eid_index,
          "empty overlay lost the caller's device or EID index");
    std::cout << "PASS versionless config, public fields, ranges and JSON/C++ parity\n";
    using namespace urma_detail;
    auto c = config();
    auto bad = c;
    bad.pool_max_size = bad.pool_size - 4096;
    bool rejected = false;
    try {
      urma_detail::MemoryPool::validate(bad);
    } catch (...) {
      rejected = true;
    }
    check(rejected, "initial capacity exceeded hard limit");
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
    auto tx = p->allocate(12000);
    auto rx = p->allocate(4096);
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
    check(p->stats().allocated_bytes == 16384,
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
    while (auto b = p->allocate(4096)) held.push_back(b);
    check(held.size() * 4096 == c.pool_size && !p->allocate(4096),
          "unused class reserve or allocation past total capacity");
    check(calls == before && p->stats().registering_bytes == 0 &&
              registered == c.pool_size,
          "fixed pool grew because hard > soft");
    held.clear();
    std::cout << "PASS fixed soft/hard, shared class capacity, 12 "
                 "KiB, DMA/export lifetime\n";
    auto a = IoBufPool::createFromPoolShared(p, 100);
    auto b = IoBufPool::createFromPoolShared(p, 100);
    check(a && b && a->buffer() == a->data() && a->length() == 0 &&
              a->capacity() == 100,
          "slice create contract");
    check(p->lookup(a->data(), 100, false).data() ==
                  p->lookup(b->data(), 100, false).data() &&
              b->data() >= a->data() + a->capacity() &&
              reinterpret_cast<uintptr_t>(b->data()) % alignof(std::max_align_t) == 0,
          "aligned small slices must share a block without overlapping");
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
    dma = {};
    check(!p->stats().dma_bytes && p->stats().exported_bytes,
          "export-only source lifetime setup");
    auto different = IoBufPool::createFromPoolShared(p, 15000);
    check(different && different->data() != pinnedAddress,
          "TLS recycled a DMA/export pinned slice");
    std::memset(different->writableData(), 0x11, 15000);
    check(pinnedAddress[14999] == 0x7c,
          "exported source was overwritten after IOBuf and DMA release");
    different.reset();
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
    // Exhaust the initial region before requesting growth.
    for (int i = 0; i < 4; ++i)
      held.push_back(p->allocate(16384));
    auto extra = p->allocate(16384);
    auto end = std::chrono::steady_clock::now() + 2s;
    while (!extra && std::chrono::steady_clock::now() < end) {
      std::this_thread::sleep_for(2ms);
      extra = p->allocate(16384);
    }
    check(bool(extra), "elastic growth did not publish");
    check(extra.segment() != initial.segment() &&
              bool(p->lookup(extra.data(), 1, true)),
          "new region supports READ pins; transport gates publication");
    check(p->stats().registered_bytes + p->stats().registering_bytes <=
              c.pool_max_size,
          "R+E exceeded hard");
    auto advertised = p->holdSegment(extra.regionId());
    check(bool(advertised), "announced region lease");
    auto retiringDma = p->lookup(extra.data(), extra.capacity(), false);
    const auto retiringId = extra.regionId();
    check(bool(retiringDma), "retirement DMA hold setup");
    p->retire(extra);
    check(p->revocableSegments().empty(), "live block was revocable");
    check(!p->holdSegment(retiringId), "retiring region accepted a new lease");
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
    check(p->revocableSegments() == std::vector<uint64_t>{retiringId},
          "idle published segment did not become revocable");
    pollfd retiredEvent{retiredWake->fd(), POLLIN, 0};
    check(::poll(&retiredEvent, 1, 0) == 1,
          "retired block became revocable without waking waiters");
    std::this_thread::sleep_for(30ms);
    check(p->stats().registered_bytes == c.pool_max_size,
          "announced segment unregistered before peer lease ended");
    advertised = {};
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
                 "HELLO and announced-region leases\n";
    // Growth reserves capacity before unlocking for registration.
    c.pool_size = 32768;
    c.pool_max_size = 65536;
    c.region_bytes = 32768;

    c.classes = {{4096}, {16384}};
    p = std::make_shared<urma_detail::MemoryPool>(nullptr, c);
    p->warm(0ms);
    tx = p->allocate(16384);
    check(bool(tx), "protected growth setup");
    auto occupied = p->allocate(16384);
    pauseRegistration = true;
    extra = p->allocate(16384);
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
          check(!p->allocate(16384),
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
      extra = p->allocate(16384);
    }
    check(extra && p->stats().registered_bytes == 65536,
          "failed registration did not recover");
    check(bool(p->allocate(16384)),
          "remaining shared capacity unavailable");
    auto cached = IoBufPool::createFromPoolShared(p, 100);
    check(bool(cached), "elastic TLS cache setup");
    cached.reset();
    extra = {};
    tx = {};
    occupied = {};
    end = std::chrono::steady_clock::now() + 2s;
    while (p->stats().registered_bytes != c.pool_size &&
           std::chrono::steady_clock::now() < end)
      std::this_thread::sleep_for(2ms);
    check(p->stats().registered_bytes == c.pool_size &&
              !p->stats().tls_retained_bytes,
          "automatic shrink did not reclaim idle TLS");
    check(p->shutdown(2s) && !registered, "protected growth cleanup");
    // Contended allocation and TLS cache reclamation share the same public
    // pool.
    c = config();
    c.pool_size = 1024 * 1024;
    c.pool_max_size = 0;

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
          auto block = p->allocate(4096);
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
    std::cout << "PASS growth rollback and 8-thread shared "
                 "pool/TLS/clone lifetime\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << '\n';
    // Avoid hiding the failed assertion behind a pool destructor terminate.
    std::_Exit(1);
  }
}
