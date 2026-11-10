/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaMemoryPool.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/FileUtil.h>
#include <folly/memory/IoBufPool.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <condition_variable>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace folly {
UrmaPoolSignal::UrmaPoolSignal() : fd_(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
  if (fd_ < 0)
    throw std::runtime_error("URMA pool notification fd unavailable");
}
UrmaPoolSignal::~UrmaPoolSignal() { close(fd_); }
void UrmaPoolSignal::signal() noexcept {
  if (enabled_.load(std::memory_order_acquire) &&
      !pending_.exchange(true, std::memory_order_acq_rel)) {
    const uint64_t one = 1;
    (void)writeNoInt(fd_, &one, sizeof(one));
  }
}
void UrmaPoolSignal::consume() noexcept {
  uint64_t value;
  (void)readNoInt(fd_, &value, sizeof(value));
  pending_.store(false, std::memory_order_release);
}
bool UrmaPoolConfig::operator==(const UrmaPoolConfig& o) const {
  return enabled == o.enabled && zero_copy == o.zero_copy &&
         pool_size == o.pool_size && pool_max_size == o.pool_max_size &&
         elastic == o.elastic && tls_block_size == o.tls_block_size &&
         tls_cache_blocks == o.tls_cache_blocks &&
         region_bytes == o.region_bytes && budgets == o.budgets &&
         classes == o.classes && idle_ms == o.idle_ms && retry_ms == o.retry_ms;
}
namespace urma_detail {
using Clock = std::chrono::steady_clock;
struct MemoryPin {
  void* data{nullptr};
  size_t bytes{0};
  urma_target_seg_t* segment{nullptr};
  uint64_t id{0};
  virtual ~MemoryPin() = default;
};
struct MemoryPool::State : std::enable_shared_from_this<State> {
  static constexpr size_t unused = std::numeric_limits<size_t>::max();
  struct Region;
  struct Block {
    Region* region{nullptr};
    size_t offset{0}, bytes{0}, tier{unused}, purpose{0};
    size_t refs[3]{};  // allocation, local DMA, exported READ
    bool cached{false}, listed{false};
    Block* previous{nullptr};
    Block* next{nullptr};
    bool busy() const { return refs[0] || refs[1] || refs[2]; }
  };
  struct Region {
    uint32_t id{0};
    size_t bytes{0}, holds{0};
    void* base{nullptr};
    urma_target_seg_t* segment{nullptr};
    std::map<size_t, Block> blocks;
    bool initial{false}, building{true}, retiring{false};
    int error{0};
    Clock::time_point idle{Clock::now()}, retry{};
    bool busy() const {
      if (holds) return true;
      for (const auto& b : blocks)
        if (b.second.busy()) return true;
      return false;
    }
  };
  struct Bucket {
    Block* free{nullptr};
    size_t count{0};
    std::array<size_t, kUrmaMemoryPurposes> allocated{};
    std::array<bool, kUrmaMemoryPurposes> requested{};
  };
  urma_context_t* context;
  const UrmaPoolConfig config;
  const size_t hard;
  const size_t page{static_cast<size_t>(sysconf(_SC_PAGESIZE))};
  mutable std::mutex mutex;
  std::condition_variable changed;
  std::vector<Bucket> buckets;
  std::vector<std::shared_ptr<Region>> regions;
  std::vector<std::weak_ptr<UrmaPoolSignal>> signals;
  std::thread worker;
  std::array<size_t, kUrmaMemoryPurposes> held{};
  size_t registered{0}, reserved{0}, nextId{1};
  bool stopping{false}, done{false};
  UrmaPoolStats counters;
  Clock::time_point growthRetry{}, cacheTrim{};
  void notify() noexcept {
    changed.notify_all();
    for (const auto& weak : signals)
      if (auto signal = weak.lock()) signal->signal();
  }

  State(urma_context_t* ctx, const UrmaPoolConfig& c)
      : context(ctx),
        config(c),
        hard(c.pool_max_size ? c.pool_max_size : c.pool_size),
        buckets(c.classes.size()) {}
  void unlink(Block& b) noexcept {
    if (!b.listed) return;
    auto& list = buckets[b.tier];
    if (b.previous)
      b.previous->next = b.next;
    else
      list.free = b.next;
    if (b.next) b.next->previous = b.previous;
    b.previous = b.next = nullptr;
    b.listed = false;
    --list.count;
  }
  void link(Block& b) noexcept {
    if (b.listed || b.busy() || b.tier == unused || b.region->building ||
        b.region->retiring)
      return;
    auto& list = buckets[b.tier];
    b.next = list.free;
    if (b.next) b.next->previous = &b;
    list.free = &b;
    b.listed = true;
    ++list.count;
  }
  size_t protectedFree(size_t tier, size_t except = unused) const {
    size_t count = 0;
    for (size_t p = 0; p < kUrmaMemoryPurposes; ++p)
      if (p != except &&
          config.classes[tier].reserve[p] > buckets[tier].allocated[p])
        count += config.classes[tier].reserve[p] - buckets[tier].allocated[p];
    return count;
  }
  Block* split(Region& r, Block& b, size_t tier) {
    const size_t bytes = config.classes[tier].bytes;
    // Allocate metadata before changing the original extent.
    if (b.bytes > bytes) {
      auto& tail = r.blocks.try_emplace(b.offset + bytes).first->second;
      tail.region = &r;
      tail.offset = b.offset + bytes;
      tail.bytes = b.bytes - bytes;
    }
    b.bytes = bytes;
    b.tier = tier;
    link(b);
    return &b;
  }
  void coalesce(Region& r) noexcept {
    for (auto& item : r.blocks) {
      auto& b = item.second;
      if (b.listed && buckets[b.tier].count > protectedFree(b.tier)) {
        unlink(b);
        b.tier = unused;
      }
    }
    for (auto it = r.blocks.begin(); it != r.blocks.end();) {
      auto next = std::next(it);
      if (it->second.tier == unused && next != r.blocks.end() &&
          next->second.tier == unused) {
        it->second.bytes += next->second.bytes;
        r.blocks.erase(next);
      } else
        ++it;
    }
  }
  Block* carve(size_t tier, const Region* excluded = nullptr) {
    for (auto& r : regions) {
      if (r.get() == excluded || r->building || r->retiring) continue;
      for (auto& item : r->blocks) {
        auto& b = item.second;
        if (b.tier == unused && b.bytes >= config.classes[tier].bytes)
          return split(*r, b, tier);
      }
    }
    return nullptr;
  }
  std::shared_ptr<Region> prepare(size_t bytes, bool initial) {
    if (nextId > UINT32_MAX)
      throw std::runtime_error("URMA region ID exhausted");
    auto r = std::make_shared<Region>();
    r->id = static_cast<uint32_t>(nextId++);
    r->bytes = bytes;
    r->initial = initial;
    auto& b = r->blocks.try_emplace(0).first->second;
    b.region = r.get();
    b.bytes = bytes;
    if (initial) {
      // Largest first; one initial segment, not one segment per purpose.
      size_t offset = 0;
      for (size_t i = config.classes.size(); i; --i) {
        size_t count = 0;
        for (auto n : config.classes[i - 1].reserve) count += n;
        for (size_t n = 0; n < count; ++n) {
          split(*r, r->blocks.at(offset), i - 1);
          offset += config.classes[i - 1].bytes;
        }
      }
    }
    if (bytes > hard - registered - reserved)
      throw std::runtime_error("URMA registration hard limit exceeded");
    regions.push_back(r);  // last allocating operation before SDK registration
    reserved += bytes;
    return r;
  }
  bool registerRegion(Region& r) noexcept {
    r.base = mmap(nullptr, r.bytes, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (r.base == MAP_FAILED) {
      r.base = nullptr;
      return false;
    }
    urma_seg_cfg_t cfg{};
    cfg.va = reinterpret_cast<uint64_t>(r.base);
    cfg.len = r.bytes;
    cfg.flag.bs.access = r.initial ? URMA_ACCESS_READ : URMA_ACCESS_LOCAL_ONLY;
    r.segment = urma_register_seg(context, &cfg);
    if (!r.segment) {
      munmap(r.base, r.bytes);
      r.base = nullptr;
      return false;
    }
    return true;
  }
  void publish(Region& r) noexcept {
    reserved -= r.bytes;
    registered += r.bytes;
    r.building = false;
    for (auto& b : r.blocks) link(b.second);
    notify();
  }
  void markRetiring(Region& r) noexcept {
    r.retiring = true;
    for (auto& b : r.blocks) unlink(b.second);
  }
  void remove(const std::shared_ptr<Region>& r) noexcept {
    if (r->building)
      reserved -= r->bytes;
    else
      registered -= r->bytes;
    auto it = std::find(regions.begin(), regions.end(), r);
    *it = std::move(regions.back());
    regions.pop_back();
  }
  UrmaPoolBuffer pin(const std::shared_ptr<Region>& r, Block* b, size_t kind) {
    struct Pin final : MemoryPin {
      std::shared_ptr<State> state;
      std::shared_ptr<Region> region;
      Block* block;
      size_t kind;
      Pin(std::shared_ptr<State> s, std::shared_ptr<Region> r, Block* b,
          size_t k)
          : state(std::move(s)), region(std::move(r)), block(b), kind(k) {}
      ~Pin() override {
        std::lock_guard<std::mutex> lock(state->mutex);
        bool returnedBudget = false;
        if (!block)
          --region->holds;
        else {
          --block->refs[kind];
          if (!block->busy()) {
            state->held[block->purpose] -= block->bytes;
            --state->buckets[block->tier].allocated[block->purpose];
            block->cached = false;
            state->link(*block);
            returnedBudget = true;
          }
        }
        if (!region->busy()) region->idle = Clock::now();
        // Ending one DMA/export pin often leaves the allocation and other
        // pins intact. That creates no allocatable storage: waking every IO
        // owner here just makes them poll and contend for the same pool again.
        // Retiring blocks cannot be allocated again, but returning their
        // purpose budget can unblock allocation from another live region.
        if (returnedBudget)
          state->notify();  // physical capacity/accounting returned first
        else
          state->changed.notify_all();  // retirement/shutdown may still advance
      }
    };
    auto p = std::make_shared<Pin>(shared_from_this(), r, b, kind);
    p->data = static_cast<char*>(r->base) + (b ? b->offset : 0);
    p->bytes = b ? b->bytes : r->bytes;
    p->segment = r->segment;
    p->id = r->id;
    if (b) {
      unlink(*b);
      ++b->refs[kind];
    } else
      ++r->holds;
    r->idle = Clock::now();
    return UrmaPoolBuffer(std::move(p));
  }
  bool preservesReserves(const Region& r) const {
    for (size_t t = 0; t < buckets.size(); ++t) {
      size_t removed = 0;
      for (const auto& b : r.blocks)
        removed += b.second.listed && b.second.tier == t;
      if (buckets[t].count - removed < protectedFree(t)) return false;
    }
    return true;
  }
  bool prepareShrink(Region& candidate) {
    if (preservesReserves(candidate)) return true;
    // A free reserved block can end up in a growth region after pressure.
    // Recreate the required free blocks in retained storage before retiring
    // that region. Never move live buffers or count the candidate as backing.
    for (auto& r : regions)
      if (r.get() != &candidate && !r->building && !r->retiring) coalesce(*r);
    for (size_t t = 0; t < buckets.size(); ++t) {
      size_t removed = 0;
      for (const auto& b : candidate.blocks)
        removed += b.second.listed && b.second.tier == t;
      while (buckets[t].count - removed < protectedFree(t))
        if (!carve(t, &candidate)) return false;
    }
    return preservesReserves(candidate);
  }
  // Called only by the elastic worker or UrmaInit's deferred cleanup worker.
  bool cleanupOne(std::unique_lock<std::mutex>& lock) {
    for (auto& entry : regions) {
      auto r = entry;
      if (!r->retiring || r->building || r->busy() || Clock::now() < r->retry)
        continue;
      lock.unlock();
      const auto began = Clock::now();
      const int status = urma_unregister_seg(r->segment);
      if (status == URMA_SUCCESS) munmap(r->base, r->bytes);
      lock.lock();
      counters.unregister_nanoseconds +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                               began)
              .count();
      ++counters.unregister_calls;
      if (status == URMA_SUCCESS)
        remove(r);
      else {
        r->error = status;
        r->retry = Clock::now() + std::chrono::milliseconds(config.retry_ms);
        ++counters.unregister_failures;
      }
      changed.notify_all();
      return true;
    }
    return false;
  }
  void run() noexcept {
    std::unique_lock<std::mutex> lock(mutex);
    for (;;) {
      try {
        if (stopping)
          for (auto& r : regions) markRetiring(*r);
        if (stopping && regions.empty()) {
          done = true;
          changed.notify_all();
          return;
        }
        if (cleanupOne(lock)) continue;
        const auto now = Clock::now();
        if (!stopping) {
          if (registered > config.pool_size && now >= cacheTrim) {
            lock.unlock();
            IoBufPool::reclaim(this, false);
            lock.lock();
            cacheTrim =
                Clock::now() + std::chrono::milliseconds(config.idle_ms);
            if (stopping) continue;
          }
          for (auto& r : regions)
            if (!r->initial && !r->building && !r->retiring && !r->busy() &&
                registered - r->bytes >= config.pool_size &&
                now - r->idle >= std::chrono::milliseconds(config.idle_ms) &&
                prepareShrink(*r))
              markRetiring(*r);
          bool demand = false;
          for (const auto& b : buckets)
            for (bool v : b.requested) demand |= v;
          if (demand && now >= growthRetry) {
            lock.unlock();
            IoBufPool::reclaim(this, false);
            lock.lock();
            if (stopping) continue;
            for (auto& r : regions)
              if (!r->retiring) coalesce(*r);
            size_t needed = 0;
            for (size_t t = 0; t < buckets.size(); ++t) {
              for (size_t p = 0; p < kUrmaMemoryPurposes; ++p) {
                auto& request = buckets[t].requested[p];
                if (!request) continue;
                const size_t n = config.classes[t].bytes;
                if (n > config.budgets[p] - held[p]) {
                  request = false;
                  continue;
                }
                if (buckets[t].count <= protectedFree(t, p)) carve(t);
                // A free block reserved for another purpose is not capacity
                // for this waiter. Re-evaluate exactly the requested class.
                if (buckets[t].count > protectedFree(t, p))
                  request = false;
                else
                  needed = std::max(needed, n);
              }
            }
            const auto minimum = std::max(config.region_bytes, needed);
            const auto padding = (page - minimum % page) % page;
            const auto available = hard - registered - reserved;
            if (needed && minimum <= available &&
                padding <= available - minimum) {
              const auto bytes = minimum + padding;
              auto r = prepare(bytes, false);  // E charged before unlocking
              lock.unlock();
              const auto began = Clock::now();
              const bool ok = registerRegion(*r);
              lock.lock();
              counters.register_nanoseconds +=
                  std::chrono::duration_cast<std::chrono::nanoseconds>(
                      Clock::now() - began)
                      .count();
              ++counters.register_calls;
              if (ok)
                publish(*r);
              else {
                ++counters.registration_failures;
                remove(r);
              }
              growthRetry =
                  Clock::now() + std::chrono::milliseconds(config.retry_ms);
            }
          }
        }
      } catch (const std::bad_alloc&) {
        growthRetry = Clock::now() + std::chrono::milliseconds(config.retry_ms);
      }
      changed.wait_for(lock, std::chrono::milliseconds(config.retry_ms));
    }
  }
};
void MemoryPool::validate(const UrmaPoolConfig& c) {
  if (!c.enabled) return;
  const auto page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
  const size_t hard = c.pool_max_size ? c.pool_max_size : c.pool_size;
  if (!page || !c.pool_size || c.pool_size % page || hard % page ||
      c.pool_size > hard || !c.region_bytes || c.region_bytes % page ||
      c.classes.empty() || c.classes.size() > 64 || !c.retry_ms || !c.idle_ms ||
      c.tls_cache_blocks > 16 || c.tls_block_size < 4096)
    throw std::invalid_argument(
        "invalid URMA soft/hard capacity, alignment or cache");
  size_t previous = 0, reserveBytes = 0;
  std::array<size_t, kUrmaMemoryPurposes> byPurpose{};
  for (const auto& tier : c.classes) {
    if (tier.bytes <= previous || tier.bytes % 4096 || tier.bytes > UINT32_MAX)
      throw std::invalid_argument(
          "URMA size classes must increase in 4 KiB units");
    previous = tier.bytes;
    for (size_t p = 0; p < kUrmaMemoryPurposes; ++p) {
      if (byPurpose[p] > c.budgets[p] ||
          tier.reserve[p] > (c.budgets[p] - byPurpose[p]) / tier.bytes ||
          tier.reserve[p] > (c.pool_size - reserveBytes) / tier.bytes)
        throw std::invalid_argument(
            "URMA startup class reserves exceed soft capacity/budget");
      const size_t n = tier.reserve[p] * tier.bytes;
      byPurpose[p] += n;
      reserveBytes += n;
    }
  }
  if (c.tls_block_size > c.classes.back().bytes)
    throw std::invalid_argument("TLS block has no registered size class");
  for (auto n : c.budgets)
    if (!n || n > hard || n % 4096)
      throw std::invalid_argument(
          "URMA purpose budget outside hard capacity/alignment");
}
MemoryPool::MemoryPool(urma_context_t* context, const UrmaPoolConfig& config) {
  validate(config);
  auto s = std::make_shared<State>(context, config);
  auto r = s->prepare(config.pool_size, true);
  const auto began = Clock::now();
  if (!s->registerRegion(*r)) {
    s->remove(r);
    throw std::runtime_error("URMA initial soft-capacity registration failed");
  }
  s->counters.register_nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - began)
          .count();
  s->counters.register_calls = 1;
  s->publish(*r);
  state_ = std::move(s);
}
MemoryPool::~MemoryPool() {
  if (state_->worker.joinable() || !state_->regions.empty()) std::terminate();
}
UrmaPoolBuffer MemoryPool::allocate(size_t bytes, UrmaMemoryPurpose purpose) {
  auto& s = *state_;
  std::lock_guard<std::mutex> lock(s.mutex);
  const size_t p = size_t(purpose);
  if (s.stopping || !bytes || p >= kUrmaMemoryPurposes) return {};
  for (size_t t = 0; t < s.buckets.size(); ++t) {
    const size_t capacity = s.config.classes[t].bytes;
    if (capacity < bytes) continue;
    if (capacity > s.config.budgets[p] - s.held[p]) break;
    try {
      if (s.buckets[t].count <= s.protectedFree(t, p)) {
        if (!s.carve(t)) {
          for (auto& r : s.regions)
            if (!r->building && !r->retiring) s.coalesce(*r);
          s.carve(t);
        }
      }
      if (s.buckets[t].count > s.protectedFree(t, p)) {
        auto* b = s.buckets[t].free;
        for (auto& r : s.regions)
          if (r.get() == b->region) {
            auto result = s.pin(r, b, 0);
            b->purpose = p;
            s.held[p] += b->bytes;
            ++s.buckets[t].allocated[p];
            s.buckets[t].requested[p] = false;
            ++s.counters.fast_allocations;
            return result;
          }
      }
      if (s.config.elastic && s.worker.joinable()) {
        s.buckets[t].requested[p] = true;
        s.changed.notify_all();
      }
    } catch (const std::bad_alloc&) {
    }
    break;  // Do not silently skip a configured 12 KiB tier for 16 KiB.
  }
  ++s.counters.allocation_failures;
  return {};
}
UrmaPoolBuffer MemoryPool::lookup(const void* address, size_t length,
                                  bool exported) {
  auto& s = *state_;
  std::lock_guard<std::mutex> lock(s.mutex);
  const auto start = reinterpret_cast<uintptr_t>(address);
  if (s.stopping || !start || !length) return {};
  for (auto& r : s.regions) {
    if (r->building || r->retiring || (exported && !r->initial)) continue;
    const auto base = reinterpret_cast<uintptr_t>(r->base);
    if (start < base || start - base >= r->bytes ||
        length > r->bytes - (start - base))
      continue;
    const size_t offset = start - base;
    auto it = r->blocks.upper_bound(offset);
    if (it == r->blocks.begin()) return {};
    auto& b = std::prev(it)->second;
    if (!b.refs[0] || length > b.bytes - (offset - b.offset)) return {};
    try {
      return s.pin(r, &b, exported ? 2 : 1);
    } catch (const std::bad_alloc&) {
      return {};
    }
  }
  return {};
}
UrmaPoolBuffer MemoryPool::initialSegment() {
  auto& s = *state_;
  std::lock_guard<std::mutex> lock(s.mutex);
  if (!s.stopping)
    for (auto& r : s.regions)
      if (r->initial && !r->retiring) return s.pin(r, nullptr, 0);
  return {};
}
void MemoryPool::retire(const UrmaPoolBuffer& buffer) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    for (auto& r : state_->regions)
      if (r->id == buffer.regionId() && r->segment == buffer.segment())
        if (!r->initial) state_->markRetiring(*r);
    state_->changed.notify_all();
  }
  IoBufPool::reclaim(state_.get(), true);
}
void MemoryPool::trim() { IoBufPool::reclaim(state_.get(), false); }
bool MemoryPool::warm(std::chrono::milliseconds) {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->stopping) return false;
  if (state_->config.elastic && !state_->worker.joinable())
    state_->worker = std::thread([s = state_] { s->run(); });
  return state_->registered >= state_->config.pool_size;
}
bool MemoryPool::shutdown(std::chrono::milliseconds timeout) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->stopping = true;
    for (auto& r : state_->regions) state_->markRetiring(*r);
    state_->changed.notify_all();
  }
  IoBufPool::reclaim(state_.get(), true);
  std::unique_lock<std::mutex> lock(state_->mutex);
  if (state_->worker.joinable()) {
    if (!state_->changed.wait_for(lock, timeout, [&] { return state_->done; }))
      return false;
    lock.unlock();
    state_->worker.join();
    return true;
  }
  // Fixed mode has no maintenance thread. This is deferred shutdown work.
  while (state_->cleanupOne(lock)) {
  }
  return state_->regions.empty();
}
size_t MemoryPool::bufferSize(size_t requested) const noexcept {
  for (const auto& t : state_->config.classes)
    if (t.bytes >= requested) return t.bytes;
  return 0;
}
const UrmaPoolConfig& MemoryPool::config() const noexcept {
  return state_->config;
}
const void* MemoryPool::identity() const noexcept { return state_.get(); }
void MemoryPool::subscribe(const std::shared_ptr<UrmaPoolSignal>& signal) {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->stopping) throw std::runtime_error("URMA pool is stopping");
  // Owners are short-lived; retaining expired weak references grows both
  // notification work and control-block storage with total connection churn.
  auto& signals = state_->signals;
  signals.erase(std::remove_if(signals.begin(), signals.end(),
                               [](const auto& p) { return p.expired(); }),
                signals.end());
  state_->signals.push_back(signal);
}
void MemoryPool::setCached(const UrmaPoolBuffer& buffer, bool cached) {
  std::lock_guard<std::mutex> lock(state_->mutex);
  for (auto& r : state_->regions)
    if (r->id == buffer.regionId())
      for (auto& item : r->blocks)
        if (static_cast<char*>(r->base) + item.second.offset == buffer.data())
          item.second.cached = cached;
}
bool MemoryPool::reusable(const UrmaPoolBuffer& buffer, bool recycle) const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (!state_->stopping)
    for (auto& r : state_->regions)
      if (r->id == buffer.regionId() && r->segment == buffer.segment()) {
        if (r->retiring) return false;
        if (!recycle) return true;
        const auto offset = static_cast<const char*>(buffer.data()) -
                            static_cast<const char*>(r->base);
        auto it = r->blocks.find(offset);
        return it != r->blocks.end() && !it->second.refs[1] &&
               !it->second.refs[2];
      }
  return false;
}
UrmaPoolStats MemoryPool::stats() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  auto out = state_->counters;
  out.soft_bytes = state_->config.pool_size;
  out.hard_bytes = state_->hard;
  out.registered_bytes = state_->registered;
  out.registering_bytes = state_->reserved;
  out.held_by_purpose = state_->held;
  out.regions = state_->regions.size();
  for (const auto& r : state_->regions) {
    if (r->building) continue;
    if (r->retiring) out.retiring_bytes += r->bytes;
    for (const auto& item : r->blocks) {
      const auto& b = item.second;
      if (b.busy()) out.allocated_bytes += b.bytes;
      if (b.refs[1]) out.dma_bytes += b.bytes;
      if (b.refs[2]) out.exported_bytes += b.bytes;
      if (b.cached) out.tls_retained_bytes += b.bytes;
      if (!b.busy() && !r->retiring) out.free_bytes += b.bytes;
    }
  }
  return out;
}
std::vector<UrmaPoolRegionInfo> MemoryPool::regions() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  std::vector<UrmaPoolRegionInfo> result;
  for (const auto& r : state_->regions) {
    UrmaPoolRegionInfo info{r->id, r->bytes, 0, 0, 0, r->retiring, r->error};
    for (const auto& item : r->blocks) {
      info.allocated_blocks += item.second.busy();
      info.dma_refs += item.second.refs[1];
      info.exported_read_refs += item.second.refs[2];
    }
    result.push_back(info);
  }
  return result;
}
}  // namespace urma_detail
void* UrmaPoolBuffer::data() const { return pin_ ? pin_->data : nullptr; }
size_t UrmaPoolBuffer::capacity() const { return pin_ ? pin_->bytes : 0; }
urma_target_seg_t* UrmaPoolBuffer::segment() const {
  return pin_ ? pin_->segment : nullptr;
}
uint64_t UrmaPoolBuffer::regionId() const { return pin_ ? pin_->id : 0; }
}  // namespace folly
#endif
