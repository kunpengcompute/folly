/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaInit.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/Indestructible.h>
#include <folly/ScopeGuard.h>
#include <folly/logging/xlog.h>
#include <folly/memory/IoBufPool.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <utility>
namespace folly {
namespace {
std::mutex mutex;
urma_context_t* context = nullptr;
urma_eid_t localEid{};
UrmaConfig settings;
size_t owners = 0;
uint32_t maxReadSize = 0;
bool libraryOwned = false;
std::atomic<bool> poolIOBufEnabled{false};
// Keep unresolved registrations alive even during static destruction.
folly::Indestructible<std::shared_ptr<urma_detail::MemoryPool>> memoryPool;
folly::Indestructible<std::shared_ptr<urma_detail::MemoryPool>> publishedPool;
std::atomic<uint64_t> poolEpoch{1};
const std::shared_ptr<urma_detail::MemoryPool>& ioBufPool() {
  static thread_local uint64_t epoch = 0;
  static thread_local std::shared_ptr<urma_detail::MemoryPool> pool;
  const auto current = poolEpoch.load(std::memory_order_acquire);
  if (epoch != current) {
    pool = std::atomic_load(&*publishedPool);
    epoch = current;
  }
  return pool;
}
bool shutdownRequested = false;
struct PoolContextCleanup final : urma_detail::CleanupTask {
  bool tryCleanup() noexcept override {
    std::unique_lock<std::mutex> lock(mutex, std::try_to_lock);
    if (!lock) return failed("wait-pool-context-lock", 0);
    if (owners) return failed("wait-pool-context-owners", 0);
    std::atomic_store(&*publishedPool,
                      std::shared_ptr<urma_detail::MemoryPool>{});
    poolEpoch.fetch_add(1, std::memory_order_release);
    poolIOBufEnabled.store(false, std::memory_order_release);
    if (*memoryPool) {
      if (!(*memoryPool)->shutdown(std::chrono::milliseconds(0)))
        return failed("wait-pool-references-or-deregistration", 0);
      memoryPool->reset();
    }
    if (context) {
      const int status = urma_delete_context(context);
      if (status != URMA_SUCCESS) return failed("delete-pool-context", status);
      context = nullptr;
    }
    if (libraryOwned) {
      const int status = urma_uninit();
      if (status != URMA_SUCCESS) return failed("uninit-library", status);
      libraryOwned = false;
    }
    shutdownRequested = false;
    return true;
  }
};
// Reserve the cleanup node at initialization. Transferring it during shutdown
// must not allocate, and the queue owns it until the last upper-layer lease
// has gone and all registrations/context have been successfully destroyed.
std::unique_ptr<PoolContextCleanup> poolCleanup;
}  // namespace
void UrmaInit::init(const UrmaConfig& requested) {
  std::lock_guard<std::mutex> lock(mutex);
  if (shutdownRequested)
    throw std::runtime_error("URMA shutdown is still pending");
  if (context) {
    if (!requested.dev_name.empty() && requested.dev_name != settings.dev_name)
      throw std::runtime_error("requested URMA device " + requested.dev_name +
          " conflicts with active device " + settings.dev_name);
    if (requested.eid_index && *requested.eid_index != context->eid_index)
      throw std::runtime_error("URMA device " + settings.dev_name +
          ": requested eid_index=" + std::to_string(*requested.eid_index) +
          " conflicts with active eid_index=" + std::to_string(context->eid_index));
    return;
  }
  const char* path = std::getenv("FOLLY_URMA_CONFIG");
  const UrmaConfig config =
      path ? loadUrmaConfigFile(path, requested) : requested;
  validateUrmaConfig(config);
  // Copy the allocating string before acquiring SDK resources. Publishing the
  // effective configuration below is a non-throwing move.
  UrmaConfig effective = config;
  auto preparedCleanup = std::make_unique<PoolContextCleanup>();
  static_assert(std::is_nothrow_move_assignable<UrmaConfig>::value,
                "URMA config publication must not allocate");
  urma_init_attr_t init{};
  auto status = urma_init(&init);
  if (status != URMA_SUCCESS && status != URMA_EEXIST)
    throw std::runtime_error("urma_init failed");
  const bool initializedHere = status == URMA_SUCCESS;
  urma_context_t* created = nullptr;
  auto rollback = makeGuard([&] {
    const bool pendingContext = created && urma_delete_context(created) != URMA_SUCCESS;
    if (pendingContext || (initializedHere && urma_uninit() != URMA_SUCCESS)) {
      // Keep the SDK alive until a rejected context can be deleted.
      context = pendingContext ? created : nullptr;
      libraryOwned = initializedHere;
      shutdownRequested = true;
      urma_detail::cleanupOrDefer(std::move(preparedCleanup));
    }
  });
  int count = 0;
  auto** devices = urma_get_device_list(&count);
  auto releaseDevices = makeGuard([&] {
    if (devices) urma_free_device_list(devices);
  });
  urma_device_t* device = nullptr;
  for (int i = 0; devices && i < count; ++i) {
    if (devices[i] && (config.dev_name.empty() || config.dev_name == devices[i]->name)) {
      device = devices[i];
      break;
    }
  }
  if (!device) {
    if (config.dev_name.empty()) throw std::runtime_error("no URMA devices found");
    throw std::runtime_error("URMA device " + config.dev_name + " not found");
  }
  effective.dev_name = device->name;
  urma_device_attr_t attr{};
  uint32_t n = 0;
  urma_eid_info_t* eids = nullptr;
  auto releaseEids = makeGuard([&] {
    if (eids) urma_free_eid_list(eids);
  });
  uint32_t readCapacity = 0;
  urma_eid_t eid{};
  if (urma_query_device(device, &attr) == URMA_SUCCESS &&
      (attr.dev_cap.trans_mode & URMA_TM_RM)) {
    auto deviceLimit = [](const char* field, uint32_t requested, uint32_t maximum) {
      if (!maximum || requested > maximum)
        throw std::invalid_argument(std::string(field) + "=" +
            std::to_string(requested) + " exceeds device capacity " + std::to_string(maximum));
    };
    deviceLimit("tx_depth", config.tx_depth, attr.dev_cap.max_jfs_depth);
    if (attr.dev_cap.max_jfc_depth <= config.tx_depth + 1)
      throw std::invalid_argument("tx_depth leaves no JFC capacity for RX");
    const auto rxLimit = std::min<uint32_t>(65536,
        std::min(attr.dev_cap.max_jfr_depth, attr.dev_cap.max_jfc_depth - config.tx_depth - 1));
    if (config.rx_depth) deviceLimit("rx_depth", config.rx_depth, rxLimit);
    effective.rx_depth = config.rx_depth ? config.rx_depth : rxLimit;
    deviceLimit("rx_target", config.rx_target, effective.rx_depth);
    effective.max_send_sge = effective.max_send_sge
        ? std::min(effective.max_send_sge, attr.dev_cap.max_jfs_sge)
        : attr.dev_cap.max_jfs_sge;
    if (!effective.max_send_sge ||
        effective.max_send_sge > std::numeric_limits<uint8_t>::max())
      throw std::invalid_argument("URMA device SEND SGE capacity is not representable");
    // Aggregated CTP SEND supports 4 KiB even when max_msg_size also advertises
    // the larger RTP limit. Larger application writes use one-sided READ.
    effective.buffer_size =
        std::min<uint64_t>(effective.buffer_size,
                           std::min<uint64_t>(4096, attr.dev_cap.max_msg_size));
    // Bound each READ by the largest reusable class and the device. The pool
    // can still allocate larger continuous IOBufs; transport slices them.
    readCapacity = std::min<uint64_t>(
        attr.dev_cap.max_read_size, config.registered_pool.classes.back().bytes);
    // Check limits before creating a context so rejection needs no teardown.
    if (effective.rx_depth && effective.rx_target <= effective.rx_depth && effective.tx_depth &&
        effective.buffer_size >= 32 &&
        uint64_t(effective.rx_depth) + effective.tx_depth + 1 <=
            attr.dev_cap.max_jfc_depth) {
      eids = urma_get_eid_list(device, &n);
      uint32_t i = 0;
      if (config.eid_index) {
        while (eids && i < n && eids[i].eid_index != *config.eid_index) ++i;
        if (!eids || i == n) {
          throw std::runtime_error(std::string("URMA device ") + device->name +
              ": eid_index=" + std::to_string(*config.eid_index) + " unavailable");
        }
      }
      if (eids && i < n) {
        const auto& selected = eids[i];
        eid = selected.eid;
        effective.eid_index = selected.eid_index;
        created = urma_create_context(device, selected.eid_index);
      }
    }
  }
  if (!created) {
    throw std::runtime_error("no usable URMA RM context for device " + effective.dev_name);
  }
  if (!created->dev || effective.dev_name != created->dev->name) {
    throw std::runtime_error("URMA requested device " + effective.dev_name +
        " but SDK context reports " + (created->dev ? created->dev->name : "<none>"));
  }
  if (created->eid_index != *effective.eid_index) {
    throw std::runtime_error("URMA device " + effective.dev_name + ": selected eid_index=" +
        std::to_string(*effective.eid_index) + " but SDK context reports eid_index=" +
        std::to_string(created->eid_index));
  }
  // Some SDK-created virtual devices leave the context EID address unset.
  const urma_eid_t unsetEid{};
  if (std::memcmp(created->eid.raw, unsetEid.raw, sizeof(eid.raw)) != 0 &&
      std::memcmp(created->eid.raw, eid.raw, sizeof(eid.raw)) != 0) {
    throw std::runtime_error("URMA device " + effective.dev_name + ": eid_index=" +
        std::to_string(*effective.eid_index) + " has a different EID address in the SDK context");
  }
  settings = std::move(effective);
  maxReadSize = std::min(attr.dev_cap.max_read_size, readCapacity);
  localEid = eid;
  libraryOwned = initializedHere;
  context = created;
  poolCleanup = std::move(preparedCleanup);
  // From here, even an initialization failure uses the preallocated retry node.
  rollback.dismiss();
  try {
    *memoryPool = std::make_shared<urma_detail::MemoryPool>(
        context, settings.registered_pool);
    const auto& pool = settings.registered_pool;
    const size_t hard = pool.pool_max_size ? pool.pool_max_size : pool.pool_size;
    maxReadSize = std::min<size_t>(maxReadSize,
        std::max(pool.pool_size, pool.elastic ? hard - pool.pool_size : size_t(0)));
    if (!(*memoryPool)->bufferSize(maxReadSize)) {
      const auto limit = maxReadSize;
      maxReadSize = 0;
      for (const auto& tier : settings.registered_pool.classes)
        if (tier.bytes <= limit && (*memoryPool)->bufferSize(tier.bytes))
          maxReadSize = tier.bytes;
    }
    if (!(*memoryPool)->warm(std::chrono::seconds(5)))
      throw std::runtime_error("URMA soft capacity unavailable at startup");
    XLOG(INFO) << "URMA context device=" << device->name
               << " eid_index=" << created->eid_index;
    std::atomic_store(&*publishedPool, *memoryPool);
    poolEpoch.fetch_add(1, std::memory_order_release);
    poolIOBufEnabled.store(
        settings.registered_pool.enabled && settings.registered_pool.zero_copy,
        std::memory_order_release);
  } catch (...) {
    shutdownRequested = true;
    urma_detail::cleanupOrDefer(std::move(poolCleanup));
    throw;
  }
}
void UrmaInit::fini() {
  std::unique_ptr<PoolContextCleanup> task;
  {
    std::lock_guard<std::mutex> lock(mutex);
    if (!context && !shutdownRequested) return;
    if (!shutdownRequested) {
      shutdownRequested = true;
      task = std::move(poolCleanup);
    }
  }
  // Reject new transports immediately. Existing owners can finish; the retry
  // node stops the pool and deletes the context after their last release.
  // Never wait under mutex: transport cleanup calls releaseContext().
  urma_detail::cleanupOrDefer(std::move(task));
  (void)urma_detail::waitForCleanups(std::chrono::milliseconds(250));
}
bool UrmaInit::isInitialized() {
  std::lock_guard<std::mutex> lock(mutex);
  return context || shutdownRequested;
}
size_t UrmaInit::pendingCleanupCount() {
  return urma_detail::pendingCleanups();
}
const UrmaConfig& UrmaInit::getConfig() { return settings; }
urma_eid_t UrmaInit::getLocalEid() { return localEid; }
uint32_t UrmaInit::getMaxReadSize() { return maxReadSize; }
urma_context_t* UrmaInit::acquireContext() {
  std::lock_guard<std::mutex> lock(mutex);
  if (context && !shutdownRequested) ++owners;
  return shutdownRequested ? nullptr : context;
}
void UrmaInit::releaseContext() {
  std::lock_guard<std::mutex> lock(mutex);
  --owners;
}
UrmaPoolBuffer UrmaInit::poolAlloc(size_t size,
                                  UrmaPoolSignal* waiter) {
  const auto& pool = ioBufPool();
  if (!pool) return {};
  auto buffer = pool->allocate(size, waiter);
  if (!buffer) {
    IoBufPool::reclaim(pool->identity(), false);
    buffer = pool->allocate(size, waiter);
  }
  return buffer;
}
std::unique_ptr<IOBuf> UrmaInit::takeBuffer(Buffer buffer, size_t length) {
  if (!buffer.hold) throw std::invalid_argument("empty registered buffer lease");
  return takePoolBuffer(std::move(buffer.hold), length);
}
UrmaInit::Buffer UrmaInit::allocateBuffer(
    size_t bytes, UrmaPoolSignal* waiter) {
  auto block = poolAlloc(bytes, waiter);
  if (!block) return {};
  return {block.data(), block.segment(), block.capacity(), std::move(block)};
}
UrmaPoolBuffer UrmaInit::initialSegment() {
  auto pool = std::atomic_load(&*publishedPool);
  return pool ? pool->initialSegment() : UrmaPoolBuffer{};
}
UrmaPoolBuffer UrmaInit::holdSegment(uint64_t id) {
  auto pool = std::atomic_load(&*publishedPool);
  return pool ? pool->holdSegment(id) : UrmaPoolBuffer{};
}
std::vector<uint64_t> UrmaInit::revocableSegments() {
  auto pool = std::atomic_load(&*publishedPool);
  return pool ? pool->revocableSegments() : std::vector<uint64_t>{};
}
size_t UrmaInit::poolBufSize(size_t requested) {
  auto pool = std::atomic_load(&*publishedPool);
  return pool ? pool->bufferSize(requested) : 0;
}
void UrmaInit::subscribePool(const std::shared_ptr<UrmaPoolSignal>& signal) {
  auto pool = std::atomic_load(&*publishedPool);
  if (!pool) throw std::runtime_error("URMA pool unavailable");
  pool->subscribe(signal);
}
UrmaPoolBuffer UrmaInit::lookupSegment(const void* addr, size_t length) {
  const auto& pool = ioBufPool();
  return pool ? (pool)->lookup(addr, length, false) : UrmaPoolBuffer{};
}
UrmaPoolBuffer UrmaInit::holdReadExport(const void* addr, size_t length) {
  const auto& pool = ioBufPool();
  return pool ? (pool)->lookup(addr, length, true) : UrmaPoolBuffer{};
}
void UrmaInit::retirePoolBuffer(const UrmaPoolBuffer& buffer) {
  std::lock_guard<std::mutex> lock(mutex);
  auto pool = *memoryPool;
  if (pool) (pool)->retire(buffer);
}
void UrmaInit::trimPool() {
  std::lock_guard<std::mutex> lock(mutex);
  auto pool = *memoryPool;
  if (pool) (pool)->trim();
}
UrmaPoolStats UrmaInit::poolStats() {
  std::lock_guard<std::mutex> lock(mutex);
  auto pool = *memoryPool;
  return pool ? (pool)->stats() : UrmaPoolStats{};
}
std::vector<UrmaPoolRegionInfo> UrmaInit::poolRegions() {
  std::lock_guard<std::mutex> lock(mutex);
  auto pool = *memoryPool;
  return pool ? (pool)->regions() : std::vector<UrmaPoolRegionInfo>{};
}
std::unique_ptr<IOBuf> UrmaInit::createPoolBuffer(size_t size) {
  auto allocation = poolAlloc(size);
  if (!allocation) return nullptr;
  // Like IOBuf::create(), capacity is uninitialized and length starts at zero.
  // Only the caller's appended range can be planned as SEND/READ input.
  auto owner = std::make_unique<UrmaPoolBuffer>(std::move(allocation));
  auto* data = owner->data();
  auto capacity = owner->capacity();
  // takeOwnership takes the free callback on failure too (freeOnError=true).
  auto* cookie = owner.release();
  return IOBuf::takeOwnership(data, capacity, 0, UrmaInit::freePoolBuffer, cookie);
}
bool UrmaInit::poolIOBufAllocationEnabled() {
  return poolIOBufEnabled.load(std::memory_order_acquire);
}
void UrmaInit::freePoolBuffer(void*, void* p) {
  delete static_cast<UrmaPoolBuffer*>(p);
}
std::unique_ptr<IOBuf> UrmaInit::createPoolSlice(
    size_t size, UrmaPoolSignal* waiter) {
  const auto& pool = ioBufPool();
  return pool ? IoBufPool::createFromPoolShared(
                    pool, std::max(size, size_t(1)), waiter) : nullptr;
}
std::unique_ptr<IOBuf> UrmaInit::tryCreateIOBuf(size_t size) {
  if (!poolIOBufAllocationEnabled()) return nullptr;
  static thread_local bool active = false;
  if (active) throw std::bad_alloc();
  active = true;
  auto reset = makeGuard([&] { active = false; });
  // The synchronous caller owns retry policy. Never block completion/reclaim
  // on an EventBase or disguise a pool miss as successful heap allocation.
  const auto& pool = ioBufPool();
  auto result = pool ? IoBufPool::createFromPoolShared(pool, std::max(size, size_t(1))) : nullptr;
  if (!result) throw std::bad_alloc();
  return result;
}
bool UrmaInit::ownsPoolBuffer(const IOBuf& buffer) {
  return (buffer.isManagedOne() && buffer.getFreeFn() == UrmaInit::freePoolBuffer) ||
         IoBufPool::owns(buffer);
}
urma_target_seg_t* UrmaInit::poolSegment(const IOBuf& buffer) {
  if (buffer.isManagedOne() && buffer.getFreeFn() == UrmaInit::freePoolBuffer)
    return static_cast<UrmaPoolBuffer*>(buffer.getUserData())->segment();
  return IoBufPool::segment(buffer);
}
std::unique_ptr<IOBuf> UrmaInit::takePoolBuffer(UrmaPoolBuffer buffer,
                                                size_t length) {
  if (!buffer || length > buffer.capacity())
    throw std::invalid_argument("invalid pool IOBuf length");
  auto owner = std::make_unique<UrmaPoolBuffer>(std::move(buffer));
  auto* data = owner->data();
  const auto capacity = owner->capacity();
  return IOBuf::takeOwnership(data, capacity, length, UrmaInit::freePoolBuffer,
                              owner.release());
}
}  // namespace folly
#endif
