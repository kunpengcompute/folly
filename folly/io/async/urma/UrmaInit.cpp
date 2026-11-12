/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaResourceExecutor.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/Indestructible.h>
#include <folly/ScopeGuard.h>
#include <folly/io/async/urma/UrmaIoContext.h>
#include <folly/logging/xlog.h>
#include <folly/memory/IoBufPool.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
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
void freePoolIOBuf(void*, void* p) { delete static_cast<UrmaPoolBuffer*>(p); }
// Keep unresolved registrations alive even during static destruction.
folly::Indestructible<std::shared_ptr<urma_detail::MemoryPool>> memoryPool;
folly::Indestructible<std::shared_ptr<urma_detail::MemoryPool>> publishedPool;
bool shutdownRequested = false;
struct PoolContextCleanup final : urma_detail::CleanupTask {
  bool tryCleanup() noexcept override {
    std::unique_lock<std::mutex> lock(mutex, std::try_to_lock);
    if (!lock) return failed("wait-pool-context-lock", 0);
    if (owners) return failed("wait-pool-context-owners", 0);
    std::atomic_store(&*publishedPool,
                      std::shared_ptr<urma_detail::MemoryPool>{});
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
  // Pay thread startup at explicit initialization, not on the first RPC.
  // Keep executor observers outside the context mutex. This acquires no SDK
  // resources and can throw without publishing a partially initialized context.
  urma_detail::warmResourceExecutor();
  std::lock_guard<std::mutex> lock(mutex);
  if (shutdownRequested)
    throw std::runtime_error("URMA shutdown is still pending");
  if (context) return;
  const char* path = std::getenv("FOLLY_URMA_CONFIG");
  const UrmaConfig config =
      path ? loadUrmaConfigFile(path, requested) : requested;
  urma_detail::MemoryPool::validate(config.registered_pool);
  if (!config.offer_window || config.offer_window > 8 ||
      (config.offer_window > 1 && !config.registered_pool.enabled))
    throw std::invalid_argument("offer window must be 1..8; >1 requires pool");
  if (!config.registered_pool.enabled || config.read_window != 1)
    throw std::invalid_argument(
        "H24 transport requires a registered pool and one READ per connection");
  if (!config.rx_depth || !config.tx_depth || config.rx_depth > 4096 ||
      config.tx_depth > 4096 || config.buffer_size < 32 ||
      config.buffer_size > 65536 || !config.handshake_timeout_ms ||
      config.max_pending_bytes < config.buffer_size) {
    throw std::invalid_argument("invalid basic URMA configuration");
  }
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
  auto rollback = makeGuard([&] {
    if (initializedHere && urma_uninit() != URMA_SUCCESS) {
      // Device discovery/context creation can fail before context publication.
      // Retain SDK ownership if even this early rollback cannot uninitialize.
      libraryOwned = true;
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
    if (config.dev_name.empty() || config.dev_name == devices[i]->name) {
      device = devices[i];
      break;
    }
  }
  urma_device_attr_t attr{};
  uint32_t n = 0;
  urma_eid_info_t* eids = nullptr;
  auto releaseEids = makeGuard([&] {
    if (eids) urma_free_eid_list(eids);
  });
  urma_context_t* created = nullptr;
  uint32_t readCapacity = 0;
  urma_eid_t eid{};
  if (device && urma_query_device(device, &attr) == URMA_SUCCESS &&
      (attr.dev_cap.trans_mode & URMA_TM_RM)) {
    effective.rx_depth =
        std::min(effective.rx_depth, attr.dev_cap.max_jfr_depth);
    effective.tx_depth =
        std::min(effective.tx_depth, attr.dev_cap.max_jfs_depth);
    effective.max_send_sge = std::min<uint32_t>(
        4,
        std::min<uint32_t>(effective.max_send_sge, attr.dev_cap.max_jfs_sge));
    if (!effective.max_send_sge)
      throw std::invalid_argument("URMA device has no SEND SGE capacity");
    // Aggregated CTP SEND supports 4 KiB even when max_msg_size also advertises
    // the larger RTP limit. Larger application writes use one-sided READ.
    effective.buffer_size =
        std::min<uint64_t>(effective.buffer_size,
                           std::min<uint64_t>(4096, attr.dev_cap.max_msg_size));
    readCapacity = urma_detail::UrmaIoContext::validateCapacity(effective);
    // Check limits before creating a context so rejection needs no teardown.
    if (effective.rx_depth && effective.tx_depth &&
        effective.buffer_size >= 32 &&
        uint64_t(effective.rx_depth) + effective.tx_depth + 1 <=
            attr.dev_cap.max_jfc_depth) {
      eids = urma_get_eid_list(device, &n);
      if (eids && n) {
        eid = eids[0].eid;
        created = urma_create_context(device, eids[0].eid_index);
      }
    }
  }
  if (!created) {
    throw std::runtime_error("no usable URMA RM context/device");
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
    if (!(*memoryPool)->warm(std::chrono::seconds(5)))
      throw std::runtime_error("URMA soft capacity unavailable at startup");
    std::atomic_store(&*publishedPool, *memoryPool);
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
UrmaPoolBuffer UrmaInit::poolAlloc(size_t size, UrmaMemoryPurpose purpose,
                                  UrmaPoolSignal* waiter) {
  auto pool = std::atomic_load(&*publishedPool);
  if (!pool) return {};
  auto buffer = pool->allocate(size, purpose);
  if (!buffer) {
    IoBufPool::reclaim(pool->identity(), false);
    buffer = pool->allocate(size, purpose, waiter);
  }
  return buffer;
}
UrmaPoolBuffer UrmaInit::initialSegment() {
  auto pool = std::atomic_load(&*publishedPool);
  return pool ? pool->initialSegment() : UrmaPoolBuffer{};
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
  auto pool = std::atomic_load(&*publishedPool);
  return pool ? (pool)->lookup(addr, length, false) : UrmaPoolBuffer{};
}
UrmaPoolBuffer UrmaInit::holdReadExport(const void* addr, size_t length) {
  auto pool = std::atomic_load(&*publishedPool);
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
std::unique_ptr<IOBuf> UrmaInit::createPoolBuffer(size_t size,
                                                  UrmaMemoryPurpose purpose) {
  auto allocation = poolAlloc(size, purpose);
  if (!allocation) return nullptr;
  // Like IOBuf::create(), capacity is uninitialized and length starts at zero.
  // Only the caller's appended range can be planned as SEND/READ input.
  auto owner = std::make_unique<UrmaPoolBuffer>(std::move(allocation));
  auto* data = owner->data();
  auto capacity = owner->capacity();
  // takeOwnership takes the free callback on failure too (freeOnError=true).
  auto* cookie = owner.release();
  return IOBuf::takeOwnership(data, capacity, 0, freePoolIOBuf, cookie);
}
std::unique_ptr<IOBuf> UrmaInit::tryCreateIOBuf(size_t size) {
  if (!size || !poolIOBufEnabled.load(std::memory_order_acquire))
    return nullptr;
  // Logging/allocator observers may themselves allocate IOBufs.
  static thread_local bool active = false;
  if (active) return nullptr;
  active = true;
  auto reset = makeGuard([&] { active = false; });
  // Never block on init/fini or register memory on an application/IO thread.
  auto pool = std::atomic_load(&*publishedPool);
  if (!pool) return nullptr;
  return IoBufPool::createFromPoolShared(pool, size);
}
bool UrmaInit::ownsPoolBuffer(const IOBuf& buffer) {
  return (buffer.isManagedOne() && buffer.getFreeFn() == freePoolIOBuf) ||
         IoBufPool::owns(buffer);
}
std::unique_ptr<IOBuf> UrmaInit::takePoolBuffer(UrmaPoolBuffer buffer,
                                                size_t length) {
  if (!buffer || length > buffer.capacity())
    throw std::invalid_argument("invalid pool IOBuf length");
  auto owner = std::make_unique<UrmaPoolBuffer>(std::move(buffer));
  auto* data = owner->data();
  const auto capacity = owner->capacity();
  return IOBuf::takeOwnership(data, capacity, length, freePoolIOBuf,
                              owner.release());
}
}  // namespace folly
#endif
