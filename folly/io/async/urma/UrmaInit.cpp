/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaInit.h>
#ifdef FOLLY_HAVE_URMA
#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <folly/ScopeGuard.h>
#include <folly/logging/xlog.h>
namespace folly {
namespace {
std::mutex mutex;
urma_context_t* context = nullptr;
urma_eid_t localEid{};
UrmaConfig settings;
size_t owners = 0;
uint32_t maxReadSize = 0;
bool libraryOwned = false;
} // namespace
void UrmaInit::init(const UrmaConfig& config) {
  std::lock_guard<std::mutex> lock(mutex);
  if (context)
    return;
  if (!config.rx_depth || !config.tx_depth || config.rx_depth > 4096 ||
      config.tx_depth > 4096 || config.buffer_size <= 128 ||
      config.buffer_size > 65536 || !config.handshake_timeout_ms ||
      config.max_pending_bytes < config.buffer_size) {
    throw std::invalid_argument("invalid basic URMA configuration");
  }
  // Copy the allocating string before acquiring SDK resources. Publishing the
  // effective configuration below is a non-throwing move.
  UrmaConfig effective = config;
  static_assert(
      std::is_nothrow_move_assignable<UrmaConfig>::value,
      "URMA config publication must not allocate");
  urma_init_attr_t init{};
  auto status = urma_init(&init);
  if (status != URMA_SUCCESS && status != URMA_EEXIST)
    throw std::runtime_error("urma_init failed");
  const bool initializedHere = status == URMA_SUCCESS;
  auto rollback = makeGuard([&] {
    if (initializedHere)
      urma_uninit();
  });
  int count = 0;
  auto** devices = urma_get_device_list(&count);
  auto releaseDevices = makeGuard([&] {
    if (devices)
      urma_free_device_list(devices);
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
    if (eids)
      urma_free_eid_list(eids);
  });
  urma_context_t* created = nullptr;
  urma_eid_t eid{};
  if (device && urma_query_device(device, &attr) == URMA_SUCCESS &&
      (attr.dev_cap.trans_mode & URMA_TM_RM)) {
    effective.rx_depth =
        std::min(effective.rx_depth, attr.dev_cap.max_jfr_depth);
    effective.tx_depth =
        std::min(effective.tx_depth, attr.dev_cap.max_jfs_depth);
    // Aggregated CTP SEND supports 4 KiB even when max_msg_size also advertises
    // the larger RTP limit. Larger application writes use one-sided READ.
    effective.buffer_size = std::min<uint64_t>(
        effective.buffer_size,
        std::min<uint64_t>(4096, attr.dev_cap.max_msg_size));
    // Check limits before creating a context so rejection needs no teardown.
    if (effective.rx_depth && effective.tx_depth &&
        effective.buffer_size > 128 &&
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
  maxReadSize = attr.dev_cap.max_read_size;
  localEid = eid;
  libraryOwned = initializedHere;
  context = created;
  rollback.dismiss();
}
void UrmaInit::fini() {
  // A cleanup task releases its context owner on another thread. Do not hold
  // the context mutex while waiting, or the final release would deadlock.
  if (!urma_detail::waitForCleanups(std::chrono::milliseconds(250))) {
    XLOG(ERR) << "URMA fini deferred: resource cleanups still pending="
              << pendingCleanupCount();
    return;
  }
  std::lock_guard<std::mutex> lock(mutex);
  if (!context)
    return;
  if (owners) {
    XLOG(ERR) << "URMA fini requires all transports to be destroyed";
    return;
  }
  if (urma_delete_context(context) != URMA_SUCCESS) {
    XLOG(ERR) << "URMA context could not be destroyed";
    return;
  }
  context = nullptr;
  if (libraryOwned)
    urma_uninit();
  libraryOwned = false;
}
bool UrmaInit::isInitialized() {
  std::lock_guard<std::mutex> lock(mutex);
  return context;
}
size_t UrmaInit::pendingCleanupCount() {
  return urma_detail::pendingCleanups();
}
const UrmaConfig& UrmaInit::getConfig() {
  return settings;
}
urma_eid_t UrmaInit::getLocalEid() {
  return localEid;
}
uint32_t UrmaInit::getMaxReadSize() {
  return maxReadSize;
}
urma_context_t* UrmaInit::acquireContext() {
  std::lock_guard<std::mutex> lock(mutex);
  if (context)
    ++owners;
  return context;
}
void UrmaInit::releaseContext() {
  std::lock_guard<std::mutex> lock(mutex);
  --owners;
}
} // namespace folly
#endif
