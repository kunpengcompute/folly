/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaInit.h>
#ifdef FOLLY_HAVE_URMA
#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <folly/logging/xlog.h>
namespace folly {
namespace {
std::mutex mutex;
urma_context_t* context = nullptr;
urma_eid_t localEid{};
UrmaConfig settings;
size_t owners = 0;
bool libraryOwned = false;
} // namespace
void UrmaInit::init(const UrmaConfig& config) {
  std::lock_guard<std::mutex> lock(mutex);
  if (context)
    return;
  if (!config.rx_depth || !config.tx_depth || config.rx_depth > 4096 ||
      config.tx_depth > 4096 || config.buffer_size < 64 ||
      config.buffer_size > 65536 || !config.handshake_timeout_ms ||
      config.max_pending_bytes < config.buffer_size) {
    throw std::invalid_argument("invalid basic URMA configuration");
  }
  urma_init_attr_t init{};
  auto status = urma_init(&init);
  if (status != URMA_SUCCESS && status != URMA_EEXIST)
    throw std::runtime_error("urma_init failed");
  libraryOwned = status == URMA_SUCCESS;
  int count = 0;
  auto** devices = urma_get_device_list(&count);
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
  if (device && urma_query_device(device, &attr) == URMA_SUCCESS &&
      (attr.dev_cap.trans_mode & URMA_TM_RM)) {
    settings = config;
    settings.rx_depth = std::min(settings.rx_depth, attr.dev_cap.max_jfr_depth);
    settings.tx_depth = std::min(settings.tx_depth, attr.dev_cap.max_jfs_depth);
    // Aggregated CTP SEND supports 4 KiB even when max_msg_size also advertises
    // the larger RTP limit. Larger application writes are split by the socket.
    settings.buffer_size = std::min<uint64_t>(
        settings.buffer_size, std::min<uint64_t>(4096, attr.dev_cap.max_msg_size));
    // Check limits before creating a context so rejection needs no teardown.
    if (settings.rx_depth && settings.tx_depth && settings.buffer_size >= 64 &&
        uint64_t(settings.rx_depth) + settings.tx_depth + 1 <=
            attr.dev_cap.max_jfc_depth) {
      eids = urma_get_eid_list(device, &n);
      if (eids && n) {
        localEid = eids[0].eid;
        context = urma_create_context(device, eids[0].eid_index);
      }
    }
  }
  if (eids)
    urma_free_eid_list(eids);
  if (devices)
    urma_free_device_list(devices);
  if (!context) {
    if (libraryOwned)
      urma_uninit();
    libraryOwned = false;
    throw std::runtime_error("no usable URMA RM context/device");
  }
}
void UrmaInit::fini() {
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
const UrmaConfig& UrmaInit::getConfig() {
  return settings;
}
urma_eid_t UrmaInit::getLocalEid() {
  return localEid;
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
