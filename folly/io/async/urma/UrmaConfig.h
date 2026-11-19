/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/io/async/urma/UrmaMemoryPool.h>

#include <cstdint>
#include <optional>
#include <string>
namespace folly {
// Process-wide pool settings and per-connection transport limits.
struct UrmaConfig {
  std::string dev_name;
  uint32_t rx_depth{0};
  uint32_t rx_target{256};
  uint32_t tx_depth{1024};
  uint32_t buffer_size{4096};  // Clamped to the CTP SEND limit of 4 KiB.
  uint32_t max_send_sge{0};  // 0 uses the device limit; nonzero caps it.
  uint32_t handshake_timeout_ms{5000};
  // A continuous registered range above this threshold may use READ.
  bool enable_read{true};
  // Default policy is calibrated around the SEND/READ latency crossover.
  // Do not derive it from buffer_size or the SEND headroom reservation.
  uint32_t read_threshold{4352};
  // Local operation bounds; no peer receive-credit protocol.
  uint32_t read_window{1024};
  // Maximum unacknowledged READ descriptions on one connection.
  uint32_t offer_window{1024};
  // Per-connection admission/receive bounds, not an up-front reservation.
  uint32_t max_pending_bytes{128 * 1024 * 1024};
  UrmaPoolConfig registered_pool;
  // Local SDK index. If omitted, select the first EID reported by the device.
  std::optional<uint32_t> eid_index;
};
// Shared by JSON overlays and direct C++ initialization, before SDK calls.
void validateUrmaConfig(const UrmaConfig& config);
// Load a strict JSON overlay; absent fields retain base values. Throws on
// errors.
UrmaConfig loadUrmaConfigFile(const std::string& path,
                              const UrmaConfig& base = {});
}  // namespace folly
#endif
