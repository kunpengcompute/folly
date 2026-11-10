/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/io/async/urma/UrmaMemoryPool.h>

#include <cstdint>
#include <string>
namespace folly {
// Process-wide pool settings and per-connection transport limits.
struct UrmaConfig {
  std::string dev_name;
  uint32_t rx_depth{32};
  uint32_t tx_depth{8};
  uint32_t buffer_size{4096};  // Clamped to the CTP SEND limit of 4 KiB.
  uint32_t max_send_sge{4};
  // Local SQ space for incoming READ and its completion SEND; no peer credit.
  uint32_t io_free_sq_reserve{1};
  uint32_t max_connections{40000};
  uint32_t handshake_timeout_ms{5000};
  // A continuous registered range above this threshold may use READ.
  bool enable_read{true};
  // Default policy is calibrated around the SEND/READ latency crossover.
  // Do not derive it from buffer_size or the SEND headroom reservation.
  uint32_t read_threshold{4352};
  // H24 transport permits one whole READ per connection.
  uint32_t read_window{1};
  // Maximum unacknowledged READ descriptions on one connection.
  uint32_t offer_window{1};
  // Bound queued data without reserving this amount up front. Covers ten
  // outstanding 8 MiB RPCs, including framing, for existing benchmark callers.
  uint32_t max_pending_bytes{128 * 1024 * 1024};
  uint32_t max_pending_records{1024};
  uint64_t max_retained_bytes{256 * 1024 * 1024};
  UrmaPoolConfig registered_pool;
};
// Load a strict JSON overlay; absent fields retain base values. Throws on
// errors.
UrmaConfig loadUrmaConfigFile(const std::string& path,
                              const UrmaConfig& base = {});
}  // namespace folly
#endif
