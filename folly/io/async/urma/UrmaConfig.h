/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <cstdint>
#include <string>
namespace folly {
// One dedicated RM jetty/JFR per connection; SEND/RECV plus large-write READ.
struct UrmaConfig {
  std::string dev_name;
  uint32_t rx_depth{32};
  uint32_t tx_depth{8};
  uint32_t buffer_size{4096}; // Clamped to the CTP SEND limit of 4 KiB.
  uint32_t handshake_timeout_ms{5000};
  // READ selection is independent of SEND block capacity. A record is eligible
  // only when its serialized length is strictly greater than this threshold.
  bool enable_read{true};
  // Default policy is calibrated around the SEND/READ latency crossover.
  // Do not derive it from buffer_size or the SEND headroom reservation.
  uint32_t read_threshold{4352};
  // Bound queued data without reserving this amount up front. Covers ten
  // outstanding 8 MiB RPCs, including framing, for existing benchmark callers.
  uint32_t max_pending_bytes{128 * 1024 * 1024};
};
} // namespace folly
#endif
