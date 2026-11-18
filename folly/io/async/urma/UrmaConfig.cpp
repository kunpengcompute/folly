/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaConfig.h>
#ifdef FOLLY_HAVE_URMA
#include <folly/json.h>

#include <fstream>
#include <limits>
#include <stdexcept>
namespace folly {
namespace {
void keys(const dynamic& obj, std::initializer_list<const char*> allowed) {
  if (!obj.isObject()) throw std::invalid_argument("expected JSON object");
  for (const auto& entry : obj.items()) {
    bool known = false;
    for (auto key : allowed)
      if (entry.first == key) known = true;
    if (!known)
      throw std::invalid_argument("unknown URMA config key: " +
                                  entry.first.asString());
  }
}
template <class T>
void number(const dynamic& obj, const char* key, T& out) {
  if (auto* v = obj.get_ptr(key)) {
    if (!v->isInt() || v->getInt() < 0 ||
        uint64_t(v->getInt()) > std::numeric_limits<T>::max())
      throw std::invalid_argument(std::string(key) +
                                  " must be an unsigned integer in range");
    out = static_cast<T>(v->getInt());
  }
}
void range(const char* field, uint64_t value, uint64_t low, uint64_t high) {
  if (value < low || value > high)
    throw std::invalid_argument(std::string(field) + "=" + std::to_string(value) +
        " must be in [" + std::to_string(low) + ", " + std::to_string(high) + "]");
}
}  // namespace
void validateUrmaConfig(const UrmaConfig& c) {
  range("rx_depth", c.rx_depth, 0, 65536);
  range("rx_target", c.rx_target, 1, c.rx_depth ? c.rx_depth : 65536);
  range("tx_depth", c.tx_depth, 1, 4096);
  range("handshake_timeout_ms", c.handshake_timeout_ms, 1, 60000);
  range("buffer_size", c.buffer_size, 129, 4096);
  range("max_send_sge", c.max_send_sge, 0, UINT8_MAX);
  range("read_window", c.read_window, 1, 1024);
  range("offer_window", c.offer_window, 1, 1024);
  range("max_pending_bytes", c.max_pending_bytes, c.buffer_size, UINT32_MAX);
  if (c.dev_name.find('\0') != std::string::npos)
    throw std::invalid_argument("dev_name must not contain NUL");
  if (!c.registered_pool.enabled)
    throw std::invalid_argument("URMA requires a registered pool");
  urma_detail::MemoryPool::validate(c.registered_pool);
}
UrmaConfig loadUrmaConfigFile(const std::string& path, const UrmaConfig& base) {
  try {
    if (path.empty()) throw std::invalid_argument("empty path");
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::invalid_argument("cannot open file");
    // Bound input before parsing, including files whose reported size is
    // unknown.
    std::string text(65537, '\0');
    in.read(&text[0], text.size());
    const auto count = in.gcount();
    if (in.bad() || count > 65536)
      throw std::invalid_argument("unreadable file or exceeds 64 KiB");
    text.resize(static_cast<size_t>(count));
    json::serialization_opts opts;
    opts.validate_keys = true;
    opts.validate_utf8 = true;
    const auto root = parseJson(text, opts);
    keys(root, {"dev_name", "rx_depth", "rx_target", "tx_depth",
                "handshake_timeout_ms", "max_pending_bytes", "registered_pool"});
    auto c = base;
    if (auto* name = root.get_ptr("dev_name")) {
      if (!name->isString() ||
          name->getString().find('\0') != std::string::npos)
        throw std::invalid_argument("dev_name must be a string without NUL");
      c.dev_name = name->asString();
    }
    number(root, "rx_depth", c.rx_depth);
    number(root, "rx_target", c.rx_target);
    number(root, "tx_depth", c.tx_depth);
    number(root, "handshake_timeout_ms", c.handshake_timeout_ms);
    number(root, "max_pending_bytes", c.max_pending_bytes);
    if (auto* pool = root.get_ptr("registered_pool")) {
      keys(*pool, {"pool_size", "pool_max_size"});
      number(*pool, "pool_size", c.registered_pool.pool_size);
      number(*pool, "pool_max_size", c.registered_pool.pool_max_size);
    }
    validateUrmaConfig(c);

    return c;
  } catch (const std::exception& e) {
    throw std::invalid_argument("URMA config " + path + ": " + e.what());
  }
}
}  // namespace folly
#endif
