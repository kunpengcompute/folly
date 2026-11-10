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
void boolean(const dynamic& obj, const char* key, bool& out) {
  if (auto* v = obj.get_ptr(key)) {
    if (!v->isBool())
      throw std::invalid_argument(std::string(key) + " must be boolean");
    out = v->getBool();
  }
}
void purposes(const dynamic& obj,
              std::array<size_t, kUrmaMemoryPurposes>& out) {
  keys(obj,
       {"rx", "tx", "control", "read_source", "read_target", "read_control"});
  const char* names[] = {"rx",          "tx",          "control",
                         "read_source", "read_target", "read_control"};
  for (size_t i = 0; i < kUrmaMemoryPurposes; ++i)
    number(obj, names[i], out[i]);
}
}  // namespace
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
    keys(root, {"version",
                "dev_name",
                "io_free_sq_reserve",
                "max_connections",
                "max_send_sge",
                "rx_depth",
                "tx_depth",
                "buffer_size",
                "handshake_timeout_ms",
                "enable_read",
                "read_threshold",
                "read_window",
                "offer_window",
                "max_pending_bytes",
                "max_pending_records",
                "max_retained_bytes",
                "registered_pool"});
    uint32_t version = 3;
    number(root, "version", version);
    if (version != 3) throw std::invalid_argument("unsupported config version");
    auto c = base;
    if (auto* name = root.get_ptr("dev_name")) {
      if (!name->isString() ||
          name->getString().find('\0') != std::string::npos)
        throw std::invalid_argument("dev_name must be a string without NUL");
      c.dev_name = name->asString();
    }
    number(root, "io_free_sq_reserve", c.io_free_sq_reserve);
    number(root, "max_connections", c.max_connections);
    number(root, "max_send_sge", c.max_send_sge);
    number(root, "rx_depth", c.rx_depth);
    number(root, "tx_depth", c.tx_depth);
    number(root, "buffer_size", c.buffer_size);
    number(root, "handshake_timeout_ms", c.handshake_timeout_ms);
    number(root, "read_threshold", c.read_threshold);
    number(root, "read_window", c.read_window);
    number(root, "offer_window", c.offer_window);
    number(root, "max_pending_bytes", c.max_pending_bytes);
    number(root, "max_pending_records", c.max_pending_records);
    number(root, "max_retained_bytes", c.max_retained_bytes);
    boolean(root, "enable_read", c.enable_read);
    if (auto* pool = root.get_ptr("registered_pool")) {
      keys(*pool,
           {"enabled", "zero_copy", "region_bytes", "pool_size",
            "pool_max_size", "elastic", "tls_block_size", "tls_cache_blocks",
            "budgets", "classes", "idle_ms", "retry_ms"});
      auto& p = c.registered_pool;
      boolean(*pool, "enabled", p.enabled);
      boolean(*pool, "zero_copy", p.zero_copy);
      number(*pool, "region_bytes", p.region_bytes);
      number(*pool, "pool_size", p.pool_size);
      number(*pool, "pool_max_size", p.pool_max_size);
      number(*pool, "tls_block_size", p.tls_block_size);
      number(*pool, "tls_cache_blocks", p.tls_cache_blocks);
      boolean(*pool, "elastic", p.elastic);
      number(*pool, "idle_ms", p.idle_ms);
      number(*pool, "retry_ms", p.retry_ms);
      if (auto* budgets = pool->get_ptr("budgets"))
        purposes(*budgets, p.budgets);
      if (auto* classes = pool->get_ptr("classes")) {
        if (!classes->isArray() || classes->size() > 64)
          throw std::invalid_argument(
              "classes must be an array of at most 64 entries");
        p.classes.clear();
        for (const auto& entry : *classes) {
          keys(entry, {"bytes", "reserve"});
          UrmaPoolClass tier{};
          number(entry, "bytes", tier.bytes);
          if (auto* reserve = entry.get_ptr("reserve"))
            purposes(*reserve, tier.reserve);
          p.classes.push_back(tier);
        }
      }
      // Validate even disabled pool settings so typos cannot become latent
      // errors.
      auto checked = p;
      checked.enabled = true;
      urma_detail::MemoryPool::validate(checked);
      if (p.zero_copy && !p.enabled)
        throw std::invalid_argument("zero_copy requires enabled pool");
    }
    if (c.read_window != 1)
      throw std::invalid_argument(
          "H24 uses one outstanding READ per connection");
    if (!c.offer_window || c.offer_window > 8 ||
        (c.offer_window > 1 && !c.registered_pool.enabled))
      throw std::invalid_argument(
          "offer window must be 1..8; >1 requires pool");
    if (!c.rx_depth || c.rx_depth > 4096 || !c.tx_depth || c.tx_depth > 4096 ||
        c.buffer_size < 32 || c.buffer_size > 65536 ||
        !c.handshake_timeout_ms || c.max_pending_bytes < c.buffer_size)
      throw std::invalid_argument("invalid basic URMA configuration");

    return c;
  } catch (const std::exception& e) {
    throw std::invalid_argument("URMA config " + path + ": " + e.what());
  }
}
}  // namespace folly
#endif
