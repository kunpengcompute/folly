/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <cstdlib>
#include <atomic>
#include <cstring>
#include <iostream>
#include <new>
#include <stdexcept>
#include <thread>
#include <vector>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaInit.h>

// Standalone binary: allocation and SDK failures must not affect other tests.
namespace {
thread_local bool failNextAllocation = false;
enum class Failure { None, Devices, Query, Eids, Context };
Failure failure = Failure::None;
bool borrowedLibrary = false;
int initializes = 0, finalizes = 0, devices = 0, eids = 0, contexts = 0;
urma_device_t device{};
urma_device_t* deviceList[]{&device};
urma_eid_info_t eid{};
urma_context_t context{};
#ifndef URMA_TEST_EXTERNAL_POOL
unsigned poolRegistrations = 0, poolUnregistrations = 0;
bool failPoolRegistration = false;
std::atomic<bool> failPoolUnregister{false};
#endif

void require(bool value, const char* message) {
  if (!value) {
    throw std::runtime_error(message);
  }
}
void reset(Failure stage = Failure::None, bool borrowed = false) {
  folly::urma_detail::waitForCleanups(std::chrono::seconds(2));
  require(!folly::UrmaInit::isInitialized(), "context leaked between tests");
  require(!devices && !eids && !contexts, "SDK resources leaked between tests");
  failure = stage;
  borrowedLibrary = borrowed;
  initializes = finalizes = 0;
}
} // namespace

void* operator new(std::size_t size) {
  if (failNextAllocation) {
    failNextAllocation = false;
    throw std::bad_alloc();
  }
  if (auto* p = std::malloc(size ? size : 1)) {
    return p;
  }
  throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
  return ::operator new(size);
}
void operator delete(void* p) noexcept {
  std::free(p);
}
void operator delete[](void* p) noexcept {
  std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
  std::free(p);
}

// These symbols replace the SDK only in this executable. No hardware is used.
extern "C" {
urma_status_t urma_init(urma_init_attr_t*) {
  ++initializes;
  return borrowedLibrary ? URMA_EEXIST : URMA_SUCCESS;
}
urma_status_t urma_uninit() {
  ++finalizes;
  return URMA_SUCCESS;
}
urma_device_t** urma_get_device_list(int* count) {
  *count = failure == Failure::Devices ? 0 : 1;
  if (!*count) {
    return nullptr;
  }
  ++devices;
  return deviceList;
}
void urma_free_device_list(urma_device_t**) {
  --devices;
}
urma_status_t urma_query_device(urma_device_t*, urma_device_attr_t* attr) {
  if (failure == Failure::Query) {
    return static_cast<urma_status_t>(-1);
  }
  *attr = {};
  attr->dev_cap.trans_mode = URMA_TM_RM;
  attr->dev_cap.max_jfr_depth = 4096;
  attr->dev_cap.max_jfs_depth = 4096;
  attr->dev_cap.max_jfs_sge = 4;
  attr->dev_cap.max_jfc_depth = 8193;
  attr->dev_cap.max_msg_size = 65536;
  attr->dev_cap.max_read_size = 1024 * 1024;
  return URMA_SUCCESS;
}
urma_eid_info_t* urma_get_eid_list(urma_device_t*, uint32_t* count) {
  *count = failure == Failure::Eids ? 0 : 1;
  if (!*count) {
    return nullptr;
  }
  ++eids;
  return &eid;
}
void urma_free_eid_list(urma_eid_info_t*) {
  --eids;
}
urma_context_t* urma_create_context(urma_device_t*, uint32_t) {
  if (failure == Failure::Context) {
    return nullptr;
  }
  ++contexts;
  return &context;
}
urma_status_t urma_delete_context(urma_context_t*) {
  --contexts;
  return URMA_SUCCESS;
}
#ifndef URMA_TEST_EXTERNAL_POOL
urma_target_seg_t* urma_register_seg(urma_context_t*, urma_seg_cfg_t* cfg) {
  if (failPoolRegistration)
    return nullptr;
  auto* segment = new urma_target_seg_t{};
  segment->seg.ubva.va = cfg->va;
  segment->seg.len = cfg->len;
  segment->seg.attr.bs.access = cfg->flag.bs.access;
  ++poolRegistrations;
  return segment;
}
urma_status_t urma_unregister_seg(urma_target_seg_t* segment) {
  if (failPoolUnregister)
    return URMA_EAGAIN;
  ++poolUnregistrations;
  delete segment;
  return URMA_SUCCESS;
}
#endif
}

#ifndef URMA_TEST_EXTERNAL_POOL
int main() {
  try {
    folly::UrmaConfig config;
    config.registered_pool.pool_size = 64 * 1024;
    config.registered_pool.elastic = false;

    config.registered_pool.tls_block_size = 4096;
    config.registered_pool.classes = {{4096}};
    // Exceed string's inline storage so copying configuration must allocate.
    config.dev_name = "urma-allocation-failure-test";
    std::strcpy(device.name, config.dev_name.c_str());
    reset();
    bool threw = false;
    failNextAllocation = true;
    try {
      folly::UrmaInit::init(config);
    } catch (const std::bad_alloc&) {
      threw = true;
    }
    failNextAllocation = false;
    require(threw, "configuration allocation failure was not injected");
    require(initializes == 0, "SDK acquired before allocating configuration");
    require(
        !folly::UrmaInit::isInitialized(), "failed init published a context");
    std::cout
        << "PASS configuration allocation failure precedes SDK acquisition\n";

    for (bool borrowed : {false, true}) {
      for (Failure stage :
           {Failure::Devices,
            Failure::Query,
            Failure::Eids,
            Failure::Context}) {
        reset(stage, borrowed);
        threw = false;
        try {
          folly::UrmaInit::init(config);
        } catch (const std::runtime_error&) {
          threw = true;
        }
        require(threw, "SDK failure did not reject initialization");
        require(
            !folly::UrmaInit::isInitialized(), "failed init published context");
        require(!devices && !eids && !contexts, "failed init leaked resources");
        require(
            initializes == 1 && finalizes == (borrowed ? 0 : 1),
            "incorrect library ownership after failed init");
        std::cout << "PASS SDK failure " << static_cast<int>(stage)
                  << " borrowed=" << borrowed << '\n';
      }
      reset(Failure::None, borrowed);
      folly::UrmaInit::init(config);
      require(folly::UrmaInit::isInitialized(), "retry did not initialize");
      require(
          !devices && !eids && contexts == 1, "successful init leaked lists");
      require(
          folly::UrmaInit::getConfig().dev_name == config.dev_name,
          "effective configuration not published");
      require(folly::UrmaInit::getMaxReadSize() == 4096,
              "READ limit exceeds the largest class");
      folly::UrmaInit::fini();
      require(
          !folly::UrmaInit::isInitialized() && contexts == 0,
          "successful init did not finalize");
      require(finalizes == (borrowed ? 0 : 1), "borrowed library finalized");
      std::cout << "PASS successful retry and finalization borrowed="
                << borrowed << '\n';
    }
    std::cout << "11 initialization cases passed\n";
    reset();
    folly::UrmaInit::init(config);
    {
      auto owned = [](const folly::IOBuf& b) {
        return folly::UrmaInit::ownsPoolBuffer(b) &&
            bool(folly::UrmaInit::lookupSegment(b.buffer(), b.capacity()));
      };
      auto a = folly::IOBuf::copyBuffer("abcd", 4);
      auto clone = a->cloneOne();
      clone->unshare();
      require(owned(*a) && owned(*clone), "unshare escaped the registered pool");
      clone->reserve(32, 8192);
      require(owned(*clone) && clone->headroom() >= 32 &&
                  !std::memcmp(clone->data(), "abcd", 4),
              "reserve lost registered storage or data");
      require(clone->capacity() > folly::UrmaInit::getMaxReadSize(),
              "READ limit incorrectly restricted continuous pool allocation");
      a->appendChain(std::move(clone));
      a->coalesce();
      require(owned(*a) && a->length() == 8, "coalesce escaped the pool");
      auto text = a->moveToFbString();
      require(text == "abcdabcd" && a->empty(), "registered-to-string ownership");
      auto combined = folly::IOBuf::createCombined(100);
      auto separate = folly::IOBuf::createSeparate(100);
      folly::IOBuf direct(folly::IOBuf::CREATE, 100);
      require(owned(*combined) && owned(*separate) && owned(direct),
              "alternate IOBuf allocation escaped the registered pool");
    }
    folly::UrmaInit::trimPool();
    {
      auto original = folly::IOBuf::copyBuffer("held", 4);
      auto clone = original->cloneOne();
      std::vector<folly::UrmaPoolBuffer> pressure;
      while (auto block = folly::UrmaInit::poolAlloc(4096))
        pressure.push_back(std::move(block));
      // Fill the remainder of the active TLS block too; no fresh registered
      // block can be acquired by any of these synchronous allocation paths.
      auto fails = [](auto operation) {
        bool failed = false;
        try { operation(); } catch (const std::bad_alloc&) { failed = true; }
        require(failed, "pool exhaustion silently returned heap storage");
      };
      fails([] { (void)folly::IOBuf::create(8192); });
      fails([] { (void)folly::IOBuf::createCombined(8192); });
      fails([] { (void)folly::IOBuf::createSeparate(8192); });
      fails([&] { clone->unshare(); });
      clone.reset();
      fails([&] { original->reserve(32, 8192); });
      require(original->length() == 4 && !std::memcmp(original->data(), "held", 4),
              "failed growth changed the original IOBuf");
      pressure.clear();
      original->reserve(32, 8192);
      require(folly::UrmaInit::ownsPoolBuffer(*original), "retry did not regain registered storage");
    }
    folly::UrmaInit::fini();
    require(!folly::UrmaInit::isInitialized(), "strict IOBuf test leaked pool");
    std::cout << "PASS registered create/reserve/unshare/coalesce, explicit exhaustion and retry\n";
    reset();
    failPoolRegistration = true;
    bool registrationRejected = false;
    try { folly::UrmaInit::init(config); }
    catch (const std::runtime_error&) { registrationRejected = true; }
    failPoolRegistration = false;
    folly::urma_detail::waitForCleanups(std::chrono::seconds(2));
    require(registrationRejected && !contexts && finalizes == 1 &&
                !folly::UrmaInit::isInitialized(), "pool init rollback");
    reset();
    folly::UrmaInit::init(config);
    const auto registered = poolRegistrations;
    auto first = folly::UrmaInit::allocateBuffer(1);
    std::vector<folly::UrmaInit::Buffer> rest;
    for (int i = 0; i < 15; ++i)
      rest.push_back(folly::UrmaInit::allocateBuffer(1));
    require(first.memory && rest.back().memory && first.size == 4096 &&
                first.segment == rest.back().segment &&
                !folly::UrmaInit::allocateBuffer(1).memory,
            "pool class, registration or exhaustion");
    require(poolRegistrations == registered, "runtime allocation registered");
    auto lease = first;
    auto owned = folly::UrmaInit::takeBuffer(std::move(lease), 1);
    auto clone = owned->clone();
    owned.reset();
    require(clone->data() == first.memory && clone->length() == 1 &&
                !folly::UrmaInit::allocateBuffer(1).memory,
            "buffer/IOBuf clone lost the shared pool lease");
    clone.reset();
    auto* address = first.memory;
    std::thread release([buffer = std::move(first)]() mutable { buffer = {}; });
    release.join();
    first = folly::UrmaInit::allocateBuffer(4096);
    require(first.memory == address, "cross-thread return was not reusable");
    failNextAllocation = true;
    try { folly::UrmaInit::allocateBuffer(4096); }
    catch (const std::bad_alloc&) {}
    failNextAllocation = false;
    rest.clear();
    folly::UrmaInit::fini();
    require(folly::UrmaInit::isInitialized(), "pool slice lost on fini");
    require(!folly::UrmaInit::allocateBuffer(4096).memory,
            "shutdown accepted a new pool allocation");
    failPoolUnregister = true;
    first = {};
    folly::UrmaInit::fini();
    require(folly::UrmaInit::isInitialized() &&
                folly::UrmaInit::pendingCleanupCount(),
            "failed unregister did not retain context and region");
    failPoolUnregister = false;
    for (int i = 0; i < 200 && folly::UrmaInit::isInitialized(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      folly::UrmaInit::fini();
    }
    require(!folly::UrmaInit::isInitialized() &&
                poolRegistrations == poolUnregistrations,
            "pool cleanup did not recover");
    std::cout << "PASS init-only registration, slices, exhaustion, cross-thread "
                 "reuse and deferred unregister\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
#endif
