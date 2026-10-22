/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <new>
#include <stdexcept>
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

void require(bool value, const char* message) {
  if (!value) {
    throw std::runtime_error(message);
  }
}
void reset(Failure stage = Failure::None, bool borrowed = false) {
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
}

int main() {
  try {
    folly::UrmaConfig config;
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
      folly::UrmaInit::fini();
      require(
          !folly::UrmaInit::isInitialized() && contexts == 0,
          "successful init did not finalize");
      require(finalizes == (borrowed ? 0 : 1), "borrowed library finalized");
      std::cout << "PASS successful retry and finalization borrowed="
                << borrowed << '\n';
    }
    std::cout << "11 initialization cases passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
