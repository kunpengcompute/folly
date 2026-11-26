/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <cstdlib>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <iostream>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>
#include <folly/FileUtil.h>
#include <folly/experimental/EnvUtil.h>
#include <folly/experimental/TestUtil.h>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaInit.h>

// Standalone binary: allocation and SDK failures must not affect other tests.
namespace {
thread_local bool failNextAllocation = false;
enum class Failure { None, Devices, Query, Eids, Context, WrongDevice, WrongIndex, WrongAddress };
Failure failure = Failure::None;
bool borrowedLibrary = false;
int initializes = 0, finalizes = 0, devices = 0, eids = 0, contexts = 0;
uint32_t deviceSendSges = 4;
constexpr uint32_t kDeviceMaxReadSize = 1024 * 1024;
uint32_t deviceRxDepth = 4096, deviceTxDepth = 4096, deviceCqDepth = 8193;
urma_device_t device{}, otherDevice{};
urma_device_t* deviceList[]{&device, &otherDevice};
int deviceCount = 1;
urma_eid_info_t deviceEids[2]{};
urma_eid_info_t otherEid{};
uint32_t eidCount = 1;
std::optional<uint32_t> createdIndex;
urma_context_t context{};
bool failNextContextDelete = false;
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
  deviceCount = 1;
  eidCount = 1;
  for (auto& entry : deviceEids) entry = {};
  otherEid = {};
  createdIndex.reset();
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
  *count = failure == Failure::Devices ? 0 : deviceCount;
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
  attr->dev_cap.max_jfr_depth = deviceRxDepth;
  attr->dev_cap.max_jfs_depth = deviceTxDepth;
  attr->dev_cap.max_jfs_sge = deviceSendSges;
  attr->dev_cap.max_jfc_depth = deviceCqDepth;
  attr->dev_cap.max_msg_size = 65536;
  attr->dev_cap.max_read_size = kDeviceMaxReadSize;
  return URMA_SUCCESS;
}
urma_eid_info_t* urma_get_eid_list(urma_device_t* selected, uint32_t* count) {
  *count = selected == &otherDevice ? 1 : eidCount;
  if (failure == Failure::Eids) *count = 0;
  if (!*count) {
    return nullptr;
  }
  ++eids;
  return selected == &otherDevice ? &otherEid : deviceEids;
}
void urma_free_eid_list(urma_eid_info_t*) {
  --eids;
}
urma_context_t* urma_create_context(urma_device_t* selected, uint32_t index) {
  createdIndex = index;
  if (failure == Failure::Context) {
    return nullptr;
  }
  const auto* entries = selected == &otherDevice ? &otherEid : deviceEids;
  const auto* end = entries + (selected == &otherDevice ? 1 : eidCount);
  const auto* entry = std::find_if(entries, end, [index](const auto& e) {
    return e.eid_index == index;
  });
  if (entry == end) return nullptr;
  context.dev = failure == Failure::WrongDevice ? &otherDevice : selected;
  context.eid_index = failure == Failure::WrongIndex ? index + 1 : index;
  context.eid = entry->eid;
  if (failure == Failure::WrongAddress) context.eid.raw[0] ^= 0xff;
  ++contexts;
  return &context;
}
urma_status_t urma_delete_context(urma_context_t*) {
  if (failNextContextDelete) {
    failNextContextDelete = false;
    return URMA_EAGAIN;
  }
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
void expectInitError(const folly::UrmaConfig& config, const std::string& expected) {
  try {
    folly::UrmaInit::init(config);
  } catch (const std::runtime_error& error) {
    if (std::string(error.what()).find(expected) != std::string::npos) return;
    throw;
  }
  throw std::runtime_error("expected initialization failure: " + expected);
}

void checkDeviceBinding(const folly::UrmaConfig& base) {
  using folly::UrmaInit;
  auto cfg = base;
  reset();
  std::strcpy(otherDevice.name, "urma-second-device");
  deviceCount = 2;
  otherEid.eid_index = 42;
  otherEid.eid.raw[0] = 0x2a;
  cfg.dev_name = "missing-device";
  expectInitError(cfg, "URMA device missing-device not found");
  require(!createdIndex && !devices && !eids && !contexts && finalizes == 1,
          "unknown device allocated resources or selected a fallback");

  cfg.eid_index = 42;
  cfg.dev_name = device.name;
  expectInitError(cfg, std::string(device.name) + ": eid_index=42 unavailable");
  cfg.dev_name = otherDevice.name;
  UrmaInit::init(cfg);
  const int starts = initializes;
  UrmaInit::init(cfg);
  UrmaInit::init({});
  cfg.dev_name = device.name;
  expectInitError(cfg, std::string("conflicts with active device ") + otherDevice.name);
  cfg.dev_name = otherDevice.name;
  cfg.eid_index = 0;
  expectInitError(cfg, "requested eid_index=0 conflicts with active eid_index=42");
  require(initializes == starts && contexts == 1 && UrmaInit::getConfig().dev_name == otherDevice.name &&
              UrmaInit::getConfig().eid_index == 42u && UrmaInit::getLocalEid().raw[0] == 0x2a,
          "a repeated init replaced the active binding or reacquired the SDK");
  UrmaInit::fini();

  reset();
  cfg = base;
  cfg.dev_name.clear();
  UrmaInit::init(cfg);
  require(UrmaInit::getConfig().dev_name == device.name, "automatic device selection was not recorded");
  UrmaInit::init(base);
  UrmaInit::fini();

  for (bool borrowed : {false, true}) {
    for (const auto& [stage, error] : {
             std::pair{Failure::WrongDevice, "SDK context reports urma-second-device"},
             std::pair{Failure::WrongIndex, "SDK context reports eid_index=1"},
             std::pair{Failure::WrongAddress, "different EID address"}}) {
      reset(stage, borrowed);
      expectInitError(base, error);
      require(!UrmaInit::isInitialized() && !contexts && !devices && !eids &&
                  finalizes == (borrowed ? 0 : 1), "rejected SDK binding leaked resources");
    }
  }
  reset(Failure::WrongDevice);
  failNextContextDelete = true;
  expectInitError(base, "SDK context reports urma-second-device");
  require(folly::urma_detail::waitForCleanups(std::chrono::seconds(2)) &&
              !UrmaInit::isInitialized() && !contexts && finalizes == 1,
          "rejected SDK binding was not cleaned up after a delete failure");
  std::cout << "PASS device discovery, per-device EIDs, active binding conflicts and SDK rollback\n";
}

void checkConfigFileReuse(const folly::UrmaConfig& base) {
  using folly::UrmaInit;
  reset();
  eidCount = 2;
  deviceEids[0].eid_index = 17;
  deviceEids[1].eid_index = 19;
  folly::test::TemporaryDirectory dir("urma-init");
  const auto path = dir.path() / "urma.json";
  folly::test::EnvVarSaver restoreEnv;
  require(::setenv("FOLLY_URMA_CONFIG", path.c_str(), 1) == 0,
          "could not set config path");
  require(folly::writeFile(std::string(R"({"eid_index":17})"), path.c_str()),
          "could not write config");

  auto caller = base;
  caller.eid_index = 3;
  UrmaInit::init(caller);
  require(UrmaInit::getConfig().eid_index == 17u,
          "first init did not read the file");
  expectInitError(caller, "requested eid_index=3 conflicts with active eid_index=17");

  require(folly::fs::remove(path), "could not remove config");
  UrmaInit::init({});
  caller.eid_index = 17;
  UrmaInit::init(caller);
  require(initializes == 1 && contexts == 1, "reuse reacquired the SDK");
  UrmaInit::fini();

  require(folly::writeFile(std::string(R"({"eid_index":19})"), path.c_str()),
          "could not replace config");
  UrmaInit::init(base);
  require(initializes == 2 && UrmaInit::getConfig().eid_index == 19u,
          "a new context did not read the replacement config");
  UrmaInit::fini();
  std::cout << "PASS config file is read once per context\n";
}

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

    auto resetSparseEids = [](uint32_t secondIndex) {
      reset();
      eidCount = 2;
      deviceEids[0].eid_index = 3;
      deviceEids[0].eid.raw[0] = 0xa3;
      deviceEids[1].eid_index = secondIndex;
      deviceEids[1].eid.raw[0] = 0xa7;
    };
    auto checkEid = [&](std::optional<uint32_t> requested, size_t entry) {
      config.eid_index = requested;
      folly::UrmaInit::init(config);
      const auto local = folly::UrmaInit::getLocalEid();
      require(createdIndex == deviceEids[entry].eid_index &&
                  folly::UrmaInit::getConfig().eid_index == createdIndex &&
                  !std::memcmp(local.raw, deviceEids[entry].eid.raw, sizeof(local.raw)),
              "context index and local EID must come from the selected entry");
      folly::UrmaInit::fini();
    };
    resetSparseEids(7);
    checkEid(std::nullopt, 0);
    resetSparseEids(7);
    checkEid(7, 1);
    resetSparseEids(7);
    config.eid_index = 0;
    bool missing = false;
    try {
      folly::UrmaInit::init(config);
    } catch (const std::runtime_error& e) {
      const std::string msg(e.what());
      missing = msg.find(config.dev_name) != std::string::npos &&
          msg.find("eid_index=0") != std::string::npos;
    }
    require(missing && !createdIndex && !eids && !devices && !contexts &&
                finalizes == 1 && !folly::UrmaInit::isInitialized(),
            "missing EID must fail before context creation and release SDK lists");
    resetSparseEids(0);
    checkEid(0, 1);
    config.eid_index.reset();
    std::cout << "PASS sparse EID selection, missing index rollback and explicit zero\n";

    checkDeviceBinding(config);
    checkConfigFileReuse(config);

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
    for (uint32_t maximum : {1, 4, 13, 255}) {
      deviceSendSges = maximum;
      for (uint32_t requested : {0, 3, 255}) {
        reset();
        config.max_send_sge = requested;
        folly::UrmaInit::init(config);
        require(folly::UrmaInit::getConfig().max_send_sge ==
                    (requested ? std::min(requested, maximum) : maximum),
                "SEND SGE limit did not follow device capability/config cap");
        folly::UrmaInit::fini();
      }
    }
    config.max_send_sge = 0;
    for (uint32_t maximum : {0, 256}) {
      reset();
      deviceSendSges = maximum;
      bool rejected = false;
      try { folly::UrmaInit::init(config); }
      catch (const std::invalid_argument&) { rejected = true; }
      require(rejected && !devices && !eids && !contexts && finalizes == 1,
              "unrepresentable device SGE limit leaked SDK resources");
    }
    deviceSendSges = 4;
    std::cout << "PASS device SEND SGE limits, explicit caps and rollback\n";
    deviceCqDepth = 2305;
    reset();
    folly::UrmaInit::init(config);
    require(folly::UrmaInit::getConfig().rx_depth == 1280,
            "automatic RX ignored shared CQ capacity");
    folly::UrmaInit::fini();
    for (int stage = 0; stage < 3; ++stage) {
      auto requested = config;
      if (stage == 0) requested.rx_depth = 2000;
      if (stage == 1) deviceRxDepth = 128;
      if (stage == 2) deviceTxDepth = 512;
      reset();
      bool rejected = false;
      try { folly::UrmaInit::init(requested); }
      catch (const std::invalid_argument& e) {
        const char* field = stage == 0 ? "rx_depth" : stage == 1 ? "rx_target" : "tx_depth";
        rejected = std::string(e.what()).find(field) != std::string::npos;
      }
      require(rejected && !devices && !contexts && finalizes == 1,
              "device limit rejection was silent or leaked SDK resources");
      deviceRxDepth = deviceTxDepth = 4096;
    }
    deviceCqDepth = 8193;
    std::cout << "PASS automatic CQ sizing, explicit device limits and rollback\n";
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
    // The same producer thread must stop using a retired pool after reinit.
    for (int round = 0; round < 3; ++round) {
      folly::UrmaInit::init(config);
      auto buffer = folly::IOBuf::create(64);
      require(folly::UrmaInit::ownsPoolBuffer(*buffer) &&
                  bool(folly::UrmaInit::lookupSegment(buffer->buffer(), buffer->capacity())),
              "reinitialized producer used a stale or unregistered pool");
      auto block = folly::UrmaInit::poolAlloc(4096);
      require(block && block.segment() == folly::UrmaInit::poolSegment(*buffer) &&
                  bool(folly::UrmaInit::holdReadExport(block.data(), block.capacity())),
              "reinitialized socket allocation/export used a stale pool");
      block = {};
      buffer.reset();
      folly::UrmaInit::fini();
      require(!folly::UrmaInit::isInitialized(), "reinitialized TLS retained registration");
    }
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
    for (bool lookup : {false, true}) {
      threw = false;
      const auto before = folly::UrmaInit::poolStats().allocated_bytes;
      failNextAllocation = true;
      try {
        if (lookup) (void)folly::UrmaInit::holdReadExport(first.memory, 1);
        else (void)folly::UrmaInit::allocateBuffer(1);
      } catch (const std::bad_alloc&) { threw = true; }
      failNextAllocation = false;
      require(threw && folly::UrmaInit::poolStats().allocated_bytes == before &&
                  !folly::UrmaInit::poolStats().exported_bytes,
              "metadata failure was retried as capacity pressure or leaked a pin");
    }
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
