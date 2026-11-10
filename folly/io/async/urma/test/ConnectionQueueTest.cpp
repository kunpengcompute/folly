/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaIoContext.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
using namespace folly;
using namespace folly::urma_detail;
struct Route : UrmaRoute {
  std::shared_ptr<RxBudget> account{std::make_shared<RxBudget>(1024 * 1024)};
  std::unique_ptr<IOBuf> received;
  size_t tx{0};
  bool error{false};
  bool stopOnTx{false}, throwOnTx{false};
  std::shared_ptr<RxBudget> receiveBudget() override { return account; }
  void onRecord(const RecordHeader&, std::unique_ptr<IOBuf> b,
                uint64_t) override {
    received = std::move(b);
  }
  void onTx(TxCompletionCtx&, bool ok) override {
    ++tx;
    error |= !ok;
    if (stopOnTx) ioOwner->stopOwner();
    if (throwOnTx) throw std::runtime_error("injected completion callback failure");
  }
  void progress() noexcept override {}
  void failed(const char* what) noexcept override {
    std::cerr << what << '\n';
    error = true;
  }
};
std::unique_ptr<TxCompletionCtx> sendWork(const std::shared_ptr<Route>& route,
                                          urma_target_jetty_t* peer) {
  auto work = std::make_unique<TxCompletionCtx>();
  work->route = route;
  work->control = UrmaInit::poolAlloc(32, UrmaMemoryPurpose::Tx);
  if (!work->control) throw std::runtime_error("control allocation");
  auto* bytes = static_cast<uint8_t*>(work->control.data());
  encodeHeader(bytes, {Message::Data, route->socketId, 1});
  std::memset(bytes + 24, 0x5a, 8);
  work->localPin = UrmaInit::lookupSegment(bytes, 32);
  work->sges[0].addr = reinterpret_cast<uint64_t>(bytes);
  work->sges[0].len = 32;
  work->sges[0].tseg = work->control.segment();
  work->wr.opcode = URMA_OPC_SEND;
  work->wr.tjetty = peer;
  work->wr.send.src.sge = work->sges.data();
  work->wr.send.src.num_sge = 1;
  return work;
}
int main(int argc, char** argv) {
  const char* device = std::getenv("FBTHRIFT_URMA_TEST_DEVICE");
  if (!device) {
    std::cout << "SKIP: FBTHRIFT_URMA_TEST_DEVICE required\n";
    return 77;
  }
  try {
    UrmaConfig c;
    c.dev_name = device;
    c.registered_pool.enabled = true;
    c.rx_depth = 8;
    c.enable_read = false;
    c.registered_pool.pool_size = 128 * 1024;
    c.registered_pool.pool_max_size = 0;
    c.registered_pool.budgets.fill(c.registered_pool.pool_size);
    c.registered_pool.tls_block_size = 4096;
    c.registered_pool.classes = {{4096, {}}};
    UrmaIoContext::validateCapacity(c);
    UrmaInit::init(c);
    auto* sdk = UrmaInit::acquireContext();
    EventBase evb;
    auto owner = UrmaIoContext::get(&evb);
    auto route = std::make_shared<Route>();
    owner->add(route);
    const auto preparedUntil = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!owner->ready() && std::chrono::steady_clock::now() < preparedUntil) {
      evb.loopOnce(EVLOOP_NONBLOCK);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!owner->ready()) throw std::runtime_error("queue preparation timed out");
    auto second = UrmaIoContext::get(&evb);
    while (!second->ready() && std::chrono::steady_clock::now() < preparedUntil) {
      evb.loopOnce(EVLOOP_NONBLOCK);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!second->ready() || owner->io().tx == second->io().tx ||
        owner->io().rx == second->io().rx || owner->io().cq == second->io().cq ||
        owner->endpoint().id == second->endpoint().id)
      throw std::runtime_error("same EventBase reused connection queues");
    second->stopOwner();
    second.reset();
    urma_rjetty_t remote{};
    remote.jetty_id = owner->endpoint();
    remote.type = URMA_JETTY;
    remote.trans_mode = URMA_TM_RM;
    remote.tp_type = URMA_CTP;
    remote.flag.bs.order_type = URMA_OI;
    urma_token_t token{};
    auto* peer = urma_import_jetty(sdk, &remote, &token);
    if (!peer) throw std::runtime_error("connection endpoint import failed");
    if (argc == 2) {
      route->stopOnTx = std::string(argv[1]) == "stop";
      route->throwOnTx = std::string(argv[1]) == "throw";
      if (!route->stopOnTx && !route->throwOnTx)
        throw std::runtime_error("unknown connection queue test mode");
      for (int i = 0; i < 2; ++i) {
        auto work = sendWork(route, peer);
        if (owner->post(work) != PostResult::Accepted)
          throw std::runtime_error("callback test post rejected");
      }
      // Let both completions arrive before dispatch so the test exercises a
      // callback stopping/throwing partway through one removed CQE batch.
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      evb.loopOnce(EVLOOP_NONBLOCK);
      if (route->tx != 2 || owner->io().inflight)
        throw std::runtime_error("callback interrupted the removed CQE batch");
      owner->remove(route->socketId, route.get());
      owner->stopOwner();
      route->received.reset();
      if (urma_unimport_jetty(peer) != URMA_SUCCESS)
        throw std::runtime_error("callback test teardown failed");
      UrmaInit::releaseContext();
      UrmaInit::fini();
      std::cout << "PASS callback " << argv[1]
                << ": all removed CQEs settled, owner detached, CQ deleted\n";
      return 0;
    }
    std::vector<UrmaPoolBuffer> pressure;
    while (auto b = UrmaInit::poolAlloc(4096, UrmaMemoryPurpose::Rx))
      pressure.push_back(std::move(b));
    auto work = sendWork(route, peer);
    if (owner->post(work) != PostResult::Accepted)
      throw std::runtime_error("independent JFS post rejected");
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((!route->received || !route->tx) && !route->error &&
           std::chrono::steady_clock::now() < until) {
      evb.loopOnce(EVLOOP_NONBLOCK);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (route->error || !route->received || route->tx != 1 ||
        route->received->length() != 8 || route->received->data()[7] != 0x5a ||
        owner->io().posted != c.rx_depth - 1 || !owner->io().refillPending ||
        !owner->io().replacementFailures)
      throw std::runtime_error(
          "missing replacement failed to preserve EMPTY/refill_pending and "
          "original DATA");
    auto late = route->received->cloneOne();
    route->received.reset();
    std::thread restore([b = std::move(pressure.back())]() mutable { b = {}; });
    pressure.pop_back();
    restore.join();
    const auto refillUntil =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (owner->io().posted != c.rx_depth &&
           std::chrono::steady_clock::now() < refillUntil)
      evb.loopOnce(EVLOOP_NONBLOCK);
    if (owner->io().posted != c.rx_depth || late->data()[7] != 0x5a)
      throw std::runtime_error(
          "cross-thread return did not refill with a different safe block");
    pressure.clear();
    for (int i = 0; i < 8; ++i) {
      evb.loopOnce(EVLOOP_NONBLOCK);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (owner->io().posted != c.rx_depth ||
        owner->io().target != c.rx_depth || owner->io().depth != c.rx_depth ||
        owner->io().refillPending || !UrmaInit::poolStats().free_bytes)
      throw std::runtime_error(
          "free memory increased the RX target or left unnecessary refill");
    owner->remove(route->socketId, route.get());
    owner->stopOwner();
    if (urma_unimport_jetty(peer) != URMA_SUCCESS)
      throw std::runtime_error("connection queue teardown failed");
    if (late->data()[7] != 0x5a)
      throw std::runtime_error("RX overwritten after owner shutdown");
    std::thread release([b = std::move(late)]() mutable { b.reset(); });
    release.join();
    UrmaInit::releaseContext();
    UrmaInit::fini();
    std::cout << "PASS real connection SEND/CQE, no replacement -> EMPTY, retained "
                 "clone, cross-thread refill, late owner-free release\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << '\n';
    std::_Exit(1);
  }
}
