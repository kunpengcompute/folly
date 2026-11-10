/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#include <folly/io/async/urma/UrmaIoContext.h>

#include <algorithm>
#include <deque>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace folly::urma_detail {
// Exercise the actual owner entry and cleanup, without creating hardware queues.
class UrmaIoContextTestPeer {
 public:
  static std::shared_ptr<UrmaIoContext> create(EventBase& evb) {
    auto owner = std::shared_ptr<UrmaIoContext>(
        new UrmaIoContext(nullptr, UrmaConfig{}));
    owner->evb_ = &evb;
    owner->attachEventBase(&evb);
    owner->detached_ = false;
    return owner;
  }
  static void poll(UrmaIoContext& owner) { owner.poll(owner.io_); }
  static void loop(UrmaIoContext& owner) {
    owner.prepared_ = owner.preparedOK_ = true;
    owner.runLoopCallback();
  }
  static bool cleanup(UrmaIoContext& owner) { return owner.cleanup(owner.io_); }
};
}  // namespace folly::urma_detail

using namespace folly;
using namespace folly::urma_detail;
namespace {
urma_jfc_t cq{};
urma_jfce_t event{};
struct Device {
  std::deque<uint64_t> ready;
  std::vector<uint64_t> delivered;
  std::string calls;
  unsigned pendingEvents{0}, retrieved{0}, acknowledged{0}, pollCalls{0};
  unsigned failPoll{0}, injectBeforeRearm{0}, injectAfterRearm{0}, deleted{0};
  unsigned rearmCalls{0}, failRearm{0}, injectAtRearm{1};
  bool armed{false}, failWait{false}, badOrder{false}, cleanup{false};
  bool closeOnFirst{false};
  UrmaIoContext* owner{nullptr};
  std::shared_ptr<UrmaRoute> route;
  void post(uint64_t id) {
    auto& g = owner->io();
    auto work = std::make_unique<TxCompletionCtx>();
    work->route = route;
    work->sequence = id;
    ready.push_back(g.flights.size());
    g.flights.push_back(std::move(work));
    ++g.inflight;
    if (armed) {
      ++pendingEvents;
      armed = false;
    }
  }
} device;
void check(bool ok, const char* message) {
  if (!ok) throw std::runtime_error(message);
}
struct Route : UrmaRoute {
  std::shared_ptr<RxBudget> receiveBudget() override { return {}; }
  void onRecord(const RecordHeader&, std::unique_ptr<IOBuf>, uint64_t) override {}
  void onTx(TxCompletionCtx& work, bool ok) override {
    check(ok, "unexpected failed completion");
    device.delivered.push_back(work.sequence);
    if (device.closeOnFirst && device.delivered.size() == 1)
      device.owner->stopOwner();
  }
  void progress() noexcept override {}
  void failed(const char*) noexcept override {}
};
struct Fixture {
  EventBase evb;
  std::shared_ptr<UrmaIoContext> owner;
  Fixture() : owner(UrmaIoContextTestPeer::create(evb)) {
    device = Device{};
    device.owner = owner.get();
    device.route = std::make_shared<Route>();
    owner->io().event = &event;
    owner->io().cq = &cq;
  }
  ~Fixture() { owner->stopOwner(); }
  struct Result {
    bool error, again;
  };
  Result cycle() {
    owner->cancelLoopCallback();
    bool error = false;
    try {
      UrmaIoContextTestPeer::poll(*owner);
    } catch (const std::exception&) {
      error = true;
    }
    check(!device.badOrder, "invalid event order/count");
    return {error, owner->isLoopCallbackScheduled()};
  }
  bool drain() {
    device.cleanup = true;
    return UrmaIoContextTestPeer::cleanup(*owner);
  }
};
void normalAndIdle() {
  Fixture f;
  device.pendingEvents = 2;  // two notifications are not two CQEs
  for (int i = 0; i < 7; ++i) device.post(i);
  auto r = f.cycle();
  check(!r.error && !r.again && device.calls == "WAPRPR", "normal cycle");
  check(device.acknowledged == 2 && device.delivered.size() == 7 &&
            !f.owner->io().inflight,
        "event and completion accounting");
  device.calls.clear();
  r = f.cycle();
  check(!r.error && !r.again && device.calls == "WPRPR", "idle must not spin/ack");
  // A notification can remain after an earlier speculative poll removed its CQE.
  device.pendingEvents = 1;
  device.calls.clear();
  r = f.cycle();
  check(!r.error && !r.again && device.calls == "WAPRPR", "stale event");
  device.calls.clear();
  device.post(8);
  UrmaIoContextTestPeer::loop(*f.owner);
  check(device.calls == "WAPRPR" && device.delivered.back() == 8 &&
            !f.owner->isLoopCallbackScheduled(),
        "software continuation must use the same bounded event path");
}
void rearmRaces() {
  for (unsigned arm : {1, 2}) {
    for (bool before : {true, false}) {
      Fixture f;
      device.injectAtRearm = arm;
      if (before)
        device.injectBeforeRearm = 41;
      else
        device.injectAfterRearm = 42;
      const auto r = f.cycle();
      check(!r.error && !r.again, "singleton race unexpectedly spun the loop");
      if (arm == 1)
        check(device.delivered.size() == 1, "post-rearm poll missed completion");
      else
        check(device.delivered.empty() && device.pendingEvents,
              "last rearm arrival has neither a CQE delivery nor an event");
      if (device.pendingEvents) check(!f.cycle().error, "pending event failed");
      check(device.delivered.size() == 1, "rearm race lost/duplicated completion");
      check(device.retrieved == device.acknowledged, "race left an unacked event");
      // After the final empty poll, a new CQE must trigger the armed notification.
      device.post(43);
      check(device.pendingEvents == 1, "sleep boundary was left unarmed");
      check(!f.cycle().error && device.delivered.size() == 2,
            "post-sleep arrival not delivered");
    }
  }
}
void batches() {
  Fixture f;
  device.pendingEvents = 19;  // more than one wait batch; repeated same handle
  for (int i = 0; i < 97; ++i) device.post(i);
  unsigned turns = 0;
  while (true) {
    const auto delivered = device.delivered.size();
    auto r = f.cycle();
    check(!r.error && ++turns < 10, "bounded continuation failed");
    check(device.delivered.size() - delivered <= 64, "unbounded active poll");
    check(device.delivered.size() - delivered < 32 || r.again,
          "full CQ batch did not arrange software continuation");
    if (!r.again && !device.pendingEvents) break;
  }
  check(device.delivered.size() == 97 && device.acknowledged == 19 &&
            !f.owner->io().inflight,
        "truncated CQ/event batch");
  for (uint64_t i = 0; i < 97; ++i)
    check(device.delivered[i] == i, "CQE duplicated/reordered");
}
void errorsAndRetry() {
  {
    Fixture f;
    device.failWait = true;
    check(f.cycle().error && device.calls == "W", "wait error ignored");
    device.failWait = false;
    device.pendingEvents = 3;
    device.failPoll = 1;
    check(f.cycle().error && device.acknowledged == 3 && device.calls == "WWAP",
          "active notifications must be acked before polling");
    const unsigned retrieved = device.retrieved;
    device.calls.clear();
    check(f.drain(), "cleanup retry did not converge");
    check(device.calls == "WP" && device.retrieved == retrieved &&
              device.acknowledged == 3 && device.deleted == 2,
          "failed poll lost/re-read/duplicated event ownership");
  }
  {
    Fixture f;
    device.pendingEvents = 1;
    device.failRearm = 1;
    check(f.cycle().error && device.calls == "WAPR" &&
              device.acknowledged == 1,
          "rearm failure accounting");
    check(f.drain(), "drain must not rearm");
  }
  {
    Fixture f;
    device.pendingEvents = 1;
    device.failPoll = 2;
    check(f.cycle().error && device.calls == "WAPRP" &&
              device.acknowledged == 1,
          "second poll error ignored");
    check(f.drain(), "second poll cleanup");
  }
  {
    Fixture f;
    device.pendingEvents = 1;
    device.failRearm = 2;
    check(f.cycle().error && device.calls == "WAPRPR" &&
              device.acknowledged == 1,
          "second rearm failure ignored");
    check(f.drain(), "second rearm cleanup");
  }
  {
    Fixture f;
    device.pendingEvents = 1;
    device.closeOnFirst = true;
    device.post(1);
    device.post(2);
    check(!f.cycle().error && device.calls == "WAP" &&
              device.delivered == std::vector<uint64_t>({1, 2}) &&
              !f.owner->io().inflight,
          "callback close lost a removed CQE or rearmed stopped queues");
    check(f.drain(), "callback close cleanup");
  }
}
void teardown() {
  {
    Fixture f;
    device.pendingEvents = 9;
    for (int i = 0; i < 70; ++i) device.post(i);
    check(f.drain(), "teardown did not drain");
    check(device.acknowledged == 9 && device.ready.empty() &&
              device.calls.find('R') == std::string::npos &&
              !device.badOrder && device.deleted == 2,
          "teardown order/accounting/rearm");
  }
  {
    Fixture f;
    device.failWait = true;
    check(!f.drain(), "teardown wait failure ignored");
    device.failWait = false;
    device.pendingEvents = 1;
    device.failPoll = 1;
    check(!f.drain() && !device.acknowledged && !device.deleted,
          "teardown poll failure discarded event");
    check(f.drain() && device.acknowledged == 1 && device.deleted == 2,
          "teardown retry must ack once");
  }
  {
    Fixture f;
    // Deep CQs yield to the existing cleanup queue instead of monopolizing it.
    for (int i = 0; i < 1500; ++i) device.post(i);
    check(!f.drain() && !device.ready.empty() && !device.deleted,
          "unbounded cleanup");
    check(f.drain() && device.ready.empty() && device.deleted == 2,
          "bounded drain lost work");
  }
}
}  // namespace

// Interpose only the SDK boundary. The owner, dispatch and cleanup above are
// production code, driven by a deterministic one-shot interrupt device.
extern "C" int urma_wait_jfc(urma_jfce_t*, uint32_t n, int timeout,
                              urma_jfc_t* out[]) {
  device.calls += 'W';
  device.badOrder |= timeout != 0 || device.retrieved != device.acknowledged;
  if (device.failWait) return -1;
  n = std::min(n, device.pendingEvents);
  for (unsigned i = 0; i < n; ++i) out[i] = &cq;
  device.pendingEvents -= n;
  device.retrieved += n;
  return n;
}
extern "C" int urma_poll_jfc(urma_jfc_t*, int n, urma_cr_t* out) {
  device.calls += 'P';
  if (++device.pollCalls == device.failPoll) return -1;
  n = std::min<size_t>(n, device.ready.size());
  for (int i = 0; i < n; ++i) {
    out[i] = {};
    out[i].status = URMA_CR_SUCCESS;
    out[i].user_ctx = device.ready.front();
    device.ready.pop_front();
  }
  return n;
}
extern "C" void urma_ack_jfc(urma_jfc_t* queues[], uint32_t counts[],
                              uint32_t n) {
  device.badOrder |= device.calls.empty() ||
      device.calls.back() != (device.cleanup ? 'P' : 'W');
  device.calls += 'A';
  for (unsigned i = 0; i < n; ++i) {
    device.badOrder |= queues[i] != &cq || counts[i] != 1;
    device.acknowledged += counts[i];
  }
  device.badOrder |= device.acknowledged > device.retrieved;
}
extern "C" urma_status_t urma_rearm_jfc(urma_jfc_t*, bool solicited) {
  device.calls += 'R';
  device.badOrder |= solicited || device.retrieved != device.acknowledged;
  if (++device.rearmCalls == device.failRearm) return URMA_FAIL;
  if (device.injectBeforeRearm && device.rearmCalls == device.injectAtRearm) {
    device.post(device.injectBeforeRearm);
    device.injectBeforeRearm = 0;
  }
  device.armed = true;
  if (device.injectAfterRearm && device.rearmCalls == device.injectAtRearm) {
    device.post(device.injectAfterRearm);
    device.injectAfterRearm = 0;
  }
  return URMA_SUCCESS;
}
extern "C" urma_status_t urma_delete_jfc(urma_jfc_t*) {
  device.badOrder |= device.retrieved != device.acknowledged ||
      !device.ready.empty();
  ++device.deleted;
  return URMA_SUCCESS;
}
extern "C" urma_status_t urma_delete_jfce(urma_jfce_t*) {
  ++device.deleted;
  return URMA_SUCCESS;
}
int main() {
  try {
    normalAndIdle();
    rearmRaces();
    batches();
    errorsAndRetry();
    teardown();
    std::cout << "PASS owner interrupt order, rearm races, bounded batches, "
                 "exact ACK counts, SDK failures and teardown retries\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << '\n';
    return 1;
  }
}
