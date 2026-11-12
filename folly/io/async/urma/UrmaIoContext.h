/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
#pragma once
#ifdef FOLLY_HAVE_URMA
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaProtocol.h>
#include <folly/io/async/urma/UrmaRxStorage.h>
#include <chrono>

namespace folly::urma_detail {
class UrmaIoContext;
struct TxCompletionCtx;
struct UrmaRoute : std::enable_shared_from_this<UrmaRoute> {
  virtual ~UrmaRoute() = default;
  uint64_t socketId{allocateSocketId()};
  UrmaIoContext* ioOwner{nullptr};
  virtual std::shared_ptr<RxBudget> receiveBudget() = 0;
  virtual void onRecord(const RecordHeader&, std::unique_ptr<IOBuf>, uint64_t) = 0;
  virtual void onTx(TxCompletionCtx&, bool) = 0;
  virtual void progress() noexcept = 0;
  virtual void failed(const char*) noexcept = 0;
  virtual void ownerStopped() noexcept { failed("URMA EventBase stopped"); }
};
enum class Operation { Data, Pull, Free, Read };
struct TxCompletionCtx {
  Operation kind{Operation::Data};
  std::shared_ptr<UrmaRoute> route;
  std::shared_ptr<void> completion, imported;
  UrmaPoolBuffer control, localPin, headerPin;
  std::array<UrmaPoolBuffer, 2> extraPins;
  std::unique_ptr<RxStorageLease> destination;
  std::array<urma_sge_t, 4> sges{};
  urma_sge_t remote{};
  urma_jfs_wr_t wr{};
  uint64_t sequence{0};
  size_t bytes{0}, writeOffset{0};
  // RNR explicitly rejected this SEND at the receiver. Retain the exact WR
  // and its pins until successful retry, bounded timeout or shutdown.
  std::chrono::steady_clock::time_point rnrSince{}, retryAt{};
  bool retryPending{false};
  bool remoteRejected{false}; // RNR proves this SEND was not delivered.
};
enum class PostResult { Accepted, Unaccepted, Unknown, Full };
enum class RxSlotState { Empty, Ready, Posted, Completed };
// Exclusively owned by one socket, even when several sockets use one EventBase.
struct ConnectionQueue {
  static constexpr int kEventBatch = 8;
  struct RxBuf {
    RxSlotState state{RxSlotState::Empty};
    std::unique_ptr<RxStorageLease> storage;
    urma_sge_t sge{};
    urma_jfr_wr_t wr{};
    urma_cr_t completion{};
    bool deferred{false};
  };
  urma_jfce_t* event{nullptr};
  urma_jfc_t* cq{nullptr};
  urma_jfs_t* tx{nullptr};
  urma_jfr_t* rx{nullptr};
  urma_jetty_t* endpoint{nullptr};
  std::array<urma_jfc_t*, kEventBatch> events{};
  int eventCount{0};
  std::vector<RxBuf> slots;
  std::vector<size_t> deferredOrder;
  size_t deferredCount{0};
  std::vector<std::unique_ptr<TxCompletionCtx>> flights;
  uint32_t target{0}, depth{0}, posted{0}, inflight{0}, sendDepth{0};
  size_t replacementFailures{0};
  size_t retrying{0};
  bool refillPending{false}, broken{false};
};
class UrmaIoContext : public EventBase::LoopCallback,
                      private AsyncTimeout,
                      public std::enable_shared_from_this<UrmaIoContext> {
 public:
  // Always creates independent queues. SDK preparation runs off the EventBase.
  static std::shared_ptr<UrmaIoContext> get(EventBase*);
  static uint32_t validateCapacity(UrmaConfig&);
  static size_t activeRoutes();
  ~UrmaIoContext() override;
  void add(const std::shared_ptr<UrmaRoute>&);
  void remove(uint64_t, const UrmaRoute*);
  void wait(const std::shared_ptr<UrmaRoute>&);
  void wake() noexcept;
  void stopOwner() noexcept;
  bool ready();
  PostResult post(std::unique_ptr<TxCompletionCtx>&);
  bool canPost(Operation) const;
  urma_context_t* context() const { return context_; }
  urma_jetty_id_t endpoint() const;
  EventBase* eventBase() const { return evb_; }
  std::shared_ptr<UrmaPoolSignal> signal() const { return signal_; }
  std::shared_ptr<RxBudget> readBudget() const { return readBudget_; }
  std::shared_ptr<RxBudget> pendingBudget() const { return pendingBudget_; }
  ConnectionQueue& io() { return io_; }
  const UrmaConfig& config() const { return config_; }
  void runLoopCallback() noexcept override;

 private:
  friend class UrmaIoContextTestPeer;
  struct Handler;
  struct RetiredConnection;
  UrmaIoContext(urma_context_t*, const UrmaConfig&);
  void prepare(ConnectionQueue&);
  bool refill(ConnectionQueue&, size_t);
  void poll(ConnectionQueue&);
  void complete(ConnectionQueue&, const urma_cr_t&) noexcept;
  void retryRnr(ConnectionQueue&);
  void finishRejected(ConnectionQueue&, size_t) noexcept;
  void receive(ConnectionQueue&, const urma_cr_t&);
  void fail(const char*) noexcept;
  void timeoutExpired() noexcept override { wake(); }
  void retryLater() noexcept;
  bool cleanup(ConnectionQueue&) noexcept;
  urma_context_t* context_;
  UrmaConfig config_;
  EventBase* evb_{nullptr};
  ConnectionQueue io_;
  std::shared_ptr<UrmaPoolSignal> signal_;
  std::shared_ptr<RxBudget> budget_, readBudget_, pendingBudget_;
  std::unique_ptr<Handler> ioHandler_, poolHandler_;
  std::unique_ptr<RetiredConnection> cleanupTask_;
  std::shared_ptr<UrmaRoute> route_;
  std::atomic<bool> stopping_{false}, prepared_{false};
  bool detached_{true}, polling_{false}, failed_{false};
  bool preparedOK_{false}, pendingProgress_{false}, readyProgress_{false};
  bool cqPending_{false};
  bool ownsContext_{false}, counted_{false};
};
}  // namespace folly::urma_detail
#endif
