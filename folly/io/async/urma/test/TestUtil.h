#pragma once
/* Copyright (c) Huawei Technologies Co., Ltd. 2026. Licensed under Apache-2.0.
 */
// Reuse the standalone fake initialization SDK, then exercise real owner and
// AsyncSocket entry points. No URMA hardware or external process is needed.
#define URMA_TEST_EXTERNAL_POOL
#include "InitTest.cpp"
#undef URMA_TEST_EXTERNAL_POOL
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <deque>
#include <functional>
#include <utility>
#include <unordered_map>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaCleanup.h>
#include <folly/io/async/urma/UrmaIoContext.h>

namespace {
std::vector<std::shared_ptr<void>> holdReadSlots(folly::UrmaIoContext& owner) {
  std::vector<std::shared_ptr<void>> holds;
  for (unsigned i = 0; i < 1024; ++i) {
    holds.push_back(owner.retainRead());
    require(bool(holds.back()), "READ responsibility fixture");
  }
  return holds;
}
struct Receive {
  uint64_t address, user;
  uint32_t length;
};
std::unordered_map<uint32_t, urma_jfr_t*> receivers;
std::unordered_map<urma_jfr_t*, std::deque<Receive>> posted;
std::unordered_map<urma_jfc_t*, std::deque<urma_cr_t>> ready;
std::unordered_map<urma_jfce_t*, std::deque<urma_jfc_t*>> notifications;
size_t eventsRetrieved = 0, eventsAcknowledged = 0;
bool failEventWait = false, failCqPoll = false, failCqRearm = false;
std::function<void()> onRearm;
std::function<void()> onImport;
uint32_t nextQueue = 1;
unsigned jfsCreated = 0, jfrCreated = 0, jfsDeleted = 0, jfrDeleted = 0;
unsigned importsCreated = 0, importsDeleted = 0;
std::atomic<unsigned> registrations{0};
std::atomic<bool> pauseRegistration{false};
std::string createFailure;
bool postAgain = false;
bool receiveAgain = false;
bool unknownAcceptance = false;
std::atomic<bool> blockUnimport{false};
bool holdFree = false, duplicateFree = false, holdPullCompletion = false;
bool holdFreeCompletions = false;
unsigned rejectFrees = 0;
uint64_t rejectSequence = 0;
using Completions = std::vector<std::pair<urma_jfc_t*, urma_cr_t>>;
Completions heldFreeCompletions;
bool reorderData = false, holdDataCompletions = false;
bool holdReads = false, holdPullReceives = false;
int rejectReadAfter = -1;
bool blockReadPosts = false, unknownReadBoundary = false;
urma_status_t rejectReadStatus = URMA_EAGAIN;
size_t readPostCalls = 0, maxReadBatch = 0, acceptedReadWrs = 0;
Completions heldReads, heldPullCompletions, heldData, heldPullReceives;
unsigned checkedPulls = 0, checkedFrees = 0;
std::atomic<unsigned> segmentImports{0}, segmentUnimports{0};
unsigned lastDataSges = 0, checkedSegments = 0;
bool holdSegmentReady = false;
Completions heldSegmentReady;
bool holdSegmentRevoked = false;
Completions heldSegmentRevoked;
unsigned checkedRevokes = 0, checkedRevoked = 0;
std::atomic<uint64_t> blockedUnimport{0};
std::atomic<unsigned> rejectedUnimports{0};
uint64_t lastCompletionContext = 0;
Completions heldFrees;
void signalQueue(urma_jfc_t* cq, urma_cr_t cr) {
  ready[cq].push_back(cr);
  notifications[cq->jfc_cfg.jfce].push_back(cq);
  uint64_t value = 1;
  auto n = ::write(cq->jfc_cfg.jfce->fd, &value, sizeof(value));
  require(n == sizeof(value), "mock event write");
}
void completeHeld(Completions& held) {
  for (const auto& item : held) signalQueue(item.first, item.second);
  held.clear();
}
void completeHeldReverse(Completions& held) {
  for (auto it = held.rbegin(); it != held.rend(); ++it)
    signalQueue(it->first, it->second);
  held.clear();
}
bool inject(const char* name) {
  if (createFailure != name)
    return false;
  createFailure.clear();
  return true;
}
} // namespace

extern "C" {
urma_jfce_t* urma_create_jfce(urma_context_t*) {
  if (inject("event"))
    return nullptr;
  auto p = new urma_jfce_t{};
  p->fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  return p;
}
urma_status_t urma_delete_jfce(urma_jfce_t* p) {
  require(notifications[p].empty(), "unacknowledged event at JFCE destruction");
  notifications.erase(p);
  close(p->fd);
  delete p;
  return URMA_SUCCESS;
}
urma_jfc_t* urma_create_jfc(urma_context_t*, urma_jfc_cfg_t* cfg) {
  if (inject("cq"))
    return nullptr;
  auto p = new urma_jfc_t{};
  p->jfc_cfg = *cfg;
  return p;
}
urma_status_t urma_delete_jfc(urma_jfc_t* p) {
  ready.erase(p);
  delete p;
  return URMA_SUCCESS;
}
urma_jfs_t* urma_create_jfs(urma_context_t*, urma_jfs_cfg_t* cfg) {
  if (inject("jfs"))
    return nullptr;
  auto p = new urma_jfs_t{};
  p->jfs_cfg = *cfg;
  p->jfs_id.id = nextQueue++;
  ++jfsCreated;
  return p;
}
urma_status_t urma_delete_jfs(urma_jfs_t* p) {
  ++jfsDeleted;
  delete p;
  return URMA_SUCCESS;
}
urma_jfr_t* urma_create_jfr(urma_context_t*, urma_jfr_cfg_t* cfg) {
  if (inject("jfr"))
    return nullptr;
  auto p = new urma_jfr_t{};
  p->jfr_cfg = *cfg;
  p->jfr_id.id = nextQueue++;
  receivers[p->jfr_id.id] = p;
  ++jfrCreated;
  return p;
}
urma_status_t urma_delete_jfr(urma_jfr_t* p) {
  receivers.erase(p->jfr_id.id);
  posted.erase(p);
  ++jfrDeleted;
  delete p;
  return URMA_SUCCESS;
}
urma_jetty_t* urma_create_jetty(urma_context_t*, urma_jetty_cfg_t*) {
  throw std::runtime_error("shared socket attempted to create a Jetty");
}
urma_target_seg_t* urma_register_seg(urma_context_t*, urma_seg_cfg_t* cfg) {
  while (pauseRegistration.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  if (inject("segment"))
    return nullptr;
  auto p = new urma_target_seg_t{};
  p->seg.ubva.va = cfg->va;
  p->seg.len = cfg->len;
  p->seg.attr.bs.access = cfg->flag.bs.access;
  ++registrations;
  return p;
}
urma_status_t urma_unregister_seg(urma_target_seg_t* p) {
  --registrations;
  delete p;
  return URMA_SUCCESS;
}
urma_target_seg_t* urma_import_seg(
    urma_context_t*,
    urma_seg_t* seg,
    urma_token_t*,
    uint64_t,
    urma_import_seg_flag_t) {
  auto p = new urma_target_seg_t{};
  p->seg = *seg;
  ++segmentImports;
  return p;
}
urma_status_t urma_unimport_seg(urma_target_seg_t* p) {
  if (blockedUnimport.load() == p->seg.ubva.va) {
    ++rejectedUnimports;
    return URMA_EAGAIN;
  }
  ++segmentUnimports;
  delete p;
  return URMA_SUCCESS;
}
urma_target_jetty_t* urma_import_jfr(
    urma_context_t*, urma_rjfr_t* remote, urma_token_t*) {
  if (onImport) onImport();
  auto p = new urma_target_jetty_t{};
  p->id = remote->jfr_id;
  ++importsCreated;
  return p;
}
urma_status_t urma_unimport_jfr(urma_target_jetty_t* p) {
  if (blockUnimport.load())
    return URMA_EAGAIN;
  ++importsDeleted;
  delete p;
  return URMA_SUCCESS;
}
urma_status_t urma_post_jfr_wr(
    urma_jfr_t* jfr, urma_jfr_wr_t* wr, urma_jfr_wr_t** bad) {
  require(wr->next == nullptr, "receive refill must remain one WR at a time");
  if (receiveAgain) {
    receiveAgain = false;
    *bad = wr;
    return URMA_EAGAIN;
  }
  auto& sg = wr->src.sge[0];
  posted[jfr].push_back({sg.addr, wr->user_ctx, sg.len});
  return URMA_SUCCESS;
}
static urma_status_t postOne(
    urma_jfs_t* jfs, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
  if (unknownAcceptance) {
    unknownAcceptance = false;
    *bad = nullptr;
    return URMA_EAGAIN;
  }
  if (postAgain) {
    postAgain = false;
    *bad = wr;
    return URMA_EAGAIN;
  }
  lastCompletionContext = wr->user_ctx;
  bool pull = false, free = false;
  urma_cr_t tx{};
  tx.user_ctx = wr->user_ctx;
  tx.status = URMA_CR_SUCCESS;
  if (wr->opcode == URMA_OPC_READ) {
    std::memcpy(
        reinterpret_cast<void*>(wr->rw.dst.sge[0].addr),
        reinterpret_cast<void*>(wr->rw.src.sge[0].addr),
        wr->rw.src.sge[0].len);
  } else {
    auto* receiver = receivers.at(wr->tjetty->id.id);
    auto& queue = posted.at(receiver);
    folly::urma_detail::PacketHeader control{};
    const auto& first = wr->send.src.sge[0];
    free = folly::urma_detail::decodeHeader(
               reinterpret_cast<const uint8_t*>(first.addr), first.len, control) &&
        control.kind == folly::urma_detail::PacketKind::Free;
    const bool rejectRecord = rejectSequence && control.sequence == rejectSequence &&
        (control.kind == folly::urma_detail::PacketKind::Data ||
         control.kind == folly::urma_detail::PacketKind::Pull);
    if (queue.empty() || (free && rejectFrees) || rejectRecord) {
      if (free && rejectFrees) --rejectFrees;
      tx.status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
    } else {
      auto entry = queue.front();
      queue.pop_front();
      std::vector<uint8_t> packetStorage;
      for (uint32_t i = 0; i < wr->send.src.num_sge; ++i) {
        const auto& part = wr->send.src.sge[i];
        auto* begin = reinterpret_cast<const uint8_t*>(part.addr);
        packetStorage.insert(packetStorage.end(), begin, begin + part.len);
      }
      urma_sge_t sg{};
      sg.addr = reinterpret_cast<uintptr_t>(packetStorage.data());
      sg.len = packetStorage.size();
      require(sg.len <= entry.length, "mock RX capacity");
      const auto* packet = reinterpret_cast<const uint8_t*>(sg.addr);
      folly::urma_detail::PacketHeader header{};
      require(
          folly::urma_detail::decodeHeader(packet, sg.len, header),
          "invalid final wire");
      using Kind = folly::urma_detail::PacketKind;
      pull = header.kind == Kind::Pull;
      if (header.kind == Kind::Data) lastDataSges = wr->send.src.num_sge;
      if (header.kind == Kind::Segment) {
        require(sg.len == 64 && wr->opcode == URMA_OPC_SEND,
                "SEGMENT descriptor framing");
        ++checkedSegments;
      }
      if (header.kind == Kind::SegmentRevoke || header.kind == Kind::SegmentRevoked) {
        require(sg.len == 24 && wr->opcode == URMA_OPC_SEND_IMM && !wr->send.imm_data,
                "segment revocation control framing");
        if (header.kind == Kind::SegmentRevoke) ++checkedRevokes;
        else ++checkedRevoked;
      }
      if (pull) {
        require(
            sg.len == 40 && wr->opcode == URMA_OPC_SEND_IMM &&
                !wr->send.imm_data && folly::urma_detail::load64(packet + 32),
            "PULL40 must carry its address in the registered payload");
        ++checkedPulls;
      }
      if (header.kind == Kind::Free) {
        require(
            sg.len == 24 && wr->opcode == URMA_OPC_SEND_IMM &&
                !wr->send.imm_data,
            "FREE24 has no body and no new stream sequence");
        ++checkedFrees;
      }
      std::memcpy(
          reinterpret_cast<void*>(entry.address),
          reinterpret_cast<void*>(sg.addr),
          sg.len);
      urma_cr_t rx{};
      rx.user_ctx = entry.user;
      rx.flag.bs.s_r = 1;
      rx.status = URMA_CR_SUCCESS;
      rx.completion_len = sg.len;
      rx.remote_id = jfs->jfs_id;
      rx.opcode = wr->opcode == URMA_OPC_SEND ? URMA_CR_OPC_SEND
                                              : URMA_CR_OPC_SEND_WITH_IMM;
      // No application fields live in immediate; providers may encode it.
      rx.imm_data = (wr->send.imm_data & 0xffffff00001fffffULL) | 0x123400000ULL;
      if (holdSegmentReady && header.kind == Kind::SegmentReady)
        heldSegmentReady.emplace_back(receiver->jfr_cfg.jfc, rx);
      else if (holdSegmentRevoked && header.kind == Kind::SegmentRevoked)
        heldSegmentRevoked.emplace_back(receiver->jfr_cfg.jfc, rx);
      else if (holdPullReceives && pull)
        heldPullReceives.emplace_back(receiver->jfr_cfg.jfc, rx);
      else if (holdFree && header.kind == Kind::Free)
        heldFrees.emplace_back(receiver->jfr_cfg.jfc, rx);
      else if (holdDataCompletions && header.kind == Kind::Data)
        heldData.emplace_back(receiver->jfr_cfg.jfc, rx);
      else if (reorderData && header.kind == Kind::Data && heldData.empty())
        heldData.emplace_back(receiver->jfr_cfg.jfc, rx);
      else {
        signalQueue(receiver->jfr_cfg.jfc, rx);
        if (reorderData && header.kind == Kind::Data && !heldData.empty()) {
          completeHeld(heldData);
          reorderData = false;
        }
        if (duplicateFree && header.kind == Kind::Free) {
          require(!queue.empty(), "duplicate FREE needs a distinct RX slot");
          auto extra = queue.front();
          queue.pop_front();
          std::memcpy(reinterpret_cast<void*>(extra.address), packet, sg.len);
          rx.user_ctx = extra.user;
          signalQueue(receiver->jfr_cfg.jfc, rx);
        }
      }
    }
  }
  if (holdReads && wr->opcode == URMA_OPC_READ) {
    heldReads.emplace_back(jfs->jfs_cfg.jfc, tx);
  } else if (holdFreeCompletions && free) {
    heldFreeCompletions.emplace_back(jfs->jfs_cfg.jfc, tx);
  } else if (holdPullCompletion && pull) {
    holdPullCompletion = false;
    heldPullCompletions.emplace_back(jfs->jfs_cfg.jfc, tx);
  } else
    signalQueue(jfs->jfs_cfg.jfc, tx);
  return URMA_SUCCESS;
}
urma_status_t urma_post_jfs_wr(
    urma_jfs_t* jfs, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
  if (wr->opcode != URMA_OPC_READ) {
    require(!wr->next, "unexpected SEND batch");
    return postOne(jfs, wr, bad);
  }
  size_t count = 0;
  std::unordered_set<uint64_t> contexts;
  for (auto* item = wr; item; item = item->next) {
    require(
        item->opcode == URMA_OPC_READ && item->flag.bs.complete_enable &&
            item->user_ctx && contexts.insert(item->user_ctx).second,
        "READ chain needs a distinct signaled context per WR");
    require(++count <= jfs->jfs_cfg.depth, "READ chain exceeds SQ depth");
  }
  ++readPostCalls;
  maxReadBatch = std::max(maxReadBatch, count);
  for (auto* item = wr; item; item = item->next) {
    if (blockReadPosts || rejectReadAfter == 0) {
      *bad = unknownReadBoundary ? nullptr : item;
      rejectReadAfter = -1;
      return rejectReadStatus;
    }
    auto status = postOne(jfs, item, bad);
    if (status != URMA_SUCCESS)
      return status;
    ++acceptedReadWrs;
    if (rejectReadAfter > 0)
      --rejectReadAfter;
  }
  return URMA_SUCCESS;
}
int urma_poll_jfc(urma_jfc_t* p, int count, urma_cr_t* cr) {
  if (std::exchange(failCqPoll, false)) return -1;
  int n = 0;
  auto& q = ready[p];
  while (n < count && !q.empty()) {
    cr[n++] = q.front();
    q.pop_front();
  }
  return n;
}
urma_status_t urma_rearm_jfc(urma_jfc_t*, bool) {
  if (std::exchange(failCqRearm, false)) return URMA_FAIL;
  if (auto inject = std::exchange(onRearm, {})) inject();
  return URMA_SUCCESS;
}
int urma_wait_jfc(urma_jfce_t* p, uint32_t count, int, urma_jfc_t** queues) {
  if (std::exchange(failEventWait, false)) return -1;
  uint64_t value;
  auto ignored = ::read(p->fd, &value, sizeof(value));
  (void)ignored;
  auto& pending = notifications[p];
  const auto n = std::min<size_t>(count, pending.size());
  for (size_t i = 0; i < n; ++i) {
    queues[i] = pending.front(); pending.pop_front();
  }
  eventsRetrieved += n;
  return n;
}
void urma_ack_jfc(urma_jfc_t**, uint32_t* counts, uint32_t n) {
  for (uint32_t i = 0; i < n; ++i) eventsAcknowledged += counts[i];
  require(eventsAcknowledged <= eventsRetrieved, "event acknowledged twice");
}
}

using namespace folly;

std::shared_ptr<AsyncServerSocket> listenOnLoopback(
    EventBase& evb, AsyncServerSocket::AcceptCallback& callback,
    SocketAddress& address, int backlog) {
  auto server = AsyncServerSocket::newSocket(&evb);
  server->bind(SocketAddress("127.0.0.1", 0));
  server->listen(backlog);
  server->addAcceptCallback(&callback, &evb);
  server->startAccepting();
  server->getAddress(&address);
  return server;
}
