/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <folly/io/async/urma/UrmaHandshake.h>

#ifdef FOLLY_HAVE_URMA

#include <algorithm>
#include <arpa/inet.h>
#include <unistd.h>

#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaSharedJfr.h>
#include <folly/io/async/urma/UrmaJfrPool.h>
#include <folly/logging/xlog.h>

namespace folly {

namespace v2_wire {

void HelloMessage::serialize(void* buf) const {
  auto* p = static_cast<uint8_t*>(buf);
  // magic "URMA"
  p[0] = 0x55; p[1] = 0x52; p[2] = 0x4D; p[3] = 0x41;
  p += 4;

  uint16_t v16;
  uint32_t v32;
  uint64_t v64;

  v16 = htons(msg_len);     memcpy(p, &v16, 2); p += 2;
  v16 = htons(hello_ver);   memcpy(p, &v16, 2); p += 2;
  v16 = htons(impl_ver);    memcpy(p, &v16, 2); p += 2;
  v32 = htonl(buffer_size); memcpy(p, &v32, 4); p += 4;
  v32 = htonl(recv_buffer_cnt); memcpy(p, &v32, 4); p += 4;
  v32 = htonl(jfr_id);     memcpy(p, &v32, 4); p += 4;
  memcpy(p, eid, 16);       p += 16;
  v32 = htonl(uasid);       memcpy(p, &v32, 4); p += 4;
  *p = tp_type;             p += 1;
  memcpy(p, capability_flags, 3); p += 3;
  memcpy(p, seg_eid, 16);   p += 16;
  v32 = htonl(seg_uasid);   memcpy(p, &v32, 4); p += 4;
  v64 = htobe64(seg_va);    memcpy(p, &v64, 8); p += 8;
  v64 = htobe64(seg_len);   memcpy(p, &v64, 8); p += 8;
  v32 = htonl(seg_token_id);memcpy(p, &v32, 4); p += 4;
  v32 = htonl(fc_jfr_id);  memcpy(p, &v32, 4); p += 4;
  v64 = htobe64(socket_id); memcpy(p, &v64, 8); p += 8;
  // total = 4 + 94 = 98
}

void HelloMessage::deserialize(const void* buf) {
  auto* p = static_cast<const uint8_t*>(buf);
  // skip magic (already verified by caller)
  p += 4;

  uint16_t v16;
  uint32_t v32;
  uint64_t v64;

  memcpy(&v16, p, 2); msg_len = ntohs(v16); p += 2;
  memcpy(&v16, p, 2); hello_ver = ntohs(v16); p += 2;
  memcpy(&v16, p, 2); impl_ver = ntohs(v16); p += 2;
  memcpy(&v32, p, 4); buffer_size = ntohl(v32); p += 4;
  memcpy(&v32, p, 4); recv_buffer_cnt = ntohl(v32); p += 4;
  memcpy(&v32, p, 4); jfr_id = ntohl(v32); p += 4;
  memcpy(eid, p, 16); p += 16;
  memcpy(&v32, p, 4); uasid = ntohl(v32); p += 4;
  tp_type = *p; p += 1;
  memcpy(capability_flags, p, 3); p += 3;
  memcpy(seg_eid, p, 16); p += 16;
  memcpy(&v32, p, 4); seg_uasid = ntohl(v32); p += 4;
  memcpy(&v64, p, 8); seg_va = be64toh(v64); p += 8;
  memcpy(&v64, p, 8); seg_len = be64toh(v64); p += 8;
  memcpy(&v32, p, 4); seg_token_id = ntohl(v32); p += 4;
  memcpy(&v32, p, 4); fc_jfr_id = ntohl(v32); p += 4;
  memcpy(&v64, p, 8); socket_id = be64toh(v64); p += 8;
}

} // namespace v2_wire

UrmaHandshake::UrmaHandshake(EventBase* evb, int tcp_fd, bool is_client,
                              UrmaSharedJfr* shared, const UrmaConfig& config,
                              UrmaAsyncSocket* socket)
    : AsyncTimeout(evb),
      tcp_fd_(tcp_fd),
      is_client_(is_client),
      shared_(shared),
      config_(config),
      socket_(socket) {
  // 填充本地 HelloMessage
  urma_context_t* ctx = UrmaInit::getContext();
  urma_target_seg_t* pool_seg = UrmaInit::getPoolSeg();

  local_hello_.buffer_size = URMA_SMALL_BUF_SIZE;
  local_hello_.recv_buffer_cnt = config_.rx_depth;
  local_hello_.tp_type = URMA_CTP;
  // jetty_id / fc_jetty_id / eid / uasid / seg_* 在 allocateResources 后填充
  if (pool_seg) {
    // pool_seg->seg.ubva 含远端 import 所需的 eid/uasid/va
    memcpy(local_hello_.seg_eid, pool_seg->seg.ubva.eid.raw, 16);
    local_hello_.seg_uasid = pool_seg->seg.ubva.uasid;
    local_hello_.seg_va = pool_seg->seg.ubva.va;
    local_hello_.seg_len = pool_seg->seg.len;
    local_hello_.seg_token_id = pool_seg->seg.token_id;
  }
  if (ctx) {
    local_hello_.uasid = ctx->uasid;
  }
}

UrmaHandshake::~UrmaHandshake() {
  cancelTimeout();
}

void UrmaHandshake::start(ReadyCallback ready_cb, ErrorCallback error_cb) {
  ready_cb_ = std::move(ready_cb);
  error_cb_ = std::move(error_cb);

  if (is_client_) {
    // 任务 A: 去 Jetty —— 通告 JFR 级共享 jfr_id(替代 per-连接 jetty_id)
    // 同一 UrmaSharedJfr(同 EVB)选定同一 JFR → 所有连接通告相同 jfr_id
    local_hello_.jfr_id = shared_->ioJfrId();
    local_hello_.fc_jfr_id = shared_->fcJfrId();
    if (urma_context_t* ctx = UrmaInit::getContext()) {
      memcpy(local_hello_.eid, UrmaInit::getLocalEid().raw, 16);
      local_hello_.uasid = ctx->uasid;
    }
    local_hello_.socket_id = socket_->socket_id_.toUint64();
    sendHello();
  } else {
    // 服务端:等客户端 Hello
    phase_ = Phase::kWaitHello;
    scheduleTimeout(handshake_timeout_ms_);
  }
}

void UrmaHandshake::sendHello() {
  phase_ = Phase::kSendHello;
  scheduleTimeout(handshake_timeout_ms_);

  // 序列化 HelloMessage 并通过 TCP fd 发送
  uint8_t buf[v2_wire::HELLO_PACKET_LEN];
  local_hello_.serialize(buf);

  ssize_t ret = write(tcp_fd_, buf, v2_wire::HELLO_PACKET_LEN);
  if (ret != static_cast<ssize_t>(v2_wire::HELLO_PACKET_LEN)) {
    onHandshakeError("sendHello: write failed");
    return;
  }

  // 客户端:发完 Hello 后等对端 Hello
  // 服务端:发完 Hello 后等 ACK
  if (is_client_) {
    phase_ = Phase::kWaitHello;
  } else {
    phase_ = Phase::kWaitAck;
  }
}

bool UrmaHandshake::validateHello(const v2_wire::HelloMessage& hello) {
  if (hello.hello_ver != v2_wire::HELLO_V2_VERSION) {
    onHandshakeError("hello_ver mismatch");
    return false;
  }
  if (hello.impl_ver != v2_wire::IMPL_V2_VERSION) {
    onHandshakeError("impl_ver mismatch");
    return false;
  }
  // Phase 8: 协商池化能力 — 对端不支持时握手失败,触发降级 TCP
  uint8_t peer_caps = hello.capability_flags[0];
  if (!(peer_caps & v2_wire::CAP_SHARED_JFR)) {
    onHandshakeError("peer does not support CAP_SHARED_JFR");
    return false;
  }
  if (!(peer_caps & v2_wire::CAP_HEADROOM_ROUTING)) {
    onHandshakeError("peer does not support CAP_HEADROOM_ROUTING");
    return false;
  }
  if (hello.buffer_size < 1024) {
    onHandshakeError("buffer_size too small");
    return false;
  }
  if (hello.recv_buffer_cnt < 3) {
    onHandshakeError("recv_buffer_cnt too small");
    return false;
  }
  if (hello.jfr_id == 0) {
    onHandshakeError("jfr_id is 0");
    return false;
  }
  if (hello.fc_jfr_id == 0) {
    onHandshakeError("fc_jfr_id is 0");
    return false;
  }
  if (hello.seg_len == 0 || hello.seg_va == 0) {
    onHandshakeError("seg invalid");
    return false;
  }
  return true;
}

void UrmaHandshake::allocateResources() {
  // 任务 A(SDD-URMA-SHARED-JFS-JFR-002): 去 Jetty —— 整个函数体清空。
  // JFR 自带 jfr_id(urma_jfr_t.jfr_id.id,类型 = urma_jetfr_id_t),
  // 对端通过 urma_import_jfr(jfr_id) 得到 urma_target_jetty_t*,
  // 与 wr.tjetty 完全兼容。本端无需创建任何 Jetty。
  // JFR/JFS/JFC/JFCE 均由进程级 UrmaJfrPool/UrmaJfsPool 持有。
}

void UrmaHandshake::importPeer(const v2_wire::HelloMessage& hello) {
  urma_context_t* ctx = UrmaInit::getContext();
  auto& res = socket_->res_;

  // 保存对端通告的 jfr_id
  res.peer_io_jetty_id = hello.jfr_id;
  res.peer_fc_jetty_id = hello.fc_jfr_id;
  res.peer_socket_id = hello.socket_id;

  // 任务 A: 去 Jetty —— 对端通告 JFR 级共享 jfr_id,
  // 通过进程级 cache(urma_import_jfr)导入,同远端 JFR 只 import 一次。
  // 注意: jfr_id 是 urma_jetty_id_t(含 eid+uasid+id),三个字段都必须填充
  urma_rjfr_t rjfr = {};
  memcpy(rjfr.jfr_id.eid.raw, hello.eid, 16);
  rjfr.jfr_id.uasid = hello.uasid;
  rjfr.jfr_id.id = hello.jfr_id;
  rjfr.trans_mode = URMA_TM_RM;
  rjfr.tp_type = URMA_CTP;
  res.remote_jetty = UrmaJfrPool::importRemoteJfr(&rjfr);
  if (!res.remote_jetty) {
    onHandshakeError("import_jfr(IO) failed");
    return;
  }

  // import FC JFR
  urma_rjfr_t fc_rjfr = {};
  memcpy(fc_rjfr.jfr_id.eid.raw, hello.eid, 16);
  fc_rjfr.jfr_id.uasid = hello.uasid;
  fc_rjfr.jfr_id.id = hello.fc_jfr_id;
  fc_rjfr.trans_mode = URMA_TM_RM;
  fc_rjfr.tp_type = URMA_CTP;
  res.remote_fc_jetty = UrmaJfrPool::importRemoteJfr(&fc_rjfr);
  if (!res.remote_fc_jetty) {
    onHandshakeError("import_jfr(FC) failed");
    return;
  }

  // import 远端 MR 段(全局共享池,进程级 cache)
  urma_seg_t peer_seg = {};
  memcpy(peer_seg.ubva.eid.raw, hello.seg_eid, 16);
  peer_seg.ubva.uasid = hello.seg_uasid;
  peer_seg.ubva.va = hello.seg_va;
  peer_seg.len = hello.seg_len;
  peer_seg.token_id = hello.seg_token_id;
  res.remote_seg = UrmaJfrPool::importRemoteSeg(&peer_seg);
  if (!res.remote_seg) {
    onHandshakeError("import_seg failed");
    return;
  }
}

void UrmaHandshake::processRemoteHello(const v2_wire::HelloMessage& hello) {
  if (!validateHello(hello)) {
    return;
  }

  // buffer_size 协商:取最小值
  peer_rx_buf_size_ = std::min(
      static_cast<uint32_t>(URMA_SMALL_BUF_SIZE), hello.buffer_size);

  if (is_client_) {
    // 客户端:收到服务端 Hello → import peer → 发 ACK
    importPeer(hello);
    sendAck();
  } else {
    // 服务端:收到客户端 Hello → import peer → 发 Hello → 等 ACK
    // 任务 A: 去 Jetty —— 直接从 UrmaSharedJfr 获取 JFR 级共享 jfr_id
    local_hello_.jfr_id = shared_->ioJfrId();
    local_hello_.fc_jfr_id = shared_->fcJfrId();
    if (urma_context_t* ctx = UrmaInit::getContext()) {
      memcpy(local_hello_.eid, UrmaInit::getLocalEid().raw, 16);
      local_hello_.uasid = ctx->uasid;
    }
    local_hello_.socket_id = socket_->socket_id_.toUint64();
    importPeer(hello);
    // 预注册 socket_map:客户端 ACK 后会立即发首包,服务端 registerSocket
    // 必须在首包 CQE 到达 harvestAndDispatch 之前完成,否则 socket_map miss。
    // 在 kWaitHello 阶段 peer_socket_id 已知,此处预注册消除竞态。
    // 任务 D(H-5):registerRoutes = 注册 + 活跃连接记账同点绑定
    // (幂等:与 onHandshakeReady 的重注册不双计;签名不变)
    socket_->registerRoutes();
    // 发 Hello(通过 TCP fd)
    sendHello();
    // sendHello 内部设置 phase_ = kWaitAck 并启动超时
  }
}

void UrmaHandshake::sendAck() {
  phase_ = Phase::kSendAck;

  // 通过 TCP fd 发送 4 字节 ACK
  uint32_t ack = htonl(v2_wire::HELLO_ACK_VALUE);
  ssize_t ret = write(tcp_fd_, &ack, sizeof(ack));
  if (ret != static_cast<ssize_t>(sizeof(ack))) {
    onHandshakeError("sendAck: write failed");
    return;
  }

  // 客户端:发完 ACK 后握手完成
  onHandshakeReady();
}

void UrmaHandshake::onDataAvailable(const uint8_t* data, size_t len) {
  recv_buf_.insert(recv_buf_.end(), data, data + len);

  while (!recv_buf_.empty()) {
    if (phase_ == Phase::kWaitHello || phase_ == Phase::kSendHello) {
      // 期望收到 HelloMessage (90 字节)
      if (recv_buf_.size() < v2_wire::HELLO_PACKET_LEN) {
        break;  // 数据不够,等更多
      }
      // 检查 magic
      if (recv_buf_[0] != 0x55 || recv_buf_[1] != 0x52 ||
          recv_buf_[2] != 0x4D || recv_buf_[3] != 0x41) {
        onHandshakeError("magic mismatch");
        return;
      }
      v2_wire::HelloMessage hello;
      hello.deserialize(recv_buf_.data());
      recv_buf_.erase(recv_buf_.begin(),
                      recv_buf_.begin() + v2_wire::HELLO_PACKET_LEN);
      cancelTimeout();
      processRemoteHello(hello);
    } else if (phase_ == Phase::kWaitAck) {
      // 期望收到 ACK (4 字节)
      if (recv_buf_.size() < sizeof(uint32_t)) {
        break;  // 数据不够,等更多
      }
      uint32_t ack;
      memcpy(&ack, recv_buf_.data(), sizeof(ack));
      ack = ntohl(ack);
      recv_buf_.erase(recv_buf_.begin(),
                      recv_buf_.begin() + sizeof(ack));
      if (ack == v2_wire::HELLO_ACK_VALUE) {
        cancelTimeout();
        onHandshakeReady();
      } else {
        onHandshakeError("invalid ACK");
        return;
      }
    } else {
      // kIdle / kDone / kSendAck: 不期望收到数据
      break;
    }
  }
}

void UrmaHandshake::onHandshakeReady() {
  phase_ = Phase::kDone;
  if (ready_cb_) {
    auto cb = std::move(ready_cb_);
    ready_cb_ = nullptr;
    cb();
  }
}

void UrmaHandshake::onHandshakeError(const std::string& reason) {
  XLOG(ERR) << "UrmaHandshake error: " << reason;
  phase_ = Phase::kDone;
  cancelTimeout();
  if (error_cb_) {
    auto cb = std::move(error_cb_);
    error_cb_ = nullptr;
    cb(reason);
  }
}

void UrmaHandshake::timeoutExpired() noexcept {
  onHandshakeError("handshake timeout (5s)");
}

} // namespace folly

#endif // FOLLY_HAVE_URMA
