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
  v32 = htonl(jetty_id);    memcpy(p, &v32, 4); p += 4;
  memcpy(p, eid, 16);       p += 16;
  v32 = htonl(uasid);       memcpy(p, &v32, 4); p += 4;
  *p = tp_type;             p += 1;
  memcpy(p, pad, 3);        p += 3;
  memcpy(p, seg_eid, 16);   p += 16;
  v32 = htonl(seg_uasid);   memcpy(p, &v32, 4); p += 4;
  v64 = htobe64(seg_va);    memcpy(p, &v64, 8); p += 8;
  v64 = htobe64(seg_len);   memcpy(p, &v64, 8); p += 8;
  v32 = htonl(seg_token_id);memcpy(p, &v32, 4); p += 4;
  v32 = htonl(fc_jetty_id); memcpy(p, &v32, 4); p += 4;
  // total = 4 + 86 = 90
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
  memcpy(&v32, p, 4); jetty_id = ntohl(v32); p += 4;
  memcpy(eid, p, 16); p += 16;
  memcpy(&v32, p, 4); uasid = ntohl(v32); p += 4;
  tp_type = *p; p += 1;
  memcpy(pad, p, 3); p += 3;
  memcpy(seg_eid, p, 16); p += 16;
  memcpy(&v32, p, 4); seg_uasid = ntohl(v32); p += 4;
  memcpy(&v64, p, 8); seg_va = be64toh(v64); p += 8;
  memcpy(&v64, p, 8); seg_len = be64toh(v64); p += 8;
  memcpy(&v32, p, 4); seg_token_id = ntohl(v32); p += 4;
  memcpy(&v32, p, 4); fc_jetty_id = ntohl(v32); p += 4;
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
  urma_seg_t* pool_seg = UrmaInit::getPoolSeg();

  local_hello_.buffer_size = URMA_SMALL_BUF_SIZE;
  local_hello_.recv_buffer_cnt = config_.rx_depth;
  local_hello_.tp_type = URMA_CTP;
  // jetty_id / fc_jetty_id / eid / uasid / seg_* 在 allocateResources 后填充
  if (pool_seg) {
    memcpy(local_hello_.seg_eid, pool_seg->eid.eid, 16);
    local_hello_.seg_uasid = ctx ? ctx->uasid : 0;
    local_hello_.seg_va = reinterpret_cast<uintptr_t>(pool_seg->va);
    local_hello_.seg_len = pool_seg->len;
    local_hello_.seg_token_id = pool_seg->token_id;
  }
}

UrmaHandshake::~UrmaHandshake() {
  cancelTimeout();
}

void UrmaHandshake::start(ReadyCallback ready_cb, ErrorCallback error_cb) {
  ready_cb_ = std::move(ready_cb);
  error_cb_ = std::move(error_cb);

  if (is_client_) {
    // 客户端:先创建资源,再发 Hello
    allocateResources();
    // 填充 jetty_id 到 local_hello_
    if (socket_->res_.io_jetty) {
      local_hello_.jetty_id = socket_->res_.io_jetty->jetty_id.id;
    }
    if (socket_->res_.fc_jetty) {
      local_hello_.fc_jetty_id = socket_->res_.fc_jetty->jetty_id.id;
    }
    if (urma_context_t* ctx = UrmaInit::getContext()) {
      memcpy(local_hello_.eid, ctx->eid.eid, 16);
      local_hello_.uasid = ctx->uasid;
    }
    sendHello();
  } else {
    // 服务端:等客户端 Hello
    phase_ = Phase::kWaitHello;
    scheduleTimeout(HANDSHAKE_TIMEOUT_MS);
  }
}

void UrmaHandshake::sendHello() {
  phase_ = Phase::kSendHello;
  scheduleTimeout(HANDSHAKE_TIMEOUT_MS);

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
  if (hello.buffer_size < 1024) {
    onHandshakeError("buffer_size too small");
    return false;
  }
  if (hello.recv_buffer_cnt < 3) {
    onHandshakeError("recv_buffer_cnt too small");
    return false;
  }
  if (hello.jetty_id == 0) {
    onHandshakeError("jetty_id is 0");
    return false;
  }
  if (hello.fc_jetty_id == 0) {
    onHandshakeError("fc_jetty_id is 0");
    return false;
  }
  if (hello.seg_len == 0 || hello.seg_va == 0) {
    onHandshakeError("seg invalid");
    return false;
  }
  return true;
}

void UrmaHandshake::allocateResources() {
  urma_context_t* ctx = UrmaInit::getContext();
  if (!ctx) {
    onHandshakeError("UrmaInit not initialized");
    return;
  }

  auto& res = socket_->res_;

  // TX JFCE (先创建)
  res.tx_jfce = urma_create_jfce(ctx);
  if (!res.tx_jfce) {
    onHandshakeError("create tx_jfce failed");
    return;
  }

  // TX JFC (depth = tx_depth + 1, 预留 flush_done, 指定 jfce)
  urma_jfc_cfg_t tx_jfc_cfg = {};
  tx_jfc_cfg.depth = config_.tx_depth + 1;
  tx_jfc_cfg.jfce = res.tx_jfce;
  res.tx_jfc = urma_create_jfc(ctx, &tx_jfc_cfg);
  if (!res.tx_jfc) {
    onHandshakeError("create tx_jfc failed");
    return;  // UrmaAsyncSocket 析构清理 res.tx_jfce
  }

  // IO jetty (share_jfr=1)
  urma_jetty_cfg_t io_cfg = {};
  io_cfg.flag.bs.share_jfr = 1;
  io_cfg.shared.jfr = shared_->ioJfr();
  io_cfg.shared.jfc = shared_->ioJfc();
  io_cfg.jfs_cfg.depth = config_.tx_depth;
  io_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
  io_cfg.jfs_cfg.rnr_retry = URMA_DEFAULT_RNR_RETRY;
  io_cfg.jfs_cfg.err_timeout = URMA_DEFAULT_ERR_TIMEOUT;
  io_cfg.jfs_cfg.jfc = res.tx_jfc;
  res.io_jetty = urma_create_jetty(ctx, &io_cfg);
  if (!res.io_jetty) {
    onHandshakeError("create io_jetty failed");
    return;  // 析构清理 res.tx_jfc + res.tx_jfce
  }

  // FC TX JFC (depth=2, 指定同一个 tx_jfce)
  urma_jfc_cfg_t fc_tx_jfc_cfg = {};
  fc_tx_jfc_cfg.depth = 2;
  fc_tx_jfc_cfg.jfce = res.tx_jfce;
  res.fc_tx_jfc = urma_create_jfc(ctx, &fc_tx_jfc_cfg);
  if (!res.fc_tx_jfc) {
    onHandshakeError("create fc_tx_jfc failed");
    return;  // 析构清理 res.io_jetty + res.tx_jfc + res.tx_jfce
  }

  // FC jetty (depth=2)
  urma_jetty_cfg_t fc_cfg = {};
  fc_cfg.flag.bs.share_jfr = 1;
  fc_cfg.shared.jfr = shared_->fcJfr();
  fc_cfg.shared.jfc = shared_->fcJfc();
  fc_cfg.jfs_cfg.depth = 2;
  fc_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
  fc_cfg.jfs_cfg.rnr_retry = URMA_DEFAULT_RNR_RETRY;
  fc_cfg.jfs_cfg.err_timeout = URMA_DEFAULT_ERR_TIMEOUT;
  fc_cfg.jfs_cfg.jfc = res.fc_tx_jfc;
  res.fc_jetty = urma_create_jetty(ctx, &fc_cfg);
  if (!res.fc_jetty) {
    onHandshakeError("create fc_jetty failed");
    return;  // 析构清理 res.fc_tx_jfc + res.io_jetty + res.tx_jfc + res.tx_jfce
  }
}

void UrmaHandshake::importPeer(const v2_wire::HelloMessage& hello) {
  urma_context_t* ctx = UrmaInit::getContext();
  auto& res = socket_->res_;

  // 构造 rjetty for IO
  urma_rjetty_t rjetty = {};
  rjetty.jetty_id.id = hello.jetty_id;
  memcpy(rjetty.jetty_id.eid.eid, hello.eid, 16);
  rjetty.trans_mode = URMA_TM_RM;
  rjetty.tp_type = URMA_CTP;

  // 构造 target_seg
  urma_target_seg_t seg_info = {};
  memcpy(seg_info.eid.eid, hello.seg_eid, 16);
  seg_info.uasid = hello.seg_uasid;
  seg_info.va = reinterpret_cast<void*>(hello.seg_va);
  seg_info.len = hello.seg_len;
  seg_info.token_id = hello.seg_token_id;

  // import_seg 必须先于 import_jetty
  res.remote_seg = urma_import_seg(ctx, &seg_info);
  if (!res.remote_seg) {
    onHandshakeError("import_seg failed");
    return;
  }

  urma_token_t token = 0;
  res.remote_jetty = urma_import_jetty(ctx, &rjetty, &token);
  if (!res.remote_jetty) {
    onHandshakeError("import_jetty(IO) failed");
    return;  // 析构清理 res.remote_seg
  }

  // import FC jetty
  urma_rjetty_t fc_rjetty = {};
  fc_rjetty.jetty_id.id = hello.fc_jetty_id;
  memcpy(fc_rjetty.jetty_id.eid.eid, hello.eid, 16);
  fc_rjetty.trans_mode = URMA_TM_RM;
  fc_rjetty.tp_type = URMA_CTP;

  res.remote_fc_jetty = urma_import_jetty(ctx, &fc_rjetty, &token);
  if (!res.remote_fc_jetty) {
    onHandshakeError("import_jetty(FC) failed");
    return;  // 析构清理 res.remote_jetty + res.remote_seg
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
    // 服务端:收到客户端 Hello → 创建资源 → import peer → 发 Hello → 等 ACK
    allocateResources();
    if (socket_->res_.io_jetty) {
      local_hello_.jetty_id = socket_->res_.io_jetty->jetty_id.id;
    }
    if (socket_->res_.fc_jetty) {
      local_hello_.fc_jetty_id = socket_->res_.fc_jetty->jetty_id.id;
    }
    if (urma_context_t* ctx = UrmaInit::getContext()) {
      memcpy(local_hello_.eid, ctx->eid.eid, 16);
      local_hello_.uasid = ctx->uasid;
    }
    importPeer(hello);
    // 发 Hello(通过 TCP fd)
    phase_ = Phase::kSendHello;
    scheduleTimeout(HANDSHAKE_TIMEOUT_MS);
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
