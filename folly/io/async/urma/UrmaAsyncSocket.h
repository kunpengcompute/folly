/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * UrmaAsyncSocket: AsyncTransport 实现,基于 URMA SEND/RECV + 单边 READ。
 *
 *   客户端: TCP connect + 异步握手 → URMA_ON
 *   服务端: accept fd + 异步握手 → URMA_ON
 *
 *   writeChain: pollTx → 多 SGE 合并 → post SEND → EAGAIN 挂起
 *   >4KB 大包: sendBigMsgByRead(控制包 SEND_IMM + 对端 READ 拉取 + FREE 释放)
 *   onRxBuf: IOBuf::takeOwnership 零拷贝 → read_queue_ → deliverReadData
 *   onControlMsg: SEND_IMM 控制包(PULL/FREE)路由
 *   pollTx: 回收 SEND/READ completion → 释放 sbuf_ → retryWrite
 *   EAGAIN: pending_write_ 挂起 → TX JFCE 中断 → retryWrite
 */
#pragma once

#ifdef FOLLY_HAVE_URMA

#include <atomic>
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <queue>
#include <vector>

#include <folly/SocketAddress.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/EventBase.h>
#include <folly/net/NetworkSocket.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaResource.h>
#include <folly/io/async/urma/UrmaJfsPool.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

class UrmaSharedJfr;
class UrmaHandshake;
class UrmaCqHandler;

class UrmaAsyncSocket : public AsyncTransport {
  friend class UrmaHandshake;
  struct BigMsgGroup;  // 前向声明(定义见 private 区,供 sendChain 签名使用)
 public:
  // 客户端构造
  UrmaAsyncSocket(EventBase* evb, const SocketAddress& addr);
  // 服务端构造(fd 已 accept)
  UrmaAsyncSocket(EventBase* evb, int fd);
  // 握手完成后构造(接管 handshake 资源)
  UrmaAsyncSocket(EventBase* evb, int fd, UrmaHandshake* handshake);

  ~UrmaAsyncSocket() override;

  // ---------- AsyncReader ----------
  void setReadCB(ReadCallback* callback) noexcept override;
  ReadCallback* getReadCallback() const noexcept override { return read_cb_; }

  // ---------- AsyncWriter ----------
  void write(WriteCallback* callback, const void* buf, size_t bytes,
             WriteFlags flags = WriteFlags::NONE) noexcept override;
  void writev(WriteCallback* callback, const iovec* vec, size_t count,
              WriteFlags flags = WriteFlags::NONE) noexcept override;
  void writeChain(WriteCallback* callback, std::unique_ptr<IOBuf>&& buf,
                  WriteFlags flags = WriteFlags::NONE) noexcept override;

  // ---------- AsyncTransport ----------
  EventBase* getEventBase() const override { return evb_; }
  void close() override;
  void closeNow() override;
  void shutdownWrite() override;
  void shutdownWriteNow() override;
  bool good() const override {
    return state_ == State::kConnecting || state_ == State::kHandshaking ||
           state_ == State::kOpen;
  }
  bool readable() const override {
    return state_ == State::kConnecting || state_ == State::kHandshaking ||
           state_ == State::kOpen;
  }
  bool connecting() const override {
    return state_ == State::kConnecting || state_ == State::kHandshaking;
  }
  bool error() const override { return state_ == State::kError; }
  void attachEventBase(EventBase* eventBase) override;
  void detachEventBase() override;
  bool isDetachable() const override { return false; }
  void setSendTimeout(uint32_t milliseconds) override;
  uint32_t getSendTimeout() const override { return send_timeout_ms_; }
  void getLocalAddress(SocketAddress* address) const override;
  void getPeerAddress(SocketAddress* address) const override;
  bool isEorTrackingEnabled() const override { return eor_tracking_; }
  void setEorTracking(bool track) override { eor_tracking_ = track; }
  size_t getAppBytesWritten() const override { return bytes_written_; }
  size_t getRawBytesWritten() const override { return bytes_written_; }
  size_t getAppBytesReceived() const override { return bytes_received_; }
  size_t getRawBytesReceived() const override { return bytes_received_; }

  // ---------- URMA 专属 ----------
  // 接收数据(由 UrmaSharedJfr::pollAndDispatch 调用,普通 SEND)
  void onRxBuf(std::unique_ptr<IOBuf> data) noexcept;

  // Phase 3: headroom 路由 — 从 recv buffer 前 64B 解析 header 路由
  // recv_buf: 接收到的 buffer 起始(header 64B + data)
  // recv_len: 实际接收字节数
  // slot:     rx_bufs_ 的 slot 索引(补投 recv WR 用)
  // 由 UrmaSharedJfr::pollAndDispatch 调用
  void onIoMessageReceived(void* recv_buf, uint32_t recv_len,
                           uint32_t slot) noexcept;

  // 控制包/FREE 通知(由 UrmaSharedJfr::pollAndDispatch 按 imm_valid 路由,SEND_IMM)
  // imm: 编码 msg_id | is_ctl<<56 | data_size<<16
  // meta: 控制包(PULL)时指向 8 字节 remote_va(FREE 时忽略)
  void onControlMsg(uint64_t imm, const void* meta) noexcept;

  // 交付缓存数据给当前 read_cb_(支持 movable 和非 movable 两种模式)
  void deliverReadData() noexcept;

  // TX completion(由 UrmaCqHandler TX 模式调用)
  void pollTx() noexcept;
  void retryWrite() noexcept;

  // 性能优化:活跃 JFS 位图管理
  // sendChain acquireIoJfs 后调用,置位 + 计数+1
  void markActiveJfs(uint32_t jfs_index) {
    if (jfs_index >= kMaxJfsBitmap) return;
    active_jfs_inflight_[jfs_index]++;
    active_jfs_mask_ |= (1ULL << jfs_index);
  }
  // pollTx 回收 CQE 后调用,计数-1,归零则清位
  void releaseActiveJfsSlot(uint32_t jfs_index) {
    if (jfs_index >= kMaxJfsBitmap) return;
    if (active_jfs_inflight_[jfs_index] > 0) {
      active_jfs_inflight_[jfs_index]--;
      if (active_jfs_inflight_[jfs_index] == 0) {
        active_jfs_mask_ &= ~(1ULL << jfs_index);
      }
    }
  }

  // 发送单个 IOBuf chain(多 SGE 合并 → post SEND,或 >4KB 走单边 READ)
  // big_deferred: 本调用有单边 READ 大包发出,writeSuccess 延迟到对端 FREE
  urma_status_t sendChain(WriteCallback* cb,
                          std::unique_ptr<IOBuf>& chain,
                          uint32_t& total_posted,
                          bool& big_deferred,
                          const std::shared_ptr<BigMsgGroup>& group) noexcept;

  void handlePendingWrites() noexcept;

  void onHandshakeData(const uint8_t* data, size_t len);

  struct ConnectCallback : AsyncSocket::ConnectCallback {
    UrmaAsyncSocket* self;
    void connectSuccess() noexcept override;
    void connectErr(const AsyncSocketException&) noexcept override;
  };

  static UniquePtr newSocket(
    EventBase* evb,
    const folly::SocketAddress& address) {
    return UniquePtr{new UrmaAsyncSocket(evb, address)};
  }

 private:
  enum class State : uint8_t {
    kIdle,
    kConnecting,
    kHandshaking,
    kOpen,
    kClosing,
    kClosed,
    kError,
  };

  void startHandshake(bool is_client);
  void onHandshakeReady();
  void onHandshakeError(const std::string& reason);
  void destroyUrmaResources();

  class TcpHandshakeHandler : public EventHandler {
   public:
    TcpHandshakeHandler(EventBase* evb, int fd, UrmaAsyncSocket* socket)
        : EventHandler(evb, NetworkSocket(fd)),
          socket_(socket) {
      registerHandler(READ | PERSIST);
    }
    ~TcpHandshakeHandler() override { unregisterHandler(); }
    void handlerReady(uint16_t /*events*/) noexcept override;
   private:
    UrmaAsyncSocket* socket_;
  };

  // ── 单边 READ 内部函数 ──
  bool sendBigMsgByRead(WriteCallback* cb, std::unique_ptr<IOBuf> node,
                        const std::shared_ptr<BigMsgGroup>& group) noexcept;
  bool postControlPacket(uint16_t msg_id, uint64_t remote_va,
                         uint32_t data_size) noexcept;
  bool startRead(uint16_t msg_id, uint64_t remote_va, uint32_t data_size) noexcept;
  bool postFreeNotify(uint16_t msg_id) noexcept;
  void onReadComplete() noexcept;
  void onReadFailed(urma_status_t status) noexcept;
  void scanBigMsgTimeout() noexcept;
  void drainRxPendingQueue() noexcept;
  static uint64_t encodeBigMsgImm(uint16_t msg_id, uint8_t is_ctl, uint32_t data_size) {
    return (static_cast<uint64_t>(msg_id)) |
           (static_cast<uint64_t>(is_ctl) << 56) |
           (static_cast<uint64_t>(data_size) << 16);
  }
  static void decodeBigMsgImm(uint64_t imm, uint16_t& msg_id, uint8_t& is_ctl, uint32_t& data_size) {
    msg_id = static_cast<uint16_t>(imm & 0xFFFF);
    is_ctl = static_cast<uint8_t>((imm >> 56) & 0xFF);
    data_size = static_cast<uint32_t>((imm >> 16) & 0xFFFFFFFF);
  }

  EventBase* evb_;
  int tcp_fd_{-1};
  State state_{State::kIdle};
  ReadCallback* read_cb_{nullptr};

  // Phase 3: SocketId(headroom 路由用,接收方据此路由到本 socket)
  SocketId socket_id_{};
  // Phase 3: per-连接发送序列号(headroom header 中的 seq 字段)
  std::atomic<uint16_t> send_seq_{0};

  // 性能优化:跟踪本连接活跃的 JFS 位图(避免 pollTx 遍历全部 64 个 JFS)
  // sendChain acquireIoJfs 后置位;pollTx 回收 CQE 后若该 JFS 无本连接在途 WR 则清位
  // 用 per-JFS 计数 active_jfs_inflight_[idx] 跟踪本连接在该 JFS 上的在途 WR 数
  // 位图 + 计数数组长度按 io_jfs_count 上限 64 设计
  static constexpr uint32_t kMaxJfsBitmap = 64;
  uint64_t active_jfs_mask_{0};              // 位图:bit i 置位表示 JFS i 有本连接在途 WR
  uint32_t active_jfs_inflight_[kMaxJfsBitmap]{0};  // per-JFS 本连接在途 WR 计数

  folly::IOBufQueue read_queue_;

  std::unique_ptr<ConnectCallback> connect_cb_;
  AsyncSocket::UniquePtr tcp_socket_;

  UrmaSharedJfr* shared_{nullptr};
  UrmaHandshake* handshake_{nullptr};
  UrmaResource res_;
  uint32_t peer_rx_buf_size_{4096};

  std::unique_ptr<TcpHandshakeHandler> tcp_handshake_handler_;

  // 一个 writeChain 内多个大包共享的完成组:
  // writeSuccess 只在组内所有大包都收到对端 FREE 后回调一次
  struct BigMsgGroup {
    uint32_t remaining{0};  // 本 writeChain 尚未收到 FREE 的大包数
    bool done{false};       // 已回调 writeSuccess/writeErr
  };

  struct PendingWrite {
    WriteCallback* cb;
    std::unique_ptr<IOBuf> buf;
    std::shared_ptr<BigMsgGroup> big_group;  // 跨 EAGAIN 复用同一个组
    std::unique_ptr<PendingWrite> next;
  };
  std::unique_ptr<PendingWrite> pending_write_head_;
  PendingWrite* pending_write_tail_{nullptr};

  // ── 单边 READ 状态 ──
  static constexpr uint64_t kCtrlUserCtx = 0xFFFFFFFFFFFFFFFFULL;
  static constexpr uint64_t kReadUserCtxBase = 0x8000000000000000ULL;
  static constexpr uint8_t kCtrlTypePull = 1;
  static constexpr uint8_t kCtrlTypeFree = 2;
  static constexpr int kCtrlMsgMaxRetries = 3;

  struct PendingBigMsg {
    std::shared_ptr<IOBuf> iobuf;
    std::chrono::steady_clock::time_point post_time;
    uint8_t retries{0};
    bool is_free{false};
    WriteCallback* cb{nullptr};  // 延迟 writeSuccess/writeErr 用
    std::shared_ptr<BigMsgGroup> group;  // 所属 writeChain 完成组
  };
  std::map<uint16_t, PendingBigMsg> pending_big_msg_;

  struct RxBufItem {
    bool is_control = false;
    uint16_t msg_id = 0;
    uint64_t remote_va = 0;
    uint32_t data_size = 0;
    std::unique_ptr<IOBuf> data;
  };
  std::queue<RxBufItem> rx_pending_queue_;

  bool has_pending_read_{false};
  uint16_t pending_read_msg_id_{0};
  std::unique_ptr<IOBuf> pending_read_buf_;
  std::shared_ptr<std::vector<urma_sge_t>> pending_read_sge_;

  uint16_t next_msg_id_{1};
  // 控制包 8 字节 remote_va 的固定 MR buffer(每连接复用,B-6)
  void* ctrl_meta_mr_{nullptr};
  // Phase 2: 控制包 SGE 引用(保留兼容,新路径用 TxCompletionCtx 管理)
  std::deque<std::shared_ptr<std::vector<urma_sge_t>>> ctrl_sge_refs_;

  uint32_t send_timeout_ms_{0};
  size_t bytes_written_{0};
  size_t bytes_received_{0};
  bool eor_tracking_{false};

  static constexpr int POLL_BATCH_MAX = 64;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA