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
 *   onControlMsgImm: SEND_IMM 控制包(PULL/FREE)路由,从 imm_data 和 recv_buf 解析
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
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/EventBase.h>
#include <folly/net/NetworkSocket.h>
#include <folly/io/async/urma/UrmaConfig.h>
#include <folly/io/async/urma/UrmaResource.h>
#include <folly/io/async/urma/UrmaTransportContext.h>
#include <folly/io/async/urma/UrmaRouteTable.h>
#include <folly/io/async/urma/UrmaSeqReorder.h>
#include <folly/io/async/urma/UrmaHeader.h>
#include <folly/io/async/urma/UrmaFlowControl.h>
#include <ub/umdk/urma/urma_api.h>

namespace folly {

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
    if (tcp_delegate_) return tcp_delegate_->good();
    return state_ == State::kConnecting || state_ == State::kHandshaking ||
           state_ == State::kOpen;
  }
  bool readable() const override {
    if (tcp_delegate_) return tcp_delegate_->readable();
    return state_ == State::kConnecting || state_ == State::kHandshaking ||
           state_ == State::kOpen;
  }
  bool connecting() const override {
    if (tcp_delegate_) return tcp_delegate_->connecting();
    return state_ == State::kConnecting || state_ == State::kHandshaking;
  }
  bool error() const override {
    if (tcp_delegate_) return tcp_delegate_->error();
    return state_ == State::kError;
  }
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
  // 接收数据(由 UrmaTransportContext::pollAndDispatch 调用,普通 SEND)
  void onRxBuf(std::unique_ptr<IOBuf> data) noexcept;

  // 独立 buffer 路由 — 从 recv buffer 前 24B 解析 header 路由
  // recv_buf: 接收到的 buffer 起始(header 24B + data)
  // recv_len: 实际接收字节数
  // ctx:      常驻 RxSlotCtx(deleter 重投凭据,见 UrmaTransportContext.h)
  // 由 UrmaTransportContext::handleRecvCqe 调用(仅 SEND 分支,MSG_DATA)
  // 返回值:  true  = buffer 所有权已随 IOBuf 转移(零拷贝,deleter 负责回收),
  //          false = 数据已拷出/未消费,收割线程立即回收 slot
  bool onIoMessageReceived(void* recv_buf, uint32_t recv_len,
                            RxSlotCtx* ctx) noexcept;

  // 保序/交付辅助(重排默认开启)
  // buildOwnedDataFrame: movable 帧构造(held++/transit-- 内完成),失败返回空
  std::unique_ptr<IOBuf> buildOwnedDataFrame(void* recv_buf, uint32_t recv_len,
                                             RxSlotCtx* ctx) noexcept;
  // deliverDataFrame: 帧交付(movable 优先,降级 memcpy),返回 owned
  bool deliverDataFrame(void* recv_buf, uint32_t recv_len,
                        RxSlotCtx* ctx) noexcept;
  // flushReorderBuffer: 重排缓冲强制放行(timeout=0),expected 重同步
  void flushReorderBuffer(uint16_t& expected,
                          std::vector<std::unique_ptr<IOBuf>>& out) noexcept;

  // 控制包/FREE 通知(由 UrmaTransportContext::handleRecvCqe SEND_IMM 分支调用)
  // type:       MSG_PULL_MEM / MSG_FREE
  // msg_id:     大包 msg_id(PULL/FREE 匹配)
  // remote_va:  PULL_MEM 时为远端 buffer 虚拟地址(从 imm_data 取);FREE 时为 0
  // data_size:  PULL_MEM 时为远端数据长度(从 recv_buf trailing 取);FREE 时为 0
  void onControlMsgImm(uint8_t type, uint16_t msg_id,
                       uint64_t remote_va, uint32_t data_size) noexcept;
  // 段表式 PULL_MEM(免拷贝,PullMemFragPacket 解析后调用,段表已拷出)
  void onPullMemFrags(uint16_t msg_id, const PullMemFragDesc* frags,
                      uint16_t frag_count, uint32_t total_size) noexcept;

  // 交付缓存数据给当前 read_cb_(支持 movable 和非 movable 两种模式)
  void deliverReadData() noexcept;

  // TX completion(由 UrmaCqHandler TX 模式调用)
  void pollTx() noexcept;
  void retryWrite() noexcept;

  // Phase 5: 处理其他连接 pollTx 收割到的本连接 READ CQE
  // JFS 池化后多连接共享 JFS,pollTx 可能收割到其他连接 post 的 READ CQE,
  // 通过 tx_ctx->socket_id 路由到对应连接调用此方法
  // 任务 B(H-2):per-EVB 独占后收割恒属主 EVB,无跨线程转发
  // 属主 EVB 后进入此方法(不变量 I1;同 EVB 短路同步调用)
  // msg_id: READ 的 msg_id(对应 PULL_MEM 的 msg_id)
  // status: CQE 状态(URMA_CR_SUCCESS 或错误码)
  void handleRemoteReadCqe(uint16_t msg_id, urma_status_t status) noexcept;

  // Phase 6: FC 消息接收入口(由 UrmaTransportContext::handleRecvCqe FC 分支调用)
  // recv_buf: payload 前 8B 携带对端 SocketId(无 64B headroom header)
  // recv_len: 实际接收字节数
  // 当前为占位实现,REQ/REP/RETURN/ACK 完整处理在 Phase 6 后续补全
  void onFcMessageReceived(void* recv_buf, uint32_t recv_len) noexcept;

  // Phase 6: FC 消息处理(REQ/REP/RETURN/ACK)
  void handleFcReq(uint64_t dst_socket_id, uint16_t credit_cnt,
                    uint16_t fc_seq) noexcept;
  void handleFcRep(uint16_t credit_cnt, uint16_t fc_seq) noexcept;
  void handleFcReturn(uint64_t dst_socket_id, uint16_t credit_cnt,
                       uint16_t fc_seq) noexcept;
  void handleFcAck(uint16_t credit_cnt, uint16_t fc_seq) noexcept;

  // Phase 6: FC 消息发送(通过 FC JFS 池)
  bool sendFcMsg(UrmaFcMsgType type, uint16_t credit_cnt,
                  uint16_t fc_seq) noexcept;

  // SDD-URMA-EVBEXCLUSIVE-001: 独占四件套下 JFS 位图/pin 逻辑整体删除
  // (I1:每 EVB 只有一个 JFS,pollAndDispatch 统一收割 TX+RX CQE)

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

  // ── 任务 D(H-5):路由注册 + 活跃连接记账同点绑定(幂等) ──
  // 注册 IO/FC 路由(幂等:同 socket_id 重注册返回既有 entry)+ 缓存安全句柄
  // + 首次绑定 incActiveConn。与 res_.remote_jetty 持有状态解耦:
  // 注册即绑定,注销(destroyUrmaResources)即解绑,降级连接天然对称。
  // 调用点: onHandshakeReady(客户端/服务端)+ UrmaHandshake::processRemoteHello
  // (服务端预注册,friend 访问)
  void registerRoutes() noexcept;

  // ── 任务 B(H-2):析构收尾(sock 置空协议) ──
  // destroyUrmaResources 已 erase 路由并置空 entry->sock —— 在途转发
  // lambda 判空跳过,socket 生命周期不再依赖 pending;本函数仅释放句柄引用
  void drainPendingForwards() noexcept;

  // ── 任务 F(H-7):关闭路径回调补全 ──
  // destroyUrmaResources 中 reset 前遍历 pending 队列,逐项 writeErr(END_OF_FILE)
  // (BigMsgGroup done 语义复用 scanBigMsgTimeout 放弃路径:组内恰好一次)
  void failPendingWrites() noexcept;

  // Phase 8: 降级 TCP — 握手失败时用 tcp_fd_ 创建 AsyncSocket 作为 delegate
  // 所有后续 AsyncTransport 操作委托给 delegate,对上层透明
  void fallbackToTcp();

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
  // 任务:去 9M tier + 打包分片免拷贝 ——
  //   发送侧 clone() 源链(零拷贝引用计数共享),遍历物理块生成段表,
  //   PullMemFragPacket 段表通告接收方;段数 > kMaxPullFrags 自动分批;
  //   接收方对每段独立 READ(WR 链),DMA 直写本地块 —— 全程无二次拷贝
  bool sendBigMsgByRead(WriteCallback* cb, std::unique_ptr<IOBuf> chain,
                        const std::shared_ptr<BigMsgGroup>& group) noexcept;
  // 段表式 PULL_MEM 控制包:SEND_IMM,SGE[0]=PullMemFragPacket(变长段表),
  // imm_data=total_size(诊断);控制包 ≤ 4K recv buffer
  bool postControlPacketFrags(uint16_t msg_id, uint16_t seq,
                              const PullMemFragDesc* frags,
                              uint16_t frag_count, uint32_t total_size) noexcept;
  // 段表式 READ(免拷贝):为每段分配本地 dst 块,WR 链一次 post
  // (每 WR complete,仅尾 WR 携带 ctx → 单 CQE 触发 onReadComplete)
  bool startRead(uint16_t msg_id, const PullMemFragDesc* frags,
                 uint16_t frag_count, uint32_t total_size) noexcept;
  // FREE 通知:SEND_IMM,SGE[0]=UrmaMsgHeader(24B),imm_data=msg_id
  // seq: 连接级发送序列号(每 WR 递增,保序用)
  bool postFreeNotify(uint16_t msg_id, uint16_t seq) noexcept;
  void onReadComplete() noexcept;
  void onReadFailed(urma_status_t status) noexcept;
  void scanBigMsgTimeout() noexcept;
  void drainRxPendingQueue() noexcept;

  // ── 保序:重排缓冲超时检查(ScanTimer 100ms tick 驱动) ──
  // 空洞超时(reorder_timeout_ms)未补齐 → 放行缓冲全部 + 重同步 + WARN
  void checkReorderTimeout() noexcept;

  // ── FC:周期 credit 归还(ScanTimer 100ms tick 驱动,任务:流控闭环) ──
  // peer_credit 超过保留阈值(初始授予量)时归还多余 → 接收方 idle 回升,
  // 修复"只借不还"导致的 credit 统计失效(peer_credit 单调下探)
  void maybeReturnCredit() noexcept;

  // ── 任务 E(H-6):大包超时扫描定时器(可取消 AsyncTimeout,替代 runAfterDelay) ──
  // runAfterDelay([this], 100) 不可取消,析构后 100ms 窗口内触发 = UAF;
  // 改为成员 AsyncTimeout(与 UrmaHandshake 同款模式),close/析构时 cancel。
  //
  // libevent 2.x event_del 无法撤回已就绪事件:如果 timer 事件已在 active
  // 队列中,reset() 析构后 vtable 破坏,event_base_loop 处理该事件时
  // 调用 timeoutExpired() → SIGSEGV。用 shared_ptr + alive 标志:
  //   - timeoutExpired 持有 shared_ptr(延长生命到回调结束)
  //   - alive_ 标志在 destroyUrmaResources 中置 false → 回调内短路返回
  //   - scan_timer_.reset() 释放 unique_ptr,但 shared_ptr 引用计数 >0
  //     (event_base active 队列持有),对象存活到回调结束自然析构
  class ScanTimer : private AsyncTimeout,
                     public std::enable_shared_from_this<ScanTimer> {
   public:
    explicit ScanTimer(UrmaAsyncSocket* sock)
        : AsyncTimeout(sock->evb_), sock_(sock) {}
    void arm() { scheduleTimeout(100); }  // 100ms 周期(设计 §4.7.8)
    void kill() noexcept { alive_.store(false, std::memory_order_release); }
   private:
    void timeoutExpired() noexcept override;
    UrmaAsyncSocket* sock_;
    std::atomic<bool> alive_{true};
  };
  std::shared_ptr<ScanTimer> scan_timer_;

  EventBase* evb_;
  int tcp_fd_{-1};
  State state_{State::kIdle};
  ReadCallback* read_cb_{nullptr};

  // 独立 buffer 路由:SocketId(接收方据此路由到本 socket)
  SocketId socket_id_{};
  // per-连接发送序列号(每 post 一个 WR 递增,DATA/PULL/FREE 都分配,保序用)
  std::atomic<uint16_t> send_seq_{0};
  // per-连接期望接收序列号(保序校验,与 send_seq_ 对应,接收侧可选校验)
  std::atomic<uint16_t> expected_seq_{0};
  // 保序重排缓冲(默认开启,无开关):
  // 未来帧暂存按序交付,空洞超时兜底放行;仅属主 EVB 访问,无锁
  UrmaSeqReorder rx_reorder_;
  // 保序统计(诊断打点):乱序入缓冲/超时放行/重复丢弃计数
  uint64_t reorder_buffered_{0};
  uint64_t reorder_released_{0};
  uint64_t reorder_dup_dropped_{0};

  // SDD-URMA-EVBEXCLUSIVE-001: 池化 JFS 位图/pin 逻辑整体删除
  // (I1:每 EVB 只有一个 JFS,FIFO 由硬件保证)

  folly::IOBufQueue read_queue_;

  std::unique_ptr<ConnectCallback> connect_cb_;
  AsyncSocket::UniquePtr tcp_socket_;

  // Phase 8: 降级 TCP delegate(握手失败时创建,接管所有 AsyncTransport 操作)
  AsyncSocket::UniquePtr tcp_delegate_;

  UrmaTransportContext* shared_{nullptr};  // per-EVB 独占四件套(I1)
  UrmaHandshake* handshake_{nullptr};
  UrmaResource res_;
  uint32_t peer_rx_buf_size_{4096};

  std::unique_ptr<TcpHandshakeHandler> tcp_handshake_handler_;

  // ── 任务 B/D:H-2 转发 drain + H-5 记账绑定 ──
  SafeRouteEntryPtr route_entry_;   // 本连接路由安全句柄(注册时缓存,析构 drain 依据)
  bool active_conn_bound_{false};   // 活跃连接记账绑定标记(幂等 inc/dec)
  // ── 任务 D(H-5):destroyUrmaResources 幂等标志 ──
  // closeNow 与析构双调消除:普通 bool + EVB 断言(隐式同 EVB 串行依赖显式化)
  bool resources_destroyed_{false};

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
  // 任务:1M 分片 —— 单次 READ 的最大分片 SGE 数(硬件 max_jfs_sge=13,
  // 预留 src 块计数余量;8M+thrift 头 ≈ 9 块,benchmark 上限 8M 覆盖;
  // 超过此限的更长消息自动分段多次 PULL_MEM,接收方 rx_pending_queue_ 串行保序)
  static constexpr uint32_t kMaxReadFragSge = 12;

  struct PendingBigMsg {
    std::shared_ptr<IOBuf> iobuf;   // 源链 clone(零拷贝,引用计数保活)
    std::vector<PullMemFragDesc> frags;  // 段表(重发用,块地址不变)
    uint32_t total_size{0};
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
    // 段表式 PULL(免拷贝):段描述副本 + 总长(旧 va/size 单段语义废弃)
    std::vector<PullMemFragDesc> frags;
    uint32_t total_size = 0;
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

  // Phase 6: FC 流控状态(per-连接)
  UrmaFcState fc_state_;

  // Phase 6: FC 发送缓冲区(16B,复用避免每次分配)
  // sendFcMsg 中序列化后 post 到 FC JFS
  uint8_t fc_send_buf_[FC_MSG_LEN] __attribute__((aligned(16)));

  static constexpr int POLL_BATCH_MAX = 64;
};

} // namespace folly

#endif // FOLLY_HAVE_URMA