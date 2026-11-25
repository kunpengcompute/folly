# URMA 传输层设计说明

> 本文描述 folly `AsyncTransport` 的 URMA 后端（`folly/io/async/urma/`）的总体结构、
> 线程模型、生命周期不变量、RX 所有权与记账、流控、大包路径与握手降级。
> 评审与新增代码必须服从本文的不变量；实现变更时同步更新本文。

## 1. 总体结构

```
上层(fbthrift ThriftServer / 客户端)
        │  AsyncTransport 接口
        ▼
UrmaAsyncSocket ──── UrmaHandshake        (TCP fd 上完成 v2 握手,失败降级 AsyncSocket)
        │
        ├── UrmaTransportContext (per-EVB,四件套 + 4 个 CqHandler + 定时器)
        │        ├── UrmaCqHandler × 4    (io_rx / io_tx / fc_rx / fc_tx)
        │        ├── UrmaRouteTable       (进程级,16 分片)
        │        ├── UrmaRecycleStack     (per-JFR RX slot 无锁回收)
        │        └── UrmaSeqReorder       (可选,默认关)
        │
        ├── UrmaInit (进程级:urma_init + MR 池 + IOBuf 分配注入)
        └── UrmaFlowControl.h (GRANT / BLOCKED 协议结构与 per-连接状态)
```

- `UrmaInit::init()` 进程级一次性完成 URMA 库初始化、MR 内存池创建，并通过
  `IOBuf::setIoBufBlockAllocFree` 把 MR 池注入 folly 的 IOBuf 分配路径；
  只有来自 MR 池的内存才能被 URMA 硬件 DMA 访问。
- `UrmaTransportContext` 每个 IO EventBase 一个，首连接时惰性创建，EVB 生命周期内常驻。
- `UrmaAsyncSocket` 是 `AsyncTransport` 实现，一个连接一个实例，归属单一 EVB。
- 全部 URMA 传输代码由 `FOLLY_HAVE_URMA` 隔离；未开启时该后端不参与构建。

## 2. 线程模型：四条不变量

```
I1 属主 EVB 独占     UrmaAsyncSocket 的全部可变状态只在其 evb_ 线程访问
I2 四件套同线程      per-EVB 的 JFS/JFR/JFC/JFCE 及水位、计数、定时器均在该 EVB 线程,
                     因此 sq_inflight / recv_posted / credit_* 等使用普通 uint32_t,不加锁
I3 跨线程安全传递    跨线程只传 {socket_id, evb, SafeRouteEntry};
                     禁止裸指针跨线程逃逸;跨线程调度一律 runInEventBaseThread
I4 RX 所有权随交付   RX buffer 所有权随 IOBuf 转移,slot 由释放回调送回回收栈;
                     recv_posted + transit + held + 回收栈深 == rx_allocated
```

违反任一条会回到已修复的故障模式：路由表 rehash 崩溃、回调在错误线程执行、
DMA 数据被覆盖、RX 饿死、计数下溢、UAF、回调悬挂。

### 2.1 线程角色

| 线程 | 允许触碰的对象 |
|---|---|
| socket 属主 EVB | 该 socket 全部状态；其所属 `UrmaTransportContext` 的四件套、水位、定时器 |
| 任意线程（IOBuf deleter） | 只做 `held_count--` 与 `UrmaRecycleStack::push`（原子操作） |
| 任意 EVB 的 CQ 收割线程 | 本 EVB 的 `UrmaTransportContext`；进程级路由表（分片读写锁） |

### 2.2 跨线程规则

- `EventBase::runInLoop(Func)` 非线程安全（无锁写入 `loopCallbacks_`，`dcheckIsInEventBaseThread`
  仅 debug 生效）：**同 EVB 线程**才可使用；**跨线程一律 `runInEventBaseThread`**。
- 跨 EVB 转发的最小单元是 `{socket_id, evb, SafeRouteEntryPtr}`，不传 socket 裸指针。
- 远端 READ 的 TX CQE 需要回到 socket 属主 EVB：
  - 同 EVB：同步短路调用，保住小包 P50 路径；
  - 跨 EVB：`entry->pending++` → `runInEventBaseThread` 转发；`msg_id/status` 在
    `delete tx_ctx` 之前按值捕获；lambda 内先回调、后 `pending--`。
- SEND / 控制包类 CQE 的资源回收只触碰本 EVB 的四件套，收割线程就地完成，零转发。

### 2.3 socket 置空协议

连接销毁时，属主 EVB 在 `destroyUrmaResources` 中把 `SafeRouteEntry::sock`（原子指针）
置为 `nullptr`；在途转发 lambda 恒在属主 EVB 执行，与置空天然串行，判空后跳过回调。
因此 **socket 生命周期不依赖 `pending`**，`pending` 只用于保活 entry。
这替代了"析构自旋等待 `pending==0`"的方案——自旋会阻塞本 EVB 队列，导致在途 lambda
永不执行，最终超时放行并触发 UAF。

## 3. per-EVB 传输上下文（四件套）

每个 IO EVB 独占：

| 资源 | 数量 | 说明 |
|---|---|---|
| IO JFS | 1 | 数据发送队列 |
| IO JFR | 1 | 数据接收队列（RX buffer 池） |
| FC JFS | 1 | 流控消息发送队列 |
| FC JFR | 1 | 流控消息接收队列 |
| IO TX JFC / JFCE | 1 / 1 | 独立 TX 完成队列与 fd |
| IO RX JFC / JFCE | 1 / 1 | 独立 RX 完成队列与 fd |
| FC TX JFC / JFCE | 1 / 1 | 独立 |
| FC RX JFC / JFCE | 1 / 1 | 独立 |

- 创建即独占：无池化、无领用、无共享降级；连接全断也不销毁（EVB 常驻），进程 `fini` 统一销毁。
- 每 EVB 4 个 `UrmaCqHandler`，把 4 个 JFCE fd 注册到 epoll：

  | Handler | 调用 |
  |---|---|
  | `io_rx_cq_handler` | `pollAndDispatch(false, false)` |
  | `io_tx_cq_handler` | `pollAndDispatch(false, true)` |
  | `fc_rx_cq_handler` | `pollAndDispatch(true, false)` |
  | `fc_tx_cq_handler` | `pollAndDispatch(true, true)` |

- 收割流程（中断模式）：`wait → poll → ack → rearm → poll`（第二遍 poll 防 rearm 窗口漏收）。
  轮询模式（`URMA_POLL_MODE=1`）跳过 wait/ack。
- SQ 配额：`sq_inflight` 为同线程普通计数；WR 链按 frag 数批量申请/归还
  （`tryAcquireIoSqN` / `releaseIoSqN`）。配额不足时登记 `SqWaiter`，配额归还时唤醒重试。

## 4. 路由表与并发安全

`UrmaRouteTable` 是进程级 `SocketId → UrmaAsyncSocket` 映射：

- 16 个分片，每分片 `folly::Synchronized<unordered_map, SharedMutex>`。
- `SafeRouteEntry` 独立分配、`shared_ptr` 保活：
  - `sock`：原子指针，置空协议见 §2.3；
  - `evb`：属主 EventBase，插入后不可变；
  - `pending`：在途跨线程转发计数（仅保活 entry）。
- `find` 持分片读锁拷出 `SafeRouteEntryPtr` 后立即放锁，后续无锁使用；
  entry 的最后一个引用释放时析构，消除"find 返回裸指针后 socket 被销毁"的 UAF 窗口。
- `insert` 幂等：同 `socket_id` 重复注册返回既有 entry，保证句柄身份稳定，
  在途 `pending` 计数不丢失。
- `erase` 只摘表，不等待在途引用。
- IO 与 FC 各有一张路由表（`route_table_` / `fc_route_table_`）。

## 5. RX 所有权与记账（不变量 I4）

### 5.1 交付与回收

- DATA 路径使用 `IOBuf::takeOwnership(data_ptr, len, deleter, ctx)`，所有权随 IOBuf 转移。
- deleter 可能在**任意线程**执行：`held_count--` 后把 slot `push` 进 per-JFR 无锁回收栈
  （`UrmaRecycleStack`，tagged Treiber 栈，用 32 位 tag 防 ABA；per-slot next 数组零堆分配）。
- `RxSlotCtx` 常驻 `EvbJfr::RxBuf`，生命周期长于一切 IOBuf；另带
  `shared_ptr<atomic<bool>> alive`，EVB 析构后 deleter 检查该标志并短路，避免 UAF。
- 控制包（SEND_IMM）与 FC 消息在解析后立即回收：数据已拷出，无需持有 slot。

### 5.2 重投

- 收割线程每轮 `harvest` 后调用 `repostRecycled` 批量重投，摊薄系统调用；
- per-EVB `RepostTimer`（默认 10ms，`URMA_REPOST_TIMER_MS` 可覆盖）兜底，
  防止"无新事件导致回收栈滞留"；
- dormant 前排空、wakeup 后排空。

### 5.3 记账

```
收割 CQE:      transit++ → recv_posted--
movable 交付:  held++ → transit--              (先加后减)
deleter 回收:  slot 入栈 → held--
立即回收:      slot 入栈 → transit--
重投:          栈弹出 N → post 成功 N 个 → recv_posted += N

不变式: recv_posted + transit_cnt + held_count + 回收栈深 == rx_allocated
```

- 转移采用先加后减序，瞬态偏差只朝"多"的方向；重投批次窗口内允许 `kRepostBatchMax + 16`
  的松弛。
- `checkRxAccounting` 在 10ms 周期检查：偏差超过松弛且**连续 5 次**越界才告警，
  捕获系统性 slot 泄漏，避免批次窗口误报。
- 批量 post 失败时按 `bad_wr` 定位成功前缀，只对成功数递增 `recv_posted`，
  失败 slot 退回回收栈。
- held 预算：movable 交付仅当 `held_count < max_water - min_water`；超预算时该包降级
  memcpy（`copyBuffer`），保证 RQ 始终保有 ≥ `min_water` 个 buffer，杜绝 RNR 风暴。

## 6. RX buffer 阶梯式按需扩容

- `rx_bufs` 一次性 `resize(rx_depth)`，之后只对前缀做惰性初始化（分配 data、填 sge/wr），
  **绝不重新分配**——已 post 的 `wr.src.sge` 被硬件异步引用，vector 重分配会让指针悬垂。
- `rx_capacity == rx_bufs.size() == rx_depth`（稳定上限），`rx_allocated` 表示已初始化并
  参与流通的 slot 数。
- 初始只分配/投递 `rx_initial`（默认 1024）；使用率达到 `rx_high_water_pct`（默认 80%）
  时按 `rx_grow_step`（默认 512）追加，直到 `rx_depth`。
- 使用率口径：`1 - recv_posted / rx_allocated`（已占用、不在 RQ 等待的比例）。
- 检查点：
  1. 握手授信前（`onHandshakeReady` 分配初始窗口之前）；
  2. `CreditTimer` 1ms tick 补令牌之前。
- 部分投递失败：`rx_allocated` 推进到本次区间末尾，未投递 slot 全部推回回收栈，
  由后续 `repostRecycled` 补投；恒等式仍成立。
- `repostRecycled` 的目标水位是 `rx_allocated`，不是 `max_water`；
  `[rx_allocated, rx_capacity)` 尚未分配，不能补投。

## 7. 流控：receiver-driven GRANT / BLOCKED

- FC 走独立的 JFR/JFS，IO 通道故障不影响流控。
- 两种消息：
  - `GRANT`：接收方 → 发送方，`delta` 可负（负值回收窗口）；
  - `BLOCKED`：发送方 → 接收方，撞底通知（纯通知，`delta` 忽略）。
- FC payload 固定 16B：

  ```
  ┌───────────────┬──────────┬───────────┬─────────┬──────────┐
  │ dst_socket_id │ msg_type │   delta   │ fc_seq  │ reserved │
  │    8 bytes    │  1 byte  │  4 bytes  │ 2 bytes │  1 byte  │
  └───────────────┴──────────┴───────────┴─────────┴──────────┘
  ```

  接收方从 payload 前 8B 取 SocketId，查 FC 路由表定位连接。
- 记账（per-JFR，同线程）：

  ```
  credit_idle + credit_allocated == recv_posted
  授信 g : idle -= g, allocated += g
  消费 1 : allocated -= 1, recv_posted -= 1
  补投 Δ : idle += Δ, recv_posted += Δ
  ```

- 1ms `CreditTimer` 遍历 `io_jfr_.conns`：按 EWMA 接收速率调整每个连接的窗口、
  回收空闲连接额度（空闲阈值默认 100ms）、池占用率超过阈值时回收。
- 默认参数：初始窗口 16、保底窗口 8；发送方撞底发 `BLOCKED`，接收方补发 `last_grant`
  并翻倍，上限 `fc_min_window × 16 = 128`。
- 发送侧 `peer_credit <= 0` 时真正停发，从源头消除 RNR。
- 连接关闭时归还其未消费额度（`reclaimConnCredit`），防止 credit 泄漏。
- 常驻监控：`URMA_FC_STATS_MS`（默认 1000ms，0=关闭）周期输出
  `credit_idle / credit_allocated / recv_posted / pool 使用率`，并检查恒等式。

## 8. 大包：单边 READ

- 判定条件：`节点长度 + sizeof(UrmaMsgHeader)(24B) > peer_rx_buf_size_`，
  不是固定 4KB 阈值；`peer_rx_buf_size_` 来自握手对端通告。
- 流程：
  1. 发送方发 `PULL_MEM` 控制包（`SEND_IMM`，imm 携带远端 VA / 长度）；
  2. 接收方对发送方内存发起单边 `READ`；
  3. 接收方完成后发 `FREE` 通知，发送方回收发送 IOBuf。
- READ 仅支持单 src/dst SGE，超过 `kMaxReadFragSge`（12）的大消息分段多次 PULL，
  接收方 `rx_pending_queue_` 串行保序。
- `BigMsgGroup` 完成组：一次 `writeChain` 内的大包共享一个组，组内
  `writeSuccess` / `writeErr` 恰好一次（跨队列去重）。
- 超时重发：`ScanTimer` 每 100ms 扫描，7s 未完成重发控制包，最多
  `kCtrlMsgMaxRetries = 3` 次，之后按失败处理。

## 9. 握手与降级

- v2 二进制协议，通过 accept/connect 得到的 TCP fd 异步驱动：
  - `98B Hello`（4B magic + 94B body）+ `ACK`，两轮；
  - 交换：`jfr_id` / `fc_jfr_id` / EID / UASID / seg 信息 / `socket_id` / capability flags。
- 超时默认 `handshake_timeout_ms = 30000`（`UrmaConfig` 可配置）。
- 对端不支持所需 capability、导入失败或超时 → `fallbackToTcp()`，改用 `AsyncSocket`，
  业务无感。
- 握手对象（handler / `UrmaHandshake`）不得在自身调用栈内同步 delete：
  先摘 epoll / 置空指针，再 `runInLoop` 异步 delete。

## 10. 生命周期与关闭

- `destroyUrmaResources` 幂等（`resources_destroyed_`），并以
  `DCHECK(evb_->inRunningEventBaseThread())` 固化"delete 只发生在 EVB 回调栈之外"的上层契约。
- 关闭时遍历 `pending_write_` 与 `pending_big_msg_`，逐项 `writeErr(END_OF_FILE)`；
  `BigMsgGroup::done` 保证组内恰好一次。
- 活跃连接记账与路由注册绑定：`registerRoutes` → `incActiveConn`，
  `destroyUrmaResources` → `decActiveConn`，两者幂等；降级 TCP 的连接天然对称。
- `active_conn_count_` 为同线程普通计数；`dormant_` 为原子（跨线程可读）。
- 连接全断时进入 dormant（摘除 CqHandler、停止定时器），不销毁四件套；
  新连接到来时 wakeup。

## 11. 定时器

| 定时器 | 周期 | 职责 |
|---|---|---|
| `CreditTimer` | 1ms（`fc_grant_period_ms`） | 流控窗口调整、空闲回收、扩容检查 |
| `RepostTimer` | 10ms（`URMA_REPOST_TIMER_MS`） | 回收栈兜底重投 |
| `ScanTimer` | 100ms | 大包超时重发、保序缓冲超时放行 |

- 三者均为 `AsyncTimeout` 子类，自续；关闭/析构时显式取消。
- `ScanTimer` 由 `shared_ptr` 持有并带 `alive_` 标志，消除"析构后仍被回调"的 UAF 窗口。

## 12. 边界与既有约定

- 进程退出前所有连接先 close，再 `UrmaInit::fini()`。
- `urma_poll_jfc` / `urma_post_jfr_wr` 的线程安全由库保证：每个 CQE 只被一个线程收割。
- `UrmaSeqReorder` 为防御性保序（默认关闭，`URMA_RX_REORDER=1` 开启）：
  空洞入缓冲（上限 64）、超时放行并重同步、重复/过时丢弃。URMA SEND 本身可靠有序，
  正常路径不应出现乱序。
- URMA 设备为独占资源：同一设备被其他进程持有时，本进程的 post 可能无 CQE 且不报错。
  部署与压测前需确认设备未被占用。
- `UrmaConfig` 字段变更会改变内嵌它的 `ThriftServer` 布局，修改后必须同步重编 fbthrift。

## 13. 文件职责

| 文件 | 职责 |
|---|---|
| `urma/UrmaInit.h/.cpp` | 全局初始化、MR 内存池、IOBuf 分配注入、远端 import |
| `urma/UrmaConfig.h` | 配置 POD 与硬件上限 clamp |
| `urma/UrmaTransportContext.h/.cpp` | per-EVB 四件套、CQ 收割分派、RX 记账/扩容、流控定时器 |
| `urma/UrmaCqHandler.h/.cpp` | JFCE fd → epoll → `pollAndDispatch` |
| `urma/UrmaAsyncSocket.h/.cpp` | `AsyncTransport` 实现、零拷贝交付、大包 READ、关闭路径 |
| `urma/UrmaHandshake.h/.cpp` | v2 握手、资源导入、TCP 降级 |
| `urma/UrmaHeader.h` | 24B 消息头、消息类型、64 位 SocketId |
| `urma/UrmaResource.h` | 远端句柄、`TxCompletionCtx` |
| `urma/UrmaRouteTable.h` | 16 分片路由表、`SafeRouteEntry` |
| `urma/UrmaRecycleStack.h` | per-JFR 无锁回收栈 |
| `urma/UrmaSeqReorder.h` | 可选 RX 保序重排 |
| `urma/UrmaFlowControl.h` | FC 消息格式与 per-连接状态 |
| `urma/test/` | 路由表 / 回收栈 / 保序重排单测（不依赖 URMA 硬件） |
