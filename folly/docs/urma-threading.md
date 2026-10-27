# URMA 线程模型与生命周期不变量

> SDD-URMA-HFIX-001(v1.0)产物 · 高风险 H-1~H-7 修复的目标线程模型固化。
> 本文是 urma 传输层多 EventBase 部署的**权威行为说明**,评审与新增代码必须服从以下不变量。

## 1. 目标线程模型:四条不变量

```
I1 socket 状态隔离   UrmaAsyncSocket 全部可变状态仅在其所属 EventBase 线程访问
I2 路由表并发安全    进程级路由表 = 16 分片读写锁(Synchronized<map, SharedMutex>)
                     + 安全句柄(entry 引用计数保活)
I3 跨线程安全传递    跨线程仅传递 {socket_id, evb, SafeRouteEntry};
                     禁止裸指针跨线程逃逸
I4 RX 所有权随交付   RX buffer 所有权随 IOBuf 转移,slot 重投由释放回调驱动,
                     账实严格一致(recv_posted + transit + held + 回收栈深 == max_water)
```

违反任何一条即回到 H-1~H-7 的故障模式(rehash 崩溃 / 回调错线程 / DMA 数据覆盖 / RX 饿死 / 计数下溢 / UAF / 回调悬挂)。

## 2. 线程角色与合法访问(任务 A/B,H-1/H-2)

| 线程 | 角色 | 允许触碰的对象 |
|---|---|---|
| 属主 EVB(socket 的 evb_) | socket 一切可变状态、上层 read/write 回调 | UrmaAsyncSocket 全部状态(唯一写者,I1) |
| 任意 EVB 收割线程 | `UrmaJfrPool::pollAndDispatch/harvestTxAndDispatch` | 进程级池(JFR/JFS,库保证线程安全)、分片路由表(读写锁) |

- **路由表**(`UrmaRouteTable.h`):16 分片,`find` 持分片读锁拷出 `SafeRouteEntryPtr` 后立即放锁,后续无锁使用;`erase`(destroyUrmaResources 第一步)只摘表不等待;entry 生命周期 = map 内引用 + 已拷出引用,最后一个释放时析构。
- **同 socket_id 幂等重注册**:服务端握手预注册 + onHandshakeReady 重注册返回**同一 entry**(句柄身份稳定,在途 pending 计数不丢失)。
- **TX CQE(READ)属主转发协议**(`UrmaJfrPool::dispatchReadCqe`):
  - 同 EVB → 同步短路调用(零延迟,保住小包 P50 路径);
  - 跨 EVB → `entry->pending++` → **`runInEventBaseThread`** 转发回属主线程;`msg_id/status` 在 `delete tx_ctx` **之前**按值捕获(评审要点①);
  - lambda 内**先回调后 pending--**;
  - **sock 置空协议**:连接销毁时(destroyUrmaResources)属主 EVB 将 `entry->sock`(原子指针)置 nullptr;在途 lambda(恒在属主 EVB 执行,与置空天然串行)判空跳过回调 —— **socket 生命周期不依赖 pending**,pending 仅作 entry 保活计数。替代原"析构自旋等待 pending==0"方案(自旋会阻塞本 EVB 队列,在途 lambda 永不执行 → 10ms 超时放行 → UAF,close_loop 高压下实测触发)。
- **跨线程调度规则(实测教训,新增代码必须遵守)**:`EventBase::runInLoop(Func)` **非线程安全**(无锁 push_back 到 `loopCallbacks_`,`dcheckIsInEventBaseThread` 仅 debug 生效)——**同 EVB 线程**才可用;**跨线程一律 `runInEventBaseThread`**(notification queue 通道)。违反后果:intrusive 链表与属主 EVB 的 runLoopCallbacks 并发损坏 → SIGSEGV(4 IO 线程 8M 场景 gdb/Release 偏移双重实证)。
- **SEND/CTRL 类 CQE**:资源回收(releaseIoJfs + delete ctx)只触碰进程级池 → 收割线程就地完成,零转发(保持小包路径性能)。ctx 内 send_data 的 IoBufBlock 归还**收割线程 TLS 缓存**,内存为全局 MR 池,跨线程归还功能正确(块级缓存亲和性损失是性能问题而非正确性问题)。
- **握手对象销毁**:handler/TcpHandshakeHandler 与 UrmaHandshake 均不得在其调用栈内同步 delete(handlerReady → onDataAvailable → onHandshakeReady 的在途对象)——统一"先摘 epoll/置空指针,再 `runInLoop` 异步 delete"(ASAN 实测 heap-use-after-free 修复)。
- **析构时序前提**(上层契约):socket 的 delete 只发生在 EVB 回调栈之外;该前提以 `DCHECK(evb_->inRunningEventBaseThread())`(destroyUrmaResources)显式固化,CI debug 构建常开。

## 3. RX 所有权交付(任务 C,H-3/H-4)

### 3.1 交付与回收

- `onIoMessageReceived` DATA 路径:`IOBuf::takeOwnership(data_ptr, len, deleter, ctx)`,**所有权随 IOBuf 转移**;deleter 在**任意线程**执行:held-- + slot 入 per-JFR 无锁回收栈(`UrmaRecycleStack`,tagged Treiber 栈,防 ABA)。
- `RxSlotCtx{pool, jfr, slot}` 常驻于 `RxBuf`(rx_bufs 在 init 后不扩容,**生命周期长于一切 IOBuf**,评审要点②:禁止指向 stack/临时对象)。
- 控制包(SEND_IMM,`onControlMsgImm` 按值解析)与 FC(`onFcMessageReceived` 按值反序列化)**立即回收**——数据已拷出,无持有风险。
- **重投触发**:① `harvestAndDispatch/harvestFcAndDispatch` 每轮收割后 `repostRecycled` 批量重投(摊薄系统调用);② per-UrmaSharedJfr **10ms RepostTimer** 兜底(防"无新事件导致回收栈滞留");③ dormant 前排空 + wakeup 后排空。JFR 为进程级共享,多个 UrmaSharedJfr 可能选定同一 JFR,重复排空幂等无害。

### 3.2 记账模型(不变量 I4)

```
收割 CQE:      transit++ → recv_posted--      (WR 离开 RQ,事实计数,无截断)
movable 交付:   held++ → transit--              (socket 侧,先加后减序)
deleter 回收:  slot 入栈 → held--               (任意线程)
立即回收:      slot 入栈 → transit--            (收割线程)
重投:          栈弹出 N → post 成功 N 个 → recv_posted+=N
                                                                      (旧 REPOST_SLOTS_CAP=256 截断根因已结构性消除)
不变式:  recv_posted + transit_cnt + held_count + 回收栈深 == max_water
```

- 转移采用**先加后减序**,瞬态偏差只朝 M+1 方向;重投批次窗口(kRepostBatchMax=64)存在 M-k 短暂偏差 —— 故诊断采用**连续 5 次采样越界才告警**(`checkRxAccounting`,10ms 周期;debug 构建 DCHECK,release 构建 ERR 日志),捕获 H-4 类系统性 slot 泄漏(截断类 bug 一次即永久漂移),零误报。
- 批量 post 失败按 `bad_wr` 定位成功前缀(`postRecvWrBatchCount`),只对成功数递增 recv_posted,失败 slot 退回回收栈,账实严格一致。

### 3.3 held 预算(安全阀,风险 R1)

- movable 交付仅当 `held_count < max_water - min_water`(per-JFR 原子计数);
- 超预算 → 该包降级 memcpy(`copyBuffer`,等价旧行为,正确性不变,性能局部退化 + DBG 打点);
- 上限保证 RQ 永远保有 ≥ min_water 个 buffer,杜绝 RNR 风暴;
- held_count 持续高位 = 上层异常持有(如缓冲泄漏),打点告警。

## 4. 活跃连接记账与休眠状态机(任务 D,H-5)

- **绑定不变式**:首次路由注册(`registerRoutes`,幂等)→ `incActiveConn`;路由注销(destroyUrmaResources,幂等)→ `decActiveConn`。与 `res_.remote_jetty` 持有状态**解耦**,降级 TCP 连接天然对称(修复"只减不增"下溢)。
- `destroyUrmaResources` 幂等化(`resources_destroyed_`),消除 closeNow+析构双调下溢。
- `active_conn_count_` 为**普通 uint32_t** + EVB 断言(同 EVB 串行的隐式依赖显式化);`dormant_` 保留原子(getOrCreate 跨线程读)。
- `dormant()` 防御性重验:unregister 前若 `active_conn_count_ > 0` 立即返回(窗口期新连接防假死)。

## 5. 定时器(任务 E,H-6)

- `scanBigMsgTimeout` 由成员 `ScanTimer : private AsyncTimeout`(100ms 自续)驱动;close/析构时 `scan_timer_.reset()` 取消 —— 消除 `runAfterDelay([this])` 析构后 100ms UAF 窗口。
- 回收栈重投兜底 `RepostTimer`(UrmaSharedJfr,10ms 自续),dormant 时停用、wakeup 重启。

## 6. 关闭路径(任务 F,H-7)

- `destroyUrmaResources` 在 reset 前遍历 `pending_write_` 与 `pending_big_msg_`,逐项 `writeErr(END_OF_FILE)`;BigMsgGroup `done` 语义保证**组内恰好一次**(跨队列去重)。
- `read_cb_` 的 `readEOF()` 通知不在 H-1~H-7 范围(与 ThriftServer 关闭序列协同,登记至 M 系列 SDD)。

## 7. 边界与既有约定(风险 R4)

- 进程退出前所有连接 close(fini 顺序既有约定);EVB 停止期间残余转发 lambda 不执行 → 析构 drain 10ms 超时 WARN 放行。
- `urma_poll_jfc`/`urma_post_jfr_wr` 线程安全(每个 CQE 只被一个线程收割),进程级池并发访问由库保证。
- FC 流控协议(REQ/REP/RETURN/ACK)为半成品(M-2),本系列修复不改其语义。

## 8. 文内引用

| 文件 | 职责 |
|---|---|
| `urma/UrmaRouteTable.h` | I2/I3:分片路由表 + SafeRouteEntry |
| `urma/UrmaRecycleStack.h` | I4:无锁回收栈(tagged Treiber,防 ABA) |
| `urma/UrmaJfrPool.h/.cpp` | I2/I3/I4:路由委托、READ CQE 转发、记账与重投 |
| `urma/UrmaAsyncSocket.h/.cpp` | I1/I4:零拷贝交付、析构 drain、幂等销毁、writeErr 补全、ScanTimer |
| `urma/UrmaSharedJfr.h/.cpp` | 休眠状态机、RepostTimer 兜底 |
| `urma/test/` | 路由表并发、回收栈单测(不依赖 URMA 硬件) |
