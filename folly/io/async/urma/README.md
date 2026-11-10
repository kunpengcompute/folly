# URMA 传输与注册内存池

本实现为 Folly/fbthrift 提供 URMA 异步字节流，包含小包合并、大包单边 READ 和注册内存池。TCP 只用于握手和连接存活；正文使用 SEND 或 READ。

## 内存池

UrmaInit 管理进程级注册 region 和分档公共空闲块，块在分配时确定用途，不按 region 永久分区。默认启用 TLS 注册分配与弹性池：启动注册 `pool_size=256 MiB`，容量不足时后台先回收可用 TLS 缓存，再按需扩容至 `pool_max_size=512 MiB`；空闲后可缩回软容量。只有全部持有结束且注销成功，才扣除注册容量。设置 `elastic=false` 可使用固定池；初始注册量不随硬上限增加。

两条分配路径共用池：

- 序列化：默认 `zero_copy=true`，`IOBuf::create(n)` 从 TLS 块切分 slice。保持 capacity ≥ n、length = 0、buffer = data，不插入隐藏传输头；未命中可回退普通内存。
- RX / READ：直接申请独占完整块。DATA 接收后把原块交给拥有型 IOBuf，接收槽立即尝试换块补投；最后一个共享引用释放才归还原池及整块预算，跨线程释放只通知 owner。

分配、DMA 和远端 READ 导出分别持有，clone 不重复计算物理容量。RX、TX、READ 描述、READ 源、READ 目标及 FREE 确认存储分别受预算约束。注册池上限不是进程 RSS 上限。

默认大块分档包含 `8 MiB + 64 KiB`，让 8 MiB 正文附带序列化和 TLS 管理信息时无需占用 16 MiB 档；这 64 KiB 是分配容量余量，不改变线上报文、READ 阈值或注册总量。默认 READ 源预算仍为 102 MiB，按实际块容量计费。自定义 `classes` 会替换整个默认列表，需为实际序列化容量保留合适档位；仅提高 `pool_size` 不会自动提高各用途 `budgets`。池外输入仍合法，资源不足或不满足直接 READ 条件时可能使用 SEND，不能把开启 READ 等同于所有大包必然 READ。

## 收发与回收

每条连接独占 JFS、JFR、JFC 和接收 jetty。同一 EventBase 上的不同连接也分别创建硬件队列，仅注册内存池在进程内共享。SDK 的 `share_jfr` 标志用于把该连接自己的 JFR 绑定到自己的接收 jetty，不将它交给其他连接。队列创建、导入和销毁通过后台任务执行。

DATA/PULL/FREE 恢复每连接 SEND 的 `place_order=URMA_STRONG_ORDER`、`comp_order=1`，JFS 使用与这些标志兼容的 `outorder_comp=0`。READ 不设置 SEND 的事务序标志。CQE 用各自的上下文结算，软件序号继续处理失败重试、字节交付及整次写回调的保序；硬件排序不能代替这些记账。去掉信用协议后，持续纯 SEND 的可靠性须单独验证，不能用仅开启 READ 的结果代替。

RX 按 `rx_depth` 预投，成功投递才增加已投数。缺块保留待补状态；上层长期持有 IOBuf 时，已完成的 DATA 暂存在原接收槽，归还通知使其继续交付。没有信用申请、授予、归还报文，也没有专用 FC 队列或信用重试定时器。对端接收 WQE 耗尽时，SDK 可能返回 RNR 错误；不能把 RDMA 设备 `rnr_retry=7` 的无限重试语义套用到所有 UB 设备。对于明确表示未被对端接收的 RNR，保留原 DATA/PULL/FREE、序号和持有，按顺序逐条恢复被拒绝的发送，间隔至少 1 ms 重试，最多等待 5 秒；期间暂停新的 DATA/PULL 提交，反向 READ 及其 FREE 可使用预留的本地 SQ 空位，避免一个方向的 RNR 阻塞另一个方向的完成。超时则报错，明确未送达的 PULL 可取消源导出；结果不明的提交失败和 ACK 超时不重发、不提前释放可能仍被远端访问的源。这是失败恢复，不交换或估计远端信用额度。`rx_depth` 决定突发容忍量；无限期停读或资源不足仍会超时。

TCP 握手由双方分别确认 HELLO 已解析且导入完成，收到对方确认后才开放业务发送。仅完成本地 TCP 写入不能保证对方已处理 HELLO，URMA 首包可能超越 TCP 握手；建连确认不依赖信用报文。

中断处理为 `wait → ack → poll → rearm → poll → rearm`。wait 使用非阻塞的 timeout=0，每次至多取 8 个事件；ACK 数量对应事件数，不是 CQE 数量。两次 poll 各至多处理 32 个 CQE，任一次满批就安排事件循环续跑。第二次 poll 补收首次 rearm 窗口内的完成；第二次 rearm 前后的新到达由已经开启的通知或持久事件入口接续。软件续跑与硬件中断使用同一流程。回调要求关闭时，仍结算本批已取出的全部 CQE，再停止队列。关闭清理先停止生产者，再排空 CQ 并确认剩余事件，不重新开启通知。

SEND 根据实际地址查询注册范围：安全 headroom 可就地放 H24，否则使用独立头；同次写入的小片段可合并，注册正文能在单个 WR 的 SGE 上限内完整描述时直接发送。SGE 不足时，将本次 DATA 的正文复制合并到一个注册块，用一个 WR 发送，避免仅因 IOBuf 分段而增加发送次数；总包长上限仍会导致必要的分片，不跨独立写调用合并，也不额外等待凑包。池外正文以及 `max_send_sge=1` 且无法使用原块 headroom 的输入同样复制合并。这些 SEND 回退允许复制，计入 `copied_send_bytes`；能够直接发送的注册正文仍保持零复制。大范围仅在属于 HELLO 公布的初始段、满足单次 READ 上限时使用 PULL32。每连接同时一个整块 READ；不连续输入按原顺序分别规划，不伪装成连续内存。新增 region 暂不导出 READ 源，改走 SEND。

READ 成功后先准备 FREE24，再移交目标 IOBuf；源端收到匹配 FREE 且本地访问结束才能释放。数据和写回调保持顺序，错误回调只执行一次。超时或断链不能证明远端已停止读取，未决源保持有界隔离，不强制释放。

同一 writeChain 的连续完成前缀可先归还 IOBuf 节点、物理容量和待发字节额度，无需等待整批尾部完成。回收位置以现有在途 DATA 和待 FREE 的 READ 源中最早的范围为界；后面的 CQE 不能越过前面的 RNR 或 READ 持有。写回调仍按整次写入完成并保持调用顺序。

免拷贝的边界是注册序列化输出到对端协议接收输入之间的正文：DATA 把已完成接收的原块 A 交给拥有型 IOBuf，接收槽换用块 B；READ 则把独立注册目标块 T 交给 IOBuf，接收槽中的 PULL/FREE 只承载控制消息。上层仍持有 A/T 时，补投不能复用该正文存储。

Header/Rocket 的移动读取入口和 IOBuf 字段可继续共享这些正文存储。业务字段反序列化成 `std::string`、压缩/解压、显式合并及普通读取回调仍可能复制；初始协议探测也使用普通读取回调。`copied_read_target_bytes` 统计普通读取回调复制的所有接收字节，包括 SEND 和探测前缀，不能只凭此计数断言 READ 正文是否复制。TLS 注册池中的 TLS 指线程本地存储，与传输加密无关。

## 配置与兼容

通过 `UrmaConfig` 或 `FOLLY_URMA_CONFIG` 指定的 [JSON](urma.json) 配置。注册池默认开启；初始化核对至少一条连接的预投、换块和 READ 确认保留能否由软容量及各档位承担，不借硬容量启动。新增连接须成功取得各自的队列及预投缓冲，失败则报告建连错误并清理已取得资源。

`max_connections` 限制连接数；`max_pending_bytes`、`max_pending_records` 和 `max_retained_bytes` 限制每连接排队及持有。`enable_read`、`read_threshold` 保留独立的 READ 开关和阈值；SEND 的 headroom 容量限制不决定 READ 阈值。库、随库 JSON 和 benchmark 默认均开启 TLS 与弹性池；JSON 可显式关闭弹性增长或 IOBuf 注册分配。注册与注销次数、耗时、用途持有、TLS 留存及分配失败由 `poolStats()` 提供。

线上格式为 HELLO98（hello_ver=4、impl_ver=2）、H24、PULL32 和 FREE24，HELLO 不再携带 FC endpoint。**两端必须同步升级，旧共享队列协议不兼容**。JSON 为 version 3，已删除的共享队列、信用流控参数会作为未知字段报错；`read_window` 仅允许 1，`offer_window` 限制本地保留的未确认 READ 描述。FREE 是 READ 完成确认，决定源存储何时可释放。未实现 TCP 业务数据降级。

URMA 配置和统计结构也有变化，依赖这些头文件的程序需要同步重编译；不能直接把新动态库替换进使用旧头文件编译的程序。IOBuf 本身未增加数据成员。

启用 `FOLLY_BUILD_URMA_TESTS` 后，测试按内存契约、独立队列及 socket 行为登记到 CTest；旧测试构建选项保留为别名。硬件测试未指定 `FBTHRIFT_URMA_TEST_DEVICE` 时明确 SKIP；功能通过不等于 RPC latency 改善。
