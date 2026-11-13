# URMA 传输

小包使用 SEND/RECV，大包使用 READ。TCP 负责握手、连接状态和结束序号。

## 注册内存

`UrmaInit` 初始化进程注册池，默认 256 MiB，按需扩容至 512 MiB。IOBuf 使用池内存切片；RX 和 READ 目标持有独占块，最后一个引用释放后归还。外部堆缓冲通过注册池复制发送。各用途共用容量，超过最大档位的申请按页对齐切分空闲段。开启池化 IOBuf 后，分配和扩容只返回注册内存；同步分配不足抛出 `std::bad_alloc`，socket 内部异步等待归还或扩容。

增长段通过 `holdSegment()` 保留租约，`revocableSegments()` 列出已排空的撤销候选；全部租约释放后才注销。`SegmentState` 管理段通告、导入和撤销状态。

## 共享队列

每个 EventBase 一套 JFS/JFR，TX/RX 共用 JFC/JFCE。连接表、SQ 槽位和等待队列由所属线程管理。TX 完成上下文持有 WR 资源，RX 按连接 ID 路由。CQ 读至空，RX 逐个补投，FREE 优先；资源暂时不足时等待并重试。

默认 JFS 深度 1024，JFR 深度取设备上限，`rx_target=256` 控制每个 EventBase 的预投块数。TX 缓冲按需申请，每个 EventBase 留一个最小档位块供资源耗尽时发送控制消息。暂存消息持有池缓冲，JFR 槽及时补投；RX 内存受注册池总上限约束。

## 数据顺序与关闭

DATA 携带数据，PULL 正文携带完整源地址和长度，FREE 释放远端读取责任。READ 可链式提交，每条 WR 产生 CQE；多个 READ 并发完成，由软件按业务序号交付。部分提交只重试未接受的后缀。单次 READ 至多 16 MiB，并受硬件、协商和可分配容量限制；更大消息拆分处理。源端待确认 READ 在进程内最多 65536 项，目的端 READ/FREE 在每个 EventBase 最多 1024 项，约束上下文数量而非用途内存额度。HELLO 版本 8、数据协议版本 5，双方需同步升级。

TCP FIN 前发送最终序号，接收端交付完整前缀后报告 EOF。关闭后继续处理在途 WR、FREE 和段撤销；未获远端释放确认的源块和段租约继续持有。

## 配置与测试

应用可构造 `UrmaConfig` 或调用 `loadUrmaConfigFile()`，配置示例见 `urma.json`。

启用 `FOLLY_BUILD_URMA_TESTS`，运行 `ctest -L URMA --output-on-failure`。双机收发入口为 `UrmaCoalescingTest` 和 `UrmaReadTest`，参数见 `--help`。
