# URMA 基础通信版本

这一版提供基础 Folly URMA 通信，已在两台服务器上通过连续大包、并发大包和接收队列耗尽后的恢复测试。

代码基于本仓库 2026-03-27 的提交 `0187fd764fc9de79b2094ee62bcce0df36cbdcd7`。基础 URMA 版本为 `2de955dba`；本次在其上增加同一次写入内的小片段合并发送。依赖库整体基线与早期测试不同，旧性能结果不应直接当作当前版本的性能结果。

小包合并实现与补充验收统一在同一个特性提交中，详见 [需求核对与验收](test/COALESCING.md)。v1 `2de955dba` 保持不变。

## 保留的功能

- TCP 交换设备端点、接收缓冲区大小和就绪状态；TCP 同时承载连接关闭通知。
- RM/CTP、OI 顺序模式的 URMA SEND/RECV。业务数据只走 URMA。
- 每个连接独立拥有 jetty、接收队列、完成队列和注册内存。
- 默认每个接收缓冲区 4 KiB；配置超过 4 KiB 时按 CTP SEND 上限收缩。更大的写入拆成多个 SEND。每个连接同时只保留一个未完成 SEND，确认成功后才消耗对应数据并发送下一块。
- 同一次 `writev` / `writeChain` 的多个小片段按字节顺序填入现有注册发送块，每次最多发送双方协商的接收容量。末块不足容量也立即发送，不补零、不等待后续写入。
- 收到设备明确的 RNR（接收端没有可用缓冲区）错误时，保留本块数据，通过 EventBase 延迟 1 ms 重试。连续 5 秒未恢复则报错退出；调用方设置了更短发送超时时，优先遵守该超时。ACK 超时等无法确认接收结果的错误直接报错，避免重复交付。
- `write`、`writev`、`writeChain`、普通读取回调和 IOBuf 读取回调，以及建连超时、发送超时和资源清理。

## 已移除的功能

应用层信用流控和流控专用端点、单边 READ 大包协议、远端内存描述符和 FREE 消息、TCP 自动降级、跨连接共享 JFS/JFR、协议窗口与重排模块，以及 URMA 对全局 IOBuf 内存池的挂接。

这里仍有本地待处理数据上限，用来限定单连接内存占用；它不向对端发送信用，也不能阻止对端耗尽接收队列。注册内存中的收发缓冲区属于基础通信资源。该版本会复制收发数据，不提供零拷贝优化。

## 构建和双机运行

在已安装 Folly 构建依赖及 URMA SDK 的服务器上，从 Folly 源码根目录构建：

```bash
cmake -S . -B _build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_LIBRARY_ARCHITECTURE="$(gcc -dumpmachine)" \
  -DBUILD_SHARED_LIBS=ON -DBUILD_TESTS=OFF \
  -DFOLLY_USE_URMA=ON -DFOLLY_BUILD_URMA_DEMO=ON \
  -DURMA_INCLUDE_DIR=/usr/include/ub/umdk/urma \
  -DURMA_LIBRARY=/usr/lib64/liburma.so
cmake --build _build --target UrmaEchoDemo -j8
```

在 .151 启动服务端：

```bash
./_build/urma_demo/UrmaEchoDemo server 21187 bonding_dev_0
```

在 .145 启动客户端：

```bash
./_build/urma_demo/UrmaEchoDemo client 192.168.25.151 21187 bonding_dev_0
```

示例完成一条消息的回显后，双方输出 `PASS URMA echo` 并退出。握手失败不会切换为 TCP 收发。

## 使用约定

1. 在创建传输对象及启动工作线程前调用 `UrmaInit::init`；所有传输对象销毁后再调用 `UrmaInit::fini`。不要并发切换初始化配置。
2. 每个传输对象由所属 EventBase 线程操作；不支持迁移 EventBase 或 EOR 标记。当前写入标志仅支持 `WriteFlags::NONE`。
3. 写入可以在握手完成前排队；`connecting()` 表示握手仍在进行，`isUrmaReady()` 表示双方已经就绪。
4. `writev` 的数据须保持有效，直到对应写回调；`writeChain` 接管 IOBuf 所有权。
5. `shutdownWrite()` 等待已排队的写完成后关闭写方向；`shutdownWriteNow()` 关闭整个连接并取消待完成写入。
6. 基础协议使用 `UBS2` 握手标识，双方必须一起更新，不能与旧 `UBS1` 或原 V2/V3 协议混用。

## 小片段合并发送

此前一次写入即使总共只有 1 KiB，只要分成协议头、正文等多个 IOBuf 节点，就需要逐节点 SEND，并逐次等待完成。现在使用 `folly::io::Cursor` 跨节点填充已有的注册发送块。例如，7 B、13 B、113 B、891 B 四个片段合计 1 KiB，可以通过一次 SEND 发送。

合并只发生在同一次写入内部，不等待新的请求，不跨独立写回调合并。上层框架若已经把多条消息放在同一个 IOBuf 链中，这些片段也会被连续打包。连续内存原本就能一次发送的情况仍然一次发送。

复制长度受 `peerBuffer` 限制；该值不大于本地注册发送块和对端接收块。发送成功完成后，才从原始链逐节点扣除本次发送的字节；提交忙或 RNR 时原始链仍保留，重试同一段数据。一次写入的全部数据完成后才调用一次成功回调。缓冲区数量、每连接一个未完成 SEND 的约束、RM/CTP 模式和线上握手格式均保持一致。

## 当前验证结论

2026-10-17 在 192.168.25.145 和 192.168.25.151 的 `bonding_dev_0` 上完成以下测试，SDK 为 umdk-urma 25.12.0-B138：

- 双向 16 项正常收发：1 B、1 KiB、4 KiB、4097 B、64 KiB、1 MiB 连续 100 轮、8 连接小包、16 连接各 10 轮 1 MiB 大包；全部通过。
- 接收端暂停 500 ms：双向 1 MiB 大包通过；接收队列缩至 1 个槽位后仍通过。
- 重试超时、发送超时、重试期间关闭、写失败回调中销毁：均正确结束，失败回调只执行一次，已完成字节数与对端实际收到的字节数一致。
- 新增 URMA 代码启用 ASan、UBSan 和泄漏检测，大包慢接收、上述异常退出和 5 项生命周期测试均通过。

此前的大包缺失来自旧 RC/CTP 路径：接收队列耗尽后，发送端仍可能得到成功通知。原生 SDK 对照程序曾复现“64 次发送完成、只收到 32 块”。改为 RM/CTP、OI，并只对明确的 RNR 错误重试后，同样的慢接收测试已收到全部 64 块。这里没有恢复应用层信用协议、单边 READ 或 TCP 数据降级。

2026-10-19 又在物理设备 `udmac0d1e2` 上通过双向 1 KiB、8 MiB 收发及 bonding 回归。接收完成记录保留 EID、jetty ID、操作码和长度检查，不再比较该设备完成记录没有填充的 UASID 字段。

2026-10-20 使用相同 URMA 源码完成 fbthrift 双机性能对比，URMA 九种包长各三轮全部成功。测试程序及参数见 [Acclibbenchmark/tcp_urma_baseline](https://gitcode.com/donghuanan/Acclibbenchmark/tree/master/fbthrift_folly_benchmark/tcp_urma_baseline)；测试使用的完整依赖基线与本分支的区别在该目录中说明。

以上结论对应测试时的两台机器及软件配置。原始设备验证记录保存在测试工作目录的 `validation/` 和 `evidence/` 中。

2026-10-22 在基础版本 `2de955dba` 上验证小片段合并发送，使用 Clang 16.0.6、两台服务器的 `udmac0d1e2`，双方绑定 CPU `80-95`、内存 NUMA 0：

- 37 组双机数据及发送次数测试通过，包含 1 B 至 8 MiB、双向收发、8 连接、空节点、块边界位于节点中间、独立写入与空写入排队、64 B 协商容量、EAGAIN / RNR 重试和慢接收恢复。合并版与 `2de955dba` 基础版双向混用通过，不需要更改握手格式。
- 4 组双机异常测试通过：重试超时、发送超时、主动关闭、失败回调中销毁。已完成字节数与对端收到的字节数一致。两端各 5 项生命周期测试通过。
- 可控碎片用例中，1 KiB 和 4 KiB 的四个片段由 4 次 SEND 减为 1 次；8 KiB 的五个片段由 5 次减为 2 次；逐字节校验一致。
- 使用未改动的旧 benchmark，仅替换 Folly 库，1/4/8 KiB 各三轮新旧对照均无失败请求。配置为 15 CPU / 1 IO 线程、1 连接、QD 10、QPS 500、fields=none、header、echo_attachment=false、关闭 io_uring；预热 10 秒、正式 30 秒。三轮平均延迟如下：

| 包长 | 基础版平均延迟 | 合并版平均延迟 | 降低 |
| --- | --- | --- | --- |
| 1 KiB | 96.49 μs | 63.59 μs | 34.1% |
| 4 KiB | 109.86 μs | 77.01 μs | 29.9% |
| 8 KiB | 131.06 μs | 98.56 μs | 24.8% |

两版实际 QPS 均约为 516，来自旧 benchmark 的初始令牌行为。本结果对应上述小包配置；测试时其他 CPU 上存在后台任务。此次 Clang 16.0.6 环境缺少 sanitizer 运行库，ASan/UBSan 构建未成功，不计入本次通过项。
