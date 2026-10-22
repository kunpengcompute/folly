# URMA 通信使用与测试

`UrmaAsyncSocket` 为 Folly 和 fbthrift 提供 URMA 传输：TCP 用于交换端点、协商能力及通知关闭，业务数据使用 RM/CTP、OI 顺序模式；小记录通过 SEND/RECV 合并发送，大记录通过单边 READ 拉取。

## 1. 构建与接入

先安装 Folly 构建依赖及相互匹配的 URMA SDK 头文件、运行库和设备 provider。将 Clang 16.0.6 的 `bin` 目录加入 `PATH`，在 Folly 源码根目录执行：

```bash
export CC=clang
export CXX=clang++
clang++ --version  # 确认版本为 16.0.6

cmake -S . -B _build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DBUILD_SHARED_LIBS=ON -DBUILD_TESTS=OFF \
  -DFOLLY_USE_URMA=ON \
  -DURMA_INCLUDE_DIR=/usr/include/ub/umdk/urma \
  -DURMA_LIBRARY=/usr/lib64/liburma.so
cmake --build _build --target folly -j8
```

SDK 路径按现场安装位置填写。需要安装时，在配置命令中增加 `-DCMAKE_INSTALL_PREFIX=/path/to/project/ins`，再执行 `cmake --install _build`。编译和运行 fbthrift、benchmark 时，应使用同一安装目录的头文件和动态库，避免混入系统中的旧版本。

fbthrift 需包含对应的 URMA 服务端接入及记录边界传递代码；旧 benchmark 源码不需要修改。日常构建只需 `folly`，直接收发验证使用第 5 节的测试程序。

## 2. 配置与使用约定

在创建传输对象及启动工作线程前初始化：

```cpp
folly::UrmaConfig config;
config.dev_name = "udmac0d1e2";
config.enable_read = true;
config.read_threshold = 4352;
folly::UrmaInit::init(config);
// 在所属 EventBase 线程创建和使用 UrmaAsyncSocket。
// 所有传输对象销毁后调用 UrmaInit::fini()。
```

| 配置 | 默认值与含义 |
| --- | --- |
| `rx_depth` / `tx_depth` | 32 / 8；初始化时还受设备能力限制 |
| `buffer_size` | 4096 B；CTP SEND 块最多 4 KiB，必须大于 128 B |
| `enable_read` | true；双方均启用且设备支持时才能使用 READ |
| `read_threshold` | 4352 B；完整记录严格超过该长度才具备 READ 条件 |
| `handshake_timeout_ms` | 5000 ms |
| `max_pending_bytes` | 128 MiB；分别限制待处理队列和相关缓冲区，不是单连接总内存上限 |

- 每个连接独立拥有 jetty、接收队列、完成队列和注册内存；传输对象由所属 EventBase 线程操作，不支持迁移 EventBase。写入标志仅支持 `WriteFlags::NONE`。
- 支持 `write`、`writev`、`writeChain`、普通读取回调和 IOBuf 读取回调。`writev` 的数据须保持有效至写回调；`writeChain` 接管 IOBuf 所有权。
- 写入可在握手完成前排队；`connecting()` 表示仍在握手，`isUrmaReady()` 表示双方已经就绪。
- `shutdownWrite()` 等待已排队写入完成后关闭写方向；`shutdownWriteNow()` 关闭整个连接并取消待完成写入。
- 当前握手协议为 `UBR2`，不与旧 `UBR1` / `UBS1` / `UBS2` 混用；两端需一起更新 Folly 并重编译依赖它的应用。
- `setBufferCallback()` 观察待发字节和持有容量，注册本身不触发回调。回调对象需存活至取消注册；统计包含排队 IOBuf 和在途 READ 源副本，不含空闲注册缓存和接收区。
- 发送超时按无发送进展的时间计算；调用 `setSendTimeout()` 会重新开始计时。内部 RNR / READ 期限不能替代上层框架设置的发送超时。
- 不要并发切换初始化配置。初始化失败会清理本次取得的资源；若 SDK 已由外部初始化，不接管外部的 SDK 退出责任。

当前会复制收发数据，尚未提供零拷贝、信用流控、跨连接共享 JFS/JFR、全局 IOBuf 池或 TCP 数据降级。本地待处理上限不等于远端接收信用。

## 3. SEND 合并与 READ 选路

### 3.1 按完整记录选路

判断对象是合批前的完整序列化记录，包含上层协议头，不是单个 IOBuf 节点，也不是 benchmark 的应用 `payload_size`。fbthrift 的 Header / Rocket 合批路径保留长度列表，通过 `writeChainWithRecords` 传递；Rocket SETUP 帧与首个请求也分开记录。透明包装层应显式转发，不能绕过会改变写入内容的包装层。

长度列表之和必须与链总长度一致，否则写入报错。普通 `write` / `writev` / `writeChain` 未提供边界时，将该次调用视为一个记录。

SEND 容量与 READ 阈值相互独立：

- 默认 SEND 正文容量为 `4096 - 128 = 3968 B`。128 B 只预留、不实际填充，也不是设计文档中的 H64 路由头。
- 默认 4352 B 记录仍走 SEND；4353 B 开始具备 READ 条件。3969–4352 B 的记录会分成多个 SEND 块。
- `read_threshold` 是本端发送策略，两端无需相同。发送端设为 1024 B 时，2048 B READ 可被默认配置的接收端正常接收；接收端校验能力和访问范围，不以自身阈值或 SEND 容量作为 READ 长度下限。

默认配置、设备支持且对端持有预算充足时，同一次写入的示例为：

| 完整记录 | 发送方式 |
| --- | --- |
| 十个 1024 B 记录，总长 10240 B | 3 个 SEND 块，0 个 READ |
| 一个 8192 B 记录，任意 IOBuf 切分 | 1 个 READ 范围 |
| 1024 + 8192 + 1024 B | SEND → READ → SEND |
| 十个相邻 8192 B 记录 | 合成一个 81920 B READ 范围 |

### 3.2 小包合并

`Impl::flush()` 使用 `folly::io::Cursor` 跨节点复制连续前缀，填入已有的注册发送块，以 1 个 SGE 提交。7、13、113、891 B 四个片段合计 1 KiB，可以通过一次 SEND 发送。块边界可以落在节点中间；空节点跳过，不修改共享输入或原地写 headroom。

合并仅发生在同一次写入的相邻 SEND 范围内，不等待新请求，不合并独立写回调。发送成功完成后才消费原链；提交 EAGAIN 或明确 RNR 时保留原数据并重试。整次写入只完成一次回调，并保持 FIFO。

当前是注册块复制方案，尚未实现注册正文直发、多 SGE 零额外复制或共享端点路由。

### 3.3 大包 READ

1. 在同次写入中选择相邻且各自满足 READ 条件的大记录；不得夹带小记录或超过对端持有上限。把选中范围复制到本连接的注册源缓冲区，以 SEND_IMM 发送 64 B 描述，包含编号、长度和内存段信息。
2. 接收端校验协商能力、长度、地址范围、段属性及对端设备信息，再导入远端段并 READ。超过设备单次 READ 上限时分段拉取，全部完成后才交付。
3. 接收端将数据复制到独立 IOBuf，排队发送完成确认；确认优先于本端普通写入，避免双向大包互相等待。
4. 发送端同时收到描述的本地完成通知和对端确认后，才消费该范围并复用源缓冲区。后续范围不得越过它；所有范围完成后才调用该次写回调。

未协商 READ、设备不支持、范围超出对端预算或源注册失败时，可在描述发布前改用 SEND 分片；描述发布后，不能因结果不明而重新发送同一数据。不支持的本地扩展段属性也在发布前回退 SEND；非法远端属性在导入前拒绝。属性检查不依赖 SDK 是否声明 `has_user_info` 成员。

只对提交 EAGAIN、控制 SEND 明确返回 RNR 等可确定未完成的情况重试。RNR 延迟 1 ms 重试，连续 5 秒无恢复则失败；READ / 确认默认 30 秒超时，上层设置更短发送期限时优先遵守。READ 错误关闭逻辑字节流，不交付部分数据，不盲目重发。

## 4. 关闭与资源回收

`stop()` 取消定时器、注销事件处理器并关闭握手连接，随后把 `Resources` 移交给清理流程。清理失败的资源由独立队列持有并重试，不依赖已经销毁的 socket、回调或 EventBase。

| 资源 | 持有及释放条件 |
| --- | --- |
| READ 源内存 | `Resources::source` 持有独立副本；正常发送等描述本地完成和对端确认后才允许复用。关闭时先成功删除 jetty，再注销内存，最后释放存储。超时或描述 TX 完成不能单独作为释放依据。 |
| READ 目标 | `Resources::destination` 持有；完成后复制到独立 IOBuf。关闭时先删除 jetty、取消远端段导入，再注销目标内存并释放。 |
| 远端 jetty / 段 import | 对应取消导入调用成功后清空句柄，失败则保留并重试。 |
| JFR、JFC、JFCE、小包注册区 | 按依赖顺序清理，成功步骤不重复；内存只在注销成功后释放。 |
| context 引用 | 每个 `Resources` 持有一份，设备资源清理完后最后释放。 |

释放依据是 SDK 成功删除设备对象、撤销导入和注销内存的接口契约；返回失败时不推断 DMA 已停止。源、目标注册缓冲区按需增长并复用，各自受 `max_pending_bytes` 限制，源还受对端上限限制；原始 IOBuf、接收 IOBuf 和小包区另占内存。

首次清理失败记录任务编号、阶段、返回码及待回收数量。后台从 10 ms 开始退避，最多 1 秒；重复失败按次数的 2 次幂记录，成功记录 `recovered`。每个任务独立推进；线程创建失败也保留任务，下次入队或 `fini()` 时重试创建。

`UrmaInit::pendingCleanupCount()` 包含正在执行的任务。`fini()` 最多等待 250 ms；存在活跃 socket 或待回收任务时保留上下文，调用方可在待回收数归零后再次调用。库或进程退出时，对仍失败的任务记录 `retained at shutdown` 及失败阶段，不强制释放。

当前每连接、每方向仅一个在途 READ。以后扩展并发 READ 时，需为每个操作独立持有目标、import、源及完成记账。

## 5. 测试方法

### 5.1 构建与本机检查

在第 1 节构建配置上增加测试开关：

```bash
cmake -S . -B _build \
  -DFOLLY_BUILD_URMA_COALESCING_TESTS=ON \
  -DFOLLY_BUILD_URMA_READ_TESTS=ON
cmake --build _build -j8 --target \
  UrmaCoalescingTest UrmaReadTest UrmaReadFaultInjection \
  UrmaInitTest UrmaBufferTest UrmaCleanupQueueTest \
  UrmaCleanupReadTest UrmaCleanupFaultInjection
cd _build/folly/io/async/urma/test
./UrmaInitTest
./UrmaBufferTest
./UrmaCleanupQueueTest
```

这三项分别检查初始化失败清理、缓冲统计及回调重入、清理队列并发与独立推进，不要求双机。测试适用于动态链接的 Linux URMA 构建；运行前用 `ldd` 确认加载本次 Folly，必要时设置 `LD_LIBRARY_PATH`。

下列命令先运行服务端，再运行客户端。IP、端口、网卡及绑核参数应按现场拓扑填写；示例 CPU 80–95 / 内存 NUMA 0 不是其他机器的通用配置。双方使用同版库；交换角色后可验证另一方向。

### 5.2 小包合并

```bash
# 服务端
numactl -C 80-95 -m 0 ./UrmaCoalescingTest \
  server 192.168.25.151 29565 udmac0d1e2 4096 1
# 客户端
numactl -C 80-95 -m 0 ./UrmaCoalescingTest \
  client 192.168.25.151 29565 udmac0d1e2 4096 1
```

末两个参数为接收块容量和连接数，两端保持一致。每连接排队 8 次写入：0、900、1024、0、3968、17、19、3968 B；默认配置应有 6 次成功 SEND，每次 1 SGE。空写入不发 SEND，17 / 19 B 独立写入不合并。程序检查逐字节回显、FIFO、回调及共享输入未修改，通过时输出 `PASS coalescing` 和逐 WR / 写入 CSV。

`queue_us` 包含握手及前序写入等待，不是性能结果。早期纯 SEND 测试曾使用 64 B 接收块；当前接收块必须大于 128 B，小容量分片请使用下面的 `TEST_BUFFER` 用例。

### 5.3 READ、边界和异常

```bash
# 服务端
LD_PRELOAD="$PWD/libUrmaReadFaultInjection.so" \
  numactl -C 80-95 -m 0 ./UrmaReadTest \
  server 192.168.25.151 29562 1 4351,4352,4353,0,8388608,1024
# 客户端
LD_PRELOAD="$PWD/libUrmaReadFaultInjection.so" \
  numactl -C 80-95 -m 0 ./UrmaReadTest \
  client 192.168.25.151 29562 1 4351,4352,4353,0,8388608,1024
```

末两个参数为连接数和每连接写入字节数列表；测试设备固定为 `udmac0d1e2`。示例每方向有 2 个 READ 范围，其余非空写入走 SEND。测试覆盖 `writev` / `writeChain`、空节点、普通 / movable 读取回调，逐字节核对顺序、写回调次数及 App 字节统计。`TRACE` 给出 SEND、描述、确认、READ 和资源计数；正常退出应注册/注销、导入/取消导入配平，`initialized=0`。

| 环境变量 | 检查内容 |
| --- | --- |
| `TEST_BATCH_RECORDS=1024,8192,1024` | 总长参数用 `10240,10240,10240`；每方向共 6 SEND、3 READ、3 次 FIFO 写回调 |
| `TEST_PART=0` / `1` / `7` / `1024` / `3968` | 0 为单节点；改变节点切分，选路与发送次数应不变 |
| `TEST_DISABLE_READ=1` | 任一端禁用 READ，双方都应 SEND 分片 |
| `TEST_THRESHOLD=1024` / `8192` | 前者允许 2048 B READ；后者让 8192 B 记录仍走 SEND；也检查两端阈值不同 |
| `TEST_PENDING=4096` | 缩小持有预算，检查对端上限与回退 |
| `TEST_DEPTH=1` | 两端缩小发送/接收队列深度 |
| `TEST_BUFFER=256` | SEND 容量变为 128 B，READ 阈值不变 |
| `TEST_READ_LIMIT=2048` / `0` | 探针缩小设备 READ 上限，分别检查分段 READ / 不支持 READ |
| `TEST_TIMEOUT=500` | 设置 500 ms 写超时；0 不设应用超时，内部期限仍有效 |
| `TEST_HALF_CLOSE=1` / `TEST_ONE_WAY=1` | 半关闭 / 仅客户端写入，双方按同一场景设置 |
| `TEST_CLOSE_MS=100` | 100 ms 后关闭仍在进行的连接 |
| `TEST_DESTROY_ON_READ=1` / `TEST_DESTROY_ON_SUCCESS=1` | 在相应回调内销毁 socket |
| `TEST_BUFFER_OBSERVER=1` / `TEST_BUFFER_LIMIT=12000` | 观察缓冲及关闭归零 / 因 READ 源副本新增容量而关闭 |
| `READ_FAULT=...` | 指定当前进程的故障注入 |

另测十个 1024 B 小记录：应为 3 SEND、0 READ；十个 8192 B 大记录应合为一个 READ 范围；`1024,8192,8192,1024` 应为 2 SEND、1 READ。长度列表不匹配或求和溢出必须拒绝。相邻大记录合并也要覆盖预算、分段、注册失败、重试与延迟完成。

`READ_FAULT` 支持以下类型，通常只注入一次，`drop-ack` 持续丢弃确认：

- 提交/重试：`send-eagain`、`send-rnr`、`read-eagain`、`offer-eagain`、`ack-eagain`、`offer-rnr`、`ack-rnr`。数据应完整交付。
- 完成延迟：`hold-offer-cq`、`hold-read-cq`，将相应完成记录保留 100 ms，检查完成次序和资源持有。
- 资源/设备错误：`source-reg`、`source-attr`、`dest-reg`、`import`、`read-submit`、`read-cq`、`unreg`、`unimport`。源注册或不支持属性在描述发布前回退 SEND；增长时旧源注销失败保留数据并改用 SEND；描述发布后的 READ / 导入错误应明确失败。
- 非法描述/确认：`bad-length`、`bad-address`、`bad-attr`、`zero-length`、`short-region`、`bad-ack`、`drop-ack`。非法描述须在提交 READ 前拒绝，丢失确认应超时，不能重复交付。
- 持续进展：`READ_FAULT=paced-send TEST_DISABLE_READ=1 TEST_TIMEOUT=80`，写入 `131072,131072`。每个 SEND 完成延迟 20 ms，整体超过 80 ms 但持续有进展，应成功。

异常按预期报错时也要核对两端日志、回调和资源计数，不能把超时强杀算作通过。这些探针只作用于测试进程，不修改驱动或生产库；性能测量去掉 `LD_PRELOAD`。

### 5.4 READ 关闭与延迟回收

```bash
# 服务端
CLEANUP_FAULT=jetty CLEANUP_PERSIST=1 \
LD_PRELOAD="$PWD/libUrmaCleanupFaultInjection.so:$PWD/libUrmaReadFaultInjection.so" \
numactl -C 80-95 -m 0 ./UrmaCleanupReadTest server 192.168.25.151 29565 1 8192
# 客户端
CLEANUP_FAULT=jetty CLEANUP_PERSIST=1 \
LD_PRELOAD="$PWD/libUrmaCleanupFaultInjection.so:$PWD/libUrmaReadFaultInjection.so" \
numactl -C 80-95 -m 0 ./UrmaCleanupReadTest client 192.168.25.151 29565 1 8192
```

`CLEANUP_FAULT` 可选 `jetty`、`peer`、`import`、`source`、`destination`、`jfr`、`jfc`、`jfce`、`io`，分别覆盖设备删除、取消导入和内存注销；默认对每个匹配句柄连续注入三次失败。

`CLEANUP_PERSIST=1` 持续阻止清理，待 socket、回调及 EventBase 销毁后确认资源仍被持有，再解除故障并等待独立线程回收。增加 `CLEANUP_LEAVE_PENDING=1` 则不解除故障，用于检查退出记录。多连接测试增加 `CLEANUP_CONNECTIONS=8`，并将命令中的连接数改为 8。

探针检查句柄不重复删除、上下文最后释放、已注册 DMA 内存不被提前 `free()`，恢复后资源计数配平；单连接还检查依赖清理顺序。`CLEANUP_PASS` 表示恢复验收通过；永久失败必须输出 `CLEANUP_EXPECTED_RETAINED` 和 `retained at shutdown`，不能算作已经回收。该探针使用 glibc 释放入口，仅适用于 Linux/glibc。

## 6. 验证记录与默认阈值依据

以下是各次开发验证的摘要，重复回归不能相加为独立用例总数；历史版本结果不能替代当前版本验收。

| 日期 | 验证摘要 |
| --- | --- |
| 2026-10-22 | 小包合并 37 组数据/发送次数、4 组异常、两端各 5 项生命周期；另有 8 组逐 WR 检查，共 96 次写调用。早期 64 B 配置仅属于纯 SEND 基线。 |
| 2026-10-22 | 初版 READ 43 组功能/异常，含边界、分段、重试、确认次序、中途关闭；退出资源计数配平。 |
| 2026-10-23 | 发送范围选择 96 组双机用例，含 21 组预期失败/关闭；另有 10 项生命周期、4 项 fbthrift 合批边界检查，以及旧 Header / Rocket benchmark 功能回归。 |
| 2026-10-26 | 26 组清理专项：九阶段各三次失败和持续失败后解除共 18 组，正常/多连接 2 组，无后续 RX 的 READ 完成 1 组，中途关闭等 4 组，永久失败退出 1 组；两端清理队列各 129 个任务。另完成 113 组 READ 回归、10 项生命周期和旧 benchmark 连通检查。 |
| 2026-10-27 | 评审修复后重跑 113 组 READ、11 组默认阈值、10 项生命周期及初始化失败测试；旧 Header / Rocket benchmark 功能回归均无失败请求。 |

默认阈值于 2026-10-26 在 .145 / .151 的 `udmac0d1e2` 上校准，使用 Clang 16.0.6、SDK 25.12.0-B138。配置为 Header、1000 QPS、15 CPU / 1 IO、1 连接、深度 10、fields=none、无回显，两端 CPU 80–95 / 内存 NUMA 0。校准副本在同连接预热 10 秒后统计，两种路径交替测量，正式计时不加 SDK 探针。

| 实际记录长度 | 配对轮数 × 每轮统计时间 | SEND 平均时延 | READ 平均时延 |
| --- | --- | --- | --- |
| 3968 B | 5 × 30 s | 66.51 μs | 75.39 μs |
| 4096 B | 5 × 30 s | 75.75 μs | 76.85 μs |
| 4352 B | 5 × 30 s | 75.99 μs | 75.63 μs |
| 8192 B | 2 × 20 s | 104.91 μs | 78.78 μs |

该场景约 4–4.25 KiB 持平，默认取实测的 4352 B，不是精确到 1 B 的通用交点。这组 Header benchmark 的记录比应用正文多 56 B，因此对应 `payload_size=4296`；其他协议不能照搬。原 benchmark 的初始令牌突发影响整场均值，冷启动结果不与同连接预热结果混算。

历史小包合并性能对照使用 Header、500 QPS、15 CPU / 1 IO、1 连接、深度 10、无回显、关闭 io_uring，预热 10 秒后正式 30 秒，各三轮。1 / 4 / 8 KiB 平均延迟从 96.49 / 109.86 / 131.06 μs 变为 63.59 / 77.01 / 98.56 μs；实际 QPS 约 516，来自旧 benchmark 初始令牌。该结果对应当时版本及设备，不作为当前 READ 版本的性能承诺。

功能验收保留逐字节、回调和资源计数日志；带探针的耗时不作为性能结论。上述近期 Clang 16.0.6 环境未完成 ASan/UBSan 验证，不将更早版本的 sanitizer 结果计入。
