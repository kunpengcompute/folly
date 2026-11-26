# API参考

本文档介绍API参考，适用于需要调用Folly接口的用户。

URMA 传输与注册内存池的接口单列在[URMA补丁接口说明](#urma补丁接口说明)中。

## Folly V1.1.0：IOBuf TLS内存池接口说明

本文档介绍Folly V1.1.0新增的IOBuf TLS内存池接口和生命周期规则。该能力默认不改变原有分配行为，调用enableMemoryPool()后才会进入池化路径。

### 接口概览

|名称|类型|说明|
|--|--|--|
|IOBuf::enableMemoryPool|配置接口|启用IOBuf TLS内存池。|
|IOBuf::setBlockSize|配置接口|设置后续池块的总大小，默认8KB。|
|IOBuf::create|已有接口扩展|根据开关和容量选择池化或原有分配路径。|
|IOBuf::createFromPoolShared|内部接口|从当前线程共享块切分slice并构造IOBuf。|
|ioBufBlockAllocate|内部接口|优先从当前线程缓存获取空闲块。|
|ioBufBlockRelease|内部接口|将空闲块放回TLS缓存或释放给系统。|
|share_block|内部接口|获取能够容纳指定容量的当前共享块。|

### IOBuf::enableMemoryPool

#### IOBuf::enableMemoryPool函数功能

启用进程内IOBuf池化创建路径。启用后，满足容量条件的IOBuf::create()会优先从当前线程的IoBufBlock切分slice；不满足条件时继续使用Folly原有分配实现。

#### IOBuf::enableMemoryPool函数定义

```cpp
static void folly::IOBuf::enableMemoryPool();
```

#### IOBuf::enableMemoryPool函数约束说明

- 建议在服务启动、工作线程创建之前调用一次。
- 未调用时保持原有createCombined()或createSeparate()行为。
- 启用后仍保留大容量请求和池化失败的回退路径。

### IOBuf::setBlockSize

#### IOBuf::setBlockSize函数功能

设置后续新建IoBufBlock的块大小。默认值为8KB，实际可切分空间需要扣除块头元数据。

#### IOBuf::setBlockSize函数定义

```cpp
static void folly::IOBuf::setBlockSize(std::size_t size);
```

#### IOBuf::setBlockSize函数参数说明

|参数名|说明|输入/输出|类型|必填性|取值范围|
|--|--|--|--|--|--|
|size|新建池块的总字节数|输入|std::size_t|必选|需大于块头元数据，建议为8KB整数倍）、默认值（8KB）|

该配置影响后续新建数据块，不应在请求处理中频繁修改。建议在调用enableMemoryPool()前完成设置，并结合请求大小分布评估内存占用。

### IOBuf::create路由

#### IOBuf::create函数定义

```cpp
static std::unique_ptr<folly::IOBuf>
folly::IOBuf::create(std::size_t capacity);
```

#### IOBuf::create路由规则

```text
IOBuf::create(capacity)
├── 内存池未启用。
│   └── 原有createCombined/createSeparate路径。
├── capacity超过池块数据区容量。
│   └── 原有createCombined/createSeparate路径。
└── capacity能够由池块容纳。
    └── createFromPoolShared(capacity)
```

池化只改变底层数据区的获取方式，不池化IOBuf对象本身，也不将大请求拆分为多个池块。

### 内部数据结构与接口

#### IoBufBlock

IoBufBlock是自描述的池化数据块。

```text
IoBufBlock
├── magic：校验块类型和有效性。
├── capacity：数据区容量。
├── ref_count：TLS持有与全部IOBuf引用计数。
├── share_count：引用该块的IOBuf数量。
├── data_len：已经切分的数据长度。
└── payloadBegin()：数据区起点。
```

默认块大小为8KB。数据区按data_len连续推进，保证同一个块内的slice互不重叠。

#### TLSBlockCache

每个线程维护独立的TLSBlockCache。

```text
TLSBlockCache
├── blocks[8]：空闲块数组。
├── count：当前空闲块数量。
└── current_share：当前用于切分slice的块。
```

空闲块使用LIFO方式复用。current_share的data_len只由所属线程推进，避免为slice分配增加共享锁。

#### 数据块申请与回收

##### 函数定义

```cpp
IoBufBlock* ioBufBlockAllocate();
void ioBufBlockRelease(IoBufBlock* block);
IoBufBlock* share_block(std::size_t minCapacity);

static std::unique_ptr<folly::IOBuf>
folly::IOBuf::createFromPoolShared(std::size_t capacity);
```

##### 处理流程

1. ioBufBlockAllocate()优先从当前线程blocks[]取出空闲块；缓存为空时再向系统申请。
2. share_block()检查current_share剩余空间；不足时释放TLS持有并切换新块。
3. createFromPoolShared()以data_len为起点切分slice，同时增加ref_count和share_count。
4. IOBuf析构时减少对应计数；最后一个引用释放后，数据块进入最终释放线程的TLS缓存。
5. TLS缓存已满时，ioBufBlockRelease()将数据块释放给系统。

```text
IoBufBlock
[块头][slice 1][slice 2][剩余空间]
       ↑ IOBuf 1 ↑ IOBuf 2
```

### 池化标记与生命周期

#### flagsAndSharedInfo_复用

内存池不为IOBuf增加新的数据成员，而是复用flagsAndSharedInfo_。

```text
普通IOBuf
└── flagsAndSharedInfo_ = flags + SharedInfo*

池IOBuf
└── flagsAndSharedInfo_ = kFlagPoolIOBuf + IoBufBlock*
```

该设计保持sizeof(IOBuf)不变，避免破坏现有ABI。池IOBuf通过block()取得数据块，sharedInfo()返回空指针，不能把块指针解释为SharedInfo。

#### 引用计数规则

- ref_count统计TLS持有和全部IOBuf引用，归零后数据块才能回池或释放。
- share_count只统计IOBuf引用，用于区分TLS持有与实际数据使用状态。
- cloneOne()和cloneOneAsValue()必须同时增加两个计数。
- decrementRefcount()在池IOBuf析构时同时减少两个计数。
- reserveSlow()迁出原slice时只释放本IOBuf的块引用，不能释放整个共享块。
- isManagedOne()应将池IOBuf识别为受管理对象。
- isSharedOne()按当前使用约束返回池化对象的共享状态。

#### 线程退出与跨线程释放

线程退出时，current_share只能释放TLS自身的持有引用；仍被其他IOBuf引用的数据块必须继续存活。IOBuf跨线程析构时，数据块可能进入最终释放线程的TLS缓存，这属于当前设计接受的跨线程漂移，需结合实际负载监控各线程缓存占用。

### 配置与回退原则

- 默认每线程最多缓存8个空闲块，默认块大小为8KB。
- 只池化数据块，不池化IOBuf对象。
- 内存池未启用、请求容量过大或池化创建失败时，回退到Folly原有路径。
- setBlockSize()建议只在启动阶段调用一次。
- 块大小和每线程缓存上限需要结合请求分布、工作线程数及总体内存预算调优。

## Folly V1.1.0：io_uring混合读写接口说明

>![表示说明的图片](./public_sys-resources/icon-note.gif) **说明**
>本节保留Folly V1.1.0提供的io_uring混合读写接口，与Folly V1.1.0的IOBuf TLS内存池分开说明。

### Folly V1.1.0：io_uring混合读写接口概览

|名称|说明|
|--|--|
|newSocket|支持与AsyncSocket相同的同步连接功能。|
|writeChain|将写请求按顺序存入写队列，通过开源send执行发送。|
|PollWriteSqe|通过io_uring接收socket可写通知。|

### AsyncIoUringSocket::writeChain

#### 函数功能

在AsyncIoUringSocket中处理写操作。优化版本不再通过io_uring WriteSqe发送数据，而是将写请求按顺序存入队列，并连续调用send，避免io_uring在保序场景下可能引入的延迟。

#### 函数定义

```cpp
void AsyncIoUringSocket::writeChain(
    WriteCallback* callback,
    std::unique_ptr<IOBuf>&& buf,
    WriteFlags flags);
```

#### 参数说明

|参数名|说明|输入/输出|
|--|--|--|
|callback|写完成回调|输入|
|buf|待发送的IOBuf链|输入|
|flags|写操作标志|输入|

该函数无返回值。当send因缓冲区不足无法继续发送时，结合PollWriteSqe侦听socket fd的可写事件，并在可写后恢复发送。

## URMA补丁接口说明

本节仅适用于 `folly_urma.patch`，介绍进程初始化、异步收发和注册内存分配接口。接口声明位于 `folly/io/async/urma/` 目录下的 `UrmaInit.h`、`UrmaAsyncSocket.h` 和 `UrmaConfig.h`。

### 接口概览

|名称|类型|说明|
|--|--|--|
|UrmaInit::init、getConfig、fini|初始化接口|建立进程级SDK上下文和注册池，读取生效配置，退出时释放资源。|
|loadUrmaConfigFile|配置接口|从JSON文件加载配置，保留基础配置中未被覆盖的字段。|
|UrmaAsyncSocket::newSocket|建连接口|创建客户端传输。服务端使用UrmaAsyncSocket构造函数接管已接受的TCP socket。|
|UrmaAsyncSocket::connecting、isUrmaReady|状态查询接口|分别查询是否仍在连接或握手，以及是否已进入URMA收发状态。|
|UrmaAsyncSocket::setReadCB、write、writev、writeChain|收发接口|在所属EventBase注册读回调或提交异步写入。|
|UrmaAsyncSocket::writeChainWithRecords|收发接口|按序列化消息边界提交IOBuf链，多条消息共用一次写回调。|
|UrmaAsyncSocket::getAppBytesBuffered、getAllocatedBytesBuffered|积压查询接口|前者返回待发送数据字节数，后者统计写队列持有的容量及为READ保留的源缓冲副本。|
|UrmaAsyncSocket::setBufferCallback|通知接口|在所属EventBase通知发送积压变化或清空。注册时不触发回调，回调对象需保持有效直至用`nullptr`注销。|
|UrmaAsyncSocket::getUrmaStats|统计接口|在所属EventBase读取SEND、RECV、READ完成数及复制字节数。连接关闭后仍可读取。|
|UrmaInit::createPoolBuffer|分配接口|从本进程的注册池申请IOBuf。|
|UrmaInit::poolStats、poolRegions|统计接口|查看进程注册池的容量、分配及各区域状态。|

### UrmaInit::init

#### UrmaInit::init函数功能

客户端和服务端分别初始化本进程的SDK上下文与注册内存池。收发队列在EventBase首次使用时创建，供同一EventBase上的连接共享。

#### UrmaInit::init函数定义

```cpp
static void init(const UrmaConfig& config);
```

#### UrmaInit::init函数参数说明

|参数名|说明|输入/输出|
|--|--|--|
|config|设备、队列、连接积压及注册内存池配置。设置`FOLLY_URMA_CONFIG`时，首次初始化会用文件中的字段覆盖同名配置。|输入|

#### UrmaInit::init函数约束说明

- 编译时启用`FOLLY_USE_URMA=ON`，在创建socket前完成初始化。初始化成功后再次调用会沿用当前配置。
- `UrmaInit::getConfig()`返回生效的配置，包括按设备能力确定的接收队列深度。
- 应用退出时先释放socket、EventBase及池化IOBuf，再调用`UrmaInit::fini()`。若DMA仍在途、对端仍持有引用或SDK清理需要重试，资源会延迟释放。清理完成前`UrmaInit::isInitialized()`仍可返回`true`。

### UrmaAsyncSocket::newSocket

#### UrmaAsyncSocket::newSocket函数功能

创建客户端传输，通过TCP握手交换URMA连接信息。服务端将已接受的TCP socket传入`UrmaAsyncSocket`构造函数。返回的客户端传输由所属EventBase推进握手和收发。

#### UrmaAsyncSocket::newSocket函数定义

```cpp
static AsyncTransport::UniquePtr newSocket(EventBase* evb, const SocketAddress& address);
```

#### UrmaAsyncSocket::newSocket函数参数说明

|参数名|说明|输入/输出|
|--|--|--|
|evb|驱动连接、握手及读写回调的EventBase。|输入|
|address|服务端的TCP监听地址和端口。|输入|

### UrmaAsyncSocket::writeChain

#### UrmaAsyncSocket::writeChain函数功能

接管并异步发送IOBuf链。`write()`和`writev()`借用调用方的数据，调用方需保持数据有效直至写回调完成。收发示例见《[快速入门](./quick_start.md#启用urma传输)》。

#### UrmaAsyncSocket::writeChain函数定义

```cpp
void writeChain(WriteCallback* cb, std::unique_ptr<IOBuf>&& data,
                WriteFlags flags = WriteFlags::NONE) noexcept override;
```

#### UrmaAsyncSocket::writeChain函数参数说明

|参数名|说明|输入/输出|
|--|--|--|
|cb|写完成或失败回调。|输入|
|data|待发送的IOBuf链，调用时转移所有权。|输入|
|flags|本版支持`WriteFlags::NONE`。|输入|

#### UrmaAsyncSocket::writeChain函数约束说明

- `writeChain()`将整条链视为一条消息。一次提交多条序列化消息时，`writeChainWithRecords()`可额外传入各条消息的长度`recordLengths`。长度总和必须等于链的数据长度，空列表表示一条消息。
- 双方支持READ且单条消息长度超过`read_threshold`时，接收端可从发送端的注册内存读入本地，发送端保留源缓冲直至读取完成。其余消息使用SEND/RECV。
- 每个连接分别限制待发送数据字节数和队列持有的IOBuf容量。新提交的写入超限时，通过写回调的`writeErr()`报告错误。已接纳的写入遇到注册内存暂时不足时可等待内存归还或扩容，关闭、错误或发送超时会结束等待。

### UrmaInit::createPoolBuffer

#### UrmaInit::createPoolBuffer函数功能

从本进程的注册池申请IOBuf。成功时初始长度为0，容量至少为请求值。写入数据后调用`append()`设置有效长度。

#### UrmaInit::createPoolBuffer函数定义

```cpp
static std::unique_ptr<IOBuf> createPoolBuffer(size_t size);
```

#### UrmaInit::createPoolBuffer函数参数说明

|参数名|说明|输入/输出|
|--|--|--|
|size|请求的数据容量，单位为字节。|输入|

该接口在池分配失败时返回空指针，分配IOBuf元数据时仍可能抛出异常。默认配置下，普通`IOBuf::create()`也使用注册池，同步分配失败会抛出`std::bad_alloc`。通过C++设置`registered_pool.zero_copy=false`后，普通IOBuf沿用原分配路径，发送时按需复制到注册内存。

### loadUrmaConfigFile

#### loadUrmaConfigFile函数功能

加载JSON配置，返回叠加到基础配置后的`UrmaConfig`。文件上限为64 KiB，未知字段、重复键、类型错误及越界值会引发`std::invalid_argument`。`UrmaInit::init(config)`在初始化时检查设备能力。

#### loadUrmaConfigFile函数定义

```cpp
UrmaConfig loadUrmaConfigFile(const std::string& path, const UrmaConfig& base = {});
```

#### loadUrmaConfigFile函数参数说明

|参数名|说明|输入/输出|
|--|--|--|
|path|本机JSON文件路径。|输入|
|base|基础配置。省略时使用`UrmaConfig`的初始值。|输入|

#### URMA配置项

下表列出JSON支持的全部字段。下限和上限涉及C++配置时，按结构体初始值说明。

|配置项|默认值|取值范围|说明|
|--|--|--|--|
|`dev_name`|空，沿用应用配置|本机设备名，不含NUL|应用也未指定时，Folly 使用枚举到的首个设备。FbThrift 服务端须指定设备。|
|`eid_index`|未指定，取设备报告的首项|本机设备列出的 EID 索引，本测试环境为 0–7|两端分别选择本机索引。显式指定的索引必须存在，否则初始化失败。|
|`rx_depth`|0|0或1～65536，受设备JFR与JFC容量限制|每个EventBase的接收队列深度，0表示按设备能力选择。|
|`rx_target`|256|1～实际`rx_depth`|每个EventBase预投的接收缓冲数。每块默认4 KiB，有效大小受设备能力约束。|
|`tx_depth`|1024|1～4096，受设备JFS与JFC容量限制|每个EventBase的发送队列深度。|
|`handshake_timeout_ms`|5000|1～60000毫秒|客户端从发起TCP连接计时，服务端从接管socket或调用方传入的握手起点计时。|
|`max_pending_bytes`|134217728（128 MiB）|`buffer_size`～4294967295字节，初始下限为4096字节|每个连接的发送、接收积压分别计限。建连时不会按此值预留内存。|
|`registered_pool.pool_size`|268435456（256 MiB）|正数，按系统页大小对齐|本进程注册池的初始容量，单位为字节。|
|`registered_pool.pool_max_size`|536870912（512 MiB）|0或不小于`pool_size`的页对齐字节数|本进程注册池的上限。0表示以初始容量为上限。有效上限须能容纳TLS块，默认块大小为16 KiB。|

例如，使用默认的256个接收缓冲、每块4 KiB时，一个EventBase为预投接收缓冲占用1 MiB，进程内8个这样的EventBase共占用8 MiB。若要将每个EventBase的预投数改为128，可在JSON文件中写入以下内容。

```json
{
  "rx_target": 128
}
```

`buffer_size`、`enable_read`、`read_threshold`、`read_window`、`offer_window`和`registered_pool.zero_copy`通过C++配置设置。`read_threshold`初始为4352字节。

Folly 随附的 [urma.json](https://gitcode.com/boostkit/folly/blob/dev_urma_20221104/folly/io/async/urma/urma.json) 省略 `dev_name` 和 `eid_index`，保留应用传入的设备和索引。压测用的完整配置见 AccLibBenchmark 的 [scripts/urma.example.json](https://gitcode.com/boostkit/AccLibBenchmark/blob/master/fbthrift/fbthrift_urma/scripts/urma.example.json)。

## 修订记录

|文档版本|发布日期|修改说明|
|:---|:---|:---|
|01|2026-09-30|第一次正式发布。|
|02|2026-12-30|新增独立的URMA补丁接口说明。|
