# API参考

## v1.1.0：IOBuf TLS内存池

> 本节说明v1.1.0新增的IOBuf TLS内存池接口和生命周期规则。该能力默认不改变原有分配行为，调用enableMemoryPool()后才会进入池化路径。

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

### `IOBuf::enableMemoryPool`

**函数功能**

启用进程内IOBuf池化创建路径。启用后，满足容量条件的IOBuf::create()会优先从当前线程的IoBufBlock切分slice；不满足条件时继续使用Folly原有分配实现。

**函数定义**

```cpp
static void folly::IOBuf::enableMemoryPool();
```

**使用约束**

- 建议在服务启动、工作线程创建之前调用一次。
- 未调用时保持原有createCombined()或createSeparate()行为。
- 启用后仍保留大容量请求和池化失败的回退路径。

### `IOBuf::setBlockSize`

**函数功能**

设置后续新建IoBufBlock的块大小。默认值为8KB，实际可切分空间需要扣除块头元数据。

**函数定义**

```cpp
static void folly::IOBuf::setBlockSize(std::size_t size);
```

**参数说明**

|参数名|描述|输入/输出|
|--|--|--|
|size|新建池块的总字节数|输入|

该配置影响后续新建数据块，不应在请求处理中频繁修改。建议在调用enableMemoryPool()前完成设置，并结合请求大小分布评估内存占用。

### `IOBuf::create`路由

**函数定义**

```cpp
static std::unique_ptr<folly::IOBuf>
folly::IOBuf::create(std::size_t capacity);
```

**路由规则**

```text
IOBuf::create(capacity)
├── 内存池未启用
│   └── 原有createCombined/createSeparate路径
├── capacity超过池块数据区容量
│   └── 原有createCombined/createSeparate路径
└── capacity能够由池块容纳
    └── createFromPoolShared(capacity)
```

池化只改变底层数据区的获取方式，不池化IOBuf对象本身，也不将大请求拆分为多个池块。

### 内部数据结构与接口

#### `IoBufBlock`

IoBufBlock是自描述的池化数据块：

```text
IoBufBlock
├── magic：校验块类型和有效性
├── capacity：数据区容量
├── ref_count：TLS持有与全部IOBuf引用计数
├── share_count：引用该块的IOBuf数量
├── data_len：已经切分的数据长度
└── payloadBegin()：数据区起点
```

默认块大小为8KB。数据区按data_len连续推进，保证同一个块内的slice互不重叠。

#### `TLSBlockCache`

每个线程维护独立的TLSBlockCache：

```text
TLSBlockCache
├── blocks[8]：空闲块数组
├── count：当前空闲块数量
└── current_share：当前用于切分slice的块
```

空闲块使用LIFO方式复用。current_share的data_len只由所属线程推进，避免为slice分配增加共享锁。

#### 数据块申请与回收

**函数定义**

```cpp
IoBufBlock* ioBufBlockAllocate();
void ioBufBlockRelease(IoBufBlock* block);
IoBufBlock* share_block(std::size_t minCapacity);

static std::unique_ptr<folly::IOBuf>
folly::IOBuf::createFromPoolShared(std::size_t capacity);
```

**处理流程**

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

#### `flagsAndSharedInfo_`复用

内存池不为IOBuf增加新的数据成员，而是复用flagsAndSharedInfo_：

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

## v1.0.0：io_uring优化

> 本节保留v1.0.0提供的io_uring混合读写接口，与v1.1.0的IOBuf TLS内存池分开说明。

### 接口概览

|名称|说明|
|--|--|
|newSocket|支持与AsyncSocket相同的同步连接功能。|
|writeChain|将写请求按顺序存入写队列，通过原生send执行发送。|
|PollWriteSqe|通过io_uring接收socket可写通知。|

### `AsyncIoUringSocket::writeChain`

**函数功能**

在AsyncIoUringSocket中处理写操作。优化版本不再通过io_uring WriteSqe发送数据，而是将写请求按顺序存入队列，并连续调用send，避免io_uring在保序场景下可能引入的延迟。

**函数定义**

```cpp
void AsyncIoUringSocket::writeChain(
    WriteCallback* callback,
    std::unique_ptr<IOBuf>&& buf,
    WriteFlags flags);
```

**参数说明**

|参数名|描述|输入/输出|
|--|--|--|
|callback|写完成回调|输入|
|buf|待发送的IOBuf链|输入|
|flags|写操作标志|输入|

该函数无返回值。当send因缓冲区不足无法继续发送时，结合PollWriteSqe监听socket fd的可写事件，并在可写后恢复发送。
