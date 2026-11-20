# 版本说明书

URMA 独立补丁的发布日期、配套环境和新增特性见[URMA补丁说明](#urma补丁说明)。

## 版本配套说明

### 产品版本信息

|项目|版本|
|--|--|
|产品名称|Kunpeng BoostKit|
|产品版本|26.2.RC1|
|软件名称|Folly|
|软件包版本|V1.1.0|

### 与操作系统、编译器和CPU配套说明

|操作系统|编译器|CPU类型|
|--|--|--|
|Linux系统，推荐Debian 12或openEuler|Clang 16|鲲鹏950处理器|

### 病毒扫描结果

不涉及软件包发布，不涉及病毒扫描。

## 版本使用注意事项

版本详细使用注意事项请参见《[快速入门](./quick_start.md)》。

## 版本更新说明

### V1.1.0

#### 新增特性

|特性描述|更新说明|
|--|--|
|IOBuf TLS内存池|每线程维护TLSBlockCache，复用IoBufBlock并从current_share连续切分slice，减少小IOBuf数据区的malloc/free。|
|可配置池块大小|新增IOBuf::setBlockSize()，默认块大小为8KB，建议只在服务启动阶段配置。|
|显式启用接口|新增IOBuf::enableMemoryPool()；未调用时保持Folly原有分配路径。|
|容量路由与回退|小容量请求优先使用池块；大容量、未启用或池化失败时回退到createCombined/createSeparate。|
|ABI兼容设计|复用flagsAndSharedInfo_保存池标记和IoBufBlock指针，不增加IOBuf数据成员，不改变sizeof(IOBuf)。|
|完整生命周期管理|使用ref_count和share_count分别管理TLS持有与IOBuf引用，覆盖clone、reserve、析构、线程退出及跨线程释放。|

#### 默认配置

|配置项|默认值|说明|
|--|--|--|
|池块大小|8KB|包含块头，实际数据区略小。|
|每线程空闲块上限|8|缓存满后将多余块释放给系统。|
|启用状态|关闭|调用enableMemoryPool()后启用。|

#### 兼容性与风险说明

- 内存池只池化数据块，IOBuf对象仍沿用原有创建和销毁方式。
- 未启用或容量不适合池化时自动回退，不影响原有大请求路径。
- setBlockSize()属于全局配置，应在创建工作线程前设置，避免运行期配置竞争。
- 线程退出时只释放TLS持有引用，仍被IOBuf引用的数据块必须继续存活。
- IOBuf跨线程析构时，数据块可能进入最终释放线程的TLS缓存，需监控线程间内存分布。
- 块大小和缓存上限会影响常驻内存，应结合线程数和请求大小分布调优。

### V1.0.0

#### 新增特性

|特性描述|更新说明|
|--|--|
|Folly io_uring混合读写优化|读操作使用io_uring multishot减少系统调用；写操作使用开源send保持连续写顺序，并通过PollWriteSqe侦听socket可写事件。|

## URMA补丁说明

URMA 传输与注册内存池作为独立补丁发布，发布日期为 2026-12-30。先用随包的 [SHA-256 校验文件](../../folly_urma.patch.sha256) 验证补丁完整性，再将 [folly_urma.patch](../../folly_urma.patch) 应用于干净的 Folly `v2022.11.14.00` 基线。

### 配套环境

|操作系统|编译器|CPU类型|
|--|--|--|
|openEuler 24.03 LTS SP3|Clang 16.0.6|AArch64|

在上述环境中，使用 CMake 3.31.12 和 URMA SDK 编译并安装了 Release 共享库。33 项 URMA 单测全部通过，内置配置和样例 JSON 下的双机 Echo 各通过一次。复现步骤见《[URMA 快速入门](./quick_start.md#urma补丁快速入门)》。

### 新增特性

|特性描述|更新说明|
|--|--|
|URMA异步传输|`UrmaAsyncSocket`接入Folly的读写回调，通过TCP握手建立连接，通过URMA收发数据。|
|共享收发队列|同一EventBase上的连接共享发送队列（JFS）、接收队列（JFR）和完成队列（JFC）。|
|大消息并发READ|双方支持READ时，接收端可并发读取超过门限的消息并按序交付。其余消息使用SEND/RECV。|
|弹性注册内存池|每个进程独立管理注册内存，按档位分配，支持TLS切片、按需扩容和空闲区域回收。|
|配置与统计|通过`FOLLY_URMA_CONFIG`加载JSON配置，可查询连接积压字节数、收发统计及注册池用量。|

### 默认配置

|配置项|默认值|说明|
|--|--|--|
|URMA构建开关|关闭|配置CMake时设置`FOLLY_USE_URMA=ON`启用。|
|预投接收缓冲数|256|`rx_target`控制每个EventBase预先投递的接收缓冲数。|
|READ门限|4352字节|单条消息长度严格超过`read_threshold`时才考虑READ。|
|连接积压上限|128 MiB|`max_pending_bytes`按连接限制积压，发送与接收分别计量。|
|注册池容量|初始256 MiB，上限512 MiB|两项容量均按进程计算。|

### 兼容性与风险说明

- 编译需要URMA SDK，双机收发还需要两端的驱动、可用设备及可达的TCP握手地址。
- 直接使用 `dev_urma_20221104` 分支时，源码已包含 URMA 实现。应用程序通过 `UrmaInit::init(config)` 启用注册内存池。
- 注册池暂时不足时，已接纳的异步写入可以等待内存归还或扩容。同步池化`IOBuf::create()`申请失败会抛出`std::bad_alloc`。
- 关闭时先释放socket、EventBase和池化IOBuf，再调用`UrmaInit::fini()`。硬件或对端仍持有的注册内存会延迟回收。

## 版本配套文档

|文档名称|内容简介|交付形式|
|---|---|---|
|《API参考》|本文档提供Folly V1.1.0优化特性API使用说明|开源仓|
|《快速入门》|本文档提供编译Folly优化版本入门操作指导|开源仓|
|《版本说明书》|本文档提供Folly优化补丁的版本发布信息|开源仓|

### 获取文档的方法

您可以通过访问[开源仓](https://gitcode.com/boostkit/folly)浏览和获取相关文档。
