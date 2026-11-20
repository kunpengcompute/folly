# Folly性能优化补丁介绍

简体中文|[English](./README_en.md)

本文档介绍Folly性能优化补丁，涵盖URMA传输与注册内存池、IOBuf TLS内存池及io_uring混合读写，适用于使用Folly组件库的开发者。

## 最新消息

- [2026-12-30]：发布Folly URMA版本补丁（`135599df`），新增共享收发队列、弹性注册内存池和大消息并发READ。
- [2026-09-30]：发布Folly V1.1.0版本补丁，新增IOBuf TLS内存池，通过线程本地数据块复用和连续slice切分减少高QPS场景下的`malloc/free`。
- [2026-06-30]：发布Folly V1.0.0版本补丁仓，针对Folly异步网络I/O框架进行io_uring优化，提升网络I/O的稳定性与基线io性能。

## 项目介绍

Folly是Meta开源的一套高性能C++11/14/17组件库，直接针对大规模、高并发、低延迟的服务器端应用场景而设计。

本项目面向Folly网络I/O和缓冲区分配路径进行性能优化。Folly URMA通过TCP握手建立连接，使用URMA传输数据。Folly V1.1.0新增IOBuf TLS内存池，仅池化数据块，不池化IOBuf对象本身。Folly V1.0.0采用io_uring混合模式，读操作使用multishot减少系统调用，写操作使用`send`系统调用保证连续写的顺序和延迟稳定性。

URMA传输的核心设计如下。

- 同一EventBase上的连接共享发送队列、接收队列和完成队列。
- 小消息通过SEND/RECV传输。双方支持READ时，接收端可并发读取超过门限的大消息，并按发送顺序交付。
- 每个进程管理自己的注册内存池。池按需扩容，在空闲时回收可释放的区域。

IOBuf TLS内存池的核心设计如下。

- 每个线程使用`TLSBlockCache`缓存空闲`IoBufBlock`，默认最多8个。
- 默认数据块大小为8KB，从`current_share`连续切分互不重叠的slice。
- 小容量`IOBuf::create()`优先复用池块；大容量、未启用或池化失败时回退到Folly原有路径。
- 复用`flagsAndSharedInfo_`保存池标记和块指针，不改变`sizeof(IOBuf)`。
- 使用`ref_count`和`share_count`分别管理块生命周期及IOBuf引用，支持clone、reserve和跨线程释放。

## 目录结构

```text
folly/
├── docs/                           # 文档目录
│   ├── en/                         # English documents
│   │   ├── api_reference.md        # API reference sheet
│   │   ├── quick_start.md          # Menu for beginners
│   │   ├── menu_folly.md           # menu
│   │   └── release_notes.md        # Release notes
│   │
│   ├── zh/                         # 中文文档
│   │   ├── api_reference.md        # API参考
│   │   ├── quick_start.md          # 快速入门
│   │   ├── menu_folly.md           # 文档列表
│   │   └── release_notes.md        # 版本说明书
│   └── LICENSE
├── LICENSE
├── folly_iobuf_iouring.patch        # io_uring与IOBuf TLS内存池补丁
├── folly_iobuf_iouring.patch.sha256 # 上述补丁的SHA-256校验文件
├── folly_urma.patch                 # URMA传输与注册内存池补丁
├── folly_urma.patch.sha256          # URMA补丁的SHA-256校验文件
├──README.md                        # 项目介绍
└──README_en.md                     # 项目介绍（英文）
```

两份补丁均以Folly `v2022.11.14.00`为基线。根据所需特性选择一份，校验和应用步骤见《[快速入门](docs/zh/quick_start.md#获取优化源码)》。

## 版本说明

关于Folly优化补丁的版本更新情况请参见《[版本说明书](docs/zh/release_notes.md)》。

## 快速入门

关于Folly的快速入门操作指导请参见《[快速入门](docs/zh/quick_start.md)》。

## 学习文档

| 学习文档名称 | 内容简介 |
| --------- | --------- |
| 《[快速入门](docs/zh/quick_start.md)》 | 提供URMA、io_uring与IOBuf TLS内存池的补丁应用、编译和验证步骤。 |
| 《[版本说明书](docs/zh/release_notes.md)》 | 提供补丁基线、源码版本、兼容性约束和特性更新。 |
| 《[API参考](docs/zh/api_reference.md)》 | 提供URMA初始化、配置和收发接口，以及IOBuf TLS内存池和io_uring接口说明。 |

## 免责声明

此代码仓参与Folly软件开源，对异步网络I/O和IOBuf内存分配路径进行性能优化。代码遵照开源软件的设计和编码风格，并保留原有分配回退路径。软件的任何漏洞与安全问题由相应上游社区根据其漏洞和安全响应机制解决，请密切关注上游社区发布的通知和版本更新。

## License

Folly遵循Apache-2.0许可证，具体请参见[LICENSE文件](LICENSE)。

本项目的文档适用CC-BY 4.0许可证，具体请参见[LICENSE文件](docs/LICENSE)。

## 贡献声明

欢迎大家为社区做贡献，如果使用过程中有任何问题/建议，或者需要反馈特性需求和bug报告，可以提交[Issues](https://gitcode.com/boostkit/community/blob/master/docs/contributor/issue-submit.md)联系我们，具体贡献方法可参考[贡献指南](https://gitcode.com/boostkit/community/blob/master/docs/contributor/contributing.md)。同时也欢迎大家在[讨论专区](https://gitcode.com/boostkit/community/discussions)展开讨论交流。感谢您的支持。

## 致谢

感谢来自社区的每一个PR，欢迎贡献Folly！
