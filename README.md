# folly 优化补丁仓介绍

## 最新消息

- 2026-03-30：发布补丁仓v1.0.0版本，针对folly异步网络io框架进行io_uring优化，提升网络io的稳定性与基线io性能。

## 项目介绍

folly是Meta开源的一套高性能C++11/14/17组件库，直接针对大规模、高并发、低延迟的服务器端应用场景而设计。

本项目是针对folly异步网络io框架的优化仓库，聚焦并优化io_uring的使用。本优化方案采用混合模式，即读操作使用io_uring的multishot模式以减少系统调用，而写操作回退到使用更擅长处理连续写的原生send系统调用，以规避io_uring在保序场景下可能引入的延迟。

## 目录结构

```text
folly/
├── docs/                           # 文档目录
│   ├── zh/                         # 中文文档
│   │   ├── api.md                  # API参考文档
│   │   ├── quick_start.md          # 快速入门文档
│   │   └── release_notes.md        # 版本说明书
│   └── LICENSE
├── iouring.patch                   # iouring优化的patch文件
├── LICENSE
└── README.md
```

## 版本说明

详见[版本说明书](docs/zh/release_notes.md) 

## 快速上手

详见[快速入门](docs/zh/quick_start.md)

## 文档

| 资源名称 | 资源简介 |
|---------|---------|
| [快速入门](docs/zh/quick_start.md) | 提供folly io_uring优化的编译安装和测试指导。 |
| [版本说明书](docs/zh/release_notes.md) | 提供folly io_uring优化版本的基础信息和特性更新信息。 |
| [API参考](docs/zh/api.md) | 提供优化后的接口说明及相关改动。 |

## 免责声明

此代码仓计划参与folly软件开源，仅对folly异步网络io部分函数进行性能优化，编码风格遵照原生开源软件，继承原生开源软件安全设计，不破坏原生开源软件设计及编码风格和方式，软件的任何漏洞与安全问题，均由相应的上游社区根据其漏洞和安全响应机制解决。请密切关注上游社区发布的通知和版本更新。对软件的漏洞及安全问题不承担任何责任。

## License

folly遵循 Apache-2.0许可证，具体请参见[LICENSE文件](LICENSE)。

本项目的文档适用CC-BY 4.0许可证，具体请参见[LICENSE文件](docs/LICENSE)。

## 贡献指南

如果使用过程中有任何问题，或者需要反馈特性需求和bug报告，可以提交isssues联系我们。

## 建议与交流

欢迎大家为社区做贡献。如果有任何疑问或建议，请提交Issues，我们会尽快回复。感谢您的支持。

## 致谢

folly补丁仓由华为公司的下列部门联合贡献：

鲲鹏计算Boostkit开发部
通算算法部

感谢来自社区的每一个PR，欢迎贡献folly补丁仓！
