# URMA 传输

URMA 通过 TCP 建链，小包使用 SEND/RECV，大包使用 READ。接收端按发送顺序交付数据。

## 配置

构建 Folly 时启用 `FOLLY_USE_URMA=ON`。应用通过 `UrmaConfig` 配置传输。也可以将 `FOLLY_URMA_CONFIG` 指向 [urma.json](urma.json)，文件中的字段会在 `UrmaInit::init()` 时覆盖对应的 C++ 配置。未知字段、重复字段或值不合法会导致初始化失败。

| 字段 | 默认值 | 取值与用途 |
|---|---:|---|
| `dev_name` | 由应用指定 | URMA 设备名。fbthrift 服务端须指定设备。 |
| `rx_depth` | 0 | 每个 EventBase 的接收队列（JFR）最多可容纳的接收请求数。0 表示自动选择。手动设置时可填 1–65536，超过设备可用深度会导致初始化失败。 |
| `rx_target` | 256 | 每个 EventBase 预投的接收缓冲块数，取值为 1 至实际接收队列深度。每块默认 4 KiB。 |
| `tx_depth` | 1024 | 每个 EventBase 的发送队列深度，取值为 1–4096，且不能超过设备支持的深度。 |
| `handshake_timeout_ms` | 5000 | 建链超时，1–60000 毫秒。客户端从发起 TCP 连接计时，服务端从识别协议计时。 |
| `max_pending_bytes` | 134217728 | 每连接的积压上限，按默认缓冲大小可设为 4096–4294967295 字节。发送和接收分别计限，须容纳单次完整写入。 |
| `registered_pool.pool_size` | 268435456 | 初始注册池容量，单位为字节，取页大小的正整数倍。 |
| `registered_pool.pool_max_size` | 536870912 | 注册池容量上限，按页对齐且不小于初始容量。0 表示使用初始容量作为上限。默认配置下，有效上限至少为 16 KiB。 |

使用默认配置（`rx_depth=0`、`rx_target=256`）时，库根据设备能力和发送队列深度确定接收队列容量，每个 EventBase 预投 256 块接收缓冲。收发共用的完成队列（JFC）由库自动创建，无需配置其深度。

## 内存与收发

注册池由进程内所有连接共享。启用池化的 IOBuf 后，同步分配无法满足时抛出 `std::bad_alloc`。已被 socket 接纳的写入会等待内存归还或扩容。外部传入的普通堆缓冲需要复制到注册内存。

同一 EventBase 上的连接共用一套发送和接收队列。大消息可拆成多个并发 READ，单次大小受设备、双方协商和内存池档位限制。收到 EOF 前会先交付已完整接收的数据。关闭连接后，仍在被本地硬件或对端使用的内存会保留到访问结束。

## 用 Folly 收发一条消息

[EchoTest.cpp](test/EchoTest.cpp) 是一个只使用 Folly 公开接口的双机示例。客户端发送 `ping`，服务端原样回复，客户端逐字节核对后关闭连接。

两端分别在创建 socket 前初始化一次。C++ 配置类型是 `folly::UrmaConfig`。下面的 `deviceName` 是本机的 URMA 设备名。

```cpp
folly::UrmaConfig config;
config.dev_name = deviceName;
folly::UrmaInit::init(config);
```

要从文件配置，可在启动进程前设置 `FOLLY_URMA_CONFIG`。例如，将 `{"rx_target": 128}` 保存为 `urma.json`，再执行 `export FOLLY_URMA_CONFIG="$PWD/urma.json"`。同一段初始化代码就会使用 128 块预投缓冲，设备名仍取自 `config.dev_name`。两台机器分别读取各自的文件。

服务端使用 `AsyncServerSocket` 监听 TCP 端口，在 `connectionAccepted()` 中将已接受的 `AsyncSocket` 移交给 `UrmaAsyncSocket`。客户端调用 `UrmaAsyncSocket::newSocket(&evb, address)`。两端用 `setReadCB()` 接收数据，用 `write()` 或 `writeChain()` 发送，由 `EventBase` 推进握手和收发。回调对象需活到操作结束，socket 和 EventBase 销毁后调用 `UrmaInit::fini()`。

在装好 URMA SDK 的编译机上，从 Folly 仓库根目录构建示例。

```bash
cmake -S . -B build -DBUILD_SHARED_LIBS=ON -DFOLLY_USE_URMA=ON -DFOLLY_BUILD_URMA_TESTS=ON
cmake --build build --target UrmaEchoTest
```

示例程序生成在 `build/folly/io/async/urma/test/UrmaEchoTest`。

将 `UrmaEchoTest` 和它依赖的共享库部署到两端。下面用 `192.0.2.10` 代表服务端地址，启动时替换为实际地址和各自的设备名。在可执行文件所在目录，先启动服务端。

```bash
./UrmaEchoTest server 192.0.2.10 32181 udmac1d1e2
```

看到 `LISTEN` 后，在客户端运行。成功时两端分别输出 `PASS server` 和 `PASS client`。

```bash
./UrmaEchoTest client 192.0.2.10 32181 udmac1d1e2
```

## 单元测试

使用相同的 CMake 选项编译测试，在构建目录运行。

```bash
ctest -L URMA --output-on-failure
```
