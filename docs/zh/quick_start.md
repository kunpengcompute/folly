# 快速入门

本文指导用户获取和编译Folly优化源码，启用URMA传输或IOBuf TLS内存池，并验证URMA与io_uring的收发功能。

## 环境准备

### 环境要求

|环境|要求|
|---|--|
|操作系统|Linux系统，推荐Debian 12或openEuler。URMA已在openEuler 24.03 LTS SP3上验证。|
|编译器|Clang 16及以上版本，所有依赖使用同一工具链。URMA验证使用Clang 16.0.6和CMake 3.31.12。|
|软件|CMake、Boost、fmt、glog、libevent、liburing及压缩库开发包。|
|URMA设备与SDK|编译需要`urma_api.h`和`liburma.so`。双机收发需要两端安装驱动，执行`urma_admin show --all`确认所选设备的链路为`ACTIVE`。|

Debian或Ubuntu执行以下命令。

```bash
sudo apt-get update
sudo apt-get install -y \
  git cmake build-essential clang-16 liburing-dev libboost-all-dev \
  libdouble-conversion-dev libgflags-dev libgoogle-glog-dev \
  libevent-dev libsodium-dev liblz4-dev libsnappy-dev libzstd-dev \
  libfmt-dev liblzma-dev libgtest-dev libgmock-dev libssl-dev \
  libaio-dev libunwind-dev libdwarf-dev binutils-dev libiberty-dev \
  zlib1g-dev libbz2-dev
```

### 获取优化源码

选用URMA传输时使用`dev_urma_20221104`分支或`folly_urma.patch`，选用io_uring与IOBuf TLS内存池时使用`dev_iouring`分支或`folly_iobuf_iouring.patch`。两份补丁分别应用于干净的上游基线。以下两种方式任选其一。

#### 方式一：直接获取开发分支的优化源码

URMA对应`dev_urma_20221104`分支的提交`135599df`。执行以下命令。

```bash
git clone --branch dev_urma_20221104 --single-branch \
  https://gitcode.com/boostkit/folly.git folly
cd folly
git checkout --detach 135599dfe4ccfcff7375bef66ce357c9ab6039fd
```

io_uring与IOBuf TLS内存池对应`dev_iouring`分支。执行以下命令。

```bash
git clone --recurse-submodules --branch dev_iouring --single-branch \
  https://gitcode.com/boostkit/folly.git folly
cd folly
```

开发分支已包含相应优化，获取后进入[编译与安装](#编译与安装)。

#### 方式二：获取基线源码、校验并应用补丁

**获取基线源码、补丁和校验文件**

在选定的工作目录下执行以下命令。基线源码位于`folly`，补丁和校验文件位于同级的`folly-patches`。

```bash
git clone --recurse-submodules --branch v2022.11.14.00 --single-branch \
  https://github.com/facebook/folly.git folly
git clone --branch master --single-branch \
  https://gitcode.com/boostkit/folly.git folly-patches
cd folly-patches
```

验证本地待发布的补丁时，将本地 `master` 工作区中的补丁和对应的 `.sha256` 文件复制到当前目录，再执行以下校验。

**软件包完整性校验**

本项目以补丁文件形式提供Folly性能优化功能，采用**SHA-256校验**确认补丁在下载、传输和存储过程中是否发生变化。

SHA-256校验用于验证文件完整性。请从[Folly官方仓库](https://gitcode.com/boostkit/folly)获取补丁及同一版本的校验文件。

**校验文件**

| 文件名称 | 说明 |
| --- | --- |
| folly_urma.patch | URMA传输与注册内存池补丁 |
| folly_urma.patch.sha256 | URMA补丁的SHA-256校验文件 |
| folly_iobuf_iouring.patch | io_uring与IOBuf TLS内存池补丁 |
| folly_iobuf_iouring.patch.sha256 | io_uring与IOBuf TLS内存池补丁的SHA-256校验文件 |

**校验步骤**

1. 在`folly-patches`目录选定补丁并校验。下例使用URMA，测试io_uring与IOBuf TLS时将首行文件名改为`folly_iobuf_iouring.patch`。

   ```bash
   patch_file=folly_urma.patch
   sha256sum --check --strict "$patch_file.sha256"
   ```

2. 该[命令](https://www.gnu.org/software/coreutils/manual/html_node/sha2-utilities.html)检查校验文件格式，并将其中的摘要值与实际补丁的SHA-256摘要比较。核对输出中的文件名，URMA校验通过时输出如下，中文环境显示“成功”。

   ```output
   folly_urma.patch: OK
   ```

**结果判定**

| 校验结果 | 判定及处理 |
| --- | --- |
| 显示OK或成功，且无错误提示 | 补丁与校验文件中的摘要一致，完整性校验通过，可继续应用补丁 |
| 显示FAILED或失败 | 补丁内容与预期不一致，停止使用并重新获取 |
| 提示文件不存在或无法读取 | 检查当前目录、文件名及文件是否下载完整 |
| 提示校验文件格式错误 | 重新获取发布方提供的校验文件 |

**异常处理**

校验失败时，从官方仓库重新获取同一版本的补丁和校验文件并再次校验。仍有错误时，请反馈补丁版本、文件名和完整的校验输出。

**应用优化补丁**

校验通过后，在同一终端进入基线源码目录，先检查补丁能否应用，再应用一次。

```bash
cd ../folly
git apply --check "../folly-patches/$patch_file" && git apply "../folly-patches/$patch_file"
```

## 编译与安装

在上一步的`folly`源码目录执行对应命令。使用独立安装的Clang时，将`CC`和`CXX`设为该工具链的`bin/clang`和`bin/clang++`。

### URMA

将`URMA_INCLUDE_DIR`设为包含`urma_api.h`的目录，将`URMA_LIBRARY`设为本机`liburma.so`的完整路径。

```bash
export CC=/usr/bin/clang-16
export CXX=/usr/bin/clang++-16
export FOLLY_INS="$HOME/folly-urma-ins"

cmake -S . -B _build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD=17 \
  -DCMAKE_POLICY_DEFAULT_CMP0060=NEW \
  -DBUILD_SHARED_LIBS=ON \
  -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=OFF \
  -DFOLLY_USE_URMA=ON -DFOLLY_BUILD_URMA_TESTS=ON \
  -DURMA_INCLUDE_DIR=/usr/include/ub/umdk/urma \
  -DURMA_LIBRARY=/usr/lib64/liburma.so \
  -DCMAKE_INSTALL_PREFIX="$FOLLY_INS"
cmake --build _build --parallel 8
cmake --install _build
```

`CMP0060=NEW`让链接器使用CMake选定的系统`libunwind`完整路径，避免独立Clang工具链中的同名库导致`unw_backtrace`未定义。头文件和库安装在`$FOLLY_INS`。`FOLLY_BUILD_URMA_TESTS=ON`单独启用URMA测试，测试程序保留在`_build`，运行方法见[启用URMA传输](#启用urma传输)。

### io_uring与IOBuf TLS内存池

```bash
export CC=/usr/bin/clang-16
export CXX=/usr/bin/clang++-16
export FOLLY_INS=/usr/local/folly

cmake -S . -B _build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD=17 \
  -DBUILD_BENCHMARKS=OFF \
  -DBUILD_TESTS=ON \
  -DBUILD_SHARED_LIBS=ON \
  -DCMAKE_INSTALL_PREFIX="$FOLLY_INS"

cmake --build _build --parallel "$(nproc)"
cmake --install _build
```

fmt或其他依赖安装在自定义位置时，通过CMAKE_PREFIX_PATH或fmt_DIR传入实际CMake package路径。

## 启用IOBuf TLS内存池

### 启动阶段配置

1. 内存池默认不改变原有IOBuf分配行为。建议在创建工作线程前完成配置。

   ```cpp
   #include <folly/io/IOBuf.h>

   int main() {
   folly::IOBuf::setBlockSize(8 * 1024);
   folly::IOBuf::enableMemoryPool();
   }
   ```

   此处8KB为默认值，生产环境建议改为256KB。

2. 启用后的IOBuf::create()路由如下。

   ```text
   容量能够由池块容纳
   └── 从当前线程current_share切分slice

   容量过大、内存池未启用或池化失败
   └── 回退到Folly原有createCombined/createSeparate路径
   ```

### 配置建议

- 默认每线程最多缓存8个空闲块，默认块大小为8KB，建议将块大小设置为256KB。
- setBlockSize()只在启动阶段调用，不要在请求处理中动态修改。
- 小请求占比较高时可提高块复用率；请求经常超过块容量时仍会走原有路径。
- 线程数较多时，需要按“线程数 × 每线程缓存上限 × 块大小”评估内存上界。
- IOBuf可能跨线程释放，数据块可进入最终释放线程的TLS缓存，应观察线程间缓存分布。

### 正确性验证

编译全部Folly测试并执行以下命令。

```bash
ctest --test-dir _build --output-on-failure
```

内存池专项验证至少覆盖以下内容。

- 未启用时保持原有IOBuf::create()路径。
- 多个小IOBuf从同一块切分且slice互不重叠。
- 超过块容量时正确回退。
- cloneOne()、cloneOneAsValue()和析构的双计数语义。
- reserveSlow()离开池slice时不释放其他IOBuf共享的数据块。
- 线程退出和跨线程析构后无泄漏、重复释放或悬空引用。

## 启用URMA传输

### 启动阶段配置

客户端和服务端分别在本进程创建socket前调用`UrmaInit::init(config)`。客户端通过`UrmaAsyncSocket::newSocket(evb, address)`建连，服务端将已接受的TCP socket交给`UrmaAsyncSocket`。释放socket、EventBase和池化IOBuf后调用`UrmaInit::fini()`。双机收发的完整示例见[EchoTest.cpp](https://gitcode.com/boostkit/folly/blob/135599dfe4ccfcff7375bef66ce357c9ab6039fd/folly/io/async/urma/test/EchoTest.cpp)。

```cpp
#include <folly/io/async/urma/UrmaInit.h>

int main() {
  folly::UrmaConfig config;
  config.dev_name = "udmac1d1e2";
  folly::UrmaInit::init(config);
  // 在此创建、使用并释放本进程的传输对象。
  folly::UrmaInit::fini();
}
```

### 配置建议

- `dev_name`使用本机设备名，两端可不同。首次初始化时，环境变量`FOLLY_URMA_CONFIG`指向的JSON文件覆盖同名C++配置，省略的字段沿用传入值。
- `rx_target`按EventBase设置预投接收缓冲数，`max_pending_bytes`按连接设置收发积压上限，`registered_pool`按进程设置注册内存容量。字段范围和默认值见《[API参考](./api_reference.md#urma配置项)》。

### 正确性验证

1. 在构建机源码目录执行URMA单测。本次版本应显示`100% tests passed, 0 tests failed out of 33`。

   ```bash
   ctest --test-dir _build -L URMA --output-on-failure
   ```

2. 在两端准备相同版本的`UrmaEchoTest`和`libfolly.so.*`，保留构建目录的相对路径。从各自的源码根目录执行以下命令，确认`ldd`列出本次构建的`libfolly`和本机的`liburma`。

   ```bash
   unset FOLLY_URMA_CONFIG
   export LD_LIBRARY_PATH="$PWD/_build${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
   ldd ./_build/folly/io/async/urma/test/UrmaEchoTest
   ```

3. 先启动服务端，将`192.0.2.10`替换为两端可达的服务端TCP地址，将`udmac1d1e2`替换为本机设备名。

   ```bash
   ./_build/folly/io/async/urma/test/UrmaEchoTest server 192.0.2.10 32181 udmac1d1e2
   ```

4. 服务端输出`LISTEN`后，在客户端使用同一服务端地址、端口和客户端本机设备名执行以下命令。

   ```bash
   ./_build/folly/io/async/urma/test/UrmaEchoTest client 192.0.2.10 32181 udmac1d1e2
   ```

   客户端发送`ping`并核对回显内容。两端分别输出`PASS client`、`PASS server`且退出码均为0，表示验证通过。

5. 验证JSON配置时，在两端各自的源码目录执行以下命令，再重复步骤3和4。随包样例省略`dev_name`，Echo沿用各端命令行传入的设备名。

   ```bash
   cp folly/io/async/urma/urma.json ./urma.json
   export FOLLY_URMA_CONFIG="$PWD/urma.json"
   ```

## 验证与测试io_uring与Benchmark

### 测试开源io_uring

构建时启用BUILD_TESTS后，可运行AsyncIoUringSocket测试。

```bash
./_build/experimental/io/test/async_iouring_socket_test
```

v1.0.0采用混合模式：读操作使用io_uring multishot，写操作使用系统调用send；当send暂时不可写时，由PollWriteSqe侦听socket fd并恢复发送。

### 测试Benchmark

1. 获取Benchmark代码。

   ```bash
   git clone https://gitcode.com/donghuanan/AccLibBenchmark.git
   cd AccLibBenchmark/folly
   ```

2. 双机压测前建议设置。

   ```bash
   sudo cpupower frequency-set -g performance
   ulimit -n 65536
   export LD_LIBRARY_PATH=/usr/local/folly/lib:$LD_LIBRARY_PATH
   ```

3. 根据实际安装路径修改Benchmark Makefile中的Folly和fmt目录，然后分别编译服务端及客户端。
   - 服务端（A机）执行以下命令。

      ```bash
      cd benchmark/server
      make
      numactl -N 0 ./net-server3-iouring
      ```

   - 客户端（B机）执行以下命令。客户端需在服务端就绪后发起压测。

      ```bash
      cd ../client/iouring
      make
      ./net-client \
      --conn_per_thread 1 \
      --qps_per_conn 10000 \
      --batch_size 1 \
      --total_requests 100000
      ```

### A/B测试原则

使用同一份二进制，通过是否调用enableMemoryPool()切换内存池状态，保持线程数、连接数、Payload和CPU绑定一致。至少比较以下方面。

- QPS与吞吐量。
- 平均延迟及P99延迟。
- malloc/free或内存分配热点。
- 进程常驻内存和各线程TLS缓存占用。
- 单位成功请求CPU周期。

正式压测应先预热，再交替运行基线与优化组，避免温度、频率和缓存状态造成单向偏差。

## 修订记录

|文档版本|发布日期|修改说明|
|:---|:---|:---|
|01|2026-09-30|第一次正式发布。|
|02|2026-12-30|新增URMA补丁获取、校验、编译、配置与双机验证说明。|
