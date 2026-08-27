# 快速入门

> 适用版本：Folly v1.1.0

本文指导用户编译Folly优化版本、启用IOBuf TLS内存池，并验证原有io_uring混合读写路径。

## 1. 环境准备

### 1.1 环境要求

- Linux系统，推荐Debian 12或openEuler。
- Clang 16或更高版本，所有依赖使用同一工具链。
- 安装CMake、Boost、fmt、glog、libevent、liburing及压缩库开发包。

Debian或Ubuntu执行以下命令。

```bash
sudo apt-get update
sudo apt-get install -y \
  git cmake build-essential liburing-dev libboost-all-dev \
  libdouble-conversion-dev libgflags-dev libgoogle-glog-dev \
  libevent-dev libsodium-dev liblz4-dev libsnappy-dev libzstd-dev \
  libfmt-dev liblzma-dev libgtest-dev libgmock-dev libssl-dev \
  libaio-dev libunwind-dev libdwarf-dev binutils-dev libiberty-dev \
  zlib1g-dev libbz2-dev
```

### 1.2 获取并应用优化补丁

1. 获取优化补丁代码。

   ```bash
   git clone --recurse-submodules --branch dev_iouring --single-branch \
   https://gitcode.com/boostkit/folly.git
   cd folly
   ```

2. IOBuf TLS内存池需要由v1.1.0优化补丁提供。源码尚未包含对应实现时，在配置前应用补丁。

   ```bash
   git apply --check /path/to/folly_iobuf_tls_pool.patch
   git apply --3way /path/to/folly_iobuf_tls_pool.patch
   ```

   如git apply --reverse --check能够成功，说明补丁已经应用，不应重复执行。

## 2. 编译与安装

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

## 3. 启用IOBuf TLS内存池

### 3.1 启动阶段配置

- 内存池默认不改变原有IOBuf分配行为。建议在创建工作线程前完成配置。

  ```cpp
  #include <folly/io/IOBuf.h>

  int main() {
  folly::IOBuf::setBlockSize(8 * 1024);
  folly::IOBuf::enableMemoryPool();
  }
  ```

- 启用后的IOBuf::create()路由如下。

  ```text
  容量能够由池块容纳
  └── 从当前线程current_share切分slice

  容量过大、内存池未启用或池化失败
  └── 回退到Folly原有createCombined/createSeparate路径
  ```

### 3.2 配置建议

- 默认每线程最多缓存8个空闲块，默认块大小为8KB,建议set至256KB。
- setBlockSize()只在启动阶段调用，不要在请求处理中动态修改。
- 小请求占比较高时可提高块复用率；请求经常超过块容量时仍会走原有路径。
- 线程数较多时，需要按“线程数 × 每线程缓存上限 × 块大小”评估内存上界。
- IOBuf可能跨线程释放，数据块可进入最终释放线程的TLS缓存，应观察线程间缓存分布。

### 3.3 正确性验证

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

## 4. 验证与测试io_uring与Benchmark

### 4.1 测试开源io_uring

构建时启用BUILD_TESTS后，可运行AsyncIoUringSocket测试。

```bash
./_build/experimental/io/test/async_iouring_socket_test
```

v1.0.0采用混合模式：读操作使用io_uring multishot，写操作使用开源send；send暂时不可写时由PollWriteSqe侦听socket fd并恢复发送。

### 4.2 测试Benchmark

1. 获取Benchmrk代码

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

   ```bash
   cd benchmark/server
   make
   numactl -N 0 ./net-server3-iouring

   cd ../client/iouring
   make
   ./net-client \
   --conn_per_thread 1 \
   --qps_per_conn 10000 \
   --batch_size 1 \
   --total_requests 100000
   ```

### 4.3 A/B测试原则

使用同一份二进制，通过是否调用enableMemoryPool()切换内存池状态，保持线程数、连接数、Payload和CPU绑定一致。至少比较以下方面。

- QPS与吞吐量。
- 平均延迟及P99延迟。
- malloc/free或内存分配热点。
- 进程常驻内存和各线程TLS缓存占用。
- 单位成功请求CPU周期。

正式压测应先预热，再交替运行基线与优化组，避免温度、频率和缓存状态造成单向偏差。

## 修订记录

|文档版本|发布日期|修改说明|
| :---| :---| :---|
|01|2026-9-30|第一次正式发布:<br>• 新增 `fbthrift_folly_benchmark` 并在benchmark侧布置外部接口  `setBlockSize()`, `enableMemoryPool()`，默认大小512KB。|
