# 快速入门

本文指导用户编译Folly优化版本、启用IOBuf TLS内存池，并验证原有io_uring混合读写路径。

> ![表示说明的图片](./public_sys-resources/icon-note.gif)**说明**：本文档适用版本：Folly v1.1.0。

## 环境准备

### 环境要求

- Linux系统，推荐Debian 12或openEuler。
- Clang 16或更高版本，所有依赖使用同一工具链。
- 安装CMake、Boost、fmt、glog、libevent、liburing及压缩库开发包。

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

 本项目提供两种获取优化源码的方式：按1.1节直接获取优化源码，或依次按获取基线源码、校验并应用补丁。完成后，进入下一章[编译与安装](#编译与安装)。

1. 直接获取dev_iouring分支的优化源码。

   dev_iouring分支已包含io_uring网络I/O优化、IOBuf TLS内存池等优化内容。 

   ```bash
   git clone --recurse-submodules --branch dev_iouring --single-branch \
   https://gitcode.com/boostkit/folly.git
   cd folly
   ```

2. 本章节剩余内容均为补丁仓的获取与应用，若已获取优化源码，即可跳转至下一章[编译与安装](#编译与安装)进行编译准备。

   **获取基线源码、补丁和校验文件**
    
    基线源码保存在folly目录，补丁和校验文件保存在同级的folly-patches目录。在选定的工作目录下执行以下命令。
    
   ```bash 
   git clone --recurse-submodules --branch v2022.11.14.00 --single-branch \ 
     https://github.com/facebook/folly.git folly 
   git clone --branch master --single-branch \ 
     https://gitcode.com/boostkit/folly.git folly-patches 
   cd folly-patches 
   ``` 
   
    **软件包完整性校验**
    
   本项目以补丁文件形式提供Folly性能优化功能，采用**SHA-256校验**确认补丁在下载、传输和存储过程中是否发生变化。 
   
   SHA-256校验用于验证文件完整性，不单独证明来源真实性。请从 [Folly官方仓库](https://gitcode.com/boostkit/folly)获取补丁及同一版本的校验文件。
   
   **校验文件** 
   
   | 文件名称 | 说明 | 
   | --- | --- | 
   | `folly_iobuf_iouring.patch` | Folly性能优化补丁 | 
   | `folly_iobuf_iouring.patch.sha256` | 记录上述补丁文件名及SHA-256摘要值的校验文件 | 
   
   补丁与校验文件应来自同一发布版本。补丁更新时，应同步更新校验文件。 
   
   **校验步骤** 
   
   按前面的步骤获取后，当前目录即为folly-patches。将补丁和校验文件放在同一目录，在该目录下执行以下命令： 
   
   ```bash 
   sha256sum --check --strict folly_iobuf_iouring.patch.sha256 
   ``` 
   
   该[命令](https://www.gnu.org/software/coreutils/manual/html_node/sha2-utilities.html)读取校验文件中的摘要值，与实际补丁的SHA-256摘要进行比较，并检查校验文件格式。
   
   校验通过时，输出如下。
   
   ```text 
   folly_iobuf_iouring.patch: OK 
   ``` 
   
   中文环境可能显示“成功”。应确认输出对应的文件名为`folly_iobuf_iouring.patch`，且命令没有报告失败或格式错误。 
   
   **结果判定** 
   
   | 校验结果 | 判定及处理 | 
   | --- | --- | 
   | 显示`OK`或“成功”，且无错误提示 | 补丁与校验文件中的摘要一致，完整性校验通过，可继续应用补丁 | 
   | 显示`FAILED`或“失败” | 补丁内容与预期不一致，停止使用并重新获取 | 
   | 提示文件不存在或无法读取 | 检查当前目录、文件名及文件是否下载完整 | 
   | 提示校验文件格式错误 | 重新获取发布方提供的校验文件 | 
   
   **异常处理** 
   
   校验失败时，请从官方仓库重新获取同一版本的补丁和校验文件，再次执行校验。 
   
   不要通过修改校验文件中的摘要值使校验通过。如重新获取后仍然失败，请向发布方反馈补丁版本、文件名和完整的校验输出。
   
   **应用优化补丁** 
    
   校验通过后，从补丁目录切换到基线源码目录，检查并应用补丁。 
   
   ```bash 
   cd ../folly 
   git apply --check ../folly-patches/folly_iobuf_iouring.patch 
   git apply ../folly-patches/folly_iobuf_iouring.patch 
   ``` 
   
   补丁只需应用一次。完成后，在当前folly源码目录继续执行下一章的编译与安装步骤。
   
## 编译与安装

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

### 配置建议

- 默认每线程最多缓存8个空闲块，默认块大小为8KB,建议将块大小设置为256KB。
- setBlockSize()只在启动阶段调用，不要在请求处理中动态修改。
- 小请求占比较高时可提高块复用率；请求经常超过块容量时仍会走原有路径。
- 线程数较多时，需要按“线程数 × 每线程缓存上限 × 块大小”评估内存上界。
- IOBuf可能跨线程释放，数据块可进入最终释放线程的TLS缓存，应观察线程间缓存分布。

### 正确性验证

编译全部Folly测试并执行以下命令。

```bash
ctest --test-dir _build --output-on-failure
```

内存池专项验证至少覆盖以下内容：

- 未启用时保持原有IOBuf::create()路径。
- 多个小IOBuf从同一块切分且slice互不重叠。
- 超过块容量时正确回退。
- cloneOne()、cloneOneAsValue()和析构的双计数语义。
- reserveSlow()离开池slice时不释放其他IOBuf共享的数据块。
- 线程退出和跨线程析构后无泄漏、重复释放或悬空引用。

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
|01|2026-9-30|第一次正式发布。|
