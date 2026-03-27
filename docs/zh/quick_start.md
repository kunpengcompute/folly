# 快速入门

## 环境要求

- 已验证的OS：Debian 12 等支持 io_uring 的 Linux 系统
- 已验证的编译器：clang-16 或更高版本
- 系统依赖：需要安装 `liburing-dev` 及相关依赖包

## 使能 folly io_uring 优化

本优化方案采用混合模式，读操作使用 io_uring 的 multishot 模式，写操作使用原生 send 系统调用，以规避 io_uring 在保序场景下可能引入的延迟。

获取优化后的 folly 源码：

```bash
git clone -b dev_iouring https://gitcode.com/boostkit/folly.git
cd folly
```

### 安装依赖包

在 debian 系统上，需要安装以下依赖：

```bash
apt install liburing-dev libboost-all-dev libdouble-conversion-dev libgflags-dev \
libgoogle-glog-dev libevent-dev libsodium-dev liblz4-dev libsnappy-dev libzstd-dev \
libfmt-dev liblzma-dev libgtest-dev libgmock-dev libssl-dev libaio-dev \
libunwind-dev libdwarf-dev binutils-dev libiberty-dev zlib1g-dev libbz2-dev
```

### 编译与安装

建议统一编译器，例如使用 clang-16：

```bash
export CC=/usr/bin/clang-16
export CXX=/usr/bin/clang++-16

mkdir -p _build
cd _build
cmake .. \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_CXX_STANDARD=17 \
-DBUILD_BENCHMARKS=OFF \
-DBUILD_TESTS=ON \
-DCMAKE_INSTALL_PREFIX=/usr/local/folly \
-DBUILD_SHARED_LIBS=ON

make -j
```

> **说明：** 
> - `DCMAKE_INSTALL_PREFIX` 可替换为自定义的 folly 安装目的地址。
> - 若有自定义路径的库（如 fmt），可通过 `-DCMAKE_PREFIX_PATH` 指定。

## folly io_uring 原生测试用例

编译时若指定了 `-DBUILD_TESTS=ON`，可运行官方的 AsyncIoUringSocket 测试用例：

```bash
# 在 _build 文件夹下运行测试
./experimental/io/test/async_iouring_socket_test
```

*注：测试用例全部通过，超时（timeout）功能尚未集成，因此相关测试用例被跳过。*

## benchmark 性能测试

为了测试不同场景下 folly 的 io 能力，我们提供了 benchmark 测试脚本，可测试不同 io 压力下的时延及最大 QPS。

### 准备工作

测试应使用双机测试，并开启 performance 模式：

```bash
cpupower frequency-set -g performance
ulimit -n 65536
```

拉取测试脚本：

```bash
git clone https://gitcode.com/boostkit/AccLibBenchmark.git
cd AccLibBenchmark/folly
```

### 编译与启动

**编译 Server:**

```bash
cd benchmark/server
export LD_LIBRARY_PATH=/usr/local/folly/lib:$LD_LIBRARY_PATH
# 注意：修改 Makefile 中的路径指向实际安装的 folly 与 fmt 路径，并修改LD_LIBRARY_PATH指向folly实际安装路径
make
numactl -N 0,1 ./net-server3-iouring
```

**编译并启动 io_uring Client:**

```bash
cd benchmark/client/iouring
export LD_LIBRARY_PATH=/usr/local/folly/lib:$LD_LIBRARY_PATH
# 注意：修改 Makefile 中的路径指向实际安装的 folly 与 fmt 路径, 并修改LD_LIBRARY_PATH指向folly实际安装路径
make

# 启动测试，例如：1个连接，每个连接 10000 qps，加压 10s
./net-client --conn_per_thread 1 --qps_per_conn 10000 --batch_size 1 --total_requests 100000
```

### 自动化脚本测试

在 `epoll` 和 `iouring` 文件夹下提供了快速启动脚本 `run.sh`：

```bash
# 修改 run.sh 中的 LD_LIBRARY_PATH 后执行：
bash run.sh 1 10000 1 100000
```

测试最大 QPS：

```bash
python3 get_max_qps.py --mode iouring --duration 10 --max_repeat 3
```

测试 QPS-时延曲线：

```bash
# 在 iouring 文件夹下运行
bash ../latency_test.sh
```

