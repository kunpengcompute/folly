# Quick Start

<!-- md-trans-meta sourceCommit=9c3416697441a78c6ecbafeaec1103ba744523db translatedAt=2026-10-13T03:53:03.661Z pushedAt=2026-10-13T07:36:34.982Z -->

This document provides guidance on how to compile an optimized version of Folly, enable the IOBuf thread-local storage (TLS) memory pool, and verify the existing `io_uring` hybrid read/write path.

> ![note](./public_sys-resources/icon-note.gif) **NOTE:**
>Applicable version: Folly v1.1.0

## Environment Setup

### Environment Requirements

- Linux system, Debian 12 or openEuler recommended
- Clang 16 or later, with all dependencies built using the same toolchain
- Development packages for CMake, Boost, fmt, glog, libevent, liburing, and compression libraries

On Debian or Ubuntu, run the following commands.

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

### Obtaining the Optimized Source Code

This project provides two methods to obtain the optimized source code. Method 1 directly obtains the optimized source code; method 2 obtains the baseline source code in sequence, verifies and applies the patches. After obtaining the optimized source code, proceed to [Compilation and Installation](#compilation-and-installation).

#### Method 1: Obtaining the Optimized Source Code from the dev_iouring Branch

The `dev_iouring` branch already includes optimizations such as io_uring network I/O optimization and the IOBuf TLS memory pool.

   ```bash
   git clone --recurse-submodules --branch dev_iouring --single-branch \
   https://gitcode.com/boostkit/folly.git
   cd folly
   ```

If you have already obtained the optimized source code, you can proceed to [Compilation and Installation](#compilation-and-installation) to prepare for compilation.

#### Method 2: Obtaining the Baseline Source Code, Verifying and Applying Patches

**Obtaining the Baseline Source Code, Patches, and Checksum Files**
    
Store the baseline source code in the `folly` directory, and store the patches and checksum files in the `folly-patches` directory at the same directory level. Run the following commands in the selected working directory:
    
   ```bash 
   git clone --recurse-submodules --branch v2022.11.14.00 --single-branch \ 
     https://github.com/facebook/folly.git folly 
   git clone --branch master --single-branch \ 
     https://gitcode.com/boostkit/folly.git folly-patches 
   cd folly-patches 
   ``` 
   
**Software Package Integrity Verification**
    
This project provides Folly performance optimization features in the form of patch files, and uses **SHA-256 verification** to confirm whether the patches have changed during download, transmission, or storage.

SHA-256 verification does not independently prove source authenticity. Obtain the patches and the checksum files of the same version from the [official Folly repository](https://gitcode.com/boostkit/folly).
   
**Checksum Files**

| File Name | Description |
| --- | --- |
| folly_iobuf_iouring.patch | Folly performance optimization patch |
| folly_iobuf_iouring.patch.sha256 | Checksum file recording the above patch file name and its SHA-256 digest value |

The patch and the checksum file must come from the same release version. When the patch is updated, the checksum file must be updated accordingly.

**Verification Steps**
   
After you obtain the files according to the preceding steps, the current directory is `folly-patches`.
   
   1. Place the patch and the checksum file in the same directory, and run the following command in that directory.
   
         ```bash 
         sha256sum --check --strict folly_iobuf_iouring.patch.sha256 
         ``` 
   
   2. This [command](https://www.gnu.org/software/coreutils/manual/html_node/sha2-utilities.html) reads the digest value in the checksum file, compares it with the SHA-256 digest of the actual patch, and checks the checksum file format. If verification passes, the output is as follows:
   
         ```output
         folly_iobuf_iouring.patch: OK 
         ``` 
   
      Confirm that the corresponding file name in the output is `folly_iobuf_iouring.patch`, and that the command does not report a failure or format error.
   
**Result Evaluation**
   
| Verification Result | Evaluation and Handling |
| --- | --- | 
| Displays `OK` with no error prompt | The digest in the patch matches that in the checksum file; integrity verification passes, and you can proceed to apply the patch. |
| Displays `FAILED` | The patch content does not match the expected value; stop using it and re-obtain the patch. |
| Prompts that the file does not exist or cannot be read | Check the current directory, file name, and whether the file was downloaded completely |
| Prompts a checksum file format error | Re-obtain the checksum file provided by the publisher |
   
**Exception Handling**
   
If verification fails, re-obtain the patch and checksum file of the same version from the official repository and run verification again.
   
Do not make verification pass by modifying the digest value in the checksum file. If verification still fails after re-obtaining the files, report the patch version, file name, and complete verification output to the publisher.
   
**Applying the Optimization Patch**
    
After verification passes, switch from the patch directory to the baseline source code directory, then check and apply the patch.
   
   ```bash 
   cd ../folly 
   git apply --check ../folly-patches/folly_iobuf_iouring.patch 
   git apply ../folly-patches/folly_iobuf_iouring.patch 
   ``` 
   
The patch only needs to be applied once. After that, continue with the compilation and installation steps in the next chapter in the current folly source directory.

## Compilation and Installation

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

When fmt or other dependencies are installed in a custom location, pass the actual CMake package path through `CMAKE_PREFIX_PATH` or `fmt_DIR`.

## Enabling the IOBuf TLS Memory Pool

### Configuration During Startup

- By default, the memory pool does not change the original IOBuf allocation behavior. It is recommended to complete the configuration before creating worker threads.

  ```cpp
  #include <folly/io/IOBuf.h>

  int main() {
  folly::IOBuf::setBlockSize(8 * 1024);
  folly::IOBuf::enableMemoryPool();
  }
  ```

- After the memory pool is enabled, `IOBuf::create()` is routed as follows.

  ```text
  Capacity can be accommodated by a pooled block
  └── Splits slices from current_share of the current thread.

  Capacity is too large, the memory pool is not enabled, or pooling failed
  └── Falls back to Folly's original createCombined/createSeparate path.
  ```

### Configuration Recommendations

- By default, each thread caches a maximum of eight free blocks, and the default block size is 8 KB. A block size of 256 KB is recommended.
- Call `setBlockSize()` only during the startup phase. Do not modify the block size dynamically while processing requests.
- When small requests account for a high proportion of requests, the block reuse rate can be improved. Requests that frequently exceed the block capacity still use the original path.
- When there are many threads, evaluate the upper bound of memory usage based on "number of threads × per-thread cache limit × block size".
- IOBuf objects may be released across threads, and data blocks may enter the TLS cache of the thread that performs the final release. The cache distribution across threads should be observed.

### Correctness Verification

Compile all Folly tests and run the following command.

```bash
ctest --test-dir _build --output-on-failure
```

The memory pool–specific verification must cover at least the following:

- When the memory pool is not enabled, the original `IOBuf::create()` path is preserved.
- Multiple small IOBuf objects are split from the same block, and their slices do not overlap.
- Requests that exceed the block capacity correctly fall back to the original path.
- `cloneOne()`, `cloneOneAsValue()`, and destruction follow the expected double-counting semantics.
- When `reserveSlow()` leaves a pooled slice, it does not release the data block shared by other IOBuf objects.
- No memory leaks, double frees, or dangling references occur after thread exit and cross-thread destruction.

## Verifying and Testing io_uring and Benchmark

### Testing Open-Source io_uring

After enabling `BUILD_TESTS` during the build, the `AsyncIoUringSocket` test can be run.

```bash
./_build/experimental/io/test/async_iouring_socket_test
```

v1.0.0 uses a hybrid mode: Read operations use `io_uring` multishot, while write operations use the system call `send`. When `send` is temporarily unable to write, `PollWriteSqe` listens on the socket file descriptor and resumes sending.

### Testing the Benchmark

1. Obtain the benchmark code.

   ```bash
   git clone https://gitcode.com/donghuanan/AccLibBenchmark.git
   cd AccLibBenchmark/folly
   ```

2. Complete the recommended settings for dual-machine stress testing.

   ```bash
   sudo cpupower frequency-set -g performance
   ulimit -n 65536
   export LD_LIBRARY_PATH=/usr/local/folly/lib:$LD_LIBRARY_PATH
   ```

3. Modify the Folly and fmt directories in the benchmark Makefile according to the actual installation paths, and then compile the server and client separately.

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

### A/B Testing Principles

Use the same binary and switch the memory pool state by calling or not calling `enableMemoryPool()`. Keep the number of threads, number of connections, payload, and CPU affinity consistent. At least compare the following aspects.

- Queries per second (QPS) and throughput
- Average latency and P99 latency
- `malloc`/`free` or memory allocation hotspots
- Process resident memory and per-thread TLS cache usage
- CPU cycles per successful request

For formal stress testing, warm up the system first, and then run the baseline and optimized groups alternately to avoid one-sided bias caused by temperature, frequency, or cache state.

## Change History

| Version | Date | Description |
| ---- | ---- | -- |
| 01 | 2026-09-30 | This is the first official release. |
