#!/bin/bash
# 通用增量覆盖率脚本（全自动：从空环境到覆盖率报告）
# CI 调用: bash run_coverage.sh [BASE_SHA] [MIN_COVERAGE] [TEST_TARGETS]
#
# 参数:
#   BASE_SHA       - 基线 commit/分支，默认 HEAD~1
#   MIN_COVERAGE   - 覆盖率阈值(%)，默认 80
#   TEST_TARGETS   - 测试目标，多个用逗号分隔，默认自动检测
#
# 前提: 当前目录是 folly 源码根目录（git 仓库）
#
# 示例:
#   bash run_coverage.sh origin/dev_iouring 80
#   bash run_coverage.sh origin/dev_iouring 80 iobuf_pool_test
#   bash run_coverage.sh HEAD~1 90

set -u

# ===== 换源（加速 apt 安装） =====
if command -v apt-get >/dev/null 2>&1; then
  echo "=== 换源 ==="
  if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS_ID=${ID:-ubuntu}
    OS_VERSION=${VERSION_CODENAME:-$(lsb_release -cs 2>/dev/null || echo "")}
    # Debian 没有 VERSION_CODENAME 的情况
    if [ -z "$OS_VERSION" ] && [ -n "${VERSION_ID:-}" ]; then
      case "$OS_ID" in
        debian)
          case "${VERSION_ID}" in
            12) OS_VERSION="bookworm" ;;
            11) OS_VERSION="bullseye" ;;
            10) OS_VERSION="buster" ;;
            9)  OS_VERSION="stretch" ;;
            *)  OS_VERSION="${VERSION_ID}" ;;
          esac ;;
        ubuntu)
          case "${VERSION_ID}" in
            24.04) OS_VERSION="noble" ;;
            22.04) OS_VERSION="jammy" ;;
            20.04) OS_VERSION="focal" ;;
            18.04) OS_VERSION="bionic" ;;
            *)     OS_VERSION="${VERSION_ID}" ;;
          esac ;;
      esac
    fi
    [ -z "$OS_VERSION" ] && OS_VERSION="focal"
  else
    OS_ID=ubuntu
    OS_VERSION=focal
  fi
  cp /etc/apt/sources.list /etc/apt/sources.list.bak 2>/dev/null || true
  cat > /etc/apt/sources.list << SRCEOF
deb http://mirrors.aliyun.com/${OS_ID} ${OS_VERSION} main restricted universe multiverse
deb http://mirrors.aliyun.com/${OS_ID} ${OS_VERSION}-updates main restricted universe multiverse
deb http://mirrors.aliyun.com/${OS_ID} ${OS_VERSION}-security main restricted universe multiverse
SRCEOF
  apt-get update -qq 2>/dev/null
  echo "  源已切换为阿里云镜像"
  echo ""
fi

BASE_SHA="${1:-HEAD~1}"
MIN_COVERAGE="${2:-80}"
TEST_TARGETS_ARG="${3:-}"

SRC_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
HEAD_SHA="$(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
BUILD_DIR="owbuild_cov"

echo "============================================"
echo "  增量覆盖率检查（全自动）"
echo "============================================"
echo "  源码:   $SRC_DIR"
echo "  基线:   ${BASE_SHA:0:12}"
echo "  当前:   ${HEAD_SHA:0:12}"
echo "  阈值:   ${MIN_COVERAGE}%"
echo "============================================"
echo ""

# ===== Step 0: 安装依赖 =====
echo "=== Step 0: 安装依赖 ==="

install_pkg() {
  local pkg=$1
  if command -v apt-get >/dev/null 2>&1; then
    apt-get install -y -qq "$pkg" 2>/dev/null || {
      # 装失败再 update 一次重试
      apt-get update -qq 2>/dev/null
      apt-get install -y -qq "$pkg" 2>/dev/null
    }
  elif command -v yum >/dev/null 2>&1; then
    yum install -y -q "$pkg" 2>/dev/null
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y -q "$pkg" 2>/dev/null
  elif command -v apk >/dev/null 2>&1; then
    apk add --no-cache "$pkg" 2>/dev/null
  fi
}

# 检查命令是否存在，不存在就装
ensure_cmd() {
  local cmd=$1
  local pkg=$2
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "  安装 $pkg ..."
    install_pkg "$pkg"
  fi
  command -v "$cmd" >/dev/null 2>&1
}

# 基础工具
ensure_cmd cmake cmake || { echo "ERROR: cmake 安装失败"; echo "RESULT: FAIL"; exit 1; }
ensure_cmd python3 python3 || { echo "ERROR: python3 安装失败"; echo "RESULT: FAIL"; exit 1; }
ensure_cmd git git || { echo "ERROR: git 安装失败"; echo "RESULT: FAIL"; exit 1; }

# clang + llvm
CLANG_CXX=""
for v in 18 17 16 15 14 13 12 11 ""; do
  if command -v "clang++-$v" >/dev/null 2>&1; then
    CLANG_CXX="clang++-$v"
    break
  elif [ -z "$v" ] && command -v clang++ >/dev/null 2>&1; then
    CLANG_CXX="clang++"
    break
  fi
done

if [ -z "$CLANG_CXX" ]; then
  echo "  安装 clang + llvm ..."
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update -qq 2>/dev/null
    apt-get install -y -qq clang llvm 2>/dev/null
  elif command -v yum >/dev/null 2>&1; then
    yum install -y -q clang llvm 2>/dev/null
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y -q clang llvm 2>/dev/null
  fi
  for v in 18 17 16 15 14 13 12 11 ""; do
    if command -v "clang++-$v" >/dev/null 2>&1; then
      CLANG_CXX="clang++-$v"
      break
    elif [ -z "$v" ] && command -v clang++ >/dev/null 2>&1; then
      CLANG_CXX="clang++"
      break
    fi
  done
fi

# llvm-profdata + llvm-cov
PROFDATA=""
COV=""
for v in 18 17 16 15 14 13 12 11 ""; do
  if command -v "llvm-profdata-$v" >/dev/null 2>&1; then
    PROFDATA="llvm-profdata-$v"
    COV="llvm-cov-$v"
    break
  elif [ -z "$v" ] && command -v llvm-profdata >/dev/null 2>&1; then
    PROFDATA="llvm-profdata"
    COV="llvm-cov"
    break
  fi
done

if [ -z "$PROFDATA" ]; then
  echo "  llvm 工具未找到，尝试安装 ..."
  install_pkg llvm
  for v in 18 17 16 15 14 13 12 11 ""; do
    if command -v "llvm-profdata-$v" >/dev/null 2>&1; then
      PROFDATA="llvm-profdata-$v"
      COV="llvm-cov-$v"
      break
    elif [ -z "$v" ] && command -v llvm-profdata >/dev/null 2>&1; then
      PROFDATA="llvm-profdata"
      COV="llvm-cov"
      break
    fi
  done
fi

# gtest
if ! find /usr -name 'libgtest*' 2>/dev/null | grep -q .; then
  echo "  安装 gtest ..."
  install_pkg libgtest-dev
  # 有些发行版需要手动编译 gtest
  if ! find /usr -name 'libgtest*.a' 2>/dev/null | grep -q .; then
    if [ -d /usr/src/gtest ]; then
      (cd /usr/src/gtest && cmake . 2>/dev/null && make 2>/dev/null && make install 2>/dev/null)
    fi
  fi
fi

# gmock
if ! find /usr -name 'libgmock*.a' 2>/dev/null | grep -q .; then
  echo "  安装 gmock ..."
  install_pkg libgmock-dev
  install_pkg libgtest-dev
fi

# 确保 gtest/gmock 的 .a 文件存在（Debian 12 需要手动编译）
if ! find /usr -name 'libgtest.a' 2>/dev/null | grep -q .; then
  if [ -d /usr/src/googletest ]; then
    echo "  编译 gtest/gmock ..."
    (cd /usr/src/googletest && cmake . 2>/dev/null && make 2>/dev/null && make install 2>/dev/null)
  elif [ -d /usr/src/gtest ]; then
    echo "  编译 gtest/gmock ..."
    (cd /usr/src/gtest && cmake . 2>/dev/null && make 2>/dev/null && make install 2>/dev/null)
  fi
fi

# 确保 cmake 能找到 gmock（创建 pkg-config 或 cmake config）
if find /usr -name 'libgmock.a' 2>/dev/null | grep -q .; then
  GMOCK_LIB=$(find /usr -name 'libgmock.a' 2>/dev/null | head -1)
  GMOCK_MAIN_LIB=$(find /usr -name 'libgmock_main.a' 2>/dev/null | head -1)
  GTEST_LIB=$(find /usr -name 'libgtest.a' 2>/dev/null | head -1)
  GTEST_MAIN_LIB=$(find /usr -name 'libgtest_main.a' 2>/dev/null | head -1)
  GTEST_INCLUDE=$(find /usr/include -name 'gtest.h' 2>/dev/null | head -1 | xargs dirname 2>/dev/null | xargs dirname 2>/dev/null)
  
  if [ -n "$GMOCK_LIB" ]; then
    # 设置环境变量供 cmake 的 FindGMock.cmake 使用
    export LIBGMOCK_LIBRARY="$GMOCK_LIB"
    export LIBGMOCK_MAIN_LIBRARY="$GMOCK_MAIN_LIB"
    export LIBGTEST_LIBRARY="$GTEST_LIB"
    export LIBGTEST_MAIN_LIBRARY="$GTEST_MAIN_LIB"
    export GMOCK_INCLUDE_DIR="${GTEST_INCLUDE}/gmock"
    export GTEST_INCLUDE_DIR="$GTEST_INCLUDE"
  fi
fi

# 常见 C++ 项目依赖（folly/fbthrift 需要）
# 先 update 确保包列表最新
apt-get update -qq 2>/dev/null || true

for pkg in libgoogle-glog-dev libgflags-dev libfmt-dev libdouble-conversion-dev \
           libboost-context-dev libboost-filesystem-dev libboost-program-options-dev \
           libboost-regex-dev libboost-system-dev libboost-thread-dev \
           libssl-dev libsodium-dev zlib1g-dev libzstd-dev liblzma-dev \
           libevent-dev libbz2-dev libiberty-dev libunwind-dev; do
  if command -v dpkg >/dev/null 2>&1 && ! dpkg -l "$pkg" 2>/dev/null | grep -q '^ii'; then
    install_pkg "$pkg"
  fi
done

echo "  cmake: $(cmake --version 2>/dev/null | head -1)"
echo "  python3: $(python3 --version 2>/dev/null)"
echo "  clang++: $CLANG_CXX"
echo "  $PROFDATA: $(${PROFDATA} --version 2>/dev/null | head -1)"
echo "  $COV: $(${COV} --version 2>/dev/null | head -1)"
echo ""

# ===== Step 1: 增量文件 =====
echo "=== Step 1: 增量文件 ==="

# 尝试多种方式解析基线
resolve_base() {
  # 直接用 commit hash
  git rev-parse --verify "$BASE_SHA^{commit}" >/dev/null 2>&1 && return 0
  # origin/<分支名>
  git rev-parse --verify "origin/$BASE_SHA^{commit}" >/dev/null 2>&1 && { BASE_SHA="origin/$BASE_SHA"; return 0; }
  # refs/remotes/origin/<分支名>
  git rev-parse --verify "refs/remotes/origin/$BASE_SHA^{commit}" >/dev/null 2>&1 && { BASE_SHA="refs/remotes/origin/$BASE_SHA"; return 0; }
  # fetch 后重试
  for remote in $(git remote 2>/dev/null); do
    git fetch "$remote" "$BASE_SHA" 2>/dev/null || true
    git fetch "$remote" 2>/dev/null || true
    git rev-parse --verify "$BASE_SHA^{commit}" >/dev/null 2>&1 && return 0
    git rev-parse --verify "$remote/$BASE_SHA^{commit}" >/dev/null 2>&1 && { BASE_SHA="$remote/$BASE_SHA"; return 0; }
  done
  return 1
}

if ! resolve_base; then
  echo "ERROR: 基线 $BASE_SHA 不存在"
  echo "  可用分支: $(git branch -a 2>/dev/null | head -10)"
  echo "  可用 remote: $(git remote -v 2>/dev/null | head -5)"
  echo "  如果是 CI 环境，请传 commit hash 而不是分支名"
  echo "RESULT: FAIL"
  exit 1
fi

INCREMENTAL_CPP=$(git diff --name-only "$BASE_SHA" "$HEAD_SHA" -- '*.cpp' '*.cc' 2>/dev/null | grep -v '/test/' | grep -v '/tests/' | grep -v '/benchmarks/' | grep -v '/tool/' | grep -v '/examples/' || true)

if [ -z "$INCREMENTAL_CPP" ]; then
  echo "没有增量源文件，跳过。"
  echo "RESULT: SKIP"
  exit 0
fi

for f in $INCREMENTAL_CPP; do echo "  $f"; done
echo ""

# ===== Step 2: cmake 配置 + 编译 =====
echo "=== Step 2: cmake 配置 ==="
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake "$SRC_DIR" \
  -DCMAKE_C_COMPILER="${CLANG_CXX/++/}" \
  -DCMAKE_CXX_COMPILER="$CLANG_CXX" \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fprofile-instr-generate -fcoverage-mapping" \
  -DCMAKE_EXE_LINKER_FLAGS="-fprofile-instr-generate -fcoverage-mapping" \
  -DBUILD_TESTS=ON \
  -DDOUBLE_CONVERSION_LIBRARY=/usr/lib/aarch64-linux-gnu/libdouble-conversion.so \
  -DDOUBLE_CONVERSION_INCLUDE_DIR=/usr/include/double-conversion \
  -DGMOCK_LIBRARY=/usr/lib/aarch64-linux-gnu/libgmock.a \
  -DGMOCK_MAIN_LIBRARY=/usr/lib/aarch64-linux-gnu/libgmock_main.a \
  -DGMOCK_INCLUDE_DIR=/usr/include \
  -DGTEST_LIBRARY=/usr/lib/aarch64-linux-gnu/libgtest.a \
  -DGTEST_MAIN_LIBRARY=/usr/lib/aarch64-linux-gnu/libgtest_main.a \
  -DGTEST_INCLUDE_DIR=/usr/include \
  2>&1 | tee /tmp/cmake_output.log | tail -20

if [ ! -f CMakeCache.txt ] || ! grep -q "CMAKE_PROJECT_NAME" CMakeCache.txt 2>/dev/null; then
  echo "ERROR: cmake 配置失败"
  echo "=== cmake 错误信息 ==="
  grep -i "error\|fail\|not found\|missing\|warning" /tmp/cmake_output.log | head -30
  echo "=== cmake 完整输出 ==="
  cat /tmp/cmake_output.log
  echo "RESULT: FAIL"
  exit 1
fi
echo ""

# ===== Step 3: 测试目标 =====
echo "=== Step 3: 测试目标 ==="
if [ -n "$TEST_TARGETS_ARG" ]; then
  IFS=',' read -ra TARGETS <<< "$TEST_TARGETS_ARG"
else
  TARGETS=()
  
  # 从 Makefile 找所有可构建目标
  ALL_TARGETS=$(grep -oP '^[a-zA-Z0-9_.-]+:' Makefile 2>/dev/null | tr -d ':' | sort -u || true)
  if [ -z "$ALL_TARGETS" ]; then
    ALL_TARGETS=$(ls CMakeFiles/ 2>/dev/null | grep '\.dir$' | sed 's/\.dir$//' || true)
  fi
  if [ -z "$ALL_TARGETS" ]; then
    ALL_TARGETS=$(make help 2>/dev/null | grep -oP '^\.\.\.\K\S+' || true)
  fi
  
  echo "  可用目标: $(echo "$ALL_TARGETS" | wc -w) 个"
  
  # 从 git diff 找新增的测试文件
  TEST_FILES=$(git -C "$SRC_DIR" diff --name-only "$BASE_SHA" "$HEAD_SHA" 2>/dev/null | grep -iP '/test[s]?/.*\.(cpp|cc)$' || true)
  
  if [ -n "$TEST_FILES" ]; then
    echo "  PR 新增测试文件:"
    for tf in $TEST_FILES; do
      echo "    $tf"
    done
  fi
  
  # 策略 1: 精确匹配测试文件名变体
  if [ -n "$TEST_FILES" ]; then
    for tf in $TEST_FILES; do
      base=$(basename "$tf" .cpp)
      for variant in \
        "$base" \
        "$(echo "$base" | sed 's/Test$//' | sed 's/\(.\)\([A-Z]\)/\1_\2/g' | tr 'A-Z' 'a-z')_test" \
        "$(echo "$base" | sed 's/Test$//' | tr 'A-Z' 'a-z')_test" \
        "$(echo "$base" | sed 's/Test$//')Test-t" \
        "$(echo "$base" | sed 's/Test$//' | sed 's/\(.\)\([A-Z]\)/\1_\2/g' | tr 'A-Z' 'a-z')" \
      ; do
        for t in $ALL_TARGETS; do
          if [ "$t" = "$variant" ] || [ "$t" = "${variant}-t" ] || [ "$t" = "${variant}_t" ]; then
            TARGETS+=("$t")
            echo "    $tf -> $t (精确匹配)"
            break 2
          fi
        done
      done
    done
  fi
  
  # 策略 2: 模糊匹配
  if [ ${#TARGETS[@]} -eq 0 ] && [ -n "$TEST_FILES" ]; then
    echo "  精确匹配失败, 尝试模糊匹配..."
    for tf in $TEST_FILES; do
      base=$(basename "$tf" .cpp)
      no_test=$(echo "$base" | sed 's/Test$//' | sed 's/test$//')
      keywords=$(echo "$no_test" | sed 's/\([A-Z]\)/ \1/g' | tr 'A-Z' 'a-z' | tr ' ' '\n' | grep -v '^$' | grep -v '^.$' | tr '\n' ' ')
      
      found=""
      for t in $ALL_TARGETS; do
        t_lower=$(echo "$t" | tr 'A-Z' 'a-z')
        if ! echo "$t_lower" | grep -q 'test'; then continue; fi
        match=true
        for kw in $keywords; do
          if ! echo "$t_lower" | grep -q "$kw"; then
            match=false
            break
          fi
        done
        if [ "$match" = "true" ]; then
          found="$t"
          break
        fi
      done
      if [ -n "$found" ]; then
        TARGETS+=("$found")
        echo "    $tf -> $found (模糊匹配)"
      fi
    done
  fi
  
  # 策略 3: 从增量文件名匹配
  if [ ${#TARGETS[@]} -eq 0 ]; then
    KEYWORDS=""
    for f in $INCREMENTAL_CPP; do
      base=$(basename "$f" .cpp)
      lower=$(echo "$base" | tr 'A-Z' 'a-z')
      KEYWORDS="$KEYWORDS $lower"
    done
    echo "  增量关键词: $KEYWORDS"
    
    for t in $ALL_TARGETS; do
      t_lower=$(echo "$t" | tr 'A-Z' 'a-z')
      if ! echo "$t_lower" | grep -q 'test'; then continue; fi
      for kw in $KEYWORDS; do
        if echo "$t_lower" | grep -q "$kw"; then
          TARGETS+=("$t")
          break
        fi
      done
    done
  fi
  
  # 策略 4: 全量兜底 — 跑所有测试目标
  if [ ${#TARGETS[@]} -eq 0 ]; then
    echo "  自动匹配失败, 编译所有测试目标..."
    for t in $ALL_TARGETS; do
      t_lower=$(echo "$t" | tr 'A-Z' 'a-z')
      if echo "$t_lower" | grep -q 'test'; then
        TARGETS+=("$t")
      fi
    done
  fi
fi

if [ ${#TARGETS[@]} -eq 0 ]; then
  echo "ERROR: 未找到测试目标"
  echo "  增量文件: $INCREMENTAL_CPP"
  echo "  请通过参数指定: bash run_coverage.sh <base> <threshold> <test_targets>"
  echo "RESULT: FAIL"
  exit 1
fi
echo "  目标: ${TARGETS[*]}"
echo ""

# ===== Step 4: 编译 + 定位二进制 =====
echo "=== Step 4: 编译 ==="
declare -a TEST_BINS
for target in "${TARGETS[@]}"; do
  echo "  编译 $target ..."
  if ! make "$target" -j"$(nproc)" 2>&1 | tail -3; then
    echo "ERROR: 编译 $target 失败"
    echo "RESULT: FAIL"
    exit 1
  fi
  TESTBIN=""
  for path in "./bin/$target" "./$target" "$target"; do
    if [ -x "$path" ]; then TESTBIN="$path"; break; fi
  done
  if [ -z "$TESTBIN" ]; then
    echo "ERROR: 找不到 $target 二进制"
    echo "RESULT: FAIL"
    exit 1
  fi
  TEST_BINS+=("$TESTBIN")
  echo "  $target -> $TESTBIN"
done
echo ""

# ===== Step 5: 运行测试 =====
echo "=== Step 5: 运行测试 ==="
rm -f coverage_*.profraw coverage.profdata

for i in "${!TARGETS[@]}"; do
  target="${TARGETS[$i]}"
  testbin="${TEST_BINS[$i]}"
  echo "  运行 $testbin ..."
  if LLVM_PROFILE_FILE="coverage_${target}_%p.profraw" "$testbin" 2>&1; then
    echo "  $target: PASSED"
  else
    echo "ERROR: $target 测试失败"
    echo "RESULT: FAIL"
    exit 1
  fi
done
echo ""

# ===== Step 6: 合并覆盖率 =====
echo "=== Step 6: 合并覆盖率 ==="
PROFRAW_LIST=$(ls coverage_*.profraw 2>/dev/null || true)
if [ -z "$PROFRAW_LIST" ]; then
  echo "ERROR: 无 profraw 文件"
  echo "RESULT: FAIL"
  exit 1
fi
echo "  profraw: $(echo $PROFRAW_LIST | wc -w) 个"
$PROFDATA merge $PROFRAW_LIST -o coverage.profdata 2>&1 || {
  echo "ERROR: profdata 合并失败"
  echo "RESULT: FAIL"
  exit 1
}
echo ""

# ===== Step 7: 增量覆盖率 =====
echo "=== Step 7: 增量覆盖率 ==="
FIRST_BIN="${TEST_BINS[0]}"

INCREMENTAL_RESULT=$(SRC_DIR="$SRC_DIR" BASE_SHA="$BASE_SHA" HEAD_SHA="$HEAD_SHA" COV="$COV" FIRST_BIN="$FIRST_BIN" INCREMENTAL_CPP="$INCREMENTAL_CPP" python3 << 'PYEOF'
import json, subprocess, os, re, sys

SRC_DIR = os.environ['SRC_DIR']
BASE_SHA = os.environ['BASE_SHA']
HEAD_SHA = os.environ['HEAD_SHA']
COV = os.environ['COV']
FIRST_BIN = os.environ['FIRST_BIN']
INCREMENTAL_CPP = os.environ['INCREMENTAL_CPP']

def get_added_lines(src_dir, base, head):
    result = subprocess.run(['git', '-C', src_dir, 'diff', base, head], capture_output=True, text=True)
    added = {}
    current_file = None
    new_line = 0
    for line in result.stdout.split('\n'):
        if line.startswith('+++ b/'):
            current_file = line[6:]
            added[current_file] = set()
        elif line.startswith('@@'):
            m = re.search(r'\+(\d+)', line)
            if m: new_line = int(m.group(1))
        elif line.startswith('+') and not line.startswith('+++') and current_file:
            added[current_file].add(new_line)
            new_line += 1
        elif not line.startswith('-') and not line.startswith('\\'):
            new_line += 1
    return added

def get_coverage(cov_tool, testbin, profdata, srcfile):
    result = subprocess.run(
        [cov_tool, 'export', testbin, '-instr-profile=' + profdata, '-format=text', srcfile],
        capture_output=True, text=True
    )
    if not result.stdout:
        return {}
    try:
        data = json.loads(result.stdout)
    except:
        return {}
    files = data.get("data", [{}])[0].get("files", [])
    if isinstance(files, list):
        for item in files:
            if isinstance(item, dict):
                segs = item.get("segments", [])
                line_cov = {}
                for seg in segs:
                    if seg[3] == 1:
                        line_cov[seg[0]] = seg[2]
                return line_cov
    elif isinstance(files, dict):
        for fname, fdata in files.items():
            segs = fdata.get("segments", [])
            line_cov = {}
            for seg in segs:
                if seg[3] == 1:
                    line_cov[seg[0]] = seg[2]
            return line_cov
    return {}

added_lines = get_added_lines(SRC_DIR, BASE_SHA, HEAD_SHA)

total_covered = 0
total_exec = 0

for f in INCREMENTAL_CPP.split():
    if f not in added_lines or not added_lines[f]:
        continue
    srcfile = os.path.join(SRC_DIR, f)
    if not os.path.exists(srcfile):
        print("  %s: 文件不存在" % f)
        continue
    line_cov = get_coverage(COV, FIRST_BIN, 'coverage.profdata', srcfile)
    if not line_cov:
        print("  %s: 无覆盖率数据" % f)
        continue
    pr_exec = {ln: line_cov[ln] for ln in added_lines[f] if ln in line_cov}
    covered = sum(1 for c in pr_exec.values() if c > 0)
    total = len(pr_exec)
    if total > 0:
        pct = covered * 100 / total
        print("  %s: %d/%d (%.1f%%)" % (f, covered, total, pct))
        total_covered += covered
        total_exec += total
    else:
        print("  %s: 无可执行新增行" % f)

if total_exec > 0:
    final_pct = total_covered * 100 / total_exec
    print("TOTAL:%d/%d/%.1f" % (total_covered, total_exec, final_pct))
else:
    print("TOTAL:0/0/SKIP")
PYEOF
)

echo ""

TOTAL_LINE=$(echo "$INCREMENTAL_RESULT" | grep "^TOTAL:")
if [ -z "$TOTAL_LINE" ]; then
  echo "ERROR: 无法计算覆盖率"
  echo "RESULT: FAIL"
  exit 1
fi

TOTAL_COVERED=$(echo "$TOTAL_LINE" | cut -d: -f2 | cut -d/ -f1)
TOTAL_EXEC=$(echo "$TOTAL_LINE" | cut -d: -f2 | cut -d/ -f2 | cut -d/ -f1)
FINAL_PCT=$(echo "$TOTAL_LINE" | cut -d/ -f3)

echo "============================================"
echo "  覆盖率汇总（仅 PR 新增可执行行）"
echo "============================================"
echo "$INCREMENTAL_RESULT" | grep -v "^TOTAL:"
echo ""
echo "  总计: $TOTAL_COVERED/$TOTAL_EXEC = ${FINAL_PCT}%"
echo "  阈值: ${MIN_COVERAGE}%"

# HTML 报告
HTML_DIR="$BUILD_DIR/coverage_html"
rm -rf "$HTML_DIR"
$COV show "$FIRST_BIN" \
  -instr-profile=coverage.profdata \
  -format=html \
  -output-dir="$HTML_DIR" \
  $(for f in $INCREMENTAL_CPP; do [ -f "$SRC_DIR/$f" ] && echo "$SRC_DIR/$f"; done) \
  2>/dev/null || true
echo "  HTML: $HTML_DIR/index.html"

# 阈值检查
PASS=$(python3 -c "print('PASS' if $FINAL_PCT >= $MIN_COVERAGE else 'FAIL')")
echo ""
echo "============================================"
echo "  RESULT: $PASS"
echo "  增量覆盖率 ${FINAL_PCT}% $PASS 阈值 ${MIN_COVERAGE}%"
echo "============================================"
[ "$PASS" = "PASS" ] && exit 0 || exit 1
