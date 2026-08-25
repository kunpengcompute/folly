cmake_minimum_required(VERSION 3.10)
project(UrmaEchoDemo CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

# ── folly 路径 ──
set(FOLLY_ROOT "" CACHE PATH "folly 源码根目录")
set(FOLLY_BUILD "" CACHE PATH "folly 构建目录(含 libfolly.a)")

if(NOT FOLLY_ROOT)
    set(FOLLY_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/..")
endif()
if(NOT FOLLY_BUILD)
    set(FOLLY_BUILD "${FOLLY_ROOT}/_build")
endif()

# ── URMA 开关 ──
option(USE_URMA "Enable URMA transport" ON)

# ── include ──
include_directories(
    ${FOLLY_ROOT}
    ${FOLLY_BUILD}
)

# ── 编译定义 ──
add_compile_definitions(
    FMT_SHARED
    DGFLAGS_IS_A_DLL=0
    _GNU_SOURCE
    _REENTRANT
    BOOST_CONTEXT_DYN_LINK
    BOOST_SYSTEM_DYN_LINK
)

if(USE_URMA)
    add_compile_definitions(FOLLY_HAVE_URMA)
endif()

# ── 可执行文件 ──
add_executable(UrmaEchoDemo UrmaEchoDemo.cpp)

# ── 链接 folly（静态库放后面，解决符号依赖顺序）──
target_link_libraries(UrmaEchoDemo PRIVATE
    ${FOLLY_BUILD}/libfolly.a
    glog gflags double-conversion fmt
    boost_context boost_system boost_filesystem boost_thread boost_program_options boost_regex
    ssl crypto
    zstd z bz2 lz4 snappy lzma
    unwind sodium dwarf iberty event
    pthread dl rt gomp
)
