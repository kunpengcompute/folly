echo 'hello'

cat /etc/os-release

export LANG=C.UTF-8
export LC_ALL=C.UTF-8

echo "正在修复 Debian 软件源配置..."

# 1. 备份原有配置
if [ -f /etc/apt/sources.list ]; then
    cp /etc/apt/sources.list /etc/apt/sources.list.bak
fi

# 2. 写入清华源配置 (Debian 12 Bookworm)
# 使用 EOF 避免转义问题，同时覆盖写入
cat <<EOF > /etc/apt/sources.list
deb https://mirrors.tuna.tsinghua.edu.cn/debian/ bookworm main contrib non-free
deb https://mirrors.tuna.tsinghua.edu.cn/debian/ bookworm-updates main contrib non-free
deb https://mirrors.tuna.tsinghua.edu.cn/debian-security/ bookworm-security main contrib non-free
EOF

# sed -i "s|repo.openeuler.org|mirrors.huaweicloud.com/openeuler|g" /etc/yum.repos.d/openEuler.repo

pwd

ls -lart

case "${codeBranch}" in
	master|br_bk26_930)

        git config --global http.version HTTP/1.1
        for src in https://gitcode.com/facebook/folly.git https://gitee.com/mirrors/folly.git https://github.com/facebook/folly.git; do
            echo "尝试克隆源: ${src}"
            if git clone -b v2022.11.14.00 --depth 1 "${src}" folly-kunpeng; then
                break
            fi
            rm -rf folly-kunpeng
        done
        if [ ! -d folly-kunpeng ]; then
            echo "ERROR: folly 基线源码克隆失败（gitee/github 源均不可用）"
            exit 1
        fi
        cd folly-kunpeng || { echo "ERROR: 进入 folly-kunpeng 目录失败"; exit 1; }

        patch -p1 < ../folly_iobuf_iouring.patch || { echo "ERROR: 应用优化补丁失败"; exit 1; }

        ;;

    *)
        echo "分支名：${codeBranch}"
        ;;
esac


ZSTD_REAL_PATH=$(find /usr -name "libzstd.so*" 2>/dev/null | head -n 1)

echo "libzstd.so : ${ZSTD_REAL_PATH}"

#
mkdir -p _build
cd _build
rm * -rf
cmake .. \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_CXX_STANDARD=17 \
-DBUILD_BENCHMARKS=OFF \
-DBUILD_TESTS=ON \
-DCMAKE_INSTALL_PREFIX=/home/test/folly \
-DBUILD_SHARED_LIBS=ON \
-DCMAKE_PREFIX_PATH="/usr/bin/cmake/fmt/" \
-DZSTD_LIBRARY_RELEASE=/usr/lib/aarch64-linux-gnu/libzstd.so.1.5.4 \
|| { echo "ERROR: cmake 配置失败"; exit 1; }

make -j16 || { echo "ERROR: 编译失败"; exit 1; }

pwd

ls -lart


case "${codeBranch}" in
	dev_urma_20221104)

        ;;

    *)
        echo "分支名：${codeBranch}"
        ./async_iouring_socket_test
        ;;
esac

