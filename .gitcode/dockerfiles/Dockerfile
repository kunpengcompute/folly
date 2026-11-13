# 使用Debian 12官方基础镜像
FROM swr.cn-north-4.myhuaweicloud.com/cloud_boostkit/debian12:latest

# 设置非交互模式与环境变量
ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=Asia/Shanghai \
    CC=/usr/bin/clang-16 \
    CXX=/usr/bin/clang++-16

# 配置华为云APT源 + 系统更新 + 安装全部依赖（合并为单层）
RUN echo "配置华为云APT源..." && \
    # 使用 echo 逐行写入，兼容所有 Docker 版本
    echo 'Types: deb' > /etc/apt/sources.list.d/debian.sources && \
    echo 'URIs: https://mirrors.huaweicloud.com/debian/' >> /etc/apt/sources.list.d/debian.sources && \
    echo 'Suites: bookworm bookworm-updates bookworm-backports' >> /etc/apt/sources.list.d/debian.sources && \
    echo 'Components: main contrib non-free non-free-firmware' >> /etc/apt/sources.list.d/debian.sources && \
    echo 'Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg' >> /etc/apt/sources.list.d/debian.sources && \
    echo '' >> /etc/apt/sources.list.d/debian.sources && \
    echo 'Types: deb' >> /etc/apt/sources.list.d/debian.sources && \
    echo 'URIs: https://mirrors.huaweicloud.com/debian-security/' >> /etc/apt/sources.list.d/debian.sources && \
    echo 'Suites: bookworm-security' >> /etc/apt/sources.list.d/debian.sources && \
    echo 'Components: main contrib non-free non-free-firmware' >> /etc/apt/sources.list.d/debian.sources && \
    echo 'Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg' >> /etc/apt/sources.list.d/debian.sources && \
    echo "配置APT偏好设置..." && \
    mkdir -p /etc/apt/preferences.d && \
    printf 'Package: *\nPin: release a=bookworm\nPin-Priority: 900\n' > /etc/apt/preferences.d/bookworm.pref && \
    echo "更新系统并安装工具..." && \
    apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        apt-utils bash wget ca-certificates gnupg2 lsb-release \
        software-properties-common aptitude gcc g++ make cmake ninja-build \
        autoconf automake libtool pkg-config git maven python3-pip unzip \
        rsync numactl sudo rpm cpio meson byacc bison flex binutils binutils-dev \
        doxygen dos2unix patch zip zlib1g zlib1g-dev strace expect openssh-client \
        openssh-server jq libc6-dev uuid-dev vim tar bzip2 openssl xz-utils \
        libxrender1 libxtst6 libxi6 libgsasl-dev libunwind-dev libnuma-dev \
        libaio-dev libncurses5-dev libncursesw5-dev libcunit1 libcunit1-dev \
        libjson-c-dev libcmocka-dev libiscsi-dev libboost-all-dev libbenchmark-dev \
        libhdf5-dev build-essential libssl-dev lcov libedit-dev libxml2-dev \
        libtinfo-dev liblzma-dev libffi-dev libgoogle-perftools-dev clang-format \
        clang-16 liburing-dev libdouble-conversion-dev libgflags-dev \
        libgoogle-glog-dev libevent-dev libsodium-dev liblz4-dev libsnappy-dev \
        libzstd-dev libfmt-dev libgtest-dev libgmock-dev libdwarf-dev libiberty-dev \
        libbz2-dev \
        python3 python3-dev python3-requests python3-pyelftools python3-gevent \
        python3-marshmallow python3-yaml python3-pandas python3-xlrd \
        python3-retrying python3-jenkins python3-concurrent-log-handler python3-openpyxl \
        openjdk-17-jdk openjdk-17-jre alien perl perl-modules libperl-dev && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /bin/bash /bin/sh && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo "$TZ" > /etc/timezone

WORKDIR /tmp

CMD ["/bin/bash"]