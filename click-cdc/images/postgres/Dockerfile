FROM bitnamilegacy/postgresql:17.6.0-debian-12-r4

# На всякий случай явно объявим PG_MAJOR
ARG PG_MAJOR=15

USER root

RUN \
    apt-get update \
    && apt install -y wget gnupg ca-certificates

RUN \
    tee -a /etc/apt/sources.list > /dev/null <<'EOF'
    deb http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye main
    deb-src http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye main
    # 15
    deb http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-11 main
    deb-src http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-11 main
EOF

RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -

RUN apt-get update

RUN \
    apt-get install -f -y --no-install-recommends \
        software-properties-common \
        build-essential \
        pkg-config \
        git \
        clang-13 \
        llvm-13 \
    && add-apt-repository "deb http://ftp.debian.org/debian testing main contrib" \
    && apt-get update \
    && apt-get install -f -y --no-install-recommends \
        libprotobuf-c-dev=1.5.* \
    && git clone https://github.com/debezium/postgres-decoderbufs -b v3.4.1.Final --single-branch \
    && cd postgres-decoderbufs \
    && make \
    && make install \
    && cd / \
    && rm -rf postgres-decoderbufs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Возвращаем стандартного пользователя bitnami
USER 1001
