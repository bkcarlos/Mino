

ARG BASE_IMAGE=ubuntu:24.04
FROM ${BASE_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ARG COMPILER_PACKAGES
ARG CC
ARG CXX
ARG WORKSPACE_PATH
ARG BAZELISK_VERSION=v1.29.0
ARG BAZELISK_SHA256=5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992

ENV CC=${CC}
ENV CXX=${CXX}
ENV LANG=C.UTF-8

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        coreutils \
        curl \
        diffutils \
        findutils \
        ${COMPILER_PACKAGES} \
        libatomic1 \
        python3 \
        unzip \
        zip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
        -o /tmp/bazelisk \
        "https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-linux-amd64" \
    && echo "${BAZELISK_SHA256}  /tmp/bazelisk" | sha256sum --check --strict \
    && install -m 0755 /tmp/bazelisk /usr/local/bin/bazel \
    && rm /tmp/bazelisk

COPY . ${WORKSPACE_PATH}
WORKDIR ${WORKSPACE_PATH}

# Download Bazel itself and all external repositories while image builds still
# have network access. The actual comparison containers run with --network=none.
RUN bazel fetch --config=release \
        //mino/schema/codegen:code_generator_test \
        //tools/minoc:canonical_wire_generated_test \
        //tools/minoc:minoc_cli_test \
        //tools/minoc:cross_directory_generated_test \
        //tools/minoc:sample_codegen \
        //tools/minoc:canonical_wire_codegen \
        //tools/minoc:mangling_codegen \
        //tools/minoc:sensor_frame_codegen \
        //mino/schema/fuzz:codegen_golden
