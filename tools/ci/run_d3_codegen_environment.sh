#!/usr/bin/env bash
set -euo pipefail

: "${MINO_CODEGEN_JOBS:?MINO_CODEGEN_JOBS must be set}"

bazel_configs=(--config=release)
if [[ -n "${MINO_BAZEL_EXTRA_CONFIG:-}" ]]; then
  bazel_configs+=("--config=${MINO_BAZEL_EXTRA_CONFIG}")
fi

bazel test "${bazel_configs[@]}" --jobs="${MINO_CODEGEN_JOBS}" \
  //mino/schema/codegen:code_generator_test \
  //tools/minoc:canonical_wire_generated_test \
  //tools/minoc:minoc_cli_test \
  //tools/minoc:cross_directory_generated_test \
  --test_output=errors

bazel build "${bazel_configs[@]}" --jobs="${MINO_CODEGEN_JOBS}" \
  --output_groups=default,descriptor \
  //tools/minoc:sample_codegen \
  //tools/minoc:canonical_wire_codegen \
  //tools/minoc:mangling_codegen \
  //tools/minoc:sensor_frame_codegen \
  //mino/schema/fuzz:codegen_golden

python3 tools/ci/collect_codegen_artifacts.py \
  --config=release \
  --out=/results/hermetic-codegen

{
  echo "compiler_cc=${CC}"
  echo "compiler_cxx=${CXX}"
  echo "jobs=${MINO_CODEGEN_JOBS}"
  echo "extra_bazel_config=${MINO_BAZEL_EXTRA_CONFIG:-none}"
  echo "tz=${TZ:-unset}"
  echo "source_date_epoch=${SOURCE_DATE_EPOCH:-unset}"
  echo "workspace=${PWD}"
  cat /etc/os-release
  uname -a
  bazel --version
  "${CXX}" --version
} > /results/hermetic-codegen/PROVENANCE.txt

# The container runs as root while the host orchestrator normally does not.
# Preserve readable/traversable evidence across the bind mount boundary.
chmod -R a+rX /results/hermetic-codegen
