#!/bin/sh
set -eu

root="${TEST_SRCDIR}/${TEST_WORKSPACE}"
tool="${root}/tools/minoc/minoc"
sample="${root}/tools/minoc/tests/testdata/sample.mino"
with_import="${root}/tools/minoc/tests/testdata/with_import.mino"
common="${root}/tools/minoc/tests/testdata/common.mino"
duplicate_a="${root}/tools/minoc/tests/testdata/duplicate_a.mino"
duplicate_b="${root}/tools/minoc/tests/testdata/duplicate_b.mino"
duplicate_root="${root}/tools/minoc/tests/testdata/duplicate_root.mino"
physical_alias="${root}/tools/minoc/tests/testdata/physical_alias.mino"
out="${TEST_TMPDIR}/minoc_cli"
mkdir -p "${out}/one" "${out}/two" "${out}/imported"

"${tool}" \
  --input "${sample}" \
  --output_header "${out}/one/sample.generated.h" \
  --output_source "${out}/one/sample.generated.cc" \
  --output_descriptor "${out}/one/sample.descriptor" \
  --header_include sample.generated.h
"${tool}" \
  --input "${sample}" \
  --output_header "${out}/two/sample.generated.h" \
  --output_source "${out}/two/sample.generated.cc" \
  --output_descriptor "${out}/two/sample.descriptor" \
  --header_include sample.generated.h
cmp "${out}/one/sample.generated.h" "${out}/two/sample.generated.h"
cmp "${out}/one/sample.generated.cc" "${out}/two/sample.generated.cc"
cmp "${out}/one/sample.descriptor" "${out}/two/sample.descriptor"

set +e
"${tool}" \
  --input "${with_import}" \
  --output_header "${out}/missing.generated.h" \
  --output_source "${out}/missing.generated.cc" \
  --output_descriptor "${out}/missing.descriptor" \
  2>"${out}/missing.stderr"
status=$?
set -e
if [ "${status}" -ne 4 ]; then
  echo "expected undeclared import exit 4, got ${status}" >&2
  cat "${out}/missing.stderr" >&2
  exit 1
fi
grep "undeclared import 'tools/minoc/tests/testdata/common.mino'" "${out}/missing.stderr"
test ! -e "${out}/missing.generated.h"
test ! -e "${out}/missing.generated.cc"
test ! -e "${out}/missing.descriptor"

"${tool}" \
  --input "${with_import}" \
  --import "tools/minoc/tests/testdata/common.mino=${common}" \
  --output_header "${out}/imported/located.generated.h" \
  --output_source "${out}/imported/located.generated.cc" \
  --output_descriptor "${out}/imported/located.descriptor" \
  --header_include located.generated.h
test -s "${out}/imported/located.descriptor"

set +e
"${tool}" \
  --input "${duplicate_root}" \
  --import "tools/minoc/tests/testdata/duplicate_a.mino=${duplicate_a}" \
  --import "tools/minoc/tests/testdata/duplicate_b.mino=${duplicate_b}" \
  --output_header "${out}/duplicate.generated.h" \
  --output_source "${out}/duplicate.generated.cc" \
  --output_descriptor "${out}/duplicate.descriptor" \
  2>"${out}/duplicate.stderr"
status=$?
set -e
test "${status}" -eq 4
grep "maps to multiple digests" "${out}/duplicate.stderr"

set +e
"${tool}" \
  --input "${physical_alias}" \
  --import "alias/one.mino=${common}" \
  --import "alias/two.mino=${common}" \
  --output_header "${out}/alias.generated.h" \
  --output_source "${out}/alias.generated.cc" \
  --output_descriptor "${out}/alias.descriptor" \
  2>"${out}/alias.stderr"
status=$?
set -e
test "${status}" -eq 4
grep "physically alias" "${out}/alias.stderr"
