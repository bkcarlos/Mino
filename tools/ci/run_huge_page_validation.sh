#!/usr/bin/env bash
# Manual Linux host validation for real MAP_HUGETLB mappings.
set -euo pipefail

RESULT_DIR="${MINO_HUGE_PAGE_RESULT_DIR:-huge-page-results}"
HUGETLBFS_PATH="${MINO_HUGETLBFS_PATH:-/mnt/mino-hugetlb}"
REQUESTED_HUGEPAGES="${MINO_HUGEPAGES_TO_RESERVE:-8}"
CONFIGURE="${MINO_CONFIGURE_HUGEPAGES:-0}"
NR_HUGEPAGES_FILE=/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
mkdir -p "${RESULT_DIR}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Huge-page validation requires Linux" | tee "${RESULT_DIR}/error.txt"
  exit 2
fi
if ! [[ "${REQUESTED_HUGEPAGES}" =~ ^[0-9]+$ ]] ||
   [[ "${REQUESTED_HUGEPAGES}" -lt 2 ]]; then
  echo "MINO_HUGEPAGES_TO_RESERVE must be an integer >= 2" \
    | tee "${RESULT_DIR}/error.txt"
  exit 2
fi
if [[ ! -r "${NR_HUGEPAGES_FILE}" ]]; then
  echo "2 MiB huge-page sysfs is unavailable" | tee "${RESULT_DIR}/error.txt"
  exit 2
fi

ORIGINAL_NR_HUGEPAGES="$(cat "${NR_HUGEPAGES_FILE}")"
PATH_EXISTED=0
ORIGINAL_MOUNTED=0
MOUNTED_BY_SCRIPT=0
ORIGINAL_UID=""
ORIGINAL_GID=""
ORIGINAL_MODE=""
ORIGINAL_MOUNT_SOURCE=""
ORIGINAL_MOUNT_FSTYPE=""
ORIGINAL_MOUNT_OPTIONS=""

if [[ -e "${HUGETLBFS_PATH}" ]]; then
  PATH_EXISTED=1
  ORIGINAL_UID="$(stat -c %u "${HUGETLBFS_PATH}")"
  ORIGINAL_GID="$(stat -c %g "${HUGETLBFS_PATH}")"
  ORIGINAL_MODE="$(stat -c %a "${HUGETLBFS_PATH}")"
fi
if mountpoint -q "${HUGETLBFS_PATH}"; then
  ORIGINAL_MOUNTED=1
  ORIGINAL_MOUNT_SOURCE="$(findmnt -n -o SOURCE --target "${HUGETLBFS_PATH}")"
  ORIGINAL_MOUNT_FSTYPE="$(findmnt -n -o FSTYPE --target "${HUGETLBFS_PATH}")"
  ORIGINAL_MOUNT_OPTIONS="$(findmnt -n -o OPTIONS --target "${HUGETLBFS_PATH}")"
fi

write_original_state() {
  {
    echo "original_nr_hugepages=${ORIGINAL_NR_HUGEPAGES}"
    echo "path=${HUGETLBFS_PATH}"
    echo "path_existed=${PATH_EXISTED}"
    echo "original_mounted=${ORIGINAL_MOUNTED}"
    echo "original_mount_source=${ORIGINAL_MOUNT_SOURCE}"
    echo "original_mount_fstype=${ORIGINAL_MOUNT_FSTYPE}"
    echo "original_mount_options=${ORIGINAL_MOUNT_OPTIONS}"
    echo "original_uid=${ORIGINAL_UID}"
    echo "original_gid=${ORIGINAL_GID}"
    echo "original_mode=${ORIGINAL_MODE}"
    echo "requested_nr_hugepages=${REQUESTED_HUGEPAGES}"
    echo "configure=${CONFIGURE}"
  } >"${RESULT_DIR}/original-host-state.txt"
}

collect_host() {
  local phase="$1"
  {
    echo "phase=${phase}"
    uname -a
    id
    echo "hugetlbfs_path=${HUGETLBFS_PATH}"
    echo "nr_hugepages=$(cat "${NR_HUGEPAGES_FILE}" 2>/dev/null || echo unavailable)"
    if [[ -e "${HUGETLBFS_PATH}" ]]; then
      stat -c 'path_uid=%u path_gid=%g path_mode=%a' "${HUGETLBFS_PATH}"
    fi
    findmnt -n --target "${HUGETLBFS_PATH}" 2>/dev/null || true
  } >"${RESULT_DIR}/host-${phase}.txt"
  cat /proc/meminfo >"${RESULT_DIR}/meminfo-${phase}.txt"
  cat /proc/mounts >"${RESULT_DIR}/mounts-${phase}.txt"
  find /sys/kernel/mm/hugepages -maxdepth 2 -type f -print -exec cat {} \; \
    >"${RESULT_DIR}/hugepages-sysfs-${phase}.txt" 2>&1 || true
}

restore_host() {
  local command_status=$?
  set +e
  local restore_status=0
  if [[ "${CONFIGURE}" == "1" ]]; then
    # Never restore below the reservation that existed when the job started.
    echo "${ORIGINAL_NR_HUGEPAGES}" | sudo tee "${NR_HUGEPAGES_FILE}" >/dev/null ||
      restore_status=1
    local restored_nr
    restored_nr="$(cat "${NR_HUGEPAGES_FILE}" 2>/dev/null)"
    if [[ -z "${restored_nr}" ||
          "${restored_nr}" -ne "${ORIGINAL_NR_HUGEPAGES}" ]]; then
      restore_status=1
    fi
    if [[ "${MOUNTED_BY_SCRIPT}" == "1" ]]; then
      sudo umount "${HUGETLBFS_PATH}" || restore_status=1
    fi
    if [[ "${PATH_EXISTED}" == "1" ]]; then
      sudo chown "${ORIGINAL_UID}:${ORIGINAL_GID}" "${HUGETLBFS_PATH}" ||
        restore_status=1
      sudo chmod "${ORIGINAL_MODE}" "${HUGETLBFS_PATH}" || restore_status=1
    elif [[ "${MOUNTED_BY_SCRIPT}" == "1" ]]; then
      sudo rmdir "${HUGETLBFS_PATH}" || restore_status=1
    fi
  fi
  collect_host restored
  {
    echo "command_exit_code=${command_status}"
    echo "restore_exit_code=${restore_status}"
    echo "restored_nr_hugepages=$(cat "${NR_HUGEPAGES_FILE}" 2>/dev/null || echo unavailable)"
  } >"${RESULT_DIR}/restore-result.txt"
  if [[ ! -f "${RESULT_DIR}/result.txt" ]]; then
    printf 'exit_code=%s\n' "${command_status}" >"${RESULT_DIR}/result.txt"
  fi
  local final_status="${command_status}"
  if [[ "${restore_status}" -ne 0 ]]; then
    echo "Host restoration failed; inspect restore-result.txt" \
      | tee -a "${RESULT_DIR}/error.txt"
    final_status=7
  fi
  trap - EXIT
  exit "${final_status}"
}

write_original_state
collect_host before
trap restore_host EXIT
trap 'exit 130' INT TERM

if [[ "${CONFIGURE}" == "1" ]]; then
  target_hugepages="${REQUESTED_HUGEPAGES}"
  if [[ "${ORIGINAL_NR_HUGEPAGES}" -gt "${target_hugepages}" ]]; then
    target_hugepages="${ORIGINAL_NR_HUGEPAGES}"
  fi
  # This write can only preserve or increase the pre-existing reservation.
  if [[ "${target_hugepages}" -gt "${ORIGINAL_NR_HUGEPAGES}" ]]; then
    echo "${target_hugepages}" | sudo tee "${NR_HUGEPAGES_FILE}" >/dev/null
  fi
  configured_nr="$(cat "${NR_HUGEPAGES_FILE}")"
  if [[ "${configured_nr}" -lt "${ORIGINAL_NR_HUGEPAGES}" ]]; then
    echo "Refusing a configuration that lowers the existing reservation" \
      | tee "${RESULT_DIR}/error.txt"
    exit 3
  fi
  if [[ "${configured_nr}" -lt "${target_hugepages}" ]]; then
    echo "Kernel reserved ${configured_nr}, below requested target ${target_hugepages}" \
      | tee "${RESULT_DIR}/error.txt"
    exit 3
  fi

  if [[ "${ORIGINAL_MOUNTED}" == "1" ]]; then
    if [[ "${ORIGINAL_MOUNT_FSTYPE}" != "hugetlbfs" ]]; then
      echo "Existing mount at ${HUGETLBFS_PATH} is not hugetlbfs" \
        | tee "${RESULT_DIR}/error.txt"
      exit 3
    fi
  else
    sudo mkdir -p "${HUGETLBFS_PATH}"
    sudo mount -t hugetlbfs -o pagesize=2M none "${HUGETLBFS_PATH}"
    MOUNTED_BY_SCRIPT=1
  fi
  sudo chown "$(id -u):$(id -g)" "${HUGETLBFS_PATH}"
  sudo chmod 0700 "${HUGETLBFS_PATH}"
fi
collect_host configured

if ! mountpoint -q "${HUGETLBFS_PATH}" ||
   [[ "$(stat -f -c %T "${HUGETLBFS_PATH}")" != "hugetlbfs" ]]; then
  echo "${HUGETLBFS_PATH} is not a hugetlbfs mount" \
    | tee "${RESULT_DIR}/error.txt"
  exit 4
fi
free_pages="$(awk '/^HugePages_Free:/ {print $2}' /proc/meminfo)"
if [[ -z "${free_pages}" || "${free_pages}" -lt 2 ]]; then
  echo "At least two free 2 MiB huge pages are required; found ${free_pages:-unknown}" \
    | tee "${RESULT_DIR}/error.txt"
  exit 5
fi

set +e
bazel test --config=gcc12 //mino/platform:shared_memory_huge_page_test \
  --test_output=all --nocache_test_results --test_timeout=180 \
  --test_env="MINO_HUGETLBFS_PATH=${HUGETLBFS_PATH}" \
  >"${RESULT_DIR}/bazel-console.log" 2>&1
status=$?
set -e
cp -f bazel-testlogs/mino/platform/shared_memory_huge_page_test/test.log \
  "${RESULT_DIR}/test.log" 2>/dev/null || true
cp -f bazel-testlogs/mino/platform/shared_memory_huge_page_test/test.xml \
  "${RESULT_DIR}/test.xml" 2>/dev/null || true
if [[ "${status}" -eq 0 ]] &&
   grep -q '\[  SKIPPED \]' "${RESULT_DIR}/test.log" 2>/dev/null; then
  echo "Prepared-host validation unexpectedly skipped a huge-page test" \
    | tee "${RESULT_DIR}/error.txt"
  status=6
fi
printf 'exit_code=%s\nfree_hugepages_before_test=%s\n' "${status}" "${free_pages}" \
  >"${RESULT_DIR}/result.txt"
cat "${RESULT_DIR}/bazel-console.log"
exit "${status}"
