# Linux Huge Page shared memory

Mino 使用一个小型 POSIX SHM **marker** 作为命名对象的唯一权威目录。marker 永远不是
用户数据：实际数据只能位于独立的 hugetlbfs 文件或独立 POSIX SHM data object。

## Marker v2 与状态机

marker 保存固定版本、双槽 copy-on-publish generation、每槽完整 payload CRC32，以及：

- `CREATING / HUGE_READY / FALLBACK_READY / UNLINKING` 状态；
- Huge Page requested、actual backing kind、fallback reason/errno；
- data size、实际 page size；
- creator `ProcessIdentity`；
- canonical hugetlbfs mount path 和 mount device；
- backing 的精确 device/inode；
- hugetlbfs 文件绝对路径或随机 POSIX data object 名称。

写者持有 marker 的进程级独占 `flock`，先完整写入并 `msync` 非活动 CRC 槽，再以一个
原子 published word 切换 generation/active slot。读者只接受 published word 前后一致、
版本匹配且 CRC 正确的活动槽；进程死于任意 payload 写入点时，上一活动槽仍可恢复。

```text
marker absent
    -> CREATING
        -> HUGE_READY
        -> FALLBACK_READY
READY -> UNLINKING -> marker absent
```

`Open` 只允许两个 READY 状态：

- `CREATING`：最多等待 `SharedMemoryOpenOptions::creating_wait_timeout_ms`，然后返回
  `kWouldBlock`；
- `UNLINKING`：立即返回 `kWouldBlock`；
- `HUGE_READY`：从 marker 读取 canonical mount 和 backing 路径，验证 hugetlbfs 类型、
  mount device、page size、文件 device/inode/size 后才使用 `MAP_HUGETLB`；
- `FALLBACK_READY`：打开 marker 记录的随机 POSIX data name，验证 device/inode/size 后
  进行普通 `MAP_SHARED`。

因此 `Open` 不读取 `MINO_HUGETLBFS_PATH`、不拼接同名路径，也绝不会把 marker 映射为
普通数据。`huge_pages_requested()` 在跨进程 `Open` 后仍来自持久化 marker，不再丢失。

## Create 与崩溃恢复

`Create` 首先以 `O_EXCL` 创建 marker，写入并 fsync 一个有效 `CREATING` 快照，然后才创建
backing。backing 使用随机、marker 先记录的候选名称；创建后立即持久化 device/inode，
最后才发布 READY。

遇到已有 marker 时：

- READY 返回 `kAlreadyExists`；
- 活着或无法验证的 CREATING owner 不被接管；
- 已死亡 owner 的 CREATING 可在取得独占 marker lock 后转为 UNLINKING，清理已记录 backing，
  删除 marker 并重新创建；
- 遗留 UNLINKING 可继续重试清理。

如果崩溃发生在 backing 创建与 inode 发布之间，恢复仅收养 marker 中预先持久化、带 Mino
随机前缀的候选，并在删除前记录和验证其 identity。普通错误返回由创建事务 guard 执行同样
清理；测试故障点使用真实子进程 `_exit`，不会运行 guard。

## Unlink 安全性

`Unlink` 取得 marker 独占锁后先发布 CRC 有效的 `UNLINKING`，再删除 backing，marker 最后
删除。任何中途失败都会保留可重试 tombstone。

Huge backing 删除不按配置猜路径：它打开 marker 记录的 mount directory，验证
hugetlbfs/device/page size，以记录的 basename `openat(O_NOFOLLOW)`，核对精确 device/inode，
最后 `unlinkat`。identity 不匹配返回错误并保留 marker，不会删除任意同名文件。

## API 状态

- `huge_pages_requested()`：创建者是否请求 Huge Page；由 marker 跨进程持久化；
- `huge_pages_actual()` / `huge_page_enabled()`：当前 mapping 是否为 Huge backing；
- `actual_page_size()`：marker 中经过 backing/mount 验证的页大小；
- `huge_page_fallback_reason()` / `huge_page_fallback_errno()`：fallback 分类。

`SharedMemoryCreateOptions::hugetlbfs_path` 仅用于 Create 选择候选 mount；为空时依次使用
`MINO_HUGETLBFS_PATH` 和 `/dev/hugepages`。Open/Unlink 完全忽略当前配置，只信 marker。

## 测试分层

常规测试：

```sh
bazel test //mino/platform:shared_memory_test \
  //mino/platform:shared_memory_concurrency_test
```

覆盖 marker/data 分离、CRC 与 identity、确定性 fallback、并发 Create、并发 Open/Unlink、
CREATING 有界等待，以及以下真实进程故障：

- 写入 CREATING 后停止/被杀；
- fallback backing identity 持久化后崩溃；
- 发布 UNLINKING 后崩溃并重试。

Linux manual target：

```sh
bazel test //mino/platform:shared_memory_huge_page_test \
  --test_output=all --nocache_test_results \
  --test_env=MINO_HUGETLBFS_PATH=/mnt/mino-hugetlb
```

它验证真实 `MAP_HUGETLB`、`/proc/self/smaps` 页大小/backing、marker mount/device/inode/page
size/CRC、跨进程 Open/读写、配置路径改变后仍按 marker Open/Unlink，以及 Huge Page Region
Attach + allocator/HandleResolver。没有预留页时 gtest 明确 **SKIP**；手工 CI 在已准备主机上
若发现 SKIP 则失败，不能作为 Huge Page 成功证据。

## 手工 CI 与主机恢复

`.github/workflows/huge-page-validation.yml` 仅 `workflow_dispatch`，要求 runner labels：

```text
self-hosted, linux, mino-hugepage
```

runner 在修改前记录：

- 原始 2 MiB `nr_hugepages`；
- mount source/fstype/options；
- mount path 是否原已存在；
- owner UID/GID 和 mode。

配置目标为 `max(原 nr_hugepages, 请求值)`，脚本禁止降低已有 reservation。EXIT trap 在成功、
测试失败或配置失败时都尝试恢复原 reservation、卸载脚本创建的 mount、恢复原 owner/mode，
并归档恢复结果；恢复失败会令 workflow 失败。

```sh
MINO_CONFIGURE_HUGEPAGES=1 \
MINO_HUGEPAGES_TO_RESERVE=8 \
MINO_HUGETLBFS_PATH=/mnt/mino-hugetlb \
MINO_HUGE_PAGE_RESULT_DIR=huge-page-results \
  bash tools/ci/run_huge_page_validation.sh
```

artifact 保留 90 天，包含 original host state、配置前/后/恢复后的 meminfo、mounts、sysfs、
owner/mode、Bazel console、test log/XML、命令退出码和恢复退出码。
