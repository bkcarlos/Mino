# V-13：Linux AArch64 qualification 方法

## 状态与边界

本文件定义 D6-18 / V-13 的可执行方法、门禁和 artifact 契约，不记录尚未发生的原生运行结果。`cross-build + QEMU` 仅是 PR 功能 smoke，不能作为原生正确性、性能或发布资格证据。

原生 qualification 入口：

```bash
python3 tools/ci/aarch64_validation.py \
  --expected-commit="$(git rev-parse HEAD)" \
  --native-attestation=physical-aarch64 \
  --expected-governor=performance \
  --bazel-config=gcc12 \
  --output-dir=/var/tmp/mino-aarch64-v13
```

`--output-dir` 必须位于 Git 工作树外，避免 runner 自己创建 artifact 后把 source state 变为 dirty。

## 1. 原生 qualification 测试矩阵

所有组使用 Linux AArch64 原生进程、`--config=release`、禁用 test result cache，并保存每组完整命令、stdout/stderr、退出码、耗时和日志 SHA-256。

| 组 | Bazel suite | 覆盖 |
|---|---|---|
| atomic/litmus | `//tests/aarch64:atomic_litmus_tests` | bool/u16/u32/u64 always lock-free 与运行时 lock-free；共享映射 CAS/fetch_add；fork 跨进程 seq_cst litmus；128-bit 仅报告，不作为 v1 ABI 前提 |
| ABI | `//tests/aarch64:abi_tests` | `SuperBlock` 256 B / align 8、`ShmHandle` 16 B / align 8、`IndexSlot` 128 B / align 64、字段 offset/CRC，以及 ≤64-bit atomic 编译/运行 smoke |
| Region | `//tests/aarch64:region_tests` | Create/Attach/Recovery、directory、liveness、HandleResolver、不同虚拟基址的跨进程 Handle、短 kill stress |
| MPMC | `//tests/aarch64:mpmc_tests` | 单进程与 fork + MAP_SHARED conservation/wraparound/full-empty |
| Channel | `//tests/aarch64:channel_tests` | SPSC/MPSC/Broadcast 单进程和跨进程路径，包括 MPSC orphan reservation 与 Broadcast fan-out/tombstone |
| Bridge | `//tests/aarch64:bridge_tests` | Wire/CRC/reliability/schema/pipeline/connection manager、loopback TCP 双节点与 remote reconstruction |
| Storage | `//tests/aarch64:storage_tests` | Segment/manifest/schema/recovery/recorder/replay/retention/snapshot/topic writer、buffer 与 fault tests |

这些 suite 只聚合现有生产测试，不修改 runtime/security 实现。hour-long/manual stress、fuzz campaign、物理双机和 huge-page privileged 验证仍由各自 workflow 负责，不在 V-13 中重复冒充。

## 2. fail-closed runner contract

原生结果只有在以下条件全部成立时才可设置 `qualification_eligible=true`：

1. `platform.system() == Linux` 且 `uname -m` 精确为 `aarch64`；`arm64`、`x86_64` 均拒绝。
2. `HEAD` 与完整 40 字符 `--expected-commit` 一致。
3. `git status --porcelain=v1 --untracked-files=all` 为空。
4. `/proc/cpuinfo`、Device Tree、DMI、`systemd-detect-virt` 和 QEMU 环境变量中没有 QEMU/TCG 指示，且操作者显式提供 `physical-aarch64` attestation。该检查用于拒绝已知仿真环境；硬件资产真实性仍由 self-hosted runner 管理和审计保证。
5. 每个在线、暴露 cpufreq 的 CPU 都有 governor 信息，且全部等于 `--expected-governor`；信息缺失或混合 governor 均失败。runner 只检查和记录，不用 sudo 修改宿主机策略。
6. 七个测试组、五类 benchmark、每个必须的 JSON/log 都存在、非空、退出码为零且可解析；测试使用完整输出，任何 GoogleTest `[ SKIPPED ]` 都按资格失败处理。
7. AArch64 独立 SLA 全部通过；manifest、原始 JSON/log 和二进制 smoke（仅 cross job）使用 SHA-256。

contract 回归：

```bash
bazel test //tests/aarch64:runner_contract_test
```

## 3. Release benchmark 矩阵

runner 先用同一个 `--config=release` 构建全部目标，然后执行：

| 类别 | 目标/负载 | 资格输出 |
|---|---|---|
| allocator | `//benchmarks/allocator:allocator_benchmark`，legacy scan 与 cursor cache | CSV、failures、ops/s、cache/legacy 比率 |
| channel | `//benchmarks:validation_benchmark --suite=memory`，V-14 1/2/8/16/64 subscribers | 原生 benchmark JSON、64-subscriber fan-out p99、所有 errors |
| bridge | `//benchmarks/transport:layer_comparison_benchmark`，Mino L2 loopback server/client、4 topics、2 lanes | client/server JSON 与日志、p99 RTT、messages/s；这不是物理双机网络 SLA |
| storage | `//benchmarks:storage_benchmark`，1 KiB、20k records、per-batch | encode、writer、fdatasync、recovery、buffer MPSC JSON |
| telemetry | `//mino/observability:v23_telemetry_benchmark`，5M operations | baseline/off/counters/1% sampled/full 的 ns/op、overhead、samples/dropped |

硬件、内核、编译器、commit、完整 argv、CPU governor、内存、块设备、文件系统与所有 JSON/log hash 进入 manifest。`tests/aarch64/benchmark_sla.json` 分开定义：

- `aarch64`：V-13 的独立 pass/fail 阈值；
- `x86_reference`：已有正式 x86 Storage SLA 及其来源；其他类别在仓库尚无已发布数值 SLA，明确标记 `NO_PUBLISHED_NUMERIC_SLA`，保留同负载原始结果供后续比较，不制造数字。

Storage 同时展示 x86 参考门槛和 AArch64 实测值，但只按 AArch64 独立门槛判定 V-13。新增正式 x86 基线时应更新受评审的 SLA JSON，而不是手工改 manifest。

## 4. Artifact 结构

JSON Schema：`docs/validation/AArch64_artifact.schema.json`。

```text
<output-dir>/
├── manifest.json
├── logs/
│   ├── test-*.log
│   ├── build-release-benchmarks.log
│   └── benchmark-*.log
└── benchmarks/
    ├── allocator.csv
    ├── channel-validation.json
    ├── bridge-client.json
    ├── bridge-server.json
    └── storage-1k.json
```

`manifest.json` 记录 preflight、host provenance、逐命令证据、派生 measurement、AArch64 SLA checks、x86 reference comparison 和 artifact hash。失败运行也必须上传 manifest 与已产生日志，但始终保持 `qualification_eligible=false`、`outcome=failed`、`artifacts_complete=false`。

## 5. PR cross-build/QEMU smoke

在有 `aarch64-linux-gnu-g++`、`readelf`、`qemu-aarch64` 的 x86 Linux 主机上：

```bash
python3 tools/ci/aarch64_cross_smoke.py --output-dir=/tmp/mino-aarch64-cross-smoke
```

该任务 cross-build `tests/aarch64/abi_atomic_smoke_test.cc` 为静态 AArch64 ELF，检查 ELF machine 后用 QEMU user-mode 执行。其 manifest schema 为 `mino.aarch64.cross_smoke_manifest.v1`，永久固定 `qualification_eligible=false`，不运行 benchmark，也不参与性能比较。

## 6. GitHub Actions 分工

`.github/workflows/aarch64-validation.yml` 有两个隔离 job：

- hosted `ubuntu-24.04`：PR/push 的 cross-build + QEMU smoke，artifact 保留 30 天；
- self-hosted `[self-hosted, linux, ARM64]`：仅 schedule/manual 的原生 qualification 与 release benchmark，artifact 保留 180 天。

QEMU job 的名称、manifest 和 assertion 都明确为 non-qualification；workflow 不在 QEMU 下执行任何性能 benchmark。
