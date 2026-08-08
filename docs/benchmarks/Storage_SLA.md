# Mino Storage SLA 与基线报告

- 状态：正式基线（单机、单 Recording Owner）
- 测试日期：2026-08-02
- Benchmark schema：`mino.storage_benchmark.v1`
- 可复现目标：`//benchmarks:storage_benchmark`

## 1. 适用范围

本 SLA 只覆盖首版 Storage：Schema Store、Recorder Buffer、Per-Topic/Partition Single Writer、Segment/Manifest、Crash Recovery、Replay 和 Retention。它不承诺分布式一致性日志、跨 Recorder exactly-once、多个 Writer 并发写同一 Segment，或底层硬件在断电时违反 flush/FUA 语义仍可持久。

“Durable”表示 Mino 已完成配置要求的 `fdatasync/fsync` 和必要的目录同步；最终介质保证仍依赖文件系统、挂载选项、控制器和设备固件。

## 2. 正确性 SLA

以下是版本语义保证，不依赖性能基线：

1. Schema Descriptor 和 Session Schema Ref Table 在引用它们的 Durable Record 之前持久化。
2. 一个活动 Segment 只有一个逻辑 Writer；同一 Partition 的 `ingestion_sequence` 严格递增。
3. Record 只有在首尾长度、Header/Payload/Record CRC32C 和 Commit Marker 全部通过后才可恢复。
4. 崩溃恢复只截断不完整尾部；已提交的内部损坏 fail closed，不静默跳过。
5. Recorder Buffer 有全局和按 Topic 字节上限；满载按显式策略 block/drop/fail，drop 必须产生可观测结果和 Gap debt。
6. `BUFFERED`、`WRITTEN`、`DURABLE` 确认级别不互相冒充。
7. Manifest 更新使用 temporary write → `fdatasync` → atomic rename → parent `fsync`。
8. Replay 默认进入独立 namespace；写入 live namespace 必须显式授权。
9. Retention 永不删除 OPEN Segment；先关闭新 Pin，再等待现存 Pin/Lease 清零，最后 unlink 并同步目录。
10. ENOSPC/EIO/EROFS 后 Writer 进入 sticky ERROR，不自动恢复，不静默丢失。

对应验证：`//mino/storage:storage_fault_test`、`//mino/storage:recorder_test`、`//mino/storage:segment_recovery_test`、`//mino/storage:replay_engine_test`。

## 3. 基线硬件与软件

| 项目 | 基线 |
|---|---|
| CPU | Intel Core i9-9900KS，1 socket，8 cores / 16 threads，4.00 GHz base，5.00 GHz max |
| CPU governor | `powersave`；测试未绑核，单 NUMA node |
| Cache | L1d 256 KiB，L2 2 MiB，L3 16 MiB |
| Memory | 60 GiB RAM，8 GiB swap |
| Storage | Samsung SSD 980 PRO 2TB NVMe，`ext4`，测试目录位于 `/code` |
| OS | Linux x86-64，Ubuntu kernel `6.17.0-14-generic` |
| Compiler | GCC 15.2.0，C++20，Bazel `--config=release`（`-O2 -DNDEBUG`） |
| Network | 不参与本 Storage benchmark |
| Telemetry | Benchmark 内未启用 exporter |

CPU counters/cache miss 未纳入本轮数据：主机 `perf_event_paranoid=4`，无 `CAP_PERFMON`。该限制明确记录，不使用推测值替代。1 KiB timed run 的进程 CPU 使用率为 49%，最大 RSS 21,792 KiB，无 major page fault；该数值包含顺序执行的 encode/write/recovery/MPSC 子阶段，不等同于生产 Recorder 稳态 CPU 占用。

## 4. 负载模型与统计口径

- 固定 payload：64 B、1 KiB、64 KiB、1 MiB。
- Segment Writer：单 Topic、单 Partition、单 Writer。
- Writer batch：128 records。
- Sync policy：`per_batch`。
- Recorder Buffer：4 producers / 1 consumer，有界 queue。
- Recovery：每个生成的 Segment 完整扫描 5 次，首次扫描 warmup。
- Encode、Flush、`fdatasync` 和 Recovery 报告 p50/p95/p99/p99.9/max；nearest-rank。
- 所有阶段报告 attempts、errors、error rate；正式基线要求错误率为 0。
- Benchmark 使用 `std::chrono::steady_clock`，固定 payload seed。

复现示例：

```bash
bazel run --config=release //benchmarks:storage_benchmark -- \
  --records=20000 \
  --payload-bytes=1024 \
  --sync-policy=per-batch \
  --directory=/code/Mino/.cache \
  --output-json=docs/benchmarks/storage_sla_timed.json
```

## 5. 性能 SLA

以下目标只对第 3、4 节的同等级或更高硬件、相同文件系统语义和负载模型有效。虚拟化、共享盘、网络文件系统、不同 mount/device flush 语义必须重新资格认证。

| Payload | 指标 | SLA 门槛 | 本次实测 | 结果 |
|---:|---|---:|---:|---|
| 64 B | Encode p99 | ≤ 1.0 µs | 0.580 µs | PASS |
| 64 B | Append throughput | ≥ 1.2 M records/s | 1.609 M records/s | PASS |
| 64 B | `fdatasync` p99 | ≤ 10 ms | 6.468 ms | PASS |
| 64 B | Recovery throughput | ≥ 60 MiB/s | 77.68 MiB/s | PASS |
| 1 KiB | Encode p99 | ≤ 10 µs | 3.859 µs | PASS |
| 1 KiB | End-to-end writer throughput | ≥ 15 MiB/s | 20.62 MiB/s | PASS |
| 1 KiB | `fdatasync` p99 | ≤ 10 ms | 6.436 ms | PASS |
| 1 KiB | Recovery throughput | ≥ 150 MiB/s | 191.62 MiB/s | PASS |
| 1 KiB | Buffer MPSC throughput | ≥ 500k records/s | 639k records/s | PASS |
| 64 KiB | End-to-end writer throughput | ≥ 150 MiB/s | 199.38 MiB/s | PASS |
| 64 KiB | `fdatasync` p99 | ≤ 20 ms | 12.90 ms | PASS |
| 64 KiB | Recovery throughput | ≥ 220 MiB/s | 286.64 MiB/s | PASS |
| 1 MiB | Encode p99 | ≤ 5 ms | 3.469 ms | PASS |
| 1 MiB | End-to-end writer throughput | ≥ 180 MiB/s | 231.40 MiB/s | PASS |
| 1 MiB | `fdatasync` p99 | ≤ 75 ms | 55.68 ms | PASS |
| 1 MiB | Recovery throughput | ≥ 220 MiB/s | 289.46 MiB/s | PASS |

所有正式运行的 stage error rate 均为 0。

原始证据：

- `docs/benchmarks/storage_64b.json`
- `docs/benchmarks/storage_sla_timed.json`
- `docs/benchmarks/storage_64k.json`
- `docs/benchmarks/storage_1m.json`

上述首版 JSON 保存负载配置和测量结果，运行日期、硬件、OS 与编译器 provenance 由本报告第 3 节记录；它们尚未内嵌 commit SHA，因此不能单独作为当前发布候选的资格证明。后续重新资格认证必须将 commit SHA、完整命令、Bazel 配置、编译器、OS、硬件/存储和运行时间写入同一 artifact manifest，并记录 JSON 与日志的 SHA-256。

## 6. Buffer Capacity SLA

默认 Buffer 配置必须同时满足：

```text
global_capacity >= peak_ingress_bytes_per_second × tolerated_disk_pause_seconds
per_topic_capacity >= topic_peak_bytes_per_second × tolerated_disk_pause_seconds
```

同时预留 fixed-class 内部碎片和 queue metadata；容量验收按 charged bytes 而不是 payload bytes。持续平均写盘吞吐必须大于持续平均输入吞吐，内存缓冲只承诺吸收短期抖动。

## 7. 故障和恢复目标

- 任意 Record 写入切点 Kill：恢复到最后完整 Commit Marker，尾部可安全截断。
- Manifest/Schema fault point：重启后只观察到旧版本或完整新版本。
- ENOSPC/EIO/EROFS：当前 Topic/Partition 进入 ERROR，并产生明确错误；不自动继续写。
- Recovery 时间按完整扫描数据量线性增长；1 KiB 基线约 191.62 MiB/s。生产部署应持久化 Durable checkpoint，降低正常恢复扫描范围。

扩展 campaign：

```bash
tools/ci/run_storage_fault_campaign.py --rounds=100 --seed=42 \
  --out=storage-fault-campaign
```

## 8. 重新资格认证条件

以下任一变化都必须重新运行 benchmark、fault campaign 和 sanitizer 矩阵：

- Segment/Record/Manifest format version 变化；
- 默认 batch、sync policy、buffer class 或 capacity 变化；
- 文件系统、mount 选项、存储控制器或设备型号变化；
- CPU architecture/NUMA 策略变化；
- Compression、encryption、telemetry exporter 加入热路径；
- Topic 数、Partition 数或 payload 分布超出本报告模型。
