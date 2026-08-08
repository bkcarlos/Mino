# Validation Benchmark 方法与报告模板

- 状态：**PENDING（尚未在资格硬件上实跑）**
- Benchmark target：`//benchmarks:validation_benchmark`
- Artifact schema：`mino.validation_benchmark.v1`
- JSON Schema：`docs/benchmarks/validation_benchmark.schema.json`
- 空结果模板：`docs/benchmarks/validation_benchmark_pending.json`

本文只定义可复现方法和结果字段，不提供推测性能数字。仓库中的模板以 `PENDING` 和 `null` 明确表示未实跑。

## 1. 覆盖矩阵

| 验证项 | 实现/API | 场景 | 主要输出 |
|---|---|---|---|
| V-14 Broadcast ACK bitmap | `BroadcastChannel::Init/RegisterSubscriber/Reserve/Commit/Poll/Borrow::Ack` | 1、2、8、16、最大 64 subscribers | 固定布局内存、publish/commit、单 subscriber poll+ACK、完整 fan-out roundtrip latency |
| V-15 static/dynamic schema view | 生成的 `golden::TelemetryAccessor`；`schema::DynamicBuilder/DynamicObject/DynamicView` | 同一 `golden.Telemetry.sequence` 字段 | static accessor 与 `DynamicView::GetUnsigned(FieldHandle)` latency、错误数 |
| V-16 1–100 Topic writer 模型 | `RecorderBufferPool`、`PartitionManifest`、`TopicWriter` | 1、10、50、100 个 Topic/Partition single writer | setup/start、enqueue+pump、stop/seal 时间与 records/s |
| V-17 sync 策略 | `SegmentWriter` 和真实 `fdatasync`（macOS 为 `fsync`） | `none`、`interval`、`per-batch`、`per-record` | append latency、sync latency/calls/errors、Seal 前后 `durable_records` |
| V-18 buffer/磁盘暂停模型 | `RecorderBufferPool::Reserve/Cancel`；显式容量公式 | 64 B 至 1 MiB+1 的真实 charged bytes；10/100/1000 MiB/s × 10/100/1000 ms | 实际 class charge；公式推导的最低 payload buffer bytes |
| V-27 Pin/lease cleanup | registry `Coordinator::AcquireTopicPin/ReleaseTopicPin/SweepExpiredNodes` | 参数化 Pin 数；lease 到期且 liveness 明确为 dead | acquire/release latency、cleanup elapsed、`pins_removed` 守恒 |

## 2. 构建和运行

资格结果必须使用 Release 配置，并显式注入 commit 和硬件信息：

```bash
bazel build --config=release //benchmarks:validation_benchmark

MINO_BENCHMARK_COMMIT="$(git rev-parse HEAD)" \
MINO_BENCHMARK_BUILD_CONFIG="bazel --config=release" \
MINO_BENCHMARK_CPU_MODEL="<CPU model>" \
MINO_BENCHMARK_MEMORY="<DIMMs/capacity/speed>" \
MINO_BENCHMARK_STORAGE_DEVICE="<device/controller>" \
MINO_BENCHMARK_FILESYSTEM="<filesystem and mount options>" \
bazel run --config=release //benchmarks:validation_benchmark -- \
  --suite=all \
  --iterations=10000 \
  --storage-records=1000 \
  --records-per-writer=100 \
  --payload-bytes=64 \
  --pin-count=1000 \
  --directory=/tmp \
  --output-json=/tmp/mino-validation-benchmark.json
```

同名 CLI 参数优先于环境变量，例如 `--commit` 覆盖 `MINO_BENCHMARK_COMMIT`。未注入的 provenance 字段写为 `PENDING`，不会猜测。程序自动记录 UTC 运行时间、完整 argv、编译器 `__VERSION__`、`__cplusplus`、`uname`、logical CPU count 和可获取的 physical memory bytes。

可以分开运行，避免磁盘阶段干扰内存阶段：

```bash
bazel run --config=release //benchmarks:validation_benchmark -- \
  --suite=memory --commit=<sha> --build-config='bazel --config=release'

bazel run --config=release //benchmarks:validation_benchmark -- \
  --suite=storage --commit=<sha> --build-config='bazel --config=release' \
  --directory=<qualified filesystem path>
```

被 `--suite` 排除的验证项在输出中保留，并标记为 `PENDING`，而不是写入零值冒充结果。

### 2.1 实现与快速契约测试结构

`benchmarks/validation_benchmark.cc` 仅负责执行顺序、输出文件和失败退出的薄入口。实现位于 `benchmarks/validation/`：

- `common/`：CLI/config、provenance、nearest-rank 统计、JSON 基础函数、运行状态、payload 和受控临时目录；
- `report/`：顶层 artifact JSON 与失败 artifact 组装；
- `validations/`：V-14、V-15、V-16、V-17、V-18、V-27 各自独立的 `.cc/.h`，验证项私有 helper 只存在于对应 `.cc`；
- `tests/contract_smoke_test.py`：使用 Python 标准库 `json`（不依赖 `jq`）分别运行小规模 memory/storage，并验证六个 V key、`MEASURED`/`PENDING` 与失败 artifact 的 `FAILED` 语义。

快速回归命令：

```bash
bazel test //benchmarks:validation_contract_smoke_test
```

## 3. 统计口径

- 所有延迟使用 `std::chrono::steady_clock`。
- latency distribution 使用 nearest-rank，输出 p50/p95/p99/max，单位 ns。
- V-14 与 V-15 warmup 为 measured iterations 的 10%，至少 1 次；warmup 不进入样本。
- benchmark 内部 sink 消费读取值，防止编译器删除热点操作。
- 每个运行时 API 失败会使 artifact 标记为 `FAILED` 且 target 非零退出；局部错误计数仍写入已完成的场景字段。
- 正式报告必须保留原始 JSON、stdout/stderr 日志和 artifact SHA-256；建议至少运行 5 个独立进程并报告跨进程分布。

## 4. 各验证项解释与限制

### V-14：ACK bitmap 内存与延迟

`BroadcastChannel` 当前 ABI 为固定最大 64 subscribers：subscriber slots 和每个 channel slot 的 exact-era ACK sidecar 在 `RequiredSize(capacity)` 中一次性分配。因此 1/2/8/16/64 active subscribers 的 `required_bytes` 相同，输出中的 `incremental_bytes_for_active_count` 为 0；这不是测量误差，而是固定 ABI 布局事实。报告同时记录 `SubscriberSlot`、`BroadcastEraMeta` 和 legacy `BroadcastSlotMeta` 的 `sizeof`，便于 ABI 变化时重新资格认证。

每轮先真实 `Reserve/Commit`，再由所有 active subscriber `Poll/Ack`。`fanout_roundtrip_latency_ns` 随 subscriber 数增加，不能与单次 `poll_ack_latency_ns` 混用。

### V-15：static/dynamic view

Static 路径使用项目已有 codegen golden 生成物 `TelemetryAccessor::sequence()`；dynamic 路径用同一份 `golden.Telemetry` IDL 编译 descriptor，通过真实 allocator/journal/pin 构造 `DynamicObject`，再调用 `DynamicView::GetUnsigned(FieldHandle)`。对象构建、schema compile、layout plan 和 Pin 建立均在热点测量外；结果只比较已建立 view 的字段读取成本。

### V-16：Topic writer 数量模型

每个 Topic 对应独立 `RecorderBufferPool`、`PartitionManifest`、`TopicWriter` 和 partition directory，直接覆盖 production single-logical-owner 写路径。场景为 1、10、50、100 writers。该项使用 `sync_policy=none` 隔离 writer 数量模型，但 `Stop()`/Seal 仍执行最终持久化；其时间单列为 `stop_seal_elapsed_ns`。不能把 `enqueue_pump_records_per_second` 当作 durable throughput。

### V-17：sync 策略与持久性

四种 `SegmentSyncPolicy` 都走真实 `SegmentWriter`。窄 hook 只包裹真实 `fdatasync`（macOS 无该接口时为 `fsync`）以采样 system call latency，不替代持久化调用。`durable_records_before_seal` 是 writer 自身的持久性边界观测；`durable_records_after_seal` 验证最终 Seal。最终介质保证仍依赖文件系统、mount、控制器和设备固件。

### V-18：容量与磁盘暂停

此项有两类输出，必须分开解读：

1. `capacity_charge_samples` 是真实 `RecorderBufferPool` reservation 返回的 charged capacity；
2. `disk_pause_capacity_model` 是确定性公式 `required_bytes = ingress_bytes_per_second × pause_seconds` 的推导值，不是性能实测。

模型明确不含 queue metadata、安全余量，以及已由 charged bytes 单独表现的 fixed-class 内部碎片。部署容量必须再加入这些项，并保证长期平均写盘吞吐大于长期平均输入吞吐。

### V-27：Pin 与 lease cleanup

steady-state 路径对 exact `TopicPinRegistration` 做 acquire/release。cleanup 路径先持有参数化数量的 Recorder Pin，再让 owner lease 到期，并由注入的 `LivenessProbe` 明确返回 `kDead`；随后计时 `SweepExpiredNodes`，且强制验证 `pins_removed == acquired pins`。`kUnknown` liveness 不属于本 benchmark 的授权清理条件。

## 5. `storage_64b.json` 命名审计

已读取 `docs/benchmarks/storage_64b.json`：

- `configuration.payload_bytes` 为 **64**；
- 文件名 `storage_64b.json` 的 `64b` 与实际 payload **一致**；
- 因此无需修正内容；按要求未修改现有 storage JSON。

审计结论也写入 `docs/benchmarks/validation_benchmark_pending.json` 的 `audits`，与性能结果的 `PENDING` 状态分离。

## 6. 正式结果填写规则

1. 不手工把模板中的 `PENDING` 改成数字；由 benchmark 直接生成 JSON。
2. `artifact_status=MEASURED` 只表示程序完成测量且运行时 API 未报告错误，不自动代表 SLA PASS；`FAILED` artifact 必须保留用于诊断但不得作为资格证据。
3. 任一 errors/sync_errors 非零、V-27 Pin 守恒失败或 provenance 仍为 `PENDING`，均不得作为发布资格证据。
4. V-18 公式值必须继续标为 modeled，不可改称 measured。
5. 使用 `docs/benchmarks/validation_benchmark.schema.json` 校验 artifact 后，再在独立评审报告中定义门槛与 PASS/FAIL；本次补齐不改开发计划，也不凭空设 SLA。
