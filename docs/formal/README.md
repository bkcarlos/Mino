# D2-14 TLA+ 形式化模型

本目录包含 D2-14 的三个有限状态模型。模型使用固定的小规模集合，目的是由 TLC 穷举协议交错，而不是模拟容量或性能。

| 模块 | 覆盖范围 | 核心检查 |
|---|---|---|
| `MpscReservation.tla` | `reserve → write → commit`、owner death/restart、recovery abort、READY/ABORTED consume | 严格有序消费不跨洞；READY 必须完成写入；死 owner 的队首 reservation 可恢复，并在公平恢复假设下最终推进 |
| `BroadcastMembership.tla` | 注册、注销、Subscriber ID/Generation 复用、publish snapshot、ACK、generation-scoped cleanup、retire | Snapshot/ACK 责任冻结；旧 generation 的 ACK/cleanup 不清除新 generation 责任 |
| `LeaseEviction.tla` | heartbeat、expiry observation、进程存活复查、EVICTING、ACK/Pin cleanup、EVICTED、lease reuse | expiry 不等于死亡；live owner 不进入剔除态；cleanup 同时绑定 generation、lease epoch 与 owner incarnation |

## TLC 运行与证据

三个模型的 TLC 配置已固化为同名 `.cfg` 文件。仓库根目录下使用统一脚本顺序运行全部模型：

```sh
python3 tools/ci/run_tla_validation.py \
  --jar /path/to/tla2tools.jar \
  --out /tmp/mino-formal-validation
```

`--out` 必须指向不存在或为空的目录。可用 `--timeout-seconds N` 设置每个模型的超时；某个模型失败或超时后，脚本仍会继续运行后续模型，全部结束后以非零状态退出。`--self-test` 不需要 JAR，用于检查日志解析和哈希逻辑：

```sh
python3 tools/ci/run_tla_validation.py --self-test
```

输出目录包含：

- 每个模型独立的完整 `stdout.log` 与 `stderr.log`；
- `manifest.json`，记录 JAR SHA-256、每个 `.tla`/`.cfg` 的 SHA-256、实际命令、退出码、TLC 版本、成功标志、耗时，以及可解析时的生成状态数、不同状态数、队列剩余状态数和搜索深度；
- 顶层 `overall_success` 与 `complete` 标志。manifest 在每个模型结束后原子更新，因此普通模型失败不会丢失已生成的证据。

`.github/workflows/formal-validation.yml` 在 push、pull request、每周定时任务和手工触发时执行同一脚本。所有触发类型都运行 MPSC、Broadcast、Lease 三个模型；PR 每模型上限 300 秒、总计上限 900 秒，避免修改 `MpscReservation` 或 `LeaseEviction` 时只检查无关模型。除模型和 runner 自身外，`mino/runtime/**` 与 `mino/shm/channel/**` 的实现改动也会触发 workflow。CI 固定使用官方 TLA+ `v1.7.4`，下载的 JAR 必须通过仓库中固定的 SHA-256 校验后才能执行，并将上述证据保留 90 天。

workflow 对 evidence **fail closed**：runner 必须匹配 `expected_commit` 且确认 clean worktree，只有此时 `qualification_eligible` 才为 true；无论 TLC 步骤成功或失败，都会检查 `manifest.json` 存在、`complete: true`、`overall_success: true`、退出码为零、三个 `selected_models` 完整，并逐项验证每个模型的 `stdout.log`、`stderr.log` 存在且 SHA-256 与 manifest 相符。缺少 manifest、模型记录、日志或 hash 不匹配都会令 job 失败；artifact 上传的 `if-no-files-found` 同样设为 `error`。本地可用 `--allow-dirty` 生成开发证据，但该 manifest 明确记录 `qualification_eligible: false`。

TLC 的 `INVARIANTS` 是逐状态安全检查；`PROPERTIES` 是 temporal/liveness 检查。后两类进展性质依赖各模块 `Spec` 中显式列出的 weak fairness。模型使用有界 sequence/generation/time，并通过 `[Next]_vars` 允许终止后的 stuttering；`-deadlock` 仅关闭 TLC 对“必须存在非 stuttering `Next` 后继”的额外检查，不会关闭 invariant 或 temporal property 检查。若只想快速检查状态不变量，可复制对应仓库 `.cfg` 到仓库外并移除 `PROPERTIES` 段；CI 始终运行完整配置。

## INV 映射

### INV-17：MPSC 失效 Reservation 最终可推进

对应 `MpscReservation.tla`：

- `DeadHeadIsRecoverable`：安全侧保证严格有序队首若由失效 owner 占据，则 recovery-abort 当前可执行，且 consumer 未越过该洞；
- `DeadReservationIsNeverConsumedAsData`：失效 reservation 不会被当作 READY 数据消费；
- `DeadReservationEventuallyAborted`：在 recovery weak fairness 下转为 `ABORTED`/`RETIRED`；
- `QueueNotPermanentlyBlockedByDeadOwner`：死 owner 位于队首时，consumer 最终越过其 Tombstone。

### INV-18：Broadcast ACK 责任绑定 Subscriber Generation

对应 `BroadcastMembership.tla`：

- `snapshot` 和 `ackDue` 使用 `(subscriber_id, generation)` token，而不是仅使用可复用 ID；
- `AckAccounting` 保证责任只能由同一 snapshot token 的 ACK 或 cleanup 完成；
- `GenerationScopedCleanup` 记录 cleanup 的精确 generation；
- `OldGenerationDoesNotClearNewResponsibility` 直接检查旧 generation cleanup 不会清除新 generation 的未完成责任。

### INV-32：失效进程的 Pin 份额最终清除

对应 `LeaseEviction.tla`：

- `GenerationLeaseEpochBinding`、`RecheckBindsCurrentLease` 与 `EvictionBindsCurrentLease` 保证 liveness 证据、剔除 CAS 和 cleanup 绑定同一个 `(subscriber_id, generation, lease_epoch, owner)`；
- `LiveOwnerIsNeverEvicted` 保证 expiry 后仍存活的 owner 只会经 `LivenessRecheckAlive` 回到 `ACTIVE`，不会误剔除；
- `CleanupIsLeaseScoped` 与 `OldLeaseCleanupDoesNotAffectCurrentLease` 保证旧 lease 的 ACK/Pin cleanup 不影响复用后的新 lease；
- `EvictedHasNoResponsibilities` 保证进入 `EVICTED` 前该 lease 的 ACK 与 Pin 份额均已清空；
- `ExpiredDeadLeaseEventuallyEvicted`、`EvictingEventuallyEvicted` 在 cleanup weak fairness 下检查失效 lease 最终完成清理，不永久阻塞 reclaim。

`INV-32` 中“Pin 计数不内嵌 Base Slot”属于实现布局约束，不是该行为状态机中的变量；本模型验证的是同一不变量的崩溃恢复与最终清除部分。
