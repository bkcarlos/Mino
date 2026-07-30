# D2-14 TLA+ 形式化模型

本目录包含 D2-14 的三个有限状态模型。模型使用固定的小规模集合，目的是由 TLC 穷举协议交错，而不是模拟容量或性能。

| 模块 | 覆盖范围 | 核心检查 |
|---|---|---|
| `MpscReservation.tla` | `reserve → write → commit`、owner death/restart、recovery abort、READY/ABORTED consume | 严格有序消费不跨洞；READY 必须完成写入；死 owner 的队首 reservation 可恢复，并在公平恢复假设下最终推进 |
| `BroadcastMembership.tla` | 注册、注销、Subscriber ID/Generation 复用、publish snapshot、ACK、generation-scoped cleanup、retire | Snapshot/ACK 责任冻结；旧 generation 的 ACK/cleanup 不清除新 generation 责任 |
| `LeaseEviction.tla` | heartbeat、expiry observation、进程存活复查、EVICTING、ACK/Pin cleanup、EVICTED、lease reuse | expiry 不等于死亡；live owner 不进入剔除态；cleanup 同时绑定 generation、lease epoch 与 owner incarnation |

## TLC 运行

前提：安装 Java，并取得官方 TLA+ tools 的 `tla2tools.jar`。下面命令从本目录运行；请把 `/path/to/tla2tools.jar` 替换为本机实际路径。配置写入 `/tmp`，不会在仓库中新增 `.cfg` 文件。

### MPSC Reservation

```sh
cat >/tmp/MpscReservation.cfg <<'EOF'
SPECIFICATION Spec
INVARIANTS
    TypeOK
    TailIsContiguous
    ConsumedPrefixClosed
    ConsumerNeverPassesTail
    ReservationOwnerBinding
    ReadyImpliesCompleteWrite
    DeadReservationIsNeverConsumedAsData
    DeadHeadIsRecoverable
PROPERTIES
    DeadReservationEventuallyAborted
    QueueNotPermanentlyBlockedByDeadOwner
EOF
java -cp /path/to/tla2tools.jar tlc2.TLC -deadlock -config /tmp/MpscReservation.cfg MpscReservation.tla
```

### Broadcast Membership

```sh
cat >/tmp/BroadcastMembership.cfg <<'EOF'
SPECIFICATION Spec
INVARIANTS
    TypeOK
    MembershipStateOK
    PublishedStateWellFormed
    SnapshotHasOneGenerationPerId
    AckAccounting
    GenerationScopedCleanup
    RetiredOnlyAfterAllResponsibilitiesClear
    OldGenerationDoesNotClearNewResponsibility
PROPERTIES
    PendingCleanupEventuallyCompletes
    PublishedMessagesEventuallyRetire
EOF
java -cp /path/to/tla2tools.jar tlc2.TLC -deadlock -config /tmp/BroadcastMembership.cfg BroadcastMembership.tla
```

### Lease Eviction

```sh
cat >/tmp/LeaseEviction.cfg <<'EOF'
SPECIFICATION Spec
INVARIANTS
    TypeOK
    CurrentLeaseWasIssued
    GenerationLeaseEpochBinding
    RecheckBindsCurrentLease
    EvictionBindsCurrentLease
    LiveOwnerIsNeverEvicted
    CleanupIsLeaseScoped
    OldLeaseCleanupDoesNotAffectCurrentLease
    EvictedHasNoResponsibilities
PROPERTIES
    ExpiredDeadLeaseEventuallyEvicted
    EvictingEventuallyEvicted
EOF
java -cp /path/to/tla2tools.jar tlc2.TLC -deadlock -config /tmp/LeaseEviction.cfg LeaseEviction.tla
```

TLC 的 `INVARIANTS` 是逐状态安全检查；`PROPERTIES` 是 temporal/liveness 检查。后两类进展性质依赖各模块 `Spec` 中显式列出的 weak fairness。模型使用有界 sequence/generation/time，并通过 `[Next]_vars` 允许终止后的 stuttering；`-deadlock` 仅关闭 TLC 对“必须存在非 stuttering `Next` 后继”的额外检查，不会关闭 invariant 或 temporal property 检查。若只想快速检查状态不变量，可从临时配置中移除 `PROPERTIES` 段。

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
