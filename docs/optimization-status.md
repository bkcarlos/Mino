# 优化状态（以 master 代码为准）

- HEAD 对照：`feature/opt-residual-copies`（基于 `505d0bf` exclusive-hop-recovery）
- 更新日期：2026-09-06（Asia/Shanghai）
- 方法：只认 `.h/.cc`；不发明新测量数字。完整中文清单见仓库外
  `/workspace/mino-results/OPTIMIZATION.md`（若你本机有该目录）。

## 已关闭（opt-complete / a1b76c3）

| 项 | 状态 | 代码入口 |
|---|---|---|
| 同机 SHM hop 拷 payload（A1） | **DONE** | `benchmarks/pipeline_comparison/mino_shm_pipeline.cc`：`RunForwarder` 用独占 hop；sink/CANBus 用 payload **span** 校验，不 `SemanticFrame.payload.assign` |
| 独占转发 API（B1） | **DONE** | `BorrowedMessage::TakeExclusive() &&` → `ExclusiveMessage<T>`；`Publisher::PublishLocal(ExclusiveMessage&&)`（`mino/runtime/subscriber.h`、`publisher.h`） |
| Exclusive hop crash recovery | **DONE** | journal `AdoptExclusiveHop` + `kExclusiveHop`；recovery Rollback；journal-backed `Subscriber`（`allocation_journal.*`、`journal_channel_recovery.cc`、`subscriber.h`） |
| `BytesView`（A2） | **DONE** | `DynamicValue::BytesView` / `Kind::kBytesView`；`EncodeInto` 接受 view（`mino/schema/dynamic_value.*`、`wire.*`） |
| 长度定界 payload `insert` memmove（A3） | **DONE** | `EncodeLengthDelimitedValue`：Leb128 前缀 + `Append`；嵌套走 scratch 再 Append |
| 流式 DecodeView / owned send / 尾帧 steal（A4） | **DONE** | `LengthPrefixedFrameDecoder::Push`→`DecodeView`；Bridge `TrySendOwned` / `TrySendUntrackedOwned`；TcpDriver 收缓冲**尾部**完整帧 `move` steal |

### Exclusive hop 契约（勿写错）

- **仅**完整 typed `Publisher<T>` / `Subscriber<T>` 路径上的 SPSC；`TakeExclusive` → `PublishLocal(ExclusiveMessage&&)`
- pin table / Broadcast / MPSC → `kUnsupported`
- 未 `PublishLocal` 的 `ExclusiveMessage` 析构 / `Release()` reclaim（有 hop lease 时走 journal `RollbackCommitted`）
- **Crash-safe（journal-backed Subscriber）**：`TakeExclusive` 在 SPSC ACK 之后写入 durable `PublicationChannelKind::kExclusiveHop` lease；`PublishLocal(ExclusiveMessage&&)` `FinalizeCommit` 该 lease；死进程由 `JournalChannelRecoveryCoordinator::RecoverOrphans` **Rollback** 回收 graph，**不必**只靠 Region recreate
- **无 journal 的 Subscriber**：仍仅 RAII reclaim；kill 窗口 fail-closed 泄漏到 Region 重建
- **残留微窗口**：ACK 成功到 `AdoptExclusiveHop` 完成之间被杀仍可能泄漏（指令级）；长持有 `ExclusiveMessage` 窗口已覆盖
- 与 `Transfer()`（Pin→`ShmSharedPtr`）不同：Transfer **不能**再发布
- **不在** `SimpleNode` 上：SimpleNode 走 `Advertise` / `Subscribe` / `Publish` / `Poll`，无 `TakeExclusive`

### SimpleNode（tip / e333069+）

`mino/runtime/simple_node.h`：同一 POSIX shm 内含 discovery、allocator journal、endpoint 所有权、channels、subscriber leases 与 payload Pins；无独立协调进程。

- 模式：`SimpleTopicMode::{kSpsc,kMpsc,kBroadcast}`（默认 SPSC）；`SimpleTopicOptions` 还含 `queue_full_policy`、`sample_rate`
- API：`Create` / `Open` / `Unlink`；字节与 typed `Advertise` / `Advertise<T>`、`Subscribe` / `Subscribe<T>`；`Publish(bytes)` / `Publish(T)`；`TryPoll` / `Poll` 与 `BorrowedBytes::As<T>` / `BorrowedValue<T>`
- 恢复：`SimpleNode::Recover()` 显式回收已证明死亡的 endpoint、MPSC reservation、Broadcast lease/borrow、Pin 与 allocation journal；公开 publish/poll 也会自动做保守恢复
- 兼容：manifest **v3**，与旧 SimpleNode segment **不兼容**（升级需重建共享段）
- 示例：`examples/simple_mp_pubsub*`、`examples/README.md`

**不存在** `Bus::CreatePublisher<T>`。`Bus` 只有非模板 `CreatePublisher(topic, SchemaIdentity)` → `BusPublisher`；类型化零拷贝请用 `Publisher<T>` / `Subscriber<T>` 或 `SimpleNode`。

## 仍残留的拷贝 / 成本

| # | 项 | 状态 | 说明 |
|---|---|---|---|
| 1 | 源端首发 `PopulateGeneratedFrame` | **KEEP** | `AllocateChild` + `memcpy` 仍在；语义/网络源不在本 Region，首发进 SHM **必须**物化。benchmark 已注明。 |
| 2 | Hybrid 桥 graph↔semantic↔wire | **DEFER P8** | 跨机零拷贝产品路径，本轮不做。 |
| 3 | 控制面 `WireFrameCodec::Decode` | **DONE（拥有路径）** | Bridge inbound 控制+数据统一 `DecodeView`；`Decode(vector&&)` + `IntoWireFrame` 就地 compact，无二次 payload 堆拷。`Decode(span)` 仍 `assign`（调用方不拥有 body 时必要）。 |
| 4 | `TcpDriver::Send` / 收帧 | **DONE（可控路径）** | `Send`/`SendUntracked` 改为锁外 body 拷 + segmented `PendingWrite`（不再 `PrefixFrame` 整帧）；收包 **头帧/尾帧** steal，仅「非零 offset 且仍有 trailing」的中段仍 `assign`。 |
| 5 | 三把 mutex | **PARTIAL** | 锁布局保留（worker / ingress / ready-receive 分离，全量 lock-free 风险高）。`Send*` body 拷已移出 `send_ingress_mutex_`；注释标明职责。 |
| 6 | `RetransmitWindow` owned 拷贝 | **KEEP** | 可靠重传故意自持；`Add`/`ResendPending` 不能挪走唯一副本。 |
| 7 | Bus memcpy；RDMA `pending.payload.assign` | **KEEP** | Bus 缝 / RDMA 插件路径低 ROI；RDMA device plugins 本轮 out of scope。 |

## 历史测量

同机 medium「Fast DDS 3571 vs Mino 2925」等数字来自 hop 改造**前**战役；**不要**写成 a1b76c3 / 当前 tip 后已反超。复测前只当方向性参考。

## 相关文档

- `examples/README.md` — SimpleNode / ZMQ 对照
- `benchmarks/pipeline_comparison/PERFORMANCE_FOLLOWUP.md` — 战役与 backlog（已与本状态对齐关键 P1）
- `docs/benchmarks/README.md` — 基准索引
