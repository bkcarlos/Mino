# 优化状态（以 master 代码为准）

- HEAD 对照：`feature/shared-host-domain-slab` tip（本提交；基于 master **`9d1c26e`**）
- 更新日期：2026-09-06（Asia/Shanghai）
- 方法：只认 `.h/.cc`；不发明新测量数字。完整中文清单见仓库外
  `/workspace/mino-results/OPTIMIZATION.md`（若你本机有该目录）。
- 仍 incomplete 总表（代码缺口 / 硬件资格 / intentional KEEP）：见
  [`docs/benchmarks/kvm-2026-08-25/UNIMPLEMENTED.md`](benchmarks/kvm-2026-08-25/UNIMPLEMENTED.md)
  文首「仍 incomplete 速览」。

## 已关闭（opt-complete / a1b76c3 + opt-closeout + tip）

| 项 | 状态 | 代码入口 |
|---|---|---|
| 同机 SHM hop 拷 payload（A1） | **DONE** | `benchmarks/pipeline_comparison/mino_shm_pipeline.cc`：`RunForwarder` 用独占 hop；sink/CANBus 用 payload **span** 校验，不 `SemanticFrame.payload.assign` |
| 独占转发 API（B1） | **DONE** | `BorrowedMessage::TakeExclusive() &&` → `ExclusiveMessage<T>`；`Publisher::PublishLocal(ExclusiveMessage&&)`（`mino/runtime/subscriber.h`、`publisher.h`） |
| Exclusive hop crash recovery | **DONE** | journal `AdoptExclusiveHop` + `kExclusiveHop`；recovery Rollback/Finalize；journal-backed `Subscriber`（`allocation_journal.*`、`journal_channel_recovery.cc`、`subscriber.h`） |
| Exclusive hop ACK→Adopt 微窗口 | **DONE（opt-closeout）** | `TakeExclusive` **先** `AdoptExclusiveHop` **再** SPSC ACK；源发布仍 Visible → Finalize（不回收）；否则 Rollback。见下契约 |
| `BytesView`（A2） | **DONE** | `DynamicValue::BytesView` / `Kind::kBytesView`；`EncodeInto` 接受 view（`mino/schema/dynamic_value.*`、`wire.*`） |
| 长度定界 payload `insert` memmove（A3） | **DONE** | `EncodeLengthDelimitedValue`：Leb128 前缀 + `Append`；嵌套走 scratch 再 Append |
| 流式 DecodeView / owned send / 尾帧 steal（A4） | **DONE** | `LengthPrefixedFrameDecoder::Push`→`DecodeView`；Bridge `TrySendOwned` / `TrySendUntrackedOwned`；TcpDriver 收缓冲**尾部**完整帧 `move` steal |
| RDMA owned Send 免 `assign` | **DONE（opt-closeout）** | `RdmaDriver::DoTrySendOwned` / `PostOwned`：`vector&&` 移入 pending；可选 `pre_registered` MR 跳过 Register/Deregister。span `Send` 仍需 staging 拷 |
| Wire 帧 AEAD | **DONE（帧层+会话 KEX）** | `wire_aead.*` + `wire_aead_session.*`；TLS exporter / PSK KeyShare；`BridgePipeline` 自动挂 keyring |
| 嵌套 owned-graph 遍历 | **DONE** | 生成 `CollectOwnedGraph` / `AppendOwnedChildren`（深度上限 32） |
| Hybrid 跨机图所有权转发（P8） | **DONE** | `graph_ownership_forward.*`；中间 SemanticFrame.assign 已消；真跨机 SHM 零拷贝仍需 RDMA/Fabric MR |
| Region ID Attach | **DONE** | `region_name_registry.*`：空 name + region_id 经持久 registry 解析 |
| 持久 Dedup Store | **DONE** | `dedup_store.*` + BridgePipeline 种子化 / HWM 持久化 |
| SharedHostDomain v3 | **DONE** | MPSC/Broadcast、borrow、typed、`Recover()`；**CentralSlab + AllocationJournal + ShmPinTable**（ABI `MINOSHD3`，与 v1/v2 不兼容） |
| P9 PTP + RDMA/Fabric 参考插件 | **DONE（软件路径）** | `PtpClockClient` + `PtpSyncSidecar` + pipeline 门控；RDMA/Fabric 参考插件；**不算** V-25 / 双机 PTP 硬件资格 |

### Exclusive hop 契约（勿写错）

- **仅**完整 typed `Publisher<T>` / `Subscriber<T>` 路径上的 SPSC；`TakeExclusive` → `PublishLocal(ExclusiveMessage&&)`
- pin table / Broadcast / MPSC → `kUnsupported`
- 未 `PublishLocal` 的 `ExclusiveMessage` 析构 / `Release()` reclaim（有 hop lease 时走 journal `RollbackCommitted`）
- **Crash-safe（journal-backed Subscriber）**：`TakeExclusive` 在 SPSC ACK **之前**写入 durable `PublicationChannelKind::kExclusiveHop` lease；`PublishLocal(ExclusiveMessage&&)` `FinalizeCommit` 该 lease；死进程由 `JournalChannelRecoveryCoordinator::RecoverOrphans` 处理——源 SPSC 仍 **Visible**（未 ACK）→ **Finalize** 仅释 lease；否则 **Rollback** 回收 graph，**不必**只靠 Region recreate
- **无 journal 的 Subscriber**：仍仅 RAII reclaim；kill 窗口 fail-closed 泄漏到 Region 重建
- **微窗口**：ACK→Adopt 指令级窗口已关闭；无 journal 路径与长持有后的 Region 级泄漏语义不变
- 与 `Transfer()`（Pin→`ShmSharedPtr`）不同：Transfer **不能**再发布
- **不在** `SimpleNode` 上：SimpleNode 走 `Advertise` / `Subscribe` / `Publish` / `Poll`，无 `TakeExclusive`

### SimpleNode（tip / `d36603e`）

`mino/runtime/simple_node.h`：同一 POSIX shm 内含 discovery、allocator journal、endpoint 所有权、channels、subscriber leases 与 payload Pins；无独立协调进程。

- 模式：`SimpleTopicMode::{kSpsc,kMpsc,kBroadcast}`（默认 SPSC）；`SimpleTopicOptions` 还含 `queue_full_policy`、`sample_rate`
- API：`Create` / `Open` / `Unlink`；字节与 typed `Advertise` / `Advertise<T>`、`Subscribe` / `Subscribe<T>`；`Publish(bytes)` / `Publish(T)`；`TryPoll` / `Poll` 与 `BorrowedBytes::As<T>` / `BorrowedValue<T>`
- 恢复：`SimpleNode::Recover()` 显式回收已证明死亡的 endpoint、MPSC reservation、Broadcast lease/borrow、Pin 与 allocation journal；公开 publish/poll 也会自动做保守恢复
- 兼容：manifest **v3**，与旧 SimpleNode segment **不兼容**（升级需重建共享段）
- 示例：`examples/simple_mp_pubsub*`、`examples/README.md`

**不存在** `Bus::CreatePublisher<T>`。`Bus` 只有非模板 `CreatePublisher(topic, SchemaIdentity)` → `BusPublisher`；类型化零拷贝请用 `Publisher<T>` / `Subscriber<T>` 或 `SimpleNode`。

### Hybrid 跨机所有权转发（P8 / A5）

- API：`mino/bridge/graph_ownership_forward.h`（`EncodeFromGraph` /
  `EncodeMessage` / `ReconstructToPrepared` / `TryRegisterPayload`）
- 桥：`benchmarks/pipeline_comparison/mino_shm_tcp_bridge.cc` 优化路径不再
  `SemanticFrame.payload.assign`
- 诚实拷贝计数：SHM→wire 1；wire→SHM 1；WireFrame/TCP（或未来 RDMA）传输仍在；
  无注册内存时**不存在**跨主机真零拷贝
- 资格：单元测试已覆盖完整性；双机 hybrid 性能战役仍待跑

### P9 PTP + RDMA/Fabric 参考插件

- PTP：`PtpClockClient` + `PtpSyncSidecar`（`mino.ptp_sync_quality.v1`）；无合格同步不报跨机单向延迟；物理双机 PTP qual 仍缺
- RDMA：`libmino_rdma_software_loopback.so` + `libmino_rdma_verbs.so`（绝对路径 `dlopen`）
- Fabric：`libmino_fabric_software_loopback.so`
- **不**声称 V-25 硬件资格；软件 provenance 含 `NOT-QUALIFICATION-ELIGIBLE`

## 仍残留的拷贝 / 成本（仅 physics KEEP / intentional KEEP）

下列为**有意保留**的物理/设计成本，不是未完工 stub。诚实标注：

| # | 项 | 状态 | 说明 |
|---|---|---|---|
| 1 | 源端首发 `PopulateGeneratedFrame` | **KEEP** | `AllocateChild` + `memcpy` 仍在；语义/网络源不在本 Region，首发进 SHM **必须**物化。benchmark 已注明。无法 Publish/BytesView 跳过：外部字节不是本 Region 图。 |
| 2 | Hybrid 桥 graph↔semantic↔wire | **DONE（P8）** | `graph_ownership_forward` + bridge：无 SemanticFrame.payload.assign；encode/reconstruct 各 1 次 payload 物化；TCP/WireFrame 拷贝仍不可避免；RDMA 注册钩子预留。 |
| 3 | 控制面 `WireFrameCodec::Decode` | **DONE（拥有路径）** | Bridge inbound 控制+数据统一 `DecodeView`；`Decode(vector&&)` + `IntoWireFrame` 就地 compact，无二次 payload 堆拷。`Decode(span)` 仍 `assign`（调用方不拥有 body 时必要）。 |
| 4 | `TcpDriver::Send` / 收帧 | **DONE（可控路径）** | `Send`/`SendUntracked` 改为锁外 body 拷 + segmented `PendingWrite`（不再 `PrefixFrame` 整帧）；收包 **头帧/尾帧** steal，仅「非零 offset 且仍有 trailing」的中段仍 `assign`。 |
| 5 | 三把 mutex | **KEEP（CLOSED）** | 锁布局保留（worker / ingress / ready-receive）。body 拷已在锁外；ingress CS 仅 admission+move。全量 lock-free/分片队列有丢唤醒与 tear-down 正确性风险，**有意停在此设计**（见 `tcp_driver.cc` 注释）。 |
| 6 | `RetransmitWindow` owned 拷贝 | **KEEP** | 可靠重传故意自持；`Add`/`ResendPending` 不能挪走唯一副本。 |
| 7 | Bus memcpy；RDMA staging | **KEEP / 部分改进** | **Bus/LocalBusDeployment**：Broadcast 槽位私有区 `memcpy` + `CanonicalMessage` 拥有向量是 API/布局物理必要（非 CentralSlab 图）；**KEEP**。**RDMA**：owned `TrySendOwned` 已 `move` 免 `assign`；span `Send` 仍 staging；真 NIC 零拷贝需调用方持 MR 并走 `pre_registered`（钩子已留，通用 Send 零拷贝仍 KEEP）。 |
| Extra | SharedHostDomain CentralSlab | **DONE（v3）** | 固定 per-topic ring 已替换为 CentralSlab 路径（journal/pins，复用 SimpleNode 模式）；ABI `MINOSHD3`。残留：Publish 仍需一次用户字节→slab `memcpy`（源不在本 Region 时物理必要）；Broadcast 多订户仍走 channel 语义。见 `shared_host_domain.h`。 |

## 历史测量

同机 medium「Fast DDS 3571 vs Mino 2925」等数字来自 hop 改造**前**战役；**不要**写成 a1b76c3 / 当前 tip 后已反超。复测前只当方向性参考。

## 相关文档

- `docs/benchmarks/kvm-2026-08-25/UNIMPLEMENTED.md` — A1–A12 / 资格门 / KEEP 仍 incomplete 清单（对照 tip）
- `examples/README.md` — SimpleNode / ZMQ 对照
- `benchmarks/pipeline_comparison/PERFORMANCE_FOLLOWUP.md` — 战役与 backlog（已与本状态对齐关键 P1）
- `docs/benchmarks/README.md` — 基准索引
- `docs/rdma-driver.md` / `docs/fabric-driver.md` — P9 参考插件与 V-25 资格边界
