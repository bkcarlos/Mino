# 优化状态（以 master 代码为准）

- 代码基线：master `77f30da`；下表记录 2026-10-03 在该基线上的修订
- 更新日期：2026-10-03（Asia/Shanghai）
- 方法：只认 `.h/.cc`；不发明新测量数字。完整中文清单见仓库外
  `/workspace/mino-results/OPTIMIZATION.md`（若你本机有该目录）。
- 仍 incomplete 总表（代码缺口 / 硬件资格 / intentional KEEP）：见
  [`docs/benchmarks/kvm-2026-08-25/UNIMPLEMENTED.md`](benchmarks/kvm-2026-08-25/UNIMPLEMENTED.md)
  文首「仍 incomplete 速览」。

## 可靠性与验证修订（2026-10-03）

| 能力 | 修订后的边界 | 回归入口 |
|---|---|---|
| 持久去重 ACK / Hello | 接收批次按 source 合并 HWM，批量落盘成功后才发送 ACK；重连 Hello 同样先持久化；失败保留重试且不重复发布 | `bridge_pipeline_test`：PersistenceFailureRetriesBeforeAckWithoutRepublishing |
| 多 lane 恢复 | 各 lane 只恢复所属 source；同一 DedupStore 实例序列化共享访问 | `bridge_pipeline_test`：SharedPersistentStoreRestoresOnlyMatchingLane；`dedup_store_test` |
| 去重窗口淘汰 | age/capacity 淘汰后在下一帧分类前恢复持久 HWM，防止重复发布或高序号永久 NACK | `bridge_pipeline_test`：DurablePrefixSurvivesWindowAgeAndCapacityEviction |
| 持久化恢复屏障 | 重开快照必须同步目录后才信任 HWM；覆盖 rename 成功但目录同步失败的恢复路径 | `dedup_store_test`：ReopenMustCommitRenameBeforeTrustingVisibleHwm |
| 快照独占所有权 | 独立 `.lock` 文件持有进程级 `flock`；重复打开拒绝；进程死亡后可重开 | `dedup_store_test`：ExclusiveOwnershipSurvivesSnapshotRename、ProcessDeathReleasesLockAndPreservesCommittedSnapshot |
| 去重运行监控 | 每 store 容量占用、持久化次数/失败/耗时、容量拒绝导出；告警看最大单库占用 | `monitoring_test`：DurableDedupCapacityAndFailuresAreExported；需配置 `MonitoringSources::dedup_stores` |
| IPsec 内核 CI | 独立网络命名空间执行，必需模式禁止权限不足时跳过；归档 XML、日志和内核信息 | `.github/workflows/ci.yml`：ipsec-kernel；`tools/ci/run_ipsec_kernel_test.sh` |
| 持久去重成本 | `RecordAcceptedBatch` 一次提交多个 HWM；无退休记录时保留 v1，首次退休升级 v2；基准输出提交次数、吞吐和提交 P99 | `//benchmarks/bridge:dedup_store_benchmark`；不是生产磁盘资格 |
| IPsec 接入 | 强制策略与加密 SA 关联；可选 socket ESP 保护覆盖后台发送；双向 SPI 固定时后台复验，过期返回 WouldBlock；普通 socket 逐次复验 | `//mino/security:ipsec_test`、`//mino/transport:ipsec_transport_test`；有权限的内核测试与无权限解析测试分别计数 |
| ZMQ TSan 超时 | 六个顺序多进程 profile 的 Bazel 总预算改为 medium（300s）；保留子进程期限 | `//mino/runtime:zmq_ipc_business_mp_stress_test`；需 Linux x86-64 TSan 复验 |
| RDMA verbs 参考插件 | MR-only；非零传输 limits 在 Start 明确拒绝；QP/CM/CQ 仍需外部完整插件 | `//mino/platform:rdma_plugin_load_test`；不算 NIC 资格 |

验证应绑定实际源码和环境；取消、跳过、未执行不能计为通过。

第一批本地验证（2026-10-03，Ubuntu 24.04 / Linux AArch64 容器，GCC 13 / Bazel 7.4.1）：

- UBSan：`dedup_store_test`、`bridge_pipeline_test`、`connection_manager_test`、
  `ipsec_test`、`ipsec_transport_test`、`rdma_plugin_load_test`、`rdma_driver_test`、
  `remote_bridge_test` 共 8 个目标通过（其中 2 个未变更目标命中测试缓存）。
- TSan：`dedup_store_test` 和 `zmq_ipc_business_mp_stress_test` 通过；ZMQ 用时 37.7s。
  Linux x86-64 GitHub CI 仍需在发布前按新提交复跑。
- 独立、无外网网络命名空间 + `CAP_NET_ADMIN`：IPsec 全部 16 个用例通过，
  包括真实 SA/策略安装，以及保留 SA、删除策略后的拒绝。普通无权限测试运行中
  跳过的两个内核用例不混计为通过。
- 持久化基准：64 个 source、1024 次更新，batch=1 为 1024 次提交，batch=32
  为 32 次提交，两者重新打开快照验证通过。该结果来自容器虚拟磁盘，
  不能替代生产磁盘 P99/SLA，也不证明物理 NIC、RDMA/Fabric 或 AArch64 生产资格。


第二批修复验证（同为 Ubuntu 24.04 / AArch64 / GCC 13 / Bazel 7.4.1）：

- UBSan 10 个目标通过：去重 store/window、bridge pipeline/connection manager、
  monitoring、remote bridge、IPsec probe/transport、monitoring drill、Prometheus endpoint。
  普通 IPsec 运行仍有两个无权限跳过用例，不能当作内核路径已执行。
- TSan 3 个目标通过：dedup store、bridge pipeline、monitoring。
- release 去重基准：batch=32、1024 更新、64 来源，32 次提交，关闭唯一 owner 后
  重开校验成功。强制内核模式在缺少 CAP_NET_ADMIN 时失败而不是跳过。
- 新增 CI 脚本在独立网络命名空间实际执行通过：16 个内核/策略用例全部通过，
  XML 检查确认零跳过；此为本地容器执行，GitHub hosted job 尚未运行。
- 第二批结束时待办（socket 保护已在第三批推进，见下）：IPsec 查询优化及内核 socket 强制保护；
  旧 publisher epoch 的协议级安全自动退休；QP/CM/CQ RDMA 数据通路。
  去重容量告警及离线维护约束不能替代自动退休协议。修改后的 GitHub x86-64
  矩阵尚未执行，本地测试不是完整发布资格。

## 第三批：socket IPsec 与持久化故障路径

- TCP/UDP 可在创建 socket 后、网络 I/O 前安装强制双向 ESP transport 策略；
  TCP listener 的策略覆盖握手并由 accepted socket 继承。IPv6 限制为 V6-only。
- 固定双向 SPI 的连接允许按 `protected_socket_recheck_ms`（默认 1000ms）复验；
  接入始终验证匹配的加密 SA，普通/未固定 SPI 的 socket 仍逐次验证。到期检查
  在第三批实现中仍是同步查询（第四批改为后台复验，见下）。换新 SPI 必须重连，
  不能复用活跃 SPI 装载空加密 SA。
- 修正 TCP 默认单连接发送缓冲少 4 字节帧前缀导致默认参数无法创建的问题。
- 持久化覆盖四阶段前/后的 8 个进程终止点，以及写入空间不足、文件同步、
  重命名、目录同步错误注入。重开时新增目录同步屏障，失败就不暴露可 ACK 的 HWM。
  新建多级目录逐级同步自身及父目录，不能只同步叶目录。
  进程终止测试不是断电/物理磁盘缓存资格测试。
- UBSan 10 目标通过：dedup store/window、bridge pipeline/connection manager、
  TCP/UDP driver、IPsec probe/transport、monitoring、remote bridge。
- TSan 7 目标通过：dedup store、bridge pipeline、TCP/UDP driver、IPsec probe/transport、
  monitoring。多级目录修订后补跑 UBSan store/pipeline/monitoring 和 TSan store 均通过。
- 新版隔离内核 CI 脚本在 UBSan 和 TSan 构建下各 25 个用例全部通过、零跳过；包含固定 SPI 不可用时
  禁止切换到其他 SA、无 SA 不得明文发送、明文拒收、策略删除后的受保护 UDP、
  TCP listener/accepted 保护和复验缓存行为。普通无权限运行的 9 个跳过用例单独计数。
- 旧 epoch 的安全自动退休仍未实现：需要持久化退休拒收标记、发布端停止重传确认，
  以及跨重连/进程重启保持的协议状态。不能通过定时删除或见到新 epoch 就清理来替代。

## 第四批：IPsec 后台复验

- 固定双向 SPI 的连接由每个 wrapper 的一个后台线程定期复验。Send/Poll 只读有效期；
  过期返回 `kWouldBlock`，失败返回 `kUnavailable`，owned-send 保留调用者 payload。
- 复验结果按连接实例 generation 绑定；关闭后复用 connection ID 不继承旧查询结果。
  慢查询按开始时刻计算有效期，不能在结束时延长已过期的授权；失败/异常后有间隔重试。
- Shutdown 和析构均停止调度并 join 在途查询；支持重新 Start。自定义 probe 必须线程安全、
  有限时返回。Scripted probe 已加互斥；netlink 同时限制 send/recv 和 multipart dump 读取时间。
- 普通 socket 和零 SPI 的 socket 仍逐次同步验证。后台查询拥塞会暂停过期连接，需按连接数及
  实际查询耗时设置间隔；没有新增 XFRM 事件订阅或密钥管理。
- 核对退休协议：`control_payload.h` 的 SessionHello 仅携带 HWM；当前控制 opcode 没有
  publisher 退休确认，第四批时 DedupStore v1 也没有持久化拒收标记（第五批加入离线退休 v2）。

- Ubuntu 24.04 / AArch64 / GCC 13 / Bazel 7.4.1 验证：UBSan 7 目标通过
  （IPsec probe/transport、TCP/UDP、bridge pipeline/connection manager、remote bridge）；
  TSan 的 IPsec probe/transport 2 目标通过，包含 5 个后台复验/线程生命周期用例。
  两种构建分别运行内核隔离脚本，均为 26 用例通过、零跳过；普通无权限运行的
  probe 套件为 17 通过、9 跳过，不能将跳过视为内核验证通过。
- Scripted probe 撤销时清空既有 peer 允许列表，补测允许后撤销和并发读写，避免测试继续误放行。
- 改后 GitHub x86-64 CI 仍未执行；RDMA QP/CM/CQ 数据通路仍是独立功能缺口。

## 第五批：持久化退休边界与受控停机维护

- 新增 `DedupStore::RetireEpochsThrough`：显式退休同一 node/publisher 的 epoch 范围，
  原子替换其 HWM 为单条持久拒收边界。新 epoch 必须大于边界；边界本身不可自动过期。
- 接收端在 schema 缓存、发布和 ACK 之前拒绝退休范围；跨重启/重连/lane 保留拒收结果。
  退休边界不进入 SessionHello 的 accepted HWM 列表，不能伪造 Accepted ACK。
- store 跟踪所有 pipeline 的挂接生命周期；尚有 pipeline 时拒绝退休及 ReplaceAll。
  创建失败会释放挂接记录。退休落盘失败要求重开，避免不确定的 rename 结果被后续写入覆盖。
- 无退休记录仍写 v1，首次退休写 v2；旧代码拒绝 v2，不能丢弃退休边界来降级。
  v2 校验 CRC、排序、边界唯一性和 HWM/边界冲突。ReplaceAll 保留所有已有边界。
- 活跃 HWM 与退休边界共用 max_sources 容量；新增 `mino_dedup_retired_publishers` 指标。
  这解决同一 publisher 多 epoch 的占用，不能无限回收不断更换 publisher ID 的历史。
- 维护前仍须停止对应 epoch 的发布并排空可靠重传；在线自动退休的发送端确认和持久状态尚未实现。
  操作流程及格式兼容性见 `docs/operations/monitoring.md`。
- 验证环境为 Ubuntu 24.04 / AArch64 / GCC 13 / Bazel 7.4.1。UBSan 6 目标、98 用例通过
  （dedup store/window、bridge pipeline/connection manager、monitoring、remote bridge）；
  TSan 4 目标、77 用例通过（dedup store、bridge pipeline/connection manager、monitoring），
  两者零失败、零跳过。包含退休写入 8 个进程中断点及 4 类 I/O 故障；不代表物理断电资格。
  GitHub x86-64 CI 尚未执行。

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
| A12 IPsec 传输软件子集 | **DONE（软件路径）** | `IpsecTransportDriver` + `NetlinkXfrmSaProbe`；内核 XFRM；**不算** IKE/硬件资格 |
| A8 subordinate writable Attach | **DONE（layout v7）** | `attachment_directory.*` + `request_subordinate_writable`；Dead-only reclaim；见 ADR-0014 |

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
