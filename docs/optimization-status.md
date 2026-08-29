# 优化状态（以 master 代码为准）

- HEAD 对照：`a1b76c38c7731f1eb6f62a4a800df95526fbdabf`
  （`perf: close opt-complete SHM hop and codec residual copies`）
- 更新日期：2026-08-27（Asia/Shanghai）
- 方法：只认 `.h/.cc`；不发明新测量数字。完整中文清单见仓库外
  `/workspace/mino-results/OPTIMIZATION.md`（若你本机有该目录）。

## 已关闭（opt-complete / a1b76c3）

| 项 | 状态 | 代码入口 |
|---|---|---|
| 同机 SHM hop 拷 payload（A1） | **DONE** | `benchmarks/pipeline_comparison/mino_shm_pipeline.cc`：`RunForwarder` 用独占 hop；sink/CANBus 用 payload **span** 校验，不 `SemanticFrame.payload.assign` |
| 独占转发 API（B1） | **DONE** | `BorrowedMessage::TakeExclusive() &&` → `ExclusiveMessage<T>`；`Publisher::PublishLocal(ExclusiveMessage&&)`（`mino/runtime/subscriber.h`、`publisher.h`） |
| `BytesView`（A2） | **DONE** | `DynamicValue::BytesView` / `Kind::kBytesView`；`EncodeInto` 接受 view（`mino/schema/dynamic_value.*`、`wire.*`） |
| 长度定界 payload `insert` memmove（A3） | **DONE** | `EncodeLengthDelimitedValue`：Leb128 前缀 + `Append`；嵌套走 scratch 再 Append |
| 流式 DecodeView / owned send / 尾帧 steal（A4） | **DONE** | `LengthPrefixedFrameDecoder::Push`→`DecodeView`；Bridge `TrySendOwned` / `TrySendUntrackedOwned`；TcpDriver 收缓冲**尾部**完整帧 `move` steal |

### Exclusive hop 契约（勿写错）

- **仅 SPSC**；pin table / Broadcast / MPSC → `kUnsupported`
- 未 `PublishLocal` 的 `ExclusiveMessage` 析构 reclaim
- `TakeExclusive` 与 `PublishLocal` 之间进程被杀：journal 已不跟踪、槽位已 ACK → **泄漏到 Region 重建**
- 与 `Transfer()`（Pin→`ShmSharedPtr`）不同：Transfer **不能**再发布

### SimpleNode（更早合入 master）

`mino/runtime/simple_node.h`：`Create` / `Open` / `Advertise` / `Subscribe` / `Publish` / `TryPoll`（及 `Poll`）。
示例：`examples/simple_mp_pubsub*`。无 journal/lease 恢复；崩溃后重建段。

**不存在** `Bus::CreatePublisher<T>`（根 README 预览过期）。

## 仍残留的拷贝 / 成本

1. 源端首发：`PopulateGeneratedFrame` 仍 `AllocateChild` + `memcpy`
2. Hybrid 桥 graph↔semantic↔wire（跨机零拷贝未做）
3. 控制面 `WireFrameCodec::Decode` 仍 `payload.assign`
4. `TcpDriver::Send()` 非 owned 仍整帧 `PrefixFrame`；中段收帧仍 `assign`
5. **三把 mutex 未改**（`mutex_` / `send_ingress_mutex_` / `receive_mutex_`）
6. `RetransmitWindow` 为可靠重传故意自持 owned 拷贝
7. Bus / LocalBusDeployment canonical memcpy；RDMA `pending.payload.assign`

## 历史测量

同机 medium「Fast DDS 3571 vs Mino 2925」等数字来自 hop 改造**前**战役；**不要**写成 a1b76c3 后已反超。复测前只当方向性参考。

## 相关文档

- `examples/README.md` — SimpleNode / ZMQ 对照
- `benchmarks/pipeline_comparison/PERFORMANCE_FOLLOWUP.md` — 战役与 backlog（已与本状态对齐关键 P1）
- `docs/benchmarks/README.md` — 基准索引
