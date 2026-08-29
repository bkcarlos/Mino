# 简单跨进程 SHM Pub/Sub

主题目录、slab、恢复 journal、Pin 表和通道都在**同一块 POSIX shm** 里，没有独立的发现服务或 SHM 管理进程。

段大小按 topic、队列深度、最大 payload 及恢复容量计算；默认配置包含固定的崩溃安全 Pin 表，仍小于 8 MiB。实验室里把 `/dev/shm` 扩到 8GiB 只是大帧压测旋钮，嵌入式和默认 64MiB tmpfs **不要**按那个去开段。

## Topic 模式、QoS 与类型

默认 `Advertise("camera")` 保持 SPSC（一个 publisher、一个 subscriber）。也可在首次发布声明 topic 模式和队列满策略；配置写入 manifest，后续同名 publisher 必须完全匹配：

```cpp
mino::SimpleTopicOptions options;
options.mode = mino::SimpleTopicMode::kMpsc;  // 多 publisher、单 subscriber
options.queue_full_policy = mino::QueueFullPolicy::kDropNewest;
auto publisher = node.Advertise("jobs", options);
```

- `kSpsc`：单 publisher、单 subscriber。
- `kMpsc`：多 publisher、单 subscriber；`queue_depth` 至少为 64。
- `kBroadcast`：单 publisher、最多 64 个 subscriber，每个订阅者独立 ACK。
- QoS 支持 `kBlock`、`kFail`、`kDropNewest`、`kDropOldest` 和 `kSample`；阻塞及被采样接纳后的等待都遵守 publish deadline。
- `Advertise<T>`、`Subscribe<T>`、`Publish(T)` 和 `Poll<T>` 使用 `StaticMessageTraits<T>` 校验固定布局 schema，并保持 payload 零拷贝借用。带 owned child slabs 的生成类型应使用完整 `Publisher<T>` API。

`SimpleNode::Recover()` 会立即清理已证明死亡的 endpoint、Broadcast lease/borrow/Pin 及孤儿 allocation；普通发布/轮询也会自动执行保守恢复。manifest v3 与旧 SimpleNode segment 不兼容，升级后需重新创建共享段。

## 编译

```bash
bazel build --config=release //examples:simple_mp_pubsub
```

## 运行（两个终端）

```bash
# 终端 1：订阅（可先启动，会等待发布端 Create）
bazel-bin/examples/simple_mp_pubsub sub /mino_demo

# 终端 2：发布
bazel-bin/examples/simple_mp_pubsub pub /mino_demo
```

## 压测（独立进程）

`simple_mp_pubsub_stress` 用 Create/Open 在两个独立进程间传带序号的 payload，
不 fork、不假设 8GiB `/dev/shm`。Create 在段超过当前 `/dev/shm` 或 RLIMIT 时失败。

```bash
bazel build --config=release //examples:simple_mp_pubsub_stress

# 终端 1
bazel-bin/examples/simple_mp_pubsub_stress sub /mino_stress \
  --messages 20000 --payload-bytes 256 --queue-depth 32

# 终端 2
bazel-bin/examples/simple_mp_pubsub_stress pub /mino_stress \
  --messages 20000 --payload-bytes 256 --queue-depth 32
```

订阅端结束时在 stdout 打一行 JSON（`received` / `lost` / `p50_ns` / `p95_ns` / `msgs_per_s`）。
64KiB payload 用 `--messages 2000`，队列深度仍须能放进默认 64MiB tmpfs。

## ZMQ ipc:// comparison (independent processes)

Same spawn model as the SimpleNode stress: two binaries, no fork-after-bind.
The subscriber binds `ZMQ_SUB` on `ipc://` (Unix domain socket under `TEST_TMPDIR`
or `/tmp`) and subscribes to topic `camera`. After bind+subscribe it writes a
ready file; the publisher waits for that file, connects `ZMQ_PUB`, and waits for
`ZMQ_EVENT_HANDSHAKE_SUCCEEDED` plus a peer file so it does not send into the
slow-joiner window. Payloads are multipart (`camera` + body). `--queue-depth`
stays a compatibility knob (still 32 in the SimpleNode-matched profiles).
`--hwm N` sets SNDHWM/RCVHWM independently (default = queue-depth; any integer
1..1000000). PUB mute-drops when HWM is hit rather than blocking like
SimpleNode Publish; raising HWM is how a 1-hop sweep measures that tradeoff.

Raw payloads match SimpleNode (`origin_ns` + `seq` + pattern). Business payloads
fill `SemanticFrame` and encode with protobuf on ZMQ / CanonicalWireCodec on
SimpleNode so serialize sits inside p50.

```bash
bazel test --config=release \
  //mino/runtime:simple_node_mp_stress_test \
  //mino/runtime:zmq_ipc_mp_stress_test \
  //mino/runtime:simple_node_business_mp_stress_test \
  //mino/runtime:zmq_ipc_business_mp_stress_test
```

## 产品 API 要点（master / a1b76c3）

- 头文件：`mino/runtime/simple_node.h`（Create / Open / Advertise / Subscribe / Publish / TryPoll）。
- 同机类型化流水线独占 hop：`BorrowedMessage::TakeExclusive` → `ExclusiveMessage` →
  `Publisher::PublishLocal(ExclusiveMessage&&)`（**仅 SPSC**；见 `docs/optimization-status.md`）。
- 编码：`DynamicValue::BytesView` + `EncodeInto`，避免默认 owning `Bytes` 整段堆拷。
- **没有** `Bus::CreatePublisher<T>`；不要抄根 README 过期预览。

优化关闭状态：`docs/optimization-status.md`。
