# 简单跨进程 SHM Pub/Sub

主题目录、slab 和 SPSC 通道都在**同一块 POSIX shm** 里，没有独立的发现服务或 SHM 管理进程。

段大小按 `topic 数 × 队列深度 × 最大 payload` 计算，256B / 深度 32 的演示大约几百 KiB。
实验室里把 `/dev/shm` 扩到 8GiB 只是大帧压测旋钮，嵌入式和默认 64MiB tmpfs **不要**按那个去开段。

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
slow-joiner window. Payloads are multipart (`camera` + body). HWM=`queue-depth`
(32); PUB drops when mute rather than blocking like SimpleNode Publish.

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

