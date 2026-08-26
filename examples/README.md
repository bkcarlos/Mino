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
