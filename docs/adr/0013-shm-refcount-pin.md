# ADR-0013：SHM 对象级引用 Pin（Lease 绑定引用计数）

- 状态：PROPOSED
- 决策：在 Channel 级回收（Cursor + ACK Bitmap）之外，增加对象级引用 Pin 机制（`ShmSharedPtr` / Counted Borrow）：对单个 Payload 的跨 Callback、跨线程、跨 Channel 长持有引用。Reclaim 的充分条件修订为「已 Retired ∧ 无有效 Borrow ∧ 无存活 Pin」。
- 约束：计数器不内嵌 Base Slot（遵守 ADR-0003），位于 Slab Header/Sidecar；Pin 的崩溃安全不依赖析构（跨进程无 RAII 保证），而由持有者 Lease 保证——进程死亡由 Lease 失效检测，Recovery 清除其 Pin 份额；Pin 获取必须校验 Handle Generation，防止 ABA 复活；Pin 总数有界（per-object 与 per-process 双上限），超出拒绝而非累积。
- 待验证：Pin 获取/释放热路径开销、Lease 失效到 Pin 清除的时延、Pin 耗尽对 Channel 回收的阻塞模型、Recovery 清除协议竞态测试（详设 26 章 V-27）。

## Context

首版回收体系是 Channel 级：Broadcast 由「最慢有效 Subscriber Cursor + ACK Bitmap」决定 Retire，Reclaim 要求无有效 Borrow（详设 8.4、10.1、11.3）。这覆盖了高速发布/消费热路径。但存在 Channel 级模型无法表达的需求：

- Subscriber 需要将某条消息**跨 Callback / 跨线程长期持有**（详设 11.2 预留的 Transfer 口子）；
- Replay、Inspector、调试器需要**钉住（Pin）**某个历史对象，不受 Channel Cursor 推进影响；
- 跨 Channel 转发同一 Payload（零拷贝 relay）时，对象生命周期跨越多个 Channel 的回收域。

这些场景的共同点是：引用对象是**单个 Payload**、持有期**远超一次 Poll**、持有者**可能异常退出**。朴素 shared_ptr 式引用计数在跨进程 SHM 下有两个已识别缺陷（ADR-0003、架构 10.2）：热路径原子热点、进程崩溃即泄漏。

## Alternatives Considered

- **热路径全量引用计数（内嵌 Base Slot）**：每条消息的每次 Borrow 都原子增减——原子热点严重，且进程被 Kill 时计数永不归零，Payload 永久泄漏；ADR-0003 已否决，本 ADR 不翻案。
- **不做对象级引用，长持有一律拷贝**：语义最简单，但违背零拷贝目标；Inspector/Replay 抓取大对象时拷贝代价不可接受，否决。
- **Epoch/RCU 延迟回收**：读路径轻量，适合读多写少；但回收时机不可预期，Inspector 类「必须钉住指定对象」的语义无法表达；留作订阅者规模扩大后的独立评估（架构 10.3 已记录），与 Pin 机制不冲突。
- **Lease 绑定引用 Pin（本方案）**：计数只用于显式 Transfer/Pin 场景（非每次 Borrow），热路径不受影响；崩溃安全由既有 Lease/Recovery 体系兜底而非析构；回收充分条件增加一个 AND 项，与 Channel 模型正交叠加。

## Consequences

- 正面：Transfer/Replay/Inspector/跨 Channel relay 获得零拷贝长持有能力；崩溃安全复用 Lease 体系，无新故障模式；ADR-0003 的热路径决策保持不动。
- 负面：Reclaim 判定多一个条件，Channel 回收线程需扫描 Pin 表；Pin 泄漏（业务忘释放 + Lease 尚存）会阻塞对应 Payload 回收，需要指标与配额兜底。
- 跟进：详设 11.2 Transfer 语义、8.4 回收条件修订、Pin 表容量纳入 20.4 节点资源预算；验证登记 V-27。
