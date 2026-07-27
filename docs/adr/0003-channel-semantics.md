# ADR-0003：Channel 与 Slot Metadata

- 状态：PROPOSED
- 决策：SPSC、MPSC、Broadcast、Work Queue 使用独立语义；Base Slot 不保存 mask_or_refcount。
- 约束：Broadcast、Work Queue、MPSC Reservation 使用同索引 Sidecar Metadata；MPSC Owner 失效后提交 ABORTED Tombstone；Broadcast ACK 绑定 Subscriber Generation。
- 待验证：TLA+、Kill、回绕和 Cache Benchmark。

## Context

四种 Channel 的并发语义本质不同：SPSC 无竞争；MPSC 需要预留仲裁与崩溃恢复；Broadcast 要求每个订阅者独立进度；Work Queue 要求每条消息恰好被一个消费者认领。若用一个「通用 Slot」承载全部语义，单字段（如 mask_or_refcount）会在不同模式下被赋予不同解释，评审与排障都无法收敛。

## Alternatives Considered

- **统一 Slot + 模式字段复用（union）**：省内存，但同一字节在不同 ChannelKind 下含义不同，ABI 演进时极易错位，否决。
- **每种 Channel 完全独立的 Ring 实现**：语义最清晰，但公共逻辑（Sequence、回绕、恢复扫描）重复四份，维护成本高，否决。
- **引用计数内嵌 Base Slot**：回收最及时，但原子热点严重且进程崩溃即泄漏，与 Lease 模型冲突，否决（留作后续 Epoch/RCU 评估）。

## Consequences

- 正面：Base Slot 保持不可变消息元数据 + 状态，语义稳定；Sidecar 与 Slot 同索引，恢复扫描可统一遍历；每种语义的内存序可独立验证。
- 负面：IndexSlot 达 128 字节（两个 Cache Line），Ring 容量相同时内存翻倍；Sidecar 数组增加常驻内存。
- 跟进：MPSC Producer Kill 的 TLA+ 模型与 Cache Benchmark（详设 26 章 V-02/V-03/V-04/V-14）。
