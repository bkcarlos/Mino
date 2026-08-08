# ADR-0008：Recorder 背压

- 状态：ACCEPTED
- 决策：Topic 显式选择强一致录制、隔离录制或尽力录制；Recorder Buffer 必须有界。
- 约束：完整录制时 Buffer Full 只能 Block 或 Fail；默认实时监控 Topic 使用隔离录制；Buffered 不等于 Durable。
- 待验证：Buffer 默认容量公式与磁盘抖动模型（详设 26 章 V-18）。

## Context

磁盘可能短期抖动或长期降速，而实时 Channel 不能被拖垮。「是否允许录制背压传播到 Publisher」本质上是 Topic 级业务决策，无法由 Recorder 单方面决定。

## Alternatives Considered

- **统一强一致（所有 Topic 背压传播）**：语义最强，但一次磁盘抖动即冻结全部实时通道，否决。
- **统一隔离（永不传播）**：实时性最好，但无法满足「消息与录制共同成败」的合规场景，否决。
- **无界缓冲 +  swapping**：背压延迟到 OOM 才暴露，恢复代价更大，否决（缓冲区必须有界）。

## Consequences

- 正面：每种拓扑的丢弃/阻塞点显式可知；隔离模式保护实时通道；强一致模式在 Recorder 崩溃时阻塞而非静默缺口（详设 17.2）。
- 负面：三个拓扑 × 四个落盘模式的组合需要合法性矩阵约束（详设 17.2），配置复杂度上升；隔离模式的 Fanout 组件引入额外一跳缓冲。
- 跟进：V-18 通过磁盘暂停模型、charged bytes 容量公式和 Recorder Buffer 实测关闭。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-07-27 | Mino 架构评审 Agent | ACCEPTED | 三种录制拓扑和有界 Buffer 约束明确；完整录制禁止静默丢失，V-18 在 D5 通过容量模型与磁盘抖动验证关闭。 |
