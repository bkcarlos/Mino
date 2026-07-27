# ADR-0006：Delivery 与 ACK 语义

- 状态：PROPOSED
- 决策：区分 Local Published、Remote Accepted、Recorder Buffered、Storage Written、Storage Durable。
- 约束：默认 Publish 成功只表示 Local Published；多目标显式选择 ALL/ANY/QUORUM；Receipt 返回逐目标状态；Deadline 不撤销消息。
- 非目标：首版不承诺跨节点 Exactly-once。

## Context

「消息已送达」在本地提交、远端接管、录制缓冲、落盘、持久化五个阶段的强度差异巨大（崩溃/掉电存活矩阵见详设 2.2）。若用单一 ACK 表述，应用层会高估保证；若默认最强保证，实时 Topic 延迟不可接受。

## Alternatives Considered

- **单一 ACK + QoS 配置（如 MQTT 级别）**：简单，但级别语义模糊，无法表达「远端已接收 + 本机已 Durable」这类组合，否决。
- **默认最强保证（同步等待 Durable）**：语义最简单，但把所有 Topic 拖到磁盘延迟量级，违背实时性目标，否决。
- **两阶段提交式跨节点事务**：提供 Exactly-once，但协调开销与可用性损失大，首版明确不做（见非目标）。

## Consequences

- 正面：应用按需购买保证强度；默认路径零额外延迟；逐目标 Receipt 使部分失败可诊断。
- 负面：API 复杂度上移（DeliveryStage/CompletionPolicy/Deadline 三要素）；At-least-once 使上层必须容忍重复或配置去重。
- 跟进：多目标 Receipt 故障注入与端到端测试（详设 26 章 V-09）。
