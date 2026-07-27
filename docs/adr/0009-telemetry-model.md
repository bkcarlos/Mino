# ADR-0009：传输性能 Telemetry

- 状态：PROPOSED
- 决策：按传输阶段统计 Counter/Gauge/Histogram，提供 Off、Counters-only、Sampled、Full Debug 四级。
- 约束：采样消息才携带 Trace Context；事件写入有界 Sidecar；Exporter 不阻塞数据路径；跨节点单向延迟依赖 Clock Quality。
- 待验证：准确性、Clock Jump、Exporter 故障和 Off/On 性能开销。

## Context

性能排障需要分阶段延迟归因（发布/编码/发送/接收/解码/提交），但全量逐消息事件在零拷贝热路径上不可接受。跨节点单向延迟还依赖时钟同步质量，盲目上报会导出误导性数据。

## Alternatives Considered

- **全量事件 + 异步聚合**：信息最全，但热路径每消息多次原子写与时间戳调用，开销不可控，否决。
- **仅 Counter/Gauge，无逐消息 Trace**：开销最低，但无法做尾延迟归因（p99 以上样本无阶段拆分），否决。
- **eBPF/外部采样 Profiler**：零侵入，但无法获得协议内部阶段语义（如 Bridge Borrow vs Encode），仅作补充手段。

## Consequences

- 正面：开销可分级购买（Counters-only ≤1% 目标）；采样消息携带 Trace Context 使低速率下仍可还原端到端链路；Clock Quality 门槛防止误导性跨节点延迟进入统计。
- 负面：四级模式与采样传播增加配置与协议面（flags 位 + PerfTraceContext）；Sidecar 溢出时需要丢弃策略与指标。
- 跟进：Off/Counter/Sampled 开销对比与 Clock Jump 测试（详设 26 章 V-23）。
