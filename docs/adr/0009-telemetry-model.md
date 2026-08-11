# ADR-0009：传输性能 Telemetry

- 状态：ACCEPTED
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

## V-23 开销口径

≤1%/≤2% 指标定义为 **相同真实 publish operation 上的增量数据路径开销**：

```text
overhead(mode) = (publish_ns(mode) / publish_ns(compile_off) - 1) × 100%
```

`publish` 必须执行同一生产数据面 operation，至少包含生产 `SpscChannel` 的 `Reserve`、固定 `IndexSlot` 字段填充、`Commit`、`Poll`、业务字段读取与 `Ack`。Compile-off baseline 只移除 Telemetry 调用；不得增加虚假工作、搬入 Telemetry 工作或缩减其他 mode 的 operation。动态 policy epoch 可在固定 256-operation 边界同步，切换生效延迟必须有界且 epoch 检查必须留在计时区；批内每条消息仍绑定同一个完整不可变 policy snapshot。Counters-only 必须计入消息数、Payload 字节与 Wire 字节更新以及计时区内的周期 flush；Sampled 必须计入稳定决策、采样消息的时钟、Trace Context、Histogram 与有界 Sidecar 入队。按照本 ADR 的异步 Exporter 决策，Sidecar 消费/导出不属于数据路径计时，但生产者入队属于。

纳秒级空循环会把不可再分的原子读/分支放大为数百个百分点，因此仅作为单独的 micro-op 物理下限诊断，不作为 ≤1%/≤2% 验收分母。V-23 必须防 DCE，报告绝对 ns/op 与配对 overhead，执行预热、时钟/noop 校准、多个独立进程和多轮 AB/BA 配对，并给出跨进程均值的 95% 置信区间。验收以置信上界不超过目标为通过，任何 Counter 不守恒、采样率异常、非预期 drop 或 worker 失败均使结果无效。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-07-27 | Mino 架构评审 Agent | ACCEPTED | Telemetry 模型与详设 21.5 一致：Off/Counters-only/Sampled/Full Debug 四级对应 PerfTelemetryMode，开销预算 ≤1%/≤2% 明确为原型验收目标；时钟规则清晰（Wall Clock + Clock Quality，uncertainty 超限停发单向 E2E，跳变/负延迟样本计数丢弃，INV-30）；Counter/Histogram 固定容量、热路径无全局锁，采样消息才携带 PerfTraceContext，Sidecar 有界且溢出丢弃不影响业务（INV-29）；可支撑 D6 开发。遗留验证项：V-23（Off/Counter/Sampled 开销对比、Clock Quality 与 Histogram 校验）在对应阶段关闭。 |
