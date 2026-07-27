# ADR-0002：ShmHandle v1 布局

- 状态：ACCEPTED
- 决策：Handle 使用 64-bit Offset、32-bit Generation、32-bit Region ID，总计 16 字节。
- 约束：Region ID 持久分配且永不复用；Generation 回绕前 Drain/迁移；Attach 校验 Region UUID/Epoch。
- 待验证：动态字段空间开销、Cache 影响和回绕故障测试。

## Context

跨进程共享内存映射基址不同，所有共享引用必须采用相对偏移。仅用 `uint32_t offset` 无法检测 Slot 回收复用后的陈旧引用（ABA）；需要同时控制 Handle 尺寸，因为它存在于每个 IndexSlot 和动态字段 Meta 中，直接影响控制面带宽。

## Alternatives Considered

- **64 位 Offset + 64 位 Generation（24 B）**：Generation 空间更大，但 Handle 变大 50%，且 32 位 Generation 配合「回绕前 Drain」策略已足够，否决。
- **分层 Handle（Region Table 间接寻址）**：解引用多一次间接，热路径开销不可接受，否决。
- **32 位 Offset（8 B Handle）**：单 Region 4 GiB 上限对大对象池过于局促，否决。

## Consequences

- 正面：16 字节对齐友好，两字段即可检测陈旧引用；Region ID 永不复用简化跨 Region 混淆防护。
- 负面：Generation 回绕前必须 Drain/迁移，需要额外的运维状态（DRAINING）；128 位 Handle 使 IndexSlot 增至两个 Cache Line（见 ADR-0003）。
- 跟进：原型验证空间开销与回绕故障路径后进入 VALIDATED（详设 26 章 V-01）。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-07-27 | Mino 架构评审 Agent | ACCEPTED | 布局与语义定义完整，符合 D1 开发入口要求；遗留验证项：V-01（Handle 布局）/ V-02（Slot 尺寸）在 D1 阶段通过原型验证关闭。 |
