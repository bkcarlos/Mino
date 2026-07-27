# ADR-0004：Canonical Wire Format v1

- 状态：ACCEPTED
- 决策：采用确定性 Tagged Format、显式 Field ID、固定小端、Varint/ZigZag 和有界 Unknown Field Set。
- 约束：生产 Schema 禁止隐式 Field ID；Float 确定性采用位模式相等；Map 不进入 v1。
- 待验证：Golden Vector、Fuzz、N/N-1 和性能对比。

## Context

网络与存储需要一种跨节点、跨版本、可演进且确定性的编码：相同 Schema 与逻辑值必须产生相同字节序列（用于去重、CRC、Golden Test）。同时要求 Decoder 能跳过未知字段以支撑 N/N-1 互通。

## Alternatives Considered

- **Schema-less 自描述格式（如 CBOR/JSON）**：无需预共享 Schema，但字节开销大、确定性差（Map 键序、浮点文本化），否决。
- **固定布局内存镜像直接上网**：最快，但与 CPU 架构/编译器 ABI 耦合，无法跨版本演进，且违反「不传输 SHM Offset」原则，否决。
- **FlatBuffers 风格偏移表**：随机访问友好，但写入需要两遍构建、Unknown Field 保留语义弱，否决。

## Consequences

- 正面：编码规则与 Protobuf 线格式同族，工程经验成熟；Tagged 结构天然支持字段增删与 Unknown Field 保留；确定性满足 Golden Vector 与去重需求。
- 负面：相比固定布局有编解码 CPU 开销；Tag 与 Varint 引入少量字节开销。
- 跟进：详设 13.8 冻结编码细节（tag 打包、Wire Type、最短 Varint、缺省省略规则）后生成 Golden Vector（详设 26 章 V-05）。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-07-27 | Mino 架构评审 Agent | ACCEPTED | Wire 格式定义完整（详设 13.8 已覆盖 Tagged 编码、tag 打包、Wire Type、小端、最短 Varint/LEB128、长度前缀、Field ID 升序、Float 位模式确定性、有界 Unknown Field Set 与资源上限），版本经头部 `mino-canonical-v1` 纳入 Canonicalization，长度/深度/元素数校验明确；Golden Vector（13.8、26 章 V-05）与 Fuzz（23.4 覆盖 IDL/Descriptor/Network Frame/Canonical Payload）方案明确。可支撑 D3 开发；遗留验证项：V-05 在 D3 阶段通过 Golden Vector、性能与 Fuzz 对比关闭。 |
