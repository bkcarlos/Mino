# ADR-0005：Schema Identity

- 状态：ACCEPTED
- 决策：64-bit Short ID 仅作热路径索引，256-bit Canonical Digest 是最终身份。
- 约束：Digest 覆盖传递类型依赖闭包；碰撞时必须比较完整 Digest；网络和存储使用映射到完整 Digest 的紧凑 schema_ref。
- 待验证：Canonicalization Golden Vector 和碰撞路径。

## Context

Schema 身份需要同时满足：热路径快速比较（每帧/每条消息）、全局唯一且抗碰撞（跨节点、跨会话、长期存储）、内容可演进（版本维度独立于内容身份）。单一字段无法同时满足三项。

## Alternatives Considered

- **仅用 128/256 位 Digest**：身份最强，但每帧 16~32 B 开销大，热路径比较成本高，否决（降级为连接/会话级紧凑引用）。
- **仅用 64 位 Short ID**：热路径最优，但生日碰撞概率在大量 Schema 下不可忽略，且无法作为长期存储身份，否决。
- **UUID 由 Registry 顺序分配**：依赖 Registry 全局可用，离线部署与跨组织合并时无法保证唯一，否决。

## Consequences

- 正面：身份强度与热路径效率解耦；Digest 内容寻址使相同 Schema 天然去重；碰撞处理路径明确（升级为完整 Digest 比较）。
- 负面：需要维护 Short ID ↔ Digest、conn/rec schema_ref ↔ Digest 两级映射；Canonicalization 规则必须冻结（详设 13.3.1），否则两个实现算出不同 Digest。
- 跟进：Canonicalization v1 Golden Vector 与碰撞注入测试（详设 26 章 V-08）。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-07-27 | Mino 架构评审 Agent | ACCEPTED | Schema Identity 定义完整：Canonicalization v1（详设 13.3.1）规范了头部前缀、字段排序、Annotation 规范化、类型名/依赖闭包参与 Digest 及版本提升规则；256-bit Canonical Digest 为最终身份、64-bit Short ID 仅作热路径索引（13.3），碰撞时升级完整 Digest 比较（INV-20）；网络/存储经 connection_schema_ref 两级映射到完整 Digest（13.9）。可支撑 D3 开发；遗留验证项：V-08 在 D3 阶段通过 Golden Vector、完整 Digest 与碰撞注入测试关闭。 |
