# Mino Architecture Decision Records

状态：`PROPOSED`、`ACCEPTED`、`VALIDATED`、`FROZEN`、`SUPERSEDED`。

ADR 通过评审后从 PROPOSED 变为 ACCEPTED；通过原型/目标硬件验证后变为 VALIDATED；进入兼容性承诺后变为 FROZEN。已冻结 ADR 的不兼容变更必须创建新 ADR 和协议版本。

## 格式约定

每篇 ADR 至少包含：状态头部（状态/决策/约束/待验证）、**Context**（面临的问题与约束）、**Alternatives Considered**（被否决方案及理由）、**Consequences**（正负影响与跟进项）。

## 流转规则

- **PROPOSED → ACCEPTED**：需架构评审通过。评审记录（评审人、日期、结论）追加到 ADR 文末「评审记录」节。
- **ACCEPTED → VALIDATED**：需引用验证产物（测试报告、Benchmark 文档或 TLA+ 模型路径），产物在《详细设计文档》第 26 章登记表中跟踪。
- **VALIDATED → FROZEN**：在对应协议版本发布冻结时由发布负责人批量提升，并在 ADR 文末记录冻结版本号。
- **→ SUPERSEDED**：被新 ADR 替代时保留原文，文首标注替代者编号；历史引用不修改。
