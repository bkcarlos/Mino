# ADR-0011：IDL Field Identity

- 状态：PROPOSED
- 决策：生产 Schema 强制显式 Field ID；已发布 ID 永不复用，删除后 Reserved；首版禁止直接和间接递归类型。
- 约束：Canonical Digest 覆盖传递类型依赖闭包，Import 文件路径不参与 Digest；Unknown Field 有界保留或 Wire Passthrough。
- 待验证：Parser、Compatibility、Golden Vector 和 N/N-1 测试。

## Context

Field ID 是 Wire Format 的唯一字段身份。源码顺序自动分配在字段插入/删除时会发生隐式重编号，造成跨版本数据错位；递归类型使有界布局规划（Layout Plan 预计算容量）不可判定。

## Alternatives Considered

- **自动分配 + 兼容性检查工具**：开发便利，但检查在 CI 之外可被绕过，共享内存 ABI 一旦错位没有运行时可挽回手段，否决。
- **允许递归 + 深度限制运行时装**：表达力强，但分配事务、对象图回收和 Canonical Digest 闭包全部复杂化，首版否决，后续可独立 ADR 引入。
- **Field Name 作为身份**：重命名即破坏兼容，与「名称可演进、ID 稳定」原则冲突，否决。

## Consequences

- 正面：Wire 身份与源码排版解耦；Reserved 机制使误删字段可被编译期拒绝；Digest 闭包规则使依赖升级可审计。
- 负面：IDL 编写多一步显式编号；递归结构需要业务侧改用 ID 引用模式（数据库式外键）。
- 跟进：Parser/Compatibility/Golden Vector 测试（详设 26 章 V-07）。
