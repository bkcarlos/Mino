# ADR-0001：首版平台基线

- 状态：ACCEPTED
- 决策：首版正式支持 Linux x86-64、C++20 和 Bazel/Bzlmod。
- 约束：所有实际进入 v1 SHM ABI 的原子类型（当前为 `bool`、16/32/64-bit）必须 Lock-free，并通过跨进程 Litmus Test。
- 128-bit：仅作为工具链能力报告；当前生产 ABI 不使用。若 `std::atomic<unsigned __int128>` 非 Lock-free，则明确禁止将 128-bit 原子加入共享结构，不阻塞现有 ≤64-bit ABI。
- 后续：AArch64 在完成同等 ABI、原子和性能矩阵后启用；任何新增原子宽度必须先扩展 V-12 并通过目标平台验证。

## Context

Mino 的共享内存 ABI 依赖跨进程原子、固定布局和明确的工具链语义。若首版同时承诺多个 OS、架构和编译器组合，原子 lock-free 保证、共享映射行为及 sanitizer 覆盖会在基础协议冻结前形成不可控矩阵，因此需要先选择单一生产基线，并把其他平台作为独立验证门禁。

## Alternatives Considered

- **首版同时支持 Linux x86-64 与 AArch64**：扩大初始验证矩阵并延迟 ABI 冻结；AArch64 改由 V-13 独立关闭。
- **允许 ABI 使用非 lock-free 原子并依赖进程内锁**：锁状态无法在 owner 崩溃后可靠恢复，否决。
- **使用 128-bit 原子压缩复合状态**：目标工具链不保证 lock-free，且 v1 ABI 不需要，否决。
- **不固定 Bazel/C++ 版本**：构建与 CodeGen 难以复现，否决。

## Consequences

- 正面：生产 ABI、Litmus、Sanitizer 和 CI 矩阵边界清晰，可对 Linux x86-64 给出可重复结论。
- 负面：其他平台即使可以编译，也不自动获得生产支持资格；新增原子宽度需要重新验证。
- 跟进：V-12 关闭 Linux x86-64 基线；V-13 负责 AArch64 资格验证。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-07-27 | Mino 架构评审 Agent | ACCEPTED | 生产平台、工具链和原子 ABI 边界明确；V-12 作为 D0 强制门禁，AArch64 由 V-13 独立验证。 |
