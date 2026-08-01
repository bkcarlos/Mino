# ADR-0001：首版平台基线

- 状态：ACCEPTED
- 决策：首版正式支持 Linux x86-64、C++20 和 Bazel/Bzlmod。
- 约束：所有实际进入 v1 SHM ABI 的原子类型（当前为 `bool`、16/32/64-bit）必须 Lock-free，并通过跨进程 Litmus Test。
- 128-bit：仅作为工具链能力报告；当前生产 ABI 不使用。若 `std::atomic<unsigned __int128>` 非 Lock-free，则明确禁止将 128-bit 原子加入共享结构，不阻塞现有 ≤64-bit ABI。
- 后续：AArch64 在完成同等 ABI、原子和性能矩阵后启用；任何新增原子宽度必须先扩展 V-12 并通过目标平台验证。
