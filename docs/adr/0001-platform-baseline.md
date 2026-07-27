# ADR-0001：首版平台基线

- 状态：ACCEPTED
- 决策：首版正式支持 Linux x86-64、C++20 和 Bazel/Bzlmod。
- 约束：共享原子必须 Lock-free，并通过跨进程 Litmus Test。
- 后续：AArch64 在完成同等 ABI、原子和性能矩阵后启用。
