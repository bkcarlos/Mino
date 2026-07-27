# ADR-0010：Shared Memory Trust Domain

- 状态：ACCEPTED
- 决策：所有获得同一 Region 读写映射的进程属于同一可信计算域。
- 约束：不同租户/安全等级拆分 Region 或通过受控 Proxy/Bridge；Accessor、CRC 和边界检查不构成对恶意已 Attach 进程的隔离。
- 非目标：首版不在单一 RW Region 内实现恶意参与者隔离。
