# ADR-0010：Shared Memory Trust Domain

- 状态：ACCEPTED
- 决策：所有获得同一 Region 读写映射的进程属于同一可信计算域。
- 约束：不同租户/安全等级拆分 Region 或通过受控 Proxy/Bridge；Accessor、CRC 和边界检查不构成对恶意已 Attach 进程的隔离。
- 非目标：首版不在单一 RW Region 内实现恶意参与者隔离。

## Context

共享内存允许参与进程直接读写映射页。Handle 边界检查、CRC、Generation 和 Accessor 可以防止普通错误或陈旧引用，但无法阻止已经获得 RW 映射的恶意进程绕过 API、伪造元数据或修改其他对象。若把同一 RW Region 当作租户隔离边界，会产生无法兑现的安全承诺。

## Alternatives Considered

- **在单一 RW Region 内依赖 Accessor/CRC 隔离不可信进程**：恶意进程可直接写内存并重算元数据，否决。
- **每次访问通过特权 Broker RPC**：可以形成安全边界，但失去共享内存热路径优势；保留为跨信任域 Proxy/Bridge 方案。
- **使用页保护动态切换对象权限**：粒度、系统调用成本和对象共页问题使其不适合 v1，否决。
- **不同信任域拆分 Region**：保留零拷贝域内路径，并通过受控 Bridge 跨域，采纳。

## Consequences

- 正面：安全边界与操作系统权限模型一致，不把健壮性检查误称为恶意隔离。
- 负面：跨租户或跨安全等级通信需要额外 Proxy/Bridge、认证和复制成本。
- 跟进：部署配置必须限制 Region 权限；V-20 在产品化阶段完成威胁模型、权限和跨域演练。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-07-27 | Mino 架构评审 Agent | ACCEPTED | 明确同一 RW Region 是单一可信域，健壮性校验不构成恶意隔离；跨域必须使用独立 Region 或受控 Proxy/Bridge。 |
