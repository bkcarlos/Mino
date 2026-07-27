# ADR-0012：Transport Driver 抽象与非网络 Fabric 扩展

- 状态：ACCEPTED
- 决策：跨节点传输统一经 `TransportDriver` 抽象路由；以 `TransportKind`（Network / RDMA / SharedFabric）、`TransportCapabilities` 与 Union 型 `EndpointDescriptor` 描述传输形态、能力与端点；共享窗口类 Fabric（IPCF、PCIe NTB、CXL 共享内存池等）作为 Driver 的一种实现形态接入，可选择性实现 `FabricWindowDriver` 扩展接口。
- 约束：非网络 Fabric 跨 Trust Domain 传输仍必须走 Canonical Wire（详设 16.2 帧格式），不得传本机 SHM Offset（INV-09 对 Fabric 同样成立）；可靠性与去重统一复用详设 16.5；DeliveryStage 语义不变；同一 Trust Domain 两端必须走本地 SHM 路径而非 Fabric Driver。
- 待验证：具体 Fabric 驱动（IPCF Channel、NTB 窗口）的能力映射、窗口容量与背压参数、跨域复位恢复测试（详设 26 章 V-25）。

## Context

首版跨节点传输面向 IP 网络（TCP/UDP），但目标部署场景包含不经过 IP 协议栈的跨 Trust Domain 通道：车载 SoC 的 A 核 Linux ↔ M 核 RTOS（NXP IPCF 类共享内存信箱）、PCIe NTB 跨主机共享窗口、CXL 共享内存池等。这些传输的共同点是以「对端可见的共享窗口 + doorbell/中断通知」为原语，而非 Socket 字节流。

若传输层仅硬编码 IP 端点（IP:Port），上述场景将无法接入，或被迫以私有分支侵入协议语义（例如直接传 SHM Offset 绕过编码），破坏跨域安全边界。需要在不改变传输协议语义的前提下，把「传输形态」参数化。

## Alternatives Considered

- **仅支持 IP 网络，Fabric 场景另起私有协议**：跨域语义边界（Canonical Wire、INV-09、At-least-once + 去重）会被私有实现逐个绕过，长期形成无法审计的协议分叉，否决。
- **为每种 Fabric 定义独立的一级传输协议**：IPCF、NTB、CXL 的语义差异仅在端点寻址与窗口管理，帧格式、可靠性、去重、确认语义完全相同；各自独立成协议会造成大量重复规范与测试矩阵，否决。
- **Driver 内嵌 Canal 式通用插件 SDK**：插件可以注册任意传输，但无约束的插件接口无法表达「共享窗口」「可靠有序」等能力差异，策略引擎无法据此路由；采纳其注册思想但收敛为能力模型——即本方案：`TransportCapabilities` 描述能力 + `FabricWindowDriver` 可选扩展接口。
- **Fabric 直连本地 SHM 路径（跨域共享 Slab）**：两端地址空间、分配器布局与生命周期互不信任，直接共享 Slab 意味着把 INV-09 撕开缺口，否决；跨域载荷必须重新编码与校验（详设 16.3 接收校验顺序对 Fabric 接收同样适用）。

## Consequences

- 正面：新传输形态只需实现 `TransportDriver`（必要时加 `FabricWindowDriver`）并在 Node Registry 注册端点元数据即可接入，协议语义、可靠性、确认与观测体系零改动；策略引擎依据 `TransportCapabilities` 统一在网络与 Fabric 间路由。
- 负面：`EndpointDescriptor` Union 化使端点解析多一层判别；窗口类传输引入新的资源枯竭模式（满窗），背压与复位语义需在 16.7 显式约束。
- 跟进：详设 15.3 接口定义、16.7 语义边界；IPCF 等具体驱动归属路线图 P5（详设 24 章 D6）；验证登记 V-25。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-07-27 | Mino 架构评审 Agent | ACCEPTED | Transport Driver 与 Fabric 语义边界定义完整，TransportKind/TransportCapabilities/EndpointDescriptor/FabricWindowDriver 与详设 15.3 完全一致；Canonical Wire 约束（INV-09）、可靠性复用 16.5、DeliveryStage 语义不变、同一 Trust Domain 走本地 SHM 等约束与详设 16.7 逐项对齐；备选方案论证充分，可支撑 D2/D4 开发；遗留验证项：V-25（UDP/RDMA/Fabric 驱动专项）在 P5 阶段关闭。 |
