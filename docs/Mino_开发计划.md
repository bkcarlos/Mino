# Mino 开发计划文档

> 依据《Mino 架构设计文档》第 17 章实施路线图与《Mino 详细设计文档》第 24 章分阶段代码交付制定。
> 版本：v1.0（首版开发计划）
> 关联文档：[架构设计文档](./Mino_架构设计文档.md)、[详细设计文档](./Mino_详细设计文档.md)、[ADR 目录](./adr/README.md)

## 1. 计划概述

### 1.1 当前状态基线

| 维度 | 现状 |
|---|---|
| 设计文档 | 架构设计 v1、详细设计 v0.5，已完成评审修订 |
| ADR 状态 | 13 篇中 3 篇 ACCEPTED（0001 平台基线、0008 Recorder 背压、0010 SHM Trust Domain），10 篇 PROPOSED |
| 代码 | 仓库尚无代码，需从工程骨架起步 |
| 验证登记 | 27 项（V-01~V-27），P0 优先级 14 项、P1 优先级 11 项、P2 优先级 2 项 |
| 关键不变量 | 32 项（INV-01~INV-32），贯穿测试与验收 |

**结论**：项目处于 P0（协议语义定稿）阶段，当前最高优先级是完成 ADR 评审并搭建工程骨架，为 P1 编码扫清障碍。

### 1.2 阶段映射

详设 D 系列与架构 P 系列的对应关系（详设第 24 章）：

| 交付阶段 | 对应路线 | 内容 | 预估工作量 |
|---|---|---|---|
| D0 | P0 | 工程骨架 + ADR 定稿 | 3~4 周 |
| D1 | P1 | SHM 基础（骨架/Region/Handle/Slab） | 6~8 周 |
| D2 | P1 | Channel 与 Runtime（SPSC/MPSC/Broadcast/Lease） | 6~8 周 |
| D3 | P2 | Schema（Compiler/Descriptor/CodeGen/Dynamic） | 8~10 周 |
| D4 | P3 | Bridge（Registry/Route/TCP/Wire） | 6~8 周 |
| D5 | P4 | Storage（Segment/Recorder/Replay/工具） | 8~10 周 |
| D6 | P5+P6 | 性能优化 + 产品化 | 8~12 周 |

> 工作量为单人全职估算，实际按团队规模线性折算；阶段内任务有明确并行空间（见各阶段"并行工作流"）。

### 1.3 关键路径

```text
D0 (ADR ACCEPTED + Bazel 骨架)
  └─▶ D1 (MPMC 骨架 + Slab Allocator)
        └─▶ D2 (SPSC/MPSC/Broadcast + Lease/Recovery)
              └─▶ D3 (IDL Compiler + CodeGen + Canonical Wire)
                    ├─▶ D4 (Bridge + TCP + Registry)
                    └─▶ D5 (Storage + Recorder + Replay)
                          └─▶ D6 (性能 + 产品化)
```

关键路径上的阻塞点：

1. **ADR-0002/0003/0004/0005/0006/0011 未 ACCEPTED** → D1 无法开始（Handle/Slot/Channel 语义未定）
2. **MPMC 骨架 + Slab Allocator** → 所有 Channel 和 Publisher/Subscriber 依赖
3. **Canonical Wire Format** → Bridge 和 Storage 共同依赖
4. **IDL Compiler Core** → `minoc`、Dynamic View、Bridge Schema 协商依赖

---

## 2. D0：工程骨架与 ADR 定稿（P0）

**目标**：所有 P0 级 ADR 达到 ACCEPTED；Bazel 工程可编译、可测试、CI 可用。

**入口条件**：无（项目起点）。

### 2.1 工作分解

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| D0-01 | Bazel + Bzlmod 工程初始化 | `MODULE.bazel`、`BUILD.bazel`、`.bazelrc`、`.bazelversion`、目录骨架（详设 3.1） | — | 3d |
| D0-02 | C++20 Toolchain + Warning Policy | 编译配置、`--config={debug,release,asan,ubsan,tsan,coverage}` | D0-01 | 2d |
| D0-03 | CI 流水线 | GitHub Actions / 内部 CI：构建 + 单测 + Sanitizer + Lint | D0-02 | 2d |
| D0-04 | `Status`/`Result<T>` 错误模型 | `//mino/common:status`、`//mino/common:result`（详设 5.1） | D0-01 | 2d |
| D0-05 | ADR-0002 Handle v1 布局评审 | 空间开销分析、Generation 回绕测试方案 → ACCEPTED | — | 3d |
| D0-06 | ADR-0003 Channel/Slot Metadata 评审 | IndexSlot 128B 定稿、Sidecar 分离确认 → ACCEPTED | — | 3d |
| D0-07 | ADR-0004 Canonical Wire v1 评审 | Golden Vector 初版、Fuzz 方案 → ACCEPTED | — | 3d |
| D0-08 | ADR-0005 Schema Identity 评审 | Canonicalization v1 规范、Short ID 碰撞策略 → ACCEPTED | — | 3d |
| D0-09 | ADR-0006 Delivery/Ack 语义评审 | 保证矩阵冻结、Receipt 协议 → ACCEPTED | — | 3d |
| D0-10 | ADR-0011 IDL Field Identity 评审 | 显式 Field ID、Reserved、递归检测 → ACCEPTED | — | 2d |
| D0-11 | ADR-0007 Segment Commit 评审 | Commit Marker 协议、目录 Sync 方案 → ACCEPTED | — | 2d |
| D0-12 | ADR-0009 Telemetry 评审 | 开销预算（≤1%/≤2%）、时钟规则 → ACCEPTED | — | 2d |
| D0-13 | ADR-0012 Transport Driver 评审 | Driver 接口边界、Fabric 语义约束 → ACCEPTED | — | 2d |
| D0-14 | ADR-0013 SHM 引用 Pin 评审 | Pin 计数位置、配额、崩溃恢复 → ACCEPTED | — | 2d |
| D0-15 | Linux x86-64 原子 ABI 验证（V-12） | Litmus Test、Lock-free 验证报告 | D0-02 | 5d |

### 2.2 并行工作流

- **轨道 A（工程）**：D0-01 → D0-02 → D0-03 → D0-04 → D0-15
- **轨道 B（ADR 评审）**：D0-05 ~ D0-14 可全部并行，按评审会议节奏推进

### 2.3 退出条件（DoD）

- [x] 全部 13 篇 ADR 达到 ACCEPTED 状态
- [x] `bazel build //...` 成功（空骨架）
- [x] `bazel test //...` 通过（含 Status/Result 单测）
- [ ] CI 五配置（debug/release/asan/ubsan/tsan）全绿（需推送 GitHub 触发首跑）
- [x] V-12 原子 ABI Litmus 报告产出（`tests/litmus/` 含 4 用例，CI 首跑产出 Linux x86-64 权威报告）

---

## 3. D1：SHM 基础（P1 上半）

**目标**：Region 创建/Attach/恢复、Handle 解析、Slab 分配/回收、MPMC 骨架全部可用并通过压力测试。

**入口条件**：D0 全部退出条件满足。

### 3.1 工作分解

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| D1-01 | 标识类型与 ProcessIdentity | TopicId/NodeId/PublisherId 等强类型 ID（详设 5.2）、ProcessIdentity（4.3） | D0-04 | 2d |
| D1-02 | Platform SHM 抽象 | `//mino/platform:shared_memory`：shm_open/mmap/munmap 封装、Huge Page 支持 | D0-01 | 3d |
| D1-03 | SuperBlock + Region 生命周期 | `//mino/shm/region:region`：Create/Attach/校验/状态机（详设 6.1~6.4） | D1-02 | 5d |
| D1-04 | Recovery Owner 协议 | Owner/Epoch/Lease CAS 接管、QUARANTINED（详设 6.5） | D1-03 | 3d |
| D1-05 | ShmHandle + HandleResolver | `//mino/abi:shm_abi` + Resolver 全项校验（详设 7.1~7.2） | D1-03 | 3d |
| D1-06 | 通用 MPMC 骨架 | Vyukov 有界 Ring、Control Block Init/Attach、Slot Sequence（详设 9.9） | D1-03 | 5d |
| D1-07 | Central Slab Allocator | Class 表、Bitmap Shard、Generation Array、Allocate/Retire/Reclaim（详设 8.1~8.4） | D1-05 | 8d |
| D1-08 | Slab Header CRC + 不变量校验 | static_assert 布局固定、CRC 计算（详设 8.1） | D1-07 | 2d |
| D1-09 | 恢复扫描器 | `//mino/shm/recovery:scanner`：孤儿 Slab、残留 ACK、位图一致性（架构 12.1） | D1-07 | 5d |
| D1-10 | Large Object Pool | 超出 Class 上限的大对象分配（详设 8.5） | D1-07 | 3d |
| D1-11 | Inspector 诊断工具 | Slab 一致性扫描、RingBuffer Dump（架构 15.6） | D1-09 | 3d |
| D1-12 | 单元 + 并发 + 压力测试 | ABI Size/Align 断言、Handle 边界、Bitmap 竞争、Generation 回绕 | 全部 | 持续 |

### 3.2 并行工作流

- **轨道 A**：D1-01 → D1-02 → D1-03 → D1-04 → D1-05（Region + Handle 主线）
- **轨道 B**：D1-06 在 D1-03 后可并行（MPMC 骨架）
- **轨道 C**：D1-07 ~ D1-10 在 D1-05 后并行（Allocator + 大对象）
- **轨道 D**：D1-09、D1-11 在 D1-07 后并行（恢复 + 工具）

### 3.3 关键风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 跨进程原子语义在共享映射上行为异常 | MPMC 骨架正确性 | V-12 Litmus 先行；TSAN + 专项压力测试；不使用 volatile |
| Bitmap/Generation/Header 发布顺序不可恢复 | 崩溃后数据不一致 | 严格按详设 8.3 分配顺序；`object_state.store(ALLOCATED, release)` 作为唯一发布点 |
| Generation 回绕未处理 | ABA 漏洞 | 达到 UINT32_MAX 时 Class 置 DRAINING；专项回绕测试 |

### 3.4 退出条件（DoD）

- [x] 不同进程映射不同虚拟地址时 Handle 正确解析（INV-03，`//mino/shm/region:cross_process_handle_test`：子进程 MAP_FIXED 占坑强制异地址 Attach，解析/payload/stale 拒绝全验，TSAN 通过）
- [x] MPMC 骨架跨进程守恒/回绕/满空判定测试通过（V-26，`//mino/shm/channel:mpmc_ring_xproc_test`：2P2C fork 进程 10000 条零丢失零重复（~156 次回绕）、满 kResourceExhausted/空 kWouldBlock 跨进程传播）
- [x] Slab 分配/回收在 TSAN 下无数据竞争（`bazel test --config=tsan //...` 23/23 通过，2026-07-28 macOS arm64）
- [ ] Kill 压力测试（≥1 小时）：恢复扫描报告孤儿 Slab = 0，可用空间恢复基线（需专门长跑环境）
- [x] 恢复扫描器双 Owner 竞争测试通过（V-11，`//mino/shm/recovery:double_owner_test` fork 双进程竞争/Kill 接管/Lease 失效单赢家）

---

## 4. D2：Channel 与 Runtime（P1 下半）

**目标**：SPSC/MPSC/Broadcast 三种 Channel 语义完整，Publisher/Subscriber API 可用，Lease/Recovery 闭环。

**入口条件**：D1 全部退出条件满足。

### 4.1 工作分解

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| D2-01 | IndexSlot ABI 定稿 ✅ | 128B 显式 Padding + static_assert、Sidecar 分离、不可变 CRC（`//mino/shm/channel:index_slot`，详设 9.2） | D1-06 | 2d |
| D2-02 | SPSC Channel ✅ | `//mino/shm/channel:spsc`：单 Producer/Consumer Cursor、Cache Line 分离、发布协议状态机（详设 9.4） | D2-01 | 3d |
| D2-03 | MPSC Channel ✅ | `//mino/shm/channel:mpsc`：Reservation/Owner Epoch/ABORTED Tombstone（详设 9.5） | D2-01 | 5d |
| D2-04 | MPSC Producer Crash 恢复 ✅ | Owner Lease 失效 → ABORTED → Journal 回收 → 队列推进（详设 9.5、12.3） | D2-03 | 3d |
| D2-05 | Broadcast Channel ✅ | `//mino/shm/channel:broadcast`：独立 Cursor、ACK Bitmap、Subscriber Set Snapshot（详设 9.6） | D2-01 | 5d |
| D2-06 | Broadcast Membership | 注册/注销/Lease 失效与 ACK 责任清理、Generation 绑定（详设 9.6、12.2） | D2-05 | 3d |
| D2-07 | QueueFullPolicy ✅ | kBlock/kFail/kDropNewest/kDropOldest/kSample 策略实现（`//mino/shm/channel:queue_full_policy` + SPSC 策略执行，详设 9.8） | D2-02 | 2d |
| D2-08 | Subscriber Lease | Lease 注册/心跳/失效/剔除流程（详设 12.2） | D2-06 | 3d |
| D2-09 | 静态 Publisher API | `Publisher<T>`：Allocate/Build/Validate/Reserve/Commit（详设 10.1、10.3） | D2-02 | 3d |
| D2-10 | 静态 Subscriber API | `Subscriber<T>` + `BorrowedMessage<T>`：Poll/ACK/Cursor 推进（详设 11.1~11.3） | D2-02 | 3d |
| D2-11 | ShmSharedPtr（引用 Pin） | Transfer/Pin 计数/配额/崩溃清除（详设 11.2.1、ADR-0013） | D2-10 | 3d |
| D2-12 | Delivery Receipt 框架 | Outstanding Table、Target Snapshot、Completion Policy（详设 10.5） | D2-09 | 3d |
| D2-13 | Kill/暂停/PID 复用压力测试 | Publisher 各点 Kill、Subscriber Kill、慢 Subscriber、Lease 误判边界 | 全部 | 持续 |
| D2-14 | TLA+ 模型验证 | MPSC Reservation、Broadcast Membership、Lease Eviction 形式化建模 | D2-04, D2-06 | 5d |

### 4.2 并行工作流

- **轨道 A**：D2-01 → D2-02 → D2-07（SPSC + 策略）
- **轨道 B**：D2-01 → D2-03 → D2-04（MPSC 主线）
- **轨道 C**：D2-01 → D2-05 → D2-06 → D2-08（Broadcast + Lease）
- **轨道 D**：D2-09 ~ D2-12 在 D2-02 后并行（API 层）
- **轨道 E**：D2-14 在 D2-04 + D2-06 完成后启动（形式化）

### 4.3 关键风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| MPSC Producer 在 Reserve/Build/Commit 任意点被 Kill 导致队列永久阻塞 | 系统不可用 | Reservation Owner Lease + ABORTED Tombstone 协议；TLA+ 建模；专项 Kill 测试覆盖每个状态点 |
| Broadcast ACK Bitmap 在 Subscriber ID 复用后误清理 | 提前回收 Payload | ACK 绑定 subscriber_generation；注销先清 ACK 再放 ID；V-04 竞态测试 |
| 慢 Subscriber 拖垮 Broadcast | 内存耗尽 | 最慢有效 Subscriber 决定回收点；kDropOldest 强制推进 + Gap 指标 |

### 4.4 退出条件（DoD）

- [x] SPSC 长时间回绕无 ABA、无重复/漏消费（INV-01，`spsc_channel_test` 10000 次回绕 + `spsc_channel_xproc_test` fork 双进程 20000 条 ~312 次回绕零丢失零重复）
- [ ] MPSC 1/2/8/32/128 Publisher 并发正确，Producer Kill 后队列不永久阻塞（INV-17）
- [ ] Broadcast 1/2/8/16 Subscriber 独立 Cursor 推进，ACK 责任不受 ID 复用影响（INV-05、INV-18）
- [ ] Publisher/Subscriber 随机 Kill ≥1 小时后恢复扫描无孤儿（P1 退出条件）
- [ ] ASAN/UBSAN/TSAN 全部通过
- [ ] TLA+ 模型不变量与 INV 对应（V-03、V-04）

---

## 5. D3：Schema（P2）

**目标**：IDL 可编译、静态代码可生成、动态 Schema 可注册/校验/编解码、Canonical Wire 静态与动态路径等价。

**入口条件**：D2 全部退出条件满足。

### 5.1 工作分解

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| D3-01 | IDL Lexer + Parser | Tokenizer、递归下降 Parser、AST（详设 13.1） | D0-04 | 5d |
| D3-02 | Semantic Validator | 显式 Field ID 校验、Reserved 校验、递归检测、Annotation 校验（详设 13.2） | D3-01 | 5d |
| D3-03 | Canonicalization v1 | Canonical Schema 文本生成、Digest 计算（详设 13.3.1） | D3-02 | 3d |
| D3-04 | Schema Descriptor | 不可变 Descriptor 对象、Digest/Version/Layout 标识（详设 13.3） | D3-03 | 3d |
| D3-05 | Compatibility 判定 | 字段级兼容性矩阵（详设 13.3.2） | D3-04 | 3d |
| D3-06 | Layout Planner | Dynamic SHM Layout Plan：字段偏移、Presence Bitmap、Child Slab 规划（详设 13.6） | D3-04 | 5d |
| D3-07 | Canonical Encoder/Decoder | Tagged Wire Format v1 编解码（详设 13.8） | D3-04 | 5d |
| D3-08 | Unknown Field Set | 有界保留、Wire Passthrough 支持（详设 13.6） | D3-07 | 2d |
| D3-09 | Dynamic Builder/View | 字段句柄、动态构建、Dynamic View 访问（详设 13.7） | D3-06 | 5d |
| D3-10 | Dynamic 对象分配事务 | Allocation Journal、Object Graph Walker、Abort/Recovery（详设 8.6） | D3-09 | 5d |
| D3-11 | Schema Registry | 注册/查找/去重/兼容性检查 API（详设 13.5） | D3-05 | 3d |
| D3-12 | C++ CodeGen | Builder/Accessor 代码生成（详设 7.2） | D3-06 | 5d |
| D3-13 | `minoc` CLI + Bazel Rule | `//tools/minoc`、Hermetic CodeGen Rule（详设 3.2） | D3-12 | 3d |
| D3-14 | Golden Vector + Fuzz | Canonical Wire Golden Vector、Parser/Codec Fuzz Harness | D3-07 | 持续 |
| D3-15 | Hermetic CodeGen 测试 | 跨目录/跨机器一致性、未声明 Import 拒绝（详设 23.8） | D3-13 | 2d |

### 5.2 并行工作流

- **轨道 A**：D3-01 → D3-02 → D3-03 → D3-04 → D3-05（Compiler Core 主线）
- **轨道 B**：D3-06 在 D3-04 后并行（Layout）
- **轨道 C**：D3-07 → D3-08 在 D3-04 后并行（Wire Codec）
- **轨道 D**：D3-09 → D3-10 在 D3-06 后并行（Dynamic Runtime）
- **轨道 E**：D3-12 → D3-13 在 D3-06 后并行（CodeGen + minoc）
- **轨道 F**：D3-11 在 D3-05 后并行（Registry）

### 5.3 关键风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 静态与动态路径 Wire Format 不等价 | 跨端解析失败 | 同一 Runtime Compiler Library 计算 Digest；等价性测试矩阵 |
| Canonical Digest 碰撞 | Schema 混淆 | Short ID 仅索引；碰撞时升级完整 Digest；Golden Vector 覆盖（V-08） |
| 动态对象图回收不完整 | 内存泄漏 | Allocation Journal 有界预分配；Object Graph Walker 确定性回收；Abort/Recovery 专项测试 |

### 5.4 退出条件（DoD）

- [ ] `.idl` 稳定生成 `.generated.h/.cc` + Descriptor
- [ ] 静态和动态路径产生字节一致的 Canonical Wire 输出（INV-08）
- [ ] 显式 Field ID、Reserved、递归拒绝全部按规则执行（INV-21）
- [ ] 兼容性测试矩阵通过（kWireCompatible/kReadCompatible/kWriteCompatible/kIncompatible）
- [ ] Fuzz（IDL/Descriptor/Canonical Payload）无 Crash、无越界
- [ ] Hermetic CodeGen 跨环境一致（V-07、V-08）
- [ ] 动态对象图可完整回收（INV 相关）

---

## 6. D4：Bridge（P3）

**目标**：同机和跨机端到端 Pub/Sub 可用，TCP Bridge 支持断链重连和 Schema 协商。

**入口条件**：D3 全部退出条件满足。

### 6.1 工作分解

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| D4-01 | Node Registry / Coordinator | 节点注册/发现/健康状态、Topic 元数据管理（详设 14.1~14.2） | D2-08 | 5d |
| D4-02 | Topic 生命周期管理 | CREATING→ACTIVE→DRAINING→RETIRED→DELETED（详设 14.4） | D4-01 | 3d |
| D4-03 | Transport Switcher | Route 选择、Route Handle 缓存、kDiscovery/kStatic 双策略（详设 15.1~15.2） | D4-01 | 5d |
| D4-04 | Transport Driver 接口 | Driver 抽象、Capabilities、EndpointDescriptor（详设 15.3） | D0-14 | 2d |
| D4-05 | Wire Frame 编解码 | Frame Header 显式 Encoder/Decoder（详设 16.2） | D3-07 | 3d |
| D4-06 | TCP Driver | 长连接、Length-prefixed Frame、心跳、Partial Read/Write（详设 16.4） | D4-04, D4-05 | 5d |
| D4-07 | Bridge 管线 | Local Subscriber → Encode → Send → Receive → Decode → Remote Publish（详设 16.1） | D4-06 | 5d |
| D4-08 | Schema 分发与协商 | connection_schema_ref 映射、Descriptor 按需请求、限频（详设 13.9） | D4-07 | 3d |
| D4-09 | 可靠传输与去重 | Sequence/ACK/Session Epoch/重传窗口/逐来源映射表（详设 16.5） | D4-07 | 5d |
| D4-10 | 远端对象重建 | 校验 → Slab 分配 → Decode → Validate → 本地发布（详设 16.3） | D4-07 | 3d |
| D4-11 | Publisher/Subscriber 门面 API | Bus 入口、CreatePublisher/CreateSubscriber、Transport Switcher 集成 | D4-03 | 3d |
| D4-12 | 双节点集成测试 | 同机/跨机端到端、断链重连、错误帧处理、N/N-1 Schema 互通 | 全部 | 持续 |

### 6.2 并行工作流

- **轨道 A**：D4-01 → D4-02 → D4-03（Registry + Route 主线）
- **轨道 B**：D4-04 → D4-05 → D4-06（Driver 层）
- **轨道 C**：D4-07 ~ D4-10 在 D4-06 后并行（Bridge 管线）
- **轨道 D**：D4-11 在 D4-03 后并行（门面 API）

### 6.3 关键风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| Bridge 重连后消息重复或丢失 | 数据完整性 | At-least-once + 接收端去重；Session Epoch 关联去重状态；kDegraded 上报 |
| 未知 Schema 帧导致无限编译/内存耗尽 | 安全风险 | 有界缓冲（4 MiB/1024 帧）；请求限频（16/s）；同 Digest 请求去重 |
| Bridge 不传输本机 Offset 约束被违反 | 协议安全 | INV-09 审计；Frame 中无 Offset 字段；Receiver 校验拒绝含 Offset 的可疑帧 |

### 6.4 退出条件（DoD）

- [ ] 同机端到端 Pub/Sub 通过
- [ ] 跨机双节点端到端通过
- [ ] 断链重连后按策略恢复，去重窗口正常工作（V-19）
- [ ] 错误帧被校验拒绝且不导致越界（INV-31）
- [ ] N/N-1 Schema 互通通过
- [ ] Bridge 去重窗口、Receiver 重启 kDegraded 路径测试通过

---

## 7. D5：Storage（P4）

**目标**：Recorder 管线完整，Segment 写入/恢复/回放可用，正式 SLA 文档发布。

**入口条件**：D4 全部退出条件满足（实际可与 D4 部分并行，见 7.2）。

### 7.1 工作分解

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| D5-01 | Schema Store | Descriptor 持久化、fdatasync + Atomic Rename、Ref Table（详设 17.3） | D3-11 | 5d |
| D5-02 | Segment/Record 格式 | SegmentHeader/RecordHeader 编码、Commit Marker、Trailer（详设 17.10） | D3-07 | 5d |
| D5-03 | Segment Writer | OPEN→SEALED 生命周期、Batch、Sync 策略、轮转（详设 17.7、17.11） | D5-02 | 5d |
| D5-04 | Recorder Buffer Pool | 有界 Chunk Pool、MPSC Queue、BufferFullPolicy（详设 17.5） | D5-02 | 3d |
| D5-05 | Recorder Subscriber | Borrow → Validate → Canonical Encode → Copy → ACK（详设 11.4） | D5-04 | 3d |
| D5-06 | TopicWriter | Per-Topic Single Writer、Ingestion Sequence、Gap/Tombstone（详设 17.6~17.7） | D5-03, D5-05 | 5d |
| D5-07 | 背压拓扑 | strong_consistent/isolated/best_effort 三种模式（详设 17.2） | D5-06 | 3d |
| D5-08 | Storage 崩溃恢复 | 尾部扫描、CRC/Commit Marker 校验、截断、索引重建（详设 17.12） | D5-03 | 5d |
| D5-09 | Manifest 管理 | 原子更新、Checkpoint、Seal 后补录/孤儿隔离（详设 17.12） | D5-03 | 3d |
| D5-10 | Replay 引擎 | Segment Reader → Decode → Allocate → Publish（详设 17.13） | D5-08 | 5d |
| D5-11 | Retention | 按时间/字节/Segment 数删除、Reader Pin/Lease（详设 17.14） | D5-09 | 3d |
| D5-12 | `mino` 运维工具 | record/replay/storage inspect/verify/repair CLI（详设 17.16） | D5-10 | 5d |
| D5-13 | 录制模式集成 | best_effort/memory_buffered/durable/snapshot 四种模式（详设 17.4） | D5-07 | 3d |
| D5-14 | 正式 SLA 文档 | 硬件型号、负载模型、统计口径、目标数值 | 全部 | 3d |
| D5-15 | Kill/ENOSPC/磁盘抖动测试 | 任意写入点 Kill 恢复、短写、ENOSPC、EIO、只读切换 | 全部 | 持续 |

### 7.2 并行工作流

- **轨道 A**：D5-01（Schema Store）— 仅需 D3 完成，可与 D4 并行启动
- **轨道 B**：D5-02 → D5-03 → D5-09（Segment + Manifest 主线）
- **轨道 C**：D5-04 → D5-05 → D5-06 → D5-07（Recorder 管线）— 需 D2 完成，可与 D4 并行
- **轨道 D**：D5-08 在 D5-03 后并行（恢复）
- **轨道 E**：D5-10 → D5-12 在 D5-08 后并行（Replay + 工具）
- **轨道 F**：D5-11、D5-13 在 D5-09 后并行

### 7.3 关键风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| Recorder 长时间持有 Borrow 阻塞 Slab 回收 | 实时通道被拖垮 | 强制 Encode + Copy + ACK 流程（INV 相关）；磁盘抖动不占用主 Channel |
| 掉电后 Segment 尾部不完整 | 数据不一致 | Length 首尾一致 + CRC + Commit Marker 三重校验；截断到最后完整 Record |
| Schema 先于 Record Durable 被违反 | 回放时无法解析 | 严格按 17.3 持久化顺序：Descriptor → fdatasync → Rename → Schema Table → Record |
| Gap Record 无法入队（满队列） | Ingestion Sequence 永久空洞 | Gap 唯一生成点在 TopicWriter 出队侧，不经入队路径（详设 17.6） |

### 7.4 退出条件（DoD）

- [ ] 多 Publisher 单 Writer 来源内有序（INV-10、INV-11）
- [ ] 任意写入点 Kill 后恢复到最后完整 Record，报告 Durable 边界
- [ ] Schema 先于引用它的 Durable Record 持久化（INV-14、INV-24）
- [ ] 磁盘短期暂停后 memory_buffered 数据可继续落盘
- [ ] 缓冲满时按策略背压/丢弃/失败，无静默丢失（INV-12、INV-13）
- [ ] Ingestion Sequence 无无法解释的永久空洞（INV-23）
- [ ] Replay 可按 Topic/时间/Sequence 过滤回放
- [ ] **正式 SLA 文档发布**

---

## 8. D6：性能优化与产品化（P5 + P6）

**目标**：达到 SLA 性能目标，完成安全、部署、监控和运维能力建设。

**入口条件**：D5 全部退出条件满足。

### 8.1 性能优化（P5）

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| D6-01 | 位图分片 + 每核缓存 | Slab Allocator 高竞争优化 | D5 | 5d |
| D6-02 | NUMA 感知分配 | NUMA Node 绑定、本地 Class 优先 | D5 | 5d |
| D6-03 | 批量发布/消费 | 批量 Reserve/Commit、批量 ACK | D5 | 3d |
| D6-04 | 网络批量收发 | sendmsg/writev、存储批量写 | D5 | 3d |
| D6-05 | UDP Driver | Fragment ID、分片、重组配额、超时（详设 16.6） | D4-04 | 5d |
| D6-06 | RDMA Driver | Buffer 注册/Pin/回收独立协议 | D4-04 | 8d |
| D6-07 | Fabric Driver（IPCF/NTB/CXL） | FabricWindowDriver 实现（详设 15.3、16.7） | D4-04 | 8d |
| D6-08 | 大对象专用池优化 | Huge Page、DMA/RDMA Buffer 隔离 | D1-10 | 3d |
| D6-09 | Topic Partition | 单 Writer 瓶颈后的分区扩展（详设 17.8） | D5-06 | 5d |
| D6-10 | 长稳测试 | ≥72 小时，RSS/Slab 增长 < 5%/24h | 全部 | 持续 |

### 8.2 产品化（P6）

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| D6-11 | Trust Domain 隔离与 ACL | Region 权限、Security Domain、Attach 校验（详设 22.1） | D5 | 5d |
| D6-12 | TLS/认证 | Bridge 双向认证、Topic ACL（详设 22.2） | D4-06 | 5d |
| D6-13 | 部署工具 | 节点启动脚本、配置生成、容器镜像 | D5 | 3d |
| D6-14 | 监控与告警 | Prometheus Endpoint、OTLP Exporter、告警规则（详设 21.5.8） | D5 | 5d |
| D6-15 | 容量规划 | Admission Control、节点资源预算（详设 20.4） | D5 | 3d |
| D6-16 | 滚动升级 | New Region + Drain/Cutover 流程与工具（详设 18.4） | D5 | 5d |
| D6-17 | 运维手册 + 故障演练 | 手册文档、至少一轮实操演练 | 全部 | 5d |
| D6-18 | AArch64 验证（V-13） | 与 x86-64 相同 ABI/Litmus/性能矩阵 | D6-01 | 5d |

### 8.3 退出条件（DoD）

- [ ] P4 发布的正式 SLA 在目标硬件上达标
- [ ] 长稳测试 ≥72 小时通过
- [ ] Trust Domain 隔离与 ACL 通过安全评审
- [ ] 滚动升级和故障演练完成至少一轮实操
- [ ] 监控告警规则在演练中验证有效
- [ ] 容量规划报告覆盖全部生产 Topic

---

## 9. 贯穿性工作

以下工作不绑定单一阶段，从 D0 开始持续进行：

### 9.1 测试基础设施

| 工作项 | 说明 | 启动阶段 |
|---|---|---|
| Sanitizer 矩阵 | ASAN/UBSAN/TSAN 五配置 CI | D0 |
| Fuzz Harness | IDL/Descriptor/Frame/Payload/Segment/Handle 六类 Fuzz | D3 起持续 |
| Litmus Test | 跨进程原子内存序验证 | D0（V-12） |
| TLA+ 模型 | MPSC/Broadcast/Lease 形式化 | D2 |
| 故障注入框架 | SIGKILL/SIGSTOP/网络断开/磁盘故障/时钟跳变 | D2 起持续 |
| Benchmark 框架 | 性能回归基线、报告口径（架构 16.2） | D1 起持续 |

### 9.2 可观测性

| 工作项 | 说明 | 启动阶段 |
|---|---|---|
| Counter 框架 | Thread-local/Sharded Counter、周期合并 | D1 |
| Histogram 框架 | 固定内存对数桶、热路径无分配 | D1 |
| Trace Context | 采样传播、Sidecar Buffer（详设 21.5.5） | D4 |
| 指标导出 | Prometheus/OTLP Exporter、有界 Snapshot Queue | D5 |

### 9.3 文档

| 工作项 | 说明 | 启动阶段 |
|---|---|---|
| API 文档 | 公共 API Doxygen/注释 | 各阶段同步 |
| 运维手册 | 部署、监控、故障处理 | D6 |
| SLA 文档 | 正式性能指标与验证报告 | D5 |
| ADR 维护 | 新决策记录、状态推进 | 持续 |

---

## 10. 里程碑与检查点

| 里程碑 | 标志 | 检查内容 |
|---|---|---|
| **M0：协议冻结** | D0 完成 | 13 篇 ADR 全部 ACCEPTED；工程骨架可构建 |
| **M1：SHM 可用** | D1 完成 | Region/Handle/Slab/MPMC 骨架通过 Kill 压力测试 |
| **M2：本地通信** | D2 完成 | SPSC/MPSC/Broadcast 语义正确；Publisher/Subscriber API 可用 |
| **M3：类型系统** | D3 完成 | IDL 编译、代码生成、动态 Schema、Canonical Wire 全链路 |
| **M4：跨机通信** | D4 完成 | 双节点端到端、断链重连、Schema 协商 |
| **M5：数据持久化** | D5 完成 | 录制/恢复/回放可用；SLA 文档发布 |
| **M6：生产就绪** | D6 完成 | 性能达标、安全评审、运维能力完备 |

每个里程碑检查点必须回顾：

1. 对应阶段 DoD 全部满足
2. 关联 INV 不变量有测试覆盖
3. 关联 V 项验证登记已关闭或更新
4. ADR 状态与实际验证结果一致

---

## 11. 验证登记跟踪

以下为 27 项验证登记（详设 26 章）按阶段的关闭计划：

| 阶段 | 计划关闭的验证项 |
|---|---|
| D0 | V-12（原子 ABI） |
| D1 | V-01（Handle 布局）、V-02（Slot 尺寸）、V-11（Recovery Owner）、V-26（MPMC 骨架） |
| D2 | V-03（MPSC Kill）、V-04（Broadcast Membership）、V-09（Delivery/Ack）、V-14（ACK Bitmap 上限）、V-27（引用 Pin） |
| D3 | V-05（Canonical Wire）、V-06（Dynamic Layout）、V-07（IDL Field ID）、V-08（Schema ID）、V-15（Dynamic View 开销） |
| D4 | V-19（Bridge 去重窗口）、V-21（Admission Control） |
| D5 | V-10（Segment Commit）、V-16（Writer 线程模型）、V-17（Sync 策略）、V-18（Buffer 容量）、V-23（Telemetry 开销） |
| D6 | V-13（AArch64）、V-20（Trust Domain）、V-22（Rollout）、V-24（Partition 阈值）、V-25（UDP/RDMA/Fabric） |

---

## 12. 团队配置建议

按关键路径工作量，建议最小团队配置：

| 角色 | 人数 | 职责 |
|---|---|---|
| 核心 Runtime 工程师 | 2 | D1/D2：SHM、Allocator、Channel、Lease/Recovery |
| Schema/工具链工程师 | 1 | D3：IDL Compiler、CodeGen、`minoc` |
| Bridge/网络工程师 | 1 | D4：Registry、TCP Bridge、Transport Driver |
| Storage 工程师 | 1 | D5：Segment、Recorder、Replay、`mino` |
| 测试/基础设施工程师 | 1（可兼职） | CI、Sanitizer、Fuzz、故障注入、Benchmark |

> 上述配置下 D0~D5 可并行推进的程度受角色间依赖约束；D1 和 D3 的 Compiler Core 部分可并行（D3 仅需 D0 的 Status/Result 即可完成 Lexer/Parser/AST 部分）。

---

## 13. 风险管理

| 风险 | 等级 | 影响阶段 | 缓解措施 |
|---|---|---|---|
| ADR 评审周期过长，阻塞 D1 | 高 | D0→D1 | 评审会议固定节奏；P0 级 ADR 优先；评审材料提前 3 天分发 |
| 跨进程原子语义在特定内核/编译器组合下异常 | 高 | D1 | V-12 Litmus 先行；锁定编译器和标准库版本；CI 覆盖多配置 |
| MPSC 正确性难以验证 | 高 | D2 | TLA+ 建模；确定性 Fault Point 重放；小位宽 Sequence 强制回绕 |
| Canonical Wire 静态/动态不等价 | 中 | D3 | 同一 Library 计算 Digest；Golden Vector 对比；等价性测试矩阵 |
| Bridge 去重状态在 Receiver 重启后丢失 | 中 | D4 | 首版接受 kDegraded 降级 + 指标；持久 Dedup Store 作为后续扩展 |
| Recorder 磁盘抖动导致实时通道阻塞 | 中 | D5 | 背压拓扑在 Topic 创建时显式配置；默认隔离录制 |
| 性能目标不可达 | 中 | D6 | P4 发布 SLA 前先建立可复现 Benchmark；P5 专项优化有缓冲时间 |
| 团队规模不足导致关键路径延迟 | 中 | 全部 | 阶段内任务有并行空间；优先保证 D1→D2→D3 关键路径人力 |

---

## 14. 依赖与外部约束

| 依赖 | 说明 | 影响 |
|---|---|---|
| Bazel/Bazelisk 版本固定 | `.bazelversion` 锁定 | 构建可复现性 |
| C++20 编译器 | GCC ≥ 12 或 Clang ≥ 15（x86-64 Lock-free 128-bit 原子需要 `-mcx16`） | D0 Toolchain 配置 |
| Linux 内核 ≥ 5.x | `shm_open`、`memfd_create`、`fdatasync` 语义 | D1 Platform SHM |
| TLA+ Toolbox | 形式化验证 | D2（可选但强烈推荐） |
| 目标硬件 | 性能验证用服务器（NUMA、10GbE+） | D5 SLA、D6 性能 |

---

## 附录 A：Bazel Target 与阶段对照

```text
D0: //mino/common:status, //mino/common:result
D1: //mino/abi:shm_abi, //mino/platform:shared_memory, //mino/platform:process_identity,
    //mino/shm/region:region, //mino/shm/allocator:central_slab, //mino/shm/recovery:scanner
D2: //mino/shm/channel:spsc, //mino/shm/channel:mpsc, //mino/shm/channel:broadcast,
    //mino/shm/channel:work_queue, //mino/runtime:bus, //mino/runtime:publisher, //mino/runtime:subscriber
D3: //mino/schema:descriptor, //mino/schema:runtime_compiler, //mino/schema:dynamic,
    //tools/minoc
D4: //mino/transport:switcher, //mino/bridge:tcp_bridge, //mino/registry
D5: //mino/storage:recorder, //mino/storage:replay, //tools/mino
```

## 附录 B：关键不变量与测试覆盖映射

| 阶段 | 主要覆盖的 INV |
|---|---|
| D1 | INV-03, INV-04, INV-19, INV-25（部分） |
| D2 | INV-01, INV-02, INV-05, INV-06, INV-16, INV-17, INV-18, INV-26, INV-27, INV-32 |
| D3 | INV-07, INV-08, INV-20, INV-21, INV-22 |
| D4 | INV-09, INV-28, INV-29, INV-30, INV-31 |
| D5 | INV-10, INV-11, INV-12, INV-13, INV-14, INV-15, INV-23, INV-24, INV-25 |

> 全部 32 项 INV 在 D5 完成时应均有测试覆盖；D6 主要补充 INV-28（Trust Domain）和性能相关的验证。
