# Mino 未实现 / 未资格 / 产品边界清单

调查日期（首次）：2026-08-25 23:45 CST（UTC+8）  
**文档同步日期：2026-09-06（Asia/Shanghai）**  
仓库：`/workspace/Mino`  
**HEAD（文档对照 tip）：`d36603ecd7ed29f684ad7187468974ffd7fb098a`**
（`fix: restore release-suite green for SimpleNode and known GCC/py flakes`；
相对 `origin/master` 超前；声称 release `//...` 154/154 green）  
范围：对照 D0–D6 计划、ADR、运维手册、pipeline follow-up、代码 TODO/stub，以及
transport / discovery / 镜像路径；并与 tip 上已合入的 AEAD / nested owned-graph /
exclusive-hop recovery / residual copies / Region ID Attach / DedupStore /
SharedHostDomain v2 / hybrid graph forward / PTP+RDMA/Fabric 插件 / opt-closeout
对齐。  
不包含：新功能开发；不发明性能数字。

**总判断（tip `d36603e`）**：D0–D6 计划内源码几乎全部落地；A 段多数「缺代码」项已在 tip
关闭或降为明确外置/延期。真正仍缺的是少数协议外置能力（AEAD 会话密钥交换、A8 多
writer layout）、架构非目标（A11/A12），以及 RDMA/Fabric/HugePage/NUMA 等**硬件或
clean-ref 资格门**（驱动与软件参考插件已在树内，不算资格通过）。同机优化残留拷贝以
intentional KEEP 为主，见 `docs/optimization-status.md`。

---

## 仍 incomplete 速览（对照 tip `d36603e`）

### 代码缺口 / 明确外置或延期（非 stub 大面积）

| 项 | 状态 | 说明 |
|---|---|---|
| A1 会话密钥交换 / PKI / pipeline 自动挂 keyring | **残留外置** | 帧 AEAD（AES-256-GCM）已实现；无帧内 KEX |
| A8 subordinate writable / multi-writer Attach | **未实现（ADR-0014）** | 需 attachment registry + layout bump；tip 仅 fail-closed 探测 |
| A10 ptp4l/pmc 侧车、双机 PTP 资格、pipeline 单向延迟字段 | **残留集成/资格** | `PtpClockClient` 已落地；无合格同步仍 fail-closed |
| A12 IPsec 传输 | **未做（架构选项）** | D6 落地 TLS；树内无 IPsec 驱动 |
| SharedHostDomain → CentralSlab | **KEEP deferred** | 固定 ring 拓扑已完成；接 slab 需 ABI v3，与 SimpleNode 重复 |

### 硬件 / clean-ref 资格（代码在，门未关）

| 项 | 说明 |
|---|---|
| A2 / B3 RDMA V-25 | 真 NIC ACTIVE/LINKUP + 批准插件 SHA；软件 loopback/verbs 参考插件不算资格 |
| A3 / B4 Fabric V-25 | IPCF+NTB+CXL 三件套双机；软件参考插件不算资格 |
| B1 D4 双机 mTLS/ACL | 当前 tip 需复验；按要求可不排 hybrid |
| B2 72h soak | 历史通过绑旧 commit；未绑 `d36603e` |
| B5–B8 HugePage / 大对象池设备注册 / 多 NUMA / Storage Partition scaling | 无目标硬件或 SKIP≠通过 |
| B9–B17 | AArch64 原生、V-14… 系列 clean-ref、安全评审、生产镜像/SBOM、容量报告、滚动升级实操、监控演练、P4 SLA、pipeline 正式资格战役 |
| Hybrid 双机性能战役 | 单元测试已过；端到端 perf 未关 |

### Intentional KEEP（physics / 设计停点，不是欠债 stub）

| 项 | 说明 |
|---|---|
| 源端首发 `PopulateGeneratedFrame` | 外部字节进本 Region 必须物化 |
| `RetransmitWindow` 自持拷贝 | 可靠重传不能挪走唯一副本 |
| TcpDriver 三 mutex 布局 | 有意保留；body 已锁外 |
| Bus Broadcast 槽位 `memcpy` | 非 CentralSlab 布局物理必要 |
| RDMA 通用 span `Send` staging | owned/`pre_registered` 路径已改进；通用 Send 仍 KEEP |
| A11 128-bit 原子 | ADR-0001 生产 ABI 非目标 |

**已关闭（勿再写成缺功能）**：帧 AEAD；嵌套 owned-graph；exclusive-hop journal 恢复与 ACK→Adopt 微窗口；Region ID Attach；DedupStore；SharedHostDomain 动态发现（非 static-only）；hybrid graph forward（P8）；PTP 客户端 + RDMA/Fabric **软件参考**插件；SimpleNode `Recover()`（存在且公开）。

---

## A. 功能项（缺代码 / stub / 外置 / 明确延期）— 按 tip 标注

### A1. Wire 帧 AEAD — 帧编解码已实现；会话密钥交换仍外置

**已实现**：`FrameFlag::kAeadPresent` 走 AES-256-GCM（OpenSSL EVP，`mino/bridge/wire_aead.*`）。可选 4 字节头字段存 `key_id`；wire payload 为 `[control opcode?][nonce 12][ciphertext][tag 16]`；AAD 覆盖 canonical header（header_crc 按零）与明文 control opcode。无密钥 fail-closed（`InvalidArgument`）。`WireAeadKeyring` 提供可注入的 encode/decode 密钥 API。

**未实现 / 外置**：没有帧内密钥交换或 PKI。会话密钥需由握手/运维侧注入 keyring（pipeline 尚未自动挂载）。TLS 1.3 mTLS 仍是套接字层（`mino/security/tls.cc`），与帧内 AEAD 正交。

### A2. RDMA 硬件资格仍缺真实 NIC：树内已有可加载参考插件（P9）

`//mino/transport:rdma_driver` 是完整 `TransportDriver` 生命周期；设备边界仍是 `dlopen` 绝对路径插件（`CreateDynamicRdmaDeviceProvider`）。

**已实现（P9）**：
- `//mino/platform:libmino_rdma_software_loopback.so` — ABI v1 + MR + loopback 语义；provenance 含 `NOT-QUALIFICATION-ELIGIBLE`
- `//mino/platform:libmino_rdma_verbs.so` — 有 `libibverbs` 头时走 `ibv_reg_mr`；无头/无设备时 create 返回 nullptr（loader → unavailable）
- 插件负载测试：`//mino/platform:rdma_plugin_load_test`；`TryRegisterPayload` 在注入 `MemoryRegistrationProvider` 后可用
- owned 发送：`RdmaDriver::DoTrySendOwned` / `PostOwned` 可 `move` 免 staging `assign`；可选 `pre_registered` MR

**仍未关闭**：V-25 物理双机 ACTIVE/LINKUP、批准插件 SHA-256、`kDevice` 真 NIC。测试内 `kMock` loopback（`rdma_driver_test.cc`）与默认 `UnavailableProvider` 行为不变；生产组装仍拒绝 `kMock`。软件参考插件**不算**硬件资格。

### A3. Fabric 硬件资格仍缺真实设备：树内已有可加载软件参考插件（P9）

`//mino/transport:fabric_driver` 实现了窗口/doorbell/Canonical Wire 协议。生产必须 `CreateDynamicFabricDeviceProvider` 加载 `kDevice` 插件。

**已实现（P9）**：`//mino/platform:libmino_fabric_software_loopback.so`（device_name 选 `ipcf*`/`ntb*`/`cxl*` kind）；`//mino/platform:fabric_plugin_load_test`。provenance 含 `NOT-QUALIFICATION-ELIGIBLE`。

**仍未关闭**：V-25 要求 IPCF+NTB+CXL 三件套双机真实 sysfs/驱动/链路；缺一种 kind 不能替代。测试 mock 仍只在 `fabric_driver_test.cc`。软件参考插件**不算**物理资格。

### A4. 嵌套 owned-graph 遍历 — 已实现（codegen）

生成的 `StaticMessageTraits::CollectOwnedGraph` / `AppendOwnedChildren` 支持嵌套 message 与递归可变容器（vector of string/bytes/message 等）：根优先、确定性顺序；嵌套层通过 `CentralSlabAllocator::Inspect` 解析子 slab；深度上限 32、环/共享句柄 fail-closed（`OwnedGraphCollector`）。`kMaxOwnedGraphHandles = layout.max_dynamic_children() + 1`。叶子图仍可传 `allocator=nullptr`；嵌套非空子图缺 allocator 时返回 `kUnsupported`（`"nested owned graph requires allocator"`）。

静态 value-only Wire 适配器：inline struct 已可编解码；仍需 SHM 的嵌套 message / 非空 variable 继续走 graph-aware 重载（`ToDynamicMessage/Encode/Decode(root, allocator, …)`）。跨机所有权转发见 A5（已实现，非真跨机 SHM 零拷贝）。

### A5. Hybrid 跨机「图所有权转发 / 零拷贝」— 已实现（P8 / tip）

产品路径：`mino/bridge/graph_ownership_forward.*` + hybrid bridge
`mino_shm_tcp_bridge.cc`。跨机**不能**做真 SHM 零拷贝（无 RDMA/Fabric 内存注册时）；本项消除的是
graph→SemanticFrame→wire / wire→SemanticFrame→graph 的**中间** payload 拷贝：

- encode：SHM child `BytesView` → `EncodeInto` → canonical wire（1 次物化）
- reconstruct：wire `borrow_bytes_fields` → `DynamicBuilder`/`PopulateGeneratedFrame`（1 次 SHM memcpy）
- 仍不可避免：WireFrame body / TCP（或未来 RDMA）传输拷贝；`TryRegisterPayload` 为 P9 verbs 插件预留注册钩子

`RemoteObjectReconstructor` 默认 borrow decode。测试：
`//mino/bridge:graph_ownership_forward_test`、`wire_test` borrow 用例。
见 `docs/optimization-status.md` 与 `PERFORMANCE_FOLLOWUP.md` P3。

### A6. Coordinator / in-process Bus 不做跨进程发现（同机动态路径已补）

`LocalBusDeployment` / `Coordinator` 仍是进程内组装（heap BroadcastChannel + 进程内 registry），没有把 Coordinator 做成跨进程服务。同机多进程动态加入改走 `SharedHostDomain`（POSIX SHM peer 表 + topic 目录，Advertise/Subscribe/Recover，无预置 peer list；ABI v2 / `MINOSHD2`，含 MPSC/Broadcast/borrow/typed）。pipeline 6 进程 SHM 基准仍可选静态 manifest 以排除发现抖动；这不再是「没有动态发现 API」或「SimpleNode/同机仅 static-only」。跨机发现仍属 Bridge / topology JSON，不在此列。

### A7. Region 按 ID / Registry 查找 Attach — 已实现（tip）

Create 在发布 ACTIVE 前将 `region_id → POSIX shm name` 写入与 Region ID HWM 同身份域的持久 `region_names/` 目录；`Attach` 允许 `name` 为空且 `region_id != 0`，经 registry 解析后再走既有校验。`name` + `region_id` 仍表示按名打开并以 ID 做身份断言。见 `mino/shm/region/region_name_registry.*` 与 `RegionTest.AttachResolvesNameFromRegistry*`。

### A8. 可写非 supervisor Attach — 架构残留（ADR-0014）

调查结论：在 Region layout v6 / SuperBlock 256B 约束下，**无法**安全实现独立进程的非 supervisor 可写 Attach。ADR-0014 已否决「单 supervisor + 任意未注册 writable clients」（supervisor 死亡后无法证明 clients 已退出，destructive recovery 不安全），并明确多 writer 需要**有界、崩溃安全的 attachment registry + layout version bump**。

本 tip 实现的最大安全子集：
- 可写 Attach（含 **仅按 Region ID**）仍只授予唯一 supervisor role；
- `RegionAttachOptions::request_subordinate_writable=true` 在映射/加锁之前 **fail-closed** 返回 `kUnsupported`（显式探测 API，不提供多 writer 语义）；
- 不引入静默的第二 writer 映射。

完整 A8（subordinate writable / multi-writer）仍未实现，需未来 layout 升级；不得以超时 lease 冒充。

### A9. 持久 Dedup Store — 已实现（tip）

`mino/bridge/dedup_store.*`：CRC 保护的主机本地 HWM 快照（write + fdatasync/fsync + rename + 目录 fsync），损坏/截断 fail-closed。`BridgePipeline` 在 Create/Rebind 时从 store 种子化 `DedupWindow`，并在接收路径 `CommitAccepted`/`SeedAccepted` 之后、发 ACK 之前持久化 HWM；成功恢复时清除 `local_dedup_state_lost`（degraded → durable）。未配置 store 时仍保留原 `kDegraded` 路径。

### A10. PTP / 跨机时钟：客户端路径已落地；无合格同步仍 fail-closed（P9）

`ClockQuality` / `CrossNodeLatencyRecorder`（`mino/observability/clock.h`）之外，P9 增加 `PtpClockClient`（`mino/observability/ptp_clock.*`）：
- 配置绝对 PHC 路径（`/dev/ptpN` + `clock_gettime(FD_TO_CLOCKID)`）或显式 `clock_id`
- `PublishSync` / 可选 `assume_synchronized` 发布质量；`AllowsCrossNodeOneWayReporting()` 仅在 synchronized + 不确定度/新鲜度阈值内为真
- 未配置或未同步：fail-closed，不报告跨机单向延迟（架构 15.4 / pipeline README 不变）
- 测试：`//mino/observability:ptp_clock_test`（含 clock_gettime 路径；PHC 不可读时 SKIP）

**仍未关闭**：与 `ptp4l`/pmc 的生产侧车集成、双机 PTP 资格契约、以及在 pipeline 结果中正式启用跨机单向延迟字段。

### A11. 128-bit 原子不进入 v1 ABI（明确非目标）

ADR-0001：「128-bit：仅作为工具链能力报告；当前生产 ABI 不使用。」不是漏实现，是冻结的非目标。

### A12. IPsec 未作为传输实现

架构 14.2 把 TLS、IPsec、受控 RDMA Fabric 并列；D6 落地的是 TLS 1.3 mTLS。树内没有 IPsec 驱动。这是架构选项，不是 D6 勾选项。

---

## B. 代码已实现，当前 tip 上尚未资格关闭

下列项都有对应源文件 / runner / workflow；缺的是 **clean exact-commit、真实硬件或评审产物**。开发计划把 D2/D5/72h soak 绑在候选 `e53e1711…` 等历史提交上，**不是** 当前 tip `d36603e`。历史 KVM 战役目录 `kvm-2026-08-25/` 内 REPORT/summary 仍钉在当时 commit `c977bd1`，那是战役归档，**不要**当成 tip 资格。

1. **D4 当前候选物理双机 mTLS/ACL 复验**  
   开发计划 D4 DoD 唯一未勾：`b02eabf` 的 v4 probe 已归档 `docs/validation/physical_two_host_31291274125_manifest.json`，「当前候选修改了 Bridge/TCP/mTLS/ACL/RemoteBridge，必须重新验证」。按现要求不排 hybrid 双机。

2. **72h soak 未绑当前 HEAD**  
   D6-10 代码：`benchmarks/soak_probe/soak_probe.cc` + `tools/ci/run_long_soak.py`。通过记录属于 `e53e1711`（259,200.027 s）。当前 tip 无对应 manifest。

3. **真实 RDMA 双物理主机 final manifest（V-25）**  
   workflow `.github/workflows/rdma-qualification.yml`；文档要求 ACTIVE/LINKUP 网卡、批准插件 SHA-256、`kDevice`。Mock/软件参考 loopback 明确不算资格。

4. **Fabric IPCF+NTB+CXL 三件套双机（V-25）**  
   `.github/workflows/fabric-qualification.yml`：缺一种 kind 不能替代。无设备则 fail-closed，不是 SKIP 充当通过。

5. **HugePage 真实 `MAP_HUGETLB`**  
   `mino/platform/shared_memory_huge_page_test.cc` + `huge-page-validation.yml`。无预留页时 gtest **SKIP**；手工 CI 把 SKIP 当失败。常规 `shared_memory_test` 不证明 HugePage。

6. **HugePage + 真实设备注册（D6-08）**  
   `large_object_pool` 代码完整；无 `--plugin` 时 benchmark 注入 `kMock`，`status=SKIPPED`、`qualification_eligible=false`。需要 hugetlbfs + RDMA 插件 + `mlock` + 设备 NUMA。

7. **物理多 NUMA（D6-02）**  
   `mino/platform/numa.{h,cc}` + `numa-allocator-qualification.yml`。单 NUMA 只允许 nonqualified SKIPPED。

8. **Storage Partition scaling（D6-09 / V-24）**  
   `mino/storage/topic_partition.{h,cc}` 已实现 key/hash/source/manual 映射；`.github/workflows/storage-partition-qualification.yml` 与 artifact schema 在，目标硬件 1/2/4/8/16 scaling 未跑。

9. **原生 AArch64（V-13）只有框架，没有原生物理结果**  
   `docs/validation/AArch64_V13.md`：「不记录尚未发生的原生运行结果。」`cross-build + QEMU` 永久 `qualification_eligible=false`。代码：`tests/aarch64/`、`tools/ci/aarch64_validation.py`。

10. **V-14/V-15/V-16/V-17/V-18/V-23/V-27 Benchmark 未按 clean exact-commit 关闭**  
    开发计划 §11：验证代码已落地，开发机 `MEASURED` 不关闭登记；工作树 dirty 或未绑最终提交则仍 `PENDING`。

11. **Trust Domain / ACL / TLS 安全评审未关闭**  
    代码：Region v6 域校验、`(domain,node)` Topic ACL、`mino/security/tls.cc` OpenSSL TLS 1.3 mTLS。D6 DoD：`[ ] Trust Domain 隔离与 ACL 通过安全评审`。

12. **生产容器镜像 / SBOM（不是 schema-codegen 镜像）**  
    生产：`tools/deployment/Dockerfile`（`mino` / `mino-deploy` / `mino-node`）+ `build-image.sh`（需要 `syft`）。`container_smoke.py` 在未设 `MINO_CONTAINER_SMOKE_IMAGE` 时打印 `SKIP`。  
    Schema codegen：`tools/ci/docker/schema-codegen.Dockerfile` 是编译器对比镜像，**不能**替代生产节点镜像资格。

13. **生产容量报告覆盖全部 Topic（D6-15）**  
    `tools/deployment/capacity_report.py` + `docs/operations/capacity.md` 已写 fail-closed 契约；缺受审阅 inventory 的真实生产跑数。

14. **滚动升级 + 运维演练 qualification**  
    代码：`mino/upgrade/`、`upgrade_supervisor.cc`。`docs/operations/quick-drill-summary.json`：9/9 通过但是 `mode=quick`、`source_state=dirty`、`qualification_eligible=false`、绑的是旧 commit `f434ce1c…`。D6 DoD：`[ ] 滚动升级和故障演练完成至少一轮实操`。

15. **监控告警在演练中验证**  
    Prometheus/OTLP 代码在 `mino/runtime/deployment/monitoring.*`。DoD 未勾。

16. **P4 正式 SLA 在目标硬件达标**  
    `docs/benchmarks/Storage_SLA.md` 已发布文档；D6 DoD 第一项仍未勾。

17. **Pipeline 优化资格（clean-ref A/B、TSAN、fuzz、perf）**  
    `PERFORMANCE_FOLLOWUP.md`：2026-08-24 Linux 验证是 dirty checkout + 4-vCPU KVM，「does not satisfy the clean-ref A/B or formal performance phases」。P0–P3 / opt-closeout 优化代码已合 tip，正式资格 campaign 未关。

---

## C. 文档写明的测量 / 产品边界（不是缺模块）

1. **无合格 PTP 同步 ⇒ 不报告跨机单向延迟**  
   `PtpClockClient` 可打开 PHC/`clock_gettime` 并在 `PublishSync` 后放行；未配置或未同步仍 fail-closed。pipeline README / `RESULTS_TWO_HOST_*` 在未具备 PTP 资格契约前继续不写跨机单向延迟字段。

2. **Bus 发现不覆盖 6 进程拓扑**  
   同 README Important scope boundary：SHM backend 测的是生产 allocator/SPSC/Publisher/Subscriber，**不声称**测到 `Bus` discovery 或 Region supervisor 生命周期。同机动态发现请用 `SharedHostDomain` / `SimpleNode`，不要写成「全栈无动态发现」。

3. **Fast DDS-Gen 未做 pinned JDK 17 再生**  
   `benchmarks/pipeline_comparison/REGENERATION.md` 与 generated 头注释：Gradle 超时，checked-in 支持是按 Fast DDS-Gen 4.2.0 **模板机械复现**，「pending a pinned JDK 17 regeneration diff before formal publication」。这是对比后端出处，不是 Mino runtime 缺功能；路径仍是真实 Fast DDS 序列化，不是 mock。

4. **ADR 全部 ACCEPTED，无一 VALIDATED/FROZEN**  
   `docs/adr/README.md`：VALIDATED 需要验证产物。0001–0014 状态均为 ACCEPTED。详设第 26 章 V-13/V-20/V-22/V-24/V-25 等仍按验证登记开放。

5. **同一 RW Region 不对恶意已 Attach 进程做隔离**  
   ADR-0010 非目标。SecurityDomainId 只防误附加；真实边界是 UID/GID/namespace（`docs/operations/deployment.md`）。

6. **UDP 是真实 POSIX 数据报路径，不是 mock**  
   `mino/transport/udp_driver.cc` 使用 `sys/socket.h`、`sendmsg`/`recv`、36B fragment header。与 RDMA/Fabric 不同：UDP 不需要外部设备插件即可在 loopback/物理网卡上跑；V-25 硬件矩阵仍要把 UDP 行算进资格表。

7. **Recorder / Replay 已实现**  
   `mino/storage/recorder*.cc`、`replay_engine.cc`、`tools/mino` CLI 均在。D5 计划任务全部打勾；剩下的是当前 tip 的资格绑定，不是 stub。

8. **TCP / TLS / ACL 是真实现，不是占位**  
   `tcp_driver.cc` 非阻塞 socket；`tls.cc` OpenSSL；Coordinator Topic ACL + `CoordinatorTopicAuthorizer`。缺的是当前 tip 双机复验和安全评审（见 B）。

9. **SimpleNode 有 `Recover()`**  
   勿再写「SimpleNode 无 Recover」。显式 `Recover()` + 发布/轮询保守恢复；独占 hop（`TakeExclusive`）不在 SimpleNode 上（完整 `Publisher<T>` 路径）。

---

## D6 源文件核对（计划打勾 vs 树内是否存在）

| 计划项 | 树内证据 | 结论 |
|---|---|---|
| D6-01 位图分片 | `mino/shm/allocator/central_slab.*` | 已实现 |
| D6-02 NUMA | `mino/platform/numa.*` | 已实现，物理多 NUMA 资格待跑 |
| D6-03 批量发布/消费 | `Publisher::PublishBatch` / `Subscriber::TryPollBatch` | 已实现 |
| D6-04 sendmsg/writev | `tcp_driver.cc` 使用 `sys/uio.h` | 已实现 |
| D6-05 UDP | `mino/transport/udp_driver.*` 真实 socket | 已实现 |
| D6-06 RDMA | driver + dlopen loader + **P9 参考插件**（software loopback / verbs） | 协议层与软件插件已实现；**V-25 真 NIC 资格未关**（插件 ≠ 资格） |
| D6-07 Fabric | driver + dlopen + **P9 software loopback 插件** | 同上；IPCF+NTB+CXL 物理三件套资格未关 |
| D6-08 大对象池 | `mino/shm/allocator/large_object_pool.*` | 已实现；默认注册器 Unavailable；真设备资格待跑 |
| D6-09 Topic Partition | `mino/storage/topic_partition.*` | 已实现，scaling 资格待跑 |
| D6-10 soak | `benchmarks/soak_probe/soak_probe.cc` | 已实现；资格绑旧 commit |
| D6-11 Trust/ACL | Region v6 + Coordinator ACL | 已实现，评审待关 |
| D6-12 TLS | `mino/security/tls.cc` + OpenSSL | 已实现，双机/评审待关 |
| D6-13 部署/镜像 | `tools/deployment/Dockerfile`、`mino_node` | 已实现，生产镜像资格待跑 |
| D6-14 监控 | `mino/runtime/deployment/monitoring.*` | 已实现，演练待关 |
| D6-15 容量 | `mino/capacity/` + `capacity_report.py` | 已实现，生产报告待跑 |
| D6-16 滚动升级 | `mino/upgrade/` | 已实现，qualification 实操待关 |
| D6-17 运维演练 | `tools/operations/drill_runner.py` | 已实现；仅 dirty quick 9/9 |
| D6-18 AArch64 | `tests/aarch64/` + runner | 框架已实现，原生物理未跑 |

---

## 代码搜索说明

在 `mino/ tools/ tests/ examples/` 内检索 `TODO|FIXME|NYI|待实现|未实现|not implemented`：

- 生产代码已无硬 `AEAD framing is not implemented` stub（帧 AEAD 见 A1；会话密钥交换仍外置）
- 另外一处明确 unsupported：writable non-supervisor Attach（A8 / ADR-0014 残留；ID-only Attach 已由 region name registry 落地）
- 持久 Dedup Store 已由 `dedup_store` + pipeline 集成落地（A9）
- Exclusive hop journal 恢复与 ACK→Adopt 微窗口已落地（见 `docs/optimization-status.md`）

没有大面积 TODO stub。缺功能主要来自 **外置密钥/layout 延期、外部设备物理资格、以及 tip 资格未关**，而不是空函数。
