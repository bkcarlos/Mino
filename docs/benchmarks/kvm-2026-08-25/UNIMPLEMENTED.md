# Mino 未实现 / 未资格 / 产品边界清单

调查日期：2026-08-25 23:45 CST（UTC+8）  
仓库：`/workspace/Mino`（已有 clone，未重新 clone）  
HEAD：`c977bd18ab67b17aa98406674ba482817e812beb`（`perf: complete pipeline optimization qualification`；shallow clone 仅见此提交）  
范围：对照 D0–D6 计划、ADR、运维手册、pipeline follow-up、代码 TODO/stub，以及 transport / discovery / 镜像路径。  
不包含：双机 hybrid 测试建议（按当前要求不排）。

**总判断**：D0–D6 计划内源码几乎全部落地（D6-01～D6-18 对应 `.h/.cc`、runner、workflow 均在树内）。真正缺的是少数明确延期/fail-closed 的协议能力，以及 RDMA/Fabric/设备注册的**硬件插件**（驱动协议层已写完，仓库内只有测试 mock）。其余多数是当前 commit 上尚未关闭的资格门禁，不是缺功能。

---

## A. 功能尚未实现（缺代码 / stub / 仅 mock / 明确延期）

### A1. Wire 帧 AEAD 未实现

`mino/bridge/wire_frame.cc` 对 `FrameFlag::kAeadPresent` 直接返回 `Unsupported("AEAD framing is not implemented")`。TLS 1.3 mTLS 走的是 OpenSSL 套接字层（`mino/security/tls.cc` 引入 `openssl/ssl.h`），不是帧内 AEAD。

### A2. RDMA 硬件路径仅 mock：仓库没有 verbs 插件

`//mino/transport:rdma_driver` 是完整 `TransportDriver` 生命周期，但设备边界是 `dlopen` 外部插件（`CreateDynamicRdmaDeviceProvider` 要求绝对路径，`docs/rdma-driver.md`：「仓库不链接 ambient `libibverbs`，不含生产 software loopback」）。树内没有任何 `mino_create_rdma_provider_v1` 实现，只有 loader 的 `dlsym`。测试用 loopback 写在 `rdma_driver_test.cc`；生产组装 `RemoteBridge::CreateRdma` 拒绝 `kMock`。默认 `MemoryRegistrationProvider` 是 `UnavailableProvider`，`Register()` 返回 `"no DMA/RDMA memory registration provider is installed"`（`mino/platform/memory_registration.cc`）。

### A3. Fabric（IPCF / NTB / CXL）同样只有协议层 + 测试 mock

`//mino/transport:fabric_driver` 实现了窗口/doorbell/Canonical Wire 协议。生产必须 `CreateDynamicFabricDeviceProvider` 加载 `kDevice` 插件；`docs/fabric-driver.md`：「normal repository build contains no selectable mock provider; mocks are defined only in test source」。树内无 IPCF/NTB/CXL 设备插件源码。

### A4. 嵌套 owned-graph 遍历未实现

`mino/schema/codegen/code_generator.cc`：「User-defined and recursively variable containers are intentionally unsupported until codegen can safely walk their nested shape。」非直接叶子字段会把 `kOwnedGraphCollectionSupported=false`，生成的 `CollectOwnedGraph` 返回 `"generated owned graph requires nested traversal"`。`PERFORMANCE_FOLLOWUP.md` 也写：O(children) reclaim 只覆盖 generated 图的直接叶子字段。

### A5. Hybrid 跨机「图所有权转发 / 零拷贝」未实现

`benchmarks/pipeline_comparison/PERFORMANCE_FOLLOWUP.md` P3：**Status: not implemented.** 「The hybrid bridge still performs graph-to-semantic-to-wire and wire-to-semantic-to-graph payload copies」。这是产品级零拷贝跨机路径，不是测试缺口。

### A6. Coordinator / Bus 不做跨进程发现

`LocalBusDeployment` 注释（`mino/runtime/deployment/local_bus.h`）：「Production, local-only Bus assembly… intentionally supports one publisher per topic」。`Coordinator` 是进程内 C++ 对象（`Create` / `CreateForTesting`），没有独立网络发现服务。pipeline README：「current Coordinator and local Bus deployment do not provide cross-process discovery for this six-process topology」，所以 6 进程 SHM 用静态 manifest，不测 Bus 发现。

### A7. Region 按 ID / Registry 查找 Attach 未实现

`mino/shm/region/region.h`：「Registry lookup and ID-only Attach are not implemented。」Attach 必须带 POSIX shm name。

### A8. 可写非 supervisor Attach 未实现

同一头文件：「Writable non-supervisor Attach remains unsupported。」v6 可写 Attach 只能是唯一 supervisor。

### A9. 持久 Dedup Store 明确延期

开发计划风险管理：「首版接受 kDegraded 降级 + 指标；持久 Dedup Store 作为后续扩展」。Receiver 重启去重状态丢失走 `kDegraded`，不是漏写的 D4 主路径。

### A10. PTP / 跨机时钟同步栈没有实现

`ClockQuality` / `CrossNodeLatencyRecorder` 有数据结构（`mino/observability/clock.h`），但仓库没有 `ptp4l`/PHC 客户端。架构 15.4 与 pipeline README 都规定：没有合格 PTP 就不报告跨机单向延迟。

### A11. 128-bit 原子不进入 v1 ABI（明确非目标）

ADR-0001：「128-bit：仅作为工具链能力报告；当前生产 ABI 不使用。」不是漏实现，是冻结的非目标。

### A12. IPsec 未作为传输实现

架构 14.2 把 TLS、IPsec、受控 RDMA Fabric 并列；D6 落地的是 TLS 1.3 mTLS。树内没有 IPsec 驱动。这是架构选项，不是 D6 勾选项。

---

## B. 代码已实现，当前 commit 上尚未资格关闭

下列项都有对应源文件 / runner / workflow；缺的是 **clean exact-commit、真实硬件或评审产物**。开发计划把 D2/D5/72h soak 绑在候选 `e53e1711…`，**不是** 当前 HEAD `c977bd1`。本 shallow clone 看不到那次提交，因此不能把旧 manifest 算作本 commit 资格。

1. **D4 当前候选物理双机 mTLS/ACL 复验**  
   开发计划 D4 DoD 唯一未勾：`b02eabf` 的 v4 probe 已归档 `docs/validation/physical_two_host_31291274125_manifest.json`，「当前候选修改了 Bridge/TCP/mTLS/ACL/RemoteBridge，必须重新验证」。按现要求不排 hybrid 双机。

2. **72h soak 未绑当前 HEAD**  
   D6-10 代码：`benchmarks/soak_probe/soak_probe.cc` + `tools/ci/run_long_soak.py`。通过记录属于 `e53e1711`（259,200.027 s）。当前 commit 无对应 manifest。

3. **真实 RDMA 双物理主机 final manifest（V-25）**  
   workflow `.github/workflows/rdma-qualification.yml`；文档要求 ACTIVE/LINKUP 网卡、批准插件 SHA-256、`kDevice`。Mock/loopback 明确不算资格。

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
    Schema codegen：`tools/ci/docker/schema-codegen.Dockerfile` 是编译器对比镜像，本环境已构建并通过，**不能**替代生产节点镜像资格。

13. **生产容量报告覆盖全部 Topic（D6-15）**  
    `tools/deployment/capacity_report.py` + `docs/operations/capacity.md` 已写 fail-closed 契约；缺受审阅 inventory 的真实生产跑数。

14. **滚动升级 + 运维演练 qualification**  
    代码：`mino/upgrade/`、`upgrade_supervisor.cc`。`docs/operations/quick-drill-summary.json`：9/9 通过但是 `mode=quick`、`source_state=dirty`、`qualification_eligible=false`、绑的是旧 commit `f434ce1c…`。D6 DoD：`[ ] 滚动升级和故障演练完成至少一轮实操`。

15. **监控告警在演练中验证**  
    Prometheus/OTLP 代码在 `mino/runtime/deployment/monitoring.*`。DoD 未勾。

16. **P4 正式 SLA 在目标硬件达标**  
    `docs/benchmarks/Storage_SLA.md` 已发布文档；D6 DoD 第一项仍未勾。

17. **Pipeline 优化资格（clean-ref A/B、TSAN、fuzz、perf）**  
    `PERFORMANCE_FOLLOWUP.md`：2026-08-24 Linux 验证是 dirty checkout + 4-vCPU KVM，「does not satisfy the clean-ref A/B or formal performance phases」。P0–P2 优化代码已合，资格 campaign 未关。

---

## C. 文档写明的测量 / 产品边界（不是缺模块）

1. **无 PTP ⇒ 不报告跨机单向延迟**  
   pipeline README：「Multi-host mode … intentionally emits no cross-host one-way latency without a future PTP qualification contract。」`RESULTS_TWO_HOST_20260816.md`：「Cross-host one-way latency: **not reported**.」

2. **Bus 发现不覆盖 6 进程拓扑**  
   同 README Important scope boundary：SHM backend 测的是生产 allocator/SPSC/Publisher/Subscriber，**不声称**测到 `Bus` discovery 或 Region supervisor 生命周期。

3. **Fast DDS-Gen 未做 pinned JDK 17 再生**  
   `benchmarks/pipeline_comparison/REGENERATION.md` 与 generated 头注释：Gradle 超时，checked-in 支持是按 Fast DDS-Gen 4.2.0 **模板机械复现**，「pending a pinned JDK 17 regeneration diff before formal publication」。这是对比后端出处，不是 Mino runtime 缺功能；路径仍是真实 Fast DDS 序列化，不是 mock。

4. **ADR 全部 ACCEPTED，无一 VALIDATED/FROZEN**  
   `docs/adr/README.md`：VALIDATED 需要验证产物。0001–0014 状态均为 ACCEPTED。详设第 26 章 V-13/V-20/V-22/V-24/V-25 等仍按验证登记开放。

5. **同一 RW Region 不对恶意已 Attach 进程做隔离**  
   ADR-0010 非目标。SecurityDomainId 只防误附加；真实边界是 UID/GID/namespace（`docs/operations/deployment.md`）。

6. **UDP 是真实 POSIX 数据报路径，不是 mock**  
   `mino/transport/udp_driver.cc` 使用 `sys/socket.h`、`sendmsg`/`recv`、36B fragment header。与 RDMA/Fabric 不同：UDP 不需要外部设备插件即可在 loopback/物理网卡上跑；V-25 硬件矩阵仍要把 UDP 行算进资格表。

7. **Recorder / Replay 已实现**  
   `mino/storage/recorder*.cc`、`replay_engine.cc`、`tools/mino` CLI 均在。D5 计划任务全部打勾；剩下的是当前 commit 的资格绑定，不是 stub。

8. **TCP / TLS / ACL 是真实现，不是占位**  
   `tcp_driver.cc` 非阻塞 socket；`tls.cc` OpenSSL；Coordinator Topic ACL + `CoordinatorTopicAuthorizer`。缺的是当前 commit 双机复验和安全评审（见 B）。

---

## D6 源文件核对（计划打勾 vs 树内是否存在）

| 计划项 | 树内证据 | 结论 |
|---|---|---|
| D6-01 位图分片 | `mino/shm/allocator/central_slab.*` | 已实现 |
| D6-02 NUMA | `mino/platform/numa.*` | 已实现，物理多 NUMA 资格待跑 |
| D6-03 批量发布/消费 | `Publisher::PublishBatch` / `Subscriber::TryPollBatch` | 已实现 |
| D6-04 sendmsg/writev | `tcp_driver.cc` 使用 `sys/uio.h` | 已实现 |
| D6-05 UDP | `mino/transport/udp_driver.*` 真实 socket | 已实现 |
| D6-06 RDMA | driver + dlopen loader；无 in-tree 插件 | 协议层已实现，硬件路径 mock-only |
| D6-07 Fabric | 同上 | 协议层已实现，硬件路径 mock-only |
| D6-08 大对象池 | `mino/shm/allocator/large_object_pool.*` | 已实现；默认注册器 Unavailable |
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

- 生产代码唯一硬 `not implemented`：`AEAD framing is not implemented`
- 另外两处明确 unsupported：嵌套 owned-graph、writable non-supervisor / ID-only Attach

没有大面积 TODO stub。缺功能主要来自 **外部设备插件缺失、明确延期项、以及资格未关**，而不是空函数。
