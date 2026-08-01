# Mino 详细设计文档

> 零拷贝共享内存、动态 Schema、跨机传输与数据录制的工程化设计  
> 版本：v0.5（详细设计稿，按文档审查意见修订；新增 15.3、16.7、双路由策略、9.9 MPMC 骨架与 11.2.1 引用 Pin）  
> 上位文档：[Mino 架构设计文档](./Mino_架构设计文档.md)

## 1. 文档说明

### 1.1 目的

本文档将总体架构展开为可以直接指导编码、测试和评审的模块级设计，定义：

- Bazel 工程和 C++ Target 边界；
- 进程、线程和部署拓扑；
- 公共 API、核心对象及错误模型；
- 共享内存 Region、Handle、Allocator 和 Channel；
- Publisher、Subscriber、借用与回收；
- 静态及动态 Schema；
- Registry、Bridge 和网络协议；
- Recorder、存储和回放的集成边界；
- 启停、故障恢复、安全和可观测性；
- 传输阶段性能指标、低开销采样、时钟质量和指标导出；
- 单元、并发、集成、Fuzz 和性能测试要求。

### 1.2 文档关系

- 《Mino 架构设计文档》定义目标、边界、架构原则和路线图；
- 本文定义模块、接口、状态机、线程模型和实现约束，并完整包含动态 Schema 与 Storage 的专题设计；
- [`docs/adr/`](./adr/README.md) 记录不可轻易逆转的最终决策；
- 代码中的公共 ABI 和 Wire Format 以冻结后的规范及测试向量为准。

### 1.3 规范用语

- **必须**：违反后将破坏正确性、兼容性或安全性；
- **应该**：默认实现方式，偏离时需要 ADR；
- **可以**：可选优化，不得改变对外语义。

### 1.4 设计状态

| 状态 | 含义 |
|---|---|
| PROPOSED | 已提出，尚未通过架构评审 |
| ACCEPTED | 架构评审通过，可以实现原型 |
| VALIDATED | 已通过测试、模型或目标硬件验证 |
| FROZEN | ABI/协议已冻结，变更需要版本升级和 ADR |

“v1”只表示目标协议版本，不自动代表决策状态；具体状态以本章表格和 ADR 为准。ABI、Wire、Storage Format 在进入 FROZEN 前必须具备 Golden Vector 和兼容性测试。

### 1.5 文档导航

- 1～5：范围、决策、工程、部署和公共类型；
- 6～12：Region、Handle、Allocator、Channel 和生命周期；
- 13～16：Schema、Registry、Transport 和 Bridge；
- 17：Recorder、Storage 与 Replay；
- 18～22：启停、并发、配置、可观测性和安全；
- 23～27：测试、交付、不变量、待验证事项和评审清单。

### 1.6 术语表

| 术语 | 含义 |
|---|---|
| Topic | 具有 Schema、Channel 和交付策略的逻辑消息流 |
| Channel | Topic 在某传输域内的具体队列实现 |
| Slot | Ring 中固定大小的控制记录 |
| Payload | Slab 中的业务对象 |
| Borrow | Subscriber 对不可回收 Payload 的有界只读引用 |
| Descriptor | Schema 的不可变运行时描述 |
| Short ID | Schema Digest 的紧凑热路径索引，不是最终身份 |
| Receipt | 对指定 Delivery Stage 的异步完成结果 |
| Segment | Append-only 的录制数据文件 |
| Durable | 达到配置要求的持久化同步点 |
| Trust Domain | 被允许读写同一 SHM Region 的可信进程集合 |
| Schema Registry | Schema 注册、Descriptor 持久化与兼容性判定（见 13.5） |
| Node Registry / Coordinator | 节点发现、租约、Topic 元数据、Region/Topic ID 分配（见 14） |
| connection_schema_ref | 单连接生命周期的 Schema 引用，由接收侧分配，连接重建即失效（见 13.9、16.2） |
| rec_schema_ref | Recording Session 内的 Schema 引用，Session 内单调不复用（见 17.3） |
| 半开连接 | TCP 连接对端无响应但未关闭的故障形态，区别于「半包」（单帧被拆分传输） |
| Fabric Driver | 以共享内存窗口/信箱为原语的非网络 Transport Driver 实现（IPCF、PCIe NTB、CXL 等），见 15.3、16.7 |
| 路由策略（RoutePolicy） | Topic 级配置，决定远端目标节点集合如何产生：`kDiscovery`（自动发现）或 `kStatic`（显式配置），见 14.2、15.2 |
| 自动发现路由（Discovery Routing） | 订阅驱动路由：Registry 按 Subscriber 注册汇总订阅节点集合，无订阅者的节点不产生流量；新订阅者从注册切点开始接收 |
| 静态路由（Static Routing） | 配置驱动路由：Topic 元数据显式给出目标节点列表，发布即按配置扇出，与订阅发现解耦 |
| 订阅节点集合 | kDiscovery 下某 Topic 当前有活跃 Subscriber 的远端节点集合，由 Node Registry 维护并带版本号推送 |
| Route Set / route_set_version | kStatic 下静态路由表及其版本号；版本变化触发 Route Handle 失效重建（14.3、15.2） |
| 通用 MPMC 骨架 | SHM Index Ring 的跨进程并发骨架：64 位 Slot Sequence + 游标 CAS 仲裁，槽位携带 ShmHandle 引用而非数据本体；Channel 语义（9.4~9.7）在其上叠加，见 9.9 |
| 引用 Pin / Counted Borrow | 对象级长持有引用（`ShmSharedPtr`）：从 Borrow 显式 Transfer 获得，延迟已 Retire Payload 的内存复用；崩溃安全由 Lease/Recovery 兜底，见 11.2.1、ADR-0013 |

---

## 2. 技术决策与状态

| 项目 | 决策 | 状态 |
|---|---|---|
| 主要语言 | C++20 起步，是否提升到 C++23 由工具链基线决定 | ACCEPTED |
| 构建系统 | Bazel + Bzlmod + Bazelisk | ACCEPTED |
| Runtime/工具链 | Runtime、IDL Compiler、`minoc`、`mino` 均使用 C++ | ACCEPTED |
| 本地数据路径 | Shared Memory + Handle + Index RingBuffer + Slab | ACCEPTED |
| 静态消息 | Generated Builder/Accessor | ACCEPTED |
| 动态消息 | Descriptor + Layout Plan + Dynamic Builder/View | ACCEPTED |
| 网络与存储 | Canonical Wire Format，不传输或持久化 SHM Offset | ACCEPTED |
| Handle v1 | 64-bit Offset + 32-bit Generation + 32-bit 永不复用 Region ID | PROPOSED |
| 发布后可变性 | 默认不可变 | ACCEPTED |
| Broadcast 回收 | 独立 Cursor + ACK Bitmap + Lease | PROPOSED |
| 多 Publisher | Topic 支持 MPSC，来源内有序 | ACCEPTED |
| 磁盘写入 | 首版 Per-Topic Single Writer | ACCEPTED |
| Writer 扩展 | Topic Partition，不并发写同一 Segment | ACCEPTED |
| 内存录制缓冲 | 有界，完整录制时满队列必须背压或失败 | ACCEPTED |
| 首版平台 | Linux x86-64；AArch64 在原子与性能验证通过后启用 | ACCEPTED |
| 自动高可用 | 首版不提供 Recorder/Coordinator 自动主备和跨节点 Exactly-once | ACCEPTED |
| Canonical Wire v1 | 确定性 Tagged Format | PROPOSED |
| Segment Commit v1 | Length/CRC/Trailer/Commit Marker + Sync | PROPOSED |
| Telemetry v1 | 分阶段、有界采样、Sidecar Event | PROPOSED |
| Transport 扩展 | Driver 抽象 + Fabric 扩展点（ADR-0012） | PROPOSED |
| Pub/Sub 路由 | 双策略：自动发现（订阅驱动）+ 显式配置静态路由（14.2、15.2） | PROPOSED |
| 通用 MPMC 骨架 | SHM Index Ring 并发骨架：Vyukov 有界算法，Handle 引用语义（9.9） | PROPOSED |
| 对象级引用 Pin | Lease 绑定引用计数（`ShmSharedPtr`，ADR-0013）：Reclaim 条件追加无存活 Pin | PROPOSED |

### 2.1 故障模型

设计必须分别处理以下故障，不得用“进程异常”笼统代替：

- 线程异常退出；
- 进程正常退出、SIGKILL 和进程长时间暂停；
- PID 复用；
- 主机重启和掉电；
- 网络断开、半开、重复、乱序和重连；
- 文件系统 ENOSPC、EIO、只读切换和短写；
- 共享内存及磁盘数据损坏；
- Coordinator、Schema Registry 或 Recorder 暂时不可用。

### 2.2 端到端确认语义

```cpp
enum class DeliveryStage {
    kLocalPublished,
    kRemoteAccepted,
    kRecorderBuffered,
    kStorageWritten,
    kStorageDurable,
};
```

| 确认阶段 | 进程崩溃 | 主机掉电 | Bridge 断线 | Recorder 崩溃 |
|---|---|---|---|---|
| Local Published | 取决于 SHM 恢复 | 可能丢失 | 尚未远端送达 | 尚未录制 |
| Remote Accepted | 远端已入数据路径 | 可能丢失 | 需按重试协议恢复 | 不代表已录制 |
| Recorder Buffered | Recorder 内存可能丢失 | 丢失 | 已到 Recorder | 丢失 |
| Storage Written | Page Cache 可能丢失 | 可能丢失 | 已到 Recorder | 取决于 OS 回写 |
| Storage Durable | 应可从 Segment 恢复 | 取决于文件系统和硬件保证 | 已落盘 | 可恢复 |

默认 `Publisher::Publish()` 成功只表示 `kLocalPublished`，不能暗示远端或磁盘 Durable。需要更强保证时使用显式异步 Receipt/Future 等待目标 Stage，并带 Deadline。首版不宣称跨节点 Exactly-once；重试可能产生重复，消费者和 Recorder 依靠来源身份与 Sequence 去重。

注意：DeliveryStage **不是线性全序**。Remote 分支（`kRemoteAccepted`）与 Storage 分支（`kRecorderBuffered/kStorageWritten/kStorageDurable`）相互独立：纯本地录制 Topic 可达 `kStorageDurable` 而不经过 `kRemoteAccepted`。禁止对枚举做 `>=` 关系运算；多目标 Receipt 按逐目标 `reached_stage` 判定（见 10.5）。

### 2.3 保证边界

“全部录制”定义为：在 Recorder 进程存活、存储最终恢复可写、容量策略允许背压且上游遵守背压的前提下，已进入完整录制路径的消息最终落盘。它不等于掉电不丢；掉电保证必须要求 `kStorageDurable`。

---

## 3. 工程目录与 Bazel 组织

### 3.1 顶层目录

```text
Mino/
├── MODULE.bazel
├── BUILD.bazel
├── .bazelrc
├── .bazelversion
├── docs/
│   ├── architecture/
│   ├── adr/
│   ├── operations/
│   └── benchmarks/
├── mino/
│   ├── abi/
│   ├── common/
│   ├── platform/
│   ├── shm/
│   │   ├── region/
│   │   ├── allocator/
│   │   ├── channel/
│   │   └── recovery/
│   ├── runtime/
│   ├── schema/
│   ├── registry/
│   ├── transport/
│   ├── bridge/
│   ├── storage/
│   └── observability/
├── tools/
│   ├── minoc/
│   ├── mino/
│   └── build_defs/
├── schemas/
├── tests/
├── benchmarks/
├── examples/
└── configs/
```

### 3.2 Bazel Package 原则

- 每个稳定模块设置独立 `BUILD.bazel`；
- Target 尽量小而明确，禁止形成单个全量 `//mino:mino` 实现 Target；
- Public Header 与实现就近放置；
- 使用 `strip_include_prefix`/`include_prefix` 暴露 `mino/...` Include 路径；
- 默认 `visibility` 为 private；
- 只有稳定公共 API 才开放 `//visibility:public`；
- 平台差异通过 `select()` 和独立 Platform Target 隔离；
- 生成代码放入 Bazel Output Tree，不写回源码树；
- `minoc` 作为 Exec Platform 工具运行，所有 IDL Import 必须显式声明；
- CodeGen 禁止读取未声明文件、当前时间、随机数和绝对源码路径；
- Schema Hash 不受工作目录影响，同一输入在不同机器必须产生字节一致输出。

### 3.3 主要 Target

```text
//mino/common:status
//mino/common:result
//mino/abi:shm_abi
//mino/platform:shared_memory
//mino/platform:process_identity
//mino/shm/region:region
//mino/shm/allocator:central_slab
//mino/shm/channel:spsc
//mino/shm/channel:mpsc
//mino/shm/channel:broadcast
//mino/shm/channel:work_queue
//mino/shm/recovery:scanner
//mino/schema:descriptor
//mino/schema:runtime_compiler
//mino/schema:dynamic
//mino/runtime:bus
//mino/runtime:publisher
//mino/runtime:subscriber
//mino/transport:switcher
//mino/bridge:tcp_bridge
//mino/storage:recorder
//mino/storage:replay
//tools/minoc
//tools/mino
```

### 3.4 构建配置

`.bazelrc` 至少提供：

- `--config=debug`；
- `--config=release`；
- `--config=asan`；
- `--config=ubsan`；
- `--config=tsan`；
- `--config=coverage`；
- 可复现 Release 编译选项；
- CI 与本地一致的 Warning Policy。

不能在同一测试进程中混用不兼容的 Sanitizer。跨进程共享内存原子语义不能只依赖 TSAN 结论，还需要专门压力测试和模型评审。

---

## 4. 部署与进程拓扑

### 4.1 节点内拓扑

```text
┌──────────────── Node ─────────────────┐
│                                       │
│  Publisher Process A ─┐               │
│  Publisher Process B ─┼─ SHM Region   │
│  Subscriber Process  ─┤               │
│  Bridge Process      ─┤               │
│  Recorder Process    ─┘               │
│                                       │
│  Coordinator/Registry                 │
└───────────────────────────────────────┘
```

首版可以将 Coordinator 作为独立进程，也可以嵌入管理进程，但其租约和元数据接口必须独立，避免与数据路径耦合。

### 4.2 跨节点拓扑

```text
Node A Runtime → Bridge A ══ TCP ══ Bridge B → Node B Runtime
                         ╲            ╱
                          Registry/Discovery
```

Bridge 是普通 Subscriber 和 Publisher 的组合：

- 入方向订阅本地 Topic、编码并发送；
- 出方向接收网络帧、校验、分配远端 Slab、解码并发布；
- Bridge 不传输本地 Handle；
- Bridge 不拥有业务对象的长期生命周期。

### 4.3 进程身份

```cpp
struct ProcessIdentity {
    uint64_t node_id;
    uint64_t process_id;
    uint64_t process_epoch;
    uint64_t start_time_ns;
};
```

`process_epoch` 必须能区分 PID 复用。Linux 首版可以结合 PID、进程启动时间和随机启动 Epoch。

---

## 5. 公共类型与错误模型

### 5.1 Status

公共 API 不使用异常跨模块边界传播错误，采用 `Status`/`Result<T>`：

```cpp
enum class StatusCode : uint16_t {
    kOk,
    kInvalidArgument,
    kNotFound,
    kAlreadyExists,
    kResourceExhausted,
    kWouldBlock,
    kTimeout,
    kSchemaMismatch,
    kCorruption,
    kUnavailable,
    kPermissionDenied,
    kUnsupported,
    kDegraded,   // 服务继续但保证降级（如重连后去重状态丢失、Recorder 掉队）
    kInternal,
};

class Status {
public:
    StatusCode code() const noexcept;
    std::string_view message() const noexcept;
    bool ok() const noexcept;
};

template <typename T>
class Result;
```

共享内存热路径错误不能动态拼接大字符串。详细诊断写入进程本地日志或诊断缓冲。

### 5.1.1 错误场景映射表

正文中的错误/拒绝场景统一映射如下，引用错误码时使用枚举字面量：

| 场景 | StatusCode | 说明 |
|---|---|---|
| 队列满且策略为 `kFail` | `kResourceExhausted` | 消息未入队 |
| 队列满且策略为 `kDropNewest`/`kDropOldest`/`kSample` | `kOk` + `mino_channel_dropped_total` 指标 | 消息已被按策略丢弃，Publish 返回成功 |
| 队列满且策略为 `kBlock` 超时 | `kTimeout` | Deadline 内未能入队 |
| Slab Class 耗尽 | `kResourceExhausted` | 见 8.3 |
| Receipt Outstanding Table 耗尽 | `kResourceExhausted` | Publish 失败，消息已本地提交但无 Receipt |
| Schema 不兼容变更 | `kSchemaMismatch` | 注册/绑定被拒绝 |
| 动态 Schema 超限（复杂度/容量） | `kInvalidArgument` | 见 13.4 CompileOptions |
| Trust Domain 拒绝 Attach | `kPermissionDenied` | 见 22.1 |
| 重连后去重状态不可证明唯一 | `kDegraded` | 见 16.5，伴随指标导出 |
| Registry/Coordinator 暂时不可用 | `kUnavailable` | 缓存可继续，新注册失败 |
| Deadline 到期 | `kTimeout` | 不撤销已提交消息 |
| 输入帧/文件损坏 | `kCorruption` | 隔离并告警 |
| 平台/驱动不支持请求的能力 | `kUnsupported` | 如 UDP 大消息未启用 |

### 5.2 标识类型

禁止在接口中混用裸 `uint32_t` 表示不同 ID：

```cpp
struct TopicId { uint32_t value; };
struct TypeId { uint32_t value; };
struct NodeId { uint64_t value; };
struct PublisherId { uint64_t value; };
struct SubscriberId { uint32_t value; };
struct SchemaId { uint64_t value; };
```

序列化时使用显式固定宽度整数；C++ Wrapper 不直接作为磁盘或网络结构写出。

### 5.3 时间

- `timestamp_ns` 必须注明时钟域；
- 延迟测量优先使用 Monotonic Clock；
- 跨节点事件时间使用同步后的 Wall Clock，并记录 Clock Quality；
- 不直接比较来自不同节点的 Monotonic Timestamp。

---

## 6. Shared Memory Region

### 6.1 Region 生命周期

```text
ABSENT
  │ Create
  ▼
INITIALIZING
  │ Header + Directory + Allocator initialized
  ▼
ACTIVE
  │ clean shutdown
  ▼
CLOSED

ACTIVE/INITIALIZING
  │ crash
  ▼
DIRTY → RECOVERING → ACTIVE or QUARANTINED
```

### 6.2 Region API

```cpp
struct RegionCreateOptions {
    std::string name;
    uint64_t size_bytes;
    // region_id 由 Authoritative Registry 在创建时分配，调用者不得指定
    bool read_only = false;
};

struct RegionAttachOptions {
    std::string name;        // 通过 Registry 解析为 region_id
    uint32_t region_id = 0;  // 或显式指定（与 name 二选一）
    bool read_only = false;
};

class SharedMemoryRegion {
public:
    static Result<SharedMemoryRegion> Create(const RegionCreateOptions&);
    static Result<SharedMemoryRegion> Attach(const RegionAttachOptions&);

    std::byte* base() noexcept;
    uint64_t size() const noexcept;
    uint32_t region_id() const noexcept;
    Status Detach();
};
```

`Create` 内部向 Registry 申请持久 `region_id`（Durable High-water Mark，见 13.10），创建成功后通过 `region_id()` 返回；`Attach` 通过名称或显式 ID 解析目标 Region。

SuperBlock v3 采用 supervisor-owner 契约（ADR-0014）：`read_only=true` 支持任意数量跨进程 Attach，且永不修改生命周期或运行恢复扫描；`read_only=false` 请求唯一 writable supervisor role，live supervisor 存在时返回 `kWouldBlock`。v3 ABI 无多进程 writer 注册表，因此独立进程并发 writable Attach 明确不支持，不能以超时 service lease 模拟支持。

### 6.3 Attach 校验顺序

1. 校验文件/对象权限；
2. 映射最小 Header；
3. 校验 Magic 和 Header 长度；
4. 校验 Layout Version、Byte Order 和 Feature Flags；
5. 校验 Region Size 与实际映射对象；
6. 校验 Page Size 和必要对齐；
7. 校验 Header CRC；
8. 映射完整 Region；
9. 校验 Directory Offset、Allocator Offset 和数据区边界；
10. writable Attach 获取唯一 supervisor lock，读取 `service_owner + service_epoch`，并用完整 `ProcessIdentity` 判活；
11. 仅当 lock 已释放且旧 incarnation 明确死亡时，将 `ACTIVE` 发布为 `DIRTY`；`Alive/Unknown` 禁止 destructive recovery；
12. 必要时获取 Recovery Ownership 并进入恢复流程。

任何长度加法必须使用 Checked Arithmetic。

### 6.4 SuperBlock

SuperBlock 的磁盘式字段与跨进程原子字段应该分区，避免普通 Header 校验与高频原子变量共享 Cache Line。

```text
SuperBlock
├── Immutable Header
│   ├── magic
│   ├── layout_version
│   ├── header_size
│   ├── region_size
│   ├── byte_order
│   ├── region_uuid        // 128 位，创建时随机生成，Attach 必须校验
│   └── offsets
├── Lifecycle Control
│   ├── region_epoch
│   ├── clean_shutdown
│   ├── recovery_owner / recovery_lease / recovery_epoch
│   └── recovery_fence_word
└── Feature/Compatibility
    ├── feature_flags
    ├── minimum_reader_version
    ├── service_owner       // v3, full ProcessIdentity
    └── service_fence_word  // v3, {service_epoch, phase}
```

`region_uuid` 在 `Create` 时由安全随机源生成，随 Immutable Header 一起初始化并受 Header CRC 保护；Attach 时与 Attach Context 中登记的部署身份比较，不一致直接拒绝。

SuperBlock 固定为 256B。v3 将 v2 的 bytes `[216,256)` compatibility padding 定义为 32B `service_owner` 与 8B `service_fence_word`，v2 既有字段偏移不变。v3 reader 对 v2 仅提供只读 Attach；v2 不具备安全 service liveness/fencing，writable Attach 必须拒绝并要求迁移/重建。

### 6.5 Recovery Owner

多个进程不能同时恢复同一 Region。SuperBlock 必须包含 `recovery_owner + recovery_epoch + recovery_lease + recovery_fence_word`。Recovery Lease 只串行化已经进入 `DIRTY/RECOVERING` 的 scanner，不承担 ACTIVE service 判活；service liveness 由 supervisor lock + `ProcessIdentity` 决定，避免长暂停/`SIGSTOP` 被 lease 超时误判。恢复流程：

1. Attach 者发现 Dirty Region 后禁止业务读写；
2. 通过 CAS 获取 Recovery Ownership；
3. 周期更新 Recovery Lease；
4. 其他进程等待、超时或只读诊断；
5. Owner 崩溃且 Lease 失效后，新进程递增 Epoch 接管；
6. 成功后先发布新 Region Epoch，再切换为 ACTIVE；若在两者之间崩溃，新 Owner 看到 Epoch 已更新但状态仍为 RECOVERING 时，必须重新执行完整恢复流程（Epoch 再次递增）；
7. 无法可靠恢复时进入 QUARANTINED，只有授权工具可 Force Recovery。

### 6.6 平台与原子 ABI

首版正式支持 Linux x86-64。AArch64 必须通过相同测试矩阵后启用。所有共享原子字段必须验证：

- 目标宽度 Lock-free；
- Alignment 满足平台要求；
- 进程间共享映射上的原子语义；
- 编译器、标准库和编译选项组合在支持矩阵内；
- 对象由 Region 初始化流程显式构造；
- 不使用 `volatile` 代替同步。

等待策略按 Topic 配置为 Busy Poll、Spin-then-park 或 Futex/Park；实时线程不得因默认 Futex 策略产生不可控调度延迟，普通线程也不得无限 Busy Poll。

---

## 7. Handle 与安全解引用

### 7.1 Handle

Handle v1 提案固定为 128 位：

```cpp
struct alignas(8) ShmHandle {
    uint64_t offset;
    uint32_t generation;
    uint32_t region_id;
};
static_assert(sizeof(ShmHandle) == 16);
```

规则：

- 64 位 Offset 消除单 Region 4 GiB 上限；
- Region ID 由 Authoritative Registry 持久化分配，在一个部署身份域内永久不复用；
- Region SuperBlock 仍保存 64 位 `region_epoch` 和 Region UUID，Attach 时必须校验；
- Handle 只在已 Attach 的 Region Directory 上解析，Region ID 不匹配直接拒绝；
- 32 位 Generation 发生回绕前必须将对应 Slab Class/Region 置为 DRAINING 并迁移，禁止静默回到零；
- Handle 不能跨网络或写入长期存储。

该布局状态为 PROPOSED，必须在 Layout v1 FROZEN 前完成空间开销、热路径和回绕测试。

### 7.2 Resolver

```cpp
class HandleResolver {
public:
    template <typename T>
    Result<T*> ResolveMutable(ShmHandle handle, TypeId expected);

    template <typename T>
    Result<const T*> Resolve(ShmHandle handle, TypeId expected) const;
};
```

解引用必须校验：

- Region ID、Region UUID 和当前 Region Epoch；
- Region ID 是否已退休或被非法复用；
- 空 Handle；
- Offset 对齐；
- Header 和 Object Size 加法溢出；
- 数据区边界；
- Slab 位图占用状态；
- `object_state`：`Resolve` 要求 `PUBLISHED`（或 Reader 明确协商的状态）；`ResolveMutable` 要求 `ALLOCATED` 或 `BUILDING` 且 Owner 为当前进程；
- Generation（与 Generation Array 中的权威代数一致）；
- Type ID；
- Schema Identity；
- Object Size 不超过 Capacity。

不得向业务代码暴露未经校验的 `region_base + offset`。

---

## 8. Central Slab Allocator

### 8.1 内存组织

```text
Allocator Metadata
├── Class Descriptor[0..N)
├── Sharded Allocation Bitmap
├── Generation Array
├── Recovery Metadata
└── Metrics Counters

Data Region
├── Class 64 B Slots
├── Class 256 B Slots
├── Class 2 KiB Slots
└── Class 64 KiB Slots
```

Class 大小必须通过真实消息尺寸分布确定，不作为永久 ABI 写死在代码中；Region 创建后，本 Region 的 Class Table 不可随意改变。

Slab Header v1 提案：

```cpp
struct alignas(64) SlabHeader {
    uint32_t magic;
    uint16_t header_version;
    uint16_t class_id;

    uint32_t generation;
    std::atomic<uint32_t> object_state;

    uint32_t capacity;
    uint32_t object_size;
    uint32_t type_id;
    uint32_t layout_version;

    uint64_t schema_short_id;
    uint64_t owner_epoch;
    uint64_t allocation_transaction_id;
    uint32_t immutable_header_crc;
    uint32_t reserved;
};
static_assert(sizeof(SlabHeader) == 64);
```

`object_state` 至少包含 ALLOCATED、BUILDING、PUBLISHED、RETIRED 和 ABORTING。Generation Array 是空闲/占用切换期间的权威代数，分配时递增后复制到 Header；Resolver 必须比较两者。Immutable Header CRC 在 Publish 前、对象字段冻结后计算，覆盖 Magic、Version、Class、Generation、Capacity、Object Size、Type、Layout 和 Schema Short ID；不覆盖 CRC 字段自身、`object_state`、Owner/Transaction 等恢复期可变字段。Schema 完整 Digest 通过 Registry 中的 Short ID 映射校验。

### 8.2 Allocator API

```cpp
struct AllocationRequest {
    uint32_t object_size;
    TypeId type_id;
    SchemaIdentity schema;
    uint32_t alignment;
};

class CentralSlabAllocator {
public:
    Result<ShmHandle> Allocate(const AllocationRequest&);
    Status Retire(ShmHandle);
    Status Reclaim(ShmHandle);
    Result<SlabView> Inspect(ShmHandle) const;
};
```

### 8.3 分配算法

分配必须保证「位图、Generation、Header」三者的发布顺序可恢复：

1. Checked Align 请求大小；
2. 选择最小可容纳 Class；
3. 选择 Bitmap Shard；
4. 查找空闲位；
5. CAS 设置占用；
6. 递增 Generation Array 中该 Slot 的代数并复制到 Header；达到 `UINT32_MAX` 时将 Class/Region 标记为 DRAINING，禁止回绕；
7. 写入 Owner Epoch、Transaction ID、Schema/Layout 和 Header 其余字段；
8. **以 `object_state.store(ALLOCATED, memory_order_release)` 作为分配完成的唯一发布点**；
9. 返回 Handle。

崩溃恢复约定：位图已占用但 `object_state` 非合法已发布状态的 Slot，其 Generation 从未对外发布，Recovery 可安全清除位图回收。`ALLOCATED` 之前的中间状态不产生有效 Handle，不产生 ABA 风险。

如果 Class 耗尽，可根据策略：

- 尝试更大 Class；
- 返回 `kResourceExhausted`；
- 触发 Publisher 背压；
- 禁止无界等待。

### 8.4 回收

`Retire` 表示不再产生新 Borrow（现存 Borrow 可能仍存活）；`Reclaim` 只能在「无有效 Borrow ∧ 无存活 Pin（11.2.1）」后清除 Bitmap。恢复扫描看到 RETIRED 状态时，必须确认两个条件同时成立后才能 Reclaim。Generation 在复用时递增，并定义回绕测试；Generation 不能替代正确的生命周期协议。

### 8.5 大对象

超过普通 Class 上限的对象使用独立 Large Object Pool：

- Huge Page 或普通 Page 后端；
- 明确最大对象和总容量；
- 可选分段，但必须生成遍历、校验和回收计划；
- DMA/RDMA Buffer 不与普通 Slab 混用所有权协议。

### 8.6 动态对象分配事务

动态对象首版禁止循环引用，也禁止多个父对象共享需要自动回收的子 Slab。Builder 维护有界 Allocation Journal：

```text
BuilderTransaction
├── owner ProcessIdentity
├── transaction_epoch
├── root_handle
├── allocated_child_handles[]
└── state: BUILDING / COMMITTED / ABORTING
```

Journal 存储在共享内存 Recovery Metadata 区域（见 8.1），按 Owner ProcessIdentity 索引，Recovery 扫描时遍历所有活跃 Journal。容量由 Schema Layout Plan 预计算（子 Slab 上限 × 条目宽度），不能在共享内存中无限增长。

每次子 Slab 分配先登记 Journal，再写入父对象。Commit 在完整 Validator 通过后原子发布 Root；Abort 或 Owner Lease 失效时按 Journal 逆序回收。Journal 本身容量必须由 Schema Layout Plan 预计算，不能在共享内存中无限增长。

Descriptor 必须提供确定性 Object Graph Walker，用于 Validate、Encode、Abort 和 Recovery。若未来支持共享子对象或环，必须引入独立引用/Tracing 协议，不得复用首版树形回收逻辑。

---

## 9. Channel 模型

### 9.1 统一 Channel 接口

```cpp
class Channel {
public:
    virtual Result<Reservation> Reserve() = 0;
    virtual Status Commit(Reservation&&, const MessageIndex&) = 0;
    virtual Result<BorrowToken> Poll(SubscriberId) = 0;
    virtual Status Ack(BorrowToken&&) = 0;
};
```

虚接口不放入共享内存。实际热路径使用模板或具体类，以上仅表达语义边界。

### 9.2 IndexSlot

```cpp
struct alignas(64) IndexSlot {
    uint32_t msg_type;
    uint32_t schema_version;
    uint64_t schema_short_id;
    uint32_t schema_layout_version;
    uint32_t reserved;

    uint64_t sequence_num;
    uint64_t timestamp_ns;

    ShmHandle payload;
    uint32_t payload_len;
    uint32_t immutable_metadata_crc;

    std::atomic<uint32_t> state;
    uint32_t flags;
};
```

- `msg_type`：CodeGen 由 `canonical_digest` 低 32 位派生的快查 ID，Registry 保证 Topic 内唯一；接收侧必须与 `connection_schema_ref`/Schema Identity 交叉校验，不一致时拒绝；
- `schema_version`：编码为 `major << 16 | minor`（见 13.3），表示 Schema 定义版本；
- `schema_layout_version`：SHM 对象布局版本（13.6 的 Layout Plan 版本），与 schema_version 独立演进；
- `flags`：bit0 = HAS_CHILD_SLABS，bit1 = LARGE_OBJECT，bit2~31 保留必须为 0。

Base Slot 不保存 ACK Bitmap、Refcount 或 Claim。Channel-specific Metadata 使用同容量、同索引 Sidecar：

```cpp
struct BroadcastSlotMeta {
    uint64_t subscriber_set_version;
    AckBitmap ack_bitmap;
};

struct WorkQueueSlotMeta {
    uint32_t claimant_id;
    uint32_t claimant_generation;
    uint32_t delivery_attempt;
    std::atomic<uint32_t> claim_state;
};

`claimant_id` 使用 `SubscriberId` 命名空间（Work Queue Consumer 在 Lease/注册体系中视为 Subscriber），避免引入第三套身份。

struct MpscReservationMeta {
    uint64_t owner_process_epoch;
    uint64_t owner_publisher_id;
    uint64_t reservation_timestamp_ns;
    uint64_t reservation_sequence;
};
```

`immutable_metadata_crc` 只覆盖 Msg Type、Schema、Sequence、Timestamp、Payload Handle 和 Payload Length；不覆盖 State、ACK Bitmap、Claim、Lease 或其他可变字段。CRC 用于损坏检测，不能提供并发同步或恶意篡改防护。

最终 ABI 必须补显式 Padding。按当前字段累计约 80 字节，固定为 **128 字节（两个 Cache Line）**，通过 `sizeof`、`alignof`、`offsetof` Static Assert 和跨编译器测试固定。Sidecar 与 Slot Sequence 一起校验，旧 Sequence 的 Sidecar Metadata 不能作用于已复用 Slot。

### 9.3 发布协议

采用「先构建 Payload，后预留 Slot」的顺序（详设 10.3 的完整流程），Slot 占用窗口内协议：

```text
Payload 已在 Slab 构建并校验完成
   │
   ▼
Reserve Slot + Owner Metadata
   │  CAS: FREE → RESERVED（成功 acquire，失败 relaxed）
   ▼
state = WRITING（store relaxed）
   │
   ├─ Fill Metadata（Handle、类型、Sequence、Timestamp）
   └─ Initialize ACK responsibility
   ▼
state.store(READY, release)
   │ failure/owner lost
   └─→ CAS: state = ABORTED（acq_rel）
```

消费者用 Acquire Load 观察 READY。普通字段不能在 READY 后修改。各转换边内存序：FREE→RESERVED 成功 CAS 使用 acquire（获得 Slot 写权限）、失败 relaxed；RESERVED→WRITING 使用 relaxed（同一线程内状态推进）；WRITING→READY 使用 release（发布 Payload 与元数据）；READY→RETIRED、*→ABORTED 使用 acq_rel CAS。

### 9.4 SPSC

- 单 Producer Cursor；
- 单 Consumer Cursor；
- Slot Sequence 防止回绕误判；
- 首个原型和 Benchmark 基线；
- Producer/Consumer Cursor 分离 Cache Line。

### 9.5 MPSC

多 Publisher Topic 使用 MPSC。Slot 状态机及转换责任者：

```text
FREE ──(Publisher CAS, acq_rel)──▶ RESERVED
RESERVED ──(Owner store, relaxed)──▶ WRITING
WRITING ──(Owner store, release)──▶ READY
WRITING ──(Owner 或 Recovery CAS, acq_rel)──▶ ABORTED
READY ──(Consumer 消费完成后 CAS, acq_rel)──▶ RETIRED
ABORTED ──(Consumer 遇到 Tombstone 直接推进, CAS acq_rel)──▶ FREE
RETIRED ──(Consumer 推进 Cursor 越过后，由 Channel 回收线程或下次 Reserve 扫描 CAS)──▶ FREE
```

- Producer 通过通用 MPMC 骨架（9.9）预留逻辑位置；
- Reservation 记录 Owner Process/Publisher Identity、Owner Epoch 和 Reservation Timestamp；
- 每个 Slot 使用 64 位 Sequence 区分预留、提交和回绕；Cursor 保存逻辑 Sequence 而非物理索引，物理位置为 `sequence % capacity`；Ring 满判定为 `producer_sequence - min_consumer_sequence >= capacity`；
- Consumer 遇到未完成 Slot 时，先检查 Owner Lease，不能仅因超时就判定崩溃；
- Owner 确认失效后，Recovery CAS 将 Slot 从 RESERVED/WRITING 切到 ABORTED；
- ABORTED 作为 Tombstone 推进顺序并回收 Builder Allocation Journal；
- 暂停但仍有效的 Producer 不得被误回收；
- 是否允许越过未失效空洞由 Channel Policy 明确，首版严格有序 MPSC 不越过；
- Producer Kill、暂停、PID 复用和 Sequence 回绕必须有专项测试。

### 9.6 Broadcast

- 每个 Subscriber 独立 Cursor；
- Subscriber Slot 包含 `subscriber_id + subscriber_generation`，ID 复用必须递增 Generation；
- 发布时读取不可变 Subscriber Set Snapshot（含 `subscriber_set_version`），并据此生成 ACK 责任；
- ACK Bitmap 操作协议：bit 索引 = `subscriber_id`（要求 `subscriber_id < max_subscribers`）；发布时初始化为 Snapshot 对应的位集合（需 ACK 位置 1）；Subscriber ACK 时 CAS 清除自己的 bit；全部 bit 清零 = 所有 Snapshot 内订阅者完成，Payload 可 Retire；
- `subscriber_set_version` 变化（注册/注销/剔除）后，新 Slot 使用新 Snapshot；已存在 Slot 的 ACK Bitmap 不追溯修改，注销流程负责清除其 Generation 对应的遗留 bit；
- 注册操作以 `join_sequence` 为切点，新 Subscriber 不追溯此前消息；
- 注销操作先阻止新 Borrow，再清理该 Generation 的 ACK 责任；
- Lease 失效清理必须校验 Generation，不能误清理复用后的新 Subscriber；
- ACK Bitmap 超过固定最大订阅者数时拒绝注册或切换其他回收方案；
- 最慢有效 Subscriber 决定 Slot/Payload 最早回收点；
- 慢订阅者策略由 Topic 配置决定。

### 9.7 Work Queue

- 多 Consumer 竞争共享消费 Cursor；
- 每条消息只交给一个 Consumer；
- 是否支持失败重试和重新投递必须单独定义；
- 不与 Broadcast 共用一套模糊 ACK 语义。

### 9.8 队列满策略

```cpp
enum class QueueFullPolicy {
    kBlock,
    kDropNewest,
    kDropOldest,
    kSample,
    kFail,
};
```

`kDropOldest` 必须保证没有 Reader 仍 Borrow 被覆盖 Payload；否则只能丢弃索引引用并延迟回收，不能直接复用内存。

策略适用性：

| 策略 | SPSC/MPSC（单消费者） | Broadcast | Work Queue |
|---|---|---|---|
| `kBlock` | ✓ | ✓ | ✓ |
| `kFail` | ✓ | ✓ | ✓ |
| `kDropNewest` | ✓ | ✓（丢弃后所有 Subscriber 跳过该 Slot，记录 Gap 指标） | ✓ |
| `kDropOldest` | ✓ | ✓（强制推进最慢 Subscriber Cursor，记录 `broadcast_gap` 指标；被越过 Slot 的 Payload 仅在无 Borrow 时可复用） | ✓ |
| `kSample` | ✓ | ✓（按统一采样率，所有 Subscriber 看到相同子集） | ✗（破坏每条消息恰好消费一次的语义） |

### 9.9 通用 MPMC 骨架（SHM Index Ring 并发骨架）

通用 MPMC 骨架是所有跨进程 Channel（9.4~9.7）共享的并发骨架，是 ADR-0003「公共 Ring 逻辑共享、语义分离」的载体。骨架本体位于 SHM Region 内，由多进程共享；它只回答一个问题——**多生产者多消费者在同一共享 Ring 上的有界预留、推进与回绕如何并发正确**，不回答任何消费语义问题。

**存储布局（SHM Region 内）**：

```text
Control Block（64B 对齐）
  enqueue_pos / dequeue_pos   游标分离 Cache Line（9.4 约定）
  magic + layout_version      Init/Attach 校验
  elem_size + elem_align      槽位内容 ABI 校验（防止不同编译配置的进程错配）
  capacity                    2 的幂
Slot Array（同 Region，capacity 个定长槽位）
  per-slot 64 位 Sequence     并发元数据：空闲 / 已提交 / 回绕周期判别
  定长存储位                  承载 IndexSlot（9.2）或裸 ShmHandle
```

**引用语义（零拷贝根基）**：槽位携带的是对 Central Slab 中 Payload 的**引用**（`ShmHandle` 或内嵌 Handle 的 IndexSlot），骨架从不移动数据本体；控制块与槽位内**不得出现进程虚拟地址指针**，跨进程寻址一律使用 Region Offset/Handle（与 4.2、INV-09 同源约束）。

**算法**：Vyukov 有界骨架——容量 2 的幂，物理槽位 = `sequence % capacity`；满判定 `producer_sequence - min_consumer_sequence >= capacity`（与 9.5 公式一致）；发布经 Slot Sequence 的 acquire 读 / release 写（「数据先于状态可见」，同 9.3 WRITING→READY）；游标 CAS 仅用 relaxed 做竞争仲裁，正确性由 Slot Sequence 保证。

**生命周期**：

- **Init**：Region Owner 建链时原地初始化控制块与全部 Slot Sequence，最后写 magic（magic 落位前结构不得被使用）；
- **Attach**：映射方校验 magic、layout_version、elem ABI 后附着；校验失败拒绝附着并告警；
- 骨架不涉及元素回收：Payload 生命周期由 10.1 状态机与 Lease 管理；进程退出不「析构」骨架，异常后的恢复扫描见 12 章。

**职责边界**：

| 骨架保证 | 骨架不保证（由上层提供） |
|---|---|
| 有界 MPMC 预留仲裁、推进、回绕区分、满/空判定的并发正确性 | 消费语义：Broadcast 每订阅者一份（9.6）/ Work Queue 竞争认领（9.7） |
| Slot Sequence 发布序（9.3 内存序） | 进程崩溃恢复：Owner Epoch、Lease、ABORTED Tombstone（9.5、12） |
| 跨进程原子可见性（前提见下） | 阻塞等待与 QueueFullPolicy 丢弃策略（9.8） |

**跨进程前提**：单拷贝原子操作在同一映射区对所有共享进程正确（无锁原子，x86-64 基线由 V-12 Litmus 验证，AArch64 由 V-13 验证后启用）；各进程映射基址可不同，结构内仅有 Offset 语义字段。

**进程内实例化**：同一骨架算法可实例化为纯进程内队列（如 Bridge Encode/Batch Queue，16.1），此时无跨进程恢复需求、元素可为任意可移动类型——这是骨架的附带使用场景，不改变其 SHM 引用语义的设计定位。

验证登记见 26 章 V-26。

---

## 10. Publisher 设计

### 10.1 静态 Publisher API

```cpp
template <typename T>
class Publisher {
public:
    Result<MessageBuilder<T>> Allocate(Deadline deadline);
    Result<DeliveryReceipt> Publish(
        MessageBuilder<T>&& builder,
        DeliveryStage wait_for,
        Deadline deadline);
    Status Abort(MessageBuilder<T>&& builder);
};
```

### 10.2 动态 Publisher API

```cpp
class DynamicPublisher {
public:
    Result<DynamicMessageBuilder> Allocate(Deadline deadline);
    Result<DeliveryReceipt> Publish(
        DynamicMessageBuilder&& builder,
        DeliveryStage wait_for,
        Deadline deadline);
};
```

### 10.3 Publish 流程

1. 检查 Publisher Lease 和 Channel 状态；
2. 根据静态类型或 Descriptor 计算最大布局；
3. 分配主对象和必要子 Slab；
4. Builder 独占构建；
5. Validator 检查字段、容量和对象图；
6. 预留 Ring Slot；
7. 分配来源 Sequence；
8. 填写 Schema Identity、Handle、长度和时间；
9. Release Commit；
10. Builder 转为无效状态。

任何步骤失败都必须通过 Builder 的 RAII Abort Path 回收已分配对象图。

### 10.4 Publisher 身份

多 Publisher Topic 每条消息携带：

```text
node_id
publisher_id
publisher_epoch
source_sequence
observed_timestamp_ns
```

同一 Epoch 的 Sequence 单调递增。Publisher 重启生成新 Epoch，不从旧 Epoch 延续而伪装成同一实例。

### 10.5 Delivery Receipt

```cpp
enum class CompletionPolicy {
    kAll,
    kAny,
    kQuorum,
};

struct DeliveryRequirement {
    DeliveryStage stage;
    CompletionPolicy completion;
    uint32_t quorum;
    Deadline deadline;
};

enum class DeliveryTargetKind {
    kNode,
    kRecorder,
};

struct DeliveryTarget {
    DeliveryTargetKind kind;
    uint64_t id;
};

struct TargetDeliveryStatus {
    DeliveryTarget target;
    DeliveryStage reached_stage;
    Status status;
};

class DeliveryReceipt {
public:
    ReceiptId id() const;
    Result<std::vector<TargetDeliveryStatus>> Wait(Deadline deadline);
    void CancelWait();
};
```

Runtime 使用有界 Outstanding Receipt Table，以 `(publisher identity, source_sequence, receipt_id)` 关联 Bridge ACK 和 Storage ACK。规则：

- Topic Route Snapshot 在 Publish 时确定目标集合；
- `kAll/kAny/kQuorum` 只作用于该 Snapshot，路由后续变化不修改已有 Receipt；
- Receipt 返回逐目标状态，单个布尔值不能表达部分成功；
- `kRemoteAccepted` ACK 由远端 Bridge 接管消息后产生；
- Written/Durable ACK 由配置的 Recorder 返回，不能由 Bridge ACK 冒充；
- Receipt Table、每 Publisher Outstanding 数和 ACK 缓冲都有上限；耗尽时 Publish 返回 Resource Exhausted 或按 Deadline 等待；
- Deadline/`CancelWait()` 只停止调用者等待，不撤销已发布消息；
- Publisher 退出后 Receipt 进入 Orphan Cleanup，ACK 可以更新指标但不保留无界状态；
- 首版不提供跨多个 Recorder 的原子事务，Quorum 只表示独立目标达到指定 Stage。

---

## 11. Subscriber 与 BorrowedMessage

### 11.1 API

```cpp
template <typename T>
class Subscriber {
public:
    Result<BorrowedMessage<T>> TryPoll();
    Result<BorrowedMessage<T>> Poll(Deadline deadline);

    template <typename Callback>
    Status Poll(Callback&& callback, Deadline deadline);
};
```

### 11.2 Borrow 语义

```cpp
template <typename T>
class BorrowedMessage {
public:
    const T* operator->() const noexcept;
    const MessageMetadata& metadata() const noexcept;
    ~BorrowedMessage();

    BorrowedMessage(BorrowedMessage&&) noexcept;
    BorrowedMessage(const BorrowedMessage&) = delete;
};
```

- BorrowedMessage 只读；
- 不允许跨 Callback 或线程长期保存，除非 API 明确支持 Transfer；
- 析构默认 ACK，但业务错误与 ACK 语义应显式配置；
- Borrow 期间 Payload 不得回收；
- Subscriber 崩溃由 Lease 失效和恢复流程解除责任。

### 11.2.1 Transfer 与对象级引用 Pin（ShmSharedPtr）

需要跨 Callback、跨线程或跨 Channel 长持有单条消息时，通过显式 Transfer 将 Borrow 升级为对象级引用 Pin（ADR-0013）：

```cpp
template <typename T>
class ShmSharedPtr {           // 对象级引用 Pin 的进程内句柄
public:
    const T* get() const noexcept;
    const MessageMetadata& metadata() const noexcept;

    ShmSharedPtr(ShmSharedPtr&&) noexcept;
    ShmSharedPtr(const ShmSharedPtr&);   // 拷贝 = 同一进程内新增一个 Pin
    ~ShmSharedPtr();                     // 释放本进程持有的 Pin
};

// 唯一获取途径：从有效 Borrow 升级；失败返回 kResourceExhausted（Pin 配额满）
Result<ShmSharedPtr<T>> BorrowedMessage<T>::Transfer();
```

语义约束：

- **回收充分条件**：Payload 可 Reclaim 当且仅当「已 Retired ∧ 无有效 Borrow ∧ 无存活 Pin」（8.4）；Pin 只延迟已 Retire 对象的内存复用，不阻止 Retire、不阻塞 Channel 推进；
- **计数器位置**：位于 Slab Header/Sidecar，不内嵌 Base Slot（ADR-0003）；每对象一个 32 位 Pin 计数；
- **崩溃安全**：Pin 的释放不依赖析构——获取 Pin 时持有者在 Channel 侧登记 `(process_identity, object_handle, pin_epoch)`，进程被 Kill 由 Lease 失效检测，Recovery 扫描清除该进程全部 Pin 份额（12 章）；析构只是正常路径的及时释放；
- **ABA 防护**：获取 Pin 必须校验 Handle Generation；已 Reclaim 对象的旧 Handle 不得再 Pin 成功（Generation 已递增）；
- **有界配额**：per-object Pin 上限（默认 64）与 per-process Pin 上限（默认 4096）双重限制，超出返回 `kResourceExhausted` 并导出 `mino_pin_acquire_denied_total`，不得无限累积；配额占用纳入 20.4 节点资源预算；
- **只读**：与 Borrow 相同，Pin 持有期间对象不可变；
- **监控**：`mino_pin_outstanding`、`mino_pin_oldest_age_seconds` 必须导出；超过配置阈值的「老 Pin」触发诊断告警（疑似业务泄漏）。

Transfer 后原 `BorrowedMessage` 立即等效析构（ACK 责任一并解除）；Channel 层视为该 Subscriber 已完成本条消息，后续生命周期由 Pin 独立承担。

Runtime Core 禁止异常跨 API/线程边界传播。若构建配置启用 C++ Exception，Poll Wrapper 必须捕获 Callback 异常、转换为 Status 并执行配置的 ACK 策略：

- Broadcast 首版 Callback 返回即 ACK，不提供自动重试；业务失败通过新消息或业务补偿处理；
- Work Queue 的 NACK、最大 Delivery Attempt 和 Dead Letter Topic 属于独立扩展，首版不开放 Work Queue 可靠重试承诺；
- `BorrowedMessage` 析构不得抛异常；
- ACK 失败进入诊断和恢复流程，不能从析构函数抛出。

### 11.3 Poll 流程

1. 读取 Subscriber Cursor；
2. Acquire 检查 Slot State 和 Sequence；
3. 校验 Schema、Payload Length、Handle 和 Generation；
4. 进入安全读取区；
5. 创建 BorrowedMessage；
6. 执行 Callback；
7. ACK（清除自身 ACK 责任）；
8. 更新 Cursor；
9. 退出读取区。

Payload 的 Retire/Reclaim **不在单个 Subscriber 的 Poll 路径上执行**——Broadcast 中单个 Subscriber 无法判断自己是否为最后 Reader。统一由 Channel 侧在以下时机检查「所有 Snapshot 内订阅者均已完成 ACK」后触发：Subscriber 推进 Cursor 后、Lease 剔除完成后、或 Channel 维护线程周期扫描时。

### 11.4 Recorder Subscriber 特例

Recorder 不能等待磁盘时长期持有 BorrowedMessage。其流程必须是：

```text
Borrow → Validate → Canonical Encode → Copy to Recorder Pool → ACK
```

复制失败时根据录制策略阻塞、丢弃并记录 Gap，或停止录制。

---

## 12. 所有权、租约与回收

### 12.1 Payload 状态

```text
FREE → ALLOCATED → BUILDING → PUBLISHED → RETIRED → FREE
                      │
                      └────────→ ABORTING → FREE
```

状态转换必须标明唯一 Owner。业务线程不得直接跳过状态转换操作 Bitmap。

### 12.2 Subscriber Lease

```cpp
struct SubscriberLease {
    SubscriberId subscriber_id;
    uint32_t subscriber_generation;  // 注册时分配；同一 subscriber_id 复用时单调递增
    ProcessIdentity owner;
    uint64_t lease_epoch;            // Coordinator 侧租约代数，与 subscriber_generation 独立
    std::atomic<uint64_t> heartbeat_ns;
    std::atomic<uint32_t> state;
};
```

`subscriber_generation` 与 `lease_epoch` 是两个独立字段：前者标识「subscriber_id 的第几次占用」，用于 Broadcast ACK 责任绑定（9.6）；后者标识 Coordinator 授予租约的代数，用于租约失效判定。Coordinator 判定失效时需要：

1. 再次确认 Process Identity；
2. CAS Lease State 为 EVICTING；
3. 阻止新 Borrow；
4. 清理该 Subscriber 的 ACK 责任；
5. 清除该进程持有的全部对象级引用 Pin（11.2.1）；
6. 推进可回收边界；
7. 标记 EVICTED。

### 12.3 Publisher 崩溃

WRITING Slot 和 BUILDING Slab 保存 Owner Identity/Lease Reference。恢复者只能在 Owner 确认失效后接管，避免暂停进程被误判后双重回收。

### 12.4 ABA

- Ring 使用 64 位逻辑 Sequence；
- Slab 使用 Generation；
- Process/Publisher/Subscriber 使用 Epoch；
- 32 位 Generation 回绕前必须 Drain/迁移，禁止回零继续复用；
- Region ID 永不复用，Attach 另行校验 Region UUID/Epoch；
- Generation、Sequence、Region Identity 和 Lease Epoch 共同防护 ABA，不能依赖单一字段。

---

## 13. Schema Compiler 与 Runtime

### 13.1 模块拆分

```text
Lexer → Parser → AST → Semantic Validator
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       Schema Descriptor       Compatibility
              │
              ▼
         Layout Planner
          /          \
 Static CodeGen    Dynamic Runtime Plan
```

### 13.2 IDL Field Identity

生产 Schema 的字段必须显式声明稳定 Field ID：

```proto
message SensorFrame {
    uint32 frame_id = 1;
    string device_name = 2 [max_bytes = 64];
    vector<Point3D> points = 3 [max_capacity = 100];
    reserved 4, 10 to 15;
}
```

规则：

- Field ID 取值范围为 1～536870911，保留实现内部区间由 Wire ADR 固定；
- 生产 Schema 禁止自动分配 Field ID；测试临时 Schema 如允许自动分配也不得持久化或发布；
- 已发布 Field ID 永不复用，删除字段必须保留 Reserved 声明；
- Oneof/Union 的成员仍使用全消息唯一 Field ID；
- Field Name 可以兼容性重命名，但 ID、类型和语义约束决定 Wire Identity；
- Schema 禁止直接或间接递归类型，Compiler 对 Import Graph 和 Type Graph 做环检测；
- Canonical Digest 包含目标类型及其传递依赖类型闭包，Import 文件路径本身不参与 Digest；
- 依赖闭包按完整类型名和 Digest 确定性排序。

### 13.3 Schema Identity

热路径使用 64 位 Short ID，但完整身份使用 256 位 Canonical Digest：

```cpp
struct SchemaIdentity {
    uint64_t short_id;
    std::array<std::byte, 32> canonical_digest;
    uint32_t schema_version;
    uint32_t layout_version;
};
```

```cpp
struct SchemaIdentity {
    uint64_t short_id;
    std::array<std::byte, 32> canonical_digest;
    uint32_t schema_version;   // major << 16 | minor
    uint32_t layout_version;
};
```

- `canonical_digest` 由带算法版本的 Canonical Schema 文本计算，是 Schema 的最终身份；
- `schema_version` 编码为 `major << 16 | minor`：breaking 变更（见 13.2 兼容性矩阵）必须提升 major；兼容变更提升 minor。该值由 IDL 显式声明（`option schema_version = "2.1";`）且**不参与** Canonical Digest——Digest 只反映 Schema 内容本身；
- Digest 相同而 `schema_version` 不同：合法（同一内容被赋予不同版本标签），Registry 以 Digest 去重；
- Digest 不同而 `schema_version` 相同：拒绝注册并告警（版本号未随内容演进）；
- `layout_version` 标识 SHM 对象布局规则（13.6 的 Layout Plan）版本，与 schema_version 独立演进；
- Short ID 取 Digest 的固定 64 位，仅用于索引和已协商连接；
- Registry 注册、Schema Store 持久化和冲突判断必须比较完整 Digest；
- 检测到 Short ID 碰撞时，连接和存储记录升级为完整 Digest 引用，不能覆盖已有 Schema。

### 13.3.1 Canonicalization v1

Canonical Digest 的输入是按下述规则生成的 Canonical Schema 文本（UTF-8）：

1. **头部前缀**：`"mino-canonical-v1\0"`（含算法版本，一并参与散列）；
2. **字段排序**：每个 message/struct 的字段按 Field ID 升序；字段序列化形式为 `field_id:type_ref:constraint_set:default`；
3. **Annotation 规范化**：Annotation 按名称字典序排序，数值字面量规范化为最短十进制（`1e-1` → `0.1`），字符串字面量保留原始字节；浮点默认值取 IEEE 位模式的十六进制；
4. **类型名**：类型名（含 package 全名）参与 Digest。因此**类型重命名会改变 Digest**，视为新 Schema；Field Name 不参与 Digest（允许兼容重命名）；
5. **依赖闭包**：传递依赖按完整类型名字典序排列，每个依赖以其自身 Digest 引用（不递归嵌入完整 Descriptor），菱形依赖去重后只出现一次；
6. **空白与注释**：Canonical 文本为规范化重建结果，不含注释与原始空白；字符串默认值内部的字节原样保留；
7. **不参与 Digest 的要素**：Field Name、`schema_version` option、Import 路径、注释、源码排版。

Golden Vector 依据本规范生成（规范决定测试向量，不是测试向量决定规范）；任何 Canonicalization 规则变更必须提升头部版本并重建全部向量。

### 13.3.2 Compatibility 判定

`SchemaRegistry::CheckCompatibility(from, to)` 的输入为两个 Digest，实现从 Registry 解析出 Descriptor 后按字段级规则判定：

```cpp
enum class Compatibility {
    kIdentical,             // Digest 相同
    kWireCompatible,        // 双向可读写（新增/删除 Optional 字段、Field 重命名）
    kReadCompatible,        // to 可读 from 的数据（单向）
    kWriteCompatible,       // from 可读 to 的数据（单向）
    kRequiresTranslation,   // 存在已注册的转换规则
    kIncompatible,          // 拒绝
};
```

判定矩阵（基础集，完整集由 Compatibility 模块单测固定）：

| 变更 | 判定 |
|---|---|
| 仅新增 Optional 字段 | kWireCompatible |
| 仅删除 Optional 字段（ID Reserved） | kWireCompatible |
| Field Name 重命名 | kWireCompatible |
| 整型提升/类型改变/Wire Type 改变 | kIncompatible（需 major 提升） |
| 收紧约束 | kWriteCompatible（旧 Writer → 新 Reader 安全，反向拒绝） |
| 放宽约束 | kReadCompatible（新 Writer → 旧 Reader 安全，反向可能拒绝超长数据） |

### 13.4 Compiler API

```cpp
struct CompileOptions {
    uint32_t max_input_bytes = 1u << 20;   // 1 MiB
    uint32_t max_fields = 1024;
    uint32_t max_nesting_depth = 32;
    uint64_t max_total_capacity = 64u << 20; // 64 MiB
};
```

class SchemaCompiler {
public:
    Result<CompiledSchema> Compile(
        std::string_view idl,
        const CompileOptions& options);
};
```

Runtime Compiler 不链接 C++ Source CodeGen。`minoc` 在 Compiler Core 上额外链接 CodeGen Target。

### 13.5 Schema Registry API

```cpp
class SchemaRegistry {
public:
    Result<SchemaHandle> RegisterIdl(std::string_view idl);
    Result<SchemaHandle> RegisterDescriptor(ByteView descriptor);
    Result<SchemaHandle> Find(SchemaIdentity identity) const;
    // 输入的两个 Digest 会先解析为 Descriptor，再按 13.3.2 的字段级规则判定
    Result<Compatibility> CheckCompatibility(
        std::array<std::byte, 32> from_digest,
        std::array<std::byte, 32> to_digest) const;
};
```

Registry 内对象发布后不可变，通过共享所有权或 RCU 风格 Cache 提供并发读取。Schema 编译不能持有 Registry 全局写锁执行；应先在外部编译，再以短临界区去重提交。

### 13.6 Dynamic SHM Layout

首版静态生成类型和 Dynamic View 共享同一 Layout Plan：

```text
DynamicObject
├── ObjectHeader
│   ├── layout_version
│   ├── schema_short_id
│   ├── object_size
│   ├── field_count
│   └── presence_bitmap_words
├── Presence Bitmap
├── Fixed Field Area
│   ├── scalar fields at descriptor offsets
│   └── variable fields as Handle + Length + Capacity
├── Child Slabs
│   ├── string/bytes data
│   ├── contiguous vector elements
│   └── nested variable objects
└── Optional UnknownFieldSet Handle
```

规则：

- Optional 字段由 Presence Bitmap 表示，不能用零值猜测缺失；
- 字段排布：按 Field ID 升序，字段间按自然对齐（标量对齐到其宽度）插入 Padding；Presence Bitmap 按 8 字节对齐；
- `ObjectHeader.object_size` 指 Fixed Field Area 的字节数（不含 Child Slabs）；`field_count` 为 Descriptor 中字段总数；`presence_bitmap_words` 为 64 位字数量；
- `string` 必须是合法 UTF-8，任意二进制使用 `bytes`（复用 StringMeta 布局，无 UTF-8 约束）；
- 固定大小嵌套对象可以内联，动态嵌套对象使用 Handle + Size Meta；
- Vector 元素连续存储，元素 Layout 由 Descriptor 固定；`element_size` 为冗余快查字段，供 Validator 不查 Descriptor 即可做边界检查；
- 空容器使用 Length=0，Handle 可以为空；Handle 为空但 Length>0 必须被 Validator 拒绝；
- Builder 扩容动态字段时重新分配新 Child Slab、拷贝、更新 Meta，旧 Child Slab 登记到 Allocation Journal，Commit 成功后回收；
- 首版禁止对象环和可回收子对象共享：Builder 在设置 Child Handle 时检查该 Handle 未登记在当前事务的其他字段上，重复则拒绝；
- Object Graph Walker 必须能够确定性校验和回收全部 Child Slab；
- Unknown Field Set 保存原始 Canonical Field Bytes，受每消息字节数和字段数上限约束（默认 64 KiB / 64 字段）；
- Bridge 无需转换 Schema 时优先 Wire Passthrough，避免不必要的 Decode/Re-encode；
- 必须版本转换时，转换器保留未消费 Unknown Field，除非策略明确允许丢弃并产生指标。

Digest 的唯一计算点是 Runtime Compiler Library：静态 CodeGen 产物中的 Digest 由 `minoc` 在构建期调用同一 Library 计算并嵌入 Descriptor；构建产物（`.generated.h/.cc` + Descriptor）随部署注册进 Registry，注册时机为应用部署/首次启动时（Registry 幂等去重）。

### 13.7 Dynamic View

Dynamic View 保存：

- 已验证 Descriptor Handle；
- Payload Base/Handle；
- Layout Plan；
- Borrow Lifetime Token。

字段访问先通过 `FieldHandle` 定位。字符串名称查找只用于低频初始化。

### 13.8 Canonical Wire Format v1

首版基线采用自描述 Tagged Wire Format，并在实现前通过 ADR 和测试向量冻结：

- 固定小端字节序；
- Tag 打包：`tag = (field_id << 3) | wire_type`，tag 本身以 Varint 编码；Field ID 上限 2^29-1；
- Wire Type：`VARINT=0`、`I64=1`、`LEN=2`、`I32=5`（LEN 为 length-delimited：Varint 长度前缀 + 字节串）；未知 Wire Type 的字段按类型自描述长度跳过，无法确定长度的视为损坏；
- Varint 为标准 LEB128，Encoder 必须产生最短形式；Decoder 必须拒绝超过 10 字节或非最短形式的 Varint；
- 字段按 Field ID 升序输出；未置位的 Optional 字段整体省略（不编码零长度占位）；重复字段保持元素顺序；
- Signed Integer 使用 ZigZag Varint，Unsigned Integer 使用 Varint；
- `fixed32/fixed64/float/double` 使用固定宽度 IEEE/整数位模式；
- Float 保留 `-0` 和 NaN Payload 位模式，不做隐式数值归一化；Wire 确定性中的 Float 逻辑相等定义为位模式相等；Decoder 原样透传 NaN 位模式，转换/回放路径必须保证位模式不丢失；
- `string` 编码 UTF-8（Decoder 必须校验），`bytes` 不解释内容；
- Vector 编码为 LEN：Varint 元素数量 + 元素顺序排列（标量元素打包，message 元素逐个 length-prefix）；嵌套 message 字段编码为 LEN：长度前缀 + 递归字段序列；
- Decoder 可以跳过未知字段；需要无损代理时将 Unknown Field Set 作为有界附属数据保留；
- Map 不进入 v1，使用显式 `vector<Entry>`；
- Union/Oneof 使用 Discriminator + 单个激活字段：Discriminator 为独立的保留 Field ID 0 的 VARINT 字段（值为激活成员的 Field ID），未激活的 Oneof 不编码任何成员；
- 所有 Length、元素数和嵌套深度在分配前检查：默认单帧 ≤ 16 MiB、Vector 元素数 ≤ 其 max_capacity、嵌套深度 ≤ 32；
- 相同 Schema 和逻辑值必须产生确定性字节序列。

Schema Descriptor 自身也使用独立版本的 Canonical Wire 编码，不能依赖 C++ ABI。

### 13.9 Schema 分发

Bridge 建连时交换已知 Schema ID 集合或按需请求 Descriptor：

```text
Frame references unknown connection_schema_ref
        │
        ├─ Pause/buffer within limit
        ├─ Request full Digest + Descriptor
        ├─ Authenticate + Validate + Persist
        └─ Resume Decode
```

- `connection_schema_ref` 由接收侧（decode 方）分配，Announcement 建立 `ref → full digest` 映射；Ref 空间 32 位、连接内单调不复用，连接重建即失效；
- 未知 Schema 的缓冲必须有界：默认每连接 ≤ 4 MiB / 1024 帧；溢出时断开连接并导出指标（可靠 Topic 对端会重连恢复）；
- 未知 Schema 请求限频：默认每连接 ≤ 16 个/秒，超限拒绝该次 Announcement；对同一 Digest 的并发请求必须去重合并；
- 请求 Descriptor 期间收到的后续同 Ref 帧进入同一有界缓冲，不重复发请求；
- 数据帧不能携带任意未认证 IDL 并触发无限编译。

### 13.10 Registry 一致性与缓存

首版采用单 Authoritative Registry/Coordinator + 节点本地只读缓存，不提供自动多主：

- Schema 注册和 Topic-Schema Binding 只能由 Authoritative Registry 分配配置版本；
- Registry 不可用时，节点可以继续使用已验证且未撤销的缓存 Schema（撤销状态以最后一次成功联系 Registry 时为准；重连后必须执行对账：上报本地缓存 Digest 集合，Registry 返回其间新增的撤销与绑定变更）；
- Registry 不可用时禁止创建新 Schema、新 Topic Binding 或执行不兼容升级；此期间 Bridge 新连接的对端若 Announce 本地缓存中不存在的 Digest，拒绝该 Schema 的消息并导出指标；
- 同一 Topic 允许的 Schema Version 集合必须显式配置；
- Descriptor 发布后不可变，撤销只阻止新绑定，不删除历史录制需要的 Descriptor；
- Cache 使用 Digest 校验和有界 LRU，但被 Topic、Borrow 或 Replay Pin 的 Descriptor 不得淘汰；
- Region ID、Topic ID 等不复用标识通过 Durable High-water Mark 分配，必须先持久化新高水位再对外发布 ID；
- 自动主备和分布式共识留待后续阶段，首版部署必须避免双 Authoritative Registry。

---

## 14. Registry 与 Coordinator

### 14.1 元数据

Registry 管理：

- Node Identity 和健康状态；
- Topic ID、名称和配置版本；
- Channel Type 和 Region Location；
- Publisher/Subscriber 注册；
- Schema Identity；
- Bridge Route；
- Recorder Policy；
- 永不复用的 Region ID、Topic ID 及其持久分配高水位；
- Lease 和权限。

### 14.2 Topic Metadata

```cpp
enum class ChannelKind : uint8_t {
    kSpsc,
    kMpsc,
    kBroadcast,
    kWorkQueue,
};

// Pub/Sub 路由策略：决定远端目标节点集合如何产生（语义见 15.2）
enum class RoutePolicy : uint8_t {
    kDiscovery = 0,  // 自动发现：按订阅注册动态计算目标节点集合（默认）
    kStatic = 1,     // 显式配置：固定发布到 static_routes 指定的节点
};

// 静态路由条目：目标节点粒度，不指定到具体进程；
// 到达目标节点后按该节点本地订阅情况投递
struct StaticRouteEntry {
    NodeId target_node;
    // 该路由可选的传输约束（如强制 Fabric 通道）；空值表示由策略引擎选择
    std::optional<TransportKind> preferred_transport;
};

enum class Reliability : uint8_t {
    kBestEffort,
    kReliableOrdered,   // At-least-once + 接收端去重 + 来源内有序
};

struct DeliveryPolicy {
    Reliability reliability;
    bool allow_drop;        // 与 QueueFullPolicy 联动，见 9.8 与 20.3
};

enum class RecordBackpressureTopology : uint8_t {
    kStrongConsistent,  // 强一致录制：Recorder 是主 Broadcast 强制 Subscriber
    kIsolated,          // 隔离录制：Fanout 复制到独立 Recorder Channel
    kBestEffort,        // 尽力录制：落后时写 Gap 并推进
};

struct TopicMetadata {
    TopicId topic_id;
    std::string name;
    ChannelKind channel_kind;
    DeliveryPolicy delivery;
    QueueFullPolicy queue_full_policy;
    SchemaIdentity schema;
    RoutePolicy route_policy;              // Pub/Sub 路由策略，创建时指定，默认 kDiscovery
    std::vector<StaticRouteEntry> static_routes;  // 仅 kStatic 时非空，见 15.2
    uint64_t route_set_version;            // static_routes 的版本，随 14.3 配置更新递增
    uint32_t capacity;
    uint32_t max_publishers;
    uint32_t max_subscribers;
    uint32_t partition_count;                 // 录制 Partition 数，默认 1
    RecordBackpressureTopology record_topology; // 录制背压拓扑，创建时必须显式指定
    uint64_t config_version;                  // 覆盖本结构全量字段
};
```

动态 Topic 如果允许多个 Schema Version，必须显式定义兼容集合和订阅者协商规则，不能让同一 Topic 无约束混入任意类型。

### 14.3 配置更新

使用配置版本和两阶段切换：

1. Validate 新配置；
2. 生成新不可变 Snapshot；
3. 原子发布 Snapshot；
4. 新操作使用新版本；
5. 旧借用和旧连接完成后回收旧 Snapshot。

涉及 Region Layout、Ring Capacity 或 Partition Count 的变更不能原位热改，必须创建新资源并迁移。

### 14.4 Topic 生命周期

```text
CREATING → ACTIVE → DRAINING → RETIRED → DELETED
```

- Topic ID 首版永久不复用；
- CREATING 完成 Schema、Region、Channel 和 ACL 校验后才能 ACTIVE；
- DRAINING 拒绝新 Publisher，允许已有消息和 Borrow 完成；
- RETIRED 不再传输实时消息，但历史 Schema 和 Recording Metadata 保留；
- 删除底层资源前必须确认 Publisher、Subscriber、Bridge、Recorder 和 Replay Pin 全部释放；
- 重命名只修改显示名称，Topic ID 和历史记录不变；
- Ring 扩容和 Schema 不兼容升级通过新 Channel/Region + Drain/Cutover，不原位改 ABI。

---

## 15. Transport Switcher

### 15.1 Route 输入

```cpp
struct RouteRequest {
    TopicId topic_id;
    NodeId target_node;
    uint32_t payload_size;
    DeliveryPolicy delivery;   // 含 allow_drop
    uint8_t priority;
};
```

目标节点集合的来源由 Topic 的 `RoutePolicy`（14.2）决定：

- **自动发现（kDiscovery，默认）**：订阅驱动路由。Subscriber 向 Node Registry 注册后，Registry 汇总出该 Topic 的**订阅节点集合**（有活跃 Subscriber 的远端节点）；Transport Switcher 按此集合扇出。新 Subscriber 从注册切点之后的消息开始接收，不追溯历史（与 9.6 Broadcast `join_sequence` 一致）；某节点最后一个 Subscriber 注销后，该节点从集合中移除，Bridge 停止向其发送——无订阅者的节点不产生网络流量；
- **显式配置（kStatic）**：配置驱动路由。目标节点集合就是 Topic 元数据中的 `static_routes`（携带 `route_set_version`），发布即按配置扇出，**与订阅发现无关**。到达目标节点后按该节点本地订阅情况投递：有 Subscriber 则正常交付，无 Subscriber 则消息在该节点按 Topic 策略丢弃或进入有限缓存等待订阅者（默认丢弃并导出 `mino_route_static_undeliverable_total` 指标，不因无订阅者而静默成功）。

典型 kStatic 场景：录制/回放节点固定接收、跨 Trust Domain 的确定性通道（如 IPCF 固定域间链路）、启动期拓扑即确定的车载网络。kStatic 目标节点必须存在于 Topic ACL 允许集合内（20.3 校验），否则配置被拒绝。

### 15.2 选择流程

1. 查询 Topic 和目标 Node；
2. 校验 ACL 和 Schema Compatibility；
3. 目标本地优先选择 SHM；
4. 目标远端选择健康 Bridge 或 Fabric Driver 实例（网络与 Fabric 统一经 15.3 接口路由）；
5. 检查 Driver 能力和 Payload 上限；
6. 返回不可变 Route Handle；
7. Route 失效时刷新，不在每条消息执行重型发现逻辑。

路由集合刷新时机：kDiscovery 由订阅注册/注销/Lease 失效触发（Registry 推送变更，带订阅集合版本号）；kStatic 仅在 `route_set_version` 变化时重建（14.3 两阶段切换），运行期与订阅者增减完全解耦。Route Handle 缓存以版本号为失效依据，不做逐消息查询。


### 15.3 Driver 接口

```cpp
// 传输通道类型：描述物理传输形态，由部署配置与 Topic 策略（arch §9.3）决定
enum class TransportKind : uint8_t {
    kNetwork = 0,      // IP 网络：TCP / UDP 等
    kRdma = 1,         // RDMA：兼容接口（IB Verbs / RoCE）或原生协议
    kSharedFabric = 2  // 跨 Trust Domain 的共享内存/信箱 Fabric：
                       // IPCF（NXP 车载 SoC A核↔M核）、PCIe NTB、CXL 共享内存池等
};

enum class TransportReliability : uint8_t {
    kUnreliable = 0,   // 可能丢包乱序（如 UDP）
    kOrderedLossy = 1, // 保序但可丢包（如裁断重传的 UDP）
    kReliable = 2      // 可靠有序消息流/字节流（如 TCP、多数 IPCF Channel）
};

// Driver 能力描述：策略引擎路由选择的输入之一（arch §9.2）
struct TransportCapabilities {
    TransportKind kind;
    TransportReliability reliability;
    uint32_t max_frame_size;        // 单帧上限；0 = 无限制（流式）
    uint64_t max_reassembly_bytes;  // 对端重组缓冲上限；超出走 16.6 大消息路径
    bool supports_zero_copy_window; // 支持共享窗口读写（RDMA Buffer、Fabric 窗口）
    bool supports_multicast;        // 原生多播（如 UDP multicast），否则 Driver 自行扇出
    bool supports_remote_write;     // RDMA 式单边写
};

// 端点描述符：同时容纳 IP 端点与非网络 Fabric 端点；
// 序列化进 Node Registry 节点元数据与 connection_info，对端按 kind 解析
struct EndpointDescriptor {
    TransportKind kind;
    uint8_t reserved[3];
    union {
        struct {                    // kNetwork；kRdma 兼容接口亦可复用
            uint8_t ip[16];         // IPv4/IPv6
            uint16_t port;
            uint16_t reserved2;
        } network;
        struct {                    // kSharedFabric：由具体 Driver 实现解释
            uint32_t fabric_domain; // Fabric 域 ID（如 IPCF instance、NTB partition）
            uint32_t channel_id;    // 通道/队列 ID（如 IPCF channel、NTB 队列）
            uint8_t opaque[24];     // 实现私有（窗口物理地址/BAR 偏移、窗口尺寸等）
        } fabric;
    };
    // kRdma 原生协议的端点格式（如 IB GID/LID）由实现自描述，经 Registry 元数据协商
};
static_assert(sizeof(EndpointDescriptor) <= 40, "Registry 元数据固定槽位约束");

// 发送出口：载荷为已 Canonical 编码的字节流（v1 大端，帧格式见 16.2）
struct OutboundMessage {
    EndpointDescriptor to;
    std::span<const std::byte> payload;
    DeliveryStage target_stage;     // 期望推进到的阶段，默认 kRemoteAccepted（见 2.2）
};

class TransportDriver {
public:
    virtual Status Send(const OutboundMessage&) = 0;
    virtual HealthState health() const = 0;
    virtual TransportCapabilities capabilities() const = 0;
};

// 可选扩展：以共享窗口为传输原语的 Fabric（IPCF 共享内存通道、PCIe NTB 窗口、
// CXL 共享内存池）与 RDMA Buffer 可额外实现本接口。
// Delivery 直接在对端可见的共享窗口中写入 Canonical 载荷，避免“本地序列化缓冲 →
// 驱动内部缓冲 → 对端缓冲”的二次拷贝；窗口按固定尺寸槽位管理，容量有界。
class FabricWindowDriver : public TransportDriver {
public:
    struct Window {
        std::span<std::byte> write_view;  // 本端可写视图
        uint64_t window_id;               // 提交时回传
    };
    // 申请窗口；无空窗时返回 kBusy，由 Topic 策略决定丢弃/等待
    virtual Result AcquireWindow(size_t payload_bytes, Window& out) = 0;
    // 提交窗口：Driver 负责通知对端（doorbell/中断），并保证对端可见性
    // （非缓存一致域间的 cache maintenance/内存屏障属 Driver 内部职责）
    virtual Result CommitWindow(const EndpointDescriptor& to,
                                const Window& w) = 0;
};
```

热路径实际实现可以使用静态分派或函数表，避免无必要虚调用；接口表达 Driver 边界。

**非网络 Fabric 的语义边界（必须遵守，详见 16.7）**：无论底层是 IP 网络、RDMA 还是 IPCF/NTB/CXL 类共享 Fabric，跨 Trust Domain 传输的语义不变——只传 Canonical Wire 载荷，不得传本机 SHM Offset；可靠性与去重统一复用 16.5；DeliveryStage 语义不变。Fabric 只是 Driver 的一种实现形态，不改变传输协议语义。

---

## 16. Bridge 与网络协议

### 16.1 线程模型

```text
Local Subscriber Threads
          │
          ▼
 Encode/Batch Queue
          │
          ▼
 Network I/O Thread
          │
════════ Network ════════
          │
          ▼
 Decode/Validate Workers
          │
          ▼
 Remote Publisher
```

Queue 必须有界。可靠 Topic 与 Best-effort Topic 应使用独立队列或优先级，防止大流量低优先级消息阻塞控制消息。

### 16.2 Frame Header

v1 逻辑字段与宽度：

```text
magic                   4 B
protocol_version        2 B
flags                   2 B  // 位分配见下
header_length           4 B  // 帧头总字节数，含本字段；可选字段存在与否由 flags 指示
topic_id                4 B
msg_type                4 B  // CodeGen 由 canonical_digest 低 32 位派生，接收侧与 connection_schema_ref 交叉校验
connection_schema_ref   4 B  // 连接级 Schema 引用，由接收侧分配，见 13.9
schema_version          4 B  // major << 16 | minor
layout_version          4 B
source_node_id          8 B
source_publisher_id     8 B
source_publisher_epoch  8 B
sequence_num            8 B  // 来源内单调，与上述四元组共同构成去重键
timestamp_ns            8 B  // 发布端 Wall Clock（经时钟同步，质量见 21.5.6）
payload_length          4 B
header_crc              4 B
payload_crc / AEAD tag  4 B  // 可选，由 flags 指示
```

`flags` 位分配：bit0 = PAYLOAD_CRC_PRESENT，bit1 = AEAD_PRESENT，bit2 = COMPRESSED，bit3 = CONTROL_FRAME，bit4 = PERF_TRACE_SAMPLED（采样消息在帧头后紧跟 PerfTraceContext，见 21.5.5；接收校验在第 4 步后解析），bit5~15 保留必须为 0。

实际 Wire Header 使用显式 Encoder/Decoder，不直接发送 C++ Struct。`connection_schema_ref` 由握手中的 Schema Announcement 映射到完整 Canonical Digest；连接重建后映射可以变化，不能持久化为全局身份。

### 16.3 接收校验顺序

1. 校验最小 Header；
2. 校验 Magic 和 Protocol Version；
3. 校验 Header Length；
4. 校验 Header CRC/AEAD；
5. 校验 Topic 和 ACL；
6. 校验 Payload Length 上限；
7. 解析 Schema Identity；
8. 查找并验证 Descriptor；
9. 分配目标 Slab；
10. Decode；
11. Validate Object；
12. 发布到本地 Channel。

任何分配必须发生在长度上限和 Schema 容量检查之后。

### 16.4 TCP 首版

- 长连接；
- Length-prefixed Frame；
- 心跳和空闲超时；
- 有界发送/接收 Buffer；
- Partial Read/Write 状态机；
- 断线后按 Topic 策略丢弃、有限缓存或失败；
- 不默认承诺断线期间无限缓存。

### 16.5 可靠传输、重连与去重

可靠 Topic 使用消息级 Sequence 和累计 ACK：

- Bridge Send Queue 在收到远端 Accepted ACK 前保留 Frame；
- ACK 只表示远端 Bridge 完成校验并接管消息，不表示业务消费或磁盘 Durable；
- 一条 Bridge 连接汇聚多个来源，重连握手必须交换**逐来源映射表**：`(node_id, publisher_id, publisher_epoch) → last_accepted_sequence`；
- **Session Epoch**：每条连接在握手时由双方各自生成 64 位随机值并交换；接收侧将 `(对端 Session Epoch, 来源身份)` 与去重状态关联，Epoch 变化即旧去重状态失效。Session Epoch 不持久化；
- 发送方从有限重传窗口恢复，窗口外按 Topic 策略失败或报告 Gap；
- 接收方使用 `(node_id, publisher_id, publisher_epoch, source_sequence)` 去重；
- 重复 Frame 返回相同 ACK，但不重复发布；
- ACK、重传窗口和断线缓存均有字节及时间上限；
- TCP `write()` 成功不能作为 Remote Accepted。

需要远端 Durable 的调用必须等待独立 Storage Receipt，不能复用 Bridge Accepted ACK。首版提供 At-least-once 网络重试 + 接收端去重，不宣称分布式 Exactly-once。

去重缓存按 Source Identity 分片，保存 `highest_contiguous_sequence + bounded_gap_bitmap`：

- 窗口同时受最大 Sequence 距离、字节数和时间限制；
- 窗口内重复 Frame 幂等返回历史 Accepted ACK，不重复发布；
- 超出窗口的旧 Frame 返回 **NACK_WITH_HIGHEST**（携带当前 `highest_contiguous_sequence`）并报告 `mino_bridge_dedup_window_miss_total`：发送方收到后将被拒 Sequence 之前的消息视为已送达并推进，**不得无限重试被拒 Frame**；窗口内正常 Frame 的重试不受限制；
- Receiver 重启后首版不承诺保留纯内存去重状态。重连时若双方 Session Epoch 表明接收侧丢失了去重状态，接收侧将握手返回的 `last_accepted_sequence` 之前的所有重传视为新消息接收——此窗口内 At-least-once 允许重复到达远端 Subscriber，同时向上游返回 `kDegraded` 状态并导出指标；
- 需要跨 Receiver 重启去重时配置持久 Dedup Store，该能力不属于首版默认路径；
- Dedup Cache 淘汰、Gap 和 Epoch 切换都必须导出指标。

### 16.6 大消息

- 每个 Driver 定义 `max_frame_size` 和 `max_reassembly_bytes`；
- TCP 首版允许 Length-prefixed 大 Frame，但必须流式编码/解码，避免额外完整拷贝；
- 超过普通 Slab 上限时使用 Large Object Pool；
- UDP 驱动必须另行定义 Fragment ID、分片数量、每连接重组配额和超时，未完成前不启用大消息 UDP；
- RDMA Buffer 注册、Pin 和回收使用独立所有权协议；
- Fabric 窗口路径：实现 `FabricWindowDriver` 的传输可将对端可见的共享窗口作为大消息载体，窗口容量及单窗上限记入 `TransportCapabilities`，超出窗口总量时按 Topic 策略丢弃或等待，不得无限阻塞；
- 控制帧和小型高优先级消息使用独立 Queue，避免大消息造成 Head-of-line Blocking。

### 16.7 非网络 Fabric Driver（IPCF / PCIe NTB / CXL 等）

车载 SoC 跨核（A 核 Linux ↔ M 核 RTOS）、PCIe NTB、CXL 共享内存池等场景，跨 Trust Domain 传输不经过 IP 网络，而以共享内存窗口/信箱为原语。这类传输作为 `TransportDriver` 的一种实现形态接入（`TransportKind::kSharedFabric`，接口见 15.3），不改变传输协议语义。语义边界：

1. **仍走 Canonical Wire**：跨 Trust Domain 传输的载荷必须是 Canonical 编码字节流，帧格式复用 16.2（帧头、flags、`connection_schema_ref` 映射均不变）。即使两端物理上共享内存，也**不得**直接传本机 SHM Offset、`ShmHandle` 或 C++ 内存镜像（INV-09 对 Fabric 同样成立）——两端地址空间、分配器布局和生命周期均不信任彼此；
2. **信任域判别**：若两端实际属于同一 Trust Domain（同 OS 实例、同一 Region 授权域），应走本地 SHM 路径而非 Fabric Driver；Fabric Driver 只用于跨 Trust Domain。判别依据为 Node Registry 中的 Trust Domain 元数据（ADR-0010），不得由 Driver 自行猜测；
3. **可靠性复用 16.5**：Sequence、ACK、Session Epoch、重传窗口、去重状态机原样复用；Fabric 通道本身有序可靠时（多数 IPCF Channel）可在 capabilities 中声明 `kReliable` 并关闭重传窗口，仅保留去重；
4. **DeliveryStage 语义不变**：`kRemoteAccepted` 仍表示对端完成 16.3 校验并接管消息，与底层是网络还是共享窗口无关；
5. **Cache 与一致性**：非缓存一致域间的 cache maintenance、内存屏障、doorbell 时序属 Driver 内部职责，对上层不可见；Driver 必须保证 Commit 后载荷对对端完整可见；
6. **窗口资源有界**：共享窗口容量有限，无空窗时返回 `kBusy` 并按 Topic 策略丢弃/等待；对端消费速度纳入背压（满窗等价于发送队列满）；
7. **对端域复位**：M 核/远端域崩溃复位按 16.5 断链处理——Session Epoch 变化使旧去重状态失效，窗口所有权复位由 Driver 在重连握手中重新协商；
8. **窗口元数据损坏**：窗口描述符/doorbell 校验失败按 `kCorruption` 处理（见 5.1.1），隔离该 Fabric 通道并导出指标，不得尝试解析可疑载荷。

Fabric Driver 的具体实现（IPCF Channel 配置、NTB BAR 映射、CXL 池租赁）属于部署集成层，不在本设计范围内；本节约束的是任何 Fabric Driver 接入时必须满足的语义契约。

---

## 17. Recorder、Storage 与 Replay

### 17.1 设计范围

Storage 层提供：

- Schema Descriptor 持久化；
- Canonical Message 录制；
- 磁盘抖动场景下的有界内存缓冲；
- Per-Topic Single Writer；
- Segment、Manifest 和 Index；
- 崩溃恢复、校验和修复；
- 按 Topic、时间和 Sequence 回放。

首版不提供分布式一致性日志、跨 Recorder Exactly-once 或多个 Writer 并发修改同一个 Segment。

持久化内容包括 Schema Identity、Topic、来源身份、来源序号、Ingestion Sequence、时间戳、Canonical Payload 和校验信息。禁止将 `ShmHandle`、共享内存 Offset、Ring Slot、C++ 内存镜像或未使用 Capacity 作为长期格式保存。

### 17.2 Recorder Pipeline

```text
Multi-Publisher Topic
      │
      ▼
Recorder Subscriber
      │ Validate + Canonical Encode
      ▼
Recorder-owned Bounded Buffer Pool
      │ MPSC Queue
      ▼
Per-Topic Single Logical Writer
      │ Batch Write / Sync / Rotate
      ▼
Schema Store + Segment + Index + Manifest
      │
      ▼
Replay Engine → New SHM Object → Normal Publish API
```

Recorder 不能在等待磁盘时长期持有 `BorrowedMessage`。它必须先编码并复制到 Recorder 自有内存池，然后及时 ACK 原消息，否则磁盘抖动会阻止 Slab 回收并拖垮实时通道。

录制背压模式必须在 Topic 创建时明确（配置字段见 17.15）：

- **强一致录制**：Recorder 是主 Broadcast 的强制 Subscriber，Buffer Full 可以向 Publisher 传播背压。Recorder 崩溃由 Lease 检测；Lease 失效确认期间 Topic 阻塞新发布（强一致语义不允许录制缺口），Topic 进入 DEGRADED；Recorder 重启后对缺口区间写入 Gap/Tombstone 或由运维显式解除绑定；
- **隔离录制**：独立 Fanout Service（Recorder 进程内线程，作为主 Broadcast 的普通 Subscriber）先复制到 Recorder Channel，磁盘背压不占用主 Channel Payload。Fanout 缓冲有界，满时按 Topic 策略丢弃并记录 Gap 指标——隔离模式在持续磁盘降速下会丢录制但不影响实时通道；
- **尽力录制**：Recorder 落后时写 Gap 并推进 Cursor。

默认实时监控 Topic 使用隔离录制；明确要求消息与录制共同成败的 Topic 才使用强一致录制。仅配置 Recorder 内部 Buffer Policy 不足以定义端到端背压。

背压拓扑（本节的三个模式）与落盘模式（17.4 的四个 `mode`）是两个独立维度，合法组合矩阵：

| 背压拓扑 ↓ \ 落盘模式 → | best_effort | memory_buffered | durable | snapshot |
|---|---|---|---|---|
| 强一致 | ✗（语义矛盾） | ✓ | ✓ | ✗ |
| 隔离 | ✓ | ✓（满时丢弃+Gap） | ✓ | ✓ |
| 尽力 | ✓ | ✓（满时丢弃+Gap） | ✗（durable 要求不丢） | ✓ |

非法组合在配置校验时拒绝（见 20.3）。

### 17.3 Schema Store

Schema Store 持久化不可变 Descriptor，目录示例：

```text
recordings/session-001/
├── manifest
├── schemas/
│   ├── manifest
│   ├── 8f4a...c2.schema
│   └── a912...7d.schema
└── topics/
```

Descriptor 文件必须使用独立版本的显式编码，包含：

- Descriptor Format Version；
- `schema_short_id`、完整 Canonical Digest、`schema_version`、`layout_version`；
- Canonical Schema 内容；
- Layout 和 Codec Metadata；
- 正文长度与 CRC；
- 可选签名和发布者身份。

Schema 必须先于引用它的 Durable Record 持久化：

```text
Validate Schema
      │
      ▼
Write Temporary Descriptor
      │
      ▼
fdatasync + Atomic Rename
      │
      ▼
Schema Durable
      │
      ▼
允许相关 Record Durable
```

等待 Schema 的消息可以暂存在内存中，但等待队列必须有界。相同 Short ID 对应不同完整 Digest 时必须进入碰撞处理，不能覆盖原文件。

Recording Session Schema Table 将紧凑 `schema_ref` 映射到完整 Digest：

```text
Persist Descriptor
  → fdatasync Descriptor
  → Atomic Rename + fsync schemas/ directory
  → Append Schema Table Entry(ref, full digest, descriptor path)
  → fdatasync Schema Table/Manifest
  → 允许 Record 引用 schema_ref
```

`schema_ref` 在 Session 内单调分配且永不复用，跨 Segment 保持稳定；新 Session 可以重新分配。恢复时从 Durable Schema Table 和 Descriptor 重建映射，任何引用未知/未提交 Ref 的 Record 都视为损坏。

### 17.4 录制模式与确认级别

| 模式 | 缓冲区满行为 | 确认语义 | 适用场景 |
|---|---|---|---|
| `best_effort` | 可丢弃 | 不保证全部保存 | 实时优先 |
| `memory_buffered` | 由 `full_policy` 配置（默认 `block`，隔离/尽力拓扑下可为 `drop_*`） | 进程存活时最终落盘 | 吸收磁盘短期抖动 |
| `durable` | 阻塞或失败 | 完成要求的持久化后确认 | 崩溃恢复要求高 |
| `snapshot` | 覆盖旧状态 | 覆盖完成即确认（非 RecordAckLevel） | 状态型 Topic |

```cpp
enum class RecordAckLevel {
    kAccepted,  // Recorder 已接收（仅内部 Flush/协调使用，无对应 DeliveryStage）
    kBuffered,  // 已复制到 Recorder 内存池 → DeliveryStage::kRecorderBuffered
    kWritten,   // write/pwrite 已完成 → DeliveryStage::kStorageWritten
    kDurable,   // fdatasync/fsync 已完成 → DeliveryStage::kStorageDurable
};
```

`kBuffered` 不代表进程崩溃或掉电安全；`kWritten` 也可能仍停留在 Page Cache。只有 `kDurable` 可以按配置作为本机持久化确认，其强度仍依赖文件系统、挂载参数和存储硬件。Storage ACK 通过控制通道返回 Publisher 侧 Receipt Table，关联键为 `(node_id, publisher_id, publisher_epoch, source_sequence)`；Publisher 在 Publish 时冻结的目标集合决定需要等待哪些 Recorder 的 ACK（Recorder 无需感知 Receipt 的存在）。

### 17.5 Memory Buffered 模式

```text
BorrowedMessage
      │
      ├─ Validate
      ├─ Canonical Encode
      ├─ Copy to Recorder Buffer Pool
      └─ ACK Shared Memory Message
                    │
                    ▼
             Bounded MPSC Queue
                    │
                    ▼
             Single Topic Writer
```

建议使用固定 Class 的 Recorder Chunk Pool，例如 4 KiB、64 KiB、1 MiB 和受限大对象 Class，避免每条 Record 直接走通用堆分配。

```cpp
struct PendingRecord {
    uint64_t ingestion_sequence;
    SchemaIdentity schema;
    TopicId topic_id;
    MessageSource source;
    uint64_t ingestion_timestamp_ns;
    BufferHandle payload;
    uint32_t payload_size;
    uint32_t payload_crc;
};
```

缓冲区必须按 Topic 和全局设置字节上限：

```cpp
enum class BufferFullPolicy {
    kBlock,
    kDropNewest,
    kDropOldest,
    kFailRecording,
};
```

- 达到高水位：告警、提高 Batch、缩短 Writer 唤醒间隔；
- 回落到低水位：解除背压和告警；
- 要求完整录制时只能使用 `kBlock` 或 `kFailRecording`；
- 丢弃必须记录指标和 Gap，不能静默发生；
- 内存缓存只能吸收短期抖动，长期必须满足平均落盘吞吐大于平均数据产生速率。

状态机：

```text
RECEIVED
   │ encode + copy
   ▼
BUFFERED
   │ dequeue
   ▼
WRITING
   │ write completed
   ▼
WRITTEN
   │ sync policy
   ▼
DURABLE
   │ release buffer
   ▼
RECLAIMED
```

### 17.6 多 Publisher 与磁盘顺序

Topic 可以由多个节点和进程发布。每个来源使用以下身份：

```cpp
struct MessageSource {
    uint64_t node_id;
    uint64_t publisher_id;
    uint64_t publisher_epoch;
    uint64_t source_sequence;
    uint64_t observed_timestamp_ns;
};
```

重复检测键为：

```text
(node_id, publisher_id, publisher_epoch, source_sequence)
```

检测到重复时**丢弃后到的副本**并导出 `mino_recorder_duplicate_total` 指标；先到记录保持原样，不覆盖。滚动升级的 Controlled Dual Publish 依赖此语义。

语义为：

- 保证同一 Publisher、同一 Epoch 内的来源顺序；
- Publisher 重启后生成新的 Epoch；
- 不承诺不同节点真实事件时间的全局顺序；
- Recorder 先在有界 Write Queue 中 Reserve 一个 Slot；Queue Full 时尚未分配 Ingestion Sequence；
- Canonical Encode/Copy 准备完成后，在已预留 Slot 上分配 `ingestion_sequence` 并原子 Commit；
- Reserve 后发生编码、复制或关闭失败时，直接在同一已预留 Slot 提交 Gap/Tombstone；
- 禁止采用“先分配 Sequence，再尝试另行入队 Tombstone”的流程，因为满队列时 Tombstone 也可能无法入队；
- 入队路径（Reserve 之前）按策略丢弃时，**Gap 的唯一生成点是 TopicWriter**：Writer 出队时按来源比对 `source_sequence` 连续性，发现跳变时在写该来源的下一条 Record 之前先生成 Gap Record（记录缺口区间与原因），Gap Record 在 Writer 侧直接写入，不经过已满的入队路径；
- 磁盘物理顺序和确定性回放以 Ingestion Sequence 为准；
- `observed_timestamp_ns` 为发布端 Wall Clock（未保证同步），仅用于展示与按事件时间过滤查询，不作为排序依据。

### 17.7 Per-Topic Single Writer

```text
Topic A Publishers → Queue A → Writer A → Segment A
Topic B Publishers → Queue B → Writer B → Segment B
Topic C Publishers → Queue C → Writer C → Segment C
```

约束：

```text
一个 Topic
  = 一个有界写入队列
  = 一个逻辑 TopicWriter
  = 一个当前活动 Segment
  = 一条顺序提交链
```

```cpp
class TopicWriter {
public:
    Status Enqueue(PendingRecord record);
    Status Start();
    Status Flush(RecordAckLevel level);
    Status Stop();

private:
    TopicId topic_id_;
    BufferPool buffer_pool_;
    OrderedWriteQueue queue_;
    SegmentWriter segment_writer_;
};
```

`TopicWriterManager` 保证同一 Recording Session、Topic 和 Partition 只有一个活动 Writer。首版可以一 Topic 一线程；Topic 数量较大时可以使用共享 Executor，但必须保证同一 TopicWriter 不被并发执行。

如果编码并行导致完成顺序变化，Ordered Commit Queue 按 Ingestion Sequence 重排。来源 Sequence Gap 按各来源独立记录，不应该阻塞其他 Publisher 的整个 Topic。

### 17.8 Topic Partition 扩展

单 Topic Writer 达到编码、CPU 或磁盘瓶颈后，通过 Partition 扩展：

```text
Logical Topic
├── Partition 0 → Writer 0 → Segment P0
├── Partition 1 → Writer 1 → Segment P1
└── Partition 2 → Writer 2 → Segment P2
```

```cpp
partition_id = StableHash(partition_key) % partition_count;
```

建议使用 `node_id` 或显式业务 Key 作为 Partition Key。约束：

- 一个 Topic 可以有多个 Partition；
- 一个 Partition 只有一个逻辑 Writer；
- 一个活动 Segment 只有一个 Writer；
- 只保证 Partition 内顺序；
- Partition Count 变化需要配置版本和迁移策略；
- 首版即使只使用 Partition 0，文件格式也预留 `partition_id`。

禁止通过原子文件 Offset 预留让多个 Writer 并发写同一个 Segment，因为这会显著增加轮转、CRC、索引、压缩、同步和崩溃恢复复杂度。

### 17.9 磁盘目录

```text
recordings/session-001/
├── manifest
├── schemas/
└── topics/
    └── 1001/
        └── partitions/
            └── 0000/
                ├── manifest
                ├── segments/
                │   ├── 00000001.mino
                │   └── 00000002.mino
                └── indexes/
                    ├── sequence.idx
                    └── timestamp.idx
```

Index 可以重建，不是数据真实性来源；已提交并通过 CRC 校验的 Segment Record 才是事实来源。

### 17.10 Segment 与 Record 格式

逻辑 Segment Header：

```cpp
struct SegmentHeader {
    uint32_t magic;
    uint16_t format_version;
    uint16_t flags;
    uint64_t recording_id;
    uint32_t topic_id;
    uint32_t partition_id;
    uint64_t writer_id;
    uint64_t first_ingestion_sequence;
    uint64_t created_at_ns;
    uint32_t header_crc;
};
```

逻辑 Record Header：

```cpp
struct RecordHeader {
    uint32_t magic;
    uint16_t format_version;
    uint16_t flags;

    uint32_t schema_ref;       // Recording Session Schema Table 紧凑序号
    uint32_t schema_version;   // major << 16 | minor
    uint32_t layout_version;
    uint32_t topic_id;
    uint32_t partition_id;

    uint64_t ingestion_sequence;
    uint64_t ingestion_timestamp_ns;  // Recorder 本地 Monotonic + Wall 快照

    uint64_t node_id;
    uint64_t publisher_id;
    uint64_t publisher_epoch;
    uint64_t source_sequence;
    uint64_t observed_timestamp_ns;   // 发布端 Wall Clock（未保证同步，仅展示/过滤用）

    uint32_t payload_size;
    uint32_t payload_crc;
    uint32_t header_crc;   // 覆盖本结构除自身外的全部字段
    uint32_t reserved;
};
```

这些 C++ Struct 只表达逻辑字段，不能直接 `write(sizeof(struct))`。磁盘格式必须通过显式 Encoder 固定字节序、字段宽度、长度和对齐。`schema_ref` 是 Recording Session Schema Table 的紧凑序号，该表将 Ref 映射到完整 Canonical Digest 和 Descriptor；Ref 只在本 Session 内有效。

Record 磁盘布局（字节序：小端）：

```text
Record
├── record_length        8 B   // 从本字段之后到 Trailer 末尾的总字节数
├── header_length        4 B
├── Encoded Header             // 含 header_crc
├── Canonical Payload
├── Alignment Padding
└── Trailer
    ├── record_length    8 B   // 与首部一致
    ├── ingestion_sequence 8 B
    ├── record_crc       4 B   // 覆盖 record_length(首) ~ Padding 的全部字节
    └── commit_marker    8 B   // 固定魔数 0x4D494E4F434D4D54 ("MINOCMMT")
```

首版 Commit Protocol：
Segment 生命周期：

```text
CREATING → OPEN → SEALED → INDEXED → RETAINED/DELETED
```

- OPEN Segment 只允许所属 TopicWriter 写；
- 达到大小、时长或 Record 数阈值后轮转；
- SEALED 后 Payload 区不可修改；
- Manifest 通过临时文件、同步和原子 Rename 更新；
- Record 长度和 Offset 运算必须防整数溢出。

首版 Commit Protocol：

1. 编码 Header 和 Payload，计算 Header CRC 与 Payload CRC；
2. 写入 `record_length + header_length + header + payload + padding + trailer`；
3. Trailer 重复 `record_length`、`ingestion_sequence` 和整 Record CRC；
4. 最后写固定 Commit Marker（8 字节魔数，见布局图）；
5. `write/writev` 短写必须循环完成或使 Writer 进入 ERROR；
6. Durable Batch 执行 `fdatasync(segment_fd)`；
7. Segment Seal 后同步 Segment，再原子更新 Manifest；
8. 临时文件 Rename 后必须 `fsync` 父目录；Schema Store 采用相同目录同步规则；
9. Index 只引用已提交 Offset，Index 丢失时从 Segment 重建。

恢复扫描仅接受长度首尾一致、CRC 正确且 Commit Marker 完整的 Record。Commit Marker 不假设单扇区原子性，最终仍以完整校验为准。

### 17.11 批处理、压缩与同步

Writer 按任一条件形成 Batch：

- 达到 `batch_bytes`；
- 达到 `batch_records`；
- 达到 `flush_interval`；
- 收到显式 Flush；
- Segment 即将轮转。

首版优先顺序 `writev`/`pwrite`，后续按平台评估 `io_uring`。压缩以独立 Record 或 Block 为单位，必须保留可扫描的恢复边界。

同步策略：

| 策略 | 行为 |
|---|---|
| `none` | 依赖 OS 回写 |
| `interval` | 按时间或字节周期 `fdatasync` |
| `per_batch` | 每个 Batch 同步 |
| `per_record` | 每条 Record 同步，延迟最高 |

`memory_buffered` 默认周期同步；`durable` 的 ACK 必须等待其配置要求的同步点。

### 17.12 Storage 崩溃恢复

启动时恢复最后一个 OPEN Segment：

1. 校验 Segment Header；
2. 从最后已知 Commit Offset（持久化于 Manifest 的检查点字段，每次 Durable Batch 后更新）开始扫描；检查点过期时从 Segment 起始全量扫描；
3. 校验 Record 长度、边界、CRC 和 Commit Marker；
4. 找到最后一条完整提交 Record；
5. 截断不完整尾部；
6. 重建索引；
7. 原子更新 Manifest；
8. 按策略继续写或创建新 Segment。

补充恢复分支：

- **Seal 后、Manifest 更新前崩溃**：恢复扫描在目录中发现不在 Manifest 中的 SEALED Segment 时，校验其完整性；完整则补录进 Manifest，不完整则隔离（重命名为 `.orphan` 并告警）；
- **Manifest 新旧版本均不可读**：从 Segment 文件逐一扫描重建 Manifest（耗时操作，导出指标并告警）；
- **Schema Ref Table 损坏**：引用未知 Ref 的 Record 视为损坏；尝试从 `schemas/` 目录中的 Descriptor 重建 Ref Table，无法重建的 Segment 隔离为 `.orphan`，其余数据不受影响；
- **ENOSPC/EIO**：Writer 进入 ERROR 状态，停止该 Topic 录制并告警；`kFailRecording` 策略下 Recording 整体标记失败；空间/介质恢复后需运维显式重启 Writer（不自动恢复，避免抖动写放大）；
- **只读切换**：同 EIO 处理。

同一 Session、Topic 和 Partition 使用文件锁和 Recorder Lease 双重保证 Writer 唯一性，身份必须包含启动 Epoch，不能只依赖可能复用的 PID。首版一个 Recording Session 只有一个 Recorder Owner，不提供自动主备；Owner 失败后由人工或受控编排确认旧 Lease 失效（判据：Lease 超时 + 文件锁已释放 + 进程不可达），再启动新 Owner，避免 Split-brain。

进程崩溃或掉电时，处于 BUFFERED 或尚未同步的 WRITTEN 数据可能丢失。恢复报告必须包含：

- 最后 Durable Ingestion Sequence；
- 最后可恢复 Record；
- 检测到的 Source Sequence Gap；
- 被截断的字节数；
- Schema 和 Manifest 校验结果。

如果业务不能接受该风险，应使用 Durable 模式，或另行设计 WAL/非易失介质。

### 17.13 Replay

```text
Manifest + Schema Store
          │
          ▼
    Segment Reader
          │
          ├─ CRC Validate
          ├─ Schema Resolve
          ├─ Canonical Decode
          ├─ Allocate New SHM Object
          └─ Publish through Normal API
```

支持：

- 原速、倍速、慢速和单步回放；
- 按 Topic、Node、时间、Source Sequence、Ingestion Sequence 过滤；
- 保留原时间戳或生成回放时间戳；
- 回放消息携带 `message_origin=REPLAY`、`replay_session_id`、原时间戳和回放时间戳；
- 默认发布到独立 Replay Namespace，只有显式授权才能注入实时 Topic；
- 单 Partition 按 Ingestion Sequence 回放；
- 跨 Topic 按接收时间归并，但不宣称真实事件全序。

默认使用记录时 Schema 重建。目标只支持其他版本时，必须使用已注册且验证过的转换规则；不兼容时隔离或停止，不能猜测字段布局。禁止恢复历史 Handle 或直接覆盖 Ring Slot。

### 17.14 Retention

Retention 可以按以下条件删除 SEALED Segment：

- 最大保留时间（以 Segment 内最晚 Record 的 `ingestion_timestamp_ns` 为准）；
- 最大总字节数（作用域：每 Topic；全局上限见 20.4 节点预算）；
- 最大 Segment 数；
- 外部归档完成标记。

Replay、Inspector、Index Builder 和归档任务读取 Segment 前获取 Segment Pin/Lease（默认时长 5 分钟，可续约）。Retention 先从新 Manifest 移除 Segment——**移除即关闭新 Pin**（Pin 获取必须校验目标 Segment 在当前 Manifest 内）；等待现存 Pin 清零后再删除文件；进程在任一步骤崩溃都通过 Manifest 和 Pin Lease 恢复。OPEN Segment 不参与普通 Retention 删除。

### 17.15 Storage 配置

```yaml
recording:
  root: /var/lib/mino/recordings

  topics:
    system/cpu:
      mode: memory_buffered
      backpressure_topology: isolated   # strong_consistent / isolated / best_effort
      partition_count: 1

      memory_buffer:
        capacity: 1GiB
        high_watermark: 80%
        low_watermark: 50%
        full_policy: block

      writer:
        batch_bytes: 8MiB
        batch_records: 1024
        flush_interval: 10ms
        sync_mode: interval
        sync_interval: 1s

      segment:
        max_size: 1GiB
        max_duration: 10m
        compression: lz4

      retention:
        max_age: 24h
        max_bytes: 1TiB
```

本节的嵌套结构为 Recording 配置的规范形式；Topic 级 `recording:` 平铺简写（20.2 示例）按固定映射展开为本结构（`buffer_capacity` → `memory_buffer.capacity`，`buffer_full` → `memory_buffer.full_policy`，`batch_bytes` → `writer.batch_bytes`，`sync_interval` → `writer.sync_interval`）。全局 `recording:` 提供默认值，Topic 级覆盖。所有容量和时间配置必须有全局上限。热更新不能改变正在写入的 Segment Format；必要时应先轮转。

### 17.16 Storage 指标与工具

指标至少包括：

- 每 Topic 入队和落盘记录数/字节数；
- Canonical Encode 延迟；
- Buffer 当前字节数、高低水位和最旧 Record 年龄；
- Buffer Full、Drop、Gap 和背压持续时间；
- Batch 大小、Write/Flush/Sync 延迟；
- `BUFFERED → WRITTEN → DURABLE` 延迟；
- Segment 数、轮转数和恢复截断字节数；
- 磁盘空间、ENOSPC、EIO 和只读错误；
- 入队速率与落盘速率差；
- 预计 Buffer 耗尽时间。

工具接口：

```bash
mino schema list
mino schema inspect <schema-id>
mino record start --topic system/cpu
mino record status
mino replay <session> --topic system/cpu --speed 1.0
mino storage inspect <session>
mino storage verify <session>
mino storage repair <session>
```

Repair 默认只读预览，执行修改需要显式确认并生成审计记录。

### 17.17 Snapshot 模式

Snapshot 必须配置 Key，不能把“整个 Topic 最新一条”作为隐含规则：

- 无 Key 时整个 Topic 只有一个最新值；
- 有 Key 时使用 Schema 中标记的稳定字段组成 Snapshot Key；
- 多 Publisher 对同一 Key 的覆盖按 Ingestion Sequence 决定；
- Snapshot Store 使用 Copy-on-write 文件和原子 Manifest，不原地覆盖唯一副本；
- Snapshot Record 保留 Source Identity、Schema 和原始时间；
- Snapshot 可以与历史 Segment 共存，Retention 策略分别配置。

### 17.18 存储加密

启用存储加密时采用独立 Block AEAD：

- Segment Header 保存算法、Key ID 和随机 Segment Nonce Prefix；
- 每 Block Nonce 由 Prefix + 唯一 Block Sequence 派生，禁止复用；
- AEAD 覆盖 Header Metadata 和 Payload；
- CRC 用于快速损坏检测，AEAD 用于真实性，不能用 CRC 替代认证；
- Key Rotation 只影响新 Segment，历史 Segment 保留原 Key ID；
- Repair/Replay 工具通过受控 Key Provider 获取密钥，日志不得输出密钥或明文 Payload。

---

## 18. 启动与关闭

### 18.1 Runtime 启动

```text
Load Config
  → Initialize Logging/Metrics
  → Open Registry Client
  → Attach/Create SHM Region
  → Validate or Recover Region
  → Load Schema Registry
  → Register Process Lease
  → Create Channels
  → Start Heartbeat
  → Start Bridge/Recorder as configured
  → READY
```

任一步骤失败都必须逆序释放已创建资源。READY 前不能接受业务发布。

### 18.2 优雅关闭

```text
RUNNING
  → Reject New Registrations
  → Stop New Publishes（含 Bridge 的远端重发布）
  → Bridge 入方向停止接受新帧并 Drain 发送队列
  → Drain/Stop Subscribers
  → Flush Recorder to configured ACK level
  → Stop Bridge
  → Release Leases
  → Mark Region clean_shutdown if owner
  → Detach Region
```

关闭必须有 Deadline。超时后记录未完成资源和最后 Durable Sequence，再执行受控退出。

顺序约束：Bridge 入方向必须先于 Recorder Flush 停止，否则 Flush 后进入本地 Channel 的远端消息将错过落盘且无 Gap 标记（对要求完整录制的 Topic 这属于静默丢失）。

### 18.3 非正常恢复

- Region 恢复和 Storage 恢复是独立流程；
- Region 扫描 Handle、Slot、Bitmap、Lease 和 ACK；
- Storage 扫描 Segment Tail、CRC、Commit Marker 和 Manifest；
- 恢复程序不能仅依据 PID 判断 Owner 存活；
- 发现不可修复损坏时进入 QUARANTINED，禁止猜测继续运行。

### 18.4 滚动升级

首版不支持 Shared Memory Region 原位升级。Layout 或不兼容 Schema 变更采用：

```text
Create New Region/Channel
  → Attach New-version Processes
  → Optional Controlled Dual Publish
  → Switch Route Config Version
  → Drain Old Region
  → Detach and Retire Old Region
```

旧进程只允许 Attach 明确兼容的 Layout Version。Storage、Descriptor 和 Network Protocol 各自协商版本；历史 Segment 保持原格式并由 Reader 选择对应 Decoder。

---

## 19. 并发与内存序规则

### 19.1 基本规则

1. Payload 构建先于 Slot READY Release Store；
2. 消费者 READY Acquire Load 后才能读取 Payload；
3. ACK 先于最后 Reader 的 Reclaim；
4. Cursor Sequence 区分 Ring 回绕；
5. 普通元数据在 Publish 后不可修改；
6. Heartbeat 可以 Relaxed，但 Lease 状态切换需要定义同步关系；
7. Bitmap CAS 的成功和失败内存序必须逐处说明；
8. 跨进程原子必须在目标平台验证 Lock-free 和共享映射语义；
9. 不在共享控制结构中使用 Mutex，除非明确采用并验证 Process-shared Robust Mutex；
10. 不使用 `volatile` 替代原子或内存屏障。

### 19.2 Cache Line

以下字段分离 Cache Line：

- Producer Cursor 与 Consumer Cursor；
- 不同高频 Subscriber Cursor；
- 高频 Allocator Bitmap Shard；
- 只读配置与高频写计数器；
- Slot 数组按固定 Alignment 排布。

### 19.3 锁顺序

控制面如需多个锁，统一顺序：

```text
Registry → Topic → Channel → Subscriber/Publisher → Storage Writer
```

数据热路径不得获取 Registry 全局锁。锁顺序必须通过注释和测试固定。

### 19.4 等待、Deadline 与取消

所有可能阻塞的 Allocate、Publish、Poll、Flush、Bridge Send 和 Shutdown 都必须接收 Deadline 或 Stop Token。`kBlock` 表示在 Deadline 内等待，不表示永久等待。等待实现按配置选择 Busy Poll、Spin-then-park 或 Futex，并导出等待时长及超时指标。

---

## 20. 配置设计

### 20.1 配置层级

```text
Global
├── Node
├── Shared Memory Regions
├── Schema Limits
├── Transports
├── Topics
│   ├── Channel
│   ├── Delivery
│   ├── Security
│   └── Recording
└── Observability
```

### 20.2 Topic 示例

```yaml
topics:
  system/cpu:
    schema: mino.system.CpuMetrics@1
    channel: mpsc
    capacity: 65536
    max_publishers: 1024
    max_subscribers: 32
    queue_full: block

    delivery:
      reliability: reliable_ordered

    recording:
      enabled: true
      mode: memory_buffered
      backpressure_topology: isolated   # strong_consistent / isolated / best_effort
      buffer_capacity: 1GiB   # → memory_buffer.capacity
      buffer_full: block      # → memory_buffer.full_policy
      batch_bytes: 8MiB       # → writer.batch_bytes
      sync_interval: 1s       # → writer.sync_interval
```

平铺字段是 17.15 嵌套结构的简写，映射规则见 17.15。`backpressure_topology` 与 `mode` 的合法组合见 17.2 矩阵。

显式配置路由（kStatic）示例：

```yaml
topics:
  chassis/vehicle_state:
    schema: mino.chassis.VehicleState@2
    channel: broadcast
    capacity: 4096
    route_policy: static            # discovery（默认）/ static
    static_routes:                  # 仅 static 时必填：固定发布的目标节点
      - node: zone-front-01
      - node: zone-rear-01
      - node: recorder-01
        preferred_transport: fabric # 可选：强制经 IPCF 类 Fabric 通道
```

`static_routes` 以节点为粒度，不含进程；Topic 创建时写入 Registry，`route_set_version` 随变更递增（14.3）。

### 20.3 校验

配置加载时检查：

- Topic Name/ID 冲突；
- Channel 类型与 Publisher/Subscriber 数量；
- Buffer、Ring 和 Slab 总内存预算；
- Schema 容量与 Topic 最大消息尺寸；
- Recording 策略是否与“不丢”要求冲突（含 17.2 背压拓扑 × 落盘模式非法组合）；
- Driver 能力；
- ACL；
- `route_policy: static` 时 `static_routes` 非空、节点已注册或声明为可离线加入、目标节点在 Topic ACL 允许集合内；`route_policy: discovery` 时不得携带 `static_routes`；
- 数值乘加溢出。

### 20.4 节点级资源预算

配置系统在创建 Topic 前执行 Admission Control：

```text
SHM SuperBlock/Directory
+ Ring Capacity
+ Slab Classes/Large Object Pool
+ Bridge Send/Receive/Reassembly Queues
+ Dynamic Schema Cache
+ Recorder Buffers
+ Replay Buffers
+ Observability Reserve
≤ Node Memory Budget
```

同时校验磁盘容量、文件描述符、线程数、Pinned Memory 和网络重传窗口。每个模块保留 Emergency Reserve，控制面和恢复工具不能因数据面耗尽而无法运行。超出预算时拒绝配置，不依赖 OOM Killer。

### 20.5 配置变更分类

- 可热更新：日志级别、部分指标、Retention 阈值；
- 需 Segment 轮转：压缩、加密 Key、Storage Format 可选项；
- 需 Drain/Cutover：Ring Capacity、Partition Count、Layout 和不兼容 Schema；
- 禁止运行时修改：已创建 Region 的基础 ABI 和 Offset Width。

---

## 21. 可观测性

### 21.1 Metric 命名

建议前缀：

```text
mino_channel_*
mino_allocator_*
mino_schema_*
mino_bridge_*
mino_transport_*
mino_recorder_*
mino_storage_*
mino_telemetry_*
mino_runtime_*
```

`mino_transport_*` 用于 Driver 层与传输阶段指标（21.5），`mino_bridge_*` 用于 Bridge 实例/连接级指标；`mino_telemetry_*` 为 Telemetry 系统自监控指标。高基数 ID（完整 Trace ID、Publisher ID）不得直接作为 Metric Label。Topic Label 需要配置数量上限。

### 21.2 Trace

Trace Context 至少包含：

- Trace ID；
- Topic ID；
- Source Identity；
- Source Sequence；
- Ingestion Sequence（录制路径）；
- Schema ID；
- Publish、Bridge Send/Receive、Remote Publish、Consume 时间点。

### 21.3 日志

- 热路径日志采样或限频；
- 不记录敏感 Payload；
- Corruption 日志包含 Offset/Sequence，但解引用失败后不得继续打印对象字段；
- 日志使用结构化字段；
- 恢复操作生成独立审计记录。

### 21.4 健康状态

```cpp
enum class HealthState {
    kStarting,
    kHealthy,
    kDegraded,
    kRecovering,
    kQuarantined,
    kStopping,
};
```

磁盘 Buffer 长期高水位、Bridge 断链或 Schema Store 不可写应进入 DEGRADED，而不是继续报告完全健康。

### 21.5 传输性能指标设计

#### 21.5.1 目标与边界

传输性能统计需要回答：

- 每个 Topic 当前发送、接收和有效 Payload 吞吐是多少；
- 延迟发生在分配、排队、编码、网络、解码还是消费阶段；
- p50、p95、p99、p99.9 和最大延迟是否异常；
- 是否发生丢弃、重传、重复、背压或队列饱和；
- 统计本身给数据热路径增加了多少开销。

指标系统不得改变消息交付语义，不得因 Exporter 变慢阻塞 Publisher、Subscriber 或 Bridge。

#### 21.5.2 传输阶段模型

```text
Publisher
  T0 Allocate Begin
  T1 Allocate End
  T2 Build/Validate End
  T3 Ring Reserve
  T4 READY Commit
        │ Local SHM
  T5 Subscriber Acquire
  T6 Subscriber Callback Begin/End
        │ Remote Bridge
  B0 Bridge Borrow
  B1 Encode End
  B2 Send Queue Enter
  B3 Socket Write Complete
  B4 Remote Frame Complete
  B5 Decode End
  B6 Remote READY Commit
  B7 Remote Subscriber Acquire
```

主要延迟定义：

| 指标 | 计算方式 | 含义 |
|---|---|---|
| Allocation latency | `T1-T0` | Slab 分配耗时 |
| Build/validate latency | `T2-T1` | 构建和校验耗时 |
| Ring reserve wait | `T3-T2` | Ring 背压等待 |
| Local delivery latency | `T5-T4` | 本地发布到 Subscriber 可见 |
| Subscriber processing | Callback End-Begin | 消费处理耗时 |
| Bridge encode latency | `B1-B0` | Canonical Encode 耗时 |
| Bridge queue wait | `B3-B2` | 网络发送队列等待 |
| Network/remote ingress | `B4-B3` | 仅在时钟语义允许时解释 |
| Decode/rebuild latency | `B6-B4` | 校验、解码、分配和远端发布 |
| Remote delivery latency | `B7-B6` | 远端 SHM 交付耗时 |
| End-to-end latency | Publish Commit 到最终 Acquire | 完整传输延迟 |

跨节点单向延迟只有在时钟同步质量满足阈值时才上报。否则只报告本机阶段耗时和 Ping/ACK RTT，禁止直接相减两个节点的 Monotonic Clock。

#### 21.5.3 指标分类

**Counter**：

```text
mino_transport_messages_total
mino_transport_payload_bytes_total
mino_transport_wire_bytes_total
mino_transport_dropped_total{reason}
mino_transport_retransmit_total
mino_transport_duplicate_total
mino_transport_crc_error_total
mino_transport_schema_error_total
mino_transport_timeout_total{stage}
```

**Gauge**：

```text
mino_transport_inflight_messages
mino_transport_send_queue_bytes
mino_transport_receive_queue_bytes
mino_transport_ring_utilization_ratio
mino_transport_oldest_queued_age_ns
mino_transport_active_connections
```

**Histogram**：

```text
mino_transport_allocate_latency_ns
mino_transport_ring_wait_ns
mino_transport_local_delivery_latency_ns
mino_transport_encode_latency_ns
mino_transport_send_queue_latency_ns
mino_transport_rtt_ns
mino_transport_decode_latency_ns
mino_transport_remote_delivery_latency_ns
mino_transport_e2e_latency_ns
mino_transport_message_size_bytes
mino_telemetry_histogram_samples_total
mino_telemetry_events_total
mino_telemetry_dropped_total{reason}
mino_telemetry_trace_incomplete_total{stage}
mino_telemetry_clock_uncertain_total
mino_telemetry_export_fail_total
```

派生指标：

```text
message_rate = Δmessages_total / Δtime
payload_throughput = Δpayload_bytes_total / Δtime
wire_throughput = Δwire_bytes_total / Δtime
protocol_overhead_ratio = (wire_bytes - payload_bytes) / payload_bytes
loss_ratio = dropped / attempted
queue_saturation = current_queue_bytes / queue_capacity_bytes
```

#### 21.5.4 采样等级

```cpp
enum class PerfTelemetryMode {
    kOff,
    kCountersOnly,
    kSampledLatency,
    kFullDebug,
};

struct PerfTelemetryPolicy {
    PerfTelemetryMode mode;
    uint32_t sample_rate_ppm;
    uint64_t slow_threshold_ns;
    uint32_t max_events_per_second;
};
```

- `kCountersOnly` 使用线程本地批量 Counter，不记录逐消息时间点；
- `kSampledLatency` 按稳定哈希 `(topic_id, source identity, sequence)` 采样，避免随机采样在重试时改变；
- 超过 `slow_threshold_ns` 的已采样消息保留完整阶段事件；
- `kFullDebug` 只用于短期诊断，并设置持续时间和事件速率上限；
- 未采样消息仍更新丢弃、错误和字节 Counter。

#### 21.5.5 Trace Context

```cpp
struct PerfTraceContext {
    uint64_t trace_id_high;
    uint64_t trace_id_low;
    uint32_t sample_flags;
    uint32_t clock_domain_id;
    uint64_t origin_wall_time_ns;
    uint64_t origin_monotonic_ns;
};
```

只有设置 `PERF_TRACE_SAMPLED` Header Flag 的消息才携带 PerfTraceContext；未采样消息不为 Telemetry 额外增加固定 Wire/SHM Payload。全部阶段时间戳不写入共享 Slot/网络 Header。每个进程将阶段事件写入进程本地有界 Sidecar Buffer，通过 Trace ID 关联。Buffer 满时丢 Telemetry Event 并增加 `telemetry_dropped_total`，不能影响业务消息。

#### 21.5.6 时钟与跨节点测量

每个节点维护：

```cpp
struct ClockQuality {
    uint32_t clock_domain_id;
    int64_t estimated_offset_ns;
    uint64_t uncertainty_ns;
    uint64_t last_sync_time_ns;
    ClockSyncState state;
};
```

- 节点内阶段耗时使用 Monotonic Clock；
- 跨节点事件时间使用同步 Wall Clock/PTP，并附带 Uncertainty；
- 当 `uncertainty_ns` 超过 Topic 阈值时停止发布单向 E2E Histogram；
- RTT 不要求节点时钟同步；
- 时钟跳变、同步失效和负延迟样本必须计数并丢弃，不能写入正常 Histogram。

#### 21.5.7 低开销采集

- 热路径 Counter 使用 Thread-local/Sharded Counter，周期合并；
- Histogram 使用固定内存的对数桶或 HDR 风格分片，不在热路径分配；
- 禁止每条消息获取全局 Metrics Lock；
- 时间戳只对采样消息读取，Counters-only 模式避免多余 Clock 调用；
- Metric Export 在独立低优先级线程执行，使用有界 Snapshot Queue；
- Exporter 失败只丢监控快照并告警，不反向阻塞数据路径；
- Topic Label、Node Label 和 Error Reason 采用受控枚举，禁止 Publisher ID、Trace ID 等高基数 Label。

性能开销验证目标：Counters-only 模式吞吐下降不超过 1%，默认采样模式不超过 2%；它们是原型验收目标，不是未经测试的正式 SLA。

#### 21.5.8 聚合与导出

```text
Per-thread Counters/Histograms
          │ periodic merge
          ▼
Process Metrics Aggregator
          ├─ Prometheus Endpoint
          ├─ OTLP Exporter
          └─ Local Diagnostic Snapshot
```

聚合周期默认 1 秒。进程级聚合保留 Topic 和 Transport Driver 维度；节点级 Dashboard 再汇总，不在进程内计算跨节点全局分位数。Histogram 聚合必须合并桶，不能对各节点 p99 直接求平均。

#### 21.5.9 配置示例

```yaml
observability:
  transport_metrics:
    enabled: true
    mode: sampled_latency
    sample_rate_ppm: 10000       # 1%
    slow_threshold: 5ms
    max_events_per_second: 10000
    aggregate_interval: 1s
    clock_uncertainty_limit: 50us
    exporters:
      - prometheus
      - otlp

  topic_overrides:
    control/critical:
      sample_rate_ppm: 100000
      slow_threshold: 500us
```

配置热更新通过不可变 Policy Snapshot 发布。采样策略变化不修改 Topic ABI，也不要求重新创建 Channel。

#### 21.5.10 统计口径与窗口

- Counter 单调累计，进程重启通过 `process_epoch` 区分；
- Rate 由查询端对累计 Counter 求差，不在热路径维护滑动速率；
- Histogram 同时提供累计桶和固定时间窗口快照，窗口边界记录开始/结束时间；
- `max` 按窗口重置并单独保留进程生命周期最大值；
- Attempted、Accepted、Committed、Delivered、Dropped 分别计数，不能只统计成功消息；
- Drop Reason 使用固定枚举：Queue Full、Deadline、Schema、CRC、ACL、Decode、Shutdown 等；
- 多 Subscriber 延迟默认按 Topic 聚合，同时输出 Slowest Subscriber Lag；仅允许受控 Subscriber Class Label，禁止 Subscriber ID Label；
- Batch 的 Wire Bytes 按实际写出字节计数，单消息延迟仍从各自阶段时间点计算；
- 压缩前 Payload Bytes 和压缩后 Wire Bytes 分开统计。

为减少 Coordinated Omission，除已完成消息延迟外必须持续上报 Queue Depth、Oldest Queued Age 和 Deadline Timeout。仅观察完成样本会漏掉仍卡在队列中的最慢消息。

#### 21.5.11 Telemetry Event

```cpp
enum class PerfStage : uint16_t {
    kPublishCommit,
    kLocalAcquire,
    kBridgeBorrow,
    kEncodeDone,
    kSendQueued,
    kSocketWriteDone,
    kRemoteFrameDone,
    kDecodeDone,
    kRemoteCommit,
    kRemoteAcquire,
};

enum class MessageOrigin : uint8_t {
    kLive,
    kReplay,
};

struct PerfEvent {
    TraceId trace_id;
    TopicId topic_id;
    PerfStage stage;
    uint32_t clock_domain_id;
    uint32_t hop_id;
    uint32_t attempt_id;
    uint32_t component_instance;
    uint32_t subscriber_generation;
    MessageOrigin message_origin;
    uint64_t monotonic_time_ns;
    uint64_t wall_time_ns;
    uint32_t payload_bytes;
    uint32_t wire_bytes;
};
```

PerfEvent 只存在于进程本地 Sidecar Buffer，不作为共享内存或 Wire ABI 直接写出。Hop、Attempt 和 Subscriber Generation 用于区分多 Bridge、重传和多 Subscriber，但不得作为 Metrics Label。跨进程关联使用 Trace ID、Source Identity 和 Sequence；聚合器发现阶段缺失时增加 `trace_incomplete_total{stage}`，不能用相邻不匹配事件推算延迟。

#### 21.5.12 采样传播

采样决策由源 Publisher 基于稳定哈希生成并随 Trace Context 传播，Bridge 和远端 Runtime 必须遵守同一决策，避免只有部分 Hop 被采样。中间节点可以因本地 Rate Limit 丢弃 Telemetry Event，但不能把未采样消息升级为全链路采样；紧急诊断升级需要新的 Policy Epoch 并受事件上限控制。

---

## 22. 安全设计

### 22.1 本地与 Trust Domain

首版安全模型明确假设：所有获得同一 Region 读写映射的进程都属于同一可信计算域。Accessor、Bounds Check 和 CRC 用于防误用及损坏，不构成对恶意已 Attach 进程的隔离。

- 一个 Region 只服务一个 Trust Domain；
- 不可信应用不得直接 Attach，通过受控 Proxy/Bridge 使用 Mino；
- 多租户或不同安全等级使用独立 Region、UID/GID 和进程；
- SHM 使用最小 UID/GID/Namespace 权限；
- Attach 前检查 Owner、Region UUID、权限和 Security Domain ID；
- 能只读映射的诊断进程不得获取写映射；
- 动态 View 仍执行边界和 Schema 校验；
- 诊断工具默认只读；
- Repair、Delete 和 Force Recovery 需要显式权限；
- 不向业务暴露任意 Offset Resolve API；
- 如果未来需要对已 Attach 恶意进程隔离，必须改为按角色拆 Region 或引入 Broker，不宣称单一 RW Region 可以实现该目标。

### 22.2 网络

- Bridge 双向认证；
- Topic ACL；
- TLS/IPsec/受控 Fabric；
- Header/Payload 长度上限；
- AEAD 或 CRC 根据威胁模型选择；
- 未知 Schema 请求限频；
- 每连接 Decode 内存和 CPU Budget。

### 22.3 动态 Schema

- IDL 输入大小上限；
- AST 节点、字段和递归深度上限；
- 总 Capacity 上限；
- 编译 Deadline；
- Descriptor 指纹和可选签名；
- Registry 冲突拒绝；
- Fuzz Parser、Descriptor Decoder 和 Dynamic Accessor。

### 22.4 密钥与凭据生命周期

- TLS、Descriptor Signature 和 Storage Encryption 使用不同用途的 Key；
- Key ID 可记录，Key Material 不进入 SHM、普通配置、日志或 Dump；
- Rotation 采用新连接/新 Segment 生效，旧 Key 在历史数据保留期内可读取；
- 凭据失效时 Bridge 拒绝新连接，Recorder 根据策略停止敏感 Topic 录制；
- 所有 Repair/Export 操作写审计日志。

---

## 23. 测试设计

### 23.1 单元测试

- Checked Arithmetic；
- ABI Size/Align/Offset；
- Handle Resolver、Region ID/UUID/Epoch 和 Generation 回绕拒绝；
- Slab Header CRC、状态和 Owner Transaction；
- Bitmap Allocator；
- Ring 回绕；
- Schema Parser/Validator/Layout、显式 Field ID、Reserved 和递归拒绝；
- Wire Encoder/Decoder、Unknown Field Set 和 Passthrough；
- Segment/Record Parser；
- 配置校验。

### 23.2 并发测试

- SPSC 长时间回绕；
- MPSC 1/2/8/32/128 Publisher；
- Broadcast 1/2/8/16 Subscriber；
- 慢 Subscriber；
- Publisher 在 Reserve、Build、Commit 各点暂停或 Kill；
- Subscriber 在 Borrow 和 ACK 之间 Kill；
- Lease 过期与误判边界；
- Allocator 高竞争和 Generation 复用。

### 23.3 集成测试

- 不同虚拟地址 Attach；
- 本地静态和动态 Pub/Sub；
- TCP Bridge 双节点；
- 动态 Schema 按需分发和 Short ID 碰撞路径；
- 多目标 Delivery Receipt 的 ALL/ANY/QUORUM、Timeout 和部分成功；
- Bridge 去重窗口、Receiver 重启和 `kDegraded` 降级路径；
- 多 Publisher 单 Recorder Writer；
- Recorder 背压；
- Segment 恢复和回放；
- N/N-1 Schema 转换。

### 23.4 Fuzz

- IDL 文本；
- Descriptor；
- Network Frame；
- Canonical Payload；
- Segment Record；
- SHM Handle 和 Header Validator。

Fuzz Harness 必须设置内存与时间上限，并保存最小化 Corpus。

### 23.5 形式化与内存模型验证

- 使用 TLA+/PlusCal 建模 MPSC Reservation、Broadcast Membership、Lease Eviction 和 ACK/Reclaim；
- 使用 C++ Litmus Test 验证 Acquire/Release 和 CAS Memory Order；
- 使用小位宽 Sequence/Generation 强制高频回绕；
- 使用随机 Scheduler 和确定性 Fault Point 重放失败交错；
- 模型不变量与第 25 章关键不变量保持对应。

### 23.6 故障注入

故障模型（2.1）与测试条目的覆盖矩阵：

| 2.1 故障类型 | 测试条目 |
|---|---|
| 线程异常退出 | 关键线程（Writer、Bridge I/O、Heartbeat）注入 `std::terminate`/异常退出，验证进程级兜底与资源不泄漏 |
| 进程正常退出/SIGKILL/长时间暂停 | 任意状态点 SIGKILL；进程在正常退出路径验证 clean_shutdown 标记；Publisher 在 Reserve、Build、Commit 各点暂停（SIGSTOP/SIGCONT）与 Kill |
| PID 复用 | 快速重启同 PID 进程，验证 ProcessIdentity（含 start_time/epoch）区分新旧实例 |
| 主机重启和掉电 | 掉电注入（或 fsync 语义验证套件）：fdatasync 后断电恢复，验证 Segment Commit 边界；Region clean_shutdown 检测 |
| 网络断开、半开、重复、乱序和重连 | 网络断开、半开连接（对端无响应）、半包、重复和乱序注入 |
| 文件系统 ENOSPC、EIO、只读切换和短写 | 磁盘写短、ENOSPC、EIO、只读切换、磁盘降速（cgroup/io-throttle） |
| 共享内存及磁盘数据损坏 | Region Header、Slab Header、Payload 和 Record CRC 损坏注入，验证隔离而非越界 |
| Coordinator/Schema Registry/Recorder 暂时不可用 | Coordinator 暂时不可用；Schema Registry 不可用（缓存续用、新注册拒绝）；Recorder 暂停与崩溃（含强一致 Topic 的阻塞行为） |

其他故障注入项：

- Schema Store 写入失败；
- 时钟跳变；
- 两个安全域 Attach 同一 Region 被权限策略拒绝；
- Receipt Table、Dedup Cache 和 Schema Ref Table 耗尽。

### 23.7 性能测试

报告：

- p50/p95/p99/p99.9/max；
- 吞吐；
- CPU、Context Switch、Cache Miss；
- 内存和内部碎片；
- 动态与静态 Schema 差异；
- Recorder 入队与落盘速率；
- Buffer 耗尽时间；
- Batch 和 Sync 延迟；
- Telemetry Off、Counters-only、1% Sampling 和 Full Debug 的开销对比；
- Counter 准确性、Histogram 分位数误差和 Sidecar Buffer 满场景；
- 时钟同步正常、Uncertainty 超限、Clock Jump 和负延迟样本处理；
- Exporter 阻塞或失败时数据路径不受影响；
- NUMA 和绑核信息。

### 23.8 Hermetic CodeGen 测试

- 不同工作目录生成结果一致；
- 不同机器和并行度生成结果一致；
- 未声明 Import 导致 Bazel Action 失败；
- 绝对路径、当前时间和环境变量不进入输出；
- Canonical Schema、Digest 和生成源码具有 Golden Test。

---

## 24. 分阶段代码交付

D 系列与架构文档 P0~P6 路线图的对应关系：D0 完成 P0 的 ADR 交付部分；D1~D2 对应 P1；D3 对应 P2；D4 对应 P3；D5 对应 P4；D6 覆盖 P5 与 P6。每个 D 阶段的完成判定必须同时满足对应 P 阶段的退出条件。UDP/RDMA/Fabric 驱动归属 P5（性能优化与平台扩展）；Topic Partition 归属 P5（单 Writer 达瓶颈后）。

### D0：工程骨架与 ADR

- Bazel/Bzlmod；
- C++ Toolchain；
- Formatting、Lint、Sanitizer 和 CI；
- ABI、Channel、Schema、Storage 关键 ADR。

### D1：SHM 基础

- Status/Result；
- SHM 通用 MPMC 骨架（9.9）；
- Platform SHM；
- SuperBlock/Region；
- Handle Resolver；
- Slab Allocator；
- Inspector。

### D2：Channel 与 Runtime

- SPSC；
- MPSC；
- Broadcast；
- Lease/Recovery；
- 静态 Publisher/Subscriber；
- Kill Tests。

### D3：Schema

- C++ Compiler Core；
- Descriptor/Layout；
- Dynamic Builder/View；
- `minoc`；
- Bazel Rule；
- Static CodeGen。

### D4：Bridge

- Registry/Route；
- Wire Frame；
- TCP Driver；
- Schema 协商；
- 双节点测试。

### D5：Storage

- Schema Store；
- Segment/Record；
- Recorder Buffer；
- Per-Topic Writer；
- Recovery/Replay；
- Storage Tools。

### D6：优化与产品化

- NUMA；
- Batch；
- UDP/RDMA/Fabric Driver（含 IPCF 类共享窗口扩展）；
- Topic Partition；
- Security；
- Capacity Planning；
- 运维工具和演练。

---

## 25. 关键不变量

实现和评审必须持续验证以下不变量（INV-01~INV-32，供测试与验收条款引用）：

1. **INV-01** READY Slot 引用的 Payload 已完整构建并可见；
2. **INV-02** 任意有效 Borrow 或存活 Pin 存在时 Payload 不会复用；
3. **INV-03** Handle Resolver 不返回 Region 边界外地址；
4. **INV-04** Generation、Sequence 和 Epoch 分别承担明确的复用检测职责；
5. **INV-05** Broadcast 的有效 Subscriber 都拥有独立 Cursor；
6. **INV-06** Work Queue 不伪装成 Broadcast；
7. **INV-07** 动态 Schema 在数据路径不重复编译；
8. **INV-08** Static 和 Dynamic Path 使用同一 Canonical Wire 语义；
9. **INV-09** 网络、Fabric 与磁盘数据不包含本机 Offset；
10. **INV-10** 多 Publisher 的来源身份和来源 Sequence 不丢失；
11. **INV-11** 一个活动 Segment 只有一个逻辑 Writer；
12. **INV-12** 内存缓冲总量有明确上限；
13. **INV-13** `BUFFERED` 不被报告为 `DURABLE`；
14. **INV-14** Schema 先于引用它的 Durable Record 落盘；
15. **INV-15** 索引可以重建，已提交 Record 才是存储真实性来源；
16. **INV-16** Base Slot 不混合 Broadcast ACK、Work Queue Claim 或 MPSC Reservation 语义；
17. **INV-17** MPSC 失效 Reservation 最终转为可推进的 ABORTED/Tombstone，不永久阻塞队列；
18. **INV-18** Broadcast ACK 责任绑定 Subscriber Generation，不受 ID 复用影响；
19. **INV-19** Handle Generation 不回绕，Region ID 不复用，Attach 校验 Region UUID/Epoch；
20. **INV-20** Short Schema ID 碰撞不能导致 Descriptor 混淆，完整 Digest 是最终身份；
21. **INV-21** 生产 Schema 的 Field ID 显式、稳定且删除后 Reserved；
22. **INV-22** Unknown Field 的保留或丢弃行为明确且有界；
23. **INV-23** Recorder Ingestion Sequence 不留下无法解释的永久空洞；
24. **INV-24** Descriptor 和 Session Schema Ref Table 先于引用 Record Durable；
25. **INV-25** Region 和 Recording Session 在任一时刻最多一个有效 Recovery/Writer Owner；
26. **INV-26** Delivery Receipt 的目标集合和完成策略在 Publish 时冻结；
27. **INV-27** 阻塞操作都受 Deadline 或取消控制；
28. **INV-28** 同一 RW Region 的参与进程属于同一 Trust Domain；
29. **INV-29** Telemetry Buffer 或 Exporter 饱和不会阻塞或改变业务消息语义；
30. **INV-30** 跨节点单向延迟只在 Clock Quality 满足阈值时进入正常 Histogram；
31. **INV-31** 任何损坏输入在解引用或分配前被限制和校验；
32. **INV-32** Pin 计数不内嵌 Base Slot；进程失效后其 Pin 份额最终被 Recovery 清除，不永久阻塞 Reclaim。

---

## 26. 决策与验证登记表

下表汇总待原型验证事项；决策状态以对应 ADR 为准。Owner 列在团队例会分配后填写；目标关闭阶段为完成验证并更新 ADR 状态的最后期限。

| ID | 优先级 | 事项 | 关联 ADR | Owner | 目标关闭阶段 | 验证产物 |
|---|---|---|---|---|---|---|
| V-01 | P0 | Handle v1 布局 | ADR-0002 | | P0 | 128 位 Handle 空间/性能、Generation 回绕和 Region ID 持久分配报告 |
| V-02 | P0 | IndexSlot 最终尺寸 | ADR-0003 | | P0 | Cache/吞吐 Benchmark |
| V-03 | P0 | MPSC 算法与 Producer Kill 恢复 | ADR-0003 | | P0 | TLA+ 模型、实现 ADR 和 Kill Test |
| V-04 | P0 | Broadcast Membership/ID 复用 | ADR-0003 | | P0 | Generation 协议和竞态测试 |
| V-05 | P0 | Canonical Wire Format v1 | ADR-0004 | | P0 | ADR、Golden Vector、性能和 Fuzz 对比 |
| V-06 | P0 | Dynamic SHM Layout v1 | ADR-0005 | | P0 | Layout、Unknown Field、Object Graph Recovery Test |
| V-07 | P0 | IDL Field Identity | ADR-0011 | | P0 | 显式 ID、Reserved、Import 闭包和递归拒绝测试 |
| V-08 | P0 | Schema ID 规范化 | ADR-0005 | | P0 | Golden Vector、完整 Digest 与碰撞策略 |
| V-09 | P0 | Delivery/Ack/Receipt | ADR-0006 | | P0 | 故障保证矩阵、多目标策略、有界 Receipt Table 和端到端测试 |
| V-10 | P0 | Segment Commit Protocol | ADR-0007 | | P0 | 任意字节 Kill、目录 Sync 和恢复测试 |
| V-11 | P0 | Region Recovery Ownership | ADR-0003 | | P0 | 双恢复者和 Owner Kill 测试 |
| V-12 | P0 | Linux x86-64 原子 ABI 基线 | ADR-0001 | | P0 | Litmus、Lock-free 和跨进程报告 |
| V-13 | P1 | Linux AArch64 支持 | ADR-0001 | | P4 | 与 x86-64 相同的 ABI、Litmus 和性能矩阵 |
| V-14 | P1 | ACK Bitmap 最大 Subscriber 数 | ADR-0003 | | P1 | 内存和延迟 Benchmark |
| V-15 | P1 | Dynamic View 开销 | ADR-0005 | | P2 | Static/Dynamic 对比 |
| V-16 | P1 | Writer 线程模型 | — | | P4 | 1～100 Topic Benchmark |
| V-17 | P1 | Sync 默认策略 | ADR-0007 | | P4 | 延迟与持久性报告 |
| V-18 | P1 | Recorder Buffer 默认容量 | ADR-0008 | | P4 | 磁盘抖动模型与容量公式 |
| V-19 | P1 | Bridge 重传/去重窗口 | ADR-0006 | | P3 | 断线、Receiver 重启、重复和缓存耗尽测试 |
| V-20 | P1 | SHM Trust Domain | ADR-0010 | | P6 | Region 权限、跨安全域 Proxy 和威胁模型评审 |
| V-21 | P1 | 节点级 Admission Control | — | | P3 | 全模块资源预算测试 |
| V-22 | P1 | Rollout/Cutover | — | | P6 | 新旧 Region 双版本演练 |
| V-23 | P1 | Telemetry 开销与准确性 | ADR-0009 | | P4 | Off/Counter/Sampled 对比、Clock Quality 和 Histogram 校验 |
| V-24 | P2 | Topic Partition 阈值 | — | | P5 | 单 Writer 饱和曲线 |
| V-25 | P2 | UDP/RDMA/Fabric | ADR-0012 | | P5 | 驱动专项设计与硬件验证 |
| V-26 | P1 | 通用 MPMC 骨架（9.9） | ADR-0003 | | P1 | 跨进程守恒/回绕/满空判定测试、Init/Attach 协议测试，依赖 V-12 原子 Litmus；进程内算法原型已过守恒与 TSAN |
| V-27 | P1 | 对象级引用 Pin（11.2.1） | ADR-0013 | | P1 | Pin 热路径开销 Benchmark、Lease 失效清除时延、Pin 耗尽阻塞模型、Recovery 清除竞态测试 |

---

## 27. 评审检查清单

### ABI

- 是否使用固定宽度字段？
- 是否有显式 Alignment/Padding？
- 是否有 Size/Offset Static Assert？
- 是否存在裸指针、STL 容器或虚表？
- Offset 和 Length 是否 Checked？

### 并发

- Owner 是谁？
- 发布和可见性的同步点是什么？
- 崩溃时谁接管？
- 是否可能提前回收？
- 是否区分队列回绕和 Slot 复用？
- 是否会无限等待失效进程？
- Base Slot 是否避免混合 ACK、Claim、Refcount 和 Reservation？
- Reservation Owner、Lease、Generation 和 Tombstone 是否闭环？
- Handle/Region ID 是否遵守不回绕、不复用和 Attach Epoch 校验？
- 所有阻塞操作是否有 Deadline/取消？

### Schema

- 是否在控制路径完成编译？
- Descriptor 是否不可变和可持久化？
- 是否限制输入复杂度？
- 静态和动态语义是否一致？
- 是否比较完整 Canonical Digest 而非只比较 64 位 Short ID？
- Dynamic Object Graph 是否可完整 Abort/Recover？
- Field ID 是否显式、稳定并正确 Reserved？
- Unknown Field 是否有界保留或明确丢弃？

### Delivery

- Receipt Target Snapshot 是否在 Publish 时冻结？
- ALL/ANY/QUORUM 和逐目标状态是否明确？
- Timeout 是否只停止等待而不错误撤销消息？
- Receipt/Dedup/ACK Table 是否有界？

### Storage

- 是否复制后及时释放 Borrow？
- Buffer 是否有界？
- Queue Full 行为是否明确？
- ACK Level 是否准确？
- 是否存在多个 Writer 修改同一 Segment？
- Schema 是否先于 Record Durable？
- Commit Marker、CRC、`fdatasync`、Rename 和父目录 `fsync` 是否按协议执行？
- Reader Pin 是否阻止 Retention 提前删除？

### Telemetry

- 是否区分节点内阶段耗时、RTT 和跨节点单向延迟？
- Clock Quality 不达标时是否停止上报单向延迟？
- Counter、Histogram 和 Trace Buffer 是否固定容量且无热路径全局锁？
- Exporter 阻塞时是否只丢 Telemetry 而不阻塞业务？
- Label 是否避免 Trace ID、Publisher ID 等高基数值？

### 故障与安全

- 任意 Kill 点能否恢复？
- 损坏输入是否在分配前检查？
- 权限和资源上限是否明确？
- 指标能否发现积压、泄漏和降级？
- 是否明确区分进程崩溃、主机掉电、网络断线和磁盘故障？
- Recovery/Writer 是否存在 Split-brain 风险？
- 所有 RW Attach 参与者是否属于同一 Trust Domain？
