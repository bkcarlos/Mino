# Mino 架构设计文档

> 零拷贝共享内存与跨机通信统一框架  
> Unified Framework for Zero-Copy Shared Memory and Cross-Node Communication  
> **Mino = Minimal Overhead**  
> 版本：v0.14（设计评审稿，按文档审查意见修订；补充 Fabric 扩展、双路由策略、MPMC 骨架与引用 Pin）  
> 状态：概念设计完成；动态 Schema、数据存储、故障语义与传输性能指标方案已补充，关键协议待原型验证

## 1. 文档概述

### 1.1 背景

高性能系统通常同时面临两类通信需求：

- 同一节点内的跨进程通信（IPC）；
- 不同节点之间的网络通信。

传统 IPC 方案通常采用 UNIX Domain Socket、管道或消息队列，并配合 Protobuf 等序列化协议。这类方案实现成熟，但即使通信双方位于同一节点，仍可能产生序列化、内核态切换和数据复制开销。

另一方面，面向极致性能设计的共享内存方案虽然可以降低同机通信延迟，却通常与业务数据结构强耦合，难以直接支持跨机传输、协议演进和多语言接入。

Mino 通过统一的类型系统、发布订阅 API 和数据生命周期模型，将同机 IPC 与跨机网络通信整合为一套统一通信框架：

- 本地节点：传递共享内存偏移量，消费者原位读取 Payload；
- 远程节点：Bridge 提取有效字段，编码成紧凑网络数据后发送；
- 应用层：使用相同的 IDL 类型和 Publisher/Subscriber API，不感知底层路径差异。

### 1.2 文档目的

本文档定义 Mino 的：

- 总体架构和组件边界；
- 共享内存布局与寻址方式；
- Index RingBuffer 控制协议；
- Slab 动态内存池；
- IDL 与动态数据结构表示；
- 本地及跨机数据流程；
- 并发、所有权与内存回收模型；
- 故障恢复、安全和可观测性要求；
- 性能验证方法与实施路线；
- 动态 Schema、数据录制与回放的总体边界；
- 模块接口、状态机、线程模型、Schema 和 Storage 文件格式等工程约束（见《Mino 详细设计文档》）。

### 1.3 适用场景

- 车载智驾域控制器；
- 高频交易及低延迟行情系统；
- 实时传感器数据总线；
- 音视频或点云数据管线；
- 单机多进程高吞吐计算平台；
- 需要同时覆盖 IPC 和跨机通信的实时系统。

---

## 2. 设计目标与边界

### 2.1 设计目标

1. **本地零拷贝**

   Payload 写入共享内存后，消费者通过相对偏移量直接访问，不再复制完整业务数据。

2. **确定性内存分配**

   通过固定尺寸 Class 和原子位图，将常见 Slab 分配与回收控制在 O(1)。

3. **支持动态数据结构**

   支持有明确容量上限的 `string`、`vector` 和嵌套领域对象，并提供受控的原位构建接口。

4. **跨机透明传输**

   Transport Switcher 根据目标 Node ID 自动选择共享内存或网络通道。

5. **协议可演进**

   共享内存布局、消息 Schema 和网络帧均包含版本、长度和必要的校验信息。

6. **故障可恢复**

   对 Publisher、Subscriber 或 Bridge 异常退出提供检测、资源回收和恢复机制。

7. **可观测和可运维**

   暴露队列水位、分配失败、订阅延迟、Bridge 积压和协议错误等指标。

8. **运行时动态 Schema**

   支持在控制路径注册、校验和缓存动态 Schema，并通过 Dynamic Builder/View 访问未知的运行时类型。

9. **消息录制与回放**

   支持 Canonical Payload、Schema Descriptor 和必要元数据的持久化，提供有界内存缓冲、顺序落盘、恢复和回放能力。

### 2.2 非目标

- 不支持将任意 C++ 对象直接放入共享内存；
- 不允许共享对象包含裸指针、虚函数表或进程私有句柄；
- 不将共享内存镜像作为跨 CPU 架构的网络格式；
- 第一阶段不提供无限容量容器或通用垃圾回收器；
- 不在首版中提供通用消息队列产品、分布式一致性日志、Exactly-once 或跨 Recorder 的持久化可靠消息系统；
- 首版存储能力定位为 Schema 持久化、消息录制与回放，不等同于分布式消息数据库；
- 不允许多个磁盘 Writer 并发修改同一个活动 Segment；
- 首版不提供 Coordinator/Recorder 自动主备，不承诺跨节点 Exactly-once；
- 首版正式平台基线为 Linux x86-64，其他平台通过原子 ABI 验证后启用；
- 不将未经目标硬件验证的性能预估视为正式 SLA。

### 2.3 核心约束

| 约束 | 设计响应 |
|---|---|
| 不同进程的共享内存映射地址不同 | 所有共享引用采用相对偏移量或 Handle，禁止裸指针 |
| 发布者和订阅者并发执行 | 使用明确的状态机和 acquire-release 内存序 |
| 进程可能异常退出 | 使用心跳、租约、恢复扫描或 robust 同步机制 |
| 编译器和语言 ABI 可能不同 | 固定宽度类型、显式对齐；网络表示独立于内存布局 |
| 共享内存容量有限 | 为 Topic 定义容量、背压和丢弃策略 |
| 动态容器可能增长 | IDL 必须声明最大容量，或使用受控的大对象策略 |
| Producer 预留后可能崩溃 | Reservation 绑定 Owner Lease，失效后转换为 ABORTED/Tombstone |
| Subscriber ID 可能复用 | ACK 责任绑定 Subscriber Generation 和发布时订阅集合快照 |
| 短 Schema ID 可能碰撞 | 64 位 ID 只作热路径索引，完整 Canonical Digest 用于最终校验 |
| 磁盘和网络确认强度不同 | 明确 Local、Remote Accepted、Buffered、Written、Durable 阶段 |
| 各模块容量单独合法但总和过大 | 创建 Topic 前执行节点级资源 Admission Control |

---

## 3. 总体架构

### 3.1 逻辑架构

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    Application Layer                                │
│             Publisher / Subscriber / Business Logic                 │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                 Static & Dynamic Schema Layer                       │
│ Generated Types / Dynamic View / Validator / Encoder / Decoder      │
│ Runtime Schema Compiler / Schema Registry                           │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ Unified Pub/Sub API
                    ┌───────────▼───────────┐
                    │ Transport Switcher    │
                    │ Topic & Node Routing  │
                    └────────┬────────┬─────┘
                       Local │        │ Remote
             ┌───────────────▼─┐   ┌──▼────────────────────┐
             │ SHM Channel      │   │ Bridge / Net / Fabric │
             │ Index RingBuffer │   │ Frame + Codec         │
             │ Central Slab     │   │ TCP/UDP/RDMA/Fabric   │
             └─────────┬────────┘   └──────────┬────────────┘
                       │                       │
                 Recorder Subscriber           │
                       │                 Remote SHM Channel
                 Buffer + TopicWriter
                       │
                Segment / Replay
```

### 3.2 核心设计理念

系统采用控制流与数据流分离的设计。

**控制流（Control Path）**

- 使用固定尺寸 Index RingBuffer；
- Slot 按 64 字节 Cache Line 对齐；v1 字段布局约 80 字节，加显式 Padding 后为 128 字节（两个 Cache Line），见 ADR-0003 与详细设计 9.2；
- 只传递消息类型、序列号、时间戳、Payload Handle 和状态；
- 不在 RingBuffer 内复制大型 Payload。

**数据流（Data Path）**

- Payload 存储在 Central Slab Pool；
- 发布者在共享内存中直接构造数据；
- 本地消费者根据 Handle 原位访问；
- Bridge 只提取有效字段并编码为网络格式；
- Recorder 将消息编码成 Canonical Payload 后复制到自身有界内存池，并异步顺序落盘。

### 3.3 组件职责

| 组件 | 主要职责 | 不承担的职责 |
|---|---|---|
| Application | 构造、发布和消费领域消息 | 不操作裸 Offset，不选择传输驱动 |
| Generated IDL Types | 静态字段访问、容量检查、结构校验和编解码 | 不管理全局路由 |
| Schema Compiler/Registry | 解析、校验、布局规划、动态 Schema 注册与 Descriptor 持久化 | 不在消息热路径重复编译 Schema |
| Dynamic Schema Runtime | Dynamic Builder/View、字段句柄和动态编解码 | 不运行时生成并加载 C++ 机器码 |
| Transport Switcher | 根据 Node、Topic 和策略选择通道 | 不解释业务字段 |
| SHM Channel | 索引入队、Handle 解析和消费协调 | 不处理网络分片 |
| Central Slab | Class 选择、分配、回收和统计 | 不决定消费语义 |
| Bridge | 编解码、网络/Fabric 收发和远端对象重建 | 不传输本机 Offset |
| Registry/Coordinator | 节点发现、订阅者租约、Topic 元数据 | 不直接处理 Payload |
| Recorder/Storage | Canonical Encode、有界缓冲、Per-Topic 顺序写盘、恢复与回放 | 不持久化本机 Offset，不提供分布式一致性日志 |

### 3.4 实现与构建约束

- Runtime、Schema Compiler、`minoc` 和 `mino` 运维工具统一使用 C++；
- 项目使用 Bazel + Bzlmod，固定 Bazel/Bazelisk 版本；
- 通用 MPMC 骨架（详设 9.9）独立为可复用组件：SHM Index Ring 的并发骨架，槽位携带 Handle 引用而非数据本体，供各 Channel 语义共享；
- Schema Compiler 拆分为 Parser、Semantic、Descriptor、Layout、Compatibility、Runtime Compiler 和 CodeGen Targets；
- Runtime 可以依赖 Runtime Compiler，但不依赖 C++ CodeGen；
- `minoc` 复用同一 Compiler Library，并通过 Bazel 自定义 Rule 生成静态类型代码；
- 关键协议决策记录在 `docs/adr/`，按 PROPOSED、ACCEPTED、VALIDATED、FROZEN 状态推进。

---

## 4. 共享内存布局与寻址

### 4.1 区域布局

```text
SharedMemoryRegion
  +0x0000  SuperBlock
            magic
            layout_version
            region_size
            byte_order
            feature_flags
            region_uuid      // 128 位，创建时随机生成
            region_epoch
            clean_shutdown

  +0x1000  Channel Directory
            topic_id -> ring_offset
            channel_type
            capacity
            subscriber_set
            backpressure_policy

  +0x....  Allocator Metadata
            class descriptors
            atomic bitmaps
            recovery metadata

  +0x....  Index RingBuffers
            producer cursors
            subscriber cursors
            index slots

  +0x....  Slab Data Region
            64 B Class
            256 B Class
            2 KiB Class
            64 KiB Class
```

进程 Attach 共享内存时必须验证：

- Magic；
- Layout Version；
- 区域大小；
- 字节序；
- 页大小和对齐要求；
- Feature Flags；
- Region UUID（128 位，与 Attach Context 中登记的部署身份一致）；
- Region Epoch。

验证失败时必须拒绝 Attach，不能继续解释现有内存。

### 4.2 共享内存 Handle

仅使用 `uint32_t offset` 虽然简单，但无法检测 Slot 被回收并复用后的陈旧引用。建议采用包含代数的 Handle：

```cpp
struct alignas(8) ShmHandle {
    uint64_t offset;       // 相对共享内存基址的字节偏移
    uint32_t generation;   // Slab Slot 每次复用时递增
    uint32_t region_id;    // Registry 持久分配且永不复用
};
static_assert(sizeof(ShmHandle) == 16);
```

解引用前必须检查：

1. `region_id` 有效；
2. `offset` 非空且满足对齐要求；
3. `offset + object_size` 未发生整数溢出；
4. 对象完整位于数据区；
5. Handle 的 `generation` 与 Slab Header 一致；
6. Region ID、Region UUID 和 Region Epoch 与当前 Attach Context 一致；
7. 对象类型和 Schema Version 与预期相符。

Region ID 在一个部署身份域内永久不复用。Generation 回绕前必须 Drain 并迁移相关 Region/Class，不能静默复用。该 Handle v1 布局在 ABI 冻结前仍需性能验证（见 ADR-0002，状态 PROPOSED）。

---

## 5. 控制面协议：Index RingBuffer

### 5.1 Index Slot

```cpp
enum class SlotState : uint32_t {
    FREE      = 0,
    RESERVED  = 1,
    WRITING   = 2,
    READY     = 3,
    ABORTED   = 4,
    RETIRED   = 5
};

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

struct MpscReservationMeta {
    uint64_t owner_process_epoch;
    uint64_t owner_publisher_id;
    uint64_t reservation_timestamp_ns;
    uint64_t reservation_sequence;
};
```

Base Slot 只包含不可变消息元数据和状态。Broadcast、Work Queue、MPSC 使用独立、同索引的 Sidecar Metadata Array，禁止使用 `mask_or_refcount` 等模糊字段。`immutable_metadata_crc` 只覆盖类型、Schema、Sequence、Timestamp、Payload Handle 和 Payload Length，不覆盖 State、ACK、Claim 或 Lease 等可变字段。

Slot 应保持固定尺寸，并按 64 字节 Cache Line 对齐。v1 字段布局约 80 字节，加显式 Padding 后固定为 **128 字节（两个 Cache Line）**，通过 `static_assert` 固定大小和对齐。

### 5.2 发布内存序

发布采用「先构建 Payload，后预留 Slot」的顺序，使 Slot 占用窗口最小化：

1. 在 Slab 中构建并校验 Payload（构建协议见第 6 章）；
2. 预留 Slot，写入 Reservation Owner Metadata，将 Slot 从 `FREE` 切换为 `RESERVED`；
3. 将状态切换为 `WRITING`，填写 Slot 普通字段（Handle、类型、Sequence、Timestamp）和订阅责任；
4. 使用 `store(memory_order_release)` 将状态设为 `READY`；
5. 预留后、提交前发生失败或 Owner 失效时，提交 `ABORTED` Tombstone。

注意：Payload 构建发生在 Slot 预留**之前**。进程在预留前崩溃只产生孤儿 Slab（由恢复扫描回收，见第 12 章），不会在 Ring 中留下空洞；MPSC Producer Crash 恢复（5.5 节）只覆盖「预留后、READY/ABORTED 提交前」的窄窗口。

消费者使用 `load(memory_order_acquire)` 观察 `READY`，从而保证 Payload 和 Slot 元数据对当前线程可见。

### 5.3 队列拓扑

| 模式 | 语义 | 推荐实现 |
|---|---|---|
| SPSC | 单生产者、单消费者 | 单 Head/Tail |
| MPSC | 多生产者、单消费者 | 每 Slot Sequence，CAS 预留 |
| SPMC 广播 | 每个订阅者都收到消息 | 每订阅者独立 Cursor |
| 竞争消费 | 多消费者中只有一个处理消息 | 共享消费 Cursor |
| MPMC | 多生产者、多消费者 | SHM 通用 MPMC 骨架：per-slot Sequence + 游标 CAS 仲裁，Handle 引用语义（见详设 9.9） |

### 5.4 SPMC 语义修正

原始方案同时使用：

- 单一全局 `head`；
- Slot 引用计数；
- 多订阅者描述。

这不足以实现可靠的 SPMC 广播。若多个订阅者共享一个 `head`，它们更接近竞争消费，而不是每个订阅者都收到消息。

正式设计必须将两种 Channel 分开：

- **Broadcast Channel**：每个订阅者维护独立 Cursor；
- **Work Queue Channel**：消费者竞争同一个 Cursor，每条消息只处理一次。

不能使用同一个模糊的数据结构同时表达两种语义。

### 5.5 MPSC Producer Crash

MPSC Slot 使用 `FREE → RESERVED → WRITING → READY/ABORTED → RETIRED` 状态机。Reservation 必须记录 Owner Identity、Owner Epoch、Sequence 和时间。Consumer 遇到空洞时先验证 Owner Lease，不能只按超时误判；Owner 确认失效后由 Recovery CAS 为 ABORTED Tombstone，使队列可以继续推进并回收构建事务。

### 5.6 Broadcast Membership

发布时对有效 Subscriber Set 建立不可变 Snapshot。ACK 责任绑定 `subscriber_id + subscriber_generation`；注册使用 `join_sequence` 作为起点，注销先禁止新 Borrow，再清理对应 Generation 的 ACK。固定 ACK Bitmap 达到上限时拒绝新 Subscriber 或切换其他回收方案。

---

## 6. Central Slab 动态内存池

### 6.1 Class 划分

初始 Class 可配置为：

| Class | 容量示例 | 典型用途 |
|---|---:|---|
| Class 0 | 64 B | 短控制消息、短字符串 |
| Class 1 | 256 B | 状态消息、小型 Vector |
| Class 2 | 2 KiB | 轨迹、批量点数据 |
| Class 3 | 64 KiB | 图像块、大型点云 |

具体 Class 应根据实际消息尺寸分布和内存预算确定，而不是固定写死。

### 6.2 Slab Header

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

### 6.3 分配过程

分配必须保证「位图、Generation、Header」三者的发布顺序可恢复，协议如下：

1. 根据请求大小选择最小可容纳 Class；
2. 在该 Class 位图中查找空闲位；
3. 通过 CAS 将空闲位切换为占用；
4. 递增 Generation Array 中该 Slot 的代数，并复制到 Header `generation`；
5. 写入 Header 其余字段（Owner、Transaction、Type、Schema 等）；
6. 以 `object_state.store(ALLOCATED, memory_order_release)` 作为分配完成的唯一发布点；
7. 返回 `ShmHandle`。

崩溃恢复约定：位图已占用但 `object_state` 不是合法已发布状态的 Slot，其 Generation 从未对外发布过，恢复扫描可安全回收（见第 12 章）。`ALLOCATED` 之前的任何中间状态都不携带有效 Handle，不产生 ABA 风险。

伪代码（仅示意，错误处理见详细设计 8.2）：

```cpp
Result<ShmHandle> CentralSlabAllocator::Allocate(const AllocationRequest& req) {
    const uint8_t class_id = SelectClass(req.object_size);
    const int slot_idx = FindAndSetFreeBit(class_id);

    if (slot_idx < 0) {
        return Status(StatusCode::kResourceExhausted, "class exhausted");
    }

    SlabHeader* header = GetHeader(class_id, slot_idx);
    header->class_id = class_id;
    header->generation = NextGenerationOrDrain(class_id, slot_idx);
    header->capacity = GetClassCapacity(class_id);
    header->object_size = 0;
    header->type_id = 0;
    header->layout_version = 0;
    header->schema_short_id = 0;
    header->owner_epoch = CurrentProcessEpoch();
    header->allocation_transaction_id = CurrentTransactionId();
    header->immutable_header_crc = 0; // Publish 前在字段冻结后计算
    // 分配完成的唯一发布点：此前进程崩溃时，Recovery 将该 Slot 视为未发布并回收
    header->object_state.store(ALLOCATED, std::memory_order_release);

    return MakeHandle(class_id, slot_idx, header->generation);
}
```

### 6.4 回收过程

回收分两个阶段，RETIRED 只表示「不再产生新 Borrow」，不表示「所有 Reader 已完成」：

1. 校验 Handle、Generation 和 Header；
2. 将对象状态切换为 `RETIRED`（禁止新 Borrow）；
3. 等待所有现存 Borrow 释放；
4. 清理必要的对象图或子 Slab；
5. `Reclaim`：通过 CAS 清除位图，Slot 回到 FREE；
6. 更新 Class 使用率和回收统计。

恢复扫描看到 `RETIRED` 状态时，必须确认不再存在有效 Borrow 后才能执行 `Reclaim`。

### 6.5 工程注意事项

- 位图本身可能成为高并发热点，可采用分片位图或每核缓存；
- 固定 Class 消除了外部碎片，但仍存在内部碎片；
- NUMA 系统应按节点划分内存池，并尽量本地分配；
- 超大对象建议使用专用大页池、DMA Buffer 或受控分段表示；
- 如果动态字段可以分配独立 Slab，必须生成对象图遍历和递归回收逻辑。

---

## 7. IDL 与动态数据结构

### 7.1 IDL 示例

```proto
syntax = "v1";
package autonomous;

struct Point3D {
    float x = 1;
    float y = 2;
    float z = 3;
}

message SensorFrame {
    uint32 frame_id = 1;
    string device_name = 2 [max_bytes = 64];
    vector<Point3D> points = 3 [max_capacity = 100];
    reserved 4, 10 to 15;
}
```

### 7.2 CodeGen 输出

IDL 编译器应生成：

- 共享内存对象布局；
- Mutable Builder；
- 只读 Accessor；
- String/Vector 边界检查；
- 对象结构 Validator；
- 网络 Serialized Size 计算；
- Canonical Encoder/Decoder；
- Schema Version 和 Type ID；
- 对象图回收代码；
- 调试打印和反射元数据；
- 可独立持久化的 Schema Descriptor 和 Layout Plan。

### 7.3 动态字段表示

```cpp
struct ShmStringMeta {
    ShmHandle data;
    uint32_t length;
    uint32_t capacity;
};

struct ShmVectorMeta {
    ShmHandle data;
    uint32_t size;
    uint32_t capacity;
    uint32_t element_size;
};
```

动态字段不得保存：

- 进程内裸指针；
- `std::string`；
- `std::vector`；
- 进程私有分配器状态；
- 未显式定义 ABI 的复杂 C++ 类型。

`bytes` 复用 `ShmStringMeta` 布局但不施加 UTF-8 约束；嵌套动态对象字段使用 Handle + Size Meta。`element_size` 为冗余快查字段（元素尺寸也可从 Descriptor 获得），保留它是为了让 Validator 无需查询 Descriptor 即可做边界检查。

### 7.4 原位修改语义

建议采用：

```text
Mutable Builder → Validate → Publish → Immutable Borrowed Object
```

默认规则：

- 对象发布前，Builder 拥有独占写权限；
- 对象发布后，普通 Subscriber 只能读取；
- 更新消息时构建新对象并重新发布；
- 若确实需要发布后原位修改，应建立独立的共享状态协议和同步机制。

“零拷贝”不等于可以在多个进程之间无同步地修改同一对象。

### 7.5 动态 Schema

Mino 使用同一套 C++ Schema Compiler Library 同时服务 `minoc` 和 Runtime：

- `minoc` 在构建期生成强类型 C++ Builder/Accessor；
- Runtime Compiler 在控制路径解析 IDL 或加载 Descriptor；
- Runtime 为未知类型提供 `DynamicMessageBuilder` 和 `DynamicMessageView`；
- 数据热路径使用已缓存的不可变 Descriptor、Layout Plan 和 `FieldHandle`；
- 动态编译必须限制输入大小、字段数、递归深度、容器上限和编译时间；
- 禁止按消息重复解析 Schema，禁止运行时生成、编译并 `dlopen` C++ 代码。

Schema 使用 `short_schema_id + canonical_digest + schema_version + layout_version` 标识。64 位 Short ID 只用于热路径索引和已协商连接；注册、持久化和冲突检查必须比较完整 Canonical Digest。Digest 来自带算法版本的规范化 Schema，不能使用进程内注册序号。

### 7.6 网络与存储表示

网络编码和持久化编码必须与 C++ 共享内存布局解耦：

- 使用固定字节序；
- 不发送本机 Offset；
- 不发送未使用的 Capacity 空洞；
- 只发送有效 String 和 Vector 元素；
- 按 Schema 定义字段顺序或字段 ID；
- 对长度和元素数量设置协议上限；
- 支持新增可选字段和未知字段跳过；需要无损代理时使用有界 Unknown Field Set 或 Wire Passthrough；
- 首版 Canonical Wire 基线采用确定性 Tagged Format，固定字段排序、字节序、数值编码和资源上限；
- 相同 Schema 和逻辑值必须生成确定性字节序列。

---

## 8. 端到端数据流程

### 8.1 本地发布流程

```text
Publisher
   │
   ├─ 1. 从 Slab Pool 分配对象
   ├─ 2. 在共享内存中构建字段
   ├─ 3. 校验对象和动态字段边界
   ├─ 4. 在 RingBuffer 中预留 Slot
   ├─ 5. 填写 Handle、类型、序列号和订阅信息
   └─ 6. Release Store：Slot → READY
```

示例 API：

```cpp
Publisher<SensorFrame> pub =
    bus.CreatePublisher<SensorFrame>("sensor/front_lidar", policy);

auto builder = pub.Allocate();
builder->set_frame_id(1001);
builder->set_device_name("Front_Lidar_Node");
builder->points().push_back({1.0f, 2.0f, 3.0f});

pub.Publish(std::move(builder));
```

### 8.2 本地订阅流程

```text
Subscriber
   │
   ├─ 1. 根据自身 Cursor 定位 Slot
   ├─ 2. Acquire Load：检查 READY/Sequence
   ├─ 3. 校验 Handle、长度、类型和 Schema
   ├─ 4. region_base + offset 原位访问对象
   └─ 5. ACK / 更新 Cursor / 退出安全读取区
```

示例 API：

```cpp
Subscriber<SensorFrame> sub =
    bus.CreateSubscriber<SensorFrame>("sensor/front_lidar", policy);

sub.Poll([](BorrowedMessage<SensorFrame> msg) {
    Process(msg->frame_id(), msg->points());
});
```

`BorrowedMessage` 的生命周期表示消费者对 Payload 的借用。借用释放或 ACK 完成后，对象才可能进入回收流程。

### 8.3 多 Publisher Topic

同一个 Topic 可以由多个节点或进程发布。每个来源使用
`node_id + publisher_id + publisher_epoch + source_sequence` 标识并保证来源内顺序；不同来源之间不承诺真实全局事件顺序。Recorder 在接收时分配 `ingestion_sequence`，作为磁盘物理顺序。

本地多发布者通道使用 MPSC；跨节点 Publisher 由 Bridge 汇聚。CPU、GPU、磁盘和进程指标等场景默认采用多 Publisher、单逻辑磁盘 Writer。

### 8.4 跨机 Bridge 流程

```text
Local Publisher
      │
      ▼
Local Slab + RingBuffer
      │
      ▼
Bridge Subscriber
      │
      ├─ Validate
      ├─ Compact Encode
      ├─ Network Framing
      ▼
TCP / UDP / RDMA / Fabric
      │
      ▼
Remote Bridge
      │
      ├─ Validate Frame
      ├─ Allocate Remote Slab
      ├─ Decode/Rebuild Object
      ▼
Remote RingBuffer
      │
      ▼
Remote Subscriber
```

Bridge 的目标节点集合由 Topic 路由策略产生：自动发现（订阅驱动）或显式配置静态路由，见 9.2。

### 8.5 录制与回放流程

```text
Multi-Publisher Topic
      │
      ▼
Recorder Subscriber
      │ Validate + Canonical Encode
      ▼
Bounded Recorder Memory Pool
      │ MPSC Queue
      ▼
Per-Topic Single Writer
      │ Batch Write / Sync / Rotate
      ▼
Schema Store + Segment + Index
      │
      ▼
Replay Engine → Allocate New SHM Object → Normal Publish API
```

Recorder 必须复制数据到自身内存池后及时释放 `BorrowedMessage`，不能因磁盘变慢长期占用共享内存 Slab。详细接口、文件格式和恢复协议见《Mino 详细设计文档》。

---

## 9. 网络协议与 Transport Switcher

### 9.1 网络帧头

v1 最小网络帧头（字段宽度和编码以详细设计 16.2 与 ADR-0004 为准）：

```text
NetworkFrameHeader
  magic                  4 B
  protocol_version       2 B
  flags                  2 B（位定义见下）
  header_length          4 B
  topic_id               4 B
  msg_type               4 B
  connection_schema_ref  4 B
  schema_version         4 B
  layout_version         4 B
  source_node_id         8 B
  source_publisher_id    8 B
  source_publisher_epoch 8 B
  sequence_num           8 B
  timestamp_ns           8 B（发布端 Wall Clock，经时钟同步）
  payload_len            4 B
  header_crc             4 B
  payload_crc / AEAD tag 4 B（可选，flags 指示）
```

flags 位分配：bit0 = PAYLOAD_CRC_PRESENT，bit1 = AEAD_PRESENT，bit2 = COMPRESSED，bit3 = CONTROL_FRAME，bit4 = PERF_TRACE_SAMPLED，bit5~15 保留必须为 0。数据帧与控制帧通过 CONTROL_FRAME 位区分。

`connection_schema_ref` 是连接握手期间分配的紧凑引用，映射到完整 Canonical Digest。Schema Announcement/Request 控制帧携带完整 Digest 和 Descriptor；Short ID 碰撞时不能复用同一 Ref。网络帧不以单独的 64 位 Short ID 作为最终 Schema 身份。

### 9.2 路由逻辑

```text
Publish(topic, target_node)
             │
             ▼
       Resolve Route
        /          \
 target local     target remote
      │                 │
 SHM Channel       Bridge Channel
```

Transport Switcher 的输入包括：

- Topic ID；
- Target Node ID；
- 可靠性策略；
- 消息优先级；
- Payload 大小；
- 是否允许丢弃；
- 当前通道健康状态；
- Driver 能力描述（`TransportCapabilities`：传输形态、单帧/重组上限、共享窗口支持等，见详设 15.3）。

远端目标节点集合的产生支持两种 **路由策略（RoutePolicy）**，按 Topic 配置（详设 14.2、15.2）：

- **自动发现（Discovery Routing，默认）**：订阅驱动路由。Subscriber 注册到 Node Registry 后，Registry 汇总该 Topic 的**订阅节点集合**并带版本号推送；无订阅者的节点不产生网络流量，新订阅者从注册切点开始接收，不追溯历史；
- **显式配置（Static Routing）**：配置驱动路由。Topic 元数据显式给出**静态路由表（Route Set）**——固定发布的目标节点列表（如 `zone-front-01`、`recorder-01`），发布即按配置扇出，与订阅者注册/注销解耦。到达目标节点后按该节点本地订阅情况投递；无订阅者时按 Topic 策略丢弃或有限缓存，并导出指标，不静默成功。

静态路由适用于录制节点固定接收、跨 Trust Domain 确定性通道（如 IPCF 固定域间链路）、启动期拓扑即确定的车载网络等场景。两种策略的 Route Handle 均以版本号为失效依据，不在消息热路径执行发现查询。

### 9.3 Topic 策略

| 策略项 | 可选值 |
|---|---|
| 传输驱动 | SHM / TCP / UDP / RDMA / Fabric（IPCF 等，见详设 15.3、16.7） |
| 路由策略 | 自动发现（默认）/ 显式配置静态路由（目标节点列表，见 9.2） |
| 交付语义 | 可靠有序 / 尽力而为 / 最新值 |
| 队列满行为 | 阻塞 / 丢最新 / 丢最旧 / 降采样 |
| 批处理 | 关闭 / 按数量 / 按时间窗口 |
| 压缩 | None / LZ4 / Zstd 等 |
| 校验 | Header CRC / Payload CRC / AEAD |
| 录制 | Disabled / Best Effort / Memory Buffered / Durable / Snapshot |
| 录制满队列 | 阻塞 / 丢最新 / 丢最旧 / 停止录制 |

实时链路不应默认采用无限期阻塞。每个 Topic 必须明确队列满时的处理策略。要求完整录制时，存储队列满只能阻塞或失败，不能静默丢弃。

### 9.4 数据存储总体架构

存储层定位为消息录制和回放，不是分布式一致性日志：

```text
Multi-Publisher Topic
      │
      ▼
Recorder Subscriber
      │ Canonical Encode + Copy
      ▼
Bounded Memory Buffer
      │
      ▼
Per-Topic Single Writer
      │
      ▼
Schema Store + Segment + Index + Manifest
```

核心约束：

- 持久化 Canonical Payload，禁止保存 SHM Offset 或 C++ 内存镜像；
- Schema Descriptor 和 Session Schema Ref Table Entry 必须先于引用它的 Durable Record 落盘；
- 一个 Topic 可以有多个 Publisher，但一个 Topic/Partition 的活动 Segment 只有一个逻辑 Writer；
- 多 Writer 通过 Topic Partition 扩展，不并发修改同一个 Segment；
- `memory_buffered` 使用有界内存吸收磁盘短期抖动；
- `BUFFERED` 不等于 `DURABLE`；
- 要求完整录制时，Buffer Full 必须背压或停止录制；
- Segment 采用 Append-only，崩溃后截断到最后完整且通过校验的 Record；
- Replay 必须重新分配 SHM 对象并通过普通 Publisher API 发布；
- Recorder 先 Reserve 有界写队列 Slot，再分配 Ingestion Sequence 并 Commit；失败时在同一预留 Slot 提交 Gap/Tombstone；
- Segment Record 通过长度首尾、CRC 和 Commit Marker 校验，Durable Batch 执行数据与目录同步；
- Retention 删除前等待 Replay/Inspector 等 Reader Pin 释放。

### 9.5 端到端确认阶段

默认 `Publish()` 成功只表示本地 Slot 已提交，不表示远端接收或磁盘持久化。更强保证通过显式 Receipt 等待：

```text
LOCAL_PUBLISHED
  → REMOTE_ACCEPTED
  → RECORDER_BUFFERED
  → STORAGE_WRITTEN
  → STORAGE_DURABLE
```

网络重试首版为 At-least-once + 来源 Sequence 去重，不宣称 Exactly-once。`RECORDER_BUFFERED` 在进程崩溃或掉电时可能丢失；只有满足文件系统和硬件前提的 `STORAGE_DURABLE` 才能作为掉电恢复目标。

多目标 Topic 的 Receipt 在发布时冻结目标 Route Snapshot，并显式选择 ALL、ANY 或 QUORUM。Receipt 返回逐目标 Stage/Status；Deadline 只停止等待，不撤销已经发布的消息。Outstanding Receipt Table 必须有界。

---

## 10. 所有权与内存回收

### 10.1 Payload 状态机

```text
FREE
  │ allocate
  ▼
ALLOCATED
  │ builder acquired
  ▼
BUILDING ────────────→ ABORTING → FREE
  │ validate + publish
  ▼
PUBLISHED
  │ retire：不再产生新 Borrow（现存 Borrow 可能仍存活）
  ▼
RETIRED
  │ reclaim：所有 Borrow 释放后回收内存
  ▼
FREE
```

`RETIRED` 只表示「不再产生新 Borrow」，不表示「所有 Reader 已完成」；从 RETIRED 到 FREE 的 `Reclaim` 必须确认不存在有效 Borrow。

### 10.2 回收方案比较

| 方案 | 优点 | 代价与风险 | 适用场景 |
|---|---|---|---|
| 原子引用计数 | 直观、及时回收 | 热点原子更新；进程崩溃可能泄漏 | 订阅者少 |
| Lease 绑定引用 Pin | 对象级长持有；崩溃由 Lease 兜底 | Reclaim 多一个判定条件；需配额防泄漏 | Transfer/Replay/Inspector/跨 Channel relay（ADR-0013） |
| Epoch/RCU | 读取路径轻量 | 延迟回收；需要进程失效检测 | 读多写少 |
| ACK 位图 | 可精确识别未完成订阅者 | 订阅者数有上限 | 固定订阅者集合 |
| 租约与扫描 | 可处理异常退出 | 恢复逻辑复杂 | 强健性要求高 |

### 10.3 首版建议

首版 Broadcast Channel 建议采用：

- 每个订阅者独立 Cursor；
- 固定最大订阅者数；
- Slot 或消息元数据保存 ACK 位图；
- 每个订阅者维护心跳和租约；
- Coordinator 剔除失效订阅者；
- 所有有效订阅者完成后回收 Payload；
- 对象级长持有（Transfer/Replay/Inspector/跨 Channel relay）使用 Lease 绑定引用 Pin（ADR-0013），Reclaim 条件追加「无存活 Pin」；热路径回收不引入引用计数。

后续在订阅者规模扩大或读路径原子竞争明显时，再评估 Epoch/RCU。

---

## 11. 一致性与并发规则

1. Payload 必须在 Slot 发布前完成构建；
2. 发布后的普通消息对象默认不可变；
3. Slot 状态是 Payload 可见性的同步点；
4. Handle Generation 用于检测 ABA 和陈旧引用；
5. Cursor 和 Slot Sequence 必须能区分队列回绕；
6. 不得在消费者仍持有借用时复用 Payload；
7. 进程退出不得永久阻塞对象回收；
8. 多生产者和多消费者模式必须分别验证；
9. 所有原子字段必须确认在目标平台上支持跨进程使用；
10. `#pragma pack(1)` 不应直接用于含原子成员的共享控制结构。

> 固定内存布局不应依赖编译器默认 Packing。建议使用显式字段顺序、Padding、`static_assert(sizeof(...))` 和 `static_assert(alignof(...))`。

---

## 12. 故障恢复设计

| 故障场景 | 检测方式 | 恢复策略 |
|---|---|---|
| Publisher 构建时崩溃 | Owner 租约失效（不得仅凭 Slot 超时判定，见详细设计 9.5） | 将构建对象视为孤儿并回收 |
| Subscriber 崩溃 | 心跳超时；PID 与启动时间不匹配 | 剔除订阅者并清理 ACK 责任；若该 Subscriber 是强一致录制的 Recorder，Topic 必须阻塞新发布或进入 DEGRADED，直至 Recorder 恢复或由运维显式解除绑定（见详细设计 17.2） |
| Bridge 断链 | 连接状态、发送队列积压 | 按 Topic 策略重连、缓存或丢弃 |
| 非正常关闭 | `clean_shutdown=false` | 启动时执行恢复扫描 |
| Offset 或数据损坏 | 边界、类型、Generation、CRC 校验失败 | 隔离消息并告警，禁止继续解引用 |
| RingBuffer 已满 | 队列水位与预留失败 | 执行 Topic 背压或丢弃策略 |
| Slab Class 耗尽 | 分配失败和 Class 水位 | 回退大 Class、背压或拒绝发布 |
| 动态 Schema 无效或冲突 | Parser、限额、指纹和兼容性检查 | 拒绝注册并隔离来源 |
| Recorder 内存缓冲已满 | Buffer 水位和入队失败 | 按 Topic 策略阻塞、丢弃或停止录制 |
| 磁盘变慢或空间不足 | 写入延迟、水位和文件系统错误 | 背压、告警、轮转或停止录制 |
| Segment 尾部不完整 | 启动扫描、长度、CRC 和 Commit Marker 校验 | 截断到最后完整提交 Record 并重建索引 |
| MPSC Producer 预留后崩溃 | Owner Lease 失效且 Slot 未提交 | CAS 为 ABORTED，回收 Allocation Journal 并推进 |
| 多进程同时尝试恢复 Region | Recovery Owner/Epoch/Lease | 只允许唯一 Owner，失败后按 Epoch 接管 |
| Bridge 重连产生重复 | 来源 Identity、Sequence 和 ACK 窗口 | 接收端去重并返回幂等 ACK |
| Registry 暂时不可用 | 健康检查和请求失败 | 已缓存 Schema 可继续，新注册和不兼容升级失败 |
| Telemetry Exporter 阻塞 | Export Queue 水位和超时 | 丢监控快照并告警，不阻塞数据路径 |

### 12.1 恢复扫描

恢复扫描至少应校验：

- SuperBlock 是否完整；
- Channel Directory 的 Offset 是否有效；
- RingBuffer Cursor 与 Sequence 是否一致；
- READY Slot 是否引用有效 Slab；
- 位图已占用 Slot 是否存在合法 Header；
- 是否存在无任何有效引用的孤儿 Slab；
- 是否存在已失效订阅者遗留的 ACK。

---

## 13. Schema 与协议演进

### 13.1 版本维度

系统至少包含七个独立版本：

1. **Region Layout Version**：共享内存整体布局；
2. **Object Layout Version**：静态/动态消息对象布局；
3. **Schema Version**：业务消息定义；
4. **Schema Descriptor/Canonicalization Version**：描述格式和 Digest 规范化规则；
5. **Canonical Wire Version**：网络 Payload 与存储 Payload 编码；
6. **Network Protocol Version**：Bridge 帧、握手和 Schema Ref；
7. **Storage Format Version**：Segment、Record、Manifest 和 Schema Table 格式。

这些版本不能使用同一个版本号代替。

### 13.2 兼容性规则

- 生产 Schema 的 Field ID 必须显式声明，禁止依赖源码顺序自动分配；
- 字段 ID 一旦发布不得复用，删除后必须标记为 Reserved；
- 新增字段必须为 Optional 或具有默认值；
- 删除字段时保留字段编号；
- Decoder 能够跳过未知字段；
- 不兼容变更必须提升 Schema Version 的 Major 维度（schema_version 编码为 `major << 16 | minor`，详见详细设计 13.3）；
- Bridge 可在受支持的版本之间执行转换；
- 滚动升级期间至少支持 N 与 N-1 版本互通。

基础兼容性矩阵（按「新 Writer → 旧 Reader」与「旧 Writer → 新 Reader」两个方向判定）：

| 变更 | 新 Writer → 旧 Reader | 旧 Writer → 新 Reader |
|---|---|---|
| 新增 Optional 字段 | 兼容（旧 Reader 跳过未知字段） | 兼容（新 Reader 按缺省/默认值处理） |
| 删除 Optional 字段（ID Reserved） | 兼容（新 Reader 按缺省处理） | 兼容（旧 Reader 跳过未知字段） |
| 字段改名为兼容重命名 | 兼容（Field ID 不变） | 兼容 |
| 整型提升（uint32 → uint64） | 不兼容（旧 Reader 拒绝） | 不兼容 |
| 收紧约束（max_bytes 由大改小） | 不兼容（新 Writer 可能产生旧 Reader 拒绝的数据） | 兼容 |
| 放宽约束（max_bytes 由小改大） | 兼容 | 不兼容（旧 Writer 不感知新上限，但数据本身合法；仍需 Major 提升并显式注册） |
| 改变字段类型或 Wire Type | 不兼容 | 不兼容 |

完整矩阵和 N/N-1 判定算法见详细设计 13 章。

### 13.3 IDL v1 语法概要

IDL 的完整规范由 ADR-0011 与详细设计 13.2 冻结。首版概要：

- **标量类型**：`int32/int64`（ZigZag Varint）、`uint32/uint64`（Varint）、`fixed32/fixed64`、`float/double`、`bool`、`string`（UTF-8）、`bytes`；
- **复合类型**：`struct`（纯固定布局、不可含动态字段、可内联）与 `message`（可含动态字段、可含 `struct` 和 `message` 成员）；
- **容器**：`vector<T>`，必须声明 `[max_capacity]`；`string/bytes` 必须声明 `[max_bytes]`；Map 不进入 v1；
- **Annotation**：`[max_bytes]`、`[max_capacity]`、`[default = ...]`、`[snapshot_key]`，未知 Annotation 拒绝编译；
- **Reserved**：`reserved <id>[, <id> to <id>];`，删除字段后必须保留；
- **Package 与 Import**：`package a.b;` 定义类型全名前缀；`import "path"` 引入依赖，Import 路径不参与 Canonical Digest；
- **递归**：禁止直接或间接递归类型，Compiler 执行环检测。

---

## 14. 安全设计

### 14.1 本地安全

首版将能够读写 Attach 同一 Region 的进程视为同一 Trust Domain。校验和 Accessor 防止误用与损坏，不隔离恶意已 Attach 进程。多租户或不同安全等级必须拆分 Region，或通过受控 Proxy/Bridge 接入。

- 共享内存对象按最小权限创建；
- 限制可 Attach 的 UID、GID 或容器安全域；
- 不向非可信进程开放任意 Offset 调试接口；
- 生产环境只允许生成的 Accessor 或经过验证的 Dynamic View 解释共享对象；
- 对诊断 Dump 设置访问权限和保留周期。

### 14.2 网络安全

- Bridge 支持双向身份认证；
- 根据环境使用 TLS、IPsec 或受控 RDMA Fabric；
- 所有长度和容器数量必须在分配前校验；
- 限制单帧大小、单连接缓存和解码资源；
- 防止整数溢出、畸形帧和内存耗尽攻击；
- 敏感 Payload 不写入普通运行日志。

---

## 15. 可观测性

### 15.1 Channel 指标

- 发布速率和消费速率；
- RingBuffer 水位；
- 丢弃、覆盖和发布失败数；
- 每个订阅者的 Cursor Lag；
- 最慢订阅者；
- 消息从发布到消费的 p50、p99、p99.9 延迟。

### 15.2 Allocator 指标

- 各 Class 已用和可用 Slot；
- 分配及回收耗时；
- 分配失败数；
- 内部碎片率；
- 孤儿块数量；
- 对象退休到实际回收的延迟；
- NUMA 远程访问比例。

### 15.3 Bridge 指标

- 连接状态；
- 重连次数；
- 发送和接收积压；
- 编码和解码耗时；
- 网络 RTT；
- 丢包和重传；
- Schema 转换次数；
- 帧校验失败数。

### 15.4 传输性能指标

传输路径按阶段统计，而不是只报告端到端单点延迟：

- Slab 分配、Builder 校验和 Ring Reserve 等待；
- 本地 `READY → Subscriber Acquire` 延迟；
- Bridge Encode、发送队列等待、RTT、Decode/Rebuild；
- 远端发布和远端 Subscriber Acquire 延迟；
- 每 Topic 消息速率、Payload/Wire 吞吐和协议开销率；
- Inflight、发送/接收队列字节数、最旧消息年龄和饱和度；
- 丢弃、超时、重传、重复、CRC 和 Schema 错误；
- Telemetry Event/Sample 数、Sidecar 丢弃、Trace 不完整、时钟不确定和 Export 失败。

统计分为 Off、Counters-only、Sampled Latency 和 Full Debug 四级。热路径使用线程本地 Counter、固定内存 Histogram 和有界 Sidecar Trace Buffer，Exporter 不得反向阻塞数据路径。

节点内耗时使用 Monotonic Clock。跨节点单向延迟只有在 PTP/时钟同步不确定度满足阈值时上报；否则只报告节点内阶段耗时和 RTT。Histogram 通过桶合并，禁止对多个节点的 p99 直接求平均。

### 15.5 Schema 与 Storage 指标

- Schema 注册、编译失败、冲突和兼容性检查延迟；
- 每 Topic Recorder 入队和落盘速率；
- 内存缓冲字节数、高低水位和最旧记录等待时间；
- Buffer Full、丢弃、Gap 和背压持续时间；
- Batch 大小、Write/Sync 延迟和 Segment 轮转次数；
- `BUFFERED → WRITTEN → DURABLE` 延迟；
- 磁盘空间、恢复截断字节数和 CRC 错误。

### 15.6 诊断工具

- 共享内存布局检查器；
- RingBuffer Dump 工具；
- Slab 一致性扫描器；
- Topic 和订阅者状态查看器；
- 容量规划报告；
- 离线网络帧解析器；
- Schema 查看和兼容性检查器；
- Recording Inspect、Verify、Repair 和 Replay 工具；
- 端到端 Trace 查询。

建议使用 `sequence_num + trace_id` 贯穿 Publisher、Bridge 和远端 Subscriber。

---

## 16. 性能目标与验证方法

下列数字来自原方案的性能预估，应作为原型阶段的验证目标，而不是已经达成的指标。

| 维度 | 传统路径参考 | Mino 目标 | 验证说明 |
|---|---:|---:|---|
| 同机传输热路径延迟 | 约 5～15 μs | 0.05～0.2 μs | 仅指热缓存 SPSC 的 `READY Commit → Subscriber Acquire`，不含 Allocate、Build、Callback、系统调用；需分别测 Telemetry Off/On 和完整业务 p99 |
| Slab 分配 | 数十至数百 ns | 小于 10 ns | 指无竞争、缓存命中的位图路径 |
| 吞吐 | 受 Socket Buffer 和协议栈限制 | 接近内存带宽上限 | 受消息大小、NUMA 和订阅者数影响 |
| 内存碎片 | 存在堆外部碎片 | 无外部碎片 | 仍存在固定 Class 的内部碎片 |
| 跨机通信 | 业务自建序列化 | IDL + Bridge 统一支持 | 仍包含编码和网络传输成本 |

### 16.1 Benchmark 矩阵

测试至少覆盖：

- Payload：64 B、256 B、2 KiB、64 KiB、1 MiB；
- 拓扑：SPSC、SPMC Broadcast、MPSC；
- Subscriber：1、2、4、8、16；
- 缓存状态：热缓存和冷缓存；
- NUMA：同节点和跨节点；
- 队列状态：低水位、高水位和满队列；
- Bridge：TCP、UDP、RDMA（如适用）；
- 异常场景：进程 Kill、网络断开、内存耗尽；
- Schema：静态/动态路径、注册冲突、复杂度上限和 N/N-1；
- Storage：多 Publisher、1～100 Topic、不同 Record 大小；
- 磁盘：正常、短期暂停、长期降速、空间不足；
- Recorder Buffer：低水位、高水位、满队列和背压；
- 同步策略：无同步、周期同步和每批同步；
- Telemetry：关闭、Counters-only、不同采样率和 Full Debug；
- 对比开启/关闭 Telemetry 的吞吐、延迟和 CPU 开销；
- 时钟同步正常、超限和跳变场景。

### 16.2 报告口径

性能报告必须包含：

- CPU、内存和网卡型号；
- 操作系统和内核版本；
- CPU 频率及节能策略；
- NUMA 和线程绑核方式；
- 编译器与优化选项；
- 消息尺寸分布；
- 生产者/订阅者数量；
- p50、p95、p99、p99.9 和最大延迟；
- 吞吐、CPU 占用、Cache Miss 和错误率。

不能只报告最佳单点延迟。

---

## 17. 实施路线图

### 阶段 P0：协议语义定稿

交付物：

- 队列拓扑定义；
- 所有权与回收状态机；
- 共享内存布局 v1；
- Schema Identity、Descriptor 和 Storage Format 草案；
- 多 Publisher 顺序与端到端 Delivery/Ack 语义；
- MPSC Producer Crash 和 Broadcast Membership 协议；
- Canonical Wire、Dynamic Layout 和 Schema Digest 测试向量；
- Segment Commit 与唯一 Recovery Owner 协议；
- Linux 原子 ABI 支持矩阵；
- 错误模型；
- C++ 与 Bazel 工程边界；
- 关键架构决策记录（ADR）。

退出条件：

- SPSC、SPMC Broadcast 和竞争消费语义评审通过；
- 发布后可变性策略确认；
- 进程异常退出的回收逻辑确认；
- Producer/Subscriber ID 复用和队列空洞协议确认；
- Local、Remote、Buffered、Written、Durable 保证矩阵确认；
- Telemetry 开销预算和跨节点时钟规则确认；
- **以上决策对应的 ADR-0002~0007、0009、0011 全部达到 ACCEPTED 状态**（ADR 是决策状态的唯一权威，见 docs/adr/README.md）。

### 阶段 P1：C++ Runtime

入口条件：P0 全部退出条件满足。

范围：

- SuperBlock；
- `ShmHandle`；
- Central Slab Allocator；
- 通用 MPMC 骨架（详设 9.9）；
- SPSC RingBuffer；
- 首版 Broadcast Channel；
- Subscriber 租约；
- 恢复扫描器。

退出条件：

- 单元测试和压力测试通过；
- ASAN、UBSAN、TSAN 目标测试通过；
- Publisher/Subscriber 随机 Kill 压力测试（≥1 小时）后，恢复扫描报告的孤儿 Slab 数量为 0，且 Region 可用空间恢复至基线。

### 阶段 P2：IDL、动态 Schema 与 CodeGen

入口条件：P1 全部退出条件满足。

范围：

- C++ IDL Parser、AST 和 Semantic Validator；
- Schema Descriptor、Layout Planner 和 Registry；
- Dynamic Builder/View 和字段句柄；
- C++ Builder/Accessor CodeGen；
- String/Vector 支持；
- Canonical Encoder/Decoder；
- `minoc` 与 Bazel IDL Rule。

退出条件：

- `.idl` 可稳定生成 `.generated.h/.cc` 和 Descriptor；
- 静态和动态路径产生等价 Wire Format；
- 动态 Schema 控制路径注册、限额和缓存测试通过；
- 兼容性测试和 Fuzz 测试通过；
- 动态对象图可完整回收。

### 阶段 P3：Transport Switcher 与 TCP Bridge

入口条件：P2 全部退出条件满足。

范围：

- Publisher/Subscriber 门面 API；
- Node Registry；
- Topic 路由；
- TCP Bridge；
- 远端对象重建；
- 基础指标和 Trace。

退出条件：

- 同机和跨机端到端流程通过；
- 断链重连和错误帧处理通过；
- N/N-1 Schema 互通通过。

### 阶段 P4：Recorder、存储与回放

入口条件：P3 全部退出条件满足。

范围：

- Schema Store；
- Segment、Record、Manifest 和 Index；
- Recorder Subscriber 和 Canonical Encode；
- 有界内存 Buffer Pool 与 MPSC 写入队列；
- Per-Topic Single Writer；
- Segment 恢复、轮转和 Retention；
- Replay、Inspect、Verify 和 Repair 工具。

退出条件：

- 多 Publisher 单 Writer 顺序语义通过；
- 磁盘短期暂停后 `memory_buffered` 数据可继续落盘；
- 任意写入点 Kill 后可恢复到最后完整 Record；
- Schema 先于引用它的 Durable Record 持久化；
- 缓冲满时按策略背压、丢弃或失败且无静默数据丢失；
- **正式 SLA 文档发布**：含硬件型号、负载模型、统计口径与目标数值（作为 P5 的验收基准）。

### 阶段 P5：性能优化

入口条件：P4 全部退出条件满足。

范围：

- 位图分片和每核缓存；
- NUMA 感知分配；
- 批量发布和消费；
- `sendmsg/writev` 和存储批量写；
- UDP/RDMA/Fabric 驱动（含 IPCF 类跨域共享窗口通道）；
- 大对象专用池；
- 达到单 Writer 瓶颈后评估 Topic Partition。

退出条件：

- 目标硬件上的正式 SLA（P4 发布的 SLA 文档）达标；
- 长稳测试（≥72 小时）资源增长率不超过阈值（每 24 小时 RSS/Slab 占用增长 < 5%）。

### 阶段 P6：产品化

入口条件：P5 全部退出条件满足。

范围：

- 权限和安全配置；
- 部署工具；
- 监控与告警；
- 容量规划；
- 滚动升级；
- 运维及故障演练手册。

退出条件：

- Trust Domain 隔离与 ACL 策略通过安全评审；
- 滚动升级和故障演练完成至少一轮实操；
- 监控告警规则在演练中验证有效；
- 容量规划报告覆盖全部生产 Topic。

---

## 18. 验收标准

### 18.1 正确性

- 不同进程映射到不同虚拟地址时消息仍可正确访问；
- 队列回绕不会造成 ABA、重复消费或漏消费；
- 不会在消费者仍持有借用时回收 Payload；
- 动态容器越界操作被拒绝；
- 生产 Schema 缺少显式 Field ID、复用 Reserved ID 或包含递归类型时被拒绝；
- Unknown Field 可按策略有界保留或 Wire Passthrough；
- 网络编码不携带本机 Offset 或未使用 Capacity。

### 18.2 并发性

- SPSC、Broadcast 和竞争消费分别满足定义语义；
- 无数据竞争、提前回收和永久占用；
- 多订阅者下每个 Cursor 可独立推进；
- 慢订阅者处理策略符合 Topic 配置；
- 多 Publisher 向同一 Topic 发布时保持各来源内部顺序；
- 一个 Topic/Partition 的活动 Segment 始终只有一个逻辑 Writer。

### 18.3 健壮性

- Publisher、Subscriber 和 Bridge 任意时点被 Kill 后系统可恢复；
- 非正常关闭后恢复扫描可重建一致状态；
- 共享内存或网络畸形数据不会导致越界访问；
- RingBuffer 或 Slab 耗尽时执行预期背压策略；
- 动态 Schema 的畸形、冲突和超限输入被拒绝；
- Segment 尾部损坏可截断到最后完整 Record；
- Recorder 或 Writer 任意时点被 Kill 后可报告 Durable 边界并恢复；
- MPSC Producer 在 Reservation/Build/Commit 任意点被 Kill 不会永久阻塞队列；
- 同一 Region 不会出现两个有效 Recovery Owner；
- Bridge 重试产生的重复消息可识别，超出去重窗口时明确降级或拒绝；
- Delivery Receipt 在多目标、超时和部分成功场景返回准确逐目标状态；
- 不同 Trust Domain 不能直接读写 Attach 同一 Region；
- Telemetry Exporter 故障不会阻塞数据路径。

### 18.4 兼容性

- 支持 N 和 N-1 Schema 互通；
- 新增可选字段不会破坏旧消费者；
- 未知字段可以安全跳过；
- 不兼容变更可以被明确拒绝；
- 静态和动态 Schema 路径使用一致的 Canonical Wire Format；
- Schema Descriptor、Network Frame 和 Storage Format 独立演进。

### 18.5 性能

- 在 P4 发布的正式 SLA 文档约定的硬件和负载模型下达到其 p99 延迟目标；测量口径遵循第 16 章（区分热路径与完整业务、Telemetry Off/On）；
- 吞吐、CPU、内存和 Cache Miss 达到 SLA 文档中的预算值；
- p99.9/p99 延迟比值不超过 SLA 文档规定上限；所有尾延迟样本均有 Telemetry 阶段归因记录；
- Counters-only 与默认采样 Telemetry 的吞吐开销满足 SLA 文档预算（原型预算目标分别为 ≤1% 与 ≤2%，以详细设计 21.5 为准）；
- 性能报告明确区分节点内阶段延迟、RTT 和时钟可信的跨节点单向延迟。

---

## 19. 风险与待决策事项

下表为摘要视图。每一项的权威状态以 `docs/adr/` 中对应 ADR 的状态字段为准；完整的决策与验证登记（含 Owner、目标关闭阶段、验证产物）见《详细设计文档》第 26 章。

| 优先级 | 待决策事项 | 建议 |
|---|---|---|
| P0 | MPSC Producer Crash | Reservation Owner Lease + ABORTED Tombstone，并做形式化和 Kill 测试 |
| P0 | Broadcast Membership | Subscriber Generation + 发布时集合快照 |
| P0 | Delivery/Ack 语义 | 冻结 Local/Remote/Buffered/Written/Durable 保证矩阵 |
| P0 | Dynamic SHM Layout | 冻结 Presence、Offset、Child Slab、Unknown Field 和 Allocation Journal |
| P0 | IDL Field Identity | 生产 Schema 强制显式 Field ID，删除后 Reserved，禁止递归类型 |
| P0 | Delivery Receipt | 冻结目标 Snapshot、ALL/ANY/QUORUM、逐目标状态和有界 Outstanding Table |
| P0 | Canonical Wire Format | 冻结 Tagged Format ADR 和 Golden Vector |
| P0 | Schema ID 与规范化 | 64 位 Short ID + 完整 Digest 和碰撞处理 |
| P0 | Storage Record Commit | 验证长度尾标、CRC、Commit Marker 和目录 Sync |
| P0 | Recovery 唯一所有者 | Owner/Epoch/Lease，验证恢复者崩溃接管 |
| P0 | 原子 ABI | 固定 Linux x86-64 基线（ADR-0001 已 ACCEPTED），AArch64 验证后启用 |
| P0 | Recorder 满队列语义 | 按 Topic 选择强一致、隔离或尽力录制（ADR-0008 已 ACCEPTED，待原型验证容量公式） |
| P0 | Handle v1 布局 | 验证 64 位 Offset、32 位 Generation、永不复用 Region ID 和回绕 Drain |
| P1 | Trust Domain | 同一 RW Region 仅允许可信进程，不同安全域拆 Region/Proxy |
| P1 | Registry 一致性 | 首版单 Authoritative Registry + 本地缓存，禁止双主 |
| P1 | Bridge 重传窗口 | 有界 At-least-once + 接收端去重 |
| P1 | 跨版本滚动升级 | 新 Region + Drain/Cutover，不原位改 ABI |
| P1 | 节点资源预算 | Topic 创建前做全模块 Admission Control |
| P1 | Writer 线程模型 | 首版一 Topic 一逻辑 Writer，Topic 多时评估共享 Executor |
| P1 | Telemetry 开销 | Counters-only 目标 ≤1%，默认采样目标 ≤2%，以原型验证为准 |
| P2 | Topic Partition | 单 Writer 达到瓶颈后启用，不并发写同一 Segment |
| P2 | 性能目标可信度 | 建立可复现 Benchmark 后确定正式 SLA |

---

## 20. 设计原则总结

1. Offset 是跨进程地址协议，Wire Format 是跨机器数据协议，两者必须分离。
2. 零拷贝不等于零同步，所有权、可见性和回收仍需严格定义。
3. 动态数据结构必须有边界，不能在固定共享内存中无限增长。
4. 默认使用“构建后发布、发布后只读”的消息语义。
5. 广播和竞争消费是不同语义，应使用不同的队列模型。
6. 共享内存中的任何 Offset 和长度都视为不可信输入并执行检查。
7. 动态 Schema 编译只发生在控制路径，数据路径只使用已验证且缓存的 Descriptor。
8. 一个 Topic 可以有多个 Publisher，但一个活动 Segment 只能有一个逻辑 Writer。
9. 多 Writer 扩展通过 Topic Partition 实现，不并发写同一个 Segment。
10. 内存缓冲必须有界；要求全部保存时，容量耗尽必须背压或失败。
11. `BUFFERED` 不等于 `DURABLE`，Schema 必须先于引用它的 Durable Record 持久化。
12. MPSC Reservation 和 Broadcast Membership 必须对进程崩溃及 ID 复用闭环。
13. 64 位 Schema ID 只作快速索引，完整 Canonical Digest 才是最终身份。
14. 默认 Publish 成功只代表本地提交，更强 Delivery Stage 必须显式等待。
15. Telemetry 必须有界、可采样、低开销，Exporter 故障不能阻塞数据路径。
16. 跨节点单向延迟只有在 Clock Quality 满足阈值时才具有统计意义。
17. Base Slot 与各 Channel Sidecar 分离，禁止一个字段承载多种并发语义。
18. Handle Generation 不回绕、Region ID 不复用，Attach 必须校验 Region UUID/Epoch。
19. 生产 Schema 使用显式稳定 Field ID，Unknown Field 行为必须明确且有界。
20. 同一 RW Region 是一个 Trust Domain，不对恶意已 Attach 进程提供隔离承诺。
21. 先实现并证明正确的 SPSC/SPMC/MPSC 子集，再扩展 MPMC、RDMA 等能力。
22. 性能数字必须在目标硬件、真实拓扑和完整尾延迟口径下验证。
