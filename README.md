# Mino

> **Mino = Minimal Overhead**
> 零拷贝共享内存与跨机通信统一框架
> Unified Framework for Zero-Copy Shared Memory and Cross-Node Communication

## 项目简介

Mino 是一个面向高性能系统的统一通信框架，通过统一的类型系统、发布订阅 API 和数据生命周期模型，将同机 IPC 与跨机网络通信整合为一套框架：

- **本地节点**：传递共享内存偏移量（Handle），消费者原位读取 Payload，实现零拷贝；
- **远程节点**：Bridge 提取有效字段，编码成紧凑网络数据后发送；
- **应用层**：使用相同的 IDL 类型和 Publisher/Subscriber API，不感知底层路径差异。

**当前状态**：概念设计完成，处于设计评审与原型验证前阶段。仓库当前包含完整的设计文档与架构决策记录（ADR），代码实现按实施路线图（P0–P6）推进中。

## 设计目标

1. **本地零拷贝** — Payload 写入共享内存后，消费者通过相对偏移量直接访问
2. **确定性内存分配** — 固定尺寸 Class + 原子位图，常见分配与回收为 O(1)
3. **动态数据结构** — 支持有容量上限的 `string`、`vector` 和嵌套对象，提供受控的原位构建接口
4. **跨机透明传输** — Transport Switcher 根据目标 Node ID 自动选择共享内存或网络通道
5. **协议可演进** — 共享内存布局、消息 Schema 和网络帧均包含版本、长度与校验信息
6. **故障可恢复** — 对进程异常退出提供检测、资源回收和恢复机制
7. **可观测可运维** — 暴露队列水位、分配失败、订阅延迟、Bridge 积压和协议错误等指标
8. **运行时动态 Schema** — 控制路径注册、校验和缓存动态 Schema，Dynamic Builder/View 访问未知类型
9. **消息录制与回放** — Canonical Payload 持久化，有界内存缓冲、顺序落盘、恢复和回放

## 核心设计

### 控制流与数据流分离

- **控制路径**：固定尺寸 Index RingBuffer，Slot 按 Cache Line 对齐，只传递消息类型、序列号、时间戳、Payload Handle 和状态，不复制大型 Payload；
- **数据路径**：Payload 存储在 Central Slab Pool，发布者在共享内存中直接构造数据，本地消费者根据 Handle 原位访问。

### 统一寻址

所有共享引用采用相对偏移量 `ShmHandle`（offset + generation + region_id），禁止裸指针，保证不同进程映射到不同虚拟地址时仍可正确访问。

### 队列拓扑

支持 SPSC、SPMC Broadcast、MPSC、Work Queue 等 Channel 语义；ACK 责任绑定 Subscriber Generation 与订阅集合快照；Producer 预留崩溃后由 Lease 机制回收。

### 技术栈与平台基线

- 语言：C++20
- 构建：Bazel + Bzlmod
- 首版平台：Linux x86-64（AArch64 在完成 ABI/原子/性能验证后启用）
- 工具链：Runtime、Schema Compiler、`minoc`（代码生成）与 `mino`（运维工具）

## API 预览

```cpp
// 发布
Publisher<SensorFrame> pub =
    bus.CreatePublisher<SensorFrame>("sensor/front_lidar", policy);

auto builder = pub.Allocate();
builder->set_frame_id(1001);
builder->set_device_name("Front_Lidar_Node");
builder->points().push_back({1.0f, 2.0f, 3.0f});
pub.Publish(std::move(builder));

// 订阅
Subscriber<SensorFrame> sub =
    bus.CreateSubscriber<SensorFrame>("sensor/front_lidar", policy);

sub.Poll([](BorrowedMessage<SensorFrame> msg) {
    Process(msg->frame_id(), msg->points());
});
```

## 适用场景

- 车载智驾域控制器
- 高频交易及低延迟行情系统
- 实时传感器数据总线
- 音视频或点云数据管线
- 单机多进程高吞吐计算平台
- 需要同时覆盖 IPC 和跨机通信的实时系统

## 仓库结构

```text
Mino/
├── docs/
│   ├── Mino_架构设计文档.md    # 总体架构：目标、边界、SHM 布局、协议、路线图
│   ├── Mino_详细设计文档.md    # 模块级设计：接口、状态机、线程模型、工程约束
│   └── adr/                    # 架构决策记录（0001–0013）
│       └── README.md           # ADR 状态流转规则
└── README.md
```

## 文档导航

| 文档 | 内容 |
|---|---|
| [架构设计文档](docs/Mino_架构设计文档.md) | 设计目标、总体架构、SHM 布局与寻址、Index RingBuffer 协议、Slab 内存池、IDL、端到端流程、故障恢复、安全、可观测性、性能目标与实施路线图 |
| [详细设计文档](docs/Mino_详细设计文档.md) | Bazel 工程边界、部署拓扑、公共 API 与错误模型、Channel/Allocator/生命周期、静态与动态 Schema、Registry/Bridge、Recorder/Storage、测试要求 |
| [ADR 目录](docs/adr/) | 关键协议决策：平台基线、Handle 布局、Channel 语义、Canonical Wire、Schema Identity、Delivery/Ack、Segment Commit、Recorder 背压、Telemetry、SHM 信任域、IDL 字段标识、Transport Driver Fabric、SHM 引用 Pin |

ADR 按 `PROPOSED → ACCEPTED → VALIDATED → FROZEN` 状态推进，冻结后的不兼容变更必须创建新 ADR 和协议版本。

## 实施路线图

| 阶段 | 内容 |
|---|---|
| P0 | 协议语义定稿（ADR 全部 ACCEPTED） |
| P1 | C++ Runtime：SuperBlock、ShmHandle、Slab Allocator、RingBuffer、恢复扫描 |
| P2 | IDL、动态 Schema 与 CodeGen：`minoc`、Descriptor、Canonical 编解码 |
| P3 | Transport Switcher 与 TCP Bridge：门面 API、Node Registry、路由 |
| P4 | Recorder、存储与回放：Segment、Schema Store、Replay 工具、正式 SLA 文档 |
| P5 | 性能优化：NUMA 感知、批量收发、UDP/RDMA/Fabric 驱动 |
| P6 | 产品化：权限安全、部署工具、监控告警、滚动升级 |

## 许可证

本项目采用 [GNU Lesser General Public License v3.0](LICENSE)（LGPL-3.0）开源。LGPL-3.0 以 [GPL-3.0](https://www.gnu.org/licenses/gpl-3.0.txt) 为基础并附加宽松条款，两个许可证的完整文本见上述链接（`LICENSE` 文件含 LGPL-3.0 全文）。

这意味着：

- **可以自由使用、修改和分发**本项目，包括在商业闭源产品中通过动态链接使用；
- **对 Mino 本身的修改必须开源**：任何修改了 Mino 源码的衍生版本，在分发时必须以 LGPL-3.0（或 GPL-3.0）公开修改后的完整源代码；
- 仅链接使用 Mino 的应用程序本身无需开源，但需允许用户替换为修改后的 Mino 版本（如提供可重新链接的目标文件或使用动态链接）；
- 分发时须保留原始版权与许可证声明；
- 作者不提供任何担保（详见许可证相关条款）。

> LGPL-3.0 是在 GPL-3.0 基础上附加宽松条款的许可证：Copyleft 效力仅覆盖 Mino 库本身，不传染链接它的应用程序。如果需要更强的传染性（链接即整体开源），可评估迁移到 GPL-3.0；如果需覆盖网络服务场景，可评估 AGPL-3.0。

## 贡献

项目处于早期设计阶段，当前最重要的贡献方式是参与设计评审：

1. 阅读 [架构设计文档](docs/Mino_架构设计文档.md) 与相关 ADR；
2. 针对未冻结的决策在 ADR 评审记录中提出意见；
3. 已冻结 ADR 的不兼容变更需创建新 ADR 并升级协议版本。
