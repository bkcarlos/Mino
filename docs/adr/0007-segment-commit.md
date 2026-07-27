# ADR-0007：Segment Commit Protocol

- 状态：PROPOSED
- 决策：Record 使用长度首尾、Header/Payload/Record CRC、Trailer 和 Commit Marker。
- 约束：Durable Batch 执行 fdatasync；Rename 后 fsync 父目录；Schema Descriptor 和 Schema Ref Table 先于引用 Record Durable；Index 可重建。
- 待验证：任意字节 Kill、短写、ENOSPC、EIO 和真实文件系统掉电测试。

## Context

录制进程可能在任意写入点被 Kill 或遭遇掉电。恢复时必须无歧义地区分「已提交 Record」与「半写尾部」，且不能依赖单扇区原子性假设。磁盘格式见详设 17.10 的字节级规范。

## Alternatives Considered

- **仅尾部 Commit Marker**：写入最省，但半写可能恰好留下假 Marker（内容来自旧 Record 复用空间），需要 CRC 二次确认，单独使用不够，否决。
- **WAL（先日志后数据）**：恢复语义最强，但双写放大明显，与「顺序追加 Segment」模型的吞吐目标冲突，首版否决，后续可在 durable 强需求场景重估。
- **仅长度前缀扫描**：半写长度字段会误导扫描器越界，必须首尾长度一致 + CRC，否决。

## Consequences

- 正面：任意字节截断都可检测；恢复扫描单向顺序完成；Index 丢失可从 Segment 重建，降低持久化状态面。
- 负面：每条 Record 有固定尾部开销（Trailer + Marker 24 B）；短写处理增加 Writer 状态机复杂度（ERROR 态见详设 17.12）。
- 跟进：掉电注入测试套件（详设 26 章 V-10）。
