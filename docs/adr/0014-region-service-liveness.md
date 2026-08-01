# ADR-0014：Region Service Liveness 与 Supervisor Fencing

- 状态：ACCEPTED
- 决策：SuperBlock v3 在保持 256B 和 v2 既有字段偏移不变的前提下，增加完整 `ProcessIdentity service_owner` 与 64-bit `{service_epoch, phase}` fence。可写 `Create/Attach` 是唯一 supervisor attachment，并持有 host-local advisory lock；任意数量跨进程只读 Attach 继续支持。恢复租约只串行化 `DIRTY/RECOVERING` 扫描，不作为 ACTIVE service 判活依据。
- 约束：v2 只允许只读兼容，禁止自动恢复；v3 不支持多个独立进程同时 writable Attach；`ACTIVE` 只有在旧 supervisor lock 已由内核释放且其 `ProcessIdentity` 明确为 `Dead` 后才能转 `DIRTY`；`Unknown` 一律拒绝 destructive recovery；SuperBlock 必须保持 256B。
- 待验证：Linux x86-64 已由 `//mino/shm/region:service_liveness_test` 覆盖；macOS `KERN_PROC_PID` 路径需纳入平台 CI；若未来需要多进程 writer，必须新增有界、崩溃安全的 attachment registry 并提升 layout version。

## Context

v2 只有 `clean_shutdown`、Region state 与 recovery lease。`ACTIVE + clean_shutdown=false` 同时可能表示“服务正常运行”或“owner 已崩溃”，因此 Attach 不能安全决定是否运行会修改 allocator/channel 元数据的 scanner。旧实现选择一律拒绝 ACTIVE recovery，避免了误扫描，但导致 owner 被 `SIGKILL` 后 Region 永久无法自动恢复。

直接把 recovery lease 复用成 service lease 也不安全：长 GC、调试暂停、`SIGSTOP` 或调度抖动都可能使合法服务错过 heartbeat，进而允许另一进程 destructive takeover。另一方面，SuperBlock 剩余 ABI 只有 40B，刚好容纳一个 32B `ProcessIdentity` 和一个 64-bit fence，容不下可证明安全的多进程 attachment registry（至少需要多槽 identity、slot generation/state 和注册/注销协议）。

因此必须明确选择 supervisor-owner 模型，而不是保留“看起来支持多 writer、实际上无法证明所有 writer 已退出”的模糊语义。

## Decision

### SuperBlock v3

v2 的 bytes `[0,216)` 完全不变；原 `compat_pad[40]` 定义为：

- bytes `[216,248)`：`ProcessIdentity service_owner`；
- bytes `[248,256)`：原子 `service_fence_word = {service_epoch, phase}`。

`phase` 为 `UNOWNED / OWNED / CLOSING`。每次 writable supervisor Attach 递增 `service_epoch`。Region 对象保存 attach 时 token；Detach 必须先将精确 `{epoch, OWNED}` CAS 为 `{epoch, CLOSING}`，失败时只能 unmap，不能发布 `clean_shutdown/CLOSED`。

### Attach 与恢复

- `read_only=true`：允许任意数量进程 Attach；只校验 header/bounds，不获取 owner，不运行 scanner，不修改生命周期。
- `read_only=false`：请求唯一 supervisor role。进程对 shm object 获取 non-blocking exclusive advisory lock并持有到 Detach/进程退出。
- lock 被 live supervisor 持有时返回 `kWouldBlock`，不会触碰 state、allocator 或 recovery fence。
- lock 成功后，若旧 fence 为 `OWNED`，必须用 `ProbeProcessIdentity` 得到明确 `Dead`；同 PID、不同 start time/epoch 属于旧 incarnation（PID reuse），可以接管；`Alive` 或 `Unknown` 均拒绝 destructive recovery。
- 对 dead owner 的 `ACTIVE + !clean`，新 supervisor 先发布新 service identity/epoch，再明确写 `DIRTY`，之后才进入既有 recovery owner/lease/epoch scanner。
- recovery scanner 的每次 mutation 仍要求精确 `{recovery_epoch, RECOVERING}` fence；旧 scanner lease 到期后即使恢复执行，也不能提交 ACTIVE/QUARANTINED。

service liveness 不使用超时 lease。advisory lock 由内核在进程退出时释放，`ProcessIdentity` 用于 PID reuse 防护、异常元数据检测与持久诊断。两者同时成立才允许 ACTIVE takeover。

### Fork 约束

Supervisor attachment 不得跨 `fork()` 作为两个进程共同使用。子进程只能在 `exec`（`FD_CLOEXEC` 自动关闭 lock fd）后重新 Attach，或在 fork 后立即关闭/销毁继承的 Region handle。继承 handle 的子进程不具备相同 `ProcessIdentity`，Region API 的 fence 校验会拒绝其 Detach；但长期保留继承的 advisory-lock fd 会延迟 takeover，因此属于违反 API 契约。

## Alternatives Considered

- **ACTIVE service heartbeat lease**：暂停超过 lease 即误判死亡，可能在合法 writer 仍持有裸 SHM pointer 时扫描并回收对象；否决。
- **只看 `kill(pid, 0)`**：PID reuse 会让无关新进程冒充旧 owner；权限错误也可能被误判；否决。
- **40B 内压缩多进程 registry**：无法同时保存多个完整 incarnation、slot generation 和原子注册状态；进程在注册中途崩溃时无法可靠判断槽内容；否决。
- **继续要求外部手工写 DIRTY**：安全但 owner kill 后没有明确、可审计的恢复路径，且 API 保留模糊自动恢复语义；否决。
- **单 supervisor + 任意未注册 writable clients**：supervisor 死亡时无法证明 clients 已退出，仍可能 destructive scan 活跃 writer；否决。

## Consequences

- 正面：正常 ACTIVE Region 不会被 destructive scanner；SIGKILL 后可自动 `ACTIVE→DIRTY→RECOVERING→ACTIVE`；PID reuse 不会阻塞或冒充 owner；stale Detach 与 stale recovery commit 都受 epoch fence 限制。
- 正面：不依赖 service heartbeat，因此 `SIGSTOP`、长暂停和调度抖动不会导致 takeover。
- 负面：v3 明确不支持多个独立进程 writable Attach。多进程业务拓扑必须由 supervisor 代理写入，或等待后续 attachment-registry ABI。
- 负面：advisory lock 是 host-local 机制；Region 本身也是 host-local POSIX SHM，此约束一致。fork 后必须遵守上面的 handle 规则。
- 兼容：SuperBlock 仍为 256B；v3 reader 可只读 Attach v2；v2 writable Attach 被明确拒绝，必须 clean migrate/recreate 为 v3。

---

## 评审记录

| 日期 | 评审人 | 结论 | 说明 |
|---|---|---|---|
| 2026-08-01 | Mino D1 Recovery Review | ACCEPTED | 采用 supervisor-owner 契约关闭 v2 ACTIVE liveness 缺口；跨进程测试覆盖 live owner、SIGKILL takeover、PID reuse 与 recovery lease/epoch fencing。 |
