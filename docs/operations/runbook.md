# Mino operations runbook

本文是 Mino 生产运维的统一处置手册。所有命令中的 `/etc/mino/node.toml`、Region 名称、session 路径和监控地址都必须替换为现场值；先记录时间、目标、部署 revision 和执行人。除已明确标注的 repair/apply 操作外，优先执行只读命令。不得为恢复服务而关闭 TLS、放宽 ACL、跳过 Schema/CRC 校验、修改共享内存内容或清除尚未保全的 journal/manifest。

## Common evidence envelope

每次处置最少保留：UTC 起止时间、告警表达式、Git/deployment revision、节点与 Security Domain（放在受控事故记录中，不作为 Prometheus label）、配置文件 SHA-256、`/-/healthy` 和 `/metrics` 快照、相关日志、执行命令与退出码、止损动作、恢复验证、值班交接。私钥、token、完整证书包和业务 payload 不得进入工单或演练日志。

自动演练场景由 `configs/drills/operations_drills.json` 唯一声明；执行方法见 [Fault drills](drills.md)。场景日志属于临时证据，保存在 `.cache/operations-drill-*`，不提交仓库。

## Alert and scenario map

| 故障域 | 告警/检测器 | 自动场景 |
|---|---|---|
| Monitoring | `MinoMonitoringDown`, `MinoMonitoringSnapshotStale` | `exporter-failure` |
| Queue/slab | `MinoQueueNearCapacity`, `MinoQueueDrops`, `MinoSlabAllocationFailures`, `MinoSlabCorruption` | `capacity-rejection`（slab corruption 只做契约测试，不自动制造生产式损坏） |
| Lease | `MinoLeaseExpirations`, `MinoLeaseHeartbeatStale` | `subscriber-lease-expired` |
| Bridge | `MinoBridgeDisconnected`, `MinoBridgeReconnectFailures` | `bridge-disconnect-reconnect`, `schema-mismatch` |
| Storage | `MinoStorageWriteFailures`, `MinoStorageBacklog` | `storage-paused-enospc` |
| Exporter | `MinoOtlpQueueNearCapacity`, `MinoOtlpDropsOrFailures` | `exporter-failure` |
| Capacity | `MinoCapacityHeadroomLow`, `MinoCapacityRejections` | `capacity-rejection` |
| TLS/ACL | `MinoTlsHandshakeFailures`, `MinoTlsCertificateExpiring`, `MinoAclDeniedSpike` | `tls-credential-invalid`, `acl-denied` |
| Upgrade | `mino upgrade status` phase/deadline、supervisor cutover 日志；当前无专用 Prometheus rule | `upgrade-cutover-interrupted` |

## Deployment and preflight

**检测指标/告警**：部署门禁失败、launcher 退出 `3/4`、readiness exec 失败、`MinoMonitoringDown`。重点观察文件权限/ownership、`RLIMIT_NOFILE`、data filesystem free bytes 与 `storage.min_free_bytes`；健康端点只证明进程/监控存活，不证明 Registry、lease 或 Bridge ready。

**确认命令**：

```sh
bazel-bin/tools/deployment/mino_deploy validate --config /etc/mino/node.toml
bazel-bin/tools/deployment/mino_deploy preflight --config /etc/mino/node.toml
ulimit -n
df -Pk /var/lib/mino/data
df -Pi /var/lib/mino/data
```

确认配置是当前 UID 拥有、单 hard-link、非 symlink、不可 group/world writable；三个 storage 目录和三个 TLS 引用均为稳定的实际文件/目录。

**止损**：从负载均衡和 publisher admission 摘除未 ready 节点；不要反复重启；不要把 PEM 内容写入 TOML/环境变量；不要为通过 preflight 放宽 private key 权限、降低安全域约束或伪造 free-space 门限。

**恢复**：修复挂载、owner/mode、资源 limit 或磁盘空间；对配置执行 reviewed 变更后重新 `validate` 和 `preflight`。保持原失败配置及 SHA-256 作为证据。

**验证**：preflight 退出 `0`；`GET /-/healthy` 成功；Registry/lease/Topic/Bridge 的组合级 readiness 也成功；观察至少一个聚合周期，确认没有新部署/证书/存储告警。

**升级条件**：同批次两个节点失败、生产节点 >10 分钟无法 ready、需要降低 TLS/ACL/Schema/容量门禁、配置来源或 secret provenance 不明，升级为 SEV-2；疑似凭据泄露或数据完整性问题直接 SEV-1。

## Start and stop

**检测指标/告警**：进程退出/重启计数、`up{job="mino"}`、`MinoMonitoringDown`、shutdown grace 超时、SIGKILL、未 drain publisher/subscriber、Bridge 未关闭或 storage pending bytes 非零。

**确认命令**：

```sh
MINO_NODE_CONFIG=/etc/mino/node.toml tools/deployment/mino-node-start --dry-run /usr/local/bin/mino-node
curl --fail --silent http://127.0.0.1:9464/-/healthy
curl --fail --silent http://127.0.0.1:9464/metrics
kill -TERM "$(cat /run/mino/mino.pid)"
```

由 supervisor 负责 PID identity；不要只按进程名杀进程。停止前确认 Topic drain、publisher fence、subscriber/receipt/borrow/pin 和 storage queue 状态。

**止损**：启动失败时保持节点不接流量；停止超时期间冻结新 admission。第一次 `SIGTERM` 后等待配置的 `node.shutdown_grace_ms`；只有确认卡死且证据已保存才允许 supervisor 的有界 `SIGKILL`。

**恢复**：修复根因后通过 launcher 启动；若被强杀，先运行 Region/storage 的只读 inspect/verify，再允许恢复扫描；不要跳过持久化恢复直接接流量。

**验证**：进程 PID/incarnation 更新；health、metrics、Registry lease、Topic readiness 和 Bridge session 全部正常；旧 PID 不再持有 lease/pin；storage manifest/segment verify 通过。

**升级条件**：需要 SIGKILL、重复 crash loop、无法证明旧 publisher 被 fenced、lease/pin 无法清理或恢复扫描报告 corruption，至少 SEV-2；消息守恒/持久化边界不明为 SEV-1。

## Certificate rotation

对应场景：`tls-credential-invalid`。

**检测指标/告警**：`MinoTlsCertificateExpiring`、`MinoTlsHandshakeFailures`、credential preflight 退出 `4`、握手日志中的 trust/SAN/principal/domain 错误。不得把证书 subject 或 peer ID 加为 Prometheus label。

**确认命令**：

```sh
openssl x509 -in /run/secrets/mino/tls.crt -noout -dates -issuer -subject
openssl verify -CAfile /run/secrets/mino/ca.pem /run/secrets/mino/tls.crt
stat -f '%Su %Sp %z %N' /run/secrets/mino/ca.pem /run/secrets/mino/tls.crt /run/secrets/mino/tls.key
bazel-bin/tools/deployment/mino_deploy preflight --config /etc/mino/node.toml
```

Linux 使用 `stat -c '%U %A %s %n' ...`。同时确认 SAN 中的 Mino principal 与 node/security domain 绑定、时间同步和 credential generation 单调。

**止损**：若疑似泄露，立即停止受影响 identity 的新连接/admission，并在 PKI 侧撤销；保持既有 ACL/TLS fail-closed。不得临时信任未知 CA、关闭 mTLS、复用旧 generation 或把 symlink secret 当稳定文件。

**恢复**：通过 secret manager 发布新的 CA bundle、chain、owner-only key 三件套；使用稳定 regular-file mount；preflight 后按节点滚动重启。当前具体 node composition 没有文档化的通用 reload signal，因此不要假设热重载。

**验证**：新旧兼容窗口内双向握手成功；peer principal/domain 正确；新 certificate fingerprint/generation 生效；Bridge 恢复且握手失败 counter 不再增长；旧凭据在完成 rollout 后撤销。

**升级条件**：私钥泄露、CA compromise、跨 Security Domain 接受、SAN/principal 错绑为 SEV-1；到期 <24h 且不能完成滚动、超过一个节点握手失败为 SEV-2。

## Region and ACL denial

对应场景：`acl-denied`。

**检测指标/告警**：`MinoAclDeniedSpike`、Region attach `PermissionDenied`、security-domain/UID/GID/mode/immutable CRC 日志、publisher/subscriber registration 拒绝。ACL 拒绝本身可能是正确防护，先区分攻击、陈旧配置和错误 identity。

**确认命令**：

```sh
bazel-bin/tools/mino/mino inspect /mino-region --output /tmp/mino-region-inspect.txt
shasum -a 256 /etc/mino/node.toml
curl --fail --silent http://127.0.0.1:9464/metrics
```

将受控 security 日志中的 node/domain/topic/version 与 Registry immutable Topic snapshot、当前部署 revision 对照；不要把这些 identity 复制到公开工单。

**止损**：保持拒绝；隔离异常来源；暂停受影响 Topic 的新 publisher/admission。不得手工修改 Region SuperBlock、放宽文件 mode、增加 wildcard grant、用“同域默认允许”替代显式 grant或原位修改 active ACL。

**恢复**：修正节点 identity/credential 或通过受审阅的 Topic replacement 更新 ACL；Region attach 权限由部署 owner/mode 修复。active ACL 变化走 replacement/cutover，不做 in-place ABI/config mutation。

**验证**：授权主体可执行最小权限动作，未授权主体仍返回 `PermissionDenied`；ACL version 不回退；Region immutable CRC/identity 正常；`mino_acl_denied_total` 仅随预期负向探针增加。

**升级条件**：跨域访问成功、ACL 绕过、Region identity/CRC 异常或权限扩大未经审批为 SEV-1；合法生产流量持续拒绝 >5 分钟或多 Topic 同时受影响为 SEV-2。

## Bridge disconnection

对应场景：`bridge-disconnect-reconnect`。

**检测指标/告警**：`MinoBridgeDisconnected`、`MinoBridgeReconnectFailures`、connected/configured gauge、reconnect/protocol failure counter、retransmit entries、queue depth、TLS/Schema 伴随告警。

**确认命令**：

```sh
curl --fail --silent http://127.0.0.1:9464/metrics
nc -vz peer.example 7443
openssl s_client -connect peer.example:7443 -CAfile /run/secrets/mino/ca.pem -verify_return_error </dev/null
```

确认两端 revision、session epoch、expected peer domain、schema availability、route generation 和 firewall；`openssl s_client` 只做 TLS 诊断，不代表 Mino session discovery ready。

**止损**：停止可选跨域 ingress 或在上游限流；保留可靠消息的 bounded retransmit 状态；若队列接近上限，按 Topic 策略拒绝新工作。不得跳过认证、ACL、Schema、epoch fencing、dedup/replay protection。

**恢复**：修复网络、peer、证书或 Schema 后让 connection manager 按有界 backoff 重连；保持新 session epoch，使用 peer HWM 重放 pending 数据；不要复制旧连接状态到新 session。

**验证**：两端 active；epoch 已更新；reconnect counter 有界增加后稳定；pending/retransmit 清零；测试消息恰好一次到达或符合声明的 Topic reliability；无 duplicate/unexplained loss。

**升级条件**：可靠消息守恒无法证明、stale epoch 被接受、跨域 peer 被接受为 SEV-1；双向断链 >5 分钟、reconnect storm 或 backlog 达容量门限为 SEV-2。

## Subscriber lease expiration

对应场景：`subscriber-lease-expired`。

**检测指标/告警**：`MinoLeaseExpirations`、`MinoLeaseHeartbeatStale`、oldest heartbeat age、publisher backpressure、pending ACK/pin。区分 dead、SIGSTOP/CPU starvation 和 liveness unknown；unknown 不得破坏性驱逐。

**确认命令**：

```sh
curl --fail --silent http://127.0.0.1:9464/metrics
ps -o pid,ppid,state,lstart,etime,command -p <pid>
bazel-bin/tools/mino/mino inspect /mino-region --output /tmp/mino-lease-inspect.txt
```

核对 PID incarnation、lease epoch、monotonic heartbeat、scheduler/CPU pressure 和 Registry registration generation。

**止损**：冻结受影响 subscriber 的新工作并限制 publisher ingress；活进程或 liveness unknown 时不驱逐、不清 ACK；不得仅因 wall clock 跳变认定 lease 过期。

**恢复**：恢复调度/心跳；仅在 owner 确认 dead 且达到精确 lease 边界后，由协调器驱逐并运行 pin/ACK cleanup；重启 subscriber 时使用新 generation/lease epoch。

**验证**：旧 lease 进入 evicted，旧 generation 无法 heartbeat；blocked publisher 恢复；ACK/pin 无泄漏；新 subscriber 消费正常且 expiration counter 不继续异常增长。

**升级条件**：活 owner 被驱逐、数据被过早回收或 PID reuse 误判为 SEV-1；多个 subscriber 因系统性 CPU/clock 问题过期或 >5 分钟无法恢复为 SEV-2。

## Schema incident

对应场景：`schema-mismatch`。

**检测指标/告警**：Bridge protocol/reconnect failure、`SchemaMismatch`/`NotFound` 日志、Schema negotiation 拒绝、storage/replay 校验失败。Schema 没有高基数 label；通过受控日志和 manifest identity 关联。

**确认命令**：

```sh
bazel-bin/tools/mino/mino storage inspect /var/lib/mino/data/<session>
bazel-bin/tools/mino/mino replay /var/lib/mino/data/<session> --validate-only
shasum -a 256 /var/lib/mino/schemas/*
```

核对 full canonical digest、short ID、schema version、layout version、Topic config/region/channel/ACL version；short ID 相同不能代替完整 digest 相同。

**止损**：阻止未知 Schema 的 publisher 和 Bridge publication；保留 descriptor artifact、manifest 和失败 payload 的受控证据；不得强制映射 short ID、忽略 digest、覆盖历史 descriptor 或绕过 payload CRC。

**恢复**：部署正确、签审过的 descriptor artifact；通过 Schema store 的原子持久化和 negotiation transaction 发布；不兼容变化使用新 Topic/Region + drain/cutover，不原位改 ABI。

**验证**：双向兼容检查通过；完整 identity 匹配；buffered batch 只在 transaction commit 后释放；storage inspect/replay validate 成功；Bridge reconnect 后无重复发布。

**升级条件**：错误 Schema 已写入 durable recording、同 identity 对应不同 digest、错误解码可能导致数据完整性问题为 SEV-1；生产 Topic 因缺失 Schema 停止 >5 分钟为 SEV-2。

## Storage disk failure

对应场景：`storage-paused-enospc`。

**检测指标/告警**：`MinoStorageWriteFailures`、`MinoStorageBacklog`、filesystem bytes/inodes、write/fsync latency、TopicWriter error、buffer pool bytes/queue/drop/timeout。ENOSPC 映射为 `ResourceExhausted`，写入错误对 writer 是 sticky。

**确认命令**：

```sh
df -Pk /var/lib/mino/data
df -Pi /var/lib/mino/data
bazel-bin/tools/mino/mino storage inspect /var/lib/mino/data/<session>
bazel-bin/tools/mino/mino storage verify /var/lib/mino/data/<session>/<segment>
```

同时检查 mount read-only、quota、I/O error、inode、manifest generation 和最后完整 ingestion sequence。

**止损**：停止新 recording admission；按既定 buffer policy 限制上游，保持 pool 有界；隔离失败 writer/topic，健康 Topic 可继续。不得删除 manifest/journal/未验证 segment，不得无限扩大内存 queue，不得在 active writer 上 destructive repair。

**恢复**：扩容或释放经过确认的非 Mino 空间；恢复 mount/I/O；停止 writer 后先 `storage verify`，对 repairable segment 先备份，再执行 `storage repair <segment> --apply --standalone`。重新创建 writer，不复用 sticky error 实例。

**验证**：filesystem headroom/inode 恢复；verify/inspect clean；durable record count 和 last complete sequence 符合保全点；buffer backlog 排空；新写+fsync+replay validate 成功；无跨 Topic 故障传播。

**升级条件**：corruption、repair 后 sequence 不守恒、manifest/segment identity 不一致或多副本同时失败为 SEV-1；ENOSPC、read-only 或 backlog >10 分钟为 SEV-2。

## Capacity exhaustion

对应场景：`capacity-rejection`。

**检测指标/告警**：`MinoCapacityHeadroomLow`、`MinoCapacityRejections`、`MinoQueueNearCapacity`、`MinoQueueDrops`、`MinoSlabAllocationFailures`、`MinoSlabCorruption`。比较 usage、headroom、minimum headroom 和 emergency reserve；counter 使用 `increase(...[5m])`。

**确认命令**：

```sh
curl --fail --silent http://127.0.0.1:9464/metrics
ulimit -n
bazel-bin/tools/mino/mino inspect /mino-region --output /tmp/mino-capacity-inspect.txt
```

按资源维度检查 SHM/slab/Topic/thread/FD/Bridge egress/recorder buffer，而不是只看总内存。

**止损**：拒绝可选 admission、限流 ingress、暂停非关键 recording/Bridge 路由；保留 emergency reserve。`MinoSlabCorruption` 立即 quarantine Region；不得手工减 accounting、切换 lossless Topic 为 dropping、复用 corrupt slab。

**恢复**：通过正常 rollback/release 归还泄漏 reservation；修复慢消费者；部署经 validation 的更大 node budget 或扩节点。容量变更保持原子 multi-resource admission，不拆成部分预留。

**验证**：headroom 高于 floor；rejection/drop 不再增长；并发 admission 不 oversubscribe；reservation 失败无 pending charge；业务 SLO 和 queue latency 恢复。

**升级条件**：slab corruption、accounting 负值/overflow、emergency reserve 被数据面消耗为 SEV-1；关键流量持续拒绝、queue >90% 超过 5 分钟或多个资源同时耗尽为 SEV-2。

## Monitoring and exporter failure

对应场景：`exporter-failure`。

**检测指标/告警**：`MinoMonitoringDown`、`MinoMonitoringSnapshotStale`、`MinoOtlpQueueNearCapacity`、`MinoOtlpDropsOrFailures`、Prometheus rejected/failure counters、OTLP queue depth/capacity/drop/failure。业务面可继续运行，但必须建立替代观测。

**确认命令**：

```sh
curl --fail --silent --max-time 2 http://127.0.0.1:9464/-/healthy
curl --fail --silent --max-time 2 http://127.0.0.1:9464/metrics
lsof -nP -iTCP:9464 -sTCP:LISTEN
```

从同一 network namespace 检查 bind、FD、worker、request/response bounds；比较 Prometheus 与 OTLP，确认是 endpoint、sink、collector 还是 snapshot 聚合故障。

**止损**：启用受控 host/supervisor 日志和进程探针作为临时观测；隔离失败 exporter。不得让 sink 变 blocking/unbounded，不得把 endpoint 暴露到不受保护网络，不得因 exporter 故障停止数据面。

**恢复**：修复端口/FD/collector；重启独立 monitoring composition 或滚动节点；保持 bounded transactional `TryBegin/Commit/Abort` 语义。只调整经文档化的固定 bounds。

**验证**：health/metrics 成功；snapshot age <2 个 aggregation interval；OTLP queue 回落；一次故障导出被 abort、无 partial payload；producer latency/业务线程不受影响。

**升级条件**：全环境无任何观测 >5 分钟、监控故障掩盖同时发生的数据/安全事故为 SEV-2；敏感 identity/payload 泄露到 telemetry 为 SEV-1。

## Rolling upgrade

对应场景：`upgrade-cutover-interrupted`；详细协议见 [rolling_upgrade.md](rolling_upgrade.md)。

**检测指标/告警**：`mino upgrade status --manifest <file>` 的 phase/generation/journal、supervisor readiness/drain/cutover/observe deadline、duplicate/unexplained loss、publisher counts。当前没有专用 Prometheus upgrade rule，必须由 deployment automation 对 phase deadline 和 proof failure 告警。

**确认命令**：

```sh
bazel-bin/tools/mino/mino upgrade status --manifest /var/lib/mino/upgrade/operation.manifest
bazel-bin/tools/mino/mino inspect /mino-old --output /tmp/mino-old.txt
bazel-bin/tools/mino/mino inspect /mino-new --output /tmp/mino-new.txt
```

核对 commit token、source/target Region ID+UUID+layout+domain、Topic/ACL/Schema binding、capacity proof、drain conservation proof 和 route generation。

**止损**：在 prepare/validate/drain 可按流程 rollback；cutover/observe 默认 forward fix，并保持旧 publisher fenced。不得以 sleep 代替 proof、生成第二 token、删除 manifest、无安全证明回切或让 source/target 同时发布。

**恢复**：进程中断后先 `status`，使用同 token 与绑定 target identity 的新鲜 evidence 执行 `resume --apply`；只有 target publisher fenced、source ready 且 publication reconciliation 完整时执行 post-cutover rollback。

**验证**：phase 为 `commit` 或有证明的 `rollback`；active token/Region 唯一；旧 publisher 为 0；新 publisher 正常；序列+receipt 守恒，duplicate/unexplained loss 为 0；source retire 仍通过 Registry pin/participant 门禁。

**升级条件**：双 publisher、stale token 接管、duplicate/unexplained loss、manifest CRC/phase chain 异常为 SEV-1；cutover/observe 超过变更窗口、target readiness 反复失败为 SEV-2并冻结后续批次。

## Backup and restore

**检测指标/告警**：备份作业 age/failure、snapshot generation、对象存储 checksum、restore rehearsal age、`MinoStorageWriteFailures`。仓库当前没有通用在线 backup CLI；备份编排必须由部署系统对静止 storage session、Schema store、配置和 upgrade manifest 做一致快照。

**确认命令**：

```sh
bazel-bin/tools/mino/mino storage inspect /var/lib/mino/data/<session>
bazel-bin/tools/mino/mino replay /var/lib/mino/data/<session> --validate-only
shasum -a 256 /etc/mino/node.toml /var/lib/mino/data/<session>/manifest
```

记录 session/recording identity、manifest generation、segment 列表与 SHA-256、Schema artifact digest、Region/Topic/ACL version；不备份 TLS private key 到数据备份。

**止损**：恢复期间隔离目标目录和 Topic，禁止 publisher/recorder 写入；保护原始介质为只读副本。不得将备份覆盖活动 session、混用不同 generation、跳过 checksum/Schema 校验或把旧 ACL 当作当前授权。

**恢复**：在空、owner-only、同 filesystem policy 的新目录恢复；先恢复 Schema + manifest + tracked segments，校验后 replay validate；通过新 node/Topic/Region composition cutover，保留原目录直到验收完成。

**验证**：所有 artifact checksum、manifest identity、segment verify 和 replay validate 通过；抽样/全量 sequence 数与备份 inventory 一致；恢复环境 ACL/TLS 使用当前受审配置；业务只读验证后再开放写入。

**升级条件**：无可用备份、checksum/identity 不一致、RPO/RTO 违约、恢复后数据缺失/重复或备份含私钥为 SEV-1；单次备份失败但 RPO 仍有冗余为 SEV-2/变更冻结并立即补跑。

## Incident escalation

**检测指标/告警**：任何本手册升级条件、多个独立故障域同时告警、证据链不完整、值班无法在 15 分钟内确定 blast radius。SEV-1：安全边界突破、数据损坏/不可解释丢失/重复、双 publisher、凭据泄露；SEV-2：生产不可用或关键 SLO 严重下降但 fail-closed/数据守恒仍成立；SEV-3：局部降级且有安全 workaround。

**确认命令**：

```sh
date -u
shasum -a 256 /etc/mino/node.toml
curl --fail --silent http://127.0.0.1:9464/metrics > /tmp/mino-metrics.txt
bazel-bin/tools/mino/mino inspect /mino-region --output /tmp/mino-inspect.txt
```

在事故系统记录 commander、operations、security、storage/data、communications 负责人；只上传去密证据与 SHA-256，不上传 secrets。

**止损**：SEV-1 立即冻结部署/升级和 destructive repair，隔离 affected Region/credential/route，保全日志与存储副本；SEV-2 冻结当前批次并限流。避免多人并发操作同一 manifest/Region。

**恢复**：由 incident commander 选择本手册对应恢复路径，明确单一执行人和 rollback/forward-fix 门槛；安全事故轮换/撤销凭据，数据事故先建立 forensic copy；每次动作记录前后证据。

**验证**：技术 owner 与独立 reviewer 双人确认安全边界、数据守恒、告警清除和业务 SLO；至少观察两个告警窗口；完成受影响节点/Topic/Region inventory 和交接。

**升级条件**：SEV-1 立即通知安全、存储/数据、平台负责人和管理层；5 分钟内设 commander，15 分钟内首次状态更新。SEV-2 在 10 分钟内设 commander，30 分钟内更新。超时、blast radius 扩大或恢复证据矛盾时提升一级；关闭事故前安排复盘和 qualification 演练。
