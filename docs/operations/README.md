# Mino operations manual index

本目录是生产部署、值班处置和故障演练的入口。统一处置原则：fail closed、先保全证据再变更、只使用受审阅配置、TLS/ACL/Schema/CRC/容量门禁不可作为临时恢复手段绕过。每个标准手册项均包含“检测指标/告警、确认命令、止损、恢复、验证、升级条件”。

## Standard runbooks

| 主题 | 统一手册 | 深入文档 |
|---|---|---|
| 部署 / preflight | [Deployment and preflight](runbook.md#deployment-and-preflight) | [deployment.md](deployment.md) |
| 启动停止 | [Start and stop](runbook.md#start-and-stop) | [deployment.md](deployment.md#preflight-and-startup) |
| 证书轮换 | [Certificate rotation](runbook.md#certificate-rotation) | [deployment.md](deployment.md#preflight-and-startup) |
| Region / ACL 拒绝 | [Region and ACL denial](runbook.md#region-and-acl-denial) | [monitoring.md](monitoring.md#tls-and-acl) |
| Bridge 断链 | [Bridge disconnection](runbook.md#bridge-disconnection) | [monitoring.md](monitoring.md#bridge-disconnected) |
| Subscriber lease 过期 | [Subscriber lease expiration](runbook.md#subscriber-lease-expiration) | [monitoring.md](monitoring.md#lease-expiration) |
| Schema | [Schema incident](runbook.md#schema-incident) | [rolling_upgrade.md](rolling_upgrade.md#1-安全与一致性原则) |
| Storage 磁盘故障 | [Storage disk failure](runbook.md#storage-disk-failure) | [monitoring.md](monitoring.md#storage-failure) |
| 容量 | [Capacity exhaustion](runbook.md#capacity-exhaustion) | [capacity.md](capacity.md) / [monitoring.md](monitoring.md#capacity-exhaustion) |
| 监控 / Exporter | [Monitoring and exporter failure](runbook.md#monitoring-and-exporter-failure) | [monitoring.md](monitoring.md) |
| 滚动升级 | [Rolling upgrade](runbook.md#rolling-upgrade) | [rolling_upgrade.md](rolling_upgrade.md) |
| 备份恢复 | [Backup and restore](runbook.md#backup-and-restore) | Storage inspect/replay 命令见统一手册 |
| 事故升级 | [Incident escalation](runbook.md#incident-escalation) | [Common evidence envelope](runbook.md#common-evidence-envelope) |

## Fault drills

- [演练执行、预算、证据与 qualification 门禁](drills.md)
- [场景清单](../../configs/drills/operations_drills.json)
- [本次 quick 小型归档](quick-drill-summary.json)
- [证据 manifest 示例 schema](drill-manifest.schema.json)

临时日志和完整机器 provenance 只保存在 `.cache/operations-drill-*`；仓库只归档去机器化的小型 summary，不提交大日志、主机名、绝对临时路径或 secrets。

## Contract validation

```sh
bazel test //tools/operations:operations_contract_test \
  //configs/alerts:alert_rules_test
```

契约测试会验证：必需主题的六段式手册、场景集合、真实测试过滤器、告警名称、runbook anchor、expected-failure 证据和 fail-closed JSON schema。修改告警、场景或手册时必须在同一变更中更新所有绑定。
