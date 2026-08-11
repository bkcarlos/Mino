# Mino automated fault drills

`tools/operations/drill_runner.py` 严格读取 `configs/drills/operations_drills.json`，逐场景直接执行现有 Bazel 测试或运维工具，不经 shell。清单未知字段、缺场景、重复 ID、未知 placeholder、空步骤、无界预算、expected-failure 接受退出 `0` 等均在执行前 fail closed。

## What is real

| 场景 | 实际执行路径 |
|---|---|
| `tls-credential-invalid` | 构建并调用真实 `mino-deploy preflight`；临时目录/证书引用有效，owner-only private key 刻意为空；只有退出 `4` 且精确 credential 拒绝文本出现才是 expected failure |
| `acl-denied` | loopback TCP Bridge pipeline + Topic authorizer；断言在 retention/dedup/ingress allocation 前返回 `PermissionDenied` |
| `bridge-disconnect-reconnect` | 主动关闭真实 loopback TCP connection，排队消息，重连、更新 epoch、fence stale hello 并验证 retransmit 清零 |
| `subscriber-lease-expired` | fork/process signal/MAP_SHARED broadcast/真实 ACK 与 lease boundary cleanup |
| `schema-mismatch` | Bridge connection manager、Schema negotiator/persistence 与 TCP reconnect scheduling |
| `storage-paused-enospc` | 实际 SegmentWriter 临时文件、ENOSPC/EIO/EROFS errno hook、blocking fsync、bounded RecorderBufferPool、scan/repair |
| `exporter-failure` | production exporter encoding、transactional bounded sink 和 export pipeline |
| `capacity-rejection` | production capacity controller 的 multi-resource atomic admission、emergency reserve 与并发竞争 |
| `upgrade-cutover-interrupted` | owner-only durable manifest、fsync/rename、CLI fail-closed cutover，以及 integration cutpoint crash/resume |

测试替身只用于注入边界（例如 errno、sink 或控制面 ack），不是用纯 mock 代替所有场景；网络、文件、共享内存、进程、编码器、容量控制器和持久化状态机均执行仓库现有实现。

## Quick local mode

用于本机和未提交开发工作树；脏树必须显式确认，并完整记录 commit、clean/dirty、`git status`、变更列表 SHA-256、工具链、CI 环境、输入 SHA-256、每步命令/退出码/预算/日志 SHA-256 和 cleanup 结果。

```sh
python3 tools/operations/drill_runner.py quick \
  --allow-dirty \
  --clean \
  --out .cache/operations-drill-quick
```

若工作树 clean，省略 `--allow-dirty`。`--clean` 只会清理由 marker 标识的既有 evidence 目录；不会删除任意目录。默认每个场景后删除 scratch，`--keep-scratch` 仅用于本地诊断。

## Qualification mode

Qualification 必须在 clean worktree 上绑定精确 40 位 commit；不接受 `--allow-dirty`。它增加 TLS negative handshake、Bridge HWM/restart、SIGSTOP lease safety、Storage SIGKILL cutpoint、monitoring HTTP、容量集成和 upgrade conservation integration。

```sh
python3 tools/operations/drill_runner.py qualification \
  --expected-commit <40-hex-git-sha> \
  --clean \
  --out .cache/operations-drill-qualification
```

任何 scenario/step 未执行、超时、退出码不符、required output marker 不符、cleanup 失败或总预算耗尽都会使 suite 失败。qualification evidence 只有在 `source_state=clean`、commit 匹配且全部场景通过时有效。

## Watchdogs and cleanup

- suite 总预算：quick 1200 秒，qualification 5400 秒；
- 每个场景另有更小预算；实际 watchdog 取 suite 剩余与 scenario 剩余的最小值；
- subprocess 独立 process group，超时先 `SIGTERM`，5 秒后 `SIGKILL`；
- Ctrl-C 同样清理 process group 并把未执行场景标记失败；
- 每场景 scratch 路径通过 `MINO_DRILL_SCRATCH` 暴露，正常/失败都 cleanup；
- Bazel 测试自身仍有 `--test_timeout`，形成内外两层有界保护。

## Evidence files

```text
.cache/operations-drill-quick/
  .mino-operations-drill
  drill-manifest.json
  summary.json
  logs/<scenario>-<step>.log
```

`drill-manifest.json` 是运行中的原子 checkpoint，也是最终完整 provenance；初始 `outcome=running` 且所有场景在 `missing_or_failed_scenarios` 中，只有每步退出、marker、hash 和 cleanup 全部确认后才移除，因而中断不会产生假阳性 manifest。`summary.json` 指向最终 manifest SHA-256。日志可能较大且含机器信息，不提交；事故系统按保留策略存储。

## Updating the suite

1. 先为故障路径提供有界、可重复、能断言止损/恢复的真实测试或工具。
2. 在严格场景清单同时登记 quick/qualification、预算、expected exit、输出证据、runbook anchor 和 alert ID。
3. 更新 `runbook.md` 的检测/确认/止损/恢复/验证/升级条件。
4. 更新告警（若有新注册 metric），并运行契约测试。
5. 运行一轮 quick；只把去机器化的小型结果更新到 `quick-drill-summary.json`。

```sh
bazel test //tools/operations:operations_contract_test \
  //configs/alerts:alert_rules_test --test_output=errors
```
