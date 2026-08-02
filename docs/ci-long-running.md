# Extended / long-running CI

Mino 将小时级验证与 PR CI 分开。所有 cron 均使用 UTC；长测 workflow 仅授予
`contents: read`，同一 ref 上不取消已经开始的长测，避免丢失部分故障证据。

## 触发方式

| Workflow | 手工触发 | 定时触发 | PR / push 行为 |
| --- | --- | --- | --- |
| `Extended Long-Running Validation` | `workflow_dispatch`，可设置 `duration_seconds`（0–7200）和请求 seed | 每周六 `01:41 UTC` | 不在 PR 或 push 触发 |
| `Reproducible Recovery Stress` | `workflow_dispatch`，可覆盖 uint64 `seed` | 每周日 `03:17 UTC` | 不在 PR 或 push 触发 |
| `D3 Extended Validation` | `workflow_dispatch` | 每周日 `02:23 UTC` | PR 仅运行 60 秒 fuzz smoke；主分支和定时运行 1 小时 fuzz |
| `Formal Validation` | `workflow_dispatch` | 每周一 `03:17 UTC` | 相关路径 PR 实际运行 `BroadcastMembership`（单模型 300 秒、总计 360 秒上限）；push 运行全部模型 |

在 GitHub Actions 页面选择对应 workflow 后使用 **Run workflow** 即可手工启动。
定时任务使用默认分支上的 workflow 定义。

## 覆盖矩阵

- **D1 MPMC TSAN**：`run_extended_long_test.py d1-mpmc-tsan` 显式运行
  `//mino/shm/channel:mpmc_ring_stress_test`，使用 `--config=tsan`、有限 Bazel
  timeout 和固定计数 conservation phase。
- **D1 / D2 recovery stress**：`recovery-stress.yml` 并行运行 Region recovery 和
  Runtime recovery，默认各 7200 秒。
- **D2 subscriber / broadcast kill**：循环运行 dead subscriber lease ACK 清理、
  cross-process ACK cleanup token 恢复和 publisher tombstone skip 场景。
- **D3 fuzz / TLA / CodeGen**：复用 `d3-extended-validation.yml` 与
  `formal-validation.yml`，避免同一小时级 fuzz campaign 被重复执行。
- **D4 双节点 / 重连长稳**：循环运行真实 fork + loopback TCP 双节点测试，以及
  Bridge reconnect / receiver restart 测试。
- **D5 fault campaign**：extended workflow 的 100 rounds **只作用于**
  `SigkillAtRecordWritesRepairsToLastCompleteCommit`，因为底层只有该测试读取 rounds/seed。
  短写、EINTR、ENOSPC、EIO、EROFS 与磁盘暂停测试仍由常规
  `//mino/storage:storage_fault_test` 每次各运行一次，不宣称被循环 100 次。

## PR 策略

PR 不运行小时级 target。常规 `CI` workflow 会：

1. 在 debug 和 TSAN 配置下显式**编译** manual MPMC stress target，但不执行长测；
2. 对 `tools/ci` Python 文件做语法检查、对 shell runner 做 `bash -n`；
3. 运行所有支持 `--self-test` 的 CI runner；
4. D3 相关 PR 只运行每 sanitizer 60 秒的 fuzz smoke；TLA 相关 PR 会实际运行一个有界 TLC 模型，而非只运行 runner self-test。

## Artifact manifest

长测 artifact 保留 90 天。每个 campaign 的 JSON manifest 至少记录：

- Git commit（`commit`）；
- seed 及 `seed_consumed`；无随机性的 TLA / CodeGen 写为 `null`，extended
  suites 因底层 target 无 seed 参数而记录 `seed: null`、请求值和
  `seed_consumed: false`，不宣称可复现测试内部行为；
- 实际命令或命令列表；
- 请求时长/timeout（适用时）和实际 elapsed time；TLA 分别记录
  `per_model_timeout_seconds` 与 `total_timeout_seconds`；
- console、test、fuzz、TLC 或 CodeGen 日志的 SHA-256；
- 统一顶层 `outcome`、`exit_code` 和 `github` run provenance。

主要文件名为 `manifest.json`；已有 D3/D5 campaign 保持兼容文件名
`campaign-manifest.json`。artifact 上传步骤使用 `if: always()`，以便测试失败时仍尽量
保留 runner 已完成写入的 manifest 和日志。

## 本地短 smoke

以下命令只验证 runner，不启动真实长测：

```sh
python3 tools/ci/run_extended_long_test.py --self-test
python3 tools/ci/run_recovery_stress.py --self-test
python3 tools/ci/run_d3_fuzz_campaign.py --self-test
python3 tools/ci/run_d5_storage_fault_campaign.py --self-test
python3 tools/ci/run_tla_validation.py --self-test
bash -n tools/ci/run_d3_codegen_environment.sh
```

`seconds=0` 只产出 `skipped` manifest，不启动任何 test。可用十秒预算检查真实
orchestration（编译时间也计入总预算）：

```sh
python3 tools/ci/run_extended_long_test.py d4-two-node-reconnect \
  --seconds=10 --seed=1 --out=/tmp/mino-d4-smoke
```
