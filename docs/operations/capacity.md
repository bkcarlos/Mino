# Production Topic capacity report

D6-15 的容量报告门禁覆盖节点上**全部生产 Topic**，而不是抽样 Topic。报告由严格节点 deployment TOML、Coordinator 的原子 Topic snapshot、受审阅 production inventory 和节点预算共同生成；任一输入缺失、重复、unknown、漂移或预算不足都生成 `FAIL` 并返回非零。

## Inputs and trust boundary

| 输入 | 约束 | 用途 |
|---|---|---|
| `--deployment-config`（可重复） | 每份先由 `mino-deploy validate` 的严格 schema 验证；Node ID/名称必须唯一 | 绑定环境、节点、Region、SHM、FD、线程、Bridge、Monitoring 上限；static route 的目标必须出现在配置集合中 |
| Coordinator snapshot | `mino.coordinator.topic-snapshot.v1`；Topic ID/名称唯一且状态为 `active` | 提供同一 Coordinator 视图中的 Topic、partition、schema、route version/targets、usage 和 recorder/bridge count |
| production inventory | `mino.capacity.production-inventory.v1`；生产集合不得为空 | 受审阅的期望 Topic 集合及峰值、slab class、recorder/disk-pause 预算；`qualification_approved=false` 的样例只能生成 nonqualification 报告 |
| node budget | `mino.capacity.node-budget.v1` | Hardware profile、limit、emergency reserve、Region/Slab/Recorder/Bridge/Monitoring 成本与上限 |
| artifact schema | [`capacity-report.schema.json`](capacity-report.schema.json) | 固定 `mino.capacity.report.v1` 输出契约 |

配置集合、Coordinator snapshot 集合和最终报告集合按 `(topic_id, name)` 做完全相等比较。报告还逐项比较 channel、capacity、publisher/subscriber maxima、partition count、record topology、primary/accepted schema、route policy/version/targets；因此 missing、unknown、重复或 drift 均不能 PASS。启用 recorder 的 Topic 必须恰有一个 Coordinator recorder，Bridge usage 必须与 inventory lane 数一致。

## Resource accounting

节点报告的每个维度均输出：

- `limit`：受审阅的硬上限；
- `committed`：固定 Monitoring/Base 成本加全部生产 Topic 的规划峰值；
- `emergency`：数据面不可消费的应急保留；
- `headroom = max(0, limit - emergency - committed)`。

维度包括 SHM、Region、Slab、Topic、partition、publisher、subscriber、Bridge lane/egress、Recorder buffer/disk throughput、Monitoring、FD 和线程。Slab 同时按 class 输出 slot、limit、committed、headroom、emergency，Topic 使用未声明 class 或 slot 不一致会失败。

每个 Topic 在 artifact 中枚举：

1. primary 和全部 accepted schema identity；
2. discovery route 或每个 static target；
3. 从 `0` 开始的每个 partition、对应 Region/Slab bytes 和 slab class；
4. recorder ID、disk pause 秒数、buffer bytes、disk bytes/s（未录制时为 `null`）；
5. Topic 的完整资源向量。

Recorder buffer 按 `max(configured buffer, disk bytes/s × disk-pause seconds)` 计算。Bridge egress 按峰值消息率、消息大小和 lane 数计算。任何 committed 超过 `limit - emergency` 都附带机器可读 rejection，其中含 dimension、requested、available 和明确原因。

## CLI

先构建严格 deployment validator 和报告命令：

```sh
bazel build //tools/deployment:mino_deploy //tools/deployment:capacity_report
```

检查仓库样例（预期 `PASS`，但 `qualification_eligible=false`）：

```sh
bazel-bin/tools/deployment/capacity_report report \
  --repo . \
  --deployment-config configs/node.production-edge.toml \
  --deployment-validator bazel-bin/tools/deployment/mino_deploy \
  --coordinator-snapshot configs/capacity/coordinator-topic-snapshot.sample.json \
  --inventory configs/capacity/production-inventory.sample.json \
  --budget configs/capacity/node-budget.sample.json \
  --contract-schema docs/operations/capacity-report.schema.json \
  --output /tmp/mino-capacity-sample.json

bazel-bin/tools/deployment/capacity_report verify \
  --artifact /tmp/mino-capacity-sample.json
```

样例 inventory 明确设置 `qualification_approved=false`，hardware architecture 为 `any`，所以不能被误用为资格证据。生产 inventory 为空时，连普通 report 也不得 PASS。

## What-if

`--peak-multiplier` 对全部 Topic 的消息率、规划 publisher/subscriber/Bridge lanes 及 recorder throughput 做向上取整缩放。以下增量仅作用于 `--what-if-topic` 选中的 Topic；未选择 Topic 时请求增量会失败：

```sh
bazel-bin/tools/deployment/capacity_report report \
  ... \
  --peak-multiplier 1.5 \
  --what-if-topic telemetry/events \
  --disk-pause-seconds 30 \
  --add-publishers 1 \
  --add-subscribers 8 \
  --add-bridge-lanes 1 \
  --add-partitions 2
```

What-if 首先检查 Coordinator 的 publisher/subscriber maxima，然后检查节点各维度及 slab class 数据面 ceiling。拒绝不会只给布尔值：`rejections[]` 保留 policy/budget 类型、Topic 或节点 scope、requested、available 和原因。

## Qualification runner

资格运行使用同一 CLI 的 `qualify` 子命令，并额外 fail closed：

- `--expected-commit` 必须是当前 `HEAD` 的完整 40 位 SHA；
- tracked/untracked 工作树必须 clean；
- inventory 必须是 `environment=production` 且 `qualification_approved=true`；
- hardware architecture 必须精确指定，CPU/内存不得低于 profile；
- Topic inventory 非空、三集合完全一致、无 drift、无 rejection；
- 所有输入记录 exact path/bytes/SHA-256；artifact 记录 hardware、完整 invocation/verification commands、schema、`PASS`/`FAIL` 和 canonical `report_sha256`。

```sh
bazel-bin/tools/deployment/capacity_report qualify \
  --repo . \
  --expected-commit "$(git rev-parse HEAD)" \
  --deployment-config /etc/mino/node.toml \
  --deployment-validator bazel-bin/tools/deployment/mino_deploy \
  --coordinator-snapshot /var/lib/mino/evidence/coordinator-topic-snapshot.json \
  --inventory /etc/mino/production-topic-inventory.json \
  --budget /etc/mino/node-capacity-budget.json \
  --contract-schema docs/operations/capacity-report.schema.json \
  --output /var/lib/mino/evidence/capacity-report.json

bazel-bin/tools/deployment/capacity_report verify \
  --artifact /var/lib/mino/evidence/capacity-report.json \
  --require-qualified \
  --expected-commit "$(git rev-parse HEAD)"
```

`verify` 会重新计算 report canonical SHA，并重新读取每个输入检查 bytes/SHA；报告或输入被篡改都会失败。GitHub 的手工 self-hosted runner 入口是 `.github/workflows/capacity-report.yml`，由受控 runner 提供生产输入绝对路径并上传完整 artifact。

## Validation

```sh
bazel test //tools/deployment:capacity_report_test
```

契约测试覆盖：样例 nonqualification、空 inventory、Coordinator 集合漂移、预算不足、峰值/disk-pause/participant/lane/partition what-if，以及 report/input hash 篡改。
