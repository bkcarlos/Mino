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
| `Huge Page Validation` | `workflow_dispatch`，可选预留 2 MiB Huge Pages 并挂载 hugetlbfs | 无 | 仅 `[self-hosted, linux, mino-hugepage]` 手工运行；trap 恢复原 reservation/mount 权限，不修改公共 PR runner |
| `Physical Two-Host Mino Validation` | `workflow_dispatch`，必须提供两端地址 | 无 | 仅两个指定 self-hosted 物理节点执行真实 Mino 网络角色；PR 只做 fake-binary self-test |

在 GitHub Actions 页面选择对应 workflow 后使用 **Run workflow** 即可手工启动。
定时任务使用默认分支上的 workflow 定义。

## 覆盖矩阵

- **D1 MPMC TSAN**：`run_extended_long_test.py d1-mpmc-tsan` 显式运行
  `//mino/shm/channel:mpmc_ring_stress_test`，使用 `--config=tsan`、有限 Bazel
  timeout 和固定计数 conservation phase。
- **D1 / D2 recovery stress**：`recovery-stress.yml` 并行运行 Region recovery 和
  Runtime recovery，默认各 7200 秒。
- **D1 Huge Page**：`huge-page-validation.yml` 检查/可选配置 hugetlbfs 与预留页，验证
  实际 `MAP_HUGETLB`、smaps backing、跨进程 Open/读写，以及 Region Attach + Handle
  解析；主机配置和测试结果归档 90 天。详见 `docs/huge-pages.md`。
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
3. 运行所有支持 `--self-test` 的 CI runner，包括双机 server/client/manifest 编排的 loopback self-test；
4. D3 相关 PR只运行每 sanitizer 60 秒的 fuzz smoke；TLA 相关 PR会实际运行一个有界 TLC 模型，而非只运行 runner self-test；
5. 独立运行 `--config=hermetic` 的 Linux build/test；不会调度真实双物理主机 workflow。

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
python3 tools/ci/run_two_host_server.py --self-test
python3 tools/ci/run_two_host_client.py --self-test
python3 tools/ci/finalize_two_host_manifest.py --self-test
bash -n tools/ci/run_d3_codegen_environment.sh
```

`seconds=0` 只产出 `skipped` manifest，不启动任何 test。可用十秒预算检查真实
orchestration（编译时间也计入总预算）：

```sh
python3 tools/ci/run_extended_long_test.py d4-two-node-reconnect \
  --seconds=10 --seed=1 --out=/tmp/mino-d4-smoke
```

## Hermetic LLVM / Clang

`MODULE.bazel` 通过 Bzlmod 锁定 `toolchains_llvm` 1.4.0。下载的编译器版本按 host
平台固定：Linux x86_64/aarch64 与 Apple arm64 使用 LLVM/Clang 18.1.8；由于 LLVM
18 不提供 Intel macOS 官方 archive，Apple x86_64 明确固定为 15.0.7。扩展及 archive
解析结果写入 `MODULE.bazel.lock`。

工具链没有全局注册。默认构建仍使用宿主 GCC/Apple Clang，现有 GCC 12 CI 矩阵继续
作为兼容性验证；只有显式传入 `--config=hermetic` 才通过
`@mino_llvm//:all` 选择扩展为**当前 host 平台**生成的 toolchain。因此配置不写死
Linux platform，也不会要求 macOS 本地具备 Linux x86_64：

```sh
bazel query --lockfile_mode=error @mino_llvm//:all
bazel build --lockfile_mode=error --config=hermetic --config=debug //...
bazel test --lockfile_mode=error --config=hermetic --config=debug //... \
  --test_output=errors
```

CI 的 `Hermetic LLVM 18.1.8 (ubuntu-22.04)` job 覆盖 Linux x86_64 全量 build/test，
并显式构建 `//tools/ci:hermetic_clang_version`。该 genrule 执行下载 archive 内的真实
`clang --version`，结果作为 `hermetic-clang-version-*` artifact 保留 90 天，而不是只
相信 MODULE 中的版本字符串。

所有主 CI 和物理二进制 build 使用 `--lockfile_mode=error`，构建后执行：

```sh
git --no-pager diff --exit-code -- MODULE.bazel MODULE.bazel.lock .bazelrc
```

由于本地 Java TLS 无法稳定访问 `bcr.bazel.build`，registry 使用 BCR git mirror；
`MODULE.bazel.lock` 对每个 registry 文件记录 checksum，严格 lock 模式禁止 moving mirror
内容更新依赖图。`--config=hermetic` 固定编译器/LLVM C++ runtime；Linux glibc 与 macOS
SDK 仍来自执行主机，因此不宣称固定完整操作系统 sysroot。

## 真实双物理主机手工验证

真实测试程序是 `//tools/ci:mino_two_host_probe`。同一 C++ 二进制支持 server/client：

1. `TcpDriver` 建立真实跨主机 TCP；
2. `BridgeConnectionManager` 自动重试连接，并通过 `SessionDiscovery` 对 Node、Process、
   lease 和 config identity 做 fencing；
3. client 通过 `BridgePipeline` 发送 `ReliableOrdered` 请求；
4. server 的 `BridgeIngressPort` 验证 commit、run proof、machine identity 和地址后提交消息，
   ACK 由 pipeline 返回；
5. server 再发送 `ReliableOrdered` 响应，client 提交并 ACK；
6. 两端只有在各自 retransmit window 清零后才写出 `remote_acknowledged: true` 的 JSON。

Python runner **不创建数据 socket**，只负责 secret 派生、启动/终止 C++ 子进程、读取 Mino
JSON、哈希日志/二进制并生成 manifest。`MINO_TWO_HOST_TOKEN` 只用于本地派生 HMAC run
proof 和不可逆 machine-id；网络上传输的是 proof，不是 token。

### Workflow 拓扑

`.github/workflows/physical-two-host.yml` 只有 `workflow_dispatch`：

- 两个独立 GitHub-hosted build job 从同一 `GITHUB_SHA` 使用 hermetic release 配置构建
  `mino_two_host_probe`，分别上传 commit、SHA-256 和二进制；
- server/client job 都 `needs` 两个 build job，因而在两份 artifact 都完成后才同时进入
  self-hosted 队列；每台物理机下载两份 artifact，验证 SHA、checksum 和 `cmp` 完全一致；
- server：`[self-hosted, linux, x64, mino-node-a]`；
- client：`[self-hosted, linux, x64, mino-node-b]`；
- evidence：`ubuntu-22.04` 只汇总 artifact，明确 **not a physical node**。

角色 deadline 输入范围为 1800–3600 秒，默认 1800 秒。client 的
`BridgeConnectionManager` 在总 deadline 内按有界指数 backoff 重试；workflow job 上限为
75 分钟。普通 PR 只运行 fake Mino binary self-test，不调度物理节点。

### Runner 与网络准备

1. 将 `mino-node-a`、`mino-node-b` labels 分配给两个不同物理主机。C++ Mino payload 和
   最终 manifest 都验证两端 HMAC machine identity 不同；管理员仍负责确保 labels 对应
   真实不同物理设备，而非两个伪装容器。
2. 创建 GitHub Environment `physical-two-host`，建议启用 required reviewers，并设置至少
   32 bytes 随机 secret `MINO_TWO_HOST_TOKEN`。
3. 仅允许 node-b 到 node-a 所选 TCP 端口的入站流量，优先使用隔离 LAN/VPN。填写
   `server_address`、`client_address`、`port` 和不少于 1800 的 `timeout_seconds`；生产
   runner 拒绝 loopback、unspecified 和 multicast advertised address。

### 脱离 GitHub 分别运行

先在可信构建机生成同一二进制并核对 lock：

```sh
bazel build --lockfile_mode=error --config=hermetic --config=release \
  //tools/ci:mino_two_host_probe
git --no-pager diff --exit-code -- MODULE.bazel MODULE.bazel.lock .bazelrc
sha256sum bazel-bin/tools/ci/mino_two_host_probe
```

将同一二进制安全复制到两台主机，并从 secret manager 向两端注入相同 token。node-a：

```sh
python3 tools/ci/run_two_host_server.py \
  --binary=/opt/mino/mino_two_host_probe \
  --bind-address=0.0.0.0 --advertise-address=10.0.0.10 \
  --port=43191 --timeout-seconds=1800 \
  --manifest=/tmp/mino-two-host/server/manifest.json \
  --log=/tmp/mino-two-host/server/server.log
```

node-b：

```sh
python3 tools/ci/run_two_host_client.py \
  --binary=/opt/mino/mino_two_host_probe \
  --server-address=10.0.0.10 --advertise-address=10.0.0.11 \
  --port=43191 --timeout-seconds=1800 \
  --manifest=/tmp/mino-two-host/client/manifest.json \
  --log=/tmp/mino-two-host/client/client.log
```

汇总：

```sh
python3 tools/ci/finalize_two_host_manifest.py \
  --server-manifest=/tmp/mino-two-host/server/manifest.json \
  --client-manifest=/tmp/mino-two-host/client/manifest.json \
  --expected-commit="$(git rev-parse HEAD)" \
  --out=/tmp/mino-two-host/manifest.json
```

最终 manifest 交叉验证两端 commit、machine identity、advertised address、执行二进制
SHA-256、SessionDiscovery、Bridge active、双向 reliable send/receive、远端 ACK、role
manifest hash 和日志 hash。任一项不一致均失败。脚本 `--self-test` 使用 fake Mino binary
验证进程 deadline、清理、JSON 信任边界和 manifest，不用 Python socket 冒充 Mino。
