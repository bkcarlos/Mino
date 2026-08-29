# Mino 拉取 / 多 Docker / 测试 / 对比测试 报告

生成时间：2026-08-25 23:20 CST（UTC+8）  
主机：Debian 13 (trixie)，8 vCPU，15.64 GiB RAM，x86_64  
执行用户：`box`（uid 1000）

---

## 1. 代码位置

| 项 | 值 |
|---|---|
| 路径 | `/workspace/Mino` |
| 远程 | https://github.com/bkcarlos/Mino |
| 分支 | `master`（跟踪 `origin/master`） |
| Commit | `c977bd18ab67b17aa98406674ba482817e812beb` |
| 说明 | `perf: complete pipeline optimization qualification` |
| 工作树 | 干净，未提交、未推送 |

仓库按任务要求使用已有 shallow clone，未重新 clone。

---

## 2. Docker 安装状态

Docker Engine 已安装并可用（嵌套容器环境）。

| 项 | 值 |
|---|---|
| 包 | Debian `docker.io` 26.1.5+dfsg1（API 1.45） |
| containerd | 1.7.24~ds1 |
| runc | 1.1.15+ds1 |
| 存储驱动 | **vfs**（见下方 blocker） |
| 守护进程 | `dockerd --host=unix:///var/run/docker.sock`，`/etc/docker/daemon.json` 为 `{"storage-driver":"vfs"}` |
| 用户组 | `box` 已加入 `docker` 组；当前会话用 `sg docker` 执行 |
| bazelisk | v1.29.0，sha256 `5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992`，安装为 `/usr/local/bin/bazel`（解析到 Bazel **7.4.1**） |

### 2.1 overlay2 无法嵌套

首次 `storage-driver=overlay2` 失败：

```
failed to mount overlay: invalid argument
failed to start daemon: error initializing graphdriver: driver not supported: overlay2
```

宿主机根文件系统本身就是 overlayfs，不能再叠 overlay2。

优先安装 `fuse-overlayfs` 失败：Debian 镜像对 `libfuse3-4_3.17.2-3_amd64.deb` 返回 **502 Bad Gateway**。按指引回退到 **vfs**，`sudo docker info` 成功。

### 2.2 官方两套隔离镜像

由 `tools/ci/run_schema_codegen_docker_check.py` 构建：

| 仓库:标签 | Image ID | 大小 | 创建（UTC） |
|---|---|---:|---|
| `mino-schema-codegen:ubuntu22-gcc12` | `d8a92b3ce3b9` | **991MB** | 2026-08-25 14:59:46 |
| `mino-schema-codegen:ubuntu24-clang18` | `8ccb0e53fd19` | **1.16GB** | 2026-08-25 15:00:56 |

- Ubuntu 22.04 + GCC 12，1 bazel job，工作区 `/workspace-gcc`，TZ=UTC，SOURCE_DATE_EPOCH=0
- Ubuntu 24.04 + Clang 18，4 bazel jobs，工作区 `/nested/workspace-clang`，TZ=Pacific/Auckland，SOURCE_DATE_EPOCH=2147483647

镜像构建阶段 `bazel fetch` 带网络；对比容器 `docker run --network=none`。

---

## 3. 两环境 Schema CodeGen 对比

**结果：PASS（清单与根哈希完全一致）**

| 项 | 值 |
|---|---|
| 输出文件数 | 15 |
| 根 SHA-256 | `ae585a9c8f93ee0403bc28130fdd39c653e6950a4bbd9a0e6ac405cc7e4e3edc` |
| SHA256SUMS | ubuntu22-gcc12 与 ubuntu24-clang18 **逐字节相同** |
| 证据目录 | `/workspace/mino-results/codegen-docker` |

两边容器内测试均通过：

- `//mino/schema/codegen:code_generator_test`
- `//tools/minoc:canonical_wire_generated_test`
- `//tools/minoc:cross_directory_generated_test`
- `//tools/minoc:minoc_cli_test`

以及 5 个 codegen 产物收集（sample / canonical_wire / mangling / sensor_frame / codegen_golden）。

清单（两边相同）：

```
19ef6cc9debebe303a3cc336997be141d47ef0b4eab68fd5758010d1d52562d5  mino/schema/fuzz/generated/codegen_golden.generated.cc
3386960a10125c06ad1d10f6b08edf2ec91a98cc8fb387f71ce111c58b4b97dc  mino/schema/fuzz/generated/codegen_golden.generated.h
4dccb4155a6cf4b58ee41d04105f2aa96def59f4fea2fc95df78032bf9125968  mino/schema/fuzz/testdata/codegen_golden.descriptor
251090b8c84809ff9804ecbd85c82c0fa3240502016360c946f81a662f2a675a  tools/minoc/tests/generated/canonical_wire.descriptor
a525c6cd9184ef31b52aa5c3f598a46cc53251332c4255edd86d2aaaac0283d8  tools/minoc/tests/generated/canonical_wire.generated.cc
dc8c33d42c42df85632df198a3abd750bdc7d949d1e1e48fcccf4d2f796c6010  tools/minoc/tests/generated/canonical_wire.generated.h
2de87eccbf232305b305d93981a876e6d6988046775a94515089d2f1c1d1a79c  tools/minoc/tests/generated/mangling.descriptor
19e8348714d60bc915f46061e456e81e863642ed76d95933c26136039d178a22  tools/minoc/tests/generated/mangling.generated.cc
2577757249bece475621e8d39614c61bdc76d5705d843ce33eebe39b0bfa107f  tools/minoc/tests/generated/mangling.generated.h
5374bf5436f87d86065008a03aa67301f8e7bde747169da2c43b8815a47e25b7  tools/minoc/tests/generated/sample.descriptor
545cf744888ee5fe77a6beaf9b44123686ad99a8b151a4d2869207906426c77b  tools/minoc/tests/generated/sample.generated.cc
b3f7d4a607b90427168266a7b77b56cf4716ea2f64f04cfa990dba07c8a2cf83  tools/minoc/tests/generated/sample.generated.h
380dfa68ed6fbd75050fa0fb2163e03e101628155796b6ea768d281d19846e32  tools/minoc/tests/generated/sensor_frame.descriptor
168f894777d62152d8574046d02c6491470a9c4dc9cd509d2efc25401549bcb2  tools/minoc/tests/generated/sensor_frame.generated.cc
370f95d1d349a7c63ecd063ad32684a34fe37bde64ceac135d34abaf7850a842  tools/minoc/tests/generated/sensor_frame.generated.h
```

证据：

- `/workspace/mino-results/codegen-docker/ubuntu22-gcc12/hermetic-codegen/{ROOT_SHA256,SHA256SUMS,PROVENANCE.txt,artifacts/}`
- `/workspace/mino-results/codegen-docker/ubuntu24-clang18/hermetic-codegen/{ROOT_SHA256,SHA256SUMS,PROVENANCE.txt,artifacts/}`
- 完整日志：`/workspace/mino-results/codegen-docker.log`

---

## 4. 宿主机单元 / sanity 测试

Debian 13 的 g++-14 **可以直接**跑通聚焦测试，未因缺 gcc-12 跳过。

```
bazel test --config=release --jobs=2 //tests:sanity_test //tests/litmus:atomic_abi_test --test_output=errors
```

| 目标 | 结果 |
|---|---|
| `//tests:sanity_test` | PASSED（0.0s） |
| `//tests/litmus:atomic_abi_test` | PASSED（0.1s） |

日志：`/workspace/mino-results/host-tests.log`  
未跑全量 `//...`（官方 CI 为 5 套配置，耗时可数小时）。Codegen 相关测试已在两个 Docker 环境中跑过。

---

## 5. Pipeline 对比测试

拓扑：Perception → Prediction → Planning → Control → Guardian → CANBus。  
同机矩阵后端：Fast DDS、Cyclone DDS、Protobuf+ZeroMQ、Mino SHM。  
测量：同机 `CLOCK_MONOTONIC_RAW` 单向端到端延迟；**饱和模式**（`--publish-interval-us=0`），百分位含排队，不能当成空载传输延迟。

### 5.1 构建

```
bazel build --config=pipeline_comparison -c opt --jobs=4 \
  //benchmarks/pipeline_comparison:fastdds_pipeline \
  //benchmarks/pipeline_comparison:protobuf_zmq_pipeline \
  //benchmarks/pipeline_comparison:mino_shm_pipeline \
  //benchmarks/pipeline_comparison:mino_shm_tcp_bridge \
  //benchmarks/pipeline_comparison:mino_tcp_pipeline \
  //benchmarks/pipeline_comparison:cyclonedds_pipeline \
  //benchmarks/pipeline_comparison:pipeline_comparison_runner \
  //benchmarks/pipeline_comparison:pipeline_network_runner \
  //benchmarks/pipeline_comparison:mino_hybrid_runner
```

**成功**（3649 actions，637.3s，Critical Path 89.22s）。BCR GitHub raw 镜像可用，未改 `--registry`。  
日志：`/workspace/mino-results/pipeline-build.log`

### 5.2 Smoke（1 round，小样本量）

配置：small=20 / medium=10 / large=2，warmup-ratio=0.1，deadline=60s。

**总 outcome：failed**（12 跑中 11 通过）。唯一失败：

- `large/round-1/mino_shm_pipeline`：`control` 退出码 **-7（SIGBUS）**
- 当时 `/dev/shm` 仅 **64 MiB**；large 为 1 MiB payload × SPSC capacity 64，映射不够

其余后端 small/medium/large 及 Mino SHM 的 small/medium 均通过，丢失/损坏为 0。  
随后执行 `sudo mount -o remount,size=8G /dev/shm`，再跑正式战役。  
产物：`/workspace/mino-results/pipeline-smoke/`

### 5.3 同机正式战役（2 rounds）— **outcome: passed**

配置：

- rounds=2，profiles=small,medium,large
- small=2000 条（256 B），medium=400 条（64 KiB），large=40 条（1 MiB）
- deadline=120s，channel/history/HWM=64
- 24/24 跑通过；lost=0，corrupt=0，duplicate=0，out_of_order=0；received=offered

下表延迟单位为 **微秒（µs）**，msgs/s 为 CANBus 吞吐。两 round 都列出；「均值」为两 round 算术平均。

#### small（256 B，2000 条）

| 后端 | r1 p50 | r1 p95 | r1 p99 | r1 msgs/s | r2 p50 | r2 p95 | r2 p99 | r2 msgs/s | 丢失/损坏 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **mino_shm_pipeline** | 1671 | 2301 | 2344 | 84892 | 1958 | 2183 | 2193 | 88350 | 0/0 |
| fastdds_pipeline | 3053 | 3647 | 3854 | 55901 | 2484 | 4755 | 4836 | 50133 | 0/0 |
| protobuf_zmq_pipeline | 10341 | 12373 | 13231 | 33161 | 10771 | 15445 | 16211 | 34103 | 0/0 |
| cyclonedds_pipeline | 50971 | 81927 | 83104 | 6404 | 30465 | 51764 | 52266 | 7065 | 0/0 |

**small 胜出：Mino SHM**（均值 p50 **1.815 ms**，均值 **86621 msgs/s**）

#### medium（64 KiB，400 条）

| 后端 | r1 p50 | r1 p95 | r1 p99 | r1 msgs/s | r2 p50 | r2 p95 | r2 p99 | r2 msgs/s | 丢失/损坏 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **mino_shm_pipeline** | 917 | 1032 | 1261 | 3298 | 1041 | 1327 | 2886 | 2552 | 0/0 |
| fastdds_pipeline | 1380 | 2496 | 2637 | 3561 | 1514 | 3075 | 4047 | 3582 | 0/0 |
| cyclonedds_pipeline | 1515 | 2376 | 2931 | 2623 | 1549 | 2244 | 3703 | 2790 | 0/0 |
| protobuf_zmq_pipeline | 2427 | 6455 | 6615 | 3085 | 1519 | 1979 | 3489 | 2337 | 0/0 |

**medium 延迟胜出：Mino SHM**（均值 p50 **0.979 ms**）  
**medium 吞吐胜出：Fast DDS**（均值 **3571 msgs/s**，略高于 Mino SHM 的 2925）

说明：饱和模式下 small 的 p50 反而高于 medium（队列堆积 + 2000 vs 400 条），这是排队行为，不是空载 hop 延迟。

#### large（1 MiB，40 条）

| 后端 | r1 p50 | r1 p95 | r1 p99 | r1 msgs/s | r2 p50 | r2 p95 | r2 p99 | r2 msgs/s | 丢失/损坏 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **mino_shm_pipeline** | 14074 | 15975 | 16881 | 193.1 | 13362 | 14704 | 14954 | 209.0 | 0/0 |
| fastdds_pipeline | 22547 | 24968 | 26515 | 164.7 | 23030 | 26876 | 28627 | 159.2 | 0/0 |
| protobuf_zmq_pipeline | 27071 | 30329 | 31183 | 157.6 | 26630 | 31004 | 32954 | 161.6 | 0/0 |
| cyclonedds_pipeline | 386924 | 507067 | 522108 | 59.0 | 130850 | 327160 | 332916 | 54.7 | 0/0 |

**large 胜出：Mino SHM**（均值 p50 **13.72 ms**，均值 **201 msgs/s**）

Cyclone DDS 在 small/large 上明显更慢（默认 UDP、无 PSMX）；small 上 Fast DDS 因 intrahost SHM+data sharing 远快于 Cyclone，二者传输不可直接等同。

产物：`/workspace/mino-results/pipeline-comparison/manifest.json` 与 `runs/`。

### 5.4 单机六进程网络 smoke

拓扑：`/workspace/mino-results/one-host.json`（六角色全部 `ssh_host=local`，`data_address=127.0.0.1`，`workdir=/workspace/Mino`）。  
profile=small，messages=50，warmup=5。`one_way_latency_valid=true`（同一 boot_id）。

| 后端 | outcome | p50 | p95 | p99 | msgs/s | 样本 | lost/corrupt |
|---|---|---:|---:|---:|---:|---:|---:|
| **mino_tcp** | passed | 855 µs | 939 µs | 949 µs | 46142 | 50 | 0/0 |
| protobuf_zmq | passed | 10.33 ms | 10.76 ms | 10.81 ms | 4552 | 50 | 0/0 |

未跑 Fast DDS / Cyclone DDS 网络矩阵（任务允许只做 mino_tcp，并在有时间时加 protobuf_zmq）。未跑多机 hybrid。

---

## 6. 失败 / 跳过项

| 项 | 状态 | 原因 |
|---|---|---|
| overlay2 Docker | 失败后改 vfs | 嵌套 overlayfs 不支持 overlay2 |
| fuse-overlayfs | 未装上 | `libfuse3-4` 502 |
| Codegen Docker 对比 | **PASS** | — |
| 宿主机 sanity | **PASS** | 未跑全量 `//...` |
| Smoke large Mino SHM | SIGBUS | `/dev/shm` 64 MiB；战役前已扩到 8 GiB |
| 同机战役 24 runs | **PASS** | — |
| 网络 mino_tcp / protobuf_zmq smoke | **PASS** | 未跑 fastdds/cyclonedds 网络、未跑 hybrid/双机 |
|  paced latency 战役 | 未跑 | 任务指定饱和战役 |
| git push / commit | 未做 | 按约束保持干净树 |

---

## 7. 实际使用的命令

```sh
# Docker
sudo apt-get update && sudo apt-get install -y docker.io python3 curl ca-certificates git unzip zip gcc g++ libatomic1
# overlay2 失败后：
echo '{"storage-driver":"vfs"}' | sudo tee /etc/docker/daemon.json
sudo sh -c 'nohup dockerd --host=unix:///var/run/docker.sock >/var/log/dockerd.log 2>&1 &'
sudo usermod -aG docker box

# bazelisk v1.29.0 → /usr/local/bin/bazel

# 两套官方镜像 + codegen 对比
sg docker -c 'cd /workspace/Mino && python3 tools/ci/run_schema_codegen_docker_check.py --out=/workspace/mino-results/codegen-docker'

# 宿主机聚焦测试
cd /workspace/Mino
bazel test --config=release --jobs=2 //tests:sanity_test //tests/litmus:atomic_abi_test --test_output=errors

# pipeline 二进制
bazel build --config=pipeline_comparison -c opt --jobs=4 \
  //benchmarks/pipeline_comparison:fastdds_pipeline \
  //benchmarks/pipeline_comparison:protobuf_zmq_pipeline \
  //benchmarks/pipeline_comparison:mino_shm_pipeline \
  //benchmarks/pipeline_comparison:mino_shm_tcp_bridge \
  //benchmarks/pipeline_comparison:mino_tcp_pipeline \
  //benchmarks/pipeline_comparison:cyclonedds_pipeline \
  //benchmarks/pipeline_comparison:pipeline_comparison_runner \
  //benchmarks/pipeline_comparison:pipeline_network_runner \
  //benchmarks/pipeline_comparison:mino_hybrid_runner

sudo mount -o remount,size=8G /dev/shm

# smoke
./bazel-bin/benchmarks/pipeline_comparison/pipeline_comparison_runner \
  --output-dir=/workspace/mino-results/pipeline-smoke \
  --rounds=1 --profiles=small,medium,large \
  --small-messages=20 --medium-messages=10 --large-messages=2 \
  --warmup-ratio=0.1 --deadline-seconds=60

# 正式同机战役
./bazel-bin/benchmarks/pipeline_comparison/pipeline_comparison_runner \
  --output-dir=/workspace/mino-results/pipeline-comparison \
  --rounds=2 --profiles=small,medium,large \
  --small-messages=2000 --medium-messages=400 --large-messages=40 \
  --deadline-seconds=120

# 单机六进程网络
./bazel-bin/benchmarks/pipeline_comparison/pipeline_network_runner \
  --topology=/workspace/mino-results/one-host.json \
  --output-dir=/workspace/mino-results/pipeline-network-mino_tcp \
  --backend=mino_tcp --profile=small --messages=50 --warmup-messages=5 --deadline-seconds=120

./bazel-bin/benchmarks/pipeline_comparison/pipeline_network_runner \
  --topology=/workspace/mino-results/one-host.json \
  --output-dir=/workspace/mino-results/pipeline-network-protobuf_zmq \
  --backend=protobuf_zmq --profile=small --messages=50 --warmup-messages=5 --deadline-seconds=120
```

---

## 8. 产物路径

| 路径 | 内容 |
|---|---|
| `/workspace/mino-results/REPORT.md` | 本报告 |
| `/workspace/mino-results/summary.json` | 结构化数字 |
| `/workspace/mino-results/codegen-docker/` | 两环境 SHA256 清单、产物、provenance |
| `/workspace/mino-results/codegen-docker.log` | Docker 构建与对比日志 |
| `/workspace/mino-results/docker-images.txt` | `docker images` |
| `/workspace/mino-results/host-tests.log` | 宿主机 bazel test |
| `/workspace/mino-results/pipeline-build.log` | pipeline 二进制构建 |
| `/workspace/mino-results/pipeline-smoke/` | smoke manifest + 12 runs |
| `/workspace/mino-results/pipeline-comparison/` | 正式战役 manifest + 24 runs |
| `/workspace/mino-results/pipeline-network-mino_tcp/` | 单机 mino_tcp 网络 smoke |
| `/workspace/mino-results/pipeline-network-protobuf_zmq/` | 单机 protobuf_zmq 网络 smoke |
| `/workspace/mino-results/one-host.json` | 六进程本机拓扑 |

仓库本身未改动。
