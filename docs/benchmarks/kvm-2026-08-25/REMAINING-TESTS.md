# 单机剩余测试报告

生成时间：2026-08-26 00:00:03 CST（UTC+8）  
主机：Debian 13，8 vCPU，15 GiB RAM，x86_64，`/dev/shm` 8G  
仓库：`/workspace/Mino` @ `c977bd18`（`perf: complete pipeline optimization qualification`）  
工具链：Bazel 7.4.1，宿主机 g++-14.2.0（**未**使用 `--config=gcc12`）  
`--lockfile_mode=error`：全程保留，未与环境冲突。

---

## 总览

| 项 | 结果 |
|---|---|
| 1. release `bazel test //...` | **140 PASS / 1 FAIL TO BUILD / 0 SKIP（补跑后）** |
| 2. 网络 one-host `fastdds` | **PASS** |
| 2. 网络 one-host `cyclonedds` | **PASS** |
| 3. 同机 hybrid / all-SHM smoke | **PASS**（5/5 边均为 `shm`，单一 boot ID） |
| 4. paced-latency smoke | **11/12 PASS**，1 FAIL（medium / protobuf_zmq） |
| 5. ASAN 子集 | **29/29 PASS**（`--jobs=2` 时 2 个 SHM 名冲突 FAIL，串行重跑后通过） |
| 6. TLA+ | **SKIP**（无 Java / TLC） |
| TSAN 全量 | **SKIP**（按任务过重） |
| 全量 `asan //...` | **SKIP**（15G 机上有 OOM 风险） |

---

## 1. Full release unit/integration suite

命令：

```sh
cd /workspace/Mino
bazel test --lockfile_mode=error --config=release --jobs=4 //... --test_output=errors
```

日志：`/workspace/mino-results/full-release-test.log`  
开始：2026-08-25 23:45:23 CST  
结束：2026-08-25 23:49:45 CST（约 261 s）

首次结果（Bazel 默认非 `--keep_going`，编译失败后其余目标未建）：

```
Executed 112 out of 141 tests: 114 tests pass, 1 fails to build and 26 were skipped.
```

- **114 PASS**（含 2 个 cached：`//tests:sanity_test`、`//tests/litmus:atomic_abi_test`）
- **1 FAIL TO BUILD**：`//mino/transport:transport_driver_test`
- **26 NO STATUS / skipped**：因编译失败中断，并非硬件缺失

### 唯一编译失败

目标：`//mino/transport:transport_driver_test`  
错误（GCC 14 + `-O2` + `-Werror=array-bounds=`，libstdc++ `vector<byte>` 拷贝内联）：

```
mino/transport/transport_driver_test.cc:600:56:
/usr/include/c++/14/bits/stl_algobase.h:452:30: error: 'void* __builtin_memmove(void*, const void*, long unsigned int)' forming offset 1 is out of the bounds [0, 1] [-Werror=array-bounds=]
```

触发用例：`TransportDriverTest_OwnedFallbackConsumesOnlySuccessfulAdmissions_Test`（同样错误出现在 test.cc:600 / 614 / 628）。  
这是 GCC 14 对 `std::vector<std::byte>` 拷贝的误报，不是运行时失败。

### 补跑（一次）

对失败目标 + 26 个未执行目标加 `--keep_going`：

日志：`/workspace/mino-results/full-release-test-rerun.log`  
结束：2026-08-25 23:53:38 CST

```
Executed 26 out of 27 tests: 26 tests pass and 1 fails to build.
```

26 个原先 NO STATUS 的目标全部 **PASS**（含 `//mino/platform:rdma_provider_test`、`//mino/transport:rdma_driver_test`、`//mino/platform:fabric_provider_test`、`//mino/transport:fabric_driver_test` 的**软件/单测路径**；它们不需要真实 NIC）。  
`transport_driver_test` 再次 FAIL TO BUILD，相同错误，停止循环。

**合并计数（141 个 test target）：140 PASS，1 FAIL TO BUILD，0 SKIP。**

---

## 2. Pipeline network one-host smoke（fastdds / cyclonedds）

拓扑：`/workspace/mino-results/one-host.json`（6 角色全部 local / 127.0.0.1）  
已构建 runner：`bazel-bin/benchmarks/pipeline_comparison/pipeline_network_runner`  
profile=small，messages=50，warmup=5，deadline=120

| 后端 | 结果 | p50_ns | p95_ns | p99_ns | samples | msgs/s | 日志 / 产物 |
|---|---|---:|---:|---:|---:|---:|---|
| fastdds | **PASS** | 1,455,789 | 1,903,685 | 1,968,440 | 50 | 23004.35 | `/workspace/mino-results/pipeline-network-fastdds` |
| cyclonedds | **PASS** | 971,225 | 1,017,078 | 1,019,792 | 50 | 35424.99 | `/workspace/mino-results/pipeline-network-cyclonedds` |

两者 `clock_mode=same-host`，`one_way_latency_valid=true`，`errors=[]`。  
先前已跑过的 `mino_tcp` / `protobuf_zmq` 未重跑。

---

## 3. 同机 Mino hybrid / all-SHM smoke

命令按任务原文执行。产物：`/workspace/mino-results/pipeline-hybrid-onehost`

- outcome：**passed**，errors=[]
- 6 个角色 boot ID 全部为 `94d19f59-bf49-46e3-aa23-edfee7d49371`（同一 boot）
- 5 条边 transport 全部为 **`shm`**（README 认可的 one-boot hybrid/all-SHM smoke）
- p50=290,985 ns，p95=806,934 ns，p99=821,866 ns，samples=50，msgs/s=35332.69
- 未因 same-boot 被拒绝

---

## 4. Paced-latency SMOKE（非正式战役）

产物：`/workspace/mino-results/pipeline-paced-smoke`  
`load_mode` 三档均为 `paced_latency`：small 100 Hz / medium 20 Hz / large 10 Hz；rounds=1。

**12 次运行中 11 PASS，1 FAIL。**

| backend | profile | outcome | interval_us | msgs | samples | p50_ns | p95_ns | msgs/s |
|---|---|---|---:|---:|---:|---:|---:|---:|
| fastdds_pipeline | small | passed | 10000 | 100 | 100 | 860344 | 1474553 | 100.94 |
| cyclonedds_pipeline | small | passed | 10000 | 100 | 100 | 625046 | 1201245 | 100.89 |
| protobuf_zmq_pipeline | small | passed | 10000 | 100 | 100 | 928064 | 21841449 | 100.90 |
| mino_shm_pipeline | small | passed | 10000 | 100 | 100 | 69609 | 84340 | 101.00 |
| mino_shm_pipeline | medium | passed | 50000 | 40 | 40 | 1039310 | 1262565 | 20.50 |
| fastdds_pipeline | medium | passed | 50000 | 40 | 40 | 2800348 | 10184609 | 20.41 |
| cyclonedds_pipeline | medium | passed | 50000 | 40 | 40 | 2221765 | 2731567 | 20.50 |
| protobuf_zmq_pipeline | medium | **failed** | 50000 | 40 | 0 | — | — | 0 |
| mino_shm_pipeline | large | passed | 100000 | 10 | 10 | 14430131 | 18180495 | 10.93 |
| cyclonedds_pipeline | large | passed | 100000 | 10 | 10 | 49794799 | 249989972 | 9.51 |
| protobuf_zmq_pipeline | large | passed | 100000 | 10 | 10 | 27346606 | 35524061 | 11.66 |
| fastdds_pipeline | large | passed | 100000 | 10 | 10 | 26691153 | 35702461 | 10.73 |

**失败原文（perception.stderr.log）：**

```
protobuf-zmq pipeline failed: paced source fell more than one interval behind schedule
```

worker `perception.json`：`"error": "paced source fell more than one interval behind schedule"`，`offered=40`，`received=0`，`lost=40`。这是 runner 对 paced 落后超过一个 interval 的硬失败，不是静默追赶。medium 64KiB + 20 Hz + protobuf 编码在本机 8 vCPU 上偶发跟不上。small/large 的 protobuf_zmq 均通过。

---

## 5. Bounded ASAN 子集

可用内存约 9.1 GiB。release 并非完全全绿（1 个编译失败），但仍按「能跑的就跑」执行有界子集，**未**跑 `asan //...`。

```sh
bazel test --lockfile_mode=error --config=asan --jobs=2 \
  //tests:sanity_test //tests/litmus:atomic_abi_test //mino/shm/... --test_output=errors
```

日志：`/workspace/mino-results/asan-subset.log`  
首次：`Executed 29 out of 29 tests: 27 tests pass and 2 fail locally.`

失败：

- `//mino/shm/region:handle_resolver_test` — `handle_resolver_test.cc:88`：`3: shared-memory object already exists`
- `//mino/shm/region:recovery_test` — `recovery_test.cc:42`：`3: shared-memory object already exists`

两测试用 `getpid()` 生成 `/hr_<pid>_<seq>` / `/ro_<pid>_<seq>`。linux-sandbox PID namespace 下 pid 碰撞 + `--jobs=2` 并行会抢同一 POSIX SHM 名。

串行补跑一次（`--jobs=1`），日志 `/workspace/mino-results/asan-subset-rerun.log`：

```
Executed 2 out of 2 tests: 2 tests pass.
```

**ASAN 子集合并：29 PASS / 0 FAIL。** 无 AddressSanitizer 报告。未跑 TSAN。

---

## 6. TLA+

`python3 tools/ci/run_tla_validation.py` 需要 `--jar`（tla2tools.jar）和 `java`。  
本机：`java: command not found`，无 TLC / OpenJDK。**SKIP。**

---

## 因无硬件 / 无第二台机器而跳过

按任务明确禁止，未尝试：

| 项 | 原因 |
|---|---|
| 物理双机 / SSH `192.168.31.x` | 无第二台机器 |
| 72 小时 soak | 时长禁止 |
| 真实 RDMA NIC 资格测试 | 无硬件（Bazel 里 `rdma_*` **单元测试**已在 release 套件 PASS） |
| 真实 Fabric NIC 资格测试 | 无硬件（`fabric_*` 单元测试已 PASS） |
| native AArch64 | 本机 x86_64；`//tests/aarch64:abi_atomic_smoke_test` 与 `runner_contract_test` 是契约/烟测，已在 release PASS |
| PTP 跨机单向时延 | 无 PTP、无第二主机 |
| `--config=gcc12` / gcc-12 | 未安装；全程 g++-14 |
| TSAN 全量 | 按任务过重 |
| 全量 `asan //...` | 15G RAM 上 OOM 风险 |
| TLA+ / TLC | 无 Java |

---

## 产物路径

| 路径 | 内容 |
|---|---|
| `/workspace/mino-results/REMAINING-TESTS.md` | 本报告 |
| `/workspace/mino-results/remaining-tests-summary.json` | 结构化计数 |
| `/workspace/mino-results/full-release-test.log` | release `//...` 首次 |
| `/workspace/mino-results/full-release-test-rerun.log` | 失败/未跑目标补跑 |
| `/workspace/mino-results/pipeline-network-fastdds/` | fastdds 网络 smoke + manifest |
| `/workspace/mino-results/pipeline-network-cyclonedds/` | cyclonedds 网络 smoke + manifest |
| `/workspace/mino-results/pipeline-hybrid-onehost/` | hybrid all-SHM smoke + manifest |
| `/workspace/mino-results/pipeline-paced-smoke/` | paced smoke + manifest |
| `/workspace/mino-results/asan-subset.log` | ASAN 子集首次 |
| `/workspace/mino-results/asan-subset-rerun.log` | 两个 SHM 测试串行重跑 |
