# Linux candidate validation results — 2026-08-24

## Status

- Candidate Linux functional gate: **PASSED**.
- Core C++ tests: **199/199 passed** across 9 targets.
- Runner Python tests: **26/26 passed** with the host Python 3.12 runtime.
- Selected ASAN tests: **134/134 passed** after pinning the allocator test to
  NUMA node 0; no sanitizer memory error was reported in either invocation.
- Mino TCP `-c opt` six-process smoke and all three saturation profiles:
  **PASSED** with zero loss, duplicate, out-of-order, or corruption counts.
- One-boot Mino hybrid/all-SHM smoke: **PASSED** with zero message-integrity
  failures.
- Source base commit: `da29b776226dc2132069a0c23e767dcc1e413888`.
- Source state was dirty because the complete candidate optimization set had not
  yet been committed: 44 tracked files differed from the base commit.

This is a candidate-only validation on one small KVM guest. It is not a clean-ref
baseline/candidate A/B campaign, a multi-round latency qualification, a `perf`
attribution run, or a publication-grade performance comparison.

## Host and toolchain

| Property | Value |
|---|---|
| Host | `iv-yetht30b9ch2cbetfc8g` (`101.96.243.194`) |
| OS | Ubuntu 24.04, Linux `6.8.0-55-generic` |
| Architecture | x86-64 |
| CPU | Intel Xeon Platinum 8336C at 2.30 GHz, KVM guest |
| Topology | 4 vCPUs, 2 sockets, 2 cores/socket, 1 thread/core |
| NUMA | node 0 CPUs `0-1`; node 1 CPUs `2-3` |
| Memory | 3.8 GiB |
| Compiler | GCC/G++ 13.3.0 |
| Bazel | 7.4.1, official binary verified against release SHA-256 |
| Python | system Python 3.12.3 for source runner tests and execution |
| Checkout | `/root/Mino-validation` |
| Results | `/root/Mino-validation-results` |

The performance runs were not CPU-pinned, did not isolate IRQs or background
work, and had no recorded CPU-frequency governor. The same-host boot ID and
monotonic clock contract were satisfied, so the recorded one-way latency is
valid for these processes, but the VM is not a controlled latency host.

## Functional and sanitizer validation

The Linux C++ gate used `--nocache_test_results` and two build jobs. Every test in
these targets passed:

| Target | Tests |
|---|---:|
| `//mino/transport:transport_driver_test` | 14 |
| `//mino/transport:tcp_driver_test` | 36 |
| `//mino/bridge:crc32c_test` | 5 |
| `//mino/bridge:wire_frame_test` | 27 |
| `//mino/schema:wire_test` | 13 |
| `//mino/schema/codegen:code_generator_test` | 13 |
| `//mino/runtime:allocation_journal_test` | 20 |
| `//mino/runtime:publisher_subscriber_test` | 33 |
| `//mino/shm/allocator:central_slab_test` | 38 |
| **Total** | **199** |

Direct `unittest` execution of `mino_hybrid_runner_test` and
`pipeline_network_runner_test` passed **26/26** tests. Bazel's hermetic
`rules_python` repository intentionally rejects execution as `root`; using the
installed Python runtime avoided weakening that upstream safety check.

Selected ASAN coverage included `tcp_driver_test`, `wire_frame_test`,
`wire_test`, `allocation_journal_test`, and `central_slab_test`, for **134** tests.
The first unpinned allocator invocation passed 37/38 and failed only this
non-sanitizer assertion:

```text
expected hint_hits=1 and fallback_scans=0
observed hint_hits=0 and fallback_scans=1
```

The VM exposes two NUMA nodes while the small test class has one bitmap shard. If
the process runs on the node without that local shard, fallback is the valid
allocator path. Re-running the same ASAN target under `taskset -c 0` passed
38/38. No ASAN memory diagnostic occurred in either run. The test should be made
NUMA-deterministic before treating its cursor-stat assertion as portable.

## Optimized binary and smoke gate

The Mino C++ benchmark binaries were built with
`--config=pipeline_comparison -c opt`. Their metadata reported:

- `compilation_mode=opt`;
- `canonical_descriptor_closure=startup-prepared` for Mino TCP;
- `receive_batch_size=1`;
- production plaintext `TcpDriver`, canonical schema codec, WireFrame v1, and
  payload CRC;
- same-host clock mode with `one_way_latency_valid=true`.

A 500-message Mino TCP small-profile smoke passed across six independent role
processes. Every worker reported 500 offered and received messages, with zero
loss, duplication, out-of-order delivery, and corruption. The sink observed
p50 `8.241028 ms`, p99 `9.026432 ms`, and `42,904.57 msg/s`.

A separate one-boot Mino hybrid smoke correctly derived an all-SHM five-edge map.
It exercised the generated SHM frame, allocation journal v3,
deadline-bounded blocking publication, typed publisher/subscriber path, and
normal ACK reclaim:

| Profile | Messages | p50 | p95 | p99 | Throughput |
|---|---:|---:|---:|---:|---:|
| small, 256 B | 500 | 0.791051 ms | 1.138499 ms | 1.279885 ms | 24,997.44 msg/s |

All 500 messages were received without integrity or ordering errors. This
one-boot run does not exercise a cross-host bridge.

## Candidate-only Mino TCP saturation observations

The network runner launched six independent Mino TCP processes and five
loopback TCP hops. Publication interval was zero and receive batch size remained
1. Each profile is one round, so these values are validation observations rather
than a statistical claim.

| Profile | Payload | Messages | Encoded bytes | p50 | p95 | p99 | Maximum | Throughput |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| small | 256 B | 10,000 | 453 B | 70.280272 ms | 104.031218 ms | 104.949162 ms | 105.193008 ms | 58,975.88 msg/s |
| medium | 64 KiB | 2,000 | 65,734 B | 25.021793 ms | 48.235397 ms | 49.901588 ms | 51.800730 ms | 1,966.35 msg/s |
| large | 1 MiB | 200 | 1,048,773 B | 73.111840 ms | 102.517418 ms | 113.092746 ms | 113.483563 ms | 127.86 msg/s |

Every profile reported exact offered/received conservation and zero duplicate,
out-of-order, corrupt, or lost messages. All worker stderr logs were empty.

The latency values include queue residence under an immediate offered burst. In
particular, the 10,000-message small run deliberately builds a long saturation
queue; its percentile latency must not be described as unloaded transport
latency. A controlled paced campaign still requires CPU/NUMA placement, at least
five order-rotated rounds, and the rates in `PERFORMANCE_FOLLOWUP.md`.

## Build and environment limitations

- The C++ benchmark actions and the three required Mino binaries completed.
  Analysis of Bazel `py_binary` runner targets then failed because upstream
  `rules_python` refuses to install its hermetic interpreter for UID 0. Runner
  source execution used system Python 3.12 and its tests passed.
- Initial remote dependency resolution against the configured GitHub raw BCR
  mirror was too slow. A content-addressed Bazel repository cache copied from the
  development host supplied checksum-verified archives; remaining registry
  metadata still came from the configured mirror.
- No competitor backend, clean baseline ref, TSAN campaign, fault-injection
  campaign, allocation profiler, or `perf` recording was run in this validation.
- The saturation runs were not repeated and were not CPU-pinned.

## Evidence

| Evidence | Location / value |
|---|---|
| Remote result tree | `/root/Mino-validation-results` |
| Archived artifacts | `/root/Mino-validation-results-20260824.tgz` |
| Archive SHA-256 | `8d740ab730a13ff9e23d4bd1be436d2460b3d117121bfd1231291d7afef9b218` |
| TCP smoke manifest | `/root/Mino-validation-results/mino-tcp-smoke/manifest.json` |
| TCP profile manifests | `/root/Mino-validation-results/mino-tcp-{small,medium,large}/manifest.json` |
| Hybrid smoke manifest | `/root/Mino-validation-results/mino-hybrid-smoke/manifest.json` |

The runners cleaned all worker processes and POSIX shared-memory objects after
the campaign. The archive contains the manifests, worker result JSON, topology,
and empty stdout/stderr logs for the benchmark runs.

## Remaining qualification work

1. Commit the candidate and create clean baseline/candidate worktrees.
2. Make the cursor-cache statistics test independent of scheduler NUMA placement.
3. Run paced latency and saturation for at least five order-rotated rounds with
   fixed CPU/NUMA placement and captured environment metadata.
4. Run TSAN, fault injection, fuzzing, allocation profiling, and Linux `perf`.
5. Repeat same-host and two-host campaigns on controlled physical hardware.
