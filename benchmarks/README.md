# Validation benchmarks

## TCP / UDP / RDMA / Fabric transport matrix (D6-06/D6-07)

`//benchmarks/transport:transport_matrix_benchmark` runs the same Canonical Wire
application payload matrix over TCP, UDP, RDMA Driver staging, provider-direct
registered RDMA zero-copy, and IPCF/NTB/CXL Fabric windows. Client JSONL includes process CPU, p50/p99 RTT,
throughput, wire size, copy mode, and provider provenance. The RDMA modes require
an absolute-path real device plugin and never fall back to the test mock.

Physical qualification is manual and two-host only:
`.github/workflows/rdma-qualification.yml` verifies ACTIVE/LINKUP device ports,
peer identity, exact commit, binary/plugin SHA-256, provider provenance, and the
complete result matrix before retaining evidence. See `docs/rdma-driver.md`.

Fabric qualification is independently manual and two-host only through
`.github/workflows/fabric-qualification.yml`. It requires an approved real device
plugin, active physical device/link evidence, cross-Trust-Domain node identities,
and emits commit/binary/plugin/provider provenance. See `docs/fabric-driver.md`.

## Large-object-pool matrix and physical qualification (D6-08)

`//benchmarks/allocator:large_object_pool_benchmark` compares ordinary, actual
HugePage, and dynamic device-registration pools across three sizes and short/batch
usage. `//benchmarks/allocator:large_object_pool_qualification` binds the clean
commit, physical device/link and NUMA provenance, HugePage pool/actual mapping,
memlock, approved plugin SHA-256, throughput/p99/fragmentation/fallback SLAs, and
five hashed artifacts. Local mock/no-device runs are explicitly non-qualified.
See `docs/large-object-pool.md` and
`.github/workflows/large-object-pool-qualification.yml`.

## Validation benchmark target

`//benchmarks:validation_benchmark` directly exercises existing production APIs for V-14, V-15, V-16, V-17, V-18, and V-27. Build and run:

```bash
bazel build --config=release //benchmarks:validation_benchmark
MINO_BENCHMARK_COMMIT="$(git rev-parse HEAD)" \
MINO_BENCHMARK_BUILD_CONFIG="bazel --config=release" \
bazel run --config=release //benchmarks:validation_benchmark -- \
  --suite=all --directory=/tmp \
  --output-json=/tmp/mino-validation-benchmark.json
```

Use `--suite=memory` or `--suite=storage` to isolate phases. Full options, methodology, JSON Schema, and the no-data template are documented in `docs/benchmarks/Validation_Benchmark_Methodology.md`.

# Topic Partition benchmark (V-24)

`//benchmarks:storage_partition_benchmark` runs the production per-partition
`RecorderBufferPool` + `TopicWriter` path at exactly 1, 2, 4, 8, and 16
partitions. It emits `mino.storage_partition_benchmark.v1` JSON containing the
measured single-writer threshold, aggregate scaling and efficiency,
per-partition throughput imbalance, and record p50/p99 latency. `--target-ingress-rps`
marks whether the measured one-writer threshold requires partitioning.

Formal qualification is driven by
`benchmarks/storage_partition_qualification.py`; free-form hardware naming is
not qualification evidence. The runner probes CPU/governors, `findmnt`, and
`lsblk`, executes multiple concurrent processes for multiple complete rounds,
validates errors/record conservation/stable-map/imbalance/p99/throughput, and
applies the versioned single-writer/scaling-efficiency policy. Non-target hosts
produce only `outcome=nonqualified` with `qualification_eligible=false`.

```bash
bazel run --config=release //benchmarks:storage_partition_benchmark -- \
  --records=20000 --payload-bytes=1024 \
  --target-ingress-rps=1000000 \
  --qualification-hardware="i9-9900KS; Samsung 980 PRO NVMe; ext4; Linux x86-64" \
  --directory=/code/Mino/.cache \
  --output-json=/tmp/mino-storage-partition-v24.json
```

The deterministic merge/order correctness tests are not inferred from benchmark
throughput; they remain part of `//mino/storage:replay_engine_test` and
`//mino/storage:topic_partition_test`. Complete qualification methodology,
policy, evidence hashes, and the self-hosted workflow are documented in
`docs/benchmarks/Storage_Partition_Qualification.md`.

# Storage benchmark

`//benchmarks:storage_benchmark` is a dependency-free C++20 benchmark driver for the production storage path. It directly exercises `segment_format`, `segment_writer`, `segment_recovery`, and `recorder_buffer_pool`; it does not use Google Benchmark.

## Release build and run

Always use the repository's Release configuration when producing numbers for an SLA document:

```bash
bazel build --config=release //benchmarks:storage_benchmark
bazel run --config=release //benchmarks:storage_benchmark -- \
  --records=20000 \
  --payload-bytes=1024 \
  --sync-policy=per-batch \
  --directory=/tmp \
  --output-json=/tmp/mino-storage-benchmark.json
```

The human-readable summary is written to stderr. The complete `mino.storage_benchmark.v1` JSON document is written to stdout and, when `--output-json` is supplied, to that file as well. This separation allows machine capture without filtering:

```bash
bazel run --config=release //benchmarks:storage_benchmark -- \
  --records=20000 --payload-bytes=1024 --sync-policy=per-batch \
  > /tmp/mino-storage-benchmark.json
```

## Options

| Option | Default | Meaning |
|---|---:|---|
| `--records` | `20000` | Measured records for encode, writer, and MPSC; must be greater than zero. |
| `--payload-bytes` | `1024` | Deterministic payload bytes per record. The maximum is 16 MiB, matching `recorder_buffer_pool`. |
| `--sync-policy` | `per-batch` | `none`, `interval`, `per-batch`, or `per-record`, mapped directly to `SegmentSyncPolicy`. |
| `--output-json` | unset | Additionally write the exact stdout JSON document to this path. |
| `--directory` | system temp | Base directory under which the benchmark creates its own unique temporary child. |

Both `--option=value` and `--option value` forms are accepted.

For `interval`, the benchmark uses a deterministic byte threshold of 128 encoded records. Writer batching is disabled internally and an explicit `Flush()` is issued every 128 records, so flush samples and interval decisions do not depend on scheduler timing.

## Measurements and methodology

All elapsed time and latency measurements use `std::chrono::steady_clock`.

- **EncodeRecord latency:** one latency sample per measured record; reports p50, p95, p99, p99.9, and max in nanoseconds.
- **SegmentWriter:** reports `Append()` records/s using accumulated append-call time and encoded write MiB/s using the complete measured writer interval (append, explicit flushes, selected sync policy, and final seal).
- **Flush and fdatasync latency:** reports the same latency percentiles. `fdatasync` is sampled by a narrow writer hook that times the real system call; the final `Seal()` sync is included. With `none`, explicit flushes do not sync and the seal supplies one fdatasync sample.
- **Recovery:** performs one warmup scan and five measured scans of the same clean sealed segment; reports scan-time distribution, aggregate records/s, MiB/s, and total measured time.
- **Buffer MPSC:** uses four producer threads and one consumer through the real bounded `RecorderBufferPool` reserve/commit/dequeue path. Producers deterministically initialize every payload byte. It reports records/s and payload MiB/s.
- **Warmup:** encode, writer, and buffer warm up with 10% of `--records`, clamped to `[1, 10000]`; recovery uses one warmup scan. Warmup counts are present in JSON and excluded from measured samples.
- **Percentiles:** nearest-rank over successful samples, with no interpolation. JSON latency units are nanoseconds and byte throughput units are MiB/s (2^20 bytes/s).
- **Errors:** every result section includes attempted operations, errors, and an explicit error rate. Unexpected storage errors fail the run; the MPSC result also reports incomplete records and internal errors.

The payload is generated from the fixed seed recorded in JSON. Record metadata, producer count, per-producer workload partition, writer batch size, and recovery sample count are fixed; operating-system thread interleaving is intentionally not claimed to be deterministic. For comparable SLA runs, keep the command line, Release compiler/toolchain, machine, filesystem, free-space state, power policy, and background load constant. Run multiple process invocations and cite both the JSON schema version and full configuration alongside selected metrics.

## Temporary-directory safety

`--directory` is only a base directory; it is never recursively removed. The benchmark creates a unique child whose name starts with `mino-storage-benchmark-` and writes `.mino-storage-benchmark-owned` inside it. Cleanup calls `remove_all` only when all of the following still hold:

1. the path is a real directory rather than a symlink;
2. it remains an immediate child of the canonical requested base;
3. its name has the benchmark prefix; and
4. the ownership marker has the exact expected contents.

If verification fails, cleanup is refused and a warning is printed.
