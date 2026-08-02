# Storage benchmark (D5-14)

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
