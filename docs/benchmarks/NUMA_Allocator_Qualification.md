# NUMA allocator benchmark and qualification

`//benchmarks/allocator:numa_allocator_benchmark` compares CentralSlab allocation on three memory-placement modes while worker threads remain pinned to CPUs from the first allowed NUMA node:

- `local`: `MPOL_BIND` to the worker CPU node;
- `interleave`: `MPOL_INTERLEAVE` (`stripe`) across every cpuset-allowed memory node;
- `remote`: `MPOL_BIND` to a different allowed node.

The benchmark uses the native `//mino/platform:numa` provider and direct Linux syscalls. It does not load or link `libnuma`. Each mode uses a fresh anonymous mapping, applies policy before allocator metadata/header first-touch, then reports throughput, p50/p95/p99/p99.9/max latency and process-local allocator NUMA metrics.

## Functional smoke

```sh
bazel build --config=gcc12 //benchmarks/allocator:numa_allocator_benchmark
bazel-bin/benchmarks/allocator/numa_allocator_benchmark \
  --threads=2 --iterations=1000 --slots=256 \
  --json=/tmp/mino-numa-smoke.json
```

A machine whose current process/cgroup cpuset allows fewer than two memory nodes produces:

```json
{
  "status": "SKIPPED",
  "qualification_eligible": false
}
```

That is a valid functional result, but it is never performance or qualification evidence. The development machine used while implementing D6-02 exposed only node 0, so only this short `SKIPPED` path was exercised locally; no formal local/interleave/remote numbers are claimed.

## Formal qualification

The dedicated workflow `.github/workflows/numa-allocator-qualification.yml` runs only on a self-hosted runner labelled `mino-numa`. Its runner is `//benchmarks/allocator:numa_qualification`.

Qualification is fail-closed and requires all of the following:

1. Linux and an explicit `physical-numa` runner attestation;
2. a clean worktree at the exact requested 40-character commit;
3. available CPU governor provenance with every CPU at the configured governor;
4. at least two memory nodes after intersecting online nodes, `Mems_allowed_list`, and cgroup cpuset;
5. at least 1,000,000 iterations per worker;
6. complete `local`, `interleave`, and `remote` JSON results;
7. zero allocation failures and zero bind errors;
8. non-empty benchmark JSON/log artifacts with SHA-256 entries in the final manifest.

`SKIPPED`, incomplete JSON, a missing mode/artifact, syscall fallback, or a non-clean source tree makes the runner exit non-zero and sets `qualification_eligible=false`. The workflow re-verifies the final manifest in an `always()` step before uploading evidence.

Example runner invocation on the prepared physical host:

```sh
python3 benchmarks/allocator/numa_qualification.py \
  --benchmark="$PWD/bazel-bin/benchmarks/allocator/numa_allocator_benchmark" \
  --output-dir=/var/tmp/mino-numa-qualification \
  --expected-commit=<full-commit-sha> \
  --native-attestation=physical-numa \
  --expected-governor=performance \
  --threads=8 --iterations=1000000 --slots=16384
```

The benchmark's `local`/`remote` counters describe allocator shard choice relative to the observed thread node. They do not prove physical page residency by themselves; a formal hardware report should retain host `numastat`/PMU evidence alongside the generated provenance when available.
