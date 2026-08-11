# D6-09 / V-24 Storage Partition Qualification

## Scope

`//benchmarks:storage_partition_benchmark` exercises the production
`RecorderBufferPool` and per-partition `TopicWriter` path without changing the
Storage implementation. The qualification runner executes the complete ordered
`1/2/4/8/16` partition matrix in multiple independent OS processes and repeats
that concurrent process group for multiple rounds.

The qualification establishes the measured single-writer threshold and the
scaling efficiency of immutable stable partition maps. It does not replace the
ordering, generation cutover, recovery, or replay correctness tests under
`//mino/storage`.

## Qualification target

The versioned policy is
`benchmarks/storage_partition_qualification_sla.json`. Qualification currently
requires all of the following, probed by the runner rather than supplied as a
free-form hardware claim:

- Linux x86-64 on an Intel Core i9-9900KS target CPU;
- every exposed CPU frequency governor set to `powersave`, matching the Storage
  SLA baseline;
- a Samsung SSD 980 PRO local, non-rotational NVMe parent device;
- the benchmark directory mounted as `ext4`, with `rw` and without `nobarrier`
  or `data=writeback`;
- a clean tracked and untracked worktree at the exact expected 40-character
  commit, both before and after all runs;
- the exact release build command recorded by the policy.

A valid run on other hardware is useful comparative evidence, but its outcome is
`nonqualified` and `qualification_eligible` remains `false`. A caller cannot
turn such a run into qualification evidence by naming target hardware: the only
accepted attestation mode is `auto-probed-local-block-device`.

## Execution model

The default qualification is three rounds with two concurrent benchmark
processes per round. Every process independently executes all five partition
counts and writes a separate log and JSON artifact. Qualification requires at
least two rounds and two processes.

```bash
bazel test --lockfile_mode=error --config=gcc12 \
  //benchmarks:storage_partition_qualification_contract_test
bazel build --lockfile_mode=error --config=gcc12 --config=release \
  //benchmarks:storage_partition_benchmark

python3 benchmarks/storage_partition_qualification.py \
  --repo="$PWD" \
  --benchmark="$PWD/bazel-bin/benchmarks/storage_partition_benchmark" \
  --output-dir=/tmp/mino-storage-partition-v24 \
  --storage-dir=/code/Mino/.cache \
  --expected-commit="$(git rev-parse HEAD)" \
  --policy="$PWD/benchmarks/storage_partition_qualification_sla.json" \
  --schema="$PWD/docs/validation/Storage_partition_qualification_artifact.schema.json" \
  --build-config="bazel --lockfile_mode=error --config=gcc12 --config=release" \
  --hardware-attestation=auto-probed-local-block-device \
  --rounds=3 --processes=2
```

The command intentionally rejects the repository's current uncommitted state.
Use it only from a clean checkout when producing qualification evidence.

## Per-process contract validation

Each `mino.storage_partition_benchmark.v1` document is rejected unless all of
these checks pass:

1. JSON identity and workload configuration are complete and exactly match the
   policy.
2. The scenario set is ordered and exactly `1/2/4/8/16`, with no missing or
   duplicate partition count.
3. Top-level, scenario, and per-partition `errors` are exactly zero.
4. `attempted == accepted == dequeued == written == configured records`, both
   per partition and after summation; deterministic remainder placement is also
   checked.
5. Every scenario names the same immutable map identity: map version 1,
   generation 1, active hash strategy, stable hash algorithm version 1, fixed
   hash seed, and the matching partition count.
6. Aggregate and per-partition throughput are positive, finite, and recompute
   from records and elapsed nanoseconds.
7. The reported throughput imbalance recomputes from per-partition rates and is
   no greater than the policy validation limit.
8. p50/p99 are positive and ordered; p99 is no greater than the policy
   validation limit.
9. Scaling, scaling efficiency, single-writer threshold, and
   `partitioning_required_for_target` recompute from the raw observations.

Validation limits protect evidence quality. SLA pass/fail is separately based
on the versioned single-writer and scaling-efficiency checks.

## SLA aggregation

The runner uses the minimum observation across every process and round. The
policy currently requires:

- single-writer threshold at least `15,360 records/s`, corresponding to the
  existing 1 KiB / 15 MiB/s Storage baseline;
- minimum scaling efficiency of `1.00`, `0.45`, `0.25`, `0.12`, and `0.06` for
  `1`, `2`, `4`, `8`, and `16` partitions respectively.

The manifest includes each policy check and throughput/imbalance/p99 summaries.
Any failed check makes the final outcome `failed`; metrics are never silently
accepted because a host is non-target.

## Evidence and outcomes

`manifest.json` follows
`docs/validation/Storage_partition_qualification_artifact.schema.json` and
records:

- expected, before, and after commits and clean/dirty source states;
- canonical workload/run/build configuration and its SHA-256;
- policy, artifact schema, and benchmark binary SHA-256;
- CPU model, kernel, architecture, all observed governors;
- filesystem source/type/target and sorted mount options;
- resolved block device/parent, model, serial, transport, and rotational flag;
- every exact argv command, shell rendering, exit code, timing, log artifact,
  benchmark JSON artifact, byte count, and SHA-256;
- validation/SLA observations, errors, completeness, eligibility, and outcome.

Outcomes are fail-closed:

| Outcome | `qualification_eligible` | Meaning |
|---|---:|---|
| `passed` | `true` | Target host, clean exact commit, complete untampered evidence, and all validation/SLA checks passed. |
| `nonqualified` | `false` | Valid complete run and SLA pass, but real probes do not match the target hardware/filesystem/governor policy. |
| `failed` | `false` | Dirty/wrong source, fake attestation, execution/JSON/hash/conservation/map/metric error, incomplete artifacts, or failed SLA. |

Re-verify retained evidence with:

```bash
python3 benchmarks/storage_partition_qualification.py \
  --verify-manifest=/tmp/mino-storage-partition-v24/manifest.json \
  --require-expected-commit="$(git rev-parse HEAD)" \
  --require-qualified
```

The scheduled/manual workflow is
`.github/workflows/storage-partition-qualification.yml` and only targets
`[self-hosted, linux, x64, storage-performance]` runners.
