# Pipeline performance follow-up plan

This document records the validation work required after the prepared canonical
codec and payload-reuse optimization, plus the remaining optimization backlog.
Historical result files remain immutable; new campaigns must write new artifact
directories and a dated result report.

## Current evidence and hypothesis

The existing reports show one competitive point estimate where Mino is slower:

- two-host, saturation, 256 B payload: Mino TCP `28,214.75 msg/s` versus
  Cyclone DDS `51,499.68 msg/s`.

This is not yet a statistically qualified loss: it came from one round, the DDS
history and transport policies differed, and CPU/IRQ placement was uncontrolled.
A later Mino-only run reached `39,557.43 msg/s`, while Mino hybrid reached
`62,530.89 msg/s`.

The strongest code-level explanation for the small-message fixed cost was that
`CanonicalWireCodec` authenticated and rebuilt the immutable descriptor resolver
for every encode and decode. The candidate implementation now uses
`PreparedCanonicalWireCodec`, prepares the closure at process startup, reuses
18-field dynamic messages, codec scratch and output capacity, reuses semantic
payload capacity, uses bulk payload copies, and combines checksum and
deterministic-byte validation into one pass. A local optimized microbenchmark of
the real 18-field, 256 B schema observed:

| Operation | Revalidating codec | Prepared codec | Local speedup |
|---|---:|---:|---:|
| encode | 7,347 ns/op | 1,666 ns/op | 4.41x |
| decode | 6,500 ns/op | 1,775 ns/op | 3.66x |

These are diagnostic codec numbers, not an end-to-end throughput claim. The
campaigns below determine whether the improvement survives framing, transport,
scheduling, validation, and five pipeline hops.

A candidate-only Linux run on 2026-08-24 has now established an initial
correctness and integration gate: 225 functional tests and 134 selected ASAN
tests passed, all three Mino TCP `-c opt` saturation profiles conserved every
message, and one-boot hybrid/all-SHM smoke passed. The checkout was dirty, the
4-vCPU KVM guest was not isolated or pinned for the performance runs, and no
baseline was run, so this evidence does not satisfy the clean-ref A/B or formal
performance phases below. See `RESULTS_LINUX_VALIDATION_20260824.md`.

## Required refs and artifact naming

Before any performance run, record two immutable refs:

- `BASELINE_REF`: the correctness-fixed code before the performance changes;
- `CANDIDATE_REF`: the prepared-codec and payload-reuse code under test.

Do not compare a dirty checkout with a clean checkout. Build each ref in a
separate worktree, record `git rev-parse HEAD`, and require an empty
`git status --short`. If a baseline ref cannot be reconstructed, use historical
manifests only as directional evidence and do not label the run an A/B result.

Use new, non-overlapping output directories, for example:

```text
.cache/pipeline-perf-YYYYMMDD/<ref>/<campaign>/<backend>/<profile>/round-N/
```

Retain every manifest, worker result, stdout/stderr log, binary hash, descriptor
hash, topology, environment file, and preflight record. Generate SHA-256 hashes
for final manifests and never overwrite a failed run.

## Phase 0: correctness and build gate

Candidate status: initial Linux functional, selected ASAN, optimized Mino binary,
and six-process smoke gates passed on 2026-08-24. Clean baseline/candidate refs,
non-root Bazel runner targets, remaining backends, TSAN/fault injection, and the
full formal gate are still required.

Run on Linux for both refs before collecting performance data:

```sh
bazel test --nocache_test_results \
  //mino/schema:wire_test \
  //benchmarks/pipeline_comparison:pipeline_common_test \
  //benchmarks/pipeline_comparison:pipeline_comparison_runner_test \
  //benchmarks/pipeline_comparison:pipeline_network_runner_test \
  //benchmarks/pipeline_comparison:mino_hybrid_runner_test

bazel build --config=pipeline_comparison -c opt \
  //benchmarks/pipeline_comparison:mino_tcp_pipeline \
  //benchmarks/pipeline_comparison:mino_shm_pipeline \
  //benchmarks/pipeline_comparison:mino_shm_tcp_bridge \
  //benchmarks/pipeline_comparison:protobuf_zmq_pipeline \
  //benchmarks/pipeline_comparison:fastdds_pipeline \
  //benchmarks/pipeline_comparison:cyclonedds_pipeline \
  //benchmarks/pipeline_comparison:pipeline_comparison_runner \
  //benchmarks/pipeline_comparison:pipeline_network_runner \
  //benchmarks/pipeline_comparison:mino_hybrid_runner
```

Required outcome:

- all tests and builds pass;
- every smoke run has zero loss, duplicate, out-of-order, and corruption counts;
- worker metadata reports `compilation_mode=opt`;
- Mino TCP metadata reports
  `canonical_descriptor_closure=startup-prepared` for the candidate only.

Run one local six-process smoke for every backend and all profiles before using
physical hosts. Use at least 100 measured messages and 10 warmup messages; this
is a correctness gate, not a performance result.

## Phase 1: controlled codec benchmark

Add or use a dedicated benchmark target that compares the revalidating and
prepared codec with identical prebuilt `DynamicMessage` inputs. Do not use a
unit-test timing assertion.

Required cases:

- the exact 18-field schema;
- payloads of 256 B, 64 KiB, and 1 MiB;
- encode, decode, and encode+decode;
- one thread and concurrent read-only calls on copied prepared codec handles;
- at least five process executions, reporting median and range;
- allocations/op and bytes allocated/op if an allocator profiler is available.

Record compiler, CPU, optimization mode, iteration count, output checksum, and
wire-byte equality. The prepared and revalidating paths must produce identical
canonical bytes.

Success criteria:

- small encode and decode median time each improve by at least 2x;
- medium and large do not regress by more than 3%;
- no increase in encoded bytes;
- no correctness or thread-safety failure.

## Phase 2: same-host regression campaign

This campaign catches latency or throughput regressions without network noise.
Use an isolated Linux host and the same CPU placement for both refs.

### Paced latency

Run at least five order-rotated rounds:

| Profile | Rate | Measured messages | Warmup |
|---|---:|---:|---:|
| small | 100 Hz | 2,000 | 200 |
| medium | 20 Hz | 500 | 50 |
| large | 10 Hz | 200 | 20 |

Use the README paced-latency runner settings. Report each round and the median of
per-round p50/p95/p99 values. Report p99.9 only when a round has at least 1,000
measured samples.

### Saturation

Run Mino TCP through the one-host network runner, plus Mino SHM as a control:

| Profile | Measured messages | Warmup |
|---|---:|---:|
| small | 200,000 | 20,000 |
| medium | 5,000 | 500 |
| large | 500 | 50 |

Success criteria for the candidate relative to the baseline:

- small Mino TCP median throughput improves by at least 15%;
- no profile loses more than 3% throughput;
- paced p99 does not regress by more than 5%;
- all paired rounds preserve message conservation and wire-byte size;
- candidate CPU cycles/message and allocations/message decrease for small.

## Phase 3: paired two-host Mino A/B campaign

Use the documented 3+3 topology with exactly one physical Ethernet edge. Run
full TCP and hybrid for both refs. Alternate order by round so thermal and load
trends do not favor one variant:

```text
round 1: baseline TCP, candidate TCP, baseline hybrid, candidate hybrid
round 2: candidate hybrid, baseline hybrid, candidate TCP, baseline TCP
round 3: baseline hybrid, candidate hybrid, baseline TCP, candidate TCP
```

Use at least five rounds if host time permits; three is the minimum. Use unique
port bases and empty output directories.

| Profile | Measured messages | Warmup | Deadline |
|---|---:|---:|---:|
| small | 200,000 | 20,000 | 180 s |
| medium | 5,000 | 500 | 180 s |
| large | 500 | 50 | 180 s |

Without qualified PTP, report only reliability, encoded size, CPU/resource
metrics, and independent-host sink completion throughput. Do not report
cross-host one-way latency.

Primary success criteria:

- candidate TCP small median throughput improves by at least 15% over baseline;
- the lower bound of a paired bootstrap 95% confidence interval for the
  candidate/baseline small throughput ratio is greater than 1.0;
- candidate hybrid small does not regress by more than 3%;
- candidate hybrid medium is not slower than candidate full TCP by more than 3%;
- every run has zero loss, duplicate, out-of-order, corruption, and timeout;
- encoded sizes are unchanged.

Secondary target: candidate Mino TCP small should be rerun against Cyclone DDS;
do not claim the historical `51.5k msg/s` gap is closed until Phase 4 is complete.

## Phase 4: fair competitor matrix

Run Mino TCP, Mino hybrid, Protobuf+ZeroMQ, Fast DDS, and Cyclone DDS under a
recorded common policy where possible.

Required controls:

- at least five order-rotated rounds;
- common, qualified DDS UDP-only transport policy;
- an explicitly qualified in-flight/backpressure contract rather than blindly
  assigning every backend the number 64;
- identical payload, message, warmup, and deadline counts;
- fixed CPU affinity for role processes;
- fixed NIC IRQ affinity and recorded queue/RSS mapping;
- recorded MTU, NIC offloads, socket buffer limits, CPU governor, turbo policy,
  kernel, boot IDs, temperatures, and competing load;
- identical opt builds and dependency versions on both hosts.

Suggested saturation counts for the full matrix:

| Profile | Measured messages | Warmup |
|---|---:|---:|
| small | 100,000 | 10,000 |
| medium | 1,000 | 100 |
| large | 100 | 10 |

If a backend cannot preserve all messages at the common in-flight bound, record
the failed qualification and adjust the policy for every backend before making a
ranking. Do not mix successful runs that use materially different resource
contracts in the same performance table.

Report per-round throughput, paired ratios, median, MAD/range, and bootstrap
confidence intervals. A competitive loss is confirmed only when the repeated
Mino/competitor ratio remains below 1.0 and the confidence interval excludes
1.0 by a practically meaningful margin.

## Phase 5: profiling and attribution

For baseline and candidate small TCP and medium hybrid, collect at least:

- `perf stat`: cycles, instructions, branches, branch misses, cache references,
  cache misses, context switches, migrations, page faults, task-clock;
- `perf record` call graphs for each role, especially the slowest forwarder;
- allocation count and allocated bytes by call site;
- process CPU time and peak RSS;
- TCP bytes, segments, retransmits, and socket queue occupancy;
- scheduler traces if wakeups or migrations remain dominant.

The profile must distinguish:

- descriptor preparation/authentication;
- dynamic message construction and field lookup;
- canonical encode/decode;
- WireFrame encode/decode and CRC;
- `TcpDriver` queueing, wake pipe, poll, recv, and sendmsg;
- semantic payload validation;
- SHM graph allocation, journal publish/reclaim, and bridge copies.

Archive raw `perf.data` files and their hashes. Optimization claims must cite the
before/after cycle share, not only wall-clock throughput.


### P0/P1 closed on master (a1b76c3): same-host exclusive SHM hop

Status: implemented. SPSC forwarders use `TakeExclusive` +
`PublishLocal(ExclusiveMessage&&)`; sink/CANBus validates via payload span.
Source first publish still `AllocateChild`+`memcpy`. Pin/broadcast/MPSC rejected.
Destructor reclaims if not published; kill between Take and PublishLocal leaks
until region recreate. See `docs/optimization-status.md`. Same-host medium
saturation vs Fast DDS has **not** been re-measured on this commit.

## Remaining optimization backlog

Items are ordered by expected value and implementation risk. Do not start a
lower item until profiling shows the higher item is no longer dominant.

### P0: validate the completed prepared-codec work

Status: implemented, awaiting Linux same-host and two-host campaigns.

- authenticate descriptor closure once at startup;
- cache the descriptor resolver;
- reuse semantic payload capacity in TCP and hybrid bridge loops;
- bulk-copy canonical payloads;
- combine checksum and deterministic-byte validation into one pass.

### P1: remove generic canonical encoder fixed allocations

Status: implementation complete on master (`50dd0b9` / `a1b76c3` lineage).
`DynamicValue::BytesView` + `EncodeInto` are available; length-delimited encode
writes LEB128 then payload (nested via scratch) and no longer
`vector::insert`-memmoves an already-written payload. Linux allocation profiling,
controlled codec benchmarks, and end-to-end campaigns remain pending — do not
invent speedups.

Expected benefit: high for small messages; medium implementation risk.

- reserve the known dynamic field count before inserting 18 fields;
- avoid one `EncodedField::bytes` vector per known scalar field;
- write descriptor-sorted known fields directly to one reserved output buffer;
- merge unknown fields in canonical field-ID order instead of stable-sorting all
  fields;
- encode bytes/string length and content directly without an intermediate
  payload vector (**and without payload insert-memmove**);
- prefer `BytesView` over owning `Bytes` on EncodeInto hot paths;
- cache field tags and wire types in the prepared plan.

Required validation: golden wire vectors, unknown-field round trips, fuzz corpus,
allocation counts, and Phase 1 benchmark.

### P1: validated WireFrame decode view and ownership-taking send

Status: default data-path integration complete on master (`a1b76c3` closes the
remaining receive-tail steal). `LengthPrefixedFrameDecoder::Push` uses
`DecodeView`; Bridge data path uses `TrySendOwned` /
`TrySendUntrackedOwned`; `TcpDriver` steals a trailing complete frame from the
receive buffer (mid-buffer frames still `assign`). Control-plane
`WireFrameCodec::Decode` may still copy into an owned `WireFrame`. The three
TcpDriver mutexes are unchanged. `RetransmitWindow` still owns a frame copy for
reliable multi-attempt resend (intentional). Fuzzing, TSAN, clean-ref wire
qualification, and formal performance campaigns remain pending.

Expected benefit: medium for small, CPU/memory reduction for large; medium risk.

- decode API validates header CRC, payload CRC, lengths, and flags but exposes
  payload as a span into the owning received body;
- ensure the view cannot outlive the body;
- `TcpDriver` ownership-taking send keeps prefix and body as separate write
  segments;
- preserve current CRC and frame limits.

Required validation: malformed-frame tests, lifetime tests, fuzzing, TSAN/ASAN,
and unchanged wire bytes.

### P1: instrument and remove bridge-only duplicate deep validation

Status: implemented. `full` retains the previous deep check, `structural` keeps
only transit invariants, and `full-instrumented` records validation calls, payload
bytes, and thread CPU nanoseconds. Formal hybrid runs select `structural`; Linux
`perf` attribution remains pending.

Expected benefit: medium for 64 KiB and 1 MiB hybrid; low-to-medium risk.

The six business stages must continue validating every field and payload byte.
The two transport bridge processes currently perform additional deep semantic
validation. First add counters/profile attribution. If bridge scans are material:

- keep sequence, phase, profile, stage-mask, timestamp, graph ownership, CRC, and
  schema checks in both bridges;
- keep full semantic/payload validation in every business stage;
- remove only bridge validation that is provably duplicated and not part of the
  fairness contract.

Do not remove graph root/child transaction ownership checks.

### P2: reuse dynamic message and codec scratch storage

Status: implemented in Mino TCP and both bridge directions with per-worker
messages, `ReserveFields(18)`, prepared `EncodeInto`/`DecodeInto`, reusable
scratch, and reusable canonical output capacity; Linux allocation/perf validation
remains pending.

Expected benefit: medium for small; medium risk.

- add `DynamicMessage::ReserveFields`;
- reuse field and output scratch buffers per worker;
- consider a bounded per-worker arena for dynamic scalar values;
- clear every field deterministically between messages;
- keep public codec calls reentrant and thread-safe.

### P2: batch ready-message draining

Status: implemented as `--receive-batch-size=[1,64]` with a local ordered cache,
completion-ACK retention, runner passthrough, and artifact metadata. The default
remains 1 for fairness; Linux batching sweeps and equivalent competitor policies
remain pending.

Expected benefit: medium for small when thread handoff dominates; medium risk.

- allow `TcpDriver` receive processing to extract multiple complete frames per
  worker turn within an explicit byte/message budget;
- let the benchmark drain a small ordered batch instead of polling one message;
- preserve per-message validation and completion ordering;
- run an equivalent batching policy for comparison backends before publishing a
  fairness ranking.

### P2: avoid SHM graph rebuild on transient queue-full

Status: implemented for the Mino SHM source/forwarders and bridge sink using
deadline-bounded `QueueFullPolicy::kBlock`; the outer queue-full rebuild loop was
removed. Linux stalled-consumer and live-set/performance validation remains
pending.

Expected benefit: unknown until instrumented; low-to-medium risk.

- count `QueueFullPolicy::kFail` rebuilds and aborted root/child graphs;
- if nonzero, evaluate deadline-bounded `QueueFullPolicy::kBlock` so an already
  built graph waits for a slot instead of being allocated and copied again;
- verify allocator live-set bounds and deadline behavior under stalled consumers.

### P2: allocation journal small-graph fast path

Status: implemented with two inline root/child handles per journal record and a
shared-memory overflow sidecar for larger graphs. Recovery, tombstone, replay,
and persistence-point tests cover both inline and overflow paths; Linux
allocation/performance qualification remains pending. This changes the journal
layout to v3: existing v2 shared segments fail closed with `kSchemaMismatch` and
must be reinitialized before deployment or process restart.

Expected benefit: medium if per-message journal allocation is visible; medium
risk.

- store the common root-plus-one-child handle list in an inline buffer;
- fall back to the bounded shared-memory sidecar for larger graphs;
- preserve crash recovery, tombstone, and replay behavior;
- add allocator/journal recovery tests before benchmarking.

### P3: O(children) normal graph reclaim

Status: implemented for generated graphs whose owned allocations are direct leaf
fields. Generated traits emit a deterministic root-first manifest; normal ACK
reclaim validates every exact handle and reclaims children before the root.
Nested owned-graph traversal remains explicitly unsupported, and Linux stress,
TSAN, and fault-injection qualification remains pending.

Expected benefit: potentially high if allocator scans remain visible; high risk.

- generated traits should enumerate child handles for normal ACK reclaim;
- use full allocator scans only for recovery append-gap cases;
- preserve generation, owner epoch, transaction ID, and pin safety;
- require stress, crash-recovery, TSAN, and fault-injection tests.

### P3: safe generated graph ownership forwarding

Status: not implemented. The hybrid bridge still performs
graph-to-semantic-to-wire and wire-to-semantic-to-graph payload copies; this
requires a reviewed lifetime and ownership-transfer design before coding.

Expected benefit: high for medium/large hybrid; very high risk.

- avoid graph-to-semantic-to-graph payload copies at a cross-host boundary;
- define explicit immutable borrow and ownership-transfer capabilities;
- prevent old-root ACK from reclaiming a child referenced by a new root;
- avoid reintroducing synchronous `ShmPinTable::PinCount()` scans;
- require a reviewed lifetime design before implementation.

### P3: fuse CRC with final encode/copy

Status: implemented on WireFrame encode with runtime-selected
`Crc32cAccumulator::CopyAndUpdate`; wire-equivalence and alignment/tail tests are
included, and selected Linux ASAN passed on 2026-08-24. `perf` attribution,
fuzzing, and the full sanitizer matrix remain pending.

Expected benefit: low for small, potentially measurable for large; medium risk.

- calculate payload CRC while writing the final WireFrame body;
- avoid a separate complete payload scan;
- keep exactly the same CRC field and validation semantics;
- do not disable CRC for benchmark ranking.

## Decision rules

After each phase:

1. If correctness fails, stop and preserve artifacts.
2. If the candidate improvement is below 5% and within run-to-run noise, do not
   claim a performance win.
3. If small improves but medium/large regress beyond the thresholds above,
   profile before merging further optimization.
4. If Mino remains slower than Cyclone small under the fair Phase 4 policy,
   prioritize canonical encoder allocations, WireFrame copies, and driver
   handoff/batching using Phase 5 profiles.
5. Do not optimize protocol integrity, message validation, or resource safety out
   of the comparison merely to improve a score.
