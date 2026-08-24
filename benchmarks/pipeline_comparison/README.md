# Autonomous pipeline end-to-end comparison

This benchmark compares process-separated pipelines derived from the
autonomous-system topology. It has a same-host comparison runner and a network
runner for one-host or multi-host, six-process qualification:

```text
Perception -> Prediction -> Planning -> Control -> Guardian -> CANBus
```

The available backends are:

1. **Fast DDS + typed DDS IDL type support**;
2. **Cyclone DDS + build-time `idlc` generated C type**;
3. **Protobuf + ZeroMQ, using IPC for host-local edges and TCP for cross-host edges**;
4. **Mino production SHM allocator + SPSC channels + generated typed runtime**;
5. **Mino production TCP driver + WireFrame + canonical Mino schema codec**;
6. **Mino hybrid, using generated SHM graphs locally and canonical TCP bridges across hosts**.

The same-host matrix uses the first four backends. The network runner compares
Mino TCP, Protobuf+ZeroMQ, Fast DDS, and Cyclone DDS. The dedicated
`mino_hybrid_runner` derives a per-edge SHM/TCP map from Linux boot IDs.

The benchmark is intentionally outside production targets. Fast DDS and
Protobuf are comparison-only dependencies.

## Current implementation status

- Same-host six-process campaigns for Fast DDS, Protobuf+ZeroMQ, and Mino SHM
  have completed; historical results are in `RESULTS_20260816.md`.
- Cyclone DDS, Mino TCP, mixed IPC/TCP Protobuf+ZeroMQ, and Mino hybrid are
  implemented for one-host and multi-host qualification.
- The network runner uses Linux boot IDs observed through every role's execution
  path to decide whether one-way latency is valid. Different boot IDs force
  `independent-hosts` mode.
- Multi-host mode measures message conservation, corruption, ordering, encoded
  size, and sink throughput. It intentionally emits no cross-host one-way
  latency without a future PTP qualification contract.
- A physical two-host capability campaign is recorded in
  `RESULTS_TWO_HOST_20260816.md`; it covers all three payload profiles with
  Mino TCP, Mino hybrid, Protobuf+ZeroMQ, Fast DDS, and Cyclone DDS. These are
  one-round capability/smoke observations, not a publication-grade comparison.
- Benchmark-side P1/P2 integration is complete: per-worker prepared canonical
  message/scratch/output reuse, zero-copy WireFrame decode views, ownership-taking
  TCP sends, bounded receive batching, deadline-bounded SHM publication, and
  structural bridge validation/artifacts are implemented.
- A candidate-only Linux validation is recorded in
  `RESULTS_LINUX_VALIDATION_20260824.md`: 225 functional tests and 134 selected
  ASAN tests passed, Mino TCP passed all three `-c opt` saturation profiles, and
  one-boot hybrid/all-SHM smoke passed with zero message-integrity failures. It
  used a dirty checkout on a 4-vCPU KVM guest, so clean-ref A/B, paced multi-round,
  `perf`, TSAN, and physical-host qualification remain pending; see
  `PERFORMANCE_FOLLOWUP.md`.

## Real schema and generation contract

`autonomy_pipeline.mino` and `autonomy_pipeline.idl` define the same 18-field
business message: source/stage timestamps, stage completion mask, perception and
trajectory counts, vehicle/control values, emergency-stop state, checksum, and
a bounded payload up to 1 MiB. This is the actual message transported at every
hop, not a mock envelope that bypasses middleware serialization.

- Mino SHM uses the `minoc`-generated `AutonomyPipelineFrame`, builder,
  accessor, graph allocation journal, and child payload slab.
- Mino TCP loads the real `minoc` descriptor artifact and uses a startup-prepared
  canonical codec with per-worker reusable 18-field `DynamicMessage`, scratch,
  and output storage inside production `WireFrameCodec` and `TcpDriver`. Receive
  framing uses an owning validated view, and send admission transfers the encoded
  body without another payload copy while preserving retry ownership on failure.
- Mino hybrid uses the generated SHM root/child-slab graph on each host; a
  cross-host bridge resolves that graph, canonical-encodes it over `TcpDriver`,
  then rebuilds the generated graph in the destination host's SHM segment. Formal
  hybrid runs use structural bridge validation because all six business stages
  retain full deterministic scalar/checksum/payload validation.
- Protobuf+ZeroMQ uses the generated `autonomy_pipeline.proto` C++ type on every
  IPC and TCP edge.
- Cyclone DDS runs official `idlc` during Bazel builds and uses its generated C
  topic and CDR descriptors.
- Fast DDS uses real typed Fast DDS serialization and transport, but its checked
  in support is still template-reproduced and must be replaced by a pinned JDK
  17/Fast DDS-Gen 4.2.0 regeneration before formal publication. It is not a
  mock data path, but its code-generation provenance remains provisional.

The repository's generic `schemas/sensor_frame.mino` is not used here because
its 4 KiB payload limit cannot represent the medium and large pipeline profiles.

## Fairness contract

All backends carry the same semantic `AutonomyPipelineFrame` fields and the
same deterministic payload. Each intermediate stage must:

1. receive/take the message;
2. deserialize or resolve and validate every semantic field and payload byte;
3. update its stage timestamp and stage-completion bit;
4. serialize/copy and publish to the next stage.

The source records `origin_timestamp_ns` immediately before constructing and
publishing a measured message. CANBus records a sample only after receiving,
deserializing/resolving, and fully validating the final message. Warmup samples
use a zero origin timestamp and are excluded.

Results are same-host one-way latency only. They use the Linux monotonic clock
shared by processes in one boot and must not be interpreted as cross-host
one-way measurements.

Default profiles:

| Profile | Deterministic application payload | Intended shape |
|---|---:|---|
| `small` | 256 B | control/guardian command |
| `medium` | 64 KiB | planning trajectory / object summary |
| `large` | 1 MiB | dense perception frame |

The result records application payload bytes and backend-specific encoded or
allocated bytes separately. The same-host comparison's nominal buffering
parameter is 64 for Fast DDS history/resource limits, ZeroMQ socket HWM, and
Mino SPSC rings; the hybrid runner uses its separately qualified SPSC default
of 8. These are not identical buffering semantics: Mino capacity is exact, DDS has
reader/writer histories, and ZeroMQ HWM is a per-pipe approximation. Every
artifact records the effective backend-specific value.

## Important scope boundary

The Mino SHM backend uses production `SharedMemorySegment`,
`CentralSlabAllocator`, `SpscChannel`, `Publisher<T>`, and `Subscriber<T>` hot
paths. Its five channels are described by a benchmark-static shared manifest.
It does **not** claim to measure `Bus` discovery or `SharedMemoryRegion`
supervisor lifecycle because the current Coordinator and local Bus deployment
do not provide cross-process discovery for this six-process topology. The hybrid
runner therefore creates one benchmark-static SHM manifest per Linux boot ID and
uses the production TCP/canonical schema path only for edges whose endpoints have
different boot IDs.

Fast DDS transport/QoS and ZeroMQ socket options are written into every result.
Initialization, DDS discovery, endpoint connection, warmup, control barriers,
and shutdown are excluded from measured latency.

## Required output

For every backend/profile/round, the sink result includes:

- p50, p95, p99, p99.9, and maximum end-to-end latency in nanoseconds;
- offered, received, duplicate, out-of-order, corrupt, and lost counts;
- elapsed time and messages/s;
- application and encoded/allocated byte sizes;
- effective QoS/socket/channel settings;
- clock, host, boot ID, commit, compiler, binary, and schema provenance.

A non-zero loss, corruption, duplicate, stage-mask mismatch, payload mismatch,
or timeout makes that run fail while preserving its artifact. Every cross-host
bridge also writes an atomic `mino.pipeline_bridge_benchmark.v1` JSON artifact
containing run/edge/mode/profile/clock/compilation identity, validation mode,
outcome/error, optional validation instrumentation, and sent/received wire counts.

## Build

The comparison dependencies are isolated behind an explicit Bazel config. Fast
DDS 3.4.x is compiled as C++17 because it uses allocator APIs removed in C++20;
Mino and all benchmark-owned code remain C++20 with warnings as errors.

```sh
bazel build --config=pipeline_comparison -c opt \
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

If the configured GitHub raw BCR mirror is temporarily unavailable, a local
build may select the official registry for that invocation:

```sh
bazel build --registry=https://bcr.bazel.build \
  --config=pipeline_comparison -c opt \
  //benchmarks/pipeline_comparison/...
```

## Run the comparison

The runner executes backends serially and rotates their order between rounds.
It starts six process groups per run, waits for explicit readiness, atomically
opens the start barrier, validates every worker result, and writes an artifact
manifest even when a run fails.

```sh
bazel-bin/benchmarks/pipeline_comparison/pipeline_comparison_runner \
  --output-dir=.cache/pipeline-comparison \
  --rounds=3 \
  --profiles=small,medium,large \
  --small-messages=10000 \
  --medium-messages=2000 \
  --large-messages=200 \
  --deadline-seconds=120
```

For a quick integration smoke:

```sh
bazel-bin/benchmarks/pipeline_comparison/pipeline_comparison_runner \
  --output-dir=.cache/pipeline-comparison-smoke \
  --rounds=1 \
  --profiles=small,medium,large \
  --small-messages=20 \
  --medium-messages=10 \
  --large-messages=2 \
  --warmup-ratio=0.1 \
  --deadline-seconds=60
```

The output directory must not contain existing files. Each run retains six
worker JSON files, stdout/stderr logs, hashes, effective commands, sink metrics,
and a top-level `manifest.json`.

## One host and multi-host network tests

`pipeline_network_runner` always launches six independent role processes. A
role topology JSON decides whether each process runs locally or through
non-interactive SSH. The repository and requested Bazel binaries must already
exist at the same configured absolute `workdir` on each remote machine.

### One host, six processes

Create a topology using the absolute checkout path (replace `/srv/Mino`):

```json
{
  "schema": "mino.pipeline_network_topology.v1",
  "roles": {
    "perception": {"ssh_host": "local", "data_address": "127.0.0.1", "workdir": "/srv/Mino", "environment": {}},
    "prediction": {"ssh_host": "local", "data_address": "127.0.0.1", "workdir": "/srv/Mino", "environment": {}},
    "planning": {"ssh_host": "local", "data_address": "127.0.0.1", "workdir": "/srv/Mino", "environment": {}},
    "control": {"ssh_host": "local", "data_address": "127.0.0.1", "workdir": "/srv/Mino", "environment": {}},
    "guardian": {"ssh_host": "local", "data_address": "127.0.0.1", "workdir": "/srv/Mino", "environment": {}},
    "canbus": {"ssh_host": "local", "data_address": "127.0.0.1", "workdir": "/srv/Mino", "environment": {}}
  }
}
```

Run the four backends handled by the general network runner into separate empty
output directories:

```sh
for backend in mino_tcp protobuf_zmq fastdds cyclonedds; do
  bazel-bin/benchmarks/pipeline_comparison/pipeline_network_runner \
    --topology=/tmp/mino-one-host.json \
    --output-dir=".cache/pipeline-network-${backend}" \
    --backend="${backend}" \
    --profile=small \
    --messages=1000 \
    --warmup-messages=100 \
    --deadline-seconds=120
done
```

All six roles must report the same boot ID and clock properties before the
manifest sets `one_way_latency_valid` to true.

### Two or more hosts, multiple processes per host

A two-host split can place perception/prediction/planning on `host-a` and
control/guardian/CANBus on `host-b`. Each `data_address` is the numeric IPv4
address reachable by the preceding Mino TCP role:

```json
{
  "schema": "mino.pipeline_network_topology.v1",
  "roles": {
    "perception": {"ssh_host": "bench-a", "data_address": "10.20.0.11", "workdir": "/srv/Mino", "environment": {}},
    "prediction": {"ssh_host": "bench-a", "data_address": "10.20.0.11", "workdir": "/srv/Mino", "environment": {}},
    "planning": {"ssh_host": "bench-a", "data_address": "10.20.0.11", "workdir": "/srv/Mino", "environment": {}},
    "control": {"ssh_host": "bench-b", "data_address": "10.20.0.12", "workdir": "/srv/Mino", "environment": {}},
    "guardian": {"ssh_host": "bench-b", "data_address": "10.20.0.12", "workdir": "/srv/Mino", "environment": {}},
    "canbus": {"ssh_host": "bench-b", "data_address": "10.20.0.12", "workdir": "/srv/Mino", "environment": {}}
  }
}
```

Mino TCP listens on `0.0.0.0` and uses `port-base` through `port-base+4`; those
five TCP ports must be allowed between role hosts.

Protobuf+ZeroMQ derives each edge from boot IDs: same-boot edges use
`ipc://.../edge-N.sock`, while cross-boot edges use TCP. Data flows downstream
over `port-base+N`; a strict reverse completion ACK uses `port-base+5+N`, so the
base must be at most 65526. The ACK prevents a process from closing after local
ZeroMQ admission but before downstream delivery.

Mino hybrid uses a separate runner. For the 3+3 topology above it records
`shm, shm, tcp, shm, shm`, creates one SHM segment per host, and launches one
source/sink bridge pair for edge 2:

```sh
bazel-bin/benchmarks/pipeline_comparison/mino_hybrid_runner \
  --topology=/tmp/mino-two-host.json \
  --output-dir=.cache/pipeline-mino-hybrid \
  --profile=small \
  --messages=1000 \
  --warmup-messages=100 \
  --channel-capacity=8 \
  --receive-batch-size=1 \
  --deadline-seconds=120
```

The runner cleans both SHM segments and all local/remote process groups on
success and failure, while retaining local logs, worker results, bridge result
artifacts, and manifest references (including partial failure artifacts). It
always launches formal bridge optimization runs with
`--bridge-validation=structural`; direct bridge diagnosis may instead use `full`
or `full-instrumented`, where the latter records validation calls, payload bytes,
and thread CPU nanoseconds. `--receive-batch-size` is bounded to `[1,64]` and
remains `1` by default for cross-backend fairness; larger values are explicitly
recorded and require an equivalent competitor policy before ranking. Its
qualified default SHM capacity is 8; larger rings increased queue residence
and allocator live-set contention in the current six-stage saturation workload.
All C++ results record Bazel `compilation_mode`; performance comparisons must use
`-c opt` on every host.

For DDS, `data_address` is informational; DDS discovery and interface selection
come from each DDS implementation:

- Fast DDS currently uses default UDP discovery and transport. The hosts must be
  on a network where discovery multicast and RTPS UDP traffic work, or an
  externally supplied Fast DDS profile/discovery setup must provide peers.
- Cyclone DDS uses default SPDP unless each role's topology `environment`
  supplies `CYCLONEDDS_URI`. On multicast-disabled or multi-interface hosts,
  configure an explicit interface and peers there.

Do not compare Fast DDS automatic intrahost SHM/data sharing with Cyclone DDS
UDP as if they were the same transport. For a strict DDS implementation
comparison, qualify and record a common UDP-only configuration first.

### Network preflight

Before a physical run, verify:

1. `ssh -o BatchMode=yes <host> true` succeeds for every `ssh_host`;
2. checkout, binaries, descriptor artifact, and dependency versions match on all
   hosts;
3. `/proc/sys/kernel/random/boot_id` is readable;
4. Mino TCP ports or DDS discovery/data ports are open;
5. MTU, UDP socket buffers, CPU affinity, power policy, and NIC offloads are
   recorded, especially for the 1 MiB profile;
6. output directories are empty and no competing run uses the same DDS domain
   or TCP port range.

A multi-host manifest uses `independent-hosts`, sets
`one_way_latency_valid=false`, and stores `latency_ns=null` in sink metrics.
Without a measured PTP offset/error bound, only reliability and sink throughput
are reportable.

### Saturation versus paced latency

`--*-publish-interval-us=0` is the default saturation mode. It measures
throughput and latency including queue buildup while the source publishes as
fast as possible. Do not present those percentiles as unloaded transport
latency or as a strict equal-window comparison, because the three middleware
buffering models are not equivalent.

For a paced application-style latency comparison, set a profile-specific
interval. The following uses 100 Hz for the small control/guardian shape, 20 Hz
for the medium planning shape, and 10 Hz for the large perception shape:

```sh
bazel-bin/benchmarks/pipeline_comparison/pipeline_comparison_runner \
  --output-dir=.cache/pipeline-comparison-latency \
  --rounds=3 \
  --profiles=small,medium,large \
  --small-messages=1000 \
  --medium-messages=200 \
  --large-messages=100 \
  --small-publish-interval-us=10000 \
  --medium-publish-interval-us=50000 \
  --large-publish-interval-us=100000 \
  --warmup-ratio=0.1 \
  --deadline-seconds=60
```

The manifest labels every profile as `saturation` or `paced_latency`. A paced
run fails instead of silently catching up in a burst if its source falls more
than one full interval behind the absolute schedule. Nearest-rank p99 requires
at least 100 measured samples, and p99.9 requires at least
1,000; smaller campaigns should use p50/p95 only and treat higher percentiles
as smoke indicators.

## Dependency and generation notes

- Fast DDS: `3.4.2.bcr.1` (benchmark-only)
- Fast CDR: `2.3.5.bcr.0`
- Protobuf resolved version: `29.0-rc3`
- ZeroMQ: `4.3.5.bcr.5`
- Fast DDS type support: Fast DDS-Gen 4.2.0-template-compatible,
  `-no-typeobjectsupport`

The Fast DDS-Gen Gradle distribution could not be downloaded in the current
environment. The checked-in support was mechanically reproduced from the 4.2.0
templates and Fast DDS 3.4.2 generated references; it is explicitly marked as
pending a pinned JDK 17 regeneration diff before formal publication.
