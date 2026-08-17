# Two-host autonomous pipeline capability results — 2026-08-16

## Status

- Physical hosts: **2**.
- Independent role processes: **6**.
- Pipeline hops: **5**, including one physical Ethernet hop.
- Original backends: Mino TCP canonical, Fast DDS, and Cyclone DDS.
- Original capability matrix: **9/9 passed** with zero reported loss,
  duplication, corruption, or out-of-order delivery.
- Cross-host one-way latency: **not reported**. The hosts have independent boot
  clocks and no PTP qualification artifact.
- This was a one-round saturation capability campaign, not a publication-grade
  comparative benchmark.

## Physical topology

```text
Host ubuntu (192.168.31.66)
  Perception -> Prediction -> Planning
                                |
                                | physical 1 GbE-class LAN, MTU 1500
                                v
Host nas (192.168.31.54)
  Control -> Guardian -> CANBus
```

| Host | CPU | Kernel | Logical CPUs | Boot ID | Interface |
|---|---|---|---:|---|---|
| `ubuntu` | Intel Core i9-9900KS | Linux 6.17.0-14 | 16 | `86b8c994-24f9-4f42-97ec-8980354a2e27` | `eno2`, MTU 1500 |
| `nas` | Intel Core i5-11400 | Linux 6.18.18.c877-trim | 12 | `878cd030-c7f4-476a-b79d-0af23bc7b757` | `enp116s0`, MTU 1500 |

ICMP preflight from `ubuntu` to `nas` observed approximately 4.16 ms average
round-trip time over two probes. Processes were not CPU-pinned and neither host
was isolated from other workloads.

The remote host did not have a compiler or Bazel checkout. Benchmark binaries
and the Mino descriptor were deployed to the isolated directory
`/home/bkcarlos/mino-pipeline-agent-20260816`. Because the build host uses a
newer glibc, the deployment includes a private matching loader/runtime under
that directory. It does not replace remote system libraries. Process startup is
outside the measured sink completion window.

## Message and schema contract

All backends transported the complete 18-field `AutonomyPipelineFrame` semantic
message and validated every field and deterministic payload byte at every hop.

- Mino TCP: real `minoc` descriptor artifact, `CanonicalWireCodec`,
  `WireFrameCodec` with payload CRC, and production `TcpDriver`.
- Fast DDS: real typed Fast DDS serialization and data plane; checked-in type
  support remains template-reproduced pending a pinned Fast DDS-Gen diff.
- Cyclone DDS: official build-time `idlc` generated C type and CDR descriptor.

This is not a mock-schema or serialization-bypass test.

A later extension on the same date added:

- Mino hybrid: real generated Mino SHM root/child-slab graphs for same-host
  edges and production canonical `TcpDriver` bridges for cross-host edges.
- Protobuf+ZeroMQ: generated Protobuf messages over `ipc://` for same-host edges
  and `tcp://` for cross-host edges, with a strict reverse completion ACK.

## Final capability matrix

Throughput uses the independent-host sink completion window:
`(received_messages - 1) / (last_completion - first_completion)`. Initialization,
discovery, the start barrier, and the first message's network latency are not in
that window. Values therefore describe sustained sink completion rate for this
single run, not one-way latency and not necessarily physical link line rate.

| Backend | Profile | Payload | Messages | History | Encoded bytes | Sink completion rate |
|---|---|---:|---:|---:|---:|---:|
| Mino TCP canonical | small | 256 B | 1,000 | 64 | 459 B | 28,214.75 msg/s |
| Mino TCP canonical | medium | 64 KiB | 200 | 64 | 65,739 B | 1,389.58 msg/s |
| Mino TCP canonical | large | 1 MiB | 20 | 64 | 1,048,778 B | 88.47 msg/s |
| Fast DDS | small | 256 B | 1,000 | 64 | 384 B | 7,721.08 msg/s |
| Fast DDS | medium | 64 KiB | 200 | 512 | 65,664 B | 72.84 msg/s |
| Fast DDS | large | 1 MiB | 20 | 64 | 1,048,704 B | 10.46 msg/s |
| Cyclone DDS | small | 256 B | 1,000 | 2,048 | 384 B | 51,499.68 msg/s |
| Cyclone DDS | medium | 64 KiB | 200 | 512 | 65,664 B | 137.06 msg/s |
| Cyclone DDS | large | 1 MiB | 20 | 64 | 1,048,704 B | 27.62 msg/s |

The different DDS history values are intentional reliability qualifications,
not a fair equal-resource comparison. Do not rank the DDS implementations from
this table without rerunning paced and saturation campaigns under a common,
qualified resource/transport policy.

## Hybrid extension and performance correction

Both hybrid maps are `SHM/IPC, SHM/IPC, TCP, SHM/IPC, SHM/IPC`. All corrected
runs passed with zero reported loss, duplicate, corruption, or out-of-order
frames.

The initial Mino hybrid smoke was invalid for performance comparison: the new
binaries were `fastbuild`, while the historical comparison used `-c opt`.
Furthermore, synchronous SPSC subscribers unnecessarily attached a
`ShmPinTable`; `perf` attributed 61.38% of sampled cycles to the resulting
per-message `ShmPinTable::PinCount()` scan. SPSC borrows in this benchmark never
escape their synchronous scope, so the extra pin was removed. Post-fix profiling
contains no `PinCount` hotspot. Compilation mode is now compiled into backend
metadata to prevent another mixed-mode comparison.

Corrected Mino comparison using the same current source, `-c opt`, physical
3+3 topology, offered count, and run order within each profile pair:

| Profile | Payload | Messages | Full TCP | SHM/TCP hybrid, SPSC 8 | Hybrid / TCP |
|---|---:|---:|---:|---:|---:|
| small | 256 B | 100,000 | 39,557.43 msg/s | 62,530.89 msg/s | 1.581x |
| medium | 64 KiB | 2,000 | 1,409.45 msg/s | 1,380.37 msg/s | 0.979x |
| large | 1 MiB | 100 | 85.16 msg/s | 87.27 msg/s | 1.025x |

Small now shows the expected SHM benefit. Medium and large are network/copy
bound and effectively near parity in these single runs. The hybrid prototype
still adds two bridge processes and generated graph-to-semantic-to-canonical
and reverse conversions at the physical edge; eliminating those extra boundary
copies requires an integrated six-process hybrid worker or a safe generated
graph ownership-forwarding API.

Protobuf+ZeroMQ capability observations remain:

| Profile | Payload | Messages | HWM | Sink completion rate |
|---|---:|---:|---:|---:|
| small | 256 B | 1,000 | 64 | 21,365.19 msg/s |
| medium | 64 KiB | 200 | 64 | 745.98 msg/s |
| large | 1 MiB | 20 | 64 | 47.61 msg/s |

These are single-round saturation observations, not order-rotated publication
results.

## Reliability findings

Two default-history saturation runs failed and were preserved:

1. Fast DDS medium with history 64: remote `control` expected sample 68 and
   received sample 132. The run passed after increasing history to 512.
2. Cyclone DDS small with history 64: `prediction` expected sample 269 and
   received an older sample 214. The run passed after increasing history to
   2,048.

Cyclone DDS medium passed with history 512. Both large runs passed with history
64 because the campaign contained only 22 total frames including warmup.

These failures show that `RELIABLE + KEEP_ALL` plus a bounded resource limit does
not by itself make a small history window sufficient for every unpaced,
multi-stage network workload. A publication campaign must either qualify a
common in-flight bound or use a paced offered load and record backpressure.

The campaign and hybrid extension also found and fixed these
orchestration/runtime issues:

- Mino TCP's benchmark control send buffer was smaller than its maximum frame
  body and was correctly rejected by `TcpDriver::Create`.
- Failed SSH runs could leave detached remote workers. Remote workers now run in
  recorded process groups; failure cleanup explicitly sends remote TERM/KILL.
  A forced one-second deadline probe confirmed no remote workers or runtime
  directories remained.
- ZeroMQ local send admission was incorrectly treated as downstream delivery.
  With 120 total frames and HWM 64, the preserved failure delivered only 65
  frames per downstream stage. Per-edge reverse completion ACKs now keep every
  upstream context alive until its downstream has consumed all frames.
- The mixed ZeroMQ ACK listener initially inherited the input-edge address;
  planning therefore bound its cross-host ACK to loopback. Address selection now
  considers both data input and reverse ACK input transports.
- The initial Mino SHM consumer manually added a second Pin while a
  `BorrowedMessage` already owned the graph. Saturation exposed reclaim races.
  Consumers now resolve and copy the generated graph under the borrow and ACK
  exactly once after publication.
- Even after removing the duplicate Pin, synchronous SPSC subscribers still
  received the optional pin table. `perf` found the resulting per-message
  `PinCount()` scan at 61.38% of cycles. Since these borrows never escape their
  synchronous scope, subscribers now omit optional pin registration; the
  post-fix profile has no `PinCount` hotspot.

## Evidence

Successful final manifests:

| Run | Artifact | SHA-256 |
|---|---|---|
| Mino small | `.cache/pipeline-network-final-mino-small-20260816/manifest.json` | `6e593a391110f19d24a3cad918c122ca6e6f3447275969c5cc2f69700ade3bea` |
| Mino medium | `.cache/pipeline-network-final-mino-medium-20260816/manifest.json` | `78bc7ec737c7d367687e973e7afa49f3cc6f18b2825fb506906f8a89c604712a` |
| Mino large | `.cache/pipeline-network-final-mino-large-20260816/manifest.json` | `39f6e6a0c3035cce0b6211ca6636e9967050879ad6ac36b22603ca3ad6754687` |
| Fast DDS small | `.cache/pipeline-network-final-fastdds-small-20260816/manifest.json` | `3ca51c71a93362b6927b6beb1a023fb4610e95c70dd38f7b7b5dc87bf9ef7e48` |
| Fast DDS medium | `.cache/pipeline-network-final-fastdds-medium-20260816/manifest.json` | `52ed8da47fa4703aaef6a04f974a8d4e33c2bb18f334854063c72d9d66c2782b` |
| Fast DDS large | `.cache/pipeline-network-final-fastdds-large-20260816/manifest.json` | `1ee5a5d87c72cba2a65fa1760f1643ee6908194d400d07ba011a2eb640388702` |
| Cyclone DDS small | `.cache/pipeline-network-final-cyclonedds-small-20260816/manifest.json` | `66bc89220607e9845b8904f85c99edbc0d1a6144d806e58a056f870e703db203` |
| Cyclone DDS medium | `.cache/pipeline-network-final-cyclonedds-medium-20260816/manifest.json` | `00856adb7b559f8959ec1b875545171212a354157ecb8281d570ece8ab0747c9` |
| Cyclone DDS large | `.cache/pipeline-network-final-cyclonedds-large-20260816/manifest.json` | `9a5e56312f8e53d16ada4be8fec33f5a00c3b39c7298fa7ddc2e9a7a99476b7b` |
| Corrected Mino TCP small | `.cache/pipeline-tcp-opt-two-host-100k-20260816/manifest.json` | `e3a30b955543337b9e792ed87bb52c853b7d9cf81c7acbfb3e26136b58d89da1` |
| Corrected Mino hybrid small | `.cache/pipeline-hybrid-fixed-opt-small-100k-20260816/manifest.json` | `ad854cb4a713f0e99186beb4f5f99e0387b4eb5ba8df211af803ee19760d0bdf` |
| Corrected Mino TCP medium | `.cache/pipeline-tcp-opt-two-host-medium-2k-20260816/manifest.json` | `5bb6f666a25a71a22f0dc668883e37c8db02ab8d9ae32094ea39bf69fef14562` |
| Corrected Mino hybrid medium | `.cache/pipeline-hybrid-fixed-opt-medium-2k-20260816/manifest.json` | `4361ea6d83e9e112566588634b3d0bfe7ab5ee37bc5ddc14b69647f3ae5f07e8` |
| Corrected Mino TCP large | `.cache/pipeline-tcp-opt-two-host-large-100-20260816/manifest.json` | `b62fffa4be721f89d0ef46c7dbbb7a01f50242163541237743e8926504d4c512` |
| Corrected Mino hybrid large | `.cache/pipeline-hybrid-fixed-opt-large-100-20260816/manifest.json` | `55200cad3068014c7ef556167b393c388a2597d7e3d204518bf6643a614d6ed7` |
| Final deployed metadata smoke | `.cache/pipeline-hybrid-final-two-host-metadata-smoke-20260816/manifest.json` | `bf3e14a10c6aeada03396f4424ea434e685e251d5d7f47b640aed5a02c1d22a0` |
| Protobuf+ZMQ small | `.cache/pipeline-network-protobuf-zmq-small-matrix-smoke-20260816/manifest.json` | `dcd3168976c74d13b35b328b3750536bac74d0cf406622427ff9296a99f218e0` |
| Protobuf+ZMQ medium | `.cache/pipeline-network-protobuf-zmq-medium-matrix-smoke-20260816/manifest.json` | `e875206b877c315f4690ed5bd48e5472dc4f3b2079afee94b3802df4bb7bd1f9` |
| Protobuf+ZMQ large | `.cache/pipeline-network-protobuf-zmq-large-matrix-smoke-20260816/manifest.json` | `fb890cef93ee32136c25db240c35c96f84349d7cdff8727d3bbf6571df05960b` |

Preserved failure manifests:

| Finding | Artifact | SHA-256 |
|---|---|---|
| Fast DDS medium, history 64 | `.cache/pipeline-network-capacity-fastdds-medium-20260816/manifest.json` | `0dcd3d9a6d6005e6ef8aceefb4d0aebca518f905c6e9a9d44bf4000c84bdb5b0` |
| Cyclone DDS small, history 64 | `.cache/pipeline-network-capacity-cyclonedds-small-20260816/manifest.json` | `80eb750e99cf1d7e6e9b366c13c6f6d3282a25766eb5fe89643d15bebd443d29` |
| ZMQ before completion ACK | `.cache/pipeline-network-protobuf-zmq-small-smoke-20260816/manifest.json` | `cad299e23d9decd11ceb13d64e020a7c147a10c6eaf1a736d6cd521460aab723` |
| ZMQ ACK bound to loopback | `.cache/pipeline-network-protobuf-zmq-small-ack-smoke-20260816/manifest.json` | `8c67cafd4e93c1eca43e3ce976ad51cfa22b889a9e308dc6221425c26b7290a0` |
| Mino SHM double-Pin race | `.cache/pipeline-hybrid-two-host-small-final-smoke-20260816/manifest.json` | `a483a0539194269bfd8566a89108e737378732f729d148c97bdc2ba2f77ccb1c` |
| Mino hybrid mixed compilation/pin-overhead result | `.cache/pipeline-hybrid-two-host-small-borrow-smoke-20260816/manifest.json` | `6d56a2a84b351ad7a7d77bf3d6553ebf563ccdd5109ff25d680c97453d353c96` |

Every manifest stores all six commands, host boot IDs, result/log hashes,
backend QoS, encoded size, counts, and validation errors. The post-fix CPU
profile is `.cache/perf-shm-fixed.data` with SHA-256
`65a4e7ac11062c8fefba0f0115e2ec6c2f9b06b2302993cc23388ff38078cb91`.

## Limitations and next qualification step

- One round per profile is insufficient for performance publication.
- DDS transport policy was each implementation's current default, not a common
  UDP-only policy. Fast DDS may use intrahost SHM/data sharing for local hops;
  Cyclone DDS reports default UDP with no PSMX target.
- DDS history differs where required to obtain a reliable saturation run.
- There was one physical Ethernet hop, not six roles distributed across six
  physical hosts.
- No PTP offset/error bound was collected; cross-host one-way latency remains
  invalid by design.
- CPU affinity, IRQ affinity, NIC offloads, socket buffers, and competing host
  load were not controlled.

A formal next campaign should use at least three rotated rounds, a common
qualified DDS UDP policy, both paced and saturation modes, fixed CPU/IRQ
placement, and a recorded network/clock preflight artifact.
