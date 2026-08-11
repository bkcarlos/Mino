# FabricWindowDriver（IPCF / PCIe NTB / CXL）

`//mino/transport:fabric_driver` implements ADR-0012 and detailed design 15.3/16.7
without changing Bridge reliability or `DeliveryStage` semantics. IPCF, NTB and
CXL are provider capabilities, not alternate application protocols.

## Trust boundary and lifecycle

`FabricWindowDriver` implements the complete `TransportDriver` lifecycle:
`Start`, `Connect`, `Listen`, `Accept`, `Send`, `SendUntracked`, `Poll`,
`PollCompletions`, `AuthenticatedPeer`, `Close`, and `Shutdown`. The base class
continues to fence concurrent calls during stop.

Every cross-Trust-Domain window payload is exactly one validated Canonical Wire
frame body (detailed design 16.2). The driver decodes it before admission and
again before receive publication. A `ShmHandle`, Region offset, process virtual
address, C++ object image, or provider-native descriptor is never a Fabric
payload or provider receive type. Same-Security-Domain peers are rejected and
must use local SHM.

A provider connection is admitted only after a deployment-controlled attestor
binds all of:

- local and peer Registry Node ID and Security Domain;
- local/peer endpoint, IPCF/NTB/CXL kind, provider provenance;
- local and peer device identity;
- window-set ID, window generation, session epoch;
- non-empty provider/device attestation evidence.

The returned `AuthenticatedPeer` must exactly match the provider peer facts.
`RemoteBridge::CreateFabric` additionally requires a real `kDevice` provider,
an expected peer Security Domain, a live Coordinator, Registry identity fencing,
and the normal publish + bridge Topic ACL. Missing evidence fails closed.

## Window ring protocol v1

Each slot starts with a 128-byte, padding-free logical header encoded explicitly
in big-endian order. Native structs are never copied into the window.

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | magic `MFW1` |
| 4 | 2 | protocol version (`1`) |
| 6 | 2 | header bytes (`128`) |
| 8 | 4 | endianness marker (`0x01020304`) |
| 12 | 4 | flags (`Canonical Wire`) |
| 16 | 8 | provider window ID |
| 24 | 8 | window generation |
| 32 | 8 | session epoch |
| 40 | 8 | producer sequence |
| 48 | 8 | consumer sequence snapshot |
| 56 | 4 | payload bytes |
| 60 | 2 | provider kind (IPCF/NTB/CXL) |
| 62 | 2 | reserved zero |
| 64 | 4 | payload CRC32C |
| 68 | 4 | header CRC32C, calculated with this field and commit marker zero |
| 72 | 8 | generation/session/sequence-bound commit marker |
| 80 | 48 | reserved zero |
| 128 | N | Canonical Wire frame body |

Producer publication is ordered as follows:

1. validate bounds and pointer alignment against provider capabilities;
2. write the zeroed header, payload, generation/session/sequence and CRC32C;
3. leave the commit marker zero and perform `MaintainCache(kForPeerRead)` for the
   complete record;
4. execute a release fence;
5. write the bound commit marker, clean the cache-line range covering that marker,
   execute another release fence, then ring the versioned, endian-marked
   `kProducerCommit` mailbox doorbell.

Consumer processing invalidates/synchronizes the provider range first and then
executes an acquire fence. It validates slot bounds/alignment, magic, version,
endianness, provider kind, window ID, generation, session, producer sequence,
commit marker, header CRC, payload CRC, payload bound and Canonical Wire. Only
then is an owned message published upward. A release fence precedes
`ReleaseReceiveWindow` and the versioned, endian-marked `kConsumerRelease`
mailbox doorbell.

The window ring, receive queue, event poll, completion queue, receive bytes and
base outstanding-send table are all bounded. A full ring returns `kWouldBlock`.
No unbounded retry buffer is hidden in the driver.

## Completion and reliable Bridge ACK

A producer commit proves only `kLocalPublished`. A consumer-release doorbell
reclaims a physical slot; it does **not** prove Bridge acceptance. Reliable
`kRemoteAccepted` completion is emitted only after the Bridge validates the
16.5 Accepted ACK and calls `ConfirmRemoteAccepted` for the exact operation.
Completions retire in producer order per connection. Control/data retransmission,
sequence/session ACK, deduplication and reconnect policy remain owned by Bridge.

## Reset, crash, and corruption

`Close` is the provider's synchronous interrupt/device-access fence. Peer reset,
link loss, window-generation change, or session-epoch change closes and removes
the connection, fails outstanding operations, drops queued receive data, and
invalidates every old window reference. Old IDs are never accepted into a new
generation.

Missing commit markers, CRC failure, malformed metadata, producer sequence gaps,
oversized provider results, and receive cache-sync failure isolate the channel as
corruption/degraded health. A transmit cache-sync or doorbell failure aborts the
lease and fails admission. Doorbell loss remains bounded by the occupied slot and
Bridge reconnect/retry policy; the driver never reports remote acceptance.

The test-only provider in `fabric_driver_test.cc` covers payload corruption,
partial commit, doorbell loss/backpressure, peer reset/generation change,
transmit and receive cache-maintenance failure, doorbell failure, ACK separation,
and rejection of non-Canonical bytes.

## Provider plugin ABI

Production assembly loads an explicitly configured absolute path using
`CreateDynamicFabricDeviceProvider`. No host search path or software fallback is
used. ABI v1 exports:

```text
mino_fabric_provider_abi_version_v1
mino_create_fabric_provider_v1
mino_destroy_fabric_provider_v1
mino_fabric_provider_provenance_v1
```

The provider must report `kDevice`, the configured IPCF/NTB/CXL kind, a non-empty
device ID/provenance, power-of-two cache line/alignment, finite window limits,
`device_present=true`, and `link_active=true`. A normal repository build contains
no selectable mock provider; mocks are defined only in test source.

## Benchmark and qualification

`//benchmarks/transport:transport_matrix_benchmark` runs TCP, RDMA and Fabric
over the same Canonical Wire payload matrix and records payload/wire bytes, CPU,
RTT, throughput, copy mode, and provider provenance. Fabric mode requires an
absolute plugin path, device/kind/domain/channel and explicit local/peer
Node/Security Domain values.

`.github/workflows/fabric-qualification.yml` runs only on two labeled self-hosted
physical hosts. D6-07 is one complete promise covering IPCF, NTB, and CXL: the
workflow and runner always execute the ordered `ipcf,ntb,cxl` matrix using three
separate channels and emit a separate final result for each kind. Selecting or
presenting one successful kind cannot substitute for either missing kind.

Each host validates an actual `/sys/devices` directory, sysfs class/subsystem, a
bound kernel driver and an active sysfs link-state file. The approved hash comes
from the protected `physical-fabric` environment variable
`FABRIC_PROVIDER_SHA256`; the plugin must have that SHA-256 on both hosts, export
ABI v1 provenance, and survive the
production loader's real `kDevice` and exact-kind capability checks. Role
artifacts bind the exact clean commit (including an empty untracked-file status),
benchmark/plugin hashes, run ID/attempt/session nonce and symmetric endpoint,
Registry Node and Security Domain identities.

The client must produce the complete payload matrix for TCP and each Fabric kind.
Positive integer p50/p99 RTT, process CPU, throughput and elapsed metrics and the
exact copy mode are independently evaluated against
`benchmarks/transport/transport_qualification_sla.json`. The always-running final
job downloads both role artifacts, re-hashes and re-parses every artifact,
re-evaluates every SLA check and writes a fail-closed final manifest. A unilateral
role artifact is never qualification-eligible. The shared role/final schema is
`mino.transport.qualification.v2`; its JSON Schema is
`docs/validation/Transport_qualification_artifact.schema.json`. Both endpoint
artifacts and the final archive are retained for 180 days. Emulator/mock,
single-kind, dirty, asymmetric, incomplete or tampered evidence cannot satisfy
physical qualification.
