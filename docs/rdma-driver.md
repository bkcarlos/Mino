# D6-06 RDMA Driver

## Scope and invariants

`//mino/transport:rdma_driver` implements the complete `TransportDriver` lifecycle:
`Start`, `Connect`, `Listen`, `Accept`, tracked and untracked `Send`, `Poll`,
`PollCompletions`, `AuthenticatedPeer`, `Close`, and `Shutdown`.

The RDMA payload is always one complete Canonical Wire byte sequence. RDMA does
not authorize transmitting `ShmHandle`, a Region offset, a virtual address, a C++
object image, or unused SHM capacity across a Trust Domain. The Bridge still owns
Wire Frame validation, schema negotiation, Topic ACL, Sequence/ACK, dedup, and
retransmit semantics from detailed design sections 16.2–16.6.

A successful send admission means only that the complete message entered a bounded
send queue. A successful verbs completion proves local WR completion; it does not
mean `kRemoteAccepted`. Only a validated Bridge Accepted ACK calls
`ConfirmRemoteAccepted`, and the driver publishes a successful completion only
after both terminal CQ completion and that protocol ACK. Storage stages remain
independent.

## Device provider

`//mino/platform:rdma_provider` is the low-level QP/CQ/MR boundary. The repository
does not link an ambient host `libibverbs` and contains no production software
loopback. Normal builds therefore remain hermetic with respect to rdma-core. A
real deployment must explicitly load an absolute-path plugin through
`CreateDynamicRdmaDeviceProvider`.

The plugin exports the four version-1 symbols documented in
`mino/platform/rdma_provider.h`, reports `MemoryRegistrationProviderClass::kDevice`,
supports `kRdma`, and returns non-empty provenance. Mino rejects a missing plugin,
ABI mismatch, empty provenance, unavailable device, `kUnavailable`, or `kMock`
provider. Because the version-1 plugin returns a C++ provider interface, plugin and
Mino must use the same compiler/standard-library ABI. Qualification records SHA-256
for both artifacts. This is an explicit deployment plugin model, not a claim that
host-installed verbs are a hermetic Bazel dependency.

`RdmaDeviceProvider::Close` is a synchronous DMA fence for the connection. It must
transition the QP to closed/error and guarantee no WR on that QP can still access
memory before returning. `RequestStop` only wakes a blocked poll; it must not tear
down MR before the driver fences QPs.

## MR ownership and cleanup

Each retained send has:

- a stable registration scope and process incarnation;
- a unique WR lease ID (also fencing stale completion identity);
- a bounded byte quota and queue slot;
- one immutable backing vector whose address remains stable while posted;
- incremental byte completion accounting and a terminal state.

Admission copies Canonical Wire into retained staging, reserves quota, registers
that exact extent, and posts the WR. MR failure rolls admission back. Partial CQ
entries advance only the completed prefix. Terminal success must account for the
complete message; over-completion or terminal partial success is corruption.

Backing memory is deregistered only after terminal CQ, synchronous connection
fence, peer-death event, or device reset. Deregistration failure leaves the buffer
and quota quarantined; it is not reused as a new WR. `Close`/`Shutdown` retries
cleanup. Provider shutdown is last. At startup, the driver calls idempotent
`RecoverStale(scope, process_id, process_epoch)` so a provider journal can remove
registrations from crashed process incarnations.

For zero-copy large objects, inject the same real `MemoryRegistrationProvider` into
a `kRdmaRegistered` `LargeObjectPool`. Its generation checks, exact lease
`Pin`/`Unpin`, quota, `ReleaseLease`, and deregister-before-bitmap-reclaim rules are
documented in `docs/large-object-pool.md`. The current generic `TransportDriver`
`Send` contract borrows input only for the call, so production Bridge sends use
registered driver staging; provider-direct pre-registered zero-copy is measured
separately and must not be mislabeled as the generic driver path.

## Bounds, ordering, and failures

`RdmaDriverOptions` independently bounds SQ depth, receive depth, CQ/result depth,
queued send bytes, queued receive bytes, message size, and registration bytes.
Full queues return `kWouldBlock`; quota exhaustion and caller-request bounds return
`kResourceExhausted`. Provider `Poll` receives the remaining capacities and
exceeding any advertised bound is corruption.

Completions may be partial, but tracked operation results are retired in post order
per connection. An ACK received before terminal CQ is remembered but not exposed.
Control and best-effort untracked sends remain bounded. CQ error, peer death, and
device reset fail affected operations at `kLocalPublished`; reset marks the driver
unavailable. Close drops queued receives for that connection and fences/deregisters
its retained sends.

The test-only loopback provider is defined inside `rdma_driver_test.cc`; no mock
provider target is available to production assembly. Fault injection covers MR
failure, CQ error, device reset, and peer death.

## Authentication and ACL

Default mode requires the RDMA CM/device provider to return a complete, verified
`AuthenticatedPeer`. A provider endpoint without verified identity is closed and
returns `kPermissionDenied`.

A physically controlled fabric without CM identity may select
`kControlledFabric` only with a non-null deployment attestor. That verifier binds
local endpoint, peer endpoint, and provider provenance to a complete principal and
fails closed. This is not an ACL bypass: `RemoteBridge::CreateRdma` additionally
requires a real device provider, expected peer Security Domain, identity fence,
and live Coordinator-backed publish + bridge Topic permissions. It reuses the same
D6-11/D6-12 manager checks as TLS.

## Benchmark and qualification

Build the matrix benchmark:

```sh
bazel build --config=release //benchmarks/transport:transport_matrix_benchmark
```

Two processes run `tcp`, `udp`, `rdma`, and `rdma-zero-copy` with the same default
application payload matrix (128 B, 1 KiB, 64 KiB, 1 MiB). Every message is a valid
Canonical Wire Frame. Client JSONL reports payload/wire bytes, process CPU time,
p50/p99 RTT, elapsed time, payload throughput, copy mode, and provider provenance.
`rdma` is the generic driver staging path; `rdma-zero-copy` posts a pre-registered
buffer through the real provider and keeps it registered until terminal CQ.

Mock results are development signals only. D6-06 qualification requires two
physical self-hosted hosts, an ACTIVE/LINKUP RDMA device port and bound kernel
driver on both, verified peer identity, the protected `physical-rdma` environment
variable `RDMA_PROVIDER_SHA256`, exact source commit, identical benchmark binary
hash, identical approved plugin hash, provider ABI v1,
`kDevice` enforcement and non-empty runtime provenance. The role artifacts bind
GitHub run ID/attempt and a shared session nonce to symmetric endpoint, Registry
Node ID and Security Domain identities. Their source check uses
`git status --porcelain=v1 --untracked-files=all`; tracked or untracked dirt and a
non-exact commit both fail closed.

The client must produce exactly one row for every payload in each of `tcp`, `udp`,
`rdma`, and `rdma-zero-copy`. Every row has positive integer p50/p99 RTT, process
CPU, throughput and elapsed metrics plus the promised copy mode. Each metric is
checked independently against the commit-owned
`benchmarks/transport/transport_qualification_sla.json`; one failed or malformed
check fails the complete matrix.

A role manifest is intentionally never qualification-eligible by itself. The
workflow's always-running final job downloads both role artifacts and invokes
`benchmarks/transport/transport_qualification.py`, which re-hashes every log and
JSONL file, re-parses metrics, re-evaluates SLA, and cross-checks commit,
benchmark/plugin hashes, run/session binding and symmetric identities before it
can emit an eligible final manifest. Both role artifacts and the final manifest
use `mino.transport.qualification.v2`, documented by
`docs/validation/Transport_qualification_artifact.schema.json`, and are archived
for 180 days. A hosted runner, software loopback, missing link/provider facts,
missing identity, tampering, or absent result row produces a failed final manifest
rather than a passing placeholder.
