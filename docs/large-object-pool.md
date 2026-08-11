# D6-08 large-object specialized pools

`//mino/shm/allocator:large_object_pool` provides isolated extents for ordinary,
HugePage, DMA, and RDMA-registered buffers. It does not implement an RDMA or DMA
driver. Device integration is supplied through
`//mino/platform:memory_registration`.

## Isolation and allocation contract

Each pool has one persisted `LargeObjectPoolPurpose`:

- `kNormal`: ordinary large objects; the source-compatible `Allocate(size, type)`
  API is accepted only here.
- `kHugePage`: mappings that requested HugePages. Actual backing and fallback are
  persisted and observable.
- `kDma`: buffers registered by a DMA provider.
- `kRdmaRegistered`: buffers registered by an RDMA provider.

Specialized pools require `LargeObjectAllocationRequest`. The request explicitly
states object size/type, pool purpose, power-of-two alignment, virtual or physical
contiguity, none/DMA/RDMA registration, allocation or lease lifetime, and (for
lease lifetime) owner identity.
A request whose purpose differs from the pool is rejected. DMA/RDMA pools also
reject objects below `minimum_registered_object_bytes`; ordinary small objects
must use a normal slab/pool and cannot consume registration quota.

The payload array is virtually contiguous. Physical contiguity is never inferred
from virtual addresses or HugePage backing: a physical-contiguous request succeeds
only if the injected provider confirms `RegisteredMemory::physically_contiguous`.

## HugePage truth and strict mode

Build `LargeObjectPoolOptions::huge_pages` from the authoritative
`SharedMemorySegment` observations:

```cpp
LargeObjectPoolOptions options{
    .purpose = LargeObjectPoolPurpose::kHugePage,
    .huge_pages = {
        .requested = segment.huge_pages_requested(),
        .actual = segment.huge_pages_actual(),
        .strict = true,
        .actual_page_size = segment.actual_page_size(),
        .fallback_reason = segment.huge_page_fallback_reason(),
        .fallback_errno = segment.huge_page_fallback_errno(),
    },
};
```

Fallback mode creates the dedicated pool on ordinary backing and increments
`huge_page_fallback_allocations` for each allocation. Strict mode rejects Create
or Attach when HugePages were requested but the authoritative mapping fell back.
`huge_pages_actual()` is the backing fact; a request/configuration flag is not
success evidence.

## Registration provider and failure safety

`MemoryRegistrationProvider` is injectable and has Register, Deregister, and
idempotent `RecoverStale` operations. The built-in provider is deliberately
unavailable and always returns `kUnsupported`; Mino does not fake registration on
a machine without a device provider. Tests and non-qualifying benchmarks inject a
provider whose class is explicitly `kMock`. Production providers report `kDevice`.

A registered pool requires:

- a stable, deployment-unique `registration_scope_id`;
- process id, process epoch, and default lease id;
- a bounded byte quota and minimum object size;
- a provider that supports the pool's DMA/RDMA registration kind.

Allocation reserves quota, claims an aligned extent, advances per-segment
generations, writes the normal SHM headers, and then calls Register. Registration
failure rolls the extent back. Reclaim calls Deregister before clearing any bitmap
bit; an active Pin returns `kWouldBlock`, and a deregistration failure leaves the
extent occupied so the device cannot access reused memory.

`Pin`/`Unpin` require the exact lease owner. `ReleaseLease` forcibly deregisters
all local registrations for that lease but does not reclaim SHM objects; normal
Retire/Reclaim or region recovery still owns object lifecycle.

### Crash recovery

Registration records contain no process pointers and are not added to the SHM ABI.
The device provider owns its recoverable registration journal keyed by stable scope
and process incarnation. Specialized Attach must pass the new process epoch and
leave `recover_stale_registrations=true`; `RecoverStale` removes records belonging
to older epochs before SHM recovery can reuse extents. Attach without a provider
may inspect a registered pool, but destructive recovery fails closed with
`kUnavailable`.

The existing Region recovery scanner remains authoritative for bitmap/header
repair. Generation checks reject stale handles (ABA), reclaim is idempotent at the
segment protocol boundary, double Reclaim returns `kNotFound`, and freed adjacent
extents coalesce naturally in the bitmap.

## Fragmentation and metrics

Allocation uses an alignment-aware best-fit extent search rather than first-fit.
`LargeObjectPool::metrics()` reports an approximate concurrent snapshot:

- capacity, requested object bytes, reserved extent bytes, free bytes;
- internal fragmentation, largest free extent, external fragmentation;
- allocation failures and HugePage fallback allocations;
- current registration bytes, registration failures, recovered registrations,
  and recovered registration bytes.

NUMA placement and local/remote/fallback accounting reuse D6-02
`NumaPlacementConfig` and `ApplyNumaPlacement`; no second NUMA policy is persisted.

## Benchmark matrix

The development smoke remains available without privileged hardware:

```sh
bazel run //benchmarks/allocator:large_object_pool_benchmark -- \
  --iterations=1000 --json=/tmp/large-object-pool.json
```

It runs 64 KiB, 256 KiB, and 1 MiB objects in `short` and `batch` usage patterns.
The complete physical matrix compares `ordinary`, `hugepage`, and
`device-registration`, in that order. The lifecycle labels intentionally describe
application usage rather than pretending all pools have the same registration
contract:

- ordinary and HugePage `batch` rows use allocation-lifetime objects allocated and
  reclaimed in bounded batches;
- device-registration `batch` rows use `LargeObjectLifetime::kLease`, an exact
  owner, and `ReleaseLease` before Retire/Reclaim;
- every row records `registration_lifetime` as `allocation` or `lease` so reports
  cannot conflate the two.

Without `--plugin`/`--device`, the benchmark injects its explicit `kMock` provider
only as a development baseline. It emits `status=SKIPPED` and
`qualification_eligible=false`; fallback HugePages, unavailable NUMA, or failed
`mlock` are reported as observed rather than hidden.

## Dynamic device provider

A physical run passes an absolute RDMA provider plugin and device name:

```sh
bazel-bin/benchmarks/allocator/large_object_pool_benchmark \
  --iterations=10000 \
  --json=/tmp/large-object-pool.json \
  --plugin=/opt/mino/providers/rdma-provider.so \
  --device=mlx5_0 \
  --hugetlbfs-path=/mnt/mino-hugetlb \
  --numa-node=0 \
  --qualification-attestation=physical-hugepage-device
```

The benchmark uses `CreateDynamicRdmaDeviceProvider`; the returned
`RdmaDeviceProvider` is also the allocator's `MemoryRegistrationProvider`. The
production loader requires ABI v1 exports, non-empty provenance,
`provider_class()==kDevice`, and RDMA registration support. It never searches host
library paths or substitutes a software/mock provider. Qualification hashes and
approves the plugin before the benchmark is allowed to load it.

A counting wrapper delegates every real Register/Deregister/RecoverStale call and
records errors without changing provider class. The device pool is placed on the
requested NUMA node in strict mode and its full 64 MiB extent must be successfully
`mlock`ed before measured registration. The JSON binds provider name/provenance,
device, NUMA topology/node, actual lock bytes, registration calls/errors, and
HugePage backing facts.

## D6-08 physical qualification

Use the manual self-hosted workflow
`.github/workflows/large-object-pool-qualification.yml`. The runner must be labeled
`mino-large-object-pool` and be provisioned before the job with:

- a clean exact checked-out commit;
- Linux x86-64, the configured RDMA device and an ACTIVE/LinkUp physical port;
- an approved provider SHA-256 in environment variable `RDMA_PROVIDER_SHA256`;
- a hugetlbfs mount with at least 64 MiB of free reserved HugePages;
- `RLIMIT_MEMLOCK` of at least 64 MiB;
- a present NUMA node equal to the device's authoritative sysfs `numa_node`.

The workflow does not reserve pages, raise memlock, fake a link, or install a
provider. `benchmarks/allocator/large_object_pool_qualification.py` verifies these
facts fail closed, snapshots preflight evidence, and only then executes the plugin.
A missing device/local developer host therefore produces a failed or skipped,
non-qualified result; it can never satisfy D6-08.

The runner enforces the versioned policy in
`benchmarks/allocator/large_object_pool_qualification_sla.json` over the exact
3 × 3 × 2 matrix. Independent checks require minimum throughput, maximum p99,
zero final internal/external fragmentation, zero HugePage fallback, zero operation
and registration failures, zero Deregister errors, full extent coalescing after
each row, and a quota probe that rejects over-quota allocation without invoking the
provider or leaking registration bytes.

Qualification output is governed by
`docs/validation/Large_object_pool_qualification_artifact.schema.json`. The output
directory contains:

- `preflight.json` with source, HugePage pool, memlock, physical device/link, NUMA,
  and approved-plugin evidence;
- `benchmark.json` and `benchmark.log`;
- immutable copies of the SLA policy and artifact schema;
- `manifest.json`, which records SHA-256 and byte length for all five artifacts,
  hashes the benchmark/plugin/policy/schema inputs, includes every SLA outcome, and
  sets `outcome`, `artifacts_complete`, and `qualification_eligible`.

`qualification_eligible=true` is possible only when the exact commit is clean, all
physical preflight evidence passes, the benchmark itself reports actual HugePages,
a `kDevice` provider, successful 64 MiB memory lock and strict NUMA provenance, all
SLA checks pass, all five artifact hashes verify, and the attestation is exactly
`physical-hugepage-device`.

Contract downgrade/tamper coverage is in
`//benchmarks/allocator:large_object_pool_qualification_contract_test`. It rejects
mock/SKIPPED reports, missing device/link evidence, dirty or untracked source,
HugePage/fallback and NUMA mutations, matrix removal, independent SLA failures,
unapproved plugin hashes, and post-manifest artifact modification.
