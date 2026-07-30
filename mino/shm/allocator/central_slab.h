// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.gnu.org/licenses/lgpl-3.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// Central Slab Allocator (design doc section 8).
//
// The allocator lives in a shared-memory Region and is organized as
// (design doc 8.1):
//
//   Allocator Metadata
//   ├── AllocatorSuperblock (fixed 128 B)
//   ├── Class Descriptor[0..N)
//   ├── Sharded Allocation Bitmap
//   ├── Generation Array
//   └── (Recovery Metadata / Metrics Counters are managed elsewhere)
//
//   Data Region
//   └── per slot: SlabHeader (64 B) + payload (class slot_size)

#ifndef MINO_SHM_ALLOCATOR_CENTRAL_SLAB_H_
#define MINO_SHM_ALLOCATOR_CENTRAL_SLAB_H_

#include <atomic>
#include <cstdint>

#include "mino/abi/shm_handle.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/allocator/bitmap.h"
#include "mino/shm/allocator/class_table.h"
#include "mino/shm/allocator/generation_array.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino {



// Schema identity carried by an AllocationRequest. Only the short id is
// stored in the Slab Header; the full digest is resolved via the Registry
// (design doc 8.1).
struct SchemaIdentity {
    uint64_t short_id = 0;
    uint32_t layout_version = 0;

    friend bool operator==(const SchemaIdentity&, const SchemaIdentity&) = default;
};

// AllocationRequest (design doc 8.2).
struct AllocationRequest {
    uint32_t object_size = 0;
    TypeId type_id;
    SchemaIdentity schema;
    uint32_t alignment = 1;
};

// SlabView is a read-only snapshot of one slot returned by Inspect().
// `data` points into shared memory and remains valid only while the slot is
// not reclaimed; callers must not retain it across lifecycle transitions.
struct SlabView {
    ShmHandle handle;
    ObjectState state = ObjectState::kFree;
    uint16_t class_id = 0;
    uint32_t generation = 0;   // Authoritative generation from the array.
    uint32_t capacity = 0;     // Slot payload capacity in bytes.
    uint32_t object_size = 0;  // Size requested at allocation time.
    TypeId type_id;
    uint64_t schema_short_id = 0;
    uint32_t layout_version = 0;
    const void* data = nullptr;  // Slot payload address in this process.
};

// MutableBuildView is the allocator-owned exclusive write window returned by
// BeginBuild(). It is valid only while the object remains kBuilding and must
// not be retained after Publish(), Abort(), Retire(), or Reclaim().
struct MutableBuildView {
    ShmHandle handle;
    uint32_t capacity = 0;
    uint32_t object_size = 0;
    TypeId type_id;
    uint64_t schema_short_id = 0;
    uint32_t layout_version = 0;
    void* data = nullptr;
};

// CentralSlabAllocator allocates fixed-size-class slots from a shared-memory
// Region following the strict 9-step protocol of design doc 8.3.
//
// The object itself is a process-local facade: all mutable state lives in
// shared memory so that Create() (first process) and Attach() (subsequent
// processes) yield equivalent allocators.
class CentralSlabAllocator {
public:
    // Initializes a fresh allocator in the shared memory at `shm_base`.
    // `data_region_size` is the total size of the Region in bytes and must
    // cover allocator metadata plus all configured slots. The memory must be
    // zero-initialized.
    static Result<CentralSlabAllocator> Create(void* shm_base,
                                               uint64_t data_region_size,
                                               const ClassTableConfig& config);

    // Attaches to an existing allocator previously initialized with Create().
    // Validates the superblock magic, metadata version and generation array
    // consistency.
    static Result<CentralSlabAllocator> Attach(void* shm_base);

    // Allocates one object following design doc 8.3 steps 1-9:
    //   1. checked-align the request size;
    //   2. select the smallest fitting class;
    //   3. select a bitmap shard;
    //   4. find a free bit;
    //   5. CAS-claim the bit;
    //   6. increment the authoritative generation, copy it into the header,
    //      and mark the class DRAINING (refusing the wrap) at UINT32_MAX;
    //   7. fill in Owner Epoch, Transaction ID, Schema/Layout and the
    //      remaining header fields;
    //   8. publish with object_state.store(kAllocated, release) — the single
    //      publication point;
    //   9. return the Handle.
    //
    // Crash-recovery convention (design doc 8.3): a slot whose bitmap bit is
    // set but whose object_state is not a legally published state never had
    // its generation exposed; recovery may safely clear the bit.
    Result<ShmHandle> Allocate(const AllocationRequest& request);

    // Claims exclusive construction ownership and transitions an object from
    // kAllocated to kBuilding. The returned writable view is process-local;
    // only the Handle and object bytes live in shared memory.
    Result<MutableBuildView> BeginBuild(ShmHandle handle);

    // Publishes a completely built object. Release ordering guarantees that a
    // reader which observes kPublished with acquire ordering sees all payload
    // writes performed through the MutableBuildView.
    Status Publish(ShmHandle handle);

    // Aborts an unpublished allocation (kAllocated or kBuilding), transitions
    // it through kAborting, and returns the slot to the allocator. Published
    // objects must instead follow Retire() -> Reclaim().
    Status Abort(ShmHandle handle);

    // Marks the slot Retired: no new Borrow will be produced, but existing
    // Borrows stay valid (design doc 8.4). Fails with kNotFound if the
    // handle is stale (generation mismatch) and with kInvalidArgument for a
    // null/foreign-region handle.
    Status Retire(ShmHandle handle);

    // Reclaims the slot: clears the allocation bitmap so the slot becomes
    // reusable (design doc 8.4). Per the D1 scope the caller asserts that no
    // valid Borrow and no live Pin exist; this is enforced once the Borrow /
    // Pin registry (design doc 11) lands. Requires the slot to be in
    // kRetired (or an unpublished/intermediate state for recovery-driven
    // reclaim).
    Status Reclaim(ShmHandle handle);

    // Returns a read-only view of the slot addressed by `handle`
    // (design doc 8.2). Validates region id, bounds, bitmap occupancy,
    // generation and header CRC.
    Result<SlabView> Inspect(ShmHandle handle) const;

    // Total number of slots managed by this allocator.
    uint32_t total_slot_count() const { return class_table_.total_slot_count(); }

    uint16_t class_count() const { return class_table_.class_count(); }

    // Exposed for recovery tooling: inspect a slot by its global slot index
    // without handle validation. `header_out` may be null.
    bool ReadSlotByIndex(uint32_t slot_index, SlabHeader* header_out,
                         const void** data_out) const;

    // Default-constructible so the facade can be stored by value (e.g. in
    // test fixtures); Create()/Attach() are the only supported ways to
    // build a usable allocator.
    CentralSlabAllocator() = default;

private:

    // Computes all offsets/sizes of the shared layout for `config` and
    // returns the metadata struct (host-side). Used by Create/Attach.
    struct Layout;

    static Result<Layout> ComputeLayout(const ClassTable& table,
                                        uint64_t data_region_size);

    // Resolves a handle to (slot_index, header, payload) with full
    // validation. `require_live` additionally demands the bitmap bit set and
    // generation match.
    Result<uint32_t> ResolveLocked(ShmHandle handle) const;
    SlabHeader& HeaderAt(uint32_t slot_index);
    const SlabHeader& HeaderAt(uint32_t slot_index) const;

    // Shared-memory state (pointers into the Region).
    void* shm_base_ = nullptr;
    ClassTable class_table_;
    ShardedBitmap bitmap_;
    GenerationArray generations_;

    // Per-slot headers and payload base, inside the data region.
    SlabHeader* headers_ = nullptr;
    std::atomic<uint32_t>* class_draining_ = nullptr;  // one per class
    uint64_t data_region_size_ = 0;
    uint32_t region_id_ = 0;
    uint64_t slot_stride_ = 0;  // sizeof(SlabHeader) + max slot payload align
};

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_CENTRAL_SLAB_H_
