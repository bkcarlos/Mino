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
#include <span>

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
inline constexpr uint32_t kAllocationFlagTransactionRoot = 1u << 0;
inline constexpr uint32_t kAllocationFlagTransactionChild = 1u << 1;
inline constexpr uint32_t kAllocationFlagMask =
    kAllocationFlagTransactionRoot | kAllocationFlagTransactionChild;

struct AllocationRequest {
    uint32_t object_size = 0;
    TypeId type_id;
    SchemaIdentity schema;
    uint32_t alignment = 1;
    uint64_t owner_epoch = 0;
    uint64_t allocation_transaction_id = 0;
    uint32_t allocation_flags = 0;
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
    uint64_t owner_epoch = 0;
    uint64_t allocation_transaction_id = 0;
    uint32_t allocation_flags = 0;
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

// Describes the allocator and data sub-regions of a real SharedMemoryRegion.
// Keeping this type in the allocator package avoids a dependency from the
// allocator onto the Region implementation while still allowing both layers to
// share one concrete metadata layout.
struct RegionAllocatorStorage {
    void* region_base = nullptr;
    uint64_t region_size = 0;
    uint64_t allocator_offset = 0;
    uint64_t allocator_size = 0;
    uint64_t data_offset = 0;
    uint64_t data_size = 0;
    uint32_t region_id = 0;
};

// Authoritative allocator facts for one Region-relative slot-header offset.
// This deliberately exposes values rather than shared-memory pointers so
// consumers cannot mutate allocation metadata or depend on its layout.
struct CentralSlabSlotMetadata {
    bool occupied = false;
    uint32_t generation = 0;
    uint16_t class_id = 0;
    uint16_t class_count = 0;
    uint32_t capacity = 0;
};

// CentralSlabAllocator allocates fixed-size-class slots from a shared-memory
// Region following the strict 9-step protocol of design doc 8.3.
//
// The object itself is a process-local facade: all mutable state lives in
// shared memory so that Create() (first process) and Attach() (subsequent
// processes) yield equivalent allocators.
class CentralSlabAllocator {
public:
    static constexpr uint64_t kMetadataHeaderSize = 64;
    static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
                  "CentralSlabAllocator requires lock-free 64-bit atomics");
    // Initializes a fresh allocator in the shared memory at `shm_base`.
    // `data_region_size` is the total size of the Region in bytes and must
    // cover allocator metadata plus all configured slots. The memory must be
    // zero-initialized.
    static Result<CentralSlabAllocator> Create(void* shm_base,
                                               uint64_t data_region_size,
                                               const ClassTableConfig& config);

    // Initializes allocator metadata in the Region allocator sub-region and
    // places all SlabHeader/payload slots in the Region data sub-region.
    // Returned Handles are relative to region_base and carry region_id.
    static Result<CentralSlabAllocator> CreateInRegion(
        const RegionAllocatorStorage& storage,
        const ClassTableConfig& config);

    // Attaches to an existing allocator previously initialized with Create().
    // Validates the superblock magic, metadata version and generation array
    // consistency.
    static Result<CentralSlabAllocator> Attach(void* shm_base);

    // Bounds-checked attach used by recovery. Unlike the legacy overload this
    // never trusts allocator metadata before proving that all metadata and slot
    // extents fit inside available_size.
    static Result<CentralSlabAllocator> Attach(void* shm_base,
                                               uint64_t available_size);

    // Region-aware attach. Restores Region-relative Handle offsets while using
    // the same persisted allocator metadata as Attach().
    static Result<CentralSlabAllocator> AttachInRegion(
        const RegionAllocatorStorage& storage);

    // Probes the allocator sub-region without duplicating allocator magic in
    // Region/Recovery. A zero first word means "not initialized"; a non-zero,
    // non-allocator word is reported as corruption.
    static Result<bool> HasAllocatorMetadata(const void* shm_base,
                                             uint64_t available_size);

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

    // Returns a globally unique, non-zero transaction id for this allocator.
    // The id is persisted in the allocator superblock and is suitable for both
    // AllocationJournal tagged control words and SlabHeader transaction stamps.
    Result<uint64_t> NextAllocationTransactionId();

    // Normal graph operations are manifest-driven and therefore O(handles).
    // `handles` is root-first; children are published/reclaimed before root.
    Status PublishTransaction(uint64_t owner_epoch, uint64_t transaction_id,
                              std::span<const ShmHandle> handles,
                              ShmHandle root);
    Status ReclaimTransaction(uint64_t owner_epoch, uint64_t transaction_id,
                              std::span<const ShmHandle> handles);

    // Recovery-only append-gap scan. Callers must have exclusively claimed the
    // dead journal transaction before invoking this O(all slots) fallback.
    Status ReclaimTransactionAppendGap(uint64_t owner_epoch,
                                       uint64_t transaction_id);

    using ReclaimGuard = bool (*)(ShmHandle, void*) noexcept;
    void SetReclaimGuard(ReclaimGuard guard, void* context) noexcept {
        reclaim_guard_ = guard;
        reclaim_guard_context_ = context;
    }

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

    // Maps an exact Region-relative SlabHeader offset to the minimum
    // authoritative metadata needed for safe handle resolution. The bitmap,
    // generation array and immutable class table are consulted directly; the
    // potentially corrupt SlabHeader is not trusted.
    Result<CentralSlabSlotMetadata> GetSlotMetadata(
        uint64_t header_offset) const;

    // Total number of slots managed by this allocator.
    uint32_t total_slot_count() const { return class_table_.total_slot_count(); }

    uint16_t class_count() const { return class_table_.class_count(); }

    // Exposed for recovery tooling: inspect a slot by its global slot index
    // without handle validation. `header_out` may be null.
    bool ReadSlotByIndex(uint32_t slot_index, SlabHeader* header_out,
                         const void** data_out) const;

    // Minimal real-metadata recovery adapter. These methods deliberately expose
    // observations and narrowly-scoped idempotent repairs rather than bitmap or
    // metadata pointers, so RecoveryScanner cannot grow a second allocator ABI.
    bool IsSlotOccupiedForRecovery(uint32_t slot_index) const noexcept;
    uint32_t AuthoritativeGenerationForRecovery(
        uint32_t slot_index) const noexcept;
    uint16_t ClassIdForRecovery(uint32_t slot_index) const noexcept;
    Status ClearSlotForRecovery(uint32_t slot_index,
                                uint32_t expected_state);
    Status ClearStaleStateForRecovery(uint32_t slot_index,
                                      uint32_t expected_state);

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
    static Result<CentralSlabAllocator> CreateWithStorage(
        void* shm_base, uint64_t available_size, uint64_t metadata_capacity,
        uint64_t slot_area_offset, uint64_t slot_capacity,
        uint32_t region_id, uint64_t handle_offset_bias,
        const ClassTableConfig& config);
    static Result<CentralSlabAllocator> AttachWithBias(
        void* shm_base, uint64_t available_size, uint64_t handle_offset_bias);

    // Resolves a handle to (slot_index, header, payload) with full
    // validation. `require_live` additionally demands the bitmap bit set and
    // generation match.
    Result<uint32_t> ResolveLocked(ShmHandle handle) const;
    Status PublishTransactionHandle(uint64_t owner_epoch,
                                    uint64_t transaction_id,
                                    ShmHandle handle, uint32_t required_role);
    Status ReclaimSlotExact(uint32_t slot_index, ShmHandle handle,
                            bool allow_published);
    bool CanReclaim(ShmHandle handle) const noexcept;
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
    uint64_t handle_offset_bias_ = 0;  // allocator base -> Region base offset.
    std::atomic<uint64_t>* next_transaction_id_ = nullptr;
    ReclaimGuard reclaim_guard_ = nullptr;
    void* reclaim_guard_context_ = nullptr;
};

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_CENTRAL_SLAB_H_
