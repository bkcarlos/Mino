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

#include "mino/shm/allocator/central_slab.h"

#include <atomic>
#include <cstring>
#include <new>

namespace mino {
namespace {

// Magic and version of the allocator superblock ("MSLA" little-endian).
constexpr uint32_t kAllocatorMagic = 0x4D534C41u;
constexpr uint32_t kAllocatorVersion = 1;

// Shared-memory allocator superblock, followed by class descriptors, the
// sharded bitmap, the generation array, and the per-slot headers/payloads.
// All offsets are relative to shm_base so that the Region can be mapped at
// different addresses in different processes.
struct alignas(64) AllocatorSuperblock {
    uint32_t magic;
    uint32_t version;
    uint32_t region_id;
    uint32_t class_count;

    uint64_t total_size;        // Total Region size in bytes.
    uint64_t metadata_size;     // Size of the metadata area (superblock +
                                // descriptors + bitmap + generations +
                                // draining flags), i.e. offset of slot area.
    uint64_t slot_stride;       // sizeof(SlabHeader) + max payload per slot.
    uint32_t total_slot_count;
    uint32_t reserved;

    // Followed by:
    //   ClassDescriptor classes[class_count]
    //   std::atomic<uint64_t> bitmap[bitmap_words]
    //   std::atomic<uint32_t> generations[total_slot_count]
    //   std::atomic<uint32_t> draining[class_count]
    //   (padding to slot_stride alignment)
    //   slots: [SlabHeader + payload] * total_slot_count
};

static_assert(sizeof(AllocatorSuperblock) == 64);

constexpr uint64_t AlignUp(uint64_t v, uint64_t a) { return (v + a - 1) / a * a; }

}  // namespace

struct CentralSlabAllocator::Layout {
    uint64_t metadata_size = 0;
    uint64_t slot_stride = 0;
    uint32_t bitmap_words = 0;
};

Result<CentralSlabAllocator::Layout> CentralSlabAllocator::ComputeLayout(
    const ClassTable& table, uint64_t data_region_size) {
    Layout layout;
    const uint32_t class_count = table.class_count();
    const uint32_t total_slots = table.total_slot_count();

    layout.bitmap_words = (total_slots + kBitmapShardBits - 1) / kBitmapShardBits;

    // Metadata area: superblock + descriptors + bitmap + generations +
    // draining flags, all naturally aligned.
    uint64_t off = sizeof(AllocatorSuperblock);
    off += sizeof(ClassDescriptor) * class_count;
    off = AlignUp(off, alignof(std::atomic<uint64_t>));
    off += sizeof(std::atomic<uint64_t>) * layout.bitmap_words;
    off = AlignUp(off, alignof(std::atomic<uint32_t>));
    off += sizeof(std::atomic<uint32_t>) * total_slots;
    off += sizeof(std::atomic<uint32_t>) * class_count;

    // Slot area begins at the next cache-line boundary; each slot is
    // SlabHeader + max payload, so every slot header stays cache-line
    // aligned.
    layout.slot_stride = sizeof(SlabHeader) + table.max_object_size();
    layout.metadata_size = AlignUp(off, alignof(SlabHeader));

    const uint64_t required =
        layout.metadata_size + layout.slot_stride * total_slots;
    if (data_region_size < required) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "data region too small for configured classes");
    }
    return layout;
}

Result<CentralSlabAllocator> CentralSlabAllocator::Create(
    void* shm_base, uint64_t data_region_size, const ClassTableConfig& config) {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument, "shm_base is null");
    }
    MINO_ASSIGN_OR_RETURN(ClassTable table, ClassTable::Create(config));
    MINO_ASSIGN_OR_RETURN(Layout layout,
                          ComputeLayout(table, data_region_size));

    auto* super = new (shm_base) AllocatorSuperblock{};
    super->magic = kAllocatorMagic;
    super->version = kAllocatorVersion;
    super->region_id = 0;  // Region id is assigned by the Region layer; the
                           // allocator uses it read-only via Attach().
    super->class_count = table.class_count();
    super->total_size = data_region_size;
    super->metadata_size = layout.metadata_size;
    super->slot_stride = layout.slot_stride;
    super->total_slot_count = table.total_slot_count();
    super->reserved = 0;

    auto* base = static_cast<std::byte*>(shm_base);
    auto* descriptors = reinterpret_cast<ClassDescriptor*>(base + sizeof(AllocatorSuperblock));
    for (uint16_t i = 0; i < table.class_count(); ++i) {
        descriptors[i] = table.GetClass(i);
    }

    auto* bitmap_words = reinterpret_cast<std::atomic<uint64_t>*>(
        base + AlignUp(sizeof(AllocatorSuperblock) +
                           sizeof(ClassDescriptor) * table.class_count(),
                       alignof(std::atomic<uint64_t>)));
    for (uint32_t i = 0; i < layout.bitmap_words; ++i) {
        new (&bitmap_words[i]) std::atomic<uint64_t>(0);
    }

    auto* generations = reinterpret_cast<std::atomic<uint32_t>*>(
        reinterpret_cast<std::byte*>(bitmap_words) +
        sizeof(std::atomic<uint64_t>) * layout.bitmap_words);
    for (uint32_t i = 0; i < table.total_slot_count(); ++i) {
        new (&generations[i]) std::atomic<uint32_t>(0);
    }

    auto* draining = reinterpret_cast<std::atomic<uint32_t>*>(
        reinterpret_cast<std::byte*>(generations) +
        sizeof(std::atomic<uint32_t>) * table.total_slot_count());
    for (uint32_t i = 0; i < table.class_count(); ++i) {
        new (&draining[i]) std::atomic<uint32_t>(0);
    }

    // Slot headers are zero-initialized; the bitmap remains all-clear.

    MINO_ASSIGN_OR_RETURN(ShardedBitmap bitmap,
                          ShardedBitmap::Create(bitmap_words, layout.bitmap_words));
    MINO_ASSIGN_OR_RETURN(GenerationArray gen_array,
                          GenerationArray::Create(generations, table.total_slot_count()));

    CentralSlabAllocator alloc;
    alloc.shm_base_ = shm_base;
    alloc.class_table_ = std::move(table);
    alloc.bitmap_ = bitmap;
    alloc.generations_ = gen_array;
    alloc.headers_ = reinterpret_cast<SlabHeader*>(base + layout.metadata_size);
    alloc.class_draining_ = draining;
    alloc.data_region_size_ = data_region_size;
    alloc.region_id_ = super->region_id;
    alloc.slot_stride_ = layout.slot_stride;
    return alloc;
}

Result<CentralSlabAllocator> CentralSlabAllocator::Attach(void* shm_base) {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument, "shm_base is null");
    }
    auto* super = static_cast<AllocatorSuperblock*>(shm_base);
    if (super->magic != kAllocatorMagic) {
        return Status::Error(StatusCode::kCorruption, "bad allocator magic");
    }
    if (super->version != kAllocatorVersion) {
        return Status::Error(StatusCode::kCorruption, "bad allocator version");
    }

    auto* base = static_cast<std::byte*>(shm_base);
    auto* descriptors = reinterpret_cast<ClassDescriptor*>(base + sizeof(AllocatorSuperblock));

    ClassTableConfig config;
    config.classes.reserve(super->class_count);
    for (uint32_t i = 0; i < super->class_count; ++i) {
        config.classes.push_back(
            {.slot_size = descriptors[i].slot_size,
             .slot_count = descriptors[i].slot_count});
    }
    MINO_ASSIGN_OR_RETURN(ClassTable table, ClassTable::Create(config));

    MINO_ASSIGN_OR_RETURN(Layout layout,
                          ComputeLayout(table, super->total_size));

    auto* bitmap_words = reinterpret_cast<std::atomic<uint64_t>*>(
        base + AlignUp(sizeof(AllocatorSuperblock) +
                           sizeof(ClassDescriptor) * table.class_count(),
                       alignof(std::atomic<uint64_t>)));
    auto* generations = reinterpret_cast<std::atomic<uint32_t>*>(
        reinterpret_cast<std::byte*>(bitmap_words) +
        sizeof(std::atomic<uint64_t>) * layout.bitmap_words);
    auto* draining = reinterpret_cast<std::atomic<uint32_t>*>(
        reinterpret_cast<std::byte*>(generations) +
        sizeof(std::atomic<uint32_t>) * table.total_slot_count());

    MINO_ASSIGN_OR_RETURN(ShardedBitmap bitmap,
                          ShardedBitmap::Create(bitmap_words, layout.bitmap_words));
    MINO_ASSIGN_OR_RETURN(GenerationArray gen_array,
                          GenerationArray::Create(generations, table.total_slot_count()));

    CentralSlabAllocator alloc;
    alloc.shm_base_ = shm_base;
    alloc.class_table_ = std::move(table);
    alloc.bitmap_ = bitmap;
    alloc.generations_ = gen_array;
    alloc.headers_ = reinterpret_cast<SlabHeader*>(base + layout.metadata_size);
    alloc.class_draining_ = draining;
    alloc.data_region_size_ = super->total_size;
    alloc.region_id_ = super->region_id;
    alloc.slot_stride_ = layout.slot_stride;
    return alloc;
}

Result<ShmHandle> CentralSlabAllocator::Allocate(const AllocationRequest& request) {
    // Step 1: checked-align the request size. Alignment must be a power of
    // two; the aligned size must not overflow.
    const uint32_t alignment = request.alignment == 0 ? 1 : request.alignment;
    if ((alignment & (alignment - 1)) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "alignment must be a power of two");
    }
    const uint64_t aligned_size =
        (static_cast<uint64_t>(request.object_size) + alignment - 1) / alignment * alignment;
    if (aligned_size > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument, "object size overflow");
    }

    // Step 2: select the smallest fitting class.
    MINO_ASSIGN_OR_RETURN(const uint16_t class_id,
                          class_table_.FindClass(static_cast<uint32_t>(aligned_size)));
    const ClassDescriptor& cls = class_table_.GetClass(class_id);

    // Reject allocations into a class that has been marked DRAINING.
    if (class_draining_[class_id].load(std::memory_order_acquire) != 0) {
        return Status::Error(StatusCode::kUnavailable, "size class is draining");
    }

    // Steps 3-5: shard selection, free-bit search, CAS claim. The shard hint
    // is the class's base shard so allocations of one class cluster together.
    MINO_ASSIGN_OR_RETURN(
        const uint32_t bit_index,
        bitmap_.FindAndSetFreeBitInRange(
            cls.bitmap_shard_offset,
            cls.bitmap_shard_offset + cls.slot_count));

    const uint32_t slot_index = bit_index;

    // Step 6: increment the authoritative generation and copy it into the
    // header; refuse to wrap at UINT32_MAX by marking the class DRAINING.
    const uint32_t generation = generations_.Increment(slot_index);
    if (generation == kGenerationDraining) {
        class_draining_[class_id].store(1, std::memory_order_release);
        // Undo the bitmap claim so the slot stays free for recovery.
        bitmap_.ClearBit(slot_index);
        return Status::Error(StatusCode::kResourceExhausted,
                             "generation exhausted; class marked DRAINING");
    }

    SlabHeader& header = HeaderAt(slot_index);
    header.magic = kSlabHeaderMagic;
    header.header_version = kSlabHeaderVersion;
    header.class_id = class_id;
    header.generation = generation;
    header.capacity = cls.slot_size;
    header.object_size = request.object_size;
    header.type_id = request.type_id.value;
    header.layout_version = request.schema.layout_version;
    header.schema_short_id = request.schema.short_id;

    // Step 7: Owner Epoch and Transaction ID. Owner epoch identifies the
    // allocating process generation; transaction id is left for the dynamic
    // allocation journal (design doc 8.6). For the D1 central allocator both
    // are stamped as zero until the runtime supplies them.
    header.owner_epoch = 0;
    header.allocation_transaction_id = 0;

    // Immutable CRC covers the frozen identity fields (design doc 8.1).
    header.immutable_header_crc = ComputeImmutableHeaderCrc(header);
    header.reserved = 0;

    // Step 8: single publication point. Everything above must be visible to
    // any thread that observes kAllocated with acquire ordering.
    header.object_state.store(static_cast<uint32_t>(ObjectState::kAllocated),
                              std::memory_order_release);

    // Step 9: return the Handle. offset is relative to shm_base so the
    // Handle stays valid across different mappings of the same Region.
    ShmHandle handle;
    handle.offset = static_cast<uint64_t>(
        reinterpret_cast<std::byte*>(&header) -
        static_cast<std::byte*>(shm_base_));
    handle.generation = generation;
    handle.region_id = region_id_;
    return handle;
}

Result<MutableBuildView> CentralSlabAllocator::BeginBuild(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    if (!VerifyImmutableHeader(header)) {
        return Status::Error(StatusCode::kCorruption,
                             "immutable header CRC mismatch");
    }
    uint32_t expected = static_cast<uint32_t>(ObjectState::kAllocated);
    if (!header.object_state.compare_exchange_strong(
            expected, static_cast<uint32_t>(ObjectState::kBuilding),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "slot is not available for exclusive build");
    }

    MutableBuildView view;
    view.handle = handle;
    view.capacity = header.capacity;
    view.object_size = header.object_size;
    view.type_id = TypeId{header.type_id};
    view.schema_short_id = header.schema_short_id;
    view.layout_version = header.layout_version;
    view.data = reinterpret_cast<std::byte*>(&header) + sizeof(SlabHeader);
    return view;
}

Status CentralSlabAllocator::Publish(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    if (!VerifyImmutableHeader(header)) {
        return Status::Error(StatusCode::kCorruption,
                             "immutable header CRC mismatch");
    }
    uint32_t expected = static_cast<uint32_t>(ObjectState::kBuilding);
    if (!header.object_state.compare_exchange_strong(
            expected, static_cast<uint32_t>(ObjectState::kPublished),
            std::memory_order_release, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "slot is not in kBuilding state");
    }
    return Status::Ok();
}

Status CentralSlabAllocator::Abort(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    for (;;) {
        const uint32_t state =
            header.object_state.load(std::memory_order_acquire);
        if (state == static_cast<uint32_t>(ObjectState::kAborting)) {
            break;
        }
        if (state != static_cast<uint32_t>(ObjectState::kAllocated) &&
            state != static_cast<uint32_t>(ObjectState::kBuilding)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "only unpublished objects may be aborted");
        }
        uint32_t expected = state;
        if (header.object_state.compare_exchange_weak(
                expected, static_cast<uint32_t>(ObjectState::kAborting),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
    }
    return Reclaim(handle);
}

Status CentralSlabAllocator::Retire(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    // Transition ALLOCATED/BUILDING/PUBLISHED -> RETIRED. If already RETIRED
    // this is a no-op; any other state (FREE, ABORTING) is rejected.
    uint32_t expected = static_cast<uint32_t>(ObjectState::kAllocated);
    for (;;) {
        const uint32_t state = header.object_state.load(std::memory_order_acquire);
        if (state == static_cast<uint32_t>(ObjectState::kRetired)) {
            return Status::Ok();
        }
        if (state != static_cast<uint32_t>(ObjectState::kAllocated) &&
            state != static_cast<uint32_t>(ObjectState::kBuilding) &&
            state != static_cast<uint32_t>(ObjectState::kPublished)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "slot is not in a retirable state");
        }
        expected = state;
        if (header.object_state.compare_exchange_weak(
                expected, static_cast<uint32_t>(ObjectState::kRetired),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            return Status::Ok();
        }
    }
}

Status CentralSlabAllocator::Reclaim(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    const uint32_t state = header.object_state.load(std::memory_order_acquire);
    // Design doc 8.4: Reclaim only after Retire, or for slots in an
    // intermediate crash state (recovery-driven reclaim).
    if (state != static_cast<uint32_t>(ObjectState::kRetired) &&
        state != static_cast<uint32_t>(ObjectState::kAborting) &&
        state != static_cast<uint32_t>(ObjectState::kAllocated) &&
        state != static_cast<uint32_t>(ObjectState::kBuilding)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "slot must be retired (or crash-intermediate) before reclaim");
    }

    // D1 scope: the caller guarantees no valid Borrow and no live Pin.
    // Enforcement lands with the Borrow/Pin registry (design doc 11).

    // Mark the slot free before clearing the bitmap so that recovery never
    // sees "bitmap clear but state non-free" as an in-use slot.
    header.object_state.store(static_cast<uint32_t>(ObjectState::kFree),
                              std::memory_order_release);
    MINO_RETURN_IF_ERROR(bitmap_.ClearBit(slot_index));
    return Status::Ok();
}

Result<SlabView> CentralSlabAllocator::Inspect(ShmHandle handle) const {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    const SlabHeader& header = HeaderAt(slot_index);
    if (!VerifyImmutableHeader(header)) {
        return Status::Error(StatusCode::kCorruption,
                             "immutable header CRC mismatch");
    }

    SlabView view;
    view.handle = handle;
    view.state = static_cast<ObjectState>(
        header.object_state.load(std::memory_order_acquire));
    view.class_id = header.class_id;
    view.generation = generations_.Get(slot_index);
    view.capacity = header.capacity;
    view.object_size = header.object_size;
    view.type_id = TypeId{header.type_id};
    view.schema_short_id = header.schema_short_id;
    view.layout_version = header.layout_version;
    view.data = reinterpret_cast<const std::byte*>(&header) + sizeof(SlabHeader);
    return view;
}

bool CentralSlabAllocator::ReadSlotByIndex(uint32_t slot_index, SlabHeader* header_out,
                                           const void** data_out) const {
    if (slot_index >= total_slot_count()) {
        return false;
    }
    const SlabHeader& header = HeaderAt(slot_index);
    if (header_out != nullptr) {
        // Copy fields under acquire ordering; the header is read-only after
        // publication for the fields that matter here.
        header_out->magic = header.magic;
        header_out->header_version = header.header_version;
        header_out->class_id = header.class_id;
        header_out->generation = header.generation;
        header_out->object_state.store(
            header.object_state.load(std::memory_order_acquire),
            std::memory_order_relaxed);
        header_out->capacity = header.capacity;
        header_out->object_size = header.object_size;
        header_out->type_id = header.type_id;
        header_out->layout_version = header.layout_version;
        header_out->schema_short_id = header.schema_short_id;
        header_out->owner_epoch = header.owner_epoch;
        header_out->allocation_transaction_id = header.allocation_transaction_id;
        header_out->immutable_header_crc = header.immutable_header_crc;
        header_out->reserved = header.reserved;
    }
    if (data_out != nullptr) {
        *data_out = reinterpret_cast<const std::byte*>(&header) + sizeof(SlabHeader);
    }
    return true;
}

SlabHeader& CentralSlabAllocator::HeaderAt(uint32_t slot_index) {
    auto* base = reinterpret_cast<std::byte*>(headers_);
    return *reinterpret_cast<SlabHeader*>(base + slot_index * slot_stride_);
}

const SlabHeader& CentralSlabAllocator::HeaderAt(uint32_t slot_index) const {
    const auto* base = reinterpret_cast<const std::byte*>(headers_);
    return *reinterpret_cast<const SlabHeader*>(base + slot_index * slot_stride_);
}

Result<uint32_t> CentralSlabAllocator::ResolveLocked(ShmHandle handle) const {
    if (handle.offset == 0 && handle.generation == 0 && handle.region_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument, "null handle");
    }
    if (handle.region_id != region_id_) {
        return Status::Error(StatusCode::kInvalidArgument, "foreign region handle");
    }
    if (handle.offset < sizeof(AllocatorSuperblock) ||
        handle.offset >= data_region_size_) {
        return Status::Error(StatusCode::kInvalidArgument, "handle offset out of range");
    }

    // Map offset -> slot index. Offsets always point at a SlabHeader.
    const uint64_t rel = handle.offset;
    const uint64_t metadata_size =
        reinterpret_cast<const std::byte*>(headers_) -
        static_cast<const std::byte*>(shm_base_);
    if (rel < metadata_size || (rel - metadata_size) % slot_stride_ != 0) {
        return Status::Error(StatusCode::kInvalidArgument, "handle offset not slot-aligned");
    }
    const uint32_t slot_index =
        static_cast<uint32_t>((rel - metadata_size) / slot_stride_);
    if (slot_index >= total_slot_count()) {
        return Status::Error(StatusCode::kInvalidArgument, "slot index out of range");
    }
    if (!bitmap_.IsSet(slot_index)) {
        return Status::Error(StatusCode::kNotFound, "slot is not allocated");
    }
    const uint32_t current_gen = generations_.Get(slot_index);
    if (current_gen != handle.generation) {
        return Status::Error(StatusCode::kNotFound,
                             "stale handle (generation mismatch)");
    }
    return slot_index;
}

}  // namespace mino
