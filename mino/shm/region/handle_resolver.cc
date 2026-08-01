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

#include "mino/shm/region/handle_resolver.h"

#include <atomic>

#include "mino/common/checked_arithmetic.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/large_object_pool.h"

namespace mino {
namespace {

// Shared validation pipeline (design doc 7.2). The 16-byte ShmHandle cannot
// carry Region UUID/Epoch, so the strongest validation expressible by the
// current ABI is to bind a resolver to an Attach Context and reject if that
// mapped Region's identity or epoch changes.
Status ValidateHandle(const SharedMemoryRegion& region,
                      const AllocatorMetadataProvider* allocator,
                      uint32_t attached_region_id,
                      uint64_t attached_region_uuid_lo,
                      uint64_t attached_region_uuid_hi,
                      uint64_t attached_region_epoch, ShmHandle handle,
                      TypeId expected_type, uint64_t expected_schema_short_id,
                      uint64_t type_size, uint64_t type_alignment,
                      bool require_mutable, uint64_t* object_offset_out) {
    const SuperBlock* sb = region.superblock();
    if (sb == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "region has no superblock");
    }
    if (allocator == nullptr) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "allocator metadata provider is required for handle resolution");
    }

    // -- Null Handle --------------------------------------------------------
    if (handle.IsNull()) {
        return Status::Error(StatusCode::kInvalidArgument, "null handle");
    }

    // -- Attach Context / Region identity -----------------------------------
    if (attached_region_id == 0 ||
        (attached_region_uuid_lo == 0 && attached_region_uuid_hi == 0) ||
        attached_region_epoch == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "invalid Region Attach Context identity");
    }
    if (region.region_id() != attached_region_id ||
        sb->region_id != attached_region_id ||
        sb->region_uuid_lo != attached_region_uuid_lo ||
        sb->region_uuid_hi != attached_region_uuid_hi) {
        return Status::Error(StatusCode::kUnavailable,
                             "Region identity changed after resolver binding");
    }
    if (LoadRegionEpoch(*sb) != attached_region_epoch) {
        return Status::Error(StatusCode::kUnavailable,
                             "Region epoch changed after resolver binding");
    }
    if (handle.region_id != attached_region_id) {
        return Status::Error(StatusCode::kNotFound,
                             "handle region_id does not match Attach Context");
    }

    // A read-only mmap must never yield a mutable pointer. Writable Region data
    // also requires the still-current supervisor fence documented by Region.
    if (require_mutable) {
        if (region.read_only()) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "mutable resolve on a read-only Region");
        }
        const Status fence = region.ValidateSupervisorFence();
        if (!fence.ok()) {
            return fence;
        }
    }

    // -- Offset alignment and data-area bounds ------------------------------
    if (handle.offset % alignof(SlabHeader) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "handle offset is not slab-header aligned");
    }
    const uint64_t data_offset = sb->data_offset;
    uint64_t data_end = 0;
    if (!CheckedAddU64(data_offset, sb->data_size, &data_end) ||
        data_end > region.size()) {
        return Status::Error(StatusCode::kCorruption,
                             "Region data extent is invalid");
    }
    if (handle.offset < data_offset) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "handle offset below data area");
    }
    uint64_t header_end = 0;
    if (!CheckedAddU64(handle.offset,
                       static_cast<uint64_t>(sizeof(SlabHeader)),
                       &header_end)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "object header extent overflow");
    }
    if (header_end > data_end) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "object header extends past data area");
    }

    // -- Authoritative allocator metadata -----------------------------------
    Result<AllocatorSlotMetadata> slot_result =
        allocator->GetSlotMetadata(handle.offset);
    if (!slot_result.ok()) {
        return slot_result.status();
    }
    const AllocatorSlotMetadata& slot = slot_result.value();
    if (!slot.occupied) {
        return Status::Error(StatusCode::kNotFound,
                             "slot not occupied in allocation bitmap");
    }
    if (handle.generation != slot.generation) {
        return Status::Error(StatusCode::kNotFound,
                             "stale handle generation");
    }
    if (slot.capacity == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "allocator returned zero object capacity");
    }
    switch (slot.object_kind) {
        case AllocatorObjectKind::kContiguousSlot:
            if (slot.class_count == 0 || slot.class_id >= slot.class_count ||
                (slot.object_extent != 0 &&
                 slot.object_extent != slot.capacity)) {
                return Status::Error(
                    StatusCode::kCorruption,
                    "allocator returned invalid regular class metadata");
            }
            break;
        case AllocatorObjectKind::kSegmented:
            if (slot.payload_offset == 0 || slot.object_extent == 0) {
                return Status::Error(
                    StatusCode::kCorruption,
                    "segmented allocator metadata lacks payload extent");
            }
            break;
        default:
            return Status::Error(StatusCode::kCorruption,
                                 "allocator returned unknown object kind");
    }

    uint64_t object_offset = slot.payload_offset;
    if (object_offset == 0 &&
        !CheckedAddU64(handle.offset,
                       static_cast<uint64_t>(sizeof(SlabHeader)),
                       &object_offset)) {
        return Status::Error(StatusCode::kCorruption,
                             "legacy payload offset overflow");
    }
    const uint64_t object_extent =
        slot.object_extent == 0 ? slot.capacity : slot.object_extent;
    if (object_extent < slot.capacity || object_offset < data_offset) {
        return Status::Error(StatusCode::kCorruption,
                             "allocator returned invalid object extent");
    }
    uint64_t object_storage_end = 0;
    if (!CheckedAddU64(object_offset, object_extent, &object_storage_end) ||
        object_storage_end > data_end) {
        return Status::Error(StatusCode::kCorruption,
                             "allocator object extent exceeds Region data");
    }

    const std::byte* base = region.base();
    const auto* header =
        reinterpret_cast<const SlabHeader*>(base + handle.offset);

    // Acquiring object_state pairs with allocator publication before any
    // immutable header field or its CRC is trusted.
    const SlabObjectState state = LoadObjectState(*header);

    // -- SlabHeader identity, class, and immutable CRC -----------------------
    if (header->magic != kSlabHeaderMagic) {
        return Status::Error(StatusCode::kCorruption, "bad slab header magic");
    }
    if (header->header_version != kSlabHeaderVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "unsupported slab header version");
    }
    if (handle.generation !=
        header->generation.load(std::memory_order_acquire)) {
        return Status::Error(StatusCode::kNotFound,
                             "handle generation does not match slab header");
    }
    if (header->class_id != slot.class_id ||
        header->capacity != slot.capacity) {
        return Status::Error(StatusCode::kCorruption,
                             "slab class metadata mismatch");
    }
    if (!VerifyImmutableHeader(*header)) {
        return Status::Error(StatusCode::kCorruption,
                             "immutable slab header CRC mismatch");
    }

    // -- object_state and owner ---------------------------------------------
    if (require_mutable) {
        if (state != SlabObjectState::kAllocated &&
            state != SlabObjectState::kBuilding) {
            return Status::Error(
                StatusCode::kPermissionDenied,
                "object not in a mutable state (ALLOCATED/BUILDING)");
        }
        if (header->owner_epoch.load(std::memory_order_acquire) !=
            ProcessIdentity::Current().process_epoch) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "object owned by a different process");
        }
    } else if (state != SlabObjectState::kPublished) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "object not PUBLISHED");
    }

    // -- Type and schema identity -------------------------------------------
    if (header->type_id != expected_type.value) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "object type_id mismatch");
    }
    if (expected_schema_short_id != 0 &&
        header->schema_short_id != expected_schema_short_id) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "object schema short id mismatch");
    }

    // -- Header, allocator, data-area, and requested C++ type extents --------
    if (header->object_size > object_extent) {
        return Status::Error(StatusCode::kCorruption,
                             "object size exceeds authoritative object extent");
    }
    uint64_t logical_object_end = 0;
    uint64_t typed_object_end = 0;
    if (!CheckedAddU64(object_offset, header->object_size,
                       &logical_object_end) ||
        !CheckedAddU64(object_offset, type_size, &typed_object_end)) {
        return Status::Error(StatusCode::kCorruption,
                             "object extent overflow");
    }
    if (logical_object_end > object_storage_end) {
        return Status::Error(StatusCode::kCorruption,
                             "object extends past its authoritative extent");
    }
    if (type_size == 0 || type_size > header->object_size ||
        typed_object_end > logical_object_end) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "C++ type size exceeds stored object extent");
    }
    if (type_alignment == 0 ||
        (type_alignment & (type_alignment - 1)) != 0 ||
        reinterpret_cast<uintptr_t>(base + object_offset) % type_alignment != 0) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "stored object does not satisfy C++ type alignment");
    }

    *object_offset_out = object_offset;
    return Status::Ok();
}

}  // namespace

CentralSlabAllocatorMetadataProvider::CentralSlabAllocatorMetadataProvider(
    const CentralSlabAllocator& allocator)
    : allocator_(&allocator) {}

Result<AllocatorSlotMetadata>
CentralSlabAllocatorMetadataProvider::GetSlotMetadata(uint64_t offset) const {
    MINO_ASSIGN_OR_RETURN(CentralSlabSlotMetadata metadata,
                          allocator_->GetSlotMetadata(offset));
    uint64_t payload_offset = 0;
    if (!CheckedAddU64(offset, static_cast<uint64_t>(sizeof(SlabHeader)),
                       &payload_offset)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "central slab payload offset overflow");
    }
    return AllocatorSlotMetadata{
        .occupied = metadata.occupied,
        .generation = metadata.generation,
        .class_id = metadata.class_id,
        .class_count = metadata.class_count,
        .capacity = metadata.capacity,
        .payload_offset = payload_offset,
        .object_extent = metadata.capacity,
        .object_kind = AllocatorObjectKind::kContiguousSlot,
    };
}

LargeObjectPoolMetadataProvider::LargeObjectPoolMetadataProvider(
    const LargeObjectPool& pool)
    : pool_(&pool) {}

Result<AllocatorSlotMetadata>
LargeObjectPoolMetadataProvider::GetSlotMetadata(uint64_t offset) const {
    MINO_ASSIGN_OR_RETURN(LargeObjectSlotMetadata metadata,
                          pool_->GetSlotMetadata(offset));
    return AllocatorSlotMetadata{
        .occupied = metadata.occupied,
        .generation = metadata.generation,
        .class_id = metadata.class_id,
        .class_count = 0,
        .capacity = metadata.capacity,
        .payload_offset = metadata.payload_offset,
        .object_extent = metadata.object_extent,
        .object_kind = metadata.segmented
                           ? AllocatorObjectKind::kSegmented
                           : AllocatorObjectKind::kContiguousSlot,
    };
}

HandleResolver::HandleResolver(SharedMemoryRegion& region,
                               const AllocatorMetadataProvider& allocator)
    : HandleResolver(region, &allocator) {}

HandleResolver::HandleResolver(SharedMemoryRegion& region,
                               const AllocatorMetadataProvider* allocator)
    : region_(&region), allocator_(allocator),
      attached_region_id_(region.region_id()) {
    const SuperBlock* sb = region.superblock();
    if (sb != nullptr) {
        attached_region_uuid_lo_ = sb->region_uuid_lo;
        attached_region_uuid_hi_ = sb->region_uuid_hi;
        attached_region_epoch_ = LoadRegionEpoch(*sb);
    }
}

HandleResolver::MutableAddress HandleResolver::ResolveMutableInternal(
    ShmHandle handle, TypeId expected, uint64_t expected_schema_short_id,
    uint64_t type_size, uint64_t type_alignment) {
    uint64_t object_off = 0;
    Status st = ValidateHandle(
        *region_, allocator_, attached_region_id_, attached_region_uuid_lo_,
        attached_region_uuid_hi_, attached_region_epoch_, handle, expected,
        expected_schema_short_id, type_size, type_alignment,
        /*require_mutable=*/true, &object_off);
    if (!st.ok()) {
        return MutableAddress{st};
    }
    void* ptr = region_->base() + object_off;
    return MutableAddress{Result<void*>(ptr)};
}

HandleResolver::ConstAddress HandleResolver::ResolveInternal(
    ShmHandle handle, TypeId expected, uint64_t expected_schema_short_id,
    uint64_t type_size, uint64_t type_alignment) const {
    uint64_t object_off = 0;
    Status st = ValidateHandle(
        *region_, allocator_, attached_region_id_, attached_region_uuid_lo_,
        attached_region_uuid_hi_, attached_region_epoch_, handle, expected,
        expected_schema_short_id, type_size, type_alignment,
        /*require_mutable=*/false, &object_off);
    if (!st.ok()) {
        return ConstAddress{st};
    }
    const void* ptr = region_->base() + object_off;
    return ConstAddress{Result<const void*>(ptr)};
}

}  // namespace mino
