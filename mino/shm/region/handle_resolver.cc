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

namespace mino {
namespace {

// Shared validation pipeline (design doc 7.2). On success, fills
// `*slab_header_out` with the validated SlabHeader location (region-relative
// offset) and `*object_offset_out` with the object payload offset.
//
// `require_mutable` selects the object_state/owner rules:
//   - read-only: object_state must be kPublished.
//   - mutable:   object_state must be kAllocated or kBuilding and the owner
//                epoch must equal the current process's epoch.
Status ValidateHandle(const SharedMemoryRegion& region,
                      const AllocatorMetadataProvider* allocator,
                      ShmHandle handle, TypeId expected_type,
                      uint64_t expected_schema_short_id, bool require_mutable,
                      uint64_t* slab_header_offset_out,
                      uint64_t* object_offset_out) {
    const SuperBlock* sb = region.superblock();
    if (sb == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "region has no superblock");
    }

    // -- Null Handle --------------------------------------------------------
    if (handle.IsNull()) {
        return Status::Error(StatusCode::kInvalidArgument, "null handle");
    }

    // -- Region ID match (7.1: mismatch is a hard rejection) ----------------
    if (handle.region_id != sb->region_id) {
        return Status::Error(StatusCode::kNotFound,
                             "handle region_id does not match region");
    }

    // -- Offset alignment ---------------------------------------------------
    // The handle offset addresses a SlabHeader, which is 64-byte aligned.
    if (handle.offset % alignof(SlabHeader) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "handle offset is not slab-header aligned");
    }

    // -- Data-area bounds (checked arithmetic) ------------------------------
    // The SlabHeader must lie fully inside [data_offset, data_end).
    const uint64_t data_offset = sb->data_offset;
    uint64_t data_end = 0;
    if (!CheckedAddU64(data_offset, sb->data_size, &data_end)) {
        return Status::Error(StatusCode::kCorruption,
                             "region data extent overflow");
    }
    if (handle.offset < data_offset) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "handle offset below data area");
    }
    uint64_t object_offset = 0;
    if (!CheckedAddU64(handle.offset,
                       static_cast<uint64_t>(sizeof(SlabHeader)),
                       &object_offset)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "object offset overflow");
    }
    if (object_offset > data_end) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "slab header extends past data area");
    }

    const std::byte* base = region.base();
    const auto* header =
        reinterpret_cast<const SlabHeader*>(base + handle.offset);

    // -- SlabHeader magic ---------------------------------------------------
    if (header->magic != kSlabHeaderMagic) {
        return Status::Error(StatusCode::kCorruption, "bad slab header magic");
    }

    // -- Slab bitmap occupancy (allocator-owned) ----------------------------
    if (allocator != nullptr &&
        !allocator->IsSlotOccupied(handle.offset)) {
        return Status::Error(StatusCode::kNotFound,
                             "slot not occupied in allocation bitmap");
    }

    // -- Generation (authoritative array, then header) ----------------------
    if (allocator != nullptr) {
        const uint32_t authoritative =
            allocator->AuthoritativeGeneration(handle.offset);
        if (handle.generation != authoritative) {
            return Status::Error(StatusCode::kNotFound,
                                 "stale handle generation");
        }
    }
    // The handle's generation must also match the one copied into the header
    // at allocation time (8.1/8.3).
    if (handle.generation != header->generation) {
        return Status::Error(StatusCode::kNotFound,
                             "handle generation does not match slab header");
    }

    // -- object_state and owner ---------------------------------------------
    const SlabObjectState state = LoadObjectState(*header);
    if (require_mutable) {
        if (state != SlabObjectState::kAllocated &&
            state != SlabObjectState::kBuilding) {
            return Status::Error(
                StatusCode::kPermissionDenied,
                "object not in a mutable state (ALLOCATED/BUILDING)");
        }
        // Mutable access requires the owner to be the current process.
        if (header->owner_epoch !=
            ProcessIdentity::Current().process_epoch) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "object owned by a different process");
        }
    } else {
        if (state != SlabObjectState::kPublished) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "object not PUBLISHED");
        }
    }

    // -- Type ID --------------------------------------------------------------
    if (header->type_id != expected_type.value) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "object type_id mismatch");
    }

    // -- Schema Identity (optional assert) ------------------------------------
    if (expected_schema_short_id != 0 &&
        header->schema_short_id != expected_schema_short_id) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "object schema short id mismatch");
    }

    // -- Object Size <= Capacity ---------------------------------------------
    if (header->object_size > header->capacity) {
        return Status::Error(StatusCode::kCorruption,
                             "object size exceeds slot capacity");
    }
    // The full object (header + capacity) must fit inside the data area.
    uint64_t object_end = 0;
    if (!CheckedAddU64(object_offset,
                       static_cast<uint64_t>(header->capacity),
                       &object_end)) {
        return Status::Error(StatusCode::kCorruption,
                             "object extent overflow");
    }
    if (object_end > data_end) {
        return Status::Error(StatusCode::kCorruption,
                             "object extends past data area");
    }

    *slab_header_offset_out = handle.offset;
    *object_offset_out = object_offset;
    return Status::Ok();
}

}  // namespace

HandleResolver::MutableAddress HandleResolver::ResolveMutableInternal(
    ShmHandle handle, TypeId expected, uint64_t expected_schema_short_id) {
    uint64_t header_off = 0;
    uint64_t object_off = 0;
    Status st = ValidateHandle(*region_, allocator_, handle, expected,
                               expected_schema_short_id,
                               /*require_mutable=*/true, &header_off,
                               &object_off);
    if (!st.ok()) {
        return MutableAddress{st};
    }
    void* ptr = region_->base() + object_off;
    return MutableAddress{Result<void*>(ptr)};
}

HandleResolver::ConstAddress HandleResolver::ResolveInternal(
    ShmHandle handle, TypeId expected, uint64_t expected_schema_short_id) const {
    uint64_t header_off = 0;
    uint64_t object_off = 0;
    Status st = ValidateHandle(*region_, allocator_, handle, expected,
                               expected_schema_short_id,
                               /*require_mutable=*/false, &header_off,
                               &object_off);
    if (!st.ok()) {
        return ConstAddress{st};
    }
    const void* ptr = region_->base() + object_off;
    return ConstAddress{Result<const void*>(ptr)};
}

}  // namespace mino
