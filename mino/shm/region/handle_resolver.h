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

#ifndef MINO_SHM_REGION_HANDLE_RESOLVER_H_
#define MINO_SHM_REGION_HANDLE_RESOLVER_H_

#include <cstddef>
#include <cstdint>

#include "mino/abi/shm_handle.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/region/region.h"
#include "mino/shm/region/superblock.h"

namespace mino {

class CentralSlabAllocator;
class LargeObjectPool;

// Describes how allocator-owned storage represents one logical object.
enum class AllocatorObjectKind : uint8_t {
    kContiguousSlot = 0,
    kSegmented = 1,
};

// Allocator-owned facts for one Region-relative SlabHeader offset. The class
// fields must come from the immutable allocator class table, not from the
// SlabHeader being validated.
struct AllocatorSlotMetadata {
    bool occupied = false;
    uint32_t generation = 0;
    uint16_t class_id = 0;
    uint16_t class_count = 0;
    uint32_t capacity = 0;

    // Added compatibly: legacy providers may leave these fields at their
    // defaults, in which case a contiguous payload immediately after the
    // SlabHeader with extent `capacity` is assumed. Segmented providers must
    // return explicit Region-relative payload and whole-object extents.
    uint64_t payload_offset = 0;
    uint64_t object_extent = 0;
    AllocatorObjectKind object_kind = AllocatorObjectKind::kContiguousSlot;
};

// AllocatorMetadataProvider abstracts the authoritative allocator metadata
// consulted by HandleResolver: slot identity, bitmap occupancy, Generation
// Array, and class-table capacity. Returning one record also lets providers
// reject offsets that are aligned but are not actual slot starts.
class AllocatorMetadataProvider {
public:
    virtual ~AllocatorMetadataProvider() = default;

    virtual Result<AllocatorSlotMetadata> GetSlotMetadata(
        uint64_t offset) const = 0;
};

// Production adapters over allocator-owned read-only metadata APIs. Wrapped
// allocators must outlive the providers and any HandleResolver using them.
class CentralSlabAllocatorMetadataProvider final
    : public AllocatorMetadataProvider {
public:
    explicit CentralSlabAllocatorMetadataProvider(
        const CentralSlabAllocator& allocator);

    Result<AllocatorSlotMetadata> GetSlotMetadata(
        uint64_t offset) const override;

private:
    const CentralSlabAllocator* allocator_;
};

class LargeObjectPoolMetadataProvider final
    : public AllocatorMetadataProvider {
public:
    explicit LargeObjectPoolMetadataProvider(const LargeObjectPool& pool);

    Result<AllocatorSlotMetadata> GetSlotMetadata(
        uint64_t offset) const override;

private:
    const LargeObjectPool* pool_;
};

// HandleResolver safely dereferences ShmHandles into typed pointers within an
// attached Region (design doc section 7.2).
//
// Every dereference validates the full checklist of section 7.2 before
// returning a pointer; the raw `region_base + offset` is never exposed without
// validation.
class HandleResolver {
public:
    // `region` and `allocator` must outlive the resolver. The reference form is
    // preferred for new callers. The pointer overload is retained for source
    // compatibility, but a null provider makes every Resolve fail with
    // kInvalidArgument; authoritative checks are never skipped.
    HandleResolver(SharedMemoryRegion& region,
                   const AllocatorMetadataProvider& allocator);
    HandleResolver(SharedMemoryRegion& region,
                   const AllocatorMetadataProvider* allocator);

    // Resolves a handle to a mutable object pointer. Requires the object's
    // SlabObjectState to be kAllocated or kBuilding AND the object's owner to
    // be the current process (design doc 7.2). The returned address is the
    // allocator-provided payload offset after complete metadata validation.
    template <typename T>
    Result<T*> ResolveMutable(ShmHandle handle, TypeId expected) {
        return ResolveMutableInternal(handle, expected, /*expected_schema=*/0,
                                      sizeof(T), alignof(T))
            .AndThenCast<T>();
    }

    // Mutable resolution that additionally asserts the schema short id.
    template <typename T>
    Result<T*> ResolveMutable(ShmHandle handle, TypeId expected,
                              uint64_t expected_schema_short_id) {
        return ResolveMutableInternal(handle, expected, expected_schema_short_id,
                                      sizeof(T), alignof(T))
            .AndThenCast<T>();
    }

    // Resolves a handle to a read-only object pointer. Requires the object's
    // SlabObjectState to be kPublished (design doc 7.2).
    template <typename T>
    Result<const T*> Resolve(ShmHandle handle, TypeId expected) const {
        return ResolveInternal(handle, expected, /*expected_schema=*/0,
                               sizeof(T), alignof(T))
            .AndThenConstCast<T>();
    }

    // Read-only resolution that additionally asserts the schema short id.
    template <typename T>
    Result<const T*> Resolve(ShmHandle handle, TypeId expected,
                             uint64_t expected_schema_short_id) const {
        return ResolveInternal(handle, expected, expected_schema_short_id,
                               sizeof(T), alignof(T))
            .AndThenConstCast<T>();
    }

private:
    // Validated object-address results with a typed-cast helper. These wrap a
    // raw (already-validated) address so the templates above can cast it to
    // the requested type without re-running validation.
    struct MutableAddress {
        Result<void*> result;
        template <typename T>
        Result<T*> AndThenCast() const {
            if (!result.ok()) {
                return result.status();
            }
            return static_cast<T*>(result.value());
        }
    };
    struct ConstAddress {
        Result<const void*> result;
        template <typename T>
        Result<const T*> AndThenConstCast() const {
            if (!result.ok()) {
                return result.status();
            }
            return static_cast<const T*>(result.value());
        }
    };

    // Full validation for mutable access (defined in handle_resolver.cc).
    MutableAddress ResolveMutableInternal(ShmHandle handle, TypeId expected,
                                          uint64_t expected_schema_short_id,
                                          uint64_t type_size,
                                          uint64_t type_alignment);
    // Full validation for read-only access (defined in handle_resolver.cc).
    ConstAddress ResolveInternal(ShmHandle handle, TypeId expected,
                                 uint64_t expected_schema_short_id,
                                 uint64_t type_size,
                                 uint64_t type_alignment) const;

    SharedMemoryRegion* region_;
    const AllocatorMetadataProvider* allocator_;
    uint32_t attached_region_id_ = 0;
    uint64_t attached_region_uuid_lo_ = 0;
    uint64_t attached_region_uuid_hi_ = 0;
    uint64_t attached_region_epoch_ = 0;
};

}  // namespace mino

#endif  // MINO_SHM_REGION_HANDLE_RESOLVER_H_
