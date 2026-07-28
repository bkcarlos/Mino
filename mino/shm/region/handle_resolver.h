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

// AllocatorMetadataProvider abstracts the allocator-owned metadata that
// HandleResolver must consult but does not own (design doc 7.2: Slab bitmap
// occupancy and the authoritative Generation Array). The Central Slab
// Allocator (D1-07) provides the production implementation; tests inject a
// fake. This indirection keeps the resolver independent of the allocator's
// internal layout.
class AllocatorMetadataProvider {
public:
    virtual ~AllocatorMetadataProvider() = default;

    // Returns true if the slot at `offset` (a SlabHeader offset within the
    // Region's data area) is marked occupied in the allocation bitmap.
    virtual bool IsSlotOccupied(uint64_t offset) const = 0;

    // Returns the authoritative generation for the slot at `offset` from the
    // Generation Array (design doc 8.1). The resolver compares this against
    // both the Handle's generation and the SlabHeader's stored generation.
    virtual uint32_t AuthoritativeGeneration(uint64_t offset) const = 0;
};

// HandleResolver safely dereferences ShmHandles into typed pointers within an
// attached Region (design doc section 7.2).
//
// Every dereference validates the full checklist of section 7.2 before
// returning a pointer; the raw `region_base + offset` is never exposed without
// validation.
class HandleResolver {
public:
    // `region` must outlive the resolver. `allocator` may be nullptr, in which
    // case the allocator-owned checks (bitmap occupancy, authoritative
    // generation) are skipped — acceptable only during bring-up; production
    // wiring must always supply the provider.
    HandleResolver(SharedMemoryRegion& region,
                   const AllocatorMetadataProvider* allocator)
        : region_(&region), allocator_(allocator) {}

    // Resolves a handle to a mutable object pointer. Requires the object's
    // SlabObjectState to be kAllocated or kBuilding AND the object's owner to
    // be the current process (design doc 7.2). The returned object is the
    // payload immediately following the SlabHeader.
    template <typename T>
    Result<T*> ResolveMutable(ShmHandle handle, TypeId expected) {
        return ResolveMutableInternal(handle, expected, /*expected_schema=*/0)
            .AndThenCast<T>();
    }

    // Mutable resolution that additionally asserts the schema short id.
    template <typename T>
    Result<T*> ResolveMutable(ShmHandle handle, TypeId expected,
                              uint64_t expected_schema_short_id) {
        return ResolveMutableInternal(handle, expected, expected_schema_short_id)
            .AndThenCast<T>();
    }

    // Resolves a handle to a read-only object pointer. Requires the object's
    // SlabObjectState to be kPublished (design doc 7.2).
    template <typename T>
    Result<const T*> Resolve(ShmHandle handle, TypeId expected) const {
        return ResolveInternal(handle, expected, /*expected_schema=*/0)
            .AndThenConstCast<T>();
    }

    // Read-only resolution that additionally asserts the schema short id.
    template <typename T>
    Result<const T*> Resolve(ShmHandle handle, TypeId expected,
                             uint64_t expected_schema_short_id) const {
        return ResolveInternal(handle, expected, expected_schema_short_id)
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
                                          uint64_t expected_schema_short_id);
    // Full validation for read-only access (defined in handle_resolver.cc).
    ConstAddress ResolveInternal(ShmHandle handle, TypeId expected,
                                 uint64_t expected_schema_short_id) const;

    SharedMemoryRegion* region_;
    const AllocatorMetadataProvider* allocator_;
};

}  // namespace mino

#endif  // MINO_SHM_REGION_HANDLE_RESOLVER_H_
