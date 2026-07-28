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

// Authoritative generation array for the Central Slab Allocator.
// See docs/Mino_详细设计文档.md sections 8.1 and 8.3.

#ifndef MINO_SHM_ALLOCATOR_GENERATION_ARRAY_H_
#define MINO_SHM_ALLOCATOR_GENERATION_ARRAY_H_

#include <atomic>
#include <cstdint>
#include <limits>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {

// Returned by GenerationArray::Increment when the slot's generation has
// reached UINT32_MAX. Per design doc 8.3 step 6, the owning Class/Region
// must be marked DRAINING and the generation must never wrap to zero.
inline constexpr uint32_t kGenerationDraining = std::numeric_limits<uint32_t>::max();

// GenerationArray is the authoritative per-slot generation store
// (design doc 8.1: "Generation Array 是空闲/占用切换期间的权威代数").
//
// On every allocation the slot's generation is incremented and copied into
// the Slab Header; the Resolver compares Handle generation against this
// array to detect stale references (ABA). Stored in shared memory as a flat
// std::atomic<uint32_t> array; index 0 corresponds to bitmap bit 0.
class GenerationArray {
public:
    // Initializes the array over `slot_count` entries located at `storage`
    // (shared memory, already zero-initialized).
    // Fails with kInvalidArgument if slot_count is zero or storage is null.
    static Result<GenerationArray> Create(std::atomic<uint32_t>* storage,
                                          uint32_t slot_count);

    // Increments the generation of `slot_index` and returns the new value
    // (design doc 8.3 step 6). Uses fetch_add(relaxed): the caller serializes
    // slot ownership via the bitmap CAS, and publication happens through
    // SlabHeader::object_state (release), so no additional ordering is
    // needed here.
    //
    // If the slot's generation has reached UINT32_MAX, returns
    // kGenerationDraining and leaves the value untouched: generations must
    // never wrap. The caller must mark the Class/Region DRAINING and fail
    // the allocation.
    uint32_t Increment(uint32_t slot_index);

    // Returns the current authoritative generation of `slot_index`
    // (acquire), or 0 for out-of-range indexes.
    uint32_t Get(uint32_t slot_index) const;

    uint32_t slot_count() const { return slot_count_; }

    // Default-constructible so facades can hold GenerationArray members;
    // Create() is the only supported way to build a usable array.
    GenerationArray() = default;

private:
    std::atomic<uint32_t>* storage_ = nullptr;
    uint32_t slot_count_ = 0;
};

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_GENERATION_ARRAY_H_
