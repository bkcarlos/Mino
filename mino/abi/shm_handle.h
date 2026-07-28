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

#ifndef MINO_ABI_SHM_HANDLE_H_
#define MINO_ABI_SHM_HANDLE_H_

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace mino {

// ---------------------------------------------------------------------------
// ShmHandle (ADR-0002, design doc section 7.1)
// ---------------------------------------------------------------------------
//
// A 128-bit, Offset-based reference to an object living in a shared-memory
// Region. Cross-process mappings have different base addresses, so all shared
// references use a relative Offset; a raw pointer is never stored.
//
//   offset      : 64-bit byte offset of the object within the Region. A value
//                 of 0 denotes the null Handle (offset 0 is the SuperBlock and
//                 is never a valid object).
//   generation  : 32-bit generation, detects stale references after a Slab
//                 slot is recycled (ABA). Must be drained/migrated before
//                 wraparound (ADR-0002).
//   region_id   : 32-bit persistent Region identifier; never reused within a
//                 deployment identity domain (ADR-0002).
struct alignas(8) ShmHandle {
    uint64_t offset = 0;
    uint32_t generation = 0;
    uint32_t region_id = 0;

    // A Handle is null iff its offset is 0 (design doc 7.1 / 7.2).
    constexpr bool IsNull() const noexcept { return offset == 0; }

    friend constexpr bool operator==(ShmHandle lhs, ShmHandle rhs) noexcept {
        return lhs.offset == rhs.offset && lhs.generation == rhs.generation &&
               lhs.region_id == rhs.region_id;
    }
    friend constexpr bool operator!=(ShmHandle lhs, ShmHandle rhs) noexcept {
        return !(lhs == rhs);
    }
};

static_assert(sizeof(ShmHandle) == 16, "ShmHandle must be exactly 16 bytes");
static_assert(alignof(ShmHandle) == 8, "ShmHandle must be 8-byte aligned");
static_assert(offsetof(ShmHandle, offset) == 0);
static_assert(offsetof(ShmHandle, generation) == 8);
static_assert(offsetof(ShmHandle, region_id) == 12);
static_assert(std::is_trivially_copyable_v<ShmHandle>);
static_assert(std::is_standard_layout_v<ShmHandle>);



}  // namespace mino

#endif  // MINO_ABI_SHM_HANDLE_H_
