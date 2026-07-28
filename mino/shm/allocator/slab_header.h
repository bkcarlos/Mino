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

// Slab Header v1 and Object State definitions for the Central Slab Allocator.
// See docs/Mino_详细设计文档.md section 8.1.

#ifndef MINO_SHM_ALLOCATOR_SLAB_HEADER_H_
#define MINO_SHM_ALLOCATOR_SLAB_HEADER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace mino {

// SlabHeaderMagic identifies a live Slab Header inside the data region
// ("MSLB" little-endian).
inline constexpr uint32_t kSlabHeaderMagic = 0x4D534C42u;

// Version of the SlabHeader layout described by this header.
inline constexpr uint16_t kSlabHeaderVersion = 1;

// ObjectState is the publication state machine of a single slot
// (design doc 8.1). The allocator publishes an allocation exclusively via
// object_state.store(kAllocated, memory_order_release) (design doc 8.3
// step 8). The values are part of the shared-memory ABI and must remain
// stable.
enum class ObjectState : uint32_t {
    kFree = 0,       // Slot is not in use (bitmap clear).
    kAllocated = 1,  // Allocation published; object may be built.
    kBuilding = 2,   // Dynamic object construction in progress.
    kPublished = 3,  // Object visible to readers (Resolve requires this).
    kRetired = 4,    // No new Borrow is produced; awaiting final Reclaim.
    kAborting = 5,   // Allocation/construction is being torn down.
};

// Compatibility name used by the resolver; ObjectState is the authoritative
// allocator ABI type.
using SlabObjectState = ObjectState;

// SlabHeader v1 (design doc 8.1). Exactly one 64-byte cache line so that the
// atomic publication word (object_state) does not share a line with any other
// slot's state.
struct alignas(64) SlabHeader {
    uint32_t magic;
    uint16_t header_version;
    uint16_t class_id;

    uint32_t generation;
    std::atomic<uint32_t> object_state;

    uint32_t capacity;
    uint32_t object_size;
    uint32_t type_id;
    uint32_t layout_version;

    uint64_t schema_short_id;
    uint64_t owner_epoch;
    uint64_t allocation_transaction_id;
    uint32_t immutable_header_crc;
    uint32_t reserved;
};

static_assert(sizeof(SlabHeader) == 64, "SlabHeader must occupy one cache line");
static_assert(alignof(SlabHeader) == 64, "SlabHeader must be cache-line aligned");
static_assert(offsetof(SlabHeader, magic) == 0);
static_assert(offsetof(SlabHeader, header_version) == 4);
static_assert(offsetof(SlabHeader, class_id) == 6);
static_assert(offsetof(SlabHeader, generation) == 8);
static_assert(offsetof(SlabHeader, object_state) == 12);
static_assert(offsetof(SlabHeader, capacity) == 16);
static_assert(offsetof(SlabHeader, object_size) == 20);
static_assert(offsetof(SlabHeader, type_id) == 24);
static_assert(offsetof(SlabHeader, layout_version) == 28);
static_assert(offsetof(SlabHeader, schema_short_id) == 32);
static_assert(offsetof(SlabHeader, owner_epoch) == 40);
static_assert(offsetof(SlabHeader, allocation_transaction_id) == 48);
static_assert(offsetof(SlabHeader, immutable_header_crc) == 56);
static_assert(offsetof(SlabHeader, reserved) == 60);

// Returns the wire (little-endian) encoding of v for CRC computation. On the
// supported little-endian targets this is the identity; the explicit form
// documents that the CRC is defined over a fixed byte order independent of
// the host.
constexpr uint32_t CrcWire32(uint32_t v) { return v; }
constexpr uint16_t CrcWire16(uint16_t v) { return v; }
constexpr uint64_t CrcWire64(uint64_t v) { return v; }

// Computes a CRC-32 (Castagnoli, iSCSI polynomial 0x1EDC6F41, reflected)
// over the immutable Slab Header fields, as defined in design doc 8.1:
//
//   covered:      magic, header_version, class_id, generation, capacity,
//                 object_size, type_id, layout_version, schema_short_id
//   not covered:  immutable_header_crc itself, object_state, owner_epoch,
//                 allocation_transaction_id, reserved
//
// Field order follows the struct declaration. The CRC is calculated before
// Publish, after the object fields are frozen; it allows recovery and
// Inspect to detect corruption of the fields that must never change while an
// allocation is alive.
constexpr uint32_t ComputeImmutableHeaderCrc(const SlabHeader& h) noexcept {
    // Reflected CRC-32C table driven implementation, constexpr-friendly.
    constexpr uint32_t kPoly = 0x82F63B78u;  // Reflected CRC-32C polynomial.

    uint32_t crc = 0xFFFFFFFFu;
    auto add_byte = [&crc](uint8_t byte) constexpr {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ ((crc & 1u) != 0u ? kPoly : 0u);
        }
    };
    auto add_u32 = [&add_byte](uint32_t v) constexpr {
        v = CrcWire32(v);
        for (int i = 0; i < 4; ++i) {
            add_byte(static_cast<uint8_t>(v & 0xFFu));
            v >>= 8;
        }
    };
    auto add_u16 = [&add_byte](uint16_t v) constexpr {
        v = CrcWire16(v);
        for (int i = 0; i < 2; ++i) {
            add_byte(static_cast<uint8_t>(v & 0xFFu));
            v >>= 8;
        }
    };
    auto add_u64 = [&add_byte](uint64_t v) constexpr {
        v = CrcWire64(v);
        for (int i = 0; i < 8; ++i) {
            add_byte(static_cast<uint8_t>(v & 0xFFu));
            v >>= 8;
        }
    };

    add_u32(h.magic);
    add_u16(h.header_version);
    add_u16(h.class_id);
    add_u32(h.generation);
    add_u32(h.capacity);
    add_u32(h.object_size);
    add_u32(h.type_id);
    add_u32(h.layout_version);
    add_u64(h.schema_short_id);

    return crc ^ 0xFFFFFFFFu;
}

// Validates the Slab Header invariants that can be checked without allocator
// context (design doc 8.1):
//   - magic and header_version match this implementation;
//   - immutable_header_crc matches the immutable fields.
// Returns true iff all invariants hold. Does not inspect object_state,
// Owner/Transaction fields or the bitmap/generation arrays.
inline SlabObjectState LoadObjectState(const SlabHeader& h) noexcept {
    return static_cast<SlabObjectState>(
        h.object_state.load(std::memory_order_acquire));
}

inline bool VerifyImmutableHeader(const SlabHeader& h) noexcept {
    if (h.magic != kSlabHeaderMagic) {
        return false;
    }
    if (h.header_version != kSlabHeaderVersion) {
        return false;
    }
    return h.immutable_header_crc == ComputeImmutableHeaderCrc(h);
}

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_SLAB_HEADER_H_
