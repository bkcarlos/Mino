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

#ifndef MINO_SHM_REGION_SUPERBLOCK_H_
#define MINO_SHM_REGION_SUPERBLOCK_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "mino/common/checked_arithmetic.h"
#include "mino/platform/process_identity.h"

namespace mino {

// ---------------------------------------------------------------------------
// SuperBlock (design doc section 6.4)
// ---------------------------------------------------------------------------
//
// The SuperBlock is the fixed-layout header at offset 0 of every shared-memory
// Region. It is partitioned so that immutable, CRC-protected "disk-style"
// fields never share a cache line with high-frequency cross-process atomic
// fields (design doc section 6.4).
//
// Cross-process fields (region_epoch, clean_shutdown, state, recovery_*) are
// plain integers in the layout and are accessed exclusively through
// std::atomic_ref at runtime. This keeps the struct standard-layout (so
// offsetof/static_assert are well-defined) while still giving atomic access
// semantics on the shared mapping (design doc section 6.6). The object is
// explicitly constructed by the Region initialization flow (memset + field
// stores) before any other process maps it.
//
// All cross-process references inside the Region use Offset semantics; no raw
// pointers are stored here.

// Constants.
inline constexpr uint32_t kSuperBlockMagic = 0x4D494E4F;  // "MINO"
inline constexpr uint16_t kRegionLayoutVersion = 4;
inline constexpr uint16_t kOldestReadableRegionLayoutVersion = 2;
// Byte-order detector: stored at Create; a reader on a different-endian host
// observes a byte-swapped value and rejects the Region (first version only
// supports the native little-endian layout).
inline constexpr uint32_t kByteOrderNative = 0x01020304;
// Total SuperBlock size in bytes (4 cache lines).
inline constexpr uint32_t kSuperBlockSize = 256;
// Default page size assumed at Create; validated against the host at Attach.
inline constexpr uint32_t kDefaultPageSize = 4096;

// RegionState is the persisted lifecycle state (design doc section 6.1).
// ABSENT is implicit (no Region object exists), so it is not stored.
enum class RegionState : uint32_t {
    kInitializing = 1,
    kActive = 2,
    kClosed = 3,
    kDirty = 4,
    kRecovering = 5,
    kQuarantined = 6,
};

struct SuperBlock {
    // ---- Immutable Header partition (cache lines 0-1: bytes [0,128)) ----
    // Written once at Create and protected by immutable_crc32. Never modified
    // afterwards, so readers can validate it without synchronization.
    uint32_t magic;              // 0
    uint16_t layout_version;     // 4
    uint16_t header_size;        // 6
    uint64_t region_size;        // 8
    uint32_t byte_order;         // 16
    uint32_t page_size;          // 20
    uint64_t region_uuid_lo;     // 24
    uint64_t region_uuid_hi;     // 32
    uint64_t directory_offset;   // 40
    uint64_t allocator_offset;   // 48
    uint64_t data_offset;        // 56
    uint64_t data_size;          // 64
    uint32_t region_id;          // 72 (immutable; assigned at Create)
    uint32_t immutable_rsv0;     // 76
    uint32_t immutable_crc32;    // 80  CRC over bytes [0, 80)
    uint32_t immutable_rsv1;     // 84
    std::byte immutable_pad[40]; // 88..128

    // ---- Lifecycle Control partition (cache line 2: bytes [128,192)) ----
    // Cross-process atomics; access via the *Ref()/Load* helpers below.
    uint64_t region_epoch;        // 128
    uint32_t clean_shutdown;      // 136 (boolean)
    uint32_t state;               // 140 (RegionState)
    uint64_t recovery_lease_ns;   // 144 (monotonic lease expiry; the mutex)
    uint64_t recovery_epoch;      // 152 (takeover generation counter)
    ProcessIdentity recovery_owner;  // 160..192 (informational owner identity)

    // ---- Feature/Compatibility partition (cache line 3: bytes [192,256)) ----
    uint64_t feature_flags;          // 192
    uint32_t minimum_reader_version; // 200
    uint32_t compat_rsv;             // 204
    uint64_t recovery_fence_word;    // 208: atomic_ref<{epoch, phase}>.

    // v3 supervisor-owner metadata. v2 readers see these bytes as zeroed
    // compatibility padding. The ProcessIdentity is read/written as four
    // atomic uint64_t fields, and service_fence_word is the publication and
    // lifecycle fencing record.
    ProcessIdentity service_owner;   // 216..248
    uint64_t service_fence_word;     // 248: atomic_ref<{epoch, phase}>.
};

// ---- Layout pinning (shared-memory ABI must be fixed) ----
static_assert(sizeof(SuperBlock) == kSuperBlockSize,
              "SuperBlock must be exactly 256 bytes");
static_assert(alignof(SuperBlock) == 8, "SuperBlock must be 8-byte aligned");
static_assert(std::is_standard_layout_v<SuperBlock>);
static_assert(std::is_trivially_copyable_v<SuperBlock>);

static_assert(offsetof(SuperBlock, magic) == 0);
static_assert(offsetof(SuperBlock, layout_version) == 4);
static_assert(offsetof(SuperBlock, header_size) == 6);
static_assert(offsetof(SuperBlock, region_size) == 8);
static_assert(offsetof(SuperBlock, byte_order) == 16);
static_assert(offsetof(SuperBlock, page_size) == 20);
static_assert(offsetof(SuperBlock, region_uuid_lo) == 24);
static_assert(offsetof(SuperBlock, region_uuid_hi) == 32);
static_assert(offsetof(SuperBlock, directory_offset) == 40);
static_assert(offsetof(SuperBlock, allocator_offset) == 48);
static_assert(offsetof(SuperBlock, data_offset) == 56);
static_assert(offsetof(SuperBlock, data_size) == 64);
static_assert(offsetof(SuperBlock, region_id) == 72);
static_assert(offsetof(SuperBlock, immutable_crc32) == 80);
// Cross-partition boundaries must land on cache-line boundaries.
static_assert(offsetof(SuperBlock, region_epoch) == 128);
static_assert(offsetof(SuperBlock, clean_shutdown) == 136);
static_assert(offsetof(SuperBlock, state) == 140);
static_assert(offsetof(SuperBlock, recovery_lease_ns) == 144);
static_assert(offsetof(SuperBlock, recovery_epoch) == 152);
static_assert(offsetof(SuperBlock, recovery_owner) == 160);
static_assert(offsetof(SuperBlock, feature_flags) == 192);
static_assert(offsetof(SuperBlock, minimum_reader_version) == 200);
static_assert(offsetof(SuperBlock, recovery_fence_word) == 208);
static_assert(offsetof(SuperBlock, service_owner) == 216);
static_assert(offsetof(SuperBlock, service_fence_word) == 248);
static_assert(offsetof(SuperBlock, recovery_fence_word) %
                  std::atomic_ref<uint64_t>::required_alignment ==
              0);
static_assert(offsetof(SuperBlock, service_fence_word) %
                  std::atomic_ref<uint64_t>::required_alignment ==
              0);

// ---------------------------------------------------------------------------
// CRC32 (IEEE 802.3, reflected, polynomial 0xEDB88320)
// ---------------------------------------------------------------------------
//
// Used to protect the SuperBlock immutable header and (in later milestones)
// Slab immutable headers. The 256-entry table is built once per process on
// first use (thread-safe via a function-local static initializer).

namespace detail {

// Builds and returns the CRC32 lookup table. Constructed exactly once
// (thread-safe function-local static).
inline const uint32_t* Crc32Table() {
    struct Table {
        uint32_t entries[256];
        Table() {
            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t c = i;
                for (int k = 0; k < 8; ++k) {
                    c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
                }
                entries[i] = c;
            }
        }
    };
    static const Table table;
    return table.entries;
}

}  // namespace detail

// Computes CRC32 over [data, data+len) with the standard seed/xorout.
inline uint32_t Crc32(const void* data, size_t len, uint32_t seed = 0) {
    const uint32_t* table = detail::Crc32Table();
    const auto* p = static_cast<const unsigned char*>(data);
    uint32_t crc = seed ^ 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ p[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

// ---------------------------------------------------------------------------
// SuperBlock helpers
// ---------------------------------------------------------------------------

// Computes the CRC over the immutable header fields [0, immutable_crc32).
// This range is fixed and excludes the CRC field itself and all mutable
// lifecycle/feature fields.
inline uint32_t SuperBlockImmutableCrc(const SuperBlock& sb) {
    return Crc32(&sb, offsetof(SuperBlock, immutable_crc32));
}

// Cross-process atomic accessors. Loads are safe on a const SuperBlock (they
// only read); stores require a mutable SuperBlock mapped read-write.
inline uint64_t LoadRegionEpoch(const SuperBlock& sb) {
    return std::atomic_ref(const_cast<uint64_t&>(sb.region_epoch))
        .load(std::memory_order_acquire);
}
inline void StoreRegionEpoch(SuperBlock& sb, uint64_t v) {
    std::atomic_ref(sb.region_epoch).store(v, std::memory_order_release);
}

inline uint32_t LoadState(const SuperBlock& sb) {
    return std::atomic_ref(const_cast<uint32_t&>(sb.state))
        .load(std::memory_order_acquire);
}
inline void StoreState(SuperBlock& sb, RegionState v) {
    std::atomic_ref(sb.state)
        .store(static_cast<uint32_t>(v), std::memory_order_release);
}
inline RegionState LoadRegionState(const SuperBlock& sb) {
    return static_cast<RegionState>(LoadState(sb));
}

inline bool LoadCleanShutdown(const SuperBlock& sb) {
    return std::atomic_ref(const_cast<uint32_t&>(sb.clean_shutdown))
               .load(std::memory_order_acquire) != 0;
}
inline void StoreCleanShutdown(SuperBlock& sb, bool v) {
    std::atomic_ref(sb.clean_shutdown)
        .store(v ? 1u : 0u, std::memory_order_release);
}

inline uint64_t LoadRecoveryLeaseNs(const SuperBlock& sb) {
    return std::atomic_ref(const_cast<uint64_t&>(sb.recovery_lease_ns))
        .load(std::memory_order_acquire);
}
inline uint64_t LoadRecoveryEpoch(const SuperBlock& sb) {
    return std::atomic_ref(const_cast<uint64_t&>(sb.recovery_epoch))
        .load(std::memory_order_acquire);
}

// recovery_fence_word is the authoritative recovery commit record. The state
// field remains the externally visible lifecycle mirror, but RECOVERING commit
// and takeover are linearized by CAS on this single word.
enum class RecoveryFencePhase : uint64_t {
    kUnset = 0,
    kRecovering = 1,
    kActive = 2,
    kQuarantined = 3,
};
inline constexpr uint64_t kRecoveryFencePhaseBits = 2;
inline constexpr uint64_t kMaxRecoveryFenceEpoch =
    std::numeric_limits<uint64_t>::max() >> kRecoveryFencePhaseBits;

constexpr uint64_t EncodeRecoveryFence(uint64_t epoch,
                                       RecoveryFencePhase phase) {
    return (epoch << kRecoveryFencePhaseBits) |
           static_cast<uint64_t>(phase);
}
constexpr uint64_t RecoveryFenceEpoch(uint64_t word) {
    return word >> kRecoveryFencePhaseBits;
}
constexpr RecoveryFencePhase RecoveryFencePhaseOf(uint64_t word) {
    return static_cast<RecoveryFencePhase>(
        word & ((uint64_t{1} << kRecoveryFencePhaseBits) - 1));
}
inline uint64_t LoadRecoveryFence(const SuperBlock& sb) {
    return std::atomic_ref(const_cast<uint64_t&>(sb.recovery_fence_word))
        .load(std::memory_order_acquire);
}
inline void StoreRecoveryFence(SuperBlock& sb, uint64_t word) {
    std::atomic_ref(sb.recovery_fence_word)
        .store(word, std::memory_order_release);
}
inline bool CompareExchangeRecoveryFence(SuperBlock& sb, uint64_t* expected,
                                         uint64_t desired) {
    return std::atomic_ref(sb.recovery_fence_word)
        .compare_exchange_strong(*expected, desired,
                                 std::memory_order_acq_rel,
                                 std::memory_order_acquire);
}

// The v3 service fence protects lifecycle ownership, not ordinary business
// object ownership. A host-local advisory lock excludes concurrent writable
// supervisors; this generation token prevents stale Region objects from
// publishing CLOSED after ownership has changed.
enum class ServiceFencePhase : uint64_t {
    kUnowned = 0,
    kOwned = 1,
    kClosing = 2,
};
inline constexpr uint64_t kServiceFencePhaseBits = 2;
inline constexpr uint64_t kMaxServiceFenceEpoch =
    std::numeric_limits<uint64_t>::max() >> kServiceFencePhaseBits;

constexpr uint64_t EncodeServiceFence(uint64_t epoch,
                                      ServiceFencePhase phase) {
    return (epoch << kServiceFencePhaseBits) |
           static_cast<uint64_t>(phase);
}
constexpr uint64_t ServiceFenceEpoch(uint64_t word) {
    return word >> kServiceFencePhaseBits;
}
constexpr ServiceFencePhase ServiceFencePhaseOf(uint64_t word) {
    return static_cast<ServiceFencePhase>(
        word & ((uint64_t{1} << kServiceFencePhaseBits) - 1));
}
inline uint64_t LoadServiceFence(const SuperBlock& sb) {
    return std::atomic_ref(const_cast<uint64_t&>(sb.service_fence_word))
        .load(std::memory_order_acquire);
}
inline void StoreServiceFence(SuperBlock& sb, uint64_t word) {
    std::atomic_ref(sb.service_fence_word)
        .store(word, std::memory_order_release);
}
inline bool CompareExchangeServiceFence(SuperBlock& sb, uint64_t* expected,
                                        uint64_t desired) {
    return std::atomic_ref(sb.service_fence_word)
        .compare_exchange_strong(*expected, desired,
                                 std::memory_order_acq_rel,
                                 std::memory_order_acquire);
}

inline ProcessIdentity LoadServiceOwner(const SuperBlock& sb) {
    ProcessIdentity owner;
    owner.node_id =
        std::atomic_ref(const_cast<uint64_t&>(sb.service_owner.node_id))
            .load(std::memory_order_relaxed);
    owner.process_id =
        std::atomic_ref(const_cast<uint64_t&>(sb.service_owner.process_id))
            .load(std::memory_order_relaxed);
    owner.process_epoch =
        std::atomic_ref(const_cast<uint64_t&>(sb.service_owner.process_epoch))
            .load(std::memory_order_relaxed);
    owner.start_time_ns =
        std::atomic_ref(const_cast<uint64_t&>(sb.service_owner.start_time_ns))
            .load(std::memory_order_relaxed);
    return owner;
}
inline void StoreServiceOwner(SuperBlock& sb,
                              const ProcessIdentity& owner) {
    std::atomic_ref(sb.service_owner.node_id)
        .store(owner.node_id, std::memory_order_relaxed);
    std::atomic_ref(sb.service_owner.process_id)
        .store(owner.process_id, std::memory_order_relaxed);
    std::atomic_ref(sb.service_owner.process_epoch)
        .store(owner.process_epoch, std::memory_order_relaxed);
    std::atomic_ref(sb.service_owner.start_time_ns)
        .store(owner.start_time_ns, std::memory_order_relaxed);
}

}  // namespace mino

#endif  // MINO_SHM_REGION_SUPERBLOCK_H_
