// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SHM_REGION_RECOVERY_DIRECTORY_H_
#define MINO_SHM_REGION_RECOVERY_DIRECTORY_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {

inline constexpr uint32_t kRecoveryDirectoryMagic = 0x4D524431u;  // "MRD1"
inline constexpr uint16_t kRecoveryDirectoryVersion = 2;
inline constexpr uint32_t kRecoveryDirectoryResourceCapacity = 32;
inline constexpr uint32_t kRecoveryDirectoryReferenceCapacity = 128;

enum class RecoveryResourceKind : uint32_t {
    kCentralAllocator = 1,
    kLargeObjectPool = 2,
    kChannelAckSource = 3,
    kPinCleanupParticipant = 4,
};

inline constexpr uint32_t kRecoveryResourceRequired = 1u << 0;
inline constexpr uint32_t kRecoveryResourceFlagMask =
    kRecoveryResourceRequired;
inline constexpr uint32_t kRecoveryDirectoryReferencesComplete = 1u << 0;
inline constexpr uint32_t kRecoveryDirectoryFlagMask =
    kRecoveryDirectoryReferencesComplete;

// Persistent resource descriptor. Every address is an offset from the Region
// base. The generic element layout is used only by generation-scoped ACK/Pin
// resources; allocator resources leave those fields zero.
struct RecoveryResourceDescriptor {
    uint32_t resource_id = 0;
    uint32_t kind = 0;
    uint32_t format_version = 1;
    uint32_t flags = 0;

    uint64_t offset = 0;
    uint64_t size = 0;
    uint64_t generation = 0;
    uint64_t element_count = 0;

    uint32_t element_stride = 0;
    uint32_t generation_offset = 0;
    uint32_t value_offset = 0;
    uint32_t reserved0 = 0;

    uint64_t control_offset = 0;
    uint64_t control_size = 0;
    uint64_t reserved1 = 0;
    uint64_t reserved2 = 0;
};
static_assert(sizeof(RecoveryResourceDescriptor) == 96);
static_assert(std::is_trivially_copyable_v<RecoveryResourceDescriptor>);

// A complete reference set identifies allocator units, not process addresses.
// For CentralSlabAllocator unit_index is the global slot index; for
// LargeObjectPool it is the first segment index. Generation prevents a stale
// reference from retaining a reused unit.
struct RecoveryObjectReference {
    uint32_t resource_id = 0;
    uint32_t unit_index = 0;
    uint32_t generation = 0;
    uint32_t reserved = 0;
};
static_assert(sizeof(RecoveryObjectReference) == 16);
static_assert(std::is_trivially_copyable_v<RecoveryObjectReference>);

// Wire layouts for generation-scoped cleanup resources. The fields are plain
// integers and are accessed through atomic_ref by recovery and participants.
struct RecoveryGenerationControl {
    uint64_t generation = 0;
    uint64_t live_mask = 0;  // Used by ACK sources; zero for Pin participants.
};
struct RecoveryGenerationValue {
    uint64_t generation = 0;
    uint64_t value = 0;  // ACK bitmap or Pin count.
};
static_assert(sizeof(RecoveryGenerationControl) == 16);
static_assert(sizeof(RecoveryGenerationValue) == 16);

struct alignas(64) RecoveryDirectoryControl {
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t header_size = 0;
    uint32_t snapshot_size = 0;
    uint32_t resource_capacity = 0;
    uint32_t reference_capacity = 0;
    uint32_t reserved0 = 0;
    uint32_t immutable_crc32 = 0;
    uint32_t reserved1 = 0;
    uint64_t published_word = 0;  // {sequence, slot}; atomic_ref publication.
    std::byte padding[24]{};
};
static_assert(sizeof(RecoveryDirectoryControl) == 64);
static_assert(offsetof(RecoveryDirectoryControl, published_word) == 32);

struct alignas(64) RecoveryDirectorySnapshot {
    uint64_t sequence = 0;
    uint32_t resource_count = 0;
    uint32_t reference_count = 0;
    uint32_t flags = 0;
    uint32_t reserved0 = 0;

    // Per-slot seqlock publication word. Stable values are sequence << 1;
    // odd values mean a writer is replacing this slot. Snapshot payload bytes
    // are transferred atomically, so a reader that observes the same stable
    // word before and after its copy has a race-free, untorn snapshot.
    union {
        uint64_t publication_word = 0;
        uint64_t reserved1;  // v1 source-compatibility alias.
    };
    RecoveryResourceDescriptor resources[kRecoveryDirectoryResourceCapacity]{};
    RecoveryObjectReference references[kRecoveryDirectoryReferenceCapacity]{};
    uint32_t crc32 = 0;
    uint32_t reserved2 = 0;
};
static_assert(std::is_trivially_copyable_v<RecoveryDirectorySnapshot>);
static_assert(offsetof(RecoveryDirectorySnapshot, publication_word) == 24);
static_assert(offsetof(RecoveryDirectorySnapshot, resources) == 32);
static_assert(offsetof(RecoveryDirectorySnapshot, crc32) == 5152);
static_assert(sizeof(RecoveryDirectorySnapshot) == 5184);
static_assert(ATOMIC_CHAR_LOCK_FREE == 2,
              "RecoveryDirectory requires lock-free byte atomics");
static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
              "RecoveryDirectory requires lock-free 64-bit atomics");

struct alignas(64) RecoveryDirectoryImage {
    RecoveryDirectoryControl control;
    RecoveryDirectorySnapshot snapshots[2];
};
static_assert(sizeof(RecoveryDirectoryImage) == 10432);

inline constexpr uint64_t kRecoveryDirectoryMinimumSize =
    sizeof(RecoveryDirectoryImage);

Status InitializeRecoveryDirectory(void* directory_base,
                                   uint64_t directory_size);
Result<RecoveryDirectorySnapshot> ReadRecoveryDirectory(
    const void* directory_base, uint64_t directory_size);
Status PublishRecoveryResource(void* directory_base, uint64_t directory_size,
                               uint64_t region_size,
                               const RecoveryResourceDescriptor& descriptor);
Status PublishRecoveryReferences(
    void* directory_base, uint64_t directory_size,
    std::span<const RecoveryObjectReference> references, bool complete);
Status ValidateRecoveryResourceDescriptor(
    const RecoveryResourceDescriptor& descriptor, uint64_t region_size);

}  // namespace mino

#endif  // MINO_SHM_REGION_RECOVERY_DIRECTORY_H_
