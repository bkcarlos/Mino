// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_SHM_REGION_ATTACHMENT_DIRECTORY_H_
#define MINO_SHM_REGION_ATTACHMENT_DIRECTORY_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/region/channel_directory.h"

namespace mino {

inline constexpr uint32_t kAttachmentDirectoryMagic = 0x4D414431u;  // "MAD1"
inline constexpr uint16_t kAttachmentDirectoryVersion = 1;
inline constexpr uint32_t kAttachmentDirectorySlotCapacity = 16;
// Hard policy for subordinate writable Attach. Additional unsupervised writers
// (without request_subordinate_writable) remain rejected by the supervisor lock.
inline constexpr uint32_t kMaxSubordinateWritableAttachments = 4;

enum class AttachmentSlotState : uint64_t {
    kFree = 0,
    kClaiming = 1,
    kLive = 2,
    kClosing = 3,
};

enum class AttachmentRole : uint32_t {
    kNone = 0,
    kSupervisor = 1,
    kSubordinateWritable = 2,
    kReader = 3,
};

inline constexpr uint64_t kAttachmentSlotStateBits = 2;
inline constexpr uint64_t kMaxAttachmentSlotGeneration =
    std::numeric_limits<uint64_t>::max() >> kAttachmentSlotStateBits;

constexpr uint64_t EncodeAttachmentStateWord(uint64_t generation,
                                             AttachmentSlotState state) {
    return (generation << kAttachmentSlotStateBits) |
           static_cast<uint64_t>(state);
}
constexpr uint64_t AttachmentStateGeneration(uint64_t word) {
    return word >> kAttachmentSlotStateBits;
}
constexpr AttachmentSlotState AttachmentStateOf(uint64_t word) {
    return static_cast<AttachmentSlotState>(
        word & ((uint64_t{1} << kAttachmentSlotStateBits) - 1));
}

// Crash-safe attachment registry entry. Layout v7 places a bounded directory of
// these slots in the Region directory sub-region (ADR-0014). Reclaim of a slot
// requires ProbeProcessIdentity == Dead (or Free/zero Claiming); Alive/Unknown
// never authorize destructive recovery.
struct alignas(64) AttachmentSlot {
    uint64_t state_word = 0;  // {generation, state}; atomic_ref access.
    uint32_t role = 0;        // AttachmentRole
    uint32_t flags = 0;
    ProcessIdentity identity{};
    uint64_t heartbeat_ns = 0;         // CLOCK_MONOTONIC; diagnostic/liveness aid
    uint64_t attach_service_epoch = 0; // supervisor service epoch at claim
    std::byte padding[64]{};
};
static_assert(sizeof(AttachmentSlot) == 128);
static_assert(offsetof(AttachmentSlot, state_word) == 0);
static_assert(offsetof(AttachmentSlot, identity) == 16);
static_assert(std::is_trivially_copyable_v<AttachmentSlot>);

struct alignas(64) AttachmentDirectoryControl {
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t header_size = 0;
    uint32_t slot_capacity = 0;
    uint32_t max_subordinate_writable = 0;
    uint32_t immutable_crc32 = 0;
    uint32_t reserved0 = 0;
    std::byte padding[40]{};
};
static_assert(sizeof(AttachmentDirectoryControl) == 64);
static_assert(offsetof(AttachmentDirectoryControl, immutable_crc32) == 16);

struct alignas(64) AttachmentDirectoryImage {
    AttachmentDirectoryControl control;
    AttachmentSlot slots[kAttachmentDirectorySlotCapacity];
};
static_assert(sizeof(AttachmentDirectoryImage) ==
              64 + 128 * kAttachmentDirectorySlotCapacity);

inline constexpr uint64_t kAttachmentDirectoryMinimumSize =
    sizeof(AttachmentDirectoryImage);

// Appended after the Channel Directory inside the reserved directory sub-region.
inline constexpr uint64_t kAttachmentDirectoryRelativeOffset =
    (kRegionDirectoryMinimumSizeV5 + 63u) & ~uint64_t{63u};

inline constexpr uint64_t kRegionDirectoryMinimumSizeV7 =
    kAttachmentDirectoryRelativeOffset + kAttachmentDirectoryMinimumSize;

// Create / current layout minimum (v7).
inline constexpr uint64_t kRegionDirectoryMinimumSize =
    kRegionDirectoryMinimumSizeV7;

struct AttachmentRegistration {
    uint32_t slot_index = 0;
    uint64_t generation = 0;
};

Status InitializeAttachmentDirectory(void* attachment_directory_base,
                                     uint64_t available_size);
Status ValidateAttachmentDirectory(const void* attachment_directory_base,
                                   uint64_t available_size);

// Claims a Free (or Dead-reclaimable) slot. For kSubordinateWritable, enforces
// kMaxSubordinateWritableAttachments against currently Live/Claiming/Closing
// subordinate writers. Never reclaims Alive/Unknown identities.
Result<AttachmentRegistration> ClaimAttachmentSlot(
    void* attachment_directory_base, uint64_t available_size,
    AttachmentRole role, const ProcessIdentity& identity,
    uint64_t attach_service_epoch);

Status ReleaseAttachmentSlot(void* attachment_directory_base,
                             uint64_t available_size, uint32_t slot_index,
                             uint64_t generation,
                             const ProcessIdentity& identity);

Status HeartbeatAttachmentSlot(void* attachment_directory_base,
                               uint64_t available_size, uint32_t slot_index,
                               uint64_t generation,
                               const ProcessIdentity& identity,
                               uint64_t heartbeat_ns);

// Supervisor-gated recovery gate: reclaim every Dead attachment slot, then
// refuse if any subordinate writable remains Alive or Unknown. Live readers do
// not block (destructive recovery only requires no live writers).
Status PrepareAttachmentDirectoryForSupervisorRecovery(
    void* attachment_directory_base, uint64_t available_size);

uint32_t CountLiveSubordinateWritableSlots(
    const void* attachment_directory_base, uint64_t available_size);

}  // namespace mino

#endif  // MINO_SHM_REGION_ATTACHMENT_DIRECTORY_H_
