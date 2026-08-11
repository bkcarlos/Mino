// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_PLATFORM_SHARED_MEMORY_MARKER_H_
#define MINO_PLATFORM_SHARED_MEMORY_MARKER_H_

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "mino/platform/process_identity.h"

namespace mino::shared_memory_internal {

constexpr uint64_t kMarkerMagic = 0x4D494E4F53484D32ull;  // MINOSHM2
constexpr uint32_t kMarkerVersion = 2;
constexpr size_t kMarkerPathCapacity = 256;

// Persisted lifecycle. Only HUGE_READY and FALLBACK_READY may be opened.
enum class MarkerState : uint32_t {
    kCreating = 1,
    kHugeReady = 2,
    kFallbackReady = 3,
    kUnlinking = 4,
};

enum class MarkerBackingKind : uint32_t {
    kNone = 0,
    kHugeFile = 1,
    kPosixData = 2,
};

constexpr uint32_t kMarkerFlagHugeRequested = 1u << 0;
constexpr uint32_t kMarkerFlagBackingSizeCommitted = 1u << 1;

// CRC-protected payload. `backing_name` is an absolute hugetlbfs file path for
// kHugeFile and a POSIX shm name for kPosixData. Device/inode identify the exact
// object; a zero inode is allowed only while CREATING before recovery adopts a
// uniquely generated candidate.
struct SharedMemoryMarkerPayload {
    uint64_t magic = kMarkerMagic;
    uint32_t version = kMarkerVersion;
    uint32_t payload_size = sizeof(SharedMemoryMarkerPayload);
    uint32_t state = static_cast<uint32_t>(MarkerState::kCreating);
    uint32_t backing_kind = static_cast<uint32_t>(MarkerBackingKind::kNone);
    uint32_t flags = 0;
    uint32_t fallback_reason = 0;
    int32_t fallback_errno = 0;
    uint32_t reserved0 = 0;
    uint64_t data_size = 0;
    uint64_t page_size = 0;
    uint64_t mount_device = 0;
    uint64_t backing_device = 0;
    uint64_t backing_inode = 0;
    ProcessIdentity creator;
    char mount_path[kMarkerPathCapacity] = {};
    char backing_name[kMarkerPathCapacity] = {};
};

struct SharedMemoryMarkerSlot {
    SharedMemoryMarkerPayload payload;
    uint32_t payload_crc32 = 0;
    uint32_t reserved = 0;
};

// Copy-on-publish marker. The low bit of published_word selects the active
// slot; upper bits are a monotonically increasing generation. Writers fully
// write and msync the inactive CRC-protected slot before one atomic publication.
struct alignas(64) SharedMemoryMarkerRecord {
    uint64_t published_word = 0;
    uint64_t reserved = 0;
    SharedMemoryMarkerSlot slots[2];
};

inline uint32_t MarkerPayloadCrc32(
    const SharedMemoryMarkerPayload& payload) noexcept {
    uint32_t crc = 0xFFFFFFFFu;
    const auto* bytes = reinterpret_cast<const uint8_t*>(&payload);
    for (size_t i = 0; i < sizeof(payload); ++i) {
        crc ^= bytes[i];
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask = 0u - (crc & 1u);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

static_assert(std::is_trivially_copyable_v<SharedMemoryMarkerPayload>);
static_assert(std::is_trivially_copyable_v<SharedMemoryMarkerSlot>);
static_assert(std::is_trivially_copyable_v<SharedMemoryMarkerRecord>);
static_assert(sizeof(SharedMemoryMarkerRecord) <= 2048);

// Test-only deterministic fault points. Production code never installs a hook.
enum class SharedMemoryTestPoint {
    kBeforeMarkerMap,
    kAfterCreatingMarker,
    kBeforeMarkerPublication,
    kAfterBackingIdentityRecorded,
    kAfterUnlinkingPublished,
};
using SharedMemoryTestHook = bool (*)(SharedMemoryTestPoint);
void SetSharedMemoryTestHook(SharedMemoryTestHook hook) noexcept;

}  // namespace mino::shared_memory_internal

#endif  // MINO_PLATFORM_SHARED_MEMORY_MARKER_H_
