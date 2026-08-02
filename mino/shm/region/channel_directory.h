// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_SHM_REGION_CHANNEL_DIRECTORY_H_
#define MINO_SHM_REGION_CHANNEL_DIRECTORY_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/channel/ring_directory_abi.h"
#include "mino/shm/region/recovery_directory.h"

namespace mino {

inline constexpr uint32_t kChannelDirectoryMagic = 0x4D434431u;  // "MCD1"
inline constexpr uint16_t kChannelDirectoryVersion = 1;
inline constexpr uint32_t kChannelDirectoryEntryCapacity = 64;

// The SuperBlock's immutable directory_offset continues to point at the
// recovery directory. Region layout v5 appends this image at a fixed,
// cache-line-aligned relative offset inside the same reserved sub-region.
inline constexpr uint64_t kChannelDirectoryRelativeOffset =
    (kRecoveryDirectoryMinimumSize + 63u) & ~uint64_t{63u};

struct alignas(64) ChannelDirectoryControl {
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t header_size = 0;
    uint32_t snapshot_size = 0;
    uint32_t entry_capacity = 0;
    uint32_t immutable_crc32 = 0;
    uint32_t reserved0 = 0;
    uint64_t published_word = 0;  // {sequence, slot}, accessed via atomic_ref.
    std::byte padding[32]{};
};
static_assert(sizeof(ChannelDirectoryControl) == 64);
static_assert(offsetof(ChannelDirectoryControl, published_word) == 24);

struct alignas(64) ChannelDirectorySnapshot {
    uint64_t sequence = 0;
    uint32_t entry_count = 0;
    uint32_t flags = 0;
    uint64_t publication_word = 0;  // Stable sequence << 1; odd means writing.
    uint64_t reserved0 = 0;
    ChannelRingDescriptor entries[kChannelDirectoryEntryCapacity]{};
    uint32_t crc32 = 0;
    uint32_t reserved1 = 0;
    std::byte padding[24]{};
};
static_assert(sizeof(ChannelDirectorySnapshot) == 4160);
static_assert(offsetof(ChannelDirectorySnapshot, publication_word) == 16);
static_assert(offsetof(ChannelDirectorySnapshot, entries) == 32);
static_assert(offsetof(ChannelDirectorySnapshot, crc32) == 4128);
static_assert(std::is_trivially_copyable_v<ChannelDirectorySnapshot>);

struct alignas(64) ChannelDirectoryImage {
    ChannelDirectoryControl control;
    ChannelDirectorySnapshot snapshots[2];
};
static_assert(sizeof(ChannelDirectoryImage) == 8384);
static_assert(ATOMIC_CHAR_LOCK_FREE == 2,
              "ChannelDirectory requires lock-free byte atomics");
static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
              "ChannelDirectory requires lock-free 64-bit atomics");

inline constexpr uint64_t kChannelDirectoryMinimumSize =
    sizeof(ChannelDirectoryImage);
inline constexpr uint64_t kRegionDirectoryMinimumSize =
    kChannelDirectoryRelativeOffset + kChannelDirectoryMinimumSize;

Status InitializeChannelDirectory(void* channel_directory_base,
                                  uint64_t available_size);
Result<ChannelDirectorySnapshot> ReadChannelDirectory(
    const void* channel_directory_base, uint64_t available_size,
    uint64_t region_size, uint64_t minimum_control_offset);
Status RegisterChannelRing(void* channel_directory_base, uint64_t available_size,
                           uint64_t region_size,
                           uint64_t minimum_control_offset,
                           const ChannelRingDescriptor& descriptor);
Status UnregisterChannelRing(void* channel_directory_base,
                             uint64_t available_size, uint64_t region_size,
                             uint64_t minimum_control_offset,
                             uint32_t channel_id, uint64_t generation);
Status ValidateChannelRingDescriptor(const ChannelRingDescriptor& descriptor,
                                     uint64_t region_size,
                                     uint64_t minimum_control_offset);

}  // namespace mino

#endif  // MINO_SHM_REGION_CHANNEL_DIRECTORY_H_
