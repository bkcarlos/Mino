// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_SHM_CHANNEL_RING_DIRECTORY_ABI_H_
#define MINO_SHM_CHANNEL_RING_DIRECTORY_ABI_H_

#include <cstdint>
#include <type_traits>

namespace mino {

// Persisted channel/ring identity. New channel layouts receive new values;
// existing values are never repurposed.
enum class ChannelRingType : uint32_t {
    kMpmcRing = 1,
};

enum class ChannelRingState : uint32_t {
    kActive = 1,
    kRetired = 2,
};

// Fixed shared-memory ABI stored in the Region Channel Directory. Every offset
// is relative to the Region base; process virtual addresses are never persisted.
struct ChannelRingDescriptor {
    uint32_t channel_id = 0;
    uint32_t channel_type = 0;
    uint32_t state = 0;
    uint32_t flags = 0;

    uint64_t control_offset = 0;
    uint64_t extent_size = 0;
    uint64_t capacity = 0;
    uint64_t generation = 0;

    uint32_t element_size = 0;
    uint32_t element_alignment = 0;
    uint32_t ring_layout_version = 0;
    uint32_t reserved0 = 0;
};

static_assert(sizeof(ChannelRingDescriptor) == 64);
static_assert(alignof(ChannelRingDescriptor) == 8);
static_assert(std::is_standard_layout_v<ChannelRingDescriptor>);
static_assert(std::is_trivially_copyable_v<ChannelRingDescriptor>);

}  // namespace mino

#endif  // MINO_SHM_CHANNEL_RING_DIRECTORY_ABI_H_
