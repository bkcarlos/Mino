// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/channel_directory.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#include <gtest/gtest.h>

#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/mpmc_ring.h"

namespace mino {
namespace {

constexpr uint64_t kRegionSize = 16 * 1024 * 1024;
constexpr uint64_t kDataOffset = 64 * 1024;
constexpr uint64_t kRingCapacity = 8;
constexpr uint64_t kRingSize = MpmcRingRequiredSize(
    kRingCapacity, sizeof(IndexSlot), alignof(IndexSlot));

ChannelRingDescriptor Descriptor(uint32_t id, uint64_t offset,
                                 uint64_t generation = 1) {
    return ChannelRingDescriptor{
        .channel_id = id,
        .channel_type = static_cast<uint32_t>(ChannelRingType::kMpmcRing),
        .state = static_cast<uint32_t>(ChannelRingState::kActive),
        .control_offset = offset,
        .extent_size = kRingSize,
        .capacity = kRingCapacity,
        .generation = generation,
        .element_size = sizeof(IndexSlot),
        .element_alignment = alignof(IndexSlot),
        .ring_layout_version = kMpmcRingLayoutVersion,
    };
}

class ChannelDirectoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::memset(storage_, 0, sizeof(storage_));
        ASSERT_TRUE(InitializeChannelDirectory(storage_, sizeof(storage_)).ok());
    }

    alignas(64) std::byte storage_[kChannelDirectoryMinimumSize];
};

TEST_F(ChannelDirectoryTest, PublishesReadOnlySnapshotAndRejectsDuplicateAndStale) {
    const ChannelRingDescriptor first = Descriptor(7, kDataOffset);
    ASSERT_TRUE(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset, first)
                    .ok());

    auto snapshot = ReadChannelDirectory(storage_, sizeof(storage_), kRegionSize,
                                         kDataOffset);
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    ASSERT_EQ(snapshot->entry_count, 1u);
    EXPECT_EQ(snapshot->entries[0].channel_id, 7u);
    EXPECT_EQ(snapshot->entries[0].capacity, kRingCapacity);
    EXPECT_EQ(snapshot->entries[0].generation, 1u);
    EXPECT_EQ(snapshot->entries[0].state,
              static_cast<uint32_t>(ChannelRingState::kActive));

    EXPECT_EQ(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                  kDataOffset, first)
                  .code(),
              StatusCode::kAlreadyExists);
    EXPECT_EQ(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                  kDataOffset,
                                  Descriptor(8, first.control_offset))
                  .code(),
              StatusCode::kAlreadyExists);
    EXPECT_EQ(UnregisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset, 7, 2)
                  .code(),
              StatusCode::kInvalidArgument);
    ASSERT_TRUE(UnregisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                      kDataOffset, 7, 1)
                    .ok());
    EXPECT_EQ(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                  kDataOffset, first)
                  .code(),
              StatusCode::kInvalidArgument);

    ChannelRingDescriptor replacement = Descriptor(7, kDataOffset + kRingSize, 2);
    ASSERT_TRUE(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset, replacement)
                    .ok());
    snapshot = ReadChannelDirectory(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset);
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    ASSERT_EQ(snapshot->entry_count, 1u);
    EXPECT_EQ(snapshot->entries[0].generation, 2u);
    EXPECT_EQ(snapshot->entries[0].control_offset, replacement.control_offset);
}

TEST_F(ChannelDirectoryTest, ActiveExtentsMustNotOverlap) {
    ASSERT_TRUE(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset, Descriptor(1, kDataOffset))
                    .ok());

    ChannelRingDescriptor partial = Descriptor(2, kDataOffset + 64);
    EXPECT_EQ(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                  kDataOffset, partial)
                  .code(),
              StatusCode::kAlreadyExists);

    ChannelRingDescriptor adjacent = Descriptor(2, kDataOffset + kRingSize);
    EXPECT_TRUE(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset, adjacent)
                    .ok());
}

TEST_F(ChannelDirectoryTest, ReRegistrationStillChecksLaterActiveOffsets) {
    ASSERT_TRUE(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset, Descriptor(7, kDataOffset))
                    .ok());
    ASSERT_TRUE(UnregisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                      kDataOffset, 7, 1)
                    .ok());
    const uint64_t active_offset = kDataOffset + 2 * kRingSize;
    ASSERT_TRUE(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset, Descriptor(8, active_offset))
                    .ok());

    EXPECT_EQ(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                  kDataOffset,
                                  Descriptor(7, active_offset, 2))
                  .code(),
              StatusCode::kAlreadyExists);
}

TEST_F(ChannelDirectoryTest, RejectsBoundsAndMalformedExtent) {
    ChannelRingDescriptor below_data = Descriptor(1, kDataOffset - 64);
    EXPECT_EQ(ValidateChannelRingDescriptor(below_data, kRegionSize, kDataOffset)
                  .code(),
              StatusCode::kInvalidArgument);

    ChannelRingDescriptor past_end = Descriptor(1, kRegionSize - 64);
    EXPECT_EQ(ValidateChannelRingDescriptor(past_end, kRegionSize, kDataOffset)
                  .code(),
              StatusCode::kInvalidArgument);

    ChannelRingDescriptor overflowing = Descriptor(
        1, std::numeric_limits<uint64_t>::max() - 63);
    EXPECT_EQ(ValidateChannelRingDescriptor(
                  overflowing, std::numeric_limits<uint64_t>::max(),
                  kDataOffset)
                  .code(),
              StatusCode::kInvalidArgument);

    ChannelRingDescriptor wrong_extent = Descriptor(1, kDataOffset);
    ++wrong_extent.extent_size;
    EXPECT_EQ(ValidateChannelRingDescriptor(wrong_extent, kRegionSize,
                                             kDataOffset)
                  .code(),
              StatusCode::kInvalidArgument);
}

TEST_F(ChannelDirectoryTest, RegistrationIsBounded) {
    for (uint32_t i = 0; i < kChannelDirectoryEntryCapacity; ++i) {
        const ChannelRingDescriptor descriptor =
            Descriptor(i + 1, kDataOffset + static_cast<uint64_t>(i) * kRingSize);
        ASSERT_TRUE(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                        kDataOffset, descriptor)
                        .ok())
            << i;
    }
    const ChannelRingDescriptor overflow = Descriptor(
        kChannelDirectoryEntryCapacity + 1,
        kDataOffset + static_cast<uint64_t>(kChannelDirectoryEntryCapacity) *
                          kRingSize);
    EXPECT_EQ(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                  kDataOffset, overflow)
                  .code(),
              StatusCode::kResourceExhausted);
}

TEST_F(ChannelDirectoryTest, PublishedSnapshotCrcCorruptionIsRejected) {
    ASSERT_TRUE(RegisterChannelRing(storage_, sizeof(storage_), kRegionSize,
                                    kDataOffset, Descriptor(1, kDataOffset))
                    .ok());
    auto* image = reinterpret_cast<ChannelDirectoryImage*>(storage_);
    const uint64_t published =
        std::atomic_ref(image->control.published_word)
            .load(std::memory_order_acquire);
    image->snapshots[published & 1u].crc32 ^= 1u;

    auto snapshot = ReadChannelDirectory(storage_, sizeof(storage_), kRegionSize,
                                         kDataOffset);
    ASSERT_FALSE(snapshot.ok());
    EXPECT_EQ(snapshot.status().code(), StatusCode::kCorruption);
}

}  // namespace
}  // namespace mino
