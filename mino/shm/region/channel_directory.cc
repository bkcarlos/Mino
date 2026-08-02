// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/channel_directory.h"

#include <atomic>
#include <cstddef>
#include <limits>
#include <new>
#include <string>
#include <thread>

#include "mino/common/checked_arithmetic.h"
#include "mino/shm/channel/mpmc_ring.h"
#include "mino/shm/region/superblock.h"

namespace mino {
namespace {

constexpr uint64_t EncodePublished(uint64_t sequence, uint32_t slot) {
    return (sequence << 1) | (slot & 1u);
}
constexpr uint64_t StablePublication(uint64_t sequence) { return sequence << 1; }
constexpr uint64_t PublishedSequence(uint64_t word) { return word >> 1; }
constexpr uint32_t PublishedSlot(uint64_t word) {
    return static_cast<uint32_t>(word & 1u);
}

uint32_t ControlCrc(const ChannelDirectoryControl& control) {
    return Crc32(&control,
                 offsetof(ChannelDirectoryControl, immutable_crc32));
}

uint32_t SnapshotCrc(const ChannelDirectorySnapshot& snapshot) {
    ChannelDirectorySnapshot input = snapshot;
    input.publication_word = 0;
    return Crc32(&input, offsetof(ChannelDirectorySnapshot, crc32));
}

bool IsPublicationByte(size_t offset) {
    return offset >= offsetof(ChannelDirectorySnapshot, publication_word) &&
           offset < offsetof(ChannelDirectorySnapshot, publication_word) +
                        sizeof(ChannelDirectorySnapshot::publication_word);
}

void AtomicLoadSnapshot(const ChannelDirectorySnapshot& source,
                        ChannelDirectorySnapshot* destination) {
    const auto* source_bytes = reinterpret_cast<const std::byte*>(&source);
    auto* destination_bytes = reinterpret_cast<std::byte*>(destination);
    for (size_t i = 0; i < sizeof(ChannelDirectorySnapshot); ++i) {
        if (IsPublicationByte(i)) {
            continue;
        }
        auto& byte = const_cast<std::byte&>(source_bytes[i]);
        destination_bytes[i] =
            std::atomic_ref<std::byte>(byte).load(std::memory_order_relaxed);
    }
}

void AtomicStoreSnapshot(ChannelDirectorySnapshot* destination,
                         const ChannelDirectorySnapshot& source) {
    auto* destination_bytes = reinterpret_cast<std::byte*>(destination);
    const auto* source_bytes = reinterpret_cast<const std::byte*>(&source);
    for (size_t i = 0; i < sizeof(ChannelDirectorySnapshot); ++i) {
        if (IsPublicationByte(i)) {
            continue;
        }
        std::atomic_ref<std::byte>(destination_bytes[i])
            .store(source_bytes[i], std::memory_order_relaxed);
    }
}

Status ValidateImage(const void* base, uint64_t available_size,
                     const ChannelDirectoryImage** image_out) {
    if (base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel directory base is null");
    }
    if (available_size < sizeof(ChannelDirectoryImage)) {
        return Status::Error(StatusCode::kCorruption,
                             "channel directory sub-region is too small");
    }
    if (reinterpret_cast<uintptr_t>(base) % alignof(ChannelDirectoryImage) != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "channel directory is misaligned");
    }
    const auto* image = static_cast<const ChannelDirectoryImage*>(base);
    const ChannelDirectoryControl& control = image->control;
    if (control.magic != kChannelDirectoryMagic ||
        control.version != kChannelDirectoryVersion ||
        control.header_size != sizeof(ChannelDirectoryControl) ||
        control.snapshot_size != sizeof(ChannelDirectorySnapshot) ||
        control.entry_capacity != kChannelDirectoryEntryCapacity ||
        control.reserved0 != 0 ||
        control.immutable_crc32 != ControlCrc(control)) {
        return Status::Error(StatusCode::kCorruption,
                             "channel directory control validation failed");
    }
    *image_out = image;
    return Status::Ok();
}

Status ActiveExtentsOverlap(const ChannelRingDescriptor& left,
                            const ChannelRingDescriptor& right,
                            bool* overlaps) {
    uint64_t left_end = 0;
    uint64_t right_end = 0;
    if (!CheckedAddU64(left.control_offset, left.extent_size, &left_end) ||
        !CheckedAddU64(right.control_offset, right.extent_size, &right_end)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel ring extent overflow");
    }
    *overlaps = left.control_offset < right_end &&
                right.control_offset < left_end;
    return Status::Ok();
}

Status TryPublishSnapshot(ChannelDirectoryImage* image,
                          ChannelDirectorySnapshot* snapshot) {
    auto published = std::atomic_ref(image->control.published_word);
    const uint64_t old_word = published.load(std::memory_order_acquire);
    const uint64_t old_sequence = PublishedSequence(old_word);
    if (old_sequence == 0 ||
        old_sequence >= (std::numeric_limits<uint64_t>::max() >> 1)) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "channel directory publication sequence exhausted");
    }
    if (snapshot->sequence != old_sequence) {
        return Status::Error(StatusCode::kWouldBlock,
                             "channel directory base snapshot is stale");
    }

    const uint32_t new_slot = PublishedSlot(old_word) ^ 1u;
    ChannelDirectorySnapshot& destination = image->snapshots[new_slot];
    auto slot_publication = std::atomic_ref(destination.publication_word);
    uint64_t prior = slot_publication.load(std::memory_order_acquire);
    if ((prior & 1u) != 0 ||
        !slot_publication.compare_exchange_strong(
            prior, prior | 1u, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "channel directory target snapshot is busy");
    }
    if (published.load(std::memory_order_acquire) != old_word) {
        slot_publication.store(prior, std::memory_order_release);
        return Status::Error(StatusCode::kWouldBlock,
                             "channel directory changed before publication");
    }

    snapshot->sequence = old_sequence + 1;
    snapshot->publication_word = StablePublication(snapshot->sequence);
    snapshot->crc32 = SnapshotCrc(*snapshot);
    AtomicStoreSnapshot(&destination, *snapshot);

    uint64_t expected = old_word;
    if (!published.compare_exchange_strong(
            expected, EncodePublished(snapshot->sequence, new_slot),
            std::memory_order_release, std::memory_order_acquire)) {
        slot_publication.store(snapshot->publication_word,
                               std::memory_order_release);
        return Status::Error(StatusCode::kWouldBlock,
                             "channel directory publication raced");
    }
    slot_publication.store(snapshot->publication_word, std::memory_order_release);
    return Status::Ok();
}

}  // namespace

Status ValidateChannelRingDescriptor(const ChannelRingDescriptor& descriptor,
                                     uint64_t region_size,
                                     uint64_t minimum_control_offset) {
    if (descriptor.channel_id == 0 || descriptor.generation == 0 ||
        descriptor.flags != 0 || descriptor.reserved0 != 0 ||
        descriptor.channel_type !=
            static_cast<uint32_t>(ChannelRingType::kMpmcRing) ||
        (descriptor.state !=
             static_cast<uint32_t>(ChannelRingState::kActive) &&
         descriptor.state !=
             static_cast<uint32_t>(ChannelRingState::kRetired))) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel ring identity/type/state is invalid");
    }
    if (descriptor.control_offset < minimum_control_offset ||
        descriptor.control_offset % alignof(MpmcRingControlBlock) != 0 ||
        descriptor.capacity < 2 ||
        (descriptor.capacity & (descriptor.capacity - 1)) != 0 ||
        descriptor.capacity > (uint64_t{1} << 32) ||
        descriptor.element_size == 0 || descriptor.element_alignment == 0 ||
        (descriptor.element_alignment & (descriptor.element_alignment - 1)) !=
            0 ||
        descriptor.element_alignment > alignof(MpmcRingControlBlock) ||
        descriptor.element_size % descriptor.element_alignment != 0 ||
        descriptor.ring_layout_version != kMpmcRingLayoutVersion) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel ring layout metadata is invalid");
    }

    uint64_t slots_size = 0;
    uint64_t required_size = 0;
    uint64_t end = 0;
    if (!CheckedMulU64(descriptor.capacity,
                       MpmcRingSlotStride(descriptor.element_size,
                                          descriptor.element_alignment),
                       &slots_size) ||
        !CheckedAddU64(sizeof(MpmcRingControlBlock), slots_size,
                       &required_size) ||
        descriptor.extent_size != required_size ||
        !CheckedAddU64(descriptor.control_offset, descriptor.extent_size, &end) ||
        end > region_size) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel ring extent is out of Region bounds");
    }
    return Status::Ok();
}

Status InitializeChannelDirectory(void* base, uint64_t available_size) {
    if (base == nullptr || available_size < sizeof(ChannelDirectoryImage) ||
        reinterpret_cast<uintptr_t>(base) % alignof(ChannelDirectoryImage) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel directory storage is invalid");
    }
    auto* image = new (base) ChannelDirectoryImage{};
    image->control.magic = kChannelDirectoryMagic;
    image->control.version = kChannelDirectoryVersion;
    image->control.header_size = sizeof(ChannelDirectoryControl);
    image->control.snapshot_size = sizeof(ChannelDirectorySnapshot);
    image->control.entry_capacity = kChannelDirectoryEntryCapacity;
    image->control.immutable_crc32 = ControlCrc(image->control);
    image->snapshots[0].sequence = 1;
    image->snapshots[0].publication_word = StablePublication(1);
    image->snapshots[0].crc32 = SnapshotCrc(image->snapshots[0]);
    std::atomic_ref(image->control.published_word)
        .store(EncodePublished(1, 0), std::memory_order_release);
    return Status::Ok();
}

Result<ChannelDirectorySnapshot> ReadChannelDirectory(
    const void* base, uint64_t available_size, uint64_t region_size,
    uint64_t minimum_control_offset) {
    const ChannelDirectoryImage* image = nullptr;
    MINO_RETURN_IF_ERROR(ValidateImage(base, available_size, &image));
    const auto published = std::atomic_ref(
        const_cast<uint64_t&>(image->control.published_word));
    for (int attempt = 0; attempt < 64; ++attempt) {
        const uint64_t before = published.load(std::memory_order_acquire);
        const uint64_t sequence = PublishedSequence(before);
        const uint32_t slot = PublishedSlot(before);
        if (sequence == 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "channel directory has no published snapshot");
        }
        const ChannelDirectorySnapshot& source = image->snapshots[slot];
        auto slot_publication = std::atomic_ref(
            const_cast<uint64_t&>(source.publication_word));
        const uint64_t slot_before =
            slot_publication.load(std::memory_order_acquire);
        if (slot_before != StablePublication(sequence)) {
            std::this_thread::yield();
            continue;
        }

        ChannelDirectorySnapshot snapshot{};
        AtomicLoadSnapshot(source, &snapshot);
        const uint64_t slot_after =
            slot_publication.load(std::memory_order_acquire);
        const uint64_t after = published.load(std::memory_order_acquire);
        if (before != after || slot_before != slot_after ||
            slot_after != StablePublication(sequence)) {
            std::this_thread::yield();
            continue;
        }
        snapshot.publication_word = slot_after;
        if (snapshot.sequence != sequence ||
            snapshot.entry_count > kChannelDirectoryEntryCapacity ||
            snapshot.flags != 0 || snapshot.reserved0 != 0 ||
            snapshot.reserved1 != 0 ||
            snapshot.crc32 != SnapshotCrc(snapshot)) {
            return Status::Error(StatusCode::kCorruption,
                                 "channel directory snapshot validation failed");
        }
        for (uint32_t i = 0; i < snapshot.entry_count; ++i) {
            if (!ValidateChannelRingDescriptor(snapshot.entries[i], region_size,
                                               minimum_control_offset)
                     .ok()) {
                return Status::Error(StatusCode::kCorruption,
                                     "channel directory contains an invalid ring descriptor");
            }
            for (uint32_t j = 0; j < i; ++j) {
                if (snapshot.entries[j].channel_id ==
                    snapshot.entries[i].channel_id) {
                    return Status::Error(StatusCode::kCorruption,
                                         "channel directory contains duplicate channel ids");
                }
                if (snapshot.entries[j].state ==
                        static_cast<uint32_t>(ChannelRingState::kActive) &&
                    snapshot.entries[i].state ==
                        static_cast<uint32_t>(ChannelRingState::kActive)) {
                    bool overlaps = false;
                    if (!ActiveExtentsOverlap(snapshot.entries[j],
                                              snapshot.entries[i], &overlaps)
                             .ok() ||
                        overlaps) {
                        return Status::Error(
                            StatusCode::kCorruption,
                            "channel directory contains overlapping active ring extents");
                    }
                }
            }
        }
        return snapshot;
    }
    return Status::Error(StatusCode::kUnavailable,
                         "channel directory changed during read");
}

Status RegisterChannelRing(void* base, uint64_t available_size,
                           uint64_t region_size,
                           uint64_t minimum_control_offset,
                           const ChannelRingDescriptor& descriptor) {
    MINO_RETURN_IF_ERROR(ValidateChannelRingDescriptor(
        descriptor, region_size, minimum_control_offset));
    if (descriptor.state !=
        static_cast<uint32_t>(ChannelRingState::kActive)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel registration must publish ACTIVE state");
    }
    for (int attempt = 0; attempt < 4096; ++attempt) {
        auto current = ReadChannelDirectory(base, available_size, region_size,
                                            minimum_control_offset);
        if (!current.ok()) {
            if (current.status().code() == StatusCode::kUnavailable) {
                std::this_thread::yield();
                continue;
            }
            return current.status();
        }
        ChannelDirectorySnapshot next = *current;
        uint32_t index = next.entry_count;
        for (uint32_t i = 0; i < next.entry_count; ++i) {
            const ChannelRingDescriptor& existing = next.entries[i];
            if (existing.channel_id == descriptor.channel_id) {
                if (existing.state ==
                    static_cast<uint32_t>(ChannelRingState::kActive)) {
                    return Status::Error(StatusCode::kAlreadyExists,
                                         "channel id is already active");
                }
                if (descriptor.generation <= existing.generation) {
                    return Status::Error(StatusCode::kInvalidArgument,
                                         "channel registration generation is stale");
                }
                index = i;
                continue;
            }
            if (existing.state ==
                static_cast<uint32_t>(ChannelRingState::kActive)) {
                bool overlaps = false;
                MINO_RETURN_IF_ERROR(
                    ActiveExtentsOverlap(existing, descriptor, &overlaps));
                if (overlaps) {
                    return Status::Error(
                        StatusCode::kAlreadyExists,
                        "ring extent overlaps an active registration");
                }
            }
        }
        if (index == next.entry_count) {
            if (next.entry_count == kChannelDirectoryEntryCapacity) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     "channel directory is full");
            }
            ++next.entry_count;
        }
        next.entries[index] = descriptor;
        Status published = TryPublishSnapshot(
            static_cast<ChannelDirectoryImage*>(base), &next);
        if (published.ok()) {
            return published;
        }
        if (published.code() != StatusCode::kWouldBlock) {
            return published;
        }
        std::this_thread::yield();
    }
    return Status::Error(StatusCode::kUnavailable,
                         "channel registration remained contended");
}

Status UnregisterChannelRing(void* base, uint64_t available_size,
                             uint64_t region_size,
                             uint64_t minimum_control_offset,
                             uint32_t channel_id, uint64_t generation) {
    if (channel_id == 0 || generation == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel id and generation must be non-zero");
    }
    for (int attempt = 0; attempt < 4096; ++attempt) {
        auto current = ReadChannelDirectory(base, available_size, region_size,
                                            minimum_control_offset);
        if (!current.ok()) {
            if (current.status().code() == StatusCode::kUnavailable) {
                std::this_thread::yield();
                continue;
            }
            return current.status();
        }
        ChannelDirectorySnapshot next = *current;
        uint32_t index = next.entry_count;
        for (uint32_t i = 0; i < next.entry_count; ++i) {
            if (next.entries[i].channel_id == channel_id) {
                index = i;
                break;
            }
        }
        if (index == next.entry_count) {
            return Status::Error(StatusCode::kNotFound,
                                 "channel id is not registered");
        }
        ChannelRingDescriptor& existing = next.entries[index];
        if (existing.state !=
                static_cast<uint32_t>(ChannelRingState::kActive) ||
            existing.generation != generation) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "channel unregistration generation is stale");
        }
        existing.state = static_cast<uint32_t>(ChannelRingState::kRetired);
        Status published = TryPublishSnapshot(
            static_cast<ChannelDirectoryImage*>(base), &next);
        if (published.ok()) {
            return published;
        }
        if (published.code() != StatusCode::kWouldBlock) {
            return published;
        }
        std::this_thread::yield();
    }
    return Status::Error(StatusCode::kUnavailable,
                         "channel unregistration remained contended");
}

}  // namespace mino
