// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/shm/region/recovery_directory.h"

#include <atomic>
#include <cstddef>
#include <limits>
#include <new>
#include <string>
#include <thread>

#include "mino/common/checked_arithmetic.h"
#include "mino/shm/region/superblock.h"

namespace mino {
namespace {

constexpr uint64_t EncodePublished(uint64_t sequence, uint32_t slot) {
    return (sequence << 1) | (slot & 1u);
}
constexpr uint64_t StablePublication(uint64_t sequence) { return sequence << 1; }
uint64_t PublishedSequence(uint64_t word) { return word >> 1; }
uint32_t PublishedSlot(uint64_t word) { return static_cast<uint32_t>(word & 1u); }

uint32_t ControlCrc(const RecoveryDirectoryControl& control) {
    return Crc32(&control,
                 offsetof(RecoveryDirectoryControl, immutable_crc32));
}

uint32_t SnapshotCrc(const RecoveryDirectorySnapshot& snapshot) {
    RecoveryDirectorySnapshot crc_input = snapshot;
    crc_input.publication_word = 0;
    return Crc32(&crc_input, offsetof(RecoveryDirectorySnapshot, crc32));
}

bool IsPublicationByte(size_t offset) {
    return offset >= offsetof(RecoveryDirectorySnapshot, publication_word) &&
           offset < offsetof(RecoveryDirectorySnapshot, publication_word) +
                        sizeof(RecoveryDirectorySnapshot::publication_word);
}

// The shared payload is copied byte-by-byte through atomic_ref. This is
// intentionally more conservative than a memcpy seqlock: C++ seqlocks do not
// make concurrent non-atomic reads and writes race-free. The slot publication
// word validates that all bytes belong to one completed writer generation.
void AtomicLoadSnapshot(const RecoveryDirectorySnapshot& source,
                        RecoveryDirectorySnapshot* destination) {
    auto* source_bytes = reinterpret_cast<const std::byte*>(&source);
    auto* destination_bytes = reinterpret_cast<std::byte*>(destination);
    for (size_t i = 0; i < sizeof(RecoveryDirectorySnapshot); ++i) {
        if (IsPublicationByte(i)) {
            continue;
        }
        auto& byte = const_cast<std::byte&>(source_bytes[i]);
        destination_bytes[i] =
            std::atomic_ref<std::byte>(byte).load(std::memory_order_relaxed);
    }
}

void AtomicStoreSnapshot(RecoveryDirectorySnapshot* destination,
                         const RecoveryDirectorySnapshot& source) {
    auto* destination_bytes = reinterpret_cast<std::byte*>(destination);
    const auto* source_bytes = reinterpret_cast<const std::byte*>(&source);
    for (size_t i = 0; i < sizeof(RecoveryDirectorySnapshot); ++i) {
        if (IsPublicationByte(i)) {
            continue;
        }
        std::atomic_ref<std::byte>(destination_bytes[i])
            .store(source_bytes[i], std::memory_order_relaxed);
    }
}

Status ValidateImage(const void* directory_base, uint64_t directory_size,
                     const RecoveryDirectoryImage** image_out) {
    if (directory_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery directory base is null");
    }
    if (directory_size < sizeof(RecoveryDirectoryImage)) {
        return Status::Error(StatusCode::kCorruption,
                             "recovery directory sub-region is too small");
    }
    if (reinterpret_cast<uintptr_t>(directory_base) %
            alignof(RecoveryDirectoryImage) !=
        0) {
        return Status::Error(StatusCode::kCorruption,
                             "recovery directory is misaligned");
    }
    const auto* image = static_cast<const RecoveryDirectoryImage*>(directory_base);
    const RecoveryDirectoryControl& control = image->control;
    if (control.magic != kRecoveryDirectoryMagic ||
        control.version != kRecoveryDirectoryVersion ||
        control.header_size != sizeof(RecoveryDirectoryControl) ||
        control.snapshot_size != sizeof(RecoveryDirectorySnapshot) ||
        control.resource_capacity != kRecoveryDirectoryResourceCapacity ||
        control.reference_capacity != kRecoveryDirectoryReferenceCapacity ||
        control.immutable_crc32 != ControlCrc(control)) {
        return Status::Error(StatusCode::kCorruption,
                             "recovery directory control validation failed");
    }
    *image_out = image;
    return Status::Ok();
}

Status CheckExtent(uint64_t offset, uint64_t size, uint64_t region_size,
                   const char* what) {
    uint64_t end = 0;
    if (size == 0 || !CheckedAddU64(offset, size, &end) || end > region_size) {
        return Status::Error(StatusCode::kInvalidArgument,
                             std::string(what) + " extent is out of Region bounds");
    }
    return Status::Ok();
}

Status TryPublishSnapshot(RecoveryDirectoryImage* image,
                          RecoveryDirectorySnapshot* snapshot) {
    auto published = std::atomic_ref(image->control.published_word);
    const uint64_t old_word = published.load(std::memory_order_acquire);
    const uint64_t old_sequence = PublishedSequence(old_word);
    if (old_sequence == 0 ||
        old_sequence >= (std::numeric_limits<uint64_t>::max() >> 1)) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "recovery directory publication sequence exhausted");
    }
    if (snapshot->sequence != old_sequence) {
        return Status::Error(StatusCode::kWouldBlock,
                             "recovery directory base snapshot is stale");
    }

    const uint32_t new_slot = PublishedSlot(old_word) ^ 1u;
    RecoveryDirectorySnapshot& destination = image->snapshots[new_slot];
    auto slot_publication = std::atomic_ref(destination.publication_word);
    uint64_t prior_slot_publication =
        slot_publication.load(std::memory_order_acquire);
    if ((prior_slot_publication & 1u) != 0 ||
        !slot_publication.compare_exchange_strong(
            prior_slot_publication, prior_slot_publication | 1u,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "recovery directory target slot is busy");
    }

    // Every writer that starts from old_word targets this same alternate slot.
    // Holding its odd seqlock therefore serializes competing publications. If a
    // different publication won before the lock, do not overwrite its payload.
    if (published.load(std::memory_order_acquire) != old_word) {
        slot_publication.store(prior_slot_publication,
                               std::memory_order_release);
        return Status::Error(StatusCode::kWouldBlock,
                             "recovery directory changed before publication");
    }

    snapshot->sequence = old_sequence + 1;
    snapshot->publication_word = StablePublication(snapshot->sequence);
    snapshot->crc32 = SnapshotCrc(*snapshot);
    AtomicStoreSnapshot(&destination, *snapshot);

    // Keep the target seqlock odd until the global publication CAS succeeds.
    // Otherwise a second writer could overwrite this slot in the small window
    // between slot completion and publication, causing the first writer to
    // report success for the second writer's payload.
    uint64_t expected_word = old_word;
    if (!published.compare_exchange_strong(
            expected_word, EncodePublished(snapshot->sequence, new_slot),
            std::memory_order_release, std::memory_order_acquire)) {
        slot_publication.store(snapshot->publication_word,
                               std::memory_order_release);
        return Status::Error(StatusCode::kWouldBlock,
                             "recovery directory publication raced");
    }
    slot_publication.store(snapshot->publication_word, std::memory_order_release);
    return Status::Ok();
}

}  // namespace

Status InitializeRecoveryDirectory(void* directory_base,
                                   uint64_t directory_size) {
    if (directory_base == nullptr ||
        directory_size < sizeof(RecoveryDirectoryImage) ||
        reinterpret_cast<uintptr_t>(directory_base) %
                alignof(RecoveryDirectoryImage) !=
            0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery directory storage is invalid");
    }
    auto* image = new (directory_base) RecoveryDirectoryImage{};
    image->control.magic = kRecoveryDirectoryMagic;
    image->control.version = kRecoveryDirectoryVersion;
    image->control.header_size = sizeof(RecoveryDirectoryControl);
    image->control.snapshot_size = sizeof(RecoveryDirectorySnapshot);
    image->control.resource_capacity = kRecoveryDirectoryResourceCapacity;
    image->control.reference_capacity = kRecoveryDirectoryReferenceCapacity;
    image->control.immutable_crc32 = ControlCrc(image->control);

    image->snapshots[0].sequence = 1;
    image->snapshots[0].publication_word = StablePublication(1);
    image->snapshots[0].crc32 = SnapshotCrc(image->snapshots[0]);
    std::atomic_ref(image->control.published_word)
        .store(EncodePublished(1, 0), std::memory_order_release);
    return Status::Ok();
}

Result<RecoveryDirectorySnapshot> ReadRecoveryDirectory(
    const void* directory_base, uint64_t directory_size) {
    const RecoveryDirectoryImage* image = nullptr;
    MINO_RETURN_IF_ERROR(ValidateImage(directory_base, directory_size, &image));
    const auto published = std::atomic_ref(
        const_cast<uint64_t&>(image->control.published_word));
    for (int attempt = 0; attempt < 64; ++attempt) {
        const uint64_t before = published.load(std::memory_order_acquire);
        const uint64_t sequence = PublishedSequence(before);
        const uint32_t slot = PublishedSlot(before);
        if (sequence == 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "recovery directory has no published snapshot");
        }
        const RecoveryDirectorySnapshot& source = image->snapshots[slot];
        auto slot_publication = std::atomic_ref(
            const_cast<uint64_t&>(source.publication_word));
        const uint64_t slot_before =
            slot_publication.load(std::memory_order_acquire);
        if (slot_before != StablePublication(sequence)) {
            std::this_thread::yield();
            continue;
        }

        RecoveryDirectorySnapshot snapshot{};
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
        if (snapshot.sequence != sequence || snapshot.reserved0 != 0 ||
            snapshot.reserved2 != 0 ||
            snapshot.resource_count > kRecoveryDirectoryResourceCapacity ||
            snapshot.reference_count > kRecoveryDirectoryReferenceCapacity ||
            (snapshot.flags & ~kRecoveryDirectoryFlagMask) != 0 ||
            snapshot.crc32 != SnapshotCrc(snapshot)) {
            return Status::Error(StatusCode::kCorruption,
                                 "recovery directory snapshot validation failed");
        }
        return snapshot;
    }
    return Status::Error(StatusCode::kUnavailable,
                         "recovery directory changed during read");
}

Status ValidateRecoveryResourceDescriptor(
    const RecoveryResourceDescriptor& descriptor, uint64_t region_size) {
    if (descriptor.resource_id == 0 || descriptor.format_version != 1 ||
        (descriptor.flags & ~kRecoveryResourceFlagMask) != 0 ||
        descriptor.reserved0 != 0 || descriptor.reserved1 != 0 ||
        descriptor.reserved2 != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery resource identity/version/flags invalid");
    }
    const auto kind = static_cast<RecoveryResourceKind>(descriptor.kind);
    MINO_RETURN_IF_ERROR(
        CheckExtent(descriptor.offset, descriptor.size, region_size, "resource"));
    if (kind == RecoveryResourceKind::kCentralAllocator ||
        kind == RecoveryResourceKind::kLargeObjectPool) {
        if (descriptor.element_count != 0 || descriptor.element_stride != 0 ||
            descriptor.generation_offset != 0 || descriptor.value_offset != 0 ||
            descriptor.control_offset != 0 || descriptor.control_size != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "allocator resource has cleanup layout fields");
        }
        return Status::Ok();
    }
    if (kind != RecoveryResourceKind::kChannelAckSource &&
        kind != RecoveryResourceKind::kPinCleanupParticipant) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "unknown recovery resource kind");
    }
    if (descriptor.element_count == 0 ||
        descriptor.element_stride < sizeof(RecoveryGenerationValue) ||
        descriptor.generation_offset >
            descriptor.element_stride - sizeof(uint64_t) ||
        descriptor.value_offset > descriptor.element_stride - sizeof(uint64_t) ||
        descriptor.generation_offset % alignof(uint64_t) != 0 ||
        descriptor.value_offset % alignof(uint64_t) != 0 ||
        descriptor.generation_offset == descriptor.value_offset ||
        descriptor.offset % alignof(uint64_t) != 0 ||
        descriptor.control_offset % alignof(uint64_t) != 0 ||
        descriptor.control_size < sizeof(RecoveryGenerationControl)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "generation-scoped cleanup layout is invalid");
    }
    MINO_RETURN_IF_ERROR(CheckExtent(descriptor.control_offset,
                                     descriptor.control_size, region_size,
                                     "cleanup control"));
    uint64_t resource_end = 0;
    uint64_t last_delta = 0;
    uint64_t last_offset = 0;
    uint64_t value_end = 0;
    if (!CheckedAddU64(descriptor.offset, descriptor.size, &resource_end) ||
        !CheckedMulU64(descriptor.element_count - 1,
                       descriptor.element_stride, &last_delta) ||
        !CheckedAddU64(descriptor.offset, last_delta, &last_offset) ||
        !CheckedAddU64(last_offset,
                       static_cast<uint64_t>(descriptor.element_stride),
                       &value_end) ||
        value_end > resource_end || value_end > region_size) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "cleanup element array is out of bounds");
    }
    return Status::Ok();
}

Status PublishRecoveryResource(void* directory_base, uint64_t directory_size,
                               uint64_t region_size,
                               const RecoveryResourceDescriptor& descriptor) {
    MINO_RETURN_IF_ERROR(
        ValidateRecoveryResourceDescriptor(descriptor, region_size));
    for (int attempt = 0; attempt < 4096; ++attempt) {
        auto current = ReadRecoveryDirectory(directory_base, directory_size);
        if (!current.ok()) {
            if (current.status().code() == StatusCode::kUnavailable) {
                std::this_thread::yield();
                continue;
            }
            return current.status();
        }
        RecoveryDirectorySnapshot next = *current;
        uint32_t index = next.resource_count;
        for (uint32_t i = 0; i < next.resource_count; ++i) {
            if (next.resources[i].resource_id == descriptor.resource_id) {
                index = i;
                break;
            }
        }
        if (index == next.resource_count) {
            if (next.resource_count == kRecoveryDirectoryResourceCapacity) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     "recovery resource directory is full");
            }
            ++next.resource_count;
        }
        next.resources[index] = descriptor;
        // Any resource topology change invalidates a previously asserted
        // complete reference set until it is republished atomically.
        next.flags &= ~kRecoveryDirectoryReferencesComplete;
        Status published = TryPublishSnapshot(
            static_cast<RecoveryDirectoryImage*>(directory_base), &next);
        if (published.ok()) {
            return published;
        }
        if (published.code() != StatusCode::kWouldBlock) {
            return published;
        }
        std::this_thread::yield();
    }
    return Status::Error(StatusCode::kUnavailable,
                         "recovery resource publication remained contended");
}

Status PublishRecoveryReferences(
    void* directory_base, uint64_t directory_size,
    std::span<const RecoveryObjectReference> references, bool complete) {
    if (references.size() > kRecoveryDirectoryReferenceCapacity) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "recovery reference set exceeds fixed capacity");
    }
    for (const RecoveryObjectReference& reference : references) {
        if (reference.resource_id == 0 || reference.generation == 0 ||
            reference.reserved != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "recovery reference is invalid");
        }
    }
    for (int attempt = 0; attempt < 4096; ++attempt) {
        auto current = ReadRecoveryDirectory(directory_base, directory_size);
        if (!current.ok()) {
            if (current.status().code() == StatusCode::kUnavailable) {
                std::this_thread::yield();
                continue;
            }
            return current.status();
        }
        RecoveryDirectorySnapshot next = *current;
        for (RecoveryObjectReference& reference : next.references) {
            reference = RecoveryObjectReference{};
        }
        next.reference_count = static_cast<uint32_t>(references.size());
        for (size_t i = 0; i < references.size(); ++i) {
            next.references[i] = references[i];
        }
        if (complete) {
            next.flags |= kRecoveryDirectoryReferencesComplete;
        } else {
            next.flags &= ~kRecoveryDirectoryReferencesComplete;
        }
        Status published = TryPublishSnapshot(
            static_cast<RecoveryDirectoryImage*>(directory_base), &next);
        if (published.ok()) {
            return published;
        }
        if (published.code() != StatusCode::kWouldBlock) {
            return published;
        }
        std::this_thread::yield();
    }
    return Status::Error(StatusCode::kUnavailable,
                         "recovery reference publication remained contended");
}

}  // namespace mino