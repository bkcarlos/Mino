// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/attachment_directory.h"

#include <new>

#include "mino/shm/region/superblock.h"

namespace mino {
namespace {

uint32_t ControlCrc(const AttachmentDirectoryControl& control) {
    return Crc32(&control,
                 offsetof(AttachmentDirectoryControl, immutable_crc32));
}

uint64_t LoadStateWord(const AttachmentSlot& slot) {
    return std::atomic_ref(const_cast<uint64_t&>(slot.state_word))
        .load(std::memory_order_acquire);
}

void StoreStateWord(AttachmentSlot& slot, uint64_t word) {
    std::atomic_ref(slot.state_word).store(word, std::memory_order_release);
}

bool CompareExchangeStateWord(AttachmentSlot& slot, uint64_t* expected,
                              uint64_t desired) {
    return std::atomic_ref(slot.state_word)
        .compare_exchange_strong(*expected, desired, std::memory_order_acq_rel,
                                 std::memory_order_acquire);
}

void StoreIdentity(AttachmentSlot& slot, const ProcessIdentity& identity) {
    std::atomic_ref(slot.identity.node_id)
        .store(identity.node_id, std::memory_order_relaxed);
    std::atomic_ref(slot.identity.process_id)
        .store(identity.process_id, std::memory_order_relaxed);
    std::atomic_ref(slot.identity.process_epoch)
        .store(identity.process_epoch, std::memory_order_relaxed);
    std::atomic_ref(slot.identity.start_time_ns)
        .store(identity.start_time_ns, std::memory_order_relaxed);
}

ProcessIdentity LoadIdentity(const AttachmentSlot& slot) {
    ProcessIdentity identity;
    identity.node_id =
        std::atomic_ref(const_cast<uint64_t&>(slot.identity.node_id))
            .load(std::memory_order_relaxed);
    identity.process_id =
        std::atomic_ref(const_cast<uint64_t&>(slot.identity.process_id))
            .load(std::memory_order_relaxed);
    identity.process_epoch =
        std::atomic_ref(const_cast<uint64_t&>(slot.identity.process_epoch))
            .load(std::memory_order_relaxed);
    identity.start_time_ns =
        std::atomic_ref(const_cast<uint64_t&>(slot.identity.start_time_ns))
            .load(std::memory_order_relaxed);
    return identity;
}

void ClearSlotPayload(AttachmentSlot& slot) {
    StoreIdentity(slot, ProcessIdentity{});
    std::atomic_ref(slot.role).store(0, std::memory_order_relaxed);
    std::atomic_ref(slot.flags).store(0, std::memory_order_relaxed);
    std::atomic_ref(slot.heartbeat_ns).store(0, std::memory_order_relaxed);
    std::atomic_ref(slot.attach_service_epoch)
        .store(0, std::memory_order_relaxed);
}

Status ValidateImage(const void* base, uint64_t available_size,
                     const AttachmentDirectoryImage** image_out) {
    if (base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "attachment directory base is null");
    }
    if (available_size < sizeof(AttachmentDirectoryImage)) {
        return Status::Error(StatusCode::kCorruption,
                             "attachment directory storage is truncated");
    }
    if (reinterpret_cast<uintptr_t>(base) %
            alignof(AttachmentDirectoryImage) !=
        0) {
        return Status::Error(StatusCode::kCorruption,
                             "attachment directory is misaligned");
    }
    const auto* image = static_cast<const AttachmentDirectoryImage*>(base);
    if (image->control.magic != kAttachmentDirectoryMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "attachment directory magic mismatch");
    }
    if (image->control.version != kAttachmentDirectoryVersion ||
        image->control.header_size != sizeof(AttachmentDirectoryControl) ||
        image->control.slot_capacity != kAttachmentDirectorySlotCapacity ||
        image->control.max_subordinate_writable !=
            kMaxSubordinateWritableAttachments) {
        return Status::Error(StatusCode::kUnsupported,
                             "unsupported attachment directory ABI");
    }
    if (image->control.immutable_crc32 != ControlCrc(image->control)) {
        return Status::Error(StatusCode::kCorruption,
                             "attachment directory control CRC mismatch");
    }
    *image_out = image;
    return Status::Ok();
}

bool SlotHoldsRole(const AttachmentSlot& slot, AttachmentRole role) {
    return std::atomic_ref(const_cast<uint32_t&>(slot.role))
               .load(std::memory_order_relaxed) ==
           static_cast<uint32_t>(role);
}

bool IsOccupiedState(AttachmentSlotState state) {
    return state == AttachmentSlotState::kClaiming ||
           state == AttachmentSlotState::kLive ||
           state == AttachmentSlotState::kClosing;
}

// Returns true when the slot may be overwritten. Alive/Unknown never reclaim.
bool TryReclaimDeadSlot(AttachmentSlot& slot) {
    uint64_t word = LoadStateWord(slot);
    AttachmentSlotState state = AttachmentStateOf(word);
    if (state == AttachmentSlotState::kFree) {
        return true;
    }
    if (!IsOccupiedState(state)) {
        return false;
    }
    const ProcessIdentity identity = LoadIdentity(slot);
    if (identity.IsZero() && state == AttachmentSlotState::kClaiming) {
        // Crash between Claim CAS and identity publication.
        const uint64_t generation = AttachmentStateGeneration(word);
        const uint64_t free_word =
            EncodeAttachmentStateWord(generation, AttachmentSlotState::kFree);
        if (CompareExchangeStateWord(slot, &word, free_word)) {
            ClearSlotPayload(slot);
            return true;
        }
        return AttachmentStateOf(LoadStateWord(slot)) ==
               AttachmentSlotState::kFree;
    }
    const ProcessIdentityLiveness liveness = ProbeProcessIdentity(identity);
    if (liveness != ProcessIdentityLiveness::kDead) {
        return false;
    }
    const uint64_t generation = AttachmentStateGeneration(word);
    const uint64_t free_word =
        EncodeAttachmentStateWord(generation, AttachmentSlotState::kFree);
    if (!CompareExchangeStateWord(slot, &word, free_word)) {
        return AttachmentStateOf(LoadStateWord(slot)) ==
               AttachmentSlotState::kFree;
    }
    ClearSlotPayload(slot);
    return true;
}

uint32_t CountOccupiedSubordinateWriters(AttachmentDirectoryImage& image) {
    uint32_t count = 0;
    for (uint32_t i = 0; i < kAttachmentDirectorySlotCapacity; ++i) {
        AttachmentSlot& slot = image.slots[i];
        const uint64_t word = LoadStateWord(slot);
        if (!IsOccupiedState(AttachmentStateOf(word))) {
            continue;
        }
        if (SlotHoldsRole(slot, AttachmentRole::kSubordinateWritable)) {
            ++count;
        }
    }
    return count;
}

}  // namespace

Status InitializeAttachmentDirectory(void* attachment_directory_base,
                                     uint64_t available_size) {
    if (attachment_directory_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "attachment directory base is null");
    }
    if (available_size < sizeof(AttachmentDirectoryImage)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "attachment directory storage is too small");
    }
    if (reinterpret_cast<uintptr_t>(attachment_directory_base) %
            alignof(AttachmentDirectoryImage) !=
        0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "attachment directory base is misaligned");
    }
    auto* image = new (attachment_directory_base) AttachmentDirectoryImage();
    image->control.magic = kAttachmentDirectoryMagic;
    image->control.version = kAttachmentDirectoryVersion;
    image->control.header_size =
        static_cast<uint16_t>(sizeof(AttachmentDirectoryControl));
    image->control.slot_capacity = kAttachmentDirectorySlotCapacity;
    image->control.max_subordinate_writable =
        kMaxSubordinateWritableAttachments;
    image->control.immutable_crc32 = ControlCrc(image->control);
    for (uint32_t i = 0; i < kAttachmentDirectorySlotCapacity; ++i) {
        StoreStateWord(image->slots[i], EncodeAttachmentStateWord(
                                            /*generation=*/0,
                                            AttachmentSlotState::kFree));
        ClearSlotPayload(image->slots[i]);
    }
    return Status::Ok();
}

Status ValidateAttachmentDirectory(const void* attachment_directory_base,
                                   uint64_t available_size) {
    const AttachmentDirectoryImage* image = nullptr;
    return ValidateImage(attachment_directory_base, available_size, &image);
}

Result<AttachmentRegistration> ClaimAttachmentSlot(
    void* attachment_directory_base, uint64_t available_size,
    AttachmentRole role, const ProcessIdentity& identity,
    uint64_t attach_service_epoch) {
    if (role != AttachmentRole::kSupervisor &&
        role != AttachmentRole::kSubordinateWritable &&
        role != AttachmentRole::kReader) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid attachment role");
    }
    if (identity.IsZero()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "attachment identity must be nonzero");
    }
    const AttachmentDirectoryImage* const_image = nullptr;
    MINO_RETURN_IF_ERROR(
        ValidateImage(attachment_directory_base, available_size, &const_image));
    auto* image =
        const_cast<AttachmentDirectoryImage*>(const_image);

    if (role == AttachmentRole::kSubordinateWritable &&
        CountOccupiedSubordinateWriters(*image) >=
            kMaxSubordinateWritableAttachments) {
        // Best-effort reclaim before failing policy, so Dead slots do not
        // permanently exhaust the budget.
        for (uint32_t i = 0; i < kAttachmentDirectorySlotCapacity; ++i) {
            (void)TryReclaimDeadSlot(image->slots[i]);
        }
        if (CountOccupiedSubordinateWriters(*image) >=
            kMaxSubordinateWritableAttachments) {
            return Status::Error(
                StatusCode::kResourceExhausted,
                "subordinate writable attachment policy exhausted");
        }
    }

    for (uint32_t attempt = 0; attempt < kAttachmentDirectorySlotCapacity;
         ++attempt) {
        for (uint32_t i = 0; i < kAttachmentDirectorySlotCapacity; ++i) {
            AttachmentSlot& slot = image->slots[i];
            if (!TryReclaimDeadSlot(slot)) {
                continue;
            }
            uint64_t word = LoadStateWord(slot);
            if (AttachmentStateOf(word) != AttachmentSlotState::kFree) {
                continue;
            }
            const uint64_t generation = AttachmentStateGeneration(word);
            if (generation >= kMaxAttachmentSlotGeneration) {
                continue;
            }
            const uint64_t claiming = EncodeAttachmentStateWord(
                generation + 1, AttachmentSlotState::kClaiming);
            if (!CompareExchangeStateWord(slot, &word, claiming)) {
                continue;
            }
            // Publish payload before Live. A crash leaves Claiming with either
            // zero identity (reclaimable) or a probeable identity.
            std::atomic_ref(slot.role).store(static_cast<uint32_t>(role),
                                             std::memory_order_relaxed);
            std::atomic_ref(slot.flags).store(0, std::memory_order_relaxed);
            std::atomic_ref(slot.attach_service_epoch)
                .store(attach_service_epoch, std::memory_order_relaxed);
            std::atomic_ref(slot.heartbeat_ns)
                .store(0, std::memory_order_relaxed);
            StoreIdentity(slot, identity);
            const uint64_t live = EncodeAttachmentStateWord(
                generation + 1, AttachmentSlotState::kLive);
            uint64_t expected = claiming;
            if (!CompareExchangeStateWord(slot, &expected, live)) {
                // Lost the slot; try another.
                continue;
            }
            return AttachmentRegistration{.slot_index = i,
                                          .generation = generation + 1};
        }
    }
    return Status::Error(StatusCode::kResourceExhausted,
                         "attachment directory has no free slots");
}

Status ReleaseAttachmentSlot(void* attachment_directory_base,
                             uint64_t available_size, uint32_t slot_index,
                             uint64_t generation,
                             const ProcessIdentity& identity) {
    const AttachmentDirectoryImage* const_image = nullptr;
    MINO_RETURN_IF_ERROR(
        ValidateImage(attachment_directory_base, available_size, &const_image));
    if (slot_index >= kAttachmentDirectorySlotCapacity) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "attachment slot index out of range");
    }
    auto* image = const_cast<AttachmentDirectoryImage*>(const_image);
    AttachmentSlot& slot = image->slots[slot_index];
    uint64_t word = LoadStateWord(slot);
    if (AttachmentStateGeneration(word) != generation ||
        AttachmentStateOf(word) != AttachmentSlotState::kLive ||
        LoadIdentity(slot) != identity) {
        return Status::Error(StatusCode::kUnavailable,
                             "attachment slot release is stale");
    }
    const uint64_t closing =
        EncodeAttachmentStateWord(generation, AttachmentSlotState::kClosing);
    if (!CompareExchangeStateWord(slot, &word, closing)) {
        return Status::Error(StatusCode::kUnavailable,
                             "attachment slot changed before release");
    }
    ClearSlotPayload(slot);
    StoreStateWord(
        slot, EncodeAttachmentStateWord(generation, AttachmentSlotState::kFree));
    return Status::Ok();
}

Status HeartbeatAttachmentSlot(void* attachment_directory_base,
                               uint64_t available_size, uint32_t slot_index,
                               uint64_t generation,
                               const ProcessIdentity& identity,
                               uint64_t heartbeat_ns) {
    const AttachmentDirectoryImage* const_image = nullptr;
    MINO_RETURN_IF_ERROR(
        ValidateImage(attachment_directory_base, available_size, &const_image));
    if (slot_index >= kAttachmentDirectorySlotCapacity) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "attachment slot index out of range");
    }
    auto* image = const_cast<AttachmentDirectoryImage*>(const_image);
    AttachmentSlot& slot = image->slots[slot_index];
    const uint64_t word = LoadStateWord(slot);
    if (AttachmentStateGeneration(word) != generation ||
        AttachmentStateOf(word) != AttachmentSlotState::kLive ||
        LoadIdentity(slot) != identity) {
        return Status::Error(StatusCode::kUnavailable,
                             "attachment heartbeat target is stale");
    }
    std::atomic_ref(slot.heartbeat_ns)
        .store(heartbeat_ns, std::memory_order_release);
    return Status::Ok();
}

Status PrepareAttachmentDirectoryForSupervisorRecovery(
    void* attachment_directory_base, uint64_t available_size) {
    const AttachmentDirectoryImage* const_image = nullptr;
    MINO_RETURN_IF_ERROR(
        ValidateImage(attachment_directory_base, available_size, &const_image));
    auto* image = const_cast<AttachmentDirectoryImage*>(const_image);

    for (uint32_t i = 0; i < kAttachmentDirectorySlotCapacity; ++i) {
        AttachmentSlot& slot = image->slots[i];
        uint64_t word = LoadStateWord(slot);
        AttachmentSlotState state = AttachmentStateOf(word);
        if (!IsOccupiedState(state)) {
            continue;
        }
        const ProcessIdentity identity = LoadIdentity(slot);
        if (identity.IsZero() && state == AttachmentSlotState::kClaiming) {
            (void)TryReclaimDeadSlot(slot);
            continue;
        }
        const bool subordinate_writer =
            SlotHoldsRole(slot, AttachmentRole::kSubordinateWritable);
        const ProcessIdentityLiveness liveness = ProbeProcessIdentity(identity);
        if (liveness == ProcessIdentityLiveness::kDead) {
            (void)TryReclaimDeadSlot(slot);
            continue;
        }
        if (subordinate_writer) {
            if (liveness == ProcessIdentityLiveness::kAlive) {
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "live subordinate writable attachment blocks destructive recovery");
            }
            return Status::Error(
                StatusCode::kUnavailable,
                "subordinate writable liveness is unknown; destructive recovery refused");
        }
        // Supervisor/reader Alive or Unknown: reclaim only when Dead. A live
        // previous supervisor is already excluded by the advisory lock + SuperBlock
        // Probe before this helper runs. Live readers do not block recovery.
        if (liveness == ProcessIdentityLiveness::kAlive ||
            liveness == ProcessIdentityLiveness::kUnknown) {
            if (SlotHoldsRole(slot, AttachmentRole::kSupervisor)) {
                if (liveness == ProcessIdentityLiveness::kAlive) {
                    return Status::Error(
                        StatusCode::kWouldBlock,
                        "live supervisor attachment slot blocks recovery");
                }
                return Status::Error(
                    StatusCode::kUnavailable,
                    "supervisor attachment liveness is unknown; recovery refused");
            }
        }
    }
    return Status::Ok();
}

uint32_t CountLiveSubordinateWritableSlots(
    const void* attachment_directory_base, uint64_t available_size) {
    const AttachmentDirectoryImage* image = nullptr;
    if (!ValidateImage(attachment_directory_base, available_size, &image).ok()) {
        return 0;
    }
    uint32_t count = 0;
    for (uint32_t i = 0; i < kAttachmentDirectorySlotCapacity; ++i) {
        const AttachmentSlot& slot = image->slots[i];
        const uint64_t word = LoadStateWord(slot);
        if (!IsOccupiedState(AttachmentStateOf(word))) {
            continue;
        }
        if (SlotHoldsRole(slot, AttachmentRole::kSubordinateWritable)) {
            ++count;
        }
    }
    return count;
}

}  // namespace mino
