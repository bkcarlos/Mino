// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/journal_channel_recovery.h"

#include <cstdint>
#include <new>

namespace mino {

Result<BroadcastPublicationView> BroadcastPublicationView::Attach(
    const void* shm_base) noexcept {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "broadcast publication view base must not be null");
    }
    if (reinterpret_cast<uintptr_t>(shm_base) %
            alignof(BroadcastChannel::ControlBlock) !=
        0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "broadcast publication view base must be 64-byte aligned");
    }

    const auto* control =
        static_cast<const BroadcastChannel::ControlBlock*>(shm_base);
    if (control->magic.load(std::memory_order_acquire) !=
        BroadcastChannel::kMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "broadcast control block magic mismatch");
    }
    if (control->layout_version.load(std::memory_order_acquire) !=
        BroadcastChannel::kLayoutVersion) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "broadcast layout version mismatch");
    }
    const uint64_t capacity = control->capacity;
    if (capacity < 2 || (capacity & (capacity - 1)) != 0 ||
        capacity > (uint64_t{1} << 32)) {
        return Status::Error(StatusCode::kCorruption,
                             "broadcast control block capacity is invalid");
    }
    const auto* bytes = static_cast<const unsigned char*>(shm_base);
    const auto* slots = reinterpret_cast<const IndexSlot*>(
        bytes + BroadcastChannel::SlotsOffset());
    return BroadcastPublicationView(control, slots, capacity);
}

BroadcastPublicationView::PublicationVisibility
BroadcastPublicationView::InspectPublication(
    uint64_t sequence, ShmHandle payload) const noexcept {
    const uint64_t published =
        control_->publisher_cursor.load(std::memory_order_acquire);
    if (published <= sequence) {
        return PublicationVisibility::kNotVisible;
    }

    const IndexSlot& slot = slots_[sequence & mask_];
    if (slot.sequence_num.load(std::memory_order_acquire) != sequence) {
        return PublicationVisibility::kIndeterminate;
    }
    const uint32_t state = slot.state.load(std::memory_order_acquire);
    if (state == static_cast<uint32_t>(SlotState::kAborted)) {
        return PublicationVisibility::kNotVisible;
    }
    if ((state == static_cast<uint32_t>(SlotState::kReady) ||
         state == static_cast<uint32_t>(SlotState::kRetiring) ||
         state == static_cast<uint32_t>(SlotState::kRetired)) &&
        slot.payload == payload) {
        return PublicationVisibility::kVisible;
    }
    return PublicationVisibility::kIndeterminate;
}

Status JournalChannelRecoveryCoordinator::RegisterChannel(
    uint64_t channel_id, const SpscChannel& channel) {
    return Register(Registration{
        .channel_id = channel_id,
        .channel_kind = PublicationChannelKind::kSpsc,
        .spsc = &channel,
        .mpsc = nullptr,
        .broadcast = std::nullopt,
    });
}

Status JournalChannelRecoveryCoordinator::RegisterChannel(
    uint64_t channel_id, const MpscChannel& channel) {
    return Register(Registration{
        .channel_id = channel_id,
        .channel_kind = PublicationChannelKind::kMpsc,
        .spsc = nullptr,
        .mpsc = &channel,
        .broadcast = std::nullopt,
    });
}

Status JournalChannelRecoveryCoordinator::RegisterChannel(
    uint64_t channel_id, const BroadcastPublicationView& channel) {
    return Register(Registration{
        .channel_id = channel_id,
        .channel_kind = PublicationChannelKind::kBroadcast,
        .spsc = nullptr,
        .mpsc = nullptr,
        .broadcast = channel,
    });
}

Status JournalChannelRecoveryCoordinator::Register(
    Registration registration) {
    if (registration.channel_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery channel id must be non-zero");
    }
    if (Find(registration.channel_id) != nullptr) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "channel id is already registered for recovery");
    }
    try {
        registrations_.push_back(registration);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "channel recovery registry allocation failed");
    }
    return Status::Ok();
}

const JournalChannelRecoveryCoordinator::Registration*
JournalChannelRecoveryCoordinator::Find(uint64_t channel_id) const noexcept {
    for (const Registration& registration : registrations_) {
        if (registration.channel_id == channel_id) {
            return &registration;
        }
    }
    return nullptr;
}

uint32_t JournalChannelRecoveryCoordinator::RecoverOrphans(
    IdentityProbe identity_probe, void* identity_probe_context) noexcept {
    return journal_->RecoverOrphans(identity_probe, identity_probe_context,
                                    &ResolveCommittedOrphan, this);
}

AllocationJournal::CommittedOrphanAction
JournalChannelRecoveryCoordinator::ResolveCommittedOrphan(
    const AllocationTransaction& transaction,
    const PublicationBinding& binding, void* context) noexcept {
    (void)transaction;
    return static_cast<JournalChannelRecoveryCoordinator*>(context)->Resolve(
        binding);
}

AllocationJournal::CommittedOrphanAction
JournalChannelRecoveryCoordinator::Resolve(
    const PublicationBinding& binding) const noexcept {
    if (binding.payload.IsNull()) {
        return AllocationJournal::CommittedOrphanAction::kDefer;
    }
    const Registration* registration = Find(binding.channel_id);
    if (registration == nullptr ||
        registration->channel_kind != binding.channel_kind) {
        return AllocationJournal::CommittedOrphanAction::kDefer;
    }

    switch (binding.channel_kind) {
        case PublicationChannelKind::kSpsc:
            switch (registration->spsc->InspectPublication(
                binding.sequence, binding.payload)) {
                case SpscChannel::PublicationVisibility::kVisible:
                    return AllocationJournal::CommittedOrphanAction::kFinalize;
                case SpscChannel::PublicationVisibility::kNotVisible:
                    return AllocationJournal::CommittedOrphanAction::kRollback;
                case SpscChannel::PublicationVisibility::kIndeterminate:
                    return AllocationJournal::CommittedOrphanAction::kDefer;
            }
            break;
        case PublicationChannelKind::kMpsc:
            switch (registration->mpsc->InspectPublication(
                binding.sequence, binding.payload)) {
                case MpscChannel::PublicationVisibility::kVisible:
                    return AllocationJournal::CommittedOrphanAction::kFinalize;
                case MpscChannel::PublicationVisibility::kNotVisible:
                    return AllocationJournal::CommittedOrphanAction::kRollback;
                case MpscChannel::PublicationVisibility::kIndeterminate:
                    return AllocationJournal::CommittedOrphanAction::kDefer;
            }
            break;
        case PublicationChannelKind::kBroadcast:
            switch (registration->broadcast->InspectPublication(
                binding.sequence, binding.payload)) {
                case BroadcastPublicationView::PublicationVisibility::kVisible:
                    return AllocationJournal::CommittedOrphanAction::kFinalize;
                case BroadcastPublicationView::PublicationVisibility::kNotVisible:
                    return AllocationJournal::CommittedOrphanAction::kRollback;
                case BroadcastPublicationView::PublicationVisibility::kIndeterminate:
                    return AllocationJournal::CommittedOrphanAction::kDefer;
            }
            break;
        case PublicationChannelKind::kNone:
            break;
    }
    return AllocationJournal::CommittedOrphanAction::kDefer;
}

}  // namespace mino
