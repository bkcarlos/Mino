// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/shm/recovery/scanner.h"

#include <chrono>
#include <limits>
#include <string>
#include <thread>
#include <utility>

#include <unistd.h>

#include "mino/common/checked_arithmetic.h"

namespace mino::shm::recovery {
namespace {

std::string Finding(const char* kind, uint32_t class_id, uint32_t slot,
                    const std::string& what) {
    std::string out;
    out.reserve(96);
    out += kind;
    out += " class=";
    out += std::to_string(class_id);
    out += " slot=";
    out += std::to_string(slot);
    out += ": ";
    out += what;
    return out;
}

}  // namespace

std::string_view ObjectStateName(uint32_t value) {
    switch (static_cast<ObjectState>(value)) {
        case ObjectState::kFree:
            return "FREE";
        case ObjectState::kAllocated:
            return "ALLOCATED";
        case ObjectState::kBuilding:
            return "BUILDING";
        case ObjectState::kPublished:
            return "PUBLISHED";
        case ObjectState::kRetired:
            return "RETIRED";
        case ObjectState::kAborting:
            return "ABORTING";
        case ObjectState::kReclaiming:
            return "RECLAIMING";
        case ObjectState::kAllocating:
            return "ALLOCATING";
    }
    return "INVALID";
}

RecoveryOwner::RecoveryOwner(RecoveryOwnerState* state, uint64_t pid) noexcept
    : state_(state), pid_(pid) {}

RecoveryOwner::RecoveryOwner(RecoveryOwnerState* state, uint64_t pid,
                             std::atomic<uint64_t>* external_token) noexcept
    : state_(state), pid_(pid), external_token_(external_token) {}

uint64_t RecoveryOwner::LeaseToken() const noexcept {
    return external_token_ != nullptr
               ? external_token_->load(std::memory_order_acquire)
               : lease_token_;
}

void RecoveryOwner::StoreLeaseToken(uint64_t token) noexcept {
    lease_token_ = token;
    if (external_token_ != nullptr) {
        external_token_->store(token, std::memory_order_release);
    }
}

uint64_t RecoveryOwner::NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

void RecoveryOwner::Initialize(RecoveryOwnerState* state) noexcept {
    if (state == nullptr) {
        return;
    }
    state->magic = RecoveryOwnerState::kMagic;
    state->reserved0 = 0;
    state->owner_pid.store(0, std::memory_order_relaxed);
    state->epoch.store(0, std::memory_order_relaxed);
    state->lease_deadline_ns.store(0, std::memory_order_relaxed);
    state->heartbeat_ns.store(0, std::memory_order_relaxed);
    for (auto& word : state->reserved1) {
        word = 0;
    }
    for (auto& word : state->reserved2) {
        word = 0;
    }
}

Status RecoveryOwner::TryAcquire() {
    if (state_ == nullptr || pid_ == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery owner is not initialized");
    }
    if (state_->magic != RecoveryOwnerState::kMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "recovery owner block has bad magic");
    }
    const uint64_t now = NowNs();
    if (IsOwner()) {
        return RenewLease();
    }
    if (now > std::numeric_limits<uint64_t>::max() - kLeaseDurationNs) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "legacy recovery lease deadline overflow");
    }
    const uint64_t new_token = now + kLeaseDurationNs;
    uint64_t expected =
        state_->lease_deadline_ns.load(std::memory_order_acquire);
    for (;;) {
        if (expected > now) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "recovery ownership already held");
        }
        if (state_->lease_deadline_ns.compare_exchange_strong(
                expected, new_token, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            break;
        }
    }
    StoreLeaseToken(new_token);
    state_->owner_pid.store(pid_, std::memory_order_release);
    state_->epoch.fetch_add(1, std::memory_order_acq_rel);
    state_->heartbeat_ns.store(now, std::memory_order_release);
    return Status::Ok();
}

Status RecoveryOwner::RenewLease() {
    if (!IsOwner()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "cannot renew: not the recovery owner");
    }
    const uint64_t now = NowNs();
    if (now > std::numeric_limits<uint64_t>::max() - kLeaseDurationNs) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "legacy recovery lease deadline overflow");
    }
    const uint64_t new_token = now + kLeaseDurationNs;
    uint64_t expected = LeaseToken();
    if (!state_->lease_deadline_ns.compare_exchange_strong(
            expected, new_token, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        StoreLeaseToken(0);
        return Status::Error(StatusCode::kPermissionDenied,
                             "cannot renew: recovery lease token changed");
    }
    StoreLeaseToken(new_token);
    return Status::Ok();
}

void RecoveryOwner::Heartbeat() noexcept {
    if (IsOwner()) {
        state_->heartbeat_ns.store(NowNs(), std::memory_order_relaxed);
    }
}

void RecoveryOwner::Release() noexcept {
    const uint64_t token = LeaseToken();
    if (state_ == nullptr || token == 0) {
        return;
    }
    uint64_t expected = token;
    (void)state_->lease_deadline_ns.compare_exchange_strong(
        expected, 0, std::memory_order_acq_rel,
        std::memory_order_acquire);
    StoreLeaseToken(0);
}

bool RecoveryOwner::IsOwner() const noexcept {
    const uint64_t token = LeaseToken();
    return state_ != nullptr && pid_ != 0 && token != 0 &&
           state_->lease_deadline_ns.load(std::memory_order_acquire) == token &&
           token > NowNs();
}

uint64_t RecoveryOwner::CurrentOwner() const noexcept {
    if (state_ == nullptr) {
        return 0;
    }
    const uint64_t owner = state_->owner_pid.load(std::memory_order_acquire);
    if (owner == 0 ||
        state_->lease_deadline_ns.load(std::memory_order_acquire) <= NowNs()) {
        return 0;
    }
    return owner;
}

uint64_t RecoveryOwner::Epoch() const noexcept {
    return state_ == nullptr ? 0
                             : state_->epoch.load(std::memory_order_acquire);
}

uint64_t RecoveryOwner::LeaseDeadlineNs() const noexcept {
    return state_ == nullptr
               ? 0
               : state_->lease_deadline_ns.load(std::memory_order_acquire);
}

Status RecoveryOwner::WaitForIdle(uint64_t timeout_ns) const {
    const uint64_t deadline = NowNs() + timeout_ns;
    while (NowNs() < deadline) {
        if (IsIdle()) {
            return Status::Ok();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return IsIdle() ? Status::Ok()
                    : Status::Error(StatusCode::kTimeout,
                                    "recovery owner still active");
}

Result<RecoveryScanner> RecoveryScanner::Create(
    std::byte* base, uint64_t size, Layout layout,
    RecoveryScannerOptions options) {
    if (base == nullptr || size == 0 ||
        layout.recovery_state_offset > size ||
        sizeof(RecoveryOwnerState) > size - layout.recovery_state_offset) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "legacy recovery layout is out of bounds");
    }
    if (layout.class_table_offset > size ||
        layout.class_count >
            (size - layout.class_table_offset) / sizeof(ClassDescriptor)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "legacy class table is out of bounds");
    }
    const uintptr_t base_address = reinterpret_cast<uintptr_t>(base);
    auto is_aligned = [&](uint64_t offset, size_t alignment) {
        return offset <= std::numeric_limits<uintptr_t>::max() - base_address &&
               (base_address + static_cast<uintptr_t>(offset)) % alignment == 0;
    };
    if (!is_aligned(layout.recovery_state_offset,
                    alignof(RecoveryOwnerState)) ||
        !is_aligned(layout.class_table_offset, alignof(ClassDescriptor))) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "legacy recovery layout is misaligned");
    }
    auto* owner_state = reinterpret_cast<RecoveryOwnerState*>(
        base + layout.recovery_state_offset);
    if (owner_state->magic != RecoveryOwnerState::kMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "recovery owner block has bad magic");
    }
    const auto* classes = reinterpret_cast<const ClassDescriptor*>(
        base + layout.class_table_offset);
    for (uint32_t i = 0; i < layout.class_count; ++i) {
        const ClassDescriptor& cls = classes[i];
        const uint64_t words = (static_cast<uint64_t>(cls.slot_count) + 63) / 64;
        if (cls.bitmap_offset > size ||
            words > (size - cls.bitmap_offset) / sizeof(BitmapWord) ||
            !is_aligned(cls.bitmap_offset, alignof(BitmapWord)) ||
            cls.slot_stride < sizeof(SlabHeader) ||
            cls.slot_stride % alignof(SlabHeader) != 0 ||
            cls.slots_offset > size ||
            !is_aligned(cls.slots_offset, alignof(SlabHeader)) ||
            (cls.slot_count != 0 &&
             (sizeof(SlabHeader) > size - cls.slots_offset ||
              static_cast<uint64_t>(cls.slot_count - 1) >
                  (size - cls.slots_offset - sizeof(SlabHeader)) /
                      cls.slot_stride))) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "legacy allocator class is out of bounds");
        }
    }
    auto legacy_context = std::make_shared<LegacyOwnershipContext>();
    legacy_context->state = owner_state;
    legacy_context->pid = SelfPid();
    RecoveryOwnership ownership{.context = legacy_context.get(),
                                .is_owner = &LegacyIsOwner,
                                .heartbeat = &LegacyHeartbeat};
    RecoveryScanner scanner(CentralSlabAllocator{}, ownership, options,
                            owner_state);
    scanner.legacy_context_ = std::move(legacy_context);
    scanner.legacy_layout_ = layout;
    scanner.legacy_base_ = base;
    scanner.legacy_size_ = size;
    return scanner;
}

Result<RecoveryScanner> RecoveryScanner::Create(
    void* allocator_base, uint64_t available_size,
    RecoveryOwnership ownership, RecoveryScannerOptions options) {
    MINO_ASSIGN_OR_RETURN(
        CentralSlabAllocator allocator,
        CentralSlabAllocator::Attach(allocator_base, available_size));
    return Create(std::move(allocator), ownership, options);
}

Result<RecoveryScanner> RecoveryScanner::Create(
    CentralSlabAllocator allocator, RecoveryOwnership ownership,
    RecoveryScannerOptions options, uint32_t resource_id,
    std::span<const RecoveryObjectReference> references,
    bool references_complete) {
    if (allocator.total_slot_count() == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery allocator has no slots");
    }
    if (references_complete && resource_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "complete references require a resource id");
    }
    RecoveryScanner scanner(std::move(allocator), ownership, options);
    scanner.resource_id_ = resource_id;
    scanner.references_.assign(references.begin(), references.end());
    scanner.references_complete_ = references_complete;
    return scanner;
}

Result<RecoveryScanner> RecoveryScanner::Create(
    LargeObjectPool pool, uint32_t resource_id, RecoveryOwnership ownership,
    RecoveryScannerOptions options,
    std::span<const RecoveryObjectReference> references,
    bool references_complete) {
    if (pool.segment_count() == 0 || resource_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large pool recovery resource is invalid");
    }
    RecoveryScanner scanner(std::move(pool), resource_id, ownership, options);
    scanner.references_.assign(references.begin(), references.end());
    scanner.references_complete_ = references_complete;
    return scanner;
}

Result<RecoveryReport> RecoveryScanner::Scan() {
    RecoveryReport report;
    if (large_pool_.has_value()) {
        MINO_RETURN_IF_ERROR(ScanLargePool(report, options_.repair));
    } else {
        MINO_RETURN_IF_ERROR(ScanSlots(report, options_.repair));
    }
    return report;
}

Status RecoveryScanner::ReclaimOrphanSlabs() {
    if (!ownership_.IsOwner()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "reclaim requires Region recovery ownership");
    }
    RecoveryReport report;
    return large_pool_.has_value()
               ? ScanLargePool(report, /*repair=*/true)
               : ScanSlots(report, /*repair=*/true);
}

Status RecoveryScanner::CleanupStaleAcks(const AckScanInput& input,
                                         uint64_t* cleared) {
    if (!ownership_.IsOwner()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "ACK cleanup requires recovery ownership");
    }
    if (input.bitmap_count > 0 && input.bitmaps == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "bitmap_count > 0 but bitmaps is null");
    }
    if (input.bitmap_count > 0 &&
        reinterpret_cast<uintptr_t>(input.bitmaps) %
                std::atomic_ref<uint64_t>::required_alignment !=
            0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "ACK bitmap storage is misaligned");
    }
    const uint64_t dead_mask = ~input.live_subscriber_mask;
    uint64_t count = 0;
    for (uint32_t i = 0; i < input.bitmap_count; ++i) {
        if ((i % 256u) == 0) {
            ownership_.Heartbeat();
            if (!ownership_.IsOwner()) {
                return Status::Error(StatusCode::kUnavailable,
                                     "recovery ownership lost during ACK cleanup");
            }
        }
        std::atomic_ref<uint64_t> bitmap(input.bitmaps[i]);
        const uint64_t before = bitmap.fetch_and(input.live_subscriber_mask,
                                                 std::memory_order_acq_rel);
        if (!ownership_.IsOwner()) {
            return Status::Error(StatusCode::kUnavailable,
                                 "recovery fence lost after ACK cleanup write");
        }
        count += static_cast<uint64_t>(
            __builtin_popcountll(before & dead_mask));
    }
    if (cleared != nullptr) {
        *cleared = count;
    }
    return Status::Ok();
}

Status RecoveryScanner::CleanupGenerationScopedResource(
    std::byte* region_base, uint64_t region_size,
    const RecoveryResourceDescriptor& descriptor,
    RecoveryOwnership ownership, RecoveryReport* report) {
    if (!ownership.IsOwner()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "generation cleanup requires recovery ownership");
    }
    if (region_base == nullptr || report == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "generation cleanup input is null");
    }
    MINO_RETURN_IF_ERROR(
        ValidateRecoveryResourceDescriptor(descriptor, region_size));
    const auto kind = static_cast<RecoveryResourceKind>(descriptor.kind);
    if (kind != RecoveryResourceKind::kChannelAckSource &&
        kind != RecoveryResourceKind::kPinCleanupParticipant) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "resource is not generation-scoped cleanup data");
    }
    auto* control_generation = reinterpret_cast<uint64_t*>(
        region_base + descriptor.control_offset);
    auto* live_mask_word = reinterpret_cast<uint64_t*>(
        region_base + descriptor.control_offset + sizeof(uint64_t));
    const uint64_t active_generation =
        std::atomic_ref(*control_generation).load(std::memory_order_acquire);
    const uint64_t live_mask = kind == RecoveryResourceKind::kChannelAckSource
                                   ? std::atomic_ref(*live_mask_word)
                                         .load(std::memory_order_acquire)
                                   : 0;
    if (active_generation == 0 || active_generation < descriptor.generation) {
        return Status::Error(StatusCode::kCorruption,
                             "cleanup control generation is invalid");
    }
    auto add_count = [](uint64_t delta, uint64_t* total) -> Status {
        uint64_t updated = 0;
        if (!CheckedAddU64(*total, delta, &updated)) {
            return Status::Error(StatusCode::kCorruption,
                                 "generation cleanup count overflow");
        }
        *total = updated;
        return Status::Ok();
    };

    for (uint64_t i = 0; i < descriptor.element_count; ++i) {
        if ((i % 256u) == 0) {
            ownership.Heartbeat();
            if (!ownership.IsOwner()) {
                return Status::Error(
                    StatusCode::kUnavailable,
                    "recovery ownership lost during generation cleanup");
            }
        }
        uint64_t element_delta = 0;
        uint64_t element_offset = 0;
        if (!CheckedMulU64(i, descriptor.element_stride, &element_delta) ||
            !CheckedAddU64(descriptor.offset, element_delta,
                           &element_offset)) {
            return Status::Error(StatusCode::kCorruption,
                                 "cleanup element offset overflow");
        }
        uint64_t generation_offset = 0;
        uint64_t value_offset = 0;
        if (!CheckedAddU64(element_offset, descriptor.generation_offset,
                           &generation_offset) ||
            !CheckedAddU64(element_offset, descriptor.value_offset,
                           &value_offset)) {
            return Status::Error(StatusCode::kCorruption,
                                 "cleanup field offset overflow");
        }
        auto* generation_word = reinterpret_cast<uint64_t*>(
            region_base + generation_offset);
        auto* value_word = reinterpret_cast<uint64_t*>(
            region_base + value_offset);
        const uint64_t generation =
            std::atomic_ref(*generation_word).load(std::memory_order_acquire);
        auto value = std::atomic_ref(*value_word);
        if (generation != active_generation) {
            const uint64_t stale = value.exchange(0, std::memory_order_acq_rel);
            if (kind == RecoveryResourceKind::kChannelAckSource) {
                MINO_RETURN_IF_ERROR(add_count(
                    static_cast<uint64_t>(__builtin_popcountll(stale)),
                    &report->stale_ack_count));
            } else {
                MINO_RETURN_IF_ERROR(
                    add_count(stale, &report->stale_pin_count));
            }
        } else if (kind == RecoveryResourceKind::kChannelAckSource) {
            const uint64_t before =
                value.fetch_and(live_mask, std::memory_order_acq_rel);
            MINO_RETURN_IF_ERROR(add_count(
                static_cast<uint64_t>(
                    __builtin_popcountll(before & ~live_mask)),
                &report->stale_ack_count));
        }
        if (!ownership.IsOwner()) {
            return Status::Error(
                StatusCode::kUnavailable,
                "recovery fence lost after generation cleanup write");
        }
    }
    return Status::Ok();
}

Status RecoveryScanner::VerifyBitmapConsistency() {
    RecoveryReport report;
    if (large_pool_.has_value()) {
        MINO_RETURN_IF_ERROR(ScanLargePool(report, /*repair=*/false));
    } else {
        MINO_RETURN_IF_ERROR(ScanSlots(report, /*repair=*/false));
    }
    if (report.bitmap_inconsistency_count != 0 ||
        report.orphan_slab_count != 0 || report.corrupted_slab_count != 0) {
        return Status::Error(
            StatusCode::kCorruption,
            "allocator verification found inconsistency=" +
                std::to_string(report.bitmap_inconsistency_count) +
                " orphan=" + std::to_string(report.orphan_slab_count) +
                " corruption=" +
                std::to_string(report.corrupted_slab_count));
    }
    return Status::Ok();
}

Status RecoveryScanner::ScanSlots(RecoveryReport& report, bool repair) {
    if (repair && !ownership_.IsOwner()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "repairing scan requires Region recovery ownership");
    }
    if (legacy_base_ != nullptr) {
        return ScanLegacyImage(report, repair);
    }

    for (uint32_t slot = 0; slot < allocator_.total_slot_count(); ++slot) {
        if (repair && (slot % 256u) == 0) {
            ownership_.Heartbeat();
            if (!ownership_.IsOwner()) {
                return Status::Error(StatusCode::kUnavailable,
                                     "Region recovery ownership was lost");
            }
        }

        ++report.slots_scanned;
        const bool occupied = allocator_.IsSlotOccupiedForRecovery(slot);
        const uint16_t expected_class = allocator_.ClassIdForRecovery(slot);
        SlabHeader header{};
        if (!allocator_.ReadSlotByIndex(slot, &header, nullptr)) {
            return Status::Error(StatusCode::kInternal,
                                 "allocator slot disappeared during recovery");
        }
        const uint32_t state =
            header.object_state.load(std::memory_order_acquire);

        if (!occupied) {
            if (state != static_cast<uint32_t>(ObjectState::kFree)) {
                ++report.bitmap_inconsistency_count;
                report.AddDetail(Finding(
                    "bitmap_inconsistency", expected_class, slot,
                    "bitmap free but object_state=" +
                        std::string(ObjectStateName(state)) +
                        (repair ? " (state cleared to FREE)"
                                : " (not repaired)")));
                if (repair) {
                    if (!ownership_.IsOwner()) {
                        return Status::Error(
                            StatusCode::kUnavailable,
                            "recovery fence lost before stale-state repair");
                    }
                    MINO_RETURN_IF_ERROR(
                        allocator_.ClearStaleStateForRecovery(slot, state));
                    if (!ownership_.IsOwner()) {
                        return Status::Error(
                            StatusCode::kUnavailable,
                            "recovery fence lost after stale-state repair");
                    }
                }
            }
            continue;
        }

        // kAllocating and kReclaiming are allocator-private transition states.
        // No Handle is published for kAllocating; a crashed kReclaiming owner
        // already committed to freeing the slot. Both are safe to finish, even
        // if the immutable header was only partially rewritten.
        if (!IsValidPublishedState(state)) {
            if (!IsProtocolReclaimableState(state)) {
                ++report.corrupted_slab_count;
                report.AddDetail(Finding(
                    "corruption", expected_class, slot,
                    "unknown object_state=" + std::to_string(state) +
                        " (NOT auto-repaired; quarantine required)"));
                continue;
            }
            ++report.orphan_slab_count;
            if (repair) {
                if (!ownership_.IsOwner()) {
                    return Status::Error(
                        StatusCode::kUnavailable,
                        "recovery fence lost before orphan repair");
                }
                MINO_RETURN_IF_ERROR(
                    allocator_.ClearSlotForRecovery(slot, state));
                if (!ownership_.IsOwner()) {
                    return Status::Error(
                        StatusCode::kUnavailable,
                        "recovery fence lost after orphan repair");
                }
                ++report.reclaimed_slab_count;
            }
            report.AddDetail(Finding(
                "orphan_slab", expected_class, slot,
                "object_state=" + std::string(ObjectStateName(state)) +
                    (repair ? " reclaimed" : " (not reclaimed)")));
            continue;
        }

        const uint32_t authoritative_generation =
            allocator_.AuthoritativeGenerationForRecovery(slot);
        if (!VerifyImmutableHeader(header) ||
            header.generation.load(std::memory_order_acquire) !=
                authoritative_generation ||
            header.class_id != expected_class ||
            header.object_size > header.capacity) {
            ++report.corrupted_slab_count;
            report.AddDetail(Finding(
                "corruption", expected_class, slot,
                "allocator SlabHeader/generation invariant failed "
                "(NOT auto-repaired; quarantine required)"));
            continue;
        }

        if (state == static_cast<uint32_t>(ObjectState::kAllocated) ||
            state == static_cast<uint32_t>(ObjectState::kBuilding) ||
            state == static_cast<uint32_t>(ObjectState::kAborting)) {
            ++report.orphan_slab_count;
            ++report.unpublished_orphan_candidate_count;
            const uint64_t owner_epoch =
                header.owner_epoch.load(std::memory_order_acquire);
            const uint64_t transaction_id =
                header.allocation_transaction_id.load(std::memory_order_acquire);
            const bool proven_dead =
                CanReclaimUnpublished(owner_epoch, transaction_id);
            if (!proven_dead) {
                ++report.deferred_reclaim_count;
            }
            if (repair && proven_dead) {
                if (!ownership_.IsOwner()) {
                    return Status::Error(
                        StatusCode::kUnavailable,
                        "recovery fence lost before unpublished slab repair");
                }
                MINO_RETURN_IF_ERROR(
                    allocator_.ClearSlotForRecovery(slot, state));
                if (!ownership_.IsOwner()) {
                    return Status::Error(
                        StatusCode::kUnavailable,
                        "recovery fence lost after unpublished slab repair");
                }
                ++report.reclaimed_slab_count;
            }
            report.AddDetail(Finding(
                "unpublished_orphan_candidate", expected_class, slot,
                "object_state=" + std::string(ObjectStateName(state)) +
                    " owner_epoch=" + std::to_string(owner_epoch) +
                    " transaction_id=" + std::to_string(transaction_id) +
                    (repair && proven_dead
                         ? " reclaimed with owner/transaction death proof"
                         : proven_dead ? " (read-only; not reclaimed)"
                                       : " deferred: no Journal/owner-death proof")));
            continue;
        }

        if (state == static_cast<uint32_t>(ObjectState::kPublished) &&
            references_complete_ &&
            !IsReferenced(slot, authoritative_generation)) {
            ++report.orphan_slab_count;
            ++report.published_orphan_candidate_count;
            const bool destructive = CanReclaimPublishedOrphan();
            if (!destructive) {
                ++report.deferred_reclaim_count;
            }
            if (repair && destructive) {
                if (!ownership_.IsOwner()) {
                    return Status::Error(
                        StatusCode::kUnavailable,
                        "recovery fence lost before published candidate repair");
                }
                MINO_RETURN_IF_ERROR(
                    allocator_.ClearSlotForRecovery(slot, state));
                if (!ownership_.IsOwner()) {
                    return Status::Error(
                        StatusCode::kUnavailable,
                        "recovery fence lost after published candidate repair");
                }
                ++report.reclaimed_slab_count;
            }
            report.AddDetail(Finding(
                "published_orphan_candidate", expected_class, slot,
                repair && destructive
                    ? "unreferenced PUBLISHED slab reclaimed with explicit "
                      "offline/quiesced proof"
                    : destructive
                          ? "unreferenced PUBLISHED slab (read-only; not reclaimed)"
                          : "unreferenced PUBLISHED slab deferred: complete "
                            "snapshot is not a publication fence"));
            continue;
        }

        if (state == static_cast<uint32_t>(ObjectState::kRetired) &&
            options_.reclaim_retired) {
            if (repair) {
                if (!ownership_.IsOwner()) {
                    return Status::Error(
                        StatusCode::kUnavailable,
                        "recovery fence lost before retired repair");
                }
                MINO_RETURN_IF_ERROR(
                    allocator_.ClearSlotForRecovery(slot, state));
                if (!ownership_.IsOwner()) {
                    return Status::Error(
                        StatusCode::kUnavailable,
                        "recovery fence lost after retired repair");
                }
                ++report.reclaimed_slab_count;
            }
            report.AddDetail(Finding(
                "retired_slab", expected_class, slot,
                repair ? "RETIRED reclaimed under exclusive Region recovery"
                       : "RETIRED (not reclaimed)"));
        }
    }
    return Status::Ok();
}

bool RecoveryScanner::CanReclaimPublishedOrphan() const noexcept {
    return options_.offline_or_quiesced &&
           ownership_.ProvesDestructiveReclaim(
               options_.destructive_reclaim_proof_token);
}

bool RecoveryScanner::CanReclaimUnpublished(
    uint64_t owner_epoch, uint64_t transaction_id) const noexcept {
    return ownership_.ProvesUnpublishedOwnerDead(owner_epoch, transaction_id);
}

bool RecoveryScanner::IsReferenced(uint32_t unit_index,
                                   uint32_t generation) const {
    for (const RecoveryObjectReference& reference : references_) {
        if (reference.resource_id == resource_id_ &&
            reference.unit_index == unit_index &&
            reference.generation == generation) {
            return true;
        }
    }
    return false;
}

Status RecoveryScanner::ScanLargePool(RecoveryReport& report, bool repair) {
    if (!large_pool_.has_value()) {
        return Status::Error(StatusCode::kInternal,
                             "large pool scanner is not initialized");
    }
    if (repair && !ownership_.IsOwner()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "repairing large pool scan requires ownership");
    }
    LargeObjectPool& pool = *large_pool_;
    for (uint32_t segment = 0; segment < pool.segment_count(); ++segment) {
        if (repair && (segment % 256u) == 0) {
            ownership_.Heartbeat();
            if (!ownership_.IsOwner()) {
                return Status::Error(StatusCode::kUnavailable,
                                     "ownership lost during large pool scan");
            }
        }
        ++report.slots_scanned;
        SlabHeader header{};
        if (!pool.ReadSegmentForRecovery(segment, &header)) {
            return Status::Error(StatusCode::kInternal,
                                 "large pool segment disappeared");
        }
        const bool occupied = pool.IsSegmentOccupiedForRecovery(segment);
        const uint32_t state =
            header.object_state.load(std::memory_order_acquire);
        if (!occupied) {
            if (state != static_cast<uint32_t>(ObjectState::kFree)) {
                ++report.bitmap_inconsistency_count;
                report.AddDetail(Finding(
                    "large_bitmap_inconsistency", 0xFFFFu, segment,
                    "bitmap free but object_state=" +
                        std::string(ObjectStateName(state))));
                if (repair) {
                    MINO_RETURN_IF_ERROR(
                        pool.ClearStaleStateForRecovery(segment, state));
                }
            }
            continue;
        }
        if (!IsValidPublishedState(state)) {
            if (!IsProtocolReclaimableState(state)) {
                ++report.corrupted_slab_count;
                report.AddDetail(Finding(
                    "large_corruption", 0xFFFFu, segment,
                    "unknown object_state=" + std::to_string(state)));
                continue;
            }
            ++report.orphan_slab_count;
            if (repair) {
                const bool complete_segment_zero =
                    VerifyImmutableHeader(header) &&
                    header.class_id == 0xFFFFu &&
                    header.allocation_role.load(std::memory_order_acquire) == 0;
                if (complete_segment_zero) {
                    MINO_RETURN_IF_ERROR(
                        pool.ClearObjectForRecovery(segment, state));
                } else {
                    MINO_RETURN_IF_ERROR(
                        pool.ClearSegmentForRecovery(segment, state));
                }
                ++report.reclaimed_slab_count;
            }
            report.AddDetail(Finding(
                "large_orphan", 0xFFFFu, segment,
                repair ? "incomplete protocol segment reclaimed"
                       : "incomplete protocol segment (not reclaimed)"));
            continue;
        }
        const uint32_t generation =
            pool.AuthoritativeGenerationForRecovery(segment);
        if (!VerifyImmutableHeader(header) ||
            header.generation.load(std::memory_order_acquire) != generation ||
            header.class_id != 0xFFFFu ||
            header.capacity != pool.segment_size() ||
            header.object_size == 0 ||
            header.object_size > pool.max_object_size()) {
            ++report.corrupted_slab_count;
            report.AddDetail(Finding(
                "large_corruption", 0xFFFFu, segment,
                "segment header/generation invariant failed"));
            continue;
        }
        const uint32_t role =
            header.allocation_role.load(std::memory_order_acquire);
        if (role > segment) {
            ++report.corrupted_slab_count;
            report.AddDetail(Finding("large_corruption", 0xFFFFu, segment,
                                     "continuation role underflows pool"));
            continue;
        }
        const uint32_t first = segment - role;
        auto handle = pool.HandleForRecovery(first);
        if (!handle.ok()) {
            ++report.corrupted_slab_count;
            report.AddDetail(Finding("large_corruption", 0xFFFFu, segment,
                                     "continuation has no segment 0"));
            continue;
        }
        auto plan = pool.InspectPlan(*handle);
        if (!plan.ok()) {
            ++report.corrupted_slab_count;
            report.AddDetail(Finding("large_corruption", 0xFFFFu, segment,
                                     plan.status().ToString()));
            continue;
        }
        if (role != 0) {
            continue;
        }
        if (state == static_cast<uint32_t>(ObjectState::kAllocated) ||
            state == static_cast<uint32_t>(ObjectState::kBuilding) ||
            state == static_cast<uint32_t>(ObjectState::kAborting)) {
            ++report.orphan_slab_count;
            ++report.unpublished_orphan_candidate_count;
            const uint64_t owner_epoch =
                header.owner_epoch.load(std::memory_order_acquire);
            const uint64_t transaction_id =
                header.allocation_transaction_id.load(std::memory_order_acquire);
            const bool proven_dead =
                CanReclaimUnpublished(owner_epoch, transaction_id);
            if (!proven_dead) {
                ++report.deferred_reclaim_count;
            }
            if (repair && proven_dead) {
                MINO_RETURN_IF_ERROR(
                    pool.ClearObjectForRecovery(first, state));
                ++report.reclaimed_slab_count;
            }
            report.AddDetail(Finding(
                "large_unpublished_orphan_candidate", 0xFFFFu, first,
                "object_state=" + std::string(ObjectStateName(state)) +
                    " owner_epoch=" + std::to_string(owner_epoch) +
                    " transaction_id=" + std::to_string(transaction_id) +
                    (repair && proven_dead
                         ? " reclaimed with owner/transaction death proof"
                         : proven_dead ? " (read-only; not reclaimed)"
                                       : " deferred: no Journal/owner-death proof")));
            continue;
        }
        if (state == static_cast<uint32_t>(ObjectState::kPublished) &&
            references_complete_ && !IsReferenced(first, generation)) {
            ++report.orphan_slab_count;
            ++report.published_orphan_candidate_count;
            const bool destructive = CanReclaimPublishedOrphan();
            if (!destructive) {
                ++report.deferred_reclaim_count;
            }
            if (repair && destructive) {
                MINO_RETURN_IF_ERROR(
                    pool.ClearObjectForRecovery(first, state));
                ++report.reclaimed_slab_count;
            }
            report.AddDetail(Finding(
                "large_published_orphan_candidate", 0xFFFFu, first,
                repair && destructive
                    ? "unreferenced PUBLISHED large object reclaimed with "
                      "explicit offline/quiesced proof"
                    : destructive
                          ? "unreferenced PUBLISHED large object (read-only; not reclaimed)"
                          : "unreferenced PUBLISHED large object deferred: complete "
                            "snapshot is not a publication fence"));
            continue;
        }
        if (state == static_cast<uint32_t>(ObjectState::kRetired) &&
            options_.reclaim_retired) {
            if (repair) {
                MINO_RETURN_IF_ERROR(
                    pool.ClearObjectForRecovery(first, state));
                ++report.reclaimed_slab_count;
            }
            report.AddDetail(Finding(
                "large_retired", 0xFFFFu, first,
                repair ? "RETIRED large object reclaimed"
                       : "RETIRED large object (not reclaimed)"));
        }
    }
    return Status::Ok();
}

Status RecoveryScanner::ScanLegacyImage(RecoveryReport& report, bool repair) {
    const auto* classes = reinterpret_cast<const ClassDescriptor*>(
        legacy_base_ + legacy_layout_.class_table_offset);
    for (uint32_t c = 0; c < legacy_layout_.class_count; ++c) {
        const ClassDescriptor& cls = classes[c];
        auto* bitmap = reinterpret_cast<BitmapWord*>(
            legacy_base_ + cls.bitmap_offset);
        if (repair) {
            ownership_.Heartbeat();
            if (!ownership_.IsOwner()) {
                return Status::Error(StatusCode::kUnavailable,
                                     "offline recovery ownership was lost");
            }
        }
        for (uint32_t slot = 0; slot < cls.slot_count; ++slot) {
            ++report.slots_scanned;
            const uint64_t mask = uint64_t{1} << (slot % 64);
            const bool occupied =
                (bitmap[slot / 64].load(std::memory_order_acquire) & mask) != 0;
            auto* header = reinterpret_cast<SlabHeader*>(
                legacy_base_ + cls.slots_offset +
                static_cast<uint64_t>(slot) * cls.slot_stride);
            const uint32_t state =
                header->object_state.load(std::memory_order_acquire);
            if (!occupied) {
                if (state != static_cast<uint32_t>(ObjectState::kFree)) {
                    ++report.bitmap_inconsistency_count;
                    report.AddDetail(Finding(
                        "bitmap_inconsistency", cls.class_id, slot,
                        "bitmap free but object_state=" +
                            std::string(ObjectStateName(state)) +
                            (repair ? " (state cleared to FREE)"
                                    : " (not repaired)")));
                    if (repair) {
                        if (!ownership_.IsOwner()) {
                            return Status::Error(
                                StatusCode::kUnavailable,
                                "offline recovery fence lost before repair");
                        }
                        uint32_t expected_state = state;
                        (void)header->object_state.compare_exchange_strong(
                            expected_state,
                            static_cast<uint32_t>(ObjectState::kFree),
                            std::memory_order_acq_rel,
                            std::memory_order_acquire);
                        if (!ownership_.IsOwner()) {
                            return Status::Error(
                                StatusCode::kUnavailable,
                                "offline recovery fence lost after repair");
                        }
                    }
                }
                continue;
            }
            if (!IsValidPublishedState(state)) {
                if (!IsProtocolReclaimableState(state)) {
                    ++report.corrupted_slab_count;
                    report.AddDetail(Finding(
                        "corruption", cls.class_id, slot,
                        "unknown object_state=" + std::to_string(state) +
                            " (NOT auto-repaired; quarantine required)"));
                    continue;
                }
                ++report.orphan_slab_count;
                if (repair) {
                    if (!ownership_.IsOwner()) {
                        return Status::Error(
                            StatusCode::kUnavailable,
                            "offline recovery fence lost before orphan repair");
                    }
                    uint32_t expected_state = state;
                    if (header->object_state.compare_exchange_strong(
                            expected_state,
                            static_cast<uint32_t>(ObjectState::kFree),
                            std::memory_order_acq_rel,
                            std::memory_order_acquire)) {
                        bitmap[slot / 64].fetch_and(~mask,
                                                    std::memory_order_acq_rel);
                        ++report.reclaimed_slab_count;
                    }
                    if (!ownership_.IsOwner()) {
                        return Status::Error(
                            StatusCode::kUnavailable,
                            "offline recovery fence lost after orphan repair");
                    }
                }
                report.AddDetail(Finding(
                    "orphan_slab", cls.class_id, slot,
                    "object_state=" + std::string(ObjectStateName(state)) +
                        (repair ? " reclaimed" : " (not reclaimed)")));
                continue;
            }
            if (!VerifyImmutableHeader(*header) ||
                header->class_id != cls.class_id ||
                header->object_size > header->capacity) {
                ++report.corrupted_slab_count;
                report.AddDetail(Finding(
                    "corruption", cls.class_id, slot,
                    "allocator SlabHeader invariant failed "
                    "(NOT auto-repaired; quarantine required)"));
                continue;
            }
            if (state == static_cast<uint32_t>(ObjectState::kAllocated) ||
                state == static_cast<uint32_t>(ObjectState::kBuilding) ||
                state == static_cast<uint32_t>(ObjectState::kAborting)) {
                ++report.orphan_slab_count;
                ++report.unpublished_orphan_candidate_count;
                const uint64_t owner_epoch =
                    header->owner_epoch.load(std::memory_order_acquire);
                const uint64_t transaction_id =
                    header->allocation_transaction_id.load(
                        std::memory_order_acquire);
                const bool proven_dead =
                    CanReclaimUnpublished(owner_epoch, transaction_id);
                if (!proven_dead) {
                    ++report.deferred_reclaim_count;
                }
                if (repair && proven_dead) {
                    if (!ownership_.IsOwner()) {
                        return Status::Error(
                            StatusCode::kUnavailable,
                            "offline recovery fence lost before unpublished repair");
                    }
                    uint32_t expected_state = state;
                    if (header->object_state.compare_exchange_strong(
                            expected_state,
                            static_cast<uint32_t>(ObjectState::kFree),
                            std::memory_order_acq_rel,
                            std::memory_order_acquire)) {
                        bitmap[slot / 64].fetch_and(~mask,
                                                    std::memory_order_acq_rel);
                        ++report.reclaimed_slab_count;
                    }
                    if (!ownership_.IsOwner()) {
                        return Status::Error(
                            StatusCode::kUnavailable,
                            "offline recovery fence lost after unpublished repair");
                    }
                }
                report.AddDetail(Finding(
                    "unpublished_orphan_candidate", cls.class_id, slot,
                    "object_state=" + std::string(ObjectStateName(state)) +
                        (repair && proven_dead
                             ? " reclaimed with owner/transaction death proof"
                             : proven_dead
                                   ? " (read-only; not reclaimed)"
                                   : " deferred: no Journal/owner-death proof")));
                continue;
            }
            if (state == static_cast<uint32_t>(ObjectState::kRetired) &&
                options_.reclaim_retired) {
                if (repair) {
                    if (!ownership_.IsOwner()) {
                        return Status::Error(
                            StatusCode::kUnavailable,
                            "offline recovery fence lost before retired repair");
                    }
                    uint32_t expected_state = state;
                    if (header->object_state.compare_exchange_strong(
                            expected_state,
                            static_cast<uint32_t>(ObjectState::kFree),
                            std::memory_order_acq_rel,
                            std::memory_order_acquire)) {
                        bitmap[slot / 64].fetch_and(~mask,
                                                    std::memory_order_acq_rel);
                        ++report.reclaimed_slab_count;
                    }
                    if (!ownership_.IsOwner()) {
                        return Status::Error(
                            StatusCode::kUnavailable,
                            "offline recovery fence lost after retired repair");
                    }
                }
                report.AddDetail(Finding(
                    "retired_slab", cls.class_id, slot,
                    repair ? "RETIRED reclaimed by offline recovery"
                           : "RETIRED (not reclaimed)"));
            }
        }
    }
    return Status::Ok();
}

RecoveryOwner RecoveryScanner::Owner() noexcept {
    return legacy_context_ == nullptr
               ? RecoveryOwner(legacy_owner_state_, SelfPid())
               : RecoveryOwner(legacy_context_->state, legacy_context_->pid,
                               &legacy_context_->lease_token);
}

RecoveryOwner RecoveryScanner::Owner() const noexcept {
    return legacy_context_ == nullptr
               ? RecoveryOwner(legacy_owner_state_, SelfPid())
               : RecoveryOwner(legacy_context_->state, legacy_context_->pid,
                               &legacy_context_->lease_token);
}

uint64_t RecoveryScanner::SelfPid() {
    return static_cast<uint64_t>(::getpid());
}

bool RecoveryScanner::LegacyIsOwner(const void* context) noexcept {
    auto* legacy = const_cast<LegacyOwnershipContext*>(
        static_cast<const LegacyOwnershipContext*>(context));
    return RecoveryOwner(legacy->state, legacy->pid, &legacy->lease_token)
        .IsOwner();
}

void RecoveryScanner::LegacyHeartbeat(void* context) noexcept {
    auto* legacy = static_cast<LegacyOwnershipContext*>(context);
    RecoveryOwner(legacy->state, legacy->pid, &legacy->lease_token)
        .Heartbeat();
}

}  // namespace mino::shm::recovery
