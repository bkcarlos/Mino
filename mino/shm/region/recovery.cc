// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.gnu.org/licenses/lgpl-3.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include "mino/shm/region/recovery.h"

#include <atomic>
#include <limits>
#include <thread>
#include <utility>

#if defined(__unix__) || defined(__APPLE__)
#include <time.h>
#endif

#include "mino/common/checked_arithmetic.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/recovery/scanner.h"
#include "mino/shm/region/region.h"

namespace mino {
namespace {

// Monotonic clock in nanoseconds. The lease uses a monotonic clock so it is
// unaffected by wall-clock adjustments and is comparable across processes on
// the same host (same boot epoch).
uint64_t MonotonicNowNs() {
#if defined(__unix__) || defined(__APPLE__)
    struct timespec ts;
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
           static_cast<uint64_t>(ts.tv_nsec);
#else
    return 0;
#endif
}

Status DeadlineFromMilliseconds(uint64_t now_ns, uint64_t milliseconds,
                                bool require_nonzero, uint64_t* deadline_ns) {
    if (require_nonzero && milliseconds == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "lease duration must be non-zero");
    }
    uint64_t duration_ns = 0;
    if (!CheckedMulU64(milliseconds, 1'000'000, &duration_ns) ||
        !CheckedAddU64(now_ns, duration_ns, deadline_ns)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "lease/wait deadline overflows");
    }
    return Status::Ok();
}

bool CompareExchangeState(SuperBlock& sb, RegionState expected,
                          RegionState desired) {
    uint32_t raw_expected = static_cast<uint32_t>(expected);
    return std::atomic_ref(sb.state).compare_exchange_strong(
        raw_expected, static_cast<uint32_t>(desired),
        std::memory_order_acq_rel, std::memory_order_acquire);
}

void PublishRegionEpochAtLeast(SuperBlock& sb, uint64_t epoch) {
    auto region_epoch = std::atomic_ref(sb.region_epoch);
    uint64_t observed = region_epoch.load(std::memory_order_acquire);
    while (observed < epoch &&
           !region_epoch.compare_exchange_weak(
               observed, epoch, std::memory_order_acq_rel,
               std::memory_order_acquire)) {
    }
}

Status ReplayCommittedFence(SuperBlock& sb, uint64_t fence) {
    const RecoveryFencePhase phase = RecoveryFencePhaseOf(fence);
    if (phase == RecoveryFencePhase::kActive) {
        PublishRegionEpochAtLeast(sb, RecoveryFenceEpoch(fence));
        StoreCleanShutdown(sb, false);
        if (LoadRegionState(sb) == RegionState::kRecovering) {
            (void)CompareExchangeState(sb, RegionState::kRecovering,
                                       RegionState::kActive);
        }
        return LoadRegionState(sb) == RegionState::kActive
                   ? Status::Ok()
                   : Status::Error(StatusCode::kCorruption,
                                   "committed ACTIVE fence conflicts with state");
    }
    if (phase == RecoveryFencePhase::kQuarantined) {
        if (LoadRegionState(sb) == RegionState::kRecovering) {
            (void)CompareExchangeState(sb, RegionState::kRecovering,
                                       RegionState::kQuarantined);
        }
        return Status::Error(StatusCode::kUnavailable,
                             "region recovery committed quarantine");
    }
    return Status::Error(StatusCode::kInvalidArgument,
                         "recovery fence is not committed");
}

bool RegionRecoveryOwnerIsCurrent(const void* context) noexcept {
    return static_cast<const RecoveryOwner*>(context)->HoldsRecoveryFence();
}

void RenewRegionRecoveryLease(void* context) noexcept {
    (void)static_cast<RecoveryOwner*>(context)->RenewLease();
}

}  // namespace

RecoveryOwner::RecoveryOwner(RecoveryOwner&& other) noexcept
    : sb_(other.sb_),
      owner_(other.owner_),
      lease_value_set_(other.lease_value_set_),
      epoch_at_acquire_(other.epoch_at_acquire_),
      is_owner_(other.is_owner_),
      lease_duration_ms_(other.lease_duration_ms_) {
    other.ResetMovedFrom();
}

RecoveryOwner& RecoveryOwner::operator=(RecoveryOwner&& other) noexcept {
    if (this != &other) {
        (void)Release();
        sb_ = other.sb_;
        owner_ = other.owner_;
        lease_value_set_ = other.lease_value_set_;
        epoch_at_acquire_ = other.epoch_at_acquire_;
        is_owner_ = other.is_owner_;
        lease_duration_ms_ = other.lease_duration_ms_;
        other.ResetMovedFrom();
    }
    return *this;
}

void RecoveryOwner::ResetMovedFrom() noexcept {
    sb_ = nullptr;
    owner_ = ProcessIdentity{};
    lease_value_set_ = 0;
    epoch_at_acquire_ = 0;
    is_owner_ = false;
    lease_duration_ms_ = kDefaultRecoveryLeaseMs;
}

Result<RecoveryOwner> RecoveryOwner::TryAcquire(SharedMemoryRegion& region,
                                                const ProcessIdentity& owner) {
    return TryAcquire(region, owner, kDefaultRecoveryLeaseMs);
}

Result<RecoveryOwner> RecoveryOwner::TryAcquire(SharedMemoryRegion& region,
                                                const ProcessIdentity& owner,
                                                uint64_t lease_duration_ms) {
    SuperBlock* sb = region.superblock();
    if (sb == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "region has no superblock");
    }

    auto& lease = sb->recovery_lease_ns;
    const uint64_t now = MonotonicNowNs();
    uint64_t new_lease = 0;
    MINO_RETURN_IF_ERROR(DeadlineFromMilliseconds(
        now, lease_duration_ms, /*require_nonzero=*/true, &new_lease));

    // The single 64-bit CAS that provides mutual exclusion: transition the
    // lease from an expired value to our fresh future value. Only one racing
    // process can win this CAS (6.5 step 2).
    uint64_t expected = std::atomic_ref(lease).load(std::memory_order_acquire);
    for (;;) {
        if (expected > now) {
            // A live owner holds the lease.
            return Status::Error(StatusCode::kWouldBlock,
                                 "recovery ownership already held");
        }
        if (std::atomic_ref(lease).compare_exchange_strong(
                expected, new_lease, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            break;  // We won the lease.
        }
        // compare_exchange reloaded `expected` with the current value; retry.
    }

    // We hold the lease. Record the owner identity and bump the takeover
    // generation (6.5 step 5). The owner identity is informational; the lease
    // CAS above is the actual lock.
    sb->recovery_owner = owner;
    auto recovery_epoch = std::atomic_ref(sb->recovery_epoch);
    uint64_t previous_epoch = recovery_epoch.load(std::memory_order_acquire);
    if (previous_epoch >= kMaxRecoveryFenceEpoch) {
        uint64_t owned_token = new_lease;
        (void)std::atomic_ref(lease).compare_exchange_strong(
            owned_token, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
        return Status::Error(StatusCode::kResourceExhausted,
                             "recovery fencing epoch exhausted");
    }
    if (!recovery_epoch.compare_exchange_strong(
            previous_epoch, previous_epoch + 1, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        uint64_t owned_token = new_lease;
        (void)std::atomic_ref(lease).compare_exchange_strong(
            owned_token, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
        return Status::Error(StatusCode::kCorruption,
                             "recovery epoch changed while lease was exclusive");
    }
    const uint64_t epoch = previous_epoch + 1;

    RecoveryOwner ro;
    ro.sb_ = sb;
    ro.owner_ = owner;
    ro.lease_value_set_ = new_lease;
    ro.epoch_at_acquire_ = epoch;
    ro.is_owner_ = true;
    ro.lease_duration_ms_ = lease_duration_ms;
    return ro;
}

Status RecoveryOwner::RenewLease() { return RenewLease(lease_duration_ms_); }

Status RecoveryOwner::RenewLease(uint64_t lease_duration_ms) {
    if (!is_owner_ || sb_ == nullptr) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "not the recovery owner");
    }
    auto& lease = sb_->recovery_lease_ns;
    const uint64_t now = MonotonicNowNs();
    uint64_t new_lease = 0;
    MINO_RETURN_IF_ERROR(DeadlineFromMilliseconds(
        now, lease_duration_ms, /*require_nonzero=*/true, &new_lease));

    // Renew only if the lease still holds exactly the value we set. If another
    // process took over after our lease expired, the value differs and our CAS
    // fails, so we detect the loss of ownership (6.5 step 3/5).
    uint64_t expected = lease_value_set_;
    if (!std::atomic_ref(lease).compare_exchange_strong(
            expected, new_lease, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        is_owner_ = false;
        return Status::Error(StatusCode::kUnavailable,
                             "recovery ownership lost");
    }
    lease_value_set_ = new_lease;
    return Status::Ok();
}

Status RecoveryOwner::Release() {
    if (!is_owner_ || sb_ == nullptr) {
        ResetMovedFrom();
        return Status::Ok();
    }
    SuperBlock* sb = sb_;
    uint64_t expected = lease_value_set_;
    const bool released = std::atomic_ref(sb->recovery_lease_ns)
                              .compare_exchange_strong(
                                  expected, 0, std::memory_order_acq_rel,
                                  std::memory_order_acquire);
    ResetMovedFrom();
    if (!released) {
        return Status::Error(StatusCode::kUnavailable,
                             "recovery ownership changed before release");
    }
    // recovery_owner is informational and deliberately left untouched here:
    // clearing it after publishing lease=0 could race and erase a new owner's
    // identity. The next successful acquire overwrites it under its lease.
    return Status::Ok();
}

bool RecoveryOwner::IsOwner() const {
    if (!is_owner_ || sb_ == nullptr) {
        return false;
    }
    // We own the lease iff it still holds the value we set and it has not
    // expired.
    const uint64_t current = LoadRecoveryLeaseNs(*sb_);
    return current == lease_value_set_ && current > MonotonicNowNs() &&
           LoadRecoveryEpoch(*sb_) == epoch_at_acquire_;
}

bool RecoveryOwner::HoldsRecoveryFence() const {
    return IsOwner() &&
           LoadRecoveryFence(*sb_) == EncodeRecoveryFence(
               epoch_at_acquire_, RecoveryFencePhase::kRecovering);
}

Status RecoveryOwner::ClaimRecoveryFence(uint64_t expected_fence) {
    if (!IsOwner()) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot claim recovery fence without lease/epoch");
    }
    const uint64_t desired = EncodeRecoveryFence(
        epoch_at_acquire_, RecoveryFencePhase::kRecovering);
    if (!CompareExchangeRecoveryFence(*sb_, &expected_fence, desired)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "recovery fence changed before takeover claim");
    }
    return HoldsRecoveryFence()
               ? Status::Ok()
               : Status::Error(StatusCode::kUnavailable,
                               "recovery fence ownership was immediately lost");
}

Status RecoveryOwner::CommitRecoveryFence(RecoveryFencePhase phase) {
    if (sb_ == nullptr || epoch_at_acquire_ == 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "recovery owner has no commit fence");
    }
    if (phase != RecoveryFencePhase::kActive &&
        phase != RecoveryFencePhase::kQuarantined) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid recovery fence commit phase");
    }
    uint64_t expected = EncodeRecoveryFence(
        epoch_at_acquire_, RecoveryFencePhase::kRecovering);
    const uint64_t desired = EncodeRecoveryFence(epoch_at_acquire_, phase);
    if (!CompareExchangeRecoveryFence(*sb_, &expected, desired)) {
        return Status::Error(StatusCode::kUnavailable,
                             "stale recovery owner cannot commit fence");
    }
    return Status::Ok();
}

Status RecoverRegionForAttach(SharedMemoryRegion& region,
                              const ProcessIdentity& self,
                              uint64_t wait_timeout_ms) {
    SuperBlock* sb = region.superblock();
    if (sb == nullptr) {
        return Status::Error(StatusCode::kCorruption,
                             "region has no superblock");
    }

    const uint64_t start_ns = MonotonicNowNs();
    uint64_t absolute_deadline_ns = 0;
    MINO_RETURN_IF_ERROR(DeadlineFromMilliseconds(
        start_ns, wait_timeout_ms, /*require_nonzero=*/false,
        &absolute_deadline_ns));

    auto wait_or_timeout = [&]() -> Status {
        if (MonotonicNowNs() >= absolute_deadline_ns) {
            return Status::Error(StatusCode::kTimeout,
                                 "timed out waiting for region recovery");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return Status::Ok();
    };

    auto recover_as_owner = [&](RecoveryOwner owner) -> Status {
        auto fail_owned = [&](Status status) {
            Status committed = owner.CommitRecoveryFence(
                RecoveryFencePhase::kQuarantined);
            if (!committed.ok()) {
                (void)owner.Release();
                return Status::Error(
                    StatusCode::kUnavailable,
                    "stale recovery owner could not commit quarantine");
            }
            if (LoadRegionState(*sb) == RegionState::kRecovering) {
                (void)CompareExchangeState(*sb, RegionState::kRecovering,
                                           RegionState::kQuarantined);
            }
            (void)owner.Release();
            return status;
        };

        const uint64_t allocator_available =
            region.size() - sb->allocator_offset;
        void* allocator_base = region.base() + sb->allocator_offset;
        auto metadata_present = CentralSlabAllocator::HasAllocatorMetadata(
            allocator_base, allocator_available);
        if (!metadata_present.ok()) {
            return fail_owned(metadata_present.status());
        }
        if (*metadata_present) {
            RegionAllocatorStorage storage{
                .region_base = region.base(),
                .region_size = region.size(),
                .allocator_offset = sb->allocator_offset,
                .allocator_size = sb->data_offset - sb->allocator_offset,
                .data_offset = sb->data_offset,
                .data_size = sb->data_size,
                .region_id = sb->region_id,
            };
            auto allocator = CentralSlabAllocator::AttachInRegion(storage);
            if (!allocator.ok()) {
                return fail_owned(allocator.status());
            }
            shm::recovery::RecoveryOwnership ownership{
                .context = &owner,
                .is_owner = &RegionRecoveryOwnerIsCurrent,
                .heartbeat = &RenewRegionRecoveryLease,
            };
            auto scanner = shm::recovery::RecoveryScanner::Create(
                std::move(*allocator), ownership);
            if (!scanner.ok()) {
                return fail_owned(scanner.status());
            }
            auto report = scanner->Scan();
            if (!report.ok()) {
                return fail_owned(report.status());
            }
            if (report->corrupted_slab_count != 0) {
                return fail_owned(Status::Error(
                    StatusCode::kCorruption,
                    "allocator recovery did not establish consistency"));
            }
        }

        // Lease renewal is liveness only. The no-window commit is the exact
        // {epoch, RECOVERING}->{epoch, ACTIVE} CAS below; a takeover and a stale
        // commit race on this same atomic word.
        (void)owner.RenewLease();
        Status committed = owner.CommitRecoveryFence(
            RecoveryFencePhase::kActive);
        if (!committed.ok()) {
            (void)owner.Release();
            return Status::Error(StatusCode::kUnavailable,
                                 "stale recovery owner cannot commit ACTIVE");
        }
        PublishRegionEpochAtLeast(*sb, owner.epoch());
        StoreCleanShutdown(*sb, false);
        if (LoadRegionState(*sb) == RegionState::kRecovering) {
            (void)CompareExchangeState(*sb, RegionState::kRecovering,
                                       RegionState::kActive);
        }
        Status mirrored = ReplayCommittedFence(
            *sb, EncodeRecoveryFence(owner.epoch(),
                                     RecoveryFencePhase::kActive));
        Status released = owner.Release();
        if (!mirrored.ok()) {
            return mirrored;
        }
        return released;
    };

    for (;;) {
        const RegionState first_state = LoadRegionState(*sb);
        const bool clean = LoadCleanShutdown(*sb);
        const RegionState state = LoadRegionState(*sb);
        if (state != first_state) {
            continue;
        }

        if (state == RegionState::kQuarantined) {
            return Status::Error(StatusCode::kUnavailable,
                                 "region is quarantined");
        }
        if (state == RegionState::kClosed && clean) {
            if (!CompareExchangeState(*sb, RegionState::kClosed,
                                      RegionState::kActive)) {
                continue;
            }
            StoreCleanShutdown(*sb, false);
            return Status::Ok();
        }
        if (state == RegionState::kActive) {
            // No business-owner lease exists in this SuperBlock version. An
            // ACTIVE/in-use Region may still have a live process, so destructive
            // crash recovery cannot be justified from clean_shutdown alone.
            return Status::Error(
                StatusCode::kWouldBlock,
                "ACTIVE Region may have live service; explicit DIRTY fencing required");
        }
        if (state == RegionState::kInitializing ||
            (state == RegionState::kClosed && !clean)) {
            if (!CompareExchangeState(*sb, state, RegionState::kDirty)) {
                continue;
            }
            continue;
        }
        if (state == RegionState::kDirty) {
            const uint64_t observed_fence = LoadRecoveryFence(*sb);
            if (RecoveryFencePhaseOf(observed_fence) ==
                RecoveryFencePhase::kQuarantined) {
                if (!CompareExchangeState(*sb, RegionState::kDirty,
                                          RegionState::kQuarantined)) {
                    continue;
                }
                return Status::Error(StatusCode::kUnavailable,
                                     "region recovery fence is quarantined");
            }
            auto acquired = RecoveryOwner::TryAcquire(region, self);
            if (!acquired.ok()) {
                if (acquired.status().code() != StatusCode::kWouldBlock) {
                    return acquired.status();
                }
                MINO_RETURN_IF_ERROR(wait_or_timeout());
                continue;
            }
            RecoveryOwner owner = std::move(*acquired);
            Status claimed = owner.ClaimRecoveryFence(observed_fence);
            if (!claimed.ok()) {
                (void)owner.Release();
                continue;
            }
            if (!CompareExchangeState(*sb, RegionState::kDirty,
                                      RegionState::kRecovering)) {
                uint64_t owned_fence = EncodeRecoveryFence(
                    owner.epoch(), RecoveryFencePhase::kRecovering);
                (void)CompareExchangeRecoveryFence(
                    *sb, &owned_fence, observed_fence);
                (void)owner.Release();
                continue;
            }
            return recover_as_owner(std::move(owner));
        }
        if (state == RegionState::kRecovering) {
            const uint64_t observed_fence = LoadRecoveryFence(*sb);
            const RecoveryFencePhase fence_phase =
                RecoveryFencePhaseOf(observed_fence);
            if (fence_phase == RecoveryFencePhase::kActive ||
                fence_phase == RecoveryFencePhase::kQuarantined) {
                return ReplayCommittedFence(*sb, observed_fence);
            }
            if (fence_phase != RecoveryFencePhase::kRecovering) {
                return Status::Error(StatusCode::kCorruption,
                                     "RECOVERING Region has invalid fence phase");
            }
            const uint64_t now = MonotonicNowNs();
            if (LoadRecoveryLeaseNs(*sb) > now) {
                MINO_RETURN_IF_ERROR(wait_or_timeout());
                continue;
            }
            auto takeover = RecoveryOwner::TryAcquire(region, self);
            if (!takeover.ok()) {
                if (takeover.status().code() != StatusCode::kWouldBlock) {
                    return takeover.status();
                }
                MINO_RETURN_IF_ERROR(wait_or_timeout());
                continue;
            }
            RecoveryOwner owner = std::move(*takeover);
            if (LoadRegionState(*sb) != RegionState::kRecovering) {
                (void)owner.Release();
                continue;
            }
            Status claimed = owner.ClaimRecoveryFence(observed_fence);
            if (!claimed.ok()) {
                (void)owner.Release();
                continue;
            }
            return recover_as_owner(std::move(owner));
        }
        return Status::Error(StatusCode::kCorruption,
                             "invalid Region lifecycle state");
    }
}

}  // namespace mino
