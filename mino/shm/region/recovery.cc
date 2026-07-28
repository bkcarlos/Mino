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
#include <thread>

#if defined(__unix__) || defined(__APPLE__)
#include <time.h>
#endif

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

uint64_t MsToNs(uint64_t ms) { return ms * 1000000ull; }

}  // namespace

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
    const uint64_t new_lease = now + MsToNs(lease_duration_ms);

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
    const uint64_t epoch = std::atomic_ref(sb->recovery_epoch)
                               .fetch_add(1, std::memory_order_acq_rel) +
                           1;

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
    const uint64_t new_lease = now + MsToNs(lease_duration_ms);

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
        is_owner_ = false;
        return Status::Ok();
    }
    // Clear the owner identity and expire the lease so another process may
    // acquire. Bump the epoch to record the transition.
    sb_->recovery_owner = ProcessIdentity{};
    std::atomic_ref(sb_->recovery_lease_ns)
        .store(0, std::memory_order_release);
    std::atomic_ref(sb_->recovery_epoch)
        .fetch_add(1, std::memory_order_acq_rel);
    is_owner_ = false;
    return Status::Ok();
}

bool RecoveryOwner::IsOwner() const {
    if (!is_owner_ || sb_ == nullptr) {
        return false;
    }
    // We own the lease iff it still holds the value we set and it has not
    // expired.
    const uint64_t current =
        LoadRecoveryLeaseNs(*sb_);
    return current == lease_value_set_ && current > MonotonicNowNs();
}

Status RecoverRegionForAttach(SharedMemoryRegion& region,
                              const ProcessIdentity& self,
                              uint64_t wait_timeout_ms) {
    SuperBlock* sb = region.superblock();
    if (sb == nullptr) {
        return Status::Error(StatusCode::kCorruption,
                             "region has no superblock");
    }

    const bool clean = LoadCleanShutdown(*sb);
    const RegionState state = LoadRegionState(*sb);

    // Fast path: previously clean and closed/inactive. Mark it in-use and
    // ACTIVE without recovery (6.3 step 10).
    if (clean && (state == RegionState::kClosed ||
                  state == RegionState::kActive)) {
        StoreCleanShutdown(*sb, false);
        StoreState(*sb, RegionState::kActive);
        return Status::Ok();
    }

    // If another process is actively recovering, wait for it to finish
    // (6.5 step 4), then re-evaluate.
    if (state == RegionState::kRecovering) {
        const uint64_t deadline_ns =
            MonotonicNowNs() + MsToNs(wait_timeout_ms);
        while (LoadRegionState(*sb) == RegionState::kRecovering) {
            if (MonotonicNowNs() >= deadline_ns) {
                return Status::Error(StatusCode::kTimeout,
                                     "timed out waiting for region recovery");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        // Recovery finished (either ACTIVE or QUARANTINED). Re-dispatch.
        return RecoverRegionForAttach(region, self, wait_timeout_ms);
    }

    if (state == RegionState::kQuarantined) {
        return Status::Error(StatusCode::kUnavailable,
                             "region is quarantined");
    }

    // Dirty path: clean_shutdown == false while the state indicates the Region
    // was in use (ACTIVE/INITIALIZING) — the previous owner crashed (6.1).
    // Transition to DIRTY, then acquire ownership and recover (6.5).
    StoreState(*sb, RegionState::kDirty);

    // Acquire single-writer Recovery Ownership (6.5 step 2).
    MINO_ASSIGN_OR_RETURN(RecoveryOwner owner,
                          RecoveryOwner::TryAcquire(region, self));
    StoreState(*sb, RegionState::kRecovering);

    // --- Recovery body ---------------------------------------------------
    // The deep consistency scan (orphan slabs, residual ACKs, bitmap
    // reconciliation) is D1-09 (//mino/shm/recovery:scanner). Here we perform
    // the protocol-level recovery: establish a fresh epoch and mark the Region
    // usable again. If a future scanner finds the Region unrecoverable, it
    // transitions to QUARANTINED (6.5 step 7).
    // ----------------------------------------------------------------------

    // Publish the new Region Epoch FIRST, then switch to ACTIVE (6.5 step 6).
    // If we crash between these two stores, the next owner sees the epoch
    // already bumped but state still RECOVERING and re-runs the full flow,
    // bumping the epoch again.
    const uint64_t prev_epoch = LoadRegionEpoch(*sb);
    StoreRegionEpoch(*sb, prev_epoch + 1);

    // Mark in-use and ACTIVE.
    StoreCleanShutdown(*sb, false);
    StoreState(*sb, RegionState::kActive);

    // Release ownership; the lease is expired so others may recover later.
    MINO_RETURN_IF_ERROR(owner.Release());
    return Status::Ok();
}

}  // namespace mino
