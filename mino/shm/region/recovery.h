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

#ifndef MINO_SHM_REGION_RECOVERY_H_
#define MINO_SHM_REGION_RECOVERY_H_

#include <cstdint>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/region/superblock.h"

namespace mino {

class SharedMemoryRegion;

// Default recovery lease duration. The owner must RenewLease() more often than
// this; if the owner crashes, the lease expires and a new process may take
// over (design doc section 6.5 step 5).
inline constexpr uint64_t kDefaultRecoveryLeaseMs = 5000;

// RecoveryOwner implements the single-writer Recovery Ownership protocol
// (design doc section 6.5). At most one process may recover a given Region at
// a time.
//
// recovery_lease_ns is the liveness/admission token. recovery_epoch allocates
// a unique generation for each acquisition, while recovery_fence_word is the
// authoritative mutation/commit fence: takeover and final commit CAS the same
// {epoch, phase} word, so a replaced owner cannot publish ACTIVE/QUARANTINED.
// recovery_owner records ProcessIdentity for diagnostics only.
class RecoveryOwner {
public:
    RecoveryOwner(const RecoveryOwner&) = delete;
    RecoveryOwner& operator=(const RecoveryOwner&) = delete;
    RecoveryOwner(RecoveryOwner&& other) noexcept;
    RecoveryOwner& operator=(RecoveryOwner&& other) noexcept;

    // Attempts to acquire Recovery Ownership of `region` for `owner`.
    //
    // Returns kWouldBlock if another live owner currently holds the lease.
    // On success the returned RecoveryOwner holds the lease and must call
    // RenewLease() periodically and Release() when done.
    static Result<RecoveryOwner> TryAcquire(SharedMemoryRegion& region,
                                            const ProcessIdentity& owner);

    // Same as TryAcquire but with an explicit lease duration.
    static Result<RecoveryOwner> TryAcquire(SharedMemoryRegion& region,
                                            const ProcessIdentity& owner,
                                            uint64_t lease_duration_ms);

    // Renews the lease. Fails (kUnavailable) if ownership was lost — i.e. the
    // lease no longer holds the value this owner last set, which means another
    // process took over after our lease expired.
    Status RenewLease();

    // Same as RenewLease but with an explicit extension.
    Status RenewLease(uint64_t lease_duration_ms);

    // Releases ownership with a CAS from this object's exact lease token to
    // zero. A stale owner cannot clear a replacement owner's lease. The
    // informational recovery_owner field is not cleared because doing so after
    // lease publication could erase a newly acquired owner's identity.
    Status Release();

    // True while this object holds its exact lease token and acquisition epoch.
    bool IsOwner() const;

    // True only while the SuperBlock commit fence is exactly
    // {epoch_at_acquire, RECOVERING}. Scanner mutation and final commit use this
    // stronger predicate rather than lease ownership alone.
    bool HoldsRecoveryFence() const;
    uint64_t epoch() const noexcept { return epoch_at_acquire_; }

    // Linearizes takeover by replacing the exact observed fence with this
    // owner's {epoch, RECOVERING} word. Commit races and takeover races resolve
    // on the same atomic word.
    Status ClaimRecoveryFence(uint64_t expected_fence);

    // Linearizes final recovery commit. Only RECOVERING -> ACTIVE/QUARANTINED
    // for this exact epoch is accepted; a replacement owner's claim makes a
    // stale commit fail deterministically.
    Status CommitRecoveryFence(RecoveryFencePhase phase);

private:
    void ResetMovedFrom() noexcept;

    RecoveryOwner() = default;

    SuperBlock* sb_ = nullptr;  // not owned
    ProcessIdentity owner_;
    uint64_t lease_value_set_ = 0;   // last lease value we published
    uint64_t epoch_at_acquire_ = 0;  // recovery_epoch when we acquired
    bool is_owner_ = false;
    uint64_t lease_duration_ms_ = kDefaultRecoveryLeaseMs;
};

// Drives the dirty-Region recovery flow on behalf of SharedMemoryRegion::Attach
// (design doc sections 6.1 and 6.5):
//
//   1. If the Region is clean, mark it ACTIVE/in-use and return OK.
//   2. If explicitly dirty, acquire Recovery Ownership, run recovery, publish a new
//      Region Epoch, then transition to ACTIVE (in that order, per 6.5 step 6).
//   3. If another process is already recovering, wait up to
//      `wait_timeout_ms` for it to finish, then re-check.
//   4. If recovery cannot complete reliably, transition to QUARANTINED.
//
// If CentralSlabAllocator metadata is present, this function runs the real
// allocator-backed RecoveryScanner while holding this Region's SuperBlock
// RecoveryOwner. Corruption quarantines the Region before it can become ACTIVE.
// ACTIVE + clean_shutdown=false is not sufficient proof of a crash because the
// current SuperBlock has no live-service lease; that ambiguous state is fenced
// with kWouldBlock rather than running destructive repair.
Status RecoverRegionForAttach(SharedMemoryRegion& region,
                              const ProcessIdentity& self,
                              uint64_t wait_timeout_ms);

}  // namespace mino

#endif  // MINO_SHM_REGION_RECOVERY_H_
