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
// Mutual exclusion is provided by a single 64-bit CAS on the SuperBlock
// recovery_lease_ns field: the lease holds a monotonic expiry timestamp and is
// "held" iff it is in the future. Acquiring is an atomic
// compare_exchange from an expired value to a fresh future value, so exactly
// one racing process wins. recovery_epoch is incremented on every acquisition
// to record the takeover generation (section 6.5 step 5), and recovery_owner
// records the owner's ProcessIdentity for diagnostics.
class RecoveryOwner {
public:
    RecoveryOwner(const RecoveryOwner&) = delete;
    RecoveryOwner& operator=(const RecoveryOwner&) = delete;
    RecoveryOwner(RecoveryOwner&&) noexcept = default;
    RecoveryOwner& operator=(RecoveryOwner&&) noexcept = default;

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

    // Releases ownership: clears the owner identity and expires the lease so
    // another process may acquire. Safe to call once; subsequent calls are
    // no-ops.
    Status Release();

    // True while this object holds the lease and it has not expired.
    bool IsOwner() const;

private:
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
//   2. If dirty, acquire Recovery Ownership, run recovery, publish a new
//      Region Epoch, then transition to ACTIVE (in that order, per 6.5 step 6).
//   3. If another process is already recovering, wait up to
//      `wait_timeout_ms` for it to finish, then re-check.
//   4. If recovery cannot complete reliably, transition to QUARANTINED.
//
// The deep consistency scanning (orphan slabs, bitmap reconciliation) is a
// later milestone (D1-09, //mino/shm/recovery:scanner); this function performs
// the ownership, epoch, and state-machine protocol that makes recovery
// single-writer and crash-safe.
Status RecoverRegionForAttach(SharedMemoryRegion& region,
                              const ProcessIdentity& self,
                              uint64_t wait_timeout_ms);

}  // namespace mino

#endif  // MINO_SHM_REGION_RECOVERY_H_
