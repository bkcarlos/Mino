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

#ifndef MINO_SHM_RECOVERY_SCANNER_H_
#define MINO_SHM_RECOVERY_SCANNER_H_

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::shm::recovery {

// ---------------------------------------------------------------------------
// ObjectState: payload lifecycle states stored in SlabHeader::object_state.
//
// Lifecycle (detailed design 12.1):
//   FREE -> ALLOCATED -> BUILDING -> PUBLISHED -> RETIRED -> FREE
//                     |
//                     +---------> ABORTING -> FREE
//
// FREE is not stored in the SlabHeader: it is defined by the allocation
// bitmap bit being clear. A free slot carries no valid header.
// ---------------------------------------------------------------------------
enum class ObjectState : uint32_t {
    kAllocated = 1,
    kBuilding = 2,
    kPublished = 3,
    kRetired = 4,
    kAborting = 5,
};

// Returns true if `value` is a valid published object_state: one that a
// correctly behaving allocator can have published via
// object_state.store(ALLOCATED, release) plus any legal successor state.
// See detailed design 8.3: the store of ALLOCATED is the single publication
// point of an allocation; anything else means the generation was never
// handed out and the slot can be safely reclaimed.
constexpr bool IsValidPublishedState(uint32_t value) {
    return value >= static_cast<uint32_t>(ObjectState::kAllocated) &&
           value <= static_cast<uint32_t>(ObjectState::kAborting);
}

std::string_view ObjectStateName(uint32_t value);

// ---------------------------------------------------------------------------
// RecoveryOwnerState: cross-process Recovery Owner control block.
//
// Mirrors SuperBlock Lifecycle Control (detailed design 6.5):
//   recovery_owner + recovery_epoch + recovery_lease.
//
// This struct lives in shared memory. It contains only atomics and
// fixed-width integers, and uses no pointers (cross-process addressing must
// use offsets). An object of this type is constructed explicitly by the
// region initialization flow (placement new), never zero-filled.
//
// Layout note (detailed design 11: "固定内存布局不应依赖编译器默认
// Packing"): members are arranged as a hot half (ownership + lease, first
// cache line) and a cold half (heartbeat + reserved, second cache line) so
// contender polling of owner_pid never shares a line with the owner's
// heartbeat stores. 128 bytes total, 64-byte aligned.
//
// NOTE: //mino/shm/region:region (D1-03/D1-04) is being developed in
// parallel. When it lands, this layout is the merge point: the SuperBlock
// Lifecycle Control section should embed (or reference by offset) this
// block, and RecoveryScanner switches from raw-pointer construction to the
// region-provided accessor.
// ---------------------------------------------------------------------------
struct alignas(64) RecoveryOwnerState {
    static constexpr uint32_t kMagic = 0x524F5731;  // "ROWS1" recovery owner state v1.

    uint32_t magic;
    uint32_t reserved0;

    // PID of the current owner; 0 means no owner. Acquired via CAS.
    std::atomic<uint64_t> owner_pid;

    // Monotonically increasing recovery epoch. Incremented by the acquiring
    // process right after a successful owner_pid CAS, before any recovery
    // work starts (publication order: owner_pid -> epoch -> lease -> work).
    // ABA protection: a contender comparing owner_pid across a sleep can
    // detect an intermediate acquire/release cycle via the epoch change.
    std::atomic<uint64_t> epoch;

    // Lease deadline in steady-clock nanoseconds. The owner renews it
    // periodically while recovering. A contender may only take over a live
    // owner after NowNs() has passed this deadline.
    std::atomic<uint64_t> lease_deadline_ns;

    uint64_t reserved1[4];  // Complete the hot cache line.

    // Owner progress heartbeat (steady-clock ns), updated between scan
    // phases. Contenders use it to distinguish "owner is alive but its
    // lease renewal thread stalled" from "owner is stuck mid-phase".
    // Deliberately on the second cache line: written by the owner, rarely
    // read by contenders.
    std::atomic<uint64_t> heartbeat_ns;

    uint64_t reserved2[7];  // Complete the cold cache line.
};
static_assert(sizeof(RecoveryOwnerState) == 128,
              "RecoveryOwnerState must be exactly two cache lines");
static_assert(alignof(RecoveryOwnerState) == 64,
              "RecoveryOwnerState must be cache-line aligned");
static_assert(std::is_standard_layout_v<RecoveryOwnerState>,
              "RecoveryOwnerState must remain a POD for SHM placement");

// ---------------------------------------------------------------------------
// RecoveryOwner: exclusive recovery ownership via CAS (detailed design 6.5).
//
// Protocol:
//   1. The attacher that finds a dirty region must not serve business
//      reads/writes; it calls TryAcquire().
//   2. TryAcquire() CASes owner_pid 0 -> self. On success the winner
//      increments epoch, publishes its lease, then may scan/mutate.
//   3. The owner calls RenewLease() periodically and Heartbeat() between
//      phases.
//   4. Contenders poll WaitForIdle() (bounded) or run read-only
//      diagnostics via RecoveryScanner::CreateDiagnostic().
//   5. If the owner crashes and its lease expires, a new process takes over
//      and increments epoch again.
//   6. If the owner dies between "epoch published" and "region ACTIVE", the
//      next owner must redo the full recovery (epoch increments again);
//      recovery operations are idempotent so this is safe.
//
// All methods are safe to call from multiple processes mapped onto the same
// shared RecoveryOwnerState. The class itself carries no ownership of the
// underlying memory.
// ---------------------------------------------------------------------------
class RecoveryOwner {
public:
    // Lease duration used for each acquire/renew. Kept short enough that a
    // crashed owner is taken over quickly, long enough that renewals stay
    // off the hot path.
    static constexpr uint64_t kLeaseDurationNs = 2'000'000'000;  // 2 s.

    // Steady-clock now in nanoseconds. Exposed for tests and for contenders
    // that want to evaluate the lease deadline themselves.
    static uint64_t NowNs();

    // Wraps a shared-memory state block previously initialized by
    // Initialize() (or by the region init flow). `pid` is the caller's own
    // process identity (getpid()); callers must not pass 0.
    RecoveryOwner(RecoveryOwnerState* state, uint64_t pid) noexcept
        : state_(state), pid_(pid) {}

    // Initializes the control block in place. Must be called exactly once by
    // the region creator before any process attaches.
    static void Initialize(RecoveryOwnerState* state) noexcept;

    // Attempts to become the recovery owner.
    //
    // Returns:
    //   Status::Ok()                     - this process is now the owner.
    //   Status::Error(kAlreadyExists)    - another live owner holds the
    //                                      lease; the message carries the
    //                                      observed owner pid/epoch.
    //
    // Takeover of a dead owner (lease expired) CASes the old pid out and
    // increments the epoch; the takeover is one atomic step so two
    // contenders cannot both win.
    Status TryAcquire();

    // Renews the lease. Only meaningful while this process is the owner;
    // returns kPermissionDenied if ownership was lost.
    Status RenewLease();

    // Updates the progress heartbeat. Cheap relaxed store; call between
    // scan phases so contenders can observe liveness.
    void Heartbeat() noexcept;

    // Releases ownership (owner_pid CAS self -> 0). Idempotent: releasing a
    // lost/never-held ownership is a no-op.
    void Release() noexcept;

    // Returns true if this process currently holds ownership.
    bool IsOwner() const noexcept;

    // Returns true if no owner holds an unexpired lease at this instant.
    bool IsIdle() const noexcept { return CurrentOwner() == 0; }

    // Returns the pid of the owner whose lease has not expired, or 0.
    uint64_t CurrentOwner() const noexcept;

    // Current recovery epoch.
    uint64_t Epoch() const noexcept {
        return state_->epoch.load(std::memory_order_acquire);
    }

    // Observed lease deadline (steady-clock ns).
    uint64_t LeaseDeadlineNs() const noexcept {
        return state_->lease_deadline_ns.load(std::memory_order_acquire);
    }

    // Waits until no live owner remains or `timeout_ns` elapses. Returns
    // Status::Ok() when idle, kTimeout otherwise. Used by contenders that
    // must not run destructive recovery concurrently (detailed design 6.5
    // step 4: "其他进程等待、超时或只读诊断").
    Status WaitForIdle(uint64_t timeout_ns) const;

private:
    RecoveryOwnerState* state_;
    uint64_t pid_;
};

// ---------------------------------------------------------------------------
// RecoveryScannerOptions: scan-time tunables.
// ---------------------------------------------------------------------------
struct RecoveryScannerOptions {
    // When true, Scan() also applies repairs (reclaim orphans, fix bitmap
    // mismatches). When false, Scan() is a pure read-only diagnostic pass.
    // ReclaimOrphanSlabs()/CleanupStaleAcks()/VerifyBitmapConsistency()
    // remain individually callable regardless.
    bool repair = true;

    // Retired slabs are reclaimed only when borrow_refcount == 0 and
    // pin_refcount == 0 (detailed design 8.4). The check requires owner
    // lease confirmation in the final system; at the scanner contract level
    // the refcount fields in the slab header are authoritative.
    bool reclaim_retired = true;
};

// ---------------------------------------------------------------------------
// RecoveryReport: aggregate outcome of a Scan().
// ---------------------------------------------------------------------------
struct RecoveryReport {
    uint64_t slots_scanned = 0;         // Total slots visited across all classes.
    uint64_t orphan_slab_count = 0;     // Bitmap occupied but state FREE/invalid.
    uint64_t reclaimed_slab_count = 0;  // Orphans + retired slabs actually reclaimed.
    uint64_t stale_ack_count = 0;       // ACK bits belonging to dead subscribers.
    uint64_t bitmap_inconsistency_count = 0;  // Bitmap free but state != FREE.
    uint64_t corrupted_slab_count = 0;  // Bad magic/CRC; NOT auto-repaired.
    std::string details;                // Human-readable per-finding lines.

    // Appends one line to details (with trailing newline).
    void AddDetail(std::string line) {
        details += line;
        details += '\n';
    }
};

// ---------------------------------------------------------------------------
// RecoveryScanner (D1-09): crash-recovery scan of a shared-memory region.
//
// Implements the recovery convention of detailed design 8.3:
//   "位图已占用但 object_state 非合法已发布状态的 Slot，其 Generation 从未对
//    外发布，Recovery 可安全清除位图回收。ALLOCATED 之前的中间状态不产生有
//    效 Handle，不产生 ABA 风险。"
//
// and the checklist of architecture doc 12.1 (orphan slabs, stale ACKs,
// bitmap consistency). All repairs are idempotent: re-running a scan after
// a completed scan observes a clean state and changes nothing.
//
// ---------------------------------------------------------------------------
// Integration contract (parallel development)
// ---------------------------------------------------------------------------
// //mino/shm/region:region and //mino/shm/allocator:central_slab are being
// built by other agents. The scanner therefore works against a minimal
// layout contract instead of concrete C++ types. Any region/allocator
// implementation can satisfy it by exposing the offsets below from its
// SuperBlock / allocator metadata:
//
//   SuperBlock/Recovery:     RecoveryOwnerState block (64 B).
//   Allocator metadata:      contiguous ClassDescriptor table.
//   Per class:               slot_count slots of slot_stride bytes; the
//                            SlabHeader sits at offset 0 of each slot and
//                            begins with the header prefix below.
//   Bitmap:                  floor(slot_count/64) full words + tail word,
//                            low bit = lowest slot index, 1 = occupied.
//
// The header prefix matches the field order of the SlabHeader v1 proposal
// (detailed design 8.1). When the real headers land, RecoveryScanner gains
// a thin adapter and these structs move behind it; the scan logic is
// unchanged.
// ---------------------------------------------------------------------------
class RecoveryScanner {
public:
    // Cache-line-sized shared bitmap word. Cross-process atomic per
    // detailed design 6.6.
    struct alignas(64) BitmapWord {
        std::atomic<uint64_t> bits;
    };

    // Slab header prefix. Field order and width match detailed design 8.1;
    // CRC coverage matches "不覆盖 CRC 字段自身、object_state、Owner/
    // Transaction 等恢复期可变字段".
    struct SlabHeaderPrefix {
        uint32_t magic;
        uint16_t header_version;
        uint16_t class_id;
        uint32_t generation;
        std::atomic<uint32_t> object_state;
        uint32_t capacity;
        uint32_t object_size;
        uint32_t type_id;
        uint32_t layout_version;
        uint64_t schema_short_id;
        uint64_t owner_epoch;
        uint64_t allocation_transaction_id;
        uint32_t immutable_header_crc;
        uint32_t reserved;
        // Recovery-time mutable trailing fields (NOT covered by the CRC):
        std::atomic<uint32_t> borrow_refcount;  // Active Borrows (12.1/8.4).
        std::atomic<uint32_t> pin_refcount;     // Live object Pins (11.2.1).
    };
    static constexpr uint32_t kSlabMagic = 0x534C4231;  // "SLB1" slab v1.

    struct ClassDescriptor {
        uint32_t class_id;
        uint32_t slot_count;       // Total slots in this class.
        uint64_t bitmap_offset;    // Region-relative offset of BitmapWord[].
        uint64_t slots_offset;     // Region-relative offset of slot 0.
        uint32_t slot_stride;      // Byte distance between consecutive slots.
        uint32_t reserved;
    };

    struct Layout {
        uint64_t recovery_state_offset = 0;
        uint64_t class_table_offset = 0;
        uint32_t class_count = 0;
        uint32_t reserved = 0;
    };

    // Broadcast ACK bitmap state handed in per cleanup pass. The scanner
    // does not own channel metadata (D2 scope); instead the caller (runtime
    // or the `mino recover` tool) supplies the live subscriber set and the
    // ACK bitmaps to scrub. See CleanupStaleAcks().
    struct AckScanInput {
        // Live subscriber bits: bit i set <=> subscriber_id i is registered
        // and alive (lease valid, generation current).
        uint64_t live_subscriber_mask = 0;
        // ACK bitmaps to scrub in place. Each bitmap: bit i set <=>
        // subscriber i still owes an ACK for the associated payload.
        uint64_t* bitmaps = nullptr;
        uint32_t bitmap_count = 0;
    };

    // Creates a scanner over [base, base+size). Validates the layout
    // contract (bounds, strides, magic of the recovery block). Does not
    // acquire recovery ownership.
    static Result<RecoveryScanner> Create(std::byte* base, uint64_t size,
                                          Layout layout,
                                          RecoveryScannerOptions options = {});

    // Full pass: verify consistency, then (if options.repair) reclaim
    // orphans and retired slabs. Read-only when options.repair == false.
    // Idempotent.
    Result<RecoveryReport> Scan();

    // Reclaims orphan slabs: bitmap occupied but object_state is not a valid
    // published state. Per 8.3 their generation was never published, so
    // clearing the bitmap bit carries no ABA risk.
    //   - Valid states keep their slot (PUBLISHED/BUILDING/... are live).
    //   - RETIRED slots with no Borrows and no Pins are reclaimed (8.4).
    //   - Corrupted headers (bad magic) are reported, never auto-repaired
    //     (18.3: 发现不可修复损坏时进入 QUARANTINED，禁止猜测继续运行).
    // Requires recovery ownership; returns kPermissionDenied otherwise.
    Status ReclaimOrphanSlabs();

    // Clears ACK bits of subscribers not present in input.live_subscriber_
    // mask, for every supplied bitmap. Generation binding is the caller's
    // responsibility (9.6): only bitmaps whose subscriber_set_version matches
    // the dead subscriber's generation must be passed in.
    // Idempotent. Does not require ownership (safe bit-AND), but the
    // coordinating runtime normally runs it while holding the recovery
    // lease. Returns the number of bits cleared in `cleared` if non-null.
    Status CleanupStaleAcks(const AckScanInput& input, uint64_t* cleared = nullptr);

    // Detects and (owner only) repairs bitmap/state mismatches:
    //   bitmap free  + state != FREE  -> stale header; state is cleared to
    //                                    FREE (0) so a future allocation
    //                                    starts clean. Counted as
    //                                    bitmap_inconsistency_count.
    //   bitmap occupied + bad magic   -> corruption; reported only.
    // Does not require ownership for detection; repair path requires it.
    Status VerifyBitmapConsistency();

    // Recovery ownership for this region. Multiple processes construct
    // their own RecoveryOwner over the same shared block; the returned
    // handle borrows the scanner's region mapping and must not outlive it.
    RecoveryOwner Owner() noexcept {
        return RecoveryOwner(RecoveryState(), SelfPid());
    }
    RecoveryOwner Owner() const noexcept {
        return RecoveryOwner(const_cast<RecoveryOwnerState*>(RecoveryState()),
                             SelfPid());
    }

    const Layout& layout() const noexcept { return layout_; }
    const RecoveryScannerOptions& options() const noexcept { return options_; }

private:
    RecoveryScanner(std::byte* base, uint64_t size, Layout layout,
                    RecoveryScannerOptions options)
        : base_(base), size_(size), layout_(layout), options_(options) {}

    // Bounds-checked region pointer arithmetic. Returns nullptr on
    // overflow/out-of-bounds (checked arithmetic per detailed design 6.3).
    const void* At(uint64_t offset, uint64_t bytes) const noexcept;
    void* AtMut(uint64_t offset, uint64_t bytes) noexcept;

    RecoveryOwnerState* RecoveryState() noexcept;
    const RecoveryOwnerState* RecoveryState() const noexcept;
    static uint64_t SelfPid();

    const ClassDescriptor* ClassAt(uint32_t index) const noexcept;
    const BitmapWord* BitmapOf(const ClassDescriptor& cls) const noexcept;
    BitmapWord* BitmapOfMut(const ClassDescriptor& cls) noexcept;
    const SlabHeaderPrefix* SlotAt(const ClassDescriptor& cls,
                                   uint32_t slot) const noexcept;
    SlabHeaderPrefix* SlotAtMut(const ClassDescriptor& cls,
                                uint32_t slot) noexcept;

    static bool IsBitSet(const BitmapWord* words, uint32_t index) noexcept;
    static void ClearBit(BitmapWord* words, uint32_t index) noexcept;

    // CRC32C (Castagnoli) over the immutable header prefix, matching the
    // documented coverage (magic..allocation_transaction_id).
    static uint32_t ComputeImmutableCrc(const SlabHeaderPrefix& h) noexcept;

    // One pass over all classes; collects findings into `report` and applies
    // repairs when `repair` is set and the scanner holds ownership.
    Status ScanClasses(RecoveryReport& report, bool repair);

    std::byte* base_;
    uint64_t size_;
    Layout layout_;
    RecoveryScannerOptions options_;
};

}  // namespace mino::shm::recovery

#endif  // MINO_SHM_RECOVERY_SCANNER_H_
