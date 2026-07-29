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

// MPSC channel (design doc 9.5, ADR-0003).
//
// Multiple producers publish into a single strictly-ordered ring consumed by
// one consumer.
//
// Reservation protocol (Vyukov-style probe-then-claim, design doc 9.5):
//   - reservation_cursor hands out logical sequences; physical slot for
//     sequence s is slots[s % capacity], and the slot's current "era" is the
//     cycle in which it was last claimed;
//   - a slot is CLAIMABLE iff its state is kFree (never used) or kRetired
//     (previous era fully consumed). The claimer first PROBES the slot's
//     state; only if claimable does it CAS reservation_cursor forward. This
//     ordering is the key invariant: the cursor is advanced only after the
//     slot is known free, so a handed-out sequence ALWAYS owns its slot —
//     there is no "phantom sequence" that consumed the cursor but lost the
//     slot (which would wedge the ordered commit prefix);
//   - a probe that finds kReady (unconsumed message) reports a full queue;
//     kReserved/kWriting (a holder that has not finished) or kAborted (an
//     unconsumed tombstone) reports kWouldBlock and leaves recovery to
//     AbortOrphanedReservations() — the claimer never stamps a foreign slot.
//   Commit/Abort stamp the slot and set the per-sequence commit bit; the
//   committed prefix advances strictly in order, so the consumer sees a
//   gapless logical sequence (INV-17).
//
// Crash recovery (design doc 9.5, 12.3): every reservation binds its owner
// through the MpscReservationMeta sidecar. AbortOrphanedReservations()
// stamps a RESERVED/WRITING slot ABORTED only when ALL of these hold:
//   1. the slot's recorded sequence equals the sequence it would be
//      reclaimed for (a recycled slot is never touched);
//   2. the owner process is unreachable (dead) — see IsOwnerAlive();
//   3. the reservation is older than the producer lease, so a
//      live-but-paused producer is never reclaimed ("不能仅因超时就判定崩
//      溃", design doc 9.5).
// Condition 2 uses /proc liveness on Linux; elsewhere the lease is the only
// signal, so deployments must size the lease well above the longest
// legitimate Reserve -> Commit window (default 30 s).
//
// MpscChannel is header-only: a non-owning view over shared memory (design
// doc 9.9 lifecycle: a process exiting does not "destruct" the channel).

#ifndef MINO_SHM_CHANNEL_MPSC_CHANNEL_H_
#define MINO_SHM_CHANNEL_MPSC_CHANNEL_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <type_traits>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/queue_full_policy.h"
#include "mino/shm/channel/spsc_channel.h"  // detail::SpinPause

#if defined(__linux__)
#include <unistd.h>
#endif

namespace mino {

class MpscChannel {
public:
    static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
                  "MpscChannel requires lock-free 64-bit atomics");

    static constexpr uint64_t kCacheLineSize = 64;
    static constexpr uint64_t kMagic = 0x4D49'4E4F'4D50'5343ULL;  // "MINOMPSC"
    static constexpr uint32_t kLayoutVersion = 1;

    // Default producer lease (design doc 9.5). 30 s tolerates long pauses
    // (SIGSTOP, scheduling) without wedging the queue for minutes.
    static constexpr uint64_t kDefaultProducerLeaseNs =
        30ULL * 1000 * 1000 * 1000;

    // -----------------------------------------------------------------------
    // Control block
    // -----------------------------------------------------------------------
    struct alignas(kCacheLineSize) ControlBlock {
        // -- Line 0: identity + immutable configuration ---------------------
        std::atomic<uint64_t> magic{0};
        std::atomic<uint32_t> layout_version{0};
        uint32_t reserved0 = 0;
        uint64_t capacity = 0;  // power of two, >= 64
        uint64_t reserved1 = 0;
        unsigned char pad0[kCacheLineSize - 8 - 4 - 4 - 8 - 8] = {};

        // -- Line 1: reservation cursor (multi-writer: all producers) -------
        alignas(kCacheLineSize) std::atomic<uint64_t> reservation_cursor{0};
        unsigned char pad1[kCacheLineSize - 8] = {};

        // -- Line 2: committed cursor (bitmap-driven prefix) ----------------
        //
        // Prefix of the logical sequence that is fully published (READY or
        // ABORTED). Advances strictly in order by scanning the commit
        // bitmap; the consumer polls against this, never against
        // reservation_cursor, so half-written slots are invisible.
        alignas(kCacheLineSize) std::atomic<uint64_t> committed_cursor{0};
        unsigned char pad2[kCacheLineSize - 8] = {};

        // -- Line 3: consumer cursor (single writer: the consumer) ----------
        alignas(kCacheLineSize) std::atomic<uint64_t> consumer_cursor{0};
        unsigned char pad3[kCacheLineSize - 8] = {};

        // -- Line 4..: per-sequence commit bits ------------------------------
        //
        // Bit (seq & mask) records "logical sequence seq finished". Bit
        // ownership is tied to the logical sequence, not the physical slot:
        // the advance scan clears a bit exactly once per era before the slot
        // can be re-claimed for its next sequence, so no ABA is possible.
        alignas(kCacheLineSize) std::atomic<uint64_t> commit_bits[1];  // tail
    };

    static constexpr uint64_t CommitWordCount(uint64_t capacity) {
        return capacity / 64;  // capacity >= 64 (validated), power of two
    }

    // IndexSlot requires 64-byte alignment, so the slot array must start on
    // a 64-byte boundary. The commit bitmap tail is only (capacity/64)*8
    // bytes, which is not a multiple of 64 for every capacity; round the
    // slot-array offset up to the next cache line.
    static constexpr uint64_t AlignUp64(uint64_t n) {
        return (n + kCacheLineSize - 1) & ~(kCacheLineSize - 1);
    }

    // Byte offset of the IndexSlot array from the control-block base.
    static constexpr uint64_t SlotsOffset(uint64_t capacity) {
        return AlignUp64(offsetof(ControlBlock, commit_bits) +
                         CommitWordCount(capacity) * 8);
    }

    // Byte offset of the MpscReservationMeta sidecar array. capacity *
    // sizeof(IndexSlot) is always a multiple of 64 (128-byte slots), so the
    // sidecar stays 64-aligned without further padding.
    static constexpr uint64_t MetasOffset(uint64_t capacity) {
        return SlotsOffset(capacity) + capacity * sizeof(IndexSlot);
    }

    // Total bytes the channel occupies in shared memory: control block
    // (fixed part + commit bitmap, padded) + IndexSlot array + sidecar array.
    static constexpr uint64_t RequiredSize(uint64_t capacity) {
        return MetasOffset(capacity) + capacity * sizeof(MpscReservationMeta);
    }

    // -----------------------------------------------------------------------
    // Init / Attach
    // -----------------------------------------------------------------------

    // Initializes a channel of `capacity` slots in place at `shm_base`.
    // `capacity` must be a power of two and >= 64 (word-based bitmap). The
    // caller must size the region with RequiredSize() and must not touch
    // the memory concurrently while Init runs. 64-byte aligned base.
    static Result<MpscChannel> Init(void* shm_base, uint64_t capacity) {
        if (shm_base == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must not be null");
        }
        if (reinterpret_cast<uintptr_t>(shm_base) % alignof(ControlBlock) !=
            0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must be 64-byte aligned");
        }
        if (capacity < 64 || (capacity & (capacity - 1)) != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "capacity must be a power of two and >= 64");
        }
        if (capacity > (uint64_t{1} << 32)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "capacity exceeds the supported maximum (2^32)");
        }

        auto* control = static_cast<ControlBlock*>(shm_base);
        control->layout_version.store(kLayoutVersion,
                                      std::memory_order_relaxed);
        control->reserved0 = 0;
        control->capacity = capacity;
        control->reserved1 = 0;
        control->reservation_cursor.store(0, std::memory_order_relaxed);
        control->committed_cursor.store(0, std::memory_order_relaxed);
        control->consumer_cursor.store(0, std::memory_order_relaxed);

        const uint64_t words = CommitWordCount(capacity);
        for (uint64_t i = 0; i < words; ++i) {
            control->commit_bits[i].store(0, std::memory_order_relaxed);
        }

        IndexSlot* slots = SlotsOf(shm_base, capacity);
        for (uint64_t i = 0; i < capacity; ++i) {
            IndexSlot& s = slots[i];
            s.msg_type = 0;
            s.schema_version = 0;
            s.schema_short_id = 0;
            s.schema_layout_version = 0;
            s.reserved0 = 0;
            s.sequence_num.store(i, std::memory_order_relaxed);
            s.timestamp_ns = 0;
            s.payload = ShmHandle{};
            s.payload_len = 0;
            s.immutable_metadata_crc = 0;
            s.flags = 0;
            std::memset(s.padding_, 0, sizeof(s.padding_));
            s.state.store(static_cast<uint32_t>(SlotState::kFree),
                          std::memory_order_relaxed);
        }

        MpscReservationMeta* metas = MetasOf(shm_base, capacity);
        for (uint64_t i = 0; i < capacity; ++i) {
            metas[i] = MpscReservationMeta{};
        }

        control->magic.store(kMagic, std::memory_order_release);
        return MpscChannel(control, slots, metas, capacity);
    }

    // Attaches to an already-initialized channel at `shm_base`, validating
    // magic, layout version and capacity. Any failure refuses the attach.
    static Result<MpscChannel> Attach(void* shm_base) {
        if (shm_base == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must not be null");
        }
        if (reinterpret_cast<uintptr_t>(shm_base) % alignof(ControlBlock) !=
            0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must be 64-byte aligned");
        }

        auto* control = static_cast<ControlBlock*>(shm_base);
        if (control->magic.load(std::memory_order_acquire) != kMagic) {
            return Status::Error(StatusCode::kCorruption,
                                 "MPSC control block magic mismatch");
        }
        if (control->layout_version.load(std::memory_order_acquire) !=
            kLayoutVersion) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "MPSC layout version mismatch");
        }
        const uint64_t capacity = control->capacity;
        if (capacity < 64 || (capacity & (capacity - 1)) != 0 ||
            capacity > (uint64_t{1} << 32)) {
            return Status::Error(StatusCode::kCorruption,
                                 "MPSC control block capacity is invalid");
        }
        return MpscChannel(control, SlotsOf(shm_base, capacity),
                           MetasOf(shm_base, capacity), capacity);
    }

    // -----------------------------------------------------------------------
    // Producer identity
    // -----------------------------------------------------------------------

    // Identity bound into every reservation sidecar. `publisher_id` is the
    // Registry-assigned PublisherId (design doc 10.4); `owner` is the
    // producing process identity used by crash recovery to distinguish a
    // dead producer from a paused one.
    struct ProducerIdentity {
        ProcessIdentity owner;
        uint64_t publisher_id = 0;
    };

    // -----------------------------------------------------------------------
    // Producer: Reserve / Fill / Commit / Abort
    // -----------------------------------------------------------------------

    // A Reservation is a producer's exclusive write window into one slot.
    // Move-only; must be Commit()ed or Abort()ed. Destroying a live
    // Reservation without committing stamps an ABORTED tombstone so the
    // queue can never wedge (design doc 9.5).
    class Reservation {
    public:
        Reservation() noexcept = default;
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;

        Reservation(Reservation&& other) noexcept
            : channel_(other.channel_),
              slot_(other.slot_),
              sequence_(other.sequence_),
              active_(other.active_) {
            other.channel_ = nullptr;
            other.slot_ = nullptr;
            other.active_ = false;
        }
        Reservation& operator=(Reservation&& other) noexcept {
            if (this != &other) {
                AbortIfActive();
                channel_ = other.channel_;
                slot_ = other.slot_;
                sequence_ = other.sequence_;
                active_ = other.active_;
                other.channel_ = nullptr;
                other.slot_ = nullptr;
                other.active_ = false;
            }
            return *this;
        }

        ~Reservation() { AbortIfActive(); }

        // The slot the producer fills. Only valid while active().
        IndexSlot* slot() noexcept { return slot_; }
        const IndexSlot* slot() const noexcept { return slot_; }
        IndexSlot* operator->() noexcept { return slot_; }
        IndexSlot& operator*() noexcept { return *slot_; }

        bool active() const noexcept { return active_; }

        // Logical sequence of this reservation (physical slot index =
        // sequence % capacity, design doc 9.5).
        uint64_t sequence() const noexcept { return sequence_; }

        // Seals the immutable CRC, publishes the slot READY and feeds the
        // ordered-prefix scan. After Commit the Reservation is empty.
        Status Commit() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "reservation is not active");
            }
            active_ = false;
            MpscChannel* ch = channel_;
            channel_ = nullptr;
            IndexSlot* slot = slot_;
            slot_ = nullptr;
            return ch->CommitSlot(slot, sequence_);
        }

        // Stamps an ABORTED tombstone (the consumer will skip it) and feeds
        // the ordered-prefix scan. After Abort the Reservation is empty.
        Status Abort() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "reservation is not active");
            }
            active_ = false;
            MpscChannel* ch = channel_;
            channel_ = nullptr;
            IndexSlot* slot = slot_;
            slot_ = nullptr;
            return ch->AbortSlot(slot, sequence_);
        }

    private:
        friend class MpscChannel;
        Reservation(MpscChannel* channel, IndexSlot* slot,
                    uint64_t sequence) noexcept
            : channel_(channel), slot_(slot), sequence_(sequence),
              active_(true) {}

        void AbortIfActive() noexcept {
            if (active_) {
                channel_->AbortSlot(slot_, sequence_).ok();
            }
        }

        MpscChannel* channel_ = nullptr;
        IndexSlot* slot_ = nullptr;
        uint64_t sequence_ = 0;
        bool active_ = false;
    };

    // Attempts to reserve the next slot for writing, applying `policy` when
    // the probe reports the queue full. On success returns an active
    // Reservation whose slot() is in state kWriting, owned exclusively by the
    // caller, with the owner identity stamped in the sidecar.
    //
    // Errors:
    //   kResourceExhausted : kFail and the queue is full.
    //   kDegraded          : kDropNewest / kSample dropped the message.
    //   kWouldBlock        : the next slot is held by an unfinished
    //                        reservation or tombstone; run recovery. (Also the
    //                        outcome for kDropOldest when the queue is wedged
    //                        on a stalled slot rather than a consumable one.)
    Result<Reservation> Reserve(
        const ProducerIdentity& owner,
        QueueFullPolicy policy = QueueFullPolicy::kFail,
        uint32_t sample_rate = 1) noexcept {
        for (;;) {
            Result<Reservation> probed = TryReserveAfterSpaceCheck(owner);
            if (probed.ok()) {
                return probed;
            }
            const StatusCode code = probed.status().code();
            if (code != StatusCode::kResourceExhausted &&
                code != StatusCode::kWouldBlock) {
                return probed;  // Not a fullness condition: propagate.
            }
            // kDropOldest can only free space by retiring a consumable kReady
            // slot. A wedge on an unfinished reservation / tombstone is not
            // droppable — report kWouldBlock and let recovery reclaim it.
            const bool genuinely_full = (code == StatusCode::kResourceExhausted);

            switch (policy) {
                case QueueFullPolicy::kFail:
                    return Status::Error(StatusCode::kResourceExhausted,
                                         "MPSC queue full");
                case QueueFullPolicy::kBlock:
                    // Unbounded spin by design (see SpscChannel): bounded
                    // waiting is layered on top by the Runtime publisher.
                    detail::SpinPause();
                    continue;
                case QueueFullPolicy::kDropNewest:
                    return Status::Error(
                        StatusCode::kDegraded,
                        "MPSC queue full: newest message dropped");
                case QueueFullPolicy::kDropOldest:
                    if (!genuinely_full) {
                        return Status::Error(
                            StatusCode::kWouldBlock,
                            "MPSC wedged on a stalled slot; kDropOldest cannot "
                            "retire an unfinished reservation");
                    }
                    TryForceDropOldest();
                    continue;
                case QueueFullPolicy::kSample: {
                    const uint32_t rate = sample_rate == 0 ? 1 : sample_rate;
                    const uint64_t prod = control_->reservation_cursor.load(
                        std::memory_order_relaxed);
                    if ((prod % rate) != 0) {
                        return Status::Error(
                            StatusCode::kDegraded,
                            "MPSC queue full: message sampled out");
                    }
                    detail::SpinPause();
                    continue;
                }
            }
        }
    }

    // Non-blocking reservation attempt. Returns kResourceExhausted if the
    // queue is full, kWouldBlock if wedged on a stalled slot/tombstone.
    Result<Reservation> TryReserve(const ProducerIdentity& owner) noexcept {
        return TryReserveAfterSpaceCheck(owner);
    }

    // -----------------------------------------------------------------------
    // Consumer: Poll / Ack
    // -----------------------------------------------------------------------

    // A Borrow is the consumer's read window into one published message
    // (same snapshot semantics as SpscChannel::Borrow). Move-only;
    // destroying a live Borrow without Ack() leaves the slot READY, so a
    // slow/failing consumer simply doesn't advance the queue.
    class Borrow {
    public:
        Borrow() noexcept = default;
        Borrow(const Borrow&) = delete;
        Borrow& operator=(const Borrow&) = delete;

        Borrow(Borrow&& other) noexcept
            : channel_(other.channel_),
              snapshot_(other.snapshot_),
              active_(other.active_) {
            other.channel_ = nullptr;
            other.active_ = false;
        }
        Borrow& operator=(Borrow&& other) noexcept {
            if (this != &other) {
                channel_ = other.channel_;
                snapshot_ = other.snapshot_;
                active_ = other.active_;
                other.channel_ = nullptr;
                other.active_ = false;
            }
            return *this;
        }

        const IndexSlotSnapshot* slot() const noexcept { return &snapshot_; }
        const IndexSlotSnapshot* operator->() const noexcept {
            return &snapshot_;
        }
        const IndexSlotSnapshot& operator*() const noexcept {
            return snapshot_;
        }
        bool active() const noexcept { return active_; }

        // Marks the slot retired and advances the consumer cursor. If the
        // message was overtaken by kDropOldest while borrowed, reports
        // kNotFound and leaves the queue untouched.
        Status Ack() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "borrow is not active");
            }
            active_ = false;
            MpscChannel* ch = channel_;
            channel_ = nullptr;
            return ch->AckSlot(snapshot_.sequence_num);
        }

    private:
        friend class MpscChannel;
        Borrow(MpscChannel* channel, const IndexSlotSnapshot& snapshot) noexcept
            : channel_(channel), snapshot_(snapshot), active_(true) {}

        MpscChannel* channel_ = nullptr;
        IndexSlotSnapshot snapshot_;
        bool active_ = false;
    };

    // Polls the next ready message. Returns:
    //   an active Borrow : a message is available.
    //   kWouldBlock      : the committed prefix has not advanced past the
    //                      consumer cursor (empty, or wedged behind an
    //                      unresolved reservation; if it persists, run
    //                      AbortOrphanedReservations()).
    //   kCorruption      : a slot failed its sequence or CRC check; the bad
    //                      slot was retired and skipped.
    //
    // Aborted tombstones are transparently retired and skipped.
    Result<Borrow> Poll() noexcept {
        while (true) {
            const uint64_t cons = control_->consumer_cursor.load(
                std::memory_order_relaxed);
            const uint64_t committed = control_->committed_cursor.load(
                std::memory_order_acquire);
            if (cons == committed) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "MPSC queue empty (or wedged behind an "
                                     "unresolved reservation)");
            }
            IndexSlot* slot = &slots_[cons & mask_];
            const uint32_t state =
                slot->state.load(std::memory_order_acquire);

            if (state == static_cast<uint32_t>(SlotState::kAborted)) {
                // Release: pairs with the producer's acquire probe in
                // TryReserveAfterSpaceCheck so this retire happens-before the
                // slot is reused for a new era.
                slot->state.store(static_cast<uint32_t>(SlotState::kRetired),
                                  std::memory_order_release);
                control_->consumer_cursor.store(cons + 1,
                                                std::memory_order_release);
                continue;
            }
            if (state != static_cast<uint32_t>(SlotState::kReady)) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "MPSC slot not yet ready");
            }
            if (slot->sequence_num.load(std::memory_order_acquire) != cons) {
                RetireAndAdvance(slot, cons);
                return Status::Error(StatusCode::kCorruption,
                                     "MPSC slot sequence mismatch (skipped)");
            }
            IndexSlotSnapshot snapshot = SnapshotIndexSlot(*slot);
            if (!VerifySnapshotCrc(snapshot)) {
                RetireAndAdvance(slot, cons);
                return Status::Error(StatusCode::kCorruption,
                                     "MPSC slot immutable CRC mismatch "
                                     "(skipped)");
            }
            return Borrow(this, snapshot);
        }
    }

    // -----------------------------------------------------------------------
    // Crash recovery (design doc 9.5 / 12.3)
    // -----------------------------------------------------------------------

    // Scans the reserved-but-unfinished window
    // [committed_cursor, reservation_cursor) and stamps ABORTED tombstones
    // on slots whose owner is dead AND whose reservation is older than
    // `lease_ns` relative to `now_ns`. Returns the number of tombstones
    // stamped. Idempotent; safe to run concurrently with producers and with
    // other scanners (CAS arbitration throughout).
    //
    // `lease_ns == 0` selects kDefaultProducerLeaseNs.
    uint64_t AbortOrphanedReservations(
        uint64_t now_ns, uint64_t lease_ns = 0) noexcept {
        if (lease_ns == 0) {
            lease_ns = kDefaultProducerLeaseNs;
        }
        const uint64_t committed =
            control_->committed_cursor.load(std::memory_order_acquire);
        const uint64_t reserved =
            control_->reservation_cursor.load(std::memory_order_acquire);
        uint64_t aborted = 0;
        for (uint64_t seq = committed; seq < reserved; ++seq) {
            const uint64_t phys = seq & mask_;
            IndexSlot* slot = &slots_[phys];
            const uint32_t state =
                slot->state.load(std::memory_order_acquire);
            if (state != static_cast<uint32_t>(SlotState::kReserved) &&
                state != static_cast<uint32_t>(SlotState::kWriting)) {
                continue;
            }
            // Validity rule (design doc 9.2): the sidecar is only
            // meaningful while the slot still carries this exact sequence.
            if (slot->sequence_num.load(std::memory_order_acquire) != seq) {
                continue;
            }
            MpscReservationMeta& meta = metas_[phys];
            if (now_ns - meta.reservation_timestamp_ns < lease_ns) {
                continue;  // Fresh reservation: owner may still be working.
            }
            if (IsOwnerAlive(meta)) {
                continue;  // Live (possibly paused) owner: never reclaim.
            }
            // CAS so a racing legitimate Commit/Abort (owner was merely
            // unreachable from this scanner, e.g. /proc hidepid) loses
            // cleanly instead of being double-stamped.
            uint32_t expected = state;
            if (!slot->state.compare_exchange_strong(
                    expected, static_cast<uint32_t>(SlotState::kAborted),
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                continue;
            }
            SetCommitBit(seq);
            ++aborted;
        }
        if (aborted != 0) {
            AdvanceCommittedCursor();
        }
        return aborted;
    }

    // -----------------------------------------------------------------------
    // Observers
    // -----------------------------------------------------------------------

    bool IsEmpty() const noexcept {
        const uint64_t committed =
            control_->committed_cursor.load(std::memory_order_acquire);
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        return committed == cons;
    }

    bool IsFull() const noexcept {
        return IsFullAt(
            control_->reservation_cursor.load(std::memory_order_acquire));
    }

    // Number of messages currently visible to the consumer.
    uint64_t Size() const noexcept {
        const uint64_t committed =
            control_->committed_cursor.load(std::memory_order_acquire);
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        return committed - cons;
    }

    uint64_t capacity() const noexcept { return capacity_; }

private:
    MpscChannel(ControlBlock* control, IndexSlot* slots,
                MpscReservationMeta* metas, uint64_t capacity) noexcept
        : control_(control),
          slots_(slots),
          metas_(metas),
          capacity_(capacity),
          mask_(capacity - 1) {}

    static IndexSlot* SlotsOf(void* shm_base, uint64_t capacity) noexcept {
        return reinterpret_cast<IndexSlot*>(
            static_cast<unsigned char*>(shm_base) + SlotsOffset(capacity));
    }

    static MpscReservationMeta* MetasOf(void* shm_base,
                                        uint64_t capacity) noexcept {
        return reinterpret_cast<MpscReservationMeta*>(
            static_cast<unsigned char*>(shm_base) + MetasOffset(capacity));
    }

    bool IsFullAt(uint64_t reservation_cursor) const noexcept {
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        return reservation_cursor - cons >= capacity_;
    }

    // kDropOldest (design doc 9.8): forcibly retire the oldest unconsumed
    // slot and advance the consumer cursor past it. Only a kReady slot is a
    // safe victim: it is already committed (committed_cursor has passed it),
    // so retiring it keeps consumer_cursor <= committed_cursor. Retiring a
    // kReserved/kWriting slot would advance the consumer past an unfinished
    // sequence and corrupt the ordered prefix. A consumer holding a Borrow of
    // the victim keeps a valid snapshot; its late Ack reports kNotFound. The
    // payload is NOT reclaimed here (only after no borrows remain). The CAS
    // keeps the cursor monotonic against concurrent producers.
    void TryForceDropOldest() noexcept {
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        IndexSlot* oldest = &slots_[cons & mask_];
        if (oldest->state.load(std::memory_order_acquire) !=
            static_cast<uint32_t>(SlotState::kReady)) {
            return;
        }
        // Release: pairs with the producer's acquire probe so the consumer's
        // prior reads of this slot happen-before its reuse in a new era.
        oldest->state.store(static_cast<uint32_t>(SlotState::kRetired),
                            std::memory_order_release);
        uint64_t expected = cons;
        control_->consumer_cursor.compare_exchange_strong(
            expected, cons + 1, std::memory_order_acq_rel,
            std::memory_order_relaxed);
    }

    // Vyukov-style probe-then-claim. Probes the slot for the current cursor
    // position and only advances the cursor once the slot is known claimable,
    // so a handed-out sequence always owns its slot (no phantom sequence).
    //
    // Returns:
    //   an active Reservation : slot claimed and in state kWriting.
    //   kResourceExhausted    : the slot holds an unconsumed kReady message
    //                         (the queue is genuinely full; kDropOldest may
    //                         forcibly retire it).
    //   kWouldBlock           : the slot is held by an unfinished reservation
    //                         (kReserved/kWriting) or an unconsumed tombstone
    //                         (kAborted); run AbortOrphanedReservations() to
    //                         reclaim a stalled holder.
    Result<Reservation> TryReserveAfterSpaceCheck(
        const ProducerIdentity& owner) noexcept {
        for (;;) {
            const uint64_t res = control_->reservation_cursor.load(
                std::memory_order_acquire);
            const uint64_t phys = res & mask_;
            IndexSlot* slot = &slots_[phys];
            const uint32_t state = slot->state.load(std::memory_order_acquire);

            if (state == static_cast<uint32_t>(SlotState::kReady)) {
                // Genuine backpressure: the previous-era message in this slot
                // has not been consumed yet (consumer_cursor has not passed
                // it), i.e. reservation_cursor - consumer_cursor >= capacity.
                return Status::Error(StatusCode::kResourceExhausted,
                                     "MPSC queue full");
            }
            if (state == static_cast<uint32_t>(SlotState::kReserved) ||
                state == static_cast<uint32_t>(SlotState::kWriting)) {
                // A previous-era holder reserved but never finished (stalled
                // or crashed). Never stamp a foreign slot from here: recovery
                // owns that decision (it checks owner liveness + lease).
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "MPSC slot held by an unfinished reservation; recovery "
                    "required");
            }
            if (state == static_cast<uint32_t>(SlotState::kAborted)) {
                // Tombstone not yet retired by the consumer; retry once the
                // consumer cursor advances past it.
                return Status::Error(StatusCode::kWouldBlock,
                                     "MPSC slot holds an unconsumed tombstone");
            }

            // state is kFree or kRetired: the slot is claimable. Claim the
            // sequence by advancing the cursor; only on success do we own the
            // slot for this era.
            uint64_t expected = res;
            if (!control_->reservation_cursor.compare_exchange_weak(
                    expected, res + 1, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                continue;  // Another producer claimed res; re-probe.
            }

            // We own sequence `res` and slot `phys` for this era. Between the
            // probe and the successful cursor CAS no one else can transition a
            // kFree/kRetired slot to an occupied state (consumers only retire
            // kReady/kAborted, recovery only touches kReserved/kWriting, and
            // another producer can only reach this same physical slot by first
            // claiming this very cursor value). So a plain store publishes the
            // reservation; no second CAS is needed and none can fail.
            MpscReservationMeta& meta = metas_[phys];
            meta.owner_process_id = owner.owner.process_id;
            meta.owner_process_epoch = owner.owner.process_epoch;
            meta.owner_publisher_id = owner.publisher_id;
            meta.reservation_timestamp_ns = MonotonicNowNs();
            slot->sequence_num.store(res, std::memory_order_relaxed);
            slot->state.store(static_cast<uint32_t>(SlotState::kWriting),
                              std::memory_order_relaxed);
            return Reservation(this, slot, res);
        }
    }

    // Seals the CRC, publishes READY, sets the commit bit and feeds the
    // ordered-prefix scan.
    Status CommitSlot(IndexSlot* slot, uint64_t sequence) noexcept {
        SealIndexSlotImmutableCrc(*slot);
        slot->state.store(static_cast<uint32_t>(SlotState::kReady),
                          std::memory_order_release);
        SetCommitBit(sequence);
        AdvanceCommittedCursor();
        return Status::Ok();
    }

    // Stamps an ABORTED tombstone, sets the commit bit and feeds the scan.
    Status AbortSlot(IndexSlot* slot, uint64_t sequence) noexcept {
        uint32_t expected = static_cast<uint32_t>(SlotState::kWriting);
        slot->state.compare_exchange_strong(
            expected, static_cast<uint32_t>(SlotState::kAborted),
            std::memory_order_acq_rel, std::memory_order_relaxed);
        SetCommitBit(sequence);
        AdvanceCommittedCursor();
        return Status::Ok();
    }

    void SetCommitBit(uint64_t sequence) noexcept {
        control_->commit_bits[(sequence & mask_) / 64].fetch_or(
            uint64_t{1} << ((sequence & mask_) % 64),
            std::memory_order_acq_rel);
    }

    void ClearCommitBit(uint64_t sequence) noexcept {
        control_->commit_bits[(sequence & mask_) / 64].fetch_and(
            ~(uint64_t{1} << ((sequence & mask_) % 64)),
            std::memory_order_acq_rel);
    }

    bool TestCommitBit(uint64_t sequence) const noexcept {
        return (control_->commit_bits[(sequence & mask_) / 64].load(
                    std::memory_order_acquire) >>
                ((sequence & mask_) % 64)) &
               uint64_t{1};
    }

    // Advances the committed prefix across every consecutively-finished
    // slot. Idempotent; concurrent scanners publish monotonic values only.
    void AdvanceCommittedCursor() noexcept {
        while (true) {
            const uint64_t committed =
                control_->committed_cursor.load(std::memory_order_acquire);
            const uint64_t reserved = control_->reservation_cursor.load(
                std::memory_order_acquire);
            if (committed >= reserved || !TestCommitBit(committed)) {
                return;
            }
            ClearCommitBit(committed);
            uint64_t expected = committed;
            if (!control_->committed_cursor.compare_exchange_strong(
                    expected, committed + 1, std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                continue;  // A peer scanner advanced; re-read and retry.
            }
        }
    }

    void RetireAndAdvance(IndexSlot* slot, uint64_t cons) noexcept {
        // Release: pairs with the producer's acquire probe so the consumer's
        // prior reads of this slot happen-before its reuse in a new era.
        slot->state.store(static_cast<uint32_t>(SlotState::kRetired),
                          std::memory_order_release);
        control_->consumer_cursor.store(cons + 1, std::memory_order_release);
    }

    Status AckSlot(uint64_t sequence) noexcept {
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_relaxed);
        if (sequence != cons) {
            return Status::Error(
                StatusCode::kNotFound,
                "message was dropped (overtaken by kDropOldest)");
        }
        RetireAndAdvance(&slots_[cons & mask_], cons);
        return Status::Ok();
    }

    // -----------------------------------------------------------------------
    // Owner liveness (design doc 9.5: never judge a crash by timeout alone)
    // -----------------------------------------------------------------------

    static uint64_t MonotonicNowNs() noexcept {
        // steady_clock is monotonic and process-local; SHM scanners compare
        // reservations only against durations, so cross-process clock
        // disagreement is bounded by lease sizing (30 s default).
        const auto now = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }

    // True iff the recorded owner could still publish. A process that no
    // longer exists can never publish again. The OS PID lives in
    // MpscReservationMeta::owner_process_id (ProcessIdentity::process_id);
    // owner_process_epoch distinguishes PID reuse (design doc 4.3 / 12.4).
    // The scanner's lease check bounds the residual window where a recycled
    // PID looks alive to a /proc probe.
    static bool IsOwnerAlive(const MpscReservationMeta& meta) noexcept {
#if defined(__linux__)
        const uint32_t pid = static_cast<uint32_t>(meta.owner_process_id);
        if (pid != 0) {
            char path[32];
            std::snprintf(path, sizeof(path), "/proc/%u", pid);
            if (::access(path, F_OK) != 0) {
                return false;  // No such process: definitively dead.
            }
        }
#else
        (void)meta;  // No /proc on this platform: lease is the only signal.
#endif
        // The process exists (or the platform has no /proc): it may be
        // alive-but-paused, so the lease timeout remains the deciding
        // factor (design doc 9.5).
        return true;
    }

    ControlBlock* control_ = nullptr;
    IndexSlot* slots_ = nullptr;
    MpscReservationMeta* metas_ = nullptr;
    uint64_t capacity_ = 0;
    uint64_t mask_ = 0;
};

}  // namespace mino

#endif  // MINO_SHM_CHANNEL_MPSC_CHANNEL_H_
