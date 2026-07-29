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
// Reservation protocol (Vyukov per-slot turn sequence, design doc 9.5):
//   - reservation_cursor hands out logical sequences; physical slot for
//     sequence s is slots[s % capacity];
//   - each slot carries a numerically-unique (non-modular) `turn` value that
//     identifies its protocol era: turn == pos means the slot is free for the
//     producer claiming cursor pos; turn == pos + 1 means the slot is READY
//     for the consumer reading pos; turn == pos + capacity means the slot was
//     consumed and is free for the next era (pos + capacity);
//   - a slot is CLAIMABLE iff slot[pos % capacity].turn == pos. Because turn
//     is not modular, a producer that wraps the ring always observes
//     turn != pos on a slot still occupied by a previous era, so the cursor
//     CAS and the slot occupation are race-free without any modular-ABA
//     window (the reason `state` alone, being capacity-modular, could not be
//     used to distinguish eras);
//   - the cursor is advanced (CAS) only after the turn probe reports the
//     slot free, so a handed-out sequence ALWAYS owns its slot — there is no
//     "phantom sequence" that consumed the cursor but lost the slot (which
//     would wedge the ordered commit prefix);
//   - commit publishes state kReady (release) then turn = pos + 1 (release);
//     the consumer polls turn == cons + 1 for readiness, so half-written
//     slots are invisible without an ordered commit bitmap;
//   - retire/Ack publishes turn = cons + capacity (release), freeing the
//     slot for the next era.
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
    // v3: per-slot Vyukov `turn` (IndexSlot offset 72) replaces state-based
    // probing for era discrimination, eliminating the modular-ABA window
    // between the cursor CAS and slot occupation. v2 dropped the ordered
    // commit bitmap; v1 introduced the bitmap-based ordered prefix.
    static constexpr uint32_t kLayoutVersion = 3;

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

        // -- Line 2: consumer cursor (single logical writer) ----------------
        //
        // The read position and the recovery window's lower bound. Advanced
        // only by the consumer (Poll/Ack) or by a producer acting for
        // kDropOldest; every writer uses CAS so the cursor stays monotonic
        // even when the two race. There is no separate committed cursor: the
        // consumer gates readiness on each slot's kReady state + sequence
        // number directly, so half-written slots are invisible without an
        // ordered commit bitmap (which suffered a cross-era ABA on wrap).
        alignas(kCacheLineSize) std::atomic<uint64_t> consumer_cursor{0};
        unsigned char pad2[kCacheLineSize - 8] = {};

        // -- Line 3: reserved for future use --------------------------------
        alignas(kCacheLineSize) std::atomic<uint64_t> reserved2{0};
        unsigned char pad3[kCacheLineSize - 8] = {};
    };

    // IndexSlot requires 64-byte alignment, so the slot array must start on
    // a 64-byte boundary. The control block is an integral number of cache
    // lines, so the slot array begins immediately after it, already aligned.
    static constexpr uint64_t AlignUp64(uint64_t n) {
        return (n + kCacheLineSize - 1) & ~(kCacheLineSize - 1);
    }

    // Byte offset of the IndexSlot array from the control-block base.
    static constexpr uint64_t SlotsOffset(uint64_t capacity) {
        (void)capacity;  // Layout no longer depends on capacity.
        return AlignUp64(sizeof(ControlBlock));
    }

    // Byte offset of the MpscReservationMeta sidecar array. capacity *
    // sizeof(IndexSlot) is always a multiple of 64 (128-byte slots), so the
    // sidecar stays 64-aligned without further padding.
    static constexpr uint64_t MetasOffset(uint64_t capacity) {
        return SlotsOffset(capacity) + capacity * sizeof(IndexSlot);
    }

    // Total bytes the channel occupies in shared memory: control block
    // (padded to a cache line) + IndexSlot array + sidecar array.
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
        control->consumer_cursor.store(0, std::memory_order_relaxed);
        control->reserved2.store(0, std::memory_order_relaxed);

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
            // Vyukov: slot i is initially free for cursor position i.
            s.turn.store(i, std::memory_order_relaxed);
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

    // Polls the next ready message. Readiness is gated on the per-slot
    // Vyukov turn: slot[cons % capacity].turn == cons + 1 means the producer
    // committed this exact sequence; anything else means empty or wedged.
    // Returns:
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
                std::memory_order_acquire);
            const uint64_t reserved = control_->reservation_cursor.load(
                std::memory_order_acquire);
            if (cons == reserved) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "MPSC queue empty");
            }
            IndexSlot* slot = &slots_[cons & mask_];
            // Acquire: pairs with the producer's release commit (turn = cons
            // + 1) so the fields we snapshot below are fully visible.
            const uint64_t turn = slot->turn.load(std::memory_order_acquire);
            const uint32_t state =
                slot->state.load(std::memory_order_acquire);

            if (state == static_cast<uint32_t>(SlotState::kAborted)) {
                // Tombstone: retire (advance turn past this slot for the next
                // era) and skip. The producer that aborted never bumped turn,
                // so we do it here on the slot's behalf.
                slot->turn.store(cons + capacity_, std::memory_order_release);
                AdvanceConsumerPast(cons);
                continue;
            }
            if (turn != cons + 1 ||
                state != static_cast<uint32_t>(SlotState::kReady)) {
                // Slot is reserved but not yet committed (turn still == cons,
                // state kReserved/kWriting) or already reclaimed: either the
                // producer is still working or it stalled/crashed. This is
                // the ordered-prefix wedge: the consumer may not skip ahead,
                // so report kWouldBlock and let the caller run
                // AbortOrphanedReservations() if it persists.
                return Status::Error(StatusCode::kWouldBlock,
                                     "MPSC slot not yet ready (wedged behind "
                                     "an unresolved reservation)");
            }
            // turn == cons + 1 && state == kReady: the producer's release
            // commit published every field covered by this acquire, so the
            // snapshot below is safe. The sequence check is a belt-and-suspend
            // guard; turn already discriminates eras exactly.
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

    // Scans the reserved-but-unconsumed window
    // [consumer_cursor, reservation_cursor) and stamps ABORTED tombstones
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
        const uint64_t consumed =
            control_->consumer_cursor.load(std::memory_order_acquire);
        const uint64_t reserved =
            control_->reservation_cursor.load(std::memory_order_acquire);
        uint64_t aborted = 0;
        for (uint64_t seq = consumed; seq < reserved; ++seq) {
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
            ++aborted;
        }
        return aborted;
    }

    // -----------------------------------------------------------------------
    // Observers
    // -----------------------------------------------------------------------

    // Approximate (not linearizable): the queue has no outstanding
    // reservations once the consumer has caught up to the reservation
    // cursor. A wedged-but-nonempty queue (a stalled reservation ahead of the
    // consumer) reports not-empty here even though Poll() would block.
    bool IsEmpty() const noexcept {
        const uint64_t reserved =
            control_->reservation_cursor.load(std::memory_order_acquire);
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        return reserved == cons;
    }

    bool IsFull() const noexcept {
        return IsFullAt(
            control_->reservation_cursor.load(std::memory_order_acquire));
    }

    // Approximate upper bound on messages not yet consumed (includes any
    // wedged unfinished reservations). Not linearizable.
    uint64_t Size() const noexcept {
        const uint64_t reserved =
            control_->reservation_cursor.load(std::memory_order_acquire);
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        return reserved - cons;
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
    // safe victim: its producer already finished, so retiring it cannot skip
    // an unfinished sequence. Retiring a kReserved/kWriting slot would
    // advance the consumer past an unfinished sequence and corrupt the
    // ordered prefix, so those are left for recovery instead. A consumer
    // holding a Borrow of the victim keeps a valid snapshot; its late Ack
    // reports kNotFound. The payload is NOT reclaimed here (only after no
    // borrows remain).
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
        oldest->turn.store(cons + capacity_, std::memory_order_release);
        AdvanceConsumerPast(cons);
    }

    // Vyukov per-slot-turn probe-then-claim. Probes slot[res % capacity].turn
    // for the current cursor position `res`; only when turn == res (the slot
    // is free for this exact era) does it CAS the cursor forward, so a
    // handed-out sequence always owns its slot (no phantom sequence).
    //
    // `turn` is numerically unique across eras (not modulo capacity), so a
    // producer wrapping the ring that finds a slot still occupied by a
    // previous era observes turn < res and reports full/wedged — it can never
    // mistake the slot for free. This is what makes the cursor CAS and the
    // slot occupation race-free.
    //
    // Returns:
    //   an active Reservation : slot claimed and in state kWriting.
    //   kResourceExhausted    : turn < res because the previous-era message
    //                         is unconsumed (state kReady) — the queue is
    //                         genuinely full; kDropOldest may forcibly retire.
    //   kWouldBlock           : turn < res because a previous-era holder is
    //                         unfinished (kReserved/kWriting) or an unconsumed
    //                         tombstone (kAborted); run recovery.
    Result<Reservation> TryReserveAfterSpaceCheck(
        const ProducerIdentity& owner) noexcept {
        for (;;) {
            const uint64_t res = control_->reservation_cursor.load(
                std::memory_order_acquire);
            const uint64_t phys = res & mask_;
            IndexSlot* slot = &slots_[phys];
            // Acquire: pairs with the consumer's release retire (turn = pos +
            // capacity) so we observe the fully-retired slot, and with the
            // previous era's release commit (turn = pos_prev + 1) so we never
            // act on a half-published slot.
            const uint64_t turn = slot->turn.load(std::memory_order_acquire);

            if (turn > res) {
                // Another producer already claimed `res` (or a later sequence
                // mapping to this physical slot); the cursor moved on. Re-read
                // the cursor and retry.
                continue;
            }
            if (turn < res) {
                // The slot is still occupied by a previous era. Distinguish
                // genuine backpressure from a wedge using the business state
                // (which the previous era published with release before
                // bumping turn, so an acquire read is consistent).
                const uint32_t state =
                    slot->state.load(std::memory_order_acquire);
                if (state == static_cast<uint32_t>(SlotState::kReady)) {
                    // Genuine backpressure: previous-era message unconsumed.
                    return Status::Error(StatusCode::kResourceExhausted,
                                         "MPSC queue full");
                }
                if (state == static_cast<uint32_t>(SlotState::kReserved) ||
                    state == static_cast<uint32_t>(SlotState::kWriting)) {
                    // Previous-era holder reserved but never finished. Never
                    // stamp a foreign slot from here: recovery owns that
                    // decision (it checks owner liveness + lease).
                    return Status::Error(
                        StatusCode::kWouldBlock,
                        "MPSC slot held by an unfinished reservation; recovery "
                        "required");
                }
                // kAborted tombstone not yet retired by the consumer; retry
                // once the consumer cursor advances past it.
                return Status::Error(StatusCode::kWouldBlock,
                                     "MPSC slot holds an unconsumed tombstone");
            }

            // turn == res: the slot is free for this era. Claim the sequence
            // by advancing the cursor; only on success do we own the slot.
            uint64_t expected = res;
            if (!control_->reservation_cursor.compare_exchange_weak(
                    expected, res + 1, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                continue;  // Another producer claimed res; re-probe.
            }

            // We own sequence `res` and slot `phys` for this era (the cursor
            // CAS serialized us against every other producer for this exact
            // cursor value, and the turn probe guaranteed the slot was free).
            // Publish the claim with RELEASE stores: the kWriting store must
            // be visible to a wrapping producer's acquire probe so it reads
            // turn < its pos and reports full/wedged instead of reusing the
            // slot. sequence_num is published by the same release.
            MpscReservationMeta& meta = metas_[phys];
            meta.owner_process_id = owner.owner.process_id;
            meta.owner_process_epoch = owner.owner.process_epoch;
            meta.owner_publisher_id = owner.publisher_id;
            meta.reservation_timestamp_ns = MonotonicNowNs();
            slot->sequence_num.store(res, std::memory_order_relaxed);
            slot->state.store(static_cast<uint32_t>(SlotState::kWriting),
                              std::memory_order_release);
            return Reservation(this, slot, res);
        }
    }

    // Seals the CRC and publishes READY. Two release-stores form the
    // publication point: `state` (business state) then `turn` (protocol
    // sequence). The consumer's Poll acquire-loads turn == cons + 1, which
    // orders it after every field the producer wrote. No separate commit
    // bitmap is needed.
    Status CommitSlot(IndexSlot* slot, uint64_t sequence) noexcept {
        SealIndexSlotImmutableCrc(*slot);
        slot->state.store(static_cast<uint32_t>(SlotState::kReady),
                          std::memory_order_release);
        // Vyukov: mark the slot ready for the consumer reading `sequence`.
        slot->turn.store(sequence + 1, std::memory_order_release);
        return Status::Ok();
    }

    // Stamps an ABORTED tombstone. The consumer retires and skips it
    // transparently in Poll() (it advances turn past the slot there).
    Status AbortSlot(IndexSlot* slot, uint64_t sequence) noexcept {
        (void)sequence;
        uint32_t expected = static_cast<uint32_t>(SlotState::kWriting);
        slot->state.compare_exchange_strong(
            expected, static_cast<uint32_t>(SlotState::kAborted),
            std::memory_order_acq_rel, std::memory_order_relaxed);
        return Status::Ok();
    }

    // Advance the consumer cursor past `cons`. The consumer is the only
    // logical writer of this cursor in the steady state (kDropOldest is the
    // sole producer-side exception and uses CAS itself), so a plain release
    // store is correct and keeps the read position and recovery lower bound
    // in lock-step.
    void AdvanceConsumerPast(uint64_t cons) noexcept {
        control_->consumer_cursor.store(cons + 1, std::memory_order_release);
    }

    // Retire slot `cons` and advance the consumer cursor. The turn release
    // marks the physical slot free for the NEXT era (cons + capacity): a
    // producer probing that position observes turn == its pos and may claim.
    // This pairs with the producer's acquire probe.
    void RetireAndAdvance(IndexSlot* slot, uint64_t cons) noexcept {
        slot->turn.store(cons + capacity_, std::memory_order_release);
        AdvanceConsumerPast(cons);
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
