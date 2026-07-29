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

#ifndef MINO_SHM_CHANNEL_SPSC_CHANNEL_H_
#define MINO_SHM_CHANNEL_SPSC_CHANNEL_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/queue_full_policy.h"

namespace mino {

namespace detail {

// Architecture-appropriate spin-wait pause. Keeps blocking reservations
// from hammering the cache line while the consumer catches up.
inline void SpinPause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#endif
}

}  // namespace detail

// ---------------------------------------------------------------------------
// SPSC Channel (design doc 9.3, 9.4, 9.8)
// ---------------------------------------------------------------------------
//
// SpscChannel is the single-producer / single-consumer channel. It is the
// first concrete channel semantics built on top of the IndexSlot ABI and is
// the benchmark baseline for the whole data plane (design doc 9.4).
//
// Layout in shared memory (all offsets from `shm_base`):
//
//   +---------------------------+ 0
//   | ControlBlock              | 3 cache lines:
//   |   line 0: magic / version / capacity
//   |   line 1: producer_cursor (only the producer thread writes)
//   |   line 2: consumer_cursor (only the consumer thread writes)
//   +---------------------------+ 192
//   | IndexSlot[capacity]       | 128B each, cache-line aligned
//   +---------------------------+
//
// Concurrency contract (design doc 9.4):
//   - Exactly one producer thread/process and one consumer thread/process.
//   - The two cursors live on separate cache lines to avoid false sharing.
//   - Capacity is a power of two; the physical slot for logical sequence s
//     is slots[s % capacity]. The sequence_num stored in a slot is what
//     distinguishes the current occupant from a stale (wrapped) one, so a
//     full wrap never looks like an empty slot and vice versa (INV-01).
//   - Full  : producer_cursor - consumer_cursor >= capacity
//   - Empty : producer_cursor == consumer_cursor
//   - The logical sequence IS the producer position: slot.sequence_num is
//     assigned at Reserve time from producer_cursor, which makes the
//     consumer's ABA check (sequence_num == consumer_cursor) exact and lets
//     a late Ack detect that its message was dropped (see below).
//
// Publication protocol (design doc 9.3):
//   Reserve: claim slot at producer_cursor, stamp sequence_num, transition
//            to WRITING (relaxed; single producer, no CAS needed). The
//            intermediate RESERVED state of the generic protocol (9.3) is
//            unnecessary here: it exists to arbitrate between competing
//            producers, and SPSC has exactly one.
//   Fill   : producer writes all immutable metadata + payload handle.
//   Commit : immutable_metadata_crc is sealed; state.store(kReady, release);
//            producer_cursor.fetch_add(1, release) publishes the slot.
//   Abort  : state.store(kAborted, relaxed); producer_cursor.fetch_add(1,
//            release). The consumer skips the tombstone and retires it.
//
// Consumption protocol:
//   Poll   : consumer observes slot at consumer_cursor; if state == kReady
//            (acquire) and sequence_num == consumer_cursor, it copies an
//            IndexSlotSnapshot out of the slot and returns it as a Borrow.
//   Ack    : if the Borrow's sequence_num still equals consumer_cursor, the
//            slot is retired and the cursor advances. If it does not, the
//            message was overtaken by kDropOldest: Ack reports kNotFound
//            and leaves the queue untouched, so a late Ack can never retire
//            a recycled slot or advance the cursor over a live message.
//
// Borrow lifetime vs. kDropOldest (design doc 9.8): a Borrow owns a snapshot,
// not the slot, so the producer overwriting the slot never tears the
// consumer's view of the header. The payload it points to is NOT reclaimed
// with the slot (9.8: reuse only after no borrows remain); before D2-11
// (ShmSharedPtr / ADR-0013 pin) lands, a kDropOldest topic must either
// tolerate the payload being recycled while a slow consumer still holds its
// handle, or keep the consumer fast enough that it never lags a full ring
// behind. The snapshot makes the header side of this guarantee exact today.
//
// Corruption handling: Poll treats a sequence mismatch or an immutable-CRC
// mismatch the same way — the slot is retired and skipped (so one bad slot
// cannot wedge the queue or livelock the consumer) and kCorruption is
// returned. Callers should count these events via telemetry (ADR-0009); a
// persistent stream of them means the segment needs recovery, not polling.
//
// QueueFullPolicy (design doc 9.8) is applied in Reserve():
//   kFail       -> kResourceExhausted immediately.
//   kBlock      -> spin (with pause) until space frees up. The channel layer
//                  deliberately offers no timeout: bounded waiting is a
//                  Runtime-level concern (D2-09 Publisher API) layered on
//                  top of this primitive. A producer blocked here burns CPU
//                  until the consumer advances — use kFail/kDrop* when the
//                  consumer may die.
//   kDropNewest -> report kDegraded; the incoming message is dropped. Note
//                  kDegraded here is a *policy outcome*, not a failure: the
//                  publish pipeline is expected to continue.
//   kDropOldest -> forcibly retire the oldest unconsumed slot (advancing the
//                  consumer cursor) and take its place. See the Borrow note
//                  above for the exact lifetime semantics.
//   kSample     -> admit with probability 1/sample_rate when full (a
//                  deterministic position-based counter, no RNG in SHM);
//                  a rejected message reports kDegraded.
//
// The channel never reclaims payload memory itself: payload lifetime is
// owned by the Central Slab allocator and the borrow/lease system (design
// doc 10, 11, 12).
class SpscChannel {
public:
    // The channel requires lock-free 64-bit atomics to be implementable.
    static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
                  "SpscChannel requires lock-free 64-bit atomics");

    static constexpr uint64_t kCacheLineSize = 64;
    static constexpr uint64_t kMagic = 0x4D49'4E4F'5350'5343ULL;  // "MINOSPSC"
    static constexpr uint32_t kLayoutVersion = 1;

    // -----------------------------------------------------------------------
    // Control block
    // -----------------------------------------------------------------------
    //
    // Shared across processes (and potentially compiler configurations), so
    // the layout is pinned statically. Three cache lines: identity/capacity
    // on line 0, producer cursor on line 1, consumer cursor on line 2.
    struct alignas(kCacheLineSize) ControlBlock {
        // -- Line 0: identity + immutable configuration ---------------------
        std::atomic<uint64_t> magic{0};
        std::atomic<uint32_t> layout_version{0};
        uint32_t reserved0 = 0;
        uint64_t capacity = 0;  // power of two, >= 2
        // Reserved for future per-channel counters. Must be 0. (The logical
        // message sequence is the producer cursor itself; see Reserve.)
        uint64_t reserved1 = 0;
        unsigned char pad0[kCacheLineSize - 8 - 4 - 4 - 8 - 8] = {};

        // -- Line 1: producer cursor (single writer: the producer) ----------
        alignas(kCacheLineSize) std::atomic<uint64_t> producer_cursor{0};
        unsigned char pad1[kCacheLineSize - 8] = {};

        // -- Line 2: consumer cursor (single writer: the consumer) ----------
        alignas(kCacheLineSize) std::atomic<uint64_t> consumer_cursor{0};
        unsigned char pad2[kCacheLineSize - 8] = {};
    };

    static_assert(sizeof(ControlBlock) == 3 * kCacheLineSize,
                  "ControlBlock must occupy exactly three cache lines");
    static_assert(alignof(ControlBlock) == kCacheLineSize);
    static_assert(std::is_standard_layout_v<ControlBlock>);
    static_assert(offsetof(ControlBlock, producer_cursor) == kCacheLineSize,
                  "producer cursor must start its own cache line");
    static_assert(offsetof(ControlBlock, consumer_cursor) == 2 * kCacheLineSize,
                  "consumer cursor must start its own cache line");

    // SpscChannel is a non-owning, trivially copyable view into shared
    // memory. Nothing is destroyed when the view goes away; a process
    // exiting does not "destruct" the channel (design doc 9.9 lifecycle).
    SpscChannel() noexcept = default;

    // -----------------------------------------------------------------------
    // Init / Attach
    // -----------------------------------------------------------------------

    // Total bytes the channel occupies in shared memory.
    static constexpr uint64_t RequiredSize(uint64_t capacity) {
        return sizeof(ControlBlock) + capacity * sizeof(IndexSlot);
    }

    // Initializes a channel of `capacity` slots in place at `shm_base`.
    // `capacity` must be a power of two and >= 2. The caller must size the
    // region with RequiredSize() and must not touch the memory concurrently
    // while Init runs. The base must be 64-byte aligned.
    static Result<SpscChannel> Init(void* shm_base, uint64_t capacity) {
        if (shm_base == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must not be null");
        }
        if (reinterpret_cast<uintptr_t>(shm_base) % alignof(ControlBlock) !=
            0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must be 64-byte aligned");
        }
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "capacity must be a power of two and >= 2");
        }
        if (capacity > (uint64_t{1} << 32)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "capacity exceeds the supported maximum (2^32)");
        }

        auto* control = static_cast<ControlBlock*>(shm_base);
        IndexSlot* slots = SlotsOf(shm_base);

        // Initialize every non-magic field before anything can observe the
        // control block; magic below is the single publication point.
        control->layout_version.store(kLayoutVersion,
                                      std::memory_order_relaxed);
        control->reserved0 = 0;
        control->capacity = capacity;
        control->reserved1 = 0;
        control->producer_cursor.store(0, std::memory_order_relaxed);
        control->consumer_cursor.store(0, std::memory_order_relaxed);

        for (uint64_t i = 0; i < capacity; ++i) {
            // Slots start FREE with sequence_num equal to their logical
            // position. This is what lets a stale occupant from a previous
            // wrap be told apart from the current one (INV-01). IndexSlot
            // is non-assignable (atomic member), so initialize field by
            // field rather than by aggregate assignment.
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

        // Publish: release so every plain/relaxed write above is visible to
        // any observer that acquires the magic.
        control->magic.store(kMagic, std::memory_order_release);
        return SpscChannel(control, slots, capacity);
    }

    // Attaches to an already-initialized channel at `shm_base`, validating
    // the magic and layout version. The capacity is read from the control
    // block (single source of truth, same style as MpmcRing::Attach); any
    // validation failure refuses the attach. Callers that want a defensive
    // check compare `channel.capacity()` against their expectation.
    static Result<SpscChannel> Attach(void* shm_base) {
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
                                 "SPSC control block magic mismatch");
        }
        if (control->layout_version.load(std::memory_order_acquire) !=
            kLayoutVersion) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "SPSC layout version mismatch");
        }
        const uint64_t capacity = control->capacity;
        if (capacity < 2 || (capacity & (capacity - 1)) != 0 ||
            capacity > (uint64_t{1} << 32)) {
            return Status::Error(StatusCode::kCorruption,
                                 "SPSC control block capacity is invalid");
        }
        return SpscChannel(control, SlotsOf(shm_base), capacity);
    }

    // -----------------------------------------------------------------------
    // Producer: Reserve / Fill / Commit / Abort
    // -----------------------------------------------------------------------

    // A Reservation is the producer's exclusive write window into one slot.
    // It is move-only and must be either Commit()ed or Abort()ed; destroying
    // a live Reservation without committing aborts the slot so it can never
    // wedge the queue (the consumer skips aborted slots).
    class Reservation {
    public:
        Reservation() noexcept = default;
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;

        Reservation(Reservation&& other) noexcept
            : channel_(other.channel_), slot_(other.slot_), active_(other.active_) {
            other.channel_ = nullptr;
            other.slot_ = nullptr;
            other.active_ = false;
        }
        Reservation& operator=(Reservation&& other) noexcept {
            if (this != &other) {
                AbortIfActive();
                channel_ = other.channel_;
                slot_ = other.slot_;
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

        // Seals the immutable CRC and publishes the slot to the consumer.
        // After Commit the Reservation is empty and slot() must not be used.
        Status Commit() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "reservation is not active");
            }
            // Mark inactive first so a failure cannot double-publish and the
            // destructor will not stamp a tombstone over a committed slot.
            active_ = false;
            SpscChannel* ch = channel_;
            channel_ = nullptr;
            IndexSlot* slot = slot_;
            slot_ = nullptr;
            return ch->CommitSlot(slot);
        }

        // Aborts the slot: stamps a tombstone the consumer will skip and
        // retire. After Abort the Reservation is empty.
        Status Abort() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "reservation is not active");
            }
            active_ = false;
            SpscChannel* ch = channel_;
            channel_ = nullptr;
            IndexSlot* slot = slot_;
            slot_ = nullptr;
            return ch->AbortSlot(slot);
        }

    private:
        friend class SpscChannel;
        Reservation(SpscChannel* channel, IndexSlot* slot) noexcept
            : channel_(channel), slot_(slot), active_(true) {}

        void AbortIfActive() noexcept {
            if (active_) {
                // Best-effort: a destroyed live reservation must not wedge
                // the queue. The tombstone is stamped unconditionally.
                channel_->AbortSlot(slot_).ok();
            }
        }

        SpscChannel* channel_ = nullptr;
        IndexSlot* slot_ = nullptr;
        bool active_ = false;
    };

    // Attempts to reserve the next slot for writing, applying `policy` if
    // the queue is full. On success returns an active Reservation whose
    // slot() is in state kWriting, owned exclusively by the caller.
    //
    // `sample_rate` is only consulted for kSample: when full, a message is
    // admitted with probability 1 / sample_rate (sample_rate >= 1). It is
    // ignored for all other policies.
    //
    // Errors:
    //   kResourceExhausted : kFail and the queue is full.
    //   kDegraded          : kDropNewest / kSample dropped the incoming
    //                        message (a policy outcome, not a failure).
    Result<Reservation> Reserve(
        QueueFullPolicy policy = QueueFullPolicy::kFail,
        uint32_t sample_rate = 1) noexcept {
        // Single producer: no CAS needed to claim our own cursor slot.
        const uint64_t prod = control_->producer_cursor.load(
            std::memory_order_relaxed);
        const uint64_t cons = control_->consumer_cursor.load(
            std::memory_order_acquire);

        if (prod - cons >= capacity_) {
            switch (policy) {
                case QueueFullPolicy::kFail:
                    return Status::Error(StatusCode::kResourceExhausted,
                                         "SPSC queue full");
                case QueueFullPolicy::kBlock: {
                    // Unbounded spin by design (see class docs): bounded
                    // waiting is layered on top by the Runtime publisher.
                    while (true) {
                        const uint64_t c = control_->consumer_cursor.load(
                            std::memory_order_acquire);
                        if (prod - c < capacity_) {
                            break;
                        }
                        detail::SpinPause();
                    }
                    break;
                }
                case QueueFullPolicy::kDropNewest:
                    return Status::Error(
                        StatusCode::kDegraded,
                        "SPSC queue full: newest message dropped");
                case QueueFullPolicy::kDropOldest: {
                    // Forcibly advance the consumer cursor past the oldest
                    // unconsumed slot. The slot header is recycled below; a
                    // consumer still holding a Borrow of it keeps a valid
                    // snapshot and its late Ack reports kNotFound. The
                    // payload is NOT reclaimed here (design doc 9.8: only
                    // after no borrows remain).
                    IndexSlot* oldest = &slots_[cons & mask_];
                    oldest->state.store(
                        static_cast<uint32_t>(SlotState::kRetired),
                        std::memory_order_relaxed);
                    control_->consumer_cursor.store(
                        cons + 1, std::memory_order_release);
                    break;
                }
                case QueueFullPolicy::kSample: {
                    const uint32_t rate = sample_rate == 0 ? 1 : sample_rate;
                    // Deterministic position-based sampling keeps the
                    // admitted subset reproducible and needs no RNG state in
                    // SHM. The producer cursor is the per-message counter.
                    if ((prod % rate) != 0) {
                        return Status::Error(
                            StatusCode::kDegraded,
                            "SPSC queue full: message sampled out");
                    }
                    // Admitted: wait for space like kBlock.
                    while (true) {
                        const uint64_t c = control_->consumer_cursor.load(
                            std::memory_order_acquire);
                        if (prod - c < capacity_) {
                            break;
                        }
                        detail::SpinPause();
                    }
                    break;
                }
            }
        }

        IndexSlot* slot = &slots_[prod & mask_];
        // The slot must have been retired/freed by the consumer before we
        // can write it. Because the queue was not full, the consumer cursor
        // has already advanced past this physical slot's previous occupant.
        // Assign the logical sequence now: it IS the producer position, so
        // the consumer's ABA check (slot.sequence_num == consumer_cursor) is
        // exact across wraps (INV-01). Single producer: a relaxed store is
        // enough; the kReady release-store publishes it to the consumer.
        slot->sequence_num.store(prod, std::memory_order_relaxed);
        slot->state.store(static_cast<uint32_t>(SlotState::kWriting),
                          std::memory_order_relaxed);
        return Reservation(this, slot);
    }

    // Non-blocking reservation attempt: kWouldBlock if the queue is full.
    // Mirrors the MpmcRing TryEnqueue surface.
    Result<Reservation> TryReserve() noexcept {
        const uint64_t prod = control_->producer_cursor.load(
            std::memory_order_relaxed);
        const uint64_t cons = control_->consumer_cursor.load(
            std::memory_order_acquire);
        if (prod - cons >= capacity_) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "SPSC queue full");
        }
        IndexSlot* slot = &slots_[prod & mask_];
        slot->sequence_num.store(prod, std::memory_order_relaxed);
        slot->state.store(static_cast<uint32_t>(SlotState::kWriting),
                          std::memory_order_relaxed);
        return Reservation(this, slot);
    }

    // -----------------------------------------------------------------------
    // Consumer: Poll / Ack
    // -----------------------------------------------------------------------

    // A Borrow is the consumer's read window into one published message. It
    // owns an IndexSlotSnapshot copied at Poll time, so it is fully
    // decoupled from later overwrites of the slot (kDropOldest). Move-only;
    // destroying a live Borrow without Ack() leaves the slot READY (not
    // consumed), so a slow/failing consumer simply doesn't advance the
    // queue.
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

        // Marks the slot retired and advances the consumer cursor, freeing
        // the slot for the producer. If this message was overtaken by
        // kDropOldest while borrowed, reports kNotFound and leaves the queue
        // untouched: a late Ack can never retire a recycled slot. After Ack
        // (either outcome) the Borrow is empty.
        Status Ack() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "borrow is not active");
            }
            active_ = false;
            SpscChannel* ch = channel_;
            channel_ = nullptr;
            return ch->AckSlot(snapshot_.sequence_num);
        }

    private:
        friend class SpscChannel;
        Borrow(SpscChannel* channel, const IndexSlotSnapshot& snapshot) noexcept
            : channel_(channel), snapshot_(snapshot), active_(true) {}

        SpscChannel* channel_ = nullptr;
        IndexSlotSnapshot snapshot_;
        bool active_ = false;
    };

    // Polls the next ready message. Returns:
    //   an active Borrow : a message is available (state kReady, sequence
    //                      matches, CRC verified).
    //   kWouldBlock      : the queue is empty.
    //   kCorruption      : a slot failed its sequence or CRC check; the bad
    //                      slot was retired and skipped so the queue keeps
    //                      making progress. Persistent corruption means the
    //                      segment needs recovery, not polling.
    //
    // Aborted tombstones are transparently retired and skipped; Poll keeps
    // advancing until it finds a READY slot or the queue is drained.
    Result<Borrow> Poll() noexcept {
        while (true) {
            const uint64_t cons = control_->consumer_cursor.load(
                std::memory_order_relaxed);
            const uint64_t prod = control_->producer_cursor.load(
                std::memory_order_acquire);
            if (cons == prod) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "SPSC queue empty");
            }
            IndexSlot* slot = &slots_[cons & mask_];
            const uint32_t state =
                slot->state.load(std::memory_order_acquire);

            if (state == static_cast<uint32_t>(SlotState::kAborted)) {
                // Tombstone: retire and skip, advancing past it.
                slot->state.store(static_cast<uint32_t>(SlotState::kRetired),
                                  std::memory_order_relaxed);
                control_->consumer_cursor.store(cons + 1,
                                                std::memory_order_release);
                continue;
            }
            if (state != static_cast<uint32_t>(SlotState::kReady)) {
                // Not yet published (e.g. the producer is between Reserve
                // and Commit with the cursor not yet advanced — which the
                // empty check above normally hides; this is a defensive
                // cover for torn views). Treat as empty.
                return Status::Error(StatusCode::kWouldBlock,
                                     "SPSC slot not yet ready");
            }
            // ABA guard: the slot's logical sequence must match our cursor.
            // A mismatch, like a CRC failure, is corruption: skip the slot
            // (never livelock on it) and report. The acquire on `state` above
            // already published the sequence, so a relaxed load suffices.
            if (slot->sequence_num.load(std::memory_order_relaxed) != cons) {
                RetireAndAdvance(slot, cons);
                return Status::Error(
                    StatusCode::kCorruption,
                    "SPSC slot sequence mismatch (skipped)");
            }
            // Copy the header out, then verify the CRC on our own copy. The
            // snapshot decouples all further use from any later overwrite.
            IndexSlotSnapshot snapshot = SnapshotIndexSlot(*slot);
            if (!VerifySnapshotCrc(snapshot)) {
                RetireAndAdvance(slot, cons);
                return Status::Error(
                    StatusCode::kCorruption,
                    "SPSC slot immutable CRC mismatch (skipped)");
            }
            return Borrow(this, snapshot);
        }
    }

    // -----------------------------------------------------------------------
    // Observers
    // -----------------------------------------------------------------------

    bool IsEmpty() const noexcept {
        const uint64_t prod =
            control_->producer_cursor.load(std::memory_order_acquire);
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        return prod == cons;
    }

    bool IsFull() const noexcept {
        const uint64_t prod =
            control_->producer_cursor.load(std::memory_order_acquire);
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        return prod - cons >= capacity_;
    }

    // Number of messages currently visible to the consumer.
    uint64_t Size() const noexcept {
        const uint64_t prod =
            control_->producer_cursor.load(std::memory_order_acquire);
        const uint64_t cons =
            control_->consumer_cursor.load(std::memory_order_acquire);
        return prod - cons;
    }

    uint64_t capacity() const noexcept { return capacity_; }

private:
    SpscChannel(ControlBlock* control, IndexSlot* slots,
                uint64_t capacity) noexcept
        : control_(control),
          slots_(slots),
          capacity_(capacity),
          mask_(capacity - 1) {}

    static IndexSlot* SlotsOf(void* shm_base) noexcept {
        return reinterpret_cast<IndexSlot*>(
            static_cast<unsigned char*>(shm_base) + sizeof(ControlBlock));
    }

    // Seals the CRC and publishes the slot. The logical sequence was
    // assigned at Reserve time (it equals the producer position, making the
    // consumer's ABA check exact across wraps) and is sealed into the CRC
    // here.
    Status CommitSlot(IndexSlot* slot) noexcept {
        SealIndexSlotImmutableCrc(*slot);
        // Publish payload + metadata before the state transition.
        slot->state.store(static_cast<uint32_t>(SlotState::kReady),
                          std::memory_order_release);
        // Advance the producer cursor: this is what makes the slot visible
        // to the consumer's Poll.
        control_->producer_cursor.fetch_add(1, std::memory_order_release);
        return Status::Ok();
    }

    // Stamps an ABORTED tombstone and advances the producer cursor.
    Status AbortSlot(IndexSlot* slot) noexcept {
        slot->state.store(static_cast<uint32_t>(SlotState::kAborted),
                          std::memory_order_relaxed);
        control_->producer_cursor.fetch_add(1, std::memory_order_release);
        return Status::Ok();
    }

    // Retires the slot at `cons` and advances the consumer cursor past it.
    void RetireAndAdvance(IndexSlot* slot, uint64_t cons) noexcept {
        slot->state.store(static_cast<uint32_t>(SlotState::kRetired),
                          std::memory_order_relaxed);
        control_->consumer_cursor.store(cons + 1, std::memory_order_release);
    }

    // Consumer-side Ack. Retires the slot and advances the cursor only if
    // `sequence` is still the head of the queue; otherwise the message was
    // overtaken by kDropOldest and there is nothing safe to do.
    Status AckSlot(uint64_t sequence) noexcept {
        const uint64_t cons = control_->consumer_cursor.load(
            std::memory_order_relaxed);
        if (sequence != cons) {
            return Status::Error(
                StatusCode::kNotFound,
                "message was dropped (overtaken by kDropOldest)");
        }
        RetireAndAdvance(&slots_[cons & mask_], cons);
        return Status::Ok();
    }

    ControlBlock* control_ = nullptr;
    IndexSlot* slots_ = nullptr;
    uint64_t capacity_ = 0;
    uint64_t mask_ = 0;
};

static_assert(std::is_trivially_copyable_v<SpscChannel>,
              "SpscChannel must be a trivially copyable view");

}  // namespace mino

#endif  // MINO_SHM_CHANNEL_SPSC_CHANNEL_H_
