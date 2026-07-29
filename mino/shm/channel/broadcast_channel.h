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

// D2-05: Broadcast channel (design doc 9.6).
//
// Single publisher / N subscriber fan-out over shared memory. Every active
// subscriber receives every published message exactly once; each subscriber
// advances its own cursor and ACKs individually through the per-slot
// AckBitmap sidecar (design doc 9.2).
//
// Publication protocol (design doc 9.6):
//   Reserve : single publisher, no Vyukov turn arbitration (turn is
//             initialized for layout consistency but never read). Full
//             iff publisher_cursor - MinActiveCursor() >= capacity; with no
//             active subscriber the channel is never full.
//   Commit  : the per-slot ack_bitmap is stamped with the subscriber-set
//             snapshot taken at Reserve time (version + membership), the
//             immutable CRC is sealed, state goes kReady (release), and
//             finally publisher_cursor advances (release).
//   Abort   : state goes kAborted, the ack_bitmap is cleared (no delivery
//             obligation), and publisher_cursor advances. Subscribers skip
//             tombstones transparently.
//
// Consumption protocol: Poll(sub) validates the subscriber registration
// (active + generation match), then walks from the subscriber's own cursor:
// kAborted slots are skipped, sequence/CRC mismatches report kCorruption
// and are skipped, otherwise a snapshot Borrow is returned. Ack(seq) clears
// the subscriber's bit in the slot's ack_bitmap and advances the
// subscriber cursor, then runs CollectGarbage() to retire fully-acked slots.
//
// BroadcastChannel is header-only: a non-owning view over shared memory
// (design doc 9.9 lifecycle: a process exiting does not "destruct" the
// channel).

#ifndef MINO_SHM_CHANNEL_BROADCAST_CHANNEL_H_
#define MINO_SHM_CHANNEL_BROADCAST_CHANNEL_H_

#include <atomic>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/queue_full_policy.h"
#include "mino/shm/channel/spsc_channel.h"  // detail::SpinPause

namespace mino {

class BroadcastChannel {
public:
    static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
                  "BroadcastChannel requires lock-free 64-bit atomics");

    static constexpr uint64_t kCacheLineSize = 64;
    static constexpr uint64_t kMagic = 0x4D49'4E4F'4252'4443ULL;  // "MINOBRDC"
    static constexpr uint32_t kLayoutVersion = 1;
    static constexpr uint32_t kMaxSubscribers = kBroadcastMaxSubscribers;  // 64

    // -----------------------------------------------------------------------
    // Control block
    // -----------------------------------------------------------------------
    //
    // Line 0: identity + immutable configuration.
    // Line 1: publisher_cursor (single writer: the publisher).
    // Line 2: current_membership + set_version (multi-writer: any process
    //         may register/unregister a subscriber; every writer uses CAS,
    //         and the pair is read seqlock-style by the publisher).
    struct alignas(kCacheLineSize) ControlBlock {
        // -- Line 0: identity + immutable configuration ---------------------
        std::atomic<uint64_t> magic{0};
        std::atomic<uint32_t> layout_version{0};
        uint32_t reserved0 = 0;
        uint64_t capacity = 0;  // power of two, >= 2
        uint64_t reserved1 = 0;
        unsigned char pad0[kCacheLineSize - 8 - 4 - 4 - 8 - 8] = {};

        // -- Line 1: publisher cursor (single writer: the publisher) --------
        alignas(kCacheLineSize) std::atomic<uint64_t> publisher_cursor{0};
        unsigned char pad1[kCacheLineSize - 8] = {};

        // -- Line 2: subscriber-set snapshot (seqlock pair) -----------------
        //
        // current_membership bit i == 1 means subscriber slot i is active.
        // set_version is bumped on every membership change; the publisher
        // snapshots {version, membership} by reading version, membership,
        // version and only using the pair when the two version reads agree
        // (seqlock-style, design doc 9.6).
        alignas(kCacheLineSize) std::atomic<uint64_t> current_membership{0};
        std::atomic<uint64_t> set_version{0};
        unsigned char pad2[kCacheLineSize - 8 - 8] = {};
    };

    static_assert(sizeof(ControlBlock) == 3 * kCacheLineSize,
                  "ControlBlock must occupy exactly three cache lines");
    static_assert(alignof(ControlBlock) == kCacheLineSize);
    static_assert(std::is_standard_layout_v<ControlBlock>);
    static_assert(offsetof(ControlBlock, publisher_cursor) == kCacheLineSize,
                  "publisher cursor must start its own cache line");
    static_assert(offsetof(ControlBlock, current_membership) ==
                      2 * kCacheLineSize,
                  "membership must start its own cache line");

    // -----------------------------------------------------------------------
    // Subscriber slot (design doc 9.6)
    // -----------------------------------------------------------------------
    //
    // One entry per subscriber ID (exactly kMaxSubscribers of them). The
    // cursor lives on its own cache line (the owning subscriber is the only
    // steady-state writer; the publisher reads it for the full check, and
    // kDropOldest CAS-advances it). The registration fields live on the
    // second cache line: they change only at (un)register time but are read
    // on every Poll, so they must not share a line with the cursor.
    enum class SubscriberState : uint32_t {
        kFree = 0,
        kActive = 1,
        kEvicting = 2,
    };

    struct alignas(kCacheLineSize) SubscriberSlot {
        // -- Line A: per-subscriber read cursor -----------------------------
        alignas(kCacheLineSize) std::atomic<uint64_t> cursor{0};
        unsigned char pad0[kCacheLineSize - 8] = {};

        // -- Line B: registration metadata ----------------------------------
        alignas(kCacheLineSize) std::atomic<uint64_t> subscriber_set_version{0};
        std::atomic<uint64_t> generation{0};
        std::atomic<uint32_t> state{0};  // SubscriberState
        uint32_t reserved0 = 0;
        unsigned char pad1[kCacheLineSize - 8 - 8 - 4 - 4] = {};
    };

    static_assert(sizeof(SubscriberSlot) == 2 * kCacheLineSize,
                  "SubscriberSlot must occupy exactly two cache lines");
    static_assert(alignof(SubscriberSlot) == kCacheLineSize);
    static_assert(std::is_standard_layout_v<SubscriberSlot>);

    // -----------------------------------------------------------------------
    // Layout offsets
    // -----------------------------------------------------------------------

    static constexpr uint64_t AlignUp64(uint64_t n) {
        return (n + kCacheLineSize - 1) & ~(kCacheLineSize - 1);
    }

    // Byte offset of the IndexSlot array from the region base. The control
    // block is an integral number of cache lines, so the slot array starts
    // immediately after it, already 64-byte aligned.
    static constexpr uint64_t SlotsOffset() {
        return AlignUp64(sizeof(ControlBlock));
    }

    // Byte offset of the BroadcastSlotMeta sidecar array. capacity *
    // sizeof(IndexSlot) is a multiple of 64 (128-byte slots), so the
    // sidecar stays 64-aligned without further padding.
    static constexpr uint64_t MetasOffset(uint64_t capacity) {
        return SlotsOffset() + capacity * sizeof(IndexSlot);
    }

    // Byte offset of the SubscriberSlot array. sizeof(BroadcastSlotMeta)
    // is 16, so capacity * 16 may end mid-cache-line: align up to keep the
    // subscriber slots cache-line aligned (their cursor lines are hot).
    static constexpr uint64_t SubsOffset(uint64_t capacity) {
        return AlignUp64(MetasOffset(capacity) +
                         capacity * sizeof(BroadcastSlotMeta));
    }

    // Total bytes the channel occupies in shared memory: ControlBlock ->
    // IndexSlot[capacity] -> BroadcastSlotMeta[capacity] ->
    // SubscriberSlot[kMaxSubscribers].
    static constexpr uint64_t RequiredSize(uint64_t capacity) {
        return SubsOffset(capacity) +
               kMaxSubscribers * sizeof(SubscriberSlot);
    }

    // -----------------------------------------------------------------------
    // Init / Attach
    // -----------------------------------------------------------------------

    // Initializes a channel of `capacity` slots in place at `shm_base`.
    // `capacity` must be a power of two and >= 2. The caller must size the
    // region with RequiredSize() and must not touch the memory concurrently
    // while Init runs. The base must be 64-byte aligned.
    static Result<BroadcastChannel> Init(void* shm_base, uint64_t capacity) {
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
        control->layout_version.store(kLayoutVersion,
                                      std::memory_order_relaxed);
        control->reserved0 = 0;
        control->capacity = capacity;
        control->reserved1 = 0;
        control->publisher_cursor.store(0, std::memory_order_relaxed);
        control->current_membership.store(0, std::memory_order_relaxed);
        control->set_version.store(0, std::memory_order_relaxed);

        IndexSlot* slots = SlotsOf(shm_base);
        for (uint64_t i = 0; i < capacity; ++i) {
            // Slots start FREE with sequence_num equal to their logical
            // position, so a stale occupant from a previous wrap is told
            // apart from the current one (INV-01). IndexSlot is
            // non-assignable (atomic members): initialize field by field.
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
            // Initialized for layout consistency with MPSC; broadcast is
            // single-publisher and never reads it (no era arbitration).
            s.turn.store(i, std::memory_order_relaxed);
        }

        BroadcastSlotMeta* metas = MetasOf(shm_base, capacity);
        for (uint64_t i = 0; i < capacity; ++i) {
            metas[i].subscriber_set_version = 0;
            metas[i].ack_bitmap.bits.store(0, std::memory_order_relaxed);
        }

        SubscriberSlot* subs = SubsOf(shm_base, capacity);
        for (uint32_t i = 0; i < kMaxSubscribers; ++i) {
            subs[i].cursor.store(0, std::memory_order_relaxed);
            subs[i].subscriber_set_version.store(0, std::memory_order_relaxed);
            subs[i].generation.store(0, std::memory_order_relaxed);
            subs[i].state.store(
                static_cast<uint32_t>(SubscriberState::kFree),
                std::memory_order_relaxed);
            subs[i].reserved0 = 0;
        }

        // Publish: release so every plain/relaxed write above is visible to
        // any observer that acquires the magic.
        control->magic.store(kMagic, std::memory_order_release);
        return BroadcastChannel(control, slots, metas, subs, capacity);
    }

    // Attaches to an already-initialized channel at `shm_base`, validating
    // the magic, layout version and capacity. The capacity is read from the
    // control block (single source of truth); any validation failure refuses
    // the attach.
    static Result<BroadcastChannel> Attach(void* shm_base) {
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
                                 "broadcast control block magic mismatch");
        }
        if (control->layout_version.load(std::memory_order_acquire) !=
            kLayoutVersion) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "broadcast layout version mismatch");
        }
        const uint64_t capacity = control->capacity;
        if (capacity < 2 || (capacity & (capacity - 1)) != 0 ||
            capacity > (uint64_t{1} << 32)) {
            return Status::Error(StatusCode::kCorruption,
                                 "broadcast control block capacity is invalid");
        }
        return BroadcastChannel(control, SlotsOf(shm_base),
                                MetasOf(shm_base, capacity),
                                SubsOf(shm_base, capacity), capacity);
    }

    // -----------------------------------------------------------------------
    // Subscriber lifecycle (design doc 9.6)
    // -----------------------------------------------------------------------

    // Handle returned by RegisterSubscriber. The generation must be quoted
    // on every Poll and at Unregister: a re-registered slot reuses the ID
    // with a fresh generation, so a stale handle can never drive another
    // subscriber's cursor.
    struct SubscriberHandle {
        SubscriberId id;
        uint64_t generation = 0;
    };

    // Registers subscriber `id` and returns its handle. The join cut point
    // is the current publisher_cursor: the new subscriber receives only
    // messages published from now on (no history replay). Id reuse is safe:
    // the generation is bumped on every registration and validated on Poll.
    //
    // Errors:
    //   kResourceExhausted : id >= kMaxSubscribers.
    //   kAlreadyExists     : id is currently registered (state kActive).
    Result<SubscriberHandle> RegisterSubscriber(SubscriberId id) noexcept {
        if (id.value >= kMaxSubscribers) {
            return Status::Error(
                StatusCode::kResourceExhausted,
                "subscriber id exceeds kBroadcastMaxSubscribers");
        }
        SubscriberSlot& sub = subs_[id.value];
        uint32_t expected = static_cast<uint32_t>(SubscriberState::kFree);
        if (!sub.state.compare_exchange_strong(
                expected, static_cast<uint32_t>(SubscriberState::kActive),
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "subscriber id already registered");
        }
        // We own the slot now. Join cut point: start at the current head;
        // messages published before this point are not delivered.
        sub.cursor.store(
            control_->publisher_cursor.load(std::memory_order_acquire),
            std::memory_order_relaxed);
        const uint64_t generation =
            sub.generation.load(std::memory_order_relaxed) + 1;
        sub.generation.store(generation, std::memory_order_relaxed);

        // Membership CAS: the publisher or another registrar may be flipping
        // other bits concurrently; CAS keeps each bit flip atomic. set_version
        // is bumped with release so a publisher that acquire-reads the version
        // also observes the membership bit.
        uint64_t membership =
            control_->current_membership.load(std::memory_order_acquire);
        while (!control_->current_membership.compare_exchange_weak(
            membership, membership | (uint64_t{1} << id.value),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        }
        const uint64_t version =
            control_->set_version.fetch_add(1, std::memory_order_acq_rel) + 1;
        // Bind this registration to the exact set version it produced: the
        // next publish snapshot will carry at least this version.
        sub.subscriber_set_version.store(version, std::memory_order_release);
        return SubscriberHandle{id, generation};
    }

    // Unregisters the subscriber. The generation must match the handle
    // returned at registration: a stale handle cannot unregister a
    // re-registered (live) subscriber. Outstanding ack_bitmap bits of the
    // departed subscriber stay set: they are excluded from new publishes
    // (the membership bit is cleared) and can be reclaimed from a full
    // window via ClearStaleAcks().
    Status UnregisterSubscriber(SubscriberId id, uint64_t generation) noexcept {
        if (id.value >= kMaxSubscribers) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "subscriber id out of range");
        }
        SubscriberSlot& sub = subs_[id.value];
        if (sub.state.load(std::memory_order_acquire) !=
            static_cast<uint32_t>(SubscriberState::kActive)) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber is not registered");
        }
        if (sub.generation.load(std::memory_order_acquire) != generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber generation mismatch (stale handle)");
        }
        sub.state.store(static_cast<uint32_t>(SubscriberState::kFree),
                        std::memory_order_release);
        uint64_t membership =
            control_->current_membership.load(std::memory_order_acquire);
        while (!control_->current_membership.compare_exchange_weak(
            membership, membership & ~(uint64_t{1} << id.value),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        }
        control_->set_version.fetch_add(1, std::memory_order_acq_rel);
        return Status::Ok();
    }

    // Clears every outstanding ack bit of subscriber `id` in the current
    // window [MinCursor, publisher_cursor) (bounded by capacity slots).
    // Returns the number of bits cleared. Primitive for D2-06 eviction:
    // reclaiming the bits of a departed/slow subscriber lets the window
    // retire fully-acked slots again. Safe to call at any time; clearing a
    // bit that is not set (or that a racing Ack is also clearing) is a
    // no-op.
    uint64_t ClearStaleAcks(SubscriberId id) noexcept {
        if (id.value >= kMaxSubscribers) {
            return 0;
        }
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_acquire);
        const uint64_t lo =
            (prod > capacity_) ? prod - capacity_ : uint64_t{0};
        uint64_t cleared = 0;
        for (uint64_t seq = lo; seq < prod; ++seq) {
            IndexSlot* slot = &slots_[seq & mask_];
            // Sidecar validity rule (design doc 9.2): only trust the bitmap
            // while the slot still carries this exact sequence.
            if (slot->sequence_num.load(std::memory_order_acquire) != seq) {
                continue;
            }
            if (metas_[seq & mask_].ack_bitmap.Clear(id.value)) {
                ++cleared;
            }
        }
        CollectGarbage();
        return cleared;
    }

    // -----------------------------------------------------------------------
    // Publisher: Reserve / Fill / Commit / Abort
    // -----------------------------------------------------------------------

    // A Reservation is the publisher's exclusive write window into one slot.
    // It is move-only and must be either Commit()ed or Abort()ed; destroying
    // a live Reservation without committing aborts the slot so it can never
    // wedge the channel (subscribers skip aborted slots).
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

        // The slot the publisher fills. Only valid while active().
        IndexSlot* slot() noexcept { return slot_; }
        const IndexSlot* slot() const noexcept { return slot_; }
        IndexSlot* operator->() noexcept { return slot_; }
        IndexSlot& operator*() noexcept { return *slot_; }

        bool active() const noexcept { return active_; }

        // Logical sequence of this reservation (physical slot index =
        // sequence % capacity).
        uint64_t sequence() const noexcept { return sequence_; }

        // Stamps the subscriber-set snapshot into the slot's sidecar, seals
        // the immutable CRC, publishes the slot kReady and advances the
        // publisher cursor. After Commit the Reservation is empty.
        Status Commit() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "reservation is not active");
            }
            // Mark inactive first so a failure cannot double-publish and the
            // destructor will not stamp a tombstone over a committed slot.
            active_ = false;
            BroadcastChannel* ch = channel_;
            channel_ = nullptr;
            IndexSlot* slot = slot_;
            slot_ = nullptr;
            return ch->CommitSlot(slot, sequence_);
        }

        // Stamps an ABORTED tombstone (subscribers will skip it), clears the
        // delivery bitmap and advances the publisher cursor. After Abort the
        // Reservation is empty.
        Status Abort() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "reservation is not active");
            }
            active_ = false;
            BroadcastChannel* ch = channel_;
            channel_ = nullptr;
            IndexSlot* slot = slot_;
            slot_ = nullptr;
            return ch->AbortSlot(slot);
        }

    private:
        friend class BroadcastChannel;
        Reservation(BroadcastChannel* channel, IndexSlot* slot,
                    uint64_t sequence) noexcept
            : channel_(channel), slot_(slot), sequence_(sequence),
              active_(true) {}

        void AbortIfActive() noexcept {
            if (active_) {
                // Best-effort: a destroyed live reservation must not wedge
                // the channel. The tombstone is stamped unconditionally.
                channel_->AbortSlot(slot_).ok();
            }
        }

        BroadcastChannel* channel_ = nullptr;
        IndexSlot* slot_ = nullptr;
        uint64_t sequence_ = 0;
        bool active_ = false;
    };

    // Attempts to reserve the next slot for writing, applying `policy` if
    // the channel is full. Full iff publisher_cursor - MinActiveCursor() >=
    // capacity; with no active subscriber the channel is never full. On
    // success returns an active Reservation whose slot() is in state
    // kWriting, owned exclusively by the caller.
    //
    // `sample_rate` is only consulted for kSample: when full, a message is
    // admitted when publisher_cursor % sample_rate == 0 (deterministic
    // position-based sampling; every subscriber sees the same admitted
    // subset). Ignored for all other policies.
    //
    // Errors:
    //   kResourceExhausted : kFail and the channel is full.
    //   kDegraded          : kDropNewest / kSample dropped the incoming
    //                        message (a policy outcome, not a failure).
    Result<Reservation> Reserve(
        QueueFullPolicy policy = QueueFullPolicy::kFail,
        uint32_t sample_rate = 1) noexcept {
        // Single publisher: no CAS needed to claim our own cursor slot.
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_relaxed);

        if (IsFullAt(prod)) {
            switch (policy) {
                case QueueFullPolicy::kFail:
                    return Status::Error(StatusCode::kResourceExhausted,
                                         "broadcast channel full");
                case QueueFullPolicy::kBlock:
                    // Unbounded spin by design (see SpscChannel): bounded
                    // waiting is layered on top by the Runtime publisher. A
                    // publisher blocked here burns CPU until the slowest
                    // subscriber advances — use kFail/kDrop* when a
                    // subscriber may die.
                    while (IsFullAt(prod)) {
                        detail::SpinPause();
                    }
                    break;
                case QueueFullPolicy::kDropNewest:
                    return Status::Error(
                        StatusCode::kDegraded,
                        "broadcast channel full: newest message dropped");
                case QueueFullPolicy::kDropOldest:
                    // Force the slowest active subscriber's cursor forward
                    // past the oldest slot, clearing its bit in every slot
                    // it jumps over. Without an active subscriber the
                    // channel is never full, so this cannot be reached with
                    // an empty membership.
                    ForceDropOldest(prod);
                    break;
                case QueueFullPolicy::kSample: {
                    const uint32_t rate = sample_rate == 0 ? 1 : sample_rate;
                    // Deterministic position-based sampling keeps the
                    // admitted subset reproducible and identical across
                    // subscribers; no RNG state in SHM.
                    if ((prod % rate) != 0) {
                        return Status::Error(
                            StatusCode::kDegraded,
                            "broadcast channel full: message sampled out");
                    }
                    // Admitted: wait for space like kBlock.
                    while (IsFullAt(prod)) {
                        detail::SpinPause();
                    }
                    break;
                }
            }
        }

        IndexSlot* slot = &slots_[prod & mask_];
        // The channel was not full, so every active subscriber cursor is
        // already past this physical slot's previous occupant (or there are
        // no active subscribers). Assign the logical sequence now: it IS the
        // publisher position, making each subscriber's ABA check
        // (slot.sequence_num == its cursor) exact across wraps (INV-01).
        slot->sequence_num.store(prod, std::memory_order_relaxed);
        // Claim the slot with a CAS, not a plain store: a concurrent
        // CollectGarbage retire (kReady -> kRetired) on the previous era
        // must not be clobbered blindly. Exactly one transition out of
        // kReady wins: if the GC wins, the CAS is retried and moves
        // kRetired -> kWriting; if we win, the GC's CAS fails and it skips
        // the slot. Every reachable state (kFree on a fresh slot, kReady /
        // kRetired after a consumed era, kAborted tombstone, kWriting from
        // an orphaned reservation reclaimed by a new publisher owner)
        // transitions into kWriting, so the loop can only spin against a GC
        // retire and always terminates.
        uint32_t expected = slot->state.load(std::memory_order_relaxed);
        while (!slot->state.compare_exchange_weak(
            expected, static_cast<uint32_t>(SlotState::kWriting),
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        }
        return Reservation(this, slot, prod);
    }

    // Non-blocking reservation attempt: kWouldBlock if the channel is full.
    // Mirrors the SpscChannel TryReserve surface.
    Result<Reservation> TryReserve() noexcept {
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_relaxed);
        if (IsFullAt(prod)) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast channel full");
        }
        IndexSlot* slot = &slots_[prod & mask_];
        slot->sequence_num.store(prod, std::memory_order_relaxed);
        // Same CAS claim as Reserve(): must not clobber a racing GC retire.
        uint32_t expected = slot->state.load(std::memory_order_relaxed);
        while (!slot->state.compare_exchange_weak(
            expected, static_cast<uint32_t>(SlotState::kWriting),
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        }
        return Reservation(this, slot, prod);
    }

    // -----------------------------------------------------------------------
    // Subscriber: Poll / Ack
    // -----------------------------------------------------------------------

    // A Borrow is one subscriber's read window into one published message.
    // It owns an IndexSlotSnapshot copied at Poll time, so it is fully
    // decoupled from later overwrites of the slot (kDropOldest). Move-only;
    // destroying a live Borrow without Ack() leaves the subscriber's ack bit
    // set, so a slow/failing subscriber simply holds the slot back (and may
    // eventually be jumped over by kDropOldest).
    class Borrow {
    public:
        Borrow() noexcept = default;
        Borrow(const Borrow&) = delete;
        Borrow& operator=(const Borrow&) = delete;

        Borrow(Borrow&& other) noexcept
            : channel_(other.channel_),
              sub_(other.sub_),
              snapshot_(other.snapshot_),
              active_(other.active_) {
            other.channel_ = nullptr;
            other.active_ = false;
        }
        Borrow& operator=(Borrow&& other) noexcept {
            if (this != &other) {
                channel_ = other.channel_;
                sub_ = other.sub_;
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

        // Clears this subscriber's ack bit on the borrowed slot, advances
        // the subscriber cursor past it and retires fully-acked slots. If
        // the message was overtaken by kDropOldest while borrowed, the bit
        // is still cleared (it may already have been cleared by the drop
        // itself; Clear is idempotent) and Ack reports kNotFound without
        // moving the cursor. After Ack (either outcome) the Borrow is empty.
        Status Ack() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "borrow is not active");
            }
            active_ = false;
            BroadcastChannel* ch = channel_;
            channel_ = nullptr;
            return ch->AckSlot(sub_, snapshot_.sequence_num);
        }

    private:
        friend class BroadcastChannel;
        Borrow(BroadcastChannel* channel, SubscriberId sub,
               const IndexSlotSnapshot& snapshot) noexcept
            : channel_(channel), sub_(sub), snapshot_(snapshot),
              active_(true) {}

        BroadcastChannel* channel_ = nullptr;
        SubscriberId sub_;
        IndexSlotSnapshot snapshot_;
        bool active_ = false;
    };

    // Polls the next message for subscriber `sub`. Returns:
    //   an active Borrow : a message is available (state kReady, sequence
    //                      matches, CRC verified).
    //   kWouldBlock      : the subscriber has caught up with the publisher.
    //   kNotFound        : the subscriber is not registered or the
    //                      generation does not match (stale handle).
    //   kCorruption      : a slot failed its sequence or CRC check; the
    //                      subscriber's cursor was advanced past it so the
    //                      channel keeps making progress.
    //
    // Aborted tombstones are transparently skipped (the ack_bitmap carries
    // no delivery obligation for them, so only the cursor advances).
    Result<Borrow> Poll(SubscriberHandle sub) noexcept {
        SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SubscriberState::kActive) ||
            ss.generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(
                StatusCode::kNotFound,
                "subscriber not registered or stale generation");
        }
        while (true) {
            // The cursor may be CAS-advanced by kDropOldest concurrently, so
            // re-read it on every iteration instead of caching it.
            const uint64_t cons = ss.cursor.load(std::memory_order_acquire);
            const uint64_t prod =
                control_->publisher_cursor.load(std::memory_order_acquire);
            if (cons == prod) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "broadcast channel empty");
            }
            IndexSlot* slot = &slots_[cons & mask_];
            const uint32_t state =
                slot->state.load(std::memory_order_acquire);

            if (state == static_cast<uint32_t>(SlotState::kAborted)) {
                // Tombstone: no delivery obligation (Commit clears the
                // bitmap on Abort); skip by advancing only our own cursor.
                // CAS so a concurrent kDropOldest advance never gets
                // overwritten with a stale value.
                uint64_t expected = cons;
                ss.cursor.compare_exchange_strong(
                    expected, cons + 1, std::memory_order_acq_rel,
                    std::memory_order_acquire);
                continue;
            }
            if (state == static_cast<uint32_t>(SlotState::kRetired)) {
                // A kRetired slot AT our cursor is always a GC-vs-Reserve
                // clobber: a legitimate retirement requires every ACK bit
                // drained (including ours), which would have advanced our
                // cursor past this slot already. The clobbered transition is
                // kReady -> kRetired landing right after a fully-committed
                // new era published kReady, so the immutable fields are
                // complete and CRC-verifiable. Fall through and deliver it
                // exactly like kReady: consuming a retired-but-valid slot is
                // harmless (recycling overwrites the state next era anyway),
                // while refusing it would wedge this subscriber and, through
                // the full check, the publisher.
                if (slot->sequence_num.load(std::memory_order_relaxed) ==
                    cons) {
                    IndexSlotSnapshot snapshot = SnapshotIndexSlot(*slot);
                    if (VerifySnapshotCrc(snapshot)) {
                        return Borrow(this, sub.id, snapshot);
                    }
                }
                // Genuinely stale or corrupt: skip like corruption below.
                uint64_t expected = cons;
                ss.cursor.compare_exchange_strong(
                    expected, cons + 1, std::memory_order_acq_rel,
                    std::memory_order_acquire);
                return Status::Error(
                    StatusCode::kCorruption,
                    "broadcast slot retired under a live cursor (skipped)");
            }
            if (state != static_cast<uint32_t>(SlotState::kReady)) {
                // Not yet published (the publisher is between Reserve and
                // Commit with the cursor not yet advanced — the empty check
                // above normally hides this; defensive cover for torn
                // views). Treat as empty.
                return Status::Error(StatusCode::kWouldBlock,
                                     "broadcast slot not yet ready");
            }
            // ABA guard: the slot's logical sequence must match our cursor.
            // A mismatch, like a CRC failure, is corruption: skip the slot
            // (never livelock on it) and report. The acquire on `state`
            // above already published the sequence, so a relaxed load
            // suffices.
            if (slot->sequence_num.load(std::memory_order_relaxed) != cons) {
                uint64_t expected = cons;
                ss.cursor.compare_exchange_strong(
                    expected, cons + 1, std::memory_order_acq_rel,
                    std::memory_order_acquire);
                return Status::Error(
                    StatusCode::kCorruption,
                    "broadcast slot sequence mismatch (skipped)");
            }
            // Copy the header out, then verify the CRC on our own copy. The
            // snapshot decouples all further use from any later overwrite
            // (kDropOldest may recycle the slot while we hold the Borrow).
            IndexSlotSnapshot snapshot = SnapshotIndexSlot(*slot);
            if (!VerifySnapshotCrc(snapshot)) {
                uint64_t expected = cons;
                ss.cursor.compare_exchange_strong(
                    expected, cons + 1, std::memory_order_acq_rel,
                    std::memory_order_acquire);
                return Status::Error(
                    StatusCode::kCorruption,
                    "broadcast slot immutable CRC mismatch (skipped)");
            }
            return Borrow(this, sub.id, snapshot);
        }
    }

    // Standalone Ack for a previously polled sequence: clears the
    // subscriber's ack bit on that slot and, when `seq` equals the
    // subscriber cursor, advances the cursor and retires fully-acked slots.
    // A `seq` behind the cursor (overtaken by kDropOldest) still clears the
    // bit but reports kNotFound, mirroring Borrow::Ack().
    Status Ack(SubscriberHandle sub, uint64_t seq) noexcept {
        SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SubscriberState::kActive) ||
            ss.generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(
                StatusCode::kNotFound,
                "subscriber not registered or stale generation");
        }
        return AckSlot(sub.id, seq);
    }

    // -----------------------------------------------------------------------
    // Garbage collection
    // -----------------------------------------------------------------------

    // Scans the window [min subscriber cursor, publisher_cursor) — bounded
    // by capacity slots — and retires every kReady slot whose ack_bitmap is
    // fully cleared. Called from Ack() after the cursor advances; also
    // exposed publicly so eviction/recovery paths (D2-06) and tests can run
    // it explicitly.
    void CollectGarbage() noexcept {
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_acquire);
        const uint64_t min_cursor = MinCursor();
        if (min_cursor >= prod) {
            return;
        }
        uint64_t n = prod - min_cursor;
        if (n > capacity_) {
            n = capacity_;
        }
        for (uint64_t i = 0; i < n; ++i) {
            const uint64_t seq = min_cursor + i;
            const uint64_t phys = seq & mask_;
            IndexSlot* slot = &slots_[phys];
            if (slot->state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SlotState::kReady)) {
                continue;
            }
            // Sidecar validity: only retire when the slot still carries this
            // exact sequence; a recycled slot is never touched.
            if (slot->sequence_num.load(std::memory_order_acquire) != seq) {
                continue;
            }
            if (metas_[phys].ack_bitmap.AllAcked()) {
                // CAS, not a plain store: the publisher may be concurrently
                // recycling this physical slot for the next era (every active
                // cursor has passed it, so reclaim is legal) and its Reserve
                // also claims with a CAS. Exactly one transition out of
                // kReady wins: if our CAS fails, the publisher moved the
                // slot on (kWriting) and the new era retires normally later;
                // if it wins, the publisher's CAS observes kRetired and
                // moves on. A plain store could clobber a fresh kReady with
                // kRetired and wedge every subscriber at that cursor.
                uint32_t expected = static_cast<uint32_t>(SlotState::kReady);
                slot->state.compare_exchange_strong(
                    expected, static_cast<uint32_t>(SlotState::kRetired),
                    std::memory_order_acq_rel, std::memory_order_relaxed);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Observers
    // -----------------------------------------------------------------------

    // Approximate (not linearizable): true iff the publisher has not
    // published anything yet.
    bool IsEmpty() const noexcept {
        return control_->publisher_cursor.load(std::memory_order_acquire) ==
               0;
    }

    // True iff the oldest active subscriber is a full ring behind the
    // publisher. With no active subscriber the channel is never full.
    bool IsFull() const noexcept {
        return IsFullAt(
            control_->publisher_cursor.load(std::memory_order_acquire));
    }

    // Number of messages the publisher has committed or aborted so far.
    uint64_t Size() const noexcept {
        return control_->publisher_cursor.load(std::memory_order_acquire);
    }

    uint64_t capacity() const noexcept { return capacity_; }

private:
    BroadcastChannel(ControlBlock* control, IndexSlot* slots,
                     BroadcastSlotMeta* metas, SubscriberSlot* subs,
                     uint64_t capacity) noexcept
        : control_(control),
          slots_(slots),
          metas_(metas),
          subs_(subs),
          capacity_(capacity),
          mask_(capacity - 1) {}

    static IndexSlot* SlotsOf(void* shm_base) noexcept {
        return reinterpret_cast<IndexSlot*>(
            static_cast<unsigned char*>(shm_base) + SlotsOffset());
    }

    static BroadcastSlotMeta* MetasOf(void* shm_base,
                                      uint64_t capacity) noexcept {
        return reinterpret_cast<BroadcastSlotMeta*>(
            static_cast<unsigned char*>(shm_base) + MetasOffset(capacity));
    }

    static SubscriberSlot* SubsOf(void* shm_base, uint64_t capacity) noexcept {
        return reinterpret_cast<SubscriberSlot*>(
            static_cast<unsigned char*>(shm_base) + SubsOffset(capacity));
    }

    // Full iff `prod` - oldest active subscriber cursor >= capacity. With
    // no active subscriber the channel is never full (MinActiveCursor ==
    // prod, so the distance is 0).
    bool IsFullAt(uint64_t prod) const noexcept {
        return prod - MinActiveCursor(prod) >= capacity_;
    }

    // Minimum cursor over all ACTIVE subscribers. The membership bit is the
    // authority for liveness (the slot state may lag it by a few stores
    // during register/unregister), and the cursor read is skipped for
    // inactive bits entirely: a retired subscriber's cursor is frozen at
    // wherever it stopped and must never count toward fullness.
    uint64_t MinActiveCursor(uint64_t prod) const noexcept {
        const uint64_t membership =
            control_->current_membership.load(std::memory_order_acquire);
        uint64_t min_cursor = prod;
        uint64_t remaining = membership;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            const uint64_t cursor =
                subs_[id].cursor.load(std::memory_order_acquire);
            if (cursor < min_cursor) {
                min_cursor = cursor;
            }
        }
        return min_cursor;
    }

    // Minimum cursor over ALL subscriber slots (including inactive ones).
    // Used as the garbage-collection lower bound: it is conservative (an
    // inactive subscriber's frozen cursor may hold the window open), so
    // slots whose only outstanding ACKs belong to departed subscribers stay
    // kReady until ClearStaleAcks() releases them. Capacity-bounded, so it
    // can never look further back than one full ring.
    uint64_t MinCursor() const noexcept {
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_acquire);
        const uint64_t floor = (prod > capacity_) ? prod - capacity_ : 0;
        uint64_t min_cursor = prod;
        for (uint32_t id = 0; id < kMaxSubscribers; ++id) {
            const uint64_t cursor =
                subs_[id].cursor.load(std::memory_order_acquire);
            if (cursor < min_cursor) {
                min_cursor = cursor;
            }
        }
        return min_cursor < floor ? floor : min_cursor;
    }

    // Seqlock-style snapshot of the subscriber set (design doc 9.6): read
    // set_version, then current_membership, then set_version again, and
    // only use the pair when the two version reads agree — otherwise a
    // concurrent register/unregister may have torn the membership.
    struct SubscriberSetSnapshot {
        uint64_t version = 0;
        uint64_t membership = 0;
    };

    SubscriberSetSnapshot SnapshotSubscriberSet() const noexcept {
        SubscriberSetSnapshot snap;
        do {
            snap.version =
                control_->set_version.load(std::memory_order_acquire);
            snap.membership =
                control_->current_membership.load(std::memory_order_acquire);
        } while (snap.version !=
                 control_->set_version.load(std::memory_order_acquire));
        return snap;
    }

    // Stamps the subscriber-set snapshot into the slot's sidecar, seals the
    // immutable CRC, publishes kReady and advances the publisher cursor.
    // Store order is the protocol contract: sidecar binding -> CRC seal ->
    // kReady (release) -> publisher_cursor (release). A subscriber that
    // acquire-reads kReady observes every sidecar and payload field.
    Status CommitSlot(IndexSlot* slot, uint64_t sequence) noexcept {
        const uint64_t phys = sequence & mask_;
        const SubscriberSetSnapshot snap = SnapshotSubscriberSet();
        metas_[phys].subscriber_set_version = snap.version;
        // One bit per active subscriber at this instant: exactly the set of
        // ACKs this message must collect before its slot may retire.
        metas_[phys].ack_bitmap.bits.store(snap.membership,
                                           std::memory_order_relaxed);
        SealIndexSlotImmutableCrc(*slot);
        slot->state.store(static_cast<uint32_t>(SlotState::kReady),
                          std::memory_order_release);
        // Advance the publisher cursor: this is what makes the slot visible
        // to every subscriber's Poll.
        control_->publisher_cursor.store(sequence + 1,
                                         std::memory_order_release);
        return Status::Ok();
    }

    // Stamps an ABORTED tombstone and advances the publisher cursor. The
    // ack_bitmap is cleared: an aborted message has no delivery obligation,
    // so subscribers skip it without touching the bitmap (design doc 9.6).
    Status AbortSlot(IndexSlot* slot) noexcept {
        metas_[control_->publisher_cursor.load(std::memory_order_relaxed) &
               mask_]
            .ack_bitmap.bits.store(0, std::memory_order_relaxed);
        slot->state.store(static_cast<uint32_t>(SlotState::kAborted),
                          std::memory_order_relaxed);
        control_->publisher_cursor.fetch_add(1, std::memory_order_release);
        return Status::Ok();
    }

    // kDropOldest (design doc 9.8): force the slowest active subscriber
    // forward past the oldest unconsumed slot, clearing its ack bit in
    // every slot it jumps over. The jumped subscriber later observes its
    // cursor moved (or finds a recycled slot with a sequence mismatch,
    // reported as kCorruption); a Borrow it still holds keeps a valid
    // snapshot and its late Ack reports kNotFound. The payload is NOT
    // reclaimed here (design doc 9.8: only after no borrows remain).
    //
    // The cursor advance uses CAS so a racing Poll/Ack of the same
    // subscriber either wins first (and we re-read) or loses cleanly; the
    // publisher's own cursor never moves backward.
    void ForceDropOldest(uint64_t prod) noexcept {
        const uint64_t membership =
            control_->current_membership.load(std::memory_order_acquire);
        if (membership == 0) {
            return;  // Unreachable via Reserve (never full), but stay safe.
        }
        uint32_t slowest = 0;
        uint64_t min_cursor = prod;
        uint64_t remaining = membership;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            const uint64_t cursor =
                subs_[id].cursor.load(std::memory_order_acquire);
            if (cursor < min_cursor) {
                min_cursor = cursor;
                slowest = id;
            }
        }
        if (min_cursor >= prod) {
            return;  // Slowest subscriber is caught up: nothing to drop.
        }
        // Clear the victim's bit in every slot it is about to skip: those
        // slots may then retire in CollectGarbage once every other
        // subscriber has acked them.
        for (uint64_t seq = min_cursor; seq < prod; ++seq) {
            IndexSlot* slot = &slots_[seq & mask_];
            if (slot->sequence_num.load(std::memory_order_acquire) != seq) {
                continue;
            }
            metas_[seq & mask_].ack_bitmap.Clear(slowest);
        }
        // CAS advance: a concurrent Poll/Ack of the victim may have moved
        // the cursor forward already; whichever writer wins, the cursor
        // stays monotonic (both advance it strictly forward).
        uint64_t expected = min_cursor;
        subs_[slowest].cursor.compare_exchange_strong(
            expected, prod, std::memory_order_acq_rel,
            std::memory_order_acquire);
        CollectGarbage();
    }

    // Subscriber-side Ack. Clears the ack bit on slot `sequence`; when
    // `sequence` is the subscriber's current head the cursor advances
    // (CAS against a racing kDropOldest) and fully-acked slots retire.
    // A sequence behind the cursor was overtaken by kDropOldest: the bit
    // is still cleared (idempotent; the drop may already have cleared it)
    // but the cursor is untouched and kNotFound reports the drop.
    Status AckSlot(SubscriberId sub, uint64_t sequence) noexcept {
        SubscriberSlot& ss = subs_[sub.value];
        const uint64_t cons = ss.cursor.load(std::memory_order_acquire);
        // Clear unconditionally, including for an overtaken sequence: the
        // slot may still be live and waiting on this bit to retire.
        metas_[sequence & mask_].ack_bitmap.Clear(sub.value);
        if (sequence != cons) {
            return Status::Error(
                StatusCode::kNotFound,
                "message was dropped (overtaken by kDropOldest)");
        }
        uint64_t expected = cons;
        ss.cursor.compare_exchange_strong(expected, cons + 1,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire);
        CollectGarbage();
        return Status::Ok();
    }

    ControlBlock* control_ = nullptr;
    IndexSlot* slots_ = nullptr;
    BroadcastSlotMeta* metas_ = nullptr;
    SubscriberSlot* subs_ = nullptr;
    uint64_t capacity_ = 0;
    uint64_t mask_ = 0;
};

static_assert(std::is_trivially_copyable_v<BroadcastChannel>,
              "BroadcastChannel must be a trivially copyable view");

}  // namespace mino

#endif  // MINO_SHM_CHANNEL_BROADCAST_CHANNEL_H_
