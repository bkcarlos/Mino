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
// D2-06: Broadcast membership (design doc 9.6 / 12.2): subscriber heartbeat
// lease, stale-subscriber eviction with generation binding and ACK-responsibility
// cleanup.
//
// Single publisher / N subscriber fan-out over shared memory. Every active
// subscriber receives every published message exactly once; each subscriber
// advances its own cursor and ACKs through a per-slot, per-subscriber exact-era
// token (layout v5). BroadcastSlotMeta::ack_bitmap remains the immutable
// membership snapshot for diagnostics/compatibility, not cleanup authority.
//
// Publication protocol (design doc 9.6):
//   Reserve : single publisher, no Vyukov turn arbitration (turn is
//             initialized for layout consistency but never read). Full
//             iff publisher_cursor - MinActiveCursor() >= capacity; with no
//             active subscriber the channel is never full.
//   Commit  : the per-slot membership bitmap and exact ack_era tokens are
//             stamped from the subscriber-set snapshot, the immutable CRC is
//             sealed, state goes kReady (release), and publisher_cursor advances.
//   Abort   : state goes kAborted, all ACK metadata is cleared (no delivery
//             obligation), and publisher_cursor advances. Subscribers skip
//             tombstones transparently.
//
// Consumption protocol: Poll(sub) validates the subscriber registration
// (active + generation match), then walks from the subscriber's own cursor:
// kAborted slots are skipped, sequence/CRC mismatches report kCorruption
// and are skipped, otherwise a snapshot Borrow is returned. Ack(seq) binds
// both the borrowed slot era (exact sequence) and subscriber generation before
// clearing the subscriber's exact ack_era token, advances the cursor, then runs
// CollectGarbage() to retire fully-acked slots.
//
// BroadcastChannel is header-only: a non-owning view over shared memory
// (design doc 9.9 lifecycle: a process exiting does not "destruct" the
// channel).

#ifndef MINO_SHM_CHANNEL_BROADCAST_CHANNEL_H_
#define MINO_SHM_CHANNEL_BROADCAST_CHANNEL_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/queue_full_policy.h"
#include "mino/shm/channel/spsc_channel.h"  // detail::SpinPause

namespace mino {

class BroadcastChannel {
public:
    using PayloadRetireObserver = Status (*)(ShmHandle, void*) noexcept;
    using RetirePersistenceHook = void (*)(uint64_t, void*) noexcept;
    static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
                  "BroadcastChannel requires lock-free 64-bit atomics");

    static constexpr uint64_t kCacheLineSize = 64;
    static constexpr uint64_t kMagic = 0x4D49'4E4F'4252'4443ULL;  // "MINOBRDC"
    // v6 adds owner-bound recoverable Borrow records, all-or-nothing DropOldest
    // transactions, ordered Gap publication, and retryable payload retirement.
    // v5 added generation-bound Gap statistics; v4 introduced exact-era ACK
    // cleanup and atomic payload snapshots.
    static constexpr uint32_t kLayoutVersion = 6;
    static constexpr uint32_t kMaxSubscribers = kBroadcastMaxSubscribers;  // 64
    // Stable cursors are ordinary logical sequences. The high bit denotes a
    // recoverable in-progress cleanup for the sequence in the low 63 bits.
    static constexpr uint64_t kCursorCleanupBit = uint64_t{1} << 63;

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

        // -- Line 3: single-publisher, helpable DropOldest transaction -------
        // drop_control encodes {sequence + 1, phase}; the remaining fields are
        // immutable while phase is prepared/committing.
        alignas(kCacheLineSize) std::atomic<uint64_t> drop_control{0};
        std::atomic<uint64_t> drop_sequence{0};
        std::atomic<uint64_t> drop_targets{0};
        unsigned char pad3[kCacheLineSize - 8 - 8 - 8] = {};
    };

    static_assert(sizeof(ControlBlock) == 4 * kCacheLineSize,
                  "ControlBlock must occupy exactly four cache lines");
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
    // second cache line: they change only at (un)register/evict time but are
    // read on every Poll, so they must not share a line with the cursor.
    // heartbeat_ns (D2-06, design doc 12.2 SubscriberLease) rides the same
    // line: it is written only by the owning subscriber's Heartbeat and read
    // only by the eviction scan, both off the Poll hot path.
    enum class SubscriberState : uint32_t {
        kFree = 0,
        kActive = 1,
        kEvicting = 2,
        kRegistering = 3,
    };

    struct alignas(kCacheLineSize) SubscriberSlot {
        // -- Line A: per-subscriber read cursor -----------------------------
        alignas(kCacheLineSize) std::atomic<uint64_t> cursor{0};
        unsigned char pad0[kCacheLineSize - 8] = {};

        // -- Line B: registration metadata + lease heartbeat ----------------
        alignas(kCacheLineSize) std::atomic<uint64_t> subscriber_set_version{0};
        std::atomic<uint64_t> generation{0};
        std::atomic<uint32_t> state{0};  // SubscriberState
        uint32_t reserved0 = 0;
        // Last Heartbeat timestamp (ns, caller-supplied monotonic clock).
        // Written relaxed by the owning subscriber only; read by
        // EvictStaleSubscribers. Meaningful only while state != kFree.
        std::atomic<uint64_t> heartbeat_ns{0};
        // Generation that authored heartbeat_ns. A stale Heartbeat racing an
        // eviction/re-registration may write late, but the new generation will
        // never trust that timestamp (D2-06 generation-bound lease cleanup).
        std::atomic<uint64_t> heartbeat_generation{0};
        std::atomic<uint64_t> lease_epoch{0};
        unsigned char pad1[kCacheLineSize - 8 - 8 - 4 - 4 - 8 - 8 - 8] = {};

        // -- Line C: generation-bound, transaction-published Gap ------------
        alignas(kCacheLineSize) std::atomic<uint64_t> gap_generation{0};
        std::atomic<uint64_t> latest_gap_first_sequence{0};
        std::atomic<uint64_t> latest_gap_next_sequence{0};
        std::atomic<uint64_t> gap_events{0};
        std::atomic<uint64_t> gap_messages{0};
        std::atomic<uint64_t> observed_gap_events{0};
        std::atomic<uint64_t> gap_committed_era{0};
        unsigned char pad2[kCacheLineSize - 7 * 8] = {};

        // -- Line D: recoverable Borrow record -------------------------------
        // borrow_control encodes {sequence + 1, phase}. Metadata is published
        // before phase becomes active and is immutable until exact-token clear.
        alignas(kCacheLineSize) std::atomic<uint64_t> borrow_control{0};
        std::atomic<uint64_t> borrow_generation{0};
        std::atomic<uint64_t> borrow_lease_epoch{0};
        std::atomic<uint64_t> borrow_owner_node_id{0};
        std::atomic<uint64_t> borrow_owner_process_id{0};
        std::atomic<uint64_t> borrow_owner_process_epoch{0};
        std::atomic<uint64_t> borrow_owner_start_time_ns{0};
        unsigned char pad3[kCacheLineSize - 7 * 8] = {};

        // -- Line E: per-target Drop transaction preparation ----------------
        alignas(kCacheLineSize) std::atomic<uint64_t> drop_claim{0};
        std::atomic<uint64_t> drop_generation{0};
        std::atomic<uint64_t> drop_gap_events{0};
        std::atomic<uint64_t> drop_gap_messages{0};
        unsigned char pad4[kCacheLineSize - 4 * 8] = {};
    };

    static_assert(sizeof(SubscriberSlot) == 5 * kCacheLineSize,
                  "SubscriberSlot must occupy exactly five cache lines");
    static_assert(alignof(SubscriberSlot) == kCacheLineSize);
    static_assert(std::is_standard_layout_v<SubscriberSlot>);
    static_assert(offsetof(SubscriberSlot, cursor) == 0,
                  "subscriber cursor must start the slot's first cache line");
    static_assert(offsetof(SubscriberSlot, subscriber_set_version) ==
                      kCacheLineSize,
                  "registration metadata must start the second cache line");
    static_assert(offsetof(SubscriberSlot, heartbeat_ns) ==
                      kCacheLineSize + 8 + 8 + 4 + 4,
                  "heartbeat must pack into the line-B padding (D2-06)");
    static_assert(offsetof(SubscriberSlot, heartbeat_generation) ==
                      kCacheLineSize + 8 + 8 + 4 + 4 + 8,
                  "heartbeat generation must remain in line B");
    static_assert(offsetof(SubscriberSlot, gap_generation) ==
                      2 * kCacheLineSize,
                  "Gap state must start the slot's third cache line");
    static_assert(offsetof(SubscriberSlot, borrow_control) ==
                      3 * kCacheLineSize,
                  "Borrow state must start the slot's fourth cache line");
    static_assert(offsetof(SubscriberSlot, drop_claim) ==
                      4 * kCacheLineSize,
                  "Drop state must start the slot's fifth cache line");

    // -----------------------------------------------------------------------
    // Broadcast-only era sidecar (layout v4+)
    // -----------------------------------------------------------------------
    //
    // The payload copy is split into lock-free 64-bit atomics. payload_era is a
    // seqlock-style publication stamp (`sequence + 1`, zero while being
    // replaced), so GC can obtain a coherent handle even while the publisher
    // recycles IndexSlot's non-atomic payload fields. Each ack_era entry is the
    // exact logical era owed by one subscriber, also encoded as sequence + 1;
    // clearing it uses an exact CAS, so even a delayed crash helper cannot touch
    // a reused era. retired_era is the monotonic observer claim. None is a lock.
    struct BroadcastEraMeta {
        std::atomic<uint64_t> payload_era{0};
        std::atomic<uint64_t> payload_offset{0};
        std::atomic<uint64_t> payload_identity{0};
        std::atomic<uint64_t> retired_era{0};
        std::atomic<uint64_t> ack_era[kMaxSubscribers]{};
    };

    static_assert(sizeof(BroadcastEraMeta) ==
                  (4 + kMaxSubscribers) * sizeof(uint64_t));
    static_assert(alignof(BroadcastEraMeta) == alignof(uint64_t));
    static_assert(std::is_standard_layout_v<BroadcastEraMeta>);

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

    // Byte offset of the layout-v4+ era sidecar array.
    static constexpr uint64_t EraMetasOffset(uint64_t capacity) {
        return MetasOffset(capacity) + capacity * sizeof(BroadcastSlotMeta);
    }

    // Byte offset of the SubscriberSlot array. Sidecars may end mid-cache-line:
    // align up to keep subscriber cursor lines isolated and cache-line aligned.
    static constexpr uint64_t SubsOffset(uint64_t capacity) {
        return AlignUp64(EraMetasOffset(capacity) +
                         capacity * sizeof(BroadcastEraMeta));
    }

    // Total bytes the channel occupies in shared memory: ControlBlock ->
    // IndexSlot[capacity] -> BroadcastSlotMeta[capacity] ->
    // BroadcastEraMeta[capacity] -> SubscriberSlot[kMaxSubscribers].
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
        control->drop_control.store(0, std::memory_order_relaxed);
        control->drop_sequence.store(0, std::memory_order_relaxed);
        control->drop_targets.store(0, std::memory_order_relaxed);

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
        BroadcastEraMeta* era_metas = EraMetasOf(shm_base, capacity);
        for (uint64_t i = 0; i < capacity; ++i) {
            metas[i].subscriber_set_version = 0;
            metas[i].ack_bitmap.bits.store(0, std::memory_order_relaxed);
            era_metas[i].payload_era.store(0, std::memory_order_relaxed);
            era_metas[i].payload_offset.store(0, std::memory_order_relaxed);
            era_metas[i].payload_identity.store(0, std::memory_order_relaxed);
            era_metas[i].retired_era.store(0, std::memory_order_relaxed);
            for (uint32_t id = 0; id < kMaxSubscribers; ++id) {
                era_metas[i].ack_era[id].store(0,
                                               std::memory_order_relaxed);
            }
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
            subs[i].heartbeat_ns.store(0, std::memory_order_relaxed);
            subs[i].heartbeat_generation.store(0,
                                               std::memory_order_relaxed);
            subs[i].lease_epoch.store(0, std::memory_order_relaxed);
            subs[i].gap_generation.store(0, std::memory_order_relaxed);
            subs[i].latest_gap_first_sequence.store(0,
                                                    std::memory_order_relaxed);
            subs[i].latest_gap_next_sequence.store(0,
                                                   std::memory_order_relaxed);
            subs[i].gap_events.store(0, std::memory_order_relaxed);
            subs[i].gap_messages.store(0, std::memory_order_relaxed);
            subs[i].observed_gap_events.store(0,
                                              std::memory_order_relaxed);
            subs[i].gap_committed_era.store(0, std::memory_order_relaxed);
            subs[i].borrow_control.store(0, std::memory_order_relaxed);
            subs[i].borrow_generation.store(0, std::memory_order_relaxed);
            subs[i].borrow_lease_epoch.store(0, std::memory_order_relaxed);
            subs[i].borrow_owner_node_id.store(0, std::memory_order_relaxed);
            subs[i].borrow_owner_process_id.store(0,
                                                  std::memory_order_relaxed);
            subs[i].borrow_owner_process_epoch.store(
                0, std::memory_order_relaxed);
            subs[i].borrow_owner_start_time_ns.store(
                0, std::memory_order_relaxed);
            subs[i].drop_claim.store(0, std::memory_order_relaxed);
            subs[i].drop_generation.store(0, std::memory_order_relaxed);
            subs[i].drop_gap_events.store(0, std::memory_order_relaxed);
            subs[i].drop_gap_messages.store(0, std::memory_order_relaxed);
        }

        // Publish: release so every plain/relaxed write above is visible to
        // any observer that acquires the magic.
        control->magic.store(kMagic, std::memory_order_release);
        return BroadcastChannel(control, slots, metas, era_metas, subs,
                                capacity);
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
                                EraMetasOf(shm_base, capacity),
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

    struct Gap {
        SubscriberId subscriber_id;
        uint64_t generation = 0;
        uint64_t first_sequence = 0;
        uint64_t next_sequence = 0;
        uint64_t total_events = 0;
        uint64_t total_messages = 0;
    };

    struct SubscriberStats {
        uint64_t gap_events = 0;
        uint64_t gap_messages = 0;
    };

    // Registers subscriber `id` and returns its handle. The join cut point
    // is the current publisher_cursor: the new subscriber receives only
    // messages published from now on (no history replay). Id reuse is safe:
    // the generation is bumped on every registration and validated on Poll.
    //
    // `now_ns` seeds the lease heartbeat (D2-06, design doc 12.2): the
    // registration instant is the subscriber's first proof of liveness, so
    // the eviction lease starts counting from here. Callers obtain it from a
    // monotonic clock (see MonotonicNowNs()); passing it in keeps the
    // channel testable and lets the Coordinator layer share one time source.
    //
    // Errors:
    //   kResourceExhausted : id >= kMaxSubscribers.
    //   kAlreadyExists     : id is currently registered (state kActive).
    Result<SubscriberHandle> RegisterSubscriber(
        SubscriberId id, const ProcessIdentity& owner,
        uint64_t now_ns) noexcept {
        if (id.value >= kMaxSubscribers) {
            return Status::Error(
                StatusCode::kResourceExhausted,
                "subscriber id exceeds kBroadcastMaxSubscribers");
        }
        if (owner.IsZero()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "subscriber owner identity is zero");
        }
        SubscriberSlot& sub = subs_[id.value];
        // Claim a private transitional state first. Publishing kActive before
        // generation/cursor initialization would let a stale handle from the
        // previous incarnation pass validation during this window.
        uint32_t expected = static_cast<uint32_t>(SubscriberState::kFree);
        if (!sub.state.compare_exchange_strong(
                expected, static_cast<uint32_t>(SubscriberState::kRegistering),
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
        const uint64_t lease_epoch =
            sub.lease_epoch.load(std::memory_order_relaxed) + 1;
        if (generation == 0 || lease_epoch == 0) {
            sub.state.store(static_cast<uint32_t>(SubscriberState::kFree),
                            std::memory_order_release);
            return Status::Error(StatusCode::kResourceExhausted,
                                 "subscriber generation or lease exhausted");
        }
        sub.generation.store(generation, std::memory_order_relaxed);
        sub.lease_epoch.store(lease_epoch, std::memory_order_relaxed);
        sub.heartbeat_ns.store(now_ns, std::memory_order_relaxed);
        sub.heartbeat_generation.store(generation,
                                       std::memory_order_relaxed);
        sub.gap_generation.store(generation, std::memory_order_relaxed);
        sub.latest_gap_first_sequence.store(0, std::memory_order_relaxed);
        sub.latest_gap_next_sequence.store(0, std::memory_order_relaxed);
        sub.gap_events.store(0, std::memory_order_relaxed);
        sub.gap_messages.store(0, std::memory_order_relaxed);
        sub.observed_gap_events.store(0, std::memory_order_relaxed);
        sub.gap_committed_era.store(0, std::memory_order_relaxed);
        sub.borrow_control.store(0, std::memory_order_relaxed);
        sub.borrow_generation.store(0, std::memory_order_relaxed);
        sub.borrow_lease_epoch.store(0, std::memory_order_relaxed);
        sub.borrow_owner_node_id.store(owner.node_id,
                                       std::memory_order_relaxed);
        sub.borrow_owner_process_id.store(owner.process_id,
                                          std::memory_order_relaxed);
        sub.borrow_owner_process_epoch.store(owner.process_epoch,
                                             std::memory_order_relaxed);
        sub.borrow_owner_start_time_ns.store(owner.start_time_ns,
                                             std::memory_order_relaxed);
        sub.drop_claim.store(0, std::memory_order_relaxed);
        sub.drop_generation.store(0, std::memory_order_relaxed);
        sub.drop_gap_events.store(0, std::memory_order_relaxed);
        sub.drop_gap_messages.store(0, std::memory_order_relaxed);

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
        sub.subscriber_set_version.store(version, std::memory_order_relaxed);
        // Final release publication: Poll/Ack/Heartbeat that acquire kActive
        // now observe the fully initialized cursor, generation and lease.
        sub.state.store(static_cast<uint32_t>(SubscriberState::kActive),
                        std::memory_order_release);
        return SubscriberHandle{id, generation};
    }

    // Convenience overload seeding the lease from the channel's own
    // monotonic clock. Call sites that do not exercise lease expiry (the
    // common case) stay free of time plumbing; the explicit overload above
    // remains for the Coordinator (D2-08) and for tests that drive time.
    Result<SubscriberHandle> RegisterSubscriber(SubscriberId id,
                                                uint64_t now_ns) noexcept {
        return RegisterSubscriber(id, ProcessIdentity::Current(), now_ns);
    }

    Result<SubscriberHandle> RegisterSubscriber(SubscriberId id) noexcept {
        return RegisterSubscriber(id, ProcessIdentity::Current(),
                                  MonotonicNowNs());
    }

    // Unregisters the subscriber. The generation must match the handle
    // returned at registration: a stale handle cannot unregister a
    // re-registered (live) subscriber.
    //
    // Normal unregister follows the same teardown protocol as lease eviction:
    // ACTIVE -> EVICTING first blocks new Poll/Ack/Heartbeat and prevents ID
    // reuse; only then is the membership bit removed and every outstanding ACK
    // responsibility drained. The slot returns to FREE last, so a new
    // generation can never overlap cleanup of the old one (design doc 9.6 /
    // 12.2: generation-bound removal).
    Status UnregisterSubscriber(SubscriberId id, uint64_t generation) noexcept {
        if (id.value >= kMaxSubscribers) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "subscriber id out of range");
        }
        SubscriberSlot& sub = subs_[id.value];
        if (sub.generation.load(std::memory_order_acquire) != generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber generation mismatch (stale handle)");
        }
        uint32_t expected = static_cast<uint32_t>(SubscriberState::kActive);
        if (!sub.state.compare_exchange_strong(
                expected, static_cast<uint32_t>(SubscriberState::kEvicting),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber is not registered");
        }
        const Status borrow_cleanup =
            ClearDeadBorrow(SubscriberHandle{id, generation});
        if (!borrow_cleanup.ok()) {
            sub.state.store(static_cast<uint32_t>(SubscriberState::kActive),
                            std::memory_order_release);
            return borrow_cleanup;
        }
        HelpDropTransaction();
        if (sub.drop_claim.load(std::memory_order_acquire) != 0) {
            sub.state.store(static_cast<uint32_t>(SubscriberState::kActive),
                            std::memory_order_release);
            return Status::Error(StatusCode::kWouldBlock,
                                 "subscriber participates in a Drop transaction");
        }

        RemoveMembershipBit(id.value);
        ClearStaleAcks(id);
        sub.heartbeat_ns.store(0, std::memory_order_relaxed);
        sub.state.store(static_cast<uint32_t>(SubscriberState::kFree),
                        std::memory_order_release);
        return Status::Ok();
    }

    // Clears a crashed Borrow only when its exact ProcessIdentity is proven
    // dead. kAlive/kUnknown remain blocked. The record is additionally bound to
    // subscriber generation and channel lease epoch, so stale recovery cannot
    // clear a reused subscriber incarnation.
    Status ClearDeadBorrow(SubscriberHandle handle) noexcept {
        if (handle.id.value >= kMaxSubscribers) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber id out of range");
        }
        SubscriberSlot& sub = subs_[handle.id.value];
        if (sub.generation.load(std::memory_order_acquire) !=
            handle.generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber generation mismatch");
        }
        for (;;) {
            const uint64_t control =
                sub.borrow_control.load(std::memory_order_acquire);
            if (control == 0) {
                return Status::Ok();
            }
            if (ProtocolPhase(control) == kBorrowClaiming) {
                uint64_t expected = control;
                sub.borrow_control.compare_exchange_strong(
                    expected, 0, std::memory_order_acq_rel,
                    std::memory_order_acquire);
                continue;
            }
            if (ProtocolPhase(control) != kBorrowActive ||
                sub.borrow_generation.load(std::memory_order_acquire) !=
                    handle.generation ||
                sub.borrow_lease_epoch.load(std::memory_order_acquire) !=
                    sub.lease_epoch.load(std::memory_order_acquire)) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "Borrow binding is not safely recoverable");
            }
            const ProcessIdentity owner = LoadBorrowOwner(sub);
            if (ProbeProcessIdentity(owner) != ProcessIdentityLiveness::kDead) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "Borrow owner is alive or liveness is unknown");
            }
            uint64_t expected = control;
            if (sub.borrow_control.compare_exchange_strong(
                    expected, 0, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return Status::Ok();
            }
        }
    }

    // Clears this subscriber's outstanding responsibilities by walking its
    // cursor to the publisher head. Each step first CASes the cursor into a
    // sequence-bound cleanup token, then clears that exact era's bit, then
    // publishes the next stable cursor. Any process (including the publisher)
    // can finish a token left by a crash, so cleanup cannot wedge the ring.
    uint64_t ClearStaleAcks(SubscriberId id) noexcept {
        if (id.value >= kMaxSubscribers) {
            return 0;
        }
        SubscriberSlot& sub = subs_[id.value];
        const uint64_t generation =
            sub.generation.load(std::memory_order_acquire);
        if (!ClearDeadBorrow(SubscriberHandle{id, generation}).ok()) {
            return 0;
        }
        HelpDropTransaction();
        if (sub.drop_claim.load(std::memory_order_acquire) != 0) {
            return 0;
        }
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_acquire);
        uint64_t cleared = 0;
        for (;;) {
            uint64_t cursor = LoadCursorAndHelp(id.value);
            if (cursor >= prod) {
                break;
            }
            uint64_t expected = cursor;
            if (!sub.cursor.compare_exchange_weak(
                    expected, CursorCleanupToken(cursor),
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                continue;
            }
            if (FinishCursorCleanup(id.value, cursor)) {
                ++cleared;
            }
        }
        CollectGarbage();
        return cleared;
    }

    // Monotonic clock in nanoseconds for lease bookkeeping (D2-06). The
    // timestamp never enters the SHM ABI as a cross-process absolute value:
    // eviction only compares heartbeat_ns against a now_ns supplied by the
    // caller, and the Coordinator layer (D2-08) owns the authoritative time
    // source. Same rationale as MpscChannel::MonotonicNowNs: durations, not
    // wall-clock agreement, are what matters.
    static uint64_t MonotonicNowNs() noexcept {
        const auto now = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }

    // Renews the subscriber's lease (D2-06, design doc 12.2 SubscriberLease).
    // `now_ns` must come from the same monotonic clock family as the now_ns
    // passed to RegisterSubscriber/EvictStaleSubscribers (see
    // MonotonicNowNs()). The timestamp is tagged with the handle generation:
    // if a stale Heartbeat races eviction and writes after ID reuse, the new
    // generation ignores it rather than inheriting the old lease proof.
    //
    // Errors:
    //   kNotFound : the subscriber is not registered or the generation does
    //               not match (a stale handle must never renew a
    //               re-registered subscriber's lease — the eviction cleanup
    //               is generation-bound, so its liveness proof is too).
    Status Heartbeat(SubscriberHandle sub, uint64_t now_ns) noexcept {
        if (sub.id.value >= kMaxSubscribers) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber id out of range");
        }
        SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SubscriberState::kActive) ||
            ss.generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(
                StatusCode::kNotFound,
                "subscriber not registered or stale generation");
        }
        ss.heartbeat_ns.store(now_ns, std::memory_order_relaxed);
        ss.heartbeat_generation.store(sub.generation,
                                      std::memory_order_release);
        // Eviction may have won immediately after our first validation. In
        // that case this heartbeat is rejected; its generation tag prevents
        // the late stores above from becoming valid for a future incarnation.
        if (ss.state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SubscriberState::kActive) ||
            ss.generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(
                StatusCode::kNotFound,
                "subscriber was evicted while renewing its lease");
        }
        return Status::Ok();
    }

    // Lease-expiry eviction orchestration (D2-06; design doc 12.2 steps
    // 2/3/4/7 landed at the channel layer). Scans all kMaxSubscribers slots
    // and evicts every subscriber whose state is kActive and whose heartbeat
    // is at least `lease_ns` old relative to `now_ns`. Returns the number of
    // subscribers evicted. Idempotent; safe to run concurrently with
    // publishers, subscribers and other scanners (CAS arbitration
    // throughout).
    //
    // Per expired slot the sequence is:
    //   a. CAS state kActive -> kEvicting (12.2 step 2). A lost CAS means a
    //      concurrent Unregister/eviction already owns the transition: skip.
    //   b. Record the slot's generation. The cleanup below is bound to the
    //      exact registration being evicted, so a late Heartbeat/Ack quoting
    //      a stale handle can never disturb a subscriber that re-registers
    //      into this slot afterwards (12.2: Lease 失效清理必须校验
    //      Generation).
    //   c. Clear the membership bit and bump set_version (12.2 step 3): new
    //      publishes stop carrying its ack responsibility, and the full
    //      check (MinActiveCursor) immediately stops counting its frozen
    //      cursor. Done BEFORE ClearStaleAcks so no publish between (c) and
    //      (d) can re-arm its bit.
    //   d. ClearStaleAcks(id) (12.2 step 4): drain its outstanding ack bits
    //      in the current window so fully-acked slots can retire.
    //   e. state -> kFree: the eviction is complete and the slot may be
    //      reused. Design doc 12.2 step 7's EVICTED terminal state is a
    //      Coordinator-level (D2-08) record; at the channel layer the slot
    //      simply returns to kFree.
    //
    // A paused-but-alive subscriber (fresh heartbeat) is never evicted here.
    // Unlike MPSC crash recovery (design doc 9.5: never judge a crash by
    // timeout alone), the broadcast channel deliberately decides on the
    // heartbeat alone: process-liveness revalidation (12.2 step 1) is layered
    // above in the D2-08 Coordinator, which chooses when (and whether) to
    // call this scan. The channel only enforces the lease arithmetic.
    uint64_t EvictStaleSubscribers(uint64_t now_ns, uint64_t lease_ns) noexcept {
        uint64_t evicted = 0;
        for (uint32_t id = 0; id < kMaxSubscribers; ++id) {
            SubscriberSlot& sub = subs_[id];
            if (sub.state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SubscriberState::kActive)) {
                continue;
            }
            const uint64_t generation =
                sub.generation.load(std::memory_order_acquire);
            const uint64_t heartbeat_generation =
                sub.heartbeat_generation.load(std::memory_order_acquire);
            const uint64_t heartbeat =
                sub.heartbeat_ns.load(std::memory_order_acquire);
            // A generation mismatch means a late stale-heartbeat write raced
            // ID reuse. Conservatively keep the subscriber: only a heartbeat
            // explicitly authored by this generation may drive its eviction.
            if (heartbeat_generation != generation ||
                now_ns - heartbeat < lease_ns) {
                continue;
            }
            // (a) Claim the transition. acq_rel arbitrates with a concurrent
            // Unregister and makes Poll/Ack/Heartbeat reject new work.
            uint32_t expected = static_cast<uint32_t>(SubscriberState::kActive);
            if (!sub.state.compare_exchange_strong(
                    expected,
                    static_cast<uint32_t>(SubscriberState::kEvicting),
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                continue;
            }
            // Heartbeat may have renewed immediately before our CAS but after
            // the first read. Re-check while owning kEvicting: if that same
            // generation published a fresh timestamp, hand the slot back to
            // kActive and let it live. A Heartbeat that starts after our CAS
            // observes kEvicting and fails, so this is the final race window.
            const uint64_t claimed_heartbeat_generation =
                sub.heartbeat_generation.load(std::memory_order_acquire);
            const uint64_t claimed_heartbeat =
                sub.heartbeat_ns.load(std::memory_order_acquire);
            if (claimed_heartbeat_generation != generation ||
                now_ns - claimed_heartbeat < lease_ns) {
                sub.state.store(static_cast<uint32_t>(SubscriberState::kActive),
                                std::memory_order_release);
                continue;
            }
            if (!ClearDeadBorrow(SubscriberHandle{SubscriberId{id}, generation})
                     .ok()) {
                sub.state.store(static_cast<uint32_t>(SubscriberState::kActive),
                                std::memory_order_release);
                continue;
            }
            HelpDropTransaction();
            if (sub.drop_claim.load(std::memory_order_acquire) != 0) {
                sub.state.store(static_cast<uint32_t>(SubscriberState::kActive),
                                std::memory_order_release);
                continue;
            }
            // (b) The cleanup below is bound to the exact registration being
            // evicted without needing to re-read the generation: holding
            // kEvicting blocks re-registration (the Register CAS requires
            // kFree), and every subscriber-facing entry point (Poll / Ack /
            // Heartbeat) validates the generation, so a stale handle of the
            // evicted registration can never renew or ack into a subscriber
            // that re-registers this slot later (12.2: Lease 失效清理必须校验
            // Generation).
            // (c) Drop the membership bit before touching ack bits: the
            // publisher snapshots membership at Commit, so from the next
            // publish on this subscriber owes nothing. Same CAS + release
            // bump discipline as UnregisterSubscriber.
            RemoveMembershipBit(id);
            // (d) Drain its outstanding ack responsibility and retire every
            // slot that was waiting only on it.
            ClearStaleAcks(SubscriberId{id});
            // (e) Eviction complete: back to kFree for reuse. Release so the
            // membership/bit cleanup above is visible to whoever re-registers
            // the slot (its Register CAS acquires).
            sub.heartbeat_ns.store(0, std::memory_order_relaxed);
            sub.state.store(static_cast<uint32_t>(SubscriberState::kFree),
                            std::memory_order_release);
            ++evicted;
        }
        return evicted;
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
              dropped_messages_(other.dropped_messages_),
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
                dropped_messages_ = other.dropped_messages_;
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
        uint64_t dropped_messages() const noexcept { return dropped_messages_; }

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
                    uint64_t sequence, uint64_t dropped_messages = 0) noexcept
            : channel_(channel), slot_(slot), sequence_(sequence),
              dropped_messages_(dropped_messages), active_(true) {}

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
        uint64_t dropped_messages_ = 0;
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
        if (prod >= (kCursorCleanupBit >> 1) - 1) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "broadcast sequence space exhausted");
        }
        HelpDropTransaction();

        uint64_t dropped_messages = 0;
        uint64_t last_dropped_era = 0;
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
                    // Tied slowest cursors may all owe the oldest era. Advance
                    // one exact era at a time, and fail conservatively if no
                    // active generation owns a valid, published candidate.
                    while (IsFullAt(prod)) {
                        Result<uint64_t> dropped = ForceDropOldest(prod);
                        if (!dropped.ok()) {
                            return dropped.status();
                        }
                        if (*dropped != 0 && *dropped != last_dropped_era) {
                            last_dropped_era = *dropped;
                            ++dropped_messages;
                        }
                    }
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

        MINO_RETURN_IF_ERROR(EnsureReusableSlot(prod));
        IndexSlot* slot = &slots_[prod & mask_];
        // The channel was not full, so every active subscriber cursor is
        // already past this physical slot's previous occupant (or there are
        // no active subscribers). Assign the logical sequence now: it IS the
        // publisher position, making each subscriber's ABA check
        // (slot.sequence_num == its cursor) exact across wraps (INV-01).
        // Help any subscriber that died after claiming ACK cleanup for this
        // exact physical era. The token is recoverable, so this can never wait
        // for the originating process. GC does not own SlotState in layout v4.
        HelpAllCursorCleanups();
        uint32_t expected = slot->state.load(std::memory_order_acquire);
        while (!slot->state.compare_exchange_weak(
            expected, static_cast<uint32_t>(SlotState::kWriting),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        }
        era_metas_[prod & mask_].payload_era.store(0,
                                                   std::memory_order_release);
        slot->sequence_num.store(prod, std::memory_order_relaxed);
        return Reservation(this, slot, prod, dropped_messages);
    }

    // Non-blocking reservation attempt: kWouldBlock if the channel is full.
    // Mirrors the SpscChannel TryReserve surface.
    Result<Reservation> TryReserve() noexcept {
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_relaxed);
        if (prod >= (kCursorCleanupBit >> 1) - 1) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "broadcast sequence space exhausted");
        }
        HelpDropTransaction();
        if (IsFullAt(prod)) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast channel full");
        }
        MINO_RETURN_IF_ERROR(EnsureReusableSlot(prod));
        IndexSlot* slot = &slots_[prod & mask_];
        // Cursor cleanup and GC are both helpable/non-exclusive in layout v4,
        // so a stale kRetiring value from a crashed legacy-style operation is
        // simply reusable rather than a reason to block forever.
        HelpAllCursorCleanups();
        uint32_t expected = slot->state.load(std::memory_order_acquire);
        while (!slot->state.compare_exchange_weak(
            expected, static_cast<uint32_t>(SlotState::kWriting),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        }
        era_metas_[prod & mask_].payload_era.store(0,
                                                   std::memory_order_release);
        slot->sequence_num.store(prod, std::memory_order_relaxed);
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
              subscriber_(other.subscriber_),
              snapshot_(other.snapshot_),
              active_(other.active_) {
            other.channel_ = nullptr;
            other.active_ = false;
        }
        Borrow& operator=(Borrow&& other) noexcept {
            if (this != &other) {
                ReleaseIfActive();
                channel_ = other.channel_;
                subscriber_ = other.subscriber_;
                snapshot_ = other.snapshot_;
                active_ = other.active_;
                other.channel_ = nullptr;
                other.active_ = false;
            }
            return *this;
        }

        ~Borrow() { ReleaseIfActive(); }

        const IndexSlotSnapshot* slot() const noexcept { return &snapshot_; }
        const IndexSlotSnapshot* operator->() const noexcept {
            return &snapshot_;
        }
        const IndexSlotSnapshot& operator*() const noexcept {
            return snapshot_;
        }
        bool active() const noexcept { return active_; }

        // Clears this subscriber generation's ack bit on the exact borrowed
        // slot era, advances the subscriber cursor past it and retires
        // fully-acked slots. If kDropOldest overtook the message while it was
        // borrowed, Ack reports kNotFound without moving the cursor or clearing
        // any bit; stale Borrows have no cleanup authority.
        // After Ack (either outcome) the Borrow is empty.
        Status Ack() && {
            if (!active_) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "borrow is not active");
            }
            active_ = false;
            BroadcastChannel* ch = channel_;
            channel_ = nullptr;
            const Status status =
                ch->AckSlot(subscriber_, snapshot_.sequence_num);
            ch->ReleaseBorrowClaim(subscriber_, snapshot_.sequence_num);
            return status;
        }

    private:
        friend class BroadcastChannel;
        Borrow(BroadcastChannel* channel, SubscriberHandle subscriber,
               const IndexSlotSnapshot& snapshot) noexcept
            : channel_(channel), subscriber_(subscriber), snapshot_(snapshot),
              active_(true) {}

        void ReleaseIfActive() noexcept {
            if (active_) {
                active_ = false;
                channel_->ReleaseBorrowClaim(subscriber_,
                                             snapshot_.sequence_num);
                channel_ = nullptr;
            }
        }

        BroadcastChannel* channel_ = nullptr;
        SubscriberHandle subscriber_;
        IndexSlotSnapshot snapshot_;
        bool active_ = false;
    };

    // Polls the next message for subscriber `sub`. Returns:
    //   an active Borrow : a message is available (state kReady, sequence
    //                      matches, CRC verified).
    //   kWouldBlock      : the subscriber has caught up with the publisher.
    //   kNotFound        : the subscriber is not registered or the
    //                      generation does not match (stale handle).
    //   kDegraded        : DropOldest advanced this generation; LastGap() and
    //                      GetSubscriberStats() expose the affected era/counts.
    //   kCorruption      : a slot failed its sequence or CRC check; the
    //                      subscriber's cursor was advanced past it so the
    //                      channel keeps making progress.
    //
    // Aborted tombstones are transparently skipped (ACK metadata carries no
    // delivery obligation for them, so only the cursor advances).
    Result<Borrow> Poll(
        SubscriberHandle sub,
        const ProcessIdentity& borrower = ProcessIdentity::Current()) noexcept {
        if (sub.id.value >= kMaxSubscribers) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber id out of range");
        }
        SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SubscriberState::kActive) ||
            ss.generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(
                StatusCode::kNotFound,
                "subscriber not registered or stale generation");
        }
        HelpDropTransaction();
        if (ConsumePendingGap(sub)) {
            return Status::Error(
                StatusCode::kDegraded,
                "broadcast subscriber observed a DropOldest gap");
        }
        while (true) {
            HelpDropTransaction();
            if (ConsumePendingGap(sub)) {
                return Status::Error(
                    StatusCode::kDegraded,
                    "broadcast subscriber observed a DropOldest gap");
            }
            // The cursor may be advanced by a committed Drop transaction. A
            // crashed ACK may also leave a helpable sequence-bound token.
            const uint64_t cons = LoadCursorAndHelp(sub.id.value);
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
                AdvanceCursorPast(sub.id.value, cons);
                continue;
            }
            if (state == static_cast<uint32_t>(SlotState::kRetired) ||
                state == static_cast<uint32_t>(SlotState::kRetiring)) {
                // Layout v4 never writes kRetiring, but an injected/recovery
                // residual must not wedge a live cursor. Likewise kRetired at
                // our cursor can only be a stale lifecycle hint. Exact sequence,
                // era publication and CRC are the authority, so deliver a valid
                // snapshot exactly like kReady.
                if (slot->sequence_num.load(std::memory_order_relaxed) == cons &&
                    era_metas_[cons & mask_].payload_era.load(
                        std::memory_order_acquire) == cons + 1) {
                    if (!TryClaimBorrow(sub, cons, borrower)) {
                        continue;
                    }
                    IndexSlotSnapshot snapshot = SnapshotIndexSlot(*slot);
                    if (VerifySnapshotCrc(snapshot)) {
                        return Borrow(this, sub, snapshot);
                    }
                    ReleaseBorrowClaim(sub, cons);
                }
                // Genuinely stale or corrupt: skip like corruption below.
                AdvanceCursorPast(sub.id.value, cons);
                return Status::Error(
                    StatusCode::kCorruption,
                    "broadcast slot has stale lifecycle state under a live cursor (skipped)");
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
                AdvanceCursorPast(sub.id.value, cons);
                return Status::Error(
                    StatusCode::kCorruption,
                    "broadcast slot sequence mismatch (skipped)");
            }
            // Claim the borrow before copying the header. DropOldest uses this
            // claim as its exclusion fence; copying first would let it recycle
            // the slot while SnapshotIndexSlot reads the old era.
            if (!TryClaimBorrow(sub, cons, borrower)) {
                continue;
            }
            IndexSlotSnapshot snapshot = SnapshotIndexSlot(*slot);
            if (!VerifySnapshotCrc(snapshot)) {
                ReleaseBorrowClaim(sub, cons);
                AdvanceCursorPast(sub.id.value, cons);
                return Status::Error(
                    StatusCode::kCorruption,
                    "broadcast slot immutable CRC mismatch (skipped)");
            }
            return Borrow(this, sub, snapshot);
        }
    }

    // Returns a later message while the caller retains the active head Borrow
    // returned by Poll(). That head's recoverable claim prevents DropOldest
    // from advancing this subscriber while the later slot is snapshotted. The
    // later Borrow intentionally owns no additional shared claim; runtime pins
    // each payload before exposing it, and ACKs the batch in sequence order.
    // This preserves the layout-v6 ABI and its single per-subscriber claim.
    Result<Borrow> PollAtOffsetWhileHeadBorrowed(
        SubscriberHandle sub, uint64_t offset) noexcept {
        if (offset == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "broadcast batch offset must be non-zero");
        }
        if (!IsActiveGeneration(sub)) {
            return Status::Error(
                StatusCode::kNotFound,
                "subscriber not registered or stale generation");
        }
        SubscriberSlot& ss = subs_[sub.id.value];
        const uint64_t cons = LoadCursorAndHelp(sub.id.value);
        const uint64_t head_claim = ProtocolToken(cons, kBorrowActive);
        if (ss.borrow_control.load(std::memory_order_acquire) != head_claim ||
            ss.borrow_generation.load(std::memory_order_acquire) !=
                sub.generation) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "broadcast batch requires an active head Borrow");
        }
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_acquire);
        if (offset >= prod - cons) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast batch reached channel tail");
        }
        const uint64_t sequence = cons + offset;
        const uint64_t phys = sequence & mask_;
        IndexSlot* slot = &slots_[phys];
        const uint32_t state = slot->state.load(std::memory_order_acquire);
        if (state != static_cast<uint32_t>(SlotState::kReady) &&
            state != static_cast<uint32_t>(SlotState::kRetired) &&
            state != static_cast<uint32_t>(SlotState::kRetiring)) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast batch encountered a non-ready slot");
        }
        if (slot->sequence_num.load(std::memory_order_acquire) != sequence ||
            era_metas_[phys].payload_era.load(std::memory_order_acquire) !=
                sequence + 1 ||
            era_metas_[phys].ack_era[sub.id.value].load(
                std::memory_order_acquire) != sequence + 1) {
            return Status::Error(StatusCode::kCorruption,
                                 "broadcast batch slot era mismatch");
        }
        IndexSlotSnapshot snapshot = SnapshotIndexSlot(*slot);
        if (!VerifySnapshotCrc(snapshot)) {
            return Status::Error(StatusCode::kCorruption,
                                 "broadcast batch slot immutable CRC mismatch");
        }
        if (ss.borrow_control.load(std::memory_order_acquire) != head_claim) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast head Borrow claim changed");
        }
        return Borrow(this, sub, snapshot);
    }

    // Standalone Ack for a previously polled sequence: validates the exact
    // subscriber generation and slot era before clearing that generation's
    // bit. Only `seq == cursor` may claim cleanup and advance. A `seq` behind
    // the cursor (overtaken by kDropOldest) reports kNotFound and never clears
    // an old or recycled era's bit.
    Status Ack(SubscriberHandle sub, uint64_t seq) noexcept {
        return AckSlot(sub, seq);
    }

    Result<Gap> LastGap(SubscriberHandle sub) const noexcept {
        if (!IsActiveGeneration(sub)) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber not registered or stale generation");
        }
        const SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.gap_generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber generation has no Gap state");
        }
        const uint64_t events = ss.gap_events.load(std::memory_order_acquire);
        if (events == 0) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber has not observed a Gap");
        }
        Gap gap{
            .subscriber_id = sub.id,
            .generation = sub.generation,
            .first_sequence =
                ss.latest_gap_first_sequence.load(std::memory_order_acquire),
            .next_sequence =
                ss.latest_gap_next_sequence.load(std::memory_order_acquire),
            .total_events = events,
            .total_messages =
                ss.gap_messages.load(std::memory_order_acquire),
        };
        if (!IsActiveGeneration(sub) ||
            ss.gap_generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber generation changed while reading Gap");
        }
        return gap;
    }

    Result<SubscriberStats> GetSubscriberStats(
        SubscriberHandle sub) const noexcept {
        if (!IsActiveGeneration(sub)) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber not registered or stale generation");
        }
        const SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.gap_generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber generation has no statistics");
        }
        SubscriberStats stats{
            .gap_events = ss.gap_events.load(std::memory_order_acquire),
            .gap_messages = ss.gap_messages.load(std::memory_order_acquire),
        };
        if (!IsActiveGeneration(sub) ||
            ss.gap_generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber generation changed while reading statistics");
        }
        return stats;
    }

    // Installs a process-local notification invoked by the facade that wins the
    // exact retired-era token. The claim is persisted before invocation: a
    // process crash may lose that notification, but can neither duplicate it nor
    // leave a lock that blocks publisher reuse. The callback receives a coherent
    // local copy from the atomic payload-era sidecar, never live IndexSlot data.
    void SetPayloadRetireObserver(PayloadRetireObserver observer,
                                  void* context) noexcept {
        payload_retire_observer_ = observer;
        payload_retire_context_ = context;
    }

    void SetRetirePersistenceHookForTesting(
        RetirePersistenceHook hook, void* context = nullptr) noexcept {
        retire_persistence_hook_ = hook;
        retire_persistence_context_ = context;
    }

    // -----------------------------------------------------------------------
    // Garbage collection
    // -----------------------------------------------------------------------

    // Scans the bounded live window and claims every fully-ACKed logical era.
    // Retirement is represented by BroadcastEraMeta::retired_era rather than a
    // transient SlotState. GC therefore never excludes the publisher and cannot
    // strand the channel if its process dies. The 32-bit SlotState remains a
    // publication-phase hint; exact retirement authority is the 64-bit era.
    void CollectGarbage() noexcept {
        const uint64_t prod =
            control_->publisher_cursor.load(std::memory_order_acquire);
        const uint64_t floor = prod > capacity_ ? prod - capacity_ : 0;
        for (uint64_t seq = floor; seq < prod; ++seq) {
            const uint64_t phys = seq & mask_;
            IndexSlot* slot = &slots_[phys];
            const uint32_t state = slot->state.load(std::memory_order_acquire);
            if (state != static_cast<uint32_t>(SlotState::kReady) &&
                state != static_cast<uint32_t>(SlotState::kRetired) &&
                state != static_cast<uint32_t>(SlotState::kRetiring)) {
                continue;
            }
            if (slot->sequence_num.load(std::memory_order_acquire) != seq ||
                !AllAckedForEra(phys, seq)) {
                continue;
            }

            BroadcastEraMeta& era = era_metas_[phys];
            const uint64_t token = seq + 1;
            if (era.payload_era.load(std::memory_order_acquire) != token) {
                continue;
            }
            const uint64_t offset =
                era.payload_offset.load(std::memory_order_acquire);
            const uint64_t identity =
                era.payload_identity.load(std::memory_order_acquire);
            if (era.payload_era.load(std::memory_order_acquire) != token) {
                continue;
            }

            if (era.retired_era.load(std::memory_order_acquire) >= token) {
                continue;
            }

            // Claim the logical era before invoking the potentially blocking
            // observer. This makes slot retirement visible immediately and
            // keeps publisher reuse independent from callback completion. A
            // crash after the claim is safe: the observer is at-most-once and
            // recovery can use the persisted retired era as its fence.
            uint64_t retired =
                era.retired_era.load(std::memory_order_acquire);
            while (retired < token &&
                   !era.retired_era.compare_exchange_weak(
                       retired, token, std::memory_order_acq_rel,
                       std::memory_order_acquire)) {
            }
            if (retired >= token) {
                continue;
            }
            if (retire_persistence_hook_ != nullptr) {
                retire_persistence_hook_(seq, retire_persistence_context_);
            }

            const ShmHandle payload{
                .offset = offset,
                .generation = static_cast<uint32_t>(identity >> 32),
                .region_id = static_cast<uint32_t>(identity)};
            if (payload_retire_observer_ != nullptr && !payload.IsNull()) {
                static_cast<void>(payload_retire_observer_(
                    payload, payload_retire_context_));
            }
        }
    }

    Status EnsureReusableSlot(uint64_t sequence) noexcept {
        if (sequence < capacity_) {
            return Status::Ok();
        }
        const uint64_t previous = sequence - capacity_;
        BroadcastEraMeta& era = era_metas_[sequence & mask_];
        const uint64_t token = previous + 1;
        const uint64_t payload_era =
            era.payload_era.load(std::memory_order_acquire);
        if (payload_era == 0) {
            return Status::Ok();
        }
        if (payload_era != token) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast physical slot has an unexpected era");
        }
        if (era.retired_era.load(std::memory_order_acquire) >= token) {
            return Status::Ok();
        }
        CollectGarbage();
        return era.retired_era.load(std::memory_order_acquire) >= token
                   ? Status::Ok()
                   : Status::Error(
                         StatusCode::kWouldBlock,
                         payload_retire_observer_ == nullptr
                             ? "broadcast payload retirement has no observer"
                             : "broadcast payload retirement is pending retry");
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
                     BroadcastSlotMeta* metas, BroadcastEraMeta* era_metas,
                     SubscriberSlot* subs, uint64_t capacity) noexcept
        : control_(control),
          slots_(slots),
          metas_(metas),
          era_metas_(era_metas),
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

    static BroadcastEraMeta* EraMetasOf(void* shm_base,
                                        uint64_t capacity) noexcept {
        return reinterpret_cast<BroadcastEraMeta*>(
            static_cast<unsigned char*>(shm_base) + EraMetasOffset(capacity));
    }

    static SubscriberSlot* SubsOf(void* shm_base, uint64_t capacity) noexcept {
        return reinterpret_cast<SubscriberSlot*>(
            static_cast<unsigned char*>(shm_base) + SubsOffset(capacity));
    }

    static bool IsCursorCleanupToken(uint64_t cursor) noexcept {
        return (cursor & kCursorCleanupBit) != 0;
    }

    static uint64_t CursorSequence(uint64_t cursor) noexcept {
        return cursor & ~kCursorCleanupBit;
    }

    static uint64_t CursorCleanupToken(uint64_t sequence) noexcept {
        return sequence | kCursorCleanupBit;
    }

    static constexpr uint64_t kProtocolPhaseMask = 0x3;
    static constexpr uint64_t kBorrowClaiming = 1;
    static constexpr uint64_t kBorrowActive = 2;
    static constexpr uint64_t kDropPrepared = 1;
    static constexpr uint64_t kDropCommitting = 2;

    static uint64_t ProtocolToken(uint64_t sequence,
                                  uint64_t phase) noexcept {
        return ((sequence + 1) << 2) | phase;
    }

    static uint64_t ProtocolPhase(uint64_t control) noexcept {
        return control & kProtocolPhaseMask;
    }

    static uint64_t ProtocolSequence(uint64_t control) noexcept {
        return (control >> 2) - 1;
    }

    static ProcessIdentity LoadBorrowOwner(
        const SubscriberSlot& sub) noexcept {
        return ProcessIdentity{
            .node_id = sub.borrow_owner_node_id.load(std::memory_order_acquire),
            .process_id =
                sub.borrow_owner_process_id.load(std::memory_order_acquire),
            .process_epoch =
                sub.borrow_owner_process_epoch.load(std::memory_order_acquire),
            .start_time_ns =
                sub.borrow_owner_start_time_ns.load(std::memory_order_acquire),
        };
    }

    // Completes exactly one claimed cursor era. The exact sequence and atomic
    // payload-era stamp jointly prevent an old helper from touching a recycled
    // bitmap. Publishing the stable next cursor is last.
    bool FinishCursorCleanup(uint32_t id, uint64_t sequence) const noexcept {
        const uint64_t token = CursorCleanupToken(sequence);
        if (subs_[id].cursor.load(std::memory_order_acquire) != token) {
            return false;
        }
        const uint64_t phys = sequence & mask_;
        bool cleared = false;
        if (slots_[phys].sequence_num.load(std::memory_order_acquire) == sequence &&
            era_metas_[phys].payload_era.load(std::memory_order_acquire) ==
                sequence + 1) {
            uint64_t expected_era = sequence + 1;
            cleared = era_metas_[phys].ack_era[id].compare_exchange_strong(
                expected_era, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
        uint64_t expected = token;
        subs_[id].cursor.compare_exchange_strong(
            expected, sequence + 1, std::memory_order_acq_rel,
            std::memory_order_acquire);
        return cleared;
    }

    uint64_t LoadCursorAndHelp(uint32_t id) const noexcept {
        for (;;) {
            const uint64_t cursor =
                subs_[id].cursor.load(std::memory_order_acquire);
            if (!IsCursorCleanupToken(cursor)) {
                return cursor;
            }
            FinishCursorCleanup(id, CursorSequence(cursor));
        }
    }

    void HelpAllCursorCleanups() const noexcept {
        for (uint32_t id = 0; id < kMaxSubscribers; ++id) {
            LoadCursorAndHelp(id);
        }
    }

    bool AllAckedForEra(uint64_t phys, uint64_t sequence) const noexcept {
        const uint64_t token = sequence + 1;
        for (uint32_t id = 0; id < kMaxSubscribers; ++id) {
            if (era_metas_[phys].ack_era[id].load(
                    std::memory_order_acquire) == token) {
                return false;
            }
        }
        return true;
    }

    void AdvanceCursorPast(uint32_t id, uint64_t sequence) const noexcept {
        uint64_t expected = sequence;
        if (subs_[id].cursor.compare_exchange_strong(
                expected, CursorCleanupToken(sequence),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            FinishCursorCleanup(id, sequence);
        } else if (IsCursorCleanupToken(expected)) {
            FinishCursorCleanup(id, CursorSequence(expected));
        }
    }

    // Removes one subscriber from future publication snapshots and advances
    // the logical set version. Callers must first own that SubscriberSlot in
    // kEvicting, which prevents generation reuse until ACK cleanup completes.
    void RemoveMembershipBit(uint32_t id) noexcept {
        uint64_t membership =
            control_->current_membership.load(std::memory_order_acquire);
        while (!control_->current_membership.compare_exchange_weak(
            membership, membership & ~(uint64_t{1} << id),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        }
        control_->set_version.fetch_add(1, std::memory_order_acq_rel);
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
            const uint64_t cursor = LoadCursorAndHelp(id);
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
            const uint64_t cursor = LoadCursorAndHelp(id);
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
        // Preserve the publication membership snapshot in the legacy compact
        // bitmap; v4's mutable cleanup authority is ack_era below.
        metas_[phys].ack_bitmap.bits.store(snap.membership,
                                           std::memory_order_relaxed);
        BroadcastEraMeta& era = era_metas_[phys];
        const uint64_t ack_token = sequence + 1;
        for (uint32_t id = 0; id < kMaxSubscribers; ++id) {
            const bool owed = (snap.membership & (uint64_t{1} << id)) != 0;
            era.ack_era[id].store(owed ? ack_token : 0,
                                  std::memory_order_relaxed);
        }
        era.payload_offset.store(slot->payload.offset,
                                 std::memory_order_relaxed);
        const uint64_t identity =
            (static_cast<uint64_t>(slot->payload.generation) << 32) |
            static_cast<uint64_t>(slot->payload.region_id);
        era.payload_identity.store(identity, std::memory_order_relaxed);
        // Release-publish the coherent payload copy before READY/cursor make
        // this logical era visible. sequence + 1 reserves zero as invalid.
        era.payload_era.store(sequence + 1, std::memory_order_release);
        SealIndexSlotImmutableCrc(*slot);
        slot->state.store(static_cast<uint32_t>(SlotState::kReady),
                          std::memory_order_release);
        // Advance the publisher cursor: this is what makes the slot visible
        // to every subscriber's Poll.
        control_->publisher_cursor.store(sequence + 1,
                                         std::memory_order_release);
        return Status::Ok();
    }

    // Stamps an ABORTED tombstone and advances the publisher cursor. All ACK
    // metadata is cleared: an aborted message has no delivery obligation.
    Status AbortSlot(IndexSlot* slot) noexcept {
        const uint64_t sequence =
            control_->publisher_cursor.load(std::memory_order_relaxed);
        const uint64_t phys = sequence & mask_;
        metas_[phys].ack_bitmap.bits.store(0, std::memory_order_relaxed);
        for (uint32_t id = 0; id < kMaxSubscribers; ++id) {
            era_metas_[phys].ack_era[id].store(0,
                                               std::memory_order_relaxed);
        }
        slot->state.store(static_cast<uint32_t>(SlotState::kAborted),
                          std::memory_order_relaxed);
        control_->publisher_cursor.fetch_add(1, std::memory_order_release);
        return Status::Ok();
    }

    bool DropTargetsSequence(uint64_t control, uint32_t id,
                             uint64_t sequence) const noexcept {
        return control != 0 && ProtocolSequence(control) == sequence &&
               (control_->drop_targets.load(std::memory_order_acquire) &
                (uint64_t{1} << id)) != 0;
    }

    void AbortPreparedDrop(uint64_t control) noexcept {
        if (ProtocolPhase(control) != kDropPrepared ||
            control_->drop_control.load(std::memory_order_acquire) != control) {
            return;
        }
        const uint64_t sequence = ProtocolSequence(control);
        uint64_t targets =
            control_->drop_targets.load(std::memory_order_acquire);
        while (targets != 0) {
            const uint32_t id = static_cast<uint32_t>(__builtin_ctzll(targets));
            targets &= targets - 1;
            uint64_t expected = sequence + 1;
            subs_[id].drop_claim.compare_exchange_strong(
                expected, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
        uint64_t expected = control;
        control_->drop_control.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    void HelpDropTransaction() noexcept {
        const uint64_t control =
            control_->drop_control.load(std::memory_order_acquire);
        if (control == 0 || ProtocolPhase(control) != kDropCommitting) {
            return;
        }
        const uint64_t sequence = ProtocolSequence(control);
        if (control_->drop_sequence.load(std::memory_order_acquire) != sequence) {
            return;
        }
        const uint64_t targets =
            control_->drop_targets.load(std::memory_order_acquire);
        const uint64_t phys = sequence & mask_;
        const uint32_t state =
            slots_[phys].state.load(std::memory_order_acquire);
        const bool tombstone =
            state == static_cast<uint32_t>(SlotState::kAborted);

        // Gap publication is the first externally visible commit phase. Every
        // target receives an idempotent absolute statistics snapshot before any
        // ACK or cursor can advance.
        uint64_t remaining = targets;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            SubscriberSlot& sub = subs_[id];
            if (sub.drop_claim.load(std::memory_order_acquire) != sequence + 1 ||
                sub.generation.load(std::memory_order_acquire) !=
                    sub.drop_generation.load(std::memory_order_acquire)) {
                return;
            }
            if (!tombstone &&
                sub.gap_committed_era.load(std::memory_order_acquire) <
                    sequence + 1) {
                sub.latest_gap_first_sequence.store(
                    sequence, std::memory_order_relaxed);
                sub.latest_gap_next_sequence.store(
                    sequence + 1, std::memory_order_relaxed);
                sub.gap_messages.store(
                    sub.drop_gap_messages.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
                sub.gap_events.store(
                    sub.drop_gap_events.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
                sub.gap_committed_era.store(sequence + 1,
                                            std::memory_order_release);
            }
        }

        // Only after all target Gaps are release-published may exact ACKs clear
        // and cursors become visible at sequence + 1.
        remaining = targets;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            uint64_t expected_era = sequence + 1;
            era_metas_[phys].ack_era[id].compare_exchange_strong(
                expected_era, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
        remaining = targets;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            uint64_t expected = sequence;
            subs_[id].cursor.compare_exchange_strong(
                expected, sequence + 1, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
        remaining = targets;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            uint64_t expected = sequence + 1;
            subs_[id].drop_claim.compare_exchange_strong(
                expected, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
        uint64_t expected = control;
        control_->drop_control.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
        CollectGarbage();
    }

    // DropOldest first preflights every subscriber tied at the minimum cursor.
    // Preparation and target claims never alter ACK/cursor/Gap state. Only the
    // single prepared->committing CAS makes the transaction helpable; completion
    // then follows Gap -> ACK -> cursor order.
    Result<uint64_t> ForceDropOldest(uint64_t prod) noexcept {
        uint64_t existing =
            control_->drop_control.load(std::memory_order_acquire);
        if (ProtocolPhase(existing) == kDropCommitting) {
            HelpDropTransaction();
            return ProtocolSequence(existing) + 1;
        }
        if (ProtocolPhase(existing) == kDropPrepared) {
            AbortPreparedDrop(existing);
        }

        const uint64_t membership =
            control_->current_membership.load(std::memory_order_acquire);
        if (membership == 0) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast has no active oldest candidate");
        }
        uint64_t min_cursor = prod;
        uint64_t remaining = membership;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            const uint64_t cursor = LoadCursorAndHelp(id);
            if (cursor < min_cursor) {
                min_cursor = cursor;
            }
        }
        if (min_cursor >= prod) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast has no lagging oldest candidate");
        }

        const uint64_t phys = min_cursor & mask_;
        IndexSlot& slot = slots_[phys];
        const uint32_t state = slot.state.load(std::memory_order_acquire);
        const bool tombstone =
            state == static_cast<uint32_t>(SlotState::kAborted);
        const bool published =
            state == static_cast<uint32_t>(SlotState::kReady) ||
            state == static_cast<uint32_t>(SlotState::kRetired) ||
            state == static_cast<uint32_t>(SlotState::kRetiring);
        if (slot.sequence_num.load(std::memory_order_acquire) != min_cursor ||
            (!tombstone &&
             (!published ||
              era_metas_[phys].payload_era.load(std::memory_order_acquire) !=
                  min_cursor + 1))) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "broadcast oldest era is not safely published");
        }

        uint64_t targets = 0;
        remaining = membership;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            SubscriberSlot& sub = subs_[id];
            if (sub.cursor.load(std::memory_order_acquire) != min_cursor) {
                continue;
            }
            const uint64_t generation =
                sub.generation.load(std::memory_order_acquire);
            const uint64_t gap_events =
                sub.gap_events.load(std::memory_order_acquire);
            const uint64_t gap_messages =
                sub.gap_messages.load(std::memory_order_acquire);
            if (sub.state.load(std::memory_order_acquire) !=
                    static_cast<uint32_t>(SubscriberState::kActive) ||
                generation == 0 ||
                sub.borrow_control.load(std::memory_order_acquire) != 0 ||
                sub.drop_claim.load(std::memory_order_acquire) != 0 ||
                (!tombstone &&
                 era_metas_[phys].ack_era[id].load(std::memory_order_acquire) !=
                     min_cursor + 1) ||
                gap_events == std::numeric_limits<uint64_t>::max() ||
                gap_messages == std::numeric_limits<uint64_t>::max()) {
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "a tied slow subscriber failed DropOldest preflight");
            }
            sub.drop_generation.store(generation, std::memory_order_relaxed);
            sub.drop_gap_events.store(gap_events + (tombstone ? 0 : 1),
                                      std::memory_order_relaxed);
            sub.drop_gap_messages.store(gap_messages + (tombstone ? 0 : 1),
                                        std::memory_order_relaxed);
            targets |= uint64_t{1} << id;
        }
        if (targets == 0 ||
            control_->current_membership.load(std::memory_order_acquire) !=
                membership) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "DropOldest membership changed during preflight");
        }

        // Full second validation closes Poll/Ack/unregister races before the
        // prepared transaction is published. No subscriber has moved yet.
        remaining = targets;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            SubscriberSlot& sub = subs_[id];
            if (sub.state.load(std::memory_order_acquire) !=
                    static_cast<uint32_t>(SubscriberState::kActive) ||
                sub.generation.load(std::memory_order_acquire) !=
                    sub.drop_generation.load(std::memory_order_relaxed) ||
                sub.cursor.load(std::memory_order_acquire) != min_cursor ||
                sub.borrow_control.load(std::memory_order_acquire) != 0 ||
                sub.drop_claim.load(std::memory_order_acquire) != 0) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "DropOldest preflight became stale");
            }
        }

        control_->drop_sequence.store(min_cursor, std::memory_order_relaxed);
        control_->drop_targets.store(targets, std::memory_order_relaxed);
        const uint64_t prepared = ProtocolToken(min_cursor, kDropPrepared);
        uint64_t idle = 0;
        if (!control_->drop_control.compare_exchange_strong(
                idle, prepared, std::memory_order_release,
                std::memory_order_acquire)) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "another Drop transaction is active");
        }

        uint64_t claimed = 0;
        remaining = targets;
        while (remaining != 0) {
            const uint32_t id =
                static_cast<uint32_t>(__builtin_ctzll(remaining));
            remaining &= remaining - 1;
            SubscriberSlot& sub = subs_[id];
            uint64_t expected_claim = 0;
            if (!sub.drop_claim.compare_exchange_strong(
                    expected_claim, min_cursor + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire) ||
                sub.state.load(std::memory_order_acquire) !=
                    static_cast<uint32_t>(SubscriberState::kActive) ||
                sub.generation.load(std::memory_order_acquire) !=
                    sub.drop_generation.load(std::memory_order_acquire) ||
                sub.cursor.load(std::memory_order_acquire) != min_cursor ||
                sub.borrow_control.load(std::memory_order_acquire) != 0) {
                uint64_t rollback = claimed;
                while (rollback != 0) {
                    const uint32_t rollback_id =
                        static_cast<uint32_t>(__builtin_ctzll(rollback));
                    rollback &= rollback - 1;
                    uint64_t token = min_cursor + 1;
                    subs_[rollback_id].drop_claim.compare_exchange_strong(
                        token, 0, std::memory_order_acq_rel,
                        std::memory_order_acquire);
                }
                if (expected_claim == 0) {
                    uint64_t token = min_cursor + 1;
                    sub.drop_claim.compare_exchange_strong(
                        token, 0, std::memory_order_acq_rel,
                        std::memory_order_acquire);
                }
                uint64_t expected_prepared = prepared;
                control_->drop_control.compare_exchange_strong(
                    expected_prepared, 0, std::memory_order_acq_rel,
                    std::memory_order_acquire);
                return Status::Error(StatusCode::kWouldBlock,
                                     "DropOldest target claim failed");
            }
            claimed |= uint64_t{1} << id;
        }

        const uint64_t committing =
            ProtocolToken(min_cursor, kDropCommitting);
        uint64_t expected_prepared = prepared;
        if (!control_->drop_control.compare_exchange_strong(
                expected_prepared, committing, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            AbortPreparedDrop(prepared);
            return Status::Error(StatusCode::kWouldBlock,
                                 "DropOldest commit authority was lost");
        }
        HelpDropTransaction();
        return tombstone ? 0 : min_cursor + 1;
    }

    // Subscriber-side ACK. Only the exact current cursor may ACK: a Borrow
    // overtaken by kDropOldest is stale authority and never clears any bit.
    // The cursor CAS installs a sequence-bound cleanup token before the bitmap
    // update. Until that token is finished, active fullness checks and Reserve's
    // help pass keep the physical era from being reused. If this process dies,
    // any observer can perform the same exact-era clear and advance the cursor.
    bool IsActiveGeneration(SubscriberHandle sub) const noexcept {
        if (sub.id.value >= kMaxSubscribers) {
            return false;
        }
        const SubscriberSlot& ss = subs_[sub.id.value];
        return ss.state.load(std::memory_order_acquire) ==
                   static_cast<uint32_t>(SubscriberState::kActive) &&
               ss.generation.load(std::memory_order_acquire) == sub.generation;
    }

    bool TryClaimBorrow(SubscriberHandle sub, uint64_t sequence,
                        const ProcessIdentity& owner) noexcept {
        if (owner.IsZero()) {
            return false;
        }
        SubscriberSlot& ss = subs_[sub.id.value];
        const uint64_t drop =
            control_->drop_control.load(std::memory_order_acquire);
        if (!IsActiveGeneration(sub) ||
            ss.cursor.load(std::memory_order_acquire) != sequence ||
            ss.drop_claim.load(std::memory_order_acquire) != 0 ||
            DropTargetsSequence(drop, sub.id.value, sequence)) {
            return false;
        }
        const uint64_t claiming = ProtocolToken(sequence, kBorrowClaiming);
        uint64_t expected = 0;
        if (!ss.borrow_control.compare_exchange_strong(
                expected, claiming, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return false;
        }
        ss.borrow_generation.store(sub.generation, std::memory_order_relaxed);
        ss.borrow_lease_epoch.store(
            ss.lease_epoch.load(std::memory_order_acquire),
            std::memory_order_relaxed);
        ss.borrow_owner_node_id.store(owner.node_id,
                                      std::memory_order_relaxed);
        ss.borrow_owner_process_id.store(owner.process_id,
                                         std::memory_order_relaxed);
        ss.borrow_owner_process_epoch.store(owner.process_epoch,
                                            std::memory_order_relaxed);
        ss.borrow_owner_start_time_ns.store(owner.start_time_ns,
                                            std::memory_order_relaxed);
        const uint64_t active = ProtocolToken(sequence, kBorrowActive);
        expected = claiming;
        if (!ss.borrow_control.compare_exchange_strong(
                expected, active, std::memory_order_release,
                std::memory_order_acquire)) {
            return false;
        }
        const uint64_t current_drop =
            control_->drop_control.load(std::memory_order_acquire);
        if (!IsActiveGeneration(sub) ||
            ss.cursor.load(std::memory_order_acquire) != sequence ||
            ss.drop_claim.load(std::memory_order_acquire) != 0 ||
            DropTargetsSequence(current_drop, sub.id.value, sequence)) {
            ReleaseBorrowClaim(sub, sequence);
            return false;
        }
        return true;
    }

    void ReleaseBorrowClaim(SubscriberHandle sub, uint64_t sequence) noexcept {
        if (sub.id.value >= kMaxSubscribers) {
            return;
        }
        SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.generation.load(std::memory_order_acquire) != sub.generation ||
            ss.borrow_generation.load(std::memory_order_acquire) !=
                sub.generation ||
            ss.borrow_lease_epoch.load(std::memory_order_acquire) !=
                ss.lease_epoch.load(std::memory_order_acquire)) {
            return;
        }
        uint64_t expected = ProtocolToken(sequence, kBorrowActive);
        ss.borrow_control.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    bool ConsumePendingGap(SubscriberHandle sub) noexcept {
        SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.gap_generation.load(std::memory_order_acquire) != sub.generation) {
            return false;
        }
        if (ss.gap_committed_era.load(std::memory_order_acquire) == 0) {
            return false;
        }
        uint64_t observed =
            ss.observed_gap_events.load(std::memory_order_acquire);
        const uint64_t events = ss.gap_events.load(std::memory_order_acquire);
        while (observed < events) {
            if (ss.observed_gap_events.compare_exchange_weak(
                    observed, events, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }


    Status AckSlot(SubscriberHandle sub, uint64_t sequence) noexcept {
        if (sub.id.value >= kMaxSubscribers) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber id out of range");
        }
        SubscriberSlot& ss = subs_[sub.id.value];
        if (ss.state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SubscriberState::kActive) ||
            ss.generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(
                StatusCode::kNotFound,
                "subscriber not registered or stale generation");
        }

        const uint64_t drop =
            control_->drop_control.load(std::memory_order_acquire);
        if (DropTargetsSequence(drop, sub.id.value, sequence)) {
            if (ProtocolPhase(drop) == kDropCommitting) {
                HelpDropTransaction();
            }
            return Status::Error(
                StatusCode::kWouldBlock,
                "message participates in a DropOldest transaction");
        }

        const uint64_t cons = LoadCursorAndHelp(sub.id.value);
        if (sequence != cons) {
            return Status::Error(
                StatusCode::kNotFound,
                "message was dropped or already acknowledged");
        }
        const uint64_t phys = sequence & mask_;
        if (control_->publisher_cursor.load(std::memory_order_acquire) <=
                sequence ||
            slots_[phys].sequence_num.load(std::memory_order_acquire) !=
                sequence ||
            era_metas_[phys].payload_era.load(std::memory_order_acquire) !=
                sequence + 1) {
            return Status::Error(StatusCode::kNotFound,
                                 "message slot is no longer ackable");
        }
        // Close the unregister/generation race before installing authority.
        if (ss.state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(SubscriberState::kActive) ||
            ss.generation.load(std::memory_order_acquire) != sub.generation) {
            return Status::Error(
                StatusCode::kNotFound,
                "subscriber not registered or stale generation");
        }

        uint64_t expected = sequence;
        if (!ss.cursor.compare_exchange_strong(
                expected, CursorCleanupToken(sequence),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            if (IsCursorCleanupToken(expected)) {
                FinishCursorCleanup(sub.id.value, CursorSequence(expected));
            }
            return Status::Error(
                StatusCode::kNotFound,
                "message was dropped or concurrently acknowledged");
        }

        FinishCursorCleanup(sub.id.value, sequence);
        CollectGarbage();
        return Status::Ok();
    }

    ControlBlock* control_ = nullptr;
    IndexSlot* slots_ = nullptr;
    BroadcastSlotMeta* metas_ = nullptr;
    BroadcastEraMeta* era_metas_ = nullptr;
    SubscriberSlot* subs_ = nullptr;
    uint64_t capacity_ = 0;
    uint64_t mask_ = 0;
    PayloadRetireObserver payload_retire_observer_ = nullptr;
    void* payload_retire_context_ = nullptr;
    RetirePersistenceHook retire_persistence_hook_ = nullptr;
    void* retire_persistence_context_ = nullptr;
};

static_assert(std::is_trivially_copyable_v<BroadcastChannel>,
              "BroadcastChannel must be a trivially copyable view");

}  // namespace mino

#endif  // MINO_SHM_CHANNEL_BROADCAST_CHANNEL_H_
