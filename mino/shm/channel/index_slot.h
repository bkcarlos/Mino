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

#ifndef MINO_SHM_CHANNEL_INDEX_SLOT_H_
#define MINO_SHM_CHANNEL_INDEX_SLOT_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "mino/abi/shm_handle.h"

namespace mino {

// ---------------------------------------------------------------------------
// IndexSlot (design doc section 9.2, ADR-0003)
// ---------------------------------------------------------------------------
//
// IndexSlot is the fixed-size control-plane record carried by every channel
// ring slot. It describes one published message: what it is (type/schema
// identity), where it lives (payload ShmHandle), and where it is in its
// lifecycle (state). Slots never carry the payload itself (reference
// semantics, design doc 9.9); consumers resolve the payload in place inside
// the Central Slab.
//
// The layout is frozen at exactly 128 bytes (two cache lines) with explicit
// padding. Any change to the field set, order, or size is an ABI break and
// requires a new layout version (ADR-0003).
//
// Field semantics (design doc 9.2):
//   msg_type              : fast-lookup ID derived by CodeGen from the low 32
//                           bits of the canonical_digest; unique within a
//                           Topic (guaranteed by the Registry). Receivers
//                           must cross-check it against the connection's
//                           schema_ref and reject on mismatch.
//   schema_version        : schema definition version, encoded as
//                           (major << 16) | minor (design doc 13.3).
//   schema_short_id       : short schema identity (ADR-0005).
//   schema_layout_version : SHM object layout version (design doc 13.6 Layout
//                           Plan version); evolves independently of
//                           schema_version.
//   sequence_num          : logical message sequence assigned at Commit.
//   timestamp_ns          : publication timestamp, nanoseconds.
//   payload               : ShmHandle of the payload slab object.
//   payload_len           : payload byte length.
//   immutable_metadata_crc: CRC over the immutable metadata only (see
//                           ImmutableFieldsView below). Covers corruption
//                           detection; it is not a concurrency or
//                           authenticity mechanism.
//   state                 : lifecycle state machine (SlotState, design doc
//                           9.3 / 9.5). Atomic; the ONLY mutable field that
//                           participates in cross-process synchronization.
//   flags                 : bit0 = HAS_CHILD_SLABS, bit1 = LARGE_OBJECT,
//                           bits 2..31 reserved and must be 0.
struct alignas(64) IndexSlot {
    // -- Immutable after READY (covered by immutable_metadata_crc) ----------

    uint32_t msg_type = 0;
    uint32_t schema_version = 0;
    uint64_t schema_short_id = 0;
    uint32_t schema_layout_version = 0;
    uint32_t reserved0 = 0;  // must be 0

    // Atomic even though it is CRC-covered "immutable" metadata: a producer
    // writes it when claiming a slot for a new era while the consumer may
    // still be reading the slot from the previous era (it checks the sequence
    // against the consumer cursor before trusting the rest). The field is
    // 8 bytes and stays at offset 24, so the frozen layout/ABI is unchanged
    // (std::atomic<uint64_t> is lock-free and layout-compatible here).
    std::atomic<uint64_t> sequence_num{0};
    uint64_t timestamp_ns = 0;

    ShmHandle payload;
    uint32_t payload_len = 0;
    uint32_t immutable_metadata_crc = 0;

    // -- Mutable lifecycle (NOT covered by the CRC) -------------------------

    std::atomic<uint32_t> state{0};
    uint32_t flags = 0;

    // -- MPSC Vyukov sequence (NOT covered by the CRC) -----------------------
    //
    // Per-slot Vyukov sequence used only by the MPSC channel (design doc 9.5).
    // It is a numerically unique era number (not modulo capacity) that
    // distinguishes the three protocol phases of a physical slot across wraps:
    //   turn == pos        -> slot is free for the producer claiming cursor pos
    //   turn == pos + 1    -> slot is READY for the consumer reading pos
    //   turn == pos + cap  -> slot has been consumed and is free for the next
    //                         era (pos + capacity)
    // This is what makes the producer's cursor CAS and the slot occupation
    // race-free: a producer wrapping around observes turn != pos and knows the
    // slot is still occupied by the previous era, even if `state` (a modulo
    // capacity value) happens to look reusable. `sequence_num` cannot serve
    // this purpose because it is business data covered by the immutable CRC.
    // SPSC initializes it for layout consistency but never reads it.
    std::atomic<uint64_t> turn{0};  // offset 72

    // -- Explicit padding to 128 bytes (two cache lines) --------------------
    //
    // Fields above occupy 80 bytes (msg_type..turn with the ShmHandle at
    // offset 40). Padding pins the ABI so the compiler cannot introduce
    // layout drift; any field change must adjust this array to keep the
    // total at exactly 128.
    unsigned char padding_[128 - 80] = {};
};

// Slot lifecycle states (design doc 9.3 / 9.5). The producer transitions
// FREE -> RESERVED -> WRITING -> READY; consumers transition READY ->
// RETIRED and reclaim through FREE. ABORTED is a tombstone left by crash
// recovery (design doc 9.5, 12.3).
enum class SlotState : uint32_t {
    kFree = 0,
    kReserved = 1,
    kWriting = 2,
    kReady = 3,
    kRetired = 4,
    kAborted = 5,
};

// IndexSlot flag bits (design doc 9.2). Bits 2..31 are reserved and must be
// written as 0; receivers must ignore them for forward compatibility.
inline constexpr uint32_t kIndexSlotFlagHasChildSlabs = 1u << 0;
inline constexpr uint32_t kIndexSlotFlagLargeObject = 1u << 1;
inline constexpr uint32_t kIndexSlotFlagReservedMask = ~uint32_t{0b11};

// ---------------------------------------------------------------------------
// ABI contract (ADR-0003: exactly 128 bytes, two cache lines)
// ---------------------------------------------------------------------------

static_assert(sizeof(IndexSlot) == 128,
              "IndexSlot must be exactly 128 bytes (two cache lines)");
static_assert(alignof(IndexSlot) == 64,
              "IndexSlot must be cache-line aligned");
static_assert(std::is_standard_layout_v<IndexSlot>,
              "IndexSlot must be standard-layout");
// NOTE: IndexSlot is NOT trivially copyable because of the atomic `state`
// member. Channels embed it in SHM and access fields in place; they never
// memcpy a live slot (a torn copy of `state` would be meaningless).

// Field offsets pinned for cross-compiler stability (ADR-0003). Drift here is
// an ABI break.
static_assert(offsetof(IndexSlot, msg_type) == 0);
static_assert(offsetof(IndexSlot, schema_version) == 4);
static_assert(offsetof(IndexSlot, schema_short_id) == 8);
static_assert(offsetof(IndexSlot, schema_layout_version) == 16);
static_assert(offsetof(IndexSlot, reserved0) == 20);
static_assert(offsetof(IndexSlot, sequence_num) == 24);
static_assert(offsetof(IndexSlot, timestamp_ns) == 32);
static_assert(offsetof(IndexSlot, payload) == 40);
static_assert(offsetof(IndexSlot, payload_len) == 56);
static_assert(offsetof(IndexSlot, immutable_metadata_crc) == 60);
static_assert(offsetof(IndexSlot, state) == 64);
static_assert(offsetof(IndexSlot, flags) == 68);
static_assert(offsetof(IndexSlot, turn) == 72);
static_assert(offsetof(IndexSlot, padding_) == 80);

// ---------------------------------------------------------------------------
// Slot lifecycle states (design doc 9.3 / 9.5)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Immutable metadata CRC (design doc 9.2)
// ---------------------------------------------------------------------------
//
// The CRC covers exactly the bytes [0, offsetof(immutable_metadata_crc)):
// msg_type, schema_version, schema_short_id, schema_layout_version,
// reserved0, sequence_num, timestamp_ns, payload, payload_len. It never
// covers state, flags, or any sidecar metadata. CRC32C (Castagnoli) is used
// to match the Slab Header CRC (design doc 8.1).
//
// The view is computed over the slot's raw bytes; the caller is responsible
// for only computing/storing the CRC while the slot is in WRITING (before
// the READY release-store publishes it).

// Size in bytes of the CRC-covered immutable prefix.
inline constexpr uint32_t kIndexSlotImmutableBytes =
    static_cast<uint32_t>(offsetof(IndexSlot, immutable_metadata_crc));

// Computes the CRC32C of the immutable metadata prefix of `slot`.
// Implemented in index_slot.cc so the CRC table stays out of the header.
uint32_t ComputeIndexSlotImmutableCrc(const IndexSlot& slot) noexcept;

// Stamps `slot.immutable_metadata_crc` with the CRC of its current immutable
// fields. Call exactly once per publication, after all immutable fields are
// filled and before the state release-store to kReady.
void SealIndexSlotImmutableCrc(IndexSlot& slot) noexcept;

// Returns true iff the stored CRC matches the recomputed CRC of the immutable
// fields. Consumers call this after acquiring kReady; a mismatch means
// corruption (kCorruption) and the slot must not be consumed.
bool VerifyIndexSlotImmutableCrc(const IndexSlot& slot) noexcept;

// ---------------------------------------------------------------------------
// IndexSlotSnapshot: an immutable, consumer-owned copy of a published header
// ---------------------------------------------------------------------------
//
// A snapshot copies every field a consumer may need out of the shared slot at
// borrow time. Once taken, the snapshot is fully decoupled from the slot: the
// producer may reuse and overwrite the slot (e.g. under kDropOldest) without
// tearing the consumer's view. The payload itself stays zero-copy — the
// snapshot only carries its ShmHandle; payload lifetime is guarded by the
// slab generation and, once D2-11 lands, the reference Pin (ADR-0013).
struct IndexSlotSnapshot {
    uint32_t msg_type = 0;
    uint32_t schema_version = 0;
    uint64_t schema_short_id = 0;
    uint32_t schema_layout_version = 0;
    uint32_t reserved0 = 0;

    uint64_t sequence_num = 0;
    uint64_t timestamp_ns = 0;

    ShmHandle payload;
    uint32_t payload_len = 0;
    uint32_t immutable_metadata_crc = 0;

    uint32_t flags = 0;
    uint32_t reserved1 = 0;
};

static_assert(std::is_trivially_copyable_v<IndexSlotSnapshot>);
static_assert(std::is_standard_layout_v<IndexSlotSnapshot>);

// Copies the immutable metadata (plus flags) out of `slot`. Must be called
// only after the slot was observed in kReady with an acquire load; the
// producer must not write the slot again before the consumer cursor advances
// past it, which every channel's full/wrap invariant guarantees.
IndexSlotSnapshot SnapshotIndexSlot(const IndexSlot& slot) noexcept;

// Computes the CRC32C over the snapshot's immutable fields. Identical field
// coverage and wire order as ComputeIndexSlotImmutableCrc, so a snapshot's
// CRC equals the originating slot's CRC.
uint32_t ComputeSnapshotCrc(const IndexSlotSnapshot& snapshot) noexcept;

// Returns true iff the snapshot's stored CRC matches its immutable fields.
bool VerifySnapshotCrc(const IndexSlotSnapshot& snapshot) noexcept;

// ---------------------------------------------------------------------------
// Channel-specific sidecar metadata (design doc 9.2)
// ---------------------------------------------------------------------------
//
// Sidecars live in a parallel array with the same capacity and indexing as
// the base slots. They carry channel-specific mutable metadata that must NOT
// bloat the 128-byte base slot. A sidecar entry is only meaningful while the
// corresponding slot's sequence is current: consumers must validate the slot
// sequence before trusting sidecar contents, and stale sidecar data must
// never be applied to a recycled slot.

// Maximum number of subscribers representable in a broadcast ACK bitmap.
// Registration beyond this is rejected (design doc 9.6).
inline constexpr uint32_t kBroadcastMaxSubscribers = 64;

// Bitmap with one bit per subscriber ID. Bit i corresponds to SubscriberId i;
// a set bit means "ACK still owed". Owned by the broadcast channel.
struct AckBitmap {
    std::atomic<uint64_t> bits{0};

    // Sets bit `index` (index < kBroadcastMaxSubscribers). Used at publish
    // time to initialize ACK responsibility from the subscriber snapshot.
    void Set(uint32_t index) noexcept {
        bits.fetch_or(uint64_t{1} << index, std::memory_order_relaxed);
    }

    // Clears bit `index`. Returns true if this call actually cleared a
    // previously-set bit (i.e. the caller owned the ACK responsibility).
    // Release ordering pairs with the consumer's message reads.
    bool Clear(uint32_t index) noexcept {
        const uint64_t mask = uint64_t{1} << index;
        return (bits.fetch_and(~mask, std::memory_order_acq_rel) & mask) != 0;
    }

    // Returns true iff no ACKs are outstanding (payload may be retired).
    bool AllAcked(std::memory_order order = std::memory_order_acquire) const
        noexcept {
        return bits.load(order) == 0;
    }

    // Returns true iff bit `index` is currently set.
    bool IsSet(uint32_t index) const noexcept {
        return (bits.load(std::memory_order_acquire) & (uint64_t{1} << index)) !=
               0;
    }
};

// Broadcast sidecar (design doc 9.2 / 9.6). `subscriber_set_version` binds
// the ACK bitmap to the exact subscriber-set snapshot used at publish time;
// membership changes bump the version and never retroactively edit existing
// slots.
struct BroadcastSlotMeta {
    uint64_t subscriber_set_version = 0;
    AckBitmap ack_bitmap;
};

static_assert(std::is_standard_layout_v<BroadcastSlotMeta>);
static_assert(sizeof(BroadcastSlotMeta) == 16);

// Work-queue sidecar (design doc 9.2 / 9.7). `claimant_id` uses the
// SubscriberId namespace: a work-queue consumer is a Subscriber in the
// lease/registration system, avoiding a third identity domain.
struct WorkQueueSlotMeta {
    uint32_t claimant_id = 0;
    uint32_t claimant_generation = 0;
    uint32_t delivery_attempt = 0;
    std::atomic<uint32_t> claim_state{0};
};

static_assert(std::is_standard_layout_v<WorkQueueSlotMeta>);
static_assert(sizeof(WorkQueueSlotMeta) == 16);

// MPSC reservation sidecar (design doc 9.2 / 9.5). Binds a reserved slot to
// its owner identity so crash recovery can distinguish a live-but-slow
// producer from a dead one before stamping the ABORTED tombstone.
//
// `owner_process_id` is the OS PID (used for /proc liveness, design doc 9.5);
// `owner_process_epoch` distinguishes PID reuse (design doc 4.3). The slot's
// logical sequence is NOT stored here: it already lives in
// IndexSlot::sequence_num (CRC-covered), so a redundant copy would only risk
// divergence. Recovery reads the sequence from the slot, not the sidecar.
struct MpscReservationMeta {
    uint64_t owner_process_id = 0;
    uint64_t owner_process_epoch = 0;
    uint64_t owner_publisher_id = 0;
    uint64_t reservation_timestamp_ns = 0;
};

static_assert(std::is_standard_layout_v<MpscReservationMeta>);
static_assert(sizeof(MpscReservationMeta) == 32);

}  // namespace mino

#endif  // MINO_SHM_CHANNEL_INDEX_SLOT_H_
