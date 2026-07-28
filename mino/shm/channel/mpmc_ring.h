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

// Generic MPMC skeleton: bounded Vyukov ring in shared memory.
//
// This skeleton is the shared concurrency foundation for all cross-process
// channels (SPSC/MPSC/Broadcast/WorkQueue). It answers exactly one question:
// how do multiple producers and multiple consumers concurrently reserve,
// publish, consume, and wrap around on one shared bounded ring, correctly?
// It answers nothing about consumption semantics — those are layered on top
// by the channel implementations.
//
// Algorithm (bounded MPMC queue after Dmitry Vyukov):
//   - Capacity is a power of two; the physical slot of a logical position is
//     (sequence % capacity), implemented as (sequence & mask).
//   - A per-slot 64-bit sequence number discriminates three states:
//       seq == pos            : slot is free for the reservation at pos
//       seq == pos + 1        : slot holds committed data for the claim at pos
//       seq <  pos            : slot still belongs to a previous cycle
//                               (full from the producer's point of view)
//   - enqueue_pos / dequeue_pos CAS uses memory_order_relaxed for contention
//     arbitration only; data-race correctness is carried by the slot sequence
//     release/acquire pair ("data becomes visible before its state").
//   - Full:  enqueue_pos - dequeue_pos >= capacity.
//   - Empty: dequeue_pos >= enqueue_pos.
//
// Reference semantics (zero-copy foundation): slots carry references into the
// Central Slab (ShmHandle, or an IndexSlot embedding one) — never the payload
// itself. The control block and slots must never contain process virtual
// addresses; cross-process addressing uses Region Offsets/Handles only.
//
// Cross-process precondition: lock-free atomics on the same mapping are
// correct for every sharing process (x86-64 baseline, design doc V-12). Each
// process may map the region at a different base address; the control block
// and slots contain offset semantics only.
//
// Lifecycle:
//   - Init: the Region Owner initializes the control block and every slot
//     sequence in place, then publishes the magic last. The structure must
//     not be used before the magic lands.
//   - Attach: the mapping side validates magic, layout_version, capacity and
//     the element ABI before attaching; validation failure refuses the attach.
//
// Design doc: section 9.9. Memory-order contract: section 9.3.

#ifndef MINO_SHM_CHANNEL_MPMC_RING_H_
#define MINO_SHM_CHANNEL_MPMC_RING_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <type_traits>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {

// Cache line size used to separate the enqueue/dequeue cursors so producers
// and consumers do not false-share a line (design doc 9.4 convention).
inline constexpr uint64_t kMpmcRingCacheLineSize = 64;

// ---------------------------------------------------------------------------
// Control block
// ---------------------------------------------------------------------------

// MpmcRingControlBlock is the fixed-layout header of the ring, stored at the
// start of the SHM region. All multi-byte fields are little-endian by
// platform convention (x86-64 / AArch64-LE baseline); the ring is not
// portable across endiannesses.
struct MpmcRingControlBlock {
    // Written LAST by Init (release store), checked FIRST by Attach. Until
    // the magic lands the structure must not be used.
    std::atomic<uint64_t> magic;
    // Bumped when the control-block or slot layout changes. Attach refuses a
    // version it does not implement.
    std::atomic<uint32_t> layout_version;
    uint32_t reserved0;  // Explicit padding: keeps magic..capacity in one line.
    // Number of slots. Always a power of two.
    uint64_t capacity;
    // Element ABI of the payload storage: byte size and alignment of the
    // fixed slot storage. Attach rejects a mismatch (protects against
    // processes built with different compilation configurations).
    uint32_t elem_size;
    uint32_t elem_align;

    // Producer cursor: next logical position to reserve. Claims a full cache
    // line; producers contend only among themselves.
    alignas(kMpmcRingCacheLineSize) std::atomic<uint64_t> enqueue_pos;

    // Consumer cursor: next logical position to claim. Claims a full cache
    // line; consumers contend only among themselves.
    alignas(kMpmcRingCacheLineSize) std::atomic<uint64_t> dequeue_pos;
};

// Fixed-layout contract for shared memory. The control block is shared across
// processes (and potentially across compiler configurations), so its layout
// must be pinned down statically.
static_assert(std::is_standard_layout_v<MpmcRingControlBlock>,
              "MpmcRingControlBlock must be standard-layout");
static_assert(sizeof(MpmcRingControlBlock) == 3 * kMpmcRingCacheLineSize,
              "MpmcRingControlBlock must occupy exactly three cache lines");
static_assert(offsetof(MpmcRingControlBlock, magic) == 0,
              "magic must be the first field");
static_assert(offsetof(MpmcRingControlBlock, capacity) == 16,
              "capacity offset drifted");
static_assert(offsetof(MpmcRingControlBlock, enqueue_pos) ==
                  kMpmcRingCacheLineSize,
              "enqueue_pos must start its own cache line");
static_assert(offsetof(MpmcRingControlBlock, dequeue_pos) ==
                  2 * kMpmcRingCacheLineSize,
              "dequeue_pos must start its own cache line");

// ---------------------------------------------------------------------------
// Slot
// ---------------------------------------------------------------------------

// MpmcRingSlot is one fixed-size ring slot. `storage_size` bytes of storage
// with alignment `storage_align` carry the element (an IndexSlot or a bare
// ShmHandle); the 64-bit sequence number is the concurrency metadata
// (free / committed / wrap-cycle discriminator).
//
// Slot stride between consecutive slots is exactly
// sizeof(MpmcRingSlot<storage_size, storage_align>): the sequence plus the
// storage rounded up to the storage alignment. The storage therefore begins
// at a multiple of storage_align within every slot, as long as the slot
// array base is aligned to storage_align (guaranteed by Init/Attach, which
// require a 64-byte-aligned region base and a small power-of-two align).
template <uint32_t storage_size, uint32_t storage_align>
struct MpmcRingSlot {
    std::atomic<uint64_t> sequence;
    alignas(storage_align) unsigned char storage[storage_size];
};

// ---------------------------------------------------------------------------
// MpmcRing
// ---------------------------------------------------------------------------

// MpmcRing<T> is a header-only, non-owning view over a ring living in shared
// memory. T must be trivially copyable and must not contain pointers
// (reference semantics: slots carry Region Offset/Handle values, never
// process virtual addresses).
//
// All member functions are safe to call concurrently from multiple threads
// and multiple processes sharing the mapping, with one exception: Init must
// complete (including the magic release store) before any concurrent use.
template <typename T>
class MpmcRing {
public:
    // The ring requires lock-free 64-bit atomics to be implementable at all.
    static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
                  "MpmcRing requires lock-free 64-bit atomics");
    // Reference semantics: elements are copied in/out bitwise and shared
    // across processes, so they must be trivially copyable and pointer-free.
    static_assert(std::is_trivially_copyable_v<T>,
                  "ring elements must be trivially copyable");
    static_assert(!std::is_pointer_v<T>,
                  "ring elements must not be pointers (use ShmHandle/Offset)");

    // MpmcRing is a non-owning, trivially copyable view into shared memory.
    // It owns no resources; nothing is destroyed when the view goes away
    // (a process exiting does not "destruct" the skeleton).
    MpmcRing() noexcept = default;

    // -----------------------------------------------------------------------
    // Init / Attach
    // -----------------------------------------------------------------------

    // Initializes a ring of `capacity` slots in place at `shm_base`.
    //
    // `capacity` must be a power of two and at least 2. `elem_size` /
    // `elem_align` describe the element ABI and must satisfy
    // elem_size >= sizeof(T) and elem_align >= alignof(T); the slot storage
    // is elem_size bytes with elem_align alignment, and the values are pinned
    // into the control block so Attach can reject ABI mismatches.
    //
    // Callers must size the shared region with RequiredSize() and must not
    // touch the memory concurrently while Init runs.
    static Result<MpmcRing> Init(void* shm_base, uint64_t capacity,
                                 uint32_t elem_size, uint32_t elem_align) {
        if (shm_base == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must not be null");
        }
        if (reinterpret_cast<uintptr_t>(shm_base) %
                alignof(MpmcRingControlBlock) !=
            0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must be aligned to the control "
                                 "block alignment (64 bytes)");
        }
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "capacity must be a power of two and >= 2");
        }
        if (capacity > (uint64_t{1} << 32)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "capacity exceeds the supported maximum (2^32)");
        }
        if (elem_size < sizeof(T)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "elem_size is smaller than sizeof(T)");
        }
        if (elem_align < alignof(T) || elem_align == 0 ||
            (elem_align & (elem_align - 1)) != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "elem_align must be a non-zero power of two "
                                 "and >= alignof(T)");
        }
        if (elem_size % elem_align != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "elem_size must be a multiple of elem_align");
        }
        if (elem_size > (UINT64_MAX - sizeof(MpmcRingControlBlock)) / capacity) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "ring footprint overflows 64-bit size");
        }

        auto* control = static_cast<MpmcRingControlBlock*>(shm_base);
        // Initialize every non-atomic field before anything can observe the
        // control block; the magic below is the single publication point.
        control->layout_version.store(kLayoutVersion,
                                      std::memory_order_relaxed);
        control->reserved0 = 0;
        control->capacity = capacity;
        control->elem_size = elem_size;
        control->elem_align = elem_align;
        control->enqueue_pos.store(0, std::memory_order_relaxed);
        control->dequeue_pos.store(0, std::memory_order_relaxed);

        // Slot i starts life free for logical position i: sequence == i.
        Slot* slots = SlotsOf(shm_base);
        for (uint64_t i = 0; i < capacity; ++i) {
            Slot* slot = SlotAt(slots, i, elem_size, elem_align);
            slot->sequence.store(i, std::memory_order_relaxed);
        }

        // Publish: release so every plain/relaxed write above is visible to
        // any observer that acquires the magic. The structure must not be
        // used before the magic lands.
        control->magic.store(kMagic, std::memory_order_release);
        return MpmcRing(control, slots);
    }

    // Attaches to an already-initialized ring at `shm_base`, validating the
    // magic, layout version, capacity and element ABI. Any validation failure
    // refuses the attach.
    static Result<MpmcRing> Attach(void* shm_base) {
        if (shm_base == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must not be null");
        }
        if (reinterpret_cast<uintptr_t>(shm_base) %
                alignof(MpmcRingControlBlock) !=
            0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "shm_base must be aligned to the control "
                                 "block alignment (64 bytes)");
        }

        auto* control = static_cast<MpmcRingControlBlock*>(shm_base);
        // Acquire pairs with the Init release store: once the magic reads as
        // valid, every field initialized before it is visible here.
        if (control->magic.load(std::memory_order_acquire) != kMagic) {
            return Status::Error(StatusCode::kCorruption,
                                 "ring magic mismatch: region is not an "
                                 "initialized MpmcRing");
        }
        if (control->layout_version.load(std::memory_order_relaxed) !=
            kLayoutVersion) {
            return Status::Error(
                StatusCode::kUnsupported,
                "ring layout_version mismatch: unsupported ring layout");
        }
        const uint64_t capacity = control->capacity;
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "ring capacity is not a power of two >= 2");
        }
        if (control->elem_size != sizeof(T) ||
            control->elem_align != alignof(T)) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "ring element ABI mismatch: elem_size or "
                                 "elem_align differs from this build");
        }
        // Consistency guard shared with Init's overflow rule.
        if (control->elem_size >
            (UINT64_MAX - sizeof(MpmcRingControlBlock)) / capacity) {
            return Status::Error(StatusCode::kCorruption,
                                 "ring footprint overflows 64-bit size");
        }
        if (control->elem_size % control->elem_align != 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "ring elem_size is not a multiple of elem_align");
        }

        Slot* slots = SlotsOf(shm_base);
        return MpmcRing(control, slots);
    }

    // Bytes of shared memory a ring of `capacity` slots with the given
    // element ABI occupies. Callers use this to size the region before Init.
    //
    // The stride between consecutive slots is exactly
    // sizeof(MpmcRingSlot<elem_size, elem_align>), so the footprint is the
    // control block plus `capacity` slot objects placed back to back.
    static constexpr uint64_t RequiredSize(uint64_t capacity,
                                           uint32_t elem_size,
                                           uint32_t elem_align) {
        return sizeof(MpmcRingControlBlock) +
               capacity * SlotStride(elem_size, elem_align);
    }

    // -----------------------------------------------------------------------
    // Producer path
    // -----------------------------------------------------------------------

    // Tries to reserve one logical position for enqueueing.
    //
    // On success returns the reserved sequence number; the caller publishes
    // data with CommitEnqueue(sequence, value). Returns
    // StatusCode::kResourceExhausted without effect when the ring is full.
    //
    // Note: mutating-through-the-view methods are const. MpmcRing is a
    // non-owning handle; const here only freezes the two view pointers, while
    // the referenced shared memory is expected to change (also from other
    // processes). ReadSlot/IsFull/IsEmpty follow the same convention.
    Result<uint64_t> TryEnqueue() const {
        uint64_t pos = control_->enqueue_pos.load(std::memory_order_relaxed);
        for (;;) {
            Slot* slot = SlotForPos(pos);
            const uint64_t seq =
                slot->sequence.load(std::memory_order_acquire);
            const int64_t diff = static_cast<int64_t>(seq) -
                                 static_cast<int64_t>(pos);
            if (diff == 0) {
                // Slot is free for this position; arbitrate the reservation.
                // Relaxed CAS: the race losers simply retry, and correctness
                // of the data path is carried by the slot sequence, not by
                // the cursor.
                if (control_->enqueue_pos.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    return pos;
                }
                // `pos` was overwritten with the current cursor; retry.
            } else if (diff < 0) {
                // The slot still belongs to a previous wrap cycle: the ring
                // is full (producer_sequence - min_consumer_sequence >=
                // capacity).
                return Status::Error(StatusCode::kResourceExhausted,
                                     "ring is full");
            } else {
                // Another producer reserved this position first; reload.
                pos = control_->enqueue_pos.load(std::memory_order_relaxed);
            }
        }
    }

    // Publishes `value` into the slot reserved by TryEnqueue().
    //
    // The caller must pass a sequence number obtained from a successful
    // TryEnqueue on this ring; committing a sequence that was not reserved
    // corrupts the ring. The data write happens-before the slot sequence
    // release store, so a consumer that acquires the committed state is
    // guaranteed to see the data ("data becomes visible before its state").
    Status CommitEnqueue(uint64_t sequence, const T& value) const {
        Slot* slot = SlotForPos(sequence);
        std::memcpy(slot->storage, &value, sizeof(T));
        slot->sequence.store(sequence + 1, std::memory_order_release);
        return Status::Ok();
    }

    // -----------------------------------------------------------------------
    // Consumer path
    // -----------------------------------------------------------------------

    // Tries to claim one committed logical position for dequeueing.
    //
    // On success returns the claimed sequence number; the caller reads the
    // data with ReadSlot(sequence) and completes the consumption with
    // CommitDequeue(sequence). Returns StatusCode::kWouldBlock without
    // effect when the ring is empty.
    Result<uint64_t> TryDequeue() const {
        uint64_t pos = control_->dequeue_pos.load(std::memory_order_relaxed);
        for (;;) {
            Slot* slot = SlotForPos(pos);
            const uint64_t seq =
                slot->sequence.load(std::memory_order_acquire);
            const int64_t diff = static_cast<int64_t>(seq) -
                                 static_cast<int64_t>(pos + 1);
            if (diff == 0) {
                // Slot holds committed data for this position; arbitrate the
                // claim. Relaxed CAS, same rationale as TryEnqueue.
                if (control_->dequeue_pos.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    return pos;
                }
            } else if (diff < 0) {
                // No committed data at this position: the ring is empty
                // (dequeue_pos >= enqueue_pos).
                return Status::Error(StatusCode::kWouldBlock,
                                     "ring is empty");
            } else {
                // Another consumer claimed this position first; reload.
                pos = control_->dequeue_pos.load(std::memory_order_relaxed);
            }
        }
    }

    // Copies the element out of the slot claimed by TryDequeue().
    //
    // Must be called after TryDequeue() returned `sequence` and before the
    // matching CommitDequeue(sequence). The acquiring read that established
    // visibility of the data happened in TryDequeue; this read itself is a
    // plain copy out of shared memory.
    Result<T> ReadSlot(uint64_t sequence) const {
        const Slot* slot = SlotForPos(sequence);
        T value;
        std::memcpy(&value, slot->storage, sizeof(T));
        return value;
    }

    // Releases the slot claimed by TryDequeue() back to producers.
    //
    // Must be called exactly once per successful TryDequeue, after the data
    // was consumed via ReadSlot. The release store pairs with the acquiring
    // load in TryEnqueue, so a producer reusing the slot in the next wrap
    // cycle observes a coherent state.
    Status CommitDequeue(uint64_t sequence) const {
        Slot* slot = SlotForPos(sequence);
        slot->sequence.store(sequence + control_->capacity,
                             std::memory_order_release);
        return Status::Ok();
    }

    // -----------------------------------------------------------------------
    // Observers
    // -----------------------------------------------------------------------

    // Approximate fullness snapshot: enqueue_pos - dequeue_pos >= capacity.
    // Under concurrency the result may be stale the moment it returns.
    bool IsFull() const {
        const uint64_t enq =
            control_->enqueue_pos.load(std::memory_order_acquire);
        const uint64_t deq =
            control_->dequeue_pos.load(std::memory_order_acquire);
        return enq - deq >= control_->capacity;
    }

    // Approximate emptiness snapshot: dequeue_pos >= enqueue_pos.
    // Under concurrency the result may be stale the moment it returns.
    bool IsEmpty() const {
        const uint64_t enq =
            control_->enqueue_pos.load(std::memory_order_acquire);
        const uint64_t deq =
            control_->dequeue_pos.load(std::memory_order_acquire);
        return deq >= enq;
    }

    // Number of slots (a power of two, fixed at Init).
    uint64_t capacity() const { return control_->capacity; }

private:
    static constexpr uint64_t kMagic = 0x4D494E4F524E4731ULL;  // "MINORNG1"
    static constexpr uint32_t kLayoutVersion = 1;

    using Slot = MpmcRingSlot<sizeof(T), alignof(T)>;

    explicit MpmcRing(MpmcRingControlBlock* control, Slot* slots) noexcept
        : control_(control), slots_(slots) {}

    // Byte distance between consecutive slots: the sequence plus the storage
    // rounded up to the storage alignment. This equals
    // sizeof(MpmcRingSlot<elem_size, elem_align>) whenever elem_align is a
    // power of two (validated by Init), so the in-memory slot array is
    // exactly an array of slot objects.
    static constexpr uint32_t SlotStride(uint32_t elem_size,
                                         uint32_t elem_align) {
        return (8 + elem_size + elem_align - 1) / elem_align * elem_align;
    }

    // The slot array immediately follows the control block. The control
    // block is a multiple of 64 bytes and elem_align is a small power of two
    // (validated by Init), so the array base satisfies the slot alignment.
    static Slot* SlotsOf(void* shm_base) {
        return reinterpret_cast<Slot*>(static_cast<unsigned char*>(shm_base) +
                                       sizeof(MpmcRingControlBlock));
    }

    static Slot* SlotAt(Slot* slots, uint64_t index, uint32_t elem_size,
                        uint32_t elem_align) {
        return reinterpret_cast<Slot*>(
            reinterpret_cast<unsigned char*>(slots) +
            index * SlotStride(elem_size, elem_align));
    }

    Slot* SlotForPos(uint64_t pos) const {
        return SlotAt(slots_, pos & (control_->capacity - 1), sizeof(T),
                      alignof(T));
    }

    MpmcRingControlBlock* control_ = nullptr;
    Slot* slots_ = nullptr;
};

}  // namespace mino

#endif  // MINO_SHM_CHANNEL_MPMC_RING_H_
