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

#include "mino/shm/channel/mpmc_ring.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <thread>
#include <type_traits>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {
namespace {

// ---------------------------------------------------------------------------
// Compile-time layout contract
// ---------------------------------------------------------------------------

// The control block layout is pinned in the header (see the static_asserts
// next to MpmcRingControlBlock); re-state the load-bearing facts here so the
// contract is covered by a test binary as well.
static_assert(sizeof(MpmcRingControlBlock) == 3 * kMpmcRingCacheLineSize,
              "control block must occupy exactly three cache lines");
static_assert(std::is_standard_layout_v<MpmcRingControlBlock>,
              "control block must be standard-layout");
static_assert(alignof(MpmcRingControlBlock) == kMpmcRingCacheLineSize,
              "control block must be 64-byte aligned");
static_assert(offsetof(MpmcRingControlBlock, enqueue_pos) %
                      kMpmcRingCacheLineSize ==
                  0,
              "enqueue_pos must start a cache line");
static_assert(offsetof(MpmcRingControlBlock, dequeue_pos) %
                      kMpmcRingCacheLineSize ==
                  0,
              "dequeue_pos must start a cache line");
static_assert(offsetof(MpmcRingControlBlock, dequeue_pos) !=
                  offsetof(MpmcRingControlBlock, enqueue_pos),
              "cursors must live on separate cache lines");

using TestSlot = MpmcRingSlot<16, 8>;
static_assert(std::is_standard_layout_v<TestSlot>,
              "slot must be standard-layout");
static_assert(offsetof(TestSlot, storage) % 8 == 0,
              "slot storage must honor the element alignment");
static_assert(sizeof(TestSlot) == 24, "slot stride drifted");

// MpmcRing is a non-owning, trivially copyable view: it can be passed between
// threads (and duplicated across processes' address spaces) freely.
static_assert(std::is_trivially_copyable_v<MpmcRing<uint64_t>>,
              "MpmcRing must be a trivially copyable view");

// Reference semantics: elements that are pointers or not trivially copyable
// must be rejected at compile time (uncomment to verify they fail to build):
//   MpmcRing<uint64_t*> bad1;             // pointer element
//   MpmcRing<std::string> bad2;           // not trivially copyable

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// 64-byte-aligned shared-memory stand-in. The ring contract requires the
// region base to be control-block aligned.
template <uint64_t kCapacity, typename T>
struct RingFixture {
    static constexpr uint64_t kBytes =
        MpmcRing<T>::RequiredSize(kCapacity, sizeof(T), alignof(T));

    RingFixture() : storage(static_cast<unsigned char*>(
                        ::operator new(kBytes, std::align_val_t(64)))) {
        std::memset(storage, 0, kBytes);
    }
    ~RingFixture() {
        ::operator delete(storage, kBytes, std::align_val_t(64));
    }

    RingFixture(const RingFixture&) = delete;
    RingFixture& operator=(const RingFixture&) = delete;

    unsigned char* storage;
};

template <uint64_t kCapacity>
using U64Fixture = RingFixture<kCapacity, uint64_t>;

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

TEST(MpmcRingInitTest, InitSucceeds) {
    U64Fixture<8> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 8, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(ring.ok()) << ring.status().ToString();
    EXPECT_EQ(ring->capacity(), 8u);
    EXPECT_TRUE(ring->IsEmpty());
    EXPECT_FALSE(ring->IsFull());
}

TEST(MpmcRingInitTest, InitRejectsNullBase) {
    auto ring = MpmcRing<uint64_t>::Init(nullptr, 8, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_FALSE(ring.ok());
    EXPECT_EQ(ring.status().code(), StatusCode::kInvalidArgument);
}

TEST(MpmcRingInitTest, InitRejectsMisalignedBase) {
    U64Fixture<8> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage + 8, 8, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_FALSE(ring.ok());
    EXPECT_EQ(ring.status().code(), StatusCode::kInvalidArgument);
}

TEST(MpmcRingInitTest, InitRejectsNonPowerOfTwoCapacity) {
    U64Fixture<16> f;
    for (uint64_t capacity : {0u, 1u, 3u, 6u, 12u}) {
        auto ring = MpmcRing<uint64_t>::Init(f.storage, capacity,
                                             sizeof(uint64_t),
                                             alignof(uint64_t));
        ASSERT_FALSE(ring.ok()) << "capacity=" << capacity;
        EXPECT_EQ(ring.status().code(), StatusCode::kInvalidArgument);
    }
}

TEST(MpmcRingInitTest, InitRejectsOversizedCapacity) {
    U64Fixture<8> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, uint64_t{1} << 40,
                                         sizeof(uint64_t), alignof(uint64_t));
    ASSERT_FALSE(ring.ok());
    EXPECT_EQ(ring.status().code(), StatusCode::kInvalidArgument);
}

TEST(MpmcRingInitTest, InitRejectsBadElementAbi) {
    U64Fixture<8> f;

    // elem_size smaller than sizeof(T).
    auto r1 = MpmcRing<uint64_t>::Init(f.storage, 8, 4, 8);
    ASSERT_FALSE(r1.ok());
    EXPECT_EQ(r1.status().code(), StatusCode::kInvalidArgument);

    // elem_align smaller than alignof(T).
    auto r2 = MpmcRing<uint64_t>::Init(f.storage, 8, 8, 4);
    ASSERT_FALSE(r2.ok());
    EXPECT_EQ(r2.status().code(), StatusCode::kInvalidArgument);

    // elem_size not a multiple of elem_align.
    auto r3 = MpmcRing<uint64_t>::Init(f.storage, 8, 24, 16);
    ASSERT_FALSE(r3.ok());
    EXPECT_EQ(r3.status().code(), StatusCode::kInvalidArgument);

    // elem_align not a power of two.
    auto r4 = MpmcRing<uint64_t>::Init(f.storage, 8, 24, 12);
    ASSERT_FALSE(r4.ok());
    EXPECT_EQ(r4.status().code(), StatusCode::kInvalidArgument);
}

// ---------------------------------------------------------------------------
// Attach
// ---------------------------------------------------------------------------

TEST(MpmcRingAttachTest, AttachSucceedsAfterInit) {
    U64Fixture<8> f;
    auto init = MpmcRing<uint64_t>::Init(f.storage, 8, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(init.ok());

    // A second "process" view over the same mapping.
    auto attached = MpmcRing<uint64_t>::Attach(f.storage);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_EQ(attached->capacity(), 8u);
    EXPECT_TRUE(attached->IsEmpty());
}

TEST(MpmcRingAttachTest, AttachRejectsNullAndMisalignedBase) {
    U64Fixture<8> f;
    auto r1 = MpmcRing<uint64_t>::Attach(nullptr);
    ASSERT_FALSE(r1.ok());
    EXPECT_EQ(r1.status().code(), StatusCode::kInvalidArgument);

    auto r2 = MpmcRing<uint64_t>::Attach(f.storage + 8);
    ASSERT_FALSE(r2.ok());
    EXPECT_EQ(r2.status().code(), StatusCode::kInvalidArgument);
}

TEST(MpmcRingAttachTest, AttachRejectsUninitializedRegion) {
    U64Fixture<8> f;  // Zero-filled: magic has not landed.
    auto ring = MpmcRing<uint64_t>::Attach(f.storage);
    ASSERT_FALSE(ring.ok());
    EXPECT_EQ(ring.status().code(), StatusCode::kCorruption);
}

TEST(MpmcRingAttachTest, AttachRejectsBadMagic) {
    U64Fixture<8> f;
    auto init = MpmcRing<uint64_t>::Init(f.storage, 8, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(init.ok());

    auto* control =
        reinterpret_cast<MpmcRingControlBlock*>(static_cast<void*>(f.storage));
    control->magic.store(0xDEADBEEF, std::memory_order_relaxed);

    auto ring = MpmcRing<uint64_t>::Attach(f.storage);
    ASSERT_FALSE(ring.ok());
    EXPECT_EQ(ring.status().code(), StatusCode::kCorruption);
}

TEST(MpmcRingAttachTest, AttachRejectsUnsupportedLayoutVersion) {
    U64Fixture<8> f;
    auto init = MpmcRing<uint64_t>::Init(f.storage, 8, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(init.ok());

    auto* control =
        reinterpret_cast<MpmcRingControlBlock*>(static_cast<void*>(f.storage));
    control->layout_version.store(999, std::memory_order_relaxed);

    auto ring = MpmcRing<uint64_t>::Attach(f.storage);
    ASSERT_FALSE(ring.ok());
    EXPECT_EQ(ring.status().code(), StatusCode::kUnsupported);
}

TEST(MpmcRingAttachTest, AttachRejectsAbiMismatch) {
    U64Fixture<8> f;
    // Initialize the region as a ring of 16-byte, 8-aligned elements...
    auto init = MpmcRing<uint64_t>::Init(f.storage, 8, 16, 8);
    ASSERT_TRUE(init.ok());

    // ...then attach with a viewer whose T has a different ABI.
    auto ring = MpmcRing<uint64_t>::Attach(f.storage);
    ASSERT_FALSE(ring.ok());
    EXPECT_EQ(ring.status().code(), StatusCode::kSchemaMismatch);
}

TEST(MpmcRingAttachTest, AttachRejectsCorruptCapacity) {
    U64Fixture<8> f;
    auto init = MpmcRing<uint64_t>::Init(f.storage, 8, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(init.ok());

    auto* control =
        reinterpret_cast<MpmcRingControlBlock*>(static_cast<void*>(f.storage));
    control->capacity = 7;  // Not a power of two.

    auto ring = MpmcRing<uint64_t>::Attach(f.storage);
    ASSERT_FALSE(ring.ok());
    EXPECT_EQ(ring.status().code(), StatusCode::kCorruption);
}

// Cross-view visibility: data committed through the Init view must be
// readable through the Attach view (same mapping, two "processes").
TEST(MpmcRingAttachTest, AttachViewSeesCommittedData) {
    U64Fixture<8> f;
    auto owner = MpmcRing<uint64_t>::Init(f.storage, 8, sizeof(uint64_t),
                                          alignof(uint64_t));
    ASSERT_TRUE(owner.ok());
    auto guest = MpmcRing<uint64_t>::Attach(f.storage);
    ASSERT_TRUE(guest.ok());

    auto seq = owner->TryEnqueue();
    ASSERT_TRUE(seq.ok());
    ASSERT_TRUE(owner->CommitEnqueue(*seq, 0xABCD).ok());

    auto got = guest->TryDequeue();
    ASSERT_TRUE(got.ok());
    EXPECT_EQ(*got, *seq);
    auto value = guest->ReadSlot(*got);
    ASSERT_TRUE(value.ok());
    EXPECT_EQ(*value, 0xABCDu);
    ASSERT_TRUE(guest->CommitDequeue(*got).ok());
}

// ---------------------------------------------------------------------------
// Single-threaded behavior
// ---------------------------------------------------------------------------

TEST(MpmcRingBasicTest, EnqueueDequeueRoundTrip) {
    U64Fixture<8> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 8, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    auto seq = ring->TryEnqueue();
    ASSERT_TRUE(seq.ok());
    EXPECT_EQ(*seq, 0u);
    ASSERT_TRUE(ring->CommitEnqueue(*seq, 42).ok());

    auto got = ring->TryDequeue();
    ASSERT_TRUE(got.ok());
    EXPECT_EQ(*got, 0u);
    auto value = ring->ReadSlot(*got);
    ASSERT_TRUE(value.ok());
    EXPECT_EQ(*value, 42u);
    ASSERT_TRUE(ring->CommitDequeue(*got).ok());

    EXPECT_TRUE(ring->IsEmpty());
}

TEST(MpmcRingBasicTest, SequencesAreMonotonic) {
    U64Fixture<4> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 4, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    // Drive several wrap cycles; logical positions must increase strictly.
    uint64_t expected = 0;
    for (int round = 0; round < 10; ++round) {
        auto seq = ring->TryEnqueue();
        ASSERT_TRUE(seq.ok());
        EXPECT_EQ(*seq, expected);
        ASSERT_TRUE(ring->CommitEnqueue(*seq, expected).ok());

        auto got = ring->TryDequeue();
        ASSERT_TRUE(got.ok());
        EXPECT_EQ(*got, expected);
        ASSERT_TRUE(ring->CommitDequeue(*got).ok());
        ++expected;
    }
}

TEST(MpmcRingBasicTest, DataConsistencyAcrossManyValues) {
    U64Fixture<16> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 16, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    // Fill with distinctive values, then drain and verify every one.
    for (uint64_t i = 0; i < 16; ++i) {
        auto seq = ring->TryEnqueue();
        ASSERT_TRUE(seq.ok());
        ASSERT_TRUE(ring->CommitEnqueue(*seq, 0xF00D0000u + i).ok());
    }
    for (uint64_t i = 0; i < 16; ++i) {
        auto got = ring->TryDequeue();
        ASSERT_TRUE(got.ok());
        auto value = ring->ReadSlot(*got);
        ASSERT_TRUE(value.ok());
        EXPECT_EQ(*value, 0xF00D0000u + i);
        ASSERT_TRUE(ring->CommitDequeue(*got).ok());
    }
}

TEST(MpmcRingBasicTest, PaddedElementAbiRoundTrips) {
    // Init with a padded element ABI (elem_size > sizeof(T)) still reads and
    // writes the element correctly; only the leading sizeof(T) bytes carry
    // meaning.
    U64Fixture<4> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 4, 16, 8);
    ASSERT_TRUE(ring.ok());
    auto attached = MpmcRing<uint64_t>::Attach(f.storage);
    // ABI differs from this build's default T layout -> must refuse.
    ASSERT_FALSE(attached.ok());

    auto seq = ring->TryEnqueue();
    ASSERT_TRUE(seq.ok());
    ASSERT_TRUE(ring->CommitEnqueue(*seq, 7).ok());
    auto got = ring->TryDequeue();
    ASSERT_TRUE(got.ok());
    auto value = ring->ReadSlot(*got);
    ASSERT_TRUE(value.ok());
    EXPECT_EQ(*value, 7u);
    ASSERT_TRUE(ring->CommitDequeue(*got).ok());
}

// ---------------------------------------------------------------------------
// Full / empty
// ---------------------------------------------------------------------------

TEST(MpmcRingFullEmptyTest, FullRefusesEnqueueAndReportsFull) {
    U64Fixture<4> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 4, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    for (uint64_t i = 0; i < 4; ++i) {
        auto seq = ring->TryEnqueue();
        ASSERT_TRUE(seq.ok()) << "i=" << i;
        ASSERT_TRUE(ring->CommitEnqueue(*seq, i).ok());
    }
    EXPECT_TRUE(ring->IsFull());
    EXPECT_FALSE(ring->IsEmpty());

    auto blocked = ring->TryEnqueue();
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kResourceExhausted);
}

TEST(MpmcRingFullEmptyTest, EmptyRefusesDequeueAndReportsEmpty) {
    U64Fixture<4> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 4, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    EXPECT_TRUE(ring->IsEmpty());
    EXPECT_FALSE(ring->IsFull());

    auto blocked = ring->TryDequeue();
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kWouldBlock);
}

TEST(MpmcRingFullEmptyTest, ReservedButUncommittedSlotIsNotDequeueable) {
    U64Fixture<4> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 4, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    auto seq = ring->TryEnqueue();
    ASSERT_TRUE(seq.ok());
    // The position is reserved but not committed: a consumer must observe
    // "empty", not garbage.
    auto blocked = ring->TryDequeue();
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kWouldBlock);

    ASSERT_TRUE(ring->CommitEnqueue(*seq, 9).ok());
    auto got = ring->TryDequeue();
    ASSERT_TRUE(got.ok());
    EXPECT_EQ(ring->ReadSlot(*got).value(), 9u);
    ASSERT_TRUE(ring->CommitDequeue(*got).ok());
}

TEST(MpmcRingFullEmptyTest, DequeueWithoutCommitStillBlocksProducers) {
    U64Fixture<2> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, 2, sizeof(uint64_t),
                                         alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    // Fill both slots, then claim one without releasing it: the ring must
    // still count as full until CommitDequeue lands.
    for (uint64_t i = 0; i < 2; ++i) {
        auto seq = ring->TryEnqueue();
        ASSERT_TRUE(seq.ok());
        ASSERT_TRUE(ring->CommitEnqueue(*seq, i).ok());
    }
    auto got = ring->TryDequeue();
    ASSERT_TRUE(got.ok());
    // The claimed slot is not yet released. The cursor-based snapshot already
    // reports room (enq - deq = 1 < 2), but the authoritative full signal is
    // the slot sequence: the producer is still rejected because the claimed
    // slot has not been released by CommitDequeue.
    EXPECT_FALSE(ring->IsFull());

    auto blocked = ring->TryEnqueue();
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kResourceExhausted);

    // Releasing the slot makes room again.
    ASSERT_TRUE(ring->CommitDequeue(*got).ok());
    EXPECT_FALSE(ring->IsFull());
    auto seq = ring->TryEnqueue();
    ASSERT_TRUE(seq.ok());
    EXPECT_EQ(*seq, 2u);
}

// ---------------------------------------------------------------------------
// Wrap-around
// ---------------------------------------------------------------------------

TEST(MpmcRingWrapTest, WrapAroundPreservesOrderAndData) {
    constexpr uint64_t kCapacity = 4;
    constexpr uint64_t kRounds = 1000;
    U64Fixture<kCapacity> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, kCapacity,
                                         sizeof(uint64_t), alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    // Keep the ring half full while forcing many wrap cycles.
    for (uint64_t i = 0; i < kRounds; ++i) {
        auto seq = ring->TryEnqueue();
        ASSERT_TRUE(seq.ok()) << "i=" << i;
        EXPECT_EQ(*seq, i);
        ASSERT_TRUE(ring->CommitEnqueue(*seq, i * 3 + 1).ok());

        if (i >= 1) {
            auto got = ring->TryDequeue();
            ASSERT_TRUE(got.ok()) << "i=" << i;
            auto value = ring->ReadSlot(*got);
            ASSERT_TRUE(value.ok());
            const uint64_t expected_pos = i - 1;
            EXPECT_EQ(*got, expected_pos);
            EXPECT_EQ(*value, expected_pos * 3 + 1);
            ASSERT_TRUE(ring->CommitDequeue(*got).ok());
        }
    }
}

TEST(MpmcRingWrapTest, SlotReuseAfterWrapDoesNotCorruptNeighbors) {
    constexpr uint64_t kCapacity = 2;
    U64Fixture<kCapacity> f;
    auto ring = MpmcRing<uint64_t>::Init(f.storage, kCapacity,
                                         sizeof(uint64_t), alignof(uint64_t));
    ASSERT_TRUE(ring.ok());

    // Fill the ring completely, drain one slot, refill, then drain all:
    // exercises the slot-state transitions across a wrap boundary.
    for (uint64_t cycle = 0; cycle < 100; ++cycle) {
        auto s1 = ring->TryEnqueue();
        auto s2 = ring->TryEnqueue();
        ASSERT_TRUE(s1.ok());
        ASSERT_TRUE(s2.ok());
        ASSERT_TRUE(ring->CommitEnqueue(*s1, cycle * 2).ok());
        ASSERT_TRUE(ring->CommitEnqueue(*s2, cycle * 2 + 1).ok());
        EXPECT_TRUE(ring->IsFull());

        auto g1 = ring->TryDequeue();
        ASSERT_TRUE(g1.ok());
        EXPECT_EQ(ring->ReadSlot(*g1).value(), cycle * 2);
        ASSERT_TRUE(ring->CommitDequeue(*g1).ok());

        // Refill the just-freed slot while the other stays occupied.
        auto s3 = ring->TryEnqueue();
        ASSERT_TRUE(s3.ok());
        ASSERT_TRUE(ring->CommitEnqueue(*s3, 0xFFFF0000u + cycle).ok());

        auto g2 = ring->TryDequeue();
        ASSERT_TRUE(g2.ok());
        EXPECT_EQ(ring->ReadSlot(*g2).value(), cycle * 2 + 1);
        ASSERT_TRUE(ring->CommitDequeue(*g2).ok());

        auto g3 = ring->TryDequeue();
        ASSERT_TRUE(g3.ok());
        EXPECT_EQ(ring->ReadSlot(*g3).value(), 0xFFFF0000u + cycle);
        ASSERT_TRUE(ring->CommitDequeue(*g3).ok());
    }
}

// ---------------------------------------------------------------------------
// Multithreaded behavior
// ---------------------------------------------------------------------------

TEST(MpmcRingConcurrentTest, FourProducersFourConsumersNoLossNoDuplication) {
    constexpr uint64_t kCapacity = 1024;
    constexpr uint64_t kPerProducer = 20000;
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr uint64_t kTotal = kPerProducer * kProducers;

    U64Fixture<kCapacity> f;
    auto init = MpmcRing<uint64_t>::Init(f.storage, kCapacity,
                                         sizeof(uint64_t), alignof(uint64_t));
    ASSERT_TRUE(init.ok());
    // Views are copied into the threads to mimic separate process mappings.
    const MpmcRing<uint64_t> ring = *init;

    // Per-producer received multisets: value v of producer p lands in
    // inbox[p]; at the end each inbox[p] must equal [0, kPerProducer).
    std::vector<std::vector<uint64_t>> inbox(kProducers);
    std::vector<std::mutex> inbox_mu(kProducers);

    std::atomic<uint64_t> consumed{0};
    std::atomic<bool> start{false};

    auto producer = [&](int id) {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (uint64_t i = 0; i < kPerProducer; ++i) {
            const uint64_t token = (static_cast<uint64_t>(id) << 56) | i;
            for (;;) {
                auto seq = ring.TryEnqueue();
                if (seq.ok()) {
                    ASSERT_TRUE(ring.CommitEnqueue(*seq, token).ok());
                    break;
                }
                ASSERT_EQ(seq.status().code(), StatusCode::kResourceExhausted);
                std::this_thread::yield();
            }
        }
    };

    auto consumer = [&]() {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (;;) {
            if (consumed.load(std::memory_order_acquire) >= kTotal) {
                return;
            }
            auto got = ring.TryDequeue();
            if (!got.ok()) {
                ASSERT_EQ(got.status().code(), StatusCode::kWouldBlock);
                std::this_thread::yield();
                continue;
            }
            auto value = ring.ReadSlot(*got);
            ASSERT_TRUE(value.ok());
            ASSERT_TRUE(ring.CommitDequeue(*got).ok());

            const uint64_t token = *value;
            const int producer_id = static_cast<int>(token >> 56);
            const uint64_t index = token & 0x00FFFFFFFFFFFFFFULL;
            ASSERT_GE(producer_id, 0);
            ASSERT_LT(producer_id, kProducers);
            ASSERT_LT(index, kPerProducer);
            {
                std::lock_guard<std::mutex> lock(inbox_mu[producer_id]);
                inbox[producer_id].push_back(index);
            }
            consumed.fetch_add(1, std::memory_order_acq_rel);
        }
    };

    std::vector<std::thread> threads;
    for (int p = 0; p < kProducers; ++p) {
        threads.emplace_back(producer, p);
    }
    for (int c = 0; c < kConsumers; ++c) {
        threads.emplace_back(consumer);
    }
    start.store(true, std::memory_order_release);
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(consumed.load(), kTotal);
    for (int p = 0; p < kProducers; ++p) {
        auto& got = inbox[p];
        ASSERT_EQ(got.size(), kPerProducer) << "producer " << p;
        std::sort(got.begin(), got.end());
        // Exactly the sequence [0, kPerProducer): no loss, no duplication.
        for (uint64_t i = 0; i < kPerProducer; ++i) {
            EXPECT_EQ(got[i], i) << "producer " << p << " index " << i;
        }
    }
    EXPECT_TRUE(ring.IsEmpty());
}

// Concurrent reservations must partition the logical positions: every
// sequence number is reserved by exactly one producer.
TEST(MpmcRingConcurrentTest, ConcurrentReservationsAreUnique) {
    constexpr uint64_t kCapacity = 256;
    constexpr uint64_t kPerProducer = 8000;
    constexpr int kProducers = 8;

    U64Fixture<kCapacity> f;
    auto init = MpmcRing<uint64_t>::Init(f.storage, kCapacity,
                                         sizeof(uint64_t), alignof(uint64_t));
    ASSERT_TRUE(init.ok());
    const MpmcRing<uint64_t> ring = *init;

    std::vector<uint64_t> seen(kPerProducer * kProducers, 0);
    std::atomic<int> producers_done{0};
    std::atomic<bool> start{false};

    // A dedicated consumer keeps the ring draining so producers never block
    // forever on a full ring.
    std::atomic<bool> stop_consumer{false};
    std::thread consumer([&]() {
        while (!stop_consumer.load(std::memory_order_acquire)) {
            auto got = ring.TryDequeue();
            if (got.ok()) {
                ASSERT_TRUE(ring.CommitDequeue(*got).ok());
            } else {
                std::this_thread::yield();
            }
        }
    });

    auto producer = [&](int) {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (uint64_t i = 0; i < kPerProducer; ++i) {
            for (;;) {
                auto seq = ring.TryEnqueue();
                if (seq.ok()) {
                    ASSERT_LT(*seq, seen.size());
                    // Exactly one producer may observe this reservation.
                    const uint64_t before =
                        std::atomic_ref(seen[*seq])
                            .fetch_add(1, std::memory_order_acq_rel);
                    ASSERT_EQ(before, 0u) << "sequence " << *seq
                                          << " reserved twice";
                    ASSERT_TRUE(ring.CommitEnqueue(*seq, *seq).ok());
                    break;
                }
                std::this_thread::yield();
            }
        }
        producers_done.fetch_add(1, std::memory_order_acq_rel);
    };

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back(producer, p);
    }
    start.store(true, std::memory_order_release);
    for (auto& t : producers) {
        t.join();
    }
    while (true) {
        auto got = ring.TryDequeue();
        if (!got.ok()) {
            break;
        }
        ASSERT_TRUE(ring.CommitDequeue(*got).ok());
    }
    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    EXPECT_EQ(producers_done.load(), kProducers);
    for (uint64_t count : seen) {
        EXPECT_EQ(count, 1u);
    }
}

}  // namespace
}  // namespace mino
