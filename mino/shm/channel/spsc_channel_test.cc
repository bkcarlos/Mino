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

#include "mino/shm/channel/spsc_channel.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <thread>
#include <type_traits>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/queue_full_policy.h"

namespace mino {
namespace {

// ---------------------------------------------------------------------------
// Compile-time layout contract
// ---------------------------------------------------------------------------

using Control = SpscChannel::ControlBlock;
static_assert(sizeof(Control) == 3 * 64,
              "control block must occupy exactly three cache lines");
static_assert(alignof(Control) == 64);
static_assert(std::is_standard_layout_v<Control>);
static_assert(offsetof(Control, producer_cursor) == 64,
              "producer cursor must start its own cache line");
static_assert(offsetof(Control, consumer_cursor) == 128,
              "consumer cursor must start its own cache line");
static_assert(std::is_trivially_copyable_v<SpscChannel>,
              "SpscChannel must be a trivially copyable view");

// ---------------------------------------------------------------------------
// Fixture: 64-byte-aligned shared-memory stand-in
// ---------------------------------------------------------------------------

template <uint64_t kCapacity>
struct ChannelFixture {
    static constexpr uint64_t kBytes = SpscChannel::RequiredSize(kCapacity);

    ChannelFixture()
        : storage(static_cast<unsigned char*>(
              ::operator new(kBytes, std::align_val_t(64)))) {
        std::memset(storage, 0, kBytes);
    }
    ~ChannelFixture() {
        ::operator delete(storage, kBytes, std::align_val_t(64));
    }

    ChannelFixture(const ChannelFixture&) = delete;
    ChannelFixture& operator=(const ChannelFixture&) = delete;

    unsigned char* storage;
};

// Fills the reserved slot with deterministic content derived from `tag`.
void FillSlot(SpscChannel::Reservation& res, uint32_t tag) {
    res->msg_type = 0x1000 + tag;
    res->schema_version = (1u << 16) | 0u;
    res->schema_short_id = 0xABCDEF00ULL + tag;
    res->schema_layout_version = 1;
    res->timestamp_ns = 1'000'000 + tag;
    res->payload.offset = 0x2000 + 16ULL * tag;
    res->payload.generation = 1;
    res->payload.region_id = 1;
    res->payload_len = 100 + tag;
    res->flags = 0;
}

// ---------------------------------------------------------------------------
// Init / Attach
// ---------------------------------------------------------------------------

TEST(SpscInitTest, InitSucceeds) {
    ChannelFixture<8> f;
    auto ch = SpscChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok()) << ch.status().ToString();
    EXPECT_EQ(ch->capacity(), 8u);
    EXPECT_TRUE(ch->IsEmpty());
    EXPECT_FALSE(ch->IsFull());
    EXPECT_EQ(ch->Size(), 0u);
}

TEST(SpscInitTest, RejectsBadCapacity) {
    ChannelFixture<8> f;
    EXPECT_FALSE(SpscChannel::Init(f.storage, 0).ok());
    EXPECT_FALSE(SpscChannel::Init(f.storage, 1).ok());
    EXPECT_FALSE(SpscChannel::Init(f.storage, 3).ok());   // not power of two
    EXPECT_FALSE(SpscChannel::Init(f.storage, 6).ok());
    EXPECT_FALSE(SpscChannel::Init(nullptr, 8).ok());
}

TEST(SpscInitTest, AttachValidatesMagic) {
    ChannelFixture<8> f;
    auto ch = SpscChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    // Attach reads the capacity from the control block (single source of
    // truth, same style as MpmcRing::Attach).
    auto attached = SpscChannel::Attach(f.storage);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_EQ(attached->capacity(), 8u);

    // Uninitialized memory is rejected.
    ChannelFixture<8> g;
    EXPECT_FALSE(SpscChannel::Attach(g.storage).ok());
}

// ---------------------------------------------------------------------------
// Single-threaded publish / consume
// ---------------------------------------------------------------------------

TEST(SpscBasicTest, PublishConsumeRoundTrip) {
    ChannelFixture<8> f;
    auto ch = SpscChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    // Publish one message.
    auto res = ch->Reserve();
    ASSERT_TRUE(res.ok()) << res.status().ToString();
    FillSlot(res.value(), 7);
    ASSERT_TRUE(std::move(res.value()).Commit().ok());

    EXPECT_FALSE(ch->IsEmpty());
    EXPECT_EQ(ch->Size(), 1u);

    // Consume it.
    auto borrow = ch->Poll();
    ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
    EXPECT_EQ(borrow.value()->msg_type, 0x1000u + 7u);
    EXPECT_EQ(borrow.value()->payload_len, 100u + 7u);
    EXPECT_EQ(borrow.value()->sequence_num, 0u);
    ASSERT_TRUE(std::move(borrow.value()).Ack().ok());

    EXPECT_TRUE(ch->IsEmpty());
    EXPECT_EQ(ch->Size(), 0u);

    // Queue empty again.
    auto empty = ch->Poll();
    ASSERT_FALSE(empty.ok());
    EXPECT_EQ(empty.status().code(), StatusCode::kWouldBlock);
}

TEST(SpscBasicTest, FifoOrderPreserved) {
    ChannelFixture<8> f;
    auto ch = SpscChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    constexpr uint32_t kCount = 5;
    for (uint32_t i = 0; i < kCount; ++i) {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    EXPECT_EQ(ch->Size(), kCount);

    for (uint32_t i = 0; i < kCount; ++i) {
        auto borrow = ch->Poll();
        ASSERT_TRUE(borrow.ok()) << "i=" << i;
        EXPECT_EQ(borrow.value()->msg_type, 0x1000u + i);
        EXPECT_EQ(borrow.value()->sequence_num, i);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    EXPECT_TRUE(ch->IsEmpty());
}

// ---------------------------------------------------------------------------
// Abort path: destroyed reservation leaves a skippable tombstone
// ---------------------------------------------------------------------------

TEST(SpscAbortTest, ExplicitAbortIsSkipped) {
    ChannelFixture<8> f;
    auto ch = SpscChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    // Abort slot 0, commit slot 1.
    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 0);
        ASSERT_TRUE(std::move(res.value()).Abort().ok());
    }
    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 1);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }

    // Poll transparently skips the tombstone and delivers message 1.
    auto borrow = ch->Poll();
    ASSERT_TRUE(borrow.ok());
    EXPECT_EQ(borrow.value()->msg_type, 0x1000u + 1u);
    ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    EXPECT_TRUE(ch->IsEmpty());
}

TEST(SpscAbortTest, ConsumedAbortedSequenceIsIndeterminate) {
    ChannelFixture<8> f;
    auto ch = SpscChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    auto res = ch->Reserve();
    ASSERT_TRUE(res.ok());
    FillSlot(*res, 0);
    const ShmHandle payload = res->slot()->payload;
    const uint64_t sequence =
        res->slot()->sequence_num.load(std::memory_order_relaxed);
    ASSERT_TRUE(std::move(*res).Abort().ok());
    EXPECT_EQ(ch->InspectPublication(sequence, payload),
              SpscChannel::PublicationVisibility::kNotVisible);

    EXPECT_EQ(ch->Poll().status().code(), StatusCode::kWouldBlock);
    EXPECT_EQ(ch->InspectPublication(sequence, payload),
              SpscChannel::PublicationVisibility::kIndeterminate);
}

TEST(SpscAbortTest, DestroyedReservationAutoAborts) {
    ChannelFixture<8> f;
    auto ch = SpscChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    // Let a reservation go out of scope without Commit/Abort.
    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 0);
    }  // ~Reservation must stamp a tombstone.

    // Commit a real message after it.
    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 1);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }

    auto borrow = ch->Poll();
    ASSERT_TRUE(borrow.ok());
    EXPECT_EQ(borrow.value()->msg_type, 0x1000u + 1u);
    ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
}

// ---------------------------------------------------------------------------
// QueueFullPolicy
// ---------------------------------------------------------------------------

TEST(SpscFullTest, FailPolicyRejectsWhenFull) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    for (int i = 0; i < 4; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kFail);
        ASSERT_TRUE(res.ok()) << i;
        FillSlot(res.value(), i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    ASSERT_TRUE(ch->IsFull());

    auto res = ch->Reserve(QueueFullPolicy::kFail);
    ASSERT_FALSE(res.ok());
    EXPECT_EQ(res.status().code(), StatusCode::kResourceExhausted);

    auto try_res = ch->TryReserve();
    ASSERT_FALSE(try_res.ok());
    EXPECT_EQ(try_res.status().code(), StatusCode::kWouldBlock);
}

TEST(SpscFullTest, DropNewestReportsDegraded) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    for (int i = 0; i < 4; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kDropNewest);
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    auto res = ch->Reserve(QueueFullPolicy::kDropNewest);
    ASSERT_FALSE(res.ok());
    EXPECT_EQ(res.status().code(), StatusCode::kDegraded);
    // Queue content is unchanged.
    EXPECT_EQ(ch->Size(), 4u);
}

TEST(SpscFullTest, DropOldestMakesRoom) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    for (uint32_t i = 0; i < 4; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kDropOldest);
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    ASSERT_TRUE(ch->IsFull());

    // Drop the oldest (message 0) and publish message 4 in its place.
    auto res = ch->Reserve(QueueFullPolicy::kDropOldest);
    ASSERT_TRUE(res.ok()) << res.status().ToString();
    FillSlot(res.value(), 4);
    ASSERT_TRUE(std::move(res.value()).Commit().ok());

    // Consumer now sees messages 1,2,3,4 (0 was dropped).
    for (uint32_t i = 1; i <= 4; ++i) {
        auto borrow = ch->Poll();
        ASSERT_TRUE(borrow.ok()) << i;
        EXPECT_EQ(borrow.value()->msg_type, 0x1000u + i);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    EXPECT_TRUE(ch->IsEmpty());
}

TEST(SpscFullTest, BlockPolicyWaitsForSpace) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    for (uint32_t i = 0; i < 4; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kFail);
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    ASSERT_TRUE(ch->IsFull());

    // A consumer thread drains one slot after a short delay; the blocking
    // Reserve must succeed once space appears.
    SpscChannel consumer_ch = *ch;
    std::thread consumer([consumer_ch]() mutable {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        auto borrow = consumer_ch.Poll();
        if (borrow.ok()) {
            std::move(borrow.value()).Ack().ok();
        }
    });

    auto res = ch->Reserve(QueueFullPolicy::kBlock);
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    if (res.ok()) {
        std::move(res.value()).Abort().ok();
    }
    consumer.join();
}

TEST(SpscFullTest, SamplePolicyAdmitsDeterministically) {
    ChannelFixture<2> f;
    auto ch = SpscChannel::Init(f.storage, 2);
    ASSERT_TRUE(ch.ok());

    // Fill the queue.
    for (uint32_t i = 0; i < 2; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kFail);
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    ASSERT_TRUE(ch->IsFull());

    // With sample_rate=1 every message is admitted (and blocks for space).
    // With a large rate the deterministic counter rejects most messages.
    // next_sequence is 2 here; 2 % 5 != 0 so it must be sampled out.
    auto res = ch->Reserve(QueueFullPolicy::kSample, /*sample_rate=*/5);
    ASSERT_FALSE(res.ok());
    EXPECT_EQ(res.status().code(), StatusCode::kDegraded);
}

// ---------------------------------------------------------------------------
// ABA / wraparound (INV-01): many full wraps must not duplicate or lose
// ---------------------------------------------------------------------------

TEST(SpscWrapTest, LongWrapNoAbaNoLoss) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    constexpr uint32_t kIterations = 10000;  // 2500 full wraps of capacity 4
    for (uint32_t i = 0; i < kIterations; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kBlock);
        ASSERT_TRUE(res.ok()) << i;
        FillSlot(res.value(), i & 0xFF);
        // Encode the monotonic index in payload_len so the consumer can
        // verify strict ordering across wraps.
        res.value()->payload_len = i;
        ASSERT_TRUE(std::move(res.value()).Commit().ok());

        auto borrow = ch->Poll();
        ASSERT_TRUE(borrow.ok()) << i;
        EXPECT_EQ(borrow.value()->sequence_num, i);
        EXPECT_EQ(borrow.value()->payload_len, i);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    EXPECT_TRUE(ch->IsEmpty());
}

// ---------------------------------------------------------------------------
// CRC corruption: a corrupt slot is skipped, not delivered
// ---------------------------------------------------------------------------

TEST(SpscCorruptionTest, CorruptSlotIsSkipped) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    // Commit message 0, then corrupt it in place; commit message 1 cleanly.
    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 0);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 1);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }

    // Corrupt slot 0's immutable field after publication (simulating a torn
    // write or memory fault).
    auto* raw = reinterpret_cast<IndexSlot*>(f.storage + sizeof(Control));
    raw[0].payload_len ^= 0xFF;

    auto borrow = ch->Poll();
    ASSERT_FALSE(borrow.ok());
    EXPECT_EQ(borrow.status().code(), StatusCode::kCorruption);

    // The next Poll delivers message 1: one bad slot does not wedge the queue.
    auto borrow2 = ch->Poll();
    ASSERT_TRUE(borrow2.ok());
    EXPECT_EQ(borrow2.value()->msg_type, 0x1000u + 1u);
    ASSERT_TRUE(std::move(borrow2.value()).Ack().ok());
}

TEST(SpscCorruptionTest, SequenceMismatchIsSkippedNotLivelocked) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    for (uint32_t i = 0; i < 2; ++i) {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }

    // Corrupt slot 0's sequence AND re-seal the CRC so the sequence check is
    // what fires (not the CRC check).
    auto* raw = reinterpret_cast<IndexSlot*>(f.storage + sizeof(Control));
    raw[0].sequence_num = 999;
    SealIndexSlotImmutableCrc(raw[0]);

    // Poll must report corruption AND skip the slot: a second Poll makes
    // progress instead of reporting the same slot forever.
    auto borrow = ch->Poll();
    ASSERT_FALSE(borrow.ok());
    EXPECT_EQ(borrow.status().code(), StatusCode::kCorruption);

    auto borrow2 = ch->Poll();
    ASSERT_TRUE(borrow2.ok());
    EXPECT_EQ(borrow2.value()->msg_type, 0x1000u + 1u);
    ASSERT_TRUE(std::move(borrow2.value()).Ack().ok());
    EXPECT_TRUE(ch->IsEmpty());
}

// ---------------------------------------------------------------------------
// Borrow snapshot semantics under kDropOldest (design doc 9.8)
// ---------------------------------------------------------------------------

TEST(SpscDropOldestBorrowTest, LateAckAfterDropReportsNotFound) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    // Publish one message and borrow it without acking.
    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 0);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    auto slow = ch->Poll();
    ASSERT_TRUE(slow.ok());
    EXPECT_EQ(slow.value()->msg_type, 0x1000u);

    // Fill the rest of the ring, then force DropOldest over the borrowed
    // slot. The producer is allowed to do this; the borrowed header must
    // remain intact (snapshot semantics).
    for (uint32_t i = 1; i <= 4; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kDropOldest);
        ASSERT_TRUE(res.ok()) << i;
        FillSlot(res.value(), i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }

    // The slow borrow still sees ITS message, not whatever overwrote the slot.
    EXPECT_EQ(slow.value()->msg_type, 0x1000u);
    EXPECT_EQ(slow.value()->sequence_num, 0u);

    // Its Ack is late: the message was overtaken, so Ack reports kNotFound
    // and must not disturb the queue.
    auto ack_status = std::move(slow.value()).Ack();
    EXPECT_EQ(ack_status.code(), StatusCode::kNotFound);

    // The queue must still deliver messages 1..4 in order, untouched by the
    // late Ack.
    for (uint32_t i = 1; i <= 4; ++i) {
        auto borrow = ch->Poll();
        ASSERT_TRUE(borrow.ok()) << i;
        EXPECT_EQ(borrow.value()->msg_type, 0x1000u + i);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    EXPECT_TRUE(ch->IsEmpty());
}

TEST(SpscDropOldestBorrowTest, TimelyAckStillWorks) {
    ChannelFixture<4> f;
    auto ch = SpscChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 0);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    auto borrow = ch->Poll();
    ASSERT_TRUE(borrow.ok());
    // Ack while the message is still the head: normal path.
    ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    EXPECT_TRUE(ch->IsEmpty());
}

// ---------------------------------------------------------------------------
// Cross-thread SPSC: producer and consumer in separate threads
// ---------------------------------------------------------------------------

TEST(SpscThreadTest, ProducerConsumerThreads) {
    ChannelFixture<64> f;
    auto ch = SpscChannel::Init(f.storage, 64);
    ASSERT_TRUE(ch.ok());

    constexpr uint64_t kCount = 50000;
    std::atomic<bool> start{false};

    SpscChannel prod_ch = *ch;
    SpscChannel cons_ch = *ch;

    std::thread producer([&]() mutable {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (uint64_t i = 0; i < kCount; ++i) {
            auto res = prod_ch.Reserve(QueueFullPolicy::kBlock);
            if (!res.ok()) {
                ADD_FAILURE() << "reserve failed at " << i;
                return;
            }
            FillSlot(res.value(), static_cast<uint32_t>(i & 0xFF));
            res.value()->payload_len = static_cast<uint32_t>(i);
            if (!std::move(res.value()).Commit().ok()) {
                ADD_FAILURE() << "commit failed at " << i;
                return;
            }
        }
    });

    std::thread consumer([&]() mutable {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (uint64_t i = 0; i < kCount; ++i) {
            auto borrow = cons_ch.Poll();
            while (!borrow.ok()) {
                if (borrow.status().code() != StatusCode::kWouldBlock) {
                    ADD_FAILURE() << "poll failed at " << i << " : "
                                  << borrow.status().ToString();
                    return;
                }
                borrow = cons_ch.Poll();
            }
            EXPECT_EQ(borrow.value()->sequence_num, i);
            EXPECT_EQ(borrow.value()->payload_len, static_cast<uint32_t>(i));
            if (!std::move(borrow.value()).Ack().ok()) {
                ADD_FAILURE() << "ack failed at " << i;
                return;
            }
        }
    });

    start.store(true, std::memory_order_release);
    producer.join();
    consumer.join();
    EXPECT_TRUE(ch->IsEmpty());
}

}  // namespace
}  // namespace mino
