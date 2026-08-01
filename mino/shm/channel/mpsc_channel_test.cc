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

#include "mino/shm/channel/mpsc_channel.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <set>
#include <thread>
#include <type_traits>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/queue_full_policy.h"

namespace mino {
namespace {

// ---------------------------------------------------------------------------
// Compile-time layout contract
// ---------------------------------------------------------------------------

static_assert(std::is_trivially_copyable_v<MpscChannel>,
              "MpscChannel must be a trivially copyable view");
static_assert(sizeof(MpscReservationMeta) == 64,
              "MPSC sidecar carries tagged claim and full process incarnation");

// ---------------------------------------------------------------------------
// Fixture: 64-byte-aligned shared-memory stand-in
// ---------------------------------------------------------------------------

template <uint64_t kCapacity>
struct ChannelFixture {
    static constexpr uint64_t kBytes = MpscChannel::RequiredSize(kCapacity);

    ChannelFixture()
        : storage(static_cast<unsigned char*>(
              ::operator new(kBytes, std::align_val_t(64)))) {
        std::memset(storage, 0, kBytes);
    }
    ~ChannelFixture() { ::operator delete(storage, kBytes, std::align_val_t(64)); }

    ChannelFixture(const ChannelFixture&) = delete;
    ChannelFixture& operator=(const ChannelFixture&) = delete;

    unsigned char* storage;
};

MpscChannel::ProducerIdentity MakeIdentity(uint64_t publisher_id) {
    MpscChannel::ProducerIdentity id;
    id.owner = ProcessIdentity::Current();
    id.publisher_id = publisher_id;
    return id;
}

// Fills the reserved slot with deterministic content derived from `tag`.
void FillSlot(MpscChannel::Reservation& res, uint32_t tag) {
    res->msg_type = 0x2000 + tag;
    res->schema_version = (1u << 16) | 0u;
    res->schema_short_id = 0xABCDEF00ULL + tag;
    res->schema_layout_version = 1;
    res->timestamp_ns = 1'000'000 + tag;
    res->payload.offset = 0x4000 + 16ULL * tag;
    res->payload.generation = 1;
    res->payload.region_id = 1;
    res->payload_len = 64 + tag;
    res->flags = 0;
}

// ---------------------------------------------------------------------------
// Init / Attach
// ---------------------------------------------------------------------------

TEST(MpscChannelTest, InitRejectsBadArgs) {
    ChannelFixture<64> fixture;
    EXPECT_FALSE(MpscChannel::Init(nullptr, 64).ok());
    EXPECT_FALSE(MpscChannel::Init(fixture.storage, 0).ok());
    EXPECT_FALSE(MpscChannel::Init(fixture.storage, 63).ok());   // < 64
    EXPECT_FALSE(MpscChannel::Init(fixture.storage, 96).ok());   // not pow2
    // Misaligned base.
    EXPECT_FALSE(MpscChannel::Init(fixture.storage + 8, 64).ok());
}

TEST(MpscChannelTest, InitThenAttachRoundTrip) {
    ChannelFixture<64> fixture;
    auto created = MpscChannel::Init(fixture.storage, 64);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    EXPECT_EQ(created->capacity(), 64u);
    EXPECT_TRUE(created->IsEmpty());
    EXPECT_FALSE(created->IsFull());

    auto attached = MpscChannel::Attach(fixture.storage);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_EQ(attached->capacity(), 64u);
}

TEST(MpscChannelTest, AttachRejectsCorruptMagic) {
    ChannelFixture<64> fixture;
    ASSERT_TRUE(MpscChannel::Init(fixture.storage, 64).ok());
    // Corrupt the magic.
    std::memset(fixture.storage, 0xFF, 8);
    EXPECT_FALSE(MpscChannel::Attach(fixture.storage).ok());
}

// ---------------------------------------------------------------------------
// Single producer / single consumer
// ---------------------------------------------------------------------------

TEST(MpscChannelTest, SingleProducerCommitAndPoll) {
    ChannelFixture<64> fixture;
    auto ch = MpscChannel::Init(fixture.storage, 64);
    ASSERT_TRUE(ch.ok());

    const auto id = MakeIdentity(7);
    auto res = ch->Reserve(id);
    ASSERT_TRUE(res.ok()) << res.status().ToString();
    ASSERT_TRUE(res->active());
    FillSlot(*res, 42);
    const uint64_t seq = res->sequence();
    ASSERT_TRUE(std::move(*res).Commit().ok());

    EXPECT_EQ(ch->Size(), 1u);
    auto borrow = ch->Poll();
    ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
    EXPECT_EQ(borrow->slot()->sequence_num, seq);
    EXPECT_EQ(borrow->slot()->msg_type, 0x2000u + 42u);
    EXPECT_EQ(borrow->slot()->payload_len, 64u + 42u);
    ASSERT_TRUE(std::move(*borrow).Ack().ok());
    EXPECT_TRUE(ch->IsEmpty());
}

TEST(MpscChannelTest, AbortLeavesSkippableTombstone) {
    ChannelFixture<64> fixture;
    auto ch = MpscChannel::Init(fixture.storage, 64);
    ASSERT_TRUE(ch.ok());
    const auto id = MakeIdentity(1);

    // seq 0: abort -> tombstone; seq 1: commit.
    auto r0 = ch->Reserve(id);
    ASSERT_TRUE(r0.ok());
    FillSlot(*r0, 1);
    ASSERT_TRUE(std::move(*r0).Abort().ok());

    auto r1 = ch->Reserve(id);
    ASSERT_TRUE(r1.ok());
    FillSlot(*r1, 2);
    ASSERT_TRUE(std::move(*r1).Commit().ok());

    // Poll must transparently skip the tombstone and deliver seq 1.
    auto borrow = ch->Poll();
    ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
    EXPECT_EQ(borrow->slot()->sequence_num, 1u);
    EXPECT_EQ(borrow->slot()->msg_type, 0x2000u + 2u);
    ASSERT_TRUE(std::move(*borrow).Ack().ok());
    EXPECT_TRUE(ch->IsEmpty());
}

TEST(MpscChannelTest, ConsumedAbortedSequenceIsIndeterminate) {
    ChannelFixture<64> fixture;
    auto ch = MpscChannel::Init(fixture.storage, 64);
    ASSERT_TRUE(ch.ok());

    auto res = ch->Reserve(MakeIdentity(1));
    ASSERT_TRUE(res.ok());
    FillSlot(*res, 1);
    const ShmHandle payload = res->slot()->payload;
    const uint64_t sequence = res->sequence();
    ASSERT_TRUE(std::move(*res).Abort().ok());
    EXPECT_EQ(ch->InspectPublication(sequence, payload),
              MpscChannel::PublicationVisibility::kNotVisible);

    EXPECT_EQ(ch->Poll().status().code(), StatusCode::kWouldBlock);
    EXPECT_EQ(ch->InspectPublication(sequence, payload),
              MpscChannel::PublicationVisibility::kIndeterminate);
}

// ---------------------------------------------------------------------------
// Ordered prefix: out-of-order commit still delivers in sequence order
// ---------------------------------------------------------------------------

TEST(MpscChannelTest, OutOfOrderCommitPreservesOrder) {
    ChannelFixture<64> fixture;
    auto ch = MpscChannel::Init(fixture.storage, 64);
    ASSERT_TRUE(ch.ok());
    const auto id = MakeIdentity(1);

    // Reserve three in order, commit out of order (2, 0, 1).
    auto r0 = ch->Reserve(id);
    auto r1 = ch->Reserve(id);
    auto r2 = ch->Reserve(id);
    ASSERT_TRUE(r0.ok() && r1.ok() && r2.ok());
    FillSlot(*r0, 0);
    FillSlot(*r1, 1);
    FillSlot(*r2, 2);

    // Commit r2 first: the prefix must NOT advance past 0/1.
    ASSERT_TRUE(std::move(*r2).Commit().ok());
    EXPECT_EQ(ch->Poll().status().code(), StatusCode::kWouldBlock);

    ASSERT_TRUE(std::move(*r0).Commit().ok());
    // seq 0 ready, but seq 1 not yet: only seq 0 is deliverable.
    auto b0 = ch->Poll();
    ASSERT_TRUE(b0.ok());
    EXPECT_EQ(b0->slot()->sequence_num, 0u);
    ASSERT_TRUE(std::move(*b0).Ack().ok());
    // seq 1 still pending: nothing more despite seq 2 being committed.
    EXPECT_EQ(ch->Poll().status().code(), StatusCode::kWouldBlock);

    ASSERT_TRUE(std::move(*r1).Commit().ok());
    auto b1 = ch->Poll();
    ASSERT_TRUE(b1.ok());
    EXPECT_EQ(b1->slot()->sequence_num, 1u);
    ASSERT_TRUE(std::move(*b1).Ack().ok());
    auto b2 = ch->Poll();
    ASSERT_TRUE(b2.ok());
    EXPECT_EQ(b2->slot()->sequence_num, 2u);
    ASSERT_TRUE(std::move(*b2).Ack().ok());
    EXPECT_TRUE(ch->IsEmpty());
}

// ---------------------------------------------------------------------------
// Wrap-around: reuse slots across eras (INV-17 conservation)
// ---------------------------------------------------------------------------

TEST(MpscChannelTest, WrapAroundReusesSlotsConsistently) {
    constexpr uint64_t kCap = 64;
    ChannelFixture<kCap> fixture;
    auto ch = MpscChannel::Init(fixture.storage, kCap);
    ASSERT_TRUE(ch.ok());
    const auto id = MakeIdentity(1);

    constexpr uint64_t kTotal = kCap * 4 + 7;  // several wraps
    for (uint64_t i = 0; i < kTotal; ++i) {
        auto res = ch->Reserve(id);
        ASSERT_TRUE(res.ok()) << "i=" << i << " " << res.status().ToString();
        FillSlot(*res, static_cast<uint32_t>(i));
        ASSERT_TRUE(std::move(*res).Commit().ok());

        auto borrow = ch->Poll();
        ASSERT_TRUE(borrow.ok()) << "i=" << i;
        EXPECT_EQ(borrow->slot()->sequence_num, i) << "i=" << i;
        EXPECT_EQ(borrow->slot()->msg_type, 0x2000u + static_cast<uint32_t>(i));
        ASSERT_TRUE(std::move(*borrow).Ack().ok());
    }
    EXPECT_TRUE(ch->IsEmpty());
}

// ---------------------------------------------------------------------------
// Full queue: kFail, then kDropOldest frees space
// ---------------------------------------------------------------------------

TEST(MpscChannelTest, FullQueueKFail) {
    constexpr uint64_t kCap = 64;
    ChannelFixture<kCap> fixture;
    auto ch = MpscChannel::Init(fixture.storage, kCap);
    ASSERT_TRUE(ch.ok());
    const auto id = MakeIdentity(1);

    // Fill the whole ring without consuming.
    for (uint64_t i = 0; i < kCap; ++i) {
        auto res = ch->Reserve(id, QueueFullPolicy::kFail);
        ASSERT_TRUE(res.ok()) << "i=" << i;
        FillSlot(*res, static_cast<uint32_t>(i));
        ASSERT_TRUE(std::move(*res).Commit().ok());
    }
    // Next reserve must fail under kFail.
    auto blocked = ch->Reserve(id, QueueFullPolicy::kFail);
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kResourceExhausted);
}

TEST(MpscChannelTest, DropOldestFreesSpaceAndSkipsVictim) {
    constexpr uint64_t kCap = 64;
    ChannelFixture<kCap> fixture;
    auto ch = MpscChannel::Init(fixture.storage, kCap);
    ASSERT_TRUE(ch.ok());
    const auto id = MakeIdentity(1);

    for (uint64_t i = 0; i < kCap; ++i) {
        auto res = ch->Reserve(id, QueueFullPolicy::kFail);
        ASSERT_TRUE(res.ok());
        FillSlot(*res, static_cast<uint32_t>(i));
        ASSERT_TRUE(std::move(*res).Commit().ok());
    }
    // Drop the oldest (seq 0) to make room for one more.
    auto res = ch->Reserve(id, QueueFullPolicy::kDropOldest);
    ASSERT_TRUE(res.ok()) << res.status().ToString();
    FillSlot(*res, 0xBEEF);
    ASSERT_TRUE(std::move(*res).Commit().ok());

    // Consumer now sees seq 1 first (seq 0 was dropped), through to the new one.
    auto first = ch->Poll();
    ASSERT_TRUE(first.ok());
    EXPECT_EQ(first->slot()->sequence_num, 1u);
    ASSERT_TRUE(std::move(*first).Ack().ok());
}

// ---------------------------------------------------------------------------
// Multi-producer concurrency: strict order + conservation (INV-17)
// ---------------------------------------------------------------------------

// N producer threads each publish kPerProducer messages; the single consumer
// must observe exactly N*kPerProducer messages in strictly increasing
// sequence order with no gaps and no duplicates.
TEST(MpscChannelTest, MultiProducerConcurrentConservation) {
    constexpr uint64_t kCap = 128;
    ChannelFixture<kCap> fixture;
    auto ch = MpscChannel::Init(fixture.storage, kCap);
    ASSERT_TRUE(ch.ok());

    constexpr int kProducers = 8;
    constexpr uint64_t kPerProducer = 500;
    constexpr uint64_t kTotal = kProducers * kPerProducer;

    std::atomic<uint64_t> produced{0};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            const auto id = MakeIdentity(static_cast<uint64_t>(p + 1));
            for (uint64_t i = 0; i < kPerProducer; ++i) {
                MpscChannel::Reservation res;
                // Spin under kBlock until a slot frees up.
                for (;;) {
                    auto attempt = ch->Reserve(id, QueueFullPolicy::kBlock);
                    if (attempt.ok()) {
                        res = std::move(*attempt);
                        break;
                    }
                }
                FillSlot(res, static_cast<uint32_t>(p));
                ASSERT_TRUE(std::move(res).Commit().ok());
                produced.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Single consumer: drain until all messages observed, in strict order.
    uint64_t expected_seq = 0;
    uint64_t consumed = 0;
    std::set<uint64_t> seen_publishers;
    while (consumed < kTotal) {
        auto borrow = ch->Poll();
        if (!borrow.ok()) {
            std::this_thread::yield();
            continue;
        }
        EXPECT_EQ(borrow->slot()->sequence_num, expected_seq)
            << "gap or reorder at consumed=" << consumed;
        seen_publishers.insert(borrow->slot()->msg_type - 0x2000u);
        ASSERT_TRUE(std::move(*borrow).Ack().ok());
        ++expected_seq;
        ++consumed;
    }
    for (auto& t : producers) {
        t.join();
    }
    EXPECT_EQ(consumed, kTotal);
    EXPECT_EQ(produced.load(), kTotal);
    EXPECT_TRUE(ch->IsEmpty());
}

TEST(MpscChannelTest, PublisherConcurrencyMatrix) {
    constexpr uint64_t kCapacity = 256;
    constexpr uint64_t kPerProducer = 16;
    constexpr std::array<int, 5> kPublisherCounts = {1, 2, 8, 32, 128};
    ChannelFixture<kCapacity> fixture;

    for (const int publisher_count : kPublisherCounts) {
        auto ch = MpscChannel::Init(fixture.storage, kCapacity);
        ASSERT_TRUE(ch.ok()) << "publishers=" << publisher_count;
        const uint64_t total =
            static_cast<uint64_t>(publisher_count) * kPerProducer;
        std::atomic<bool> start{false};
        std::atomic<uint64_t> failures{0};
        std::vector<std::thread> producers;
        producers.reserve(static_cast<size_t>(publisher_count));
        for (int p = 0; p < publisher_count; ++p) {
            producers.emplace_back([&, p]() {
                const auto identity = MakeIdentity(static_cast<uint64_t>(p + 1));
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (uint64_t i = 0; i < kPerProducer; ++i) {
                    MpscChannel::Reservation reservation;
                    for (;;) {
                        auto attempt = ch->TryReserve(identity);
                        if (attempt.ok()) {
                            reservation = std::move(*attempt);
                            break;
                        }
                        const StatusCode code = attempt.status().code();
                        if (code != StatusCode::kResourceExhausted &&
                            code != StatusCode::kWouldBlock) {
                            failures.fetch_add(1, std::memory_order_relaxed);
                            return;
                        }
                        std::this_thread::yield();
                    }
                    FillSlot(reservation, static_cast<uint32_t>(p));
                    if (!std::move(reservation).Commit().ok()) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                        return;
                    }
                }
            });
        }

        start.store(true, std::memory_order_release);
        uint64_t consumed = 0;
        while (consumed < total) {
            auto borrow = ch->Poll();
            if (!borrow.ok()) {
                ASSERT_EQ(borrow.status().code(), StatusCode::kWouldBlock);
                std::this_thread::yield();
                continue;
            }
            EXPECT_EQ(borrow->slot()->sequence_num, consumed)
                << "publishers=" << publisher_count;
            ASSERT_TRUE(std::move(*borrow).Ack().ok());
            ++consumed;
        }
        for (auto& producer : producers) {
            producer.join();
        }
        EXPECT_EQ(failures.load(std::memory_order_relaxed), 0u)
            << "publishers=" << publisher_count;
        EXPECT_TRUE(ch->IsEmpty());
    }
}

// Destroying a live Reservation without Commit() must stamp an ABORTED
// tombstone (RAII abort), which the consumer then skips transparently.
TEST(MpscChannelTest, ReservationDestructorAborts) {
    constexpr uint64_t kCap = 64;
    ChannelFixture<kCap> fixture;
    auto ch = MpscChannel::Init(fixture.storage, kCap);
    ASSERT_TRUE(ch.ok());
    const auto id = MakeIdentity(1);

    {
        auto res = ch->Reserve(id);
        ASSERT_TRUE(res.ok());
        FillSlot(*res, 9);
        // Fall out of scope without Commit/Abort -> destructor aborts.
    }
    // Now commit a following message; the tombstone at seq 0 must be skipped.
    auto res = ch->Reserve(id);
    ASSERT_TRUE(res.ok());
    FillSlot(*res, 10);
    ASSERT_TRUE(std::move(*res).Commit().ok());

    auto borrow = ch->Poll();
    ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
    EXPECT_EQ(borrow->slot()->sequence_num, 1u);
    EXPECT_EQ(borrow->slot()->msg_type, 0x2000u + 10u);
    ASSERT_TRUE(std::move(*borrow).Ack().ok());
    EXPECT_TRUE(ch->IsEmpty());
}

// Recovery must never reclaim a reservation whose owner is still alive, no
// matter how far past the lease it has aged (design doc 9.5: "暂停但仍有效的
// Producer 不得被误回收"; never judge a crash by timeout alone). Here the
// owner is this very process — IsOwnerAlive() is trivially true on every
// platform — so even a zero-length lease reclaims nothing. The reservation is
// deliberately leaked (never destructed) so it stays in kWriting rather than
// being RAII-aborted, simulating a live-but-stalled producer.
TEST(MpscChannelTest, LiveOwnerIsNeverReclaimedDespiteExpiredLease) {
    constexpr uint64_t kCap = 64;
    ChannelFixture<kCap> fixture;
    auto ch = MpscChannel::Init(fixture.storage, kCap);
    ASSERT_TRUE(ch.ok());
    const auto id = MakeIdentity(1);

    auto res = ch->Reserve(id);
    ASSERT_TRUE(res.ok());
    FillSlot(*res, 7);
    // Leak the Reservation so its destructor never aborts the slot; it stays
    // kWriting with this (alive) process as the recorded owner.
    auto* leaked = new MpscChannel::Reservation(std::move(*res));

    // lease_ns=0 selects the default, but pass an explicitly tiny lease: the
    // owner is alive, so the liveness check (not the lease) decides, and the
    // scan must reclaim nothing.
    const uint64_t aborted = ch->AbortOrphanedReservations(
        /*now_ns=*/UINT64_MAX / 2, /*lease_ns=*/1);
    EXPECT_EQ(aborted, 0u)
        << "a live owner must never be reclaimed, lease notwithstanding";

    // The consumer is wedged behind the live kWriting slot: no tombstone.
    auto blocked = ch->Poll();
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kWouldBlock);

    // Reclaim the leaked Reservation now that the assertions are done (its
    // destructor aborts the slot, which no longer matters); this keeps the
    // LeakSanitizer-clean contract without weakening the live-owner test.
    delete leaked;
}

// The lease is the FIRST gate: a reservation younger than the lease is never
// reclaimed, even before owner liveness is consulted. This protects a
// live-but-slow producer inside its legitimate Reserve -> Commit window
// (design doc 9.5). A huge lease makes the fresh reservation look young.
TEST(MpscChannelTest, UnexpiredLeaseProtectsFreshReservation) {
    constexpr uint64_t kCap = 64;
    ChannelFixture<kCap> fixture;
    auto ch = MpscChannel::Init(fixture.storage, kCap);
    ASSERT_TRUE(ch.ok());
    const auto id = MakeIdentity(1);

    auto res = ch->Reserve(id);
    ASSERT_TRUE(res.ok());
    FillSlot(*res, 8);
    auto* leaked = new MpscChannel::Reservation(std::move(*res));

    // now_ns=0 makes now - reservation_timestamp underflow to a huge value in
    // unsigned arithmetic; pass a lease of UINT64_MAX so the reservation is
    // always "younger than the lease" regardless of its real timestamp.
    const uint64_t aborted = ch->AbortOrphanedReservations(
        /*now_ns=*/0, /*lease_ns=*/UINT64_MAX);
    EXPECT_EQ(aborted, 0u)
        << "an unexpired lease must protect the reservation before liveness "
           "is even consulted";

    // Reclaim the leaked Reservation after the assertion (see the live-owner
    // test above); keeps LeakSanitizer clean.
    delete leaked;
}

}  // namespace
}  // namespace mino
