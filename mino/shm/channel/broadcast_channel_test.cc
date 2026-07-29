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

// D2-05 broadcast channel unit tests (design doc 9.6 / 9.8, test matrix of
// the D2-05 design): layout contract, Init/Attach, basic fan-out, ACK
// bitmap + garbage collection, subscriber-set snapshot binding, unregister,
// full detection, all five QueueFullPolicy states, abort tombstones,
// corruption, wraparound, empty membership, and concurrent publisher vs.
// subscribers.

#include "mino/shm/channel/broadcast_channel.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <thread>
#include <type_traits>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/queue_full_policy.h"

namespace mino {
namespace {

// ---------------------------------------------------------------------------
// Compile-time layout contract
// ---------------------------------------------------------------------------

using Control = BroadcastChannel::ControlBlock;
using SubSlot = BroadcastChannel::SubscriberSlot;
static_assert(sizeof(Control) == 3 * 64,
              "control block must occupy exactly three cache lines");
static_assert(alignof(Control) == 64);
static_assert(std::is_standard_layout_v<Control>);
static_assert(offsetof(Control, publisher_cursor) == 64,
              "publisher cursor must start its own cache line");
static_assert(offsetof(Control, current_membership) == 128,
              "membership must start its own cache line");
static_assert(sizeof(SubSlot) == 2 * 64,
              "subscriber slot must occupy exactly two cache lines");
static_assert(alignof(SubSlot) == 64);
static_assert(std::is_standard_layout_v<SubSlot>);
static_assert(offsetof(SubSlot, cursor) == 0,
              "subscriber cursor must start the slot's first cache line");
static_assert(offsetof(SubSlot, subscriber_set_version) == 64,
              "registration metadata must start the second cache line");
static_assert(std::is_trivially_copyable_v<BroadcastChannel>,
              "BroadcastChannel must be a trivially copyable view");
static_assert(BroadcastChannel::kMaxSubscribers == kBroadcastMaxSubscribers);

TEST(BroadcastLayoutTest, RequiredSizeMatchesLayout) {
    // ControlBlock -> IndexSlot[cap] -> BroadcastSlotMeta[cap] ->
    // SubscriberSlot[64]. 64 * sizeof(IndexSlot) is 64-aligned, so the
    // sidecar starts flush; 8 * 16B of sidecar is 64-aligned for cap >= 4,
    // so SubsOffset needs no extra padding at cap 8.
    constexpr uint64_t kCap = 8;
    EXPECT_EQ(BroadcastChannel::SlotsOffset(), 3u * 64u);
    EXPECT_EQ(BroadcastChannel::MetasOffset(kCap), 3u * 64u + kCap * 128u);
    EXPECT_EQ(BroadcastChannel::SubsOffset(kCap),
              3u * 64u + kCap * 128u + kCap * 16u);
    EXPECT_EQ(BroadcastChannel::RequiredSize(kCap),
              3u * 64u + kCap * 128u + kCap * 16u +
                  kBroadcastMaxSubscribers * 128u);
}

// ---------------------------------------------------------------------------
// Fixture: 64-byte-aligned shared-memory stand-in
// ---------------------------------------------------------------------------

template <uint64_t kCapacity>
struct ChannelFixture {
    static constexpr uint64_t kBytes =
        BroadcastChannel::RequiredSize(kCapacity);

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

    // Raw views into the fixture storage (white-box probes).
    Control* control() { return reinterpret_cast<Control*>(storage); }
    IndexSlot* slots() {
        return reinterpret_cast<IndexSlot*>(storage +
                                            BroadcastChannel::SlotsOffset());
    }
    BroadcastSlotMeta* metas() {
        return reinterpret_cast<BroadcastSlotMeta*>(
            storage + BroadcastChannel::MetasOffset(kCapacity));
    }
    SubSlot* subs() {
        return reinterpret_cast<SubSlot*>(storage +
                                          BroadcastChannel::SubsOffset(kCapacity));
    }

    unsigned char* storage;
};

// Fills the reserved slot with deterministic content derived from `tag`.
void FillSlot(BroadcastChannel::Reservation& res, uint32_t tag) {
    res->msg_type = 0x4000 + tag;
    res->schema_version = (1u << 16) | 0u;
    res->schema_short_id = 0xABCDEF00ULL + tag;
    res->schema_layout_version = 1;
    res->timestamp_ns = 1'000'000 + tag;
    res->payload.offset = 0x6000 + 16ULL * tag;
    res->payload.generation = 1;
    res->payload.region_id = 1;
    res->payload_len = 100 + tag;
    res->flags = 0;
}

// Publishes one message with the default policy; fails the test on error.
void Publish(BroadcastChannel& ch, uint32_t tag) {
    auto res = ch.Reserve();
    ASSERT_TRUE(res.ok()) << res.status().ToString();
    FillSlot(res.value(), tag);
    ASSERT_TRUE(std::move(res.value()).Commit().ok());
}

// Registers subscriber `id`; fails the test on error.
BroadcastChannel::SubscriberHandle Register(BroadcastChannel& ch,
                                            uint32_t id) {
    auto sub = ch.RegisterSubscriber(SubscriberId{id});
    EXPECT_TRUE(sub.ok()) << sub.status().ToString();
    return sub.ValueOr(BroadcastChannel::SubscriberHandle{SubscriberId{0}, 0});
}

// ---------------------------------------------------------------------------
// Init / Attach
// ---------------------------------------------------------------------------

TEST(BroadcastInitTest, InitSucceeds) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok()) << ch.status().ToString();
    EXPECT_EQ(ch->capacity(), 8u);
    EXPECT_TRUE(ch->IsEmpty());
    EXPECT_FALSE(ch->IsFull());
    EXPECT_EQ(ch->Size(), 0u);
    // Every slot starts FREE with its logical position as sequence (INV-01).
    for (uint64_t i = 0; i < 8; ++i) {
        EXPECT_EQ(f.slots()[i].state.load(), static_cast<uint32_t>(SlotState::kFree));
        EXPECT_EQ(f.slots()[i].sequence_num.load(), i);
        EXPECT_EQ(f.metas()[i].ack_bitmap.bits.load(), 0u);
    }
    // Every subscriber slot starts FREE with generation 0.
    for (uint32_t i = 0; i < kBroadcastMaxSubscribers; ++i) {
        EXPECT_EQ(f.subs()[i].state.load(),
                  static_cast<uint32_t>(
                      BroadcastChannel::SubscriberState::kFree));
        EXPECT_EQ(f.subs()[i].generation.load(), 0u);
    }
    EXPECT_EQ(f.control()->current_membership.load(), 0u);
    EXPECT_EQ(f.control()->set_version.load(), 0u);
}

TEST(BroadcastInitTest, RejectsBadCapacity) {
    ChannelFixture<8> f;
    EXPECT_FALSE(BroadcastChannel::Init(f.storage, 0).ok());
    EXPECT_FALSE(BroadcastChannel::Init(f.storage, 1).ok());
    EXPECT_FALSE(BroadcastChannel::Init(f.storage, 3).ok());   // not power of two
    EXPECT_FALSE(BroadcastChannel::Init(f.storage, 6).ok());
    EXPECT_FALSE(BroadcastChannel::Init(nullptr, 8).ok());
    // A misaligned base must be rejected even inside a valid region.
    EXPECT_FALSE(BroadcastChannel::Init(f.storage + 8, 8).ok());
}

TEST(BroadcastInitTest, AttachValidatesMagicVersionAndCapacity) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    // Attach reads the capacity from the control block (single source of
    // truth, same style as SpscChannel::Attach).
    auto attached = BroadcastChannel::Attach(f.storage);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_EQ(attached->capacity(), 8u);

    // Uninitialized memory is rejected.
    ChannelFixture<8> g;
    EXPECT_FALSE(BroadcastChannel::Attach(g.storage).ok());

    // Layout version mismatch is rejected.
    f.control()->layout_version.store(999);
    auto bad_version = BroadcastChannel::Attach(f.storage);
    ASSERT_FALSE(bad_version.ok());
    EXPECT_EQ(bad_version.status().code(), StatusCode::kSchemaMismatch);
}

// ---------------------------------------------------------------------------
// Basic fan-out
// ---------------------------------------------------------------------------

TEST(BroadcastBasicTest, FanOutToAllSubscribers) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto a = Register(*ch, 0);
    auto b = Register(*ch, 1);
    auto c = Register(*ch, 2);

    Publish(*ch, 7);

    for (const auto* sub : {&a, &b, &c}) {
        auto borrow = ch->Poll(*sub);
        ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
        EXPECT_EQ(borrow.value()->msg_type, 0x4000u + 7u);
        EXPECT_EQ(borrow.value()->payload_len, 100u + 7u);
        EXPECT_EQ(borrow.value()->sequence_num, 0u);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
        auto empty = ch->Poll(*sub);
        ASSERT_FALSE(empty.ok());
        EXPECT_EQ(empty.status().code(), StatusCode::kWouldBlock);
    }
    // All three acked: the slot retires.
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kRetired));
}

TEST(BroadcastBasicTest, RegistrationJoinCutPointSkipsHistory) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    Publish(*ch, 0);  // published with an empty membership
    Publish(*ch, 1);

    // The new subscriber joins at the current head: it must NOT see the two
    // historical messages.
    auto late = Register(*ch, 5);
    auto blocked = ch->Poll(late);
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kWouldBlock);

    Publish(*ch, 2);
    auto borrow = ch->Poll(late);
    ASSERT_TRUE(borrow.ok());
    EXPECT_EQ(borrow.value()->sequence_num, 2u);
    EXPECT_EQ(borrow.value()->msg_type, 0x4000u + 2u);
    ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
}

TEST(BroadcastBasicTest, PollRejectsUnregisteredAndStaleGeneration) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    // Never registered.
    auto bad = ch->Poll(BroadcastChannel::SubscriberHandle{SubscriberId{3}, 1});
    ASSERT_FALSE(bad.ok());
    EXPECT_EQ(bad.status().code(), StatusCode::kNotFound);

    // Wrong generation.
    auto sub = Register(*ch, 3);
    auto stale_gen = BroadcastChannel::SubscriberHandle{sub.id,
                                                        sub.generation + 1};
    auto bad2 = ch->Poll(stale_gen);
    ASSERT_FALSE(bad2.ok());
    EXPECT_EQ(bad2.status().code(), StatusCode::kNotFound);

    // Standalone Ack follows the same validation.
    EXPECT_EQ(ch->Ack(stale_gen, 0).code(), StatusCode::kNotFound);
}

TEST(BroadcastBasicTest, UnregisterThenPollFails) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);
    Publish(*ch, 1);

    ASSERT_TRUE(ch->UnregisterSubscriber(sub.id, sub.generation).ok());
    auto poll = ch->Poll(sub);
    ASSERT_FALSE(poll.ok());
    EXPECT_EQ(poll.status().code(), StatusCode::kNotFound);
    // Standalone Ack of the departed subscriber is rejected too.
    EXPECT_EQ(ch->Ack(sub, 0).code(), StatusCode::kNotFound);
    // Unregistering twice fails.
    EXPECT_EQ(ch->UnregisterSubscriber(sub.id, sub.generation).code(),
              StatusCode::kNotFound);
}

// ---------------------------------------------------------------------------
// ACK bitmap / garbage collection
// ---------------------------------------------------------------------------

TEST(BroadcastAckBitmapTest, BitsClearedPerSubscriberAndSlotRetires) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto a = Register(*ch, 0);
    auto b = Register(*ch, 1);

    Publish(*ch, 3);
    // Commit stamped exactly the two active subscriber bits.
    EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0b11u);
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kReady));

    {  // Subscriber 0 acks: its bit clears, the slot stays READY.
        auto borrow = ch->Poll(a);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
        EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0b10u);
        EXPECT_EQ(f.slots()[0].state.load(),
                  static_cast<uint32_t>(SlotState::kReady));
    }
    {  // Subscriber 1 acks: the bitmap drains and the slot retires.
        auto borrow = ch->Poll(b);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
        EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0u);
        EXPECT_EQ(f.slots()[0].state.load(),
                  static_cast<uint32_t>(SlotState::kRetired));
    }
    // Garbage collection is idempotent.
    ch->CollectGarbage();
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kRetired));
}

TEST(BroadcastAckBitmapTest, PublicAckApiClearsBitAndAdvancesCursor) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 4);
    Publish(*ch, 9);

    ASSERT_TRUE(ch->Ack(sub, 0).ok());
    EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0u);
    EXPECT_EQ(f.subs()[4].cursor.load(), 1u);
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kRetired));

    // An Ack behind the cursor reports kNotFound (and stays harmless).
    EXPECT_EQ(ch->Ack(sub, 0).code(), StatusCode::kNotFound);
    EXPECT_EQ(f.subs()[4].cursor.load(), 1u);
}

// ---------------------------------------------------------------------------
// Subscriber-set snapshot binding (seqlock-style, design doc 9.6)
// ---------------------------------------------------------------------------

TEST(BroadcastSnapshotBindingTest, SlotBindsVersionAndMembershipAtCommit) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    Publish(*ch, 0);  // empty membership: version 0, bitmap 0.
    EXPECT_EQ(f.metas()[0].subscriber_set_version, 0u);
    EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0u);

    auto a = Register(*ch, 0);  // set_version -> 1
    Publish(*ch, 1);
    EXPECT_EQ(f.metas()[1].subscriber_set_version, 1u);
    EXPECT_EQ(f.metas()[1].ack_bitmap.bits.load(), 0b01u);

    auto b = Register(*ch, 1);  // set_version -> 2
    Publish(*ch, 2);
    EXPECT_EQ(f.metas()[2].subscriber_set_version, 2u);
    EXPECT_EQ(f.metas()[2].ack_bitmap.bits.load(), 0b11u);

    ASSERT_TRUE(ch->UnregisterSubscriber(a.id, a.generation).ok());  // -> 3
    Publish(*ch, 3);
    EXPECT_EQ(f.metas()[3].subscriber_set_version, 3u);
    EXPECT_EQ(f.metas()[3].ack_bitmap.bits.load(), 0b10u);
    (void)b;
}

// ---------------------------------------------------------------------------
// Unregister semantics
// ---------------------------------------------------------------------------

TEST(BroadcastUnregisterTest, DepartedSubscriberBitsBlockRetirement) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto a = Register(*ch, 0);
    auto b = Register(*ch, 1);

    Publish(*ch, 5);  // bitmap 0b11
    {  // Subscriber 0 acks, then unregisters without draining the window.
        auto borrow = ch->Poll(a);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    ASSERT_TRUE(ch->UnregisterSubscriber(a.id, a.generation).ok());
    // Membership bit cleared; the departed subscriber's set_version stays as
    // a historical record.
    EXPECT_EQ(f.control()->current_membership.load(), 0b10u);

    {  // Subscriber 1 acks: the bitmap drains to 0 and the slot retires.
        auto borrow = ch->Poll(b);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
        EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0u);
        EXPECT_EQ(f.slots()[0].state.load(),
                  static_cast<uint32_t>(SlotState::kRetired));
    }
}

TEST(BroadcastUnregisterTest, StaleAcksPrimitiveReclaimsDepartedBits) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto a = Register(*ch, 0);
    auto b = Register(*ch, 1);

    Publish(*ch, 5);  // bitmap 0b11 on slot 0
    // Subscriber 1 unregisters WITHOUT acking: its bit stays set, and the
    // frozen cursor (0) holds the GC window open from below.
    ASSERT_TRUE(ch->UnregisterSubscriber(b.id, b.generation).ok());
    {
        auto borrow = ch->Poll(a);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    // Bit 0 cleared, bit 1 still set: the slot cannot retire yet.
    EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0b10u);
    ch->CollectGarbage();
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kReady));

    // The D2-06 primitive clears the departed subscriber's leftover bit.
    EXPECT_EQ(ch->ClearStaleAcks(b.id), 1u);
    EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0u);
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kRetired));
    // Idempotent: nothing left to clear.
    EXPECT_EQ(ch->ClearStaleAcks(b.id), 0u);
    // Out-of-range id is a no-op.
    EXPECT_EQ(ch->ClearStaleAcks(SubscriberId{64}), 0u);
    (void)a;
}

TEST(BroadcastUnregisterTest, GenerationGuardsStaleHandle) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto first = Register(*ch, 2);
    ASSERT_TRUE(ch->UnregisterSubscriber(first.id, first.generation).ok());
    auto second = Register(*ch, 2);
    EXPECT_NE(second.generation, first.generation);

    // The stale handle cannot unregister (or drive) the re-registered
    // subscriber.
    EXPECT_EQ(ch->UnregisterSubscriber(first.id, first.generation).code(),
              StatusCode::kNotFound);
    auto poll = ch->Poll(first);
    ASSERT_FALSE(poll.ok());
    EXPECT_EQ(poll.status().code(), StatusCode::kNotFound);

    // The fresh handle works.
    ASSERT_TRUE(ch->UnregisterSubscriber(second.id, second.generation).ok());
}

TEST(BroadcastUnregisterTest, ExhaustionAndDoubleRegisterRejected) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());

    auto over = ch->RegisterSubscriber(SubscriberId{kBroadcastMaxSubscribers});
    ASSERT_FALSE(over.ok());
    EXPECT_EQ(over.status().code(), StatusCode::kResourceExhausted);
    auto way_over = ch->RegisterSubscriber(SubscriberId{1000});
    ASSERT_FALSE(way_over.ok());
    EXPECT_EQ(way_over.status().code(), StatusCode::kResourceExhausted);

    auto sub = Register(*ch, 7);
    auto dup = ch->RegisterSubscriber(SubscriberId{7});
    ASSERT_FALSE(dup.ok());
    EXPECT_EQ(dup.status().code(), StatusCode::kAlreadyExists);

    // All 64 slots are individually registerable.
    for (uint32_t id = 0; id < kBroadcastMaxSubscribers; ++id) {
        if (id == 7) {
            continue;
        }
        auto r = ch->RegisterSubscriber(SubscriberId{id});
        ASSERT_TRUE(r.ok()) << id;
    }
    // Every bit set.
    EXPECT_EQ(f.control()->current_membership.load(), ~uint64_t{0});
    (void)sub;
}

// ---------------------------------------------------------------------------
// Full detection (publisher_cursor - MinActiveCursor >= capacity)
// ---------------------------------------------------------------------------

TEST(BroadcastFullTest, SlowestSubscriberDefinesFullness) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    auto a = Register(*ch, 0);
    auto b = Register(*ch, 1);

    for (uint32_t i = 0; i < 4; ++i) {
        Publish(*ch, i);
    }
    EXPECT_TRUE(ch->IsFull());
    auto full = ch->Reserve(QueueFullPolicy::kFail);
    ASSERT_FALSE(full.ok());
    EXPECT_EQ(full.status().code(), StatusCode::kResourceExhausted);

    // The FAST subscriber alone cannot relieve backpressure.
    {
        auto borrow = ch->Poll(a);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    EXPECT_TRUE(ch->IsFull());

    // Advancing the SLOWEST subscriber frees exactly one slot.
    {
        auto borrow = ch->Poll(b);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    EXPECT_FALSE(ch->IsFull());
    auto res = ch->Reserve(QueueFullPolicy::kFail);
    ASSERT_TRUE(res.ok());
    ASSERT_TRUE(std::move(res.value()).Abort().ok());
}

TEST(BroadcastFullTest, TryReserveReportsWouldBlockWhenFull) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);
    for (uint32_t i = 0; i < 4; ++i) {
        Publish(*ch, i);
    }
    auto full = ch->TryReserve();
    ASSERT_FALSE(full.ok());
    EXPECT_EQ(full.status().code(), StatusCode::kWouldBlock);
    (void)sub;
}

// ---------------------------------------------------------------------------
// QueueFullPolicy: five states (design doc 9.8)
// ---------------------------------------------------------------------------

TEST(BroadcastPolicyTest, DropNewestReportsDegraded) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);
    for (uint32_t i = 0; i < 4; ++i) {
        Publish(*ch, i);
    }
    auto dropped = ch->Reserve(QueueFullPolicy::kDropNewest);
    ASSERT_FALSE(dropped.ok());
    EXPECT_EQ(dropped.status().code(), StatusCode::kDegraded);
    // The ring still holds exactly messages 0..3 for the subscriber.
    for (uint32_t i = 0; i < 4; ++i) {
        auto borrow = ch->Poll(sub);
        ASSERT_TRUE(borrow.ok()) << i;
        EXPECT_EQ(borrow.value()->sequence_num, i);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
}

TEST(BroadcastPolicyTest, DropOldestAdvancesSlowestAndLateAckNotFound) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    auto a = Register(*ch, 0);  // will be the slowest: holds an unacked borrow
    auto b = Register(*ch, 1);  // fast

    for (uint32_t i = 0; i < 4; ++i) {
        Publish(*ch, i);
    }
    EXPECT_TRUE(ch->IsFull());

    // The fast subscriber acks message 0, leaving subscriber 0 as the unique
    // slowest (cursor 0) while it holds an unacked borrow of message 0.
    {
        auto borrow = ch->Poll(b);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    auto held = ch->Poll(a);
    ASSERT_TRUE(held.ok());
    EXPECT_EQ(held.value()->sequence_num, 0u);

    // kDropOldest: subscriber 0 (cursor 0) is forced to the head (4) and its
    // bit is cleared on every jumped slot; the publish succeeds.
    {
        auto res = ch->Reserve(QueueFullPolicy::kDropOldest);
        ASSERT_TRUE(res.ok()) << res.status().ToString();
        FillSlot(res.value(), 4);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    EXPECT_EQ(f.subs()[0].cursor.load(), 4u);
    // The drop cleared subscriber 0's bit on the three slots it jumped over
    // (seq 1..3 in physical slots 1..3; bit 1 still outstanding). Slot 0
    // itself was immediately recycled for message 4 and carries a fresh
    // 0b11 bitmap stamped by Commit.
    EXPECT_EQ(f.metas()[1].ack_bitmap.bits.load(), 0b10u);
    EXPECT_EQ(f.metas()[2].ack_bitmap.bits.load(), 0b10u);
    EXPECT_EQ(f.metas()[3].ack_bitmap.bits.load(), 0b10u);
    EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0b11u);

    // The held borrow still sees ITS message (snapshot semantics); its late
    // Ack reports kNotFound, moves no cursor, and harmlessly clears the bit
    // on the recycled slot (design doc 9.6: 被越过后仍尝试 Clear).
    EXPECT_EQ(held.value()->msg_type, 0x4000u + 0u);
    auto late = std::move(held.value()).Ack();
    EXPECT_EQ(late.code(), StatusCode::kNotFound);
    EXPECT_EQ(f.subs()[0].cursor.load(), 4u);
    EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0b10u);

    // Subscriber 0 resumes at the head: it sees message 4 next.
    auto borrow = ch->Poll(a);
    ASSERT_TRUE(borrow.ok());
    EXPECT_EQ(borrow.value()->sequence_num, 4u);
    ASSERT_TRUE(std::move(borrow.value()).Ack().ok());

    // The fast subscriber continues in order from its own cursor.
    auto b1 = ch->Poll(b);
    ASSERT_TRUE(b1.ok());
    EXPECT_EQ(b1.value()->sequence_num, 1u);
    ASSERT_TRUE(std::move(b1.value()).Ack().ok());
}

TEST(BroadcastPolicyTest, DropOldestNoActiveSubscriberIsNeverFull) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    // No subscribers: every policy succeeds immediately, forever.
    for (uint32_t i = 0; i < 16; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kDropOldest);
        ASSERT_TRUE(res.ok()) << i;
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    EXPECT_EQ(ch->Size(), 16u);
}

TEST(BroadcastPolicyTest, SampleAdmitsDeterministically) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);

    // Fill exactly capacity; the next publisher position is 4.
    for (uint32_t i = 0; i < 4; ++i) {
        Publish(*ch, i);
    }

    // prod = 4: rate 2 admits (4 % 2 == 0) but there is no space yet — the
    // admitted publish spins until the subscriber frees a slot.
    std::atomic<bool> admitted_done{false};
    std::thread publisher([&]() {
        auto res = ch->Reserve(QueueFullPolicy::kSample, /*sample_rate=*/2);
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 4);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
        admitted_done.store(true, std::memory_order_release);
    });
    // Give the publisher a moment to prove it is spinning, then free a slot.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(admitted_done.load(std::memory_order_acquire));
    {
        auto borrow = ch->Poll(sub);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    publisher.join();
    EXPECT_TRUE(admitted_done.load(std::memory_order_acquire));

    // Drain the subscriber so the ring is empty again (cursor catches up to
    // prod == 5).
    for (uint32_t i = 1; i <= 4; ++i) {
        auto borrow = ch->Poll(sub);
        ASSERT_TRUE(borrow.ok()) << i;
        EXPECT_EQ(borrow.value()->sequence_num, i);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }

    // Fill the ring again (messages 5..8); the next publisher position is 9
    // and 9 % 2 != 0, so the message is sampled out (kDegraded).
    for (uint32_t i = 0; i < 4; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kFail);
        ASSERT_TRUE(res.ok()) << i;
        FillSlot(res.value(), 10 + i);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    EXPECT_TRUE(ch->IsFull());
    auto sampled_out = ch->Reserve(QueueFullPolicy::kSample, 2);
    ASSERT_FALSE(sampled_out.ok());
    EXPECT_EQ(sampled_out.status().code(), StatusCode::kDegraded);
}

TEST(BroadcastPolicyTest, BlockWaitsForSlowestSubscriber) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);
    for (uint32_t i = 0; i < 4; ++i) {
        Publish(*ch, i);
    }

    std::atomic<bool> published{false};
    std::thread publisher([&]() {
        auto res = ch->Reserve(QueueFullPolicy::kBlock);
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 4);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
        published.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(published.load(std::memory_order_acquire));
    {
        auto borrow = ch->Poll(sub);
        ASSERT_TRUE(borrow.ok());
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    publisher.join();
    EXPECT_TRUE(published.load(std::memory_order_acquire));
    EXPECT_EQ(ch->Size(), 5u);
}

// ---------------------------------------------------------------------------
// Abort tombstones (design doc 9.6 / 12.3)
// ---------------------------------------------------------------------------

TEST(BroadcastAbortTest, ExplicitAbortIsSkippedTransparently) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);

    {  // Aborted reservation leaves a tombstone with a cleared bitmap.
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 0xAA);
        ASSERT_TRUE(std::move(res.value()).Abort().ok());
    }
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kAborted));
    EXPECT_EQ(f.metas()[0].ack_bitmap.bits.load(), 0u);

    Publish(*ch, 1);  // seq 1

    // Poll skips the tombstone and delivers message 1 directly.
    auto borrow = ch->Poll(sub);
    ASSERT_TRUE(borrow.ok());
    EXPECT_EQ(borrow.value()->sequence_num, 1u);
    ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    // The tombstone advanced the subscriber cursor without touching bitmaps.
    EXPECT_EQ(f.subs()[0].cursor.load(), 2u);
    auto empty = ch->Poll(sub);
    ASSERT_FALSE(empty.ok());
    EXPECT_EQ(empty.status().code(), StatusCode::kWouldBlock);
}

TEST(BroadcastAbortTest, DestroyedReservationAbortsItself) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);
    {
        auto res = ch->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 0xBB);
        // No Commit/Abort: the destructor must stamp the tombstone.
    }
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kAborted));
    EXPECT_EQ(ch->Size(), 1u);

    Publish(*ch, 2);
    auto borrow = ch->Poll(sub);
    ASSERT_TRUE(borrow.ok());
    EXPECT_EQ(borrow.value()->sequence_num, 1u);
    ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
}

// ---------------------------------------------------------------------------
// Corruption handling
// ---------------------------------------------------------------------------

TEST(BroadcastCorruptionTest, CrcMismatchIsSkippedPerSubscriber) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto a = Register(*ch, 0);
    auto b = Register(*ch, 1);

    Publish(*ch, 0);
    Publish(*ch, 1);

    // Corrupt slot 0's immutable field after publication.
    f.slots()[0].payload_len ^= 0xFF;

    // Subscriber 0 hits the corruption: reported, and the cursor advances
    // past the bad slot so the next poll makes progress.
    auto bad = ch->Poll(a);
    ASSERT_FALSE(bad.ok());
    EXPECT_EQ(bad.status().code(), StatusCode::kCorruption);
    auto good = ch->Poll(a);
    ASSERT_TRUE(good.ok());
    EXPECT_EQ(good.value()->sequence_num, 1u);
    ASSERT_TRUE(std::move(good.value()).Ack().ok());

    // Subscriber 1 walks the same path independently.
    auto bad_b = ch->Poll(b);
    ASSERT_FALSE(bad_b.ok());
    EXPECT_EQ(bad_b.status().code(), StatusCode::kCorruption);
    auto good_b = ch->Poll(b);
    ASSERT_TRUE(good_b.ok());
    EXPECT_EQ(good_b.value()->sequence_num, 1u);
    ASSERT_TRUE(std::move(good_b.value()).Ack().ok());
}

TEST(BroadcastCorruptionTest, SequenceMismatchIsSkippedNotLivelocked) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);

    Publish(*ch, 0);
    Publish(*ch, 1);

    // Corrupt slot 0's sequence AND re-seal the CRC so the sequence check is
    // what fires (not the CRC check).
    f.slots()[0].sequence_num = 999;
    SealIndexSlotImmutableCrc(f.slots()[0]);

    auto bad = ch->Poll(sub);
    ASSERT_FALSE(bad.ok());
    EXPECT_EQ(bad.status().code(), StatusCode::kCorruption);

    auto good = ch->Poll(sub);
    ASSERT_TRUE(good.ok());
    EXPECT_EQ(good.value()->sequence_num, 1u);
    ASSERT_TRUE(std::move(good.value()).Ack().ok());
    auto empty = ch->Poll(sub);
    ASSERT_FALSE(empty.ok());
    EXPECT_EQ(empty.status().code(), StatusCode::kWouldBlock);
}

// ---------------------------------------------------------------------------
// Wraparound
// ---------------------------------------------------------------------------

TEST(BroadcastWrapTest, LongWrapNoLossNoDuplicationPerSubscriber) {
    ChannelFixture<8> f;
    auto ch = BroadcastChannel::Init(f.storage, 8);
    ASSERT_TRUE(ch.ok());
    auto a = Register(*ch, 0);
    auto b = Register(*ch, 1);

    constexpr uint32_t kTotal = 1000;  // 125 full wraps of the 8-slot ring.
    for (uint32_t i = 0; i < kTotal; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kBlock);
        ASSERT_TRUE(res.ok()) << i;
        FillSlot(res.value(), i & 0xFF);
        res.value()->payload_len = i;
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
        // Drain both subscribers each iteration to keep the ring from
        // filling (and to verify per-iteration delivery).
        for (const auto* sub : {&a, &b}) {
            auto borrow = ch->Poll(*sub);
            ASSERT_TRUE(borrow.ok()) << i;
            EXPECT_EQ(borrow.value()->sequence_num, i);
            EXPECT_EQ(borrow.value()->payload_len, i);
            ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
        }
    }
    EXPECT_EQ(ch->Size(), kTotal);
    for (uint64_t i = 0; i < 8; ++i) {
        EXPECT_EQ(f.slots()[i].state.load(),
                  static_cast<uint32_t>(SlotState::kRetired)) << i;
    }
}

TEST(BroadcastWrapTest, StaleSlotSequenceNeverConfusesSubscribers) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    auto sub = Register(*ch, 0);

    // Two full wraps with the subscriber idle: physical slot 0 carries
    // sequences 0, 4, 8 across eras.
    for (uint32_t round = 0; round < 2; ++round) {
        for (uint32_t i = 0; i < 4; ++i) {
            Publish(*ch, round * 4 + i);
        }
        for (uint32_t i = 0; i < 4; ++i) {
            auto borrow = ch->Poll(sub);
            ASSERT_TRUE(borrow.ok());
            EXPECT_EQ(borrow.value()->sequence_num, round * 4 + i);
            ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
        }
    }
    // Every physical slot is retired; a third wrap starts clean.
    for (uint32_t i = 0; i < 4; ++i) {
        EXPECT_EQ(f.slots()[i].state.load(),
                  static_cast<uint32_t>(SlotState::kRetired));
    }
}

// ---------------------------------------------------------------------------
// Empty membership (design doc 9.6: never full with zero subscribers)
// ---------------------------------------------------------------------------

TEST(BroadcastEmptyMembershipTest, PublishingNeedsNoAcks) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());

    for (uint32_t i = 0; i < 32; ++i) {  // 8 wraps, no subscriber at all.
        Publish(*ch, i);
        EXPECT_FALSE(ch->IsFull());
    }
    EXPECT_EQ(ch->Size(), 32u);
    // Every bitmap stayed empty; every slot recycled freely.
    for (uint64_t i = 0; i < 4; ++i) {
        EXPECT_EQ(f.metas()[i].ack_bitmap.bits.load(), 0u);
    }
}

TEST(BroadcastEmptyMembershipTest, EmptyMembershipSlotsRetireViaGc) {
    ChannelFixture<4> f;
    auto ch = BroadcastChannel::Init(f.storage, 4);
    ASSERT_TRUE(ch.ok());
    Publish(*ch, 0);
    // The slot was published with bitmap 0: it is trivially fully-acked.
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kReady));
    ch->CollectGarbage();
    EXPECT_EQ(f.slots()[0].state.load(),
              static_cast<uint32_t>(SlotState::kRetired));
}

// ---------------------------------------------------------------------------
// Concurrent publisher vs. subscribers (in-process threads)
// ---------------------------------------------------------------------------

TEST(BroadcastThreadTest, ConcurrentPublisherAndSubscribers) {
    ChannelFixture<64> f;
    auto ch = BroadcastChannel::Init(f.storage, 64);
    ASSERT_TRUE(ch.ok());

    constexpr uint32_t kSubscribers = 3;
    constexpr uint64_t kCount = 20000;  // 312 wraps of the 64-slot ring.
    std::vector<BroadcastChannel::SubscriberHandle> subs;
    for (uint32_t i = 0; i < kSubscribers; ++i) {
        subs.push_back(Register(*ch, i));
    }

    std::atomic<bool> start{false};
    std::atomic<uint64_t> done{0};

    std::thread publisher([&]() {
        BroadcastChannel pub = *ch;
        while (!start.load(std::memory_order_acquire)) {
        }
        for (uint64_t i = 0; i < kCount; ++i) {
            auto res = pub.Reserve(QueueFullPolicy::kBlock);
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

    std::vector<std::thread> consumers;
    for (uint32_t s = 0; s < kSubscribers; ++s) {
        consumers.emplace_back([&, s]() {
            BroadcastChannel con = *ch;
            const auto handle = subs[s];
            while (!start.load(std::memory_order_acquire)) {
            }
            for (uint64_t i = 0; i < kCount; ++i) {
                auto borrow = con.Poll(handle);
                while (!borrow.ok()) {
                    if (borrow.status().code() != StatusCode::kWouldBlock) {
                        ADD_FAILURE() << "subscriber " << s << " poll failed at "
                                      << i << " : "
                                      << borrow.status().ToString();
                        return;
                    }
                    borrow = con.Poll(handle);
                }
                EXPECT_EQ(borrow.value()->sequence_num, i)
                    << "subscriber " << s;
                EXPECT_EQ(borrow.value()->payload_len, static_cast<uint32_t>(i))
                    << "subscriber " << s;
                if (!std::move(borrow.value()).Ack().ok()) {
                    ADD_FAILURE() << "subscriber " << s << " ack failed at " << i;
                    return;
                }
            }
            done.fetch_add(1, std::memory_order_acq_rel);
        });
    }

    start.store(true, std::memory_order_release);
    publisher.join();
    for (auto& t : consumers) {
        t.join();
    }
    EXPECT_EQ(done.load(), kSubscribers);
    EXPECT_EQ(ch->Size(), kCount);
    // Every slot fully acked and retired.
    for (uint64_t i = 0; i < 64; ++i) {
        EXPECT_EQ(f.slots()[i].state.load(),
                  static_cast<uint32_t>(SlotState::kRetired)) << i;
        EXPECT_EQ(f.metas()[i].ack_bitmap.bits.load(), 0u) << i;
    }
}

TEST(BroadcastThreadTest, ConcurrentRegisterUnregisterDuringPublish) {
    ChannelFixture<16> f;
    auto ch = BroadcastChannel::Init(f.storage, 16);
    ASSERT_TRUE(ch.ok());

    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> published{0};

    // Publisher: commits as fast as the churn allows (never blocks forever
    // because a just-unregistered subscriber no longer counts as active).
    std::thread publisher([&]() {
        BroadcastChannel pub = *ch;
        while (!start.load(std::memory_order_acquire)) {
        }
        uint64_t i = 0;
        while (!stop.load(std::memory_order_acquire)) {
            auto res = pub.Reserve(QueueFullPolicy::kFail);
            if (res.ok()) {
                FillSlot(res.value(), static_cast<uint32_t>(i & 0xFF));
                if (!std::move(res.value()).Commit().ok()) {
                    ADD_FAILURE() << "commit failed";
                    return;
                }
                ++i;
            }
        }
        published.store(i, std::memory_order_release);
    });

    // Churn: register/poll/ack/unregister in a loop on disjoint ids. The
    // seqlock snapshot in Commit must never tear the membership.
    std::thread churn([&]() {
        BroadcastChannel c = *ch;
        while (!start.load(std::memory_order_acquire)) {
        }
        uint32_t round = 0;
        while (!stop.load(std::memory_order_acquire)) {
            const uint32_t id = 10 + (round % 4);
            auto sub = c.RegisterSubscriber(SubscriberId{id});
            if (sub.ok()) {
                // Drain whatever is visible, ack it, leave.
                for (int k = 0; k < 4; ++k) {
                    auto borrow = c.Poll(*sub);
                    if (!borrow.ok()) {
                        break;
                    }
                    if (!std::move(borrow.value()).Ack().ok()) {
                        break;
                    }
                }
                ASSERT_TRUE(
                    c.UnregisterSubscriber(sub->id, sub->generation).ok());
            }
            ++round;
        }
    });

    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop.store(true, std::memory_order_release);
    publisher.join();
    churn.join();

    EXPECT_GT(published.load(std::memory_order_acquire), 0u);
    // Every membership bit cleared by the churn; version kept counting.
    EXPECT_EQ(f.control()->current_membership.load(), 0u);
}

}  // namespace
}  // namespace mino
