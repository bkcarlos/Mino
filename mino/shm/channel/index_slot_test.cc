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

#include "mino/shm/channel/index_slot.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace mino {
namespace {

// ---------------------------------------------------------------------------
// Compile-time ABI contract (ADR-0003)
// ---------------------------------------------------------------------------

static_assert(sizeof(IndexSlot) == 128,
              "IndexSlot must be exactly 128 bytes (two cache lines)");
static_assert(alignof(IndexSlot) == 64, "IndexSlot must be 64-byte aligned");
static_assert(std::is_standard_layout_v<IndexSlot>);
// Copyability of IndexSlot is implementation-defined (it holds an atomic
// state member). Channels never copy a live slot either way: they access
// fields in place, because a byte-copy of `state` would be meaningless.

static_assert(offsetof(IndexSlot, msg_type) == 0);
static_assert(offsetof(IndexSlot, schema_version) == 4);
static_assert(offsetof(IndexSlot, schema_short_id) == 8);
static_assert(offsetof(IndexSlot, schema_layout_version) == 16);
static_assert(offsetof(IndexSlot, sequence_num) == 24);
static_assert(offsetof(IndexSlot, timestamp_ns) == 32);
static_assert(offsetof(IndexSlot, payload) == 40);
static_assert(offsetof(IndexSlot, payload_len) == 56);
static_assert(offsetof(IndexSlot, immutable_metadata_crc) == 60);
static_assert(offsetof(IndexSlot, state) == 64);
static_assert(offsetof(IndexSlot, flags) == 68);

static_assert(sizeof(BroadcastSlotMeta) == 16);
static_assert(sizeof(WorkQueueSlotMeta) == 16);
static_assert(sizeof(MpscReservationMeta) == 48);

// ---------------------------------------------------------------------------
// Immutable CRC: seal / verify round-trip
// ---------------------------------------------------------------------------

// Fills `slot` in place with deterministic content. IndexSlot is
// non-copyable and non-movable (atomic member), so tests fill in place.
void FillSlot(IndexSlot& slot) {
    slot.msg_type = 0xDEADBEEF;
    slot.schema_version = (2u << 16) | 3u;  // major 2, minor 3
    slot.schema_short_id = 0x0123456789ABCDEFULL;
    slot.schema_layout_version = 7;
    slot.reserved0 = 0;
    slot.sequence_num.store(42, std::memory_order_relaxed);
    slot.timestamp_ns = 1'700'000'000'000'000'000ULL;
    slot.payload.offset = 0x1000;
    slot.payload.generation = 9;
    slot.payload.region_id = 3;
    slot.payload_len = 4096;
    slot.state.store(static_cast<uint32_t>(SlotState::kWriting),
                     std::memory_order_relaxed);
    slot.flags = kIndexSlotFlagHasChildSlabs;
}

TEST(IndexSlotCrcTest, SealThenVerifyRoundTrips) {
    IndexSlot slot{};
    FillSlot(slot);
    SealIndexSlotImmutableCrc(slot);
    EXPECT_NE(slot.immutable_metadata_crc, 0u);
    EXPECT_TRUE(VerifyIndexSlotImmutableCrc(slot));
}

TEST(IndexSlotCrcTest, DeterministicAcrossSlots) {
    IndexSlot a{};
    IndexSlot b{};
    FillSlot(a);
    FillSlot(b);
    EXPECT_EQ(ComputeIndexSlotImmutableCrc(a), ComputeIndexSlotImmutableCrc(b));
}

TEST(IndexSlotCrcTest, ImmutableFieldChangeBreaksCrc) {
    IndexSlot slot{};
    FillSlot(slot);
    SealIndexSlotImmutableCrc(slot);
    ASSERT_TRUE(VerifyIndexSlotImmutableCrc(slot));

    // Flip one immutable field; verification must fail.
    slot.payload_len = 8192;
    EXPECT_FALSE(VerifyIndexSlotImmutableCrc(slot));
}

TEST(IndexSlotCrcTest, MutableFieldChangeKeepsCrc) {
    IndexSlot slot{};
    FillSlot(slot);
    SealIndexSlotImmutableCrc(slot);
    ASSERT_TRUE(VerifyIndexSlotImmutableCrc(slot));

    // state and flags are NOT covered by the CRC (design doc 9.2).
    slot.state.store(static_cast<uint32_t>(SlotState::kReady),
                     std::memory_order_relaxed);
    slot.flags = kIndexSlotFlagLargeObject;
    EXPECT_TRUE(VerifyIndexSlotImmutableCrc(slot));
}

TEST(IndexSlotCrcTest, EveryImmutableFieldIsCovered) {
    // Each immutable field, when perturbed, must change the CRC. This pins
    // the documented coverage set.
    IndexSlot base{};
    FillSlot(base);
    const uint32_t base_crc = ComputeIndexSlotImmutableCrc(base);

    auto expect_changes = [base_crc](auto mutate) {
        IndexSlot s{};
        FillSlot(s);
        mutate(s);
        EXPECT_NE(ComputeIndexSlotImmutableCrc(s), base_crc);
    };

    expect_changes([](IndexSlot& s) { s.msg_type ^= 1; });
    expect_changes([](IndexSlot& s) { s.schema_version ^= 1; });
    expect_changes([](IndexSlot& s) { s.schema_short_id ^= 1; });
    expect_changes([](IndexSlot& s) { s.schema_layout_version ^= 1; });
    expect_changes([](IndexSlot& s) { s.reserved0 ^= 1; });
    expect_changes([](IndexSlot& s) {
        s.sequence_num.store(s.sequence_num.load(std::memory_order_relaxed) ^
                                 1,
                             std::memory_order_relaxed);
    });
    expect_changes([](IndexSlot& s) { s.timestamp_ns ^= 1; });
    expect_changes([](IndexSlot& s) { s.payload.offset ^= 1; });
    expect_changes([](IndexSlot& s) { s.payload.generation ^= 1; });
    expect_changes([](IndexSlot& s) { s.payload.region_id ^= 1; });
    expect_changes([](IndexSlot& s) { s.payload_len ^= 1; });
}

TEST(IndexSlotCrcTest, KnownVector) {
    // Pin the CRC against regression of the field order / polynomial. If
    // this value changes, the wire meaning of existing SHM segments changes.
    IndexSlot slot{};
    slot.msg_type = 1;
    slot.schema_version = 0x00010002u;  // 1.2
    slot.schema_short_id = 0xA5A5A5A5A5A5A5A5ULL;
    slot.schema_layout_version = 1;
    slot.sequence_num.store(7, std::memory_order_relaxed);
    slot.timestamp_ns = 123456789;
    slot.payload.offset = 0x800;
    slot.payload.generation = 1;
    slot.payload.region_id = 1;
    slot.payload_len = 64;
    const uint32_t crc = ComputeIndexSlotImmutableCrc(slot);
    // Golden vector for the CRC32C over the field sequence above. Computed
    // once with the reference implementation; any future change to the
    // field set, order, or polynomial must update this constant
    // deliberately (it is an ABI-affecting change).
    EXPECT_EQ(crc, 0xF280AD78u);
}

// ---------------------------------------------------------------------------
// AckBitmap
// ---------------------------------------------------------------------------

TEST(AckBitmapTest, SetClearLifecycle) {
    AckBitmap bmp;
    EXPECT_TRUE(bmp.AllAcked());

    bmp.Set(0);
    bmp.Set(5);
    bmp.Set(63);
    EXPECT_FALSE(bmp.AllAcked());
    EXPECT_TRUE(bmp.IsSet(0));
    EXPECT_TRUE(bmp.IsSet(5));
    EXPECT_TRUE(bmp.IsSet(63));
    EXPECT_FALSE(bmp.IsSet(1));

    EXPECT_TRUE(bmp.Clear(5));   // previously set -> cleared
    EXPECT_FALSE(bmp.Clear(5));  // already clear -> not owned
    EXPECT_TRUE(bmp.Clear(0));
    EXPECT_FALSE(bmp.AllAcked());
    EXPECT_TRUE(bmp.Clear(63));
    EXPECT_TRUE(bmp.AllAcked());
}

}  // namespace
}  // namespace mino
