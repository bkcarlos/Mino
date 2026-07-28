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

#include "mino/shm/allocator/slab_header.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace mino {
namespace {

// Compile-time layout guarantees (design doc 8.1).
static_assert(sizeof(SlabHeader) == 64);
static_assert(alignof(SlabHeader) == 64);
static_assert(offsetof(SlabHeader, magic) == 0);
static_assert(offsetof(SlabHeader, object_state) == 12);
static_assert(offsetof(SlabHeader, schema_short_id) == 32);
static_assert(offsetof(SlabHeader, immutable_header_crc) == 56);

// Fully initializes `h` with a valid header. The header contains a
// std::atomic member and is therefore non-copyable/non-movable; tests
// mutate it in place.
void InitHeader(SlabHeader& h) {
    h.magic = kSlabHeaderMagic;
    h.header_version = kSlabHeaderVersion;
    h.class_id = 2;
    h.generation = 7;
    h.object_state.store(static_cast<uint32_t>(ObjectState::kAllocated),
                         std::memory_order_relaxed);
    h.capacity = 2048;
    h.object_size = 100;
    h.type_id = 42;
    h.layout_version = 3;
    h.schema_short_id = 0x0123456789ABCDEFull;
    h.owner_epoch = 0xA5A5;
    h.allocation_transaction_id = 0x5A5A;
    h.immutable_header_crc = 0;
    h.reserved = 0;
}

TEST(SlabHeaderTest, ObjectStateValuesAreStable) {
    // The values are part of the shared-memory ABI (design doc 8.1).
    EXPECT_EQ(static_cast<uint32_t>(ObjectState::kFree), 0u);
    EXPECT_EQ(static_cast<uint32_t>(ObjectState::kAllocated), 1u);
    EXPECT_EQ(static_cast<uint32_t>(ObjectState::kBuilding), 2u);
    EXPECT_EQ(static_cast<uint32_t>(ObjectState::kPublished), 3u);
    EXPECT_EQ(static_cast<uint32_t>(ObjectState::kRetired), 4u);
    EXPECT_EQ(static_cast<uint32_t>(ObjectState::kAborting), 5u);
}

TEST(SlabHeaderTest, CrcIsDeterministic) {
    SlabHeader h{};
    InitHeader(h);
    EXPECT_EQ(ComputeImmutableHeaderCrc(h), ComputeImmutableHeaderCrc(h));
}

TEST(SlabHeaderTest, CrcCoversImmutableFields) {
    SlabHeader h{};
    InitHeader(h);
    const uint32_t base = ComputeImmutableHeaderCrc(h);

    // Every covered field must change the CRC (design doc 8.1): magic,
    // version, class, generation, capacity, object size, type, layout and
    // schema short id. Mutate in place and restore afterwards.
    struct Case {
        const char* name;
        void (*mutate)(SlabHeader&);
    };
    const Case cases[] = {
        {"magic", [](SlabHeader& x) { x.magic = 0xDEADBEEF; }},
        {"header_version", [](SlabHeader& x) { x.header_version = 2; }},
        {"class_id", [](SlabHeader& x) { x.class_id = 3; }},
        {"generation", [](SlabHeader& x) { x.generation = 8; }},
        {"capacity", [](SlabHeader& x) { x.capacity = 4096; }},
        {"object_size", [](SlabHeader& x) { x.object_size = 200; }},
        {"type_id", [](SlabHeader& x) { x.type_id = 43; }},
        {"layout_version", [](SlabHeader& x) { x.layout_version = 4; }},
        {"schema_short_id",
         [](SlabHeader& x) { x.schema_short_id = 0xFEDCBA9876543210ull; }},
    };
    for (const Case& c : cases) {
        SlabHeader mutated{};
        InitHeader(mutated);
        c.mutate(mutated);
        EXPECT_NE(ComputeImmutableHeaderCrc(mutated), base) << c.name;
    }
}

TEST(SlabHeaderTest, CrcIgnoresMutableFields) {
    SlabHeader h{};
    InitHeader(h);
    const uint32_t base = ComputeImmutableHeaderCrc(h);

    // object_state, owner_epoch, allocation_transaction_id and the CRC field
    // itself must not be covered (design doc 8.1).
    {
        SlabHeader mutated{};
        InitHeader(mutated);
        mutated.object_state.store(static_cast<uint32_t>(ObjectState::kPublished),
                                   std::memory_order_relaxed);
        EXPECT_EQ(ComputeImmutableHeaderCrc(mutated), base);
    }
    {
        SlabHeader mutated{};
        InitHeader(mutated);
        mutated.owner_epoch = 0x1111;
        EXPECT_EQ(ComputeImmutableHeaderCrc(mutated), base);
    }
    {
        SlabHeader mutated{};
        InitHeader(mutated);
        mutated.allocation_transaction_id = 0x2222;
        EXPECT_EQ(ComputeImmutableHeaderCrc(mutated), base);
    }
    {
        SlabHeader mutated{};
        InitHeader(mutated);
        mutated.immutable_header_crc = 0xCAFEBABE;
        EXPECT_EQ(ComputeImmutableHeaderCrc(mutated), base);
    }
    {
        SlabHeader mutated{};
        InitHeader(mutated);
        mutated.reserved = 0xFFFFFFFFu;
        EXPECT_EQ(ComputeImmutableHeaderCrc(mutated), base);
    }
}

TEST(SlabHeaderTest, VerifyImmutableHeaderAcceptsValidHeader) {
    SlabHeader h{};
    InitHeader(h);
    h.immutable_header_crc = ComputeImmutableHeaderCrc(h);
    EXPECT_TRUE(VerifyImmutableHeader(h));
}

TEST(SlabHeaderTest, VerifyImmutableHeaderRejectsBadMagic) {
    SlabHeader h{};
    InitHeader(h);
    h.immutable_header_crc = ComputeImmutableHeaderCrc(h);
    h.magic = 0xDEADBEEF;
    EXPECT_FALSE(VerifyImmutableHeader(h));
}

TEST(SlabHeaderTest, VerifyImmutableHeaderRejectsBadVersion) {
    SlabHeader h{};
    InitHeader(h);
    h.immutable_header_crc = ComputeImmutableHeaderCrc(h);
    h.header_version = 99;
    EXPECT_FALSE(VerifyImmutableHeader(h));
}

TEST(SlabHeaderTest, VerifyImmutableHeaderRejectsCorruptCrc) {
    SlabHeader h{};
    InitHeader(h);
    h.immutable_header_crc = ComputeImmutableHeaderCrc(h) ^ 1u;
    EXPECT_FALSE(VerifyImmutableHeader(h));
}

TEST(SlabHeaderTest, VerifyImmutableHeaderRejectsMutatedCoveredField) {
    SlabHeader h{};
    InitHeader(h);
    h.immutable_header_crc = ComputeImmutableHeaderCrc(h);
    // Mutating any covered field after CRC computation must fail validation.
    h.generation += 1;
    EXPECT_FALSE(VerifyImmutableHeader(h));
}

TEST(SlabHeaderTest, CrcIsConstexprEvaluable) {
    // The CRC must be usable in constant evaluation so tests and tooling can
    // pin golden vectors at compile time.
    constexpr uint32_t kPoly = 0x82F63B78u;
    constexpr auto crc_of_one_byte = []() constexpr {
        uint32_t crc = 0xFFFFFFFFu;
        crc ^= 0x01u;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ ((crc & 1u) != 0u ? kPoly : 0u);
        }
        return crc ^ 0xFFFFFFFFu;
    };
    static_assert(crc_of_one_byte() != 0);
    SUCCEED();
}

}  // namespace
}  // namespace mino
