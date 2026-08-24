// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/crc32c.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace mino::bridge {
namespace {

uint32_t ReferenceCrc32c(std::span<const std::byte> bytes) noexcept {
    uint32_t state = 0xffffffffu;
    for (std::byte byte : bytes) {
        state ^= static_cast<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                    ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

std::vector<std::byte> Bytes(std::string_view text) {
    std::vector<std::byte> bytes;
    bytes.reserve(text.size());
    for (char value : text) {
        bytes.push_back(static_cast<std::byte>(value));
    }
    return bytes;
}

uint64_t NextRandom(uint64_t& state) noexcept {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}

TEST(Crc32cTest, MatchesStandardGoldenVectors) {
    EXPECT_EQ(Crc32c({}), 0x00000000u);
    EXPECT_EQ(Crc32c(Bytes("123456789")), 0xe3069283u);
    EXPECT_EQ(Crc32c(Bytes("abc")), 0x364b3fb7u);

    std::array<std::byte, 32> zeros{};
    EXPECT_EQ(Crc32c(zeros), 0x8a9136aau);

    std::array<std::byte, 32> ones;
    ones.fill(std::byte{0xff});
    EXPECT_EQ(Crc32c(ones), 0x62a8ab43u);

    std::array<std::byte, 32> ascending;
    std::array<std::byte, 32> descending;
    for (size_t i = 0; i < ascending.size(); ++i) {
        ascending[i] = static_cast<std::byte>(i);
        descending[i] = static_cast<std::byte>(descending.size() - 1 - i);
    }
    EXPECT_EQ(Crc32c(ascending), 0x46dd794eu);
    EXPECT_EQ(Crc32c(descending), 0x113fdb5cu);
}

TEST(Crc32cTest, HandlesEveryShortTailAtUnalignedAddresses) {
    std::array<std::byte, 96> storage;
    uint64_t random = 0x4d595df4d0f33173ull;
    for (std::byte& byte : storage) {
        byte = static_cast<std::byte>(NextRandom(random));
    }

    for (size_t offset = 0; offset < 16; ++offset) {
        for (size_t length = 0; length <= 64; ++length) {
            const std::span<const std::byte> input(storage.data() + offset,
                                                   length);
            SCOPED_TRACE(offset);
            SCOPED_TRACE(length);
            EXPECT_EQ(Crc32c(input), ReferenceCrc32c(input));
        }
    }
}

TEST(Crc32cTest, CopyAndUpdateMatchesIndependentCrcForAlignmentAndTails) {
    std::array<std::byte, 128> source_storage;
    uint64_t random = 0xd1b54a32d192ed03ull;
    for (std::byte& byte : source_storage) {
        byte = static_cast<std::byte>(NextRandom(random));
    }

    std::array<std::byte, 128> destination_storage;
    for (size_t source_offset = 0; source_offset < 16; ++source_offset) {
        for (size_t destination_offset = 0; destination_offset < 16;
             ++destination_offset) {
            for (size_t length = 0; length <= 96; ++length) {
                destination_storage.fill(std::byte{0xa5});
                const std::span<const std::byte> input(
                    source_storage.data() + source_offset, length);
                const std::span<std::byte> output(
                    destination_storage.data() + destination_offset, length);

                Crc32cAccumulator accumulator;
                accumulator.CopyAndUpdate(input, output);

                SCOPED_TRACE(source_offset);
                SCOPED_TRACE(destination_offset);
                SCOPED_TRACE(length);
                EXPECT_TRUE(std::equal(input.begin(), input.end(),
                                       output.begin(), output.end()));
                EXPECT_EQ(accumulator.Finish(), ReferenceCrc32c(input));
                if (destination_offset != 0) {
                    EXPECT_EQ(destination_storage[destination_offset - 1],
                              std::byte{0xa5});
                }
                if (destination_offset + length <
                    destination_storage.size()) {
                    EXPECT_EQ(destination_storage[destination_offset + length],
                              std::byte{0xa5});
                }
            }
        }
    }
}

TEST(Crc32cTest, CopyAndUpdateMatchesIndependentCrcAcrossRandomChunks) {
    std::vector<std::byte> source_storage(4096 + 16);
    std::vector<std::byte> destination_storage(source_storage.size(),
                                               std::byte{0xa5});
    uint64_t random = 0x94d049bb133111ebull;
    for (std::byte& byte : source_storage) {
        byte = static_cast<std::byte>(NextRandom(random));
    }

    for (size_t iteration = 0; iteration < 256; ++iteration) {
        const size_t source_offset = NextRandom(random) % 16;
        const size_t destination_offset = NextRandom(random) % 16;
        const size_t maximum_length =
            std::min(source_storage.size() - source_offset,
                     destination_storage.size() - destination_offset);
        const size_t length = NextRandom(random) % (maximum_length + 1);
        const std::span<const std::byte> input(
            source_storage.data() + source_offset, length);
        const std::span<std::byte> output(
            destination_storage.data() + destination_offset, length);
        std::fill(output.begin(), output.end(), std::byte{0xa5});

        Crc32cAccumulator accumulator;
        size_t consumed = 0;
        while (consumed < length) {
            const size_t chunk_size = std::min<size_t>(
                1 + NextRandom(random) % 37, length - consumed);
            accumulator.CopyAndUpdate(input.subspan(consumed, chunk_size),
                                      output.subspan(consumed, chunk_size));
            consumed += chunk_size;
        }
        accumulator.CopyAndUpdate({}, {});

        SCOPED_TRACE(iteration);
        SCOPED_TRACE(source_offset);
        SCOPED_TRACE(destination_offset);
        SCOPED_TRACE(length);
        EXPECT_TRUE(std::equal(input.begin(), input.end(), output.begin(),
                               output.end()));
        EXPECT_EQ(accumulator.Finish(), Crc32c(input));
    }
}

TEST(Crc32cTest, MatchesReferenceForRandomLengthsAndChunking) {
    std::vector<std::byte> storage(4096 + 16);
    uint64_t random = 0x9e3779b97f4a7c15ull;
    for (std::byte& byte : storage) {
        byte = static_cast<std::byte>(NextRandom(random));
    }

    for (size_t iteration = 0; iteration < 1024; ++iteration) {
        const size_t offset = NextRandom(random) % 16;
        const size_t available = storage.size() - offset;
        const size_t length = NextRandom(random) % (available + 1);
        const std::span<const std::byte> input(storage.data() + offset, length);
        const uint32_t expected = ReferenceCrc32c(input);

        SCOPED_TRACE(iteration);
        SCOPED_TRACE(offset);
        SCOPED_TRACE(length);
        EXPECT_EQ(Crc32c(input), expected);

        Crc32cAccumulator accumulator;
        accumulator.Update({});
        size_t consumed = 0;
        while (consumed < input.size()) {
            const size_t chunk_size = std::min<size_t>(
                1 + NextRandom(random) % 31, input.size() - consumed);
            accumulator.Update(input.subspan(consumed, chunk_size));
            consumed += chunk_size;
        }
        accumulator.Update({});
        EXPECT_EQ(accumulator.Finish(), expected);
    }
}

}  // namespace
}  // namespace mino::bridge
