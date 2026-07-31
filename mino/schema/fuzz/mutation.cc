// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/fuzz/mutation.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <vector>

#include "mino/common/status.h"

namespace mino::schema::fuzz {
namespace {

uint64_t Mix(uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ull;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ull;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebull;
    return value ^ (value >> 31);
}

size_t BoundedIndex(uint64_t entropy, size_t count) noexcept {
    return count == 0 ? 0 : static_cast<size_t>(entropy % count);
}

}  // namespace

Result<std::vector<std::byte>> Mutate(std::span<const std::byte> seed,
                                      MutationKind kind, uint64_t entropy,
                                      size_t max_output_bytes) noexcept {
    try {
        const size_t limit =
            std::min(max_output_bytes, kMaxMutationOutputBytes);
        const size_t initial_size = std::min(seed.size(), limit);
        std::vector<std::byte> output(seed.begin(), seed.begin() + initial_size);
        uint64_t random = Mix(entropy);

        switch (kind) {
            case MutationKind::kTruncate: {
                output.resize(BoundedIndex(random, output.size() + 1));
                break;
            }
            case MutationKind::kBitFlip: {
                if (output.empty()) {
                    if (limit != 0) output.push_back(std::byte{0});
                }
                if (!output.empty()) {
                    const size_t index = BoundedIndex(random, output.size());
                    random = Mix(random);
                    const uint8_t bit = static_cast<uint8_t>(1u << (random & 7u));
                    output[index] ^= static_cast<std::byte>(bit);
                }
                break;
            }
            case MutationKind::kInsert: {
                const size_t available = limit - output.size();
                if (available == 0) break;
                const size_t count = std::min<size_t>(1 + (random & 7u), available);
                random = Mix(random);
                const size_t position = BoundedIndex(random, output.size() + 1);
                output.insert(output.begin() + position, count, std::byte{0});
                for (size_t i = 0; i < count; ++i) {
                    random = Mix(random);
                    output[position + i] = static_cast<std::byte>(random & 0xffu);
                }
                break;
            }
            case MutationKind::kRepeat: {
                const size_t available = limit - output.size();
                if (output.empty() || available == 0) break;
                constexpr size_t kMaxRepeatBytes = 32;
                const size_t source = BoundedIndex(random, output.size());
                random = Mix(random);
                const size_t source_available = output.size() - source;
                const size_t count = std::min(
                    {size_t{1} + static_cast<size_t>(random & 31u),
                     source_available, available, kMaxRepeatBytes});
                std::array<std::byte, kMaxRepeatBytes> copy{};
                std::copy_n(output.begin() + source, count, copy.begin());
                random = Mix(random);
                const size_t position = BoundedIndex(random, output.size() + 1);
                output.insert(output.begin() + position, copy.begin(),
                              copy.begin() + count);
                break;
            }
            case MutationKind::kLengthBomb: {
                constexpr std::array<std::byte, 12> kBomb = {
                    std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                    std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                    std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                    std::byte{0xff}, std::byte{0x02}, std::byte{'9'},
                };
                const size_t available = limit - output.size();
                const size_t count = std::min(available, kBomb.size());
                if (count != 0) {
                    const size_t position =
                        BoundedIndex(random, output.size() + 1);
                    output.insert(output.begin() + position, kBomb.begin(),
                                  kBomb.begin() + count);
                } else if (!output.empty()) {
                    const size_t position = BoundedIndex(random, output.size());
                    output[position] = std::byte{0xff};
                }
                break;
            }
        }
        return output;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema::fuzz
