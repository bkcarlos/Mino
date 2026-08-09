// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_SOURCE_IDENTITY_H_
#define MINO_BRIDGE_SOURCE_IDENTITY_H_

#include <cstddef>
#include <cstdint>

namespace mino::bridge {

inline constexpr uint16_t kMaxBridgeLaneCount = 8;

// Stable publisher identity used by reliable transport. This type is a logical
// value only; wire codecs serialize each field explicitly in big-endian order.
struct SourceIdentity {
    uint64_t node_id = 0;
    uint64_t publisher_id = 0;
    uint64_t publisher_epoch = 0;

    bool operator==(const SourceIdentity&) const = default;
};

inline constexpr uint64_t StableBridgeLaneHash(
    const SourceIdentity& source) noexcept {
    constexpr uint64_t kFnv1aOffset = 14695981039346656037ull;
    constexpr uint64_t kFnv1aPrime = 1099511628211ull;

    uint64_t hash = kFnv1aOffset;
    const auto consume_big_endian = [&hash](uint64_t value) constexpr noexcept {
        for (int shift = 56; shift >= 0; shift -= 8) {
            hash ^= static_cast<uint8_t>(value >> shift);
            hash *= kFnv1aPrime;
        }
    };
    consume_big_endian(source.node_id);
    consume_big_endian(source.publisher_id);
    consume_big_endian(source.publisher_epoch);

    // FNV-1a has weak low-bit diffusion, while lane counts are commonly powers
    // of two. Apply a stable SplitMix64 finalizer before modulo selection so
    // structured publisher identities do not collapse onto one lane.
    hash ^= hash >> 30;
    hash *= 0xbf58476d1ce4e5b9ull;
    hash ^= hash >> 27;
    hash *= 0x94d049bb133111ebull;
    hash ^= hash >> 31;
    return hash;
}

inline constexpr uint16_t BridgeLaneFor(const SourceIdentity& source,
                                        uint16_t lane_count) noexcept {
    if (lane_count == 0 || lane_count > kMaxBridgeLaneCount) return 0;
    return static_cast<uint16_t>(StableBridgeLaneHash(source) % lane_count);
}

struct SourceIdentityHash {
    size_t operator()(const SourceIdentity& source) const noexcept {
        uint64_t value = source.node_id + 0x9e3779b97f4a7c15ull;
        value ^= source.publisher_id + 0x9e3779b97f4a7c15ull +
                 (value << 6) + (value >> 2);
        value ^= source.publisher_epoch + 0x9e3779b97f4a7c15ull +
                 (value << 6) + (value >> 2);
        if constexpr (sizeof(size_t) < sizeof(uint64_t)) {
            value ^= value >> 32;
        }
        return static_cast<size_t>(value);
    }
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_SOURCE_IDENTITY_H_
