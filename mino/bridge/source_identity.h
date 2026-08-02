// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_SOURCE_IDENTITY_H_
#define MINO_BRIDGE_SOURCE_IDENTITY_H_

#include <cstddef>
#include <cstdint>

namespace mino::bridge {

// Stable publisher identity used by reliable transport. This type is a logical
// value only; wire codecs serialize each field explicitly in big-endian order.
struct SourceIdentity {
    uint64_t node_id = 0;
    uint64_t publisher_id = 0;
    uint64_t publisher_epoch = 0;

    bool operator==(const SourceIdentity&) const = default;
};

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
