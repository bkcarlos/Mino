// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/common/payload.h"

#include <cstdint>

namespace mino::benchmarks::validation {

std::vector<std::byte> MakePayload(size_t payload_bytes) {
    constexpr uint64_t kPayloadSeed = 0x4d494e4f563134ULL;
    std::vector<std::byte> payload(payload_bytes);
    uint64_t state = kPayloadSeed;
    for (std::byte& byte : payload) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        byte = static_cast<std::byte>(state & 0xffu);
    }
    return payload;
}

}  // namespace mino::benchmarks::validation
