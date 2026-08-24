// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_CRC32C_H_
#define MINO_BRIDGE_CRC32C_H_

#include <cstddef>
#include <cstdint>
#include <span>

namespace mino::bridge {

// Incremental CRC-32C (Castagnoli) calculator. The wire value is the standard
// reflected CRC-32C with initial and final XOR of 0xffffffff.
class Crc32cAccumulator {
public:
    void Update(std::span<const std::byte> bytes) noexcept;

    // Copies bytes to an equally sized, non-overlapping output span while
    // updating this accumulator from exactly the values written. This uses the
    // same runtime-selected implementation as Update.
    void CopyAndUpdate(std::span<const std::byte> bytes,
                       std::span<std::byte> output) noexcept;

    uint32_t Finish() const noexcept { return state_ ^ 0xffffffffu; }

private:
    uint32_t state_ = 0xffffffffu;
};

uint32_t Crc32c(std::span<const std::byte> bytes) noexcept;

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_CRC32C_H_
