// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/crc32c.h"

#include <array>

namespace mino::bridge {
namespace {

const std::array<uint32_t, 256>& Crc32cTable() noexcept {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> values{};
        for (uint32_t i = 0; i < values.size(); ++i) {
            uint32_t value = i;
            for (int bit = 0; bit < 8; ++bit) {
                value = (value >> 1) ^
                        ((value & 1u) != 0 ? 0x82f63b78u : 0u);
            }
            values[i] = value;
        }
        return values;
    }();
    return table;
}

}  // namespace

void Crc32cAccumulator::Update(std::span<const std::byte> bytes) noexcept {
    const auto& table = Crc32cTable();
    for (std::byte byte : bytes) {
        const uint8_t value = static_cast<uint8_t>(byte);
        state_ = table[(state_ ^ value) & 0xffu] ^ (state_ >> 8);
    }
}

uint32_t Crc32c(std::span<const std::byte> bytes) noexcept {
    Crc32cAccumulator accumulator;
    accumulator.Update(bytes);
    return accumulator.Finish();
}

}  // namespace mino::bridge
