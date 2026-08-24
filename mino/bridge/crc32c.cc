// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/crc32c.h"

#include <array>
#include <cassert>
#include <cstring>

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#include <nmmintrin.h>
#define MINO_CRC32C_X86_SSE42 1
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32) && \
    defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#include <arm_acle.h>
#define MINO_CRC32C_ARM_CRC 1
#endif

namespace mino::bridge {
namespace {

constexpr std::array<uint32_t, 256> MakeCrc32cTable() noexcept {
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
}

constexpr auto kCrc32cTable = MakeCrc32cTable();

uint32_t UpdateSoftware(uint32_t state,
                        std::span<const std::byte> bytes) noexcept {
    for (std::byte byte : bytes) {
        const uint8_t value = static_cast<uint8_t>(byte);
        state = kCrc32cTable[(state ^ value) & 0xffu] ^ (state >> 8);
    }
    return state;
}

uint32_t CopyAndUpdateSoftware(uint32_t state,
                               std::span<const std::byte> bytes,
                               std::span<std::byte> output) noexcept {
    for (size_t i = 0; i < bytes.size(); ++i) {
        const std::byte byte = bytes[i];
        output[i] = byte;
        const uint8_t value = static_cast<uint8_t>(byte);
        state = kCrc32cTable[(state ^ value) & 0xffu] ^ (state >> 8);
    }
    return state;
}

#if defined(MINO_CRC32C_X86_SSE42)
__attribute__((target("sse4.2")))
uint32_t UpdateSse42(uint32_t state,
                     std::span<const std::byte> bytes) noexcept {
    const std::byte* data = bytes.data();
    size_t remaining = bytes.size();

    while (remaining >= sizeof(uint64_t)) {
        uint64_t value;
        std::memcpy(&value, data, sizeof(value));
        state = static_cast<uint32_t>(_mm_crc32_u64(state, value));
        data += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining >= sizeof(uint32_t)) {
        uint32_t value;
        std::memcpy(&value, data, sizeof(value));
        state = _mm_crc32_u32(state, value);
        data += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining >= sizeof(uint16_t)) {
        uint16_t value;
        std::memcpy(&value, data, sizeof(value));
        state = _mm_crc32_u16(state, value);
        data += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining != 0) {
        state = _mm_crc32_u8(state, static_cast<uint8_t>(*data));
    }
    return state;
}

__attribute__((target("sse4.2")))
uint32_t CopyAndUpdateSse42(uint32_t state,
                            std::span<const std::byte> bytes,
                            std::span<std::byte> output) noexcept {
    const std::byte* data = bytes.data();
    std::byte* destination = output.data();
    size_t remaining = bytes.size();

    while (remaining >= sizeof(uint64_t)) {
        uint64_t value;
        std::memcpy(&value, data, sizeof(value));
        std::memcpy(destination, &value, sizeof(value));
        state = static_cast<uint32_t>(_mm_crc32_u64(state, value));
        data += sizeof(value);
        destination += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining >= sizeof(uint32_t)) {
        uint32_t value;
        std::memcpy(&value, data, sizeof(value));
        std::memcpy(destination, &value, sizeof(value));
        state = _mm_crc32_u32(state, value);
        data += sizeof(value);
        destination += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining >= sizeof(uint16_t)) {
        uint16_t value;
        std::memcpy(&value, data, sizeof(value));
        std::memcpy(destination, &value, sizeof(value));
        state = _mm_crc32_u16(state, value);
        data += sizeof(value);
        destination += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining != 0) {
        *destination = *data;
        state = _mm_crc32_u8(state, static_cast<uint8_t>(*data));
    }
    return state;
}
#endif

#if defined(MINO_CRC32C_ARM_CRC)
uint32_t UpdateArmCrc(uint32_t state,
                      std::span<const std::byte> bytes) noexcept {
    const std::byte* data = bytes.data();
    size_t remaining = bytes.size();

    while (remaining >= sizeof(uint64_t)) {
        uint64_t value;
        std::memcpy(&value, data, sizeof(value));
        state = __crc32cd(state, value);
        data += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining >= sizeof(uint32_t)) {
        uint32_t value;
        std::memcpy(&value, data, sizeof(value));
        state = __crc32cw(state, value);
        data += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining >= sizeof(uint16_t)) {
        uint16_t value;
        std::memcpy(&value, data, sizeof(value));
        state = __crc32ch(state, value);
        data += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining != 0) {
        state = __crc32cb(state, static_cast<uint8_t>(*data));
    }
    return state;
}

uint32_t CopyAndUpdateArmCrc(uint32_t state,
                             std::span<const std::byte> bytes,
                             std::span<std::byte> output) noexcept {
    const std::byte* data = bytes.data();
    std::byte* destination = output.data();
    size_t remaining = bytes.size();

    while (remaining >= sizeof(uint64_t)) {
        uint64_t value;
        std::memcpy(&value, data, sizeof(value));
        std::memcpy(destination, &value, sizeof(value));
        state = __crc32cd(state, value);
        data += sizeof(value);
        destination += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining >= sizeof(uint32_t)) {
        uint32_t value;
        std::memcpy(&value, data, sizeof(value));
        std::memcpy(destination, &value, sizeof(value));
        state = __crc32cw(state, value);
        data += sizeof(value);
        destination += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining >= sizeof(uint16_t)) {
        uint16_t value;
        std::memcpy(&value, data, sizeof(value));
        std::memcpy(destination, &value, sizeof(value));
        state = __crc32ch(state, value);
        data += sizeof(value);
        destination += sizeof(value);
        remaining -= sizeof(value);
    }
    if (remaining != 0) {
        *destination = *data;
        state = __crc32cb(state, static_cast<uint8_t>(*data));
    }
    return state;
}
#endif

using UpdateFunction =
    uint32_t (*)(uint32_t, std::span<const std::byte>) noexcept;
using CopyAndUpdateFunction =
    uint32_t (*)(uint32_t, std::span<const std::byte>,
                 std::span<std::byte>) noexcept;

UpdateFunction SelectUpdateFunction() noexcept {
#if defined(MINO_CRC32C_X86_SSE42)
    if (__builtin_cpu_supports("sse4.2")) {
        return UpdateSse42;
    }
#elif defined(MINO_CRC32C_ARM_CRC)
    return UpdateArmCrc;
#endif
    return UpdateSoftware;
}

CopyAndUpdateFunction SelectCopyAndUpdateFunction() noexcept {
#if defined(MINO_CRC32C_X86_SSE42)
    if (__builtin_cpu_supports("sse4.2")) {
        return CopyAndUpdateSse42;
    }
#elif defined(MINO_CRC32C_ARM_CRC)
    return CopyAndUpdateArmCrc;
#endif
    return CopyAndUpdateSoftware;
}

}  // namespace

void Crc32cAccumulator::Update(std::span<const std::byte> bytes) noexcept {
    static const UpdateFunction update = SelectUpdateFunction();
    state_ = update(state_, bytes);
}

void Crc32cAccumulator::CopyAndUpdate(
    std::span<const std::byte> bytes, std::span<std::byte> output) noexcept {
    assert(bytes.size() == output.size());
    static const CopyAndUpdateFunction copy_and_update =
        SelectCopyAndUpdateFunction();
    state_ = copy_and_update(state_, bytes, output);
}

uint32_t Crc32c(std::span<const std::byte> bytes) noexcept {
    Crc32cAccumulator accumulator;
    accumulator.Update(bytes);
    return accumulator.Finish();
}

}  // namespace mino::bridge
