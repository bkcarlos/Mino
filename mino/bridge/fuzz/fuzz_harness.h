// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_BRIDGE_FUZZ_FUZZ_HARNESS_H_
#define MINO_BRIDGE_FUZZ_FUZZ_HARNESS_H_

#include <cstddef>
#include <cstdint>
#include <span>

#include "mino/common/status.h"

namespace mino::bridge::fuzz {

inline constexpr size_t kMaxFrameFuzzInputBytes = 64u << 10;

enum class FrameFuzzSelector : uint8_t {
    kFrameBody = 0,
    kStream = 1,
    kControl = 2,
};

FrameFuzzSelector SelectFrameHarness(std::span<const std::byte> input) noexcept;
Status FuzzFrameBody(std::span<const std::byte> input) noexcept;
Status FuzzFrameStream(std::span<const std::byte> input) noexcept;
Status FuzzControlPayload(std::span<const std::byte> input) noexcept;
Status FuzzOneInput(std::span<const std::byte> input) noexcept;

}  // namespace mino::bridge::fuzz

#endif  // MINO_BRIDGE_FUZZ_FUZZ_HARNESS_H_
