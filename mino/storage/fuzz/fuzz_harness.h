// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_STORAGE_FUZZ_FUZZ_HARNESS_H_
#define MINO_STORAGE_FUZZ_FUZZ_HARNESS_H_

#include <cstddef>
#include <cstdint>
#include <span>

#include "mino/common/status.h"

namespace mino::storage::fuzz {

inline constexpr size_t kMaxSegmentFuzzInputBytes = 64u << 10;

enum class SegmentFuzzSelector : uint8_t {
    kFormat = 0,
    kScanner = 1,
};

SegmentFuzzSelector SelectSegmentHarness(
    std::span<const std::byte> input) noexcept;
Status FuzzSegmentFormat(std::span<const std::byte> input) noexcept;
Status FuzzSegmentScanner(std::span<const std::byte> input) noexcept;
Status FuzzOneInput(std::span<const std::byte> input) noexcept;

}  // namespace mino::storage::fuzz

#endif  // MINO_STORAGE_FUZZ_FUZZ_HARNESS_H_
