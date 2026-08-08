// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_SHM_REGION_FUZZ_FUZZ_HARNESS_H_
#define MINO_SHM_REGION_FUZZ_FUZZ_HARNESS_H_

#include <cstddef>
#include <span>

#include "mino/common/status.h"

namespace mino::shm::region::fuzz {

inline constexpr size_t kMaxHandleFuzzInputBytes = 256;

Status FuzzHandleResolver(std::span<const std::byte> input) noexcept;
Status FuzzOneInput(std::span<const std::byte> input) noexcept;

}  // namespace mino::shm::region::fuzz

#endif  // MINO_SHM_REGION_FUZZ_FUZZ_HARNESS_H_
