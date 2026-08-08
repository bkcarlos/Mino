// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_COMMON_PAYLOAD_H_
#define BENCHMARKS_VALIDATION_COMMON_PAYLOAD_H_

#include <cstddef>
#include <vector>

namespace mino::benchmarks::validation {

std::vector<std::byte> MakePayload(size_t payload_bytes);

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_COMMON_PAYLOAD_H_
