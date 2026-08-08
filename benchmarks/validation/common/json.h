// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_COMMON_JSON_H_
#define BENCHMARKS_VALIDATION_COMMON_JSON_H_

#include <string>
#include <string_view>

namespace mino::benchmarks::validation {

std::string JsonEscape(std::string_view input);
std::string PendingResult(std::string_view reason);

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_COMMON_JSON_H_
