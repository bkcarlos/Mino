// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_COMMON_RUNTIME_H_
#define BENCHMARKS_VALIDATION_COMMON_RUNTIME_H_

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::benchmarks::validation {

using Clock = std::chrono::steady_clock;

uint64_t DurationNs(Clock::time_point begin, Clock::time_point end);
void SinkXor(uint64_t value);
uint64_t SinkValue();
void MarkFailed();
bool HasFailed();

inline void Require(const Status& status, std::string_view operation) {
    if (!status.ok()) {
        throw std::runtime_error(std::string(operation) + ": " +
                                 status.ToString());
    }
}

template <typename T>
T Take(Result<T>&& result, std::string_view operation) {
    if (!result.ok()) {
        throw std::runtime_error(std::string(operation) + ": " +
                                 result.status().ToString());
    }
    return std::move(*result);
}

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_COMMON_RUNTIME_H_
