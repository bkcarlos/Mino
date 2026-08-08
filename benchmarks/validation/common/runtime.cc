// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/common/runtime.h"

#include <atomic>
#include <chrono>

namespace mino::benchmarks::validation {
namespace {

std::atomic<uint64_t> g_sink{0};
std::atomic<bool> g_failed{false};

}  // namespace

uint64_t DurationNs(Clock::time_point begin, Clock::time_point end) {
    const auto value =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
    return value <= 0 ? 0 : static_cast<uint64_t>(value);
}

void SinkXor(uint64_t value) {
    g_sink.fetch_xor(value, std::memory_order_relaxed);
}

uint64_t SinkValue() { return g_sink.load(std::memory_order_relaxed); }

void MarkFailed() { g_failed.store(true, std::memory_order_relaxed); }

bool HasFailed() { return g_failed.load(std::memory_order_relaxed); }

}  // namespace mino::benchmarks::validation
