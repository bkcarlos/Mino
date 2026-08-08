// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>

#include "mino/shm/region/fuzz/fuzz_harness.h"

namespace mino::shm::region::fuzz {
namespace {
struct Statistics {
    std::atomic<uint64_t> calls{0};
    ~Statistics() {
        std::fprintf(stderr,
                     "Extended libFuzzer selectors (handle): "
                     "ResolverBoundary=%llu\n",
                     static_cast<unsigned long long>(calls.load()));
    }
};
Statistics& GetStatistics() {
    static Statistics statistics;
    return statistics;
}
}  // namespace
}  // namespace mino::shm::region::fuzz

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    const auto input = std::span(
        reinterpret_cast<const std::byte*>(data), size);
    mino::shm::region::fuzz::GetStatistics().calls.fetch_add(
        1, std::memory_order_relaxed);
    const mino::Status status = mino::shm::region::fuzz::FuzzOneInput(input);
    if (status.code() == mino::StatusCode::kInternal) std::abort();
    return 0;
}
