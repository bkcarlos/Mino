// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>

#include "mino/storage/fuzz/fuzz_harness.h"

namespace mino::storage::fuzz {
namespace {

struct Statistics {
    std::array<std::atomic<uint64_t>, 2> calls{};
    ~Statistics() {
        std::fprintf(stderr,
                     "Extended libFuzzer selectors (segment): Format=%llu "
                     "Scanner=%llu\n",
                     static_cast<unsigned long long>(calls[0].load()),
                     static_cast<unsigned long long>(calls[1].load()));
    }
};

Statistics& GetStatistics() {
    static Statistics statistics;
    return statistics;
}

}  // namespace
}  // namespace mino::storage::fuzz

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    const auto input = std::span(
        reinterpret_cast<const std::byte*>(data), size);
    const size_t selector = static_cast<size_t>(
        mino::storage::fuzz::SelectSegmentHarness(input));
    mino::storage::fuzz::GetStatistics().calls[selector].fetch_add(
        1, std::memory_order_relaxed);
    const mino::Status status = mino::storage::fuzz::FuzzOneInput(input);
    if (status.code() == mino::StatusCode::kInternal) std::abort();
    return 0;
}
