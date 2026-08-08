// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>

#include "mino/common/status.h"
#include "mino/schema/fuzz/fuzz_harness.h"

namespace mino::schema::fuzz {
namespace {

struct HarnessStatistics {
    std::array<std::atomic<uint64_t>, 3> calls{};

    ~HarnessStatistics() {
        std::fprintf(stderr,
                     "Schema libFuzzer selectors: IDL=%llu Descriptor=%llu "
                     "CanonicalPayload=%llu\n",
                     static_cast<unsigned long long>(
                         calls[0].load(std::memory_order_relaxed)),
                     static_cast<unsigned long long>(
                         calls[1].load(std::memory_order_relaxed)),
                     static_cast<unsigned long long>(
                         calls[2].load(std::memory_order_relaxed)));
    }
};

HarnessStatistics& Statistics() {
    static HarnessStatistics statistics;
    return statistics;
}



}  // namespace
}  // namespace mino::schema::fuzz

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    const auto* bytes = reinterpret_cast<const std::byte*>(data);
    const std::span<const std::byte> input(bytes, size);
    const auto harness = mino::schema::fuzz::SelectFuzzHarness(input);
    const size_t selector = static_cast<size_t>(harness);
    mino::schema::fuzz::Statistics().calls[selector].fetch_add(
        1, std::memory_order_relaxed);

    const mino::Status status = mino::schema::fuzz::FuzzOneInput(input);
    if (!mino::schema::fuzz::IsExpectedFuzzStatus(harness, status.code())) {
        std::fprintf(stderr,
                     "Schema libFuzzer harness returned unexpected status: "
                     "selector=%zu code=%u\n",
                     selector, static_cast<unsigned>(status.code()));
        std::abort();
    }
    return 0;
}
