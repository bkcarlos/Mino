// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string_view>
#include <vector>

#include "mino/storage/fuzz/corpus_support.h"
#include "mino/storage/fuzz/fuzz_harness.h"

namespace {
constexpr std::array<std::string_view, 5> kCorpus = {
    "mino/storage/fuzz/testdata/format_header.hex",
    "mino/storage/fuzz/testdata/format_record.hex",
    "mino/storage/fuzz/testdata/format_size.hex",
    "mino/storage/fuzz/testdata/scanner_header.hex",
    "mino/storage/fuzz/testdata/scanner_tail.hex",
};
uint64_t Next(uint64_t* state) {
    *state ^= *state >> 12;
    *state ^= *state << 25;
    *state ^= *state >> 27;
    return *state * 0x2545f4914f6cdd1dull;
}
}  // namespace

int main() {
    uint64_t state = 0x5345474d454e5455ull;
    uint64_t executions = 0;
    for (std::string_view path : kCorpus) {
        auto seed = mino::storage::fuzz::ReadHexCorpus(path);
        if (!seed || seed->empty()) {
            std::cerr << "invalid segment corpus: " << path << '\n';
            return 2;
        }
        for (size_t iteration = 0; iteration < 128; ++iteration) {
            std::vector<std::byte> input = *seed;
            if (iteration != 0) {
                const size_t offset = Next(&state) % input.size();
                input[offset] ^= static_cast<std::byte>(Next(&state) & 0xffu);
            }
            const mino::Status status = mino::storage::fuzz::FuzzOneInput(input);
            if (status.code() == mino::StatusCode::kInternal) return 1;
            ++executions;
        }
    }
    std::cout << "segment standalone fuzz: seed=" << state
              << " executions=" << executions << '\n';
    return 0;
}
