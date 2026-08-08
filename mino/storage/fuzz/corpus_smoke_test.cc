// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include <array>
#include <iostream>
#include <string_view>

#include "mino/storage/fuzz/corpus_support.h"
#include "mino/storage/fuzz/fuzz_harness.h"

int main() {
    constexpr std::array<std::string_view, 5> kCorpus = {
        "mino/storage/fuzz/testdata/format_header.hex",
        "mino/storage/fuzz/testdata/format_record.hex",
        "mino/storage/fuzz/testdata/format_size.hex",
        "mino/storage/fuzz/testdata/scanner_header.hex",
        "mino/storage/fuzz/testdata/scanner_tail.hex",
    };
    std::array<bool, 2> selectors{};
    for (std::string_view path : kCorpus) {
        auto input = mino::storage::fuzz::ReadHexCorpus(path);
        if (!input || input->empty()) return 2;
        const auto selector = mino::storage::fuzz::SelectSegmentHarness(*input);
        selectors[static_cast<size_t>(selector)] = true;
        const mino::Status first = mino::storage::fuzz::FuzzOneInput(*input);
        const mino::Status second = mino::storage::fuzz::FuzzOneInput(*input);
        if (first.code() != second.code() ||
            first.code() == mino::StatusCode::kInternal) {
            std::cerr << "non-deterministic or internal segment corpus result\n";
            return 1;
        }
    }
    for (bool covered : selectors) if (!covered) return 1;
    std::cout << "segment deterministic corpus smoke: PASS\n";
    return 0;
}
