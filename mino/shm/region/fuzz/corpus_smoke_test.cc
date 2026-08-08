// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include <array>
#include <iostream>
#include <string_view>

#include "mino/shm/region/fuzz/corpus_support.h"
#include "mino/shm/region/fuzz/fuzz_harness.h"

int main() {
    constexpr std::array<std::string_view, 6> kCorpus = {
        "mino/shm/region/fuzz/testdata/valid.hex",
        "mino/shm/region/fuzz/testdata/offset.hex",
        "mino/shm/region/fuzz/testdata/identity.hex",
        "mino/shm/region/fuzz/testdata/metadata.hex",
        "mino/shm/region/fuzz/testdata/extent.hex",
        "mino/shm/region/fuzz/testdata/state.hex",
    };
    for (std::string_view path : kCorpus) {
        auto input = mino::shm::region::fuzz::ReadHexCorpus(path);
        if (!input || input->empty()) return 2;
        const mino::Status first =
            mino::shm::region::fuzz::FuzzOneInput(*input);
        const mino::Status second =
            mino::shm::region::fuzz::FuzzOneInput(*input);
        if ((path == kCorpus.front() && !first.ok()) ||
            first.code() != second.code() ||
            first.code() == mino::StatusCode::kInternal) {
            std::cerr << "invalid handle corpus result: path=" << path
                      << " first=" << first.ToString()
                      << " second=" << second.ToString() << '\n';
            return 1;
        }
    }
    std::cout << "handle deterministic corpus smoke: PASS\n";
    return 0;
}
