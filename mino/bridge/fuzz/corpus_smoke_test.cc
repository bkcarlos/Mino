// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include <array>
#include <iostream>
#include <string_view>

#include "mino/bridge/fuzz/corpus_support.h"
#include "mino/bridge/fuzz/fuzz_harness.h"

int main() {
    constexpr std::array<std::string_view, 5> kCorpus = {
        "mino/bridge/fuzz/testdata/frame_body.hex",
        "mino/bridge/fuzz/testdata/stream.hex",
        "mino/bridge/fuzz/testdata/control_ack.hex",
        "mino/bridge/fuzz/testdata/control_hello.hex",
        "mino/bridge/fuzz/testdata/control_discovery.hex",
    };
    std::array<bool, 3> selectors{};
    for (std::string_view path : kCorpus) {
        auto input = mino::bridge::fuzz::ReadHexCorpus(path);
        if (!input || input->empty()) {
            std::cerr << "invalid frame corpus: " << path << '\n';
            return 2;
        }
        const auto selector = mino::bridge::fuzz::SelectFrameHarness(*input);
        selectors[static_cast<size_t>(selector)] = true;
        const mino::Status first = mino::bridge::fuzz::FuzzOneInput(*input);
        const mino::Status second = mino::bridge::fuzz::FuzzOneInput(*input);
        if (first.code() != second.code() ||
            first.code() == mino::StatusCode::kInternal) {
            std::cerr << "non-deterministic or internal frame corpus result\n";
            return 1;
        }
    }
    for (bool covered : selectors) {
        if (!covered) return 1;
    }
    std::cout << "frame deterministic corpus smoke: PASS\n";
    return 0;
}
