// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_BRIDGE_FUZZ_CORPUS_SUPPORT_H_
#define MINO_BRIDGE_FUZZ_CORPUS_SUPPORT_H_

#include <cctype>
#include <cstddef>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace mino::bridge::fuzz {

inline std::optional<std::vector<std::byte>> ReadHexCorpus(
    std::string_view path) {
    std::ifstream input(std::string(path), std::ios::binary);
    if (!input) return std::nullopt;
    const std::string text{std::istreambuf_iterator<char>(input),
                           std::istreambuf_iterator<char>()};
    std::vector<std::byte> bytes;
    int high = -1;
    for (unsigned char character : text) {
        if (std::isspace(character) != 0) continue;
        int nibble = -1;
        if (character >= '0' && character <= '9') nibble = character - '0';
        if (character >= 'a' && character <= 'f') nibble = character - 'a' + 10;
        if (character >= 'A' && character <= 'F') nibble = character - 'A' + 10;
        if (nibble < 0) return std::nullopt;
        if (high < 0) {
            high = nibble;
        } else {
            bytes.push_back(static_cast<std::byte>((high << 4) | nibble));
            high = -1;
        }
    }
    if (high >= 0) return std::nullopt;
    return bytes;
}

}  // namespace mino::bridge::fuzz

#endif  // MINO_BRIDGE_FUZZ_CORPUS_SUPPORT_H_
