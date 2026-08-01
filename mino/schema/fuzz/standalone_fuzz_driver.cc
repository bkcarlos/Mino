// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/fuzz/fuzz_harness.h"
#include "mino/schema/fuzz/mutation.h"

namespace mino::schema::fuzz {
namespace {

constexpr uint64_t kDefaultSeconds = 2;
constexpr uint64_t kMaxSeconds = 3600;
constexpr uint64_t kDefaultSeed = 0xd314f0226a09e667ull;

constexpr std::string_view kIdlSeed = R"idl(
syntax = "v1";
package fuzz;
message Payload {
  int32 delta = 1;
  uint64 sequence = 2;
  fixed32 code = 3;
  float sample = 4;
  optional string label = 5 [max_bytes = 32];
  vector<uint32> values = 6 [max_capacity = 4];
}
)idl";



constexpr std::array<uint8_t, 26> kWireSeed = {
    0x08, 0x01, 0x10, 0xac, 0x02, 0x1d, 0x12, 0x34, 0x56,
    0x78, 0x25, 0x34, 0x12, 0xc0, 0x7f, 0x2a, 0x02, 0xc3,
    0xa9, 0x32, 0x05, 0x03, 0x01, 0x7f, 0x80, 0x01,
};

bool ParseEnvironment(std::string_view name, uint64_t default_value,
                      uint64_t& value) {
    const std::string name_string(name);
    const char* raw = std::getenv(name_string.c_str());
    if (raw == nullptr || *raw == '\0') {
        value = default_value;
        return true;
    }
    const std::string_view text(raw);
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

uint64_t Next(uint64_t& state) {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return state * 0x2545f4914f6cdd1dull;
}

std::vector<std::byte> StringBytes(std::string_view text) {
    const auto bytes = std::as_bytes(std::span(text.data(), text.size()));
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

std::vector<std::byte> DescriptorBytes() {
    std::ifstream input(
        "mino/schema/fuzz/testdata/codegen_golden.descriptor",
        std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot read MINODSC2 fuzz seed");
    }
    const std::string artifact{std::istreambuf_iterator<char>(input),
                               std::istreambuf_iterator<char>()};
    return StringBytes(artifact);
}

std::vector<std::byte> WireBytes() {
    std::vector<std::byte> bytes;
    bytes.reserve(kWireSeed.size());
    for (uint8_t value : kWireSeed) bytes.push_back(static_cast<std::byte>(value));
    return bytes;
}

}  // namespace
}  // namespace mino::schema::fuzz

int main() try {
    using namespace mino::schema::fuzz;
    uint64_t seconds = 0;
    uint64_t seed = 0;
    if (!ParseEnvironment("MINO_D3_FUZZ_SECONDS", kDefaultSeconds, seconds) ||
        seconds > kMaxSeconds) {
        std::cerr << "MINO_D3_FUZZ_SECONDS must be an integer in [0, 3600]\n";
        return 2;
    }
    if (!ParseEnvironment("MINO_D3_FUZZ_SEED", kDefaultSeed, seed)) {
        std::cerr << "MINO_D3_FUZZ_SEED must be an unsigned integer\n";
        return 2;
    }
    if (seed == 0) seed = kDefaultSeed;

    const std::array<std::vector<std::byte>, 3> corpus = {
        StringBytes(kIdlSeed), DescriptorBytes(), WireBytes()};
    const std::array<size_t, 3> limits = {
        kMaxIdlInputBytes, kMaxDescriptorInputBytes,
        kMaxCanonicalPayloadBytes};
    std::array<uint64_t, 14> statuses{};
    uint64_t iterations = 0;
    uint64_t state = seed;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(seconds);

    do {
        const size_t harness = static_cast<size_t>(Next(state) % corpus.size());
        const auto kind =
            static_cast<MutationKind>(Next(state) % kMutationKindCount);
        auto input = Mutate(corpus[harness], kind, Next(state), limits[harness]);
        mino::Status status =
            input.ok() ? mino::Status::Ok() : input.status();
        if (input.ok()) {
            if (harness == 0) status = FuzzIdl(*input);
            if (harness == 1) status = FuzzDescriptor(*input);
            if (harness == 2) status = FuzzCanonicalPayload(*input);
        }
        const size_t code = static_cast<size_t>(status.code());
        if (code >= statuses.size()) {
            std::cerr << "invalid StatusCode from harness\n";
            return 1;
        }
        ++statuses[code];
        ++iterations;
    } while (std::chrono::steady_clock::now() < deadline);

    std::cout << "D3-14 standalone fuzz: seconds=" << seconds
              << " seed=" << seed << " iterations=" << iterations << '\n';
    for (size_t code = 0; code < statuses.size(); ++code) {
        if (statuses[code] != 0) {
            std::cout << "status[" << code << "]=" << statuses[code] << '\n';
        }
    }
    return 0;
} catch (const std::bad_alloc&) {
    std::cerr << "standalone fuzz driver exhausted memory\n";
    return 1;
} catch (...) {
    std::cerr << "standalone fuzz driver caught an unexpected exception\n";
    return 1;
}
