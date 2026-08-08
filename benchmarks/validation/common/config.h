// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_COMMON_CONFIG_H_
#define BENCHMARKS_VALIDATION_COMMON_CONFIG_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iosfwd>
#include <optional>
#include <string>
#include <string_view>

namespace mino::benchmarks::validation {

struct Config {
    uint64_t iterations = 10'000;
    uint64_t storage_records = 1'000;
    uint64_t records_per_writer = 100;
    size_t payload_bytes = 64;
    size_t pin_count = 1'000;
    std::string suite = "all";
    std::optional<std::filesystem::path> output_json;
    std::optional<std::filesystem::path> directory;
    std::string commit;
    std::string build_config;
    std::string cpu_model;
    std::string memory_description;
    std::string storage_device;
    std::string filesystem;
    std::string command;
};

void PrintUsage(std::ostream& output, std::string_view program);
Config ParseArguments(int argc, char** argv);
std::string ShellQuote(std::string_view value);

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_COMMON_CONFIG_H_
