// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/common/config.h"

#include <charconv>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace mino::benchmarks::validation {
namespace {

uint64_t ParseUnsigned(std::string_view text, std::string_view option) {
    uint64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        throw std::runtime_error(std::string(option) +
                                 " requires an unsigned integer");
    }
    return value;
}

std::optional<std::string_view> InlineOption(std::string_view argument,
                                             std::string_view option) {
    if (argument == option) return std::string_view{};
    if (argument.size() > option.size() && argument.starts_with(option) &&
        argument[option.size()] == '=') {
        return argument.substr(option.size() + 1);
    }
    return std::nullopt;
}

std::string_view OptionValue(int* index, int argc, char** argv,
                             std::string_view option,
                             std::string_view inline_value) {
    if (inline_value.data() != nullptr) return inline_value;
    if (*index + 1 >= argc) {
        throw std::runtime_error(std::string(option) + " requires a value");
    }
    ++(*index);
    return argv[*index];
}

std::string EnvironmentOr(std::string_view name, std::string fallback) {
    const char* value = std::getenv(std::string(name).c_str());
    return value == nullptr || *value == '\0' ? std::move(fallback)
                                               : std::string(value);
}

}  // namespace

std::string ShellQuote(std::string_view value) {
    if (value.find_first_of(" \t\n'\"\\$`") == std::string_view::npos) {
        return std::string(value);
    }
    std::string quoted = "'";
    for (char character : value) {
        if (character == '\'') quoted += "'\\''";
        else quoted += character;
    }
    quoted += '\'';
    return quoted;
}

void PrintUsage(std::ostream& output, std::string_view program) {
    output
        << "Usage: " << program << " [options]\n"
        << "  --suite all|memory|storage\n"
        << "  --iterations N              V-14/V-15 measured operations\n"
        << "  --storage-records N         V-17 records per sync policy\n"
        << "  --records-per-writer N      V-16 records per TopicWriter\n"
        << "  --payload-bytes N           V-16/V-17 payload bytes\n"
        << "  --pin-count N               V-27 lease-cleanup pins\n"
        << "  --directory PATH            temporary storage base\n"
        << "  --output-json PATH          additionally write JSON to PATH\n"
        << "  --commit SHA                overrides MINO_BENCHMARK_COMMIT\n"
        << "  --build-config TEXT         overrides MINO_BENCHMARK_BUILD_CONFIG\n"
        << "  --cpu-model TEXT            overrides MINO_BENCHMARK_CPU_MODEL\n"
        << "  --memory TEXT               overrides MINO_BENCHMARK_MEMORY\n"
        << "  --storage-device TEXT       overrides MINO_BENCHMARK_STORAGE_DEVICE\n"
        << "  --filesystem TEXT           overrides MINO_BENCHMARK_FILESYSTEM\n";
}

Config ParseArguments(int argc, char** argv) {
    Config config;
    std::ostringstream command;
    for (int index = 0; index < argc; ++index) {
        if (index != 0) command << ' ';
        command << ShellQuote(argv[index]);
    }
    config.command = command.str();

    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--help") {
            PrintUsage(std::cout, argv[0]);
            std::exit(0);
        }
        const auto parse_string = [&](std::string_view option,
                                      std::string* destination) -> bool {
            const auto value = InlineOption(argument, option);
            if (!value.has_value()) return false;
            *destination = OptionValue(&index, argc, argv, option, *value);
            return true;
        };
        const auto parse_number = [&](std::string_view option,
                                      uint64_t* destination) -> bool {
            const auto value = InlineOption(argument, option);
            if (!value.has_value()) return false;
            *destination = ParseUnsigned(
                OptionValue(&index, argc, argv, option, *value), option);
            return true;
        };
        uint64_t parsed = 0;
        if (parse_string("--suite", &config.suite) ||
            parse_string("--commit", &config.commit) ||
            parse_string("--build-config", &config.build_config) ||
            parse_string("--cpu-model", &config.cpu_model) ||
            parse_string("--memory", &config.memory_description) ||
            parse_string("--storage-device", &config.storage_device) ||
            parse_string("--filesystem", &config.filesystem)) {
            continue;
        }
        if (parse_number("--iterations", &config.iterations) ||
            parse_number("--storage-records", &config.storage_records) ||
            parse_number("--records-per-writer", &config.records_per_writer)) {
            continue;
        }
        if (parse_number("--payload-bytes", &parsed)) {
            if (parsed > std::numeric_limits<size_t>::max()) {
                throw std::runtime_error("--payload-bytes exceeds size_t");
            }
            config.payload_bytes = static_cast<size_t>(parsed);
            continue;
        }
        if (parse_number("--pin-count", &parsed)) {
            if (parsed > std::numeric_limits<size_t>::max()) {
                throw std::runtime_error("--pin-count exceeds size_t");
            }
            config.pin_count = static_cast<size_t>(parsed);
            continue;
        }
        if (const auto value = InlineOption(argument, "--directory")) {
            config.directory = std::filesystem::path(
                OptionValue(&index, argc, argv, "--directory", *value));
            continue;
        }
        if (const auto value = InlineOption(argument, "--output-json")) {
            config.output_json = std::filesystem::path(
                OptionValue(&index, argc, argv, "--output-json", *value));
            continue;
        }
        throw std::runtime_error("unknown option: " + std::string(argument));
    }

    if (config.suite != "all" && config.suite != "memory" &&
        config.suite != "storage") {
        throw std::runtime_error("--suite must be all, memory, or storage");
    }
    if (config.iterations == 0 || config.storage_records == 0 ||
        config.records_per_writer == 0 || config.pin_count == 0) {
        throw std::runtime_error("operation counts must be greater than zero");
    }
    if (config.payload_bytes > 16u * 1024u * 1024u) {
        throw std::runtime_error("--payload-bytes exceeds the 16 MiB pool limit");
    }
    if (config.iterations > 1'000'000 ||
        config.storage_records > 1'000'000 ||
        config.records_per_writer > 100'000) {
        throw std::runtime_error("operation count exceeds benchmark safety limit");
    }
    if (config.pin_count > 50'000) {
        throw std::runtime_error("--pin-count exceeds benchmark safety limit 50000");
    }

    if (config.commit.empty()) {
        config.commit = EnvironmentOr("MINO_BENCHMARK_COMMIT", "PENDING");
    }
    if (config.build_config.empty()) {
        config.build_config =
            EnvironmentOr("MINO_BENCHMARK_BUILD_CONFIG", "PENDING");
    }
    if (config.cpu_model.empty()) {
        config.cpu_model = EnvironmentOr("MINO_BENCHMARK_CPU_MODEL", "PENDING");
    }
    if (config.memory_description.empty()) {
        config.memory_description =
            EnvironmentOr("MINO_BENCHMARK_MEMORY", "PENDING");
    }
    if (config.storage_device.empty()) {
        config.storage_device =
            EnvironmentOr("MINO_BENCHMARK_STORAGE_DEVICE", "PENDING");
    }
    if (config.filesystem.empty()) {
        config.filesystem =
            EnvironmentOr("MINO_BENCHMARK_FILESYSTEM", "PENDING");
    }
    return config;
}

}  // namespace mino::benchmarks::validation
