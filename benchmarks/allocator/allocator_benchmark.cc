// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "mino/shm/allocator/central_slab.h"

namespace mino::benchmarks {
namespace {

using Clock = std::chrono::steady_clock;

struct Config {
    uint32_t threads = std::thread::hardware_concurrency() == 0
                           ? 4
                           : std::thread::hardware_concurrency();
    uint64_t iterations_per_thread = 100'000;
    uint32_t slots = 4096;
};

struct AlignedDeleter {
    void operator()(std::byte* pointer) const {
        ::operator delete[](pointer, std::align_val_t(64));
    }
};

struct ResultRow {
    std::string_view mode;
    uint64_t operations = 0;
    uint64_t elapsed_ns = 0;
    uint64_t failures = 0;
    AllocatorLocalCacheStats stats;
};

uint64_t ParsePositive(std::string_view argument, std::string_view prefix) {
    const std::string text(argument.substr(prefix.size()));
    size_t parsed = 0;
    const unsigned long long value = std::stoull(text, &parsed, 10);
    if (parsed != text.size() || value == 0) {
        throw std::invalid_argument("benchmark arguments must be positive integers");
    }
    return static_cast<uint64_t>(value);
}

Config ParseArguments(int argc, char** argv) {
    Config config;
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        if (argument.starts_with("--threads=")) {
            const uint64_t value = ParsePositive(argument, "--threads=");
            if (value > 256) throw std::invalid_argument("--threads exceeds 256");
            config.threads = static_cast<uint32_t>(value);
        } else if (argument.starts_with("--iterations=")) {
            const uint64_t value = ParsePositive(argument, "--iterations=");
            if (value > 10'000'000) {
                throw std::invalid_argument("--iterations exceeds 10000000");
            }
            config.iterations_per_thread = value;
        } else if (argument.starts_with("--slots=")) {
            const uint64_t value = ParsePositive(argument, "--slots=");
            if (value > 1'000'000) {
                throw std::invalid_argument("--slots exceeds 1000000");
            }
            config.slots = static_cast<uint32_t>(value);
        } else if (argument == "--help") {
            std::cout << "Usage: allocator_benchmark [--threads=N] "
                         "[--iterations=N] [--slots=N]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown argument: " + std::string(argument));
        }
    }
    if (config.slots < config.threads) {
        throw std::invalid_argument("--slots must be at least --threads");
    }
    return config;
}

ResultRow Run(const Config& config, bool cache_enabled) {
    constexpr uint64_t kMetadataAllowance = 1u << 20;
    constexpr uint64_t kBytesPerSlotAllowance = 256;
    if (config.slots >
        (std::numeric_limits<uint64_t>::max() - kMetadataAllowance) /
            kBytesPerSlotAllowance) {
        throw std::overflow_error("allocator benchmark region size overflow");
    }
    const uint64_t region_size =
        kMetadataAllowance + kBytesPerSlotAllowance * config.slots;
    auto region = std::unique_ptr<std::byte[], AlignedDeleter>(
        new (std::align_val_t(64)) std::byte[region_size]);
    std::memset(region.get(), 0, region_size);

    ClassTableConfig classes;
    classes.classes = {{.slot_size = 64, .slot_count = config.slots}};
    auto created = CentralSlabAllocator::Create(region.get(), region_size, classes);
    if (!created.ok()) {
        throw std::runtime_error(created.status().ToString());
    }
    CentralSlabAllocator allocator = *created;
    allocator.ConfigureLocalCache({.enabled = cache_enabled});

    AllocationRequest request;
    request.object_size = 32;
    request.type_id = TypeId{0xD601};
    request.schema = {.short_id = 0xD601, .layout_version = 1};

    std::atomic<uint32_t> ready{0};
    std::atomic<bool> start{false};
    std::atomic<uint64_t> failures{0};
    std::vector<std::thread> workers;
    workers.reserve(config.threads);
    for (uint32_t thread = 0; thread < config.threads; ++thread) {
        workers.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (uint64_t i = 0; i < config.iterations_per_thread; ++i) {
                auto handle = allocator.Allocate(request);
                if (!handle.ok() || !allocator.Retire(*handle).ok() ||
                    !allocator.Reclaim(*handle).ok()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != config.threads) {
        std::this_thread::yield();
    }
    const Clock::time_point begin = Clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin)
            .count());

    return ResultRow{
        .mode = cache_enabled ? "cursor_cache" : "legacy_scan",
        .operations = config.iterations_per_thread * config.threads,
        .elapsed_ns = elapsed_ns,
        .failures = failures.load(std::memory_order_relaxed),
        .stats = allocator.local_cache_stats(),
    };
}

void Print(const Config& config, const ResultRow& row) {
    const double seconds = static_cast<double>(row.elapsed_ns) / 1'000'000'000.0;
    const double operations_per_second =
        seconds == 0.0 ? 0.0 : static_cast<double>(row.operations) / seconds;
    std::cout << row.mode << ',' << config.threads << ',' << config.slots << ','
              << row.operations << ',' << row.elapsed_ns << ',' << std::fixed
              << std::setprecision(2) << operations_per_second << ','
              << row.stats.hint_hits << ',' << row.stats.fallback_scans << ','
              << row.stats.cache_bypasses << ',' << row.stats.exhaustions << ','
              << row.failures << '\n';
}

}  // namespace
}  // namespace mino::benchmarks

int main(int argc, char** argv) {
    try {
        const mino::benchmarks::Config config =
            mino::benchmarks::ParseArguments(argc, argv);
        std::cout << "mode,threads,slots,operations,elapsed_ns,ops_per_second,"
                     "hint_hits,fallback_scans,cache_bypasses,exhaustions,failures\n";
        mino::benchmarks::Print(config,
                                mino::benchmarks::Run(config, false));
        mino::benchmarks::Print(config,
                                mino::benchmarks::Run(config, true));
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "allocator_benchmark: " << error.what() << '\n';
        return 2;
    }
}
