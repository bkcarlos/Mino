// Copyright 2026 The Mino Authors
// Durable snapshot/group-commit microbenchmark. Run on the deployment disk;
// tmpfs and container results are not production storage qualification.
#include "mino/bridge/dedup_store.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {
size_t Number(const char* arg) {
    size_t result = 0;
    const std::string text(arg);
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) return 0;
    return result;
}
struct TemporaryDirectory {
    std::filesystem::path path;
    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }
};
}

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "usage: dedup_store_benchmark DIRECTORY BATCH_SIZE UPDATES SOURCES\n";
        return 2;
    }
    const size_t batch_size = Number(argv[2]);
    const size_t updates = Number(argv[3]);
    const size_t sources = Number(argv[4]);
    if (batch_size == 0 || batch_size > 65536 || updates == 0 ||
        updates > 10000000 || sources == 0 || sources > 65536) return 2;
    std::string pattern = (std::filesystem::path(argv[1]) / "mino-dedup-XXXXXX").string();
    if (::mkdtemp(pattern.data()) == nullptr) return 1;
    TemporaryDirectory directory{pattern};
    const auto path = directory.path / "dedup.snap";
    auto store = mino::bridge::DedupStore::Open({.path = path.string(), .max_sources = sources});
    if (!store.ok()) return 1;
    std::vector<double> latency_us;
    std::vector<mino::bridge::DedupResumeEntry> batch;
    batch.reserve(batch_size);
    const auto begin = std::chrono::steady_clock::now();
    for (size_t i = 0; i < updates;) {
        batch.clear();
        for (size_t j = 0; j < batch_size && i < updates; ++j, ++i) {
            batch.push_back({{1, i % sources + 1, 1}, i / sources + 1});
        }
        const auto start = std::chrono::steady_clock::now();
        const auto status = (*store)->RecordAcceptedBatch(batch);
        if (!status.ok()) {
            std::cerr << status.ToString() << '\n';
            return 1;
        }
        latency_us.push_back(std::chrono::duration<double, std::micro>(
            std::chrono::steady_clock::now() - start).count());
    }
    const double seconds = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - begin).count();
    const auto commits = (*store)->persistence_count();
    store->reset();
    auto reopened = mino::bridge::DedupStore::Open({.path = path.string(), .max_sources = sources});
    if (!reopened.ok()) return 1;
    auto snapshot = (*reopened)->Load();
    if (!snapshot.ok() || snapshot->size() != std::min(updates, sources)) return 1;
    for (const auto& entry : *snapshot) {
        const uint64_t expected = (updates - entry.source.publisher_id) / sources + 1;
        if (entry.highest_contiguous_sequence != expected) return 1;
    }
    std::sort(latency_us.begin(), latency_us.end());
    const size_t p99 = (latency_us.size() * 99 + 99) / 100 - 1;
    std::cout << "{\"batch_size\":" << batch_size << ",\"updates\":" << updates
              << ",\"sources\":" << sources << ",\"durable_commits\":"
              << commits << ",\"updates_per_second\":" << updates / seconds
              << ",\"commit_p99_us\":" << latency_us[p99] << ",\"reopen_verified\":true}\n";
}
