// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/recovery_directory.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mino {
namespace {

TEST(RecoveryDirectoryTest, ConcurrentReadersNeverObserveTornSnapshot) {
    alignas(64) std::byte storage[kRecoveryDirectoryMinimumSize];
    std::memset(storage, 0, sizeof(storage));
    ASSERT_TRUE(InitializeRecoveryDirectory(storage, sizeof(storage)).ok());

    RecoveryResourceDescriptor descriptor{
        .resource_id = 7,
        .kind = static_cast<uint32_t>(RecoveryResourceKind::kCentralAllocator),
        .format_version = 1,
        .offset = 64 * 1024,
        .size = 64 * 1024,
    };
    ASSERT_TRUE(PublishRecoveryResource(storage, sizeof(storage), 1024 * 1024,
                                        descriptor)
                    .ok());

    std::atomic<bool> start{false};
    std::atomic<int> writers_running{2};
    std::atomic<uint32_t> failures{0};
    auto resource_writer = [&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (uint64_t i = 1; i <= 500; ++i) {
            RecoveryResourceDescriptor update = descriptor;
            update.generation = i;
            if (!PublishRecoveryResource(storage, sizeof(storage),
                                         1024 * 1024, update)
                     .ok()) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
        writers_running.fetch_sub(1, std::memory_order_release);
    };
    auto reference_writer = [&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (uint32_t i = 1; i <= 500; ++i) {
            const RecoveryObjectReference reference{
                .resource_id = 7,
                .unit_index = i % 32,
                .generation = i,
            };
            if (!PublishRecoveryReferences(
                     storage, sizeof(storage),
                     std::span<const RecoveryObjectReference>(&reference, 1),
                     (i & 1u) != 0)
                     .ok()) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
        writers_running.fetch_sub(1, std::memory_order_release);
    };
    auto reader = [&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        do {
            auto snapshot = ReadRecoveryDirectory(storage, sizeof(storage));
            if (!snapshot.ok()) {
                if (snapshot.status().code() != StatusCode::kUnavailable) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
                continue;
            }
            if (snapshot->sequence == 0 || snapshot->resource_count != 1 ||
                snapshot->resources[0].resource_id != 7 ||
                snapshot->reference_count > 1 ||
                (snapshot->reference_count == 1 &&
                 (snapshot->references[0].resource_id != 7 ||
                  snapshot->references[0].generation == 0))) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        } while (writers_running.load(std::memory_order_acquire) != 0);
    };

    std::thread first_writer(resource_writer);
    std::thread second_writer(reference_writer);
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back(reader);
    }
    start.store(true, std::memory_order_release);
    first_writer.join();
    second_writer.join();
    for (std::thread& thread : readers) {
        thread.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_acquire), 0u);
    auto final_snapshot = ReadRecoveryDirectory(storage, sizeof(storage));
    ASSERT_TRUE(final_snapshot.ok()) << final_snapshot.status().ToString();
    EXPECT_EQ(final_snapshot->resource_count, 1u);
    EXPECT_EQ(final_snapshot->resources[0].resource_id, 7u);
}

}  // namespace
}  // namespace mino
