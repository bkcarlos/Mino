// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/region_id_allocator.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mino/common/status.h"

namespace mino::region_internal {
namespace {

class RegionIdAllocatorTest : public ::testing::Test {
protected:
    std::string HwmPath(const char* tag) {
        static std::atomic<uint32_t> sequence{0};
        std::string pattern =
            "/tmp/mino-rid-test-" + std::to_string(::getpid()) + "-" +
            std::to_string(sequence.fetch_add(1)) + "-XXXXXX";
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        char* directory = ::mkdtemp(writable.data());
        EXPECT_NE(directory, nullptr);
        if (directory == nullptr) return {};
        roots_.emplace_back(directory);
        return (roots_.back() / (std::string("region-id-") + tag)).string();
    }

    std::string LegacyName(const char* tag) {
        static std::atomic<uint32_t> sequence{0};
        std::string name = "/mino_rid_test_" + std::to_string(::getpid()) +
                           "_" + std::to_string(sequence.fetch_add(1)) + tag;
        legacy_names_.push_back(name);
        return name;
    }

    void TearDown() override {
        for (const std::string& name : legacy_names_) {
            (void)::shm_unlink(name.c_str());
        }
        for (const std::filesystem::path& root : roots_) {
            std::error_code error;
            std::filesystem::remove_all(root, error);
        }
    }

private:
    std::vector<std::filesystem::path> roots_;
    std::vector<std::string> legacy_names_;
};

TEST_F(RegionIdAllocatorTest, ConcurrentFirstInitializationAllocatesUniqueIds) {
    constexpr size_t kThreadCount = 64;
    const RegionIdAllocatorOptions options{
        .hwm_path = HwmPath("concurrent"),
        .legacy_shm_name = "",
    };
    std::atomic<bool> start{false};
    std::vector<uint32_t> ids(kThreadCount, 0);
    std::vector<Status> statuses(
        kThreadCount, Status::Error(StatusCode::kInternal, "not run"));
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            auto allocated = AllocateRegionId(options);
            if (allocated.ok()) {
                ids[i] = *allocated;
                statuses[i] = Status::Ok();
            } else {
                statuses[i] = allocated.status();
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (std::thread& thread : threads) thread.join();

    for (const Status& status : statuses) {
        EXPECT_TRUE(status.ok()) << status.ToString();
    }
    std::sort(ids.begin(), ids.end());
    for (size_t i = 0; i < ids.size(); ++i) {
        EXPECT_EQ(ids[i], i + 1);
        EXPECT_NE(ids[i], 0u);
    }
}

TEST_F(RegionIdAllocatorTest, ForkedProcessesShareDurableHighWaterMark) {
    constexpr size_t kProcessCount = 16;
    const RegionIdAllocatorOptions options{
        .hwm_path = HwmPath("fork"),
        .legacy_shm_name = "",
    };
    int values[2];
    ASSERT_EQ(::pipe(values), 0);
    std::vector<pid_t> children;
    for (size_t i = 0; i < kProcessCount; ++i) {
        const pid_t child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            (void)::close(values[0]);
            auto allocated = AllocateRegionId(options);
            const uint32_t value = allocated.ok() ? *allocated : 0;
            const ssize_t written = ::write(values[1], &value, sizeof(value));
            (void)::close(values[1]);
            ::_exit(written == static_cast<ssize_t>(sizeof(value)) && value != 0
                        ? 0
                        : 1);
        }
        children.push_back(child);
    }
    ASSERT_EQ(::close(values[1]), 0);
    std::vector<uint32_t> ids;
    ids.reserve(kProcessCount);
    for (size_t i = 0; i < kProcessCount; ++i) {
        uint32_t value = 0;
        size_t consumed = 0;
        while (consumed < sizeof(value)) {
            const ssize_t count = ::read(
                values[0], reinterpret_cast<char*>(&value) + consumed,
                sizeof(value) - consumed);
            ASSERT_GT(count, 0);
            consumed += static_cast<size_t>(count);
        }
        ids.push_back(value);
    }
    ASSERT_EQ(::close(values[0]), 0);
    for (pid_t child : children) {
        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFEXITED(status));
        EXPECT_EQ(WEXITSTATUS(status), 0);
    }
    std::sort(ids.begin(), ids.end());
    for (size_t i = 0; i < ids.size(); ++i) EXPECT_EQ(ids[i], i + 1);
}

TEST_F(RegionIdAllocatorTest, PersistsAcrossAllocatorReconstruction) {
    const std::string path = HwmPath("reopen");
    RegionIdAllocatorOptions options{.hwm_path = path, .legacy_shm_name = ""};
    auto first = AllocateRegionId(options);
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_EQ(*first, 1u);

    RegionIdAllocatorOptions reconstructed{
        .hwm_path = path,
        .legacy_shm_name = "",
        .first_id = 999,
    };
    auto second = AllocateRegionId(reconstructed);
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(*second, 2u);
}

TEST_F(RegionIdAllocatorTest, AllocatesUint32MaxOnceThenReportsExhaustion) {
    RegionIdAllocatorOptions options{
        .hwm_path = HwmPath("overflow"),
        .legacy_shm_name = "",
        .first_id = std::numeric_limits<uint32_t>::max(),
    };

    auto last = AllocateRegionId(options);
    ASSERT_TRUE(last.ok()) << last.status().ToString();
    EXPECT_EQ(*last, std::numeric_limits<uint32_t>::max());

    auto exhausted = AllocateRegionId(options);
    ASSERT_FALSE(exhausted.ok());
    EXPECT_EQ(exhausted.status().code(), StatusCode::kResourceExhausted);
    EXPECT_NE(exhausted.status().ToString().find("UINT32_MAX"),
              std::string::npos);

    auto still_exhausted = AllocateRegionId(options);
    ASSERT_FALSE(still_exhausted.ok());
    EXPECT_EQ(still_exhausted.status().code(), StatusCode::kResourceExhausted);
}

TEST_F(RegionIdAllocatorTest, MigratesLegacyPosixHighWaterMarkAndUnlinksIt) {
    const std::string name = LegacyName("_legacy");
    const int fd = ::shm_open(name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    ASSERT_GE(fd, 0);
    const uint32_t legacy_next_id = 41;
    ASSERT_EQ(::pwrite(fd, &legacy_next_id, sizeof(legacy_next_id), 0),
              static_cast<ssize_t>(sizeof(legacy_next_id)));
    ASSERT_EQ(::close(fd), 0);

    RegionIdAllocatorOptions options{
        .hwm_path = HwmPath("legacy-migration"),
        .legacy_shm_name = name,
    };
    auto migrated = AllocateRegionId(options);
    ASSERT_TRUE(migrated.ok()) << migrated.status().ToString();
    EXPECT_EQ(*migrated, legacy_next_id);

    errno = 0;
    EXPECT_EQ(::shm_open(name.c_str(), O_RDWR, 0600), -1);
    EXPECT_EQ(errno, ENOENT);
    auto following = AllocateRegionId(options);
    ASSERT_TRUE(following.ok()) << following.status().ToString();
    EXPECT_EQ(*following, legacy_next_id + 1);
}

TEST_F(RegionIdAllocatorTest, MigratesLegacyUint32DurableFile) {
    const std::string path = HwmPath("legacy-file");
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    ASSERT_GE(fd, 0);
    const uint32_t legacy_next_id = 77;
    ASSERT_EQ(::pwrite(fd, &legacy_next_id, sizeof(legacy_next_id), 0),
              static_cast<ssize_t>(sizeof(legacy_next_id)));
    ASSERT_EQ(::fsync(fd), 0);
    ASSERT_EQ(::close(fd), 0);

    auto migrated = AllocateRegionId(
        {.hwm_path = path, .legacy_shm_name = ""});
    ASSERT_TRUE(migrated.ok()) << migrated.status().ToString();
    EXPECT_EQ(*migrated, legacy_next_id);
    auto following = AllocateRegionId(
        {.hwm_path = path, .legacy_shm_name = ""});
    ASSERT_TRUE(following.ok()) << following.status().ToString();
    EXPECT_EQ(*following, legacy_next_id + 1);
}

TEST_F(RegionIdAllocatorTest, RejectsReservedZeroInitialId) {
    RegionIdAllocatorOptions options{
        .hwm_path = HwmPath("zero-initial"),
        .legacy_shm_name = "",
        .first_id = 0,
    };
    auto allocated = AllocateRegionId(options);
    ASSERT_FALSE(allocated.ok());
    EXPECT_EQ(allocated.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(RegionIdAllocatorTest, RejectsWrappedZeroWithoutReinitializing) {
    const std::string path = HwmPath("zero-existing");
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    ASSERT_GE(fd, 0);
    const uint64_t wrapped_next_id = 0;
    ASSERT_EQ(::pwrite(fd, &wrapped_next_id, sizeof(wrapped_next_id), 0),
              static_cast<ssize_t>(sizeof(wrapped_next_id)));
    ASSERT_EQ(::fsync(fd), 0);
    ASSERT_EQ(::close(fd), 0);

    RegionIdAllocatorOptions options{.hwm_path = path, .legacy_shm_name = ""};
    auto allocated = AllocateRegionId(options);
    ASSERT_FALSE(allocated.ok());
    EXPECT_EQ(allocated.status().code(), StatusCode::kCorruption);
    EXPECT_NE(allocated.status().ToString().find("reserved ID 0"),
              std::string::npos);

    auto still_corrupt = AllocateRegionId(options);
    ASSERT_FALSE(still_corrupt.ok());
    EXPECT_EQ(still_corrupt.status().code(), StatusCode::kCorruption);
}

}  // namespace
}  // namespace mino::region_internal
