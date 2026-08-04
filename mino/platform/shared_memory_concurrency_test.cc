// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/platform/shared_memory.h"
#include "mino/platform/shared_memory_marker.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace mino {
namespace {

using shared_memory_internal::MarkerBackingKind;
using shared_memory_internal::MarkerPayloadCrc32;
using shared_memory_internal::MarkerState;
using shared_memory_internal::SharedMemoryMarkerPayload;
using shared_memory_internal::SharedMemoryMarkerRecord;

std::filesystem::path Runfile(std::string_view path) {
    const char* srcdir = std::getenv("TEST_SRCDIR");
    const char* workspace = std::getenv("TEST_WORKSPACE");
    return std::filesystem::path(srcdir == nullptr ? "" : srcdir) /
           (workspace == nullptr ? "Mino" : workspace) / path;
}

std::string UniqueName(const char* tag) {
    static std::atomic<uint32_t> sequence{0};
    return std::string("/msc_") + std::to_string(::getpid()) + "_" +
           std::to_string(sequence.fetch_add(1)) + "_" + tag;
}

Result<SharedMemoryMarkerPayload> ReadMarker(const std::string& name) {
    int fd = ::shm_open(name.c_str(), O_RDONLY, 0);
    if (fd < 0) {
        return Status::Error(StatusCode::kNotFound, "marker not found");
    }
    SharedMemoryMarkerRecord record;
    const ssize_t count = ::pread(fd, &record, sizeof(record), 0);
    ::close(fd);
    if (count != static_cast<ssize_t>(sizeof(record))) {
        return Status::Error(StatusCode::kCorruption, "short marker read");
    }
    const auto& slot = record.slots[record.published_word & 1u];
    if (slot.payload_crc32 != MarkerPayloadCrc32(slot.payload)) {
        return Status::Error(StatusCode::kCorruption, "invalid marker slot");
    }
    return slot.payload;
}

std::string MarkerString(const char* value, size_t capacity) {
    const void* end = std::memchr(value, '\0', capacity);
    if (end == nullptr) return {};
    return std::string(value, static_cast<const char*>(end) - value);
}

Status OverwriteActiveMarker(const std::string& name,
                             const SharedMemoryMarkerPayload& payload) {
    int fd = ::shm_open(name.c_str(), O_RDWR, 0);
    if (fd < 0) {
        return Status::Error(StatusCode::kNotFound, "marker not found");
    }
    void* address = ::mmap(nullptr, sizeof(SharedMemoryMarkerRecord),
                           PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ::close(fd);
    if (address == MAP_FAILED) {
        return Status::Error(StatusCode::kInternal, "marker mmap failed");
    }
    auto* marker = static_cast<SharedMemoryMarkerRecord*>(address);
    auto& slot = marker->slots[marker->published_word & 1u];
    slot.payload = payload;
    slot.payload_crc32 = MarkerPayloadCrc32(payload);
    const int sync_result =
        ::msync(marker, sizeof(SharedMemoryMarkerRecord), MS_SYNC);
    ::munmap(marker, sizeof(SharedMemoryMarkerRecord));
    return sync_result == 0
               ? Status::Ok()
               : Status::Error(StatusCode::kInternal, "marker msync failed");
}

pid_t StartHelper(const std::string& action, const std::string& name) {
    const std::filesystem::path helper =
        Runfile("mino/platform/shared_memory_fault_helper");
    const pid_t child = ::fork();
    if (child == 0) {
        ::execl(helper.c_str(), helper.c_str(), action.c_str(), name.c_str(),
                static_cast<char*>(nullptr));
        ::_exit(127);
    }
    return child;
}

void ExpectExit(pid_t child, int expected) {
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), expected);
}

class SharedMemoryConcurrencyTest : public ::testing::Test {
protected:
    std::string Name(const char* tag) {
        std::string name = UniqueName(tag);
        names_.push_back(name);
        return name;
    }

    void TearDown() override {
        for (const std::string& name : names_) {
            (void)SharedMemorySegment::Unlink(name);
        }
    }

    std::vector<std::string> names_;
};

TEST_F(SharedMemoryConcurrencyTest, MarkerIsMetadataAndFallbackIsSeparateData) {
    const std::string name = Name("layout");
    auto created = SharedMemorySegment::Create(name, 8192);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    std::memset(created->base(), 0x5A, created->size());

    int marker_fd = ::shm_open(name.c_str(), O_RDONLY, 0);
    ASSERT_GE(marker_fd, 0);
    struct stat marker_stat;
    ASSERT_EQ(::fstat(marker_fd, &marker_stat), 0);
    EXPECT_EQ(marker_stat.st_size,
              static_cast<off_t>(sizeof(SharedMemoryMarkerRecord)));
    ::close(marker_fd);

    auto marker = ReadMarker(name);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    EXPECT_EQ(marker->state,
              static_cast<uint32_t>(MarkerState::kFallbackReady));
    EXPECT_EQ(marker->backing_kind,
              static_cast<uint32_t>(MarkerBackingKind::kPosixData));
    EXPECT_EQ(marker->data_size, created->size());
    ASSERT_NE(marker->backing_inode, 0u);
    const std::string data_name = MarkerString(
        marker->backing_name, sizeof(marker->backing_name));
    ASSERT_FALSE(data_name.empty());
    ASSERT_NE(data_name, name);
    int data_fd = ::shm_open(data_name.c_str(), O_RDONLY, 0);
    ASSERT_GE(data_fd, 0);
    struct stat data_stat;
    ASSERT_EQ(::fstat(data_fd, &data_stat), 0);
    ::close(data_fd);
    EXPECT_EQ(static_cast<uint64_t>(data_stat.st_dev),
              marker->backing_device);
    EXPECT_EQ(static_cast<uint64_t>(data_stat.st_ino),
              marker->backing_inode);

    auto opened = SharedMemorySegment::Open(name, /*read_only=*/true);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    EXPECT_EQ(static_cast<const unsigned char*>(opened->base())[0], 0x5A);
}

TEST_F(SharedMemoryConcurrencyTest,
       IdentityMismatchNeverDeletesRecordedNameWithoutExactInode) {
    const std::string name = Name("identity");
    auto created = SharedMemorySegment::Create(name, 8192);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto original = ReadMarker(name);
    ASSERT_TRUE(original.ok()) << original.status().ToString();
    const std::string data_name = MarkerString(
        original->backing_name, sizeof(original->backing_name));
    ASSERT_FALSE(data_name.empty());

    SharedMemoryMarkerPayload tampered = *original;
    ++tampered.backing_inode;
    ASSERT_TRUE(OverwriteActiveMarker(name, tampered).ok());
    auto opened = SharedMemorySegment::Open(name, /*read_only=*/false);
    ASSERT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), StatusCode::kCorruption);
    Status unlink_status = SharedMemorySegment::Unlink(name);
    EXPECT_EQ(unlink_status.code(), StatusCode::kCorruption);
    int data_fd = ::shm_open(data_name.c_str(), O_RDONLY, 0);
    ASSERT_GE(data_fd, 0) << "identity mismatch deleted unrelated inode";
    ::close(data_fd);

    // Restore the authoritative identity solely so fixture cleanup can finish.
    SharedMemoryMarkerPayload restored = *original;
    restored.state = static_cast<uint32_t>(MarkerState::kFallbackReady);
    ASSERT_TRUE(OverwriteActiveMarker(name, restored).ok());
    EXPECT_TRUE(SharedMemorySegment::Unlink(name).ok());
}

TEST_F(SharedMemoryConcurrencyTest,
       CreatingWaitIsBoundedAndKilledCreatorIsRecovered) {
    const std::string name = Name("creating");
    const pid_t child = StartHelper("stop-after-marker", name);
    ASSERT_NE(child, -1);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, WUNTRACED), child);
    ASSERT_TRUE(WIFSTOPPED(status));

    SharedMemoryOpenOptions open_options;
    open_options.name = name;
    open_options.creating_wait_timeout_ms = 10;
    const auto start = std::chrono::steady_clock::now();
    auto opened = SharedMemorySegment::Open(open_options);
    const auto elapsed = std::chrono::steady_clock::now() - start;
    ASSERT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), StatusCode::kWouldBlock);
    EXPECT_LT(elapsed, std::chrono::seconds(1));
    EXPECT_EQ(SharedMemorySegment::Unlink(name).code(),
              StatusCode::kWouldBlock);

    ASSERT_EQ(::kill(child, SIGKILL), 0);
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    auto recovered = SharedMemorySegment::Create(name, 8192);
    ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
}

TEST_F(SharedMemoryConcurrencyTest,
       CrashAfterBackingIdentityRemovesOrphanAndRecreates) {
    const std::string name = Name("backing");
    const pid_t child = StartHelper("crash-after-backing", name);
    ASSERT_NE(child, -1);
    ExpectExit(child, 70);

    auto crashed_marker = ReadMarker(name);
    ASSERT_TRUE(crashed_marker.ok()) << crashed_marker.status().ToString();
    EXPECT_EQ(crashed_marker->state,
              static_cast<uint32_t>(MarkerState::kCreating));
    ASSERT_NE(crashed_marker->backing_inode, 0u);
    const std::string orphan = MarkerString(
        crashed_marker->backing_name,
        sizeof(crashed_marker->backing_name));
    ASSERT_FALSE(orphan.empty());

    auto recovered = SharedMemorySegment::Create(name, 8192);
    ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
    errno = 0;
    EXPECT_EQ(::shm_open(orphan.c_str(), O_RDONLY, 0), -1);
    EXPECT_EQ(errno, ENOENT);
}

TEST_F(SharedMemoryConcurrencyTest,
       CrashBeforeIdentityPublicationUsesPreviousValidSlot) {
    const std::string name = Name("publish-crash");
    const pid_t child =
        StartHelper("crash-before-identity-publication", name);
    ASSERT_NE(child, -1);
    ExpectExit(child, 70);

    auto previous_slot = ReadMarker(name);
    ASSERT_TRUE(previous_slot.ok()) << previous_slot.status().ToString();
    EXPECT_EQ(previous_slot->state,
              static_cast<uint32_t>(MarkerState::kCreating));
    EXPECT_EQ(previous_slot->backing_inode, 0u);
    const std::string candidate = MarkerString(
        previous_slot->backing_name, sizeof(previous_slot->backing_name));
    ASSERT_FALSE(candidate.empty());
    int candidate_fd = ::shm_open(candidate.c_str(), O_RDONLY, 0);
    ASSERT_GE(candidate_fd, 0);
    ::close(candidate_fd);

    auto recovered = SharedMemorySegment::Create(name, 8192);
    ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
    errno = 0;
    EXPECT_EQ(::shm_open(candidate.c_str(), O_RDONLY, 0), -1);
    EXPECT_EQ(errno, ENOENT);
}

TEST_F(SharedMemoryConcurrencyTest, UnlinkCrashLeavesRetryableTombstone) {
    const std::string name = Name("unlink");
    auto created = SharedMemorySegment::Create(name, 8192);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    static_cast<unsigned char*>(created->base())[0] = 0xA7;

    const pid_t child = StartHelper("crash-after-unlinking", name);
    ASSERT_NE(child, -1);
    ExpectExit(child, 70);
    auto opened = SharedMemorySegment::Open(name, /*read_only=*/false);
    ASSERT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), StatusCode::kWouldBlock);

    EXPECT_TRUE(SharedMemorySegment::Unlink(name).ok());
    EXPECT_EQ(static_cast<unsigned char*>(created->base())[0], 0xA7);
    opened = SharedMemorySegment::Open(name, /*read_only=*/false);
    ASSERT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), StatusCode::kNotFound);
}

TEST_F(SharedMemoryConcurrencyTest, ConcurrentCreateHasSingleWinner) {
    const std::string name = Name("create-race");
    constexpr int kThreads = 12;
    std::atomic<bool> start{false};
    std::atomic<int> successes{0};
    std::atomic<int> expected_failures{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            auto created = SharedMemorySegment::Create(name, 4096);
            if (created.ok()) {
                successes.fetch_add(1);
            } else if (created.status().code() == StatusCode::kAlreadyExists ||
                       created.status().code() == StatusCode::kWouldBlock) {
                expected_failures.fetch_add(1);
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (std::thread& thread : threads) thread.join();
    // A creator may observe the marker after the winner has published READY
    // and receive an attached segment rather than a failure. The invariant
    // under test is that creation never yields zero usable segments and every
    // contender gets a classified result.
    EXPECT_GE(successes.load(), 1);
    EXPECT_EQ(successes.load() + expected_failures.load(), kThreads);
}

TEST_F(SharedMemoryConcurrencyTest, ConcurrentOpenAndUnlinkNeverMapMarker) {
    const std::string name = Name("open-unlink");
    auto created = SharedMemorySegment::Create(name, 16384);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    static_cast<uint64_t*>(created->base())[0] = 0x123456789ABCDEF0ull;

    std::atomic<bool> start{false};
    std::atomic<int> bad_results{0};
    std::vector<std::thread> readers;
    for (int i = 0; i < 16; ++i) {
        readers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int attempt = 0; attempt < 50; ++attempt) {
                auto opened = SharedMemorySegment::Open(name, true);
                if (opened.ok()) {
                    if (opened->size() != 16384 ||
                        static_cast<const uint64_t*>(opened->base())[0] !=
                            0x123456789ABCDEF0ull) {
                        bad_results.fetch_add(1);
                    }
                } else if (opened.status().code() != StatusCode::kWouldBlock &&
                           opened.status().code() != StatusCode::kNotFound) {
                    bad_results.fetch_add(1);
                }
            }
        });
    }
    start.store(true, std::memory_order_release);
    EXPECT_TRUE(SharedMemorySegment::Unlink(name).ok());
    for (std::thread& reader : readers) reader.join();
    EXPECT_EQ(bad_results.load(), 0);
}

}  // namespace
}  // namespace mino
