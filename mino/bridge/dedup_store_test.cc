// Copyright 2026 The Mino Authors

#include "mino/bridge/dedup_store.h"
#include "mino/bridge/crc32c.h"

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <limits>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>

#include <gtest/gtest.h>
#include <unistd.h>

namespace mino::bridge {
namespace {

constexpr SourceIdentity kSource{1, 2, 3};
constexpr SourceIdentity kOther{4, 5, 6};

class DedupStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        const char* tmp = std::getenv("TEST_TMPDIR");
        ASSERT_NE(tmp, nullptr);
        dir_ = std::filesystem::path(tmp) /
               ("dedup_store_" + std::to_string(::getpid()) + "_" +
                std::to_string(++sequence_));
        std::error_code error;
        std::filesystem::create_directories(dir_, error);
        ASSERT_FALSE(error) << error.message();
        path_ = (dir_ / "dedup.snap").string();
    }

    void TearDown() override {
        std::error_code error;
        std::filesystem::remove_all(dir_, error);
    }

    DedupStoreOptions Options(size_t max_sources = 8) const {
        return DedupStoreOptions{.path = path_, .max_sources = max_sources};
    }

    static uint32_t sequence_;
    std::filesystem::path dir_;
    std::string path_;
};

uint32_t DedupStoreTest::sequence_ = 0;

TEST_F(DedupStoreTest, RetirementUpgradesLegacySnapshotAndReclaimsEpochSlots) {
    auto store = DedupStore::Open(Options(4));
    ASSERT_TRUE(store.ok());
    const SourceIdentity first{1, 2, 1}, second{1, 2, 2}, third{1, 2, 3};
    ASSERT_TRUE((*store)->RecordAcceptedBatch(std::vector<DedupResumeEntry>{
        {first, 11}, {second, 22}, {third, 33}, {kOther, 44}}).ok());
    {
        std::ifstream input(path_, std::ios::binary);
        input.seekg(11);
        EXPECT_EQ(input.get(), 1); // Existing stores remain v1 before retirement.
    }
    ASSERT_TRUE((*store)->RetireEpochsThrough(second).ok());
    EXPECT_EQ((*store)->size(), 3u); // two HWMs plus one boundary
    EXPECT_EQ((*store)->stats().retired_publishers, 1u);
    EXPECT_EQ((*store)->stats().sources, 3u);
    EXPECT_EQ((*store)->HighestAccepted(first), 0u);
    EXPECT_EQ((*store)->HighestAccepted(third), 33u);
    store->reset();
    store = DedupStore::Open(Options(4));
    ASSERT_TRUE(store.ok());
    EXPECT_TRUE((*store)->IsRetired(first));
    EXPECT_TRUE((*store)->IsRetired(second));
    EXPECT_FALSE((*store)->IsRetired(third));
    EXPECT_FALSE((*store)->IsRetired({9, 2, 1}));
    EXPECT_FALSE((*store)->IsRetired({1, 9, 1}));
    auto snapshot = (*store)->Load();
    ASSERT_TRUE(snapshot.ok());
    ASSERT_EQ(snapshot->size(), 2u); // Boundaries must never become Hello ACKs.
    for (const auto& entry : *snapshot) EXPECT_NE(entry.highest_contiguous_sequence, 0u);
    ASSERT_TRUE((*store)->RecordAccepted({1, 2, 4}, 1).ok());
    EXPECT_EQ((*store)->size(), 4u);
    std::ifstream input(path_, std::ios::binary);
    input.seekg(11);
    EXPECT_EQ(input.get(), 2);
}

TEST_F(DedupStoreTest, RetirementIsMonotonicAndCannotBeResurrectedByOtherWrites) {
    auto store = DedupStore::Open(Options(3));
    ASSERT_TRUE(store.ok());
    ASSERT_TRUE((*store)->RetireEpochsThrough(kSource).ok());
    const auto commits = (*store)->persistence_count();
    EXPECT_TRUE((*store)->RetireEpochsThrough({1, 2, 2}).ok());
    EXPECT_EQ((*store)->persistence_count(), commits);
    EXPECT_EQ((*store)->RecordAccepted(kSource, 1).code(), StatusCode::kPermissionDenied);
    EXPECT_EQ((*store)->RecordAcceptedBatch(std::vector<DedupResumeEntry>{
        {kOther, 8}, {kSource, 1}}).code(), StatusCode::kPermissionDenied);
    EXPECT_EQ((*store)->HighestAccepted(kOther), 0u);
    EXPECT_EQ((*store)->ReplaceAll({{kSource, 1}}).code(), StatusCode::kPermissionDenied);
    ASSERT_TRUE((*store)->ReplaceAll({{kOther, 8}}).ok());
    ASSERT_TRUE((*store)->ReplaceAll({}).ok());
    EXPECT_EQ((*store)->size(), 1u); // ReplaceAll cannot remove boundaries.
    store->reset();
    store = DedupStore::Open(Options(3));
    ASSERT_TRUE(store.ok());
    EXPECT_TRUE((*store)->IsRetired(kSource));
    const SourceIdentity last{1, 2, std::numeric_limits<uint64_t>::max()};
    ASSERT_TRUE((*store)->RetireEpochsThrough(last).ok());
    EXPECT_TRUE((*store)->IsRetired(last));
    EXPECT_EQ((*store)->size(), 1u);
}

TEST_F(DedupStoreTest, RetirementBoundariesShareCapacityAndRejectInvalidIdentity) {
    auto store = DedupStore::Open(Options(1));
    ASSERT_TRUE(store.ok());
    EXPECT_EQ((*store)->RetireEpochsThrough({}).code(), StatusCode::kInvalidArgument);
    ASSERT_TRUE((*store)->RecordAccepted(kSource, 1).ok());
    EXPECT_EQ((*store)->RetireEpochsThrough(kOther).code(), StatusCode::kResourceExhausted);
    EXPECT_FALSE((*store)->IsRetired(kOther));
    ASSERT_TRUE((*store)->RetireEpochsThrough(kSource).ok());
    EXPECT_EQ((*store)->RecordAccepted(kOther, 1).code(), StatusCode::kResourceExhausted);
    EXPECT_EQ((*store)->ReplaceAll({{kOther, 1}}).code(), StatusCode::kResourceExhausted);
    EXPECT_EQ((*store)->size(), 1u);
    EXPECT_EQ((*store)->stats().capacity_rejections, 3u);
}

TEST_F(DedupStoreTest, RejectsContradictoryOrLegacyRetirementSnapshots) {
    // Write CRC-valid semantic corruption, so failure must come from validation.
    auto write_snapshot = [&](uint32_t version, const std::vector<DedupResumeEntry>& rows) {
        std::vector<std::byte> bytes;
        for (char c : std::string("MINODEDU")) bytes.push_back(static_cast<std::byte>(c));
        const auto append = [&](uint64_t value, int width) {
            for (int i = width - 1; i >= 0; --i) bytes.push_back(static_cast<std::byte>(value >> (8 * i)));
        };
        append(version, 4);
        append(rows.size(), 4);
        for (const auto& row : rows) {
            append(row.source.node_id, 8); append(row.source.publisher_id, 8);
            append(row.source.publisher_epoch, 8); append(row.highest_contiguous_sequence, 8);
        }
        append(Crc32c(bytes), 4);
        std::ofstream output(path_, std::ios::binary | std::ios::trunc);
        output.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    };
    for (const auto& rows : std::vector<std::vector<DedupResumeEntry>>{
        {{{1, 2, 2}, 8}, {{1, 2, 3}, 0}}, // live HWM covered by a boundary
        {{{1, 2, 2}, 0}, {{1, 2, 3}, 0}}, // duplicate publisher boundaries
        {{{1, 2, 0}, 0}},               // zero retirement epoch
        {{{1, 2, 4}, 1}, {{1, 2, 3}, 0}} // unordered records
    }) {
        write_snapshot(2, rows);
        EXPECT_EQ(DedupStore::Open(Options()).status().code(), StatusCode::kCorruption);
    }
    write_snapshot(1, {{kSource, 0}}); // v1 has no zero-HWM retirement record.
    EXPECT_EQ(DedupStore::Open(Options()).status().code(), StatusCode::kCorruption);
    write_snapshot(2, {{kSource, 0}});
    auto store = DedupStore::Open(Options());
    ASSERT_TRUE(store.ok());
    EXPECT_TRUE((*store)->IsRetired(kSource));
}

TEST_F(DedupStoreTest, BatchCoalescesUpdatesInOneDurableCommit) {
    auto store = DedupStore::Open(Options());
    ASSERT_TRUE(store.ok());
    const std::vector<DedupResumeEntry> updates = {
        {kSource, 1}, {kOther, 3}, {kSource, 7}, {kSource, 5}};
    ASSERT_TRUE((*store)->RecordAcceptedBatch(updates).ok());
    EXPECT_EQ((*store)->persistence_count(), 1u);
    EXPECT_TRUE((*store)->RecordAcceptedBatch(updates).ok());
    EXPECT_EQ((*store)->persistence_count(), 1u);
    store->reset();
    auto reopened = DedupStore::Open(Options());
    ASSERT_TRUE(reopened.ok());
    auto snapshot = (*reopened)->Load();
    ASSERT_TRUE(snapshot.ok());
    ASSERT_EQ(snapshot->size(), 2u);
    EXPECT_EQ(snapshot->front().highest_contiguous_sequence, 7u);
}

TEST_F(DedupStoreTest, FailedBatchDoesNotAdvanceAnySource) {
    auto store = DedupStore::Open(Options(2));
    ASSERT_TRUE(store.ok());
    ASSERT_TRUE((*store)->RecordAccepted(kSource, 1).ok());
    const std::vector<DedupResumeEntry> updates = {{kSource, 7}, {kOther, 3}};
    ASSERT_TRUE(std::filesystem::remove(path_));
    ASSERT_TRUE(std::filesystem::create_directory(path_));
    EXPECT_FALSE((*store)->RecordAcceptedBatch(updates).ok());
    auto snapshot = (*store)->Load();
    ASSERT_TRUE(snapshot.ok());
    ASSERT_EQ(snapshot->size(), 1u);
    EXPECT_EQ(snapshot->front().highest_contiguous_sequence, 1u);
    ASSERT_TRUE(std::filesystem::remove(path_));
    EXPECT_TRUE((*store)->RecordAcceptedBatch(updates).ok());
    const std::vector<DedupResumeEntry> overflow = {{kSource, 9}, {{7, 8, 9}, 1}};
    EXPECT_EQ((*store)->RecordAcceptedBatch(overflow).code(),
              StatusCode::kResourceExhausted);
    snapshot = (*store)->Load();
    ASSERT_TRUE(snapshot.ok());
    EXPECT_EQ(snapshot->front().highest_contiguous_sequence, 7u);
    const auto stats = (*store)->stats();
    EXPECT_EQ(stats.sources, 2u);
    EXPECT_EQ(stats.capacity, 2u);
    EXPECT_EQ(stats.persistence_count, 2u);
    EXPECT_EQ(stats.persistence_failures, 1u);
    EXPECT_EQ(stats.capacity_rejections, 1u);
    EXPECT_GE(stats.persistence_total_ns, stats.persistence_max_ns);
}

TEST_F(DedupStoreTest, SharedStoreSerializesConcurrentLaneUpdates) {
    auto store = DedupStore::Open(Options());
    ASSERT_TRUE(store.ok());
    auto writer = [&](SourceIdentity source) {
        for (uint64_t seq = 1; seq <= 16; ++seq) {
            EXPECT_TRUE((*store)->RecordAccepted(source, seq).ok());
            EXPECT_TRUE((*store)->Load().ok());
        }
    };
    std::thread first(writer, kSource);
    std::thread second(writer, kOther);
    first.join();
    second.join();
    store->reset();
    auto reopened = DedupStore::Open(Options());
    ASSERT_TRUE(reopened.ok());
    auto snapshot = (*reopened)->Load();
    ASSERT_TRUE(snapshot.ok());
    ASSERT_EQ(snapshot->size(), 2u);
    for (const auto& entry : *snapshot) EXPECT_EQ(entry.highest_contiguous_sequence, 16u);
}

TEST_F(DedupStoreTest, OpenMissingIsEmptyAndPersistsAcrossReopen) {
    {
        auto opened = DedupStore::Open(Options());
        ASSERT_TRUE(opened.ok()) << opened.status().ToString();
        EXPECT_EQ((*opened)->size(), 0u);
        ASSERT_TRUE((*opened)->RecordAccepted(kSource, 7).ok());
        ASSERT_TRUE((*opened)->RecordAccepted(kOther, 3).ok());
        ASSERT_TRUE((*opened)->RecordAccepted(kSource, 5).ok());  // no-op
        EXPECT_EQ((*opened)->size(), 2u);
    }

    auto reopened = DedupStore::Open(Options());
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    auto snapshot = (*reopened)->Load();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    ASSERT_EQ(snapshot->size(), 2u);
    EXPECT_EQ((*snapshot)[0].source, kSource);
    EXPECT_EQ((*snapshot)[0].highest_contiguous_sequence, 7u);
    EXPECT_EQ((*snapshot)[1].source, kOther);
    EXPECT_EQ((*snapshot)[1].highest_contiguous_sequence, 3u);
}

TEST_F(DedupStoreTest, RestartedWindowRejectsPreviouslySeenSequences) {
    {
        auto store = DedupStore::Open(Options());
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 11).ok());
    }

    auto store = DedupStore::Open(Options());
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    auto window = DedupWindow::Create(DedupWindowOptions{
        .max_sources = 4,
        .max_bytes = 4096,
        .max_sequence_distance = 64,
        .max_source_age_ns = 1'000'000'000ull,
    });
    ASSERT_TRUE(window.ok()) << window.status().ToString();
    (*window)->BeginSession(/*peer_session_epoch=*/42, /*now_ns=*/0);
    ASSERT_TRUE(
        SeedDedupWindowFromStore(window->get(), store->get(), 42, 0).ok());

    auto duplicate = (*window)->Check(42, kSource, 11, 1);
    ASSERT_TRUE(duplicate.ok()) << duplicate.status().ToString();
    EXPECT_EQ(duplicate->decision, DedupDecision::kDuplicateAccepted);
    EXPECT_EQ(duplicate->highest_contiguous_sequence, 11u);

    auto next = (*window)->Check(42, kSource, 12, 2);
    ASSERT_TRUE(next.ok()) << next.status().ToString();
    EXPECT_EQ(next->decision, DedupDecision::kAccept);
}

TEST_F(DedupStoreTest, CorruptSnapshotFailsClosed) {
    {
        auto store = DedupStore::Open(Options());
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 9).ok());
    }

    {
        std::fstream file(path_, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(file.good());
        file.seekp(0);
        const char bogus[] = {'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X'};
        file.write(bogus, sizeof(bogus));
        ASSERT_TRUE(file.good());
    }

    auto opened = DedupStore::Open(Options());
    ASSERT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), StatusCode::kCorruption);

    // Flip the trailing CRC byte instead of the magic.
    {
        const std::string other = (dir_ / "other.snap").string();
        auto store = DedupStore::Open(DedupStoreOptions{
            .path = other,
            .max_sources = 8,
        });
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 2).ok());
        store->reset();

        std::fstream file(other, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(file.good());
        file.seekg(0, std::ios::end);
        const auto size = file.tellg();
        ASSERT_GT(size, 4);
        file.seekg(static_cast<std::streamoff>(size) - 4);
        char crc[4] = {};
        file.read(crc, 4);
        ASSERT_TRUE(file.good());
        crc[0] = static_cast<char>(static_cast<unsigned char>(crc[0]) ^ 0xffu);
        file.clear();
        file.seekp(static_cast<std::streamoff>(size) - 4);
        file.write(crc, 4);
        file.flush();
        ASSERT_TRUE(file.good());
        file.close();

        auto corrupt = DedupStore::Open(DedupStoreOptions{
            .path = other,
            .max_sources = 8,
        });
        ASSERT_FALSE(corrupt.ok());
        EXPECT_EQ(corrupt.status().code(), StatusCode::kCorruption);
    }
}

TEST_F(DedupStoreTest, EmptyFileAndTruncationFailClosed) {
    {
        std::ofstream empty(path_, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(empty.good());
    }
    auto empty_open = DedupStore::Open(Options());
    ASSERT_FALSE(empty_open.ok());
    EXPECT_EQ(empty_open.status().code(), StatusCode::kCorruption);

    {
        auto store = DedupStore::Open(DedupStoreOptions{
            .path = (dir_ / "trunc.snap").string(),
            .max_sources = 8,
        });
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 4).ok());
        const std::string trunc_path = (*store)->path();
        store->reset();
        std::filesystem::resize_file(trunc_path, 8);
    }
    auto trunc_open = DedupStore::Open(DedupStoreOptions{
        .path = (dir_ / "trunc.snap").string(),
        .max_sources = 8,
    });
    ASSERT_FALSE(trunc_open.ok());
    EXPECT_EQ(trunc_open.status().code(), StatusCode::kCorruption);
}

TEST_F(DedupStoreTest, ExclusiveOwnershipSurvivesSnapshotRename) {
    auto owner = DedupStore::Open(Options());
    ASSERT_TRUE(owner.ok());
    for (uint64_t sequence = 1; sequence <= 2; ++sequence) {
        ASSERT_TRUE((*owner)->RecordAccepted(kSource, sequence).ok());
        auto second = DedupStore::Open(Options());
        ASSERT_FALSE(second.ok());
        EXPECT_EQ(second.status().code(), StatusCode::kAlreadyExists);
    }
    // A different process must not truncate the snapshot or reuse the temp path.
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        owner->reset(); // Close the inherited descriptor, not the parent's lock.
        auto second = DedupStore::Open(Options());
        ::_exit(!second.ok() && second.status().code() == StatusCode::kAlreadyExists ? 0 : 1);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
    owner->reset();
    auto next = DedupStore::Open(Options());
    ASSERT_TRUE(next.ok());
    EXPECT_EQ((*next)->HighestAccepted(kSource), 2u);
    EXPECT_EQ((*next)->HighestAccepted(kOther), 0u);
}

TEST_F(DedupStoreTest, ProcessDeathReleasesLockAndPreservesCommittedSnapshot) {
    int ready[2];
    ASSERT_EQ(::pipe(ready), 0);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        (void)::close(ready[0]);
        auto store = DedupStore::Open(Options());
        const char result = store.ok() && (*store)->RecordAccepted(kSource, 9).ok() ? 'y' : 'n';
        ssize_t written;
        do {
            written = ::write(ready[1], &result, 1);
        } while (written < 0 && errno == EINTR);
        if (written != 1) ::_exit(98);
        for (;;) ::pause();
    }
    (void)::close(ready[1]);
    char result = 'n';
    ssize_t count;
    do {
        count = ::read(ready[0], &result, 1);
    } while (count < 0 && errno == EINTR);
    (void)::close(ready[0]);
    EXPECT_EQ(::kill(child, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(result, 'y');
    ASSERT_TRUE(WIFSIGNALED(status));
    auto reopened = DedupStore::Open(Options());
    ASSERT_TRUE(reopened.ok());
    EXPECT_EQ((*reopened)->HighestAccepted(kSource), 9u);
}

class IoFailure final : public DedupStoreTestHooks {
public:
    DedupStoreIoStage stage = DedupStoreIoStage::kWrite;
    bool crash = false;
    bool after_operation = false;
    bool armed = true;
    Status Checkpoint(DedupStoreIoStage current, bool after) const noexcept override {
        if (armed && current == stage && after == after_operation) {
            if (crash) {
                (void)::kill(::getpid(), SIGKILL);
                ::_exit(99);
            }
            return Status::Error(stage == DedupStoreIoStage::kWrite ?
                StatusCode::kResourceExhausted : StatusCode::kInternal,
                "injected write ENOSPC or sync/rename EIO");
        }
        return Status::Ok();
    }
};

TEST_F(DedupStoreTest, FailedRetirementRequiresReopenBeforeAnyFurtherUse) {
    for (const auto stage : {DedupStoreIoStage::kWrite, DedupStoreIoStage::kFileSync,
                            DedupStoreIoStage::kRename, DedupStoreIoStage::kDirectorySync}) {
        IoFailure fault;
        fault.stage = stage;
        fault.armed = false;
        auto options = Options();
        options.path += std::to_string(static_cast<int>(stage));
        options.test_hooks = &fault;
        auto store = DedupStore::Open(options);
        ASSERT_TRUE(store.ok());
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 7).ok());
        fault.armed = true;
        EXPECT_FALSE((*store)->RetireEpochsThrough(kSource).ok());
        EXPECT_EQ((*store)->Load().status().code(), StatusCode::kUnavailable);
        EXPECT_EQ((*store)->RecordAccepted(kOther, 1).code(), StatusCode::kUnavailable);
        EXPECT_EQ((*store)->ReplaceAll({}).code(), StatusCode::kUnavailable);
        EXPECT_EQ((*store)->RetireEpochsThrough(kSource).code(), StatusCode::kUnavailable);
        EXPECT_TRUE((*store)->IsRetired(kOther)); // uncertain stores deny all traffic
        store->reset();
        fault.armed = false;
        store = DedupStore::Open(options);
        ASSERT_TRUE(store.ok());
        const bool renamed = stage == DedupStoreIoStage::kDirectorySync;
        EXPECT_EQ((*store)->IsRetired(kSource), renamed);
        EXPECT_EQ((*store)->HighestAccepted(kSource), renamed ? 0u : 7u);
        ASSERT_TRUE((*store)->RetireEpochsThrough(kSource).ok());
        EXPECT_TRUE((*store)->IsRetired(kSource));
    }
}

TEST_F(DedupStoreTest, RetirementCrashNeverLosesBothHwmAndBoundary) {
    for (const auto stage : {DedupStoreIoStage::kWrite, DedupStoreIoStage::kFileSync,
                            DedupStoreIoStage::kRename, DedupStoreIoStage::kDirectorySync}) {
        for (const bool after : {false, true}) {
            auto options = Options();
            options.path += std::to_string(static_cast<int>(stage)) + (after ? "after" : "before");
            auto store = DedupStore::Open(options);
            ASSERT_TRUE(store.ok());
            ASSERT_TRUE((*store)->RecordAccepted(kSource, 7).ok());
            store->reset();
            const pid_t child = ::fork();
            ASSERT_GE(child, 0);
            if (child == 0) {
                IoFailure fault;
                fault.stage = stage;
                fault.after_operation = after;
                fault.crash = true;
                options.test_hooks = &fault;
                auto writer = DedupStore::Open(options);
                if (!writer.ok()) ::_exit(98);
                (void)(*writer)->RetireEpochsThrough(kSource);
                ::_exit(97);
            }
            int status = 0;
            ASSERT_EQ(::waitpid(child, &status, 0), child);
            ASSERT_TRUE(WIFSIGNALED(status));
            ASSERT_EQ(WTERMSIG(status), SIGKILL);
            store = DedupStore::Open(options);
            ASSERT_TRUE(store.ok());
            const bool renamed = stage == DedupStoreIoStage::kDirectorySync ||
                (stage == DedupStoreIoStage::kRename && after);
            EXPECT_EQ((*store)->IsRetired(kSource), renamed);
            EXPECT_EQ((*store)->HighestAccepted(kSource), renamed ? 0u : 7u);
            ASSERT_EQ((*store)->size(), 1u);
        }
    }
}

TEST_F(DedupStoreTest, IoFailuresRollBackMemoryAndAllowDurableRetry) {
    for (const auto stage : {DedupStoreIoStage::kWrite, DedupStoreIoStage::kFileSync,
                            DedupStoreIoStage::kRename, DedupStoreIoStage::kDirectorySync}) {
        IoFailure fault;
        fault.stage = stage;
        fault.armed = false;
        auto options = Options();
        options.path += std::to_string(static_cast<int>(stage));
        options.test_hooks = &fault;
        auto store = DedupStore::Open(options);
        ASSERT_TRUE(store.ok());
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 1).ok());
        fault.armed = true;
        EXPECT_FALSE((*store)->RecordAccepted(kSource, 2).ok());
        EXPECT_EQ((*store)->HighestAccepted(kSource), 1u);
        EXPECT_EQ((*store)->stats().persistence_failures, 1u);
        fault.armed = false;
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 2).ok());
        store->reset();
        store = DedupStore::Open(options);
        ASSERT_TRUE(store.ok());
        EXPECT_EQ((*store)->HighestAccepted(kSource), 2u);
    }
}

TEST_F(DedupStoreTest, ReopenMustCommitRenameBeforeTrustingVisibleHwm) {
    IoFailure fault;
    fault.armed = false;
    auto options = Options();
    options.test_hooks = &fault;
    auto store = DedupStore::Open(options);
    ASSERT_TRUE(store.ok());
    ASSERT_TRUE((*store)->RecordAccepted(kSource, 1).ok());
    fault.stage = DedupStoreIoStage::kDirectorySync;
    fault.armed = true;
    EXPECT_FALSE((*store)->RecordAccepted(kSource, 2).ok());
    EXPECT_EQ((*store)->HighestAccepted(kSource), 1u);
    store->reset();
    // Rename has made HWM 2 visible. Recovery must not trust it if it cannot
    // sync the directory, otherwise a no-op retry/Hello could ACK too early.
    fault.stage = DedupStoreIoStage::kRecoveryDirectorySync;
    EXPECT_FALSE(DedupStore::Open(options).ok());
    fault.armed = false;
    store = DedupStore::Open(options);
    ASSERT_TRUE(store.ok());
    EXPECT_EQ((*store)->HighestAccepted(kSource), 2u);
    EXPECT_TRUE((*store)->RecordAccepted(kSource, 2).ok());
}

TEST_F(DedupStoreTest, ProcessCrashAtEveryPublishBoundaryKeepsCompleteSnapshot) {
    for (const auto stage : {DedupStoreIoStage::kWrite, DedupStoreIoStage::kFileSync,
                            DedupStoreIoStage::kRename, DedupStoreIoStage::kDirectorySync}) {
        for (const bool after : {false, true}) {
            auto options = Options();
            options.path += std::to_string(static_cast<int>(stage)) + (after ? "after" : "before");
            auto baseline = DedupStore::Open(options);
            ASSERT_TRUE(baseline.ok());
            ASSERT_TRUE((*baseline)->RecordAccepted(kSource, 1).ok());
            baseline->reset();
            const pid_t child = ::fork();
            ASSERT_GE(child, 0);
            if (child == 0) {
                IoFailure fault;
                fault.stage = stage;
                fault.after_operation = after;
                fault.crash = true;
                options.test_hooks = &fault;
                auto store = DedupStore::Open(options);
                if (!store.ok()) ::_exit(98);
                (void)(*store)->RecordAccepted(kSource, 2);
                ::_exit(97); // checkpoint must terminate the writer
            }
            int status = 0;
            ASSERT_EQ(::waitpid(child, &status, 0), child);
            ASSERT_TRUE(WIFSIGNALED(status));
            ASSERT_EQ(WTERMSIG(status), SIGKILL);
            auto reopened = DedupStore::Open(options);
            ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
            const bool renamed = stage == DedupStoreIoStage::kDirectorySync ||
                (stage == DedupStoreIoStage::kRename && after);
            EXPECT_EQ((*reopened)->HighestAccepted(kSource), renamed ? 2u : 1u);
            // Process death tests atomic visibility, not power-loss durability.
            ASSERT_TRUE((*reopened)->RecordAccepted(kSource, 3).ok());
        }
    }
}

TEST_F(DedupStoreTest, CreatesAndPersistsEveryMissingParentDirectory) {
    auto options = Options();
    options.path = (dir_ / "first" / "second" / "third" / "dedup.snap").string();
    auto store = DedupStore::Open(options);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    ASSERT_TRUE((*store)->RecordAccepted(kSource, 7).ok());
    for (auto directory = std::filesystem::path(options.path).parent_path();
         directory != dir_; directory = directory.parent_path()) {
        struct stat info {};
        ASSERT_EQ(::stat(directory.c_str(), &info), 0);
        EXPECT_EQ(info.st_mode & 0777, 0700);
    }
    store->reset();
    store = DedupStore::Open(options);
    ASSERT_TRUE(store.ok());
    EXPECT_EQ((*store)->HighestAccepted(kSource), 7u);
}

TEST_F(DedupStoreTest, RejectsUnrepresentableSnapshotCapacity) {
    EXPECT_FALSE(DedupStore::Open(Options(std::numeric_limits<size_t>::max())).ok());
}

TEST_F(DedupStoreTest, RejectsInvalidInputsAndEnforcesMaxSources) {
    auto store = DedupStore::Open(Options(/*max_sources=*/1));
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    EXPECT_EQ((*store)->RecordAccepted(SourceIdentity{}, 1).code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ((*store)->RecordAccepted(kSource, 0).code(),
              StatusCode::kInvalidArgument);
    ASSERT_TRUE((*store)->RecordAccepted(kSource, 1).ok());
    EXPECT_EQ((*store)->RecordAccepted(kOther, 1).code(),
              StatusCode::kResourceExhausted);
}

}  // namespace
}  // namespace mino::bridge
