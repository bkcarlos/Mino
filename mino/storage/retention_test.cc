// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/retention.h"

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "mino/common/status.h"
#include "mino/storage/recording_manifest.h"

namespace mino::storage {
namespace {

class TemporaryDirectory final {
public:
    explicit TemporaryDirectory(std::string_view name) {
        static std::atomic<uint64_t> sequence{0};
        const char* temporary = std::getenv("TEST_TMPDIR");
        const std::filesystem::path base =
            temporary == nullptr ? std::filesystem::temp_directory_path()
                                 : std::filesystem::path(temporary);
        path_ = base / ("mino_retention_" + std::string(name) + "_" +
                        std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                        std::to_string(sequence.fetch_add(1)));
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
        std::filesystem::create_directories(path_ / "segments");
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

PartitionMetadata Metadata() {
    return PartitionMetadata{
        .recording_id = 101,
        .topic_id = 7,
        .partition_id = 0,
        .writer_id = 23,
        .owner_epoch = 2,
        .config_version = 1,
    };
}

SegmentManifestEntry Segment(uint64_t id, SegmentPersistentState state,
                             uint64_t size_bytes = 100) {
    return SegmentManifestEntry{
        .segment_id = id,
        .state = state,
        .first_ingestion_sequence = id * 10,
        .last_ingestion_sequence = id * 10 + 9,
        .created_at_ns = id * 100,
        .sealed_at_ns =
            state == SegmentPersistentState::kCreating ||
                    state == SegmentPersistentState::kOpen
                ? 0
                : id * 100 + 50,
        .size_bytes = size_bytes,
        .relative_path = std::filesystem::path("segments") /
                         ("0000000" + std::to_string(id) + ".mino"),
    };
}

void WriteSegmentFile(const std::filesystem::path& root,
                      const SegmentManifestEntry& segment) {
    std::ofstream output(root / segment.relative_path,
                         std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output << "segment-" << segment.segment_id;
    ASSERT_TRUE(output.good());
}

std::unique_ptr<PartitionManifest> CreateManifest(
    const std::filesystem::path& root,
    const std::vector<SegmentManifestEntry>& segments) {
    auto created = PartitionManifest::Create(root, Metadata());
    EXPECT_TRUE(created.ok()) << created.status().ToString();
    if (!created.ok()) return nullptr;
    std::unique_ptr<PartitionManifest> manifest = std::move(*created);
    for (const SegmentManifestEntry& segment : segments) {
        const Status added = manifest->AddSegment(segment);
        EXPECT_TRUE(added.ok()) << added.ToString();
        if (!added.ok()) return nullptr;
    }
    return manifest;
}

RetentionPlan PlanFor(std::initializer_list<uint64_t> ids) {
    RetentionPlan plan;
    for (uint64_t id : ids) {
        plan.deletions.push_back(RetentionDecision{
            .segment_id = id,
            .reasons = RetentionReason::kAge,
        });
    }
    return plan;
}

struct FakeClock {
    uint64_t now_ns = 0;

    static uint64_t Now(void* context) noexcept {
        return static_cast<FakeClock*>(context)->now_ns;
    }
};

TEST(RetentionPlannerTest, CombinesAgeArchiveByteAndCountPolicies) {
    const std::vector<RetentionSegment> segments = {
        RetentionSegment{.manifest = Segment(1, SegmentPersistentState::kSealed,
                                             10),
                         .latest_ingestion_timestamp_ns = 10},
        RetentionSegment{.manifest = Segment(2, SegmentPersistentState::kIndexed,
                                             20),
                         .latest_ingestion_timestamp_ns = 80},
        RetentionSegment{.manifest = Segment(3, SegmentPersistentState::kRetained,
                                             30),
                         .latest_ingestion_timestamp_ns = 90,
                         .archive_complete = true},
        RetentionSegment{.manifest = Segment(4, SegmentPersistentState::kOpen,
                                             40),
                         .latest_ingestion_timestamp_ns = 1,
                         .archive_complete = true},
    };
    RetentionPolicy policy;
    policy.max_age_ns = 50;
    policy.max_total_bytes = 40;
    policy.max_segment_count = 1;
    policy.delete_archived_segments = true;

    auto plan = RetentionPlanner::Plan(segments, policy, 100);
    ASSERT_TRUE(plan.ok()) << plan.status().ToString();
    ASSERT_EQ(plan->deletions.size(), 3u);
    EXPECT_EQ(plan->deletions[0].segment_id, 1u);
    EXPECT_TRUE(HasRetentionReason(plan->deletions[0].reasons,
                                   RetentionReason::kAge));
    EXPECT_EQ(plan->deletions[1].segment_id, 2u);
    EXPECT_TRUE(HasRetentionReason(plan->deletions[1].reasons,
                                   RetentionReason::kTotalBytes));
    EXPECT_TRUE(HasRetentionReason(plan->deletions[1].reasons,
                                   RetentionReason::kSegmentCount));
    EXPECT_EQ(plan->deletions[2].segment_id, 3u);
    EXPECT_TRUE(HasRetentionReason(plan->deletions[2].reasons,
                                   RetentionReason::kArchiveComplete));
    EXPECT_EQ(plan->remaining_total_bytes, 40u);
    EXPECT_EQ(plan->remaining_segment_count, 1u);
    EXPECT_TRUE(plan->byte_limit_satisfied);
    EXPECT_TRUE(plan->segment_limit_satisfied);
}

TEST(RetentionPlannerTest, NeverSelectsOpenAndReportsUnsatisfiedLimits) {
    const std::vector<RetentionSegment> segments = {
        RetentionSegment{.manifest = Segment(1, SegmentPersistentState::kOpen,
                                             100),
                         .latest_ingestion_timestamp_ns = 1,
                         .archive_complete = true},
    };
    RetentionPolicy policy;
    policy.max_age_ns = 0;
    policy.max_total_bytes = 0;
    policy.max_segment_count = 0;
    policy.delete_archived_segments = true;

    auto plan = RetentionPlanner::Plan(segments, policy, 100);
    ASSERT_TRUE(plan.ok()) << plan.status().ToString();
    EXPECT_TRUE(plan->deletions.empty());
    EXPECT_FALSE(plan->byte_limit_satisfied);
    EXPECT_FALSE(plan->segment_limit_satisfied);
}

TEST(SegmentPinManagerTest, SupportsTtlRenewReleaseAndManifestValidation) {
    TemporaryDirectory directory("pin_ttl");
    const SegmentManifestEntry segment =
        Segment(1, SegmentPersistentState::kIndexed);
    WriteSegmentFile(directory.path(), segment);
    std::unique_ptr<PartitionManifest> manifest =
        CreateManifest(directory.path(), {segment});
    ASSERT_NE(manifest, nullptr);

    FakeClock clock{.now_ns = 100};
    SegmentPinManager pins(
        *manifest, SegmentPinManagerOptions{
                       .default_ttl_ns = 10,
                       .max_ttl_ns = 100,
                       .now = &FakeClock::Now,
                       .now_context = &clock,
                   });
    auto pin = pins.Acquire(1);
    ASSERT_TRUE(pin.ok()) << pin.status().ToString();
    EXPECT_EQ(pin->expires_at_ns, 110u);
    EXPECT_EQ(pins.ActivePinCount(1), 1u);

    clock.now_ns = 105;
    auto renewed = pins.Renew(pin->pin_id);
    ASSERT_TRUE(renewed.ok()) << renewed.status().ToString();
    EXPECT_EQ(renewed->expires_at_ns, 115u);
    clock.now_ns = 114;
    EXPECT_EQ(pins.ActivePinCount(1), 1u);
    clock.now_ns = 115;
    EXPECT_EQ(pins.ActivePinCount(1), 0u);
    EXPECT_EQ(pins.Renew(pin->pin_id).status().code(), StatusCode::kNotFound);
    EXPECT_TRUE(pins.Release(pin->pin_id).ok());
    EXPECT_EQ(pins.Acquire(1, 101).status().code(),
              StatusCode::kInvalidArgument);

    SegmentManifestEntry deleted = segment;
    deleted.state = SegmentPersistentState::kDeleted;
    ASSERT_TRUE(manifest->UpdateSegment(deleted).ok());
    EXPECT_EQ(pins.Acquire(1).status().code(), StatusCode::kNotFound);
}

TEST(RetentionExecutorTest, ClosesNewPinsBeforeWaitingForExistingPin) {
    TemporaryDirectory directory("pin_race");
    const SegmentManifestEntry segment =
        Segment(1, SegmentPersistentState::kSealed);
    WriteSegmentFile(directory.path(), segment);
    std::unique_ptr<PartitionManifest> manifest =
        CreateManifest(directory.path(), {segment});
    ASSERT_NE(manifest, nullptr);
    FakeClock clock{.now_ns = 100};
    SegmentPinManager pins(
        *manifest, SegmentPinManagerOptions{
                       .default_ttl_ns = 100,
                       .max_ttl_ns = 100,
                       .now = &FakeClock::Now,
                       .now_context = &clock,
                   });
    auto existing = pins.Acquire(1);
    ASSERT_TRUE(existing.ok()) << existing.status().ToString();
    auto executor = RetentionExecutor::Create(directory.path(), *manifest, pins);
    ASSERT_TRUE(executor.ok()) << executor.status().ToString();

    auto first = (*executor)->Execute(PlanFor({1}));
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_EQ(first->marked_deleted, std::vector<uint64_t>({1}));
    EXPECT_EQ(first->pending_pins, std::vector<uint64_t>({1}));
    EXPECT_TRUE(std::filesystem::exists(directory.path() / segment.relative_path));
    ASSERT_TRUE(manifest->FindSegment(1).ok());
    EXPECT_EQ(manifest->FindSegment(1)->state, SegmentPersistentState::kDeleted);
    EXPECT_EQ(pins.Acquire(1).status().code(), StatusCode::kNotFound);

    ASSERT_TRUE(pins.Release(existing->pin_id).ok());
    auto recovered = (*executor)->RecoverPendingDeletions();
    ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
    EXPECT_EQ(recovered->unlinked, std::vector<uint64_t>({1}));
    EXPECT_FALSE(std::filesystem::exists(directory.path() / segment.relative_path));
}

struct BlockingHookState {
    std::atomic<bool> entered{false};
    std::atomic<bool> proceed{false};
};

Status BlockingAfterManifestHook(RetentionFaultPoint point, uint64_t,
                                 void* context) noexcept {
    if (point != RetentionFaultPoint::kAfterManifestDeleted) {
        return Status::Ok();
    }
    auto* state = static_cast<BlockingHookState*>(context);
    state->entered.store(true, std::memory_order_release);
    while (!state->proceed.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    return Status::Ok();
}

TEST(RetentionExecutorTest, ConcurrentAcquireCannotCrossManifestClosure) {
    TemporaryDirectory directory("concurrent_pin_race");
    const SegmentManifestEntry segment =
        Segment(1, SegmentPersistentState::kIndexed);
    WriteSegmentFile(directory.path(), segment);
    std::unique_ptr<PartitionManifest> manifest =
        CreateManifest(directory.path(), {segment});
    ASSERT_NE(manifest, nullptr);
    SegmentPinManager pins(*manifest);
    BlockingHookState hook_state;
    auto executor = RetentionExecutor::Create(
        directory.path(), *manifest, pins,
        RetentionExecutorOptions{
            .fault_hook = &BlockingAfterManifestHook,
            .fault_hook_context = &hook_state,
        });
    ASSERT_TRUE(executor.ok()) << executor.status().ToString();

    Result<RetentionExecutionReport> execution(
        Status::Error(StatusCode::kInternal, "not run"));
    std::thread worker([&] { execution = (*executor)->Execute(PlanFor({1})); });
    while (!hook_state.entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    EXPECT_EQ(pins.Acquire(1).status().code(), StatusCode::kNotFound);
    hook_state.proceed.store(true, std::memory_order_release);
    worker.join();
    ASSERT_TRUE(execution.ok()) << execution.status().ToString();
    EXPECT_EQ(execution->unlinked, std::vector<uint64_t>({1}));
}

TEST(RetentionExecutorTest, PlannerAndExecutorBothProtectOpenSegments) {
    TemporaryDirectory directory("open");
    const SegmentManifestEntry segment =
        Segment(1, SegmentPersistentState::kOpen);
    WriteSegmentFile(directory.path(), segment);
    std::unique_ptr<PartitionManifest> manifest =
        CreateManifest(directory.path(), {segment});
    ASSERT_NE(manifest, nullptr);
    SegmentPinManager pins(*manifest);
    auto executor = RetentionExecutor::Create(directory.path(), *manifest, pins);
    ASSERT_TRUE(executor.ok()) << executor.status().ToString();

    auto result = (*executor)->Execute(PlanFor({1}));
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_TRUE(std::filesystem::exists(directory.path() / segment.relative_path));
    ASSERT_TRUE(manifest->FindSegment(1).ok());
    EXPECT_EQ(manifest->FindSegment(1)->state, SegmentPersistentState::kOpen);
}

struct CrashHookState {
    RetentionFaultPoint point = RetentionFaultPoint::kAfterManifestDeleted;
    bool fired = false;
};

Status CrashHook(RetentionFaultPoint point, uint64_t, void* context) noexcept {
    auto* state = static_cast<CrashHookState*>(context);
    if (!state->fired && point == state->point) {
        state->fired = true;
        return Status::Error(StatusCode::kUnavailable,
                             "injected retention crash");
    }
    return Status::Ok();
}

TEST(RetentionExecutorTest, EveryCrashPhaseRecoversIdempotently) {
    const std::vector<RetentionFaultPoint> points = {
        RetentionFaultPoint::kAfterManifestDeleted,
        RetentionFaultPoint::kBeforeUnlink,
        RetentionFaultPoint::kAfterUnlink,
        RetentionFaultPoint::kAfterParentDirectorySync,
    };
    for (RetentionFaultPoint point : points) {
        SCOPED_TRACE(static_cast<int>(point));
        TemporaryDirectory directory("crash_phase");
        const SegmentManifestEntry segment =
            Segment(1, SegmentPersistentState::kIndexed);
        WriteSegmentFile(directory.path(), segment);
        std::unique_ptr<PartitionManifest> manifest =
            CreateManifest(directory.path(), {segment});
        ASSERT_NE(manifest, nullptr);
        SegmentPinManager pins(*manifest);
        CrashHookState hook_state{.point = point};
        auto crashing = RetentionExecutor::Create(
            directory.path(), *manifest, pins,
            RetentionExecutorOptions{
                .fault_hook = &CrashHook,
                .fault_hook_context = &hook_state,
            });
        ASSERT_TRUE(crashing.ok()) << crashing.status().ToString();
        auto interrupted = (*crashing)->Execute(PlanFor({1}));
        ASSERT_FALSE(interrupted.ok());
        ASSERT_TRUE(manifest->FindSegment(1).ok());
        EXPECT_EQ(manifest->FindSegment(1)->state,
                  SegmentPersistentState::kDeleted);

        auto recovering =
            RetentionExecutor::Create(directory.path(), *manifest, pins);
        ASSERT_TRUE(recovering.ok()) << recovering.status().ToString();
        auto recovered = (*recovering)->RecoverPendingDeletions();
        ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
        EXPECT_FALSE(
            std::filesystem::exists(directory.path() / segment.relative_path));
        auto repeated = (*recovering)->RecoverPendingDeletions();
        ASSERT_TRUE(repeated.ok()) << repeated.status().ToString();
        EXPECT_EQ(repeated->already_missing, std::vector<uint64_t>({1}));
    }
}

TEST(RetentionExecutorTest, ExpiredLeaseAllowsPendingDeletionToFinish) {
    TemporaryDirectory directory("lease_expiry");
    const SegmentManifestEntry segment =
        Segment(1, SegmentPersistentState::kRetained);
    WriteSegmentFile(directory.path(), segment);
    std::unique_ptr<PartitionManifest> manifest =
        CreateManifest(directory.path(), {segment});
    ASSERT_NE(manifest, nullptr);
    FakeClock clock{.now_ns = 10};
    SegmentPinManager pins(
        *manifest, SegmentPinManagerOptions{
                       .default_ttl_ns = 5,
                       .max_ttl_ns = 10,
                       .now = &FakeClock::Now,
                       .now_context = &clock,
                   });
    ASSERT_TRUE(pins.Acquire(1).ok());
    auto executor = RetentionExecutor::Create(directory.path(), *manifest, pins);
    ASSERT_TRUE(executor.ok()) << executor.status().ToString();
    auto pending = (*executor)->Execute(PlanFor({1}));
    ASSERT_TRUE(pending.ok()) << pending.status().ToString();
    EXPECT_EQ(pending->pending_pins, std::vector<uint64_t>({1}));

    clock.now_ns = 15;
    auto completed = (*executor)->RecoverPendingDeletions();
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
    EXPECT_EQ(completed->unlinked, std::vector<uint64_t>({1}));
}

TEST(RetentionExecutorTest, RefusesSegmentAndDirectorySymlinks) {
    TemporaryDirectory directory("symlink_file");
    TemporaryDirectory outside("symlink_outside");
    const SegmentManifestEntry segment =
        Segment(1, SegmentPersistentState::kIndexed);
    const std::filesystem::path outside_file = outside.path() / "keep.mino";
    {
        std::ofstream output(outside_file);
        ASSERT_TRUE(output.is_open());
        output << "keep";
    }
    std::filesystem::create_symlink(outside_file,
                                    directory.path() / segment.relative_path);
    std::unique_ptr<PartitionManifest> manifest =
        CreateManifest(directory.path(), {segment});
    ASSERT_NE(manifest, nullptr);
    SegmentPinManager pins(*manifest);
    auto executor = RetentionExecutor::Create(directory.path(), *manifest, pins);
    ASSERT_TRUE(executor.ok()) << executor.status().ToString();
    auto result = (*executor)->Execute(PlanFor({1}));
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_TRUE(std::filesystem::exists(outside_file));

    TemporaryDirectory directory_link("symlink_directory");
    TemporaryDirectory target("symlink_directory_target");
    std::filesystem::remove(directory_link.path() / "segments");
    std::filesystem::create_directory_symlink(
        target.path() / "segments", directory_link.path() / "segments");
    auto second_manifest =
        PartitionManifest::Create(directory_link.path(), Metadata());
    ASSERT_TRUE(second_manifest.ok()) << second_manifest.status().ToString();
    SegmentPinManager second_pins(**second_manifest);
    auto unsafe = RetentionExecutor::Create(
        directory_link.path(), **second_manifest, second_pins);
    EXPECT_FALSE(unsafe.ok());
}

TEST(RetentionExecutorTest, RefusesNonRegularPathWithExplicitError) {
    TemporaryDirectory directory("unlink_error");
    const SegmentManifestEntry segment =
        Segment(1, SegmentPersistentState::kIndexed);
    std::filesystem::create_directory(directory.path() / segment.relative_path);
    std::unique_ptr<PartitionManifest> manifest =
        CreateManifest(directory.path(), {segment});
    ASSERT_NE(manifest, nullptr);
    SegmentPinManager pins(*manifest);
    auto executor = RetentionExecutor::Create(directory.path(), *manifest, pins);
    ASSERT_TRUE(executor.ok()) << executor.status().ToString();

    auto result = (*executor)->Execute(PlanFor({1}));
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(result.status().message().find("non-regular"),
              std::string_view::npos);
    EXPECT_TRUE(std::filesystem::is_directory(
        directory.path() / segment.relative_path));
}

}  // namespace
}  // namespace mino::storage
