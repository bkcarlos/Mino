// Copyright 2026 The Mino Authors

#include "mino/bridge/dedup_window.h"

#include <gtest/gtest.h>

#include <memory>

#include "mino/common/status.h"

namespace mino::bridge {
namespace {

constexpr SourceIdentity kSource{1, 2, 3};
constexpr SourceIdentity kOtherSource{4, 5, 6};

std::unique_ptr<DedupWindow> MakeWindow(DedupWindowOptions options = {}) {
    auto result = DedupWindow::Create(options);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    return result.ok() ? std::move(*result) : nullptr;
}

TEST(DedupWindowTest, HandlesOutOfOrderDuplicateAndGapClosure) {
    auto window = MakeWindow(DedupWindowOptions{
        .max_sources = 4,
        .max_bytes = 4096,
        .max_sequence_distance = 8,
        .max_source_age_ns = 100,
    });
    ASSERT_NE(window, nullptr);
    window->BeginSession(10, 0);

    ASSERT_TRUE(window->CommitAccepted(10, kSource, 1, 1).ok());
    auto three = window->Check(10, kSource, 3, 2);
    ASSERT_TRUE(three.ok());
    EXPECT_EQ(three->decision, DedupDecision::kAccept);
    EXPECT_EQ(three->highest_contiguous_sequence, 1);
    ASSERT_TRUE(window->CommitAccepted(10, kSource, 3, 2).ok());

    auto duplicate_three = window->Check(10, kSource, 3, 3);
    ASSERT_TRUE(duplicate_three.ok());
    EXPECT_EQ(duplicate_three->decision,
              DedupDecision::kDuplicateAccepted);
    EXPECT_EQ(duplicate_three->highest_contiguous_sequence, 1);

    ASSERT_TRUE(window->CommitAccepted(10, kSource, 2, 4).ok());
    auto duplicate_one = window->Check(10, kSource, 1, 5);
    ASSERT_TRUE(duplicate_one.ok());
    EXPECT_EQ(duplicate_one->decision,
              DedupDecision::kDuplicateAccepted);
    EXPECT_EQ(duplicate_one->highest_contiguous_sequence, 3);
}

TEST(DedupWindowTest, FirstOutOfOrderFrameWaitsForContiguousPrefix) {
    auto window = MakeWindow(DedupWindowOptions{
        .max_sources = 4,
        .max_bytes = 4096,
        .max_sequence_distance = 10,
        .max_source_age_ns = 100,
    });
    ASSERT_NE(window, nullptr);
    window->BeginSession(7, 0);

    auto ten = window->Check(7, kSource, 10, 1);
    ASSERT_TRUE(ten.ok());
    EXPECT_EQ(ten->decision, DedupDecision::kAccept);
    EXPECT_EQ(ten->highest_contiguous_sequence, 0);
    ASSERT_TRUE(window->CommitAccepted(7, kSource, 10, 1).ok());

    auto snapshot = window->SnapshotAccepted();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    ASSERT_EQ(snapshot->size(), 1u);
    EXPECT_EQ((*snapshot)[0].highest_contiguous_sequence, 0u);

    for (uint64_t sequence = 1; sequence <= 9; ++sequence) {
        auto next = window->Check(7, kSource, sequence, sequence + 1);
        ASSERT_TRUE(next.ok());
        EXPECT_EQ(next->decision, DedupDecision::kAccept);
        EXPECT_EQ(next->highest_contiguous_sequence, sequence - 1);
        ASSERT_TRUE(
            window->CommitAccepted(7, kSource, sequence, sequence + 1).ok());
    }

    auto duplicate_ten = window->Check(7, kSource, 10, 11);
    ASSERT_TRUE(duplicate_ten.ok());
    EXPECT_EQ(duplicate_ten->decision,
              DedupDecision::kDuplicateAccepted);
    EXPECT_EQ(duplicate_ten->highest_contiguous_sequence, 10);
}

TEST(DedupWindowTest, NewSourceBeyondWindowNacksWithHighestZero) {
    auto window = MakeWindow(DedupWindowOptions{
        .max_sources = 2,
        .max_bytes = 4096,
        .max_sequence_distance = 4,
        .max_source_age_ns = 100,
    });
    ASSERT_NE(window, nullptr);
    window->BeginSession(7, 0);

    auto far = window->Check(7, kSource, 5, 1);
    ASSERT_TRUE(far.ok());
    EXPECT_EQ(far->decision, DedupDecision::kNackWithHighest);
    EXPECT_EQ(far->highest_contiguous_sequence, 0);
    EXPECT_EQ(window->source_count(), 0u);
}

TEST(DedupWindowTest, NacksOldAndFarAheadSequencesWithHighest) {
    auto window = MakeWindow(DedupWindowOptions{
        .max_sources = 2,
        .max_bytes = 4096,
        .max_sequence_distance = 4,
        .max_source_age_ns = 100,
    });
    ASSERT_NE(window, nullptr);
    window->BeginSession(7, 0);
    ASSERT_TRUE(window->SeedAccepted(7, kSource, 10, 0).ok());

    auto old = window->Check(7, kSource, 6, 1);
    ASSERT_TRUE(old.ok());
    EXPECT_EQ(old->decision, DedupDecision::kNackWithHighest);
    EXPECT_EQ(old->highest_contiguous_sequence, 10);

    auto far = window->Check(7, kSource, 15, 2);
    ASSERT_TRUE(far.ok());
    EXPECT_EQ(far->decision, DedupDecision::kNackWithHighest);
    EXPECT_EQ(far->highest_contiguous_sequence, 10);
    EXPECT_GE(window->stats().gap_events, 2u);
}

TEST(DedupWindowTest, SessionEpochFencesStaleChecksAndCommits) {
    auto window = MakeWindow();
    ASSERT_NE(window, nullptr);
    window->BeginSession(1, 0);
    ASSERT_TRUE(window->CommitAccepted(1, kSource, 1, 0).ok());
    window->BeginSession(2, 1);
    EXPECT_EQ(window->source_count(), 0u);

    auto stale = window->Check(1, kSource, 1, 2);
    ASSERT_TRUE(stale.ok());
    EXPECT_EQ(stale->decision, DedupDecision::kStaleSession);
    Status stale_commit = window->CommitAccepted(1, kSource, 1, 2);
    EXPECT_EQ(stale_commit.code(), StatusCode::kUnavailable);

    auto fresh = window->Check(2, kSource, 1, 2);
    ASSERT_TRUE(fresh.ok());
    EXPECT_EQ(fresh->decision, DedupDecision::kAccept);
    EXPECT_EQ(window->stats().session_switches, 1u);
}

TEST(DedupWindowTest, PreservesStateAcrossReconnectAndSnapshotsInStableOrder) {
    auto window = MakeWindow();
    ASSERT_NE(window, nullptr);
    window->BeginSession(1, 0);
    ASSERT_TRUE(window->SeedAccepted(1, kOtherSource, 4, 1).ok());
    ASSERT_TRUE(window->SeedAccepted(1, kSource, 7, 2).ok());

    window->BeginSession(2, 3, true);
    auto duplicate = window->Check(2, kSource, 7, 4);
    ASSERT_TRUE(duplicate.ok());
    EXPECT_EQ(duplicate->decision, DedupDecision::kDuplicateAccepted);

    auto snapshot = window->SnapshotAccepted();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    ASSERT_EQ(snapshot->size(), 2u);
    EXPECT_EQ((*snapshot)[0].source, kSource);
    EXPECT_EQ((*snapshot)[0].highest_contiguous_sequence, 7u);
    EXPECT_EQ((*snapshot)[1].source, kOtherSource);
    EXPECT_EQ((*snapshot)[1].highest_contiguous_sequence, 4u);
}

TEST(DedupWindowTest, EnforcesSourceByteAndAgeBounds) {
    auto impossible = DedupWindow::Create(DedupWindowOptions{
        .max_sources = 1,
        .max_bytes = 1,
        .max_sequence_distance = 64,
        .max_source_age_ns = 10,
    });
    ASSERT_FALSE(impossible.ok());
    EXPECT_EQ(impossible.status().code(), StatusCode::kResourceExhausted);

    auto window = MakeWindow(DedupWindowOptions{
        .max_sources = 1,
        .max_bytes = 4096,
        .max_sequence_distance = 64,
        .max_source_age_ns = 10,
    });
    ASSERT_NE(window, nullptr);
    window->BeginSession(1, 0);
    ASSERT_TRUE(window->CommitAccepted(1, kSource, 1, 0).ok());
    ASSERT_TRUE(window->CommitAccepted(1, kOtherSource, 1, 1).ok());
    EXPECT_EQ(window->source_count(), 1u);
    EXPECT_EQ(window->stats().source_evictions, 1u);

    EXPECT_EQ(window->PurgeExpired(12), 1u);
    EXPECT_EQ(window->source_count(), 0u);
    EXPECT_EQ(window->stats().age_evictions, 1u);
}

}  // namespace
}  // namespace mino::bridge
