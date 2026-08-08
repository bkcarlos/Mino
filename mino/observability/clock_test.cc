// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/clock.h"

#include <gtest/gtest.h>

namespace mino::observability {
namespace {

TEST(AtomicClockQualityTest, PublishesConsistentSnapshot) {
    AtomicClockQuality atomic_quality;
    const ClockQuality input{7, -25, 10, 1'000,
                             ClockSyncState::kSynchronized};
    atomic_quality.Store(input);
    ClockQuality output;
    ASSERT_TRUE(atomic_quality.TryLoad(&output));
    EXPECT_EQ(output.clock_domain_id, 7u);
    EXPECT_EQ(output.estimated_offset_ns, -25);
    EXPECT_EQ(output.uncertainty_ns, 10u);
    EXPECT_EQ(output.last_sync_time_ns, 1'000u);
    EXPECT_EQ(output.state, ClockSyncState::kSynchronized);
}

TEST(CrossNodeLatencyRecorderTest, RejectsUntrustedSamplesByReason) {
    CrossNodeLatencyRecorder<1> recorder(/*maximum_uncertainty_ns=*/20,
                                         /*maximum_clock_jump_ns=*/100,
                                         /*maximum_sync_age_ns=*/1'000);
    const TraceContext context =
        MakeTraceContext({1, 2, 3}, kPerfTraceSampled, 9, 1'000, 500);
    ClockQuality quality{9, 0, 10, 900, ClockSyncState::kSynchronized};

    EXPECT_EQ(recorder.Record(context, 1'100, 600, quality, 0),
              LatencySampleDecision::kAccepted);

    TraceContext future = context;
    future.origin_wall_time_ns = 1'200;
    EXPECT_EQ(recorder.Record(future, 1'150, 650, quality, 0),
              LatencySampleDecision::kNegativeLatency);

    quality.uncertainty_ns = 21;
    EXPECT_EQ(recorder.Record(context, 1'200, 700, quality, 0),
              LatencySampleDecision::kClockUncertain);

    quality.uncertainty_ns = 10;
    EXPECT_EQ(recorder.Record(context, 2'200, 750, quality, 0),
              LatencySampleDecision::kClockJump);

    EXPECT_EQ(recorder.accepted(), 1u);
    EXPECT_EQ(recorder.negative_latency(), 1u);
    EXPECT_EQ(recorder.clock_uncertain(), 1u);
    EXPECT_EQ(recorder.clock_jump(), 1u);
    const LogHistogramSnapshot histogram = recorder.latency_snapshot();
    EXPECT_EQ(histogram.count, 1u);
    EXPECT_EQ(histogram.sum, 100u);
}

TEST(CrossNodeLatencyRecorderTest, RejectsWrongDomainAndStaleSync) {
    CrossNodeLatencyRecorder<2> recorder(100, 1'000, 50);
    const TraceContext context =
        MakeTraceContext({1, 1, 1}, kPerfTraceSampled, 3, 100, 10);
    ClockQuality wrong_domain{4, 0, 1, 100,
                              ClockSyncState::kSynchronized};
    EXPECT_EQ(recorder.Record(context, 120, 20, wrong_domain, 0),
              LatencySampleDecision::kClockUncertain);
    ClockQuality stale{3, 0, 1, 100, ClockSyncState::kSynchronized};
    EXPECT_EQ(recorder.Record(context, 200, 100, stale, 1),
              LatencySampleDecision::kClockUncertain);
    EXPECT_EQ(recorder.latency_snapshot().count, 0u);
}

}  // namespace
}  // namespace mino::observability
