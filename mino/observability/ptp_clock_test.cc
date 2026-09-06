// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/ptp_clock.h"

#include <gtest/gtest.h>

#include <time.h>

#include <fstream>
#include <string>

namespace mino::observability {
namespace {

TEST(PtpClockClientTest, FailClosedWithoutConfiguration) {
    PtpClockClientOptions options;
    options.clock_domain_id = 9;
    auto missing = PtpClockClient::Create(options);
    ASSERT_FALSE(missing.ok());
    EXPECT_EQ(missing.status().code(), StatusCode::kInvalidArgument);
}

TEST(PtpClockClientTest, ClockGettimePathStaysFailClosedUntilSync) {
    PtpClockClientOptions options;
    options.clock_domain_id = 11;
    options.clock_id = CLOCK_REALTIME;
    options.clock_id_explicit = true;
    options.maximum_uncertainty_ns = 1'000;
    options.maximum_sync_age_ns = 5'000'000'000ull;
    auto client = PtpClockClient::Create(options);
    ASSERT_TRUE(client.ok()) << client.status().ToString();
    EXPECT_EQ(client->backend(), PtpClockBackend::kClockGettime);
    EXPECT_FALSE(client->AllowsCrossNodeOneWayReporting());

    CrossNodeLatencyRecorder<1> recorder(1'000, 1'000'000'000ull,
                                         5'000'000'000ull);
    const TraceContext context =
        MakeTraceContext({1, 2, 3}, kPerfTraceSampled, 11, 1, 1);
    EXPECT_EQ(client->TryRecord(&recorder, context, 0),
              LatencySampleDecision::kClockUncertain);
    EXPECT_EQ(recorder.accepted(), 0u);
}

TEST(PtpClockClientTest, SyncEnablesCrossNodeReporting) {
    PtpClockClientOptions options;
    options.clock_domain_id = 42;
    options.clock_id = CLOCK_REALTIME;
    options.clock_id_explicit = true;
    options.maximum_uncertainty_ns = 500;
    options.maximum_sync_age_ns = 5'000'000'000ull;
    auto client = PtpClockClient::Create(options);
    ASSERT_TRUE(client.ok()) << client.status().ToString();

    ASSERT_TRUE(
        client->PublishSync(/*estimated_offset_ns=*/-12, /*uncertainty_ns=*/40,
                            ClockSyncState::kSynchronized)
            .ok());
    ASSERT_TRUE(client->AllowsCrossNodeOneWayReporting());

    const auto wall = client->NowWallNs();
    const auto mono = client->NowMonotonicNs();
    ASSERT_TRUE(wall.ok());
    ASSERT_TRUE(mono.ok());
    CrossNodeLatencyRecorder<1> recorder(500, 1'000'000'000ull,
                                         5'000'000'000ull);
    const TraceContext context =
        MakeTraceContext({9, 9, 9}, kPerfTraceSampled, 42, *wall - 100, *mono);
    EXPECT_EQ(client->TryRecord(&recorder, context, 0),
              LatencySampleDecision::kAccepted);
    EXPECT_EQ(recorder.accepted(), 1u);
}

TEST(PtpClockClientTest, ExcessiveUncertaintyDegradesAndBlocksReporting) {
    PtpClockClientOptions options;
    options.clock_domain_id = 7;
    options.clock_id = CLOCK_REALTIME;
    options.clock_id_explicit = true;
    options.maximum_uncertainty_ns = 100;
    options.maximum_sync_age_ns = 5'000'000'000ull;
    auto client = PtpClockClient::Create(options);
    ASSERT_TRUE(client.ok());
    ASSERT_TRUE(client
                    ->PublishSync(0, /*uncertainty_ns=*/250,
                                  ClockSyncState::kSynchronized)
                    .ok());
    ClockQuality quality = client->LatestQualityOrUncertain();
    EXPECT_EQ(quality.state, ClockSyncState::kDegraded);
    EXPECT_FALSE(client->AllowsCrossNodeOneWayReporting());
}

TEST(PtpClockClientTest, PhcDeviceOpensWhenAccessible) {
    std::ifstream accessible("/dev/ptp0");
    if (!accessible.good()) {
        GTEST_SKIP() << "/dev/ptp0 not readable in this environment";
    }
    PtpClockClientOptions options;
    options.clock_domain_id = 3;
    options.phc_device_path = "/dev/ptp0";
    options.maximum_uncertainty_ns = 1'000;
    options.maximum_sync_age_ns = 5'000'000'000ull;
    auto client = PtpClockClient::Create(options);
    if (!client.ok()) {
        GTEST_SKIP() << "PHC open failed: " << client.status().ToString();
    }
    EXPECT_EQ(client->backend(), PtpClockBackend::kPhcDevice);
    EXPECT_FALSE(client->AllowsCrossNodeOneWayReporting());
    EXPECT_TRUE(client->NowWallNs().ok());
}

TEST(TryParsePtpOffsetLineTest, ParsesOffsetToken) {
    int64_t offset = 0;
    uint64_t uncertainty = 0;
    ASSERT_TRUE(TryParsePtpOffsetLine("master offset         -1234 s2 freq",
                                      &offset, &uncertainty));
    EXPECT_EQ(offset, -1234);
    EXPECT_EQ(uncertainty, 1234u);
    EXPECT_FALSE(TryParsePtpOffsetLine("no relevant fields", &offset,
                                       &uncertainty));
}

}  // namespace
}  // namespace mino::observability
