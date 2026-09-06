// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/ptp_sync_sidecar.h"

#include <gtest/gtest.h>

#include <time.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <string>
#include <thread>

namespace mino::observability {
namespace {

PtpClockClientOptions TestClientOptions() {
    PtpClockClientOptions options;
    options.clock_domain_id = 17;
    options.clock_id = CLOCK_REALTIME;
    options.clock_id_explicit = true;
    options.maximum_uncertainty_ns = 1'000;
    options.maximum_sync_age_ns = 5'000'000'000ull;
    return options;
}

std::string WriteTempQuality(const std::string& body) {
    const std::string path = "/tmp/mino-ptp-sync-quality-" +
                             std::to_string(::getpid()) + "-" +
                             std::to_string(
                                 std::chrono::steady_clock::now()
                                     .time_since_epoch()
                                     .count()) +
                             ".txt";
    {
        std::ofstream out(path, std::ios::trunc);
        out << body;
    }
    return path;
}

TEST(PtpSyncQualityParseTest, AcceptsDocumentedSchema) {
    PtpSyncQualitySample sample;
    ASSERT_TRUE(TryParsePtpSyncQualityText(
        "mino.ptp_sync_quality.v1\n"
        "# operator wrapper around pmc GET TIME_STATUS_NP\n"
        "state=synchronized\n"
        "offset_ns=-42\n"
        "uncertainty_ns=100\n"
        "clock_domain_id=17\n",
        &sample));
    EXPECT_EQ(sample.state, ClockSyncState::kSynchronized);
    EXPECT_EQ(sample.estimated_offset_ns, -42);
    EXPECT_EQ(sample.uncertainty_ns, 100u);
    EXPECT_EQ(sample.clock_domain_id, 17u);
}

TEST(PtpSyncQualityParseTest, AcceptsCompactSingleLine) {
    PtpSyncQualitySample sample;
    ASSERT_TRUE(TryParsePtpSyncQualityText(
        "mino.ptp_sync_quality.v1 state=synchronized offset_ns=8 "
        "uncertainty_ns=40",
        &sample));
    EXPECT_EQ(sample.estimated_offset_ns, 8);
    EXPECT_EQ(sample.uncertainty_ns, 40u);
    EXPECT_EQ(sample.state, ClockSyncState::kSynchronized);
}

TEST(PtpSyncQualityParseTest, RejectsMissingFieldsAndUnknownKeys) {
    PtpSyncQualitySample sample;
    EXPECT_FALSE(TryParsePtpSyncQualityText(
        "state=synchronized\noffset_ns=1\nuncertainty_ns=1\n", &sample));
    EXPECT_FALSE(TryParsePtpSyncQualityText(
        "mino.ptp_sync_quality.v1\nstate=synchronized\noffset_ns=1\n",
        &sample));
    EXPECT_FALSE(TryParsePtpSyncQualityText(
        "mino.ptp_sync_quality.v1\nstate=synchronized\noffset_ns=1\n"
        "uncertainty_ns=1\nextra=1\n",
        &sample));
}

TEST(PtpSyncSidecarTest, InjectedSyncEnablesCrossNodeReporting) {
    auto client = PtpClockClient::Create(TestClientOptions());
    ASSERT_TRUE(client.ok()) << client.status().ToString();
    EXPECT_FALSE(client->AllowsCrossNodeOneWayReporting());

    PtpSyncSidecarOptions options;
    options.quality_path = "/tmp/unused-in-this-test";
    PtpSyncSidecar sidecar(&*client, options);

    PtpSyncQualitySample sample;
    sample.state = ClockSyncState::kSynchronized;
    sample.estimated_offset_ns = -7;
    sample.uncertainty_ns = 25;
    sample.clock_domain_id = 17;
    ASSERT_TRUE(sidecar.ApplySample(sample).ok());
    ASSERT_TRUE(client->AllowsCrossNodeOneWayReporting());

    CrossNodeLatencyRecorder<1> recorder(1'000, 1'000'000'000ull,
                                         5'000'000'000ull);
    const auto wall = client->NowWallNs();
    const auto mono = client->NowMonotonicNs();
    ASSERT_TRUE(wall.ok());
    ASSERT_TRUE(mono.ok());
    const TraceContext context =
        MakeTraceContext({1, 2, 3}, kPerfTraceSampled, 17, *wall - 50, *mono);
    EXPECT_EQ(client->TryRecord(&recorder, context, 0),
              LatencySampleDecision::kAccepted);
    EXPECT_EQ(recorder.accepted(), 1u);
}

TEST(PtpSyncSidecarTest, DomainMismatchFailClosesReporting) {
    auto client = PtpClockClient::Create(TestClientOptions());
    ASSERT_TRUE(client.ok());
    PtpSyncSidecar sidecar(&*client, {});
    ASSERT_TRUE(
        sidecar
            .ApplySample(PtpSyncQualitySample{
                .estimated_offset_ns = 0,
                .uncertainty_ns = 10,
                .state = ClockSyncState::kSynchronized,
                .clock_domain_id = 17,
            })
            .ok());
    ASSERT_TRUE(client->AllowsCrossNodeOneWayReporting());

    ASSERT_TRUE(
        sidecar
            .ApplySample(PtpSyncQualitySample{
                .estimated_offset_ns = 0,
                .uncertainty_ns = 10,
                .state = ClockSyncState::kSynchronized,
                .clock_domain_id = 99,
            })
            .ok());
    EXPECT_FALSE(client->AllowsCrossNodeOneWayReporting());
    EXPECT_EQ(sidecar.last_sample().state, ClockSyncState::kUnsynchronized);
}

TEST(PtpSyncSidecarTest, PollOnceFromQualityFile) {
    auto client = PtpClockClient::Create(TestClientOptions());
    ASSERT_TRUE(client.ok());
    const std::string path = WriteTempQuality(
        "mino.ptp_sync_quality.v1\n"
        "state=synchronized\n"
        "offset_ns=3\n"
        "uncertainty_ns=50\n");
    PtpSyncSidecarOptions options;
    options.quality_path = path;
    options.maximum_file_age_ns = 5'000'000'000ull;
    PtpSyncSidecar sidecar(&*client, options);
    ASSERT_TRUE(sidecar.PollOnce().ok()) << "poll failed";
    EXPECT_TRUE(client->AllowsCrossNodeOneWayReporting());
    ::unlink(path.c_str());
}

TEST(PtpSyncSidecarTest, MissingFileFailCloses) {
    auto client = PtpClockClient::Create(TestClientOptions());
    ASSERT_TRUE(client.ok());
    ASSERT_TRUE(
        client
            ->PublishSync(0, 10, ClockSyncState::kSynchronized)
            .ok());
    ASSERT_TRUE(client->AllowsCrossNodeOneWayReporting());

    PtpSyncSidecarOptions options;
    options.quality_path = "/tmp/mino-ptp-sync-quality-missing-does-not-exist";
    PtpSyncSidecar sidecar(&*client, options);
    ASSERT_TRUE(sidecar.PollOnce().ok());
    EXPECT_FALSE(client->AllowsCrossNodeOneWayReporting());
}

TEST(PtpSyncSidecarTest, UnsynchronizedSampleDisablesReporting) {
    auto client = PtpClockClient::Create(TestClientOptions());
    ASSERT_TRUE(client.ok());
    PtpSyncSidecar sidecar(&*client, {});
    ASSERT_TRUE(
        sidecar
            .ApplySample(PtpSyncQualitySample{
                .estimated_offset_ns = 1,
                .uncertainty_ns = 10,
                .state = ClockSyncState::kSynchronized,
            })
            .ok());
    ASSERT_TRUE(client->AllowsCrossNodeOneWayReporting());
    ASSERT_TRUE(
        sidecar
            .ApplySample(PtpSyncQualitySample{
                .estimated_offset_ns = 0,
                .uncertainty_ns = std::numeric_limits<uint64_t>::max(),
                .state = ClockSyncState::kUnsynchronized,
            })
            .ok());
    EXPECT_FALSE(client->AllowsCrossNodeOneWayReporting());
}

}  // namespace
}  // namespace mino::observability
