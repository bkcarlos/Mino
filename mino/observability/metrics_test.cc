// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/metrics.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <iterator>
#include <thread>
#include <vector>

namespace mino::observability {
namespace {

TEST(ShardedCounterTest, AggregatesConcurrentDedicatedShards) {
    ShardedCounter<4> counter;
    std::vector<std::thread> threads;
    for (size_t shard = 0; shard < 4; ++shard) {
        threads.emplace_back([&, shard] {
            for (size_t i = 0; i < 25'000; ++i) counter.Increment(shard);
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(counter.Value(), 100'000u);
}

TEST(LogHistogramTest, UsesDocumentedPowerOfTwoBuckets) {
    ShardedLogHistogram<2> histogram;
    constexpr uint64_t kValues[] = {0, 1, 2, 3, 4, 7, 8, UINT64_MAX};
    for (size_t i = 0; i < std::size(kValues); ++i) {
        histogram.Record(kValues[i], i % 2);
    }
    const LogHistogramSnapshot snapshot = histogram.Snapshot();
    EXPECT_EQ(snapshot.count, std::size(kValues));
    EXPECT_EQ(snapshot.buckets[0], 1u);
    EXPECT_EQ(snapshot.buckets[1], 1u);
    EXPECT_EQ(snapshot.buckets[2], 2u);
    EXPECT_EQ(snapshot.buckets[3], 2u);
    EXPECT_EQ(snapshot.buckets[4], 1u);
    EXPECT_EQ(snapshot.buckets[64], 1u);
    EXPECT_EQ(ShardedLogHistogram<>::UpperBound(4), 15u);
    EXPECT_EQ(ShardedLogHistogram<>::UpperBound(64), UINT64_MAX);
}

TEST(MetricRegistryTest, ValidatesNamesAndTakesSelfContainedSnapshot) {
    MetricRegistry registry(/*process_start_unix_ns=*/100);
    CounterMetric* messages = nullptr;
    GaugeMetric* inflight = nullptr;
    HistogramMetric* latency = nullptr;
    EXPECT_TRUE(registry.RegisterCounter("mino_messages_total", &messages).ok());
    EXPECT_TRUE(registry.RegisterGauge("mino_inflight", &inflight).ok());
    EXPECT_TRUE(registry.RegisterHistogram("mino_latency_ns", &latency).ok());
    EXPECT_FALSE(registry.RegisterCounter("bad-name", &messages).ok());
    EXPECT_FALSE(registry.RegisterCounter("mino_messages_total", &messages).ok());

    messages->counter().Add(7, 3);
    inflight->gauge().Set(4, 0);
    inflight->gauge().Set(5, 1);
    latency->histogram().Record(10, 2);
    TelemetrySnapshot snapshot;
    registry.TakeSnapshot(123, &snapshot);

    ASSERT_EQ(snapshot.counter_count, 1u);
    ASSERT_EQ(snapshot.gauge_count, 1u);
    ASSERT_EQ(snapshot.histogram_count, 1u);
    EXPECT_EQ(snapshot.timestamp_unix_ns, 123u);
    EXPECT_EQ(snapshot.process_start_unix_ns, 100u);
    EXPECT_EQ(snapshot.counters[0].name.view(), "mino_messages_total");
    EXPECT_EQ(snapshot.counters[0].value, 7u);
    EXPECT_EQ(snapshot.gauges[0].name.view(), "mino_inflight");
    EXPECT_EQ(snapshot.gauges[0].value, 9u);
    EXPECT_EQ(snapshot.histograms[0].value.count, 1u);
    EXPECT_EQ(snapshot.histograms[0].value.sum, 10u);
}

}  // namespace
}  // namespace mino::observability
