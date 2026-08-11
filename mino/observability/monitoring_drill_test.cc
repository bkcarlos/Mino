// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/operational_metrics.h"
#include "mino/observability/prometheus_exporter.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

namespace mino::observability {
namespace {

uint64_t CounterValue(const TelemetrySnapshot& snapshot,
                      std::string_view name) {
    for (size_t index = 0; index < snapshot.counter_count; ++index) {
        if (snapshot.counters[index].name.view() == name) {
            return snapshot.counters[index].value;
        }
    }
    return 0;
}

uint64_t GaugeValue(const TelemetrySnapshot& snapshot,
                    std::string_view name) {
    for (size_t index = 0; index < snapshot.gauge_count; ++index) {
        if (snapshot.gauges[index].name.view() == name) {
            return snapshot.gauges[index].value;
        }
    }
    return 0;
}

bool QueueNearCapacity(const TelemetrySnapshot& snapshot) {
    const uint64_t capacity = GaugeValue(snapshot, "mino_queue_capacity");
    const uint64_t depth = GaugeValue(snapshot, "mino_queue_depth");
    return capacity != 0 && depth * 10 > capacity * 9;
}

class StringSink final : public TextSink {
public:
    Status Write(std::string_view text) noexcept override {
        output.append(text);
        return Status::Ok();
    }
    std::string output;
};

std::filesystem::path Runfile(std::string_view path) {
    const char* srcdir = std::getenv("TEST_SRCDIR");
    const char* workspace = std::getenv("TEST_WORKSPACE");
    return std::filesystem::path(srcdir == nullptr ? "" : srcdir) /
           (workspace == nullptr ? "mino" : workspace) / path;
}

TEST(MonitoringDrillTest, TriggeredSignalsExistInMetricsAndAlertExpressions) {
    MetricRegistry registry(/*process_start_unix_ns=*/1000);
    OperationalMetrics metrics;
    ASSERT_TRUE(RegisterOperationalMetrics(registry, &metrics).ok());

    TelemetrySnapshot healthy;
    registry.TakeSnapshot(/*timestamp_unix_ns=*/1000, &healthy);
    metrics.queue_capacity->gauge().Set(100, 0);
    metrics.queue_depth->gauge().Set(95, 0);
    metrics.queue_dropped_total->counter().Increment(0);
    metrics.slab_allocation_failures_total->counter().Increment(0);
    metrics.otlp_export_failures_total->counter().Increment(0);
    metrics.tls_handshake_failures_total->counter().Add(3, 0);

    TelemetrySnapshot snapshot;
    registry.TakeSnapshot(/*timestamp_unix_ns=*/2000, &snapshot);
    EXPECT_TRUE(QueueNearCapacity(snapshot));
    EXPECT_GT(CounterValue(snapshot, "mino_queue_dropped_total") -
                  CounterValue(healthy, "mino_queue_dropped_total"),
              0u);
    EXPECT_GT(CounterValue(snapshot, "mino_tls_handshake_failures_total") -
                  CounterValue(healthy, "mino_tls_handshake_failures_total"),
              2u);
    StringSink sink;
    PrometheusTextExporter exporter(sink);
    ASSERT_TRUE(exporter.Export(snapshot).ok());
    EXPECT_NE(sink.output.find("mino_queue_capacity 100\n"), std::string::npos);
    EXPECT_NE(sink.output.find("mino_queue_depth 95\n"), std::string::npos);
    EXPECT_NE(sink.output.find("mino_queue_dropped_total 1\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("mino_slab_allocation_failures_total 1\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("mino_otlp_export_failures_total 1\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("mino_tls_handshake_failures_total 3\n"),
              std::string::npos);

    // Alert recovery drill: queue pressure clears immediately, while increase()
    // alerts recover after a quiet evaluation window whose boundary starts at
    // the triggered sample.
    metrics.queue_depth->gauge().Set(20, 0);
    TelemetrySnapshot recovered;
    registry.TakeSnapshot(/*timestamp_unix_ns=*/3000, &recovered);
    EXPECT_FALSE(QueueNearCapacity(recovered));
    EXPECT_EQ(CounterValue(recovered, "mino_queue_dropped_total") -
                  CounterValue(snapshot, "mino_queue_dropped_total"),
              0u);
    EXPECT_EQ(CounterValue(recovered, "mino_tls_handshake_failures_total") -
                  CounterValue(snapshot, "mino_tls_handshake_failures_total"),
              0u);

    std::ifstream rules(Runfile("configs/alerts/mino.rules.yml"));
    ASSERT_TRUE(rules.is_open());
    const std::string text((std::istreambuf_iterator<char>(rules)),
                           std::istreambuf_iterator<char>());
    EXPECT_NE(text.find("MinoQueueDrops"), std::string::npos);
    EXPECT_NE(text.find("increase(mino_queue_dropped_total[5m]) > 0"),
              std::string::npos);
    EXPECT_NE(text.find("MinoSlabAllocationFailures"), std::string::npos);
    EXPECT_NE(text.find("MinoOtlpDropsOrFailures"), std::string::npos);
    EXPECT_NE(text.find("MinoTlsHandshakeFailures"), std::string::npos);
}

}  // namespace
}  // namespace mino::observability
