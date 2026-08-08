// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/otlp_exporter.h"
#include "mino/observability/prometheus_exporter.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <span>
#include <string>
#include <string_view>

namespace mino::observability {
namespace {

TelemetrySnapshot MakeSnapshot() {
    MetricRegistry registry(/*process_start_unix_ns=*/1000);
    CounterMetric* counter = nullptr;
    GaugeMetric* gauge = nullptr;
    HistogramMetric* histogram = nullptr;
    EXPECT_TRUE(registry.RegisterCounter("mino_messages_total", &counter).ok());
    EXPECT_TRUE(registry.RegisterGauge("mino_inflight", &gauge).ok());
    EXPECT_TRUE(registry.RegisterHistogram("mino_latency_ns", &histogram).ok());
    counter->counter().Add(3, 0);
    gauge->gauge().Set(2, 0);
    histogram->histogram().Record(0, 0);
    histogram->histogram().Record(4, 1);
    TelemetrySnapshot snapshot;
    registry.TakeSnapshot(123456, &snapshot);
    return snapshot;
}

class StringTextSink final : public TextSink {
public:
    Status Write(std::string_view text) noexcept override {
        output.append(text);
        return Status::Ok();
    }
    std::string output;
};

class TransactionalStringSink final : public OtlpJsonSink {
public:
    explicit TransactionalStringSink(size_t capacity) : capacity_(capacity) {}

    bool TryBegin() noexcept override {
        pending.clear();
        began = true;
        return true;
    }
    bool TryAppend(std::span<const char> fragment) noexcept override {
        if (pending.size() + fragment.size() > capacity_) return false;
        pending.append(fragment.data(), fragment.size());
        return true;
    }
    void Commit() noexcept override {
        output = pending;
        committed = true;
    }
    void Abort() noexcept override {
        pending.clear();
        aborted = true;
    }

    size_t capacity_;
    bool began = false;
    bool committed = false;
    bool aborted = false;
    std::string pending;
    std::string output;
};

class FailingExporter final : public Exporter {
public:
    Status Export(const TelemetrySnapshot&) noexcept override {
        return Status::Error(StatusCode::kUnavailable);
    }
};

TEST(PrometheusTextExporterTest, EncodesCountersAndCumulativeHistogram) {
    StringTextSink sink;
    PrometheusTextExporter exporter(sink);
    ASSERT_TRUE(exporter.Export(MakeSnapshot()).ok());
    EXPECT_NE(sink.output.find("# TYPE mino_messages_total counter\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("mino_messages_total 3\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("# TYPE mino_inflight gauge\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("mino_inflight 2\n"), std::string::npos);
    EXPECT_NE(sink.output.find("mino_latency_ns_bucket{le=\"0\"} 1\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("mino_latency_ns_bucket{le=\"7\"} 2\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("mino_latency_ns_bucket{le=\"+Inf\"} 2\n"),
              std::string::npos);
    EXPECT_NE(sink.output.find("mino_latency_ns_count 2\n"),
              std::string::npos);
}

TEST(OtlpJsonExporterTest, StreamsOtlpJsonWithoutNetworkDependency) {
    TransactionalStringSink sink(32'768);
    OtlpJsonExporter exporter(sink);
    ASSERT_TRUE(exporter.Export(MakeSnapshot()).ok());
    EXPECT_TRUE(sink.began);
    EXPECT_TRUE(sink.committed);
    EXPECT_FALSE(sink.aborted);
    EXPECT_NE(sink.output.find("\"resourceMetrics\""), std::string::npos);
    EXPECT_NE(sink.output.find("\"name\":\"mino_messages_total\""),
              std::string::npos);
    EXPECT_NE(sink.output.find("\"asInt\":\"3\""),
              std::string::npos);
    EXPECT_NE(sink.output.find("\"startTimeUnixNano\":\"1000\""),
              std::string::npos);
    EXPECT_NE(sink.output.find("\"name\":\"mino_inflight\""),
              std::string::npos);
    EXPECT_NE(sink.output.find("\"bucketCounts\""), std::string::npos);
}

TEST(OtlpJsonExporterTest, AbortsWhenBoundedSinkIsFull) {
    TransactionalStringSink sink(32);
    OtlpJsonExporter exporter(sink);
    const Status status = exporter.Export(MakeSnapshot());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(sink.aborted);
    EXPECT_FALSE(sink.committed);
    EXPECT_TRUE(sink.output.empty());
}

TEST(ExportPipelineTest, FullQueueAndExporterFailureNeverReachProducer) {
    FailingExporter exporter;
    ExportPipeline<2> pipeline(exporter);
    const TelemetrySnapshot snapshot = MakeSnapshot();
    EXPECT_TRUE(pipeline.TrySubmit(snapshot));
    EXPECT_TRUE(pipeline.TrySubmit(snapshot));
    EXPECT_FALSE(pipeline.TrySubmit(snapshot));
    EXPECT_EQ(pipeline.submitted(), 2u);
    EXPECT_EQ(pipeline.dropped(), 1u);

    EXPECT_TRUE(pipeline.DrainOne());
    EXPECT_TRUE(pipeline.DrainOne());
    EXPECT_FALSE(pipeline.DrainOne());
    EXPECT_EQ(pipeline.failures(), 2u);
    EXPECT_EQ(pipeline.exported(), 0u);
}

}  // namespace
}  // namespace mino::observability
