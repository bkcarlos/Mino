// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/prometheus_exporter.h"

#include <algorithm>
#include <cstdio>
#include <string_view>

namespace mino::observability {
namespace {

Status WriteBuffer(TextSink& sink, const char* buffer, int length,
                   size_t capacity) noexcept {
    if (length < 0 || static_cast<size_t>(length) >= capacity) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
    return sink.Write(std::string_view(buffer, static_cast<size_t>(length)));
}

Status WriteType(TextSink& sink, const MetricName& name,
                 const char* type) noexcept {
    char buffer[160];
    const int length = std::snprintf(buffer, sizeof(buffer), "# TYPE %s %s\n",
                                     name.c_str(), type);
    return WriteBuffer(sink, buffer, length, sizeof(buffer));
}

Status WriteValue(TextSink& sink, const MetricName& name, const char* suffix,
                  uint64_t value) noexcept {
    char buffer[160];
    const int length = std::snprintf(
        buffer, sizeof(buffer), "%s%s %llu\n", name.c_str(), suffix,
        static_cast<unsigned long long>(value));
    return WriteBuffer(sink, buffer, length, sizeof(buffer));
}

Status WriteBucket(TextSink& sink, const MetricName& name,
                   const char* boundary, uint64_t value) noexcept {
    char buffer[192];
    const int length = std::snprintf(
        buffer, sizeof(buffer), "%s_bucket{le=\"%s\"} %llu\n", name.c_str(),
        boundary, static_cast<unsigned long long>(value));
    return WriteBuffer(sink, buffer, length, sizeof(buffer));
}

}  // namespace

Status PrometheusTextExporter::Export(
    const TelemetrySnapshot& snapshot) noexcept {
    const size_t counter_count =
        std::min(snapshot.counter_count, snapshot.counters.size());
    for (size_t i = 0; i < counter_count; ++i) {
        const CounterSnapshot& counter = snapshot.counters[i];
        Status status = WriteType(sink_, counter.name, "counter");
        if (!status.ok()) return status;
        status = WriteValue(sink_, counter.name, "", counter.value);
        if (!status.ok()) return status;
    }

    const size_t gauge_count =
        std::min(snapshot.gauge_count, snapshot.gauges.size());
    for (size_t i = 0; i < gauge_count; ++i) {
        const GaugeSnapshot& gauge = snapshot.gauges[i];
        Status status = WriteType(sink_, gauge.name, "gauge");
        if (!status.ok()) return status;
        status = WriteValue(sink_, gauge.name, "", gauge.value);
        if (!status.ok()) return status;
    }

    const size_t histogram_count =
        std::min(snapshot.histogram_count, snapshot.histograms.size());
    for (size_t i = 0; i < histogram_count; ++i) {
        const HistogramSnapshot& histogram = snapshot.histograms[i];
        Status status = WriteType(sink_, histogram.name, "histogram");
        if (!status.ok()) return status;
        uint64_t cumulative = 0;
        for (size_t bucket = 0; bucket < 64; ++bucket) {
            cumulative += histogram.value.buckets[bucket];
            char boundary[32];
            const int boundary_length = std::snprintf(
                boundary, sizeof(boundary), "%llu",
                static_cast<unsigned long long>(
                    ShardedLogHistogram<>::UpperBound(bucket)));
            if (boundary_length < 0 ||
                static_cast<size_t>(boundary_length) >= sizeof(boundary)) {
                return Status::Error(StatusCode::kInternal);
            }
            status = WriteBucket(sink_, histogram.name, boundary, cumulative);
            if (!status.ok()) return status;
        }
        cumulative += histogram.value.buckets[64];
        status = WriteBucket(sink_, histogram.name, "+Inf", cumulative);
        if (!status.ok()) return status;
        status = WriteValue(sink_, histogram.name, "_sum",
                            histogram.value.sum);
        if (!status.ok()) return status;
        status = WriteValue(sink_, histogram.name, "_count",
                            histogram.value.count);
        if (!status.ok()) return status;
    }
    return Status::Ok();
}

}  // namespace mino::observability
