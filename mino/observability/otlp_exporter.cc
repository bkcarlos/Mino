// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/otlp_exporter.h"

#include <algorithm>
#include <cstdio>
#include <span>
#include <string_view>

namespace mino::observability {

bool OtlpJsonExporter::Append(std::string_view value) noexcept {
    return sink_.TryAppend(std::span<const char>(value.data(), value.size()));
}

bool OtlpJsonExporter::AppendUnsigned(uint64_t value) noexcept {
    char buffer[32];
    const int length = std::snprintf(buffer, sizeof(buffer), "%llu",
                                     static_cast<unsigned long long>(value));
    return length >= 0 && static_cast<size_t>(length) < sizeof(buffer) &&
           Append(std::string_view(buffer, static_cast<size_t>(length)));
}

bool OtlpJsonExporter::AppendCounter(const CounterSnapshot& counter,
                                     uint64_t timestamp_ns,
                                     uint64_t start_timestamp_ns,
                                     bool leading_comma) noexcept {
    if (leading_comma && !Append(",")) return false;
    if (!Append("{\"name\":\"") || !Append(counter.name.view()) ||
        !Append("\",\"sum\":{\"aggregationTemporality\":2,"
                "\"isMonotonic\":true,\"dataPoints\":[{"
                "\"timeUnixNano\":\"")) {
        return false;
    }
    if (!AppendUnsigned(timestamp_ns) ||
        !Append("\",\"startTimeUnixNano\":\"") ||
        !AppendUnsigned(start_timestamp_ns) || !Append("\",\"asInt\":\"") ||
        !AppendUnsigned(counter.value) || !Append("\"}]}}")) {
        return false;
    }
    return true;
}

bool OtlpJsonExporter::AppendGauge(const GaugeSnapshot& gauge,
                                   uint64_t timestamp_ns,
                                   bool leading_comma) noexcept {
    if (leading_comma && !Append(",")) return false;
    if (!Append("{\"name\":\"") || !Append(gauge.name.view()) ||
        !Append("\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"")) {
        return false;
    }
    return AppendUnsigned(timestamp_ns) && Append("\",\"asInt\":\"") &&
           AppendUnsigned(gauge.value) && Append("\"}]}}");
}

bool OtlpJsonExporter::AppendHistogram(const HistogramSnapshot& histogram,
                                       uint64_t timestamp_ns,
                                       uint64_t start_timestamp_ns,
                                       bool leading_comma) noexcept {
    if (leading_comma && !Append(",")) return false;
    if (!Append("{\"name\":\"") || !Append(histogram.name.view()) ||
        !Append("\",\"histogram\":{\"aggregationTemporality\":2,"
                "\"dataPoints\":[{\"timeUnixNano\":\"")) {
        return false;
    }
    if (!AppendUnsigned(timestamp_ns) ||
        !Append("\",\"startTimeUnixNano\":\"") ||
        !AppendUnsigned(start_timestamp_ns) || !Append("\",\"count\":\"") ||
        !AppendUnsigned(histogram.value.count) || !Append("\",\"sum\":") ||
        !AppendUnsigned(histogram.value.sum) ||
        !Append(",\"bucketCounts\":[")) {
        return false;
    }
    for (size_t bucket = 0; bucket < kLogHistogramBuckets; ++bucket) {
        if (bucket != 0 && !Append(",")) return false;
        if (!Append("\"") ||
            !AppendUnsigned(histogram.value.buckets[bucket]) ||
            !Append("\"")) {
            return false;
        }
    }
    if (!Append("],\"explicitBounds\":[")) return false;
    for (size_t bucket = 0; bucket < 64; ++bucket) {
        if (bucket != 0 && !Append(",")) return false;
        if (!AppendUnsigned(ShardedLogHistogram<>::UpperBound(bucket))) {
            return false;
        }
    }
    return Append("]}]}}");
}

Status OtlpJsonExporter::Export(const TelemetrySnapshot& snapshot) noexcept {
    if (!sink_.TryBegin()) {
        return Status::Error(StatusCode::kWouldBlock);
    }
    bool ok = Append("{\"resourceMetrics\":[{\"scopeMetrics\":[{"
                     "\"scope\":{\"name\":\"mino\"},\"metrics\":[");
    bool has_metric = false;
    const size_t counter_count =
        std::min(snapshot.counter_count, snapshot.counters.size());
    for (size_t i = 0; ok && i < counter_count; ++i) {
        ok = AppendCounter(snapshot.counters[i], snapshot.timestamp_unix_ns,
                           snapshot.process_start_unix_ns, has_metric);
        has_metric = true;
    }
    const size_t gauge_count =
        std::min(snapshot.gauge_count, snapshot.gauges.size());
    for (size_t i = 0; ok && i < gauge_count; ++i) {
        ok = AppendGauge(snapshot.gauges[i], snapshot.timestamp_unix_ns,
                         has_metric);
        has_metric = true;
    }
    const size_t histogram_count =
        std::min(snapshot.histogram_count, snapshot.histograms.size());
    for (size_t i = 0; ok && i < histogram_count; ++i) {
        ok = AppendHistogram(snapshot.histograms[i],
                             snapshot.timestamp_unix_ns,
                             snapshot.process_start_unix_ns, has_metric);
        has_metric = true;
    }
    if (ok) ok = Append("]}]}]}");
    if (!ok) {
        sink_.Abort();
        return Status::Error(StatusCode::kResourceExhausted);
    }
    sink_.Commit();
    return Status::Ok();
}

}  // namespace mino::observability
