// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_OTLP_EXPORTER_H_
#define MINO_OBSERVABILITY_OTLP_EXPORTER_H_

#include <span>
#include <string_view>

#include "mino/observability/exporter.h"

namespace mino::observability {

// Transactional, non-network OTLP sink. A bounded implementation reserves one
// frame in TryBegin, accepts fragments without waiting, then atomically commits
// or discards it. The observability package performs no network I/O and owns no
// unbounded buffer.
class OtlpJsonSink {
public:
    virtual ~OtlpJsonSink() = default;
    virtual bool TryBegin() noexcept = 0;
    virtual bool TryAppend(std::span<const char> fragment) noexcept = 0;
    virtual void Commit() noexcept = 0;
    virtual void Abort() noexcept = 0;
};

// Encodes the official OTLP/HTTP JSON mapping for cumulative monotonic sums and
// base-2 histograms. Resource attributes are intentionally left to a wrapping
// collector/sink so the hot registry cannot acquire high-cardinality labels.
class OtlpJsonExporter final : public Exporter {
public:
    explicit OtlpJsonExporter(OtlpJsonSink& sink) noexcept : sink_(sink) {}
    Status Export(const TelemetrySnapshot& snapshot) noexcept override;

private:
    bool Append(std::string_view value) noexcept;
    bool AppendUnsigned(uint64_t value) noexcept;
    bool AppendCounter(const CounterSnapshot& counter, uint64_t timestamp_ns,
                       uint64_t start_timestamp_ns,
                       bool leading_comma) noexcept;
    bool AppendGauge(const GaugeSnapshot& gauge, uint64_t timestamp_ns,
                     bool leading_comma) noexcept;
    bool AppendHistogram(const HistogramSnapshot& histogram,
                         uint64_t timestamp_ns, uint64_t start_timestamp_ns,
                         bool leading_comma) noexcept;

    OtlpJsonSink& sink_;
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_OTLP_EXPORTER_H_
