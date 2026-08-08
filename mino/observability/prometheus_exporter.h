// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_PROMETHEUS_EXPORTER_H_
#define MINO_OBSERVABILITY_PROMETHEUS_EXPORTER_H_

#include <string_view>

#include "mino/common/status.h"
#include "mino/observability/exporter.h"

namespace mino::observability {

// Implementations may write to an HTTP response, file, or bounded buffer.
// Export runs off the business path; sink failures are propagated to the
// ExportPipeline failure counter.
class TextSink {
public:
    virtual ~TextSink() = default;
    virtual Status Write(std::string_view text) noexcept = 0;
};

class PrometheusTextExporter final : public Exporter {
public:
    explicit PrometheusTextExporter(TextSink& sink) noexcept : sink_(sink) {}
    Status Export(const TelemetrySnapshot& snapshot) noexcept override;

private:
    TextSink& sink_;
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_PROMETHEUS_EXPORTER_H_
