// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_EXPORTER_H_
#define MINO_OBSERVABILITY_EXPORTER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "mino/common/status.h"
#include "mino/observability/bounded_queue.h"
#include "mino/observability/metrics.h"

namespace mino::observability {

// Export is called only by the export consumer, never by the business path.
class Exporter {
public:
    virtual ~Exporter() = default;
    virtual Status Export(const TelemetrySnapshot& snapshot) noexcept = 0;
};

struct ExportPipelineMetrics {
    CounterMetric* submitted_total = nullptr;
    CounterMetric* dropped_total = nullptr;
    CounterMetric* exported_total = nullptr;
    CounterMetric* failures_total = nullptr;
    GaugeMetric* queue_depth = nullptr;
    GaugeMetric* queue_capacity = nullptr;
};

// Decouples snapshot producers from a possibly blocking/failing Exporter.
// TrySubmit only copies into fixed queue storage and immediately drops on full.
// DrainOne belongs on an independent low-priority consumer thread.
template <size_t Capacity>
class ExportPipeline {
public:
    explicit ExportPipeline(Exporter& exporter,
                            ExportPipelineMetrics* metrics = nullptr) noexcept
        : exporter_(exporter), metrics_(metrics) {
        SetGauge(metrics_ == nullptr ? nullptr : metrics_->queue_capacity,
                 Capacity);
    }

    bool TrySubmit(const TelemetrySnapshot& snapshot) noexcept {
        const size_t pending =
            pending_.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (!queue_.TryPush(snapshot)) {
            const size_t remaining =
                pending_.fetch_sub(1, std::memory_order_acq_rel) - 1;
            dropped_.fetch_add(1, std::memory_order_relaxed);
            Increment(metrics_ == nullptr ? nullptr : metrics_->dropped_total);
            SetGauge(metrics_ == nullptr ? nullptr : metrics_->queue_depth,
                     remaining);
            return false;
        }
        submitted_.fetch_add(1, std::memory_order_relaxed);
        Increment(metrics_ == nullptr ? nullptr : metrics_->submitted_total);
        SetGauge(metrics_ == nullptr ? nullptr : metrics_->queue_depth, pending);
        return true;
    }

    bool DrainOne() noexcept {
        TelemetrySnapshot snapshot;
        if (!queue_.TryPop(&snapshot)) return false;
        const size_t remaining =
            pending_.fetch_sub(1, std::memory_order_acq_rel) - 1;
        SetGauge(metrics_ == nullptr ? nullptr : metrics_->queue_depth,
                 remaining);
        const Status status = exporter_.Export(snapshot);
        if (!status.ok()) {
            failures_.fetch_add(1, std::memory_order_relaxed);
            Increment(metrics_ == nullptr ? nullptr : metrics_->failures_total);
        } else {
            exported_.fetch_add(1, std::memory_order_relaxed);
            Increment(metrics_ == nullptr ? nullptr : metrics_->exported_total);
        }
        return true;
    }

    uint64_t submitted() const noexcept {
        return submitted_.load(std::memory_order_relaxed);
    }
    uint64_t dropped() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }
    uint64_t exported() const noexcept {
        return exported_.load(std::memory_order_relaxed);
    }
    uint64_t failures() const noexcept {
        return failures_.load(std::memory_order_relaxed);
    }

private:
    static void Increment(CounterMetric* metric) noexcept {
        if (metric != nullptr) metric->counter().Increment(0);
    }
    static void SetGauge(GaugeMetric* metric, uint64_t value) noexcept {
        if (metric != nullptr) metric->gauge().Set(value, 0);
    }

    Exporter& exporter_;
    ExportPipelineMetrics* metrics_ = nullptr;
    BoundedQueue<TelemetrySnapshot, Capacity> queue_;
    std::atomic<size_t> pending_{0};
    std::atomic<uint64_t> submitted_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<uint64_t> exported_{0};
    std::atomic<uint64_t> failures_{0};
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_EXPORTER_H_
