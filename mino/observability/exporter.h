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

// Decouples snapshot producers from a possibly blocking/failing Exporter.
// TrySubmit only copies into fixed queue storage and immediately drops on full.
// DrainOne belongs on an independent low-priority consumer thread.
template <size_t Capacity>
class ExportPipeline {
public:
    explicit ExportPipeline(Exporter& exporter) noexcept : exporter_(exporter) {}

    bool TrySubmit(const TelemetrySnapshot& snapshot) noexcept {
        if (!queue_.TryPush(snapshot)) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        submitted_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool DrainOne() noexcept {
        TelemetrySnapshot snapshot;
        if (!queue_.TryPop(&snapshot)) return false;
        const Status status = exporter_.Export(snapshot);
        if (!status.ok()) {
            failures_.fetch_add(1, std::memory_order_relaxed);
        } else {
            exported_.fetch_add(1, std::memory_order_relaxed);
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
    Exporter& exporter_;
    BoundedQueue<TelemetrySnapshot, Capacity> queue_;
    std::atomic<uint64_t> submitted_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<uint64_t> exported_{0};
    std::atomic<uint64_t> failures_{0};
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_EXPORTER_H_
