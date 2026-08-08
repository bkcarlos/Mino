// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_TELEMETRY_H_
#define MINO_OBSERVABILITY_TELEMETRY_H_

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace mino::observability {

enum class PerfTelemetryMode : uint8_t {
    kOff = 0,
    kCountersOnly = 1,
    kSampledLatency = 2,
    kFullDebug = 3,
};

using TelemetryMode = PerfTelemetryMode;

struct PerfTelemetryPolicy {
    PerfTelemetryMode mode = PerfTelemetryMode::kOff;
    uint32_t sample_rate_ppm = 0;
    uint64_t slow_threshold_ns = 0;
    uint32_t max_events_per_second = 0;
};

using TelemetryPolicy = PerfTelemetryPolicy;

struct SampleKey {
    uint64_t topic_id = 0;
    uint64_t source_identity = 0;
    uint64_t sequence = 0;
};

uint64_t StableSampleHash(const SampleKey& key) noexcept;
bool IsSampled(const SampleKey& key, uint32_t sample_rate_ppm) noexcept;
bool PolicyShouldTrace(const PerfTelemetryPolicy& policy,
                       const SampleKey& key) noexcept;

// Atomically publishes a complete immutable policy snapshot. Policy fields are
// atomic to keep the seqlock free of C++ data races; readers either observe one
// complete generation or fail closed to Off after a bounded number of attempts.
class TelemetryControl {
public:
    TelemetryControl() noexcept = default;
    explicit TelemetryControl(const PerfTelemetryPolicy& policy) noexcept {
        (void)SetPolicy(policy);
    }

    bool SetPolicy(const PerfTelemetryPolicy& policy) noexcept;
    bool TryLoadPolicy(PerfTelemetryPolicy* policy,
                       uint32_t max_attempts = 4) const noexcept;
    PerfTelemetryPolicy policy() const noexcept;

    bool CountersEnabled() const noexcept;
    bool LatencyEnabled() const noexcept;
    bool ShouldTrace(const SampleKey& key) const noexcept;

private:
    std::atomic<uint64_t> sequence_{0};
    std::atomic<PerfTelemetryMode> mode_{PerfTelemetryMode::kOff};
    std::atomic<uint32_t> sample_rate_ppm_{0};
    std::atomic<uint64_t> slow_threshold_ns_{0};
    std::atomic<uint32_t> max_events_per_second_{0};
};

static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "Mino telemetry requires lock-free 64-bit atomics");

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_TELEMETRY_H_
