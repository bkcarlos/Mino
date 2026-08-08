// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/telemetry.h"

namespace mino::observability {
namespace {

uint64_t Mix(uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

bool ValidMode(PerfTelemetryMode mode) noexcept {
    switch (mode) {
        case PerfTelemetryMode::kOff:
        case PerfTelemetryMode::kCountersOnly:
        case PerfTelemetryMode::kSampledLatency:
        case PerfTelemetryMode::kFullDebug:
            return true;
    }
    return false;
}

}  // namespace

uint64_t StableSampleHash(const SampleKey& key) noexcept {
    uint64_t hash = Mix(key.topic_id);
    hash = Mix(hash ^ key.source_identity);
    return Mix(hash ^ key.sequence);
}

bool IsSampled(const SampleKey& key, uint32_t sample_rate_ppm) noexcept {
    if (sample_rate_ppm == 0) return false;
    if (sample_rate_ppm >= 1'000'000) return true;
    return StableSampleHash(key) % 1'000'000ULL < sample_rate_ppm;
}

bool PolicyShouldTrace(const PerfTelemetryPolicy& policy,
                       const SampleKey& key) noexcept {
    if (policy.mode == PerfTelemetryMode::kFullDebug) return true;
    return policy.mode == PerfTelemetryMode::kSampledLatency &&
           IsSampled(key, policy.sample_rate_ppm);
}

bool TelemetryControl::SetPolicy(const PerfTelemetryPolicy& policy) noexcept {
    if (!ValidMode(policy.mode) || policy.sample_rate_ppm > 1'000'000 ||
        (policy.mode == PerfTelemetryMode::kFullDebug &&
         policy.max_events_per_second == 0)) {
        return false;
    }

    uint64_t generation = sequence_.load(std::memory_order_acquire);
    for (;;) {
        if ((generation & 1u) != 0) {
            generation = sequence_.load(std::memory_order_acquire);
            continue;
        }
        if (sequence_.compare_exchange_weak(
                generation, generation + 1, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            break;
        }
    }
    mode_.store(policy.mode, std::memory_order_relaxed);
    sample_rate_ppm_.store(policy.sample_rate_ppm, std::memory_order_relaxed);
    slow_threshold_ns_.store(policy.slow_threshold_ns,
                             std::memory_order_relaxed);
    max_events_per_second_.store(policy.max_events_per_second,
                                 std::memory_order_relaxed);
    sequence_.store(generation + 2, std::memory_order_release);
    return true;
}

bool TelemetryControl::TryLoadPolicy(PerfTelemetryPolicy* policy,
                                     uint32_t max_attempts) const noexcept {
    if (policy == nullptr) return false;
    for (uint32_t attempt = 0; attempt < max_attempts; ++attempt) {
        const uint64_t before = sequence_.load(std::memory_order_acquire);
        if ((before & 1u) != 0) continue;
        PerfTelemetryPolicy candidate;
        candidate.mode = mode_.load(std::memory_order_relaxed);
        candidate.sample_rate_ppm =
            sample_rate_ppm_.load(std::memory_order_relaxed);
        candidate.slow_threshold_ns =
            slow_threshold_ns_.load(std::memory_order_relaxed);
        candidate.max_events_per_second =
            max_events_per_second_.load(std::memory_order_relaxed);
        const uint64_t after = sequence_.load(std::memory_order_acquire);
        if (before == after) {
            *policy = candidate;
            return true;
        }
    }
    return false;
}

PerfTelemetryPolicy TelemetryControl::policy() const noexcept {
    PerfTelemetryPolicy result;
    (void)TryLoadPolicy(&result);
    return result;
}

bool TelemetryControl::CountersEnabled() const noexcept {
    PerfTelemetryPolicy snapshot;
    return TryLoadPolicy(&snapshot) &&
           snapshot.mode != PerfTelemetryMode::kOff;
}

bool TelemetryControl::LatencyEnabled() const noexcept {
    PerfTelemetryPolicy snapshot;
    if (!TryLoadPolicy(&snapshot)) return false;
    return snapshot.mode == PerfTelemetryMode::kSampledLatency ||
           snapshot.mode == PerfTelemetryMode::kFullDebug;
}

bool TelemetryControl::ShouldTrace(const SampleKey& key) const noexcept {
    PerfTelemetryPolicy snapshot;
    return TryLoadPolicy(&snapshot) && PolicyShouldTrace(snapshot, key);
}

}  // namespace mino::observability
