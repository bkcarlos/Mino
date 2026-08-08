// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/clock.h"

namespace mino::observability {

void AtomicClockQuality::Store(const ClockQuality& quality) noexcept {
    sequence_.fetch_add(1, std::memory_order_acq_rel);
    clock_domain_id_.store(quality.clock_domain_id, std::memory_order_relaxed);
    estimated_offset_ns_.store(quality.estimated_offset_ns,
                               std::memory_order_relaxed);
    uncertainty_ns_.store(quality.uncertainty_ns, std::memory_order_relaxed);
    last_sync_time_ns_.store(quality.last_sync_time_ns,
                             std::memory_order_relaxed);
    state_.store(quality.state, std::memory_order_relaxed);
    sequence_.fetch_add(1, std::memory_order_release);
}

bool AtomicClockQuality::TryLoad(ClockQuality* quality,
                                 uint32_t max_attempts) const noexcept {
    if (quality == nullptr) return false;
    for (uint32_t attempt = 0; attempt < max_attempts; ++attempt) {
        const uint64_t before = sequence_.load(std::memory_order_acquire);
        if ((before & 1u) != 0) continue;
        ClockQuality candidate;
        candidate.clock_domain_id =
            clock_domain_id_.load(std::memory_order_relaxed);
        candidate.estimated_offset_ns =
            estimated_offset_ns_.load(std::memory_order_relaxed);
        candidate.uncertainty_ns =
            uncertainty_ns_.load(std::memory_order_relaxed);
        candidate.last_sync_time_ns =
            last_sync_time_ns_.load(std::memory_order_relaxed);
        candidate.state = state_.load(std::memory_order_relaxed);
        const uint64_t after = sequence_.load(std::memory_order_acquire);
        if (before == after) {
            *quality = candidate;
            return true;
        }
    }
    return false;
}

}  // namespace mino::observability
