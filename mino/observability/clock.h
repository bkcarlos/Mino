// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_CLOCK_H_
#define MINO_OBSERVABILITY_CLOCK_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "mino/observability/metrics.h"
#include "mino/observability/tracing.h"

namespace mino::observability {

enum class ClockSyncState : uint8_t {
    kUnsynchronized = 0,
    kSynchronizing = 1,
    kSynchronized = 2,
    kDegraded = 3,
};

struct ClockQuality {
    uint32_t clock_domain_id = 0;
    int64_t estimated_offset_ns = 0;
    uint64_t uncertainty_ns = std::numeric_limits<uint64_t>::max();
    uint64_t last_sync_time_ns = 0;
    ClockSyncState state = ClockSyncState::kUnsynchronized;
};

// Single-writer, multi-reader bounded seqlock publication. TryLoad performs at
// most max_attempts and treats contention as uncertain instead of spinning.
class AtomicClockQuality {
public:
    void Store(const ClockQuality& quality) noexcept;
    bool TryLoad(ClockQuality* quality, uint32_t max_attempts = 4) const noexcept;

private:
    std::atomic<uint64_t> sequence_{0};
    std::atomic<uint32_t> clock_domain_id_{0};
    std::atomic<int64_t> estimated_offset_ns_{0};
    std::atomic<uint64_t> uncertainty_ns_{
        std::numeric_limits<uint64_t>::max()};
    std::atomic<uint64_t> last_sync_time_ns_{0};
    std::atomic<ClockSyncState> state_{ClockSyncState::kUnsynchronized};
};

enum class LatencySampleDecision : uint8_t {
    kAccepted = 0,
    kClockUncertain = 1,
    kClockJump = 2,
    kNegativeLatency = 3,
};

// Validates and records cross-node one-way latency. It never compares remote
// monotonic clocks. Clock uncertainty/staleness, local wall-clock jumps, and
// negative samples are counted and excluded from the normal histogram.
template <size_t Shards = kMetricShards>
class CrossNodeLatencyRecorder {
public:
    CrossNodeLatencyRecorder(uint64_t maximum_uncertainty_ns,
                             uint64_t maximum_clock_jump_ns,
                             uint64_t maximum_sync_age_ns) noexcept
        : maximum_uncertainty_ns_(maximum_uncertainty_ns),
          maximum_clock_jump_ns_(maximum_clock_jump_ns),
          maximum_sync_age_ns_(maximum_sync_age_ns) {}

    LatencySampleDecision Record(const TraceContext& context,
                                 uint64_t local_wall_time_ns,
                                 uint64_t local_monotonic_time_ns,
                                 const ClockQuality& quality,
                                 size_t shard) noexcept {
        LocalClockShard& local = local_clocks_[shard % Shards];
        const uint64_t previous_wall =
            local.wall.exchange(local_wall_time_ns, std::memory_order_relaxed);
        const uint64_t previous_monotonic = local.monotonic.exchange(
            local_monotonic_time_ns, std::memory_order_relaxed);
        if (previous_wall != 0 || previous_monotonic != 0) {
            if (local_wall_time_ns < previous_wall ||
                local_monotonic_time_ns < previous_monotonic) {
                clock_jump_.Increment(shard);
                return LatencySampleDecision::kClockJump;
            }
            const uint64_t wall_elapsed = local_wall_time_ns - previous_wall;
            const uint64_t monotonic_elapsed =
                local_monotonic_time_ns - previous_monotonic;
            const uint64_t divergence = wall_elapsed > monotonic_elapsed
                                            ? wall_elapsed - monotonic_elapsed
                                            : monotonic_elapsed - wall_elapsed;
            if (divergence > maximum_clock_jump_ns_) {
                clock_jump_.Increment(shard);
                return LatencySampleDecision::kClockJump;
            }
        }

        const bool stale =
            local_wall_time_ns < quality.last_sync_time_ns ||
            (maximum_sync_age_ns_ != 0 &&
             local_wall_time_ns - quality.last_sync_time_ns >
                 maximum_sync_age_ns_);
        if (quality.state != ClockSyncState::kSynchronized ||
            quality.clock_domain_id != context.clock_domain_id ||
            quality.uncertainty_ns > maximum_uncertainty_ns_ || stale) {
            clock_uncertain_.Increment(shard);
            return LatencySampleDecision::kClockUncertain;
        }
        if (local_wall_time_ns < context.origin_wall_time_ns) {
            negative_latency_.Increment(shard);
            return LatencySampleDecision::kNegativeLatency;
        }

        latency_.Record(local_wall_time_ns - context.origin_wall_time_ns,
                        shard);
        accepted_.Increment(shard);
        return LatencySampleDecision::kAccepted;
    }

    uint64_t accepted() const noexcept { return accepted_.Value(); }
    uint64_t clock_uncertain() const noexcept {
        return clock_uncertain_.Value();
    }
    uint64_t clock_jump() const noexcept { return clock_jump_.Value(); }
    uint64_t negative_latency() const noexcept {
        return negative_latency_.Value();
    }
    LogHistogramSnapshot latency_snapshot() const noexcept {
        return latency_.Snapshot();
    }

private:
    struct alignas(64) LocalClockShard {
        std::atomic<uint64_t> wall{0};
        std::atomic<uint64_t> monotonic{0};
    };

    uint64_t maximum_uncertainty_ns_;
    uint64_t maximum_clock_jump_ns_;
    uint64_t maximum_sync_age_ns_;
    std::array<LocalClockShard, Shards> local_clocks_{};
    ShardedCounter<Shards> accepted_;
    ShardedCounter<Shards> clock_uncertain_;
    ShardedCounter<Shards> clock_jump_;
    ShardedCounter<Shards> negative_latency_;
    ShardedLogHistogram<Shards> latency_;
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_CLOCK_H_
