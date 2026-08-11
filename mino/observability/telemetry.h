// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_TELEMETRY_H_
#define MINO_OBSERVABILITY_TELEMETRY_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

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

inline uint64_t StableSampleMix(uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

inline uint64_t StableSamplePrefix(uint64_t topic_id,
                                   uint64_t source_identity) noexcept {
    return StableSampleMix(StableSampleMix(topic_id) ^ source_identity);
}

constexpr uint64_t kStableSequenceSampleStep = 0x9e3779b97f4a7c15ULL;

// Once topic/source are cached, sequence sampling uses a cheap bijective Weyl
// projection rather than re-running the multi-round identity hash. The odd step
// permutes uint64_t; consecutive sequences can update it with one addition.
inline uint64_t StableSequenceSampleHash(uint64_t prefix,
                                         uint64_t sequence) noexcept {
    return prefix + sequence * kStableSequenceSampleStep;
}

inline uint64_t StableSampleHash(const SampleKey& key) noexcept {
    return StableSequenceSampleHash(
        StableSamplePrefix(key.topic_id, key.source_identity), key.sequence);
}

inline uint32_t StableSampleProjection(uint64_t prefix,
                                       uint64_t sequence) noexcept {
    const uint32_t identity =
        static_cast<uint32_t>(prefix) ^ static_cast<uint32_t>(prefix >> 32);
    return static_cast<uint32_t>(sequence) * 0x9e3779b9u + identity;
}

// A projection cutoff avoids integer division on every sampled-policy decision.
// The mapping remains deterministic across retries and monotonic in ppm.
uint32_t SampleProjectionCutoff(uint32_t sample_rate_ppm) noexcept;
bool IsSampled(const SampleKey& key, uint32_t sample_rate_ppm) noexcept;
bool PolicyShouldTrace(const PerfTelemetryPolicy& policy,
                       const SampleKey& key) noexcept;

constexpr uint8_t kTelemetryCount = 1u << 0;
constexpr uint8_t kTelemetryTrace = 1u << 1;
constexpr uint8_t kTelemetryNeedsTraceDecision = 1u << 7;

// Output-only hot-path value. Decision APIs initialize every field when they
// return kTelemetryTrace/true; callers must not inspect it otherwise. Keeping
// default construction trivial avoids clearing an unused decision per message.
struct TraceDecision {
    uint64_t sample_hash;
    uint64_t trace_id_low;
    uint64_t policy_epoch;
    uint64_t slow_threshold_ns;
    uint32_t max_events_per_second;
    PerfTelemetryMode mode;
};

// Caller-owned, allocation-free policy and sampling cache. A cache belongs to
// one hot path/worker and may be reused across messages. Immediate APIs observe
// one atomic epoch per decision; batch APIs observe it only at Synchronize and
// reload the coherent policy only when that epoch moves.
class TelemetryThreadCache {
public:
    PerfTelemetryMode mode() const noexcept { return policy_.mode; }
    uint64_t policy_epoch() const noexcept { return policy_epoch_; }

private:
    friend class TelemetryControl;
    PerfTelemetryPolicy policy_{};
    uint64_t policy_epoch_ = std::numeric_limits<uint64_t>::max();
    uint32_t sample_cutoff_ = 0;
    uint8_t fast_actions_ = 0;
    uint64_t sample_prefix_ = 0;
    uint64_t topic_id_ = 0;
    uint64_t source_identity_ = 0;
    bool has_sample_prefix_ = false;
    bool sample_all_ = false;
    bool initialized_ = false;
};

static_assert(std::is_trivially_copyable_v<TelemetryThreadCache>);
static_assert(std::is_trivially_copyable_v<TraceDecision>);

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

    bool Refresh(TelemetryThreadCache* cache) const noexcept;

    // Synchronize once at a caller-owned fixed batch boundary. The epoch load
    // remains in the measured data path but is amortized across the batch.
    bool Synchronize(TelemetryThreadCache* cache) const noexcept {
        return LoadCachedPolicy(cache);
    }

    // Fixed-source batch synchronization. In addition to the one policy epoch
    // check, this prepares the stable sampling prefix once for the whole batch;
    // EvaluateSequenceCached then performs no cache mutation or policy load.
    bool Synchronize(TelemetryThreadCache* cache, uint64_t topic_id,
                     uint64_t source_identity) const noexcept {
        if (!LoadCachedPolicy(cache)) return false;
        if (!cache->has_sample_prefix_ || cache->topic_id_ != topic_id ||
            cache->source_identity_ != source_identity) {
            cache->topic_id_ = topic_id;
            cache->source_identity_ = source_identity;
            cache->sample_prefix_ =
                StableSamplePrefix(topic_id, source_identity);
            cache->has_sample_prefix_ = true;
        }
        return true;
    }

    bool CountersEnabled(TelemetryThreadCache* cache) const noexcept {
        return LoadCachedPolicy(cache) &&
               cache->policy_.mode != PerfTelemetryMode::kOff;
    }

    bool LatencyEnabled(TelemetryThreadCache* cache) const noexcept {
        if (!LoadCachedPolicy(cache)) return false;
        return cache->policy_.mode == PerfTelemetryMode::kSampledLatency ||
               cache->policy_.mode == PerfTelemetryMode::kFullDebug;
    }

    bool ShouldTrace(const SampleKey& key, TelemetryThreadCache* cache,
                     TraceDecision* decision) const noexcept {
        return decision != nullptr && LoadCachedPolicy(cache) &&
               BuildTraceDecision(key, cache, decision);
    }

    // Returns kTelemetryCount/kTelemetryTrace using exactly one epoch load.
    // The TraceDecision output is written only when kTelemetryTrace is set.
    uint8_t Evaluate(const SampleKey& key, TelemetryThreadCache* cache,
                     TraceDecision* decision) const noexcept {
        if (!LoadCachedPolicy(cache)) return 0;
        return EvaluateCached(key, cache, decision);
    }

    // Batch fast path after a successful Synchronize(). Policy changes become
    // visible at the next caller-owned boundary; decisions inside one batch use
    // one coherent immutable epoch.
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((always_inline))
#endif
    uint8_t EvaluateCached(const SampleKey& key, TelemetryThreadCache* cache,
                           TraceDecision* decision) const noexcept {
        // Precondition: cache is non-null and Synchronize(cache) succeeded at
        // this batch boundary. Off and Counters return from the predecoded byte
        // without entering the sampling decision path.
        const uint8_t fast_actions = cache->fast_actions_;
        if ((fast_actions & kTelemetryNeedsTraceDecision) == 0) {
            return fast_actions;
        }
        uint8_t actions = kTelemetryCount;
        if (decision != nullptr && BuildTraceDecision(key, cache, decision)) {
            actions |= kTelemetryTrace;
        }
        return actions;
    }

#if defined(__GNUC__) || defined(__clang__)
    __attribute__((always_inline))
#endif
    uint8_t EvaluateSequenceCached(uint64_t sequence,
                                   const TelemetryThreadCache* cache,
                                   TraceDecision* decision) const noexcept {
        const uint8_t fast_actions = cache->fast_actions_;
        if ((fast_actions & kTelemetryNeedsTraceDecision) == 0) {
            return fast_actions;
        }
        uint8_t actions = kTelemetryCount;
        if (decision != nullptr &&
            BuildTraceDecisionForSequence(sequence, cache, decision)) {
            actions |= kTelemetryTrace;
        }
        return actions;
    }

private:
    static bool BuildTraceDecision(const SampleKey& key,
                                   TelemetryThreadCache* cache,
                                   TraceDecision* decision) noexcept {
        const PerfTelemetryMode mode = cache->policy_.mode;
        uint64_t hash = 0;
        if (mode == PerfTelemetryMode::kSampledLatency) {
            if (cache->sample_cutoff_ == 0 && !cache->sample_all_) return false;
            if (!cache->has_sample_prefix_ || cache->topic_id_ != key.topic_id ||
                cache->source_identity_ != key.source_identity) {
                cache->topic_id_ = key.topic_id;
                cache->source_identity_ = key.source_identity;
                cache->sample_prefix_ =
                    StableSamplePrefix(key.topic_id, key.source_identity);
                cache->has_sample_prefix_ = true;
            }
            const uint32_t projection = StableSampleProjection(
                cache->sample_prefix_, key.sequence);
            if (!cache->sample_all_ &&
                projection >= cache->sample_cutoff_) {
                return false;
            }
            // The 64-bit trace identity is needed only for accepted samples.
            hash = StableSequenceSampleHash(cache->sample_prefix_,
                                            key.sequence);
        } else if (mode == PerfTelemetryMode::kFullDebug) {
            hash = StableSampleHash(key);
        } else {
            return false;
        }
        decision->sample_hash = hash;
        decision->trace_id_low = StableSampleMix(hash ^ 0xd1b54a32d192ed03ULL);
        decision->policy_epoch = cache->policy_epoch_;
        decision->slow_threshold_ns = cache->policy_.slow_threshold_ns;
        decision->max_events_per_second =
            cache->policy_.max_events_per_second;
        decision->mode = mode;
        return true;
    }

    static bool BuildTraceDecisionForSequence(
        uint64_t sequence, const TelemetryThreadCache* cache,
        TraceDecision* decision) noexcept {
        const PerfTelemetryMode mode = cache->policy_.mode;
        if (mode == PerfTelemetryMode::kSampledLatency) {
            if (cache->sample_cutoff_ == 0 && !cache->sample_all_) return false;
            const uint32_t projection =
                StableSampleProjection(cache->sample_prefix_, sequence);
            if (!cache->sample_all_ && projection >= cache->sample_cutoff_) {
                return false;
            }
        } else if (mode != PerfTelemetryMode::kFullDebug) {
            return false;
        }
        const uint64_t hash =
            StableSequenceSampleHash(cache->sample_prefix_, sequence);
        decision->sample_hash = hash;
        decision->trace_id_low = StableSampleMix(hash ^ 0xd1b54a32d192ed03ULL);
        decision->policy_epoch = cache->policy_epoch_;
        decision->slow_threshold_ns = cache->policy_.slow_threshold_ns;
        decision->max_events_per_second =
            cache->policy_.max_events_per_second;
        decision->mode = mode;
        return true;
    }

    bool LoadCachedPolicy(TelemetryThreadCache* cache) const noexcept {
        if (cache == nullptr) return false;
        const uint64_t epoch = sequence_.load(std::memory_order_acquire);
        if (cache->initialized_ && epoch == cache->policy_epoch_) return true;
        // A writer owns an odd epoch. Keep using an already coherent snapshot;
        // a new caller fails closed until publication completes.
        if ((epoch & 1u) != 0) return cache->initialized_;
        return Refresh(cache);
    }

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
