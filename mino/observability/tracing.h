// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_TRACING_H_
#define MINO_OBSERVABILITY_TRACING_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "mino/observability/bounded_queue.h"
#include "mino/observability/telemetry.h"

namespace mino::observability {

constexpr uint32_t kPerfTraceSampled = 1u << 0;
constexpr uint32_t kPerfTraceSlow = 1u << 1;

struct PerfTraceContext {
    uint64_t trace_id_high = 0;
    uint64_t trace_id_low = 0;
    uint32_t sample_flags = 0;
    uint32_t clock_domain_id = 0;
    uint64_t origin_wall_time_ns = 0;
    uint64_t origin_monotonic_ns = 0;
};

using TraceContext = PerfTraceContext;
static_assert(sizeof(PerfTraceContext) == 40);
static_assert(std::is_trivially_copyable_v<PerfTraceContext>);

PerfTraceContext MakeTraceContext(const SampleKey& key, uint32_t sample_flags,
                                  uint32_t clock_domain_id,
                                  uint64_t origin_wall_time_ns,
                                  uint64_t origin_monotonic_ns) noexcept;
PerfTraceContext MakeTraceContext(const SampleKey& key,
                                  const TraceDecision& decision,
                                  uint32_t sample_flags,
                                  uint32_t clock_domain_id,
                                  uint64_t origin_wall_time_ns,
                                  uint64_t origin_monotonic_ns) noexcept;

enum class TraceStage : uint16_t {
    kAllocateBegin,
    kAllocateEnd,
    kBuildEnd,
    kRingReserve,
    kReadyCommit,
    kSubscriberAcquire,
    kCallbackBegin,
    kCallbackEnd,
    kBridgeBorrow,
    kEncodeEnd,
    kSendQueueEnter,
    kSocketWriteComplete,
    kRemoteFrameComplete,
    kDecodeEnd,
    kRemoteReadyCommit,
    kRemoteSubscriberAcquire,
    kBridgeReconnect,
};

enum class MessageOrigin : uint8_t {
    kLive = 0,
    kReplay = 1,
};

// Process-local bounded PerfEvent. IDs are numeric and controlled; no strings,
// pointers, ownership, or dynamic labels enter the sidecar ABI.
struct TraceEvent {
    uint64_t trace_id_high = 0;
    uint64_t trace_id_low = 0;
    uint64_t topic_id = 0;
    uint64_t monotonic_time_ns = 0;
    uint64_t wall_time_ns = 0;
    uint64_t duration_ns = 0;
    uint32_t clock_domain_id = 0;
    uint32_t hop_id = 0;
    uint32_t attempt_id = 0;
    uint32_t component_instance = 0;
    uint32_t subscriber_generation = 0;
    uint32_t payload_bytes = 0;
    uint32_t wire_bytes = 0;
    uint32_t flags = 0;
    TraceStage stage = TraceStage::kAllocateBegin;
    MessageOrigin message_origin = MessageOrigin::kLive;
    uint8_t reserved = 0;
};

static_assert(std::is_trivially_copyable_v<TraceEvent>);

template <size_t Capacity>
class SidecarTraceQueue {
public:
    bool TryPush(const TraceEvent& event) noexcept {
        if (!queue_.TryPush(event)) {
            RecordDrop();
            return false;
        }
        accepted_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool TryPop(TraceEvent* event) noexcept { return queue_.TryPop(event); }

    void RecordDrop() noexcept {
        dropped_.fetch_add(1, std::memory_order_relaxed);
    }

    uint64_t accepted() const noexcept {
        return accepted_.load(std::memory_order_relaxed);
    }
    uint64_t dropped() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }
    static constexpr size_t capacity() noexcept { return Capacity; }

private:
    BoundedQueue<TraceEvent, Capacity> queue_;
    std::atomic<uint64_t> accepted_{0};
    std::atomic<uint64_t> dropped_{0};
};

// Process-wide event-rate limiter. The template parameter is retained for API
// compatibility but does not multiply the configured total budget. Epoch and
// count share one CAS word, avoiding reset races at second boundaries.
template <size_t Shards = 32>
class EventRateLimiter {
    static_assert(Shards > 0);

public:
    bool TryAcquire(uint32_t limit_per_second, uint64_t now_ns,
                    size_t shard) noexcept {
        (void)shard;
        if (limit_per_second == 0) return true;
        const uint32_t epoch =
            static_cast<uint32_t>(now_ns / 1'000'000'000ULL);
        uint64_t observed = state_.load(std::memory_order_relaxed);
        for (;;) {
            const uint32_t observed_epoch =
                static_cast<uint32_t>(observed >> 32);
            const uint32_t count = static_cast<uint32_t>(observed);
            uint64_t desired = 0;
            if (observed_epoch != epoch) {
                desired = (static_cast<uint64_t>(epoch) << 32) | 1u;
            } else {
                if (count >= limit_per_second) return false;
                desired = (static_cast<uint64_t>(epoch) << 32) |
                          static_cast<uint64_t>(count + 1);
            }
            if (state_.compare_exchange_weak(
                    observed, desired, std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                return true;
            }
        }
    }

private:
    std::atomic<uint64_t> state_{0};
};

// Optional production instrumentation seam. Implementations must remain
// allocation-free and non-blocking in TryRecordEvent.
class TraceEventSink {
public:
    virtual ~TraceEventSink() = default;
    virtual bool TryRecordEvent(const SampleKey& key, const TraceEvent& event,
                                size_t shard, uint64_t now_ns) noexcept = 0;
};

// Combines one coherent mode/sampling/rate policy snapshot with a bounded
// sidecar. For SampledLatency, a non-zero slow threshold retains only events
// whose completed duration reaches the threshold (or are explicitly slow).
template <size_t Capacity, size_t RateShards = 32>
class TelemetryTracer final : public TraceEventSink {
public:
    explicit TelemetryTracer(const TelemetryControl& control) noexcept
        : control_(control) {}
    TelemetryTracer(TelemetryControl&&) = delete;

    bool TryRecordEvent(const SampleKey& key, const TraceEvent& event,
                        size_t shard, uint64_t now_ns) noexcept override {
        PerfTelemetryPolicy policy;
        if (!control_.TryLoadPolicy(&policy) ||
            !PolicyShouldTrace(policy, key)) {
            return false;
        }
        const uint64_t hash = StableSampleHash(key);
        const TraceDecision decision{
            .sample_hash = hash,
            .trace_id_low =
                StableSampleMix(hash ^ 0xd1b54a32d192ed03ULL),
            .policy_epoch = 0,
            .slow_threshold_ns = policy.slow_threshold_ns,
            .max_events_per_second = policy.max_events_per_second,
            .mode = policy.mode,
        };
        return TryRecordSampledEvent(decision, event, shard, now_ns);
    }

    // Fast path after TelemetryControl::ShouldTrace has already produced one
    // coherent per-message decision. It intentionally does not reload policy or
    // hash the key again; a message keeps the epoch selected at publication.
    bool TryRecordSampledEvent(const TraceDecision& decision,
                               const TraceEvent& event, size_t shard,
                               uint64_t now_ns) noexcept {
        if (decision.mode == PerfTelemetryMode::kSampledLatency &&
            decision.slow_threshold_ns != 0 &&
            event.duration_ns < decision.slow_threshold_ns &&
            (event.flags & kPerfTraceSlow) == 0) {
            return false;
        }
        if (decision.mode != PerfTelemetryMode::kSampledLatency &&
            decision.mode != PerfTelemetryMode::kFullDebug) {
            return false;
        }
        if (!limiter_.TryAcquire(decision.max_events_per_second, now_ns,
                                 shard)) {
            sidecar_.RecordDrop();
            return false;
        }
        return sidecar_.TryPush(event);
    }

    bool TryPop(TraceEvent* event) noexcept { return sidecar_.TryPop(event); }
    uint64_t accepted() const noexcept { return sidecar_.accepted(); }
    uint64_t dropped() const noexcept { return sidecar_.dropped(); }

private:
    const TelemetryControl& control_;
    SidecarTraceQueue<Capacity> sidecar_;
    EventRateLimiter<RateShards> limiter_;
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_TRACING_H_
