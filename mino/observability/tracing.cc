// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/tracing.h"

namespace mino::observability {

PerfTraceContext MakeTraceContext(const SampleKey& key, uint32_t sample_flags,
                                  uint32_t clock_domain_id,
                                  uint64_t origin_wall_time_ns,
                                  uint64_t origin_monotonic_ns) noexcept {
    const uint64_t hash = StableSampleHash(key);
    const TraceDecision decision{
        .sample_hash = hash,
        .trace_id_low = StableSampleMix(hash ^ 0xd1b54a32d192ed03ULL),
        .policy_epoch = 0,
        .slow_threshold_ns = 0,
        .max_events_per_second = 0,
        .mode = PerfTelemetryMode::kSampledLatency,
    };
    return MakeTraceContext(key, decision, sample_flags, clock_domain_id,
                            origin_wall_time_ns, origin_monotonic_ns);
}

PerfTraceContext MakeTraceContext(const SampleKey&,
                                  const TraceDecision& decision,
                                  uint32_t sample_flags,
                                  uint32_t clock_domain_id,
                                  uint64_t origin_wall_time_ns,
                                  uint64_t origin_monotonic_ns) noexcept {
    PerfTraceContext context;
    context.trace_id_high = decision.sample_hash;
    context.trace_id_low = decision.trace_id_low != 0
                               ? decision.trace_id_low
                               : StableSampleMix(decision.sample_hash ^
                                                 0xd1b54a32d192ed03ULL);
    context.sample_flags = sample_flags;
    context.clock_domain_id = clock_domain_id;
    context.origin_wall_time_ns = origin_wall_time_ns;
    context.origin_monotonic_ns = origin_monotonic_ns;
    return context;
}

}  // namespace mino::observability
