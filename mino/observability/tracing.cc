// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/tracing.h"

namespace mino::observability {

PerfTraceContext MakeTraceContext(const SampleKey& key, uint32_t sample_flags,
                                  uint32_t clock_domain_id,
                                  uint64_t origin_wall_time_ns,
                                  uint64_t origin_monotonic_ns) noexcept {
    PerfTraceContext context;
    context.trace_id_high = StableSampleHash(key);
    context.trace_id_low = StableSampleHash(
        SampleKey{key.sequence, key.topic_id, key.source_identity});
    context.sample_flags = sample_flags;
    context.clock_domain_id = clock_domain_id;
    context.origin_wall_time_ns = origin_wall_time_ns;
    context.origin_monotonic_ns = origin_monotonic_ns;
    return context;
}

}  // namespace mino::observability
