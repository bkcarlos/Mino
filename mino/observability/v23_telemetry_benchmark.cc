// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <string_view>

#include "mino/observability/metrics.h"
#include "mino/observability/tracing.h"

namespace mino::observability {
namespace {

using Clock = std::chrono::steady_clock;
constexpr uint64_t kDefaultIterations = 10'000'000;

struct Result {
    const char* name;
    double nanoseconds_per_operation;
    uint64_t counter;
    uint64_t samples;
    uint64_t dropped;
};

uint64_t Work(uint64_t value) noexcept {
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    return value * 0x2545f4914f6cdd1dULL;
}

Result Run(const char* name, PerfTelemetryPolicy policy,
           uint64_t iterations, bool instrumented) {
    TelemetryControl control(policy);
    TelemetryTracer<1024, 1> tracer(control);
    ShardedCounter<1> counter;
    ShardedLogHistogram<1> histogram;
    uint64_t state = 0x123456789abcdef0ULL;
    uint64_t elapsed_ns = 0;
    constexpr uint64_t kBatchIterations = 512;
    for (uint64_t batch_begin = 0; batch_begin < iterations;
         batch_begin += kBatchIterations) {
        const uint64_t batch_end =
            std::min(iterations, batch_begin + kBatchIterations);
        const auto begin = Clock::now();
        for (uint64_t i = batch_begin; i < batch_end; ++i) {
            state = Work(state + i);
            if (instrumented && control.CountersEnabled()) counter.Increment(0);
            const SampleKey key{7, 11, i};
            const bool trace = instrumented && control.ShouldTrace(key);
            if (trace) {
                const auto sample_begin = Clock::now();
                state = Work(state);
                const auto sample_end = Clock::now();
                const auto elapsed =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        sample_end - sample_begin)
                        .count();
                const uint64_t duration =
                    elapsed > 0 ? static_cast<uint64_t>(elapsed) : 0;
                const uint64_t monotonic_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        sample_end.time_since_epoch())
                        .count());
                histogram.Record(duration, 0);
                const TraceEvent event{
                    .trace_id_high = StableSampleHash(key),
                    .trace_id_low = StableSampleHash({i, 7, 11}),
                    .topic_id = key.topic_id,
                    .monotonic_time_ns = monotonic_ns,
                    .duration_ns = duration,
                    .payload_bytes = 64,
                    .wire_bytes = 128,
                    .flags = kPerfTraceSampled,
                    .stage = TraceStage::kEncodeEnd,
                };
                (void)tracer.TryRecordEvent(key, event, 0, monotonic_ns);
            } else {
                // Every mode executes the same synthetic business work. Only
                // instrumentation and sampled clock/event operations differ.
                state = Work(state);
            }
        }
        const auto end = Clock::now();
        elapsed_ns += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
                .count());
        TraceEvent consumed;
        while (tracer.TryPop(&consumed)) {
        }
    }
    std::printf("sink[%s]=%llu\n", name,
                static_cast<unsigned long long>(state));
    return {name,
            static_cast<double>(elapsed_ns) / static_cast<double>(iterations),
            counter.Value(), histogram.Snapshot().count, tracer.dropped()};
}

uint64_t ParseIterations(int argc, char** argv) {
    if (argc == 1) return kDefaultIterations;
    if (argc != 2) return 0;
    const std::string_view input(argv[1]);
    uint64_t value = 0;
    const auto parsed =
        std::from_chars(input.data(), input.data() + input.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size() ||
        value == 0) {
        return 0;
    }
    return value;
}

}  // namespace
}  // namespace mino::observability

int main(int argc, char** argv) {
    using mino::observability::PerfTelemetryMode;
    using mino::observability::Result;
    using mino::observability::Run;
    const uint64_t iterations = mino::observability::ParseIterations(argc, argv);
    if (iterations == 0) {
        std::fprintf(stderr, "usage: %s [positive-iterations]\n", argv[0]);
        return 2;
    }

    const Result baseline = Run("baseline", {}, iterations, false);
    const Result off = Run("off", {PerfTelemetryMode::kOff, 0, 0, 0},
                           iterations, true);
    const Result counters =
        Run("counters", {PerfTelemetryMode::kCountersOnly, 0, 0, 0},
            iterations, true);
    const Result sampled =
        Run("sampled-1pct",
            {PerfTelemetryMode::kSampledLatency, 10'000, 0, 1'000'000},
            iterations, true);
    const Result full =
        Run("full", {PerfTelemetryMode::kFullDebug, 0, 0, 1'000'000'000},
            iterations, true);

    std::printf("\nV-23 telemetry comparison (%llu operations)\n",
                static_cast<unsigned long long>(iterations));
    std::printf("%-14s %12s %12s %12s %12s %12s\n", "mode", "ns/op",
                "overhead", "counter", "samples", "dropped");
    for (const Result result : {baseline, off, counters, sampled, full}) {
        const double overhead = baseline.nanoseconds_per_operation == 0.0
                                    ? 0.0
                                    : (result.nanoseconds_per_operation /
                                           baseline.nanoseconds_per_operation -
                                       1.0) *
                                          100.0;
        std::printf("%-14s %12.3f %11.2f%% %12llu %12llu %12llu\n",
                    result.name, result.nanoseconds_per_operation, overhead,
                    static_cast<unsigned long long>(result.counter),
                    static_cast<unsigned long long>(result.samples),
                    static_cast<unsigned long long>(result.dropped));
    }
    return 0;
}
