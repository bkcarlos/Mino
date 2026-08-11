// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "mino/observability/metrics.h"
#include "mino/observability/tracing.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino::observability {
namespace {

using Clock = std::chrono::steady_clock;
constexpr uint64_t kDefaultIterations = 1'000'000;
constexpr uint32_t kDefaultRounds = 7;
constexpr uint32_t kDefaultProcesses = 5;
constexpr uint32_t kMaxRounds = 32;
constexpr uint64_t kBatchIterations = 256;
constexpr uint64_t kChannelCapacity = 256;
constexpr uint64_t kPayloadBytes = 64;
constexpr uint64_t kWireBytes = 128;
constexpr uint32_t kSampleRatePpm = 10'000;
constexpr size_t kModeCount = 5;
constexpr size_t kWorkloadCount = 2;

std::atomic<uint64_t> g_sink{0};

enum class Mode : uint8_t {
    kCompileOffBaseline = 0,
    kRuntimeOff = 1,
    kCounters = 2,
    kSampled = 3,
    kFull = 4,
};

enum class Workload : uint8_t {
    kPublish = 0,
    kMicroOp = 1,
};

constexpr std::array<const char*, kModeCount> kModeNames = {
    "compile-off", "runtime-off", "counters", "sampled-1pct", "full-debug"};
constexpr std::array<const char*, kWorkloadCount> kWorkloadNames = {
    "real-publish", "micro-op"};

struct Options {
    uint64_t iterations = kDefaultIterations;
    uint32_t rounds = kDefaultRounds;
    uint32_t processes = kDefaultProcesses;
    std::array<char, 1024> json_path{};
};

struct CounterAccumulator {
    uint64_t messages = 0;
    uint64_t payload_bytes = 0;
    uint64_t wire_bytes = 0;
};

struct Observation {
    double ns_per_operation = 0;
    uint64_t messages = 0;
    uint64_t payload_bytes = 0;
    uint64_t wire_bytes = 0;
    uint64_t samples = 0;
    uint64_t accepted = 0;
    uint64_t dropped = 0;
    uint64_t sink = 0;
    bool valid = false;
};

struct ProcessReport {
    Observation observations[kMaxRounds][kWorkloadCount][kModeCount]{};
    double paired_baseline_ns[kMaxRounds][kWorkloadCount][kModeCount]{};
    double clock_pair_ns = 0;
    double noop_ns = 0;
    int64_t process_id = -1;
    int32_t pinned_cpu = -1;
    uint32_t rounds = 0;
    bool valid = false;
};

struct Summary {
    double mean = 0;
    double lower95 = 0;
    double upper95 = 0;
};

uint64_t NowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch())
            .count());
}

uint64_t Work(uint64_t value) noexcept {
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    return value * 0x2545f4914f6cdd1dULL;
}

void DoNotOptimize(uint64_t value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    asm volatile("" : : "r,m"(value) : "memory");
#else
    g_sink.fetch_xor(value, std::memory_order_relaxed);
#endif
}

PerfTelemetryPolicy PolicyFor(Mode mode) noexcept {
    switch (mode) {
        case Mode::kCompileOffBaseline:
        case Mode::kRuntimeOff:
            return {PerfTelemetryMode::kOff, 0, 0, 0};
        case Mode::kCounters:
            return {PerfTelemetryMode::kCountersOnly, 0, 0, 0};
        case Mode::kSampled:
            // Keep the limiter above the run rate so every selected sample pays
            // Histogram + Sidecar insertion instead of measuring cheap drops.
            return {PerfTelemetryMode::kSampledLatency, kSampleRatePpm, 0,
                    std::numeric_limits<uint32_t>::max()};
        case Mode::kFull:
            return {PerfTelemetryMode::kFullDebug, 0, 0,
                    std::numeric_limits<uint32_t>::max()};
    }
    return {};
}

class Instrumentation {
public:
    explicit Instrumentation(Mode mode)
        : control_(PolicyFor(mode)), tracer_(control_),
          message_local_(messages_.BindLocalBatch(0)),
          payload_local_(payload_bytes_.BindLocalBatch(0)),
          wire_local_(wire_bytes_.BindLocalBatch(0)) {}

    bool Synchronize() noexcept {
        return control_.Synchronize(&cache_, /*topic_id=*/7,
                                    /*source_identity=*/11);
    }
    PerfTelemetryMode mode() const noexcept { return cache_.mode(); }

#if defined(__GNUC__) || defined(__clang__)
    __attribute__((always_inline))
#endif
    uint8_t Evaluate(uint64_t sequence, TraceDecision* decision) noexcept {
        return control_.EvaluateSequenceCached(sequence, &cache_, decision);
    }

    void BeginTrace(const SampleKey& key, const TraceDecision& decision,
                    PerfTraceContext* context, uint64_t* begin_ns) noexcept {
        *begin_ns = NowNs();
        *context = MakeTraceContext(key, decision, kPerfTraceSampled,
                                    /*clock_domain_id=*/1,
                                    /*origin_wall_time_ns=*/0, *begin_ns);
        DoNotOptimize(context->trace_id_high ^ context->trace_id_low);
    }

    void EndTrace(const SampleKey& key, const TraceDecision& decision,
                  const PerfTraceContext& context,
                  uint64_t begin_ns) noexcept {
        const uint64_t end_ns = NowNs();
        const uint64_t duration_ns = end_ns - begin_ns;
        histogram_.Record(duration_ns, 0);
        const TraceEvent event{
            .trace_id_high = context.trace_id_high,
            .trace_id_low = context.trace_id_low,
            .topic_id = key.topic_id,
            .monotonic_time_ns = end_ns,
            .duration_ns = duration_ns,
            .payload_bytes = static_cast<uint32_t>(kPayloadBytes),
            .wire_bytes = static_cast<uint32_t>(kWireBytes),
            .flags = kPerfTraceSampled,
            .stage = TraceStage::kReadyCommit,
        };
        (void)tracer_.TryRecordSampledEvent(decision, event, 0, end_ns);
    }



    void FlushCounters(uint64_t messages) noexcept {
        if (messages == 0) return;
        message_local_.Accumulate(messages);
        payload_local_.Accumulate(messages * kPayloadBytes);
        wire_local_.Accumulate(messages * kWireBytes);
        message_local_.Flush();
        payload_local_.Flush();
        wire_local_.Flush();
    }

    void Drain() noexcept {
        TraceEvent event;
        while (tracer_.TryPop(&event)) sink_ ^= event.trace_id_low;
    }

    uint64_t messages() const noexcept { return messages_.Value(); }
    uint64_t payload_bytes() const noexcept { return payload_bytes_.Value(); }
    uint64_t wire_bytes() const noexcept { return wire_bytes_.Value(); }
    uint64_t samples() const noexcept { return histogram_.Snapshot().count; }
    uint64_t accepted() const noexcept { return tracer_.accepted(); }
    uint64_t dropped() const noexcept { return tracer_.dropped(); }
    uint64_t sink() const noexcept { return sink_; }

private:
    TelemetryControl control_;
    TelemetryThreadCache cache_;
    TelemetryTracer<1024, 1> tracer_;
    ShardedCounter<1> messages_;
    ShardedCounter<1> payload_bytes_;
    ShardedCounter<1> wire_bytes_;
    ShardedCounter<1>::LocalBatch message_local_;
    ShardedCounter<1>::LocalBatch payload_local_;
    ShardedCounter<1>::LocalBatch wire_local_;
    ShardedLogHistogram<1> histogram_;
    uint64_t sink_ = 0;
};

struct FreeDeleter {
    void operator()(std::byte* pointer) const noexcept { std::free(pointer); }
};
using AlignedBytes = std::unique_ptr<std::byte, FreeDeleter>;

AlignedBytes AllocateChannelMemory() {
    const size_t bytes = static_cast<size_t>(
        SpscChannel::RequiredSize(kChannelCapacity));
    void* memory = nullptr;
#if defined(_MSC_VER)
    memory = _aligned_malloc(bytes, SpscChannel::kCacheLineSize);
#else
    if (posix_memalign(&memory, SpscChannel::kCacheLineSize, bytes) != 0) {
        memory = nullptr;
    }
#endif
    if (memory != nullptr) std::memset(memory, 0, bytes);
    return AlignedBytes(static_cast<std::byte*>(memory));
}

bool ValidTelemetryResult(Mode mode, uint64_t iterations, uint64_t messages,
                          uint64_t payload_bytes, uint64_t wire_bytes,
                          uint64_t samples, uint64_t accepted,
                          uint64_t dropped) noexcept {
    const uint64_t expected_messages =
        mode == Mode::kCompileOffBaseline || mode == Mode::kRuntimeOff
            ? 0
            : iterations;
    bool samples_valid = samples == 0;
    if (mode == Mode::kSampled) {
        const uint64_t minimum = iterations * 95 / 10'000;
        const uint64_t maximum = iterations * 105 / 10'000 + 1;
        samples_valid = samples >= minimum && samples <= maximum;
    } else if (mode == Mode::kFull) {
        samples_valid = samples == iterations;
    }
    return messages == expected_messages &&
           payload_bytes == expected_messages * kPayloadBytes &&
           wire_bytes == expected_messages * kWireBytes && samples_valid &&
           accepted == samples && dropped == 0;
}

void FillSlot(SpscChannel::Reservation* reservation, uint64_t sequence) noexcept {
    (*reservation)->msg_type = 0x563233u;
    (*reservation)->schema_version = 0x00010000u;
    (*reservation)->schema_short_id = 0x563233563233ULL;
    (*reservation)->schema_layout_version = 1;
    (*reservation)->timestamp_ns = sequence + 1;
    (*reservation)->payload = ShmHandle{
        .offset = 4096 + sequence * kPayloadBytes,
        .generation = 1,
        .region_id = 1,
    };
    (*reservation)->payload_len = static_cast<uint32_t>(kPayloadBytes);
    (*reservation)->flags = 0;
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((always_inline))
#endif
inline bool PublishOne(SpscChannel* channel, uint64_t sequence,
                uint64_t* sink) noexcept {
    auto reserved = channel->Reserve();
    if (!reserved.ok()) return false;
    auto reservation = std::move(reserved).value();
    FillSlot(&reservation, sequence);
    if (!std::move(reservation).Commit().ok()) return false;
    auto polled = channel->Poll();
    if (!polled.ok()) return false;
    auto borrow = std::move(polled).value();
    *sink ^= borrow->sequence_num;
    *sink ^= borrow->payload.offset;
    return std::move(borrow).Ack().ok();
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#endif
bool RunTracedPublish(Instrumentation* telemetry, SpscChannel* channel,
                      const SampleKey& key, const TraceDecision& decision,
                      uint64_t* sink) {
    PerfTraceContext context;
    uint64_t sample_begin_ns = 0;
    telemetry->BeginTrace(key, decision, &context, &sample_begin_ns);
    if (!PublishOne(channel, key.sequence, sink)) return false;
    telemetry->EndTrace(key, decision, context, sample_begin_ns);
    return true;
}

template <PerfTelemetryMode mode>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#endif
bool RunPublishBatch(SpscChannel* channel, Instrumentation* telemetry,
                     uint64_t batch_begin, uint64_t batch_end,
                     CounterAccumulator* counters, uint64_t* sink) {
    constexpr bool kCounts = mode != PerfTelemetryMode::kOff;
    constexpr bool kMayTrace =
        mode == PerfTelemetryMode::kSampledLatency ||
        mode == PerfTelemetryMode::kFullDebug;
    for (uint64_t sequence = batch_begin; sequence < batch_end; ++sequence) {
        if constexpr (kMayTrace) {
            TraceDecision decision;
            if ((telemetry->Evaluate(sequence, &decision) & kTelemetryTrace) !=
                0) {
                const SampleKey key{7, 11, sequence};
                if (!RunTracedPublish(telemetry, channel, key, decision, sink)) {
                    return false;
                }
                continue;
            }
        }
        if (!PublishOne(channel, sequence, sink)) return false;
    }
    if constexpr (kCounts) {
        counters->messages = batch_end - batch_begin;
        counters->payload_bytes = counters->messages * kPayloadBytes;
        counters->wire_bytes = counters->messages * kWireBytes;
    }
    return true;
}

bool DispatchPublishBatch(PerfTelemetryMode mode, SpscChannel* channel,
                          Instrumentation* telemetry, uint64_t batch_begin,
                          uint64_t batch_end, CounterAccumulator* counters,
                          uint64_t* sink) {
    switch (mode) {
        case PerfTelemetryMode::kOff:
            return RunPublishBatch<PerfTelemetryMode::kOff>(
                channel, telemetry, batch_begin, batch_end, counters, sink);
        case PerfTelemetryMode::kCountersOnly:
            return RunPublishBatch<PerfTelemetryMode::kCountersOnly>(
                channel, telemetry, batch_begin, batch_end, counters, sink);
        case PerfTelemetryMode::kSampledLatency:
            return RunPublishBatch<PerfTelemetryMode::kSampledLatency>(
                channel, telemetry, batch_begin, batch_end, counters, sink);
        case PerfTelemetryMode::kFullDebug:
            return RunPublishBatch<PerfTelemetryMode::kFullDebug>(
                channel, telemetry, batch_begin, batch_end, counters, sink);
    }
    return false;
}

template <PerfTelemetryMode mode>
#if defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#endif
void RunMicroBatch(Instrumentation* telemetry, uint64_t batch_begin,
                   uint64_t batch_end, CounterAccumulator* counters,
                   uint64_t* state) {
    constexpr bool kCounts = mode != PerfTelemetryMode::kOff;
    constexpr bool kMayTrace =
        mode == PerfTelemetryMode::kSampledLatency ||
        mode == PerfTelemetryMode::kFullDebug;
    for (uint64_t sequence = batch_begin; sequence < batch_end; ++sequence) {
        const SampleKey key{7, 11, sequence};
        TraceDecision decision;
        PerfTraceContext context;
        uint64_t sample_begin_ns = 0;
        uint8_t actions = kCounts ? kTelemetryCount : 0;
        if constexpr (kMayTrace) {
            actions = telemetry->Evaluate(sequence, &decision);
            if ((actions & kTelemetryTrace) != 0) {
                telemetry->BeginTrace(key, decision, &context,
                                      &sample_begin_ns);
            }
        }
        *state = Work(*state + sequence);
        *state = Work(*state);
        if constexpr (kMayTrace) {
            if ((actions & kTelemetryTrace) != 0) {
                telemetry->EndTrace(key, decision, context, sample_begin_ns);
            }
        }
    }
    if constexpr (kCounts) {
        counters->messages = batch_end - batch_begin;
        counters->payload_bytes = counters->messages * kPayloadBytes;
        counters->wire_bytes = counters->messages * kWireBytes;
    }
}

void DispatchMicroBatch(PerfTelemetryMode mode, Instrumentation* telemetry,
                        uint64_t batch_begin, uint64_t batch_end,
                        CounterAccumulator* counters, uint64_t* state) {
    switch (mode) {
        case PerfTelemetryMode::kOff:
            return RunMicroBatch<PerfTelemetryMode::kOff>(
                telemetry, batch_begin, batch_end, counters, state);
        case PerfTelemetryMode::kCountersOnly:
            return RunMicroBatch<PerfTelemetryMode::kCountersOnly>(
                telemetry, batch_begin, batch_end, counters, state);
        case PerfTelemetryMode::kSampledLatency:
            return RunMicroBatch<PerfTelemetryMode::kSampledLatency>(
                telemetry, batch_begin, batch_end, counters, state);
        case PerfTelemetryMode::kFullDebug:
            return RunMicroBatch<PerfTelemetryMode::kFullDebug>(
                telemetry, batch_begin, batch_end, counters, state);
    }
}

template <bool instrumented>
Observation RunPublishMode(Mode mode, uint64_t iterations) {
    AlignedBytes memory = AllocateChannelMemory();
    if (memory == nullptr) return {};
    auto initialized = SpscChannel::Init(memory.get(), kChannelCapacity);
    if (!initialized.ok()) return {};
    SpscChannel channel = std::move(initialized).value();
    Instrumentation telemetry(mode);
    uint64_t elapsed_ns = 0;
    uint64_t sink = 0;

    for (uint64_t batch_begin = 0; batch_begin < iterations;
         batch_begin += kBatchIterations) {
        const uint64_t batch_end =
            std::min(iterations, batch_begin + kBatchIterations);
        const auto begin = Clock::now();
        CounterAccumulator counters;
        bool batch_ok = false;
        if constexpr (instrumented) {
            if (!telemetry.Synchronize()) return {};
            batch_ok = DispatchPublishBatch(
                telemetry.mode(), &channel, &telemetry, batch_begin, batch_end,
                &counters, &sink);
        } else {
            batch_ok = RunPublishBatch<PerfTelemetryMode::kOff>(
                &channel, &telemetry, batch_begin, batch_end, &counters, &sink);
        }
        if (!batch_ok) return {};
        if constexpr (instrumented) {
            telemetry.FlushCounters(counters.messages);
        }
        elapsed_ns += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                  begin)
                .count());
        telemetry.Drain();
    }

    const bool conserved = ValidTelemetryResult(
        mode, iterations, telemetry.messages(), telemetry.payload_bytes(),
        telemetry.wire_bytes(), telemetry.samples(), telemetry.accepted(),
        telemetry.dropped());
    sink ^= telemetry.sink();
    DoNotOptimize(sink);
    g_sink.fetch_xor(sink, std::memory_order_relaxed);
    return {
        .ns_per_operation = static_cast<double>(elapsed_ns) /
                            static_cast<double>(iterations),
        .messages = telemetry.messages(),
        .payload_bytes = telemetry.payload_bytes(),
        .wire_bytes = telemetry.wire_bytes(),
        .samples = telemetry.samples(),
        .accepted = telemetry.accepted(),
        .dropped = telemetry.dropped(),
        .sink = sink,
        .valid = conserved,
    };
}

template <bool instrumented>
Observation RunMicroOpMode(Mode mode, uint64_t iterations) {
    Instrumentation telemetry(mode);
    uint64_t elapsed_ns = 0;
    uint64_t state = 0x123456789abcdef0ULL;
    for (uint64_t batch_begin = 0; batch_begin < iterations;
         batch_begin += kBatchIterations) {
        const uint64_t batch_end =
            std::min(iterations, batch_begin + kBatchIterations);
        const auto begin = Clock::now();
        CounterAccumulator counters;
        if constexpr (instrumented) {
            if (!telemetry.Synchronize()) return {};
            DispatchMicroBatch(telemetry.mode(), &telemetry, batch_begin,
                               batch_end, &counters, &state);
        } else {
            RunMicroBatch<PerfTelemetryMode::kOff>(
                &telemetry, batch_begin, batch_end, &counters, &state);
        }
        if constexpr (instrumented) {
            telemetry.FlushCounters(counters.messages);
        }
        elapsed_ns += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                  begin)
                .count());
        telemetry.Drain();
    }
    const bool conserved = ValidTelemetryResult(
        mode, iterations, telemetry.messages(), telemetry.payload_bytes(),
        telemetry.wire_bytes(), telemetry.samples(), telemetry.accepted(),
        telemetry.dropped());
    state ^= telemetry.sink();
    DoNotOptimize(state);
    g_sink.fetch_xor(state, std::memory_order_relaxed);
    return {
        .ns_per_operation = static_cast<double>(elapsed_ns) /
                            static_cast<double>(iterations),
        .messages = telemetry.messages(),
        .payload_bytes = telemetry.payload_bytes(),
        .wire_bytes = telemetry.wire_bytes(),
        .samples = telemetry.samples(),
        .accepted = telemetry.accepted(),
        .dropped = telemetry.dropped(),
        .sink = state,
        .valid = conserved,
    };
}

Observation RunPublish(Mode mode, uint64_t iterations) {
    return mode == Mode::kCompileOffBaseline
               ? RunPublishMode<false>(mode, iterations)
               : RunPublishMode<true>(mode, iterations);
}

Observation RunMicroOp(Mode mode, uint64_t iterations) {
    return mode == Mode::kCompileOffBaseline
               ? RunMicroOpMode<false>(mode, iterations)
               : RunMicroOpMode<true>(mode, iterations);
}

Observation Run(Workload workload, Mode mode, uint64_t iterations) {
    return workload == Workload::kPublish ? RunPublish(mode, iterations)
                                          : RunMicroOp(mode, iterations);
}

void Calibrate(double* clock_pair_ns, double* noop_ns) {
    constexpr uint64_t kCalibrationIterations = 1'000'000;
    uint64_t elapsed = 0;
    const auto clock_begin = Clock::now();
    for (uint64_t i = 0; i < kCalibrationIterations; ++i) {
        const auto first = Clock::now();
        const auto second = Clock::now();
        elapsed += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(second - first)
                .count());
    }
    const auto clock_end = Clock::now();
    DoNotOptimize(elapsed);
    *clock_pair_ns = static_cast<double>(
                         std::chrono::duration_cast<std::chrono::nanoseconds>(
                             clock_end - clock_begin)
                             .count()) /
                     static_cast<double>(kCalibrationIterations);

    uint64_t state = 1;
    const auto noop_begin = Clock::now();
    for (uint64_t i = 0; i < kCalibrationIterations; ++i) {
        state += i;
        DoNotOptimize(state);
    }
    *noop_ns = static_cast<double>(
                   std::chrono::duration_cast<std::chrono::nanoseconds>(
                       Clock::now() - noop_begin)
                       .count()) /
               static_cast<double>(kCalibrationIterations);
    DoNotOptimize(state);
}

int32_t PinToFirstAllowedCpu() noexcept {
#if defined(__linux__)
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) return -1;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &allowed)) continue;
        cpu_set_t selected;
        CPU_ZERO(&selected);
        CPU_SET(cpu, &selected);
        return sched_setaffinity(0, sizeof(selected), &selected) == 0 ? cpu : -1;
    }
#endif
    return -1;
}

Observation AverageObservations(const Observation& first,
                                const Observation& second) {
    Observation result = first;
    result.ns_per_operation =
        (first.ns_per_operation + second.ns_per_operation) / 2.0;
    result.sink = StableSampleMix(first.sink ^ second.sink);
    result.valid = first.valid && second.valid &&
                   first.messages == second.messages &&
                   first.payload_bytes == second.payload_bytes &&
                   first.wire_bytes == second.wire_bytes &&
                   first.samples == second.samples &&
                   first.accepted == second.accepted &&
                   first.dropped == second.dropped;
    return result;
}

ProcessReport RunProcess(const Options& options, uint32_t process_index) {
    ProcessReport report;
    report.rounds = options.rounds;
#if defined(__linux__)
    report.process_id = static_cast<int64_t>(getpid());
#endif
    report.pinned_cpu = PinToFirstAllowedCpu();
    Calibrate(&report.clock_pair_ns, &report.noop_ns);
    const uint64_t warmup_iterations =
        std::clamp<uint64_t>(options.iterations / 20, 10'000, 100'000);

    for (uint32_t round = 0; round < options.rounds; ++round) {
        for (size_t workload_index = 0; workload_index < kWorkloadCount;
             ++workload_index) {
            const Workload workload = static_cast<Workload>(workload_index);
            const size_t rotation =
                (static_cast<size_t>(process_index) + round + workload_index) %
                kModeCount;
            for (size_t order = 0; order < kModeCount; ++order) {
                const size_t mode_index =
                    ((round & 1u) == 0)
                        ? (rotation + order) % kModeCount
                        : (rotation + kModeCount - order) % kModeCount;
                const Mode mode = static_cast<Mode>(mode_index);
                const Observation warmup = Run(workload, mode, warmup_iterations);
                if (!warmup.valid) return report;
                if (mode == Mode::kCompileOffBaseline) {
                    report.observations[round][workload_index][mode_index] =
                        Run(workload, mode, options.iterations);
                    report.paired_baseline_ns[round][workload_index][mode_index] =
                        report.observations[round][workload_index][mode_index]
                            .ns_per_operation;
                } else {
                    const Observation baseline_warmup =
                        Run(workload, Mode::kCompileOffBaseline,
                            warmup_iterations);
                    if (!baseline_warmup.valid) return report;
                    Observation baseline;
                    if (((round + order) & 1u) == 0) {
                        const Observation before =
                            Run(workload, Mode::kCompileOffBaseline,
                                options.iterations);
                        report.observations[round][workload_index][mode_index] =
                            Run(workload, mode, options.iterations);
                        const Observation after =
                            Run(workload, Mode::kCompileOffBaseline,
                                options.iterations);
                        baseline = AverageObservations(before, after);
                    } else {
                        const Observation before =
                            Run(workload, mode, options.iterations);
                        baseline = Run(workload, Mode::kCompileOffBaseline,
                                       options.iterations);
                        const Observation after =
                            Run(workload, mode, options.iterations);
                        report.observations[round][workload_index][mode_index] =
                            AverageObservations(before, after);
                    }
                    if (!baseline.valid) return report;
                    report.paired_baseline_ns[round][workload_index][mode_index] =
                        baseline.ns_per_operation;
                }
                if (!report.observations[round][workload_index][mode_index]
                         .valid) {
                    return report;
                }
            }
        }
    }
    report.valid = true;
    return report;
}

bool ParseUnsigned(std::string_view input, uint64_t* value) {
    const auto parsed =
        std::from_chars(input.data(), input.data() + input.size(), *value);
    return parsed.ec == std::errc{} &&
           parsed.ptr == input.data() + input.size() && *value != 0;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    if (argc == 2 && argv[1][0] != '-') {
        return ParseUnsigned(argv[1], &options->iterations);
    }
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        const size_t equals = argument.find('=');
        if (equals == std::string_view::npos) return false;
        const std::string_view name = argument.substr(0, equals);
        const std::string_view value = argument.substr(equals + 1);
        if (name == "--json") {
            if (value.empty() || value.size() >= options->json_path.size()) {
                return false;
            }
            std::memcpy(options->json_path.data(), value.data(), value.size());
            options->json_path[value.size()] = '\0';
            continue;
        }
        uint64_t parsed = 0;
        if (!ParseUnsigned(value, &parsed)) return false;
        if (name == "--iterations") {
            options->iterations = parsed;
        } else if (name == "--rounds" && parsed <= kMaxRounds) {
            options->rounds = static_cast<uint32_t>(parsed);
        } else if (name == "--processes" && parsed <= 64) {
            options->processes = static_cast<uint32_t>(parsed);
        } else {
            return false;
        }
    }
    return true;
}

Summary Summarize(const std::vector<double>& values) {
    Summary summary;
    if (values.empty()) return summary;
    for (double value : values) summary.mean += value;
    summary.mean /= static_cast<double>(values.size());
    if (values.size() == 1) {
        summary.lower95 = summary.mean;
        summary.upper95 = summary.mean;
        return summary;
    }
    double squared = 0;
    for (double value : values) {
        const double delta = value - summary.mean;
        squared += delta * delta;
    }
    const double deviation =
        std::sqrt(squared / static_cast<double>(values.size() - 1));
    // Two-sided Student-t critical values, 95%, df 1..30; normal thereafter.
    constexpr std::array<double, 31> kT95 = {
        0,      12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365,
        2.306,  2.262,  2.228, 2.201, 2.179, 2.160, 2.145, 2.131,
        2.120,  2.110,  2.101, 2.093, 2.086, 2.080, 2.074, 2.069,
        2.064,  2.060,  2.056, 2.052, 2.048, 2.045, 2.042};
    const size_t degrees = values.size() - 1;
    const double critical =
        degrees < kT95.size() ? kT95[degrees] : 1.96;
    const double margin =
        critical * deviation / std::sqrt(static_cast<double>(values.size()));
    summary.lower95 = summary.mean - margin;
    summary.upper95 = summary.mean + margin;
    return summary;
}

struct AggregatedMode {
    Summary absolute;
    Summary overhead;
    uint64_t samples = 0;
    uint64_t accepted = 0;
    uint64_t dropped = 0;
};

AggregatedMode AggregateMode(const Options& options,
                             const std::vector<ProcessReport>& reports,
                             size_t workload_index, size_t mode_index) {
    AggregatedMode result;
    std::vector<double> process_ns;
    std::vector<double> process_overhead;
    for (const ProcessReport& report : reports) {
        double ns = 0;
        double overhead = 0;
        for (uint32_t round = 0; round < options.rounds; ++round) {
            const Observation& current =
                report.observations[round][workload_index][mode_index];
            const double baseline_ns =
                report.paired_baseline_ns[round][workload_index][mode_index];
            ns += current.ns_per_operation;
            overhead += (current.ns_per_operation / baseline_ns - 1.0) * 100.0;
            result.samples += current.samples;
            result.accepted += current.accepted;
            result.dropped += current.dropped;
        }
        process_ns.push_back(ns / options.rounds);
        process_overhead.push_back(overhead / options.rounds);
    }
    result.absolute = Summarize(process_ns);
    result.overhead = Summarize(process_overhead);
    return result;
}

bool AcceptancePassed(const Options& options,
                      const std::vector<ProcessReport>& reports) {
    const size_t publish = static_cast<size_t>(Workload::kPublish);
    const AggregatedMode counters = AggregateMode(
        options, reports, publish, static_cast<size_t>(Mode::kCounters));
    const AggregatedMode sampled = AggregateMode(
        options, reports, publish, static_cast<size_t>(Mode::kSampled));
    return counters.overhead.upper95 <= 1.0 &&
           sampled.overhead.upper95 <= 2.0 && counters.dropped == 0 &&
           sampled.dropped == 0 && sampled.accepted == sampled.samples;
}

bool WriteJsonArtifact(const Options& options,
                       const std::vector<ProcessReport>& reports,
                       bool acceptance_passed) {
    if (options.json_path[0] == '\0') return true;
    std::FILE* output = std::fopen(options.json_path.data(), "w");
    if (output == nullptr) return false;
    std::fprintf(
        output,
        "{\n  \"schema\":\"mino.v23_telemetry_benchmark.v1\",\n"
        "  \"iterations\":%llu,\n  \"rounds\":%u,\n"
        "  \"independent_processes\":%u,\n  \"batch_operations\":%llu,\n"
        "  \"sample_rate_ppm\":%u,\n  \"acceptance_passed\":%s,\n"
        "  \"workloads\":{\"acceptance\":\"real-publish\","
        "\"diagnostic_only\":\"micro-op\"},\n  \"summaries\":[\n",
        static_cast<unsigned long long>(options.iterations), options.rounds,
        options.processes, static_cast<unsigned long long>(kBatchIterations),
        kSampleRatePpm, acceptance_passed ? "true" : "false");
    bool first_summary = true;
    for (size_t workload = 0; workload < kWorkloadCount; ++workload) {
        for (size_t mode = 0; mode < kModeCount; ++mode) {
            const AggregatedMode aggregate =
                AggregateMode(options, reports, workload, mode);
            std::fprintf(
                output,
                "%s    {\"workload\":\"%s\",\"mode\":\"%s\","
                "\"ns_per_op\":{\"mean\":%.9f,\"lower95\":%.9f,"
                "\"upper95\":%.9f},\"paired_overhead_percent\":{"
                "\"mean\":%.9f,\"lower95\":%.9f,\"upper95\":%.9f},"
                "\"samples\":%llu,\"accepted\":%llu,\"dropped\":%llu}",
                first_summary ? "" : ",\n", kWorkloadNames[workload],
                kModeNames[mode], aggregate.absolute.mean,
                aggregate.absolute.lower95, aggregate.absolute.upper95,
                aggregate.overhead.mean, aggregate.overhead.lower95,
                aggregate.overhead.upper95,
                static_cast<unsigned long long>(aggregate.samples),
                static_cast<unsigned long long>(aggregate.accepted),
                static_cast<unsigned long long>(aggregate.dropped));
            first_summary = false;
        }
    }
    std::fprintf(output, "\n  ],\n  \"processes\":[\n");
    for (size_t process = 0; process < reports.size(); ++process) {
        const ProcessReport& report = reports[process];
        std::fprintf(
            output,
            "%s    {\"index\":%zu,\"pid\":%lld,\"pinned_cpu\":%d,"
            "\"clock_pair_ns\":%.9f,\"noop_ns\":%.9f,\"rounds\":[",
            process == 0 ? "" : ",\n", process,
            static_cast<long long>(report.process_id), report.pinned_cpu,
            report.clock_pair_ns, report.noop_ns);
        bool first_observation = true;
        for (uint32_t round = 0; round < options.rounds; ++round) {
            for (size_t workload = 0; workload < kWorkloadCount; ++workload) {
                for (size_t mode = 0; mode < kModeCount; ++mode) {
                    const Observation& observation =
                        report.observations[round][workload][mode];
                    std::fprintf(
                        output,
                        "%s{\"round\":%u,\"workload\":\"%s\","
                        "\"mode\":\"%s\",\"ns_per_op\":%.9f,"
                        "\"paired_baseline_ns_per_op\":%.9f,"
                        "\"messages\":%llu,\"payload_bytes\":%llu,"
                        "\"wire_bytes\":%llu,\"samples\":%llu,"
                        "\"accepted\":%llu,\"dropped\":%llu,"
                        "\"valid\":%s}",
                        first_observation ? "" : ",", round,
                        kWorkloadNames[workload], kModeNames[mode],
                        observation.ns_per_operation,
                        report.paired_baseline_ns[round][workload][mode],
                        static_cast<unsigned long long>(observation.messages),
                        static_cast<unsigned long long>(
                            observation.payload_bytes),
                        static_cast<unsigned long long>(observation.wire_bytes),
                        static_cast<unsigned long long>(observation.samples),
                        static_cast<unsigned long long>(observation.accepted),
                        static_cast<unsigned long long>(observation.dropped),
                        observation.valid ? "true" : "false");
                    first_observation = false;
                }
            }
        }
        std::fprintf(output, "]}");
    }
    std::fprintf(output, "\n  ]\n}\n");
    const bool write_ok = std::ferror(output) == 0;
    const bool close_ok = std::fclose(output) == 0;
    return write_ok && close_ok;
}

#if defined(__linux__)
bool WriteAll(int descriptor, const void* data, size_t bytes) {
    const auto* cursor = static_cast<const std::byte*>(data);
    while (bytes != 0) {
        const ssize_t written = write(descriptor, cursor, bytes);
        if (written <= 0) return false;
        cursor += written;
        bytes -= static_cast<size_t>(written);
    }
    return true;
}

bool ReadAll(int descriptor, void* data, size_t bytes) {
    auto* cursor = static_cast<std::byte*>(data);
    while (bytes != 0) {
        const ssize_t received = read(descriptor, cursor, bytes);
        if (received <= 0) return false;
        cursor += received;
        bytes -= static_cast<size_t>(received);
    }
    return true;
}
#endif

bool CollectReports(const Options& options,
                    std::vector<ProcessReport>* reports) {
    reports->clear();
    reports->reserve(options.processes);
    for (uint32_t process = 0; process < options.processes; ++process) {
#if defined(__linux__)
        int descriptors[2];
        if (pipe(descriptors) != 0) return false;
        const pid_t child = fork();
        if (child < 0) return false;
        if (child == 0) {
            close(descriptors[0]);
            const ProcessReport report = RunProcess(options, process);
            const bool written =
                WriteAll(descriptors[1], &report, sizeof(report));
            close(descriptors[1]);
            _exit(written && report.valid ? 0 : 1);
        }
        close(descriptors[1]);
        ProcessReport report;
        const bool read = ReadAll(descriptors[0], &report, sizeof(report));
        close(descriptors[0]);
        int status = 0;
        if (waitpid(child, &status, 0) != child || !read ||
            !WIFEXITED(status) || WEXITSTATUS(status) != 0 || !report.valid) {
            return false;
        }
        reports->push_back(report);
#else
        reports->push_back(RunProcess(options, process));
        if (!reports->back().valid) return false;
#endif
    }
    return true;
}

void PrintResults(const Options& options,
                  const std::vector<ProcessReport>& reports) {
    std::vector<double> clock_values;
    std::vector<double> noop_values;
    uint64_t reported_sink = 0;
    for (const ProcessReport& report : reports) {
        clock_values.push_back(report.clock_pair_ns);
        noop_values.push_back(report.noop_ns);
        for (uint32_t round = 0; round < options.rounds; ++round) {
            for (size_t workload = 0; workload < kWorkloadCount; ++workload) {
                for (size_t mode = 0; mode < kModeCount; ++mode) {
                    reported_sink = StableSampleMix(
                        reported_sink ^
                        report.observations[round][workload][mode].sink ^
                        (static_cast<uint64_t>(round) << 32) ^
                        (static_cast<uint64_t>(workload) << 16) ^ mode);
                }
            }
        }
    }
    const Summary clock = Summarize(clock_values);
    const Summary noop = Summarize(noop_values);

    std::printf("V-23 telemetry benchmark: %llu operations, %u rounds x %u processes\n",
                static_cast<unsigned long long>(options.iterations),
                options.rounds, options.processes);
    std::printf("operation(real-publish)=SpscChannel Reserve+fixed-slot fill+Commit+Poll+read+Ack\n");
    std::printf("sampling=stable 1%% with no rate-limit drops; counters=messages+payload_bytes+wire_bytes with timed batch flush; sidecar drain excluded from data-path timer\n");
    std::printf("calibration clock-pair %.3f ns [%.3f, %.3f], noop %.3f ns [%.3f, %.3f]\n",
                clock.mean, clock.lower95, clock.upper95, noop.mean,
                noop.lower95, noop.upper95);
    std::printf("pinned_cpus=");
    for (size_t index = 0; index < reports.size(); ++index) {
        std::printf("%s%d", index == 0 ? "" : ",", reports[index].pinned_cpu);
    }
    std::printf("\n\n");

    for (size_t workload_index = 0; workload_index < kWorkloadCount;
         ++workload_index) {
        std::printf("%s%s\n", kWorkloadNames[workload_index],
                    workload_index == 0
                        ? " (ADR-0009 acceptance baseline)"
                        : " (physical lower-bound diagnostic; not an acceptance baseline)");
        std::printf("%-14s %25s %25s %12s %12s %12s %10s\n", "mode",
                    "absolute ns/op mean [95% CI]",
                    "paired overhead mean [95% CI]", "samples", "accepted",
                    "dropped", "target");
        for (size_t mode_index = 0; mode_index < kModeCount; ++mode_index) {
            const AggregatedMode aggregate =
                AggregateMode(options, reports, workload_index, mode_index);
            const char* target = "n/a";
            if (workload_index == static_cast<size_t>(Workload::kPublish)) {
                if (mode_index == static_cast<size_t>(Mode::kCounters)) {
                    target = aggregate.overhead.upper95 <= 1.0 ? "PASS<=1%"
                                                               : "FAIL<=1%";
                } else if (mode_index ==
                           static_cast<size_t>(Mode::kSampled)) {
                    target = aggregate.overhead.upper95 <= 2.0 ? "PASS<=2%"
                                                               : "FAIL<=2%";
                }
            }
            std::printf("%-14s %8.3f [%8.3f,%8.3f] %8.3f%% [%8.3f,%8.3f] %12llu %12llu %12llu %10s\n",
                        kModeNames[mode_index], aggregate.absolute.mean,
                        aggregate.absolute.lower95, aggregate.absolute.upper95,
                        aggregate.overhead.mean, aggregate.overhead.lower95,
                        aggregate.overhead.upper95,
                        static_cast<unsigned long long>(aggregate.samples),
                        static_cast<unsigned long long>(aggregate.accepted),
                        static_cast<unsigned long long>(aggregate.dropped),
                        target);
        }
        std::printf("\n");
    }
    std::printf("sink=%llu\n",
                static_cast<unsigned long long>(reported_sink));
}

}  // namespace
}  // namespace mino::observability

int main(int argc, char** argv) {
    mino::observability::Options options;
    if (!mino::observability::ParseOptions(argc, argv, &options)) {
        std::fprintf(stderr,
                     "usage: %s [positive-iterations] | "
                     "[--iterations=N --rounds=N --processes=N "
                     "--json=PATH]\n",
                     argv[0]);
        return 2;
    }
    std::vector<mino::observability::ProcessReport> reports;
    if (!mino::observability::CollectReports(options, &reports)) {
        std::fprintf(stderr, "V-23 benchmark worker failed\n");
        return 1;
    }
    mino::observability::PrintResults(options, reports);
    const bool passed = mino::observability::AcceptancePassed(options, reports);
    if (!mino::observability::WriteJsonArtifact(options, reports, passed)) {
        std::fprintf(stderr, "failed to write V-23 JSON artifact: %s\n",
                     options.json_path.data());
        return 1;
    }
    if (options.json_path[0] != '\0') {
        std::printf("json_artifact=%s\n", options.json_path.data());
    }
    return passed ? 0 : 1;
}
