// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/tracing.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <vector>

namespace mino::observability {
namespace {

TEST(TelemetryControlTest, ImplementsAllModesAndStableSampling) {
    const SampleKey key{11, 22, 33};
    TelemetryControl control;
    EXPECT_FALSE(control.CountersEnabled());
    EXPECT_FALSE(control.ShouldTrace(key));

    ASSERT_TRUE(control.SetPolicy(
        {PerfTelemetryMode::kCountersOnly, 1'000'000, 0, 0}));
    EXPECT_TRUE(control.CountersEnabled());
    EXPECT_FALSE(control.ShouldTrace(key));

    ASSERT_TRUE(control.SetPolicy(
        {PerfTelemetryMode::kSampledLatency, 1'000'000, 100, 0}));
    EXPECT_TRUE(control.ShouldTrace(key));
    EXPECT_EQ(StableSampleHash(key), StableSampleHash(key));

    ASSERT_TRUE(control.SetPolicy(
        {PerfTelemetryMode::kSampledLatency, 0, 100, 0}));
    EXPECT_FALSE(control.ShouldTrace(key));

    ASSERT_TRUE(control.SetPolicy(
        {PerfTelemetryMode::kFullDebug, 0, 0, 10}));
    EXPECT_TRUE(control.ShouldTrace(key));
    EXPECT_FALSE(control.SetPolicy(
        {PerfTelemetryMode::kFullDebug, 0, 0, 0}));
}

TEST(TelemetryControlTest, StableSequenceProjectionTracksConfiguredRate) {
    constexpr uint64_t kSequences = 1'000'000;
    uint64_t sampled = 0;
    for (uint64_t sequence = 0; sequence < kSequences; ++sequence) {
        sampled += IsSampled(SampleKey{17, 29, sequence}, 10'000) ? 1 : 0;
    }
    const SampleKey retry{17, 29, 12345};
    EXPECT_EQ(IsSampled(retry, 10'000), IsSampled(retry, 10'000));
    EXPECT_NEAR(static_cast<double>(sampled), 10'000.0, 300.0);
}

TEST(TelemetryControlTest, CachedDecisionTracksEpochAndStableIdentityPrefix) {
    TelemetryControl control(
        {PerfTelemetryMode::kSampledLatency, 1'000'000, 25, 100});
    TelemetryThreadCache cache;
    TraceDecision first;
    const SampleKey key{11, 22, 33};
    ASSERT_TRUE(control.CountersEnabled(&cache));
    ASSERT_TRUE(control.ShouldTrace(key, &cache, &first));
    EXPECT_EQ(first.sample_hash, StableSampleHash(key));
    EXPECT_EQ(first.slow_threshold_ns, 25u);
    const uint64_t first_epoch = first.policy_epoch;

    ASSERT_TRUE(control.SetPolicy(
        {PerfTelemetryMode::kCountersOnly, 0, 0, 0}));
    EXPECT_TRUE(control.CountersEnabled(&cache));
    TraceDecision disabled;
    EXPECT_FALSE(control.ShouldTrace(key, &cache, &disabled));
    EXPECT_GT(cache.policy_epoch(), first_epoch);

    ASSERT_TRUE(control.SetPolicy(
        {PerfTelemetryMode::kOff, 0, 0, 0}));
    EXPECT_FALSE(control.CountersEnabled(&cache));
}

TEST(TelemetryControlTest, BatchCacheChangesOnlyAtSynchronizedBoundary) {
    TelemetryControl control(
        {PerfTelemetryMode::kSampledLatency, 1'000'000, 0, 100});
    TelemetryThreadCache cache;
    ASSERT_TRUE(control.Synchronize(&cache));
    TraceDecision before;
    EXPECT_EQ(control.EvaluateCached(SampleKey{1, 2, 3}, &cache, &before),
              kTelemetryCount | kTelemetryTrace);

    ASSERT_TRUE(control.SetPolicy({PerfTelemetryMode::kOff, 0, 0, 0}));
    TraceDecision same_batch;
    EXPECT_EQ(control.EvaluateCached(SampleKey{1, 2, 4}, &cache, &same_batch),
              kTelemetryCount | kTelemetryTrace);
    EXPECT_EQ(same_batch.policy_epoch, before.policy_epoch);

    ASSERT_TRUE(control.Synchronize(&cache));
    TraceDecision next_batch;
    EXPECT_EQ(control.EvaluateCached(SampleKey{1, 2, 5}, &cache, &next_batch),
              0u);
    EXPECT_GT(cache.policy_epoch(), before.policy_epoch);
}

TEST(TelemetryControlTest, FixedSourceBatchBoundsPolicyActivationTo256Ops) {
    TelemetryControl control(
        {PerfTelemetryMode::kSampledLatency, 1'000'000, 0, 100});
    TelemetryThreadCache cache;
    ASSERT_TRUE(control.Synchronize(&cache, 7, 11));
    const uint64_t first_epoch = cache.policy_epoch();
    ASSERT_TRUE(control.SetPolicy({PerfTelemetryMode::kOff, 0, 0, 0}));

    for (uint64_t sequence = 0; sequence < 256; ++sequence) {
        TraceDecision decision;
        EXPECT_EQ(control.EvaluateSequenceCached(sequence, &cache, &decision),
                  kTelemetryCount | kTelemetryTrace);
        EXPECT_EQ(decision.sample_hash,
                  StableSampleHash(SampleKey{7, 11, sequence}));
        EXPECT_NE(decision.trace_id_low, 0u);
        EXPECT_EQ(decision.policy_epoch, first_epoch);
    }

    ASSERT_TRUE(control.Synchronize(&cache, 7, 11));
    TraceDecision next_batch;
    EXPECT_EQ(control.EvaluateSequenceCached(256, &cache, &next_batch), 0u);
    EXPECT_GT(cache.policy_epoch(), first_epoch);
}

TEST(TelemetryControlTest, ConcurrentPublicationNeverTearsPolicy) {
    const PerfTelemetryPolicy first{
        PerfTelemetryMode::kSampledLatency, 123, 456, 789};
    const PerfTelemetryPolicy second{
        PerfTelemetryMode::kFullDebug, 0, 654, 321};
    TelemetryControl control(first);
    std::atomic<bool> done{false};
    std::atomic<bool> writer_failed{false};
    bool torn = false;
    PerfTelemetryPolicy torn_policy;
    std::thread writer([&] {
        for (size_t i = 0; i < 100'000; ++i) {
            if (!control.SetPolicy((i & 1u) == 0 ? second : first)) {
                writer_failed.store(true, std::memory_order_relaxed);
            }
        }
        done.store(true, std::memory_order_release);
    });
    while (!done.load(std::memory_order_acquire)) {
        PerfTelemetryPolicy policy;
        if (!control.TryLoadPolicy(&policy)) continue;
        const bool is_first =
            policy.mode == first.mode &&
            policy.sample_rate_ppm == first.sample_rate_ppm &&
            policy.slow_threshold_ns == first.slow_threshold_ns &&
            policy.max_events_per_second == first.max_events_per_second;
        const bool is_second =
            policy.mode == second.mode &&
            policy.sample_rate_ppm == second.sample_rate_ppm &&
            policy.slow_threshold_ns == second.slow_threshold_ns &&
            policy.max_events_per_second == second.max_events_per_second;
        if (!is_first && !is_second) {
            torn = true;
            torn_policy = policy;
            break;
        }
    }
    writer.join();
    EXPECT_FALSE(writer_failed.load(std::memory_order_relaxed));
    EXPECT_FALSE(torn) << "mode=" << static_cast<int>(torn_policy.mode)
                       << " sample_rate_ppm=" << torn_policy.sample_rate_ppm
                       << " slow_threshold_ns="
                       << torn_policy.slow_threshold_ns
                       << " max_events_per_second="
                       << torn_policy.max_events_per_second;
}

TEST(BoundedQueueTest, PreservesAllValuesWithConcurrentProducers) {
    constexpr size_t kProducers = 4;
    constexpr uint64_t kPerProducer = 5'000;
    BoundedQueue<uint64_t, 64> queue;
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> producers;
    for (size_t producer = 0; producer < kProducers; ++producer) {
        producers.emplace_back([&, producer] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (uint64_t value = 1; value <= kPerProducer; ++value) {
                const uint64_t encoded =
                    static_cast<uint64_t>(producer) * kPerProducer + value;
                while (!queue.TryPush(encoded)) std::this_thread::yield();
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != kProducers) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    uint64_t sum = 0;
    size_t consumed = 0;
    while (consumed < kProducers * kPerProducer) {
        uint64_t value = 0;
        if (queue.TryPop(&value)) {
            sum += value;
            ++consumed;
        } else {
            std::this_thread::yield();
        }
    }
    for (auto& producer : producers) producer.join();

    const uint64_t total = kProducers * kPerProducer;
    EXPECT_EQ(sum, total * (total + 1) / 2);
}

TEST(SidecarTraceQueueTest, IsBoundedAndCountsDrops) {
    SidecarTraceQueue<4> queue;
    TraceEvent event;
    for (size_t i = 0; i < 4; ++i) {
        event.attempt_id = static_cast<uint32_t>(i);
        EXPECT_TRUE(queue.TryPush(event));
    }
    EXPECT_FALSE(queue.TryPush(event));
    EXPECT_EQ(queue.accepted(), 4u);
    EXPECT_EQ(queue.dropped(), 1u);

    for (size_t i = 0; i < 4; ++i) {
        TraceEvent output;
        ASSERT_TRUE(queue.TryPop(&output));
        EXPECT_EQ(output.attempt_id, i);
    }
    TraceEvent output;
    EXPECT_FALSE(queue.TryPop(&output));
}

TEST(EventRateLimiterTest, AppliesOneTotalBudgetAcrossShards) {
    EventRateLimiter<4> limiter;
    EXPECT_TRUE(limiter.TryAcquire(2, 1, 0));
    EXPECT_TRUE(limiter.TryAcquire(2, 2, 1));
    EXPECT_FALSE(limiter.TryAcquire(2, 3, 2));
    EXPECT_FALSE(limiter.TryAcquire(2, 4, 3));
    EXPECT_TRUE(limiter.TryAcquire(2, 1'000'000'001, 3));
}

TEST(TelemetryTracerTest, AppliesFullDebugEventRateLimit) {
    TelemetryControl control(
        {PerfTelemetryMode::kFullDebug, 0, 0, 2});
    TelemetryTracer<8, 1> tracer(control);
    const SampleKey key{1, 2, 3};
    const TraceEvent event{};
    EXPECT_TRUE(tracer.TryRecordEvent(key, event, 0, 1));
    EXPECT_TRUE(tracer.TryRecordEvent(key, event, 0, 2));
    EXPECT_FALSE(tracer.TryRecordEvent(key, event, 0, 3));
    EXPECT_EQ(tracer.accepted(), 2u);
    EXPECT_EQ(tracer.dropped(), 1u);
    EXPECT_TRUE(tracer.TryRecordEvent(key, event, 0, 1'000'000'001));
}

TEST(TelemetryTracerTest, AcceptsPrecomputedDecisionWithoutPolicyReload) {
    TelemetryControl control(
        {PerfTelemetryMode::kSampledLatency, 1'000'000, 0, 10});
    TelemetryThreadCache cache;
    const SampleKey key{1, 2, 3};
    TraceDecision decision;
    ASSERT_TRUE(control.ShouldTrace(key, &cache, &decision));
    ASSERT_TRUE(control.SetPolicy({PerfTelemetryMode::kOff, 0, 0, 0}));

    TelemetryTracer<8> tracer(control);
    TraceEvent event;
    event.trace_id_high = decision.sample_hash;
    EXPECT_TRUE(tracer.TryRecordSampledEvent(decision, event, 0, 1));
    TraceEvent observed;
    ASSERT_TRUE(tracer.TryPop(&observed));
    EXPECT_EQ(observed.trace_id_high, decision.sample_hash);
}

TEST(TelemetryTracerTest, AppliesSampledSlowThreshold) {
    TelemetryControl control(
        {PerfTelemetryMode::kSampledLatency, 1'000'000, 100, 10});
    TelemetryTracer<8> tracer(control);
    const SampleKey key{1, 2, 3};
    TraceEvent event;
    event.duration_ns = 99;
    EXPECT_FALSE(tracer.TryRecordEvent(key, event, 0, 1));
    event.duration_ns = 100;
    EXPECT_TRUE(tracer.TryRecordEvent(key, event, 0, 2));
    TraceEvent output;
    ASSERT_TRUE(tracer.TryPop(&output));
    EXPECT_EQ(output.duration_ns, 100u);
}

TEST(TelemetryTracerTest, RejectsTemporaryControlAtCompileTime) {
    EXPECT_FALSE((std::is_constructible_v<TelemetryTracer<8>,
                                          TelemetryControl&&>));
}

TEST(TraceContextTest, ReusesPrecomputedDecisionIds) {
    const SampleKey key{5, 7, 9};
    const TraceDecision decision{
        .sample_hash = 123,
        .trace_id_low = 456,
        .policy_epoch = 0,
        .slow_threshold_ns = 0,
        .max_events_per_second = 0,
        .mode = PerfTelemetryMode::kSampledLatency,
    };
    const TraceContext context =
        MakeTraceContext(key, decision, kPerfTraceSampled, 42, 100, 50);
    EXPECT_EQ(context.trace_id_high, 123u);
    EXPECT_EQ(context.trace_id_low, 456u);
}

TEST(TraceContextTest, IsDeterministicAndCarriesClockDomain) {
    const SampleKey key{5, 7, 9};
    const TraceContext first =
        MakeTraceContext(key, kPerfTraceSampled, 42, 100, 50);
    const TraceContext second =
        MakeTraceContext(key, kPerfTraceSampled, 42, 100, 50);
    EXPECT_EQ(first.trace_id_high, second.trace_id_high);
    EXPECT_EQ(first.trace_id_low, second.trace_id_low);
    EXPECT_EQ(first.clock_domain_id, 42u);
    EXPECT_EQ(first.origin_wall_time_ns, 100u);
}

}  // namespace
}  // namespace mino::observability
