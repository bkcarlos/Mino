// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recording_topology.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace mino::storage {
namespace {

constexpr std::array<RecordingMode, 4> kModes = {
    RecordingMode::kBestEffort,
    RecordingMode::kMemoryBuffered,
    RecordingMode::kDurable,
    RecordingMode::kSnapshot,
};

constexpr std::array<RecordBackpressureTopology, 3> kTopologies = {
    RecordBackpressureTopology::kStrongConsistent,
    RecordBackpressureTopology::kIsolated,
    RecordBackpressureTopology::kBestEffort,
};

bool ExpectedTopologyAllowed(RecordingMode mode,
                             RecordBackpressureTopology topology) {
    if (topology == RecordBackpressureTopology::kStrongConsistent) {
        return mode == RecordingMode::kMemoryBuffered ||
               mode == RecordingMode::kDurable;
    }
    if (topology == RecordBackpressureTopology::kIsolated) return true;
    return mode != RecordingMode::kDurable;
}

Result<EffectiveRecordingPolicy> MakeEffective(
    RecordingMode mode, RecordBackpressureTopology topology,
    std::optional<BufferFullPolicy> full_policy = std::nullopt) {
    RecordingPolicy requested;
    requested.mode = mode;
    requested.backpressure_topology = topology;
    requested.full_policy = full_policy;
    requested.is_state_topic = mode == RecordingMode::kSnapshot;
    return ValidateRecordingPolicy(requested);
}

std::unique_ptr<RecordingTopologyCoordinator> CreateCoordinator(
    RecordingMode mode, RecordBackpressureTopology topology,
    std::optional<BufferFullPolicy> full_policy = std::nullopt,
    uint64_t cursor = 0, uint64_t now_ns = 0,
    RecordingTopologyObserver* observer = nullptr) {
    Result<EffectiveRecordingPolicy> effective =
        MakeEffective(mode, topology, full_policy);
    EXPECT_TRUE(effective.ok()) << effective.status().ToString();
    if (!effective.ok()) return nullptr;
    auto coordinator = RecordingTopologyCoordinator::Create(
        *effective, cursor, now_ns, observer);
    EXPECT_TRUE(coordinator.ok()) << coordinator.status().ToString();
    return coordinator.ok() ? std::move(*coordinator) : nullptr;
}

RecordingAdmissionRequest Request(
    uint64_t cursor,
    RecordingSinkCapacity capacity = RecordingSinkCapacity::kAvailable) {
    RecordingAdmissionRequest request;
    request.available_cursor = cursor;
    request.sink_capacity = capacity;
    return request;
}

class FakeObserver final : public RecordingTopologyObserver {
public:
    void OnStateTransition(
        const RecordingTopologyTransition& transition) noexcept override {
        transitions.push_back(transition);
        events.push_back(0);
    }

    void OnGapDebt(const RecordingGapDebt& debt) noexcept override {
        gaps.push_back(debt);
        events.push_back(1);
    }

    void OnAdmissionDecision(
        const RecordingAdmissionDecision& decision) noexcept override {
        decisions.push_back(decision);
        events.push_back(2);
    }

    std::vector<RecordingTopologyTransition> transitions;
    std::vector<RecordingGapDebt> gaps;
    std::vector<RecordingAdmissionDecision> decisions;
    std::vector<int> events;
};

TEST(RecordingTopologyTest, ExhaustsEffectiveModeTopologyMatrix) {
    for (RecordingMode mode : kModes) {
        for (RecordBackpressureTopology topology : kTopologies) {
            Result<EffectiveRecordingPolicy> effective =
                MakeEffective(mode, topology);
            const bool expected = ExpectedTopologyAllowed(mode, topology);
            ASSERT_EQ(effective.ok(), expected)
                << "mode=" << static_cast<int>(mode)
                << " topology=" << static_cast<int>(topology)
                << " status=" << effective.status().ToString();
            if (!effective.ok()) continue;

            auto coordinator = RecordingTopologyCoordinator::Create(*effective);
            EXPECT_TRUE(coordinator.ok())
                << "mode=" << static_cast<int>(mode)
                << " topology=" << static_cast<int>(topology)
                << " status=" << coordinator.status().ToString();
            if (coordinator.ok()) {
                EXPECT_EQ((*coordinator)->policy().mode, mode);
                EXPECT_EQ((*coordinator)->policy().backpressure_topology,
                          topology);
            }
        }
    }
}

TEST(RecordingTopologyTest, RejectsForgedOrNonNormalizedEffectivePolicies) {
    auto base = MakeEffective(RecordingMode::kMemoryBuffered,
                              RecordBackpressureTopology::kIsolated);
    ASSERT_TRUE(base.ok()) << base.status().ToString();

    EffectiveRecordingPolicy forged = *base;
    forged.backpressure_topology =
        static_cast<RecordBackpressureTopology>(0xff);
    EXPECT_EQ(RecordingTopologyCoordinator::Create(forged).status().code(),
              StatusCode::kInvalidArgument);

    forged = *base;
    forged.required_ack = RecordAckLevel::kAccepted;
    EXPECT_EQ(RecordingTopologyCoordinator::Create(forged).status().code(),
              StatusCode::kInvalidArgument);

    forged = *base;
    forged.require_complete_recording = true;
    forged.full_policy = BufferFullPolicy::kDropNewest;
    EXPECT_EQ(RecordingTopologyCoordinator::Create(forged).status().code(),
              StatusCode::kInvalidArgument);

    forged = *base;
    forged.mode = RecordingMode::kBestEffort;
    forged.backpressure_topology =
        RecordBackpressureTopology::kStrongConsistent;
    EXPECT_EQ(RecordingTopologyCoordinator::Create(forged).status().code(),
              StatusCode::kInvalidArgument);

    EXPECT_EQ(RecordingTopologyCoordinator::Create(
                  *base, std::numeric_limits<uint64_t>::max())
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
}

TEST(RecordingTopologyTest, ExhaustsTopologyStateAndCapacityAdmissions) {
    constexpr std::array<RecordingTopologyState, 4> states = {
        RecordingTopologyState::kActive,
        RecordingTopologyState::kDegraded,
        RecordingTopologyState::kFailed,
        RecordingTopologyState::kUnbound,
    };
    constexpr std::array<RecordingSinkCapacity, 2> capacities = {
        RecordingSinkCapacity::kAvailable,
        RecordingSinkCapacity::kFull,
    };

    for (RecordBackpressureTopology topology : kTopologies) {
        const RecordingMode mode =
            topology == RecordBackpressureTopology::kStrongConsistent
                ? RecordingMode::kMemoryBuffered
                : RecordingMode::kBestEffort;
        for (RecordingTopologyState state : states) {
            for (RecordingSinkCapacity capacity : capacities) {
                auto coordinator = CreateCoordinator(mode, topology,
                                                     std::nullopt, 10, 0);
                ASSERT_NE(coordinator, nullptr);
                if (state == RecordingTopologyState::kDegraded) {
                    ASSERT_TRUE(coordinator->RecorderLeaseLost(1).ok());
                } else if (state == RecordingTopologyState::kFailed) {
                    ASSERT_TRUE(coordinator->RecorderFailed(1).ok());
                } else if (state == RecordingTopologyState::kUnbound) {
                    ASSERT_TRUE(coordinator->UnbindRecorder(1).ok());
                }

                auto decision =
                    coordinator->DecideAdmission(Request(10, capacity), 2);
                ASSERT_TRUE(decision.ok())
                    << "topology=" << static_cast<int>(topology)
                    << " state=" << static_cast<int>(state)
                    << " capacity=" << static_cast<int>(capacity)
                    << " status=" << decision.status().ToString();

                if (state == RecordingTopologyState::kUnbound) {
                    EXPECT_EQ(decision->outcome,
                              RecordingAdmissionOutcome::kBypassRecording);
                    EXPECT_TRUE(decision->primary_admitted);
                    EXPECT_FALSE(decision->recorder_admitted);
                    EXPECT_EQ(coordinator->next_cursor(), 11u);
                    EXPECT_TRUE(decision->gap_debts.empty());
                    continue;
                }

                if (topology ==
                    RecordBackpressureTopology::kStrongConsistent) {
                    if (state == RecordingTopologyState::kActive &&
                        capacity == RecordingSinkCapacity::kAvailable) {
                        EXPECT_EQ(decision->outcome,
                                  RecordingAdmissionOutcome::kRecord);
                        EXPECT_TRUE(decision->primary_admitted);
                        EXPECT_TRUE(decision->recorder_admitted);
                        EXPECT_EQ(coordinator->next_cursor(), 11u);
                    } else if (state == RecordingTopologyState::kFailed) {
                        EXPECT_EQ(decision->outcome,
                                  RecordingAdmissionOutcome::kFailPrimary);
                        EXPECT_FALSE(decision->primary_admitted);
                        EXPECT_EQ(coordinator->next_cursor(), 10u);
                    } else {
                        EXPECT_EQ(decision->outcome,
                                  RecordingAdmissionOutcome::kBlockPrimary);
                        EXPECT_FALSE(decision->primary_admitted);
                        EXPECT_EQ(coordinator->next_cursor(), 10u);
                    }
                    EXPECT_TRUE(decision->gap_debts.empty());
                } else if (state == RecordingTopologyState::kActive &&
                           capacity == RecordingSinkCapacity::kAvailable) {
                    EXPECT_EQ(decision->outcome,
                              RecordingAdmissionOutcome::kRecord);
                    EXPECT_TRUE(decision->recorder_admitted);
                    EXPECT_TRUE(decision->gap_debts.empty());
                    EXPECT_EQ(coordinator->next_cursor(), 11u);
                } else {
                    EXPECT_EQ(decision->outcome,
                              RecordingAdmissionOutcome::kDropRecording);
                    EXPECT_TRUE(decision->primary_admitted);
                    EXPECT_FALSE(decision->recorder_admitted);
                    ASSERT_EQ(decision->gap_debts.size(), 1u);
                    EXPECT_EQ(decision->gap_debts[0].first_cursor, 10u);
                    EXPECT_EQ(decision->gap_debts[0].end_cursor, 11u);
                    EXPECT_EQ(coordinator->next_cursor(), 11u);
                }
            }
        }
    }
}

TEST(RecordingTopologyTest,
     StrongConsistentLeaseLossBlocksWithoutDropUntilExplicitRecovery) {
    auto coordinator = CreateCoordinator(
        RecordingMode::kMemoryBuffered,
        RecordBackpressureTopology::kStrongConsistent,
        BufferFullPolicy::kBlock, 10, 100);
    ASSERT_NE(coordinator, nullptr);

    auto accepted = coordinator->DecideAdmission(Request(10), 105);
    ASSERT_TRUE(accepted.ok());
    EXPECT_EQ(accepted->outcome, RecordingAdmissionOutcome::kRecord);
    EXPECT_EQ(coordinator->next_cursor(), 11u);

    ASSERT_TRUE(coordinator->RecorderLeaseLost(110).ok());
    EXPECT_EQ(coordinator->state(), RecordingTopologyState::kDegraded);
    auto blocked = coordinator->DecideAdmission(Request(11), 120);
    ASSERT_TRUE(blocked.ok());
    EXPECT_EQ(blocked->outcome, RecordingAdmissionOutcome::kBlockPrimary);
    EXPECT_EQ(blocked->status.code(), StatusCode::kWouldBlock);
    EXPECT_FALSE(blocked->primary_admitted);
    EXPECT_EQ(blocked->cursor_before, 11u);
    EXPECT_EQ(blocked->cursor_after, 11u);
    EXPECT_TRUE(blocked->gap_debts.empty());
    EXPECT_EQ(coordinator->next_cursor(), 11u);

    auto during_degradation = coordinator->MetricsSnapshot(150);
    ASSERT_TRUE(during_degradation.ok());
    EXPECT_EQ(during_degradation->blocked, 1u);
    EXPECT_EQ(during_degradation->dropped, 0u);
    EXPECT_EQ(during_degradation->gaps, 0u);
    EXPECT_EQ(during_degradation->degraded_duration_ns, 40u);

    ASSERT_TRUE(coordinator->RecoverRecorder(160).ok());
    EXPECT_EQ(coordinator->state(), RecordingTopologyState::kActive);
    auto retried = coordinator->DecideAdmission(Request(11), 165);
    ASSERT_TRUE(retried.ok());
    EXPECT_EQ(retried->outcome, RecordingAdmissionOutcome::kRecord);
    EXPECT_EQ(coordinator->next_cursor(), 12u);

    auto recovered = coordinator->MetricsSnapshot(200);
    ASSERT_TRUE(recovered.ok());
    EXPECT_EQ(recovered->degraded_duration_ns, 50u);
}

TEST(RecordingTopologyTest,
     StrongConsistentFailPolicyFailsAndCursorGapNeverCreatesDebt) {
    auto coordinator = CreateCoordinator(
        RecordingMode::kMemoryBuffered,
        RecordBackpressureTopology::kStrongConsistent,
        BufferFullPolicy::kFailRecording, 7, 0);
    ASSERT_NE(coordinator, nullptr);

    auto full = coordinator->DecideAdmission(
        Request(7, RecordingSinkCapacity::kFull), 1);
    ASSERT_TRUE(full.ok());
    EXPECT_EQ(full->outcome, RecordingAdmissionOutcome::kFailPrimary);
    EXPECT_EQ(full->status.code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(coordinator->state(), RecordingTopologyState::kFailed);
    EXPECT_EQ(coordinator->next_cursor(), 7u);
    EXPECT_TRUE(full->gap_debts.empty());

    ASSERT_TRUE(coordinator->RecoverRecorder(2).ok());
    auto cursor_gap = coordinator->DecideAdmission(Request(9), 3);
    ASSERT_TRUE(cursor_gap.ok());
    EXPECT_EQ(cursor_gap->outcome, RecordingAdmissionOutcome::kFailPrimary);
    EXPECT_EQ(cursor_gap->reason, RecordingAdmissionReason::kCursorLag);
    EXPECT_EQ(coordinator->state(), RecordingTopologyState::kFailed);
    EXPECT_EQ(coordinator->next_cursor(), 7u);
    EXPECT_TRUE(cursor_gap->gap_debts.empty());

    auto metrics = coordinator->MetricsSnapshot(4);
    ASSERT_TRUE(metrics.ok());
    EXPECT_EQ(metrics->dropped, 0u);
    EXPECT_EQ(metrics->gaps, 0u);
}

TEST(RecordingTopologyTest,
     IsolatedFullAlwaysProtectsPrimaryAndReturnsDropGapDebt) {
    struct Configuration {
        RecordingMode mode;
        BufferFullPolicy full_policy;
    };
    constexpr std::array<Configuration, 4> configurations = {{
        {RecordingMode::kBestEffort, BufferFullPolicy::kDropNewest},
        {RecordingMode::kSnapshot, BufferFullPolicy::kDropOldest},
        {RecordingMode::kMemoryBuffered, BufferFullPolicy::kBlock},
        {RecordingMode::kMemoryBuffered, BufferFullPolicy::kFailRecording},
    }};

    for (const Configuration& configuration : configurations) {
        auto coordinator = CreateCoordinator(
            configuration.mode, RecordBackpressureTopology::kIsolated,
            configuration.full_policy, 20, 0);
        ASSERT_NE(coordinator, nullptr);
        auto decision = coordinator->DecideAdmission(
            Request(20, RecordingSinkCapacity::kFull), 1);
        ASSERT_TRUE(decision.ok()) << decision.status().ToString();
        EXPECT_EQ(decision->outcome,
                  RecordingAdmissionOutcome::kDropRecording);
        EXPECT_EQ(decision->status.code(), StatusCode::kDegraded);
        EXPECT_TRUE(decision->primary_admitted);
        EXPECT_FALSE(decision->recorder_admitted);
        ASSERT_EQ(decision->gap_debts.size(), 1u);
        EXPECT_EQ(decision->gap_debts[0].first_cursor, 20u);
        EXPECT_EQ(decision->gap_debts[0].end_cursor, 21u);
        EXPECT_EQ(decision->gap_debts[0].record_count(), 1u);
        EXPECT_EQ(decision->gap_debts[0].reason,
                  RecordingGapReason::kIsolatedFanoutFull);
        EXPECT_EQ(coordinator->next_cursor(), 21u);

        auto metrics = coordinator->MetricsSnapshot(2);
        ASSERT_TRUE(metrics.ok());
        EXPECT_EQ(metrics->blocked, 0u);
        EXPECT_EQ(metrics->dropped, 1u);
        EXPECT_EQ(metrics->gaps, 1u);
        EXPECT_EQ(coordinator->state(),
                  configuration.full_policy ==
                          BufferFullPolicy::kFailRecording
                      ? RecordingTopologyState::kFailed
                      : RecordingTopologyState::kActive);
    }
}

TEST(RecordingTopologyTest, IsolatedLagRecordsVisibleItemAndReturnsGapDebt) {
    auto coordinator = CreateCoordinator(
        RecordingMode::kMemoryBuffered,
        RecordBackpressureTopology::kIsolated, BufferFullPolicy::kBlock, 5, 0);
    ASSERT_NE(coordinator, nullptr);

    auto decision = coordinator->DecideAdmission(Request(8), 1);
    ASSERT_TRUE(decision.ok());
    EXPECT_EQ(decision->outcome, RecordingAdmissionOutcome::kRecord);
    EXPECT_EQ(decision->reason, RecordingAdmissionReason::kCursorLag);
    EXPECT_EQ(decision->status.code(), StatusCode::kDegraded);
    EXPECT_TRUE(decision->primary_admitted);
    EXPECT_TRUE(decision->recorder_admitted);
    ASSERT_EQ(decision->gap_debts.size(), 1u);
    EXPECT_EQ(decision->gap_debts[0].first_cursor, 5u);
    EXPECT_EQ(decision->gap_debts[0].end_cursor, 8u);
    EXPECT_EQ(decision->gap_debts[0].record_count(), 3u);
    EXPECT_EQ(decision->gap_debts[0].reason,
              RecordingGapReason::kIsolatedFanoutLag);
    EXPECT_EQ(coordinator->next_cursor(), 9u);
}

TEST(RecordingTopologyTest,
     BestEffortLagAndFullAdvanceCursorWithSeparateGapDebts) {
    auto coordinator = CreateCoordinator(
        RecordingMode::kBestEffort,
        RecordBackpressureTopology::kBestEffort,
        BufferFullPolicy::kDropNewest, 100, 0);
    ASSERT_NE(coordinator, nullptr);

    auto decision = coordinator->DecideAdmission(
        Request(103, RecordingSinkCapacity::kFull), 1);
    ASSERT_TRUE(decision.ok());
    EXPECT_EQ(decision->outcome,
              RecordingAdmissionOutcome::kDropRecording);
    EXPECT_TRUE(decision->primary_admitted);
    EXPECT_FALSE(decision->recorder_admitted);
    EXPECT_EQ(decision->cursor_before, 100u);
    EXPECT_EQ(decision->cursor_after, 104u);
    ASSERT_EQ(decision->gap_debts.size(), 2u);
    EXPECT_EQ(decision->gap_debts[0].first_cursor, 100u);
    EXPECT_EQ(decision->gap_debts[0].end_cursor, 103u);
    EXPECT_EQ(decision->gap_debts[0].reason,
              RecordingGapReason::kBestEffortLag);
    EXPECT_EQ(decision->gap_debts[1].first_cursor, 103u);
    EXPECT_EQ(decision->gap_debts[1].end_cursor, 104u);
    EXPECT_EQ(decision->gap_debts[1].reason,
              RecordingGapReason::kBestEffortBufferFull);
    EXPECT_LT(decision->gap_debts[0].debt_id,
              decision->gap_debts[1].debt_id);
    EXPECT_EQ(coordinator->next_cursor(), 104u);

    auto metrics = coordinator->MetricsSnapshot(2);
    ASSERT_TRUE(metrics.ok());
    EXPECT_EQ(metrics->blocked, 0u);
    EXPECT_EQ(metrics->dropped, 4u);
    EXPECT_EQ(metrics->gaps, 2u);
}

TEST(RecordingTopologyTest,
     RecoveryUnbindAndRebindAreExplicitForEveryTopology) {
    for (RecordBackpressureTopology topology : kTopologies) {
        const RecordingMode mode =
            topology == RecordBackpressureTopology::kStrongConsistent
                ? RecordingMode::kMemoryBuffered
                : RecordingMode::kBestEffort;
        auto coordinator = CreateCoordinator(mode, topology, std::nullopt, 3, 0);
        ASSERT_NE(coordinator, nullptr);

        EXPECT_EQ(coordinator->RecoverRecorder(1).code(),
                  StatusCode::kInvalidArgument);
        ASSERT_TRUE(coordinator->RecorderLeaseLost(2).ok());
        EXPECT_EQ(coordinator->state(), RecordingTopologyState::kDegraded);

        auto while_degraded = coordinator->DecideAdmission(Request(3), 3);
        ASSERT_TRUE(while_degraded.ok());
        EXPECT_NE(while_degraded->outcome,
                  RecordingAdmissionOutcome::kRecord);
        EXPECT_EQ(coordinator->state(), RecordingTopologyState::kDegraded);

        ASSERT_TRUE(coordinator->RecoverRecorder(4).ok());
        EXPECT_EQ(coordinator->state(), RecordingTopologyState::kActive);
        ASSERT_TRUE(coordinator->UnbindRecorder(5).ok());
        EXPECT_EQ(coordinator->state(), RecordingTopologyState::kUnbound);
        EXPECT_EQ(coordinator->RecorderLeaseLost(6).code(),
                  StatusCode::kInvalidArgument);

        const uint64_t visible = coordinator->next_cursor() + 2;
        auto bypass = coordinator->DecideAdmission(Request(visible), 7);
        ASSERT_TRUE(bypass.ok());
        EXPECT_EQ(bypass->outcome,
                  RecordingAdmissionOutcome::kBypassRecording);
        EXPECT_TRUE(bypass->gap_debts.empty());
        ASSERT_TRUE(coordinator->RebindRecorder(50, 8).ok());
        EXPECT_EQ(coordinator->state(), RecordingTopologyState::kActive);
        EXPECT_EQ(coordinator->next_cursor(), 50u);
        EXPECT_EQ(coordinator->RebindRecorder(50, 9).code(),
                  StatusCode::kInvalidArgument);
    }
}

TEST(RecordingTopologyTest, MetricsAccumulateImpairedIntervalsExactly) {
    auto coordinator = CreateCoordinator(
        RecordingMode::kBestEffort,
        RecordBackpressureTopology::kBestEffort,
        BufferFullPolicy::kDropNewest, 0, 10);
    ASSERT_NE(coordinator, nullptr);

    ASSERT_TRUE(coordinator->RecorderLeaseLost(20).ok());
    auto first = coordinator->MetricsSnapshot(25);
    ASSERT_TRUE(first.ok());
    EXPECT_EQ(first->degraded_duration_ns, 5u);

    ASSERT_TRUE(coordinator->RecorderFailed(30).ok());
    auto failed = coordinator->MetricsSnapshot(35);
    ASSERT_TRUE(failed.ok());
    EXPECT_EQ(failed->degraded_duration_ns, 15u);

    ASSERT_TRUE(coordinator->RecoverRecorder(40).ok());
    auto recovered = coordinator->MetricsSnapshot(50);
    ASSERT_TRUE(recovered.ok());
    EXPECT_EQ(recovered->degraded_duration_ns, 20u);

    ASSERT_TRUE(coordinator->RecorderLeaseLost(55).ok());
    ASSERT_TRUE(coordinator->UnbindRecorder(65).ok());
    auto unbound = coordinator->MetricsSnapshot(100);
    ASSERT_TRUE(unbound.ok());
    EXPECT_EQ(unbound->degraded_duration_ns, 30u);
}

TEST(RecordingTopologyTest, ObserverProvidesOrderedCallbackAdapterSeam) {
    FakeObserver observer;
    auto coordinator = CreateCoordinator(
        RecordingMode::kBestEffort,
        RecordBackpressureTopology::kIsolated,
        BufferFullPolicy::kDropNewest, 0, 0, &observer);
    ASSERT_NE(coordinator, nullptr);

    ASSERT_TRUE(coordinator->RecorderLeaseLost(1).ok());
    auto decision = coordinator->DecideAdmission(Request(2), 2);
    ASSERT_TRUE(decision.ok());
    ASSERT_TRUE(coordinator->RecoverRecorder(3).ok());

    ASSERT_EQ(observer.transitions.size(), 2u);
    EXPECT_EQ(observer.transitions[0].from, RecordingTopologyState::kActive);
    EXPECT_EQ(observer.transitions[0].to,
              RecordingTopologyState::kDegraded);
    EXPECT_EQ(observer.transitions[1].from,
              RecordingTopologyState::kDegraded);
    EXPECT_EQ(observer.transitions[1].to, RecordingTopologyState::kActive);
    ASSERT_EQ(observer.gaps.size(), 1u);
    EXPECT_EQ(observer.gaps[0].first_cursor, 0u);
    EXPECT_EQ(observer.gaps[0].end_cursor, 3u);
    ASSERT_EQ(observer.decisions.size(), 1u);
    EXPECT_EQ(observer.decisions[0].outcome,
              RecordingAdmissionOutcome::kDropRecording);
    EXPECT_EQ(observer.events, (std::vector<int>{0, 1, 2, 0}));
}

TEST(RecordingTopologyTest, RejectsInvalidObservationsWithoutMutation) {
    auto coordinator = CreateCoordinator(
        RecordingMode::kBestEffort,
        RecordBackpressureTopology::kBestEffort,
        BufferFullPolicy::kDropNewest, 10, 100);
    ASSERT_NE(coordinator, nullptr);

    RecordingAdmissionRequest invalid_capacity = Request(10);
    invalid_capacity.sink_capacity =
        static_cast<RecordingSinkCapacity>(0xff);
    EXPECT_EQ(coordinator->DecideAdmission(invalid_capacity, 101)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(coordinator->DecideAdmission(Request(9), 101).status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(coordinator
                  ->DecideAdmission(
                      Request(std::numeric_limits<uint64_t>::max()), 101)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(coordinator->DecideAdmission(Request(10), 99).status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(coordinator->MetricsSnapshot(99).status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(coordinator->next_cursor(), 10u);
    EXPECT_EQ(coordinator->state(), RecordingTopologyState::kActive);

    auto metrics = coordinator->MetricsSnapshot(101);
    ASSERT_TRUE(metrics.ok());
    EXPECT_EQ(metrics->blocked, 0u);
    EXPECT_EQ(metrics->dropped, 0u);
    EXPECT_EQ(metrics->gaps, 0u);
    EXPECT_EQ(metrics->degraded_duration_ns, 0u);
}

}  // namespace
}  // namespace mino::storage
