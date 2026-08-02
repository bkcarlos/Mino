// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recording_policy.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <optional>

#include "mino/common/status.h"

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

constexpr std::array<BufferFullPolicy, 4> kFullPolicies = {
    BufferFullPolicy::kBlock,
    BufferFullPolicy::kDropNewest,
    BufferFullPolicy::kDropOldest,
    BufferFullPolicy::kFailRecording,
};

constexpr std::array<RecordAckLevel, 4> kAckLevels = {
    RecordAckLevel::kAccepted,
    RecordAckLevel::kBuffered,
    RecordAckLevel::kWritten,
    RecordAckLevel::kDurable,
};

constexpr std::array<SegmentSyncPolicy, 4> kSyncPolicies = {
    SegmentSyncPolicy::kNone,
    SegmentSyncPolicy::kInterval,
    SegmentSyncPolicy::kPerBatch,
    SegmentSyncPolicy::kPerRecord,
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

bool IsNonDropping(BufferFullPolicy policy) {
    return policy == BufferFullPolicy::kBlock ||
           policy == BufferFullPolicy::kFailRecording;
}

bool ExpectedFullPolicyAllowed(
    RecordingMode mode, RecordBackpressureTopology topology,
    BufferFullPolicy full_policy, bool require_complete) {
    if (!ExpectedTopologyAllowed(mode, topology)) return false;
    if (require_complete &&
        (mode == RecordingMode::kBestEffort ||
         mode == RecordingMode::kSnapshot ||
         topology == RecordBackpressureTopology::kBestEffort)) {
        return false;
    }
    if (mode == RecordingMode::kSnapshot &&
        full_policy != BufferFullPolicy::kDropOldest) {
        return false;
    }

    const bool effective_complete =
        require_complete || mode == RecordingMode::kDurable ||
        topology == RecordBackpressureTopology::kStrongConsistent;
    return !effective_complete || IsNonDropping(full_policy);
}

std::optional<RecordAckLevel> ExpectedAck(RecordingMode mode) {
    switch (mode) {
        case RecordingMode::kBestEffort:
            return RecordAckLevel::kAccepted;
        case RecordingMode::kMemoryBuffered:
            return RecordAckLevel::kBuffered;
        case RecordingMode::kDurable:
            return RecordAckLevel::kDurable;
        case RecordingMode::kSnapshot:
            return std::nullopt;
    }
    return std::nullopt;
}

std::optional<SegmentSyncPolicy> ExpectedDefaultSync(RecordingMode mode) {
    switch (mode) {
        case RecordingMode::kBestEffort:
            return SegmentSyncPolicy::kNone;
        case RecordingMode::kMemoryBuffered:
        case RecordingMode::kDurable:
            return SegmentSyncPolicy::kInterval;
        case RecordingMode::kSnapshot:
            return std::nullopt;
    }
    return std::nullopt;
}

RecordingPolicy BasePolicy(RecordingMode mode) {
    RecordingPolicy policy;
    policy.mode = mode;
    policy.backpressure_topology = RecordBackpressureTopology::kIsolated;
    policy.is_state_topic = mode == RecordingMode::kSnapshot;
    return policy;
}

TEST(RecordingPolicyTest, ExhaustsModeAndBackpressureTopologyMatrix) {
    for (RecordingMode mode : kModes) {
        for (RecordBackpressureTopology topology : kTopologies) {
            RecordingPolicy policy = BasePolicy(mode);
            policy.backpressure_topology = topology;

            const bool expected = ExpectedTopologyAllowed(mode, topology);
            EXPECT_EQ(IsRecordingModeTopologyAllowed(mode, topology), expected)
                << "mode=" << static_cast<int>(mode)
                << " topology=" << static_cast<int>(topology);

            auto validated = ValidateRecordingPolicy(policy);
            EXPECT_EQ(validated.ok(), expected)
                << "mode=" << static_cast<int>(mode)
                << " topology=" << static_cast<int>(topology)
                << " status=" << validated.status().ToString();
            if (validated.ok()) {
                EXPECT_EQ(validated->mode, mode);
                EXPECT_EQ(validated->backpressure_topology, topology);
            } else {
                EXPECT_EQ(validated.status().code(),
                          StatusCode::kInvalidArgument);
            }
        }
    }
}

TEST(RecordingPolicyTest, ExhaustsFullPolicyAndCompletenessMatrix) {
    for (RecordingMode mode : kModes) {
        for (RecordBackpressureTopology topology : kTopologies) {
            for (BufferFullPolicy full_policy : kFullPolicies) {
                for (bool require_complete : {false, true}) {
                    RecordingPolicy policy = BasePolicy(mode);
                    policy.backpressure_topology = topology;
                    policy.full_policy = full_policy;
                    policy.require_complete_recording = require_complete;

                    const bool expected = ExpectedFullPolicyAllowed(
                        mode, topology, full_policy, require_complete);
                    auto validated = ValidateRecordingPolicy(policy);
                    EXPECT_EQ(validated.ok(), expected)
                        << "mode=" << static_cast<int>(mode)
                        << " topology=" << static_cast<int>(topology)
                        << " full_policy=" << static_cast<int>(full_policy)
                        << " complete=" << require_complete
                        << " status=" << validated.status().ToString();
                    if (validated.ok()) {
                        EXPECT_EQ(validated->full_policy, full_policy);
                        EXPECT_EQ(
                            validated->require_complete_recording,
                            require_complete ||
                                mode == RecordingMode::kDurable ||
                                topology == RecordBackpressureTopology::
                                                kStrongConsistent);
                    } else {
                        EXPECT_EQ(validated.status().code(),
                                  StatusCode::kInvalidArgument);
                    }
                }
            }
        }
    }
}

TEST(RecordingPolicyTest, ExhaustsAckAndSyncPolicyMatrix) {
    std::array<std::optional<RecordAckLevel>, 5> ack_requests;
    ack_requests[0] = std::nullopt;
    for (size_t index = 0; index < kAckLevels.size(); ++index) {
        ack_requests[index + 1] = kAckLevels[index];
    }
    std::array<std::optional<SegmentSyncPolicy>, 5> sync_requests;
    sync_requests[0] = std::nullopt;
    for (size_t index = 0; index < kSyncPolicies.size(); ++index) {
        sync_requests[index + 1] = kSyncPolicies[index];
    }

    for (RecordingMode mode : kModes) {
        for (std::optional<RecordAckLevel> ack : ack_requests) {
            for (std::optional<SegmentSyncPolicy> sync : sync_requests) {
                RecordingPolicy policy = BasePolicy(mode);
                policy.ack_level = ack;
                policy.sync_policy = sync;

                bool expected = false;
                if (mode == RecordingMode::kSnapshot) {
                    expected = !ack.has_value() && !sync.has_value();
                } else {
                    const bool ack_allowed =
                        !ack.has_value() || ack == ExpectedAck(mode);
                    const bool sync_allowed =
                        mode != RecordingMode::kDurable ||
                        !sync.has_value() ||
                        sync != SegmentSyncPolicy::kNone;
                    expected = ack_allowed && sync_allowed;
                }

                auto validated = ValidateRecordingPolicy(policy);
                EXPECT_EQ(validated.ok(), expected)
                    << "mode=" << static_cast<int>(mode)
                    << " ack="
                    << (ack.has_value() ? static_cast<int>(*ack) : -1)
                    << " sync="
                    << (sync.has_value() ? static_cast<int>(*sync) : -1)
                    << " status=" << validated.status().ToString();
                if (validated.ok()) {
                    EXPECT_EQ(validated->required_ack, ExpectedAck(mode));
                    EXPECT_EQ(validated->sync_policy,
                              sync.has_value() ? sync
                                               : ExpectedDefaultSync(mode));
                } else {
                    EXPECT_EQ(validated.status().code(),
                              StatusCode::kInvalidArgument);
                }
            }
        }
    }
}

TEST(RecordingPolicyTest, ResolvesModeDefaultsIntoExplicitEffectivePolicy) {
    const std::array<BufferFullPolicy, 4> expected_full = {
        BufferFullPolicy::kDropNewest,
        BufferFullPolicy::kBlock,
        BufferFullPolicy::kBlock,
        BufferFullPolicy::kDropOldest,
    };

    for (size_t index = 0; index < kModes.size(); ++index) {
        const RecordingMode mode = kModes[index];
        auto validated = ValidateRecordingPolicy(BasePolicy(mode));
        ASSERT_TRUE(validated.ok()) << validated.status().ToString();
        EXPECT_EQ(validated->full_policy, expected_full[index]);
        EXPECT_EQ(validated->required_ack, ExpectedAck(mode));
        EXPECT_EQ(validated->sync_policy, ExpectedDefaultSync(mode));
        EXPECT_EQ(validated->require_complete_recording,
                  mode == RecordingMode::kDurable);
    }
}

TEST(RecordingPolicyTest, SnapshotRequiresStateTopic) {
    RecordingPolicy policy = BasePolicy(RecordingMode::kSnapshot);
    policy.is_state_topic = false;
    auto validated = ValidateRecordingPolicy(policy);
    ASSERT_FALSE(validated.ok());
    EXPECT_EQ(validated.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(validated.status().message().find("state topic"),
              std::string_view::npos);
}

TEST(RecordingPolicyTest, RejectsEveryInvalidEnumAtTheBoundary) {
    constexpr auto invalid_mode = static_cast<RecordingMode>(0xff);
    constexpr auto invalid_topology =
        static_cast<RecordBackpressureTopology>(0xff);
    constexpr auto invalid_full = static_cast<BufferFullPolicy>(0xff);
    constexpr auto invalid_ack = static_cast<RecordAckLevel>(0xff);
    constexpr auto invalid_sync = static_cast<SegmentSyncPolicy>(0xff);

    EXPECT_FALSE(IsRecordingModeTopologyAllowed(
        invalid_mode, RecordBackpressureTopology::kIsolated));
    EXPECT_FALSE(IsRecordingModeTopologyAllowed(
        RecordingMode::kMemoryBuffered, invalid_topology));

    RecordingPolicy policy = BasePolicy(RecordingMode::kMemoryBuffered);
    policy.mode = invalid_mode;
    EXPECT_EQ(ValidateRecordingPolicy(policy).status().code(),
              StatusCode::kInvalidArgument);

    policy = BasePolicy(RecordingMode::kMemoryBuffered);
    policy.backpressure_topology = invalid_topology;
    EXPECT_EQ(ValidateRecordingPolicy(policy).status().code(),
              StatusCode::kInvalidArgument);

    policy = BasePolicy(RecordingMode::kMemoryBuffered);
    policy.full_policy = invalid_full;
    EXPECT_EQ(ValidateRecordingPolicy(policy).status().code(),
              StatusCode::kInvalidArgument);

    policy = BasePolicy(RecordingMode::kMemoryBuffered);
    policy.ack_level = invalid_ack;
    EXPECT_EQ(ValidateRecordingPolicy(policy).status().code(),
              StatusCode::kInvalidArgument);

    policy = BasePolicy(RecordingMode::kMemoryBuffered);
    policy.sync_policy = invalid_sync;
    EXPECT_EQ(ValidateRecordingPolicy(policy).status().code(),
              StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::storage
