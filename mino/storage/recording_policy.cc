// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recording_policy.h"

#include <optional>
#include <string_view>

#include "mino/common/status.h"

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool IsValidMode(RecordingMode mode) noexcept {
    switch (mode) {
        case RecordingMode::kBestEffort:
        case RecordingMode::kMemoryBuffered:
        case RecordingMode::kDurable:
        case RecordingMode::kSnapshot:
            return true;
    }
    return false;
}

bool IsValidTopology(RecordBackpressureTopology topology) noexcept {
    switch (topology) {
        case RecordBackpressureTopology::kStrongConsistent:
        case RecordBackpressureTopology::kIsolated:
        case RecordBackpressureTopology::kBestEffort:
            return true;
    }
    return false;
}

bool IsValidFullPolicy(BufferFullPolicy policy) noexcept {
    switch (policy) {
        case BufferFullPolicy::kBlock:
        case BufferFullPolicy::kDropNewest:
        case BufferFullPolicy::kDropOldest:
        case BufferFullPolicy::kFailRecording:
            return true;
    }
    return false;
}

bool IsValidAckLevel(RecordAckLevel level) noexcept {
    switch (level) {
        case RecordAckLevel::kAccepted:
        case RecordAckLevel::kBuffered:
        case RecordAckLevel::kWritten:
        case RecordAckLevel::kDurable:
            return true;
    }
    return false;
}

bool IsValidSyncPolicy(SegmentSyncPolicy policy) noexcept {
    switch (policy) {
        case SegmentSyncPolicy::kNone:
        case SegmentSyncPolicy::kInterval:
        case SegmentSyncPolicy::kPerBatch:
        case SegmentSyncPolicy::kPerRecord:
            return true;
    }
    return false;
}

bool IsNonDropping(BufferFullPolicy policy) noexcept {
    return policy == BufferFullPolicy::kBlock ||
           policy == BufferFullPolicy::kFailRecording;
}

BufferFullPolicy DefaultFullPolicy(RecordingMode mode) noexcept {
    switch (mode) {
        case RecordingMode::kBestEffort:
            return BufferFullPolicy::kDropNewest;
        case RecordingMode::kMemoryBuffered:
        case RecordingMode::kDurable:
            return BufferFullPolicy::kBlock;
        case RecordingMode::kSnapshot:
            return BufferFullPolicy::kDropOldest;
    }
    return BufferFullPolicy::kBlock;
}

std::optional<RecordAckLevel> RequiredAck(RecordingMode mode) noexcept {
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

std::optional<SegmentSyncPolicy> DefaultSyncPolicy(
    RecordingMode mode) noexcept {
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

Status AckMismatch(RecordingMode mode) {
    switch (mode) {
        case RecordingMode::kBestEffort:
            return Invalid(
                "best-effort recording requires RecordAckLevel::kAccepted");
        case RecordingMode::kMemoryBuffered:
            return Invalid(
                "memory-buffered recording requires "
                "RecordAckLevel::kBuffered");
        case RecordingMode::kDurable:
            return Invalid(
                "durable recording requires RecordAckLevel::kDurable");
        case RecordingMode::kSnapshot:
            return Invalid("snapshot recording does not use RecordAckLevel");
    }
    return Invalid("invalid recording mode");
}

}  // namespace

bool IsRecordingModeTopologyAllowed(
    RecordingMode mode, RecordBackpressureTopology topology) noexcept {
    if (!IsValidMode(mode) || !IsValidTopology(topology)) return false;

    switch (topology) {
        case RecordBackpressureTopology::kStrongConsistent:
            return mode == RecordingMode::kMemoryBuffered ||
                   mode == RecordingMode::kDurable;
        case RecordBackpressureTopology::kIsolated:
            return true;
        case RecordBackpressureTopology::kBestEffort:
            return mode != RecordingMode::kDurable;
    }
    return false;
}

Result<EffectiveRecordingPolicy> ValidateRecordingPolicy(
    const RecordingPolicy& policy) {
    if (!IsValidMode(policy.mode)) return Invalid("invalid recording mode");
    if (!IsValidTopology(policy.backpressure_topology)) {
        return Invalid("invalid recording backpressure topology");
    }
    if (policy.full_policy.has_value() &&
        !IsValidFullPolicy(*policy.full_policy)) {
        return Invalid("invalid recorder buffer full policy");
    }
    if (policy.ack_level.has_value() &&
        !IsValidAckLevel(*policy.ack_level)) {
        return Invalid("invalid record acknowledgement level");
    }
    if (policy.sync_policy.has_value() &&
        !IsValidSyncPolicy(*policy.sync_policy)) {
        return Invalid("invalid segment synchronization policy");
    }
    if (!IsRecordingModeTopologyAllowed(policy.mode,
                                        policy.backpressure_topology)) {
        return Invalid(
            "recording mode and backpressure topology are not a legal "
            "combination");
    }

    if (policy.mode == RecordingMode::kSnapshot) {
        if (!policy.is_state_topic) {
            return Invalid("snapshot recording requires a state topic");
        }
        if (policy.ack_level.has_value()) {
            return Invalid("snapshot recording does not use RecordAckLevel");
        }
        if (policy.sync_policy.has_value()) {
            return Invalid(
                "snapshot recording does not use SegmentSyncPolicy");
        }
        if (policy.require_complete_recording) {
            return Invalid(
                "snapshot recording cannot satisfy complete-recording "
                "semantics");
        }
    } else if (policy.ack_level.has_value() &&
               policy.ack_level != RequiredAck(policy.mode)) {
        return AckMismatch(policy.mode);
    }

    if (policy.require_complete_recording &&
        policy.mode == RecordingMode::kBestEffort) {
        return Invalid(
            "best-effort recording cannot satisfy complete-recording "
            "semantics");
    }
    if (policy.require_complete_recording &&
        policy.backpressure_topology ==
            RecordBackpressureTopology::kBestEffort) {
        return Invalid(
            "best-effort backpressure topology cannot satisfy "
            "complete-recording semantics");
    }

    EffectiveRecordingPolicy effective;
    effective.mode = policy.mode;
    effective.backpressure_topology = policy.backpressure_topology;
    effective.full_policy =
        policy.full_policy.value_or(DefaultFullPolicy(policy.mode));
    effective.required_ack = RequiredAck(policy.mode);
    effective.sync_policy = policy.sync_policy.has_value()
                                ? policy.sync_policy
                                : DefaultSyncPolicy(policy.mode);
    effective.require_complete_recording =
        policy.require_complete_recording ||
        policy.mode == RecordingMode::kDurable ||
        policy.backpressure_topology ==
            RecordBackpressureTopology::kStrongConsistent;

    if (policy.mode == RecordingMode::kSnapshot &&
        effective.full_policy != BufferFullPolicy::kDropOldest) {
        return Invalid(
            "snapshot recording requires BufferFullPolicy::kDropOldest");
    }
    if (effective.require_complete_recording &&
        !IsNonDropping(effective.full_policy)) {
        return Invalid(
            "complete recording requires BufferFullPolicy::kBlock or "
            "BufferFullPolicy::kFailRecording");
    }
    if (policy.mode == RecordingMode::kDurable &&
        effective.sync_policy == SegmentSyncPolicy::kNone) {
        return Invalid(
            "durable recording requires interval, per-batch, or per-record "
            "synchronization");
    }

    return effective;
}

}  // namespace mino::storage
