// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RECORDING_POLICY_H_
#define MINO_STORAGE_RECORDING_POLICY_H_

#include <cstdint>
#include <optional>

#include "mino/common/result.h"
#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/segment_writer.h"

namespace mino::storage {

enum class RecordingMode : uint8_t {
    kBestEffort = 0,
    kMemoryBuffered = 1,
    kDurable = 2,
    kSnapshot = 3,
};

enum class RecordBackpressureTopology : uint8_t {
    kStrongConsistent = 0,
    kIsolated = 1,
    kBestEffort = 2,
};

enum class RecordAckLevel : uint8_t {
    kAccepted = 0,
    kBuffered = 1,
    kWritten = 2,
    kDurable = 3,
};

// Optional fields mean "use the mode default". Snapshot recording does not use
// RecordAckLevel or SegmentSyncPolicy and therefore requires both to be absent.
struct RecordingPolicy {
    RecordingMode mode = RecordingMode::kBestEffort;
    RecordBackpressureTopology backpressure_topology =
        RecordBackpressureTopology::kIsolated;
    std::optional<BufferFullPolicy> full_policy;
    std::optional<RecordAckLevel> ack_level;
    std::optional<SegmentSyncPolicy> sync_policy;

    // A complete recording may backpressure or stop, but may not silently lose
    // records. Durable and strong-consistent policies imply this requirement.
    bool require_complete_recording = false;

    // Snapshot mode is only defined for state-valued topics.
    bool is_state_topic = false;
};

// Fully resolved policy consumed by recorder admission, acknowledgement, and
// writer setup. required_ack and sync_policy are absent only for snapshot mode.
struct EffectiveRecordingPolicy {
    RecordingMode mode = RecordingMode::kBestEffort;
    RecordBackpressureTopology backpressure_topology =
        RecordBackpressureTopology::kIsolated;
    BufferFullPolicy full_policy = BufferFullPolicy::kDropNewest;
    std::optional<RecordAckLevel> required_ack = RecordAckLevel::kAccepted;
    std::optional<SegmentSyncPolicy> sync_policy = SegmentSyncPolicy::kNone;
    bool require_complete_recording = false;
};

// Implements the design-section 17.2 mode/topology matrix. Invalid enum values
// are rejected rather than used as table indexes.
bool IsRecordingModeTopologyAllowed(
    RecordingMode mode, RecordBackpressureTopology topology) noexcept;

// Resolves defaults and rejects combinations whose configured acknowledgement,
// synchronization, buffer-full, completeness, or snapshot semantics conflict.
Result<EffectiveRecordingPolicy> ValidateRecordingPolicy(
    const RecordingPolicy& policy);

}  // namespace mino::storage

#endif  // MINO_STORAGE_RECORDING_POLICY_H_
