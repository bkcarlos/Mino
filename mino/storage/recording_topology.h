// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RECORDING_TOPOLOGY_H_
#define MINO_STORAGE_RECORDING_TOPOLOGY_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/storage/recording_policy.h"

namespace mino::storage {

// Process-local state of one topic's recorder binding. Recovery and binding
// changes are never inferred from a successful admission; callers must invoke
// RecoverRecorder(), UnbindRecorder(), or RebindRecorder() explicitly.
enum class RecordingTopologyState : uint8_t {
    kActive = 0,
    kDegraded = 1,
    kFailed = 2,
    kUnbound = 3,
};

enum class RecordingSinkCapacity : uint8_t {
    kAvailable = 0,
    kFull = 1,
};

enum class RecordingAdmissionOutcome : uint8_t {
    // The same source item may proceed on the primary path and recorder path.
    kRecord = 0,
    // Strong-consistent recording prevents the primary path from proceeding.
    kBlockPrimary = 1,
    kFailPrimary = 2,
    // The primary path proceeds, but recording drops one or more source items.
    kDropRecording = 3,
    // An explicit unbind permits the primary path without recording or Gap debt.
    kBypassRecording = 4,
};

enum class RecordingAdmissionReason : uint8_t {
    kReady = 0,
    kBufferFull = 1,
    kRecorderDegraded = 2,
    kRecorderFailed = 3,
    kCursorLag = 4,
    kExplicitlyUnbound = 5,
};

enum class RecordingGapReason : uint8_t {
    kIsolatedFanoutLag = 0,
    kIsolatedFanoutFull = 1,
    kIsolatedRecorderUnavailable = 2,
    kBestEffortLag = 3,
    kBestEffortBufferFull = 4,
    kBestEffortRecorderUnavailable = 5,
};

// Cursor ranges are half-open: [first_cursor, end_cursor). The coordinator
// rejects UINT64_MAX cursors so end_cursor is always representable.
struct RecordingGapDebt {
    uint64_t debt_id = 0;
    uint64_t first_cursor = 0;
    uint64_t end_cursor = 0;
    RecordingGapReason reason = RecordingGapReason::kBestEffortLag;

    uint64_t record_count() const noexcept {
        return end_cursor - first_cursor;
    }

    friend bool operator==(const RecordingGapDebt&,
                           const RecordingGapDebt&) = default;
};

struct RecordingAdmissionRequest {
    // Cursor of the next source item currently visible to the adapter. A value
    // greater than next_cursor() means that source items were missed.
    uint64_t available_cursor = 0;
    RecordingSinkCapacity sink_capacity = RecordingSinkCapacity::kAvailable;
};

struct RecordingAdmissionDecision {
    RecordingAdmissionOutcome outcome = RecordingAdmissionOutcome::kFailPrimary;
    RecordingAdmissionReason reason = RecordingAdmissionReason::kReady;
    // kDropRecording carries kDegraded while still admitting the primary path.
    // kBlockPrimary and kFailPrimary carry the corresponding actionable error.
    Status status = Status::Ok();
    bool primary_admitted = false;
    bool recorder_admitted = false;
    uint64_t cursor_before = 0;
    uint64_t cursor_after = 0;
    std::vector<RecordingGapDebt> gap_debts;
};

struct RecordingTopologyTransition {
    RecordingTopologyState from = RecordingTopologyState::kActive;
    RecordingTopologyState to = RecordingTopologyState::kActive;
    uint64_t at_ns = 0;
};

// Optional callback seam for adapting the pure decisions to a Bus, metrics
// exporter, or Gap/Tombstone writer. Callbacks must not call back into the same
// coordinator. Their lifetime must exceed the coordinator's lifetime.
class RecordingTopologyObserver {
public:
    virtual ~RecordingTopologyObserver() = default;
    virtual void OnStateTransition(
        const RecordingTopologyTransition& transition) noexcept = 0;
    virtual void OnGapDebt(const RecordingGapDebt& debt) noexcept = 0;
    virtual void OnAdmissionDecision(
        const RecordingAdmissionDecision& decision) noexcept = 0;
};

struct RecordingTopologyMetrics {
    uint64_t blocked = 0;
    uint64_t dropped = 0;
    uint64_t gaps = 0;
    // Includes time in both kDegraded and kFailed while a recorder is bound.
    uint64_t degraded_duration_ns = 0;
};

// Deterministic, single-caller coordination core. It owns no threads and does
// no I/O. The caller supplies monotonic timestamps and source/sink observations.
class RecordingTopologyCoordinator final {
public:
    static Result<std::unique_ptr<RecordingTopologyCoordinator>> Create(
        const EffectiveRecordingPolicy& policy, uint64_t initial_cursor = 0,
        uint64_t initial_now_ns = 0,
        RecordingTopologyObserver* observer = nullptr);

    RecordingTopologyCoordinator(const RecordingTopologyCoordinator&) = delete;
    RecordingTopologyCoordinator& operator=(
        const RecordingTopologyCoordinator&) = delete;
    RecordingTopologyCoordinator(RecordingTopologyCoordinator&&) = delete;
    RecordingTopologyCoordinator& operator=(
        RecordingTopologyCoordinator&&) = delete;

    // Lease loss enters kDegraded. A hard recorder error enters kFailed.
    // Neither state can recover from admission observations alone.
    Status RecorderLeaseLost(uint64_t now_ns);
    Status RecorderFailed(uint64_t now_ns);
    Status RecoverRecorder(uint64_t now_ns);
    Status UnbindRecorder(uint64_t now_ns);
    Status RebindRecorder(uint64_t next_cursor, uint64_t now_ns);

    // Applies one source observation and atomically advances the logical cursor
    // as directed by the returned decision. Configuration/enum errors and stale
    // cursors return an error without changing state or metrics.
    Result<RecordingAdmissionDecision> DecideAdmission(
        const RecordingAdmissionRequest& request, uint64_t now_ns);

    Result<RecordingTopologyMetrics> MetricsSnapshot(uint64_t now_ns) const;

    const EffectiveRecordingPolicy& policy() const noexcept { return policy_; }
    RecordingTopologyState state() const noexcept { return state_; }
    uint64_t next_cursor() const noexcept { return next_cursor_; }

private:
    RecordingTopologyCoordinator(EffectiveRecordingPolicy policy,
                                 uint64_t initial_cursor,
                                 uint64_t initial_now_ns,
                                 RecordingTopologyObserver* observer) noexcept;

    Status ValidateTime(uint64_t now_ns) const;
    void SetState(RecordingTopologyState state, uint64_t now_ns) noexcept;
    void CloseDegradedInterval(uint64_t now_ns) noexcept;
    void AddGap(RecordingAdmissionDecision* decision, uint64_t first_cursor,
                uint64_t end_cursor, RecordingGapReason reason);
    RecordingAdmissionDecision StrongDecision(
        const RecordingAdmissionRequest& request, uint64_t now_ns);
    RecordingAdmissionDecision IsolatedDecision(
        const RecordingAdmissionRequest& request, uint64_t now_ns);
    RecordingAdmissionDecision BestEffortDecision(
        const RecordingAdmissionRequest& request, uint64_t now_ns);
    RecordingAdmissionDecision UnboundDecision(
        const RecordingAdmissionRequest& request);
    RecordingAdmissionDecision StrongUnavailableDecision(
        RecordingAdmissionReason reason, uint64_t now_ns);
    void NotifyDecision(const RecordingAdmissionDecision& decision) noexcept;

    EffectiveRecordingPolicy policy_;
    RecordingTopologyState state_ = RecordingTopologyState::kActive;
    uint64_t next_cursor_ = 0;
    uint64_t last_event_ns_ = 0;
    uint64_t next_debt_id_ = 1;
    RecordingTopologyMetrics metrics_;
    std::optional<uint64_t> degraded_since_ns_;
    RecordingTopologyObserver* observer_ = nullptr;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_RECORDING_TOPOLOGY_H_
