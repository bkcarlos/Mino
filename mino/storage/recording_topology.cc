// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recording_topology.h"

#include <limits>
#include <memory>
#include <string_view>
#include <utility>

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool IsImpaired(RecordingTopologyState state) noexcept {
    return state == RecordingTopologyState::kDegraded ||
           state == RecordingTopologyState::kFailed;
}

void SaturatingAdd(uint64_t value, uint64_t* target) noexcept {
    const uint64_t maximum = std::numeric_limits<uint64_t>::max();
    *target = value > maximum - *target ? maximum : *target + value;
}

Status ValidateEffectivePolicy(const EffectiveRecordingPolicy& policy) {
    RecordingPolicy requested;
    requested.mode = policy.mode;
    requested.backpressure_topology = policy.backpressure_topology;
    requested.full_policy = policy.full_policy;
    requested.ack_level = policy.required_ack;
    requested.sync_policy = policy.sync_policy;
    requested.require_complete_recording = policy.require_complete_recording;
    requested.is_state_topic = policy.mode == RecordingMode::kSnapshot;

    Result<EffectiveRecordingPolicy> validated =
        ValidateRecordingPolicy(requested);
    if (!validated.ok()) return validated.status();
    if (validated->mode != policy.mode ||
        validated->backpressure_topology != policy.backpressure_topology ||
        validated->full_policy != policy.full_policy ||
        validated->required_ack != policy.required_ack ||
        validated->sync_policy != policy.sync_policy ||
        validated->require_complete_recording !=
            policy.require_complete_recording) {
        return Invalid("effective recording policy is not fully normalized");
    }
    return Status::Ok();
}

bool IsValidSinkCapacity(RecordingSinkCapacity capacity) noexcept {
    switch (capacity) {
        case RecordingSinkCapacity::kAvailable:
        case RecordingSinkCapacity::kFull:
            return true;
    }
    return false;
}

}  // namespace

Result<std::unique_ptr<RecordingTopologyCoordinator>>
RecordingTopologyCoordinator::Create(const EffectiveRecordingPolicy& policy,
                                     uint64_t initial_cursor,
                                     uint64_t initial_now_ns,
                                     RecordingTopologyObserver* observer) {
    const Status policy_status = ValidateEffectivePolicy(policy);
    if (!policy_status.ok()) return policy_status;
    if (initial_cursor == std::numeric_limits<uint64_t>::max()) {
        return Invalid("initial recording cursor cannot be UINT64_MAX");
    }
    return std::unique_ptr<RecordingTopologyCoordinator>(
        new RecordingTopologyCoordinator(policy, initial_cursor,
                                         initial_now_ns, observer));
}

RecordingTopologyCoordinator::RecordingTopologyCoordinator(
    EffectiveRecordingPolicy policy, uint64_t initial_cursor,
    uint64_t initial_now_ns, RecordingTopologyObserver* observer) noexcept
    : policy_(std::move(policy)),
      next_cursor_(initial_cursor),
      last_event_ns_(initial_now_ns),
      observer_(observer) {}

Status RecordingTopologyCoordinator::ValidateTime(uint64_t now_ns) const {
    if (now_ns < last_event_ns_) {
        return Invalid("recording topology time must be monotonic");
    }
    return Status::Ok();
}

void RecordingTopologyCoordinator::CloseDegradedInterval(
    uint64_t now_ns) noexcept {
    if (!degraded_since_ns_.has_value()) return;
    SaturatingAdd(now_ns - *degraded_since_ns_,
                  &metrics_.degraded_duration_ns);
    degraded_since_ns_.reset();
}

void RecordingTopologyCoordinator::SetState(RecordingTopologyState state,
                                            uint64_t now_ns) noexcept {
    if (state_ == state) return;
    const RecordingTopologyState previous = state_;
    const bool was_impaired = IsImpaired(previous);
    const bool is_impaired = IsImpaired(state);
    if (!was_impaired && is_impaired) {
        degraded_since_ns_ = now_ns;
    } else if (was_impaired && !is_impaired) {
        CloseDegradedInterval(now_ns);
    }
    state_ = state;
    if (observer_ != nullptr) {
        observer_->OnStateTransition(RecordingTopologyTransition{
            .from = previous,
            .to = state,
            .at_ns = now_ns,
        });
    }
}

Status RecordingTopologyCoordinator::RecorderLeaseLost(uint64_t now_ns) {
    const Status time_status = ValidateTime(now_ns);
    if (!time_status.ok()) return time_status;
    if (state_ == RecordingTopologyState::kUnbound) {
        return Invalid("an unbound recorder has no lease to lose");
    }
    if (state_ == RecordingTopologyState::kActive) {
        SetState(RecordingTopologyState::kDegraded, now_ns);
    }
    last_event_ns_ = now_ns;
    return Status::Ok();
}

Status RecordingTopologyCoordinator::RecorderFailed(uint64_t now_ns) {
    const Status time_status = ValidateTime(now_ns);
    if (!time_status.ok()) return time_status;
    if (state_ == RecordingTopologyState::kUnbound) {
        return Invalid("an unbound recorder cannot fail");
    }
    SetState(RecordingTopologyState::kFailed, now_ns);
    last_event_ns_ = now_ns;
    return Status::Ok();
}

Status RecordingTopologyCoordinator::RecoverRecorder(uint64_t now_ns) {
    const Status time_status = ValidateTime(now_ns);
    if (!time_status.ok()) return time_status;
    if (!IsImpaired(state_)) {
        return Invalid("recorder recovery requires a degraded or failed state");
    }
    SetState(RecordingTopologyState::kActive, now_ns);
    last_event_ns_ = now_ns;
    return Status::Ok();
}

Status RecordingTopologyCoordinator::UnbindRecorder(uint64_t now_ns) {
    const Status time_status = ValidateTime(now_ns);
    if (!time_status.ok()) return time_status;
    if (state_ == RecordingTopologyState::kUnbound) {
        return Invalid("recorder is already unbound");
    }
    SetState(RecordingTopologyState::kUnbound, now_ns);
    last_event_ns_ = now_ns;
    return Status::Ok();
}

Status RecordingTopologyCoordinator::RebindRecorder(uint64_t next_cursor,
                                                     uint64_t now_ns) {
    const Status time_status = ValidateTime(now_ns);
    if (!time_status.ok()) return time_status;
    if (state_ != RecordingTopologyState::kUnbound) {
        return Invalid("recorder rebind requires an unbound state");
    }
    if (next_cursor == std::numeric_limits<uint64_t>::max()) {
        return Invalid("recording cursor cannot be UINT64_MAX");
    }
    next_cursor_ = next_cursor;
    SetState(RecordingTopologyState::kActive, now_ns);
    last_event_ns_ = now_ns;
    return Status::Ok();
}

void RecordingTopologyCoordinator::AddGap(
    RecordingAdmissionDecision* decision, uint64_t first_cursor,
    uint64_t end_cursor, RecordingGapReason reason) {
    if (first_cursor == end_cursor) return;
    const RecordingGapDebt debt{
        .debt_id = next_debt_id_,
        .first_cursor = first_cursor,
        .end_cursor = end_cursor,
        .reason = reason,
    };
    if (next_debt_id_ != std::numeric_limits<uint64_t>::max()) {
        ++next_debt_id_;
    }
    decision->gap_debts.push_back(debt);
    SaturatingAdd(debt.record_count(), &metrics_.dropped);
    SaturatingAdd(1, &metrics_.gaps);
    if (observer_ != nullptr) observer_->OnGapDebt(debt);
}

RecordingAdmissionDecision
RecordingTopologyCoordinator::StrongUnavailableDecision(
    RecordingAdmissionReason reason, uint64_t now_ns) {
    RecordingAdmissionDecision decision{
        .reason = reason,
        .cursor_before = next_cursor_,
        .cursor_after = next_cursor_,
        .gap_debts = {},
    };
    const bool fail = state_ == RecordingTopologyState::kFailed ||
                      policy_.full_policy == BufferFullPolicy::kFailRecording;
    if (fail) {
        SetState(RecordingTopologyState::kFailed, now_ns);
        decision.outcome = RecordingAdmissionOutcome::kFailPrimary;
        decision.status = Status::Error(
            reason == RecordingAdmissionReason::kBufferFull
                ? StatusCode::kResourceExhausted
                : StatusCode::kUnavailable,
            "strong-consistent recorder cannot admit the primary publish");
        return decision;
    }

    decision.outcome = RecordingAdmissionOutcome::kBlockPrimary;
    decision.status = Status::Error(
        StatusCode::kWouldBlock,
        "strong-consistent recorder is not ready; primary publish blocked");
    SaturatingAdd(1, &metrics_.blocked);
    return decision;
}

RecordingAdmissionDecision RecordingTopologyCoordinator::StrongDecision(
    const RecordingAdmissionRequest& request, uint64_t now_ns) {
    if (state_ == RecordingTopologyState::kFailed) {
        return StrongUnavailableDecision(
            RecordingAdmissionReason::kRecorderFailed, now_ns);
    }
    if (state_ == RecordingTopologyState::kDegraded) {
        return StrongUnavailableDecision(
            RecordingAdmissionReason::kRecorderDegraded, now_ns);
    }
    if (request.available_cursor != next_cursor_) {
        SetState(RecordingTopologyState::kDegraded, now_ns);
        return StrongUnavailableDecision(RecordingAdmissionReason::kCursorLag,
                                         now_ns);
    }
    if (request.sink_capacity == RecordingSinkCapacity::kFull) {
        return StrongUnavailableDecision(RecordingAdmissionReason::kBufferFull,
                                         now_ns);
    }

    RecordingAdmissionDecision decision{
        .outcome = RecordingAdmissionOutcome::kRecord,
        .reason = RecordingAdmissionReason::kReady,
        .status = Status::Ok(),
        .primary_admitted = true,
        .recorder_admitted = true,
        .cursor_before = next_cursor_,
        .cursor_after = request.available_cursor + 1,
        .gap_debts = {},
    };
    next_cursor_ = decision.cursor_after;
    return decision;
}

RecordingAdmissionDecision RecordingTopologyCoordinator::IsolatedDecision(
    const RecordingAdmissionRequest& request, uint64_t now_ns) {
    RecordingAdmissionDecision decision{
        .outcome = RecordingAdmissionOutcome::kRecord,
        .reason = RecordingAdmissionReason::kReady,
        .status = Status::Ok(),
        .primary_admitted = true,
        .recorder_admitted = true,
        .cursor_before = next_cursor_,
        .cursor_after = request.available_cursor + 1,
        .gap_debts = {},
    };

    if (IsImpaired(state_)) {
        decision.outcome = RecordingAdmissionOutcome::kDropRecording;
        decision.reason = state_ == RecordingTopologyState::kFailed
                              ? RecordingAdmissionReason::kRecorderFailed
                              : RecordingAdmissionReason::kRecorderDegraded;
        decision.status = Status::Error(
            StatusCode::kDegraded,
            "isolated recorder unavailable; primary path admitted with Gap");
        decision.recorder_admitted = false;
        AddGap(&decision, next_cursor_, request.available_cursor + 1,
               RecordingGapReason::kIsolatedRecorderUnavailable);
    } else {
        if (request.available_cursor > next_cursor_) {
            decision.reason = RecordingAdmissionReason::kCursorLag;
            decision.status = Status::Error(
                StatusCode::kDegraded,
                "isolated fanout lagged; primary path admitted with Gap");
            AddGap(&decision, next_cursor_, request.available_cursor,
                   RecordingGapReason::kIsolatedFanoutLag);
        }
        if (request.sink_capacity == RecordingSinkCapacity::kFull) {
            decision.outcome = RecordingAdmissionOutcome::kDropRecording;
            decision.reason = RecordingAdmissionReason::kBufferFull;
            decision.status = Status::Error(
                StatusCode::kDegraded,
                "isolated fanout full; primary path admitted with Gap");
            decision.recorder_admitted = false;
            AddGap(&decision, request.available_cursor,
                   request.available_cursor + 1,
                   RecordingGapReason::kIsolatedFanoutFull);
            if (policy_.full_policy == BufferFullPolicy::kFailRecording) {
                SetState(RecordingTopologyState::kFailed, now_ns);
            }
        }
    }

    next_cursor_ = decision.cursor_after;
    return decision;
}

RecordingAdmissionDecision RecordingTopologyCoordinator::BestEffortDecision(
    const RecordingAdmissionRequest& request, uint64_t now_ns) {
    RecordingAdmissionDecision decision{
        .outcome = RecordingAdmissionOutcome::kRecord,
        .reason = RecordingAdmissionReason::kReady,
        .status = Status::Ok(),
        .primary_admitted = true,
        .recorder_admitted = true,
        .cursor_before = next_cursor_,
        .cursor_after = request.available_cursor + 1,
        .gap_debts = {},
    };

    if (IsImpaired(state_)) {
        decision.outcome = RecordingAdmissionOutcome::kDropRecording;
        decision.reason = state_ == RecordingTopologyState::kFailed
                              ? RecordingAdmissionReason::kRecorderFailed
                              : RecordingAdmissionReason::kRecorderDegraded;
        decision.status = Status::Error(
            StatusCode::kDegraded,
            "best-effort recorder unavailable; cursor advanced with Gap");
        decision.recorder_admitted = false;
        AddGap(&decision, next_cursor_, request.available_cursor + 1,
               RecordingGapReason::kBestEffortRecorderUnavailable);
    } else {
        if (request.available_cursor > next_cursor_) {
            decision.reason = RecordingAdmissionReason::kCursorLag;
            decision.status = Status::Error(
                StatusCode::kDegraded,
                "best-effort recorder lagged; cursor advanced with Gap");
            AddGap(&decision, next_cursor_, request.available_cursor,
                   RecordingGapReason::kBestEffortLag);
        }
        if (request.sink_capacity == RecordingSinkCapacity::kFull) {
            decision.outcome = RecordingAdmissionOutcome::kDropRecording;
            decision.reason = RecordingAdmissionReason::kBufferFull;
            decision.status = Status::Error(
                StatusCode::kDegraded,
                "best-effort recorder full; cursor advanced with Gap");
            decision.recorder_admitted = false;
            AddGap(&decision, request.available_cursor,
                   request.available_cursor + 1,
                   RecordingGapReason::kBestEffortBufferFull);
            if (policy_.full_policy == BufferFullPolicy::kFailRecording) {
                SetState(RecordingTopologyState::kFailed, now_ns);
            }
        }
    }

    next_cursor_ = decision.cursor_after;
    return decision;
}

RecordingAdmissionDecision RecordingTopologyCoordinator::UnboundDecision(
    const RecordingAdmissionRequest& request) {
    RecordingAdmissionDecision decision{
        .outcome = RecordingAdmissionOutcome::kBypassRecording,
        .reason = RecordingAdmissionReason::kExplicitlyUnbound,
        .status = Status::Ok(),
        .primary_admitted = true,
        .recorder_admitted = false,
        .cursor_before = next_cursor_,
        .cursor_after = request.available_cursor + 1,
        .gap_debts = {},
    };
    next_cursor_ = decision.cursor_after;
    return decision;
}

void RecordingTopologyCoordinator::NotifyDecision(
    const RecordingAdmissionDecision& decision) noexcept {
    if (observer_ != nullptr) observer_->OnAdmissionDecision(decision);
}

Result<RecordingAdmissionDecision>
RecordingTopologyCoordinator::DecideAdmission(
    const RecordingAdmissionRequest& request, uint64_t now_ns) {
    const Status time_status = ValidateTime(now_ns);
    if (!time_status.ok()) return time_status;
    if (!IsValidSinkCapacity(request.sink_capacity)) {
        return Invalid("invalid recording sink capacity");
    }
    if (request.available_cursor == std::numeric_limits<uint64_t>::max()) {
        return Invalid("available recording cursor cannot be UINT64_MAX");
    }
    if (request.available_cursor < next_cursor_) {
        return Invalid("available recording cursor is stale");
    }

    RecordingAdmissionDecision decision;
    if (state_ == RecordingTopologyState::kUnbound) {
        decision = UnboundDecision(request);
    } else {
        switch (policy_.backpressure_topology) {
            case RecordBackpressureTopology::kStrongConsistent:
                decision = StrongDecision(request, now_ns);
                break;
            case RecordBackpressureTopology::kIsolated:
                decision = IsolatedDecision(request, now_ns);
                break;
            case RecordBackpressureTopology::kBestEffort:
                decision = BestEffortDecision(request, now_ns);
                break;
        }
    }
    last_event_ns_ = now_ns;
    NotifyDecision(decision);
    return decision;
}

Result<RecordingTopologyMetrics>
RecordingTopologyCoordinator::MetricsSnapshot(uint64_t now_ns) const {
    const Status time_status = ValidateTime(now_ns);
    if (!time_status.ok()) return time_status;
    RecordingTopologyMetrics snapshot = metrics_;
    if (degraded_since_ns_.has_value()) {
        SaturatingAdd(now_ns - *degraded_since_ns_,
                      &snapshot.degraded_duration_ns);
    }
    return snapshot;
}

}  // namespace mino::storage
