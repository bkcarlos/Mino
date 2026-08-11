// Copyright 2026 The Mino Authors

#include "mino/upgrade/orchestrator.h"

#include <string_view>

#include "mino/common/status.h"

namespace mino::upgrade {
namespace {

Status Blocked(std::string_view message) {
    return Status::Error(StatusCode::kWouldBlock, message);
}

}  // namespace

UpgradeStepPreview UpgradeOrchestrator::Preview() const {
    UpgradeStepPreview preview;
    if (store_ == nullptr) {
        preview.terminal = true;
        preview.action = "invalid orchestrator: manifest store is absent";
        return preview;
    }
    preview.current = store_->snapshot().phase;
    switch (preview.current) {
        case UpgradePhase::kPrepare:
            preview.next = UpgradePhase::kValidate;
            preview.action = "prepare distinct target Region/channels/processes";
            break;
        case UpgradePhase::kValidate:
            preview.next = UpgradePhase::kDrain;
            preview.action = "validate readiness/domain/ACL/schema/capacity, then fence old publishers";
            break;
        case UpgradePhase::kDrain:
            preview.next = UpgradePhase::kCutover;
            preview.action = "verify participants/pins/receipts/borrows/queue drain proof";
            break;
        case UpgradePhase::kCutover:
            preview.next = UpgradePhase::kObserve;
            preview.action = "atomically route publishers to target using commit token";
            break;
        case UpgradePhase::kObserve:
            preview.next = UpgradePhase::kCommit;
            preview.action = "observe target-only publish and zero duplicate/unexplained loss";
            break;
        case UpgradePhase::kCommit:
        case UpgradePhase::kRollback:
        case UpgradePhase::kFail:
            preview.next = preview.current;
            preview.terminal = true;
            preview.action = "terminal";
            break;
    }
    return preview;
}

Status UpgradeOrchestrator::Step(uint64_t now_ns) {
    if (store_ == nullptr || control_ == nullptr || now_ns == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade orchestrator dependencies or time are invalid");
    }
    if (store_->poisoned()) {
        return Status::Error(StatusCode::kUnavailable,
                             "upgrade manifest outcome is uncertain; reopen and resume");
    }
    const UpgradePlan& plan = store_->snapshot().plan;
    switch (store_->snapshot().phase) {
        case UpgradePhase::kPrepare:
            MINO_RETURN_IF_ERROR(ValidateUpgradePlan(plan));
            MINO_RETURN_IF_ERROR(control_->Prepare(plan));
            return store_->Advance(UpgradePhase::kValidate, now_ns,
                                   "target preparation acknowledged");
        case UpgradePhase::kValidate: {
            MINO_ASSIGN_OR_RETURN(const TargetReadinessProof proof,
                                  control_->ObserveTarget(plan));
            MINO_RETURN_IF_ERROR(ValidateTargetReadiness(plan, proof));
            // This call must fence old publisher creation and request existing
            // publishers to stop. It is retried with the same commit token.
            MINO_RETURN_IF_ERROR(control_->BeginDrain(plan));
            return store_->Advance(UpgradePhase::kDrain, now_ns,
                                   "target validated and old publisher drain requested");
        }
        case UpgradePhase::kDrain: {
            MINO_ASSIGN_OR_RETURN(const DrainProof proof,
                                  control_->ObserveDrain(plan));
            if (!proof.complete()) {
                return Blocked(
                    "drain proof incomplete: publisher/subscriber/pin/receipt/borrow/queue conservation is required");
            }
            // Persist cutover intent before the route switch. A crash here is
            // recovered by probing/reissuing token-idempotent Cutover().
            return store_->Advance(UpgradePhase::kCutover, now_ns,
                                   "complete drain proof durably accepted");
        }
        case UpgradePhase::kCutover:
            MINO_RETURN_IF_ERROR(control_->Cutover(plan));
            return store_->Advance(UpgradePhase::kObserve, now_ns,
                                   "cutover token acknowledged; target is publisher-active");
        case UpgradePhase::kObserve:
            return ObserveAndCommit(now_ns);
        case UpgradePhase::kCommit:
        case UpgradePhase::kRollback:
        case UpgradePhase::kFail:
            return Status::Ok();
    }
    return Status::Error(StatusCode::kCorruption,
                         "upgrade manifest contains an unknown phase");
}

Status UpgradeOrchestrator::ObserveAndCommit(uint64_t now_ns) {
    const UpgradePlan& plan = store_->snapshot().plan;
    MINO_ASSIGN_OR_RETURN(const CutoverObservation observation,
                          control_->ObserveCutover(plan));
    if (observation.acknowledged_commit_token != plan.commit_token ||
        !(observation.active_region == plan.target_region)) {
        return Blocked("cutover token or active target Region is not yet acknowledged");
    }
    if (observation.old_publisher_count != 0 ||
        observation.new_publisher_count == 0) {
        return Blocked("publishers are not exclusively active in the target Region");
    }
    if (observation.duplicate_count != 0 ||
        observation.unexplained_loss_count != 0) {
        return Status::Error(StatusCode::kDegraded,
                             "cutover observation found duplicate or unexplained loss");
    }
    if (observation.observed_samples < plan.minimum_observation_samples) {
        return Blocked("cutover observation sample bound is not yet met");
    }
    MINO_RETURN_IF_ERROR(control_->Commit(plan));
    return store_->Advance(UpgradePhase::kCommit, now_ns,
                           "target observation passed; old Region may be retired");
}

Status UpgradeOrchestrator::Execute(uint64_t now_ns, size_t max_steps) {
    if (max_steps == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade execution step bound must be nonzero");
    }
    for (size_t step = 0; step < max_steps; ++step) {
        if (store_ != nullptr && IsTerminalPhase(store_->snapshot().phase)) {
            return Status::Ok();
        }
        const Status status = Step(now_ns);
        if (!status.ok()) return status;
    }
    return IsTerminalPhase(store_->snapshot().phase)
               ? Status::Ok()
               : Status::Error(StatusCode::kWouldBlock,
                               "upgrade execution reached the bounded step limit");
}

Status UpgradeOrchestrator::Rollback(uint64_t now_ns) {
    if (store_ == nullptr || control_ == nullptr || now_ns == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade rollback dependencies or time are invalid");
    }
    const UpgradePhase phase = store_->snapshot().phase;
    if (IsTerminalPhase(phase)) {
        return Status::Error(StatusCode::kUnsupported,
                             "terminal upgrade cannot be rolled back");
    }
    const bool after_cutover_intent = !IsPreCutoverPhase(phase);
    if (after_cutover_intent) {
        MINO_ASSIGN_OR_RETURN(
            const SafeRollbackProof proof,
            control_->ObserveRollbackSafety(store_->snapshot().plan));
        if (!proof.safe()) {
            return Status::Error(
                StatusCode::kPermissionDenied,
                "post-cutover rollback lacks target fence/source readiness/reconciliation proof; use forward fix");
        }
    }
    MINO_RETURN_IF_ERROR(
        control_->Rollback(store_->snapshot().plan, after_cutover_intent));
    return store_->Advance(
        UpgradePhase::kRollback, now_ns,
        after_cutover_intent ? "explicit safe post-cutover rollback completed"
                             : "pre-cutover rollback completed");
}

Status UpgradeOrchestrator::Fail(uint64_t now_ns, std::string reason) {
    if (store_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade manifest store is absent");
    }
    return store_->Fail(now_ns, reason);
}

}  // namespace mino::upgrade
