// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/delivery_receipt.h"

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace mino {

struct DeliveryReceipt::State {
    ReceiptId id;
    PublisherReceiptIdentity publisher;
    uint64_t source_sequence = 0;
    DeliveryRequirement requirement;
    mutable std::mutex mutex;
    mutable std::condition_variable condition;
    std::vector<TargetDeliveryStatus> targets;
    std::vector<bool> updated;
    bool completed = false;
    bool canceled = false;
    bool orphaned = false;
};

namespace {

struct PublisherIdentityHash {
    size_t operator()(const PublisherReceiptIdentity& value) const noexcept {
        size_t hash = std::hash<uint64_t>{}(value.publisher_id.value);
        auto combine = [&hash](uint64_t field) {
            hash ^= std::hash<uint64_t>{}(field) + 0x9e3779b97f4a7c15ULL +
                    (hash << 6) + (hash >> 2);
        };
        combine(value.process.node_id);
        combine(value.process.process_id);
        combine(value.process.process_epoch);
        combine(value.process.start_time_ns);
        return hash;
    }
};

uint32_t RequiredSuccesses(const DeliveryRequirement& requirement,
                           uint32_t target_count) noexcept {
    switch (requirement.completion) {
        case CompletionPolicy::kAll:
            return target_count;
        case CompletionPolicy::kAny:
            return target_count == 0 ? 0 : 1;
        case CompletionPolicy::kQuorum:
            return requirement.quorum;
    }
    return target_count;
}

bool EvaluateCompletion(DeliveryReceipt::State& state) {
    const uint32_t required = RequiredSuccesses(
        state.requirement, static_cast<uint32_t>(state.targets.size()));
    uint32_t successes = 0;
    uint32_t pending = 0;
    for (size_t i = 0; i < state.targets.size(); ++i) {
        if (!state.updated[i]) {
            ++pending;
            continue;
        }
        const TargetDeliveryStatus& target = state.targets[i];
        if (target.status.ok() &&
            DeliveryStageSatisfies(target.target.kind, target.reached_stage,
                                   state.requirement.stage)) {
            ++successes;
        }
    }
    return successes >= required || successes + pending < required;
}

}  // namespace

struct OutstandingReceiptTable::Impl {
    explicit Impl(Limits configured_limits) : limits(configured_limits) {}

    Limits limits;
    mutable std::mutex mutex;
    std::unordered_map<uint64_t, std::shared_ptr<DeliveryReceipt::State>> entries;
    std::unordered_map<PublisherReceiptIdentity, uint32_t, PublisherIdentityHash>
        per_publisher;
    std::atomic<uint64_t> next_id{1};
};

bool DeliveryStageSatisfies(DeliveryTargetKind target_kind,
                            DeliveryStage reached,
                            DeliveryStage required) noexcept {
    if (required == DeliveryStage::kLocalPublished) {
        return true;
    }
    if (target_kind == DeliveryTargetKind::kNode) {
        return required == DeliveryStage::kRemoteAccepted &&
               reached == DeliveryStage::kRemoteAccepted;
    }
    if (required == DeliveryStage::kRemoteAccepted ||
        reached == DeliveryStage::kRemoteAccepted) {
        return false;
    }
    switch (required) {
        case DeliveryStage::kRecorderBuffered:
            return reached == DeliveryStage::kRecorderBuffered ||
                   reached == DeliveryStage::kStorageWritten ||
                   reached == DeliveryStage::kStorageDurable;
        case DeliveryStage::kStorageWritten:
            return reached == DeliveryStage::kStorageWritten ||
                   reached == DeliveryStage::kStorageDurable;
        case DeliveryStage::kStorageDurable:
            return reached == DeliveryStage::kStorageDurable;
        case DeliveryStage::kLocalPublished:
            return true;
        case DeliveryStage::kRemoteAccepted:
            return false;
    }
    return false;
}

ReceiptId DeliveryReceipt::id() const noexcept {
    return state_ == nullptr ? ReceiptId{} : state_->id;
}

Result<std::vector<TargetDeliveryStatus>> DeliveryReceipt::Wait(
    Deadline deadline) const {
    if (state_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "delivery receipt is not valid");
    }
    const Deadline::Clock::time_point wait_until = std::min(
        deadline.time_point(), state_->requirement.deadline.time_point());
    std::unique_lock lock(state_->mutex);
    while (!state_->completed && !state_->canceled && !state_->orphaned) {
        if (wait_until == Deadline::Clock::time_point::max()) {
            state_->condition.wait(lock);
        } else if (state_->condition.wait_until(lock, wait_until) ==
                   std::cv_status::timeout) {
            if (!state_->completed && !state_->canceled && !state_->orphaned) {
                return Status::Error(StatusCode::kTimeout,
                                     "delivery receipt wait timed out");
            }
        }
    }
    if (state_->canceled) {
        return Status::Error(StatusCode::kUnavailable,
                             "delivery receipt wait was canceled");
    }
    if (state_->orphaned) {
        return Status::Error(StatusCode::kUnavailable,
                             "delivery receipt publisher was cleaned up");
    }
    return state_->targets;
}

void DeliveryReceipt::CancelWait() noexcept {
    if (state_ == nullptr) {
        return;
    }
    {
        std::lock_guard lock(state_->mutex);
        state_->canceled = true;
    }
    state_->condition.notify_all();
}

OutstandingReceiptTable::OutstandingReceiptTable()
    : OutstandingReceiptTable(Limits{}) {}

OutstandingReceiptTable::OutstandingReceiptTable(Limits limits)
    : impl_(std::make_unique<Impl>(limits)) {}

OutstandingReceiptTable::~OutstandingReceiptTable() = default;

Result<DeliveryReceipt> OutstandingReceiptTable::Create(
    const PublisherReceiptIdentity& publisher, uint64_t source_sequence,
    std::span<const DeliveryTarget> targets,
    const DeliveryRequirement& requirement) {
    if (publisher.process.IsZero() || publisher.publisher_id.value == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "receipt publisher identity is invalid");
    }
    if (targets.empty() && requirement.stage != DeliveryStage::kLocalPublished) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "non-local receipt requires at least one target");
    }
    if (requirement.completion == CompletionPolicy::kQuorum &&
        (requirement.quorum == 0 || requirement.quorum > targets.size())) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "receipt quorum is outside the target snapshot");
    }

    std::lock_guard table_lock(impl_->mutex);
    if (impl_->entries.size() >= impl_->limits.max_outstanding) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "outstanding receipt table is full");
    }
    uint32_t& publisher_count = impl_->per_publisher[publisher];
    if (publisher_count >= impl_->limits.max_per_publisher) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "publisher outstanding receipt quota exhausted");
    }

    const uint64_t raw_id = impl_->next_id.fetch_add(1, std::memory_order_relaxed);
    if (raw_id == 0) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "receipt id space exhausted");
    }
    auto state = std::make_shared<DeliveryReceipt::State>();
    state->id = ReceiptId{raw_id};
    state->publisher = publisher;
    state->source_sequence = source_sequence;
    state->requirement = requirement;
    state->targets.reserve(targets.size());
    state->updated.reserve(targets.size());
    for (DeliveryTarget target : targets) {
        TargetDeliveryStatus status;
        status.target = target;
        if (requirement.stage == DeliveryStage::kLocalPublished) {
            status.reached_stage = DeliveryStage::kLocalPublished;
            status.status = Status::Ok();
            state->updated.push_back(true);
        } else {
            state->updated.push_back(false);
        }
        state->targets.push_back(std::move(status));
    }
    state->completed = EvaluateCompletion(*state);

    if (!state->completed) {
        impl_->entries.emplace(raw_id, state);
        ++publisher_count;
    } else if (publisher_count == 0) {
        impl_->per_publisher.erase(publisher);
    }
    return DeliveryReceipt(std::move(state));
}

Status OutstandingReceiptTable::Acknowledge(ReceiptId id,
                                            DeliveryTarget target,
                                            DeliveryStage reached_stage,
                                            Status status) {
    std::shared_ptr<DeliveryReceipt::State> state;
    bool completed = false;
    {
        std::lock_guard table_lock(impl_->mutex);
        const auto found = impl_->entries.find(id.value);
        if (found == impl_->entries.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "outstanding receipt was not found");
        }
        state = found->second;
        {
            std::lock_guard state_lock(state->mutex);
            auto target_it = std::find_if(
                state->targets.begin(), state->targets.end(),
                [target](const TargetDeliveryStatus& entry) {
                    return entry.target == target;
                });
            if (target_it == state->targets.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "delivery target is not in the snapshot");
            }
            if (status.ok() &&
                !DeliveryStageSatisfies(target.kind, reached_stage,
                                        state->requirement.stage)) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "ack stage is incompatible with target branch");
            }
            const size_t index = static_cast<size_t>(
                std::distance(state->targets.begin(), target_it));
            target_it->reached_stage = reached_stage;
            target_it->status = std::move(status);
            state->updated[index] = true;
            state->completed = EvaluateCompletion(*state);
            completed = state->completed;
        }
        if (completed) {
            impl_->entries.erase(found);
            auto count = impl_->per_publisher.find(state->publisher);
            if (count != impl_->per_publisher.end() && --count->second == 0) {
                impl_->per_publisher.erase(count);
            }
        }
    }
    state->condition.notify_all();
    return Status::Ok();
}

uint32_t OutstandingReceiptTable::CleanupPublisher(
    const PublisherReceiptIdentity& publisher) noexcept {
    std::vector<std::shared_ptr<DeliveryReceipt::State>> removed;
    {
        std::lock_guard table_lock(impl_->mutex);
        for (auto it = impl_->entries.begin(); it != impl_->entries.end();) {
            if (it->second->publisher == publisher) {
                removed.push_back(it->second);
                it = impl_->entries.erase(it);
            } else {
                ++it;
            }
        }
        impl_->per_publisher.erase(publisher);
    }
    for (const auto& state : removed) {
        {
            std::lock_guard state_lock(state->mutex);
            state->orphaned = true;
        }
        state->condition.notify_all();
    }
    return static_cast<uint32_t>(removed.size());
}

uint32_t OutstandingReceiptTable::outstanding() const noexcept {
    std::lock_guard lock(impl_->mutex);
    return static_cast<uint32_t>(impl_->entries.size());
}

uint32_t OutstandingReceiptTable::outstanding_for(
    const PublisherReceiptIdentity& publisher) const noexcept {
    std::lock_guard lock(impl_->mutex);
    const auto found = impl_->per_publisher.find(publisher);
    return found == impl_->per_publisher.end() ? 0u : found->second;
}

}  // namespace mino
