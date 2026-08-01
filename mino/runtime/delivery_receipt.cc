// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/delivery_receipt.h"

#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <new>
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
    bool admitted = false;
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

struct PublisherReceiptCounts {
    uint32_t outstanding = 0;
    uint32_t reserved = 0;
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
    // entries contains both reservations and committed outstanding receipts.
    // admitted distinguishes the latter; reservations preallocate the map node
    // so Commit cannot fail after local publication.
    std::unordered_map<uint64_t, std::shared_ptr<DeliveryReceipt::State>> entries;
    std::unordered_map<PublisherReceiptIdentity, PublisherReceiptCounts,
                       PublisherIdentityHash>
        per_publisher;
    uint32_t reserved = 0;
    uint64_t next_id = 1;
    bool id_space_exhausted = false;
    std::atomic<uint32_t> reserve_failure_point{0};
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

OutstandingReceiptTable::Reservation::Reservation(
    Reservation&& other) noexcept
    : table_(other.table_), id_(other.id_) {
    other.table_ = nullptr;
    other.id_ = {};
}

OutstandingReceiptTable::Reservation&
OutstandingReceiptTable::Reservation::operator=(Reservation&& other) noexcept {
    if (this != &other) {
        Cancel();
        table_ = other.table_;
        id_ = other.id_;
        other.table_ = nullptr;
        other.id_ = {};
    }
    return *this;
}

OutstandingReceiptTable::Reservation::~Reservation() { Cancel(); }

DeliveryReceipt OutstandingReceiptTable::Reservation::Commit(
    uint64_t source_sequence) && noexcept {
    assert(table_ != nullptr);
    OutstandingReceiptTable* table = table_;
    const ReceiptId id = id_;
    table_ = nullptr;
    id_ = {};
    return table->CommitReservation(id, source_sequence);
}

void OutstandingReceiptTable::Reservation::Cancel() noexcept {
    if (table_ == nullptr) {
        return;
    }
    table_->CancelReservation(id_);
    table_ = nullptr;
    id_ = {};
}

OutstandingReceiptTable::OutstandingReceiptTable()
    : OutstandingReceiptTable(Limits{}) {}

OutstandingReceiptTable::OutstandingReceiptTable(Limits limits)
    : impl_(std::make_unique<Impl>(limits)) {}

OutstandingReceiptTable::~OutstandingReceiptTable() = default;

void OutstandingReceiptTable::SetReserveFailurePointForTesting(
    ReserveFailurePointForTesting point) noexcept {
    impl_->reserve_failure_point.store(static_cast<uint32_t>(point),
                                       std::memory_order_relaxed);
}

void OutstandingReceiptTable::SetNextReceiptIdForTesting(uint64_t next_id) {
    std::lock_guard table_lock(impl_->mutex);
    impl_->next_id = next_id;
    impl_->id_space_exhausted = next_id == 0;
}

Result<OutstandingReceiptTable::Reservation> OutstandingReceiptTable::Reserve(
    const PublisherReceiptIdentity& publisher,
    std::span<const DeliveryTarget> targets,
    const DeliveryRequirement& requirement) {
    const auto maybe_fail_allocation =
        [this](ReserveFailurePointForTesting point) {
            uint32_t expected = static_cast<uint32_t>(point);
            if (impl_->reserve_failure_point.compare_exchange_strong(
                    expected,
                    static_cast<uint32_t>(
                        ReserveFailurePointForTesting::kNone),
                    std::memory_order_relaxed)) {
                throw std::bad_alloc();
            }
        };

    try {
        if (publisher.process.IsZero() || publisher.publisher_id.value == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "receipt publisher identity is invalid");
        }
        if (targets.empty() &&
            requirement.stage != DeliveryStage::kLocalPublished) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "non-local receipt requires at least one target");
        }
        if (requirement.completion == CompletionPolicy::kQuorum &&
            (requirement.quorum == 0 || requirement.quorum > targets.size())) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "receipt quorum is outside the target snapshot");
        }

        maybe_fail_allocation(ReserveFailurePointForTesting::kState);
        auto state = std::make_shared<DeliveryReceipt::State>();
        state->publisher = publisher;
        state->requirement = requirement;
        maybe_fail_allocation(ReserveFailurePointForTesting::kTargets);
        state->targets.reserve(targets.size());
        maybe_fail_allocation(ReserveFailurePointForTesting::kUpdated);
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

        std::lock_guard table_lock(impl_->mutex);
        if (impl_->entries.size() >= impl_->limits.max_outstanding) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "outstanding receipt table is full");
        }

        auto publisher_counts = impl_->per_publisher.find(publisher);
        if (publisher_counts != impl_->per_publisher.end() &&
            publisher_counts->second.outstanding +
                    publisher_counts->second.reserved >=
                impl_->limits.max_per_publisher) {
            return Status::Error(
                StatusCode::kResourceExhausted,
                "publisher outstanding receipt quota exhausted");
        }
        if (publisher_counts == impl_->per_publisher.end() &&
            impl_->limits.max_per_publisher == 0) {
            return Status::Error(
                StatusCode::kResourceExhausted,
                "publisher outstanding receipt quota exhausted");
        }
        if (impl_->id_space_exhausted || impl_->next_id == 0) {
            impl_->id_space_exhausted = true;
            return Status::Error(StatusCode::kResourceExhausted,
                                 "receipt id space exhausted");
        }

        bool inserted_publisher = false;
        try {
            if (publisher_counts == impl_->per_publisher.end()) {
                maybe_fail_allocation(
                    ReserveFailurePointForTesting::kPublisherEntry);
                auto inserted = impl_->per_publisher.try_emplace(publisher);
                publisher_counts = inserted.first;
                inserted_publisher = inserted.second;
            }

            while (true) {
                const uint64_t raw_id = impl_->next_id;
                state->id = ReceiptId{raw_id};
                maybe_fail_allocation(
                    ReserveFailurePointForTesting::kReceiptEntry);
                const auto inserted = impl_->entries.emplace(raw_id, state);
                if (inserted.second) {
                    if (raw_id == std::numeric_limits<uint64_t>::max()) {
                        impl_->id_space_exhausted = true;
                    } else {
                        impl_->next_id = raw_id + 1;
                    }
                    ++publisher_counts->second.reserved;
                    ++impl_->reserved;
                    return Reservation(this, state->id);
                }

                if (raw_id == std::numeric_limits<uint64_t>::max()) {
                    impl_->id_space_exhausted = true;
                    if (inserted_publisher) {
                        impl_->per_publisher.erase(publisher_counts);
                        inserted_publisher = false;
                    }
                    return Status::Error(StatusCode::kResourceExhausted,
                                         "receipt id space exhausted");
                }
                impl_->next_id = raw_id + 1;
            }
        } catch (const std::bad_alloc&) {
            if (inserted_publisher) {
                impl_->per_publisher.erase(publisher_counts);
            }
            return Status::Error(StatusCode::kResourceExhausted);
        }
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

DeliveryReceipt OutstandingReceiptTable::CommitReservation(
    ReceiptId id, uint64_t source_sequence) noexcept {
    std::shared_ptr<DeliveryReceipt::State> state;
    {
        std::lock_guard table_lock(impl_->mutex);
        const auto found = impl_->entries.find(id.value);
        assert(found != impl_->entries.end());
        assert(!found->second->admitted);
        state = found->second;
        state->source_sequence = source_sequence;
        state->admitted = true;

        auto publisher = impl_->per_publisher.find(state->publisher);
        assert(publisher != impl_->per_publisher.end());
        assert(publisher->second.reserved != 0);
        --publisher->second.reserved;
        assert(impl_->reserved != 0);
        --impl_->reserved;

        if (state->completed) {
            impl_->entries.erase(found);
            if (publisher->second.outstanding == 0 &&
                publisher->second.reserved == 0) {
                impl_->per_publisher.erase(publisher);
            }
        } else {
            ++publisher->second.outstanding;
        }
    }
    return DeliveryReceipt(std::move(state));
}

void OutstandingReceiptTable::CancelReservation(ReceiptId id) noexcept {
    std::lock_guard table_lock(impl_->mutex);
    const auto found = impl_->entries.find(id.value);
    if (found == impl_->entries.end() || found->second->admitted) {
        return;
    }
    auto publisher = impl_->per_publisher.find(found->second->publisher);
    assert(publisher != impl_->per_publisher.end());
    assert(publisher->second.reserved != 0);
    --publisher->second.reserved;
    assert(impl_->reserved != 0);
    --impl_->reserved;
    impl_->entries.erase(found);
    if (publisher->second.outstanding == 0 &&
        publisher->second.reserved == 0) {
        impl_->per_publisher.erase(publisher);
    }
}

Result<DeliveryReceipt> OutstandingReceiptTable::Create(
    const PublisherReceiptIdentity& publisher, uint64_t source_sequence,
    std::span<const DeliveryTarget> targets,
    const DeliveryRequirement& requirement) {
    Result<Reservation> reservation = Reserve(publisher, targets, requirement);
    if (!reservation.ok()) {
        return reservation.status();
    }
    return std::move(*reservation).Commit(source_sequence);
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
        if (found == impl_->entries.end() || !found->second->admitted) {
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
            assert(count != impl_->per_publisher.end());
            assert(count->second.outstanding != 0);
            --count->second.outstanding;
            if (count->second.outstanding == 0 &&
                count->second.reserved == 0) {
                impl_->per_publisher.erase(count);
            }
        }
    }
    state->condition.notify_all();
    return Status::Ok();
}

uint32_t OutstandingReceiptTable::CleanupPublisher(
    const PublisherReceiptIdentity& publisher) noexcept {
    uint32_t removed = 0;
    std::lock_guard table_lock(impl_->mutex);
    for (auto it = impl_->entries.begin(); it != impl_->entries.end();) {
        if (!it->second->admitted || it->second->publisher != publisher) {
            ++it;
            continue;
        }

        const std::shared_ptr<DeliveryReceipt::State> state = it->second;
        {
            std::lock_guard state_lock(state->mutex);
            state->orphaned = true;
        }
        it = impl_->entries.erase(it);
        ++removed;
        state->condition.notify_all();
    }

    auto count = impl_->per_publisher.find(publisher);
    if (count != impl_->per_publisher.end()) {
        assert(count->second.outstanding == removed);
        count->second.outstanding = 0;
        if (count->second.reserved == 0) {
            impl_->per_publisher.erase(count);
        }
    }
    return removed;
}

uint32_t OutstandingReceiptTable::outstanding() const noexcept {
    std::lock_guard lock(impl_->mutex);
    assert(impl_->entries.size() >= impl_->reserved);
    return static_cast<uint32_t>(impl_->entries.size() - impl_->reserved);
}

uint32_t OutstandingReceiptTable::outstanding_for(
    const PublisherReceiptIdentity& publisher) const noexcept {
    std::lock_guard lock(impl_->mutex);
    const auto found = impl_->per_publisher.find(publisher);
    return found == impl_->per_publisher.end() ? 0u
                                               : found->second.outstanding;
}

}  // namespace mino
