// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/subscriber_lease.h"

#include <new>

namespace mino {

Result<SubscriberLeaseTable> SubscriberLeaseTable::Init(void* shm_base) {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "lease table base must not be null");
    }
    if (reinterpret_cast<uintptr_t>(shm_base) % alignof(ControlBlock) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "lease table base must be cache-line aligned");
    }

    auto* control = new (shm_base) ControlBlock{};
    control->layout_version.store(kLayoutVersion, std::memory_order_relaxed);
    control->max_subscribers = kMaxSubscribers;

    SubscriberLeaseRecord* records = RecordsOf(shm_base);
    for (uint32_t id = 0; id < kMaxSubscribers; ++id) {
        new (&records[id]) SubscriberLeaseRecord{};
        records[id].subscriber_id = id;
        records[id].state.store(
            static_cast<uint32_t>(SubscriberLeaseState::kFree),
            std::memory_order_relaxed);
    }
    control->magic.store(kMagic, std::memory_order_release);
    return SubscriberLeaseTable(records);
}

Result<SubscriberLeaseTable> SubscriberLeaseTable::Attach(void* shm_base) {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "lease table base must not be null");
    }
    if (reinterpret_cast<uintptr_t>(shm_base) % alignof(ControlBlock) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "lease table base must be cache-line aligned");
    }
    auto* control = static_cast<ControlBlock*>(shm_base);
    if (control->magic.load(std::memory_order_acquire) != kMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "subscriber lease table magic mismatch");
    }
    if (control->layout_version.load(std::memory_order_acquire) !=
            kLayoutVersion ||
        control->max_subscribers != kMaxSubscribers) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "subscriber lease table layout mismatch");
    }
    return SubscriberLeaseTable(RecordsOf(shm_base));
}

bool SubscriberLeaseTable::Matches(
    const SubscriberLeaseRecord& record,
    SubscriberLeaseHandle handle) noexcept {
    return record.subscriber_id == handle.subscriber.id.value &&
           record.subscriber_generation.load(std::memory_order_acquire) ==
               handle.subscriber.generation &&
           record.lease_epoch.load(std::memory_order_acquire) ==
               handle.lease_epoch;
}

Result<SubscriberLeaseHandle> SubscriberLeaseTable::Register(
    BroadcastChannel::SubscriberHandle subscriber,
    const ProcessIdentity& owner, uint64_t now_ns) noexcept {
    if (subscriber.id.value >= kMaxSubscribers || owner.IsZero()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid subscriber id or owner identity");
    }
    SubscriberLeaseRecord& record = records_[subscriber.id.value];
    uint32_t state = record.state.load(std::memory_order_acquire);
    for (;;) {
        if (state != static_cast<uint32_t>(SubscriberLeaseState::kFree) &&
            state != static_cast<uint32_t>(SubscriberLeaseState::kEvicted)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "subscriber lease is already active");
        }
        if (record.state.compare_exchange_weak(
                state,
                static_cast<uint32_t>(SubscriberLeaseState::kRegistering),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
    }

    uint64_t lease_epoch =
        record.lease_epoch.load(std::memory_order_relaxed) + 1;
    if (lease_epoch == 0) {
        record.state.store(
            static_cast<uint32_t>(SubscriberLeaseState::kEvicted),
            std::memory_order_release);
        return Status::Error(StatusCode::kResourceExhausted,
                             "subscriber lease epoch exhausted");
    }
    record.subscriber_id = subscriber.id.value;
    record.subscriber_generation.store(subscriber.generation,
                                       std::memory_order_relaxed);
    record.owner = owner;
    record.lease_epoch.store(lease_epoch, std::memory_order_relaxed);
    record.heartbeat_ns.store(now_ns, std::memory_order_relaxed);
    record.state.store(static_cast<uint32_t>(SubscriberLeaseState::kActive),
                       std::memory_order_release);
    return SubscriberLeaseHandle{subscriber, lease_epoch};
}

Status SubscriberLeaseTable::Heartbeat(SubscriberLeaseHandle handle,
                                       uint64_t now_ns) noexcept {
    if (handle.subscriber.id.value >= kMaxSubscribers) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease id out of range");
    }
    SubscriberLeaseRecord& record = records_[handle.subscriber.id.value];
    if (record.state.load(std::memory_order_acquire) !=
            static_cast<uint32_t>(SubscriberLeaseState::kActive) ||
        !Matches(record, handle)) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease is stale or inactive");
    }
    record.heartbeat_ns.store(now_ns, std::memory_order_release);
    if (record.state.load(std::memory_order_acquire) !=
            static_cast<uint32_t>(SubscriberLeaseState::kActive) ||
        !Matches(record, handle)) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease was evicted during heartbeat");
    }
    return Status::Ok();
}

Result<SubscriberLeaseSnapshot> SubscriberLeaseTable::Read(
    uint32_t subscriber_id) const noexcept {
    if (subscriber_id >= kMaxSubscribers) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "subscriber lease id out of range");
    }
    const SubscriberLeaseRecord& record = records_[subscriber_id];
    SubscriberLeaseSnapshot snapshot;
    snapshot.state = static_cast<SubscriberLeaseState>(
        record.state.load(std::memory_order_acquire));
    snapshot.handle.subscriber.id = SubscriberId{subscriber_id};
    snapshot.handle.subscriber.generation =
        record.subscriber_generation.load(std::memory_order_acquire);
    snapshot.handle.lease_epoch =
        record.lease_epoch.load(std::memory_order_acquire);
    snapshot.owner = record.owner;
    snapshot.heartbeat_ns =
        record.heartbeat_ns.load(std::memory_order_acquire);
    return snapshot;
}

Result<SubscriberLeaseSnapshot> SubscriberLeaseTable::BeginEviction(
    SubscriberLeaseHandle handle, uint64_t now_ns, uint64_t lease_ns,
    bool require_expired) noexcept {
    if (handle.subscriber.id.value >= kMaxSubscribers) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease id out of range");
    }
    SubscriberLeaseRecord& record = records_[handle.subscriber.id.value];
    if (!Matches(record, handle)) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease generation or epoch mismatch");
    }
    uint64_t heartbeat = record.heartbeat_ns.load(std::memory_order_acquire);
    if (require_expired &&
        (now_ns < heartbeat || now_ns - heartbeat < lease_ns)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "subscriber lease has not expired");
    }

    uint32_t expected = static_cast<uint32_t>(SubscriberLeaseState::kActive);
    if (!record.state.compare_exchange_strong(
            expected, static_cast<uint32_t>(SubscriberLeaseState::kEvicting),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease is not active");
    }

    heartbeat = record.heartbeat_ns.load(std::memory_order_acquire);
    if (require_expired &&
        (now_ns < heartbeat || now_ns - heartbeat < lease_ns)) {
        record.state.store(
            static_cast<uint32_t>(SubscriberLeaseState::kActive),
            std::memory_order_release);
        return Status::Error(StatusCode::kWouldBlock,
                             "subscriber heartbeat renewed during eviction");
    }

    SubscriberLeaseSnapshot snapshot;
    snapshot.handle = handle;
    snapshot.owner = record.owner;
    snapshot.heartbeat_ns = heartbeat;
    snapshot.state = SubscriberLeaseState::kEvicting;
    return snapshot;
}

Status SubscriberLeaseTable::CancelEviction(
    SubscriberLeaseHandle handle) noexcept {
    if (handle.subscriber.id.value >= kMaxSubscribers) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease id out of range");
    }
    SubscriberLeaseRecord& record = records_[handle.subscriber.id.value];
    if (!Matches(record, handle)) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease generation or epoch mismatch");
    }
    uint32_t expected = static_cast<uint32_t>(SubscriberLeaseState::kEvicting);
    if (!record.state.compare_exchange_strong(
            expected, static_cast<uint32_t>(SubscriberLeaseState::kActive),
            std::memory_order_release, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "subscriber lease is not being evicted");
    }
    return Status::Ok();
}

Status SubscriberLeaseTable::FinishEviction(
    SubscriberLeaseHandle handle) noexcept {
    if (handle.subscriber.id.value >= kMaxSubscribers) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease id out of range");
    }
    SubscriberLeaseRecord& record = records_[handle.subscriber.id.value];
    if (!Matches(record, handle) ||
        record.state.load(std::memory_order_acquire) !=
            static_cast<uint32_t>(SubscriberLeaseState::kEvicting)) {
        return Status::Error(StatusCode::kNotFound,
                             "subscriber lease eviction ownership lost");
    }
    record.heartbeat_ns.store(0, std::memory_order_relaxed);
    record.state.store(static_cast<uint32_t>(SubscriberLeaseState::kEvicted),
                       std::memory_order_release);
    return Status::Ok();
}

SubscriberLeaseState SubscriberLeaseTable::State(
    uint32_t subscriber_id) const noexcept {
    if (subscriber_id >= kMaxSubscribers) {
        return SubscriberLeaseState::kFree;
    }
    return static_cast<SubscriberLeaseState>(
        records_[subscriber_id].state.load(std::memory_order_acquire));
}

SubscriberLeaseCoordinator::SubscriberLeaseCoordinator(
    BroadcastChannel& channel, SubscriberLeaseTable& leases,
    IdentityProbe identity_probe, void* identity_probe_context,
    PinCleanup pin_cleanup, void* pin_cleanup_context) noexcept
    : channel_(&channel),
      leases_(&leases),
      identity_probe_(identity_probe != nullptr ? identity_probe
                                                : &DefaultIdentityProbe),
      identity_probe_context_(identity_probe_context),
      pin_cleanup_(pin_cleanup),
      pin_cleanup_context_(pin_cleanup_context) {}

Result<SubscriberLeaseHandle> SubscriberLeaseCoordinator::Register(
    SubscriberId id, const ProcessIdentity& owner, uint64_t now_ns) noexcept {
    Result<BroadcastChannel::SubscriberHandle> subscriber =
        channel_->RegisterSubscriber(id, owner, now_ns);
    if (!subscriber.ok()) {
        return subscriber.status();
    }
    Result<SubscriberLeaseHandle> lease =
        leases_->Register(*subscriber, owner, now_ns);
    if (!lease.ok()) {
        channel_->UnregisterSubscriber(subscriber->id,
                                       subscriber->generation).ok();
        return lease.status();
    }
    return lease;
}

Status SubscriberLeaseCoordinator::Heartbeat(SubscriberLeaseHandle handle,
                                             uint64_t now_ns) noexcept {
    MINO_RETURN_IF_ERROR(leases_->Heartbeat(handle, now_ns));
    return channel_->Heartbeat(handle.subscriber, now_ns);
}

Status SubscriberLeaseCoordinator::Unregister(
    SubscriberLeaseHandle handle) noexcept {
    Result<SubscriberLeaseSnapshot> claimed = leases_->BeginEviction(
        handle, /*now_ns=*/0, /*lease_ns=*/0, /*require_expired=*/false);
    if (!claimed.ok()) {
        return claimed.status();
    }
    const Status channel_status = channel_->UnregisterSubscriber(
        handle.subscriber.id, handle.subscriber.generation);
    if (!channel_status.ok()) {
        leases_->CancelEviction(handle).ok();
        return channel_status;
    }
    CleanupPins(claimed->owner);
    return leases_->FinishEviction(handle);
}

uint64_t SubscriberLeaseCoordinator::EvictExpired(uint64_t now_ns,
                                                  uint64_t lease_ns) noexcept {
    uint64_t evicted = 0;
    for (uint32_t id = 0; id < SubscriberLeaseTable::kMaxSubscribers; ++id) {
        Result<SubscriberLeaseSnapshot> observed = leases_->Read(id);
        if (!observed.ok() ||
            observed->state != SubscriberLeaseState::kActive ||
            now_ns < observed->heartbeat_ns ||
            now_ns - observed->heartbeat_ns < lease_ns ||
            identity_probe_(observed->owner, identity_probe_context_) !=
                ProcessIdentityLiveness::kDead) {
            continue;
        }

        Result<SubscriberLeaseSnapshot> claimed = leases_->BeginEviction(
            observed->handle, now_ns, lease_ns, /*require_expired=*/true);
        if (!claimed.ok()) {
            continue;
        }
        const Status channel_status = channel_->UnregisterSubscriber(
            claimed->handle.subscriber.id,
            claimed->handle.subscriber.generation);
        if (!channel_status.ok()) {
            leases_->CancelEviction(claimed->handle).ok();
            continue;
        }
        CleanupPins(claimed->owner);
        if (leases_->FinishEviction(claimed->handle).ok()) {
            ++evicted;
        }
    }
    return evicted;
}

}  // namespace mino
