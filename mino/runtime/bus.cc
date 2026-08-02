// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/bus.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <exception>
#include <limits>
#include <mutex>
#include <new>
#include <type_traits>
#include <utility>

namespace mino {

void BusLifecycleFaultInjector::FailPublisherUnregisterAttempts(
    size_t attempts) noexcept {
    publisher_unregister_failures_.store(attempts, std::memory_order_relaxed);
}

void BusLifecycleFaultInjector::FailSubscriberUnregisterAttempts(
    size_t attempts) noexcept {
    subscriber_unregister_failures_.store(attempts, std::memory_order_relaxed);
}

void BusLifecycleFaultInjector::FailPublisherSlotReservationAttempts(
    size_t attempts) noexcept {
    publisher_slot_reservation_failures_.store(attempts,
                                               std::memory_order_relaxed);
}

void BusLifecycleFaultInjector::FailSubscriberSlotReservationAttempts(
    size_t attempts) noexcept {
    subscriber_slot_reservation_failures_.store(attempts,
                                                std::memory_order_relaxed);
}

bool BusLifecycleFaultInjector::ShouldFailPublisherUnregister() noexcept {
    size_t remaining =
        publisher_unregister_failures_.load(std::memory_order_relaxed);
    while (remaining != 0) {
        if (publisher_unregister_failures_.compare_exchange_weak(
                remaining, remaining - 1, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

bool BusLifecycleFaultInjector::ShouldFailSubscriberUnregister() noexcept {
    size_t remaining =
        subscriber_unregister_failures_.load(std::memory_order_relaxed);
    while (remaining != 0) {
        if (subscriber_unregister_failures_.compare_exchange_weak(
                remaining, remaining - 1, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

bool BusLifecycleFaultInjector::ShouldFailPublisherSlotReservation() noexcept {
    size_t remaining =
        publisher_slot_reservation_failures_.load(std::memory_order_relaxed);
    while (remaining != 0) {
        if (publisher_slot_reservation_failures_.compare_exchange_weak(
                remaining, remaining - 1, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

bool BusLifecycleFaultInjector::ShouldFailSubscriberSlotReservation() noexcept {
    size_t remaining =
        subscriber_slot_reservation_failures_.load(std::memory_order_relaxed);
    while (remaining != 0) {
        if (subscriber_slot_reservation_failures_.compare_exchange_weak(
                remaining, remaining - 1, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

static_assert(std::is_nothrow_copy_constructible_v<
              registry::PublisherRegistration>);
static_assert(std::is_nothrow_copy_constructible_v<
              registry::SubscriberRegistration>);
static_assert(std::is_nothrow_move_assignable_v<
              registry::PublisherRegistration>);
static_assert(std::is_nothrow_move_assignable_v<
              registry::SubscriberRegistration>);

class DeferredDebtAnchor final {
public:
    DeferredDebtAnchor(
        std::shared_ptr<registry::Coordinator> coordinator,
        std::shared_ptr<BusLifecycleFaultInjector> fault_injector) noexcept
        : coordinator(std::move(coordinator)),
          fault_injector(std::move(fault_injector)) {}

    Status UnregisterPublisher(
        const registry::PublisherRegistration& registration) {
        if (fault_injector != nullptr &&
            fault_injector->ShouldFailPublisherUnregister()) {
            return Status::Error(StatusCode::kUnavailable);
        }
        return coordinator->UnregisterPublisher(registration);
    }

    Status UnregisterSubscriber(
        const registry::SubscriberRegistration& registration) {
        if (fault_injector != nullptr &&
            fault_injector->ShouldFailSubscriberUnregister()) {
            return Status::Error(StatusCode::kUnavailable);
        }
        return coordinator->UnregisterSubscriber(registration);
    }

    void Retry() noexcept {
        try {
            std::lock_guard lock(mutex);
            for (auto current = deferred_publishers.begin();
                 current != deferred_publishers.end();) {
                if (UnregisterPublisher(*current).ok()) {
                    current = deferred_publishers.erase(current);
                } else {
                    ++current;
                }
            }
            for (auto current = deferred_subscribers.begin();
                 current != deferred_subscribers.end();) {
                if (UnregisterSubscriber(*current).ok()) {
                    current = deferred_subscribers.erase(current);
                } else {
                    ++current;
                }
            }
        } catch (...) {
        }
    }

    bool empty() noexcept {
        std::lock_guard lock(mutex);
        return deferred_publishers.empty() && deferred_subscribers.empty();
    }

    std::shared_ptr<registry::Coordinator> coordinator;
    std::shared_ptr<BusLifecycleFaultInjector> fault_injector;
    std::mutex mutex;
    std::vector<registry::PublisherRegistration> deferred_publishers;
    std::vector<registry::SubscriberRegistration> deferred_subscribers;
    size_t active_publisher_slots = 0;
    size_t active_subscriber_slots = 0;
    DeferredDebtAnchor* next = nullptr;
};

namespace {

class ProcessDeferredDebtRegistry final {
public:
    ~ProcessDeferredDebtRegistry() {
        RetryAll();
        DeferredDebtAnchor* remaining = nullptr;
        {
            std::lock_guard lock(mutex_);
            remaining = head_;
            head_ = nullptr;
        }
        while (remaining != nullptr) {
            DeferredDebtAnchor* next = remaining->next;
            remaining->Retry();
            delete remaining;
            remaining = next;
        }
    }

    void Adopt(std::unique_ptr<DeferredDebtAnchor> anchor) noexcept {
        assert(anchor != nullptr);
        assert(!anchor->empty());
        std::lock_guard lock(mutex_);
        DeferredDebtAnchor* raw = anchor.release();
        raw->next = head_;
        head_ = raw;
    }

    void RetryAll() noexcept {
        try {
            std::lock_guard lock(mutex_);
            DeferredDebtAnchor** link = &head_;
            while (*link != nullptr) {
                DeferredDebtAnchor* current = *link;
                current->Retry();
                if (current->empty()) {
                    *link = current->next;
                    delete current;
                } else {
                    link = &current->next;
                }
            }
        } catch (...) {
        }
    }

private:
    std::mutex mutex_;
    DeferredDebtAnchor* head_ = nullptr;
};

ProcessDeferredDebtRegistry& DeferredDebtRegistry() {
    static ProcessDeferredDebtRegistry registry;
    return registry;
}

void RetryProcessDeferredDebts() noexcept {
    DeferredDebtRegistry().RetryAll();
}

}  // namespace

class BusSharedContext final {
public:
    BusSharedContext(
        registry::NodeLeaseOwner owner,
        std::shared_ptr<registry::Coordinator> coordinator,
        std::shared_ptr<transport::TransportSwitcher> transport_switcher,
        std::shared_ptr<ParticipantIdAllocator> participant_ids,
        std::shared_ptr<BusLocalEndpointProvider> local_endpoints,
        std::shared_ptr<BridgeDispatcher> bridge_dispatcher,
        std::shared_ptr<BusLifecycleFaultInjector> fault_injector)
        : owner(std::move(owner)),
          coordinator(std::move(coordinator)),
          transport_switcher(std::move(transport_switcher)),
          participant_ids(std::move(participant_ids)),
          local_endpoints(std::move(local_endpoints)),
          bridge_dispatcher(std::move(bridge_dispatcher)),
          fault_injector(std::move(fault_injector)),
          debt_anchor_(std::make_unique<DeferredDebtAnchor>(this->coordinator,
                                                           this->fault_injector)) {}

    ~BusSharedContext() {
        debt_anchor_->Retry();
        assert(debt_anchor_->active_publisher_slots == 0);
        assert(debt_anchor_->active_subscriber_slots == 0);
        if (!debt_anchor_->empty()) {
            DeferredDebtRegistry().Adopt(std::move(debt_anchor_));
        }
    }

    uint64_t NowNs() const noexcept {
        const auto now = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }

    Status UnregisterPublisher(
        const registry::PublisherRegistration& registration) {
        return debt_anchor_->UnregisterPublisher(registration);
    }

    Status UnregisterSubscriber(
        const registry::SubscriberRegistration& registration) {
        return debt_anchor_->UnregisterSubscriber(registration);
    }

    Status ReservePublisherUnregistrationSlot() {
        if (fault_injector != nullptr &&
            fault_injector->ShouldFailPublisherSlotReservation()) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        try {
            std::lock_guard lock(debt_anchor_->mutex);
            if (debt_anchor_->active_publisher_slots >=
                std::numeric_limits<size_t>::max() -
                    debt_anchor_->deferred_publishers.size()) {
                return Status::Error(StatusCode::kResourceExhausted);
            }
            const size_t required = debt_anchor_->deferred_publishers.size() +
                                    debt_anchor_->active_publisher_slots + 1;
            debt_anchor_->deferred_publishers.reserve(required);
            ++debt_anchor_->active_publisher_slots;
            assert(debt_anchor_->deferred_publishers.size() +
                       debt_anchor_->active_publisher_slots <=
                   debt_anchor_->deferred_publishers.capacity());
            return Status::Ok();
        } catch (const std::bad_alloc&) {
            return Status::Error(StatusCode::kResourceExhausted);
        } catch (...) {
            return Status::Error(StatusCode::kInternal);
        }
    }

    Status ReserveSubscriberUnregistrationSlot() {
        if (fault_injector != nullptr &&
            fault_injector->ShouldFailSubscriberSlotReservation()) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        try {
            std::lock_guard lock(debt_anchor_->mutex);
            if (debt_anchor_->active_subscriber_slots >=
                std::numeric_limits<size_t>::max() -
                    debt_anchor_->deferred_subscribers.size()) {
                return Status::Error(StatusCode::kResourceExhausted);
            }
            const size_t required = debt_anchor_->deferred_subscribers.size() +
                                    debt_anchor_->active_subscriber_slots + 1;
            debt_anchor_->deferred_subscribers.reserve(required);
            ++debt_anchor_->active_subscriber_slots;
            assert(debt_anchor_->deferred_subscribers.size() +
                       debt_anchor_->active_subscriber_slots <=
                   debt_anchor_->deferred_subscribers.capacity());
            return Status::Ok();
        } catch (const std::bad_alloc&) {
            return Status::Error(StatusCode::kResourceExhausted);
        } catch (...) {
            return Status::Error(StatusCode::kInternal);
        }
    }

    void ReleasePublisherUnregistrationSlot() noexcept {
        std::lock_guard lock(debt_anchor_->mutex);
        assert(debt_anchor_->active_publisher_slots != 0);
        --debt_anchor_->active_publisher_slots;
    }

    void ReleaseSubscriberUnregistrationSlot() noexcept {
        std::lock_guard lock(debt_anchor_->mutex);
        assert(debt_anchor_->active_subscriber_slots != 0);
        --debt_anchor_->active_subscriber_slots;
    }

    void DeferPublisherUnregistration(
        const registry::PublisherRegistration& registration) noexcept {
        std::lock_guard lock(debt_anchor_->mutex);
        assert(debt_anchor_->active_publisher_slots != 0);
        assert(debt_anchor_->deferred_publishers.size() <
               debt_anchor_->deferred_publishers.capacity());
        debt_anchor_->deferred_publishers.push_back(registration);
        --debt_anchor_->active_publisher_slots;
    }

    void DeferSubscriberUnregistration(
        const registry::SubscriberRegistration& registration) noexcept {
        std::lock_guard lock(debt_anchor_->mutex);
        assert(debt_anchor_->active_subscriber_slots != 0);
        assert(debt_anchor_->deferred_subscribers.size() <
               debt_anchor_->deferred_subscribers.capacity());
        debt_anchor_->deferred_subscribers.push_back(registration);
        --debt_anchor_->active_subscriber_slots;
    }

    Status RetryDeferredPublisherUnregistration(
        const registry::PublisherRegistration& registration) {
        std::lock_guard lock(debt_anchor_->mutex);
        const auto found = std::find_if(
            debt_anchor_->deferred_publishers.begin(),
            debt_anchor_->deferred_publishers.end(),
            [&registration](const registry::PublisherRegistration& candidate) {
                return candidate.topic_id == registration.topic_id &&
                       candidate.publisher_id == registration.publisher_id &&
                       candidate.generation == registration.generation &&
                       candidate.owner == registration.owner;
            });
        if (found == debt_anchor_->deferred_publishers.end()) {
            return Status::Ok();
        }
        const Status status = debt_anchor_->UnregisterPublisher(*found);
        if (status.ok()) {
            debt_anchor_->deferred_publishers.erase(found);
        }
        return status;
    }

    Status RetryDeferredSubscriberUnregistration(
        const registry::SubscriberRegistration& registration) {
        std::lock_guard lock(debt_anchor_->mutex);
        const auto found = std::find_if(
            debt_anchor_->deferred_subscribers.begin(),
            debt_anchor_->deferred_subscribers.end(),
            [&registration](const registry::SubscriberRegistration& candidate) {
                return candidate.topic_id == registration.topic_id &&
                       candidate.subscriber_id == registration.subscriber_id &&
                       candidate.generation == registration.generation &&
                       candidate.owner == registration.owner;
            });
        if (found == debt_anchor_->deferred_subscribers.end()) {
            return Status::Ok();
        }
        const Status status = debt_anchor_->UnregisterSubscriber(*found);
        if (status.ok()) {
            debt_anchor_->deferred_subscribers.erase(found);
        }
        return status;
    }

    void RetryDeferredUnregistrations() noexcept { debt_anchor_->Retry(); }

    registry::NodeLeaseOwner owner;
    std::shared_ptr<registry::Coordinator> coordinator;
    std::shared_ptr<transport::TransportSwitcher> transport_switcher;
    std::shared_ptr<ParticipantIdAllocator> participant_ids;
    std::shared_ptr<BusLocalEndpointProvider> local_endpoints;
    std::shared_ptr<BridgeDispatcher> bridge_dispatcher;
    std::shared_ptr<BusLifecycleFaultInjector> fault_injector;

private:
    std::unique_ptr<DeferredDebtAnchor> debt_anchor_;
};

namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Internal(std::string_view message) {
    return Status::Error(StatusCode::kInternal, message);
}

Status AllocationFailure() {
    return Status::Error(StatusCode::kResourceExhausted,
                         "Bus allocation failed");
}

bool HasNonzeroDigest(const schema::CanonicalDigest& digest) noexcept {
    return std::any_of(digest.begin(), digest.end(), [](std::byte value) {
        return value != std::byte{0};
    });
}

bool CompleteSchema(const schema::SchemaIdentity& schema) noexcept {
    return schema.short_id() != 0 && schema.schema_version() != 0 &&
           schema.layout_version() != 0 &&
           HasNonzeroDigest(schema.canonical_digest());
}

Status ValidateTopicAndSchema(const registry::TopicMetadata& topic,
                              const schema::SchemaIdentity& requested,
                              bool allow_accepted_reader_schema) {
    if (topic.state != registry::TopicState::kActive) {
        return Status::Error(StatusCode::kUnavailable,
                             "Bus endpoints require an active topic");
    }
    if (!CompleteSchema(topic.schema)) {
        return Status::Error(StatusCode::kCorruption,
                             "topic schema identity is incomplete");
    }
    if (!CompleteSchema(requested)) {
        return Invalid("requested schema identity is incomplete");
    }
    const bool primary =
        registry::SchemaIdentityEqual(topic.schema, requested);
    const bool accepted =
        allow_accepted_reader_schema &&
        std::any_of(topic.accepted_schemas.begin(),
                    topic.accepted_schemas.end(),
                    [&requested](const schema::SchemaIdentity& candidate) {
                        return registry::SchemaIdentityEqual(candidate,
                                                             requested);
                    });
    if (!primary && !accepted) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "requested schema is not allowed by topic policy");
    }
    return Status::Ok();
}

Status ValidateBinding(const registry::TopicMetadata& topic,
                       const BusLocalResourceBinding& binding) {
    if (binding.publication == nullptr) {
        return Status::Error(StatusCode::kUnavailable,
                             "local publication binding is unavailable");
    }
    if (binding.topic_id != topic.topic_id ||
        binding.region_version != topic.region_version ||
        binding.channel_version != topic.channel_version ||
        binding.acl_version != topic.acl_version) {
        return Status::Error(StatusCode::kUnavailable,
                             "local resource binding is stale or mismatched");
    }
    return Status::Ok();
}

Status ValidatePublication(
    const LocalPublication& publication,
    const registry::PublisherRegistration& registration) {
    if (publication.source.node_id != registration.owner.node_id.value ||
        publication.source.publisher_id != registration.publisher_id.value ||
        publication.source.publisher_epoch != registration.generation) {
        return Status::Error(StatusCode::kCorruption,
                             "local publisher returned a mismatched source identity");
    }
    if (publication.sequence_num == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "local publisher returned sequence zero");
    }
    return Status::Ok();
}

class PublisherRegistrationRollback final {
public:
    PublisherRegistrationRollback(
        std::shared_ptr<BusSharedContext> context,
        registry::PublisherRegistration registration) noexcept
        : context_(std::move(context)), registration_(registration) {}

    ~PublisherRegistrationRollback() {
        if (!active_) {
            return;
        }
        if (!registered_) {
            context_->ReleasePublisherUnregistrationSlot();
            return;
        }
        const Status rollback = context_->UnregisterPublisher(registration_);
        if (rollback.ok()) {
            context_->ReleasePublisherUnregistrationSlot();
        } else {
            context_->DeferPublisherUnregistration(registration_);
        }
    }

    void MarkRegistered() noexcept { registered_ = true; }

    Status Fail(const Status& original) {
        assert(registered_);
        const Status rollback = context_->UnregisterPublisher(registration_);
        active_ = false;
        if (rollback.ok()) {
            context_->ReleasePublisherUnregistrationSlot();
            return original;
        }
        context_->DeferPublisherUnregistration(registration_);
        return Internal("publisher creation failed and registration rollback failed");
    }

    void Release() noexcept {
        assert(registered_);
        active_ = false;
    }

private:
    std::shared_ptr<BusSharedContext> context_;
    registry::PublisherRegistration registration_;
    bool registered_ = false;
    bool active_ = true;
};

class SubscriberRegistrationRollback final {
public:
    SubscriberRegistrationRollback(
        std::shared_ptr<BusSharedContext> context,
        registry::SubscriberRegistration registration) noexcept
        : context_(std::move(context)), registration_(registration) {}

    ~SubscriberRegistrationRollback() {
        if (!active_) {
            return;
        }
        if (!registered_) {
            context_->ReleaseSubscriberUnregistrationSlot();
            return;
        }
        const Status rollback = context_->UnregisterSubscriber(registration_);
        if (rollback.ok()) {
            context_->ReleaseSubscriberUnregistrationSlot();
        } else {
            context_->DeferSubscriberUnregistration(registration_);
        }
    }

    void MarkRegistered() noexcept { registered_ = true; }

    Status Fail(const Status& original) {
        assert(registered_);
        const Status rollback = context_->UnregisterSubscriber(registration_);
        active_ = false;
        if (rollback.ok()) {
            context_->ReleaseSubscriberUnregistrationSlot();
            return original;
        }
        context_->DeferSubscriberUnregistration(registration_);
        return Internal("subscriber creation failed and registration rollback failed");
    }

    void Release() noexcept {
        assert(registered_);
        active_ = false;
    }

private:
    std::shared_ptr<BusSharedContext> context_;
    registry::SubscriberRegistration registration_;
    bool registered_ = false;
    bool active_ = true;
};

}  // namespace

BusPublisher::BusPublisher(std::shared_ptr<BusSharedContext> context,
                           registry::PublisherRegistration registration,
                           registry::DeliveryPolicy delivery,
                           schema::SchemaIdentity schema,
                           BusLocalPublisherResources resources) noexcept
    : context_(std::move(context)),
      registration_(registration),
      delivery_(delivery),
      schema_(std::move(schema)),
      binding_(std::move(resources.binding.publication)),
      endpoint_(std::move(resources.endpoint)) {}

BusPublisher::BusPublisher(BusPublisher&& other) noexcept
    : context_(std::move(other.context_)),
      registration_(other.registration_),
      delivery_(other.delivery_),
      schema_(std::move(other.schema_)),
      binding_(std::move(other.binding_)),
      endpoint_(std::move(other.endpoint_)),
      owns_registration_slot_(other.owns_registration_slot_),
      registration_deferred_(other.registration_deferred_) {
    other.owns_registration_slot_ = false;
    other.registration_deferred_ = false;
}

BusPublisher& BusPublisher::operator=(BusPublisher&& other) noexcept {
    if (this != &other) {
        Reset();
        context_ = std::move(other.context_);
        registration_ = other.registration_;
        delivery_ = other.delivery_;
        schema_ = std::move(other.schema_);
        binding_ = std::move(other.binding_);
        endpoint_ = std::move(other.endpoint_);
        owns_registration_slot_ = other.owns_registration_slot_;
        registration_deferred_ = other.registration_deferred_;
        other.owns_registration_slot_ = false;
        other.registration_deferred_ = false;
    }
    return *this;
}

BusPublisher::~BusPublisher() { Reset(); }

Status BusPublisher::Close() {
    // Disable local activity before changing the registry, but retain the
    // binding and context until exact unregistration succeeds.
    endpoint_.reset();
    if (context_ == nullptr) {
        binding_.reset();
        owns_registration_slot_ = false;
        registration_deferred_ = false;
        return Status::Ok();
    }

    Status unregistered;
    try {
        unregistered = registration_deferred_
                           ? context_->RetryDeferredPublisherUnregistration(
                                 registration_)
                           : context_->UnregisterPublisher(registration_);
    } catch (const std::bad_alloc&) {
        unregistered = Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::exception&) {
        unregistered = Status::Error(StatusCode::kInternal);
    } catch (...) {
        unregistered = Status::Error(StatusCode::kInternal);
    }

    if (!unregistered.ok()) {
        if (owns_registration_slot_) {
            context_->DeferPublisherUnregistration(registration_);
            owns_registration_slot_ = false;
            registration_deferred_ = true;
        }
        return unregistered;
    }

    if (owns_registration_slot_) {
        context_->ReleasePublisherUnregistrationSlot();
        owns_registration_slot_ = false;
    }
    registration_deferred_ = false;
    binding_.reset();
    context_.reset();
    return Status::Ok();
}

void BusPublisher::Reset() noexcept {
    const Status closed = Close();
    if (!closed.ok() && context_ != nullptr) {
        std::shared_ptr<BusSharedContext> context = context_;
        binding_.reset();
        context_.reset();
        owns_registration_slot_ = false;
        registration_deferred_ = false;
        context->RetryDeferredUnregistrations();
    }
}

Result<BusPublishResult> BusPublisher::Publish(
    std::span<const std::byte> canonical_payload, uint8_t priority) {
    RetryProcessDeferredDebts();
    if (context_ == nullptr || endpoint_ == nullptr || binding_ == nullptr) {
        return Invalid("publisher endpoint is not active");
    }
    if (canonical_payload.size() > kMaxBusCanonicalPayloadBytes ||
        canonical_payload.size() > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "canonical payload exceeds the Bus bound");
    }

    context_->RetryDeferredUnregistrations();
    try {
        transport::RouteRequest route_request{
            .topic_id = registration_.topic_id,
            .payload_size = static_cast<uint32_t>(canonical_payload.size()),
            .delivery = delivery_,
            .priority = priority,
            .publisher_schema = schema_,
        };
        Result<std::shared_ptr<const transport::RouteHandle>> route =
            context_->transport_switcher->Resolve(route_request);
        if (!route.ok()) {
            return route.status();
        }

        Result<LocalPublication> local =
            endpoint_->Publish(canonical_payload, priority);
        if (!local.ok()) {
            return local.status();
        }
        const Status publication_status =
            ValidatePublication(*local, registration_);
        if (!publication_status.ok()) {
            return publication_status;
        }

        BusPublishResult result{
            .publication = *local,
            .route = std::move(*route),
            .bridge_status = Status::Ok(),
        };
        BridgeDispatchRequest dispatch{
            .topic_id = registration_.topic_id,
            .schema = schema_,
            .publication = result.publication,
            .priority = priority,
            .canonical_payload = canonical_payload,
            .route = result.route,
        };
        try {
            result.bridge_status =
                context_->bridge_dispatcher->Dispatch(dispatch);
        } catch (const std::bad_alloc&) {
            result.bridge_status =
                Status::Error(StatusCode::kResourceExhausted);
        } catch (const std::exception&) {
            result.bridge_status = Status::Error(StatusCode::kInternal);
        } catch (...) {
            result.bridge_status = Status::Error(StatusCode::kInternal);
        }
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception&) {
        return Internal("publisher seam threw an exception");
    } catch (...) {
        return Internal("publisher seam threw a non-standard exception");
    }
}

BusSubscriber::BusSubscriber(std::shared_ptr<BusSharedContext> context,
                             registry::SubscriberRegistration registration,
                             schema::SchemaIdentity schema,
                             BusLocalSubscriberResources resources) noexcept
    : context_(std::move(context)),
      registration_(registration),
      schema_(std::move(schema)),
      binding_(std::move(resources.binding.publication)),
      endpoint_(std::move(resources.endpoint)) {}

BusSubscriber::BusSubscriber(BusSubscriber&& other) noexcept
    : context_(std::move(other.context_)),
      registration_(other.registration_),
      schema_(std::move(other.schema_)),
      binding_(std::move(other.binding_)),
      endpoint_(std::move(other.endpoint_)),
      owns_registration_slot_(other.owns_registration_slot_),
      registration_deferred_(other.registration_deferred_) {
    other.owns_registration_slot_ = false;
    other.registration_deferred_ = false;
}

BusSubscriber& BusSubscriber::operator=(BusSubscriber&& other) noexcept {
    if (this != &other) {
        Reset();
        context_ = std::move(other.context_);
        registration_ = other.registration_;
        schema_ = std::move(other.schema_);
        binding_ = std::move(other.binding_);
        endpoint_ = std::move(other.endpoint_);
        owns_registration_slot_ = other.owns_registration_slot_;
        registration_deferred_ = other.registration_deferred_;
        other.owns_registration_slot_ = false;
        other.registration_deferred_ = false;
    }
    return *this;
}

BusSubscriber::~BusSubscriber() { Reset(); }

Status BusSubscriber::Close() {
    // Disable local activity before changing the registry, but retain the
    // binding and context until exact unregistration succeeds.
    endpoint_.reset();
    if (context_ == nullptr) {
        binding_.reset();
        owns_registration_slot_ = false;
        registration_deferred_ = false;
        return Status::Ok();
    }

    Status unregistered;
    try {
        unregistered = registration_deferred_
                           ? context_->RetryDeferredSubscriberUnregistration(
                                 registration_)
                           : context_->UnregisterSubscriber(registration_);
    } catch (const std::bad_alloc&) {
        unregistered = Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::exception&) {
        unregistered = Status::Error(StatusCode::kInternal);
    } catch (...) {
        unregistered = Status::Error(StatusCode::kInternal);
    }

    if (!unregistered.ok()) {
        if (owns_registration_slot_) {
            context_->DeferSubscriberUnregistration(registration_);
            owns_registration_slot_ = false;
            registration_deferred_ = true;
        }
        return unregistered;
    }

    if (owns_registration_slot_) {
        context_->ReleaseSubscriberUnregistrationSlot();
        owns_registration_slot_ = false;
    }
    registration_deferred_ = false;
    binding_.reset();
    context_.reset();
    return Status::Ok();
}

void BusSubscriber::Reset() noexcept {
    const Status closed = Close();
    if (!closed.ok() && context_ != nullptr) {
        std::shared_ptr<BusSharedContext> context = context_;
        binding_.reset();
        context_.reset();
        owns_registration_slot_ = false;
        registration_deferred_ = false;
        context->RetryDeferredUnregistrations();
    }
}

Result<CanonicalMessage> BusSubscriber::TryPoll() {
    RetryProcessDeferredDebts();
    if (context_ == nullptr || endpoint_ == nullptr || binding_ == nullptr) {
        return Invalid("subscriber endpoint is not active");
    }
    context_->RetryDeferredUnregistrations();
    try {
        Result<CanonicalMessage> message = endpoint_->TryPoll();
        if (!message.ok()) {
            return message.status();
        }
        if (message->payload.size() > kMaxBusCanonicalPayloadBytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "polled canonical payload exceeds the Bus bound");
        }
        if (!registry::SchemaIdentityEqual(message->schema, schema_)) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "polled message schema does not match subscriber");
        }
        return std::move(*message);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception&) {
        return Internal("subscriber seam threw an exception");
    } catch (...) {
        return Internal("subscriber seam threw a non-standard exception");
    }
}

Result<std::unique_ptr<Bus>> Bus::Create(
    registry::NodeLeaseOwner local_owner,
    std::shared_ptr<registry::Coordinator> coordinator,
    std::shared_ptr<transport::TransportSwitcher> transport_switcher,
    std::shared_ptr<ParticipantIdAllocator> participant_ids,
    std::shared_ptr<BusLocalEndpointProvider> local_endpoints,
    std::shared_ptr<BridgeDispatcher> bridge_dispatcher,
    std::shared_ptr<BusLifecycleFaultInjector> fault_injector) {
    RetryProcessDeferredDebts();
    if (local_owner.node_id.value == 0 || local_owner.process_identity.IsZero() ||
        local_owner.lease_epoch == 0) {
        return Invalid("local node lease owner is incomplete");
    }
    if (coordinator == nullptr || transport_switcher == nullptr ||
        participant_ids == nullptr || local_endpoints == nullptr ||
        bridge_dispatcher == nullptr) {
        return Invalid("Bus dependencies must not be null");
    }

    Result<std::shared_ptr<const registry::NodeMetadata>> node =
        coordinator->GetNode(local_owner.node_id);
    if (!node.ok()) {
        return node.status();
    }
    if ((*node)->process_identity != local_owner.process_identity ||
        (*node)->lease_epoch != local_owner.lease_epoch) {
        return Status::Error(StatusCode::kNotFound,
                             "local owner does not match the registered node lease");
    }
    if ((*node)->lease_state != registry::NodeLeaseState::kActive) {
        return Status::Error(StatusCode::kUnavailable,
                             "local node lease is not active");
    }

    try {
        auto context = std::make_shared<BusSharedContext>(
            local_owner, std::move(coordinator), std::move(transport_switcher),
            std::move(participant_ids), std::move(local_endpoints),
            std::move(bridge_dispatcher), std::move(fault_injector));
        return std::unique_ptr<Bus>(new Bus(std::move(context)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Bus::Bus(std::shared_ptr<BusSharedContext> context) noexcept
    : context_(std::move(context)) {}

Bus::~Bus() = default;

Result<BusPublisher> Bus::CreatePublisher(
    TopicId topic_id, const schema::SchemaIdentity& schema) {
    RetryProcessDeferredDebts();
    context_->RetryDeferredUnregistrations();
    if (topic_id.value == 0) {
        return Invalid("topic ID is zero");
    }
    Result<std::shared_ptr<const registry::TopicSnapshot>> topic =
        context_->coordinator->GetTopic(topic_id);
    if (!topic.ok()) {
        return topic.status();
    }
    return CreatePublisherFromSnapshot(std::move(*topic), schema);
}

Result<BusPublisher> Bus::CreatePublisher(
    std::string_view topic_name, const schema::SchemaIdentity& schema) {
    RetryProcessDeferredDebts();
    context_->RetryDeferredUnregistrations();
    if (topic_name.empty()) {
        return Invalid("topic name is empty");
    }
    Result<std::shared_ptr<const registry::TopicSnapshot>> topic =
        context_->coordinator->FindTopic(topic_name);
    if (!topic.ok()) {
        return topic.status();
    }
    return CreatePublisherFromSnapshot(std::move(*topic), schema);
}

Result<BusSubscriber> Bus::CreateSubscriber(
    TopicId topic_id, const schema::SchemaIdentity& schema) {
    RetryProcessDeferredDebts();
    context_->RetryDeferredUnregistrations();
    if (topic_id.value == 0) {
        return Invalid("topic ID is zero");
    }
    Result<std::shared_ptr<const registry::TopicSnapshot>> topic =
        context_->coordinator->GetTopic(topic_id);
    if (!topic.ok()) {
        return topic.status();
    }
    return CreateSubscriberFromSnapshot(std::move(*topic), schema);
}

Result<BusSubscriber> Bus::CreateSubscriber(
    std::string_view topic_name, const schema::SchemaIdentity& schema) {
    RetryProcessDeferredDebts();
    context_->RetryDeferredUnregistrations();
    if (topic_name.empty()) {
        return Invalid("topic name is empty");
    }
    Result<std::shared_ptr<const registry::TopicSnapshot>> topic =
        context_->coordinator->FindTopic(topic_name);
    if (!topic.ok()) {
        return topic.status();
    }
    return CreateSubscriberFromSnapshot(std::move(*topic), schema);
}

Result<BusPublisher> Bus::CreatePublisherFromSnapshot(
    std::shared_ptr<const registry::TopicSnapshot> topic,
    const schema::SchemaIdentity& schema) {
    const Status valid =
        ValidateTopicAndSchema(topic->metadata, schema, false);
    if (!valid.ok()) {
        return valid;
    }

    try {
        Result<PublisherParticipantIdentity> allocated =
            context_->participant_ids->AllocatePublisher();
        if (!allocated.ok()) {
            return allocated.status();
        }
        if (allocated->publisher_id.value == 0 || allocated->generation == 0) {
            return Internal("participant allocator returned an incomplete publisher ID");
        }

        const registry::PublisherRegistration registration{
            .topic_id = topic->metadata.topic_id,
            .publisher_id = allocated->publisher_id,
            .generation = allocated->generation,
            .owner = context_->owner,
        };
        const Status reserved =
            context_->ReservePublisherUnregistrationSlot();
        if (!reserved.ok()) {
            return reserved;
        }
        PublisherRegistrationRollback rollback(context_, registration);
        const Status registered = context_->coordinator->RegisterPublisher(
            registration, context_->NowNs());
        if (!registered.ok()) {
            return registered;
        }
        rollback.MarkRegistered();

        Result<BusLocalPublisherResources> resources =
            context_->local_endpoints->OpenPublisher(topic->metadata,
                                                     registration);
        if (!resources.ok()) {
            return rollback.Fail(resources.status());
        }
        if (resources->endpoint == nullptr) {
            const Status failure =
                Internal("local provider returned a null publisher endpoint");
            resources->binding.publication.reset();
            return rollback.Fail(failure);
        }
        const Status binding =
            ValidateBinding(topic->metadata, resources->binding);
        if (!binding.ok()) {
            resources->endpoint.reset();
            resources->binding.publication.reset();
            return rollback.Fail(binding);
        }

        const Status refreshed =
            context_->transport_switcher->RefreshTopic(topic->metadata.topic_id);
        if (!refreshed.ok()) {
            resources->endpoint.reset();
            resources->binding.publication.reset();
            return rollback.Fail(refreshed);
        }

        BusPublisher publisher(context_, registration, topic->metadata.delivery,
                               schema, std::move(*resources));
        rollback.Release();
        return publisher;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception&) {
        return Internal("publisher creation seam threw an exception");
    } catch (...) {
        return Internal("publisher creation seam threw a non-standard exception");
    }
}

Result<BusSubscriber> Bus::CreateSubscriberFromSnapshot(
    std::shared_ptr<const registry::TopicSnapshot> topic,
    const schema::SchemaIdentity& schema) {
    const Status valid =
        ValidateTopicAndSchema(topic->metadata, schema, true);
    if (!valid.ok()) {
        return valid;
    }

    try {
        Result<SubscriberParticipantIdentity> allocated =
            context_->participant_ids->AllocateSubscriber();
        if (!allocated.ok()) {
            return allocated.status();
        }
        if (allocated->subscriber_id.value == 0 || allocated->generation == 0) {
            return Internal("participant allocator returned an incomplete subscriber ID");
        }

        const registry::SubscriberRegistration registration{
            .topic_id = topic->metadata.topic_id,
            .subscriber_id = allocated->subscriber_id,
            .generation = allocated->generation,
            .owner = context_->owner,
        };
        const Status reserved =
            context_->ReserveSubscriberUnregistrationSlot();
        if (!reserved.ok()) {
            return reserved;
        }
        SubscriberRegistrationRollback rollback(context_, registration);
        const Status registered = context_->coordinator->RegisterSubscriber(
            registration, context_->NowNs());
        if (!registered.ok()) {
            return registered;
        }
        rollback.MarkRegistered();

        Result<BusLocalSubscriberResources> resources =
            context_->local_endpoints->OpenSubscriber(topic->metadata,
                                                      registration);
        if (!resources.ok()) {
            return rollback.Fail(resources.status());
        }
        if (resources->endpoint == nullptr) {
            const Status failure =
                Internal("local provider returned a null subscriber endpoint");
            resources->binding.publication.reset();
            return rollback.Fail(failure);
        }
        const Status binding =
            ValidateBinding(topic->metadata, resources->binding);
        if (!binding.ok()) {
            resources->endpoint.reset();
            resources->binding.publication.reset();
            return rollback.Fail(binding);
        }

        const Status refreshed =
            context_->transport_switcher->RefreshTopic(topic->metadata.topic_id);
        if (!refreshed.ok()) {
            resources->endpoint.reset();
            resources->binding.publication.reset();
            return rollback.Fail(refreshed);
        }

        BusSubscriber subscriber(context_, registration, schema,
                                 std::move(*resources));
        rollback.Release();
        return subscriber;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception&) {
        return Internal("subscriber creation seam threw an exception");
    } catch (...) {
        return Internal("subscriber creation seam threw a non-standard exception");
    }
}

}  // namespace mino
