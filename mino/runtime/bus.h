// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_BUS_H_
#define MINO_RUNTIME_BUS_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "mino/bridge/source_identity.h"
#include "mino/common/result.h"
#include "mino/registry/coordinator.h"
#include "mino/registry/metadata.h"
#include "mino/schema/descriptor.h"
#include "mino/transport/transport_switcher.h"

namespace mino {

// Canonical payloads are copied or consumed synchronously at the seams below.
// Implementations that retain a payload after a call returns must make their own
// bounded copy. The bound matches the default Bridge wire payload limit.
inline constexpr size_t kMaxBusCanonicalPayloadBytes = 16u * 1024u * 1024u;

struct PublisherParticipantIdentity {
    PublisherId publisher_id;
    uint64_t generation = 0;
};

struct SubscriberParticipantIdentity {
    SubscriberId subscriber_id;
    uint64_t generation = 0;
};

class ParticipantIdAllocator {
public:
    virtual ~ParticipantIdAllocator() = default;
    virtual Result<PublisherParticipantIdentity> AllocatePublisher() = 0;
    virtual Result<SubscriberParticipantIdentity> AllocateSubscriber() = 0;
};

// Identity and sequence are assigned by the local publisher seam. Bus validates
// that the returned source is the participant registered with Coordinator.
struct LocalPublication {
    bridge::SourceIdentity source;
    uint64_t sequence_num = 0;
    uint64_t timestamp_ns = 0;
    uint32_t message_type = 0;
};

struct CanonicalMessage {
    schema::SchemaIdentity schema{0, {}, 0, 0};
    LocalPublication publication;
    uint8_t priority = 0;
    std::vector<std::byte> payload;
};

class BusLocalPublisherEndpoint {
public:
    virtual ~BusLocalPublisherEndpoint() = default;
    virtual Result<LocalPublication> Publish(
        std::span<const std::byte> canonical_payload, uint8_t priority) = 0;
};

class BusLocalSubscriberEndpoint {
public:
    virtual ~BusLocalSubscriberEndpoint() = default;
    virtual Result<CanonicalMessage> TryPoll() = 0;
};

// Identifies the exact resource generation opened by a local endpoint. The
// opaque publication binding owns the Region, Channel, and allocator closure.
struct BusLocalResourceBinding {
    TopicId topic_id;
    uint64_t region_version = 0;
    uint64_t channel_version = 0;
    uint64_t acl_version = 0;
    std::shared_ptr<const transport::LocalPublicationBinding> publication;
};

struct BusLocalPublisherResources {
    BusLocalResourceBinding binding;
    std::shared_ptr<BusLocalPublisherEndpoint> endpoint;
};

struct BusLocalSubscriberResources {
    BusLocalResourceBinding binding;
    std::shared_ptr<BusLocalSubscriberEndpoint> endpoint;
};

class BusLocalEndpointProvider {
public:
    virtual ~BusLocalEndpointProvider() = default;

    // These calls happen after participant registration. Any error, exception,
    // null endpoint, or mismatched resource generation is rolled back exactly.
    virtual Result<BusLocalPublisherResources> OpenPublisher(
        const registry::TopicMetadata& topic,
        const registry::PublisherRegistration& registration) = 0;
    virtual Result<BusLocalSubscriberResources> OpenSubscriber(
        const registry::TopicMetadata& topic,
        const registry::SubscriberRegistration& registration) = 0;
};

struct BridgeDispatchRequest {
    TopicId topic_id;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    LocalPublication publication;
    uint8_t priority = 0;
    std::span<const std::byte> canonical_payload;
    std::shared_ptr<const transport::RouteHandle> route;
};

class BridgeDispatcher {
public:
    virtual ~BridgeDispatcher() = default;

    // Dispatch is synchronous with respect to request and canonical_payload
    // lifetime. An asynchronous implementation must retain route and copy the
    // payload before returning.
    virtual Status Dispatch(const BridgeDispatchRequest& request) = 0;
};

struct BusPublishResult {
    LocalPublication publication;
    std::shared_ptr<const transport::RouteHandle> route;
    // Once publication succeeds locally, dispatch failures are reported here
    // while Publish still returns this value. Callers must not retry the local
    // publication based solely on a failed bridge dispatch.
    Status bridge_status = Status::Ok();
};

// Deterministic transient failure injection for Bus endpoint lifecycle tests.
// Production code normally leaves this unset.
class BusLifecycleFaultInjector final {
public:
    void FailPublisherUnregisterAttempts(size_t attempts) noexcept;
    void FailSubscriberUnregisterAttempts(size_t attempts) noexcept;
    void FailPublisherSlotReservationAttempts(size_t attempts) noexcept;
    void FailSubscriberSlotReservationAttempts(size_t attempts) noexcept;

private:
    friend class BusSharedContext;
    friend class DeferredDebtAnchor;
    bool ShouldFailPublisherUnregister() noexcept;
    bool ShouldFailSubscriberUnregister() noexcept;
    bool ShouldFailPublisherSlotReservation() noexcept;
    bool ShouldFailSubscriberSlotReservation() noexcept;

    std::atomic<size_t> publisher_unregister_failures_{0};
    std::atomic<size_t> subscriber_unregister_failures_{0};
    std::atomic<size_t> publisher_slot_reservation_failures_{0};
    std::atomic<size_t> subscriber_slot_reservation_failures_{0};
};

class BusSharedContext;
class Bus;

class BusPublisher final {
public:
    BusPublisher(const BusPublisher&) = delete;
    BusPublisher& operator=(const BusPublisher&) = delete;
    BusPublisher(BusPublisher&& other) noexcept;
    BusPublisher& operator=(BusPublisher&& other) noexcept;
    ~BusPublisher();

    Result<BusPublishResult> Publish(
        std::span<const std::byte> canonical_payload,
        uint8_t priority = 0);
    Status Close();

    TopicId topic_id() const noexcept { return registration_.topic_id; }
    PublisherId publisher_id() const noexcept {
        return registration_.publisher_id;
    }
    bool active() const noexcept { return endpoint_ != nullptr; }

private:
    friend class Bus;
    BusPublisher(std::shared_ptr<BusSharedContext> context,
                 registry::PublisherRegistration registration,
                 registry::DeliveryPolicy delivery,
                 schema::SchemaIdentity schema,
                 BusLocalPublisherResources resources) noexcept;
    void Reset() noexcept;

    std::shared_ptr<BusSharedContext> context_;
    registry::PublisherRegistration registration_;
    registry::DeliveryPolicy delivery_;
    schema::SchemaIdentity schema_{0, {}, 0, 0};
    std::shared_ptr<const transport::LocalPublicationBinding> binding_;
    std::shared_ptr<BusLocalPublisherEndpoint> endpoint_;
    bool owns_registration_slot_ = true;
    bool registration_deferred_ = false;
};

class BusSubscriber final {
public:
    BusSubscriber(const BusSubscriber&) = delete;
    BusSubscriber& operator=(const BusSubscriber&) = delete;
    BusSubscriber(BusSubscriber&& other) noexcept;
    BusSubscriber& operator=(BusSubscriber&& other) noexcept;
    ~BusSubscriber();

    Result<CanonicalMessage> TryPoll();
    Status Close();

    TopicId topic_id() const noexcept { return registration_.topic_id; }
    SubscriberId subscriber_id() const noexcept {
        return registration_.subscriber_id;
    }
    bool active() const noexcept { return endpoint_ != nullptr; }

private:
    friend class Bus;
    BusSubscriber(std::shared_ptr<BusSharedContext> context,
                  registry::SubscriberRegistration registration,
                  schema::SchemaIdentity schema,
                  BusLocalSubscriberResources resources) noexcept;
    void Reset() noexcept;

    std::shared_ptr<BusSharedContext> context_;
    registry::SubscriberRegistration registration_;
    schema::SchemaIdentity schema_{0, {}, 0, 0};
    std::shared_ptr<const transport::LocalPublicationBinding> binding_;
    std::shared_ptr<BusLocalSubscriberEndpoint> endpoint_;
    bool owns_registration_slot_ = true;
    bool registration_deferred_ = false;
};

class Bus final {
public:
    static Result<std::unique_ptr<Bus>> Create(
        registry::NodeLeaseOwner local_owner,
        std::shared_ptr<registry::Coordinator> coordinator,
        std::shared_ptr<transport::TransportSwitcher> transport_switcher,
        std::shared_ptr<ParticipantIdAllocator> participant_ids,
        std::shared_ptr<BusLocalEndpointProvider> local_endpoints,
        std::shared_ptr<BridgeDispatcher> bridge_dispatcher,
        std::shared_ptr<BusLifecycleFaultInjector> fault_injector = {});

    Bus(const Bus&) = delete;
    Bus& operator=(const Bus&) = delete;
    ~Bus();

    Result<BusPublisher> CreatePublisher(
        TopicId topic_id, const schema::SchemaIdentity& schema);
    Result<BusPublisher> CreatePublisher(
        std::string_view topic_name, const schema::SchemaIdentity& schema);
    Result<BusSubscriber> CreateSubscriber(
        TopicId topic_id, const schema::SchemaIdentity& schema);
    Result<BusSubscriber> CreateSubscriber(
        std::string_view topic_name, const schema::SchemaIdentity& schema);

private:
    explicit Bus(std::shared_ptr<BusSharedContext> context) noexcept;

    Result<BusPublisher> CreatePublisherFromSnapshot(
        std::shared_ptr<const registry::TopicSnapshot> topic,
        const schema::SchemaIdentity& schema);
    Result<BusSubscriber> CreateSubscriberFromSnapshot(
        std::shared_ptr<const registry::TopicSnapshot> topic,
        const schema::SchemaIdentity& schema);

    std::shared_ptr<BusSharedContext> context_;
};

}  // namespace mino

#endif  // MINO_RUNTIME_BUS_H_
