// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_TRANSPORT_TRANSPORT_SWITCHER_H_
#define MINO_TRANSPORT_TRANSPORT_SWITCHER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "mino/common/result.h"
#include "mino/registry/coordinator.h"
#include "mino/registry/metadata.h"
#include "mino/schema/descriptor.h"
#include "mino/transport/transport_driver.h"

namespace mino::transport {

inline constexpr size_t kMaxSwitcherDrivers = 256;
inline constexpr size_t kMaxSwitcherCachedTopics = 65'536;

class RouteAccessValidator {
public:
    virtual ~RouteAccessValidator() = default;
    virtual uint64_t version() const noexcept = 0;
    virtual Status Validate(const registry::TopicMetadata& topic,
                            NodeId source_node,
                            NodeId target_node) const = 0;
};

class SchemaRouteValidator {
public:
    virtual ~SchemaRouteValidator() = default;
    virtual uint64_t version() const noexcept = 0;
    virtual Status Validate(const registry::TopicMetadata& topic,
                            NodeId target_node,
                            const schema::SchemaIdentity& publisher_schema) const = 0;
};

// Owns the complete local publication resource closure (Region, Channel and any
// allocator/facade state). A RouteHandle retaining this object keeps those
// resources alive for an already-frozen publication.
class LocalPublicationBinding {
public:
    virtual ~LocalPublicationBinding() = default;
};

class LocalRouteProvider {
public:
    virtual ~LocalRouteProvider() = default;
    virtual uint64_t version() const noexcept = 0;
    virtual Result<std::shared_ptr<const LocalPublicationBinding>> Resolve(
        const registry::TopicMetadata& topic) const = 0;
};

class EndpointMatcher {
public:
    virtual ~EndpointMatcher() = default;
    virtual bool Supports(const EndpointDescriptor& endpoint) const noexcept = 0;
};

// Basic matcher for deployments with one implementation per transport kind.
class TransportKindEndpointMatcher final : public EndpointMatcher {
public:
    explicit TransportKindEndpointMatcher(TransportKind kind) noexcept
        : kind_(kind) {}
    bool Supports(const EndpointDescriptor& endpoint) const noexcept override {
        return endpoint.kind() == kind_;
    }

private:
    TransportKind kind_;
};

class NetworkProtocolEndpointMatcher final : public EndpointMatcher {
public:
    explicit NetworkProtocolEndpointMatcher(NetworkProtocol protocol) noexcept
        : protocol_(protocol) {}
    bool Supports(const EndpointDescriptor& endpoint) const noexcept override {
        return endpoint.kind() == TransportKind::kNetwork &&
               endpoint.protocol() == protocol_;
    }

private:
    NetworkProtocol protocol_;
};

struct DriverRegistration {
    uint64_t driver_id = 0;
    uint64_t generation = 0;
    uint32_t selection_priority = 0;
    std::shared_ptr<TransportDriver> driver;
    std::shared_ptr<const EndpointMatcher> endpoint_matcher;
};

struct RouteRequest {
    TopicId topic_id;
    uint32_t payload_size = 0;
    registry::DeliveryPolicy delivery;
    uint8_t priority = 0;
    schema::SchemaIdentity publisher_schema{0, {}, 0, 0};
};

struct RouteStamp {
    TopicId topic_id;
    registry::RoutePolicy policy = registry::RoutePolicy::kDiscovery;
    uint64_t topic_config_version = 0;
    uint64_t route_version = 0;
    uint64_t node_registry_version = 0;
    uint64_t driver_registry_version = 0;
    uint64_t acl_validator_version = 0;
    uint64_t schema_validator_version = 0;
    uint64_t local_provider_version = 0;

    friend bool operator==(const RouteStamp&, const RouteStamp&) = default;
};

struct LocalTargetRoute {
    std::shared_ptr<const LocalPublicationBinding> binding;
};

struct RemoteTargetRoute {
    EndpointDescriptor endpoint;
    uint64_t node_config_version = 0;
    ProcessIdentity process_identity;
    uint64_t lease_epoch = 0;
    uint64_t driver_id = 0;
    uint64_t driver_generation = 0;
    TransportCapabilities capabilities;
    std::shared_ptr<TransportDriver> driver;
};

using TargetTransport = std::variant<LocalTargetRoute, RemoteTargetRoute>;

struct TargetRoute {
    NodeId target_node;
    TargetTransport transport;
};

// Immutable per-publication target set. Existing holders may finish work after
// a cache refresh; callers must Resolve() again before starting a new publish.
class RouteHandle final {
public:
    const RouteStamp& stamp() const noexcept { return stamp_; }
    const registry::DeliveryPolicy& delivery() const noexcept {
        return delivery_;
    }
    uint32_t payload_size() const noexcept { return payload_size_; }
    uint8_t priority() const noexcept { return priority_; }
    std::span<const TargetRoute> targets() const noexcept { return targets_; }

private:
    friend class TransportSwitcher;
    RouteStamp stamp_;
    registry::DeliveryPolicy delivery_;
    uint32_t payload_size_ = 0;
    uint8_t priority_ = 0;
    std::vector<TargetRoute> targets_;
};

class TransportSwitcher final {
public:
    static Result<std::unique_ptr<TransportSwitcher>> Create(
        NodeId local_node, const registry::Coordinator* coordinator,
        std::shared_ptr<const RouteAccessValidator> access_validator,
        std::shared_ptr<const SchemaRouteValidator> schema_validator,
        std::shared_ptr<const LocalRouteProvider> local_provider,
        size_t max_cached_topics = 4096,
        size_t max_drivers = 64);

    ~TransportSwitcher();
    TransportSwitcher(const TransportSwitcher&) = delete;
    TransportSwitcher& operator=(const TransportSwitcher&) = delete;

    Status RegisterDriver(DriverRegistration registration);
    Status UnregisterDriver(uint64_t driver_id, uint64_t generation);

    // Heavy control-plane operation. Registry/validator/provider callbacks run
    // without the cache mutex. A failed latest refresh invalidates the old plan.
    Status RefreshTopic(TopicId topic_id);
    Status InvalidateTopic(TopicId topic_id);

    // Hot-path operation over an immutable cached plan. Driver state, health,
    // capabilities and payload limits are rechecked because they are dynamic.
    Result<std::shared_ptr<const RouteHandle>> Resolve(
        const RouteRequest& request) const;

private:
    class Impl;
    explicit TransportSwitcher(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::transport

#endif  // MINO_TRANSPORT_TRANSPORT_SWITCHER_H_
