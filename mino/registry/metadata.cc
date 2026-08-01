// Copyright 2026 The Mino Authors

#include "mino/registry/metadata.h"

#include <algorithm>
#include <cctype>
#include <limits>

namespace mino::registry {
namespace {

Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool IsBoundedName(const std::string& value, size_t max_size,
                   bool allow_slash) noexcept {
    if (value.empty() || value.size() > max_size || value.front() == '/' ||
        value.back() == '/') {
        return false;
    }
    bool previous_slash = false;
    for (const unsigned char character : value) {
        const bool slash = character == '/';
        if (!(std::isalnum(character) != 0 || character == '.' ||
              character == '_' || character == '-' ||
              (allow_slash && slash))) {
            return false;
        }
        if (slash && previous_slash) {
            return false;
        }
        previous_slash = slash;
    }
    return std::isalnum(static_cast<unsigned char>(value.front())) != 0;
}

bool ValidTransportKind(transport::TransportKind kind) noexcept {
    switch (kind) {
        case transport::TransportKind::kNetwork:
        case transport::TransportKind::kRdma:
        case transport::TransportKind::kSharedFabric:
            return true;
    }
    return false;
}

bool ValidNodeHealth(NodeHealth health) noexcept {
    switch (health) {
        case NodeHealth::kHealthy:
        case NodeHealth::kDegraded:
        case NodeHealth::kUnavailable:
            return true;
    }
    return false;
}

bool ValidChannelKind(ChannelKind kind) noexcept {
    switch (kind) {
        case ChannelKind::kSpsc:
        case ChannelKind::kMpsc:
        case ChannelKind::kBroadcast:
        case ChannelKind::kWorkQueue:
            return true;
    }
    return false;
}

bool ValidQueueFullPolicy(QueueFullPolicy policy) noexcept {
    switch (policy) {
        case QueueFullPolicy::kBlock:
        case QueueFullPolicy::kFail:
        case QueueFullPolicy::kDropNewest:
        case QueueFullPolicy::kDropOldest:
        case QueueFullPolicy::kSample:
            return true;
    }
    return false;
}

bool IsDroppingPolicy(QueueFullPolicy policy) noexcept {
    return policy == QueueFullPolicy::kDropNewest ||
           policy == QueueFullPolicy::kDropOldest ||
           policy == QueueFullPolicy::kSample;
}

bool ValidReliability(Reliability reliability) noexcept {
    switch (reliability) {
        case Reliability::kBestEffort:
        case Reliability::kReliableOrdered:
            return true;
    }
    return false;
}

bool ValidRoutePolicy(RoutePolicy policy) noexcept {
    switch (policy) {
        case RoutePolicy::kDiscovery:
        case RoutePolicy::kStatic:
            return true;
    }
    return false;
}

bool ValidRecordTopology(RecordBackpressureTopology topology) noexcept {
    switch (topology) {
        case RecordBackpressureTopology::kStrongConsistent:
        case RecordBackpressureTopology::kIsolated:
        case RecordBackpressureTopology::kBestEffort:
            return true;
    }
    return false;
}

bool ValidTopicState(TopicState state) noexcept {
    switch (state) {
        case TopicState::kCreating:
        case TopicState::kActive:
        case TopicState::kDraining:
        case TopicState::kRetired:
        case TopicState::kDeleted:
            return true;
    }
    return false;
}

bool IsPowerOfTwo(uint32_t value) noexcept {
    return value != 0 && (value & (value - 1)) == 0;
}

bool HasNonzeroDigest(const schema::CanonicalDigest& digest) noexcept {
    return std::any_of(digest.begin(), digest.end(),
                       [](std::byte value) { return value != std::byte{0}; });
}

}  // namespace

Status ValidateCoordinatorLimits(const CoordinatorLimits& limits) {
    if (limits.max_nodes == 0 || limits.max_nodes > kMaxNodeCount ||
        limits.max_topics == 0 || limits.max_topics > kMaxTopicCount ||
        limits.max_endpoints_per_node == 0 ||
        limits.max_endpoints_per_node > kMaxEndpointsPerNode ||
        limits.max_static_routes_per_topic == 0 ||
        limits.max_static_routes_per_topic > kMaxStaticRoutes ||
        limits.max_publisher_registrations == 0 ||
        limits.max_publisher_registrations > kMaxParticipantRegistrations ||
        limits.max_subscriber_registrations == 0 ||
        limits.max_subscriber_registrations > kMaxParticipantRegistrations ||
        limits.max_topic_pins == 0 || limits.max_topic_pins > kMaxTopicPins) {
        return Invalid("coordinator limits are zero or exceed hard bounds");
    }
    return Status::Ok();
}

Status ValidateNodeRegistration(const NodeRegistration& registration,
                                const CoordinatorLimits& limits) {
    MINO_RETURN_IF_ERROR(ValidateCoordinatorLimits(limits));
    if (registration.node_id.value == 0 ||
        registration.process_identity.IsZero() ||
        registration.process_identity.node_id != registration.node_id.value) {
        return Invalid("node and exact process identity must be nonzero and agree");
    }
    if (registration.endpoints.empty()) {
        return Invalid("node must advertise at least one endpoint");
    }
    if (registration.endpoints.size() > limits.max_endpoints_per_node) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "node endpoint count exceeds configured bound");
    }
    for (size_t i = 0; i < registration.endpoints.size(); ++i) {
        MINO_RETURN_IF_ERROR(
            transport::ValidateEndpointDescriptor(registration.endpoints[i]));
        for (size_t j = 0; j < i; ++j) {
            if (registration.endpoints[i] == registration.endpoints[j]) {
                return Invalid("node contains a duplicate endpoint");
            }
        }
    }
    if (!IsBoundedName(registration.trust_domain, kMaxTrustDomainBytes,
                       false)) {
        return Invalid("trust domain is empty, oversized, or malformed");
    }
    if (!ValidNodeHealth(registration.health) || registration.lease_epoch == 0 ||
        registration.lease_duration_ns == 0 ||
        registration.lease_duration_ns > kMaxLeaseDurationNs ||
        registration.config_version == 0) {
        return Invalid("node health, lease, or config version is invalid");
    }
    return Status::Ok();
}

bool SchemaIdentityEqual(const schema::SchemaIdentity& lhs,
                         const schema::SchemaIdentity& rhs) noexcept {
    return lhs.short_id() == rhs.short_id() &&
           lhs.canonical_digest() == rhs.canonical_digest() &&
           lhs.schema_version() == rhs.schema_version() &&
           lhs.layout_version() == rhs.layout_version();
}

Status ValidateTopicMetadata(const TopicMetadata& metadata,
                             const CoordinatorLimits& limits,
                             bool creating_candidate) {
    MINO_RETURN_IF_ERROR(ValidateCoordinatorLimits(limits));
    if (!IsBoundedName(metadata.name, kMaxTopicNameBytes, true)) {
        return Invalid("topic name is empty, oversized, or malformed");
    }
    if (!ValidChannelKind(metadata.channel_kind) ||
        !ValidReliability(metadata.delivery.reliability) ||
        !ValidQueueFullPolicy(metadata.queue_full_policy) ||
        !ValidRoutePolicy(metadata.route_policy) ||
        !ValidRecordTopology(metadata.record_topology) ||
        !ValidTopicState(metadata.state)) {
        return Invalid("topic contains an unknown enum value");
    }
    if (!IsPowerOfTwo(metadata.capacity) ||
        metadata.capacity > kMaxTopicCapacity) {
        return Invalid("topic capacity must be a bounded power of two");
    }
    if (metadata.max_publishers == 0 ||
        metadata.max_publishers > kMaxTopicPublishers ||
        metadata.max_subscribers == 0 ||
        metadata.max_subscribers > kMaxTopicSubscribers ||
        metadata.partition_count == 0 ||
        metadata.partition_count > kMaxTopicPartitions) {
        return Invalid("topic publisher, subscriber, or partition bound is invalid");
    }
    if (metadata.channel_kind == ChannelKind::kSpsc &&
        (metadata.max_publishers != 1 || metadata.max_subscribers != 1)) {
        return Invalid("SPSC requires exactly one publisher and one subscriber");
    }
    if (metadata.channel_kind == ChannelKind::kMpsc &&
        metadata.max_subscribers != 1) {
        return Invalid("MPSC requires exactly one subscriber");
    }
    if (metadata.channel_kind == ChannelKind::kBroadcast &&
        metadata.max_subscribers > kMaxBroadcastSubscribers) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "broadcast subscriber count exceeds ACK bitmap");
    }
    if (metadata.channel_kind == ChannelKind::kWorkQueue &&
        metadata.queue_full_policy == QueueFullPolicy::kSample) {
        return Invalid("work queue does not support sampling admission");
    }
    if ((!metadata.delivery.allow_drop &&
         IsDroppingPolicy(metadata.queue_full_policy)) ||
        (metadata.delivery.reliability == Reliability::kReliableOrdered &&
         metadata.delivery.allow_drop)) {
        return Invalid("delivery and queue-full policies are contradictory");
    }
    if (metadata.schema.short_id() == 0 ||
        metadata.schema.schema_version() == 0 ||
        metadata.schema.layout_version() == 0 ||
        !HasNonzeroDigest(metadata.schema.canonical_digest())) {
        return Invalid("schema identity is incomplete");
    }
    if (metadata.route_policy == RoutePolicy::kDiscovery) {
        if (!metadata.static_routes.empty()) {
            return Invalid("discovery routing must not contain static routes");
        }
    } else {
        if (metadata.static_routes.empty()) {
            return Invalid("static routing requires at least one target node");
        }
        if (metadata.static_routes.size() >
            limits.max_static_routes_per_topic) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "static route count exceeds configured bound");
        }
        for (size_t i = 0; i < metadata.static_routes.size(); ++i) {
            const StaticRouteEntry& route = metadata.static_routes[i];
            if (route.target_node.value == 0 ||
                (route.preferred_transport.has_value() &&
                 !ValidTransportKind(*route.preferred_transport))) {
                return Invalid("static route target or transport is invalid");
            }
            for (size_t j = 0; j < i; ++j) {
                if (route.target_node == metadata.static_routes[j].target_node) {
                    return Invalid("static route target nodes must be unique");
                }
            }
        }
    }
    if (metadata.region_version == 0 || metadata.channel_version == 0 ||
        metadata.acl_version == 0) {
        return Invalid("topic resource versions must be nonzero");
    }
    if (creating_candidate) {
        if (metadata.topic_id.value != 0 || metadata.config_version != 0 ||
            metadata.route_set_version != 0 ||
            metadata.state != TopicState::kCreating) {
            return Invalid("new topic IDs and coordinator versions are coordinator-assigned");
        }
    } else if (metadata.topic_id.value == 0 || metadata.config_version == 0 ||
               metadata.route_set_version == 0) {
        return Invalid("published topic metadata has an invalid ID or version");
    }
    return Status::Ok();
}

}  // namespace mino::registry
