// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_REGISTRY_METADATA_H_
#define MINO_REGISTRY_METADATA_H_

#include <compare>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/schema/descriptor.h"
#include "mino/security/security_domain.h"
#include "mino/shm/channel/queue_full_policy.h"
#include "mino/transport/transport_driver.h"

namespace mino::registry {

// Hard API bounds. CoordinatorLimits may lower these values, never raise them.
inline constexpr size_t kMaxNodeCount = 4096;
inline constexpr size_t kMaxEndpointsPerNode = 16;
inline constexpr size_t kMaxTrustDomainBytes = 255;
inline constexpr size_t kMaxTopicCount = 65'536;
inline constexpr size_t kMaxTopicNameBytes = 255;
inline constexpr size_t kMaxStaticRoutes = 4096;
inline constexpr size_t kMaxTopicAclEntries = 4096;
inline constexpr size_t kMaxAcceptedSchemasPerTopic = 16;
inline constexpr uint32_t kMaxTopicCapacity = 1u << 30;
inline constexpr uint32_t kMaxTopicPublishers = 65'536;
inline constexpr uint32_t kMaxTopicSubscribers = 65'536;
inline constexpr uint32_t kMaxBroadcastSubscribers = 64;
inline constexpr uint32_t kMaxTopicPartitions = 4096;
inline constexpr size_t kMaxParticipantRegistrations = 1u << 20;
inline constexpr size_t kMaxTopicPins = 1u << 20;
inline constexpr uint64_t kMaxLeaseDurationNs = 24ull * 60 * 60 * 1'000'000'000;

struct CoordinatorLimits {
    size_t max_nodes = 1024;
    size_t max_topics = 4096;
    size_t max_endpoints_per_node = 8;
    size_t max_static_routes_per_topic = 1024;
    size_t max_publisher_registrations = 65'536;
    size_t max_subscriber_registrations = 65'536;
    size_t max_topic_pins = 65'536;
};

enum class NodeHealth : uint8_t {
    kHealthy = 0,
    kDegraded = 1,
    kUnavailable = 2,
};

enum class NodeLeaseState : uint8_t {
    kActive = 0,
    // Expired records are retained until the exact ProcessIdentity is known
    // dead. In particular, kUnknown liveness never authorizes eviction.
    kExpired = 1,
};

struct NodeRegistration {
    NodeId node_id;
    ProcessIdentity process_identity;
    std::vector<transport::EndpointDescriptor> endpoints;
    SecurityDomainId security_domain_id;
    std::string trust_domain;
    NodeHealth health = NodeHealth::kHealthy;
    uint64_t lease_epoch = 0;
    uint64_t lease_duration_ns = 0;
    uint64_t config_version = 0;
};

struct NodeMetadata {
    NodeId node_id;
    ProcessIdentity process_identity;
    std::vector<transport::EndpointDescriptor> endpoints;
    SecurityDomainId security_domain_id;
    std::string trust_domain;
    NodeHealth health = NodeHealth::kHealthy;
    NodeLeaseState lease_state = NodeLeaseState::kActive;
    ProcessIdentityLiveness liveness = ProcessIdentityLiveness::kAlive;
    uint64_t lease_epoch = 0;
    uint64_t lease_duration_ns = 0;
    uint64_t last_heartbeat_ns = 0;
    uint64_t lease_deadline_ns = 0;
    uint64_t config_version = 0;
};

struct NodeRegistrySnapshot {
    uint64_t version = 0;
    std::vector<NodeMetadata> nodes;
};

struct NodeLeaseOwner {
    NodeId node_id;
    ProcessIdentity process_identity;
    uint64_t lease_epoch = 0;

    friend bool operator==(const NodeLeaseOwner&, const NodeLeaseOwner&) = default;
};

enum class ChannelKind : uint8_t {
    kSpsc = 0,
    kMpsc = 1,
    kBroadcast = 2,
    kWorkQueue = 3,
};

enum class RoutePolicy : uint8_t {
    kDiscovery = 0,
    kStatic = 1,
};

enum class Reliability : uint8_t {
    kBestEffort = 0,
    kReliableOrdered = 1,
};

struct DeliveryPolicy {
    Reliability reliability = Reliability::kBestEffort;
    bool allow_drop = false;

    friend bool operator==(const DeliveryPolicy&, const DeliveryPolicy&) = default;
};

enum class RecordBackpressureTopology : uint8_t {
    kStrongConsistent = 0,
    kIsolated = 1,
    kBestEffort = 2,
};

enum class TopicState : uint8_t {
    kCreating = 0,
    kActive = 1,
    kDraining = 2,
    kRetired = 3,
    kDeleted = 4,
};

struct StaticRouteEntry {
    NodeId target_node;
    std::optional<transport::TransportKind> preferred_transport;

    friend bool operator==(const StaticRouteEntry&, const StaticRouteEntry&) =
        default;
};

enum class TopicPermission : uint32_t {
    kPublish = 1u << 0,
    kSubscribe = 1u << 1,
    kBridge = 1u << 2,
    kRecord = 1u << 3,
    kReplay = 1u << 4,
};

inline constexpr uint32_t kAllTopicPermissions =
    static_cast<uint32_t>(TopicPermission::kPublish) |
    static_cast<uint32_t>(TopicPermission::kSubscribe) |
    static_cast<uint32_t>(TopicPermission::kBridge) |
    static_cast<uint32_t>(TopicPermission::kRecord) |
    static_cast<uint32_t>(TopicPermission::kReplay);

struct TopicAclEntry {
    NodeId node_id;
    SecurityDomainId security_domain_id;
    uint32_t permissions = 0;

    friend bool operator==(const TopicAclEntry&, const TopicAclEntry&) = default;
};

struct TopicAcl {
    // No wildcard and no implicit same-domain grant: an empty or missing entry
    // denies access. This is intentional fail-closed behavior.
    std::vector<TopicAclEntry> entries;

    friend bool operator==(const TopicAcl&, const TopicAcl&) = default;
};

struct TopicMetadata {
    TopicId topic_id;
    std::string name;
    ChannelKind channel_kind = ChannelKind::kSpsc;
    DeliveryPolicy delivery;
    QueueFullPolicy queue_full_policy = QueueFullPolicy::kBlock;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    // Explicitly accepted reader/wire versions in addition to the primary
    // publish schema. Empty means only schema is accepted.
    std::vector<schema::SchemaIdentity> accepted_schemas;
    RoutePolicy route_policy = RoutePolicy::kDiscovery;
    std::vector<StaticRouteEntry> static_routes;
    uint64_t route_set_version = 0;
    uint32_t capacity = 0;
    uint32_t max_publishers = 0;
    uint32_t max_subscribers = 0;
    uint32_t partition_count = 1;
    RecordBackpressureTopology record_topology =
        RecordBackpressureTopology::kIsolated;
    TopicAcl acl;
    // Versions of the concrete resources to which lifecycle proofs bind.
    uint64_t region_version = 0;
    uint64_t channel_version = 0;
    uint64_t acl_version = 0;
    uint64_t config_version = 0;
    TopicState state = TopicState::kCreating;
};

struct TopicUsageCounts {
    uint32_t publishers = 0;
    uint32_t subscribers = 0;
    uint32_t bridges = 0;
    uint32_t recorders = 0;
    uint32_t replay_pins = 0;

    bool empty() const noexcept {
        return publishers == 0 && subscribers == 0 && bridges == 0 &&
               recorders == 0 && replay_pins == 0;
    }
};

struct TopicSnapshot {
    TopicMetadata metadata;
    TopicUsageCounts usage;
};

struct ActivationReadinessProof {
    TopicId topic_id;
    uint64_t config_version = 0;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    uint64_t region_version = 0;
    uint64_t channel_version = 0;
    uint64_t acl_version = 0;
    bool schema_ready = false;
    bool region_ready = false;
    bool channel_ready = false;
    bool acl_ready = false;

    bool complete() const noexcept {
        return schema_ready && region_ready && channel_ready && acl_ready;
    }
};

struct DrainCompletionProof {
    TopicId topic_id;
    uint64_t config_version = 0;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    uint64_t region_version = 0;
    uint64_t channel_version = 0;
    uint64_t acl_version = 0;
    bool channel_drained = false;
    bool borrows_released = false;

    bool complete() const noexcept {
        return channel_drained && borrows_released;
    }
};

struct PublisherRegistration {
    TopicId topic_id;
    PublisherId publisher_id;
    uint64_t generation = 0;
    NodeLeaseOwner owner;
};

struct SubscriberRegistration {
    TopicId topic_id;
    SubscriberId subscriber_id;
    uint64_t generation = 0;
    NodeLeaseOwner owner;
};

struct SubscriberNodeSetSnapshot {
    TopicId topic_id;
    uint64_t version = 0;
    std::vector<NodeId> nodes;
};

struct RouteSnapshot {
    TopicId topic_id;
    RoutePolicy policy = RoutePolicy::kDiscovery;
    uint64_t version = 0;
    std::vector<StaticRouteEntry> routes;
};

// Topic metadata and its policy-specific target set captured under one
// Coordinator lock. Consumers must use this combined view rather than joining
// separate GetTopic()/ResolveRoutes() results across a policy update.
struct RoutingSnapshot {
    std::shared_ptr<const TopicSnapshot> topic;
    std::shared_ptr<const RouteSnapshot> routes;
};

enum class TopicPinKind : uint8_t {
    kBridge = 0,
    kRecorder = 1,
    kReplay = 2,
};

struct TopicPinId {
    uint64_t value = 0;
    friend constexpr auto operator<=>(const TopicPinId&, const TopicPinId&) =
        default;
};

struct TopicPinRegistration {
    TopicId topic_id;
    TopicPinId pin_id;
    TopicPinKind kind = TopicPinKind::kBridge;
    uint64_t generation = 0;
    NodeLeaseOwner owner;

    friend bool operator==(const TopicPinRegistration&,
                           const TopicPinRegistration&) = default;
};

Status ValidateCoordinatorLimits(const CoordinatorLimits& limits);
Status ValidateNodeRegistration(const NodeRegistration& registration,
                                const CoordinatorLimits& limits);
Status ValidateTopicMetadata(const TopicMetadata& metadata,
                             const CoordinatorLimits& limits,
                             bool creating_candidate = false);
Status ValidateTopicPermission(const TopicMetadata& metadata,
                               SecurityDomainId security_domain_id,
                               NodeId node_id, TopicPermission permission);
bool SchemaIdentityEqual(const schema::SchemaIdentity& lhs,
                         const schema::SchemaIdentity& rhs) noexcept;

}  // namespace mino::registry

#endif  // MINO_REGISTRY_METADATA_H_
