// Copyright 2026 The Mino Authors

#include "mino/registry/coordinator.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

namespace mino::registry {
namespace {

Status AllocationFailure() {
    return Status::Error(StatusCode::kResourceExhausted);
}

bool SamePublisherRegistration(const PublisherRegistration& lhs,
                               const PublisherRegistration& rhs) noexcept {
    return lhs.topic_id == rhs.topic_id &&
           lhs.publisher_id == rhs.publisher_id &&
           lhs.generation == rhs.generation && lhs.owner == rhs.owner;
}

bool SameSubscriberRegistration(const SubscriberRegistration& lhs,
                                const SubscriberRegistration& rhs) noexcept {
    return lhs.topic_id == rhs.topic_id &&
           lhs.subscriber_id == rhs.subscriber_id &&
           lhs.generation == rhs.generation && lhs.owner == rhs.owner;
}

bool SameRoutes(const TopicMetadata& lhs, const TopicMetadata& rhs) noexcept {
    return lhs.route_policy == rhs.route_policy &&
           lhs.static_routes == rhs.static_routes;
}

bool ProofMatches(const TopicMetadata& metadata,
                  const ActivationReadinessProof& proof) noexcept {
    return proof.topic_id == metadata.topic_id &&
           proof.config_version == metadata.config_version &&
           SchemaIdentityEqual(proof.schema, metadata.schema) &&
           proof.region_version == metadata.region_version &&
           proof.channel_version == metadata.channel_version &&
           proof.acl_version == metadata.acl_version;
}

bool ProofMatches(const TopicMetadata& metadata,
                  const DrainCompletionProof& proof) noexcept {
    return proof.topic_id == metadata.topic_id &&
           proof.config_version == metadata.config_version &&
           SchemaIdentityEqual(proof.schema, metadata.schema) &&
           proof.region_version == metadata.region_version &&
           proof.channel_version == metadata.channel_version &&
           proof.acl_version == metadata.acl_version;
}

bool ValidPinKind(TopicPinKind kind) noexcept {
    return kind == TopicPinKind::kBridge || kind == TopicPinKind::kRecorder ||
           kind == TopicPinKind::kReplay;
}

}  // namespace

void RegistryFaultInjector::FailOwnerCleanupAllocationAfter(
    size_t successful_allocations) noexcept {
    cleanup_allocation_countdown_.store(successful_allocations + 1,
                                        std::memory_order_release);
}

void RegistryFaultInjector::MaybeFailOwnerCleanupAllocation() {
    size_t observed =
        cleanup_allocation_countdown_.load(std::memory_order_acquire);
    while (observed != 0) {
        if (cleanup_allocation_countdown_.compare_exchange_weak(
                observed, observed - 1, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            if (observed == 1) {
                throw std::bad_alloc();
            }
            return;
        }
    }
}

size_t Coordinator::PublisherKeyHash::operator()(
    const PublisherKey& key) const noexcept {
    const size_t first = std::hash<TopicId>{}(key.topic_id);
    const size_t second = std::hash<PublisherId>{}(key.publisher_id);
    return first ^ (second + 0x9e3779b9u + (first << 6) + (first >> 2));
}

size_t Coordinator::SubscriberKeyHash::operator()(
    const SubscriberKey& key) const noexcept {
    const size_t first = std::hash<TopicId>{}(key.topic_id);
    const size_t second = std::hash<SubscriberId>{}(key.subscriber_id);
    return first ^ (second + 0x9e3779b9u + (first << 6) + (first >> 2));
}

size_t Coordinator::TopicPinKeyHash::operator()(
    const TopicPinKey& key) const noexcept {
    const size_t first = std::hash<TopicId>{}(key.topic_id);
    const size_t second = std::hash<uint64_t>{}(key.pin_id.value);
    return first ^ (second + 0x9e3779b9u + (first << 6) + (first >> 2));
}

Result<std::unique_ptr<Coordinator>> Coordinator::Create(
    CoordinatorLimits limits, std::shared_ptr<IdAllocator> id_allocator,
    std::shared_ptr<const LivenessProbe> liveness_probe) {
    return CreateImpl(limits, std::move(id_allocator),
                      std::move(liveness_probe), {}, true);
}

Result<std::unique_ptr<Coordinator>> Coordinator::CreateForTesting(
    CoordinatorLimits limits, std::shared_ptr<IdAllocator> id_allocator,
    std::shared_ptr<const LivenessProbe> liveness_probe,
    std::shared_ptr<RegistryFaultInjector> fault_injector) {
    return CreateImpl(limits, std::move(id_allocator),
                      std::move(liveness_probe), std::move(fault_injector),
                      false);
}

Result<std::unique_ptr<Coordinator>> Coordinator::CreateImpl(
    CoordinatorLimits limits, std::shared_ptr<IdAllocator> id_allocator,
    std::shared_ptr<const LivenessProbe> liveness_probe,
    std::shared_ptr<RegistryFaultInjector> fault_injector,
    bool require_durable) {
    try {
        MINO_RETURN_IF_ERROR(ValidateCoordinatorLimits(limits));
        if (!id_allocator) {
            if (require_durable) {
                return Status::Error(
                    StatusCode::kInvalidArgument,
                    "production Coordinator requires an explicit durable IdAllocator");
            }
            MINO_ASSIGN_OR_RETURN(id_allocator, DefaultProcessIdAllocator());
        }
        // This is a virtual call. It intentionally happens before any registry
        // object or registry lock exists.
        if (require_durable &&
            id_allocator->durability() != IdAllocatorDurability::kDurable) {
            return Status::Error(
                StatusCode::kUnsupported,
                "production Coordinator requires a durable IdAllocator");
        }
        MINO_ASSIGN_OR_RETURN(
            std::unique_ptr<NodeRegistry> nodes,
            NodeRegistry::Create(limits, std::move(liveness_probe)));
        return std::unique_ptr<Coordinator>(new Coordinator(
            limits, std::move(id_allocator), std::move(nodes),
            std::move(fault_injector)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Coordinator::Coordinator(
    CoordinatorLimits limits, std::shared_ptr<IdAllocator> allocator,
    std::unique_ptr<NodeRegistry> nodes,
    std::shared_ptr<RegistryFaultInjector> fault_injector)
    : limits_(limits),
      id_allocator_(std::move(allocator)),
      nodes_(std::move(nodes)),
      fault_injector_(std::move(fault_injector)) {
    topics_.reserve(limits_.max_topics);
    topic_names_.reserve(limits_.max_topics);
    publishers_.reserve(limits_.max_publisher_registrations);
    subscribers_.reserve(limits_.max_subscriber_registrations);
    pins_.reserve(limits_.max_topic_pins);
    pending_cleanup_owners_.reserve(
        limits_.max_publisher_registrations +
        limits_.max_subscriber_registrations + limits_.max_topic_pins);
}

Result<NodeRegistrationOutcome> Coordinator::RegisterNode(
    const NodeRegistration& request, uint64_t now_ns) {
    try {
        {
            std::lock_guard lock(mutex_);
            CoordinatorSweepResult ignored;
            MINO_RETURN_IF_ERROR(RetryPendingCleanupLocked(&ignored));
        }

        // NodeRegistry performs any virtual liveness probe without its own
        // mutex. The Coordinator mutex is also deliberately not held here.
        MINO_ASSIGN_OR_RETURN(NodeRegistrationOutcome outcome,
                              nodes_->Register(request, now_ns));

        std::lock_guard lock(mutex_);
        CoordinatorSweepResult ignored;
        if (outcome.displaced_owner.has_value()) {
            QueueOwnerCleanupLocked(*outcome.displaced_owner);
        }
        MINO_RETURN_IF_ERROR(RetryPendingCleanupLocked(&ignored));
        return outcome;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::HeartbeatNode(const NodeLeaseOwner& owner,
                                  NodeHealth health, uint64_t now_ns) {
    return nodes_->Heartbeat(owner, health, now_ns);
}

Status Coordinator::UpdateNode(const NodeRegistration& replacement,
                               uint64_t expected_config_version,
                               uint64_t now_ns) {
    return nodes_->Update(replacement, expected_config_version, now_ns);
}

Result<CoordinatorSweepResult> Coordinator::SweepExpiredNodes(uint64_t now_ns) {
    try {
        {
            std::lock_guard lock(mutex_);
            CoordinatorSweepResult ignored;
            MINO_RETURN_IF_ERROR(RetryPendingCleanupLocked(&ignored));
        }

        CoordinatorSweepResult result;
        // No Coordinator mutex is held while NodeRegistry calls the virtual
        // liveness probe.
        MINO_ASSIGN_OR_RETURN(result.nodes, nodes_->SweepExpired(now_ns));

        std::lock_guard lock(mutex_);
        for (const NodeLeaseOwner& owner : result.nodes.removed) {
            QueueOwnerCleanupLocked(owner);
        }
        MINO_RETURN_IF_ERROR(RetryPendingCleanupLocked(&result));
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::shared_ptr<const NodeMetadata>> Coordinator::GetNode(
    NodeId node_id) const {
    return nodes_->Get(node_id);
}

Result<std::shared_ptr<const NodeRegistrySnapshot>> Coordinator::NodeSnapshot()
    const {
    return nodes_->Snapshot();
}

Status Coordinator::ValidateStaticRouteNodesLocked(
    const TopicMetadata& metadata) const {
    if (metadata.route_policy != RoutePolicy::kStatic) {
        return Status::Ok();
    }
    for (const StaticRouteEntry& route : metadata.static_routes) {
        auto node = nodes_->Get(route.target_node);
        if (!node.ok()) {
            return Status::Error(StatusCode::kNotFound,
                                 "static route target is not registered");
        }
        if (route.preferred_transport.has_value()) {
            const bool found_transport = std::any_of(
                node.value()->endpoints.begin(), node.value()->endpoints.end(),
                [&route](const transport::EndpointDescriptor& endpoint) {
                    return endpoint.kind() == *route.preferred_transport;
                });
            if (!found_transport) {
                return Status::Error(
                    StatusCode::kUnsupported,
                    "static route preferred transport is unavailable on node");
            }
        }
    }
    return Status::Ok();
}

Result<std::shared_ptr<const TopicSnapshot>> Coordinator::CreateTopic(
    TopicMetadata candidate) {
    try {
        MINO_RETURN_IF_ERROR(
            ValidateTopicMetadata(candidate, limits_, true));
        {
            std::lock_guard lock(mutex_);
            if (topics_.size() >= limits_.max_topics) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     "topic registry capacity reached");
            }
            if (topic_names_.contains(candidate.name)) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "topic name already exists");
            }
            MINO_RETURN_IF_ERROR(ValidateStaticRouteNodesLocked(candidate));
        }

        // AllocateTopicId is a persistence boundary and a user-supplied virtual
        // call. It is never made under a registry lock. Durable allocators must
        // persist the HWM before returning; every later failure burns this ID.
        MINO_ASSIGN_OR_RETURN(candidate.topic_id,
                              id_allocator_->AllocateTopicId());
        candidate.config_version = 1;
        candidate.route_set_version = 1;
        candidate.state = TopicState::kCreating;
        MINO_RETURN_IF_ERROR(ValidateTopicMetadata(candidate, limits_));

        auto snapshot = std::make_shared<const TopicSnapshot>(TopicSnapshot{
            .metadata = candidate,
            .usage = {},
        });
        auto subscriber_nodes =
            std::make_shared<const SubscriberNodeSetSnapshot>(
                SubscriberNodeSetSnapshot{
                    .topic_id = candidate.topic_id,
                    .version = 0,
                    .nodes = {},
                });
        TopicEntry entry{
            .snapshot = snapshot,
            .subscriber_nodes = std::move(subscriber_nodes),
            .subscriber_counts_by_node = {},
        };

        std::lock_guard lock(mutex_);
        if (topics_.size() >= limits_.max_topics) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "topic registry capacity reached; TopicId burned");
        }
        if (topic_names_.contains(candidate.name)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "topic name already exists; TopicId burned");
        }
        MINO_RETURN_IF_ERROR(ValidateStaticRouteNodesLocked(candidate));
        if (candidate.topic_id.value == 0 ||
            topics_.contains(candidate.topic_id)) {
            return Status::Error(
                StatusCode::kCorruption,
                "IdAllocator returned an invalid or reused TopicId");
        }

        const TopicId topic_id = candidate.topic_id;
        topics_.emplace(topic_id, std::move(entry));
        try {
            topic_names_.emplace(candidate.name, topic_id);
        } catch (...) {
            topics_.erase(topic_id);
            throw;
        }
        return snapshot;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::shared_ptr<const TopicSnapshot>> Coordinator::GetTopic(
    TopicId topic_id) const {
    try {
        std::lock_guard lock(mutex_);
        const auto found = topics_.find(topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        return found->second.snapshot;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::vector<std::shared_ptr<const TopicSnapshot>>>
Coordinator::ListTopics() const {
    try {
        std::lock_guard lock(mutex_);
        std::vector<std::shared_ptr<const TopicSnapshot>> snapshots;
        snapshots.reserve(topics_.size());
        for (const auto& [topic_id, entry] : topics_) {
            (void)topic_id;
            snapshots.push_back(entry.snapshot);
        }
        std::sort(snapshots.begin(), snapshots.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return lhs->metadata.topic_id < rhs->metadata.topic_id;
                  });
        return snapshots;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::UpdateTopic(TopicMetadata replacement,
                                uint64_t expected_config_version) {
    try {
        MINO_RETURN_IF_ERROR(ValidateTopicMetadata(replacement, limits_));
        if (expected_config_version == std::numeric_limits<uint64_t>::max() ||
            replacement.config_version != expected_config_version + 1) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "topic config version must advance by one");
        }
        auto validated =
            std::make_shared<const TopicMetadata>(std::move(replacement));

        std::lock_guard lock(mutex_);
        auto found = topics_.find(validated->topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        TopicEntry& entry = found->second;
        const TopicMetadata& current = entry.snapshot->metadata;
        if (current.config_version != expected_config_version) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "topic config CAS version mismatch");
        }
        if (validated->state != current.state) {
            return Status::Error(StatusCode::kUnsupported,
                                 "topic state is lifecycle-managed");
        }
        if (current.state == TopicState::kDeleted) {
            return Status::Error(StatusCode::kUnsupported,
                                 "deleted topic metadata is immutable");
        }
        if (validated->capacity != current.capacity ||
            validated->partition_count != current.partition_count ||
            validated->channel_kind != current.channel_kind ||
            validated->record_topology != current.record_topology) {
            return Status::Error(
                StatusCode::kUnsupported,
                "capacity, partition, channel, and record topology need replacement");
        }
        if (!SchemaIdentityEqual(validated->schema, current.schema)) {
            return Status::Error(
                StatusCode::kUnsupported,
                "schema changes require compatibility proof and replacement");
        }
        if (validated->max_publishers < entry.snapshot->usage.publishers ||
            validated->max_subscribers < entry.snapshot->usage.subscribers) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "new limits are below active registrations");
        }
        const bool routes_changed = !SameRoutes(*validated, current);
        const uint64_t expected_route_version =
            routes_changed ? current.route_set_version + 1
                           : current.route_set_version;
        if ((routes_changed &&
             current.route_set_version == std::numeric_limits<uint64_t>::max()) ||
            validated->route_set_version != expected_route_version) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "route set version does not match route change");
        }
        if (validated->name != current.name &&
            topic_names_.contains(validated->name)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "topic name already exists");
        }
        MINO_RETURN_IF_ERROR(ValidateStaticRouteNodesLocked(*validated));

        auto next_snapshot = std::make_shared<const TopicSnapshot>(TopicSnapshot{
            .metadata = *validated,
            .usage = entry.snapshot->usage,
        });
        if (validated->name != current.name) {
            topic_names_.emplace(validated->name, validated->topic_id);
            topic_names_.erase(current.name);
        }
        entry.snapshot = std::move(next_snapshot);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::AdvanceTopicStateLocked(TopicEntry& entry,
                                            TopicState expected,
                                            TopicState next) {
    if (entry.snapshot->metadata.state != expected) {
        return Status::Error(StatusCode::kUnsupported,
                             "illegal topic lifecycle transition");
    }
    if (entry.snapshot->metadata.config_version ==
        std::numeric_limits<uint64_t>::max()) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "topic config version exhausted");
    }
    TopicSnapshot replacement = *entry.snapshot;
    replacement.metadata.state = next;
    ++replacement.metadata.config_version;
    entry.snapshot =
        std::make_shared<const TopicSnapshot>(std::move(replacement));
    return Status::Ok();
}

Status Coordinator::ActivateTopic(
    TopicId topic_id, const ActivationReadinessProof& readiness) {
    try {
        if (!readiness.complete()) {
            return Status::Error(StatusCode::kUnavailable,
                                 "schema, region, channel, and ACL must be ready");
        }
        std::lock_guard lock(mutex_);
        auto found = topics_.find(topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        if (!ProofMatches(found->second.snapshot->metadata, readiness)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "activation proof is stale or belongs to another topic");
        }
        return AdvanceTopicStateLocked(found->second, TopicState::kCreating,
                                       TopicState::kActive);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::DrainTopic(TopicId topic_id) {
    try {
        std::lock_guard lock(mutex_);
        auto found = topics_.find(topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        return AdvanceTopicStateLocked(found->second, TopicState::kActive,
                                       TopicState::kDraining);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::RetireTopic(
    TopicId topic_id, const DrainCompletionProof& completion) {
    try {
        if (!completion.complete()) {
            return Status::Error(
                StatusCode::kUnavailable,
                "channel drain and borrow release proofs are required");
        }
        std::lock_guard lock(mutex_);
        auto found = topics_.find(topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        if (!ProofMatches(found->second.snapshot->metadata, completion)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "drain completion proof is stale or mismatched");
        }
        const TopicUsageCounts& usage = found->second.snapshot->usage;
        if (usage.publishers != 0 || usage.subscribers != 0 ||
            usage.bridges != 0 || usage.recorders != 0 ||
            TopicHasParticipantsLocked(topic_id) ||
            TopicHasNonReplayPinsLocked(topic_id)) {
            return Status::Error(
                StatusCode::kWouldBlock,
                "topic still has live publishers, subscribers, bridges, or recorders");
        }
        // Replay tokens deliberately do not block retirement. They remain in
        // the exact token table and continue to block DeleteTopic.
        return AdvanceTopicStateLocked(found->second, TopicState::kDraining,
                                       TopicState::kRetired);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::DeleteTopic(TopicId topic_id) {
    try {
        std::lock_guard lock(mutex_);
        auto found = topics_.find(topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        if (found->second.snapshot->metadata.state != TopicState::kRetired) {
            return Status::Error(StatusCode::kUnsupported,
                                 "only a retired topic can be deleted");
        }
        if (TopicHasParticipantsLocked(topic_id) || TopicHasPinsLocked(topic_id) ||
            !found->second.snapshot->usage.empty()) {
            return Status::Error(
                StatusCode::kWouldBlock,
                "topic still has registrations or exact pin tokens");
        }
        return AdvanceTopicStateLocked(found->second, TopicState::kRetired,
                                       TopicState::kDeleted);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::ValidateOwnerLocked(const NodeLeaseOwner& owner,
                                        uint64_t now_ns) const {
    auto node = nodes_->Get(owner.node_id);
    if (!node.ok() || node.value()->process_identity != owner.process_identity ||
        node.value()->lease_epoch != owner.lease_epoch) {
        return Status::Error(StatusCode::kNotFound,
                             "registration owner is not the active node lease");
    }
    if (node.value()->lease_state != NodeLeaseState::kActive ||
        now_ns >= node.value()->lease_deadline_ns) {
        return Status::Error(StatusCode::kUnavailable,
                             "registration owner lease is expired");
    }
    return Status::Ok();
}

Status Coordinator::RegisterPublisher(
    const PublisherRegistration& registration, uint64_t now_ns) {
    try {
        if (registration.topic_id.value == 0 ||
            registration.publisher_id.value == 0 || registration.generation == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "publisher registration is incomplete");
        }
        std::lock_guard lock(mutex_);
        auto topic = topics_.find(registration.topic_id);
        if (topic == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        const PublisherKey key{registration.topic_id,
                               registration.publisher_id};
        const auto existing = publishers_.find(key);
        if (existing != publishers_.end()) {
            return SamePublisherRegistration(existing->second, registration)
                       ? Status::Ok()
                       : Status::Error(StatusCode::kAlreadyExists,
                                       "publisher ID is already registered");
        }
        if (topic->second.snapshot->metadata.state != TopicState::kActive) {
            return Status::Error(StatusCode::kUnavailable,
                                 "new publishers require an active topic");
        }
        MINO_RETURN_IF_ERROR(ValidateOwnerLocked(registration.owner, now_ns));
        TopicUsageCounts usage = topic->second.snapshot->usage;
        if (usage.publishers >=
                topic->second.snapshot->metadata.max_publishers ||
            publishers_.size() >= limits_.max_publisher_registrations) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "publisher registration capacity reached");
        }
        ++usage.publishers;
        auto next = std::make_shared<const TopicSnapshot>(TopicSnapshot{
            .metadata = topic->second.snapshot->metadata,
            .usage = usage,
        });
        publishers_.emplace(key, registration);
        topic->second.snapshot = std::move(next);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::UnregisterPublisher(
    const PublisherRegistration& registration) {
    try {
        std::lock_guard lock(mutex_);
        const PublisherKey key{registration.topic_id,
                               registration.publisher_id};
        auto found = publishers_.find(key);
        if (found == publishers_.end() ||
            !SamePublisherRegistration(found->second, registration)) {
            return Status::Error(StatusCode::kNotFound,
                                 "publisher registration does not match");
        }
        auto topic = topics_.find(registration.topic_id);
        if (topic == topics_.end() ||
            topic->second.snapshot->usage.publishers == 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "publisher counter is inconsistent");
        }
        TopicUsageCounts usage = topic->second.snapshot->usage;
        --usage.publishers;
        auto next = std::make_shared<const TopicSnapshot>(TopicSnapshot{
            .metadata = topic->second.snapshot->metadata,
            .usage = usage,
        });
        publishers_.erase(found);
        topic->second.snapshot = std::move(next);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::shared_ptr<const SubscriberNodeSetSnapshot>>
Coordinator::BuildSubscriberNodeSnapshotLocked(const TopicEntry& entry,
                                               std::optional<NodeId> add,
                                               std::optional<NodeId> remove) const {
    auto snapshot = std::make_shared<SubscriberNodeSetSnapshot>();
    snapshot->topic_id = entry.snapshot->metadata.topic_id;
    snapshot->version = entry.subscriber_nodes->version;
    snapshot->nodes.reserve(entry.subscriber_counts_by_node.size() +
                            (add.has_value() ? 1u : 0u));
    for (const auto& [node_id, count] : entry.subscriber_counts_by_node) {
        if (count != 0 && (!remove.has_value() || node_id != *remove)) {
            snapshot->nodes.push_back(node_id);
        }
    }
    if (add.has_value() &&
        !entry.subscriber_counts_by_node.contains(*add)) {
        snapshot->nodes.push_back(*add);
    }
    if (add.has_value() || remove.has_value()) {
        if (snapshot->version == std::numeric_limits<uint64_t>::max()) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "subscriber node set version exhausted");
        }
        ++snapshot->version;
    }
    std::sort(snapshot->nodes.begin(), snapshot->nodes.end());
    return std::shared_ptr<const SubscriberNodeSetSnapshot>(
        std::move(snapshot));
}

Status Coordinator::RegisterSubscriber(
    const SubscriberRegistration& registration, uint64_t now_ns) {
    try {
        if (registration.topic_id.value == 0 ||
            registration.subscriber_id.value == 0 ||
            registration.generation == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "subscriber registration is incomplete");
        }
        std::lock_guard lock(mutex_);
        auto topic = topics_.find(registration.topic_id);
        if (topic == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        const SubscriberKey key{registration.topic_id,
                                registration.subscriber_id};
        const auto existing = subscribers_.find(key);
        if (existing != subscribers_.end()) {
            return SameSubscriberRegistration(existing->second, registration)
                       ? Status::Ok()
                       : Status::Error(StatusCode::kAlreadyExists,
                                       "subscriber ID is already registered");
        }
        const TopicState state = topic->second.snapshot->metadata.state;
        if (state != TopicState::kActive && state != TopicState::kDraining) {
            return Status::Error(StatusCode::kUnavailable,
                                 "topic does not accept subscribers");
        }
        MINO_RETURN_IF_ERROR(ValidateOwnerLocked(registration.owner, now_ns));
        TopicUsageCounts usage = topic->second.snapshot->usage;
        if (usage.subscribers >=
                topic->second.snapshot->metadata.max_subscribers ||
            subscribers_.size() >= limits_.max_subscriber_registrations) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "subscriber registration capacity reached");
        }
        ++usage.subscribers;
        const bool new_node =
            !topic->second.subscriber_counts_by_node.contains(
                registration.owner.node_id);
        MINO_ASSIGN_OR_RETURN(
            auto node_snapshot,
            BuildSubscriberNodeSnapshotLocked(
                topic->second,
                new_node ? std::optional<NodeId>(registration.owner.node_id)
                         : std::nullopt,
                std::nullopt));
        auto topic_snapshot =
            std::make_shared<const TopicSnapshot>(TopicSnapshot{
                .metadata = topic->second.snapshot->metadata,
                .usage = usage,
            });
        subscribers_.emplace(key, registration);
        try {
            auto [count, inserted] =
                topic->second.subscriber_counts_by_node.try_emplace(
                    registration.owner.node_id, 0);
            (void)inserted;
            ++count->second;
        } catch (...) {
            subscribers_.erase(key);
            throw;
        }
        topic->second.snapshot = std::move(topic_snapshot);
        topic->second.subscriber_nodes = std::move(node_snapshot);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::UnregisterSubscriber(
    const SubscriberRegistration& registration) {
    try {
        std::lock_guard lock(mutex_);
        const SubscriberKey key{registration.topic_id,
                                registration.subscriber_id};
        auto found = subscribers_.find(key);
        if (found == subscribers_.end() ||
            !SameSubscriberRegistration(found->second, registration)) {
            return Status::Error(StatusCode::kNotFound,
                                 "subscriber registration does not match");
        }
        auto topic = topics_.find(registration.topic_id);
        if (topic == topics_.end() ||
            topic->second.snapshot->usage.subscribers == 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "subscriber counter is inconsistent");
        }
        auto count = topic->second.subscriber_counts_by_node.find(
            registration.owner.node_id);
        if (count == topic->second.subscriber_counts_by_node.end() ||
            count->second == 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "subscriber node counter is inconsistent");
        }
        const bool remove_node = count->second == 1;
        MINO_ASSIGN_OR_RETURN(
            auto node_snapshot,
            BuildSubscriberNodeSnapshotLocked(
                topic->second, std::nullopt,
                remove_node
                    ? std::optional<NodeId>(registration.owner.node_id)
                    : std::nullopt));
        TopicUsageCounts usage = topic->second.snapshot->usage;
        --usage.subscribers;
        auto topic_snapshot =
            std::make_shared<const TopicSnapshot>(TopicSnapshot{
                .metadata = topic->second.snapshot->metadata,
                .usage = usage,
            });
        subscribers_.erase(found);
        if (--count->second == 0) {
            topic->second.subscriber_counts_by_node.erase(count);
        }
        topic->second.snapshot = std::move(topic_snapshot);
        topic->second.subscriber_nodes = std::move(node_snapshot);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::AcquireTopicPin(
    const TopicPinRegistration& registration, uint64_t now_ns) {
    try {
        if (registration.topic_id.value == 0 ||
            registration.pin_id.value == 0 || registration.generation == 0 ||
            !ValidPinKind(registration.kind)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "topic pin registration is incomplete");
        }
        std::lock_guard lock(mutex_);
        auto found = topics_.find(registration.topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        const TopicPinKey key{registration.topic_id, registration.pin_id};
        const auto existing = pins_.find(key);
        if (existing != pins_.end()) {
            return existing->second == registration
                       ? Status::Ok()
                       : Status::Error(
                             StatusCode::kAlreadyExists,
                             "TopicPinId is held by another owner, generation, or kind");
        }
        const TopicState state = found->second.snapshot->metadata.state;
        const bool accepted =
            state == TopicState::kActive || state == TopicState::kDraining ||
            (state == TopicState::kRetired &&
             registration.kind == TopicPinKind::kReplay);
        if (!accepted) {
            return Status::Error(StatusCode::kUnavailable,
                                 "topic state does not accept this pin kind");
        }
        MINO_RETURN_IF_ERROR(ValidateOwnerLocked(registration.owner, now_ns));
        if (total_topic_pins_ >= limits_.max_topic_pins) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "topic pin capacity reached");
        }
        TopicUsageCounts usage = found->second.snapshot->usage;
        uint32_t* counter = nullptr;
        switch (registration.kind) {
            case TopicPinKind::kBridge:
                counter = &usage.bridges;
                break;
            case TopicPinKind::kRecorder:
                counter = &usage.recorders;
                break;
            case TopicPinKind::kReplay:
                counter = &usage.replay_pins;
                break;
        }
        if (*counter == std::numeric_limits<uint32_t>::max()) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "topic pin counter exhausted");
        }
        ++*counter;
        auto snapshot = std::make_shared<const TopicSnapshot>(TopicSnapshot{
            .metadata = found->second.snapshot->metadata,
            .usage = usage,
        });
        pins_.emplace(key, registration);
        found->second.snapshot = std::move(snapshot);
        ++total_topic_pins_;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::ReleaseTopicPin(
    const TopicPinRegistration& registration) {
    try {
        std::lock_guard lock(mutex_);
        const TopicPinKey key{registration.topic_id, registration.pin_id};
        auto pin = pins_.find(key);
        if (pin == pins_.end() || pin->second != registration) {
            return Status::Error(
                StatusCode::kNotFound,
                "topic pin token does not exactly match owner and generation");
        }
        auto found = topics_.find(registration.topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kCorruption,
                                 "topic pin refers to a missing topic");
        }
        TopicUsageCounts usage = found->second.snapshot->usage;
        uint32_t* counter = nullptr;
        switch (registration.kind) {
            case TopicPinKind::kBridge:
                counter = &usage.bridges;
                break;
            case TopicPinKind::kRecorder:
                counter = &usage.recorders;
                break;
            case TopicPinKind::kReplay:
                counter = &usage.replay_pins;
                break;
            default:
                return Status::Error(StatusCode::kCorruption,
                                     "stored topic pin kind is invalid");
        }
        if (*counter == 0 || total_topic_pins_ == 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "topic pin counter is inconsistent");
        }
        --*counter;
        auto snapshot = std::make_shared<const TopicSnapshot>(TopicSnapshot{
            .metadata = found->second.snapshot->metadata,
            .usage = usage,
        });
        pins_.erase(pin);
        found->second.snapshot = std::move(snapshot);
        --total_topic_pins_;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::shared_ptr<const SubscriberNodeSetSnapshot>>
Coordinator::DiscoverySubscriberNodes(TopicId topic_id) const {
    try {
        std::lock_guard lock(mutex_);
        const auto found = topics_.find(topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound, "topic not found");
        }
        if (found->second.snapshot->metadata.route_policy !=
            RoutePolicy::kDiscovery) {
            return Status::Error(StatusCode::kUnsupported,
                                 "topic does not use discovery routing");
        }
        return found->second.subscriber_nodes;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::shared_ptr<const RouteSnapshot>> Coordinator::ResolveRoutes(
    TopicId topic_id) const {
    MINO_ASSIGN_OR_RETURN(auto routing, GetRoutingSnapshot(topic_id));
    return routing->routes;
}

Result<std::shared_ptr<const RoutingSnapshot>>
Coordinator::GetRoutingSnapshot(TopicId topic_id) const {
    try {
        auto routes = std::make_shared<RouteSnapshot>();
        std::shared_ptr<const TopicSnapshot> topic;
        {
            std::lock_guard lock(mutex_);
            const auto found = topics_.find(topic_id);
            if (found == topics_.end()) {
                return Status::Error(StatusCode::kNotFound, "topic not found");
            }
            topic = found->second.snapshot;
            routes->topic_id = topic_id;
            routes->policy = topic->metadata.route_policy;
            if (routes->policy == RoutePolicy::kStatic) {
                routes->version = topic->metadata.route_set_version;
                routes->routes = topic->metadata.static_routes;
            } else {
                routes->version = found->second.subscriber_nodes->version;
                routes->routes.reserve(
                    found->second.subscriber_nodes->nodes.size());
                for (NodeId node_id : found->second.subscriber_nodes->nodes) {
                    routes->routes.push_back(StaticRouteEntry{
                        .target_node = node_id,
                        .preferred_transport = std::nullopt,
                    });
                }
            }
        }
        auto routing = std::make_shared<const RoutingSnapshot>(RoutingSnapshot{
            .topic = std::move(topic),
            .routes = std::shared_ptr<const RouteSnapshot>(std::move(routes)),
        });
        return routing;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status Coordinator::PublishUsageLocked(TopicEntry& entry,
                                       TopicUsageCounts usage) {
    entry.snapshot = std::make_shared<const TopicSnapshot>(TopicSnapshot{
        .metadata = entry.snapshot->metadata,
        .usage = usage,
    });
    return Status::Ok();
}

bool Coordinator::OwnerHasResourcesLocked(
    const NodeLeaseOwner& owner) const noexcept {
    return std::any_of(publishers_.begin(), publishers_.end(),
                       [&owner](const auto& item) {
                           return item.second.owner == owner;
                       }) ||
           std::any_of(subscribers_.begin(), subscribers_.end(),
                       [&owner](const auto& item) {
                           return item.second.owner == owner;
                       }) ||
           std::any_of(pins_.begin(), pins_.end(), [&owner](const auto& item) {
               return item.second.owner == owner;
           });
}

bool Coordinator::TopicHasParticipantsLocked(TopicId topic_id) const noexcept {
    return std::any_of(publishers_.begin(), publishers_.end(),
                       [topic_id](const auto& item) {
                           return item.second.topic_id == topic_id;
                       }) ||
           std::any_of(subscribers_.begin(), subscribers_.end(),
                       [topic_id](const auto& item) {
                           return item.second.topic_id == topic_id;
                       });
}

bool Coordinator::TopicHasNonReplayPinsLocked(TopicId topic_id) const noexcept {
    return std::any_of(pins_.begin(), pins_.end(),
                       [topic_id](const auto& item) {
                           return item.second.topic_id == topic_id &&
                                  item.second.kind != TopicPinKind::kReplay;
                       });
}

bool Coordinator::TopicHasPinsLocked(TopicId topic_id) const noexcept {
    return std::any_of(pins_.begin(), pins_.end(),
                       [topic_id](const auto& item) {
                           return item.second.topic_id == topic_id;
                       });
}

void Coordinator::QueueOwnerCleanupLocked(const NodeLeaseOwner& owner) {
    if (!OwnerHasResourcesLocked(owner)) {
        return;
    }
    if (std::find(pending_cleanup_owners_.begin(),
                  pending_cleanup_owners_.end(), owner) !=
        pending_cleanup_owners_.end()) {
        return;
    }
    // Capacity was reserved for every bounded registration and pin. Since an
    // owner is queued only while it owns at least one such record, this push
    // cannot allocate and cannot lose cleanup responsibility after node commit.
    pending_cleanup_owners_.push_back(owner);
}

Status Coordinator::RetryPendingCleanupLocked(
    CoordinatorSweepResult* result) {
    while (!pending_cleanup_owners_.empty()) {
        const NodeLeaseOwner owner = pending_cleanup_owners_.front();
        MINO_RETURN_IF_ERROR(CleanupOwnerLocked(owner, result));
        pending_cleanup_owners_.erase(pending_cleanup_owners_.begin());
    }
    return Status::Ok();
}

Status Coordinator::CleanupOwnerLocked(const NodeLeaseOwner& owner,
                                       CoordinatorSweepResult* result) {
    // Build every potentially allocating snapshot before changing maps or
    // counters. After allocation succeeds, snapshot publication, map erasure,
    // and scalar updates are non-throwing. A retry therefore sees either the
    // intact record or an already committed removal.
    for (auto iterator = publishers_.begin(); iterator != publishers_.end();) {
        if (iterator->second.owner != owner) {
            ++iterator;
            continue;
        }
        auto topic = topics_.find(iterator->second.topic_id);
        if (topic != topics_.end() &&
            topic->second.snapshot->usage.publishers != 0) {
            TopicUsageCounts usage = topic->second.snapshot->usage;
            --usage.publishers;
            if (fault_injector_) {
                fault_injector_->MaybeFailOwnerCleanupAllocation();
            }
            MINO_RETURN_IF_ERROR(PublishUsageLocked(topic->second, usage));
        }
        iterator = publishers_.erase(iterator);
        ++result->publishers_removed;
    }

    for (auto iterator = subscribers_.begin(); iterator != subscribers_.end();) {
        if (iterator->second.owner != owner) {
            ++iterator;
            continue;
        }
        auto topic = topics_.find(iterator->second.topic_id);
        if (topic != topics_.end() &&
            topic->second.snapshot->usage.subscribers != 0) {
            const NodeId node_id = iterator->second.owner.node_id;
            auto count = topic->second.subscriber_counts_by_node.find(node_id);
            const bool remove_node =
                count != topic->second.subscriber_counts_by_node.end() &&
                count->second == 1;
            if (fault_injector_) {
                fault_injector_->MaybeFailOwnerCleanupAllocation();
            }
            MINO_ASSIGN_OR_RETURN(
                auto node_snapshot,
                BuildSubscriberNodeSnapshotLocked(
                    topic->second, std::nullopt,
                    remove_node ? std::optional<NodeId>(node_id)
                                : std::nullopt));
            TopicUsageCounts usage = topic->second.snapshot->usage;
            --usage.subscribers;
            if (fault_injector_) {
                fault_injector_->MaybeFailOwnerCleanupAllocation();
            }
            MINO_RETURN_IF_ERROR(PublishUsageLocked(topic->second, usage));
            if (count != topic->second.subscriber_counts_by_node.end()) {
                if (--count->second == 0) {
                    topic->second.subscriber_counts_by_node.erase(count);
                }
            }
            topic->second.subscriber_nodes = std::move(node_snapshot);
        }
        iterator = subscribers_.erase(iterator);
        ++result->subscribers_removed;
    }

    for (auto iterator = pins_.begin(); iterator != pins_.end();) {
        if (iterator->second.owner != owner) {
            ++iterator;
            continue;
        }
        auto topic = topics_.find(iterator->second.topic_id);
        if (topic != topics_.end()) {
            TopicUsageCounts usage = topic->second.snapshot->usage;
            uint32_t* counter = nullptr;
            switch (iterator->second.kind) {
                case TopicPinKind::kBridge:
                    counter = &usage.bridges;
                    break;
                case TopicPinKind::kRecorder:
                    counter = &usage.recorders;
                    break;
                case TopicPinKind::kReplay:
                    counter = &usage.replay_pins;
                    break;
            }
            if (counter == nullptr || *counter == 0 || total_topic_pins_ == 0) {
                return Status::Error(StatusCode::kCorruption,
                                     "topic pin cleanup counter is inconsistent");
            }
            --*counter;
            if (fault_injector_) {
                fault_injector_->MaybeFailOwnerCleanupAllocation();
            }
            MINO_RETURN_IF_ERROR(PublishUsageLocked(topic->second, usage));
        }
        iterator = pins_.erase(iterator);
        --total_topic_pins_;
        ++result->pins_removed;
    }
    return Status::Ok();
}

}  // namespace mino::registry
