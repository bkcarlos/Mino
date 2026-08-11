// Copyright 2026 The Mino Authors

#ifndef MINO_REGISTRY_COORDINATOR_H_
#define MINO_REGISTRY_COORDINATOR_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "mino/capacity/capacity.h"
#include "mino/common/result.h"
#include "mino/registry/id_allocator.h"
#include "mino/registry/metadata.h"
#include "mino/registry/node_registry.h"

namespace mino::registry {

struct CoordinatorSweepResult {
    NodeSweepResult nodes;
    size_t publishers_removed = 0;
    size_t subscribers_removed = 0;
    size_t pins_removed = 0;
};

// Deterministic bad_alloc injection for cleanup transaction tests. Production
// code normally leaves this unset.
class RegistryFaultInjector final {
public:
    void FailOwnerCleanupAllocationAfter(size_t successful_allocations) noexcept;

private:
    friend class Coordinator;
    void MaybeFailOwnerCleanupAllocation();
    std::atomic<size_t> cleanup_allocation_countdown_{0};
};

class Coordinator final {
public:
    // Production construction requires an explicitly supplied durable allocator.
    static Result<std::unique_ptr<Coordinator>> Create(
        CoordinatorLimits limits = {},
        std::shared_ptr<IdAllocator> id_allocator = {},
        std::shared_ptr<const LivenessProbe> liveness_probe = {},
        std::shared_ptr<capacity::CapacityController> capacity_controller = {});

    // Explicitly non-durable construction for unit tests and local development.
    static Result<std::unique_ptr<Coordinator>> CreateForTesting(
        CoordinatorLimits limits = {},
        std::shared_ptr<IdAllocator> id_allocator = {},
        std::shared_ptr<const LivenessProbe> liveness_probe = {},
        std::shared_ptr<RegistryFaultInjector> fault_injector = {},
        std::shared_ptr<capacity::CapacityController> capacity_controller = {});

    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;

    Result<NodeRegistrationOutcome> RegisterNode(const NodeRegistration& request,
                                                 uint64_t now_ns);
    Status HeartbeatNode(const NodeLeaseOwner& owner, NodeHealth health,
                         uint64_t now_ns);
    Status UpdateNode(const NodeRegistration& replacement,
                      uint64_t expected_config_version, uint64_t now_ns);
    Result<CoordinatorSweepResult> SweepExpiredNodes(uint64_t now_ns);
    Result<std::shared_ptr<const NodeMetadata>> GetNode(NodeId node_id) const;
    Result<std::shared_ptr<const NodeRegistrySnapshot>> NodeSnapshot() const;

    // additional_resources models the Topic's concrete SHM/ring/slab/schema
    // resources. The registry always adds one Topic unit atomically.
    Result<std::shared_ptr<const TopicSnapshot>> CreateTopic(
        TopicMetadata candidate,
        capacity::ResourceVector additional_resources = {});
    Result<std::shared_ptr<const TopicSnapshot>> GetTopic(TopicId topic_id) const;
    Result<std::shared_ptr<const TopicSnapshot>> FindTopic(
        std::string_view name) const;
    Result<std::vector<std::shared_ptr<const TopicSnapshot>>> ListTopics() const;

    // Two-phase update: validate and build an immutable candidate first, then
    // publish it only if expected_config_version still matches.
    Status UpdateTopic(TopicMetadata replacement,
                       uint64_t expected_config_version);

    Status ActivateTopic(TopicId topic_id,
                         const ActivationReadinessProof& readiness);
    Status DrainTopic(TopicId topic_id);
    Status RetireTopic(TopicId topic_id,
                       const DrainCompletionProof& completion);
    Status DeleteTopic(TopicId topic_id);

    // Rolling-upgrade control-plane operations. BeginUpgradeDrain linearizes
    // the complete source set under one lock: after it returns, all topics are
    // Draining and RegisterPublisher rejects every new source publisher. The
    // returned immutable snapshots are the authoritative usage baseline.
    Result<std::vector<std::shared_ptr<const TopicSnapshot>>> BeginUpgradeDrain(
        std::span<const TopicId> topic_ids);
    Result<std::vector<std::shared_ptr<const TopicSnapshot>>> UpgradeUsageSnapshot(
        std::span<const TopicId> topic_ids) const;
    Status CancelUpgradeDrain(std::span<const TopicId> topic_ids);

    // Validates the complete source set before changing any topic. Exact
    // participant/pin tables, usage counters, and per-topic channel/borrow
    // proofs all gate the atomic Draining -> Deleted transition. Replaying the
    // operation after a crash is idempotent when every topic is already Deleted.
    Status RetireAndDeleteUpgradeTopics(
        std::span<const DrainCompletionProof> completions);

    Status RegisterPublisher(const PublisherRegistration& registration,
                             uint64_t now_ns);
    Status UnregisterPublisher(const PublisherRegistration& registration);
    Status RegisterSubscriber(const SubscriberRegistration& registration,
                              uint64_t now_ns);
    Status UnregisterSubscriber(const SubscriberRegistration& registration);

    Status AcquireTopicPin(const TopicPinRegistration& registration,
                           uint64_t now_ns);
    Status ReleaseTopicPin(const TopicPinRegistration& registration);

    Result<std::shared_ptr<const SubscriberNodeSetSnapshot>>
    DiscoverySubscriberNodes(TopicId topic_id) const;
    Result<std::shared_ptr<const RouteSnapshot>> ResolveRoutes(
        TopicId topic_id) const;
    Result<std::shared_ptr<const RoutingSnapshot>> GetRoutingSnapshot(
        TopicId topic_id) const;

private:
    struct PublisherKey {
        TopicId topic_id;
        PublisherId publisher_id;
        friend bool operator==(const PublisherKey&, const PublisherKey&) =
            default;
    };
    struct SubscriberKey {
        TopicId topic_id;
        SubscriberId subscriber_id;
        friend bool operator==(const SubscriberKey&, const SubscriberKey&) =
            default;
    };
    struct PublisherKeyHash {
        size_t operator()(const PublisherKey& key) const noexcept;
    };
    struct SubscriberKeyHash {
        size_t operator()(const SubscriberKey& key) const noexcept;
    };
    struct TopicPinKey {
        TopicId topic_id;
        TopicPinId pin_id;
        friend bool operator==(const TopicPinKey&, const TopicPinKey&) = default;
    };
    struct TopicPinKeyHash {
        size_t operator()(const TopicPinKey& key) const noexcept;
    };
    struct PublisherEntry {
        PublisherRegistration registration;
        capacity::CapacityLease capacity_lease;
    };
    struct SubscriberEntry {
        SubscriberRegistration registration;
        capacity::CapacityLease capacity_lease;
    };
    struct TopicEntry {
        // Declared first so the charge outlives all registry-side Topic state.
        capacity::CapacityLease capacity_lease;
        std::shared_ptr<const TopicSnapshot> snapshot;
        std::shared_ptr<const SubscriberNodeSetSnapshot> subscriber_nodes;
        std::unordered_map<NodeId, uint32_t> subscriber_counts_by_node;
    };

    Coordinator(CoordinatorLimits limits, std::shared_ptr<IdAllocator> allocator,
                std::unique_ptr<NodeRegistry> nodes,
                std::shared_ptr<RegistryFaultInjector> fault_injector,
                std::shared_ptr<capacity::CapacityController> capacity_controller);
    static Result<std::unique_ptr<Coordinator>> CreateImpl(
        CoordinatorLimits limits, std::shared_ptr<IdAllocator> id_allocator,
        std::shared_ptr<const LivenessProbe> liveness_probe,
        std::shared_ptr<RegistryFaultInjector> fault_injector,
        std::shared_ptr<capacity::CapacityController> capacity_controller,
        bool require_durable);
    Result<capacity::CapacityReservation> ReserveCapacity(
        capacity::ResourceVector resources, capacity::ResourceScope scope,
        std::string_view name);

    Status ValidateStaticRouteNodesLocked(const TopicMetadata& metadata) const;
    Result<SecurityDomainId> ValidateOwnerLocked(
        const NodeLeaseOwner& owner, uint64_t now_ns) const;
    Status AdvanceTopicStateLocked(TopicEntry& entry, TopicState expected,
                                   TopicState next);
    Result<std::shared_ptr<const SubscriberNodeSetSnapshot>>
    BuildSubscriberNodeSnapshotLocked(const TopicEntry& entry,
                                      std::optional<NodeId> add,
                                      std::optional<NodeId> remove) const;
    Status PublishUsageLocked(TopicEntry& entry, TopicUsageCounts usage);
    bool OwnerHasResourcesLocked(const NodeLeaseOwner& owner) const noexcept;
    bool TopicHasParticipantsLocked(TopicId topic_id) const noexcept;
    bool TopicHasNonReplayPinsLocked(TopicId topic_id) const noexcept;
    bool TopicHasPinsLocked(TopicId topic_id) const noexcept;
    void QueueOwnerCleanupLocked(const NodeLeaseOwner& owner);
    Status RetryPendingCleanupLocked(CoordinatorSweepResult* result);
    Status CleanupOwnerLocked(const NodeLeaseOwner& owner,
                              CoordinatorSweepResult* result);

    CoordinatorLimits limits_;
    std::shared_ptr<IdAllocator> id_allocator_;
    std::unique_ptr<NodeRegistry> nodes_;
    // Lock order: Coordinator::mutex_ may acquire NodeRegistry's mutex through
    // non-virtual Get/Snapshot operations. NodeRegistry never calls back into
    // Coordinator. LivenessProbe and IdAllocator virtual calls occur with
    // neither registry mutex held.
    mutable std::mutex mutex_;
    std::unordered_map<TopicId, TopicEntry> topics_;
    std::unordered_map<std::string, TopicId> topic_names_;
    std::unordered_map<PublisherKey, PublisherEntry, PublisherKeyHash> publishers_;
    std::unordered_map<SubscriberKey, SubscriberEntry, SubscriberKeyHash>
        subscribers_;
    std::unordered_map<TopicPinKey, TopicPinRegistration, TopicPinKeyHash> pins_;
    std::vector<NodeLeaseOwner> pending_cleanup_owners_;
    std::shared_ptr<RegistryFaultInjector> fault_injector_;
    std::shared_ptr<capacity::CapacityController> capacity_controller_;
    size_t total_topic_pins_ = 0;
};

}  // namespace mino::registry

#endif  // MINO_REGISTRY_COORDINATOR_H_
