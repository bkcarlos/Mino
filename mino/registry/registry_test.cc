// Copyright 2026 The Mino Authors

#include "mino/registry/registry.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mino::registry {
namespace {

class FakeLivenessProbe final : public LivenessProbe {
public:
    ProcessIdentityLiveness Probe(
        const ProcessIdentity& identity) const noexcept override {
        std::lock_guard lock(mutex_);
        const auto found = values_.find(identity.process_epoch);
        return found == values_.end() ? ProcessIdentityLiveness::kUnknown
                                     : found->second;
    }

    void Set(const ProcessIdentity& identity,
             ProcessIdentityLiveness liveness) {
        std::lock_guard lock(mutex_);
        values_[identity.process_epoch] = liveness;
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, ProcessIdentityLiveness> values_;
};

class ReentrantLivenessProbe final : public LivenessProbe {
public:
    void SetRegistry(const NodeRegistry* registry) noexcept {
        registry_ = registry;
    }

    ProcessIdentityLiveness Probe(
        const ProcessIdentity&) const noexcept override {
        EXPECT_NE(registry_, nullptr);
        if (registry_ != nullptr) {
            EXPECT_TRUE(registry_->Snapshot().ok());
        }
        called_ = true;
        return ProcessIdentityLiveness::kDead;
    }

    bool called() const noexcept { return called_; }

private:
    const NodeRegistry* registry_ = nullptr;
    mutable bool called_ = false;
};

struct DurableHighWatermark {
    mutable std::mutex mutex;
    uint32_t value = 0;
    bool fail_next_after_persist = false;
};

class DurableTestIdAllocator final : public IdAllocator {
public:
    explicit DurableTestIdAllocator(
        std::shared_ptr<DurableHighWatermark> storage)
        : storage_(std::move(storage)) {}

    IdAllocatorDurability durability() const noexcept override {
        return IdAllocatorDurability::kDurable;
    }

    Result<TopicId> AllocateTopicId() override {
        if (reentrant_coordinator_ != nullptr) {
            auto topics = reentrant_coordinator_->ListTopics();
            if (!topics.ok()) {
                return topics.status();
            }
            reentered_ = true;
        }
        std::lock_guard lock(storage_->mutex);
        if (storage_->value == std::numeric_limits<uint32_t>::max()) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        const TopicId persisted{++storage_->value};
        if (storage_->fail_next_after_persist) {
            storage_->fail_next_after_persist = false;
            return Status::Error(StatusCode::kUnavailable,
                                 "injected post-persist failure");
        }
        return persisted;
    }

    void FailNextAfterPersist() {
        std::lock_guard lock(storage_->mutex);
        storage_->fail_next_after_persist = true;
    }

    uint32_t high_watermark() const {
        std::lock_guard lock(storage_->mutex);
        return storage_->value;
    }

    void Reenter(Coordinator* coordinator) noexcept {
        reentrant_coordinator_ = coordinator;
    }

    bool reentered() const noexcept { return reentered_; }

private:
    std::shared_ptr<DurableHighWatermark> storage_;
    Coordinator* reentrant_coordinator_ = nullptr;
    bool reentered_ = false;
};

ProcessIdentity Identity(NodeId node_id, uint64_t pid, uint64_t epoch) {
    return ProcessIdentity{
        .node_id = node_id.value,
        .process_id = pid,
        .process_epoch = epoch,
        .start_time_ns = epoch * 100,
    };
}

transport::EndpointDescriptor Endpoint(uint8_t host, uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0},
        static_cast<std::byte>(host)};
    auto endpoint = transport::EndpointDescriptor::Ipv4Tcp(address, port);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    return endpoint.ok() ? *endpoint : transport::EndpointDescriptor{};
}

NodeRegistration Node(NodeId node_id, uint64_t pid, uint64_t epoch,
                      uint64_t lease_ns = 100) {
    return NodeRegistration{
        .node_id = node_id,
        .process_identity = Identity(node_id, pid, epoch),
        .endpoints = {Endpoint(static_cast<uint8_t>(node_id.value),
                               static_cast<uint16_t>(4000 + node_id.value))},
        .trust_domain = "test-domain",
        .health = NodeHealth::kHealthy,
        .lease_epoch = epoch,
        .lease_duration_ns = lease_ns,
        .config_version = 1,
    };
}

schema::SchemaIdentity Schema(uint64_t short_id = 7,
                              uint32_t schema_version = 1) {
    schema::CanonicalDigest digest{};
    digest[0] = static_cast<std::byte>(short_id);
    digest[31] = std::byte{0x5a};
    return schema::SchemaIdentity(short_id, digest, schema_version, 1);
}

TopicMetadata DiscoveryTopic(std::string name = "test/topic") {
    return TopicMetadata{
        .topic_id = {},
        .name = std::move(name),
        .channel_kind = ChannelKind::kBroadcast,
        .delivery = {.reliability = Reliability::kBestEffort,
                     .allow_drop = false},
        .queue_full_policy = QueueFullPolicy::kBlock,
        .schema = Schema(),
        .route_policy = RoutePolicy::kDiscovery,
        .static_routes = {},
        .route_set_version = 0,
        .capacity = 1024,
        .max_publishers = 16,
        .max_subscribers = 16,
        .partition_count = 1,
        .record_topology = RecordBackpressureTopology::kIsolated,
        .region_version = 1,
        .channel_version = 1,
        .acl_version = 1,
        .config_version = 0,
        .state = TopicState::kCreating,
    };
}

TopicMetadata StaticTopic(std::string name, NodeId target) {
    TopicMetadata topic = DiscoveryTopic(std::move(name));
    topic.route_policy = RoutePolicy::kStatic;
    topic.static_routes = {{.target_node = target,
                            .preferred_transport =
                                transport::TransportKind::kNetwork}};
    return topic;
}

std::unique_ptr<Coordinator> MakeCoordinator(
    const std::shared_ptr<FakeLivenessProbe>& probe,
    const std::shared_ptr<IdAllocator>& allocator =
        std::make_shared<InMemoryMonotonicIdAllocator>()) {
    auto coordinator = Coordinator::CreateForTesting({}, allocator, probe);
    EXPECT_TRUE(coordinator.ok()) << coordinator.status().ToString();
    return coordinator.ok() ? std::move(*coordinator) : nullptr;
}

NodeLeaseOwner Owner(const NodeRegistration& node) {
    return NodeLeaseOwner{.node_id = node.node_id,
                          .process_identity = node.process_identity,
                          .lease_epoch = node.lease_epoch};
}

ActivationReadinessProof ActivationProof(const TopicMetadata& metadata) {
    return ActivationReadinessProof{
        .topic_id = metadata.topic_id,
        .config_version = metadata.config_version,
        .schema = metadata.schema,
        .region_version = metadata.region_version,
        .channel_version = metadata.channel_version,
        .acl_version = metadata.acl_version,
        .schema_ready = true,
        .region_ready = true,
        .channel_ready = true,
        .acl_ready = true,
    };
}

DrainCompletionProof DrainProof(const TopicMetadata& metadata) {
    return DrainCompletionProof{
        .topic_id = metadata.topic_id,
        .config_version = metadata.config_version,
        .schema = metadata.schema,
        .region_version = metadata.region_version,
        .channel_version = metadata.channel_version,
        .acl_version = metadata.acl_version,
        .channel_drained = true,
        .borrows_released = true,
    };
}

TopicId CreateAndActivate(Coordinator& coordinator, TopicMetadata metadata) {
    auto topic = coordinator.CreateTopic(std::move(metadata));
    EXPECT_TRUE(topic.ok()) << topic.status().ToString();
    if (!topic.ok()) {
        return {};
    }
    EXPECT_TRUE(coordinator
                    .ActivateTopic((*topic)->metadata.topic_id,
                                   ActivationProof((*topic)->metadata))
                    .ok());
    return (*topic)->metadata.topic_id;
}

TEST(MetadataValidationTest, RejectsMalformedCapacitySchemaRoutesAndChannels) {
    CoordinatorLimits limits;
    TopicMetadata topic = DiscoveryTopic();
    EXPECT_TRUE(ValidateTopicMetadata(topic, limits, true).ok());

    topic.capacity = 1000;
    EXPECT_EQ(ValidateTopicMetadata(topic, limits, true).code(),
              StatusCode::kInvalidArgument);
    topic = DiscoveryTopic();
    topic.schema = Schema(0);
    EXPECT_EQ(ValidateTopicMetadata(topic, limits, true).code(),
              StatusCode::kInvalidArgument);
    topic = DiscoveryTopic();
    topic.static_routes = {{.target_node = NodeId{1},
                            .preferred_transport = std::nullopt}};
    EXPECT_EQ(ValidateTopicMetadata(topic, limits, true).code(),
              StatusCode::kInvalidArgument);
    topic = StaticTopic("static/topic", NodeId{1});
    topic.static_routes.push_back(topic.static_routes.front());
    EXPECT_EQ(ValidateTopicMetadata(topic, limits, true).code(),
              StatusCode::kInvalidArgument);
    topic = DiscoveryTopic();
    topic.channel_kind = ChannelKind::kSpsc;
    EXPECT_EQ(ValidateTopicMetadata(topic, limits, true).code(),
              StatusCode::kInvalidArgument);
}

TEST(NodeRegistryTest, RegistrationIsIdempotentAndSnapshotsAreImmutableCopies) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto registry_result = NodeRegistry::Create({}, probe);
    ASSERT_TRUE(registry_result.ok());
    auto registry = std::move(*registry_result);
    const NodeRegistration node = Node(NodeId{1}, 101, 11);
    ASSERT_TRUE(registry->Register(node, 10).ok());
    auto old_snapshot = registry->Snapshot();
    ASSERT_TRUE(old_snapshot.ok());
    ASSERT_TRUE(registry->Register(node, 20).ok());
    auto current = registry->Get(node.node_id);
    ASSERT_TRUE(current.ok());
    EXPECT_EQ((*current)->last_heartbeat_ns, 20u);
    ASSERT_EQ((*old_snapshot)->nodes.size(), 1u);
    EXPECT_EQ((*old_snapshot)->nodes[0].last_heartbeat_ns, 10u);
    EXPECT_EQ(registry->size(), 1u);
}

TEST(NodeRegistryTest, ConcurrentRegistrationRemainsSingleAndBounded) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto registry_result = NodeRegistry::Create({}, probe);
    ASSERT_TRUE(registry_result.ok());
    auto registry = std::move(*registry_result);
    const NodeRegistration node = Node(NodeId{2}, 202, 22);
    std::vector<std::thread> threads;
    std::array<bool, 32> success{};
    for (size_t i = 0; i < success.size(); ++i) {
        threads.emplace_back(
            [&, i] { success[i] = registry->Register(node, 1).ok(); });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (bool value : success) {
        EXPECT_TRUE(value);
    }
    EXPECT_EQ(registry->size(), 1u);
}

TEST(NodeRegistryTest, UnknownExpiredLeaseIsRetainedAndPidReuseIsExact) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto registry_result = NodeRegistry::Create({}, probe);
    ASSERT_TRUE(registry_result.ok());
    auto registry = std::move(*registry_result);
    const NodeRegistration old_process = Node(NodeId{3}, 303, 31, 10);
    ASSERT_TRUE(registry->Register(old_process, 0).ok());

    probe->Set(old_process.process_identity, ProcessIdentityLiveness::kUnknown);
    auto sweep = registry->SweepExpired(11);
    ASSERT_TRUE(sweep.ok());
    EXPECT_EQ(sweep->retained_unknown, 1u);
    EXPECT_TRUE(sweep->removed.empty());
    auto retained = registry->Get(old_process.node_id);
    ASSERT_TRUE(retained.ok());
    EXPECT_EQ((*retained)->lease_state, NodeLeaseState::kExpired);

    NodeRegistration reused_pid = Node(NodeId{3}, 303, 32, 10);
    auto blocked = registry->Register(reused_pid, 12);
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kUnavailable);

    probe->Set(old_process.process_identity, ProcessIdentityLiveness::kDead);
    auto replaced = registry->Register(reused_pid, 13);
    ASSERT_TRUE(replaced.ok()) << replaced.status().ToString();
    ASSERT_TRUE(replaced->displaced_owner.has_value());
    EXPECT_NE(replaced->node->process_identity.process_epoch,
              old_process.process_identity.process_epoch);
}

TEST(TopicLifecycleTest, EnforcesReadinessOrderingAndNeverReusesIds) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto allocator = std::make_shared<InMemoryMonotonicIdAllocator>();
    auto coordinator = MakeCoordinator(probe, allocator);
    auto first = coordinator->CreateTopic(DiscoveryTopic("topic/first"));
    ASSERT_TRUE(first.ok());
    const TopicId first_id = (*first)->metadata.topic_id;
    EXPECT_EQ(coordinator->DrainTopic(first_id).code(), StatusCode::kUnsupported);
    EXPECT_EQ(coordinator->ActivateTopic(first_id, {}).code(),
              StatusCode::kUnavailable);
    const ActivationReadinessProof activation =
        ActivationProof((*first)->metadata);
    ASSERT_TRUE(coordinator->ActivateTopic(first_id, activation).ok());
    EXPECT_EQ(coordinator->ActivateTopic(first_id, activation).code(),
              StatusCode::kAlreadyExists);
    ASSERT_TRUE(coordinator->DrainTopic(first_id).ok());
    auto draining = coordinator->GetTopic(first_id);
    ASSERT_TRUE(draining.ok());
    ASSERT_TRUE(
        coordinator->RetireTopic(first_id, DrainProof((*draining)->metadata)).ok());
    ASSERT_TRUE(coordinator->DeleteTopic(first_id).ok());

    auto second = coordinator->CreateTopic(DiscoveryTopic("topic/second"));
    ASSERT_TRUE(second.ok());
    EXPECT_GT((*second)->metadata.topic_id.value, first_id.value);
    EXPECT_EQ(allocator->high_watermark(), (*second)->metadata.topic_id.value);
}

TEST(TopicLifecycleTest, DrainRejectsPublishersAndDeleteWaitsForEveryUser) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto coordinator = MakeCoordinator(probe);
    const NodeRegistration node = Node(NodeId{4}, 404, 41, 1000);
    ASSERT_TRUE(coordinator->RegisterNode(node, 0).ok());
    const TopicId topic_id =
        CreateAndActivate(*coordinator, DiscoveryTopic("topic/gated"));
    const PublisherRegistration publisher{
        .topic_id = topic_id,
        .publisher_id = PublisherId{1},
        .generation = 1,
        .owner = Owner(node),
    };
    const SubscriberRegistration subscriber{
        .topic_id = topic_id,
        .subscriber_id = SubscriberId{1},
        .generation = 1,
        .owner = Owner(node),
    };
    ASSERT_TRUE(coordinator->RegisterPublisher(publisher, 1).ok());
    ASSERT_TRUE(coordinator->RegisterSubscriber(subscriber, 1).ok());
    const TopicPinRegistration bridge{
        .topic_id = topic_id,
        .pin_id = TopicPinId{1},
        .kind = TopicPinKind::kBridge,
        .generation = 1,
        .owner = Owner(node),
    };
    const TopicPinRegistration recorder{
        .topic_id = topic_id,
        .pin_id = TopicPinId{2},
        .kind = TopicPinKind::kRecorder,
        .generation = 1,
        .owner = Owner(node),
    };
    const TopicPinRegistration replay{
        .topic_id = topic_id,
        .pin_id = TopicPinId{3},
        .kind = TopicPinKind::kReplay,
        .generation = 1,
        .owner = Owner(node),
    };
    ASSERT_TRUE(coordinator->AcquireTopicPin(bridge, 1).ok());
    ASSERT_TRUE(coordinator->AcquireTopicPin(recorder, 1).ok());
    ASSERT_TRUE(coordinator->AcquireTopicPin(replay, 1).ok());
    ASSERT_TRUE(coordinator->DrainTopic(topic_id).ok());

    PublisherRegistration late = publisher;
    late.publisher_id = PublisherId{2};
    EXPECT_EQ(coordinator->RegisterPublisher(late, 2).code(),
              StatusCode::kUnavailable);
    auto draining = coordinator->GetTopic(topic_id);
    ASSERT_TRUE(draining.ok());
    const DrainCompletionProof completion = DrainProof((*draining)->metadata);
    EXPECT_EQ(coordinator->RetireTopic(topic_id, completion).code(),
              StatusCode::kWouldBlock);

    ASSERT_TRUE(coordinator->UnregisterPublisher(publisher).ok());
    ASSERT_TRUE(coordinator->UnregisterSubscriber(subscriber).ok());
    ASSERT_TRUE(coordinator->ReleaseTopicPin(bridge).ok());
    ASSERT_TRUE(coordinator->ReleaseTopicPin(recorder).ok());
    ASSERT_TRUE(coordinator->RetireTopic(topic_id, completion).ok());
    EXPECT_EQ(coordinator->DeleteTopic(topic_id).code(), StatusCode::kWouldBlock);
    ASSERT_TRUE(coordinator->ReleaseTopicPin(replay).ok());
    ASSERT_TRUE(coordinator->DeleteTopic(topic_id).ok());
}

TEST(RegistrationConcurrencyTest, PublisherCountersRemainExact) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto coordinator = MakeCoordinator(probe);
    const NodeRegistration node = Node(NodeId{10}, 1001, 101, 1000);
    ASSERT_TRUE(coordinator->RegisterNode(node, 0).ok());
    TopicMetadata metadata = DiscoveryTopic("topic/concurrent-publishers");
    metadata.max_publishers = 64;
    const TopicId topic_id = CreateAndActivate(*coordinator, std::move(metadata));

    std::array<bool, 32> success{};
    std::vector<std::thread> threads;
    for (size_t i = 0; i < success.size(); ++i) {
        threads.emplace_back([&, i] {
            success[i] = coordinator
                             ->RegisterPublisher(
                                 PublisherRegistration{
                                     .topic_id = topic_id,
                                     .publisher_id = PublisherId{i + 1},
                                     .generation = 1,
                                     .owner = Owner(node),
                                 },
                                 1)
                             .ok();
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (bool value : success) {
        EXPECT_TRUE(value);
    }
    auto topic = coordinator->GetTopic(topic_id);
    ASSERT_TRUE(topic.ok());
    EXPECT_EQ((*topic)->usage.publishers, success.size());
}

TEST(DiscoveryTest, TracksUniqueSubscriberNodesAndLeaseCleanupVersions) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto coordinator = MakeCoordinator(probe);
    const NodeRegistration first_node = Node(NodeId{5}, 501, 51, 10);
    const NodeRegistration second_node = Node(NodeId{6}, 601, 61, 1000);
    ASSERT_TRUE(coordinator->RegisterNode(first_node, 0).ok());
    ASSERT_TRUE(coordinator->RegisterNode(second_node, 0).ok());
    const TopicId topic_id =
        CreateAndActivate(*coordinator, DiscoveryTopic("topic/discovery"));

    const SubscriberRegistration first{
        .topic_id = topic_id,
        .subscriber_id = SubscriberId{1},
        .generation = 1,
        .owner = Owner(first_node),
    };
    SubscriberRegistration same_node = first;
    same_node.subscriber_id = SubscriberId{2};
    SubscriberRegistration other_node = first;
    other_node.subscriber_id = SubscriberId{3};
    other_node.owner = Owner(second_node);
    ASSERT_TRUE(coordinator->RegisterSubscriber(first, 1).ok());
    auto one = coordinator->DiscoverySubscriberNodes(topic_id);
    ASSERT_TRUE(one.ok());
    ASSERT_TRUE(coordinator->RegisterSubscriber(same_node, 1).ok());
    auto still_one = coordinator->DiscoverySubscriberNodes(topic_id);
    ASSERT_TRUE(still_one.ok());
    EXPECT_EQ((*still_one)->version, (*one)->version);
    ASSERT_TRUE(coordinator->RegisterSubscriber(other_node, 1).ok());
    auto two = coordinator->DiscoverySubscriberNodes(topic_id);
    ASSERT_TRUE(two.ok());
    EXPECT_EQ((*two)->nodes.size(), 2u);
    EXPECT_GT((*two)->version, (*still_one)->version);

    probe->Set(first_node.process_identity, ProcessIdentityLiveness::kDead);
    auto swept = coordinator->SweepExpiredNodes(11);
    ASSERT_TRUE(swept.ok());
    EXPECT_EQ(swept->subscribers_removed, 2u);
    auto after = coordinator->DiscoverySubscriberNodes(topic_id);
    ASSERT_TRUE(after.ok());
    ASSERT_EQ((*after)->nodes.size(), 1u);
    EXPECT_EQ((*after)->nodes[0], second_node.node_id);
    EXPECT_GT((*after)->version, (*two)->version);
}

TEST(RoutingTest, StaticRoutesIgnoreDiscoveryMembership) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto coordinator = MakeCoordinator(probe);
    const NodeRegistration target = Node(NodeId{7}, 701, 71, 1000);
    const NodeRegistration subscriber_node = Node(NodeId{8}, 801, 81, 1000);
    ASSERT_TRUE(coordinator->RegisterNode(target, 0).ok());
    ASSERT_TRUE(coordinator->RegisterNode(subscriber_node, 0).ok());
    const TopicId topic_id = CreateAndActivate(
        *coordinator, StaticTopic("topic/static", target.node_id));
    auto before = coordinator->ResolveRoutes(topic_id);
    ASSERT_TRUE(before.ok());
    ASSERT_EQ((*before)->routes.size(), 1u);
    EXPECT_EQ((*before)->routes[0].target_node, target.node_id);

    const SubscriberRegistration subscriber{
        .topic_id = topic_id,
        .subscriber_id = SubscriberId{1},
        .generation = 1,
        .owner = Owner(subscriber_node),
    };
    ASSERT_TRUE(coordinator->RegisterSubscriber(subscriber, 1).ok());
    auto after = coordinator->ResolveRoutes(topic_id);
    ASSERT_TRUE(after.ok());
    EXPECT_EQ((*after)->version, (*before)->version);
    ASSERT_EQ((*after)->routes.size(), 1u);
    EXPECT_EQ((*after)->routes[0].target_node, target.node_id);
    EXPECT_EQ(coordinator->DiscoverySubscriberNodes(topic_id).status().code(),
              StatusCode::kUnsupported);
}

TEST(ConfigUpdateTest, UsesCasSnapshotsAndRejectsInPlaceAbiChanges) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto coordinator = MakeCoordinator(probe);
    const NodeRegistration target = Node(NodeId{9}, 901, 91, 1000);
    ASSERT_TRUE(coordinator->RegisterNode(target, 0).ok());
    const TopicId topic_id =
        CreateAndActivate(*coordinator, DiscoveryTopic("topic/config"));
    auto old = coordinator->GetTopic(topic_id);
    ASSERT_TRUE(old.ok());

    TopicMetadata replacement = (*old)->metadata;
    replacement.name = "topic/config-renamed";
    ++replacement.config_version;
    ASSERT_TRUE(coordinator->UpdateTopic(replacement,
                                         (*old)->metadata.config_version)
                    .ok());
    auto current = coordinator->GetTopic(topic_id);
    ASSERT_TRUE(current.ok());
    EXPECT_EQ((*old)->metadata.name, "topic/config");
    EXPECT_EQ((*current)->metadata.name, "topic/config-renamed");
    EXPECT_EQ(coordinator->UpdateTopic(replacement,
                                       (*old)->metadata.config_version)
                  .code(),
              StatusCode::kAlreadyExists);

    TopicMetadata capacity_change = (*current)->metadata;
    capacity_change.capacity *= 2;
    ++capacity_change.config_version;
    EXPECT_EQ(coordinator
                  ->UpdateTopic(capacity_change,
                                (*current)->metadata.config_version)
                  .code(),
              StatusCode::kUnsupported);

    TopicMetadata schema_change = (*current)->metadata;
    schema_change.schema = Schema(8, 2);
    ++schema_change.config_version;
    EXPECT_EQ(coordinator
                  ->UpdateTopic(schema_change,
                                (*current)->metadata.config_version)
                  .code(),
              StatusCode::kUnsupported);

    TopicMetadata route_change = (*current)->metadata;
    route_change.route_policy = RoutePolicy::kStatic;
    route_change.static_routes = {{.target_node = target.node_id,
                                   .preferred_transport = std::nullopt}};
    ++route_change.route_set_version;
    ++route_change.config_version;
    ASSERT_TRUE(coordinator
                    ->UpdateTopic(route_change,
                                  (*current)->metadata.config_version)
                    .ok());
    auto routes = coordinator->ResolveRoutes(topic_id);
    ASSERT_TRUE(routes.ok());
    EXPECT_EQ((*routes)->policy, RoutePolicy::kStatic);
    EXPECT_EQ((*routes)->version, route_change.route_set_version);
}

TEST(PlatformLivenessProbeTest, RemoteNodeIdentityIsAlwaysUnknown) {
    ProcessIdentity remote = ProcessIdentity::Current();
    remote.node_id = remote.node_id == std::numeric_limits<uint64_t>::max()
                         ? remote.node_id - 1
                         : remote.node_id + 1;
    ASSERT_NE(remote.node_id, ProcessIdentity::Current().node_id);
    PlatformLivenessProbe probe;
    EXPECT_EQ(probe.Probe(remote), ProcessIdentityLiveness::kUnknown);
}

TEST(NodeRegistryLockingTest, LivenessProbeCanReenterRegistry) {
    auto probe = std::make_shared<ReentrantLivenessProbe>();
    auto registry_result = NodeRegistry::Create({}, probe);
    ASSERT_TRUE(registry_result.ok());
    auto registry = std::move(*registry_result);
    probe->SetRegistry(registry.get());

    const NodeRegistration old_process = Node(NodeId{20}, 2001, 201, 10);
    ASSERT_TRUE(registry->Register(old_process, 0).ok());
    const NodeRegistration replacement = Node(NodeId{20}, 2002, 202, 10);
    ASSERT_TRUE(registry->Register(replacement, 11).ok());
    EXPECT_TRUE(probe->called());
}

TEST(IdAllocatorTest, ProductionRequiresExplicitDurability) {
    EXPECT_EQ(Coordinator::Create().status().code(),
              StatusCode::kInvalidArgument);
    auto ephemeral = std::make_shared<InMemoryMonotonicIdAllocator>();
    EXPECT_EQ(Coordinator::Create({}, ephemeral).status().code(),
              StatusCode::kUnsupported);

    auto storage = std::make_shared<DurableHighWatermark>();
    auto durable = std::make_shared<DurableTestIdAllocator>(storage);
    EXPECT_TRUE(Coordinator::Create({}, durable).ok());
    EXPECT_TRUE(Coordinator::CreateForTesting().ok());
}

TEST(IdAllocatorTest, PersistsBeforePublishAndBurnsFailedIdsAcrossRestart) {
    auto storage = std::make_shared<DurableHighWatermark>();
    TopicId first_id;
    {
        auto allocator = std::make_shared<DurableTestIdAllocator>(storage);
        auto coordinator_result = Coordinator::Create({}, allocator);
        ASSERT_TRUE(coordinator_result.ok());
        auto coordinator = std::move(*coordinator_result);
        auto first = coordinator->CreateTopic(DiscoveryTopic("durable/first"));
        ASSERT_TRUE(first.ok());
        first_id = (*first)->metadata.topic_id;

        allocator->FailNextAfterPersist();
        auto failed = coordinator->CreateTopic(DiscoveryTopic("durable/burned"));
        ASSERT_FALSE(failed.ok());
        EXPECT_EQ(failed.status().code(), StatusCode::kUnavailable);
        EXPECT_EQ(allocator->high_watermark(), first_id.value + 1);
    }

    auto restarted_allocator =
        std::make_shared<DurableTestIdAllocator>(storage);
    auto restarted_result = Coordinator::Create({}, restarted_allocator);
    ASSERT_TRUE(restarted_result.ok());
    auto restarted = std::move(*restarted_result);
    auto after_restart =
        restarted->CreateTopic(DiscoveryTopic("durable/after-restart"));
    ASSERT_TRUE(after_restart.ok());
    EXPECT_EQ((*after_restart)->metadata.topic_id.value, first_id.value + 2);
}

TEST(IdAllocatorTest, ConcurrentCandidatesAreUniqueAndAllocatorCanReenter) {
    auto storage = std::make_shared<DurableHighWatermark>();
    auto allocator = std::make_shared<DurableTestIdAllocator>(storage);
    auto coordinator_result = Coordinator::Create({}, allocator);
    ASSERT_TRUE(coordinator_result.ok());
    auto coordinator = std::move(*coordinator_result);

    allocator->Reenter(coordinator.get());
    auto reentrant = coordinator->CreateTopic(DiscoveryTopic("durable/reentrant"));
    ASSERT_TRUE(reentrant.ok());
    EXPECT_TRUE(allocator->reentered());
    allocator->Reenter(nullptr);

    std::array<TopicId, 16> ids{};
    std::array<bool, 16> success{};
    std::vector<std::thread> threads;
    for (size_t i = 0; i < ids.size(); ++i) {
        threads.emplace_back([&, i] {
            auto created = coordinator->CreateTopic(
                DiscoveryTopic("durable/concurrent-" + std::to_string(i)));
            success[i] = created.ok();
            if (created.ok()) {
                ids[i] = (*created)->metadata.topic_id;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (size_t i = 0; i < ids.size(); ++i) {
        ASSERT_TRUE(success[i]);
        for (size_t j = 0; j < i; ++j) {
            EXPECT_NE(ids[i], ids[j]);
        }
    }
}

TEST(TopicProofTest, RejectsStaleOrMismatchedLifecycleProofs) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto coordinator = MakeCoordinator(probe);
    auto created = coordinator->CreateTopic(DiscoveryTopic("topic/proofs"));
    ASSERT_TRUE(created.ok());

    ActivationReadinessProof wrong = ActivationProof((*created)->metadata);
    ++wrong.config_version;
    EXPECT_EQ(coordinator->ActivateTopic((*created)->metadata.topic_id, wrong).code(),
              StatusCode::kAlreadyExists);
    wrong = ActivationProof((*created)->metadata);
    wrong.schema = Schema(99);
    EXPECT_EQ(coordinator->ActivateTopic((*created)->metadata.topic_id, wrong).code(),
              StatusCode::kAlreadyExists);
    ASSERT_TRUE(coordinator
                    ->ActivateTopic((*created)->metadata.topic_id,
                                    ActivationProof((*created)->metadata))
                    .ok());
    ASSERT_TRUE(coordinator->DrainTopic((*created)->metadata.topic_id).ok());

    auto draining = coordinator->GetTopic((*created)->metadata.topic_id);
    ASSERT_TRUE(draining.ok());
    DrainCompletionProof incomplete = DrainProof((*draining)->metadata);
    incomplete.borrows_released = false;
    EXPECT_EQ(coordinator
                  ->RetireTopic((*created)->metadata.topic_id, incomplete)
                  .code(),
              StatusCode::kUnavailable);
    DrainCompletionProof stale = DrainProof((*draining)->metadata);
    ++stale.channel_version;
    EXPECT_EQ(coordinator->RetireTopic((*created)->metadata.topic_id, stale).code(),
              StatusCode::kAlreadyExists);
    EXPECT_TRUE(coordinator
                    ->RetireTopic((*created)->metadata.topic_id,
                                  DrainProof((*draining)->metadata))
                    .ok());
}

TEST(TopicPinTest, ReleaseRequiresExactIdOwnerGenerationAndIsNotRepeatable) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto coordinator = MakeCoordinator(probe);
    const NodeRegistration first_node = Node(NodeId{21}, 2101, 211, 1000);
    const NodeRegistration second_node = Node(NodeId{22}, 2201, 221, 1000);
    ASSERT_TRUE(coordinator->RegisterNode(first_node, 0).ok());
    ASSERT_TRUE(coordinator->RegisterNode(second_node, 0).ok());
    const TopicId topic_id =
        CreateAndActivate(*coordinator, DiscoveryTopic("topic/exact-pin"));
    const TopicPinRegistration token{
        .topic_id = topic_id,
        .pin_id = TopicPinId{77},
        .kind = TopicPinKind::kBridge,
        .generation = 3,
        .owner = Owner(first_node),
    };
    ASSERT_TRUE(coordinator->AcquireTopicPin(token, 1).ok());
    ASSERT_TRUE(coordinator->AcquireTopicPin(token, 1).ok());

    TopicPinRegistration stale = token;
    --stale.generation;
    EXPECT_EQ(coordinator->ReleaseTopicPin(stale).code(), StatusCode::kNotFound);
    TopicPinRegistration other = token;
    other.owner = Owner(second_node);
    EXPECT_EQ(coordinator->ReleaseTopicPin(other).code(), StatusCode::kNotFound);
    EXPECT_TRUE(coordinator->ReleaseTopicPin(token).ok());
    EXPECT_EQ(coordinator->ReleaseTopicPin(token).code(), StatusCode::kNotFound);
    auto topic = coordinator->GetTopic(topic_id);
    ASSERT_TRUE(topic.ok());
    EXPECT_EQ((*topic)->usage.bridges, 0u);
}

TEST(OwnerCleanupTest, BadAllocAfterNodeReplacementIsRetriedIdempotently) {
    auto probe = std::make_shared<FakeLivenessProbe>();
    auto faults = std::make_shared<RegistryFaultInjector>();
    auto coordinator_result = Coordinator::CreateForTesting(
        {}, std::make_shared<InMemoryMonotonicIdAllocator>(), probe, faults);
    ASSERT_TRUE(coordinator_result.ok());
    auto coordinator = std::move(*coordinator_result);

    const NodeRegistration old_process = Node(NodeId{23}, 2301, 231, 10);
    ASSERT_TRUE(coordinator->RegisterNode(old_process, 0).ok());
    const TopicId topic_id =
        CreateAndActivate(*coordinator, DiscoveryTopic("topic/cleanup-retry"));
    const PublisherRegistration publisher{
        .topic_id = topic_id,
        .publisher_id = PublisherId{1},
        .generation = 1,
        .owner = Owner(old_process),
    };
    const SubscriberRegistration subscriber{
        .topic_id = topic_id,
        .subscriber_id = SubscriberId{1},
        .generation = 1,
        .owner = Owner(old_process),
    };
    const TopicPinRegistration pin{
        .topic_id = topic_id,
        .pin_id = TopicPinId{1},
        .kind = TopicPinKind::kRecorder,
        .generation = 1,
        .owner = Owner(old_process),
    };
    ASSERT_TRUE(coordinator->RegisterPublisher(publisher, 1).ok());
    ASSERT_TRUE(coordinator->RegisterSubscriber(subscriber, 1).ok());
    ASSERT_TRUE(coordinator->AcquireTopicPin(pin, 1).ok());

    probe->Set(old_process.process_identity, ProcessIdentityLiveness::kDead);
    faults->FailOwnerCleanupAllocationAfter(1);
    const NodeRegistration replacement = Node(NodeId{23}, 2302, 232, 10);
    auto interrupted = coordinator->RegisterNode(replacement, 11);
    ASSERT_FALSE(interrupted.ok());
    EXPECT_EQ(interrupted.status().code(), StatusCode::kResourceExhausted);
    auto partially_cleaned = coordinator->GetTopic(topic_id);
    ASSERT_TRUE(partially_cleaned.ok());
    EXPECT_EQ((*partially_cleaned)->usage.publishers, 0u);
    EXPECT_EQ((*partially_cleaned)->usage.subscribers, 1u);
    EXPECT_EQ((*partially_cleaned)->usage.recorders, 1u);
    auto current_node = coordinator->GetNode(replacement.node_id);
    ASSERT_TRUE(current_node.ok());
    EXPECT_EQ((*current_node)->process_identity, replacement.process_identity);

    auto retried = coordinator->SweepExpiredNodes(12);
    ASSERT_TRUE(retried.ok()) << retried.status().ToString();
    auto topic = coordinator->GetTopic(topic_id);
    ASSERT_TRUE(topic.ok());
    EXPECT_TRUE((*topic)->usage.empty());
    auto nodes = coordinator->DiscoverySubscriberNodes(topic_id);
    ASSERT_TRUE(nodes.ok());
    EXPECT_TRUE((*nodes)->nodes.empty());
    EXPECT_EQ(coordinator->UnregisterPublisher(publisher).code(),
              StatusCode::kNotFound);
    EXPECT_EQ(coordinator->UnregisterSubscriber(subscriber).code(),
              StatusCode::kNotFound);
    EXPECT_EQ(coordinator->ReleaseTopicPin(pin).code(), StatusCode::kNotFound);
}

}  // namespace
}  // namespace mino::registry
