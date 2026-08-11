// Copyright 2026 The Mino Authors

#include "mino/transport/transport_switcher.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace mino::transport {
namespace {

EndpointDescriptor Endpoint(uint8_t host, uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0},
        static_cast<std::byte>(host)};
    auto endpoint = EndpointDescriptor::Ipv4Tcp(address, port);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    return endpoint.ok() ? *endpoint : EndpointDescriptor{};
}

EndpointDescriptor FabricEndpoint(uint32_t domain, uint32_t channel) {
    auto endpoint = EndpointDescriptor::SharedFabric(domain, channel);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    return endpoint.ok() ? *endpoint : EndpointDescriptor{};
}

ProcessIdentity Identity(NodeId node, uint64_t epoch) {
    return ProcessIdentity{
        .node_id = node.value,
        .process_id = 10'000 + epoch,
        .process_epoch = epoch,
        .start_time_ns = epoch * 100,
    };
}

registry::NodeRegistration Node(NodeId node, uint64_t epoch) {
    return registry::NodeRegistration{
        .node_id = node,
        .process_identity = Identity(node, epoch),
        .endpoints = {Endpoint(static_cast<uint8_t>(node.value),
                               static_cast<uint16_t>(5000 + node.value))},
        .security_domain_id = SecurityDomainId{1},
        .trust_domain = "switcher-test",
        .health = registry::NodeHealth::kHealthy,
        .lease_epoch = epoch,
        .lease_duration_ns = 1'000'000,
        .config_version = 1,
    };
}

schema::SchemaIdentity Schema(uint64_t short_id = 77) {
    schema::CanonicalDigest digest{};
    digest[0] = static_cast<std::byte>(short_id);
    digest[31] = std::byte{0x5a};
    return schema::SchemaIdentity(short_id, digest, 1, 1);
}

class TestDriver final : public TransportDriver {
public:
    explicit TestDriver(TransportCapabilities capabilities)
        : capabilities_(capabilities) {}

    HealthState health() const noexcept override {
        return health_.load(std::memory_order_acquire);
    }

    TransportCapabilities capabilities() const noexcept override {
        std::lock_guard lock(mutex_);
        return capabilities_;
    }

    void SetHealth(HealthState health) noexcept {
        health_.store(health, std::memory_order_release);
    }

    void SetReliability(TransportReliability reliability) {
        std::lock_guard lock(mutex_);
        capabilities_.reliability = reliability;
    }

    void SetMaxFrameSize(uint32_t max_frame_size) {
        std::lock_guard lock(mutex_);
        capabilities_.max_frame_size = max_frame_size;
    }

protected:
    Status DoStart(const DriverConfig&) override { return Status::Ok(); }
    Status DoShutdown() override { return Status::Ok(); }
    Result<ConnectionInfo> DoConnect(const ConnectRequest&) override {
        return Status::Error(StatusCode::kUnsupported);
    }
    Result<ConnectionInfo> DoListen(const ListenRequest&) override {
        return Status::Error(StatusCode::kUnsupported);
    }
    Result<SendResult> DoSend(const SendRequest&, SendOperation) override {
        return Status::Error(StatusCode::kUnsupported);
    }
    Result<ReceiveResult> DoPoll(const ReceiveRequest&) override {
        return Status::Error(StatusCode::kWouldBlock);
    }
    Result<CompletionPollResult> DoPollCompletions(
        const CompletionPollRequest&) override {
        return Status::Error(StatusCode::kWouldBlock);
    }
    Status DoClose(ConnectionId) override { return Status::Ok(); }

private:
    mutable std::mutex mutex_;
    TransportCapabilities capabilities_;
    std::atomic<HealthState> health_{HealthState::kHealthy};
};

class TestAccessValidator final : public RouteAccessValidator {
public:
    uint64_t version() const noexcept override {
        return version_.load(std::memory_order_acquire);
    }

    Status Validate(const registry::TopicMetadata&, NodeId source,
                    NodeId target) const override {
        calls_.fetch_add(1, std::memory_order_relaxed);
        if (source.value == 0 || target.value == 0) {
            return Status::Error(StatusCode::kInvalidArgument);
        }
        return allowed_.load(std::memory_order_acquire)
                   ? Status::Ok()
                   : Status::Error(StatusCode::kPermissionDenied,
                                   "route denied by test ACL");
    }

    void SetAllowed(bool allowed) noexcept {
        allowed_.store(allowed, std::memory_order_release);
        version_.fetch_add(1, std::memory_order_acq_rel);
    }

    uint64_t calls() const noexcept {
        return calls_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<uint64_t> version_{1};
    std::atomic<bool> allowed_{true};
    mutable std::atomic<uint64_t> calls_{0};
};

class TestSchemaValidator final : public SchemaRouteValidator {
public:
    uint64_t version() const noexcept override {
        return version_.load(std::memory_order_acquire);
    }

    Status Validate(const registry::TopicMetadata& topic, NodeId target,
                    const schema::SchemaIdentity& publisher_schema) const override {
        calls_.fetch_add(1, std::memory_order_relaxed);
        if (target.value == 0 ||
            !registry::SchemaIdentityEqual(topic.schema, publisher_schema)) {
            return Status::Error(StatusCode::kSchemaMismatch);
        }
        return allowed_.load(std::memory_order_acquire)
                   ? Status::Ok()
                   : Status::Error(StatusCode::kSchemaMismatch,
                                   "route schema is incompatible");
    }

    void SetAllowed(bool allowed) noexcept {
        allowed_.store(allowed, std::memory_order_release);
        version_.fetch_add(1, std::memory_order_acq_rel);
    }

    uint64_t calls() const noexcept {
        return calls_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<uint64_t> version_{1};
    std::atomic<bool> allowed_{true};
    mutable std::atomic<uint64_t> calls_{0};
};

class TestLocalBinding final : public LocalPublicationBinding {};

class TestLocalProvider final : public LocalRouteProvider {
public:
    TestLocalProvider()
        : binding_(std::make_shared<const TestLocalBinding>()) {}

    uint64_t version() const noexcept override {
        return version_.load(std::memory_order_acquire);
    }

    Result<std::shared_ptr<const LocalPublicationBinding>> Resolve(
        const registry::TopicMetadata&) const override {
        calls_.fetch_add(1, std::memory_order_relaxed);
        if (!available_.load(std::memory_order_acquire)) {
            return Status::Error(StatusCode::kUnavailable);
        }
        return binding_;
    }

    uint64_t calls() const noexcept {
        return calls_.load(std::memory_order_relaxed);
    }

private:
    std::shared_ptr<const LocalPublicationBinding> binding_;
    std::atomic<uint64_t> version_{1};
    std::atomic<bool> available_{true};
    mutable std::atomic<uint64_t> calls_{0};
};

TransportCapabilities ReliableNetworkCapabilities(uint32_t max_frame = 4096) {
    return TransportCapabilities{
        .kind = TransportKind::kNetwork,
        .reliability = TransportReliability::kReliable,
        .max_frame_size = max_frame,
        .max_reassembly_bytes = max_frame,
        .features = Capability::kConnect,
    };
}

class TransportSwitcherTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto created = registry::Coordinator::CreateForTesting();
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        coordinator_ = std::move(*created);
        for (uint64_t value = 1; value <= 3; ++value) {
            const NodeId node{value};
            const auto registered =
                coordinator_->RegisterNode(Node(node, value), 1);
            ASSERT_TRUE(registered.ok()) << registered.status().ToString();
        }

        access_ = std::make_shared<TestAccessValidator>();
        schema_ = std::make_shared<TestSchemaValidator>();
        local_ = std::make_shared<TestLocalProvider>();
        auto switcher = TransportSwitcher::Create(NodeId{1}, coordinator_.get(),
                                                   access_, schema_, local_);
        ASSERT_TRUE(switcher.ok()) << switcher.status().ToString();
        switcher_ = std::move(*switcher);
    }

    TopicId CreateTopic(registry::RoutePolicy policy,
                        std::vector<registry::StaticRouteEntry> routes = {},
                        registry::Reliability reliability =
                            registry::Reliability::kBestEffort,
                        uint32_t source_permissions =
                            registry::kAllTopicPermissions) {
        registry::TopicMetadata topic{
            .topic_id = {},
            .name = "switcher/topic" + std::to_string(next_topic_name_++),
            .channel_kind = registry::ChannelKind::kBroadcast,
            .delivery = {.reliability = reliability, .allow_drop = false},
            .queue_full_policy = QueueFullPolicy::kBlock,
            .schema = Schema(),
            .accepted_schemas = {},
            .route_policy = policy,
            .static_routes = std::move(routes),
            .route_set_version = 0,
            .capacity = 1024,
            .max_publishers = 8,
            .max_subscribers = 8,
            .partition_count = 1,
            .record_topology =
                registry::RecordBackpressureTopology::kIsolated,
            .acl = registry::TopicAcl{
                .entries = {
                    {.node_id = NodeId{1},
                     .security_domain_id = SecurityDomainId{1},
                     .permissions = source_permissions},
                    {.node_id = NodeId{2},
                     .security_domain_id = SecurityDomainId{1},
                     .permissions = registry::kAllTopicPermissions},
                    {.node_id = NodeId{3},
                     .security_domain_id = SecurityDomainId{1},
                     .permissions = registry::kAllTopicPermissions},
                },
            },
            .region_version = 1,
            .channel_version = 1,
            .acl_version = 1,
            .config_version = 0,
            .state = registry::TopicState::kCreating,
        };
        auto created = coordinator_->CreateTopic(std::move(topic));
        EXPECT_TRUE(created.ok()) << created.status().ToString();
        if (!created.ok()) return {};
        const registry::TopicMetadata& metadata = (*created)->metadata;
        const registry::ActivationReadinessProof proof{
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
        EXPECT_TRUE(coordinator_->ActivateTopic(metadata.topic_id, proof).ok());
        return metadata.topic_id;
    }

    std::shared_ptr<TestDriver> RegisterNetworkDriver(
        uint64_t id = 1, uint64_t generation = 1, uint32_t priority = 10,
        uint32_t max_frame = 4096) {
        auto driver =
            std::make_shared<TestDriver>(ReliableNetworkCapabilities(max_frame));
        EXPECT_TRUE(driver->Start({}).ok());
        EXPECT_TRUE(switcher_
                        ->RegisterDriver(DriverRegistration{
                            .driver_id = id,
                            .generation = generation,
                            .selection_priority = priority,
                            .driver = driver,
                            .endpoint_matcher =
                                std::make_shared<TransportKindEndpointMatcher>(
                                    TransportKind::kNetwork),
                        })
                        .ok());
        return driver;
    }

    RouteRequest Request(TopicId topic_id, uint32_t payload = 128) {
        auto topic = coordinator_->GetTopic(topic_id);
        EXPECT_TRUE(topic.ok()) << topic.status().ToString();
        if (!topic.ok()) {
            return RouteRequest{
                .topic_id = topic_id,
                .payload_size = payload,
                .delivery = {},
                .priority = 0,
                .publisher_schema = Schema(),
            };
        }
        return RouteRequest{
            .topic_id = topic_id,
            .payload_size = payload,
            .delivery = (*topic)->metadata.delivery,
            .priority = 3,
            .publisher_schema = (*topic)->metadata.schema,
        };
    }

    registry::NodeLeaseOwner Owner(NodeId node, uint64_t epoch) const {
        return registry::NodeLeaseOwner{
            .node_id = node,
            .process_identity = Identity(node, epoch),
            .lease_epoch = epoch,
        };
    }

    std::unique_ptr<registry::Coordinator> coordinator_;
    std::shared_ptr<TestAccessValidator> access_;
    std::shared_ptr<TestSchemaValidator> schema_;
    std::shared_ptr<TestLocalProvider> local_;
    std::unique_ptr<TransportSwitcher> switcher_;
    uint32_t next_topic_name_ = 1;
};

TEST_F(TransportSwitcherTest, ProductionCreationRequiresAllValidatorsAndProviders) {
    auto missing_access = TransportSwitcher::Create(
        NodeId{1}, coordinator_.get(), nullptr, schema_, local_);
    ASSERT_FALSE(missing_access.ok());
    EXPECT_EQ(missing_access.status().code(), StatusCode::kInvalidArgument);

    auto missing_schema = TransportSwitcher::Create(
        NodeId{1}, coordinator_.get(), access_, nullptr, local_);
    ASSERT_FALSE(missing_schema.ok());
    EXPECT_EQ(missing_schema.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(TransportSwitcherTest, StaticRemoteRouteChecksDynamicDriverConstraints) {
    auto driver = RegisterNetworkDriver();
    const TopicId topic = CreateTopic(
        registry::RoutePolicy::kStatic,
        {{.target_node = NodeId{2}, .preferred_transport = std::nullopt}},
        registry::Reliability::kReliableOrdered);

    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto route = switcher_->Resolve(Request(topic, 1024));
    ASSERT_TRUE(route.ok()) << route.status().ToString();
    ASSERT_EQ((*route)->targets().size(), 1u);
    const auto* remote =
        std::get_if<RemoteTargetRoute>(&(*route)->targets()[0].transport);
    ASSERT_NE(remote, nullptr);
    EXPECT_EQ(remote->driver_id, 1u);
    EXPECT_EQ(remote->node_config_version, 1u);
    EXPECT_EQ((*route)->stamp().policy, registry::RoutePolicy::kStatic);

    driver->SetMaxFrameSize(512);
    auto oversized = switcher_->Resolve(Request(topic, 1024));
    ASSERT_FALSE(oversized.ok());
    EXPECT_EQ(oversized.status().code(), StatusCode::kResourceExhausted);

    driver->SetMaxFrameSize(4096);
    driver->SetReliability(TransportReliability::kOrderedLossy);
    auto unreliable = switcher_->Resolve(Request(topic));
    ASSERT_FALSE(unreliable.ok());
    EXPECT_EQ(unreliable.status().code(), StatusCode::kUnsupported);

    driver->SetReliability(TransportReliability::kReliable);
    driver->SetHealth(HealthState::kUnavailable);
    auto unhealthy = switcher_->Resolve(Request(topic));
    ASSERT_FALSE(unhealthy.ok());
    EXPECT_EQ(unhealthy.status().code(), StatusCode::kUnavailable);

    driver->SetHealth(static_cast<HealthState>(0xff));
    auto invalid_health = switcher_->Resolve(Request(topic));
    ASSERT_FALSE(invalid_health.ok());
    EXPECT_EQ(invalid_health.status().code(), StatusCode::kUnavailable);
}

TEST_F(TransportSwitcherTest, MetadataAclCannotBeBypassedByCustomValidator) {
    RegisterNetworkDriver();
    const TopicId topic = CreateTopic(
        registry::RoutePolicy::kStatic,
        {{.target_node = NodeId{2}, .preferred_transport = std::nullopt}},
        registry::Reliability::kBestEffort,
        static_cast<uint32_t>(registry::TopicPermission::kSubscribe));

    const Status denied = switcher_->RefreshTopic(topic);
    EXPECT_EQ(denied.code(), StatusCode::kPermissionDenied);
    EXPECT_EQ(access_->calls(), 0u);
    EXPECT_EQ(switcher_->Resolve(Request(topic)).status().code(),
              StatusCode::kUnavailable);
}

TEST_F(TransportSwitcherTest, LocalTargetUsesShmBindingBeforeNetwork) {
    RegisterNetworkDriver();
    const TopicId topic = CreateTopic(
        registry::RoutePolicy::kStatic,
        {{.target_node = NodeId{1}, .preferred_transport = std::nullopt}});

    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto route = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(route.ok()) << route.status().ToString();
    ASSERT_EQ((*route)->targets().size(), 1u);
    EXPECT_NE(std::get_if<LocalTargetRoute>(
                  &(*route)->targets()[0].transport),
              nullptr);
    EXPECT_EQ(local_->calls(), 1u);
    EXPECT_EQ(access_->calls(), 1u);
    EXPECT_EQ(schema_->calls(), 1u);
}

TEST_F(TransportSwitcherTest, DiscoveryMembershipChangesOnlyAfterRefresh) {
    RegisterNetworkDriver();
    const TopicId topic = CreateTopic(registry::RoutePolicy::kDiscovery);
    ASSERT_TRUE(coordinator_
                    ->RegisterSubscriber(
                        registry::SubscriberRegistration{
                            .topic_id = topic,
                            .subscriber_id = SubscriberId{1},
                            .generation = 1,
                            .owner = Owner(NodeId{2}, 2),
                        },
                        2)
                    .ok());

    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto first = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(first.ok());
    ASSERT_EQ((*first)->targets().size(), 1u);
    const uint64_t first_version = (*first)->stamp().route_version;

    ASSERT_TRUE(coordinator_
                    ->RegisterSubscriber(
                        registry::SubscriberRegistration{
                            .topic_id = topic,
                            .subscriber_id = SubscriberId{2},
                            .generation = 1,
                            .owner = Owner(NodeId{3}, 3),
                        },
                        2)
                    .ok());
    auto still_frozen = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(still_frozen.ok());
    EXPECT_EQ((*still_frozen)->targets().size(), 1u);

    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto refreshed = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(refreshed.ok());
    EXPECT_EQ((*refreshed)->targets().size(), 2u);
    EXPECT_GT((*refreshed)->stamp().route_version, first_version);
}

TEST_F(TransportSwitcherTest, StaticRouteVersionIgnoresSubscriberMembership) {
    RegisterNetworkDriver();
    const TopicId topic = CreateTopic(
        registry::RoutePolicy::kStatic,
        {{.target_node = NodeId{2}, .preferred_transport = std::nullopt}});
    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto before = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(before.ok());

    ASSERT_TRUE(coordinator_
                    ->RegisterSubscriber(
                        registry::SubscriberRegistration{
                            .topic_id = topic,
                            .subscriber_id = SubscriberId{8},
                            .generation = 1,
                            .owner = Owner(NodeId{3}, 3),
                        },
                        2)
                    .ok());
    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto after = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(after.ok());
    EXPECT_EQ((*after)->stamp(), (*before)->stamp());
    ASSERT_EQ((*after)->targets().size(), 1u);
    EXPECT_EQ((*after)->targets()[0].target_node, NodeId{2});
}

TEST_F(TransportSwitcherTest, ValidatorVersionsFailClosedAndFailedRefreshInvalidates) {
    RegisterNetworkDriver();
    const TopicId topic = CreateTopic(
        registry::RoutePolicy::kStatic,
        {{.target_node = NodeId{2}, .preferred_transport = std::nullopt}});
    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());

    access_->SetAllowed(false);
    auto stale = switcher_->Resolve(Request(topic));
    ASSERT_FALSE(stale.ok());
    EXPECT_EQ(stale.status().code(), StatusCode::kUnavailable);

    const Status denied = switcher_->RefreshTopic(topic);
    EXPECT_EQ(denied.code(), StatusCode::kPermissionDenied);
    auto invalidated = switcher_->Resolve(Request(topic));
    ASSERT_FALSE(invalidated.ok());
    EXPECT_EQ(invalidated.status().code(), StatusCode::kUnavailable);

    access_->SetAllowed(true);
    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    RouteRequest wrong_schema = Request(topic);
    wrong_schema.publisher_schema = Schema(99);
    auto mismatch = switcher_->Resolve(wrong_schema);
    ASSERT_FALSE(mismatch.ok());
    EXPECT_EQ(mismatch.status().code(), StatusCode::kSchemaMismatch);
}

TEST_F(TransportSwitcherTest, NodeRuntimeVersionRefreshesAndUnavailableNodeInvalidates) {
    RegisterNetworkDriver();
    const TopicId topic = CreateTopic(
        registry::RoutePolicy::kStatic,
        {{.target_node = NodeId{2}, .preferred_transport = std::nullopt}});
    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto before = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(before.ok());

    ASSERT_TRUE(coordinator_
                    ->HeartbeatNode(Owner(NodeId{2}, 2),
                                    registry::NodeHealth::kDegraded, 2)
                    .ok());
    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto after = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(after.ok());
    EXPECT_GT((*after)->stamp().node_registry_version,
              (*before)->stamp().node_registry_version);

    ASSERT_TRUE(coordinator_
                    ->HeartbeatNode(Owner(NodeId{2}, 2),
                                    registry::NodeHealth::kUnavailable, 3)
                    .ok());
    EXPECT_EQ(switcher_->RefreshTopic(topic).code(), StatusCode::kUnavailable);
    EXPECT_FALSE(switcher_->Resolve(Request(topic)).ok());
}

TEST_F(TransportSwitcherTest, DriverUnregisterInvalidatesCacheButFrozenHandleOwnsDriver) {
    auto driver = RegisterNetworkDriver(9, 4);
    std::weak_ptr<TestDriver> weak = driver;
    const TopicId topic = CreateTopic(
        registry::RoutePolicy::kStatic,
        {{.target_node = NodeId{2}, .preferred_transport = std::nullopt}});
    ASSERT_TRUE(switcher_->RefreshTopic(topic).ok());
    auto route = switcher_->Resolve(Request(topic));
    ASSERT_TRUE(route.ok());

    EXPECT_EQ(switcher_->UnregisterDriver(9, 3).code(), StatusCode::kNotFound);
    ASSERT_TRUE(switcher_->UnregisterDriver(9, 4).ok());
    EXPECT_FALSE(switcher_->Resolve(Request(topic)).ok());

    driver.reset();
    EXPECT_FALSE(weak.expired());
    route = Status::Error(StatusCode::kUnavailable);
    EXPECT_TRUE(weak.expired());
}

TEST_F(TransportSwitcherTest, StaticPreferredTransportIsAHardConstraint) {
    RegisterNetworkDriver();
    registry::NodeRegistration replacement = Node(NodeId{2}, 2);
    replacement.endpoints.push_back(FabricEndpoint(7, 9));
    replacement.config_version = 2;
    ASSERT_TRUE(coordinator_->UpdateNode(replacement, 1, 2).ok());
    const TopicId topic = CreateTopic(
        registry::RoutePolicy::kStatic,
        {{.target_node = NodeId{2},
          .preferred_transport = TransportKind::kSharedFabric}});
    const Status refreshed = switcher_->RefreshTopic(topic);
    EXPECT_EQ(refreshed.code(), StatusCode::kUnsupported);
    EXPECT_FALSE(switcher_->Resolve(Request(topic)).ok());
}

}  // namespace
}  // namespace mino::transport
