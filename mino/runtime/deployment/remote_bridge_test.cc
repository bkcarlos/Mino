// Copyright 2026 The Mino Authors

#include "mino/runtime/deployment/remote_bridge.h"
#include "mino/runtime/deployment/remote_bridge_test_helper.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"

namespace mino::deployment {
namespace {

class AcceptingAuth final : public bridge::DescriptorAuth {
public:
    Status Authenticate(const schema::SchemaIdentity&,
                        std::span<const std::byte>) override {
        return Status::Ok();
    }
};

class SinkIngress final : public bridge::BridgeIngressPort {
public:
    Status DecodeValidatePublish(const bridge::WireFrame&) override {
        return Status::Ok();
    }
};

class AllowAllTopics final : public bridge::BridgeTopicAuthorizer {
public:
    Status AuthorizeInbound(const security::AuthenticatedPeer&,
                            TopicId) const noexcept override {
        return Status::Ok();
    }
};

class FakeAuthorizerClock final : public CoordinatorTopicAuthorizerClock {
public:
    explicit FakeAuthorizerClock(uint64_t now_ns) noexcept : now_ns_(now_ns) {}
    uint64_t NowNs() const noexcept override {
        return now_ns_.load(std::memory_order_acquire);
    }
    void Set(uint64_t now_ns) noexcept {
        now_ns_.store(now_ns, std::memory_order_release);
    }

private:
    std::atomic<uint64_t> now_ns_;
};

ProcessIdentity Process(NodeId node, uint64_t value) {
    return ProcessIdentity{
        .node_id = node.value,
        .process_id = value,
        .process_epoch = value + 1,
        .start_time_ns = value + 2,
    };
}

bridge::BridgeNodeIdentityFence Fence(NodeId node, uint64_t value) {
    return bridge::BridgeNodeIdentityFence{
        .node_id = node,
        .process_identity = Process(node, value),
        .lease_epoch = value + 3,
        .node_config_version = value + 4,
    };
}

struct TestArtifact {
    schema::SchemaIdentity identity;
    std::string bytes;
};

Result<TestArtifact> CompileArtifact(std::string_view idl) {
    MINO_ASSIGN_OR_RETURN(auto compiled, schema::SchemaCompiler::Compile(idl));
    if (compiled.types().size() != 1) {
        return Status::Error(StatusCode::kCorruption,
                             "test artifact must contain one schema");
    }
    MINO_ASSIGN_OR_RETURN(auto layout,
                          schema::LayoutPlanner::Plan(*compiled.types().front()));
    const std::array<schema::LayoutPlan, 1> layouts = {std::move(layout)};
    MINO_ASSIGN_OR_RETURN(
        auto bytes,
        schema::codegen::EncodeDescriptorArtifact(compiled, layouts));
    return TestArtifact{compiled.types().front()->identity(), std::move(bytes)};
}

std::span<const std::byte> Bytes(std::string_view value) {
    return std::as_bytes(std::span<const char>(value.data(), value.size()));
}

bridge::SourceIdentity SourceForLane(uint16_t lane_index,
                                     uint16_t lane_count) {
    for (uint64_t publisher_id = 1; publisher_id != 100'000; ++publisher_id) {
        const bridge::SourceIdentity source{101, publisher_id, 7001};
        if (bridge::BridgeLaneFor(source, lane_count) == lane_index) {
            return source;
        }
    }
    return {};
}

bridge::WireFrame FrameFor(const bridge::SourceIdentity& source,
                           uint64_t sequence) {
    bridge::WireFrame frame;
    frame.header.source_node_id = source.node_id;
    frame.header.source_publisher_id = source.publisher_id;
    frame.header.source_publisher_epoch = source.publisher_epoch;
    frame.header.sequence_num = sequence;
    frame.payload = {std::byte{0x5a}};
    return frame;
}

std::filesystem::path StoreRoot() {
    static std::atomic<uint64_t> sequence{0};
    const char* test_tmpdir = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        test_tmpdir == nullptr ? std::filesystem::temp_directory_path()
                               : std::filesystem::path(test_tmpdir);
    return base / ("mino_remote_bridge_" + std::to_string(::getpid()) + "_" +
                   std::to_string(sequence.fetch_add(1)));
}

registry::TopicMetadata AuthorizedTopic() {
    schema::CanonicalDigest digest{};
    digest[0] = std::byte{1};
    registry::TopicMetadata topic;
    topic.name = "deployment/authorized";
    topic.channel_kind = registry::ChannelKind::kBroadcast;
    topic.schema = schema::SchemaIdentity(1, digest, 1, 1);
    topic.capacity = 16;
    topic.max_publishers = 1;
    topic.max_subscribers = 1;
    topic.acl.entries = {{
        .node_id = NodeId{101},
        .security_domain_id = SecurityDomainId{77},
        .permissions = static_cast<uint32_t>(
                           registry::TopicPermission::kPublish) |
                       static_cast<uint32_t>(
                           registry::TopicPermission::kBridge),
    }};
    topic.region_version = 1;
    topic.channel_version = 1;
    topic.acl_version = 1;
    return topic;
}

RemoteBridgeConfig Config(const std::filesystem::path& root) {
    const std::array<std::byte, 4> loopback = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = transport::EndpointDescriptor::Ipv4Tcp(loopback, 43199);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();

    RemoteBridgeConfig config;
    config.schema_store_root = root;

    config.connection.topic_authorizer =
        std::make_shared<AllowAllTopics>();
    config.connection.mode = bridge::BridgeConnectionMode::kConnect;
    if (endpoint.ok()) config.connection.remote_endpoint = *endpoint;
    config.connection.local_identity = Fence(NodeId{101}, 1001);
    config.connection.expected_peer = Fence(NodeId{202}, 2001);
    config.connection.route_driver_id = 71;
    config.connection.route_driver_generation = 1;
    config.connection.driver_config = transport::DriverConfig{
        .max_connections = 4,
        .max_listeners = 1,
        .max_queued_sends = 32,
    };
    config.tcp.max_frame_body_bytes = 4096;
    config.connection.pipeline.max_control_bytes = 4096;
    config.connection.pipeline.wire_limits.max_payload_length = 4096;
    config.connection.pipeline.wire_limits.max_buffered_bytes = 8192;
    config.schema_negotiation.max_descriptor_bytes = 2048;
    config.schema_negotiation.max_control_frame_bytes = 4096;
    config.tcp.max_total_send_buffer_bytes = 64 * 1024;
    config.tcp.max_connection_send_buffer_bytes = 32 * 1024;
    config.tcp.max_ready_receive_bytes = 64 * 1024;
    config.tcp.max_ready_receive_messages = 32;
    config.tcp.max_pending_accepts = 4;
    return config;
}

TEST(RemoteBridgeTest, ProductionConfigurationRejectsPlaintextAndMissingCoordinator) {
    SinkIngress ingress;
    auto descriptor_auth = std::make_shared<AcceptingAuth>();
    auto coordinator_created = registry::Coordinator::CreateForTesting();
    ASSERT_TRUE(coordinator_created.ok());
    auto coordinator = std::shared_ptr<registry::Coordinator>(
        std::move(*coordinator_created));
    RemoteBridgeConfig plaintext = Config(StoreRoot());
    auto rejected = RemoteBridge::Create(std::move(plaintext), &ingress,
                                         descriptor_auth, coordinator);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kPermissionDenied);

    auto denied = RemoteBridge::Create(Config(StoreRoot()), &ingress,
                                       descriptor_auth, nullptr);
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kInvalidArgument);
}

TEST(RemoteBridgeTest, CoordinatorAuthorizerRequiresActivePublishAndBridgeAcl) {
    auto coordinator_created = registry::Coordinator::CreateForTesting();
    ASSERT_TRUE(coordinator_created.ok())
        << coordinator_created.status().ToString();
    auto coordinator = std::shared_ptr<registry::Coordinator>(
        std::move(*coordinator_created));
    const auto endpoint = Config(StoreRoot()).connection.remote_endpoint;
    ASSERT_TRUE(endpoint.has_value());
    ASSERT_TRUE(coordinator
                    ->RegisterNode(
                        registry::NodeRegistration{
                            .node_id = NodeId{101},
                            .process_identity = Process(NodeId{101}, 1001),
                            .endpoints = {*endpoint},
                            .security_domain_id = SecurityDomainId{77},
                            .trust_domain = "test-domain",
                            .health = registry::NodeHealth::kHealthy,
                            .lease_epoch = 1,
                            .lease_duration_ns = 10,
                            .config_version = 1,
                        },
                        100)
                    .ok());
    auto topic = coordinator->CreateTopic(AuthorizedTopic());
    ASSERT_TRUE(topic.ok()) << topic.status().ToString();
    const registry::TopicMetadata& metadata = (*topic)->metadata;
    ASSERT_TRUE(coordinator
                    ->ActivateTopic(
                        metadata.topic_id,
                        registry::ActivationReadinessProof{
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
                        })
                    .ok());
    auto clock = std::make_shared<FakeAuthorizerClock>(109);
    CoordinatorTopicAuthorizer authorizer(coordinator, clock);
    const security::AuthenticatedPeer peer{
        .node_id = NodeId{101},
        .security_domain = SecurityDomainId{77},
        .credential_generation = 1,
    };
    EXPECT_TRUE(authorizer.AuthorizeInbound(peer, metadata.topic_id).ok());
    clock->Set(110);
    EXPECT_EQ(authorizer.AuthorizeInbound(peer, metadata.topic_id).code(),
              StatusCode::kPermissionDenied);
    const registry::NodeLeaseOwner owner{
        .node_id = peer.node_id,
        .process_identity = Process(NodeId{101}, 1001),
        .lease_epoch = 1,
    };
    ASSERT_TRUE(coordinator
                    ->HeartbeatNode(owner, registry::NodeHealth::kHealthy, 110)
                    .ok());
    EXPECT_TRUE(authorizer.AuthorizeInbound(peer, metadata.topic_id).ok());
    ASSERT_TRUE(coordinator
                    ->HeartbeatNode(owner, registry::NodeHealth::kUnavailable,
                                    111)
                    .ok());
    clock->Set(111);
    EXPECT_EQ(authorizer.AuthorizeInbound(peer, metadata.topic_id).code(),
              StatusCode::kPermissionDenied);
    ASSERT_TRUE(coordinator
                    ->HeartbeatNode(owner, registry::NodeHealth::kDegraded, 112)
                    .ok());
    clock->Set(112);
    EXPECT_EQ(authorizer.AuthorizeInbound(peer, metadata.topic_id).code(),
              StatusCode::kPermissionDenied);
    ASSERT_TRUE(coordinator
                    ->HeartbeatNode(owner, registry::NodeHealth::kHealthy, 113)
                    .ok());
    clock->Set(113);
    EXPECT_TRUE(authorizer.AuthorizeInbound(peer, metadata.topic_id).ok());

    security::AuthenticatedPeer wrong_domain = peer;
    wrong_domain.security_domain = SecurityDomainId{88};
    EXPECT_EQ(authorizer.AuthorizeInbound(wrong_domain, metadata.topic_id).code(),
              StatusCode::kPermissionDenied);
    EXPECT_EQ(authorizer.AuthorizeInbound(peer, TopicId{9999}).code(),
              StatusCode::kPermissionDenied);
    EXPECT_EQ(authorizer.denied_total(), 5u);
    coordinator.reset();
    EXPECT_TRUE(authorizer.AuthorizeInbound(peer, metadata.topic_id).ok());
}

TEST(RemoteBridgeTest, ComposesOwnedProductionDependenciesFailClosed) {
    const std::filesystem::path root = StoreRoot();
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();

    auto created = testing::RemoteBridgeTestFactory::Create(
        Config(root), &ingress, auth);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    EXPECT_EQ((*created)->manager().state(),
              bridge::BridgeConnectionState::kStopped);
    EXPECT_EQ((*created)->schema_registry().size(), 0u);
    EXPECT_EQ((*created)->schema_store().size(), 0u);
    EXPECT_EQ((*created)->RegisterLocalDescriptor({}).status().code(),
              StatusCode::kInvalidArgument);
    ASSERT_TRUE((*created)->Start(100).ok());
    auto pumped = (*created)->Pump(bridge::BridgePumpBudget{.now_ns = 100});
    ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
    const RemoteBridgeOperationalStats stats = (*created)->OperationalStats();
    EXPECT_EQ(stats.configured_connections, 1u);
    EXPECT_EQ(stats.connected_connections, 0u);
    EXPECT_GE(stats.reconnect_failures, 1u);
    ASSERT_TRUE((*created)->Shutdown().ok());

    created->reset();
    std::filesystem::remove_all(root, ignored);
}

TEST(RemoteBridgeTest, HydratesRegistryFromDurableSchemaStore) {
    const std::filesystem::path root = StoreRoot();
    auto artifact = CompileArtifact(
        "option schema_version = \"1.0\"; package deployment; "
        "message Restored { uint64 value = 1; }");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    schema::SchemaRegistry writer_registry;
    auto store = storage::SchemaStore::Open(root, &writer_registry);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    ASSERT_TRUE((*store)->Persist(artifact->identity, Bytes(artifact->bytes)).ok());
    store->reset();

    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();
    auto created = testing::RemoteBridgeTestFactory::Create(
        Config(root), &ingress, auth);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    EXPECT_EQ((*created)->schema_store().size(), 1u);
    EXPECT_EQ((*created)->schema_registry().size(), 1u);
    EXPECT_TRUE((*created)->schema_registry().Find(artifact->identity).ok());
}

TEST(RemoteBridgeTest,
     MultiLanePinsSourcesAndSharesOneLogicalEgressQuota) {
    const std::filesystem::path root = StoreRoot();
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
    auto artifact = CompileArtifact(
        "option schema_version = \"1.0\"; package deployment; "
        "message LaneData { uint64 value = 1; }");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();

    RemoteBridgeConfig config = Config(root);
    config.tcp_lane_count = 2;
    config.connection.max_egress_frames = 2;
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();
    auto created =
        testing::RemoteBridgeTestFactory::Create(config, &ingress, auth);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto bridge = std::move(*created);
    ASSERT_EQ(bridge->tcp_lane_count(), 2u);
    EXPECT_EQ(bridge->manager(0).lane_index(), 0u);
    EXPECT_EQ(bridge->manager(1).lane_index(), 1u);
    EXPECT_EQ(bridge->manager(0).lane_count(), 2u);
    EXPECT_EQ(bridge->manager(1).lane_count(), 2u);
    ASSERT_TRUE(
        bridge->RegisterLocalDescriptor(Bytes(artifact->bytes)).ok());
    ASSERT_TRUE(bridge->Start(1).ok());

    const bridge::SourceIdentity lane0 = SourceForLane(0, 2);
    const bridge::SourceIdentity lane1 = SourceForLane(1, 2);
    ASSERT_NE(lane0.publisher_id, 0u);
    ASSERT_NE(lane1.publisher_id, 0u);
    ASSERT_TRUE(bridge
                    ->Enqueue(FrameFor(lane0, 1), artifact->identity,
                              registry::Reliability::kReliableOrdered)
                    .ok());
    ASSERT_TRUE(bridge
                    ->Enqueue(FrameFor(lane1, 1), artifact->identity,
                              registry::Reliability::kReliableOrdered)
                    .ok());
    EXPECT_EQ(bridge->manager(0).queued_egress_frames(), 1u);
    EXPECT_EQ(bridge->manager(1).queued_egress_frames(), 1u);
    EXPECT_EQ(bridge->connection_pool().queued_egress_frames(), 2u);

    EXPECT_EQ(bridge
                  ->Enqueue(FrameFor(lane0, 2), artifact->identity,
                            registry::Reliability::kReliableOrdered)
                  .code(),
              StatusCode::kWouldBlock);
    EXPECT_EQ(bridge->manager(0)
                  .Enqueue(bridge::EncodedOutboundFrame{
                      .frame = FrameFor(lane1, 2),
                      .reliability =
                          registry::Reliability::kReliableOrdered,
                      .allow_drop = false,
                      .schema_identity = artifact->identity,
                      .descriptor_artifact = std::vector<std::byte>(
                          Bytes(artifact->bytes).begin(),
                          Bytes(artifact->bytes).end()),
                  })
                  .code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(bridge->manager(0).queued_egress_frames(), 1u);
    EXPECT_TRUE(bridge->Shutdown().ok());
    std::filesystem::remove_all(root, ignored);
}

TEST(RemoteBridgeTest, RejectsInvalidTcpLaneConfiguration) {
    const std::filesystem::path root = StoreRoot();
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();

    RemoteBridgeConfig zero = Config(root);
    zero.tcp_lane_count = 0;
    EXPECT_EQ(testing::RemoteBridgeTestFactory::Create(zero, &ingress, auth)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
    RemoteBridgeConfig excessive = Config(root);
    excessive.tcp_lane_count = bridge::kMaxBridgeLaneCount + 1;
    EXPECT_EQ(testing::RemoteBridgeTestFactory::Create(excessive, &ingress, auth)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
    RemoteBridgeConfig conflicting = Config(root);
    conflicting.connection.lane_count = 2;
    EXPECT_EQ(testing::RemoteBridgeTestFactory::Create(conflicting, &ingress, auth)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
}

TEST(RemoteBridgeTest, CapacityScalesOnlyConnectionLocalLaneState) {
    const std::filesystem::path root = StoreRoot();
    RemoteBridgeConfig single = Config(root);
    RemoteBridgeConfig four = single;
    four.tcp_lane_count = 4;
    auto single_estimate =
        testing::RemoteBridgeTestFactory::EstimateResources(single);
    auto four_estimate =
        testing::RemoteBridgeTestFactory::EstimateResources(four);
    ASSERT_TRUE(single_estimate.ok()) << single_estimate.status().ToString();
    ASSERT_TRUE(four_estimate.ok()) << four_estimate.status().ToString();
    EXPECT_EQ(single_estimate->bridge_connections, 1u);
    EXPECT_EQ(four_estimate->bridge_connections, 4u);
    EXPECT_EQ(four_estimate->threads, single_estimate->threads);
    EXPECT_EQ(
        four_estimate->bridge_egress_bytes -
            single_estimate->bridge_egress_bytes,
        3 * single.connection.pipeline.retransmit.max_bytes);
    EXPECT_EQ(
        four_estimate->schema_buffer_bytes -
            single_estimate->schema_buffer_bytes,
        3 * (single.schema_negotiation.max_buffered_bytes +
             single.schema_negotiation.max_descriptor_bytes));
}

TEST(RemoteBridgeTest, RejectsIncompatibleCompositionLimits) {
    const std::filesystem::path root = StoreRoot();
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();
    RemoteBridgeConfig config = Config(root);
    config.schema_negotiation.max_descriptor_bytes = 8192;
    config.schema_negotiation.max_control_frame_bytes = 16 * 1024;

    auto created = testing::RemoteBridgeTestFactory::Create(
        std::move(config), &ingress, auth);
    ASSERT_FALSE(created.ok());
    EXPECT_EQ(created.status().code(), StatusCode::kInvalidArgument);
}

TEST(RemoteBridgeTest, CapacityAdmissionCommitsAndReleasesEstimatedResources) {
    const std::filesystem::path first_root = StoreRoot();
    const std::filesystem::path second_root = StoreRoot();
    RemoteBridgeConfig config = Config(first_root);
    auto estimate =
        testing::RemoteBridgeTestFactory::EstimateResources(config);
    ASSERT_TRUE(estimate.ok()) << estimate.status().ToString();
    capacity::NodeBudget budget;
    budget.limit = *estimate;
    auto controller_result = capacity::CapacityController::Create(budget);
    ASSERT_TRUE(controller_result.ok())
        << controller_result.status().ToString();
    auto controller = std::move(*controller_result);
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();

    auto first = testing::RemoteBridgeTestFactory::Create(
        std::move(config), &ingress, auth, controller);
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_EQ(controller->Snapshot().committed, *estimate);

    auto denied = testing::RemoteBridgeTestFactory::Create(
        Config(second_root), &ingress, auth, controller);
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(controller->Snapshot().pending.empty());
    EXPECT_EQ(controller->Snapshot().committed, *estimate);

    first->reset();
    EXPECT_TRUE(controller->Snapshot().committed.empty());
    std::error_code ignored;
    std::filesystem::remove_all(first_root, ignored);
    std::filesystem::remove_all(second_root, ignored);
}

TEST(RemoteBridgeTest, CompositionFailureRollsBackPendingCapacity) {
    const std::filesystem::path root = StoreRoot();
    {
        std::ofstream file(root);
        ASSERT_TRUE(file.good());
        file << "not a schema store directory";
    }
    capacity::NodeBudget budget;
    budget.limit.bridge_connections = 1;
    auto controller_result = capacity::CapacityController::Create(budget);
    ASSERT_TRUE(controller_result.ok())
        << controller_result.status().ToString();
    auto controller = std::move(*controller_result);
    capacity::ResourceVector charge;
    charge.bridge_connections = 1;
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();

    auto failed = testing::RemoteBridgeTestFactory::Create(
        Config(root), &ingress, auth, controller, charge);
    ASSERT_FALSE(failed.ok());
    EXPECT_TRUE(controller->Snapshot().pending.empty());
    EXPECT_TRUE(controller->Snapshot().committed.empty());
    std::error_code ignored;
    std::filesystem::remove(root, ignored);
}

TEST(RemoteBridgeTest, RejectsMissingAuthenticationAndIngress) {
    const std::filesystem::path root = StoreRoot();
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();

    EXPECT_EQ(testing::RemoteBridgeTestFactory::Create(Config(root), &ingress,
                                                       nullptr)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(testing::RemoteBridgeTestFactory::Create(Config(root), nullptr,
                                                       auth)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::deployment
