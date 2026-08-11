// Copyright 2026 The Mino Authors

#include "mino/capacity/capacity.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <utility>

#include "mino/registry/coordinator.h"
#include "mino/runtime/deployment/remote_bridge.h"
#include "mino/runtime/deployment/remote_bridge_test_helper.h"

namespace mino::capacity {
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

std::filesystem::path StoreRoot() {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    return base / ("mino_capacity_modules_" + std::to_string(::getpid()) +
                   "_" + std::to_string(sequence.fetch_add(1)));
}

deployment::RemoteBridgeConfig BridgeConfig(
    const std::filesystem::path& root) {
    const std::array<std::byte, 4> loopback = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = transport::EndpointDescriptor::Ipv4Tcp(loopback, 43201);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    deployment::RemoteBridgeConfig config;
    config.schema_store_root = root;

    config.connection.topic_authorizer =
        std::make_shared<AllowAllTopics>();
    config.connection.mode = bridge::BridgeConnectionMode::kConnect;
    if (endpoint.ok()) config.connection.remote_endpoint = *endpoint;
    config.connection.local_identity = Fence(NodeId{1001}, 1101);
    config.connection.expected_peer = Fence(NodeId{1002}, 1201);
    config.connection.route_driver_id = 1;
    config.connection.route_driver_generation = 1;
    config.connection.driver_config = {
        .max_connections = 1,
        .max_listeners = 1,
        .max_queued_sends = 8,
    };
    config.connection.max_egress_bytes = 1024;
    config.tcp.max_frame_body_bytes = 4096;
    config.tcp.max_total_send_buffer_bytes = 8192;
    config.tcp.max_connection_send_buffer_bytes = 8192;
    config.tcp.max_ready_receive_bytes = 8192;
    config.tcp.max_ready_receive_messages = 8;
    config.tcp.max_pending_accepts = 1;
    config.tcp.max_control_send_buffer_bytes = 8192;
    config.tcp.max_control_send_messages = 8;
    config.connection.pipeline.max_control_bytes = 4096;
    config.connection.pipeline.max_pending_inbound_bytes = 4096;
    config.connection.pipeline.wire_limits.max_payload_length = 4096;
    config.connection.pipeline.wire_limits.max_buffered_bytes = 8192;
    config.connection.pipeline.retransmit.max_bytes = 4096;
    config.schema_negotiation.max_buffered_bytes = 4096;
    config.schema_negotiation.max_descriptor_bytes = 2048;
    config.schema_negotiation.max_control_frame_bytes = 4096;
    return config;
}

registry::TopicMetadata Topic() {
    schema::CanonicalDigest digest{};
    digest[0] = std::byte{1};
    return registry::TopicMetadata{
        .topic_id = {},
        .name = "capacity/cross-module",
        .channel_kind = registry::ChannelKind::kBroadcast,
        .delivery = {.reliability = registry::Reliability::kBestEffort,
                     .allow_drop = false},
        .queue_full_policy = QueueFullPolicy::kBlock,
        .schema = schema::SchemaIdentity(1, digest, 1, 1),
        .accepted_schemas = {},
        .route_policy = registry::RoutePolicy::kDiscovery,
        .static_routes = {},
        .route_set_version = 0,
        .capacity = 64,
        .max_publishers = 1,
        .max_subscribers = 1,
        .partition_count = 1,
        .record_topology =
            registry::RecordBackpressureTopology::kIsolated,
        .acl = registry::TopicAcl{
            .entries = {{.node_id = NodeId{1},
                         .security_domain_id = SecurityDomainId{1},
                         .permissions = registry::kAllTopicPermissions}},
        },
        .region_version = 1,
        .channel_version = 1,
        .acl_version = 1,
        .config_version = 0,
        .state = registry::TopicState::kCreating,
    };
}

TEST(ModuleCapacityIntegrationTest,
     RegistryAndBridgeCompeteInOneAtomicNodeBudget) {
    NodeBudget budget;
    budget.limit.shm_bytes = 100;
    budget.limit.topics = 1;
    budget.limit.bridge_connections = 1;
    auto controller_result = CapacityController::Create(budget);
    ASSERT_TRUE(controller_result.ok())
        << controller_result.status().ToString();
    auto controller = std::move(*controller_result);

    auto coordinator_result = registry::Coordinator::CreateForTesting(
        {}, std::make_shared<registry::InMemoryMonotonicIdAllocator>(), {}, {},
        controller);
    ASSERT_TRUE(coordinator_result.ok())
        << coordinator_result.status().ToString();
    auto coordinator = std::move(*coordinator_result);
    ResourceVector topic_charge;
    topic_charge.shm_bytes = 60;
    ASSERT_TRUE(coordinator->CreateTopic(Topic(), topic_charge).ok());

    ResourceVector bridge_charge;
    bridge_charge.shm_bytes = 50;
    bridge_charge.bridge_connections = 1;
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();
    const std::filesystem::path denied_root = StoreRoot();
    auto denied = deployment::testing::RemoteBridgeTestFactory::Create(
        BridgeConfig(denied_root), &ingress, auth, controller, bridge_charge);
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(controller->Snapshot().committed.shm_bytes, 60u);
    EXPECT_TRUE(controller->Snapshot().pending.empty());

    coordinator.reset();
    EXPECT_TRUE(controller->Snapshot().committed.empty());
    const std::filesystem::path accepted_root = StoreRoot();
    auto accepted = deployment::testing::RemoteBridgeTestFactory::Create(
        BridgeConfig(accepted_root), &ingress, auth, controller, bridge_charge);
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
    EXPECT_EQ(controller->Snapshot().committed.shm_bytes, 50u);
    EXPECT_EQ(controller->Snapshot().committed.bridge_connections, 1u);
    accepted->reset();
    EXPECT_TRUE(controller->Snapshot().committed.empty());

    std::error_code ignored;
    std::filesystem::remove_all(denied_root, ignored);
    std::filesystem::remove_all(accepted_root, ignored);
}

}  // namespace
}  // namespace mino::capacity
