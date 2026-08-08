// Copyright 2026 The Mino Authors

#include "mino/runtime/deployment/remote_bridge.h"

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

std::filesystem::path StoreRoot() {
    static std::atomic<uint64_t> sequence{0};
    const char* test_tmpdir = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        test_tmpdir == nullptr ? std::filesystem::temp_directory_path()
                               : std::filesystem::path(test_tmpdir);
    return base / ("mino_remote_bridge_" + std::to_string(::getpid()) + "_" +
                   std::to_string(sequence.fetch_add(1)));
}

RemoteBridgeConfig Config(const std::filesystem::path& root) {
    const std::array<std::byte, 4> loopback = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = transport::EndpointDescriptor::Ipv4Tcp(loopback, 43199);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();

    RemoteBridgeConfig config;
    config.schema_store_root = root;
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

TEST(RemoteBridgeTest, ComposesOwnedProductionDependenciesFailClosed) {
    const std::filesystem::path root = StoreRoot();
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();

    auto created = RemoteBridge::Create(Config(root), &ingress, auth);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    EXPECT_EQ((*created)->manager().state(),
              bridge::BridgeConnectionState::kStopped);
    EXPECT_EQ((*created)->schema_registry().size(), 0u);
    EXPECT_EQ((*created)->schema_store().size(), 0u);
    EXPECT_EQ((*created)->RegisterLocalDescriptor({}).status().code(),
              StatusCode::kInvalidArgument);

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
    auto created = RemoteBridge::Create(Config(root), &ingress, auth);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    EXPECT_EQ((*created)->schema_store().size(), 1u);
    EXPECT_EQ((*created)->schema_registry().size(), 1u);
    EXPECT_TRUE((*created)->schema_registry().Find(artifact->identity).ok());
}

TEST(RemoteBridgeTest, RejectsIncompatibleCompositionLimits) {
    const std::filesystem::path root = StoreRoot();
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();
    RemoteBridgeConfig config = Config(root);
    config.schema_negotiation.max_descriptor_bytes = 8192;
    config.schema_negotiation.max_control_frame_bytes = 16 * 1024;

    auto created = RemoteBridge::Create(std::move(config), &ingress, auth);
    ASSERT_FALSE(created.ok());
    EXPECT_EQ(created.status().code(), StatusCode::kInvalidArgument);
}

TEST(RemoteBridgeTest, RejectsMissingAuthenticationAndIngress) {
    const std::filesystem::path root = StoreRoot();
    SinkIngress ingress;
    auto auth = std::make_shared<AcceptingAuth>();

    EXPECT_EQ(RemoteBridge::Create(Config(root), &ingress, nullptr)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(RemoteBridge::Create(Config(root), nullptr, auth)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::deployment
