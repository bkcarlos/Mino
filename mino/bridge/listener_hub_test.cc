// Copyright 2026 The Mino Authors

#include "mino/bridge/bridge_runtime/connection_manager.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "mino/transport/tcp_driver.h"

namespace mino::bridge {
namespace {

using namespace std::chrono_literals;

uint16_t FreePort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd < 0) return 0;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        (void)::close(fd);
        return 0;
    }
    socklen_t size = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
        (void)::close(fd);
        return 0;
    }
    (void)::close(fd);
    return ntohs(address.sin_port);
}

transport::EndpointDescriptor Loopback(uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = transport::EndpointDescriptor::Ipv4Tcp(address, port);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    return endpoint.ok() ? *endpoint : transport::EndpointDescriptor{};
}

transport::TcpDriverOptions TcpOptions() {
    transport::TcpDriverOptions options;
    options.max_frame_body_bytes = 4096;
    options.max_total_send_buffer_bytes = 128 * 1024;
    options.max_connection_send_buffer_bytes = 32 * 1024;
    options.max_ready_receive_bytes = 128 * 1024;
    options.max_ready_receive_messages = 256;
    options.max_pending_accepts = 16;
    options.heartbeat_interval_ms = 20;
    options.idle_timeout_ms = 3000;
    options.partial_frame_timeout_ms = 1000;
    options.io_poll_max_ms = 2;
    return options;
}

transport::DriverConfig DriverConfig() {
    return transport::DriverConfig{
        .max_connections = 16,
        .max_listeners = 2,
        .max_queued_sends = 256,
    };
}

BridgeNodeIdentityFence Fence(NodeId node, uint64_t incarnation) {
    return BridgeNodeIdentityFence{
        .node_id = node,
        .process_identity = ProcessIdentity{
            .node_id = node.value,
            .process_id = 1000 + incarnation,
            .process_epoch = 2000 + incarnation,
            .start_time_ns = 3000 + incarnation,
        },
        .lease_epoch = 4000 + incarnation,
        .node_config_version = 5000 + incarnation,
    };
}

BridgeConnectionManagerOptions ManagerOptions(
    BridgeConnectionMode mode,
    const transport::EndpointDescriptor& endpoint,
    BridgeNodeIdentityFence local,
    BridgeNodeIdentityFence peer,
    bool manage_driver,
    uint16_t lane_index = 0,
    uint16_t lane_count = 1) {
    BridgeConnectionManagerOptions options;
    options.mode = mode;
    if (mode == BridgeConnectionMode::kConnect) {
        options.remote_endpoint = endpoint;
    } else {
        options.peer_route_endpoint = endpoint;
    }
    options.local_identity = local;
    options.expected_peer = peer;
    options.route_driver_id = 81;
    options.route_driver_generation = 2;
    options.driver_config = DriverConfig();
    options.manage_driver_lifecycle = manage_driver;
    options.connect_timeout_ms = 50;
    options.handshake_timeout_ns = 2'000'000'000ull;
    options.initial_reconnect_backoff_ns = 2'000'000ull;
    options.max_reconnect_backoff_ns = 32'000'000ull;
    options.health_probe_interval_ns = 2'000'000ull;
    options.max_egress_frames = 32;
    options.max_egress_bytes = 64 * 1024;
    options.pipeline.wire_limits.max_payload_length = 4096;
    options.pipeline.wire_limits.max_buffered_bytes = 8192;
    options.pipeline.retransmit.max_entries = 32;
    options.pipeline.retransmit.max_bytes = 64 * 1024;
    options.pipeline.retransmit.max_age_ns = 30'000'000'000ull;
    options.lane_index = lane_index;
    options.lane_count = lane_count;
    return options;
}

class CollectingIngress final : public BridgeIngressPort {
public:
    Status DecodeValidatePublish(const WireFrame& frame) override {
        frames.push_back(frame);
        return Status::Ok();
    }
    std::vector<WireFrame> frames;
};

SourceIdentity SourceForLane(uint64_t node, uint16_t lane_index,
                             uint16_t lane_count) {
    for (uint64_t publisher = 1; publisher != 10'000; ++publisher) {
        const SourceIdentity source{node, publisher, 1};
        if (BridgeLaneFor(source, lane_count) == lane_index) return source;
    }
    return {};
}

WireFrame DataFrame(uint64_t node, uint64_t publisher, uint64_t sequence,
                    std::byte value) {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 7;
    frame.header.msg_type = 8;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = node;
    frame.header.source_publisher_id = publisher;
    frame.header.source_publisher_epoch = 1;
    frame.header.sequence_num = sequence;
    frame.payload.assign(32, value);
    return frame;
}

Status QueueReliable(BridgeConnectionManager* manager, WireFrame frame) {
    return manager->Enqueue(EncodedOutboundFrame{
        .frame = std::move(frame),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
}

TEST(BridgeListenerHubTest, RoutesTwoPeersThroughOneListenerAndSharedDriver) {
    const transport::EndpointDescriptor endpoint = Loopback(FreePort());
    const BridgeNodeIdentityFence server = Fence(NodeId{900}, 1);
    const BridgeNodeIdentityFence peer_a = Fence(NodeId{901}, 2);
    const BridgeNodeIdentityFence peer_b = Fence(NodeId{902}, 3);

    auto server_created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(server_created.ok()) << server_created.status().ToString();
    auto server_driver =
        std::shared_ptr<transport::TcpDriver>(std::move(*server_created));
    ASSERT_TRUE(server_driver->Start(DriverConfig()).ok());

    CollectingIngress inbound_a_ingress;
    CollectingIngress inbound_b_ingress;
    auto inbound_a_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kAccepted, endpoint, server,
                       peer_a, false),
        server_driver, &inbound_a_ingress);
    auto inbound_b_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kAccepted, endpoint, server,
                       peer_b, false),
        server_driver, &inbound_b_ingress);
    ASSERT_TRUE(inbound_a_created.ok())
        << inbound_a_created.status().ToString();
    ASSERT_TRUE(inbound_b_created.ok())
        << inbound_b_created.status().ToString();
    auto inbound_a = std::shared_ptr<BridgeConnectionManager>(
        std::move(*inbound_a_created));
    auto inbound_b = std::shared_ptr<BridgeConnectionManager>(
        std::move(*inbound_b_created));
    ASSERT_TRUE(inbound_a->Start(1).ok());
    ASSERT_TRUE(inbound_b->Start(1).ok());

    auto hub_created = BridgeListenerHub::Create(
        BridgeListenerHubOptions{
            .local_endpoint = endpoint,
            .driver_config = DriverConfig(),
            .manage_driver_lifecycle = false,
            .listen_backlog = 8,
            .max_peers = 4,
            .max_pending_handshakes = 4,
            .max_accepts_per_pump = 4,
            .handshake_timeout_ns = 2'000'000'000ull,
            .wire_limits = WireFrameLimits{
                .max_payload_length = 4096,
                .max_buffered_bytes = 8192,
            },
        },
        server_driver);
    ASSERT_TRUE(hub_created.ok()) << hub_created.status().ToString();
    auto hub = std::move(*hub_created);
    ASSERT_TRUE(hub->RegisterPeer(inbound_a).ok());
    ASSERT_TRUE(hub->RegisterPeer(inbound_b).ok());
    ASSERT_TRUE(hub->Start().ok());

    auto connector_a_driver_created = transport::TcpDriver::Create(TcpOptions());
    auto connector_b_driver_created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(connector_a_driver_created.ok());
    ASSERT_TRUE(connector_b_driver_created.ok());
    auto connector_a_driver = std::shared_ptr<transport::TcpDriver>(
        std::move(*connector_a_driver_created));
    auto connector_b_driver = std::shared_ptr<transport::TcpDriver>(
        std::move(*connector_b_driver_created));
    CollectingIngress connector_a_ingress;
    CollectingIngress connector_b_ingress;
    auto connector_a_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kConnect, endpoint, peer_a,
                       server, true),
        connector_a_driver, &connector_a_ingress);
    auto connector_b_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kConnect, endpoint, peer_b,
                       server, true),
        connector_b_driver, &connector_b_ingress);
    ASSERT_TRUE(connector_a_created.ok());
    ASSERT_TRUE(connector_b_created.ok());
    auto connector_a = std::move(*connector_a_created);
    auto connector_b = std::move(*connector_b_created);
    ASSERT_TRUE(connector_a->Start(1).ok());
    ASSERT_TRUE(connector_b->Start(1).ok());

    uint64_t now_ns = 1;
    auto pump_all = [&]() -> Status {
        now_ns += 1'000'000;
        BridgePumpBudget budget;
        budget.now_ns = now_ns;
        auto a = connector_a->Pump(budget);
        if (!a.ok()) return a.status();
        auto b = connector_b->Pump(budget);
        if (!b.ok()) return b.status();
        auto accepted = hub->Pump(now_ns);
        if (!accepted.ok()) return accepted.status();
        auto inbound_a_result = inbound_a->Pump(budget);
        if (!inbound_a_result.ok()) return inbound_a_result.status();
        auto inbound_b_result = inbound_b->Pump(budget);
        if (!inbound_b_result.ok()) return inbound_b_result.status();
        return Status::Ok();
    };
    auto all_ready = [&] {
        return connector_a->state() == BridgeConnectionState::kActive &&
               connector_b->state() == BridgeConnectionState::kActive &&
               inbound_a->state() == BridgeConnectionState::kActive &&
               inbound_b->state() == BridgeConnectionState::kActive;
    };
    for (size_t i = 0; i < 4000 && !all_ready(); ++i) {
        ASSERT_TRUE(pump_all().ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(all_ready());
    EXPECT_EQ(hub->stats().accepted_connections, 2u);
    EXPECT_EQ(hub->stats().dispatched_connections, 2u);
    EXPECT_EQ(server_driver->stats().listeners, 1u);
    EXPECT_EQ(server_driver->stats().active_connections, 2u);

    ASSERT_TRUE(QueueReliable(
                    connector_a.get(),
                    DataFrame(901, 1001, 1, std::byte{0xa1}))
                    .ok());
    ASSERT_TRUE(QueueReliable(
                    connector_b.get(),
                    DataFrame(902, 1002, 1, std::byte{0xb2}))
                    .ok());
    for (size_t i = 0;
         i < 4000 && (inbound_a_ingress.frames.size() != 1 ||
                      inbound_b_ingress.frames.size() != 1);
         ++i) {
        ASSERT_TRUE(pump_all().ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(inbound_a_ingress.frames.size(), 1u);
    ASSERT_EQ(inbound_b_ingress.frames.size(), 1u);
    EXPECT_EQ(inbound_a_ingress.frames.front().header.source_node_id, 901u);
    EXPECT_EQ(inbound_b_ingress.frames.front().header.source_node_id, 902u);

    EXPECT_TRUE(connector_a->Shutdown().ok());
    EXPECT_TRUE(connector_b->Shutdown().ok());
    EXPECT_TRUE(inbound_a->Shutdown().ok());
    EXPECT_TRUE(inbound_b->Shutdown().ok());
    EXPECT_TRUE(hub->Shutdown().ok());
    EXPECT_EQ(server_driver->state(), transport::DriverState::kRunning);
    EXPECT_TRUE(server_driver->Shutdown().ok());
}

TEST(BridgeListenerHubTest,
     RoutesTwoLanesOfOneIdentityThroughOneSharedListener) {
    const transport::EndpointDescriptor endpoint = Loopback(FreePort());
    const BridgeNodeIdentityFence server = Fence(NodeId{910}, 10);
    const BridgeNodeIdentityFence peer = Fence(NodeId{911}, 11);

    auto server_created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(server_created.ok()) << server_created.status().ToString();
    auto server_driver =
        std::shared_ptr<transport::TcpDriver>(std::move(*server_created));
    ASSERT_TRUE(server_driver->Start(DriverConfig()).ok());
    CollectingIngress inbound_lane0_ingress;
    CollectingIngress inbound_lane1_ingress;
    auto inbound_lane0_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kAccepted, endpoint, server, peer,
                       false, 0, 2),
        server_driver, &inbound_lane0_ingress);
    auto inbound_lane1_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kAccepted, endpoint, server, peer,
                       false, 1, 2),
        server_driver, &inbound_lane1_ingress);
    ASSERT_TRUE(inbound_lane0_created.ok())
        << inbound_lane0_created.status().ToString();
    ASSERT_TRUE(inbound_lane1_created.ok())
        << inbound_lane1_created.status().ToString();
    auto inbound_lane0 = std::shared_ptr<BridgeConnectionManager>(
        std::move(*inbound_lane0_created));
    auto inbound_lane1 = std::shared_ptr<BridgeConnectionManager>(
        std::move(*inbound_lane1_created));
    ASSERT_TRUE(inbound_lane0->Start(1).ok());
    ASSERT_TRUE(inbound_lane1->Start(1).ok());

    auto hub_created = BridgeListenerHub::Create(
        BridgeListenerHubOptions{
            .local_endpoint = endpoint,
            .driver_config = DriverConfig(),
            .manage_driver_lifecycle = false,
            .listen_backlog = 8,
            .max_peers = 1,
            .max_pending_handshakes = 4,
            .max_accepts_per_pump = 4,
            .handshake_timeout_ns = 2'000'000'000ull,
            .wire_limits = WireFrameLimits{
                .max_payload_length = 4096,
                .max_buffered_bytes = 8192,
            },
        },
        server_driver);
    ASSERT_TRUE(hub_created.ok()) << hub_created.status().ToString();
    auto hub = std::move(*hub_created);
    ASSERT_TRUE(hub->RegisterPeer(inbound_lane0).ok());
    ASSERT_TRUE(hub->RegisterPeer(inbound_lane1).ok());
    ASSERT_TRUE(hub->Start().ok());

    auto connector_created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(connector_created.ok())
        << connector_created.status().ToString();
    auto connector_driver =
        std::shared_ptr<transport::TcpDriver>(std::move(*connector_created));
    ASSERT_TRUE(connector_driver->Start(DriverConfig()).ok());
    CollectingIngress connector_lane0_ingress;
    CollectingIngress connector_lane1_ingress;
    auto connector_lane0_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kConnect, endpoint, peer, server,
                       false, 0, 2),
        connector_driver, &connector_lane0_ingress);
    auto connector_lane1_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kConnect, endpoint, peer, server,
                       false, 1, 2),
        connector_driver, &connector_lane1_ingress);
    ASSERT_TRUE(connector_lane0_created.ok())
        << connector_lane0_created.status().ToString();
    ASSERT_TRUE(connector_lane1_created.ok())
        << connector_lane1_created.status().ToString();
    auto connector_lane0 = std::move(*connector_lane0_created);
    auto connector_lane1 = std::move(*connector_lane1_created);
    ASSERT_TRUE(connector_lane0->Start(1).ok());
    ASSERT_TRUE(connector_lane1->Start(1).ok());

    uint64_t now_ns = 1;
    const auto pump_all = [&]() -> Status {
        now_ns += 1'000'000;
        BridgePumpBudget budget;
        budget.now_ns = now_ns;
        auto connector0 = connector_lane0->Pump(budget);
        if (!connector0.ok()) return connector0.status();
        auto connector1 = connector_lane1->Pump(budget);
        if (!connector1.ok()) return connector1.status();
        auto accepted = hub->Pump(now_ns);
        if (!accepted.ok()) return accepted.status();
        auto inbound0 = inbound_lane0->Pump(budget);
        if (!inbound0.ok()) return inbound0.status();
        auto inbound1 = inbound_lane1->Pump(budget);
        return inbound1.ok() ? Status::Ok() : inbound1.status();
    };
    const auto all_ready = [&] {
        return connector_lane0->state() == BridgeConnectionState::kActive &&
               connector_lane1->state() == BridgeConnectionState::kActive &&
               inbound_lane0->state() == BridgeConnectionState::kActive &&
               inbound_lane1->state() == BridgeConnectionState::kActive;
    };
    for (size_t i = 0; i < 4000 && !all_ready(); ++i) {
        ASSERT_TRUE(pump_all().ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(all_ready());
    EXPECT_EQ(hub->stats().accepted_connections, 2u);
    EXPECT_EQ(hub->stats().dispatched_connections, 2u);
    EXPECT_EQ(server_driver->stats().listeners, 1u);

    const SourceIdentity lane0_source = SourceForLane(peer.node_id.value, 0, 2);
    const SourceIdentity lane1_source = SourceForLane(peer.node_id.value, 1, 2);
    ASSERT_NE(lane0_source.node_id, 0u);
    ASSERT_NE(lane1_source.node_id, 0u);
    ASSERT_TRUE(QueueReliable(
                    connector_lane0.get(),
                    DataFrame(lane0_source.node_id,
                              lane0_source.publisher_id, 1,
                              std::byte{0xa0}))
                    .ok());
    ASSERT_TRUE(QueueReliable(
                    connector_lane1.get(),
                    DataFrame(lane1_source.node_id,
                              lane1_source.publisher_id, 1,
                              std::byte{0xa1}))
                    .ok());
    for (size_t i = 0;
         i < 4000 && (inbound_lane0_ingress.frames.size() != 1 ||
                      inbound_lane1_ingress.frames.size() != 1);
         ++i) {
        ASSERT_TRUE(pump_all().ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(inbound_lane0_ingress.frames.size(), 1u);
    ASSERT_EQ(inbound_lane1_ingress.frames.size(), 1u);
    EXPECT_EQ(inbound_lane0_ingress.frames[0].header.source_publisher_id,
              lane0_source.publisher_id);
    EXPECT_EQ(inbound_lane1_ingress.frames[0].header.source_publisher_id,
              lane1_source.publisher_id);

    EXPECT_TRUE(connector_lane0->Shutdown().ok());
    EXPECT_TRUE(connector_lane1->Shutdown().ok());
    EXPECT_TRUE(inbound_lane0->Shutdown().ok());
    EXPECT_TRUE(inbound_lane1->Shutdown().ok());
    EXPECT_TRUE(hub->Shutdown().ok());
    EXPECT_TRUE(connector_driver->Shutdown().ok());
    EXPECT_TRUE(server_driver->Shutdown().ok());
}

TEST(BridgeListenerHubTest,
     RejectsDuplicateIncompleteMismatchedAndExcessLogicalPeers) {
    const transport::EndpointDescriptor endpoint = Loopback(FreePort());
    const BridgeNodeIdentityFence server = Fence(NodeId{920}, 20);
    const BridgeNodeIdentityFence peer = Fence(NodeId{921}, 21);
    const BridgeNodeIdentityFence other_peer = Fence(NodeId{922}, 22);
    auto driver_created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(driver_created.ok())
        << driver_created.status().ToString();
    auto driver =
        std::shared_ptr<transport::TcpDriver>(std::move(*driver_created));
    ASSERT_TRUE(driver->Start(DriverConfig()).ok());
    CollectingIngress ingress;
    const auto make_manager = [&](const BridgeNodeIdentityFence& identity,
                                  uint16_t lane_index,
                                  uint16_t lane_count) {
        auto created = BridgeConnectionManager::Create(
            ManagerOptions(BridgeConnectionMode::kAccepted, endpoint, server,
                           identity, false, lane_index, lane_count),
            driver, &ingress);
        EXPECT_TRUE(created.ok()) << created.status().ToString();
        return created.ok()
                   ? std::shared_ptr<BridgeConnectionManager>(
                         std::move(*created))
                   : std::shared_ptr<BridgeConnectionManager>{};
    };
    auto lane0 = make_manager(peer, 0, 2);
    auto lane1 = make_manager(peer, 1, 2);
    auto mismatched = make_manager(peer, 1, 3);
    auto other = make_manager(other_peer, 0, 1);
    ASSERT_NE(lane0, nullptr);
    ASSERT_NE(lane1, nullptr);
    ASSERT_NE(mismatched, nullptr);
    ASSERT_NE(other, nullptr);

    auto hub_created = BridgeListenerHub::Create(
        BridgeListenerHubOptions{
            .local_endpoint = endpoint,
            .driver_config = DriverConfig(),
            .manage_driver_lifecycle = false,
            .listen_backlog = 8,
            .max_peers = 1,
            .max_pending_handshakes = 4,
            .max_accepts_per_pump = 4,
            .handshake_timeout_ns = 2'000'000'000ull,
            .wire_limits = WireFrameLimits{
                .max_payload_length = 4096,
                .max_buffered_bytes = 8192,
            },
        },
        driver);
    ASSERT_TRUE(hub_created.ok()) << hub_created.status().ToString();
    auto hub = std::move(*hub_created);

    ASSERT_TRUE(hub->RegisterPeer(lane0).ok());
    EXPECT_EQ(hub->RegisterPeer(lane0).code(), StatusCode::kAlreadyExists);
    EXPECT_EQ(hub->RegisterPeer(mismatched).code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(hub->RegisterPeer(other).code(),
              StatusCode::kResourceExhausted);
    ASSERT_TRUE(hub->RegisterPeer(lane1).ok());
    ASSERT_TRUE(hub->UnregisterPeer(peer).ok());
    EXPECT_EQ(hub->UnregisterPeer(peer).code(), StatusCode::kNotFound);

    ASSERT_TRUE(hub->RegisterPeer(lane0).ok());
    EXPECT_EQ(hub->Start().code(), StatusCode::kInvalidArgument);
    ASSERT_TRUE(hub->RegisterPeer(lane1).ok());
    ASSERT_TRUE(hub->Start().ok());
    EXPECT_TRUE(hub->Shutdown().ok());
    EXPECT_TRUE(driver->Shutdown().ok());
}

}  // namespace
}  // namespace mino::bridge
