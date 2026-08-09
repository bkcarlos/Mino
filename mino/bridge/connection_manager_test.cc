// Copyright 2026 The Mino Authors

#include "mino/bridge/bridge_runtime/connection_manager.h"
#include "mino/bridge/bridge_runtime/connection_pool.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "mino/schema/canonical.h"
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
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
        0) {
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

ProcessIdentity Identity(NodeId node, uint64_t incarnation) {
    return ProcessIdentity{
        .node_id = node.value,
        .process_id = 10'000 + incarnation,
        .process_epoch = 20'000 + incarnation,
        .start_time_ns = 30'000 + incarnation,
    };
}

BridgeNodeIdentityFence Fence(NodeId node, uint64_t incarnation) {
    return BridgeNodeIdentityFence{
        .node_id = node,
        .process_identity = Identity(node, incarnation),
        .lease_epoch = 40'000 + incarnation,
        .node_config_version = 50'000 + incarnation,
    };
}

transport::TcpDriverOptions TcpOptions() {
    transport::TcpDriverOptions options;
    options.max_frame_body_bytes = 4096;
    options.max_total_send_buffer_bytes = 64 * 1024;
    options.max_connection_send_buffer_bytes = 32 * 1024;
    options.max_ready_receive_bytes = 64 * 1024;
    options.max_ready_receive_messages = 128;
    options.max_pending_accepts = 8;
    options.heartbeat_interval_ms = 20;
    options.idle_timeout_ms = 3000;
    options.partial_frame_timeout_ms = 1000;
    options.io_poll_max_ms = 2;
    return options;
}

BridgeConnectionManagerOptions ManagerOptions(
    BridgeConnectionMode mode,
    const transport::EndpointDescriptor& endpoint,
    BridgeNodeIdentityFence local_identity,
    BridgeNodeIdentityFence expected_peer,
    bool manage_driver_lifecycle = true,
    observability::TraceEventSink* telemetry = nullptr,
    uint16_t lane_index = 0,
    uint16_t lane_count = 1) {
    BridgeConnectionManagerOptions options;
    options.mode = mode;
    if (mode == BridgeConnectionMode::kListen) {
        options.local_endpoint = endpoint;
        options.peer_route_endpoint = endpoint;
    } else {
        options.remote_endpoint = endpoint;
    }
    options.local_identity = local_identity;
    options.expected_peer = expected_peer;
    options.route_driver_id = 71;
    options.route_driver_generation = 3;
    options.manage_driver_lifecycle = manage_driver_lifecycle;
    options.driver_config = transport::DriverConfig{
        .max_connections = 8,
        .max_listeners = 2,
        .max_queued_sends = 128,
    };
    options.listen_backlog = 4;
    options.connect_timeout_ms = 50;
    options.handshake_timeout_ns = 2'000'000'000ull;
    options.initial_reconnect_backoff_ns = 2'000'000ull;
    options.max_reconnect_backoff_ns = 32'000'000ull;
    options.health_probe_interval_ns = 2'000'000ull;
    options.max_egress_frames = 32;
    options.max_egress_bytes = 64 * 1024;
    options.telemetry = telemetry;
    options.telemetry_component_instance = 9;
    options.telemetry_hop_id = 2;
    options.pipeline.wire_limits.max_payload_length = 4096;
    options.pipeline.wire_limits.max_buffered_bytes = 8192;
    options.pipeline.retransmit.max_age_ns = 30'000'000'000ull;
    options.pipeline.retransmit.max_entries = 32;
    options.pipeline.retransmit.max_bytes = 64 * 1024;
    options.lane_index = lane_index;
    options.lane_count = lane_count;
    return options;
}

SourceIdentity SourceForLane(uint16_t lane_index, uint16_t lane_count) {
    for (uint64_t publisher_id = 1; publisher_id != 100'000; ++publisher_id) {
        const SourceIdentity source{101, publisher_id, 103};
        if (BridgeLaneFor(source, lane_count) == lane_index) return source;
    }
    return {};
}

WireFrame DataFrame(uint64_t sequence, std::byte value) {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 7;
    frame.header.msg_type = 8;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 101;
    frame.header.source_publisher_id = 102;
    frame.header.source_publisher_epoch = 103;
    frame.header.sequence_num = sequence;
    frame.payload.assign(64, value);
    return frame;
}

class CollectingIngress final : public BridgeIngressPort {
public:
    Status DecodeValidatePublish(const WireFrame& frame) override {
        if (!failure.ok()) return failure;
        frames.push_back(frame);
        return Status::Ok();
    }

    Status failure = Status::Ok();
    std::vector<WireFrame> frames;
};

class CountingTelemetrySink final : public observability::TraceEventSink {
public:
    bool TryRecordEvent(const observability::SampleKey&,
                        const observability::TraceEvent& event, size_t,
                        uint64_t) noexcept override {
        const size_t index = static_cast<size_t>(event.stage);
        if (index < counts.size()) ++counts[index];
        last_event = event;
        return true;
    }

    uint64_t Count(observability::TraceStage stage) const noexcept {
        return counts[static_cast<size_t>(stage)];
    }

    std::array<uint64_t, 32> counts{};
    observability::TraceEvent last_event;
};

struct ManagerPair {
    CountingTelemetrySink connector_telemetry;
    CountingTelemetrySink listener_telemetry;
    std::shared_ptr<transport::TcpDriver> connector_driver;
    std::shared_ptr<transport::TcpDriver> listener_driver;
    std::unique_ptr<BridgeConnectionManager> connector;
    std::unique_ptr<BridgeConnectionManager> listener;
    CollectingIngress connector_ingress;
    CollectingIngress listener_ingress;
    uint64_t now_ns = 1;
};

ManagerPair MakePair(bool listener_rejects_connector = false,
                     uint16_t connector_lane_index = 0,
                     uint16_t connector_lane_count = 1,
                     uint16_t listener_lane_index = 0,
                     uint16_t listener_lane_count = 1) {
    ManagerPair pair;
    const uint16_t port = FreePort();
    EXPECT_NE(port, 0);
    const transport::EndpointDescriptor endpoint = Loopback(port);
    auto connector_driver = transport::TcpDriver::Create(TcpOptions());
    auto listener_driver = transport::TcpDriver::Create(TcpOptions());
    EXPECT_TRUE(connector_driver.ok());
    EXPECT_TRUE(listener_driver.ok());
    if (!connector_driver.ok() || !listener_driver.ok()) return pair;
    pair.connector_driver =
        std::shared_ptr<transport::TcpDriver>(std::move(*connector_driver));
    pair.listener_driver =
        std::shared_ptr<transport::TcpDriver>(std::move(*listener_driver));
    const BridgeNodeIdentityFence connector_identity =
        Fence(NodeId{101}, 1);
    const BridgeNodeIdentityFence listener_identity =
        Fence(NodeId{202}, 2);
    const BridgeNodeIdentityFence expected_connector =
        listener_rejects_connector ? Fence(NodeId{101}, 99)
                                   : connector_identity;
    auto connector = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kConnect, endpoint,
                       connector_identity, listener_identity, true,
                       &pair.connector_telemetry, connector_lane_index,
                       connector_lane_count),
        pair.connector_driver, &pair.connector_ingress);
    auto listener = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kListen, endpoint,
                       listener_identity, expected_connector, true,
                       &pair.listener_telemetry, listener_lane_index,
                       listener_lane_count),
        pair.listener_driver, &pair.listener_ingress);
    EXPECT_TRUE(connector.ok()) << connector.status().ToString();
    EXPECT_TRUE(listener.ok()) << listener.status().ToString();
    if (connector.ok()) pair.connector = std::move(*connector);
    if (listener.ok()) pair.listener = std::move(*listener);
    if (pair.listener != nullptr) {
        EXPECT_TRUE(pair.listener->Start(pair.now_ns).ok());
    }
    if (pair.connector != nullptr) {
        EXPECT_TRUE(pair.connector->Start(pair.now_ns).ok());
    }
    return pair;
}

Status PumpUntil(ManagerPair* pair, const std::function<bool()>& done,
                 size_t iterations = 5000) {
    for (size_t i = 0; i < iterations; ++i) {
        pair->now_ns += 1'000'000;
        BridgePumpBudget budget;
        budget.now_ns = pair->now_ns;
        auto connector = pair->connector->Pump(budget);
        if (!connector.ok()) return connector.status();
        auto listener = pair->listener->Pump(budget);
        if (!listener.ok()) return listener.status();
        if (done()) return Status::Ok();
        std::this_thread::sleep_for(1ms);
    }
    return Status::Error(StatusCode::kTimeout,
                         "connection manager test timed out");
}

bool Ready(const ManagerPair& pair) {
    return pair.connector->state() == BridgeConnectionState::kActive &&
           pair.listener->state() == BridgeConnectionState::kActive &&
           pair.connector->pipeline() != nullptr &&
           pair.listener->pipeline() != nullptr &&
           pair.connector->pipeline()->session_ready() &&
           pair.listener->pipeline()->session_ready();
}

Status QueueReliable(BridgeConnectionManager* manager, uint64_t sequence,
                     std::byte value) {
    return manager->Enqueue(EncodedOutboundFrame{
        .frame = DataFrame(sequence, value),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
}

schema::SchemaIdentity Schema(uint64_t salt = 1) {
    schema::CanonicalDigest digest{};
    digest[0] = static_cast<std::byte>(salt & 0xffu);
    digest[8] = static_cast<std::byte>((salt >> 8) & 0xffu);
    digest[31] = std::byte{0x5a};
    return schema::SchemaIdentity(schema::DigestShortId(digest), digest, 7, 9);
}

BridgeRouteContract RouteContract(
    const BridgeDispatchRequest& request,
    registry::DeliveryPolicy delivery) {
    return BridgeRouteContract{
        .stamp = transport::RouteStamp{
            .topic_id = request.topic_id,
            .policy = registry::RoutePolicy::kDiscovery,
            .topic_config_version = 1,
            .route_version = 2,
            .node_registry_version = 3,
            .driver_registry_version = 4,
            .acl_validator_version = 5,
            .schema_validator_version = 6,
            .local_provider_version = 7,
        },
        .delivery = delivery,
        .payload_size = static_cast<uint32_t>(
            request.canonical_payload.size()),
        .priority = request.priority,
    };
}

class CountingDescriptorProvider final : public BridgeDescriptorProvider {
public:
    Result<std::vector<std::byte>> GetDescriptorArtifact(
        const schema::SchemaIdentity&) override {
        ++calls;
        return std::vector<std::byte>{std::byte{0xaa}, std::byte{0xbb}};
    }

    constexpr size_t artifact_size() const noexcept { return 2; }

    size_t calls = 0;
};

class BusTestAccessValidator final : public transport::RouteAccessValidator {
public:
    uint64_t version() const noexcept override { return 1; }

    Status Validate(const registry::TopicMetadata&, NodeId source,
                    NodeId target) const override {
        return source.value != 0 && target.value != 0
                   ? Status::Ok()
                   : Status::Error(StatusCode::kInvalidArgument);
    }
};

class BusTestSchemaValidator final : public transport::SchemaRouteValidator {
public:
    uint64_t version() const noexcept override { return 1; }

    Status Validate(
        const registry::TopicMetadata& topic, NodeId,
        const schema::SchemaIdentity& publisher_schema) const override {
        return registry::SchemaIdentityEqual(topic.schema, publisher_schema)
                   ? Status::Ok()
                   : Status::Error(StatusCode::kSchemaMismatch);
    }
};

class BusTestPublicationBinding final
    : public transport::LocalPublicationBinding {};

class BusTestParticipantIds final : public ParticipantIdAllocator {
public:
    Result<PublisherParticipantIdentity> AllocatePublisher() override {
        return PublisherParticipantIdentity{
            .publisher_id = PublisherId{next_publisher_++},
            .generation = next_generation_++,
        };
    }

    Result<SubscriberParticipantIdentity> AllocateSubscriber() override {
        return SubscriberParticipantIdentity{
            .subscriber_id = SubscriberId{next_subscriber_++},
            .generation = next_generation_++,
        };
    }

private:
    uint64_t next_publisher_ = 1;
    uint32_t next_subscriber_ = 1;
    uint64_t next_generation_ = 1;
};

class BusTestPublisherEndpoint final : public BusLocalPublisherEndpoint {
public:
    explicit BusTestPublisherEndpoint(
        registry::PublisherRegistration registration) noexcept
        : registration_(registration) {}

    Result<LocalPublication> Publish(std::span<const std::byte>,
                                     uint8_t) override {
        return LocalPublication{
            .source = SourceIdentity{
                registration_.owner.node_id.value,
                registration_.publisher_id.value,
                registration_.generation,
            },
            .sequence_num = next_sequence_++,
            .timestamp_ns = 1,
            .message_type = 1,
        };
    }

private:
    registry::PublisherRegistration registration_;
    uint64_t next_sequence_ = 1;
};

class BusTestLocalProvider final : public transport::LocalRouteProvider,
                                   public BusLocalEndpointProvider {
public:
    BusTestLocalProvider()
        : binding_(std::make_shared<const BusTestPublicationBinding>()) {}

    uint64_t version() const noexcept override { return 1; }

    Result<std::shared_ptr<const transport::LocalPublicationBinding>> Resolve(
        const registry::TopicMetadata&) const override {
        return std::shared_ptr<const transport::LocalPublicationBinding>(binding_);
    }

    Result<BusLocalPublisherResources> OpenPublisher(
        const registry::TopicMetadata& topic,
        const registry::PublisherRegistration& registration) override {
        return BusLocalPublisherResources{
            .binding = BusLocalResourceBinding{
                .topic_id = topic.topic_id,
                .region_version = topic.region_version,
                .channel_version = topic.channel_version,
                .acl_version = topic.acl_version,
                .publication = binding_,
            },
            .endpoint =
                std::make_shared<BusTestPublisherEndpoint>(registration),
        };
    }

    Result<BusLocalSubscriberResources> OpenSubscriber(
        const registry::TopicMetadata&,
        const registry::SubscriberRegistration&) override {
        return Status::Error(StatusCode::kUnsupported);
    }

private:
    std::shared_ptr<const BusTestPublicationBinding> binding_;
};

Result<std::vector<std::byte>> StaleHello(uint64_t sender_epoch,
                                         uint64_t receiver_epoch) {
    MINO_ASSIGN_OR_RETURN(
        auto payload,
        ControlPayloadCodec::EncodeSessionHello(SessionHello{
            .sender_session_epoch = sender_epoch,
            .receiver_session_epoch = receiver_epoch,
            .dedup_state_lost = false,
            .sources = {},
        }));
    WireFrame frame;
    frame.header.frame_type = FrameType::kSessionHello;
    frame.header.flags = FlagValue(FrameFlag::kControlFrame) |
                         FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.payload = std::move(payload);
    return WireFrameCodec::Encode(frame);
}

TEST(BridgeConnectionManagerTest, ValidatesLaneOptionsAndExposesTuple) {
    auto created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto driver = std::shared_ptr<transport::TcpDriver>(std::move(*created));
    CollectingIngress ingress;
    const transport::EndpointDescriptor endpoint = Loopback(FreePort());
    const BridgeNodeIdentityFence local = Fence(NodeId{111}, 1);
    const BridgeNodeIdentityFence peer = Fence(NodeId{222}, 2);
    const auto create = [&](uint16_t lane_index, uint16_t lane_count) {
        return BridgeConnectionManager::Create(
            ManagerOptions(BridgeConnectionMode::kConnect, endpoint, local,
                           peer, false, nullptr, lane_index, lane_count),
            driver, &ingress);
    };

    EXPECT_EQ(create(0, 0).status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(create(0, kMaxBridgeLaneCount + 1).status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(create(2, 2).status().code(), StatusCode::kInvalidArgument);
    auto valid = create(1, 2);
    ASSERT_TRUE(valid.ok()) << valid.status().ToString();
    EXPECT_EQ((*valid)->lane_index(), 1u);
    EXPECT_EQ((*valid)->lane_count(), 2u);
}

TEST(BridgeConnectionManagerTest, RejectsDiscoveryWithWrongLaneTuple) {
    ManagerPair pair = MakePair(false, 0, 2, 1, 2);
    ASSERT_NE(pair.connector, nullptr);
    ASSERT_NE(pair.listener, nullptr);
    const Status rejected = PumpUntil(&pair, [&] {
        return pair.connector->stats().protocol_failures != 0 ||
               pair.listener->stats().protocol_failures != 0;
    });
    ASSERT_TRUE(rejected.ok()) << rejected.ToString();
    EXPECT_NE(pair.connector->state(), BridgeConnectionState::kActive);
    EXPECT_NE(pair.listener->state(), BridgeConnectionState::kActive);
    EXPECT_TRUE(pair.connector->last_failure().code() ==
                    StatusCode::kPermissionDenied ||
                pair.listener->last_failure().code() ==
                    StatusCode::kPermissionDenied);
    EXPECT_TRUE(pair.connector->Shutdown().ok());
    EXPECT_TRUE(pair.listener->Shutdown().ok());
}

TEST(BridgeConnectionManagerTest, RejectsAdoptionWithWrongLaneTuple) {
    auto created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto driver = std::shared_ptr<transport::TcpDriver>(std::move(*created));
    CollectingIngress ingress;
    const transport::EndpointDescriptor endpoint = Loopback(FreePort());
    const BridgeNodeIdentityFence local = Fence(NodeId{333}, 3);
    const BridgeNodeIdentityFence peer = Fence(NodeId{444}, 4);
    auto manager_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kAccepted, endpoint, local, peer,
                       true, nullptr, 0, 2),
        driver, &ingress);
    ASSERT_TRUE(manager_created.ok())
        << manager_created.status().ToString();
    auto manager = std::move(*manager_created);
    ASSERT_TRUE(manager->Start(1).ok());

    const Status adopted = manager->AdoptAcceptedConnection(
        transport::ConnectionInfo{
            .id = 123,
            .kind = transport::TransportKind::kNetwork,
            .is_listener = false,
            .local_endpoint = endpoint,
            .peer_endpoint = endpoint,
        },
        SessionDiscovery{
            .session_epoch = 99,
            .node_id = peer.node_id,
            .process_identity = peer.process_identity,
            .lease_epoch = peer.lease_epoch,
            .node_config_version = peer.node_config_version,
            .lane_index = 1,
            .lane_count = 2,
        },
        2);
    EXPECT_EQ(adopted.code(), StatusCode::kPermissionDenied);
    EXPECT_EQ(manager->state(), BridgeConnectionState::kWaiting);
    EXPECT_EQ(manager->connection_id(), transport::kInvalidConnectionId);
    EXPECT_TRUE(manager->Shutdown().ok());
}

TEST(BridgeConnectionManagerTest,
     AutomaticallyConnectsDiscoversEpochAndPublishesOverTcp) {
    ManagerPair pair = MakePair();
    ASSERT_NE(pair.connector, nullptr);
    ASSERT_NE(pair.listener, nullptr);
    const Status connected = PumpUntil(&pair, [&] { return Ready(pair); });
    ASSERT_TRUE(connected.ok()) << connected.ToString();

    EXPECT_NE(pair.connector->local_session_epoch(), 0);
    EXPECT_NE(pair.listener->local_session_epoch(), 0);
    EXPECT_EQ(pair.connector->remote_session_epoch(),
              pair.listener->local_session_epoch());
    EXPECT_EQ(pair.listener->remote_session_epoch(),
              pair.connector->local_session_epoch());

    ASSERT_TRUE(QueueReliable(pair.connector.get(), 1, std::byte{0x41}).ok());
    const Status delivered = PumpUntil(&pair, [&] {
        return pair.listener_ingress.frames.size() == 1 &&
               pair.connector->pipeline()->retransmit_entries() == 0;
    });
    ASSERT_TRUE(delivered.ok()) << delivered.ToString();
    EXPECT_EQ(pair.listener_ingress.frames.front().payload,
              std::vector<std::byte>(64, std::byte{0x41}));
    EXPECT_EQ(pair.connector->queued_egress_frames(), 0u);
    EXPECT_GT(pair.connector_telemetry.Count(
                  observability::TraceStage::kSocketWriteComplete),
              0u);
    EXPECT_GT(pair.listener_telemetry.Count(
                  observability::TraceStage::kRemoteFrameComplete),
              0u);
    EXPECT_EQ(pair.connector_telemetry.last_event.component_instance, 9u);
    EXPECT_EQ(pair.connector_telemetry.last_event.hop_id, 2u);
    ASSERT_TRUE(QueueReliable(pair.connector.get(), 99, std::byte{0x7f}).ok());
    EXPECT_EQ(pair.connector->queued_egress_frames(), 1u);
    EXPECT_TRUE(pair.connector->Shutdown().ok());
    EXPECT_EQ(pair.connector->queued_egress_frames(), 0u);
    EXPECT_EQ(pair.connector->stats().discarded_egress_frames, 1u);
    const Status stopped_enqueue =
        QueueReliable(pair.connector.get(), 100, std::byte{0x7e});
    EXPECT_EQ(stopped_enqueue.code(), StatusCode::kUnavailable);
    EXPECT_TRUE(pair.listener->Shutdown().ok());
    EXPECT_EQ(pair.connector_driver->state(), transport::DriverState::kStopped);
    EXPECT_EQ(pair.listener_driver->state(), transport::DriverState::kStopped);
}

TEST(BridgeConnectionManagerTest,
     ReconnectsFencesStaleEpochAndRecoversQueuedMessages) {
    ManagerPair pair = MakePair();
    ASSERT_NE(pair.connector, nullptr);
    ASSERT_NE(pair.listener, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] { return Ready(pair); }).ok());
    const uint64_t first_connector_epoch =
        pair.connector->local_session_epoch();
    const uint64_t first_listener_epoch = pair.listener->local_session_epoch();

    ASSERT_TRUE(pair.connector_driver
                    ->Close(pair.connector->connection_id())
                    .ok());
    ASSERT_TRUE(QueueReliable(pair.connector.get(), 2, std::byte{0x52}).ok());
    const Status recovered = PumpUntil(&pair, [&] {
        return Ready(pair) && pair.connector->stats().reconnects >= 1 &&
               pair.listener->stats().reconnects >= 1 &&
               pair.listener_ingress.frames.size() == 1 &&
               pair.connector->pipeline()->retransmit_entries() == 0;
    });
    ASSERT_TRUE(recovered.ok()) << recovered.ToString();
    EXPECT_NE(pair.connector->local_session_epoch(), first_connector_epoch);
    EXPECT_NE(pair.listener->local_session_epoch(), first_listener_epoch);
    EXPECT_GT(pair.connector_telemetry.Count(
                  observability::TraceStage::kBridgeReconnect),
              0u);
    EXPECT_GT(pair.listener_telemetry.Count(
                  observability::TraceStage::kBridgeReconnect),
              0u);
    EXPECT_EQ(pair.listener_ingress.frames.front().payload,
              std::vector<std::byte>(64, std::byte{0x52}));

    const uint64_t pre_fence_connector_epoch =
        pair.connector->local_session_epoch();
    auto stale = StaleHello(first_listener_epoch,
                            pair.connector->local_session_epoch());
    ASSERT_TRUE(stale.ok()) << stale.status().ToString();
    ASSERT_TRUE(pair.listener_driver
                    ->SendUntracked(transport::UntrackedSendRequest{
                        .connection_id = pair.listener->connection_id(),
                        .payload = *stale,
                        .traffic_class = transport::UntrackedTrafficClass::
                            kProtocolControl,
                    })
                    .ok());
    const Status fenced = PumpUntil(&pair, [&] {
        return pair.connector->stats().protocol_failures >= 1 && Ready(pair) &&
               pair.connector->stats().reconnects >= 2;
    });
    ASSERT_TRUE(fenced.ok()) << fenced.ToString();
    EXPECT_NE(pair.connector->local_session_epoch(),
              pre_fence_connector_epoch);

    ASSERT_TRUE(QueueReliable(pair.connector.get(), 3, std::byte{0x63}).ok());
    const Status resumed = PumpUntil(&pair, [&] {
        return pair.listener_ingress.frames.size() == 2 &&
               pair.connector->pipeline()->retransmit_entries() == 0;
    });
    ASSERT_TRUE(resumed.ok()) << resumed.ToString();
    EXPECT_EQ(pair.listener_ingress.frames[1].payload,
              std::vector<std::byte>(64, std::byte{0x63}));
    EXPECT_EQ(pair.listener_ingress.frames.size(), 2u);
    EXPECT_TRUE(pair.connector->Shutdown().ok());
    EXPECT_TRUE(pair.listener->Shutdown().ok());
}

TEST(BridgeConnectionManagerTest,
     ListenerRejectsUnexpectedProcessLeaseAndConfigIdentity) {
    ManagerPair pair = MakePair(true);
    ASSERT_NE(pair.connector, nullptr);
    ASSERT_NE(pair.listener, nullptr);
    const Status rejected = PumpUntil(&pair, [&] {
        return pair.listener->stats().protocol_failures >= 1;
    });
    ASSERT_TRUE(rejected.ok()) << rejected.ToString();
    EXPECT_NE(pair.listener->state(), BridgeConnectionState::kActive);
    EXPECT_EQ(pair.listener->pipeline(), nullptr);
    EXPECT_EQ(pair.listener->last_failure().code(),
              StatusCode::kPermissionDenied);
    EXPECT_TRUE(pair.connector->Shutdown().ok());
    EXPECT_TRUE(pair.listener->Shutdown().ok());
}

TEST(BridgeConnectionManagerTest,
     PeerSchemaMismatchClosesConnectionAndSchedulesReconnect) {
    ManagerPair pair = MakePair();
    ASSERT_NE(pair.connector, nullptr);
    ASSERT_NE(pair.listener, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] { return Ready(pair); }).ok());
    pair.listener_ingress.failure = Status::Error(
        StatusCode::kSchemaMismatch, "peer schema contract rejected frame");
    ASSERT_TRUE(QueueReliable(pair.connector.get(), 1, std::byte{0x44}).ok());
    const Status rejected = PumpUntil(&pair, [&] {
        return pair.listener->stats().protocol_failures >= 1 &&
               pair.listener->state() == BridgeConnectionState::kWaiting;
    });
    ASSERT_TRUE(rejected.ok()) << rejected.ToString();
    EXPECT_EQ(pair.listener->last_failure().code(),
              StatusCode::kSchemaMismatch);
    EXPECT_TRUE(pair.connector->Shutdown().ok());
    EXPECT_TRUE(pair.listener->Shutdown().ok());
}

TEST(BridgeConnectionManagerTest,
     SharedDriverIsNotStoppedAndShutdownDiscardsQueuedEgress) {
    auto created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto driver = std::shared_ptr<transport::TcpDriver>(std::move(*created));
    const transport::DriverConfig config{
        .max_connections = 8,
        .max_listeners = 2,
        .max_queued_sends = 128,
    };
    ASSERT_TRUE(driver->Start(config).ok());
    const transport::EndpointDescriptor endpoint = Loopback(FreePort());
    CollectingIngress first_ingress;
    CollectingIngress second_ingress;
    const BridgeNodeIdentityFence local = Fence(NodeId{301}, 3);
    const BridgeNodeIdentityFence peer = Fence(NodeId{302}, 4);
    auto first_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kConnect, endpoint, local, peer,
                       false),
        driver, &first_ingress);
    auto second_created = BridgeConnectionManager::Create(
        ManagerOptions(BridgeConnectionMode::kConnect, endpoint, local, peer,
                       false),
        driver, &second_ingress);
    ASSERT_TRUE(first_created.ok()) << first_created.status().ToString();
    ASSERT_TRUE(second_created.ok()) << second_created.status().ToString();
    auto first = std::move(*first_created);
    auto second = std::move(*second_created);
    ASSERT_TRUE(first->Start(1).ok());
    ASSERT_TRUE(second->Start(1).ok());
    ASSERT_TRUE(QueueReliable(first.get(), 1, std::byte{0x31}).ok());
    ASSERT_TRUE(first->Shutdown().ok());
    EXPECT_EQ(first->queued_egress_frames(), 0u);
    EXPECT_EQ(first->stats().discarded_egress_frames, 1u);
    EXPECT_EQ(driver->state(), transport::DriverState::kRunning);
    EXPECT_EQ(QueueReliable(first.get(), 2, std::byte{0x32}).code(),
              StatusCode::kUnavailable);
    ASSERT_TRUE(second->Shutdown().ok());
    EXPECT_EQ(driver->state(), transport::DriverState::kRunning);
    EXPECT_TRUE(driver->Shutdown().ok());
}

TEST(BridgeRuntimeDispatcherTest,
     RealBusCanPublishSameRouteBeyondDefaultBindingLimit) {
    const NodeId local_node{501};
    auto coordinator_created = registry::Coordinator::CreateForTesting();
    ASSERT_TRUE(coordinator_created.ok())
        << coordinator_created.status().ToString();
    auto coordinator = std::shared_ptr<registry::Coordinator>(
        std::move(*coordinator_created));

    auto endpoint = transport::EndpointDescriptor::SharedFabric(7, 11);
    ASSERT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    const uint64_t now_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    registry::NodeRegistration node{
        .node_id = local_node,
        .process_identity = Identity(local_node, 8),
        .endpoints = {*endpoint},
        .trust_domain = "bridge-bus-test",
        .health = registry::NodeHealth::kHealthy,
        .lease_epoch = 1,
        .lease_duration_ns = registry::kMaxLeaseDurationNs,
        .config_version = 1,
    };
    auto registered = coordinator->RegisterNode(node, now_ns);
    ASSERT_TRUE(registered.ok()) << registered.status().ToString();
    const registry::NodeLeaseOwner owner{
        .node_id = local_node,
        .process_identity = node.process_identity,
        .lease_epoch = node.lease_epoch,
    };

    auto access = std::make_shared<BusTestAccessValidator>();
    auto schema_validator = std::make_shared<BusTestSchemaValidator>();
    auto local_provider = std::make_shared<BusTestLocalProvider>();
    auto switcher_created = transport::TransportSwitcher::Create(
        local_node, coordinator.get(), access, schema_validator, local_provider);
    ASSERT_TRUE(switcher_created.ok())
        << switcher_created.status().ToString();
    auto switcher = std::shared_ptr<transport::TransportSwitcher>(
        std::move(*switcher_created));
    auto dispatcher_created = BridgeRuntimeDispatcher::Create();
    ASSERT_TRUE(dispatcher_created.ok())
        << dispatcher_created.status().ToString();
    auto dispatcher = *dispatcher_created;
    auto participant_ids = std::make_shared<BusTestParticipantIds>();
    auto bus_created = Bus::Create(owner, coordinator, switcher, participant_ids,
                                   local_provider, dispatcher);
    ASSERT_TRUE(bus_created.ok()) << bus_created.status().ToString();
    auto bus = std::move(*bus_created);

    const schema::SchemaIdentity schema = Schema(0x501);
    registry::TopicMetadata candidate{
        .topic_id = {},
        .name = "bridge/bus/repeated-route",
        .channel_kind = registry::ChannelKind::kBroadcast,
        .delivery = registry::DeliveryPolicy{
            .reliability = registry::Reliability::kBestEffort,
            .allow_drop = false,
        },
        .queue_full_policy = QueueFullPolicy::kBlock,
        .schema = schema,
        .accepted_schemas = {},
        .route_policy = registry::RoutePolicy::kStatic,
        .static_routes = {{
            .target_node = local_node,
            .preferred_transport = std::nullopt,
        }},
        .route_set_version = 0,
        .capacity = 64,
        .max_publishers = 1,
        .max_subscribers = 1,
        .partition_count = 1,
        .record_topology =
            registry::RecordBackpressureTopology::kIsolated,
        .region_version = 1,
        .channel_version = 1,
        .acl_version = 1,
        .config_version = 0,
        .state = registry::TopicState::kCreating,
    };
    auto topic_created = coordinator->CreateTopic(std::move(candidate));
    ASSERT_TRUE(topic_created.ok()) << topic_created.status().ToString();
    const registry::TopicMetadata& topic = (*topic_created)->metadata;
    const registry::ActivationReadinessProof proof{
        .topic_id = topic.topic_id,
        .config_version = topic.config_version,
        .schema = topic.schema,
        .region_version = topic.region_version,
        .channel_version = topic.channel_version,
        .acl_version = topic.acl_version,
        .schema_ready = true,
        .region_ready = true,
        .channel_ready = true,
        .acl_ready = true,
    };
    ASSERT_TRUE(coordinator->ActivateTopic(topic.topic_id, proof).ok());

    auto publisher = bus->CreatePublisher(topic.topic_id, schema);
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    const std::array<std::byte, 1> payload = {std::byte{0x5a}};
    std::shared_ptr<const transport::RouteHandle> previous_route;
    std::weak_ptr<const transport::RouteHandle> first_route;
    constexpr size_t kPublishCount = 5000;
    for (size_t i = 0; i < kPublishCount; ++i) {
        auto published = publisher->Publish(payload, 3);
        ASSERT_TRUE(published.ok())
            << "publish " << i << ": " << published.status().ToString();
        ASSERT_TRUE(published->bridge_status.ok())
            << "bridge publish " << i << ": "
            << published->bridge_status.ToString();
        ASSERT_NE(published->route, nullptr);
        if (i == 0) first_route = published->route;
        if (previous_route != nullptr) {
            EXPECT_NE(published->route.get(), previous_route.get());
        }
        previous_route = published->route;
    }
    EXPECT_TRUE(first_route.expired());
}

TEST(BridgeRuntimeDispatcherTest,
     SortsBindingsByTopicAndFencesOnlyMatchingRouteContracts) {
    constexpr size_t kMaxRouteBindings = 11;
    auto dispatcher_created =
        BridgeRuntimeDispatcher::Create(1, {}, kMaxRouteBindings);
    ASSERT_TRUE(dispatcher_created.ok())
        << dispatcher_created.status().ToString();
    auto dispatcher = *dispatcher_created;

    const std::array<std::byte, 4> payload = {
        std::byte{0x10}, std::byte{0x20}, std::byte{0x30}, std::byte{0x40}};
    const registry::DeliveryPolicy delivery{
        .reliability = registry::Reliability::kBestEffort,
        .allow_drop = false,
    };
    const transport::TargetRoute first_target{
        .target_node = NodeId{1},
        .transport = transport::LocalTargetRoute{},
    };
    const transport::TargetRoute second_target{
        .target_node = NodeId{2},
        .transport = transport::LocalTargetRoute{},
    };
    uint64_t sequence = 1;
    const auto dispatch = [&](uint32_t topic_value,
                              schema::SchemaIdentity schema_identity,
                              const transport::TargetRoute& target,
                              uint64_t route_version_delta) {
        BridgeDispatchRequest request{
            .topic_id = TopicId{topic_value},
            .schema = std::move(schema_identity),
            .publication = LocalPublication{
                .source = SourceIdentity{1, 2, 3},
                .sequence_num = sequence++,
                .timestamp_ns = 4,
                .message_type = 5,
            },
            .priority = 6,
            .canonical_payload = payload,
            .route = {},
        };
        BridgeRouteContract route = RouteContract(request, delivery);
        route.stamp.route_version += route_version_delta;
        return dispatcher->DispatchTargets(
            request, std::span<const transport::TargetRoute>(&target, 1),
            route);
    };

    constexpr std::array<uint32_t, 9> kNonSortedTopics = {
        90, 10, 70, 30, 80, 20, 60, 40, 50};
    for (const uint32_t topic : kNonSortedTopics) {
        const Status status = dispatch(topic, Schema(topic), first_target, 0);
        ASSERT_TRUE(status.ok())
            << "topic " << topic << ": " << status.ToString();
    }

    constexpr std::array<uint32_t, 3> kRevisitedTopics = {10, 50, 90};
    for (const uint32_t topic : kRevisitedTopics) {
        EXPECT_TRUE(dispatch(topic, Schema(topic), first_target, 0).ok())
            << "failed to revisit topic " << topic;
    }

    EXPECT_EQ(dispatch(50, Schema(5'000), first_target, 0).code(),
              StatusCode::kSchemaMismatch);
    EXPECT_TRUE(dispatch(50, Schema(5'000), first_target, 1).ok());
    EXPECT_TRUE(dispatch(50, Schema(6'000), second_target, 0).ok());
    EXPECT_EQ(dispatch(50, Schema(6'000), first_target, 1).code(),
              StatusCode::kSchemaMismatch);
    EXPECT_EQ(dispatch(50, Schema(5'000), second_target, 0).code(),
              StatusCode::kSchemaMismatch);

    EXPECT_EQ(dispatch(100, Schema(100), first_target, 0).code(),
              StatusCode::kResourceExhausted);
    EXPECT_EQ(dispatch(50, Schema(7'000), first_target, 2).code(),
              StatusCode::kResourceExhausted);
    for (const uint32_t topic : kRevisitedTopics) {
        EXPECT_TRUE(dispatch(topic, Schema(topic), first_target, 0).ok())
            << "full table rejected existing topic " << topic;
    }
}

TEST(BridgeRuntimeDispatcherTest,
     SelectsStableSourceLaneWithoutFallbackOrQueueMultiplication) {
    auto created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto driver = std::shared_ptr<transport::TcpDriver>(std::move(*created));
    ASSERT_TRUE(driver
                    ->Start(transport::DriverConfig{
                        .max_connections = 8,
                        .max_listeners = 1,
                        .max_queued_sends = 128,
                    })
                    .ok());
    const transport::EndpointDescriptor endpoint = Loopback(FreePort());
    const BridgeNodeIdentityFence local = Fence(NodeId{401}, 5);
    const BridgeNodeIdentityFence peer = Fence(NodeId{402}, 6);
    CollectingIngress ingress;
    std::vector<std::shared_ptr<BridgeConnectionManager>> lanes;
    for (uint16_t lane = 0; lane < 2; ++lane) {
        BridgeConnectionManagerOptions options = ManagerOptions(
            BridgeConnectionMode::kConnect, endpoint, local, peer, false,
            nullptr, lane, 2);
        options.route_driver_id = 901;
        options.route_driver_generation = 12;
        options.max_egress_frames = 2;
        auto manager = BridgeConnectionManager::Create(options, driver, &ingress);
        ASSERT_TRUE(manager.ok()) << manager.status().ToString();
        lanes.push_back(std::shared_ptr<BridgeConnectionManager>(
            std::move(*manager)));
    }
    auto pool_created = BridgeConnectionPool::Create(lanes, 2, 64 * 1024);
    ASSERT_TRUE(pool_created.ok()) << pool_created.status().ToString();
    auto pool = *pool_created;
    ASSERT_TRUE(pool->Start(1).ok());
    auto dispatcher_created = BridgeRuntimeDispatcher::Create(4);
    ASSERT_TRUE(dispatcher_created.ok())
        << dispatcher_created.status().ToString();
    auto dispatcher = *dispatcher_created;
    ASSERT_TRUE(dispatcher->RegisterPeer(peer.node_id, pool).ok());

    const transport::RemoteTargetRoute remote{
        .endpoint = endpoint,
        .node_config_version = peer.node_config_version,
        .process_identity = peer.process_identity,
        .lease_epoch = peer.lease_epoch,
        .driver_id = 901,
        .driver_generation = 12,
        .capabilities = driver->capabilities(),
        .driver = driver,
    };
    const transport::TargetRoute target{
        .target_node = peer.node_id,
        .transport = remote,
    };
    const std::vector<std::byte> payload(16, std::byte{0x5a});
    const registry::DeliveryPolicy reliable{
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
    };
    for (uint16_t lane = 0; lane < 2; ++lane) {
        const SourceIdentity source = SourceForLane(lane, 2);
        ASSERT_NE(source.publisher_id, 0u);
        BridgeDispatchRequest request{
            .topic_id = TopicId{77},
            .schema = Schema(0x7788),
            .publication = LocalPublication{
                .source = source,
                .sequence_num = 1,
                .timestamp_ns = 700,
                .message_type = 8,
            },
            .priority = 3,
            .canonical_payload = payload,
            .route = {},
        };
        ASSERT_TRUE(dispatcher
                        ->DispatchTargets(
                            request,
                            std::span<const transport::TargetRoute>(&target, 1),
                            RouteContract(request, reliable))
                        .ok());
        EXPECT_EQ(pool->manager(lane).queued_egress_frames(), 1u);
    }
    EXPECT_EQ(pool->queued_egress_frames(), 2u);

    const SourceIdentity lane0 = SourceForLane(0, 2);
    BridgeDispatchRequest full{
        .topic_id = TopicId{77},
        .schema = Schema(0x7788),
        .publication = LocalPublication{
            .source = lane0,
            .sequence_num = 2,
            .timestamp_ns = 701,
            .message_type = 8,
        },
        .priority = 3,
        .canonical_payload = payload,
        .route = {},
    };
    EXPECT_EQ(dispatcher
                  ->DispatchTargets(
                      full,
                      std::span<const transport::TargetRoute>(&target, 1),
                      RouteContract(full, reliable))
                  .code(),
              StatusCode::kWouldBlock);
    EXPECT_EQ(pool->manager(0).queued_egress_frames(), 1u);
    EXPECT_EQ(pool->manager(1).queued_egress_frames(), 1u);

    ASSERT_TRUE(pool->manager(0).Shutdown().ok());
    EXPECT_EQ(dispatcher
                  ->DispatchTargets(
                      full,
                      std::span<const transport::TargetRoute>(&target, 1),
                      RouteContract(full, reliable))
                  .code(),
              StatusCode::kUnavailable);
    EXPECT_EQ(pool->manager(1).queued_egress_frames(), 1u);
    EXPECT_TRUE(pool->Shutdown().ok());
    EXPECT_TRUE(driver->Shutdown().ok());
}

TEST(BridgeRuntimeDispatcherTest,
     DerivesMessageTypePreflightsCapabilityAndFencesEveryRouteContract) {
    auto created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto driver = std::shared_ptr<transport::TcpDriver>(std::move(*created));
    ASSERT_TRUE(driver
                    ->Start(transport::DriverConfig{
                        .max_connections = 8,
                        .max_listeners = 2,
                        .max_queued_sends = 128,
                    })
                    .ok());
    const transport::EndpointDescriptor endpoint = Loopback(FreePort());
    const BridgeNodeIdentityFence local = Fence(NodeId{401}, 5);
    const BridgeNodeIdentityFence peer = Fence(NodeId{402}, 6);
    BridgeConnectionManagerOptions options = ManagerOptions(
        BridgeConnectionMode::kConnect, endpoint, local, peer, false);
    options.route_driver_id = 901;
    options.route_driver_generation = 12;
    CollectingIngress ingress;
    auto manager_created = BridgeConnectionManager::Create(
        options, driver, &ingress);
    ASSERT_TRUE(manager_created.ok())
        << manager_created.status().ToString();
    auto manager = std::shared_ptr<BridgeConnectionManager>(
        std::move(*manager_created));
    ASSERT_TRUE(manager->Start(1).ok());

    auto descriptors = std::make_shared<CountingDescriptorProvider>();
    auto dispatcher_created =
        BridgeRuntimeDispatcher::Create(4, descriptors);
    ASSERT_TRUE(dispatcher_created.ok())
        << dispatcher_created.status().ToString();
    auto dispatcher = *dispatcher_created;
    ASSERT_TRUE(dispatcher->RegisterPeer(peer.node_id, manager).ok());

    transport::RemoteTargetRoute remote{
        .endpoint = endpoint,
        .node_config_version = peer.node_config_version,
        .process_identity = peer.process_identity,
        .lease_epoch = peer.lease_epoch,
        .driver_id = options.route_driver_id,
        .driver_generation = options.route_driver_generation,
        .capabilities = driver->capabilities(),
        .driver = driver,
    };
    const transport::TargetRoute remote_target{
        .target_node = peer.node_id,
        .transport = remote,
    };
    EXPECT_TRUE(manager->MatchesRoute(peer.node_id, remote));
    EXPECT_FALSE(manager->MatchesRoute(NodeId{999}, remote));
    auto stale = remote;
    ++stale.node_config_version;
    EXPECT_FALSE(manager->MatchesRoute(peer.node_id, stale));
    stale = remote;
    ++stale.lease_epoch;
    EXPECT_FALSE(manager->MatchesRoute(peer.node_id, stale));
    stale = remote;
    ++stale.process_identity.process_epoch;
    EXPECT_FALSE(manager->MatchesRoute(peer.node_id, stale));
    stale = remote;
    ++stale.driver_generation;
    EXPECT_FALSE(manager->MatchesRoute(peer.node_id, stale));

    const std::vector<std::byte> payload(16, std::byte{0x5a});
    BridgeDispatchRequest request{
        .topic_id = TopicId{77},
        .schema = Schema(0x7788),
        .publication = LocalPublication{
            .source = SourceIdentity{401, 501, 601},
            .sequence_num = 1,
            .timestamp_ns = 700,
            .message_type = 0xdeadbeefu,
        },
        .priority = 3,
        .canonical_payload = payload,
        .route = {},
    };
    const registry::DeliveryPolicy reliable{
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
    };
    const BridgeRouteContract route = RouteContract(request, reliable);

    const transport::TargetRoute local_target{
        .target_node = local.node_id,
        .transport = transport::LocalTargetRoute{},
    };
    EXPECT_TRUE(dispatcher
                    ->DispatchTargets(
                        request,
                        std::span<const transport::TargetRoute>(&local_target, 1),
                        route)
                    .ok());
    EXPECT_EQ(descriptors->calls, 0u);

    BridgeDispatchRequest wrong_topic = request;
    wrong_topic.topic_id = TopicId{78};
    EXPECT_EQ(dispatcher
                  ->DispatchTargets(
                      wrong_topic,
                      std::span<const transport::TargetRoute>(&local_target, 1),
                      route)
                  .code(),
              StatusCode::kInvalidArgument);
    const std::vector<std::byte> wrong_payload(15, std::byte{0x5a});
    BridgeDispatchRequest wrong_size = request;
    wrong_size.canonical_payload = wrong_payload;
    EXPECT_EQ(dispatcher
                  ->DispatchTargets(
                      wrong_size,
                      std::span<const transport::TargetRoute>(&local_target, 1),
                      route)
                  .code(),
              StatusCode::kInvalidArgument);
    BridgeDispatchRequest wrong_priority = request;
    ++wrong_priority.priority;
    EXPECT_EQ(dispatcher
                  ->DispatchTargets(
                      wrong_priority,
                      std::span<const transport::TargetRoute>(&local_target, 1),
                      route)
                  .code(),
              StatusCode::kInvalidArgument);
    BridgeDispatchRequest wrong_schema = request;
    wrong_schema.schema = Schema(0x8899);
    EXPECT_EQ(dispatcher
                  ->DispatchTargets(
                      wrong_schema,
                      std::span<const transport::TargetRoute>(&local_target, 1),
                      route)
                  .code(),
              StatusCode::kSchemaMismatch);
    BridgeRouteContract unknown_policy = route;
    unknown_policy.stamp.policy =
        static_cast<registry::RoutePolicy>(255);
    EXPECT_EQ(dispatcher
                  ->DispatchTargets(
                      request,
                      std::span<const transport::TargetRoute>(&local_target, 1),
                      unknown_policy)
                  .code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(descriptors->calls, 0u);

    transport::TargetRoute no_ack_target = remote_target;
    auto& no_ack =
        std::get<transport::RemoteTargetRoute>(no_ack_target.transport);
    no_ack.capabilities.features = no_ack.capabilities.features.Without(
        transport::Capability::kRemoteAcceptedConfirmation);
    const Status unsupported = dispatcher->DispatchTargets(
        request,
        std::span<const transport::TargetRoute>(&no_ack_target, 1), route);
    EXPECT_EQ(unsupported.code(), StatusCode::kUnsupported);
    EXPECT_EQ(descriptors->calls, 0u);
    EXPECT_EQ(manager->queued_egress_frames(), 0u);

    WireFrameHeader announcement_header;
    announcement_header.frame_type = FrameType::kSchemaAnnounce;
    announcement_header.flags = FlagValue(FrameFlag::kControlFrame);
    auto announcement_size = WireFrameCodec::EncodedSize(
        announcement_header,
        kSchemaAnnouncementFixedPayloadBytes + descriptors->artifact_size());
    ASSERT_TRUE(announcement_size.ok())
        << announcement_size.status().ToString();
    ASSERT_LE(*announcement_size, std::numeric_limits<uint32_t>::max());

    transport::TargetRoute oversized_target = remote_target;
    auto& oversized =
        std::get<transport::RemoteTargetRoute>(oversized_target.transport);
    oversized.capabilities.max_frame_size =
        static_cast<uint32_t>(*announcement_size - 1);
    const Status too_large = dispatcher->DispatchTargets(
        request,
        std::span<const transport::TargetRoute>(&oversized_target, 1), route);
    EXPECT_EQ(too_large.code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(descriptors->calls, 1u);
    EXPECT_EQ(manager->queued_egress_frames(), 0u);

    transport::TargetRoute exact_limit_target = remote_target;
    auto& exact_limit =
        std::get<transport::RemoteTargetRoute>(exact_limit_target.transport);
    exact_limit.capabilities.max_frame_size =
        static_cast<uint32_t>(*announcement_size);
    ASSERT_TRUE(dispatcher
                    ->DispatchTargets(
                        request,
                        std::span<const transport::TargetRoute>(
                            &exact_limit_target, 1),
                        route)
                    .ok());
    EXPECT_EQ(descriptors->calls, 2u);
    auto outbound = manager->TryPeekAndEncode();
    ASSERT_TRUE(outbound.ok()) << outbound.status().ToString();
    EXPECT_EQ(outbound->frame.header.msg_type,
              static_cast<uint32_t>(request.schema.short_id()));
    EXPECT_NE(outbound->frame.header.msg_type,
              request.publication.message_type);
    EXPECT_EQ(outbound->descriptor_artifact,
              (std::vector<std::byte>{std::byte{0xaa}, std::byte{0xbb}}));

    const BridgeNodeIdentityFence peer_two = Fence(NodeId{403}, 7);
    BridgeConnectionManagerOptions second_options = ManagerOptions(
        BridgeConnectionMode::kConnect, endpoint, local, peer_two, false);
    second_options.route_driver_id = options.route_driver_id;
    second_options.route_driver_generation = options.route_driver_generation;
    second_options.max_egress_frames = 1;
    CollectingIngress second_ingress;
    auto second_created = BridgeConnectionManager::Create(
        second_options, driver, &second_ingress);
    ASSERT_TRUE(second_created.ok()) << second_created.status().ToString();
    auto second = std::shared_ptr<BridgeConnectionManager>(
        std::move(*second_created));
    ASSERT_TRUE(second->Start(1).ok());
    ASSERT_TRUE(dispatcher->RegisterPeer(peer_two.node_id, second).ok());
    ASSERT_TRUE(QueueReliable(second.get(), 90, std::byte{0x70}).ok());
    transport::RemoteTargetRoute remote_two = remote;
    remote_two.node_config_version = peer_two.node_config_version;
    remote_two.process_identity = peer_two.process_identity;
    remote_two.lease_epoch = peer_two.lease_epoch;
    const std::array<transport::TargetRoute, 2> fanout_targets = {
        remote_target,
        transport::TargetRoute{
            .target_node = peer_two.node_id,
            .transport = remote_two,
        },
    };
    BridgeDispatchRequest second_request = request;
    second_request.publication.sequence_num = 2;
    const Status fanout_full = dispatcher->DispatchTargets(
        second_request, fanout_targets, route);
    EXPECT_EQ(fanout_full.code(), StatusCode::kWouldBlock);
    EXPECT_EQ(manager->queued_egress_frames(), 1u);
    EXPECT_EQ(second->queued_egress_frames(), 1u);
    EXPECT_EQ(descriptors->calls, 3u);

    ASSERT_TRUE(second->TryPeekAndEncode().ok());
    second->CommitPolled();
    BridgeDispatchRequest third_request = request;
    third_request.publication.sequence_num = 3;
    ASSERT_TRUE(dispatcher
                    ->DispatchTargets(third_request, fanout_targets, route)
                    .ok());
    EXPECT_EQ(manager->queued_egress_frames(), 2u);
    EXPECT_EQ(second->queued_egress_frames(), 1u);
    EXPECT_TRUE(second->TryPeekAndEncode().ok());
    EXPECT_EQ(descriptors->calls, 4u);

    EXPECT_TRUE(second->Shutdown().ok());
    EXPECT_TRUE(manager->Shutdown().ok());
    EXPECT_EQ(driver->state(), transport::DriverState::kRunning);
    EXPECT_TRUE(driver->Shutdown().ok());
}

}  // namespace
}  // namespace mino::bridge
