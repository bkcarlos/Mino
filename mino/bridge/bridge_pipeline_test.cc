// Copyright 2026 The Mino Authors

#include "mino/bridge/bridge_pipeline.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "mino/schema/compiler.h"
#include "mino/transport/tcp_driver.h"

namespace mino::bridge {
namespace {

using namespace std::chrono_literals;

class ScopedFd final {
public:
    explicit ScopedFd(int fd) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) (void)::close(fd_);
    }
    int get() const noexcept { return fd_; }

private:
    int fd_;
};

uint16_t FreePort() {
    ScopedFd fd(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    EXPECT_EQ(::bind(fd.get(), reinterpret_cast<sockaddr*>(&address),
                     sizeof(address)),
              0);
    socklen_t size = sizeof(address);
    EXPECT_EQ(::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&address),
                            &size),
              0);
    return ntohs(address.sin_port);
}

transport::EndpointDescriptor Loopback(uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = transport::EndpointDescriptor::Ipv4Tcp(address, port);
    EXPECT_TRUE(endpoint.ok());
    return endpoint.ok() ? *endpoint : transport::EndpointDescriptor{};
}

class QueueEgress final : public BridgeEgressPort {
public:
    Result<EncodedOutboundFrame> TryPeekAndEncode() override {
        if (frames.empty()) {
            return Status::Error(StatusCode::kWouldBlock);
        }
        return frames.front();
    }

    void CommitPolled() noexcept override { frames.pop_front(); }

    std::deque<EncodedOutboundFrame> frames;
};

class RecordingIngress final : public BridgeIngressPort {
public:
    Status DecodeValidatePublish(const WireFrame& frame) override {
        if (fail) return Status::Error(StatusCode::kUnavailable);
        frames.push_back(frame);
        return Status::Ok();
    }

    bool fail = false;
    std::vector<WireFrame> frames;
};

class RecordingTopicAuthorizer final : public BridgeTopicAuthorizer {
public:
    Status AuthorizeInbound(
        const security::AuthenticatedPeer& peer,
        TopicId topic_id) const noexcept override {
        ++calls;
        last_source = peer.node_id;
        last_topic = topic_id;
        return status;
    }

    mutable size_t calls = 0;
    mutable NodeId last_source;
    mutable TopicId last_topic;
    Status status = Status::Ok();
};

SourceIdentity SourceForLane(uint16_t lane_index, uint16_t lane_count) {
    for (uint64_t publisher_id = 1; publisher_id != 10'000; ++publisher_id) {
        const SourceIdentity source{11, publisher_id, 33};
        if (BridgeLaneFor(source, lane_count) == lane_index) return source;
    }
    return {};
}

void SetSource(WireFrame* frame, const SourceIdentity& source) {
    frame->header.source_node_id = source.node_id;
    frame->header.source_publisher_id = source.publisher_id;
    frame->header.source_publisher_epoch = source.publisher_epoch;
}

WireFrame DataFrame(uint64_t sequence, std::byte value = std::byte{0x42}) {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 1;
    frame.header.msg_type = 2;
    frame.header.connection_schema_ref = 3;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 11;
    frame.header.source_publisher_id = 22;
    frame.header.source_publisher_epoch = 33;
    frame.header.sequence_num = sequence;
    frame.payload.assign(32, value);
    return frame;
}

struct ConnectedPipelines {
    std::shared_ptr<transport::TcpDriver> a_driver;
    std::shared_ptr<transport::TcpDriver> b_driver;
    transport::EndpointDescriptor endpoint;
    transport::ConnectionInfo listener;
    transport::ConnectionInfo a_connection;
    transport::ConnectionInfo b_connection;
    QueueEgress a_egress;
    RecordingIngress a_ingress;
    RecordingIngress b_ingress;
    std::unique_ptr<BridgePipeline> a;
    std::unique_ptr<BridgePipeline> b;
};

ConnectedPipelines MakePipelines(
    uint64_t retransmit_max_age_ns =
        RetransmitWindowOptions{}.max_age_ns,
    size_t retransmit_max_entries =
        RetransmitWindowOptions{}.max_entries,
    SchemaNegotiator* a_negotiator = nullptr,
    SchemaNegotiator* b_negotiator = nullptr,
    uint16_t lane_index = 0,
    uint16_t lane_count = 1,
    size_t max_control_frames = BridgePipelineOptions{}.max_control_frames,
    const BridgeTopicAuthorizer* b_authorizer = nullptr,
    NodeId b_authenticated_peer = {}) {
    ConnectedPipelines result;
    transport::TcpDriverOptions tcp_options;
    tcp_options.max_frame_body_bytes = 4096;
    tcp_options.max_total_send_buffer_bytes = 32 * 1024;
    tcp_options.max_connection_send_buffer_bytes = 16 * 1024;
    tcp_options.max_ready_receive_bytes = 32 * 1024;
    tcp_options.max_ready_receive_messages = 64;
    tcp_options.max_pending_accepts = 4;
    tcp_options.heartbeat_interval_ms = 50;
    tcp_options.idle_timeout_ms = 2000;
    tcp_options.partial_frame_timeout_ms = 1000;
    tcp_options.io_poll_max_ms = 5;
    auto a_created = transport::TcpDriver::Create(tcp_options);
    auto b_created = transport::TcpDriver::Create(tcp_options);
    EXPECT_TRUE(a_created.ok());
    EXPECT_TRUE(b_created.ok());
    if (!a_created.ok() || !b_created.ok()) return result;
    result.a_driver = std::shared_ptr<transport::TcpDriver>(
        std::move(*a_created));
    result.b_driver = std::shared_ptr<transport::TcpDriver>(
        std::move(*b_created));
    const transport::DriverConfig config{
        .max_connections = 8,
        .max_listeners = 2,
        .max_queued_sends = 64,
    };
    EXPECT_TRUE(result.a_driver->Start(config).ok());
    EXPECT_TRUE(result.b_driver->Start(config).ok());
    result.endpoint = Loopback(FreePort());
    auto listener = result.b_driver->Listen(
        {.local_endpoint = result.endpoint, .backlog = 2});
    EXPECT_TRUE(listener.ok());
    if (!listener.ok()) return result;
    result.listener = *listener;
    auto connected = result.a_driver->Connect({
        .remote_endpoint = result.endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    EXPECT_TRUE(connected.ok());
    if (!connected.ok()) return result;
    result.a_connection = *connected;
    auto accepted = result.b_driver->Accept(
        {.listener_id = listener->id, .timeout_ms = 1000});
    EXPECT_TRUE(accepted.ok());
    if (!accepted.ok()) return result;
    result.b_connection = *accepted;

    BridgePipelineOptions a_options;
    a_options.local_session_epoch = 101;
    a_options.remote_session_epoch = 202;
    a_options.wire_limits.max_payload_length = 4096;
    a_options.wire_limits.max_buffered_bytes = 8192;
    a_options.retransmit.max_age_ns = retransmit_max_age_ns;
    a_options.retransmit.max_entries = retransmit_max_entries;
    a_options.max_control_frames = max_control_frames;
    a_options.lane_index = lane_index;
    a_options.lane_count = lane_count;
    BridgePipelineOptions b_options = a_options;
    b_options.local_session_epoch = 202;
    b_options.remote_session_epoch = 101;
    b_options.topic_authorizer = b_authorizer;
    if (b_authenticated_peer.value != 0) {
        b_options.authenticated_peer = security::AuthenticatedPeer{
            .node_id = b_authenticated_peer,
            .security_domain = SecurityDomainId{1},
            .credential_generation = 1,
        };
    }
    auto a_pipeline = BridgePipeline::Create(
        a_options, result.a_driver, result.a_connection.id, &result.a_egress,
        &result.a_ingress, a_negotiator);
    auto b_pipeline = BridgePipeline::Create(
        b_options, result.b_driver, result.b_connection.id, nullptr,
        &result.b_ingress, b_negotiator);
    EXPECT_TRUE(a_pipeline.ok());
    EXPECT_TRUE(b_pipeline.ok());
    if (a_pipeline.ok()) result.a = std::move(*a_pipeline);
    if (b_pipeline.ok()) result.b = std::move(*b_pipeline);
    return result;
}

Status Reconnect(ConnectedPipelines* pair, uint64_t a_epoch,
                 uint64_t b_epoch, bool b_dedup_state_lost,
                 uint64_t now_ns) {
    MINO_RETURN_IF_ERROR(pair->a_driver->Close(pair->a_connection.id));
    MINO_RETURN_IF_ERROR(pair->b_driver->Close(pair->b_connection.id));
    auto connected = pair->a_driver->Connect({
        .remote_endpoint = pair->endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    if (!connected.ok()) return connected.status();
    auto accepted = pair->b_driver->Accept({
        .listener_id = pair->listener.id,
        .timeout_ms = 1000,
    });
    if (!accepted.ok()) return accepted.status();
    pair->a_connection = *connected;
    pair->b_connection = *accepted;
    MINO_RETURN_IF_ERROR(pair->a->RebindConnection(
        pair->a_connection.id, a_epoch, b_epoch, false, now_ns));
    return pair->b->RebindConnection(pair->b_connection.id, b_epoch, a_epoch,
                                     b_dedup_state_lost, now_ns);
}

Status SendFrame(const std::shared_ptr<transport::TcpDriver>& driver,
                 transport::ConnectionId connection_id,
                 const WireFrame& frame) {
    MINO_ASSIGN_OR_RETURN(auto encoded, WireFrameCodec::Encode(frame));
    auto sent = driver->SendUntracked(transport::UntrackedSendRequest{
        .connection_id = connection_id,
        .payload = encoded,
        .traffic_class = transport::UntrackedTrafficClass::kProtocolControl,
    });
    return sent.ok() ? Status::Ok() : sent.status();
}

Status PumpUntil(ConnectedPipelines* pair,
                 const std::function<bool()>& done,
                 size_t iterations = 1000,
                 uint64_t* retransmitted_frames = nullptr) {
    for (size_t i = 0; i < iterations; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = i * 1'000'000;
        auto a = pair->a->Pump(budget);
        if (!a.ok()) return a.status();
        auto b = pair->b->Pump(budget);
        if (!b.ok()) return b.status();
        if (retransmitted_frames != nullptr) {
            *retransmitted_frames += a->retransmitted_frames;
            *retransmitted_frames += b->retransmitted_frames;
        }
        if (done()) return Status::Ok();
        std::this_thread::sleep_for(1ms);
    }
    return Status::Error(StatusCode::kTimeout);
}

TEST(BridgePipelineTest,
     TopicAclDeniesBeforePendingRetentionDedupAndIngressAllocation) {
    RecordingTopicAuthorizer authorizer;
    authorizer.status = Status::Error(StatusCode::kPermissionDenied,
                                      "test Topic ACL denied");
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr, nullptr, 0, 1,
        BridgePipelineOptions{}.max_control_frames, &authorizer, NodeId{11});
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());

    ASSERT_TRUE(SendFrame(pair.a_driver, pair.a_connection.id,
                          DataFrame(1)).ok());
    Status denied = Status::Ok();
    for (size_t attempt = 0; attempt < 1000; ++attempt) {
        auto pumped = pair.b->Pump(BridgePumpBudget{.now_ns = attempt + 1});
        if (!pumped.ok()) {
            denied = pumped.status();
            break;
        }
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(denied.code(), StatusCode::kPermissionDenied);
    EXPECT_EQ(authorizer.calls, 1u);
    EXPECT_EQ(authorizer.last_source, NodeId{11});
    EXPECT_EQ(authorizer.last_topic, TopicId{1});
    EXPECT_TRUE(pair.b_ingress.frames.empty());
    EXPECT_EQ(pair.b->pending_inbound_frames(), 0u);
    EXPECT_EQ(pair.b->dedup_stats().accepted_checks, 0u);
}

TEST(BridgePipelineTest,
     AuthenticatedSourceMismatchIsRejectedBeforeTopicAclAndDedup) {
    RecordingTopicAuthorizer authorizer;
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr, nullptr, 0, 1,
        BridgePipelineOptions{}.max_control_frames, &authorizer, NodeId{11});
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());

    WireFrame forged = DataFrame(1);
    forged.header.source_node_id = 99;
    ASSERT_TRUE(SendFrame(pair.a_driver, pair.a_connection.id, forged).ok());
    Status denied = Status::Ok();
    for (size_t attempt = 0; attempt < 1000; ++attempt) {
        auto pumped = pair.b->Pump(BridgePumpBudget{.now_ns = attempt + 1});
        if (!pumped.ok()) {
            denied = pumped.status();
            break;
        }
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(denied.code(), StatusCode::kPermissionDenied);
    EXPECT_EQ(authorizer.calls, 0u);
    EXPECT_TRUE(pair.b_ingress.frames.empty());
    EXPECT_EQ(pair.b->dedup_stats().accepted_checks, 0u);
}

TEST(BridgePipelineTest, RejectsInvalidLaneOptions) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a_driver, nullptr);

    BridgePipelineOptions options;
    options.local_session_epoch = 301;
    options.remote_session_epoch = 302;
    options.wire_limits.max_payload_length = 4096;
    options.wire_limits.max_buffered_bytes = 8192;
    const auto create = [&](uint16_t lane_index, uint16_t lane_count) {
        BridgePipelineOptions candidate = options;
        candidate.lane_index = lane_index;
        candidate.lane_count = lane_count;
        return BridgePipeline::Create(
            candidate, pair.a_driver, pair.a_connection.id, nullptr,
            &pair.a_ingress);
    };

    EXPECT_EQ(create(0, 0).status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(create(0, kMaxBridgeLaneCount + 1).status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(create(2, 2).status().code(), StatusCode::kInvalidArgument);
}

TEST(BridgePipelineTest,
     FencesOutboundSourcesAndAcceptsMatchingLaneDataAndAck) {
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr, nullptr, 0, 2);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    const SourceIdentity matching = SourceForLane(0, 2);
    const SourceIdentity other = SourceForLane(1, 2);
    ASSERT_NE(matching.node_id, 0u);
    ASSERT_NE(other.node_id, 0u);

    WireFrame wrong_reliable = DataFrame(1);
    SetSource(&wrong_reliable, other);
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = wrong_reliable,
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    auto reliable_rejected = pair.a->Pump(BridgePumpBudget{.now_ns = 1});
    ASSERT_FALSE(reliable_rejected.ok());
    EXPECT_EQ(reliable_rejected.status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(pair.a->retransmit_entries(), 0u);
    EXPECT_EQ(pair.a_egress.frames.size(), 1u);
    pair.a_egress.CommitPolled();

    WireFrame wrong_best_effort = DataFrame(2);
    SetSource(&wrong_best_effort, other);
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = wrong_best_effort,
        .reliability = registry::Reliability::kBestEffort,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    auto best_effort_rejected = pair.a->Pump(BridgePumpBudget{.now_ns = 2});
    ASSERT_FALSE(best_effort_rejected.ok());
    EXPECT_EQ(best_effort_rejected.status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(pair.a_egress.frames.size(), 1u);
    pair.a_egress.CommitPolled();

    WireFrame accepted = DataFrame(3);
    SetSource(&accepted, matching);
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = accepted,
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    const Status delivered = PumpUntil(&pair, [&] {
        return pair.a_egress.frames.empty() &&
               pair.b_ingress.frames.size() == 1 &&
               pair.a->retransmit_entries() == 0;
    });
    ASSERT_TRUE(delivered.ok()) << delivered.ToString();
    const SourceIdentity received{
        pair.b_ingress.frames[0].header.source_node_id,
        pair.b_ingress.frames[0].header.source_publisher_id,
        pair.b_ingress.frames[0].header.source_publisher_epoch,
    };
    EXPECT_EQ(received, matching);
}

TEST(BridgePipelineTest, RejectsInboundDataFromAnotherLaneBeforePublish) {
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr, nullptr, 0, 2);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    WireFrame wrong = DataFrame(1);
    SetSource(&wrong, SourceForLane(1, 2));
    ASSERT_TRUE(SendFrame(pair.a_driver, pair.a_connection.id, wrong).ok());

    Status rejected = Status::Ok();
    for (size_t i = 0; i < 200; ++i) {
        auto pumped = pair.b->Pump(BridgePumpBudget{
            .now_ns = 1'000'000'000ull + i * 1'000'000,
        });
        if (!pumped.ok()) {
            rejected = pumped.status();
            break;
        }
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(rejected.code(), StatusCode::kCorruption);
    EXPECT_TRUE(pair.b_ingress.frames.empty());
}

TEST(BridgePipelineTest, RejectsAckSourceFromAnotherLaneBeforeRetirement) {
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr, nullptr, 0, 2);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    WireFrame data = DataFrame(7);
    SetSource(&data, SourceForLane(0, 2));
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = data,
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    ASSERT_TRUE(pair.a->Pump(BridgePumpBudget{.now_ns = 1}).ok());
    ASSERT_EQ(pair.a->retransmit_entries(), 1u);

    auto ack_payload = ControlPayloadCodec::EncodeAck(AckPayload{
        .sender_session_epoch = 202,
        .receiver_session_epoch = 101,
        .source = SourceForLane(1, 2),
        .observed_sequence = 7,
        .highest_contiguous_sequence = 7,
        .disposition = AckDisposition::kAccepted,
    });
    ASSERT_TRUE(ack_payload.ok()) << ack_payload.status().ToString();
    WireFrame ack;
    ack.header.frame_type = FrameType::kAck;
    ack.header.flags = FlagValue(FrameFlag::kControlFrame) |
                       FlagValue(FrameFlag::kPayloadCrcPresent);
    ack.payload = std::move(*ack_payload);
    ASSERT_TRUE(SendFrame(pair.b_driver, pair.b_connection.id, ack).ok());

    Status rejected = Status::Ok();
    for (size_t i = 0; i < 200; ++i) {
        auto pumped = pair.a->Pump(BridgePumpBudget{
            .now_ns = 2'000'000'000ull + i * 1'000'000,
        });
        if (!pumped.ok()) {
            rejected = pumped.status();
            break;
        }
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(rejected.code(), StatusCode::kCorruption);
    EXPECT_EQ(pair.a->retransmit_entries(), 1u);
}

TEST(BridgePipelineTest, FencesEveryHelloSourceByLane) {
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr, nullptr, 0, 2);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    const auto send_hello = [&](const SourceIdentity& source) -> Status {
        MINO_ASSIGN_OR_RETURN(
            auto payload,
            ControlPayloadCodec::EncodeSessionHello(SessionHello{
                .sender_session_epoch = 101,
                .receiver_session_epoch = 202,
                .dedup_state_lost = false,
                .sources = {{
                    .source = source,
                    .last_accepted_sequence = 1,
                }},
            }));
        WireFrame hello;
        hello.header.frame_type = FrameType::kSessionHello;
        hello.header.flags = FlagValue(FrameFlag::kControlFrame) |
                             FlagValue(FrameFlag::kPayloadCrcPresent);
        hello.payload = std::move(payload);
        return SendFrame(pair.a_driver, pair.a_connection.id, hello);
    };

    ASSERT_TRUE(send_hello(SourceForLane(0, 2)).ok());
    for (size_t i = 0; i < 100; ++i) {
        auto pumped = pair.b->Pump(BridgePumpBudget{
            .now_ns = 3'000'000'000ull + i * 1'000'000,
        });
        ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_TRUE(pair.b->session_ready());

    ASSERT_TRUE(send_hello(SourceForLane(1, 2)).ok());
    Status rejected = Status::Ok();
    for (size_t i = 0; i < 200; ++i) {
        auto pumped = pair.b->Pump(BridgePumpBudget{
            .now_ns = 4'000'000'000ull + i * 1'000'000,
        });
        if (!pumped.ok()) {
            rejected = pumped.status();
            break;
        }
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(rejected.code(), StatusCode::kCorruption);
}

TEST(BridgePipelineTest, ReliableTcpPublishesThenAcknowledgesAndRetiresWindow) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(1),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });

    uint64_t retransmitted_frames = 0;
    const Status pumped = PumpUntil(
        &pair,
        [&] {
            return pair.b_ingress.frames.size() == 1 &&
                   pair.a->retransmit_entries() == 0;
        },
        1000, &retransmitted_frames);
    ASSERT_TRUE(pumped.ok()) << pumped.ToString();
    EXPECT_TRUE(pair.a->session_ready());
    EXPECT_TRUE(pair.b->session_ready());
    ASSERT_EQ(pair.b_ingress.frames.size(), 1u);
    EXPECT_EQ(pair.b_ingress.frames[0].payload,
              std::vector<std::byte>(32, std::byte{0x42}));
    EXPECT_EQ(retransmitted_frames, 0u);
}

TEST(BridgePipelineTest,
     ContinuousAcceptedAcksCoalesceAtControlCapacityAndRetireCumulatively) {
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr, nullptr, 0, 1, 1);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    for (uint64_t sequence = 1; sequence <= 3; ++sequence) {
        pair.a_egress.frames.push_back(EncodedOutboundFrame{
            .frame = DataFrame(sequence),
            .reliability = registry::Reliability::kReliableOrdered,
            .allow_drop = false,
            .schema_identity = std::nullopt,
            .descriptor_artifact = {},
        });
    }

    ASSERT_TRUE(pair.a->Pump(BridgePumpBudget{.now_ns = 1'000'000'000ull})
                    .ok());
    ASSERT_TRUE(pair.a_egress.frames.empty());
    ASSERT_EQ(pair.a->retransmit_entries(), 3u);
    std::this_thread::sleep_for(20ms);

    BridgePumpBudget receive;
    receive.max_outbound_frames = 1;
    receive.now_ns = 1'100'000'000ull;
    auto received = pair.b->Pump(receive);
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(pair.b_ingress.frames.size(), 3u);
    EXPECT_EQ(received->outbound_frames, 1u);

    for (size_t i = 0; i < 500 && pair.a->retransmit_entries() != 0; ++i) {
        auto pumped = pair.a->Pump(BridgePumpBudget{
            .now_ns = 1'200'000'000ull + i * 1'000'000,
        });
        ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(pair.a->retransmit_entries(), 0u);
}

TEST(BridgePipelineTest, OutOfOrderAcceptedAcksCoalesceOnlyAfterGapCloses) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    for (uint64_t sequence : {2u, 1u}) {
        pair.a_egress.frames.push_back(EncodedOutboundFrame{
            .frame = DataFrame(sequence),
            .reliability = registry::Reliability::kReliableOrdered,
            .allow_drop = false,
            .schema_identity = std::nullopt,
            .descriptor_artifact = {},
        });
    }

    ASSERT_TRUE(pair.a->Pump(BridgePumpBudget{.now_ns = 2'000'000'000ull})
                    .ok());
    ASSERT_EQ(pair.a->retransmit_entries(), 2u);
    std::this_thread::sleep_for(20ms);
    BridgePumpBudget receive;
    receive.max_outbound_frames = 1;
    receive.now_ns = 2'100'000'000ull;
    auto received = pair.b->Pump(receive);
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(pair.b_ingress.frames.size(), 2u);
    EXPECT_EQ(received->outbound_frames, 1u);

    for (size_t i = 0; i < 500 && pair.a->retransmit_entries() != 0; ++i) {
        auto pumped = pair.a->Pump(BridgePumpBudget{
            .now_ns = 2'200'000'000ull + i * 1'000'000,
        });
        ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(pair.a->retransmit_entries(), 0u);
}

TEST(BridgePipelineTest, NackRemainsIndependentFromPendingCumulativeAck) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    for (uint64_t sequence : {1u, 5000u}) {
        pair.a_egress.frames.push_back(EncodedOutboundFrame{
            .frame = DataFrame(sequence),
            .reliability = registry::Reliability::kReliableOrdered,
            .allow_drop = false,
            .schema_identity = std::nullopt,
            .descriptor_artifact = {},
        });
    }

    ASSERT_TRUE(pair.a->Pump(BridgePumpBudget{.now_ns = 3'000'000'000ull})
                    .ok());
    ASSERT_EQ(pair.a->retransmit_entries(), 2u);
    std::this_thread::sleep_for(20ms);
    BridgePumpBudget receive;
    receive.max_outbound_frames = 2;
    receive.now_ns = 3'100'000'000ull;
    auto received = pair.b->Pump(receive);
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(pair.b_ingress.frames.size(), 1u);
    EXPECT_EQ(received->outbound_frames, 2u);

    for (size_t i = 0; i < 500 && pair.a->retransmit_entries() == 2; ++i) {
        auto pumped = pair.a->Pump(BridgePumpBudget{
            .now_ns = 3'200'000'000ull + i * 1'000'000,
        });
        ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(pair.a->retransmit_entries(), 1u);
}

TEST(BridgePipelineTest, ReconnectHelloCumulativelyRetiresCoalescedBatch) {
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr, nullptr, 0, 1, 1);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    for (uint64_t sequence = 1; sequence <= 3; ++sequence) {
        pair.a_egress.frames.push_back(EncodedOutboundFrame{
            .frame = DataFrame(sequence),
            .reliability = registry::Reliability::kReliableOrdered,
            .allow_drop = false,
            .schema_identity = std::nullopt,
            .descriptor_artifact = {},
        });
    }
    ASSERT_TRUE(pair.a->Pump(BridgePumpBudget{.now_ns = 4'000'000'000ull})
                    .ok());
    ASSERT_EQ(pair.a->retransmit_entries(), 3u);
    std::this_thread::sleep_for(20ms);
    auto received = pair.b->Pump(BridgePumpBudget{
        .max_outbound_frames = 1,
        .now_ns = 4'100'000'000ull,
    });
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(pair.b_ingress.frames.size(), 3u);
    ASSERT_EQ(pair.a->retransmit_entries(), 3u);

    ASSERT_TRUE(Reconnect(&pair, 303, 404, false, 4'200'000'000ull).ok());
    const Status resumed = PumpUntil(&pair, [&] {
        return pair.a->session_ready() && pair.b->session_ready() &&
               pair.a->retransmit_entries() == 0;
    });
    ASSERT_TRUE(resumed.ok()) << resumed.ToString();
    EXPECT_EQ(pair.b_ingress.frames.size(), 3u);
}

TEST(BridgePipelineTest, DuplicateFrameIsAckedWithoutDuplicatePublication) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    const WireFrame frame = DataFrame(7);
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = frame,
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.b_ingress.frames.size() == 1 &&
                           pair.a->retransmit_entries() == 0;
                }).ok());

    auto body = WireFrameCodec::Encode(frame);
    ASSERT_TRUE(body.ok());
    ASSERT_TRUE(pair.a_driver
                    ->SendUntracked(transport::UntrackedSendRequest{
                        .connection_id = pair.a_connection.id,
                        .payload = *body,
                    })
                    .ok());
    for (size_t i = 0; i < 100; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 2'000'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.a->Pump(budget).ok());
        ASSERT_TRUE(pair.b->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(pair.b_ingress.frames.size(), 1u);
}

TEST(BridgePipelineTest, ReconnectUsesPeerHwmWithoutDuplicatePublication) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(10),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });

    for (size_t i = 0; i < 200 && pair.a->retransmit_entries() == 0; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'000'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.a->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(pair.a->retransmit_entries(), 1u);
    for (size_t i = 0; i < 200 && pair.b_ingress.frames.empty(); ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'500'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.b->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(pair.b_ingress.frames.size(), 1u);

    ASSERT_TRUE(Reconnect(&pair, 303, 404, false, 2'000'000'000ull).ok());
    const Status resumed = PumpUntil(&pair, [&] {
        return pair.a->session_ready() && pair.b->session_ready() &&
               pair.a->retransmit_entries() == 0;
    });
    ASSERT_TRUE(resumed.ok()) << resumed.ToString();
    EXPECT_FALSE(pair.a->peer_dedup_state_lost());
    EXPECT_FALSE(pair.a->reliability_degraded());
    EXPECT_EQ(pair.b_ingress.frames.size(), 1u);
}

TEST(BridgePipelineTest, ReconnectRebindsPendingFrameToNewSchemaRef) {
    auto current_compiled = schema::SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package bridge_pipeline_reconnect;
message Current { uint32 value = 1; }
)idl");
    auto other_compiled = schema::SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package bridge_pipeline_reconnect;
message Other { uint32 value = 1; }
)idl");
    ASSERT_TRUE(current_compiled.ok());
    ASSERT_TRUE(other_compiled.ok());
    schema::SchemaRegistry a_registry;
    schema::SchemaRegistry b_registry;
    auto a_current = a_registry.RegisterCompiled(*current_compiled);
    auto b_current = b_registry.RegisterCompiled(*current_compiled);
    auto a_other = a_registry.RegisterCompiled(*other_compiled);
    auto b_other = b_registry.RegisterCompiled(*other_compiled);
    ASSERT_TRUE(a_current.ok());
    ASSERT_TRUE(b_current.ok());
    ASSERT_TRUE(a_other.ok());
    ASSERT_TRUE(b_other.ok());
    ASSERT_EQ(a_current->size(), 1u);
    ASSERT_EQ(a_other->size(), 1u);
    const schema::SchemaIdentity current = (*a_current)[0]->identity();
    const schema::SchemaIdentity other = (*a_other)[0]->identity();
    SchemaNegotiator a_negotiator(&a_registry, nullptr, nullptr);
    SchemaNegotiator b_negotiator(&b_registry, nullptr, nullptr);
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, &a_negotiator,
        &b_negotiator);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());

    WireFrame frame = DataFrame(1);
    frame.header.connection_schema_ref = 0;
    frame.header.msg_type = static_cast<uint32_t>(current.short_id());
    frame.header.schema_version = current.schema_version();
    frame.header.layout_version = current.layout_version();
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = std::move(frame),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = current,
        .descriptor_artifact = {},
    });
    for (size_t i = 0;
         i < 500 &&
         (pair.a->retransmit_entries() == 0 ||
          !pair.a_egress.frames.empty());
         ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'000'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.a->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(pair.a->retransmit_entries(), 1u);
    ASSERT_TRUE(pair.a_egress.frames.empty());

    ASSERT_TRUE(Reconnect(&pair, 707, 808, false, 2'000'000'000ull).ok());
    auto occupied = a_negotiator.BindLocalSchema(other);
    ASSERT_TRUE(occupied.ok()) << occupied.status().ToString();
    auto occupied_announcement =
        SchemaControlCodec::DecodeAnnouncement(occupied->payload);
    ASSERT_TRUE(occupied_announcement.ok());
    EXPECT_EQ(occupied_announcement->connection_schema_ref, 1u);

    uint64_t retransmitted_frames = 0;
    const Status resumed = PumpUntil(
        &pair,
        [&] {
            return pair.b_ingress.frames.size() == 1 &&
                   pair.a->retransmit_entries() == 0;
        },
        1000, &retransmitted_frames);
    ASSERT_TRUE(resumed.ok())
        << resumed.ToString()
        << " a_ready=" << pair.a->session_ready()
        << " b_ready=" << pair.b->session_ready()
        << " retransmit=" << pair.a->retransmit_entries()
        << " b_pending=" << pair.b->pending_inbound_frames()
        << " b_published=" << pair.b_ingress.frames.size()
        << " a_local_ref=" << a_negotiator.local_ref_high_watermark()
        << " b_remote_ref=" << b_negotiator.remote_ref_high_watermark();
    ASSERT_EQ(pair.b_ingress.frames.size(), 1u);
    EXPECT_EQ(pair.b_ingress.frames[0].header.connection_schema_ref, 2u);
    EXPECT_EQ(retransmitted_frames, 1u);
}

TEST(BridgePipelineTest, SchemaAnnouncementTransactionallyReleasesBufferedBatch) {
    auto compiled = schema::SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package bridge_pipeline_batch;
message Payload { uint32 value = 1; }
)idl");
    ASSERT_TRUE(compiled.ok());
    schema::SchemaRegistry sender_registry;
    schema::SchemaRegistry receiver_registry;
    auto sender_registered = sender_registry.RegisterCompiled(*compiled);
    auto receiver_registered = receiver_registry.RegisterCompiled(*compiled);
    ASSERT_TRUE(sender_registered.ok());
    ASSERT_TRUE(receiver_registered.ok());
    ASSERT_EQ(sender_registered->size(), 1u);
    const schema::SchemaIdentity identity =
        (*sender_registered)[0]->identity();
    SchemaNegotiator sender_negotiator(&sender_registry, nullptr, nullptr);
    SchemaNegotiator receiver_negotiator(&receiver_registry, nullptr, nullptr);
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns,
        RetransmitWindowOptions{}.max_entries, nullptr,
        &receiver_negotiator);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());

    auto announcement = sender_negotiator.BindLocalSchema(identity);
    ASSERT_TRUE(announcement.ok()) << announcement.status().ToString();
    auto decoded_announcement =
        SchemaControlCodec::DecodeAnnouncement(announcement->payload);
    ASSERT_TRUE(decoded_announcement.ok());
    for (uint64_t sequence = 1; sequence <= 2; ++sequence) {
        WireFrame frame = DataFrame(sequence);
        frame.header.connection_schema_ref =
            decoded_announcement->connection_schema_ref;
        frame.header.msg_type = static_cast<uint32_t>(identity.short_id());
        frame.header.schema_version = identity.schema_version();
        frame.header.layout_version = identity.layout_version();
        auto body = WireFrameCodec::Encode(frame);
        ASSERT_TRUE(body.ok());
        ASSERT_TRUE(pair.a_driver
                        ->SendUntracked(transport::UntrackedSendRequest{
                            .connection_id = pair.a_connection.id,
                            .payload = *body,
                            .traffic_class = transport::
                                UntrackedTrafficClass::kProtocolControl,
                        })
                        .ok());
    }
    auto announcement_body = WireFrameCodec::Encode(*announcement);
    ASSERT_TRUE(announcement_body.ok());
    ASSERT_TRUE(pair.a_driver
                    ->SendUntracked(transport::UntrackedSendRequest{
                        .connection_id = pair.a_connection.id,
                        .payload = *announcement_body,
                        .traffic_class = transport::
                            UntrackedTrafficClass::kProtocolControl,
                    })
                    .ok());

    for (size_t i = 0;
         i < 1000 && pair.b_ingress.frames.size() != 2; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'000'000'000ull + i * 1'000'000;
        auto pumped = pair.b->Pump(budget);
        ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(pair.b_ingress.frames.size(), 2u);
    EXPECT_EQ(pair.b->pending_inbound_frames(), 0u);
    EXPECT_EQ(receiver_negotiator.buffered_frames(), 0u);
}

TEST(BridgePipelineTest, ReceiverRestartSignalsDegradedAndReplaysPendingData) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(11),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });

    for (size_t i = 0; i < 200 && pair.a->retransmit_entries() == 0; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'000'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.a->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(pair.a->retransmit_entries(), 1u);
    for (size_t i = 0; i < 200 && pair.b_ingress.frames.empty(); ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'500'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.b->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(pair.b_ingress.frames.size(), 1u);

    ASSERT_TRUE(Reconnect(&pair, 505, 606, true, 2'000'000'000ull).ok());
    const Status resumed = PumpUntil(&pair, [&] {
        return pair.a->peer_dedup_state_lost() &&
               pair.b_ingress.frames.size() == 2 &&
               pair.a->retransmit_entries() == 0;
    });
    ASSERT_TRUE(resumed.ok()) << resumed.ToString();
    EXPECT_TRUE(pair.a->session_ready());
    EXPECT_TRUE(pair.a->reliability_degraded());
    EXPECT_EQ(pair.a->reliability_status().code(), StatusCode::kDegraded);
}

TEST(BridgePipelineTest, ReceiverRestartAcceptsHighSequenceAsDegradedBaseline) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(5000),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    for (size_t i = 0; i < 200 && pair.a->retransmit_entries() == 0; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'000'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.a->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(pair.a->retransmit_entries(), 1u);

    ASSERT_TRUE(Reconnect(&pair, 909, 1001, true, 2'000'000'000ull).ok());
    const Status recovered = PumpUntil(&pair, [&] {
        return pair.a->peer_dedup_state_lost() &&
               pair.b_ingress.frames.size() == 1 &&
               pair.a->retransmit_entries() == 0;
    });
    ASSERT_TRUE(recovered.ok()) << recovered.ToString();
    EXPECT_EQ(pair.b_ingress.frames[0].header.sequence_num, 5000u);
    EXPECT_EQ(pair.a->reliability_status().code(), StatusCode::kDegraded);
}

TEST(BridgePipelineTest, FailedTransportCompletionIsObservableAndRetained) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(1),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    for (size_t i = 0; i < 200 && pair.a->retransmit_entries() == 0; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'000'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.a->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(pair.a->retransmit_entries(), 1u);
    ASSERT_TRUE(pair.a_driver->Close(pair.a_connection.id).ok());

    Status failure = Status::Ok();
    for (size_t i = 0; i < 500 && failure.ok(); ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'500'000'000ull + i * 1'000'000;
        auto pumped = pair.a->Pump(budget);
        if (!pumped.ok()) failure = pumped.status();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_FALSE(failure.ok());
    EXPECT_EQ(pair.a->retransmit_entries(), 1u);
    EXPECT_FALSE(pair.a->session_ready());
}

TEST(BridgePipelineTest, OutOfWindowNackDoesNotRetireRejectedFrame) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(5000),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a_egress.frames.empty() &&
                           pair.a->retransmit_entries() == 1 &&
                           pair.b->dedup_stats().nack_checks != 0;
                }).ok());
    for (size_t i = 0; i < 100; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 1'000'000'000ull + i * 1'000'000;
        ASSERT_TRUE(pair.a->Pump(budget).ok());
        ASSERT_TRUE(pair.b->Pump(budget).ok());
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(pair.a->retransmit_entries(), 1u);
    EXPECT_TRUE(pair.b_ingress.frames.empty());
}

TEST(BridgePipelineTest, FullRetransmitWindowDoesNotConsumeNextEgressFrame) {
    ConnectedPipelines pair = MakePipelines(
        RetransmitWindowOptions{}.max_age_ns, 1);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    for (uint64_t sequence = 1; sequence <= 2; ++sequence) {
        pair.a_egress.frames.push_back(EncodedOutboundFrame{
            .frame = DataFrame(sequence),
            .reliability = registry::Reliability::kReliableOrdered,
            .allow_drop = false,
            .schema_identity = std::nullopt,
            .descriptor_artifact = {},
        });
    }

    BridgePumpBudget one_pump;
    one_pump.now_ns = 1'000'000'000ull;
    ASSERT_TRUE(pair.a->Pump(one_pump).ok());
    EXPECT_EQ(pair.a->retransmit_entries(), 1u);
    EXPECT_EQ(pair.a_egress.frames.size(), 1u);

    const Status drained = PumpUntil(&pair, [&] {
        return pair.b_ingress.frames.size() == 2 &&
               pair.a->retransmit_entries() == 0 &&
               pair.a_egress.frames.empty();
    });
    ASSERT_TRUE(drained.ok()) << drained.ToString();
}

TEST(BridgePipelineTest, PumpByteBudgetDoesNotConsumeUnsentBestEffortFrame) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(1),
        .reliability = registry::Reliability::kBestEffort,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });

    BridgePumpBudget tiny;
    tiny.max_bytes = 1;
    tiny.now_ns = 1'000'000'000ull;
    ASSERT_TRUE(pair.a->Pump(tiny).ok());
    EXPECT_EQ(pair.a_egress.frames.size(), 1u);

    const Status sent = PumpUntil(&pair, [&] {
        return pair.a_egress.frames.empty() &&
               pair.b_ingress.frames.size() == 1;
    });
    ASSERT_TRUE(sent.ok()) << sent.ToString();
}

TEST(BridgePipelineTest, RetransmitExpiryClosesSessionAndReportsTimeout) {
    ConnectedPipelines pair = MakePipelines(10);
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(13),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    BridgePumpBudget send_budget;
    send_budget.now_ns = 1'000'000'000ull;
    ASSERT_TRUE(pair.a->Pump(send_budget).ok());
    ASSERT_EQ(pair.a->retransmit_entries(), 1u);

    BridgePumpBudget expire_budget;
    expire_budget.now_ns = send_budget.now_ns + 11;
    auto expired = pair.a->Pump(expire_budget);
    ASSERT_FALSE(expired.ok());
    EXPECT_EQ(expired.status().code(), StatusCode::kTimeout);
    EXPECT_EQ(pair.a->retransmit_entries(), 0u);
    EXPECT_EQ(pair.a->retransmit_stats().expired_entries, 1u);
    EXPECT_FALSE(pair.a->session_ready());
}

TEST(BridgePipelineTest, CorruptWireFrameFailsWithoutPublication) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    auto encoded = WireFrameCodec::Encode(DataFrame(12));
    ASSERT_TRUE(encoded.ok());
    encoded->back() ^= std::byte{0x01};
    ASSERT_TRUE(pair.a_driver
                    ->SendUntracked(transport::UntrackedSendRequest{
                        .connection_id = pair.a_connection.id,
                        .payload = *encoded,
                    })
                    .ok());

    Status failure = Status::Ok();
    for (size_t i = 0; i < 500 && failure.ok(); ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 2'000'000'000ull + i * 1'000'000;
        auto a_pumped = pair.a->Pump(budget);
        ASSERT_TRUE(a_pumped.ok()) << a_pumped.status().ToString();
        auto b_pumped = pair.b->Pump(budget);
        if (!b_pumped.ok()) failure = b_pumped.status();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(failure.code(), StatusCode::kCorruption);
    EXPECT_TRUE(pair.b_ingress.frames.empty());
}

TEST(BridgePipelineTest, SessionDiscoveryIsConnectionOwnerOnlyAndFailsClosed) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.a->session_ready() && pair.b->session_ready();
                }).ok());
    auto payload = ControlPayloadCodec::EncodeSessionDiscovery(
        SessionDiscovery{
            .session_epoch = 999,
            .node_id = NodeId{101},
            .process_identity = ProcessIdentity{
                .node_id = 101,
                .process_id = 102,
                .process_epoch = 103,
                .start_time_ns = 104,
            },
            .lease_epoch = 105,
            .node_config_version = 106,
        });
    ASSERT_TRUE(payload.ok()) << payload.status().ToString();
    WireFrame discovery;
    discovery.header.frame_type = FrameType::kSessionDiscovery;
    discovery.header.flags = FlagValue(FrameFlag::kControlFrame) |
                             FlagValue(FrameFlag::kPayloadCrcPresent);
    discovery.payload = std::move(*payload);
    auto encoded = WireFrameCodec::Encode(discovery);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    ASSERT_TRUE(pair.a_driver
                    ->SendUntracked(transport::UntrackedSendRequest{
                        .connection_id = pair.a_connection.id,
                        .payload = *encoded,
                    })
                    .ok());

    Status rejected = Status::Ok();
    for (size_t i = 0; i < 200; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 10'000'000'000ull + i * 1'000'000;
        auto pumped = pair.b->Pump(budget);
        if (!pumped.ok()) {
            rejected = pumped.status();
            break;
        }
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(rejected.code(), StatusCode::kCorruption);
}

TEST(BridgePipelineTest, TransientRemoteBackpressureRetainsAndRetriesFrame) {
    ConnectedPipelines pair = MakePipelines();
    ASSERT_NE(pair.a, nullptr);
    ASSERT_NE(pair.b, nullptr);
    pair.b_ingress.fail = true;
    pair.a_egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(9),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });

    ASSERT_TRUE(PumpUntil(&pair, [&] {
                    return pair.b->pending_inbound_frames() == 1;
                }).ok());
    EXPECT_EQ(pair.a->retransmit_entries(), 1u);
    EXPECT_TRUE(pair.b_ingress.frames.empty());

    pair.b_ingress.fail = false;
    const Status recovered = PumpUntil(&pair, [&] {
        return pair.b_ingress.frames.size() == 1 &&
               pair.b->pending_inbound_frames() == 0 &&
               pair.a->retransmit_entries() == 0;
    });
    ASSERT_TRUE(recovered.ok()) << recovered.ToString();
    EXPECT_EQ(pair.b_ingress.frames.size(), 1u);
}

}  // namespace
}  // namespace mino::bridge
