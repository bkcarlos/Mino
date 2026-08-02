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
    SchemaNegotiator* b_negotiator = nullptr) {
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
    BridgePipelineOptions b_options = a_options;
    b_options.local_session_epoch = 202;
    b_options.remote_session_epoch = 101;
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

Status PumpUntil(ConnectedPipelines* pair,
                 const std::function<bool()>& done,
                 size_t iterations = 1000) {
    for (size_t i = 0; i < iterations; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = i * 1'000'000;
        auto a = pair->a->Pump(budget);
        if (!a.ok()) return a.status();
        auto b = pair->b->Pump(budget);
        if (!b.ok()) return b.status();
        if (done()) return Status::Ok();
        std::this_thread::sleep_for(1ms);
    }
    return Status::Error(StatusCode::kTimeout);
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

    const Status pumped = PumpUntil(
        &pair, [&] {
            return pair.b_ingress.frames.size() == 1 &&
                   pair.a->retransmit_entries() == 0;
        });
    ASSERT_TRUE(pumped.ok()) << pumped.ToString();
    EXPECT_TRUE(pair.a->session_ready());
    EXPECT_TRUE(pair.b->session_ready());
    ASSERT_EQ(pair.b_ingress.frames.size(), 1u);
    EXPECT_EQ(pair.b_ingress.frames[0].payload,
              std::vector<std::byte>(32, std::byte{0x42}));
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

    const Status resumed = PumpUntil(&pair, [&] {
        return pair.b_ingress.frames.size() == 1 &&
               pair.a->retransmit_entries() == 0;
    });
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

TEST(BridgePipelineTest, CorruptWireFrameClosesTransportWithoutPublication) {
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

    for (size_t i = 0;
         i < 500 && pair.b_driver->stats().active_connections != 0; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = 2'000'000'000ull + i * 1'000'000;
        auto a_pumped = pair.a->Pump(budget);
        ASSERT_TRUE(a_pumped.ok()) << a_pumped.status().ToString();
        auto b_pumped = pair.b->Pump(budget);
        ASSERT_TRUE(b_pumped.ok()) << b_pumped.status().ToString();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_EQ(pair.b_driver->stats().active_connections, 0u);
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
