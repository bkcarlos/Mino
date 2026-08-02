// Copyright 2026 The Mino Authors

#include "mino/transport/transport_driver.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <new>
#include <span>
#include <thread>
#include <utility>
#include <vector>

namespace mino::transport {
namespace {

EndpointDescriptor MakeIpv4(uint8_t last_octet, uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0},
        static_cast<std::byte>(last_octet)};
    auto endpoint = EndpointDescriptor::Ipv4Tcp(address, port);
    if (!endpoint.ok()) {
        ADD_FAILURE() << endpoint.status().ToString();
        return {};
    }
    return *endpoint;
}

std::vector<std::byte> Serialize(const EndpointDescriptor& endpoint) {
    std::array<std::byte, EndpointDescriptor::kMaxSerializedSize> storage{};
    auto encoded = SerializeEndpointDescriptor(endpoint, storage);
    if (!encoded.ok()) {
        ADD_FAILURE() << encoded.status().ToString();
        return {};
    }
    return {storage.begin(), storage.begin() + static_cast<ptrdiff_t>(*encoded)};
}

TEST(CapabilitiesTest, TypedOperationsMaskAndValidateBits) {
    constexpr Capabilities io =
        Capability::kConnect | Capability::kListen;
    static_assert(io.Has(Capability::kConnect));
    static_assert(io.Has(Capability::kListen));
    static_assert(!io.Has(Capability::kRemoteWrite));

    const Capabilities with_window = io | Capability::kZeroCopyWindow;
    EXPECT_TRUE(with_window.ContainsAll(io));
    EXPECT_TRUE(with_window.Has(Capability::kZeroCopyWindow));
    EXPECT_FALSE(with_window.Without(Capability::kListen)
                     .Has(Capability::kListen));
    EXPECT_EQ((~Capabilities{}).bits(), Capabilities::kKnownBits);
    EXPECT_EQ((with_window & io), io);

    auto known = Capabilities::FromBits(io.bits());
    ASSERT_TRUE(known.ok());
    EXPECT_EQ(*known, io);

    auto unknown = Capabilities::FromBits(1u << 31);
    ASSERT_FALSE(unknown.ok());
    EXPECT_EQ(unknown.status().code(), StatusCode::kInvalidArgument);
}

TEST(CapabilitiesTest, RejectsInvalidSemanticCombinationsAndLimits) {
    TransportCapabilities capabilities{
        .kind = TransportKind::kNetwork,
        .reliability = TransportReliability::kReliable,
        .max_frame_size = 4096,
        .max_reassembly_bytes = 8192,
        .features = Capability::kConnect | Capability::kListen,
    };
    EXPECT_TRUE(ValidateTransportCapabilities(capabilities).ok());

    capabilities.features = Capability::kRemoteWrite;
    EXPECT_EQ(ValidateTransportCapabilities(capabilities).code(),
              StatusCode::kInvalidArgument);
    capabilities.features = {};
    capabilities.max_reassembly_bytes = kMaxPayloadBytes + 1;
    EXPECT_EQ(ValidateTransportCapabilities(capabilities).code(),
              StatusCode::kResourceExhausted);

    capabilities.max_reassembly_bytes = 4096;
    capabilities.reliability = TransportReliability::kUnreliable;
    capabilities.features = Capability::kRemoteAcceptedConfirmation;
    EXPECT_EQ(ValidateTransportCapabilities(capabilities).code(),
              StatusCode::kInvalidArgument);
}

TEST(TransportValidationTest, RejectsResultsOutsideConnectionFilter) {
    const ReceiveRequest receive_request{
        .max_messages = 1,
        .max_bytes = 16,
        .timeout_ms = 0,
        .connection_id = 7,
    };
    ReceiveResult receive_result;
    receive_result.messages.push_back(ReceivedMessage{
        .connection_id = 8,
        .from = MakeIpv4(2, 3001),
        .payload = {std::byte{1}},
    });
    EXPECT_EQ(ValidateReceiveResult(receive_request, receive_result).code(),
              StatusCode::kInvalidArgument);
    receive_result.messages[0].connection_id = 7;
    EXPECT_TRUE(ValidateReceiveResult(receive_request, receive_result).ok());

    const CompletionPollRequest completion_request{
        .max_completions = 1,
        .timeout_ms = 0,
        .connection_id = 7,
    };
    CompletionPollResult completion_result;
    completion_result.completions.push_back(DeliveryCompletion{
        .operation = {.id = 1, .connection_id = 8},
        .reached_stage = DeliveryStage::kRemoteAccepted,
    });
    EXPECT_EQ(ValidateCompletionPollResult(completion_request,
                                           completion_result).code(),
              StatusCode::kInvalidArgument);
    completion_result.completions[0].operation.connection_id = 7;
    EXPECT_TRUE(ValidateCompletionPollResult(completion_request,
                                              completion_result).ok());
}

TEST(TransportValidationTest, RejectsUnknownUntrackedTrafficClass) {
    const std::array<std::byte, 1> payload = {std::byte{1}};
    const TransportCapabilities capabilities{
        .kind = TransportKind::kNetwork,
        .reliability = TransportReliability::kReliable,
        .max_frame_size = 1024,
        .max_reassembly_bytes = 4096,
        .features = Capability::kConnect,
    };
    const UntrackedSendRequest request{
        .connection_id = 1,
        .payload = payload,
        .traffic_class = static_cast<UntrackedTrafficClass>(255),
    };
    EXPECT_EQ(ValidateUntrackedSendRequest(request, capabilities).code(),
              StatusCode::kInvalidArgument);
}

TEST(EndpointDescriptorTest, Ipv4Ipv6AndFabricMatchFixedGolden) {
    const EndpointDescriptor ipv4 = MakeIpv4(1, 4242);

    std::array<std::byte, 16> ipv6_address{};
    ipv6_address[0] = std::byte{0x20};
    ipv6_address[1] = std::byte{0x01};
    ipv6_address[15] = std::byte{0x01};
    auto ipv6 = EndpointDescriptor::Ipv6Tcp(ipv6_address, 65535);
    ASSERT_TRUE(ipv6.ok()) << ipv6.status().ToString();

    const std::array<std::byte, 5> opaque = {
        std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}, std::byte{5}};
    auto fabric = EndpointDescriptor::SharedFabric(0x11223344, 0xaabbccdd,
                                                    opaque);
    ASSERT_TRUE(fabric.ok()) << fabric.status().ToString();

    for (const EndpointDescriptor& endpoint :
         std::array<EndpointDescriptor, 3>{ipv4, *ipv6, *fabric}) {
        const std::vector<std::byte> first = Serialize(endpoint);
        const std::vector<std::byte> second = Serialize(endpoint);
        EXPECT_EQ(first, second);
        auto parsed = ParseEndpointDescriptor(first);
        ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
        EXPECT_EQ(*parsed, endpoint);
    }

    const std::vector<std::byte> ipv4_golden = {
        std::byte{0x4d}, std::byte{0x01}, std::byte{0x00}, std::byte{0x01},
        std::byte{0x01}, std::byte{0x10}, std::byte{0x92}, std::byte{0x7f},
        std::byte{0x00}, std::byte{0x00}, std::byte{0x01}};
    const std::vector<std::byte> ipv6_golden = {
        std::byte{0x4d}, std::byte{0x01}, std::byte{0x00}, std::byte{0x02},
        std::byte{0x01}, std::byte{0xff}, std::byte{0xff}, std::byte{0x20},
        std::byte{0x01}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
        std::byte{0x00}, std::byte{0x00}, std::byte{0x01}};
    const std::vector<std::byte> fabric_golden = {
        std::byte{0x4d}, std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
        std::byte{0x00}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33},
        std::byte{0x44}, std::byte{0xaa}, std::byte{0xbb}, std::byte{0xcc},
        std::byte{0xdd}, std::byte{0x05}, std::byte{0x01}, std::byte{0x02},
        std::byte{0x03}, std::byte{0x04}, std::byte{0x05}};
    EXPECT_EQ(Serialize(ipv4), ipv4_golden);
    EXPECT_EQ(Serialize(*ipv6), ipv6_golden);
    EXPECT_EQ(Serialize(*fabric), fabric_golden);
    EXPECT_EQ(fabric->fabric_opaque().size(), opaque.size());
    EXPECT_TRUE(std::equal(fabric->fabric_opaque().begin(),
                           fabric->fabric_opaque().end(), opaque.begin()));
}

TEST(EndpointDescriptorTest, RejectsBadFactoryInputs) {
    const std::array<std::byte, 3> short_ipv4{};
    auto endpoint = EndpointDescriptor::Ipv4Tcp(short_ipv4, 80);
    ASSERT_FALSE(endpoint.ok());
    EXPECT_EQ(endpoint.status().code(), StatusCode::kInvalidArgument);

    const std::array<std::byte, 4> ipv4{};
    endpoint = EndpointDescriptor::Ipv4Tcp(ipv4, 0);
    ASSERT_FALSE(endpoint.ok());
    EXPECT_EQ(endpoint.status().code(), StatusCode::kInvalidArgument);

    const std::array<std::byte,
                     EndpointDescriptor::kMaxFabricOpaqueBytes + 1>
        oversized_opaque{};
    endpoint = EndpointDescriptor::SharedFabric(1, 2, oversized_opaque);
    ASSERT_FALSE(endpoint.ok());
    EXPECT_EQ(endpoint.status().code(), StatusCode::kInvalidArgument);

    std::array<std::byte, 4> too_small{};
    auto encoded = SerializeEndpointDescriptor(MakeIpv4(1, 80), too_small);
    ASSERT_FALSE(encoded.ok());
    EXPECT_EQ(encoded.status().code(), StatusCode::kResourceExhausted);
}

TEST(EndpointDescriptorTest, RejectsUnknownOversizedAndNonCanonicalWire) {
    const std::vector<std::byte> canonical = Serialize(MakeIpv4(2, 8080));

    std::vector<std::byte> unknown_kind = canonical;
    unknown_kind[2] = std::byte{0x7f};
    auto parsed = ParseEndpointDescriptor(unknown_kind);
    ASSERT_FALSE(parsed.ok());
    EXPECT_EQ(parsed.status().code(), StatusCode::kUnsupported);

    std::vector<std::byte> unknown_protocol = canonical;
    unknown_protocol[4] = std::byte{0x7f};
    parsed = ParseEndpointDescriptor(unknown_protocol);
    ASSERT_FALSE(parsed.ok());
    EXPECT_EQ(parsed.status().code(), StatusCode::kUnsupported);

    std::vector<std::byte> mismatched_kind = canonical;
    mismatched_kind[2] = static_cast<std::byte>(TransportKind::kRdma);
    parsed = ParseEndpointDescriptor(mismatched_kind);
    ASSERT_FALSE(parsed.ok());
    EXPECT_EQ(parsed.status().code(), StatusCode::kCorruption);

    std::vector<std::byte> zero_port = canonical;
    zero_port[5] = std::byte{0};
    zero_port[6] = std::byte{0};
    parsed = ParseEndpointDescriptor(zero_port);
    ASSERT_FALSE(parsed.ok());
    EXPECT_EQ(parsed.status().code(), StatusCode::kCorruption);

    std::vector<std::byte> trailing = canonical;
    trailing.push_back(std::byte{0});
    parsed = ParseEndpointDescriptor(trailing);
    ASSERT_FALSE(parsed.ok());
    EXPECT_EQ(parsed.status().code(), StatusCode::kCorruption);

    std::array<std::byte, EndpointDescriptor::kMaxSerializedSize + 1>
        oversized{};
    parsed = ParseEndpointDescriptor(oversized);
    ASSERT_FALSE(parsed.ok());
    EXPECT_EQ(parsed.status().code(), StatusCode::kResourceExhausted);

    auto fabric = EndpointDescriptor::SharedFabric(
        1, 2, std::array<std::byte, 1>{std::byte{3}});
    ASSERT_TRUE(fabric.ok());
    std::vector<std::byte> oversized_fabric = Serialize(*fabric);
    oversized_fabric[13] = static_cast<std::byte>(
        EndpointDescriptor::kMaxFabricOpaqueBytes + 1);
    parsed = ParseEndpointDescriptor(oversized_fabric);
    ASSERT_FALSE(parsed.ok());
    EXPECT_EQ(parsed.status().code(), StatusCode::kResourceExhausted);
}

class MockTransportDriver final : public TransportDriver {
public:
    MockTransportDriver()
        : local_(MakeIpv4(1, 3000)), peer_(MakeIpv4(2, 3001)) {}

    HealthState health() const noexcept override {
        return state() == DriverState::kRunning ? HealthState::kHealthy
                                                : HealthState::kUnavailable;
    }
    TransportCapabilities capabilities() const noexcept override {
        return {
            .kind = TransportKind::kNetwork,
            .reliability = TransportReliability::kReliable,
            .max_frame_size = 1024,
            .max_reassembly_bytes = 4096,
            .features = Capability::kConnect | Capability::kListen,
        };
    }

    void return_bad_send_result(bool value) {
        std::lock_guard lock(data_mutex_);
        bad_send_result_ = value;
    }

    void throw_bad_alloc_from_send(bool value) {
        std::lock_guard lock(data_mutex_);
        throw_bad_alloc_from_send_ = value;
    }

    void QueueCompletion(DeliveryCompletion completion) {
        std::lock_guard lock(data_mutex_);
        completions_.push_back(std::move(completion));
    }

    void BlockStart() {
        std::lock_guard lock(control_mutex_);
        block_start_ = true;
        start_entered_ = false;
    }

    void WaitForStartEntered() {
        std::unique_lock lock(control_mutex_);
        control_cv_.wait(lock, [this] { return start_entered_; });
    }

    void ReleaseStart() {
        std::lock_guard lock(control_mutex_);
        block_start_ = false;
        control_cv_.notify_all();
    }

    void BlockSend() {
        std::lock_guard lock(control_mutex_);
        block_send_ = true;
        send_entered_ = false;
    }

    void WaitForSendEntered() {
        std::unique_lock lock(control_mutex_);
        control_cv_.wait(lock, [this] { return send_entered_; });
    }

    void ReleaseSend() {
        std::lock_guard lock(control_mutex_);
        block_send_ = false;
        control_cv_.notify_all();
    }

    uint32_t start_calls() const noexcept {
        return start_calls_.load(std::memory_order_relaxed);
    }

    uint32_t shutdown_calls() const noexcept {
        return shutdown_calls_.load(std::memory_order_relaxed);
    }

protected:
    Status DoStart(const DriverConfig& config) override {
        start_calls_.fetch_add(1, std::memory_order_relaxed);
        {
            std::unique_lock lock(control_mutex_);
            start_entered_ = true;
            control_cv_.notify_all();
            control_cv_.wait(lock, [this] { return !block_start_; });
        }
        std::lock_guard lock(data_mutex_);
        config_ = config;
        connection_closed_ = false;
        return Status::Ok();
    }

    Status DoShutdown() override {
        shutdown_calls_.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard lock(data_mutex_);
        queued_.clear();
        completions_.clear();
        connection_closed_ = true;
        return Status::Ok();
    }

    Result<ConnectionInfo> DoConnect(
        const ConnectRequest& request) override {
        std::lock_guard lock(data_mutex_);
        connection_closed_ = false;
        return ConnectionInfo{
            .id = 1,
            .kind = TransportKind::kNetwork,
            .is_listener = false,
            .local_endpoint = request.local_bind.value_or(local_),
            .peer_endpoint = request.remote_endpoint,
        };
    }

    Result<ConnectionInfo> DoListen(const ListenRequest& request) override {
        return ConnectionInfo{
            .id = 2,
            .kind = TransportKind::kNetwork,
            .is_listener = true,
            .local_endpoint = request.local_endpoint,
            .peer_endpoint = std::nullopt,
        };
    }

    Result<SendResult> DoSend(const SendRequest& request,
                              SendOperation operation) override {
        {
            std::unique_lock lock(control_mutex_);
            send_entered_ = true;
            control_cv_.notify_all();
            control_cv_.wait(lock, [this] { return !block_send_; });
        }
        std::lock_guard lock(data_mutex_);
        if (throw_bad_alloc_from_send_) throw std::bad_alloc();
        if (request.connection_id != 1 || connection_closed_) {
            return Status::Error(StatusCode::kNotFound);
        }
        queued_.push_back(ReceivedMessage{
            .connection_id = request.connection_id,
            .from = peer_,
            .payload = std::vector<std::byte>(request.payload.begin(),
                                              request.payload.end()),
        });
        if (bad_send_result_) ++operation.id;
        return SendResult{.operation = operation,
                          .admitted_bytes = request.payload.size()};
    }

    Result<size_t> DoSendUntracked(
        const UntrackedSendRequest& request) override {
        std::lock_guard lock(data_mutex_);
        if (request.connection_id != 1 || connection_closed_) {
            return Status::Error(StatusCode::kNotFound);
        }
        queued_.push_back(ReceivedMessage{
            .connection_id = request.connection_id,
            .from = peer_,
            .payload = std::vector<std::byte>(request.payload.begin(),
                                              request.payload.end()),
        });
        return request.payload.size();
    }

    Result<ReceiveResult> DoPoll(const ReceiveRequest& request) override {
        std::lock_guard lock(data_mutex_);
        if (queued_.empty()) {
            return Status::Error(request.timeout_ms == 0
                                     ? StatusCode::kWouldBlock
                                     : StatusCode::kTimeout);
        }
        if (queued_.front().payload.size() > request.max_bytes) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        ReceiveResult result;
        result.messages.push_back(std::move(queued_.front()));
        queued_.erase(queued_.begin());
        return result;
    }

    Result<CompletionPollResult> DoPollCompletions(
        const CompletionPollRequest& request) override {
        std::lock_guard lock(data_mutex_);
        if (completions_.empty()) {
            return Status::Error(request.timeout_ms == 0
                                     ? StatusCode::kWouldBlock
                                     : StatusCode::kTimeout);
        }
        CompletionPollResult result;
        const size_t count =
            std::min<size_t>(request.max_completions, completions_.size());
        result.completions.insert(result.completions.end(),
                                  completions_.begin(),
                                  completions_.begin() + count);
        completions_.erase(completions_.begin(),
                           completions_.begin() + count);
        return result;
    }

    Status DoClose(ConnectionId connection_id) override {
        std::lock_guard lock(data_mutex_);
        if (connection_id == 1) {
            connection_closed_ = true;
            return Status::Ok();
        }
        if (connection_id == 2) return Status::Ok();
        return Status::Error(StatusCode::kNotFound);
    }

private:
    mutable std::mutex data_mutex_;
    std::mutex control_mutex_;
    std::condition_variable control_cv_;
    DriverConfig config_;
    EndpointDescriptor local_;
    EndpointDescriptor peer_;
    bool connection_closed_ = true;
    bool bad_send_result_ = false;
    bool throw_bad_alloc_from_send_ = false;
    bool block_start_ = false;
    bool start_entered_ = false;
    bool block_send_ = false;
    bool send_entered_ = false;
    std::vector<ReceivedMessage> queued_;
    std::vector<DeliveryCompletion> completions_;
    std::atomic<uint32_t> start_calls_{0};
    std::atomic<uint32_t> shutdown_calls_{0};
};

TEST(TransportDriverTest, SendAdmissionDoesNotCompleteRemoteReceipt) {
    MockTransportDriver driver;
    const EndpointDescriptor peer = MakeIpv4(2, 3001);

    auto connection = driver.Connect(ConnectRequest{
        .remote_endpoint = peer, .local_bind = std::nullopt, .timeout_ms = 0});
    ASSERT_FALSE(connection.ok());
    EXPECT_EQ(connection.status().code(), StatusCode::kUnavailable);

    DriverConfig invalid_config;
    invalid_config.max_connections = 0;
    EXPECT_EQ(driver.Start(invalid_config).code(),
              StatusCode::kInvalidArgument);
    ASSERT_TRUE(driver.Start(DriverConfig{}).ok());
    EXPECT_EQ(driver.Start(DriverConfig{}).code(),
              StatusCode::kAlreadyExists);
    EXPECT_EQ(driver.health(), HealthState::kHealthy);

    connection = driver.Connect(ConnectRequest{
        .remote_endpoint = peer, .local_bind = std::nullopt, .timeout_ms = 0});
    ASSERT_TRUE(connection.ok()) << connection.status().ToString();
    EXPECT_FALSE(connection->is_listener);
    EXPECT_EQ(connection->id, 1u);

    auto listener = driver.Listen(ListenRequest{
        .local_endpoint = MakeIpv4(1, 4000), .backlog = 8});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    EXPECT_TRUE(listener->is_listener);

    const std::array<std::byte, 3> payload = {
        std::byte{1}, std::byte{2}, std::byte{3}};
    auto sent = driver.Send(
        SendRequest{.connection_id = connection->id, .payload = payload});
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();
    EXPECT_EQ(sent->admitted_bytes, payload.size());
    EXPECT_NE(sent->operation.id, kInvalidOperationId);
    EXPECT_EQ(sent->operation.connection_id, connection->id);

    auto completed = driver.PollCompletions(CompletionPollRequest{});
    ASSERT_FALSE(completed.ok());
    EXPECT_EQ(completed.status().code(), StatusCode::kWouldBlock);

    driver.QueueCompletion(DeliveryCompletion{
        .operation = sent->operation,
        .reached_stage = DeliveryStage::kRemoteAccepted,
    });
    completed = driver.PollCompletions(CompletionPollRequest{});
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
    ASSERT_EQ(completed->completions.size(), 1u);
    EXPECT_EQ(completed->completions[0].operation, sent->operation);
    EXPECT_EQ(completed->completions[0].reached_stage,
              DeliveryStage::kRemoteAccepted);

    auto received = driver.Poll(
        ReceiveRequest{.max_messages = 1, .max_bytes = payload.size()});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].payload,
              std::vector<std::byte>(payload.begin(), payload.end()));

    received = driver.Poll(ReceiveRequest{});
    ASSERT_FALSE(received.ok());
    EXPECT_EQ(received.status().code(), StatusCode::kWouldBlock);

    auto untracked = driver.SendUntracked(
        UntrackedSendRequest{.connection_id = connection->id,
                             .payload = payload});
    ASSERT_TRUE(untracked.ok()) << untracked.status().ToString();
    EXPECT_EQ(*untracked, payload.size());
    completed = driver.PollCompletions(CompletionPollRequest{});
    ASSERT_FALSE(completed.ok());
    EXPECT_EQ(completed.status().code(), StatusCode::kWouldBlock);
    received = driver.Poll(
        ReceiveRequest{.max_messages = 1, .max_bytes = payload.size()});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(received->messages[0].payload,
              std::vector<std::byte>(payload.begin(), payload.end()));

    ASSERT_TRUE(driver.Close(connection->id).ok());
    ASSERT_TRUE(driver.Close(connection->id).ok());
    ASSERT_TRUE(driver.Shutdown().ok());
    ASSERT_TRUE(driver.Shutdown().ok());
    EXPECT_EQ(driver.state(), DriverState::kStopped);
}

TEST(TransportDriverTest, WrapperRejectsBoundsAndBrokenDriverResults) {
    MockTransportDriver driver;
    ASSERT_TRUE(driver.Start(DriverConfig{}).ok());
    auto connection = driver.Connect(ConnectRequest{
        .remote_endpoint = MakeIpv4(2, 3001),
        .local_bind = std::nullopt,
        .timeout_ms = 0,
    });
    ASSERT_TRUE(connection.ok());

    std::array<std::byte, 1025> oversized_frame{};
    auto sent = driver.Send(SendRequest{.connection_id = connection->id,
                                        .payload = oversized_frame});
    ASSERT_FALSE(sent.ok());
    EXPECT_EQ(sent.status().code(), StatusCode::kResourceExhausted);

    sent = driver.Send(SendRequest{.connection_id = connection->id,
                                   .payload = {}});
    ASSERT_FALSE(sent.ok());
    EXPECT_EQ(sent.status().code(), StatusCode::kInvalidArgument);

    auto received = driver.Poll(ReceiveRequest{.max_messages = 0});
    ASSERT_FALSE(received.ok());
    EXPECT_EQ(received.status().code(), StatusCode::kInvalidArgument);

    driver.return_bad_send_result(true);
    const std::array<std::byte, 2> payload = {std::byte{1}, std::byte{2}};
    sent = driver.Send(SendRequest{.connection_id = connection->id,
                                   .payload = payload});
    ASSERT_FALSE(sent.ok());
    EXPECT_EQ(sent.status().code(), StatusCode::kInternal);

    driver.return_bad_send_result(false);
    driver.throw_bad_alloc_from_send(true);
    sent = driver.Send(SendRequest{.connection_id = connection->id,
                                   .payload = payload});
    ASSERT_FALSE(sent.ok());
    EXPECT_EQ(sent.status().code(), StatusCode::kResourceExhausted);
}

TEST(TransportDriverTest, CompletionMustMatchTicketAndRequestedStage) {
    MockTransportDriver driver;
    ASSERT_TRUE(driver.Start(DriverConfig{}).ok());
    const std::array<std::byte, 2> payload = {std::byte{1}, std::byte{2}};
    auto sent = driver.Send(
        SendRequest{.connection_id = 1, .payload = payload});
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();

    SendOperation wrong_identity = sent->operation;
    ++wrong_identity.connection_id;
    driver.QueueCompletion(DeliveryCompletion{
        .operation = wrong_identity,
        .reached_stage = DeliveryStage::kRemoteAccepted,
    });
    auto completed = driver.PollCompletions(CompletionPollRequest{});
    ASSERT_FALSE(completed.ok());
    EXPECT_EQ(completed.status().code(), StatusCode::kInternal);

    driver.QueueCompletion(DeliveryCompletion{
        .operation = sent->operation,
        .reached_stage = DeliveryStage::kRecorderBuffered,
    });
    completed = driver.PollCompletions(CompletionPollRequest{});
    ASSERT_FALSE(completed.ok());
    EXPECT_EQ(completed.status().code(), StatusCode::kInternal);

    driver.QueueCompletion(DeliveryCompletion{
        .operation = sent->operation,
        .reached_stage = DeliveryStage::kLocalPublished,
    });
    completed = driver.PollCompletions(CompletionPollRequest{});
    ASSERT_FALSE(completed.ok());
    EXPECT_EQ(completed.status().code(), StatusCode::kInternal);

    driver.QueueCompletion(DeliveryCompletion{
        .operation = sent->operation,
        .reached_stage = DeliveryStage::kRemoteAccepted,
    });
    completed = driver.PollCompletions(CompletionPollRequest{});
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
}

TEST(TransportDriverTest, ConcurrentStartsInvokeDoStartOnce) {
    MockTransportDriver driver;
    driver.BlockStart();
    Status first_status;
    std::thread first([&] { first_status = driver.Start(DriverConfig{}); });
    driver.WaitForStartEntered();

    EXPECT_EQ(driver.Start(DriverConfig{}).code(),
              StatusCode::kAlreadyExists);
    EXPECT_EQ(driver.start_calls(), 1u);

    driver.ReleaseStart();
    first.join();
    EXPECT_TRUE(first_status.ok()) << first_status.ToString();
    EXPECT_EQ(driver.state(), DriverState::kRunning);
    EXPECT_TRUE(driver.Shutdown().ok());
}

TEST(TransportDriverTest, ShutdownFencesAndWaitsForActiveSend) {
    MockTransportDriver driver;
    ASSERT_TRUE(driver.Start(DriverConfig{}).ok());
    driver.BlockSend();
    const std::array<std::byte, 1> payload = {std::byte{1}};
    StatusCode send_code = StatusCode::kInternal;
    Status shutdown_status;

    std::thread sender([&] {
        auto sent = driver.Send(
            SendRequest{.connection_id = 1, .payload = payload});
        send_code = sent.ok() ? StatusCode::kOk : sent.status().code();
    });
    driver.WaitForSendEntered();
    std::thread shutdown([&] { shutdown_status = driver.Shutdown(); });
    while (driver.state() != DriverState::kStopping) std::this_thread::yield();

    EXPECT_EQ(driver.shutdown_calls(), 0u);
    auto rejected = driver.Poll(ReceiveRequest{});
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kUnavailable);

    driver.ReleaseSend();
    sender.join();
    shutdown.join();
    EXPECT_EQ(send_code, StatusCode::kOk);
    EXPECT_TRUE(shutdown_status.ok()) << shutdown_status.ToString();
    EXPECT_EQ(driver.shutdown_calls(), 1u);
    EXPECT_EQ(driver.state(), DriverState::kStopped);
}

TEST(TransportDriverTest, StartShutdownAndIoRacesAreFenced) {
    MockTransportDriver driver;
    ASSERT_TRUE(driver.Start(DriverConfig{}).ok());
    constexpr int kIterations = 200;
    const std::array<std::byte, 1> payload = {std::byte{7}};
    std::atomic<bool> go{false};
    std::atomic<uint32_t> unexpected{0};

    auto wait_to_start = [&] {
        while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
    };
    auto allowed = [&](StatusCode code,
                       std::initializer_list<StatusCode> expected) {
        if (std::find(expected.begin(), expected.end(), code) == expected.end()) {
            unexpected.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::thread starter([&] {
        wait_to_start();
        for (int i = 0; i < kIterations; ++i) {
            const Status status = driver.Start(DriverConfig{});
            allowed(status.code(), {StatusCode::kOk,
                                    StatusCode::kAlreadyExists,
                                    StatusCode::kUnavailable});
        }
    });
    std::thread stopper([&] {
        wait_to_start();
        for (int i = 0; i < kIterations; ++i) {
            const Status status = driver.Shutdown();
            allowed(status.code(), {StatusCode::kOk});
        }
    });
    std::thread sender([&] {
        wait_to_start();
        for (int i = 0; i < kIterations; ++i) {
            auto result = driver.Send(
                SendRequest{.connection_id = 1, .payload = payload});
            allowed(result.ok() ? StatusCode::kOk : result.status().code(),
                    {StatusCode::kOk, StatusCode::kUnavailable,
                     StatusCode::kNotFound, StatusCode::kWouldBlock});
        }
    });
    std::thread poller([&] {
        wait_to_start();
        for (int i = 0; i < kIterations; ++i) {
            auto received = driver.Poll(ReceiveRequest{});
            allowed(received.ok() ? StatusCode::kOk : received.status().code(),
                    {StatusCode::kOk, StatusCode::kUnavailable,
                     StatusCode::kWouldBlock});
            auto completed =
                driver.PollCompletions(CompletionPollRequest{});
            allowed(completed.ok() ? StatusCode::kOk
                                   : completed.status().code(),
                    {StatusCode::kOk, StatusCode::kUnavailable,
                     StatusCode::kWouldBlock});
        }
    });
    std::thread closer([&] {
        wait_to_start();
        for (int i = 0; i < kIterations; ++i) {
            const Status status = driver.Close(1);
            allowed(status.code(), {StatusCode::kOk,
                                    StatusCode::kUnavailable,
                                    StatusCode::kNotFound});
        }
    });

    go.store(true, std::memory_order_release);
    starter.join();
    stopper.join();
    sender.join();
    poller.join();
    closer.join();
    EXPECT_EQ(unexpected.load(std::memory_order_relaxed), 0u);
    EXPECT_TRUE(driver.Shutdown().ok());
    EXPECT_EQ(driver.state(), DriverState::kStopped);
}

}  // namespace
}  // namespace mino::transport
