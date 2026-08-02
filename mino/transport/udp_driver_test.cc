// Copyright 2026 The Mino Authors

#include "mino/transport/udp_driver.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

namespace mino::transport {
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

uint16_t FindUnusedUdpPort() {
    ScopedFd fd(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    EXPECT_GE(fd.get(), 0);
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

EndpointDescriptor UdpLoopback(uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = EndpointDescriptor::Ipv4Udp(address, port);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    return endpoint.ok() ? *endpoint : EndpointDescriptor{};
}

DriverConfig Config() {
    return DriverConfig{
        .max_connections = 16,
        .max_listeners = 4,
        .max_queued_sends = 32,
    };
}

TEST(UdpDriverOptionsTest, ValidatesDatagramAndSocketBounds) {
    EXPECT_TRUE(ValidateUdpDriverOptions({}).ok());
    UdpDriverOptions options;
    options.max_datagram_bytes = kUdpMaximumDatagramBytes + 1;
    EXPECT_EQ(ValidateUdpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
    options = {};
    options.socket_receive_buffer_bytes = options.max_datagram_bytes - 1;
    EXPECT_EQ(ValidateUdpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
}

TEST(UdpEndpointTest, ExplicitCodecRoundTripsWithoutConfusingTcp) {
    const EndpointDescriptor udp = UdpLoopback(4242);
    EXPECT_EQ(udp.protocol(), NetworkProtocol::kUdp);
    EXPECT_NE(udp, EndpointDescriptor::Ipv4Tcp(udp.ip_address(), udp.port()).value());

    std::array<std::byte, EndpointDescriptor::kMaxSerializedSize> storage{};
    auto encoded = SerializeEndpointDescriptor(udp, storage);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    auto parsed = ParseEndpointDescriptor(
        std::span<const std::byte>(storage).first(*encoded));
    ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
    EXPECT_EQ(*parsed, udp);
}

TEST(UdpDriverTest, PreservesDatagramBoundariesAndReportsDegradedLocalCompletion) {
    auto server_result = UdpDriver::Create();
    auto client_result = UdpDriver::Create();
    ASSERT_TRUE(server_result.ok());
    ASSERT_TRUE(client_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    std::unique_ptr<UdpDriver> client = std::move(*client_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    ASSERT_TRUE(client->Start(Config()).ok());
    EXPECT_FALSE(client->capabilities().features.Has(
        Capability::kRemoteAcceptedConfirmation));

    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    auto connected = client->Connect(
        {.remote_endpoint = endpoint, .local_bind = std::nullopt, .timeout_ms = 0});
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();

    const std::vector<std::byte> first(17, std::byte{0x11});
    const std::vector<std::byte> second(31, std::byte{0x22});
    auto first_send = client->Send({
        .connection_id = connected->id,
        .payload = first,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    auto second_send = client->Send({
        .connection_id = connected->id,
        .payload = second,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_TRUE(first_send.ok()) << first_send.status().ToString();
    ASSERT_TRUE(second_send.ok()) << second_send.status().ToString();
    EXPECT_EQ(client->ConfirmRemoteAccepted(first_send->operation).code(),
              StatusCode::kUnsupported);

    auto received = server->Poll(
        {.max_messages = 2, .max_bytes = 1200, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 2u);
    EXPECT_EQ(received->messages[0].connection_id, listener->id);
    EXPECT_EQ(received->messages[0].payload, first);
    EXPECT_EQ(received->messages[1].payload, second);
    EXPECT_EQ(received->messages[0].from.protocol(), NetworkProtocol::kUdp);

    auto completed = client->PollCompletions(
        {.max_completions = 2, .timeout_ms = 1000});
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
    ASSERT_EQ(completed->completions.size(), 2u);
    for (const DeliveryCompletion& completion : completed->completions) {
        EXPECT_EQ(completion.reached_stage, DeliveryStage::kLocalPublished);
        EXPECT_EQ(completion.status.code(), StatusCode::kDegraded);
    }
}

TEST(UdpDriverTest, ConnectionFiltersPreserveOtherDatagramsAndCompletions) {
    auto server_result = UdpDriver::Create();
    auto client_result = UdpDriver::Create();
    ASSERT_TRUE(server_result.ok());
    ASSERT_TRUE(client_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    std::unique_ptr<UdpDriver> client = std::move(*client_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    ASSERT_TRUE(client->Start(Config()).ok());

    const EndpointDescriptor first_endpoint =
        UdpLoopback(FindUnusedUdpPort());
    auto first_listener = server->Listen(
        {.local_endpoint = first_endpoint, .backlog = 1});
    ASSERT_TRUE(first_listener.ok()) << first_listener.status().ToString();
    const EndpointDescriptor second_endpoint =
        UdpLoopback(FindUnusedUdpPort());
    auto second_listener = server->Listen(
        {.local_endpoint = second_endpoint, .backlog = 1});
    ASSERT_TRUE(second_listener.ok()) << second_listener.status().ToString();

    auto first_connection = client->Connect({
        .remote_endpoint = first_endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 0,
    });
    auto second_connection = client->Connect({
        .remote_endpoint = second_endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 0,
    });
    ASSERT_TRUE(first_connection.ok()) << first_connection.status().ToString();
    ASSERT_TRUE(second_connection.ok()) << second_connection.status().ToString();

    const std::vector<std::byte> first(19, std::byte{0x31});
    const std::vector<std::byte> second(29, std::byte{0x32});
    auto first_send = client->Send({
        .connection_id = first_connection->id,
        .payload = first,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    auto second_send = client->Send({
        .connection_id = second_connection->id,
        .payload = second,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_TRUE(first_send.ok()) << first_send.status().ToString();
    ASSERT_TRUE(second_send.ok()) << second_send.status().ToString();

    auto second_received = server->Poll({
        .max_messages = 2,
        .max_bytes = 1200,
        .timeout_ms = 1000,
        .connection_id = second_listener->id,
    });
    ASSERT_TRUE(second_received.ok()) << second_received.status().ToString();
    ASSERT_EQ(second_received->messages.size(), 1u);
    EXPECT_EQ(second_received->messages[0].connection_id,
              second_listener->id);
    EXPECT_EQ(second_received->messages[0].payload, second);
    EXPECT_EQ(server->Poll({
                  .max_messages = 1,
                  .max_bytes = 1200,
                  .timeout_ms = 0,
                  .connection_id = second_listener->id,
              }).status().code(),
              StatusCode::kWouldBlock);

    auto first_received = server->Poll({
        .max_messages = 1,
        .max_bytes = 1200,
        .timeout_ms = 0,
        .connection_id = first_listener->id,
    });
    ASSERT_TRUE(first_received.ok()) << first_received.status().ToString();
    ASSERT_EQ(first_received->messages.size(), 1u);
    EXPECT_EQ(first_received->messages[0].connection_id, first_listener->id);
    EXPECT_EQ(first_received->messages[0].payload, first);

    auto second_completion = client->PollCompletions({
        .max_completions = 2,
        .timeout_ms = 0,
        .connection_id = second_connection->id,
    });
    ASSERT_TRUE(second_completion.ok())
        << second_completion.status().ToString();
    ASSERT_EQ(second_completion->completions.size(), 1u);
    EXPECT_EQ(second_completion->completions[0].operation,
              second_send->operation);
    EXPECT_EQ(client->PollCompletions({
                  .max_completions = 1,
                  .timeout_ms = 0,
                  .connection_id = second_connection->id,
              }).status().code(),
              StatusCode::kWouldBlock);

    auto first_completion = client->PollCompletions({
        .max_completions = 1,
        .timeout_ms = 0,
        .connection_id = first_connection->id,
    });
    ASSERT_TRUE(first_completion.ok())
        << first_completion.status().ToString();
    ASSERT_EQ(first_completion->completions.size(), 1u);
    EXPECT_EQ(first_completion->completions[0].operation,
              first_send->operation);
}

TEST(UdpDriverTest, UntrackedDatagramProducesNoCompletion) {
    auto server_result = UdpDriver::Create();
    auto client_result = UdpDriver::Create();
    ASSERT_TRUE(server_result.ok());
    ASSERT_TRUE(client_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    std::unique_ptr<UdpDriver> client = std::move(*client_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    ASSERT_TRUE(client->Start(Config()).ok());

    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    auto connected = client->Connect({
        .remote_endpoint = endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 0,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();

    const std::vector<std::byte> payload(23, std::byte{0x3c});
    auto sent = client->SendUntracked({
        .connection_id = connected->id,
        .payload = payload,
        .traffic_class = UntrackedTrafficClass::kData,
    });
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();
    EXPECT_EQ(*sent, payload.size());

    auto received = server->Poll(
        {.max_messages = 1, .max_bytes = 1200, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].payload, payload);

    auto completion = client->PollCompletions(
        {.max_completions = 1, .timeout_ms = 50});
    ASSERT_FALSE(completion.ok());
    EXPECT_EQ(completion.status().code(), StatusCode::kTimeout);
}

TEST(UdpDriverTest, RejectsOversizedDatagramAndListenerSend) {
    UdpDriverOptions options;
    options.max_datagram_bytes = 256;
    auto server_result = UdpDriver::Create(options);
    auto client_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    ASSERT_TRUE(client_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    std::unique_ptr<UdpDriver> client = std::move(*client_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    ASSERT_TRUE(client->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());
    auto connected = client->Connect({
        .remote_endpoint = endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 0,
    });
    ASSERT_TRUE(connected.ok());

    const std::vector<std::byte> oversized(257, std::byte{1});
    auto rejected = client->Send({
        .connection_id = connected->id,
        .payload = oversized,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kResourceExhausted);

    const std::vector<std::byte> payload(8, std::byte{2});
    auto listener_send = server->Send({
        .connection_id = listener->id,
        .payload = payload,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_FALSE(listener_send.ok());
    EXPECT_EQ(listener_send.status().code(), StatusCode::kUnsupported);
    EXPECT_EQ(server->Accept({.listener_id = listener->id}).status().code(),
              StatusCode::kUnsupported);
}

TEST(UdpDriverTest, EmptyDatagramDoesNotSpinOrEscapeAsMessage) {
    auto server_result = UdpDriver::Create();
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(endpoint.port());
    ASSERT_EQ(::sendto(peer.get(), nullptr, 0, 0,
                       reinterpret_cast<sockaddr*>(&address), sizeof(address)),
              0);
    auto polled = server->Poll(
        {.max_messages = 1, .max_bytes = 1200, .timeout_ms = 30});
    ASSERT_FALSE(polled.ok());
    EXPECT_EQ(polled.status().code(), StatusCode::kTimeout);
}

TEST(UdpDriverTest, ShutdownWakesBlockingReceive) {
    auto driver_result = UdpDriver::Create();
    ASSERT_TRUE(driver_result.ok());
    std::unique_ptr<UdpDriver> driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(Config()).ok());

    std::atomic<StatusCode> result{StatusCode::kOk};
    std::thread receiver([&] {
        auto polled = driver->Poll(
            {.max_messages = 1, .max_bytes = 1200, .timeout_ms = 60'000});
        result.store(polled.ok() ? StatusCode::kOk : polled.status().code(),
                     std::memory_order_release);
    });
    std::this_thread::sleep_for(10ms);
    const auto started = std::chrono::steady_clock::now();
    EXPECT_TRUE(driver->Shutdown().ok());
    receiver.join();
    EXPECT_LT(std::chrono::steady_clock::now() - started, 1s);
    EXPECT_EQ(result.load(std::memory_order_acquire), StatusCode::kUnavailable);
}

}  // namespace
}  // namespace mino::transport
