// Copyright 2026 The Mino Authors

#include "mino/transport/tcp_driver.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <thread>
#include <vector>

#include "mino/bridge/wire_frame.h"

namespace mino::transport {
namespace {

using namespace std::chrono_literals;

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    ScopedFd& operator=(ScopedFd&&) = delete;
    ~ScopedFd() {
        if (fd_ >= 0) (void)::close(fd_);
    }
    int get() const noexcept { return fd_; }

private:
    int fd_;
};

uint16_t FindUnusedLoopbackPort() {
    ScopedFd socket_fd(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    EXPECT_GE(socket_fd.get(), 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    EXPECT_EQ(::bind(socket_fd.get(), reinterpret_cast<sockaddr*>(&address),
                     sizeof(address)),
              0);
    socklen_t size = sizeof(address);
    EXPECT_EQ(::getsockname(socket_fd.get(),
                            reinterpret_cast<sockaddr*>(&address), &size),
              0);
    return ntohs(address.sin_port);
}

EndpointDescriptor Loopback(uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = EndpointDescriptor::Ipv4Tcp(address, port);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    return endpoint.ok() ? *endpoint : EndpointDescriptor{};
}

std::vector<std::byte> FrameBody(size_t payload_size = 128,
                                 uint64_t sequence = 1) {
    bridge::WireFrame frame;
    frame.header.topic_id = 7;
    frame.header.msg_type = 9;
    frame.header.connection_schema_ref = 3;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 11;
    frame.header.source_publisher_id = 13;
    frame.header.source_publisher_epoch = 17;
    frame.header.sequence_num = sequence;
    frame.payload.resize(payload_size, std::byte{0x5a});
    auto encoded = bridge::WireFrameCodec::Encode(frame);
    EXPECT_TRUE(encoded.ok()) << encoded.status().ToString();
    return encoded.ok() ? std::move(*encoded) : std::vector<std::byte>{};
}

std::vector<std::byte> Prefix(std::span<const std::byte> body) {
    std::vector<std::byte> result(4 + body.size());
    const uint32_t size = static_cast<uint32_t>(body.size());
    result[0] = static_cast<std::byte>(size >> 24);
    result[1] = static_cast<std::byte>(size >> 16);
    result[2] = static_cast<std::byte>(size >> 8);
    result[3] = static_cast<std::byte>(size);
    std::copy(body.begin(), body.end(), result.begin() + 4);
    return result;
}

TcpDriverOptions TestOptions() {
    return TcpDriverOptions{
        .max_frame_body_bytes = 4096,
        .max_total_send_buffer_bytes = 32 * 1024,
        .max_connection_send_buffer_bytes = 16 * 1024,
        .max_ready_receive_bytes = 32 * 1024,
        .max_ready_receive_messages = 32,
        .max_pending_accepts = 8,
        .heartbeat_interval_ms = 20,
        .idle_timeout_ms = 500,
        .partial_frame_timeout_ms = 250,
        .io_poll_max_ms = 5,
    };
}

DriverConfig TestConfig() {
    return DriverConfig{
        .max_connections = 16,
        .max_listeners = 4,
        .max_queued_sends = 32,
    };
}

struct DriverPair {
    std::unique_ptr<TcpDriver> server;
    std::unique_ptr<TcpDriver> client;
    ConnectionInfo listener;
    ConnectionInfo client_connection;
    ConnectionInfo server_connection;
};

DriverPair ConnectPair(TcpDriverOptions options = TestOptions()) {
    DriverPair pair;
    auto server = TcpDriver::Create(options);
    auto client = TcpDriver::Create(options);
    EXPECT_TRUE(server.ok()) << server.status().ToString();
    EXPECT_TRUE(client.ok()) << client.status().ToString();
    if (!server.ok() || !client.ok()) return pair;
    pair.server = std::move(*server);
    pair.client = std::move(*client);
    EXPECT_TRUE(pair.server->Start(TestConfig()).ok());
    EXPECT_TRUE(pair.client->Start(TestConfig()).ok());

    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = pair.server->Listen({.local_endpoint = endpoint, .backlog = 4});
    EXPECT_TRUE(listener.ok()) << listener.status().ToString();
    if (!listener.ok()) return pair;
    pair.listener = *listener;

    auto connected = pair.client->Connect(
        {.remote_endpoint = endpoint, .local_bind = std::nullopt, .timeout_ms = 1000});
    EXPECT_TRUE(connected.ok()) << connected.status().ToString();
    if (!connected.ok()) return pair;
    pair.client_connection = *connected;

    auto accepted = pair.server->Accept(
        {.listener_id = pair.listener.id, .timeout_ms = 1000});
    EXPECT_TRUE(accepted.ok()) << accepted.status().ToString();
    if (accepted.ok()) pair.server_connection = *accepted;
    return pair;
}

ScopedFd ConnectRaw(const EndpointDescriptor& endpoint) {
    ScopedFd fd(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    EXPECT_GE(fd.get(), 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(endpoint.port());
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    EXPECT_EQ(::connect(fd.get(), reinterpret_cast<sockaddr*>(&address),
                        sizeof(address)),
              0);
    return fd;
}

bool WaitForConnectionCount(const TcpDriver& driver, size_t expected,
                            std::chrono::milliseconds timeout = 1000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (driver.stats().active_connections == expected) return true;
        std::this_thread::sleep_for(2ms);
    }
    return driver.stats().active_connections == expected;
}

TEST(TcpDriverOptionsTest, RejectsUnboundedOrContradictoryConfiguration) {
    TcpDriverOptions options = TestOptions();
    EXPECT_TRUE(ValidateTcpDriverOptions(options).ok());

    options.heartbeat_interval_ms = options.idle_timeout_ms;
    EXPECT_EQ(ValidateTcpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
    options = TestOptions();
    options.max_ready_receive_bytes = options.max_frame_body_bytes - 1;
    EXPECT_EQ(ValidateTcpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
}

TEST(TcpDriverTest, LoopbackRoundTripStripsPrefixAndWriteIsNotRemoteAccepted) {
    DriverPair pair = ConnectPair();
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);
    ASSERT_NE(pair.client_connection.id, kInvalidConnectionId);
    ASSERT_NE(pair.server_connection.id, kInvalidConnectionId);
    EXPECT_NE(pair.listener.id, pair.server_connection.id);

    const std::vector<std::byte> body = FrameBody(256);
    auto sent = pair.client->Send({
        .connection_id = pair.client_connection.id,
        .payload = body,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();
    EXPECT_EQ(sent->admitted_bytes, body.size());

    auto received = pair.server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].connection_id,
              pair.server_connection.id);
    EXPECT_EQ(received->messages[0].payload, body);

    auto no_ack = pair.client->PollCompletions(
        {.max_completions = 1, .timeout_ms = 50});
    ASSERT_FALSE(no_ack.ok());
    EXPECT_EQ(no_ack.status().code(), StatusCode::kTimeout);

    ASSERT_TRUE(pair.client->Close(pair.client_connection.id).ok());
    auto failed = pair.client->PollCompletions(
        {.max_completions = 1, .timeout_ms = 1000});
    ASSERT_TRUE(failed.ok()) << failed.status().ToString();
    ASSERT_EQ(failed->completions.size(), 1u);
    EXPECT_EQ(failed->completions[0].operation, sent->operation);
    EXPECT_EQ(failed->completions[0].reached_stage,
              DeliveryStage::kLocalPublished);
    EXPECT_FALSE(failed->completions[0].status.ok());
}

TEST(TcpDriverTest, IncrementalReaderAcceptsOneBytePrefixAndBodyFragments) {
    auto server_result = TcpDriver::Create(TestOptions());
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<TcpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 2});
    ASSERT_TRUE(listener.ok());
    ScopedFd peer = ConnectRaw(endpoint);
    auto accepted = server->Accept(
        {.listener_id = listener->id, .timeout_ms = 1000});
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();

    const std::vector<std::byte> body = FrameBody(64, 2);
    const std::vector<std::byte> wire = Prefix(body);
    for (std::byte value : wire) {
        ASSERT_EQ(::send(peer.get(), &value, 1, 0), 1);
    }

    auto received = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].payload, body);
}

TEST(TcpDriverTest, OversizedPrefixClosesBeforeAllocatingBody) {
    TcpDriverOptions options = TestOptions();
    options.idle_timeout_ms = 1000;
    auto server_result = TcpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<TcpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 2});
    ASSERT_TRUE(listener.ok());
    ScopedFd peer = ConnectRaw(endpoint);
    auto accepted = server->Accept(
        {.listener_id = listener->id, .timeout_ms = 1000});
    ASSERT_TRUE(accepted.ok());

    const std::array<std::byte, 4> bad_prefix = {
        std::byte{0x7f}, std::byte{0xff}, std::byte{0xff}, std::byte{0xff}};
    ASSERT_EQ(::send(peer.get(), bad_prefix.data(), bad_prefix.size(), 0),
              static_cast<ssize_t>(bad_prefix.size()));
    EXPECT_TRUE(WaitForConnectionCount(*server, 0));
    EXPECT_EQ(server->stats().ready_receive_bytes, 0u);
}

TEST(TcpDriverTest, CanonicalHeartbeatsKeepIdleConnectionsAliveAndStayInternal) {
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 10;
    options.idle_timeout_ms = 120;
    DriverPair pair = ConnectPair(options);
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    std::this_thread::sleep_for(350ms);
    EXPECT_EQ(pair.server->stats().active_connections, 1u);
    EXPECT_EQ(pair.client->stats().active_connections, 1u);
    auto heartbeat_hidden = pair.server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 20});
    ASSERT_FALSE(heartbeat_hidden.ok());
    EXPECT_EQ(heartbeat_hidden.status().code(), StatusCode::kTimeout);
}

TEST(TcpDriverTest, SilentPeerTriggersIdleTimeout) {
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 10;
    options.idle_timeout_ms = 80;
    auto server_result = TcpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<TcpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 2});
    ASSERT_TRUE(listener.ok());
    ScopedFd peer = ConnectRaw(endpoint);
    auto accepted = server->Accept(
        {.listener_id = listener->id, .timeout_ms = 1000});
    ASSERT_TRUE(accepted.ok());

    EXPECT_TRUE(WaitForConnectionCount(*server, 0, 1000ms));
}

TEST(TcpDriverTest, ShutdownWakesLongBlockingPoll) {
    auto driver_result = TcpDriver::Create(TestOptions());
    ASSERT_TRUE(driver_result.ok());
    std::unique_ptr<TcpDriver> driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());

    std::atomic<bool> entered{false};
    std::atomic<StatusCode> result{StatusCode::kOk};
    std::thread poller([&] {
        entered.store(true, std::memory_order_release);
        auto polled = driver->Poll(
            {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 60'000});
        result.store(polled.ok() ? StatusCode::kOk : polled.status().code(),
                     std::memory_order_release);
    });
    while (!entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(10ms);
    const auto started = std::chrono::steady_clock::now();
    EXPECT_TRUE(driver->Shutdown().ok());
    const auto elapsed = std::chrono::steady_clock::now() - started;
    poller.join();
    EXPECT_LT(elapsed, 1s);
    EXPECT_EQ(result.load(std::memory_order_acquire), StatusCode::kUnavailable);
}

}  // namespace
}  // namespace mino::transport
