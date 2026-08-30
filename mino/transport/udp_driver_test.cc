// Copyright 2026 The Mino Authors

#include "mino/transport/udp_driver.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
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
#include <optional>
#include <span>
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

sockaddr_in UdpLoopbackAddress(uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    return address;
}

void StoreBe16(uint16_t value, std::span<std::byte> output) {
    output[0] = static_cast<std::byte>(value >> 8);
    output[1] = static_cast<std::byte>(value);
}

void StoreBe32(uint32_t value, std::span<std::byte> output) {
    output[0] = static_cast<std::byte>(value >> 24);
    output[1] = static_cast<std::byte>(value >> 16);
    output[2] = static_cast<std::byte>(value >> 8);
    output[3] = static_cast<std::byte>(value);
}

void StoreBe64(uint64_t value, std::span<std::byte> output) {
    StoreBe32(static_cast<uint32_t>(value >> 32), output.first<4>());
    StoreBe32(static_cast<uint32_t>(value), output.subspan<4, 4>());
}

uint32_t TestCrc32(std::span<const std::byte> input) {
    uint32_t crc = 0xffffffffu;
    for (const std::byte value : input) {
        crc ^= std::to_integer<uint8_t>(value);
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask = 0u - (crc & 1u);
            crc = (crc >> 1) ^ (0xedb88320u & mask);
        }
    }
    return ~crc;
}

std::vector<std::vector<std::byte>> FragmentMessage(
    std::span<const std::byte> payload, uint32_t max_datagram_bytes,
    uint64_t message_id, std::optional<uint32_t> crc_override = std::nullopt) {
    const size_t capacity = max_datagram_bytes - kUdpFragmentHeaderBytes;
    const uint32_t count =
        static_cast<uint32_t>((payload.size() + capacity - 1) / capacity);
    const uint32_t crc = crc_override.value_or(TestCrc32(payload));
    std::vector<std::vector<std::byte>> fragments;
    fragments.reserve(count);
    for (uint32_t fragment_id = 0; fragment_id < count; ++fragment_id) {
        const size_t offset = static_cast<size_t>(fragment_id) * capacity;
        const size_t bytes = std::min(capacity, payload.size() - offset);
        std::vector<std::byte> fragment(kUdpFragmentHeaderBytes + bytes);
        StoreBe32(kUdpFragmentMagic,
                  std::span<std::byte>(fragment).subspan<0, 4>());
        fragment[4] = static_cast<std::byte>(kUdpFragmentVersion);
        fragment[5] = static_cast<std::byte>(kUdpFragmentFlag);
        StoreBe16(kUdpFragmentHeaderBytes,
                  std::span<std::byte>(fragment).subspan<6, 2>());
        StoreBe64(message_id,
                  std::span<std::byte>(fragment).subspan<8, 8>());
        StoreBe32(fragment_id,
                  std::span<std::byte>(fragment).subspan<16, 4>());
        StoreBe32(count, std::span<std::byte>(fragment).subspan<20, 4>());
        StoreBe32(static_cast<uint32_t>(payload.size()),
                  std::span<std::byte>(fragment).subspan<24, 4>());
        StoreBe32(static_cast<uint32_t>(offset),
                  std::span<std::byte>(fragment).subspan<28, 4>());
        StoreBe32(crc, std::span<std::byte>(fragment).subspan<32, 4>());
        std::copy(payload.begin() + offset, payload.begin() + offset + bytes,
                  fragment.begin() + kUdpFragmentHeaderBytes);
        fragments.push_back(std::move(fragment));
    }
    return fragments;
}

void SendDatagram(int fd, const sockaddr_in& address,
                  std::span<const std::byte> datagram) {
    ASSERT_EQ(::sendto(fd, datagram.data(), datagram.size(), 0,
                       reinterpret_cast<const sockaddr*>(&address),
                       sizeof(address)),
              static_cast<ssize_t>(datagram.size()));
}

UdpDriverOptions FragmentTestOptions() {
    UdpDriverOptions options;
    options.max_datagram_bytes = 256;
    options.max_message_bytes = 4096;
    options.max_fragments_per_message = 32;
    options.max_reassembly_bytes_per_connection = 8192;
    options.max_reassembly_messages_per_connection = 4;
    options.max_reassembly_bytes_global = 16 * 1024;
    options.max_reassembly_messages_global = 8;
    options.reassembly_timeout_ms = 100;
    options.io_poll_max_ms = 5;
    return options;
}

class EmptyDatagramFlood final {
public:
    EmptyDatagramFlood(int fd, sockaddr_in address)
        : thread_([this, fd, address] {
              while (running_.load(std::memory_order_acquire)) {
                  const ssize_t sent = ::sendto(
                      fd, nullptr, 0, 0,
                      reinterpret_cast<const sockaddr*>(&address),
                      sizeof(address));
                  if (sent == 0) {
                      successful_sends_.fetch_add(1, std::memory_order_release);
                      std::this_thread::yield();
                      continue;
                  }
                  if (errno == EINTR) continue;
                  if (errno == EAGAIN || errno == EWOULDBLOCK) {
                      std::this_thread::yield();
                      continue;
                  }
                  failed_.store(true, std::memory_order_release);
                  break;
              }
          }) {}

    EmptyDatagramFlood(const EmptyDatagramFlood&) = delete;
    EmptyDatagramFlood& operator=(const EmptyDatagramFlood&) = delete;

    ~EmptyDatagramFlood() { Stop(); }

    bool WaitUntilStarted() const {
        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (successful_sends_.load(std::memory_order_acquire) == 0 &&
               !failed_.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        return successful_sends_.load(std::memory_order_acquire) != 0;
    }

    void Stop() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
    }

private:
    std::atomic<bool> running_{true};
    std::atomic<bool> failed_{false};
    std::atomic<size_t> successful_sends_{0};
    std::thread thread_;
};

DriverConfig Config() {
    return DriverConfig{
        .max_connections = 16,
        .max_listeners = 4,
        .max_queued_sends = 32,
    };
}

TEST(UdpDriverOptionsTest, ValidatesDatagramFragmentAndQuotaBounds) {
    EXPECT_TRUE(ValidateUdpDriverOptions({}).ok());
    UdpDriverOptions options;
    options.max_datagram_bytes = kUdpMaximumDatagramBytes + 1;
    EXPECT_EQ(ValidateUdpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
    options = {};
    options.socket_receive_buffer_bytes = options.max_datagram_bytes - 1;
    EXPECT_EQ(ValidateUdpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
    options = FragmentTestOptions();
    options.max_fragments_per_message = 2;
    EXPECT_EQ(ValidateUdpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
    options = FragmentTestOptions();
    options.max_reassembly_bytes_per_connection =
        options.max_message_bytes - 1;
    EXPECT_EQ(ValidateUdpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
    options = FragmentTestOptions();
    options.max_reassembly_messages_global =
        options.max_reassembly_messages_per_connection - 1;
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
    if (received->messages.size() < 2u) {
        auto remaining = server->Poll(
            {.max_messages = static_cast<uint32_t>(
                 2u - received->messages.size()),
             .max_bytes = 1200,
             .timeout_ms = 1000});
        ASSERT_TRUE(remaining.ok()) << remaining.status().ToString();
        for (auto& message : remaining->messages) {
            received->messages.push_back(std::move(message));
        }
    }
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

TEST(UdpDriverTest, FragmentsLargeLoopbackMessageAndAdvertisesMessageLimit) {
    const UdpDriverOptions options = FragmentTestOptions();
    auto server_result = UdpDriver::Create(options);
    auto client_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    ASSERT_TRUE(client_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    std::unique_ptr<UdpDriver> client = std::move(*client_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    ASSERT_TRUE(client->Start(Config()).ok());
    EXPECT_EQ(client->capabilities().max_frame_size,
              options.max_message_bytes);
    EXPECT_EQ(client->capabilities().max_reassembly_bytes,
              options.max_message_bytes);

    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());
    auto connected = client->Connect({
        .remote_endpoint = endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 0,
    });
    ASSERT_TRUE(connected.ok());

    std::vector<std::byte> payload(2000);
    for (size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::byte>(index);
    }
    auto sent = client->Send({
        .connection_id = connected->id,
        .payload = payload,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();
    auto received = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].payload, payload);
    EXPECT_EQ(server->stats().reassembly_bytes, 0u);
    EXPECT_EQ(server->stats().reassembly_messages, 0u);
    EXPECT_EQ(server->stats().reassembled_messages, 1u);
    EXPECT_EQ(client->stats().fragmented_messages_sent, 1u);
    EXPECT_EQ(client->stats().fragments_sent,
              FragmentMessage(payload, options.max_datagram_bytes, 1).size());
}

TEST(UdpDriverTest, ReassemblesOutOfOrderAndIgnoresIdenticalDuplicate) {
    const UdpDriverOptions options = FragmentTestOptions();
    auto server_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const sockaddr_in address = UdpLoopbackAddress(endpoint.port());
    std::vector<std::byte> payload(700);
    for (size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::byte>(index * 7);
    }
    const auto fragments =
        FragmentMessage(payload, options.max_datagram_bytes, 77);
    ASSERT_EQ(fragments.size(), 4u);
    for (const size_t index : {2u, 0u, 2u, 3u, 1u}) {
        SendDatagram(peer.get(), address, fragments[index]);
    }

    auto received = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].payload, payload);
    EXPECT_EQ(server->stats().duplicate_fragments, 1u);
    EXPECT_EQ(server->stats().fragments_received, fragments.size());
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

TEST(UdpDriverTest, RejectsOversizedMessageAndListenerSend) {
    UdpDriverOptions options = FragmentTestOptions();
    options.max_message_bytes = 512;
    options.max_fragments_per_message = 4;
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

    const std::vector<std::byte> oversized(513, std::byte{1});
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

TEST(UdpDriverTest, ReassemblyTimeoutReleasesReservedQuota) {
    UdpDriverOptions options = FragmentTestOptions();
    options.reassembly_timeout_ms = 20;
    auto server_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const sockaddr_in address = UdpLoopbackAddress(endpoint.port());
    const std::vector<std::byte> payload(700, std::byte{0x44});
    const auto fragments =
        FragmentMessage(payload, options.max_datagram_bytes, 91);
    SendDatagram(peer.get(), address, fragments[0]);

    auto polled = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 80});
    ASSERT_FALSE(polled.ok());
    EXPECT_EQ(polled.status().code(), StatusCode::kTimeout);
    const UdpDriverStats stats = server->stats();
    EXPECT_EQ(stats.reassembly_bytes, 0u);
    EXPECT_EQ(stats.reassembly_messages, 0u);
    EXPECT_EQ(stats.reassembly_timeouts, 1u);
}

TEST(UdpDriverTest, EnforcesPerConnectionReassemblyQuotaAndCloseCleansIt) {
    UdpDriverOptions options = FragmentTestOptions();
    options.max_message_bytes = 1024;
    options.max_fragments_per_message = 8;
    options.max_reassembly_bytes_per_connection = 1024;
    options.max_reassembly_messages_per_connection = 1;
    options.max_reassembly_bytes_global = 2048;
    options.max_reassembly_messages_global = 2;
    auto server_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const sockaddr_in address = UdpLoopbackAddress(endpoint.port());
    const std::vector<std::byte> payload(700, std::byte{0x51});
    const auto first = FragmentMessage(payload, options.max_datagram_bytes, 101);
    const auto second = FragmentMessage(payload, options.max_datagram_bytes, 102);
    SendDatagram(peer.get(), address, first[0]);
    SendDatagram(peer.get(), address, second[0]);

    auto rejected = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(server->stats().reassembly_bytes, payload.size());
    EXPECT_EQ(server->stats().reassembly_messages, 1u);
    EXPECT_EQ(server->stats().rejected_fragments, 1u);
    EXPECT_TRUE(server->Close(listener->id).ok());
    EXPECT_EQ(server->stats().reassembly_bytes, 0u);
    EXPECT_EQ(server->stats().reassembly_messages, 0u);
}

TEST(UdpDriverTest, EnforcesGlobalReassemblyMessageQuotaAcrossListeners) {
    UdpDriverOptions options = FragmentTestOptions();
    options.max_reassembly_messages_per_connection = 1;
    options.max_reassembly_messages_global = 1;
    auto server_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor first_endpoint =
        UdpLoopback(FindUnusedUdpPort());
    const EndpointDescriptor second_endpoint =
        UdpLoopback(FindUnusedUdpPort());
    auto first_listener = server->Listen(
        {.local_endpoint = first_endpoint, .backlog = 1});
    auto second_listener = server->Listen(
        {.local_endpoint = second_endpoint, .backlog = 1});
    ASSERT_TRUE(first_listener.ok());
    ASSERT_TRUE(second_listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const std::vector<std::byte> payload(700, std::byte{0x52});
    const auto first = FragmentMessage(payload, options.max_datagram_bytes, 201);
    const auto second = FragmentMessage(payload, options.max_datagram_bytes, 202);
    SendDatagram(peer.get(), UdpLoopbackAddress(first_endpoint.port()), first[0]);
    SendDatagram(peer.get(), UdpLoopbackAddress(second_endpoint.port()), second[0]);

    auto rejected = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(server->stats().reassembly_messages, 1u);
    EXPECT_EQ(server->stats().rejected_fragments, 1u);
}

TEST(UdpDriverTest, EnforcesGlobalReassemblyByteQuotaAcrossListeners) {
    UdpDriverOptions options = FragmentTestOptions();
    options.max_reassembly_bytes_per_connection = 4096;
    options.max_reassembly_messages_per_connection = 2;
    options.max_reassembly_bytes_global = 4096;
    options.max_reassembly_messages_global = 2;
    auto server_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor first_endpoint =
        UdpLoopback(FindUnusedUdpPort());
    const EndpointDescriptor second_endpoint =
        UdpLoopback(FindUnusedUdpPort());
    auto first_listener = server->Listen(
        {.local_endpoint = first_endpoint, .backlog = 1});
    auto second_listener = server->Listen(
        {.local_endpoint = second_endpoint, .backlog = 1});
    ASSERT_TRUE(first_listener.ok());
    ASSERT_TRUE(second_listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const std::vector<std::byte> payload(3000, std::byte{0x53});
    const auto first = FragmentMessage(payload, options.max_datagram_bytes, 211);
    const auto second = FragmentMessage(payload, options.max_datagram_bytes, 212);
    SendDatagram(peer.get(), UdpLoopbackAddress(first_endpoint.port()), first[0]);
    SendDatagram(peer.get(), UdpLoopbackAddress(second_endpoint.port()), second[0]);

    auto rejected = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(server->stats().reassembly_bytes, payload.size());
    EXPECT_EQ(server->stats().reassembly_messages, 1u);
}

TEST(UdpDriverTest, ShutdownClearsPartialReassemblies) {
    UdpDriverOptions options = FragmentTestOptions();
    options.reassembly_timeout_ms = 5000;
    auto server_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());
    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const std::vector<std::byte> payload(700, std::byte{0x54});
    const auto fragments =
        FragmentMessage(payload, options.max_datagram_bytes, 220);
    SendDatagram(peer.get(), UdpLoopbackAddress(endpoint.port()), fragments[0]);
    const auto receive_deadline = std::chrono::steady_clock::now() + 1s;
    while (server->stats().reassembly_messages == 0u &&
           std::chrono::steady_clock::now() < receive_deadline) {
        auto received = server->Poll(
            {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 0});
        ASSERT_FALSE(received.ok());
        ASSERT_EQ(received.status().code(), StatusCode::kWouldBlock);
        std::this_thread::yield();
    }
    ASSERT_EQ(server->stats().reassembly_messages, 1u);
    EXPECT_TRUE(server->Shutdown().ok());
    EXPECT_EQ(server->stats().reassembly_messages, 0u);
    EXPECT_EQ(server->stats().reassembly_bytes, 0u);
}

TEST(UdpDriverTest, RejectsMaliciousHeadersConflictingDuplicatesAndBadCrc) {
    const UdpDriverOptions options = FragmentTestOptions();
    auto server_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());
    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const sockaddr_in address = UdpLoopbackAddress(endpoint.port());
    const std::vector<std::byte> payload(700, std::byte{0x63});

    std::vector<std::byte> truncated(8);
    StoreBe32(kUdpFragmentMagic,
              std::span<std::byte>(truncated).first<4>());
    SendDatagram(peer.get(), address, truncated);
    auto truncated_result = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(truncated_result.ok());
    EXPECT_EQ(truncated_result.status().code(), StatusCode::kCorruption);

    auto bad_version =
        FragmentMessage(payload, options.max_datagram_bytes, 301)[0];
    bad_version[4] = std::byte{99};
    SendDatagram(peer.get(), address, bad_version);
    auto version_result = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(version_result.ok());
    EXPECT_EQ(version_result.status().code(), StatusCode::kCorruption);

    auto bad_count =
        FragmentMessage(payload, options.max_datagram_bytes, 302)[0];
    StoreBe32(options.max_fragments_per_message + 1,
              std::span<std::byte>(bad_count).subspan<20, 4>());
    SendDatagram(peer.get(), address, bad_count);
    auto count_result = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(count_result.ok());
    EXPECT_EQ(count_result.status().code(), StatusCode::kCorruption);

    auto bad_offset =
        FragmentMessage(payload, options.max_datagram_bytes, 303)[0];
    StoreBe32(1, std::span<std::byte>(bad_offset).subspan<28, 4>());
    SendDatagram(peer.get(), address, bad_offset);
    auto offset_result = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(offset_result.ok());
    EXPECT_EQ(offset_result.status().code(), StatusCode::kCorruption);

    auto conflicting =
        FragmentMessage(payload, options.max_datagram_bytes, 304);
    SendDatagram(peer.get(), address, conflicting[0]);
    conflicting[0].back() ^= std::byte{1};
    SendDatagram(peer.get(), address, conflicting[0]);
    auto conflict_result = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(conflict_result.ok());
    EXPECT_EQ(conflict_result.status().code(), StatusCode::kCorruption);

    const auto bad_crc = FragmentMessage(payload, options.max_datagram_bytes,
                                         305, TestCrc32(payload) ^ 1u);
    for (const auto& fragment : bad_crc) {
        SendDatagram(peer.get(), address, fragment);
    }
    auto crc_result = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_FALSE(crc_result.ok());
    EXPECT_EQ(crc_result.status().code(), StatusCode::kCorruption);
    EXPECT_EQ(server->stats().reassembly_messages, 0u);
    EXPECT_GE(server->stats().rejected_fragments, 6u);
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
    const sockaddr_in address = UdpLoopbackAddress(endpoint.port());
    ASSERT_EQ(::sendto(peer.get(), nullptr, 0, 0,
                       reinterpret_cast<const sockaddr*>(&address),
                       sizeof(address)),
              0);
    auto polled = server->Poll(
        {.max_messages = 1, .max_bytes = 1200, .timeout_ms = 30});
    ASSERT_FALSE(polled.ok());
    EXPECT_EQ(polled.status().code(), StatusCode::kTimeout);
}

TEST(UdpDriverTest, ContinuousEmptyDatagramsRespectReceiveDeadline) {
    auto server_result = UdpDriver::Create();
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const int flags = ::fcntl(peer.get(), F_GETFL, 0);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(peer.get(), F_SETFL, flags | O_NONBLOCK), 0);
    EmptyDatagramFlood flood(peer.get(),
                             UdpLoopbackAddress(endpoint.port()));
    ASSERT_TRUE(flood.WaitUntilStarted());

    const auto started = std::chrono::steady_clock::now();
    auto polled = server->Poll(
        {.max_messages = 4, .max_bytes = 1200, .timeout_ms = 50});
    const auto elapsed = std::chrono::steady_clock::now() - started;
    flood.Stop();

    ASSERT_FALSE(polled.ok());
    EXPECT_EQ(polled.status().code(), StatusCode::kTimeout);
    EXPECT_LT(elapsed, 1s);
}

TEST(UdpDriverTest, EmptyDatagramFloodDoesNotStarveOtherReadySocket) {
    for (const bool flood_second_listener : {false, true}) {
        SCOPED_TRACE(flood_second_listener);
        auto server_result = UdpDriver::Create();
        ASSERT_TRUE(server_result.ok());
        std::unique_ptr<UdpDriver> server = std::move(*server_result);
        ASSERT_TRUE(server->Start(Config()).ok());

        const EndpointDescriptor first_endpoint =
            UdpLoopback(FindUnusedUdpPort());
        auto first_listener = server->Listen(
            {.local_endpoint = first_endpoint, .backlog = 1});
        ASSERT_TRUE(first_listener.ok());
        const EndpointDescriptor second_endpoint =
            UdpLoopback(FindUnusedUdpPort());
        auto second_listener = server->Listen(
            {.local_endpoint = second_endpoint, .backlog = 1});
        ASSERT_TRUE(second_listener.ok());

        const EndpointDescriptor& flood_endpoint =
            flood_second_listener ? second_endpoint : first_endpoint;
        const ConnectionInfo& normal_listener =
            flood_second_listener ? *first_listener : *second_listener;
        const EndpointDescriptor& normal_endpoint =
            flood_second_listener ? first_endpoint : second_endpoint;

        ScopedFd flood_peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
        ASSERT_GE(flood_peer.get(), 0);
        const sockaddr_in flood_address =
            UdpLoopbackAddress(flood_endpoint.port());
        for (size_t index = 0; index < 256; ++index) {
            ASSERT_EQ(::sendto(
                          flood_peer.get(), nullptr, 0, 0,
                          reinterpret_cast<const sockaddr*>(&flood_address),
                          sizeof(flood_address)),
                      0);
        }
        const int flags = ::fcntl(flood_peer.get(), F_GETFL, 0);
        ASSERT_GE(flags, 0);
        ASSERT_EQ(::fcntl(flood_peer.get(), F_SETFL, flags | O_NONBLOCK), 0);
        EmptyDatagramFlood flood(flood_peer.get(), flood_address);
        ASSERT_TRUE(flood.WaitUntilStarted());

        ScopedFd normal_peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
        ASSERT_GE(normal_peer.get(), 0);
        const sockaddr_in normal_address =
            UdpLoopbackAddress(normal_endpoint.port());
        const std::vector<std::byte> payload(37, std::byte{0x5a});
        ASSERT_EQ(::sendto(
                      normal_peer.get(), payload.data(), payload.size(), 0,
                      reinterpret_cast<const sockaddr*>(&normal_address),
                      sizeof(normal_address)),
                  static_cast<ssize_t>(payload.size()));

        auto received = server->Poll(
            {.max_messages = 1, .max_bytes = 1200, .timeout_ms = 500});
        flood.Stop();

        ASSERT_TRUE(received.ok()) << received.status().ToString();
        ASSERT_EQ(received->messages.size(), 1u);
        EXPECT_EQ(received->messages[0].connection_id, normal_listener.id);
        EXPECT_EQ(received->messages[0].payload, payload);
    }
}

TEST(UdpDriverTest, NonBlockingPollPersistsRoundRobinAcrossCalls) {
    for (const bool ordinary_flood : {false, true}) {
        for (const bool flood_second_listener : {false, true}) {
            SCOPED_TRACE(ordinary_flood);
            SCOPED_TRACE(flood_second_listener);
            auto server_result = UdpDriver::Create();
            ASSERT_TRUE(server_result.ok());
            std::unique_ptr<UdpDriver> server = std::move(*server_result);
            ASSERT_TRUE(server->Start(Config()).ok());

            const EndpointDescriptor first_endpoint =
                UdpLoopback(FindUnusedUdpPort());
            auto first_listener = server->Listen(
                {.local_endpoint = first_endpoint, .backlog = 1});
            ASSERT_TRUE(first_listener.ok());
            const EndpointDescriptor second_endpoint =
                UdpLoopback(FindUnusedUdpPort());
            auto second_listener = server->Listen(
                {.local_endpoint = second_endpoint, .backlog = 1});
            ASSERT_TRUE(second_listener.ok());

            const EndpointDescriptor& flood_endpoint =
                flood_second_listener ? second_endpoint : first_endpoint;
            const ConnectionInfo& flood_listener =
                flood_second_listener ? *second_listener : *first_listener;
            const EndpointDescriptor& target_endpoint =
                flood_second_listener ? first_endpoint : second_endpoint;
            const ConnectionInfo& target_listener =
                flood_second_listener ? *first_listener : *second_listener;

            ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
            ASSERT_GE(peer.get(), 0);
            const sockaddr_in flood_address =
                UdpLoopbackAddress(flood_endpoint.port());
            const std::vector<std::byte> flood_payload(13, std::byte{0x61});
            const void* flood_data =
                ordinary_flood ? flood_payload.data() : nullptr;
            const size_t flood_size =
                ordinary_flood ? flood_payload.size() : 0;
            const sockaddr_in target_address =
                UdpLoopbackAddress(target_endpoint.port());
            const std::vector<std::byte> target_payload(29, std::byte{0x62});
            ASSERT_EQ(::sendto(
                          peer.get(), target_payload.data(), target_payload.size(),
                          0,
                          reinterpret_cast<const sockaddr*>(&target_address),
                          sizeof(target_address)),
                      static_cast<ssize_t>(target_payload.size()));
            for (size_t index = 0; index < 32; ++index) {
                ASSERT_EQ(::sendto(
                              peer.get(), flood_data, flood_size, 0,
                              reinterpret_cast<const sockaddr*>(&flood_address),
                              sizeof(flood_address)),
                          static_cast<ssize_t>(flood_size));
            }

            bool consumed_target = false;
            size_t consumed_messages = 0;
            const auto deadline = std::chrono::steady_clock::now() + 500ms;
            while (consumed_messages < 2 &&
                   std::chrono::steady_clock::now() < deadline) {
                auto received = server->Poll(
                    {.max_messages = 1, .max_bytes = 1200, .timeout_ms = 0});
                if (!received.ok()) {
                    ASSERT_EQ(received.status().code(), StatusCode::kWouldBlock);
                    std::this_thread::yield();
                    continue;
                }
                ++consumed_messages;
                ASSERT_EQ(received->messages.size(), 1u);
                const ReceivedMessage& message = received->messages[0];
                if (message.connection_id == target_listener.id) {
                    EXPECT_EQ(message.payload, target_payload);
                    consumed_target = true;
                    break;
                }
                EXPECT_EQ(message.connection_id, flood_listener.id);
                ASSERT_TRUE(ordinary_flood);
                EXPECT_EQ(message.payload, flood_payload);
            }
            EXPECT_TRUE(consumed_target);
        }
    }
}

TEST(UdpDriverTest, ShutdownWakesReceiveDuringEmptyDatagramFlood) {
    auto server_result = UdpDriver::Create();
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const int flags = ::fcntl(peer.get(), F_GETFL, 0);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(peer.get(), F_SETFL, flags | O_NONBLOCK), 0);
    EmptyDatagramFlood flood(peer.get(),
                             UdpLoopbackAddress(endpoint.port()));
    ASSERT_TRUE(flood.WaitUntilStarted());

    std::atomic<StatusCode> result{StatusCode::kOk};
    std::thread receiver([&] {
        auto polled = server->Poll(
            {.max_messages = 4, .max_bytes = 1200, .timeout_ms = 60'000});
        result.store(polled.ok() ? StatusCode::kOk : polled.status().code(),
                     std::memory_order_release);
    });
    std::this_thread::sleep_for(10ms);
    const auto started = std::chrono::steady_clock::now();
    EXPECT_TRUE(server->Shutdown().ok());
    receiver.join();
    const auto elapsed = std::chrono::steady_clock::now() - started;
    flood.Stop();

    EXPECT_LT(elapsed, 1s);
    EXPECT_EQ(result.load(std::memory_order_acquire), StatusCode::kUnavailable);
}

TEST(UdpDriverTest, ReturnsMessagesBeforeFollowingOversizedDatagramError) {
    UdpDriverOptions options;
    options.max_datagram_bytes = 256;
    auto server_result = UdpDriver::Create(options);
    ASSERT_TRUE(server_result.ok());
    std::unique_ptr<UdpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(Config()).ok());
    const EndpointDescriptor endpoint = UdpLoopback(FindUnusedUdpPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 1});
    ASSERT_TRUE(listener.ok());

    ScopedFd peer(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
    ASSERT_GE(peer.get(), 0);
    const sockaddr_in address = UdpLoopbackAddress(endpoint.port());
    const std::vector<std::byte> normal(31, std::byte{0x41});
    const std::vector<std::byte> oversized(257, std::byte{0x42});
    ASSERT_EQ(::sendto(peer.get(), normal.data(), normal.size(), 0,
                       reinterpret_cast<const sockaddr*>(&address),
                       sizeof(address)),
              static_cast<ssize_t>(normal.size()));
    ASSERT_EQ(::sendto(peer.get(), oversized.data(), oversized.size(), 0,
                       reinterpret_cast<const sockaddr*>(&address),
                       sizeof(address)),
              static_cast<ssize_t>(oversized.size()));

    auto received = server->Poll(
        {.max_messages = 2, .max_bytes = 1200, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].payload, normal);

    auto rejected = server->Poll(
        {.max_messages = 1, .max_bytes = 1200, .timeout_ms = 0});
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(server->Poll({.max_messages = 1,
                            .max_bytes = 1200,
                            .timeout_ms = 0})
                  .status()
                  .code(),
              StatusCode::kWouldBlock);
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
