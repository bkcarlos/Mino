// Copyright 2026 The Mino Authors

#include "mino/transport/ipsec_transport.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/security/ipsec.h"
#include "mino/transport/tcp_driver.h"

namespace mino::transport {
namespace {

using namespace std::chrono_literals;

TcpDriverOptions TestTcpOptions() {
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
        .max_receive_frames_per_turn = 64,
        .max_receive_bytes_per_turn = 256 * 1024,
        .tls_factory = {},
    };
}

DriverConfig TestConfig() {
    return DriverConfig{
        .max_connections = 16,
        .max_listeners = 4,
        .max_queued_sends = 32,
    };
}


class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
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

std::vector<std::byte> FrameBody(size_t payload_size = 64) {
    bridge::WireFrame frame;
    frame.header.topic_id = 1;
    frame.header.msg_type = 2;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.sequence_num = 1;
    frame.payload.resize(payload_size, std::byte{0xab});
    auto encoded = bridge::WireFrameCodec::Encode(frame);
    EXPECT_TRUE(encoded.ok()) << encoded.status().ToString();
    return encoded.ok() ? std::move(*encoded) : std::vector<std::byte>{};
}

TEST(IpsecTransportTest, CreateRequiresInnerAndProbe) {
    IpsecTransportOptions options;
    auto missing = IpsecTransportDriver::Create(std::move(options));
    EXPECT_EQ(missing.status().code(), StatusCode::kInvalidArgument);

    auto tcp = TcpDriver::Create(TestTcpOptions());
    ASSERT_TRUE(tcp.ok()) << tcp.status().ToString();
    options.inner = std::move(*tcp);
    options.enforcement = IpsecEnforcement::kRequireKernelSa;
    auto missing_probe = IpsecTransportDriver::Create(std::move(options));
    EXPECT_EQ(missing_probe.status().code(), StatusCode::kInvalidArgument);
}

TEST(IpsecTransportTest, ConnectFailClosedWithoutSa) {
    auto tcp = TcpDriver::Create(TestTcpOptions());
    ASSERT_TRUE(tcp.ok()) << tcp.status().ToString();
    auto probe = std::make_shared<security::ScriptedIpsecSaProbe>();
    // default deny

    IpsecTransportOptions options;
    options.inner = std::move(*tcp);
    options.probe = probe;
    options.enforcement = IpsecEnforcement::kRequireKernelSa;
    auto driver = IpsecTransportDriver::Create(std::move(options));
    ASSERT_TRUE(driver.ok()) << driver.status().ToString();
    ASSERT_TRUE((*driver)->Start(TestConfig()).ok());

    ConnectRequest request;
    request.remote_endpoint = Loopback(FindUnusedLoopbackPort());
    request.timeout_ms = 50;
    auto connected = (*driver)->Connect(request);
    EXPECT_FALSE(connected.ok());
    EXPECT_EQ(connected.status().code(), StatusCode::kUnavailable);
    EXPECT_TRUE((*driver)->Shutdown().ok());
}

TEST(IpsecTransportTest, ConnectAndExchangeWhenProbeAllows) {
    auto server_tcp = TcpDriver::Create(TestTcpOptions());
    auto client_tcp = TcpDriver::Create(TestTcpOptions());
    ASSERT_TRUE(server_tcp.ok()) << server_tcp.status().ToString();
    ASSERT_TRUE(client_tcp.ok()) << client_tcp.status().ToString();

    auto allow = std::make_shared<security::ScriptedIpsecSaProbe>();
    allow->AllowAll();

    IpsecTransportOptions server_opts;
    server_opts.inner = std::move(*server_tcp);
    server_opts.probe = allow;
    auto server = IpsecTransportDriver::Create(std::move(server_opts));
    ASSERT_TRUE(server.ok()) << server.status().ToString();

    IpsecTransportOptions client_opts;
    client_opts.inner = std::move(*client_tcp);
    client_opts.probe = allow;
    auto client = IpsecTransportDriver::Create(std::move(client_opts));
    ASSERT_TRUE(client.ok()) << client.status().ToString();

    ASSERT_TRUE((*server)->Start(TestConfig()).ok());
    ASSERT_TRUE((*client)->Start(TestConfig()).ok());

    const uint16_t port = FindUnusedLoopbackPort();
    ListenRequest listen;
    listen.local_endpoint = Loopback(port);
    listen.backlog = 8;
    auto listener = (*server)->Listen(listen);
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();

    ConnectRequest connect;
    connect.remote_endpoint = Loopback(port);
    connect.timeout_ms = 1000;
    auto connected = (*client)->Connect(connect);
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();

    AcceptRequest accept;
    accept.listener_id = listener->id;
    accept.timeout_ms = 1000;
    auto accepted = (*server)->Accept(accept);
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();

    const auto body = FrameBody();
    SendRequest send;
    send.connection_id = connected->id;
    send.payload = body;
    send.target_stage = DeliveryStage::kRemoteAccepted;
    auto sent = (*client)->Send(send);
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();

    ReceiveRequest receive;
    receive.connection_id = accepted->id;
    receive.max_messages = 1;
    receive.max_bytes = 1u << 20;
    receive.timeout_ms = 1000;
    Result<ReceiveResult> polled = Status::Error(StatusCode::kWouldBlock);
    for (int i = 0; i < 50; ++i) {
        polled = (*server)->Poll(receive);
        if (polled.ok()) break;
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_TRUE(polled.ok()) << polled.status().ToString();
    ASSERT_EQ(polled->messages.size(), 1u);
    EXPECT_EQ(polled->messages[0].payload, body);

    EXPECT_TRUE((*client)->Shutdown().ok());
    EXPECT_TRUE((*server)->Shutdown().ok());
}

TEST(IpsecTransportTest, AssumedProtectedWithoutProbeAdmits) {
    auto tcp = TcpDriver::Create(TestTcpOptions());
    ASSERT_TRUE(tcp.ok()) << tcp.status().ToString();
    IpsecTransportOptions options;
    options.inner = std::move(*tcp);
    options.enforcement = IpsecEnforcement::kAssumedProtected;
    options.probe = nullptr;
    auto driver = IpsecTransportDriver::Create(std::move(options));
    ASSERT_TRUE(driver.ok()) << driver.status().ToString();
    EXPECT_EQ((*driver)->enforcement(), IpsecEnforcement::kAssumedProtected);
    ASSERT_TRUE((*driver)->Start(TestConfig()).ok());

    // Listening does not require SA in assumed mode without probe.
    ListenRequest listen;
    listen.local_endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = (*driver)->Listen(listen);
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    EXPECT_TRUE((*driver)->Shutdown().ok());
}

TEST(IpsecTransportTest, NetlinkProbeFailClosedOnConnect) {
    auto tcp = TcpDriver::Create(TestTcpOptions());
    ASSERT_TRUE(tcp.ok()) << tcp.status().ToString();
    auto probe = security::NetlinkXfrmSaProbe::Create();
    ASSERT_TRUE(probe.ok()) << probe.status().ToString();

    IpsecTransportOptions options;
    options.inner = std::move(*tcp);
    options.probe = *probe;
    options.enforcement = IpsecEnforcement::kRequireKernelSa;
    auto driver = IpsecTransportDriver::Create(std::move(options));
    ASSERT_TRUE(driver.ok()) << driver.status().ToString();
    ASSERT_TRUE((*driver)->Start(TestConfig()).ok());

    // TEST-NET address with no SA.
    const std::array<std::byte, 4> addr = {
        std::byte{192}, std::byte{0}, std::byte{2}, std::byte{50}};
    auto endpoint = EndpointDescriptor::Ipv4Tcp(addr, 443);
    ASSERT_TRUE(endpoint.ok());
    ConnectRequest request;
    request.remote_endpoint = *endpoint;
    request.timeout_ms = 50;
    auto connected = (*driver)->Connect(request);
    EXPECT_EQ(connected.status().code(), StatusCode::kUnavailable);
    EXPECT_TRUE((*driver)->Shutdown().ok());
}

}  // namespace
}  // namespace mino::transport
