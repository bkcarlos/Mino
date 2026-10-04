// Copyright 2026 The Mino Authors
#include "mino/security/socket_ipsec.h"
#include "mino/bridge/wire_frame.h"
#include "mino/security/ipsec.h"
#include "mino/transport/ipsec_transport.h"
#include "mino/transport/tcp_driver.h"
#include "mino/transport/udp_driver.h"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <thread>
#include <gtest/gtest.h>

namespace mino::security {
namespace {
using namespace std::chrono_literals;

#define ASSERT_KERNEL_OK(value)                                               \
    do {                                                                      \
        if (!(value).ok() && std::getenv("MINO_REQUIRE_KERNEL_IPSEC") == nullptr && \
            ((value).code() == StatusCode::kPermissionDenied ||                 \
             (value).code() == StatusCode::kUnavailable)) {                     \
            GTEST_SKIP() << (value).ToString();                                \
        }                                                                     \
        ASSERT_TRUE((value).ok()) << (value).ToString();                        \
    } while (false)

class Fd {
public:
    explicit Fd(int fd) : fd_(fd) {}
    ~Fd() { if (fd_ >= 0) ::close(fd_); }
    int get() const { return fd_; }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
private:
    int fd_;
};

sockaddr_in BindLoopback(int fd) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    EXPECT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    socklen_t length = sizeof(address);
    EXPECT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length), 0);
    return address;
}

bool Readable(int fd, int timeout_ms) {
    pollfd descriptor{fd, POLLIN, 0};
    return ::poll(&descriptor, 1, timeout_ms) > 0 && (descriptor.revents & POLLIN);
}

TEST(SocketIpsecTest, RejectsInvalidSocketAndFamily) {
    EXPECT_EQ(InstallSocketIpsecPolicy(-1, AF_INET, {}).code(), StatusCode::kInvalidArgument);
    Fd fd(::socket(AF_INET, SOCK_DGRAM, 0));
    EXPECT_EQ(InstallSocketIpsecPolicy(fd.get(), AF_UNIX, {}).code(), StatusCode::kInvalidArgument);
}

TEST(SocketIpsecTest, MissingSaCannotFallBackToPlaintext) {
    Fd receiver(::socket(AF_INET, SOCK_DGRAM, 0));
    const auto address = BindLoopback(receiver.get());
    Fd sender(::socket(AF_INET, SOCK_DGRAM, 0));
    const auto status = InstallSocketIpsecPolicy(sender.get(), AF_INET, {});
    ASSERT_KERNEL_OK(status);
    // Use sendto so the mandatory policy must be resolved on this send.
    const char payload[] = "must-not-be-plaintext";
    EXPECT_LT(::sendto(sender.get(), payload, sizeof(payload), MSG_DONTWAIT,
        reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    EXPECT_FALSE(Readable(receiver.get(), 30));
}

TEST(SocketIpsecTest, GlobalPolicyRemovalKeepsSocketGuardAndRejectsPlaintext) {
    auto session = TestLoopbackXfrmSession::InstallIpv4Loopback(IpsecProtocol::kUdp);
    ASSERT_KERNEL_OK(session.status());
    ASSERT_TRUE(session->RemovePoliciesForTesting().ok());
    Fd receiver(::socket(AF_INET, SOCK_DGRAM, 0));
    const auto status = InstallSocketIpsecPolicy(receiver.get(), AF_INET, {});
    ASSERT_KERNEL_OK(status);
    const auto address = BindLoopback(receiver.get());
    Fd plain(::socket(AF_INET, SOCK_DGRAM, 0));
    const char payload[] = "socket-policy-test";
    ASSERT_EQ(::sendto(plain.get(), payload, sizeof(payload), 0,
        reinterpret_cast<const sockaddr*>(&address), sizeof(address)), sizeof(payload));
    EXPECT_FALSE(Readable(receiver.get(), 30));

    Fd protected_sender(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_TRUE(InstallSocketIpsecPolicy(protected_sender.get(), AF_INET, {}).ok());
    ASSERT_EQ(::sendto(protected_sender.get(), payload, sizeof(payload), 0,
        reinterpret_cast<const sockaddr*>(&address), sizeof(address)), sizeof(payload));
    ASSERT_TRUE(Readable(receiver.get(), 1000));
    std::array<char, sizeof(payload)> received{};
    ASSERT_EQ(::recv(receiver.get(), received.data(), received.size(), 0), sizeof(payload));
    EXPECT_EQ(std::string(received.data()), payload);
}

TEST(SocketIpsecTest, PinnedSpiCannotFallBackToAnotherSa) {
    auto session = TestLoopbackXfrmSession::InstallIpv4Loopback(IpsecProtocol::kUdp);
    ASSERT_KERNEL_OK(session.status());
    ASSERT_TRUE(session->RemovePoliciesForTesting().ok());
    Fd receiver(::socket(AF_INET, SOCK_DGRAM, 0));
    const auto address = BindLoopback(receiver.get());
    Fd sender(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_TRUE(InstallSocketIpsecPolicy(sender.get(), AF_INET,
        {.outbound_spi = 0xA11CE099u}).ok());
    const char payload[] = "uninstalled-spi";
    EXPECT_LT(::sendto(sender.get(), payload, sizeof(payload), MSG_DONTWAIT,
        reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    EXPECT_FALSE(Readable(receiver.get(), 30));
}

TEST(SocketIpsecTest, Ipv6GuardDisablesMappedIpv4) {
    Fd fd(::socket(AF_INET6, SOCK_DGRAM, 0));
    const auto status = InstallSocketIpsecPolicy(fd.get(), AF_INET6, {});
    ASSERT_KERNEL_OK(status);
    int v6_only = 0;
    socklen_t size = sizeof(v6_only);
    ASSERT_EQ(::getsockopt(fd.get(), IPPROTO_IPV6, IPV6_V6ONLY, &v6_only, &size), 0);
    EXPECT_EQ(v6_only, 1);
}

class CountingProbe final : public IpsecSaProbe {
public:
    std::atomic<bool> allow{true};
    std::atomic<size_t> checks{0};
    Status RequireProtection(const IpsecSaSelector&) override {
        ++checks;
        return allow ? Status::Ok() : Status::Error(StatusCode::kUnavailable);
    }
};

transport::EndpointDescriptor Endpoint(uint16_t port, bool tcp = false) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    return tcp ? *transport::EndpointDescriptor::Ipv4Tcp(address, port) :
                 *transport::EndpointDescriptor::Ipv4Udp(address, port);
}

void VerifyGuardedRecheck(uint32_t interval_ms) {
    auto session = TestLoopbackXfrmSession::InstallIpv4Loopback(IpsecProtocol::kUdp);
    ASSERT_KERNEL_OK(session.status());
    ASSERT_TRUE(session->RemovePoliciesForTesting().ok());
    Fd receiver(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_TRUE(InstallSocketIpsecPolicy(receiver.get(), AF_INET, {}).ok());
    const auto address = BindLoopback(receiver.get());
    transport::UdpDriverOptions inner_options;
    inner_options.ipsec_policy = SocketIpsecPolicy{
        .outbound_spi = 0xA11CE001u, .inbound_spi = 0xA11CE001u};
    auto inner = transport::UdpDriver::Create(inner_options);
    ASSERT_TRUE(inner.ok());
    auto probe = std::make_shared<CountingProbe>();
    transport::IpsecTransportOptions options;
    options.inner = std::move(*inner);
    options.probe = probe;
    options.protected_socket_recheck_ms = interval_ms;
    auto driver = transport::IpsecTransportDriver::Create(std::move(options));
    ASSERT_TRUE(driver.ok());
    ASSERT_TRUE((*driver)->Start({}).ok());
    auto connection = (*driver)->Connect({.remote_endpoint = Endpoint(ntohs(address.sin_port)), .local_bind = std::nullopt});
    ASSERT_TRUE(connection.ok()) << connection.status().ToString();
    const auto checks = probe->checks.load();
    const std::array<std::byte, 1> payload{std::byte{42}};
    transport::UntrackedSendRequest send{.connection_id = connection->id, .payload = payload};
    if (interval_ms == 60000) {
        for (int i = 0; i < 10; ++i) ASSERT_TRUE((*driver)->SendUntracked(send).ok());
        EXPECT_EQ(probe->checks.load(), checks); // no XFRM probe on protected data path
        EXPECT_TRUE(Readable(receiver.get(), 1000));
    } else {
        probe->allow = false;
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (probe->checks.load() <= checks + 1 &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(1ms);
        }
        EXPECT_FALSE((*driver)->SendUntracked(send).ok());
        EXPECT_GT(probe->checks.load(), checks);
        // A failed refresh must not extend the previous allow deadline.
        EXPECT_FALSE((*driver)->SendUntracked(send).ok());
        EXPECT_GT(probe->checks.load(), checks + 1);
    }
    EXPECT_TRUE((*driver)->Shutdown().ok());
}
TEST(SocketIpsecTest, GuardedUdpAvoidsPerMessageProbe) { VerifyGuardedRecheck(60000); }
TEST(SocketIpsecTest, GuardedUdpRechecksAndDoesNotCacheFailure) { VerifyGuardedRecheck(1); }

TEST(SocketIpsecTest, TcpListenerAndAcceptedSocketRemainGuarded) {
    auto session = TestLoopbackXfrmSession::InstallIpv4Loopback(IpsecProtocol::kTcp);
    ASSERT_KERNEL_OK(session.status());
    ASSERT_TRUE(session->RemovePoliciesForTesting().ok());
    transport::TcpDriverOptions options;
    options.ipsec_policy.emplace();
    auto server = transport::TcpDriver::Create(options);
    auto client = transport::TcpDriver::Create(options);
    ASSERT_TRUE(server.ok()) << server.status().ToString();
    ASSERT_TRUE(client.ok()) << client.status().ToString();
    ASSERT_TRUE((*server)->Start({}).ok());
    ASSERT_TRUE((*client)->Start({}).ok());
    uint16_t port = 0;
    {
        Fd reserve(::socket(AF_INET, SOCK_STREAM, 0));
        port = ntohs(BindLoopback(reserve.get()).sin_port);
    }
    auto listener = (*server)->Listen({.local_endpoint = Endpoint(port, true)});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    ASSERT_TRUE(listener->local_endpoint.has_value());
    auto connected = (*client)->Connect({.remote_endpoint = *listener->local_endpoint, .local_bind = std::nullopt, .timeout_ms = 1000});
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();
    auto accepted = (*server)->Accept({.listener_id = listener->id, .timeout_ms = 1000});
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
    EXPECT_TRUE((*server)->MandatoryIpsecSocketPolicy().has_value());
    bridge::WireFrame frame;
    frame.header.topic_id = 1;
    frame.header.msg_type = 2;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.sequence_num = 1;
    frame.payload = {std::byte{42}};
    auto encoded = bridge::WireFrameCodec::Encode(frame);
    ASSERT_TRUE(encoded.ok());
    ASSERT_TRUE((*client)->SendUntracked({.connection_id = connected->id, .payload = *encoded}).ok());
    auto received = (*server)->Poll({.timeout_ms = 1000, .connection_id = accepted->id});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages.front().payload, *encoded);
    // A plaintext client cannot complete a handshake with this listener.
    Fd plain(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(listener->local_endpoint->port());
    (void)::connect(plain.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    auto rejected = (*server)->Accept({.listener_id = listener->id, .timeout_ms = 30});
    EXPECT_FALSE(rejected.ok());
    EXPECT_TRUE((*client)->Shutdown().ok());
    EXPECT_TRUE((*server)->Shutdown().ok());
}
#undef ASSERT_KERNEL_OK
}  // namespace
}  // namespace mino::security
