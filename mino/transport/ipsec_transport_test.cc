// Copyright 2026 The Mino Authors

#include "mino/transport/ipsec_transport.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <stdexcept>
#include <memory>
#include <thread>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/security/ipsec.h"
#include "mino/transport/tcp_driver.h"

namespace mino::transport {
namespace {

using namespace std::chrono_literals;

class PolicyCheckingProbe final : public security::IpsecSaProbe {
public:
    bool allowed = true;
    size_t checks = 0;
    Status RequireProtection(const security::IpsecSaSelector& selector) override {
        ++checks;
        EXPECT_TRUE(selector.require_out_policy);
        if (!allowed || !selector.require_out_policy) {
            return Status::Error(StatusCode::kUnavailable, "policy revoked");
        }
        return Status::Ok();
    }
};

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

// This test double advertises a pinned socket policy without requiring kernel
// privileges. Real policy installation/data flow is covered by SocketIpsecTest.
class PinnedTestDriver final : public TransportDriver {
public:
    std::atomic<size_t> io_calls{0};
    HealthState health() const noexcept override { return HealthState::kHealthy; }
    TransportCapabilities capabilities() const noexcept override {
        return {.kind = TransportKind::kNetwork,
                .reliability = TransportReliability::kReliable,
                .max_frame_size = 4096,
                .features = Capability::kConnect};
    }
    std::optional<security::SocketIpsecPolicy>
    MandatoryIpsecSocketPolicy() const noexcept override {
        return security::SocketIpsecPolicy{.outbound_spi = 1, .inbound_spi = 2};
    }
protected:
    Status DoStart(const DriverConfig&) override { return Status::Ok(); }
    Status DoShutdown() override { return Status::Ok(); }
    Result<ConnectionInfo> DoConnect(const ConnectRequest& request) override {
        return ConnectionInfo{.id = 1, .local_endpoint = Loopback(8000),
                              .peer_endpoint = request.remote_endpoint};
    }
    Result<ConnectionInfo> DoListen(const ListenRequest&) override {
        return Status::Error(StatusCode::kUnsupported);
    }
    Result<SendResult> DoSend(const SendRequest& request, SendOperation operation) override {
        ++io_calls;
        return SendResult{.operation = operation, .admitted_bytes = request.payload.size()};
    }
    Result<size_t> DoSendUntracked(const UntrackedSendRequest& request) override {
        ++io_calls;
        return request.payload.size();
    }
    Result<ReceiveResult> DoPoll(const ReceiveRequest&) override {
        ++io_calls;
        return Status::Error(StatusCode::kWouldBlock);
    }
    Result<CompletionPollResult> DoPollCompletions(const CompletionPollRequest&) override {
        return Status::Error(StatusCode::kWouldBlock);
    }
    Status DoClose(ConnectionId) override { return Status::Ok(); }
};

class ControlledProbe final : public security::IpsecSaProbe {
public:
    std::atomic<bool> allowed{true};
    std::atomic<bool> throws{false};
    std::atomic<size_t> checks{0};
    void BlockNext(bool result) {
        std::lock_guard lock(mutex_);
        block_next_ = true;
        blocked_ = released_ = returned_ = false;
        blocked_result_ = result;
    }
    bool WaitBlocked() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, 2s, [this] { return blocked_; });
    }
    void Release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }
    bool WaitReturned() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, 2s, [this] { return returned_; });
    }
    Status RequireProtection(const security::IpsecSaSelector&) override {
        ++checks;
        {
            std::unique_lock lock(mutex_);
            if (block_next_) {
                block_next_ = false;
                blocked_ = true;
                cv_.notify_all();
                // Bound even a failed test so driver destruction cannot hang.
                const bool released = cv_.wait_for(lock, 2s, [this] { return released_; });
                returned_ = true;
                cv_.notify_all();
                return released && blocked_result_ ? Status::Ok() :
                    Status::Error(StatusCode::kUnavailable);
            }
        }
        if (throws.load()) throw std::runtime_error("injected probe failure");
        return allowed.load() ? Status::Ok() : Status::Error(StatusCode::kUnavailable);
    }
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool block_next_ = false;
    bool blocked_ = false;
    bool released_ = false;
    bool returned_ = false;
    bool blocked_result_ = false;
};

class BackgroundIpsecTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto inner = std::make_unique<PinnedTestDriver>();
        inner_ = inner.get();
        IpsecTransportOptions options;
        options.inner = std::move(inner);
        options.probe = probe_;
        options.protected_socket_recheck_ms = 100;
        auto result = IpsecTransportDriver::Create(std::move(options));
        ASSERT_TRUE(result.ok());
        driver_ = std::move(*result);
        ASSERT_TRUE(driver_->Start(TestConfig()).ok());
        ASSERT_TRUE(Connect().ok());
    }
    void TearDown() override {
        probe_->Release();
        if (driver_) {
            EXPECT_TRUE(driver_->Shutdown().ok());
        }
    }
    Result<ConnectionInfo> Connect() {
        return driver_->Connect({.remote_endpoint = Loopback(9000), .local_bind = std::nullopt});
    }
    Status SendStatus() {
        return driver_->SendUntracked({.connection_id = 1, .payload = body_}).status();
    }
    bool WaitStatus(StatusCode code) {
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (SendStatus().code() == code) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }
    std::shared_ptr<ControlledProbe> probe_ = std::make_shared<ControlledProbe>();
    std::unique_ptr<IpsecTransportDriver> driver_;
    PinnedTestDriver* inner_ = nullptr;
    std::vector<std::byte> body_ = FrameBody();
};

TEST_F(BackgroundIpsecTest, ExpiryNeverRunsProbeOnIoThreadAndRetainsOwnedPayload) {
    probe_->BlockNext(true);
    ASSERT_TRUE(probe_->WaitBlocked()); // Worker runs without any Send/Poll calls.
    std::this_thread::sleep_for(110ms);
    const auto started = std::chrono::steady_clock::now();
    const auto checks = probe_->checks.load();
    EXPECT_EQ(SendStatus().code(), StatusCode::kWouldBlock);
    EXPECT_EQ(driver_->Send({.connection_id = 1, .payload = body_}).status().code(),
              StatusCode::kWouldBlock);
    auto owned = body_;
    EXPECT_EQ(driver_->TrySendOwned(1, std::move(owned)).status().code(), StatusCode::kWouldBlock);
    EXPECT_EQ(owned, body_);
    EXPECT_EQ(driver_->TrySendUntrackedOwned(1, std::move(owned),
                  UntrackedTrafficClass::kData).status().code(), StatusCode::kWouldBlock);
    EXPECT_EQ(owned, body_);
    EXPECT_EQ(driver_->Poll({.connection_id = 1}).status().code(), StatusCode::kWouldBlock);
    EXPECT_LT(std::chrono::steady_clock::now() - started, 250ms);
    EXPECT_EQ(probe_->checks.load(), checks);
    EXPECT_EQ(inner_->io_calls.load(), 0u);
    probe_->Release();
    // The slow check has already expired; a later fresh check resumes traffic.
    EXPECT_TRUE(WaitStatus(StatusCode::kOk));
}

TEST_F(BackgroundIpsecTest, DeniedAndThrowingChecksFailClosedThenRecover) {
    probe_->allowed = false;
    ASSERT_TRUE(WaitStatus(StatusCode::kUnavailable));
    probe_->throws = true;
    probe_->allowed = true;
    std::this_thread::sleep_for(120ms);
    EXPECT_EQ(SendStatus().code(), StatusCode::kUnavailable);
    probe_->throws = false;
    EXPECT_TRUE(WaitStatus(StatusCode::kOk));
}

TEST_F(BackgroundIpsecTest, CloseAndReusedConnectionIdIgnoreOldFailedCheck) {
    probe_->BlockNext(false);
    ASSERT_TRUE(probe_->WaitBlocked());
    ASSERT_TRUE(driver_->Close(1).ok());
    ASSERT_TRUE(Connect().ok()); // Reuses ID 1 while its old check is still pending.
    probe_->Release();
    ASSERT_TRUE(probe_->WaitReturned());
    // The worker must consume the old result before starting this next check.
    // Hold the new check so it cannot hide an incorrectly applied old denial.
    probe_->BlockNext(true);
    ASSERT_TRUE(probe_->WaitBlocked());
    EXPECT_NE(SendStatus().code(), StatusCode::kUnavailable);
    probe_->Release();
}

TEST_F(BackgroundIpsecTest, ShutdownJoinsPendingVerifierAndRestartWorks) {
    probe_->BlockNext(true);
    ASSERT_TRUE(probe_->WaitBlocked());
    auto shutdown = std::async(std::launch::async, [this] { return driver_->Shutdown(); });
    EXPECT_EQ(shutdown.wait_for(20ms), std::future_status::timeout);
    probe_->Release();
    ASSERT_EQ(shutdown.wait_for(2s), std::future_status::ready);
    EXPECT_TRUE(shutdown.get().ok());
    const auto checks = probe_->checks.load();
    std::this_thread::sleep_for(120ms);
    EXPECT_EQ(probe_->checks.load(), checks);
    ASSERT_TRUE(driver_->Start(TestConfig()).ok());
    ASSERT_TRUE(Connect().ok());
    EXPECT_TRUE(SendStatus().ok());
    probe_->allowed = false;
    EXPECT_TRUE(WaitStatus(StatusCode::kUnavailable));
}

TEST_F(BackgroundIpsecTest, DestructionJoinsWorkerWithoutExplicitShutdown) {
    probe_->BlockNext(true);
    ASSERT_TRUE(probe_->WaitBlocked());
    auto destroyed = std::async(std::launch::async,
        [driver = std::move(driver_)]() mutable { driver.reset(); });
    EXPECT_EQ(destroyed.wait_for(20ms), std::future_status::timeout);
    probe_->Release();
    ASSERT_EQ(destroyed.wait_for(2s), std::future_status::ready);
    destroyed.get();
    const auto checks = probe_->checks.load();
    std::this_thread::sleep_for(120ms);
    EXPECT_EQ(probe_->checks.load(), checks);
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

    auto allow = std::make_shared<PolicyCheckingProbe>();

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

    // Revocation after connect must gate every send entry point and receive.
    allow->allowed = false;
    EXPECT_EQ((*client)->Send(send).status().code(), StatusCode::kUnavailable);
    UntrackedSendRequest untracked;
    untracked.connection_id = connected->id;
    untracked.payload = body;
    EXPECT_EQ((*client)->SendUntracked(untracked).status().code(),
              StatusCode::kUnavailable);
    auto owned = body;
    EXPECT_EQ((*client)->TrySendOwned(connected->id, std::move(owned)).status().code(),
              StatusCode::kUnavailable);
    EXPECT_EQ(owned, body);
    owned = body;
    EXPECT_EQ((*client)->TrySendUntrackedOwned(connected->id, std::move(owned),
                  UntrackedTrafficClass::kData).status().code(),
              StatusCode::kUnavailable);
    EXPECT_EQ(owned, body);
    EXPECT_EQ((*server)->Poll(receive).status().code(), StatusCode::kUnavailable);
    EXPECT_GE(allow->checks, 9u);

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
