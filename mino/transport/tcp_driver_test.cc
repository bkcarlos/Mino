// Copyright 2026 The Mino Authors

#include "mino/transport/tcp_driver.h"

#include <arpa/inet.h>
#if defined(__linux__)
#include <dirent.h>
#include <sys/epoll.h>
#endif
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/security/test_tls_credentials.h"

namespace mino::transport {

const char* TcpDriverReadinessBackendForTest() noexcept;

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

std::vector<std::byte> HeartbeatBody(uint64_t timestamp_ns = 0) {
    bridge::WireFrame heartbeat;
    heartbeat.header.frame_type = bridge::FrameType::kHeartbeat;
    heartbeat.header.flags = bridge::FlagValue(bridge::FrameFlag::kControlFrame);
    heartbeat.header.timestamp_ns = timestamp_ns;
    auto encoded = bridge::WireFrameCodec::Encode(heartbeat);
    EXPECT_TRUE(encoded.ok()) << encoded.status().ToString();
    return encoded.ok() ? std::move(*encoded) : std::vector<std::byte>{};
}

ssize_t SendRawNoSignal(int fd, std::span<const std::byte> bytes) {
#if defined(MSG_NOSIGNAL)
    return ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
#else
#if defined(SO_NOSIGPIPE)
    const int enabled = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
                     sizeof(enabled)) != 0) {
        return -1;
    }
#endif
    return ::send(fd, bytes.data(), bytes.size(), 0);
#endif
}

#if defined(__linux__)
int FindProcessEpollFd() {
    DIR* directory = ::opendir("/proc/self/fd");
    EXPECT_NE(directory, nullptr);
    if (directory == nullptr) return -1;
    int epoll_fd = -1;
    while (const dirent* entry = ::readdir(directory)) {
        char* end = nullptr;
        const long candidate = std::strtol(entry->d_name, &end, 10);
        if (end == entry->d_name || *end != '\0' || candidate < 0) continue;
        const std::string path =
            "/proc/self/fd/" + std::to_string(candidate);
        std::array<char, 64> target{};
        const ssize_t size =
            ::readlink(path.c_str(), target.data(), target.size() - 1);
        if (size < 0) continue;
        target[static_cast<size_t>(size)] = '\0';
        if (std::string(target.data()) != "anon_inode:[eventpoll]") continue;
        EXPECT_EQ(epoll_fd, -1) << "test expected one live epoll instance";
        epoll_fd = static_cast<int>(candidate);
    }
    (void)::closedir(directory);
    return epoll_fd;
}

bool EpollHasEventMask(int epoll_fd, uint32_t mask) {
    const std::string path = "/proc/self/fdinfo/" + std::to_string(epoll_fd);
    FILE* file = std::fopen(path.c_str(), "r");
    if (file == nullptr) return false;
    std::array<char, 256> line{};
    bool found = false;
    while (std::fgets(line.data(), static_cast<int>(line.size()), file) !=
           nullptr) {
        unsigned int events = 0;
        if (std::sscanf(line.data(), "tfd: %*d events: %x", &events) == 1 &&
            (events & mask) != 0) {
            found = true;
            break;
        }
    }
    (void)std::fclose(file);
    return found;
}

bool WaitForEpollEventMask(int epoll_fd, uint32_t mask, bool expected,
                           std::chrono::milliseconds timeout = 1000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (EpollHasEventMask(epoll_fd, mask) == expected) return true;
        std::this_thread::sleep_for(2ms);
    }
    return EpollHasEventMask(epoll_fd, mask) == expected;
}
#endif

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

enum class ScriptedTlsMode { kWriteWantsRead, kReadWantsWrite };

class ScriptedTlsState final {
public:
    explicit ScriptedTlsState(ScriptedTlsMode mode) noexcept : mode(mode) {}

    void Record(char operation) {
        std::lock_guard lock(mutex);
        calls.push_back(operation);
        cv.notify_all();
    }

    bool WaitForCalls(size_t count,
                      std::chrono::milliseconds timeout = 1000ms) {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout,
                           [&] { return calls.size() >= count; });
    }

    std::vector<char> Calls() const {
        std::lock_guard lock(mutex);
        return calls;
    }

    void EnterCreateAndWait() {
        std::unique_lock lock(mutex);
        create_entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_create; });
    }

    bool WaitForCreate(std::chrono::milliseconds timeout = 1000ms) {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [&] { return create_entered; });
    }

    void ReleaseCreate() {
        std::lock_guard lock(mutex);
        release_create = true;
        cv.notify_all();
    }

    void WaitOnReadRetryIfRequested() {
        std::unique_lock lock(mutex);
        if (!block_read_retry) return;
        read_retry_entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_read_retry; });
    }

    void ReleaseReadRetry() {
        std::lock_guard lock(mutex);
        release_read_retry = true;
        cv.notify_all();
    }

    const ScriptedTlsMode mode;
    std::atomic<bool> crossed_operations{false};
    bool block_server_create = false;
    bool block_read_retry = false;

private:
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::vector<char> calls;
    bool create_entered = false;
    bool release_create = false;
    bool read_retry_entered = false;
    bool release_read_retry = false;
};

class ScriptedTlsChannel final : public security::TlsChannel {
public:
    explicit ScriptedTlsChannel(std::shared_ptr<ScriptedTlsState> state)
        : state_(std::move(state)) {}

    Result<security::TlsIoResult> Handshake() noexcept override {
        return security::TlsIoResult{};
    }

    Result<security::TlsIoResult> Read(
        std::span<std::byte>) noexcept override {
        state_->Record('R');
        if (pending_ == 'W') return Crossed();
        if (pending_ == 'R') {
            state_->WaitOnReadRetryIfRequested();
            pending_ = 0;
            if (state_->mode == ScriptedTlsMode::kReadWantsWrite) {
                return security::TlsIoResult{.peer_closed = true};
            }
            pending_ = 'R';
            return security::TlsIoResult{
                .need = security::TlsIoNeed::kRead};
        }
        pending_ = 'R';
        return security::TlsIoResult{
            .need = state_->mode == ScriptedTlsMode::kReadWantsWrite
                        ? security::TlsIoNeed::kWrite
                        : security::TlsIoNeed::kRead};
    }

    Result<security::TlsIoResult> Write(
        std::span<const std::byte> input) noexcept override {
        state_->Record('W');
        if (pending_ == 'R') return Crossed();
        if (pending_ == 'W') {
            pending_ = 0;
            return security::TlsIoResult{.bytes = input.size()};
        }
        if (state_->mode == ScriptedTlsMode::kWriteWantsRead) {
            pending_ = 'W';
            return security::TlsIoResult{
                .need = security::TlsIoNeed::kRead};
        }
        return security::TlsIoResult{.bytes = input.size()};
    }

    bool handshake_complete() const noexcept override { return true; }
    bool has_buffered_read() const noexcept override {
        return pending_ == 'W';
    }
    Result<security::AuthenticatedPeer> peer() const noexcept override {
        return security::AuthenticatedPeer{
            .node_id = NodeId{1},
            .security_domain = SecurityDomainId{1},
            .credential_generation = 1,
        };
    }

private:
    Result<security::TlsIoResult> Crossed() noexcept {
        state_->crossed_operations.store(true, std::memory_order_release);
        return Status::Error(StatusCode::kInternal,
                             "fake TLS operation was crossed");
    }

    std::shared_ptr<ScriptedTlsState> state_;
    char pending_ = 0;
};

class ScriptedTlsFactory final : public security::TlsChannelFactory {
public:
    explicit ScriptedTlsFactory(std::shared_ptr<ScriptedTlsState> state)
        : state_(std::move(state)) {}

    Status Prepare() override { return Status::Ok(); }

    Result<std::unique_ptr<security::TlsChannel>> Create(
        int, security::TlsRole role) override {
        if (role == security::TlsRole::kServer && state_->block_server_create) {
            state_->EnterCreateAndWait();
        }
        return std::unique_ptr<security::TlsChannel>(
            new ScriptedTlsChannel(state_));
    }

private:
    std::shared_ptr<ScriptedTlsState> state_;
};

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

struct RawListener {
    ScopedFd fd;
    EndpointDescriptor endpoint;
};

RawListener ListenRaw(int receive_buffer_bytes = 4096) {
    ScopedFd fd(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    EXPECT_GE(fd.get(), 0);
    const int reuse = 1;
    EXPECT_EQ(::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &reuse,
                           sizeof(reuse)),
              0);
    EXPECT_EQ(::setsockopt(fd.get(), SOL_SOCKET, SO_RCVBUF,
                           &receive_buffer_bytes,
                           sizeof(receive_buffer_bytes)),
              0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    EXPECT_EQ(::bind(fd.get(), reinterpret_cast<sockaddr*>(&address),
                     sizeof(address)),
              0);
    EXPECT_EQ(::listen(fd.get(), 1), 0);
    socklen_t size = sizeof(address);
    EXPECT_EQ(::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&address),
                            &size),
              0);
    return RawListener{.fd = std::move(fd),
                       .endpoint = Loopback(ntohs(address.sin_port))};
}

ScopedFd AcceptRaw(const RawListener& listener) {
    pollfd descriptor{.fd = listener.fd.get(),
                      .events = POLLIN,
                      .revents = 0};
    EXPECT_EQ(::poll(&descriptor, 1, 1000), 1);
    ScopedFd accepted(::accept(listener.fd.get(), nullptr, nullptr));
    EXPECT_GE(accepted.get(), 0);
    return accepted;
}

bool ReceiveUntil(int fd, std::vector<std::byte>* bytes, size_t target_size,
                  std::chrono::milliseconds timeout = 3000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::array<std::byte, 64 * 1024> buffer{};
    while (bytes->size() < target_size) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) return false;
        pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
        const int ready = ::poll(
            &descriptor, 1,
            static_cast<int>(std::min<int64_t>(remaining.count(), 100)));
        if (ready < 0) return false;
        if (ready == 0) continue;
        const size_t amount =
            std::min(buffer.size(), target_size - bytes->size());
        const ssize_t received = ::recv(fd, buffer.data(), amount, 0);
        if (received <= 0) return false;
        bytes->insert(bytes->end(), buffer.begin(),
                      buffer.begin() + received);
    }
    return true;
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

bool WaitForReadyMessageCount(const TcpDriver& driver, size_t expected,
                              std::chrono::milliseconds timeout = 1000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (driver.stats().ready_receive_messages == expected) return true;
        std::this_thread::sleep_for(2ms);
    }
    return driver.stats().ready_receive_messages == expected;
}

bool WaitForQueuedSendBytes(const TcpDriver& driver, size_t expected,
                            std::chrono::milliseconds timeout = 5000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (driver.stats().queued_send_bytes == expected) return true;
        std::this_thread::sleep_for(2ms);
    }
    return driver.stats().queued_send_bytes == expected;
}

TEST(TcpDriverBackendTest, SelectsPlatformReadinessBackend) {
#if defined(__linux__)
    EXPECT_STREQ(TcpDriverReadinessBackendForTest(), "epoll");
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || \
    defined(__OpenBSD__) || defined(__DragonFly__)
    EXPECT_STREQ(TcpDriverReadinessBackendForTest(), "kqueue");
#else
    EXPECT_STREQ(TcpDriverReadinessBackendForTest(), "poll");
#endif
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
    options = TestOptions();
    options.max_control_send_buffer_bytes = options.max_frame_body_bytes;
    EXPECT_EQ(ValidateTcpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
    options = TestOptions();
    options.max_control_send_messages = 0;
    EXPECT_EQ(ValidateTcpDriverOptions(options).code(),
              StatusCode::kInvalidArgument);
}

TEST(TcpDriverTest, TlsWriteWantReadRetriesWriteBeforeAnyRead) {
    RawListener listener = ListenRaw();
    auto state = std::make_shared<ScriptedTlsState>(
        ScriptedTlsMode::kWriteWantsRead);
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 2000;
    options.idle_timeout_ms = 5000;
    options.partial_frame_timeout_ms = 5000;
    options.tls_factory = std::make_shared<ScriptedTlsFactory>(state);
    auto driver_result = TcpDriver::Create(options);
    ASSERT_TRUE(driver_result.ok()) << driver_result.status().ToString();
    auto driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());
    auto connected = driver->Connect({
        .remote_endpoint = listener.endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();
    ScopedFd peer = AcceptRaw(listener);

    const std::vector<std::byte> body = FrameBody(64, 8001);
    ASSERT_TRUE(driver->SendUntracked({
        .connection_id = connected->id,
        .payload = body,
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
    ASSERT_TRUE(state->WaitForCalls(1));
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(state->Calls(), std::vector<char>({'W'}));
    EXPECT_FALSE(state->crossed_operations.load(std::memory_order_acquire));

    const std::byte readiness{0x01};
    ASSERT_EQ(SendRawNoSignal(peer.get(), std::span(&readiness, 1)), 1);
    ASSERT_TRUE(state->WaitForCalls(2));
    const auto calls = state->Calls();
    ASSERT_GE(calls.size(), 2u);
    EXPECT_EQ(calls[0], 'W');
    EXPECT_EQ(calls[1], 'W');
    EXPECT_FALSE(state->crossed_operations.load(std::memory_order_acquire));
}

TEST(TcpDriverTest, TlsReadWantWriteRetriesReadBeforeAnyWrite) {
    RawListener listener = ListenRaw();
    auto state = std::make_shared<ScriptedTlsState>(
        ScriptedTlsMode::kReadWantsWrite);
    state->block_read_retry = true;
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 2000;
    options.idle_timeout_ms = 5000;
    options.partial_frame_timeout_ms = 5000;
    options.tls_factory = std::make_shared<ScriptedTlsFactory>(state);
    auto driver_result = TcpDriver::Create(options);
    ASSERT_TRUE(driver_result.ok()) << driver_result.status().ToString();
    auto driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());
    auto connected = driver->Connect({
        .remote_endpoint = listener.endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();
    ScopedFd peer = AcceptRaw(listener);

    const std::byte readiness{0x02};
    ASSERT_EQ(SendRawNoSignal(peer.get(), std::span(&readiness, 1)), 1);
    ASSERT_TRUE(state->WaitForCalls(1));
    ASSERT_TRUE(driver->SendUntracked({
        .connection_id = connected->id,
        .payload = FrameBody(64, 8002),
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
    state->ReleaseReadRetry();
    ASSERT_TRUE(state->WaitForCalls(2));
    const auto calls = state->Calls();
    ASSERT_GE(calls.size(), 2u);
    EXPECT_EQ(calls[0], 'R');
    EXPECT_EQ(calls[1], 'R');
    EXPECT_FALSE(state->crossed_operations.load(std::memory_order_acquire));
}

TEST(TcpDriverTest, AcceptedTlsChannelCreationDoesNotHoldDriverMutex) {
    auto state = std::make_shared<ScriptedTlsState>(
        ScriptedTlsMode::kWriteWantsRead);
    state->block_server_create = true;
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 2000;
    options.idle_timeout_ms = 5000;
    options.partial_frame_timeout_ms = 5000;
    options.tls_factory = std::make_shared<ScriptedTlsFactory>(state);
    auto driver_result = TcpDriver::Create(options);
    ASSERT_TRUE(driver_result.ok()) << driver_result.status().ToString();
    auto driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = driver->Listen({.local_endpoint = endpoint, .backlog = 4});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    ScopedFd peer = ConnectRaw(endpoint);
    ASSERT_TRUE(state->WaitForCreate());

    auto stats = std::async(std::launch::async, [&] { return driver->stats(); });
    const auto wait_status = stats.wait_for(200ms);
    state->ReleaseCreate();
    EXPECT_EQ(wait_status, std::future_status::ready);
    EXPECT_EQ(stats.get().listeners, 1u);
}

TEST(TcpDriverTest, MutualTlsGatesAcceptAndCredentialRotationAffectsNewConnections) {
    const std::array principals = {
        security::testing::TestPrincipal{
            NodeId{101}, security::SecurityDomainId{77}},
        security::testing::TestPrincipal{
            NodeId{202}, security::SecurityDomainId{77}},
        security::testing::TestPrincipal{
            NodeId{303}, security::SecurityDomainId{77}},
    };
    auto generated = security::testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    auto client_provider = security::StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    auto server_provider = security::StaticTlsCredentialProvider::Create(
        std::move((*generated)[1]));
    ASSERT_TRUE(client_provider.ok()) << client_provider.status().ToString();
    ASSERT_TRUE(server_provider.ok()) << server_provider.status().ToString();
    auto client_factory =
        security::CreateOpenSslTlsChannelFactory(*client_provider);
    auto server_factory =
        security::CreateOpenSslTlsChannelFactory(*server_provider);
    ASSERT_TRUE(client_factory.ok()) << client_factory.status().ToString();
    ASSERT_TRUE(server_factory.ok()) << server_factory.status().ToString();

    TcpDriverOptions client_options = TestOptions();
    TcpDriverOptions server_options = TestOptions();
    client_options.tls_factory = *client_factory;
    server_options.tls_factory = *server_factory;
    client_options.idle_timeout_ms = 2000;
    server_options.idle_timeout_ms = 2000;
    auto client_result = TcpDriver::Create(client_options);
    auto server_result = TcpDriver::Create(server_options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();
    ASSERT_TRUE(server_result.ok()) << server_result.status().ToString();
    auto client = std::move(*client_result);
    auto server = std::move(*server_result);
    ASSERT_TRUE(client->Start(TestConfig()).ok());
    ASSERT_TRUE(server->Start(TestConfig()).ok());

    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 4});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    auto first_client = client->Connect({
        .remote_endpoint = endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(first_client.ok()) << first_client.status().ToString();
    auto first_server = server->Accept({
        .listener_id = listener->id,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(first_server.ok()) << first_server.status().ToString();
    auto first_client_peer = client->AuthenticatedPeer(first_client->id);
    auto first_server_peer = server->AuthenticatedPeer(first_server->id);
    ASSERT_TRUE(first_client_peer.ok())
        << first_client_peer.status().ToString();
    ASSERT_TRUE(first_server_peer.ok())
        << first_server_peer.status().ToString();
    EXPECT_EQ(first_client_peer->node_id, NodeId{202});
    EXPECT_EQ(first_server_peer->node_id, NodeId{101});
    EXPECT_EQ(first_client_peer->credential_generation, 1u);

    const std::vector<std::byte> body = FrameBody(128, 9001);
    ASSERT_TRUE(client->SendUntracked({
        .connection_id = first_client->id,
        .payload = body,
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
    auto received = server->Poll({
        .max_messages = 1,
        .max_bytes = 4096,
        .timeout_ms = 1000,
        .connection_id = first_server->id,
    });
    ASSERT_TRUE(received.ok())
        << received.status().ToString()
        << " client_queued=" << client->stats().queued_send_bytes
        << " server_active=" << server->stats().active_connections
        << " server_ready=" << server->stats().ready_receive_messages;
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages.front().payload, body);

    ASSERT_TRUE((*client_provider)
                    ->Rotate(std::move((*generated)[2]))
                    .ok());
    auto second_client = client->Connect({
        .remote_endpoint = endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(second_client.ok()) << second_client.status().ToString();
    auto second_server = server->Accept({
        .listener_id = listener->id,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(second_server.ok()) << second_server.status().ToString();
    auto second_client_peer = client->AuthenticatedPeer(second_client->id);
    auto second_server_peer = server->AuthenticatedPeer(second_server->id);
    ASSERT_TRUE(second_client_peer.ok())
        << second_client_peer.status().ToString();
    ASSERT_TRUE(second_server_peer.ok())
        << second_server_peer.status().ToString();
    EXPECT_EQ(second_client_peer->node_id, NodeId{202});
    EXPECT_EQ(second_client_peer->credential_generation, 2u);
    EXPECT_EQ(second_server_peer->node_id, NodeId{303});
    auto unchanged = server->AuthenticatedPeer(first_server->id);
    ASSERT_TRUE(unchanged.ok()) << unchanged.status().ToString();
    EXPECT_EQ(unchanged->node_id, NodeId{101});
}

TEST(TcpDriverTest, TlsHandshakeTimeoutClosesSilentPeer) {
    const std::array principals = {
        security::testing::TestPrincipal{
            NodeId{101}, SecurityDomainId{77}},
    };
    auto generated = security::testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    auto provider = security::StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    ASSERT_TRUE(provider.ok()) << provider.status().ToString();
    auto factory = security::CreateOpenSslTlsChannelFactory(*provider);
    ASSERT_TRUE(factory.ok()) << factory.status().ToString();

    TcpDriverOptions options = TestOptions();
    options.tls_factory = *factory;
    options.tls_handshake_timeout_ms = 50;
    options.idle_timeout_ms = 1000;
    auto server_result = TcpDriver::Create(options);
    ASSERT_TRUE(server_result.ok()) << server_result.status().ToString();
    auto server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 4});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    ScopedFd raw = ConnectRaw(endpoint);
    ASSERT_GE(raw.get(), 0);
    ASSERT_TRUE(WaitForConnectionCount(*server, 1));
    EXPECT_TRUE(WaitForConnectionCount(*server, 0, 2000ms));
}

TEST(TcpDriverTest, TlsHandshakePeerCloseIsCleanlyReaped) {
    const std::array principals = {
        security::testing::TestPrincipal{
            NodeId{101}, SecurityDomainId{77}},
    };
    auto generated = security::testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    auto provider = security::StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    ASSERT_TRUE(provider.ok()) << provider.status().ToString();
    auto factory = security::CreateOpenSslTlsChannelFactory(*provider);
    ASSERT_TRUE(factory.ok()) << factory.status().ToString();

    TcpDriverOptions options = TestOptions();
    options.tls_factory = *factory;
    options.tls_handshake_timeout_ms = 1000;
    auto server_result = TcpDriver::Create(options);
    ASSERT_TRUE(server_result.ok()) << server_result.status().ToString();
    auto server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 4});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    {
        ScopedFd raw = ConnectRaw(endpoint);
        ASSERT_GE(raw.get(), 0);
        ASSERT_TRUE(WaitForConnectionCount(*server, 1));
    }
    EXPECT_TRUE(WaitForConnectionCount(*server, 0, 2000ms));
}

TEST(TcpDriverTest, TlsBidirectionalLargeFramesSurviveSocketBackpressure) {
    const std::array principals = {
        security::testing::TestPrincipal{
            NodeId{101}, SecurityDomainId{77}},
    };
    auto generated = security::testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    auto provider = security::StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    ASSERT_TRUE(provider.ok()) << provider.status().ToString();
    auto factory = security::CreateOpenSslTlsChannelFactory(*provider);
    ASSERT_TRUE(factory.ok()) << factory.status().ToString();

    TcpDriverOptions options = TestOptions();
    options.max_frame_body_bytes = 2u * 1024u * 1024u;
    options.max_total_send_buffer_bytes = 32u * 1024u * 1024u;
    options.max_connection_send_buffer_bytes = 32u * 1024u * 1024u;
    options.max_ready_receive_bytes = 32u * 1024u * 1024u;
    options.max_ready_receive_messages = 32;
    options.max_control_send_buffer_bytes = 4u * 1024u * 1024u;
    options.idle_timeout_ms = 10'000;
    options.partial_frame_timeout_ms = 10'000;
    options.tls_factory = *factory;
    DriverPair pair = ConnectPair(options);
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    constexpr size_t kFrameCount = 16;
    constexpr size_t kPayloadBytes = 1024u * 1024u;
    std::vector<std::vector<std::byte>> client_frames;
    std::vector<std::vector<std::byte>> server_frames;
    client_frames.reserve(kFrameCount);
    server_frames.reserve(kFrameCount);
    for (size_t index = 0; index < kFrameCount; ++index) {
        client_frames.push_back(FrameBody(kPayloadBytes, 10'000 + index));
        server_frames.push_back(FrameBody(kPayloadBytes, 20'000 + index));
        ASSERT_TRUE(pair.client->SendUntracked({
            .connection_id = pair.client_connection.id,
            .payload = client_frames.back(),
            .traffic_class = UntrackedTrafficClass::kData,
        }).ok());
        ASSERT_TRUE(pair.server->SendUntracked({
            .connection_id = pair.server_connection.id,
            .payload = server_frames.back(),
            .traffic_class = UntrackedTrafficClass::kData,
        }).ok());
    }

    for (size_t index = 0; index < kFrameCount; ++index) {
        auto at_server = pair.server->Poll({
            .max_messages = 1,
            .max_bytes = options.max_frame_body_bytes,
            .timeout_ms = 5000,
            .connection_id = pair.server_connection.id,
        });
        ASSERT_TRUE(at_server.ok())
            << at_server.status().ToString()
            << " index=" << index
            << " client_queued=" << pair.client->stats().queued_send_bytes
            << " server_queued=" << pair.server->stats().queued_send_bytes
            << " server_ready=" << pair.server->stats().ready_receive_messages
            << " client_ready=" << pair.client->stats().ready_receive_messages;
        ASSERT_EQ(at_server->messages.size(), 1u);
        EXPECT_EQ(at_server->messages.front().payload, client_frames[index]);

        auto at_client = pair.client->Poll({
            .max_messages = 1,
            .max_bytes = options.max_frame_body_bytes,
            .timeout_ms = 5000,
            .connection_id = pair.client_connection.id,
        });
        ASSERT_TRUE(at_client.ok()) << at_client.status().ToString();
        ASSERT_EQ(at_client->messages.size(), 1u);
        EXPECT_EQ(at_client->messages.front().payload, server_frames[index]);
    }
    EXPECT_TRUE(WaitForQueuedSendBytes(*pair.client, 0));
    EXPECT_TRUE(WaitForQueuedSendBytes(*pair.server, 0));
}

TEST(TcpDriverTest, RemoteAcceptedCompletesOnlyAfterExplicitConfirmation) {
    DriverPair pair = ConnectPair();
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);
    ASSERT_NE(pair.client_connection.id, kInvalidConnectionId);
    ASSERT_NE(pair.server_connection.id, kInvalidConnectionId);
    EXPECT_NE(pair.listener.id, pair.server_connection.id);
    EXPECT_TRUE(pair.client->capabilities().features.Has(
        Capability::kRemoteAcceptedConfirmation));

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

    auto before_ack = pair.client->PollCompletions(
        {.max_completions = 1, .timeout_ms = 50});
    ASSERT_FALSE(before_ack.ok());
    EXPECT_EQ(before_ack.status().code(), StatusCode::kTimeout);

    EXPECT_TRUE(pair.client->ConfirmRemoteAccepted(sent->operation).ok());
    EXPECT_EQ(pair.client->ConfirmRemoteAccepted(sent->operation).code(),
              StatusCode::kNotFound);
    SendOperation unknown = sent->operation;
    ++unknown.id;
    EXPECT_EQ(pair.client->ConfirmRemoteAccepted(unknown).code(),
              StatusCode::kNotFound);

    auto completed = pair.client->PollCompletions(
        {.max_completions = 1, .timeout_ms = 1000});
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
    ASSERT_EQ(completed->completions.size(), 1u);
    EXPECT_EQ(completed->completions[0].operation, sent->operation);
    EXPECT_EQ(completed->completions[0].reached_stage,
              DeliveryStage::kRemoteAccepted);
    EXPECT_TRUE(completed->completions[0].status.ok());
    EXPECT_EQ(pair.client->ConfirmRemoteAccepted(sent->operation).code(),
              StatusCode::kNotFound);
}

TEST(TcpDriverTest, ConnectionFiltersPreserveOtherMessagesAndCompletions) {
    DriverPair pair = ConnectPair();
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    auto second_client = pair.client->Connect({
        .remote_endpoint = *pair.listener.local_endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(second_client.ok()) << second_client.status().ToString();
    auto second_server = pair.server->Accept({
        .listener_id = pair.listener.id,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(second_server.ok()) << second_server.status().ToString();

    const std::vector<std::byte> first = FrameBody(40, 101);
    const std::vector<std::byte> second = FrameBody(48, 102);
    auto first_send = pair.client->Send({
        .connection_id = pair.client_connection.id,
        .payload = first,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    auto second_send = pair.client->Send({
        .connection_id = second_client->id,
        .payload = second,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_TRUE(first_send.ok()) << first_send.status().ToString();
    ASSERT_TRUE(second_send.ok()) << second_send.status().ToString();

    auto second_received = pair.server->Poll({
        .max_messages = 2,
        .max_bytes = 4096,
        .timeout_ms = 1000,
        .connection_id = second_server->id,
    });
    ASSERT_TRUE(second_received.ok()) << second_received.status().ToString();
    ASSERT_EQ(second_received->messages.size(), 1u);
    EXPECT_EQ(second_received->messages[0].connection_id, second_server->id);
    EXPECT_EQ(second_received->messages[0].payload, second);
    EXPECT_EQ(pair.server->Poll({
                  .max_messages = 1,
                  .max_bytes = 4096,
                  .timeout_ms = 0,
                  .connection_id = second_server->id,
              }).status().code(),
              StatusCode::kWouldBlock);

    auto first_received = pair.server->Poll({
        .max_messages = 1,
        .max_bytes = 4096,
        .timeout_ms = 1000,
        .connection_id = pair.server_connection.id,
    });
    ASSERT_TRUE(first_received.ok()) << first_received.status().ToString();
    ASSERT_EQ(first_received->messages.size(), 1u);
    EXPECT_EQ(first_received->messages[0].connection_id,
              pair.server_connection.id);
    EXPECT_EQ(first_received->messages[0].payload, first);

    ASSERT_TRUE(pair.client->ConfirmRemoteAccepted(first_send->operation).ok());
    ASSERT_TRUE(pair.client->ConfirmRemoteAccepted(second_send->operation).ok());
    auto second_completion = pair.client->PollCompletions({
        .max_completions = 2,
        .timeout_ms = 0,
        .connection_id = second_client->id,
    });
    ASSERT_TRUE(second_completion.ok())
        << second_completion.status().ToString();
    ASSERT_EQ(second_completion->completions.size(), 1u);
    EXPECT_EQ(second_completion->completions[0].operation,
              second_send->operation);
    EXPECT_EQ(pair.client->PollCompletions({
                  .max_completions = 1,
                  .timeout_ms = 0,
                  .connection_id = second_client->id,
              }).status().code(),
              StatusCode::kWouldBlock);

    auto first_completion = pair.client->PollCompletions({
        .max_completions = 1,
        .timeout_ms = 0,
        .connection_id = pair.client_connection.id,
    });
    ASSERT_TRUE(first_completion.ok())
        << first_completion.status().ToString();
    ASSERT_EQ(first_completion->completions.size(), 1u);
    EXPECT_EQ(first_completion->completions[0].operation,
              first_send->operation);
}

TEST(TcpDriverTest, IndexedReadyQueuePreservesInterleavedGlobalOrder) {
    DriverPair pair = ConnectPair();
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    auto second_client = pair.client->Connect({
        .remote_endpoint = *pair.listener.local_endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(second_client.ok()) << second_client.status().ToString();
    auto second_server = pair.server->Accept({
        .listener_id = pair.listener.id,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(second_server.ok()) << second_server.status().ToString();

    const std::vector<std::byte> first_a = FrameBody(32, 201);
    const std::vector<std::byte> first_b = FrameBody(32, 202);
    const std::vector<std::byte> second_a = FrameBody(32, 203);
    const std::vector<std::byte> second_b = FrameBody(32, 204);
    const auto send_and_wait = [&](ConnectionId connection_id,
                                   const std::vector<std::byte>& body,
                                   size_t ready_count) {
        auto sent = pair.client->SendUntracked({
            .connection_id = connection_id,
            .payload = body,
            .traffic_class = UntrackedTrafficClass::kData,
        });
        EXPECT_TRUE(sent.ok()) << sent.status().ToString();
        EXPECT_TRUE(WaitForReadyMessageCount(*pair.server, ready_count));
    };
    send_and_wait(pair.client_connection.id, first_a, 1);
    send_and_wait(second_client->id, first_b, 2);
    send_and_wait(pair.client_connection.id, second_a, 3);
    send_and_wait(second_client->id, second_b, 4);

    auto filtered = pair.server->Poll({
        .max_messages = 1,
        .max_bytes = 4096,
        .timeout_ms = 0,
        .connection_id = second_server->id,
    });
    ASSERT_TRUE(filtered.ok()) << filtered.status().ToString();
    ASSERT_EQ(filtered->messages.size(), 1u);
    EXPECT_EQ(filtered->messages[0].payload, first_b);
    EXPECT_EQ(pair.server->stats().ready_receive_messages, 3u);

    auto remaining = pair.server->Poll({
        .max_messages = 3,
        .max_bytes = 4096,
        .timeout_ms = 0,
    });
    ASSERT_TRUE(remaining.ok()) << remaining.status().ToString();
    ASSERT_EQ(remaining->messages.size(), 3u);
    EXPECT_EQ(remaining->messages[0].payload, first_a);
    EXPECT_EQ(remaining->messages[1].payload, second_a);
    EXPECT_EQ(remaining->messages[2].payload, second_b);
    EXPECT_EQ(pair.server->stats().ready_receive_messages, 0u);
    EXPECT_EQ(pair.server->stats().ready_receive_bytes, 0u);
}

TEST(TcpDriverTest, IndexedReadyQueuePreservesOversizedHeadAndByteCutoff) {
    DriverPair pair = ConnectPair();
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    const std::vector<std::byte> first = FrameBody(256, 211);
    const std::vector<std::byte> second = FrameBody(64, 212);
    ASSERT_TRUE(pair.client->SendUntracked({
        .connection_id = pair.client_connection.id,
        .payload = first,
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
    ASSERT_TRUE(pair.client->SendUntracked({
        .connection_id = pair.client_connection.id,
        .payload = second,
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
    ASSERT_TRUE(WaitForReadyMessageCount(*pair.server, 2));

    const TcpDriverStats before = pair.server->stats();
    auto oversized = pair.server->Poll({
        .max_messages = 2,
        .max_bytes = static_cast<uint32_t>(first.size() - 1),
        .timeout_ms = 0,
        .connection_id = pair.server_connection.id,
    });
    ASSERT_FALSE(oversized.ok());
    EXPECT_EQ(oversized.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(pair.server->stats().ready_receive_messages,
              before.ready_receive_messages);
    EXPECT_EQ(pair.server->stats().ready_receive_bytes,
              before.ready_receive_bytes);

    auto cutoff = pair.server->Poll({
        .max_messages = 2,
        .max_bytes = static_cast<uint32_t>(first.size() + second.size() - 1),
        .timeout_ms = 0,
        .connection_id = pair.server_connection.id,
    });
    ASSERT_TRUE(cutoff.ok()) << cutoff.status().ToString();
    ASSERT_EQ(cutoff->messages.size(), 1u);
    EXPECT_EQ(cutoff->messages[0].payload, first);

    auto tail = pair.server->Poll({
        .max_messages = 1,
        .max_bytes = 4096,
        .timeout_ms = 0,
        .connection_id = pair.server_connection.id,
    });
    ASSERT_TRUE(tail.ok()) << tail.status().ToString();
    ASSERT_EQ(tail->messages.size(), 1u);
    EXPECT_EQ(tail->messages[0].payload, second);
}

TEST(TcpDriverTest, IndexedReadyQueueCompactsBoundedTombstoneStorage) {
    TcpDriverOptions options = TestOptions();
    options.max_ready_receive_messages = 8;
    DriverPair pair = ConnectPair(options);
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    auto second_client = pair.client->Connect({
        .remote_endpoint = *pair.listener.local_endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(second_client.ok()) << second_client.status().ToString();
    auto second_server = pair.server->Accept({
        .listener_id = pair.listener.id,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(second_server.ok()) << second_server.status().ToString();

    const std::vector<std::byte> pinned = FrameBody(32, 221);
    ASSERT_TRUE(pair.client->SendUntracked({
        .connection_id = pair.client_connection.id,
        .payload = pinned,
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
    ASSERT_TRUE(WaitForReadyMessageCount(*pair.server, 1));

    for (uint64_t iteration = 0; iteration < 64; ++iteration) {
        const std::vector<std::byte> transient =
            FrameBody(32, 222 + iteration);
        ASSERT_TRUE(pair.client->SendUntracked({
            .connection_id = second_client->id,
            .payload = transient,
            .traffic_class = UntrackedTrafficClass::kData,
        }).ok());
        ASSERT_TRUE(WaitForReadyMessageCount(*pair.server, 2));
        auto received = pair.server->Poll({
            .max_messages = 1,
            .max_bytes = 4096,
            .timeout_ms = 0,
            .connection_id = second_server->id,
        });
        ASSERT_TRUE(received.ok()) << received.status().ToString();
        ASSERT_EQ(received->messages.size(), 1u);
        EXPECT_EQ(received->messages[0].payload, transient);
        const TcpDriverStats stats = pair.server->stats();
        EXPECT_EQ(stats.ready_receive_messages, 1u);
        EXPECT_LE(stats.ready_receive_storage_slots,
                  options.max_ready_receive_messages);
    }

    auto remaining = pair.server->Poll({
        .max_messages = 1,
        .max_bytes = 4096,
        .timeout_ms = 0,
    });
    ASSERT_TRUE(remaining.ok()) << remaining.status().ToString();
    ASSERT_EQ(remaining->messages.size(), 1u);
    EXPECT_EQ(remaining->messages[0].payload, pinned);
}

TEST(TcpDriverTest, CloseDiscardsReadyMessagesForConnection) {
    DriverPair pair = ConnectPair();
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    const std::vector<std::byte> body = FrameBody(64, 103);
    ASSERT_TRUE(pair.client->SendUntracked({
        .connection_id = pair.client_connection.id,
        .payload = body,
    }).ok());
    ASSERT_TRUE(WaitForReadyMessageCount(*pair.server, 1));
    ASSERT_TRUE(pair.server->Close(pair.server_connection.id).ok());
    EXPECT_EQ(pair.server->stats().ready_receive_messages, 0u);
    EXPECT_EQ(pair.server->stats().ready_receive_bytes, 0u);
}

TEST(TcpDriverTest, CloseBeforeAckFailsTrackedOperation) {
    DriverPair pair = ConnectPair();
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    const std::vector<std::byte> body = FrameBody(128);
    auto sent = pair.client->Send({
        .connection_id = pair.client_connection.id,
        .payload = body,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();
    auto received = pair.server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();

    ASSERT_TRUE(pair.client->Close(pair.client_connection.id).ok());
    auto rejected = pair.client->SendUntracked({
        .connection_id = pair.client_connection.id,
        .payload = body,
        .traffic_class = UntrackedTrafficClass::kData,
    });
    EXPECT_FALSE(rejected.ok());
    auto failed = pair.client->PollCompletions(
        {.max_completions = 1, .timeout_ms = 1000});
    ASSERT_TRUE(failed.ok()) << failed.status().ToString();
    ASSERT_EQ(failed->completions.size(), 1u);
    EXPECT_EQ(failed->completions[0].operation, sent->operation);
    EXPECT_EQ(failed->completions[0].reached_stage,
              DeliveryStage::kLocalPublished);
    EXPECT_FALSE(failed->completions[0].status.ok());
}

TEST(TcpDriverTest, UntrackedSendProducesNoCompletion) {
    DriverPair pair = ConnectPair();
    ASSERT_NE(pair.server, nullptr);
    ASSERT_NE(pair.client, nullptr);

    const std::vector<std::byte> body = FrameBody(64);
    auto sent = pair.client->SendUntracked({
        .connection_id = pair.client_connection.id,
        .payload = body,
        .traffic_class = UntrackedTrafficClass::kData,
    });
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();
    EXPECT_EQ(*sent, body.size());

    auto received = pair.server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].payload, body);

    auto completion = pair.client->PollCompletions(
        {.max_completions = 1, .timeout_ms = 50});
    ASSERT_FALSE(completion.ok());
    EXPECT_EQ(completion.status().code(), StatusCode::kTimeout);
}

TEST(TcpDriverTest, ControlReserveWorksAtFullDataQuotaWithoutInterleaving) {
    const std::vector<std::byte> data = FrameBody(8u * 1024u * 1024u, 20);
    const std::vector<std::byte> control = FrameBody(32, 21);
    TcpDriverOptions options = TestOptions();
    options.max_frame_body_bytes = static_cast<uint32_t>(data.size());
    options.max_total_send_buffer_bytes = data.size() + 4;
    options.max_connection_send_buffer_bytes = data.size() + 4;
    options.max_control_send_buffer_bytes = data.size() + 4;
    options.max_control_send_messages = 1;
    options.max_ready_receive_bytes = data.size();
    options.heartbeat_interval_ms = 1000;
    options.idle_timeout_ms = 5000;
    options.partial_frame_timeout_ms = 5000;

    RawListener listener = ListenRaw();
    auto driver_result = TcpDriver::Create(options);
    ASSERT_TRUE(driver_result.ok()) << driver_result.status().ToString();
    std::unique_ptr<TcpDriver> driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());
    auto connected = driver->Connect({
        .remote_endpoint = listener.endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();
    ScopedFd peer = AcceptRaw(listener);
    ASSERT_GE(peer.get(), 0);

    auto tracked = driver->Send({
        .connection_id = connected->id,
        .payload = data,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_TRUE(tracked.ok()) << tracked.status().ToString();

    std::vector<std::byte> received_wire;
    ASSERT_TRUE(ReceiveUntil(peer.get(), &received_wire, 1));
    ASSERT_NE(driver->stats().queued_send_bytes, 0u);
    auto data_blocked = driver->Send({
        .connection_id = connected->id,
        .payload = data,
        .target_stage = DeliveryStage::kRemoteAccepted,
    });
    ASSERT_FALSE(data_blocked.ok());
    EXPECT_EQ(data_blocked.status().code(), StatusCode::kWouldBlock);

    auto best_effort_blocked = driver->SendUntracked({
        .connection_id = connected->id,
        .payload = control,
        .traffic_class = UntrackedTrafficClass::kData,
    });
    ASSERT_FALSE(best_effort_blocked.ok());
    EXPECT_EQ(best_effort_blocked.status().code(), StatusCode::kWouldBlock);

    auto control_sent = driver->SendUntracked({
        .connection_id = connected->id,
        .payload = control,
    });
    ASSERT_TRUE(control_sent.ok()) << control_sent.status().ToString();
    EXPECT_EQ(*control_sent, control.size());
    auto control_blocked = driver->SendUntracked({
        .connection_id = connected->id,
        .payload = control,
    });
    ASSERT_FALSE(control_blocked.ok());
    EXPECT_EQ(control_blocked.status().code(), StatusCode::kWouldBlock);

    std::vector<std::byte> expected_wire = Prefix(data);
    const std::vector<std::byte> control_wire = Prefix(control);
    expected_wire.insert(expected_wire.end(), control_wire.begin(),
                         control_wire.end());
    ASSERT_TRUE(ReceiveUntil(peer.get(), &received_wire,
                             expected_wire.size(), 5000ms));
    EXPECT_EQ(received_wire, expected_wire);
}

TEST(TcpDriverTest, GathersQueuedFramesAndPreservesAckCompletionSemantics) {
    TcpDriverOptions options = TestOptions();
    options.max_frame_body_bytes = 256 * 1024;
    options.max_total_send_buffer_bytes = 16 * 1024 * 1024;
    options.max_connection_send_buffer_bytes = 8 * 1024 * 1024;
    options.max_control_send_buffer_bytes = 256 * 1024 + 4;
    options.max_ready_receive_bytes = 16 * 1024 * 1024;
    options.heartbeat_interval_ms = 1000;
    options.idle_timeout_ms = 5000;
    options.partial_frame_timeout_ms = 5000;

    RawListener listener = ListenRaw(4096);
    auto driver_result = TcpDriver::Create(options);
    ASSERT_TRUE(driver_result.ok()) << driver_result.status().ToString();
    std::unique_ptr<TcpDriver> driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());
    auto connected = driver->Connect({
        .remote_endpoint = listener.endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();
    ScopedFd peer = AcceptRaw(listener);

    constexpr size_t kFrameCount = 24;
    std::vector<std::vector<std::byte>> bodies;
    std::vector<SendOperation> operations;
    std::vector<std::byte> expected_wire;
    bodies.reserve(kFrameCount);
    operations.reserve(kFrameCount);
    for (size_t index = 0; index < kFrameCount; ++index) {
        bodies.push_back(FrameBody(128 * 1024, 1000 + index));
        const std::vector<std::byte> wire = Prefix(bodies.back());
        expected_wire.insert(expected_wire.end(), wire.begin(), wire.end());
        auto sent = driver->Send({
            .connection_id = connected->id,
            .payload = bodies.back(),
            .target_stage = DeliveryStage::kRemoteAccepted,
        });
        ASSERT_TRUE(sent.ok()) << sent.status().ToString();
        operations.push_back(sent->operation);
    }

    std::vector<std::byte> received_wire;
    ASSERT_TRUE(ReceiveUntil(peer.get(), &received_wire, expected_wire.size(),
                             10s));
    EXPECT_EQ(received_wire, expected_wire);
    ASSERT_TRUE(WaitForQueuedSendBytes(*driver, 0));
    const TcpDriverStats stats = driver->stats();
    EXPECT_GT(stats.successful_send_syscalls, 0u);
    EXPECT_GT(stats.gathered_send_syscalls, 0u);
    EXPECT_GT(stats.gathered_send_buffers, stats.gathered_send_syscalls);
    EXPECT_GE(stats.sent_bytes, expected_wire.size());

    for (const SendOperation operation : operations) {
        ASSERT_TRUE(driver->ConfirmRemoteAccepted(operation).ok());
    }
    auto completions = driver->PollCompletions({
        .max_completions = static_cast<uint32_t>(operations.size()),
        .timeout_ms = 1000,
        .connection_id = connected->id,
    });
    ASSERT_TRUE(completions.ok()) << completions.status().ToString();
    ASSERT_EQ(completions->completions.size(), operations.size());
    for (size_t index = 0; index < operations.size(); ++index) {
        EXPECT_EQ(completions->completions[index].operation, operations[index]);
        EXPECT_TRUE(completions->completions[index].status.ok());
        EXPECT_EQ(completions->completions[index].reached_stage,
                  DeliveryStage::kRemoteAccepted);
    }
}

TEST(TcpDriverTest, WakePathSendsSmallMessagesWithoutWritableInterest) {
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 2000;
    options.idle_timeout_ms = 5000;
    options.partial_frame_timeout_ms = 5000;

    RawListener listener = ListenRaw();
    auto driver_result = TcpDriver::Create(options);
    ASSERT_TRUE(driver_result.ok()) << driver_result.status().ToString();
    std::unique_ptr<TcpDriver> driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());
    auto connected = driver->Connect({
        .remote_endpoint = listener.endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();
    ScopedFd peer = AcceptRaw(listener);

    const std::vector<std::byte> first = FrameBody(256, 2001);
    const std::vector<std::byte> first_wire = Prefix(first);
    ASSERT_TRUE(driver->SendUntracked({
        .connection_id = connected->id,
        .payload = first,
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
    std::vector<std::byte> received;
    ASSERT_TRUE(ReceiveUntil(peer.get(), &received, first_wire.size()));
    EXPECT_EQ(received, first_wire);
    ASSERT_TRUE(WaitForQueuedSendBytes(*driver, 0));

    const uint64_t sends_after_drain =
        driver->stats().successful_send_syscalls;
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(driver->stats().successful_send_syscalls, sends_after_drain);

    const std::vector<std::byte> second = FrameBody(256, 2002);
    const std::vector<std::byte> second_wire = Prefix(second);
    ASSERT_TRUE(driver->SendUntracked({
        .connection_id = connected->id,
        .payload = second,
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
    received.clear();
    ASSERT_TRUE(ReceiveUntil(peer.get(), &received, second_wire.size()));
    EXPECT_EQ(received, second_wire);
    EXPECT_TRUE(WaitForQueuedSendBytes(*driver, 0));
#if defined(__linux__)
    const int epoll_fd = FindProcessEpollFd();
    ASSERT_GE(epoll_fd, 0);
    EXPECT_TRUE(WaitForEpollEventMask(epoll_fd, EPOLLOUT, false));
#endif
}

TEST(TcpDriverTest, ConcurrentSendIngressPreservesPerProducerOrder) {
    constexpr size_t kProducerCount = 8;
    constexpr size_t kMessagesPerProducer = 64;

    TcpDriverOptions options = TestOptions();
    options.max_total_send_buffer_bytes = 4 * 1024 * 1024;
    options.max_connection_send_buffer_bytes = 4 * 1024 * 1024;
    options.heartbeat_interval_ms = 2000;
    options.idle_timeout_ms = 5000;
    options.partial_frame_timeout_ms = 5000;
    options.io_poll_max_ms = 1000;

    RawListener listener = ListenRaw(1024 * 1024);
    auto driver_result = TcpDriver::Create(options);
    ASSERT_TRUE(driver_result.ok()) << driver_result.status().ToString();
    std::unique_ptr<TcpDriver> driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());
    auto connected = driver->Connect({
        .remote_endpoint = listener.endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();
    ScopedFd peer = AcceptRaw(listener);

    std::vector<std::vector<std::vector<std::byte>>> bodies(kProducerCount);
    size_t expected_wire_bytes = 0;
    for (size_t producer = 0; producer < kProducerCount; ++producer) {
        bodies[producer].reserve(kMessagesPerProducer);
        for (size_t index = 0; index < kMessagesPerProducer; ++index) {
            const uint64_t sequence = producer * 1000 + index + 1;
            bodies[producer].push_back(FrameBody(32, sequence));
            expected_wire_bytes += bodies[producer].back().size() + 4;
        }
    }

    std::array<std::atomic<bool>, kProducerCount> succeeded{};
    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);
    for (size_t producer = 0; producer < kProducerCount; ++producer) {
        producers.emplace_back([&, producer] {
            bool ok = true;
            for (const std::vector<std::byte>& body : bodies[producer]) {
                if (!driver->SendUntracked({
                        .connection_id = connected->id,
                        .payload = body,
                        .traffic_class = UntrackedTrafficClass::kData,
                    }).ok()) {
                    ok = false;
                    break;
                }
            }
            succeeded[producer].store(ok, std::memory_order_release);
        });
    }
    for (std::thread& producer : producers) producer.join();
    for (const std::atomic<bool>& success : succeeded) {
        ASSERT_TRUE(success.load(std::memory_order_acquire));
    }

    std::vector<std::byte> received;
    ASSERT_TRUE(ReceiveUntil(peer.get(), &received, expected_wire_bytes, 3000ms));
    ASSERT_EQ(received.size(), expected_wire_bytes);
    std::array<size_t, kProducerCount> next_index{};
    size_t offset = 0;
    while (offset < received.size()) {
        ASSERT_LE(offset + 4, received.size());
        const uint32_t body_size =
            (static_cast<uint32_t>(std::to_integer<uint8_t>(received[offset]))
             << 24) |
            (static_cast<uint32_t>(
                 std::to_integer<uint8_t>(received[offset + 1]))
             << 16) |
            (static_cast<uint32_t>(
                 std::to_integer<uint8_t>(received[offset + 2]))
             << 8) |
            static_cast<uint32_t>(
                std::to_integer<uint8_t>(received[offset + 3]));
        ASSERT_LE(offset + 4 + body_size, received.size());
        auto decoded = bridge::WireFrameCodec::Decode(
            std::span<const std::byte>(received).subspan(offset + 4, body_size));
        ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
        const uint64_t sequence = (*decoded).header.sequence_num;
        const size_t producer = static_cast<size_t>(sequence / 1000);
        ASSERT_LT(producer, kProducerCount);
        EXPECT_EQ(sequence, producer * 1000 + next_index[producer] + 1);
        ++next_index[producer];
        offset += 4 + body_size;
    }
    for (size_t count : next_index) EXPECT_EQ(count, kMessagesPerProducer);
    EXPECT_TRUE(WaitForQueuedSendBytes(*driver, 0));
}

TEST(TcpDriverTest, WritableReadinessRecoversBlockedWriteAndDisablesAfterDrain) {
    const std::vector<std::byte> body = FrameBody(8u * 1024u * 1024u, 2100);
    const size_t wire_size = body.size() + 4;
    TcpDriverOptions options = TestOptions();
    options.max_frame_body_bytes = static_cast<uint32_t>(body.size());
    options.max_total_send_buffer_bytes = wire_size;
    options.max_connection_send_buffer_bytes = wire_size;
    options.max_control_send_buffer_bytes = wire_size;
    options.max_ready_receive_bytes = body.size();
    options.heartbeat_interval_ms = 2000;
    options.idle_timeout_ms = 5000;
    options.partial_frame_timeout_ms = 5000;

    RawListener listener = ListenRaw();
    auto driver_result = TcpDriver::Create(options);
    ASSERT_TRUE(driver_result.ok()) << driver_result.status().ToString();
    std::unique_ptr<TcpDriver> driver = std::move(*driver_result);
    ASSERT_TRUE(driver->Start(TestConfig()).ok());
    auto connected = driver->Connect({
        .remote_endpoint = listener.endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 1000,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();
    ScopedFd peer = AcceptRaw(listener);

    ASSERT_TRUE(driver->SendUntracked({
        .connection_id = connected->id,
        .payload = body,
        .traffic_class = UntrackedTrafficClass::kData,
    }).ok());
#if defined(__linux__)
    const int epoll_fd = FindProcessEpollFd();
    ASSERT_GE(epoll_fd, 0);
    ASSERT_TRUE(WaitForEpollEventMask(epoll_fd, EPOLLOUT, true, 3000ms));
#endif
    ASSERT_NE(driver->stats().queued_send_bytes, 0u);
    const uint64_t sends_while_blocked =
        driver->stats().successful_send_syscalls;
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(driver->stats().successful_send_syscalls, sends_while_blocked);

    std::vector<std::byte> received;
    ASSERT_TRUE(ReceiveUntil(peer.get(), &received, wire_size, 10s));
    EXPECT_EQ(received, Prefix(body));
    ASSERT_TRUE(WaitForQueuedSendBytes(*driver, 0));
#if defined(__linux__)
    ASSERT_TRUE(WaitForEpollEventMask(epoll_fd, EPOLLOUT, false));
#endif
    const uint64_t sends_after_drain =
        driver->stats().successful_send_syscalls;
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(driver->stats().successful_send_syscalls, sends_after_drain);
}

TEST(TcpDriverTest, StaleEventsDoNotAffectReusedConnectionDescriptors) {
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 1000;
    options.idle_timeout_ms = 5000;
    auto server_result = TcpDriver::Create(options);
    ASSERT_TRUE(server_result.ok()) << server_result.status().ToString();
    std::unique_ptr<TcpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 4});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();

    for (uint64_t iteration = 0; iteration < 24; ++iteration) {
        ScopedFd peer = ConnectRaw(endpoint);
        auto accepted = server->Accept(
            {.listener_id = listener->id, .timeout_ms = 1000});
        ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
        const std::vector<std::byte> body = FrameBody(32, 3000 + iteration);
        const std::vector<std::byte> wire = Prefix(body);
        ASSERT_EQ(SendRawNoSignal(peer.get(), wire),
                  static_cast<ssize_t>(wire.size()));
        auto received = server->Poll({
            .max_messages = 1,
            .max_bytes = 4096,
            .timeout_ms = 1000,
            .connection_id = accepted->id,
        });
        ASSERT_TRUE(received.ok()) << received.status().ToString();
        ASSERT_EQ(received->messages.size(), 1u);
        EXPECT_EQ(received->messages[0].payload, body);
        ASSERT_TRUE(server->Close(accepted->id).ok());
        ASSERT_TRUE(WaitForConnectionCount(*server, 0));
    }
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

TEST(TcpDriverTest, CapacityPausedReadAheadDoesNotPartialTimeout) {
    TcpDriverOptions options = TestOptions();
    options.max_ready_receive_messages = 1;
    options.partial_frame_timeout_ms = 50;
    options.heartbeat_interval_ms = 1000;
    options.idle_timeout_ms = 5000;
    options.io_poll_max_ms = 1000;

    auto server_result = TcpDriver::Create(options);
    ASSERT_TRUE(server_result.ok()) << server_result.status().ToString();
    std::unique_ptr<TcpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 2});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    ScopedFd peer = ConnectRaw(endpoint);
    auto accepted = server->Accept(
        {.listener_id = listener->id, .timeout_ms = 1000});
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();

    const std::vector<std::byte> first = FrameBody(64, 4101);
    const std::vector<std::byte> second = FrameBody(64, 4102);
    std::vector<std::byte> combined = Prefix(first);
    const std::vector<std::byte> second_wire = Prefix(second);
    combined.insert(combined.end(), second_wire.begin(), second_wire.end());
    ASSERT_EQ(SendRawNoSignal(peer.get(), combined),
              static_cast<ssize_t>(combined.size()));
    ASSERT_TRUE(WaitForReadyMessageCount(*server, 1));
    std::this_thread::sleep_for(100ms);

    auto first_received = server->Poll({
        .max_messages = 1,
        .max_bytes = 4096,
        .timeout_ms = 0,
        .connection_id = accepted->id,
    });
    ASSERT_TRUE(first_received.ok()) << first_received.status().ToString();
    ASSERT_EQ(first_received->messages.size(), 1u);
    EXPECT_EQ(first_received->messages[0].payload, first);

    auto second_received = server->Poll({
        .max_messages = 1,
        .max_bytes = 4096,
        .timeout_ms = 500,
        .connection_id = accepted->id,
    });
    ASSERT_TRUE(second_received.ok()) << second_received.status().ToString();
    ASSERT_EQ(second_received->messages.size(), 1u);
    EXPECT_EQ(second_received->messages[0].payload, second);
    EXPECT_EQ(server->stats().active_connections, 1u);
}

TEST(TcpDriverTest, NoncanonicalHeartbeatIsDeliveredToBridge) {
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 1000;
    options.idle_timeout_ms = 5000;
    auto server_result = TcpDriver::Create(options);
    ASSERT_TRUE(server_result.ok()) << server_result.status().ToString();
    std::unique_ptr<TcpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 2});
    ASSERT_TRUE(listener.ok());
    ScopedFd peer = ConnectRaw(endpoint);
    auto accepted = server->Accept(
        {.listener_id = listener->id, .timeout_ms = 1000});
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();

    const std::vector<std::byte> heartbeat = HeartbeatBody(1);
    const std::vector<std::byte> wire = Prefix(heartbeat);
    ASSERT_EQ(SendRawNoSignal(peer.get(), wire),
              static_cast<ssize_t>(wire.size()));
    auto received = server->Poll(
        {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 1000});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    ASSERT_EQ(received->messages.size(), 1u);
    EXPECT_EQ(received->messages[0].payload, heartbeat);
}

TEST(TcpDriverTest, MalformedBodiesReachBridgeButDoNotRefreshIdleTimeout) {
    TcpDriverOptions options = TestOptions();
    options.heartbeat_interval_ms = 10;
    options.idle_timeout_ms = 100;
    auto server_result = TcpDriver::Create(options);
    ASSERT_TRUE(server_result.ok()) << server_result.status().ToString();
    std::unique_ptr<TcpDriver> server = std::move(*server_result);
    ASSERT_TRUE(server->Start(TestConfig()).ok());
    const EndpointDescriptor endpoint = Loopback(FindUnusedLoopbackPort());
    auto listener = server->Listen({.local_endpoint = endpoint, .backlog = 2});
    ASSERT_TRUE(listener.ok());
    ScopedFd peer = ConnectRaw(endpoint);
    auto accepted = server->Accept(
        {.listener_id = listener->id, .timeout_ms = 1000});
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();

    const std::vector<std::byte> malformed(
        bridge::kWireBaseHeaderLength, std::byte{0});
    const std::vector<std::byte> wire = Prefix(malformed);
    size_t delivered = 0;
    const auto deadline = std::chrono::steady_clock::now() + 400ms;
    while (server->stats().active_connections != 0 &&
           std::chrono::steady_clock::now() < deadline) {
        if (SendRawNoSignal(peer.get(), wire) < 0) break;
        auto received = server->Poll(
            {.max_messages = 1, .max_bytes = 4096, .timeout_ms = 20});
        if (received.ok()) {
            ASSERT_EQ(received->messages.size(), 1u);
            EXPECT_EQ(received->messages[0].payload, malformed);
            ++delivered;
        } else {
            EXPECT_TRUE(received.status().code() == StatusCode::kTimeout ||
                        received.status().code() == StatusCode::kWouldBlock);
        }
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_GT(delivered, 0u);
    EXPECT_TRUE(WaitForConnectionCount(*server, 0, 500ms));
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
