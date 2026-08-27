// Copyright 2026 The Mino Authors

#include "mino/transport/tcp_driver.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#if defined(__linux__)
#define MINO_TCP_USE_EPOLL 1
#include <sys/epoll.h>
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || \
    defined(__OpenBSD__) || defined(__DragonFly__)
#define MINO_TCP_USE_KQUEUE 1
#include <sys/event.h>
#include <sys/time.h>
#endif
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cassert>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mino/bridge/wire_frame.h"

namespace mino::transport {
namespace {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

constexpr size_t kTcpPrefixBytes = 4;
constexpr size_t kTcpReadChunkBytes = 64u * 1024u;
constexpr size_t kIoBudgetBytes = 256u * 1024u;
constexpr size_t kTlsRecordPlaintextBytes = 4u * 1024u;
constexpr size_t kMaxGatheredWriteBuffers = 64;

#if defined(MINO_TCP_USE_KQUEUE)
using KqueueUserData =
    decltype(static_cast<struct kevent*>(nullptr)->udata);
using KqueueFilter = decltype(static_cast<struct kevent*>(nullptr)->filter);
using KqueueFlags = decltype(static_cast<struct kevent*>(nullptr)->flags);

template <typename UserData>
UserData EncodeKqueueToken(uint64_t token) noexcept {
    if constexpr (std::is_pointer_v<UserData>) {
        return reinterpret_cast<UserData>(static_cast<uintptr_t>(token));
    } else {
        return static_cast<UserData>(token);
    }
}

template <typename UserData>
uint64_t DecodeKqueueToken(UserData data) noexcept {
    if constexpr (std::is_pointer_v<UserData>) {
        return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(data));
    } else {
        return static_cast<uint64_t>(data);
    }
}
#endif

Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(const char* message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status WouldBlock(const char* message) {
    return Status::Error(StatusCode::kWouldBlock, message);
}

Status Timeout(const char* message) {
    return Status::Error(StatusCode::kTimeout, message);
}

Status Unavailable(const char* message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

Status Corruption(const char* message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Internal(const char* message) {
    return Status::Error(StatusCode::kInternal, message);
}

Status AllocationFailure() {
    return Exhausted("TCP driver allocation failed");
}

void StoreBe32(uint32_t value, std::span<std::byte> output) noexcept {
    output[0] = static_cast<std::byte>(value >> 24);
    output[1] = static_cast<std::byte>(value >> 16);
    output[2] = static_cast<std::byte>(value >> 8);
    output[3] = static_cast<std::byte>(value);
}

uint16_t LoadBe16(std::span<const std::byte> input) noexcept {
    return static_cast<uint16_t>(
        (static_cast<uint16_t>(std::to_integer<uint8_t>(input[0])) << 8) |
        static_cast<uint16_t>(std::to_integer<uint8_t>(input[1])));
}

uint32_t LoadBe32(std::span<const std::byte> input) noexcept {
    return (static_cast<uint32_t>(std::to_integer<uint8_t>(input[0])) << 24) |
           (static_cast<uint32_t>(std::to_integer<uint8_t>(input[1])) << 16) |
           (static_cast<uint32_t>(std::to_integer<uint8_t>(input[2])) << 8) |
           static_cast<uint32_t>(std::to_integer<uint8_t>(input[3]));
}

class UniqueFd final {
public:
    explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&& other) noexcept : fd_(other.release()) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) reset(other.release());
        return *this;
    }
    ~UniqueFd() { reset(); }

    int get() const noexcept { return fd_; }
    int release() noexcept {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }
    void reset(int fd = -1) noexcept {
        if (fd_ >= 0) (void)::close(fd_);
        fd_ = fd;
    }

private:
    int fd_;
};

Status SetNonBlockingAndCloseOnExec(int fd) {
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0 ||
        ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0) {
        return Internal("failed to make TCP descriptor non-blocking");
    }
    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    if (descriptor_flags < 0 ||
        ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
        return Internal("failed to make TCP descriptor close-on-exec");
    }
    return Status::Ok();
}

Status ConfigureTcpSocket(int fd) {
    MINO_RETURN_IF_ERROR(SetNonBlockingAndCloseOnExec(fd));
    const int enabled = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                     sizeof(enabled)) != 0 ||
        ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enabled,
                     sizeof(enabled)) != 0) {
        return Internal("failed to configure TCP socket options");
    }
#if defined(SO_NOSIGPIPE)
    if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
                     sizeof(enabled)) != 0) {
        return Internal("failed to disable SIGPIPE for TCP socket");
    }
#endif
    return Status::Ok();
}

ssize_t SendNoSignal(int fd, const void* data, size_t size) noexcept {
#if defined(MSG_NOSIGNAL)
    return ::send(fd, data, size, MSG_NOSIGNAL);
#else
    return ::send(fd, data, size, 0);
#endif
}

ssize_t SendMessageNoSignal(int fd, std::span<iovec> buffers) noexcept {
    msghdr message{};
    message.msg_iov = buffers.data();
    message.msg_iovlen = buffers.size();
#if defined(MSG_NOSIGNAL)
    return ::sendmsg(fd, &message, MSG_NOSIGNAL);
#else
    return ::sendmsg(fd, &message, 0);
#endif
}

struct SocketAddress {
    sockaddr_storage storage{};
    socklen_t size = 0;
};

Result<SocketAddress> ToSocketAddress(const EndpointDescriptor& endpoint) {
    if (endpoint.kind() != TransportKind::kNetwork ||
        endpoint.protocol() != NetworkProtocol::kTcp) {
        return Invalid("TCP driver requires a network TCP endpoint");
    }
    SocketAddress result;
    if (endpoint.address_family() == EndpointAddressFamily::kIpv4) {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(endpoint.port());
        const auto bytes = endpoint.ip_address();
        if (bytes.size() != sizeof(address.sin_addr)) {
            return Invalid("IPv4 endpoint size is invalid");
        }
        std::memcpy(&address.sin_addr, bytes.data(), bytes.size());
        std::memcpy(&result.storage, &address, sizeof(address));
        result.size = sizeof(address);
        return result;
    }
    if (endpoint.address_family() == EndpointAddressFamily::kIpv6) {
        sockaddr_in6 address{};
        address.sin6_family = AF_INET6;
        address.sin6_port = htons(endpoint.port());
        const auto bytes = endpoint.ip_address();
        if (bytes.size() != sizeof(address.sin6_addr)) {
            return Invalid("IPv6 endpoint size is invalid");
        }
        std::memcpy(&address.sin6_addr, bytes.data(), bytes.size());
        std::memcpy(&result.storage, &address, sizeof(address));
        result.size = sizeof(address);
        return result;
    }
    return Invalid("TCP endpoint address family is unsupported");
}

Result<EndpointDescriptor> FromSocketAddress(const sockaddr_storage& storage,
                                             socklen_t size) {
    if (storage.ss_family == AF_INET && size >= sizeof(sockaddr_in)) {
        const auto* address = reinterpret_cast<const sockaddr_in*>(&storage);
        const auto bytes = std::as_bytes(
            std::span(&address->sin_addr, static_cast<size_t>(1)));
        return EndpointDescriptor::Ipv4Tcp(bytes, ntohs(address->sin_port));
    }
    if (storage.ss_family == AF_INET6 && size >= sizeof(sockaddr_in6)) {
        const auto* address = reinterpret_cast<const sockaddr_in6*>(&storage);
        const auto bytes = std::as_bytes(
            std::span(&address->sin6_addr, static_cast<size_t>(1)));
        return EndpointDescriptor::Ipv6Tcp(bytes, ntohs(address->sin6_port));
    }
    return Status::Error(StatusCode::kUnsupported,
                         "socket address family is unsupported");
}

int SocketFamily(const EndpointDescriptor& endpoint) noexcept {
    return endpoint.address_family() == EndpointAddressFamily::kIpv4
               ? AF_INET
           : endpoint.address_family() == EndpointAddressFamily::kIpv6 ? AF_INET6
                                                                        : -1;
}

std::vector<std::byte> PrefixFrame(std::span<const std::byte> frame_body) {
    std::vector<std::byte> wire(kTcpPrefixBytes + frame_body.size());
    StoreBe32(static_cast<uint32_t>(frame_body.size()), wire);
    std::copy(frame_body.begin(), frame_body.end(),
              wire.begin() + static_cast<ptrdiff_t>(kTcpPrefixBytes));
    return wire;
}

Result<std::vector<std::byte>> BuildHeartbeatWire(
    const TcpDriverOptions& options) {
    bridge::WireFrame heartbeat;
    heartbeat.header.frame_type = bridge::FrameType::kHeartbeat;
    heartbeat.header.flags = bridge::FlagValue(bridge::FrameFlag::kControlFrame);
    bridge::WireFrameLimits limits;
    limits.max_payload_length = options.max_frame_body_bytes;
    limits.max_buffered_bytes =
        static_cast<size_t>(options.max_frame_body_bytes) + kTcpPrefixBytes;
    MINO_ASSIGN_OR_RETURN(auto body,
                          bridge::WireFrameCodec::Encode(heartbeat, limits));
    return PrefixFrame(body);
}

bool IsKnownControlOpcode(uint32_t opcode) noexcept {
    switch (static_cast<bridge::FrameType>(opcode)) {
        case bridge::FrameType::kSchemaAnnounce:
        case bridge::FrameType::kSchemaRequest:
        case bridge::FrameType::kAck:
        case bridge::FrameType::kHeartbeat:
        case bridge::FrameType::kSessionHello:
        case bridge::FrameType::kSessionDiscovery:
            return true;
        case bridge::FrameType::kData:
            return false;
    }
    return false;
}

// This deliberately omits CRC and payload validation. It is only used to decide
// whether a non-heartbeat frame may refresh the transport idle timer; Bridge
// remains the sole full WireFrame decoder for every body delivered upstream.
bool IsMinimallyStructuredWireFrame(
    std::span<const std::byte> body) noexcept {
    constexpr size_t kVersionOffset = 4;
    constexpr size_t kFlagsOffset = 6;
    constexpr size_t kHeaderLengthOffset = 8;
    constexpr size_t kPayloadLengthOffset = 72;
    if (body.size() < bridge::kWireBaseHeaderLength ||
        LoadBe32(body.first<4>()) != bridge::kWireFrameMagic ||
        LoadBe16(body.subspan<kVersionOffset, 2>()) !=
            bridge::kWireProtocolVersion) {
        return false;
    }

    const uint16_t flags = LoadBe16(body.subspan<kFlagsOffset, 2>());
    if ((flags & ~bridge::kKnownFrameFlags) != 0 ||
        bridge::HasFrameFlag(flags, bridge::FrameFlag::kAeadPresent)) {
        return false;
    }
    uint32_t canonical_header_length = bridge::kWireBaseHeaderLength;
    if (bridge::HasFrameFlag(flags, bridge::FrameFlag::kPayloadCrcPresent)) {
        canonical_header_length += bridge::kWirePayloadCrcLength;
    }
    if (bridge::HasFrameFlag(flags, bridge::FrameFlag::kPerfTraceSampled)) {
        canonical_header_length += bridge::kWirePerfTraceContextLength;
    }
    const uint32_t header_length =
        LoadBe32(body.subspan<kHeaderLengthOffset, 4>());
    const uint32_t payload_length =
        LoadBe32(body.subspan<kPayloadLengthOffset, 4>());
    if (header_length != canonical_header_length ||
        static_cast<uint64_t>(header_length) + payload_length != body.size()) {
        return false;
    }

    const bool is_control =
        bridge::HasFrameFlag(flags, bridge::FrameFlag::kControlFrame);
    if (!is_control) return true;
    return payload_length >= bridge::kWireControlOpcodeLength &&
           IsKnownControlOpcode(LoadBe32(body.subspan(header_length, 4)));
}

}  // namespace

const char* TcpDriverReadinessBackendForTest() noexcept {
#if defined(MINO_TCP_USE_EPOLL)
    return "epoll";
#elif defined(MINO_TCP_USE_KQUEUE)
    return "kqueue";
#else
    return "poll";
#endif
}

Status ValidateTcpDriverOptions(const TcpDriverOptions& options) {
    if (options.max_frame_body_bytes < bridge::kWireBaseHeaderLength ||
        options.max_frame_body_bytes > kMaxPayloadBytes) {
        return Invalid("TCP frame-body bound is invalid");
    }
    const size_t minimum_wire =
        static_cast<size_t>(options.max_frame_body_bytes) + kTcpPrefixBytes;
    if (options.max_total_send_buffer_bytes < minimum_wire ||
        options.max_total_send_buffer_bytes > kMaxPayloadBytes + kTcpPrefixBytes ||
        options.max_connection_send_buffer_bytes < minimum_wire ||
        options.max_connection_send_buffer_bytes >
            options.max_total_send_buffer_bytes ||
        options.max_control_send_buffer_bytes < minimum_wire ||
        options.max_control_send_buffer_bytes >
            kMaxPayloadBytes + kTcpPrefixBytes ||
        options.max_ready_receive_bytes < options.max_frame_body_bytes ||
        options.max_ready_receive_bytes > kMaxReceiveBatchBytes) {
        return Invalid("TCP send or receive byte bounds are inconsistent");
    }
    if (options.max_control_send_messages == 0 ||
        options.max_control_send_messages > kMaxQueuedSends ||
        options.max_ready_receive_messages == 0 ||
        options.max_ready_receive_messages > kMaxQueuedSends ||
        options.max_pending_accepts == 0 ||
        options.max_pending_accepts > kMaxConnections ||
        options.max_receive_frames_per_turn == 0 ||
        options.max_receive_frames_per_turn > kMaxReceiveBatchMessages ||
        options.max_receive_bytes_per_turn == 0 ||
        options.max_receive_bytes_per_turn > kMaxReceiveBatchBytes) {
        return Invalid("TCP message, receive-turn, or accept bounds are invalid");
    }
    if (options.heartbeat_interval_ms == 0 || options.idle_timeout_ms == 0 ||
        options.partial_frame_timeout_ms == 0 ||
        options.tls_handshake_timeout_ms == 0 || options.io_poll_max_ms == 0 ||
        options.heartbeat_interval_ms >= options.idle_timeout_ms ||
        options.idle_timeout_ms > kMaxOperationTimeoutMs ||
        options.partial_frame_timeout_ms > kMaxOperationTimeoutMs ||
        options.tls_handshake_timeout_ms > kMaxOperationTimeoutMs ||
        options.io_poll_max_ms > 1000) {
        return Invalid("TCP heartbeat, idle, or poll timing is invalid");
    }
    return Status::Ok();
}

class TcpDriver::Impl final {
public:
    Impl(TcpDriverOptions options, std::atomic<HealthState>* health,
         std::vector<std::byte> heartbeat_wire)
        : options_(options),
          health_(health),
          heartbeat_wire_(std::move(heartbeat_wire)) {}

    ~Impl() { StopAndJoin(); }

    Status Start(const DriverConfig& config) {
        std::lock_guard lock(mutex_);
        if (worker_.joinable()) {
            return Status::Error(StatusCode::kAlreadyExists);
        }
        int pipe_fds[2] = {-1, -1};
        if (::pipe(pipe_fds) != 0) {
            return Internal("failed to create TCP driver wake pipe");
        }
        UniqueFd read_end(pipe_fds[0]);
        UniqueFd write_end(pipe_fds[1]);
        Status status = SetNonBlockingAndCloseOnExec(read_end.get());
        if (status.ok()) {
            status = SetNonBlockingAndCloseOnExec(write_end.get());
        }
        if (!status.ok()) return status;

        config_ = config;
        stop_requested_.store(false, std::memory_order_release);
        wake_pending_.store(false, std::memory_order_release);
        receive_capacity_blocked_.store(false, std::memory_order_release);
        worker_failed_.store(false, std::memory_order_release);
        wake_read_fd_ = read_end.release();
        wake_write_fd_ = write_end.release();
        health_->store(HealthState::kHealthy, std::memory_order_release);
        try {
            worker_ = std::thread([this] { WorkerLoop(); });
        } catch (const std::system_error&) {
            CloseWakePipeLocked();
            health_->store(HealthState::kUnavailable,
                           std::memory_order_release);
            return Internal("failed to start TCP I/O worker");
        }
        return Status::Ok();
    }

    void RequestStop() noexcept {
        stop_requested_.store(true, std::memory_order_release);
        {
            // Synchronize with each predicate-to-wait transition so the stop
            // notifications cannot be lost.
            std::lock_guard lock(mutex_);
        }
        {
            std::lock_guard receive_lock(receive_mutex_);
        }
        Wake();
        receive_cv_.notify_all();
        completion_cv_.notify_all();
        accept_cv_.notify_all();
    }

    Status StopAndJoin() noexcept {
        RequestStop();
        if (worker_.joinable() &&
            worker_.get_id() != std::this_thread::get_id()) {
            worker_.join();
        }
        std::lock_guard lock(mutex_);
        for (auto& [id, connection] : connections_) {
            (void)id;
            if (connection.fd >= 0) (void)::close(connection.fd);
        }
        for (auto& [id, listener] : listeners_) {
            (void)id;
            if (listener.fd >= 0) (void)::close(listener.fd);
        }
        connections_.clear();
        listeners_.clear();
        accepted_.clear();
        completions_.clear();
        recently_closed_.clear();
        {
            std::lock_guard receive_lock(receive_mutex_);
            ready_messages_by_connection_.clear();
            ready_messages_.clear();
            ready_receive_messages_ = 0;
            ready_receive_bytes_ = 0;
            reserved_receive_bytes_ = 0;
            reserved_receive_messages_ = 0;
        }
        {
            std::lock_guard send_lock(send_ingress_mutex_);
            send_admission_.clear();
            total_data_send_bytes_ = 0;
            total_control_send_bytes_ = 0;
            total_control_send_messages_ = 0;
        }
        CloseWakePipeLocked();
        health_->store(HealthState::kUnavailable, std::memory_order_release);
        return Status::Ok();
    }

    Result<ConnectionInfo> Connect(const ConnectRequest& request) {
        MINO_ASSIGN_OR_RETURN(const SocketAddress remote,
                              ToSocketAddress(request.remote_endpoint));
        const int family = SocketFamily(request.remote_endpoint);
        if (family < 0) return Invalid("TCP connect family is invalid");
        UniqueFd socket_fd(::socket(family, SOCK_STREAM, IPPROTO_TCP));
        if (socket_fd.get() < 0) {
            return Unavailable("failed to create TCP socket");
        }
        MINO_RETURN_IF_ERROR(ConfigureTcpSocket(socket_fd.get()));

        if (request.local_bind.has_value()) {
            MINO_ASSIGN_OR_RETURN(const SocketAddress local,
                                  ToSocketAddress(*request.local_bind));
            if (::bind(socket_fd.get(),
                       reinterpret_cast<const sockaddr*>(&local.storage),
                       local.size) != 0) {
                return Unavailable("failed to bind TCP connect socket");
            }
        }

        int connected = ::connect(
            socket_fd.get(), reinterpret_cast<const sockaddr*>(&remote.storage),
            remote.size);
        if (connected != 0 && errno != EINPROGRESS) {
            return Unavailable("TCP connect failed");
        }
        if (connected != 0) {
            if (request.timeout_ms == 0) {
                return WouldBlock("TCP connect is in progress");
            }
#if defined(MINO_TCP_USE_EPOLL)
            UniqueFd connect_epoll(::epoll_create1(EPOLL_CLOEXEC));
            if (connect_epoll.get() < 0) {
                return Unavailable("failed to create TCP connect epoll");
            }
            epoll_event interest{};
            interest.events = EPOLLOUT | EPOLLERR | EPOLLHUP;
            interest.data.fd = socket_fd.get();
            if (::epoll_ctl(connect_epoll.get(), EPOLL_CTL_ADD, socket_fd.get(),
                            &interest) != 0) {
                return Unavailable("failed to register TCP connect socket");
            }
#elif defined(MINO_TCP_USE_KQUEUE)
            UniqueFd connect_kqueue(::kqueue());
            if (connect_kqueue.get() < 0) {
                return Unavailable("failed to create TCP connect kqueue");
            }
            const int descriptor_flags =
                ::fcntl(connect_kqueue.get(), F_GETFD, 0);
            if (descriptor_flags < 0 ||
                ::fcntl(connect_kqueue.get(), F_SETFD,
                        descriptor_flags | FD_CLOEXEC) != 0) {
                return Internal("failed to make TCP connect kqueue close-on-exec");
            }
            struct kevent interest;
            EV_SET(&interest, static_cast<uintptr_t>(socket_fd.get()),
                   EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0,
                   EncodeKqueueToken<KqueueUserData>(0));
            int registered;
            do {
                registered = ::kevent(connect_kqueue.get(), &interest, 1,
                                      nullptr, 0, nullptr);
            } while (registered != 0 && errno == EINTR);
            if (registered != 0) {
                return Unavailable("failed to register TCP connect socket");
            }
#else
            pollfd descriptor{
                .fd = socket_fd.get(),
                .events = POLLOUT,
                .revents = 0,
            };
#endif
            const TimePoint deadline =
                Clock::now() + std::chrono::milliseconds(request.timeout_ms);
            for (;;) {
                if (stop_requested_.load(std::memory_order_acquire)) {
                    return Unavailable("TCP driver stopped during connect");
                }
                const TimePoint now = Clock::now();
                if (now >= deadline) return Timeout("TCP connect timed out");
                const auto remaining =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        deadline - now);
                const uint32_t poll_ms = std::max<uint32_t>(
                    1, std::min<uint32_t>(
                           options_.io_poll_max_ms,
                           static_cast<uint32_t>(remaining.count())));
#if defined(MINO_TCP_USE_EPOLL)
                epoll_event completion{};
                const int ready = ::epoll_wait(connect_epoll.get(), &completion,
                                               1, static_cast<int>(poll_ms));
                if (ready > 0) break;
                if (ready < 0 && errno != EINTR) {
                    return Unavailable("TCP connect epoll wait failed");
                }
#elif defined(MINO_TCP_USE_KQUEUE)
                const timespec timeout{
                    .tv_sec = static_cast<time_t>(poll_ms / 1000u),
                    .tv_nsec = static_cast<long>(poll_ms % 1000u) * 1'000'000L,
                };
                struct kevent completion;
                const int ready = ::kevent(connect_kqueue.get(), nullptr, 0,
                                           &completion, 1, &timeout);
                if (ready > 0) {
                    if ((completion.flags & EV_ERROR) != 0) {
                        return Unavailable("TCP connect kqueue event failed");
                    }
                    break;
                }
                if (ready < 0 && errno != EINTR) {
                    return Unavailable("TCP connect kqueue wait failed");
                }
#else
                descriptor.revents = 0;
                const int ready =
                    ::poll(&descriptor, 1, static_cast<int>(poll_ms));
                if (ready > 0) break;
                if (ready < 0 && errno != EINTR) {
                    return Unavailable("TCP connect poll failed");
                }
#endif
            }
            int socket_error = 0;
            socklen_t error_size = sizeof(socket_error);
            if (::getsockopt(socket_fd.get(), SOL_SOCKET, SO_ERROR,
                             &socket_error, &error_size) != 0 ||
                socket_error != 0) {
                return Unavailable("TCP connect completion failed");
            }
        }

        sockaddr_storage local_storage{};
        socklen_t local_size = sizeof(local_storage);
        if (::getsockname(socket_fd.get(),
                          reinterpret_cast<sockaddr*>(&local_storage),
                          &local_size) != 0) {
            return Internal("failed to query local TCP endpoint");
        }
        MINO_ASSIGN_OR_RETURN(auto local_endpoint,
                              FromSocketAddress(local_storage, local_size));
        std::unique_ptr<security::TlsChannel> tls;
        if (options_.tls_factory) {
            MINO_ASSIGN_OR_RETURN(
                tls, options_.tls_factory->Create(
                         socket_fd.get(), security::TlsRole::kClient));
        }

        std::lock_guard lock(mutex_);
        if (stop_requested_.load(std::memory_order_acquire)) {
            return Unavailable("TCP driver is stopping");
        }
        if (connections_.size() >= config_.max_connections) {
            return Exhausted("TCP connection limit reached");
        }
        MINO_ASSIGN_OR_RETURN(const ConnectionId id, NextIdLocked());
        const TimePoint now = Clock::now();
        Connection connection;
        connection.info = ConnectionInfo{
            .id = id,
            .kind = TransportKind::kNetwork,
            .is_listener = false,
            .local_endpoint = local_endpoint,
            .peer_endpoint = request.remote_endpoint,
        };
        connection.fd = socket_fd.release();
        connection.tls = std::move(tls);
        connection.tls_write_frame_credits = connection.tls ? 1 : 0;
        connection.tls_handshake_started = now;
        connection.last_valid_receive = now;
        connection.last_transmit = now;
        auto [inserted, was_inserted] =
            connections_.emplace(id, std::move(connection));
        (void)was_inserted;
        try {
            std::lock_guard send_lock(send_ingress_mutex_);
            send_admission_.emplace(id, SendAdmission{});
        } catch (...) {
            if (inserted->second.fd >= 0) (void)::close(inserted->second.fd);
            connections_.erase(inserted);
            throw;
        }
        Wake();
        return inserted->second.info;
    }

    Result<ConnectionInfo> Listen(const ListenRequest& request) {
        MINO_ASSIGN_OR_RETURN(const SocketAddress local,
                              ToSocketAddress(request.local_endpoint));
        const int family = SocketFamily(request.local_endpoint);
        if (family < 0) return Invalid("TCP listen family is invalid");
        UniqueFd socket_fd(::socket(family, SOCK_STREAM, IPPROTO_TCP));
        if (socket_fd.get() < 0) {
            return Unavailable("failed to create TCP listener");
        }
        MINO_RETURN_IF_ERROR(SetNonBlockingAndCloseOnExec(socket_fd.get()));
        const int enabled = 1;
        if (::setsockopt(socket_fd.get(), SOL_SOCKET, SO_REUSEADDR, &enabled,
                         sizeof(enabled)) != 0) {
            return Internal("failed to configure TCP listener");
        }
        if (::bind(socket_fd.get(),
                   reinterpret_cast<const sockaddr*>(&local.storage),
                   local.size) != 0 ||
            ::listen(socket_fd.get(), static_cast<int>(request.backlog)) != 0) {
            return Unavailable("failed to bind or listen on TCP endpoint");
        }

        std::lock_guard lock(mutex_);
        if (listeners_.size() >= config_.max_listeners) {
            return Exhausted("TCP listener limit reached");
        }
        for (const auto& [id, listener] : listeners_) {
            (void)id;
            if (listener.info.local_endpoint == request.local_endpoint) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "TCP endpoint is already listening");
            }
        }
        MINO_ASSIGN_OR_RETURN(const ConnectionId id, NextIdLocked());
        Listener listener;
        listener.info = ConnectionInfo{
            .id = id,
            .kind = TransportKind::kNetwork,
            .is_listener = true,
            .local_endpoint = request.local_endpoint,
            .peer_endpoint = std::nullopt,
        };
        listener.fd = socket_fd.release();
        listeners_.emplace(id, std::move(listener));
        Wake();
        return listeners_.at(id).info;
    }

    Result<ConnectionInfo> Accept(const AcceptRequest& request) {
        std::unique_lock lock(mutex_);
        auto find_accepted = [this, &request] {
            return std::find_if(
                accepted_.begin(), accepted_.end(),
                [&request](const AcceptedConnection& accepted) {
                    return accepted.listener_id == request.listener_id;
                });
        };
        if (!listeners_.contains(request.listener_id) &&
            find_accepted() == accepted_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "TCP listener does not exist");
        }
        auto found = find_accepted();
        if (found == accepted_.end()) {
            if (request.timeout_ms == 0) {
                return WouldBlock("no TCP connection is ready to accept");
            }
            const bool ready = accept_cv_.wait_for(
                lock, std::chrono::milliseconds(request.timeout_ms), [&] {
                    return stop_requested_.load(std::memory_order_acquire) ||
                           find_accepted() != accepted_.end() ||
                           !listeners_.contains(request.listener_id);
                });
            if (!ready) return Timeout("TCP accept timed out");
            if (stop_requested_.load(std::memory_order_acquire)) {
                return Unavailable("TCP driver is stopping");
            }
            found = find_accepted();
            if (found == accepted_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "TCP listener was closed");
            }
        }
        ConnectionInfo info = found->info;
        accepted_.erase(found);
        Wake();
        return info;
    }

    Result<SendResult> Send(const SendRequest& request,
                            SendOperation operation) {
        const size_t wire_size = kTcpPrefixBytes + request.payload.size();
        {
            std::lock_guard lock(send_ingress_mutex_);
            if (stop_requested_.load(std::memory_order_acquire)) {
                return Unavailable("TCP driver is stopping");
            }
            const auto found = send_admission_.find(request.connection_id);
            if (found == send_admission_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "TCP connection does not exist");
            }
            SendAdmission& admission = found->second;
            if (!admission.accepting) {
                return Unavailable("TCP connection is closing");
            }
            if (wire_size > options_.max_total_send_buffer_bytes -
                                total_data_send_bytes_ ||
                wire_size > options_.max_connection_send_buffer_bytes -
                                admission.queued_data_bytes) {
                return WouldBlock("TCP data send byte queue is full");
            }
            std::vector<std::byte> wire = PrefixFrame(request.payload);
            admission.data_writes.emplace_back();
            admission.data_writes.back().InitializeContiguous(
                std::move(wire), operation);
            admission.queued_data_bytes += wire_size;
            total_data_send_bytes_ += wire_size;
        }
        Wake();
        return SendResult{
            .operation = operation,
            .admitted_bytes = request.payload.size(),
        };
    }

    Result<SendResult> SendOwned(const SendRequest& request,
                                 std::vector<std::byte>&& payload,
                                 SendOperation operation) {
        const size_t payload_size = payload.size();
        const size_t wire_size = kTcpPrefixBytes + payload_size;
        {
            std::lock_guard lock(send_ingress_mutex_);
            if (stop_requested_.load(std::memory_order_acquire)) {
                return Unavailable("TCP driver is stopping");
            }
            const auto found = send_admission_.find(request.connection_id);
            if (found == send_admission_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "TCP connection does not exist");
            }
            SendAdmission& admission = found->second;
            if (!admission.accepting) {
                return Unavailable("TCP connection is closing");
            }
            if (wire_size > options_.max_total_send_buffer_bytes -
                                total_data_send_bytes_ ||
                wire_size > options_.max_connection_send_buffer_bytes -
                                admission.queued_data_bytes) {
                return WouldBlock("TCP data send byte queue is full");
            }
            // deque growth is the final fallible step. Only consume payload
            // after it succeeds so every failed admission preserves ownership.
            admission.data_writes.emplace_back();
            admission.data_writes.back().Initialize(std::move(payload), operation);
            admission.queued_data_bytes += wire_size;
            total_data_send_bytes_ += wire_size;
        }
        Wake();
        return SendResult{
            .operation = operation,
            .admitted_bytes = payload_size,
        };
    }

    Result<size_t> SendUntracked(const UntrackedSendRequest& request) {
        const size_t wire_size = kTcpPrefixBytes + request.payload.size();
        {
            std::lock_guard lock(send_ingress_mutex_);
            if (stop_requested_.load(std::memory_order_acquire)) {
                return Unavailable("TCP driver is stopping");
            }
            const auto found = send_admission_.find(request.connection_id);
            if (found == send_admission_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "TCP connection does not exist");
            }
            SendAdmission& admission = found->second;
            if (!admission.accepting) {
                return Unavailable("TCP connection is closing");
            }
            const bool is_control =
                request.traffic_class ==
                UntrackedTrafficClass::kProtocolControl;
            if (is_control) {
                if (wire_size > options_.max_control_send_buffer_bytes -
                                    total_control_send_bytes_ ||
                    total_control_send_messages_ >=
                        options_.max_control_send_messages) {
                    return WouldBlock("TCP control send queue is full");
                }
            } else if (wire_size > options_.max_total_send_buffer_bytes -
                                       total_data_send_bytes_ ||
                       wire_size > options_.max_connection_send_buffer_bytes -
                                       admission.queued_data_bytes) {
                return WouldBlock("TCP data send byte queue is full");
            }
            std::vector<std::byte> wire = PrefixFrame(request.payload);
            std::deque<PendingWrite>& writes =
                is_control ? admission.control_writes : admission.data_writes;
            writes.emplace_back();
            writes.back().InitializeContiguous(std::move(wire), {});
            if (is_control) {
                admission.queued_control_bytes += wire_size;
                ++admission.queued_control_messages;
                total_control_send_bytes_ += wire_size;
                ++total_control_send_messages_;
            } else {
                admission.queued_data_bytes += wire_size;
                total_data_send_bytes_ += wire_size;
            }
        }
        Wake();
        return request.payload.size();
    }

    Result<size_t> SendUntrackedOwned(
        const UntrackedSendRequest& request,
        std::vector<std::byte>&& payload) {
        const size_t payload_size = payload.size();
        const size_t wire_size = kTcpPrefixBytes + payload_size;
        {
            std::lock_guard lock(send_ingress_mutex_);
            if (stop_requested_.load(std::memory_order_acquire)) {
                return Unavailable("TCP driver is stopping");
            }
            const auto found = send_admission_.find(request.connection_id);
            if (found == send_admission_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "TCP connection does not exist");
            }
            SendAdmission& admission = found->second;
            if (!admission.accepting) {
                return Unavailable("TCP connection is closing");
            }
            const bool is_control =
                request.traffic_class ==
                UntrackedTrafficClass::kProtocolControl;
            if (is_control) {
                if (wire_size > options_.max_control_send_buffer_bytes -
                                    total_control_send_bytes_ ||
                    total_control_send_messages_ >=
                        options_.max_control_send_messages) {
                    return WouldBlock("TCP control send queue is full");
                }
            } else if (wire_size > options_.max_total_send_buffer_bytes -
                                       total_data_send_bytes_ ||
                       wire_size > options_.max_connection_send_buffer_bytes -
                                       admission.queued_data_bytes) {
                return WouldBlock("TCP data send byte queue is full");
            }
            std::deque<PendingWrite>& writes =
                is_control ? admission.control_writes : admission.data_writes;
            writes.emplace_back();
            writes.back().Initialize(std::move(payload), {});
            if (is_control) {
                admission.queued_control_bytes += wire_size;
                ++admission.queued_control_messages;
                total_control_send_bytes_ += wire_size;
                ++total_control_send_messages_;
            } else {
                admission.queued_data_bytes += wire_size;
                total_data_send_bytes_ += wire_size;
            }
        }
        Wake();
        return payload_size;
    }

    Status ConfirmRemoteAccepted(SendOperation operation) {
        std::lock_guard lock(mutex_);
        const auto found = connections_.find(operation.connection_id);
        if (found == connections_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "TCP connection does not exist");
        }
        auto pending = std::find(found->second.awaiting_ack.begin(),
                                 found->second.awaiting_ack.end(), operation);
        if (pending == found->second.awaiting_ack.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "TCP operation is not awaiting ACK");
        }
        completions_.push_back(DeliveryCompletion{
            .operation = operation,
            .reached_stage = DeliveryStage::kRemoteAccepted,
            .status = Status::Ok(),
        });
        found->second.awaiting_ack.erase(pending);
        completion_cv_.notify_all();
        return Status::Ok();
    }

    Result<ReceiveResult> PollMessages(const ReceiveRequest& request) {
        std::unique_lock lock(receive_mutex_);
        const auto find_ready = [this, &request] {
            return FirstReadyMessageLocked(request.connection_id);
        };
        if (find_ready() == nullptr) {
            if (request.timeout_ms == 0) {
                return WouldBlock("TCP receive queue is empty");
            }
            FilteredReceiveWaitRegistration registration(
                &filtered_receive_waiters_,
                request.connection_id != kInvalidConnectionId);
            const bool ready = receive_cv_.wait_for(
                lock, std::chrono::milliseconds(request.timeout_ms),
                [this, &find_ready] {
                    return find_ready() != nullptr ||
                           stop_requested_.load(std::memory_order_acquire) ||
                           worker_failed_.load(std::memory_order_acquire);
                });
            if (!ready) return Timeout("TCP receive timed out");
            if (find_ready() == nullptr) {
                return Unavailable("TCP receive worker stopped");
            }
        }
        if (find_ready()->message->payload.size() > request.max_bytes) {
            NotifyReadyWaitersLocked();
            return Exhausted("next TCP frame exceeds receive byte budget");
        }

        ReceiveResult result;
        try {
            result.messages.reserve(
                std::min<size_t>(request.max_messages,
                                 ready_receive_messages_));
        } catch (...) {
            if (ready_receive_messages_ != 0) NotifyReadyWaitersLocked();
            throw;
        }
        size_t bytes = 0;
        while (result.messages.size() < request.max_messages) {
            ReadyMessageEntry* const message = find_ready();
            if (message == nullptr) break;
            const size_t frame_size = message->message->payload.size();
            if (frame_size > request.max_bytes - bytes) break;
            bytes += frame_size;
            ConsumeReadyMessageLocked(message, &result);
        }
        if (ready_receive_messages_ != 0) NotifyReadyWaitersLocked();
        const bool should_wake_worker = !result.messages.empty();
        if (should_wake_worker) {
            receive_capacity_blocked_.store(false, std::memory_order_release);
        }
        lock.unlock();
        if (should_wake_worker) Wake();
        return result;
    }

    Result<CompletionPollResult> PollCompletions(
        const CompletionPollRequest& request) {
        std::unique_lock lock(mutex_);
        const auto matches_filter =
            [&request](const DeliveryCompletion& completion) {
                return request.connection_id == kInvalidConnectionId ||
                       completion.operation.connection_id ==
                           request.connection_id;
            };
        const auto find_ready = [this, &matches_filter] {
            return std::find_if(completions_.begin(), completions_.end(),
                                matches_filter);
        };
        if (find_ready() == completions_.end()) {
            if (request.timeout_ms == 0) {
                return WouldBlock("TCP completion queue is empty");
            }
            const bool ready = completion_cv_.wait_for(
                lock, std::chrono::milliseconds(request.timeout_ms),
                [this, &find_ready] {
                    return find_ready() != completions_.end() ||
                           stop_requested_.load(std::memory_order_acquire) ||
                           worker_failed_.load(std::memory_order_acquire);
                });
            if (!ready) return Timeout("TCP completion poll timed out");
            if (find_ready() == completions_.end()) {
                return Unavailable("TCP completion worker stopped");
            }
        }
        CompletionPollResult result;
        result.completions.reserve(
            std::min<size_t>(request.max_completions, completions_.size()));
        while (result.completions.size() < request.max_completions) {
            const auto completion = find_ready();
            if (completion == completions_.end()) break;
            result.completions.push_back(std::move(*completion));
            completions_.erase(completion);
        }
        return result;
    }

    Status Close(ConnectionId id) {
        std::lock_guard lock(mutex_);
        const auto listener = listeners_.find(id);
        if (listener != listeners_.end()) {
            listener->second.closing = true;
            Wake();
            return Status::Ok();
        }
        const auto connection = connections_.find(id);
        if (connection != connections_.end()) {
            connection->second.closing = true;
            {
                std::lock_guard send_lock(send_ingress_mutex_);
                const auto admission = send_admission_.find(id);
                if (admission != send_admission_.end()) {
                    admission->second.accepting = false;
                }
            }
            {
                std::lock_guard receive_lock(receive_mutex_);
                RemoveReadyMessagesLocked(id);
            }
            Wake();
            return Status::Ok();
        }
        if (recently_closed_.contains(id)) return Status::Ok();
        return Status::Error(StatusCode::kNotFound,
                             "TCP connection does not exist");
    }

    Result<security::AuthenticatedPeer> AuthenticatedPeer(
        ConnectionId id) const {
        std::lock_guard lock(mutex_);
        const auto found = connections_.find(id);
        if (found == connections_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "TCP connection does not exist");
        }
        if (!found->second.tls) {
            return Status::Error(StatusCode::kUnsupported,
                                 "TCP connection is plaintext");
        }
        return found->second.tls->peer();
    }

    TcpDriverStats Stats() const noexcept {
        std::lock_guard lock(mutex_);
        std::lock_guard send_lock(send_ingress_mutex_);
        std::lock_guard receive_lock(receive_mutex_);
        return TcpDriverStats{
            .active_connections = connections_.size(),
            .listeners = listeners_.size(),
            .queued_send_bytes =
                total_data_send_bytes_ + total_control_send_bytes_,
            .ready_receive_bytes = ready_receive_bytes_,
            .ready_receive_messages = ready_receive_messages_,
            .ready_receive_storage_slots = ready_messages_.size(),
            .pending_accepts = accepted_.size(),
            .successful_send_syscalls = successful_send_syscalls_,
            .gathered_send_syscalls = gathered_send_syscalls_,
            .gathered_send_buffers = gathered_send_buffers_,
            .sent_bytes = sent_bytes_,
        };
    }

private:
    struct ReadyMessageEntry {
        std::optional<ReceivedMessage> message;
    };


    struct FilteredReceiveWaitRegistration {
        FilteredReceiveWaitRegistration(size_t* waiters, bool filtered) noexcept
            : waiters(filtered ? waiters : nullptr) {
            if (this->waiters != nullptr) ++*this->waiters;
        }
        ~FilteredReceiveWaitRegistration() {
            if (waiters != nullptr) --*waiters;
        }

        FilteredReceiveWaitRegistration(
            const FilteredReceiveWaitRegistration&) = delete;
        FilteredReceiveWaitRegistration& operator=(
            const FilteredReceiveWaitRegistration&) = delete;

        size_t* waiters;
    };

    struct PendingWrite {
        void Initialize(std::vector<std::byte>&& owned_body,
                        SendOperation send_operation) noexcept {
            StoreBe32(static_cast<uint32_t>(owned_body.size()), prefix);
            body = std::move(owned_body);
            segmented = true;
            offset = 0;
            operation = send_operation;
        }

        void InitializeContiguous(std::vector<std::byte>&& wire,
                                  SendOperation send_operation) noexcept {
            body = std::move(wire);
            segmented = false;
            offset = 0;
            operation = send_operation;
        }

        size_t size() const noexcept {
            return body.size() + (segmented ? prefix.size() : 0);
        }

        std::array<std::byte, kTcpPrefixBytes> prefix{};
        std::vector<std::byte> body;
        bool segmented = true;
        // Aggregate offset across prefix followed by body.
        size_t offset = 0;
        SendOperation operation;
    };

    struct SendAdmission {
        bool accepting = true;
        std::deque<PendingWrite> control_writes;
        std::deque<PendingWrite> data_writes;
        size_t queued_data_bytes = 0;
        size_t queued_control_bytes = 0;
        size_t queued_control_messages = 0;
    };

    enum class TlsWriteSource : uint8_t {
        kNone,
        kHeartbeat,
        kControl,
        kData,
    };

    enum class TlsPendingOperation : uint8_t { kNone, kRead, kWrite };

    struct Connection {
        ConnectionInfo info;
        int fd = -1;
        bool closing = false;
        std::unique_ptr<security::TlsChannel> tls;
        security::TlsIoNeed tls_handshake_need = security::TlsIoNeed::kNone;
        TlsPendingOperation tls_pending_operation = TlsPendingOperation::kNone;
        security::TlsIoNeed tls_io_need = security::TlsIoNeed::kNone;
        size_t tls_read_retry_length = 0;
        size_t tls_write_retry_length = 0;
        const std::byte* tls_write_retry_data = nullptr;
        TlsWriteSource tls_write_source = TlsWriteSource::kNone;
        std::array<std::byte, kTlsRecordPlaintextBytes> tls_write_buffer{};
        std::array<std::byte, kTcpReadChunkBytes> tls_read_buffer{};
        size_t tls_write_frame_credits = 0;
        bool tls_heartbeat_bypasses_credit = false;
        bool tls_credit_request_outstanding = false;
        TimePoint tls_handshake_started{};
        ConnectionId accepting_listener_id = kInvalidConnectionId;
        bool accepted_announced = false;
        std::vector<std::byte> receive_buffer;
        size_t receive_offset = 0;
        uint32_t expected_body_size = 0;
        uint64_t completed_receive_frames = 0;
        size_t reserved_body_bytes = 0;
        bool receive_paused_for_capacity = false;
        std::optional<TimePoint> partial_frame_started;
        std::deque<PendingWrite> control_writes;
        std::deque<PendingWrite> data_writes;
        std::vector<SendOperation> awaiting_ack;
        bool heartbeat_pending = false;
        size_t heartbeat_offset = 0;
        bool write_blocked = false;
#if defined(MINO_TCP_USE_EPOLL)
        uint64_t event_token = 0;
        uint32_t registered_events = 0;
#elif defined(MINO_TCP_USE_KQUEUE)
        uint64_t event_token = 0;
        bool read_registered = false;
        bool read_enabled = false;
        bool write_registered = false;
        bool write_enabled = false;
#endif
        TimePoint last_valid_receive{};
        TimePoint last_transmit{};
    };

    struct Listener {
        ConnectionInfo info;
        int fd = -1;
        bool closing = false;
#if defined(MINO_TCP_USE_EPOLL)
        uint64_t event_token = 0;
        uint32_t registered_events = 0;
#elif defined(MINO_TCP_USE_KQUEUE)
        uint64_t event_token = 0;
        bool read_registered = false;
        bool read_enabled = false;
#endif
    };

    struct AcceptedConnection {
        ConnectionId listener_id = kInvalidConnectionId;
        ConnectionInfo info;
    };

    enum class PollKind : uint8_t { kWake, kListener, kConnection };

    struct PollToken {
        PollKind kind = PollKind::kWake;
        ConnectionId id = kInvalidConnectionId;
        int fd = -1;
    };

    static bool HasPendingWriteLocked(
        const Connection& connection) noexcept {
        return !connection.control_writes.empty() ||
               !connection.data_writes.empty() ||
               connection.heartbeat_pending;
    }

    bool HasPendingOrIngressApplicationWriteLocked(
        const Connection& connection) const {
        if (!connection.control_writes.empty() ||
            !connection.data_writes.empty()) {
            return true;
        }
        std::lock_guard send_lock(send_ingress_mutex_);
        const auto admission = send_admission_.find(connection.info.id);
        return admission != send_admission_.end() &&
               (!admission->second.control_writes.empty() ||
                !admission->second.data_writes.empty());
    }

    bool HasPendingOrIngressWriteLocked(
        const Connection& connection) const {
        return connection.heartbeat_pending ||
               HasPendingOrIngressApplicationWriteLocked(connection);
    }

    static bool TlsHandshakeNeedsRead(const Connection& connection) noexcept {
        return connection.tls && !connection.tls->handshake_complete() &&
               connection.tls_handshake_need != security::TlsIoNeed::kWrite;
    }

    static bool TlsHandshakeNeedsWrite(const Connection& connection) noexcept {
        return connection.tls && !connection.tls->handshake_complete() &&
               connection.tls_handshake_need != security::TlsIoNeed::kRead;
    }

    static bool TlsNeedReady(security::TlsIoNeed need,
                             short events) noexcept {
        return need == security::TlsIoNeed::kNone ||
               (need == security::TlsIoNeed::kRead &&
                (events & POLLIN) != 0) ||
               (need == security::TlsIoNeed::kWrite &&
                (events & POLLOUT) != 0);
    }

    static bool SocketReadableNow(int fd) noexcept {
        pollfd descriptor{.fd = fd, .events = POLLIN, .revents = 0};
        int ready;
        do {
            ready = ::poll(&descriptor, 1, 0);
        } while (ready < 0 && errno == EINTR);
        return ready > 0 &&
               (descriptor.revents & (POLLIN | POLLHUP | POLLERR)) != 0;
    }

    static bool TlsPendingRead(const Connection& connection) noexcept {
        return connection.tls_pending_operation == TlsPendingOperation::kRead;
    }

    static bool TlsPendingWrite(const Connection& connection) noexcept {
        return connection.tls_pending_operation == TlsPendingOperation::kWrite;
    }

    static void ClearTlsPendingOperation(Connection& connection) noexcept {
        connection.tls_pending_operation = TlsPendingOperation::kNone;
        connection.tls_io_need = security::TlsIoNeed::kNone;
        connection.tls_read_retry_length = 0;
        connection.tls_write_retry_length = 0;
        connection.tls_write_retry_data = nullptr;
        connection.tls_write_source = TlsWriteSource::kNone;
    }

    Status PublishAcceptedLocked(Connection& connection) {
        if (connection.accepting_listener_id == kInvalidConnectionId ||
            connection.accepted_announced ||
            accepted_.size() >= options_.max_pending_accepts) {
            return Status::Ok();
        }
        if (!listeners_.contains(connection.accepting_listener_id)) {
            return Unavailable("TCP listener closed during TLS handshake");
        }
        accepted_.push_back(AcceptedConnection{
            .listener_id = connection.accepting_listener_id,
            .info = connection.info,
        });
        connection.accepted_announced = true;
        accept_cv_.notify_all();
        return Status::Ok();
    }

    Status AdvanceTlsHandshakeLocked(Connection& connection) {
        if (!connection.tls) return Status::Ok();
        if (!connection.tls->handshake_complete()) {
            auto advanced = connection.tls->Handshake();
            if (!advanced.ok()) return advanced.status();
            if (advanced->peer_closed) {
                return Unavailable("TLS peer closed during handshake");
            }
            connection.tls_handshake_need = advanced->need;
            if (!connection.tls->handshake_complete()) return Status::Ok();
            connection.tls_handshake_need = security::TlsIoNeed::kNone;
            connection.last_valid_receive = Clock::now();
            connection.last_transmit = connection.last_valid_receive;
        }
        return PublishAcceptedLocked(connection);
    }

    void AdvanceTlsHandshakesLocked() {
        std::vector<std::pair<ConnectionId, Status>> failures;
        for (auto& [id, connection] : connections_) {
            if (!connection.tls || connection.tls->handshake_complete()) {
                if (connection.tls && !connection.accepted_announced &&
                    connection.accepting_listener_id != kInvalidConnectionId) {
                    const Status published = PublishAcceptedLocked(connection);
                    if (!published.ok()) failures.emplace_back(id, published);
                }
                continue;
            }
            if (connection.tls_handshake_need == security::TlsIoNeed::kNone) {
                const Status advanced = AdvanceTlsHandshakeLocked(connection);
                if (!advanced.ok()) failures.emplace_back(id, advanced);
            }
        }
        for (const auto& [id, failure] : failures) {
            CloseConnectionLocked(id, failure);
        }
    }

    Result<ConnectionId> NextIdLocked() {
        if (connections_.size() + listeners_.size() >=
            static_cast<size_t>(std::numeric_limits<ConnectionId>::max() - 1)) {
            return Exhausted("TCP connection ID space exhausted");
        }
        for (;;) {
            ConnectionId candidate = next_id_++;
            if (next_id_ == kInvalidConnectionId) next_id_ = 1;
            if (candidate == kInvalidConnectionId) continue;
            if (!connections_.contains(candidate) &&
                !listeners_.contains(candidate)) {
                return candidate;
            }
        }
    }

    void Wake() noexcept {
        if (wake_write_fd_ < 0 ||
            wake_pending_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        const std::byte byte{1};
        ssize_t written;
        do {
            written = ::write(wake_write_fd_, &byte, 1);
        } while (written < 0 && errno == EINTR);
        if (written < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            wake_pending_.store(false, std::memory_order_release);
        }
    }

    void ConsumeWake() noexcept {
        std::array<std::byte, 128> bytes{};
        while (::read(wake_read_fd_, bytes.data(), bytes.size()) > 0) {
        }
        // Clear before the next maintenance pass. A producer that raced with the
        // drain either left work visible to that pass or will write a fresh byte.
        wake_pending_.store(false, std::memory_order_release);
    }

    bool DrainSendIngressLocked() noexcept {
        std::lock_guard send_lock(send_ingress_mutex_);
        bool staged_immediate_write = false;
        for (auto& [id, admission] : send_admission_) {
            const auto connection = connections_.find(id);
            if (connection == connections_.end()) continue;
            if (connection->second.control_writes.empty() &&
                !admission.control_writes.empty()) {
                connection->second.control_writes.swap(
                    admission.control_writes);
                staged_immediate_write |= !connection->second.write_blocked;
            }
            if (connection->second.data_writes.empty() &&
                !admission.data_writes.empty()) {
                connection->second.data_writes.swap(admission.data_writes);
                staged_immediate_write |= !connection->second.write_blocked;
            }
        }
        return staged_immediate_write;
    }

    void ReleaseSendAdmissionLocked(ConnectionId id, bool is_control,
                                    size_t bytes, size_t messages) noexcept {
        std::lock_guard send_lock(send_ingress_mutex_);
        const auto found = send_admission_.find(id);
        if (found == send_admission_.end()) return;
        if (is_control) {
            found->second.queued_control_bytes -= bytes;
            found->second.queued_control_messages -= messages;
            total_control_send_bytes_ -= bytes;
            total_control_send_messages_ -= messages;
        } else {
            found->second.queued_data_bytes -= bytes;
            total_data_send_bytes_ -= bytes;
        }
    }

    void CloseWakePipeLocked() noexcept {
        if (wake_read_fd_ >= 0) (void)::close(wake_read_fd_);
        if (wake_write_fd_ >= 0) (void)::close(wake_write_fd_);
        wake_read_fd_ = -1;
        wake_write_fd_ = -1;
    }

    void DrainBufferedReceivesLocked() {
        std::vector<std::pair<ConnectionId, Status>> failures;
        for (auto& [id, connection] : connections_) {
            if (connection.closing) continue;
            Status status = Status::Ok();
            if (connection.receive_offset != connection.receive_buffer.size() ||
                (connection.tls && connection.tls->handshake_complete() &&
                 connection.tls_pending_operation ==
                     TlsPendingOperation::kNone &&
                 connection.tls->has_buffered_read())) {
                status = ReadConnectionLocked(connection, false);
            }
            if (!status.ok()) failures.emplace_back(id, status);
        }
        for (const auto& [id, status] : failures) {
            CloseConnectionLocked(id, status);
        }
    }

    // Newly queued writes optimistically run in software. EAGAIN is the only
    // transition to kernel-managed writable readiness; budget exhaustion stays
    // software-ready so short queues avoid writable-interest churn.
    bool DrainUnblockedWritesLocked() {
        std::vector<std::pair<ConnectionId, Status>> failures;
        bool has_immediate_writes = false;
        for (auto& [id, connection] : connections_) {
            if (connection.closing || connection.write_blocked ||
                (connection.tls && !connection.tls->handshake_complete()) ||
                TlsPendingRead(connection) ||
                !HasPendingWriteLocked(connection)) {
                continue;
            }
            if (connection.tls && connection.tls_write_frame_credits == 0) {
                // Exactly one peer owns the TLS application-write turn. A peer
                // with queued application traffic may request that turn using a
                // canonical heartbeat without changing the wire protocol.
                if (!connection.tls_credit_request_outstanding &&
                    HasPendingOrIngressApplicationWriteLocked(connection)) {
                    connection.heartbeat_pending = true;
                    connection.tls_heartbeat_bypasses_credit = true;
                }
                if (!connection.tls_heartbeat_bypasses_credit) continue;
            }
            const Status status = WriteConnectionLocked(connection);
            if (!status.ok()) {
                failures.emplace_back(id, status);
                continue;
            }
            has_immediate_writes |=
                HasPendingWriteLocked(connection) && !connection.write_blocked;
        }
        for (const auto& [id, status] : failures) {
            CloseConnectionLocked(id, status);
        }
        return has_immediate_writes;
    }

#if defined(MINO_TCP_USE_EPOLL)
    uint64_t NextEventTokenLocked() noexcept {
        for (;;) {
            const uint64_t token = next_event_token_++;
            if (next_event_token_ == 0) next_event_token_ = 1;
            if (token != 0 && !event_tokens_.contains(token)) return token;
        }
    }

    Status SetEpollInterestLocked(const PollToken& poll_token, uint32_t events,
                                  uint64_t* event_token,
                                  uint32_t* registered_events) {
        if (events == 0) {
            RemoveEpollRegistrationLocked(poll_token.fd, event_token,
                                          registered_events);
            return Status::Ok();
        }
        if (*event_token != 0 && *registered_events == events) {
            return Status::Ok();
        }

        epoll_event event{};
        event.events = events;
        if (*event_token == 0) {
            const uint64_t token = NextEventTokenLocked();
            event.data.u64 = token;
            event_tokens_.emplace(token, poll_token);
            int result;
            do {
                result = ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, poll_token.fd,
                                     &event);
            } while (result != 0 && errno == EINTR);
            if (result != 0) {
                event_tokens_.erase(token);
                return Internal("failed to register TCP descriptor with epoll");
            }
            *event_token = token;
            *registered_events = events;
            return Status::Ok();
        }

        event.data.u64 = *event_token;
        int result;
        do {
            result = ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, poll_token.fd,
                                 &event);
        } while (result != 0 && errno == EINTR);
        if (result != 0) {
            return Internal("failed to update TCP descriptor in epoll");
        }
        *registered_events = events;
        return Status::Ok();
    }

    void RemoveEpollRegistrationLocked(int fd, uint64_t* event_token,
                                       uint32_t* registered_events) noexcept {
        if (*event_token == 0) return;
        event_tokens_.erase(*event_token);
        if (epoll_fd_ >= 0 && fd >= 0) {
            int result;
            do {
                result = ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            } while (result != 0 && errno == EINTR);
        }
        *event_token = 0;
        *registered_events = 0;
    }

    Status SyncEpollInterestsLocked() {
        MINO_RETURN_IF_ERROR(SetEpollInterestLocked(
            PollToken{.kind = PollKind::kWake,
                      .id = kInvalidConnectionId,
                      .fd = wake_read_fd_},
            EPOLLIN, &wake_event_token_, &wake_registered_events_));

        const bool can_accept =
            accepted_.size() < options_.max_pending_accepts &&
            connections_.size() < config_.max_connections;
        for (auto& [id, listener] : listeners_) {
            const uint32_t events =
                !listener.closing && can_accept
                    ? static_cast<uint32_t>(EPOLLIN)
                    : 0u;
            MINO_RETURN_IF_ERROR(SetEpollInterestLocked(
                PollToken{.kind = PollKind::kListener,
                          .id = id,
                          .fd = listener.fd},
                events, &listener.event_token, &listener.registered_events));
        }
        for (auto& [id, connection] : connections_) {
            uint32_t events = EPOLLRDHUP;
            if (CanReadLocked(connection)) events |= EPOLLIN;
            if (TlsHandshakeNeedsWrite(connection) ||
                (connection.tls_pending_operation !=
                     TlsPendingOperation::kNone &&
                 connection.tls_io_need == security::TlsIoNeed::kWrite) ||
                (connection.write_blocked &&
                 connection.tls_pending_operation ==
                     TlsPendingOperation::kNone &&
                 HasPendingWriteLocked(connection) &&
                 (!connection.tls ||
                  connection.tls_write_frame_credits != 0 ||
                  connection.tls_heartbeat_bypasses_credit))) {
                events |= EPOLLOUT;
            }
            MINO_RETURN_IF_ERROR(SetEpollInterestLocked(
                PollToken{.kind = PollKind::kConnection,
                          .id = id,
                          .fd = connection.fd},
                events, &connection.event_token,
                &connection.registered_events));
        }
        return Status::Ok();
    }

    void ResetEpollStateLocked() noexcept {
        epoll_fd_ = -1;
        event_tokens_.clear();
        wake_event_token_ = 0;
        wake_registered_events_ = 0;
        for (auto& [id, listener] : listeners_) {
            (void)id;
            listener.event_token = 0;
            listener.registered_events = 0;
        }
        for (auto& [id, connection] : connections_) {
            (void)id;
            connection.event_token = 0;
            connection.registered_events = 0;
        }
    }

    void WorkerLoopEpoll() {
        UniqueFd epoll_fd(::epoll_create1(EPOLL_CLOEXEC));
        if (epoll_fd.get() < 0) {
            FailWorker();
            return;
        }
        {
            std::lock_guard lock(mutex_);
            epoll_fd_ = epoll_fd.get();
        }

        std::array<epoll_event, 256> events{};
        while (!stop_requested_.load(std::memory_order_acquire)) {
            Status sync_status;
            bool has_immediate_writes = false;
            {
                std::lock_guard lock(mutex_);
                const TimePoint now = Clock::now();
                has_immediate_writes = DrainSendIngressLocked();
                ProcessClosuresAndTimersLocked(now);
                AdvanceTlsHandshakesLocked();
                for (auto& [id, connection] : connections_) {
                    (void)id;
                    PrepareReceiveReservationLocked(connection);
                    PrepareHeartbeatLocked(connection, now);
                }
                DrainBufferedReceivesLocked();
                has_immediate_writes |= DrainUnblockedWritesLocked();
                // Echoes may arrive while the first write pass is running. Send
                // newly staged work now instead of adding an epoll_wait(0) turn.
                if (DrainSendIngressLocked()) {
                    has_immediate_writes |= DrainUnblockedWritesLocked();
                }
                sync_status = SyncEpollInterestsLocked();
            }
            if (!sync_status.ok()) {
                {
                    std::lock_guard lock(mutex_);
                    ResetEpollStateLocked();
                }
                FailWorker();
                return;
            }

            const int timeout_ms =
                has_immediate_writes
                    ? 0
                    : static_cast<int>(options_.io_poll_max_ms);
            const int ready = ::epoll_wait(
                epoll_fd.get(), events.data(), static_cast<int>(events.size()),
                timeout_ms);
            if (ready < 0) {
                if (errno == EINTR) continue;
                {
                    std::lock_guard lock(mutex_);
                    ResetEpollStateLocked();
                }
                FailWorker();
                return;
            }
            for (int index = 0; index < ready; ++index) {
                PollToken token;
                {
                    std::lock_guard lock(mutex_);
                    const auto found =
                        event_tokens_.find(events[index].data.u64);
                    if (found == event_tokens_.end()) continue;
                    token = found->second;
                }
                if (token.kind == PollKind::kWake) {
                    ConsumeWake();
                    continue;
                }

                short poll_events = 0;
                if ((events[index].events & EPOLLIN) != 0) {
                    poll_events |= POLLIN;
                }
                if ((events[index].events & EPOLLOUT) != 0) {
                    poll_events |= POLLOUT;
                }
                if ((events[index].events & EPOLLERR) != 0) {
                    poll_events |= POLLERR;
                }
                if ((events[index].events & (EPOLLHUP | EPOLLRDHUP)) != 0) {
                    poll_events |= POLLHUP;
                }
                if (token.kind == PollKind::kListener) {
                    ProcessListenerEvent(token, poll_events);
                } else {
                    std::lock_guard lock(mutex_);
                    ProcessConnectionEventLocked(token, poll_events);
                }
            }
        }
        std::lock_guard lock(mutex_);
        ResetEpollStateLocked();
    }
#elif defined(MINO_TCP_USE_KQUEUE)
    static_assert(sizeof(uintptr_t) >= sizeof(uint64_t));
    static_assert(sizeof(KqueueUserData) >= sizeof(uint64_t));

    uint64_t NextEventTokenLocked() noexcept {
        for (;;) {
            const uint64_t token = next_event_token_++;
            if (next_event_token_ == 0) next_event_token_ = 1;
            if (token != 0 && !event_tokens_.contains(token)) return token;
        }
    }

    Status ChangeKqueueFilterLocked(int fd, KqueueFilter filter,
                                    KqueueFlags flags, uint64_t event_token) {
        struct kevent change;
        EV_SET(&change, static_cast<uintptr_t>(fd), filter, flags, 0, 0,
               EncodeKqueueToken<KqueueUserData>(event_token));
        int result;
        do {
            result = ::kevent(kqueue_fd_, &change, 1, nullptr, 0, nullptr);
        } while (result != 0 && errno == EINTR);
        if (result != 0) {
            return Internal("failed to update TCP descriptor in kqueue");
        }
        return Status::Ok();
    }

    Status EnsureKqueueTokenLocked(const PollToken& poll_token,
                                   uint64_t* event_token) {
        if (*event_token != 0) return Status::Ok();
        const uint64_t token = NextEventTokenLocked();
        event_tokens_.emplace(token, poll_token);
        *event_token = token;
        return Status::Ok();
    }

    Status SetKqueueFilterLocked(int fd, KqueueFilter filter, bool enabled,
                                 uint64_t event_token, bool* registered,
                                 bool* was_enabled) {
        if (!*registered) {
            const KqueueFlags flags = static_cast<KqueueFlags>(
                EV_ADD | (enabled ? EV_ENABLE : EV_DISABLE));
            MINO_RETURN_IF_ERROR(
                ChangeKqueueFilterLocked(fd, filter, flags, event_token));
            *registered = true;
            *was_enabled = enabled;
            return Status::Ok();
        }
        if (*was_enabled == enabled) return Status::Ok();
        const KqueueFlags flags = static_cast<KqueueFlags>(
            enabled ? EV_ENABLE : EV_DISABLE);
        MINO_RETURN_IF_ERROR(
            ChangeKqueueFilterLocked(fd, filter, flags, event_token));
        *was_enabled = enabled;
        return Status::Ok();
    }

    void DeleteKqueueFilterLocked(int fd, KqueueFilter filter,
                                  bool* registered,
                                  bool* enabled) noexcept {
        if (!*registered) return;
        if (kqueue_fd_ >= 0 && fd >= 0) {
            struct kevent change;
            EV_SET(&change, static_cast<uintptr_t>(fd), filter, EV_DELETE, 0, 0,
                   EncodeKqueueToken<KqueueUserData>(0));
            int result;
            do {
                result = ::kevent(kqueue_fd_, &change, 1, nullptr, 0, nullptr);
            } while (result != 0 && errno == EINTR);
            (void)result;
        }
        *registered = false;
        *enabled = false;
    }

    void RemoveKqueueListenerLocked(Listener& listener) noexcept {
        if (listener.event_token == 0) return;
        event_tokens_.erase(listener.event_token);
        DeleteKqueueFilterLocked(listener.fd, EVFILT_READ,
                                 &listener.read_registered,
                                 &listener.read_enabled);
        listener.event_token = 0;
    }

    void RemoveKqueueConnectionLocked(Connection& connection) noexcept {
        if (connection.event_token == 0) return;
        event_tokens_.erase(connection.event_token);
        DeleteKqueueFilterLocked(connection.fd, EVFILT_READ,
                                 &connection.read_registered,
                                 &connection.read_enabled);
        DeleteKqueueFilterLocked(connection.fd, EVFILT_WRITE,
                                 &connection.write_registered,
                                 &connection.write_enabled);
        connection.event_token = 0;
    }

    Status SyncKqueueInterestsLocked() {
        const PollToken wake_token{.kind = PollKind::kWake,
                                   .id = kInvalidConnectionId,
                                   .fd = wake_read_fd_};
        MINO_RETURN_IF_ERROR(
            EnsureKqueueTokenLocked(wake_token, &wake_event_token_));
        MINO_RETURN_IF_ERROR(SetKqueueFilterLocked(
            wake_read_fd_, EVFILT_READ, true, wake_event_token_,
            &wake_read_registered_, &wake_read_enabled_));

        const bool can_accept =
            accepted_.size() < options_.max_pending_accepts &&
            connections_.size() < config_.max_connections;
        for (auto& [id, listener] : listeners_) {
            const PollToken token{.kind = PollKind::kListener,
                                  .id = id,
                                  .fd = listener.fd};
            MINO_RETURN_IF_ERROR(
                EnsureKqueueTokenLocked(token, &listener.event_token));
            MINO_RETURN_IF_ERROR(SetKqueueFilterLocked(
                listener.fd, EVFILT_READ, !listener.closing && can_accept,
                listener.event_token, &listener.read_registered,
                &listener.read_enabled));
        }
        for (auto& [id, connection] : connections_) {
            const PollToken token{.kind = PollKind::kConnection,
                                  .id = id,
                                  .fd = connection.fd};
            MINO_RETURN_IF_ERROR(
                EnsureKqueueTokenLocked(token, &connection.event_token));
            MINO_RETURN_IF_ERROR(SetKqueueFilterLocked(
                connection.fd, EVFILT_READ, CanReadLocked(connection),
                connection.event_token, &connection.read_registered,
                &connection.read_enabled));
            const bool needs_write =
                TlsHandshakeNeedsWrite(connection) ||
                (connection.tls_pending_operation !=
                     TlsPendingOperation::kNone &&
                 connection.tls_io_need == security::TlsIoNeed::kWrite) ||
                (connection.write_blocked &&
                 connection.tls_pending_operation ==
                     TlsPendingOperation::kNone &&
                 HasPendingWriteLocked(connection) &&
                 (!connection.tls ||
                  connection.tls_write_frame_credits != 0 ||
                  connection.tls_heartbeat_bypasses_credit));
            MINO_RETURN_IF_ERROR(SetKqueueFilterLocked(
                connection.fd, EVFILT_WRITE, needs_write,
                connection.event_token, &connection.write_registered,
                &connection.write_enabled));
        }
        return Status::Ok();
    }

    void ResetKqueueStateLocked() noexcept {
        kqueue_fd_ = -1;
        event_tokens_.clear();
        wake_event_token_ = 0;
        wake_read_registered_ = false;
        wake_read_enabled_ = false;
        for (auto& [id, listener] : listeners_) {
            (void)id;
            listener.event_token = 0;
            listener.read_registered = false;
            listener.read_enabled = false;
        }
        for (auto& [id, connection] : connections_) {
            (void)id;
            connection.event_token = 0;
            connection.read_registered = false;
            connection.read_enabled = false;
            connection.write_registered = false;
            connection.write_enabled = false;
        }
    }

    void WorkerLoopKqueue() {
        UniqueFd kqueue_fd(::kqueue());
        if (kqueue_fd.get() < 0) {
            FailWorker();
            return;
        }
        const int descriptor_flags = ::fcntl(kqueue_fd.get(), F_GETFD, 0);
        if (descriptor_flags < 0 ||
            ::fcntl(kqueue_fd.get(), F_SETFD,
                    descriptor_flags | FD_CLOEXEC) != 0) {
            FailWorker();
            return;
        }
        {
            std::lock_guard lock(mutex_);
            kqueue_fd_ = kqueue_fd.get();
        }

        std::array<struct kevent, 256> events{};
        while (!stop_requested_.load(std::memory_order_acquire)) {
            Status sync_status;
            bool has_immediate_writes = false;
            {
                std::lock_guard lock(mutex_);
                const TimePoint now = Clock::now();
                has_immediate_writes = DrainSendIngressLocked();
                ProcessClosuresAndTimersLocked(now);
                AdvanceTlsHandshakesLocked();
                for (auto& [id, connection] : connections_) {
                    (void)id;
                    PrepareReceiveReservationLocked(connection);
                    PrepareHeartbeatLocked(connection, now);
                }
                DrainBufferedReceivesLocked();
                has_immediate_writes |= DrainUnblockedWritesLocked();
                // Echoes may arrive while the first write pass is running. Send
                // newly staged work now instead of adding a kevent timeout turn.
                if (DrainSendIngressLocked()) {
                    has_immediate_writes |= DrainUnblockedWritesLocked();
                }
                sync_status = SyncKqueueInterestsLocked();
            }
            if (!sync_status.ok()) {
                {
                    std::lock_guard lock(mutex_);
                    ResetKqueueStateLocked();
                }
                FailWorker();
                return;
            }

            const uint32_t timeout_ms =
                has_immediate_writes ? 0u : options_.io_poll_max_ms;
            const timespec timeout{
                .tv_sec = static_cast<time_t>(timeout_ms / 1000u),
                .tv_nsec =
                    static_cast<long>(timeout_ms % 1000u) * 1'000'000L,
            };
            const int ready = ::kevent(
                kqueue_fd.get(), nullptr, 0, events.data(),
                static_cast<int>(events.size()), &timeout);
            if (ready < 0) {
                if (errno == EINTR) continue;
                {
                    std::lock_guard lock(mutex_);
                    ResetKqueueStateLocked();
                }
                FailWorker();
                return;
            }
            bool wake_failed = false;
            // kqueue reports read and write filters as separate events. Drain
            // every readable/control event before processing writable events so
            // symmetric TLS senders cannot both enter SSL_write WANT_WRITE while
            // their inbound application records remain unread.
            for (int phase = 0; phase < 2 && !wake_failed; ++phase) {
                for (int index = 0; index < ready; ++index) {
                    const bool is_write =
                        events[index].filter == EVFILT_WRITE;
                    if ((phase == 0 && is_write) ||
                        (phase == 1 && !is_write)) {
                        continue;
                    }
                    PollToken token;
                    {
                        std::lock_guard lock(mutex_);
                        const uint64_t event_token =
                            DecodeKqueueToken(events[index].udata);
                        const auto found = event_tokens_.find(event_token);
                        if (found == event_tokens_.end()) continue;
                        token = found->second;
                    }
                    if (static_cast<uintptr_t>(token.fd) !=
                        events[index].ident) {
                        continue;
                    }
                    if (token.kind == PollKind::kWake) {
                        if ((events[index].flags & (EV_ERROR | EV_EOF)) != 0) {
                            wake_failed = true;
                            break;
                        }
                        ConsumeWake();
                        continue;
                    }

                    short poll_events = 0;
                    if (events[index].filter == EVFILT_READ) {
                        poll_events |= POLLIN;
                    } else if (is_write) {
                        poll_events |= POLLOUT;
                    }
                    if ((events[index].flags & EV_ERROR) != 0) {
                        poll_events |= POLLERR;
                    }
                    if ((events[index].flags & EV_EOF) != 0) {
                        poll_events |= POLLHUP;
                    }
                    if (token.kind == PollKind::kListener) {
                        ProcessListenerEvent(token, poll_events);
                    } else {
                        std::lock_guard lock(mutex_);
                        ProcessConnectionEventLocked(token, poll_events);
                    }
                }
            }
            if (wake_failed) {
                {
                    std::lock_guard lock(mutex_);
                    ResetKqueueStateLocked();
                }
                FailWorker();
                return;
            }
        }
        std::lock_guard lock(mutex_);
        ResetKqueueStateLocked();
    }
#else
    void WorkerLoopPoll() {
        while (!stop_requested_.load(std::memory_order_acquire)) {
            std::vector<pollfd> descriptors;
            std::vector<PollToken> tokens;
            {
                std::lock_guard lock(mutex_);
                (void)DrainSendIngressLocked();
                ProcessClosuresAndTimersLocked(Clock::now());
                AdvanceTlsHandshakesLocked();
                DrainBufferedReceivesLocked();
                descriptors.reserve(1 + listeners_.size() + connections_.size());
                tokens.reserve(descriptors.capacity());
                descriptors.push_back(pollfd{.fd = wake_read_fd_,
                                             .events = POLLIN,
                                             .revents = 0});
                tokens.push_back(PollToken{.kind = PollKind::kWake,
                                           .id = kInvalidConnectionId,
                                           .fd = wake_read_fd_});
                const bool can_accept =
                    accepted_.size() < options_.max_pending_accepts &&
                    connections_.size() < config_.max_connections;
                for (const auto& [id, listener] : listeners_) {
                    if (!listener.closing && can_accept) {
                        descriptors.push_back(pollfd{.fd = listener.fd,
                                                     .events = POLLIN,
                                                     .revents = 0});
                        tokens.push_back(PollToken{.kind = PollKind::kListener,
                                                   .id = id,
                                                   .fd = listener.fd});
                    }
                }
                for (auto& [id, connection] : connections_) {
                    PrepareReceiveReservationLocked(connection);
                    PrepareHeartbeatLocked(connection, Clock::now());
                    short poll_events = 0;
                    if (CanReadLocked(connection)) poll_events |= POLLIN;
                    if (TlsHandshakeNeedsWrite(connection) ||
                        (connection.tls_pending_operation !=
                             TlsPendingOperation::kNone &&
                         connection.tls_io_need ==
                             security::TlsIoNeed::kWrite) ||
                        (connection.tls_pending_operation ==
                             TlsPendingOperation::kNone &&
                         HasPendingWriteLocked(connection) &&
                         (!connection.tls ||
                          connection.tls_write_frame_credits != 0 ||
                          connection.tls_heartbeat_bypasses_credit))) {
                        poll_events |= POLLOUT;
                    }
                    descriptors.push_back(pollfd{.fd = connection.fd,
                                                 .events = poll_events,
                                                 .revents = 0});
                    tokens.push_back(PollToken{.kind = PollKind::kConnection,
                                               .id = id,
                                               .fd = connection.fd});
                }
            }

            const int ready = ::poll(descriptors.data(), descriptors.size(),
                                     static_cast<int>(options_.io_poll_max_ms));
            if (ready < 0) {
                if (errno == EINTR) continue;
                FailWorker();
                return;
            }
            if (ready == 0) continue;
            for (size_t index = 0; index < descriptors.size(); ++index) {
                if (descriptors[index].revents == 0) continue;
                const PollToken token = tokens[index];
                if (token.kind == PollKind::kWake) {
                    ConsumeWake();
                    continue;
                }
                if (token.kind == PollKind::kListener) {
                    ProcessListenerEvent(token, descriptors[index].revents);
                } else {
                    std::lock_guard lock(mutex_);
                    ProcessConnectionEventLocked(token,
                                                 descriptors[index].revents);
                }
            }
        }
    }
#endif

    void WorkerLoop() noexcept {
        // OpenSSL's socket BIO does not expose MSG_NOSIGNAL. Block SIGPIPE only
        // on this dedicated I/O thread so a peer close becomes an SSL error
        // instead of terminating the process; application threads are untouched.
        sigset_t blocked_signals;
        (void)sigemptyset(&blocked_signals);
        (void)sigaddset(&blocked_signals, SIGPIPE);
        (void)::pthread_sigmask(SIG_BLOCK, &blocked_signals, nullptr);
        try {
#if defined(MINO_TCP_USE_EPOLL)
            WorkerLoopEpoll();
#elif defined(MINO_TCP_USE_KQUEUE)
            WorkerLoopKqueue();
#else
            WorkerLoopPoll();
#endif
        } catch (const std::bad_alloc&) {
#if defined(MINO_TCP_USE_EPOLL)
            {
                std::lock_guard lock(mutex_);
                ResetEpollStateLocked();
            }
#elif defined(MINO_TCP_USE_KQUEUE)
            {
                std::lock_guard lock(mutex_);
                ResetKqueueStateLocked();
            }
#endif
            FailWorker();
        } catch (...) {
#if defined(MINO_TCP_USE_EPOLL)
            {
                std::lock_guard lock(mutex_);
                ResetEpollStateLocked();
            }
#elif defined(MINO_TCP_USE_KQUEUE)
            {
                std::lock_guard lock(mutex_);
                ResetKqueueStateLocked();
            }
#endif
            FailWorker();
        }
    }

    void FailWorker() noexcept {
        health_->store(HealthState::kUnavailable, std::memory_order_release);
        stop_requested_.store(true, std::memory_order_release);
        worker_failed_.store(true, std::memory_order_release);
        {
            std::lock_guard lock(mutex_);
        }
        {
            std::lock_guard receive_lock(receive_mutex_);
        }
        receive_cv_.notify_all();
        completion_cv_.notify_all();
        accept_cv_.notify_all();
    }

    void ProcessClosuresAndTimersLocked(TimePoint now) {
        std::vector<ConnectionId> close_listeners;
        for (const auto& [id, listener] : listeners_) {
            if (listener.closing) close_listeners.push_back(id);
        }
        for (ConnectionId id : close_listeners) CloseListenerLocked(id);

        std::vector<std::pair<ConnectionId, Status>> close_connections;
        const auto idle_timeout =
            std::chrono::milliseconds(options_.idle_timeout_ms);
        const auto partial_timeout =
            std::chrono::milliseconds(options_.partial_frame_timeout_ms);
        const auto tls_handshake_timeout =
            std::chrono::milliseconds(options_.tls_handshake_timeout_ms);
        for (const auto& [id, connection] : connections_) {
            if (connection.closing) {
                close_connections.emplace_back(
                    id, Unavailable("TCP connection was closed"));
            } else if (connection.tls &&
                       !connection.tls->handshake_complete() &&
                       now - connection.tls_handshake_started >=
                           tls_handshake_timeout) {
                close_connections.emplace_back(
                    id, Timeout("TLS handshake timed out"));
            } else if (now - connection.last_valid_receive >= idle_timeout) {
                close_connections.emplace_back(
                    id, Unavailable("TCP connection idle timeout"));
            } else if (!connection.receive_paused_for_capacity &&
                       connection.partial_frame_started.has_value() &&
                       now - *connection.partial_frame_started >=
                           partial_timeout) {
                close_connections.emplace_back(
                    id, Corruption("TCP partial frame timed out"));
            }
        }
        for (const auto& [id, status] : close_connections) {
            CloseConnectionLocked(id, status);
        }
    }

    void CloseListenerLocked(ConnectionId id) {
        const auto found = listeners_.find(id);
        if (found == listeners_.end()) return;
#if defined(MINO_TCP_USE_EPOLL)
        RemoveEpollRegistrationLocked(found->second.fd,
                                      &found->second.event_token,
                                      &found->second.registered_events);
#elif defined(MINO_TCP_USE_KQUEUE)
        RemoveKqueueListenerLocked(found->second);
#endif
        if (found->second.fd >= 0) (void)::close(found->second.fd);
        listeners_.erase(found);
        RememberClosedLocked(id);
        accept_cv_.notify_all();
    }

    void TrimConsumedReadyMessagesLocked() noexcept {
        while (!ready_messages_.empty() &&
               !ready_messages_.front().message.has_value()) {
            ready_messages_.pop_front();
        }
    }

    ReadyMessageEntry* FirstReadyMessageLocked(ConnectionId id) noexcept {
        if (id == kInvalidConnectionId) {
            TrimConsumedReadyMessagesLocked();
            return ready_messages_.empty() ? nullptr : &ready_messages_.front();
        }
        const auto found = ready_messages_by_connection_.find(id);
        return found == ready_messages_by_connection_.end()
                   ? nullptr
                   : found->second.front();
    }

    void NotifyReadyWaitersLocked() noexcept {
        if (filtered_receive_waiters_ == 0) {
            receive_cv_.notify_one();
        } else {
            // A single CV cannot target a connection. Wake all whenever a
            // filtered waiter exists so a notification cannot select a waiter
            // whose predicate rejects this connection and strand the message.
            receive_cv_.notify_all();
        }
    }

    void CompactReadyMessagesLocked() {
        std::deque<ReadyMessageEntry> compacted;
        std::unordered_map<ConnectionId, std::deque<ReadyMessageEntry*>>
            compacted_index;
        compacted_index.reserve(ready_messages_by_connection_.size());
        for (const ReadyMessageEntry& entry : ready_messages_) {
            if (!entry.message.has_value()) continue;
            compacted.push_back(ReadyMessageEntry{.message = *entry.message});
            ReadyMessageEntry* const ready = &compacted.back();
            compacted_index[ready->message->connection_id].push_back(ready);
        }
        assert(compacted.size() == ready_receive_messages_);
        ready_messages_.swap(compacted);
        ready_messages_by_connection_.swap(compacted_index);
    }

    void EnqueueReadyMessageLocked(ReceivedMessage message) {
        if (ready_messages_.size() >=
                options_.max_ready_receive_messages &&
            ready_messages_.size() != ready_receive_messages_) {
            CompactReadyMessagesLocked();
        }
        const ConnectionId id = message.connection_id;
        const size_t frame_size = message.payload.size();
        ready_messages_.push_back(
            ReadyMessageEntry{.message = std::move(message)});
        ReadyMessageEntry* const ready = &ready_messages_.back();
        auto bucket = ready_messages_by_connection_.end();
        try {
            bucket = ready_messages_by_connection_.try_emplace(id).first;
            bucket->second.push_back(ready);
        } catch (...) {
            if (bucket != ready_messages_by_connection_.end() &&
                bucket->second.empty()) {
                ready_messages_by_connection_.erase(bucket);
            }
            ready_messages_.pop_back();
            throw;
        }
        ready_receive_bytes_ += frame_size;
        ++ready_receive_messages_;
        NotifyReadyWaitersLocked();
    }

    void ConsumeReadyMessageLocked(ReadyMessageEntry* message,
                                   ReceiveResult* result) {
        assert(message != nullptr);
        assert(message->message.has_value());
        const ConnectionId id = message->message->connection_id;
        const size_t frame_size = message->message->payload.size();
        const auto bucket = ready_messages_by_connection_.find(id);
        assert(bucket != ready_messages_by_connection_.end());
        assert(!bucket->second.empty());
        assert(bucket->second.front() == message);
        result->messages.push_back(std::move(*message->message));
        bucket->second.pop_front();
        if (bucket->second.empty()) {
            ready_messages_by_connection_.erase(bucket);
        }
        message->message.reset();
        ready_receive_bytes_ -= frame_size;
        --ready_receive_messages_;
        TrimConsumedReadyMessagesLocked();
    }

    void RemoveReadyMessagesLocked(ConnectionId id) {
        const auto bucket = ready_messages_by_connection_.find(id);
        if (bucket == ready_messages_by_connection_.end()) return;
        for (ReadyMessageEntry* const ready : bucket->second) {
            assert(ready != nullptr);
            assert(ready->message.has_value());
            ready_receive_bytes_ -= ready->message->payload.size();
            --ready_receive_messages_;
            ready->message.reset();
        }
        ready_messages_by_connection_.erase(bucket);
        TrimConsumedReadyMessagesLocked();
    }

    void CloseConnectionLocked(ConnectionId id, const Status& failure) {
        const auto found = connections_.find(id);
        if (found == connections_.end()) return;
        Connection& connection = found->second;
#if defined(MINO_TCP_USE_EPOLL)
        RemoveEpollRegistrationLocked(connection.fd, &connection.event_token,
                                      &connection.registered_events);
#elif defined(MINO_TCP_USE_KQUEUE)
        RemoveKqueueConnectionLocked(connection);
#endif
        if (connection.fd >= 0) (void)::close(connection.fd);
        {
            std::lock_guard receive_lock(receive_mutex_);
            RemoveReadyMessagesLocked(id);
            if (connection.reserved_body_bytes != 0) {
                reserved_receive_bytes_ -= connection.reserved_body_bytes;
                --reserved_receive_messages_;
            }
        }
        for (const PendingWrite& write : connection.data_writes) {
            if (write.operation.id == kInvalidOperationId) continue;
            completions_.push_back(DeliveryCompletion{
                .operation = write.operation,
                .reached_stage = DeliveryStage::kLocalPublished,
                .status = failure,
            });
        }
        for (const SendOperation operation : connection.awaiting_ack) {
            completions_.push_back(DeliveryCompletion{
                .operation = operation,
                .reached_stage = DeliveryStage::kLocalPublished,
                .status = failure,
            });
        }
        {
            std::lock_guard send_lock(send_ingress_mutex_);
            const auto admission = send_admission_.find(id);
            if (admission != send_admission_.end()) {
                admission->second.accepting = false;
                for (const PendingWrite& write :
                     admission->second.data_writes) {
                    if (write.operation.id == kInvalidOperationId) continue;
                    completions_.push_back(DeliveryCompletion{
                        .operation = write.operation,
                        .reached_stage = DeliveryStage::kLocalPublished,
                        .status = failure,
                    });
                }
                total_data_send_bytes_ -= admission->second.queued_data_bytes;
                total_control_send_bytes_ -=
                    admission->second.queued_control_bytes;
                total_control_send_messages_ -=
                    admission->second.queued_control_messages;
                send_admission_.erase(admission);
            }
        }

        accepted_.erase(
            std::remove_if(accepted_.begin(), accepted_.end(),
                           [id](const AcceptedConnection& accepted) {
                               return accepted.info.id == id;
                           }),
            accepted_.end());
        connections_.erase(found);
        RememberClosedLocked(id);
        completion_cv_.notify_all();
        receive_cv_.notify_all();
        accept_cv_.notify_all();
    }

    void RememberClosedLocked(ConnectionId id) {
        recently_closed_.insert(id);
        while (recently_closed_.size() >
               static_cast<size_t>(config_.max_connections) +
                   static_cast<size_t>(config_.max_listeners)) {
            recently_closed_.erase(recently_closed_.begin());
        }
    }

    void PrepareHeartbeatLocked(Connection& connection, TimePoint now) noexcept {
        if ((!connection.tls || connection.tls->handshake_complete()) &&
            !connection.heartbeat_pending &&
            connection.control_writes.empty() &&
            connection.data_writes.empty() &&
            now - connection.last_transmit >=
                std::chrono::milliseconds(options_.heartbeat_interval_ms)) {
            connection.heartbeat_pending = true;
            connection.heartbeat_offset = 0;
        }
    }

    bool CanReadLocked(Connection& connection) noexcept {
        if (connection.closing) return false;
        if (connection.tls && !connection.tls->handshake_complete()) {
            return TlsHandshakeNeedsRead(connection);
        }
        if (connection.accepting_listener_id != kInvalidConnectionId &&
            !connection.accepted_announced) {
            return false;
        }
        // Readiness for a pending TLS operation always retries that exact
        // operation. In particular, WANT_READ from Write must not dispatch Read,
        // and WANT_WRITE from Read must not dispatch Write.
        if (connection.tls_pending_operation != TlsPendingOperation::kNone) {
            return connection.tls_io_need != security::TlsIoNeed::kWrite;
        }
        if (connection.reserved_body_bytes != 0) return true;
        std::lock_guard receive_lock(receive_mutex_);
        const bool can_read =
            connection.expected_body_size == 0 &&
            ready_receive_messages_ + reserved_receive_messages_ <
                options_.max_ready_receive_messages &&
            ready_receive_bytes_ + reserved_receive_bytes_ <
                options_.max_ready_receive_bytes;
        connection.receive_paused_for_capacity = !can_read;
        if (!can_read) {
            connection.partial_frame_started.reset();
            receive_capacity_blocked_.store(true, std::memory_order_release);
        }
        return can_read;
    }

    void PrepareReceiveReservationLocked(Connection& connection) {
        if (connection.expected_body_size == 0 ||
            connection.reserved_body_bytes != 0) {
            return;
        }
        const size_t body_size = connection.expected_body_size;
        std::lock_guard receive_lock(receive_mutex_);
        if (ready_receive_messages_ + reserved_receive_messages_ >=
                options_.max_ready_receive_messages ||
            ready_receive_bytes_ + reserved_receive_bytes_ >
                options_.max_ready_receive_bytes ||
            body_size > options_.max_ready_receive_bytes -
                            ready_receive_bytes_ - reserved_receive_bytes_) {
            connection.receive_paused_for_capacity = true;
            connection.partial_frame_started.reset();
            receive_capacity_blocked_.store(true, std::memory_order_release);
            return;
        }
        connection.reserved_body_bytes = body_size;
        connection.receive_paused_for_capacity = false;
        if (!connection.partial_frame_started.has_value()) {
            connection.partial_frame_started = Clock::now();
        }
        reserved_receive_bytes_ += body_size;
        ++reserved_receive_messages_;
    }

    void ProcessListenerEvent(const PollToken& token, short events) {
        if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            std::lock_guard lock(mutex_);
            const auto found = listeners_.find(token.id);
            if (found != listeners_.end() && found->second.fd == token.fd) {
                CloseListenerLocked(token.id);
            }
            return;
        }
        if ((events & POLLIN) == 0) return;

        for (;;) {
            {
                std::lock_guard lock(mutex_);
                const auto found = listeners_.find(token.id);
                if (found == listeners_.end() || found->second.fd != token.fd ||
                    found->second.closing ||
                    accepted_.size() >= options_.max_pending_accepts ||
                    connections_.size() >= config_.max_connections) {
                    return;
                }
            }

            sockaddr_storage peer_storage{};
            socklen_t peer_size = sizeof(peer_storage);
            UniqueFd accepted_fd(::accept(
                token.fd, reinterpret_cast<sockaddr*>(&peer_storage),
                &peer_size));
            if (accepted_fd.get() < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                std::lock_guard lock(mutex_);
                const auto found = listeners_.find(token.id);
                if (found != listeners_.end() && found->second.fd == token.fd) {
                    CloseListenerLocked(token.id);
                }
                return;
            }
            if (!ConfigureTcpSocket(accepted_fd.get()).ok()) continue;

            sockaddr_storage local_storage{};
            socklen_t local_size = sizeof(local_storage);
            if (::getsockname(accepted_fd.get(),
                              reinterpret_cast<sockaddr*>(&local_storage),
                              &local_size) != 0) {
                continue;
            }
            auto local_endpoint = FromSocketAddress(local_storage, local_size);
            auto peer_endpoint = FromSocketAddress(peer_storage, peer_size);
            if (!local_endpoint.ok() || !peer_endpoint.ok()) continue;

            // Credential snapshots, PEM parsing, key validation, SSL_CTX creation,
            // and SSL allocation are intentionally outside mutex_.
            std::unique_ptr<security::TlsChannel> tls;
            if (options_.tls_factory) {
                auto created = options_.tls_factory->Create(
                    accepted_fd.get(), security::TlsRole::kServer);
                if (!created.ok()) continue;
                tls = std::move(*created);
            }

            std::lock_guard lock(mutex_);
            const auto listener = listeners_.find(token.id);
            if (listener == listeners_.end() || listener->second.fd != token.fd ||
                listener->second.closing ||
                accepted_.size() >= options_.max_pending_accepts ||
                connections_.size() >= config_.max_connections) {
                continue;
            }
            auto id_result = NextIdLocked();
            if (!id_result.ok()) return;
            const ConnectionId id = *id_result;
            const TimePoint now = Clock::now();
            Connection connection;
            connection.info = ConnectionInfo{
                .id = id,
                .kind = TransportKind::kNetwork,
                .is_listener = false,
                .local_endpoint = *local_endpoint,
                .peer_endpoint = *peer_endpoint,
            };
            connection.fd = accepted_fd.release();
            connection.tls = std::move(tls);
            connection.tls_handshake_started = now;
            connection.accepting_listener_id = token.id;
            connection.last_valid_receive = now;
            connection.last_transmit = now;
            const ConnectionInfo info = connection.info;
            auto [inserted, was_inserted] =
                connections_.emplace(id, std::move(connection));
            (void)was_inserted;
            try {
                std::lock_guard send_lock(send_ingress_mutex_);
                send_admission_.emplace(id, SendAdmission{});
            } catch (...) {
                if (inserted->second.fd >= 0) {
                    (void)::close(inserted->second.fd);
                }
                connections_.erase(inserted);
                throw;
            }
            if (!inserted->second.tls) {
                inserted->second.accepted_announced = true;
                accepted_.push_back(AcceptedConnection{
                    .listener_id = token.id,
                    .info = info,
                });
                accept_cv_.notify_all();
            }
        }
    }

    void ProcessConnectionEventLocked(const PollToken& token, short events) {
        auto found = connections_.find(token.id);
        if (found == connections_.end() || found->second.fd != token.fd ||
            found->second.closing) {
            return;
        }
        if ((events & (POLLERR | POLLNVAL)) != 0) {
            CloseConnectionLocked(token.id,
                                  Unavailable("TCP socket poll failed"));
            return;
        }
        if (found->second.tls &&
            !found->second.tls->handshake_complete()) {
            if (!TlsNeedReady(found->second.tls_handshake_need, events)) return;
            const Status handshake = AdvanceTlsHandshakeLocked(found->second);
            if (!handshake.ok()) {
                CloseConnectionLocked(token.id, handshake);
            }
            // The readiness was consumed by SSL_do_handshake. Never reuse a
            // possibly stale bit to start application Read or Write in this turn.
            return;
        }
        if (found->second.accepting_listener_id != kInvalidConnectionId &&
            !found->second.accepted_announced) {
            return;
        }
        if (found->second.tls_pending_operation !=
            TlsPendingOperation::kNone) {
            const bool retry_ready =
                TlsNeedReady(found->second.tls_io_need, events);
            if (!retry_ready) {
                if ((events & POLLHUP) != 0) {
                    CloseConnectionLocked(
                        token.id, Unavailable("TCP peer closed connection"));
                }
                return;
            }
            Status retried = Status::Ok();
            if (TlsPendingWrite(found->second)) {
                found->second.write_blocked = false;
                retried = WriteConnectionLocked(found->second);
            } else {
                retried = ReadConnectionLocked(found->second, true);
            }
            if (!retried.ok()) {
                CloseConnectionLocked(token.id, retried);
                return;
            }
            // A WANT_* retry consumes this readiness even when it succeeds.
            // Never reuse the same possibly stale event for the opposite SSL
            // operation or for a different write buffer.
            return;
        }
        if ((events & POLLIN) != 0) {
            const Status read_status =
                ReadConnectionLocked(found->second, true);
            if (!read_status.ok()) {
                CloseConnectionLocked(token.id, read_status);
                return;
            }
        }
        found = connections_.find(token.id);
        if (found == connections_.end()) return;
        if ((events & POLLOUT) != 0 &&
            found->second.tls_pending_operation ==
                TlsPendingOperation::kNone) {
            if (found->second.tls &&
                (found->second.tls->has_buffered_read() ||
                 SocketReadableNow(found->second.fd))) {
                const Status read_status =
                    ReadConnectionLocked(found->second, true);
                if (!read_status.ok()) {
                    CloseConnectionLocked(token.id, read_status);
                }
                return;
            }
            found->second.write_blocked = false;
            const Status write_status = WriteConnectionLocked(found->second);
            if (!write_status.ok()) {
                CloseConnectionLocked(token.id, write_status);
                return;
            }
        }
        if ((events & POLLHUP) != 0 &&
            connections_.contains(token.id)) {
            CloseConnectionLocked(token.id,
                                  Unavailable("TCP peer closed connection"));
        }
    }

    void ClearConnectionReceiveBufferLocked(Connection& connection) {
        connection.receive_buffer.clear();
        connection.receive_offset = 0;
        if (connection.receive_buffer.capacity() > kTcpReadChunkBytes) {
            std::vector<std::byte>().swap(connection.receive_buffer);
        }
    }

    void CompactConnectionReceiveBufferLocked(Connection& connection) {
        if (connection.receive_offset == 0) return;
        if (connection.receive_offset == connection.receive_buffer.size()) {
            ClearConnectionReceiveBufferLocked(connection);
        } else {
            connection.receive_buffer.erase(
                connection.receive_buffer.begin(),
                connection.receive_buffer.begin() +
                    static_cast<ptrdiff_t>(connection.receive_offset));
        }
        connection.receive_offset = 0;
    }

    Status ProcessConnectionReceiveBufferLocked(Connection& connection,
                                                size_t* frame_budget) {
        while (*frame_budget != 0) {
            size_t available =
                connection.receive_buffer.size() - connection.receive_offset;
            if (connection.expected_body_size == 0) {
                if (available < kTcpPrefixBytes) return Status::Ok();
                const auto prefix = std::span<const std::byte>(
                    connection.receive_buffer.data() + connection.receive_offset,
                    kTcpPrefixBytes);
                connection.expected_body_size = LoadBe32(prefix);
                connection.receive_offset += kTcpPrefixBytes;
                if (connection.expected_body_size <
                        bridge::kWireBaseHeaderLength ||
                    connection.expected_body_size >
                        options_.max_frame_body_bytes) {
                    return Corruption("TCP frame length prefix is out of bounds");
                }
                PrepareReceiveReservationLocked(connection);
                if (connection.reserved_body_bytes == 0) return Status::Ok();
            } else if (connection.reserved_body_bytes == 0) {
                PrepareReceiveReservationLocked(connection);
                if (connection.reserved_body_bytes == 0) return Status::Ok();
            }

            available =
                connection.receive_buffer.size() - connection.receive_offset;
            if (available < connection.expected_body_size) return Status::Ok();
            const auto body = std::span<const std::byte>(
                connection.receive_buffer.data() + connection.receive_offset,
                connection.expected_body_size);
            const bool is_canonical_heartbeat =
                body.size() + kTcpPrefixBytes == heartbeat_wire_.size() &&
                std::equal(body.begin(), body.end(),
                           heartbeat_wire_.begin() + kTcpPrefixBytes);
            if (is_canonical_heartbeat ||
                IsMinimallyStructuredWireFrame(body)) {
                connection.last_valid_receive = Clock::now();
            }

            // Steal a complete trailing frame out of the receive buffer when
            // possible so the ready queue owns the bytes without an extra
            // assign/memcpy. Mid-buffer frames still copy; mutex layout is
            // unchanged.
            std::vector<std::byte> payload;
            bool stole_receive_tail = false;
            if (!is_canonical_heartbeat) {
                const size_t body_begin = connection.receive_offset;
                const size_t body_end =
                    body_begin + connection.expected_body_size;
                if (body_end == connection.receive_buffer.size()) {
                    payload = std::move(connection.receive_buffer);
                    connection.receive_buffer.clear();
                    if (body_begin != 0) {
                        payload.erase(
                            payload.begin(),
                            payload.begin() +
                                static_cast<ptrdiff_t>(body_begin));
                    }
                    connection.receive_offset = 0;
                    stole_receive_tail = true;
                } else {
                    payload.assign(body.begin(), body.end());
                }
            }
            if (!stole_receive_tail) {
                connection.receive_offset += connection.expected_body_size;
            }
            const bool held_tls_write_turn =
                connection.tls && connection.tls_write_frame_credits != 0;
            // An application frame always transfers the TLS write turn. A
            // heartbeat received while this peer still holds the turn is a
            // credit request. If no application response is queued, return the
            // turn with one ordinary heartbeat; that response is not echoed.
            if (connection.tls &&
                (!is_canonical_heartbeat || held_tls_write_turn) &&
                !HasPendingOrIngressApplicationWriteLocked(connection)) {
                connection.heartbeat_pending = true;
            }
            {
                std::lock_guard receive_lock(receive_mutex_);
                reserved_receive_bytes_ -= connection.reserved_body_bytes;
                --reserved_receive_messages_;
                connection.reserved_body_bytes = 0;
                if (!is_canonical_heartbeat) {
                    EnqueueReadyMessageLocked(ReceivedMessage{
                        .connection_id = connection.info.id,
                        .from = *connection.info.peer_endpoint,
                        .payload = std::move(payload),
                    });
                }
                ++connection.completed_receive_frames;
                if (connection.tls) {
                    connection.tls_write_frame_credits = 1;
                    connection.tls_credit_request_outstanding = false;
                }
                --*frame_budget;
                if (ready_receive_messages_ >=
                        options_.max_ready_receive_messages ||
                    ready_receive_bytes_ + reserved_receive_bytes_ >=
                        options_.max_ready_receive_bytes) {
                    connection.receive_paused_for_capacity = true;
                    connection.partial_frame_started.reset();
                    receive_capacity_blocked_.store(true,
                                                    std::memory_order_release);
                    connection.expected_body_size = 0;
                    if (connection.receive_offset ==
                        connection.receive_buffer.size()) {
                        ClearConnectionReceiveBufferLocked(connection);
                    }
                    return Status::Ok();
                }
                connection.receive_paused_for_capacity = false;
            }

            connection.expected_body_size = 0;
            if (connection.receive_offset == connection.receive_buffer.size()) {
                ClearConnectionReceiveBufferLocked(connection);
                connection.partial_frame_started.reset();
                return Status::Ok();
            }
            connection.partial_frame_started = Clock::now();
        }
        return Status::Ok();
    }

    Status ReadConnectionLocked(Connection& connection, bool read_ready) {
        if (TlsPendingWrite(connection)) return Status::Ok();
        size_t frame_budget = options_.max_receive_frames_per_turn;
        MINO_RETURN_IF_ERROR(
            ProcessConnectionReceiveBufferLocked(connection, &frame_budget));
        const bool inbound_available =
            read_ready ||
            (connection.tls && connection.tls->has_buffered_read());
        if (connection.tls && connection.tls_write_frame_credits != 0 &&
            HasPendingOrIngressWriteLocked(connection) && !inbound_available) {
            return Status::Ok();
        }
        if (frame_budget == 0 || connection.receive_paused_for_capacity ||
            (connection.expected_body_size != 0 &&
             connection.reserved_body_bytes == 0)) {
            return Status::Ok();
        }
        size_t budget = options_.max_receive_bytes_per_turn;
        std::array<std::byte, kTcpReadChunkBytes> plaintext_chunk{};
        while (budget != 0) {
            CompactConnectionReceiveBufferLocked(connection);
            const size_t max_buffered =
                static_cast<size_t>(options_.max_frame_body_bytes) +
                kTcpPrefixBytes;
            if (connection.receive_buffer.size() >= max_buffered) {
                return Corruption("TCP receive buffer exceeded frame bound");
            }
            const size_t amount = connection.tls_read_retry_length != 0
                                      ? connection.tls_read_retry_length
                                      : std::min({kTcpReadChunkBytes, budget,
                                                  max_buffered -
                                                      connection.receive_buffer.size()});
            size_t received_bytes = 0;
            const std::byte* received_data = nullptr;
            if (connection.tls) {
                auto received = connection.tls->Read(
                    std::span<std::byte>(connection.tls_read_buffer).first(amount));
                if (!received.ok()) return received.status();
                if (received->peer_closed) {
                    return connection.receive_buffer.empty() &&
                                   connection.expected_body_size == 0
                               ? Unavailable("TLS peer closed connection")
                               : Corruption("TLS stream ended inside frame");
                }
                connection.tls_io_need = received->need;
                if (received->bytes == 0 &&
                    received->need != security::TlsIoNeed::kNone) {
                    connection.tls_pending_operation =
                        TlsPendingOperation::kRead;
                    connection.tls_read_retry_length = amount;
                    return Status::Ok();
                }
                ClearTlsPendingOperation(connection);
                received_bytes = received->bytes;
                received_data = connection.tls_read_buffer.data();
            } else {
                ssize_t received;
                do {
                    received = ::recv(connection.fd, plaintext_chunk.data(), amount, 0);
                } while (received < 0 && errno == EINTR);
                if (received == 0) {
                    return connection.receive_buffer.empty() &&
                                   connection.expected_body_size == 0
                               ? Unavailable("TCP peer closed connection")
                               : Corruption("TCP stream ended inside frame");
                }
                if (received < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        return Status::Ok();
                    }
                    return Unavailable("TCP receive failed");
                }
                received_bytes = static_cast<size_t>(received);
                received_data = plaintext_chunk.data();
            }
            if (!connection.partial_frame_started.has_value()) {
                connection.partial_frame_started = Clock::now();
            }
            connection.receive_buffer.insert(
                connection.receive_buffer.end(), received_data,
                received_data + static_cast<ptrdiff_t>(received_bytes));
            budget -= received_bytes;
            MINO_RETURN_IF_ERROR(ProcessConnectionReceiveBufferLocked(
                connection, &frame_budget));
            if (frame_budget == 0 || connection.receive_paused_for_capacity ||
                (connection.expected_body_size != 0 &&
                 connection.reserved_body_bytes == 0)) {
                return Status::Ok();
            }
            // Do not begin another SSL_read while outbound frames are queued.
            // A successful read is complete; yielding here lets full-duplex peers
            // write before either side creates a new WANT_* read dependency.
            if (connection.tls &&
                HasPendingOrIngressWriteLocked(connection)) {
                return Status::Ok();
            }
        }
        return Status::Ok();
    }

    Result<size_t> WriteContiguousLocked(
        Connection& connection, std::span<const std::byte> bytes,
        TlsWriteSource source) {
        if (connection.tls) {
            if (TlsPendingRead(connection)) {
                return Internal("TLS read must complete before TLS write");
            }
            if (connection.tls_write_retry_length != 0) {
                if (source != connection.tls_write_source ||
                    bytes.data() != connection.tls_write_retry_data ||
                    bytes.size() < connection.tls_write_retry_length) {
                    return Internal("TLS write retry source or arguments changed");
                }
                bytes = bytes.first(connection.tls_write_retry_length);
            }
            auto written = connection.tls->Write(bytes);
            if (!written.ok()) return written.status();
            if (written->peer_closed) {
                return Unavailable("TLS peer closed during write");
            }
            connection.tls_io_need = written->need;
            if (written->bytes == 0 &&
                written->need != security::TlsIoNeed::kNone) {
                connection.tls_pending_operation =
                    TlsPendingOperation::kWrite;
                connection.tls_write_retry_length = bytes.size();
                connection.tls_write_retry_data = bytes.data();
                connection.tls_write_source = source;
                connection.write_blocked = true;
                return size_t{0};
            }
            ClearTlsPendingOperation(connection);
            return written->bytes;
        }
        ssize_t sent;
        do {
            sent = SendNoSignal(connection.fd, bytes.data(), bytes.size());
        } while (sent < 0 && errno == EINTR);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                connection.write_blocked = true;
                return size_t{0};
            }
            return Unavailable("TCP send failed");
        }
        if (sent == 0) return Unavailable("TCP send made no progress");
        return static_cast<size_t>(sent);
    }

    Status WriteConnectionLocked(Connection& connection) {
        if (connection.tls && !connection.tls->handshake_complete()) {
            return Status::Ok();
        }
        if (TlsPendingRead(connection)) return Status::Ok();
        if (connection.tls && connection.tls_write_frame_credits == 0 &&
            !connection.tls_heartbeat_bypasses_credit) {
            return Status::Ok();
        }
        size_t budget = kIoBudgetBytes;
        while (budget != 0) {
            // A heartbeat is also a stream frame. Once any part of it has been
            // written, finish it before switching to either user queue.
            if (connection.heartbeat_pending &&
                (connection.heartbeat_offset != 0 ||
                 connection.tls_write_source == TlsWriteSource::kHeartbeat ||
                 connection.tls_heartbeat_bypasses_credit)) {
                const size_t remaining =
                    heartbeat_wire_.size() - connection.heartbeat_offset;
                const size_t amount = std::min(
                    remaining,
                    connection.tls
                        ? std::min(budget, kTlsRecordPlaintextBytes)
                        : budget);
                MINO_ASSIGN_OR_RETURN(
                    const size_t sent,
                    WriteContiguousLocked(
                        connection,
                        std::span<const std::byte>(heartbeat_wire_)
                            .subspan(connection.heartbeat_offset, amount),
                        TlsWriteSource::kHeartbeat));
                if (sent == 0) return Status::Ok();
                ++successful_send_syscalls_;
                sent_bytes_ += sent;
                connection.heartbeat_offset += sent;
                budget -= sent;
                connection.last_transmit = Clock::now();
                if (connection.heartbeat_offset == heartbeat_wire_.size()) {
                    connection.heartbeat_pending = false;
                    connection.heartbeat_offset = 0;
                    if (connection.tls_heartbeat_bypasses_credit) {
                        connection.tls_heartbeat_bypasses_credit = false;
                        connection.tls_credit_request_outstanding = true;
                    } else if (connection.tls) {
                        --connection.tls_write_frame_credits;
                    }
                }
                if (connection.tls) {
                    connection.write_blocked = HasPendingWriteLocked(connection);
                    return Status::Ok();
                }
                continue;
            }

            std::deque<PendingWrite>* writes = nullptr;
            bool is_control = false;
            size_t gather_limit = kMaxGatheredWriteBuffers;
            if (connection.tls_write_source == TlsWriteSource::kControl) {
                writes = &connection.control_writes;
                is_control = true;
            } else if (connection.tls_write_source == TlsWriteSource::kData) {
                writes = &connection.data_writes;
            } else if (!connection.data_writes.empty() &&
                       connection.data_writes.front().offset != 0) {
                writes = &connection.data_writes;
                // Finishing this frame may expose higher-priority control data.
                if (!connection.control_writes.empty()) gather_limit = 1;
            } else if (!connection.control_writes.empty() &&
                       connection.control_writes.front().offset != 0) {
                writes = &connection.control_writes;
                is_control = true;
            } else if (!connection.control_writes.empty()) {
                writes = &connection.control_writes;
                is_control = true;
            } else if (!connection.data_writes.empty()) {
                writes = &connection.data_writes;
            }

            if (writes != nullptr) {
                std::array<iovec, kMaxGatheredWriteBuffers> vectors{};
                size_t vector_count = 0;
                size_t vector_bytes = 0;
                const size_t write_budget =
                    connection.tls
                        ? std::min(budget, kTlsRecordPlaintextBytes)
                        : budget;
                for (PendingWrite& write : *writes) {
                    if (vector_count >= gather_limit ||
                        vector_bytes >= write_budget) {
                        break;
                    }
                    if (write.segmented && write.offset < kTcpPrefixBytes) {
                        const size_t amount = std::min(
                            kTcpPrefixBytes - write.offset,
                            write_budget - vector_bytes);
                        vectors[vector_count++] = iovec{
                            .iov_base = write.prefix.data() + write.offset,
                            .iov_len = amount,
                        };
                        vector_bytes += amount;
                        if (write.offset + amount < kTcpPrefixBytes ||
                            vector_count >= gather_limit ||
                            vector_bytes >= write_budget) {
                            continue;
                        }
                    }
                    const size_t body_offset =
                        write.segmented
                            ? (write.offset > kTcpPrefixBytes
                                   ? write.offset - kTcpPrefixBytes
                                   : 0)
                            : write.offset;
                    const size_t amount = std::min(
                        write.body.size() - body_offset,
                        write_budget - vector_bytes);
                    vectors[vector_count++] = iovec{
                        .iov_base = write.body.data() + body_offset,
                        .iov_len = amount,
                    };
                    vector_bytes += amount;
                    // TLS keeps one frame per stable retry buffer. Plaintext may
                    // gather segments from subsequent frames into one sendmsg.
                    if (connection.tls) break;
                }

                size_t sent_bytes = 0;
                if (connection.tls) {
                    std::span<const std::byte> tls_bytes;
                    if (vector_count == 1) {
                        tls_bytes = std::span<const std::byte>(
                            static_cast<const std::byte*>(vectors[0].iov_base),
                            vectors[0].iov_len);
                    } else {
                        size_t copied = 0;
                        for (size_t index = 0; index < vector_count; ++index) {
                            const auto* data = static_cast<const std::byte*>(
                                vectors[index].iov_base);
                            std::copy_n(
                                data, vectors[index].iov_len,
                                connection.tls_write_buffer.data() + copied);
                            copied += vectors[index].iov_len;
                        }
                        tls_bytes = std::span<const std::byte>(
                            connection.tls_write_buffer).first(copied);
                    }
                    MINO_ASSIGN_OR_RETURN(
                        sent_bytes,
                        WriteContiguousLocked(
                            connection, tls_bytes,
                            is_control ? TlsWriteSource::kControl
                                       : TlsWriteSource::kData));
                    if (sent_bytes == 0) return Status::Ok();
                } else {
                    ssize_t sent = -1;
                    do {
                        sent = SendMessageNoSignal(
                            connection.fd,
                            std::span<iovec>(vectors).first(vector_count));
                    } while (sent < 0 && errno == EINTR);
                    if (sent < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            connection.write_blocked = true;
                            return Status::Ok();
                        }
                        return Unavailable("TCP gathered send failed");
                    }
                    if (sent == 0) {
                        return Unavailable("TCP send made no progress");
                    }
                    sent_bytes = static_cast<size_t>(sent);
                }

                ++successful_send_syscalls_;
                if (!connection.tls && vector_count > 1) {
                    ++gathered_send_syscalls_;
                    gathered_send_buffers_ += vector_count;
                }
                sent_bytes_ += sent_bytes;
                budget -= sent_bytes;
                connection.last_transmit = Clock::now();

                size_t consumed = sent_bytes;
                size_t released_bytes = 0;
                size_t released_messages = 0;
                while (consumed != 0) {
                    PendingWrite& write = writes->front();
                    const size_t remaining = write.size() - write.offset;
                    const size_t amount = std::min(remaining, consumed);
                    write.offset += amount;
                    consumed -= amount;
                    if (write.offset != write.size()) break;

                    const size_t wire_size = write.size();
                    if (!is_control &&
                        write.operation.id != kInvalidOperationId) {
                        connection.awaiting_ack.push_back(write.operation);
                    }
                    released_bytes += wire_size;
                    ++released_messages;
                    writes->pop_front();
                    if (connection.tls) {
                        --connection.tls_write_frame_credits;
                    }
                    // TCP local write is not remote acceptance. The protocol ACK
                    // confirms tracked operations after each full frame is sent.
                }
                if (released_bytes != 0) {
                    ReleaseSendAdmissionLocked(connection.info.id, is_control,
                                               released_bytes,
                                               released_messages);
                }
                if (connection.tls) {
                    connection.write_blocked = HasPendingWriteLocked(connection);
                    return Status::Ok();
                }
                continue;
            }

            if (!connection.heartbeat_pending) break;
            const size_t amount = std::min(
                heartbeat_wire_.size(),
                connection.tls ? std::min(budget, kTlsRecordPlaintextBytes)
                               : budget);
            MINO_ASSIGN_OR_RETURN(
                const size_t sent,
                WriteContiguousLocked(
                    connection,
                    std::span<const std::byte>(heartbeat_wire_).first(amount),
                    TlsWriteSource::kHeartbeat));
            if (sent == 0) return Status::Ok();
            ++successful_send_syscalls_;
            sent_bytes_ += sent;
            connection.heartbeat_offset = sent;
            budget -= sent;
            connection.last_transmit = Clock::now();
            if (connection.heartbeat_offset == heartbeat_wire_.size()) {
                connection.heartbeat_pending = false;
                connection.heartbeat_offset = 0;
                if (connection.tls_heartbeat_bypasses_credit) {
                    connection.tls_heartbeat_bypasses_credit = false;
                    connection.tls_credit_request_outstanding = true;
                } else if (connection.tls) {
                    --connection.tls_write_frame_credits;
                }
            }
            if (connection.tls) {
                connection.write_blocked = HasPendingWriteLocked(connection);
                return Status::Ok();
            }
        }
        if (!HasPendingWriteLocked(connection)) {
            connection.write_blocked = false;
        }
        return Status::Ok();
    }

    TcpDriverOptions options_;
    std::atomic<HealthState>* health_;
    std::vector<std::byte> heartbeat_wire_;
    DriverConfig config_{};

    mutable std::mutex mutex_;
    mutable std::mutex send_ingress_mutex_;
    mutable std::mutex receive_mutex_;
    std::condition_variable receive_cv_;
    std::condition_variable completion_cv_;
    std::condition_variable accept_cv_;
    std::thread worker_;
    std::atomic<bool> stop_requested_{true};
    std::atomic<bool> wake_pending_{false};
    std::atomic<bool> receive_capacity_blocked_{false};
    std::atomic<bool> worker_failed_{false};
    int wake_read_fd_ = -1;
    int wake_write_fd_ = -1;
#if defined(MINO_TCP_USE_EPOLL)
    int epoll_fd_ = -1;
    uint64_t next_event_token_ = 1;
    uint64_t wake_event_token_ = 0;
    uint32_t wake_registered_events_ = 0;
    std::unordered_map<uint64_t, PollToken> event_tokens_;
#elif defined(MINO_TCP_USE_KQUEUE)
    int kqueue_fd_ = -1;
    uint64_t next_event_token_ = 1;
    uint64_t wake_event_token_ = 0;
    bool wake_read_registered_ = false;
    bool wake_read_enabled_ = false;
    std::unordered_map<uint64_t, PollToken> event_tokens_;
#endif
    ConnectionId next_id_ = 1;
    std::unordered_map<ConnectionId, Connection> connections_;
    std::unordered_map<ConnectionId, SendAdmission> send_admission_;
    std::unordered_map<ConnectionId, Listener> listeners_;
    std::deque<AcceptedConnection> accepted_;
    std::deque<ReadyMessageEntry> ready_messages_;
    std::unordered_map<ConnectionId, std::deque<ReadyMessageEntry*>>
        ready_messages_by_connection_;
    size_t ready_receive_messages_ = 0;
    size_t filtered_receive_waiters_ = 0;
    std::deque<DeliveryCompletion> completions_;
    std::unordered_set<ConnectionId> recently_closed_;
    size_t total_data_send_bytes_ = 0;
    size_t total_control_send_bytes_ = 0;
    size_t total_control_send_messages_ = 0;
    size_t ready_receive_bytes_ = 0;
    size_t reserved_receive_bytes_ = 0;
    size_t reserved_receive_messages_ = 0;
    uint64_t successful_send_syscalls_ = 0;
    uint64_t gathered_send_syscalls_ = 0;
    uint64_t gathered_send_buffers_ = 0;
    uint64_t sent_bytes_ = 0;
};

Result<std::unique_ptr<TcpDriver>> TcpDriver::Create(
    TcpDriverOptions options) {
    try {
        MINO_RETURN_IF_ERROR(ValidateTcpDriverOptions(options));
        MINO_ASSIGN_OR_RETURN(auto heartbeat_wire,
                              BuildHeartbeatWire(options));
        auto driver = std::unique_ptr<TcpDriver>(new TcpDriver(options));
        driver->impl_ = std::make_unique<Impl>(
            options, &driver->health_, std::move(heartbeat_wire));
        return driver;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

TcpDriver::TcpDriver(TcpDriverOptions options) : options_(options) {}

TcpDriver::~TcpDriver() { (void)Shutdown(); }

TransportCapabilities TcpDriver::capabilities() const noexcept {
    return TransportCapabilities{
        .kind = TransportKind::kNetwork,
        .reliability = TransportReliability::kReliable,
        .max_frame_size = options_.max_frame_body_bytes,
        .max_reassembly_bytes = options_.max_frame_body_bytes,
        .features = Capability::kConnect | Capability::kListen |
                    Capability::kRemoteAcceptedConfirmation,
    };
}

TcpDriverStats TcpDriver::stats() const noexcept {
    return impl_ == nullptr ? TcpDriverStats{} : impl_->Stats();
}

Status TcpDriver::DoStart(const DriverConfig& config) {
    return impl_->Start(config);
}

void TcpDriver::DoRequestStop() noexcept { impl_->RequestStop(); }

Status TcpDriver::DoShutdown() { return impl_->StopAndJoin(); }

Result<ConnectionInfo> TcpDriver::DoConnect(const ConnectRequest& request) {
    return impl_->Connect(request);
}

Result<ConnectionInfo> TcpDriver::DoListen(const ListenRequest& request) {
    return impl_->Listen(request);
}

Result<ConnectionInfo> TcpDriver::DoAccept(const AcceptRequest& request) {
    return impl_->Accept(request);
}

Result<SendResult> TcpDriver::DoSend(const SendRequest& request,
                                     SendOperation operation) {
    return impl_->Send(request, operation);
}

Result<size_t> TcpDriver::DoSendUntracked(
    const UntrackedSendRequest& request) {
    return impl_->SendUntracked(request);
}

Result<SendResult> TcpDriver::DoTrySendOwned(
    const SendRequest& request, std::vector<std::byte>&& payload,
    SendOperation operation) {
    return impl_->SendOwned(request, std::move(payload), operation);
}

Result<size_t> TcpDriver::DoTrySendUntrackedOwned(
    const UntrackedSendRequest& request,
    std::vector<std::byte>&& payload) {
    return impl_->SendUntrackedOwned(request, std::move(payload));
}

Status TcpDriver::DoConfirmRemoteAccepted(SendOperation operation) {
    return impl_->ConfirmRemoteAccepted(operation);
}

Result<ReceiveResult> TcpDriver::DoPoll(const ReceiveRequest& request) {
    return impl_->PollMessages(request);
}

Result<CompletionPollResult> TcpDriver::DoPollCompletions(
    const CompletionPollRequest& request) {
    return impl_->PollCompletions(request);
}

Result<security::AuthenticatedPeer> TcpDriver::DoAuthenticatedPeer(
    ConnectionId connection_id) {
    return impl_->AuthenticatedPeer(connection_id);
}

Status TcpDriver::DoClose(ConnectionId connection_id) {
    return impl_->Close(connection_id);
}

}  // namespace mino::transport