// Copyright 2026 The Mino Authors

#include "mino/transport/tcp_driver.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <system_error>
#include <thread>
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
constexpr size_t kIoBudgetBytes = 256u * 1024u;

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

}  // namespace

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
        options.max_ready_receive_bytes < options.max_frame_body_bytes ||
        options.max_ready_receive_bytes > kMaxReceiveBatchBytes) {
        return Invalid("TCP send or receive byte bounds are inconsistent");
    }
    if (options.max_ready_receive_messages == 0 ||
        options.max_ready_receive_messages > kMaxQueuedSends ||
        options.max_pending_accepts == 0 ||
        options.max_pending_accepts > kMaxConnections) {
        return Invalid("TCP message or accept bounds are invalid");
    }
    if (options.heartbeat_interval_ms == 0 || options.idle_timeout_ms == 0 ||
        options.partial_frame_timeout_ms == 0 || options.io_poll_max_ms == 0 ||
        options.heartbeat_interval_ms >= options.idle_timeout_ms ||
        options.idle_timeout_ms > kMaxOperationTimeoutMs ||
        options.partial_frame_timeout_ms > kMaxOperationTimeoutMs ||
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
        worker_failed_ = false;
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
        ready_messages_.clear();
        completions_.clear();
        recently_closed_.clear();
        total_send_bytes_ = 0;
        ready_receive_bytes_ = 0;
        reserved_receive_bytes_ = 0;
        reserved_receive_messages_ = 0;
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
            pollfd descriptor{
                .fd = socket_fd.get(),
                .events = POLLOUT,
                .revents = 0,
            };
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
                descriptor.revents = 0;
                const int polled =
                    ::poll(&descriptor, 1, static_cast<int>(poll_ms));
                if (polled > 0) break;
                if (polled < 0 && errno != EINTR) {
                    return Unavailable("TCP connect poll failed");
                }
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
        connection.last_valid_receive = now;
        connection.last_transmit = now;
        connections_.emplace(id, std::move(connection));
        Wake();
        return connections_.at(id).info;
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
        std::vector<std::byte> wire = PrefixFrame(request.payload);
        const size_t wire_size = wire.size();
        std::lock_guard lock(mutex_);
        const auto found = connections_.find(request.connection_id);
        if (found == connections_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "TCP connection does not exist");
        }
        Connection& connection = found->second;
        if (connection.closing) {
            return Unavailable("TCP connection is closing");
        }
        if (wire_size > options_.max_total_send_buffer_bytes -
                            total_send_bytes_ ||
            wire_size > options_.max_connection_send_buffer_bytes -
                            connection.queued_send_bytes) {
            return WouldBlock("TCP send byte queue is full");
        }
        connection.writes.push_back(PendingWrite{
            .bytes = std::move(wire),
            .offset = 0,
            .operation = operation,
        });
        connection.queued_send_bytes += wire_size;
        total_send_bytes_ += wire_size;
        Wake();
        return SendResult{
            .operation = operation,
            .admitted_bytes = request.payload.size(),
        };
    }

    Result<ReceiveResult> PollMessages(const ReceiveRequest& request) {
        std::unique_lock lock(mutex_);
        if (ready_messages_.empty()) {
            if (request.timeout_ms == 0) {
                return WouldBlock("TCP receive queue is empty");
            }
            const bool ready = receive_cv_.wait_for(
                lock, std::chrono::milliseconds(request.timeout_ms), [this] {
                    return !ready_messages_.empty() ||
                           stop_requested_.load(std::memory_order_acquire) ||
                           worker_failed_;
                });
            if (!ready) return Timeout("TCP receive timed out");
            if (ready_messages_.empty()) {
                return Unavailable("TCP receive worker stopped");
            }
        }
        if (ready_messages_.front().payload.size() > request.max_bytes) {
            return Exhausted("next TCP frame exceeds receive byte budget");
        }

        ReceiveResult result;
        result.messages.reserve(
            std::min<size_t>(request.max_messages, ready_messages_.size()));
        size_t bytes = 0;
        while (!ready_messages_.empty() &&
               result.messages.size() < request.max_messages) {
            const size_t frame_size = ready_messages_.front().payload.size();
            if (frame_size > request.max_bytes - bytes) break;
            bytes += frame_size;
            ready_receive_bytes_ -= frame_size;
            result.messages.push_back(std::move(ready_messages_.front()));
            ready_messages_.pop_front();
        }
        Wake();
        return result;
    }

    Result<CompletionPollResult> PollCompletions(
        const CompletionPollRequest& request) {
        std::unique_lock lock(mutex_);
        if (completions_.empty()) {
            if (request.timeout_ms == 0) {
                return WouldBlock("TCP completion queue is empty");
            }
            const bool ready = completion_cv_.wait_for(
                lock, std::chrono::milliseconds(request.timeout_ms), [this] {
                    return !completions_.empty() ||
                           stop_requested_.load(std::memory_order_acquire) ||
                           worker_failed_;
                });
            if (!ready) return Timeout("TCP completion poll timed out");
            if (completions_.empty()) {
                return Unavailable("TCP completion worker stopped");
            }
        }
        CompletionPollResult result;
        result.completions.reserve(
            std::min<size_t>(request.max_completions, completions_.size()));
        while (!completions_.empty() &&
               result.completions.size() < request.max_completions) {
            result.completions.push_back(std::move(completions_.front()));
            completions_.pop_front();
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
            Wake();
            return Status::Ok();
        }
        if (recently_closed_.contains(id)) return Status::Ok();
        return Status::Error(StatusCode::kNotFound,
                             "TCP connection does not exist");
    }

    TcpDriverStats Stats() const noexcept {
        std::lock_guard lock(mutex_);
        return TcpDriverStats{
            .active_connections = connections_.size(),
            .listeners = listeners_.size(),
            .queued_send_bytes = total_send_bytes_,
            .ready_receive_bytes = ready_receive_bytes_,
            .ready_receive_messages = ready_messages_.size(),
            .pending_accepts = accepted_.size(),
        };
    }

private:
    struct PendingWrite {
        std::vector<std::byte> bytes;
        size_t offset = 0;
        SendOperation operation;
    };

    struct Connection {
        ConnectionInfo info;
        int fd = -1;
        bool closing = false;
        std::array<std::byte, kTcpPrefixBytes> prefix{};
        size_t prefix_size = 0;
        uint32_t expected_body_size = 0;
        std::vector<std::byte> body;
        size_t body_size = 0;
        size_t reserved_body_bytes = 0;
        std::optional<TimePoint> partial_frame_started;
        std::deque<PendingWrite> writes;
        std::vector<SendOperation> awaiting_ack;
        size_t queued_send_bytes = 0;
        bool heartbeat_pending = false;
        size_t heartbeat_offset = 0;
        TimePoint last_valid_receive{};
        TimePoint last_transmit{};
    };

    struct Listener {
        ConnectionInfo info;
        int fd = -1;
        bool closing = false;
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

    void Wake() const noexcept {
        if (wake_write_fd_ < 0) return;
        const std::byte byte{1};
        const ssize_t ignored = ::write(wake_write_fd_, &byte, 1);
        (void)ignored;
    }

    void DrainWakePipe() noexcept {
        std::array<std::byte, 128> bytes{};
        while (::read(wake_read_fd_, bytes.data(), bytes.size()) > 0) {
        }
    }

    void CloseWakePipeLocked() noexcept {
        if (wake_read_fd_ >= 0) (void)::close(wake_read_fd_);
        if (wake_write_fd_ >= 0) (void)::close(wake_write_fd_);
        wake_read_fd_ = -1;
        wake_write_fd_ = -1;
    }

    void WorkerLoop() noexcept {
        try {
            while (!stop_requested_.load(std::memory_order_acquire)) {
                std::vector<pollfd> descriptors;
                std::vector<PollToken> tokens;
                {
                    std::lock_guard lock(mutex_);
                    ProcessClosuresAndTimersLocked(Clock::now());
                    descriptors.reserve(1 + listeners_.size() +
                                        connections_.size());
                    tokens.reserve(descriptors.capacity());
                    descriptors.push_back(
                        pollfd{.fd = wake_read_fd_,
                               .events = POLLIN,
                               .revents = 0});
                    tokens.push_back(PollToken{
                        .kind = PollKind::kWake,
                        .id = kInvalidConnectionId,
                        .fd = wake_read_fd_,
                    });
                    const bool can_accept =
                        accepted_.size() < options_.max_pending_accepts &&
                        connections_.size() < config_.max_connections;
                    for (const auto& [id, listener] : listeners_) {
                        if (!listener.closing && can_accept) {
                            descriptors.push_back(pollfd{
                                .fd = listener.fd,
                                .events = POLLIN,
                                .revents = 0,
                            });
                            tokens.push_back(PollToken{
                                .kind = PollKind::kListener,
                                .id = id,
                                .fd = listener.fd,
                            });
                        }
                    }
                    for (auto& [id, connection] : connections_) {
                        PrepareReceiveReservationLocked(connection);
                        PrepareHeartbeatLocked(connection, Clock::now());
                        short events = 0;
                        if (CanReadLocked(connection)) events |= POLLIN;
                        if (!connection.writes.empty() ||
                            connection.heartbeat_pending) {
                            events |= POLLOUT;
                        }
                        descriptors.push_back(pollfd{
                            .fd = connection.fd,
                            .events = events,
                            .revents = 0,
                        });
                        tokens.push_back(PollToken{
                            .kind = PollKind::kConnection,
                            .id = id,
                            .fd = connection.fd,
                        });
                    }
                }

                int polled = ::poll(descriptors.data(), descriptors.size(),
                                    static_cast<int>(options_.io_poll_max_ms));
                if (polled < 0) {
                    if (errno == EINTR) continue;
                    FailWorker();
                    return;
                }
                if (polled == 0) continue;
                for (size_t i = 0; i < descriptors.size(); ++i) {
                    if (descriptors[i].revents == 0) continue;
                    const PollToken token = tokens[i];
                    if (token.kind == PollKind::kWake) {
                        DrainWakePipe();
                        continue;
                    }
                    std::lock_guard lock(mutex_);
                    if (token.kind == PollKind::kListener) {
                        ProcessListenerEventLocked(token, descriptors[i].revents);
                    } else {
                        ProcessConnectionEventLocked(token,
                                                     descriptors[i].revents);
                    }
                }
            }
        } catch (const std::bad_alloc&) {
            FailWorker();
        } catch (...) {
            FailWorker();
        }
    }

    void FailWorker() noexcept {
        health_->store(HealthState::kUnavailable, std::memory_order_release);
        stop_requested_.store(true, std::memory_order_release);
        {
            std::lock_guard lock(mutex_);
            worker_failed_ = true;
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
        for (const auto& [id, connection] : connections_) {
            if (connection.closing) {
                close_connections.emplace_back(
                    id, Unavailable("TCP connection was closed"));
            } else if (now - connection.last_valid_receive >= idle_timeout) {
                close_connections.emplace_back(
                    id, Unavailable("TCP connection idle timeout"));
            } else if (connection.partial_frame_started.has_value() &&
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
        if (found->second.fd >= 0) (void)::close(found->second.fd);
        listeners_.erase(found);
        RememberClosedLocked(id);
        accept_cv_.notify_all();
    }

    void CloseConnectionLocked(ConnectionId id, const Status& failure) {
        const auto found = connections_.find(id);
        if (found == connections_.end()) return;
        Connection& connection = found->second;
        if (connection.fd >= 0) (void)::close(connection.fd);
        if (connection.reserved_body_bytes != 0) {
            reserved_receive_bytes_ -= connection.reserved_body_bytes;
            --reserved_receive_messages_;
        }
        for (const PendingWrite& write : connection.writes) {
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
        total_send_bytes_ -= connection.queued_send_bytes;

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
        if (!connection.heartbeat_pending && connection.writes.empty() &&
            now - connection.last_transmit >=
                std::chrono::milliseconds(options_.heartbeat_interval_ms)) {
            connection.heartbeat_pending = true;
            connection.heartbeat_offset = 0;
        }
    }

    bool CanReadLocked(const Connection& connection) const noexcept {
        if (connection.closing) return false;
        if (connection.expected_body_size != 0 &&
            connection.reserved_body_bytes == 0) {
            return false;
        }
        if (connection.reserved_body_bytes != 0) return true;
        return ready_messages_.size() + reserved_receive_messages_ <
                   options_.max_ready_receive_messages &&
               ready_receive_bytes_ + reserved_receive_bytes_ <
                   options_.max_ready_receive_bytes;
    }

    void PrepareReceiveReservationLocked(Connection& connection) {
        if (connection.expected_body_size == 0 ||
            connection.reserved_body_bytes != 0) {
            return;
        }
        const size_t body_size = connection.expected_body_size;
        if (ready_messages_.size() + reserved_receive_messages_ >=
                options_.max_ready_receive_messages ||
            ready_receive_bytes_ + reserved_receive_bytes_ >
                options_.max_ready_receive_bytes ||
            body_size > options_.max_ready_receive_bytes -
                            ready_receive_bytes_ - reserved_receive_bytes_) {
            return;
        }
        connection.body.resize(body_size);
        connection.body_size = 0;
        connection.reserved_body_bytes = body_size;
        reserved_receive_bytes_ += body_size;
        ++reserved_receive_messages_;
    }

    void ProcessListenerEventLocked(const PollToken& token, short events) {
        const auto found = listeners_.find(token.id);
        if (found == listeners_.end() || found->second.fd != token.fd) return;
        if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            CloseListenerLocked(token.id);
            return;
        }
        if ((events & POLLIN) == 0) return;

        while (accepted_.size() < options_.max_pending_accepts &&
               connections_.size() < config_.max_connections) {
            sockaddr_storage peer_storage{};
            socklen_t peer_size = sizeof(peer_storage);
            UniqueFd accepted_fd(::accept(
                found->second.fd, reinterpret_cast<sockaddr*>(&peer_storage),
                &peer_size));
            if (accepted_fd.get() < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                CloseListenerLocked(token.id);
                return;
            }
            Status configured = ConfigureTcpSocket(accepted_fd.get());
            if (!configured.ok()) continue;

            sockaddr_storage local_storage{};
            socklen_t local_size = sizeof(local_storage);
            if (::getsockname(accepted_fd.get(),
                              reinterpret_cast<sockaddr*>(&local_storage),
                              &local_size) != 0) {
                continue;
            }
            auto local_endpoint =
                FromSocketAddress(local_storage, local_size);
            auto peer_endpoint = FromSocketAddress(peer_storage, peer_size);
            if (!local_endpoint.ok() || !peer_endpoint.ok()) continue;
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
            connection.last_valid_receive = now;
            connection.last_transmit = now;
            const ConnectionInfo info = connection.info;
            connections_.emplace(id, std::move(connection));
            accepted_.push_back(AcceptedConnection{
                .listener_id = token.id,
                .info = info,
            });
            accept_cv_.notify_all();
        }
    }

    void ProcessConnectionEventLocked(const PollToken& token, short events) {
        auto found = connections_.find(token.id);
        if (found == connections_.end() || found->second.fd != token.fd) return;
        if ((events & (POLLERR | POLLNVAL)) != 0) {
            CloseConnectionLocked(token.id,
                                  Unavailable("TCP socket poll failed"));
            return;
        }
        if ((events & POLLIN) != 0) {
            const Status read_status = ReadConnectionLocked(found->second);
            if (!read_status.ok()) {
                CloseConnectionLocked(token.id, read_status);
                return;
            }
        }
        found = connections_.find(token.id);
        if (found == connections_.end()) return;
        if ((events & POLLOUT) != 0) {
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

    Status ReadConnectionLocked(Connection& connection) {
        size_t budget = kIoBudgetBytes;
        while (budget != 0) {
            if (connection.prefix_size < kTcpPrefixBytes) {
                const size_t remaining = kTcpPrefixBytes - connection.prefix_size;
                const size_t amount = std::min(remaining, budget);
                const ssize_t received = ::recv(
                    connection.fd,
                    connection.prefix.data() + connection.prefix_size, amount, 0);
                if (received == 0) {
                    return connection.prefix_size == 0 &&
                                   connection.expected_body_size == 0
                               ? Unavailable("TCP peer closed connection")
                               : Corruption("TCP stream ended inside frame prefix");
                }
                if (received < 0) {
                    if (errno == EINTR) continue;
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        return Status::Ok();
                    }
                    return Unavailable("TCP receive failed");
                }
                if (!connection.partial_frame_started.has_value()) {
                    connection.partial_frame_started = Clock::now();
                }
                connection.prefix_size += static_cast<size_t>(received);
                budget -= static_cast<size_t>(received);
                if (connection.prefix_size < kTcpPrefixBytes) continue;

                connection.expected_body_size = LoadBe32(connection.prefix);
                if (connection.expected_body_size <
                        bridge::kWireBaseHeaderLength ||
                    connection.expected_body_size >
                        options_.max_frame_body_bytes) {
                    return Corruption("TCP frame length prefix is out of bounds");
                }
                PrepareReceiveReservationLocked(connection);
                if (connection.reserved_body_bytes == 0) {
                    return Status::Ok();
                }
            }

            if (connection.reserved_body_bytes == 0) {
                PrepareReceiveReservationLocked(connection);
                if (connection.reserved_body_bytes == 0) return Status::Ok();
            }
            const size_t remaining =
                connection.expected_body_size - connection.body_size;
            const size_t amount = std::min(remaining, budget);
            const ssize_t received = ::recv(
                connection.fd, connection.body.data() + connection.body_size,
                amount, 0);
            if (received == 0) {
                return Corruption("TCP stream ended inside frame body");
            }
            if (received < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return Status::Ok();
                }
                return Unavailable("TCP receive failed");
            }
            connection.body_size += static_cast<size_t>(received);
            budget -= static_cast<size_t>(received);
            if (connection.body_size < connection.expected_body_size) continue;

            bridge::WireFrameLimits limits;
            limits.max_payload_length = options_.max_frame_body_bytes;
            limits.max_buffered_bytes =
                static_cast<size_t>(options_.max_frame_body_bytes) +
                kTcpPrefixBytes;
            auto decoded = bridge::WireFrameCodec::Decode(connection.body,
                                                           limits);
            if (!decoded.ok()) {
                return Corruption("TCP stream contains an invalid Wire Frame");
            }
            connection.last_valid_receive = Clock::now();
            const size_t complete_size = connection.expected_body_size;
            reserved_receive_bytes_ -= connection.reserved_body_bytes;
            --reserved_receive_messages_;
            connection.reserved_body_bytes = 0;

            if (decoded->header.frame_type != bridge::FrameType::kHeartbeat) {
                ready_receive_bytes_ += complete_size;
                ready_messages_.push_back(ReceivedMessage{
                    .connection_id = connection.info.id,
                    .from = *connection.info.peer_endpoint,
                    .payload = std::move(connection.body),
                });
                receive_cv_.notify_all();
            } else {
                connection.body.clear();
            }
            connection.prefix.fill(std::byte{0});
            connection.prefix_size = 0;
            connection.expected_body_size = 0;
            connection.body_size = 0;
            connection.partial_frame_started.reset();
            if (ready_messages_.size() >=
                    options_.max_ready_receive_messages ||
                ready_receive_bytes_ + reserved_receive_bytes_ >=
                    options_.max_ready_receive_bytes) {
                return Status::Ok();
            }
        }
        return Status::Ok();
    }

    Status WriteConnectionLocked(Connection& connection) {
        size_t budget = kIoBudgetBytes;
        while (budget != 0 && !connection.writes.empty()) {
            PendingWrite& write = connection.writes.front();
            const size_t remaining = write.bytes.size() - write.offset;
            const size_t amount = std::min(remaining, budget);
            const ssize_t sent = SendNoSignal(
                connection.fd, write.bytes.data() + write.offset, amount);
            if (sent < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return Status::Ok();
                }
                return Unavailable("TCP send failed");
            }
            if (sent == 0) return Unavailable("TCP send made no progress");
            write.offset += static_cast<size_t>(sent);
            budget -= static_cast<size_t>(sent);
            connection.last_transmit = Clock::now();
            if (write.offset != write.bytes.size()) continue;

            const size_t wire_size = write.bytes.size();
            connection.awaiting_ack.push_back(write.operation);
            connection.writes.pop_front();
            connection.queued_send_bytes -= wire_size;
            total_send_bytes_ -= wire_size;
            // Deliberately no successful DeliveryCompletion here: TCP write is
            // not remote acceptance. D4-09 moves awaiting_ack via peer ACKs.
        }
        if (!connection.writes.empty() || !connection.heartbeat_pending ||
            budget == 0) {
            return Status::Ok();
        }

        const size_t remaining =
            heartbeat_wire_.size() - connection.heartbeat_offset;
        const size_t amount = std::min(remaining, budget);
        ssize_t sent = -1;
        do {
            sent = SendNoSignal(
                connection.fd,
                heartbeat_wire_.data() + connection.heartbeat_offset, amount);
        } while (sent < 0 && errno == EINTR);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return Status::Ok();
            return Unavailable("TCP heartbeat send failed");
        }
        if (sent == 0) return Unavailable("TCP heartbeat made no progress");
        connection.heartbeat_offset += static_cast<size_t>(sent);
        connection.last_transmit = Clock::now();
        if (connection.heartbeat_offset == heartbeat_wire_.size()) {
            connection.heartbeat_pending = false;
            connection.heartbeat_offset = 0;
        }
        return Status::Ok();
    }

    TcpDriverOptions options_;
    std::atomic<HealthState>* health_;
    std::vector<std::byte> heartbeat_wire_;
    DriverConfig config_{};

    mutable std::mutex mutex_;
    std::condition_variable receive_cv_;
    std::condition_variable completion_cv_;
    std::condition_variable accept_cv_;
    std::thread worker_;
    std::atomic<bool> stop_requested_{true};
    bool worker_failed_ = false;
    int wake_read_fd_ = -1;
    int wake_write_fd_ = -1;
    ConnectionId next_id_ = 1;
    std::unordered_map<ConnectionId, Connection> connections_;
    std::unordered_map<ConnectionId, Listener> listeners_;
    std::deque<AcceptedConnection> accepted_;
    std::deque<ReceivedMessage> ready_messages_;
    std::deque<DeliveryCompletion> completions_;
    std::unordered_set<ConnectionId> recently_closed_;
    size_t total_send_bytes_ = 0;
    size_t ready_receive_bytes_ = 0;
    size_t reserved_receive_bytes_ = 0;
    size_t reserved_receive_messages_ = 0;
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
        .features = Capability::kConnect | Capability::kListen,
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

Result<ReceiveResult> TcpDriver::DoPoll(const ReceiveRequest& request) {
    return impl_->PollMessages(request);
}

Result<CompletionPollResult> TcpDriver::DoPollCompletions(
    const CompletionPollRequest& request) {
    return impl_->PollCompletions(request);
}

Status TcpDriver::DoClose(ConnectionId connection_id) {
    return impl_->Close(connection_id);
}

}  // namespace mino::transport