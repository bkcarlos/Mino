// Copyright 2026 The Mino Authors

#include "mino/transport/udp_driver.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <span>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace mino::transport {
namespace {

using Clock = std::chrono::steady_clock;

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
Status Internal(const char* message) {
    return Status::Error(StatusCode::kInternal, message);
}
Status AllocationFailure() {
    return Exhausted("UDP driver allocation failed");
}

class UniqueFd final {
public:
    explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    ~UniqueFd() {
        if (fd_ >= 0) (void)::close(fd_);
    }
    int get() const noexcept { return fd_; }
    int release() noexcept {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

private:
    int fd_;
};

Status SetDescriptorFlags(int fd) {
    const int status_flags = ::fcntl(fd, F_GETFL, 0);
    if (status_flags < 0 ||
        ::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0) {
        return Internal("failed to make UDP descriptor non-blocking");
    }
    const int descriptor_flags = ::fcntl(fd, F_GETFD, 0);
    if (descriptor_flags < 0 ||
        ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0) {
        return Internal("failed to make UDP descriptor close-on-exec");
    }
    return Status::Ok();
}

struct SocketAddress {
    sockaddr_storage storage{};
    socklen_t size = 0;
};

Result<SocketAddress> ToSocketAddress(const EndpointDescriptor& endpoint) {
    if (endpoint.kind() != TransportKind::kNetwork ||
        endpoint.protocol() != NetworkProtocol::kUdp) {
        return Invalid("UDP driver requires a network UDP endpoint");
    }
    SocketAddress result;
    if (endpoint.address_family() == EndpointAddressFamily::kIpv4) {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(endpoint.port());
        const auto bytes = endpoint.ip_address();
        if (bytes.size() != sizeof(address.sin_addr)) {
            return Invalid("UDP IPv4 endpoint size is invalid");
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
            return Invalid("UDP IPv6 endpoint size is invalid");
        }
        std::memcpy(&address.sin6_addr, bytes.data(), bytes.size());
        std::memcpy(&result.storage, &address, sizeof(address));
        result.size = sizeof(address);
        return result;
    }
    return Invalid("UDP endpoint address family is unsupported");
}

Result<EndpointDescriptor> FromSocketAddress(const sockaddr_storage& storage,
                                             socklen_t size) {
    if (storage.ss_family == AF_INET && size >= sizeof(sockaddr_in)) {
        const auto* address = reinterpret_cast<const sockaddr_in*>(&storage);
        return EndpointDescriptor::Ipv4Udp(
            std::as_bytes(std::span(&address->sin_addr, size_t{1})),
            ntohs(address->sin_port));
    }
    if (storage.ss_family == AF_INET6 && size >= sizeof(sockaddr_in6)) {
        const auto* address = reinterpret_cast<const sockaddr_in6*>(&storage);
        return EndpointDescriptor::Ipv6Udp(
            std::as_bytes(std::span(&address->sin6_addr, size_t{1})),
            ntohs(address->sin6_port));
    }
    return Status::Error(StatusCode::kUnsupported,
                         "UDP socket address family is unsupported");
}

int SocketFamily(const EndpointDescriptor& endpoint) noexcept {
    if (endpoint.address_family() == EndpointAddressFamily::kIpv4) return AF_INET;
    if (endpoint.address_family() == EndpointAddressFamily::kIpv6) return AF_INET6;
    return -1;
}

}  // namespace

Status ValidateUdpDriverOptions(const UdpDriverOptions& options) {
    if (options.max_datagram_bytes == 0 ||
        options.max_datagram_bytes > kUdpMaximumDatagramBytes) {
        return Invalid("UDP datagram bound is invalid");
    }
    if (options.socket_receive_buffer_bytes < options.max_datagram_bytes ||
        options.socket_receive_buffer_bytes > kMaxReceiveBatchBytes) {
        return Invalid("UDP receive buffer bound is invalid");
    }
    if (options.io_poll_max_ms == 0 || options.io_poll_max_ms > 1000) {
        return Invalid("UDP poll interval is invalid");
    }
    return Status::Ok();
}

class UdpDriver::Impl final {
public:
    Impl(UdpDriverOptions options, std::atomic<HealthState>* health)
        : options_(options), health_(health) {}

    Status Start(const DriverConfig& config) {
        std::lock_guard lock(mutex_);
        int pipe_fds[2] = {-1, -1};
        if (::pipe(pipe_fds) != 0) {
            return Internal("failed to create UDP wake pipe");
        }
        UniqueFd read_end(pipe_fds[0]);
        UniqueFd write_end(pipe_fds[1]);
        MINO_RETURN_IF_ERROR(SetDescriptorFlags(read_end.get()));
        MINO_RETURN_IF_ERROR(SetDescriptorFlags(write_end.get()));
        config_ = config;
        completions_.clear();
        completions_.resize(config.max_queued_sends);
        completion_head_ = 0;
        completion_tail_ = 0;
        completion_count_ = 0;
        next_poll_connection_id_ = kInvalidConnectionId;
        stop_requested_.store(false, std::memory_order_release);
        wake_read_fd_ = read_end.release();
        wake_write_fd_ = write_end.release();
        health_->store(HealthState::kHealthy, std::memory_order_release);
        return Status::Ok();
    }

    void RequestStop() noexcept {
        stop_requested_.store(true, std::memory_order_release);
        Wake();
        completion_cv_.notify_all();
    }

    Status Shutdown() noexcept {
        std::lock_guard lock(mutex_);
        for (auto& [id, socket] : sockets_) {
            (void)id;
            if (socket.fd >= 0) (void)::close(socket.fd);
        }
        sockets_.clear();
        closed_.clear();
        completions_.clear();
        completion_count_ = 0;
        if (wake_read_fd_ >= 0) (void)::close(wake_read_fd_);
        if (wake_write_fd_ >= 0) (void)::close(wake_write_fd_);
        wake_read_fd_ = -1;
        wake_write_fd_ = -1;
        health_->store(HealthState::kUnavailable, std::memory_order_release);
        return Status::Ok();
    }

    Result<ConnectionInfo> Connect(const ConnectRequest& request) {
        MINO_ASSIGN_OR_RETURN(const SocketAddress remote,
                              ToSocketAddress(request.remote_endpoint));
        UniqueFd fd(::socket(SocketFamily(request.remote_endpoint), SOCK_DGRAM,
                             IPPROTO_UDP));
        if (fd.get() < 0) return Unavailable("failed to create UDP socket");
        MINO_RETURN_IF_ERROR(ConfigureSocket(fd.get()));
        if (request.local_bind.has_value()) {
            MINO_ASSIGN_OR_RETURN(const SocketAddress local,
                                  ToSocketAddress(*request.local_bind));
            if (::bind(fd.get(), reinterpret_cast<const sockaddr*>(&local.storage),
                       local.size) != 0) {
                return Unavailable("failed to bind UDP socket");
            }
        }
        if (::connect(fd.get(), reinterpret_cast<const sockaddr*>(&remote.storage),
                      remote.size) != 0) {
            return Unavailable("failed to connect UDP socket");
        }
        sockaddr_storage local_storage{};
        socklen_t local_size = sizeof(local_storage);
        if (::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&local_storage),
                          &local_size) != 0) {
            return Internal("failed to query UDP local endpoint");
        }
        MINO_ASSIGN_OR_RETURN(auto local_endpoint,
                              FromSocketAddress(local_storage, local_size));

        Wake();
        std::lock_guard lock(mutex_);
        if (sockets_.size() >= config_.max_connections) {
            return Exhausted("UDP socket limit reached");
        }
        MINO_ASSIGN_OR_RETURN(const ConnectionId id, NextIdLocked());
        ConnectionInfo info{
            .id = id,
            .kind = TransportKind::kNetwork,
            .is_listener = false,
            .local_endpoint = local_endpoint,
            .peer_endpoint = request.remote_endpoint,
        };
        sockets_.emplace(id, SocketState{.info = info, .fd = fd.release()});
        return info;
    }

    Result<ConnectionInfo> Listen(const ListenRequest& request) {
        MINO_ASSIGN_OR_RETURN(const SocketAddress local,
                              ToSocketAddress(request.local_endpoint));
        UniqueFd fd(::socket(SocketFamily(request.local_endpoint), SOCK_DGRAM,
                             IPPROTO_UDP));
        if (fd.get() < 0) return Unavailable("failed to create UDP listener");
        MINO_RETURN_IF_ERROR(ConfigureSocket(fd.get()));
        const int enabled = 1;
        if (::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &enabled,
                         sizeof(enabled)) != 0 ||
            ::bind(fd.get(), reinterpret_cast<const sockaddr*>(&local.storage),
                   local.size) != 0) {
            return Unavailable("failed to bind UDP listener");
        }

        Wake();
        std::lock_guard lock(mutex_);
        size_t listeners = 0;
        for (const auto& [id, socket] : sockets_) {
            (void)id;
            if (socket.info.is_listener) ++listeners;
            if (socket.info.is_listener &&
                socket.info.local_endpoint == request.local_endpoint) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "UDP endpoint is already listening");
            }
        }
        if (listeners >= config_.max_listeners) {
            return Exhausted("UDP listener limit reached");
        }
        MINO_ASSIGN_OR_RETURN(const ConnectionId id, NextIdLocked());
        ConnectionInfo info{
            .id = id,
            .kind = TransportKind::kNetwork,
            .is_listener = true,
            .local_endpoint = request.local_endpoint,
            .peer_endpoint = std::nullopt,
        };
        sockets_.emplace(id, SocketState{.info = info, .fd = fd.release()});
        return info;
    }

    Result<SendResult> Send(const SendRequest& request,
                            SendOperation operation) {
        Wake();
        std::lock_guard lock(mutex_);
        const auto found = sockets_.find(request.connection_id);
        if (found == sockets_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "UDP connection does not exist");
        }
        if (found->second.info.is_listener) {
            return Status::Error(StatusCode::kUnsupported,
                                 "UDP listener has no configured send peer");
        }
        ssize_t sent = -1;
        do {
            sent = ::send(found->second.fd, request.payload.data(),
                          request.payload.size(), 0);
        } while (sent < 0 && errno == EINTR);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return WouldBlock("UDP socket send buffer is full");
            }
            return Unavailable("UDP datagram send failed");
        }
        if (static_cast<size_t>(sent) != request.payload.size()) {
            return Internal("UDP socket reported a partial datagram send");
        }
        if (completion_count_ >= completions_.size()) {
            return Internal("UDP completion ring is inconsistent");
        }
        completions_[completion_tail_] = DeliveryCompletion{
            .operation = operation,
            .reached_stage = DeliveryStage::kLocalPublished,
            .status = Status::Error(StatusCode::kDegraded),
        };
        completion_tail_ = (completion_tail_ + 1) % completions_.size();
        ++completion_count_;
        completion_cv_.notify_all();
        return SendResult{
            .operation = operation,
            .admitted_bytes = request.payload.size(),
        };
    }

    Result<size_t> SendUntracked(const UntrackedSendRequest& request) {
        Wake();
        std::lock_guard lock(mutex_);
        const auto found = sockets_.find(request.connection_id);
        if (found == sockets_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "UDP connection does not exist");
        }
        if (found->second.info.is_listener) {
            return Status::Error(StatusCode::kUnsupported,
                                 "UDP listener has no configured send peer");
        }
        ssize_t sent = -1;
        do {
            sent = ::send(found->second.fd, request.payload.data(),
                          request.payload.size(), 0);
        } while (sent < 0 && errno == EINTR);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return WouldBlock("UDP socket send buffer is full");
            }
            return Unavailable("UDP datagram send failed");
        }
        if (static_cast<size_t>(sent) != request.payload.size()) {
            return Internal("UDP socket reported a partial datagram send");
        }
        return request.payload.size();
    }

    Result<ReceiveResult> PollMessages(const ReceiveRequest& request) {
        std::unique_lock lock(mutex_);
        const auto deadline =
            Clock::now() + std::chrono::milliseconds(request.timeout_ms);
        for (;;) {
            if (stop_requested_.load(std::memory_order_acquire)) {
                return Unavailable("UDP driver is stopping");
            }
            std::vector<pollfd> descriptors;
            std::vector<ConnectionId> ids;
            descriptors.reserve(1 + sockets_.size());
            ids.reserve(descriptors.capacity());
            descriptors.push_back(
                pollfd{.fd = wake_read_fd_, .events = POLLIN, .revents = 0});
            ids.push_back(kInvalidConnectionId);
            for (const auto& [id, socket] : sockets_) {
                if (request.connection_id != kInvalidConnectionId &&
                    id != request.connection_id) {
                    continue;
                }
                descriptors.push_back(
                    pollfd{.fd = socket.fd, .events = POLLIN, .revents = 0});
                ids.push_back(id);
            }
            int timeout_ms = 0;
            if (request.timeout_ms != 0) {
                const auto remaining =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        deadline - Clock::now());
                if (remaining.count() <= 0) {
                    return Timeout("UDP receive timed out");
                }
                timeout_ms = static_cast<int>(std::min<int64_t>(
                    options_.io_poll_max_ms, remaining.count()));
            }
            const int polled =
                ::poll(descriptors.data(), descriptors.size(), timeout_ms);
            if (polled < 0) {
                if (errno == EINTR) continue;
                return Unavailable("UDP receive poll failed");
            }
            if (polled == 0) {
                if (request.timeout_ms == 0) {
                    return WouldBlock("UDP receive queue is empty");
                }
                if (Clock::now() >= deadline) {
                    return Timeout("UDP receive timed out");
                }
                continue;
            }
            if ((descriptors[0].revents & POLLIN) != 0) {
                DrainWakePipe();
                lock.unlock();
                std::this_thread::yield();
                lock.lock();
                continue;
            }

            ReceiveResult result;
            result.messages.reserve(request.max_messages);
            std::vector<std::byte> inspection_buffer(
                static_cast<size_t>(options_.max_datagram_bytes) + 1);
            size_t total_bytes = 0;
            size_t inspected_datagrams = 0;
            const size_t inspected_datagram_budget = request.max_messages;
            const size_t socket_descriptor_count = descriptors.size() - 1;
            size_t next_descriptor_offset = 0;
            if (next_poll_connection_id_ != kInvalidConnectionId) {
                for (size_t offset = 0; offset < socket_descriptor_count;
                     ++offset) {
                    if (ids[offset + 1] == next_poll_connection_id_) {
                        next_descriptor_offset = offset;
                        break;
                    }
                }
            }
            while (result.messages.size() < request.max_messages &&
                   inspected_datagrams < inspected_datagram_budget) {
                bool inspected_in_sweep = false;
                const size_t sweep_start = next_descriptor_offset;
                for (size_t offset = 0;
                     offset < socket_descriptor_count &&
                     result.messages.size() < request.max_messages &&
                     inspected_datagrams < inspected_datagram_budget;
                     ++offset) {
                    const size_t descriptor_offset =
                        (sweep_start + offset) % socket_descriptor_count;
                    const size_t index = descriptor_offset + 1;
                    if ((descriptors[index].revents & POLLIN) == 0) continue;
                    if (stop_requested_.load(std::memory_order_acquire)) {
                        if (!result.messages.empty()) return result;
                        return Unavailable("UDP driver is stopping");
                    }
                    if (request.timeout_ms != 0 && Clock::now() >= deadline) {
                        if (!result.messages.empty()) return result;
                        return Timeout("UDP receive timed out");
                    }
                    ssize_t inspected = -1;
                    do {
                        inspected = ::recv(descriptors[index].fd,
                                           inspection_buffer.data(),
                                           inspection_buffer.size(), MSG_PEEK);
                    } while (inspected < 0 && errno == EINTR);
                    if (inspected < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                        return Unavailable("failed to inspect UDP datagram size");
                    }
                    inspected_in_sweep = true;
                    ++inspected_datagrams;
                    next_descriptor_offset =
                        (descriptor_offset + 1) % socket_descriptor_count;
                    next_poll_connection_id_ =
                        ids[next_descriptor_offset + 1];
                    const size_t size = static_cast<size_t>(inspected);
                    if (size > options_.max_datagram_bytes) {
                        if (!result.messages.empty()) return result;
                        ssize_t discarded = -1;
                        do {
                            discarded = ::recv(descriptors[index].fd,
                                               inspection_buffer.data(),
                                               inspection_buffer.size(), 0);
                        } while (discarded < 0 && errno == EINTR);
                        if (discarded < 0 && errno != EAGAIN &&
                            errno != EWOULDBLOCK) {
                            return Unavailable(
                                "failed to discard oversized UDP datagram");
                        }
                        return Exhausted("UDP datagram exceeds configured bound");
                    }
                    if (size == 0) {
                        std::array<std::byte, 1> discard{};
                        ssize_t discarded = -1;
                        do {
                            discarded = ::recv(descriptors[index].fd,
                                               discard.data(), discard.size(), 0);
                        } while (discarded < 0 && errno == EINTR);
                        if (discarded < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                            return Unavailable(
                                "failed to discard empty UDP datagram");
                        }
                        if (discarded != 0) {
                            return Internal(
                                "UDP datagram size changed while receiving");
                        }
                        continue;
                    }
                    if (size > request.max_bytes - total_bytes) {
                        if (result.messages.empty()) {
                            return Exhausted(
                                "next UDP datagram exceeds receive byte budget");
                        }
                        return result;
                    }
                    sockaddr_storage peer{};
                    socklen_t peer_size = sizeof(peer);
                    std::vector<std::byte> payload(size);
                    ssize_t received = -1;
                    do {
                        received = ::recvfrom(
                            descriptors[index].fd, payload.data(), payload.size(),
                            0, reinterpret_cast<sockaddr*>(&peer), &peer_size);
                    } while (received < 0 && errno == EINTR);
                    if (received < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                        return Unavailable("UDP datagram receive failed");
                    }
                    if (static_cast<size_t>(received) != size) {
                        return Internal("UDP datagram size changed while receiving");
                    }
                    MINO_ASSIGN_OR_RETURN(auto from,
                                          FromSocketAddress(peer, peer_size));
                    result.messages.push_back(ReceivedMessage{
                        .connection_id = ids[index],
                        .from = from,
                        .payload = std::move(payload),
                    });
                    total_bytes += size;
                }
                if (!inspected_in_sweep) break;
            }
            if (!result.messages.empty()) return result;
            if (request.timeout_ms == 0) {
                return WouldBlock("UDP receive queue is empty");
            }
            if (inspected_datagrams != 0) {
                lock.unlock();
                std::this_thread::yield();
                lock.lock();
            }
        }
    }

    Result<CompletionPollResult> PollCompletions(
        const CompletionPollRequest& request) {
        std::unique_lock lock(mutex_);
        const auto has_matching_completion = [this, &request] {
            for (size_t offset = 0; offset < completion_count_; ++offset) {
                const size_t index =
                    (completion_head_ + offset) % completions_.size();
                if (request.connection_id == kInvalidConnectionId ||
                    completions_[index].operation.connection_id ==
                        request.connection_id) {
                    return true;
                }
            }
            return false;
        };
        if (!has_matching_completion()) {
            if (request.timeout_ms == 0) {
                return WouldBlock("UDP completion queue is empty");
            }
            const bool ready = completion_cv_.wait_for(
                lock, std::chrono::milliseconds(request.timeout_ms),
                [this, &has_matching_completion] {
                    return has_matching_completion() ||
                           stop_requested_.load(std::memory_order_acquire);
                });
            if (!ready) return Timeout("UDP completion poll timed out");
            if (!has_matching_completion()) {
                return Unavailable("UDP driver is stopping");
            }
        }
        CompletionPollResult result;
        result.completions.reserve(
            std::min<size_t>(request.max_completions, completion_count_));
        const size_t initial_count = completion_count_;
        for (size_t scanned = 0; scanned < initial_count; ++scanned) {
            DeliveryCompletion completion =
                std::move(completions_[completion_head_]);
            completion_head_ = (completion_head_ + 1) % completions_.size();
            --completion_count_;
            const bool matches =
                request.connection_id == kInvalidConnectionId ||
                completion.operation.connection_id == request.connection_id;
            if (matches &&
                result.completions.size() < request.max_completions) {
                result.completions.push_back(std::move(completion));
                continue;
            }
            completions_[completion_tail_] = std::move(completion);
            completion_tail_ = (completion_tail_ + 1) % completions_.size();
            ++completion_count_;
        }
        return result;
    }

    Status Close(ConnectionId id) {
        Wake();
        std::lock_guard lock(mutex_);
        const auto found = sockets_.find(id);
        if (found == sockets_.end()) {
            return closed_.contains(id)
                       ? Status::Ok()
                       : Status::Error(StatusCode::kNotFound,
                                       "UDP connection does not exist");
        }
        if (found->second.fd >= 0) (void)::close(found->second.fd);
        sockets_.erase(found);
        closed_.insert(id);
        return Status::Ok();
    }

    UdpDriverStats Stats() const noexcept {
        std::lock_guard lock(mutex_);
        size_t listeners = 0;
        for (const auto& [id, socket] : sockets_) {
            (void)id;
            if (socket.info.is_listener) ++listeners;
        }
        return UdpDriverStats{
            .connected_sockets = sockets_.size() - listeners,
            .listeners = listeners,
            .pending_completions = completion_count_,
        };
    }

private:
    struct SocketState {
        ConnectionInfo info;
        int fd = -1;
    };

    Status ConfigureSocket(int fd) const {
        MINO_RETURN_IF_ERROR(SetDescriptorFlags(fd));
        const int receive_bytes = static_cast<int>(
            std::min<size_t>(options_.socket_receive_buffer_bytes,
                             static_cast<size_t>(std::numeric_limits<int>::max())));
        if (::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &receive_bytes,
                         sizeof(receive_bytes)) != 0) {
            return Internal("failed to configure UDP receive buffer");
        }
        return Status::Ok();
    }

    Result<ConnectionId> NextIdLocked() {
        for (;;) {
            ConnectionId id = next_id_++;
            if (next_id_ == kInvalidConnectionId) next_id_ = 1;
            if (id != kInvalidConnectionId && !sockets_.contains(id)) return id;
        }
    }

    void Wake() const noexcept {
        if (wake_write_fd_ < 0) return;
        const std::byte value{1};
        const ssize_t ignored = ::write(wake_write_fd_, &value, 1);
        (void)ignored;
    }

    void DrainWakePipe() const noexcept {
        std::array<std::byte, 64> bytes{};
        while (::read(wake_read_fd_, bytes.data(), bytes.size()) > 0) {
        }
    }

    UdpDriverOptions options_;
    std::atomic<HealthState>* health_;
    DriverConfig config_{};
    mutable std::mutex mutex_;
    std::condition_variable completion_cv_;
    std::atomic<bool> stop_requested_{true};
    int wake_read_fd_ = -1;
    int wake_write_fd_ = -1;
    ConnectionId next_id_ = 1;
    ConnectionId next_poll_connection_id_ = kInvalidConnectionId;
    std::unordered_map<ConnectionId, SocketState> sockets_;
    std::unordered_set<ConnectionId> closed_;
    std::vector<DeliveryCompletion> completions_;
    size_t completion_head_ = 0;
    size_t completion_tail_ = 0;
    size_t completion_count_ = 0;
};

Result<std::unique_ptr<UdpDriver>> UdpDriver::Create(UdpDriverOptions options) {
    try {
        MINO_RETURN_IF_ERROR(ValidateUdpDriverOptions(options));
        auto driver = std::unique_ptr<UdpDriver>(new UdpDriver(options));
        driver->impl_ = std::make_unique<Impl>(options, &driver->health_);
        return driver;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

UdpDriver::UdpDriver(UdpDriverOptions options) : options_(options) {}
UdpDriver::~UdpDriver() { (void)Shutdown(); }

TransportCapabilities UdpDriver::capabilities() const noexcept {
    return TransportCapabilities{
        .kind = TransportKind::kNetwork,
        .reliability = TransportReliability::kUnreliable,
        .max_frame_size = options_.max_datagram_bytes,
        .max_reassembly_bytes = 0,
        .features = Capability::kConnect | Capability::kListen,
    };
}

UdpDriverStats UdpDriver::stats() const noexcept {
    return impl_ == nullptr ? UdpDriverStats{} : impl_->Stats();
}
Status UdpDriver::DoStart(const DriverConfig& config) {
    return impl_->Start(config);
}
void UdpDriver::DoRequestStop() noexcept { impl_->RequestStop(); }
Status UdpDriver::DoShutdown() { return impl_->Shutdown(); }
Result<ConnectionInfo> UdpDriver::DoConnect(const ConnectRequest& request) {
    return impl_->Connect(request);
}
Result<ConnectionInfo> UdpDriver::DoListen(const ListenRequest& request) {
    return impl_->Listen(request);
}
Result<SendResult> UdpDriver::DoSend(const SendRequest& request,
                                     SendOperation operation) {
    return impl_->Send(request, operation);
}
Result<size_t> UdpDriver::DoSendUntracked(
    const UntrackedSendRequest& request) {
    return impl_->SendUntracked(request);
}

Result<ReceiveResult> UdpDriver::DoPoll(const ReceiveRequest& request) {
    return impl_->PollMessages(request);
}
Result<CompletionPollResult> UdpDriver::DoPollCompletions(
    const CompletionPollRequest& request) {
    return impl_->PollCompletions(request);
}
Status UdpDriver::DoClose(ConnectionId connection_id) {
    return impl_->Close(connection_id);
}

}  // namespace mino::transport
