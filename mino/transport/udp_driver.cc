// Copyright 2026 The Mino Authors

#include "mino/transport/udp_driver.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
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
using TimePoint = Clock::time_point;

constexpr size_t kReceiveDatagramBudget = 4096;

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
    return Exhausted("UDP driver allocation failed");
}

void StoreBe16(uint16_t value, std::span<std::byte> output) noexcept {
    output[0] = static_cast<std::byte>(value >> 8);
    output[1] = static_cast<std::byte>(value);
}

void StoreBe32(uint32_t value, std::span<std::byte> output) noexcept {
    output[0] = static_cast<std::byte>(value >> 24);
    output[1] = static_cast<std::byte>(value >> 16);
    output[2] = static_cast<std::byte>(value >> 8);
    output[3] = static_cast<std::byte>(value);
}

void StoreBe64(uint64_t value, std::span<std::byte> output) noexcept {
    StoreBe32(static_cast<uint32_t>(value >> 32), output.first<4>());
    StoreBe32(static_cast<uint32_t>(value), output.subspan<4, 4>());
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

uint64_t LoadBe64(std::span<const std::byte> input) noexcept {
    return (static_cast<uint64_t>(LoadBe32(input.first<4>())) << 32) |
           LoadBe32(input.subspan<4, 4>());
}

uint32_t Crc32(std::span<const std::byte> input) noexcept {
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

size_t FragmentPayloadCapacity(const UdpDriverOptions& options) noexcept {
    return options.max_datagram_bytes - kUdpFragmentHeaderBytes;
}

uint32_t RequiredFragmentCount(size_t message_bytes,
                               const UdpDriverOptions& options) noexcept {
    const size_t capacity = FragmentPayloadCapacity(options);
    return static_cast<uint32_t>((message_bytes + capacity - 1) / capacity);
}

std::array<std::byte, kUdpFragmentHeaderBytes> BuildFragmentHeader(
    uint64_t message_id, uint32_t fragment_id, uint32_t fragment_count,
    uint32_t total_length, uint32_t fragment_offset, uint32_t crc32) noexcept {
    std::array<std::byte, kUdpFragmentHeaderBytes> header{};
    StoreBe32(kUdpFragmentMagic, std::span<std::byte>(header).subspan<0, 4>());
    header[4] = static_cast<std::byte>(kUdpFragmentVersion);
    header[5] = static_cast<std::byte>(kUdpFragmentFlag);
    StoreBe16(kUdpFragmentHeaderBytes,
              std::span<std::byte>(header).subspan<6, 2>());
    StoreBe64(message_id, std::span<std::byte>(header).subspan<8, 8>());
    StoreBe32(fragment_id, std::span<std::byte>(header).subspan<16, 4>());
    StoreBe32(fragment_count, std::span<std::byte>(header).subspan<20, 4>());
    StoreBe32(total_length, std::span<std::byte>(header).subspan<24, 4>());
    StoreBe32(fragment_offset, std::span<std::byte>(header).subspan<28, 4>());
    StoreBe32(crc32, std::span<std::byte>(header).subspan<32, 4>());
    return header;
}

}  // namespace

Status ValidateUdpDriverOptions(const UdpDriverOptions& options) {
    if (options.max_datagram_bytes < kUdpMinimumFragmentDatagramBytes ||
        options.max_datagram_bytes > kUdpMaximumDatagramBytes) {
        return Invalid("UDP datagram bound is invalid");
    }
    if (options.max_message_bytes < options.max_datagram_bytes ||
        options.max_message_bytes > kMaxPayloadBytes) {
        return Invalid("UDP message bound is invalid");
    }
    if (options.max_fragments_per_message < 2 ||
        options.max_fragments_per_message > kUdpMaximumFragmentsPerMessage ||
        RequiredFragmentCount(options.max_message_bytes, options) >
            options.max_fragments_per_message) {
        return Invalid("UDP fragment-count bound is invalid");
    }
    if (options.max_reassembly_bytes_per_connection <
            options.max_message_bytes ||
        options.max_reassembly_bytes_global <
            options.max_reassembly_bytes_per_connection ||
        options.max_reassembly_bytes_global > kMaxReceiveBatchBytes) {
        return Invalid("UDP reassembly byte quotas are inconsistent");
    }
    if (options.max_reassembly_messages_per_connection == 0 ||
        options.max_reassembly_messages_global <
            options.max_reassembly_messages_per_connection ||
        options.max_reassembly_messages_global > kMaxQueuedSends) {
        return Invalid("UDP reassembly message quotas are inconsistent");
    }
    if (options.reassembly_timeout_ms == 0 ||
        options.reassembly_timeout_ms > kMaxOperationTimeoutMs) {
        return Invalid("UDP reassembly timeout is invalid");
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
        reassemblies_.clear();
        recently_completed_.clear();
        reassembly_bytes_ = 0;
        reassembly_messages_ = 0;
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

        std::lock_guard lock(mutex_);
        CleanupExpiredLocked(Clock::now());
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
        Wake();
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

        std::lock_guard lock(mutex_);
        CleanupExpiredLocked(Clock::now());
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
        Wake();
        return info;
    }

    Result<SendResult> Send(const SendRequest& request,
                            SendOperation operation) {
        std::lock_guard lock(mutex_);
        CleanupExpiredLocked(Clock::now());
        const auto found = sockets_.find(request.connection_id);
        if (found == sockets_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "UDP connection does not exist");
        }
        if (found->second.info.is_listener) {
            return Status::Error(StatusCode::kUnsupported,
                                 "UDP listener has no configured send peer");
        }
        MINO_RETURN_IF_ERROR(SendMessageLocked(found->second, request.payload));
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
        std::lock_guard lock(mutex_);
        CleanupExpiredLocked(Clock::now());
        const auto found = sockets_.find(request.connection_id);
        if (found == sockets_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "UDP connection does not exist");
        }
        if (found->second.info.is_listener) {
            return Status::Error(StatusCode::kUnsupported,
                                 "UDP listener has no configured send peer");
        }
        MINO_RETURN_IF_ERROR(SendMessageLocked(found->second, request.payload));
        return request.payload.size();
    }

    Result<ReceiveResult> PollMessages(const ReceiveRequest& request) {
        std::unique_lock lock(mutex_);
        const TimePoint deadline =
            Clock::now() + std::chrono::milliseconds(request.timeout_ms);
        size_t inspected_datagrams = 0;
        for (;;) {
            if (stop_requested_.load(std::memory_order_acquire)) {
                return Unavailable("UDP driver is stopping");
            }
            CleanupExpiredLocked(Clock::now());

            ReceiveResult result;
            result.messages.reserve(request.max_messages);
            size_t total_bytes = 0;
            const Status ready_status =
                DrainCompletedLocked(request, &result, &total_bytes);
            if (!ready_status.ok()) return ready_status;
            if (!result.messages.empty()) return result;

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
                    CleanupExpiredLocked(Clock::now());
                    return Timeout("UDP receive timed out");
                }
                timeout_ms = static_cast<int>(std::max<int64_t>(
                    1, std::min<int64_t>(options_.io_poll_max_ms,
                                         remaining.count())));
            }
            const int polled =
                ::poll(descriptors.data(), descriptors.size(), timeout_ms);
            if (polled < 0) {
                if (errno == EINTR) continue;
                return Unavailable("UDP receive poll failed");
            }
            if (polled == 0) {
                CleanupExpiredLocked(Clock::now());
                if (request.timeout_ms == 0) {
                    return WouldBlock("UDP receive queue is empty");
                }
                if (Clock::now() >= deadline) {
                    return Timeout("UDP receive timed out");
                }
                continue;
            }
            if ((descriptors[0].revents & POLLIN) != 0) DrainWakePipe();

            std::vector<std::byte> inspection_buffer(
                static_cast<size_t>(options_.max_datagram_bytes) + 1);
            const size_t socket_count = descriptors.size() - 1;
            if (socket_count == 0) continue;
            size_t next_offset = 0;
            if (next_poll_connection_id_ != kInvalidConnectionId) {
                for (size_t offset = 0; offset < socket_count; ++offset) {
                    if (ids[offset + 1] == next_poll_connection_id_) {
                        next_offset = offset;
                        break;
                    }
                }
            }

            bool made_progress = false;
            const size_t sweep_start = next_offset;
            for (size_t offset = 0;
                 offset < socket_count &&
                 result.messages.size() < request.max_messages &&
                 inspected_datagrams < kReceiveDatagramBudget;
                 ++offset) {
                const size_t descriptor_offset =
                    (sweep_start + offset) % socket_count;
                const size_t index = descriptor_offset + 1;
                if ((descriptors[index].revents & POLLIN) == 0) continue;

                while (result.messages.size() < request.max_messages &&
                       inspected_datagrams < kReceiveDatagramBudget) {
                    if (stop_requested_.load(std::memory_order_acquire)) {
                        if (!result.messages.empty()) return result;
                        return Unavailable("UDP driver is stopping");
                    }
                    ssize_t inspected = -1;
                    do {
                        inspected = ::recv(descriptors[index].fd,
                                           inspection_buffer.data(),
                                           inspection_buffer.size(), MSG_PEEK);
                    } while (inspected < 0 && errno == EINTR);
                    if (inspected < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        return Unavailable("failed to inspect UDP datagram size");
                    }
                    made_progress = true;
                    ++inspected_datagrams;
                    next_offset = (descriptor_offset + 1) % socket_count;
                    next_poll_connection_id_ = ids[next_offset + 1];
                    const size_t datagram_size = static_cast<size_t>(inspected);
                    if (datagram_size > options_.max_datagram_bytes) {
                        if (!result.messages.empty()) return result;
                        MINO_RETURN_IF_ERROR(DiscardDatagram(descriptors[index].fd,
                                                            inspection_buffer));
                        return Exhausted("UDP datagram exceeds configured bound");
                    }
                    if (datagram_size == 0) {
                        MINO_RETURN_IF_ERROR(DiscardDatagram(descriptors[index].fd,
                                                            inspection_buffer));
                        continue;
                    }

                    const bool fragment_candidate =
                        datagram_size >= 4 &&
                        LoadBe32(std::span<const std::byte>(inspection_buffer)
                                     .first<4>()) == kUdpFragmentMagic;
                    if (!fragment_candidate &&
                        datagram_size > request.max_bytes - total_bytes) {
                        if (result.messages.empty()) {
                            return Exhausted(
                                "next UDP datagram exceeds receive byte budget");
                        }
                        return result;
                    }

                    sockaddr_storage peer{};
                    socklen_t peer_size = sizeof(peer);
                    std::vector<std::byte> datagram(datagram_size);
                    ssize_t received = -1;
                    do {
                        received = ::recvfrom(
                            descriptors[index].fd, datagram.data(), datagram.size(),
                            0, reinterpret_cast<sockaddr*>(&peer), &peer_size);
                    } while (received < 0 && errno == EINTR);
                    if (received < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        return Unavailable("UDP datagram receive failed");
                    }
                    if (static_cast<size_t>(received) != datagram_size) {
                        return Internal("UDP datagram size changed while receiving");
                    }
                    MINO_ASSIGN_OR_RETURN(auto from,
                                          FromSocketAddress(peer, peer_size));
                    MINO_ASSIGN_OR_RETURN(
                        auto message,
                        ProcessDatagramLocked(ids[index], from, datagram));
                    if (message.has_value()) {
                        total_bytes += message->payload.size();
                        result.messages.push_back(std::move(*message));
                    }
                    const Status completed_status =
                        DrainCompletedLocked(request, &result, &total_bytes);
                    if (!completed_status.ok()) {
                        if (!result.messages.empty()) return result;
                        return completed_status;
                    }
                }
            }
            if (!result.messages.empty()) return result;
            if (inspected_datagrams >= kReceiveDatagramBudget) {
                if (request.timeout_ms == 0) {
                    return WouldBlock("UDP receive datagram work quota exhausted");
                }
                if (stop_requested_.load(std::memory_order_acquire)) {
                    return Unavailable("UDP driver is stopping");
                }
                if (Clock::now() >= deadline) {
                    CleanupExpiredLocked(Clock::now());
                    return Timeout("UDP receive timed out");
                }
                inspected_datagrams = 0;
                lock.unlock();
                std::this_thread::yield();
                lock.lock();
                continue;
            }
            if (request.timeout_ms == 0) {
                return WouldBlock("UDP receive queue is empty");
            }
            if (Clock::now() >= deadline) {
                CleanupExpiredLocked(Clock::now());
                return Timeout("UDP receive timed out");
            }
            if (made_progress) {
                lock.unlock();
                std::this_thread::yield();
                lock.lock();
            }
        }
    }

    Result<CompletionPollResult> PollCompletions(
        const CompletionPollRequest& request) {
        std::unique_lock lock(mutex_);
        CleanupExpiredLocked(Clock::now());
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
        std::lock_guard lock(mutex_);
        CleanupExpiredLocked(Clock::now());
        const auto found = sockets_.find(id);
        if (found == sockets_.end()) {
            return closed_.contains(id)
                       ? Status::Ok()
                       : Status::Error(StatusCode::kNotFound,
                                       "UDP connection does not exist");
        }
        RemoveConnectionReassembliesLocked(id);
        if (found->second.fd >= 0) (void)::close(found->second.fd);
        sockets_.erase(found);
        closed_.insert(id);
        Wake();
        return Status::Ok();
    }

    UdpDriverStats Stats() noexcept {
        std::lock_guard lock(mutex_);
        CleanupExpiredLocked(Clock::now());
        size_t listeners = 0;
        for (const auto& [id, socket] : sockets_) {
            (void)id;
            if (socket.info.is_listener) ++listeners;
        }
        return UdpDriverStats{
            .connected_sockets = sockets_.size() - listeners,
            .listeners = listeners,
            .pending_completions = completion_count_,
            .reassembly_bytes = reassembly_bytes_,
            .reassembly_messages = reassembly_messages_,
            .fragmented_messages_sent = fragmented_messages_sent_,
            .fragments_sent = fragments_sent_,
            .fragments_received = fragments_received_,
            .duplicate_fragments = duplicate_fragments_,
            .reassembled_messages = reassembled_messages_,
            .reassembly_timeouts = reassembly_timeouts_,
            .rejected_fragments = rejected_fragments_,
        };
    }

private:
    struct SocketState {
        ConnectionInfo info;
        int fd = -1;
        uint64_t next_message_id = 1;
        size_t reassembly_bytes = 0;
        size_t reassembly_messages = 0;
    };

    struct PeerIdentity {
        EndpointAddressFamily family = EndpointAddressFamily::kUnspecified;
        std::array<std::byte, EndpointDescriptor::kIpv6AddressBytes> address{};
        uint16_t port = 0;

        friend bool operator==(const PeerIdentity&, const PeerIdentity&) = default;
    };

    struct ReassemblyKey {
        ConnectionId connection_id = kInvalidConnectionId;
        PeerIdentity peer;
        uint64_t message_id = 0;

        friend bool operator==(const ReassemblyKey&, const ReassemblyKey&) =
            default;
    };

    struct ReassemblyKeyHash {
        size_t operator()(const ReassemblyKey& key) const noexcept {
            size_t hash = std::hash<uint64_t>{}(key.connection_id);
            hash ^= std::hash<uint64_t>{}(key.message_id) + 0x9e3779b9u +
                    (hash << 6) + (hash >> 2);
            hash ^= static_cast<size_t>(key.peer.family) + key.peer.port;
            for (const std::byte value : key.peer.address) {
                hash = (hash * 16777619u) ^ std::to_integer<uint8_t>(value);
            }
            return hash;
        }
    };

    struct ReassemblyState {
        EndpointDescriptor from;
        uint32_t total_length = 0;
        uint32_t fragment_count = 0;
        uint32_t expected_crc32 = 0;
        uint32_t received_count = 0;
        size_t received_bytes = 0;
        TimePoint created{};
        std::vector<std::byte> payload;
        std::vector<uint8_t> received;
        bool complete = false;
    };

    using ReassemblyMap =
        std::unordered_map<ReassemblyKey, ReassemblyState, ReassemblyKeyHash>;

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

    Status SendMessageLocked(SocketState& socket,
                             std::span<const std::byte> payload) {
        if (payload.size() <= options_.max_datagram_bytes) {
            ssize_t sent = -1;
            do {
                sent = ::send(socket.fd, payload.data(), payload.size(), 0);
            } while (sent < 0 && errno == EINTR);
            if (sent < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return WouldBlock("UDP socket send buffer is full");
                }
                return Unavailable("UDP datagram send failed");
            }
            if (static_cast<size_t>(sent) != payload.size()) {
                return Internal("UDP socket reported a partial datagram send");
            }
            return Status::Ok();
        }

        const uint32_t fragment_count =
            RequiredFragmentCount(payload.size(), options_);
        if (fragment_count > options_.max_fragments_per_message) {
            return Exhausted("UDP message exceeds fragment-count bound");
        }
        uint64_t message_id = socket.next_message_id++;
        if (message_id == 0) {
            message_id = socket.next_message_id++;
        }
        const uint32_t crc32 = Crc32(payload);
        const size_t capacity = FragmentPayloadCapacity(options_);
        for (uint32_t fragment_id = 0; fragment_id < fragment_count;
             ++fragment_id) {
            const size_t offset = static_cast<size_t>(fragment_id) * capacity;
            const size_t fragment_bytes =
                std::min(capacity, payload.size() - offset);
            auto header = BuildFragmentHeader(
                message_id, fragment_id, fragment_count,
                static_cast<uint32_t>(payload.size()),
                static_cast<uint32_t>(offset), crc32);
            std::array<iovec, 2> vectors{
                iovec{.iov_base = header.data(), .iov_len = header.size()},
                iovec{.iov_base = const_cast<std::byte*>(payload.data() + offset),
                      .iov_len = fragment_bytes},
            };
            msghdr message{};
            message.msg_iov = vectors.data();
            message.msg_iovlen = vectors.size();
            ssize_t sent = -1;
            do {
#if defined(MSG_NOSIGNAL)
                sent = ::sendmsg(socket.fd, &message, MSG_NOSIGNAL);
#else
                sent = ::sendmsg(socket.fd, &message, 0);
#endif
            } while (sent < 0 && errno == EINTR);
            if (sent < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return WouldBlock("UDP fragment send buffer is full");
                }
                return Unavailable("UDP fragment send failed");
            }
            if (static_cast<size_t>(sent) != header.size() + fragment_bytes) {
                return Internal("UDP socket reported a partial fragment send");
            }
            ++fragments_sent_;
        }
        ++fragmented_messages_sent_;
        return Status::Ok();
    }

    static PeerIdentity MakePeerIdentity(
        const EndpointDescriptor& endpoint) noexcept {
        PeerIdentity identity;
        identity.family = endpoint.address_family();
        identity.port = endpoint.port();
        const auto address = endpoint.ip_address();
        std::copy(address.begin(), address.end(), identity.address.begin());
        return identity;
    }

    Result<std::optional<ReceivedMessage>> RejectFragment(
        const Status& status) {
        ++rejected_fragments_;
        return status;
    }

    Result<std::optional<ReceivedMessage>> ProcessDatagramLocked(
        ConnectionId connection_id, const EndpointDescriptor& from,
        std::span<const std::byte> datagram) {
        if (datagram.size() < 4 || LoadBe32(datagram.first<4>()) !=
                                      kUdpFragmentMagic) {
            return std::optional<ReceivedMessage>(ReceivedMessage{
                .connection_id = connection_id,
                .from = from,
                .payload = std::vector<std::byte>(datagram.begin(),
                                                  datagram.end()),
            });
        }
        if (datagram.size() < kUdpFragmentHeaderBytes) {
            return RejectFragment(Corruption("UDP fragment header is truncated"));
        }
        if (std::to_integer<uint8_t>(datagram[4]) != kUdpFragmentVersion ||
            std::to_integer<uint8_t>(datagram[5]) != kUdpFragmentFlag ||
            LoadBe16(datagram.subspan<6, 2>()) != kUdpFragmentHeaderBytes) {
            return RejectFragment(
                Corruption("UDP fragment version, flags, or header size is invalid"));
        }

        const uint64_t message_id = LoadBe64(datagram.subspan<8, 8>());
        const uint32_t fragment_id = LoadBe32(datagram.subspan<16, 4>());
        const uint32_t fragment_count = LoadBe32(datagram.subspan<20, 4>());
        const uint32_t total_length = LoadBe32(datagram.subspan<24, 4>());
        const uint32_t fragment_offset = LoadBe32(datagram.subspan<28, 4>());
        const uint32_t expected_crc32 = LoadBe32(datagram.subspan<32, 4>());
        const size_t fragment_bytes = datagram.size() - kUdpFragmentHeaderBytes;
        const size_t capacity = FragmentPayloadCapacity(options_);

        if (message_id == 0 || fragment_count < 2 ||
            fragment_count > options_.max_fragments_per_message ||
            fragment_id >= fragment_count ||
            total_length <= options_.max_datagram_bytes ||
            total_length > options_.max_message_bytes || fragment_bytes == 0) {
            return RejectFragment(Corruption("UDP fragment metadata is out of bounds"));
        }
        const uint32_t required_count =
            RequiredFragmentCount(total_length, options_);
        const uint64_t required_offset =
            static_cast<uint64_t>(fragment_id) * capacity;
        if (fragment_count != required_count ||
            required_offset != fragment_offset ||
            required_offset >= total_length) {
            return RejectFragment(
                Corruption("UDP fragment count or offset is non-canonical"));
        }
        const size_t expected_fragment_bytes = std::min<size_t>(
            capacity, static_cast<size_t>(total_length) - fragment_offset);
        if (fragment_bytes != expected_fragment_bytes) {
            return RejectFragment(
                Corruption("UDP fragment payload length is invalid"));
        }

        const ReassemblyKey key{
            .connection_id = connection_id,
            .peer = MakePeerIdentity(from),
            .message_id = message_id,
        };
        if (recently_completed_.contains(key)) {
            ++duplicate_fragments_;
            return std::optional<ReceivedMessage>{};
        }

        auto found = reassemblies_.find(key);
        if (found == reassemblies_.end()) {
            auto socket = sockets_.find(connection_id);
            if (socket == sockets_.end()) {
                return RejectFragment(
                    Corruption("UDP fragment references a closed connection"));
            }
            if (socket->second.reassembly_messages >=
                    options_.max_reassembly_messages_per_connection ||
                reassembly_messages_ >=
                    options_.max_reassembly_messages_global ||
                total_length >
                    options_.max_reassembly_bytes_per_connection -
                        socket->second.reassembly_bytes ||
                total_length >
                    options_.max_reassembly_bytes_global - reassembly_bytes_) {
                ++rejected_fragments_;
                return Exhausted("UDP reassembly quota is full");
            }
            ReassemblyState state{
                .from = from,
                .total_length = total_length,
                .fragment_count = fragment_count,
                .expected_crc32 = expected_crc32,
                .created = Clock::now(),
                .payload = std::vector<std::byte>(total_length),
                .received = std::vector<uint8_t>(fragment_count, 0),
            };
            auto inserted = reassemblies_.emplace(key, std::move(state));
            found = inserted.first;
            socket->second.reassembly_bytes += total_length;
            ++socket->second.reassembly_messages;
            reassembly_bytes_ += total_length;
            ++reassembly_messages_;
        }

        ReassemblyState& state = found->second;
        if (state.total_length != total_length ||
            state.fragment_count != fragment_count ||
            state.expected_crc32 != expected_crc32 || state.from != from) {
            RemoveReassemblyLocked(found, false);
            return RejectFragment(
                Corruption("UDP fragments disagree on message metadata"));
        }
        if (state.complete) {
            ++duplicate_fragments_;
            return std::optional<ReceivedMessage>{};
        }
        const auto fragment_payload =
            datagram.subspan(kUdpFragmentHeaderBytes);
        if (state.received[fragment_id] != 0) {
            const auto existing = std::span<const std::byte>(state.payload)
                                      .subspan(fragment_offset, fragment_bytes);
            if (!std::equal(existing.begin(), existing.end(),
                            fragment_payload.begin())) {
                RemoveReassemblyLocked(found, false);
                return RejectFragment(
                    Corruption("duplicate UDP fragment payload conflicts"));
            }
            ++duplicate_fragments_;
            return std::optional<ReceivedMessage>{};
        }

        std::copy(fragment_payload.begin(), fragment_payload.end(),
                  state.payload.begin() + fragment_offset);
        state.received[fragment_id] = 1;
        ++state.received_count;
        state.received_bytes += fragment_bytes;
        ++fragments_received_;
        if (state.received_count != state.fragment_count) {
            return std::optional<ReceivedMessage>{};
        }
        if (state.received_bytes != state.total_length ||
            Crc32(state.payload) != state.expected_crc32) {
            RemoveReassemblyLocked(found, false);
            return RejectFragment(
                Corruption("reassembled UDP message failed integrity check"));
        }
        state.complete = true;
        ++reassembled_messages_;
        return std::optional<ReceivedMessage>{};
    }

    Status DrainCompletedLocked(const ReceiveRequest& request,
                                ReceiveResult* result, size_t* total_bytes) {
        for (auto found = reassemblies_.begin();
             found != reassemblies_.end() &&
             result->messages.size() < request.max_messages;) {
            ReassemblyState& state = found->second;
            if (!state.complete ||
                (request.connection_id != kInvalidConnectionId &&
                 found->first.connection_id != request.connection_id)) {
                ++found;
                continue;
            }
            if (state.payload.size() > request.max_bytes - *total_bytes) {
                if (result->messages.empty()) {
                    return Exhausted(
                        "next reassembled UDP message exceeds receive byte budget");
                }
                break;
            }
            *total_bytes += state.payload.size();
            result->messages.push_back(ReceivedMessage{
                .connection_id = found->first.connection_id,
                .from = state.from,
                .payload = std::move(state.payload),
            });
            found = RemoveReassemblyLocked(found, true);
        }
        return Status::Ok();
    }

    ReassemblyMap::iterator RemoveReassemblyLocked(
        ReassemblyMap::iterator found, bool remember_completed) {
        const ReassemblyKey key = found->first;
        const size_t bytes = found->second.total_length;
        auto socket = sockets_.find(key.connection_id);
        if (socket != sockets_.end()) {
            socket->second.reassembly_bytes -= bytes;
            --socket->second.reassembly_messages;
        }
        reassembly_bytes_ -= bytes;
        --reassembly_messages_;
        auto next = reassemblies_.erase(found);
        if (remember_completed) RememberCompletedLocked(key, Clock::now());
        return next;
    }

    void RememberCompletedLocked(const ReassemblyKey& key, TimePoint now) {
        while (recently_completed_.size() >=
               options_.max_reassembly_messages_global) {
            auto oldest = std::min_element(
                recently_completed_.begin(), recently_completed_.end(),
                [](const auto& lhs, const auto& rhs) {
                    return lhs.second < rhs.second;
                });
            if (oldest == recently_completed_.end()) break;
            recently_completed_.erase(oldest);
        }
        recently_completed_.insert_or_assign(key, now);
    }

    void CleanupExpiredLocked(TimePoint now) {
        const auto timeout =
            std::chrono::milliseconds(options_.reassembly_timeout_ms);
        for (auto found = reassemblies_.begin(); found != reassemblies_.end();) {
            if (now - found->second.created >= timeout) {
                found = RemoveReassemblyLocked(found, false);
                ++reassembly_timeouts_;
            } else {
                ++found;
            }
        }
        for (auto found = recently_completed_.begin();
             found != recently_completed_.end();) {
            if (now - found->second >= timeout) {
                found = recently_completed_.erase(found);
            } else {
                ++found;
            }
        }
    }

    void RemoveConnectionReassembliesLocked(ConnectionId id) {
        for (auto found = reassemblies_.begin(); found != reassemblies_.end();) {
            if (found->first.connection_id == id) {
                found = RemoveReassemblyLocked(found, false);
            } else {
                ++found;
            }
        }
        for (auto found = recently_completed_.begin();
             found != recently_completed_.end();) {
            if (found->first.connection_id == id) {
                found = recently_completed_.erase(found);
            } else {
                ++found;
            }
        }
    }

    Status DiscardDatagram(int fd,
                           std::span<std::byte> inspection_buffer) const {
        ssize_t discarded = -1;
        do {
            discarded = ::recv(fd, inspection_buffer.data(),
                               inspection_buffer.size(), 0);
        } while (discarded < 0 && errno == EINTR);
        if (discarded < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            return Unavailable("failed to discard UDP datagram");
        }
        return Status::Ok();
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
    ReassemblyMap reassemblies_;
    std::unordered_map<ReassemblyKey, TimePoint, ReassemblyKeyHash>
        recently_completed_;
    size_t reassembly_bytes_ = 0;
    size_t reassembly_messages_ = 0;
    uint64_t fragmented_messages_sent_ = 0;
    uint64_t fragments_sent_ = 0;
    uint64_t fragments_received_ = 0;
    uint64_t duplicate_fragments_ = 0;
    uint64_t reassembled_messages_ = 0;
    uint64_t reassembly_timeouts_ = 0;
    uint64_t rejected_fragments_ = 0;
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
        .max_frame_size = options_.max_message_bytes,
        .max_reassembly_bytes = options_.max_message_bytes,
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
