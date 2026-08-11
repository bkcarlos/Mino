// Copyright 2026 The Mino Authors

#include "mino/transport/fabric_driver.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mino/bridge/wire_frame.h"

namespace mino::transport {
namespace {

constexpr uint32_t kCanonicalPayloadFlag = 1u;
constexpr uint64_t kCommitMarkerBase = 0x434f4d4d49543100ull;  // "COMMIT1\0"

Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status Exhausted(const char* message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}
Status WouldBlock(const char* message) {
    return Status::Error(StatusCode::kWouldBlock, message);
}
Status Unavailable(const char* message) {
    return Status::Error(StatusCode::kUnavailable, message);
}
Status Corruption(const char* message) {
    return Status::Error(StatusCode::kCorruption, message);
}

bool IsWaitStatus(const Status& status) noexcept {
    return status.code() == StatusCode::kWouldBlock ||
           status.code() == StatusCode::kTimeout;
}

uint32_t RemainingMilliseconds(
    std::chrono::steady_clock::time_point deadline) noexcept {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) return 0;
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    return static_cast<uint32_t>(std::max<int64_t>(1, remaining.count()));
}

void Put16(std::span<std::byte> output, size_t offset, uint16_t value) noexcept {
    output[offset] = static_cast<std::byte>((value >> 8) & 0xff);
    output[offset + 1] = static_cast<std::byte>(value & 0xff);
}
void Put32(std::span<std::byte> output, size_t offset, uint32_t value) noexcept {
    for (size_t i = 0; i < 4; ++i) {
        output[offset + i] =
            static_cast<std::byte>((value >> ((3 - i) * 8)) & 0xff);
    }
}
void Put64(std::span<std::byte> output, size_t offset, uint64_t value) noexcept {
    for (size_t i = 0; i < 8; ++i) {
        output[offset + i] =
            static_cast<std::byte>((value >> ((7 - i) * 8)) & 0xff);
    }
}
uint16_t Get16(std::span<const std::byte> input, size_t offset) noexcept {
    return (static_cast<uint16_t>(std::to_integer<uint8_t>(input[offset])) << 8) |
           static_cast<uint16_t>(std::to_integer<uint8_t>(input[offset + 1]));
}
uint32_t Get32(std::span<const std::byte> input, size_t offset) noexcept {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value = (value << 8) | std::to_integer<uint8_t>(input[offset + i]);
    }
    return value;
}
uint64_t Get64(std::span<const std::byte> input, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value = (value << 8) | std::to_integer<uint8_t>(input[offset + i]);
    }
    return value;
}

uint32_t Crc32c(std::span<const std::byte> bytes) noexcept {
    uint32_t crc = 0xffffffffu;
    for (std::byte byte : bytes) {
        crc ^= std::to_integer<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask = 0u - (crc & 1u);
            crc = (crc >> 1) ^ (0x82f63b78u & mask);
        }
    }
    return ~crc;
}

uint64_t CommitMarker(uint64_t window_id, uint64_t generation,
                      uint64_t session_epoch,
                      uint64_t producer_sequence) noexcept {
    const uint64_t marker = kCommitMarkerBase ^ window_id ^ generation ^
                            session_epoch ^ producer_sequence;
    return marker == 0 ? kCommitMarkerBase : marker;
}

Result<bridge::WireFrame> ValidateCanonical(std::span<const std::byte> payload,
                                            size_t max_message_bytes) noexcept {
    if (payload.size() > max_message_bytes ||
        max_message_bytes > std::numeric_limits<uint32_t>::max()) {
        return Exhausted("fabric Canonical Wire payload exceeds configured limit");
    }
    bridge::WireFrameLimits limits;
    limits.max_payload_length = static_cast<uint32_t>(max_message_bytes);
    limits.max_buffered_bytes = max_message_bytes + bridge::kLengthPrefixSize;
    return bridge::WireFrameCodec::Decode(payload, limits);
}

bool Aligned(const void* pointer, uint32_t alignment) noexcept {
    return reinterpret_cast<uintptr_t>(pointer) % alignment == 0;
}

}  // namespace

Status ValidateFabricDriverOptions(const FabricDriverOptions& options,
                                   bool production) noexcept {
    if (options.provider == nullptr || options.attestation_verifier == nullptr) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "fabric provider and attestation verifier are required");
    }
    const auto provider_capabilities = options.provider->capabilities();
    MINO_RETURN_IF_ERROR(ValidateFabricProviderCapabilities(
        provider_capabilities, production));
    if (provider_capabilities.provider_class ==
            platform::FabricProviderClass::kMock &&
        (!options.allow_mock_provider_for_testing || production)) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "mock fabric provider is test-only");
    }
    if (options.local_node_id.value == 0 ||
        options.local_security_domain.value == 0 ||
        options.max_windows_per_connection == 0 ||
        options.event_queue_depth == 0 || options.receive_queue_depth == 0 ||
        options.completion_queue_depth == 0 || options.max_message_bytes == 0 ||
        options.max_message_bytes > kMaxPayloadBytes ||
        options.max_queued_receive_bytes < options.max_message_bytes ||
        options.max_windows_per_connection >
            provider_capabilities.max_windows_per_connection ||
        options.max_message_bytes >
            provider_capabilities.max_window_bytes -
                std::min(provider_capabilities.max_window_bytes,
                         kFabricWindowHeaderBytes)) {
        return Invalid("fabric queue, window, identity, or payload limit is invalid");
    }
    if (options.provider->provenance().empty() ||
        options.provider->device_id().empty()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "fabric provider provenance or device identity is empty");
    }
    return Status::Ok();
}

class FabricWindowDriver::Impl {
public:
    Impl(FabricDriverOptions& options, std::atomic<HealthState>& health) noexcept
        : options_(options), health_(health) {}

    Status Start(const DriverConfig& config) {
        std::lock_guard lock(mutex_);
        const auto capabilities = options_.provider->capabilities();
        MINO_RETURN_IF_ERROR(
            ValidateFabricProviderCapabilities(capabilities));
        if (config.max_connections > capabilities.max_connections ||
            config.max_listeners > capabilities.max_listeners) {
            return Invalid("fabric DriverConfig exceeds provider capabilities");
        }
        config_ = config;
        stopping_.store(false, std::memory_order_release);
        next_connection_id_ = 1;
        next_pending_id_ = 1;
        const Status status = options_.provider->Start({
            .max_connections = config.max_connections,
            .max_listeners = config.max_listeners,
            .max_windows_per_connection = options_.max_windows_per_connection,
            .max_events_per_poll = options_.event_queue_depth,
            .max_window_bytes = kFabricWindowHeaderBytes +
                                options_.max_message_bytes,
        });
        if (!status.ok()) return status;
        const auto active_capabilities = options_.provider->capabilities();
        const Status active =
            ValidateFabricProviderCapabilities(active_capabilities);
        if (!active.ok() || active_capabilities.kind != capabilities.kind ||
            active_capabilities.max_window_bytes <
                kFabricWindowHeaderBytes + options_.max_message_bytes) {
            static_cast<void>(options_.provider->Shutdown());
            return !active.ok()
                       ? active
                       : Unavailable("fabric capabilities changed during Start");
        }
        health_.store(HealthState::kHealthy, std::memory_order_release);
        return Status::Ok();
    }

    void RequestStop() noexcept {
        stopping_.store(true, std::memory_order_release);
        options_.provider->RequestStop();
    }

    Status Shutdown() {
        std::lock_guard lock(mutex_);
        stopping_.store(true, std::memory_order_release);
        Status first = Status::Ok();
        for (const auto& [id, connection] : connections_) {
            (void)id;
            const Status closed = options_.provider->Close(connection.provider_id);
            if (first.ok() && !closed.ok()) first = closed;
        }
        for (const auto& [id, listener] : listeners_) {
            (void)id;
            const Status closed = options_.provider->Close(listener.provider_id);
            if (first.ok() && !closed.ok()) first = closed;
        }
        const Status stopped = options_.provider->Shutdown();
        if (first.ok() && !stopped.ok()) first = stopped;
        connections_.clear();
        provider_to_connection_.clear();
        listeners_.clear();
        pending_.clear();
        operation_to_pending_.clear();
        order_.clear();
        receives_.clear();
        completions_.clear();
        queued_receive_bytes_ = 0;
        health_.store(HealthState::kUnavailable, std::memory_order_release);
        return first;
    }

    Result<ConnectionInfo> Connect(const ConnectRequest& request) {
        std::lock_guard lock(mutex_);
        if (connections_.size() >= config_.max_connections) {
            return Exhausted("fabric connection table is full");
        }
        MINO_ASSIGN_OR_RETURN(auto provider_connection,
                              options_.provider->Connect(request));
        return VerifyAndInsert(std::move(provider_connection));
    }

    Result<ConnectionInfo> Listen(const ListenRequest& request) {
        std::lock_guard lock(mutex_);
        if (listeners_.size() >= config_.max_listeners) {
            return Exhausted("fabric listener table is full");
        }
        for (const auto& [id, listener] : listeners_) {
            (void)id;
            if (listener.info.local_endpoint == request.local_endpoint) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "fabric listener already exists");
            }
        }
        MINO_ASSIGN_OR_RETURN(auto provider_listener,
                              options_.provider->Listen(request));
        const Status endpoint_valid =
            ValidateEndpointDescriptor(provider_listener.local_endpoint);
        if (provider_listener.id == 0 || !endpoint_valid.ok() ||
            provider_listener.local_endpoint.kind() !=
                TransportKind::kSharedFabric) {
            static_cast<void>(options_.provider->Close(provider_listener.id));
            return Corruption("fabric provider returned an invalid listener");
        }
        const ConnectionId id = AllocateConnectionId();
        ConnectionInfo info{.id = id,
                            .kind = TransportKind::kSharedFabric,
                            .is_listener = true,
                            .local_endpoint = provider_listener.local_endpoint,
                            .peer_endpoint = std::nullopt};
        listeners_.emplace(id, Listener{.provider_id = provider_listener.id,
                                        .info = info});
        return info;
    }

    Result<ConnectionInfo> Accept(const AcceptRequest& request) {
        std::lock_guard lock(mutex_);
        const auto listener = listeners_.find(request.listener_id);
        if (listener == listeners_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "fabric listener is not open");
        }
        if (connections_.size() >= config_.max_connections) {
            return Exhausted("fabric connection table is full");
        }
        MINO_ASSIGN_OR_RETURN(
            auto provider_connection,
            options_.provider->Accept(listener->second.provider_id,
                                      request.timeout_ms));
        return VerifyAndInsert(std::move(provider_connection));
    }

    Result<SendResult> Send(const SendRequest& request,
                            SendOperation operation) {
        MINO_ASSIGN_OR_RETURN(auto canonical,
                              ValidateCanonical(request.payload,
                                                options_.max_message_bytes));
        (void)canonical;
        std::lock_guard lock(mutex_);
        MINO_RETURN_IF_ERROR(Progress(0));
        MINO_RETURN_IF_ERROR(Post(request.connection_id, request.payload,
                                  operation, request.target_stage));
        return SendResult{.operation = operation,
                          .admitted_bytes = request.payload.size()};
    }

    Result<size_t> SendUntracked(const UntrackedSendRequest& request) {
        MINO_ASSIGN_OR_RETURN(auto canonical,
                              ValidateCanonical(request.payload,
                                                options_.max_message_bytes));
        (void)canonical;
        std::lock_guard lock(mutex_);
        MINO_RETURN_IF_ERROR(Progress(0));
        MINO_RETURN_IF_ERROR(Post(request.connection_id, request.payload,
                                  std::nullopt,
                                  DeliveryStage::kLocalPublished));
        return request.payload.size();
    }

    Status ConfirmRemoteAccepted(SendOperation operation) {
        std::lock_guard lock(mutex_);
        MINO_RETURN_IF_ERROR(Progress(0));
        const auto indexed = operation_to_pending_.find(operation.id);
        if (indexed == operation_to_pending_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "fabric send operation is not outstanding");
        }
        auto pending = pending_.find(indexed->second);
        if (pending == pending_.end() || !pending->second.operation.has_value() ||
            *pending->second.operation != operation) {
            return Corruption("fabric operation index is inconsistent");
        }
        pending->second.remote_accepted = true;
        FlushOrdered(operation.connection_id);
        return Status::Ok();
    }

    Result<ReceiveResult> Poll(const ReceiveRequest& request) {
        std::unique_lock lock(mutex_);
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(request.timeout_ms);
        for (;;) {
            MINO_RETURN_IF_ERROR(Progress(0));
            ReceiveResult result;
            size_t bytes = 0;
            for (auto iterator = receives_.begin();
                 iterator != receives_.end() &&
                 result.messages.size() < request.max_messages;) {
                if (request.connection_id != kInvalidConnectionId &&
                    iterator->connection_id != request.connection_id) {
                    ++iterator;
                    continue;
                }
                if (iterator->payload.size() > request.max_bytes - bytes) {
                    if (result.messages.empty()) {
                        return Exhausted(
                            "next fabric message exceeds receive byte budget");
                    }
                    break;
                }
                bytes += iterator->payload.size();
                queued_receive_bytes_ -= iterator->payload.size();
                result.messages.push_back(std::move(*iterator));
                iterator = receives_.erase(iterator);
            }
            if (!result.messages.empty()) return result;
            if (request.timeout_ms == 0) {
                return WouldBlock("no fabric message is ready");
            }
            const uint32_t remaining = RemainingMilliseconds(deadline);
            if (remaining == 0) {
                return Status::Error(StatusCode::kTimeout,
                                     "fabric receive timed out");
            }
            MINO_RETURN_IF_ERROR(Progress(remaining));
        }
    }

    Result<CompletionPollResult> PollCompletions(
        const CompletionPollRequest& request) {
        std::unique_lock lock(mutex_);
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(request.timeout_ms);
        for (;;) {
            MINO_RETURN_IF_ERROR(Progress(0));
            FlushAllOrdered();
            CompletionPollResult result;
            for (auto iterator = completions_.begin();
                 iterator != completions_.end() &&
                 result.completions.size() < request.max_completions;) {
                if (request.connection_id != kInvalidConnectionId &&
                    iterator->operation.connection_id != request.connection_id) {
                    ++iterator;
                    continue;
                }
                result.completions.push_back(std::move(*iterator));
                iterator = completions_.erase(iterator);
            }
            if (!result.completions.empty()) return result;
            if (request.timeout_ms == 0) {
                return WouldBlock("no fabric completion is ready");
            }
            const uint32_t remaining = RemainingMilliseconds(deadline);
            if (remaining == 0) {
                return Status::Error(StatusCode::kTimeout,
                                     "fabric completion poll timed out");
            }
            MINO_RETURN_IF_ERROR(Progress(remaining));
        }
    }

    Result<security::AuthenticatedPeer> AuthenticatedPeer(
        ConnectionId connection_id) {
        std::lock_guard lock(mutex_);
        const auto connection = connections_.find(connection_id);
        if (connection == connections_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "fabric connection is not open");
        }
        return connection->second.peer;
    }

    Status Close(ConnectionId connection_id) {
        std::lock_guard lock(mutex_);
        const auto listener = listeners_.find(connection_id);
        if (listener != listeners_.end()) {
            const Status status = options_.provider->Close(listener->second.provider_id);
            listeners_.erase(listener);
            return status;
        }
        const auto connection = connections_.find(connection_id);
        if (connection == connections_.end()) return Status::Ok();
        const Status closed = options_.provider->Close(connection->second.provider_id);
        InvalidateConnection(connection_id,
                             Unavailable("fabric connection closed"),
                             /*provider_already_closed=*/true);
        return closed;
    }

    FabricDriverStats stats() const noexcept {
        std::lock_guard lock(mutex_);
        FabricDriverStats result = stats_;
        result.active_connections = connections_.size();
        result.listeners = listeners_.size();
        result.outstanding_windows = pending_.size();
        result.queued_receive_messages = receives_.size();
        result.queued_receive_bytes = queued_receive_bytes_;
        result.queued_completions = completions_.size();
        return result;
    }

private:
    struct Connection {
        platform::FabricProviderConnectionId provider_id = 0;
        ConnectionInfo info;
        security::AuthenticatedPeer peer;
        uint64_t window_set_id = 0;
        uint64_t generation = 0;
        uint64_t session_epoch = 0;
        uint64_t next_producer_sequence = 1;
        uint64_t next_receive_sequence = 1;
        uint64_t last_peer_consumer_sequence = 0;
    };
    struct Listener {
        platform::FabricProviderListenerId provider_id = 0;
        ConnectionInfo info;
    };
    struct Pending {
        uint64_t id = 0;
        ConnectionId connection_id = kInvalidConnectionId;
        platform::FabricProviderConnectionId provider_connection_id = 0;
        uint64_t window_id = 0;
        uint64_t generation = 0;
        uint64_t session_epoch = 0;
        uint64_t producer_sequence = 0;
        std::optional<SendOperation> operation;
        DeliveryStage target_stage = DeliveryStage::kLocalPublished;
        bool committed = false;
        bool transport_consumed = false;
        bool remote_accepted = false;
        bool terminal = false;
        bool completion_emitted = false;
        Status status = Status::Ok();
    };

    Result<ConnectionInfo> VerifyAndInsert(
        platform::FabricProviderConnection provider_connection) {
        const Status local_valid =
            ValidateEndpointDescriptor(provider_connection.local_endpoint);
        const Status peer_valid =
            ValidateEndpointDescriptor(provider_connection.peer_endpoint);
        if (!local_valid.ok() || !peer_valid.ok() ||
            provider_connection.local_endpoint.kind() !=
                TransportKind::kSharedFabric ||
            provider_connection.peer_endpoint.kind() !=
                TransportKind::kSharedFabric || provider_connection.id == 0 ||
            provider_to_connection_.contains(provider_connection.id) ||
            provider_connection.window_set_id == 0 ||
            provider_connection.window_generation == 0 ||
            provider_connection.session_epoch == 0 ||
            provider_connection.peer_node_id.value == 0 ||
            provider_connection.peer_security_domain.value == 0 ||
            provider_connection.peer_security_domain ==
                options_.local_security_domain ||
            provider_connection.peer_device_id.empty() ||
            provider_connection.attestation_evidence.empty()) {
            static_cast<void>(options_.provider->Close(provider_connection.id));
            return Status::Error(
                StatusCode::kPermissionDenied,
                "fabric handshake, cross-domain identity, or attestation is incomplete");
        }
        const auto provider_capabilities = options_.provider->capabilities();
        FabricAttestation attestation{
            .local_node_id = options_.local_node_id,
            .local_security_domain = options_.local_security_domain,
            .peer_node_id = provider_connection.peer_node_id,
            .peer_security_domain = provider_connection.peer_security_domain,
            .local_endpoint = provider_connection.local_endpoint,
            .peer_endpoint = provider_connection.peer_endpoint,
            .kind = provider_capabilities.kind,
            .provider_provenance = options_.provider->provenance(),
            .local_device_id = options_.provider->device_id(),
            .peer_device_id = provider_connection.peer_device_id,
            .window_set_id = provider_connection.window_set_id,
            .window_generation = provider_connection.window_generation,
            .session_epoch = provider_connection.session_epoch,
            .evidence = provider_connection.attestation_evidence,
        };
        auto verified = options_.attestation_verifier->Verify(attestation);
        if (!verified.ok() || !verified->complete() ||
            verified->node_id != provider_connection.peer_node_id ||
            verified->security_domain !=
                provider_connection.peer_security_domain) {
            static_cast<void>(options_.provider->Close(provider_connection.id));
            return Status::Error(StatusCode::kPermissionDenied,
                                 "fabric peer attestation binding failed");
        }
        const ConnectionId id = AllocateConnectionId();
        ConnectionInfo info{.id = id,
                            .kind = TransportKind::kSharedFabric,
                            .is_listener = false,
                            .local_endpoint = provider_connection.local_endpoint,
                            .peer_endpoint = provider_connection.peer_endpoint};
        provider_to_connection_.emplace(provider_connection.id, id);
        connections_.emplace(
            id, Connection{.provider_id = provider_connection.id,
                           .info = info,
                           .peer = *verified,
                           .window_set_id = provider_connection.window_set_id,
                           .generation = provider_connection.window_generation,
                           .session_epoch = provider_connection.session_epoch});
        return info;
    }

    ConnectionId AllocateConnectionId() noexcept {
        while (next_connection_id_ == kInvalidConnectionId ||
               connections_.contains(next_connection_id_) ||
               listeners_.contains(next_connection_id_)) {
            ++next_connection_id_;
        }
        return next_connection_id_++;
    }

    uint64_t AllocatePendingId() noexcept {
        while (next_pending_id_ == 0 || pending_.contains(next_pending_id_)) {
            ++next_pending_id_;
        }
        return next_pending_id_++;
    }

    Status Post(ConnectionId connection_id, std::span<const std::byte> payload,
                std::optional<SendOperation> operation,
                DeliveryStage target_stage) {
        const auto connection = connections_.find(connection_id);
        if (connection == connections_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "fabric connection is not open");
        }
        if (pending_.size() >= config_.max_queued_sends ||
            order_[connection_id].size() >= options_.max_windows_per_connection) {
            return WouldBlock("fabric window ring is full");
        }
        const size_t record_bytes = kFabricWindowHeaderBytes + payload.size();
        auto acquired = options_.provider->AcquireTransmitWindow(
            connection->second.provider_id, record_bytes);
        if (!acquired.ok()) return acquired.status();
        platform::FabricTransmitWindow window = *acquired;
        const auto provider_capabilities = options_.provider->capabilities();
        if (window.connection_id != connection->second.provider_id ||
            window.window_id == 0 ||
            window.window_generation != connection->second.generation ||
            window.session_epoch != connection->second.session_epoch ||
            window.bytes.size() < record_bytes ||
            window.bytes.size() > provider_capabilities.max_window_bytes ||
            !Aligned(window.bytes.data(),
                     provider_capabilities.required_alignment)) {
            static_cast<void>(options_.provider->AbortTransmitWindow(window));
            return Corruption("fabric provider returned an invalid window lease");
        }
        const uint64_t sequence = connection->second.next_producer_sequence++;
        if (sequence == 0 || window.consumer_sequence > sequence) {
            static_cast<void>(options_.provider->AbortTransmitWindow(window));
            return Corruption("fabric producer or consumer sequence is invalid");
        }
        std::span<std::byte> record = window.bytes.first(record_bytes);
        std::fill(record.begin(), record.end(), std::byte{0});
        std::copy(payload.begin(), payload.end(),
                  record.begin() + kFabricWindowHeaderBytes);
        Put32(record, 0, kFabricWindowMagic);
        Put16(record, 4, kFabricWindowProtocolVersion);
        Put16(record, 6, static_cast<uint16_t>(kFabricWindowHeaderBytes));
        Put32(record, 8, kFabricWindowEndianMarker);
        Put32(record, 12, kCanonicalPayloadFlag);
        Put64(record, 16, window.window_id);
        Put64(record, 24, window.window_generation);
        Put64(record, 32, window.session_epoch);
        Put64(record, 40, sequence);
        Put64(record, 48, window.consumer_sequence);
        Put32(record, 56, static_cast<uint32_t>(payload.size()));
        Put16(record, 60,
              static_cast<uint16_t>(provider_capabilities.kind));
        Put32(record, 64, Crc32c(payload));
        Put32(record, 68,
              Crc32c(record.first(kFabricWindowHeaderBytes)));
        // Commit marker remains zero through the first cache clean. The release
        // fence and second marker-only clean make partial publication detectable.
        Status cache = options_.provider->MaintainCache({
            .connection_id = window.connection_id,
            .window_id = window.window_id,
            .window_generation = window.window_generation,
            .session_epoch = window.session_epoch,
            .direction = platform::FabricCacheDirection::kForPeerRead,
            .offset = 0,
            .bytes = record_bytes,
        });
        if (!cache.ok()) {
            ++stats_.cache_maintenance_failures;
            static_cast<void>(options_.provider->AbortTransmitWindow(window));
            return cache;
        }
        std::atomic_thread_fence(std::memory_order_release);
        Put64(record, kFabricWindowCommitMarkerOffset,
              CommitMarker(window.window_id, window.window_generation,
                           window.session_epoch, sequence));
        cache = options_.provider->MaintainCache({
            .connection_id = window.connection_id,
            .window_id = window.window_id,
            .window_generation = window.window_generation,
            .session_epoch = window.session_epoch,
            .direction = platform::FabricCacheDirection::kForPeerRead,
            .offset =
                (kFabricWindowCommitMarkerOffset /
                 provider_capabilities.cache_line_bytes) *
                provider_capabilities.cache_line_bytes,
            .bytes = std::min<size_t>(
                provider_capabilities.cache_line_bytes,
                record_bytes -
                    (kFabricWindowCommitMarkerOffset /
                     provider_capabilities.cache_line_bytes) *
                        provider_capabilities.cache_line_bytes),
        });
        if (!cache.ok()) {
            ++stats_.cache_maintenance_failures;
            static_cast<void>(options_.provider->AbortTransmitWindow(window));
            return cache;
        }
        std::atomic_thread_fence(std::memory_order_release);

        const uint64_t pending_id = AllocatePendingId();
        Pending pending{.id = pending_id,
                        .connection_id = connection_id,
                        .provider_connection_id = connection->second.provider_id,
                        .window_id = window.window_id,
                        .generation = window.window_generation,
                        .session_epoch = window.session_epoch,
                        .producer_sequence = sequence,
                        .operation = operation,
                        .target_stage = target_stage,
                        .committed = true};
        pending_.emplace(pending_id, pending);
        order_[connection_id].push_back(pending_id);
        if (operation.has_value()) {
            operation_to_pending_.emplace(operation->id, pending_id);
        }
        const Status rang = options_.provider->RingDoorbell({
            .protocol_version = platform::kMinoFabricMailboxProtocolVersion,
            .endian_marker = platform::kMinoFabricMailboxEndianMarker,
            .connection_id = window.connection_id,
            .kind = platform::FabricDoorbellKind::kProducerCommit,
            .window_id = window.window_id,
            .window_generation = window.window_generation,
            .session_epoch = window.session_epoch,
            .producer_sequence = sequence,
        });
        if (!rang.ok()) {
            ++stats_.doorbell_failures;
            if (operation.has_value()) operation_to_pending_.erase(operation->id);
            order_[connection_id].pop_back();
            if (order_[connection_id].empty()) order_.erase(connection_id);
            pending_.erase(pending_id);
            static_cast<void>(options_.provider->AbortTransmitWindow(window));
            return rang;
        }
        ++stats_.committed_windows;
        FlushOrdered(connection_id);
        return Status::Ok();
    }

    Status Progress(uint32_t timeout_ms) {
        const uint32_t receive_capacity =
            receives_.size() >= options_.receive_queue_depth
                ? 0
                : options_.receive_queue_depth - receives_.size();
        auto polled = options_.provider->Poll({
            .max_receive_events = receive_capacity,
            .max_control_events = options_.event_queue_depth,
            .timeout_ms = timeout_ms,
        });
        if (!polled.ok()) {
            if (IsWaitStatus(polled.status())) return Status::Ok();
            if (polled.status().code() == StatusCode::kUnavailable) {
                health_.store(HealthState::kUnavailable,
                              std::memory_order_release);
            }
            return polled.status();
        }
        uint32_t receive_events = 0;
        uint32_t control_events = 0;
        for (const auto& event : polled->events) {
            if (event.kind == platform::FabricProviderEventKind::kReceiveReady) {
                ++receive_events;
            } else {
                ++control_events;
            }
        }
        if (receive_events > receive_capacity ||
            control_events > options_.event_queue_depth) {
            return Corruption("fabric provider exceeded poll event bounds");
        }
        for (const auto& event : polled->events) {
            MINO_RETURN_IF_ERROR(HandleEvent(event));
        }
        return Status::Ok();
    }

    Status HandleEvent(const platform::FabricProviderEvent& event) {
        const auto mapped = provider_to_connection_.find(event.connection_id);
        if (mapped == provider_to_connection_.end()) {
            ++stats_.stale_window_events;
            return Status::Ok();
        }
        const ConnectionId connection_id = mapped->second;
        const auto connection = connections_.find(connection_id);
        if (connection == connections_.end()) {
            ++stats_.stale_window_events;
            return Status::Ok();
        }
        if (event.kind == platform::FabricProviderEventKind::kPeerReset ||
            event.kind == platform::FabricProviderEventKind::kLinkError) {
            ++stats_.peer_resets;
            health_.store(HealthState::kDegraded, std::memory_order_release);
            const Status reason = event.status.ok()
                                      ? Unavailable("fabric peer reset or link loss")
                                      : event.status;
            InvalidateConnection(connection_id, reason);
            return Status::Ok();
        }
        if (event.window_generation != connection->second.generation ||
            event.session_epoch != connection->second.session_epoch) {
            ++stats_.stale_window_events;
            ++stats_.peer_resets;
            health_.store(HealthState::kDegraded, std::memory_order_release);
            InvalidateConnection(
                connection_id,
                Unavailable("fabric window generation or session changed"));
            return Status::Ok();
        }
        if (event.mailbox_protocol_version !=
                platform::kMinoFabricMailboxProtocolVersion ||
            event.mailbox_endian_marker !=
                platform::kMinoFabricMailboxEndianMarker) {
            InvalidateConnection(connection_id,
                                 Corruption("fabric mailbox version is invalid"));
            return Status::Ok();
        }
        if (!event.status.ok()) {
            InvalidateConnection(connection_id, event.status);
            return Status::Ok();
        }
        if (event.kind ==
            platform::FabricProviderEventKind::kTransmitConsumed) {
            return HandleConsumed(connection_id, event);
        }
        if (event.kind == platform::FabricProviderEventKind::kReceiveReady) {
            return HandleReceive(connection_id, event);
        }
        return Corruption("fabric provider returned an unknown event");
    }

    Status HandleConsumed(ConnectionId connection_id,
                          const platform::FabricProviderEvent& event) {
        auto found = std::find_if(
            pending_.begin(), pending_.end(), [&](const auto& item) {
                const Pending& pending = item.second;
                return pending.connection_id == connection_id &&
                       pending.window_id == event.window_id &&
                       pending.generation == event.window_generation &&
                       pending.session_epoch == event.session_epoch &&
                       pending.producer_sequence == event.producer_sequence;
            });
        if (found == pending_.end()) {
            ++stats_.stale_window_events;
            return Status::Ok();
        }
        found->second.transport_consumed = true;
        ++stats_.consumed_windows;
        FlushOrdered(connection_id);
        return Status::Ok();
    }

    Status HandleReceive(ConnectionId connection_id,
                         const platform::FabricProviderEvent& event) {
        auto connection = connections_.find(connection_id);
        const auto provider_capabilities = options_.provider->capabilities();
        if (event.window_id == 0 ||
            event.window.size() < kFabricWindowHeaderBytes ||
            event.window.size() > provider_capabilities.max_window_bytes ||
            !Aligned(event.window.data(),
                     provider_capabilities.required_alignment)) {
            return RejectReceive(connection_id, event,
                                 Corruption("fabric receive window bounds invalid"));
        }
        Status cache = options_.provider->MaintainCache({
            .connection_id = event.connection_id,
            .window_id = event.window_id,
            .window_generation = event.window_generation,
            .session_epoch = event.session_epoch,
            .direction = platform::FabricCacheDirection::kForCpuRead,
            .offset = 0,
            .bytes = event.window.size(),
        });
        if (!cache.ok()) {
            ++stats_.cache_maintenance_failures;
            return RejectReceive(connection_id, event, cache);
        }
        std::atomic_thread_fence(std::memory_order_acquire);
        const auto header = event.window.first(kFabricWindowHeaderBytes);
        if (Get64(header, kFabricWindowCommitMarkerOffset) !=
            CommitMarker(event.window_id, event.window_generation,
                         event.session_epoch, event.producer_sequence)) {
            ++stats_.partial_commits;
            return RejectReceive(connection_id, event,
                                 Corruption("fabric window commit marker is absent"));
        }
        if (Get32(header, 0) != kFabricWindowMagic ||
            Get16(header, 4) != kFabricWindowProtocolVersion ||
            Get16(header, 6) != kFabricWindowHeaderBytes ||
            Get32(header, 8) != kFabricWindowEndianMarker ||
            Get32(header, 12) != kCanonicalPayloadFlag ||
            Get64(header, 16) != event.window_id ||
            Get64(header, 24) != event.window_generation ||
            Get64(header, 32) != event.session_epoch ||
            Get64(header, 40) != event.producer_sequence ||
            Get64(header, 48) > event.producer_sequence ||
            Get64(header, 48) < connection->second.last_peer_consumer_sequence ||
            Get16(header, 60) !=
                static_cast<uint16_t>(provider_capabilities.kind)) {
            return RejectReceive(connection_id, event,
                                 Corruption("fabric window header is invalid"));
        }
        const uint32_t payload_bytes = Get32(header, 56);
        if (payload_bytes == 0 || payload_bytes > options_.max_message_bytes ||
            payload_bytes > event.window.size() - kFabricWindowHeaderBytes ||
            payload_bytes >
                options_.max_queued_receive_bytes - queued_receive_bytes_) {
            return RejectReceive(connection_id, event,
                                 Corruption("fabric payload bounds are invalid"));
        }
        std::array<std::byte, kFabricWindowHeaderBytes> header_copy{};
        std::copy(header.begin(), header.end(), header_copy.begin());
        Put32(header_copy, 68, 0);
        Put64(header_copy, kFabricWindowCommitMarkerOffset, 0);
        const auto payload = event.window.subspan(kFabricWindowHeaderBytes,
                                                  payload_bytes);
        if (Get32(header, 68) != Crc32c(header_copy) ||
            Get32(header, 64) != Crc32c(payload)) {
            ++stats_.crc_failures;
            return RejectReceive(connection_id, event,
                                 Corruption("fabric window CRC mismatch"));
        }
        auto canonical = ValidateCanonical(payload, options_.max_message_bytes);
        if (!canonical.ok()) {
            return RejectReceive(connection_id, event, canonical.status());
        }
        if (event.producer_sequence < connection->second.next_receive_sequence) {
            // Duplicate doorbell/replay: release the exact old record without
            // publishing it twice. Bridge-level dedup remains authoritative too.
            return ReleaseReceive(connection_id, event);
        }
        if (event.producer_sequence != connection->second.next_receive_sequence) {
            return RejectReceive(connection_id, event,
                                 Corruption("fabric producer sequence has a gap"));
        }
        std::vector<std::byte> owned;
        try {
            owned.assign(payload.begin(), payload.end());
        } catch (const std::bad_alloc&) {
            return Exhausted("fabric receive allocation failed");
        }
        connection->second.last_peer_consumer_sequence = Get64(header, 48);
        receives_.push_back(ReceivedMessage{
            .connection_id = connection_id,
            .from = connection->second.info.peer_endpoint.value(),
            .payload = std::move(owned),
        });
        queued_receive_bytes_ += payload_bytes;
        ++connection->second.next_receive_sequence;
        return ReleaseReceive(connection_id, event);
    }

    Status ReleaseReceive(ConnectionId connection_id,
                          const platform::FabricProviderEvent& event) {
        std::atomic_thread_fence(std::memory_order_release);
        const Status released = options_.provider->ReleaseReceiveWindow(event);
        if (!released.ok()) {
            InvalidateConnection(connection_id, released);
            return released;
        }
        const Status rang = options_.provider->RingDoorbell({
            .protocol_version = platform::kMinoFabricMailboxProtocolVersion,
            .endian_marker = platform::kMinoFabricMailboxEndianMarker,
            .connection_id = event.connection_id,
            .kind = platform::FabricDoorbellKind::kConsumerRelease,
            .window_id = event.window_id,
            .window_generation = event.window_generation,
            .session_epoch = event.session_epoch,
            .producer_sequence = event.producer_sequence,
        });
        if (!rang.ok()) {
            ++stats_.doorbell_failures;
            InvalidateConnection(connection_id, rang);
            return rang;
        }
        return Status::Ok();
    }

    Status RejectReceive(ConnectionId connection_id,
                         const platform::FabricProviderEvent& event,
                         const Status& reason) {
        static_cast<void>(options_.provider->ReleaseReceiveWindow(event));
        health_.store(HealthState::kDegraded, std::memory_order_release);
        InvalidateConnection(connection_id, reason);
        return reason;
    }

    void InvalidateConnection(ConnectionId connection_id, const Status& reason,
                              bool provider_already_closed = false) {
        const auto connection = connections_.find(connection_id);
        if (connection == connections_.end()) return;
        const platform::FabricProviderConnectionId provider_id =
            connection->second.provider_id;
        if (!provider_already_closed) {
            static_cast<void>(options_.provider->Close(provider_id));
        }
        provider_to_connection_.erase(provider_id);
        connections_.erase(connection);
        for (auto iterator = receives_.begin(); iterator != receives_.end();) {
            if (iterator->connection_id == connection_id) {
                queued_receive_bytes_ -= iterator->payload.size();
                iterator = receives_.erase(iterator);
            } else {
                ++iterator;
            }
        }
        for (auto& [id, pending] : pending_) {
            (void)id;
            if (pending.connection_id == connection_id) {
                pending.terminal = true;
                pending.transport_consumed = true;  // Close is the device fence.
                pending.status = reason;
            }
        }
        FlushOrdered(connection_id);
    }

    void FlushAllOrdered() {
        std::vector<ConnectionId> ids;
        ids.reserve(order_.size());
        for (const auto& [id, queue] : order_) {
            (void)queue;
            ids.push_back(id);
        }
        for (ConnectionId id : ids) FlushOrdered(id);
    }

    void FlushOrdered(ConnectionId connection_id) {
        auto ordered = order_.find(connection_id);
        if (ordered == order_.end()) return;
        while (!ordered->second.empty()) {
            const uint64_t id = ordered->second.front();
            auto found = pending_.find(id);
            if (found == pending_.end()) {
                ordered->second.pop_front();
                continue;
            }
            Pending& pending = found->second;
            if (!pending.operation.has_value()) {
                if (!pending.transport_consumed && !pending.terminal) break;
                ordered->second.pop_front();
                pending_.erase(found);
                continue;
            }
            const bool successful_target =
                pending.status.ok() &&
                ((pending.target_stage == DeliveryStage::kLocalPublished &&
                  pending.committed) ||
                 (pending.target_stage == DeliveryStage::kRemoteAccepted &&
                  pending.remote_accepted));
            if (!pending.completion_emitted &&
                (pending.terminal || successful_target)) {
                if (completions_.size() >= options_.completion_queue_depth) break;
                completions_.push_back(DeliveryCompletion{
                    .operation = *pending.operation,
                    .reached_stage = pending.status.ok()
                                         ? pending.target_stage
                                         : DeliveryStage::kLocalPublished,
                    .status = pending.status,
                });
                operation_to_pending_.erase(pending.operation->id);
                pending.completion_emitted = true;
            }
            if (!pending.completion_emitted || !pending.transport_consumed) break;
            ordered->second.pop_front();
            pending_.erase(found);
        }
        if (ordered->second.empty()) order_.erase(ordered);
    }

    FabricDriverOptions& options_;
    std::atomic<HealthState>& health_;
    mutable std::mutex mutex_;
    DriverConfig config_;
    std::atomic<bool> stopping_{false};
    ConnectionId next_connection_id_ = 1;
    uint64_t next_pending_id_ = 1;
    std::unordered_map<ConnectionId, Connection> connections_;
    std::unordered_map<platform::FabricProviderConnectionId, ConnectionId>
        provider_to_connection_;
    std::unordered_map<ConnectionId, Listener> listeners_;
    std::unordered_map<uint64_t, Pending> pending_;
    std::unordered_map<OperationId, uint64_t> operation_to_pending_;
    std::unordered_map<ConnectionId, std::deque<uint64_t>> order_;
    std::deque<ReceivedMessage> receives_;
    std::deque<DeliveryCompletion> completions_;
    size_t queued_receive_bytes_ = 0;
    FabricDriverStats stats_;
};

Result<std::unique_ptr<FabricWindowDriver>> FabricWindowDriver::Create(
    FabricDriverOptions options) noexcept {
    const Status valid = ValidateFabricDriverOptions(options);
    if (!valid.ok()) return valid;
    try {
        return std::unique_ptr<FabricWindowDriver>(
            new FabricWindowDriver(std::move(options)));
    } catch (const std::bad_alloc&) {
        return Exhausted("fabric driver allocation failed");
    }
}

FabricWindowDriver::FabricWindowDriver(FabricDriverOptions options) noexcept
    : options_(std::move(options)),
      impl_(std::make_unique<Impl>(options_, health_)) {}

FabricWindowDriver::~FabricWindowDriver() {
    if (state() != DriverState::kStopped) static_cast<void>(Shutdown());
}

TransportCapabilities FabricWindowDriver::capabilities() const noexcept {
    return {.kind = TransportKind::kSharedFabric,
            .reliability = TransportReliability::kReliable,
            .max_frame_size = static_cast<uint32_t>(options_.max_message_bytes),
            .max_reassembly_bytes = options_.max_queued_receive_bytes,
            .features = Capability::kConnect | Capability::kListen |
                        Capability::kZeroCopyWindow |
                        Capability::kRemoteAcceptedConfirmation};
}

FabricDriverStats FabricWindowDriver::stats() const noexcept {
    return impl_->stats();
}
Status FabricWindowDriver::DoStart(const DriverConfig& config) {
    return impl_->Start(config);
}
void FabricWindowDriver::DoRequestStop() noexcept { impl_->RequestStop(); }
Status FabricWindowDriver::DoShutdown() { return impl_->Shutdown(); }
Result<ConnectionInfo> FabricWindowDriver::DoConnect(
    const ConnectRequest& request) {
    return impl_->Connect(request);
}
Result<ConnectionInfo> FabricWindowDriver::DoListen(
    const ListenRequest& request) {
    return impl_->Listen(request);
}
Result<ConnectionInfo> FabricWindowDriver::DoAccept(
    const AcceptRequest& request) {
    return impl_->Accept(request);
}
Result<SendResult> FabricWindowDriver::DoSend(const SendRequest& request,
                                               SendOperation operation) {
    return impl_->Send(request, operation);
}
Result<size_t> FabricWindowDriver::DoSendUntracked(
    const UntrackedSendRequest& request) {
    return impl_->SendUntracked(request);
}
Status FabricWindowDriver::DoConfirmRemoteAccepted(SendOperation operation) {
    return impl_->ConfirmRemoteAccepted(operation);
}
Result<ReceiveResult> FabricWindowDriver::DoPoll(
    const ReceiveRequest& request) {
    return impl_->Poll(request);
}
Result<CompletionPollResult> FabricWindowDriver::DoPollCompletions(
    const CompletionPollRequest& request) {
    return impl_->PollCompletions(request);
}
Result<security::AuthenticatedPeer> FabricWindowDriver::DoAuthenticatedPeer(
    ConnectionId connection_id) {
    return impl_->AuthenticatedPeer(connection_id);
}
Status FabricWindowDriver::DoClose(ConnectionId connection_id) {
    return impl_->Close(connection_id);
}

}  // namespace mino::transport
