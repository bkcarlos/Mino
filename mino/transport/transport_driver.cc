// Copyright 2026 The Mino Authors

#include "mino/transport/transport_driver.h"

#include <algorithm>
#include <new>
#include <string_view>

namespace mino::transport {
namespace {

constexpr std::byte kEndpointMagic{0x4d};
constexpr uint8_t kEndpointVersion = 1;
constexpr size_t kEndpointHeaderSize = 5;

enum class EndpointForm : uint8_t {
    kIpv4 = 1,
    kIpv6 = 2,
    kFabric = 3,
};

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status Corrupt(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Unsupported(std::string_view message) {
    return Status::Error(StatusCode::kUnsupported, message);
}

Status AllocationFailure() {
    return Status::Error(StatusCode::kResourceExhausted);
}

Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

bool IsKnownKind(TransportKind kind) noexcept {
    switch (kind) {
        case TransportKind::kNetwork:
        case TransportKind::kRdma:
        case TransportKind::kSharedFabric:
            return true;
    }
    return false;
}

bool IsKnownReliability(TransportReliability reliability) noexcept {
    switch (reliability) {
        case TransportReliability::kUnreliable:
        case TransportReliability::kOrderedLossy:
        case TransportReliability::kReliable:
            return true;
    }
    return false;
}

bool IsTransportDeliveryStage(DeliveryStage stage) noexcept {
    return stage == DeliveryStage::kLocalPublished ||
           stage == DeliveryStage::kRemoteAccepted;
}

bool ReachedStageDoesNotExceed(DeliveryStage reached,
                               DeliveryStage target) noexcept {
    if (!IsTransportDeliveryStage(reached) ||
        !IsTransportDeliveryStage(target)) {
        return false;
    }
    if (target == DeliveryStage::kLocalPublished) {
        return reached == DeliveryStage::kLocalPublished;
    }
    return true;
}

void StoreBigEndian16(uint16_t value, std::span<std::byte> output) {
    output[0] = static_cast<std::byte>(value >> 8);
    output[1] = static_cast<std::byte>(value);
}

void StoreBigEndian32(uint32_t value, std::span<std::byte> output) {
    output[0] = static_cast<std::byte>(value >> 24);
    output[1] = static_cast<std::byte>(value >> 16);
    output[2] = static_cast<std::byte>(value >> 8);
    output[3] = static_cast<std::byte>(value);
}

uint16_t LoadBigEndian16(std::span<const std::byte> input) {
    return static_cast<uint16_t>(
        (static_cast<uint16_t>(std::to_integer<uint8_t>(input[0])) << 8) |
        static_cast<uint16_t>(std::to_integer<uint8_t>(input[1])));
}

uint32_t LoadBigEndian32(std::span<const std::byte> input) {
    return (static_cast<uint32_t>(std::to_integer<uint8_t>(input[0])) << 24) |
           (static_cast<uint32_t>(std::to_integer<uint8_t>(input[1])) << 16) |
           (static_cast<uint32_t>(std::to_integer<uint8_t>(input[2])) << 8) |
           static_cast<uint32_t>(std::to_integer<uint8_t>(input[3]));
}

Status ValidateDriverCapabilitiesForCall(
    const TransportCapabilities& capabilities) {
    const Status status = ValidateTransportCapabilities(capabilities);
    if (status.ok()) return status;
    return Status::Error(StatusCode::kInternal,
                         "driver returned invalid capabilities");
}

Status InvalidDriverResult(std::string_view operation) {
    return Status::Error(StatusCode::kInternal, operation);
}

}  // namespace

Result<Capabilities> Capabilities::FromBits(uint32_t bits) {
    if ((bits & ~kKnownBits) != 0) {
        return Invalid("capability mask contains unknown bits");
    }
    return FromKnownBits(bits);
}

Status ValidateTransportCapabilities(
    const TransportCapabilities& capabilities) {
    if (!IsKnownKind(capabilities.kind)) {
        return Invalid("unknown transport kind");
    }
    if (!IsKnownReliability(capabilities.reliability)) {
        return Invalid("unknown transport reliability");
    }
    if (capabilities.max_frame_size > kMaxPayloadBytes) {
        return Exhausted("driver frame limit exceeds transport API bound");
    }
    if (capabilities.max_reassembly_bytes > kMaxPayloadBytes) {
        return Exhausted("driver reassembly limit exceeds transport API bound");
    }
    if (capabilities.features.Has(Capability::kRemoteWrite) &&
        capabilities.kind == TransportKind::kNetwork) {
        return Invalid("network transport cannot advertise remote write");
    }
    if (capabilities.features.Has(Capability::kZeroCopyWindow) &&
        capabilities.kind == TransportKind::kNetwork) {
        return Invalid("network transport cannot advertise a shared window");
    }
    return Status::Ok();
}

Result<EndpointDescriptor> EndpointDescriptor::Ipv4Tcp(
    std::span<const std::byte> address, uint16_t port) {
    return Ip(TransportKind::kNetwork, EndpointAddressFamily::kIpv4,
              NetworkProtocol::kTcp, address, port);
}

Result<EndpointDescriptor> EndpointDescriptor::Ipv6Tcp(
    std::span<const std::byte> address, uint16_t port) {
    return Ip(TransportKind::kNetwork, EndpointAddressFamily::kIpv6,
              NetworkProtocol::kTcp, address, port);
}

Result<EndpointDescriptor> EndpointDescriptor::Ipv4Udp(
    std::span<const std::byte> address, uint16_t port) {
    return Ip(TransportKind::kNetwork, EndpointAddressFamily::kIpv4,
              NetworkProtocol::kUdp, address, port);
}

Result<EndpointDescriptor> EndpointDescriptor::Ipv6Udp(
    std::span<const std::byte> address, uint16_t port) {
    return Ip(TransportKind::kNetwork, EndpointAddressFamily::kIpv6,
              NetworkProtocol::kUdp, address, port);
}

Result<EndpointDescriptor> EndpointDescriptor::Ip(
    TransportKind kind, EndpointAddressFamily family, NetworkProtocol protocol,
    std::span<const std::byte> address, uint16_t port) {
    const size_t expected_size = family == EndpointAddressFamily::kIpv4
                                     ? kIpv4AddressBytes
                                 : family == EndpointAddressFamily::kIpv6
                                     ? kIpv6AddressBytes
                                     : 0;
    if (expected_size == 0) {
        return Invalid("IP endpoint has an unknown address family");
    }
    if (address.size() != expected_size) {
        return Invalid("IP address length does not match its family");
    }
    if (port == 0) {
        return Invalid("IP endpoint port must be non-zero");
    }
    if (kind == TransportKind::kNetwork &&
        protocol != NetworkProtocol::kTcp &&
        protocol != NetworkProtocol::kUdp) {
        return Invalid("network IP endpoint must use TCP or UDP");
    }
    if (kind == TransportKind::kRdma &&
        protocol != NetworkProtocol::kRdmaCompatible) {
        return Invalid("RDMA IP endpoint has an incompatible protocol");
    }
    if (kind != TransportKind::kNetwork && kind != TransportKind::kRdma) {
        return Invalid("IP endpoint has an incompatible transport kind");
    }

    EndpointDescriptor endpoint;
    endpoint.kind_ = kind;
    endpoint.family_ = family;
    endpoint.protocol_ = protocol;
    endpoint.port_ = port;
    std::copy(address.begin(), address.end(), endpoint.data_.begin());
    return endpoint;
}

Result<EndpointDescriptor> EndpointDescriptor::SharedFabric(
    uint32_t fabric_domain, uint32_t channel_id,
    std::span<const std::byte> opaque) {
    if (opaque.size() > kMaxFabricOpaqueBytes) {
        return Invalid("fabric opaque endpoint exceeds 24 bytes");
    }
    EndpointDescriptor endpoint;
    endpoint.kind_ = TransportKind::kSharedFabric;
    endpoint.family_ = EndpointAddressFamily::kUnspecified;
    endpoint.protocol_ = NetworkProtocol::kNone;
    endpoint.fabric_domain_ = fabric_domain;
    endpoint.channel_id_ = channel_id;
    endpoint.opaque_size_ = static_cast<uint8_t>(opaque.size());
    std::copy(opaque.begin(), opaque.end(), endpoint.data_.begin());
    return endpoint;
}

std::span<const std::byte> EndpointDescriptor::ip_address() const noexcept {
    if (family_ == EndpointAddressFamily::kIpv4) {
        return std::span<const std::byte>(data_).first(kIpv4AddressBytes);
    }
    if (family_ == EndpointAddressFamily::kIpv6) {
        return std::span<const std::byte>(data_).first(kIpv6AddressBytes);
    }
    return {};
}

std::span<const std::byte> EndpointDescriptor::fabric_opaque() const noexcept {
    if (kind_ != TransportKind::kSharedFabric ||
        opaque_size_ > kMaxFabricOpaqueBytes) {
        return {};
    }
    return std::span<const std::byte>(data_).first(opaque_size_);
}

Status ValidateEndpointDescriptor(const EndpointDescriptor& endpoint) {
    if (!IsKnownKind(endpoint.kind())) {
        return Invalid("endpoint has unknown transport kind");
    }
    if (endpoint.kind() == TransportKind::kSharedFabric) {
        if (endpoint.address_family() != EndpointAddressFamily::kUnspecified ||
            endpoint.protocol() != NetworkProtocol::kNone ||
            endpoint.port() != 0) {
            return Invalid("fabric endpoint contains network fields");
        }
        if (endpoint.fabric_opaque().size() >
            EndpointDescriptor::kMaxFabricOpaqueBytes) {
            return Exhausted("fabric opaque endpoint exceeds its bound");
        }
        return Status::Ok();
    }

    const size_t expected_size =
        endpoint.address_family() == EndpointAddressFamily::kIpv4
            ? EndpointDescriptor::kIpv4AddressBytes
        : endpoint.address_family() == EndpointAddressFamily::kIpv6
            ? EndpointDescriptor::kIpv6AddressBytes
            : 0;
    if (expected_size == 0 || endpoint.ip_address().size() != expected_size) {
        return Invalid("IP endpoint has invalid address family or length");
    }
    if (endpoint.port() == 0) {
        return Invalid("IP endpoint port must be non-zero");
    }
    if (endpoint.kind() == TransportKind::kNetwork &&
        endpoint.protocol() != NetworkProtocol::kTcp &&
        endpoint.protocol() != NetworkProtocol::kUdp) {
        return Invalid("network endpoint does not use TCP or UDP");
    }
    if (endpoint.kind() == TransportKind::kRdma &&
        endpoint.protocol() != NetworkProtocol::kRdmaCompatible) {
        return Invalid("RDMA endpoint protocol is invalid");
    }
    return Status::Ok();
}

Result<size_t> SerializeEndpointDescriptor(
    const EndpointDescriptor& endpoint, std::span<std::byte> output) {
    const Status endpoint_status = ValidateEndpointDescriptor(endpoint);
    if (!endpoint_status.ok()) return endpoint_status;

    EndpointForm form = EndpointForm::kFabric;
    size_t required_size = kEndpointHeaderSize;
    if (endpoint.kind() == TransportKind::kSharedFabric) {
        required_size += 9 + endpoint.fabric_opaque().size();
    } else {
        form = endpoint.address_family() == EndpointAddressFamily::kIpv4
                   ? EndpointForm::kIpv4
                   : EndpointForm::kIpv6;
        required_size += 2 + endpoint.ip_address().size();
    }
    if (output.size() < required_size) {
        return Exhausted("endpoint serialization output is too small");
    }

    output[0] = kEndpointMagic;
    output[1] = static_cast<std::byte>(kEndpointVersion);
    output[2] = static_cast<std::byte>(endpoint.kind());
    output[3] = static_cast<std::byte>(form);
    output[4] = static_cast<std::byte>(endpoint.protocol());
    if (form == EndpointForm::kFabric) {
        StoreBigEndian32(endpoint.fabric_domain(), output.subspan(5, 4));
        StoreBigEndian32(endpoint.channel_id(), output.subspan(9, 4));
        output[13] = static_cast<std::byte>(endpoint.fabric_opaque().size());
        std::copy(endpoint.fabric_opaque().begin(),
                  endpoint.fabric_opaque().end(), output.begin() + 14);
    } else {
        StoreBigEndian16(endpoint.port(), output.subspan(5, 2));
        std::copy(endpoint.ip_address().begin(), endpoint.ip_address().end(),
                  output.begin() + 7);
    }
    return required_size;
}

Result<EndpointDescriptor> ParseEndpointDescriptor(
    std::span<const std::byte> input) {
    if (input.size() > EndpointDescriptor::kMaxSerializedSize) {
        return Exhausted("serialized endpoint exceeds Registry slot bound");
    }
    if (input.size() < kEndpointHeaderSize) {
        return Corrupt("serialized endpoint is truncated");
    }
    if (input[0] != kEndpointMagic) {
        return Corrupt("serialized endpoint has invalid magic");
    }
    if (std::to_integer<uint8_t>(input[1]) != kEndpointVersion) {
        return Unsupported("serialized endpoint version is unsupported");
    }

    const auto kind =
        static_cast<TransportKind>(std::to_integer<uint8_t>(input[2]));
    if (!IsKnownKind(kind)) {
        return Unsupported("serialized endpoint kind is unsupported");
    }
    const auto form =
        static_cast<EndpointForm>(std::to_integer<uint8_t>(input[3]));
    const auto protocol =
        static_cast<NetworkProtocol>(std::to_integer<uint8_t>(input[4]));

    if (form == EndpointForm::kFabric) {
        if (kind != TransportKind::kSharedFabric ||
            protocol != NetworkProtocol::kNone) {
            return Corrupt("fabric endpoint discriminants are non-canonical");
        }
        if (input.size() < 14) {
            return Corrupt("serialized fabric endpoint is truncated");
        }
        const size_t opaque_size = std::to_integer<uint8_t>(input[13]);
        if (opaque_size > EndpointDescriptor::kMaxFabricOpaqueBytes) {
            return Exhausted("serialized fabric opaque endpoint is oversized");
        }
        if (input.size() != 14 + opaque_size) {
            return Corrupt("serialized fabric endpoint has trailing or missing bytes");
        }
        return EndpointDescriptor::SharedFabric(
            LoadBigEndian32(input.subspan(5, 4)),
            LoadBigEndian32(input.subspan(9, 4)), input.subspan(14, opaque_size));
    }

    EndpointAddressFamily family = EndpointAddressFamily::kUnspecified;
    size_t address_size = 0;
    if (form == EndpointForm::kIpv4) {
        family = EndpointAddressFamily::kIpv4;
        address_size = EndpointDescriptor::kIpv4AddressBytes;
    } else if (form == EndpointForm::kIpv6) {
        family = EndpointAddressFamily::kIpv6;
        address_size = EndpointDescriptor::kIpv6AddressBytes;
    } else {
        return Corrupt("serialized endpoint form is unknown");
    }
    if (kind == TransportKind::kSharedFabric) {
        return Corrupt("IP endpoint uses the fabric transport kind");
    }
    if (protocol != NetworkProtocol::kTcp &&
        protocol != NetworkProtocol::kRdmaCompatible &&
        protocol != NetworkProtocol::kUdp) {
        return Unsupported("serialized network protocol is unsupported");
    }
    if ((kind == TransportKind::kNetwork &&
         protocol != NetworkProtocol::kTcp &&
         protocol != NetworkProtocol::kUdp) ||
        (kind == TransportKind::kRdma &&
         protocol != NetworkProtocol::kRdmaCompatible)) {
        return Corrupt("serialized endpoint kind and protocol disagree");
    }
    const size_t expected_size = 7 + address_size;
    if (input.size() != expected_size) {
        return Corrupt("serialized IP endpoint has trailing or missing bytes");
    }
    const uint16_t port = LoadBigEndian16(input.subspan(5, 2));
    if (port == 0) {
        return Corrupt("serialized IP endpoint has a zero port");
    }
    return EndpointDescriptor::Ip(kind, family, protocol,
                                  input.subspan(7, address_size), port);
}

Status ValidateDriverConfig(const DriverConfig& config) {
    if (config.max_connections == 0 || config.max_listeners == 0 ||
        config.max_queued_sends == 0) {
        return Invalid("driver capacities must be non-zero");
    }
    if (config.max_connections > kMaxConnections ||
        config.max_listeners > kMaxListeners ||
        config.max_queued_sends > kMaxQueuedSends) {
        return Exhausted("driver configuration exceeds an API capacity bound");
    }
    return Status::Ok();
}

Status ValidateConnectionInfo(const ConnectionInfo& info) {
    if (info.id == kInvalidConnectionId) {
        return Invalid("connection id must be non-zero");
    }
    if (!IsKnownKind(info.kind)) {
        return Invalid("connection has unknown transport kind");
    }
    if (!info.local_endpoint.has_value()) {
        return Invalid("connection is missing its local endpoint");
    }
    MINO_RETURN_IF_ERROR(ValidateEndpointDescriptor(*info.local_endpoint));
    if (info.local_endpoint->kind() != info.kind) {
        return Invalid("local endpoint kind differs from connection kind");
    }
    if (info.is_listener) {
        if (info.peer_endpoint.has_value()) {
            return Invalid("listener must not have a peer endpoint");
        }
        return Status::Ok();
    }
    if (!info.peer_endpoint.has_value()) {
        return Invalid("connected endpoint is missing its peer");
    }
    MINO_RETURN_IF_ERROR(ValidateEndpointDescriptor(*info.peer_endpoint));
    if (info.peer_endpoint->kind() != info.kind) {
        return Invalid("peer endpoint kind differs from connection kind");
    }
    return Status::Ok();
}

Status ValidateConnectRequest(const ConnectRequest& request,
                              const TransportCapabilities& capabilities) {
    MINO_RETURN_IF_ERROR(ValidateTransportCapabilities(capabilities));
    MINO_RETURN_IF_ERROR(ValidateEndpointDescriptor(request.remote_endpoint));
    if (!capabilities.features.Has(Capability::kConnect)) {
        return Unsupported("driver does not support Connect");
    }
    if (request.remote_endpoint.kind() != capabilities.kind) {
        return Unsupported("remote endpoint kind is unsupported by driver");
    }
    if (request.timeout_ms > kMaxOperationTimeoutMs) {
        return Invalid("connect timeout exceeds the public maximum");
    }
    if (request.local_bind.has_value()) {
        MINO_RETURN_IF_ERROR(ValidateEndpointDescriptor(*request.local_bind));
        if (request.local_bind->kind() != request.remote_endpoint.kind()) {
            return Invalid("local and remote endpoint kinds differ");
        }
    }
    return Status::Ok();
}

Status ValidateListenRequest(const ListenRequest& request,
                             const TransportCapabilities& capabilities) {
    MINO_RETURN_IF_ERROR(ValidateTransportCapabilities(capabilities));
    MINO_RETURN_IF_ERROR(ValidateEndpointDescriptor(request.local_endpoint));
    if (!capabilities.features.Has(Capability::kListen)) {
        return Unsupported("driver does not support Listen");
    }
    if (request.local_endpoint.kind() != capabilities.kind) {
        return Unsupported("listen endpoint kind is unsupported by driver");
    }
    if (request.backlog == 0) {
        return Invalid("listen backlog must be non-zero");
    }
    if (request.backlog > kMaxConnections) {
        return Exhausted("listen backlog exceeds the connection bound");
    }
    return Status::Ok();
}

Status ValidateAcceptRequest(const AcceptRequest& request) {
    if (request.listener_id == kInvalidConnectionId) {
        return Invalid("accept listener id must be non-zero");
    }
    if (request.timeout_ms > kMaxOperationTimeoutMs) {
        return Invalid("accept timeout exceeds the public maximum");
    }
    return Status::Ok();
}

Status ValidateSendRequest(const SendRequest& request,
                           const TransportCapabilities& capabilities) {
    MINO_RETURN_IF_ERROR(ValidateTransportCapabilities(capabilities));
    if (request.connection_id == kInvalidConnectionId) {
        return Invalid("send connection id must be non-zero");
    }
    if (request.payload.empty()) {
        return Invalid("send payload must be non-empty");
    }
    if (request.payload.size() > kMaxPayloadBytes) {
        return Exhausted("send payload exceeds the transport API bound");
    }
    if (capabilities.max_frame_size != 0 &&
        request.payload.size() > capabilities.max_frame_size) {
        return Exhausted("send payload exceeds the driver frame limit");
    }
    if (request.target_stage != DeliveryStage::kRemoteAccepted) {
        return Unsupported("transport driver only targets remote acceptance");
    }
    return Status::Ok();
}

Status ValidateReceiveRequest(const ReceiveRequest& request) {
    if (request.max_messages == 0 || request.max_bytes == 0) {
        return Invalid("receive limits must be non-zero");
    }
    if (request.max_messages > kMaxReceiveBatchMessages ||
        request.max_bytes > kMaxReceiveBatchBytes) {
        return Exhausted("receive request exceeds an API capacity bound");
    }
    if (request.timeout_ms > kMaxOperationTimeoutMs) {
        return Invalid("receive timeout exceeds the public maximum");
    }
    return Status::Ok();
}

Status ValidateCompletionPollRequest(const CompletionPollRequest& request) {
    if (request.max_completions == 0) {
        return Invalid("completion poll limit must be non-zero");
    }
    if (request.max_completions > kMaxCompletionBatchOperations) {
        return Exhausted("completion poll exceeds the batch operation bound");
    }
    if (request.timeout_ms > kMaxOperationTimeoutMs) {
        return Invalid("completion poll timeout exceeds the public maximum");
    }
    return Status::Ok();
}

Status ValidateSendResult(const SendRequest& request,
                          const SendOperation& expected_operation,
                          const SendResult& result) {
    if (result.operation.id == kInvalidOperationId ||
        result.operation != expected_operation) {
        return Invalid("successful send returned the wrong operation identity");
    }
    if (result.admitted_bytes != request.payload.size()) {
        return Invalid("successful send must admit the complete message");
    }
    return Status::Ok();
}

Status ValidateReceiveResult(const ReceiveRequest& request,
                             const ReceiveResult& result) {
    if (result.messages.empty()) {
        return Invalid("successful receive result must contain a message");
    }
    if (result.messages.size() > request.max_messages) {
        return Exhausted("receive result exceeds requested message count");
    }
    size_t total_bytes = 0;
    for (const ReceivedMessage& message : result.messages) {
        if (message.connection_id == kInvalidConnectionId ||
            message.payload.empty()) {
            return Invalid("received message has invalid id or empty payload");
        }
        MINO_RETURN_IF_ERROR(ValidateEndpointDescriptor(message.from));
        if (message.payload.size() > request.max_bytes - total_bytes) {
            return Exhausted("receive result exceeds requested byte count");
        }
        total_bytes += message.payload.size();
    }
    return Status::Ok();
}

Status ValidateCompletionPollResult(const CompletionPollRequest& request,
                                    const CompletionPollResult& result) {
    if (result.completions.empty()) {
        return Invalid("successful completion poll must contain a completion");
    }
    if (result.completions.size() > request.max_completions) {
        return Exhausted("completion result exceeds requested operation count");
    }
    for (size_t i = 0; i < result.completions.size(); ++i) {
        const DeliveryCompletion& completion = result.completions[i];
        if (completion.operation.id == kInvalidOperationId ||
            completion.operation.connection_id == kInvalidConnectionId ||
            !IsTransportDeliveryStage(completion.reached_stage)) {
            return Invalid("completion has invalid identity or delivery stage");
        }
        for (size_t j = 0; j < i; ++j) {
            if (result.completions[j].operation.id == completion.operation.id) {
                return Invalid("completion result contains a duplicate operation");
            }
        }
    }
    return Status::Ok();
}

void TransportDriver::DoRequestStop() noexcept {}

Result<ConnectionInfo> TransportDriver::DoAccept(const AcceptRequest&) {
    return Unsupported("driver does not support Accept");
}

TransportDriver::ActiveOperation::~ActiveOperation() {
    if (driver_ != nullptr) driver_->ReleaseActiveOperation();
}

Result<TransportDriver::ActiveOperation>
TransportDriver::AcquireActiveOperation() {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_relaxed) != DriverState::kRunning) {
        return Unavailable("transport driver is not running");
    }
    ++active_operations_;
    return ActiveOperation(this);
}

void TransportDriver::ReleaseActiveOperation() noexcept {
    std::lock_guard lock(lifecycle_mutex_);
    --active_operations_;
    if (active_operations_ == 0) lifecycle_cv_.notify_all();
}

Result<SendOperation> TransportDriver::ReserveSendOperation(
    const SendRequest& request) {
    std::lock_guard lock(lifecycle_mutex_);
    if (outstanding_sends_.size() >= max_outstanding_sends_) {
        return Status::Error(StatusCode::kWouldBlock,
                             "send completion table is full");
    }

    OperationId candidate = next_operation_id_;
    for (;;) {
        if (candidate == kInvalidOperationId) candidate = 1;
        next_operation_id_ = candidate + 1;
        if (next_operation_id_ == kInvalidOperationId) next_operation_id_ = 1;
        if (!outstanding_sends_.contains(candidate)) break;
        candidate = next_operation_id_;
    }

    outstanding_sends_.emplace(
        candidate, OutstandingSend{.connection_id = request.connection_id,
                                   .target_stage = request.target_stage});
    return SendOperation{.id = candidate,
                         .connection_id = request.connection_id};
}

void TransportDriver::CancelSendOperation(OperationId operation_id) noexcept {
    std::lock_guard lock(lifecycle_mutex_);
    outstanding_sends_.erase(operation_id);
}

Status TransportDriver::ValidateAndRetireCompletions(
    const CompletionPollResult& result) {
    std::lock_guard lock(lifecycle_mutex_);
    for (const DeliveryCompletion& completion : result.completions) {
        const auto outstanding =
            outstanding_sends_.find(completion.operation.id);
        if (outstanding == outstanding_sends_.end() ||
            outstanding->second.connection_id !=
                completion.operation.connection_id ||
            !ReachedStageDoesNotExceed(completion.reached_stage,
                                       outstanding->second.target_stage) ||
            (completion.status.ok() &&
             completion.reached_stage != outstanding->second.target_stage)) {
            return Status::Error(StatusCode::kInternal);
        }
    }
    for (const DeliveryCompletion& completion : result.completions) {
        outstanding_sends_.erase(completion.operation.id);
    }
    return Status::Ok();
}

Status TransportDriver::Start(const DriverConfig& config) {
    try {
        MINO_RETURN_IF_ERROR(ValidateDriverConfig(config));
        MINO_RETURN_IF_ERROR(
            ValidateDriverCapabilitiesForCall(capabilities()));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }

    {
        std::lock_guard lock(lifecycle_mutex_);
        const DriverState current = state_.load(std::memory_order_relaxed);
        if (current == DriverState::kRunning || start_in_progress_) {
            return Status::Error(StatusCode::kAlreadyExists);
        }
        if (current == DriverState::kStopping) {
            return Status::Error(StatusCode::kUnavailable);
        }
        start_in_progress_ = true;
        max_outstanding_sends_ = config.max_queued_sends;
        outstanding_sends_.clear();
    }

    Status start_status;
    try {
        start_status = DoStart(config);
    } catch (const std::bad_alloc&) {
        start_status = AllocationFailure();
    } catch (...) {
        std::lock_guard lock(lifecycle_mutex_);
        start_in_progress_ = false;
        max_outstanding_sends_ = 0;
        outstanding_sends_.clear();
        state_.store(DriverState::kStopped, std::memory_order_release);
        lifecycle_cv_.notify_all();
        throw;
    }

    {
        std::lock_guard lock(lifecycle_mutex_);
        start_in_progress_ = false;
        if (start_status.ok()) {
            state_.store(DriverState::kRunning, std::memory_order_release);
        } else {
            max_outstanding_sends_ = 0;
            outstanding_sends_.clear();
            state_.store(DriverState::kStopped, std::memory_order_release);
        }
        lifecycle_cv_.notify_all();
    }
    return start_status;
}

Status TransportDriver::Shutdown() {
    {
        std::unique_lock lock(lifecycle_mutex_);
        lifecycle_cv_.wait(lock, [this] { return !start_in_progress_; });
        const DriverState current = state_.load(std::memory_order_relaxed);
        if (current == DriverState::kStopped) return Status::Ok();
        if (current == DriverState::kStopping) {
            lifecycle_cv_.wait(lock, [this] {
                return state_.load(std::memory_order_relaxed) ==
                       DriverState::kStopped;
            });
            return Status::Ok();
        }
        state_.store(DriverState::kStopping, std::memory_order_release);
    }

    DoRequestStop();
    {
        std::unique_lock lock(lifecycle_mutex_);
        lifecycle_cv_.wait(lock,
                           [this] { return active_operations_ == 0; });
    }

    Status shutdown_status;
    try {
        shutdown_status = DoShutdown();
    } catch (const std::bad_alloc&) {
        shutdown_status = AllocationFailure();
    } catch (...) {
        std::lock_guard lock(lifecycle_mutex_);
        outstanding_sends_.clear();
        max_outstanding_sends_ = 0;
        state_.store(DriverState::kStopped, std::memory_order_release);
        lifecycle_cv_.notify_all();
        throw;
    }

    {
        std::lock_guard lock(lifecycle_mutex_);
        outstanding_sends_.clear();
        max_outstanding_sends_ = 0;
        state_.store(DriverState::kStopped, std::memory_order_release);
        lifecycle_cv_.notify_all();
    }
    return shutdown_status;
}

Result<ConnectionInfo> TransportDriver::Connect(
    const ConnectRequest& request) {
    try {
        auto active = AcquireActiveOperation();
        if (!active.ok()) return active.status();
        const TransportCapabilities driver_capabilities = capabilities();
        MINO_RETURN_IF_ERROR(
            ValidateDriverCapabilitiesForCall(driver_capabilities));
        MINO_RETURN_IF_ERROR(
            ValidateConnectRequest(request, driver_capabilities));
        auto result = DoConnect(request);
        if (!result.ok()) return result.status();
        if (!ValidateConnectionInfo(*result).ok() || result->is_listener ||
            result->kind != driver_capabilities.kind) {
            return InvalidDriverResult("driver returned invalid Connect result");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<ConnectionInfo> TransportDriver::Listen(const ListenRequest& request) {
    try {
        auto active = AcquireActiveOperation();
        if (!active.ok()) return active.status();
        const TransportCapabilities driver_capabilities = capabilities();
        MINO_RETURN_IF_ERROR(
            ValidateDriverCapabilitiesForCall(driver_capabilities));
        MINO_RETURN_IF_ERROR(
            ValidateListenRequest(request, driver_capabilities));
        auto result = DoListen(request);
        if (!result.ok()) return result.status();
        if (!ValidateConnectionInfo(*result).ok() || !result->is_listener ||
            result->kind != driver_capabilities.kind) {
            return InvalidDriverResult("driver returned invalid Listen result");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<ConnectionInfo> TransportDriver::Accept(
    const AcceptRequest& request) {
    try {
        auto active = AcquireActiveOperation();
        if (!active.ok()) return active.status();
        MINO_RETURN_IF_ERROR(ValidateAcceptRequest(request));
        const TransportCapabilities driver_capabilities = capabilities();
        MINO_RETURN_IF_ERROR(
            ValidateDriverCapabilitiesForCall(driver_capabilities));
        auto result = DoAccept(request);
        if (!result.ok()) return result.status();
        if (!ValidateConnectionInfo(*result).ok() || result->is_listener ||
            result->kind != driver_capabilities.kind) {
            return InvalidDriverResult("driver returned invalid Accept result");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<SendResult> TransportDriver::Send(const SendRequest& request) {
    try {
        auto active = AcquireActiveOperation();
        if (!active.ok()) return active.status();
        const TransportCapabilities driver_capabilities = capabilities();
        MINO_RETURN_IF_ERROR(
            ValidateDriverCapabilitiesForCall(driver_capabilities));
        MINO_RETURN_IF_ERROR(
            ValidateSendRequest(request, driver_capabilities));
        auto reserved = ReserveSendOperation(request);
        if (!reserved.ok()) return reserved.status();
        const SendOperation operation = *reserved;

        try {
            auto result = DoSend(request, operation);
            if (!result.ok()) {
                CancelSendOperation(operation.id);
                return result.status();
            }
            if (!ValidateSendResult(request, operation, *result).ok()) {
                CancelSendOperation(operation.id);
                return InvalidDriverResult(
                    "driver returned invalid Send admission");
            }
            return result;
        } catch (const std::bad_alloc&) {
            CancelSendOperation(operation.id);
            return AllocationFailure();
        } catch (...) {
            CancelSendOperation(operation.id);
            throw;
        }
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<ReceiveResult> TransportDriver::Poll(const ReceiveRequest& request) {
    try {
        auto active = AcquireActiveOperation();
        if (!active.ok()) return active.status();
        MINO_RETURN_IF_ERROR(ValidateReceiveRequest(request));
        MINO_RETURN_IF_ERROR(
            ValidateDriverCapabilitiesForCall(capabilities()));
        auto result = DoPoll(request);
        if (!result.ok()) return result.status();
        if (!ValidateReceiveResult(request, *result).ok()) {
            return InvalidDriverResult("driver returned invalid Poll result");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<CompletionPollResult> TransportDriver::PollCompletions(
    const CompletionPollRequest& request) {
    try {
        auto active = AcquireActiveOperation();
        if (!active.ok()) return active.status();
        MINO_RETURN_IF_ERROR(ValidateCompletionPollRequest(request));
        MINO_RETURN_IF_ERROR(
            ValidateDriverCapabilitiesForCall(capabilities()));
        auto result = DoPollCompletions(request);
        if (!result.ok()) return result.status();
        if (!ValidateCompletionPollResult(request, *result).ok()) {
            return InvalidDriverResult(
                "driver returned invalid completion batch");
        }
        if (!ValidateAndRetireCompletions(*result).ok()) {
            return InvalidDriverResult(
                "driver returned mismatched delivery completion");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status TransportDriver::Close(ConnectionId connection_id) {
    try {
        if (connection_id == kInvalidConnectionId) {
            return Invalid("close connection id must be non-zero");
        }
        if (state() == DriverState::kStopped) return Status::Ok();
        auto active = AcquireActiveOperation();
        if (!active.ok()) return active.status();
        return DoClose(connection_id);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

}  // namespace mino::transport
