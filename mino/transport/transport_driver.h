// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_TRANSPORT_TRANSPORT_DRIVER_H_
#define MINO_TRANSPORT_TRANSPORT_DRIVER_H_

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/runtime/delivery_receipt.h"
#include "mino/security/tls.h"

namespace mino::transport {

// Absolute API limits. A driver may advertise lower limits, but never higher
// ones. These bounds apply before an implementation allocates or retains data.
inline constexpr uint32_t kMaxConnections = 65'536;
inline constexpr uint32_t kMaxListeners = 4'096;
inline constexpr uint32_t kMaxQueuedSends = 1u << 20;
inline constexpr uint32_t kMaxReceiveBatchMessages = 1'024;
inline constexpr uint32_t kMaxCompletionBatchOperations = 1'024;
inline constexpr size_t kMaxPayloadBytes = 64u << 20;
inline constexpr size_t kMaxReceiveBatchBytes = 64u << 20;
inline constexpr uint32_t kMaxOperationTimeoutMs = 60'000;

using ConnectionId = uint64_t;
inline constexpr ConnectionId kInvalidConnectionId = 0;

enum class TransportKind : uint8_t {
    kNetwork = 0,
    kRdma = 1,
    kSharedFabric = 2,
};

enum class TransportReliability : uint8_t {
    kUnreliable = 0,
    kOrderedLossy = 1,
    kReliable = 2,
};

// Individual feature bits. Capability masks can only be formed through the
// typed operations below; FromBits() rejects unknown bits at trust boundaries.
enum class Capability : uint32_t {
    kNone = 0,
    kConnect = 1u << 0,
    kListen = 1u << 1,
    kZeroCopyWindow = 1u << 2,
    kMulticast = 1u << 3,
    kRemoteWrite = 1u << 4,
    // The driver can turn a protocol-validated peer ACK into a successful
    // kRemoteAccepted completion through ConfirmRemoteAccepted().
    kRemoteAcceptedConfirmation = 1u << 5,
};

class Capabilities;
constexpr Capabilities operator|(Capability lhs, Capability rhs) noexcept;

class Capabilities {
public:
    constexpr Capabilities() noexcept = default;
    constexpr Capabilities(Capability capability) noexcept
        : bits_(static_cast<uint32_t>(capability)) {}

    static Result<Capabilities> FromBits(uint32_t bits);

    constexpr uint32_t bits() const noexcept { return bits_; }
    constexpr bool empty() const noexcept { return bits_ == 0; }
    constexpr bool Has(Capability capability) const noexcept {
        const uint32_t bit = static_cast<uint32_t>(capability);
        return bit != 0 && (bits_ & bit) == bit;
    }
    constexpr bool ContainsAll(Capabilities other) const noexcept {
        return (bits_ & other.bits_) == other.bits_;
    }
    constexpr Capabilities With(Capability capability) const noexcept {
        return FromKnownBits(bits_ | static_cast<uint32_t>(capability));
    }
    constexpr Capabilities Without(Capability capability) const noexcept {
        return FromKnownBits(bits_ & ~static_cast<uint32_t>(capability));
    }

    friend constexpr bool operator==(Capabilities, Capabilities) noexcept =
        default;
    friend constexpr Capabilities operator|(Capabilities lhs,
                                             Capabilities rhs) noexcept {
        return FromKnownBits(lhs.bits_ | rhs.bits_);
    }
    friend constexpr Capabilities operator|(Capabilities lhs,
                                             Capability rhs) noexcept {
        return lhs.With(rhs);
    }
    friend constexpr Capabilities operator|(Capability lhs,
                                             Capability rhs) noexcept {
        return Capabilities(lhs).With(rhs);
    }
    friend constexpr Capabilities operator&(Capabilities lhs,
                                             Capabilities rhs) noexcept {
        return FromKnownBits(lhs.bits_ & rhs.bits_);
    }
    friend constexpr Capabilities operator~(Capabilities value) noexcept {
        return FromKnownBits(kKnownBits & ~value.bits_);
    }

    static constexpr uint32_t kKnownBits =
        static_cast<uint32_t>(Capability::kConnect) |
        static_cast<uint32_t>(Capability::kListen) |
        static_cast<uint32_t>(Capability::kZeroCopyWindow) |
        static_cast<uint32_t>(Capability::kMulticast) |
        static_cast<uint32_t>(Capability::kRemoteWrite) |
        static_cast<uint32_t>(Capability::kRemoteAcceptedConfirmation);

private:
    static constexpr Capabilities FromKnownBits(uint32_t bits) noexcept {
        Capabilities capabilities;
        capabilities.bits_ = bits & kKnownBits;
        return capabilities;
    }

    uint32_t bits_ = 0;
};

struct TransportCapabilities {
    TransportKind kind = TransportKind::kNetwork;
    TransportReliability reliability = TransportReliability::kUnreliable;
    // Zero denotes a streaming transport without a transport-level frame cap;
    // kMaxPayloadBytes remains the absolute Send() input bound.
    uint32_t max_frame_size = 0;
    // Zero means that the driver performs no reassembly.
    uint64_t max_reassembly_bytes = 0;
    Capabilities features;
};

Status ValidateTransportCapabilities(
    const TransportCapabilities& capabilities);

enum class EndpointAddressFamily : uint8_t {
    kUnspecified = 0,
    kIpv4 = 4,
    kIpv6 = 6,
};

enum class NetworkProtocol : uint8_t {
    kNone = 0,
    kTcp = 1,
    // An RDMA implementation may reuse an IP address for control-plane
    // connection establishment without claiming TCP semantics.
    kRdmaCompatible = 2,
    kUdp = 3,
};

// Owned, pointer-free endpoint value suitable for Registry metadata. It is not
// serialized by copying its object representation; use the explicit codec
// below. Unused bytes are zeroed by all factories.
class EndpointDescriptor {
public:
    static constexpr size_t kIpv4AddressBytes = 4;
    static constexpr size_t kIpv6AddressBytes = 16;
    static constexpr size_t kMaxFabricOpaqueBytes = 24;
    static constexpr size_t kMaxSerializedSize = 38;

    EndpointDescriptor() noexcept = default;

    static Result<EndpointDescriptor> Ipv4Tcp(
        std::span<const std::byte> address, uint16_t port);
    static Result<EndpointDescriptor> Ipv6Tcp(
        std::span<const std::byte> address, uint16_t port);
    static Result<EndpointDescriptor> Ipv4Udp(
        std::span<const std::byte> address, uint16_t port);
    static Result<EndpointDescriptor> Ipv6Udp(
        std::span<const std::byte> address, uint16_t port);
    static Result<EndpointDescriptor> Ip(
        TransportKind kind, EndpointAddressFamily family,
        NetworkProtocol protocol, std::span<const std::byte> address,
        uint16_t port);
    static Result<EndpointDescriptor> SharedFabric(
        uint32_t fabric_domain, uint32_t channel_id,
        std::span<const std::byte> opaque = {});

    TransportKind kind() const noexcept { return kind_; }
    EndpointAddressFamily address_family() const noexcept { return family_; }
    NetworkProtocol protocol() const noexcept { return protocol_; }
    uint16_t port() const noexcept { return port_; }
    uint32_t fabric_domain() const noexcept { return fabric_domain_; }
    uint32_t channel_id() const noexcept { return channel_id_; }
    std::span<const std::byte> ip_address() const noexcept;
    std::span<const std::byte> fabric_opaque() const noexcept;

    friend bool operator==(const EndpointDescriptor&,
                           const EndpointDescriptor&) noexcept = default;

private:
    TransportKind kind_ = TransportKind::kNetwork;
    EndpointAddressFamily family_ = EndpointAddressFamily::kUnspecified;
    NetworkProtocol protocol_ = NetworkProtocol::kNone;
    uint8_t opaque_size_ = 0;
    uint16_t port_ = 0;
    uint16_t reserved_ = 0;
    uint32_t fabric_domain_ = 0;
    uint32_t channel_id_ = 0;
    std::array<std::byte, kMaxFabricOpaqueBytes> data_{};
};

static_assert(sizeof(EndpointDescriptor) <= 40,
              "Registry endpoint slot must remain bounded");
static_assert(std::is_standard_layout_v<EndpointDescriptor>);
static_assert(std::is_trivially_copyable_v<EndpointDescriptor>);

Status ValidateEndpointDescriptor(const EndpointDescriptor& endpoint);

// Registry persistence format, version 1:
//   magic(0x4d), version(1), kind, form, protocol, form-specific payload.
// Integers are big-endian. The encoding is minimal and variable length; Parse
// rejects unknown kinds/protocols, oversized input, mismatched discriminants,
// truncation, and trailing non-canonical bytes.
Result<size_t> SerializeEndpointDescriptor(
    const EndpointDescriptor& endpoint, std::span<std::byte> output);
Result<EndpointDescriptor> ParseEndpointDescriptor(
    std::span<const std::byte> input);

struct DriverConfig {
    uint32_t max_connections = 1'024;
    uint32_t max_listeners = 64;
    uint32_t max_queued_sends = 4'096;
};

struct ConnectionInfo {
    ConnectionId id = kInvalidConnectionId;
    TransportKind kind = TransportKind::kNetwork;
    bool is_listener = false;
    std::optional<EndpointDescriptor> local_endpoint;
    std::optional<EndpointDescriptor> peer_endpoint;
};

struct ConnectRequest {
    EndpointDescriptor remote_endpoint;
    std::optional<EndpointDescriptor> local_bind;
    // Zero requests a non-blocking attempt. Values above the public maximum are
    // rejected with kInvalidArgument.
    uint32_t timeout_ms = 0;
};

struct ListenRequest {
    EndpointDescriptor local_endpoint;
    uint32_t backlog = 1;
};

struct AcceptRequest {
    ConnectionId listener_id = kInvalidConnectionId;
    // Zero is non-blocking. A positive timeout returns kTimeout when it expires.
    uint32_t timeout_ms = 0;
};

struct SendRequest {
    ConnectionId connection_id = kInvalidConnectionId;
    // Borrowed only for the duration of Send(); implementations must not retain
    // this span. Empty payloads are rejected.
    std::span<const std::byte> payload;
    DeliveryStage target_stage = DeliveryStage::kRemoteAccepted;
};

enum class UntrackedTrafficClass : uint8_t {
    // ACK/Schema/Session traffic that must retain an independent progress
    // reserve. This remains the default for source compatibility.
    kProtocolControl = 0,
    // Best-effort application data: charged to normal data quota without
    // creating a SendOperation or DeliveryCompletion.
    kData = 1,
};

struct UntrackedSendRequest {
    ConnectionId connection_id = kInvalidConnectionId;
    // Success is local bounded admission only; the payload never creates an
    // operation or completion.
    std::span<const std::byte> payload;
    UntrackedTrafficClass traffic_class =
        UntrackedTrafficClass::kProtocolControl;
};

using OperationId = uint64_t;
inline constexpr OperationId kInvalidOperationId = 0;

struct SendOperation {
    OperationId id = kInvalidOperationId;
    ConnectionId connection_id = kInvalidConnectionId;

    friend constexpr bool operator==(SendOperation, SendOperation) = default;
};

struct SendResult {
    // A successful result only admits the complete message into the driver's
    // bounded queue/window. It does not report any delivery stage. The ticket
    // remains outstanding until PollCompletions() reports it.
    SendOperation operation;
    size_t admitted_bytes = 0;
};

struct DeliveryCompletion {
    // Must exactly match the ticket returned by Send(). A successful completion
    // is emitted only after the peer ACK reaches the requested target_stage.
    SendOperation operation;
    DeliveryStage reached_stage = DeliveryStage::kLocalPublished;
    Status status = Status::Ok();
};

struct CompletionPollRequest {
    uint32_t max_completions = 1;
    // Zero is non-blocking. A positive timeout returns kTimeout when it expires.
    uint32_t timeout_ms = 0;
    // Invalid means any connection. A specific ID must not consume completion
    // events belonging to other connections.
    ConnectionId connection_id = kInvalidConnectionId;
};

struct CompletionPollResult {
    // On success this is non-empty and bounded by max_completions.
    std::vector<DeliveryCompletion> completions;
};

struct ReceivedMessage {
    ConnectionId connection_id = kInvalidConnectionId;
    EndpointDescriptor from;
    std::vector<std::byte> payload;
};

struct ReceiveRequest {
    uint32_t max_messages = 1;
    size_t max_bytes = 1u << 20;
    // Zero is non-blocking. Poll returns kWouldBlock when no message is ready;
    // a positive timeout returns kTimeout when it expires.
    uint32_t timeout_ms = 0;
    // Invalid means any connection. A specific ID must not consume messages
    // belonging to other connections.
    ConnectionId connection_id = kInvalidConnectionId;
};

struct ReceiveResult {
    // On success this is non-empty, has at most request.max_messages entries,
    // and aggregate payload bytes do not exceed request.max_bytes.
    std::vector<ReceivedMessage> messages;
};

Status ValidateDriverConfig(const DriverConfig& config);
Status ValidateConnectionInfo(const ConnectionInfo& info);
Status ValidateConnectRequest(const ConnectRequest& request,
                              const TransportCapabilities& capabilities);
Status ValidateListenRequest(const ListenRequest& request,
                             const TransportCapabilities& capabilities);
Status ValidateAcceptRequest(const AcceptRequest& request);
Status ValidateSendRequest(const SendRequest& request,
                           const TransportCapabilities& capabilities);
Status ValidateUntrackedSendRequest(
    const UntrackedSendRequest& request,
    const TransportCapabilities& capabilities);
Status ValidateReceiveRequest(const ReceiveRequest& request);
Status ValidateCompletionPollRequest(const CompletionPollRequest& request);
Status ValidateSendResult(const SendRequest& request,
                          const SendOperation& expected_operation,
                          const SendResult& result);
Status ValidateReceiveResult(const ReceiveRequest& request,
                             const ReceiveResult& result);
Status ValidateCompletionPollResult(const CompletionPollRequest& request,
                                    const CompletionPollResult& result);

enum class DriverState : uint8_t {
    kStopped = 0,
    kRunning = 1,
    kStopping = 2,
};

enum class HealthState : uint8_t {
    kHealthy = 0,
    kDegraded = 1,
    kUnavailable = 2,
};

// Status contract for implementations:
// - malformed local input: kInvalidArgument; a declared API bound exceeded:
//   kResourceExhausted; unsupported kind/feature: kUnsupported;
// - operation before Start()/during shutdown: kUnavailable; duplicate Start or
//   listener: kAlreadyExists; unknown connection: kNotFound;
// - bounded queue/window full: kWouldBlock; elapsed positive timeout: kTimeout;
// - peer/transport failure: kUnavailable; damaged received metadata: kCorruption.
// Shutdown() and Close() of an already closed object should be idempotent OK.
class TransportDriver {
public:
    virtual ~TransportDriver() = default;

    Status Start(const DriverConfig& config);
    Status Shutdown();
    Result<ConnectionInfo> Connect(const ConnectRequest& request);
    Result<ConnectionInfo> Listen(const ListenRequest& request);
    Result<ConnectionInfo> Accept(const AcceptRequest& request);
    Result<SendResult> Send(const SendRequest& request);
    Result<size_t> SendUntracked(const UntrackedSendRequest& request);
    // Additive owned admission APIs. On success payload is consumed (left
    // empty); on every failure it remains unchanged. Implementations may retain
    // the vector's allocation after admission instead of copying its bytes.
    Result<SendResult> TrySendOwned(
        ConnectionId connection_id, std::vector<std::byte>&& payload,
        DeliveryStage target_stage = DeliveryStage::kRemoteAccepted);
    Result<size_t> TrySendUntrackedOwned(
        ConnectionId connection_id, std::vector<std::byte>&& payload,
        UntrackedTrafficClass traffic_class =
            UntrackedTrafficClass::kProtocolControl);
    // Called only after Bridge protocol validation proves peer acceptance.
    // The resulting successful completion remains observable through
    // PollCompletions().
    Status ConfirmRemoteAccepted(SendOperation operation);
    Result<ReceiveResult> Poll(const ReceiveRequest& request);
    Result<CompletionPollResult> PollCompletions(
        const CompletionPollRequest& request);
    // Returns only a post-handshake, certificate-verified principal. Plaintext
    // drivers report kUnsupported; an in-progress TLS handshake reports
    // kWouldBlock without exposing provisional certificate data.
    Result<security::AuthenticatedPeer> AuthenticatedPeer(
        ConnectionId connection_id);
    Status Close(ConnectionId connection_id);

    DriverState state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }
    virtual HealthState health() const noexcept = 0;
    virtual TransportCapabilities capabilities() const noexcept = 0;

protected:
    virtual Status DoStart(const DriverConfig& config) = 0;
    // Called immediately after the base transitions to kStopping and before it
    // waits for active operations. Implementations must wake blocking waits.
    virtual void DoRequestStop() noexcept;
    virtual Status DoShutdown() = 0;
    virtual Result<ConnectionInfo> DoConnect(const ConnectRequest& request) = 0;
    virtual Result<ConnectionInfo> DoListen(const ListenRequest& request) = 0;
    virtual Result<ConnectionInfo> DoAccept(const AcceptRequest& request);
    // The base-assigned operation must be returned unchanged on admission and
    // retained by the driver until its ACK completion is polled.
    virtual Result<SendResult> DoSend(const SendRequest& request,
                                      SendOperation operation) = 0;
    virtual Result<size_t> DoSendUntracked(
        const UntrackedSendRequest& request);
    // The default owned implementations use the borrowed hooks above and only
    // consume payload after valid admission. Fast-path implementations must
    // preserve the same no-consume-on-failure contract.
    virtual Result<SendResult> DoTrySendOwned(
        const SendRequest& request, std::vector<std::byte>&& payload,
        SendOperation operation);
    virtual Result<size_t> DoTrySendUntrackedOwned(
        const UntrackedSendRequest& request,
        std::vector<std::byte>&& payload);
    virtual Status DoConfirmRemoteAccepted(SendOperation operation);
    virtual Result<ReceiveResult> DoPoll(const ReceiveRequest& request) = 0;
    virtual Result<CompletionPollResult> DoPollCompletions(
        const CompletionPollRequest& request) = 0;
    virtual Result<security::AuthenticatedPeer> DoAuthenticatedPeer(
        ConnectionId connection_id);
    virtual Status DoClose(ConnectionId connection_id) = 0;

private:
    class ActiveOperation {
    public:
        explicit ActiveOperation(TransportDriver* driver) noexcept
            : driver_(driver) {}
        ActiveOperation(const ActiveOperation&) = delete;
        ActiveOperation& operator=(const ActiveOperation&) = delete;
        ActiveOperation(ActiveOperation&& other) noexcept
            : driver_(other.driver_) {
            other.driver_ = nullptr;
        }
        ActiveOperation& operator=(ActiveOperation&&) = delete;
        ~ActiveOperation();

    private:
        TransportDriver* driver_;
    };

    struct OutstandingSend {
        ConnectionId connection_id = kInvalidConnectionId;
        DeliveryStage target_stage = DeliveryStage::kRemoteAccepted;
    };

    Result<ActiveOperation> AcquireActiveOperation();
    void ReleaseActiveOperation() noexcept;
    Result<SendOperation> ReserveSendOperation(const SendRequest& request);
    void CancelSendOperation(OperationId operation_id) noexcept;
    Status ValidateAndRetireCompletions(
        const CompletionPollResult& result);

    std::atomic<DriverState> state_{DriverState::kStopped};
    mutable std::mutex lifecycle_mutex_;
    std::condition_variable lifecycle_cv_;
    bool start_in_progress_ = false;
    uint64_t active_operations_ = 0;
    uint32_t max_outstanding_sends_ = 0;
    OperationId next_operation_id_ = 1;
    std::unordered_map<OperationId, OutstandingSend> outstanding_sends_;
};

}  // namespace mino::transport

#endif  // MINO_TRANSPORT_TRANSPORT_DRIVER_H_
