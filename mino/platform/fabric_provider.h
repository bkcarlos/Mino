// Copyright 2026 The Mino Authors
//
// Device boundary for IPCF, PCIe NTB, and CXL shared-window transports.

#ifndef MINO_PLATFORM_FABRIC_PROVIDER_H_
#define MINO_PLATFORM_FABRIC_PROVIDER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/transport/transport_driver.h"

namespace mino::platform {

inline constexpr uint32_t kMinoFabricProviderAbiVersion = 1;
inline constexpr uint16_t kMinoFabricMailboxProtocolVersion = 1;
inline constexpr uint32_t kMinoFabricMailboxEndianMarker = 0x01020304u;
using FabricProviderConnectionId = uint64_t;
using FabricProviderListenerId = uint64_t;

// This classification is authoritative at production composition boundaries.
enum class FabricProviderClass : uint8_t {
    kUnavailable = 0,
    kDevice = 1,
    kMock = 2,
};

enum class FabricKind : uint8_t {
    kIpcf = 1,
    kNtb = 2,
    kCxl = 3,
};

struct FabricProviderCapabilities {
    FabricProviderClass provider_class = FabricProviderClass::kUnavailable;
    FabricKind kind = FabricKind::kIpcf;
    bool device_present = false;
    bool link_active = false;
    bool cache_coherent = false;
    uint32_t cache_line_bytes = 0;
    uint32_t required_alignment = 0;
    uint32_t max_connections = 0;
    uint32_t max_listeners = 0;
    uint32_t max_windows_per_connection = 0;
    size_t max_window_bytes = 0;
};

Status ValidateFabricProviderCapabilities(
    const FabricProviderCapabilities& capabilities,
    bool production = false) noexcept;

struct FabricProviderLimits {
    uint32_t max_connections = 0;
    uint32_t max_listeners = 0;
    uint32_t max_windows_per_connection = 0;
    uint32_t max_events_per_poll = 0;
    size_t max_window_bytes = 0;
};

// Immutable facts returned only after the provider completed its device-level
// link/window handshake. The attestation verifier binds all of these fields.
struct FabricProviderConnection {
    FabricProviderConnectionId id = 0;
    transport::EndpointDescriptor local_endpoint;
    transport::EndpointDescriptor peer_endpoint;
    NodeId peer_node_id;
    SecurityDomainId peer_security_domain;
    std::string peer_device_id;
    uint64_t window_set_id = 0;
    uint64_t window_generation = 0;
    uint64_t session_epoch = 0;
    std::vector<std::byte> attestation_evidence;
};

struct FabricProviderListener {
    FabricProviderListenerId id = 0;
    transport::EndpointDescriptor local_endpoint;
};

// Provider-owned writable lease. The span remains valid until a matching
// consumer event, AbortTransmitWindow(), Close(), reset, or Shutdown().
struct FabricTransmitWindow {
    FabricProviderConnectionId connection_id = 0;
    uint64_t window_id = 0;
    uint64_t window_generation = 0;
    uint64_t session_epoch = 0;
    uint64_t consumer_sequence = 0;
    std::span<std::byte> bytes;
};

enum class FabricCacheDirection : uint8_t {
    kForPeerRead = 0,
    kForCpuRead = 1,
};

struct FabricCacheRequest {
    FabricProviderConnectionId connection_id = 0;
    uint64_t window_id = 0;
    uint64_t window_generation = 0;
    uint64_t session_epoch = 0;
    FabricCacheDirection direction = FabricCacheDirection::kForPeerRead;
    size_t offset = 0;
    size_t bytes = 0;
};

enum class FabricDoorbellKind : uint8_t {
    kProducerCommit = 1,
    kConsumerRelease = 2,
};

struct FabricDoorbell {
    uint16_t protocol_version = kMinoFabricMailboxProtocolVersion;
    uint32_t endian_marker = kMinoFabricMailboxEndianMarker;
    FabricProviderConnectionId connection_id = 0;
    FabricDoorbellKind kind = FabricDoorbellKind::kProducerCommit;
    uint64_t window_id = 0;
    uint64_t window_generation = 0;
    uint64_t session_epoch = 0;
    uint64_t producer_sequence = 0;
};

enum class FabricProviderEventKind : uint8_t {
    kReceiveReady = 1,
    kTransmitConsumed = 2,
    kPeerReset = 3,
    kLinkError = 4,
};

struct FabricProviderEvent {
    FabricProviderEventKind kind = FabricProviderEventKind::kLinkError;
    uint16_t mailbox_protocol_version = kMinoFabricMailboxProtocolVersion;
    uint32_t mailbox_endian_marker = kMinoFabricMailboxEndianMarker;
    FabricProviderConnectionId connection_id = 0;
    uint64_t window_id = 0;
    uint64_t window_generation = 0;
    uint64_t session_epoch = 0;
    uint64_t producer_sequence = 0;
    // Present only for kReceiveReady. Valid until ReleaseReceiveWindow(), Close(),
    // reset, Shutdown(), or the next provider call documented by the plugin.
    std::span<const std::byte> window;
    Status status = Status::Ok();
};

struct FabricProviderPollRequest {
    uint32_t max_receive_events = 0;
    uint32_t max_control_events = 0;
    uint32_t timeout_ms = 0;
};

struct FabricProviderPollResult {
    std::vector<FabricProviderEvent> events;
};

// A real provider owns BAR/channel/pool mapping and interrupt integration. Mino
// never fabricates device presence, link state, cache maintenance, or doorbells.
class FabricDeviceProvider {
public:
    virtual ~FabricDeviceProvider() = default;

    virtual FabricProviderCapabilities capabilities() const noexcept = 0;
    virtual std::string provenance() const = 0;
    virtual std::string device_id() const = 0;
    virtual Status Start(const FabricProviderLimits& limits) = 0;
    virtual void RequestStop() noexcept = 0;
    virtual Status Shutdown() noexcept = 0;
    virtual Result<FabricProviderConnection> Connect(
        const transport::ConnectRequest& request) = 0;
    virtual Result<FabricProviderListener> Listen(
        const transport::ListenRequest& request) = 0;
    virtual Result<FabricProviderConnection> Accept(
        FabricProviderListenerId listener_id, uint32_t timeout_ms) = 0;
    virtual Result<FabricTransmitWindow> AcquireTransmitWindow(
        FabricProviderConnectionId connection_id, size_t minimum_bytes) = 0;
    virtual Status AbortTransmitWindow(
        const FabricTransmitWindow& window) noexcept = 0;
    // For non-coherent fabrics this performs the authoritative clean/invalidate.
    // Coherent providers must still validate the request and return success.
    virtual Status MaintainCache(const FabricCacheRequest& request) = 0;
    virtual Status RingDoorbell(const FabricDoorbell& doorbell) = 0;
    virtual Result<FabricProviderPollResult> Poll(
        const FabricProviderPollRequest& request) = 0;
    virtual Status ReleaseReceiveWindow(
        const FabricProviderEvent& event) noexcept = 0;
    // Synchronously masks interrupts/doorbells and fences device access before
    // invalidating all views for this connection.
    virtual Status Close(FabricProviderConnectionId connection_id) noexcept = 0;
};

struct DynamicFabricProviderOptions {
    // Absolute path only; host loader search paths are never consulted.
    std::string plugin_path;
    std::string device_name;
    FabricKind expected_kind = FabricKind::kIpcf;
};

// Required plugin exports:
//   mino_fabric_provider_abi_version_v1() -> uint32_t
//   mino_create_fabric_provider_v1(const char*) -> FabricDeviceProvider*
//   mino_destroy_fabric_provider_v1(FabricDeviceProvider*)
//   mino_fabric_provider_provenance_v1() -> const char*
Result<std::shared_ptr<FabricDeviceProvider>> CreateDynamicFabricDeviceProvider(
    const DynamicFabricProviderOptions& options) noexcept;

}  // namespace mino::platform

#endif  // MINO_PLATFORM_FABRIC_PROVIDER_H_
