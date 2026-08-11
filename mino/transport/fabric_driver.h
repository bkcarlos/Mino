// Copyright 2026 The Mino Authors

#ifndef MINO_TRANSPORT_FABRIC_DRIVER_H_
#define MINO_TRANSPORT_FABRIC_DRIVER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

#include "mino/platform/fabric_provider.h"
#include "mino/transport/transport_driver.h"

namespace mino::transport {

inline constexpr uint32_t kFabricWindowMagic = 0x4d465731u;  // "MFW1"
inline constexpr uint16_t kFabricWindowProtocolVersion = 1;
inline constexpr uint32_t kFabricWindowEndianMarker = 0x01020304u;
inline constexpr size_t kFabricWindowHeaderBytes = 128;
inline constexpr size_t kFabricWindowCommitMarkerOffset = 72;

struct FabricAttestation {
    NodeId local_node_id;
    SecurityDomainId local_security_domain;
    NodeId peer_node_id;
    SecurityDomainId peer_security_domain;
    EndpointDescriptor local_endpoint;
    EndpointDescriptor peer_endpoint;
    platform::FabricKind kind = platform::FabricKind::kIpcf;
    std::string provider_provenance;
    std::string local_device_id;
    std::string peer_device_id;
    uint64_t window_set_id = 0;
    uint64_t window_generation = 0;
    uint64_t session_epoch = 0;
    std::span<const std::byte> evidence;
};

class FabricAttestationVerifier {
public:
    virtual ~FabricAttestationVerifier() = default;
    virtual Result<security::AuthenticatedPeer> Verify(
        const FabricAttestation& attestation) const noexcept = 0;
};

struct FabricDriverOptions {
    std::shared_ptr<platform::FabricDeviceProvider> provider;
    std::shared_ptr<const FabricAttestationVerifier> attestation_verifier;
    NodeId local_node_id;
    SecurityDomainId local_security_domain;
    uint32_t max_windows_per_connection = 64;
    uint32_t event_queue_depth = 256;
    uint32_t receive_queue_depth = 256;
    uint32_t completion_queue_depth = 256;
    size_t max_message_bytes = 16u * 1024u * 1024u;
    size_t max_queued_receive_bytes = 64u * 1024u * 1024u;
    // Test targets may inject kMock explicitly. Production RemoteBridge rejects it.
    bool allow_mock_provider_for_testing = false;
};

Status ValidateFabricDriverOptions(const FabricDriverOptions& options,
                                   bool production = false) noexcept;

struct FabricDriverStats {
    size_t active_connections = 0;
    size_t listeners = 0;
    size_t outstanding_windows = 0;
    size_t queued_receive_messages = 0;
    size_t queued_receive_bytes = 0;
    size_t queued_completions = 0;
    uint64_t committed_windows = 0;
    uint64_t consumed_windows = 0;
    uint64_t cache_maintenance_failures = 0;
    uint64_t doorbell_failures = 0;
    uint64_t crc_failures = 0;
    uint64_t partial_commits = 0;
    uint64_t stale_window_events = 0;
    uint64_t peer_resets = 0;
};

// Complete TransportDriver implementation for shared-window IPCF/NTB/CXL.
// Only validated Canonical Wire frame bodies cross the Trust Domain. Native SHM
// offsets, handles, pointers, and object representations are never provider ABI.
class FabricWindowDriver final : public TransportDriver {
public:
    static Result<std::unique_ptr<FabricWindowDriver>> Create(
        FabricDriverOptions options) noexcept;
    ~FabricWindowDriver() override;

    HealthState health() const noexcept override {
        return health_.load(std::memory_order_acquire);
    }
    TransportCapabilities capabilities() const noexcept override;
    FabricDriverStats stats() const noexcept;

protected:
    Status DoStart(const DriverConfig& config) override;
    void DoRequestStop() noexcept override;
    Status DoShutdown() override;
    Result<ConnectionInfo> DoConnect(const ConnectRequest& request) override;
    Result<ConnectionInfo> DoListen(const ListenRequest& request) override;
    Result<ConnectionInfo> DoAccept(const AcceptRequest& request) override;
    Result<SendResult> DoSend(const SendRequest& request,
                              SendOperation operation) override;
    Result<size_t> DoSendUntracked(
        const UntrackedSendRequest& request) override;
    Status DoConfirmRemoteAccepted(SendOperation operation) override;
    Result<ReceiveResult> DoPoll(const ReceiveRequest& request) override;
    Result<CompletionPollResult> DoPollCompletions(
        const CompletionPollRequest& request) override;
    Result<security::AuthenticatedPeer> DoAuthenticatedPeer(
        ConnectionId connection_id) override;
    Status DoClose(ConnectionId connection_id) override;

private:
    class Impl;
    explicit FabricWindowDriver(FabricDriverOptions options) noexcept;

    FabricDriverOptions options_;
    std::atomic<HealthState> health_{HealthState::kUnavailable};
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::transport

#endif  // MINO_TRANSPORT_FABRIC_DRIVER_H_
