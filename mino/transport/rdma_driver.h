// Copyright 2026 The Mino Authors

#ifndef MINO_TRANSPORT_RDMA_DRIVER_H_
#define MINO_TRANSPORT_RDMA_DRIVER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "mino/common/result.h"
#include "mino/platform/rdma_provider.h"
#include "mino/transport/transport_driver.h"

namespace mino::transport {

enum class RdmaAuthenticationMode : uint8_t {
    // RDMA CM/device provider supplies a cryptographically verified identity.
    kVerifiedPeer = 0,
    // Identity is verified by a deployment-specific, fail-closed fabric
    // attestor. This is for physically controlled fabrics without CM identity.
    kControlledFabric = 1,
};

class RdmaControlledFabricVerifier {
public:
    virtual ~RdmaControlledFabricVerifier() = default;
    virtual Result<security::AuthenticatedPeer> Verify(
        const EndpointDescriptor& local_endpoint,
        const EndpointDescriptor& peer_endpoint,
        std::string_view provider_provenance) const noexcept = 0;
};

struct RdmaDriverOptions {
    std::shared_ptr<platform::RdmaDeviceProvider> provider;
    uint32_t send_queue_depth = 256;
    uint32_t receive_queue_depth = 256;
    uint32_t completion_queue_depth = 512;
    size_t max_message_bytes = 16u * 1024u * 1024u;
    size_t max_queued_send_bytes = 64u * 1024u * 1024u;
    size_t max_queued_receive_bytes = 64u * 1024u * 1024u;
    uint64_t registration_quota_bytes = 256u * 1024u * 1024u;
    uint64_t registration_scope_id = 0;
    MemoryRegistrationOwner registration_owner;
    RdmaAuthenticationMode authentication_mode =
        RdmaAuthenticationMode::kVerifiedPeer;
    std::shared_ptr<const RdmaControlledFabricVerifier>
        controlled_fabric_verifier;
    // Test targets may explicitly inject kMock. Production RemoteBridge rejects
    // this flag and accepts only kDevice providers.
    bool allow_mock_provider_for_testing = false;
};

Status ValidateRdmaDriverOptions(const RdmaDriverOptions& options,
                                 bool production = false) noexcept;

struct RdmaDriverStats {
    size_t active_connections = 0;
    size_t listeners = 0;
    size_t outstanding_work_requests = 0;
    size_t queued_send_bytes = 0;
    size_t queued_receive_messages = 0;
    size_t queued_receive_bytes = 0;
    size_t queued_completions = 0;
    uint64_t registered_bytes = 0;
    uint64_t partial_completions = 0;
    uint64_t cq_errors = 0;
    uint64_t device_resets = 0;
    uint64_t peer_deaths = 0;
    uint64_t registration_failures = 0;
    uint64_t stale_registrations_recovered = 0;
    uint64_t stale_registration_bytes_recovered = 0;
};

// Reliable, message-oriented RDMA transport. Send/Poll carry complete Canonical
// Wire payloads only. A verbs CQ success is local publication, never remote
// acceptance; Bridge calls ConfirmRemoteAccepted only after its validated ACK.
class RdmaDriver final : public TransportDriver {
public:
    static Result<std::unique_ptr<RdmaDriver>> Create(
        RdmaDriverOptions options) noexcept;
    ~RdmaDriver() override;

    HealthState health() const noexcept override {
        return health_.load(std::memory_order_acquire);
    }
    TransportCapabilities capabilities() const noexcept override;
    RdmaDriverStats stats() const noexcept;
    MemoryRegistrationProvider& registration_provider() noexcept;

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
    explicit RdmaDriver(RdmaDriverOptions options) noexcept;

    RdmaDriverOptions options_;
    std::atomic<HealthState> health_{HealthState::kUnavailable};
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::transport

#endif  // MINO_TRANSPORT_RDMA_DRIVER_H_
