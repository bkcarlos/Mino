// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_TRANSPORT_IPSEC_TRANSPORT_H_
#define MINO_TRANSPORT_IPSEC_TRANSPORT_H_

#include <atomic>
#include <memory>
#include <optional>
#include <vector>

#include "mino/common/result.h"
#include "mino/security/ipsec.h"
#include "mino/transport/transport_driver.h"

namespace mino::transport {

// How IpsecTransportDriver enforces kernel IPsec coverage.
enum class IpsecEnforcement : uint8_t {
    // Default. Connect/Listen/Accept/Send fail-closed unless IpsecSaProbe
    // reports a covering SA (and optional policy) for the peer endpoint.
    kRequireKernelSa = 0,
    // Operator asserts the socket path is already IPsec-protected (external
    // XFRM/strongSwan/VTI). The driver still binds to an ordinary TCP/UDP
    // TransportDriver; it does NOT skip probe when a probe is supplied — pass
    // a no-op ScriptedIpsecSaProbe::AllowAll only in tests. Production
    // compositions should prefer kRequireKernelSa with NetlinkXfrmSaProbe.
    kAssumedProtected = 1,
};

struct IpsecTransportOptions {
    // Required inner driver (typically TcpDriver or UdpDriver). Ownership
    // transfers to IpsecTransportDriver.
    std::unique_ptr<TransportDriver> inner;
    // Required for kRequireKernelSa. Optional for kAssumedProtected (when
    // omitted, assumed mode admits traffic without an SA check — document
    // operator setup; prefer supplying NetlinkXfrmSaProbe anyway).
    std::shared_ptr<security::IpsecSaProbe> probe;
    IpsecEnforcement enforcement = IpsecEnforcement::kRequireKernelSa;
    // When true, Listen/Accept also require an inbound SA covering the local
    // endpoint once the peer is known (Accept) or the listen address (Listen
    // with require_inbound_on_listen).
    bool require_inbound_on_listen = false;
};

Status ValidateIpsecTransportOptions(const IpsecTransportOptions& options);

// TransportDriver wrapper for architecture §14.2 IPsec option.
//
// Mino does not implement userspace ESP. This driver:
//   1. Guards Connect/Listen/Accept/Send with IpsecSaProbe (fail-closed when
//      SA missing under kRequireKernelSa);
//   2. Forwards all I/O to an owned inner TransportDriver that speaks plaintext
//      to the kernel; the kernel XFRM stack applies ESP when SAs/policies exist.
//
// Operator setup (see docs/operations/ipsec.md): install transport-mode or
// tunnel-mode SAs/policies covering the Bridge endpoints before Start, or run
// strongSwan/Libreswan and point NetlinkXfrmSaProbe at the resulting SAs.
class IpsecTransportDriver final : public TransportDriver {
public:
    static Result<std::unique_ptr<IpsecTransportDriver>> Create(
        IpsecTransportOptions options);

    ~IpsecTransportDriver() override;

    HealthState health() const noexcept override;
    TransportCapabilities capabilities() const noexcept override;
    IpsecEnforcement enforcement() const noexcept { return enforcement_; }

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
    Result<SendResult> DoTrySendOwned(
        const SendRequest& request, std::vector<std::byte>&& payload,
        SendOperation operation) override;
    Result<size_t> DoTrySendUntrackedOwned(
        const UntrackedSendRequest& request,
        std::vector<std::byte>&& payload) override;
    Status DoConfirmRemoteAccepted(SendOperation operation) override;
    Result<ReceiveResult> DoPoll(const ReceiveRequest& request) override;
    Result<CompletionPollResult> DoPollCompletions(
        const CompletionPollRequest& request) override;
    Result<security::AuthenticatedPeer> DoAuthenticatedPeer(
        ConnectionId connection_id) override;
    Status DoClose(ConnectionId connection_id) override;

private:
    explicit IpsecTransportDriver(IpsecTransportOptions options);

    Status RequireForEndpoint(
        const EndpointDescriptor& peer,
        const std::optional<EndpointDescriptor>& local,
        bool require_inbound) const;
    Result<security::IpsecEndpoint> ToIpsecEndpoint(
        const EndpointDescriptor& endpoint) const;

    std::unique_ptr<TransportDriver> inner_;
    std::shared_ptr<security::IpsecSaProbe> probe_;
    IpsecEnforcement enforcement_ = IpsecEnforcement::kRequireKernelSa;
    bool require_inbound_on_listen_ = false;
};

}  // namespace mino::transport

#endif  // MINO_TRANSPORT_IPSEC_TRANSPORT_H_
