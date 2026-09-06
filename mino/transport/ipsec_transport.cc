// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/transport/ipsec_transport.h"

#include <algorithm>
#include <array>
#include <utility>

namespace mino::transport {
namespace {

security::IpsecProtocol ProtocolOf(const EndpointDescriptor& endpoint) {
    switch (endpoint.protocol()) {
        case NetworkProtocol::kTcp:
            return security::IpsecProtocol::kTcp;
        case NetworkProtocol::kUdp:
            return security::IpsecProtocol::kUdp;
        default:
            return security::IpsecProtocol::kAny;
    }
}

}  // namespace

Status ValidateIpsecTransportOptions(const IpsecTransportOptions& options) {
    if (options.inner == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "IpsecTransportOptions.inner is required");
    }
    if (options.enforcement == IpsecEnforcement::kRequireKernelSa &&
        options.probe == nullptr) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "IpsecTransportOptions.probe is required for kRequireKernelSa");
    }
    return Status::Ok();
}

Result<std::unique_ptr<IpsecTransportDriver>> IpsecTransportDriver::Create(
    IpsecTransportOptions options) {
    MINO_RETURN_IF_ERROR(ValidateIpsecTransportOptions(options));
    return std::unique_ptr<IpsecTransportDriver>(
        new IpsecTransportDriver(std::move(options)));
}

IpsecTransportDriver::IpsecTransportDriver(IpsecTransportOptions options)
    : inner_(std::move(options.inner)),
      probe_(std::move(options.probe)),
      enforcement_(options.enforcement),
      require_inbound_on_listen_(options.require_inbound_on_listen) {}

IpsecTransportDriver::~IpsecTransportDriver() = default;

HealthState IpsecTransportDriver::health() const noexcept {
    return inner_ != nullptr ? inner_->health() : HealthState::kUnavailable;
}

TransportCapabilities IpsecTransportDriver::capabilities() const noexcept {
    return inner_ != nullptr ? inner_->capabilities() : TransportCapabilities{};
}

Result<security::IpsecEndpoint> IpsecTransportDriver::ToIpsecEndpoint(
    const EndpointDescriptor& endpoint) const {
    if (endpoint.kind() != TransportKind::kNetwork) {
        return Status::Error(
            StatusCode::kUnsupported,
            "IpsecTransportDriver only guards network (IP) endpoints");
    }
    const auto address = endpoint.ip_address();
    if (endpoint.address_family() == EndpointAddressFamily::kIpv4) {
        if (address.size() != 4) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "IPv4 endpoint address size mismatch");
        }
        std::array<std::byte, 4> bytes{};
        std::copy(address.begin(), address.end(), bytes.begin());
        return security::MakeIpv4Endpoint(bytes, endpoint.port(),
                                          ProtocolOf(endpoint));
    }
    if (endpoint.address_family() == EndpointAddressFamily::kIpv6) {
        if (address.size() != 16) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "IPv6 endpoint address size mismatch");
        }
        security::IpsecEndpoint ipsec;
        ipsec.family = security::IpsecAddressFamily::kIpv6;
        ipsec.protocol = ProtocolOf(endpoint);
        ipsec.address_bytes = 16;
        ipsec.port = endpoint.port();
        std::copy(address.begin(), address.end(), ipsec.address.begin());
        return ipsec;
    }
    return Status::Error(StatusCode::kInvalidArgument,
                         "IpsecTransportDriver requires IPv4 or IPv6 endpoint");
}

Status IpsecTransportDriver::RequireForEndpoint(
    const EndpointDescriptor& peer,
    const std::optional<EndpointDescriptor>& local,
    bool require_inbound) const {
    if (enforcement_ == IpsecEnforcement::kAssumedProtected &&
        probe_ == nullptr) {
        // Explicit operator-assumed mode without a probe: admit. Prefer
        // supplying NetlinkXfrmSaProbe in production.
        return Status::Ok();
    }
    if (probe_ == nullptr) {
        return Status::Error(StatusCode::kUnavailable,
                             "IPsec SA probe missing (fail-closed)");
    }
    MINO_ASSIGN_OR_RETURN(auto peer_ep, ToIpsecEndpoint(peer));
    security::IpsecSaSelector selector;
    selector.peer = peer_ep;
    selector.require_inbound = require_inbound;
    if (local.has_value()) {
        MINO_ASSIGN_OR_RETURN(auto local_ep, ToIpsecEndpoint(*local));
        selector.local = local_ep;
    }
    return probe_->RequireProtection(selector);
}

Status IpsecTransportDriver::DoStart(const DriverConfig& config) {
    return inner_->Start(config);
}

void IpsecTransportDriver::DoRequestStop() noexcept {
    if (inner_ != nullptr) {
        inner_->DoRequestStop();
    }
}

Status IpsecTransportDriver::DoShutdown() { return inner_->Shutdown(); }

Result<ConnectionInfo> IpsecTransportDriver::DoConnect(
    const ConnectRequest& request) {
    MINO_RETURN_IF_ERROR(RequireForEndpoint(
        request.remote_endpoint, request.local_bind, /*require_inbound=*/false));
    return inner_->DoConnect(request);
}

Result<ConnectionInfo> IpsecTransportDriver::DoListen(
    const ListenRequest& request) {
    if (require_inbound_on_listen_) {
        // Before peers connect we can only assert a local-covering SA by using
        // the listen address as both peer and local (loopback / anycast setups
        // that install bidirectional SAs). Non-loopback deployments should
        // rely on Accept-time checks instead.
        MINO_RETURN_IF_ERROR(RequireForEndpoint(
            request.local_endpoint, request.local_endpoint,
            /*require_inbound=*/true));
    }
    return inner_->DoListen(request);
}

Result<ConnectionInfo> IpsecTransportDriver::DoAccept(
    const AcceptRequest& request) {
    MINO_ASSIGN_OR_RETURN(auto info, inner_->DoAccept(request));
    if (info.peer_endpoint.has_value()) {
        auto status = RequireForEndpoint(
            *info.peer_endpoint, info.local_endpoint, /*require_inbound=*/true);
        if (!status.ok()) {
            (void)inner_->DoClose(info.id);
            return status;
        }
    }
    return info;
}

Result<SendResult> IpsecTransportDriver::DoSend(const SendRequest& request,
                                                SendOperation operation) {
    return inner_->DoSend(request, operation);
}

Result<size_t> IpsecTransportDriver::DoSendUntracked(
    const UntrackedSendRequest& request) {
    return inner_->DoSendUntracked(request);
}

Result<SendResult> IpsecTransportDriver::DoTrySendOwned(
    const SendRequest& request, std::vector<std::byte>&& payload,
    SendOperation operation) {
    return inner_->DoTrySendOwned(request, std::move(payload), operation);
}

Result<size_t> IpsecTransportDriver::DoTrySendUntrackedOwned(
    const UntrackedSendRequest& request, std::vector<std::byte>&& payload) {
    return inner_->DoTrySendUntrackedOwned(request, std::move(payload));
}

Status IpsecTransportDriver::DoConfirmRemoteAccepted(SendOperation operation) {
    return inner_->DoConfirmRemoteAccepted(operation);
}

Result<ReceiveResult> IpsecTransportDriver::DoPoll(
    const ReceiveRequest& request) {
    return inner_->DoPoll(request);
}

Result<CompletionPollResult> IpsecTransportDriver::DoPollCompletions(
    const CompletionPollRequest& request) {
    return inner_->DoPollCompletions(request);
}

Result<security::AuthenticatedPeer> IpsecTransportDriver::DoAuthenticatedPeer(
    ConnectionId connection_id) {
    return inner_->DoAuthenticatedPeer(connection_id);
}

Status IpsecTransportDriver::DoClose(ConnectionId connection_id) {
    return inner_->DoClose(connection_id);
}

}  // namespace mino::transport
