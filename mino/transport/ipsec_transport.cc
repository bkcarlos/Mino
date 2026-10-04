// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/transport/ipsec_transport.h"

#include <algorithm>
#include <array>
#include <new>
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
    if (options.protected_socket_recheck_ms > kMaxOperationTimeoutMs) {
        return Status::Error(StatusCode::kInvalidArgument, "IPsec recheck interval is too large");
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
      require_inbound_on_listen_(options.require_inbound_on_listen) {
    const auto policy = inner_->MandatoryIpsecSocketPolicy();
    if (policy && policy->outbound_spi != 0 && policy->inbound_spi != 0) {
        protected_socket_recheck_ms_ = options.protected_socket_recheck_ms;
    }
}

IpsecTransportDriver::~IpsecTransportDriver() { StopVerifier(); }

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
    selector.require_out_policy = true;
    selector.socket_policy = inner_->MandatoryIpsecSocketPolicy();
    if (local.has_value()) {
        MINO_ASSIGN_OR_RETURN(auto local_ep, ToIpsecEndpoint(*local));
        selector.local = local_ep;
    }
    return probe_->RequireProtection(selector);
}

Status IpsecTransportDriver::DoStart(const DriverConfig& config) {
    MINO_RETURN_IF_ERROR(inner_->Start(config));
    {
        std::lock_guard lock(connections_mutex_);
        max_connections_ = config.max_connections;
        verifier_stopping_ = false;
    }
    if (protected_socket_recheck_ms_ != 0) {
        try {
            verifier_ = std::thread([this] { VerificationLoop(); });
        } catch (...) {
            StopVerifier();
            (void)inner_->Shutdown();
            return Status::Error(StatusCode::kResourceExhausted,
                                 "cannot start IPsec verifier");
        }
    }
    return Status::Ok();
}

void IpsecTransportDriver::DoRequestStop() noexcept {
    {
        std::lock_guard lock(connections_mutex_);
        verifier_stopping_ = true;
    }
    verification_cv_.notify_all();
    if (inner_ != nullptr) {
        inner_->DoRequestStop();
    }
}

Status IpsecTransportDriver::DoShutdown() {
    StopVerifier();
    const Status status = inner_->Shutdown();
    std::lock_guard lock(connections_mutex_);
    connections_.clear();
    return status;
}

Result<ConnectionInfo> IpsecTransportDriver::AdmitConnection(ConnectionInfo info) {
    if (!info.peer_endpoint.has_value() || !info.local_endpoint.has_value()) {
        (void)inner_->DoClose(info.id);
        return Status::Error(StatusCode::kUnavailable,
                             "IPsec connection endpoint identity is missing");
    }
    const auto verification_started = std::chrono::steady_clock::now();
    const Status status = RequireForEndpoint(*info.peer_endpoint,
                                             info.local_endpoint, true);
    if (!status.ok()) {
        (void)inner_->DoClose(info.id);
        return status;
    }
    std::lock_guard lock(connections_mutex_);
    if (!connections_.contains(info.id) && connections_.size() >= max_connections_) {
        (void)inner_->DoClose(info.id);
        return Status::Error(StatusCode::kResourceExhausted,
                             "IPsec connection tracking is full");
    }
    try {
        const auto now = std::chrono::steady_clock::now();
        const auto interval = std::chrono::milliseconds(protected_socket_recheck_ms_);
        connections_.insert_or_assign(info.id, AdmittedConnection{
            info, verification_started + interval, now + interval / 2,
            ++next_generation_, true});
        verification_cv_.notify_all();
    } catch (const std::bad_alloc&) {
        (void)inner_->DoClose(info.id);
        return Status::Error(StatusCode::kResourceExhausted);
    }
    return info;
}

Status IpsecTransportDriver::RequireForConnection(ConnectionId connection_id) {
    ConnectionInfo info;
    {
        std::lock_guard lock(connections_mutex_);
        const auto it = connections_.find(connection_id);
        if (verifier_stopping_ || it == connections_.end()) {
            return Status::Error(StatusCode::kUnavailable,
                                 "IPsec connection was not admitted or is stopping");
        }
        if (protected_socket_recheck_ms_ != 0) {
            if (!it->second.verified) {
                return Status::Error(StatusCode::kUnavailable,
                                     "IPsec background verification failed");
            }
            if (std::chrono::steady_clock::now() >= it->second.valid_until) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "IPsec background verification pending");
            }
            return Status::Ok();
        }
        info = it->second.info;
    }
    return RequireForEndpoint(*info.peer_endpoint, info.local_endpoint, true);
}

void IpsecTransportDriver::StopVerifier() noexcept {
    {
        std::lock_guard lock(connections_mutex_);
        verifier_stopping_ = true;
    }
    verification_cv_.notify_all();
    if (verifier_.joinable()) verifier_.join();
}

void IpsecTransportDriver::VerificationLoop() noexcept {
    const auto interval = std::chrono::milliseconds(protected_socket_recheck_ms_);
    // A minimum delay also prevents a busy loop for the 1 ms option or when a
    // slow/failed probe consumes the entire validity interval.
    const auto retry_delay = std::max(interval / 2, std::chrono::milliseconds(1));
    std::unique_lock lock(connections_mutex_);
    while (!verifier_stopping_) {
        auto selected = connections_.end();
        for (auto it = connections_.begin(); it != connections_.end(); ++it) {
            if (selected == connections_.end() ||
                it->second.refresh_at < selected->second.refresh_at) selected = it;
        }
        if (selected == connections_.end()) {
            verification_cv_.wait(lock);
            continue;
        }
        const auto started = std::chrono::steady_clock::now();
        if (started < selected->second.refresh_at) {
            const auto wake_at = selected->second.refresh_at;
            verification_cv_.wait_until(lock, wake_at);
            continue;
        }
        const auto info = selected->second.info;
        const auto generation = selected->second.generation;
        lock.unlock();
        bool verified = false;
        try {
            verified = RequireForEndpoint(*info.peer_endpoint, info.local_endpoint, true).ok();
        } catch (...) {
            // A throwing custom probe must fail closed without killing the worker.
        }
        lock.lock();
        const auto current = connections_.find(info.id);
        // Close/reconnect may reuse a driver's connection ID while the probe is
        // in flight. Its old result must never authorize the new connection.
        if (current != connections_.end() && current->second.generation == generation) {
            current->second.verified = verified;
            current->second.valid_until = started + interval;
            current->second.refresh_at = std::chrono::steady_clock::now() + retry_delay;
        }
    }
}

Result<ConnectionInfo> IpsecTransportDriver::DoConnect(
    const ConnectRequest& request) {
    MINO_RETURN_IF_ERROR(RequireForEndpoint(
        request.remote_endpoint, request.local_bind, /*require_inbound=*/false));
    MINO_ASSIGN_OR_RETURN(auto info, inner_->DoConnect(request));
    return AdmitConnection(std::move(info));
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
    return AdmitConnection(std::move(info));
}

Result<SendResult> IpsecTransportDriver::DoSend(const SendRequest& request,
                                                SendOperation operation) {
    MINO_RETURN_IF_ERROR(RequireForConnection(request.connection_id));
    return inner_->DoSend(request, operation);
}

Result<size_t> IpsecTransportDriver::DoSendUntracked(
    const UntrackedSendRequest& request) {
    MINO_RETURN_IF_ERROR(RequireForConnection(request.connection_id));
    return inner_->DoSendUntracked(request);
}

Result<SendResult> IpsecTransportDriver::DoTrySendOwned(
    const SendRequest& request, std::vector<std::byte>&& payload,
    SendOperation operation) {
    MINO_RETURN_IF_ERROR(RequireForConnection(request.connection_id));
    return inner_->DoTrySendOwned(request, std::move(payload), operation);
}

Result<size_t> IpsecTransportDriver::DoTrySendUntrackedOwned(
    const UntrackedSendRequest& request, std::vector<std::byte>&& payload) {
    MINO_RETURN_IF_ERROR(RequireForConnection(request.connection_id));
    return inner_->DoTrySendUntrackedOwned(request, std::move(payload));
}

Status IpsecTransportDriver::DoConfirmRemoteAccepted(SendOperation operation) {
    return inner_->DoConfirmRemoteAccepted(operation);
}

Result<ReceiveResult> IpsecTransportDriver::DoPoll(
    const ReceiveRequest& request) {
    MINO_RETURN_IF_ERROR(RequireForConnection(request.connection_id));
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
    {
        std::lock_guard lock(connections_mutex_);
        connections_.erase(connection_id);
        verification_cv_.notify_all();
    }
    return inner_->DoClose(connection_id);
}

}  // namespace mino::transport
