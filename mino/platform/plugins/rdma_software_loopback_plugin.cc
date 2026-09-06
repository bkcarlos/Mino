// Copyright 2026 The Mino Authors
//
// Reference RDMA device plugin (software loopback).
//
// Exports ABI v1 symbols required by CreateDynamicRdmaDeviceProvider.
// Reports MemoryRegistrationProviderClass::kDevice so the production loader
// accepts the ABI, but provenance is explicitly
// "mino-rdma-software-reference-loopback/v1;NOT-QUALIFICATION-ELIGIBLE".
// This is for CI dlopen / MemoryRegistration wiring — never V-25 hardware
// qualification.

#include "mino/platform/rdma_provider.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mino::platform {
namespace rdma_software_loopback {
namespace {

constexpr const char* kProvenance =
    "mino-rdma-software-reference-loopback/v1;NOT-QUALIFICATION-ELIGIBLE";

transport::EndpointDescriptor RdmaEndpoint(uint8_t host, uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{10}, std::byte{0}, std::byte{0}, static_cast<std::byte>(host)};
    auto endpoint = transport::EndpointDescriptor::Ip(
        transport::TransportKind::kRdma,
        transport::EndpointAddressFamily::kIpv4,
        transport::NetworkProtocol::kRdmaCompatible, address, port);
    return endpoint.ok() ? *endpoint : transport::EndpointDescriptor{};
}

security::AuthenticatedPeer MakePeer(uint64_t node) {
    security::AuthenticatedPeer peer;
    peer.node_id = NodeId{node};
    peer.security_domain = SecurityDomainId{7};
    peer.certificate_sha256[0] = std::byte{0x51};
    peer.credential_generation = 1;
    return peer;
}

}  // namespace

class SoftwareLoopbackRdmaProvider final : public RdmaDeviceProvider {
public:
    explicit SoftwareLoopbackRdmaProvider(std::string device_name)
        : device_name_(std::move(device_name)) {}

    MemoryRegistrationProviderClass provider_class() const noexcept override {
        return MemoryRegistrationProviderClass::kDevice;
    }
    std::string name() const override {
        return "mino-rdma-software-loopback:" + device_name_;
    }
    std::string provenance() const override { return kProvenance; }
    bool Supports(MemoryRegistrationKind kind) const noexcept override {
        return kind == MemoryRegistrationKind::kRdma ||
               kind == MemoryRegistrationKind::kDma;
    }

    Result<RegisteredMemory> Register(
        const MemoryRegistrationRequest& request) override {
        std::lock_guard lock(mutex_);
        if (!started_ || request.address == nullptr || request.bytes == 0 ||
            !request.owner.valid() || !Supports(request.kind)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "software RDMA MR request is invalid");
        }
        RegisteredMemory registration{
            .registration_id = next_registration_++,
            .bytes = request.bytes,
            .device_key = 0x50170000ull + next_registration_,
            .kind = request.kind,
            .owner = request.owner,
            .physically_contiguous = false,
        };
        registrations_.emplace(registration.registration_id, registration);
        return registration;
    }

    Status Deregister(const RegisteredMemory& registration) override {
        std::lock_guard lock(mutex_);
        registrations_.erase(registration.registration_id);
        return Status::Ok();
    }

    Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest& request) override {
        std::lock_guard lock(mutex_);
        MemoryRegistrationRecoveryResult result;
        for (auto it = registrations_.begin(); it != registrations_.end();) {
            const auto& owner = it->second.owner;
            if (owner.process_id != request.current_process_id ||
                owner.process_epoch != request.current_process_epoch) {
                result.registrations_released++;
                result.bytes_released += it->second.bytes;
                it = registrations_.erase(it);
            } else {
                ++it;
            }
        }
        return result;
    }

    Status Start(const RdmaProviderLimits& limits) override {
        std::lock_guard lock(mutex_);
        limits_ = limits;
        started_ = true;
        stop_requested_ = false;
        return Status::Ok();
    }

    void RequestStop() noexcept override {
        std::lock_guard lock(mutex_);
        stop_requested_ = true;
    }

    Status Shutdown() noexcept override {
        std::lock_guard lock(mutex_);
        started_ = false;
        completions_.clear();
        receives_.clear();
        registrations_.clear();
        pending_accepts_.clear();
        return Status::Ok();
    }

    Result<RdmaProviderConnection> Connect(
        const transport::ConnectRequest& request) override {
        std::lock_guard lock(mutex_);
        if (!started_) {
            return Status::Error(StatusCode::kUnavailable,
                                 "software RDMA provider not started");
        }
        const auto local =
            request.local_bind.value_or(RdmaEndpoint(1, 18000));
        RdmaProviderConnection connection{
            .id = next_connection_++,
            .local_endpoint = local,
            .peer_endpoint = request.remote_endpoint,
            .verified_peer = MakePeer(2),
        };
        pending_accepts_.push_back(RdmaProviderConnection{
            .id = next_connection_++,
            .local_endpoint = request.remote_endpoint,
            .peer_endpoint = local,
            .verified_peer = MakePeer(1),
        });
        return connection;
    }

    Result<RdmaProviderListener> Listen(
        const transport::ListenRequest& request) override {
        std::lock_guard lock(mutex_);
        if (!started_) {
            return Status::Error(StatusCode::kUnavailable);
        }
        return RdmaProviderListener{.id = next_listener_++,
                                    .local_endpoint = request.local_endpoint};
    }

    Result<RdmaProviderConnection> Accept(RdmaProviderConnectionId,
                                          uint32_t timeout_ms) override {
        std::lock_guard lock(mutex_);
        if (pending_accepts_.empty()) {
            return Status::Error(timeout_ms == 0 ? StatusCode::kWouldBlock
                                                 : StatusCode::kTimeout);
        }
        auto connection = std::move(pending_accepts_.front());
        pending_accepts_.pop_front();
        return connection;
    }

    Status PostSend(const RdmaProviderSendRequest& request) override {
        std::lock_guard lock(mutex_);
        if (!registrations_.contains(request.registration.registration_id)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "WR registration is unknown");
        }
        if (limits_.max_message_bytes != 0 &&
            request.canonical_wire.size() > limits_.max_message_bytes) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        receives_.push_back(RdmaProviderReceive{
            .connection_id = request.connection_id,
            .peer_endpoint = RdmaEndpoint(2, 19000),
            .canonical_wire =
                std::vector<std::byte>(request.canonical_wire.begin(),
                                       request.canonical_wire.end()),
        });
        completions_.push_back(RdmaProviderCompletion{
            .work_request_id = request.work_request_id,
            .connection_id = request.connection_id,
            .bytes_completed = request.canonical_wire.size(),
            .terminal = true,
        });
        return Status::Ok();
    }

    Result<RdmaProviderPollResult> Poll(
        const RdmaProviderPollRequest& request) override {
        std::lock_guard lock(mutex_);
        if (stop_requested_) {
            return Status::Error(StatusCode::kUnavailable,
                                 "software RDMA provider stopped");
        }
        RdmaProviderPollResult result;
        while (!completions_.empty() &&
               result.completions.size() < request.max_completions) {
            result.completions.push_back(std::move(completions_.front()));
            completions_.pop_front();
        }
        size_t bytes = 0;
        while (!receives_.empty() &&
               result.receives.size() < request.max_receives) {
            const size_t next = receives_.front().canonical_wire.size();
            if (request.max_receive_bytes != 0 &&
                bytes + next > request.max_receive_bytes) {
                break;
            }
            bytes += next;
            result.receives.push_back(std::move(receives_.front()));
            receives_.pop_front();
        }
        if (result.completions.empty() && result.receives.empty()) {
            return Status::Error(request.timeout_ms == 0
                                     ? StatusCode::kWouldBlock
                                     : StatusCode::kTimeout);
        }
        return result;
    }

    Status Close(RdmaProviderConnectionId) noexcept override {
        return Status::Ok();
    }

private:
    std::string device_name_;
    mutable std::mutex mutex_;
    bool started_ = false;
    bool stop_requested_ = false;
    RdmaProviderLimits limits_{};
    uint64_t next_registration_ = 1;
    uint64_t next_connection_ = 1;
    uint64_t next_listener_ = 1;
    std::map<uint64_t, RegisteredMemory> registrations_;
    std::deque<RdmaProviderCompletion> completions_;
    std::deque<RdmaProviderReceive> receives_;
    std::deque<RdmaProviderConnection> pending_accepts_;
};

const char* ProvenanceText() noexcept { return kProvenance; }

RdmaDeviceProvider* Create(const char* device_name) {
    std::string name =
        (device_name == nullptr || device_name[0] == '\0') ? "loopback0"
                                                          : device_name;
    return new SoftwareLoopbackRdmaProvider(std::move(name));
}

}  // namespace rdma_software_loopback
}  // namespace mino::platform

extern "C" {

__attribute__((visibility("default"))) uint32_t
mino_rdma_provider_abi_version_v1() {
    return mino::platform::kMinoRdmaProviderAbiVersion;
}

__attribute__((visibility("default"))) mino::platform::RdmaDeviceProvider*
mino_create_rdma_provider_v1(const char* device_name) {
    try {
        return mino::platform::rdma_software_loopback::Create(device_name);
    } catch (...) {
        return nullptr;
    }
}

__attribute__((visibility("default"))) void mino_destroy_rdma_provider_v1(
    mino::platform::RdmaDeviceProvider* provider) {
    delete provider;
}

__attribute__((visibility("default"))) const char*
mino_rdma_provider_provenance_v1() {
    return mino::platform::rdma_software_loopback::ProvenanceText();
}

}  // extern "C"
