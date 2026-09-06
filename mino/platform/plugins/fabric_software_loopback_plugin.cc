// Copyright 2026 The Mino Authors
//
// Reference Fabric device plugin (software shared-window loopback).
// Reports FabricProviderClass::kDevice for the production ABI loader.
// Provenance is "mino-fabric-software-reference-loopback/v1;NOT-QUALIFICATION-ELIGIBLE".
// Kind is selected from device_name: ipcf*, ntb*, cxl* (default ntb).

#include "mino/platform/fabric_provider.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mino::platform {
namespace fabric_software_loopback {
namespace {

constexpr const char* kProvenance =
    "mino-fabric-software-reference-loopback/v1;NOT-QUALIFICATION-ELIGIBLE";
constexpr size_t kWindowBytes = 64u * 1024u;

FabricKind KindFromName(std::string_view name) {
    if (name.rfind("ipcf", 0) == 0 || name.rfind("IPCF", 0) == 0) {
        return FabricKind::kIpcf;
    }
    if (name.rfind("cxl", 0) == 0 || name.rfind("CXL", 0) == 0) {
        return FabricKind::kCxl;
    }
    return FabricKind::kNtb;
}

}  // namespace

class SoftwareLoopbackFabricProvider final : public FabricDeviceProvider {
public:
    explicit SoftwareLoopbackFabricProvider(std::string device_name)
        : device_name_(std::move(device_name)),
          kind_(KindFromName(device_name_)) {
        storage_.assign(kWindowBytes, std::byte{0});
    }

    FabricProviderCapabilities capabilities() const noexcept override {
        return {
            .provider_class = FabricProviderClass::kDevice,
            .kind = kind_,
            .device_present = true,
            .link_active = true,
            .cache_coherent = true,
            .cache_line_bytes = 64,
            .required_alignment = 8,
            .max_connections = 8,
            .max_listeners = 4,
            .max_windows_per_connection = 1,
            .max_window_bytes = storage_.size(),
        };
    }
    std::string provenance() const override { return kProvenance; }
    std::string device_id() const override {
        return "software-" + device_name_;
    }

    Status Start(const FabricProviderLimits&) override {
        std::lock_guard lock(mutex_);
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
        events_.clear();
        in_use_ = false;
        return Status::Ok();
    }

    Result<FabricProviderConnection> Connect(
        const transport::ConnectRequest& request) override {
        std::lock_guard lock(mutex_);
        if (!started_) {
            return Status::Error(StatusCode::kUnavailable);
        }
        return MakeConnection(request.remote_endpoint);
    }

    Result<FabricProviderListener> Listen(
        const transport::ListenRequest& request) override {
        std::lock_guard lock(mutex_);
        if (!started_) {
            return Status::Error(StatusCode::kUnavailable);
        }
        return FabricProviderListener{.id = next_listener_++,
                                      .local_endpoint = request.local_endpoint};
    }

    Result<FabricProviderConnection> Accept(FabricProviderListenerId,
                                            uint32_t) override {
        std::lock_guard lock(mutex_);
        auto endpoint = transport::EndpointDescriptor::SharedFabric(7, 3);
        if (!endpoint.ok()) return endpoint.status();
        return MakeConnection(*endpoint);
    }

    Result<FabricTransmitWindow> AcquireTransmitWindow(
        FabricProviderConnectionId connection_id,
        size_t minimum_bytes) override {
        std::lock_guard lock(mutex_);
        if (connection_id != connection_id_ || in_use_) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "software fabric window busy");
        }
        if (minimum_bytes > storage_.size()) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        in_use_ = true;
        return FabricTransmitWindow{
            .connection_id = connection_id,
            .window_id = window_id_,
            .window_generation = generation_,
            .session_epoch = session_epoch_,
            .consumer_sequence = consumer_sequence_,
            .bytes = storage_,
        };
    }

    Status AbortTransmitWindow(const FabricTransmitWindow&) noexcept override {
        std::lock_guard lock(mutex_);
        in_use_ = false;
        return Status::Ok();
    }

    Status MaintainCache(const FabricCacheRequest& request) override {
        std::lock_guard lock(mutex_);
        if (request.offset > storage_.size() ||
            request.bytes > storage_.size() - request.offset) {
            return Status::Error(StatusCode::kInvalidArgument);
        }
        return Status::Ok();
    }

    Status RingDoorbell(const FabricDoorbell& doorbell) override {
        std::lock_guard lock(mutex_);
        if (doorbell.kind == FabricDoorbellKind::kProducerCommit) {
            events_.push_back(FabricProviderEvent{
                .kind = FabricProviderEventKind::kReceiveReady,
                .mailbox_protocol_version = doorbell.protocol_version,
                .mailbox_endian_marker = doorbell.endian_marker,
                .connection_id = doorbell.connection_id,
                .window_id = doorbell.window_id,
                .window_generation = doorbell.window_generation,
                .session_epoch = doorbell.session_epoch,
                .producer_sequence = doorbell.producer_sequence,
                .window = storage_,
            });
        } else {
            ++consumer_sequence_;
            in_use_ = false;
            events_.push_back(FabricProviderEvent{
                .kind = FabricProviderEventKind::kTransmitConsumed,
                .mailbox_protocol_version = doorbell.protocol_version,
                .mailbox_endian_marker = doorbell.endian_marker,
                .connection_id = doorbell.connection_id,
                .window_id = doorbell.window_id,
                .window_generation = doorbell.window_generation,
                .session_epoch = doorbell.session_epoch,
                .producer_sequence = doorbell.producer_sequence,
                .window = {},
            });
        }
        return Status::Ok();
    }

    Result<FabricProviderPollResult> Poll(
        const FabricProviderPollRequest& request) override {
        std::lock_guard lock(mutex_);
        if (stop_requested_) {
            return Status::Error(StatusCode::kUnavailable);
        }
        FabricProviderPollResult result;
        uint32_t receives = 0;
        uint32_t controls = 0;
        for (auto it = events_.begin(); it != events_.end();) {
            const bool receive =
                it->kind == FabricProviderEventKind::kReceiveReady;
            if ((receive && receives >= request.max_receive_events) ||
                (!receive && controls >= request.max_control_events)) {
                ++it;
                continue;
            }
            if (receive) {
                ++receives;
            } else {
                ++controls;
            }
            result.events.push_back(*it);
            it = events_.erase(it);
        }
        return result;
    }

    Status ReleaseReceiveWindow(const FabricProviderEvent&) noexcept override {
        return Status::Ok();
    }

    Status Close(FabricProviderConnectionId) noexcept override {
        std::lock_guard lock(mutex_);
        events_.clear();
        in_use_ = false;
        return Status::Ok();
    }

private:
    Result<FabricProviderConnection> MakeConnection(
        const transport::EndpointDescriptor& peer) {
        auto local = transport::EndpointDescriptor::SharedFabric(7, 4);
        if (!local.ok()) return local.status();
        connection_id_ = 11;
        return FabricProviderConnection{
            .id = connection_id_,
            .local_endpoint = *local,
            .peer_endpoint = peer,
            .peer_node_id = NodeId{202},
            .peer_security_domain = SecurityDomainId{88},
            .peer_device_id = "peer-software-" + device_name_,
            .window_set_id = 19,
            .window_generation = generation_,
            .session_epoch = session_epoch_,
            .attestation_evidence = {std::byte{0xa5}, std::byte{0x5a}},
        };
    }

    std::string device_name_;
    FabricKind kind_;
    mutable std::mutex mutex_;
    bool started_ = false;
    bool stop_requested_ = false;
    bool in_use_ = false;
    uint64_t next_listener_ = 1;
    uint64_t connection_id_ = 11;
    uint64_t window_id_ = 41;
    uint64_t generation_ = 1;
    uint64_t session_epoch_ = 1;
    uint64_t consumer_sequence_ = 0;
    std::vector<std::byte> storage_;
    std::deque<FabricProviderEvent> events_;
};

const char* ProvenanceText() noexcept { return kProvenance; }

FabricDeviceProvider* Create(const char* device_name) {
    std::string name =
        (device_name == nullptr || device_name[0] == '\0') ? "ntb0" : device_name;
    return new SoftwareLoopbackFabricProvider(std::move(name));
}

}  // namespace fabric_software_loopback
}  // namespace mino::platform

extern "C" {

__attribute__((visibility("default"))) uint32_t
mino_fabric_provider_abi_version_v1() {
    return mino::platform::kMinoFabricProviderAbiVersion;
}

__attribute__((visibility("default"))) mino::platform::FabricDeviceProvider*
mino_create_fabric_provider_v1(const char* device_name) {
    try {
        return mino::platform::fabric_software_loopback::Create(device_name);
    } catch (...) {
        return nullptr;
    }
}

__attribute__((visibility("default"))) void mino_destroy_fabric_provider_v1(
    mino::platform::FabricDeviceProvider* provider) {
    delete provider;
}

__attribute__((visibility("default"))) const char*
mino_fabric_provider_provenance_v1() {
    return mino::platform::fabric_software_loopback::ProvenanceText();
}

}  // extern "C"
