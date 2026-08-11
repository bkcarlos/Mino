// Copyright 2026 The Mino Authors

#include "mino/platform/fabric_provider.h"

#include <dlfcn.h>

#include <bit>
#include <new>
#include <utility>

namespace mino::platform {
namespace {

using AbiVersionFunction = uint32_t (*)();
using CreateFunction = FabricDeviceProvider* (*)(const char*);
using DestroyFunction = void (*)(FabricDeviceProvider*);
using ProvenanceFunction = const char* (*)();

Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status Unsupported(const char* message) {
    return Status::Error(StatusCode::kUnsupported, message);
}

bool KnownKind(FabricKind kind) noexcept {
    return kind == FabricKind::kIpcf || kind == FabricKind::kNtb ||
           kind == FabricKind::kCxl;
}

class DynamicProvider final : public FabricDeviceProvider {
public:
    DynamicProvider(void* library, FabricDeviceProvider* implementation,
                    DestroyFunction destroy, std::string provenance) noexcept
        : library_(library),
          implementation_(implementation),
          destroy_(destroy),
          provenance_(std::move(provenance)) {}

    ~DynamicProvider() override {
        if (implementation_ != nullptr) destroy_(implementation_);
        if (library_ != nullptr) dlclose(library_);
    }

    FabricProviderCapabilities capabilities() const noexcept override {
        return implementation_->capabilities();
    }
    std::string provenance() const override { return provenance_; }
    std::string device_id() const override { return implementation_->device_id(); }
    Status Start(const FabricProviderLimits& limits) override {
        return implementation_->Start(limits);
    }
    void RequestStop() noexcept override { implementation_->RequestStop(); }
    Status Shutdown() noexcept override { return implementation_->Shutdown(); }
    Result<FabricProviderConnection> Connect(
        const transport::ConnectRequest& request) override {
        return implementation_->Connect(request);
    }
    Result<FabricProviderListener> Listen(
        const transport::ListenRequest& request) override {
        return implementation_->Listen(request);
    }
    Result<FabricProviderConnection> Accept(
        FabricProviderListenerId listener_id, uint32_t timeout_ms) override {
        return implementation_->Accept(listener_id, timeout_ms);
    }
    Result<FabricTransmitWindow> AcquireTransmitWindow(
        FabricProviderConnectionId connection_id,
        size_t minimum_bytes) override {
        return implementation_->AcquireTransmitWindow(connection_id,
                                                       minimum_bytes);
    }
    Status AbortTransmitWindow(
        const FabricTransmitWindow& window) noexcept override {
        return implementation_->AbortTransmitWindow(window);
    }
    Status MaintainCache(const FabricCacheRequest& request) override {
        return implementation_->MaintainCache(request);
    }
    Status RingDoorbell(const FabricDoorbell& doorbell) override {
        return implementation_->RingDoorbell(doorbell);
    }
    Result<FabricProviderPollResult> Poll(
        const FabricProviderPollRequest& request) override {
        return implementation_->Poll(request);
    }
    Status ReleaseReceiveWindow(
        const FabricProviderEvent& event) noexcept override {
        return implementation_->ReleaseReceiveWindow(event);
    }
    Status Close(FabricProviderConnectionId connection_id) noexcept override {
        return implementation_->Close(connection_id);
    }

private:
    void* library_ = nullptr;
    FabricDeviceProvider* implementation_ = nullptr;
    DestroyFunction destroy_ = nullptr;
    std::string provenance_;
};

}  // namespace

Status ValidateFabricProviderCapabilities(
    const FabricProviderCapabilities& capabilities, bool production) noexcept {
    if (!KnownKind(capabilities.kind)) {
        return Invalid("fabric provider kind is invalid");
    }
    if (capabilities.provider_class == FabricProviderClass::kUnavailable ||
        (production && capabilities.provider_class != FabricProviderClass::kDevice)) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "production fabric requires a device provider");
    }
    if (!capabilities.device_present || !capabilities.link_active) {
        return Status::Error(StatusCode::kUnavailable,
                             "fabric device or peer link is not active");
    }
    if (capabilities.cache_line_bytes == 0 ||
        !std::has_single_bit(capabilities.cache_line_bytes) ||
        capabilities.required_alignment == 0 ||
        !std::has_single_bit(capabilities.required_alignment) ||
        capabilities.max_connections == 0 ||
        capabilities.max_listeners == 0 ||
        capabilities.max_windows_per_connection == 0 ||
        capabilities.max_window_bytes == 0) {
        return Invalid("fabric provider bounds or alignment are invalid");
    }
    return Status::Ok();
}

Result<std::shared_ptr<FabricDeviceProvider>> CreateDynamicFabricDeviceProvider(
    const DynamicFabricProviderOptions& options) noexcept {
    if (options.plugin_path.empty() || options.plugin_path.front() != '/') {
        return Invalid("fabric provider plugin path must be absolute");
    }
    if (options.device_name.empty() || !KnownKind(options.expected_kind)) {
        return Invalid("fabric provider device or expected kind is invalid");
    }
    void* library = dlopen(options.plugin_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (library == nullptr) {
        return Unsupported("configured fabric device plugin could not be loaded");
    }
    const auto abi = reinterpret_cast<AbiVersionFunction>(
        dlsym(library, "mino_fabric_provider_abi_version_v1"));
    const auto create = reinterpret_cast<CreateFunction>(
        dlsym(library, "mino_create_fabric_provider_v1"));
    const auto destroy = reinterpret_cast<DestroyFunction>(
        dlsym(library, "mino_destroy_fabric_provider_v1"));
    const auto provenance = reinterpret_cast<ProvenanceFunction>(
        dlsym(library, "mino_fabric_provider_provenance_v1"));
    if (abi == nullptr || create == nullptr || destroy == nullptr ||
        provenance == nullptr || abi() != kMinoFabricProviderAbiVersion) {
        dlclose(library);
        return Unsupported("fabric provider ABI is missing or incompatible");
    }
    const char* provenance_text = provenance();
    if (provenance_text == nullptr || *provenance_text == '\0') {
        dlclose(library);
        return Status::Error(StatusCode::kPermissionDenied,
                             "fabric provider provenance is empty");
    }
    FabricDeviceProvider* implementation = create(options.device_name.c_str());
    if (implementation == nullptr) {
        dlclose(library);
        return Status::Error(StatusCode::kUnavailable,
                             "fabric provider could not open the device");
    }
    const FabricProviderCapabilities capabilities =
        implementation->capabilities();
    const Status valid =
        ValidateFabricProviderCapabilities(capabilities, /*production=*/true);
    if (!valid.ok() || capabilities.kind != options.expected_kind ||
        implementation->device_id().empty()) {
        destroy(implementation);
        dlclose(library);
        return !valid.ok()
                   ? valid
                   : Status::Error(StatusCode::kPermissionDenied,
                                   "fabric provider identity or kind mismatch");
    }
    try {
        return std::shared_ptr<FabricDeviceProvider>(new DynamicProvider(
            library, implementation, destroy, provenance_text));
    } catch (const std::bad_alloc&) {
        destroy(implementation);
        dlclose(library);
        return Status::Error(StatusCode::kResourceExhausted,
                             "fabric provider wrapper allocation failed");
    }
}

}  // namespace mino::platform
