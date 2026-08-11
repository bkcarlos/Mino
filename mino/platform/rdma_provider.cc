// Copyright 2026 The Mino Authors

#include "mino/platform/rdma_provider.h"

#include <dlfcn.h>

#include <new>
#include <string_view>
#include <utility>

namespace mino::platform {
namespace {

using AbiVersionFunction = uint32_t (*)();
using CreateFunction = RdmaDeviceProvider* (*)(const char*);
using DestroyFunction = void (*)(RdmaDeviceProvider*);
using ProvenanceFunction = const char* (*)();

class DynamicProvider final : public RdmaDeviceProvider {
public:
    DynamicProvider(void* library, RdmaDeviceProvider* implementation,
                    DestroyFunction destroy, std::string provenance) noexcept
        : library_(library),
          implementation_(implementation),
          destroy_(destroy),
          provenance_(std::move(provenance)) {}

    ~DynamicProvider() override {
        if (implementation_ != nullptr) destroy_(implementation_);
        if (library_ != nullptr) dlclose(library_);
    }

    MemoryRegistrationProviderClass provider_class() const noexcept override {
        return implementation_->provider_class();
    }
    std::string name() const override { return implementation_->name(); }
    std::string provenance() const override { return provenance_; }
    bool Supports(MemoryRegistrationKind kind) const noexcept override {
        return implementation_->Supports(kind);
    }
    Result<RegisteredMemory> Register(
        const MemoryRegistrationRequest& request) override {
        return implementation_->Register(request);
    }
    Status Deregister(const RegisteredMemory& registration) override {
        return implementation_->Deregister(registration);
    }
    Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest& request) override {
        return implementation_->RecoverStale(request);
    }
    Status Start(const RdmaProviderLimits& limits) override {
        return implementation_->Start(limits);
    }
    void RequestStop() noexcept override { implementation_->RequestStop(); }
    Status Shutdown() noexcept override { return implementation_->Shutdown(); }
    Result<RdmaProviderConnection> Connect(
        const transport::ConnectRequest& request) override {
        return implementation_->Connect(request);
    }
    Result<RdmaProviderListener> Listen(
        const transport::ListenRequest& request) override {
        return implementation_->Listen(request);
    }
    Result<RdmaProviderConnection> Accept(
        RdmaProviderConnectionId listener_id, uint32_t timeout_ms) override {
        return implementation_->Accept(listener_id, timeout_ms);
    }
    Status PostSend(const RdmaProviderSendRequest& request) override {
        return implementation_->PostSend(request);
    }
    Result<RdmaProviderPollResult> Poll(
        const RdmaProviderPollRequest& request) override {
        return implementation_->Poll(request);
    }
    Status Close(RdmaProviderConnectionId connection_id) noexcept override {
        return implementation_->Close(connection_id);
    }

private:
    void* library_ = nullptr;
    RdmaDeviceProvider* implementation_ = nullptr;
    DestroyFunction destroy_ = nullptr;
    std::string provenance_;
};

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status Unsupported(std::string_view message) {
    return Status::Error(StatusCode::kUnsupported, message);
}

}  // namespace

Result<std::shared_ptr<RdmaDeviceProvider>> CreateDynamicRdmaDeviceProvider(
    const DynamicRdmaProviderOptions& options) noexcept {
    if (options.plugin_path.empty() || options.plugin_path.front() != '/') {
        return Invalid("RDMA provider plugin path must be absolute");
    }
    void* library = dlopen(options.plugin_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (library == nullptr) {
        return Unsupported("configured RDMA device plugin could not be loaded");
    }

    const auto abi = reinterpret_cast<AbiVersionFunction>(
        dlsym(library, "mino_rdma_provider_abi_version_v1"));
    const auto create = reinterpret_cast<CreateFunction>(
        dlsym(library, "mino_create_rdma_provider_v1"));
    const auto destroy = reinterpret_cast<DestroyFunction>(
        dlsym(library, "mino_destroy_rdma_provider_v1"));
    const auto provenance = reinterpret_cast<ProvenanceFunction>(
        dlsym(library, "mino_rdma_provider_provenance_v1"));
    if (abi == nullptr || create == nullptr || destroy == nullptr ||
        provenance == nullptr || abi() != kMinoRdmaProviderAbiVersion) {
        dlclose(library);
        return Unsupported("RDMA device plugin ABI or provenance is invalid");
    }
    const char* provenance_text = provenance();
    if (provenance_text == nullptr || provenance_text[0] == '\0') {
        dlclose(library);
        return Unsupported("RDMA device plugin provenance is empty");
    }

    RdmaDeviceProvider* implementation = nullptr;
    try {
        implementation = create(options.device_name.c_str());
        if (implementation == nullptr) {
            dlclose(library);
            return Status::Error(StatusCode::kUnavailable,
                                 "RDMA device plugin found no usable device");
        }
        if (implementation->provider_class() !=
                MemoryRegistrationProviderClass::kDevice ||
            !implementation->Supports(MemoryRegistrationKind::kRdma)) {
            destroy(implementation);
            dlclose(library);
            return Unsupported(
                "production RDMA plugin is not a real RDMA device provider");
        }
        auto provider = std::make_shared<DynamicProvider>(
            library, implementation, destroy, std::string(provenance_text));
        return std::static_pointer_cast<RdmaDeviceProvider>(provider);
    } catch (const std::bad_alloc&) {
        if (implementation != nullptr) destroy(implementation);
        dlclose(library);
        return Status::Error(StatusCode::kResourceExhausted,
                             "RDMA provider allocation failed");
    } catch (...) {
        if (implementation != nullptr) destroy(implementation);
        dlclose(library);
        return Status::Error(StatusCode::kInternal,
                             "RDMA provider plugin threw during creation");
    }
}

}  // namespace mino::platform
