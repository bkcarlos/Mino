// Copyright 2026 The Mino Authors
//
// Reference RDMA verbs plugin. When <infiniband/verbs.h> is present at compile
// time, Create opens the named ibv device and registers MRs via ibv_reg_mr.
// Without verbs headers / usable device, create returns nullptr (loader maps
// that to kUnavailable). Not a substitute for V-25 NIC qualification.

#include "mino/platform/rdma_provider.h"

#include <cstdint>
#include <map>
#include <mutex>
#include <new>
#include <string>
#include <utility>

#if defined(__has_include)
#if __has_include(<infiniband/verbs.h>)
#define MINO_HAVE_IBVERBS 1
#include <infiniband/verbs.h>
#endif
#endif

namespace mino::platform {
namespace rdma_verbs_plugin {

constexpr const char* kProvenance =
    "mino-rdma-verbs-reference/v1;requires-libibverbs;NOT-AUTO-QUALIFIED";

#if defined(MINO_HAVE_IBVERBS)

class VerbsRdmaProvider final : public RdmaDeviceProvider {
public:
    VerbsRdmaProvider(std::string device_name, ibv_context* context, ibv_pd* pd)
        : device_name_(std::move(device_name)), context_(context), pd_(pd) {}

    ~VerbsRdmaProvider() override {
        std::lock_guard lock(mutex_);
        for (auto& entry : mrs_) {
            if (entry.second != nullptr) ibv_dereg_mr(entry.second);
        }
        mrs_.clear();
        if (pd_ != nullptr) ibv_dealloc_pd(pd_);
        if (context_ != nullptr) ibv_close_device(context_);
    }

    MemoryRegistrationProviderClass provider_class() const noexcept override {
        return MemoryRegistrationProviderClass::kDevice;
    }
    std::string name() const override {
        return "mino-rdma-verbs:" + device_name_;
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
            !request.owner.valid() || pd_ == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument);
        }
        ibv_mr* mr = ibv_reg_mr(
            pd_, request.address, request.bytes,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_READ);
        if (mr == nullptr) {
            return Status::Error(StatusCode::kUnavailable, "ibv_reg_mr failed");
        }
        const uint64_t id = next_registration_++;
        mrs_.emplace(id, mr);
        return RegisteredMemory{
            .registration_id = id,
            .bytes = request.bytes,
            .device_key = mr->rkey,
            .kind = request.kind,
            .owner = request.owner,
            .physically_contiguous = false,
        };
    }

    Status Deregister(const RegisteredMemory& registration) override {
        std::lock_guard lock(mutex_);
        auto it = mrs_.find(registration.registration_id);
        if (it == mrs_.end()) return Status::Ok();
        if (ibv_dereg_mr(it->second) != 0) {
            return Status::Error(StatusCode::kUnavailable,
                                 "ibv_dereg_mr failed");
        }
        mrs_.erase(it);
        return Status::Ok();
    }

    Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest&) override {
        return MemoryRegistrationRecoveryResult{};
    }

    Status Start(const RdmaProviderLimits&) override {
        std::lock_guard lock(mutex_);
        started_ = true;
        return Status::Ok();
    }
    void RequestStop() noexcept override {
        std::lock_guard lock(mutex_);
        stop_requested_ = true;
    }
    Status Shutdown() noexcept override {
        std::lock_guard lock(mutex_);
        started_ = false;
        for (auto& entry : mrs_) {
            if (entry.second != nullptr) ibv_dereg_mr(entry.second);
        }
        mrs_.clear();
        return Status::Ok();
    }

    Result<RdmaProviderConnection> Connect(
        const transport::ConnectRequest&) override {
        return Status::Error(
            StatusCode::kUnsupported,
            "verbs reference plugin MR path only; QP/CM not bundled");
    }
    Result<RdmaProviderListener> Listen(
        const transport::ListenRequest&) override {
        return Status::Error(
            StatusCode::kUnsupported,
            "verbs reference plugin MR path only; QP/CM not bundled");
    }
    Result<RdmaProviderConnection> Accept(RdmaProviderConnectionId,
                                          uint32_t) override {
        return Status::Error(StatusCode::kUnsupported);
    }
    Status PostSend(const RdmaProviderSendRequest&) override {
        return Status::Error(StatusCode::kUnsupported);
    }
    Result<RdmaProviderPollResult> Poll(
        const RdmaProviderPollRequest&) override {
        return Status::Error(StatusCode::kUnsupported);
    }
    Status Close(RdmaProviderConnectionId) noexcept override {
        return Status::Ok();
    }

private:
    std::string device_name_;
    ibv_context* context_ = nullptr;
    ibv_pd* pd_ = nullptr;
    mutable std::mutex mutex_;
    bool started_ = false;
    bool stop_requested_ = false;
    uint64_t next_registration_ = 1;
    std::map<uint64_t, ibv_mr*> mrs_;
};

RdmaDeviceProvider* Create(const char* device_name) {
    if (device_name == nullptr || device_name[0] == '\0') return nullptr;
    int num = 0;
    ibv_device** list = ibv_get_device_list(&num);
    if (list == nullptr || num <= 0) {
        if (list != nullptr) ibv_free_device_list(list);
        return nullptr;
    }
    ibv_device* selected = nullptr;
    for (int i = 0; i < num; ++i) {
        if (list[i] != nullptr && list[i]->name != nullptr &&
            device_name == std::string(list[i]->name)) {
            selected = list[i];
            break;
        }
    }
    if (selected == nullptr) {
        ibv_free_device_list(list);
        return nullptr;
    }
    ibv_context* context = ibv_open_device(selected);
    ibv_free_device_list(list);
    if (context == nullptr) return nullptr;
    ibv_pd* pd = ibv_alloc_pd(context);
    if (pd == nullptr) {
        ibv_close_device(context);
        return nullptr;
    }
    try {
        return new VerbsRdmaProvider(device_name, context, pd);
    } catch (...) {
        ibv_dealloc_pd(pd);
        ibv_close_device(context);
        return nullptr;
    }
}

#else  // !MINO_HAVE_IBVERBS

RdmaDeviceProvider* Create(const char*) { return nullptr; }

#endif

const char* ProvenanceText() noexcept { return kProvenance; }

}  // namespace rdma_verbs_plugin
}  // namespace mino::platform

extern "C" {

__attribute__((visibility("default"))) uint32_t
mino_rdma_provider_abi_version_v1() {
    return mino::platform::kMinoRdmaProviderAbiVersion;
}

__attribute__((visibility("default"))) mino::platform::RdmaDeviceProvider*
mino_create_rdma_provider_v1(const char* device_name) {
    try {
        return mino::platform::rdma_verbs_plugin::Create(device_name);
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
    return mino::platform::rdma_verbs_plugin::ProvenanceText();
}

}  // extern "C"
