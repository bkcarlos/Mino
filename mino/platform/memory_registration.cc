// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/platform/memory_registration.h"

namespace mino {
namespace {

class UnavailableProvider final : public MemoryRegistrationProvider {
public:
    MemoryRegistrationProviderClass provider_class() const noexcept override {
        return MemoryRegistrationProviderClass::kUnavailable;
    }

    std::string name() const override { return "unavailable"; }

    bool Supports(MemoryRegistrationKind) const noexcept override {
        return false;
    }

    Result<RegisteredMemory> Register(
        const MemoryRegistrationRequest&) override {
        return Status::Error(
            StatusCode::kUnsupported,
            "no DMA/RDMA memory registration provider is installed");
    }

    Status Deregister(const RegisteredMemory&) override {
        return Status::Error(
            StatusCode::kUnsupported,
            "no DMA/RDMA memory registration provider is installed");
    }

    Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest&) override {
        return Status::Error(
            StatusCode::kUnsupported,
            "no DMA/RDMA memory registration provider is installed");
    }
};

}  // namespace

MemoryRegistrationProvider& UnavailableMemoryRegistrationProvider() noexcept {
    static UnavailableProvider provider;
    return provider;
}

}  // namespace mino
