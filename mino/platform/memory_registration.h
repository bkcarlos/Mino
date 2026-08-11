// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_PLATFORM_MEMORY_REGISTRATION_H_
#define MINO_PLATFORM_MEMORY_REGISTRATION_H_

#include <cstdint>
#include <string>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {

enum class MemoryRegistrationKind : uint8_t {
    kDma = 1,
    kRdma = 2,
};

enum class MemoryRegistrationProviderClass : uint8_t {
    kUnavailable = 0,
    kMock = 1,
    kDevice = 2,
};

struct MemoryRegistrationOwner {
    uint64_t process_id = 0;
    uint64_t process_epoch = 0;
    uint64_t lease_id = 0;

    bool valid() const noexcept {
        return process_id != 0 && process_epoch != 0 && lease_id != 0;
    }
    friend bool operator==(const MemoryRegistrationOwner&,
                           const MemoryRegistrationOwner&) = default;
};

struct MemoryRegistrationRequest {
    void* address = nullptr;
    uint64_t bytes = 0;
    uint64_t alignment = 1;
    uint64_t scope_id = 0;
    MemoryRegistrationKind kind = MemoryRegistrationKind::kDma;
    MemoryRegistrationOwner owner;
    bool require_physical_contiguous = false;
};

// Provider-owned opaque registration. Device keys are intentionally opaque to
// the allocator; an RDMA driver may consume them later without the allocator
// depending on a verbs implementation.
struct RegisteredMemory {
    uint64_t registration_id = 0;
    uint64_t bytes = 0;
    uint64_t device_key = 0;
    MemoryRegistrationKind kind = MemoryRegistrationKind::kDma;
    MemoryRegistrationOwner owner;
    bool physically_contiguous = false;
};

struct MemoryRegistrationRecoveryRequest {
    uint64_t scope_id = 0;
    uint64_t current_process_id = 0;
    uint64_t current_process_epoch = 0;
};

struct MemoryRegistrationRecoveryResult {
    uint64_t registrations_released = 0;
    uint64_t bytes_released = 0;
};

// Injection boundary for DMA/RDMA memory registration. The native default is
// deliberately unavailable: Mino never reports successful registration unless
// a device or an explicitly injected mock provider performed it.
class MemoryRegistrationProvider {
public:
    virtual ~MemoryRegistrationProvider() = default;

    virtual MemoryRegistrationProviderClass provider_class() const noexcept = 0;
    virtual std::string name() const = 0;
    virtual bool Supports(MemoryRegistrationKind kind) const noexcept = 0;
    virtual Result<RegisteredMemory> Register(
        const MemoryRegistrationRequest& request) = 0;
    virtual Status Deregister(const RegisteredMemory& registration) = 0;

    // Removes registrations in scope_id owned by a process incarnation other
    // than (current_process_id, current_process_epoch). Providers must make this
    // operation idempotent so attach/recovery can retry after a crash.
    virtual Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest& request) = 0;
};

MemoryRegistrationProvider& UnavailableMemoryRegistrationProvider() noexcept;

}  // namespace mino

#endif  // MINO_PLATFORM_MEMORY_REGISTRATION_H_
