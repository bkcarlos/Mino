// Copyright 2026 The Mino Authors
//
// Low-level RDMA device boundary. The built-in tree intentionally contains no
// software provider that can be selected by production assembly.

#ifndef MINO_PLATFORM_RDMA_PROVIDER_H_
#define MINO_PLATFORM_RDMA_PROVIDER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/memory_registration.h"
#include "mino/security/tls.h"
#include "mino/transport/transport_driver.h"

namespace mino::platform {

inline constexpr uint32_t kMinoRdmaProviderAbiVersion = 1;
using RdmaProviderConnectionId = uint64_t;
using RdmaWorkRequestId = uint64_t;

struct RdmaProviderLimits {
    uint32_t max_connections = 0;
    uint32_t max_listeners = 0;
    uint32_t send_queue_depth = 0;
    uint32_t receive_queue_depth = 0;
    uint32_t completion_queue_depth = 0;
    size_t max_message_bytes = 0;
};

struct RdmaProviderConnection {
    RdmaProviderConnectionId id = 0;
    transport::EndpointDescriptor local_endpoint;
    transport::EndpointDescriptor peer_endpoint;
    std::optional<security::AuthenticatedPeer> verified_peer;
};

struct RdmaProviderListener {
    RdmaProviderConnectionId id = 0;
    transport::EndpointDescriptor local_endpoint;
};

struct RdmaProviderSendRequest {
    RdmaWorkRequestId work_request_id = 0;
    RdmaProviderConnectionId connection_id = 0;
    // The provider may retain this memory only until a terminal completion for
    // work_request_id. The registration must describe this exact extent.
    std::span<const std::byte> canonical_wire;
    RegisteredMemory registration;
};

enum class RdmaProviderCompletionKind : uint8_t {
    kSendProgress = 0,
    kCqError = 1,
    kDeviceReset = 2,
    kPeerDeath = 3,
};

struct RdmaProviderCompletion {
    RdmaWorkRequestId work_request_id = 0;
    RdmaProviderConnectionId connection_id = 0;
    // Completions are incremental. A terminal success must make the cumulative
    // bytes equal the posted message size. Errors may terminate a partial WR.
    size_t bytes_completed = 0;
    bool terminal = false;
    RdmaProviderCompletionKind kind =
        RdmaProviderCompletionKind::kSendProgress;
    Status status = Status::Ok();
};

struct RdmaProviderReceive {
    RdmaProviderConnectionId connection_id = 0;
    transport::EndpointDescriptor peer_endpoint;
    // Complete Canonical Wire only. Providers must never expose a remote SHM
    // offset, ShmHandle, virtual address, or native object representation.
    std::vector<std::byte> canonical_wire;
};

struct RdmaProviderPollRequest {
    uint32_t max_completions = 0;
    uint32_t max_receives = 0;
    size_t max_receive_bytes = 0;
    uint32_t timeout_ms = 0;
};

struct RdmaProviderPollResult {
    std::vector<RdmaProviderCompletion> completions;
    std::vector<RdmaProviderReceive> receives;
};

// Implemented by a real kernel/device integration (for example an rdma-core
// plugin) or by a test-only loopback. Production composition accepts only
// provider_class()==kDevice. Register/Deregister are the same authoritative MR
// operations consumed by LargeObjectPool.
class RdmaDeviceProvider : public MemoryRegistrationProvider {
public:
    ~RdmaDeviceProvider() override = default;

    virtual std::string provenance() const = 0;
    virtual Status Start(const RdmaProviderLimits& limits) = 0;
    // Wakes blocking Poll without destroying QPs, CQs, or MRs. Shutdown is
    // called only after the driver has observed/fenced WRs and deregistered MR.
    virtual void RequestStop() noexcept = 0;
    virtual Status Shutdown() noexcept = 0;
    virtual Result<RdmaProviderConnection> Connect(
        const transport::ConnectRequest& request) = 0;
    virtual Result<RdmaProviderListener> Listen(
        const transport::ListenRequest& request) = 0;
    virtual Result<RdmaProviderConnection> Accept(
        RdmaProviderConnectionId listener_id, uint32_t timeout_ms) = 0;
    virtual Status PostSend(const RdmaProviderSendRequest& request) = 0;
    virtual Result<RdmaProviderPollResult> Poll(
        const RdmaProviderPollRequest& request) = 0;
    // Synchronously transitions the QP/listener to an error/closed state and
    // fences all DMA for that connection before returning. The driver may then
    // deregister and reclaim retained send buffers. Implementations should still
    // publish terminal CQ errors when possible for diagnostic accounting.
    virtual Status Close(RdmaProviderConnectionId connection_id) noexcept = 0;
};

struct DynamicRdmaProviderOptions {
    // Absolute path to a deployment-controlled plugin. Host search paths are
    // deliberately not consulted.
    std::string plugin_path;
    std::string device_name;
};

// Loads an explicitly provisioned device plugin. No libibverbs/rdma-core host
// dependency is silently discovered by the normal Mino build. The plugin must
// export:
//   mino_rdma_provider_abi_version_v1() -> uint32_t
//   mino_create_rdma_provider_v1(const char*) -> RdmaDeviceProvider*
//   mino_destroy_rdma_provider_v1(RdmaDeviceProvider*)
//   mino_rdma_provider_provenance_v1() -> const char*
// The plugin and Mino binary must be built with the same C++ toolchain/ABI; the
// qualification workflow records and hashes both artifacts.
Result<std::shared_ptr<RdmaDeviceProvider>> CreateDynamicRdmaDeviceProvider(
    const DynamicRdmaProviderOptions& options) noexcept;

}  // namespace mino::platform

#endif  // MINO_PLATFORM_RDMA_PROVIDER_H_
