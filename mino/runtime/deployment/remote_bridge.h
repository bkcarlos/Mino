// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_DEPLOYMENT_REMOTE_BRIDGE_H_
#define MINO_RUNTIME_DEPLOYMENT_REMOTE_BRIDGE_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "mino/bridge/bridge_runtime/connection_manager.h"
#include "mino/bridge/bridge_runtime/connection_pool.h"
#include "mino/bridge/schema_negotiator.h"
#include "mino/capacity/capacity.h"
#include "mino/common/result.h"
#include "mino/registry/metadata.h"
#include "mino/schema/registry.h"
#include "mino/storage/schema_store.h"
#include "mino/transport/tcp_driver.h"

namespace mino::deployment {

struct RemoteBridgeConfig {
    bridge::BridgeConnectionManagerOptions connection;
    transport::TcpDriverOptions tcp;
    // Immutable for the lifetime of this composition. Every SourceIdentity is
    // deterministically pinned to one lane; unavailable lanes never fall back.
    uint16_t tcp_lane_count = 1;
    bridge::SchemaNegotiatorLimits schema_negotiation;
    std::filesystem::path schema_store_root;
};

Result<capacity::ResourceVector> EstimateRemoteBridgeResources(
    const RemoteBridgeConfig& config) noexcept;

// Single-peer production composition root. It owns the TCP transport, schema
// registry and durable store, schema negotiator, and reconnecting Bridge manager.
// The ingress port is externally owned and must outlive RemoteBridge. Descriptor
// authentication is shared-owned so every remotely learned artifact remains
// fail-closed for the lifetime of the connection.
class RemoteBridge final {
public:
    static Result<std::unique_ptr<RemoteBridge>> Create(
        RemoteBridgeConfig config, bridge::BridgeIngressPort* ingress,
        std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
        std::shared_ptr<capacity::CapacityController> capacity_controller = {},
        std::optional<capacity::ResourceVector> capacity_charge =
            std::nullopt) noexcept;

    ~RemoteBridge();
    RemoteBridge(const RemoteBridge&) = delete;
    RemoteBridge& operator=(const RemoteBridge&) = delete;
    RemoteBridge(RemoteBridge&&) = delete;
    RemoteBridge& operator=(RemoteBridge&&) = delete;

    Status Start(uint64_t now_ns = 0) noexcept;
    Result<bridge::BridgeConnectionPumpResult> Pump(
        bridge::BridgePumpBudget budget = {}) noexcept;
    Status Shutdown() noexcept;

    // Registration is deployment-time only and must finish before Start(). The
    // complete artifact is retained to answer peer schema requests.
    Result<std::vector<schema::SchemaHandle>> RegisterLocalDescriptor(
        std::span<const std::byte> descriptor_artifact) noexcept;

    // Adds canonical application data to the manager's bounded reliable/best-
    // effort queue. Header schema fields are fenced to the registered identity.
    Status Enqueue(bridge::WireFrame frame,
                   const schema::SchemaIdentity& identity,
                   registry::Reliability reliability,
                   bool allow_drop = false) noexcept;

    bridge::BridgeConnectionManager& manager() noexcept {
        return pool_->manager(0);
    }
    const bridge::BridgeConnectionManager& manager() const noexcept {
        return pool_->manager(0);
    }
    bridge::BridgeConnectionManager& manager(uint16_t lane_index) noexcept {
        return pool_->manager(lane_index);
    }
    const bridge::BridgeConnectionManager& manager(
        uint16_t lane_index) const noexcept {
        return pool_->manager(lane_index);
    }
    bridge::BridgeConnectionPool& connection_pool() noexcept { return *pool_; }
    const bridge::BridgeConnectionPool& connection_pool() const noexcept {
        return *pool_;
    }
    uint16_t tcp_lane_count() const noexcept { return pool_->lane_count(); }
    const transport::TcpDriver& driver() const noexcept { return *driver_; }
    schema::SchemaRegistry& schema_registry() noexcept { return *registry_; }
    const schema::SchemaRegistry& schema_registry() const noexcept {
        return *registry_;
    }
    storage::SchemaStore& schema_store() noexcept { return *store_; }
    const storage::SchemaStore& schema_store() const noexcept { return *store_; }

private:
    class StorePersistence;

    RemoteBridge(
        capacity::CapacityLease capacity_lease,
        std::unique_ptr<schema::SchemaRegistry> registry,
        std::unique_ptr<storage::SchemaStore> store,
        std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
        std::unique_ptr<StorePersistence> persistence,
        std::vector<std::unique_ptr<bridge::SchemaNegotiator>> negotiators,
        std::shared_ptr<transport::TcpDriver> driver,
        std::shared_ptr<bridge::BridgeConnectionPool> pool,
        std::unique_ptr<bridge::BridgeListenerHub> listener_hub,
        transport::DriverConfig driver_config) noexcept;

    // Declared first so the charge remains held until every composed resource
    // has been destroyed (members are destroyed in reverse declaration order).
    capacity::CapacityLease capacity_lease_;
    std::unique_ptr<schema::SchemaRegistry> registry_;
    std::unique_ptr<storage::SchemaStore> store_;
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth_;
    std::unique_ptr<StorePersistence> persistence_;
    std::vector<std::unique_ptr<bridge::SchemaNegotiator>> negotiators_;
    std::shared_ptr<transport::TcpDriver> driver_;
    std::shared_ptr<bridge::BridgeConnectionPool> pool_;
    std::unique_ptr<bridge::BridgeListenerHub> listener_hub_;
    transport::DriverConfig driver_config_;
    bool started_ = false;
    std::map<schema::CanonicalDigest, std::vector<std::byte>> local_artifacts_;
};

}  // namespace mino::deployment

#endif  // MINO_RUNTIME_DEPLOYMENT_REMOTE_BRIDGE_H_
