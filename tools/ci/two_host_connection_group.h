// Copyright 2026 The Mino Authors

#ifndef TOOLS_CI_TWO_HOST_CONNECTION_GROUP_H_
#define TOOLS_CI_TWO_HOST_CONNECTION_GROUP_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "mino/bridge/bridge_runtime/connection_pool.h"
#include "mino/bridge/schema_negotiator.h"
#include "mino/schema/registry.h"
#include "mino/transport/tcp_driver.h"

namespace mino::tools::ci {

struct TwoHostConnectionStats {
    uint64_t connection_attempts = 0;
    uint64_t accepted_connections = 0;
    uint64_t completed_handshakes = 0;
    uint64_t reconnects = 0;
    uint64_t disconnects = 0;
    uint64_t accepted_acks = 0;
    uint64_t duplicate_checks = 0;
};

// Probe-only production composition for one logical peer. It mirrors
// RemoteBridge lane ownership while allowing the qualification probe to inject
// its strict descriptor authentication and persistence dependencies.
class TwoHostConnectionGroup final {
public:
    static Result<std::unique_ptr<TwoHostConnectionGroup>> Create(
        bridge::BridgeConnectionManagerOptions base_options,
        uint16_t lane_count, std::shared_ptr<transport::TcpDriver> driver,
        bridge::BridgeIngressPort* ingress,
        schema::SchemaRegistry* registry,
        bridge::DescriptorAuth* descriptor_auth,
        bridge::DescriptorPersistence* descriptor_persistence,
        bridge::SchemaNegotiatorLimits schema_limits = {}) noexcept;

    ~TwoHostConnectionGroup();
    TwoHostConnectionGroup(const TwoHostConnectionGroup&) = delete;
    TwoHostConnectionGroup& operator=(const TwoHostConnectionGroup&) = delete;

    Status Start(uint64_t now_ns) noexcept;
    Result<bridge::BridgeConnectionPumpResult> Pump(
        bridge::BridgePumpBudget budget = {}) noexcept;
    Status Shutdown() noexcept;

    bridge::BridgeConnectionState state() const noexcept {
        return pool_->state();
    }
    bool all_pipelines_ready() const noexcept;
    size_t retransmit_entries() const noexcept;
    uint64_t accepted_acks() const noexcept;
    uint64_t duplicate_checks() const noexcept;
    bool reliability_degraded() const noexcept;
    bool schema_announcement_observed() const noexcept;
    bool schema_request_pending() const noexcept;
    size_t active_lane_connections() const noexcept;
    TwoHostConnectionStats stats() const noexcept;

    bridge::BridgeConnectionManager& manager_for(
        const bridge::SourceIdentity& source) noexcept;
    const bridge::BridgeConnectionManager& manager_for(
        const bridge::SourceIdentity& source) const noexcept;
    bridge::BridgeConnectionManager& manager(uint16_t lane_index) noexcept {
        return pool_->manager(lane_index);
    }
    const bridge::BridgeConnectionManager& manager(
        uint16_t lane_index) const noexcept {
        return pool_->manager(lane_index);
    }
    std::shared_ptr<bridge::BridgeConnectionPool> pool() const noexcept {
        return pool_;
    }
    uint16_t lane_count() const noexcept { return pool_->lane_count(); }

private:
    TwoHostConnectionGroup(
        transport::DriverConfig driver_config,
        std::vector<std::unique_ptr<bridge::SchemaNegotiator>> negotiators,
        std::shared_ptr<transport::TcpDriver> driver,
        std::shared_ptr<bridge::BridgeConnectionPool> pool,
        std::unique_ptr<bridge::BridgeListenerHub> listener_hub) noexcept;

    transport::DriverConfig driver_config_;
    std::vector<std::unique_ptr<bridge::SchemaNegotiator>> negotiators_;
    std::shared_ptr<transport::TcpDriver> driver_;
    std::shared_ptr<bridge::BridgeConnectionPool> pool_;
    std::unique_ptr<bridge::BridgeListenerHub> listener_hub_;
    bool started_ = false;
};

}  // namespace mino::tools::ci

#endif  // TOOLS_CI_TWO_HOST_CONNECTION_GROUP_H_
