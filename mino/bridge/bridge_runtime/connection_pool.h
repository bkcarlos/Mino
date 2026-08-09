// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_BRIDGE_RUNTIME_CONNECTION_POOL_H_
#define MINO_BRIDGE_BRIDGE_RUNTIME_CONNECTION_POOL_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

#include "mino/bridge/bridge_runtime/connection_manager.h"

namespace mino::bridge {
namespace detail {

// Shared by every lane of one logical peer. It bounds retained application
// egress once per peer instead of multiplying the configured queue by lane count.
class BridgeEgressQuota final {
public:
    BridgeEgressQuota(size_t max_frames, size_t max_bytes) noexcept
        : max_frames_(max_frames), max_bytes_(max_bytes) {}

    Status Reserve(size_t frames, size_t bytes) noexcept;
    void Release(size_t frames, size_t bytes) noexcept;
    size_t frames() const noexcept;
    size_t bytes() const noexcept;

private:
    const size_t max_frames_;
    const size_t max_bytes_;
    mutable std::mutex mutex_;
    size_t frames_ = 0;
    size_t bytes_ = 0;
};

}  // namespace detail

struct BridgeEgressReservationToken {
    uint16_t lane_index = 0;
    uint64_t reservation_id = 0;
};

// Logical peer endpoint composed from a complete, immutable TCP lane set.
// SourceIdentity hashing is stable for the process incarnation: a source never
// falls back to another lane when its selected lane is unavailable or full.
class BridgeConnectionPool final {
public:
    static Result<std::shared_ptr<BridgeConnectionPool>> Create(
        std::vector<std::shared_ptr<BridgeConnectionManager>> managers,
        size_t max_egress_frames, size_t max_egress_bytes) noexcept;

    // Compatibility wrapper for an already-created single-lane manager. The
    // manager keeps its existing queue accounting and may already be running.
    static Result<std::shared_ptr<BridgeConnectionPool>> WrapSingle(
        std::shared_ptr<BridgeConnectionManager> manager) noexcept;

    Status Start(uint64_t now_ns = 0) noexcept;
    Result<BridgeConnectionPumpResult> Pump(
        BridgePumpBudget budget = {}) noexcept;
    Status Shutdown() noexcept;

    Status Enqueue(EncodedOutboundFrame frame) noexcept;
    Status ValidateOutboundSize(
        const EncodedOutboundFrame& frame,
        const transport::TransportCapabilities& route_capabilities) const noexcept;
    Result<BridgeEgressReservationToken> ReserveEgress(
        EncodedOutboundFrame frame,
        std::shared_ptr<BridgeEgressAdmission> admission) noexcept;
    void CancelEgressReservation(
        BridgeEgressReservationToken reservation) noexcept;

    bool MatchesRoute(NodeId target_node,
                      const transport::RemoteTargetRoute& target) const noexcept;
    BridgeConnectionState state() const noexcept;
    uint16_t lane_count() const noexcept {
        return static_cast<uint16_t>(managers_.size());
    }
    BridgeConnectionManager& manager(uint16_t lane_index) noexcept {
        return *managers_[lane_index];
    }
    const BridgeConnectionManager& manager(uint16_t lane_index) const noexcept {
        return *managers_[lane_index];
    }
    std::span<const std::shared_ptr<BridgeConnectionManager>> managers()
        const noexcept {
        return managers_;
    }
    size_t queued_egress_frames() const noexcept;
    size_t queued_egress_bytes() const noexcept;

private:
    BridgeConnectionPool(
        std::shared_ptr<detail::BridgeEgressQuota> aggregate_quota,
        std::vector<std::shared_ptr<BridgeConnectionManager>> managers) noexcept;

    BridgeConnectionManager& ManagerFor(
        const SourceIdentity& source) noexcept;
    const BridgeConnectionManager& ManagerFor(
        const SourceIdentity& source) const noexcept;

    // Declared before managers so managers release their queue charges before
    // the quota is destroyed.
    std::shared_ptr<detail::BridgeEgressQuota> aggregate_quota_;
    std::vector<std::shared_ptr<BridgeConnectionManager>> managers_;
    size_t next_pump_lane_ = 0;
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_BRIDGE_RUNTIME_CONNECTION_POOL_H_
