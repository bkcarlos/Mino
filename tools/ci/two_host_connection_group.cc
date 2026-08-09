// Copyright 2026 The Mino Authors

#include "tools/ci/two_host_connection_group.h"

#include <algorithm>
#include <new>
#include <string_view>
#include <utility>

namespace mino::tools::ci {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

}  // namespace

TwoHostConnectionGroup::TwoHostConnectionGroup(
    transport::DriverConfig driver_config,
    std::vector<std::unique_ptr<bridge::SchemaNegotiator>> negotiators,
    std::shared_ptr<transport::TcpDriver> driver,
    std::shared_ptr<bridge::BridgeConnectionPool> pool,
    std::unique_ptr<bridge::BridgeListenerHub> listener_hub) noexcept
    : driver_config_(driver_config),
      negotiators_(std::move(negotiators)),
      driver_(std::move(driver)),
      pool_(std::move(pool)),
      listener_hub_(std::move(listener_hub)) {}

Result<std::unique_ptr<TwoHostConnectionGroup>>
TwoHostConnectionGroup::Create(
    bridge::BridgeConnectionManagerOptions base_options, uint16_t lane_count,
    std::shared_ptr<transport::TcpDriver> driver,
    bridge::BridgeIngressPort* ingress, schema::SchemaRegistry* registry,
    bridge::DescriptorAuth* descriptor_auth,
    bridge::DescriptorPersistence* descriptor_persistence,
    bridge::SchemaNegotiatorLimits schema_limits) noexcept {
    try {
        if (lane_count == 0 || lane_count > bridge::kMaxBridgeLaneCount ||
            driver == nullptr || ingress == nullptr || registry == nullptr ||
            descriptor_auth == nullptr || descriptor_persistence == nullptr ||
            base_options.lane_index != 0 || base_options.lane_count != 1 ||
            base_options.mode == bridge::BridgeConnectionMode::kAccepted ||
            base_options.driver_config.max_connections < lane_count) {
            return Invalid("two-host connection group configuration is invalid");
        }
        const bridge::BridgeConnectionMode configured_mode = base_options.mode;
        std::vector<std::unique_ptr<bridge::SchemaNegotiator>> negotiators;
        std::vector<std::shared_ptr<bridge::BridgeConnectionManager>> managers;
        negotiators.reserve(lane_count);
        managers.reserve(lane_count);
        for (uint16_t lane = 0; lane < lane_count; ++lane) {
            auto negotiator = std::make_unique<bridge::SchemaNegotiator>(
                registry, descriptor_auth, descriptor_persistence,
                schema_limits);
            bridge::BridgeConnectionManagerOptions lane_options = base_options;
            lane_options.manage_driver_lifecycle = false;
            lane_options.lane_index = lane;
            lane_options.lane_count = lane_count;
            if (configured_mode == bridge::BridgeConnectionMode::kListen) {
                lane_options.mode = bridge::BridgeConnectionMode::kAccepted;
            }
            MINO_ASSIGN_OR_RETURN(
                auto manager,
                bridge::BridgeConnectionManager::Create(
                    std::move(lane_options), driver, ingress,
                    negotiator.get()));
            negotiators.push_back(std::move(negotiator));
            managers.push_back(
                std::shared_ptr<bridge::BridgeConnectionManager>(
                    std::move(manager)));
        }
        MINO_ASSIGN_OR_RETURN(
            auto pool,
            bridge::BridgeConnectionPool::Create(
                managers, base_options.max_egress_frames,
                base_options.max_egress_bytes));

        std::unique_ptr<bridge::BridgeListenerHub> listener_hub;
        if (configured_mode == bridge::BridgeConnectionMode::kListen) {
            if (!base_options.local_endpoint.has_value()) {
                return Invalid("two-host listener endpoint is missing");
            }
            MINO_ASSIGN_OR_RETURN(
                listener_hub,
                bridge::BridgeListenerHub::Create(
                    bridge::BridgeListenerHubOptions{
                        .local_endpoint = *base_options.local_endpoint,
                        .driver_config = base_options.driver_config,
                        .manage_driver_lifecycle = false,
                        .listen_backlog = base_options.listen_backlog,
                        .max_peers = 1,
                        .max_pending_handshakes = std::max<size_t>(
                            lane_count, base_options.listen_backlog),
                        .max_accepts_per_pump = lane_count,
                        .handshake_timeout_ns =
                            base_options.handshake_timeout_ns,
                        .wire_limits = base_options.pipeline.wire_limits,
                    },
                    driver));
            for (const auto& manager : managers) {
                MINO_RETURN_IF_ERROR(listener_hub->RegisterPeer(manager));
            }
        }
        return std::unique_ptr<TwoHostConnectionGroup>(
            new TwoHostConnectionGroup(
                base_options.driver_config, std::move(negotiators),
                std::move(driver), std::move(pool),
                std::move(listener_hub)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "two-host connection group allocation failed");
    }
}

TwoHostConnectionGroup::~TwoHostConnectionGroup() {
    static_cast<void>(Shutdown());
}

Status TwoHostConnectionGroup::Start(uint64_t now_ns) noexcept {
    if (started_) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "two-host connection group is already started");
    }
    MINO_RETURN_IF_ERROR(driver_->Start(driver_config_));
    const Status pool_started = pool_->Start(now_ns);
    if (!pool_started.ok()) {
        static_cast<void>(driver_->Shutdown());
        return pool_started;
    }
    if (listener_hub_ != nullptr) {
        const Status hub_started = listener_hub_->Start();
        if (!hub_started.ok()) {
            static_cast<void>(pool_->Shutdown());
            static_cast<void>(driver_->Shutdown());
            return hub_started;
        }
    }
    started_ = true;
    return Status::Ok();
}

Result<bridge::BridgeConnectionPumpResult> TwoHostConnectionGroup::Pump(
    bridge::BridgePumpBudget budget) noexcept {
    if (!started_) {
        return Status::Error(StatusCode::kUnavailable,
                             "two-host connection group is not started");
    }
    if (listener_hub_ != nullptr) {
        auto accepted = listener_hub_->Pump(budget.now_ns);
        if (!accepted.ok()) return accepted.status();
    }
    return pool_->Pump(budget);
}

Status TwoHostConnectionGroup::Shutdown() noexcept {
    Status first = Status::Ok();
    if (listener_hub_ != nullptr) {
        const Status stopped = listener_hub_->Shutdown();
        if (!stopped.ok()) first = stopped;
    }
    const Status pool_stopped = pool_->Shutdown();
    if (first.ok() && !pool_stopped.ok()) first = pool_stopped;
    if (driver_->state() != transport::DriverState::kStopped) {
        const Status driver_stopped = driver_->Shutdown();
        if (first.ok() && !driver_stopped.ok()) first = driver_stopped;
    }
    started_ = false;
    return first;
}

bool TwoHostConnectionGroup::all_pipelines_ready() const noexcept {
    for (const auto& manager : pool_->managers()) {
        if (manager->pipeline() == nullptr ||
            !manager->pipeline()->session_ready()) {
            return false;
        }
    }
    return true;
}

size_t TwoHostConnectionGroup::retransmit_entries() const noexcept {
    size_t total = 0;
    for (const auto& manager : pool_->managers()) {
        if (manager->pipeline() != nullptr) {
            total += manager->pipeline()->retransmit_entries();
        }
    }
    return total;
}

uint64_t TwoHostConnectionGroup::accepted_acks() const noexcept {
    uint64_t total = 0;
    for (const auto& manager : pool_->managers()) {
        if (manager->pipeline() != nullptr) {
            total += manager->pipeline()->retransmit_stats().accepted_acks;
        }
    }
    return total;
}

uint64_t TwoHostConnectionGroup::duplicate_checks() const noexcept {
    uint64_t total = 0;
    for (const auto& manager : pool_->managers()) {
        if (manager->pipeline() != nullptr) {
            total += manager->pipeline()->dedup_stats().duplicate_checks;
        }
    }
    return total;
}

bool TwoHostConnectionGroup::reliability_degraded() const noexcept {
    for (const auto& manager : pool_->managers()) {
        if (manager->pipeline() == nullptr ||
            manager->pipeline()->reliability_degraded()) {
            return true;
        }
    }
    return false;
}

bool TwoHostConnectionGroup::schema_announcement_observed() const noexcept {
    return std::any_of(
        negotiators_.begin(), negotiators_.end(), [](const auto& negotiator) {
            return negotiator->local_ref_high_watermark() != 0 ||
                   negotiator->remote_ref_high_watermark() != 0;
        });
}

bool TwoHostConnectionGroup::schema_request_pending() const noexcept {
    return std::any_of(
        negotiators_.begin(), negotiators_.end(), [](const auto& negotiator) {
            return negotiator->pending_request_count() != 0;
        });
}

size_t TwoHostConnectionGroup::active_lane_connections() const noexcept {
    return static_cast<size_t>(std::count_if(
        pool_->managers().begin(), pool_->managers().end(),
        [](const auto& manager) {
            return manager->state() == bridge::BridgeConnectionState::kActive;
        }));
}

TwoHostConnectionStats TwoHostConnectionGroup::stats() const noexcept {
    TwoHostConnectionStats result;
    for (const auto& manager : pool_->managers()) {
        const bridge::BridgeConnectionManagerStats& lane = manager->stats();
        result.connection_attempts += lane.connection_attempts;
        result.accepted_connections += lane.accepted_connections;
        result.completed_handshakes += lane.completed_handshakes;
        result.reconnects += lane.reconnects;
        result.disconnects += lane.disconnects;
    }
    result.accepted_acks = accepted_acks();
    result.duplicate_checks = duplicate_checks();
    return result;
}

bridge::BridgeConnectionManager& TwoHostConnectionGroup::manager_for(
    const bridge::SourceIdentity& source) noexcept {
    return pool_->manager(bridge::BridgeLaneFor(source, lane_count()));
}

const bridge::BridgeConnectionManager& TwoHostConnectionGroup::manager_for(
    const bridge::SourceIdentity& source) const noexcept {
    return pool_->manager(bridge::BridgeLaneFor(source, lane_count()));
}

}  // namespace mino::tools::ci
