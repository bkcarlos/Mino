// Copyright 2026 The Mino Authors

#include "mino/bridge/bridge_runtime/connection_pool.h"

#include <algorithm>
#include <new>
#include <string_view>
#include <utility>

namespace mino::bridge {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

SourceIdentity SourceFrom(const EncodedOutboundFrame& frame) noexcept {
    return SourceIdentity{
        .node_id = frame.frame.header.source_node_id,
        .publisher_id = frame.frame.header.source_publisher_id,
        .publisher_epoch = frame.frame.header.source_publisher_epoch,
    };
}

bool ValidSource(const SourceIdentity& source) noexcept {
    return source.node_id != 0 && source.publisher_id != 0 &&
           source.publisher_epoch != 0;
}

BridgeConnectionState AggregateState(
    std::span<const std::shared_ptr<BridgeConnectionManager>> managers) noexcept {
    bool all_stopped = true;
    bool all_active = true;
    bool any_shutting_down = false;
    bool any_handshaking = false;
    for (const auto& manager : managers) {
        const BridgeConnectionState lane_state = manager->state();
        all_stopped = all_stopped && lane_state == BridgeConnectionState::kStopped;
        all_active = all_active && lane_state == BridgeConnectionState::kActive;
        any_shutting_down = any_shutting_down ||
                            lane_state == BridgeConnectionState::kShuttingDown;
        any_handshaking = any_handshaking ||
                          lane_state == BridgeConnectionState::kHandshaking;
    }
    if (all_stopped) return BridgeConnectionState::kStopped;
    if (all_active) return BridgeConnectionState::kActive;
    if (any_shutting_down) return BridgeConnectionState::kShuttingDown;
    if (any_handshaking) return BridgeConnectionState::kHandshaking;
    return BridgeConnectionState::kWaiting;
}

void AddPumpResult(const BridgePumpResult& lane,
                   BridgePumpResult* aggregate) noexcept {
    aggregate->completions += lane.completions;
    aggregate->inbound_frames += lane.inbound_frames;
    aggregate->outbound_frames += lane.outbound_frames;
    aggregate->retransmitted_frames += lane.retransmitted_frames;
    aggregate->bytes += lane.bytes;
    aggregate->made_progress = aggregate->made_progress || lane.made_progress;
}

}  // namespace

Status detail::BridgeEgressQuota::Reserve(size_t frames,
                                           size_t bytes) noexcept {
    std::lock_guard lock(mutex_);
    if (frames > max_frames_ - frames_ || bytes > max_bytes_ - bytes_) {
        return Status::Error(StatusCode::kWouldBlock,
                             "bridge peer egress quota is full");
    }
    frames_ += frames;
    bytes_ += bytes;
    return Status::Ok();
}

void detail::BridgeEgressQuota::Release(size_t frames, size_t bytes) noexcept {
    std::lock_guard lock(mutex_);
    frames_ = frames > frames_ ? 0 : frames_ - frames;
    bytes_ = bytes > bytes_ ? 0 : bytes_ - bytes;
}

size_t detail::BridgeEgressQuota::frames() const noexcept {
    std::lock_guard lock(mutex_);
    return frames_;
}

size_t detail::BridgeEgressQuota::bytes() const noexcept {
    std::lock_guard lock(mutex_);
    return bytes_;
}

BridgeConnectionPool::BridgeConnectionPool(
    std::shared_ptr<detail::BridgeEgressQuota> aggregate_quota,
    std::vector<std::shared_ptr<BridgeConnectionManager>> managers) noexcept
    : aggregate_quota_(std::move(aggregate_quota)),
      managers_(std::move(managers)) {}

Result<std::shared_ptr<BridgeConnectionPool>> BridgeConnectionPool::Create(
    std::vector<std::shared_ptr<BridgeConnectionManager>> managers,
    size_t max_egress_frames, size_t max_egress_bytes) noexcept {
    try {
        if (managers.empty() || managers.size() > kMaxBridgeLaneCount ||
            max_egress_frames == 0 || max_egress_bytes == 0) {
            return Invalid("bridge connection pool bounds are invalid");
        }
        const uint16_t lane_count = static_cast<uint16_t>(managers.size());
        std::sort(managers.begin(), managers.end(),
                  [](const auto& lhs, const auto& rhs) {
                      if (lhs == nullptr) return false;
                      if (rhs == nullptr) return true;
                      return lhs->lane_index() < rhs->lane_index();
                  });
        const auto& first = managers.front();
        if (first == nullptr) {
            return Invalid("bridge connection pool contains a null lane");
        }
        for (uint16_t lane = 0; lane < lane_count; ++lane) {
            const auto& manager = managers[lane];
            if (manager == nullptr || manager->options_.lane_count != lane_count ||
                manager->options_.lane_index != lane ||
                manager->state() != BridgeConnectionState::kStopped ||
                manager->queued_egress_frames() != 0 ||
                manager->aggregate_egress_quota_ != nullptr) {
                return Invalid(
                    "bridge connection pool lane set is incomplete or active");
            }
            if (manager->options_.local_identity !=
                    first->options_.local_identity ||
                manager->options_.expected_peer !=
                    first->options_.expected_peer ||
                manager->options_.route_driver_id !=
                    first->options_.route_driver_id ||
                manager->options_.route_driver_generation !=
                    first->options_.route_driver_generation ||
                manager->options_.peer_route_endpoint !=
                    first->options_.peer_route_endpoint ||
                manager->driver_.get() != first->driver_.get() ||
                manager->options_.max_egress_frames < max_egress_frames ||
                manager->options_.max_egress_bytes < max_egress_bytes) {
                return Invalid(
                    "bridge connection pool lanes do not share peer fencing");
            }
        }
        auto quota = std::make_shared<detail::BridgeEgressQuota>(
            max_egress_frames, max_egress_bytes);
        for (auto& manager : managers) {
            manager->aggregate_egress_quota_ = quota;
        }
        return std::shared_ptr<BridgeConnectionPool>(new BridgeConnectionPool(
            std::move(quota), std::move(managers)));
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge connection pool allocation failed");
    }
}

Result<std::shared_ptr<BridgeConnectionPool>>
BridgeConnectionPool::WrapSingle(
    std::shared_ptr<BridgeConnectionManager> manager) noexcept {
    try {
        if (manager == nullptr || manager->lane_count() != 1 ||
            manager->lane_index() != 0) {
            return Invalid("single bridge manager has invalid lane fencing");
        }
        std::vector<std::shared_ptr<BridgeConnectionManager>> managers;
        managers.push_back(std::move(manager));
        return std::shared_ptr<BridgeConnectionPool>(new BridgeConnectionPool(
            {}, std::move(managers)));
    } catch (const std::bad_alloc&) {
        return Exhausted("single bridge pool allocation failed");
    }
}

BridgeConnectionManager& BridgeConnectionPool::ManagerFor(
    const SourceIdentity& source) noexcept {
    return *managers_[BridgeLaneFor(source, lane_count())];
}

const BridgeConnectionManager& BridgeConnectionPool::ManagerFor(
    const SourceIdentity& source) const noexcept {
    return *managers_[BridgeLaneFor(source, lane_count())];
}

Status BridgeConnectionPool::Start(uint64_t now_ns) noexcept {
    size_t started = 0;
    for (; started < managers_.size(); ++started) {
        const Status status = managers_[started]->Start(now_ns);
        if (!status.ok()) {
            while (started != 0) {
                --started;
                static_cast<void>(managers_[started]->Shutdown());
            }
            return status;
        }
    }
    next_pump_lane_ = 0;
    return Status::Ok();
}

Result<BridgeConnectionPumpResult> BridgeConnectionPool::Pump(
    BridgePumpBudget budget) noexcept {
    try {
        if (managers_.empty()) {
            return Status::Error(StatusCode::kUnavailable,
                                 "bridge connection pool has no lanes");
        }
        BridgeConnectionPumpResult aggregate;
        const size_t first_lane = next_pump_lane_ % managers_.size();
        for (size_t offset = 0; offset < managers_.size(); ++offset) {
            const size_t lane = (first_lane + offset) % managers_.size();
            BridgePumpBudget remaining = budget;
            remaining.max_completions = static_cast<uint32_t>(
                aggregate.pipeline.completions >= budget.max_completions
                    ? 0
                    : budget.max_completions -
                          aggregate.pipeline.completions);
            remaining.max_inbound_frames = static_cast<uint32_t>(
                aggregate.pipeline.inbound_frames >= budget.max_inbound_frames
                    ? 0
                    : budget.max_inbound_frames -
                          aggregate.pipeline.inbound_frames);
            remaining.max_outbound_frames = static_cast<uint32_t>(
                aggregate.pipeline.outbound_frames >= budget.max_outbound_frames
                    ? 0
                    : budget.max_outbound_frames -
                          aggregate.pipeline.outbound_frames);
            remaining.max_bytes =
                aggregate.pipeline.bytes >= budget.max_bytes
                    ? 0
                    : budget.max_bytes - aggregate.pipeline.bytes;
            // BridgePipeline rejects a zero outbound or byte budget. Reaching
            // either shared limit is normal pool backpressure, so finish this
            // round instead of turning a fully consumed budget into an error on
            // the next lane.
            if (remaining.max_outbound_frames == 0 ||
                remaining.max_bytes == 0) {
                break;
            }
            auto pumped = managers_[lane]->Pump(remaining);
            if (!pumped.ok()) return pumped.status();
            AddPumpResult(pumped->pipeline, &aggregate.pipeline);
            aggregate.connection_opened =
                aggregate.connection_opened || pumped->connection_opened;
            aggregate.connection_lost =
                aggregate.connection_lost || pumped->connection_lost;
            aggregate.handshake_completed =
                aggregate.handshake_completed || pumped->handshake_completed;
        }
        next_pump_lane_ = (first_lane + 1) % managers_.size();
        aggregate.state = state();
        return aggregate;
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge connection pool pump allocation failed");
    }
}

Status BridgeConnectionPool::Shutdown() noexcept {
    Status first = Status::Ok();
    for (auto it = managers_.rbegin(); it != managers_.rend(); ++it) {
        const Status status = (*it)->Shutdown();
        if (first.ok() && !status.ok()) first = status;
    }
    next_pump_lane_ = 0;
    return first;
}

Status BridgeConnectionPool::Enqueue(EncodedOutboundFrame frame) noexcept {
    const SourceIdentity source = SourceFrom(frame);
    if (!ValidSource(source)) {
        return Invalid("bridge pool egress source identity is incomplete");
    }
    return ManagerFor(source).Enqueue(std::move(frame));
}

Status BridgeConnectionPool::ValidateOutboundSize(
    const EncodedOutboundFrame& frame,
    const transport::TransportCapabilities& route_capabilities) const noexcept {
    const SourceIdentity source = SourceFrom(frame);
    if (!ValidSource(source)) {
        return Invalid("bridge pool egress source identity is incomplete");
    }
    return ManagerFor(source).ValidateOutboundSize(frame, route_capabilities);
}

Result<BridgeEgressReservationToken> BridgeConnectionPool::ReserveEgress(
    EncodedOutboundFrame frame,
    std::shared_ptr<BridgeEgressAdmission> admission) noexcept {
    const SourceIdentity source = SourceFrom(frame);
    if (!ValidSource(source)) {
        return Invalid("bridge pool egress source identity is incomplete");
    }
    BridgeConnectionManager& manager = ManagerFor(source);
    MINO_ASSIGN_OR_RETURN(
        const uint64_t reservation_id,
        manager.ReserveEgress(std::move(frame), std::move(admission)));
    return BridgeEgressReservationToken{
        .lane_index = manager.lane_index(),
        .reservation_id = reservation_id,
    };
}

void BridgeConnectionPool::CancelEgressReservation(
    BridgeEgressReservationToken reservation) noexcept {
    if (reservation.lane_index >= managers_.size() ||
        reservation.reservation_id == 0) {
        return;
    }
    managers_[reservation.lane_index]->CancelEgressReservation(
        reservation.reservation_id);
}

bool BridgeConnectionPool::MatchesRoute(
    NodeId target_node,
    const transport::RemoteTargetRoute& target) const noexcept {
    return !managers_.empty() &&
           managers_.front()->MatchesRoute(target_node, target);
}

BridgeConnectionState BridgeConnectionPool::state() const noexcept {
    return AggregateState(managers_);
}

size_t BridgeConnectionPool::queued_egress_frames() const noexcept {
    if (aggregate_quota_ != nullptr) return aggregate_quota_->frames();
    size_t total = 0;
    for (const auto& manager : managers_) {
        total += manager->queued_egress_frames();
    }
    return total;
}

size_t BridgeConnectionPool::queued_egress_bytes() const noexcept {
    if (aggregate_quota_ != nullptr) return aggregate_quota_->bytes();
    size_t total = 0;
    for (const auto& manager : managers_) {
        total += manager->queued_egress_bytes();
    }
    return total;
}

}  // namespace mino::bridge
