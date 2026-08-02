// Copyright 2026 The Mino Authors

#include "mino/bridge/bridge_runtime/connection_manager.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <new>
#include <random>
#include <utility>
#include <variant>

#include "mino/schema/canonical.h"

namespace mino::bridge {
namespace {

Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Unavailable(const char* message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

Status Exhausted(const char* message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

bool IsWouldBlock(const Status& status) noexcept {
    return status.code() == StatusCode::kWouldBlock ||
           status.code() == StatusCode::kTimeout;
}

bool IsPeerProtocolFailure(const Status& status) noexcept {
    return status.code() == StatusCode::kCorruption ||
           status.code() == StatusCode::kSchemaMismatch ||
           status.code() == StatusCode::kUnsupported ||
           status.code() == StatusCode::kPermissionDenied;
}

bool IsConnectionFailure(const Status& status) noexcept {
    return status.code() == StatusCode::kNotFound ||
           status.code() == StatusCode::kUnavailable ||
           status.code() == StatusCode::kTimeout ||
           IsPeerProtocolFailure(status);
}

bool CapabilitiesEqual(const transport::TransportCapabilities& lhs,
                       const transport::TransportCapabilities& rhs) noexcept {
    return lhs.kind == rhs.kind && lhs.reliability == rhs.reliability &&
           lhs.max_frame_size == rhs.max_frame_size &&
           lhs.max_reassembly_bytes == rhs.max_reassembly_bytes &&
           lhs.features == rhs.features;
}

bool TargetRouteEqual(const transport::TargetRoute& lhs,
                      const transport::TargetRoute& rhs) noexcept {
    if (lhs.target_node != rhs.target_node ||
        lhs.transport.index() != rhs.transport.index()) {
        return false;
    }
    if (const auto* lhs_local =
            std::get_if<transport::LocalTargetRoute>(&lhs.transport)) {
        const auto& rhs_local =
            std::get<transport::LocalTargetRoute>(rhs.transport);
        return lhs_local->binding == rhs_local.binding;
    }
    const auto& lhs_remote =
        std::get<transport::RemoteTargetRoute>(lhs.transport);
    const auto& rhs_remote =
        std::get<transport::RemoteTargetRoute>(rhs.transport);
    return lhs_remote.endpoint == rhs_remote.endpoint &&
           lhs_remote.node_config_version == rhs_remote.node_config_version &&
           lhs_remote.process_identity == rhs_remote.process_identity &&
           lhs_remote.lease_epoch == rhs_remote.lease_epoch &&
           lhs_remote.driver_id == rhs_remote.driver_id &&
           lhs_remote.driver_generation == rhs_remote.driver_generation &&
           CapabilitiesEqual(lhs_remote.capabilities,
                             rhs_remote.capabilities) &&
           lhs_remote.driver == rhs_remote.driver;
}

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) noexcept {
    return rhs > std::numeric_limits<uint64_t>::max() - lhs
               ? std::numeric_limits<uint64_t>::max()
               : lhs + rhs;
}

uint64_t SplitMix64(uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ull;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ull;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebull;
    return value ^ (value >> 31);
}

WireFrame ControlFrame(FrameType type, std::vector<std::byte> payload = {}) {
    WireFrame frame;
    frame.header.frame_type = type;
    frame.header.flags = FlagValue(FrameFlag::kControlFrame) |
                         FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.payload = std::move(payload);
    return frame;
}

}  // namespace

Result<std::unique_ptr<BridgeConnectionManager>>
BridgeConnectionManager::Create(
    BridgeConnectionManagerOptions options,
    std::shared_ptr<transport::TransportDriver> driver,
    BridgeIngressPort* ingress,
    SchemaNegotiator* schema_negotiator) noexcept {
    try {
        if (driver == nullptr || ingress == nullptr) {
            return Invalid("bridge connection dependencies are null");
        }
        if (options.mode == BridgeConnectionMode::kConnect &&
            !options.remote_endpoint.has_value()) {
            return Invalid("bridge connector has no remote endpoint");
        }
        if (options.mode == BridgeConnectionMode::kListen &&
            !options.local_endpoint.has_value()) {
            return Invalid("bridge listener has no local endpoint");
        }
        if (!options.local_identity.complete() ||
            !options.expected_peer.complete() ||
            options.route_driver_id == 0 ||
            options.route_driver_generation == 0) {
            return Invalid("bridge node or route driver identity is incomplete");
        }
        if (options.listen_backlog == 0 ||
            options.connect_timeout_ms > transport::kMaxOperationTimeoutMs ||
            options.handshake_timeout_ns == 0 ||
            options.initial_reconnect_backoff_ns == 0 ||
            options.max_reconnect_backoff_ns <
                options.initial_reconnect_backoff_ns ||
            options.health_probe_interval_ns == 0 ||
            options.max_egress_frames == 0 || options.max_egress_bytes == 0) {
            return Invalid("bridge connection limits are invalid");
        }
        if (options.mode == BridgeConnectionMode::kConnect &&
            !driver->capabilities().features.Has(
                transport::Capability::kConnect)) {
            return Status::Error(StatusCode::kUnsupported,
                                 "bridge driver cannot connect");
        }
        if (options.mode == BridgeConnectionMode::kListen &&
            !driver->capabilities().features.Has(
                transport::Capability::kListen)) {
            return Status::Error(StatusCode::kUnsupported,
                                 "bridge driver cannot listen");
        }
        options.pipeline.local_session_epoch = 0;
        options.pipeline.remote_session_epoch = 0;
        if (!options.peer_route_endpoint.has_value() &&
            options.remote_endpoint.has_value()) {
            options.peer_route_endpoint = options.remote_endpoint;
        }
        return std::unique_ptr<BridgeConnectionManager>(
            new BridgeConnectionManager(std::move(options), std::move(driver),
                                        ingress, schema_negotiator));
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge connection manager allocation failed");
    }
}

BridgeConnectionManager::BridgeConnectionManager(
    BridgeConnectionManagerOptions options,
    std::shared_ptr<transport::TransportDriver> driver,
    BridgeIngressPort* ingress,
    SchemaNegotiator* schema_negotiator) noexcept
    : options_(std::move(options)),
      driver_(std::move(driver)),
      ingress_(ingress),
      schema_negotiator_(schema_negotiator) {}

BridgeConnectionManager::~BridgeConnectionManager() { (void)Shutdown(); }

uint64_t BridgeConnectionManager::EffectiveNow(
    uint64_t supplied_now_ns) const noexcept {
    if (supplied_now_ns != 0) return supplied_now_ns;
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

uint64_t BridgeConnectionManager::GenerateNewEpoch() noexcept {
    static std::atomic<uint64_t> fallback_seed{1};
    uint64_t candidate = 0;
    try {
        std::random_device random;
        candidate = (static_cast<uint64_t>(random()) << 32) ^
                    static_cast<uint64_t>(random());
    } catch (...) {
        candidate = 0;
    }
    candidate ^= SplitMix64(EffectiveNow(0));
    candidate ^= SplitMix64(
        fallback_seed.fetch_add(1, std::memory_order_relaxed));
    candidate = SplitMix64(candidate);
    if (candidate == 0 || candidate == local_session_epoch_) {
        candidate = SplitMix64(candidate ^ fallback_seed.fetch_add(
                                               1, std::memory_order_relaxed));
    }
    if (candidate == 0 || candidate == local_session_epoch_) {
        candidate = local_session_epoch_ == std::numeric_limits<uint64_t>::max()
                        ? 1
                        : local_session_epoch_ + 1;
    }
    return candidate;
}

Status BridgeConnectionManager::OpenListener() noexcept {
    auto listener = driver_->Listen(transport::ListenRequest{
        .local_endpoint = *options_.local_endpoint,
        .backlog = options_.listen_backlog,
    });
    if (!listener.ok()) return listener.status();
    listener_id_ = listener->id;
    return Status::Ok();
}

Status BridgeConnectionManager::Start(uint64_t now_ns) noexcept {
    if (state_ != BridgeConnectionState::kStopped) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "bridge connection manager is already started");
    }
    const uint64_t now = EffectiveNow(now_ns);
    if (driver_->state() == transport::DriverState::kStopped) {
        if (!options_.manage_driver_lifecycle) {
            return Unavailable("bridge driver is not running");
        }
        const Status started = driver_->Start(options_.driver_config);
        if (!started.ok()) return started;
        driver_started_by_manager_ = true;
    } else if (driver_->state() != transport::DriverState::kRunning) {
        return Unavailable("bridge driver is stopping");
    }

    if (options_.mode == BridgeConnectionMode::kListen) {
        const Status listening = OpenListener();
        if (!listening.ok()) {
            if (driver_started_by_manager_) {
                (void)driver_->Shutdown();
                driver_started_by_manager_ = false;
            }
            return listening;
        }
    }
    state_ = BridgeConnectionState::kWaiting;
    next_retry_ns_ = now;
    current_backoff_ns_ = 0;
    last_failure_ = Status::Ok();
    return Status::Ok();
}

Status BridgeConnectionManager::BeginConnection(
    transport::ConnectionInfo connection, uint64_t now_ns) noexcept {
    if (connection.id == transport::kInvalidConnectionId ||
        connection.is_listener) {
        return Invalid("bridge received invalid connected transport");
    }
    connection_id_ = connection.id;
    local_session_epoch_ = GenerateNewEpoch();
    remote_session_epoch_ = 0;
    handshake_started_ns_ = now_ns;
    discovery_sent_ = false;
    adopted_discovery_.reset();
    next_probe_ns_ = SaturatingAdd(now_ns,
                                   options_.health_probe_interval_ns);
    state_ = BridgeConnectionState::kHandshaking;
    return Status::Ok();
}

Status BridgeConnectionManager::AdoptAcceptedConnection(
    transport::ConnectionInfo connection,
    const SessionDiscovery& peer_discovery,
    uint64_t now_ns) noexcept {
    if (options_.mode != BridgeConnectionMode::kAccepted) {
        return Invalid("bridge manager is not hub-accepted");
    }
    if (state_.load(std::memory_order_acquire) !=
        BridgeConnectionState::kWaiting) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "bridge peer manager already owns a connection");
    }
    if (now_ns < next_retry_ns_) {
        return Status::Error(StatusCode::kWouldBlock,
                             "bridge peer manager is in reconnect backoff");
    }
    const BridgeNodeIdentityFence actual{
        .node_id = peer_discovery.node_id,
        .process_identity = peer_discovery.process_identity,
        .lease_epoch = peer_discovery.lease_epoch,
        .node_config_version = peer_discovery.node_config_version,
    };
    if (actual != options_.expected_peer || peer_discovery.session_epoch == 0) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "accepted bridge peer identity fencing failed");
    }
    MINO_RETURN_IF_ERROR(BeginConnection(std::move(connection), now_ns));
    adopted_discovery_ = peer_discovery;
    return Status::Ok();
}

Status BridgeConnectionManager::SendDiscoveryHello() noexcept {
    MINO_ASSIGN_OR_RETURN(
        auto payload,
        ControlPayloadCodec::EncodeSessionDiscovery(SessionDiscovery{
            .session_epoch = local_session_epoch_,
            .node_id = options_.local_identity.node_id,
            .process_identity = options_.local_identity.process_identity,
            .lease_epoch = options_.local_identity.lease_epoch,
            .node_config_version =
                options_.local_identity.node_config_version,
        }));
    MINO_ASSIGN_OR_RETURN(
        auto encoded,
        WireFrameCodec::Encode(
            ControlFrame(FrameType::kSessionDiscovery, std::move(payload)),
            options_.pipeline.wire_limits));
    auto sent = driver_->SendUntracked(transport::UntrackedSendRequest{
        .connection_id = connection_id_,
        .payload = encoded,
        .traffic_class = transport::UntrackedTrafficClass::kProtocolControl,
    });
    if (!sent.ok()) return sent.status();
    discovery_sent_ = true;
    return Status::Ok();
}

Result<std::optional<SessionDiscovery>>
BridgeConnectionManager::PollDiscoveryHello() noexcept {
    auto received = driver_->Poll(transport::ReceiveRequest{
        .max_messages = 1,
        .max_bytes = options_.pipeline.wire_limits.max_buffered_bytes,
        .timeout_ms = 0,
        .connection_id = connection_id_,
    });
    if (!received.ok()) {
        if (IsWouldBlock(received.status())) {
            return std::optional<SessionDiscovery>{};
        }
        return received.status();
    }
    if (received->messages.size() != 1) {
        return Status::Error(StatusCode::kCorruption,
                             "epoch discovery returned an invalid batch");
    }
    MINO_ASSIGN_OR_RETURN(
        auto frame,
        WireFrameCodec::Decode(received->messages.front().payload,
                               options_.pipeline.wire_limits));
    if (frame.header.frame_type != FrameType::kSessionDiscovery) {
        return Status::Error(StatusCode::kCorruption,
                             "non-discovery frame preceded identity fencing");
    }
    MINO_ASSIGN_OR_RETURN(
        auto discovery,
        ControlPayloadCodec::DecodeSessionDiscovery(frame.payload));
    const BridgeNodeIdentityFence actual{
        .node_id = discovery.node_id,
        .process_identity = discovery.process_identity,
        .lease_epoch = discovery.lease_epoch,
        .node_config_version = discovery.node_config_version,
    };
    if (actual != options_.expected_peer) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "bridge discovery peer identity fencing failed");
    }
    return std::optional<SessionDiscovery>{std::move(discovery)};
}

Status BridgeConnectionManager::CompleteHandshake(uint64_t remote_epoch,
                                                  uint64_t now_ns) noexcept {
    if (remote_epoch == 0) {
        return Invalid("bridge remote session epoch is zero");
    }
    BridgePipelineOptions pipeline_options = options_.pipeline;
    pipeline_options.local_session_epoch = local_session_epoch_;
    pipeline_options.remote_session_epoch = remote_epoch;
    if (pipeline_ == nullptr) {
        MINO_ASSIGN_OR_RETURN(
            pipeline_,
            BridgePipeline::Create(pipeline_options, driver_, connection_id_,
                                   this, ingress_, schema_negotiator_));
    } else {
        MINO_RETURN_IF_ERROR(pipeline_->RebindConnection(
            connection_id_, local_session_epoch_, remote_epoch,
            pipeline_options.local_dedup_state_lost, now_ns));
    }
    remote_session_epoch_ = remote_epoch;
    // Remain handshaking until BridgePipeline receives the peer's fully fenced
    // HELLO. Epoch discovery alone is not an application-ready session.
    state_ = BridgeConnectionState::kHandshaking;
    return Status::Ok();
}

Status BridgeConnectionManager::SendHealthProbe(uint64_t now_ns) noexcept {
    if (now_ns < next_probe_ns_) return Status::Ok();
    MINO_ASSIGN_OR_RETURN(
        auto encoded,
        WireFrameCodec::Encode(ControlFrame(FrameType::kHeartbeat),
                               options_.pipeline.wire_limits));
    auto sent = driver_->SendUntracked(transport::UntrackedSendRequest{
        .connection_id = connection_id_,
        .payload = encoded,
        .traffic_class = transport::UntrackedTrafficClass::kProtocolControl,
    });
    if (!sent.ok()) return sent.status();
    ++stats_.health_probes;
    next_probe_ns_ = SaturatingAdd(now_ns,
                                   options_.health_probe_interval_ns);
    return Status::Ok();
}

void BridgeConnectionManager::ScheduleRetry(uint64_t now_ns) noexcept {
    if (current_backoff_ns_ == 0) {
        current_backoff_ns_ = options_.initial_reconnect_backoff_ns;
    } else if (current_backoff_ns_ >=
               options_.max_reconnect_backoff_ns / 2) {
        current_backoff_ns_ = options_.max_reconnect_backoff_ns;
    } else {
        current_backoff_ns_ = std::min(options_.max_reconnect_backoff_ns,
                                       current_backoff_ns_ * 2);
    }
    next_retry_ns_ = SaturatingAdd(now_ns, current_backoff_ns_);
    state_ = BridgeConnectionState::kWaiting;
}

void BridgeConnectionManager::LoseConnection(
    const Status& failure, uint64_t now_ns, bool protocol_failure,
    BridgeConnectionPumpResult* result) noexcept {
    last_failure_ = failure;
    if (connection_id_ != transport::kInvalidConnectionId) {
        (void)driver_->Close(connection_id_);
    }
    connection_id_ = transport::kInvalidConnectionId;
    remote_session_epoch_ = 0;
    discovery_sent_ = false;
    adopted_discovery_.reset();
    ++stats_.disconnects;
    if (protocol_failure) ++stats_.protocol_failures;
    result->connection_lost = true;
    ScheduleRetry(now_ns);
}

Result<BridgeConnectionPumpResult> BridgeConnectionManager::Pump(
    BridgePumpBudget budget) noexcept {
    try {
        if (state_ == BridgeConnectionState::kStopped ||
            state_ == BridgeConnectionState::kShuttingDown) {
            return Unavailable("bridge connection manager is not running");
        }
        const uint64_t now = EffectiveNow(budget.now_ns);
        budget.now_ns = now;
        BridgeConnectionPumpResult result;

        if (state_ == BridgeConnectionState::kWaiting && now >= next_retry_ns_) {
            if (options_.mode == BridgeConnectionMode::kAccepted) {
                result.state = state_;
                return result;
            }
            Result<transport::ConnectionInfo> opened =
                Status::Error(StatusCode::kWouldBlock);
            if (options_.mode == BridgeConnectionMode::kConnect) {
                ++stats_.connection_attempts;
                opened = driver_->Connect(transport::ConnectRequest{
                    .remote_endpoint = *options_.remote_endpoint,
                    .local_bind = options_.local_endpoint,
                    .timeout_ms = options_.connect_timeout_ms,
                });
            } else {
                if (listener_id_ == transport::kInvalidConnectionId) {
                    const Status listening = OpenListener();
                    if (!listening.ok()) {
                        last_failure_ = listening;
                        ScheduleRetry(now);
                        result.state = state_;
                        return result;
                    }
                }
                opened = driver_->Accept(transport::AcceptRequest{
                    .listener_id = listener_id_,
                    .timeout_ms = 0,
                });
            }
            if (!opened.ok()) {
                if (!IsWouldBlock(opened.status())) {
                    last_failure_ = opened.status();
                    if (options_.mode == BridgeConnectionMode::kListen &&
                        opened.status().code() == StatusCode::kNotFound) {
                        listener_id_ = transport::kInvalidConnectionId;
                    }
                    ScheduleRetry(now);
                }
                result.state = state_;
                return result;
            }
            if (options_.mode == BridgeConnectionMode::kListen) {
                ++stats_.accepted_connections;
            }
            const Status begun = BeginConnection(std::move(*opened), now);
            if (!begun.ok()) {
                LoseConnection(begun, now, false, &result);
                result.state = state_;
                return result;
            }
            result.connection_opened = true;
        }

        if (state_ == BridgeConnectionState::kHandshaking &&
            remote_session_epoch_ == 0) {
            if (!discovery_sent_) {
                const Status sent = SendDiscoveryHello();
                if (!sent.ok() && !IsWouldBlock(sent)) {
                    LoseConnection(sent, now, IsPeerProtocolFailure(sent),
                                   &result);
                    result.state = state_;
                    return result;
                }
            }
            if (discovery_sent_ && adopted_discovery_.has_value()) {
                const Status completed = CompleteHandshake(
                    adopted_discovery_->session_epoch, now);
                if (!completed.ok()) {
                    if (!IsConnectionFailure(completed)) return completed;
                    LoseConnection(completed, now,
                                   IsPeerProtocolFailure(completed), &result);
                    result.state = state_;
                    return result;
                }
                adopted_discovery_.reset();
            } else if (discovery_sent_) {
                auto discovered = PollDiscoveryHello();
                if (!discovered.ok()) {
                    LoseConnection(discovered.status(), now,
                                   IsPeerProtocolFailure(discovered.status()),
                                   &result);
                    result.state = state_;
                    return result;
                }
                if (discovered->has_value()) {
                    const Status completed = CompleteHandshake(
                        (**discovered).session_epoch, now);
                    if (!completed.ok()) {
                        if (!IsConnectionFailure(completed)) return completed;
                        LoseConnection(completed, now,
                                       IsPeerProtocolFailure(completed),
                                       &result);
                        result.state = state_;
                        return result;
                    }
                }
            }
        }

        if ((state_ == BridgeConnectionState::kHandshaking &&
             remote_session_epoch_ != 0) ||
            state_ == BridgeConnectionState::kActive) {
            auto pumped = pipeline_->Pump(budget);
            if (!pumped.ok()) {
                if (!IsConnectionFailure(pumped.status())) {
                    return pumped.status();
                }
                LoseConnection(pumped.status(), now,
                               IsPeerProtocolFailure(pumped.status()),
                               &result);
            } else {
                result.pipeline = std::move(*pumped);
                if (state_ == BridgeConnectionState::kHandshaking &&
                    pipeline_->session_ready()) {
                    state_ = BridgeConnectionState::kActive;
                    current_backoff_ns_ = 0;
                    next_retry_ns_ = 0;
                    next_probe_ns_ = SaturatingAdd(
                        now, options_.health_probe_interval_ns);
                    ++stats_.completed_handshakes;
                    if (ever_active_) ++stats_.reconnects;
                    ever_active_ = true;
                    last_failure_ = Status::Ok();
                    result.handshake_completed = true;
                }
                if (state_ == BridgeConnectionState::kActive) {
                    const Status probe = SendHealthProbe(now);
                    if (!probe.ok()) {
                        if (!IsConnectionFailure(probe) &&
                            !IsWouldBlock(probe)) {
                            return probe;
                        }
                        if (!IsWouldBlock(probe)) {
                            LoseConnection(probe, now, false, &result);
                        }
                    }
                }
            }
        }
        if (state_ == BridgeConnectionState::kHandshaking &&
            now >= handshake_started_ns_ &&
            now - handshake_started_ns_ >=
                options_.handshake_timeout_ns) {
            LoseConnection(
                Status::Error(StatusCode::kTimeout,
                              "bridge session handshake timed out"),
                now, false, &result);
        }
        result.state = state_;
        return result;
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge connection pump allocation failed");
    }
}

Status BridgeConnectionManager::Shutdown() noexcept {
    if (state_ == BridgeConnectionState::kStopped) return Status::Ok();
    state_ = BridgeConnectionState::kShuttingDown;
    Status first = Status::Ok();
    if (connection_id_ != transport::kInvalidConnectionId) {
        const Status closed = driver_->Close(connection_id_);
        if (!closed.ok() && closed.code() != StatusCode::kNotFound) first = closed;
        connection_id_ = transport::kInvalidConnectionId;
    }
    if (listener_id_ != transport::kInvalidConnectionId) {
        const Status closed = driver_->Close(listener_id_);
        if (first.ok() && !closed.ok() &&
            closed.code() != StatusCode::kNotFound) {
            first = closed;
        }
        listener_id_ = transport::kInvalidConnectionId;
    }
    pipeline_.reset();
    {
        std::lock_guard lock(egress_mutex_);
        stats_.discarded_egress_frames += egress_queue_.size();
        stats_.discarded_egress_bytes += egress_bytes_;
        egress_queue_.clear();
        egress_bytes_ = 0;
    }
    if (driver_started_by_manager_) {
        const Status stopped = driver_->Shutdown();
        if (first.ok() && !stopped.ok()) first = stopped;
        driver_started_by_manager_ = false;
    }
    local_session_epoch_ = 0;
    remote_session_epoch_ = 0;
    state_ = BridgeConnectionState::kStopped;
    return first;
}

size_t BridgeConnectionManager::FrameCharge(
    const EncodedOutboundFrame& frame) const noexcept {
    const size_t payload = frame.frame.payload.size();
    const size_t artifact = frame.descriptor_artifact.size();
    if (payload > std::numeric_limits<size_t>::max() - artifact ||
        payload + artifact >
            std::numeric_limits<size_t>::max() - kWireMaximumHeaderLength) {
        return std::numeric_limits<size_t>::max();
    }
    return payload + artifact + kWireMaximumHeaderLength;
}

Status BridgeConnectionManager::ValidateOutboundSize(
    const EncodedOutboundFrame& frame,
    const transport::TransportCapabilities& route_capabilities) const noexcept {
    if (frame.frame.header.frame_type != FrameType::kData) {
        return Invalid("bridge egress only accepts data frames");
    }
    if (frame.reliability == registry::Reliability::kReliableOrdered &&
        !driver_->capabilities().features.Has(
            transport::Capability::kRemoteAcceptedConfirmation)) {
        return Status::Error(
            StatusCode::kUnsupported,
            "reliable bridge egress lacks remote-ACK capability");
    }
    MINO_ASSIGN_OR_RETURN(
        auto data_body,
        WireFrameCodec::Encode(frame.frame, options_.pipeline.wire_limits));
    size_t largest_body = data_body.size();
    if (frame.schema_identity.has_value()) {
        MINO_ASSIGN_OR_RETURN(
            auto announcement_payload,
            SchemaControlCodec::EncodeAnnouncement(SchemaAnnouncement(
                1, *frame.schema_identity, frame.descriptor_artifact)));
        WireFrame announcement = ControlFrame(
            FrameType::kSchemaAnnounce, std::move(announcement_payload));
        announcement.header.flags = FlagValue(FrameFlag::kControlFrame);
        MINO_ASSIGN_OR_RETURN(
            auto control_body,
            WireFrameCodec::Encode(announcement,
                                   options_.pipeline.wire_limits));
        largest_body = std::max(largest_body, control_body.size());
    }

    const uint32_t route_limit = route_capabilities.max_frame_size;
    const uint32_t live_limit = driver_->capabilities().max_frame_size;
    uint32_t effective_limit = route_limit;
    if (effective_limit == 0 ||
        (live_limit != 0 && live_limit < effective_limit)) {
        effective_limit = live_limit;
    }
    if (effective_limit != 0 && largest_body > effective_limit) {
        return Exhausted(
            "bridge data or schema control frame exceeds driver frame limit");
    }
    return Status::Ok();
}

Result<uint64_t> BridgeConnectionManager::ReserveEgress(
    EncodedOutboundFrame frame,
    std::shared_ptr<BridgeEgressAdmission> admission) noexcept {
    try {
        MINO_RETURN_IF_ERROR(
            ValidateOutboundSize(frame, driver_->capabilities()));
        const size_t charge = FrameCharge(frame);
        std::lock_guard lock(egress_mutex_);
        const BridgeConnectionState state =
            state_.load(std::memory_order_acquire);
        if (state == BridgeConnectionState::kStopped ||
            state == BridgeConnectionState::kShuttingDown) {
            return Unavailable("bridge egress is not accepting messages");
        }
        if (egress_queue_.size() >= options_.max_egress_frames ||
            charge > options_.max_egress_bytes - egress_bytes_) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "bridge egress queue is full");
        }
        uint64_t reservation_id = next_reservation_id_++;
        if (reservation_id == 0) reservation_id = next_reservation_id_++;
        egress_queue_.push_back(QueuedEgress{
            .reservation_id = reservation_id,
            .frame = std::move(frame),
            .admission = std::move(admission),
        });
        egress_bytes_ += charge;
        return reservation_id;
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge egress allocation failed");
    }
}

Status BridgeConnectionManager::Enqueue(EncodedOutboundFrame frame) noexcept {
    auto reserved = ReserveEgress(std::move(frame), {});
    return reserved.ok() ? Status::Ok() : reserved.status();
}

void BridgeConnectionManager::CancelEgressReservation(
    uint64_t reservation_id) noexcept {
    std::lock_guard lock(egress_mutex_);
    const auto found = std::find_if(
        egress_queue_.begin(), egress_queue_.end(),
        [reservation_id](const QueuedEgress& queued) {
            return queued.reservation_id == reservation_id;
        });
    if (found == egress_queue_.end()) return;
    egress_bytes_ -= FrameCharge(found->frame);
    egress_queue_.erase(found);
}

Result<EncodedOutboundFrame>
BridgeConnectionManager::TryPeekAndEncode() {
    std::lock_guard lock(egress_mutex_);
    const BridgeConnectionState state =
        state_.load(std::memory_order_acquire);
    if (state == BridgeConnectionState::kStopped ||
        state == BridgeConnectionState::kShuttingDown) {
        return Unavailable("bridge egress is not running");
    }
    while (!egress_queue_.empty() && egress_queue_.front().admission != nullptr &&
           egress_queue_.front().admission->rolled_back()) {
        egress_bytes_ -= FrameCharge(egress_queue_.front().frame);
        egress_queue_.pop_front();
    }
    if (egress_queue_.empty()) {
        return Status::Error(StatusCode::kWouldBlock);
    }
    if (egress_queue_.front().admission != nullptr &&
        !egress_queue_.front().admission->committed()) {
        return Status::Error(StatusCode::kWouldBlock,
                             "bridge fanout admission is pending");
    }
    try {
        return egress_queue_.front().frame;
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge egress copy failed");
    }
}

void BridgeConnectionManager::CommitPolled() noexcept {
    std::lock_guard lock(egress_mutex_);
    if (egress_queue_.empty()) return;
    egress_bytes_ -= FrameCharge(egress_queue_.front().frame);
    egress_queue_.pop_front();
}

size_t BridgeConnectionManager::queued_egress_frames() const noexcept {
    std::lock_guard lock(egress_mutex_);
    return egress_queue_.size();
}

size_t BridgeConnectionManager::queued_egress_bytes() const noexcept {
    std::lock_guard lock(egress_mutex_);
    return egress_bytes_;
}

bool BridgeConnectionManager::MatchesRoute(
    NodeId target_node,
    const transport::RemoteTargetRoute& target) const noexcept {
    return options_.peer_route_endpoint.has_value() &&
           target_node == options_.expected_peer.node_id &&
           *options_.peer_route_endpoint == target.endpoint &&
           target.process_identity ==
               options_.expected_peer.process_identity &&
           target.lease_epoch == options_.expected_peer.lease_epoch &&
           target.node_config_version ==
               options_.expected_peer.node_config_version &&
           target.driver_id == options_.route_driver_id &&
           target.driver_generation == options_.route_driver_generation &&
           driver_.get() == target.driver.get();
}

Result<std::shared_ptr<BridgeRuntimeDispatcher>>
BridgeRuntimeDispatcher::Create(
    size_t max_peers,
    std::shared_ptr<BridgeDescriptorProvider> descriptors,
    size_t max_route_bindings) noexcept {
    if (max_peers == 0 || max_peers > transport::kMaxConnections ||
        max_route_bindings == 0) {
        return Invalid("bridge dispatcher bound is invalid");
    }
    try {
        return std::shared_ptr<BridgeRuntimeDispatcher>(
            new BridgeRuntimeDispatcher(max_peers, std::move(descriptors),
                                        max_route_bindings));
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge dispatcher allocation failed");
    }
}

BridgeRuntimeDispatcher::BridgeRuntimeDispatcher(
    size_t max_peers,
    std::shared_ptr<BridgeDescriptorProvider> descriptors,
    size_t max_route_bindings)
    : max_peers_(max_peers),
      descriptors_(std::move(descriptors)),
      max_route_bindings_(max_route_bindings) {
    peers_.reserve(max_peers_);
    route_bindings_.reserve(max_route_bindings_);
}

Status BridgeRuntimeDispatcher::RegisterPeer(
    NodeId node, std::shared_ptr<BridgeConnectionManager> manager) noexcept {
    if (node.value == 0 || manager == nullptr) {
        return Invalid("bridge dispatcher peer is incomplete");
    }
    try {
        std::lock_guard lock(peers_mutex_);
        const auto existing = std::find_if(
            peers_.begin(), peers_.end(),
            [node](const Peer& peer) { return peer.node == node; });
        if (existing != peers_.end()) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "bridge dispatcher peer already exists");
        }
        if (peers_.size() >= max_peers_) {
            return Exhausted("bridge dispatcher peer table is full");
        }
        peers_.push_back(Peer{.node = node, .manager = std::move(manager)});
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge dispatcher peer allocation failed");
    }
}

Status BridgeRuntimeDispatcher::UnregisterPeer(NodeId node) noexcept {
    std::lock_guard lock(peers_mutex_);
    const auto existing = std::find_if(
        peers_.begin(), peers_.end(),
        [node](const Peer& peer) { return peer.node == node; });
    if (existing == peers_.end()) {
        return Status::Error(StatusCode::kNotFound,
                             "bridge dispatcher peer does not exist");
    }
    peers_.erase(existing);
    return Status::Ok();
}

Status BridgeRuntimeDispatcher::Dispatch(
    const BridgeDispatchRequest& request) {
    if (request.route == nullptr) {
        return Invalid("bridge dispatch request has no route");
    }
    return DispatchTargets(
        request, request.route->targets(),
        BridgeRouteContract{
            .stamp = request.route->stamp(),
            .delivery = request.route->delivery(),
            .payload_size = request.route->payload_size(),
            .priority = request.route->priority(),
        });
}

Status BridgeRuntimeDispatcher::ValidateAndBindRoute(
    const BridgeDispatchRequest& request,
    std::span<const transport::TargetRoute> targets,
    const BridgeRouteContract& route) {
    if (route.stamp.topic_id.value == 0 ||
        route.stamp.topic_id != request.topic_id || route.payload_size == 0 ||
        route.payload_size != request.canonical_payload.size() ||
        route.priority != request.priority) {
        return Invalid("bridge request does not match route topic/payload/priority");
    }
    switch (route.stamp.policy) {
        case registry::RoutePolicy::kDiscovery:
        case registry::RoutePolicy::kStatic:
            break;
        default:
            return Invalid("bridge route policy is unknown");
    }
    if (request.schema.short_id() == 0 ||
        request.schema.short_id() !=
            schema::DigestShortId(request.schema.canonical_digest()) ||
        request.schema.schema_version() == 0 ||
        request.schema.layout_version() == 0) {
        return Invalid("bridge request schema is incomplete");
    }

    const auto contract_matches = [&route, targets](
                                      const RouteBinding& binding) {
        return binding.stamp == route.stamp &&
               binding.delivery == route.delivery &&
               binding.payload_size == route.payload_size &&
               binding.priority == route.priority &&
               binding.targets.size() == targets.size() &&
               std::equal(binding.targets.begin(), binding.targets.end(),
                          targets.begin(), TargetRouteEqual);
    };

    std::lock_guard lock(route_bindings_mutex_);
    const auto existing = std::find_if(route_bindings_.begin(),
                                       route_bindings_.end(), contract_matches);
    if (existing != route_bindings_.end()) {
        if (!registry::SchemaIdentityEqual(existing->schema, request.schema)) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "bridge route was rebound to another schema");
        }
        return Status::Ok();
    }
    if (route_bindings_.size() >= max_route_bindings_) {
        return Exhausted("bridge route-schema binding table is full");
    }
    route_bindings_.push_back(RouteBinding{
        .stamp = route.stamp,
        .targets = std::vector<transport::TargetRoute>(targets.begin(),
                                                       targets.end()),
        .delivery = route.delivery,
        .payload_size = route.payload_size,
        .priority = route.priority,
        .schema = request.schema,
    });
    return Status::Ok();
}

Status BridgeRuntimeDispatcher::DispatchTargets(
    const BridgeDispatchRequest& request,
    std::span<const transport::TargetRoute> targets,
    const BridgeRouteContract& route) {
    if (request.topic_id.value == 0 ||
        request.publication.source.node_id == 0 ||
        request.publication.source.publisher_id == 0 ||
        request.publication.source.publisher_epoch == 0 ||
        request.publication.sequence_num == 0) {
        return Invalid("bridge dispatch request is incomplete");
    }
    struct Destination {
        std::shared_ptr<BridgeConnectionManager> manager;
        transport::TransportCapabilities capabilities;
    };
    struct Reservation {
        std::shared_ptr<BridgeConnectionManager> manager;
        uint64_t id = 0;
    };

    try {
        std::vector<Destination> destinations;
        destinations.reserve(targets.size());
        for (const transport::TargetRoute& target : targets) {
            const auto* remote =
                std::get_if<transport::RemoteTargetRoute>(&target.transport);
            if (remote == nullptr) continue;
            if (remote->driver == nullptr) {
                return Unavailable("bridge route has no transport driver");
            }
            if (route.delivery.reliability ==
                    registry::Reliability::kReliableOrdered &&
                (!remote->capabilities.features.Has(
                     transport::Capability::kRemoteAcceptedConfirmation) ||
                 !remote->driver->capabilities().features.Has(
                     transport::Capability::kRemoteAcceptedConfirmation))) {
                return Status::Error(
                    StatusCode::kUnsupported,
                    "reliable bridge route lacks remote-ACK capability");
            }

            std::shared_ptr<BridgeConnectionManager> manager;
            {
                std::lock_guard lock(peers_mutex_);
                const auto peer = std::find_if(
                    peers_.begin(), peers_.end(),
                    [&target](const Peer& candidate) {
                        return candidate.node == target.target_node;
                    });
                if (peer != peers_.end()) manager = peer->manager;
            }
            if (manager == nullptr) {
                return Unavailable(
                    "bridge route has no registered peer manager");
            }
            if (!manager->MatchesRoute(target.target_node, *remote)) {
                return Unavailable("bridge route does not match peer manager");
            }
            destinations.push_back(Destination{
                .manager = std::move(manager),
                .capabilities = remote->capabilities,
            });
        }
        MINO_RETURN_IF_ERROR(ValidateAndBindRoute(request, targets, route));
        if (destinations.empty()) return Status::Ok();

        std::vector<std::byte> artifact;
        if (descriptors_ != nullptr) {
            MINO_ASSIGN_OR_RETURN(
                artifact,
                descriptors_->GetDescriptorArtifact(request.schema));
        }

        WireFrame frame;
        frame.header.frame_type = FrameType::kData;
        frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
        frame.header.topic_id = request.topic_id.value;
        frame.header.msg_type = static_cast<uint32_t>(
            request.schema.short_id() & uint64_t{0xffff'ffff});
        frame.header.schema_version = request.schema.schema_version();
        frame.header.layout_version = request.schema.layout_version();
        frame.header.source_node_id = request.publication.source.node_id;
        frame.header.source_publisher_id =
            request.publication.source.publisher_id;
        frame.header.source_publisher_epoch =
            request.publication.source.publisher_epoch;
        frame.header.sequence_num = request.publication.sequence_num;
        frame.header.timestamp_ns = request.publication.timestamp_ns;
        frame.payload.assign(request.canonical_payload.begin(),
                             request.canonical_payload.end());
        EncodedOutboundFrame outbound{
            .frame = std::move(frame),
            .reliability = route.delivery.reliability,
            .allow_drop = route.delivery.allow_drop,
            .schema_identity = request.schema,
            .descriptor_artifact = std::move(artifact),
        };

        for (const Destination& destination : destinations) {
            MINO_RETURN_IF_ERROR(destination.manager->ValidateOutboundSize(
                outbound, destination.capabilities));
        }

        auto admission = std::make_shared<BridgeEgressAdmission>();
        std::vector<Reservation> reservations;
        reservations.reserve(destinations.size());
        for (const Destination& destination : destinations) {
            auto reserved = destination.manager->ReserveEgress(
                outbound, admission);
            if (!reserved.ok()) {
                admission->RollBack();
                for (const Reservation& prior : reservations) {
                    prior.manager->CancelEgressReservation(prior.id);
                }
                return reserved.status();
            }
            reservations.push_back(Reservation{
                .manager = destination.manager,
                .id = *reserved,
            });
        }
        admission->Commit();
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge dispatch allocation failed");
    }
}

size_t BridgeRuntimeDispatcher::peer_count() const noexcept {
    std::lock_guard lock(peers_mutex_);
    return peers_.size();
}

Result<std::unique_ptr<BridgeListenerHub>> BridgeListenerHub::Create(
    BridgeListenerHubOptions options,
    std::shared_ptr<transport::TransportDriver> driver) noexcept {
    if (driver == nullptr || options.listen_backlog == 0 ||
        options.max_peers == 0 ||
        options.max_peers > transport::kMaxConnections ||
        options.max_pending_handshakes == 0 ||
        options.max_pending_handshakes > transport::kMaxConnections ||
        options.max_accepts_per_pump == 0 ||
        options.handshake_timeout_ns == 0 ||
        options.wire_limits.max_buffered_bytes == 0 ||
        options.wire_limits.max_buffered_bytes >
            transport::kMaxReceiveBatchBytes ||
        options.wire_limits.max_payload_length <
            kWireControlOpcodeLength + kSessionDiscoveryPayloadWireSize) {
        return Invalid("bridge listener hub dependencies or bounds are invalid");
    }
    if (!driver->capabilities().features.Has(
            transport::Capability::kListen)) {
        return Status::Error(StatusCode::kUnsupported,
                             "bridge listener hub driver cannot listen");
    }
    try {
        return std::unique_ptr<BridgeListenerHub>(
            new BridgeListenerHub(std::move(options), std::move(driver)));
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge listener hub allocation failed");
    }
}

BridgeListenerHub::BridgeListenerHub(
    BridgeListenerHubOptions options,
    std::shared_ptr<transport::TransportDriver> driver)
    : options_(std::move(options)), driver_(std::move(driver)) {
    peers_.reserve(options_.max_peers);
    pending_.reserve(options_.max_pending_handshakes);
}

BridgeListenerHub::~BridgeListenerHub() { (void)Shutdown(); }

uint64_t BridgeListenerHub::EffectiveNow(
    uint64_t supplied_now_ns) const noexcept {
    if (supplied_now_ns != 0) return supplied_now_ns;
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

Status BridgeListenerHub::RegisterPeer(
    std::shared_ptr<BridgeConnectionManager> manager) noexcept {
    if (manager == nullptr ||
        manager->options_.mode != BridgeConnectionMode::kAccepted ||
        manager->driver_.get() != driver_.get() ||
        !manager->options_.expected_peer.complete()) {
        return Invalid("bridge listener hub peer manager is incompatible");
    }
    try {
        const BridgeNodeIdentityFence identity =
            manager->options_.expected_peer;
        if (!peers_.empty() &&
            (manager->options_.local_identity !=
                 peers_.front().manager->options_.local_identity ||
             manager->options_.route_driver_id !=
                 peers_.front().manager->options_.route_driver_id ||
             manager->options_.route_driver_generation !=
                 peers_.front().manager->options_.route_driver_generation)) {
            return Invalid(
                "bridge listener hub managers do not share local fencing");
        }
        const auto existing = std::find_if(
            peers_.begin(), peers_.end(),
            [&identity](const Peer& peer) {
                return peer.identity == identity;
            });
        if (existing != peers_.end()) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "bridge listener hub peer already exists");
        }
        if (peers_.size() >= options_.max_peers) {
            return Exhausted("bridge listener hub peer table is full");
        }
        peers_.push_back(Peer{
            .identity = identity,
            .manager = std::move(manager),
        });
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge listener hub peer allocation failed");
    }
}

Status BridgeListenerHub::UnregisterPeer(
    const BridgeNodeIdentityFence& peer) noexcept {
    const auto found = std::find_if(
        peers_.begin(), peers_.end(),
        [&peer](const Peer& candidate) {
            return candidate.identity == peer;
        });
    if (found == peers_.end()) {
        return Status::Error(StatusCode::kNotFound,
                             "bridge listener hub peer does not exist");
    }
    peers_.erase(found);
    return Status::Ok();
}

Status BridgeListenerHub::Start() noexcept {
    if (running_) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "bridge listener hub is already running");
    }
    if (driver_->state() == transport::DriverState::kStopped) {
        if (!options_.manage_driver_lifecycle) {
            return Unavailable("bridge listener hub driver is not running");
        }
        MINO_RETURN_IF_ERROR(driver_->Start(options_.driver_config));
        driver_started_by_hub_ = true;
    } else if (driver_->state() != transport::DriverState::kRunning) {
        return Unavailable("bridge listener hub driver is stopping");
    }
    auto listener = driver_->Listen(transport::ListenRequest{
        .local_endpoint = options_.local_endpoint,
        .backlog = options_.listen_backlog,
    });
    if (!listener.ok()) {
        if (driver_started_by_hub_) {
            (void)driver_->Shutdown();
            driver_started_by_hub_ = false;
        }
        return listener.status();
    }
    listener_id_ = listener->id;
    running_ = true;
    return Status::Ok();
}

void BridgeListenerHub::Reject(
    size_t index, bool timeout,
    BridgeListenerHubPumpResult* result) noexcept {
    (void)driver_->Close(pending_[index].connection.id);
    pending_.erase(pending_.begin() + static_cast<std::ptrdiff_t>(index));
    ++stats_.rejected_connections;
    ++result->rejected;
    if (timeout) ++stats_.handshake_timeouts;
}

Result<BridgeListenerHubPumpResult> BridgeListenerHub::Pump(
    uint64_t now_ns) noexcept {
    try {
        if (!running_) {
            return Unavailable("bridge listener hub is not running");
        }
        const uint64_t now = EffectiveNow(now_ns);
        BridgeListenerHubPumpResult result;
        while (result.accepted < options_.max_accepts_per_pump &&
               pending_.size() < options_.max_pending_handshakes) {
            auto accepted = driver_->Accept(transport::AcceptRequest{
                .listener_id = listener_id_,
                .timeout_ms = 0,
            });
            if (!accepted.ok()) {
                if (IsWouldBlock(accepted.status())) break;
                return accepted.status();
            }
            pending_.push_back(Pending{
                .connection = std::move(*accepted),
                .accepted_ns = now,
                .discovery = std::nullopt,
            });
            ++result.accepted;
            ++stats_.accepted_connections;
        }

        for (size_t index = 0; index < pending_.size();) {
            Pending& pending = pending_[index];
            if (now >= pending.accepted_ns &&
                now - pending.accepted_ns >= options_.handshake_timeout_ns) {
                Reject(index, true, &result);
                continue;
            }
            if (!pending.discovery.has_value()) {
                auto received = driver_->Poll(transport::ReceiveRequest{
                    .max_messages = 1,
                    .max_bytes = options_.wire_limits.max_buffered_bytes,
                    .timeout_ms = 0,
                    .connection_id = pending.connection.id,
                });
                if (!received.ok()) {
                    if (IsWouldBlock(received.status())) {
                        ++index;
                        continue;
                    }
                    Reject(index, false, &result);
                    continue;
                }
                if (received->messages.size() != 1) {
                    Reject(index, false, &result);
                    continue;
                }
                auto frame = WireFrameCodec::Decode(
                    received->messages.front().payload, options_.wire_limits);
                if (!frame.ok() ||
                    frame->header.frame_type != FrameType::kSessionDiscovery) {
                    Reject(index, false, &result);
                    continue;
                }
                auto discovery =
                    ControlPayloadCodec::DecodeSessionDiscovery(frame->payload);
                if (!discovery.ok()) {
                    Reject(index, false, &result);
                    continue;
                }
                pending.discovery = std::move(*discovery);
            }

            const BridgeNodeIdentityFence identity{
                .node_id = pending.discovery->node_id,
                .process_identity = pending.discovery->process_identity,
                .lease_epoch = pending.discovery->lease_epoch,
                .node_config_version =
                    pending.discovery->node_config_version,
            };
            const auto peer = std::find_if(
                peers_.begin(), peers_.end(),
                [&identity](const Peer& candidate) {
                    return candidate.identity == identity;
                });
            if (peer == peers_.end()) {
                Reject(index, false, &result);
                continue;
            }
            const Status adopted = peer->manager->AdoptAcceptedConnection(
                pending.connection, *pending.discovery, now);
            if (!adopted.ok()) {
                if (adopted.code() == StatusCode::kWouldBlock) {
                    ++index;
                    continue;
                }
                Reject(index, false, &result);
                continue;
            }
            pending_.erase(
                pending_.begin() + static_cast<std::ptrdiff_t>(index));
            ++result.dispatched;
            ++stats_.dispatched_connections;
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Exhausted("bridge listener hub pump allocation failed");
    }
}

Status BridgeListenerHub::Shutdown() noexcept {
    if (!running_ && listener_id_ == transport::kInvalidConnectionId) {
        return Status::Ok();
    }
    Status first = Status::Ok();
    for (const Pending& pending : pending_) {
        const Status closed = driver_->Close(pending.connection.id);
        if (first.ok() && !closed.ok() &&
            closed.code() != StatusCode::kNotFound) {
            first = closed;
        }
    }
    pending_.clear();
    if (listener_id_ != transport::kInvalidConnectionId) {
        const Status closed = driver_->Close(listener_id_);
        if (first.ok() && !closed.ok() &&
            closed.code() != StatusCode::kNotFound) {
            first = closed;
        }
        listener_id_ = transport::kInvalidConnectionId;
    }
    running_ = false;
    if (driver_started_by_hub_) {
        const Status stopped = driver_->Shutdown();
        if (first.ok() && !stopped.ok()) first = stopped;
        driver_started_by_hub_ = false;
    }
    return first;
}

}  // namespace mino::bridge
