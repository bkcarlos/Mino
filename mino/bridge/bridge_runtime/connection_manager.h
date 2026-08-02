// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_BRIDGE_RUNTIME_CONNECTION_MANAGER_H_
#define MINO_BRIDGE_BRIDGE_RUNTIME_CONNECTION_MANAGER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <vector>

#include "mino/bridge/bridge_pipeline.h"
#include "mino/common/result.h"
#include "mino/runtime/bus.h"
#include "mino/transport/transport_driver.h"

namespace mino::bridge {

enum class BridgeConnectionMode : uint8_t {
    kConnect = 0,
    kListen = 1,
    // Connection is accepted and identity-routed by BridgeListenerHub.
    kAccepted = 2,
};

enum class BridgeConnectionState : uint8_t {
    kStopped = 0,
    kWaiting = 1,
    kHandshaking = 2,
    kActive = 3,
    kShuttingDown = 4,
};

struct BridgeNodeIdentityFence {
    NodeId node_id;
    ProcessIdentity process_identity;
    uint64_t lease_epoch = 0;
    uint64_t node_config_version = 0;

    bool complete() const noexcept {
        return node_id.value != 0 && !process_identity.IsZero() &&
               process_identity.node_id == node_id.value && lease_epoch != 0 &&
               node_config_version != 0;
    }

    bool operator==(const BridgeNodeIdentityFence&) const = default;
};

struct BridgeConnectionManagerOptions {
    BridgeConnectionMode mode = BridgeConnectionMode::kConnect;
    std::optional<transport::EndpointDescriptor> local_endpoint;
    std::optional<transport::EndpointDescriptor> remote_endpoint;
    // Endpoint advertised by Registry for this peer. For connectors this
    // defaults to remote_endpoint. Listeners should set it when the manager is
    // also used as a Bus dispatch target.
    std::optional<transport::EndpointDescriptor> peer_route_endpoint;

    // Both identities are mandatory. Discovery is accepted only when every
    // peer fencing field exactly matches expected_peer.
    BridgeNodeIdentityFence local_identity;
    BridgeNodeIdentityFence expected_peer;
    // Identity of the local route-driver registration captured by RouteHandle.
    uint64_t route_driver_id = 0;
    uint64_t route_driver_generation = 0;

    transport::DriverConfig driver_config;
    // A driver is normally shared by many peer managers and therefore is not
    // started or stopped by one manager unless exclusivity is explicit.
    bool manage_driver_lifecycle = false;
    uint32_t listen_backlog = 16;
    uint32_t connect_timeout_ms = 100;
    uint64_t handshake_timeout_ns = 5'000'000'000ull;
    uint64_t initial_reconnect_backoff_ns = 10'000'000ull;
    uint64_t max_reconnect_backoff_ns = 5'000'000'000ull;
    uint64_t health_probe_interval_ns = 500'000'000ull;

    size_t max_egress_frames = 4096;
    size_t max_egress_bytes = 64u * 1024u * 1024u;
    BridgePipelineOptions pipeline;
};

struct BridgeConnectionManagerStats {
    uint64_t connection_attempts = 0;
    uint64_t accepted_connections = 0;
    uint64_t completed_handshakes = 0;
    uint64_t reconnects = 0;
    uint64_t disconnects = 0;
    uint64_t protocol_failures = 0;
    uint64_t health_probes = 0;
    uint64_t discarded_egress_frames = 0;
    uint64_t discarded_egress_bytes = 0;
};

struct BridgeConnectionPumpResult {
    BridgeConnectionState state = BridgeConnectionState::kStopped;
    BridgePumpResult pipeline;
    bool connection_opened = false;
    bool connection_lost = false;
    bool handshake_completed = false;
};

// Single-owner connection lifecycle for one remote peer. Start(), Pump(), and
// Shutdown() must be called serially by the owning event loop. Enqueue() is
// thread-safe so Bus publishers may dispatch concurrently with that owner.
class BridgeEgressAdmission;
class BridgeListenerHub;

class BridgeConnectionManager final : public BridgeEgressPort {
public:
    static Result<std::unique_ptr<BridgeConnectionManager>> Create(
        BridgeConnectionManagerOptions options,
        std::shared_ptr<transport::TransportDriver> driver,
        BridgeIngressPort* ingress,
        SchemaNegotiator* schema_negotiator = nullptr) noexcept;

    ~BridgeConnectionManager() override;
    BridgeConnectionManager(const BridgeConnectionManager&) = delete;
    BridgeConnectionManager& operator=(const BridgeConnectionManager&) = delete;

    Status Start(uint64_t now_ns = 0) noexcept;
    Result<BridgeConnectionPumpResult> Pump(
        BridgePumpBudget budget = {}) noexcept;
    Status Shutdown() noexcept;
    // Used only by BridgeListenerHub after it has decoded discovery. The manager
    // independently revalidates identity before taking ownership.
    Status AdoptAcceptedConnection(
        transport::ConnectionInfo connection,
        const SessionDiscovery& peer_discovery,
        uint64_t now_ns) noexcept;

    // Concrete production egress seam. Ownership is copied into a bounded queue
    // and retained across disconnect/backoff until BridgePipeline admits it.
    Status Enqueue(EncodedOutboundFrame frame) noexcept;
    Result<EncodedOutboundFrame> TryPeekAndEncode() override;
    void CommitPolled() noexcept override;

    BridgeConnectionState state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }
    uint64_t local_session_epoch() const noexcept { return local_session_epoch_; }
    uint64_t remote_session_epoch() const noexcept {
        return remote_session_epoch_;
    }
    transport::ConnectionId connection_id() const noexcept {
        return connection_id_;
    }
    size_t queued_egress_frames() const noexcept;
    size_t queued_egress_bytes() const noexcept;
    const Status& last_failure() const noexcept { return last_failure_; }
    const BridgeConnectionManagerStats& stats() const noexcept { return stats_; }
    const BridgePipeline* pipeline() const noexcept { return pipeline_.get(); }

    bool MatchesRoute(NodeId target_node,
                      const transport::RemoteTargetRoute& target) const noexcept;

private:
    friend class BridgeRuntimeDispatcher;
    friend class BridgeListenerHub;

    struct QueuedEgress {
        uint64_t reservation_id = 0;
        EncodedOutboundFrame frame;
        std::shared_ptr<BridgeEgressAdmission> admission;
    };
    BridgeConnectionManager(BridgeConnectionManagerOptions options,
                            std::shared_ptr<transport::TransportDriver> driver,
                            BridgeIngressPort* ingress,
                            SchemaNegotiator* schema_negotiator) noexcept;

    Status OpenListener() noexcept;
    Status BeginConnection(transport::ConnectionInfo connection,
                           uint64_t now_ns) noexcept;
    Status SendDiscoveryHello() noexcept;
    Result<std::optional<SessionDiscovery>> PollDiscoveryHello() noexcept;
    Status CompleteHandshake(uint64_t remote_epoch,
                             uint64_t now_ns) noexcept;
    Status SendHealthProbe(uint64_t now_ns) noexcept;
    void LoseConnection(const Status& failure, uint64_t now_ns,
                        bool protocol_failure,
                        BridgeConnectionPumpResult* result) noexcept;
    void ScheduleRetry(uint64_t now_ns) noexcept;
    uint64_t GenerateNewEpoch() noexcept;
    uint64_t EffectiveNow(uint64_t supplied_now_ns) const noexcept;
    size_t FrameCharge(const EncodedOutboundFrame& frame) const noexcept;
    Status ValidateOutboundSize(
        const EncodedOutboundFrame& frame,
        const transport::TransportCapabilities& route_capabilities) const noexcept;
    Result<uint64_t> ReserveEgress(
        EncodedOutboundFrame frame,
        std::shared_ptr<BridgeEgressAdmission> admission) noexcept;
    void CancelEgressReservation(uint64_t reservation_id) noexcept;

    BridgeConnectionManagerOptions options_;
    std::shared_ptr<transport::TransportDriver> driver_;
    BridgeIngressPort* ingress_;
    SchemaNegotiator* schema_negotiator_;
    std::unique_ptr<BridgePipeline> pipeline_;

    transport::ConnectionId listener_id_ = transport::kInvalidConnectionId;
    transport::ConnectionId connection_id_ = transport::kInvalidConnectionId;
    std::atomic<BridgeConnectionState> state_{
        BridgeConnectionState::kStopped};
    uint64_t local_session_epoch_ = 0;
    uint64_t remote_session_epoch_ = 0;
    uint64_t handshake_started_ns_ = 0;
    uint64_t next_retry_ns_ = 0;
    uint64_t current_backoff_ns_ = 0;
    uint64_t next_probe_ns_ = 0;
    bool discovery_sent_ = false;
    std::optional<SessionDiscovery> adopted_discovery_;
    bool ever_active_ = false;
    bool driver_started_by_manager_ = false;
    Status last_failure_ = Status::Ok();
    BridgeConnectionManagerStats stats_;

    mutable std::mutex egress_mutex_;
    std::deque<QueuedEgress> egress_queue_;
    size_t egress_bytes_ = 0;
    uint64_t next_reservation_id_ = 1;
};

class BridgeEgressAdmission final {
public:
    bool committed() const noexcept {
        return state_.load(std::memory_order_acquire) == State::kCommitted;
    }
    bool rolled_back() const noexcept {
        return state_.load(std::memory_order_acquire) == State::kRolledBack;
    }

private:
    friend class BridgeRuntimeDispatcher;
    enum class State : uint8_t { kPending, kCommitted, kRolledBack };
    void Commit() noexcept {
        state_.store(State::kCommitted, std::memory_order_release);
    }
    void RollBack() noexcept {
        state_.store(State::kRolledBack, std::memory_order_release);
    }
    std::atomic<State> state_{State::kPending};
};

class BridgeDescriptorProvider {
public:
    virtual ~BridgeDescriptorProvider() = default;
    // Dispatch may call this concurrently from multiple Bus publisher threads.
    // Implementations must be thread-safe and return an owned bounded artifact.
    virtual Result<std::vector<std::byte>> GetDescriptorArtifact(
        const schema::SchemaIdentity& identity) = 0;
};

struct BridgeRouteContract {
    transport::RouteStamp stamp;
    registry::DeliveryPolicy delivery;
    uint32_t payload_size = 0;
    uint8_t priority = 0;
};

// Concrete Bus bridge wiring. It fans each immutable remote TargetRoute into the
// registered peer manager while preserving Bus publication identity and policy.
// Local targets remain on the Bus local path and are intentionally ignored.
class BridgeRuntimeDispatcher final : public ::mino::BridgeDispatcher {
public:
    static Result<std::shared_ptr<BridgeRuntimeDispatcher>> Create(
        size_t max_peers = 1024,
        std::shared_ptr<BridgeDescriptorProvider> descriptors = {},
        size_t max_route_bindings = 4096) noexcept;

    Status RegisterPeer(NodeId node,
                        std::shared_ptr<BridgeConnectionManager> manager) noexcept;
    Status UnregisterPeer(NodeId node) noexcept;
    Status Dispatch(const BridgeDispatchRequest& request) override;
    // Synchronous lower-level adapter used by Dispatch after unwrapping the
    // immutable RouteHandle. It is also useful to non-Bus producers that already
    // hold a fenced target snapshot.
    Status DispatchTargets(
        const BridgeDispatchRequest& request,
        std::span<const transport::TargetRoute> targets,
        const BridgeRouteContract& route);
    size_t peer_count() const noexcept;

private:
    struct Peer {
        NodeId node;
        std::shared_ptr<BridgeConnectionManager> manager;
    };

    // Stable contract key. Target resource references are retained once per
    // distinct contract; the per-publication RouteHandle is never retained.
    struct RouteBinding {
        transport::RouteStamp stamp;
        std::vector<transport::TargetRoute> targets;
        registry::DeliveryPolicy delivery;
        uint32_t payload_size = 0;
        uint8_t priority = 0;
        schema::SchemaIdentity schema{0, {}, 0, 0};
    };

    BridgeRuntimeDispatcher(
        size_t max_peers,
        std::shared_ptr<BridgeDescriptorProvider> descriptors,
        size_t max_route_bindings);
    Status ValidateAndBindRoute(
        const BridgeDispatchRequest& request,
        std::span<const transport::TargetRoute> targets,
        const BridgeRouteContract& route);

    size_t max_peers_;
    std::shared_ptr<BridgeDescriptorProvider> descriptors_;
    mutable std::mutex peers_mutex_;
    std::vector<Peer> peers_;
    size_t max_route_bindings_;
    mutable std::mutex route_bindings_mutex_;
    std::vector<RouteBinding> route_bindings_;
};

struct BridgeListenerHubOptions {
    transport::EndpointDescriptor local_endpoint;
    transport::DriverConfig driver_config;
    bool manage_driver_lifecycle = false;
    uint32_t listen_backlog = 64;
    size_t max_peers = 1024;
    size_t max_pending_handshakes = 256;
    uint32_t max_accepts_per_pump = 16;
    uint64_t handshake_timeout_ns = 5'000'000'000ull;
    WireFrameLimits wire_limits;
};

struct BridgeListenerHubStats {
    uint64_t accepted_connections = 0;
    uint64_t dispatched_connections = 0;
    uint64_t rejected_connections = 0;
    uint64_t handshake_timeouts = 0;
};

struct BridgeListenerHubPumpResult {
    size_t accepted = 0;
    size_t dispatched = 0;
    size_t rejected = 0;
};

// One listener/acceptor shared by many inbound peer managers on the same driver
// and endpoint. It consumes exactly one SessionDiscovery before dispatching the
// connection by full expected peer identity.
class BridgeListenerHub final {
public:
    static Result<std::unique_ptr<BridgeListenerHub>> Create(
        BridgeListenerHubOptions options,
        std::shared_ptr<transport::TransportDriver> driver) noexcept;
    ~BridgeListenerHub();

    Status RegisterPeer(
        std::shared_ptr<BridgeConnectionManager> manager) noexcept;
    Status UnregisterPeer(const BridgeNodeIdentityFence& peer) noexcept;
    Status Start() noexcept;
    Result<BridgeListenerHubPumpResult> Pump(uint64_t now_ns = 0) noexcept;
    Status Shutdown() noexcept;

    const BridgeListenerHubStats& stats() const noexcept { return stats_; }
    size_t pending_handshakes() const noexcept { return pending_.size(); }

private:
    struct Peer {
        BridgeNodeIdentityFence identity;
        std::shared_ptr<BridgeConnectionManager> manager;
    };
    struct Pending {
        transport::ConnectionInfo connection;
        uint64_t accepted_ns = 0;
        std::optional<SessionDiscovery> discovery;
    };

    BridgeListenerHub(BridgeListenerHubOptions options,
                      std::shared_ptr<transport::TransportDriver> driver);
    uint64_t EffectiveNow(uint64_t supplied_now_ns) const noexcept;
    void Reject(size_t index, bool timeout,
                BridgeListenerHubPumpResult* result) noexcept;

    BridgeListenerHubOptions options_;
    std::shared_ptr<transport::TransportDriver> driver_;
    transport::ConnectionId listener_id_ = transport::kInvalidConnectionId;
    bool running_ = false;
    bool driver_started_by_hub_ = false;
    std::vector<Peer> peers_;
    std::vector<Pending> pending_;
    BridgeListenerHubStats stats_;
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_BRIDGE_RUNTIME_CONNECTION_MANAGER_H_
