// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/remote_bridge.h"

#include <algorithm>
#include <chrono>
#include <new>
#include <string_view>
#include <utility>

#include "mino/common/status.h"

namespace mino::deployment {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

class SteadyCoordinatorTopicAuthorizerClock final
    : public CoordinatorTopicAuthorizerClock {
public:
    uint64_t NowNs() const noexcept override {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }
};

bool SameIdentity(const schema::SchemaIdentity& lhs,
                  const schema::SchemaIdentity& rhs) noexcept {
    return lhs.short_id() == rhs.short_id() &&
           lhs.canonical_digest() == rhs.canonical_digest() &&
           lhs.schema_version() == rhs.schema_version() &&
           lhs.layout_version() == rhs.layout_version();
}

Status ValidateCompositionLimits(
    const RemoteBridgeConfig& config, bool allow_plaintext_for_testing,
    const transport::RdmaDriverOptions* rdma = nullptr,
    const transport::FabricDriverOptions* fabric = nullptr) {
    constexpr size_t kAnnouncementPayloadOverhead =
        bridge::kWireControlOpcodeLength +
        bridge::kSchemaAnnouncementFixedPayloadBytes;
    constexpr size_t kAnnouncementBodyOverhead =
        bridge::kWireBaseHeaderLength + kAnnouncementPayloadOverhead;
    const auto& negotiation = config.schema_negotiation;
    const auto& pipeline = config.connection.pipeline;
    const bool tls_enabled = config.tcp.tls_factory != nullptr;
    if (rdma != nullptr && fabric != nullptr) {
        return Invalid("remote Bridge transport selection is ambiguous");
    }
    if (config.connection.topic_authorizer == nullptr) {
        return Invalid("remote Bridge Topic ACL authorizer is missing");
    }
    if (rdma == nullptr && fabric == nullptr) {
        if (!tls_enabled && !allow_plaintext_for_testing) {
            return Status::Error(
                StatusCode::kPermissionDenied,
                "remote Bridge TLS is required by production configuration");
        }
        if ((!tls_enabled && config.connection.require_authenticated_peer) ||
            (tls_enabled &&
             config.connection.expected_peer_security_domain.value == 0)) {
            return Invalid("remote Bridge TLS identity configuration is invalid");
        }
    } else if (rdma != nullptr) {
        MINO_RETURN_IF_ERROR(
            transport::ValidateRdmaDriverOptions(*rdma, /*production=*/true));
        if (config.connection.expected_peer_security_domain.value == 0) {
            return Invalid("remote Bridge RDMA peer security domain is missing");
        }
    } else {
        MINO_RETURN_IF_ERROR(
            transport::ValidateFabricDriverOptions(*fabric, /*production=*/true));
        if (config.connection.expected_peer_security_domain.value == 0) {
            return Invalid("remote Bridge Fabric peer security domain is missing");
        }
    }
    if (config.tcp_lane_count == 0 ||
        config.tcp_lane_count > bridge::kMaxBridgeLaneCount ||
        config.connection.lane_index != 0 ||
        config.connection.lane_count != 1) {
        return Invalid("remote Bridge TCP lane configuration is invalid");
    }
    if (config.connection.mode ==
            bridge::BridgeConnectionMode::kAccepted ||
        config.connection.driver_config.max_connections <
            config.tcp_lane_count) {
        return Invalid("remote Bridge mode or connection capacity is invalid");
    }
    if (negotiation.max_control_frame_bytes < kAnnouncementBodyOverhead) {
        return Invalid("remote Bridge schema control frame limit is too small");
    }
    const size_t accepted_artifact_bytes = std::min(
        negotiation.max_descriptor_bytes,
        negotiation.max_control_frame_bytes - kAnnouncementBodyOverhead);
    const size_t payload_bytes =
        kAnnouncementPayloadOverhead + accepted_artifact_bytes;
    const size_t body_bytes =
        bridge::kWireBaseHeaderLength + payload_bytes;
    const size_t transport_frame_bytes =
        rdma != nullptr
            ? rdma->max_message_bytes
            : (fabric != nullptr ? fabric->max_message_bytes
                                 : config.tcp.max_frame_body_bytes);
    if (payload_bytes > pipeline.wire_limits.max_payload_length ||
        body_bytes > transport_frame_bytes ||
        body_bytes > pipeline.max_control_bytes ||
        bridge::kLengthPrefixSize + body_bytes >
            pipeline.wire_limits.max_buffered_bytes) {
        return Invalid(
            "remote Bridge TCP, wire, control, and schema limits are incompatible");
    }
    return Status::Ok();
}

}  // namespace

CoordinatorTopicAuthorizer::CoordinatorTopicAuthorizer(
    std::shared_ptr<const registry::Coordinator> coordinator)
    : CoordinatorTopicAuthorizer(
          std::move(coordinator),
          std::make_shared<SteadyCoordinatorTopicAuthorizerClock>()) {}

CoordinatorTopicAuthorizer::CoordinatorTopicAuthorizer(
    std::shared_ptr<const registry::Coordinator> coordinator,
    std::shared_ptr<const CoordinatorTopicAuthorizerClock> clock) noexcept
    : coordinator_(std::move(coordinator)), clock_(std::move(clock)) {}

Status CoordinatorTopicAuthorizer::Deny(Status status) const noexcept {
    denied_total_.fetch_add(1, std::memory_order_relaxed);
    return status;
}

Status CoordinatorTopicAuthorizer::AuthorizeInbound(
    const security::AuthenticatedPeer& peer, TopicId topic_id) const noexcept {
    if (coordinator_ == nullptr || clock_ == nullptr || !peer.complete() ||
        topic_id.value == 0) {
        return Deny(Status::Error(StatusCode::kPermissionDenied,
                                  "inbound Topic ACL context is incomplete"));
    }
    const uint64_t now_ns = clock_->NowNs();
    auto node = coordinator_->GetNode(peer.node_id);
    if (!node.ok() || (*node)->security_domain_id != peer.security_domain ||
        (*node)->lease_state != registry::NodeLeaseState::kActive ||
        (*node)->liveness != ProcessIdentityLiveness::kAlive ||
        (*node)->health != registry::NodeHealth::kHealthy ||
        now_ns >= (*node)->lease_deadline_ns) {
        return Deny(Status::Error(
            StatusCode::kPermissionDenied,
            "TLS principal does not match a live healthy Registry node lease"));
    }
    auto topic = coordinator_->GetTopic(topic_id);
    if (!topic.ok()) {
        return Deny(Status::Error(StatusCode::kPermissionDenied,
                                  "inbound Topic is not authorized"));
    }
    if ((*topic)->metadata.state != registry::TopicState::kActive) {
        return Deny(Status::Error(StatusCode::kPermissionDenied,
                                  "inbound Topic is not active"));
    }
    const Status publish = registry::ValidateTopicPermission(
        (*topic)->metadata, peer.security_domain, peer.node_id,
        registry::TopicPermission::kPublish);
    if (!publish.ok()) return Deny(publish);
    const Status bridge = registry::ValidateTopicPermission(
        (*topic)->metadata, peer.security_domain, peer.node_id,
        registry::TopicPermission::kBridge);
    return bridge.ok() ? bridge : Deny(bridge);
}

Result<capacity::ResourceVector> EstimateRemoteBridgeResourcesImpl(
    const RemoteBridgeConfig& config, bool allow_plaintext_for_testing,
    const transport::RdmaDriverOptions* rdma = nullptr,
    const transport::FabricDriverOptions* fabric = nullptr) noexcept {
    MINO_RETURN_IF_ERROR(ValidateCompositionLimits(
        config, allow_plaintext_for_testing, rdma, fabric));
    capacity::ResourceVector resources;
    resources.bridge_connections = config.tcp_lane_count;
    resources.threads = 1;  // One shared transport progress owner.

    capacity::ResourceVector egress;
    // Application egress and TCP driver buffers are shared by the logical peer
    // and therefore charged once, not once per lane.
    egress.bridge_egress_bytes = config.connection.max_egress_bytes;
    MINO_ASSIGN_OR_RETURN(resources,
                          capacity::CheckedAdd(resources, egress));
    egress.bridge_egress_bytes =
        rdma != nullptr
            ? rdma->max_queued_send_bytes
            : (fabric != nullptr ? fabric->max_queued_receive_bytes
                                 : config.tcp.max_total_send_buffer_bytes);
    MINO_ASSIGN_OR_RETURN(resources,
                          capacity::CheckedAdd(resources, egress));
    egress.bridge_egress_bytes =
        (rdma == nullptr && fabric == nullptr)
            ? config.tcp.max_control_send_buffer_bytes
            : 0;
    MINO_ASSIGN_OR_RETURN(resources,
                          capacity::CheckedAdd(resources, egress));

    // Retransmit and schema state are connection-local and must be charged for
    // every lane.
    for (uint16_t lane = 0; lane < config.tcp_lane_count; ++lane) {
        egress.bridge_egress_bytes =
            config.connection.pipeline.retransmit.max_bytes;
        MINO_ASSIGN_OR_RETURN(resources,
                              capacity::CheckedAdd(resources, egress));
        capacity::ResourceVector schema_buffers;
        schema_buffers.schema_buffer_bytes =
            config.schema_negotiation.max_buffered_bytes;
        MINO_ASSIGN_OR_RETURN(resources,
                              capacity::CheckedAdd(resources, schema_buffers));
        schema_buffers.schema_buffer_bytes =
            config.schema_negotiation.max_descriptor_bytes;
        MINO_ASSIGN_OR_RETURN(resources,
                              capacity::CheckedAdd(resources, schema_buffers));
    }

    capacity::ResourceVector descriptors;
    descriptors.file_descriptors = config.connection.driver_config.max_connections;
    MINO_ASSIGN_OR_RETURN(resources,
                          capacity::CheckedAdd(resources, descriptors));
    descriptors.file_descriptors = config.connection.driver_config.max_listeners;
    MINO_ASSIGN_OR_RETURN(resources,
                          capacity::CheckedAdd(resources, descriptors));
    return resources;
}

Result<capacity::ResourceVector> EstimateRemoteBridgeResources(
    const RemoteBridgeConfig& config) noexcept {
    return EstimateRemoteBridgeResourcesImpl(config, false);
}

Result<capacity::ResourceVector> EstimateRemoteBridgeResourcesForTesting(
    const RemoteBridgeConfig& config) noexcept {
    return EstimateRemoteBridgeResourcesImpl(config, true);
}

class RemoteBridge::StorePersistence final
    : public bridge::DescriptorPersistence {
public:
    explicit StorePersistence(storage::SchemaStore* store) noexcept
        : store_(store) {}

    Status Persist(
        const schema::SchemaIdentity& identity,
        std::span<const std::byte> descriptor_artifact) override {
        if (store_ == nullptr) {
            return Status::Error(StatusCode::kUnavailable,
                                 "remote Bridge schema store is unavailable");
        }
        auto persisted = store_->Persist(identity, descriptor_artifact);
        return persisted.ok() ? Status::Ok() : persisted.status();
    }

private:
    storage::SchemaStore* store_;
};

Result<std::unique_ptr<RemoteBridge>> RemoteBridge::Create(
    RemoteBridgeConfig config, bridge::BridgeIngressPort* ingress,
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
    std::shared_ptr<const registry::Coordinator> coordinator,
    std::shared_ptr<capacity::CapacityController> capacity_controller,
    std::optional<capacity::ResourceVector> capacity_charge) noexcept {
    try {
        if (!coordinator) {
            return Invalid("remote Bridge Coordinator is null");
        }
        config.connection.topic_authorizer =
            std::make_shared<CoordinatorTopicAuthorizer>(
                std::move(coordinator));
        return CreateImpl(std::move(config), ingress,
                          std::move(descriptor_auth),
                          std::move(capacity_controller), capacity_charge,
                          false, std::nullopt);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "remote Bridge allocation failed");
    }
}

Result<std::unique_ptr<RemoteBridge>> RemoteBridge::CreateRdma(
    RemoteBridgeConfig config, transport::RdmaDriverOptions rdma,
    bridge::BridgeIngressPort* ingress,
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
    std::shared_ptr<const registry::Coordinator> coordinator,
    std::shared_ptr<capacity::CapacityController> capacity_controller,
    std::optional<capacity::ResourceVector> capacity_charge) noexcept {
    try {
        if (!coordinator) return Invalid("remote Bridge Coordinator is null");
        config.connection.topic_authorizer =
            std::make_shared<CoordinatorTopicAuthorizer>(std::move(coordinator));
        return CreateImpl(std::move(config), ingress,
                          std::move(descriptor_auth),
                          std::move(capacity_controller), capacity_charge,
                          false, std::move(rdma));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "remote RDMA Bridge allocation failed");
    }
}

Result<std::unique_ptr<RemoteBridge>> RemoteBridge::CreateFabric(
    RemoteBridgeConfig config, transport::FabricDriverOptions fabric,
    bridge::BridgeIngressPort* ingress,
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
    std::shared_ptr<const registry::Coordinator> coordinator,
    std::shared_ptr<capacity::CapacityController> capacity_controller,
    std::optional<capacity::ResourceVector> capacity_charge) noexcept {
    try {
        if (!coordinator) return Invalid("remote Bridge Coordinator is null");
        config.connection.topic_authorizer =
            std::make_shared<CoordinatorTopicAuthorizer>(std::move(coordinator));
        return CreateImpl(std::move(config), ingress,
                          std::move(descriptor_auth),
                          std::move(capacity_controller), capacity_charge,
                          false, std::nullopt, std::move(fabric));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "remote Fabric Bridge allocation failed");
    }
}

Result<std::unique_ptr<RemoteBridge>> RemoteBridge::CreateImpl(
    RemoteBridgeConfig config, bridge::BridgeIngressPort* ingress,
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
    std::shared_ptr<capacity::CapacityController> capacity_controller,
    std::optional<capacity::ResourceVector> capacity_charge,
    bool allow_plaintext_for_testing,
    std::optional<transport::RdmaDriverOptions> rdma,
    std::optional<transport::FabricDriverOptions> fabric) noexcept {
    try {
        if (ingress == nullptr || descriptor_auth == nullptr) {
            return Invalid("remote Bridge ingress or descriptor auth is null");
        }
        if (config.schema_store_root.empty()) {
            return Invalid("remote Bridge schema store root is empty");
        }
        MINO_RETURN_IF_ERROR(ValidateCompositionLimits(
            config, allow_plaintext_for_testing,
            rdma.has_value() ? &*rdma : nullptr,
            fabric.has_value() ? &*fabric : nullptr));
        // TLS and device/fabric attestation both bind a complete principal;
        // callers cannot silently downgrade manager identity or Topic ACL checks.
        config.connection.require_authenticated_peer =
            rdma.has_value() || fabric.has_value() ||
            config.tcp.tls_factory != nullptr;

        capacity::CapacityReservation capacity_reservation;
        if (capacity_controller) {
            capacity::ResourceVector charge;
            if (capacity_charge.has_value()) {
                charge = *capacity_charge;
            } else {
                MINO_ASSIGN_OR_RETURN(charge,
                                      EstimateRemoteBridgeResourcesImpl(
                                          config, allow_plaintext_for_testing,
                                          rdma.has_value() ? &*rdma : nullptr,
                                          fabric.has_value() ? &*fabric : nullptr));
            }
            MINO_ASSIGN_OR_RETURN(
                capacity_reservation,
                capacity_controller->Reserve(capacity::ResourceRequest{
                    .resources = charge,
                    .scope = capacity::ResourceScope::kBridge,
                    .admission_class = capacity::AdmissionClass::kDataPlane,
                    .name = "remote Bridge",
                }));
        }

        auto registry = std::make_unique<schema::SchemaRegistry>();
        MINO_ASSIGN_OR_RETURN(
            auto store,
            storage::SchemaStore::Open(config.schema_store_root,
                                       registry.get()));
        MINO_RETURN_IF_ERROR(store->HydrateRegistry());
        auto persistence =
            std::make_unique<StorePersistence>(store.get());
        std::shared_ptr<transport::TransportDriver> driver;
        if (rdma.has_value()) {
            MINO_ASSIGN_OR_RETURN(auto created_driver,
                                  transport::RdmaDriver::Create(std::move(*rdma)));
            driver = std::shared_ptr<transport::RdmaDriver>(
                std::move(created_driver));
        } else if (fabric.has_value()) {
            MINO_ASSIGN_OR_RETURN(
                auto created_driver,
                transport::FabricWindowDriver::Create(std::move(*fabric)));
            driver = std::shared_ptr<transport::FabricWindowDriver>(
                std::move(created_driver));
        } else {
            MINO_ASSIGN_OR_RETURN(auto created_driver,
                                  transport::TcpDriver::Create(config.tcp));
            driver = std::shared_ptr<transport::TcpDriver>(
                std::move(created_driver));
        }

        auto topic_authorizer =
            std::dynamic_pointer_cast<const CoordinatorTopicAuthorizer>(
                config.connection.topic_authorizer);
        const bridge::BridgeConnectionMode configured_mode =
            config.connection.mode;
        std::vector<std::unique_ptr<bridge::SchemaNegotiator>> negotiators;
        std::vector<std::shared_ptr<bridge::BridgeConnectionManager>> managers;
        negotiators.reserve(config.tcp_lane_count);
        managers.reserve(config.tcp_lane_count);
        for (uint16_t lane = 0; lane < config.tcp_lane_count; ++lane) {
            auto negotiator = std::make_unique<bridge::SchemaNegotiator>(
                registry.get(), descriptor_auth.get(), persistence.get(),
                config.schema_negotiation);
            bridge::BridgeConnectionManagerOptions lane_options =
                config.connection;
            lane_options.manage_driver_lifecycle = false;
            lane_options.lane_index = lane;
            lane_options.lane_count = config.tcp_lane_count;
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
                managers, config.connection.max_egress_frames,
                config.connection.max_egress_bytes));

        std::unique_ptr<bridge::BridgeListenerHub> listener_hub;
        if (configured_mode == bridge::BridgeConnectionMode::kListen) {
            if (!config.connection.local_endpoint.has_value()) {
                return Invalid("remote Bridge listener has no local endpoint");
            }
            MINO_ASSIGN_OR_RETURN(
                listener_hub,
                bridge::BridgeListenerHub::Create(
                    bridge::BridgeListenerHubOptions{
                        .local_endpoint = *config.connection.local_endpoint,
                        .driver_config = config.connection.driver_config,
                        .manage_driver_lifecycle = false,
                        .listen_backlog = config.connection.listen_backlog,
                        .max_peers = 1,
                        .max_pending_handshakes = std::max<size_t>(
                            config.tcp_lane_count,
                            config.connection.listen_backlog),
                        .max_accepts_per_pump = config.tcp_lane_count,
                        .handshake_timeout_ns =
                            config.connection.handshake_timeout_ns,
                        .wire_limits = config.connection.pipeline.wire_limits,
                    },
                    driver));
            for (const auto& manager : managers) {
                MINO_RETURN_IF_ERROR(listener_hub->RegisterPeer(manager));
            }
        }

        MINO_ASSIGN_OR_RETURN(auto capacity_lease,
                              capacity_reservation.Commit());
        return std::unique_ptr<RemoteBridge>(new RemoteBridge(
            std::move(capacity_lease), std::move(registry), std::move(store),
            std::move(descriptor_auth), std::move(persistence),
            std::move(negotiators), std::move(driver), std::move(pool),
            std::move(listener_hub), std::move(topic_authorizer),
            config.connection.driver_config));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "remote Bridge composition allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "remote Bridge composition failed");
    }
}

RemoteBridge::RemoteBridge(
    capacity::CapacityLease capacity_lease,
    std::unique_ptr<schema::SchemaRegistry> registry,
    std::unique_ptr<storage::SchemaStore> store,
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
    std::unique_ptr<StorePersistence> persistence,
    std::vector<std::unique_ptr<bridge::SchemaNegotiator>> negotiators,
    std::shared_ptr<transport::TransportDriver> driver,
    std::shared_ptr<bridge::BridgeConnectionPool> pool,
    std::unique_ptr<bridge::BridgeListenerHub> listener_hub,
    std::shared_ptr<const CoordinatorTopicAuthorizer> topic_authorizer,
    transport::DriverConfig driver_config) noexcept
    : capacity_lease_(std::move(capacity_lease)),
      registry_(std::move(registry)),
      store_(std::move(store)),
      descriptor_auth_(std::move(descriptor_auth)),
      persistence_(std::move(persistence)),
      negotiators_(std::move(negotiators)),
      driver_(std::move(driver)),
      pool_(std::move(pool)),
      listener_hub_(std::move(listener_hub)),
      topic_authorizer_(std::move(topic_authorizer)),
      driver_config_(driver_config) {}

RemoteBridge::~RemoteBridge() { static_cast<void>(Shutdown()); }

Status RemoteBridge::Start(uint64_t now_ns) noexcept {
    if (started_) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "remote Bridge is already started");
    }
    MINO_RETURN_IF_ERROR(driver_->Start(driver_config_));
    const Status pool_started = pool_->Start(now_ns);
    if (!pool_started.ok()) {
        static_cast<void>(driver_->Shutdown());
        return pool_started;
    }
    if (listener_hub_ != nullptr) {
        const Status listener_started = listener_hub_->Start();
        if (!listener_started.ok()) {
            static_cast<void>(pool_->Shutdown());
            static_cast<void>(driver_->Shutdown());
            return listener_started;
        }
    }
    started_ = true;
    return Status::Ok();
}

Result<bridge::BridgeConnectionPumpResult> RemoteBridge::Pump(
    bridge::BridgePumpBudget budget) noexcept {
    if (!started_) {
        return Status::Error(StatusCode::kUnavailable,
                             "remote Bridge is not started");
    }
    if (listener_hub_ != nullptr) {
        auto accepted = listener_hub_->Pump(budget.now_ns);
        if (!accepted.ok()) return accepted.status();
    }
    auto pumped = pool_->Pump(budget);
    if (!pumped.ok()) return pumped.status();
    uint64_t connected = 0;
    uint64_t connections = 0;
    uint64_t disconnects = 0;
    uint64_t reconnects = 0;
    uint64_t reconnect_failures = 0;
    uint64_t protocol_failures = 0;
    for (const auto& manager : pool_->managers()) {
        if (manager->state() == bridge::BridgeConnectionState::kActive) {
            ++connected;
        }
        const bridge::BridgeConnectionManagerStats& stats = manager->stats();
        connections += stats.completed_handshakes;
        disconnects += stats.disconnects;
        reconnects += stats.reconnects;
        reconnect_failures += stats.connection_failures;
        protocol_failures += stats.protocol_failures;
    }
    connected_connections_.store(connected, std::memory_order_relaxed);
    connections_.store(connections, std::memory_order_relaxed);
    disconnects_.store(disconnects, std::memory_order_relaxed);
    reconnects_.store(reconnects, std::memory_order_relaxed);
    reconnect_failures_.store(reconnect_failures, std::memory_order_relaxed);
    protocol_failures_.store(protocol_failures, std::memory_order_relaxed);
    return pumped;
}

Status RemoteBridge::Shutdown() noexcept {
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
    connected_connections_.store(0, std::memory_order_relaxed);
    started_ = false;
    return first;
}

Result<std::vector<schema::SchemaHandle>>
RemoteBridge::RegisterLocalDescriptor(
    std::span<const std::byte> descriptor_artifact) noexcept {
    try {
        if (pool_->state() != bridge::BridgeConnectionState::kStopped) {
            return Status::Error(
                StatusCode::kUnavailable,
                "local descriptors must be registered before Bridge Start");
        }
        if (descriptor_artifact.empty()) {
            return Invalid("local descriptor artifact is empty");
        }
        MINO_ASSIGN_OR_RETURN(
            auto validated,
            registry_->ValidateDescriptorArtifact(descriptor_artifact));
        if (validated.descriptors().empty()) {
            return Invalid("local descriptor artifact has no schemas");
        }

        std::vector<schema::SchemaHandle> descriptors(
            validated.descriptors().begin(), validated.descriptors().end());
        std::map<schema::CanonicalDigest, std::vector<std::byte>> next =
            local_artifacts_;
        for (const schema::SchemaHandle& descriptor : descriptors) {
            if (descriptor == nullptr) {
                return Invalid("local descriptor artifact contains null schema");
            }
            std::vector<std::byte> owned(descriptor_artifact.begin(),
                                         descriptor_artifact.end());
            auto [found, inserted] = next.emplace(
                descriptor->identity().canonical_digest(), std::move(owned));
            if (!inserted &&
                !std::equal(found->second.begin(), found->second.end(),
                            descriptor_artifact.begin(),
                            descriptor_artifact.end())) {
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "local schema digest has different descriptor bytes");
            }
        }
        MINO_ASSIGN_OR_RETURN(
            auto published,
            registry_->PublishDescriptorArtifact(std::move(validated)));
        static_cast<void>(published);
        local_artifacts_.swap(next);
        return descriptors;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "local descriptor registration allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "local descriptor registration failed");
    }
}

RemoteBridgeOperationalStats RemoteBridge::OperationalStats() const noexcept {
    return RemoteBridgeOperationalStats{
        .configured_connections = pool_->lane_count(),
        .connected_connections =
            connected_connections_.load(std::memory_order_relaxed),
        .connections = connections_.load(std::memory_order_relaxed),
        .disconnects = disconnects_.load(std::memory_order_relaxed),
        .reconnects = reconnects_.load(std::memory_order_relaxed),
        .reconnect_failures =
            reconnect_failures_.load(std::memory_order_relaxed),
        .protocol_failures =
            protocol_failures_.load(std::memory_order_relaxed),
        .queued_egress_bytes = pool_->queued_egress_bytes(),
        .acl_denials = topic_authorizer_ == nullptr
                           ? 0
                           : topic_authorizer_->denied_total(),
    };
}

Status RemoteBridge::Enqueue(bridge::WireFrame frame,
                             const schema::SchemaIdentity& identity,
                             registry::Reliability reliability,
                             bool allow_drop) noexcept {
    try {
        const auto artifact =
            local_artifacts_.find(identity.canonical_digest());
        if (artifact == local_artifacts_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "local Bridge schema is not registered");
        }
        auto registered = registry_->Find(identity);
        if (!registered.ok()) return registered.status();
        if (!SameIdentity((*registered)->identity(), identity)) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "local Bridge schema identity mismatch");
        }
        frame.header.frame_type = bridge::FrameType::kData;
        frame.header.msg_type = static_cast<uint32_t>(identity.short_id());
        frame.header.schema_version = identity.schema_version();
        frame.header.layout_version = identity.layout_version();
        return pool_->Enqueue(bridge::EncodedOutboundFrame{
            .frame = std::move(frame),
            .reliability = reliability,
            .allow_drop = allow_drop,
            .schema_identity = identity,
            .descriptor_artifact = artifact->second,
        });
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "remote Bridge enqueue allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "remote Bridge enqueue failed");
    }
}

}  // namespace mino::deployment
