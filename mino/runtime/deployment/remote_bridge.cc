// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/remote_bridge.h"

#include <algorithm>
#include <new>
#include <string_view>
#include <utility>

#include "mino/common/status.h"

namespace mino::deployment {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool SameIdentity(const schema::SchemaIdentity& lhs,
                  const schema::SchemaIdentity& rhs) noexcept {
    return lhs.short_id() == rhs.short_id() &&
           lhs.canonical_digest() == rhs.canonical_digest() &&
           lhs.schema_version() == rhs.schema_version() &&
           lhs.layout_version() == rhs.layout_version();
}

Status ValidateCompositionLimits(const RemoteBridgeConfig& config) {
    constexpr size_t kAnnouncementPayloadOverhead =
        bridge::kWireControlOpcodeLength +
        bridge::kSchemaAnnouncementFixedPayloadBytes;
    constexpr size_t kAnnouncementBodyOverhead =
        bridge::kWireBaseHeaderLength + kAnnouncementPayloadOverhead;
    const auto& negotiation = config.schema_negotiation;
    const auto& pipeline = config.connection.pipeline;
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
    if (payload_bytes > pipeline.wire_limits.max_payload_length ||
        body_bytes > config.tcp.max_frame_body_bytes ||
        body_bytes > pipeline.max_control_bytes ||
        bridge::kLengthPrefixSize + body_bytes >
            pipeline.wire_limits.max_buffered_bytes) {
        return Invalid(
            "remote Bridge TCP, wire, control, and schema limits are incompatible");
    }
    return Status::Ok();
}

}  // namespace

Result<capacity::ResourceVector> EstimateRemoteBridgeResources(
    const RemoteBridgeConfig& config) noexcept {
    MINO_RETURN_IF_ERROR(ValidateCompositionLimits(config));
    capacity::ResourceVector resources;
    resources.bridge_connections = config.tcp_lane_count;
    resources.threads = 1;  // One shared TcpDriver worker.

    capacity::ResourceVector egress;
    // Application egress and TCP driver buffers are shared by the logical peer
    // and therefore charged once, not once per lane.
    egress.bridge_egress_bytes = config.connection.max_egress_bytes;
    MINO_ASSIGN_OR_RETURN(resources,
                          capacity::CheckedAdd(resources, egress));
    egress.bridge_egress_bytes = config.tcp.max_total_send_buffer_bytes;
    MINO_ASSIGN_OR_RETURN(resources,
                          capacity::CheckedAdd(resources, egress));
    egress.bridge_egress_bytes = config.tcp.max_control_send_buffer_bytes;
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
    std::shared_ptr<capacity::CapacityController> capacity_controller,
    std::optional<capacity::ResourceVector> capacity_charge) noexcept {
    try {
        if (ingress == nullptr || descriptor_auth == nullptr) {
            return Invalid("remote Bridge ingress or descriptor auth is null");
        }
        if (config.schema_store_root.empty()) {
            return Invalid("remote Bridge schema store root is empty");
        }
        MINO_RETURN_IF_ERROR(ValidateCompositionLimits(config));

        capacity::CapacityReservation capacity_reservation;
        if (capacity_controller) {
            capacity::ResourceVector charge;
            if (capacity_charge.has_value()) {
                charge = *capacity_charge;
            } else {
                MINO_ASSIGN_OR_RETURN(charge,
                                      EstimateRemoteBridgeResources(config));
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
        MINO_ASSIGN_OR_RETURN(auto created_driver,
                              transport::TcpDriver::Create(config.tcp));
        auto driver =
            std::shared_ptr<transport::TcpDriver>(std::move(created_driver));

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
            std::move(listener_hub), config.connection.driver_config));
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
    std::shared_ptr<transport::TcpDriver> driver,
    std::shared_ptr<bridge::BridgeConnectionPool> pool,
    std::unique_ptr<bridge::BridgeListenerHub> listener_hub,
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
    return pool_->Pump(budget);
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
