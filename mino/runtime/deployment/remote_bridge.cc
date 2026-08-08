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
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth) noexcept {
    try {
        if (ingress == nullptr || descriptor_auth == nullptr) {
            return Invalid("remote Bridge ingress or descriptor auth is null");
        }
        if (config.schema_store_root.empty()) {
            return Invalid("remote Bridge schema store root is empty");
        }
        MINO_RETURN_IF_ERROR(ValidateCompositionLimits(config));

        auto registry = std::make_unique<schema::SchemaRegistry>();
        MINO_ASSIGN_OR_RETURN(
            auto store,
            storage::SchemaStore::Open(config.schema_store_root,
                                       registry.get()));
        MINO_RETURN_IF_ERROR(store->HydrateRegistry());
        auto persistence =
            std::make_unique<StorePersistence>(store.get());
        auto negotiator = std::make_unique<bridge::SchemaNegotiator>(
            registry.get(), descriptor_auth.get(), persistence.get(),
            config.schema_negotiation);
        MINO_ASSIGN_OR_RETURN(auto created_driver,
                              transport::TcpDriver::Create(config.tcp));
        auto driver =
            std::shared_ptr<transport::TcpDriver>(std::move(created_driver));
        config.connection.manage_driver_lifecycle = true;
        MINO_ASSIGN_OR_RETURN(
            auto manager,
            bridge::BridgeConnectionManager::Create(
                std::move(config.connection), driver, ingress,
                negotiator.get()));
        return std::unique_ptr<RemoteBridge>(new RemoteBridge(
            std::move(registry), std::move(store), std::move(descriptor_auth),
            std::move(persistence), std::move(negotiator), std::move(driver),
            std::move(manager)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "remote Bridge composition allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "remote Bridge composition failed");
    }
}

RemoteBridge::RemoteBridge(
    std::unique_ptr<schema::SchemaRegistry> registry,
    std::unique_ptr<storage::SchemaStore> store,
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
    std::unique_ptr<StorePersistence> persistence,
    std::unique_ptr<bridge::SchemaNegotiator> negotiator,
    std::shared_ptr<transport::TcpDriver> driver,
    std::unique_ptr<bridge::BridgeConnectionManager> manager) noexcept
    : registry_(std::move(registry)),
      store_(std::move(store)),
      descriptor_auth_(std::move(descriptor_auth)),
      persistence_(std::move(persistence)),
      negotiator_(std::move(negotiator)),
      driver_(std::move(driver)),
      manager_(std::move(manager)) {}

RemoteBridge::~RemoteBridge() { static_cast<void>(Shutdown()); }

Status RemoteBridge::Start(uint64_t now_ns) noexcept {
    return manager_->Start(now_ns);
}

Result<bridge::BridgeConnectionPumpResult> RemoteBridge::Pump(
    bridge::BridgePumpBudget budget) noexcept {
    return manager_->Pump(budget);
}

Status RemoteBridge::Shutdown() noexcept { return manager_->Shutdown(); }

Result<std::vector<schema::SchemaHandle>>
RemoteBridge::RegisterLocalDescriptor(
    std::span<const std::byte> descriptor_artifact) noexcept {
    try {
        if (manager_->state() != bridge::BridgeConnectionState::kStopped) {
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
        return manager_->Enqueue(bridge::EncodedOutboundFrame{
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
