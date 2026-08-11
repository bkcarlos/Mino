// Copyright 2026 The Mino Authors

#include "mino/upgrade/production_control_plane.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "mino/common/status.h"

namespace mino::upgrade {
namespace {

Status IdentityMismatch(std::string_view which) {
    return Status::Error(StatusCode::kAlreadyExists,
                         std::string(which) +
                             " Region name resolved to a different UUID/id/layout/domain");
}

std::vector<TopicId> SourceTopicIds(const UpgradePlan& plan) {
    std::vector<TopicId> ids;
    ids.reserve(plan.topics.size());
    for (const TopicUpgrade& topic : plan.topics) ids.push_back(topic.source.topic_id);
    return ids;
}

std::vector<TopicId> TargetTopicIds(const UpgradePlan& plan) {
    std::vector<TopicId> ids;
    ids.reserve(plan.topics.size());
    for (const TopicUpgrade& topic : plan.topics) ids.push_back(topic.target.topic_id);
    return ids;
}

bool SchemaAccepted(const registry::TopicMetadata& reader,
                    const schema::SchemaIdentity& writer) {
    if (registry::SchemaIdentityEqual(reader.schema, writer)) return true;
    return std::any_of(reader.accepted_schemas.begin(),
                       reader.accepted_schemas.end(),
                       [&](const schema::SchemaIdentity& accepted) {
                           return registry::SchemaIdentityEqual(accepted, writer);
                       });
}

TopicBinding BindingFrom(const registry::TopicMetadata& metadata) {
    return TopicBinding{
        .topic_id = metadata.topic_id,
        .name = metadata.name,
        .config_version = metadata.config_version,
        .region_version = metadata.region_version,
        .channel_version = metadata.channel_version,
        .acl_version = metadata.acl_version,
        .schema = metadata.schema,
        .acl = metadata.acl,
    };
}

Result<std::vector<ProductionTopicState>> OrderedProbeSnapshot(
    ProductionUpgradeProbe* probe, std::span<const TopicId> topic_ids) {
    MINO_ASSIGN_OR_RETURN(auto observed, probe->ObserveTopics(topic_ids));
    if (observed.size() != topic_ids.size()) {
        return Status::Error(StatusCode::kCorruption,
                             "production probe returned an incomplete topic set");
    }
    for (size_t index = 0; index < topic_ids.size(); ++index) {
        if (observed[index].topic_id != topic_ids[index]) {
            return Status::Error(StatusCode::kCorruption,
                                 "production probe topic set is stale or reordered");
        }
    }
    return observed;
}

bool TopicQuiescent(const ProductionTopicState& topic) {
    return topic.publisher_creation_fenced && topic.local_publishers == 0 &&
           topic.local_readers == 0 && topic.pins == 0 &&
           topic.outstanding_receipts == 0 &&
           topic.outstanding_borrows == 0 && topic.queue_depth == 0 &&
           topic.last_consumed_sequence >= topic.last_published_sequence;
}

}  // namespace

RegionIdentity RegionIdentityFrom(const std::string& name,
                                  const SharedMemoryRegion& region) {
    const SuperBlock& superblock = *region.superblock();
    return RegionIdentity{
        .name = name,
        .region_id = superblock.region_id,
        .uuid_lo = superblock.region_uuid_lo,
        .uuid_hi = superblock.region_uuid_hi,
        .layout_version = superblock.layout_version,
        .security_domain = SecurityDomainId{superblock.security_domain_id},
    };
}

ProductionUpgradeControlPlane::ProductionUpgradeControlPlane(
    registry::Coordinator* coordinator, RegionRoutingCatalog* catalog,
    ProductionUpgradeProbe* probe,
    ProductionUpgradeControlPlaneOptions options) noexcept
    : coordinator_(coordinator),
      catalog_(catalog),
      probe_(probe),
      options_(std::move(options)) {}

Result<std::unique_ptr<ProductionUpgradeControlPlane>>
ProductionUpgradeControlPlane::Create(
    registry::Coordinator* coordinator, RegionRoutingCatalog* catalog,
    ProductionUpgradeProbe* probe,
    ProductionUpgradeControlPlaneOptions options) noexcept {
    try {
        if (coordinator == nullptr || catalog == nullptr || probe == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "production upgrade dependencies are incomplete");
        }
        return std::unique_ptr<ProductionUpgradeControlPlane>(
            new ProductionUpgradeControlPlane(coordinator, catalog, probe,
                                              std::move(options)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<ProvisionedTargetRegion> ProductionUpgradeControlPlane::ProvisionTarget(
    RegionCreateOptions options) noexcept {
    const std::string name = options.name;
    MINO_ASSIGN_OR_RETURN(SharedMemoryRegion region,
                          SharedMemoryRegion::Create(options));
    RegionIdentity identity = RegionIdentityFrom(name, region);
    return ProvisionedTargetRegion{.identity = std::move(identity),
                                   .region = std::move(region)};
}

Status ProductionUpgradeControlPlane::EnsureRegions(const UpgradePlan& plan) {
    if (!source_attachment_.has_value()) {
        RegionAttachOptions source_options;
        source_options.name = plan.source_region.name;
        source_options.region_id = plan.source_region.region_id;
        source_options.read_only = true;
        source_options.security_domain = plan.source_region.security_domain;
        MINO_ASSIGN_OR_RETURN(SharedMemoryRegion source,
                              SharedMemoryRegion::Attach(source_options));
        if (!(RegionIdentityFrom(plan.source_region.name, source) ==
              plan.source_region)) {
            return IdentityMismatch("source");
        }
        source_attachment_.emplace(std::move(source));
    }
    if (!target_attachment_.has_value()) {
        RegionAttachOptions target_options;
        target_options.name = plan.target_region.name;
        target_options.region_id = plan.target_region.region_id;
        target_options.read_only = true;
        target_options.security_domain = plan.target_region.security_domain;
        auto attached = SharedMemoryRegion::Attach(target_options);
        if (!attached.ok() && attached.status().code() == StatusCode::kNotFound &&
            options_.create_target.has_value()) {
            if (options_.create_target->name != plan.target_region.name ||
                options_.create_target->security_domain !=
                    plan.target_region.security_domain) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "target create options disagree with the plan");
            }
            MINO_ASSIGN_OR_RETURN(SharedMemoryRegion created,
                                  SharedMemoryRegion::Create(*options_.create_target));
            if (!(RegionIdentityFrom(plan.target_region.name, created) ==
                  plan.target_region)) {
                static_cast<void>(created.Detach());
                return IdentityMismatch("newly created target");
            }
            target_attachment_.emplace(std::move(created));
        } else if (!attached.ok()) {
            return attached.status();
        } else {
            if (!(RegionIdentityFrom(plan.target_region.name, *attached) ==
                  plan.target_region)) {
                return IdentityMismatch("target");
            }
            target_attachment_.emplace(std::move(*attached));
        }
    }
    return Status::Ok();
}

Result<std::vector<std::shared_ptr<const registry::TopicSnapshot>>>
ProductionUpgradeControlPlane::TopicSnapshots(
    std::span<const TopicId> topic_ids) const {
    return coordinator_->UpgradeUsageSnapshot(topic_ids);
}

Status ProductionUpgradeControlPlane::Prepare(const UpgradePlan& plan) {
    MINO_RETURN_IF_ERROR(ValidateUpgradePlan(plan));
    if (!(catalog_->snapshot().active_region == plan.source_region) &&
        !(catalog_->snapshot().active_region == plan.target_region)) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "routing catalog is not bound to this upgrade");
    }
    MINO_RETURN_IF_ERROR(EnsureRegions(plan));
    return probe_->PrepareTarget(plan, *target_attachment_);
}

Result<TargetReadinessProof> ProductionUpgradeControlPlane::ObserveTarget(
    const UpgradePlan& plan) {
    MINO_RETURN_IF_ERROR(EnsureRegions(plan));
    const SuperBlock& superblock = *target_attachment_->superblock();
    if (!(RegionIdentityFrom(plan.target_region.name, *target_attachment_) ==
          plan.target_region)) {
        return IdentityMismatch("target");
    }
    MINO_ASSIGN_OR_RETURN(const ProductionTargetState runtime,
                          probe_->ObserveTarget(plan, *target_attachment_));
    const std::vector<TopicId> source_ids = SourceTopicIds(plan);
    const std::vector<TopicId> target_ids = TargetTopicIds(plan);
    MINO_ASSIGN_OR_RETURN(const auto sources, TopicSnapshots(source_ids));
    MINO_ASSIGN_OR_RETURN(const auto targets, TopicSnapshots(target_ids));

    TargetReadinessProof proof{
        .region = RegionIdentityFrom(plan.target_region.name, *target_attachment_),
        .topics = {},
        .region_active = LoadRegionState(superblock) == RegionState::kActive,
        .processes_ready = runtime.processes_ready,
        .channels_ready = runtime.channels_ready,
        .routes_ready = runtime.routes_ready,
        .schema_bidirectionally_compatible = true,
        .acl_exactly_preserved = true,
        .capacity_admitted = true,
        .available_shm_bytes = superblock.data_size,
    };
    proof.topics.reserve(targets.size());
    uint64_t publisher_slots = 0;
    uint64_t subscriber_slots = 0;
    for (size_t index = 0; index < targets.size(); ++index) {
        const registry::TopicMetadata& source = sources[index]->metadata;
        const registry::TopicMetadata& target = targets[index]->metadata;
        proof.topics.push_back(BindingFrom(target));
        proof.schema_bidirectionally_compatible =
            proof.schema_bidirectionally_compatible &&
            SchemaAccepted(target, source.schema) &&
            SchemaAccepted(source, target.schema);
        proof.acl_exactly_preserved =
            proof.acl_exactly_preserved && source.acl == target.acl &&
            target.acl == plan.topics[index].target.acl;
        publisher_slots += target.max_publishers - targets[index]->usage.publishers;
        subscriber_slots +=
            target.max_subscribers - targets[index]->usage.subscribers;
    }
    proof.available_publisher_slots = static_cast<uint32_t>(std::min<uint64_t>(
        publisher_slots, std::numeric_limits<uint32_t>::max()));
    proof.available_subscriber_slots = static_cast<uint32_t>(std::min<uint64_t>(
        subscriber_slots, std::numeric_limits<uint32_t>::max()));
    proof.capacity_admitted =
        proof.available_shm_bytes >= plan.required_shm_bytes &&
        publisher_slots >= plan.required_publisher_slots &&
        subscriber_slots >= plan.required_subscriber_slots;
    return proof;
}

Status ProductionUpgradeControlPlane::BeginDrain(const UpgradePlan& plan) {
    const std::vector<TopicId> ids = SourceTopicIds(plan);
    MINO_ASSIGN_OR_RETURN(const auto baseline,
                          coordinator_->BeginUpgradeDrain(ids));
    static_cast<void>(baseline);
    MINO_RETURN_IF_ERROR(probe_->FenceSourcePublishers(plan));
    return catalog_->FenceSource(plan);
}

Result<DrainProof> ProductionUpgradeControlPlane::ObserveDrain(
    const UpgradePlan& plan) {
    const std::vector<TopicId> ids = SourceTopicIds(plan);
    MINO_ASSIGN_OR_RETURN(const auto registry, TopicSnapshots(ids));
    MINO_ASSIGN_OR_RETURN(const auto runtime,
                          OrderedProbeSnapshot(probe_, ids));
    DrainProof proof{.old_publishers_fenced = true};
    bool sequence_conserved = true;
    for (size_t index = 0; index < ids.size(); ++index) {
        const registry::TopicUsageCounts& usage = registry[index]->usage;
        proof.old_publishers_fenced =
            proof.old_publishers_fenced &&
            registry[index]->metadata.state == registry::TopicState::kDraining &&
            runtime[index].publisher_creation_fenced;
        proof.publishers += std::max<uint64_t>(usage.publishers,
                                               runtime[index].local_publishers);
        proof.subscribers += std::max<uint64_t>(usage.subscribers,
                                                runtime[index].local_readers);
        proof.pins += usage.bridges + usage.recorders + usage.replay_pins +
                      runtime[index].pins;
        proof.outstanding_receipts += runtime[index].outstanding_receipts;
        proof.outstanding_borrows += runtime[index].outstanding_borrows;
        proof.queue_depth += runtime[index].queue_depth;
        proof.last_published_sequence += runtime[index].last_published_sequence;
        proof.last_consumed_sequence += runtime[index].last_consumed_sequence;
        sequence_conserved = sequence_conserved &&
                             runtime[index].last_consumed_sequence >=
                                 runtime[index].last_published_sequence;
    }
    if (!sequence_conserved) {
        proof.last_published_sequence = std::max<uint64_t>(
            proof.last_published_sequence, proof.last_consumed_sequence + 1);
    }
    return proof;
}

Status ProductionUpgradeControlPlane::Cutover(const UpgradePlan& plan) {
    MINO_RETURN_IF_ERROR(EnsureRegions(plan));
    if (catalog_->snapshot().commit_token != plan.commit_token ||
        !catalog_->snapshot().source_fenced) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "durable source fence is absent");
    }
    MINO_RETURN_IF_ERROR(catalog_->Cutover(plan));
    const std::vector<TopicId> ids = TargetTopicIds(plan);
    MINO_ASSIGN_OR_RETURN(const auto targets, TopicSnapshots(ids));
    for (const auto& target : targets) {
        if (target->metadata.state == registry::TopicState::kActive) continue;
        if (target->metadata.state != registry::TopicState::kCreating) {
            return Status::Error(StatusCode::kUnsupported,
                                 "target topic cannot be activated for cutover");
        }
        const registry::ActivationReadinessProof readiness{
            .topic_id = target->metadata.topic_id,
            .config_version = target->metadata.config_version,
            .schema = target->metadata.schema,
            .region_version = target->metadata.region_version,
            .channel_version = target->metadata.channel_version,
            .acl_version = target->metadata.acl_version,
            .schema_ready = true,
            .region_ready = true,
            .channel_ready = true,
            .acl_ready = true,
        };
        MINO_RETURN_IF_ERROR(
            coordinator_->ActivateTopic(target->metadata.topic_id, readiness));
    }
    return probe_->ActivateTarget(plan, catalog_->snapshot());
}

Result<CutoverObservation>
ProductionUpgradeControlPlane::ObserveCutover(const UpgradePlan& plan) {
    const std::vector<TopicId> source_ids = SourceTopicIds(plan);
    const std::vector<TopicId> target_ids = TargetTopicIds(plan);
    MINO_ASSIGN_OR_RETURN(const auto source_registry,
                          TopicSnapshots(source_ids));
    MINO_ASSIGN_OR_RETURN(const auto target_registry,
                          TopicSnapshots(target_ids));
    MINO_ASSIGN_OR_RETURN(const auto sources,
                          OrderedProbeSnapshot(probe_, source_ids));
    MINO_ASSIGN_OR_RETURN(const auto targets,
                          OrderedProbeSnapshot(probe_, target_ids));
    CutoverObservation observation{
        .acknowledged_commit_token = catalog_->snapshot().commit_token,
        .active_region = catalog_->snapshot().active_region,
    };
    for (size_t index = 0; index < sources.size(); ++index) {
        observation.old_publisher_count += std::max<uint64_t>(
            source_registry[index]->usage.publishers,
            sources[index].local_publishers);
        observation.duplicate_count += sources[index].duplicate_count;
        observation.unexplained_loss_count +=
            sources[index].unexplained_loss_count;
        if (sources[index].outstanding_receipts != 0 ||
            sources[index].last_consumed_sequence <
                sources[index].last_published_sequence) {
            ++observation.unexplained_loss_count;
        }
    }
    for (size_t index = 0; index < targets.size(); ++index) {
        observation.new_publisher_count += std::max<uint64_t>(
            target_registry[index]->usage.publishers,
            targets[index].local_publishers);
        observation.observed_samples += targets[index].observed_samples;
        observation.duplicate_count += targets[index].duplicate_count;
        observation.unexplained_loss_count +=
            targets[index].unexplained_loss_count;
        if (targets[index].outstanding_receipts != 0 ||
            targets[index].last_consumed_sequence >
                targets[index].last_published_sequence) {
            ++observation.unexplained_loss_count;
        }
    }
    return observation;
}

Status ProductionUpgradeControlPlane::Commit(const UpgradePlan& plan) {
    MINO_RETURN_IF_ERROR(EnsureRegions(plan));
    const std::vector<TopicId> ids = SourceTopicIds(plan);
    MINO_ASSIGN_OR_RETURN(const auto runtime,
                          OrderedProbeSnapshot(probe_, ids));
    MINO_ASSIGN_OR_RETURN(const auto registry, TopicSnapshots(ids));
    std::vector<registry::DrainCompletionProof> completions;
    completions.reserve(ids.size());
    for (size_t index = 0; index < ids.size(); ++index) {
        if (registry[index]->metadata.state == registry::TopicState::kDeleted) {
            completions.push_back(registry::DrainCompletionProof{
                .topic_id = ids[index],
                .config_version = registry[index]->metadata.config_version,
                .schema = registry[index]->metadata.schema,
                .region_version = registry[index]->metadata.region_version,
                .channel_version = registry[index]->metadata.channel_version,
                .acl_version = registry[index]->metadata.acl_version,
                .channel_drained = true,
                .borrows_released = true,
            });
            continue;
        }
        if (!TopicQuiescent(runtime[index])) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "source Region still has publisher/reader/pin/receipt/borrow/queue state");
        }
        const registry::TopicMetadata& metadata = registry[index]->metadata;
        completions.push_back(registry::DrainCompletionProof{
            .topic_id = metadata.topic_id,
            .config_version = metadata.config_version,
            .schema = metadata.schema,
            .region_version = metadata.region_version,
            .channel_version = metadata.channel_version,
            .acl_version = metadata.acl_version,
            .channel_drained = runtime[index].queue_depth == 0 &&
                               runtime[index].last_consumed_sequence >=
                                   runtime[index].last_published_sequence,
            .borrows_released = runtime[index].outstanding_borrows == 0,
        });
    }
    MINO_RETURN_IF_ERROR(
        coordinator_->RetireAndDeleteUpgradeTopics(completions));
    if (source_attachment_.has_value()) {
        MINO_RETURN_IF_ERROR(source_attachment_->Detach());
        source_attachment_.reset();
    }
    return probe_->CleanShutdownSource(plan);
}

Result<SafeRollbackProof>
ProductionUpgradeControlPlane::ObserveRollbackSafety(const UpgradePlan& plan) {
    MINO_RETURN_IF_ERROR(EnsureRegions(plan));
    const std::vector<TopicId> target_ids = TargetTopicIds(plan);
    MINO_ASSIGN_OR_RETURN(const auto targets,
                          OrderedProbeSnapshot(probe_, target_ids));
    SafeRollbackProof proof{.target_publishers_fenced = true,
                            .source_ready = source_attachment_.has_value()};
    for (const ProductionTopicState& target : targets) {
        proof.target_publishers_fenced =
            proof.target_publishers_fenced &&
            target.publisher_creation_fenced && target.local_publishers == 0;
        proof.target_publications_after_cutover += target.observed_samples;
        proof.sequence_receipt_reconciliation_complete =
            proof.sequence_receipt_reconciliation_complete ||
            (target.duplicate_count == 0 &&
             target.unexplained_loss_count == 0 &&
             target.outstanding_receipts == 0 &&
             target.last_consumed_sequence >= target.last_published_sequence);
    }
    return proof;
}

Status ProductionUpgradeControlPlane::Rollback(const UpgradePlan& plan,
                                                bool after_cutover) {
    if (after_cutover) {
        MINO_RETURN_IF_ERROR(probe_->FenceTargetPublishers(plan));
        const std::vector<TopicId> target_ids = TargetTopicIds(plan);
        MINO_ASSIGN_OR_RETURN(const auto target_drain,
                              coordinator_->BeginUpgradeDrain(target_ids));
        static_cast<void>(target_drain);
    }
    MINO_RETURN_IF_ERROR(catalog_->RestoreSource(plan));
    const std::vector<TopicId> source_ids = SourceTopicIds(plan);
    MINO_RETURN_IF_ERROR(coordinator_->CancelUpgradeDrain(source_ids));
    return probe_->RestoreSource(plan, catalog_->snapshot());
}

}  // namespace mino::upgrade
