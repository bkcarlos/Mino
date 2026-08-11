// Copyright 2026 The Mino Authors

#include "mino/runtime/deployment/upgrade_adapter.h"

#include <utility>

#include "mino/common/status.h"

namespace mino::deployment {
namespace {

Status CheckSupervisor(const SharedMemoryRegion& region,
                       std::string_view which) {
    if (!region.is_supervisor()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             std::string(which) +
                                 " Region is not bound to a writable supervisor");
    }
    return region.ValidateSupervisorFence();
}

Status CheckSourceBinding(const SharedMemoryRegion& region) {
    if (region.is_supervisor()) return region.ValidateSupervisorFence();
    if (LoadRegionState(*region.superblock()) == RegionState::kClosed &&
        LoadCleanShutdown(*region.superblock())) {
        return Status::Ok();
    }
    return Status::Error(
        StatusCode::kPermissionDenied,
        "source Region binding is neither a live supervisor nor clean CLOSED recovery evidence");
}

}  // namespace

LocalBusProductionUpgradeAdapter::LocalBusProductionUpgradeAdapter(
    LocalBusDeployment* deployment, SharedMemoryRegion* source_supervisor,
    SharedMemoryRegion* target_supervisor) noexcept
    : deployment_(deployment),
      source_supervisor_(source_supervisor),
      target_supervisor_(target_supervisor) {}

Result<std::unique_ptr<LocalBusProductionUpgradeAdapter>>
LocalBusProductionUpgradeAdapter::Create(
    LocalBusDeployment* deployment,
    std::filesystem::path routing_catalog_path,
    SharedMemoryRegion* source_supervisor,
    SharedMemoryRegion* target_supervisor) noexcept {
    try {
        if (deployment == nullptr || routing_catalog_path.empty() ||
            source_supervisor == nullptr || target_supervisor == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "LocalBus production upgrade binding is incomplete");
        }
        MINO_RETURN_IF_ERROR(CheckSourceBinding(*source_supervisor));
        MINO_RETURN_IF_ERROR(CheckSupervisor(*target_supervisor, "target"));
        MINO_RETURN_IF_ERROR(
            deployment->BindRoutingCatalog(std::move(routing_catalog_path)));
        return std::unique_ptr<LocalBusProductionUpgradeAdapter>(
            new LocalBusProductionUpgradeAdapter(
                deployment, source_supervisor, target_supervisor));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status LocalBusProductionUpgradeAdapter::PrepareTarget(
    const upgrade::UpgradePlan& plan, SharedMemoryRegion& target) {
    MINO_RETURN_IF_ERROR(CheckSupervisor(*target_supervisor_, "target"));
    if (!(upgrade::RegionIdentityFrom(plan.target_region.name, target) ==
          plan.target_region) ||
        !(upgrade::RegionIdentityFrom(plan.target_region.name,
                                      *target_supervisor_) ==
          plan.target_region)) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "target Region probe binding does not match the plan");
    }
    for (const upgrade::TopicUpgrade& topic : plan.topics) {
        MINO_ASSIGN_OR_RETURN(
            const LocalBusUpgradeTopicStats state,
            deployment_->UpgradeTopicStats(topic.target.topic_id));
        if (state.region_id != plan.target_region.region_id ||
            !state.publisher_creation_fenced) {
            return Status::Error(
                StatusCode::kUnavailable,
                "target LocalBus channel is absent or publisher-active before cutover");
        }
    }
    return Status::Ok();
}

Result<upgrade::ProductionTargetState>
LocalBusProductionUpgradeAdapter::ObserveTarget(
    const upgrade::UpgradePlan& plan,
    const SharedMemoryRegion& target) const {
    MINO_RETURN_IF_ERROR(CheckSupervisor(*target_supervisor_, "target"));
    if (!(upgrade::RegionIdentityFrom(plan.target_region.name, target) ==
          plan.target_region)) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "observed target Region identity changed");
    }
    bool channels_ready = true;
    bool routes_ready = true;
    for (const upgrade::TopicUpgrade& topic : plan.topics) {
        auto channel = deployment_->UpgradeTopicStats(topic.target.topic_id);
        channels_ready = channels_ready && channel.ok() &&
                         channel->region_id == plan.target_region.region_id &&
                         channel->publisher_creation_fenced;
        auto metadata =
            deployment_->coordinator().GetTopic(topic.target.topic_id);
        routes_ready = routes_ready && metadata.ok() &&
                       !(*metadata)->metadata.static_routes.empty();
    }
    return upgrade::ProductionTargetState{
        .processes_ready = true,
        .channels_ready = channels_ready,
        .routes_ready = routes_ready,
    };
}

Status LocalBusProductionUpgradeAdapter::FenceSourcePublishers(
    const upgrade::UpgradePlan& plan) {
    for (const upgrade::TopicUpgrade& topic : plan.topics) {
        MINO_RETURN_IF_ERROR(
            deployment_->FenceUpgradePublisher(topic.source.topic_id));
    }
    return Status::Ok();
}

Result<std::vector<upgrade::ProductionTopicState>>
LocalBusProductionUpgradeAdapter::ObserveTopics(
    std::span<const TopicId> topic_ids) const {
    try {
        std::vector<upgrade::ProductionTopicState> result;
        result.reserve(topic_ids.size());
        for (TopicId topic_id : topic_ids) {
            MINO_ASSIGN_OR_RETURN(const LocalBusUpgradeTopicStats state,
                                  deployment_->UpgradeTopicStats(topic_id));
            result.push_back(upgrade::ProductionTopicState{
                .topic_id = state.topic_id,
                .publisher_creation_fenced =
                    state.publisher_creation_fenced,
                .local_publishers = state.local_publishers,
                .local_readers = state.local_readers,
                .pins = 0,
                .outstanding_receipts = state.outstanding_receipts,
                .outstanding_borrows = state.outstanding_borrows,
                .queue_depth = state.queue_depth,
                .last_published_sequence = state.last_published_sequence,
                .last_consumed_sequence = state.last_consumed_sequence,
                .observed_samples = state.observed_samples,
                .duplicate_count = state.duplicate_count,
                .unexplained_loss_count = state.unexplained_loss_count,
            });
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status LocalBusProductionUpgradeAdapter::ActivateTarget(
    const upgrade::UpgradePlan& plan,
    const upgrade::RegionRoutingSnapshot& durable_route) {
    if (!(durable_route.active_region == plan.target_region) ||
        durable_route.commit_token != plan.commit_token ||
        !durable_route.source_fenced) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "target activation lacks durable catalog cutover");
    }
    for (const upgrade::TopicUpgrade& topic : plan.topics) {
        MINO_RETURN_IF_ERROR(
            deployment_->UnfenceUpgradePublisher(topic.target.topic_id));
        MINO_RETURN_IF_ERROR(
            deployment_->RefreshUpgradeRoute(topic.target.topic_id));
    }
    return Status::Ok();
}

Status LocalBusProductionUpgradeAdapter::CleanShutdownSource(
    const upgrade::UpgradePlan& plan) {
    std::lock_guard lock(mutex_);
    if (source_shutdown_) return Status::Ok();
    if (!source_supervisor_->is_supervisor() &&
        LoadRegionState(*source_supervisor_->superblock()) ==
            RegionState::kClosed &&
        LoadCleanShutdown(*source_supervisor_->superblock())) {
        source_shutdown_ = true;
        return Status::Ok();
    }
    for (const upgrade::TopicUpgrade& topic : plan.topics) {
        MINO_ASSIGN_OR_RETURN(const LocalBusUpgradeTopicStats state,
                              deployment_->UpgradeTopicStats(
                                  topic.source.topic_id));
        if (state.local_publishers != 0 || state.local_readers != 0 ||
            state.outstanding_receipts != 0 ||
            state.outstanding_borrows != 0 || state.queue_depth != 0) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "source Region has live LocalBus state");
        }
    }
    MINO_RETURN_IF_ERROR(CheckSupervisor(*source_supervisor_, "source"));
    MINO_RETURN_IF_ERROR(source_supervisor_->Detach());
    source_shutdown_ = true;
    return Status::Ok();
}

Status LocalBusProductionUpgradeAdapter::FenceTargetPublishers(
    const upgrade::UpgradePlan& plan) {
    for (const upgrade::TopicUpgrade& topic : plan.topics) {
        MINO_RETURN_IF_ERROR(
            deployment_->FenceUpgradePublisher(topic.target.topic_id));
    }
    return Status::Ok();
}

Status LocalBusProductionUpgradeAdapter::RestoreSource(
    const upgrade::UpgradePlan& plan,
    const upgrade::RegionRoutingSnapshot& durable_route) {
    if (!(durable_route.active_region == plan.source_region) ||
        durable_route.source_fenced || !durable_route.commit_token.empty()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "source restore lacks durable source catalog route");
    }
    for (const upgrade::TopicUpgrade& topic : plan.topics) {
        MINO_RETURN_IF_ERROR(
            deployment_->UnfenceUpgradePublisher(topic.source.topic_id));
        MINO_RETURN_IF_ERROR(
            deployment_->RefreshUpgradeRoute(topic.source.topic_id));
    }
    return Status::Ok();
}

}  // namespace mino::deployment
