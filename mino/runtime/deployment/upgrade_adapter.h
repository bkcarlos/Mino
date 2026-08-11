// Copyright 2026 The Mino Authors

#ifndef MINO_RUNTIME_DEPLOYMENT_UPGRADE_ADAPTER_H_
#define MINO_RUNTIME_DEPLOYMENT_UPGRADE_ADAPTER_H_

#include <filesystem>
#include <memory>
#include <mutex>

#include "mino/runtime/deployment/local_bus.h"
#include "mino/shm/region/region.h"
#include "mino/upgrade/production_control_plane.h"

namespace mino::deployment {

// Production composition adapter. Construction requires concrete deployment
// and Region supervisor objects; there is no constructor from evidence values.
class LocalBusProductionUpgradeAdapter final
    : public upgrade::ProductionUpgradeProbe {
public:
    static Result<std::unique_ptr<LocalBusProductionUpgradeAdapter>> Create(
        LocalBusDeployment* deployment,
        std::filesystem::path routing_catalog_path,
        SharedMemoryRegion* source_supervisor,
        SharedMemoryRegion* target_supervisor) noexcept;

    Status PrepareTarget(const upgrade::UpgradePlan& plan,
                         SharedMemoryRegion& target) override;
    Result<upgrade::ProductionTargetState> ObserveTarget(
        const upgrade::UpgradePlan& plan,
        const SharedMemoryRegion& target) const override;
    Status FenceSourcePublishers(const upgrade::UpgradePlan& plan) override;
    Result<std::vector<upgrade::ProductionTopicState>> ObserveTopics(
        std::span<const TopicId> topic_ids) const override;
    Status ActivateTarget(
        const upgrade::UpgradePlan& plan,
        const upgrade::RegionRoutingSnapshot& durable_route) override;
    Status CleanShutdownSource(const upgrade::UpgradePlan& plan) override;
    Status FenceTargetPublishers(const upgrade::UpgradePlan& plan) override;
    Status RestoreSource(
        const upgrade::UpgradePlan& plan,
        const upgrade::RegionRoutingSnapshot& durable_route) override;

private:
    LocalBusProductionUpgradeAdapter(LocalBusDeployment* deployment,
                                     SharedMemoryRegion* source_supervisor,
                                     SharedMemoryRegion* target_supervisor) noexcept;

    LocalBusDeployment* deployment_ = nullptr;
    SharedMemoryRegion* source_supervisor_ = nullptr;
    SharedMemoryRegion* target_supervisor_ = nullptr;
    mutable std::mutex mutex_;
    bool source_shutdown_ = false;
};

}  // namespace mino::deployment

#endif  // MINO_RUNTIME_DEPLOYMENT_UPGRADE_ADAPTER_H_
