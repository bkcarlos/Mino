// Copyright 2026 The Mino Authors

#ifndef MINO_UPGRADE_PRODUCTION_CONTROL_PLANE_H_
#define MINO_UPGRADE_PRODUCTION_CONTROL_PLANE_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "mino/registry/coordinator.h"
#include "mino/shm/region/region.h"
#include "mino/upgrade/routing_catalog.h"
#include "mino/upgrade/upgrade.h"

namespace mino::upgrade {

struct ProductionTargetState {
    bool processes_ready = false;
    bool channels_ready = false;
    bool routes_ready = false;
};

// Per-topic cold-path snapshot produced from concrete LocalBus/Channel/receipt
// objects. It is deliberately numeric: callers cannot inject a pre-computed
// "drained=true" attestation.
struct ProductionTopicState {
    TopicId topic_id;
    bool publisher_creation_fenced = false;
    uint64_t local_publishers = 0;
    uint64_t local_readers = 0;
    uint64_t pins = 0;
    uint64_t outstanding_receipts = 0;
    uint64_t outstanding_borrows = 0;
    uint64_t queue_depth = 0;
    uint64_t last_published_sequence = 0;
    uint64_t last_consumed_sequence = 0;
    uint64_t observed_samples = 0;
    uint64_t duplicate_count = 0;
    uint64_t unexplained_loss_count = 0;
};

// Implemented by a deployment composition root that owns the actual Bus,
// Channel, receipt tracker, routing cache, process supervisor and Region
// supervisor. CLI/evidence code does not implement or construct this interface.
class ProductionUpgradeProbe {
public:
    virtual ~ProductionUpgradeProbe() = default;
    virtual Status PrepareTarget(const UpgradePlan& plan,
                                 SharedMemoryRegion& target) = 0;
    virtual Result<ProductionTargetState> ObserveTarget(
        const UpgradePlan& plan, const SharedMemoryRegion& target) const = 0;
    virtual Status FenceSourcePublishers(const UpgradePlan& plan) = 0;
    virtual Result<std::vector<ProductionTopicState>> ObserveTopics(
        std::span<const TopicId> topic_ids) const = 0;
    virtual Status ActivateTarget(
        const UpgradePlan& plan,
        const RegionRoutingSnapshot& durable_route) = 0;
    virtual Status CleanShutdownSource(const UpgradePlan& plan) = 0;
    virtual Status FenceTargetPublishers(const UpgradePlan& plan) = 0;
    virtual Status RestoreSource(const UpgradePlan& plan,
                                 const RegionRoutingSnapshot& durable_route) = 0;
};

struct ProvisionedTargetRegion {
    RegionIdentity identity;
    SharedMemoryRegion region;
};

struct ProductionUpgradeControlPlaneOptions {
    // Used only if target Attach reports NotFound. A normal production rollout
    // provisions the target first, records its returned identity in the plan,
    // and keeps its supervisor object bound in the deployment composition root.
    std::optional<RegionCreateOptions> create_target;
};

class ProductionUpgradeControlPlane final : public UpgradeControlPlane {
public:
    static Result<std::unique_ptr<ProductionUpgradeControlPlane>> Create(
        registry::Coordinator* coordinator, RegionRoutingCatalog* catalog,
        ProductionUpgradeProbe* probe,
        ProductionUpgradeControlPlaneOptions options = {}) noexcept;

    // Actual SharedMemoryRegion::Create entry point for production plan
    // generation. The returned UUID/id/layout/domain must be copied verbatim
    // into UpgradePlan::target_region.
    static Result<ProvisionedTargetRegion> ProvisionTarget(
        RegionCreateOptions options) noexcept;

    Status Prepare(const UpgradePlan& plan) override;
    Result<TargetReadinessProof> ObserveTarget(
        const UpgradePlan& plan) override;
    Status BeginDrain(const UpgradePlan& plan) override;
    Result<DrainProof> ObserveDrain(const UpgradePlan& plan) override;
    Status Cutover(const UpgradePlan& plan) override;
    Result<CutoverObservation> ObserveCutover(
        const UpgradePlan& plan) override;
    Status Commit(const UpgradePlan& plan) override;
    Result<SafeRollbackProof> ObserveRollbackSafety(
        const UpgradePlan& plan) override;
    Status Rollback(const UpgradePlan& plan, bool after_cutover) override;

private:
    ProductionUpgradeControlPlane(
        registry::Coordinator* coordinator, RegionRoutingCatalog* catalog,
        ProductionUpgradeProbe* probe,
        ProductionUpgradeControlPlaneOptions options) noexcept;

    Status EnsureRegions(const UpgradePlan& plan);
    Result<std::vector<std::shared_ptr<const registry::TopicSnapshot>>>
    TopicSnapshots(std::span<const TopicId> topic_ids) const;

    registry::Coordinator* coordinator_ = nullptr;
    RegionRoutingCatalog* catalog_ = nullptr;
    ProductionUpgradeProbe* probe_ = nullptr;
    ProductionUpgradeControlPlaneOptions options_;
    std::optional<SharedMemoryRegion> source_attachment_;
    std::optional<SharedMemoryRegion> target_attachment_;
};

RegionIdentity RegionIdentityFrom(const std::string& name,
                                  const SharedMemoryRegion& region);

}  // namespace mino::upgrade

#endif  // MINO_UPGRADE_PRODUCTION_CONTROL_PLANE_H_
