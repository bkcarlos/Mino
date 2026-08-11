// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_DEPLOYMENT_LOCAL_BUS_H_
#define MINO_RUNTIME_DEPLOYMENT_LOCAL_BUS_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "mino/common/result.h"
#include "mino/runtime/bus.h"
#include "mino/security/security_domain.h"
#include "mino/schema/descriptor.h"

namespace mino::deployment {

inline constexpr size_t kMaximumLocalDeploymentTopics = 1024;
inline constexpr uint64_t kMaximumLocalRegionBytes = 1ull << 30;
inline constexpr uint32_t kMaximumLocalChannelCapacity = 1u << 16;

struct LocalTopicConfig {
    std::string name;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    uint32_t channel_capacity = 256;
    uint32_t max_subscribers = 16;
    size_t max_payload_bytes = 1024u * 1024u;
    // Zero inherits LocalBusConfig::region_id. Rolling-upgrade composition may
    // preinstall target topics in a distinct Region generation.
    uint32_t region_id = 0;
    // False keeps the Registry topic Creating and the local publisher seam
    // fenced until ProductionUpgradeControlPlane::Cutover.
    bool activate = true;
};

struct LocalBusOperationalStats {
    uint64_t queue_depth = 0;
    uint64_t queue_capacity = 0;
    uint64_t queue_full_events = 0;
    uint64_t queue_dropped = 0;
    uint64_t lease_expirations = 0;
    uint64_t oldest_heartbeat_age_ns = 0;
};

struct LocalBusUpgradeTopicStats {
    TopicId topic_id;
    uint32_t region_id = 0;
    bool publisher_creation_fenced = false;
    uint64_t local_publishers = 0;
    uint64_t local_readers = 0;
    uint64_t outstanding_receipts = 0;
    uint64_t outstanding_borrows = 0;
    uint64_t queue_depth = 0;
    uint64_t last_published_sequence = 0;
    uint64_t last_consumed_sequence = 0;
    uint64_t observed_samples = 0;
    uint64_t duplicate_count = 0;
    uint64_t unexplained_loss_count = 0;
};

struct LocalBusConfig {
    NodeId node_id{};
    SecurityDomainId security_domain_id{1};
    uint64_t lease_epoch = 1;
    uint64_t lease_duration_ns = 60ull * 1'000'000'000ull;
    uint32_t region_id = 1;
    uint64_t region_bytes = 64u * 1024u * 1024u;
    std::filesystem::path topic_id_state_path;
    std::vector<LocalTopicConfig> topics;
};

// Production, local-only Bus assembly. Each configured topic owns a real
// BroadcastChannel and a fixed, bounded canonical-payload region retained by
// LocalPublicationBinding. The deployment intentionally supports one publisher
// per topic, matching BroadcastChannel's single-publisher contract.
class LocalBusDeployment final {
public:
    static Result<std::unique_ptr<LocalBusDeployment>> Create(
        LocalBusConfig config) noexcept;

    ~LocalBusDeployment();
    LocalBusDeployment(const LocalBusDeployment&) = delete;
    LocalBusDeployment& operator=(const LocalBusDeployment&) = delete;
    LocalBusDeployment(LocalBusDeployment&&) = delete;
    LocalBusDeployment& operator=(LocalBusDeployment&&) = delete;

    Bus& bus() noexcept;
    const Bus& bus() const noexcept;

    // Cold-path aggregate. No topic or node identity is exposed to monitoring.
    LocalBusOperationalStats OperationalStats(uint64_t now_ns) const noexcept;
    uint64_t SweepExpiredSubscribers(uint64_t now_ns) noexcept;

    // Cold production-upgrade seams. They are backed by the installed real
    // BroadcastChannel and endpoint lifecycle state; no caller supplies proof
    // booleans. BindRoutingCatalog makes every subsequently opened endpoint and
    // TransportSwitcher refresh read the CRC-validated durable catalog.
    Status BindRoutingCatalog(std::filesystem::path catalog_path);
    Result<LocalBusUpgradeTopicStats> UpgradeTopicStats(TopicId topic_id) const;
    Status FenceUpgradePublisher(TopicId topic_id);
    Status UnfenceUpgradePublisher(TopicId topic_id);
    Status RefreshUpgradeRoute(TopicId topic_id);
    registry::Coordinator& coordinator() noexcept;

private:
    class Impl;
    explicit LocalBusDeployment(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::deployment

#endif  // MINO_RUNTIME_DEPLOYMENT_LOCAL_BUS_H_
