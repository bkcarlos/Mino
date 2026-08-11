// Copyright 2026 The Mino Authors
//
// Rolling upgrade control-plane contracts. No type in this package is stored in
// shared memory; Region ABI and data-plane layouts remain unchanged.

#ifndef MINO_UPGRADE_UPGRADE_H_
#define MINO_UPGRADE_UPGRADE_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/registry/metadata.h"
#include "mino/security/security_domain.h"

namespace mino::upgrade {

inline constexpr uint16_t kUpgradeFormatVersion = 1;
inline constexpr size_t kMaximumUpgradeFileBytes = 4u * 1024u * 1024u;
inline constexpr size_t kMaximumUpgradeJournalEntries = 4096;
inline constexpr size_t kMaximumUpgradeTopics = 4096;
inline constexpr size_t kMaximumUpgradeStringBytes = 4096;

enum class UpgradePhase : uint8_t {
    kPrepare = 1,
    kValidate = 2,
    kDrain = 3,
    kCutover = 4,
    kObserve = 5,
    kCommit = 6,
    kRollback = 7,
    kFail = 8,
};

std::string_view UpgradePhaseName(UpgradePhase phase) noexcept;
bool IsTerminalPhase(UpgradePhase phase) noexcept;
bool IsPreCutoverPhase(UpgradePhase phase) noexcept;

struct RegionIdentity {
    std::string name;
    uint32_t region_id = 0;
    uint64_t uuid_lo = 0;
    uint64_t uuid_hi = 0;
    uint16_t layout_version = 0;
    SecurityDomainId security_domain;

    friend bool operator==(const RegionIdentity&, const RegionIdentity&) = default;
};

struct TopicBinding {
    TopicId topic_id;
    std::string name;
    uint64_t config_version = 0;
    uint64_t region_version = 0;
    uint64_t channel_version = 0;
    uint64_t acl_version = 0;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    registry::TopicAcl acl;

    friend bool operator==(const TopicBinding& lhs,
                           const TopicBinding& rhs) noexcept {
        return lhs.topic_id == rhs.topic_id && lhs.name == rhs.name &&
               lhs.config_version == rhs.config_version &&
               lhs.region_version == rhs.region_version &&
               lhs.channel_version == rhs.channel_version &&
               lhs.acl_version == rhs.acl_version && lhs.acl == rhs.acl &&
               registry::SchemaIdentityEqual(lhs.schema, rhs.schema);
    }
};

struct TopicUpgrade {
    TopicBinding source;
    TopicBinding target;

    friend bool operator==(const TopicUpgrade&, const TopicUpgrade&) = default;
};

struct UpgradePlan {
    std::string operation_id;
    std::string commit_token;
    RegionIdentity source_region;
    RegionIdentity target_region;
    std::vector<TopicUpgrade> topics;
    uint64_t required_shm_bytes = 0;
    uint32_t required_publisher_slots = 0;
    uint32_t required_subscriber_slots = 0;
    uint64_t minimum_observation_samples = 1;

    friend bool operator==(const UpgradePlan&, const UpgradePlan&) = default;
};

struct UpgradeJournalEntry {
    uint64_t sequence = 0;
    UpgradePhase phase = UpgradePhase::kPrepare;
    uint64_t timestamp_ns = 0;
    std::string detail;

    friend bool operator==(const UpgradeJournalEntry&,
                           const UpgradeJournalEntry&) = default;
};

struct UpgradeSnapshot {
    uint64_t generation = 0;
    uint64_t created_at_ns = 0;
    uint64_t updated_at_ns = 0;
    UpgradePhase phase = UpgradePhase::kPrepare;
    UpgradePlan plan;
    std::vector<UpgradeJournalEntry> journal;
    std::string terminal_reason;

    friend bool operator==(const UpgradeSnapshot&, const UpgradeSnapshot&) = default;
};

// Every field is required at validate. Security/ACL/schema failures are never
// downgraded to warnings and cannot be bypassed by elapsed time.
struct TargetReadinessProof {
    RegionIdentity region;
    std::vector<TopicBinding> topics;
    bool region_active = false;
    bool processes_ready = false;
    bool channels_ready = false;
    bool routes_ready = false;
    bool schema_bidirectionally_compatible = false;
    bool acl_exactly_preserved = false;
    bool capacity_admitted = false;
    uint64_t available_shm_bytes = 0;
    uint32_t available_publisher_slots = 0;
    uint32_t available_subscriber_slots = 0;
};

// Drain is a conservation proof, not a timer. All old publishers/subscribers,
// object pins, receipts, borrows and queue entries must be accounted for.
struct DrainProof {
    bool old_publishers_fenced = false;
    uint64_t publishers = 0;
    uint64_t subscribers = 0;
    uint64_t pins = 0;
    uint64_t outstanding_receipts = 0;
    uint64_t outstanding_borrows = 0;
    uint64_t queue_depth = 0;
    uint64_t last_published_sequence = 0;
    uint64_t last_consumed_sequence = 0;

    bool complete() const noexcept {
        return old_publishers_fenced && publishers == 0 && subscribers == 0 &&
               pins == 0 && outstanding_receipts == 0 &&
               outstanding_borrows == 0 && queue_depth == 0 &&
               last_consumed_sequence >= last_published_sequence;
    }
};

struct CutoverObservation {
    std::string acknowledged_commit_token;
    RegionIdentity active_region;
    uint64_t old_publisher_count = 0;
    uint64_t new_publisher_count = 0;
    uint64_t observed_samples = 0;
    uint64_t duplicate_count = 0;
    uint64_t unexplained_loss_count = 0;
};

// Post-cutover rollback is exceptional. It is safe only after target writers
// are fenced, source is ready, and target publications are either absent or
// explicitly reconciled by sequence/receipt identity.
struct SafeRollbackProof {
    bool target_publishers_fenced = false;
    bool source_ready = false;
    uint64_t target_publications_after_cutover = 0;
    bool sequence_receipt_reconciliation_complete = false;

    bool safe() const noexcept {
        return target_publishers_fenced && source_ready &&
               (target_publications_after_cutover == 0 ||
                sequence_receipt_reconciliation_complete);
    }
};

Status ValidateRegionIdentity(const RegionIdentity& region);
Status ValidateUpgradePlan(const UpgradePlan& plan);
Status ValidateTargetReadiness(const UpgradePlan& plan,
                               const TargetReadinessProof& proof);

// Deployment adapters implement these methods against Registry, process
// supervisors and routing. Every method must be idempotent for commit_token.
class UpgradeControlPlane {
public:
    virtual ~UpgradeControlPlane() = default;
    virtual Status Prepare(const UpgradePlan& plan) = 0;
    virtual Result<TargetReadinessProof> ObserveTarget(
        const UpgradePlan& plan) = 0;
    virtual Status BeginDrain(const UpgradePlan& plan) = 0;
    virtual Result<DrainProof> ObserveDrain(const UpgradePlan& plan) = 0;
    virtual Status Cutover(const UpgradePlan& plan) = 0;
    virtual Result<CutoverObservation> ObserveCutover(
        const UpgradePlan& plan) = 0;
    virtual Status Commit(const UpgradePlan& plan) = 0;
    virtual Result<SafeRollbackProof> ObserveRollbackSafety(
        const UpgradePlan& plan) = 0;
    virtual Status Rollback(const UpgradePlan& plan, bool after_cutover) = 0;
};

}  // namespace mino::upgrade

#endif  // MINO_UPGRADE_UPGRADE_H_