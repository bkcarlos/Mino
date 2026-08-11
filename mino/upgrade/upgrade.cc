// Copyright 2026 The Mino Authors

#include "mino/upgrade/upgrade.h"

#include <algorithm>
#include <limits>
#include <set>
#include <tuple>

#include "mino/common/status.h"

namespace mino::upgrade {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool CompleteSchema(const schema::SchemaIdentity& identity) noexcept {
    return identity.short_id() != 0 && identity.schema_version() != 0 &&
           identity.layout_version() != 0 &&
           std::any_of(identity.canonical_digest().begin(),
                       identity.canonical_digest().end(),
                       [](std::byte value) { return value != std::byte{0}; });
}

Status ValidateRegion(const RegionIdentity& region) {
    if (region.name.empty() || region.name.size() > kMaximumUpgradeStringBytes ||
        region.name.find('\0') != std::string::npos || region.region_id == 0 ||
        (region.uuid_lo == 0 && region.uuid_hi == 0) ||
        region.layout_version == 0 || region.security_domain.value == 0) {
        return Invalid("upgrade Region identity is incomplete or out of bounds");
    }
    return Status::Ok();
}

Status ValidateAcl(const registry::TopicAcl& acl) {
    if (acl.entries.empty() || acl.entries.size() > registry::kMaxTopicAclEntries) {
        return Invalid("upgrade Topic ACL is empty or out of bounds");
    }
    std::set<std::tuple<uint64_t, uint64_t>> subjects;
    for (const registry::TopicAclEntry& entry : acl.entries) {
        if (entry.node_id.value == 0 || entry.security_domain_id.value == 0 ||
            entry.permissions == 0 ||
            (entry.permissions & ~registry::kAllTopicPermissions) != 0 ||
            !subjects.emplace(entry.security_domain_id.value,
                              entry.node_id.value).second) {
            return Invalid("upgrade Topic ACL contains an invalid or duplicate subject");
        }
    }
    return Status::Ok();
}

Status ValidateTopic(const TopicBinding& topic) {
    if (topic.topic_id.value == 0 || topic.name.empty() ||
        topic.name.size() > registry::kMaxTopicNameBytes ||
        topic.name.find('\0') != std::string::npos || topic.config_version == 0 ||
        topic.region_version == 0 || topic.channel_version == 0 ||
        topic.acl_version == 0 || !CompleteSchema(topic.schema)) {
        return Invalid("upgrade Topic binding is incomplete or out of bounds");
    }
    return ValidateAcl(topic.acl);
}

}  // namespace

Status ValidateRegionIdentity(const RegionIdentity& region) {
    return ValidateRegion(region);
}

std::string_view UpgradePhaseName(UpgradePhase phase) noexcept {
    switch (phase) {
        case UpgradePhase::kPrepare:
            return "prepare";
        case UpgradePhase::kValidate:
            return "validate";
        case UpgradePhase::kDrain:
            return "drain";
        case UpgradePhase::kCutover:
            return "cutover";
        case UpgradePhase::kObserve:
            return "observe";
        case UpgradePhase::kCommit:
            return "commit";
        case UpgradePhase::kRollback:
            return "rollback";
        case UpgradePhase::kFail:
            return "fail";
    }
    return "unknown";
}

bool IsTerminalPhase(UpgradePhase phase) noexcept {
    return phase == UpgradePhase::kCommit || phase == UpgradePhase::kRollback ||
           phase == UpgradePhase::kFail;
}

bool IsPreCutoverPhase(UpgradePhase phase) noexcept {
    return phase == UpgradePhase::kPrepare || phase == UpgradePhase::kValidate ||
           phase == UpgradePhase::kDrain;
}

Status ValidateUpgradePlan(const UpgradePlan& plan) {
    if (plan.operation_id.empty() ||
        plan.operation_id.size() > kMaximumUpgradeStringBytes ||
        plan.operation_id.find('\0') != std::string::npos ||
        plan.commit_token.size() < 16 ||
        plan.commit_token.size() > kMaximumUpgradeStringBytes ||
        plan.commit_token.find('\0') != std::string::npos || plan.topics.empty() ||
        plan.topics.size() > kMaximumUpgradeTopics ||
        plan.minimum_observation_samples == 0) {
        return Invalid("upgrade operation, token, topics, or observation bound is invalid");
    }
    MINO_RETURN_IF_ERROR(ValidateRegion(plan.source_region));
    MINO_RETURN_IF_ERROR(ValidateRegion(plan.target_region));
    if (plan.source_region.security_domain != plan.target_region.security_domain) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "source and target Regions must share one Security Domain");
    }
    if (plan.source_region == plan.target_region ||
        (plan.source_region.uuid_lo == plan.target_region.uuid_lo &&
         plan.source_region.uuid_hi == plan.target_region.uuid_hi) ||
        plan.source_region.region_id == plan.target_region.region_id) {
        return Invalid("rolling upgrade requires a distinct target Region identity");
    }

    std::set<uint32_t> source_ids;
    std::set<uint32_t> target_ids;
    for (const TopicUpgrade& topic : plan.topics) {
        MINO_RETURN_IF_ERROR(ValidateTopic(topic.source));
        MINO_RETURN_IF_ERROR(ValidateTopic(topic.target));
        if (!source_ids.insert(topic.source.topic_id.value).second ||
            !target_ids.insert(topic.target.topic_id.value).second) {
            return Invalid("upgrade Topic IDs must be unique within each Region");
        }
        // D6-11 revocation semantics are preserved by requiring exact grants.
        // ACL version may advance for the replacement, but it may not roll back.
        if (!(topic.source.acl == topic.target.acl) ||
            topic.target.acl_version < topic.source.acl_version) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "target Topic must exactly preserve ACL grants and not roll back ACL version");
        }
    }
    return Status::Ok();
}

Status ValidateTargetReadiness(const UpgradePlan& plan,
                               const TargetReadinessProof& proof) {
    MINO_RETURN_IF_ERROR(ValidateUpgradePlan(plan));
    if (!(proof.region == plan.target_region)) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "target readiness belongs to another Region identity");
    }
    if (proof.region.security_domain != plan.source_region.security_domain) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "target readiness crosses the Security Domain boundary");
    }
    if (!proof.region_active || !proof.processes_ready || !proof.channels_ready ||
        !proof.routes_ready) {
        return Status::Error(StatusCode::kUnavailable,
                             "target Region, processes, channels, and routes must be ready");
    }
    if (!proof.acl_exactly_preserved) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "target ACL compatibility proof is absent");
    }
    if (!proof.schema_bidirectionally_compatible) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "target schema is not bidirectionally compatible");
    }
    if (!proof.capacity_admitted ||
        proof.available_shm_bytes < plan.required_shm_bytes ||
        proof.available_publisher_slots < plan.required_publisher_slots ||
        proof.available_subscriber_slots < plan.required_subscriber_slots) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "target capacity admission or headroom proof is insufficient");
    }
    if (proof.topics.size() != plan.topics.size()) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "target readiness Topic set is incomplete");
    }
    for (size_t index = 0; index < plan.topics.size(); ++index) {
        if (!(proof.topics[index] == plan.topics[index].target)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "target readiness Topic binding is stale or reordered");
        }
    }
    return Status::Ok();
}

}  // namespace mino::upgrade
