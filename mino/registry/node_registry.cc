// Copyright 2026 The Mino Authors

#include "mino/registry/node_registry.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

namespace mino::registry {
namespace {

Status AllocationFailure() {
    return Status::Error(StatusCode::kResourceExhausted);
}

Result<uint64_t> LeaseDeadline(uint64_t now_ns, uint64_t duration_ns) {
    if (now_ns > std::numeric_limits<uint64_t>::max() - duration_ns) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "lease deadline overflows uint64");
    }
    return now_ns + duration_ns;
}

NodeMetadata BuildMetadata(const NodeRegistration& request, uint64_t now_ns,
                           uint64_t deadline_ns) {
    return NodeMetadata{
        .node_id = request.node_id,
        .process_identity = request.process_identity,
        .endpoints = request.endpoints,
        .trust_domain = request.trust_domain,
        .health = request.health,
        .lease_state = NodeLeaseState::kActive,
        .liveness = ProcessIdentityLiveness::kAlive,
        .lease_epoch = request.lease_epoch,
        .lease_duration_ns = request.lease_duration_ns,
        .last_heartbeat_ns = now_ns,
        .lease_deadline_ns = deadline_ns,
        .config_version = request.config_version,
    };
}

}  // namespace

ProcessIdentityLiveness PlatformLivenessProbe::Probe(
    const ProcessIdentity& identity) const noexcept {
    // OS PID namespaces are local to this host. A PID lookup must never prove a
    // process on another node dead, even if that PID is absent or reused here.
    if (identity.node_id != ProcessIdentity::Current().node_id) {
        return ProcessIdentityLiveness::kUnknown;
    }
    return ProbeProcessIdentity(identity);
}

Result<std::unique_ptr<NodeRegistry>> NodeRegistry::Create(
    CoordinatorLimits limits,
    std::shared_ptr<const LivenessProbe> liveness_probe) {
    try {
        MINO_RETURN_IF_ERROR(ValidateCoordinatorLimits(limits));
        if (!liveness_probe) {
            liveness_probe = std::make_shared<PlatformLivenessProbe>();
        }
        return std::unique_ptr<NodeRegistry>(
            new NodeRegistry(limits, std::move(liveness_probe)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

NodeRegistry::NodeRegistry(
    CoordinatorLimits limits,
    std::shared_ptr<const LivenessProbe> liveness_probe)
    : limits_(limits), liveness_probe_(std::move(liveness_probe)) {
    nodes_.reserve(limits_.max_nodes);
}

bool NodeRegistry::SameConfiguration(
    const NodeMetadata& current,
    const NodeRegistration& request) noexcept {
    return current.node_id == request.node_id &&
           current.process_identity == request.process_identity &&
           current.endpoints == request.endpoints &&
           current.trust_domain == request.trust_domain &&
           current.health == request.health &&
           current.lease_epoch == request.lease_epoch &&
           current.lease_duration_ns == request.lease_duration_ns &&
           current.config_version == request.config_version;
}

bool NodeRegistry::OwnerMatches(const NodeMetadata& current,
                                const NodeLeaseOwner& owner) noexcept {
    return current.node_id == owner.node_id &&
           current.process_identity == owner.process_identity &&
           current.lease_epoch == owner.lease_epoch;
}

Result<NodeRegistrationOutcome> NodeRegistry::Register(
    const NodeRegistration& request, uint64_t now_ns) {
    try {
        MINO_RETURN_IF_ERROR(ValidateNodeRegistration(request, limits_));
        MINO_ASSIGN_OR_RETURN(const uint64_t deadline_ns,
                              LeaseDeadline(now_ns, request.lease_duration_ns));
        NodeMetadata candidate = BuildMetadata(request, now_ns, deadline_ns);
        auto candidate_snapshot =
            std::make_shared<const NodeMetadata>(candidate);

        for (;;) {
            ProcessIdentity identity_to_probe;
            uint64_t observed_generation = 0;
            {
                std::lock_guard lock(mutex_);
                auto found = nodes_.find(request.node_id);
                if (found == nodes_.end()) {
                    if (nodes_.size() >= limits_.max_nodes) {
                        return Status::Error(StatusCode::kResourceExhausted,
                                             "node registry capacity reached");
                    }
                    nodes_.emplace(request.node_id,
                                   NodeEntry{.metadata = candidate});
                    ++version_;
                    return NodeRegistrationOutcome{
                        .node = std::move(candidate_snapshot),
                        .displaced_owner = std::nullopt,
                    };
                }

                NodeEntry& entry = found->second;
                NodeMetadata& current = entry.metadata;
                if (current.process_identity == request.process_identity &&
                    current.lease_epoch == request.lease_epoch) {
                    if (now_ns < current.last_heartbeat_ns) {
                        return Status::Error(
                            StatusCode::kInvalidArgument,
                            "stale node registration heartbeat");
                    }
                    if (!SameConfiguration(current, request)) {
                        return Status::Error(
                            StatusCode::kAlreadyExists,
                            "same node lease is registered with different configuration");
                    }
                    NodeMetadata refreshed = current;
                    refreshed.last_heartbeat_ns = now_ns;
                    refreshed.lease_deadline_ns = deadline_ns;
                    refreshed.lease_state = NodeLeaseState::kActive;
                    refreshed.liveness = ProcessIdentityLiveness::kAlive;
                    auto published =
                        std::make_shared<const NodeMetadata>(refreshed);
                    current = std::move(refreshed);
                    ++entry.generation;
                    ++version_;
                    return NodeRegistrationOutcome{
                        .node = std::move(published),
                        .displaced_owner = std::nullopt,
                    };
                }

                if (now_ns < current.lease_deadline_ns) {
                    return Status::Error(
                        StatusCode::kAlreadyExists,
                        "node has an unexpired process incarnation");
                }
                identity_to_probe = current.process_identity;
                observed_generation = entry.generation;
            }

            // LivenessProbe is user-supplied and may re-enter registry APIs.
            // Never invoke it while mutex_ is held.
            const ProcessIdentityLiveness liveness =
                liveness_probe_->Probe(identity_to_probe);
            std::optional<NodeMetadata> replacement;
            if (liveness == ProcessIdentityLiveness::kDead) {
                replacement.emplace(candidate);
            }

            std::lock_guard lock(mutex_);
            auto found = nodes_.find(request.node_id);
            if (found == nodes_.end() ||
                found->second.generation != observed_generation ||
                found->second.metadata.process_identity != identity_to_probe) {
                continue;
            }
            NodeEntry& entry = found->second;
            NodeMetadata& current = entry.metadata;
            if (now_ns < current.lease_deadline_ns) {
                continue;
            }
            if (liveness != ProcessIdentityLiveness::kDead) {
                if (current.lease_state != NodeLeaseState::kExpired ||
                    current.liveness != liveness ||
                    current.health != NodeHealth::kUnavailable) {
                    current.lease_state = NodeLeaseState::kExpired;
                    current.liveness = liveness;
                    current.health = NodeHealth::kUnavailable;
                    ++entry.generation;
                    ++version_;
                }
                return Status::Error(
                    liveness == ProcessIdentityLiveness::kUnknown
                        ? StatusCode::kUnavailable
                        : StatusCode::kAlreadyExists,
                    "expired node cannot be replaced until exact owner is dead");
            }

            const NodeLeaseOwner displaced{
                .node_id = current.node_id,
                .process_identity = current.process_identity,
                .lease_epoch = current.lease_epoch,
            };
            current = std::move(*replacement);
            ++entry.generation;
            ++version_;
            return NodeRegistrationOutcome{
                .node = std::move(candidate_snapshot),
                .displaced_owner = displaced,
            };
        }
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status NodeRegistry::Heartbeat(const NodeLeaseOwner& owner, NodeHealth health,
                               uint64_t now_ns) {
    try {
        if (owner.node_id.value == 0 || owner.process_identity.IsZero() ||
            owner.lease_epoch == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "heartbeat owner is incomplete");
        }
        switch (health) {
            case NodeHealth::kHealthy:
            case NodeHealth::kDegraded:
            case NodeHealth::kUnavailable:
                break;
            default:
                return Status::Error(StatusCode::kInvalidArgument,
                                     "heartbeat health is invalid");
        }
        std::lock_guard lock(mutex_);
        auto found = nodes_.find(owner.node_id);
        if (found == nodes_.end() ||
            !OwnerMatches(found->second.metadata, owner)) {
            return Status::Error(StatusCode::kNotFound,
                                 "node lease owner does not match");
        }
        NodeEntry& entry = found->second;
        if (now_ns < entry.metadata.last_heartbeat_ns) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "stale node heartbeat");
        }
        MINO_ASSIGN_OR_RETURN(
            const uint64_t deadline_ns,
            LeaseDeadline(now_ns, entry.metadata.lease_duration_ns));
        entry.metadata.health = health;
        entry.metadata.lease_state = NodeLeaseState::kActive;
        entry.metadata.liveness = ProcessIdentityLiveness::kAlive;
        entry.metadata.last_heartbeat_ns = now_ns;
        entry.metadata.lease_deadline_ns = deadline_ns;
        ++entry.generation;
        ++version_;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Status NodeRegistry::Update(const NodeRegistration& replacement,
                            uint64_t expected_config_version,
                            uint64_t now_ns) {
    try {
        MINO_RETURN_IF_ERROR(ValidateNodeRegistration(replacement, limits_));
        if (replacement.config_version != expected_config_version + 1 ||
            expected_config_version == std::numeric_limits<uint64_t>::max()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "node config version must advance by one");
        }
        MINO_ASSIGN_OR_RETURN(
            const uint64_t deadline_ns,
            LeaseDeadline(now_ns, replacement.lease_duration_ns));
        NodeMetadata candidate =
            BuildMetadata(replacement, now_ns, deadline_ns);

        std::lock_guard lock(mutex_);
        auto found = nodes_.find(replacement.node_id);
        if (found == nodes_.end()) {
            return Status::Error(StatusCode::kNotFound, "node not registered");
        }
        NodeEntry& entry = found->second;
        if (entry.metadata.config_version != expected_config_version) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "node config CAS version mismatch");
        }
        if (entry.metadata.process_identity != replacement.process_identity ||
            entry.metadata.lease_epoch != replacement.lease_epoch) {
            return Status::Error(StatusCode::kUnsupported,
                                 "node identity and lease epoch are immutable");
        }
        entry.metadata = std::move(candidate);
        ++entry.generation;
        ++version_;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<NodeSweepResult> NodeRegistry::SweepExpired(uint64_t now_ns) {
    try {
        struct ProbeCandidate {
            NodeId node_id;
            ProcessIdentity identity;
            uint64_t generation;
        };
        struct ProbeResult {
            ProbeCandidate candidate;
            ProcessIdentityLiveness liveness;
        };

        std::vector<ProbeCandidate> candidates;
        {
            std::lock_guard lock(mutex_);
            candidates.reserve(nodes_.size());
            for (const auto& [node_id, entry] : nodes_) {
                if (now_ns >= entry.metadata.lease_deadline_ns) {
                    candidates.push_back(ProbeCandidate{
                        .node_id = node_id,
                        .identity = entry.metadata.process_identity,
                        .generation = entry.generation,
                    });
                }
            }
        }

        std::vector<ProbeResult> probe_results;
        probe_results.reserve(candidates.size());
        for (const ProbeCandidate& candidate : candidates) {
            // No registry mutex is held across this virtual call.
            probe_results.push_back(ProbeResult{
                .candidate = candidate,
                .liveness = liveness_probe_->Probe(candidate.identity),
            });
        }

        NodeSweepResult result;
        result.removed.reserve(probe_results.size());
        std::lock_guard lock(mutex_);
        for (const ProbeResult& observed : probe_results) {
            auto found = nodes_.find(observed.candidate.node_id);
            if (found == nodes_.end() ||
                found->second.generation != observed.candidate.generation ||
                found->second.metadata.process_identity !=
                    observed.candidate.identity ||
                now_ns < found->second.metadata.lease_deadline_ns) {
                continue;
            }
            NodeEntry& entry = found->second;
            NodeMetadata& node = entry.metadata;
            if (observed.liveness == ProcessIdentityLiveness::kDead) {
                result.removed.push_back(NodeLeaseOwner{
                    .node_id = node.node_id,
                    .process_identity = node.process_identity,
                    .lease_epoch = node.lease_epoch,
                });
                nodes_.erase(found);
                ++version_;
                continue;
            }
            if (node.lease_state != NodeLeaseState::kExpired ||
                node.liveness != observed.liveness ||
                node.health != NodeHealth::kUnavailable) {
                node.lease_state = NodeLeaseState::kExpired;
                node.liveness = observed.liveness;
                node.health = NodeHealth::kUnavailable;
                ++entry.generation;
                ++version_;
            }
            if (observed.liveness == ProcessIdentityLiveness::kUnknown) {
                ++result.retained_unknown;
            } else {
                ++result.retained_alive;
            }
        }
        return result;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::shared_ptr<const NodeMetadata>> NodeRegistry::Get(
    NodeId node_id) const {
    try {
        std::lock_guard lock(mutex_);
        const auto found = nodes_.find(node_id);
        if (found == nodes_.end()) {
            return Status::Error(StatusCode::kNotFound, "node not registered");
        }
        return std::make_shared<const NodeMetadata>(found->second.metadata);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::shared_ptr<const NodeRegistrySnapshot>> NodeRegistry::Snapshot()
    const {
    try {
        auto snapshot = std::make_shared<NodeRegistrySnapshot>();
        std::lock_guard lock(mutex_);
        snapshot->version = version_;
        snapshot->nodes.reserve(nodes_.size());
        for (const auto& [node_id, entry] : nodes_) {
            (void)node_id;
            snapshot->nodes.push_back(entry.metadata);
        }
        std::sort(snapshot->nodes.begin(), snapshot->nodes.end(),
                  [](const NodeMetadata& lhs, const NodeMetadata& rhs) {
                      return lhs.node_id < rhs.node_id;
                  });
        return std::shared_ptr<const NodeRegistrySnapshot>(std::move(snapshot));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

size_t NodeRegistry::size() const noexcept {
    std::lock_guard lock(mutex_);
    return nodes_.size();
}

uint64_t NodeRegistry::version() const noexcept {
    std::lock_guard lock(mutex_);
    return version_;
}

}  // namespace mino::registry
