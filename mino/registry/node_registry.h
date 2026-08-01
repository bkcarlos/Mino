// Copyright 2026 The Mino Authors

#ifndef MINO_REGISTRY_NODE_REGISTRY_H_
#define MINO_REGISTRY_NODE_REGISTRY_H_

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "mino/common/result.h"
#include "mino/registry/metadata.h"

namespace mino::registry {

class LivenessProbe {
public:
    virtual ~LivenessProbe() = default;
    virtual ProcessIdentityLiveness Probe(
        const ProcessIdentity& identity) const noexcept = 0;
};

class PlatformLivenessProbe final : public LivenessProbe {
public:
    ProcessIdentityLiveness Probe(
        const ProcessIdentity& identity) const noexcept override;
};

struct NodeRegistrationOutcome {
    std::shared_ptr<const NodeMetadata> node;
    // Set only when a proven-dead prior incarnation was displaced.
    std::optional<NodeLeaseOwner> displaced_owner;
};

struct NodeSweepResult {
    std::vector<NodeLeaseOwner> removed;
    size_t retained_alive = 0;
    size_t retained_unknown = 0;
};

// Bounded, single-authority node registry. It deliberately contains no leader
// election or replication: a deployment must instantiate exactly one
// authoritative Coordinator until a durable consensus implementation exists.
class NodeRegistry final {
public:
    static Result<std::unique_ptr<NodeRegistry>> Create(
        CoordinatorLimits limits = {},
        std::shared_ptr<const LivenessProbe> liveness_probe = {});

    NodeRegistry(const NodeRegistry&) = delete;
    NodeRegistry& operator=(const NodeRegistry&) = delete;

    Result<NodeRegistrationOutcome> Register(const NodeRegistration& request,
                                             uint64_t now_ns);
    Status Heartbeat(const NodeLeaseOwner& owner, NodeHealth health,
                     uint64_t now_ns);
    Status Update(const NodeRegistration& replacement,
                  uint64_t expected_config_version, uint64_t now_ns);

    Result<NodeSweepResult> SweepExpired(uint64_t now_ns);

    Result<std::shared_ptr<const NodeMetadata>> Get(NodeId node_id) const;
    Result<std::shared_ptr<const NodeRegistrySnapshot>> Snapshot() const;
    size_t size() const noexcept;
    uint64_t version() const noexcept;

private:
    struct NodeEntry {
        NodeMetadata metadata;
        // Internal mutation generation used to validate lock-free probe results.
        uint64_t generation = 1;
    };

    NodeRegistry(CoordinatorLimits limits,
                 std::shared_ptr<const LivenessProbe> liveness_probe);

    static bool SameConfiguration(const NodeMetadata& current,
                                  const NodeRegistration& request) noexcept;
    static bool OwnerMatches(const NodeMetadata& current,
                             const NodeLeaseOwner& owner) noexcept;

    CoordinatorLimits limits_;
    std::shared_ptr<const LivenessProbe> liveness_probe_;
    mutable std::mutex mutex_;
    std::unordered_map<NodeId, NodeEntry> nodes_;
    uint64_t version_ = 0;
};

}  // namespace mino::registry

#endif  // MINO_REGISTRY_NODE_REGISTRY_H_
