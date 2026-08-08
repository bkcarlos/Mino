// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/validations/pin_lease.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "benchmarks/validation/common/runtime.h"
#include "benchmarks/validation/common/stats.h"
#include "mino/registry/registry.h"

namespace mino::benchmarks::validation {
namespace {

class BenchmarkLivenessProbe final : public registry::LivenessProbe {
public:
    ProcessIdentityLiveness Probe(
        const ProcessIdentity&) const noexcept override {
        return dead_.load(std::memory_order_acquire)
            ? ProcessIdentityLiveness::kDead
            : ProcessIdentityLiveness::kAlive;
    }

    void MarkDead() noexcept { dead_.store(true, std::memory_order_release); }

private:
    std::atomic<bool> dead_{false};
};

registry::NodeRegistration BenchmarkNode() {
    const NodeId node_id{1};
    const ProcessIdentity identity{
        .node_id = 1,
        .process_id = 1,
        .process_epoch = 1,
        .start_time_ns = 1,
    };
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = Take(transport::EndpointDescriptor::Ipv4Tcp(address, 43141),
                         "EndpointDescriptor::Ipv4Tcp");
    return registry::NodeRegistration{
        .node_id = node_id,
        .process_identity = identity,
        .endpoints = {endpoint},
        .trust_domain = "benchmark",
        .health = registry::NodeHealth::kHealthy,
        .lease_epoch = 1,
        .lease_duration_ns = 100,
        .config_version = 1,
    };
}

registry::TopicMetadata BenchmarkTopic() {
    schema::CanonicalDigest digest{};
    digest[0] = std::byte{1};
    digest[31] = std::byte{0x5a};
    return registry::TopicMetadata{
        .topic_id = {},
        .name = "benchmark/pins",
        .channel_kind = registry::ChannelKind::kBroadcast,
        .delivery = {.reliability = registry::Reliability::kBestEffort,
                     .allow_drop = false},
        .queue_full_policy = QueueFullPolicy::kBlock,
        .schema = schema::SchemaIdentity(1, digest, 1, 1),
        .accepted_schemas = {},
        .route_policy = registry::RoutePolicy::kDiscovery,
        .static_routes = {},
        .route_set_version = 0,
        .capacity = 256,
        .max_publishers = 1,
        .max_subscribers = 1,
        .partition_count = 1,
        .record_topology = registry::RecordBackpressureTopology::kIsolated,
        .region_version = 1,
        .channel_version = 1,
        .acl_version = 1,
        .config_version = 0,
        .state = registry::TopicState::kCreating,
    };
}

}  // namespace

std::string RunPinLease(size_t pin_count) {
    auto probe = std::make_shared<BenchmarkLivenessProbe>();
    registry::CoordinatorLimits limits;
    limits.max_topic_pins = std::max<size_t>(pin_count + 16, 1024);
    auto coordinator = Take(registry::Coordinator::CreateForTesting(
                                limits,
                                std::make_shared<
                                    registry::InMemoryMonotonicIdAllocator>(),
                                probe),
                            "Coordinator::CreateForTesting");
    const auto node = BenchmarkNode();
    Take(coordinator->RegisterNode(node, 1), "Coordinator::RegisterNode");
    auto created = Take(coordinator->CreateTopic(BenchmarkTopic()),
                        "Coordinator::CreateTopic");
    const TopicId topic_id = created->metadata.topic_id;
    const registry::ActivationReadinessProof proof{
        .topic_id = topic_id,
        .config_version = created->metadata.config_version,
        .schema = created->metadata.schema,
        .region_version = created->metadata.region_version,
        .channel_version = created->metadata.channel_version,
        .acl_version = created->metadata.acl_version,
        .schema_ready = true,
        .region_ready = true,
        .channel_ready = true,
        .acl_ready = true,
    };
    Require(coordinator->ActivateTopic(topic_id, proof),
            "Coordinator::ActivateTopic");
    const registry::NodeLeaseOwner owner{
        .node_id = node.node_id,
        .process_identity = node.process_identity,
        .lease_epoch = node.lease_epoch,
    };

    std::vector<uint64_t> acquire_samples;
    std::vector<uint64_t> release_samples;
    acquire_samples.reserve(pin_count);
    release_samples.reserve(pin_count);
    for (size_t index = 0; index < pin_count; ++index) {
        const registry::TopicPinRegistration pin{
            .topic_id = topic_id,
            .pin_id = registry::TopicPinId{index + 1},
            .kind = registry::TopicPinKind::kRecorder,
            .generation = index + 1,
            .owner = owner,
        };
        auto begin = Clock::now();
        Require(coordinator->AcquireTopicPin(pin, 1),
                "Coordinator::AcquireTopicPin");
        acquire_samples.push_back(DurationNs(begin, Clock::now()));
        begin = Clock::now();
        Require(coordinator->ReleaseTopicPin(pin),
                "Coordinator::ReleaseTopicPin");
        release_samples.push_back(DurationNs(begin, Clock::now()));
    }

    for (size_t index = 0; index < pin_count; ++index) {
        const registry::TopicPinRegistration pin{
            .topic_id = topic_id,
            .pin_id = registry::TopicPinId{1'000'000 + index},
            .kind = registry::TopicPinKind::kRecorder,
            .generation = index + 1,
            .owner = owner,
        };
        Require(coordinator->AcquireTopicPin(pin, 1),
                "Coordinator::AcquireTopicPin(cleanup set)");
    }
    probe->MarkDead();
    const auto cleanup_begin = Clock::now();
    auto swept = Take(coordinator->SweepExpiredNodes(101),
                      "Coordinator::SweepExpiredNodes");
    const uint64_t cleanup_ns = DurationNs(cleanup_begin, Clock::now());
    if (swept.pins_removed != pin_count) {
        throw std::runtime_error(
            "lease cleanup did not remove every benchmark pin");
    }
    std::ostringstream output;
    output << "{\"status\":\"MEASURED\",\"pin_operations\":" << pin_count
           << ",\"acquire_latency_ns\":";
    WriteDistribution(output, Summarize(std::move(acquire_samples)));
    output << ",\"release_latency_ns\":";
    WriteDistribution(output, Summarize(std::move(release_samples)));
    output << ",\"lease_cleanup\":{\"expired_owner_pins\":" << pin_count
           << ",\"pins_removed\":" << swept.pins_removed
           << ",\"elapsed_ns\":" << cleanup_ns
           << ",\"liveness\":\"explicitly marked dead after lease deadline\"}}";
    return output.str();
}

}  // namespace mino::benchmarks::validation
