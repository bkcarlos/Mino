// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/platform/numa.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>

namespace mino {
namespace {

NumaTopology TwoNodeTopology() {
    NumaDiscoverySnapshot snapshot;
    snapshot.linux_native = true;
    snapshot.online_nodes = "0-1";
    snapshot.node_cpu_lists = {{0, "0-3"}, {1, "4-7"}};
    snapshot.process_allowed_mems = "0-1";
    snapshot.process_allowed_cpus = "0-7";
    snapshot.current_cpu = 1;
    auto topology = BuildNumaTopology(snapshot);
    EXPECT_TRUE(topology.ok()) << topology.status().ToString();
    return *topology;
}

class FakeNumaSystem final : public NumaSystem {
public:
    Result<NumaTopology> DiscoverTopology() const override { return topology; }
    int CurrentCpu() const noexcept override { return current_cpu; }
    NumaSyscallResult Mbind(void*, size_t, int mode,
                            const unsigned long* node_mask,
                            unsigned long max_node,
                            unsigned) const noexcept override {
        ++bind_calls;
        observed_mode = mode;
        observed_max_node = max_node;
        observed_mask = node_mask == nullptr ? 0 : node_mask[0];
        return bind_result;
    }

    NumaTopology topology = TwoNodeTopology();
    int current_cpu = 1;
    NumaSyscallResult bind_result;
    mutable int bind_calls = 0;
    mutable int observed_mode = -1;
    mutable unsigned long observed_max_node = 0;
    mutable unsigned long observed_mask = 0;
};

TEST(NumaTopologyTest, ParsesRangesAndIntersectsProcessAndCgroupCpuset) {
    NumaDiscoverySnapshot snapshot;
    snapshot.linux_native = true;
    snapshot.online_nodes = "0-3";
    snapshot.node_cpu_lists = {
        {0, "0-1"}, {1, "2-3"}, {2, "4-5"}, {3, "6-7"}};
    snapshot.process_allowed_mems = "0-2";
    snapshot.cgroup_allowed_mems = "1-3";
    snapshot.process_allowed_cpus = "1-6";
    snapshot.cgroup_allowed_cpus = "2-5";
    snapshot.current_cpu = 4;

    auto topology = BuildNumaTopology(snapshot);
    ASSERT_TRUE(topology.ok()) << topology.status().ToString();
    EXPECT_EQ(topology->allowed_nodes, (std::vector<uint32_t>{1, 2}));
    EXPECT_EQ(topology->allowed_cpus,
              (std::vector<uint32_t>{2, 3, 4, 5}));
    EXPECT_EQ(topology->current_node, 2);
    EXPECT_TRUE(topology->numa_available);
}

TEST(NumaTopologyTest, RejectsEmptyCpusetIntersection) {
    NumaDiscoverySnapshot snapshot;
    snapshot.linux_native = true;
    snapshot.online_nodes = "0-1";
    snapshot.node_cpu_lists = {{0, "0"}, {1, "1"}};
    snapshot.process_allowed_mems = "0";
    snapshot.cgroup_allowed_mems = "1";
    snapshot.process_allowed_cpus = "0-1";

    auto topology = BuildNumaTopology(snapshot);
    ASSERT_FALSE(topology.ok());
    EXPECT_EQ(topology.status().code(), StatusCode::kUnavailable);
}

TEST(NumaTopologyTest, NonLinuxAndSingleNodeAreExplicitFallbacks) {
    NumaDiscoverySnapshot snapshot;
    snapshot.linux_native = false;
    snapshot.online_nodes = "0";
    snapshot.node_cpu_lists = {{0, "0"}};
    snapshot.process_allowed_mems = "0";
    snapshot.process_allowed_cpus = "0";
    snapshot.current_cpu = 0;

    auto topology = BuildNumaTopology(snapshot);
    ASSERT_TRUE(topology.ok());
    EXPECT_FALSE(topology->numa_available);
    EXPECT_FALSE(topology->fallback_reason.empty());
}

TEST(NumaPlacementTest, StripeUsesAllAllowedNodesWithoutLibnuma) {
    FakeNumaSystem system;
    alignas(4096) std::byte extent[4096]{};
    auto result = ApplyNumaPlacement(
        extent, sizeof(extent),
        {.policy = NumaMemoryPolicy::kStripe,
         .failure_policy = NumaFailurePolicy::kStrict,
         .system = &system});
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_TRUE(result->policy_applied);
    EXPECT_EQ(result->effective_nodes, (std::vector<uint32_t>{0, 1}));
    EXPECT_EQ(system.bind_calls, 1);
    EXPECT_EQ(system.observed_mode, 3);
    EXPECT_EQ(system.observed_max_node, 2u);
    EXPECT_EQ(system.observed_mask & 0x3ul, 0x3ul);
}

TEST(NumaPlacementTest, StrictMbindFailureFailsAndFallbackRecordsError) {
    FakeNumaSystem system;
    system.bind_result = {.result = -1, .error_number = EPERM};
    alignas(4096) std::byte extent[4096]{};
    auto strict = ApplyNumaPlacement(
        extent, sizeof(extent),
        {.policy = NumaMemoryPolicy::kNode,
         .node = 1,
         .failure_policy = NumaFailurePolicy::kStrict,
         .system = &system});
    ASSERT_FALSE(strict.ok());
    EXPECT_EQ(strict.status().code(), StatusCode::kPermissionDenied);

    auto fallback = ApplyNumaPlacement(
        extent, sizeof(extent),
        {.policy = NumaMemoryPolicy::kNode,
         .node = 1,
         .failure_policy = NumaFailurePolicy::kFallback,
         .system = &system});
    ASSERT_TRUE(fallback.ok());
    EXPECT_TRUE(fallback->fallback);
    EXPECT_TRUE(fallback->bind_error);
    EXPECT_EQ(fallback->error_number, EPERM);
}

}  // namespace
}  // namespace mino
