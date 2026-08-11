// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_PLATFORM_NUMA_H_
#define MINO_PLATFORM_NUMA_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {

enum class NumaMemoryPolicy : uint8_t {
    kDefault = 0,
    kLocal = 1,
    kNode = 2,
    kStripe = 3,
};

enum class NumaFailurePolicy : uint8_t {
    kFallback = 0,
    kStrict = 1,
};

struct NumaNode {
    uint32_t id = 0;
    std::vector<uint32_t> cpus;
};

struct NumaTopology {
    bool linux_native = false;
    bool numa_available = false;
    std::string fallback_reason;
    std::vector<NumaNode> nodes;
    std::vector<uint32_t> allowed_nodes;
    std::vector<uint32_t> allowed_cpus;
    int current_cpu = -1;
    int current_node = -1;

    int NodeForCpu(int cpu) const noexcept;
    bool IsNodeAllowed(uint32_t node) const noexcept;
};

// A value-only input used by parser tests and alternate topology providers. The
// native provider fills it from sysfs, /proc/self/status and the active cgroup
// cpuset; no process-global state is consulted by BuildNumaTopology().
struct NumaDiscoverySnapshot {
    bool linux_native = false;
    std::string online_nodes;
    std::map<uint32_t, std::string> node_cpu_lists;
    std::string process_allowed_mems;
    std::string cgroup_allowed_mems;
    std::string process_allowed_cpus;
    std::string cgroup_allowed_cpus;
    int current_cpu = -1;
    std::string fallback_reason;
};

Result<std::vector<uint32_t>> ParseNumaRangeList(std::string_view text);
Result<NumaTopology> BuildNumaTopology(const NumaDiscoverySnapshot& snapshot);

struct NumaSyscallResult {
    int result = 0;
    int error_number = 0;
};

// Injection boundary for topology discovery, migration checks and mbind. The
// native implementation invokes Linux syscalls directly and never links libnuma.
class NumaSystem {
public:
    virtual ~NumaSystem() = default;
    virtual Result<NumaTopology> DiscoverTopology() const = 0;
    virtual int CurrentCpu() const noexcept = 0;
    virtual NumaSyscallResult Mbind(void* address, size_t length, int mode,
                                    const unsigned long* node_mask,
                                    unsigned long max_node,
                                    unsigned flags) const noexcept = 0;
};

const NumaSystem& NativeNumaSystem() noexcept;

struct NumaPlacementConfig {
    NumaMemoryPolicy policy = NumaMemoryPolicy::kDefault;
    int node = -1;
    NumaFailurePolicy failure_policy = NumaFailurePolicy::kFallback;
    const NumaSystem* system = nullptr;
};

struct NumaPlacementResult {
    NumaTopology topology;
    std::vector<uint32_t> effective_nodes;
    bool policy_applied = false;
    bool fallback = false;
    bool bind_error = false;
    int error_number = 0;
};

// Applies policy to an existing mapped extent. kLocal resolves the calling
// thread's current node at call time; kNode binds one configured allowed node;
// kStripe interleaves pages across all allowed nodes. In fallback mode an
// unsupported topology or mbind error is returned as a successful result with
// fallback=true; strict mode returns an error Status.
Result<NumaPlacementResult> ApplyNumaPlacement(
    void* address, size_t length, const NumaPlacementConfig& config);

}  // namespace mino

#endif  // MINO_PLATFORM_NUMA_H_
