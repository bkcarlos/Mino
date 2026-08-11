// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/platform/numa.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <climits>
#include <cstdint>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>
#include <string>
#include <system_error>

#if defined(__linux__)
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace mino {
namespace {

constexpr uint32_t kMaximumTopologyId = 1u << 20;
constexpr int kMpolBind = 2;
constexpr int kMpolInterleave = 3;

std::string Trim(std::string value) {
    const size_t first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    const size_t last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string ReadTextFile(const std::string& path) {
    std::ifstream input(path);
    if (!input) return {};
    std::ostringstream contents;
    contents << input.rdbuf();
    return Trim(contents.str());
}

std::string StatusList(std::string_view status, std::string_view key) {
    size_t begin = 0;
    while (begin < status.size()) {
        const size_t end = status.find('\n', begin);
        const std::string_view line = status.substr(
            begin, end == std::string_view::npos ? status.size() - begin
                                                 : end - begin);
        if (line.starts_with(key)) {
            return Trim(std::string(line.substr(key.size())));
        }
        if (end == std::string_view::npos) break;
        begin = end + 1;
    }
    return {};
}

std::vector<uint32_t> Intersect(const std::vector<uint32_t>& left,
                                const std::vector<uint32_t>& right) {
    std::vector<uint32_t> result;
    std::set_intersection(left.begin(), left.end(), right.begin(), right.end(),
                          std::back_inserter(result));
    return result;
}

Result<std::vector<uint32_t>> OptionalRange(std::string_view text,
                                            const std::vector<uint32_t>& fallback) {
    if (Trim(std::string(text)).empty()) return fallback;
    return ParseNumaRangeList(text);
}

Status ErrnoStatus(std::string_view operation, int error_number) {
    StatusCode code = StatusCode::kInternal;
    if (error_number == EACCES || error_number == EPERM) {
        code = StatusCode::kPermissionDenied;
    } else if (error_number == ENOSYS || error_number == ENODEV ||
               error_number == EOPNOTSUPP) {
        code = StatusCode::kUnsupported;
    } else if (error_number == EINVAL) {
        code = StatusCode::kInvalidArgument;
    } else if (error_number == ENOMEM) {
        code = StatusCode::kResourceExhausted;
    }
    return Status::Error(code, std::string(operation) + ": errno=" +
                                   std::to_string(error_number));
}

Result<NumaPlacementResult> PlacementFallback(
    NumaPlacementResult result, const NumaPlacementConfig& config,
    Status status, bool bind_error = false, int error_number = 0) {
    result.fallback = true;
    result.bind_error = bind_error;
    result.error_number = error_number;
    if (config.failure_policy == NumaFailurePolicy::kStrict) return status;
    return result;
}

#if defined(__linux__)
std::pair<std::string, std::string> CgroupCpusetPaths() {
    const std::string cgroup = ReadTextFile("/proc/self/cgroup");
    size_t begin = 0;
    while (begin < cgroup.size()) {
        const size_t end = cgroup.find('\n', begin);
        const std::string line = cgroup.substr(
            begin, end == std::string::npos ? cgroup.size() - begin
                                            : end - begin);
        const size_t first_colon = line.find(':');
        const size_t second_colon =
            first_colon == std::string::npos ? std::string::npos
                                              : line.find(':', first_colon + 1);
        if (second_colon != std::string::npos) {
            const std::string controllers =
                line.substr(first_colon + 1, second_colon - first_colon - 1);
            std::string relative = line.substr(second_colon + 1);
            if (relative.empty() || relative.front() != '/') relative.insert(0, "/");
            if (first_colon == 1 && line.front() == '0') {
                const std::string root = "/sys/fs/cgroup" + relative;
                return {root + "/cpuset.mems.effective",
                        root + "/cpuset.cpus.effective"};
            }
            if (controllers == "cpuset" ||
                controllers.find("cpuset,") != std::string::npos ||
                controllers.find(",cpuset") != std::string::npos) {
                const std::string root = "/sys/fs/cgroup/cpuset" + relative;
                return {root + "/cpuset.mems", root + "/cpuset.cpus"};
            }
        }
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    return {};
}
#endif

class NativeNumaSystemImpl final : public NumaSystem {
public:
    Result<NumaTopology> DiscoverTopology() const override {
#if defined(__linux__)
        NumaDiscoverySnapshot snapshot;
        snapshot.linux_native = true;
        snapshot.online_nodes =
            ReadTextFile("/sys/devices/system/node/online");
        if (snapshot.online_nodes.empty()) {
            snapshot.online_nodes = "0";
            snapshot.fallback_reason = "Linux NUMA node sysfs is unavailable";
        }
        MINO_ASSIGN_OR_RETURN(const std::vector<uint32_t> online_nodes,
                              ParseNumaRangeList(snapshot.online_nodes));
        for (uint32_t node : online_nodes) {
            snapshot.node_cpu_lists.emplace(
                node, ReadTextFile("/sys/devices/system/node/node" +
                                   std::to_string(node) + "/cpulist"));
        }
        if (online_nodes.size() == 1 &&
            snapshot.node_cpu_lists.begin()->second.empty()) {
            snapshot.node_cpu_lists.begin()->second =
                ReadTextFile("/sys/devices/system/cpu/online");
        }
        const std::string status = ReadTextFile("/proc/self/status");
        snapshot.process_allowed_mems =
            StatusList(status, "Mems_allowed_list:");
        snapshot.process_allowed_cpus =
            StatusList(status, "Cpus_allowed_list:");
        const auto cpuset_paths = CgroupCpusetPaths();
        snapshot.cgroup_allowed_mems = ReadTextFile(cpuset_paths.first);
        snapshot.cgroup_allowed_cpus = ReadTextFile(cpuset_paths.second);
        snapshot.current_cpu = CurrentCpu();
        return BuildNumaTopology(snapshot);
#else
        NumaDiscoverySnapshot snapshot;
        snapshot.linux_native = false;
        snapshot.online_nodes = "0";
        snapshot.node_cpu_lists.emplace(0, "0");
        snapshot.process_allowed_mems = "0";
        snapshot.process_allowed_cpus = "0";
        snapshot.current_cpu = 0;
        snapshot.fallback_reason = "native NUMA discovery is Linux-only";
        return BuildNumaTopology(snapshot);
#endif
    }

    int CurrentCpu() const noexcept override {
#if defined(__linux__)
        return ::sched_getcpu();
#else
        return 0;
#endif
    }

    NumaSyscallResult Mbind(void* address, size_t length, int mode,
                            const unsigned long* node_mask,
                            unsigned long max_node,
                            unsigned flags) const noexcept override {
#if defined(__linux__) && defined(SYS_mbind)
        errno = 0;
        const long result = ::syscall(SYS_mbind, address, length, mode, node_mask,
                                      max_node, flags);
        return {.result = static_cast<int>(result),
                .error_number = result == 0 ? 0 : errno};
#else
        (void)address;
        (void)length;
        (void)mode;
        (void)node_mask;
        (void)max_node;
        (void)flags;
        return {.result = -1, .error_number = ENOSYS};
#endif
    }
};

}  // namespace

int NumaTopology::NodeForCpu(int cpu) const noexcept {
    if (cpu < 0) return -1;
    for (const NumaNode& node : nodes) {
        if (std::binary_search(node.cpus.begin(), node.cpus.end(),
                               static_cast<uint32_t>(cpu))) {
            return static_cast<int>(node.id);
        }
    }
    return -1;
}

bool NumaTopology::IsNodeAllowed(uint32_t node) const noexcept {
    return std::binary_search(allowed_nodes.begin(), allowed_nodes.end(), node);
}

Result<std::vector<uint32_t>> ParseNumaRangeList(std::string_view text) {
    std::vector<uint32_t> values;
    const std::string cleaned = Trim(std::string(text));
    if (cleaned.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "NUMA range list is empty");
    }
    size_t begin = 0;
    while (begin < cleaned.size()) {
        const size_t comma = cleaned.find(',', begin);
        const std::string_view token(cleaned.data() + begin,
                                     (comma == std::string::npos ? cleaned.size()
                                                                 : comma) - begin);
        const size_t dash = token.find('-');
        const std::string_view first_text = token.substr(0, dash);
        const std::string_view last_text =
            dash == std::string_view::npos ? first_text : token.substr(dash + 1);
        uint32_t first = 0;
        uint32_t last = 0;
        const auto first_result = std::from_chars(
            first_text.data(), first_text.data() + first_text.size(), first);
        const auto last_result = std::from_chars(
            last_text.data(), last_text.data() + last_text.size(), last);
        if (first_text.empty() || last_text.empty() ||
            first_result.ec != std::errc() ||
            first_result.ptr != first_text.data() + first_text.size() ||
            last_result.ec != std::errc() ||
            last_result.ptr != last_text.data() + last_text.size() || first > last ||
            last > kMaximumTopologyId) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "invalid NUMA range list");
        }
        if (static_cast<uint64_t>(last) - first + values.size() + 1 >
            kMaximumTopologyId) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "NUMA range list is too large");
        }
        for (uint32_t value = first;; ++value) {
            values.push_back(value);
            if (value == last) break;
        }
        if (comma == std::string::npos) break;
        begin = comma + 1;
        if (begin == cleaned.size()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "invalid trailing comma in NUMA range list");
        }
    }
    std::sort(values.begin(), values.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
    return values;
}

Result<NumaTopology> BuildNumaTopology(const NumaDiscoverySnapshot& snapshot) {
    MINO_ASSIGN_OR_RETURN(std::vector<uint32_t> online_nodes,
                          ParseNumaRangeList(snapshot.online_nodes));
    NumaTopology topology;
    topology.linux_native = snapshot.linux_native;
    topology.current_cpu = snapshot.current_cpu;
    topology.fallback_reason = snapshot.fallback_reason;

    std::vector<uint32_t> all_cpus;
    for (uint32_t node_id : online_nodes) {
        auto found = snapshot.node_cpu_lists.find(node_id);
        std::vector<uint32_t> cpus;
        if (found != snapshot.node_cpu_lists.end() && !Trim(found->second).empty()) {
            MINO_ASSIGN_OR_RETURN(cpus, ParseNumaRangeList(found->second));
        }
        all_cpus.insert(all_cpus.end(), cpus.begin(), cpus.end());
        topology.nodes.push_back({.id = node_id, .cpus = std::move(cpus)});
    }
    std::sort(all_cpus.begin(), all_cpus.end());
    all_cpus.erase(std::unique(all_cpus.begin(), all_cpus.end()), all_cpus.end());

    MINO_ASSIGN_OR_RETURN(std::vector<uint32_t> process_nodes,
                          OptionalRange(snapshot.process_allowed_mems,
                                        online_nodes));
    MINO_ASSIGN_OR_RETURN(std::vector<uint32_t> cgroup_nodes,
                          OptionalRange(snapshot.cgroup_allowed_mems,
                                        process_nodes));
    topology.allowed_nodes = Intersect(online_nodes, process_nodes);
    topology.allowed_nodes = Intersect(topology.allowed_nodes, cgroup_nodes);

    MINO_ASSIGN_OR_RETURN(std::vector<uint32_t> process_cpus,
                          OptionalRange(snapshot.process_allowed_cpus, all_cpus));
    MINO_ASSIGN_OR_RETURN(std::vector<uint32_t> cgroup_cpus,
                          OptionalRange(snapshot.cgroup_allowed_cpus,
                                        process_cpus));
    topology.allowed_cpus = Intersect(all_cpus, process_cpus);
    topology.allowed_cpus = Intersect(topology.allowed_cpus, cgroup_cpus);
    if (topology.allowed_nodes.empty()) {
        return Status::Error(StatusCode::kUnavailable,
                             "cpuset permits no online NUMA memory node");
    }
    if (topology.allowed_cpus.empty() && !all_cpus.empty()) {
        return Status::Error(StatusCode::kUnavailable,
                             "cpuset permits no online NUMA CPU");
    }
    topology.current_node = topology.NodeForCpu(topology.current_cpu);
    topology.numa_available =
        topology.linux_native && topology.allowed_nodes.size() >= 2;
    if (!topology.numa_available && topology.fallback_reason.empty()) {
        topology.fallback_reason = topology.linux_native
                                       ? "only one NUMA memory node is allowed"
                                       : "native NUMA discovery is Linux-only";
    }
    return topology;
}

const NumaSystem& NativeNumaSystem() noexcept {
    static const NativeNumaSystemImpl system;
    return system;
}

Result<NumaPlacementResult> ApplyNumaPlacement(
    void* address, size_t length, const NumaPlacementConfig& config) {
    const NumaSystem& system =
        config.system == nullptr ? NativeNumaSystem() : *config.system;
    Result<NumaTopology> discovered = system.DiscoverTopology();
    if (!discovered.ok()) {
        NumaPlacementResult result;
        result.topology.fallback_reason = discovered.status().ToString();
        return PlacementFallback(std::move(result), config, discovered.status());
    }
    NumaPlacementResult result;
    result.topology = *discovered;
    if (config.policy == NumaMemoryPolicy::kDefault) return result;
    if (address == nullptr || length == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "NUMA placement extent is empty");
    }
    if (!result.topology.numa_available) {
        return PlacementFallback(
            std::move(result), config,
            Status::Error(StatusCode::kUnsupported,
                          result.topology.fallback_reason));
    }

    if (config.policy == NumaMemoryPolicy::kStripe) {
        result.effective_nodes = result.topology.allowed_nodes;
    } else {
        int node = config.node;
        if (config.policy == NumaMemoryPolicy::kLocal) {
            const int cpu = system.CurrentCpu();
            node = result.topology.NodeForCpu(cpu);
        }
        if (node < 0 ||
            !result.topology.IsNodeAllowed(static_cast<uint32_t>(node))) {
            return PlacementFallback(
                std::move(result), config,
                Status::Error(StatusCode::kInvalidArgument,
                              "requested NUMA node is not allowed by cpuset"));
        }
        result.effective_nodes.push_back(static_cast<uint32_t>(node));
    }

    const uint32_t highest = result.effective_nodes.back();
    constexpr size_t kBitsPerWord = sizeof(unsigned long) * CHAR_BIT;
    const size_t words = static_cast<size_t>(highest) / kBitsPerWord + 1;
    std::vector<unsigned long> mask(words, 0);
    for (uint32_t node : result.effective_nodes) {
        mask[node / kBitsPerWord] |=
            1ul << static_cast<unsigned long>(node % kBitsPerWord);
    }

#if defined(__linux__)
    const long page_size_value = ::sysconf(_SC_PAGESIZE);
#else
    const long page_size_value = 4096;
#endif
    if (page_size_value <= 0) {
        return PlacementFallback(
            std::move(result), config,
            Status::Error(StatusCode::kUnavailable,
                          "cannot determine system page size"));
    }
    const uintptr_t page_size = static_cast<uintptr_t>(page_size_value);
    const uintptr_t raw_begin = reinterpret_cast<uintptr_t>(address);
    const uintptr_t raw_end = raw_begin + length;
    if (raw_end < raw_begin) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "NUMA placement extent overflows address space");
    }
    const uintptr_t aligned_begin = raw_begin / page_size * page_size;
    if (raw_end > std::numeric_limits<uintptr_t>::max() - (page_size - 1)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "NUMA placement alignment overflows address space");
    }
    const uintptr_t aligned_end =
        (raw_end + page_size - 1) / page_size * page_size;
    const int mode = config.policy == NumaMemoryPolicy::kStripe
                         ? kMpolInterleave
                         : kMpolBind;
    const NumaSyscallResult bound = system.Mbind(
        reinterpret_cast<void*>(aligned_begin), aligned_end - aligned_begin, mode,
        mask.data(), static_cast<unsigned long>(highest) + 1, 0);
    if (bound.result != 0) {
        const int error_number = bound.error_number == 0 ? EIO : bound.error_number;
        return PlacementFallback(std::move(result), config,
                                 ErrnoStatus("mbind", error_number), true,
                                 error_number);
    }
    result.policy_applied = true;
    return result;
}

}  // namespace mino
