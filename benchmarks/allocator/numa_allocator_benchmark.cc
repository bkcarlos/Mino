// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/utsname.h>
#endif

#include "mino/platform/numa.h"
#include "mino/shm/allocator/central_slab.h"

namespace mino::benchmarks {
namespace {

using Clock = std::chrono::steady_clock;

struct Config {
    uint32_t threads = 4;
    uint64_t iterations = 100'000;
    uint32_t slots = 4096;
    std::string json_path;
    std::string qualification_attestation;
};

struct ModeResult {
    std::string name;
    uint32_t memory_node = 0;
    uint32_t cpu_node = 0;
    std::vector<uint32_t> pinned_cpus;
    uint64_t operations = 0;
    uint64_t failures = 0;
    uint64_t elapsed_ns = 0;
    uint64_t p50_ns = 0;
    uint64_t p95_ns = 0;
    uint64_t p99_ns = 0;
    uint64_t p999_ns = 0;
    uint64_t max_ns = 0;
    AllocatorLocalCacheStats stats;
};

uint64_t ParsePositive(std::string_view argument, std::string_view prefix) {
    const std::string value(argument.substr(prefix.size()));
    size_t parsed = 0;
    const unsigned long long result = std::stoull(value, &parsed, 10);
    if (parsed != value.size() || result == 0) {
        throw std::invalid_argument("NUMA benchmark values must be positive");
    }
    return static_cast<uint64_t>(result);
}

Config ParseArguments(int argc, char** argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument.starts_with("--threads=")) {
            const uint64_t value = ParsePositive(argument, "--threads=");
            if (value > 256) throw std::invalid_argument("--threads exceeds 256");
            config.threads = static_cast<uint32_t>(value);
        } else if (argument.starts_with("--iterations=")) {
            config.iterations = ParsePositive(argument, "--iterations=");
            if (config.iterations > 10'000'000) {
                throw std::invalid_argument("--iterations exceeds 10000000");
            }
        } else if (argument.starts_with("--slots=")) {
            const uint64_t value = ParsePositive(argument, "--slots=");
            if (value > 1'000'000) {
                throw std::invalid_argument("--slots exceeds 1000000");
            }
            config.slots = static_cast<uint32_t>(value);
        } else if (argument.starts_with("--json=")) {
            config.json_path = std::string(argument.substr(7));
        } else if (argument.starts_with("--qualification-attestation=")) {
            config.qualification_attestation = std::string(argument.substr(28));
        } else if (argument == "--help") {
            std::cout << "Usage: numa_allocator_benchmark [--threads=N] "
                         "[--iterations=N] [--slots=N] [--json=PATH] "
                         "[--qualification-attestation=physical-numa]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown argument: " +
                                        std::string(argument));
        }
    }
    if (config.slots < config.threads) {
        throw std::invalid_argument("--slots must be at least --threads");
    }
    return config;
}

std::string JsonEscape(std::string_view value) {
    std::ostringstream output;
    for (const unsigned char character : value) {
        switch (character) {
            case '"': output << "\\\""; break;
            case '\\': output << "\\\\"; break;
            case '\n': output << "\\n"; break;
            case '\r': output << "\\r"; break;
            case '\t': output << "\\t"; break;
            default:
                if (character < 0x20) {
                    output << "\\u" << std::hex << std::setw(4)
                           << std::setfill('0') << static_cast<int>(character)
                           << std::dec;
                } else {
                    output << character;
                }
        }
    }
    return output.str();
}

std::string TimestampUtc() {
    const std::time_t now = std::time(nullptr);
    std::tm value{};
#if defined(_WIN32)
    if (::gmtime_s(&value, &now) != 0) return "PENDING";
#else
    if (::gmtime_r(&now, &value) == nullptr) return "PENDING";
#endif
    char buffer[32]{};
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &value) ==
        0) {
        return "PENDING";
    }
    return buffer;
}

std::string OperatingSystem() {
#if defined(__linux__)
    struct utsname value {};
    if (::uname(&value) == 0) {
        return std::string(value.sysname) + " " + value.release + " " +
               value.machine;
    }
#endif
    return "non-Linux";
}

std::string CpuModel() {
    std::ifstream input("/proc/cpuinfo");
    std::string line;
    while (std::getline(input, line)) {
        if ((line.starts_with("model name") || line.starts_with("Hardware")) &&
            line.find(':') != std::string::npos) {
            return line.substr(line.find(':') + 1);
        }
    }
    return "PENDING";
}

std::string JsonArray(const std::vector<uint32_t>& values) {
    std::ostringstream output;
    output << '[';
    for (size_t index = 0; index < values.size(); ++index) {
        if (index != 0) output << ',';
        output << values[index];
    }
    output << ']';
    return output.str();
}

std::vector<uint32_t> AllowedCpusForNode(const NumaTopology& topology,
                                         uint32_t node_id) {
    for (const NumaNode& node : topology.nodes) {
        if (node.id != node_id) continue;
        std::vector<uint32_t> result;
        std::set_intersection(node.cpus.begin(), node.cpus.end(),
                              topology.allowed_cpus.begin(),
                              topology.allowed_cpus.end(),
                              std::back_inserter(result));
        return result;
    }
    return {};
}

bool PinCurrentThread(uint32_t cpu) {
#if defined(__linux__)
    cpu_set_t affinity;
    CPU_ZERO(&affinity);
    CPU_SET(cpu, &affinity);
    return ::pthread_setaffinity_np(::pthread_self(), sizeof(affinity),
                                    &affinity) == 0;
#else
    (void)cpu;
    return false;
#endif
}

class Mapping {
public:
    explicit Mapping(size_t size) : size_(size) {
#if defined(__linux__)
        address_ = ::mmap(nullptr, size, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (address_ == MAP_FAILED) address_ = nullptr;
#else
        address_ = ::operator new[](size, std::align_val_t(4096));
#endif
        if (address_ == nullptr) throw std::bad_alloc();
#if !defined(__linux__)
        std::memset(address_, 0, size);
#endif
    }

    ~Mapping() {
#if defined(__linux__)
        if (address_ != nullptr) (void)::munmap(address_, size_);
#else
        ::operator delete[](address_, std::align_val_t(4096));
#endif
    }

    void* get() const { return address_; }

private:
    void* address_ = nullptr;
    size_t size_ = 0;
};

uint64_t Percentile(const std::vector<uint64_t>& sorted, double percentile) {
    if (sorted.empty()) return 0;
    const size_t index = static_cast<size_t>(
        percentile * static_cast<double>(sorted.size() - 1));
    return sorted[index];
}

ModeResult RunMode(const Config& config, const NumaTopology& topology,
                   std::string name, NumaMemoryPolicy policy,
                   uint32_t memory_node, uint32_t cpu_node) {
    constexpr uint64_t kMetadataAllowance = 1u << 20;
    constexpr uint64_t kBytesPerSlotAllowance = 256;
    const uint64_t region_size =
        kMetadataAllowance + kBytesPerSlotAllowance * config.slots;
    Mapping region(static_cast<size_t>(region_size));
    ClassTableConfig classes;
    classes.classes = {{.slot_size = 64, .slot_count = config.slots}};
    const NumaPlacementConfig placement{
        .policy = policy,
        .node = policy == NumaMemoryPolicy::kNode
                    ? static_cast<int>(memory_node)
                    : -1,
        .failure_policy = NumaFailurePolicy::kStrict,
    };
    auto created = CentralSlabAllocator::Create(
        region.get(), region_size, classes,
        {.placement = placement, .prefer_local_shards = true});
    if (!created.ok()) throw std::runtime_error(created.status().ToString());
    CentralSlabAllocator allocator = *created;

    const std::vector<uint32_t> cpus = AllowedCpusForNode(topology, cpu_node);
    if (cpus.empty()) throw std::runtime_error("benchmark CPU node has no allowed CPU");
    AllocationRequest request;
    request.object_size = 32;
    request.type_id = TypeId{0xD602};
    request.schema = {.short_id = 0xD602, .layout_version = 1};

    std::atomic<uint32_t> ready{0};
    std::atomic<bool> start{false};
    std::atomic<uint64_t> failures{0};
    std::vector<std::vector<uint64_t>> samples(config.threads);
    std::vector<std::thread> workers;
    workers.reserve(config.threads);
    for (uint32_t thread_index = 0; thread_index < config.threads;
         ++thread_index) {
        workers.emplace_back([&, thread_index] {
            if (!PinCurrentThread(cpus[thread_index % cpus.size()])) {
                failures.fetch_add(config.iterations, std::memory_order_relaxed);
                ready.fetch_add(1, std::memory_order_release);
                return;
            }
            std::vector<uint64_t>& local_samples = samples[thread_index];
            local_samples.reserve(static_cast<size_t>(config.iterations));
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (uint64_t iteration = 0; iteration < config.iterations;
                 ++iteration) {
                const Clock::time_point begin = Clock::now();
                auto handle = allocator.Allocate(request);
                if (!handle.ok() || !allocator.Retire(*handle).ok() ||
                    !allocator.Reclaim(*handle).ok()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
                local_samples.push_back(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        Clock::now() - begin)
                        .count()));
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != config.threads) {
        std::this_thread::yield();
    }
    const Clock::time_point begin = Clock::now();
    start.store(true, std::memory_order_release);
    for (std::thread& worker : workers) worker.join();
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin)
            .count());

    std::vector<uint64_t> combined;
    combined.reserve(static_cast<size_t>(config.iterations * config.threads));
    for (std::vector<uint64_t>& local : samples) {
        combined.insert(combined.end(), local.begin(), local.end());
    }
    std::sort(combined.begin(), combined.end());
    return {
        .name = std::move(name),
        .memory_node = memory_node,
        .cpu_node = cpu_node,
        .pinned_cpus = cpus,
        .operations = config.iterations * config.threads,
        .failures = failures.load(std::memory_order_relaxed),
        .elapsed_ns = elapsed_ns,
        .p50_ns = Percentile(combined, 0.50),
        .p95_ns = Percentile(combined, 0.95),
        .p99_ns = Percentile(combined, 0.99),
        .p999_ns = Percentile(combined, 0.999),
        .max_ns = combined.empty() ? 0 : combined.back(),
        .stats = allocator.local_cache_stats(),
    };
}

std::string Report(const Config& config, const NumaTopology& topology,
                   std::string_view status, std::string_view reason,
                   const std::vector<ModeResult>& modes) {
    const bool passed = status == "PASSED";
    const bool eligible = passed && topology.numa_available &&
                          config.qualification_attestation == "physical-numa";
    std::ostringstream output;
    output << "{\n"
           << "  \"schema\": \"mino.allocator.numa_benchmark.v1\",\n"
           << "  \"status\": \"" << status << "\",\n"
           << "  \"reason\": \"" << JsonEscape(reason) << "\",\n"
           << "  \"qualification_eligible\": "
           << (eligible ? "true" : "false") << ",\n"
           << "  \"attestation\": \""
           << JsonEscape(config.qualification_attestation) << "\",\n"
           << "  \"provenance\": {\"timestamp_utc\": \""
           << TimestampUtc() << "\", \"os\": \""
           << JsonEscape(OperatingSystem()) << "\", \"cpu_model\": \""
           << JsonEscape(CpuModel()) << "\", \"logical_cpus\": "
           << std::thread::hardware_concurrency() << "},\n"
           << "  \"topology\": {\"linux_native\": "
           << (topology.linux_native ? "true" : "false")
           << ", \"numa_available\": "
           << (topology.numa_available ? "true" : "false")
           << ", \"allowed_nodes\": " << JsonArray(topology.allowed_nodes)
           << ", \"allowed_cpus\": " << JsonArray(topology.allowed_cpus)
           << ", \"fallback_reason\": \""
           << JsonEscape(topology.fallback_reason) << "\"},\n"
           << "  \"config\": {\"threads\": " << config.threads
           << ", \"iterations_per_thread\": " << config.iterations
           << ", \"slots\": " << config.slots << "},\n"
           << "  \"modes\": [";
    for (size_t index = 0; index < modes.size(); ++index) {
        const ModeResult& mode = modes[index];
        if (index != 0) output << ',';
        const double seconds = static_cast<double>(mode.elapsed_ns) / 1e9;
        const double throughput =
            seconds == 0 ? 0 : static_cast<double>(mode.operations) / seconds;
        output << "\n    {\"name\": \"" << mode.name
               << "\", \"memory_node\": " << mode.memory_node
               << ", \"cpu_node\": " << mode.cpu_node
               << ", \"pinned_cpus\": " << JsonArray(mode.pinned_cpus)
               << ", \"operations\": " << mode.operations
               << ", \"failures\": " << mode.failures
               << ", \"elapsed_ns\": " << mode.elapsed_ns
               << ", \"ops_per_second\": " << std::fixed
               << std::setprecision(2) << throughput << std::defaultfloat
               << ", \"latency_ns\": {\"p50\": " << mode.p50_ns
               << ", \"p95\": " << mode.p95_ns << ", \"p99\": "
               << mode.p99_ns << ", \"p999\": " << mode.p999_ns
               << ", \"max\": " << mode.max_ns
               << "}, \"metrics\": {\"local\": "
               << mode.stats.numa_local_allocations << ", \"remote\": "
               << mode.stats.numa_remote_allocations << ", \"fallback\": "
               << mode.stats.numa_fallback_allocations
               << ", \"bind_errors\": " << mode.stats.numa_bind_errors
               << ", \"migrations\": " << mode.stats.numa_migrations
               << "}}";
    }
    if (!modes.empty()) output << '\n';
    output << "  ]\n}\n";
    return output.str();
}

void Emit(const Config& config, const std::string& report) {
    std::cout << report;
    if (!config.json_path.empty()) {
        std::ofstream output(config.json_path, std::ios::trunc);
        if (!output) throw std::runtime_error("cannot open JSON output path");
        output << report;
        if (!output) throw std::runtime_error("cannot write JSON output");
    }
}

}  // namespace
}  // namespace mino::benchmarks

int main(int argc, char** argv) {
    using namespace mino;
    using namespace mino::benchmarks;
    try {
        const Config config = ParseArguments(argc, argv);
        auto discovered = NativeNumaSystem().DiscoverTopology();
        if (!discovered.ok()) {
            NumaTopology unavailable;
            unavailable.fallback_reason = discovered.status().ToString();
            Emit(config, Report(config, unavailable, "SKIPPED",
                                discovered.status().ToString(), {}));
            return 0;
        }
        const NumaTopology topology = *discovered;
        if (!topology.numa_available || topology.allowed_nodes.size() < 2) {
            Emit(config, Report(config, topology, "SKIPPED",
                                topology.fallback_reason, {}));
            return 0;
        }
        const uint32_t cpu_node = topology.allowed_nodes[0];
        const uint32_t remote_node = topology.allowed_nodes[1];
        std::vector<ModeResult> modes;
        modes.push_back(RunMode(config, topology, "local", NumaMemoryPolicy::kNode,
                                cpu_node, cpu_node));
        modes.push_back(RunMode(config, topology, "interleave",
                                NumaMemoryPolicy::kStripe, cpu_node, cpu_node));
        modes.push_back(RunMode(config, topology, "remote", NumaMemoryPolicy::kNode,
                                remote_node, cpu_node));
        const bool failures = std::any_of(
            modes.begin(), modes.end(), [](const ModeResult& mode) {
                return mode.failures != 0 || mode.stats.numa_bind_errors != 0;
            });
        Emit(config, Report(config, topology, failures ? "FAILED" : "PASSED",
                            failures ? "allocation or binding failure" : "", modes));
        return failures ? 1 : 0;
    } catch (const std::exception& error) {
        std::cerr << "numa_allocator_benchmark: " << error.what() << '\n';
        return 2;
    }
}
