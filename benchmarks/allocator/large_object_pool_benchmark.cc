// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sys/mman.h>
#endif

#include "mino/platform/memory_registration.h"
#include "mino/platform/numa.h"
#include "mino/platform/rdma_provider.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/large_object_pool.h"

namespace mino::benchmarks {
namespace {

using Clock = std::chrono::steady_clock;
constexpr uint64_t kPoolBytes = 64u * 1024u * 1024u;
constexpr uint64_t kQuotaProbeBytes = 4u * 1024u * 1024u;
constexpr uint32_t kSegmentBytes = 64u * 1024u;
constexpr uint32_t kMaxObjectBytes = 1u * 1024u * 1024u;
constexpr std::string_view kBenchmarkSchema =
    "mino.allocator.large_object_pool.benchmark.v1";
constexpr std::string_view kQualificationAttestation =
    "physical-hugepage-device";

struct Config {
    uint64_t iterations = 1000;
    std::string json_path;
    std::string plugin_path;
    std::string device;
    std::string hugetlbfs_path;
    int numa_node = -1;
    std::string qualification_attestation;
};

struct AlignedDeleter {
    void operator()(std::byte* pointer) const {
        ::operator delete[](pointer, std::align_val_t(2u * 1024u * 1024u));
    }
};

using AlignedMemory = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedMemory AllocateMemory(uint64_t bytes) {
    return AlignedMemory(new (std::align_val_t(2u * 1024u * 1024u))
                             std::byte[bytes]);
}

class MemoryLock {
public:
    MemoryLock() = default;
    MemoryLock(const MemoryLock&) = delete;
    MemoryLock& operator=(const MemoryLock&) = delete;
    ~MemoryLock() {
#if defined(__linux__)
        if (locked_) (void)::munlock(address_, bytes_);
#endif
    }

    bool Lock(void* address, size_t bytes) noexcept {
        address_ = address;
        bytes_ = bytes;
#if defined(__linux__)
        locked_ = ::mlock(address, bytes) == 0;
#else
        locked_ = false;
#endif
        return locked_;
    }
    bool locked() const noexcept { return locked_; }
    uint64_t bytes() const noexcept {
        return locked_ ? static_cast<uint64_t>(bytes_) : 0;
    }

private:
    void* address_ = nullptr;
    size_t bytes_ = 0;
    bool locked_ = false;
};

class BenchmarkRegistrationProvider final : public MemoryRegistrationProvider {
public:
    MemoryRegistrationProviderClass provider_class() const noexcept override {
        return MemoryRegistrationProviderClass::kMock;
    }
    std::string name() const override { return "benchmark-mock"; }
    bool Supports(MemoryRegistrationKind) const noexcept override { return true; }
    Result<RegisteredMemory> Register(
        const MemoryRegistrationRequest& request) override {
        RegisteredMemory registration{
            .registration_id = next_id_++,
            .bytes = request.bytes,
            .device_key = 1,
            .kind = request.kind,
            .owner = request.owner,
            .physically_contiguous = false,
        };
        registrations_.emplace(registration.registration_id, registration);
        return registration;
    }
    Status Deregister(const RegisteredMemory& registration) override {
        if (registrations_.erase(registration.registration_id) == 0) {
            return Status::Error(StatusCode::kNotFound,
                                 "benchmark registration was not found");
        }
        return Status::Ok();
    }
    Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest& request) override {
        MemoryRegistrationRecoveryResult result;
        for (auto iterator = registrations_.begin();
             iterator != registrations_.end();) {
            const RegisteredMemory& registration = iterator->second;
            if (registration.owner.process_id == request.current_process_id &&
                registration.owner.process_epoch ==
                    request.current_process_epoch) {
                ++iterator;
                continue;
            }
            ++result.registrations_released;
            result.bytes_released += registration.bytes;
            iterator = registrations_.erase(iterator);
        }
        return result;
    }

private:
    uint64_t next_id_ = 1;
    std::map<uint64_t, RegisteredMemory> registrations_;
};

class CountingRegistrationProvider final : public MemoryRegistrationProvider {
public:
    explicit CountingRegistrationProvider(MemoryRegistrationProvider& delegate)
        : delegate_(delegate) {}

    MemoryRegistrationProviderClass provider_class() const noexcept override {
        return delegate_.provider_class();
    }
    std::string name() const override { return delegate_.name(); }
    bool Supports(MemoryRegistrationKind kind) const noexcept override {
        return delegate_.Supports(kind);
    }
    Result<RegisteredMemory> Register(
        const MemoryRegistrationRequest& request) override {
        ++register_calls_;
        auto result = delegate_.Register(request);
        if (!result.ok()) ++register_errors_;
        return result;
    }
    Status Deregister(const RegisteredMemory& registration) override {
        ++deregister_calls_;
        const Status status = delegate_.Deregister(registration);
        if (!status.ok()) ++deregister_errors_;
        return status;
    }
    Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest& request) override {
        return delegate_.RecoverStale(request);
    }

    uint64_t register_calls() const noexcept { return register_calls_; }
    uint64_t register_errors() const noexcept { return register_errors_; }
    uint64_t deregister_calls() const noexcept { return deregister_calls_; }
    uint64_t deregister_errors() const noexcept { return deregister_errors_; }

private:
    MemoryRegistrationProvider& delegate_;
    uint64_t register_calls_ = 0;
    uint64_t register_errors_ = 0;
    uint64_t deregister_calls_ = 0;
    uint64_t deregister_errors_ = 0;
};

struct ResultRow {
    std::string mode;
    std::string lifetime;
    uint32_t bytes = 0;
    uint64_t operations = 0;
    uint64_t failures = 0;
    uint64_t elapsed_ns = 0;
    uint64_t p99_ns = 0;
    double operations_per_second = 0;
    uint64_t internal_fragmentation_bytes = 0;
    uint64_t external_fragmentation_bytes = 0;
    uint64_t hugepage_fallback_allocations = 0;
    uint64_t registration_failures = 0;
    uint64_t deregister_errors = 0;
    uint64_t coalesce_errors = 0;
    uint64_t quota_errors = 0;
    LargeObjectNumaStats numa;
};

uint64_t ParsePositive(std::string_view text, std::string_view name) {
    size_t parsed = 0;
    const uint64_t value = std::stoull(std::string(text), &parsed, 10);
    if (parsed != text.size() || value == 0 || value > 1'000'000) {
        throw std::invalid_argument(std::string(name) +
                                    " must be in [1, 1000000]");
    }
    return value;
}

int ParseNode(std::string_view text) {
    size_t parsed = 0;
    const long value = std::stol(std::string(text), &parsed, 10);
    if (parsed != text.size() || value < 0 || value > 1'000'000) {
        throw std::invalid_argument("--numa-node must be non-negative");
    }
    return static_cast<int>(value);
}

Config ParseArguments(int argc, char** argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument.starts_with("--iterations=")) {
            config.iterations = ParsePositive(argument.substr(13), "iterations");
        } else if (argument.starts_with("--json=")) {
            config.json_path = std::string(argument.substr(7));
        } else if (argument.starts_with("--plugin=")) {
            config.plugin_path = std::string(argument.substr(9));
        } else if (argument.starts_with("--device=")) {
            config.device = std::string(argument.substr(9));
        } else if (argument.starts_with("--hugetlbfs-path=")) {
            config.hugetlbfs_path = std::string(argument.substr(18));
        } else if (argument.starts_with("--numa-node=")) {
            config.numa_node = ParseNode(argument.substr(12));
        } else if (argument.starts_with("--qualification-attestation=")) {
            config.qualification_attestation = std::string(argument.substr(28));
        } else if (argument == "--help") {
            std::cout
                << "Usage: large_object_pool_benchmark [--iterations=N] "
                   "[--json=PATH] [--plugin=/absolute/provider.so] "
                   "[--device=NAME] [--hugetlbfs-path=PATH] "
                   "[--numa-node=N] "
                   "[--qualification-attestation=physical-hugepage-device]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown argument: " +
                                        std::string(argument));
        }
    }
    if (config.plugin_path.empty() != config.device.empty()) {
        throw std::invalid_argument("--plugin and --device must be supplied together");
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

uint64_t Percentile99(std::vector<uint64_t>& values) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(
        0.99 * static_cast<double>(values.size() - 1));
    return values[index];
}

NumaPlacementConfig Placement(const Config& config) {
    if (config.numa_node < 0) return {};
    return {
        .policy = NumaMemoryPolicy::kNode,
        .node = config.numa_node,
        .failure_policy = NumaFailurePolicy::kStrict,
        .system = nullptr,
    };
}

ResultRow RunCase(LargeObjectPool& pool, std::string mode, uint32_t bytes,
                  LargeObjectLifetime lifetime, uint64_t iterations,
                  CountingRegistrationProvider* provider,
                  uint64_t quota_errors) {
    const MemoryRegistrationOwner lease{
        .process_id = 1,
        .process_epoch = 1,
        .lease_id = static_cast<uint64_t>(bytes) +
                    (lifetime == LargeObjectLifetime::kLease ? 1u : 0u),
    };
    const bool registered_purpose =
        pool.purpose() == LargeObjectPoolPurpose::kRdmaRegistered ||
        pool.purpose() == LargeObjectPoolPurpose::kDma;
    const LargeObjectAllocationRequest request{
        .object_size = bytes,
        .type_id = TypeId{0xD608},
        .purpose = pool.purpose(),
        .alignment = 64,
        .contiguity = LargeObjectContiguity::kVirtual,
        .registration =
            pool.purpose() == LargeObjectPoolPurpose::kRdmaRegistered
                ? LargeObjectRegistration::kRdma
                : pool.purpose() == LargeObjectPoolPurpose::kDma
                      ? LargeObjectRegistration::kDma
                      : LargeObjectRegistration::kNone,
        .lifetime = lifetime == LargeObjectLifetime::kLease &&
                            registered_purpose
                        ? LargeObjectLifetime::kLease
                        : LargeObjectLifetime::kAllocation,
        .lease = lifetime == LargeObjectLifetime::kLease && registered_purpose
                     ? lease
                     : MemoryRegistrationOwner{},
    };
    const LargeObjectPoolMetrics before = pool.metrics();
    const LargeObjectNumaStats numa_before = pool.numa_stats();
    const uint64_t deregister_before =
        provider == nullptr ? 0 : provider->deregister_errors();
    uint64_t failures = 0;
    std::vector<uint64_t> latencies;
    latencies.reserve(static_cast<size_t>(iterations));
    const Clock::time_point begin = Clock::now();
    if (lifetime == LargeObjectLifetime::kAllocation) {
        for (uint64_t index = 0; index < iterations; ++index) {
            const Clock::time_point operation_begin = Clock::now();
            auto handle = pool.Allocate(request);
            if (!handle.ok()) {
                ++failures;
            } else {
                const Status retired = pool.Retire(*handle);
                const Status reclaimed = pool.Reclaim(*handle);
                if (!retired.ok() || !reclaimed.ok()) ++failures;
            }
            latencies.push_back(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now() - operation_begin)
                    .count()));
        }
    } else {
        constexpr size_t kBatch = 16;
        uint64_t completed = 0;
        while (completed < iterations) {
            const Clock::time_point operation_begin = Clock::now();
            std::vector<ShmHandle> handles;
            const uint64_t count =
                std::min<uint64_t>(kBatch, iterations - completed);
            handles.reserve(static_cast<size_t>(count));
            for (uint64_t index = 0; index < count; ++index) {
                auto handle = pool.Allocate(request);
                if (!handle.ok()) {
                    ++failures;
                } else {
                    handles.push_back(*handle);
                }
            }
            if (registered_purpose) {
                auto released = pool.ReleaseLease(lease);
                if (!released.ok()) failures += handles.size();
            }
            for (ShmHandle handle : handles) {
                const Status retired = pool.Retire(handle);
                const Status reclaimed = pool.Reclaim(handle);
                if (!retired.ok() || !reclaimed.ok()) ++failures;
            }
            const uint64_t batch_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now() - operation_begin)
                    .count());
            const uint64_t per_operation = count == 0 ? 0 : batch_ns / count;
            latencies.insert(latencies.end(), static_cast<size_t>(count),
                             per_operation);
            completed += count;
        }
    }
    const uint64_t elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin)
            .count());
    const LargeObjectPoolMetrics after = pool.metrics();
    const LargeObjectNumaStats numa_after = pool.numa_stats();
    const bool coalesced = after.free_bytes == after.capacity_bytes &&
                           after.largest_free_extent_bytes ==
                               after.capacity_bytes &&
                           after.external_fragmentation_bytes == 0;
    return {
        .mode = std::move(mode),
        .lifetime = lifetime == LargeObjectLifetime::kLease ? "batch"
                                                             : "short",
        .bytes = bytes,
        .operations = iterations,
        .failures = failures,
        .elapsed_ns = elapsed,
        .p99_ns = Percentile99(latencies),
        .operations_per_second =
            elapsed == 0
                ? 0.0
                : static_cast<double>(iterations) * 1'000'000'000.0 /
                      static_cast<double>(elapsed),
        .internal_fragmentation_bytes = after.internal_fragmentation_bytes,
        .external_fragmentation_bytes = after.external_fragmentation_bytes,
        .hugepage_fallback_allocations =
            after.huge_page_fallback_allocations -
            before.huge_page_fallback_allocations,
        .registration_failures =
            after.registration_failures - before.registration_failures,
        .deregister_errors = provider == nullptr
                                 ? 0
                                 : provider->deregister_errors() -
                                       deregister_before,
        .coalesce_errors = coalesced ? 0u : 1u,
        .quota_errors = quota_errors,
        .numa = {
            .local_allocations = numa_after.local_allocations -
                                 numa_before.local_allocations,
            .remote_allocations = numa_after.remote_allocations -
                                  numa_before.remote_allocations,
            .fallback_allocations = numa_after.fallback_allocations -
                                    numa_before.fallback_allocations,
            .bind_errors = numa_after.bind_errors - numa_before.bind_errors,
        },
    };
}

void RunSizes(std::vector<ResultRow>& rows, LargeObjectPool& pool,
              std::string_view mode, uint64_t iterations,
              CountingRegistrationProvider* provider,
              uint64_t quota_errors) {
    for (uint32_t bytes : {64u * 1024u, 256u * 1024u, 1024u * 1024u}) {
        rows.push_back(RunCase(pool, std::string(mode), bytes,
                               LargeObjectLifetime::kAllocation, iterations,
                               provider, quota_errors));
        rows.push_back(RunCase(pool, std::string(mode), bytes,
                               LargeObjectLifetime::kLease, iterations, provider,
                               quota_errors));
    }
}

uint64_t ExerciseQuotaContract(const Config& config,
                               CountingRegistrationProvider& provider) {
    const uint64_t register_calls_before = provider.register_calls();
    const uint64_t deregister_calls_before = provider.deregister_calls();
    AlignedMemory memory = AllocateMemory(kQuotaProbeBytes);
    const LargeObjectPoolOptions options{
        .purpose = LargeObjectPoolPurpose::kRdmaRegistered,
        .huge_pages = {},
        .numa = Placement(config),
        .registration_provider = &provider,
        .registration_scope_id = 0xD6080002,
        .registration_owner = {.process_id = 1,
                               .process_epoch = 1,
                               .lease_id = 2},
        .registration_quota_bytes = kSegmentBytes,
        .minimum_registered_object_bytes = kSegmentBytes,
        .recover_stale_registrations = true,
    };
    auto pool = LargeObjectPool::Create(memory.get(), kQuotaProbeBytes,
                                        kMaxObjectBytes, kSegmentBytes, options);
    if (!pool.ok()) return 1;
    const LargeObjectAllocationRequest request{
        .object_size = 256u * 1024u,
        .type_id = TypeId{0xD608},
        .purpose = LargeObjectPoolPurpose::kRdmaRegistered,
        .alignment = 64,
        .contiguity = LargeObjectContiguity::kVirtual,
        .registration = LargeObjectRegistration::kRdma,
        .lifetime = LargeObjectLifetime::kAllocation,
        .lease = {},
    };
    auto handle = pool->Allocate(request);
    uint64_t errors = 0;
    if (handle.ok() || handle.status().code() != StatusCode::kResourceExhausted) {
        ++errors;
        if (handle.ok()) {
            if (!pool->Retire(*handle).ok() || !pool->Reclaim(*handle).ok()) {
                ++errors;
            }
        }
    }
    const LargeObjectPoolMetrics metrics = pool->metrics();
    if (metrics.registration_bytes != 0 ||
        metrics.free_bytes != metrics.capacity_bytes ||
        provider.register_calls() != register_calls_before ||
        provider.deregister_calls() != deregister_calls_before) {
        ++errors;
    }
    return errors;
}

std::string ProviderClassName(MemoryRegistrationProviderClass provider_class) {
    switch (provider_class) {
        case MemoryRegistrationProviderClass::kDevice: return "device";
        case MemoryRegistrationProviderClass::kMock: return "mock";
        case MemoryRegistrationProviderClass::kUnavailable: return "unavailable";
    }
    return "unavailable";
}

bool RowsPassed(const std::vector<ResultRow>& rows) {
    return std::all_of(rows.begin(), rows.end(), [](const ResultRow& row) {
        return row.operations > 0 && row.p99_ns > 0 &&
               row.operations_per_second > 0 && row.failures == 0 &&
               row.internal_fragmentation_bytes == 0 &&
               row.external_fragmentation_bytes == 0 &&
               row.hugepage_fallback_allocations == 0 &&
               row.registration_failures == 0 && row.deregister_errors == 0 &&
               row.coalesce_errors == 0 && row.quota_errors == 0 &&
               row.numa.fallback_allocations == 0 && row.numa.bind_errors == 0;
    });
}

std::string Report(const Config& config, const NumaTopology& topology,
                   const std::vector<ResultRow>& rows,
                   const CountingRegistrationProvider& provider,
                   std::string_view provider_provenance, bool huge_requested,
                   bool huge_actual, uint64_t actual_page_size,
                   HugePageFallbackReason fallback_reason, int fallback_errno,
                   bool memory_locked, uint64_t locked_bytes,
                   uint64_t quota_errors) {
    const bool device_provider = provider.provider_class() ==
                                 MemoryRegistrationProviderClass::kDevice;
    const bool attested = config.qualification_attestation ==
                          kQualificationAttestation;
    const bool matrix_complete = rows.size() == 18;
    const bool passed = matrix_complete && RowsPassed(rows) &&
                        provider.deregister_errors() == 0 && quota_errors == 0;
    const bool eligible = passed && attested && device_provider && huge_actual &&
                          memory_locked && config.numa_node >= 0 &&
                          topology.linux_native &&
                          topology.IsNodeAllowed(
                              static_cast<uint32_t>(config.numa_node));
    std::ostringstream output;
    output << "{\n"
           << "  \"schema\": \"" << kBenchmarkSchema << "\",\n"
           << "  \"status\": \""
           << (!device_provider ? "SKIPPED" : passed ? "PASSED" : "FAILED")
           << "\",\n"
           << "  \"qualification_eligible\": "
           << (eligible ? "true" : "false") << ",\n"
           << "  \"attestation\": \""
           << JsonEscape(config.qualification_attestation) << "\",\n"
           << "  \"config\": {\"iterations\": " << config.iterations
           << ", \"pool_bytes\": " << kPoolBytes
           << ", \"segment_bytes\": " << kSegmentBytes << "},\n"
           << "  \"provider\": {\"class\": \""
           << ProviderClassName(provider.provider_class())
           << "\", \"name\": \"" << JsonEscape(provider.name())
           << "\", \"provenance\": \""
           << JsonEscape(provider_provenance) << "\", \"device\": \""
           << JsonEscape(config.device) << "\", \"register_calls\": "
           << provider.register_calls() << ", \"register_errors\": "
           << provider.register_errors() << ", \"deregister_calls\": "
           << provider.deregister_calls() << ", \"deregister_errors\": "
           << provider.deregister_errors() << "},\n"
           << "  \"hugepages\": {\"requested\": "
           << (huge_requested ? "true" : "false")
           << ", \"actual\": " << (huge_actual ? "true" : "false")
           << ", \"actual_page_size\": " << actual_page_size
           << ", \"fallback_reason\": \""
           << HugePageFallbackReasonName(fallback_reason)
           << "\", \"fallback_errno\": " << fallback_errno << "},\n"
           << "  \"locked_memory\": {\"succeeded\": "
           << (memory_locked ? "true" : "false") << ", \"bytes\": "
           << locked_bytes << "},\n"
           << "  \"numa\": {\"linux_native\": "
           << (topology.linux_native ? "true" : "false")
           << ", \"numa_available\": "
           << (topology.numa_available ? "true" : "false")
           << ", \"configured_node\": " << config.numa_node
           << ", \"current_cpu\": " << topology.current_cpu
           << ", \"current_node\": " << topology.current_node
           << ", \"allowed_nodes\": " << JsonArray(topology.allowed_nodes)
           << ", \"allowed_cpus\": " << JsonArray(topology.allowed_cpus)
           << "},\n"
           << "  \"contract\": {\"deregister_errors\": "
           << provider.deregister_errors()
           << ", \"coalesce_errors\": "
           << std::count_if(rows.begin(), rows.end(), [](const ResultRow& row) {
                  return row.coalesce_errors != 0;
              })
           << ", \"quota_errors\": " << quota_errors << "},\n"
           << "  \"rows\": [";
    for (size_t index = 0; index < rows.size(); ++index) {
        const ResultRow& row = rows[index];
        if (index != 0) output << ',';
        output << "\n    {\"mode\": \"" << row.mode
               << "\", \"bytes\": " << row.bytes
               << ", \"lifetime\": \"" << row.lifetime
               << "\", \"registration_lifetime\": \""
               << (row.mode == "device-registration" &&
                           row.lifetime == "batch"
                       ? "lease"
                       : "allocation")
               << "\", \"operations\": " << row.operations
               << ", \"elapsed_ns\": " << row.elapsed_ns
               << ", \"operations_per_second\": " << std::fixed
               << std::setprecision(3) << row.operations_per_second
               << std::defaultfloat << ", \"p99_ns\": " << row.p99_ns
               << ", \"failures\": " << row.failures
               << ", \"internal_fragmentation_bytes\": "
               << row.internal_fragmentation_bytes
               << ", \"external_fragmentation_bytes\": "
               << row.external_fragmentation_bytes
               << ", \"hugepage_fallback_allocations\": "
               << row.hugepage_fallback_allocations
               << ", \"registration_failures\": "
               << row.registration_failures
               << ", \"deregister_errors\": " << row.deregister_errors
               << ", \"coalesce_errors\": " << row.coalesce_errors
               << ", \"quota_errors\": " << row.quota_errors
               << ", \"numa\": {\"local_allocations\": "
               << row.numa.local_allocations
               << ", \"remote_allocations\": "
               << row.numa.remote_allocations
               << ", \"fallback_allocations\": "
               << row.numa.fallback_allocations << ", \"bind_errors\": "
               << row.numa.bind_errors << "}}";
    }
    if (!rows.empty()) output << '\n';
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
        NumaTopology topology;
        if (discovered.ok()) topology = *discovered;

        std::vector<ResultRow> rows;
        AlignedMemory normal_memory = AllocateMemory(kPoolBytes);
        const LargeObjectPoolOptions normal_options{
            .purpose = LargeObjectPoolPurpose::kNormal,
            .huge_pages = {},
            .numa = Placement(config),
            .registration_provider = nullptr,
            .registration_scope_id = 0,
            .registration_owner = {},
            .registration_quota_bytes = 0,
            .minimum_registered_object_bytes = kSegmentBytes,
            .recover_stale_registrations = true,
        };
        auto normal = LargeObjectPool::Create(normal_memory.get(), kPoolBytes,
                                              kMaxObjectBytes, kSegmentBytes,
                                              normal_options);
        if (!normal.ok()) throw std::runtime_error(normal.status().ToString());
        RunSizes(rows, *normal, "ordinary", config.iterations, nullptr, 0);

        const std::string shm_name =
            "/mino_d608_benchmark_" +
            std::to_string(Clock::now().time_since_epoch().count());
        const SharedMemoryCreateOptions shm_options{
            .name = shm_name,
            .size = kPoolBytes,
            .use_huge_pages = true,
            .hugetlbfs_path = config.hugetlbfs_path,
        };
        auto segment = SharedMemorySegment::Create(shm_options);
        if (!segment.ok()) throw std::runtime_error(segment.status().ToString());
        const bool huge_requested = segment->huge_pages_requested();
        const bool huge_actual = segment->huge_pages_actual();
        const uint64_t actual_page_size = segment->actual_page_size();
        const HugePageFallbackReason fallback_reason =
            segment->huge_page_fallback_reason();
        const int fallback_errno = segment->huge_page_fallback_errno();
        {
            const LargeObjectPoolOptions huge_options{
                .purpose = LargeObjectPoolPurpose::kHugePage,
                .huge_pages = {
                    .requested = huge_requested,
                    .actual = huge_actual,
                    .strict = false,
                    .actual_page_size = actual_page_size,
                    .fallback_reason = fallback_reason,
                    .fallback_errno = fallback_errno,
                },
                .numa = Placement(config),
                .registration_provider = nullptr,
                .registration_scope_id = 0,
                .registration_owner = {},
                .registration_quota_bytes = 0,
                .minimum_registered_object_bytes = kSegmentBytes,
                .recover_stale_registrations = true,
            };
            auto huge = LargeObjectPool::Create(
                segment->base(), segment->size(), kMaxObjectBytes,
                kSegmentBytes, huge_options);
            if (!huge.ok()) throw std::runtime_error(huge.status().ToString());
            RunSizes(rows, *huge, "hugepage", config.iterations, nullptr, 0);
        }

        BenchmarkRegistrationProvider mock;
        std::shared_ptr<platform::RdmaDeviceProvider> dynamic_provider;
        MemoryRegistrationProvider* implementation = &mock;
        std::string provider_provenance = "test-only-mock";
        bool provider_started = false;
        if (!config.plugin_path.empty()) {
            auto loaded = platform::CreateDynamicRdmaDeviceProvider(
                {.plugin_path = config.plugin_path,
                 .device_name = config.device});
            if (!loaded.ok()) {
                throw std::runtime_error(loaded.status().ToString());
            }
            dynamic_provider = *loaded;
            const platform::RdmaProviderLimits limits{
                .max_connections = 1,
                .max_listeners = 1,
                .send_queue_depth = 64,
                .receive_queue_depth = 64,
                .completion_queue_depth = 128,
                .max_message_bytes = kMaxObjectBytes,
            };
            const Status started = dynamic_provider->Start(limits);
            if (!started.ok()) throw std::runtime_error(started.ToString());
            provider_started = true;
            implementation = dynamic_provider.get();
            provider_provenance = dynamic_provider->provenance();
        }
        CountingRegistrationProvider provider(*implementation);
        const uint64_t quota_errors = ExerciseQuotaContract(config, provider);

        AlignedMemory registered_memory = AllocateMemory(kPoolBytes);
        const LargeObjectPoolOptions registered_options{
            .purpose = LargeObjectPoolPurpose::kRdmaRegistered,
            .huge_pages = {},
            .numa = Placement(config),
            .registration_provider = &provider,
            .registration_scope_id = 0xD6080001,
            .registration_owner = {.process_id = 1,
                                   .process_epoch = 1,
                                   .lease_id = 1},
            .registration_quota_bytes = kPoolBytes,
            .minimum_registered_object_bytes = kSegmentBytes,
            .recover_stale_registrations = true,
        };
        auto registered = LargeObjectPool::Create(
            registered_memory.get(), kPoolBytes, kMaxObjectBytes, kSegmentBytes,
            registered_options);
        if (!registered.ok()) {
            throw std::runtime_error(registered.status().ToString());
        }
        MemoryLock memory_lock;
        const bool memory_locked =
            memory_lock.Lock(registered_memory.get(), kPoolBytes);
        RunSizes(rows, *registered,
                 provider.provider_class() ==
                         MemoryRegistrationProviderClass::kDevice
                     ? "device-registration"
                     : "mock-registration",
                 config.iterations, &provider, quota_errors);

        if (provider_started) {
            const Status shutdown = dynamic_provider->Shutdown();
            if (!shutdown.ok()) throw std::runtime_error(shutdown.ToString());
        }
        const std::string report = Report(
            config, topology, rows, provider, provider_provenance,
            huge_requested, huge_actual, actual_page_size, fallback_reason,
            fallback_errno, memory_locked, memory_lock.bytes(), quota_errors);
        Emit(config, report);

        const Status close = segment->Close();
        const Status unlink = SharedMemorySegment::Unlink(shm_name);
        if (!close.ok() || !unlink.ok()) {
            throw std::runtime_error("failed to clean benchmark shared memory");
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "large_object_pool_benchmark: " << error.what() << '\n';
        return 2;
    }
}
