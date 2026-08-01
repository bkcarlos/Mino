// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include <gtest/gtest.h>

#if defined(__unix__) || defined(__APPLE__)

#include <signal.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/recovery/scanner.h"
#include "mino/shm/region/recovery.h"
#include "mino/shm/region/region.h"
#include "mino/shm/region/superblock.h"

namespace mino {
namespace {

using StressClock = std::chrono::steady_clock;
using StressDeadline = StressClock::time_point;

constexpr uint64_t kDefaultStressSeconds = 1;
constexpr auto kDeterministicWatchdog = std::chrono::seconds(30);
constexpr auto kMinimumCleanupReserve = std::chrono::milliseconds(250);
constexpr auto kMaximumCleanupReserve = std::chrono::milliseconds(2000);
constexpr auto kEstimatedMinimumRoundBudget = std::chrono::milliseconds(20);
constexpr auto kElapsedUpperSlack = std::chrono::seconds(1);
constexpr uint64_t kRegionBytes = 1u << 20;
constexpr uint64_t kRecoveryLeaseMs = 10;
constexpr uint64_t kRecoveryWaitMs = 1000;
constexpr uint64_t kSurvivorValue = 0xD1005A5AA55A11EEULL;
constexpr uint64_t kTargetValue = 0xD100C0DEC0FFEE11ULL;

constexpr std::chrono::milliseconds CleanupReserveFor(
    std::chrono::milliseconds configured_duration) {
    if (configured_duration <= std::chrono::milliseconds::zero()) {
        return std::chrono::milliseconds::zero();
    }
    auto reserve = configured_duration / 2;
    reserve = std::max(reserve, kMinimumCleanupReserve);
    reserve = std::min(reserve, kMaximumCleanupReserve);
    return std::min(reserve, configured_duration);
}

enum class CrashScenario : uint32_t {
    kAllocate = 1,
    kBuild = 2,
    kPublish = 3,
    kRetire = 4,
    kReclaim = 5,
    kRecovery = 6,
    kCorruptRecovery = 7,
};

constexpr std::array<CrashScenario, 7> kScenarios = {
    CrashScenario::kAllocate,
    CrashScenario::kBuild,
    CrashScenario::kPublish,
    CrashScenario::kRetire,
    CrashScenario::kReclaim,
    CrashScenario::kRecovery,
    CrashScenario::kCorruptRecovery,
};

enum class Interruption : uint8_t {
    kSigkill,
    kSigstopThenKill,
};

const char* ScenarioName(CrashScenario scenario) {
    switch (scenario) {
        case CrashScenario::kAllocate:
            return "allocate-ALLOCATING";
        case CrashScenario::kBuild:
            return "build-BUILDING";
        case CrashScenario::kPublish:
            return "publish-PUBLISHED";
        case CrashScenario::kRetire:
            return "retire-RETIRED";
        case CrashScenario::kReclaim:
            return "reclaim-RECLAIMING";
        case CrashScenario::kRecovery:
            return "recovery-RECOVERING";
        case CrashScenario::kCorruptRecovery:
            return "recovery-corruption-QUARANTINED";
    }
    return "unknown";
}

const char* InterruptionName(Interruption interruption) {
    switch (interruption) {
        case Interruption::kSigkill:
            return "SIGKILL";
        case Interruption::kSigstopThenKill:
            return "SIGSTOP->SIGKILL";
    }
    return "unknown";
}

struct SharedControl {
    std::atomic<uint32_t> reached{0};
    uint32_t region_id = 0;
    uint32_t baseline_available = 0;
    ShmHandle survivor;
    ShmHandle target;
};

ClassTableConfig AllocatorConfig() {
    ClassTableConfig config;
    config.classes = {{.slot_size = 64, .slot_count = 128}};
    return config;
}

AllocationRequest Request() {
    return AllocationRequest{
        .object_size = sizeof(uint64_t),
        .type_id = TypeId{0xD101},
        .schema = SchemaIdentity{.short_id = 0xD100D100D100D100ULL,
                                 .layout_version = 1},
        .alignment = alignof(uint64_t),
    };
}

RegionAllocatorStorage AllocatorStorage(void* base, uint64_t size,
                                        const SuperBlock& sb) {
    return RegionAllocatorStorage{
        .region_base = base,
        .region_size = size,
        .allocator_offset = sb.allocator_offset,
        .allocator_size = sb.data_offset - sb.allocator_offset,
        .data_offset = sb.data_offset,
        .data_size = sb.data_size,
        .region_id = sb.region_id,
    };
}

RegionAllocatorStorage AllocatorStorage(SharedMemoryRegion& region) {
    return AllocatorStorage(region.base(), region.size(), *region.superblock());
}

RegionAllocatorStorage AllocatorStorage(SharedMemorySegment& segment) {
    const auto* sb = static_cast<const SuperBlock*>(segment.base());
    return AllocatorStorage(segment.base(), segment.size(), *sb);
}

uint64_t NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            StressClock::now().time_since_epoch())
            .count());
}

uint64_t DefaultStressSeed() {
    std::random_device random_device;
    uint64_t seed = static_cast<uint64_t>(random_device()) << 32;
    seed ^= static_cast<uint64_t>(random_device());
    seed ^= NowNs();
    seed ^= static_cast<uint64_t>(::getpid()) * 0x9E3779B97F4A7C15ULL;
    return seed;
}

class ScopedSharedMemoryUnlink {
public:
    explicit ScopedSharedMemoryUnlink(std::string name)
        : name_(std::move(name)) {}

    ~ScopedSharedMemoryUnlink() {
        (void)SharedMemorySegment::Unlink(name_);
    }

    ScopedSharedMemoryUnlink(const ScopedSharedMemoryUnlink&) = delete;
    ScopedSharedMemoryUnlink& operator=(const ScopedSharedMemoryUnlink&) = delete;

private:
    std::string name_;
};

bool ReadUnsignedEnvironment(const char* name, uint64_t fallback,
                             uint64_t* value, std::string* error) {
    const char* text = std::getenv(name);
    if (text == nullptr) {
        *value = fallback;
        return true;
    }
    if (*text == '\0' || *text == '-') {
        *error = std::string(name) + " must be an unsigned integer";
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text, &end, 0);
    if (errno == ERANGE || end == text || *end != '\0') {
        *error = std::string(name) + " has invalid value '" + text + "'";
        return false;
    }
    *value = static_cast<uint64_t>(parsed);
    return true;
}

void ArmChildWatchdog(StressDeadline deadline) {
    const auto remaining = deadline - StressClock::now();
    if (remaining <= StressClock::duration::zero()) {
        _exit(124);
    }
    const auto rounded =
        std::chrono::ceil<std::chrono::seconds>(remaining).count();
    const auto maximum = std::numeric_limits<unsigned int>::max();
    const unsigned int seconds = static_cast<unsigned int>(
        std::min<uint64_t>(static_cast<uint64_t>(rounded), maximum));
    sigset_t alarm_signal;
    ::sigemptyset(&alarm_signal);
    ::sigaddset(&alarm_signal, SIGALRM);
    (void)::sigprocmask(SIG_UNBLOCK, &alarm_signal, nullptr);
    ::signal(SIGALRM, SIG_DFL);
    ::alarm(std::max(1u, seconds));
}

[[noreturn]] void ReachCrashPoint(SharedControl* shared,
                                  CrashScenario scenario,
                                  Interruption interruption) noexcept {
    shared->reached.store(static_cast<uint32_t>(scenario),
                          std::memory_order_release);
    if (interruption == Interruption::kSigstopThenKill) {
        (void)::raise(SIGSTOP);
    }
    for (;;) {
        (void)::pause();
    }
}

struct ReclaimHookContext {
    SharedControl* shared = nullptr;
    Interruption interruption = Interruption::kSigkill;
    uint32_t calls = 0;
};

bool StopInReclaiming(ShmHandle, void* opaque) noexcept {
    auto* context = static_cast<ReclaimHookContext*>(opaque);
    ++context->calls;
    // ReclaimSlotExact invokes the guard once before claiming kReclaiming and
    // once after the claim while the bitmap still excludes allocation.
    if (context->calls == 2) {
        ReachCrashPoint(context->shared, CrashScenario::kReclaim,
                        context->interruption);
    }
    return true;
}

bool PrepareChildBaseline(CentralSlabAllocator* allocator,
                          SharedControl* shared) {
    auto survivor = allocator->Allocate(Request());
    if (!survivor.ok()) {
        return false;
    }
    auto build = allocator->BeginBuild(*survivor);
    if (!build.ok()) {
        return false;
    }
    *static_cast<uint64_t*>(build->data) = kSurvivorValue;
    if (!allocator->Publish(*survivor).ok()) {
        return false;
    }
    shared->survivor = *survivor;

    std::vector<ShmHandle> handles;
    handles.reserve(allocator->total_slot_count());
    for (uint32_t i = 0; i <= allocator->total_slot_count(); ++i) {
        auto allocation = allocator->Allocate(Request());
        if (!allocation.ok()) {
            if (allocation.status().code() != StatusCode::kResourceExhausted) {
                return false;
            }
            break;
        }
        handles.push_back(*allocation);
    }
    shared->baseline_available = static_cast<uint32_t>(handles.size());
    for (ShmHandle handle : handles) {
        if (!allocator->Abort(handle).ok()) {
            return false;
        }
    }
    return shared->baseline_available + 1 == allocator->total_slot_count();
}

[[noreturn]] void ChildAtCrashPoint(const std::string& name,
                                    SharedControl* shared,
                                    CrashScenario scenario,
                                    Interruption interruption,
                                    StressDeadline deadline) {
    ArmChildWatchdog(deadline);
    RegionCreateOptions create_options;
    create_options.name = name;
    create_options.size_bytes = kRegionBytes;
    auto created = SharedMemoryRegion::Create(create_options);
    if (!created.ok()) {
        _exit(10);
    }
    shared->region_id = created->region_id();
    auto created_allocator = CentralSlabAllocator::CreateInRegion(
        AllocatorStorage(*created), AllocatorConfig());
    if (!created_allocator.ok()) {
        _exit(11);
    }
    CentralSlabAllocator allocator = *created_allocator;
    if (!PrepareChildBaseline(&allocator, shared)) {
        _exit(12);
    }

    auto target = allocator.Allocate(Request());
    if (!target.ok()) {
        _exit(13);
    }
    shared->target = *target;

    if (scenario == CrashScenario::kAllocate ||
        scenario == CrashScenario::kRecovery) {
        // kAllocating is intentionally private and Allocate() returns only after
        // kAllocated publication. Fault injection rewinds only the lifecycle
        // word, matching scanner's documented crash convention without exposing
        // allocator metadata or adding a production hook.
        auto* header = reinterpret_cast<SlabHeader*>(
            created->base() + target->offset);
        header->object_state.store(static_cast<uint32_t>(ObjectState::kAllocating),
                                   std::memory_order_release);
        if (scenario == CrashScenario::kAllocate) {
            ReachCrashPoint(shared, scenario, interruption);
        }
    } else {
        auto build = allocator.BeginBuild(*target);
        if (!build.ok()) {
            _exit(12);
        }
        *static_cast<uint64_t*>(build->data) = kTargetValue;
        if (scenario == CrashScenario::kBuild) {
            ReachCrashPoint(shared, scenario, interruption);
        }
        if (!allocator.Publish(*target).ok()) {
            _exit(13);
        }
        if (scenario == CrashScenario::kPublish) {
            ReachCrashPoint(shared, scenario, interruption);
        }
        if (scenario == CrashScenario::kCorruptRecovery) {
            auto* header = reinterpret_cast<SlabHeader*>(
                created->base() + target->offset);
            header->object_size ^= 1u;
            ReachCrashPoint(shared, scenario, interruption);
        }
        if (!allocator.Retire(*target).ok()) {
            _exit(14);
        }
        if (scenario == CrashScenario::kRetire) {
            ReachCrashPoint(shared, scenario, interruption);
        }
        if (scenario == CrashScenario::kReclaim) {
            ReclaimHookContext hook{.shared = shared,
                                    .interruption = interruption};
            allocator.SetReclaimGuard(&StopInReclaiming, &hook);
            if (!allocator.Reclaim(*target).ok()) {
                _exit(15);
            }
            _exit(16);  // The second guard call must have stopped this process.
        }
    }

    // kRecovery is the one deliberate construction exception: to crash at the
    // RECOVERING state itself, the child must first claim the public recovery
    // lease/fence and publish DIRTY -> RECOVERING. All other scenarios leave the
    // Region ACTIVE + !clean for the parent's v3 service takeover path.
    StoreState(*created->superblock(), RegionState::kDirty);
    auto acquired = RecoveryOwner::TryAcquire(
        *created, ProcessIdentity::Current(), kRecoveryLeaseMs);
    if (!acquired.ok()) {
        _exit(17);
    }
    RecoveryOwner owner = std::move(*acquired);
    if (!owner
             .ClaimRecoveryFence(LoadRecoveryFence(*created->superblock()))
             .ok()) {
        _exit(18);
    }
    StoreState(*created->superblock(), RegionState::kRecovering);
    ReachCrashPoint(shared, CrashScenario::kRecovery, interruption);
}

class D1RegionRecoveryKillStressTest : public ::testing::Test {
protected:
    void SetUp() override {
        void* mapped = ::mmap(nullptr, sizeof(SharedControl),
                              PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapped, MAP_FAILED) << std::strerror(errno);
        shared_ = new (mapped) SharedControl{};
        watchdog_deadline_ = StressClock::now() + kDeterministicWatchdog;
    }

    void TearDown() override {
        if (child_pid_ > 0) {
            (void)::kill(child_pid_, SIGKILL);
            int status = 0;
            (void)WaitForExit(watchdog_deadline_, &status);
        }
        if (shared_ != nullptr) {
            shared_->~SharedControl();
            (void)::munmap(shared_, sizeof(SharedControl));
        }
    }

    std::string NextRegionName() {
        static std::atomic<uint64_t> sequence{0};
        const uint64_t value = sequence.fetch_add(1, std::memory_order_relaxed);
        std::string name = "/d1k_" + std::to_string(::getpid()) + "_" +
                           std::to_string(value);
        EXPECT_LE(name.size(), 31u);
        return name;
    }

    bool WaitForExit(StressDeadline deadline, int* status) {
        for (;;) {
            const pid_t result = ::waitpid(child_pid_, status, WNOHANG);
            if (result == child_pid_) {
                child_pid_ = -1;
                return true;
            }
            if (result == -1 && errno != EINTR) {
                if (errno == ECHILD) {
                    child_pid_ = -1;
                }
                return false;
            }
            if (StressClock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }

    bool WaitReached(CrashScenario scenario, StressDeadline deadline) {
        wait_reached_failure_.clear();
        const uint32_t expected = static_cast<uint32_t>(scenario);
        for (;;) {
            if (shared_->reached.load(std::memory_order_acquire) == expected) {
                return true;
            }
            int status = 0;
            const pid_t result = ::waitpid(child_pid_, &status, WNOHANG);
            if (result == child_pid_) {
                child_pid_ = -1;
                std::ostringstream message;
                if (WIFEXITED(status)) {
                    message << "child exited early with code "
                            << WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    message << "child terminated early by signal "
                            << WTERMSIG(status);
                } else {
                    message << "child changed state before reaching crash point"
                            << " (wait status=" << status << ")";
                }
                wait_reached_failure_ = message.str();
                return false;
            }
            if (result == -1 && errno != EINTR) {
                const int wait_errno = errno;
                if (wait_errno == ECHILD) {
                    child_pid_ = -1;
                }
                wait_reached_failure_ =
                    std::string("waitpid failed: ") + std::strerror(wait_errno);
                return false;
            }
            if (StressClock::now() >= deadline) {
                wait_reached_failure_ = "global watchdog deadline expired";
                return false;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }

    bool WaitStopped(StressDeadline deadline) {
        for (;;) {
            int status = 0;
            const pid_t result =
                ::waitpid(child_pid_, &status, WNOHANG | WUNTRACED);
            if (result == child_pid_) {
                if (WIFEXITED(status) || WIFSIGNALED(status)) {
                    child_pid_ = -1;
                    return false;
                }
                return WIFSTOPPED(status) && WSTOPSIG(status) == SIGSTOP;
            }
            if (result == -1 && errno != EINTR) {
                if (errno == ECHILD) {
                    child_pid_ = -1;
                }
                return false;
            }
            if (StressClock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }

    void KillAndReap(StressDeadline deadline) {
        ASSERT_GT(child_pid_, 0);
        ASSERT_EQ(::kill(child_pid_, SIGKILL), 0) << std::strerror(errno);
        int status = 0;
        ASSERT_TRUE(WaitForExit(deadline, &status))
            << "child did not exit before the global watchdog deadline";
        ASSERT_TRUE(WIFSIGNALED(status));
        EXPECT_EQ(WTERMSIG(status), SIGKILL);
    }

    void ProbeAvailableSlots(CentralSlabAllocator* allocator,
                             uint32_t* available) {
        std::vector<ShmHandle> handles;
        handles.reserve(allocator->total_slot_count());
        for (uint32_t i = 0; i <= allocator->total_slot_count(); ++i) {
            auto allocation = allocator->Allocate(Request());
            if (!allocation.ok()) {
                EXPECT_EQ(allocation.status().code(),
                          StatusCode::kResourceExhausted)
                    << allocation.status().ToString();
                break;
            }
            handles.push_back(*allocation);
        }
        *available = static_cast<uint32_t>(handles.size());
        for (ShmHandle handle : handles) {
            ASSERT_TRUE(allocator->Abort(handle).ok());
        }
    }



    void VerifySurvivor(CentralSlabAllocator* allocator, ShmHandle survivor) {
        auto view = allocator->Inspect(survivor);
        ASSERT_TRUE(view.ok()) << view.status().ToString();
        EXPECT_EQ(view->state, ObjectState::kPublished);
        ASSERT_NE(view->data, nullptr);
        EXPECT_EQ(*static_cast<const uint64_t*>(view->data), kSurvivorValue);
    }

    void VerifyCleanScanner(CentralSlabAllocator allocator) {
        shm::recovery::RecoveryScannerOptions options;
        options.repair = false;
        auto scanner = shm::recovery::RecoveryScanner::Create(
            allocator, shm::recovery::RecoveryOwnership{}, options);
        ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
        auto report = scanner->Scan();
        ASSERT_TRUE(report.ok()) << report.status().ToString();
        EXPECT_EQ(report->orphan_slab_count, 0u) << report->details;
        EXPECT_EQ(report->bitmap_inconsistency_count, 0u) << report->details;
        EXPECT_EQ(report->corrupted_slab_count, 0u) << report->details;
        EXPECT_TRUE(scanner->VerifyBitmapConsistency().ok());
    }

    void CleanupTarget(CentralSlabAllocator* allocator,
                       CrashScenario scenario, ShmHandle target) {
        auto view = allocator->Inspect(target);
        switch (scenario) {
            case CrashScenario::kAllocate:
            case CrashScenario::kRetire:
            case CrashScenario::kReclaim:
            case CrashScenario::kRecovery:
                EXPECT_FALSE(view.ok());
                EXPECT_EQ(view.status().code(), StatusCode::kNotFound);
                return;
            case CrashScenario::kBuild:
                ASSERT_TRUE(view.ok()) << view.status().ToString();
                EXPECT_EQ(view->state, ObjectState::kBuilding);
                EXPECT_TRUE(allocator->Abort(target).ok());
                return;
            case CrashScenario::kPublish:
                ASSERT_TRUE(view.ok()) << view.status().ToString();
                EXPECT_EQ(view->state, ObjectState::kPublished);
                EXPECT_EQ(*static_cast<const uint64_t*>(view->data),
                          kTargetValue);
                EXPECT_TRUE(allocator->Retire(target).ok());
                EXPECT_TRUE(allocator->Reclaim(target).ok());
                return;
            case CrashScenario::kCorruptRecovery:
                return;
        }
    }

    void RunScenario(CrashScenario scenario, Interruption interruption,
                     StressDeadline deadline) {
        ASSERT_LT(StressClock::now(), deadline);
        const std::string name = NextRegionName();
        ScopedSharedMemoryUnlink unlink_region(name);
        shared_->region_id = 0;
        shared_->baseline_available = 0;
        shared_->survivor = {};
        shared_->target = {};
        shared_->reached.store(0, std::memory_order_relaxed);

        // The child is the sole writable supervisor. Killing it releases the
        // kernel lock, allowing the parent to prove service death and fence the
        // next supervisor generation through SharedMemoryRegion::Attach().
        child_pid_ = ::fork();
        ASSERT_NE(child_pid_, -1) << std::strerror(errno);
        if (child_pid_ == 0) {
            ChildAtCrashPoint(name, shared_, scenario, interruption, deadline);
        }
        ASSERT_TRUE(WaitReached(scenario, deadline))
            << "child did not reach " << ScenarioName(scenario) << ": "
            << wait_reached_failure_;
        if (interruption == Interruption::kSigstopThenKill) {
            ASSERT_TRUE(WaitStopped(deadline))
                << "child did not enter SIGSTOP before the deadline";
        }
        const pid_t crashed_pid = child_pid_;
        KillAndReap(deadline);
        ASSERT_NE(shared_->region_id, 0u);
        ASSERT_FALSE(shared_->survivor.IsNull());
        ASSERT_FALSE(shared_->target.IsNull());

        auto observer = SharedMemorySegment::Open(name, /*read_only=*/false);
        ASSERT_TRUE(observer.ok()) << observer.status().ToString();
        auto* observed_sb = static_cast<SuperBlock*>(observer->base());
        ASSERT_EQ(observed_sb->region_id, shared_->region_id);
        const uint64_t epoch_before = LoadRegionEpoch(*observed_sb);
        const uint64_t fence_before = LoadRecoveryFence(*observed_sb);
        const uint64_t service_fence_before = LoadServiceFence(*observed_sb);
        const ProcessIdentity dead_owner = LoadServiceOwner(*observed_sb);
        ASSERT_EQ(dead_owner.process_id,
                  static_cast<uint64_t>(crashed_pid));
        ASSERT_EQ(ProbeProcessIdentity(dead_owner),
                  ProcessIdentityLiveness::kDead);
        ASSERT_FALSE(LoadCleanShutdown(*observed_sb));
        ASSERT_EQ(ServiceFencePhaseOf(service_fence_before),
                  ServiceFencePhase::kOwned);
        if (scenario == CrashScenario::kRecovery) {
            // The child intentionally died after claiming the recovery fence;
            // Attach must take over both the dead service owner and the expired
            // recovery lease before committing ACTIVE.
            EXPECT_EQ(LoadRegionState(*observed_sb), RegionState::kRecovering);
            EXPECT_EQ(RecoveryFencePhaseOf(fence_before),
                      RecoveryFencePhase::kRecovering);
        } else {
            // Do not write DIRTY here. The public v3 Attach path must acquire the
            // advisory lock, prove dead_owner dead, advance the service epoch,
            // and drive ACTIVE -> DIRTY -> RECOVERING itself.
            EXPECT_EQ(LoadRegionState(*observed_sb), RegionState::kActive);
            EXPECT_EQ(RecoveryFencePhaseOf(fence_before),
                      RecoveryFencePhase::kActive);
        }

        RegionAttachOptions attach_options;
        attach_options.name = name;
        attach_options.region_id = shared_->region_id;
        attach_options.recovery_wait_timeout_ms = kRecoveryWaitMs;
        auto recovered = SharedMemoryRegion::Attach(attach_options);

        if (scenario == CrashScenario::kCorruptRecovery) {
            ASSERT_FALSE(recovered.ok());
            EXPECT_EQ(recovered.status().code(), StatusCode::kCorruption)
                << recovered.status().ToString();
            EXPECT_EQ(LoadRegionState(*observed_sb),
                      RegionState::kQuarantined);
            EXPECT_EQ(RecoveryFencePhaseOf(LoadRecoveryFence(*observed_sb)),
                      RecoveryFencePhase::kQuarantined);
            EXPECT_GT(ServiceFenceEpoch(LoadServiceFence(*observed_sb)),
                      ServiceFenceEpoch(service_fence_before));
            // Attach did advance the service generation before scanner failure,
            // then CloseWithoutLifecycleUpdate safely relinquished that failed
            // supervisor generation. A quarantined Region has no live owner.
            EXPECT_EQ(ServiceFencePhaseOf(LoadServiceFence(*observed_sb)),
                      ServiceFencePhase::kUnowned);
            EXPECT_TRUE(LoadServiceOwner(*observed_sb).IsZero());
            EXPECT_EQ(LoadRecoveryLeaseNs(*observed_sb), 0u);

            auto allocator = CentralSlabAllocator::AttachInRegion(
                AllocatorStorage(*observer));
            ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
            shm::recovery::RecoveryScannerOptions options;
            options.repair = false;
            auto scanner = shm::recovery::RecoveryScanner::Create(
                *allocator, shm::recovery::RecoveryOwnership{}, options);
            ASSERT_TRUE(scanner.ok());
            auto report = scanner->Scan();
            ASSERT_TRUE(report.ok()) << report.status().ToString();
            EXPECT_EQ(report->orphan_slab_count, 0u) << report->details;
            EXPECT_EQ(report->bitmap_inconsistency_count, 0u)
                << report->details;
            EXPECT_EQ(report->corrupted_slab_count, 1u) << report->details;
            VerifySurvivor(&*allocator, shared_->survivor);

            // Restore the one test-injected CRC-covered field only for capacity
            // accounting and unlink cleanup. The Region remains quarantined.
            auto* header = reinterpret_cast<SlabHeader*>(
                static_cast<std::byte*>(observer->base()) +
                shared_->target.offset);
            header->object_size ^= 1u;
            ASSERT_TRUE(allocator->Inspect(shared_->target).ok());
            ASSERT_TRUE(allocator->Retire(shared_->target).ok());
            ASSERT_TRUE(allocator->Reclaim(shared_->target).ok());
            VerifyCleanScanner(*allocator);
            uint32_t available = 0;
            ProbeAvailableSlots(&*allocator, &available);
            EXPECT_EQ(available, shared_->baseline_available);
            VerifySurvivor(&*allocator, shared_->survivor);
            EXPECT_EQ(LoadRegionState(*observed_sb),
                      RegionState::kQuarantined);
        } else {
            ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
            EXPECT_TRUE(recovered->ValidateSupervisorFence().ok());
            EXPECT_EQ(LoadRegionState(*recovered->superblock()),
                      RegionState::kActive);
            EXPECT_EQ(RecoveryFencePhaseOf(
                          LoadRecoveryFence(*recovered->superblock())),
                      RecoveryFencePhase::kActive);
            EXPECT_EQ(RecoveryFenceEpoch(
                          LoadRecoveryFence(*recovered->superblock())),
                      LoadRegionEpoch(*recovered->superblock()));
            EXPECT_GT(LoadRegionEpoch(*recovered->superblock()), epoch_before);
            EXPECT_NE(LoadRecoveryFence(*recovered->superblock()), fence_before);
            EXPECT_GT(ServiceFenceEpoch(
                          LoadServiceFence(*recovered->superblock())),
                      ServiceFenceEpoch(service_fence_before));
            EXPECT_EQ(ServiceFencePhaseOf(
                          LoadServiceFence(*recovered->superblock())),
                      ServiceFencePhase::kOwned);
            EXPECT_EQ(LoadServiceOwner(*recovered->superblock()),
                      ProcessIdentity::Current());
            EXPECT_EQ(LoadRecoveryLeaseNs(*recovered->superblock()), 0u);

            auto recovered_allocator = CentralSlabAllocator::AttachInRegion(
                AllocatorStorage(*recovered));
            ASSERT_TRUE(recovered_allocator.ok())
                << recovered_allocator.status().ToString();
            VerifySurvivor(&*recovered_allocator, shared_->survivor);
            CleanupTarget(&*recovered_allocator, scenario, shared_->target);
            VerifyCleanScanner(*recovered_allocator);
            uint32_t available = 0;
            ProbeAvailableSlots(&*recovered_allocator, &available);
            EXPECT_EQ(available, shared_->baseline_available)
                << "allocator capacity did not return to survivor baseline";
            VerifySurvivor(&*recovered_allocator, shared_->survivor);
        }
        ASSERT_LT(StressClock::now(), deadline)
            << "scenario cleanup exceeded the global watchdog deadline";
    }

    SharedControl* shared_ = nullptr;
    pid_t child_pid_ = -1;
    StressDeadline watchdog_deadline_{};
    std::string wait_reached_failure_;
};

TEST_F(D1RegionRecoveryKillStressTest, RandomizedTimedRecoveryStress) {
    uint64_t stress_seconds = 0;
    uint64_t seed = DefaultStressSeed();
    std::string seconds_error;
    std::string seed_error;
    ASSERT_TRUE(ReadUnsignedEnvironment(
        "MINO_D1_REGION_RECOVERY_STRESS_SECONDS", kDefaultStressSeconds,
        &stress_seconds, &seconds_error))
        << seconds_error;
    ASSERT_TRUE(ReadUnsignedEnvironment(
        "MINO_D1_REGION_RECOVERY_STRESS_SEED", seed, &seed, &seed_error))
        << seed_error;

    const auto started = StressClock::now();
    const auto maximum_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        StressClock::time_point::max() - started).count();
    ASSERT_LE(stress_seconds, static_cast<uint64_t>(maximum_seconds));
    const auto configured_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::seconds(stress_seconds));
    const auto cleanup_reserve = CleanupReserveFor(configured_duration);
    const StressDeadline hard_deadline = started + configured_duration;
    const StressDeadline scheduling_deadline = hard_deadline - cleanup_reserve;
    watchdog_deadline_ = hard_deadline;

    RecordProperty("stress_seed", std::to_string(seed));
    RecordProperty("stress_seconds", std::to_string(stress_seconds));
    std::cout << "D1 Region recovery kill stress: seed=" << seed
              << " seconds=" << stress_seconds
              << " cleanup_reserve_ms=" << cleanup_reserve.count()
              << std::endl;

    std::mt19937_64 random(seed);
    uint64_t iteration = 0;
    while (scheduling_deadline - StressClock::now() >=
           kEstimatedMinimumRoundBudget) {
        const CrashScenario scenario =
            kScenarios[std::uniform_int_distribution<size_t>(
                0, kScenarios.size() - 1)(random)];
        const Interruption interruption =
            (random() & 3u) == 0 ? Interruption::kSigstopThenKill
                                 : Interruption::kSigkill;
        std::ostringstream trace;
        trace << "seed=" << seed << " iteration=" << iteration
              << " scenario=" << ScenarioName(scenario)
              << " signal=" << InterruptionName(interruption);
        SCOPED_TRACE(trace.str());
        ASSERT_NO_FATAL_FAILURE(
            RunScenario(scenario, interruption, hard_deadline));
        ++iteration;
    }

    const auto elapsed = StressClock::now() - started;
    const auto reserved_and_round_budget =
        cleanup_reserve + kEstimatedMinimumRoundBudget;
    const auto elapsed_lower_bound =
        configured_duration > reserved_and_round_budget
            ? configured_duration - reserved_and_round_budget
            : std::chrono::milliseconds::zero();
    EXPECT_GE(elapsed, elapsed_lower_bound)
        << "stress stopped before its scheduling lower bound; seed=" << seed
        << " iterations=" << iteration;
    EXPECT_LE(elapsed, configured_duration + kElapsedUpperSlack)
        << "stress exceeded configured deadline; seed=" << seed
        << " iterations=" << iteration;
    if (stress_seconds != 0) {
        EXPECT_GT(iteration, 0u)
            << "stress scheduled no rounds; seed=" << seed;
    }
    RecordProperty("iterations", std::to_string(iteration));
    std::cout << "D1 Region recovery kill stress completed: seed=" << seed
              << " iterations=" << iteration << " elapsed_ms="
              << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                     .count()
              << std::endl;
}

TEST_F(D1RegionRecoveryKillStressTest,
       EveryLifecyclePointHandlesSigkillAndSigstop) {
    const StressDeadline deadline =
        StressClock::now() + kDeterministicWatchdog;
    watchdog_deadline_ = deadline;
    uint64_t iteration = 0;
    for (CrashScenario scenario : kScenarios) {
        for (Interruption interruption :
             {Interruption::kSigkill, Interruption::kSigstopThenKill}) {
            std::ostringstream trace;
            trace << "seed=deterministic iteration=" << iteration++
                  << " scenario=" << ScenarioName(scenario)
                  << " signal=" << InterruptionName(interruption);
            SCOPED_TRACE(trace.str());
            ASSERT_NO_FATAL_FAILURE(
                RunScenario(scenario, interruption, deadline));
        }
    }
}

TEST_F(D1RegionRecoveryKillStressTest,
       RequiresPosixSharedMemoryAndProcessSignals) {
    SUCCEED();
}

}  // namespace
}  // namespace mino

#else

TEST(D1RegionRecoveryKillStressTest,
     RequiresPosixSharedMemoryAndProcessSignals) {
    GTEST_SKIP() << "requires POSIX fork, mmap, shm_open, and process signals";
}

#endif
