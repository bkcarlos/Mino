// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <gtest/gtest.h>

#if defined(__unix__) || defined(__APPLE__)

#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/journal_channel_recovery.h"
#include "mino/runtime/publisher.h"
#include "mino/runtime/subscriber.h"
#include "mino/runtime/subscriber_lease.h"

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
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/shm/channel/mpsc_channel.h"

namespace mino {

struct D2RecoveryStressMessage {
    uint64_t id;
    uint64_t checksum;
};

template <>
struct StaticMessageTraits<D2RecoveryStressMessage> {
    static constexpr bool kIsSpecialized = true;
    static constexpr TypeId type_id{0xD213};
    static constexpr uint32_t message_type = 0xD2130001u;
    static constexpr uint32_t schema_version = (1u << 16);
    static constexpr uint64_t schema_short_id = 0xD213D213D213D213ULL;
    static constexpr uint32_t layout_version = 1;
    static constexpr uint32_t index_flags = kIndexSlotFlagHasChildSlabs;
    static constexpr uint64_t kMask = 0xA55AA55AA55AA55AULL;

    static Status Validate(const D2RecoveryStressMessage& message) noexcept {
        return message.id != 0 && message.checksum == (message.id ^ kMask)
                   ? Status::Ok()
                   : Status::Error(StatusCode::kInvalidArgument,
                                   "invalid recovery stress message");
    }
};

namespace {

constexpr uint64_t kMpscCapacity = 64;
constexpr uint64_t kMpscChannelId = 0xD213'0000'0000'0001ULL;
constexpr uint64_t kBroadcastCapacity = 8;
constexpr size_t kAllocatorBytes = 1u << 20;
constexpr size_t kJournalBytes = 4096;
constexpr uint32_t kJournalTransactions = 16;
constexpr uint32_t kJournalHandles = 4;
constexpr size_t kMpscBytes =
    static_cast<size_t>(MpscChannel::RequiredSize(kMpscCapacity));
constexpr size_t kBroadcastBytes =
    static_cast<size_t>(BroadcastChannel::RequiredSize(kBroadcastCapacity));
constexpr size_t kLeaseBytes =
    static_cast<size_t>(SubscriberLeaseTable::RequiredSize());

enum class CrashScenario : uint32_t {
    kJournalInitializingTagged = 1,
    kJournalBuildingPublished = 2,
    kJournalAllocationPublished = 3,
    kJournalHandleAppended = 4,
    kMpscClaimTagged = 5,
    kMpscOwnerPublished = 6,
    kMpscCursorAdvanced = 7,
    kMpscWritingPublished = 8,
    kMpscReadyPublished = 9,
    kMpscTurnPublished = 10,
    kJournalReclaimTagged = 11,
    kJournalReclaimProgress = 12,
    kJournalFinalizingTagged = 13,
};

constexpr std::array<CrashScenario, 13> kScenarios = {
    CrashScenario::kJournalInitializingTagged,
    CrashScenario::kJournalBuildingPublished,
    CrashScenario::kJournalAllocationPublished,
    CrashScenario::kJournalHandleAppended,
    CrashScenario::kMpscClaimTagged,
    CrashScenario::kMpscOwnerPublished,
    CrashScenario::kMpscCursorAdvanced,
    CrashScenario::kMpscWritingPublished,
    CrashScenario::kMpscReadyPublished,
    CrashScenario::kMpscTurnPublished,
    CrashScenario::kJournalReclaimTagged,
    CrashScenario::kJournalReclaimProgress,
    CrashScenario::kJournalFinalizingTagged,
};

constexpr uint64_t kDefaultStressSeconds = 1;
using StressClock = std::chrono::steady_clock;
using StressDeadline = StressClock::time_point;
constexpr auto kDeterministicWatchdog = std::chrono::seconds(30);
constexpr auto kMinimumCleanupReserve = std::chrono::milliseconds(250);
constexpr auto kMaximumCleanupReserve = std::chrono::milliseconds(2000);
constexpr auto kEstimatedMinimumRoundBudget = std::chrono::milliseconds(75);
constexpr auto kForcedReapBudget = std::chrono::seconds(2);
constexpr auto kElapsedUpperSlack = std::chrono::seconds(1);

constexpr std::chrono::milliseconds CleanupReserveFor(
    std::chrono::milliseconds configured_duration) {
    if (configured_duration <= std::chrono::milliseconds::zero()) {
        return std::chrono::milliseconds::zero();
    }
    auto reserve = configured_duration / 2;
    if (reserve < kMinimumCleanupReserve) {
        reserve = kMinimumCleanupReserve;
    }
    if (reserve > kMaximumCleanupReserve) {
        reserve = kMaximumCleanupReserve;
    }
    return std::min(reserve, configured_duration);
}

static_assert(CleanupReserveFor(std::chrono::seconds(1)) ==
              std::chrono::milliseconds(500));
static_assert(CleanupReserveFor(std::chrono::milliseconds::zero()) ==
              std::chrono::milliseconds::zero());

enum class Interruption : uint8_t {
    kSigkill,
    kSigstopThenKill,
    kSigstopThenContinue,
};

struct StressRound {
    bool pid_incarnation = false;
    bool mutate_epoch = false;
    CrashScenario scenario = CrashScenario::kJournalInitializingTagged;
    Interruption interruption = Interruption::kSigkill;
};

const char* ScenarioName(CrashScenario scenario) {
    switch (scenario) {
        case CrashScenario::kJournalInitializingTagged:
            return "journal-initializing-tagged";
        case CrashScenario::kJournalBuildingPublished:
            return "journal-building-published";
        case CrashScenario::kJournalAllocationPublished:
            return "journal-allocation-published";
        case CrashScenario::kJournalHandleAppended:
            return "journal-handle-appended";
        case CrashScenario::kMpscClaimTagged:
            return "mpsc-claim-tagged";
        case CrashScenario::kMpscOwnerPublished:
            return "mpsc-owner-published";
        case CrashScenario::kMpscCursorAdvanced:
            return "mpsc-cursor-advanced";
        case CrashScenario::kMpscWritingPublished:
            return "mpsc-writing-published";
        case CrashScenario::kMpscReadyPublished:
            return "mpsc-ready-published";
        case CrashScenario::kMpscTurnPublished:
            return "mpsc-turn-published";
        case CrashScenario::kJournalReclaimTagged:
            return "journal-reclaim-tagged";
        case CrashScenario::kJournalReclaimProgress:
            return "journal-reclaim-progress";
        case CrashScenario::kJournalFinalizingTagged:
            return "journal-finalizing-tagged";
    }
    return "unknown-crash-point";
}

const char* InterruptionName(Interruption interruption) {
    switch (interruption) {
        case Interruption::kSigkill:
            return "SIGKILL";
        case Interruption::kSigstopThenKill:
            return "SIGSTOP->SIGKILL";
        case Interruption::kSigstopThenContinue:
            return "SIGSTOP->SIGCONT";
    }
    return "unknown-signal";
}

struct SharedBlock {
    alignas(64) unsigned char allocator_storage[kAllocatorBytes];
    alignas(64) unsigned char journal_storage[kJournalBytes];
    alignas(64) unsigned char mpsc_storage[kMpscBytes];
    alignas(64) unsigned char broadcast_storage[kBroadcastBytes];
    alignas(64) unsigned char lease_storage[kLeaseBytes];

    std::atomic<uint32_t> reached{0};
    std::atomic<uint32_t> child_ready{0};
    std::atomic<uint32_t> child_command{0};
    ProcessIdentity child_identity;
    ShmHandle root;
    ShmHandle child;
};

ClassTableConfig AllocatorConfig() {
    ClassTableConfig config;
    config.classes = {{.slot_size = 64, .slot_count = 256}};
    return config;
}

AllocationRequest RootRequest() {
    AllocationRequest request;
    request.object_size = sizeof(D2RecoveryStressMessage);
    request.type_id = StaticMessageTraits<D2RecoveryStressMessage>::type_id;
    request.schema = SchemaIdentity{
        .short_id = StaticMessageTraits<D2RecoveryStressMessage>::schema_short_id,
        .layout_version = StaticMessageTraits<D2RecoveryStressMessage>::layout_version};
    request.alignment = alignof(D2RecoveryStressMessage);
    return request;
}

AllocationRequest ChildRequest() {
    AllocationRequest request;
    request.object_size = sizeof(uint64_t);
    request.type_id = TypeId{0xD214};
    request.schema = SchemaIdentity{.short_id = 0xD214, .layout_version = 1};
    request.alignment = alignof(uint64_t);
    return request;
}

uint64_t NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            StressClock::now().time_since_epoch()).count());
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
    sigemptyset(&alarm_signal);
    sigaddset(&alarm_signal, SIGALRM);
    (void)::sigprocmask(SIG_UNBLOCK, &alarm_signal, nullptr);
    ::signal(SIGALRM, SIG_DFL);
    ::alarm(std::max(1u, seconds));
}

uint64_t DefaultStressSeed() {
    std::random_device random_device;
    uint64_t seed = static_cast<uint64_t>(random_device()) << 32;
    seed ^= static_cast<uint64_t>(random_device());
    seed ^= NowNs();
    seed ^= static_cast<uint64_t>(::getpid()) * 0x9E3779B97F4A7C15ULL;
    return seed;
}

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

StressRound MakeRandomRound(std::mt19937_64* random) {
    std::uniform_int_distribution<size_t> scenario(0, kScenarios.size());
    const size_t selected = scenario(*random);
    if (selected == kScenarios.size()) {
        return StressRound{
            .pid_incarnation = true,
            .mutate_epoch = ((*random)() & 1u) != 0,
        };
    }

    // Keep SIGKILL dominant while continuously exercising both SIGSTOP paths.
    std::uniform_int_distribution<uint32_t> signal(0, 7);
    const uint32_t selected_signal = signal(*random);
    Interruption interruption = Interruption::kSigkill;
    if (selected_signal == 0) {
        interruption = Interruption::kSigstopThenKill;
    } else if (selected_signal == 1) {
        interruption = Interruption::kSigstopThenContinue;
    }
    return StressRound{
        .scenario = kScenarios[selected],
        .interruption = interruption,
    };
}

struct HookContext {
    SharedBlock* shared = nullptr;
    CrashScenario target = CrashScenario::kJournalAllocationPublished;
    bool await_continue_command = false;
    bool triggered = false;
};

[[noreturn]] void PauseForKill(HookContext* context) {
    context->shared->reached.store(static_cast<uint32_t>(context->target),
                                   std::memory_order_release);
    for (;;) {
        ::pause();
    }
}

void ReachHook(HookContext* context) {
    context->shared->reached.store(static_cast<uint32_t>(context->target),
                                   std::memory_order_release);
    if (context->await_continue_command) {
        while (context->shared->child_command.load(
                   std::memory_order_acquire) == 0) {
            std::this_thread::yield();
        }
        return;
    }
    PauseForKill(context);
}

void JournalHook(AllocationJournal::PersistencePoint point, uint64_t,
                 void* opaque) noexcept {
    auto* context = static_cast<HookContext*>(opaque);
    const bool match =
        (context->target == CrashScenario::kJournalInitializingTagged &&
         point == AllocationJournal::PersistencePoint::kInitializingTagged) ||
        (context->target == CrashScenario::kJournalBuildingPublished &&
         point == AllocationJournal::PersistencePoint::kBuildingPublished) ||
        (context->target == CrashScenario::kJournalAllocationPublished &&
         point == AllocationJournal::PersistencePoint::kAllocationPublished) ||
        (context->target == CrashScenario::kJournalHandleAppended &&
         point == AllocationJournal::PersistencePoint::kHandleAppended) ||
        (context->target == CrashScenario::kJournalReclaimTagged &&
         point == AllocationJournal::PersistencePoint::kReclaimTagged) ||
        (context->target == CrashScenario::kJournalReclaimProgress &&
         point == AllocationJournal::PersistencePoint::kReclaimProgress) ||
        (context->target == CrashScenario::kJournalFinalizingTagged &&
         point == AllocationJournal::PersistencePoint::kFinalizingTagged);
    if (match && !context->triggered) {
        context->triggered = true;
        ReachHook(context);
    }
}

void MpscHook(MpscChannel::PersistencePoint point, uint64_t,
              void* opaque) noexcept {
    auto* context = static_cast<HookContext*>(opaque);
    const bool match =
        (context->target == CrashScenario::kMpscClaimTagged &&
         point == MpscChannel::PersistencePoint::kClaimTagged) ||
        (context->target == CrashScenario::kMpscOwnerPublished &&
         point == MpscChannel::PersistencePoint::kOwnerPublished) ||
        (context->target == CrashScenario::kMpscCursorAdvanced &&
         point == MpscChannel::PersistencePoint::kCursorAdvanced) ||
        (context->target == CrashScenario::kMpscWritingPublished &&
         point == MpscChannel::PersistencePoint::kWritingPublished) ||
        (context->target == CrashScenario::kMpscReadyPublished &&
         point == MpscChannel::PersistencePoint::kReadyPublished) ||
        (context->target == CrashScenario::kMpscTurnPublished &&
         point == MpscChannel::PersistencePoint::kTurnPublished);
    if (match && !context->triggered) {
        context->triggered = true;
        ReachHook(context);
    }
}

[[noreturn]] void PublisherChild(
    SharedBlock* shared, CrashScenario scenario, StressDeadline deadline,
    bool await_continue_command = false) {
    ArmChildWatchdog(deadline);
    auto allocator = CentralSlabAllocator::Attach(shared->allocator_storage);
    auto journal = AllocationJournal::Attach(
        shared->journal_storage, kJournalBytes, *allocator);
    auto channel = MpscChannel::Attach(shared->mpsc_storage);
    if (!allocator.ok() || !journal.ok() || !channel.ok()) {
        _exit(10);
    }

    HookContext hooks{.shared = shared,
                      .target = scenario,
                      .await_continue_command = await_continue_command};
    journal->SetPersistenceHook(&JournalHook, &hooks);
    channel->SetPersistenceHook(&MpscHook, &hooks);

    const ProcessIdentity owner = ProcessIdentity::Current();
    MpscChannel::ProducerIdentity producer{
        .owner = owner,
        .publisher_id = static_cast<uint64_t>(scenario),
    };
    Publisher<D2RecoveryStressMessage> publisher(
        *allocator, *channel, kMpscChannelId, producer, *journal);
    auto builder = publisher.Allocate();
    if (!builder.ok()) {
        _exit(11);
    }
    shared->root = builder->handle();
    (*builder)->id = 0xD2130000ULL + static_cast<uint32_t>(scenario);
    (*builder)->checksum =
            (*builder)->id ^ StaticMessageTraits<D2RecoveryStressMessage>::kMask;

    auto child_build = builder->AllocateChild(ChildRequest());
    if (!child_build.ok()) {
        _exit(12);
    }
    shared->child = child_build->handle;
    *static_cast<uint64_t*>(child_build->data) = (*builder)->id;

    if (scenario == CrashScenario::kJournalReclaimTagged ||
        scenario == CrashScenario::kJournalReclaimProgress) {
        const Status status = publisher.Abort(std::move(*builder));
        _exit(status.ok() ? 0 : 13);
    }
    const Status status = publisher.PublishLocal(std::move(*builder));
    _exit(status.ok() ? 0 : 14);
}

class D2RecoveryStressTest : public ::testing::Test {
protected:
    void SetUp() override {
        void* mapped = ::mmap(nullptr, sizeof(SharedBlock),
                              PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapped, MAP_FAILED) << std::strerror(errno);
        shared_ = new (mapped) SharedBlock{};

        auto allocator = CentralSlabAllocator::Create(
            shared_->allocator_storage, kAllocatorBytes, AllocatorConfig());
        ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
        allocator_ = *allocator;
        auto journal = AllocationJournal::Init(
            shared_->journal_storage, kJournalBytes, kJournalTransactions,
            kJournalHandles, allocator_);
        ASSERT_TRUE(journal.ok()) << journal.status().ToString();
        journal_.emplace(*journal);
        auto mpsc = MpscChannel::Init(shared_->mpsc_storage, kMpscCapacity);
        ASSERT_TRUE(mpsc.ok()) << mpsc.status().ToString();
        mpsc_.emplace(*mpsc);
        recovery_.emplace(*journal_);
        MpscChannel::ProducerIdentity registration_identity{
            .owner = ProcessIdentity::Current(),
            .publisher_id = kMpscChannelId,
        };
        Publisher<D2RecoveryStressMessage> registered_publisher(
            allocator_, *mpsc_, kMpscChannelId, registration_identity,
            *journal_);
        ASSERT_TRUE(
            registered_publisher.RegisterRecoveryChannel(*recovery_).ok());
        auto broadcast = BroadcastChannel::Init(shared_->broadcast_storage,
                                                 kBroadcastCapacity);
        ASSERT_TRUE(broadcast.ok());
        broadcast_.emplace(*broadcast);
        auto leases = SubscriberLeaseTable::Init(shared_->lease_storage);
        ASSERT_TRUE(leases.ok());
        leases_.emplace(*leases);

        baseline_available_slots_ = ProbeAvailableSlots();
        ASSERT_EQ(baseline_available_slots_, allocator_.total_slot_count());
    }

    void TearDown() override {
        if (child_pid_ > 0) {
            const int kill_result = ::kill(child_pid_, SIGKILL);
            const int kill_error = errno;
            if (kill_result != 0 && kill_error != ESRCH) {
                ADD_FAILURE() << "failed to SIGKILL child during cleanup: "
                              << std::strerror(kill_error);
            }
            int status = 0;
            const StressDeadline reap_deadline =
                StressClock::now() + kForcedReapBudget;
            if (!WaitForExit(reap_deadline, &status)) {
                ADD_FAILURE() << "failed to reap child within the bounded "
                                 "cleanup budget";
            }
        }
        if (shared_ != nullptr) {
            ::munmap(shared_, sizeof(SharedBlock));
        }
    }

    uint32_t LiveSlabs() const {
        uint32_t live = 0;
        for (uint32_t i = 0; i < allocator_.total_slot_count(); ++i) {
            SlabHeader header{};
            EXPECT_TRUE(allocator_.ReadSlotByIndex(i, &header, nullptr));
            if (header.object_state.load(std::memory_order_acquire) !=
                static_cast<uint32_t>(ObjectState::kFree)) {
                ++live;
            }
        }
        return live;
    }

    uint32_t ProbeAvailableSlots() {
        EXPECT_EQ(LiveSlabs(), 0u)
            << "capacity probe requires all slab states to start FREE";
        std::vector<ShmHandle> handles;
        handles.reserve(allocator_.total_slot_count());
        for (uint32_t i = 0; i <= allocator_.total_slot_count(); ++i) {
            auto allocation = allocator_.Allocate(RootRequest());
            if (!allocation.ok()) {
                EXPECT_EQ(allocation.status().code(),
                          StatusCode::kResourceExhausted)
                    << allocation.status().ToString();
                break;
            }
            handles.push_back(*allocation);
        }
        const uint32_t available = static_cast<uint32_t>(handles.size());
        EXPECT_EQ(LiveSlabs(), available)
            << "bitmap occupancy and non-FREE slab states diverged while full";
        for (ShmHandle handle : handles) {
            EXPECT_TRUE(allocator_.Abort(handle).ok());
        }
        EXPECT_EQ(LiveSlabs(), 0u)
            << "capacity probe must restore every slab state to FREE";
        return available;
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

    bool WaitForAtomic(std::atomic<uint32_t>* value, uint32_t expected,
                       StressDeadline deadline) {
        for (;;) {
            if (value->load(std::memory_order_acquire) == expected) {
                return true;
            }
            int status = 0;
            const pid_t result = ::waitpid(child_pid_, &status, WNOHANG);
            if (result == child_pid_) {
                child_pid_ = -1;
                return false;
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

    bool WaitReached(uint32_t value, StressDeadline deadline) {
        return WaitForAtomic(&shared_->reached, value, deadline);
    }

    bool WaitStopped(StressDeadline deadline) {
        for (;;) {
            int status = 0;
            const pid_t result = ::waitpid(child_pid_, &status,
                                           WNOHANG | WUNTRACED);
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

    void KillAndReap() {
        ASSERT_GT(child_pid_, 0);
        ASSERT_EQ(::kill(child_pid_, SIGKILL), 0);
        int status = 0;
        const StressDeadline reap_deadline =
            StressClock::now() + kForcedReapBudget;
        ASSERT_TRUE(WaitForExit(reap_deadline, &status))
            << "child did not exit within the bounded cleanup budget";
        ASSERT_TRUE(WIFSIGNALED(status));
    }

    bool ExpectedVisible(CrashScenario scenario) const {
        return scenario == CrashScenario::kMpscReadyPublished ||
               scenario == CrashScenario::kMpscTurnPublished ||
               scenario == CrashScenario::kJournalFinalizingTagged;
    }

    bool ExpectedVisibleAfterCompletion(CrashScenario scenario) const {
        return scenario != CrashScenario::kJournalReclaimTagged &&
               scenario != CrashScenario::kJournalReclaimProgress;
    }

    void RecoverProducts() {
        (void)mpsc_->AbortOrphanedReservations(NowNs() + 1000,
                                               /*lease_ns=*/1);
        (void)recovery_->RecoverOrphans();
        // A second pass verifies idempotency and lets kReclaiming/kFinalizing
        // work that was exposed by the first pass reach FREE.
        (void)mpsc_->AbortOrphanedReservations(NowNs() + 1000,
                                               /*lease_ns=*/1);
        (void)recovery_->RecoverOrphans();
    }

    void RunScenario(
        CrashScenario scenario, StressDeadline deadline,
        Interruption interruption = Interruption::kSigkill) {
        ASSERT_EQ(LiveSlabs(), 0u);
        ASSERT_EQ(journal_->ActiveTransactionCount(), 0u);
        ASSERT_TRUE(mpsc_->IsEmpty());
        shared_->root = {};
        shared_->child = {};
        shared_->reached.store(0, std::memory_order_relaxed);
        shared_->child_command.store(0, std::memory_order_relaxed);

        child_pid_ = ::fork();
        ASSERT_NE(child_pid_, -1) << std::strerror(errno);
        if (child_pid_ == 0) {
            PublisherChild(shared_, scenario, deadline,
                           interruption != Interruption::kSigkill);
        }
        ASSERT_TRUE(WaitReached(static_cast<uint32_t>(scenario), deadline))
            << "child did not reach crash point before the absolute deadline";

        bool child_completed = false;
        if (interruption == Interruption::kSigkill) {
            KillAndReap();
        } else {
            ASSERT_EQ(::kill(child_pid_, SIGSTOP), 0);
            ASSERT_TRUE(WaitStopped(deadline))
                << "child did not stop before the absolute deadline";
            if (interruption == Interruption::kSigstopThenKill) {
                KillAndReap();
            } else {
                shared_->child_command.store(1, std::memory_order_release);
                ASSERT_EQ(::kill(child_pid_, SIGCONT), 0);
                int status = 0;
                ASSERT_TRUE(WaitForExit(deadline, &status))
                    << "continued child did not exit before the absolute deadline";
                ASSERT_TRUE(WIFEXITED(status));
                ASSERT_EQ(WEXITSTATUS(status), 0);
                child_completed = true;
            }
        }

        RecoverProducts();
        EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);

        Subscriber<D2RecoveryStressMessage> subscriber(allocator_, *mpsc_);
        auto message = subscriber.TryPoll();
        const bool expected_visible =
            child_completed ? ExpectedVisibleAfterCompletion(scenario)
                            : ExpectedVisible(scenario);
        if (expected_visible) {
            ASSERT_TRUE(message.ok()) << message.status().ToString();
            EXPECT_EQ(message->metadata().payload, shared_->root);
            EXPECT_TRUE(std::move(*message).Ack().ok());
        } else {
            ASSERT_FALSE(message.ok());
            EXPECT_EQ(message.status().code(), StatusCode::kWouldBlock);
        }
        EXPECT_TRUE(mpsc_->IsEmpty());
        EXPECT_EQ(LiveSlabs(), 0u)
            << "product recovery/ACK path must reclaim root and child";
    }

    void RunPidIncarnationScenario(bool mutate_epoch,
                                   StressDeadline deadline) {
        ASSERT_EQ(LiveSlabs(), 0u);
        ASSERT_EQ(journal_->ActiveTransactionCount(), 0u);
        ASSERT_TRUE(mpsc_->IsEmpty());
        shared_->child_ready.store(0, std::memory_order_relaxed);
        shared_->child_command.store(0, std::memory_order_relaxed);
        shared_->root = {};
        shared_->child = {};

        child_pid_ = ::fork();
        ASSERT_NE(child_pid_, -1) << std::strerror(errno);
        if (child_pid_ == 0) {
            ArmChildWatchdog(deadline);
            shared_->child_identity = ProcessIdentity::Current();
            shared_->child_ready.store(1, std::memory_order_release);
            while (shared_->child_command.load(std::memory_order_acquire) == 0) {
                std::this_thread::yield();
            }
            _exit(0);
        }
        ASSERT_TRUE(WaitForAtomic(&shared_->child_ready, 1, deadline))
            << "PID incarnation child was not ready before the absolute deadline";

        ProcessIdentity reused_incarnation = shared_->child_identity;
        reused_incarnation.start_time_ns ^= 0x100000001ULL;
        if (mutate_epoch) {
            reused_incarnation.process_epoch ^= 0x200000001ULL;
        }
        auto transaction = journal_->Begin(reused_incarnation);
        ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
        auto root = journal_->AllocateRoot(*transaction, RootRequest());
        ASSERT_TRUE(root.ok()) << root.status().ToString();
        shared_->root = *root;
        ASSERT_TRUE(allocator_.BeginBuild(*root).ok());
        auto child = journal_->AllocateChild(*transaction, ChildRequest());
        ASSERT_TRUE(child.ok()) << child.status().ToString();
        shared_->child = *child;
        ASSERT_TRUE(allocator_.BeginBuild(*child).ok());

        EXPECT_EQ(journal_->RecoverOrphans(), 0u)
            << "a live PID with a mismatched incarnation must not be reclaimed";
        EXPECT_TRUE(allocator_.Inspect(*root).ok());
        EXPECT_TRUE(allocator_.Inspect(*child).ok());

        KillAndReap();
        RecoverProducts();
        EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
        EXPECT_TRUE(mpsc_->IsEmpty());
        EXPECT_EQ(LiveSlabs(), 0u);
    }

    void VerifyQueueProgressAndCapacity(uint64_t iteration,
                                        StressDeadline deadline) {
        ASSERT_LT(StressClock::now(), deadline)
            << "round verification started after the absolute deadline";
        ASSERT_EQ(journal_->ActiveTransactionCount(), 0u);
        ASSERT_TRUE(mpsc_->IsEmpty());
        ASSERT_EQ(LiveSlabs(), 0u);

        MpscChannel::ProducerIdentity producer{
            .owner = ProcessIdentity::Current(),
            .publisher_id = 0xD213000000000000ULL ^ iteration,
        };
        Publisher<D2RecoveryStressMessage> publisher(
            allocator_, *mpsc_, kMpscChannelId, producer, *journal_);
        auto builder = publisher.Allocate();
        ASSERT_TRUE(builder.ok()) << builder.status().ToString();
        const ShmHandle progress_root = builder->handle();
        (*builder)->id = 0xD213000000000000ULL ^ (iteration + 1);
        (*builder)->checksum =
            (*builder)->id ^ StaticMessageTraits<D2RecoveryStressMessage>::kMask;
        auto child = builder->AllocateChild(ChildRequest());
        ASSERT_TRUE(child.ok()) << child.status().ToString();
        const ShmHandle progress_child = child->handle;
        *static_cast<uint64_t*>(child->data) = (*builder)->id;
        ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

        Subscriber<D2RecoveryStressMessage> subscriber(allocator_, *mpsc_);
        auto message = subscriber.TryPoll();
        ASSERT_TRUE(message.ok()) << message.status().ToString();
        EXPECT_EQ(message->metadata().payload, progress_root);
        ASSERT_TRUE(std::move(*message).Ack().ok());

        EXPECT_FALSE(allocator_.Inspect(progress_root).ok());
        EXPECT_FALSE(allocator_.Inspect(progress_child).ok());
        EXPECT_TRUE(mpsc_->IsEmpty());
        EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
        EXPECT_EQ(LiveSlabs(), 0u)
            << "normal progress must reclaim both root and child";
        EXPECT_EQ(ProbeAvailableSlots(), baseline_available_slots_)
            << "allocator availability did not return to its baseline";
        EXPECT_EQ(LiveSlabs(), 0u)
            << "the availability probe itself must not retain slabs";
        EXPECT_LT(StressClock::now(), deadline)
            << "round verification exceeded the absolute deadline";
    }

    SharedBlock* shared_ = nullptr;
    CentralSlabAllocator allocator_;
    std::optional<AllocationJournal> journal_;
    std::optional<MpscChannel> mpsc_;
    std::optional<JournalChannelRecoveryCoordinator> recovery_;
    std::optional<BroadcastChannel> broadcast_;
    std::optional<SubscriberLeaseTable> leases_;
    pid_t child_pid_ = -1;
    uint32_t baseline_available_slots_ = 0;
};

TEST_F(D2RecoveryStressTest, RandomizedTimedRecoveryStress) {
    uint64_t stress_seconds = 0;
    uint64_t seed = DefaultStressSeed();
    std::string seconds_error;
    std::string seed_error;
    const bool seconds_ok = ReadUnsignedEnvironment(
        "MINO_D2_RECOVERY_STRESS_SECONDS", kDefaultStressSeconds,
        &stress_seconds, &seconds_error);
    const bool seed_ok = ReadUnsignedEnvironment(
        "MINO_D2_RECOVERY_STRESS_SEED", seed, &seed, &seed_error);
    ASSERT_TRUE(seconds_ok) << seconds_error;
    ASSERT_TRUE(seed_ok) << seed_error;

    const auto started = StressClock::now();
    const auto maximum_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        StressClock::time_point::max() - started - kElapsedUpperSlack).count();
    ASSERT_LE(stress_seconds, static_cast<uint64_t>(maximum_seconds))
        << "MINO_D2_RECOVERY_STRESS_SECONDS is too large";
    const auto configured_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::seconds(stress_seconds));
    const auto cleanup_reserve = CleanupReserveFor(configured_duration);
    const StressDeadline hard_deadline = started + configured_duration;
    const StressDeadline scheduling_deadline =
        hard_deadline - cleanup_reserve;
    // Correctness waits use hard_deadline. Only post-SIGKILL recovery and
    // verification may consume the test's existing elapsed-time slack.
    const StressDeadline cleanup_deadline =
        hard_deadline + kElapsedUpperSlack;

    RecordProperty("stress_seed", std::to_string(seed));
    RecordProperty("stress_seconds", std::to_string(stress_seconds));
    RecordProperty("cleanup_reserve_ms",
                   std::to_string(cleanup_reserve.count()));
    std::cout << "D2 recovery stress: seed=" << seed
              << " seconds=" << stress_seconds
              << " cleanup_reserve_ms=" << cleanup_reserve.count()
              << " round_start_budget_ms="
              << kEstimatedMinimumRoundBudget.count() << std::endl;

    std::mt19937_64 random(seed);
    uint64_t iteration = 0;
    // Preserve both the estimated minimum round budget and the cleanup reserve;
    // never start a new child close to the hard deadline.
    while (scheduling_deadline - StressClock::now() >=
           kEstimatedMinimumRoundBudget) {
        const StressRound round = MakeRandomRound(&random);
        std::ostringstream trace;
        trace << "seed=" << seed << " iteration=" << iteration
              << " scenario=";
        if (round.pid_incarnation) {
            trace << "pid-incarnation-reuse"
                  << " mutation="
                  << (round.mutate_epoch ? "start-time+epoch"
                                         : "start-time");
        } else {
            trace << ScenarioName(round.scenario)
                  << " signal=" << InterruptionName(round.interruption);
        }
        SCOPED_TRACE(trace.str());

        if (round.pid_incarnation) {
            ASSERT_NO_FATAL_FAILURE(
                RunPidIncarnationScenario(round.mutate_epoch, hard_deadline));
        } else {
            ASSERT_NO_FATAL_FAILURE(
                RunScenario(round.scenario, hard_deadline,
                            round.interruption));
        }
        ASSERT_NO_FATAL_FAILURE(
            VerifyQueueProgressAndCapacity(iteration, cleanup_deadline));
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
        << "timed stress stopped before its scheduling lower bound; seed="
        << seed << " iterations=" << iteration;
    EXPECT_LE(elapsed, configured_duration + kElapsedUpperSlack)
        << "timed stress exceeded its configured hard bound plus tolerance; seed="
        << seed << " iterations=" << iteration;
    if (stress_seconds != 0) {
        EXPECT_GT(iteration, 0u)
            << "timed stress scheduled no rounds; seed=" << seed;
    }
    RecordProperty(
        "elapsed_ms",
        std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                           elapsed).count()));
    std::cout << "D2 recovery stress completed: seed=" << seed
              << " iterations=" << iteration
              << " elapsed_ms="
              << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                     .count()
              << std::endl;
}

TEST_F(D2RecoveryStressTest, SigkillAtEveryPersistentStoreRecovers) {
    const StressDeadline deadline =
        StressClock::now() + kDeterministicWatchdog;
    uint64_t iteration = 0;
    for (CrashScenario scenario : kScenarios) {
        SCOPED_TRACE(ScenarioName(scenario));
        ASSERT_NO_FATAL_FAILURE(RunScenario(scenario, deadline));
        ASSERT_NO_FATAL_FAILURE(
            VerifyQueueProgressAndCapacity(iteration++, deadline));
    }
}

TEST_F(D2RecoveryStressTest, SigstopLiveOwnerIsNeverRecovered) {
    const StressDeadline deadline =
        StressClock::now() + kDeterministicWatchdog;
    shared_->reached.store(0, std::memory_order_relaxed);
    shared_->child_command.store(0, std::memory_order_relaxed);
    child_pid_ = ::fork();
    ASSERT_NE(child_pid_, -1);
    if (child_pid_ == 0) {
        PublisherChild(shared_, CrashScenario::kMpscWritingPublished, deadline,
                       /*await_continue_command=*/true);
    }
    ASSERT_TRUE(WaitReached(
        static_cast<uint32_t>(CrashScenario::kMpscWritingPublished), deadline));
    ASSERT_EQ(::kill(child_pid_, SIGSTOP), 0);
    ASSERT_TRUE(WaitStopped(deadline));

    EXPECT_EQ(mpsc_->AbortOrphanedReservations(NowNs() + 1000, 1), 0u);
    EXPECT_EQ(recovery_->RecoverOrphans(), 0u);
    EXPECT_GT(LiveSlabs(), 0u);

    shared_->child_command.store(1, std::memory_order_release);
    ASSERT_EQ(::kill(child_pid_, SIGCONT), 0);
    int status = 0;
    ASSERT_TRUE(WaitForExit(deadline, &status));
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);

    Subscriber<D2RecoveryStressMessage> subscriber(allocator_, *mpsc_);
    auto message = subscriber.TryPoll();
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    EXPECT_TRUE(std::move(*message).Ack().ok());
    EXPECT_EQ(LiveSlabs(), 0u);
    VerifyQueueProgressAndCapacity(0, deadline);
}

TEST_F(D2RecoveryStressTest, ForeignLivePidIncarnationMismatchIsUnknown) {
    const StressDeadline deadline =
        StressClock::now() + kDeterministicWatchdog;
    ASSERT_NO_FATAL_FAILURE(
        RunPidIncarnationScenario(/*mutate_epoch=*/true, deadline));
    ASSERT_NO_FATAL_FAILURE(VerifyQueueProgressAndCapacity(0, deadline));
}

TEST_F(D2RecoveryStressTest, DeadBroadcastSubscriberLeaseClearsRealAcks) {
    const StressDeadline deadline =
        StressClock::now() + kDeterministicWatchdog;
    child_pid_ = ::fork();
    ASSERT_NE(child_pid_, -1);
    if (child_pid_ == 0) {
        ArmChildWatchdog(deadline);
        shared_->child_identity = ProcessIdentity::Current();
        shared_->child_ready.store(1, std::memory_order_release);
        for (;;) {
            ::pause();
        }
    }
    ASSERT_TRUE(WaitForAtomic(&shared_->child_ready, 1, deadline));

    constexpr uint64_t kT0 = 10'000;
    SubscriberLeaseCoordinator coordinator(*broadcast_, *leases_);
    auto lease = coordinator.Register(SubscriberId{0}, shared_->child_identity,
                                      kT0);
    ASSERT_TRUE(lease.ok());
    for (uint64_t i = 1; i <= kBroadcastCapacity; ++i) {
        auto reservation = broadcast_->Reserve();
        ASSERT_TRUE(reservation.ok());
        reservation->slot()->msg_type = static_cast<uint32_t>(i);
        reservation->slot()->schema_version = 1;
        reservation->slot()->schema_short_id = i;
        reservation->slot()->schema_layout_version = 1;
        reservation->slot()->timestamp_ns = i;
        reservation->slot()->payload = {};
        reservation->slot()->payload_len = 0;
        reservation->slot()->flags = 0;
        ASSERT_TRUE(std::move(*reservation).Commit().ok());
    }
    ASSERT_TRUE(broadcast_->IsFull());

    KillAndReap();
    EXPECT_EQ(coordinator.EvictExpired(kT0 + 100, 1), 1u);
    EXPECT_FALSE(broadcast_->IsFull());
    EXPECT_EQ(leases_->State(0), SubscriberLeaseState::kEvicted);
}

}  // namespace
}  // namespace mino

#else

TEST(D2RecoveryStressTest, RequiresPosixSharedMemoryAndProcessSignals) {
    GTEST_SKIP() << "D2 recovery stress requires POSIX mmap/fork/signals";
}

#endif  // defined(__unix__) || defined(__APPLE__)
