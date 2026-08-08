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

enum class CampaignScenario : uint8_t {
    kPublisherCrash,
    kSubscriberKill,
    kSlowSubscriber,
    kLeaseBoundary,
    kPidIncarnation,
    kCount,
};

constexpr std::array<CampaignScenario, 5> kCampaignScenarios = {
    CampaignScenario::kPublisherCrash,
    CampaignScenario::kSubscriberKill,
    CampaignScenario::kSlowSubscriber,
    CampaignScenario::kLeaseBoundary,
    CampaignScenario::kPidIncarnation,
};
constexpr uint64_t kSubscriberT0 = 10'000;
constexpr uint64_t kSubscriberLeaseNs = 100;
constexpr uint32_t kSubscriberBorrowReached = 0xD215;

struct StressRound {
    CampaignScenario campaign = CampaignScenario::kPublisherCrash;
    bool mutate_epoch = false;
    CrashScenario scenario = CrashScenario::kJournalInitializingTagged;
    Interruption interruption = Interruption::kSigkill;
    uint32_t slow_yields = 0;
};

const char* CampaignScenarioName(CampaignScenario scenario) {
    switch (scenario) {
        case CampaignScenario::kPublisherCrash:
            return "publisher_crash";
        case CampaignScenario::kSubscriberKill:
            return "subscriber_kill";
        case CampaignScenario::kSlowSubscriber:
            return "slow_subscriber";
        case CampaignScenario::kLeaseBoundary:
            return "lease_boundary";
        case CampaignScenario::kPidIncarnation:
            return "pid_incarnation";
        case CampaignScenario::kCount:
            break;
    }
    return "unknown_campaign_scenario";
}

constexpr size_t CampaignScenarioIndex(CampaignScenario scenario) {
    return static_cast<size_t>(scenario);
}

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

const char* InterruptionMarkerName(Interruption interruption) {
    switch (interruption) {
        case Interruption::kSigkill:
            return "sigkill";
        case Interruption::kSigstopThenKill:
            return "sigstop_then_sigkill";
        case Interruption::kSigstopThenContinue:
            return "sigstop_then_sigcont";
    }
    return "unknown";
}

void ReportCutEvent(std::string_view event, CrashScenario scenario,
                    std::string_view interruption) {
    std::cout << "D2_RECOVERY_CUT_" << event
              << " cut=" << ScenarioName(scenario)
              << " interruption=" << interruption << std::endl;
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

StressRound MakeRandomRound(
    std::mt19937_64* random,
    std::optional<CampaignScenario> forced_campaign = std::nullopt) {
    CampaignScenario campaign = CampaignScenario::kPublisherCrash;
    if (forced_campaign.has_value()) {
        campaign = *forced_campaign;
    } else {
        // Publisher crash points remain dominant while subscriber-side recovery
        // continuously receives a meaningful share of a long campaign.
        std::uniform_int_distribution<uint32_t> campaign_distribution(0, 11);
        const uint32_t selected = campaign_distribution(*random);
        if (selected >= 8) {
            campaign = kCampaignScenarios[1 + (selected - 8)];
        }
    }

    StressRound round;
    round.campaign = campaign;
    round.mutate_epoch = ((*random)() & 1u) != 0;
    round.slow_yields = static_cast<uint32_t>((*random)() & 7u);
    if (campaign != CampaignScenario::kPublisherCrash) {
        return round;
    }

    std::uniform_int_distribution<size_t> scenario(0, kScenarios.size() - 1);
    round.scenario = kScenarios[scenario(*random)];

    // Keep SIGKILL dominant while continuously exercising both SIGSTOP paths.
    std::uniform_int_distribution<uint32_t> signal(0, 7);
    const uint32_t selected_signal = signal(*random);
    if (selected_signal == 0) {
        round.interruption = Interruption::kSigstopThenKill;
    } else if (selected_signal == 1) {
        round.interruption = Interruption::kSigstopThenContinue;
    }
    return round;
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

[[noreturn]] void SubscriberChild(SharedBlock* shared,
                                  bool hold_borrow,
                                  StressDeadline deadline) {
    ArmChildWatchdog(deadline);
    auto channel = BroadcastChannel::Attach(shared->broadcast_storage);
    auto leases = SubscriberLeaseTable::Attach(shared->lease_storage);
    if (!channel.ok() || !leases.ok()) {
        _exit(20);
    }
    const ProcessIdentity owner = ProcessIdentity::Current();
    SubscriberLeaseCoordinator coordinator(*channel, *leases);
    auto lease = coordinator.Register(SubscriberId{0}, owner, kSubscriberT0);
    if (!lease.ok()) {
        _exit(21);
    }
    shared->child_identity = owner;
    shared->child_ready.store(1, std::memory_order_release);
    while (shared->child_command.load(std::memory_order_acquire) == 0) {
        std::this_thread::yield();
    }
    if (!hold_borrow) {
        for (;;) {
            ::pause();
        }
    }
    auto borrow = channel->Poll(lease->subscriber);
    if (!borrow.ok()) {
        _exit(22);
    }
    shared->reached.store(kSubscriberBorrowReached,
                          std::memory_order_release);
    for (;;) {
        ::pause();
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
        ASSERT_EQ(WTERMSIG(status), SIGKILL)
            << "child terminated by an unexpected signal";
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

    uint64_t OutstandingBroadcastAcks() const {
        const auto* era_metas =
            reinterpret_cast<const BroadcastChannel::BroadcastEraMeta*>(
                shared_->broadcast_storage +
                BroadcastChannel::EraMetasOffset(kBroadcastCapacity));
        uint64_t outstanding = 0;
        for (uint64_t slot = 0; slot < kBroadcastCapacity; ++slot) {
            for (uint32_t id = 0; id < BroadcastChannel::kMaxSubscribers; ++id) {
                if (era_metas[slot].ack_era[id].load(
                        std::memory_order_acquire) != 0) {
                    ++outstanding;
                }
            }
        }
        return outstanding;
    }

    void PublishBroadcast(uint64_t count) {
        for (uint64_t i = 0; i < count; ++i) {
            auto reservation = broadcast_->Reserve();
            ASSERT_TRUE(reservation.ok())
                << reservation.status().ToString();
            const uint64_t sequence = reservation->sequence();
            reservation->slot()->msg_type =
                static_cast<uint32_t>(0xD2150000u ^ sequence);
            reservation->slot()->schema_version = 1;
            reservation->slot()->schema_short_id = 0xD215000000000000ULL ^ sequence;
            reservation->slot()->schema_layout_version = 1;
            reservation->slot()->timestamp_ns = sequence;
            reservation->slot()->payload = {};
            reservation->slot()->payload_len = 0;
            reservation->slot()->flags = 0;
            ASSERT_TRUE(std::move(*reservation).Commit().ok());
        }
    }

    void DrainBroadcast(BroadcastChannel::SubscriberHandle subscriber,
                        uint64_t count, uint32_t yields = 0) {
        for (uint64_t i = 0; i < count; ++i) {
            for (uint32_t delay = 0; delay < yields; ++delay) {
                std::this_thread::yield();
            }
            auto borrow = broadcast_->Poll(subscriber);
            ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
            ASSERT_TRUE(std::move(*borrow).Ack().ok());
        }
    }

    void VerifyConservation(uint64_t obligations, uint64_t acknowledged,
                            uint64_t recovered) {
        broadcast_->CollectGarbage();
        EXPECT_EQ(obligations, acknowledged + recovered)
            << "every broadcast responsibility must be ACKed or recovered";
        EXPECT_EQ(OutstandingBroadcastAcks(), 0u)
            << "no broadcast ACK responsibility may remain orphaned";
        EXPECT_FALSE(broadcast_->IsFull());
        for (uint32_t id = 0; id < SubscriberLeaseTable::kMaxSubscribers; ++id) {
            const SubscriberLeaseState state = leases_->State(id);
            EXPECT_TRUE(state == SubscriberLeaseState::kFree ||
                        state == SubscriberLeaseState::kEvicted)
                << "subscriber lease " << id << " remained transitional/active";
        }
        EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
        EXPECT_TRUE(mpsc_->IsEmpty());
        EXPECT_EQ(LiveSlabs(), 0u)
            << "broadcast recovery must not retain allocator slabs";
    }

    void ResetSubscriberChildState() {
        shared_->child_ready.store(0, std::memory_order_relaxed);
        shared_->child_command.store(0, std::memory_order_relaxed);
        shared_->reached.store(0, std::memory_order_relaxed);
        shared_->child_identity = {};
    }

    void RunSubscriberKillScenario(StressDeadline deadline) {
        ResetSubscriberChildState();
        child_pid_ = ::fork();
        ASSERT_NE(child_pid_, -1) << std::strerror(errno);
        if (child_pid_ == 0) {
            SubscriberChild(shared_, /*hold_borrow=*/true, deadline);
        }
        ASSERT_TRUE(WaitForAtomic(&shared_->child_ready, 1, deadline));
        auto observed = leases_->Read(0);
        ASSERT_TRUE(observed.ok()) << observed.status().ToString();
        ASSERT_EQ(observed->state, SubscriberLeaseState::kActive);
        const SubscriberLeaseHandle stale = observed->handle;

        ASSERT_NO_FATAL_FAILURE(PublishBroadcast(kBroadcastCapacity));
        ASSERT_TRUE(broadcast_->IsFull());
        shared_->child_command.store(1, std::memory_order_release);
        ASSERT_TRUE(WaitReached(kSubscriberBorrowReached, deadline))
            << "subscriber did not acquire a recoverable Borrow";
        KillAndReap();

        SubscriberLeaseCoordinator coordinator(*broadcast_, *leases_);
        ASSERT_EQ(coordinator.EvictExpired(
                      kSubscriberT0 + kSubscriberLeaseNs,
                      kSubscriberLeaseNs),
                  1u);
        EXPECT_EQ(leases_->State(0), SubscriberLeaseState::kEvicted);
        EXPECT_EQ(broadcast_->Poll(stale.subscriber).status().code(),
                  StatusCode::kNotFound);

        auto replacement = coordinator.Register(
            SubscriberId{0}, ProcessIdentity::Current(),
            kSubscriberT0 + kSubscriberLeaseNs + 1);
        ASSERT_TRUE(replacement.ok()) << replacement.status().ToString();
        EXPECT_GT(replacement->subscriber.generation,
                  stale.subscriber.generation);
        EXPECT_GT(replacement->lease_epoch, stale.lease_epoch);
        EXPECT_EQ(coordinator.Heartbeat(stale,
                                       kSubscriberT0 + kSubscriberLeaseNs + 2)
                      .code(),
                  StatusCode::kNotFound);
        ASSERT_NO_FATAL_FAILURE(PublishBroadcast(1));
        ASSERT_NO_FATAL_FAILURE(DrainBroadcast(replacement->subscriber, 1));
        ASSERT_TRUE(coordinator.Unregister(*replacement).ok());
        VerifyConservation(kBroadcastCapacity + 1, /*acknowledged=*/1,
                           /*recovered=*/kBroadcastCapacity);
    }

    void RunSlowSubscriberScenario(uint32_t yields) {
        SubscriberLeaseCoordinator coordinator(*broadcast_, *leases_);
        auto fast = coordinator.Register(SubscriberId{0},
                                         ProcessIdentity::Current(),
                                         kSubscriberT0);
        auto slow = coordinator.Register(SubscriberId{1},
                                         ProcessIdentity::Current(),
                                         kSubscriberT0);
        ASSERT_TRUE(fast.ok()) << fast.status().ToString();
        ASSERT_TRUE(slow.ok()) << slow.status().ToString();

        ASSERT_NO_FATAL_FAILURE(PublishBroadcast(kBroadcastCapacity));
        ASSERT_TRUE(broadcast_->IsFull());
        ASSERT_NO_FATAL_FAILURE(
            DrainBroadcast(fast->subscriber, kBroadcastCapacity));
        EXPECT_TRUE(broadcast_->IsFull())
            << "the slowest subscriber must continue to apply backpressure";
        EXPECT_EQ(coordinator.EvictExpired(
                      kSubscriberT0 + 100 * kSubscriberLeaseNs,
                      kSubscriberLeaseNs),
                  0u)
            << "a slow but live subscriber must not be evicted by timeout";
        ASSERT_NO_FATAL_FAILURE(
            DrainBroadcast(slow->subscriber, kBroadcastCapacity, yields));
        EXPECT_FALSE(broadcast_->IsFull());
        ASSERT_TRUE(coordinator.Unregister(*fast).ok());
        ASSERT_TRUE(coordinator.Unregister(*slow).ok());
        VerifyConservation(2 * kBroadcastCapacity,
                           /*acknowledged=*/2 * kBroadcastCapacity,
                           /*recovered=*/0);
    }

    void RunLeaseBoundaryScenario(StressDeadline deadline) {
        ResetSubscriberChildState();
        child_pid_ = ::fork();
        ASSERT_NE(child_pid_, -1) << std::strerror(errno);
        if (child_pid_ == 0) {
            SubscriberChild(shared_, /*hold_borrow=*/false, deadline);
        }
        ASSERT_TRUE(WaitForAtomic(&shared_->child_ready, 1, deadline));
        ASSERT_NO_FATAL_FAILURE(PublishBroadcast(kBroadcastCapacity));
        ASSERT_TRUE(broadcast_->IsFull());
        shared_->child_command.store(1, std::memory_order_release);
        KillAndReap();

        SubscriberLeaseCoordinator coordinator(*broadcast_, *leases_);
        EXPECT_EQ(coordinator.EvictExpired(
                      kSubscriberT0 + kSubscriberLeaseNs - 1,
                      kSubscriberLeaseNs),
                  0u);
        EXPECT_EQ(leases_->State(0), SubscriberLeaseState::kActive);
        EXPECT_TRUE(broadcast_->IsFull());
        EXPECT_EQ(OutstandingBroadcastAcks(), kBroadcastCapacity);
        ASSERT_EQ(coordinator.EvictExpired(
                      kSubscriberT0 + kSubscriberLeaseNs,
                      kSubscriberLeaseNs),
                  1u)
            << "a dead lease must become eligible at the exact boundary";
        VerifyConservation(kBroadcastCapacity, /*acknowledged=*/0,
                           /*recovered=*/kBroadcastCapacity);
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
    std::array<CampaignScenario, kCampaignScenarios.size()> coverage_order =
        kCampaignScenarios;
    std::shuffle(coverage_order.begin(), coverage_order.end(), random);
    std::array<uint64_t, kCampaignScenarios.size()> attempted_counts{};
    std::array<uint64_t, kCampaignScenarios.size()> completed_counts{};
    uint64_t iteration = 0;
    // Preserve both the estimated minimum round budget and the cleanup reserve;
    // never start a new child close to the hard deadline. The first five rounds
    // are a seed-shuffled coverage bag; subsequent rounds use weighted random
    // selection. Long campaigns therefore cannot accidentally become
    // publisher-only, while the one-second smoke remains deadline bounded.
    while (scheduling_deadline - StressClock::now() >=
           kEstimatedMinimumRoundBudget) {
        const std::optional<CampaignScenario> forced_campaign =
            iteration < coverage_order.size()
                ? std::optional<CampaignScenario>(coverage_order[iteration])
                : std::nullopt;
        const StressRound round = MakeRandomRound(&random, forced_campaign);
        const char* campaign_name = CampaignScenarioName(round.campaign);
        ++attempted_counts[CampaignScenarioIndex(round.campaign)];
        std::cout << "D2_RECOVERY_SCENARIO_ATTEMPT class=" << campaign_name
                  << std::endl;
        if (round.campaign == CampaignScenario::kPublisherCrash) {
            ReportCutEvent("ATTEMPT", round.scenario,
                           InterruptionMarkerName(round.interruption));
        }

        std::ostringstream trace;
        trace << "seed=" << seed << " iteration=" << iteration
              << " campaign=" << campaign_name;
        switch (round.campaign) {
            case CampaignScenario::kPublisherCrash:
                trace << " scenario=" << ScenarioName(round.scenario)
                      << " signal=" << InterruptionName(round.interruption);
                break;
            case CampaignScenario::kSlowSubscriber:
                trace << " yields=" << round.slow_yields;
                break;
            case CampaignScenario::kPidIncarnation:
                trace << " mutation="
                      << (round.mutate_epoch ? "start-time+epoch"
                                             : "start-time");
                break;
            case CampaignScenario::kSubscriberKill:
            case CampaignScenario::kLeaseBoundary:
            case CampaignScenario::kCount:
                break;
        }
        SCOPED_TRACE(trace.str());

        switch (round.campaign) {
            case CampaignScenario::kPublisherCrash:
                ASSERT_NO_FATAL_FAILURE(
                    RunScenario(round.scenario, hard_deadline,
                                round.interruption));
                break;
            case CampaignScenario::kSubscriberKill:
                ASSERT_NO_FATAL_FAILURE(
                    RunSubscriberKillScenario(hard_deadline));
                break;
            case CampaignScenario::kSlowSubscriber:
                ASSERT_NO_FATAL_FAILURE(
                    RunSlowSubscriberScenario(round.slow_yields));
                break;
            case CampaignScenario::kLeaseBoundary:
                ASSERT_NO_FATAL_FAILURE(
                    RunLeaseBoundaryScenario(hard_deadline));
                break;
            case CampaignScenario::kPidIncarnation:
                ASSERT_NO_FATAL_FAILURE(RunPidIncarnationScenario(
                    round.mutate_epoch, hard_deadline));
                break;
            case CampaignScenario::kCount:
                FAIL() << "invalid campaign scenario";
        }
        ASSERT_NO_FATAL_FAILURE(
            VerifyQueueProgressAndCapacity(iteration, cleanup_deadline));
        ++completed_counts[CampaignScenarioIndex(round.campaign)];
        std::cout << "D2_RECOVERY_SCENARIO_COMPLETED class=" << campaign_name
                  << std::endl;
        if (round.campaign == CampaignScenario::kPublisherCrash) {
            ReportCutEvent("COMPLETED", round.scenario,
                           InterruptionMarkerName(round.interruption));
        }
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
    if (stress_seconds >= 10) {
        EXPECT_GE(iteration, kCampaignScenarios.size())
            << "long campaign did not complete its shuffled coverage bag";
    }
    for (CampaignScenario scenario : kCampaignScenarios) {
        const size_t index = CampaignScenarioIndex(scenario);
        const std::string name = CampaignScenarioName(scenario);
        RecordProperty("scenario_" + name + "_attempted",
                       std::to_string(attempted_counts[index]));
        RecordProperty("scenario_" + name + "_completed",
                       std::to_string(completed_counts[index]));
        std::cout << "D2_RECOVERY_SCENARIO_COUNT class=" << name
                  << " attempted=" << attempted_counts[index]
                  << " completed=" << completed_counts[index] << std::endl;
        if (stress_seconds >= 10) {
            EXPECT_GT(completed_counts[index], 0u)
                << "long campaign omitted scenario class " << name;
        }
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
        ReportCutEvent("ATTEMPT", scenario, "sigkill");
        ASSERT_NO_FATAL_FAILURE(RunScenario(scenario, deadline));
        ASSERT_NO_FATAL_FAILURE(
            VerifyQueueProgressAndCapacity(iteration++, deadline));
        ReportCutEvent("COMPLETED", scenario, "sigkill");
    }
}

TEST_F(D2RecoveryStressTest, SigstopLiveOwnerIsNeverRecovered) {
    const StressDeadline deadline =
        StressClock::now() + kDeterministicWatchdog;
    shared_->reached.store(0, std::memory_order_relaxed);
    shared_->child_command.store(0, std::memory_order_relaxed);
    ReportCutEvent("ATTEMPT", CrashScenario::kMpscWritingPublished,
                   "sigstop_live_sigcont");
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
    ReportCutEvent("COMPLETED", CrashScenario::kMpscWritingPublished,
                   "sigstop_live_sigcont");
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
    ASSERT_NO_FATAL_FAILURE(RunSubscriberKillScenario(deadline));
    ASSERT_NO_FATAL_FAILURE(VerifyQueueProgressAndCapacity(0, deadline));
}

TEST_F(D2RecoveryStressTest, SlowSubscriberBackpressurePreservesConservation) {
    const StressDeadline deadline =
        StressClock::now() + kDeterministicWatchdog;
    ASSERT_NO_FATAL_FAILURE(RunSlowSubscriberScenario(/*yields=*/4));
    ASSERT_NO_FATAL_FAILURE(VerifyQueueProgressAndCapacity(0, deadline));
}

TEST_F(D2RecoveryStressTest, DeadSubscriberLeaseBoundaryIsExact) {
    const StressDeadline deadline =
        StressClock::now() + kDeterministicWatchdog;
    ASSERT_NO_FATAL_FAILURE(RunLeaseBoundaryScenario(deadline));
    ASSERT_NO_FATAL_FAILURE(VerifyQueueProgressAndCapacity(0, deadline));
}

}  // namespace
}  // namespace mino

#else

TEST(D2RecoveryStressTest, RequiresPosixSharedMemoryAndProcessSignals) {
    GTEST_SKIP() << "D2 recovery stress requires POSIX mmap/fork/signals";
}

#endif  // defined(__unix__) || defined(__APPLE__)
