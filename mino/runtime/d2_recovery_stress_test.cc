// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/publisher.h"
#include "mino/runtime/subscriber.h"
#include "mino/runtime/subscriber_lease.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <optional>
#include <thread>

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
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

struct HookContext {
    SharedBlock* shared = nullptr;
    CrashScenario target = CrashScenario::kJournalAllocationPublished;
    bool stop_instead_of_pause = false;
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
    if (context->stop_instead_of_pause) {
        ::raise(SIGSTOP);
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
    if (match) {
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
    if (match) {
        ReachHook(context);
    }
}

[[noreturn]] void PublisherChild(SharedBlock* shared,
                                 CrashScenario scenario,
                                 bool stop_instead_of_pause = false) {
    auto allocator = CentralSlabAllocator::Attach(shared->allocator_storage);
    auto journal = AllocationJournal::Attach(
        shared->journal_storage, kJournalBytes, *allocator);
    auto channel = MpscChannel::Attach(shared->mpsc_storage);
    if (!allocator.ok() || !journal.ok() || !channel.ok()) {
        _exit(10);
    }

    HookContext hooks{.shared = shared,
                      .target = scenario,
                      .stop_instead_of_pause = stop_instead_of_pause};
    journal->SetPersistenceHook(&JournalHook, &hooks);
    channel->SetPersistenceHook(&MpscHook, &hooks);

    const ProcessIdentity owner = ProcessIdentity::Current();
    MpscChannel::ProducerIdentity producer{
        .owner = owner,
        .publisher_id = static_cast<uint64_t>(scenario),
    };
    Publisher<D2RecoveryStressMessage> publisher(
        *allocator, *channel, producer, *journal);
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

AllocationJournal::CommittedOrphanAction ResolveFromMpsc(
    const AllocationTransaction&, const PublicationBinding& binding,
    void* opaque) noexcept {
    auto* channel = static_cast<MpscChannel*>(opaque);
    if (binding.channel_kind != PublicationChannelKind::kMpsc ||
        binding.payload.IsNull()) {
        return AllocationJournal::CommittedOrphanAction::kDefer;
    }
    switch (channel->InspectPublication(binding.sequence, binding.payload)) {
        case MpscChannel::PublicationVisibility::kVisible:
            return AllocationJournal::CommittedOrphanAction::kFinalize;
        case MpscChannel::PublicationVisibility::kNotVisible:
            return AllocationJournal::CommittedOrphanAction::kRollback;
        case MpscChannel::PublicationVisibility::kIndeterminate:
            return AllocationJournal::CommittedOrphanAction::kDefer;
    }
    return AllocationJournal::CommittedOrphanAction::kDefer;
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
        auto broadcast = BroadcastChannel::Init(shared_->broadcast_storage,
                                                 kBroadcastCapacity);
        ASSERT_TRUE(broadcast.ok());
        broadcast_.emplace(*broadcast);
        auto leases = SubscriberLeaseTable::Init(shared_->lease_storage);
        ASSERT_TRUE(leases.ok());
        leases_.emplace(*leases);
    }

    void TearDown() override {
        if (child_pid_ > 0) {
            ::kill(child_pid_, SIGKILL);
            int status = 0;
            ::waitpid(child_pid_, &status, 0);
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

    bool WaitReached(uint32_t value) {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            if (shared_->reached.load(std::memory_order_acquire) == value) {
                return true;
            }
            int status = 0;
            if (child_pid_ > 0 &&
                ::waitpid(child_pid_, &status, WNOHANG) == child_pid_) {
                child_pid_ = -1;
                return false;
            }
            std::this_thread::yield();
        }
        return false;
    }

    void KillAndReap() {
        ASSERT_GT(child_pid_, 0);
        ASSERT_EQ(::kill(child_pid_, SIGKILL), 0);
        int status = 0;
        ASSERT_EQ(::waitpid(child_pid_, &status, 0), child_pid_);
        child_pid_ = -1;
        ASSERT_TRUE(WIFSIGNALED(status));
    }

    bool ExpectedVisible(CrashScenario scenario) const {
        return scenario == CrashScenario::kMpscReadyPublished ||
               scenario == CrashScenario::kMpscTurnPublished ||
               scenario == CrashScenario::kJournalFinalizingTagged;
    }

    void RunScenario(CrashScenario scenario) {
        ASSERT_EQ(LiveSlabs(), 0u);
        ASSERT_EQ(journal_->ActiveTransactionCount(), 0u);
        ASSERT_TRUE(mpsc_->IsEmpty());
        shared_->root = {};
        shared_->child = {};
        shared_->reached.store(0, std::memory_order_relaxed);

        child_pid_ = ::fork();
        ASSERT_NE(child_pid_, -1) << std::strerror(errno);
        if (child_pid_ == 0) {
            PublisherChild(shared_, scenario);
        }
        ASSERT_TRUE(WaitReached(static_cast<uint32_t>(scenario)));
        KillAndReap();

        (void)mpsc_->AbortOrphanedReservations(NowNs() + 1000,
                                               /*lease_ns=*/1);
        (void)journal_->RecoverOrphans(nullptr, nullptr, &ResolveFromMpsc,
                                      &*mpsc_);
        // A second pass verifies idempotency and lets kReclaiming/kFinalizing
        // work that was exposed by the first pass reach FREE.
        (void)mpsc_->AbortOrphanedReservations(NowNs() + 1000,
                                               /*lease_ns=*/1);
        (void)journal_->RecoverOrphans(nullptr, nullptr, &ResolveFromMpsc,
                                      &*mpsc_);
        EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);

        Subscriber<D2RecoveryStressMessage> subscriber(allocator_, *mpsc_);
        auto message = subscriber.TryPoll();
        if (ExpectedVisible(scenario)) {
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

    SharedBlock* shared_ = nullptr;
    CentralSlabAllocator allocator_;
    std::optional<AllocationJournal> journal_;
    std::optional<MpscChannel> mpsc_;
    std::optional<BroadcastChannel> broadcast_;
    std::optional<SubscriberLeaseTable> leases_;
    pid_t child_pid_ = -1;
};

TEST_F(D2RecoveryStressTest, SigkillAtEveryPersistentStoreRecovers) {
    for (CrashScenario scenario : kScenarios) {
        SCOPED_TRACE(static_cast<uint32_t>(scenario));
        RunScenario(scenario);
    }
}

TEST_F(D2RecoveryStressTest, SigstopLiveOwnerIsNeverRecovered) {
    shared_->reached.store(0, std::memory_order_relaxed);
    child_pid_ = ::fork();
    ASSERT_NE(child_pid_, -1);
    if (child_pid_ == 0) {
        PublisherChild(shared_, CrashScenario::kMpscWritingPublished,
                       /*stop_instead_of_pause=*/true);
    }
    ASSERT_TRUE(WaitReached(
        static_cast<uint32_t>(CrashScenario::kMpscWritingPublished)));
    int status = 0;
    ASSERT_EQ(::waitpid(child_pid_, &status, WUNTRACED), child_pid_);
    ASSERT_TRUE(WIFSTOPPED(status));

    EXPECT_EQ(mpsc_->AbortOrphanedReservations(NowNs() + 1000, 1), 0u);
    EXPECT_EQ(journal_->RecoverOrphans(nullptr, nullptr, &ResolveFromMpsc,
                                      &*mpsc_), 0u);
    EXPECT_GT(LiveSlabs(), 0u);

    ASSERT_EQ(::kill(child_pid_, SIGCONT), 0);
    ASSERT_EQ(::waitpid(child_pid_, &status, 0), child_pid_);
    child_pid_ = -1;
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);

    Subscriber<D2RecoveryStressMessage> subscriber(allocator_, *mpsc_);
    auto message = subscriber.TryPoll();
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    EXPECT_TRUE(std::move(*message).Ack().ok());
    EXPECT_EQ(LiveSlabs(), 0u);
}

TEST_F(D2RecoveryStressTest, ForeignLivePidIncarnationMismatchIsUnknown) {
    shared_->child_ready.store(0, std::memory_order_relaxed);
    shared_->child_command.store(0, std::memory_order_relaxed);
    child_pid_ = ::fork();
    ASSERT_NE(child_pid_, -1);
    if (child_pid_ == 0) {
        shared_->child_identity = ProcessIdentity::Current();
        shared_->child_ready.store(1, std::memory_order_release);
        while (shared_->child_command.load(std::memory_order_acquire) == 0) {
            std::this_thread::yield();
        }
        _exit(0);
    }
    while (shared_->child_ready.load(std::memory_order_acquire) == 0) {
        std::this_thread::yield();
    }

    ProcessIdentity old_incarnation = shared_->child_identity;
    old_incarnation.start_time_ns ^= 0x100000001ULL;
    old_incarnation.process_epoch ^= 0x200000001ULL;
    auto transaction = journal_->Begin(old_incarnation);
    ASSERT_TRUE(transaction.ok());
    auto root = journal_->AllocateRoot(*transaction, [] {
        AllocationRequest request;
        request.object_size = sizeof(D2RecoveryStressMessage);
        request.type_id = StaticMessageTraits<D2RecoveryStressMessage>::type_id;
        request.schema = SchemaIdentity{
            .short_id = StaticMessageTraits<D2RecoveryStressMessage>::schema_short_id,
            .layout_version = 1};
        request.alignment = alignof(D2RecoveryStressMessage);
        return request;
    }());
    ASSERT_TRUE(root.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*root).ok());

    EXPECT_EQ(journal_->RecoverOrphans(), 0u)
        << "a live foreign PID with unverifiable incarnation is Unknown";
    EXPECT_TRUE(allocator_.Inspect(*root).ok());

    ASSERT_EQ(::kill(child_pid_, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child_pid_, &status, 0), child_pid_);
    child_pid_ = -1;
    EXPECT_EQ(journal_->RecoverOrphans(), 1u);
    EXPECT_EQ(LiveSlabs(), 0u);
}

TEST_F(D2RecoveryStressTest, DeadBroadcastSubscriberLeaseClearsRealAcks) {
    child_pid_ = ::fork();
    ASSERT_NE(child_pid_, -1);
    if (child_pid_ == 0) {
        shared_->child_identity = ProcessIdentity::Current();
        shared_->child_ready.store(1, std::memory_order_release);
        for (;;) {
            ::pause();
        }
    }
    while (shared_->child_ready.load(std::memory_order_acquire) == 0) {
        std::this_thread::yield();
    }

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
