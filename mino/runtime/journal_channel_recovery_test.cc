// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/journal_channel_recovery.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>

namespace mino {
namespace {

struct AlignedDeleter {
    void operator()(std::byte* p) const {
        ::operator delete[](p, std::align_val_t(64));
    }
};
using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(size_t bytes) {
    AlignedBytes memory(new (std::align_val_t(64)) std::byte[bytes]);
    std::memset(memory.get(), 0, bytes);
    return memory;
}

ClassTableConfig AllocatorConfig() {
    ClassTableConfig config;
    config.classes = {{.slot_size = 64, .slot_count = 64}};
    return config;
}

ProcessLiveness AlwaysDead(const ProcessIdentity&, void*) noexcept {
    return ProcessLiveness::kDead;
}

ProcessLiveness AlwaysUnknown(const ProcessIdentity&, void*) noexcept {
    return ProcessLiveness::kUnknown;
}

class JournalChannelRecoveryTest : public ::testing::Test {
protected:
    static constexpr size_t kAllocatorBytes = 1u << 20;
    static constexpr uint32_t kTransactionCapacity = 8;
    static constexpr uint32_t kHandlesPerTransaction = 2;
    static constexpr uint64_t kSpscChannelId = 11;
    static constexpr uint64_t kMpscChannelId = 22;
    static constexpr uint64_t kBroadcastChannelId = 33;

    struct Orphan {
        AllocationTransaction transaction;
        ShmHandle root;
    };

    void SetUp() override {
        allocator_memory_ = AllocateAligned(kAllocatorBytes);
        auto allocator = CentralSlabAllocator::Create(
            allocator_memory_.get(), kAllocatorBytes, AllocatorConfig());
        ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
        allocator_ = *allocator;

        const size_t journal_size = AllocationJournal::RequiredSize(
            kTransactionCapacity, kHandlesPerTransaction);
        journal_memory_ = AllocateAligned(journal_size);
        auto journal = AllocationJournal::Init(
            journal_memory_.get(), journal_size, kTransactionCapacity,
            kHandlesPerTransaction, allocator_);
        ASSERT_TRUE(journal.ok()) << journal.status().ToString();
        journal_.emplace(*journal);

        spsc_memory_ = AllocateAligned(SpscChannel::RequiredSize(4));
        auto spsc = SpscChannel::Init(spsc_memory_.get(), 4);
        ASSERT_TRUE(spsc.ok()) << spsc.status().ToString();
        spsc_.emplace(*spsc);

        mpsc_memory_ = AllocateAligned(MpscChannel::RequiredSize(64));
        auto mpsc = MpscChannel::Init(mpsc_memory_.get(), 64);
        ASSERT_TRUE(mpsc.ok()) << mpsc.status().ToString();
        mpsc_.emplace(*mpsc);

        broadcast_memory_ =
            AllocateAligned(BroadcastChannel::RequiredSize(4));
        auto broadcast = BroadcastChannel::Init(broadcast_memory_.get(), 4);
        ASSERT_TRUE(broadcast.ok()) << broadcast.status().ToString();
        broadcast_.emplace(*broadcast);
        auto broadcast_view =
            BroadcastPublicationView::Attach(broadcast_memory_.get());
        ASSERT_TRUE(broadcast_view.ok())
            << broadcast_view.status().ToString();

        recovery_.emplace(*journal_);
        ASSERT_TRUE(recovery_->RegisterChannel(kSpscChannelId, *spsc_).ok());
        ASSERT_TRUE(recovery_->RegisterChannel(kMpscChannelId, *mpsc_).ok());
        ASSERT_TRUE(recovery_->RegisterChannel(kBroadcastChannelId,
                                               *broadcast_view).ok());
    }

    AllocationRequest Request() const {
        AllocationRequest request;
        request.object_size = 16;
        request.type_id = TypeId{7};
        request.schema = SchemaIdentity{.short_id = 9, .layout_version = 1};
        request.alignment = 8;
        return request;
    }

    Result<Orphan> BeginPublishedOrphan() {
        ProcessIdentity owner = ProcessIdentity::Current();
        owner.process_epoch ^= 0xA5A5u;
        auto transaction = journal_->Begin(owner);
        if (!transaction.ok()) {
            return transaction.status();
        }
        auto root = journal_->AllocateRoot(*transaction, Request());
        if (!root.ok()) {
            return root.status();
        }
        auto build = allocator_.BeginBuild(*root);
        if (!build.ok()) {
            return build.status();
        }
        const Status published = journal_->PublishGraph(*transaction);
        if (!published.ok()) {
            return published;
        }
        return Orphan{.transaction = *transaction, .root = *root};
    }

    void ReclaimFinalized(ShmHandle root) {
        ASSERT_TRUE(allocator_.Retire(root).ok());
        ASSERT_TRUE(allocator_.Reclaim(root).ok());
    }

    AlignedBytes allocator_memory_;
    AlignedBytes journal_memory_;
    AlignedBytes spsc_memory_;
    AlignedBytes mpsc_memory_;
    AlignedBytes broadcast_memory_;
    CentralSlabAllocator allocator_;
    std::optional<AllocationJournal> journal_;
    std::optional<SpscChannel> spsc_;
    std::optional<MpscChannel> mpsc_;
    std::optional<BroadcastChannel> broadcast_;
    std::optional<JournalChannelRecoveryCoordinator> recovery_;
};

TEST_F(JournalChannelRecoveryTest, RejectsDuplicateChannelIdsAcrossTypes) {
    EXPECT_EQ(recovery_->RegisterChannel(kSpscChannelId, *mpsc_).code(),
              StatusCode::kAlreadyExists);
}

TEST_F(JournalChannelRecoveryTest, RejectsZeroChannelId) {
    EXPECT_EQ(recovery_->RegisterChannel(0, *spsc_).code(),
              StatusCode::kInvalidArgument);
}

TEST_F(JournalChannelRecoveryTest, RoutesSameKindChannelsByStableId) {
    constexpr uint64_t kSecondSpscChannelId = 44;
    auto second_memory = AllocateAligned(SpscChannel::RequiredSize(4));
    auto second = SpscChannel::Init(second_memory.get(), 4);
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    ASSERT_TRUE(recovery_->RegisterChannel(kSecondSpscChannelId, *second).ok());

    auto absent = BeginPublishedOrphan();
    auto visible = BeginPublishedOrphan();
    ASSERT_TRUE(absent.ok() && visible.ok());
    ASSERT_TRUE(journal_->Commit(
        absent->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kSpsc,
                           .channel_id = kSpscChannelId,
                           .sequence = 0,
                           .payload = absent->root}).ok());

    auto reservation = second->Reserve();
    ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
    reservation->slot()->payload = visible->root;
    const uint64_t sequence =
        reservation->slot()->sequence_num.load(std::memory_order_relaxed);
    ASSERT_TRUE(journal_->Commit(
        visible->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kSpsc,
                           .channel_id = kSecondSpscChannelId,
                           .sequence = sequence,
                           .payload = visible->root}).ok());
    ASSERT_TRUE(std::move(*reservation).Commit().ok());

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 2u);
    EXPECT_EQ(allocator_.Inspect(absent->root).status().code(),
              StatusCode::kNotFound);
    EXPECT_TRUE(allocator_.Inspect(visible->root).ok());
    ReclaimFinalized(visible->root);
}

TEST_F(JournalChannelRecoveryTest, VisibleSpscPublicationIsFinalized) {
    auto orphan = BeginPublishedOrphan();
    ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
    auto reservation = spsc_->Reserve();
    ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
    reservation->slot()->payload = orphan->root;
    const uint64_t sequence =
        reservation->slot()->sequence_num.load(std::memory_order_relaxed);
    ASSERT_TRUE(journal_->Commit(
        orphan->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kSpsc,
                           .channel_id = kSpscChannelId,
                           .sequence = sequence,
                           .payload = orphan->root}).ok());
    ASSERT_TRUE(std::move(*reservation).Commit().ok());

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 1u);
    EXPECT_EQ(*journal_->State(orphan->transaction),
              AllocationJournalState::kFree);
    EXPECT_TRUE(allocator_.Inspect(orphan->root).ok());
    ReclaimFinalized(orphan->root);
}

TEST_F(JournalChannelRecoveryTest, VisibleMpscPublicationIsFinalized) {
    auto orphan = BeginPublishedOrphan();
    ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
    MpscChannel::ProducerIdentity producer{
        .owner = ProcessIdentity::Current(), .publisher_id = 7};
    auto reservation = mpsc_->Reserve(producer);
    ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
    reservation->slot()->payload = orphan->root;
    const uint64_t sequence = reservation->sequence();
    ASSERT_TRUE(journal_->Commit(
        orphan->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kMpsc,
                           .channel_id = kMpscChannelId,
                           .sequence = sequence,
                           .payload = orphan->root}).ok());
    ASSERT_TRUE(std::move(*reservation).Commit().ok());

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 1u);
    EXPECT_EQ(*journal_->State(orphan->transaction),
              AllocationJournalState::kFree);
    EXPECT_TRUE(allocator_.Inspect(orphan->root).ok());
    ReclaimFinalized(orphan->root);
}

TEST_F(JournalChannelRecoveryTest,
       VisibleRetiredBroadcastPublicationIsFinalized) {
    auto orphan = BeginPublishedOrphan();
    ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
    auto reservation = broadcast_->Reserve();
    ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
    reservation->slot()->payload = orphan->root;
    const uint64_t sequence = reservation->sequence();
    ASSERT_TRUE(journal_->Commit(
        orphan->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kBroadcast,
                           .channel_id = kBroadcastChannelId,
                           .sequence = sequence,
                           .payload = orphan->root}).ok());
    ASSERT_TRUE(std::move(*reservation).Commit().ok());
    broadcast_->CollectGarbage();

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 1u);
    EXPECT_EQ(*journal_->State(orphan->transaction),
              AllocationJournalState::kFree);
    EXPECT_TRUE(allocator_.Inspect(orphan->root).ok());
    ReclaimFinalized(orphan->root);
}

TEST_F(JournalChannelRecoveryTest,
       DefinitelyNotVisiblePublicationsAreRolledBackForEveryChannelKind) {
    const PublicationBinding bindings[] = {
        {.channel_kind = PublicationChannelKind::kSpsc,
         .channel_id = kSpscChannelId,
         .sequence = 0,
         .payload = {}},
        {.channel_kind = PublicationChannelKind::kMpsc,
         .channel_id = kMpscChannelId,
         .sequence = 0,
         .payload = {}},
        {.channel_kind = PublicationChannelKind::kBroadcast,
         .channel_id = kBroadcastChannelId,
         .sequence = 0,
         .payload = {}},
    };
    for (PublicationBinding binding : bindings) {
        auto orphan = BeginPublishedOrphan();
        ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
        binding.payload = orphan->root;
        ASSERT_TRUE(journal_->Commit(orphan->transaction, binding).ok());

        EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 1u);
        EXPECT_EQ(*journal_->State(orphan->transaction),
                  AllocationJournalState::kFree);
        EXPECT_EQ(allocator_.Inspect(orphan->root).status().code(),
                  StatusCode::kNotFound);
    }
}

TEST_F(JournalChannelRecoveryTest, UnknownChannelAndTypeMismatchAreDeferred) {
    auto unknown = BeginPublishedOrphan();
    auto mismatch = BeginPublishedOrphan();
    ASSERT_TRUE(unknown.ok() && mismatch.ok());
    ASSERT_TRUE(journal_->Commit(
        unknown->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kSpsc,
                           .channel_id = 999,
                           .sequence = 0,
                           .payload = unknown->root}).ok());
    ASSERT_TRUE(journal_->Commit(
        mismatch->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kBroadcast,
                           .channel_id = kSpscChannelId,
                           .sequence = 0,
                           .payload = mismatch->root}).ok());

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 0u);
    EXPECT_EQ(*journal_->State(unknown->transaction),
              AllocationJournalState::kCommitted);
    EXPECT_EQ(*journal_->State(mismatch->transaction),
              AllocationJournalState::kCommitted);
    EXPECT_TRUE(journal_->RollbackCommitted(unknown->transaction).ok());
    EXPECT_TRUE(journal_->RollbackCommitted(mismatch->transaction).ok());
}

TEST_F(JournalChannelRecoveryTest,
       ConsumedAbortedSpscPublicationIsDeferred) {
    auto orphan = BeginPublishedOrphan();
    ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
    auto reservation = spsc_->Reserve();
    ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
    reservation->slot()->payload = orphan->root;
    const uint64_t sequence =
        reservation->slot()->sequence_num.load(std::memory_order_relaxed);
    ASSERT_TRUE(journal_->Commit(
        orphan->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kSpsc,
                           .channel_id = kSpscChannelId,
                           .sequence = sequence,
                           .payload = orphan->root}).ok());
    ASSERT_TRUE(std::move(*reservation).Abort().ok());
    EXPECT_EQ(spsc_->Poll().status().code(), StatusCode::kWouldBlock);

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 0u);
    EXPECT_EQ(*journal_->State(orphan->transaction),
              AllocationJournalState::kCommitted);
    EXPECT_TRUE(journal_->RollbackCommitted(orphan->transaction).ok());
}

TEST_F(JournalChannelRecoveryTest,
       ConsumedAbortedMpscPublicationIsDeferred) {
    auto orphan = BeginPublishedOrphan();
    ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
    MpscChannel::ProducerIdentity producer{
        .owner = ProcessIdentity::Current(), .publisher_id = 8};
    auto reservation = mpsc_->Reserve(producer);
    ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
    reservation->slot()->payload = orphan->root;
    ASSERT_TRUE(journal_->Commit(
        orphan->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kMpsc,
                           .channel_id = kMpscChannelId,
                           .sequence = reservation->sequence(),
                           .payload = orphan->root}).ok());
    ASSERT_TRUE(std::move(*reservation).Abort().ok());
    EXPECT_EQ(mpsc_->Poll().status().code(), StatusCode::kWouldBlock);

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 0u);
    EXPECT_EQ(*journal_->State(orphan->transaction),
              AllocationJournalState::kCommitted);
    EXPECT_TRUE(journal_->RollbackCommitted(orphan->transaction).ok());
}

TEST_F(JournalChannelRecoveryTest, IndeterminatePublicationIsDeferred) {
    auto orphan = BeginPublishedOrphan();
    ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
    MpscChannel::ProducerIdentity producer{
        .owner = ProcessIdentity::Current(), .publisher_id = 9};
    auto reservation = mpsc_->Reserve(producer);
    ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
    ShmHandle different_payload = orphan->root;
    ++different_payload.generation;
    reservation->slot()->payload = different_payload;
    ASSERT_TRUE(journal_->Commit(
        orphan->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kMpsc,
                           .channel_id = kMpscChannelId,
                           .sequence = reservation->sequence(),
                           .payload = orphan->root}).ok());
    ASSERT_TRUE(std::move(*reservation).Commit().ok());

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 0u);
    EXPECT_EQ(*journal_->State(orphan->transaction),
              AllocationJournalState::kCommitted);
    EXPECT_TRUE(journal_->RollbackCommitted(orphan->transaction).ok());
}

TEST_F(JournalChannelRecoveryTest, UnknownOwnerLivenessPreventsRollback) {
    auto orphan = BeginPublishedOrphan();
    ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
    ASSERT_TRUE(journal_->Commit(
        orphan->transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kSpsc,
                           .channel_id = kSpscChannelId,
                           .sequence = 0,
                           .payload = orphan->root}).ok());

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysUnknown), 0u);
    EXPECT_EQ(*journal_->State(orphan->transaction),
              AllocationJournalState::kCommitted);
    EXPECT_TRUE(allocator_.Inspect(orphan->root).ok());

    EXPECT_EQ(recovery_->RecoverOrphans(&AlwaysDead), 1u);
    EXPECT_EQ(allocator_.Inspect(orphan->root).status().code(),
              StatusCode::kNotFound);
}

TEST(BroadcastPublicationViewTest, RejectsUninitializedSharedMemory) {
    auto memory = AllocateAligned(BroadcastChannel::RequiredSize(4));
    auto view = BroadcastPublicationView::Attach(memory.get());
    ASSERT_FALSE(view.ok());
    EXPECT_EQ(view.status().code(), StatusCode::kCorruption);
}

}  // namespace
}  // namespace mino
