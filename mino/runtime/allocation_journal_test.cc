// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/allocation_journal.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

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
    config.classes = {{.slot_size = 64, .slot_count = 32}};
    return config;
}

ProcessLiveness AlwaysDead(const ProcessIdentity&, void*) noexcept {
    return ProcessLiveness::kDead;
}
ProcessLiveness AlwaysUnknown(const ProcessIdentity&, void*) noexcept {
    return ProcessLiveness::kUnknown;
}

struct BlockingResolverContext {
    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
};

struct ReverseReclaimContext {
    CentralSlabAllocator* allocator = nullptr;
    std::array<ShmHandle, 3> manifest = {};
    uint32_t progress = 0;
    bool order_correct = true;
};

void ObserveReverseReclaim(AllocationJournal::PersistencePoint point, uint64_t,
                           void* opaque) noexcept {
    if (point != AllocationJournal::PersistencePoint::kReclaimProgress) {
        return;
    }
    auto* context = static_cast<ReverseReclaimContext*>(opaque);
    ++context->progress;
    for (size_t i = 0; i < context->manifest.size(); ++i) {
        const bool reclaimed =
            !context->allocator->Inspect(context->manifest[i]).ok();
        const bool expected_reclaimed =
            i >= context->manifest.size() - context->progress;
        if (reclaimed != expected_reclaimed) {
            context->order_correct = false;
        }
    }
}

AllocationJournal::CommittedOrphanAction BlockingRollback(
    const AllocationTransaction&, const PublicationBinding&,
    void* opaque) noexcept {
    auto* context = static_cast<BlockingResolverContext*>(opaque);
    context->entered.store(true, std::memory_order_release);
    while (!context->release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    return AllocationJournal::CommittedOrphanAction::kRollback;
}

AllocationJournal::CommittedOrphanAction ImmediateRollback(
    const AllocationTransaction&, const PublicationBinding&,
    void*) noexcept {
    return AllocationJournal::CommittedOrphanAction::kRollback;
}

class AllocationJournalTest : public ::testing::Test {
protected:
    static constexpr size_t kAllocatorBytes = 1u << 20;
    static constexpr uint32_t kTransactionCapacity = 1;
    static constexpr uint32_t kHandlesPerTransaction = 4;

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
    }

    AllocationRequest Request(uint64_t type = 7) const {
        AllocationRequest request;
        request.object_size = 16;
        request.type_id = TypeId{static_cast<uint32_t>(type)};
        request.schema = SchemaIdentity{.short_id = 9, .layout_version = 1};
        request.alignment = 8;
        return request;
    }

    Result<ShmHandle> AllocateRoot(const AllocationTransaction& transaction) {
        auto handle = journal_->AllocateRoot(transaction, Request());
        if (!handle.ok()) {
            return handle.status();
        }
        auto build = allocator_.BeginBuild(*handle);
        return build.ok() ? handle : Result<ShmHandle>(build.status());
    }

    Result<ShmHandle> AllocateChild(const AllocationTransaction& transaction,
                                    uint64_t type = 8) {
        auto handle = journal_->AllocateChild(transaction, Request(type));
        if (!handle.ok()) {
            return handle.status();
        }
        auto build = allocator_.BeginBuild(*handle);
        return build.ok() ? handle : Result<ShmHandle>(build.status());
    }

    AlignedBytes allocator_memory_;
    AlignedBytes journal_memory_;
    CentralSlabAllocator allocator_;
    std::optional<AllocationJournal> journal_;
};

TEST(AllocationJournalLayoutTest, RequiredSizeStoresOnlyOverflowInSidecar) {
    constexpr size_t kControlSize = 64;
    constexpr size_t kRecordSize = 128;
    constexpr size_t kHandleSize = 16;

    EXPECT_EQ(AllocationJournal::RequiredSize(1, 1),
              kControlSize + kRecordSize);
    EXPECT_EQ(AllocationJournal::RequiredSize(1, 2),
              kControlSize + kRecordSize);
    EXPECT_EQ(AllocationJournal::RequiredSize(1, 3),
              kControlSize + kRecordSize + kHandleSize);
    EXPECT_EQ(AllocationJournal::RequiredSize(4, 4),
              kControlSize + 4 * kRecordSize + 8 * kHandleSize);
    EXPECT_EQ(AllocationJournal::RequiredSize(0, 2), 0u);
    EXPECT_EQ(AllocationJournal::RequiredSize(1, 0), 0u);
    EXPECT_EQ(AllocationJournal::RequiredSize(
                  std::numeric_limits<uint32_t>::max(),
                  std::numeric_limits<uint32_t>::max()),
              0u);
}

TEST_F(AllocationJournalTest, InlineOnlyLayoutNeedsNoSidecar) {
    constexpr uint32_t kHandles = AllocationJournal::kInlineHandleCapacity;
    const size_t required = AllocationJournal::RequiredSize(1, kHandles);
    auto memory = AllocateAligned(required + 64);
    std::memset(memory.get() + required, 0xA5, 64);
    auto journal = AllocationJournal::Init(memory.get(), required, 1, kHandles,
                                           allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();

    auto transaction = journal->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = journal->AllocateRoot(*transaction, Request());
    ASSERT_TRUE(root.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*root).ok());
    auto child = journal->AllocateChild(*transaction, Request(8));
    ASSERT_TRUE(child.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*child).ok());

    ASSERT_TRUE(journal->PublishGraph(*transaction).ok());
    ASSERT_TRUE(journal->Abort(*transaction).ok());
    for (size_t i = required; i < required + 64; ++i) {
        EXPECT_EQ(memory[i], std::byte{0xA5});
    }
}

TEST_F(AllocationJournalTest, OverflowLayoutUsesSidecarAfterTwoInlineHandles) {
    constexpr uint32_t kHandles =
        AllocationJournal::kInlineHandleCapacity + 1;
    const size_t required = AllocationJournal::RequiredSize(1, kHandles);
    auto memory = AllocateAligned(required + 64);
    std::memset(memory.get() + required, 0xA5, 64);
    auto journal = AllocationJournal::Init(memory.get(), required, 1, kHandles,
                                           allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();
    auto attached = AllocationJournal::Attach(memory.get(), required, allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();

    auto transaction = journal->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = journal->AllocateRoot(*transaction, Request());
    ASSERT_TRUE(root.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*root).ok());
    auto inline_child = journal->AllocateChild(*transaction, Request(8));
    ASSERT_TRUE(inline_child.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*inline_child).ok());
    auto overflow_child = journal->AllocateChild(*transaction, Request(9));
    ASSERT_TRUE(overflow_child.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*overflow_child).ok());

    const std::array<ShmHandle, 3> graph = {
        *root, *inline_child, *overflow_child};
    EXPECT_TRUE(attached->ValidateManifest(*transaction, graph).ok());
    ASSERT_TRUE(attached->Abort(*transaction).ok());
    for (size_t i = required; i < required + 64; ++i) {
        EXPECT_EQ(memory[i], std::byte{0xA5});
    }
}

TEST_F(AllocationJournalTest, JournalFirstAbortReclaimsRootAndChildren) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    auto root = AllocateRoot(*transaction);
    auto first = AllocateChild(*transaction, 8);
    auto second = AllocateChild(*transaction, 9);
    ASSERT_TRUE(root.ok() && first.ok() && second.ok());

    ASSERT_TRUE(journal_->Abort(*transaction).ok());
    EXPECT_EQ(allocator_.Inspect(*second).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*first).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
}

TEST_F(AllocationJournalTest, SuccessfulGraphCommitReclaimsChildrenWithRoot) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    auto child = AllocateChild(*transaction);
    ASSERT_TRUE(root.ok() && child.ok());

    ASSERT_TRUE(journal_->PublishGraph(*transaction).ok());
    EXPECT_EQ(allocator_.Inspect(*root)->state, ObjectState::kPublished);
    EXPECT_EQ(allocator_.Inspect(*child)->state, ObjectState::kPublished);
    ASSERT_TRUE(journal_->Commit(
        *transaction,
        PublicationBinding{.channel_kind = PublicationChannelKind::kSpsc,
                           .sequence = 11,
                           .payload = *root}).ok());
    ASSERT_TRUE(journal_->FinalizeCommit(*transaction).ok());
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);

    ASSERT_TRUE(allocator_.Retire(*root).ok());
    ASSERT_TRUE(allocator_.Reclaim(*root).ok());
    EXPECT_EQ(allocator_.Inspect(*root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*child).status().code(), StatusCode::kNotFound);
}

TEST_F(AllocationJournalTest, StaleEpochCannotMutateReusedRecord) {
    auto old_transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(old_transaction.ok());
    auto old_root = AllocateRoot(*old_transaction);
    ASSERT_TRUE(old_root.ok());
    ASSERT_TRUE(journal_->PublishGraph(*old_transaction).ok());
    ASSERT_TRUE(journal_->Commit(*old_transaction).ok());
    ASSERT_TRUE(journal_->RollbackCommitted(*old_transaction).ok());

    auto new_transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(new_transaction.ok());
    auto new_root = AllocateRoot(*new_transaction);
    ASSERT_TRUE(new_root.ok());
    EXPECT_EQ(new_transaction->journal_index, old_transaction->journal_index);
    EXPECT_NE(new_transaction->transaction_epoch,
              old_transaction->transaction_epoch);

    EXPECT_EQ(journal_->Abort(*old_transaction).code(), StatusCode::kNotFound);
    auto state = journal_->State(*new_transaction);
    ASSERT_TRUE(state.ok());
    EXPECT_EQ(*state, AllocationJournalState::kBuilding);
    EXPECT_TRUE(allocator_.Inspect(*new_root).ok());
    EXPECT_TRUE(journal_->Abort(*new_transaction).ok());
}

TEST_F(AllocationJournalTest, ConcurrentScannerCannotClaimReusedEpoch) {
    ProcessIdentity dead_owner = ProcessIdentity::Current();
    dead_owner.process_epoch ^= 0x55AAu;
    auto old_transaction = journal_->Begin(dead_owner);
    ASSERT_TRUE(old_transaction.ok());
    auto old_root = AllocateRoot(*old_transaction);
    ASSERT_TRUE(old_root.ok());
    ASSERT_TRUE(journal_->PublishGraph(*old_transaction).ok());
    ASSERT_TRUE(journal_->Commit(*old_transaction).ok());

    BlockingResolverContext blocked;
    uint32_t first_recovered = 99;
    std::thread first_scanner([&]() {
        first_recovered = journal_->RecoverOrphans(
            &AlwaysDead, nullptr, &BlockingRollback, &blocked);
    });
    while (!blocked.entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    EXPECT_EQ(journal_->RecoverOrphans(
                  &AlwaysDead, nullptr, &ImmediateRollback, nullptr),
              1u);
    auto new_transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(new_transaction.ok());
    auto new_root = AllocateRoot(*new_transaction);
    ASSERT_TRUE(new_root.ok());
    ASSERT_TRUE(journal_->PublishGraph(*new_transaction).ok());
    ASSERT_TRUE(journal_->Commit(*new_transaction).ok());

    blocked.release.store(true, std::memory_order_release);
    first_scanner.join();
    EXPECT_EQ(first_recovered, 0u);
    EXPECT_EQ(*journal_->State(*new_transaction),
              AllocationJournalState::kCommitted);
    EXPECT_TRUE(allocator_.Inspect(*new_root).ok());
    EXPECT_TRUE(journal_->RollbackCommitted(*new_transaction).ok());
}

TEST_F(AllocationJournalTest, UnknownLivenessNeverReclaims) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    ASSERT_TRUE(root.ok());

    EXPECT_EQ(journal_->RecoverOrphans(&AlwaysUnknown), 0u);
    EXPECT_TRUE(allocator_.Inspect(*root).ok());
    EXPECT_EQ(*journal_->State(*transaction), AllocationJournalState::kBuilding);
    EXPECT_TRUE(journal_->Abort(*transaction).ok());
}

TEST_F(AllocationJournalTest, DeadBuildingOwnerIsRecoveredByAllocatorStamp) {
    ProcessIdentity owner = ProcessIdentity::Current();
    owner.process_epoch ^= 0x1234u;
    auto transaction = journal_->Begin(owner);
    ASSERT_TRUE(transaction.ok());

    // Simulate death after allocator publication but before handle append: the
    // slab carries the transaction stamp even though the journal count stays 0.
    AllocationRequest stamped = Request();
    stamped.owner_epoch = owner.process_epoch;
    stamped.allocation_transaction_id = transaction->transaction_epoch;
    stamped.allocation_flags = kAllocationFlagTransactionRoot;
    auto root = allocator_.Allocate(stamped);
    ASSERT_TRUE(root.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*root).ok());

    EXPECT_EQ(journal_->RecoverOrphans(&AlwaysDead), 1u);
    EXPECT_EQ(allocator_.Inspect(*root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
}

TEST_F(AllocationJournalTest, InterruptedReclaimIsRetriedFromPersistentState) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    auto child = AllocateChild(*transaction);
    ASSERT_TRUE(root.ok() && child.ok());

    auto child_view = allocator_.Inspect(*child);
    ASSERT_TRUE(child_view.ok());
    auto* child_header = reinterpret_cast<SlabHeader*>(
        static_cast<std::byte*>(const_cast<void*>(child_view->data)) -
        sizeof(SlabHeader));
    const uint32_t original_crc = child_header->immutable_header_crc;
    child_header->immutable_header_crc ^= 1u;

    const Status first = journal_->Abort(*transaction);
    EXPECT_EQ(first.code(), StatusCode::kCorruption);
    EXPECT_EQ(*journal_->State(*transaction),
              AllocationJournalState::kReclaiming);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 1u);

    child_header->immutable_header_crc = original_crc;
    EXPECT_EQ(journal_->RecoverOrphans(&AlwaysUnknown), 1u)
        << "kReclaiming resumes independently of owner liveness";
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
    EXPECT_EQ(allocator_.Inspect(*root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*child).status().code(), StatusCode::kNotFound);
}

TEST_F(AllocationJournalTest,
       AttachedInstanceRecoversBuildingAcrossInlineOverflowBoundary) {
    constexpr uint32_t kHandles =
        AllocationJournal::kInlineHandleCapacity + 1;
    const size_t journal_size = AllocationJournal::RequiredSize(1, kHandles);
    auto memory = AllocateAligned(journal_size);
    std::optional<AllocationJournal> original;
    {
        auto initialized = AllocationJournal::Init(
            memory.get(), journal_size, 1, kHandles, allocator_);
        ASSERT_TRUE(initialized.ok()) << initialized.status().ToString();
        original.emplace(*initialized);
    }

    ProcessIdentity dead_owner = ProcessIdentity::Current();
    dead_owner.process_epoch ^= 0xB017D1u;
    auto transaction = original->Begin(dead_owner);
    ASSERT_TRUE(transaction.ok());
    auto root = original->AllocateRoot(*transaction, Request());
    ASSERT_TRUE(root.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*root).ok());
    auto inline_child = original->AllocateChild(*transaction, Request(8));
    ASSERT_TRUE(inline_child.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*inline_child).ok());
    auto overflow_child = original->AllocateChild(*transaction, Request(9));
    ASSERT_TRUE(overflow_child.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*overflow_child).ok());

    original.reset();
    auto attached =
        AllocationJournal::Attach(memory.get(), journal_size, allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    ASSERT_EQ(*attached->State(*transaction), AllocationJournalState::kBuilding);
    EXPECT_EQ(attached->RecoverOrphans(&AlwaysDead), 1u);
    EXPECT_EQ(allocator_.Inspect(*overflow_child).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*inline_child).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(attached->ActiveTransactionCount(), 0u);

    auto reused = attached->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(reused.ok());
    EXPECT_EQ(reused->journal_index, transaction->journal_index);
    EXPECT_NE(reused->transaction_epoch, transaction->transaction_epoch);
    EXPECT_EQ(attached->State(*transaction).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(attached->Abort(*transaction).code(), StatusCode::kNotFound);
    EXPECT_EQ(*attached->State(*reused), AllocationJournalState::kBuilding);
    auto reused_root = attached->AllocateRoot(*reused, Request(10));
    ASSERT_TRUE(reused_root.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*reused_root).ok());
    EXPECT_TRUE(attached->Abort(*reused).ok());
}

TEST_F(AllocationJournalTest,
       AttachedInstanceResumesReclaimingAcrossOverflowInlineBoundary) {
    constexpr uint32_t kHandles =
        AllocationJournal::kInlineHandleCapacity + 1;
    const size_t journal_size = AllocationJournal::RequiredSize(1, kHandles);
    auto memory = AllocateAligned(journal_size);
    std::optional<AllocationJournal> original;
    {
        auto initialized = AllocationJournal::Init(
            memory.get(), journal_size, 1, kHandles, allocator_);
        ASSERT_TRUE(initialized.ok()) << initialized.status().ToString();
        original.emplace(*initialized);
    }

    auto transaction = original->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = original->AllocateRoot(*transaction, Request());
    ASSERT_TRUE(root.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*root).ok());
    auto inline_child = original->AllocateChild(*transaction, Request(8));
    ASSERT_TRUE(inline_child.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*inline_child).ok());
    auto overflow_child = original->AllocateChild(*transaction, Request(9));
    ASSERT_TRUE(overflow_child.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*overflow_child).ok());

    auto inline_view = allocator_.Inspect(*inline_child);
    ASSERT_TRUE(inline_view.ok());
    auto* inline_header = reinterpret_cast<SlabHeader*>(
        static_cast<std::byte*>(const_cast<void*>(inline_view->data)) -
        sizeof(SlabHeader));
    const uint32_t original_crc = inline_header->immutable_header_crc;
    inline_header->immutable_header_crc ^= 1u;

    const Status interrupted = original->Abort(*transaction);
    EXPECT_EQ(interrupted.code(), StatusCode::kCorruption);
    EXPECT_EQ(*original->State(*transaction),
              AllocationJournalState::kReclaiming);
    EXPECT_EQ(allocator_.Inspect(*overflow_child).status().code(),
              StatusCode::kNotFound);
    EXPECT_TRUE(allocator_.Inspect(*root).ok());
    inline_header->immutable_header_crc = original_crc;
    EXPECT_TRUE(allocator_.Inspect(*inline_child).ok());

    original.reset();
    auto attached =
        AllocationJournal::Attach(memory.get(), journal_size, allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    ASSERT_EQ(*attached->State(*transaction),
              AllocationJournalState::kReclaiming);
    EXPECT_EQ(attached->RecoverOrphans(&AlwaysUnknown), 1u)
        << "kReclaiming must resume without consulting owner liveness";
    EXPECT_EQ(allocator_.Inspect(*overflow_child).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*inline_child).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(attached->ActiveTransactionCount(), 0u);

    auto reused = attached->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(reused.ok());
    EXPECT_EQ(reused->journal_index, transaction->journal_index);
    EXPECT_NE(reused->transaction_epoch, transaction->transaction_epoch);
    EXPECT_EQ(attached->State(*transaction).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(attached->Abort(*transaction).code(), StatusCode::kNotFound);
    EXPECT_EQ(*attached->State(*reused), AllocationJournalState::kBuilding);
    auto reused_root = attached->AllocateRoot(*reused, Request(10));
    ASSERT_TRUE(reused_root.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*reused_root).ok());
    EXPECT_TRUE(attached->Abort(*reused).ok());
}

TEST_F(AllocationJournalTest, ReleasedInlineAndOverflowSlotsAreReused) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    auto inline_child = AllocateChild(*transaction, 8);
    auto overflow_child = AllocateChild(*transaction, 9);
    auto retained_overflow_child = AllocateChild(*transaction, 10);
    ASSERT_TRUE(root.ok() && inline_child.ok() && overflow_child.ok() &&
                retained_overflow_child.ok());

    ASSERT_TRUE(journal_->ReleaseChild(*transaction, *inline_child).ok());
    ASSERT_TRUE(journal_->ReleaseChild(*transaction, *overflow_child).ok());
    auto inline_replacement = AllocateChild(*transaction, 11);
    auto overflow_replacement = AllocateChild(*transaction, 12);
    ASSERT_TRUE(inline_replacement.ok() && overflow_replacement.ok());
    EXPECT_EQ(AllocateChild(*transaction, 13).status().code(),
              StatusCode::kResourceExhausted);

    const std::array<ShmHandle, 4> graph = {
        *root, *inline_replacement, *overflow_replacement,
        *retained_overflow_child};
    EXPECT_TRUE(journal_->ValidateManifest(*transaction, graph).ok());
    EXPECT_TRUE(journal_->Abort(*transaction).ok());
}

TEST_F(AllocationJournalTest, AbortReclaimsOverflowThenInlineThenRoot) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    auto inline_child = AllocateChild(*transaction, 8);
    auto overflow_child = AllocateChild(*transaction, 9);
    ASSERT_TRUE(root.ok() && inline_child.ok() && overflow_child.ok());

    ReverseReclaimContext context{
        .allocator = &allocator_,
        .manifest = {*root, *inline_child, *overflow_child},
    };
    journal_->SetPersistenceHook(&ObserveReverseReclaim, &context);
    ASSERT_TRUE(journal_->Abort(*transaction).ok());
    journal_->SetPersistenceHook(nullptr, nullptr);

    EXPECT_EQ(context.progress, context.manifest.size());
    EXPECT_TRUE(context.order_correct);
}

TEST_F(AllocationJournalTest, ReleasedChildManifestSlotIsReusableBeyondCapacity) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    ASSERT_TRUE(root.ok());

    for (uint32_t i = 0; i < kHandlesPerTransaction * 8; ++i) {
        auto child = AllocateChild(*transaction, 100 + i);
        ASSERT_TRUE(child.ok()) << child.status().ToString();
        ASSERT_TRUE(journal_->ReleaseChild(*transaction, *child).ok());
        EXPECT_EQ(allocator_.Inspect(*child).status().code(),
                  StatusCode::kNotFound);
    }
    EXPECT_EQ(journal_->ReleaseChild(*transaction, *root).code(),
              StatusCode::kNotFound);
    EXPECT_TRUE(journal_->Abort(*transaction).ok());
}

TEST_F(AllocationJournalTest, RegisterChildRejectsForeignAndPublishedHandles) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    ASSERT_TRUE(root.ok());

    auto foreign = allocator_.Allocate(Request(99));
    ASSERT_TRUE(foreign.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*foreign).ok());
    EXPECT_EQ(journal_->RegisterChild(*transaction, *foreign).code(),
              StatusCode::kPermissionDenied);
    ASSERT_TRUE(allocator_.Publish(*foreign).ok());
    EXPECT_EQ(journal_->RegisterChild(*transaction, *foreign).code(),
              StatusCode::kPermissionDenied);

    EXPECT_TRUE(journal_->Abort(*transaction).ok());
    EXPECT_TRUE(allocator_.Retire(*foreign).ok());
    EXPECT_TRUE(allocator_.Reclaim(*foreign).ok());
}

TEST_F(AllocationJournalTest, AttachRejectsOldLayoutAndInvalidCapacityMetadata) {
    struct alignas(64) ControlMirror {
        std::atomic<uint64_t> magic{0};
        std::atomic<uint32_t> layout_version{0};
        uint32_t transaction_capacity = 0;
        uint32_t handles_per_transaction = 0;
        uint32_t reserved0 = 0;
        unsigned char reserved[40] = {};
    };
    static_assert(sizeof(ControlMirror) == 64);

    const size_t journal_size = AllocationJournal::RequiredSize(
        kTransactionCapacity, kHandlesPerTransaction);
    auto* control = reinterpret_cast<ControlMirror*>(journal_memory_.get());

    control->layout_version.store(AllocationJournal::kLayoutVersion - 1,
                                  std::memory_order_release);
    auto old_layout = AllocationJournal::Attach(
        journal_memory_.get(), journal_size, allocator_);
    ASSERT_FALSE(old_layout.ok());
    EXPECT_EQ(old_layout.status().code(), StatusCode::kSchemaMismatch);
    control->layout_version.store(AllocationJournal::kLayoutVersion,
                                  std::memory_order_release);

    const uint32_t saved_handles = control->handles_per_transaction;
    control->handles_per_transaction = 0;
    auto zero_capacity = AllocationJournal::Attach(
        journal_memory_.get(), journal_size, allocator_);
    ASSERT_FALSE(zero_capacity.ok());
    EXPECT_EQ(zero_capacity.status().code(), StatusCode::kCorruption);

    control->handles_per_transaction = std::numeric_limits<uint32_t>::max();
    auto oversized_layout = AllocationJournal::Attach(
        journal_memory_.get(), journal_size, allocator_);
    ASSERT_FALSE(oversized_layout.ok());
    EXPECT_EQ(oversized_layout.status().code(), StatusCode::kCorruption);
    control->handles_per_transaction = saved_handles;
}

TEST_F(AllocationJournalTest, AttachRejectsBufferSmallerThanControlBeforeRead) {
    auto attached = AllocationJournal::Attach(
        journal_memory_.get(), 1, allocator_);
    ASSERT_FALSE(attached.ok());
    EXPECT_EQ(attached.status().code(), StatusCode::kCorruption);
}

TEST_F(AllocationJournalTest, ManifestMustExactlyEqualReachableGraph) {
    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    auto child = AllocateChild(*transaction, 8);
    auto unreachable = AllocateChild(*transaction, 9);
    ASSERT_TRUE(root.ok() && child.ok() && unreachable.ok());

    const std::vector<ShmHandle> incomplete = {*root, *child};
    EXPECT_EQ(journal_->ValidateManifest(*transaction, incomplete).code(),
              StatusCode::kCorruption);
    const std::vector<ShmHandle> duplicate = {*root, *child, *child};
    EXPECT_EQ(journal_->ValidateManifest(*transaction, duplicate).code(),
              StatusCode::kCorruption);
    const std::vector<ShmHandle> exact = {*root, *child, *unreachable};
    EXPECT_TRUE(journal_->ValidateManifest(*transaction, exact).ok());
    EXPECT_TRUE(journal_->Abort(*transaction).ok());
}

TEST_F(AllocationJournalTest, AttachSharesExactTaggedState) {
    const size_t journal_size = AllocationJournal::RequiredSize(
        kTransactionCapacity, kHandlesPerTransaction);
    auto attached = AllocationJournal::Attach(
        journal_memory_.get(), journal_size, allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();

    auto transaction = journal_->Begin(ProcessIdentity::Current());
    ASSERT_TRUE(transaction.ok());
    auto root = AllocateRoot(*transaction);
    ASSERT_TRUE(root.ok());
    auto state = attached->State(*transaction);
    ASSERT_TRUE(state.ok());
    EXPECT_EQ(*state, AllocationJournalState::kBuilding);
    EXPECT_TRUE(attached->Abort(*transaction).ok());
}

}  // namespace
}  // namespace mino
