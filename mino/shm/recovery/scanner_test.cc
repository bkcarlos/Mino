// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/recovery/scanner.h"

#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>

#include <gtest/gtest.h>

#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/large_object_pool.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/region/recovery_directory.h"

namespace mino::shm::recovery {
namespace {

constexpr uint64_t kAllocatorBytes = 1u << 20;

ClassTableConfig TestConfig() {
    ClassTableConfig config;
    config.classes = {{.slot_size = 256, .slot_count = 128}};
    return config;
}

struct Authority {
    bool owner = true;
    uint32_t heartbeats = 0;
    uint64_t destructive_proof = 0;
    uint64_t dead_owner_epoch = 0;
    uint64_t dead_transaction_id = 0;
};

bool IsOwner(const void* context) noexcept {
    return static_cast<const Authority*>(context)->owner;
}

void Heartbeat(void* context) noexcept {
    ++static_cast<Authority*>(context)->heartbeats;
}

uint64_t DestructiveProof(const void* context) noexcept {
    return static_cast<const Authority*>(context)->destructive_proof;
}

bool CanReclaimUnpublished(const void* context, uint64_t owner_epoch,
                           uint64_t transaction_id) noexcept {
    const auto& authority = *static_cast<const Authority*>(context);
    return authority.dead_owner_epoch == owner_epoch &&
           authority.dead_transaction_id == transaction_id;
}

class RecoveryScannerTest : public ::testing::Test {
protected:
    struct AlignedDeleter {
        void operator()(std::byte* p) const {
            ::operator delete[](p, std::align_val_t(64));
        }
    };

    void SetUp() override {
        memory_.reset(new (std::align_val_t(64)) std::byte[kAllocatorBytes]);
        std::memset(memory_.get(), 0, kAllocatorBytes);
        auto allocator = CentralSlabAllocator::Create(
            memory_.get(), kAllocatorBytes, TestConfig());
        ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
        allocator_ = *allocator;
    }

    AllocationRequest Request() const {
        return AllocationRequest{
            .object_size = 64,
            .type_id = TypeId{42},
            .schema = SchemaIdentity{.short_id = 0xABCD,
                                     .layout_version = 1},
            .alignment = 8,
        };
    }

    Result<ShmHandle> AllocatePublished() {
        auto handle = allocator_.Allocate(Request());
        if (!handle.ok()) {
            return handle.status();
        }
        auto build = allocator_.BeginBuild(*handle);
        if (!build.ok()) {
            return build.status();
        }
        Status published = allocator_.Publish(*handle);
        if (!published.ok()) {
            return published;
        }
        return *handle;
    }

    SlabHeader* Header(ShmHandle handle) {
        return reinterpret_cast<SlabHeader*>(memory_.get() + handle.offset);
    }

    Result<RecoveryScanner> Scanner(RecoveryScannerOptions options = {}) {
        RecoveryOwnership ownership{
            .context = &authority_,
            .is_owner = &IsOwner,
            .heartbeat = &Heartbeat,
            .destructive_reclaim_proof = &DestructiveProof,
            .can_reclaim_unpublished = &CanReclaimUnpublished,
        };
        return RecoveryScanner::Create(allocator_, ownership, options);
    }

    std::unique_ptr<std::byte[], AlignedDeleter> memory_;
    CentralSlabAllocator allocator_;
    Authority authority_;
};

static_assert(std::is_same_v<RecoveryScanner::SlabHeaderPrefix, SlabHeader>);
static_assert(std::is_same_v<RecoveryScanner::BitmapWord,
                             std::atomic<uint64_t>>);

TEST_F(RecoveryScannerTest, RealAllocatorCleanScanPreservesPublishedSlots) {
    auto first = AllocatePublished();
    auto second = AllocatePublished();
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(second.ok());

    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->slots_scanned, 128u);
    EXPECT_EQ(report->orphan_slab_count, 0u);
    EXPECT_EQ(report->corrupted_slab_count, 0u);
    EXPECT_TRUE(allocator_.Inspect(*first).ok());
    EXPECT_TRUE(allocator_.Inspect(*second).ok());
    EXPECT_GT(authority_.heartbeats, 0u);
}

TEST_F(RecoveryScannerTest, ReclaimsRealAllocatingAndReclaimingSlots) {
    auto allocating = allocator_.Allocate(Request());
    auto reclaiming = allocator_.Allocate(Request());
    ASSERT_TRUE(allocating.ok());
    ASSERT_TRUE(reclaiming.ok());
    Header(*allocating)->object_state.store(
        static_cast<uint32_t>(ObjectState::kAllocating),
        std::memory_order_release);
    Header(*reclaiming)->object_state.store(
        static_cast<uint32_t>(ObjectState::kReclaiming),
        std::memory_order_release);

    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok());
    EXPECT_EQ(scanner->VerifyBitmapConsistency().code(),
              StatusCode::kCorruption);
    EXPECT_TRUE(allocator_.Inspect(*allocating).ok());
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->orphan_slab_count, 2u);
    EXPECT_EQ(report->reclaimed_slab_count, 2u);
    EXPECT_EQ(allocator_.Inspect(*allocating).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(*reclaiming).status().code(),
              StatusCode::kNotFound);

    auto second = scanner->Scan();
    ASSERT_TRUE(second.ok());
    EXPECT_EQ(second->orphan_slab_count, 0u);
    EXPECT_EQ(second->reclaimed_slab_count, 0u);
}

TEST_F(RecoveryScannerTest, UnknownObjectStateIsCorruptionNotOrphan) {
    auto handle = allocator_.Allocate(Request());
    ASSERT_TRUE(handle.ok());
    Header(*handle)->object_state.store(0xCAFEu, std::memory_order_release);

    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok());
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->corrupted_slab_count, 1u);
    EXPECT_EQ(report->orphan_slab_count, 0u);
    EXPECT_EQ(report->reclaimed_slab_count, 0u);
    EXPECT_TRUE(allocator_.IsSlotOccupiedForRecovery(0));
    EXPECT_EQ(Header(*handle)->object_state.load(std::memory_order_acquire),
              0xCAFEu);
}

TEST_F(RecoveryScannerTest, RepairRequiresSuppliedRegionOwnership) {
    auto orphan = allocator_.Allocate(Request());
    ASSERT_TRUE(orphan.ok());
    Header(*orphan)->object_state.store(
        static_cast<uint32_t>(ObjectState::kAllocating),
        std::memory_order_release);
    authority_.owner = false;

    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok());
    EXPECT_EQ(scanner->Scan().status().code(), StatusCode::kPermissionDenied);
    EXPECT_EQ(scanner->ReclaimOrphanSlabs().code(),
              StatusCode::kPermissionDenied);
    EXPECT_TRUE(allocator_.IsSlotOccupiedForRecovery(0));
}

TEST_F(RecoveryScannerTest, ReadOnlyScanUsesRealMetadataWithoutRepair) {
    auto orphan = allocator_.Allocate(Request());
    ASSERT_TRUE(orphan.ok());
    Header(*orphan)->object_state.store(
        static_cast<uint32_t>(ObjectState::kAllocating),
        std::memory_order_release);
    RecoveryScannerOptions options;
    options.repair = false;
    authority_.owner = false;

    auto scanner = Scanner(options);
    ASSERT_TRUE(scanner.ok());
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok());
    EXPECT_EQ(report->orphan_slab_count, 1u);
    EXPECT_EQ(report->reclaimed_slab_count, 0u);
    EXPECT_TRUE(allocator_.Inspect(*orphan).ok());
}

TEST_F(RecoveryScannerTest, RealHeaderCrcCorruptionIsNotRepaired) {
    auto handle = AllocatePublished();
    ASSERT_TRUE(handle.ok());
    Header(*handle)->object_size ^= 1u;

    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok());
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok());
    EXPECT_EQ(report->corrupted_slab_count, 1u);
    EXPECT_EQ(report->reclaimed_slab_count, 0u);
    EXPECT_TRUE(allocator_.IsSlotOccupiedForRecovery(0));
    EXPECT_EQ(scanner->VerifyBitmapConsistency().code(),
              StatusCode::kCorruption);
}

TEST_F(RecoveryScannerTest, RepairsBitmapFreeStaleRealHeaderState) {
    auto handle = AllocatePublished();
    ASSERT_TRUE(handle.ok());
    const uint32_t published =
        static_cast<uint32_t>(ObjectState::kPublished);
    ASSERT_TRUE(allocator_.ClearSlotForRecovery(0, published).ok());
    Header(*handle)->object_state.store(published, std::memory_order_release);

    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok());
    EXPECT_EQ(scanner->VerifyBitmapConsistency().code(),
              StatusCode::kCorruption);
    EXPECT_EQ(Header(*handle)->object_state.load(std::memory_order_acquire),
              published);
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok());
    EXPECT_EQ(report->bitmap_inconsistency_count, 1u);
    EXPECT_EQ(Header(*handle)->object_state.load(std::memory_order_acquire),
              static_cast<uint32_t>(ObjectState::kFree));
}

TEST_F(RecoveryScannerTest, CleanupStaleAcksRequiresOwnerAndAlignment) {
    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok());
    uint64_t bitmap = 0b11;
    RecoveryScanner::AckScanInput input{.live_subscriber_mask = 0b01,
                                        .bitmaps = &bitmap,
                                        .bitmap_count = 1};
    authority_.owner = false;
    EXPECT_EQ(scanner->CleanupStaleAcks(input).code(),
              StatusCode::kPermissionDenied);
    EXPECT_EQ(bitmap, 0b11u);

    authority_.owner = true;
    alignas(std::atomic_ref<uint64_t>::required_alignment)
        std::byte storage[sizeof(uint64_t) + 1]{};
    input.bitmaps = reinterpret_cast<uint64_t*>(storage + 1);
    EXPECT_EQ(scanner->CleanupStaleAcks(input).code(),
              StatusCode::kInvalidArgument);
}

TEST_F(RecoveryScannerTest, CleanupStaleAcksRemainsIdempotent) {
    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok());
    uint64_t bitmaps[] = {0b1111, 0b0110, 0};
    RecoveryScanner::AckScanInput input{.live_subscriber_mask = 0b0101,
                                        .bitmaps = bitmaps,
                                        .bitmap_count = 3};
    uint64_t cleared = 0;
    ASSERT_TRUE(scanner->CleanupStaleAcks(input, &cleared).ok());
    EXPECT_EQ(cleared, 3u);
    EXPECT_EQ(bitmaps[0], 0b0101u);
    EXPECT_EQ(bitmaps[1], 0b0100u);
    ASSERT_TRUE(scanner->CleanupStaleAcks(input, &cleared).ok());
    EXPECT_EQ(cleared, 0u);
}

TEST_F(RecoveryScannerTest, CompleteReferencesOnlyReportPublishedCandidateByDefault) {
    auto referenced = AllocatePublished();
    auto candidate = AllocatePublished();
    ASSERT_TRUE(referenced.ok());
    ASSERT_TRUE(candidate.ok());
    const RecoveryObjectReference references[] = {{
        .resource_id = 9,
        .unit_index = 0,
        .generation = referenced->generation,
    }};
    RecoveryOwnership ownership{.context = &authority_,
                                .is_owner = &IsOwner,
                                .heartbeat = &Heartbeat};
    auto scanner = RecoveryScanner::Create(
        allocator_, ownership, {}, 9, references, /*references_complete=*/true);
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->orphan_slab_count, 1u);
    EXPECT_EQ(report->published_orphan_candidate_count, 1u);
    EXPECT_EQ(report->deferred_reclaim_count, 1u);
    EXPECT_EQ(report->reclaimed_slab_count, 0u);
    EXPECT_NE(report->details.find("snapshot is not a publication fence"),
              std::string::npos);
    EXPECT_TRUE(allocator_.Inspect(*referenced).ok());
    EXPECT_TRUE(allocator_.Inspect(*candidate).ok());
}

TEST_F(RecoveryScannerTest, PublishedCandidateRequiresOfflineOptionAndLiveProof) {
    auto candidate = AllocatePublished();
    ASSERT_TRUE(candidate.ok());
    RecoveryOwnership ownership{
        .context = &authority_,
        .is_owner = &IsOwner,
        .heartbeat = &Heartbeat,
        .destructive_reclaim_proof = &DestructiveProof,
    };
    RecoveryScannerOptions options;
    options.destructive_reclaim_proof_token = 0xC0FFEEu;

    authority_.destructive_proof = options.destructive_reclaim_proof_token;
    auto without_offline_option = RecoveryScanner::Create(
        allocator_, ownership, options, 9, {}, /*references_complete=*/true);
    ASSERT_TRUE(without_offline_option.ok());
    auto option_deferred = without_offline_option->Scan();
    ASSERT_TRUE(option_deferred.ok());
    EXPECT_EQ(option_deferred->reclaimed_slab_count, 0u);
    EXPECT_TRUE(allocator_.Inspect(*candidate).ok());

    options.offline_or_quiesced = true;
    authority_.destructive_proof = 0;
    auto without_proof = RecoveryScanner::Create(
        allocator_, ownership, options, 9, {}, /*references_complete=*/true);
    ASSERT_TRUE(without_proof.ok());
    auto proof_deferred = without_proof->Scan();
    ASSERT_TRUE(proof_deferred.ok());
    EXPECT_EQ(proof_deferred->reclaimed_slab_count, 0u);
    EXPECT_TRUE(allocator_.Inspect(*candidate).ok());

    authority_.destructive_proof = options.destructive_reclaim_proof_token;
    auto proven = RecoveryScanner::Create(
        allocator_, ownership, options, 9, {}, /*references_complete=*/true);
    ASSERT_TRUE(proven.ok());
    auto reclaimed = proven->Scan();
    ASSERT_TRUE(reclaimed.ok()) << reclaimed.status().ToString();
    EXPECT_EQ(reclaimed->published_orphan_candidate_count, 1u);
    EXPECT_EQ(reclaimed->reclaimed_slab_count, 1u);
    EXPECT_EQ(allocator_.Inspect(*candidate).status().code(),
              StatusCode::kNotFound);
}

TEST_F(RecoveryScannerTest, UnpublishedTransactionDefersUntilDeathIsProven) {
    AllocationRequest request = Request();
    request.owner_epoch = 41;
    request.allocation_transaction_id = 73;
    request.allocation_flags = kAllocationFlagTransactionRoot;
    auto candidate = allocator_.Allocate(request);
    ASSERT_TRUE(candidate.ok());

    auto scanner = Scanner();
    ASSERT_TRUE(scanner.ok());
    auto deferred = scanner->Scan();
    ASSERT_TRUE(deferred.ok());
    EXPECT_EQ(deferred->unpublished_orphan_candidate_count, 1u);
    EXPECT_EQ(deferred->deferred_reclaim_count, 1u);
    EXPECT_EQ(deferred->reclaimed_slab_count, 0u);
    EXPECT_NE(deferred->details.find("no Journal/owner-death proof"),
              std::string::npos);
    EXPECT_TRUE(allocator_.Inspect(*candidate).ok());

    authority_.dead_owner_epoch = request.owner_epoch;
    authority_.dead_transaction_id = request.allocation_transaction_id;
    auto proven = Scanner();
    ASSERT_TRUE(proven.ok());
    auto reclaimed = proven->Scan();
    ASSERT_TRUE(reclaimed.ok());
    EXPECT_EQ(reclaimed->reclaimed_slab_count, 1u);
    EXPECT_EQ(allocator_.Inspect(*candidate).status().code(),
              StatusCode::kNotFound);
}

TEST_F(RecoveryScannerTest, LargePoolRecoveryHonorsReferenceCompleteness) {
    std::memset(memory_.get(), 0, kAllocatorBytes);
    auto pool = LargeObjectPool::Create(memory_.get(), kAllocatorBytes,
                                        256 * 1024, 64 * 1024);
    ASSERT_TRUE(pool.ok()) << pool.status().ToString();
    auto survivor = pool->Allocate(100 * 1024, TypeId{7});
    auto orphan = pool->Allocate(32 * 1024, TypeId{8});
    ASSERT_TRUE(survivor.ok());
    ASSERT_TRUE(orphan.ok());
    ASSERT_TRUE(pool->Publish(*survivor).ok());
    ASSERT_TRUE(pool->Publish(*orphan).ok());
    auto survivor_plan = pool->InspectPlan(*survivor);
    ASSERT_TRUE(survivor_plan.ok());
    const RecoveryObjectReference references[] = {{
        .resource_id = 11,
        .unit_index = survivor_plan->segments[0].segment_index,
        .generation = survivor->generation,
    }};
    RecoveryOwnership ownership{.context = &authority_,
                                .is_owner = &IsOwner,
                                .heartbeat = &Heartbeat};

    auto conservative = RecoveryScanner::Create(
        *pool, 11, ownership, {}, references, /*references_complete=*/false);
    ASSERT_TRUE(conservative.ok());
    auto first_report = conservative->Scan();
    ASSERT_TRUE(first_report.ok());
    EXPECT_EQ(first_report->orphan_slab_count, 0u);
    EXPECT_TRUE(pool->InspectPlan(*orphan).ok());

    auto complete = RecoveryScanner::Create(
        *pool, 11, ownership, {}, references, /*references_complete=*/true);
    ASSERT_TRUE(complete.ok());
    auto report = complete->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->orphan_slab_count, 1u);
    EXPECT_EQ(report->published_orphan_candidate_count, 1u);
    EXPECT_EQ(report->deferred_reclaim_count, 1u);
    EXPECT_EQ(report->reclaimed_slab_count, 0u);
    EXPECT_TRUE(pool->InspectPlan(*survivor).ok());
    EXPECT_TRUE(pool->InspectPlan(*orphan).ok());
}

TEST_F(RecoveryScannerTest, LargePoolRecoveryClearsWholeIncompleteRun) {
    std::memset(memory_.get(), 0, kAllocatorBytes);
    auto pool = LargeObjectPool::Create(memory_.get(), kAllocatorBytes,
                                        256 * 1024, 64 * 1024);
    ASSERT_TRUE(pool.ok());
    auto handle = pool->Allocate(100 * 1024, TypeId{12});
    ASSERT_TRUE(handle.ok());
    auto plan = pool->InspectPlan(*handle);
    ASSERT_TRUE(plan.ok());
    ASSERT_EQ(plan->segments.size(), 2u);
    auto* first = reinterpret_cast<SlabHeader*>(memory_.get() + handle->offset);
    first->object_state.store(static_cast<uint32_t>(ObjectState::kAllocating),
                              std::memory_order_release);
    RecoveryOwnership ownership{.context = &authority_,
                                .is_owner = &IsOwner,
                                .heartbeat = &Heartbeat};
    auto scanner = RecoveryScanner::Create(*pool, 3, ownership);
    ASSERT_TRUE(scanner.ok());
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->reclaimed_slab_count, 1u);
    EXPECT_EQ(pool->InspectPlan(*handle).status().code(), StatusCode::kNotFound);
    for (const LargeObjectSegment& segment : plan->segments) {
        EXPECT_FALSE(pool->IsSegmentOccupiedForRecovery(segment.segment_index));
    }
}

TEST_F(RecoveryScannerTest, LargePoolCorruptionIsReportedWithoutRepair) {
    std::memset(memory_.get(), 0, kAllocatorBytes);
    auto pool = LargeObjectPool::Create(memory_.get(), kAllocatorBytes,
                                        256 * 1024, 64 * 1024);
    ASSERT_TRUE(pool.ok());
    auto handle = pool->Allocate(100 * 1024, TypeId{13});
    ASSERT_TRUE(handle.ok());
    ASSERT_TRUE(pool->Publish(*handle).ok());
    auto* continuation = reinterpret_cast<SlabHeader*>(
        memory_.get() + handle->offset + sizeof(SlabHeader));
    continuation->object_size ^= 1u;
    RecoveryOwnership ownership{.context = &authority_,
                                .is_owner = &IsOwner,
                                .heartbeat = &Heartbeat};
    auto scanner = RecoveryScanner::Create(*pool, 4, ownership);
    ASSERT_TRUE(scanner.ok());
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok());
    EXPECT_GT(report->corrupted_slab_count, 0u);
    EXPECT_EQ(report->reclaimed_slab_count, 0u);
    EXPECT_TRUE(pool->IsSegmentOccupiedForRecovery(0));
}

TEST_F(RecoveryScannerTest, GenerationScopedAckAndPinCleanupIsIdempotent) {
    alignas(64) std::byte image[512]{};
    auto* ack_control = new (image) RecoveryGenerationControl{
        .generation = 2, .live_mask = 0b01};
    auto* ack_values = new (image + 64) RecoveryGenerationValue[2]{
        {.generation = 1, .value = 0b11},
        {.generation = 2, .value = 0b11},
    };
    auto* pin_control = new (image + 128) RecoveryGenerationControl{
        .generation = 4, .live_mask = 0};
    auto* pin_values = new (image + 192) RecoveryGenerationValue[2]{
        {.generation = 3, .value = 5},
        {.generation = 4, .value = 7},
    };
    (void)ack_control;
    (void)pin_control;
    RecoveryResourceDescriptor ack{
        .resource_id = 1,
        .kind = static_cast<uint32_t>(RecoveryResourceKind::kChannelAckSource),
        .format_version = 1,
        .offset = 64,
        .size = 2 * sizeof(RecoveryGenerationValue),
        .generation = 1,
        .element_count = 2,
        .element_stride = sizeof(RecoveryGenerationValue),
        .generation_offset = offsetof(RecoveryGenerationValue, generation),
        .value_offset = offsetof(RecoveryGenerationValue, value),
        .control_offset = 0,
        .control_size = sizeof(RecoveryGenerationControl),
    };
    RecoveryResourceDescriptor pin = ack;
    pin.resource_id = 2;
    pin.kind = static_cast<uint32_t>(
        RecoveryResourceKind::kPinCleanupParticipant);
    pin.offset = 192;
    pin.generation = 3;
    pin.control_offset = 128;
    RecoveryOwnership ownership{.context = &authority_,
                                .is_owner = &IsOwner,
                                .heartbeat = &Heartbeat};
    RecoveryReport report;
    ASSERT_TRUE(RecoveryScanner::CleanupGenerationScopedResource(
                    image, sizeof(image), ack, ownership, &report)
                    .ok());
    ASSERT_TRUE(RecoveryScanner::CleanupGenerationScopedResource(
                    image, sizeof(image), pin, ownership, &report)
                    .ok());
    EXPECT_EQ(report.stale_ack_count, 3u);
    EXPECT_EQ(report.stale_pin_count, 5u);
    EXPECT_EQ(ack_values[0].value, 0u);
    EXPECT_EQ(ack_values[1].value, 0b01u);
    EXPECT_EQ(pin_values[0].value, 0u);
    EXPECT_EQ(pin_values[1].value, 7u);

    RecoveryReport second;
    ASSERT_TRUE(RecoveryScanner::CleanupGenerationScopedResource(
                    image, sizeof(image), ack, ownership, &second)
                    .ok());
    ASSERT_TRUE(RecoveryScanner::CleanupGenerationScopedResource(
                    image, sizeof(image), pin, ownership, &second)
                    .ok());
    EXPECT_EQ(second.stale_ack_count, 0u);
    EXPECT_EQ(second.stale_pin_count, 0u);
}

TEST_F(RecoveryScannerTest, LegacyLayoutRejectsMisalignment) {
    alignas(64) std::byte image[512]{};
    auto* owner = new (image) RecoveryOwnerState();
    RecoveryOwner::Initialize(owner);
    RecoveryScanner::Layout layout;
    layout.recovery_state_offset = 0;
    layout.class_table_offset = sizeof(RecoveryOwnerState) + 1;
    layout.class_count = 0;
    EXPECT_EQ(RecoveryScanner::Create(image, sizeof(image), layout)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::shm::recovery
