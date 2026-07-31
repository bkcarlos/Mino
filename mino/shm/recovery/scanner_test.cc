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
#include "mino/shm/allocator/slab_header.h"

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
};

bool IsOwner(const void* context) noexcept {
    return static_cast<const Authority*>(context)->owner;
}

void Heartbeat(void* context) noexcept {
    ++static_cast<Authority*>(context)->heartbeats;
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
        RecoveryOwnership ownership{.context = &authority_,
                                    .is_owner = &IsOwner,
                                    .heartbeat = &Heartbeat};
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
