// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/recovery.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <thread>
#include <utility>

#include <gtest/gtest.h>
#include <unistd.h>

#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/region/region.h"

namespace mino {
namespace {



// SharedMemoryRegion has no public default constructor, so use a helper fixture
// that owns the move-only Result until each test body.
class RecoveryOwnerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static uint32_t sequence = 0;
    name_ = "/ro_" + std::to_string(::getpid()) + "_" +
            std::to_string(++sequence);
    ASSERT_LE(name_.size(), 31u);
    RegionCreateOptions options;
    options.name = name_;
    options.size_bytes = 1024 * 1024;
    created_ = SharedMemoryRegion::Create(options);
    ASSERT_TRUE(created_.ok()) << created_.status().ToString();
  }
  void TearDown() override { SharedMemorySegment::Unlink(name_); }

  SharedMemoryRegion& region() { return created_.value(); }
  std::string name_;
  Result<SharedMemoryRegion> created_ = Status::Error(StatusCode::kInternal);
};

TEST_F(RecoveryOwnerTest, AcquireRenewRelease) {
  const uint64_t initial_epoch = LoadRecoveryEpoch(*region().superblock());
  auto acquired = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
  ASSERT_TRUE(acquired.ok()) << acquired.status().ToString();
  RecoveryOwner owner = std::move(acquired).value();
  EXPECT_TRUE(owner.IsOwner());
  EXPECT_EQ(LoadRecoveryEpoch(*region().superblock()), initial_epoch + 1);
  EXPECT_TRUE(owner.RenewLease(1000).ok());
  EXPECT_TRUE(owner.IsOwner());
  EXPECT_TRUE(owner.Release().ok());
  EXPECT_FALSE(owner.IsOwner());
  EXPECT_EQ(LoadRecoveryLeaseNs(*region().superblock()), 0u);
  // recovery_owner is informational and may retain the last owner while the
  // authoritative lease is zero.
  EXPECT_EQ(LoadRecoveryEpoch(*region().superblock()), initial_epoch + 1);
  EXPECT_TRUE(owner.Release().ok());
}

TEST_F(RecoveryOwnerTest, ContentionAllowsOnlyOneOwner) {
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::atomic<int> winners{0};
  std::atomic<int> blocked{0};
  std::atomic<int> attempted{0};
  auto compete = [&] {
    ready.fetch_add(1, std::memory_order_release);
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    auto result = RecoveryOwner::TryAcquire(
        region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
    if (result.ok()) {
      winners.fetch_add(1, std::memory_order_relaxed);
      attempted.fetch_add(1, std::memory_order_release);
      while (attempted.load(std::memory_order_acquire) != 2) {
        std::this_thread::yield();
      }
      std::move(result).value().Release();
    } else {
      if (result.status().code() == StatusCode::kWouldBlock) {
        blocked.fetch_add(1, std::memory_order_relaxed);
      }
      attempted.fetch_add(1, std::memory_order_release);
    }
  };
  std::thread first(compete);
  std::thread second(compete);
  while (ready.load(std::memory_order_acquire) != 2) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  first.join();
  second.join();
  EXPECT_EQ(winners.load(), 1);
  EXPECT_EQ(blocked.load(), 1);
}

TEST_F(RecoveryOwnerTest, ExpiredLeaseCanBeTakenOverAndFencesOldOwner) {
  auto first_result = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1);
  ASSERT_TRUE(first_result.ok()) << first_result.status().ToString();
  RecoveryOwner first = std::move(first_result).value();
  const uint64_t first_epoch = LoadRecoveryEpoch(*region().superblock());
  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  auto second_result = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
  ASSERT_TRUE(second_result.ok()) << second_result.status().ToString();
  RecoveryOwner second = std::move(second_result).value();
  EXPECT_EQ(LoadRecoveryEpoch(*region().superblock()), first_epoch + 1);
  EXPECT_FALSE(first.IsOwner());
  const uint64_t second_lease = LoadRecoveryLeaseNs(*region().superblock());
  EXPECT_EQ(first.Release().code(), StatusCode::kUnavailable);
  EXPECT_EQ(LoadRecoveryLeaseNs(*region().superblock()), second_lease);
  EXPECT_TRUE(second.IsOwner());
  EXPECT_TRUE(second.Release().ok());
}

TEST_F(RecoveryOwnerTest, DirtyAttachScansRealRegionAllocatorMetadata) {
  SharedMemoryRegion& created = region();
  const SuperBlock& sb = *created.superblock();
  RegionAllocatorStorage storage{
      .region_base = created.base(),
      .region_size = created.size(),
      .allocator_offset = sb.allocator_offset,
      .allocator_size = sb.data_offset - sb.allocator_offset,
      .data_offset = sb.data_offset,
      .data_size = sb.data_size,
      .region_id = sb.region_id,
  };
  ClassTableConfig config;
  config.classes = {{.slot_size = 256, .slot_count = 8}};
  auto allocator = CentralSlabAllocator::CreateInRegion(storage, config);
  ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

  AllocationRequest request{
      .object_size = 64,
      .type_id = TypeId{17},
      .schema = SchemaIdentity{.short_id = 0x1234, .layout_version = 1},
      .alignment = 8,
  };
  auto survivor = allocator->Allocate(request);
  ASSERT_TRUE(survivor.ok()) << survivor.status().ToString();
  ASSERT_GE(survivor->offset, sb.data_offset);
  EXPECT_EQ(survivor->region_id, created.region_id());
  ASSERT_TRUE(allocator->BeginBuild(*survivor).ok());
  ASSERT_TRUE(allocator->Publish(*survivor).ok());

  auto orphan = allocator->Allocate(request);
  ASSERT_TRUE(orphan.ok()) << orphan.status().ToString();
  auto* orphan_header = reinterpret_cast<SlabHeader*>(
      created.base() + orphan->offset);
  orphan_header->object_state.store(
      static_cast<uint32_t>(ObjectState::kAllocating),
      std::memory_order_release);

  const uint64_t epoch_before = LoadRegionEpoch(sb);
  // The fixture already owns the unique writable supervisor role. Exercise the
  // scanner directly after an explicit DIRTY transition; a second writable
  // Attach must not bypass the live supervisor lock.
  StoreState(*created.superblock(), RegionState::kDirty);
  Status recovery = RecoverRegionForAttach(
      created, ProcessIdentity::Current(), /*wait_timeout_ms=*/1000);
  ASSERT_TRUE(recovery.ok()) << recovery.ToString();
  EXPECT_EQ(LoadRegionState(*created.superblock()), RegionState::kActive);
  EXPECT_EQ(LoadRegionEpoch(*created.superblock()), epoch_before + 1);

  RegionAllocatorStorage attached_storage{
      .region_base = created.base(),
      .region_size = created.size(),
      .allocator_offset = created.superblock()->allocator_offset,
      .allocator_size = created.superblock()->data_offset -
                        created.superblock()->allocator_offset,
      .data_offset = created.superblock()->data_offset,
      .data_size = created.superblock()->data_size,
      .region_id = created.region_id(),
  };
  auto recovered = CentralSlabAllocator::AttachInRegion(attached_storage);
  ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
  auto survivor_view = recovered->Inspect(*survivor);
  ASSERT_TRUE(survivor_view.ok()) << survivor_view.status().ToString();
  EXPECT_EQ(survivor_view->state, ObjectState::kPublished);
  EXPECT_EQ(recovered->Inspect(*orphan).status().code(),
            StatusCode::kNotFound);
  EXPECT_EQ(LoadRecoveryLeaseNs(*created.superblock()), 0u);
}

TEST_F(RecoveryOwnerTest, MoveClearsSourceOwnership) {
  auto acquired = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
  ASSERT_TRUE(acquired.ok());
  RecoveryOwner source = std::move(*acquired);
  RecoveryOwner destination = std::move(source);
  EXPECT_FALSE(source.IsOwner());
  EXPECT_TRUE(source.Release().ok());
  EXPECT_TRUE(destination.IsOwner());
  EXPECT_TRUE(destination.Release().ok());
}

TEST_F(RecoveryOwnerTest, RejectsZeroAndOverflowingLeaseDurations) {
  EXPECT_EQ(RecoveryOwner::TryAcquire(
                region(), ProcessIdentity::Current(), /*lease_duration_ms=*/0)
                .status()
                .code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(RecoveryOwner::TryAcquire(
                region(), ProcessIdentity::Current(),
                std::numeric_limits<uint64_t>::max())
                .status()
                .code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(RecoverRegionForAttach(
                region(), ProcessIdentity::Current(),
                std::numeric_limits<uint64_t>::max())
                .code(),
            StatusCode::kInvalidArgument);

  auto acquired = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
  ASSERT_TRUE(acquired.ok());
  RecoveryOwner owner = std::move(*acquired);
  EXPECT_EQ(owner.RenewLease(0).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(owner.RenewLease(std::numeric_limits<uint64_t>::max()).code(),
            StatusCode::kInvalidArgument);
  EXPECT_TRUE(owner.IsOwner());
  EXPECT_TRUE(owner.Release().ok());
}

TEST_F(RecoveryOwnerTest, ContenderDoesNotOverwriteLiveRecoveringState) {
  auto acquired = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
  ASSERT_TRUE(acquired.ok());
  RecoveryOwner owner = std::move(*acquired);
  const uint64_t observed_fence = LoadRecoveryFence(*region().superblock());
  ASSERT_TRUE(owner.ClaimRecoveryFence(observed_fence).ok());
  StoreState(*region().superblock(), RegionState::kRecovering);

  Status contender = RecoverRegionForAttach(
      region(), ProcessIdentity::Current(), /*wait_timeout_ms=*/0);
  EXPECT_EQ(contender.code(), StatusCode::kTimeout);
  EXPECT_EQ(LoadRegionState(*region().superblock()),
            RegionState::kRecovering);
  EXPECT_TRUE(owner.IsOwner());
  ASSERT_TRUE(owner.CommitRecoveryFence(RecoveryFencePhase::kActive).ok());
  StoreRegionEpoch(*region().superblock(), owner.epoch());
  StoreState(*region().superblock(), RegionState::kActive);
  EXPECT_TRUE(owner.Release().ok());
}

TEST_F(RecoveryOwnerTest, RecoveringWithExpiredLeaseCanBeTakenOver) {
  SuperBlock* sb = region().superblock();
  auto first_result = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1);
  ASSERT_TRUE(first_result.ok());
  RecoveryOwner first = std::move(*first_result);
  ASSERT_TRUE(first.ClaimRecoveryFence(LoadRecoveryFence(*sb)).ok());
  StoreState(*sb, RegionState::kRecovering);
  const uint64_t epoch_before = LoadRegionEpoch(*sb);
  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  Status recovered = RecoverRegionForAttach(
      region(), ProcessIdentity::Current(), /*wait_timeout_ms=*/1000);
  ASSERT_TRUE(recovered.ok()) << recovered.ToString();
  EXPECT_EQ(LoadRegionState(*sb), RegionState::kActive);
  EXPECT_GT(LoadRegionEpoch(*sb), epoch_before);
  EXPECT_EQ(LoadRegionEpoch(*sb),
            RecoveryFenceEpoch(LoadRecoveryFence(*sb)));
  EXPECT_EQ(LoadRecoveryLeaseNs(*sb), 0u);
}

TEST_F(RecoveryOwnerTest, StaleOwnerCannotCommitAfterFenceTakeover) {
  SuperBlock* sb = region().superblock();
  auto first_result = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1);
  ASSERT_TRUE(first_result.ok());
  RecoveryOwner first = std::move(*first_result);
  ASSERT_TRUE(first.ClaimRecoveryFence(LoadRecoveryFence(*sb)).ok());
  StoreState(*sb, RegionState::kRecovering);
  const uint64_t first_fence = LoadRecoveryFence(*sb);

  // A is paused until its lease expires. B then acquires both a new lease
  // generation and the same atomic commit fence before A resumes.
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  auto second_result = RecoveryOwner::TryAcquire(
      region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
  ASSERT_TRUE(second_result.ok());
  RecoveryOwner second = std::move(*second_result);
  ASSERT_TRUE(second.ClaimRecoveryFence(first_fence).ok());
  const uint64_t second_fence = LoadRecoveryFence(*sb);
  EXPECT_NE(second_fence, first_fence);

  EXPECT_EQ(first.CommitRecoveryFence(RecoveryFencePhase::kActive).code(),
            StatusCode::kUnavailable);
  EXPECT_EQ(LoadRecoveryFence(*sb), second_fence);
  ASSERT_TRUE(second.CommitRecoveryFence(RecoveryFencePhase::kActive).ok());
  StoreRegionEpoch(*sb, second.epoch());
  StoreState(*sb, RegionState::kActive);
  EXPECT_TRUE(second.Release().ok());
}

TEST_F(RecoveryOwnerTest, ActiveInUseRegionIsFencedWithoutRepair) {
  SharedMemoryRegion& active = region();
  const SuperBlock& sb = *active.superblock();
  RegionAllocatorStorage storage{
      .region_base = active.base(),
      .region_size = active.size(),
      .allocator_offset = sb.allocator_offset,
      .allocator_size = sb.data_offset - sb.allocator_offset,
      .data_offset = sb.data_offset,
      .data_size = sb.data_size,
      .region_id = sb.region_id,
  };
  ClassTableConfig config;
  config.classes = {{.slot_size = 256, .slot_count = 8}};
  auto allocator = CentralSlabAllocator::CreateInRegion(storage, config);
  ASSERT_TRUE(allocator.ok());
  AllocationRequest request{.object_size = 64,
                            .type_id = TypeId{19},
                            .schema = SchemaIdentity{.short_id = 0x99,
                                                     .layout_version = 1},
                            .alignment = 8};
  auto orphan = allocator->Allocate(request);
  ASSERT_TRUE(orphan.ok());
  auto* header = reinterpret_cast<SlabHeader*>(active.base() + orphan->offset);
  header->object_state.store(static_cast<uint32_t>(ObjectState::kAllocating),
                             std::memory_order_release);

  RegionAttachOptions options;
  options.name = name_;
  auto second_attach = SharedMemoryRegion::Attach(options);
  ASSERT_FALSE(second_attach.ok());
  EXPECT_EQ(second_attach.status().code(), StatusCode::kWouldBlock);
  EXPECT_EQ(LoadRegionState(*active.superblock()), RegionState::kActive);
  EXPECT_TRUE(allocator->IsSlotOccupiedForRecovery(0));
  EXPECT_EQ(header->object_state.load(std::memory_order_acquire),
            static_cast<uint32_t>(ObjectState::kAllocating));
}

}  // namespace
}  // namespace mino
