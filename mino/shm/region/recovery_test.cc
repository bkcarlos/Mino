// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/recovery.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>

#include <gtest/gtest.h>
#include <unistd.h>

#include "mino/platform/shared_memory.h"
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
  EXPECT_TRUE(region().superblock()->recovery_owner.IsZero());
  EXPECT_EQ(LoadRecoveryEpoch(*region().superblock()), initial_epoch + 2);
  EXPECT_TRUE(owner.Release().ok());
}

TEST_F(RecoveryOwnerTest, ContentionAllowsOnlyOneOwner) {
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::atomic<int> winners{0};
  std::atomic<int> blocked{0};
  auto compete = [&] {
    ready.fetch_add(1, std::memory_order_release);
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    auto result = RecoveryOwner::TryAcquire(
        region(), ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
    if (result.ok()) {
      winners.fetch_add(1, std::memory_order_relaxed);
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      std::move(result).value().Release();
    } else if (result.status().code() == StatusCode::kWouldBlock) {
      blocked.fetch_add(1, std::memory_order_relaxed);
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
  EXPECT_EQ(first.RenewLease().code(), StatusCode::kUnavailable);
  EXPECT_TRUE(second.IsOwner());
  EXPECT_TRUE(second.Release().ok());
}

}  // namespace
}  // namespace mino
