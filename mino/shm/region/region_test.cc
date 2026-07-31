// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/region.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "mino/platform/shared_memory.h"

namespace mino {
namespace {

class RegionTest : public ::testing::Test {
 protected:
  std::string Name(const char* tag) {
    static uint32_t sequence = 0;
    std::string name = "/mr_" + std::to_string(::getpid()) + "_" +
                       std::to_string(++sequence) + tag;
    EXPECT_LE(name.size(), 31u);
    names_.push_back(name);
    return name;
  }

  Result<SharedMemoryRegion> Create(const std::string& name) {
    RegionCreateOptions options;
    options.name = name;
    options.size_bytes = 1024 * 1024;
    return SharedMemoryRegion::Create(options);
  }

  void TearDown() override {
    for (const std::string& name : names_) {
      SharedMemorySegment::Unlink(name);
    }
  }

  std::vector<std::string> names_;
};

TEST_F(RegionTest, CreateAttachAndDetachLifecycle) {
  const std::string name = Name("a");
  auto created = Create(name);
  ASSERT_TRUE(created.ok()) << created.status().ToString();
  const uint32_t region_id = created->region_id();
  EXPECT_GT(region_id, 0u);
  EXPECT_EQ(created->superblock()->magic, kSuperBlockMagic);
  EXPECT_EQ(LoadRegionState(*created->superblock()), RegionState::kActive);
  EXPECT_EQ(RecoveryFencePhaseOf(LoadRecoveryFence(*created->superblock())),
            RecoveryFencePhase::kActive);
  EXPECT_EQ(RecoveryFenceEpoch(LoadRecoveryFence(*created->superblock())), 1u);

  ASSERT_TRUE(created->Detach().ok());
  EXPECT_TRUE(created->Detach().ok());

  RegionAttachOptions options;
  options.name = name;
  options.region_id = region_id;
  auto attached = SharedMemoryRegion::Attach(options);
  ASSERT_TRUE(attached.ok()) << attached.status().ToString();
  EXPECT_EQ(attached->region_id(), region_id);
  EXPECT_EQ(LoadRegionState(*attached->superblock()), RegionState::kActive);
  EXPECT_FALSE(LoadCleanShutdown(*attached->superblock()));
  EXPECT_TRUE(attached->Detach().ok());
}

TEST_F(RegionTest, DuplicateCreateFails) {
  const std::string name = Name("d");
  auto first = Create(name);
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  auto second = Create(name);
  ASSERT_FALSE(second.ok());
  EXPECT_EQ(second.status().code(), StatusCode::kAlreadyExists);
}

TEST_F(RegionTest, RejectsInvalidCreateOptions) {
  RegionCreateOptions zero;
  zero.name = Name("z");
  EXPECT_EQ(SharedMemoryRegion::Create(zero).status().code(),
            StatusCode::kInvalidArgument);

  RegionCreateOptions too_small;
  too_small.name = Name("s");
  too_small.size_bytes = kSuperBlockSize;
  EXPECT_EQ(SharedMemoryRegion::Create(too_small).status().code(),
            StatusCode::kInvalidArgument);

  RegionCreateOptions bad_name;
  bad_name.name = "no_slash";
  bad_name.size_bytes = 1024 * 1024;
  EXPECT_EQ(SharedMemoryRegion::Create(bad_name).status().code(),
            StatusCode::kInvalidArgument);
}

TEST_F(RegionTest, AttachRejectsBadMagic) {
  const std::string name = Name("m");
  auto region = Create(name);
  ASSERT_TRUE(region.ok()) << region.status().ToString();
  region->superblock()->magic ^= 1u;

  RegionAttachOptions options;
  options.name = name;
  options.read_only = true;
  auto attached = SharedMemoryRegion::Attach(options);
  ASSERT_FALSE(attached.ok());
  EXPECT_EQ(attached.status().code(), StatusCode::kCorruption);
}

TEST_F(RegionTest, AttachRejectsUnsupportedLayoutVersion) {
  const std::string name = Name("v");
  auto region = Create(name);
  ASSERT_TRUE(region.ok()) << region.status().ToString();
  SuperBlock* sb = region->superblock();
  ++sb->layout_version;
  sb->immutable_crc32 = SuperBlockImmutableCrc(*sb);

  RegionAttachOptions options;
  options.name = name;
  options.read_only = true;
  auto attached = SharedMemoryRegion::Attach(options);
  ASSERT_FALSE(attached.ok());
  EXPECT_EQ(attached.status().code(), StatusCode::kUnsupported);
}

}  // namespace
}  // namespace mino
