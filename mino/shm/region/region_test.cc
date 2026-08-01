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
  EXPECT_EQ(sizeof(SuperBlock), 256u);
  EXPECT_EQ(created->superblock()->layout_version, kRegionLayoutVersion);
  EXPECT_TRUE(created->is_supervisor());
  EXPECT_TRUE(created->ValidateSupervisorFence().ok());
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

TEST_F(RegionTest, StaleServiceEpochCannotPublishClosed) {
  const std::string name = Name("f");
  auto region = Create(name);
  ASSERT_TRUE(region.ok()) << region.status().ToString();
  auto observer = SharedMemorySegment::Open(name, /*read_only=*/true);
  ASSERT_TRUE(observer.ok()) << observer.status().ToString();
  const uint64_t stale_epoch = region->service_epoch();
  StoreServiceFence(
      *region->superblock(),
      EncodeServiceFence(stale_epoch + 1, ServiceFencePhase::kOwned));

  EXPECT_EQ(region->Detach().code(), StatusCode::kUnavailable);
  const auto* observed = static_cast<const SuperBlock*>(observer->base());
  EXPECT_EQ(LoadRegionState(*observed), RegionState::kActive);
  EXPECT_FALSE(LoadCleanShutdown(*observed));
  EXPECT_EQ(ServiceFenceEpoch(LoadServiceFence(*observed)), stale_epoch + 1);
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

TEST_F(RegionTest, QuarantinedDiagnosticAttachIsExplicitAndReadOnly) {
  const std::string name = Name("q");
  auto region = Create(name);
  ASSERT_TRUE(region.ok()) << region.status().ToString();
  SuperBlock* sb = region->superblock();
  StoreState(*sb, RegionState::kQuarantined);
  StoreRecoveryFence(
      *sb, EncodeRecoveryFence(LoadRecoveryEpoch(*sb),
                               RecoveryFencePhase::kQuarantined));

  const RegionState state_before = LoadRegionState(*sb);
  const uint64_t epoch_before = LoadRegionEpoch(*sb);
  const bool clean_before = LoadCleanShutdown(*sb);
  const uint64_t service_fence_before = LoadServiceFence(*sb);
  const uint64_t recovery_fence_before = LoadRecoveryFence(*sb);

  RegionAttachOptions ordinary_reader_options;
  ordinary_reader_options.name = name;
  ordinary_reader_options.read_only = true;
  auto ordinary_reader = SharedMemoryRegion::Attach(ordinary_reader_options);
  ASSERT_FALSE(ordinary_reader.ok());
  EXPECT_EQ(ordinary_reader.status().code(), StatusCode::kUnavailable);

  RegionAttachOptions ordinary_writer_options;
  ordinary_writer_options.name = name;
  auto ordinary_writer = SharedMemoryRegion::Attach(ordinary_writer_options);
  ASSERT_FALSE(ordinary_writer.ok());
  EXPECT_EQ(ordinary_writer.status().code(), StatusCode::kUnavailable);

  RegionAttachOptions flagged_writer_options;
  flagged_writer_options.name = name;
  flagged_writer_options.allow_quarantined_read_only = true;
  auto flagged_writer = SharedMemoryRegion::Attach(flagged_writer_options);
  ASSERT_FALSE(flagged_writer.ok());
  EXPECT_EQ(flagged_writer.status().code(), StatusCode::kUnavailable);


  RegionAttachOptions diagnostic_options;
  diagnostic_options.name = name;
  diagnostic_options.read_only = true;
  diagnostic_options.allow_quarantined_read_only = true;
  auto diagnostic = SharedMemoryRegion::Attach(diagnostic_options);
  ASSERT_TRUE(diagnostic.ok()) << diagnostic.status().ToString();
  EXPECT_TRUE(diagnostic->read_only());
  EXPECT_FALSE(diagnostic->is_supervisor());
  EXPECT_TRUE(diagnostic->Detach().ok());

  EXPECT_EQ(LoadRegionState(*sb), state_before);
  EXPECT_EQ(LoadRegionEpoch(*sb), epoch_before);
  EXPECT_EQ(LoadCleanShutdown(*sb), clean_before);
  EXPECT_EQ(LoadServiceFence(*sb), service_fence_before);
  EXPECT_EQ(LoadRecoveryFence(*sb), recovery_fence_before);
}

TEST_F(RegionTest, V2LayoutIsReadOnlyCompatible) {
  const std::string name = Name("v2");
  auto region = Create(name);
  ASSERT_TRUE(region.ok()) << region.status().ToString();
  SuperBlock* sb = region->superblock();
  sb->layout_version = 2;
  sb->immutable_crc32 = SuperBlockImmutableCrc(*sb);

  RegionAttachOptions reader_options;
  reader_options.name = name;
  reader_options.read_only = true;
  auto reader = SharedMemoryRegion::Attach(reader_options);
  ASSERT_TRUE(reader.ok()) << reader.status().ToString();
  EXPECT_TRUE(reader->read_only());

  RegionAttachOptions writer_options;
  writer_options.name = name;
  auto writer = SharedMemoryRegion::Attach(writer_options);
  ASSERT_FALSE(writer.ok());
  EXPECT_EQ(writer.status().code(), StatusCode::kUnsupported);

  ASSERT_TRUE(reader->Detach().ok());
  sb->layout_version = kRegionLayoutVersion;
  sb->immutable_crc32 = SuperBlockImmutableCrc(*sb);
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
