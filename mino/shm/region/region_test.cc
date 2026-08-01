// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/region.h"

#include <cstdint>
#include <string>
#include <thread>
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

TEST_F(RegionTest, RecoveryDirectoryPublishesResourcesAndReferences) {
  const std::string name = Name("rd");
  auto region = Create(name);
  ASSERT_TRUE(region.ok()) << region.status().ToString();
  auto initial = region->recovery_directory();
  ASSERT_TRUE(initial.ok()) << initial.status().ToString();
  EXPECT_EQ(initial->resource_count, 0u);
  EXPECT_EQ(initial->reference_count, 0u);

  const SuperBlock& sb = *region->superblock();
  RecoveryResourceDescriptor central{
      .resource_id = 17,
      .kind = static_cast<uint32_t>(RecoveryResourceKind::kCentralAllocator),
      .format_version = 1,
      .offset = sb.allocator_offset,
      .size = sb.data_offset - sb.allocator_offset,
  };
  ASSERT_TRUE(region->RegisterRecoveryResource(central).ok());
  const RecoveryObjectReference reference{
      .resource_id = 17, .unit_index = 3, .generation = 9};
  ASSERT_TRUE(region->PublishRecoveryReferences(
                        std::span<const RecoveryObjectReference>(&reference, 1),
                        /*complete=*/true)
                  .ok());
  auto published = region->recovery_directory();
  ASSERT_TRUE(published.ok()) << published.status().ToString();
  EXPECT_GT(published->sequence, initial->sequence);
  ASSERT_EQ(published->resource_count, 1u);
  EXPECT_EQ(published->resources[0].resource_id, 17u);
  ASSERT_EQ(published->reference_count, 1u);
  EXPECT_EQ(published->references[0].unit_index, 3u);
  EXPECT_NE(published->flags & kRecoveryDirectoryReferencesComplete, 0u);
}

TEST_F(RegionTest, ConcurrentDirectoryReplacementPublishesOneValidSnapshot) {
  const std::string name = Name("rt");
  auto region = Create(name);
  ASSERT_TRUE(region.ok()) << region.status().ToString();
  const SuperBlock& sb = *region->superblock();
  RecoveryResourceDescriptor descriptor{
      .resource_id = 22,
      .kind = static_cast<uint32_t>(RecoveryResourceKind::kCentralAllocator),
      .format_version = 1,
      .offset = sb.allocator_offset,
      .size = sb.data_offset - sb.allocator_offset,
  };
  std::vector<Status> statuses(8, Status::Error(StatusCode::kInternal));
  std::vector<std::thread> threads;
  for (size_t i = 0; i < statuses.size(); ++i) {
    threads.emplace_back([&, i] {
      RecoveryResourceDescriptor update = descriptor;
      update.generation = i + 1;
      statuses[i] = region->RegisterRecoveryResource(update);
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  for (const Status& status : statuses) {
    EXPECT_TRUE(status.ok()) << status.ToString();
  }
  auto snapshot = region->recovery_directory();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  ASSERT_EQ(snapshot->resource_count, 1u);
  EXPECT_EQ(snapshot->resources[0].resource_id, 22u);
  EXPECT_GE(snapshot->resources[0].generation, 1u);
  EXPECT_LE(snapshot->resources[0].generation, statuses.size());
}

TEST_F(RegionTest, AttachRejectsPublishedDirectoryCrcCorruption) {
  const std::string name = Name("rc");
  auto region = Create(name);
  ASSERT_TRUE(region.ok()) << region.status().ToString();
  const SuperBlock& sb = *region->superblock();
  auto* image = reinterpret_cast<RecoveryDirectoryImage*>(
      region->base() + sb.directory_offset);
  const uint64_t word = std::atomic_ref(image->control.published_word)
                            .load(std::memory_order_acquire);
  image->snapshots[word & 1u].crc32 ^= 1u;

  RegionAttachOptions options;
  options.name = name;
  options.read_only = true;
  auto attached = SharedMemoryRegion::Attach(options);
  ASSERT_FALSE(attached.ok());
  EXPECT_EQ(attached.status().code(), StatusCode::kCorruption);
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
