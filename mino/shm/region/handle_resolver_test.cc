// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/handle_resolver.h"

#include <cstdint>
#include <new>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino {
namespace {

constexpr TypeId kType{17};
constexpr uint32_t kGeneration = 9;
constexpr uint64_t kSchema = 0x1234;

class FakeAllocatorMetadataProvider : public AllocatorMetadataProvider {
 public:
  bool IsSlotOccupied(uint64_t offset) const override {
    return occupied && offset == slot_offset;
  }
  uint32_t AuthoritativeGeneration(uint64_t offset) const override {
    return offset == slot_offset ? generation : 0;
  }

  uint64_t slot_offset = 0;
  uint32_t generation = kGeneration;
  bool occupied = true;
};

class HandleResolverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static uint32_t sequence = 0;
    name_ = "/hr_" + std::to_string(::getpid()) + "_" +
            std::to_string(++sequence);
    ASSERT_LE(name_.size(), 31u);
    RegionCreateOptions options;
    options.name = name_;
    options.size_bytes = 1024 * 1024;
    region_result_ = SharedMemoryRegion::Create(options);
    ASSERT_TRUE(region_result_.ok()) << region_result_.status().ToString();

    SharedMemoryRegion& region = region_result_.value();
    allocator_.slot_offset = region.superblock()->data_offset;
    header_ = new (region.base() + allocator_.slot_offset) SlabHeader{};
    ResetHeader(ObjectState::kPublished);
    handle_ = {allocator_.slot_offset, kGeneration, region.region_id()};
  }

  void TearDown() override { SharedMemorySegment::Unlink(name_); }

  void ResetHeader(ObjectState state) {
    header_->magic = kSlabHeaderMagic;
    header_->header_version = kSlabHeaderVersion;
    header_->generation = kGeneration;
    header_->object_state.store(static_cast<uint32_t>(state));
    header_->capacity = 128;
    header_->object_size = sizeof(uint64_t);
    header_->type_id = kType.value;
    header_->schema_short_id = kSchema;
    header_->owner_epoch = ProcessIdentity::Current().process_epoch;
  }

  SharedMemoryRegion& region() { return region_result_.value(); }
  HandleResolver resolver() { return HandleResolver(region(), &allocator_); }

  std::string name_;
  Result<SharedMemoryRegion> region_result_ =
      Status::Error(StatusCode::kInternal);
  FakeAllocatorMetadataProvider allocator_;
  SlabHeader* header_ = nullptr;
  ShmHandle handle_;
};

TEST_F(HandleResolverTest, RejectsNullAndForeignRegionHandles) {
  auto r = resolver();
  EXPECT_EQ(r.Resolve<uint64_t>(ShmHandle{}, kType).status().code(),
            StatusCode::kInvalidArgument);
  ShmHandle foreign = handle_;
  ++foreign.region_id;
  EXPECT_EQ(r.Resolve<uint64_t>(foreign, kType).status().code(),
            StatusCode::kNotFound);
}

TEST_F(HandleResolverTest, RejectsUnoccupiedAndGenerationMismatch) {
  auto r = resolver();
  allocator_.occupied = false;
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kNotFound);
  allocator_.occupied = true;
  ++allocator_.generation;
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kNotFound);
  allocator_.generation = kGeneration;
  ++header_->generation;
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kNotFound);
}

TEST_F(HandleResolverTest, RejectsTypeAndSchemaMismatch) {
  auto r = resolver();
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, TypeId{18}).status().code(),
            StatusCode::kSchemaMismatch);
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType, kSchema + 1).status().code(),
            StatusCode::kSchemaMismatch);
}

TEST_F(HandleResolverTest, MutableRequiresMutableStateAndCurrentOwner) {
  auto r = resolver();
  EXPECT_EQ(r.ResolveMutable<uint64_t>(handle_, kType).status().code(),
            StatusCode::kPermissionDenied);
  ResetHeader(ObjectState::kAllocated);
  auto allocated = r.ResolveMutable<uint64_t>(handle_, kType, kSchema);
  ASSERT_TRUE(allocated.ok()) << allocated.status().ToString();
  EXPECT_EQ(reinterpret_cast<std::byte*>(allocated.value()),
            region().base() + handle_.offset + sizeof(SlabHeader));
  ResetHeader(ObjectState::kBuilding);
  EXPECT_TRUE(r.ResolveMutable<uint64_t>(handle_, kType).ok());
  ++header_->owner_epoch;
  EXPECT_EQ(r.ResolveMutable<uint64_t>(handle_, kType).status().code(),
            StatusCode::kPermissionDenied);
}

TEST_F(HandleResolverTest, PublishedObjectResolvesReadOnly) {
  auto r = resolver();
  auto resolved = r.Resolve<uint64_t>(handle_, kType, kSchema);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(reinterpret_cast<const std::byte*>(resolved.value()),
            region().base() + handle_.offset + sizeof(SlabHeader));
  ResetHeader(ObjectState::kAllocated);
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kPermissionDenied);
}

TEST_F(HandleResolverTest, RejectsMisalignedAndOutOfBoundsOffsets) {
  auto r = resolver();
  ShmHandle misaligned = handle_;
  ++misaligned.offset;
  EXPECT_EQ(r.Resolve<uint64_t>(misaligned, kType).status().code(),
            StatusCode::kInvalidArgument);

  ShmHandle below = handle_;
  below.offset = region().superblock()->data_offset - alignof(SlabHeader);
  EXPECT_EQ(r.Resolve<uint64_t>(below, kType).status().code(),
            StatusCode::kInvalidArgument);

  header_->capacity = static_cast<uint32_t>(region().superblock()->data_size);
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kCorruption);
}

}  // namespace
}  // namespace mino
