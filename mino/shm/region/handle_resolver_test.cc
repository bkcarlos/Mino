// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/handle_resolver.h"

#include <cstdint>
#include <cstring>
#include <new>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/large_object_pool.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino {
namespace {

constexpr TypeId kType{17};
constexpr uint32_t kGeneration = 9;
constexpr uint64_t kSchema = 0x1234;

class FakeAllocatorMetadataProvider : public AllocatorMetadataProvider {
 public:
  Result<AllocatorSlotMetadata> GetSlotMetadata(
      uint64_t offset) const override {
    if (offset != slot_offset) {
      return Status::Error(StatusCode::kInvalidArgument,
                           "offset is not an allocator slot");
    }
    return AllocatorSlotMetadata{.occupied = occupied,
                                 .generation = generation,
                                 .class_id = class_id,
                                 .class_count = class_count,
                                 .capacity = capacity};
  }

  uint64_t slot_offset = 0;
  uint32_t generation = kGeneration;
  uint16_t class_id = 0;
  uint16_t class_count = 1;
  uint32_t capacity = 128;
  bool occupied = true;
};

struct OversizedType {
  uint64_t words[2];
};

struct alignas(128) OverAlignedType {
  std::byte bytes[128];
};

RegionAllocatorStorage CentralStorage(SharedMemoryRegion& region) {
  const SuperBlock& sb = *region.superblock();
  return RegionAllocatorStorage{
      .region_base = region.base(),
      .region_size = region.size(),
      .allocator_offset = sb.allocator_offset,
      .allocator_size = sb.data_offset - sb.allocator_offset,
      .data_offset = sb.data_offset,
      .data_size = sb.data_size,
      .region_id = sb.region_id,
  };
}

ClassTableConfig ResolverClassConfig() {
  ClassTableConfig config;
  config.classes = {{.slot_size = 128, .slot_count = 8}};
  return config;
}

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
    header_->class_id = 0;
    header_->generation.store(kGeneration, std::memory_order_relaxed);
    header_->capacity = allocator_.capacity;
    header_->object_size = sizeof(uint64_t);
    header_->type_id = kType.value;
    header_->layout_version = 1;
    header_->schema_short_id = kSchema;
    header_->owner_epoch.store(ProcessIdentity::Current().process_epoch,
                               std::memory_order_relaxed);
    header_->allocation_transaction_id.store(0, std::memory_order_relaxed);
    header_->allocation_role.store(0, std::memory_order_relaxed);
    header_->immutable_header_crc = ComputeImmutableHeaderCrc(*header_);
    header_->object_state.store(static_cast<uint32_t>(state),
                                std::memory_order_release);
  }

  void SealHeader() {
    header_->immutable_header_crc = ComputeImmutableHeaderCrc(*header_);
  }

  SharedMemoryRegion& region() { return region_result_.value(); }
  HandleResolver resolver() { return HandleResolver(region(), allocator_); }

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
  header_->owner_epoch.fetch_add(1, std::memory_order_relaxed);
  SealHeader();
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

TEST_F(HandleResolverTest, RequiresAuthoritativeAllocatorMetadata) {
  HandleResolver r(
      region(), static_cast<const AllocatorMetadataProvider*>(nullptr));
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kInvalidArgument);
}

TEST_F(HandleResolverTest, RejectsRegionIdentityOrEpochChange) {
  auto r = resolver();
  SuperBlock* sb = region().superblock();

  const uint64_t uuid_lo = sb->region_uuid_lo;
  sb->region_uuid_lo ^= 1;
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kUnavailable);
  sb->region_uuid_lo = uuid_lo;

  const uint64_t epoch = LoadRegionEpoch(*sb);
  StoreRegionEpoch(*sb, epoch + 1);
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kUnavailable);
  StoreRegionEpoch(*sb, epoch);
}

TEST_F(HandleResolverTest, RejectsInvalidHeaderVersionClassCapacityAndCrc) {
  auto r = resolver();

  ++header_->header_version;
  SealHeader();
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kUnsupported);

  ResetHeader(ObjectState::kPublished);
  header_->class_id = 1;
  SealHeader();
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kCorruption);

  ResetHeader(ObjectState::kPublished);
  allocator_.class_id = 1;
  allocator_.class_count = 1;
  header_->class_id = 1;
  SealHeader();
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kCorruption);
  allocator_.class_id = 0;
  allocator_.class_count = 1;

  ResetHeader(ObjectState::kPublished);
  header_->capacity /= 2;
  SealHeader();
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kCorruption);

  ResetHeader(ObjectState::kPublished);
  header_->immutable_header_crc ^= 1;
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kCorruption);
}

TEST_F(HandleResolverTest, RejectsIncompatibleCppTypeSizeAndAlignment) {
  auto r = resolver();
  EXPECT_EQ(r.Resolve<OversizedType>(handle_, kType).status().code(),
            StatusCode::kSchemaMismatch);

  header_->object_size = sizeof(OverAlignedType);
  SealHeader();
  EXPECT_EQ(r.Resolve<OverAlignedType>(handle_, kType).status().code(),
            StatusCode::kSchemaMismatch);
}

TEST_F(HandleResolverTest, MutableResolveRejectsReadOnlyRegion) {
  ResetHeader(ObjectState::kAllocated);
  RegionAttachOptions options;
  options.name = name_;
  options.region_id = region().region_id();
  options.read_only = true;
  auto attached = SharedMemoryRegion::Attach(options);
  ASSERT_TRUE(attached.ok()) << attached.status().ToString();

  HandleResolver read_only_resolver(attached.value(), allocator_);
  EXPECT_EQ(read_only_resolver.ResolveMutable<uint64_t>(handle_, kType)
                .status()
                .code(),
            StatusCode::kPermissionDenied);
}

TEST_F(HandleResolverTest, ResolvesThroughRealCentralSlabProvider) {
  SuperBlock& sb = *region().superblock();
  std::memset(region().base() + sb.data_offset, 0, sb.data_size);
  auto allocator = CentralSlabAllocator::CreateInRegion(
      CentralStorage(region()), ResolverClassConfig());
  ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

  AllocationRequest request;
  request.object_size = sizeof(uint64_t);
  request.type_id = kType;
  request.schema = {.short_id = kSchema, .layout_version = 1};
  request.alignment = alignof(uint64_t);
  request.owner_epoch = ProcessIdentity::Current().process_epoch;
  request.allocation_transaction_id = 1;
  auto handle = allocator->Allocate(request);
  ASSERT_TRUE(handle.ok()) << handle.status().ToString();
  auto build = allocator->BeginBuild(*handle);
  ASSERT_TRUE(build.ok()) << build.status().ToString();
  *static_cast<uint64_t*>(build->data) = 0xA5A55A5AF00DFACEull;
  ASSERT_TRUE(allocator->Publish(*handle).ok());

  CentralSlabAllocatorMetadataProvider provider(*allocator);
  HandleResolver real_resolver(region(), provider);
  auto resolved = real_resolver.Resolve<uint64_t>(*handle, kType, kSchema);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(*resolved.value(), 0xA5A55A5AF00DFACEull);
  EXPECT_EQ(reinterpret_cast<const std::byte*>(resolved.value()),
            region().base() + handle->offset + sizeof(SlabHeader));
}

TEST_F(HandleResolverTest, ResolvesSeparatedPayloadThroughRealLargePoolProvider) {
  SuperBlock& sb = *region().superblock();
  std::memset(region().base() + sb.data_offset, 0, sb.data_size);
  LargeObjectPoolStorage storage{
      .region_base = region().base(),
      .region_size = region().size(),
      .pool_offset = sb.data_offset,
      .pool_size = sb.data_size,
      .region_id = sb.region_id,
  };
  auto pool = LargeObjectPool::Create(storage, 256u * 1024u, 64u * 1024u);
  ASSERT_TRUE(pool.ok()) << pool.status().ToString();
  auto handle = pool->Allocate(100u * 1024u, kType);
  ASSERT_TRUE(handle.ok()) << handle.status().ToString();
  auto plan = pool->InspectPlan(*handle);
  ASSERT_TRUE(plan.ok()) << plan.status().ToString();
  ASSERT_EQ(plan->segments.size(), 2u);
  auto* payload = reinterpret_cast<uint64_t*>(
      region().base() + plan->segments[0].payload_offset);
  *payload = 0x123456789ABCDEF0ull;
  ASSERT_TRUE(pool->Publish(*handle).ok());

  LargeObjectPoolMetadataProvider provider(*pool);
  auto metadata = provider.GetSlotMetadata(handle->offset);
  ASSERT_TRUE(metadata.ok()) << metadata.status().ToString();
  EXPECT_EQ(metadata->class_count, 0u);
  EXPECT_EQ(metadata->object_kind, AllocatorObjectKind::kSegmented);
  EXPECT_EQ(metadata->payload_offset, plan->segments[0].payload_offset);
  EXPECT_EQ(metadata->object_extent, 2u * 64u * 1024u);

  HandleResolver large_resolver(region(), provider);
  auto resolved = large_resolver.Resolve<uint64_t>(*handle, kType);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(resolved.value(), payload);
  EXPECT_EQ(*resolved.value(), 0x123456789ABCDEF0ull);
  EXPECT_NE(reinterpret_cast<const std::byte*>(resolved.value()),
            region().base() + handle->offset + sizeof(SlabHeader));
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

  allocator_.capacity =
      static_cast<uint32_t>(region().superblock()->data_size);
  header_->capacity = allocator_.capacity;
  SealHeader();
  EXPECT_EQ(r.Resolve<uint64_t>(handle_, kType).status().code(),
            StatusCode::kCorruption);
}

}  // namespace
}  // namespace mino
