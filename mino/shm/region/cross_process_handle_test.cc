// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

// INV-03: a ShmHandle must resolve correctly in a process that maps the
// Region at a different virtual base address. The parent creates a Region
// and publishes one object; the child inherits the parent's mapping, then
// occupies that address range with a PROT_NONE placeholder (MAP_FIXED
// implicitly unmaps the inherited range) so its own Attach is forced onto a
// different base. Resolution must therefore depend only on the relative
// Offset encoded in the Handle, never on the mapping address.
//
// The child must not use gtest assertions (forked gtest state is
// unreliable); it reports the failing stage via its _exit code.

#include "mino/shm/region/handle_resolver.h"

#include <cstdint>
#include <string>

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/central_slab.h"

namespace mino {
namespace {

constexpr TypeId kType{17};
constexpr uint64_t kSchema = 0x1234;
constexpr uint64_t kPayloadMagic = 0xDEADBEEFCAFEF00Dull;

// Child exit codes identifying the failing stage.
constexpr int kExitRemapFailed = 1;       // Placeholder mmap or Attach failed.
constexpr int kExitSameBaseAddress = 2;   // Attach reused the parent's base.
constexpr int kExitResolutionFailed = 3;  // Resolve/payload/stale check failed.

RegionAllocatorStorage AllocatorStorage(SharedMemoryRegion& region) {
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

ClassTableConfig CrossProcessClassConfig() {
  ClassTableConfig config;
  config.classes = {{.slot_size = 128, .slot_count = 8}};
  return config;
}

// Child body. Never returns; reports success as _exit(0).
[[noreturn]] void ChildMain(const std::string& name, uint32_t region_id,
                            std::byte* old_base, uint64_t region_size,
                            ShmHandle handle) {
  // Occupy the inherited mapping's address: MAP_FIXED implicitly unmaps the
  // inherited range, so the subsequent Attach must be placed elsewhere.
  const void* placeholder =
      ::mmap(old_base, region_size, PROT_NONE,
             MAP_FIXED | MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if (placeholder != old_base) {
    ::_exit(kExitRemapFailed);
  }

  // The parent is still a live ACTIVE service. A writable attach cannot prove
  // quiescence and is fenced; this resolver-only child uses a read-only attach,
  // which never runs destructive recovery.
  RegionAttachOptions options;
  options.name = name;
  options.region_id = region_id;
  options.read_only = true;
  auto attached = SharedMemoryRegion::Attach(options);
  if (!attached.ok()) {
    ::_exit(kExitRemapFailed);
  }
  SharedMemoryRegion& child_region = attached.value();
  if (child_region.base() == old_base) {
    ::_exit(kExitSameBaseAddress);
  }

  auto allocator =
      CentralSlabAllocator::AttachInRegion(AllocatorStorage(child_region));
  if (!allocator.ok()) {
    ::_exit(kExitResolutionFailed);
  }
  CentralSlabAllocatorMetadataProvider provider(*allocator);
  HandleResolver resolver(child_region, provider);

  auto resolved = resolver.Resolve<uint64_t>(handle, kType, kSchema);
  if (!resolved.ok()) {
    ::_exit(kExitResolutionFailed);
  }
  const uint64_t* payload = resolved.value();
  auto inspected = allocator->Inspect(handle);
  if (!inspected.ok() || payload != inspected->data ||
      *payload != kPayloadMagic) {
    ::_exit(kExitResolutionFailed);
  }

  // The child mapping is PROT_READ. Mutable resolution must reject it before
  // it could ever expose a writable pointer into that mapping.
  auto mutable_result = resolver.ResolveMutable<uint64_t>(handle, kType, kSchema);
  if (mutable_result.ok() ||
      mutable_result.status().code() != StatusCode::kPermissionDenied) {
    ::_exit(kExitResolutionFailed);
  }

  // A stale handle (recycled generation) must be rejected identically from
  // the other process's mapping.
  ShmHandle stale = handle;
  ++stale.generation;
  auto stale_result = resolver.Resolve<uint64_t>(stale, kType, kSchema);
  if (stale_result.ok() ||
      stale_result.status().code() != StatusCode::kNotFound) {
    ::_exit(kExitResolutionFailed);
  }

  ::_exit(0);
}

class CrossProcessHandleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // macOS limits POSIX shm names to 31 characters.
    name_ = "/xp_" + std::to_string(::getpid());
    ASSERT_LE(name_.size(), 31u);
  }

  void TearDown() override { SharedMemorySegment::Unlink(name_); }

  void WaitChild(pid_t pid) {
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    EXPECT_TRUE(WIFEXITED(status)) << "child did not exit normally";
    if (WIFEXITED(status)) {
      EXPECT_EQ(WEXITSTATUS(status), 0) << "child reported failure";
    }
  }

  std::string name_;
};

TEST_F(CrossProcessHandleTest,
       ResolvesWithDifferentBaseAddressAcrossProcesses) {
  RegionCreateOptions options;
  options.name = name_;
  options.size_bytes = 1024 * 1024;
  auto created = SharedMemoryRegion::Create(options);
  ASSERT_TRUE(created.ok()) << created.status().ToString();
  SharedMemoryRegion& region = created.value();

  auto allocator = CentralSlabAllocator::CreateInRegion(
      AllocatorStorage(region), CrossProcessClassConfig());
  ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
  AllocationRequest request;
  request.object_size = sizeof(uint64_t);
  request.type_id = kType;
  request.schema = {.short_id = kSchema, .layout_version = 1};
  request.alignment = alignof(uint64_t);
  auto handle_result = allocator->Allocate(request);
  ASSERT_TRUE(handle_result.ok()) << handle_result.status().ToString();
  const ShmHandle handle = *handle_result;
  auto build = allocator->BeginBuild(handle);
  ASSERT_TRUE(build.ok()) << build.status().ToString();
  *static_cast<uint64_t*>(build->data) = kPayloadMagic;
  ASSERT_TRUE(allocator->Publish(handle).ok());

  const uint32_t region_id = region.region_id();

  // Sanity-check the real allocator/provider path in the parent before forking.
  CentralSlabAllocatorMetadataProvider provider(*allocator);
  HandleResolver parent_resolver(region, provider);
  auto parent_check =
      parent_resolver.Resolve<uint64_t>(handle, kType, kSchema);
  ASSERT_TRUE(parent_check.ok()) << parent_check.status().ToString();

  std::byte* const old_base = region.base();
  const uint64_t region_size = region.size();

  const pid_t pid = ::fork();
  ASSERT_NE(pid, -1) << "fork failed";
  if (pid == 0) {
    ChildMain(name_, region_id, old_base, region_size, handle);
  }
  WaitChild(pid);
}

}  // namespace
}  // namespace mino
