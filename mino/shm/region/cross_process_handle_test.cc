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
#include <new>
#include <string>

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino {
namespace {

constexpr TypeId kType{17};
constexpr uint32_t kGeneration = 9;
constexpr uint64_t kSchema = 0x1234;
constexpr uint64_t kPayloadMagic = 0xDEADBEEFCAFEF00Dull;

// Child exit codes identifying the failing stage.
constexpr int kExitRemapFailed = 1;       // Placeholder mmap or Attach failed.
constexpr int kExitSameBaseAddress = 2;   // Attach reused the parent's base.
constexpr int kExitResolutionFailed = 3;  // Resolve/payload/stale check failed.

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

// Child body. Never returns; reports success as _exit(0).
[[noreturn]] void ChildMain(const std::string& name, uint32_t region_id,
                            std::byte* old_base, uint64_t region_size,
                            uint64_t slot_offset) {
  // Occupy the inherited mapping's address: MAP_FIXED implicitly unmaps the
  // inherited range, so the subsequent Attach must be placed elsewhere.
  const void* placeholder =
      ::mmap(old_base, region_size, PROT_NONE,
             MAP_FIXED | MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if (placeholder != old_base) {
    ::_exit(kExitRemapFailed);
  }

  // The parent left the Region ACTIVE without a clean shutdown; Attach takes
  // the dirty path of RecoverRegionForAttach, which acquires the (unset)
  // recovery lease without waiting and re-publishes the Region ACTIVE.
  RegionAttachOptions options;
  options.name = name;
  options.region_id = region_id;
  auto attached = SharedMemoryRegion::Attach(options);
  if (!attached.ok()) {
    ::_exit(kExitRemapFailed);
  }
  SharedMemoryRegion& child_region = attached.value();
  if (child_region.base() == old_base) {
    ::_exit(kExitSameBaseAddress);
  }

  FakeAllocatorMetadataProvider allocator;
  allocator.slot_offset = slot_offset;
  HandleResolver resolver(child_region, &allocator);

  const ShmHandle handle{slot_offset, kGeneration, region_id};
  auto resolved = resolver.Resolve<uint64_t>(handle, kType, kSchema);
  if (!resolved.ok()) {
    ::_exit(kExitResolutionFailed);
  }
  const uint64_t* payload = resolved.value();
  const auto* expected = reinterpret_cast<const uint64_t*>(
      child_region.base() + handle.offset + sizeof(SlabHeader));
  if (payload != expected || *payload != kPayloadMagic) {
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

  const uint64_t slot_offset = region.superblock()->data_offset;
  auto* header = new (region.base() + slot_offset) SlabHeader{};
  header->magic = kSlabHeaderMagic;
  header->header_version = kSlabHeaderVersion;
  header->generation = kGeneration;
  header->object_state.store(static_cast<uint32_t>(ObjectState::kPublished));
  header->capacity = 128;
  header->object_size = sizeof(uint64_t);
  header->type_id = kType.value;
  header->schema_short_id = kSchema;
  header->owner_epoch = ProcessIdentity::Current().process_epoch;

  auto* payload = reinterpret_cast<uint64_t*>(region.base() + slot_offset +
                                              sizeof(SlabHeader));
  *payload = kPayloadMagic;

  const uint32_t region_id = region.region_id();
  const ShmHandle handle{slot_offset, kGeneration, region_id};

  // Sanity-check the fixture in the parent before forking.
  FakeAllocatorMetadataProvider allocator;
  allocator.slot_offset = slot_offset;
  HandleResolver parent_resolver(region, &allocator);
  auto parent_check =
      parent_resolver.Resolve<uint64_t>(handle, kType, kSchema);
  ASSERT_TRUE(parent_check.ok()) << parent_check.status().ToString();

  std::byte* const old_base = region.base();
  const uint64_t region_size = region.size();

  const pid_t pid = ::fork();
  ASSERT_NE(pid, -1) << "fork failed";
  if (pid == 0) {
    ChildMain(name_, region_id, old_base, region_size, slot_offset);
  }
  WaitChild(pid);
}

}  // namespace
}  // namespace mino
