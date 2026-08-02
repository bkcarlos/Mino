// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/platform/shared_memory.h"
#include "mino/platform/shared_memory_marker.h"

#include <cerrno>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#if defined(__linux__)
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/region/handle_resolver.h"
#include "mino/shm/region/region.h"

namespace mino {
namespace {

#if defined(__linux__)

constexpr TypeId kType{71};
constexpr uint64_t kSchema = 0x4855474550414745ull;
constexpr uint64_t kPayload = 0x4D494E4F48554745ull;

using shared_memory_internal::MarkerPayloadCrc32;
using shared_memory_internal::MarkerState;
using shared_memory_internal::SharedMemoryMarkerPayload;
using shared_memory_internal::SharedMemoryMarkerRecord;

struct SmapsBacking {
    uint64_t kernel_page_size = 0;
    uint64_t mmu_page_size = 0;
    uint64_t hugetlb_bytes = 0;
    bool hugetlb_vm_flag = false;
};

uint64_t ParseKilobytes(const std::string& line) {
    std::istringstream input(line.substr(line.find(':') + 1));
    uint64_t kilobytes = 0;
    input >> kilobytes;
    return kilobytes * 1024;
}

Result<SmapsBacking> ReadSmapsBacking(const void* address) {
    std::ifstream smaps("/proc/self/smaps");
    if (!smaps) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot open /proc/self/smaps");
    }
    const uint64_t target = reinterpret_cast<uintptr_t>(address);
    bool in_mapping = false;
    SmapsBacking backing;
    std::string line;
    while (std::getline(smaps, line)) {
        unsigned long long begin = 0;
        unsigned long long end = 0;
        if (std::sscanf(line.c_str(), "%llx-%llx", &begin, &end) == 2) {
            if (in_mapping) break;
            in_mapping = target >= begin && target < end;
            continue;
        }
        if (!in_mapping) continue;
        if (line.rfind("KernelPageSize:", 0) == 0) {
            backing.kernel_page_size = ParseKilobytes(line);
        } else if (line.rfind("MMUPageSize:", 0) == 0) {
            backing.mmu_page_size = ParseKilobytes(line);
        } else if (line.rfind("Shared_Hugetlb:", 0) == 0 ||
                   line.rfind("Private_Hugetlb:", 0) == 0) {
            backing.hugetlb_bytes += ParseKilobytes(line);
        } else if (line.rfind("VmFlags:", 0) == 0) {
            std::istringstream flags(line.substr(line.find(':') + 1));
            std::string flag;
            while (flags >> flag) {
                if (flag == "ht") backing.hugetlb_vm_flag = true;
            }
        }
    }
    if (!in_mapping) {
        return Status::Error(StatusCode::kNotFound,
                             "mapping not found in /proc/self/smaps");
    }
    return backing;
}

void FaultEveryPage(void* base, uint64_t size, uint64_t page_size) {
    auto* bytes = static_cast<volatile unsigned char*>(base);
    for (uint64_t offset = 0; offset < size; offset += page_size) {
        bytes[offset] = static_cast<unsigned char>(offset / page_size + 1);
    }
}

void AssertActualHugetlb(void* base, uint64_t size, uint64_t page_size) {
    FaultEveryPage(base, size, page_size);
    auto backing = ReadSmapsBacking(base);
    ASSERT_TRUE(backing.ok()) << backing.status().ToString();
    EXPECT_GT(backing->kernel_page_size, 4096u);
    EXPECT_GT(backing->mmu_page_size, 4096u);
    EXPECT_TRUE(backing->hugetlb_vm_flag);
    EXPECT_GE(backing->hugetlb_bytes, page_size);
}

std::string UniqueName(const char* tag) {
    static uint32_t sequence = 0;
    return std::string("/mh_") + std::to_string(::getpid()) + "_" +
           std::to_string(++sequence) + "_" + tag;
}

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

Result<SharedMemoryMarkerPayload> ReadMarker(const std::string& name) {
    int fd = ::shm_open(name.c_str(), O_RDONLY, 0);
    if (fd < 0) {
        return Status::Error(StatusCode::kNotFound, "marker not found");
    }
    SharedMemoryMarkerRecord marker;
    const ssize_t count = ::pread(fd, &marker, sizeof(marker), 0);
    ::close(fd);
    if (count != static_cast<ssize_t>(sizeof(marker))) {
        return Status::Error(StatusCode::kCorruption, "short marker");
    }
    const auto& slot = marker.slots[marker.published_word & 1u];
    if (slot.payload_crc32 != MarkerPayloadCrc32(slot.payload)) {
        return Status::Error(StatusCode::kCorruption, "invalid marker");
    }
    return slot.payload;
}

std::string MarkerString(const char* value, size_t capacity) {
    const void* end = std::memchr(value, '\0', capacity);
    if (end == nullptr) return {};
    return std::string(value, static_cast<const char*>(end) - value);
}

ClassTableConfig HugePageClassConfig() {
    ClassTableConfig config;
    config.classes = {{.slot_size = 128, .slot_count = 8}};
    return config;
}

class HugePageValidationTest : public ::testing::Test {
protected:
    void TearDown() override {
        for (const std::string& name : names_) {
            (void)SharedMemorySegment::Unlink(name);
        }
    }

    std::string Name(const char* tag) {
        std::string name = UniqueName(tag);
        names_.push_back(name);
        return name;
    }

    Result<SharedMemorySegment> CreateHuge(const std::string& name,
                                           uint64_t size) {
        SharedMemoryCreateOptions options;
        options.name = name;
        options.size = size;
        options.use_huge_pages = true;
        return SharedMemorySegment::Create(options);
    }

    std::vector<std::string> names_;
};

TEST_F(HugePageValidationTest, MapHugetlbSmapsAndCrossProcessOpenReadWrite) {
    const std::string name = Name("map");
    auto created = CreateHuge(name, 2ull * 1024 * 1024);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    if (!created->huge_pages_actual()) {
        GTEST_SKIP() << "host has no usable reserved huge pages: reason="
                     << HugePageFallbackReasonName(
                            created->huge_page_fallback_reason())
                     << " errno=" << created->huge_page_fallback_errno();
    }

    EXPECT_TRUE(created->huge_pages_requested());
    EXPECT_TRUE(created->huge_pages_actual());
    EXPECT_EQ(created->huge_page_fallback_reason(),
              HugePageFallbackReason::kNone);
    ASSERT_GT(created->actual_page_size(), 4096u);
    AssertActualHugetlb(created->base(), created->size(),
                        created->actual_page_size());

    auto marker = ReadMarker(name);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    EXPECT_EQ(marker->version, shared_memory_internal::kMarkerVersion);
    EXPECT_EQ(marker->state,
              static_cast<uint32_t>(MarkerState::kHugeReady));
    EXPECT_EQ(marker->data_size, created->size());
    EXPECT_EQ(marker->page_size, created->actual_page_size());
    ASSERT_NE(marker->backing_inode, 0u);
    const std::string mount_path = MarkerString(
        marker->mount_path, sizeof(marker->mount_path));
    const std::string huge_path = MarkerString(
        marker->backing_name, sizeof(marker->backing_name));
    ASSERT_FALSE(mount_path.empty());
    ASSERT_FALSE(huge_path.empty());
    struct stat mount_stat;
    ASSERT_EQ(::stat(mount_path.c_str(), &mount_stat), 0);
    EXPECT_EQ(static_cast<uint64_t>(mount_stat.st_dev),
              marker->mount_device);
    struct stat huge_stat;
    ASSERT_EQ(::stat(huge_path.c_str(), &huge_stat), 0);
    EXPECT_EQ(static_cast<uint64_t>(huge_stat.st_dev),
              marker->backing_device);
    EXPECT_EQ(static_cast<uint64_t>(huge_stat.st_ino),
              marker->backing_inode);

    auto* bytes = static_cast<unsigned char*>(created->base());
    bytes[17] = 0x31;
    const pid_t child = ::fork();
    ASSERT_NE(child, -1) << "fork failed";
    if (child == 0) {
        auto opened = SharedMemorySegment::Open(name, /*read_only=*/false);
        if (!opened.ok() || !opened->huge_pages_actual() ||
            !opened->huge_pages_requested() ||
            opened->actual_page_size() <= 4096 ||
            static_cast<unsigned char*>(opened->base())[17] != 0x31) {
            ::_exit(11);
        }
        auto backing = ReadSmapsBacking(opened->base());
        if (!backing.ok() || !backing->hugetlb_vm_flag ||
            backing->kernel_page_size <= 4096) {
            ::_exit(12);
        }
        static_cast<unsigned char*>(opened->base())[29] = 0x72;
        ::_exit(0);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
    EXPECT_EQ(bytes[29], 0x72);

    const char* previous_mount = std::getenv("MINO_HUGETLBFS_PATH");
    const std::string saved_mount =
        previous_mount == nullptr ? "" : previous_mount;
    ASSERT_EQ(::setenv("MINO_HUGETLBFS_PATH", "/definitely/not/the/mount", 1),
              0);
    auto recorded_open = SharedMemorySegment::Open(name, /*read_only=*/true);
    ASSERT_TRUE(recorded_open.ok()) << recorded_open.status().ToString();
    EXPECT_TRUE(recorded_open->huge_pages_actual());
    EXPECT_TRUE(SharedMemorySegment::Unlink(name).ok());
    errno = 0;
    EXPECT_EQ(::stat(huge_path.c_str(), &huge_stat), -1);
    EXPECT_EQ(errno, ENOENT);
    EXPECT_EQ(bytes[17], 0x31);
    if (saved_mount.empty()) {
        (void)::unsetenv("MINO_HUGETLBFS_PATH");
    } else {
        ASSERT_EQ(::setenv("MINO_HUGETLBFS_PATH", saved_mount.c_str(), 1), 0);
    }
}

TEST_F(HugePageValidationTest, RegionAttachAndHandleResolveUseHugetlbBacking) {
    const std::string name = Name("region");
    RegionCreateOptions options;
    options.name = name;
    options.size_bytes = 4ull * 1024 * 1024;
    options.use_huge_pages = true;
    auto created = SharedMemoryRegion::Create(options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedMemoryRegion& region = *created;

    auto backing = ReadSmapsBacking(region.base());
    ASSERT_TRUE(backing.ok()) << backing.status().ToString();
    if (!backing->hugetlb_vm_flag) {
        GTEST_SKIP() << "Region huge-page request fell back; no usable reserved "
                        "huge pages on this host";
    }
    EXPECT_GT(backing->kernel_page_size, 4096u);

    auto allocator = CentralSlabAllocator::CreateInRegion(
        AllocatorStorage(region), HugePageClassConfig());
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
    *static_cast<uint64_t*>(build->data) = kPayload;
    ASSERT_TRUE(allocator->Publish(handle).ok());

    const uint32_t region_id = region.region_id();
    void* inherited_base = region.base();
    const uint64_t inherited_size = region.size();
    const pid_t child = ::fork();
    ASSERT_NE(child, -1) << "fork failed";
    if (child == 0) {
        if (::munmap(inherited_base, inherited_size) != 0) ::_exit(21);
        RegionAttachOptions attach;
        attach.name = name;
        attach.region_id = region_id;
        attach.read_only = true;
        auto attached = SharedMemoryRegion::Attach(attach);
        if (!attached.ok()) ::_exit(22);
        auto child_backing = ReadSmapsBacking(attached->base());
        if (!child_backing.ok() || !child_backing->hugetlb_vm_flag ||
            child_backing->kernel_page_size <= 4096) {
            ::_exit(23);
        }
        auto child_allocator = CentralSlabAllocator::AttachInRegion(
            AllocatorStorage(*attached));
        if (!child_allocator.ok()) ::_exit(24);
        CentralSlabAllocatorMetadataProvider provider(*child_allocator);
        HandleResolver resolver(*attached, provider);
        auto resolved = resolver.Resolve<uint64_t>(handle, kType, kSchema);
        if (!resolved.ok() || **resolved != kPayload) ::_exit(25);
        ::_exit(0);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

#else

TEST(HugePageValidationTest, RequiresLinux) {
    GTEST_SKIP() << "MAP_HUGETLB validation requires Linux";
}

#endif

}  // namespace
}  // namespace mino
