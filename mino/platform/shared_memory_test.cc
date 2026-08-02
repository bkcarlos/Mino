// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.gnu.org/licenses/lgpl-3.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include "mino/platform/shared_memory.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>

#if defined(__unix__) || defined(__APPLE__)
#define MINO_HAS_POSIX 1
#include <unistd.h>
#else
#define MINO_HAS_POSIX 0
#endif

namespace mino {
namespace {

// Generates a unique shm object name per test to avoid collisions on the
// shared /dev/shm namespace.
std::string UniqueName(const char* tag) {
    static uint32_t sequence = 0;
    // Darwin limits POSIX SHM names to 31 characters. Keep test names short
    // while retaining process-local uniqueness.
    return std::string("/mt_") + std::to_string(::getpid()) + "_" +
           std::to_string(++sequence) + "_" + tag;
}

class SharedMemoryTest : public ::testing::Test {
protected:
    void TearDown() override {
        // Best-effort cleanup of any segments this test created.
        for (const auto& n : created_) {
            SharedMemorySegment::Unlink(n);
        }
    }

    std::string MakeName(const char* tag) {
        std::string n = UniqueName(tag);
        created_.push_back(n);
        return n;
    }

    std::vector<std::string> created_;
};

TEST_F(SharedMemoryTest, CreateMapsSegment) {
    const std::string name = MakeName("create");
    auto result = SharedMemorySegment::Create(name, 8192);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    SharedMemorySegment seg = std::move(result).value();

    EXPECT_NE(seg.base(), nullptr);
    EXPECT_GE(seg.size(), 8192u);
    EXPECT_EQ(seg.name(), name);
    EXPECT_FALSE(seg.read_only());

    // Memory must be usable.
    std::memset(seg.base(), 0xAB, 4096);
    EXPECT_EQ(static_cast<unsigned char*>(seg.base())[0], 0xAB);
}

TEST_F(SharedMemoryTest, CreateExistingFailsWithAlreadyExists) {
    const std::string name = MakeName("dup");
    auto first = SharedMemorySegment::Create(name, 4096);
    ASSERT_TRUE(first.ok()) << first.status().ToString();

    auto second = SharedMemorySegment::Create(name, 4096);
    ASSERT_FALSE(second.ok());
    EXPECT_EQ(second.status().code(), StatusCode::kAlreadyExists);
}

TEST_F(SharedMemoryTest, CreateRejectsInvalidName) {
    auto r1 = SharedMemorySegment::Create("no_leading_slash", 4096);
    EXPECT_FALSE(r1.ok());
    EXPECT_EQ(r1.status().code(), StatusCode::kInvalidArgument);

    auto r2 = SharedMemorySegment::Create("/", 4096);
    EXPECT_FALSE(r2.ok());
    EXPECT_EQ(r2.status().code(), StatusCode::kInvalidArgument);

    auto r3 = SharedMemorySegment::Create("/a/b", 4096);
    EXPECT_FALSE(r3.ok());
    EXPECT_EQ(r3.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(SharedMemoryTest, CreateRejectsZeroSize) {
    const std::string name = MakeName("zero");
    auto r = SharedMemorySegment::Create(name, 0);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(SharedMemoryTest, OpenNonexistentFailsWithNotFound) {
    auto r = SharedMemorySegment::Open("/mt_absent_xyz",
                                       /*read_only=*/false);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.status().code(), StatusCode::kNotFound);
}

TEST_F(SharedMemoryTest, OpenExistingReadWrite) {
    const std::string name = MakeName("open");
    {
        auto created = SharedMemorySegment::Create(name, 4096);
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        // Write a marker through the creator's mapping.
        static_cast<char*>(created->base())[0] = 'M';
    }  // creator unmaps but does NOT unlink

    auto opened = SharedMemorySegment::Open(name, /*read_only=*/false);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    EXPECT_GE(opened->size(), 4096u);
    // Data persists across mappings of the same object.
    EXPECT_EQ(static_cast<char*>(opened->base())[0], 'M');
}

TEST_F(SharedMemoryTest, OpenReadOnly) {
    const std::string name = MakeName("ro");
    {
        auto created = SharedMemorySegment::Create(name, 4096);
        ASSERT_TRUE(created.ok());
        static_cast<char*>(created->base())[0] = 'R';
    }

    auto opened = SharedMemorySegment::Open(name, /*read_only=*/true);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    EXPECT_TRUE(opened->read_only());
    EXPECT_EQ(static_cast<const char*>(opened->base())[0], 'R');
}

TEST_F(SharedMemoryTest, CloseIsIdempotent) {
    const std::string name = MakeName("close");
    auto created = SharedMemorySegment::Create(name, 4096);
    ASSERT_TRUE(created.ok());
    SharedMemorySegment seg = std::move(created).value();

    EXPECT_TRUE(seg.Close().ok());
    EXPECT_EQ(seg.base(), nullptr);
    EXPECT_EQ(seg.size(), 0u);
    // Second close is a no-op OK.
    EXPECT_TRUE(seg.Close().ok());
}

TEST_F(SharedMemoryTest, UnlinkRemovesObject) {
    const std::string name = MakeName("unlink");
    {
        auto created = SharedMemorySegment::Create(name, 4096);
        ASSERT_TRUE(created.ok());
    }
    EXPECT_TRUE(SharedMemorySegment::Unlink(name).ok());
    // Now it must not exist.
    auto opened = SharedMemorySegment::Open(name, /*read_only=*/false);
    EXPECT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), StatusCode::kNotFound);
}

TEST_F(SharedMemoryTest, UnlinkNonexistentFailsWithNotFound) {
    Status s = SharedMemorySegment::Unlink("/mino_test_absent_unlink_xyz");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kNotFound);
}

TEST_F(SharedMemoryTest, MoveTransfersOwnership) {
    const std::string name = MakeName("move");
    auto created = SharedMemorySegment::Create(name, 4096);
    ASSERT_TRUE(created.ok());

    SharedMemorySegment a = std::move(created).value();
    void* base = a.base();
    SharedMemorySegment b = std::move(a);

    EXPECT_EQ(a.base(), nullptr);  // moved-from is empty
    EXPECT_EQ(b.base(), base);
    EXPECT_GE(b.size(), 4096u);
}

TEST_F(SharedMemoryTest, HugePageFallbackReportsRequestedActualAndReason) {
    // Force a deterministic fallback even on a host with reserved huge pages.
    const std::string name = MakeName("huge");
    SharedMemoryCreateOptions options;
    options.name = name;
    options.size = 2 * 1024 * 1024;
    options.use_huge_pages = true;
    options.hugetlbfs_path = "/mino/nonexistent/hugetlbfs";

    auto created = SharedMemorySegment::Create(options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    EXPECT_NE(created->base(), nullptr);
    EXPECT_GE(created->size(), 2u * 1024 * 1024);
    EXPECT_TRUE(created->huge_pages_requested());
    EXPECT_FALSE(created->huge_pages_actual());
    EXPECT_FALSE(created->huge_page_enabled());
#if defined(__linux__)
    EXPECT_EQ(created->huge_page_fallback_reason(),
              HugePageFallbackReason::kHugetlbfsUnavailable);
    EXPECT_NE(created->huge_page_fallback_errno(), 0);
#else
    EXPECT_EQ(created->huge_page_fallback_reason(),
              HugePageFallbackReason::kUnsupportedPlatform);
#endif
    EXPECT_LT(created->actual_page_size(), 2u * 1024 * 1024);
    std::memset(created->base(), 1, 4096);

    auto opened = SharedMemorySegment::Open(name, /*read_only=*/true);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    EXPECT_TRUE(opened->huge_pages_requested());
    EXPECT_FALSE(opened->huge_pages_actual());
    EXPECT_EQ(opened->huge_page_fallback_reason(),
              created->huge_page_fallback_reason());
    EXPECT_EQ(static_cast<const unsigned char*>(opened->base())[0], 1u);
}

TEST_F(SharedMemoryTest, OrdinaryMappingReportsNoHugePageRequest) {
    const std::string name = MakeName("normal");
    auto created = SharedMemorySegment::Create(name, 4096);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    EXPECT_FALSE(created->huge_pages_requested());
    EXPECT_FALSE(created->huge_pages_actual());
    EXPECT_EQ(created->huge_page_fallback_reason(),
              HugePageFallbackReason::kNone);
    EXPECT_EQ(created->huge_page_fallback_errno(), 0);
    EXPECT_GT(created->actual_page_size(), 0u);
}

}  // namespace
}  // namespace mino
