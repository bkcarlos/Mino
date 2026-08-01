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

#include "mino/shm/allocator/large_object_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <thread>
#include <vector>

#include "mino/abi/shm_handle.h"
#include "mino/common/status.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino {
namespace {

constexpr uint64_t kPoolSize = 1u << 20;         // 1 MiB
constexpr uint32_t kMaxObject = 256u * 1024u;    // 256 KiB
constexpr uint32_t kSegmentSize = 64u * 1024u;   // 64 KiB

class LargeObjectPoolTest : public ::testing::Test {
protected:
    // 64-byte-aligned allocation: LargePoolSuperblock placed at the base
    // carries cache-line-aligned atomics (UBSAN rejects placement-new on a
    // merely 16-aligned heap pointer, which is what glibc malloc returns).
    struct AlignedDeleter {
        void operator()(std::byte* p) const {
            ::operator delete[](p, std::align_val_t(64));
        }
    };

    std::unique_ptr<std::byte[], AlignedDeleter> region_;
    LargeObjectPool pool_;

    void SetUp() override {
        region_.reset(new (std::align_val_t(64)) std::byte[kPoolSize]);
        std::memset(region_.get(), 0, kPoolSize);
        auto result = LargeObjectPool::Create(region_.get(), kPoolSize,
                                              kMaxObject, kSegmentSize);
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        pool_ = result.value();
    }
};

TEST_F(LargeObjectPoolTest, CreateRejectsNullBase) {
    auto result = LargeObjectPool::Create(nullptr, kPoolSize, kMaxObject, kSegmentSize);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(LargeObjectPoolTest, CreateRejectsZeroMaxObject) {
    auto result = LargeObjectPool::Create(region_.get(), kPoolSize, 0, kSegmentSize);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(LargeObjectPoolTest, CreateRejectsMisalignedSegmentSize) {
    auto result = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject, 100);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(LargeObjectPoolTest, CreateRejectsPoolTooSmallForOneMaxObject) {
    // Pool barely bigger than metadata: cannot fit a 256 KiB object.
    auto result = LargeObjectPool::Create(region_.get(), 128 * 1024, kMaxObject,
                                          kSegmentSize);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kResourceExhausted);
}

TEST_F(LargeObjectPoolTest, ConfigurationIsReported) {
    EXPECT_EQ(pool_.pool_size(), kPoolSize);
    EXPECT_EQ(pool_.max_object_size(), kMaxObject);
    EXPECT_EQ(pool_.segment_size(), kSegmentSize);
    // 1 MiB pool, 64 KiB segments, small metadata: expect ~15 segments.
    EXPECT_GE(pool_.segment_count(), 4u);
    EXPECT_LT(pool_.segment_count(), 16u);
}

TEST_F(LargeObjectPoolTest, AllocateRejectsZeroSize) {
    auto handle = pool_.Allocate(0, LargeObjectTypeId{1});
    ASSERT_FALSE(handle.ok());
    EXPECT_EQ(handle.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(LargeObjectPoolTest, AllocateRejectsOversizedObject) {
    auto handle = pool_.Allocate(kMaxObject + 1, LargeObjectTypeId{1});
    ASSERT_FALSE(handle.ok());
    EXPECT_EQ(handle.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(LargeObjectPoolTest, SingleSegmentObjectHasOneSegmentPlan) {
    auto handle = pool_.Allocate(1000, LargeObjectTypeId{42});
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();
    EXPECT_EQ(handle->generation, 1u);

    auto plan = pool_.InspectPlan(*handle);
    ASSERT_TRUE(plan.ok()) << plan.status().ToString();
    EXPECT_EQ(plan->object_size, 1000u);
    EXPECT_EQ(plan->type_id, LargeObjectTypeId{42u});
    ASSERT_EQ(plan->segments.size(), 1u);
    EXPECT_EQ(plan->segments[0].segment_size, 1000u);
}

TEST_F(LargeObjectPoolTest, MultiSegmentObjectSpansSegments) {
    // 100 KiB over 64 KiB segments -> 2 segments.
    auto handle = pool_.Allocate(100 * 1024, LargeObjectTypeId{7});
    ASSERT_TRUE(handle.ok());

    auto plan = pool_.InspectPlan(*handle);
    ASSERT_TRUE(plan.ok());
    ASSERT_EQ(plan->segments.size(), 2u);
    EXPECT_EQ(plan->segments[0].segment_size, kSegmentSize);
    EXPECT_EQ(plan->segments[1].segment_size, 100 * 1024 - kSegmentSize);
    // Segments are consecutive.
    EXPECT_EQ(plan->segments[1].segment_index,
              plan->segments[0].segment_index + 1);
}

TEST_F(LargeObjectPoolTest,
       SlotMetadataReportsSeparatedPayloadAndWholeSegmentedExtent) {
    auto handle = pool_.Allocate(100 * 1024, TypeId{77});
    ASSERT_TRUE(handle.ok());
    auto plan = pool_.InspectPlan(*handle);
    ASSERT_TRUE(plan.ok());
    ASSERT_EQ(plan->segments.size(), 2u);

    auto metadata = pool_.GetSlotMetadata(handle->offset);
    ASSERT_TRUE(metadata.ok()) << metadata.status().ToString();
    EXPECT_TRUE(metadata->occupied);
    EXPECT_TRUE(metadata->segmented);
    EXPECT_EQ(metadata->generation, handle->generation);
    EXPECT_EQ(metadata->capacity, kSegmentSize);
    EXPECT_EQ(metadata->payload_offset, plan->segments[0].payload_offset);
    EXPECT_EQ(metadata->object_extent, 2u * kSegmentSize);
    EXPECT_NE(metadata->payload_offset,
              handle->offset + sizeof(SlabHeader));

    EXPECT_EQ(pool_.GetSlotMetadata(handle->offset + sizeof(SlabHeader))
                  .status()
                  .code(),
              StatusCode::kCorruption);
    EXPECT_EQ(pool_.GetSlotMetadata(std::numeric_limits<uint64_t>::max())
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
}

TEST_F(LargeObjectPoolTest, MaxObjectFitsAndUsesAllNeededSegments) {
    auto handle = pool_.Allocate(kMaxObject, LargeObjectTypeId{1});
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();

    auto plan = pool_.InspectPlan(*handle);
    ASSERT_TRUE(plan.ok());
    EXPECT_EQ(plan->segments.size(), kMaxObject / kSegmentSize);
}

TEST_F(LargeObjectPoolTest, GenerationBeforeSentinelMarksPoolDraining) {
    const uint32_t bitmap_words = (pool_.segment_count() + 63u) / 64u;
    const uint64_t generations_offset =
        64u + static_cast<uint64_t>(bitmap_words) * sizeof(std::atomic<uint64_t>);
    auto* generations = reinterpret_cast<std::atomic<uint32_t>*>(
        region_.get() + generations_offset);
    generations[0].store(std::numeric_limits<uint32_t>::max() - 1u,
                         std::memory_order_release);

    auto handle = pool_.Allocate(1024, TypeId{99});
    ASSERT_FALSE(handle.ok());
    EXPECT_EQ(handle.status().code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(pool_.is_draining());
    EXPECT_EQ(pool_.AuthoritativeGenerationForRecovery(0),
              std::numeric_limits<uint32_t>::max());
    EXPECT_FALSE(pool_.IsSegmentOccupiedForRecovery(0));

    auto attached = LargeObjectPool::Attach(region_.get(), kPoolSize);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_TRUE(attached->is_draining());
    EXPECT_EQ(attached->Allocate(1024, TypeId{100}).status().code(),
              StatusCode::kUnavailable);
}

TEST_F(LargeObjectPoolTest, PoolExhaustionReturnsResourceExhausted) {
    // Fill the pool with max-size objects until it refuses.
    int allocated = 0;
    for (;; ++allocated) {
        auto handle = pool_.Allocate(kMaxObject, LargeObjectTypeId{1});
        if (!handle.ok()) {
            EXPECT_EQ(handle.status().code(), StatusCode::kResourceExhausted);
            break;
        }
    }
    EXPECT_GE(allocated, 1);
    EXPECT_LT(allocated, 10);
}

TEST_F(LargeObjectPoolTest, RetireAndReclaimFreeSegments) {
    auto first = pool_.Allocate(100 * 1024, LargeObjectTypeId{1});
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(pool_.Retire(*first).ok());
    ASSERT_TRUE(pool_.Reclaim(*first).ok());

    // The same segments are reusable; generation moves on.
    auto second = pool_.Allocate(100 * 1024, LargeObjectTypeId{1});
    ASSERT_TRUE(second.ok());
    EXPECT_EQ(second->offset, first->offset);
    EXPECT_EQ(second->generation, first->generation + 1);
}

TEST_F(LargeObjectPoolTest, ReclaimRejectsDoubleReclaim) {
    auto handle = pool_.Allocate(1000, LargeObjectTypeId{1});
    ASSERT_TRUE(handle.ok());
    ASSERT_TRUE(pool_.Retire(*handle).ok());
    ASSERT_TRUE(pool_.Reclaim(*handle).ok());

    const Status again = pool_.Reclaim(*handle);
    ASSERT_FALSE(again.ok());
    EXPECT_EQ(again.code(), StatusCode::kNotFound);
}

TEST_F(LargeObjectPoolTest, InspectRejectsStaleHandle) {
    auto handle = pool_.Allocate(1000, LargeObjectTypeId{1});
    ASSERT_TRUE(handle.ok());
    ASSERT_TRUE(pool_.Retire(*handle).ok());
    ASSERT_TRUE(pool_.Reclaim(*handle).ok());

    auto plan = pool_.InspectPlan(*handle);
    ASSERT_FALSE(plan.ok());
    EXPECT_EQ(plan.status().code(), StatusCode::kNotFound);
}

TEST_F(LargeObjectPoolTest, InspectRejectsNullHandle) {
    auto plan = pool_.InspectPlan(ShmHandle{});
    ASSERT_FALSE(plan.ok());
    EXPECT_EQ(plan.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(LargeObjectPoolTest, PlanValidationDetectsCorruptDerivedSegmentCount) {
    auto handle = pool_.Allocate(100 * 1024, LargeObjectTypeId{1});
    ASSERT_TRUE(handle.ok());

    // Make the CRC-valid object size imply a segment run beyond pool bounds.
    auto* header = reinterpret_cast<SlabHeader*>(region_.get() + handle->offset);
    header->object_size = std::numeric_limits<uint32_t>::max();
    header->immutable_header_crc = ComputeImmutableHeaderCrc(*header);

    auto plan = pool_.InspectPlan(*handle);
    ASSERT_FALSE(plan.ok());
    EXPECT_EQ(plan.status().code(), StatusCode::kCorruption);
}

TEST_F(LargeObjectPoolTest, AttachSeesExistingObjects) {
    auto handle = pool_.Allocate(100 * 1024, TypeId{9});
    ASSERT_TRUE(handle.ok());

    auto attached = LargeObjectPool::Attach(region_.get(), kPoolSize);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();

    auto plan = attached->InspectPlan(*handle);
    ASSERT_TRUE(plan.ok());
    EXPECT_EQ(plan->segments.size(), 2u);
    EXPECT_EQ(plan->type_id, LargeObjectTypeId{9u});
}

TEST_F(LargeObjectPoolTest, AttachRejectsCorruptMagic) {
    std::memset(region_.get(), 0xFF, 4);
    auto attached = LargeObjectPool::Attach(region_.get(), kPoolSize);
    ASSERT_FALSE(attached.ok());
    EXPECT_EQ(attached.status().code(), StatusCode::kCorruption);
}

TEST_F(LargeObjectPoolTest, RegionRelativeHandleAndBoundedAttach) {
    constexpr uint64_t kOffset = 4096;
    constexpr uint32_t kRegionId = 77;
    std::memset(region_.get(), 0, kPoolSize);
    LargeObjectPoolStorage storage{
        .region_base = region_.get(),
        .region_size = kPoolSize,
        .pool_offset = kOffset,
        .pool_size = kPoolSize - kOffset,
        .region_id = kRegionId,
    };
    auto created = LargeObjectPool::Create(storage, kMaxObject, kSegmentSize);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto handle = created->Allocate(100 * 1024, TypeId{91});
    ASSERT_TRUE(handle.ok());
    EXPECT_EQ(handle->region_id, kRegionId);
    EXPECT_GT(handle->offset, kOffset);
    ASSERT_TRUE(created->Publish(*handle).ok());
    auto plan = created->InspectPlan(*handle);
    ASSERT_TRUE(plan.ok()) << plan.status().ToString();
    EXPECT_GT(plan->segments[0].payload_offset, kOffset);

    auto attached = LargeObjectPool::Attach(storage);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_TRUE(attached->InspectPlan(*handle).ok());

    LargeObjectPoolStorage wrong_id = storage;
    wrong_id.region_id++;
    EXPECT_EQ(LargeObjectPool::Attach(wrong_id).status().code(),
              StatusCode::kCorruption);
    LargeObjectPoolStorage unbounded = storage;
    unbounded.pool_size = kPoolSize;
    EXPECT_EQ(LargeObjectPool::Attach(unbounded).status().code(),
              StatusCode::kInvalidArgument);
}

TEST_F(LargeObjectPoolTest, AttachRejectsImmutableMetadataCorruption) {
    region_[32] ^= std::byte{1};  // Persisted pool_size is CRC protected.
    EXPECT_EQ(LargeObjectPool::Attach(region_.get(), kPoolSize).status().code(),
              StatusCode::kCorruption);
}

TEST_F(LargeObjectPoolTest, ConcurrentAllocationClaimsUniqueRuns) {
    std::mutex mutex;
    std::vector<ShmHandle> handles;
    auto allocate_until_full = [&] {
        for (;;) {
            auto handle = pool_.Allocate(1024, TypeId{5});
            if (!handle.ok()) {
                EXPECT_EQ(handle.status().code(), StatusCode::kResourceExhausted);
                return;
            }
            std::lock_guard lock(mutex);
            handles.push_back(*handle);
        }
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back(allocate_until_full);
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    std::set<uint64_t> offsets;
    for (const ShmHandle handle : handles) {
        EXPECT_TRUE(offsets.insert(handle.offset).second);
        ASSERT_TRUE(pool_.Retire(handle).ok());
        ASSERT_TRUE(pool_.Reclaim(handle).ok());
    }
    EXPECT_EQ(handles.size(), pool_.segment_count());
}

}  // namespace
}  // namespace mino
