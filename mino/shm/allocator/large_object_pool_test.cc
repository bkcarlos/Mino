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
#include <map>
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

class MockRegistrationProvider final : public MemoryRegistrationProvider {
public:
    struct Record {
        RegisteredMemory memory;
        uint64_t scope_id = 0;
    };

    MemoryRegistrationProviderClass provider_class() const noexcept override {
        return MemoryRegistrationProviderClass::kMock;
    }
    std::string name() const override { return "large-pool-test-mock"; }
    bool Supports(MemoryRegistrationKind) const noexcept override {
        return true;
    }
    Result<RegisteredMemory> Register(
        const MemoryRegistrationRequest& request) override {
        ++register_calls;
        if (fail_register) {
            fail_register = false;
            return Status::Error(StatusCode::kUnavailable,
                                 "injected registration failure");
        }
        if (request.address == nullptr || request.bytes == 0 ||
            request.scope_id == 0 || !request.owner.valid()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "invalid mock registration request");
        }
        RegisteredMemory memory{
            .registration_id = next_id++,
            .bytes = request.bytes,
            .device_key = 0xCAFE,
            .kind = request.kind,
            .owner = request.owner,
            .physically_contiguous = physical_contiguous,
        };
        records.emplace(memory.registration_id,
                        Record{.memory = memory, .scope_id = request.scope_id});
        return memory;
    }
    Status Deregister(const RegisteredMemory& registration) override {
        ++deregister_calls;
        if (fail_deregister) {
            fail_deregister = false;
            return Status::Error(StatusCode::kUnavailable,
                                 "injected deregistration failure");
        }
        if (records.erase(registration.registration_id) == 0) {
            return Status::Error(StatusCode::kNotFound,
                                 "mock registration not found");
        }
        return Status::Ok();
    }
    Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest& request) override {
        ++recovery_calls;
        MemoryRegistrationRecoveryResult result;
        for (auto iterator = records.begin(); iterator != records.end();) {
            const Record& record = iterator->second;
            if (record.scope_id != request.scope_id ||
                (record.memory.owner.process_id == request.current_process_id &&
                 record.memory.owner.process_epoch ==
                     request.current_process_epoch)) {
                ++iterator;
                continue;
            }
            ++result.registrations_released;
            result.bytes_released += record.memory.bytes;
            iterator = records.erase(iterator);
        }
        return result;
    }

    bool fail_register = false;
    bool fail_deregister = false;
    bool physical_contiguous = true;
    uint64_t register_calls = 0;
    uint64_t deregister_calls = 0;
    uint64_t recovery_calls = 0;
    uint64_t next_id = 1;
    std::map<uint64_t, Record> records;
};

LargeObjectPoolOptions RegisteredOptions(
    MockRegistrationProvider* provider, uint64_t process_epoch = 1) {
    return LargeObjectPoolOptions{
        .purpose = LargeObjectPoolPurpose::kRdmaRegistered,
        .huge_pages = {},
        .numa = {},
        .registration_provider = provider,
        .registration_scope_id = 0xD608,
        .registration_owner = {.process_id = 7,
                               .process_epoch = process_epoch,
                               .lease_id = 1},
        .registration_quota_bytes = 4u * kSegmentSize,
        .minimum_registered_object_bytes = kSegmentSize,
    };
}

LargeObjectAllocationRequest RegisteredRequest(
    uint32_t bytes, uint64_t process_epoch = 1) {
    return LargeObjectAllocationRequest{
        .object_size = bytes,
        .type_id = TypeId{0xD608},
        .purpose = LargeObjectPoolPurpose::kRdmaRegistered,
        .alignment = 64,
        .contiguity = LargeObjectContiguity::kVirtual,
        .registration = LargeObjectRegistration::kRdma,
        .lifetime = LargeObjectLifetime::kLease,
        .lease = {.process_id = 7,
                  .process_epoch = process_epoch,
                  .lease_id = 99},
    };
}

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

TEST_F(LargeObjectPoolTest, HugePageFallbackIsObservableAndStrictFails) {
    std::memset(region_.get(), 0, kPoolSize);
    LargeObjectPoolOptions options{
        .purpose = LargeObjectPoolPurpose::kHugePage,
        .huge_pages = {.requested = true,
                       .actual = false,
                       .strict = false,
                       .actual_page_size = 4096,
                       .fallback_reason =
                           HugePageFallbackReason::kInsufficientHugePages,
                       .fallback_errno = 12},
        .numa = {},
        .registration_provider = nullptr,
        .registration_scope_id = 0,
        .registration_owner = {},
        .registration_quota_bytes = 0,
        .minimum_registered_object_bytes = 64u * 1024u,
        .recover_stale_registrations = true,
    };
    auto fallback = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                            kSegmentSize, options);
    ASSERT_TRUE(fallback.ok()) << fallback.status().ToString();
    EXPECT_TRUE(fallback->huge_pages_requested());
    EXPECT_FALSE(fallback->huge_pages_actual());
    EXPECT_EQ(fallback->huge_page_fallback_reason(),
              HugePageFallbackReason::kInsufficientHugePages);
    auto handle = fallback->Allocate({
        .object_size = kSegmentSize,
        .type_id = TypeId{1},
        .purpose = LargeObjectPoolPurpose::kHugePage,
        .alignment = 64,
        .contiguity = LargeObjectContiguity::kVirtual,
        .registration = LargeObjectRegistration::kNone,
        .lifetime = LargeObjectLifetime::kAllocation,
        .lease = {},
    });
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();
    EXPECT_EQ(fallback->metrics().huge_page_fallback_allocations, 1u);

    options.huge_pages.strict = true;
    EXPECT_EQ(LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                      kSegmentSize, options)
                  .status()
                  .code(),
              StatusCode::kUnavailable);
}

TEST_F(LargeObjectPoolTest, RegisteredPoolRequiresProviderAndRejectsSmallObjects) {
    std::memset(region_.get(), 0, kPoolSize);
    LargeObjectPoolOptions unavailable{
        .purpose = LargeObjectPoolPurpose::kDma,
        .huge_pages = {},
        .numa = {},
        .registration_provider = nullptr,
        .registration_scope_id = 1,
        .registration_owner = {.process_id = 1,
                               .process_epoch = 1,
                               .lease_id = 1},
        .registration_quota_bytes = kSegmentSize,
        .minimum_registered_object_bytes = kSegmentSize,
    };
    EXPECT_EQ(LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                      kSegmentSize, unavailable)
                  .status()
                  .code(),
              StatusCode::kUnsupported);

    MockRegistrationProvider provider;
    auto options = RegisteredOptions(&provider);
    auto created = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                           kSegmentSize, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    EXPECT_EQ(created->Allocate(kSegmentSize, TypeId{1}).status().code(),
              StatusCode::kInvalidArgument);
    auto small = RegisteredRequest(kSegmentSize - 1);
    EXPECT_EQ(created->Allocate(small).status().code(),
              StatusCode::kInvalidArgument);
    auto wrong = RegisteredRequest(kSegmentSize);
    wrong.purpose = LargeObjectPoolPurpose::kNormal;
    EXPECT_EQ(created->Allocate(wrong).status().code(),
              StatusCode::kInvalidArgument);
    auto unregistered = RegisteredRequest(kSegmentSize);
    unregistered.registration = LargeObjectRegistration::kNone;
    EXPECT_EQ(created->Allocate(unregistered).status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(provider.register_calls, 0u);
}

TEST_F(LargeObjectPoolTest, RegistrationPinLeaseAndQuotaGateReclaim) {
    std::memset(region_.get(), 0, kPoolSize);
    MockRegistrationProvider provider;
    auto options = RegisteredOptions(&provider);
    options.registration_quota_bytes = 2u * kSegmentSize;
    auto created = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                           kSegmentSize, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    const auto request = RegisteredRequest(100u * 1024u);
    auto handle = created->Allocate(request);
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();
    EXPECT_EQ(created->metrics().registration_bytes, 2u * kSegmentSize);
    EXPECT_EQ(created->Allocate(request).status().code(),
              StatusCode::kResourceExhausted);
    ASSERT_TRUE(created->Retire(*handle).ok());
    EXPECT_EQ(created->Reclaim(*handle).code(), StatusCode::kWouldBlock);
    ASSERT_TRUE(created->Unpin(*handle, request.lease).ok());
    EXPECT_EQ(created->Unpin(*handle, request.lease).code(),
              StatusCode::kInvalidArgument);
    ASSERT_TRUE(created->Reclaim(*handle).ok());
    EXPECT_EQ(created->metrics().registration_bytes, 0u);
    EXPECT_EQ(provider.deregister_calls, 1u);
}

TEST_F(LargeObjectPoolTest, ReleaseLeaseDeregistersBeforeReclaim) {
    std::memset(region_.get(), 0, kPoolSize);
    MockRegistrationProvider provider;
    auto options = RegisteredOptions(&provider);
    auto created = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                           kSegmentSize, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    const auto request = RegisteredRequest(kSegmentSize);
    auto handle = created->Allocate(request);
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();
    auto released = created->ReleaseLease(request.lease);
    ASSERT_TRUE(released.ok()) << released.status().ToString();
    EXPECT_EQ(*released, 1u);
    EXPECT_TRUE(provider.records.empty());
    ASSERT_TRUE(created->Retire(*handle).ok());
    ASSERT_TRUE(created->Reclaim(*handle).ok());
}

TEST_F(LargeObjectPoolTest, RegistrationFailureRollsBackExtent) {
    std::memset(region_.get(), 0, kPoolSize);
    MockRegistrationProvider provider;
    provider.fail_register = true;
    auto options = RegisteredOptions(&provider);
    auto created = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                           kSegmentSize, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    const auto before = created->metrics();
    EXPECT_EQ(created->Allocate(RegisteredRequest(kSegmentSize)).status().code(),
              StatusCode::kUnavailable);
    const auto after = created->metrics();
    EXPECT_EQ(after.free_bytes, before.free_bytes);
    EXPECT_EQ(after.registration_bytes, 0u);
    EXPECT_EQ(after.registration_failures, 1u);
}

TEST_F(LargeObjectPoolTest, DeregistrationFailurePreventsExtentReuse) {
    std::memset(region_.get(), 0, kPoolSize);
    MockRegistrationProvider provider;
    auto options = RegisteredOptions(&provider);
    auto created = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                           kSegmentSize, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    const auto request = RegisteredRequest(kSegmentSize);
    auto handle = created->Allocate(request);
    ASSERT_TRUE(handle.ok());
    ASSERT_TRUE(created->Unpin(*handle, request.lease).ok());
    ASSERT_TRUE(created->Retire(*handle).ok());
    provider.fail_deregister = true;
    EXPECT_EQ(created->Reclaim(*handle).code(), StatusCode::kUnavailable);
    EXPECT_TRUE(created->InspectPlan(*handle).ok());
    ASSERT_TRUE(created->Reclaim(*handle).ok());
    EXPECT_FALSE(created->InspectPlan(*handle).ok());
}

TEST_F(LargeObjectPoolTest, AttachRecoversOldProcessRegistrations) {
    std::memset(region_.get(), 0, kPoolSize);
    MockRegistrationProvider provider;
    auto first_options = RegisteredOptions(&provider, 1);
    auto first = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                         kSegmentSize, first_options);
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    auto stale = first->Allocate(RegisteredRequest(kSegmentSize, 1));
    ASSERT_TRUE(stale.ok()) << stale.status().ToString();
    ASSERT_EQ(provider.records.size(), 1u);
    auto observer = LargeObjectPool::Attach(region_.get(), kPoolSize);
    ASSERT_TRUE(observer.ok()) << observer.status().ToString();
    auto observer_plan = observer->InspectPlan(*stale);
    ASSERT_TRUE(observer_plan.ok()) << observer_plan.status().ToString();
    EXPECT_EQ(observer->ClearObjectForRecovery(
                  observer_plan->segments.front().segment_index,
                  static_cast<uint32_t>(ObjectState::kAllocated))
                  .code(),
              StatusCode::kUnavailable);

    auto second_options = RegisteredOptions(&provider, 2);
    auto attached = LargeObjectPool::Attach(region_.get(), kPoolSize, 0,
                                            second_options);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_TRUE(provider.records.empty());
    EXPECT_EQ(attached->metrics().registrations_recovered, 1u);
    EXPECT_EQ(attached->metrics().registration_recovery_bytes, kSegmentSize);
    auto stale_plan = attached->InspectPlan(*stale);
    ASSERT_TRUE(stale_plan.ok()) << stale_plan.status().ToString();
    ASSERT_TRUE(attached->ClearObjectForRecovery(
        stale_plan->segments.front().segment_index,
        static_cast<uint32_t>(ObjectState::kAllocated)).ok());
}

TEST_F(LargeObjectPoolTest, ProviderMustConfirmPhysicalContiguity) {
    std::memset(region_.get(), 0, kPoolSize);
    MockRegistrationProvider provider;
    provider.physical_contiguous = false;
    auto options = RegisteredOptions(&provider);
    auto created = LargeObjectPool::Create(region_.get(), kPoolSize, kMaxObject,
                                           kSegmentSize, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto request = RegisteredRequest(kSegmentSize);
    request.contiguity = LargeObjectContiguity::kPhysical;
    EXPECT_EQ(created->Allocate(request).status().code(), StatusCode::kCorruption);
    EXPECT_EQ(created->metrics().reserved_extent_bytes, 0u);
    EXPECT_TRUE(provider.records.empty());
}

TEST_F(LargeObjectPoolTest, ExplicitAlignmentSelectsAlignedExtent) {
    constexpr uint64_t kAlignedPoolSize = 4u * 1024u * 1024u;
    struct AlignedRegionDeleter {
        void operator()(std::byte* pointer) const {
            ::operator delete[](pointer,
                                std::align_val_t(2u * 1024u * 1024u));
        }
    };
    auto memory = std::unique_ptr<std::byte[], AlignedRegionDeleter>(
        new (std::align_val_t(2u * 1024u * 1024u))
            std::byte[kAlignedPoolSize]);
    std::memset(memory.get(), 0, kAlignedPoolSize);
    auto created = LargeObjectPool::Create(memory.get(), kAlignedPoolSize,
                                           kMaxObject, kSegmentSize);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto handle = created->Allocate({
        .object_size = kSegmentSize,
        .type_id = TypeId{8},
        .purpose = LargeObjectPoolPurpose::kNormal,
        .alignment = 128u * 1024u,
        .contiguity = LargeObjectContiguity::kVirtual,
        .registration = LargeObjectRegistration::kNone,
        .lifetime = LargeObjectLifetime::kAllocation,
        .lease = {},
    });
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();
    auto plan = created->InspectPlan(*handle);
    ASSERT_TRUE(plan.ok()) << plan.status().ToString();
    ASSERT_EQ(plan->segments.size(), 1u);
    const uintptr_t payload = reinterpret_cast<uintptr_t>(
        memory.get() + plan->segments[0].payload_offset);
    EXPECT_EQ(payload % (128u * 1024u), 0u);
}

TEST_F(LargeObjectPoolTest, ExtentsCoalesceAndMetricsExposeFragmentation) {
    auto first = pool_.Allocate(kSegmentSize, TypeId{1});
    auto second = pool_.Allocate(kSegmentSize, TypeId{2});
    auto third = pool_.Allocate(kSegmentSize, TypeId{3});
    ASSERT_TRUE(first.ok() && second.ok() && third.ok());
    ASSERT_TRUE(pool_.Retire(*first).ok());
    ASSERT_TRUE(pool_.Reclaim(*first).ok());
    ASSERT_TRUE(pool_.Retire(*second).ok());
    ASSERT_TRUE(pool_.Reclaim(*second).ok());
    const auto fragmented = pool_.metrics();
    EXPECT_GE(fragmented.largest_free_extent_bytes, 2u * kSegmentSize);
    auto coalesced = pool_.Allocate(2u * kSegmentSize, TypeId{4});
    ASSERT_TRUE(coalesced.ok()) << coalesced.status().ToString();
    EXPECT_EQ(coalesced->offset, first->offset);
    EXPECT_GE(pool_.metrics().reserved_extent_bytes, 3u * kSegmentSize);
}

}  // namespace
}  // namespace mino
