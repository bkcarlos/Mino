// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_
#define MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "mino/abi/shm_handle.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/memory_registration.h"
#include "mino/platform/numa.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino {

// Source compatibility only. Large-object identity is the common TypeId ABI.
using LargeObjectTypeId = TypeId;

struct LargeObjectPoolStorage {
    void* region_base = nullptr;
    uint64_t region_size = 0;
    uint64_t pool_offset = 0;
    uint64_t pool_size = 0;
    uint32_t region_id = 0;
};

enum class LargeObjectPoolPurpose : uint8_t {
    kNormal = 0,
    kHugePage = 1,
    kDma = 2,
    kRdmaRegistered = 3,
};

enum class LargeObjectContiguity : uint8_t {
    kVirtual = 0,
    kPhysical = 1,
};

enum class LargeObjectRegistration : uint8_t {
    kNone = 0,
    kDma = 1,
    kRdma = 2,
};

enum class LargeObjectLifetime : uint8_t {
    kAllocation = 0,
    kLease = 1,
};

struct LargeObjectHugePageBacking {
    bool requested = false;
    bool actual = false;
    bool strict = false;
    uint64_t actual_page_size = 0;
    HugePageFallbackReason fallback_reason = HugePageFallbackReason::kNone;
    int fallback_errno = 0;
};

struct LargeObjectPoolOptions {
    LargeObjectPoolPurpose purpose = LargeObjectPoolPurpose::kNormal;
    LargeObjectHugePageBacking huge_pages;
    NumaPlacementConfig numa;

    // Required for DMA/RDMA pools. scope_id must be stable across process
    // restarts so the provider can clean registrations from an old epoch.
    MemoryRegistrationProvider* registration_provider = nullptr;
    uint64_t registration_scope_id = 0;
    MemoryRegistrationOwner registration_owner;
    uint64_t registration_quota_bytes = 0;
    uint64_t minimum_registered_object_bytes = 64u * 1024u;
    bool recover_stale_registrations = true;
};

struct LargeObjectAllocationRequest {
    uint32_t object_size = 0;
    TypeId type_id;
    LargeObjectPoolPurpose purpose = LargeObjectPoolPurpose::kNormal;
    uint64_t alignment = 1;
    LargeObjectContiguity contiguity = LargeObjectContiguity::kVirtual;
    LargeObjectRegistration registration = LargeObjectRegistration::kNone;
    LargeObjectLifetime lifetime = LargeObjectLifetime::kAllocation;
    MemoryRegistrationOwner lease;
};

struct LargeObjectSegment {
    uint32_t segment_index = 0;
    uint32_t segment_size = 0;
    uint64_t payload_offset = 0;  // Region-relative.
};

struct LargeObjectPlan {
    ShmHandle handle;
    uint32_t object_size = 0;
    TypeId type_id;
    LargeObjectPoolPurpose purpose = LargeObjectPoolPurpose::kNormal;
    std::vector<LargeObjectSegment> segments;
};

// Authoritative pool facts for a Region-relative segment-0 header offset.
// `payload_offset` points into the separate payload array and `object_extent`
// covers the complete contiguous segment run, including tail capacity.
struct LargeObjectNumaStats {
    uint64_t local_allocations = 0;
    uint64_t remote_allocations = 0;
    uint64_t fallback_allocations = 0;
    uint64_t bind_errors = 0;
};

struct LargeObjectPoolMetrics {
    uint64_t capacity_bytes = 0;
    uint64_t allocated_object_bytes = 0;
    uint64_t reserved_extent_bytes = 0;
    uint64_t free_bytes = 0;
    uint64_t internal_fragmentation_bytes = 0;
    uint64_t largest_free_extent_bytes = 0;
    uint64_t external_fragmentation_bytes = 0;
    uint64_t allocations = 0;
    uint64_t allocation_failures = 0;
    uint64_t huge_page_fallback_allocations = 0;
    uint64_t registration_bytes = 0;
    uint64_t registration_failures = 0;
    uint64_t registrations_recovered = 0;
    uint64_t registration_recovery_bytes = 0;
};

struct LargeObjectSlotMetadata {
    bool occupied = false;
    uint32_t generation = 0;
    uint16_t class_id = 0;
    uint32_t capacity = 0;  // Capacity represented by each segment header.
    uint64_t payload_offset = 0;
    uint64_t object_extent = 0;
    bool segmented = true;
};

class LargeObjectPool {
public:
    // Region-aware production APIs. The pool extent must be wholly contained in
    // [region_base, region_base + region_size), and pool_offset is persisted and
    // verified at Attach so every Handle remains Region-relative.
    static Result<LargeObjectPool> Create(
        const LargeObjectPoolStorage& storage, uint32_t max_object_size,
        uint32_t segment_size = 0,
        const NumaPlacementConfig& numa_config = {});
    static Result<LargeObjectPool> Create(
        const LargeObjectPoolStorage& storage, uint32_t max_object_size,
        uint32_t segment_size, const LargeObjectPoolOptions& options);
    static Result<LargeObjectPool> Attach(
        const LargeObjectPoolStorage& storage);
    static Result<LargeObjectPool> Attach(
        const LargeObjectPoolStorage& storage,
        const LargeObjectPoolOptions& options);

    // Legacy standalone creation remains source-compatible. Handles are
    // relative to shm_base and carry region_id=0.
    static Result<LargeObjectPool> Create(
        void* shm_base, uint64_t pool_size, uint32_t max_object_size,
        uint32_t segment_size = 0,
        const NumaPlacementConfig& numa_config = {});
    static Result<LargeObjectPool> Create(
        void* shm_base, uint64_t pool_size, uint32_t max_object_size,
        uint32_t segment_size, const LargeObjectPoolOptions& options);

    // Bounded standalone attach. The old unbounded Attach(void*) is intentionally
    // removed because corrupt metadata cannot be validated without an extent.
    static Result<LargeObjectPool> Attach(void* shm_base, uint64_t pool_size,
                                          uint32_t region_id = 0);
    static Result<LargeObjectPool> Attach(
        void* shm_base, uint64_t pool_size, uint32_t region_id,
        const LargeObjectPoolOptions& options);

    // The legacy allocation API is intentionally accepted only by normal pools.
    // Specialized pools require every use/lifetime/contiguity choice to be
    // explicit, preventing ordinary small objects from entering registered pools.
    Result<ShmHandle> Allocate(uint32_t object_size, TypeId type_id);
    Result<ShmHandle> Allocate(const LargeObjectAllocationRequest& request);
    Status Pin(ShmHandle handle, MemoryRegistrationOwner lease);
    Status Unpin(ShmHandle handle, MemoryRegistrationOwner lease);
    Result<uint64_t> ReleaseLease(MemoryRegistrationOwner lease);
    Status Publish(ShmHandle handle);
    Status Retire(ShmHandle handle);
    Status Reclaim(ShmHandle handle);
    Result<LargeObjectPlan> InspectPlan(ShmHandle handle) const;

    // Maps an exact Region-relative segment-0 header offset to authoritative
    // bitmap/generation/pool-layout metadata. Continuation-header offsets and
    // corrupt occupied runs are rejected.
    Result<LargeObjectSlotMetadata> GetSlotMetadata(
        uint64_t header_offset) const;

    uint64_t pool_offset() const { return pool_offset_; }
    uint64_t pool_size() const { return pool_size_; }
    uint32_t region_id() const { return region_id_; }
    uint32_t max_object_size() const { return max_object_size_; }
    uint32_t segment_size() const { return segment_size_; }
    uint32_t segment_count() const { return segment_count_; }
    LargeObjectPoolPurpose purpose() const noexcept { return purpose_; }
    bool huge_pages_requested() const noexcept { return huge_pages_requested_; }
    bool huge_pages_actual() const noexcept { return huge_pages_actual_; }
    uint64_t actual_page_size() const noexcept { return actual_page_size_; }
    HugePageFallbackReason huge_page_fallback_reason() const noexcept {
        return huge_page_fallback_reason_;
    }
    bool is_draining() const noexcept;
    LargeObjectNumaStats numa_stats() const noexcept;
    LargeObjectPoolMetrics metrics() const noexcept;
    MemoryRegistrationProviderClass registration_provider_class() const noexcept;

    // Narrow recovery adapter. Callers must hold Region recovery ownership.
    // ClearSegmentForRecovery is for incomplete protocol states whose run cannot
    // be trusted. ClearObjectForRecovery requires a CRC-valid segment-0 plan and
    // may reclaim any exact state, including an unreferenced PUBLISHED object.
    bool IsSegmentOccupiedForRecovery(uint32_t segment_index) const noexcept;
    bool ReadSegmentForRecovery(uint32_t segment_index,
                                SlabHeader* header_out) const noexcept;
    uint32_t AuthoritativeGenerationForRecovery(
        uint32_t segment_index) const noexcept;
    Result<ShmHandle> HandleForRecovery(uint32_t segment_index) const;
    Status ClearSegmentForRecovery(uint32_t segment_index,
                                   uint32_t expected_state);
    Status ClearStaleStateForRecovery(uint32_t segment_index,
                                      uint32_t expected_state);
    Status ClearObjectForRecovery(uint32_t first_segment,
                                  uint32_t expected_state);

    LargeObjectPool() = default;

private:
    struct LocalNumaState;
    struct LocalSpecializedState;

    Result<uint32_t> ResolveLocked(ShmHandle handle) const;
    Result<uint32_t> FindAndClaimExtent(uint32_t segments_needed,
                                        uint64_t alignment);
    Status PrepareRegistration(ShmHandle handle,
                               const LargeObjectAllocationRequest& request,
                               uint32_t segments_needed);
    Status ReleaseRegistration(ShmHandle handle, bool require_unpinned);
    bool IsSegmentSet(uint32_t segment_index) const noexcept;
    void ClearSegmentBit(uint32_t segment_index) noexcept;

    void* region_base_ = nullptr;
    std::byte* pool_base_ = nullptr;
    uint64_t region_size_ = 0;
    uint64_t pool_offset_ = 0;
    uint64_t pool_size_ = 0;
    uint32_t max_object_size_ = 0;
    uint32_t segment_size_ = 0;
    uint32_t segment_count_ = 0;
    uint32_t region_id_ = 0;
    LargeObjectPoolPurpose purpose_ = LargeObjectPoolPurpose::kNormal;
    bool huge_pages_requested_ = false;
    bool huge_pages_actual_ = false;
    uint64_t actual_page_size_ = 0;
    HugePageFallbackReason huge_page_fallback_reason_ =
        HugePageFallbackReason::kNone;

    std::atomic<uint64_t>* segment_bitmap_ = nullptr;
    uint32_t bitmap_words_ = 0;
    std::atomic<uint32_t>* generations_ = nullptr;
    SlabHeader* headers_ = nullptr;
    std::byte* payload_base_ = nullptr;
    uint64_t headers_region_offset_ = 0;
    uint64_t payload_region_offset_ = 0;
    uint32_t* draining_ = nullptr;
    std::shared_ptr<LocalNumaState> local_numa_state_;
    std::shared_ptr<LocalSpecializedState> local_specialized_state_;
};

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_
