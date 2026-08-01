// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_
#define MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "mino/abi/shm_handle.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
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

struct LargeObjectSegment {
    uint32_t segment_index = 0;
    uint32_t segment_size = 0;
    uint64_t payload_offset = 0;  // Region-relative.
};

struct LargeObjectPlan {
    ShmHandle handle;
    uint32_t object_size = 0;
    TypeId type_id;
    std::vector<LargeObjectSegment> segments;
};

// Authoritative pool facts for a Region-relative segment-0 header offset.
// `payload_offset` points into the separate payload array and `object_extent`
// covers the complete contiguous segment run, including tail capacity.
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
        uint32_t segment_size = 0);
    static Result<LargeObjectPool> Attach(
        const LargeObjectPoolStorage& storage);

    // Legacy standalone creation remains source-compatible. Handles are
    // relative to shm_base and carry region_id=0.
    static Result<LargeObjectPool> Create(void* shm_base, uint64_t pool_size,
                                          uint32_t max_object_size,
                                          uint32_t segment_size = 0);

    // Bounded standalone attach. The old unbounded Attach(void*) is intentionally
    // removed because corrupt metadata cannot be validated without an extent.
    static Result<LargeObjectPool> Attach(void* shm_base, uint64_t pool_size,
                                          uint32_t region_id = 0);

    Result<ShmHandle> Allocate(uint32_t object_size, TypeId type_id);
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
    bool is_draining() const noexcept;

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
    Result<uint32_t> ResolveLocked(ShmHandle handle) const;
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

    std::atomic<uint64_t>* segment_bitmap_ = nullptr;
    uint32_t bitmap_words_ = 0;
    std::atomic<uint32_t>* generations_ = nullptr;
    SlabHeader* headers_ = nullptr;
    std::byte* payload_base_ = nullptr;
    uint64_t headers_region_offset_ = 0;
    uint64_t payload_region_offset_ = 0;
    uint32_t* draining_ = nullptr;
};

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_
