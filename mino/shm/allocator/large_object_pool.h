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

// Large Object Pool (design doc 8.5).
//
// Objects that exceed the largest regular size class are allocated from a
// dedicated pool with its own ownership protocol. The pool supports
// segmented allocation: a single object may span multiple consecutive
// segments, and the pool produces an explicit traversal/validation/reclaim
// plan for recovery.

#ifndef MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_
#define MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_

#include <atomic>
#include <cstdint>
#include <vector>

#include "mino/abi/shm_handle.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/allocator/generation_array.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino {

// Strong ID wrapper for type_id (design doc 5.2). Declared here so the pool
// is self-contained; when the runtime provides a shared identity header this
// can be unified.
struct LargeObjectTypeId {
    uint32_t value = 0;

    friend bool operator==(const LargeObjectTypeId&, const LargeObjectTypeId&) = default;
};

// Segment descriptor of one large object. Segment 0 carries the SlabHeader
// (identity + immutable CRC); all segments store their payload in the pool's
// payload area.
struct LargeObjectSegment {
    uint32_t segment_index;   // Index within the pool's segment array.
    uint32_t segment_size;    // Payload bytes in this segment.
    uint64_t payload_offset;  // Offset of segment payload from pool base.
};

// Traversal/validation/reclaim plan for one large object (design doc 8.5:
// "必须生成遍历、校验和回收计划"). The plan is deterministic: segments are
// listed in allocation order and reclaimed in reverse order so that
// segment 0 (which carries the header) is freed last.
struct LargeObjectPlan {
    ShmHandle handle;
    uint32_t object_size = 0;
    LargeObjectTypeId type_id;
    std::vector<LargeObjectSegment> segments;  // Traversal order.
};

// LargeObjectPool manages objects larger than the largest regular size
// class (design doc 8.5). Storage is a contiguous extent of shared memory
// carved into fixed-size segments; each object occupies
// ceil(size / segment_size) consecutive segments.
//
// Allocation protocol mirrors the central allocator: bitmap claim,
// generation bump, header fill, then a single release publication point on
// segment 0's object_state.
class LargeObjectPool {
public:
    // Creates a pool over [shm_base, shm_base + pool_size). The memory must
    // be zero-initialized. `max_object_size` is the largest single object
    // the pool can serve; `segment_size` is the fixed segment granularity
    // (0 selects the 64 KiB default). Fails with kInvalidArgument for
    // inconsistent sizes and with kResourceExhausted if the pool cannot hold
    // the metadata plus at least one max-size object.
    static Result<LargeObjectPool> Create(void* shm_base, uint64_t pool_size,
                                          uint32_t max_object_size,
                                          uint32_t segment_size = 0);

    // Attaches to an existing pool previously initialized with Create().
    // Validates the superblock magic and version.
    static Result<LargeObjectPool> Attach(void* shm_base);

    // Allocates one large object. Fails with kResourceExhausted when the
    // pool lacks a long enough run of consecutive free segments, with
    // kInvalidArgument for size > max_object_size, and with
    // kResourceExhausted when any involved segment generation is exhausted
    // (pool DRAINING, design doc 8.3 step 6 semantics).
    Result<ShmHandle> Allocate(uint32_t object_size, LargeObjectTypeId type_id);

    // Marks the object Retired: no new Borrow will be produced (design doc
    // 8.4). Same semantics as the central allocator's Retire.
    Status Retire(ShmHandle handle);

    // Reclaims the object: validates the reclaim plan, then frees segments
    // in reverse order (segment 0 last) so the header stays valid until the
    // end. Requires the object to be Retired or in a crash-intermediate
    // state (recovery-driven reclaim).
    Status Reclaim(ShmHandle handle);

    // Builds the traversal/validation/reclaim plan for `handle`
    // (design doc 8.5). Fails with kNotFound for stale handles and with
    // kCorruption when the stored segment count disagrees with the object
    // size or a planned segment is not allocated.
    Result<LargeObjectPlan> InspectPlan(ShmHandle handle) const;

    uint64_t pool_size() const { return pool_size_; }
    uint32_t max_object_size() const { return max_object_size_; }
    uint32_t segment_size() const { return segment_size_; }
    uint32_t segment_count() const { return segment_count_; }

    // Default-constructible so the facade can be stored by value (e.g. in
    // test fixtures); Create()/Attach() are the only supported ways to
    // build a usable pool.
    LargeObjectPool() = default;

private:

    Result<uint32_t> ResolveLocked(ShmHandle handle) const;
    bool IsSegmentSet(uint32_t segment_index) const;
    void ClearSegmentBit(uint32_t segment_index);

    void* shm_base_ = nullptr;
    uint64_t pool_size_ = 0;
    uint32_t max_object_size_ = 0;
    uint32_t segment_size_ = 0;
    uint32_t segment_count_ = 0;

    // Shared-memory state.
    std::atomic<uint64_t>* segment_bitmap_ = nullptr;  // one bit per segment
    uint32_t bitmap_words_ = 0;
    std::atomic<uint32_t>* generations_ = nullptr;  // one per segment
    SlabHeader* headers_ = nullptr;                 // one per segment
    std::byte* payload_base_ = nullptr;
    uint32_t region_id_ = 0;
};

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_LARGE_OBJECT_POOL_H_
