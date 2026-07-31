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

#include <atomic>
#include <new>

namespace mino {
namespace {

constexpr uint32_t kLargePoolMagic = 0x4D4C504Fu;  // "MLPO"
constexpr uint32_t kLargePoolVersion = 1;
constexpr uint32_t kDefaultSegmentSize = 64u * 1024u;

// Large-object segment marker for SlabHeader::class_id (never a valid
// central-allocator class id).
constexpr uint16_t kLargeObjectClassId = 0xFFFFu;

// Shared-memory pool superblock, followed by the segment bitmap, the
// generation array, the per-segment headers, and the segment payload area.
// All offsets are relative to shm_base.
struct alignas(64) LargePoolSuperblock {
    uint32_t magic;
    uint32_t version;
    uint32_t region_id;
    uint32_t max_object_size;

    uint64_t pool_size;
    uint32_t segment_size;
    uint32_t segment_count;

    uint32_t bitmap_words;
    uint32_t reserved;
};

static_assert(sizeof(LargePoolSuperblock) == 64);

constexpr uint64_t AlignUp(uint64_t v, uint64_t a) { return (v + a - 1) / a * a; }

uint32_t ToStateBits(ObjectState s) { return static_cast<uint32_t>(s); }

// Returns the metadata size (superblock + bitmap + generations + headers +
// alignment padding) for `segment_count` segments of `segment_size` bytes.
uint64_t MetadataSize(uint32_t segment_count, uint32_t segment_size) {
    const uint32_t bitmap_words = (segment_count + 63) / 64;
    uint64_t meta = sizeof(LargePoolSuperblock);
    meta += sizeof(std::atomic<uint64_t>) * bitmap_words;
    meta = AlignUp(meta, alignof(std::atomic<uint32_t>));
    meta += sizeof(std::atomic<uint32_t>) * segment_count;
    meta = AlignUp(meta, alignof(SlabHeader));
    meta += sizeof(SlabHeader) * segment_count;
    meta = AlignUp(meta, segment_size);
    return meta;
}

}  // namespace

Result<LargeObjectPool> LargeObjectPool::Create(void* shm_base, uint64_t pool_size,
                                                uint32_t max_object_size,
                                                uint32_t segment_size) {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument, "shm_base is null");
    }
    if (max_object_size == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "max_object_size must be positive");
    }
    if (segment_size == 0) {
        segment_size = kDefaultSegmentSize;
    }
    if (segment_size < 64 || (segment_size & 63u) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "segment_size must be a multiple of 64 and >= 64");
    }

    // Determine the largest segment_count that fits: metadata grows with
    // segment_count, so iterate until stable.
    uint32_t segment_count = 0;
    for (;;) {
        const uint64_t required = MetadataSize(segment_count + 1, segment_size) +
                                  static_cast<uint64_t>(segment_size) * (segment_count + 1);
        if (required > pool_size) {
            break;
        }
        ++segment_count;
    }
    if (segment_count == 0) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "pool too small for any segment");
    }
    // The pool must be able to hold at least one max-size object.
    const uint32_t segments_per_max =
        (max_object_size + segment_size - 1) / segment_size;
    if (segment_count < segments_per_max) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "pool too small for one max-size object");
    }

    auto* super = new (shm_base) LargePoolSuperblock{};
    super->magic = kLargePoolMagic;
    super->version = kLargePoolVersion;
    super->region_id = 0;  // assigned by the Region layer
    super->max_object_size = max_object_size;
    super->pool_size = pool_size;
    super->segment_size = segment_size;
    super->segment_count = segment_count;
    super->bitmap_words = (segment_count + 63) / 64;
    super->reserved = 0;

    auto* base = static_cast<std::byte*>(shm_base);
    auto* bitmap = reinterpret_cast<std::atomic<uint64_t>*>(base + sizeof(LargePoolSuperblock));
    for (uint32_t i = 0; i < super->bitmap_words; ++i) {
        new (&bitmap[i]) std::atomic<uint64_t>(0);
    }
    auto* generations = reinterpret_cast<std::atomic<uint32_t>*>(
        AlignUp(reinterpret_cast<uintptr_t>(bitmap) +
                    sizeof(std::atomic<uint64_t>) * super->bitmap_words,
                alignof(std::atomic<uint32_t>)));
    for (uint32_t i = 0; i < segment_count; ++i) {
        new (&generations[i]) std::atomic<uint32_t>(0);
    }
    auto* headers = reinterpret_cast<SlabHeader*>(
        AlignUp(reinterpret_cast<uintptr_t>(generations) +
                    sizeof(std::atomic<uint32_t>) * segment_count,
                alignof(SlabHeader)));
    // Headers are zero-initialized by the caller's memset of the Region.
    auto* payload = reinterpret_cast<std::byte*>(
        AlignUp(reinterpret_cast<uintptr_t>(headers) +
                    sizeof(SlabHeader) * segment_count,
                segment_size));

    LargeObjectPool pool;
    pool.shm_base_ = shm_base;
    pool.pool_size_ = pool_size;
    pool.max_object_size_ = max_object_size;
    pool.segment_size_ = segment_size;
    pool.segment_count_ = segment_count;
    pool.segment_bitmap_ = bitmap;
    pool.bitmap_words_ = super->bitmap_words;
    pool.generations_ = generations;
    pool.headers_ = headers;
    pool.payload_base_ = payload;
    pool.region_id_ = super->region_id;
    return pool;
}

Result<LargeObjectPool> LargeObjectPool::Attach(void* shm_base) {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument, "shm_base is null");
    }
    auto* super = static_cast<LargePoolSuperblock*>(shm_base);
    if (super->magic != kLargePoolMagic) {
        return Status::Error(StatusCode::kCorruption, "bad pool magic");
    }
    if (super->version != kLargePoolVersion) {
        return Status::Error(StatusCode::kCorruption, "bad pool version");
    }

    auto* base = static_cast<std::byte*>(shm_base);
    auto* bitmap = reinterpret_cast<std::atomic<uint64_t>*>(base + sizeof(LargePoolSuperblock));
    auto* generations = reinterpret_cast<std::atomic<uint32_t>*>(
        AlignUp(reinterpret_cast<uintptr_t>(bitmap) +
                    sizeof(std::atomic<uint64_t>) * super->bitmap_words,
                alignof(std::atomic<uint32_t>)));
    auto* headers = reinterpret_cast<SlabHeader*>(
        AlignUp(reinterpret_cast<uintptr_t>(generations) +
                    sizeof(std::atomic<uint32_t>) * super->segment_count,
                alignof(SlabHeader)));
    auto* payload = reinterpret_cast<std::byte*>(
        AlignUp(reinterpret_cast<uintptr_t>(headers) +
                    sizeof(SlabHeader) * super->segment_count,
                super->segment_size));

    LargeObjectPool pool;
    pool.shm_base_ = shm_base;
    pool.pool_size_ = super->pool_size;
    pool.max_object_size_ = super->max_object_size;
    pool.segment_size_ = super->segment_size;
    pool.segment_count_ = super->segment_count;
    pool.segment_bitmap_ = bitmap;
    pool.bitmap_words_ = super->bitmap_words;
    pool.generations_ = generations;
    pool.headers_ = headers;
    pool.payload_base_ = payload;
    pool.region_id_ = super->region_id;
    return pool;
}

Result<ShmHandle> LargeObjectPool::Allocate(uint32_t object_size,
                                            LargeObjectTypeId type_id) {
    if (object_size == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "object_size must be positive");
    }
    if (object_size > max_object_size_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "object exceeds pool max_object_size");
    }

    const uint32_t segments_needed =
        (object_size + segment_size_ - 1) / segment_size_;

    // Find and claim a run of consecutive free segments. Claims are made
    // bit-by-bit with CAS; on contention we roll back the partial run and
    // retry the scan from the beginning.
    uint32_t run_start = 0;
    bool claimed = false;
    for (uint32_t attempt = 0; attempt < segment_count_ && !claimed; ++attempt) {
        uint32_t run_length = 0;
        run_start = 0;
        for (uint32_t seg = 0; seg < segment_count_; ++seg) {
            if (IsSegmentSet(seg)) {
                run_length = 0;
                continue;
            }
            if (run_length == 0) {
                run_start = seg;
            }
            ++run_length;
            if (run_length == segments_needed) {
                break;
            }
        }
        if (run_length < segments_needed) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "large object pool exhausted");
        }
        // Try to claim [run_start, run_start + segments_needed).
        uint32_t done = 0;
        for (; done < segments_needed; ++done) {
            const uint32_t seg = run_start + done;
            const uint64_t mask = uint64_t{1} << (seg % 64);
            auto& cell = segment_bitmap_[seg / 64];
            uint64_t expected = cell.load(std::memory_order_acquire);
            do {
                if ((expected & mask) != 0) {
                    break;
                }
            } while (!cell.compare_exchange_weak(expected, expected | mask,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire));
            if ((expected & mask) != 0) {
                break;  // lost the race; roll back below
            }
        }
        if (done == segments_needed) {
            claimed = true;
        } else {
            for (uint32_t i = 0; i < done; ++i) {
                ClearSegmentBit(run_start + i);
            }
        }
    }
    if (!claimed) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "large object pool exhausted");
    }

    // Bump generations for all segments; refuse wrap on any segment
    // (design doc 8.3 step 6 / ADR-0002: never wrap).
    for (uint32_t i = 0; i < segments_needed; ++i) {
        const uint32_t seg = run_start + i;
        std::atomic<uint32_t>& gen = generations_[seg];
        if (gen.load(std::memory_order_relaxed) == kGenerationDraining) {
            for (uint32_t j = 0; j < segments_needed; ++j) {
                ClearSegmentBit(run_start + j);
            }
            return Status::Error(StatusCode::kResourceExhausted,
                                 "segment generation exhausted; pool DRAINING");
        }
        gen.fetch_add(1, std::memory_order_relaxed);
    }

    // Fill segment headers. Segment 0 carries the immutable CRC. The segment
    // count is derived deterministically from object_size and segment_size.
    for (uint32_t i = 0; i < segments_needed; ++i) {
        const uint32_t seg = run_start + i;
        SlabHeader& h = headers_[seg];
        h.magic = kSlabHeaderMagic;
        h.header_version = kSlabHeaderVersion;
        h.class_id = kLargeObjectClassId;
        h.generation = generations_[seg].load(std::memory_order_acquire);
        h.capacity = segment_size_;
        h.object_size = object_size;
        h.type_id = type_id.value;
        h.layout_version = 0;
        h.schema_short_id = 0;
        h.owner_epoch.store(0, std::memory_order_relaxed);
        h.allocation_transaction_id.store(0, std::memory_order_relaxed);
        h.allocation_role.store(0, std::memory_order_relaxed);
        h.immutable_header_crc = i == 0 ? ComputeImmutableHeaderCrc(h) : 0;
    }

    // Single publication point: mark all segments allocated, segment 0 last
    // with release so that observing kAllocated on segment 0 implies the
    // whole run is visible.
    for (uint32_t i = segments_needed; i-- > 0;) {
        headers_[run_start + i].object_state.store(
            ToStateBits(ObjectState::kAllocated), std::memory_order_release);
    }

    ShmHandle handle;
    handle.offset = static_cast<uint64_t>(
        reinterpret_cast<std::byte*>(&headers_[run_start]) -
        static_cast<std::byte*>(shm_base_));
    handle.generation = generations_[run_start].load(std::memory_order_acquire);
    handle.region_id = region_id_;
    return handle;
}

Status LargeObjectPool::Retire(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t seg0, ResolveLocked(handle));
    SlabHeader& h = headers_[seg0];
    const uint32_t state = h.object_state.load(std::memory_order_acquire);
    if (state == ToStateBits(ObjectState::kRetired)) {
        return Status::Ok();
    }
    if (state != ToStateBits(ObjectState::kAllocated) &&
        state != ToStateBits(ObjectState::kBuilding) &&
        state != ToStateBits(ObjectState::kPublished)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "object is not in a retirable state");
    }
    h.object_state.store(ToStateBits(ObjectState::kRetired),
                         std::memory_order_release);
    return Status::Ok();
}

Status LargeObjectPool::Reclaim(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(LargeObjectPlan plan, InspectPlan(handle));

    SlabHeader& h0 = headers_[plan.segments[0].segment_index];
    const uint32_t state = h0.object_state.load(std::memory_order_acquire);
    if (state != ToStateBits(ObjectState::kRetired) &&
        state != ToStateBits(ObjectState::kAborting) &&
        state != ToStateBits(ObjectState::kAllocated) &&
        state != ToStateBits(ObjectState::kBuilding)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "object must be retired (or crash-intermediate) before reclaim");
    }

    // Free segments in reverse order so that segment 0 (the header) remains
    // valid until the end of the reclaim plan.
    for (auto it = plan.segments.rbegin(); it != plan.segments.rend(); ++it) {
        const uint32_t seg = it->segment_index;
        headers_[seg].object_state.store(ToStateBits(ObjectState::kFree),
                                         std::memory_order_release);
        ClearSegmentBit(seg);
    }
    return Status::Ok();
}

Result<LargeObjectPlan> LargeObjectPool::InspectPlan(ShmHandle handle) const {
    MINO_ASSIGN_OR_RETURN(const uint32_t seg0, ResolveLocked(handle));

    const SlabHeader& h0 = headers_[seg0];
    if (!VerifyImmutableHeader(h0)) {
        return Status::Error(StatusCode::kCorruption,
                             "segment 0 immutable CRC mismatch");
    }
    const uint32_t segments_needed =
        (h0.object_size + segment_size_ - 1) / segment_size_;
    if (segments_needed == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "large object has an invalid segment count");
    }
    if (seg0 + segments_needed > segment_count_) {
        return Status::Error(StatusCode::kCorruption,
                             "segment plan exceeds pool bounds");
    }

    LargeObjectPlan plan;
    plan.handle = handle;
    plan.object_size = h0.object_size;
    plan.type_id = LargeObjectTypeId{h0.type_id};
    plan.segments.reserve(segments_needed);

    uint32_t remaining = h0.object_size;
    for (uint32_t i = 0; i < segments_needed; ++i) {
        const uint32_t seg = seg0 + i;
        if (!IsSegmentSet(seg)) {
            return Status::Error(StatusCode::kCorruption,
                                 "segment plan references a free segment");
        }
        LargeObjectSegment s;
        s.segment_index = seg;
        s.segment_size = remaining < segment_size_ ? remaining : segment_size_;
        s.payload_offset = static_cast<uint64_t>(
            payload_base_ + static_cast<uint64_t>(seg) * segment_size_ -
            static_cast<std::byte*>(shm_base_));
        plan.segments.push_back(s);
        remaining -= s.segment_size;
    }
    return plan;
}

Result<uint32_t> LargeObjectPool::ResolveLocked(ShmHandle handle) const {
    if (handle.offset == 0 && handle.generation == 0 && handle.region_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument, "null handle");
    }
    if (handle.region_id != region_id_) {
        return Status::Error(StatusCode::kInvalidArgument, "foreign region handle");
    }
    const auto* base = static_cast<const std::byte*>(shm_base_);
    const auto* hdr_base = reinterpret_cast<const std::byte*>(headers_);
    if (handle.offset < static_cast<uint64_t>(hdr_base - base)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "handle offset in metadata area");
    }
    const uint64_t rel = handle.offset - static_cast<uint64_t>(hdr_base - base);
    if (rel % sizeof(SlabHeader) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "handle offset not header-aligned");
    }
    const uint32_t seg = static_cast<uint32_t>(rel / sizeof(SlabHeader));
    if (seg >= segment_count_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "segment index out of range");
    }
    if (!IsSegmentSet(seg)) {
        return Status::Error(StatusCode::kNotFound, "segment not allocated");
    }
    if (generations_[seg].load(std::memory_order_acquire) != handle.generation) {
        return Status::Error(StatusCode::kNotFound,
                             "stale handle (generation mismatch)");
    }
    return seg;
}

bool LargeObjectPool::IsSegmentSet(uint32_t segment_index) const {
    if (segment_index >= segment_count_) {
        return false;
    }
    return (segment_bitmap_[segment_index / 64].load(std::memory_order_acquire) &
            (uint64_t{1} << (segment_index % 64))) != 0;
}

void LargeObjectPool::ClearSegmentBit(uint32_t segment_index) {
    segment_bitmap_[segment_index / 64].fetch_and(
        ~(uint64_t{1} << (segment_index % 64)), std::memory_order_acq_rel);
}

}  // namespace mino
