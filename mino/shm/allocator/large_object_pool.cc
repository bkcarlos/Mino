// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/shm/allocator/large_object_pool.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <new>

#include "mino/common/checked_arithmetic.h"
#include "mino/shm/allocator/generation_array.h"

namespace mino {
namespace {

constexpr uint32_t kLargePoolMagic = 0x4D4C504Fu;  // "MLPO"
constexpr uint16_t kLargePoolVersion = 2;
constexpr uint32_t kDefaultSegmentSize = 64u * 1024u;
constexpr uint16_t kLargeObjectClassId = 0xFFFFu;

struct alignas(64) LargePoolSuperblock {
    uint32_t magic;
    uint16_t version;
    uint16_t header_size;
    uint32_t region_id;
    uint32_t max_object_size;
    uint64_t pool_offset;
    uint64_t pool_size;
    uint32_t segment_size;
    uint32_t segment_count;
    uint32_t bitmap_words;
    uint32_t reserved0;
    uint64_t metadata_size;
    uint32_t immutable_crc32;
    uint32_t draining;  // Mutable atomic_ref word; excluded from immutable CRC.
};
static_assert(sizeof(LargePoolSuperblock) == 64);

struct PoolLayout {
    uint64_t bitmap_offset = 0;
    uint64_t generations_offset = 0;
    uint64_t headers_offset = 0;
    uint64_t payload_offset = 0;
    uint64_t required_size = 0;
    uint32_t bitmap_words = 0;
};

uint32_t Crc32(const void* data, size_t size) {
    constexpr uint32_t kPoly = 0xEDB88320u;
    uint32_t crc = 0xFFFFFFFFu;
    const auto* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < size; ++i) {
        crc ^= bytes[i];
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ ((crc & 1u) != 0 ? kPoly : 0u);
        }
    }
    return crc ^ 0xFFFFFFFFu;
}

uint32_t SuperblockCrc(const LargePoolSuperblock& super) {
    return Crc32(&super, offsetof(LargePoolSuperblock, immutable_crc32));
}

Result<PoolLayout> ComputeLayout(uint32_t segment_count,
                                 uint32_t segment_size) {
    if (segment_count == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large pool has zero segments");
    }
    PoolLayout layout;
    layout.bitmap_offset = sizeof(LargePoolSuperblock);
    layout.bitmap_words = static_cast<uint32_t>(
        (static_cast<uint64_t>(segment_count) + 63u) / 64u);

    uint64_t bytes = 0;
    uint64_t end = 0;
    if (!CheckedMulU64(sizeof(std::atomic<uint64_t>), layout.bitmap_words,
                       &bytes) ||
        !CheckedAddU64(layout.bitmap_offset, bytes, &end) ||
        !CheckedAlignUpU64(end, alignof(std::atomic<uint32_t>),
                           &layout.generations_offset) ||
        !CheckedMulU64(sizeof(std::atomic<uint32_t>), segment_count, &bytes) ||
        !CheckedAddU64(layout.generations_offset, bytes, &end) ||
        !CheckedAlignUpU64(end, alignof(SlabHeader), &layout.headers_offset) ||
        !CheckedMulU64(sizeof(SlabHeader), segment_count, &bytes) ||
        !CheckedAddU64(layout.headers_offset, bytes, &end) ||
        !CheckedAlignUpU64(end, segment_size, &layout.payload_offset) ||
        !CheckedMulU64(segment_size, segment_count, &bytes) ||
        !CheckedAddU64(layout.payload_offset, bytes, &layout.required_size)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large pool metadata arithmetic overflow");
    }
    return layout;
}

Status ValidateStorage(const LargeObjectPoolStorage& storage) {
    if (storage.region_base == nullptr || storage.region_size == 0 ||
        storage.pool_size < sizeof(LargePoolSuperblock)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large pool storage is empty");
    }
    uint64_t pool_end = 0;
    if (!CheckedAddU64(storage.pool_offset, storage.pool_size, &pool_end) ||
        pool_end > storage.region_size) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large pool extent exceeds Region bounds");
    }
    uintptr_t address = reinterpret_cast<uintptr_t>(storage.region_base);
    if (storage.pool_offset > std::numeric_limits<uintptr_t>::max() - address ||
        (address + static_cast<uintptr_t>(storage.pool_offset)) %
                alignof(LargePoolSuperblock) !=
            0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large pool base is misaligned");
    }
    return Status::Ok();
}

uint32_t StateBits(ObjectState state) {
    return static_cast<uint32_t>(state);
}

bool IsKnownObjectState(uint32_t state) {
    return state <= StateBits(ObjectState::kAllocating);
}

void CopyHeader(const SlabHeader& source, SlabHeader* destination) {
    destination->magic = source.magic;
    destination->header_version = source.header_version;
    destination->class_id = source.class_id;
    destination->generation.store(
        source.generation.load(std::memory_order_acquire),
        std::memory_order_relaxed);
    destination->object_state.store(
        source.object_state.load(std::memory_order_acquire),
        std::memory_order_relaxed);
    destination->capacity = source.capacity;
    destination->object_size = source.object_size;
    destination->type_id = source.type_id;
    destination->layout_version = source.layout_version;
    destination->schema_short_id = source.schema_short_id;
    destination->owner_epoch.store(
        source.owner_epoch.load(std::memory_order_acquire),
        std::memory_order_relaxed);
    destination->allocation_transaction_id.store(
        source.allocation_transaction_id.load(std::memory_order_acquire),
        std::memory_order_relaxed);
    destination->immutable_header_crc = source.immutable_header_crc;
    destination->allocation_role.store(
        source.allocation_role.load(std::memory_order_acquire),
        std::memory_order_relaxed);
}

}  // namespace

Result<LargeObjectPool> LargeObjectPool::Create(
    void* shm_base, uint64_t pool_size, uint32_t max_object_size,
    uint32_t segment_size) {
    return Create(LargeObjectPoolStorage{.region_base = shm_base,
                                         .region_size = pool_size,
                                         .pool_offset = 0,
                                         .pool_size = pool_size,
                                         .region_id = 0},
                  max_object_size, segment_size);
}

Result<LargeObjectPool> LargeObjectPool::Attach(void* shm_base,
                                                uint64_t pool_size,
                                                uint32_t region_id) {
    return Attach(LargeObjectPoolStorage{.region_base = shm_base,
                                         .region_size = pool_size,
                                         .pool_offset = 0,
                                         .pool_size = pool_size,
                                         .region_id = region_id});
}

Result<LargeObjectPool> LargeObjectPool::Create(
    const LargeObjectPoolStorage& storage, uint32_t max_object_size,
    uint32_t segment_size) {
    MINO_RETURN_IF_ERROR(ValidateStorage(storage));
    if (max_object_size == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "max_object_size must be positive");
    }
    if (segment_size == 0) {
        segment_size = kDefaultSegmentSize;
    }
    if (segment_size < 64 || (segment_size & (segment_size - 1u)) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "segment_size must be a power of two and >= 64");
    }

    const uint64_t max_possible = std::min<uint64_t>(
        storage.pool_size / segment_size,
        std::numeric_limits<uint32_t>::max() - 63u);
    uint32_t low = 0;
    uint32_t high = static_cast<uint32_t>(max_possible);
    while (low < high) {
        const uint32_t candidate = low + (high - low + 1u) / 2u;
        auto layout = ComputeLayout(candidate, segment_size);
        if (layout.ok() && layout->required_size <= storage.pool_size) {
            low = candidate;
        } else {
            high = candidate - 1;
        }
    }
    const uint32_t segment_count = low;
    if (segment_count == 0) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "pool too small for any segment");
    }
    const uint64_t segments_per_max =
        (static_cast<uint64_t>(max_object_size) + segment_size - 1u) /
        segment_size;
    if (segments_per_max > segment_count) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "pool too small for one max-size object");
    }
    MINO_ASSIGN_OR_RETURN(PoolLayout layout,
                          ComputeLayout(segment_count, segment_size));

    auto* region_base = static_cast<std::byte*>(storage.region_base);
    std::byte* pool_base = region_base + storage.pool_offset;
    auto* super = new (pool_base) LargePoolSuperblock{};
    super->magic = kLargePoolMagic;
    super->version = kLargePoolVersion;
    super->header_size = sizeof(LargePoolSuperblock);
    super->region_id = storage.region_id;
    super->max_object_size = max_object_size;
    super->pool_offset = storage.pool_offset;
    super->pool_size = storage.pool_size;
    super->segment_size = segment_size;
    super->segment_count = segment_count;
    super->bitmap_words = layout.bitmap_words;
    super->metadata_size = layout.payload_offset;
    super->immutable_crc32 = SuperblockCrc(*super);
    std::atomic_ref(super->draining).store(0, std::memory_order_relaxed);

    auto* bitmap = reinterpret_cast<std::atomic<uint64_t>*>(
        pool_base + layout.bitmap_offset);
    for (uint32_t i = 0; i < layout.bitmap_words; ++i) {
        new (&bitmap[i]) std::atomic<uint64_t>(0);
    }
    auto* generations = reinterpret_cast<std::atomic<uint32_t>*>(
        pool_base + layout.generations_offset);
    for (uint32_t i = 0; i < segment_count; ++i) {
        new (&generations[i]) std::atomic<uint32_t>(0);
    }
    auto* headers = reinterpret_cast<SlabHeader*>(pool_base + layout.headers_offset);
    for (uint32_t i = 0; i < segment_count; ++i) {
        new (&headers[i]) SlabHeader{};
    }

    uint64_t headers_region_offset = 0;
    uint64_t payload_region_offset = 0;
    if (!CheckedAddU64(storage.pool_offset, layout.headers_offset,
                       &headers_region_offset) ||
        !CheckedAddU64(storage.pool_offset, layout.payload_offset,
                       &payload_region_offset)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large pool Region-relative offset overflow");
    }
    LargeObjectPool pool;
    pool.region_base_ = storage.region_base;
    pool.pool_base_ = pool_base;
    pool.region_size_ = storage.region_size;
    pool.pool_offset_ = storage.pool_offset;
    pool.pool_size_ = storage.pool_size;
    pool.region_id_ = storage.region_id;
    pool.max_object_size_ = max_object_size;
    pool.segment_size_ = segment_size;
    pool.segment_count_ = segment_count;
    pool.segment_bitmap_ = bitmap;
    pool.bitmap_words_ = layout.bitmap_words;
    pool.generations_ = generations;
    pool.headers_ = headers;
    pool.payload_base_ = pool_base + layout.payload_offset;
    pool.headers_region_offset_ = headers_region_offset;
    pool.payload_region_offset_ = payload_region_offset;
    pool.draining_ = &super->draining;
    return pool;
}

Result<LargeObjectPool> LargeObjectPool::Attach(
    const LargeObjectPoolStorage& storage) {
    MINO_RETURN_IF_ERROR(ValidateStorage(storage));
    auto* region_base = static_cast<std::byte*>(storage.region_base);
    std::byte* pool_base = region_base + storage.pool_offset;
    const auto* super = reinterpret_cast<const LargePoolSuperblock*>(pool_base);
    if (super->magic != kLargePoolMagic ||
        super->version != kLargePoolVersion ||
        super->header_size != sizeof(LargePoolSuperblock) ||
        super->immutable_crc32 != SuperblockCrc(*super)) {
        return Status::Error(StatusCode::kCorruption,
                             "large pool superblock validation failed");
    }
    if (super->pool_offset != storage.pool_offset ||
        super->pool_size != storage.pool_size ||
        super->region_id != storage.region_id) {
        return Status::Error(StatusCode::kCorruption,
                             "large pool storage identity mismatch");
    }
    if (super->max_object_size == 0 || super->segment_size < 64 ||
        (super->segment_size & (super->segment_size - 1u)) != 0 ||
        super->segment_count == 0 ||
        std::atomic_ref(const_cast<uint32_t&>(super->draining))
                .load(std::memory_order_acquire) > 1) {
        return Status::Error(StatusCode::kCorruption,
                             "large pool configuration is invalid");
    }
    MINO_ASSIGN_OR_RETURN(
        PoolLayout layout,
        ComputeLayout(super->segment_count, super->segment_size));
    if (layout.required_size > storage.pool_size ||
        layout.bitmap_words != super->bitmap_words ||
        layout.payload_offset != super->metadata_size ||
        (static_cast<uint64_t>(super->max_object_size) +
             super->segment_size - 1u) /
                super->segment_size >
            super->segment_count) {
        return Status::Error(StatusCode::kCorruption,
                             "large pool metadata exceeds bounded extent");
    }

    auto* bitmap = reinterpret_cast<std::atomic<uint64_t>*>(
        pool_base + layout.bitmap_offset);
    if ((super->segment_count % 64u) != 0) {
        const uint64_t valid_mask =
            (uint64_t{1} << (super->segment_count % 64u)) - 1u;
        if ((bitmap[layout.bitmap_words - 1].load(std::memory_order_acquire) &
             ~valid_mask) != 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "large pool bitmap has out-of-range bits");
        }
    }

    uint64_t headers_region_offset = 0;
    uint64_t payload_region_offset = 0;
    if (!CheckedAddU64(storage.pool_offset, layout.headers_offset,
                       &headers_region_offset) ||
        !CheckedAddU64(storage.pool_offset, layout.payload_offset,
                       &payload_region_offset)) {
        return Status::Error(StatusCode::kCorruption,
                             "large pool Region-relative offset overflow");
    }
    LargeObjectPool pool;
    pool.region_base_ = storage.region_base;
    pool.pool_base_ = pool_base;
    pool.region_size_ = storage.region_size;
    pool.pool_offset_ = storage.pool_offset;
    pool.pool_size_ = storage.pool_size;
    pool.region_id_ = storage.region_id;
    pool.max_object_size_ = super->max_object_size;
    pool.segment_size_ = super->segment_size;
    pool.segment_count_ = super->segment_count;
    pool.segment_bitmap_ = bitmap;
    pool.bitmap_words_ = layout.bitmap_words;
    pool.generations_ = reinterpret_cast<std::atomic<uint32_t>*>(
        pool_base + layout.generations_offset);
    pool.headers_ = reinterpret_cast<SlabHeader*>(pool_base + layout.headers_offset);
    pool.payload_base_ = pool_base + layout.payload_offset;
    pool.headers_region_offset_ = headers_region_offset;
    pool.payload_region_offset_ = payload_region_offset;
    pool.draining_ = const_cast<uint32_t*>(&super->draining);
    return pool;
}

bool LargeObjectPool::is_draining() const noexcept {
    return draining_ != nullptr &&
           std::atomic_ref(*draining_).load(std::memory_order_acquire) != 0;
}

Result<ShmHandle> LargeObjectPool::Allocate(uint32_t object_size,
                                            TypeId type_id) {
    if (object_size == 0 || object_size > max_object_size_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large object size is outside pool bounds");
    }
    if (is_draining()) {
        return Status::Error(StatusCode::kUnavailable,
                             "large object pool is draining");
    }
    const uint32_t segments_needed = static_cast<uint32_t>(
        (static_cast<uint64_t>(object_size) + segment_size_ - 1u) /
        segment_size_);

    uint32_t run_start = 0;
    bool claimed = false;
    for (uint32_t attempt = 0; attempt < segment_count_ && !claimed; ++attempt) {
        uint32_t run_length = 0;
        for (uint32_t segment = 0; segment < segment_count_; ++segment) {
            if (IsSegmentSet(segment)) {
                run_length = 0;
                continue;
            }
            if (run_length == 0) {
                run_start = segment;
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
        uint32_t done = 0;
        for (; done < segments_needed; ++done) {
            const uint32_t segment = run_start + done;
            const uint64_t mask = uint64_t{1} << (segment % 64u);
            auto& word = segment_bitmap_[segment / 64u];
            uint64_t expected = word.load(std::memory_order_acquire);
            while ((expected & mask) == 0 &&
                   !word.compare_exchange_weak(expected, expected | mask,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
            }
            if ((expected & mask) != 0) {
                break;
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
                             "large object pool exhausted under contention");
    }

    for (uint32_t i = 0; i < segments_needed; ++i) {
        headers_[run_start + i].object_state.store(
            StateBits(ObjectState::kAllocating), std::memory_order_release);
    }
    for (uint32_t i = 0; i < segments_needed; ++i) {
        auto& generation = generations_[run_start + i];
        uint32_t observed = generation.load(std::memory_order_acquire);
        const bool exhausted = observed >= kGenerationDraining - 1u;
        bool generation_advanced = false;
        if (exhausted) {
            if (observed == kGenerationDraining - 1u) {
                (void)generation.compare_exchange_strong(
                    observed, kGenerationDraining, std::memory_order_acq_rel,
                    std::memory_order_acquire);
            }
        } else {
            generation_advanced = generation.compare_exchange_strong(
                observed, observed + 1u, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
        if (exhausted || !generation_advanced) {
            if (exhausted && draining_ != nullptr) {
                std::atomic_ref(*draining_).store(1, std::memory_order_release);
            }
            for (uint32_t j = 0; j < segments_needed; ++j) {
                headers_[run_start + j].object_state.store(
                    StateBits(ObjectState::kFree), std::memory_order_release);
                ClearSegmentBit(run_start + j);
            }
            return Status::Error(
                StatusCode::kResourceExhausted,
                exhausted ? "segment generation exhausted; pool marked DRAINING"
                          : "segment generation raced");
        }
    }

    for (uint32_t i = 0; i < segments_needed; ++i) {
        const uint32_t segment = run_start + i;
        SlabHeader& header = headers_[segment];
        header.magic = kSlabHeaderMagic;
        header.header_version = kSlabHeaderVersion;
        header.class_id = kLargeObjectClassId;
        header.generation.store(
            generations_[segment].load(std::memory_order_acquire),
            std::memory_order_relaxed);
        header.capacity = segment_size_;
        header.object_size = object_size;
        header.type_id = type_id.value;
        header.layout_version = 0;
        header.schema_short_id = 0;
        header.owner_epoch.store(0, std::memory_order_relaxed);
        header.allocation_transaction_id.store(0, std::memory_order_relaxed);
        header.allocation_role.store(i, std::memory_order_relaxed);
        header.immutable_header_crc = ComputeImmutableHeaderCrc(header);
    }
    for (uint32_t i = segments_needed; i-- > 0;) {
        headers_[run_start + i].object_state.store(
            StateBits(ObjectState::kAllocated), std::memory_order_release);
    }

    uint64_t header_delta = 0;
    uint64_t handle_offset = 0;
    if (!CheckedMulU64(run_start, sizeof(SlabHeader), &header_delta) ||
        !CheckedAddU64(headers_region_offset_, header_delta, &handle_offset)) {
        return Status::Error(StatusCode::kInternal,
                             "validated large pool handle arithmetic failed");
    }
    return ShmHandle{.offset = handle_offset,
                     .generation = generations_[run_start].load(
                         std::memory_order_acquire),
                     .region_id = region_id_};
}

Status LargeObjectPool::Publish(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t first, ResolveLocked(handle));
    MINO_ASSIGN_OR_RETURN(LargeObjectPlan plan, InspectPlan(handle));
    (void)plan;
    uint32_t expected = StateBits(ObjectState::kAllocated);
    if (headers_[first].object_state.compare_exchange_strong(
            expected, StateBits(ObjectState::kPublished),
            std::memory_order_release, std::memory_order_acquire) ||
        expected == StateBits(ObjectState::kPublished)) {
        return Status::Ok();
    }
    return Status::Error(StatusCode::kInvalidArgument,
                         "large object is not publishable");
}

Status LargeObjectPool::Retire(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t first, ResolveLocked(handle));
    SlabHeader& header = headers_[first];
    uint32_t state = header.object_state.load(std::memory_order_acquire);
    if (state == StateBits(ObjectState::kRetired)) {
        return Status::Ok();
    }
    if (state != StateBits(ObjectState::kAllocated) &&
        state != StateBits(ObjectState::kBuilding) &&
        state != StateBits(ObjectState::kPublished)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large object is not in a retirable state");
    }
    MINO_ASSIGN_OR_RETURN(LargeObjectPlan plan, InspectPlan(handle));
    (void)plan;
    // Segment 0 is the sole lifecycle publication word. Continuation segment
    // states are not transitioned, avoiding a crash-visible half-retired run.
    header.object_state.store(StateBits(ObjectState::kRetired),
                              std::memory_order_release);
    return Status::Ok();
}

Status LargeObjectPool::Reclaim(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t first, ResolveLocked(handle));
    const uint32_t state =
        headers_[first].object_state.load(std::memory_order_acquire);
    if (state != StateBits(ObjectState::kRetired) &&
        state != StateBits(ObjectState::kAborting) &&
        state != StateBits(ObjectState::kAllocated) &&
        state != StateBits(ObjectState::kBuilding)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large object must be retired or unpublished");
    }
    return ClearObjectForRecovery(first, state);
}

Result<LargeObjectPlan> LargeObjectPool::InspectPlan(ShmHandle handle) const {
    MINO_ASSIGN_OR_RETURN(const uint32_t first, ResolveLocked(handle));
    const SlabHeader& first_header = headers_[first];
    const uint32_t first_state =
        first_header.object_state.load(std::memory_order_acquire);
    if (!VerifyImmutableHeader(first_header) ||
        first_header.class_id != kLargeObjectClassId ||
        first_header.capacity != segment_size_ ||
        first_header.object_size == 0 ||
        first_header.object_size > max_object_size_ ||
        first_header.allocation_role.load(std::memory_order_acquire) != 0 ||
        !IsKnownObjectState(first_state) ||
        first_state == StateBits(ObjectState::kFree)) {
        return Status::Error(StatusCode::kCorruption,
                             "large object segment-0 header is invalid");
    }
    const uint32_t segment_count = static_cast<uint32_t>(
        (static_cast<uint64_t>(first_header.object_size) + segment_size_ - 1u) /
        segment_size_);
    uint32_t end = 0;
    if (!CheckedAddU32(first, segment_count, &end) || end > segment_count_) {
        return Status::Error(StatusCode::kCorruption,
                             "large object plan exceeds pool bounds");
    }

    LargeObjectPlan plan;
    plan.handle = handle;
    plan.object_size = first_header.object_size;
    plan.type_id = TypeId{first_header.type_id};
    plan.segments.reserve(segment_count);
    uint32_t remaining = first_header.object_size;
    for (uint32_t i = 0; i < segment_count; ++i) {
        const uint32_t segment = first + i;
        const SlabHeader& header = headers_[segment];
        if (!IsSegmentSet(segment) || !VerifyImmutableHeader(header) ||
            header.class_id != kLargeObjectClassId ||
            header.capacity != segment_size_ ||
            header.object_size != first_header.object_size ||
            header.type_id != first_header.type_id ||
            header.allocation_role.load(std::memory_order_acquire) != i ||
            header.generation.load(std::memory_order_acquire) !=
                generations_[segment].load(std::memory_order_acquire) ||
            !IsKnownObjectState(
                header.object_state.load(std::memory_order_acquire)) ||
            header.object_state.load(std::memory_order_acquire) ==
                StateBits(ObjectState::kFree)) {
            return Status::Error(StatusCode::kCorruption,
                                 "large object continuation segment is invalid");
        }
        uint64_t payload_delta = 0;
        uint64_t payload_offset = 0;
        if (!CheckedMulU64(segment, segment_size_, &payload_delta) ||
            !CheckedAddU64(payload_region_offset_, payload_delta,
                           &payload_offset)) {
            return Status::Error(StatusCode::kCorruption,
                                 "large object payload offset overflow");
        }
        const uint32_t bytes = std::min(remaining, segment_size_);
        plan.segments.push_back(LargeObjectSegment{
            .segment_index = segment,
            .segment_size = bytes,
            .payload_offset = payload_offset,
        });
        remaining -= bytes;
    }
    return plan;
}

Result<LargeObjectSlotMetadata> LargeObjectPool::GetSlotMetadata(
    uint64_t header_offset) const {
    if (region_base_ == nullptr || segment_bitmap_ == nullptr ||
        generations_ == nullptr || headers_ == nullptr || segment_size_ == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large object pool facade is not initialized");
    }
    if (header_offset < headers_region_offset_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "offset is below the large-pool header array");
    }
    const uint64_t relative = header_offset - headers_region_offset_;
    if (relative % sizeof(SlabHeader) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "offset is not an exact large-pool header start");
    }
    const uint64_t segment64 = relative / sizeof(SlabHeader);
    if (segment64 >= segment_count_ ||
        segment64 > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large-pool header offset is out of range");
    }
    const uint32_t segment = static_cast<uint32_t>(segment64);

    uint64_t checked_delta = 0;
    uint64_t checked_header_offset = 0;
    uint64_t payload_delta = 0;
    uint64_t payload_offset = 0;
    if (!CheckedMulU64(segment64, sizeof(SlabHeader), &checked_delta) ||
        !CheckedAddU64(headers_region_offset_, checked_delta,
                       &checked_header_offset) ||
        checked_header_offset != header_offset ||
        !CheckedMulU64(segment64, segment_size_, &payload_delta) ||
        !CheckedAddU64(payload_region_offset_, payload_delta, &payload_offset)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large-pool offset arithmetic overflow");
    }

    LargeObjectSlotMetadata metadata{
        .occupied = IsSegmentSet(segment),
        .generation = generations_[segment].load(std::memory_order_acquire),
        .class_id = kLargeObjectClassId,
        .capacity = segment_size_,
        .payload_offset = payload_offset,
        .object_extent = segment_size_,
        .segmented = true,
    };
    if (!metadata.occupied) {
        return metadata;
    }

    const ShmHandle handle{.offset = header_offset,
                           .generation = metadata.generation,
                           .region_id = region_id_};
    MINO_ASSIGN_OR_RETURN(LargeObjectPlan plan, InspectPlan(handle));
    if (plan.segments.empty() ||
        plan.segments.front().segment_index != segment ||
        plan.segments.front().payload_offset != payload_offset) {
        return Status::Error(StatusCode::kCorruption,
                             "large object metadata does not name segment 0");
    }
    if (!CheckedMulU64(plan.segments.size(), segment_size_,
                       &metadata.object_extent)) {
        return Status::Error(StatusCode::kCorruption,
                             "large object whole extent overflow");
    }
    return metadata;
}

Result<uint32_t> LargeObjectPool::ResolveLocked(ShmHandle handle) const {
    if (handle.offset == 0 && handle.generation == 0 && handle.region_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument, "null handle");
    }
    if (handle.region_id != region_id_ ||
        handle.offset < headers_region_offset_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "foreign or metadata-area large object handle");
    }
    const uint64_t relative = handle.offset - headers_region_offset_;
    if (relative % sizeof(SlabHeader) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large object handle is not header-aligned");
    }
    const uint64_t index = relative / sizeof(SlabHeader);
    if (index >= segment_count_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large object handle exceeds pool headers");
    }
    const uint32_t segment = static_cast<uint32_t>(index);
    if (!IsSegmentSet(segment)) {
        return Status::Error(StatusCode::kNotFound,
                             "large object segment is not allocated");
    }
    if (generations_[segment].load(std::memory_order_acquire) !=
        handle.generation) {
        return Status::Error(StatusCode::kNotFound,
                             "stale large object handle generation");
    }
    return segment;
}

bool LargeObjectPool::IsSegmentSet(uint32_t segment_index) const noexcept {
    if (segment_index >= segment_count_) {
        return false;
    }
    return (segment_bitmap_[segment_index / 64u].load(std::memory_order_acquire) &
            (uint64_t{1} << (segment_index % 64u))) != 0;
}

void LargeObjectPool::ClearSegmentBit(uint32_t segment_index) noexcept {
    segment_bitmap_[segment_index / 64u].fetch_and(
        ~(uint64_t{1} << (segment_index % 64u)), std::memory_order_acq_rel);
}

bool LargeObjectPool::IsSegmentOccupiedForRecovery(
    uint32_t segment_index) const noexcept {
    return IsSegmentSet(segment_index);
}

bool LargeObjectPool::ReadSegmentForRecovery(
    uint32_t segment_index, SlabHeader* header_out) const noexcept {
    if (segment_index >= segment_count_) {
        return false;
    }
    if (header_out != nullptr) {
        CopyHeader(headers_[segment_index], header_out);
    }
    return true;
}

uint32_t LargeObjectPool::AuthoritativeGenerationForRecovery(
    uint32_t segment_index) const noexcept {
    return segment_index < segment_count_
               ? generations_[segment_index].load(std::memory_order_acquire)
               : 0;
}

Result<ShmHandle> LargeObjectPool::HandleForRecovery(
    uint32_t segment_index) const {
    if (segment_index >= segment_count_ || !IsSegmentSet(segment_index)) {
        return Status::Error(StatusCode::kNotFound,
                             "large object recovery segment is free");
    }
    uint64_t delta = 0;
    uint64_t offset = 0;
    if (!CheckedMulU64(segment_index, sizeof(SlabHeader), &delta) ||
        !CheckedAddU64(headers_region_offset_, delta, &offset)) {
        return Status::Error(StatusCode::kCorruption,
                             "large object recovery handle overflow");
    }
    return ShmHandle{.offset = offset,
                     .generation = generations_[segment_index].load(
                         std::memory_order_acquire),
                     .region_id = region_id_};
}

Status LargeObjectPool::ClearSegmentForRecovery(uint32_t segment_index,
                                                uint32_t expected_state) {
    if (segment_index >= segment_count_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery segment index out of range");
    }
    if (!IsSegmentSet(segment_index)) {
        return Status::Ok();
    }
    SlabHeader& header = headers_[segment_index];
    uint32_t observed = expected_state;
    if (expected_state != StateBits(ObjectState::kReclaiming) &&
        !header.object_state.compare_exchange_strong(
            observed, StateBits(ObjectState::kReclaiming),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "large recovery segment state changed");
    }
    header.object_state.store(StateBits(ObjectState::kFree),
                              std::memory_order_release);
    ClearSegmentBit(segment_index);
    return Status::Ok();
}

Status LargeObjectPool::ClearStaleStateForRecovery(
    uint32_t segment_index, uint32_t expected_state) {
    if (segment_index >= segment_count_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery segment index out of range");
    }
    if (IsSegmentSet(segment_index)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "large recovery segment became occupied");
    }
    uint32_t observed = expected_state;
    if (headers_[segment_index].object_state.compare_exchange_strong(
            observed, StateBits(ObjectState::kFree),
            std::memory_order_acq_rel, std::memory_order_acquire) ||
        observed == StateBits(ObjectState::kFree)) {
        return Status::Ok();
    }
    return Status::Error(StatusCode::kWouldBlock,
                         "large stale segment state changed");
}

Status LargeObjectPool::ClearObjectForRecovery(uint32_t first_segment,
                                               uint32_t expected_state) {
    if (first_segment >= segment_count_ || !IsSegmentSet(first_segment)) {
        return Status::Error(StatusCode::kNotFound,
                             "large recovery object segment 0 is free");
    }
    const SlabHeader& first = headers_[first_segment];
    if (!VerifyImmutableHeader(first) ||
        first.class_id != kLargeObjectClassId ||
        first.capacity != segment_size_ || first.object_size == 0 ||
        first.object_size > max_object_size_ ||
        first.allocation_role.load(std::memory_order_acquire) != 0 ||
        first.object_state.load(std::memory_order_acquire) != expected_state) {
        return Status::Error(StatusCode::kCorruption,
                             "large recovery object segment 0 is invalid");
    }
    const uint32_t count = static_cast<uint32_t>(
        (static_cast<uint64_t>(first.object_size) + segment_size_ - 1u) /
        segment_size_);
    uint32_t end = 0;
    if (!CheckedAddU32(first_segment, count, &end) || end > segment_count_) {
        return Status::Error(StatusCode::kCorruption,
                             "large recovery run exceeds pool bounds");
    }
    for (uint32_t i = 0; i < count; ++i) {
        const uint32_t segment = first_segment + i;
        if (!IsSegmentSet(segment)) {
            continue;  // Idempotent completion of a partially reclaimed run.
        }
        const SlabHeader& header = headers_[segment];
        if (!VerifyImmutableHeader(header) ||
            header.class_id != kLargeObjectClassId ||
            header.object_size != first.object_size ||
            header.allocation_role.load(std::memory_order_acquire) != i ||
            header.generation.load(std::memory_order_acquire) !=
                generations_[segment].load(std::memory_order_acquire)) {
            return Status::Error(StatusCode::kCorruption,
                                 "large recovery continuation is invalid");
        }
    }
    for (uint32_t i = count; i-- > 0;) {
        const uint32_t segment = first_segment + i;
        if (!IsSegmentSet(segment)) {
            continue;
        }
        const uint32_t state =
            headers_[segment].object_state.load(std::memory_order_acquire);
        MINO_RETURN_IF_ERROR(ClearSegmentForRecovery(segment, state));
    }
    return Status::Ok();
}

}  // namespace mino
