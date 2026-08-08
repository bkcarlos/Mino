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

#include "mino/shm/allocator/central_slab.h"

#include <array>
#include <atomic>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <new>
#include <thread>

#include "mino/common/checked_arithmetic.h"

namespace mino {
namespace {

// Magic and version of the allocator superblock ("MSLA" little-endian).
constexpr uint32_t kAllocatorMagic = 0x4D534C41u;
constexpr uint32_t kAllocatorVersion = 3;

// Shared-memory allocator superblock, followed by class descriptors, the
// sharded bitmap, the generation array, and the per-slot headers/payloads.
// All offsets are relative to shm_base so that the Region can be mapped at
// different addresses in different processes.
struct alignas(64) AllocatorSuperblock {
    uint32_t magic;
    uint32_t version;
    uint32_t region_id;
    uint32_t class_count;

    uint64_t total_size;        // Total Region size in bytes.
    uint64_t metadata_size;     // Size of the metadata area (superblock +
                                // descriptors + bitmap + generations +
                                // draining flags), i.e. offset of slot area.
    uint64_t slot_stride;       // sizeof(SlabHeader) + max payload per slot.
    uint32_t total_slot_count;
    uint32_t reserved;
    std::atomic<uint64_t> next_transaction_id{1};
    // Offset from this superblock to the first SlabHeader. Zero is accepted
    // when attaching allocator v3 images and means metadata_size.
    uint64_t slot_area_offset = 0;

    // Followed by:
    //   ClassDescriptor classes[class_count]
    //   std::atomic<uint64_t> bitmap[bitmap_words]
    //   std::atomic<uint32_t> generations[total_slot_count]
    //   std::atomic<uint32_t> draining[class_count]
    //   (padding to slot_stride alignment)
    //   slots: [SlabHeader + payload] * total_slot_count
};

static_assert(sizeof(AllocatorSuperblock) == 64);

constexpr uint64_t AlignUp(uint64_t v, uint64_t a) { return (v + a - 1) / a * a; }

std::atomic<uint64_t> g_next_local_cache_id{1};

struct ThreadCursorMagazine {
    uint64_t cache_id = 0;
    uint64_t epoch = 0;
    std::array<uint32_t, kMaxClassCount> next_bits{};
    std::array<bool, kMaxClassCount> valid{};
};

thread_local ThreadCursorMagazine g_thread_cursor_magazine;

uint32_t GetThreadCursor(uint64_t cache_id, uint64_t epoch,
                         uint16_t class_id, uint32_t begin,
                         uint32_t slot_count) {
    ThreadCursorMagazine& magazine = g_thread_cursor_magazine;
    if (magazine.cache_id != cache_id || magazine.epoch != epoch) {
        magazine.cache_id = cache_id;
        magazine.epoch = epoch;
        magazine.valid.fill(false);
    }
    if (!magazine.valid[class_id]) {
        const uint64_t thread_hash = static_cast<uint64_t>(
            std::hash<std::thread::id>{}(std::this_thread::get_id()));
        const uint64_t seed = thread_hash ^ (cache_id * 0x9E3779B97F4A7C15ull) ^
                              static_cast<uint64_t>(class_id);
        magazine.next_bits[class_id] =
            begin + static_cast<uint32_t>(seed % slot_count);
        magazine.valid[class_id] = true;
    }
    return magazine.next_bits[class_id];
}

void SetThreadCursor(uint64_t cache_id, uint64_t epoch, uint16_t class_id,
                     uint32_t next_bit) {
    ThreadCursorMagazine& magazine = g_thread_cursor_magazine;
    if (magazine.cache_id != cache_id || magazine.epoch != epoch) {
        magazine.cache_id = cache_id;
        magazine.epoch = epoch;
        magazine.valid.fill(false);
    }
    magazine.next_bits[class_id] = next_bit;
    magazine.valid[class_id] = true;
}

}  // namespace

struct CentralSlabAllocator::Layout {
    uint64_t metadata_size = 0;
    uint64_t slot_stride = 0;
    uint32_t bitmap_words = 0;
};

struct CentralSlabAllocator::LocalCacheState {
    LocalCacheState()
        : cache_id(g_next_local_cache_id.fetch_add(1,
                                                   std::memory_order_relaxed)) {}

    const uint64_t cache_id;
    std::atomic<uint64_t> epoch{1};
    std::atomic<bool> enabled{true};
    std::atomic<uint64_t> hint_hits{0};
    std::atomic<uint64_t> fallback_scans{0};
    std::atomic<uint64_t> cache_bypasses{0};
    std::atomic<uint64_t> exhaustions{0};
    std::atomic<uint64_t> drain_count{0};
};

Result<CentralSlabAllocator::Layout> CentralSlabAllocator::ComputeLayout(
    const ClassTable& table, uint64_t data_region_size) {
    Layout layout;
    const uint32_t class_count = table.class_count();
    const uint32_t total_slots = table.total_slot_count();

    layout.bitmap_words = (total_slots + kBitmapShardBits - 1) / kBitmapShardBits;

    // Metadata area: superblock + descriptors + bitmap + generations +
    // draining flags, all naturally aligned.
    uint64_t off = sizeof(AllocatorSuperblock);
    off += sizeof(ClassDescriptor) * class_count;
    off = AlignUp(off, alignof(std::atomic<uint64_t>));
    off += sizeof(std::atomic<uint64_t>) * layout.bitmap_words;
    off = AlignUp(off, alignof(std::atomic<uint32_t>));
    off += sizeof(std::atomic<uint32_t>) * total_slots;
    off += sizeof(std::atomic<uint32_t>) * class_count;

    // Slot area begins at the next cache-line boundary; each slot is
    // SlabHeader + max payload, so every slot header stays cache-line
    // aligned.
    layout.slot_stride = AlignUp(
        sizeof(SlabHeader) + table.max_object_size(), alignof(SlabHeader));
    layout.metadata_size = AlignUp(off, alignof(SlabHeader));

    const uint64_t required =
        layout.metadata_size + layout.slot_stride * total_slots;
    if (data_region_size < required) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "data region too small for configured classes");
    }
    return layout;
}

Result<CentralSlabAllocator> CentralSlabAllocator::Create(
    void* shm_base, uint64_t data_region_size, const ClassTableConfig& config) {
    return CreateWithStorage(shm_base, data_region_size, data_region_size,
                             /*slot_area_offset=*/0, data_region_size,
                             /*region_id=*/0, /*handle_offset_bias=*/0, config);
}

Result<CentralSlabAllocator> CentralSlabAllocator::CreateInRegion(
    const RegionAllocatorStorage& storage, const ClassTableConfig& config) {
    if (storage.region_base == nullptr || storage.allocator_offset > storage.region_size ||
        storage.data_offset > storage.region_size ||
        storage.data_offset < storage.allocator_offset ||
        storage.allocator_size > storage.region_size - storage.allocator_offset ||
        storage.allocator_size > storage.data_offset - storage.allocator_offset ||
        storage.data_size > storage.region_size - storage.data_offset ||
        storage.region_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid Region allocator storage");
    }
    const uint64_t available_size = storage.region_size - storage.allocator_offset;
    const uint64_t slot_area_offset = storage.data_offset - storage.allocator_offset;
    return CreateWithStorage(
        static_cast<std::byte*>(storage.region_base) + storage.allocator_offset,
        available_size, storage.allocator_size, slot_area_offset,
        storage.data_size, storage.region_id, storage.allocator_offset, config);
}

Result<CentralSlabAllocator> CentralSlabAllocator::CreateWithStorage(
    void* shm_base, uint64_t available_size, uint64_t metadata_capacity,
    uint64_t slot_area_offset, uint64_t slot_capacity, uint32_t region_id,
    uint64_t handle_offset_bias, const ClassTableConfig& config) {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument, "shm_base is null");
    }
    if (reinterpret_cast<uintptr_t>(shm_base) % alignof(AllocatorSuperblock) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator base is not cache-line aligned");
    }
    MINO_ASSIGN_OR_RETURN(ClassTable table, ClassTable::Create(config));
    MINO_ASSIGN_OR_RETURN(Layout layout, ComputeLayout(table, available_size));
    if (slot_area_offset == 0) {
        slot_area_offset = layout.metadata_size;
    }
    const uint64_t slot_bytes = layout.slot_stride * table.total_slot_count();
    if (metadata_capacity < layout.metadata_size ||
        slot_area_offset < layout.metadata_size ||
        slot_area_offset % alignof(SlabHeader) != 0 ||
        slot_capacity < slot_bytes || slot_area_offset > available_size ||
        slot_bytes > available_size - slot_area_offset) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "Region allocator metadata or data area is too small");
    }

    auto* super = new (shm_base) AllocatorSuperblock{};
    super->magic = kAllocatorMagic;
    super->version = kAllocatorVersion;
    super->region_id = region_id;
    super->class_count = table.class_count();
    super->total_size = available_size;
    super->metadata_size = layout.metadata_size;
    super->slot_stride = layout.slot_stride;
    super->total_slot_count = table.total_slot_count();
    super->reserved = 0;
    super->next_transaction_id.store(1, std::memory_order_relaxed);
    super->slot_area_offset = slot_area_offset;

    auto* base = static_cast<std::byte*>(shm_base);
    auto* descriptors = reinterpret_cast<ClassDescriptor*>(
        base + sizeof(AllocatorSuperblock));
    for (uint16_t i = 0; i < table.class_count(); ++i) {
        descriptors[i] = table.GetClass(i);
    }

    auto* bitmap_words = reinterpret_cast<std::atomic<uint64_t>*>(
        base + AlignUp(sizeof(AllocatorSuperblock) +
                           sizeof(ClassDescriptor) * table.class_count(),
                       alignof(std::atomic<uint64_t>)));
    for (uint32_t i = 0; i < layout.bitmap_words; ++i) {
        new (&bitmap_words[i]) std::atomic<uint64_t>(0);
    }
    auto* generations = reinterpret_cast<std::atomic<uint32_t>*>(
        reinterpret_cast<std::byte*>(bitmap_words) +
        sizeof(std::atomic<uint64_t>) * layout.bitmap_words);
    for (uint32_t i = 0; i < table.total_slot_count(); ++i) {
        new (&generations[i]) std::atomic<uint32_t>(0);
    }
    auto* draining = reinterpret_cast<std::atomic<uint32_t>*>(
        reinterpret_cast<std::byte*>(generations) +
        sizeof(std::atomic<uint32_t>) * table.total_slot_count());
    for (uint32_t i = 0; i < table.class_count(); ++i) {
        new (&draining[i]) std::atomic<uint32_t>(0);
    }

    MINO_ASSIGN_OR_RETURN(ShardedBitmap bitmap,
                          ShardedBitmap::Create(bitmap_words, layout.bitmap_words));
    MINO_ASSIGN_OR_RETURN(GenerationArray gen_array,
                          GenerationArray::Create(generations,
                                                  table.total_slot_count()));

    CentralSlabAllocator alloc;
    alloc.shm_base_ = shm_base;
    alloc.class_table_ = std::move(table);
    alloc.bitmap_ = bitmap;
    alloc.generations_ = gen_array;
    alloc.headers_ = reinterpret_cast<SlabHeader*>(base + slot_area_offset);
    alloc.class_draining_ = draining;
    alloc.data_region_size_ = available_size;
    alloc.region_id_ = region_id;
    alloc.slot_stride_ = layout.slot_stride;
    alloc.handle_offset_bias_ = handle_offset_bias;
    alloc.next_transaction_id_ = &super->next_transaction_id;
    alloc.local_cache_state_ = std::make_shared<LocalCacheState>();
    return alloc;
}

Result<bool> CentralSlabAllocator::HasAllocatorMetadata(
    const void* shm_base, uint64_t available_size) {
    if (shm_base == nullptr || available_size < sizeof(uint32_t)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator metadata probe is out of bounds");
    }
    const uint32_t magic = *static_cast<const uint32_t*>(shm_base);
    if (magic == 0) {
        return false;
    }
    if (magic != kAllocatorMagic) {
        return Status::Error(StatusCode::kCorruption, "bad allocator magic");
    }
    return true;
}

Result<CentralSlabAllocator> CentralSlabAllocator::Attach(void* shm_base) {
    if (shm_base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument, "shm_base is null");
    }
    auto* super = static_cast<AllocatorSuperblock*>(shm_base);
    if (super->magic != kAllocatorMagic) {
        return Status::Error(StatusCode::kCorruption, "bad allocator magic");
    }
    return AttachWithBias(shm_base, super->total_size,
                          /*handle_offset_bias=*/0);
}

Result<CentralSlabAllocator> CentralSlabAllocator::Attach(
    void* shm_base, uint64_t available_size) {
    return AttachWithBias(shm_base, available_size,
                          /*handle_offset_bias=*/0);
}

Result<CentralSlabAllocator> CentralSlabAllocator::AttachInRegion(
    const RegionAllocatorStorage& storage) {
    if (storage.region_base == nullptr || storage.allocator_offset > storage.region_size ||
        storage.data_offset < storage.allocator_offset ||
        storage.data_offset > storage.region_size ||
        storage.allocator_size > storage.data_offset - storage.allocator_offset ||
        storage.data_size > storage.region_size - storage.data_offset) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid Region allocator storage");
    }
    const uint64_t available_size = storage.region_size - storage.allocator_offset;
    MINO_ASSIGN_OR_RETURN(
        CentralSlabAllocator allocator,
        AttachWithBias(
            static_cast<std::byte*>(storage.region_base) + storage.allocator_offset,
            available_size, storage.allocator_offset));
    const auto* super = static_cast<const AllocatorSuperblock*>(allocator.shm_base_);
    const uint64_t persisted_slot_offset =
        super->slot_area_offset == 0 ? super->metadata_size
                                     : super->slot_area_offset;
    const uint64_t slot_bytes =
        super->slot_stride * static_cast<uint64_t>(super->total_slot_count);
    if (persisted_slot_offset != storage.data_offset - storage.allocator_offset ||
        super->metadata_size > storage.allocator_size ||
        slot_bytes > storage.data_size ||
        super->total_size != available_size ||
        allocator.region_id_ != storage.region_id) {
        return Status::Error(StatusCode::kCorruption,
                             "allocator metadata does not match Region layout");
    }
    return allocator;
}

Result<CentralSlabAllocator> CentralSlabAllocator::AttachWithBias(
    void* shm_base, uint64_t available_size, uint64_t handle_offset_bias) {
    if (shm_base == nullptr || available_size < sizeof(AllocatorSuperblock)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator metadata is too small");
    }
    if (reinterpret_cast<uintptr_t>(shm_base) % alignof(AllocatorSuperblock) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator base is not cache-line aligned");
    }
    auto* super = static_cast<AllocatorSuperblock*>(shm_base);
    if (super->magic != kAllocatorMagic) {
        return Status::Error(StatusCode::kCorruption, "bad allocator magic");
    }
    if (super->version != kAllocatorVersion || super->class_count == 0 ||
        super->class_count > kMaxClassCount || super->total_size > available_size ||
        super->total_size < sizeof(AllocatorSuperblock)) {
        return Status::Error(StatusCode::kCorruption,
                             "invalid allocator superblock");
    }
    const uint64_t descriptor_bytes =
        sizeof(ClassDescriptor) * static_cast<uint64_t>(super->class_count);
    if (descriptor_bytes > available_size - sizeof(AllocatorSuperblock)) {
        return Status::Error(StatusCode::kCorruption,
                             "allocator class table is out of bounds");
    }

    auto* base = static_cast<std::byte*>(shm_base);
    auto* descriptors = reinterpret_cast<ClassDescriptor*>(
        base + sizeof(AllocatorSuperblock));
    ClassTableConfig config;
    config.classes.reserve(super->class_count);
    for (uint32_t i = 0; i < super->class_count; ++i) {
        config.classes.push_back({.slot_size = descriptors[i].slot_size,
                                  .slot_count = descriptors[i].slot_count});
    }
    MINO_ASSIGN_OR_RETURN(ClassTable table, ClassTable::Create(config));
    for (uint16_t i = 0; i < table.class_count(); ++i) {
        const ClassDescriptor& expected = table.GetClass(i);
        if (descriptors[i].class_id != expected.class_id ||
            descriptors[i].slot_size != expected.slot_size ||
            descriptors[i].slot_count != expected.slot_count ||
            descriptors[i].bitmap_shard_offset !=
                expected.bitmap_shard_offset) {
            return Status::Error(StatusCode::kCorruption,
                                 "allocator class descriptor is inconsistent");
        }
    }
    MINO_ASSIGN_OR_RETURN(Layout layout, ComputeLayout(table, super->total_size));
    const uint64_t slot_area_offset =
        super->slot_area_offset == 0 ? super->metadata_size
                                     : super->slot_area_offset;
    const uint64_t slot_bytes = layout.slot_stride * table.total_slot_count();
    if (super->metadata_size != layout.metadata_size ||
        super->slot_stride != layout.slot_stride ||
        super->total_slot_count != table.total_slot_count() ||
        slot_area_offset < layout.metadata_size ||
        slot_area_offset % alignof(SlabHeader) != 0 ||
        slot_area_offset > super->total_size ||
        slot_bytes > super->total_size - slot_area_offset) {
        return Status::Error(StatusCode::kCorruption,
                             "allocator layout metadata is inconsistent");
    }

    auto* bitmap_words = reinterpret_cast<std::atomic<uint64_t>*>(
        base + AlignUp(sizeof(AllocatorSuperblock) +
                           sizeof(ClassDescriptor) * table.class_count(),
                       alignof(std::atomic<uint64_t>)));
    auto* generations = reinterpret_cast<std::atomic<uint32_t>*>(
        reinterpret_cast<std::byte*>(bitmap_words) +
        sizeof(std::atomic<uint64_t>) * layout.bitmap_words);
    auto* draining = reinterpret_cast<std::atomic<uint32_t>*>(
        reinterpret_cast<std::byte*>(generations) +
        sizeof(std::atomic<uint32_t>) * table.total_slot_count());

    MINO_ASSIGN_OR_RETURN(ShardedBitmap bitmap,
                          ShardedBitmap::Create(bitmap_words, layout.bitmap_words));
    MINO_ASSIGN_OR_RETURN(GenerationArray gen_array,
                          GenerationArray::Create(generations,
                                                  table.total_slot_count()));

    CentralSlabAllocator alloc;
    alloc.shm_base_ = shm_base;
    alloc.class_table_ = std::move(table);
    alloc.bitmap_ = bitmap;
    alloc.generations_ = gen_array;
    alloc.headers_ = reinterpret_cast<SlabHeader*>(base + slot_area_offset);
    alloc.class_draining_ = draining;
    alloc.data_region_size_ = super->total_size;
    alloc.region_id_ = super->region_id;
    alloc.slot_stride_ = layout.slot_stride;
    alloc.handle_offset_bias_ = handle_offset_bias;
    alloc.next_transaction_id_ = &super->next_transaction_id;
    alloc.local_cache_state_ = std::make_shared<LocalCacheState>();
    return alloc;
}

void CentralSlabAllocator::ConfigureLocalCache(
    AllocatorLocalCacheConfig config) noexcept {
    if (local_cache_state_ == nullptr) return;
    local_cache_state_->enabled.store(config.enabled, std::memory_order_release);
    DrainLocalCache();
}

AllocatorLocalCacheConfig CentralSlabAllocator::local_cache_config() const noexcept {
    return AllocatorLocalCacheConfig{
        .enabled = local_cache_state_ != nullptr &&
                   local_cache_state_->enabled.load(std::memory_order_acquire),
    };
}

AllocatorLocalCacheStats CentralSlabAllocator::local_cache_stats() const noexcept {
    if (local_cache_state_ == nullptr) return {};
    return AllocatorLocalCacheStats{
        .hint_hits = local_cache_state_->hint_hits.load(std::memory_order_relaxed),
        .fallback_scans =
            local_cache_state_->fallback_scans.load(std::memory_order_relaxed),
        .cache_bypasses =
            local_cache_state_->cache_bypasses.load(std::memory_order_relaxed),
        .exhaustions =
            local_cache_state_->exhaustions.load(std::memory_order_relaxed),
        .drain_count =
            local_cache_state_->drain_count.load(std::memory_order_relaxed),
    };
}

void CentralSlabAllocator::DrainLocalCache() noexcept {
    if (local_cache_state_ == nullptr) return;
    local_cache_state_->epoch.fetch_add(1, std::memory_order_acq_rel);
    local_cache_state_->drain_count.fetch_add(1, std::memory_order_relaxed);
}

Result<ShmHandle> CentralSlabAllocator::Allocate(const AllocationRequest& request) {
    // Step 1: checked-align the request size. Alignment must be a power of
    // two; the aligned size must not overflow.
    const uint32_t alignment = request.alignment == 0 ? 1 : request.alignment;
    if ((request.allocation_flags & ~kAllocationFlagMask) != 0 ||
        ((request.allocation_flags & kAllocationFlagTransactionRoot) != 0 &&
         (request.allocation_flags & kAllocationFlagTransactionChild) != 0) ||
        ((request.owner_epoch == 0) !=
         (request.allocation_transaction_id == 0)) ||
        (request.allocation_transaction_id == 0 &&
         request.allocation_flags != 0)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid allocation transaction metadata");
    }
    if ((alignment & (alignment - 1)) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "alignment must be a power of two");
    }
    const uint64_t aligned_size =
        (static_cast<uint64_t>(request.object_size) + alignment - 1) / alignment * alignment;
    if (aligned_size > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument, "object size overflow");
    }

    // Step 2: select the smallest fitting class.
    MINO_ASSIGN_OR_RETURN(const uint16_t class_id,
                          class_table_.FindClass(static_cast<uint32_t>(aligned_size)));
    const ClassDescriptor& cls = class_table_.GetClass(class_id);

    // Reject allocations into a class that has been marked DRAINING.
    if (class_draining_[class_id].load(std::memory_order_acquire) != 0) {
        return Status::Error(StatusCode::kUnavailable, "size class is draining");
    }

    // Steps 3-5: use a bounded process-local cursor magazine to select a shard,
    // then claim directly in the shared bitmap. No free slot is ever held in the
    // magazine, so process death cannot hide capacity from Attach/recovery.
    const uint32_t range_begin = cls.bitmap_shard_offset;
    const uint32_t range_end = range_begin + cls.slot_count;
    uint32_t bit_index = 0;
    LocalCacheState* const cache = local_cache_state_.get();
    if (cache != nullptr && cache->enabled.load(std::memory_order_acquire)) {
        const uint64_t epoch = cache->epoch.load(std::memory_order_acquire);
        const uint32_t hint = GetThreadCursor(
            cache->cache_id, epoch, class_id, range_begin, cls.slot_count);
        Result<BitmapClaim> claim = bitmap_.FindAndSetFreeBitInRangeHinted(
            range_begin, range_end, hint);
        if (!claim.ok()) {
            cache->fallback_scans.fetch_add(1, std::memory_order_relaxed);
            if (claim.status().code() == StatusCode::kResourceExhausted) {
                cache->exhaustions.fetch_add(1, std::memory_order_relaxed);
            }
            return claim.status();
        }
        bit_index = claim->bit_index;
        if (claim->shards_probed == 1) {
            cache->hint_hits.fetch_add(1, std::memory_order_relaxed);
        } else {
            cache->fallback_scans.fetch_add(1, std::memory_order_relaxed);
        }
        const uint32_t next_bit =
            bit_index + 1 == range_end ? range_begin : bit_index + 1;
        SetThreadCursor(cache->cache_id, epoch, class_id, next_bit);
    } else {
        if (cache != nullptr) {
            cache->cache_bypasses.fetch_add(1, std::memory_order_relaxed);
        }
        Result<uint32_t> claim =
            bitmap_.FindAndSetFreeBitInRange(range_begin, range_end);
        if (!claim.ok()) {
            if (cache != nullptr &&
                claim.status().code() == StatusCode::kResourceExhausted) {
                cache->exhaustions.fetch_add(1, std::memory_order_relaxed);
            }
            return claim.status();
        }
        bit_index = *claim;
    }

    const uint32_t slot_index = bit_index;
    SlabHeader& header = HeaderAt(slot_index);
    uint32_t free_state = static_cast<uint32_t>(ObjectState::kFree);
    if (!header.object_state.compare_exchange_strong(
            free_state, static_cast<uint32_t>(ObjectState::kAllocating),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        (void)bitmap_.ClearBit(slot_index);
        return Status::Error(StatusCode::kCorruption,
                             "free bitmap slot has a non-free lifecycle state");
    }

    // Step 6: increment the authoritative generation and copy it into the
    // header; refuse to wrap at UINT32_MAX by marking the class DRAINING.
    const uint32_t generation = generations_.Increment(slot_index);
    if (generation == kGenerationDraining) {
        class_draining_[class_id].store(1, std::memory_order_release);
        // Undo the lifecycle/bitmap claim so the slot stays free for recovery.
        header.object_state.store(static_cast<uint32_t>(ObjectState::kFree),
                                  std::memory_order_release);
        bitmap_.ClearBit(slot_index);
        return Status::Error(StatusCode::kResourceExhausted,
                             "generation exhausted; class marked DRAINING");
    }

    header.magic = kSlabHeaderMagic;
    header.header_version = kSlabHeaderVersion;
    header.class_id = class_id;
    header.generation.store(generation, std::memory_order_relaxed);
    header.capacity = cls.slot_size;
    header.object_size = request.object_size;
    header.type_id = request.type_id.value;
    header.layout_version = request.schema.layout_version;
    header.schema_short_id = request.schema.short_id;

    // Step 7: transaction identity is written before kAllocated is published.
    // Journal recovery can therefore find the allocation even if its process
    // dies before appending the returned Handle to the journal record.
    header.owner_epoch.store(request.owner_epoch, std::memory_order_relaxed);
    header.allocation_transaction_id.store(
        request.allocation_transaction_id, std::memory_order_relaxed);
    header.allocation_role.store(request.allocation_flags,
                                 std::memory_order_relaxed);

    // The safety stamp is immutable for this generation and CRC-covered.
    header.immutable_header_crc = ComputeImmutableHeaderCrc(header);

    // Step 8: single publication point. Everything above must be visible to
    // any thread that observes kAllocated with acquire ordering.
    header.object_state.store(static_cast<uint32_t>(ObjectState::kAllocated),
                              std::memory_order_release);

    // Step 9: return the Handle. offset is relative to shm_base so the
    // Handle stays valid across different mappings of the same Region.
    ShmHandle handle;
    handle.offset = handle_offset_bias_ + static_cast<uint64_t>(
        reinterpret_cast<std::byte*>(&header) -
        static_cast<std::byte*>(shm_base_));
    handle.generation = generation;
    handle.region_id = region_id_;
    return handle;
}

Result<uint64_t> CentralSlabAllocator::NextAllocationTransactionId() {
    if (next_transaction_id_ == nullptr) {
        return Status::Error(StatusCode::kInternal,
                             "allocator transaction counter is unavailable");
    }
    uint64_t current = next_transaction_id_->load(std::memory_order_relaxed);
    for (;;) {
        if (current == 0 || current == std::numeric_limits<uint64_t>::max()) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "allocation transaction id exhausted");
        }
        if (next_transaction_id_->compare_exchange_weak(
                current, current + 1, std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return current;
        }
    }
}

Status CentralSlabAllocator::PublishTransactionHandle(
    uint64_t owner_epoch, uint64_t transaction_id, ShmHandle handle,
    uint32_t required_role) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));
    SlabHeader& header = HeaderAt(slot_index);
    if (header.generation.load(std::memory_order_acquire) != handle.generation ||
        header.owner_epoch.load(std::memory_order_acquire) != owner_epoch ||
        header.allocation_transaction_id.load(std::memory_order_acquire) !=
            transaction_id ||
        (header.allocation_role.load(std::memory_order_acquire) & required_role) ==
            0 ||
        !VerifyImmutableHeader(header)) {
        return Status::Error(StatusCode::kCorruption,
                             "manifest handle stamp does not match transaction");
    }
    uint32_t state = header.object_state.load(std::memory_order_acquire);
    if (state == static_cast<uint32_t>(ObjectState::kPublished)) {
        return Status::Ok();
    }
    if (state != static_cast<uint32_t>(ObjectState::kBuilding) ||
        !header.object_state.compare_exchange_strong(
            state, static_cast<uint32_t>(ObjectState::kPublished),
            std::memory_order_release, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "transaction object is not fully built");
    }
    return Status::Ok();
}

Status CentralSlabAllocator::PublishTransaction(
    uint64_t owner_epoch, uint64_t transaction_id,
    std::span<const ShmHandle> handles, ShmHandle root) {
    if (owner_epoch == 0 || transaction_id == 0 || root.IsNull() ||
        handles.empty() || handles.front() != root) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid allocation transaction manifest");
    }
    for (size_t i = handles.size(); i > 1; --i) {
        if (!handles[i - 1].IsNull()) {
            MINO_RETURN_IF_ERROR(PublishTransactionHandle(
                owner_epoch, transaction_id, handles[i - 1],
                kAllocationFlagTransactionChild));
        }
    }
    return PublishTransactionHandle(owner_epoch, transaction_id, root,
                                    kAllocationFlagTransactionRoot);
}

bool CentralSlabAllocator::CanReclaim(ShmHandle handle) const noexcept {
    return reclaim_guard_ == nullptr ||
           reclaim_guard_(handle, reclaim_guard_context_);
}

void CentralSlabAllocator::RememberLocalCursor(uint16_t class_id,
                                               uint32_t slot_index) noexcept {
    LocalCacheState* const cache = local_cache_state_.get();
    if (cache == nullptr ||
        !cache->enabled.load(std::memory_order_acquire) ||
        class_id >= class_table_.class_count()) {
        return;
    }
    const ClassDescriptor& cls = class_table_.GetClass(class_id);
    if (slot_index < cls.bitmap_shard_offset ||
        slot_index - cls.bitmap_shard_offset >= cls.slot_count) {
        return;
    }
    const uint64_t epoch = cache->epoch.load(std::memory_order_acquire);
    SetThreadCursor(cache->cache_id, epoch, class_id, slot_index);
}

Status CentralSlabAllocator::ReclaimSlotExact(uint32_t slot_index,
                                              ShmHandle handle,
                                              bool allow_published) {
    SlabHeader& header = HeaderAt(slot_index);
    for (;;) {
        if (!bitmap_.IsSet(slot_index) ||
            generations_.Get(slot_index) != handle.generation ||
            header.generation.load(std::memory_order_acquire) !=
                handle.generation) {
            return Status::Error(StatusCode::kNotFound,
                                 "stale handle during reclaim");
        }
        uint32_t state = header.object_state.load(std::memory_order_acquire);
        if (state == static_cast<uint32_t>(ObjectState::kReclaiming)) {
            return Status::Ok();
        }
        if (state == static_cast<uint32_t>(ObjectState::kPublished) &&
            !allow_published) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "published slot must be retired first");
        }
        if (state != static_cast<uint32_t>(ObjectState::kRetired) &&
            state != static_cast<uint32_t>(ObjectState::kAborting) &&
            state != static_cast<uint32_t>(ObjectState::kAllocated) &&
            state != static_cast<uint32_t>(ObjectState::kBuilding) &&
            state != static_cast<uint32_t>(ObjectState::kAllocating) &&
            !(allow_published &&
              state == static_cast<uint32_t>(ObjectState::kPublished))) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "slot is not reclaimable");
        }
        if (!CanReclaim(handle)) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "slot has a live or in-flight Pin");
        }
        if (!header.object_state.compare_exchange_weak(
                state, static_cast<uint32_t>(ObjectState::kReclaiming),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            continue;
        }
        // The bitmap remains set while kReclaiming is owned, so Allocate cannot
        // advance the generation. Recheck all ABA-sensitive identity before the
        // irreversible bitmap clear.
        if (!bitmap_.IsSet(slot_index) ||
            generations_.Get(slot_index) != handle.generation ||
            header.generation.load(std::memory_order_acquire) !=
                handle.generation ||
            !CanReclaim(handle)) {
            uint32_t reclaiming =
                static_cast<uint32_t>(ObjectState::kReclaiming);
            (void)header.object_state.compare_exchange_strong(
                reclaiming, state, std::memory_order_release,
                std::memory_order_relaxed);
            return Status::Error(StatusCode::kWouldBlock,
                                 "reclaim identity or Pin guard changed");
        }
        // Publish kFree while the bitmap still excludes Allocate; only this
        // exact-generation reclaimer can clear the bit, so reuse cannot race the
        // final state store.
        header.object_state.store(static_cast<uint32_t>(ObjectState::kFree),
                                  std::memory_order_release);
        const Status cleared = bitmap_.ClearBit(slot_index);
        if (cleared.ok()) RememberLocalCursor(header.class_id, slot_index);
        return cleared;
    }
}

Status CentralSlabAllocator::ReclaimTransaction(
    uint64_t owner_epoch, uint64_t transaction_id,
    std::span<const ShmHandle> handles) {
    if (owner_epoch == 0 || transaction_id == 0 || handles.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid allocation transaction manifest");
    }
    for (size_t i = handles.size(); i > 0; --i) {
        const ShmHandle handle = handles[i - 1];
        if (handle.IsNull()) continue;
        Result<uint32_t> slot_index = ResolveLocked(handle);
        if (!slot_index.ok()) {
            if (slot_index.status().code() == StatusCode::kNotFound) continue;
            return slot_index.status();
        }
        SlabHeader& header = HeaderAt(*slot_index);
        if (header.owner_epoch.load(std::memory_order_acquire) != owner_epoch ||
            header.allocation_transaction_id.load(std::memory_order_acquire) !=
                transaction_id ||
            !VerifyImmutableHeader(header)) {
            return Status::Error(StatusCode::kCorruption,
                                 "manifest reclaim stamp mismatch");
        }
        MINO_RETURN_IF_ERROR(ReclaimSlotExact(*slot_index, handle, true));
    }
    return Status::Ok();
}

Status CentralSlabAllocator::ReclaimTransactionAppendGap(
    uint64_t owner_epoch, uint64_t transaction_id) {
    if (owner_epoch == 0 || transaction_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid allocation transaction identity");
    }
    for (uint32_t i = 0; i < total_slot_count(); ++i) {
        if (!bitmap_.IsSet(i)) continue;
        SlabHeader& header = HeaderAt(i);
        const uint32_t state =
            header.object_state.load(std::memory_order_acquire);
        if (state == static_cast<uint32_t>(ObjectState::kFree) ||
            state == static_cast<uint32_t>(ObjectState::kReclaiming) ||
            header.owner_epoch.load(std::memory_order_acquire) != owner_epoch ||
            header.allocation_transaction_id.load(std::memory_order_acquire) !=
                transaction_id) {
            continue;
        }
        const uint32_t generation = generations_.Get(i);
        if (header.generation.load(std::memory_order_acquire) != generation ||
            !VerifyImmutableHeader(header)) {
            continue;
        }
        ShmHandle handle{.offset = handle_offset_bias_ + static_cast<uint64_t>(
                             reinterpret_cast<std::byte*>(&header) -
                             static_cast<std::byte*>(shm_base_)),
                         .generation = generation,
                         .region_id = region_id_};
        MINO_RETURN_IF_ERROR(ReclaimSlotExact(i, handle, true));
    }
    return Status::Ok();
}

Result<MutableBuildView> CentralSlabAllocator::BeginBuild(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    if (!VerifyImmutableHeader(header)) {
        return Status::Error(StatusCode::kCorruption,
                             "immutable header CRC mismatch");
    }
    uint32_t expected = static_cast<uint32_t>(ObjectState::kAllocated);
    if (!header.object_state.compare_exchange_strong(
            expected, static_cast<uint32_t>(ObjectState::kBuilding),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "slot is not available for exclusive build");
    }

    MutableBuildView view;
    view.handle = handle;
    view.capacity = header.capacity;
    view.object_size = header.object_size;
    view.type_id = TypeId{header.type_id};
    view.schema_short_id = header.schema_short_id;
    view.layout_version = header.layout_version;
    view.data = reinterpret_cast<std::byte*>(&header) + sizeof(SlabHeader);
    return view;
}

Status CentralSlabAllocator::Publish(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    if (!VerifyImmutableHeader(header)) {
        return Status::Error(StatusCode::kCorruption,
                             "immutable header CRC mismatch");
    }
    uint32_t expected = static_cast<uint32_t>(ObjectState::kBuilding);
    if (!header.object_state.compare_exchange_strong(
            expected, static_cast<uint32_t>(ObjectState::kPublished),
            std::memory_order_release, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "slot is not in kBuilding state");
    }
    return Status::Ok();
}

Status CentralSlabAllocator::Abort(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    for (;;) {
        const uint32_t state =
            header.object_state.load(std::memory_order_acquire);
        if (state == static_cast<uint32_t>(ObjectState::kAborting)) {
            break;
        }
        if (state != static_cast<uint32_t>(ObjectState::kAllocated) &&
            state != static_cast<uint32_t>(ObjectState::kBuilding)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "only unpublished objects may be aborted");
        }
        uint32_t expected = state;
        if (header.object_state.compare_exchange_weak(
                expected, static_cast<uint32_t>(ObjectState::kAborting),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
    }
    return Reclaim(handle);
}

Status CentralSlabAllocator::Retire(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    SlabHeader& header = HeaderAt(slot_index);
    // Transition ALLOCATED/BUILDING/PUBLISHED -> RETIRED. If already RETIRED
    // this is a no-op; any other state (FREE, ABORTING) is rejected.
    uint32_t expected = static_cast<uint32_t>(ObjectState::kAllocated);
    for (;;) {
        const uint32_t state = header.object_state.load(std::memory_order_acquire);
        if (state == static_cast<uint32_t>(ObjectState::kRetired)) {
            return Status::Ok();
        }
        if (state != static_cast<uint32_t>(ObjectState::kAllocated) &&
            state != static_cast<uint32_t>(ObjectState::kBuilding) &&
            state != static_cast<uint32_t>(ObjectState::kPublished)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "slot is not in a retirable state");
        }
        expected = state;
        if (header.object_state.compare_exchange_weak(
                expected, static_cast<uint32_t>(ObjectState::kRetired),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            return Status::Ok();
        }
    }
}

Status CentralSlabAllocator::Reclaim(ShmHandle handle) {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));
    SlabHeader& header = HeaderAt(slot_index);
    const uint64_t owner_epoch =
        header.owner_epoch.load(std::memory_order_acquire);
    const uint64_t transaction_id =
        header.allocation_transaction_id.load(std::memory_order_acquire);
    const uint32_t role =
        header.allocation_role.load(std::memory_order_acquire);
    if ((role & kAllocationFlagTransactionRoot) != 0 && owner_epoch != 0 &&
        transaction_id != 0) {
        if (!CanReclaim(handle)) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "transaction root has a live or in-flight Pin");
        }
        // The Journal record may already be finalized after Channel publication.
        // Transaction stamps remain in every SlabHeader, so root retirement can
        // still reclaim the complete graph. This recovery-safe fallback is
        // bounded by allocator capacity; a dedicated persistent graph index can
        // replace the scan without changing the ownership contract.
        return ReclaimTransactionAppendGap(owner_epoch, transaction_id);
    }
    return ReclaimSlotExact(slot_index, handle, false);
}

Result<ObjectState> CentralSlabAllocator::InspectState(
    ShmHandle handle) const {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));
    const SlabHeader& header = HeaderAt(slot_index);
    const ObjectState state = static_cast<ObjectState>(
        header.object_state.load(std::memory_order_acquire));
    if (generations_.Get(slot_index) != handle.generation ||
        header.generation.load(std::memory_order_acquire) != handle.generation) {
        return Status::Error(StatusCode::kNotFound,
                             "slot changed while reading lifecycle state");
    }
    return state;
}

Result<SlabView> CentralSlabAllocator::Inspect(ShmHandle handle) const {
    MINO_ASSIGN_OR_RETURN(const uint32_t slot_index, ResolveLocked(handle));

    const SlabHeader& header = HeaderAt(slot_index);
    if (!VerifyImmutableHeader(header)) {
        return Status::Error(StatusCode::kCorruption,
                             "immutable header CRC mismatch");
    }

    SlabView view;
    view.handle = handle;
    view.state = static_cast<ObjectState>(
        header.object_state.load(std::memory_order_acquire));
    view.class_id = header.class_id;
    view.generation = generations_.Get(slot_index);
    view.capacity = header.capacity;
    view.object_size = header.object_size;
    view.type_id = TypeId{header.type_id};
    view.schema_short_id = header.schema_short_id;
    view.layout_version = header.layout_version;
    view.owner_epoch = header.owner_epoch.load(std::memory_order_acquire);
    view.allocation_transaction_id =
        header.allocation_transaction_id.load(std::memory_order_acquire);
    view.allocation_flags =
        header.allocation_role.load(std::memory_order_acquire) &
        kAllocationFlagMask;
    view.data = reinterpret_cast<const std::byte*>(&header) + sizeof(SlabHeader);
    return view;
}

Result<CentralSlabSlotMetadata> CentralSlabAllocator::GetSlotMetadata(
    uint64_t header_offset) const {
    if (shm_base_ == nullptr || headers_ == nullptr || !class_table_.valid() ||
        slot_stride_ == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator metadata facade is not initialized");
    }

    const uint64_t headers_local_offset = static_cast<uint64_t>(
        reinterpret_cast<const std::byte*>(headers_) -
        static_cast<const std::byte*>(shm_base_));
    uint64_t first_header_offset = 0;
    if (!CheckedAddU64(handle_offset_bias_, headers_local_offset,
                       &first_header_offset) ||
        header_offset < first_header_offset) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "offset is below the allocator slot area");
    }

    const uint64_t relative = header_offset - first_header_offset;
    if (relative % slot_stride_ != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "offset is not an exact allocator slot start");
    }
    const uint64_t slot_index64 = relative / slot_stride_;
    if (slot_index64 >= total_slot_count() ||
        slot_index64 > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator slot offset is out of range");
    }
    const uint32_t slot_index = static_cast<uint32_t>(slot_index64);

    uint64_t checked_delta = 0;
    uint64_t checked_offset = 0;
    if (!CheckedMulU64(slot_index64, slot_stride_, &checked_delta) ||
        !CheckedAddU64(first_header_offset, checked_delta, &checked_offset) ||
        checked_offset != header_offset) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator slot offset arithmetic overflow");
    }

    const uint16_t class_id = ClassIdForRecovery(slot_index);
    if (class_id >= class_table_.class_count()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "offset names a reserved bitmap slot");
    }
    const ClassDescriptor& cls = class_table_.GetClass(class_id);
    return CentralSlabSlotMetadata{
        .occupied = bitmap_.IsSet(slot_index),
        .generation = generations_.Get(slot_index),
        .class_id = class_id,
        .class_count = class_table_.class_count(),
        .capacity = cls.slot_size,
    };
}

bool CentralSlabAllocator::ReadSlotByIndex(uint32_t slot_index, SlabHeader* header_out,
                                           const void** data_out) const {
    if (slot_index >= total_slot_count()) {
        return false;
    }
    const SlabHeader& header = HeaderAt(slot_index);
    if (header_out != nullptr) {
        // Copy fields under acquire ordering; the header is read-only after
        // publication for the fields that matter here.
        header_out->magic = header.magic;
        header_out->header_version = header.header_version;
        header_out->class_id = header.class_id;
        header_out->generation.store(
            header.generation.load(std::memory_order_acquire),
            std::memory_order_relaxed);
        header_out->object_state.store(
            header.object_state.load(std::memory_order_acquire),
            std::memory_order_relaxed);
        header_out->capacity = header.capacity;
        header_out->object_size = header.object_size;
        header_out->type_id = header.type_id;
        header_out->layout_version = header.layout_version;
        header_out->schema_short_id = header.schema_short_id;
        header_out->owner_epoch.store(
            header.owner_epoch.load(std::memory_order_acquire),
            std::memory_order_relaxed);
        header_out->allocation_transaction_id.store(
            header.allocation_transaction_id.load(std::memory_order_acquire),
            std::memory_order_relaxed);
        header_out->immutable_header_crc = header.immutable_header_crc;
        header_out->allocation_role.store(
            header.allocation_role.load(std::memory_order_acquire),
            std::memory_order_relaxed);
    }
    if (data_out != nullptr) {
        *data_out = reinterpret_cast<const std::byte*>(&header) + sizeof(SlabHeader);
    }
    return true;
}

bool CentralSlabAllocator::IsSlotOccupiedForRecovery(
    uint32_t slot_index) const noexcept {
    return slot_index < total_slot_count() && bitmap_.IsSet(slot_index);
}

uint32_t CentralSlabAllocator::AuthoritativeGenerationForRecovery(
    uint32_t slot_index) const noexcept {
    return slot_index < total_slot_count() ? generations_.Get(slot_index) : 0;
}

uint16_t CentralSlabAllocator::ClassIdForRecovery(
    uint32_t slot_index) const noexcept {
    for (uint16_t class_id = 0; class_id < class_table_.class_count(); ++class_id) {
        const ClassDescriptor& cls = class_table_.GetClass(class_id);
        if (slot_index >= cls.bitmap_shard_offset &&
            slot_index - cls.bitmap_shard_offset < cls.slot_count) {
            return class_id;
        }
    }
    return class_table_.class_count();
}

Status CentralSlabAllocator::ClearSlotForRecovery(uint32_t slot_index,
                                                  uint32_t expected_state) {
    if (slot_index >= total_slot_count()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery slot index out of range");
    }
    SlabHeader& header = HeaderAt(slot_index);
    uint32_t observed = header.object_state.load(std::memory_order_acquire);
    if (!bitmap_.IsSet(slot_index)) {
        return observed == static_cast<uint32_t>(ObjectState::kFree)
                   ? Status::Ok()
                   : Status::Error(StatusCode::kWouldBlock,
                                   "recovery slot is no longer occupied");
    }
    if (observed != expected_state) {
        return Status::Error(StatusCode::kWouldBlock,
                             "recovery slot state changed");
    }
    if (observed != static_cast<uint32_t>(ObjectState::kReclaiming) &&
        !header.object_state.compare_exchange_strong(
            observed, static_cast<uint32_t>(ObjectState::kReclaiming),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "recovery slot state raced");
    }
    uint32_t reclaiming = static_cast<uint32_t>(ObjectState::kReclaiming);
    if (!header.object_state.compare_exchange_strong(
            reclaiming, static_cast<uint32_t>(ObjectState::kFree),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (reclaiming == static_cast<uint32_t>(ObjectState::kFree) &&
            !bitmap_.IsSet(slot_index)) {
            return Status::Ok();
        }
        return Status::Error(StatusCode::kWouldBlock,
                             "recovery slot was completed or reused");
    }
    Status cleared = bitmap_.ClearBit(slot_index);
    if (cleared.code() == StatusCode::kNotFound) {
        return Status::Ok();
    }
    return cleared;
}

Status CentralSlabAllocator::ClearStaleStateForRecovery(
    uint32_t slot_index, uint32_t expected_state) {
    if (slot_index >= total_slot_count()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery slot index out of range");
    }
    if (bitmap_.IsSet(slot_index)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "slot became occupied during recovery");
    }
    SlabHeader& header = HeaderAt(slot_index);
    uint32_t observed = expected_state;
    if (header.object_state.compare_exchange_strong(
            observed, static_cast<uint32_t>(ObjectState::kFree),
            std::memory_order_acq_rel, std::memory_order_acquire) ||
        observed == static_cast<uint32_t>(ObjectState::kFree)) {
        return Status::Ok();
    }
    return Status::Error(StatusCode::kWouldBlock,
                         "stale slot state changed during recovery");
}

SlabHeader& CentralSlabAllocator::HeaderAt(uint32_t slot_index) {
    auto* base = reinterpret_cast<std::byte*>(headers_);
    return *reinterpret_cast<SlabHeader*>(base + slot_index * slot_stride_);
}

const SlabHeader& CentralSlabAllocator::HeaderAt(uint32_t slot_index) const {
    const auto* base = reinterpret_cast<const std::byte*>(headers_);
    return *reinterpret_cast<const SlabHeader*>(base + slot_index * slot_stride_);
}

Result<uint32_t> CentralSlabAllocator::ResolveLocked(ShmHandle handle) const {
    if (handle.offset == 0 && handle.generation == 0 && handle.region_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument, "null handle");
    }
    if (handle.region_id != region_id_) {
        return Status::Error(StatusCode::kInvalidArgument, "foreign region handle");
    }
    if (handle.offset < handle_offset_bias_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "handle offset below allocator base");
    }

    // Map Region-relative offset -> allocator-local slot index. Offsets always
    // point at a SlabHeader.
    const uint64_t rel = handle.offset - handle_offset_bias_;
    if (rel < sizeof(AllocatorSuperblock) || rel >= data_region_size_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "handle offset out of range");
    }
    const uint64_t metadata_size =
        reinterpret_cast<const std::byte*>(headers_) -
        static_cast<const std::byte*>(shm_base_);
    if (rel < metadata_size || (rel - metadata_size) % slot_stride_ != 0) {
        return Status::Error(StatusCode::kInvalidArgument, "handle offset not slot-aligned");
    }
    const uint64_t slot_index64 =
        (rel - metadata_size) / slot_stride_;
    if (slot_index64 >= total_slot_count() ||
        slot_index64 > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument, "slot index out of range");
    }
    const uint32_t slot_index = static_cast<uint32_t>(slot_index64);
    if (slot_index >= total_slot_count()) {
        return Status::Error(StatusCode::kInvalidArgument, "slot index out of range");
    }
    if (!bitmap_.IsSet(slot_index)) {
        return Status::Error(StatusCode::kNotFound, "slot is not allocated");
    }
    const uint32_t current_gen = generations_.Get(slot_index);
    if (current_gen != handle.generation ||
        HeaderAt(slot_index).generation.load(std::memory_order_acquire) !=
            handle.generation) {
        return Status::Error(StatusCode::kNotFound,
                             "stale handle (generation mismatch)");
    }
    return slot_index;
}

}  // namespace mino
