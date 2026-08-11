// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/shm/allocator/large_object_pool.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <utility>

#include "mino/common/checked_arithmetic.h"
#include "mino/shm/allocator/generation_array.h"

namespace mino {
namespace {

constexpr uint32_t kLargePoolMagic = 0x4D4C504Fu;  // "MLPO"
constexpr uint16_t kLargePoolVersion = 2;
constexpr uint32_t kDefaultSegmentSize = 64u * 1024u;
constexpr uint16_t kLargeObjectClassId = 0xFFFFu;
constexpr uint32_t kPurposeMask = 0x7u;
constexpr uint32_t kHugeRequestedBit = 1u << 3;
constexpr uint32_t kHugeActualBit = 1u << 4;
constexpr uint32_t kHugeFallbackShift = 8;
constexpr uint32_t kHugeFallbackMask = 0xFFu << kHugeFallbackShift;
constexpr uint32_t kKnownPoolFlags = kPurposeMask | kHugeRequestedBit |
                                     kHugeActualBit | kHugeFallbackMask;

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

bool IsRegisteredPurpose(LargeObjectPoolPurpose purpose) {
    return purpose == LargeObjectPoolPurpose::kDma ||
           purpose == LargeObjectPoolPurpose::kRdmaRegistered;
}

MemoryRegistrationKind RegistrationKindFor(LargeObjectPoolPurpose purpose) {
    return purpose == LargeObjectPoolPurpose::kRdmaRegistered
               ? MemoryRegistrationKind::kRdma
               : MemoryRegistrationKind::kDma;
}

LargeObjectRegistration RegistrationRequirementFor(
    LargeObjectPoolPurpose purpose) {
    if (purpose == LargeObjectPoolPurpose::kDma) {
        return LargeObjectRegistration::kDma;
    }
    if (purpose == LargeObjectPoolPurpose::kRdmaRegistered) {
        return LargeObjectRegistration::kRdma;
    }
    return LargeObjectRegistration::kNone;
}

bool IsValidPurpose(uint32_t value) {
    return value <= static_cast<uint32_t>(
                        LargeObjectPoolPurpose::kRdmaRegistered);
}

uint32_t EncodePoolFlags(const LargeObjectPoolOptions& options) {
    uint32_t flags = static_cast<uint32_t>(options.purpose);
    if (options.huge_pages.requested) flags |= kHugeRequestedBit;
    if (options.huge_pages.actual) flags |= kHugeActualBit;
    flags |= (static_cast<uint32_t>(options.huge_pages.fallback_reason) <<
              kHugeFallbackShift) &
             kHugeFallbackMask;
    return flags;
}

Status ValidateOptions(const LargeObjectPoolOptions& options,
                       bool require_registration_provider) {
    if (options.huge_pages.actual && !options.huge_pages.requested) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "actual HugePage backing was not requested");
    }
    if (options.purpose == LargeObjectPoolPurpose::kHugePage &&
        !options.huge_pages.requested) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "HugePage pool requires a backing observation");
    }
    if (options.huge_pages.strict && options.huge_pages.requested &&
        !options.huge_pages.actual) {
        return Status::Error(StatusCode::kUnavailable,
                             "strict HugePage pool cannot use fallback backing");
    }
    if (!IsRegisteredPurpose(options.purpose)) return Status::Ok();
    if (options.registration_scope_id == 0 ||
        !options.registration_owner.valid() ||
        options.registration_quota_bytes == 0 ||
        options.minimum_registered_object_bytes == 0) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "registered pool requires scope, owner, quota and minimum size");
    }
    if (!require_registration_provider) return Status::Ok();
    if (options.registration_provider == nullptr ||
        options.registration_provider->provider_class() ==
            MemoryRegistrationProviderClass::kUnavailable ||
        !options.registration_provider->Supports(
            RegistrationKindFor(options.purpose))) {
        return Status::Error(StatusCode::kUnsupported,
                             "requested registration provider is unavailable");
    }
    return Status::Ok();
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

struct LargeObjectPool::LocalNumaState {
    NumaTopology topology;
    const NumaSystem* system = nullptr;
    std::vector<uint32_t> effective_nodes;
    bool placement_fallback = false;
    std::atomic<uint64_t> local_allocations{0};
    std::atomic<uint64_t> remote_allocations{0};
    std::atomic<uint64_t> fallback_allocations{0};
    std::atomic<uint64_t> bind_errors{0};
};

struct LargeObjectPool::LocalSpecializedState {
    struct Registration {
        RegisteredMemory memory;
        MemoryRegistrationOwner lease;
        LargeObjectLifetime lifetime = LargeObjectLifetime::kAllocation;
        uint64_t pins = 0;
    };

    MemoryRegistrationProvider* provider = nullptr;
    MemoryRegistrationProviderClass provider_class =
        MemoryRegistrationProviderClass::kUnavailable;
    uint64_t scope_id = 0;
    MemoryRegistrationOwner owner;
    uint64_t quota_bytes = 0;
    uint64_t minimum_object_bytes = 0;
    mutable std::mutex mutex;
    std::map<std::pair<uint64_t, uint32_t>, Registration> registrations;
    std::atomic<uint64_t> registration_bytes{0};
    std::atomic<uint64_t> allocations{0};
    std::atomic<uint64_t> allocation_failures{0};
    std::atomic<uint64_t> huge_page_fallback_allocations{0};
    std::atomic<uint64_t> registration_failures{0};
    std::atomic<uint64_t> registrations_recovered{0};
    std::atomic<uint64_t> registration_recovery_bytes{0};
};

Result<LargeObjectPool> LargeObjectPool::Create(
    void* shm_base, uint64_t pool_size, uint32_t max_object_size,
    uint32_t segment_size, const NumaPlacementConfig& numa_config) {
    LargeObjectPoolOptions options;
    options.numa = numa_config;
    return Create(shm_base, pool_size, max_object_size, segment_size, options);
}

Result<LargeObjectPool> LargeObjectPool::Create(
    void* shm_base, uint64_t pool_size, uint32_t max_object_size,
    uint32_t segment_size, const LargeObjectPoolOptions& options) {
    return Create(LargeObjectPoolStorage{.region_base = shm_base,
                                         .region_size = pool_size,
                                         .pool_offset = 0,
                                         .pool_size = pool_size,
                                         .region_id = 0},
                  max_object_size, segment_size, options);
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

Result<LargeObjectPool> LargeObjectPool::Attach(
    void* shm_base, uint64_t pool_size, uint32_t region_id,
    const LargeObjectPoolOptions& options) {
    return Attach(LargeObjectPoolStorage{.region_base = shm_base,
                                         .region_size = pool_size,
                                         .pool_offset = 0,
                                         .pool_size = pool_size,
                                         .region_id = region_id},
                  options);
}

Result<LargeObjectPool> LargeObjectPool::Create(
    const LargeObjectPoolStorage& storage, uint32_t max_object_size,
    uint32_t segment_size, const NumaPlacementConfig& numa_config) {
    LargeObjectPoolOptions options;
    options.numa = numa_config;
    return Create(storage, max_object_size, segment_size, options);
}

Result<LargeObjectPool> LargeObjectPool::Create(
    const LargeObjectPoolStorage& storage, uint32_t max_object_size,
    uint32_t segment_size, const LargeObjectPoolOptions& options) {
    MINO_RETURN_IF_ERROR(ValidateStorage(storage));
    MINO_RETURN_IF_ERROR(ValidateOptions(options, /*require_provider=*/true));
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
    if (storage.pool_size > std::numeric_limits<size_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "NUMA placement extent exceeds addressable size");
    }
    MINO_ASSIGN_OR_RETURN(
        NumaPlacementResult placement,
        ApplyNumaPlacement(pool_base, static_cast<size_t>(storage.pool_size),
                           options.numa));
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
    super->reserved0 = EncodePoolFlags(options);
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
    pool.purpose_ = options.purpose;
    pool.huge_pages_requested_ = options.huge_pages.requested;
    pool.huge_pages_actual_ = options.huge_pages.actual;
    pool.actual_page_size_ = options.huge_pages.actual_page_size;
    pool.huge_page_fallback_reason_ = options.huge_pages.fallback_reason;
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
    pool.local_numa_state_ = std::make_shared<LocalNumaState>();
    pool.local_numa_state_->topology = std::move(placement.topology);
    pool.local_numa_state_->system = options.numa.system == nullptr
                                         ? &NativeNumaSystem()
                                         : options.numa.system;
    pool.local_numa_state_->effective_nodes =
        std::move(placement.effective_nodes);
    pool.local_numa_state_->placement_fallback = placement.fallback;
    if (placement.bind_error) {
        pool.local_numa_state_->bind_errors.store(1,
                                                  std::memory_order_relaxed);
    }
    pool.local_specialized_state_ = std::make_shared<LocalSpecializedState>();
    pool.local_specialized_state_->provider = options.registration_provider;
    pool.local_specialized_state_->provider_class =
        options.registration_provider == nullptr
            ? MemoryRegistrationProviderClass::kUnavailable
            : options.registration_provider->provider_class();
    pool.local_specialized_state_->scope_id = options.registration_scope_id;
    pool.local_specialized_state_->owner = options.registration_owner;
    pool.local_specialized_state_->quota_bytes =
        options.registration_quota_bytes;
    pool.local_specialized_state_->minimum_object_bytes =
        options.minimum_registered_object_bytes;
    if (IsRegisteredPurpose(pool.purpose_) &&
        options.recover_stale_registrations) {
        MINO_ASSIGN_OR_RETURN(
            MemoryRegistrationRecoveryResult recovered,
            options.registration_provider->RecoverStale({
                .scope_id = options.registration_scope_id,
                .current_process_id = options.registration_owner.process_id,
                .current_process_epoch = options.registration_owner.process_epoch,
            }));
        pool.local_specialized_state_->registrations_recovered.store(
            recovered.registrations_released, std::memory_order_relaxed);
        pool.local_specialized_state_->registration_recovery_bytes.store(
            recovered.bytes_released, std::memory_order_relaxed);
    }
    return pool;
}

Result<LargeObjectPool> LargeObjectPool::Attach(
    const LargeObjectPoolStorage& storage) {
    return Attach(storage, LargeObjectPoolOptions{});
}

Result<LargeObjectPool> LargeObjectPool::Attach(
    const LargeObjectPoolStorage& storage,
    const LargeObjectPoolOptions& options) {
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
    const uint32_t flags = super->reserved0;
    const uint32_t purpose_bits = flags & kPurposeMask;
    const uint32_t fallback_bits =
        (flags & kHugeFallbackMask) >> kHugeFallbackShift;
    if ((flags & ~kKnownPoolFlags) != 0 || !IsValidPurpose(purpose_bits) ||
        fallback_bits > static_cast<uint32_t>(
                            HugePageFallbackReason::kSystemError) ||
        ((flags & kHugeActualBit) != 0 &&
         (flags & kHugeRequestedBit) == 0) ||
        super->max_object_size == 0 || super->segment_size < 64 ||
        (super->segment_size & (super->segment_size - 1u)) != 0 ||
        super->segment_count == 0 ||
        std::atomic_ref(const_cast<uint32_t&>(super->draining))
                .load(std::memory_order_acquire) > 1) {
        return Status::Error(StatusCode::kCorruption,
                             "large pool configuration is invalid");
    }
    const auto persisted_purpose =
        static_cast<LargeObjectPoolPurpose>(purpose_bits);
    const bool explicit_registration = options.registration_provider != nullptr ||
                                       options.registration_scope_id != 0;
    if (explicit_registration) {
        if (options.purpose != persisted_purpose) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "registration options do not match pool purpose");
        }
        MINO_RETURN_IF_ERROR(
            ValidateOptions(options, /*require_registration_provider=*/true));
    }
    if (options.huge_pages.strict && (flags & kHugeRequestedBit) != 0 &&
        (flags & kHugeActualBit) == 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "strict HugePage attach rejected fallback backing");
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
    pool.purpose_ = persisted_purpose;
    pool.huge_pages_requested_ = (flags & kHugeRequestedBit) != 0;
    pool.huge_pages_actual_ = (flags & kHugeActualBit) != 0;
    pool.actual_page_size_ = options.huge_pages.actual_page_size;
    pool.huge_page_fallback_reason_ =
        static_cast<HugePageFallbackReason>(fallback_bits);
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
    pool.local_numa_state_ = std::make_shared<LocalNumaState>();
    pool.local_numa_state_->system = &NativeNumaSystem();
    Result<NumaTopology> topology =
        pool.local_numa_state_->system->DiscoverTopology();
    if (topology.ok()) {
        pool.local_numa_state_->topology = std::move(*topology);
    } else {
        pool.local_numa_state_->placement_fallback = true;
        pool.local_numa_state_->topology.fallback_reason =
            topology.status().ToString();
    }
    pool.local_specialized_state_ = std::make_shared<LocalSpecializedState>();
    if (explicit_registration) {
        pool.local_specialized_state_->provider = options.registration_provider;
        pool.local_specialized_state_->provider_class =
            options.registration_provider->provider_class();
        pool.local_specialized_state_->scope_id = options.registration_scope_id;
        pool.local_specialized_state_->owner = options.registration_owner;
        pool.local_specialized_state_->quota_bytes =
            options.registration_quota_bytes;
        pool.local_specialized_state_->minimum_object_bytes =
            options.minimum_registered_object_bytes;
        if (options.recover_stale_registrations) {
            MINO_ASSIGN_OR_RETURN(
                MemoryRegistrationRecoveryResult recovered,
                options.registration_provider->RecoverStale({
                    .scope_id = options.registration_scope_id,
                    .current_process_id = options.registration_owner.process_id,
                    .current_process_epoch =
                        options.registration_owner.process_epoch,
                }));
            pool.local_specialized_state_->registrations_recovered.store(
                recovered.registrations_released, std::memory_order_relaxed);
            pool.local_specialized_state_->registration_recovery_bytes.store(
                recovered.bytes_released, std::memory_order_relaxed);
        }
    }
    return pool;
}

bool LargeObjectPool::is_draining() const noexcept {
    return draining_ != nullptr &&
           std::atomic_ref(*draining_).load(std::memory_order_acquire) != 0;
}

LargeObjectNumaStats LargeObjectPool::numa_stats() const noexcept {
    if (local_numa_state_ == nullptr) return {};
    return {
        .local_allocations = local_numa_state_->local_allocations.load(
            std::memory_order_relaxed),
        .remote_allocations = local_numa_state_->remote_allocations.load(
            std::memory_order_relaxed),
        .fallback_allocations = local_numa_state_->fallback_allocations.load(
            std::memory_order_relaxed),
        .bind_errors = local_numa_state_->bind_errors.load(
            std::memory_order_relaxed),
    };
}

MemoryRegistrationProviderClass
LargeObjectPool::registration_provider_class() const noexcept {
    return local_specialized_state_ == nullptr
               ? MemoryRegistrationProviderClass::kUnavailable
               : local_specialized_state_->provider_class;
}

LargeObjectPoolMetrics LargeObjectPool::metrics() const noexcept {
    LargeObjectPoolMetrics result;
    result.capacity_bytes =
        static_cast<uint64_t>(segment_count_) * segment_size_;
    uint64_t occupied_segments = 0;
    uint64_t current_free = 0;
    uint64_t largest_free = 0;
    for (uint32_t segment = 0; segment < segment_count_; ++segment) {
        if (!IsSegmentSet(segment)) {
            ++current_free;
            largest_free = std::max(largest_free, current_free);
            continue;
        }
        current_free = 0;
        ++occupied_segments;
        const SlabHeader& header = headers_[segment];
        if (header.allocation_role.load(std::memory_order_acquire) == 0 &&
            VerifyImmutableHeader(header) && header.object_size != 0 &&
            header.object_size <= max_object_size_) {
            result.allocated_object_bytes += header.object_size;
        }
    }
    result.reserved_extent_bytes = occupied_segments * segment_size_;
    result.free_bytes = result.capacity_bytes - result.reserved_extent_bytes;
    result.internal_fragmentation_bytes =
        result.reserved_extent_bytes >= result.allocated_object_bytes
            ? result.reserved_extent_bytes - result.allocated_object_bytes
            : 0;
    result.largest_free_extent_bytes = largest_free * segment_size_;
    result.external_fragmentation_bytes =
        result.free_bytes >= result.largest_free_extent_bytes
            ? result.free_bytes - result.largest_free_extent_bytes
            : 0;
    if (local_specialized_state_ != nullptr) {
        result.allocations = local_specialized_state_->allocations.load(
            std::memory_order_relaxed);
        result.allocation_failures =
            local_specialized_state_->allocation_failures.load(
                std::memory_order_relaxed);
        result.huge_page_fallback_allocations =
            local_specialized_state_->huge_page_fallback_allocations.load(
                std::memory_order_relaxed);
        result.registration_bytes =
            local_specialized_state_->registration_bytes.load(
                std::memory_order_relaxed);
        result.registration_failures =
            local_specialized_state_->registration_failures.load(
                std::memory_order_relaxed);
        result.registrations_recovered =
            local_specialized_state_->registrations_recovered.load(
                std::memory_order_relaxed);
        result.registration_recovery_bytes =
            local_specialized_state_->registration_recovery_bytes.load(
                std::memory_order_relaxed);
    }
    return result;
}

Result<uint32_t> LargeObjectPool::FindAndClaimExtent(
    uint32_t segments_needed, uint64_t alignment) {
    for (uint32_t attempt = 0; attempt < segment_count_; ++attempt) {
        uint32_t best_start = segment_count_;
        uint32_t best_run_length = std::numeric_limits<uint32_t>::max();
        uint32_t run_begin = 0;
        while (run_begin < segment_count_) {
            while (run_begin < segment_count_ && IsSegmentSet(run_begin)) {
                ++run_begin;
            }
            uint32_t run_end = run_begin;
            while (run_end < segment_count_ && !IsSegmentSet(run_end)) {
                ++run_end;
            }
            const uint32_t run_length = run_end - run_begin;
            if (run_length >= segments_needed) {
                for (uint32_t candidate = run_begin;
                     candidate <= run_end - segments_needed; ++candidate) {
                    const uintptr_t address = reinterpret_cast<uintptr_t>(
                        payload_base_ +
                        static_cast<uint64_t>(candidate) * segment_size_);
                    if (address % alignment == 0) {
                        if (run_length < best_run_length) {
                            best_start = candidate;
                            best_run_length = run_length;
                        }
                        break;
                    }
                }
            }
            run_begin = run_end + (run_end == run_begin ? 1u : 0u);
        }
        if (best_start == segment_count_) {
            return Status::Error(
                StatusCode::kResourceExhausted,
                "no aligned contiguous extent satisfies allocation request");
        }
        uint32_t claimed = 0;
        for (; claimed < segments_needed; ++claimed) {
            const uint32_t segment = best_start + claimed;
            const uint64_t mask = uint64_t{1} << (segment % 64u);
            auto& word = segment_bitmap_[segment / 64u];
            uint64_t expected = word.load(std::memory_order_acquire);
            while ((expected & mask) == 0 &&
                   !word.compare_exchange_weak(expected, expected | mask,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
            }
            if ((expected & mask) != 0) break;
        }
        if (claimed == segments_needed) return best_start;
        for (uint32_t index = 0; index < claimed; ++index) {
            ClearSegmentBit(best_start + index);
        }
    }
    return Status::Error(StatusCode::kResourceExhausted,
                         "large object extent claim lost under contention");
}

Status LargeObjectPool::PrepareRegistration(
    ShmHandle handle, const LargeObjectAllocationRequest& request,
    uint32_t segments_needed) {
    LocalSpecializedState& state = *local_specialized_state_;
    uint64_t bytes = 0;
    if (!CheckedMulU64(segments_needed, segment_size_, &bytes)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "registration extent size overflow");
    }
    uint64_t used = state.registration_bytes.load(std::memory_order_acquire);
    for (;;) {
        if (bytes > state.quota_bytes || used > state.quota_bytes - bytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "memory registration quota exceeded");
        }
        if (state.registration_bytes.compare_exchange_weak(
                used, used + bytes, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            break;
        }
    }

    MINO_ASSIGN_OR_RETURN(const uint32_t first, ResolveLocked(handle));
    const MemoryRegistrationOwner owner =
        request.lifetime == LargeObjectLifetime::kLease ? request.lease
                                                        : state.owner;
    auto registered = state.provider->Register({
        .address = payload_base_ + static_cast<uint64_t>(first) * segment_size_,
        .bytes = bytes,
        .alignment = request.alignment,
        .scope_id = state.scope_id,
        .kind = RegistrationKindFor(purpose_),
        .owner = owner,
        .require_physical_contiguous =
            request.contiguity == LargeObjectContiguity::kPhysical,
    });
    if (!registered.ok()) {
        state.registration_bytes.fetch_sub(bytes, std::memory_order_acq_rel);
        return registered.status();
    }
    const bool invalid =
        registered->registration_id == 0 || registered->bytes != bytes ||
        registered->kind != RegistrationKindFor(purpose_) ||
        registered->owner != owner ||
        (request.contiguity == LargeObjectContiguity::kPhysical &&
         !registered->physically_contiguous);
    if (invalid) {
        const Status cleanup = state.provider->Deregister(*registered);
        if (cleanup.ok()) {
            state.registration_bytes.fetch_sub(bytes, std::memory_order_acq_rel);
            return Status::Error(
                StatusCode::kCorruption,
                "registration provider returned invalid facts");
        }
        std::lock_guard lock(state.mutex);
        state.registrations.emplace(
            std::make_pair(handle.offset, handle.generation),
            LocalSpecializedState::Registration{
                .memory = *registered,
                .lease = owner,
                .lifetime = request.lifetime,
                .pins = 0,
            });
        return cleanup;
    }
    std::lock_guard lock(state.mutex);
    const auto [iterator, inserted] = state.registrations.emplace(
        std::make_pair(handle.offset, handle.generation),
        LocalSpecializedState::Registration{
            .memory = *registered,
            .lease = owner,
            .lifetime = request.lifetime,
            .pins = request.lifetime == LargeObjectLifetime::kLease ? 1u : 0u,
        });
    (void)iterator;
    if (!inserted) {
        const Status cleanup = state.provider->Deregister(*registered);
        state.registration_bytes.fetch_sub(bytes, std::memory_order_acq_rel);
        return cleanup.ok()
                   ? Status::Error(StatusCode::kAlreadyExists,
                                   "handle is already registered")
                   : cleanup;
    }
    return Status::Ok();
}

Status LargeObjectPool::ReleaseRegistration(ShmHandle handle,
                                            bool require_unpinned) {
    if (local_specialized_state_ == nullptr ||
        local_specialized_state_->provider == nullptr) {
        return IsRegisteredPurpose(purpose_)
                   ? Status::Error(
                         StatusCode::kUnavailable,
                         "registered-pool recovery requires its device provider")
                   : Status::Ok();
    }
    LocalSpecializedState& state = *local_specialized_state_;
    std::lock_guard lock(state.mutex);
    auto iterator = state.registrations.find(
        std::make_pair(handle.offset, handle.generation));
    if (iterator == state.registrations.end()) return Status::Ok();
    if (require_unpinned && iterator->second.pins != 0) {
        return Status::Error(StatusCode::kWouldBlock,
                             "registered object still has an active Pin lease");
    }
    const Status status = state.provider->Deregister(iterator->second.memory);
    if (!status.ok()) {
        state.registration_failures.fetch_add(1, std::memory_order_relaxed);
        return status;
    }
    state.registration_bytes.fetch_sub(iterator->second.memory.bytes,
                                       std::memory_order_acq_rel);
    state.registrations.erase(iterator);
    return Status::Ok();
}

Status LargeObjectPool::Pin(ShmHandle handle,
                            MemoryRegistrationOwner lease) {
    MINO_ASSIGN_OR_RETURN(const uint32_t first, ResolveLocked(handle));
    (void)first;
    if (!lease.valid() || local_specialized_state_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Pin requires a valid registered-buffer lease");
    }
    std::lock_guard lock(local_specialized_state_->mutex);
    auto iterator = local_specialized_state_->registrations.find(
        std::make_pair(handle.offset, handle.generation));
    if (iterator == local_specialized_state_->registrations.end()) {
        return Status::Error(StatusCode::kNotFound,
                             "registered buffer is not local to this process");
    }
    if (iterator->second.lease != lease) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "Pin lease does not own this registration");
    }
    ++iterator->second.pins;
    return Status::Ok();
}

Status LargeObjectPool::Unpin(ShmHandle handle,
                              MemoryRegistrationOwner lease) {
    MINO_ASSIGN_OR_RETURN(const uint32_t first, ResolveLocked(handle));
    (void)first;
    if (!lease.valid() || local_specialized_state_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Unpin requires a valid registered-buffer lease");
    }
    std::lock_guard lock(local_specialized_state_->mutex);
    auto iterator = local_specialized_state_->registrations.find(
        std::make_pair(handle.offset, handle.generation));
    if (iterator == local_specialized_state_->registrations.end()) {
        return Status::Error(StatusCode::kNotFound,
                             "registered buffer is not local to this process");
    }
    if (iterator->second.lease != lease) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "Unpin lease does not own this registration");
    }
    if (iterator->second.pins == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "registered buffer is already unpinned");
    }
    --iterator->second.pins;
    return Status::Ok();
}

Result<uint64_t> LargeObjectPool::ReleaseLease(
    MemoryRegistrationOwner lease) {
    if (!lease.valid() || local_specialized_state_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "lease release requires a valid lease");
    }
    LocalSpecializedState& state = *local_specialized_state_;
    std::lock_guard lock(state.mutex);
    uint64_t released = 0;
    for (auto iterator = state.registrations.begin();
         iterator != state.registrations.end();) {
        if (iterator->second.lease != lease) {
            ++iterator;
            continue;
        }
        const Status status = state.provider->Deregister(iterator->second.memory);
        if (!status.ok()) {
            state.registration_failures.fetch_add(1,
                                                  std::memory_order_relaxed);
            return status;
        }
        state.registration_bytes.fetch_sub(iterator->second.memory.bytes,
                                           std::memory_order_acq_rel);
        iterator = state.registrations.erase(iterator);
        ++released;
    }
    return released;
}

Result<ShmHandle> LargeObjectPool::Allocate(uint32_t object_size,
                                            TypeId type_id) {
    if (purpose_ != LargeObjectPoolPurpose::kNormal) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "specialized pool allocation requires an explicit request");
    }
    return Allocate(LargeObjectAllocationRequest{
        .object_size = object_size,
        .type_id = type_id,
        .purpose = LargeObjectPoolPurpose::kNormal,
        .alignment = 1,
        .contiguity = LargeObjectContiguity::kVirtual,
        .registration = LargeObjectRegistration::kNone,
        .lifetime = LargeObjectLifetime::kAllocation,
        .lease = {},
    });
}

Result<ShmHandle> LargeObjectPool::Allocate(
    const LargeObjectAllocationRequest& request) {
    const uint32_t object_size = request.object_size;
    const TypeId type_id = request.type_id;
    const bool registered = IsRegisteredPurpose(purpose_);
    if (object_size == 0 || object_size > max_object_size_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large object size is outside pool bounds");
    }
    if (request.purpose != purpose_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation purpose does not match isolated pool");
    }
    if (request.registration != RegistrationRequirementFor(purpose_)) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "allocation registration requirement does not match pool purpose");
    }
    if (request.alignment == 0 ||
        (request.alignment & (request.alignment - 1u)) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "large object alignment must be a power of two");
    }
    if (!registered &&
        (request.contiguity == LargeObjectContiguity::kPhysical ||
         request.lifetime == LargeObjectLifetime::kLease)) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "physical contiguity and lease lifetime require a registered pool");
    }
    if (registered) {
        if (local_specialized_state_ == nullptr ||
            local_specialized_state_->provider == nullptr ||
            local_specialized_state_->provider_class ==
                MemoryRegistrationProviderClass::kUnavailable) {
            return Status::Error(StatusCode::kUnsupported,
                                 "registered pool has no device provider");
        }
        if (object_size < local_specialized_state_->minimum_object_bytes) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "object is below registered-pool minimum; use a normal pool");
        }
        if (request.lifetime == LargeObjectLifetime::kLease &&
            !request.lease.valid()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "lease lifetime requires a valid lease");
        }
    }
    if (is_draining()) {
        return Status::Error(StatusCode::kUnavailable,
                             "large object pool is draining");
    }
    const uint32_t segments_needed = static_cast<uint32_t>(
        (static_cast<uint64_t>(object_size) + segment_size_ - 1u) /
        segment_size_);
    auto extent = FindAndClaimExtent(segments_needed, request.alignment);
    if (!extent.ok()) {
        if (local_specialized_state_ != nullptr) {
            local_specialized_state_->allocation_failures.fetch_add(
                1, std::memory_order_relaxed);
        }
        return extent.status();
    }
    const uint32_t run_start = *extent;

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
        const MemoryRegistrationOwner owner =
            request.lifetime == LargeObjectLifetime::kLease
                ? request.lease
                : (local_specialized_state_ == nullptr
                       ? MemoryRegistrationOwner{}
                       : local_specialized_state_->owner);
        header.owner_epoch.store(registered ? owner.process_epoch : 0,
                                 std::memory_order_relaxed);
        header.allocation_transaction_id.store(
            registered ? owner.lease_id : 0, std::memory_order_relaxed);
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
    const ShmHandle handle{
        .offset = handle_offset,
        .generation =
            generations_[run_start].load(std::memory_order_acquire),
        .region_id = region_id_,
    };
    if (registered) {
        const Status registration =
            PrepareRegistration(handle, request, segments_needed);
        if (!registration.ok()) {
            if (local_specialized_state_ != nullptr) {
                local_specialized_state_->registration_failures.fetch_add(
                    1, std::memory_order_relaxed);
            }
            const Status rollback = ClearObjectForRecovery(
                run_start, StateBits(ObjectState::kAllocated));
            return rollback.ok() ? registration : rollback;
        }
    }
    if (huge_pages_requested_ && !huge_pages_actual_ &&
        local_specialized_state_ != nullptr) {
        local_specialized_state_->huge_page_fallback_allocations.fetch_add(
            1, std::memory_order_relaxed);
    }
    if (local_numa_state_ != nullptr) {
        if (local_numa_state_->placement_fallback ||
            !local_numa_state_->topology.numa_available ||
            local_numa_state_->effective_nodes.empty() ||
            local_numa_state_->system == nullptr) {
            local_numa_state_->fallback_allocations.fetch_add(
                1, std::memory_order_relaxed);
        } else {
            const int cpu = local_numa_state_->system->CurrentCpu();
            const int node = local_numa_state_->topology.NodeForCpu(cpu);
            if (node >= 0 &&
                std::binary_search(local_numa_state_->effective_nodes.begin(),
                                   local_numa_state_->effective_nodes.end(),
                                   static_cast<uint32_t>(node))) {
                local_numa_state_->local_allocations.fetch_add(
                    1, std::memory_order_relaxed);
            } else {
                local_numa_state_->remote_allocations.fetch_add(
                    1, std::memory_order_relaxed);
            }
        }
    }
    if (local_specialized_state_ != nullptr) {
        local_specialized_state_->allocations.fetch_add(
            1, std::memory_order_relaxed);
    }
    return handle;
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
    MINO_RETURN_IF_ERROR(ReleaseRegistration(handle, /*require_unpinned=*/true));
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
    plan.purpose = purpose_;
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
    MINO_ASSIGN_OR_RETURN(ShmHandle recovery_handle,
                          HandleForRecovery(first_segment));
    MINO_RETURN_IF_ERROR(
        ReleaseRegistration(recovery_handle, /*require_unpinned=*/false));
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
