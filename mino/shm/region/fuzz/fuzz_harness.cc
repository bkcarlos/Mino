// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/fuzz/fuzz_harness.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <unistd.h>

#include "mino/abi/shm_handle.h"
#include "mino/common/ids.h"
#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/region/handle_resolver.h"
#include "mino/shm/region/region.h"

namespace mino::shm::region::fuzz {
namespace {

constexpr TypeId kValidType{17};
constexpr uint32_t kValidGeneration = 9;
constexpr uint64_t kValidSchema = 0x1234;
constexpr uint32_t kValidCapacity = 128;

Status Internal(std::string_view message) {
    return Status::Error(StatusCode::kInternal, message);
}

class Reader {
public:
    explicit Reader(std::span<const std::byte> input) : input_(input) {}

    uint8_t U8() noexcept {
        if (offset_ == input_.size()) return 0;
        return static_cast<uint8_t>(input_[offset_++]);
    }

    uint16_t U16() noexcept {
        uint16_t value = 0;
        for (size_t index = 0; index < 2; ++index) {
            value |= static_cast<uint16_t>(U8()) << (8u * index);
        }
        return value;
    }

    uint32_t U32() noexcept {
        uint32_t value = 0;
        for (size_t index = 0; index < 4; ++index) {
            value |= static_cast<uint32_t>(U8()) << (8u * index);
        }
        return value;
    }

    uint64_t U64() noexcept {
        uint64_t value = 0;
        for (size_t index = 0; index < 8; ++index) {
            value |= static_cast<uint64_t>(U8()) << (8u * index);
        }
        return value;
    }

private:
    std::span<const std::byte> input_;
    size_t offset_ = 0;
};

class FuzzMetadataProvider final : public AllocatorMetadataProvider {
public:
    Result<AllocatorSlotMetadata> GetSlotMetadata(
        uint64_t offset) const override {
        if (offset != slot_offset) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "not the fuzz fixture slot");
        }
        return metadata;
    }

    uint64_t slot_offset = 0;
    AllocatorSlotMetadata metadata;
};

class ResolverFixture {
public:
    ResolverFixture() {
        name_ = "/mino_handle_fuzz_" + std::to_string(::getpid());
        (void)SharedMemorySegment::Unlink(name_);
        RegionCreateOptions options;
        options.name = name_;
        options.size_bytes = 1024u * 1024u;
        auto created = SharedMemoryRegion::Create(options);
        if (!created.ok()) {
            setup_status_ = created.status();
            return;
        }
        region_.emplace(std::move(*created));
        provider_.slot_offset = region_->superblock()->data_offset;
        header_ = new (region_->base() + provider_.slot_offset) SlabHeader{};
        resolver_ = std::make_unique<HandleResolver>(*region_, provider_);
        setup_status_ = Status::Ok();
    }

    ~ResolverFixture() {
        resolver_.reset();
        region_.reset();
        (void)SharedMemorySegment::Unlink(name_);
    }

    const Status& setup_status() const noexcept { return setup_status_; }
    SharedMemoryRegion& region() noexcept { return *region_; }
    const SharedMemoryRegion& region() const noexcept { return *region_; }
    FuzzMetadataProvider& provider() noexcept { return provider_; }
    SlabHeader& header() noexcept { return *header_; }
    HandleResolver& resolver() noexcept { return *resolver_; }

private:
    std::string name_;
    Status setup_status_ = Status::Error(StatusCode::kUnavailable);
    std::optional<SharedMemoryRegion> region_;
    FuzzMetadataProvider provider_;
    SlabHeader* header_ = nullptr;
    std::unique_ptr<HandleResolver> resolver_;
};

ResolverFixture& Fixture() {
    static ResolverFixture fixture;
    return fixture;
}

uint64_t OffsetCandidate(uint8_t selector, uint64_t raw,
                         const ResolverFixture& fixture) {
    const SuperBlock& superblock = *fixture.region().superblock();
    const uint64_t data_begin = superblock.data_offset;
    const uint64_t data_end = data_begin + superblock.data_size;
    switch (selector % 8u) {
        case 0: return 0;
        case 1: return data_begin - 1u;
        case 2: return data_begin;
        case 3: return data_begin + 1u;
        case 4: return data_end - sizeof(SlabHeader);
        case 5: return data_end;
        case 6: return std::numeric_limits<uint64_t>::max();
        default: return raw;
    }
}

void ResetValidFixture(ResolverFixture* fixture) {
    FuzzMetadataProvider& provider = fixture->provider();
    provider.metadata = AllocatorSlotMetadata{
        .occupied = true,
        .generation = kValidGeneration,
        .class_id = 0,
        .class_count = 1,
        .capacity = kValidCapacity,
        .payload_offset = provider.slot_offset + sizeof(SlabHeader),
        .object_extent = kValidCapacity,
        .object_kind = AllocatorObjectKind::kContiguousSlot,
    };

    SlabHeader& header = fixture->header();
    header.magic = kSlabHeaderMagic;
    header.header_version = kSlabHeaderVersion;
    header.class_id = 0;
    header.generation.store(kValidGeneration, std::memory_order_relaxed);
    header.object_state.store(static_cast<uint32_t>(ObjectState::kPublished),
                              std::memory_order_relaxed);
    header.capacity = kValidCapacity;
    header.object_size = sizeof(uint64_t);
    header.type_id = kValidType.value;
    header.layout_version = 1;
    header.schema_short_id = kValidSchema;
    header.owner_epoch.store(ProcessIdentity::Current().process_epoch,
                             std::memory_order_relaxed);
    header.allocation_transaction_id.store(0, std::memory_order_relaxed);
    header.allocation_role.store(0, std::memory_order_relaxed);
    header.immutable_header_crc = ComputeImmutableHeaderCrc(header);
}

class SuperblockRestore {
public:
    SuperblockRestore(SuperBlock* superblock, uint64_t uuid_lo,
                      uint64_t epoch) noexcept
        : superblock_(superblock), uuid_lo_(uuid_lo), epoch_(epoch) {}

    ~SuperblockRestore() { Restore(); }

    void Restore() noexcept {
        if (superblock_ == nullptr) return;
        superblock_->region_uuid_lo = uuid_lo_;
        StoreRegionEpoch(*superblock_, epoch_);
        superblock_ = nullptr;
    }

private:
    SuperBlock* superblock_;
    uint64_t uuid_lo_;
    uint64_t epoch_;
};

bool PointerInRegion(const ResolverFixture& fixture, const void* pointer,
                     size_t size) {
    const uintptr_t begin =
        reinterpret_cast<uintptr_t>(fixture.region().base());
    const uintptr_t value = reinterpret_cast<uintptr_t>(pointer);
    const uint64_t region_size = fixture.region().size();
    return value >= begin && value - begin <= region_size &&
           size <= region_size - (value - begin);
}

}  // namespace

Status FuzzHandleResolver(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxHandleFuzzInputBytes) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        ResolverFixture& fixture = Fixture();
        if (!fixture.setup_status().ok()) {
            return Internal("handle fuzz Region fixture could not be created");
        }
        ResetValidFixture(&fixture);

        Reader reader(input);
        const uint8_t mode = reader.U8() % 8u;
        ShmHandle handle{
            .offset = fixture.provider().slot_offset,
            .generation = kValidGeneration,
            .region_id = fixture.region().region_id(),
        };
        TypeId expected_type = kValidType;
        uint64_t expected_schema = kValidSchema;
        bool mutable_resolve = false;
        bool byte_type = false;
        SuperBlock& superblock = *fixture.region().superblock();
        const uint64_t saved_uuid_lo = superblock.region_uuid_lo;
        const uint64_t saved_epoch = LoadRegionEpoch(superblock);
        SuperblockRestore restore(&superblock, saved_uuid_lo, saved_epoch);

        if (mode == 1) {
            handle.offset = OffsetCandidate(reader.U8(), reader.U64(), fixture);
        } else if (mode == 2) {
            handle.generation = reader.U32();
            handle.region_id = reader.U32();
        } else if (mode == 3) {
            auto& metadata = fixture.provider().metadata;
            metadata.occupied = (reader.U8() & 1u) != 0;
            metadata.generation = reader.U32();
            metadata.class_id = reader.U16();
            metadata.class_count = reader.U16();
            metadata.capacity = reader.U32();
            metadata.object_kind =
                static_cast<AllocatorObjectKind>(reader.U8() % 4u);
        } else if (mode == 4) {
            SlabHeader& header = fixture.header();
            header.magic = reader.U32();
            header.header_version = reader.U16();
            header.class_id = reader.U16();
            header.generation.store(reader.U32(), std::memory_order_relaxed);
            if ((reader.U8() & 1u) == 0) {
                header.immutable_header_crc = ComputeImmutableHeaderCrc(header);
            } else {
                header.immutable_header_crc ^= reader.U32() | 1u;
            }
        } else if (mode == 5) {
            auto& metadata = fixture.provider().metadata;
            metadata.payload_offset = reader.U64();
            metadata.object_extent = reader.U64();
            metadata.capacity = reader.U32();
            fixture.header().capacity = reader.U32();
            fixture.header().object_size = reader.U32();
            fixture.header().immutable_header_crc =
                ComputeImmutableHeaderCrc(fixture.header());
        } else if (mode == 6) {
            mutable_resolve = (reader.U8() & 1u) != 0;
            byte_type = (reader.U8() & 1u) != 0;
            expected_type = TypeId{reader.U32()};
            expected_schema = reader.U64();
            fixture.header().object_state.store(reader.U32(),
                                                 std::memory_order_relaxed);
            fixture.header().owner_epoch.store(reader.U64(),
                                               std::memory_order_relaxed);
            fixture.header().immutable_header_crc =
                ComputeImmutableHeaderCrc(fixture.header());
        } else if (mode == 7) {
            if ((reader.U8() & 1u) != 0) superblock.region_uuid_lo ^= 1u;
            if ((reader.U8() & 1u) != 0) StoreRegionEpoch(superblock, saved_epoch + 1u);
        }

        Status result = Status::Ok();
        const void* pointer = nullptr;
        if (mutable_resolve) {
            if (byte_type) {
                auto resolved = fixture.resolver().ResolveMutable<std::byte>(
                    handle, expected_type, expected_schema);
                if (resolved.ok()) pointer = *resolved;
                else result = resolved.status();
            } else {
                auto resolved = fixture.resolver().ResolveMutable<uint64_t>(
                    handle, expected_type, expected_schema);
                if (resolved.ok()) pointer = *resolved;
                else result = resolved.status();
            }
        } else if (byte_type) {
            auto resolved = fixture.resolver().Resolve<std::byte>(
                handle, expected_type, expected_schema);
            if (resolved.ok()) pointer = *resolved;
            else result = resolved.status();
        } else {
            auto resolved = fixture.resolver().Resolve<uint64_t>(
                handle, expected_type, expected_schema);
            if (resolved.ok()) pointer = *resolved;
            else result = resolved.status();
        }

        if (mode == 0) {
            const void* expected = fixture.region().base() +
                                   fixture.provider().metadata.payload_offset;
            if (!result.ok() || pointer == nullptr || pointer != expected) {
                return Internal("valid handle fixture did not resolve exactly");
            }
        }
        restore.Restore();
        if (pointer != nullptr &&
            !PointerInRegion(fixture, pointer,
                             byte_type ? sizeof(std::byte) : sizeof(uint64_t))) {
            return Internal("resolver returned an out-of-Region pointer");
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Internal("handle resolver harness threw an exception");
    }
}

Status FuzzOneInput(std::span<const std::byte> input) noexcept {
    return FuzzHandleResolver(input);
}

}  // namespace mino::shm::region::fuzz
