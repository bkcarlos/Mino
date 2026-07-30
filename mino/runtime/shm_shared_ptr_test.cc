// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/shm_shared_ptr.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace mino {

struct PinTestMessage {
    uint64_t id;
    uint32_t value;
    uint32_t reserved;
};

static_assert(std::is_trivially_copyable_v<PinTestMessage>);
static_assert(std::is_standard_layout_v<PinTestMessage>);

template <>
struct StaticMessageTraits<PinTestMessage> {
    static constexpr bool kIsSpecialized = true;
    static constexpr TypeId type_id{71};
    static constexpr uint32_t message_type = 0x50494E31u;
    static constexpr uint32_t schema_version = 1;
    static constexpr uint64_t schema_short_id = 0x1020304050607080ULL;
    static constexpr uint32_t layout_version = 3;
    static constexpr uint32_t index_flags = 0;

    static Status Validate(const PinTestMessage&) noexcept {
        return Status::Ok();
    }
};

namespace {

static_assert(!std::is_copy_constructible_v<ShmPinToken>);
static_assert(!std::is_copy_assignable_v<ShmPinToken>);
static_assert(std::is_nothrow_move_constructible_v<ShmPinToken>);
static_assert(!std::is_copy_constructible_v<ShmSharedPtr<PinTestMessage>>);
static_assert(!std::is_copy_assignable_v<ShmSharedPtr<PinTestMessage>>);
static_assert(
    std::is_nothrow_move_constructible_v<ShmSharedPtr<PinTestMessage>>);

struct AlignedDeleter {
    void operator()(std::byte* memory) const {
        ::operator delete[](memory, std::align_val_t(64));
    }
};

using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(size_t bytes) {
    AlignedBytes memory(
        new (std::align_val_t(64)) std::byte[static_cast<size_t>(bytes)]);
    std::memset(memory.get(), 0, bytes);
    return memory;
}

ProcessIdentity Owner(uint64_t epoch) {
    return ProcessIdentity{
        .node_id = 11,
        .process_id = 22,
        .process_epoch = epoch,
        .start_time_ns = 33,
    };
}

ShmPinContract Contract() {
    return ShmPinContract{
        .type_id = StaticMessageTraits<PinTestMessage>::type_id,
        .schema_short_id =
            StaticMessageTraits<PinTestMessage>::schema_short_id,
        .layout_version =
            StaticMessageTraits<PinTestMessage>::layout_version,
        .object_size = sizeof(PinTestMessage),
    };
}

class ShmSharedPtrTest : public ::testing::Test {
protected:
    static constexpr size_t kAllocatorBytes = 2u << 20;

    void SetUp() override {
        allocator_memory_ = AllocateAligned(kAllocatorBytes);
        ClassTableConfig config;
        config.classes = {{.slot_size = 64, .slot_count = 128}};
        auto allocator = CentralSlabAllocator::Create(
            allocator_memory_.get(), kAllocatorBytes, config);
        ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
        allocator_ = *allocator;

        pin_memory_ = AllocateAligned(ShmPinTable::RequiredSize());
        auto pins = ShmPinTable::Init(pin_memory_.get(),
                                      ShmPinTable::RequiredSize(), allocator_);
        ASSERT_TRUE(pins.ok()) << pins.status().ToString();
        pins_.emplace(*pins);
    }

    Result<ShmHandle> Publish(uint64_t id) {
        AllocationRequest request{
            .object_size = sizeof(PinTestMessage),
            .type_id = StaticMessageTraits<PinTestMessage>::type_id,
            .schema =
                SchemaIdentity{
                    .short_id =
                        StaticMessageTraits<PinTestMessage>::schema_short_id,
                    .layout_version =
                        StaticMessageTraits<PinTestMessage>::layout_version,
                },
            .alignment = alignof(PinTestMessage),
        };
        Result<ShmHandle> handle = allocator_.Allocate(request);
        if (!handle.ok()) {
            return handle.status();
        }
        Result<MutableBuildView> build = allocator_.BeginBuild(*handle);
        if (!build.ok()) {
            allocator_.Abort(*handle).ok();
            return build.status();
        }
        const PinTestMessage value{.id = id,
                                   .value = static_cast<uint32_t>(id),
                                   .reserved = 0};
        std::memcpy(build->data, &value, sizeof(value));
        const Status published = allocator_.Publish(*handle);
        if (!published.ok()) {
            allocator_.Abort(*handle).ok();
            return published;
        }
        return *handle;
    }

    AlignedBytes allocator_memory_;
    AlignedBytes pin_memory_;
    CentralSlabAllocator allocator_;
    std::optional<ShmPinTable> pins_;
};

TEST_F(ShmSharedPtrTest, PinReadsAndExplicitReleaseDropsRecord) {
    auto handle = Publish(7);
    ASSERT_TRUE(handle.ok());

    auto pointer =
        ShmSharedPtr<PinTestMessage>::Pin(*pins_, *handle, Owner(1));
    ASSERT_TRUE(pointer.ok()) << pointer.status().ToString();
    EXPECT_EQ(pointer->get()->id, 7u);
    EXPECT_EQ(pointer->handle(), *handle);
    EXPECT_EQ(pins_->PinCount(*handle), 1u);
    EXPECT_EQ(pins_->OwnerPinCount(Owner(1)), 1u);

    EXPECT_TRUE(pointer->Release().ok());
    EXPECT_FALSE(pointer->active());
    EXPECT_EQ(pins_->PinCount(*handle), 0u);
    ASSERT_TRUE(allocator_.Inspect(*handle).ok());
}

TEST_F(ShmSharedPtrTest, AttachedFacadeSharesExplicitPinTokenState) {
    auto handle = Publish(16);
    ASSERT_TRUE(handle.ok());
    auto attached = ShmPinTable::Attach(pin_memory_.get(),
                                        ShmPinTable::RequiredSize(), allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();

    auto token = attached->Pin(*handle, Contract(), Owner(10));
    ASSERT_TRUE(token.ok()) << token.status().ToString();
    EXPECT_EQ(pins_->PinCount(*handle), 1u);
    EXPECT_EQ(pins_->OwnerPinCount(Owner(10)), 1u);

    EXPECT_TRUE(token->Release().ok());
    EXPECT_EQ(pins_->PinCount(*handle), 0u);
}

TEST_F(ShmSharedPtrTest, RetiredObjectIsReclaimedAfterLastRelease) {
    auto handle = Publish(8);
    ASSERT_TRUE(handle.ok());
    auto pointer =
        ShmSharedPtr<PinTestMessage>::Pin(*pins_, *handle, Owner(2));
    ASSERT_TRUE(pointer.ok());

    ASSERT_TRUE(allocator_.Retire(*handle).ok());
    auto retired = allocator_.Inspect(*handle);
    ASSERT_TRUE(retired.ok());
    EXPECT_EQ(retired->state, ObjectState::kRetired);

    ASSERT_TRUE(pointer->Release().ok());
    auto reclaimed = allocator_.Inspect(*handle);
    ASSERT_FALSE(reclaimed.ok());
    EXPECT_EQ(reclaimed.status().code(), StatusCode::kNotFound);
}

TEST_F(ShmSharedPtrTest, CloneRemainsAvailableAfterRetire) {
    auto handle = Publish(9);
    ASSERT_TRUE(handle.ok());
    auto first = ShmSharedPtr<PinTestMessage>::Pin(*pins_, *handle, Owner(3));
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(allocator_.Retire(*handle).ok());

    auto clone = first->Clone();
    ASSERT_TRUE(clone.ok()) << clone.status().ToString();
    EXPECT_EQ(clone->get()->id, 9u);
    EXPECT_EQ(pins_->PinCount(*handle), 2u);

    ASSERT_TRUE(first->Release().ok());
    EXPECT_TRUE(allocator_.Inspect(*handle).ok());
    ASSERT_TRUE(clone->Release().ok());
    EXPECT_EQ(allocator_.Inspect(*handle).status().code(),
              StatusCode::kNotFound);
}

TEST_F(ShmSharedPtrTest, PerObjectQuotaRejectsSixtyFifthPin) {
    auto handle = Publish(10);
    ASSERT_TRUE(handle.ok());
    auto first = ShmSharedPtr<PinTestMessage>::Pin(*pins_, *handle, Owner(4));
    ASSERT_TRUE(first.ok());

    std::vector<ShmSharedPtr<PinTestMessage>> pointers;
    pointers.reserve(ShmPinTable::kMaxPinsPerObject);
    pointers.push_back(std::move(*first));
    for (uint32_t i = 1; i < ShmPinTable::kMaxPinsPerObject; ++i) {
        auto clone = pointers.front().Clone();
        ASSERT_TRUE(clone.ok()) << "clone " << i << ": "
                                << clone.status().ToString();
        pointers.push_back(std::move(*clone));
    }

    auto denied = pointers.front().Clone();
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(pins_->PinCount(*handle),
              ShmPinTable::kMaxPinsPerObject);
}

TEST_F(ShmSharedPtrTest, PerProcessQuotaRejectsPinBeyond4096) {
    constexpr uint32_t kObjectCount =
        ShmPinTable::kMaxPinsPerProcess / ShmPinTable::kMaxPinsPerObject;
    std::vector<ShmHandle> handles;
    handles.reserve(kObjectCount + 1);
    for (uint32_t i = 0; i <= kObjectCount; ++i) {
        auto handle = Publish(100 + i);
        ASSERT_TRUE(handle.ok()) << handle.status().ToString();
        handles.push_back(*handle);
    }

    std::vector<ShmPinToken> tokens;
    tokens.reserve(ShmPinTable::kMaxPinsPerProcess);
    for (uint32_t object = 0; object < kObjectCount; ++object) {
        for (uint32_t pin = 0; pin < ShmPinTable::kMaxPinsPerObject; ++pin) {
            auto token = pins_->Pin(handles[object], Contract(), Owner(5));
            ASSERT_TRUE(token.ok())
                << "object " << object << " pin " << pin << ": "
                << token.status().ToString();
            tokens.push_back(std::move(*token));
        }
    }
    EXPECT_EQ(pins_->OwnerPinCount(Owner(5)),
              ShmPinTable::kMaxPinsPerProcess);

    auto denied = pins_->Pin(handles.back(), Contract(), Owner(5));
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kResourceExhausted);
}

TEST_F(ShmSharedPtrTest, RecycledGenerationRejectsStaleHandle) {
    auto stale = Publish(11);
    ASSERT_TRUE(stale.ok());
    ASSERT_TRUE(allocator_.Retire(*stale).ok());
    ASSERT_TRUE(allocator_.Reclaim(*stale).ok());

    auto fresh = Publish(12);
    ASSERT_TRUE(fresh.ok());
    EXPECT_EQ(fresh->offset, stale->offset);
    EXPECT_GT(fresh->generation, stale->generation);

    auto denied = pins_->Pin(*stale, Contract(), Owner(6));
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kNotFound);
}

TEST_F(ShmSharedPtrTest, OwnerCleanupUsesFullIdentityAndReclaimsRetired) {
    auto retired_handle = Publish(13);
    auto live_handle = Publish(14);
    ASSERT_TRUE(retired_handle.ok());
    ASSERT_TRUE(live_handle.ok());

    auto old_incarnation = ShmSharedPtr<PinTestMessage>::Pin(
        *pins_, *retired_handle, Owner(7));
    auto new_incarnation =
        ShmSharedPtr<PinTestMessage>::Pin(*pins_, *live_handle, Owner(8));
    ASSERT_TRUE(old_incarnation.ok());
    ASSERT_TRUE(new_incarnation.ok());
    ASSERT_TRUE(allocator_.Retire(*retired_handle).ok());

    ShmPinTable::CleanupOwnerCallback(Owner(7), &*pins_);
    EXPECT_EQ(pins_->OwnerPinCount(Owner(7)), 0u);
    EXPECT_EQ(pins_->OwnerPinCount(Owner(8)), 1u);
    EXPECT_EQ(allocator_.Inspect(*retired_handle).status().code(),
              StatusCode::kNotFound);
    EXPECT_TRUE(allocator_.Inspect(*live_handle).ok());

    EXPECT_TRUE(new_incarnation->Release().ok());
}

TEST_F(ShmSharedPtrTest, PinValidatesTypeSchemaLayoutAndSize) {
    auto handle = Publish(15);
    ASSERT_TRUE(handle.ok());

    ShmPinContract wrong = Contract();
    wrong.layout_version += 1;
    auto denied = pins_->Pin(*handle, wrong, Owner(9));
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_EQ(pins_->PinCount(*handle), 0u);
}

}  // namespace
}  // namespace mino
