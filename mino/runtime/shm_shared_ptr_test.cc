// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/shm_shared_ptr.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <chrono>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

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
    size_t bytes = 0;

    void operator()(std::byte* memory) const {
#if defined(__unix__) || defined(__APPLE__)
        if (memory != nullptr) {
            (void)::munmap(memory, bytes);
        }
#else
        ::operator delete[](memory, std::align_val_t(64));
#endif
    }
};

using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(size_t bytes) {
#if defined(__unix__) || defined(__APPLE__)
#if defined(MAP_ANONYMOUS)
    constexpr int kAnonymous = MAP_ANONYMOUS;
#else
    constexpr int kAnonymous = MAP_ANON;
#endif
    void* mapped =
        ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
               MAP_SHARED | kAnonymous, -1, 0);
    if (mapped == MAP_FAILED) throw std::bad_alloc();
    return AlignedBytes(static_cast<std::byte*>(mapped),
                        AlignedDeleter{bytes});
#else
    AlignedBytes memory(
        new (std::align_val_t(64)) std::byte[static_cast<size_t>(bytes)],
        AlignedDeleter{bytes});
    std::memset(memory.get(), 0, bytes);
    return memory;
#endif
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

struct PauseAtPinFault {
    ShmPinTable::PinFaultPointForTesting target;
    std::atomic<bool> reached{false};
    std::atomic<uint32_t> arrivals{0};
    std::atomic<bool> resume{false};
};

constexpr size_t kMutatorAreaOffset = 64;
constexpr size_t kMutatorSlotSize = 64;
constexpr size_t kRecordAreaOffset =
    64 + ShmPinTable::kMutatorCapacity * kMutatorSlotSize + 256 * 64 +
    8192 * 4 + 256 * 4 + 256 * 8;
constexpr size_t kRecordSize = 128;
constexpr size_t kRecordReservationOffset = 64;

std::atomic<uint64_t>* RecordReservationAt(std::byte* table_memory,
                                           uint32_t record_index) {
    return reinterpret_cast<std::atomic<uint64_t>*>(
        table_memory + kRecordAreaOffset + record_index * kRecordSize +
        kRecordReservationOffset);
}

uint32_t CountCurrentMutatorSlots(std::byte* table_memory) {
    const ProcessIdentity current = ProcessIdentity::Current();
    uint32_t count = 0;
    for (uint32_t i = 0; i < ShmPinTable::kMutatorCapacity; ++i) {
        std::byte* slot = table_memory + kMutatorAreaOffset +
                          i * kMutatorSlotSize;
        auto* state = reinterpret_cast<std::atomic<uint64_t>*>(slot);
        auto* node = reinterpret_cast<std::atomic<uint64_t>*>(slot + 8);
        auto* process = reinterpret_cast<std::atomic<uint64_t>*>(slot + 16);
        auto* epoch = reinterpret_cast<std::atomic<uint64_t>*>(slot + 24);
        auto* start = reinterpret_cast<std::atomic<uint64_t>*>(slot + 32);
        const uint64_t observed = state->load(std::memory_order_acquire);
        if ((observed & 0x3u) == 2 &&
            node->load(std::memory_order_relaxed) == current.node_id &&
            process->load(std::memory_order_relaxed) == current.process_id &&
            epoch->load(std::memory_order_relaxed) == current.process_epoch &&
            start->load(std::memory_order_relaxed) == current.start_time_ns &&
            state->load(std::memory_order_acquire) == observed) {
            ++count;
        }
    }
    return count;
}

void PauseAtFault(ShmPinTable::PinFaultPointForTesting point,
                  void* context) noexcept {
    auto* pause = static_cast<PauseAtPinFault*>(context);
    if (pause == nullptr || point != pause->target) return;
    pause->arrivals.fetch_add(1, std::memory_order_acq_rel);
    pause->reached.store(true, std::memory_order_release);
    while (!pause->resume.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

#if defined(__unix__) || defined(__APPLE__)
void KillAtPinFault(ShmPinTable::PinFaultPointForTesting point,
                    void* context) noexcept {
    if (context != nullptr &&
        point == *static_cast<ShmPinTable::PinFaultPointForTesting*>(context)) {
        (void)::kill(::getpid(), SIGKILL);
        ::_exit(127);
    }
}
#endif

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
    constexpr uint32_t kMinimumObjectCount =
        ShmPinTable::kMaxPinsPerProcess / ShmPinTable::kMaxPinsPerObject;
    constexpr uint32_t kObjectCount = kMinimumObjectCount * 2;
    std::vector<ShmHandle> handles;
    handles.reserve(kObjectCount);
    for (uint32_t i = 0; i < kObjectCount; ++i) {
        auto handle = Publish(100 + i);
        ASSERT_TRUE(handle.ok()) << handle.status().ToString();
        handles.push_back(*handle);
    }

    std::vector<ShmPinToken> tokens;
    tokens.reserve(ShmPinTable::kMaxPinsPerProcess);
    for (uint32_t round = 0;
         round < ShmPinTable::kMaxPinsPerObject &&
         tokens.size() < ShmPinTable::kMaxPinsPerProcess;
         ++round) {
        for (uint32_t object = 0;
             object < kObjectCount &&
             tokens.size() < ShmPinTable::kMaxPinsPerProcess;
             ++object) {
            auto token = pins_->Pin(handles[object], Contract(), Owner(5));
            if (!token.ok()) {
                ASSERT_EQ(token.status().code(),
                          StatusCode::kResourceExhausted)
                    << "object " << object << " round " << round << ": "
                    << token.status().ToString();
                continue;
            }
            tokens.push_back(std::move(*token));
        }
    }
    ASSERT_EQ(tokens.size(), ShmPinTable::kMaxPinsPerProcess);
    EXPECT_EQ(pins_->OwnerPinCount(Owner(5)),
              ShmPinTable::kMaxPinsPerProcess);

    auto denied = pins_->Pin(handles.front(), Contract(), Owner(5));
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

TEST_F(ShmSharedPtrTest, CleanupFenceRejectsPinThatPassedInitialCheck) {
    auto handle = Publish(4900);
    ASSERT_TRUE(handle.ok());
    auto warmup = pins_->Pin(*handle, Contract(), Owner(290));
    ASSERT_TRUE(warmup.ok());
    ASSERT_TRUE(warmup->Release().ok());

    const ProcessIdentity owner = Owner(291);
    PauseAtPinFault pause{
        .target = ShmPinTable::PinFaultPointForTesting::kRecordClaimed,
    };
    ShmPinTable::SetFaultInjectorForTesting(&PauseAtFault, &pause);
    std::optional<Result<ShmPinToken>> pin_result;
    std::thread pin_thread(
        [&] { pin_result.emplace(pins_->Pin(*handle, Contract(), owner)); });
    for (uint32_t i = 0;
         i < 100000 && !pause.reached.load(std::memory_order_acquire); ++i) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(pause.reached.load(std::memory_order_acquire));
    EXPECT_EQ(pins_->CleanupOwner(owner), 0u);
    pause.resume.store(true, std::memory_order_release);
    pin_thread.join();
    ShmPinTable::SetFaultInjectorForTesting(nullptr);

    ASSERT_TRUE(pin_result.has_value());
    EXPECT_FALSE(pin_result->ok());
    EXPECT_EQ(pins_->OwnerPinCount(owner), 0u);
    EXPECT_EQ(pins_->PinCount(*handle), 0u);
}

TEST_F(ShmSharedPtrTest, CleanupFenceWinsImmediatelyBeforeActiveCas) {
    auto handle = Publish(4901);
    ASSERT_TRUE(handle.ok());
    auto warmup = pins_->Pin(*handle, Contract(), Owner(292));
    ASSERT_TRUE(warmup.ok());
    ASSERT_TRUE(warmup->Release().ok());

    const ProcessIdentity owner = Owner(293);
    PauseAtPinFault pause{
        .target = ShmPinTable::PinFaultPointForTesting::kOwnerQuotaCommitted,
    };
    ShmPinTable::SetFaultInjectorForTesting(&PauseAtFault, &pause);
    std::optional<Result<ShmPinToken>> pin_result;
    std::thread pin_thread(
        [&] { pin_result.emplace(pins_->Pin(*handle, Contract(), owner)); });
    for (uint32_t i = 0;
         i < 100000 && !pause.reached.load(std::memory_order_acquire); ++i) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(pause.reached.load(std::memory_order_acquire));
    EXPECT_EQ(pins_->CleanupOwner(owner), 1u);
    pause.resume.store(true, std::memory_order_release);
    pin_thread.join();
    ShmPinTable::SetFaultInjectorForTesting(nullptr);

    ASSERT_TRUE(pin_result.has_value());
    EXPECT_FALSE(pin_result->ok());
    EXPECT_EQ(pins_->OwnerPinCount(owner), 0u);
    EXPECT_EQ(pins_->PinCount(*handle), 0u);
}

TEST_F(ShmSharedPtrTest, ConcurrentFirstPinsPublishOneMutatorSlot) {
    auto first_handle = Publish(4902);
    auto second_handle = Publish(4903);
    ASSERT_TRUE(first_handle.ok());
    ASSERT_TRUE(second_handle.ok());

    PauseAtPinFault pause{
        .target =
            ShmPinTable::PinFaultPointForTesting::kMutatorBeforeRegistrationLock,
    };
    ShmPinTable::SetFaultInjectorForTesting(&PauseAtFault, &pause);
    std::optional<Result<ShmPinToken>> first;
    std::optional<Result<ShmPinToken>> second;
    std::atomic<bool> second_done{false};
    std::thread first_thread([&] {
        first.emplace(pins_->Pin(*first_handle, Contract(), Owner(294)));
    });
    for (uint32_t i = 0;
         i < 100000 && !pause.reached.load(std::memory_order_acquire); ++i) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(pause.reached.load(std::memory_order_acquire));
    std::thread second_thread([&] {
        second.emplace(pins_->Pin(*second_handle, Contract(), Owner(295)));
        second_done.store(true, std::memory_order_release);
    });
    for (uint32_t i = 0;
         i < 100000 && pause.arrivals.load(std::memory_order_acquire) < 2; ++i) {
        std::this_thread::yield();
    }
    EXPECT_EQ(pause.arrivals.load(std::memory_order_acquire), 2u);
    EXPECT_FALSE(second_done.load(std::memory_order_acquire));
    pause.resume.store(true, std::memory_order_release);
    first_thread.join();
    second_thread.join();
    ShmPinTable::SetFaultInjectorForTesting(nullptr);

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(first->ok()) << first->status().ToString();
    ASSERT_TRUE(second->ok()) << second->status().ToString();
    EXPECT_EQ(CountCurrentMutatorSlots(pin_memory_.get()), 1u);
    EXPECT_TRUE((*first)->Release().ok());
    EXPECT_TRUE((*second)->Release().ok());
}

TEST_F(ShmSharedPtrTest, StaleTokenCannotClearNewReservationGeneration) {
    auto handle = Publish(4904);
    ASSERT_TRUE(handle.ok());
    const ProcessIdentity owner = Owner(296);
    auto stale = pins_->Pin(*handle, Contract(), owner);
    ASSERT_TRUE(stale.ok());
    const uint32_t record_index = stale->record_index_for_testing();
    const uint64_t old_reservation =
        stale->record_reservation_for_testing();
    ASSERT_NE(old_reservation, 0u);
    ASSERT_EQ(pins_->CleanupOwner(owner), 1u);

    std::atomic<uint64_t>* reservation =
        RecordReservationAt(pin_memory_.get(), record_index);
    const uint64_t new_reservation = old_reservation + (1ULL << 20);
    uint64_t expected_free = 0;
    ASSERT_TRUE(reservation->compare_exchange_strong(
        expected_free, new_reservation, std::memory_order_acq_rel,
        std::memory_order_acquire));
    const Status released = stale->Release();
    EXPECT_EQ(released.code(), StatusCode::kNotFound);
    EXPECT_TRUE(stale->active());
    EXPECT_EQ(reservation->load(std::memory_order_acquire), new_reservation);
    uint64_t expected_new = new_reservation;
    ASSERT_TRUE(reservation->compare_exchange_strong(
        expected_new, 0, std::memory_order_acq_rel,
        std::memory_order_acquire));
    EXPECT_TRUE(stale->Release().ok());
}

TEST_F(ShmSharedPtrTest, ConcurrentReleasesWaitForTransientQuotaLock) {
    auto first_handle = Publish(5000);
    auto second_handle = Publish(5001);
    ASSERT_TRUE(first_handle.ok());
    ASSERT_TRUE(second_handle.ok());
    const ProcessIdentity owner = Owner(300);
    auto first = pins_->Pin(*first_handle, Contract(), owner);
    auto second = pins_->Pin(*second_handle, Contract(), owner);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(second.ok());

    PauseAtPinFault pause{
        .target = ShmPinTable::PinFaultPointForTesting::
            kOwnerQuotaReleaseLocked,
    };
    ShmPinTable::SetFaultInjectorForTesting(&PauseAtFault, &pause);
    Status first_status;
    Status second_status;
    std::atomic<bool> second_done{false};
    std::thread first_release([&] { first_status = first->Release(); });
    for (uint32_t i = 0;
         i < 100000 && !pause.reached.load(std::memory_order_acquire); ++i) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(pause.reached.load(std::memory_order_acquire));

    std::thread second_release([&] {
        second_status = second->Release();
        second_done.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(second_done.load(std::memory_order_acquire));
    pause.resume.store(true, std::memory_order_release);
    first_release.join();
    second_release.join();
    ShmPinTable::SetFaultInjectorForTesting(nullptr);

    EXPECT_TRUE(first_status.ok()) << first_status.ToString();
    EXPECT_TRUE(second_status.ok()) << second_status.ToString();
    EXPECT_FALSE(first->active());
    EXPECT_FALSE(second->active());
    EXPECT_EQ(pins_->OwnerPinCount(owner), 0u);
}

TEST_F(ShmSharedPtrTest, CleanupFencesPausedPinPublication) {
    auto handle = Publish(5100);
    ASSERT_TRUE(handle.ok());
    const ProcessIdentity owner = Owner(301);
    (void)ProcessIdentity::Current();

    PauseAtPinFault pause{
        .target = ShmPinTable::PinFaultPointForTesting::kRecordPrepared,
    };
    ShmPinTable::SetFaultInjectorForTesting(&PauseAtFault, &pause);
    std::optional<Result<ShmPinToken>> pin_result;
    std::thread pin_thread(
        [&] { pin_result.emplace(pins_->Pin(*handle, Contract(), owner)); });
    for (uint32_t i = 0;
         i < 100000 && !pause.reached.load(std::memory_order_acquire); ++i) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(pause.reached.load(std::memory_order_acquire));
    EXPECT_EQ(pins_->CleanupOwner(owner), 1u);
    pause.resume.store(true, std::memory_order_release);
    pin_thread.join();
    ShmPinTable::SetFaultInjectorForTesting(nullptr);

    ASSERT_TRUE(pin_result.has_value());
    EXPECT_FALSE(pin_result->ok());
    EXPECT_EQ(pins_->PinCount(*handle), 0u);
    EXPECT_EQ(pins_->OwnerPinCount(owner), 0u);
    auto retry = pins_->Pin(*handle, Contract(), Owner(302));
    ASSERT_TRUE(retry.ok()) << retry.status().ToString();
    EXPECT_TRUE(retry->Release().ok());
}

TEST_F(ShmSharedPtrTest, CleanupFencesPinHoldingObjectQuotaLock) {
    auto handle = Publish(5150);
    ASSERT_TRUE(handle.ok());
    const ProcessIdentity owner = Owner(307);
    (void)ProcessIdentity::Current();

    PauseAtPinFault pause{
        .target = ShmPinTable::PinFaultPointForTesting::kObjectQuotaLocked,
    };
    ShmPinTable::SetFaultInjectorForTesting(&PauseAtFault, &pause);
    std::optional<Result<ShmPinToken>> pin_result;
    std::thread pin_thread(
        [&] { pin_result.emplace(pins_->Pin(*handle, Contract(), owner)); });
    for (uint32_t i = 0;
         i < 100000 && !pause.reached.load(std::memory_order_acquire); ++i) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(pause.reached.load(std::memory_order_acquire));

    uint32_t cleaned = 0;
    std::atomic<bool> cleanup_done{false};
    std::thread cleanup_thread([&] {
        cleaned = pins_->CleanupOwner(owner);
        cleanup_done.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(cleanup_done.load(std::memory_order_acquire));
    auto during_cleanup = pins_->Pin(*handle, Contract(), owner);
    ASSERT_FALSE(during_cleanup.ok());
    EXPECT_EQ(during_cleanup.status().code(), StatusCode::kWouldBlock);
    pause.resume.store(true, std::memory_order_release);
    pin_thread.join();
    cleanup_thread.join();
    ShmPinTable::SetFaultInjectorForTesting(nullptr);

    ASSERT_TRUE(pin_result.has_value());
    EXPECT_FALSE(pin_result->ok());
    EXPECT_EQ(cleaned, 1u);
    EXPECT_EQ(pins_->PinCount(*handle), 0u);
    EXPECT_EQ(pins_->OwnerPinCount(owner), 0u);

    std::vector<ShmPinToken> refill;
    refill.reserve(ShmPinTable::kMaxPinsPerObject);
    for (uint32_t i = 0; i < ShmPinTable::kMaxPinsPerObject; ++i) {
        auto token = pins_->Pin(*handle, Contract(), Owner(308));
        ASSERT_TRUE(token.ok()) << token.status().ToString();
        refill.push_back(std::move(*token));
    }
}

TEST_F(ShmSharedPtrTest, AttachProbesMutatorNotLogicalOwner) {
    auto handle = Publish(5200);
    ASSERT_TRUE(handle.ok());
    const ProcessIdentity logical_owner = Owner(303);
    auto token = pins_->Pin(*handle, Contract(), logical_owner);
    ASSERT_TRUE(token.ok());

    auto attached = ShmPinTable::Attach(pin_memory_.get(),
                                        ShmPinTable::RequiredSize(), allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_EQ(attached->PinCount(*handle), 1u);
    EXPECT_EQ(attached->OwnerPinCount(logical_owner), 1u);
    EXPECT_EQ(attached->CleanupOwner(logical_owner), 1u);
    EXPECT_EQ(attached->PinCount(*handle), 0u);
}

TEST_F(ShmSharedPtrTest, AttachDoesNotCleanUnknownCrossNodeMutator) {
    const ProcessIdentity current = ProcessIdentity::Current();
    if (current.node_id == 0) GTEST_SKIP() << "node identity is unavailable";
    auto handle = Publish(5300);
    ASSERT_TRUE(handle.ok());
    auto token = pins_->Pin(*handle, Contract(), Owner(304));
    ASSERT_TRUE(token.ok());

    std::atomic<uint64_t>* mutator_node = nullptr;
    auto* bytes = pin_memory_.get() + 64;
    for (uint32_t i = 0; i < ShmPinTable::kMutatorCapacity; ++i) {
        auto* state = reinterpret_cast<std::atomic<uint64_t>*>(bytes + i * 64);
        auto* node = reinterpret_cast<std::atomic<uint64_t>*>(bytes + i * 64 + 8);
        auto* process =
            reinterpret_cast<std::atomic<uint64_t>*>(bytes + i * 64 + 16);
        auto* epoch =
            reinterpret_cast<std::atomic<uint64_t>*>(bytes + i * 64 + 24);
        if ((state->load(std::memory_order_acquire) & 0x3u) == 2 &&
            process->load(std::memory_order_relaxed) == current.process_id &&
            epoch->load(std::memory_order_relaxed) == current.process_epoch) {
            mutator_node = node;
            break;
        }
    }
    ASSERT_NE(mutator_node, nullptr);
    mutator_node->store(current.node_id ^ 1u, std::memory_order_release);
    auto attached = ShmPinTable::Attach(pin_memory_.get(),
                                        ShmPinTable::RequiredSize(), allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_EQ(attached->PinCount(*handle), 1u);
    const Status blocked_release = token->Release();
    EXPECT_EQ(blocked_release.code(), StatusCode::kWouldBlock);
    EXPECT_TRUE(token->active());
    mutator_node->store(current.node_id, std::memory_order_release);
    EXPECT_TRUE(token->Release().ok());
    EXPECT_FALSE(token->active());
}

#if defined(__unix__) || defined(__APPLE__)
TEST_F(ShmSharedPtrTest, AttachRecoversCleanerKilledAfterOddFence) {
    auto handle = Publish(5320);
    ASSERT_TRUE(handle.ok());
    const ProcessIdentity owner = Owner(311);
    auto token = pins_->Pin(*handle, Contract(), owner);
    ASSERT_TRUE(token.ok());

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto fault = ShmPinTable::PinFaultPointForTesting::kOwnerCleanupOdd;
        ShmPinTable::SetFaultInjectorForTesting(&KillAtPinFault, &fault);
        (void)pins_->CleanupOwner(owner);
        ::_exit(2);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);

    auto blocked = pins_->Pin(*handle, Contract(), owner);
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kWouldBlock);
    auto attached = ShmPinTable::Attach(pin_memory_.get(),
                                        ShmPinTable::RequiredSize(), allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_EQ(attached->OwnerPinCount(owner), 0u);
    EXPECT_TRUE(token->Release().ok());
    auto retry = attached->Pin(*handle, Contract(), owner);
    ASSERT_TRUE(retry.ok()) << retry.status().ToString();
    EXPECT_TRUE(retry->Release().ok());
}

#if defined(__linux__)
TEST_F(ShmSharedPtrTest, AttachRecoversZombieWritingMutatorBeforeWaitpid) {
    auto handle = Publish(5340);
    ASSERT_TRUE(handle.ok());
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto fault = ShmPinTable::PinFaultPointForTesting::kMutatorSlotWriting;
        ShmPinTable::SetFaultInjectorForTesting(&KillAtPinFault, &fault);
        (void)pins_->Pin(*handle, Contract(), Owner(312));
        ::_exit(2);
    }

    siginfo_t info{};
    for (uint32_t i = 0; i < 100000 && info.si_pid != child; ++i) {
        ASSERT_EQ(::waitid(P_PID, child, &info,
                           WEXITED | WNOHANG | WNOWAIT),
                  0);
        if (info.si_pid != child) std::this_thread::yield();
    }
    ASSERT_EQ(info.si_pid, child);
    ASSERT_EQ(info.si_code, CLD_KILLED);

    auto attached = ShmPinTable::Attach(pin_memory_.get(),
                                        ShmPinTable::RequiredSize(), allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    auto retry = attached->Pin(*handle, Contract(), Owner(313));
    ASSERT_TRUE(retry.ok()) << retry.status().ToString();
    EXPECT_TRUE(retry->Release().ok());

    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    EXPECT_EQ(WTERMSIG(status), SIGKILL);
}
#endif

TEST_F(ShmSharedPtrTest, AttachRecoversCrashedMutatorRegistration) {
    auto handle = Publish(5350);
    ASSERT_TRUE(handle.ok());
    constexpr std::array registration_faults{
        ShmPinTable::PinFaultPointForTesting::kMutatorSlotWriting,
        ShmPinTable::PinFaultPointForTesting::kMutatorIdentityReady,
        ShmPinTable::PinFaultPointForTesting::kMutatorActivePublished,
    };
    for (const auto fault_point : registration_faults) {
        const pid_t child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            auto fault = fault_point;
            ShmPinTable::SetFaultInjectorForTesting(&KillAtPinFault, &fault);
            (void)pins_->Pin(*handle, Contract(), Owner(309));
            ::_exit(2);
        }
        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFSIGNALED(status));
        ASSERT_EQ(WTERMSIG(status), SIGKILL);

        auto attached = ShmPinTable::Attach(
            pin_memory_.get(), ShmPinTable::RequiredSize(), allocator_);
        ASSERT_TRUE(attached.ok()) << attached.status().ToString();
        auto retry = attached->Pin(*handle, Contract(), Owner(310));
        ASSERT_TRUE(retry.ok()) << retry.status().ToString();
        EXPECT_TRUE(retry->Release().ok());
    }
}

TEST_F(ShmSharedPtrTest, AttachRecoversClaimOnlyRecordByMutatorIdentity) {
    auto handle = Publish(5400);
    ASSERT_TRUE(handle.ok());
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto fault = ShmPinTable::PinFaultPointForTesting::kRecordClaimed;
        ShmPinTable::SetFaultInjectorForTesting(&KillAtPinFault, &fault);
        (void)pins_->Pin(*handle, Contract(), Owner(305));
        ::_exit(2);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);

    auto attached = ShmPinTable::Attach(pin_memory_.get(),
                                        ShmPinTable::RequiredSize(), allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    auto retry = attached->Pin(*handle, Contract(), Owner(306));
    ASSERT_TRUE(retry.ok()) << retry.status().ToString();
    EXPECT_TRUE(retry->Release().ok());
}

TEST_F(ShmSharedPtrTest, CrashAtEveryPinAndReleaseStateLeaksNoQuota) {
    const ProcessIdentity owner = Owner(100);
    auto handle = Publish(1000);
    ASSERT_TRUE(handle.ok());

    constexpr std::array acquisition_faults{
        ShmPinTable::PinFaultPointForTesting::kRecordClaimed,
        ShmPinTable::PinFaultPointForTesting::kRecordMetadataReady,
        ShmPinTable::PinFaultPointForTesting::kRecordPrepared,
        ShmPinTable::PinFaultPointForTesting::kObjectQuotaLocked,
        ShmPinTable::PinFaultPointForTesting::kObjectQuotaCommitted,
        ShmPinTable::PinFaultPointForTesting::kObjectInspected,
        ShmPinTable::PinFaultPointForTesting::kOwnerQuotaLocked,
        ShmPinTable::PinFaultPointForTesting::kOwnerQuotaCommitted,
        ShmPinTable::PinFaultPointForTesting::kRecordActive,
    };
    for (const auto fault : acquisition_faults) {
        const pid_t child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            auto armed_fault = fault;
            ShmPinTable::SetFaultInjectorForTesting(&KillAtPinFault,
                                                     &armed_fault);
            (void)pins_->Pin(*handle, Contract(), owner);
            ::_exit(2);
        }
        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFSIGNALED(status));
        EXPECT_EQ(WTERMSIG(status), SIGKILL);
        if (fault ==
                ShmPinTable::PinFaultPointForTesting::kRecordClaimed ||
            fault == ShmPinTable::PinFaultPointForTesting::
                         kRecordMetadataReady) {
            auto attached = ShmPinTable::Attach(
                pin_memory_.get(), ShmPinTable::RequiredSize(), allocator_);
            ASSERT_TRUE(attached.ok()) << attached.status().ToString();
            EXPECT_EQ(pins_->CleanupOwner(owner), 0u);
        } else {
            EXPECT_EQ(pins_->CleanupOwner(owner), 1u);
        }
        EXPECT_EQ(pins_->PinCount(*handle), 0u);
        EXPECT_EQ(pins_->OwnerPinCount(owner), 0u);
    }

    constexpr std::array release_faults{
        ShmPinTable::PinFaultPointForTesting::kRecordReleasing,
        ShmPinTable::PinFaultPointForTesting::kOwnerQuotaReleaseLocked,
        ShmPinTable::PinFaultPointForTesting::kObjectQuotaReleaseLocked,
        ShmPinTable::PinFaultPointForTesting::kRecordStateCleared,
    };
    for (const auto fault : release_faults) {
        const pid_t child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            auto token = pins_->Pin(*handle, Contract(), owner);
            if (!token.ok()) ::_exit(3);
            auto armed_fault = fault;
            ShmPinTable::SetFaultInjectorForTesting(&KillAtPinFault,
                                                     &armed_fault);
            (void)token->Release();
            ::_exit(4);
        }
        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFSIGNALED(status));
        EXPECT_EQ(WTERMSIG(status), SIGKILL);
        EXPECT_EQ(pins_->CleanupOwner(owner), 1u);
        EXPECT_EQ(pins_->PinCount(*handle), 0u);
        EXPECT_EQ(pins_->OwnerPinCount(owner), 0u);
    }

    // Refill the exact object quota. A hidden object charge from any crash
    // point would make one of these 64 Pins fail.
    std::vector<ShmPinToken> object_tokens;
    object_tokens.reserve(ShmPinTable::kMaxPinsPerObject);
    for (uint32_t i = 0; i < ShmPinTable::kMaxPinsPerObject; ++i) {
        auto token = pins_->Pin(*handle, Contract(), owner);
        ASSERT_TRUE(token.ok()) << "object quota refill " << i << ": "
                                << token.status().ToString();
        object_tokens.push_back(std::move(*token));
    }
    object_tokens.clear();

    // Refill the exact owner quota across enough objects to tolerate
    // conservative object-quota hash collisions. This catches owner quota leaks
    // that OwnerPinCount() alone cannot observe.
    constexpr uint32_t kMinimumObjectCount =
        ShmPinTable::kMaxPinsPerProcess / ShmPinTable::kMaxPinsPerObject;
    constexpr uint32_t kObjectCount = kMinimumObjectCount * 2;
    std::vector<ShmHandle> handles;
    handles.reserve(kObjectCount);
    handles.push_back(*handle);
    for (uint32_t i = 1; i < kObjectCount; ++i) {
        auto owner_handle = Publish(2000 + i);
        ASSERT_TRUE(owner_handle.ok()) << owner_handle.status().ToString();
        handles.push_back(*owner_handle);
    }
    std::vector<ShmPinToken> owner_tokens;
    owner_tokens.reserve(ShmPinTable::kMaxPinsPerProcess);
    for (uint32_t round = 0;
         round < ShmPinTable::kMaxPinsPerObject &&
         owner_tokens.size() < ShmPinTable::kMaxPinsPerProcess;
         ++round) {
        for (const ShmHandle owner_handle : handles) {
            if (owner_tokens.size() == ShmPinTable::kMaxPinsPerProcess) break;
            auto token = pins_->Pin(owner_handle, Contract(), owner);
            if (!token.ok()) {
                ASSERT_EQ(token.status().code(),
                          StatusCode::kResourceExhausted)
                    << "owner quota refill " << owner_tokens.size()
                    << " round " << round << ": "
                    << token.status().ToString();
                continue;
            }
            owner_tokens.push_back(std::move(*token));
        }
    }
    ASSERT_EQ(owner_tokens.size(), ShmPinTable::kMaxPinsPerProcess);
    EXPECT_EQ(pins_->OwnerPinCount(owner),
              ShmPinTable::kMaxPinsPerProcess);
    owner_tokens.clear();

    ASSERT_TRUE(pins_->RetirePayload(*handle).ok());
    EXPECT_EQ(allocator_.Inspect(*handle).status().code(),
              StatusCode::kNotFound);
}

TEST_F(ShmSharedPtrTest, DeadOwnerCleanupReclaimsRetiredCrashPin) {
    auto handle = Publish(3000);
    ASSERT_TRUE(handle.ok());
    const ProcessIdentity owner = Owner(101);

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto fault = ShmPinTable::PinFaultPointForTesting::kRecordActive;
        ShmPinTable::SetFaultInjectorForTesting(&KillAtPinFault, &fault);
        (void)pins_->Pin(*handle, Contract(), owner);
        ::_exit(2);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);

    ASSERT_TRUE(pins_->RetirePayload(*handle).ok());
    ASSERT_TRUE(allocator_.Inspect(*handle).ok());
    EXPECT_EQ(pins_->CleanupOwner(owner), 1u);
    EXPECT_EQ(allocator_.Inspect(*handle).status().code(),
              StatusCode::kNotFound);
}

TEST_F(ShmSharedPtrTest, AttachRecoversDeadTransitionalOwner) {
    auto handle = Publish(4000);
    ASSERT_TRUE(handle.ok());

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto fault =
            ShmPinTable::PinFaultPointForTesting::kObjectQuotaLocked;
        ShmPinTable::SetFaultInjectorForTesting(&KillAtPinFault, &fault);
        (void)pins_->Pin(*handle, Contract(), Owner(202));
        ::_exit(2);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);

    ASSERT_TRUE(pins_->RetirePayload(*handle).ok());
    ASSERT_TRUE(allocator_.Inspect(*handle).ok());
    auto attached = ShmPinTable::Attach(pin_memory_.get(),
                                        ShmPinTable::RequiredSize(), allocator_);
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();
    EXPECT_EQ(allocator_.Inspect(*handle).status().code(),
              StatusCode::kNotFound);
}
#endif

TEST_F(ShmSharedPtrTest, AttachRejectsLayoutsWithoutV5RecoveryFencing) {
    static_assert(ShmPinTable::kLayoutVersion == 5);
    auto* layout_version = reinterpret_cast<std::atomic<uint32_t>*>(
        pin_memory_.get() + sizeof(std::atomic<uint64_t>));
    for (const uint32_t old_version : {1u, 2u, 3u, 4u}) {
        layout_version->store(old_version, std::memory_order_release);
        auto attached = ShmPinTable::Attach(
            pin_memory_.get(), ShmPinTable::RequiredSize(), allocator_);
        ASSERT_FALSE(attached.ok());
        EXPECT_EQ(attached.status().code(), StatusCode::kCorruption);
    }
}

}  // namespace
}  // namespace mino
