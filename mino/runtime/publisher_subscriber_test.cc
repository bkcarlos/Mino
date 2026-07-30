// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/publisher.h"
#include "mino/runtime/subscriber.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <set>
#include <thread>
#include <vector>

namespace mino {

struct RuntimeTestMessage {
    uint64_t id;
    uint32_t value;
    uint32_t reserved;
};

static_assert(std::is_trivially_copyable_v<RuntimeTestMessage>);
static_assert(std::is_standard_layout_v<RuntimeTestMessage>);

template <>
struct StaticMessageTraits<RuntimeTestMessage> {
    static constexpr bool kIsSpecialized = true;
    static constexpr TypeId type_id{42};
    static constexpr uint32_t message_type = 0x10203040u;
    static constexpr uint32_t schema_version = (1u << 16);
    static constexpr uint64_t schema_short_id = 0xAABBCCDDEEFF0011ULL;
    static constexpr uint32_t layout_version = 1;
    static constexpr uint32_t index_flags = 0;

    static Status Validate(const RuntimeTestMessage& message) noexcept {
        if (message.id == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "message id must be non-zero");
        }
        if (message.reserved != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "reserved field must be zero");
        }
        return Status::Ok();
    }
};

namespace {

struct AlignedDeleter {
    void operator()(std::byte* p) const {
        ::operator delete[](p, std::align_val_t(64));
    }
};

using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(uint64_t bytes) {
    AlignedBytes memory(
        new (std::align_val_t(64)) std::byte[static_cast<size_t>(bytes)]);
    std::memset(memory.get(), 0, static_cast<size_t>(bytes));
    return memory;
}

ClassTableConfig AllocatorConfig() {
    ClassTableConfig config;
    config.classes = {{.slot_size = 64, .slot_count = 16}};
    return config;
}

class RuntimeSpscTest : public ::testing::Test {
protected:
    static constexpr uint64_t kAllocatorBytes = 1u << 20;
    static constexpr uint64_t kChannelCapacity = 4;

    AlignedBytes allocator_memory_;
    AlignedBytes channel_memory_;
    CentralSlabAllocator allocator_;
    std::optional<SpscChannel> channel_;

    void SetUp() override {
        allocator_memory_ = AllocateAligned(kAllocatorBytes);
        auto allocator = CentralSlabAllocator::Create(
            allocator_memory_.get(), kAllocatorBytes, AllocatorConfig());
        ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
        allocator_ = *allocator;

        channel_memory_ =
            AllocateAligned(SpscChannel::RequiredSize(kChannelCapacity));
        auto channel = SpscChannel::Init(channel_memory_.get(), kChannelCapacity);
        ASSERT_TRUE(channel.ok()) << channel.status().ToString();
        channel_.emplace(*channel);
    }

    Result<MessageBuilder<RuntimeTestMessage>> Build(uint64_t id,
                                                     uint32_t value) {
        Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
        auto builder = publisher.Allocate();
        if (!builder.ok()) {
            return builder.status();
        }
        (*builder)->id = id;
        (*builder)->value = value;
        return std::move(*builder);
    }
};

TEST_F(RuntimeSpscTest, PublishPollAndExplicitAckAreEndToEnd) {
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok()) << builder.status().ToString();
    (*builder)->id = 7;
    (*builder)->value = 99;
    const ShmHandle handle = builder->handle();

    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());
    EXPECT_EQ(publisher.published_count(), 1u);

    auto published = allocator_.Inspect(handle);
    ASSERT_TRUE(published.ok());
    EXPECT_EQ(published->state, ObjectState::kPublished);

    auto message = subscriber.TryPoll();
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    EXPECT_EQ((*message)->id, 7u);
    EXPECT_EQ((*message)->value, 99u);
    EXPECT_EQ(message->metadata().message_type,
              StaticMessageTraits<RuntimeTestMessage>::message_type);
    EXPECT_EQ(message->metadata().schema_short_id,
              StaticMessageTraits<RuntimeTestMessage>::schema_short_id);
    EXPECT_EQ(message->metadata().payload, handle);

    ASSERT_TRUE(std::move(*message).Ack().ok());
    EXPECT_TRUE(channel_->IsEmpty());
    auto reclaimed = allocator_.Inspect(handle);
    ASSERT_FALSE(reclaimed.ok());
    EXPECT_EQ(reclaimed.status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, BorrowDestructorAcksAndReclaims) {
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 1;
    const ShmHandle handle = builder->handle();
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

    {
        auto message = subscriber.TryPoll();
        ASSERT_TRUE(message.ok());
        EXPECT_EQ((*message)->id, 1u);
    }

    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ(allocator_.Inspect(handle).status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, ValidationFailureAbortsBuilderAndPublishesNothing) {
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 0;
    const ShmHandle handle = builder->handle();

    const Status status = publisher.PublishLocal(std::move(*builder));
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ(allocator_.Inspect(handle).status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, DropNewestIsNormalizedToSuccessAndCounted) {
    PublisherOptions options;
    options.queue_full_policy = QueueFullPolicy::kDropNewest;
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_, options);

    for (uint64_t i = 1; i <= kChannelCapacity; ++i) {
        auto builder = publisher.Allocate();
        ASSERT_TRUE(builder.ok());
        (*builder)->id = i;
        ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());
    }
    EXPECT_TRUE(channel_->IsFull());

    auto dropped = publisher.Allocate();
    ASSERT_TRUE(dropped.ok());
    (*dropped)->id = 99;
    const ShmHandle dropped_handle = dropped->handle();
    EXPECT_TRUE(publisher.PublishLocal(std::move(*dropped)).ok());
    EXPECT_EQ(publisher.dropped_count(), 1u);
    EXPECT_EQ(publisher.published_count(), kChannelCapacity);
    EXPECT_EQ(allocator_.Inspect(dropped_handle).status().code(),
              StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, BlockingPublishHonorsDeadlineAndAbortsPayload) {
    Publisher<RuntimeTestMessage> fill(allocator_, *channel_);
    for (uint64_t i = 1; i <= kChannelCapacity; ++i) {
        auto builder = fill.Allocate();
        ASSERT_TRUE(builder.ok());
        (*builder)->id = i;
        ASSERT_TRUE(fill.PublishLocal(std::move(*builder)).ok());
    }

    PublisherOptions options;
    options.queue_full_policy = QueueFullPolicy::kBlock;
    Publisher<RuntimeTestMessage> blocking(allocator_, *channel_, options);
    auto builder = blocking.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 100;
    const ShmHandle handle = builder->handle();

    const Status status = blocking.PublishLocal(
        std::move(*builder), Deadline::FromNow(std::chrono::milliseconds(2)));
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kTimeout);
    EXPECT_EQ(allocator_.Inspect(handle).status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, PublishCreatesReceiptAfterLocalCommit) {
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);
    OutstandingReceiptTable receipts;
    const PublisherReceiptIdentity identity{
        .process = ProcessIdentity::Current(),
        .publisher_id = PublisherId{1},
    };
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 9},
    };
    const DeliveryRequirement requirement{
        .stage = DeliveryStage::kRemoteAccepted,
        .completion = CompletionPolicy::kAll,
        .quorum = 0,
        .deadline = Deadline::Infinite(),
    };

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 66;
    auto receipt = publisher.Publish(std::move(*builder), receipts, identity,
                                     targets, requirement);
    ASSERT_TRUE(receipt.ok()) << receipt.status().ToString();
    EXPECT_EQ(receipts.outstanding(), 1u);

    auto message = subscriber.TryPoll();
    ASSERT_TRUE(message.ok());
    EXPECT_EQ((*message)->id, 66u);
    ASSERT_TRUE(std::move(*message).Ack().ok());

    ASSERT_TRUE(receipts.Acknowledge(
        receipt->id(), targets[0], DeliveryStage::kRemoteAccepted).ok());
    EXPECT_TRUE(receipt->Wait(Deadline::Infinite()).ok());
}

TEST_F(RuntimeSpscTest, ReceiptExhaustionDoesNotRollbackLocalCommit) {
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);
    OutstandingReceiptTable receipts(
        {.max_outstanding = 0, .max_per_publisher = 0});
    const PublisherReceiptIdentity identity{
        .process = ProcessIdentity::Current(),
        .publisher_id = PublisherId{2},
    };
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 10},
    };
    const DeliveryRequirement requirement{
        .stage = DeliveryStage::kRemoteAccepted,
        .completion = CompletionPolicy::kAll,
        .quorum = 0,
        .deadline = Deadline::Infinite(),
    };

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 88;
    auto receipt = publisher.Publish(std::move(*builder), receipts, identity,
                                     targets, requirement);
    ASSERT_FALSE(receipt.ok());
    EXPECT_EQ(receipt.status().code(), StatusCode::kResourceExhausted);

    auto message = subscriber.TryPoll();
    ASSERT_TRUE(message.ok()) << "local Commit must remain visible";
    EXPECT_EQ((*message)->id, 88u);
    EXPECT_TRUE(std::move(*message).Ack().ok());
}

TEST_F(RuntimeSpscTest, TransferPinsPayloadBeyondChannelAck) {
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  allocator_);
    ASSERT_TRUE(pins.ok()) << pins.status().ToString();

    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    const ProcessIdentity owner = ProcessIdentity::Current();
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_, &*pins,
                                              owner);

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 77;
    const ShmHandle handle = builder->handle();
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

    auto borrowed = subscriber.TryPoll();
    ASSERT_TRUE(borrowed.ok());
    auto pinned = std::move(*borrowed).Transfer();
    ASSERT_TRUE(pinned.ok()) << pinned.status().ToString();
    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ((*pinned)->id, 77u);
    EXPECT_EQ(pins->PinCount(handle), 1u);

    auto retired = allocator_.Inspect(handle);
    ASSERT_TRUE(retired.ok());
    EXPECT_EQ(retired->state, ObjectState::kRetired);

    ASSERT_TRUE(pinned->Release().ok());
    EXPECT_EQ(allocator_.Inspect(handle).status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, BroadcastReclaimsOnlyAfterFinalAck) {
    constexpr uint64_t kCapacity = 4;
    auto broadcast_memory =
        AllocateAligned(BroadcastChannel::RequiredSize(kCapacity));
    auto broadcast = BroadcastChannel::Init(broadcast_memory.get(), kCapacity);
    ASSERT_TRUE(broadcast.ok()) << broadcast.status().ToString();
    auto first_handle = broadcast->RegisterSubscriber(SubscriberId{0});
    auto second_handle = broadcast->RegisterSubscriber(SubscriberId{1});
    ASSERT_TRUE(first_handle.ok() && second_handle.ok());

    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  allocator_);
    ASSERT_TRUE(pins.ok());

    Publisher<RuntimeTestMessage> publisher(allocator_, *broadcast, *pins);
    Subscriber<RuntimeTestMessage> first(allocator_, *broadcast, *first_handle,
                                         *pins);
    Subscriber<RuntimeTestMessage> second(allocator_, *broadcast,
                                          *second_handle, *pins);

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 123;
    const ShmHandle payload = builder->handle();
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

    auto first_message = first.TryPoll();
    auto second_message = second.TryPoll();
    ASSERT_TRUE(first_message.ok() && second_message.ok());
    ASSERT_TRUE(std::move(*first_message).Ack().ok());
    auto still_published = allocator_.Inspect(payload);
    ASSERT_TRUE(still_published.ok());
    EXPECT_EQ(still_published->state, ObjectState::kPublished);

    ASSERT_TRUE(std::move(*second_message).Ack().ok());
    EXPECT_EQ(allocator_.Inspect(payload).status().code(),
              StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, BroadcastTransferDelaysFinalReclaim) {
    constexpr uint64_t kCapacity = 4;
    auto broadcast_memory =
        AllocateAligned(BroadcastChannel::RequiredSize(kCapacity));
    auto broadcast = BroadcastChannel::Init(broadcast_memory.get(), kCapacity);
    ASSERT_TRUE(broadcast.ok());
    auto first_handle = broadcast->RegisterSubscriber(SubscriberId{0});
    auto second_handle = broadcast->RegisterSubscriber(SubscriberId{1});
    ASSERT_TRUE(first_handle.ok() && second_handle.ok());

    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  allocator_);
    ASSERT_TRUE(pins.ok());

    Publisher<RuntimeTestMessage> publisher(allocator_, *broadcast, *pins);
    Subscriber<RuntimeTestMessage> first(allocator_, *broadcast, *first_handle,
                                         *pins);
    Subscriber<RuntimeTestMessage> second(allocator_, *broadcast,
                                          *second_handle, *pins);
    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 321;
    const ShmHandle payload = builder->handle();
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

    auto first_message = first.TryPoll();
    auto second_message = second.TryPoll();
    ASSERT_TRUE(first_message.ok() && second_message.ok());
    auto pinned = std::move(*first_message).Transfer();
    ASSERT_TRUE(pinned.ok()) << pinned.status().ToString();
    ASSERT_TRUE(std::move(*second_message).Ack().ok());

    auto retired = allocator_.Inspect(payload);
    ASSERT_TRUE(retired.ok());
    EXPECT_EQ(retired->state, ObjectState::kRetired);
    EXPECT_EQ((*pinned)->id, 321u);
    ASSERT_TRUE(pinned->Release().ok());
    EXPECT_EQ(allocator_.Inspect(payload).status().code(),
              StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, MpscRuntimeSupportsConcurrentPublishers) {
    constexpr uint64_t kCapacity = 64;
    auto mpsc_memory = AllocateAligned(MpscChannel::RequiredSize(kCapacity));
    auto mpsc = MpscChannel::Init(mpsc_memory.get(), kCapacity);
    ASSERT_TRUE(mpsc.ok()) << mpsc.status().ToString();

    constexpr uint64_t kPublishers = 8;
    std::vector<std::thread> threads;
    threads.reserve(kPublishers);
    std::atomic<uint64_t> failures{0};
    for (uint64_t id = 1; id <= kPublishers; ++id) {
        threads.emplace_back([&, id]() {
            MpscChannel::ProducerIdentity identity{
                .owner = ProcessIdentity::Current(),
                .publisher_id = id,
            };
            Publisher<RuntimeTestMessage> publisher(allocator_, *mpsc, identity);
            auto builder = publisher.Allocate();
            if (!builder.ok()) {
                failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            (*builder)->id = id;
            (*builder)->value = static_cast<uint32_t>(id * 10);
            if (!publisher.PublishLocal(std::move(*builder)).ok()) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(failures.load(std::memory_order_relaxed), 0u);

    Subscriber<RuntimeTestMessage> subscriber(allocator_, *mpsc);
    std::set<uint64_t> observed;
    for (uint64_t i = 0; i < kPublishers; ++i) {
        auto message = subscriber.Poll(
            Deadline::FromNow(std::chrono::milliseconds(50)));
        ASSERT_TRUE(message.ok()) << message.status().ToString();
        observed.insert((*message)->id);
        ASSERT_TRUE(std::move(*message).Ack().ok());
    }
    EXPECT_EQ(observed.size(), kPublishers);
    EXPECT_TRUE(mpsc->IsEmpty());
}

TEST_F(RuntimeSpscTest, PollDeadlineAndCallbackAck) {
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);

    auto empty = subscriber.Poll(
        Deadline::FromNow(std::chrono::milliseconds(1)));
    ASSERT_FALSE(empty.ok());
    EXPECT_EQ(empty.status().code(), StatusCode::kTimeout);

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 55;
    const ShmHandle handle = builder->handle();
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

    uint64_t observed = 0;
    const Status callback = subscriber.Poll(
        [&observed](const BorrowedMessage<RuntimeTestMessage>& message) {
            observed = message->id;
            return Status::Ok();
        },
        Deadline::FromNow(std::chrono::milliseconds(10)));
    EXPECT_TRUE(callback.ok()) << callback.ToString();
    EXPECT_EQ(observed, 55u);
    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ(allocator_.Inspect(handle).status().code(), StatusCode::kNotFound);
}

}  // namespace
}  // namespace mino
