// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/journal_channel_recovery.h"
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

struct FinalizeRaceContext {
    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
};

void BlockFirstFinalize(AllocationJournal::PersistencePoint point, uint64_t,
                        void* opaque) noexcept {
    if (point != AllocationJournal::PersistencePoint::kFinalizingTagged) {
        return;
    }
    auto* context = static_cast<FinalizeRaceContext*>(opaque);
    if (context->entered.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    while (!context->release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
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

TEST_F(RuntimeSpscTest,
       PublisherUsesStableIdsAndRegistersItsRecoveryChannelOnce) {
    const size_t journal_size = AllocationJournal::RequiredSize(1, 1);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 1, allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();
    JournalChannelRecoveryCoordinator recovery(*journal);

    Publisher<RuntimeTestMessage> compatible(allocator_, *channel_);
    EXPECT_EQ(compatible.channel_id(),
              Publisher<RuntimeTestMessage>::kDefaultSingleChannelId);

    Publisher<RuntimeTestMessage> legacy_zero(allocator_, *channel_,
                                              /*channel_id=*/0);
    EXPECT_EQ(legacy_zero.channel_id(),
              Publisher<RuntimeTestMessage>::kDefaultSingleChannelId);

    constexpr uint64_t kExplicitChannelId = 0xA501;
    Publisher<RuntimeTestMessage> explicit_id(
        allocator_, *channel_, kExplicitChannelId);
    EXPECT_EQ(explicit_id.channel_id(), kExplicitChannelId);
    EXPECT_TRUE(explicit_id.RegisterRecoveryChannel(recovery).ok());
    EXPECT_EQ(explicit_id.RegisterRecoveryChannel(recovery).code(),
              StatusCode::kAlreadyExists);
}

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

TEST_F(RuntimeSpscTest, AllocationJournalTracksBuilderRootAndChild) {
    constexpr uint32_t kTransactionCapacity = 1;
    constexpr uint32_t kHandlesPerTransaction = 2;
    const size_t journal_size = AllocationJournal::RequiredSize(
        kTransactionCapacity, kHandlesPerTransaction);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, kTransactionCapacity,
        kHandlesPerTransaction, allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();

    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_, *journal);
    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok()) << builder.status().ToString();
    (*builder)->id = 17;
    const ShmHandle root = builder->handle();

    AllocationRequest child_request;
    child_request.object_size = 16;
    child_request.type_id = TypeId{43};
    child_request.schema = SchemaIdentity{.short_id = 2, .layout_version = 1};
    child_request.alignment = 8;
    auto child_build = builder->AllocateChild(child_request);
    ASSERT_TRUE(child_build.ok()) << child_build.status().ToString();
    const ShmHandle child = child_build->handle;

    ASSERT_TRUE(publisher.Abort(std::move(*builder)).ok());
    EXPECT_EQ(allocator_.Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);

    auto committed = publisher.Allocate();
    ASSERT_TRUE(committed.ok());
    (*committed)->id = 18;
    auto committed_child_build = committed->AllocateChild(child_request);
    ASSERT_TRUE(committed_child_build.ok())
        << committed_child_build.status().ToString();
    const ShmHandle committed_child = committed_child_build->handle;
    ASSERT_TRUE(publisher.PublishLocal(std::move(*committed)).ok());
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);
    auto message = subscriber.TryPoll();
    ASSERT_TRUE(message.ok());
    EXPECT_EQ((*message)->id, 18u);
    EXPECT_TRUE(std::move(*message).Ack().ok());
    EXPECT_EQ(allocator_.Inspect(committed_child).status().code(),
              StatusCode::kNotFound)
        << "root ACK must reclaim every published transaction child";

    auto reused = publisher.Allocate();
    ASSERT_TRUE(reused.ok()) << "successful Channel commit must finalize Journal";
    EXPECT_TRUE(publisher.Abort(std::move(*reused)).ok());
}

TEST_F(RuntimeSpscTest, VisibleCommitWithFinalizeRaceReturnsPublishedSuccess) {
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, allocator_);
    ASSERT_TRUE(journal.ok());

    FinalizeRaceContext race;
    journal->SetPersistenceHook(&BlockFirstFinalize, &race);
    constexpr uint64_t kChannelId = 0xA502;
    const ProcessIdentity owner = ProcessIdentity::Current();
    Publisher<RuntimeTestMessage> publisher(
        allocator_, *channel_, kChannelId, *journal, owner);
    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 0xF1;

    Status publish_status = Status::Error(StatusCode::kInternal);
    std::thread publishing([&]() {
        publish_status = publisher.PublishLocal(std::move(*builder));
    });
    while (!race.entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    EXPECT_EQ(journal->RecoverOrphans(), 1u)
        << "kFinalizing is completed without owner liveness arbitration";
    race.release.store(true, std::memory_order_release);
    publishing.join();

    EXPECT_TRUE(publish_status.ok()) << publish_status.ToString();
    EXPECT_EQ(publisher.journal_cleanup_debt_count(), 1u);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);
    auto message = subscriber.TryPoll();
    ASSERT_TRUE(message.ok()) << "channel commit was already visible";
    EXPECT_EQ((*message)->id, 0xF1u);
    EXPECT_TRUE(std::move(*message).Ack().ok());
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

TEST_F(RuntimeSpscTest, ReceiptExhaustionPreventsLocalCommit) {
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
    const ShmHandle handle = builder->handle();
    auto receipt = publisher.Publish(std::move(*builder), receipts, identity,
                                     targets, requirement);
    ASSERT_FALSE(receipt.ok());
    EXPECT_EQ(receipt.status().code(), StatusCode::kResourceExhausted);

    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ(publisher.published_count(), 0u);
    EXPECT_EQ(receipts.outstanding(), 0u);
    EXPECT_EQ(allocator_.Inspect(handle).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(subscriber.TryPoll().status().code(), StatusCode::kWouldBlock);
}

TEST_F(RuntimeSpscTest, ReceiptReservationIsCanceledWhenLocalPublishFails) {
    Publisher<RuntimeTestMessage> fill(allocator_, *channel_);
    for (uint64_t i = 1; i <= kChannelCapacity; ++i) {
        auto builder = fill.Allocate();
        ASSERT_TRUE(builder.ok());
        (*builder)->id = i;
        ASSERT_TRUE(fill.PublishLocal(std::move(*builder)).ok());
    }

    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    OutstandingReceiptTable receipts(
        {.max_outstanding = 1, .max_per_publisher = 1});
    const PublisherReceiptIdentity identity{
        .process = ProcessIdentity::Current(),
        .publisher_id = PublisherId{4},
    };
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 12},
    };
    const DeliveryRequirement requirement{
        .stage = DeliveryStage::kRemoteAccepted,
        .completion = CompletionPolicy::kAll,
        .quorum = 0,
        .deadline = Deadline::Infinite(),
    };

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 90;
    const ShmHandle handle = builder->handle();
    auto receipt = publisher.Publish(std::move(*builder), receipts, identity,
                                     targets, requirement);
    ASSERT_FALSE(receipt.ok());
    EXPECT_EQ(receipt.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(receipts.outstanding(), 0u);
    EXPECT_EQ(allocator_.Inspect(handle).status().code(), StatusCode::kNotFound);

    auto replacement = receipts.Reserve(identity, targets, requirement);
    ASSERT_TRUE(replacement.ok())
        << "local failure must release the receipt reservation";
    replacement->Cancel();
}

TEST_F(RuntimeSpscTest, ReceiptReservationIsCanceledWhenLocalPublishDrops) {
    Publisher<RuntimeTestMessage> fill(allocator_, *channel_);
    for (uint64_t i = 1; i <= kChannelCapacity; ++i) {
        auto builder = fill.Allocate();
        ASSERT_TRUE(builder.ok());
        (*builder)->id = i;
        ASSERT_TRUE(fill.PublishLocal(std::move(*builder)).ok());
    }

    PublisherOptions options;
    options.queue_full_policy = QueueFullPolicy::kDropNewest;
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_, options);
    OutstandingReceiptTable receipts(
        {.max_outstanding = 1, .max_per_publisher = 1});
    const PublisherReceiptIdentity identity{
        .process = ProcessIdentity::Current(),
        .publisher_id = PublisherId{3},
    };
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 11},
    };
    const DeliveryRequirement requirement{
        .stage = DeliveryStage::kRemoteAccepted,
        .completion = CompletionPolicy::kAll,
        .quorum = 0,
        .deadline = Deadline::Infinite(),
    };

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 89;
    const ShmHandle handle = builder->handle();
    auto receipt = publisher.Publish(std::move(*builder), receipts, identity,
                                     targets, requirement);
    ASSERT_FALSE(receipt.ok());
    EXPECT_EQ(receipt.status().code(), StatusCode::kDegraded);
    EXPECT_EQ(publisher.dropped_count(), 1u);
    EXPECT_EQ(receipts.outstanding(), 0u);
    EXPECT_EQ(allocator_.Inspect(handle).status().code(), StatusCode::kNotFound);

    auto replacement = receipts.Reserve(identity, targets, requirement);
    ASSERT_TRUE(replacement.ok())
        << "Drop must release the per-publisher receipt reservation";
    replacement->Cancel();
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

TEST_F(RuntimeSpscTest,
       BroadcastDropOldestReturnsWouldBlockForLongBorrow) {
    constexpr uint64_t kCapacity = 4;
    auto broadcast_memory =
        AllocateAligned(BroadcastChannel::RequiredSize(kCapacity));
    auto broadcast = BroadcastChannel::Init(broadcast_memory.get(), kCapacity);
    ASSERT_TRUE(broadcast.ok());
    auto handle = broadcast->RegisterSubscriber(SubscriberId{0});
    ASSERT_TRUE(handle.ok());

    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  allocator_);
    ASSERT_TRUE(pins.ok());

    PublisherOptions options;
    options.queue_full_policy = QueueFullPolicy::kDropOldest;
    Publisher<RuntimeTestMessage> publisher(allocator_, *broadcast, *pins,
                                            options);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *broadcast, *handle,
                                              *pins);
    for (uint64_t id = 1; id <= kCapacity; ++id) {
        auto builder = publisher.Allocate();
        ASSERT_TRUE(builder.ok());
        (*builder)->id = id;
        ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());
    }

    auto held = subscriber.TryPoll();
    ASSERT_TRUE(held.ok()) << held.status().ToString();
    const ShmHandle oldest = held->metadata().payload;
    ASSERT_EQ((*held)->id, 1u);
    EXPECT_EQ(pins->PinCount(oldest), 1u);

    auto replacement = publisher.Allocate();
    ASSERT_TRUE(replacement.ok());
    (*replacement)->id = 5;
    const ShmHandle rejected = replacement->handle();
    const Status blocked = publisher.PublishLocal(std::move(*replacement));
    EXPECT_EQ(blocked.code(), StatusCode::kWouldBlock);
    EXPECT_EQ(publisher.published_count(), 4u);
    EXPECT_EQ(publisher.dropped_count(), 0u);
    EXPECT_EQ(allocator_.Inspect(rejected).status().code(),
              StatusCode::kNotFound);

    auto published = allocator_.Inspect(oldest);
    ASSERT_TRUE(published.ok()) << published.status().ToString();
    EXPECT_EQ(published->state, ObjectState::kPublished);
    EXPECT_EQ((*held)->id, 1u) << "long Borrow must keep payload readable";
    EXPECT_EQ(subscriber.LastBroadcastGap().status().code(),
              StatusCode::kNotFound);

    EXPECT_TRUE(std::move(*held).Ack().ok());
    EXPECT_EQ(pins->PinCount(oldest), 0u);
    EXPECT_EQ(allocator_.Inspect(oldest).status().code(), StatusCode::kNotFound);

    auto retry = publisher.Allocate();
    ASSERT_TRUE(retry.ok());
    (*retry)->id = 5;
    EXPECT_TRUE(publisher.PublishLocal(std::move(*retry)).ok());
    EXPECT_EQ(publisher.published_count(), 5u);
    EXPECT_EQ(publisher.dropped_count(), 0u);
}

TEST_F(RuntimeSpscTest,
       BroadcastDropOldestDefersReclaimForTransferredPin) {
    constexpr uint64_t kCapacity = 2;
    auto broadcast_memory =
        AllocateAligned(BroadcastChannel::RequiredSize(kCapacity));
    auto broadcast = BroadcastChannel::Init(broadcast_memory.get(), kCapacity);
    ASSERT_TRUE(broadcast.ok());
    auto pinning_handle = broadcast->RegisterSubscriber(SubscriberId{0});
    auto slow_handle = broadcast->RegisterSubscriber(SubscriberId{1});
    ASSERT_TRUE(pinning_handle.ok() && slow_handle.ok());

    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  allocator_);
    ASSERT_TRUE(pins.ok());
    PublisherOptions options;
    options.queue_full_policy = QueueFullPolicy::kDropOldest;
    Publisher<RuntimeTestMessage> publisher(allocator_, *broadcast, *pins,
                                            options);
    Subscriber<RuntimeTestMessage> pinning(
        allocator_, *broadcast, *pinning_handle, *pins);
    Subscriber<RuntimeTestMessage> slow(allocator_, *broadcast, *slow_handle,
                                        *pins);

    for (uint64_t id = 1; id <= kCapacity; ++id) {
        auto builder = publisher.Allocate();
        ASSERT_TRUE(builder.ok());
        (*builder)->id = id;
        ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());
    }
    auto borrowed = pinning.TryPoll();
    ASSERT_TRUE(borrowed.ok());
    const ShmHandle oldest = borrowed->metadata().payload;
    auto pinned = std::move(*borrowed).Transfer();
    ASSERT_TRUE(pinned.ok()) << pinned.status().ToString();
    EXPECT_EQ(pins->PinCount(oldest), 1u);

    auto replacement = publisher.Allocate();
    ASSERT_TRUE(replacement.ok());
    (*replacement)->id = 3;
    ASSERT_TRUE(publisher.PublishLocal(std::move(*replacement)).ok());
    EXPECT_EQ(publisher.dropped_count(), 1u);
    auto retired = allocator_.Inspect(oldest);
    ASSERT_TRUE(retired.ok());
    EXPECT_EQ(retired->state, ObjectState::kRetired);
    EXPECT_EQ((*pinned)->id, 1u);

    auto gap_signal = slow.TryPoll();
    ASSERT_FALSE(gap_signal.ok());
    EXPECT_EQ(gap_signal.status().code(), StatusCode::kDegraded);
    ASSERT_TRUE(pinned->Release().ok());
    EXPECT_EQ(allocator_.Inspect(oldest).status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, BroadcastDropOldestPublisherAndSubscriberRaceSafely) {
    constexpr uint64_t kCapacity = 4;
    constexpr uint64_t kMessages = 200;
    auto broadcast_memory =
        AllocateAligned(BroadcastChannel::RequiredSize(kCapacity));
    auto broadcast = BroadcastChannel::Init(broadcast_memory.get(), kCapacity);
    ASSERT_TRUE(broadcast.ok());
    auto handle = broadcast->RegisterSubscriber(SubscriberId{0});
    ASSERT_TRUE(handle.ok());
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  allocator_);
    ASSERT_TRUE(pins.ok());

    PublisherOptions options;
    options.queue_full_policy = QueueFullPolicy::kDropOldest;
    Publisher<RuntimeTestMessage> publisher(allocator_, *broadcast, *pins,
                                            options);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *broadcast, *handle,
                                              *pins);
    std::atomic<bool> publisher_done{false};
    std::atomic<uint64_t> failures{0};
    std::atomic<uint64_t> gap_signals{0};

    std::thread publishing([&]() {
        for (uint64_t id = 1; id <= kMessages;) {
            auto builder = publisher.Allocate();
            if (!builder.ok()) {
                failures.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            (*builder)->id = id;
            const Status status = publisher.PublishLocal(std::move(*builder));
            if (status.ok()) {
                ++id;
                continue;
            }
            if (status.code() != StatusCode::kWouldBlock) {
                failures.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            std::this_thread::yield();
        }
        publisher_done.store(true, std::memory_order_release);
    });

    std::thread consuming([&]() {
        while (publisher.dropped_count() == 0 &&
               !publisher_done.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        uint64_t last_id = 0;
        for (;;) {
            auto message = subscriber.TryPoll();
            if (message.ok()) {
                if ((*message)->id <= last_id) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
                last_id = (*message)->id;
                const Status ack = std::move(*message).Ack();
                if (!ack.ok()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
                continue;
            }
            if (message.status().code() == StatusCode::kDegraded) {
                gap_signals.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            if (message.status().code() != StatusCode::kWouldBlock) {
                failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            if (publisher_done.load(std::memory_order_acquire)) {
                return;
            }
            std::this_thread::yield();
        }
    });

    publishing.join();
    consuming.join();
    ASSERT_EQ(failures.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(publisher.published_count(), kMessages);
    EXPECT_GT(publisher.dropped_count(), 0u);
    EXPECT_GT(gap_signals.load(std::memory_order_relaxed), 0u);
    auto stats = subscriber.BroadcastStats();
    ASSERT_TRUE(stats.ok()) << stats.status().ToString();
    EXPECT_EQ(stats->gap_messages, publisher.dropped_count());
    EXPECT_EQ(stats->gap_events, stats->gap_messages);
    EXPECT_LE(gap_signals.load(std::memory_order_relaxed), stats->gap_events);
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
