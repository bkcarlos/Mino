// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/journal_channel_recovery.h"
#include "mino/runtime/publisher.h"
#include "mino/runtime/subscriber.h"
#include "mino/schema/codegen/testdata/golden.generated.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <set>
#include <string>
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

struct RuntimeGraphMessage {
    uint64_t id;
    uint64_t child_offset;
    uint32_t child_generation;
    uint32_t child_region_id;
};

static_assert(std::is_trivially_copyable_v<RuntimeGraphMessage>);
static_assert(std::is_standard_layout_v<RuntimeGraphMessage>);

template <>
struct StaticMessageTraits<RuntimeGraphMessage> {
    static constexpr bool kIsSpecialized = true;
    static constexpr TypeId type_id{43};
    static constexpr uint32_t message_type = 0x10203041u;
    static constexpr uint32_t schema_version = (1u << 16);
    static constexpr uint64_t schema_short_id = 0xAABBCCDDEEFF0022ULL;
    static constexpr uint32_t layout_version = 1;
    static constexpr uint32_t index_flags = kIndexSlotFlagHasChildSlabs;
    static constexpr bool kOwnedGraphCollectionSupported = true;
    static constexpr size_t kMaxOwnedGraphHandles = 2;

    static Status Validate(const RuntimeGraphMessage& message) noexcept {
        return message.id == 0
                   ? Status::Error(StatusCode::kInvalidArgument,
                                   "graph message id must be non-zero")
                   : Status::Ok();
    }

    static Status CollectOwnedGraph(
        ShmHandle root, const RuntimeGraphMessage& message,
        std::span<ShmHandle> output, size_t& handle_count) noexcept {
        handle_count = 0;
        OwnedGraphCollector collector(output);
        MINO_RETURN_IF_ERROR(collector.AddRoot(root));
        const ShmHandle child{.offset = message.child_offset,
                              .generation = message.child_generation,
                              .region_id = message.child_region_id};
        if (!child.IsNull()) {
            MINO_RETURN_IF_ERROR(collector.AddOwnedChild(child));
        }
        handle_count = collector.size();
        return Status::Ok();
    }
};

namespace {

void SetGraphChild(RuntimeGraphMessage& message, ShmHandle child) {
    message.child_offset = child.offset;
    message.child_generation = child.generation;
    message.child_region_id = child.region_id;
}

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

ClassTableConfig AllocatorConfig(uint32_t slot_count = 16) {
    ClassTableConfig config;
    config.classes = {{.slot_size = 64, .slot_count = slot_count}};
    return config;
}

ClassTableConfig GeneratedGraphAllocatorConfig() {
    ClassTableConfig config;
    config.classes = {
        {.slot_size = 64, .slot_count = 32},
        {.slot_size = 256, .slot_count = 16},
    };
    return config;
}

struct GeneratedGraphBuild {
    MessageBuilder<golden::Telemetry> root;
    ShmHandle child;
};

Result<GeneratedGraphBuild> AllocateGeneratedGraph(
    Publisher<golden::Telemetry>& publisher, uint32_t sequence) {
    MINO_ASSIGN_OR_RETURN(auto root, publisher.Allocate());
    golden::TelemetryBuilder generated(*root);
    generated.set_sequence(sequence);
    if (!generated.set_samples({.element_size = 8})) {
        return Status::Error(StatusCode::kInternal,
                             "failed to initialize generated samples metadata");
    }

    AllocationRequest child_request;
    child_request.object_size = 32;
    child_request.type_id = TypeId{0x4743};
    child_request.schema = {.short_id = 0x4743, .layout_version = 1};
    child_request.alignment = 8;
    MINO_ASSIGN_OR_RETURN(MutableBuildView child,
                          root.AllocateChild(child_request));
    std::memset(child.data, 0x5A, child.object_size);
    if (!generated.set_payload({.offset = child.handle.offset,
                                .generation = child.handle.generation,
                                .region_id = child.handle.region_id,
                                .length = 8,
                                .capacity = child.object_size,
                                .element_size = 1})) {
        return Status::Error(StatusCode::kInternal,
                             "failed to set generated child metadata");
    }
    return GeneratedGraphBuild{.root = std::move(root), .child = child.handle};
}

void InitializeGeneratedLeaf(golden::Telemetry& value,
                             uint32_t sequence) {
    golden::TelemetryBuilder generated(value);
    generated.set_sequence(sequence);
    ASSERT_TRUE(generated.set_payload({.element_size = 1}));
    ASSERT_TRUE(generated.set_samples({.element_size = 8}));
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

Result<std::vector<MessageBuilder<RuntimeTestMessage>>> AllocateBatch(
    Publisher<RuntimeTestMessage>& publisher, uint64_t first_id,
    size_t count) {
    std::vector<MessageBuilder<RuntimeTestMessage>> builders;
    builders.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        auto builder = publisher.Allocate();
        if (!builder.ok()) {
            return builder.status();
        }
        (*builder)->id = first_id + i;
        (*builder)->value = static_cast<uint32_t>(first_id + i);
        builders.push_back(std::move(*builder));
    }
    return builders;
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
    const uint64_t append_gap_before =
        allocator_.local_cache_stats().append_gap_reclaim_scans;
    EXPECT_TRUE(std::move(*message).Ack().ok());
    EXPECT_EQ(allocator_.local_cache_stats().append_gap_reclaim_scans,
              append_gap_before + 1)
        << "traits without a collector retain recovery-compatible root fallback";
    EXPECT_EQ(allocator_.Inspect(committed_child).status().code(),
              StatusCode::kNotFound)
        << "root ACK must reclaim every published transaction child";

    auto reused = publisher.Allocate();
    ASSERT_TRUE(reused.ok()) << "successful Channel commit must finalize Journal";
    EXPECT_TRUE(publisher.Abort(std::move(*reused)).ok());
}

TEST_F(RuntimeSpscTest,
       GeneratedGraphPublishAndNormalAckUseExactManifestWithoutAppendGap) {
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();

    Publisher<RuntimeGraphMessage> publisher(allocator_, *channel_, *journal);
    Subscriber<RuntimeGraphMessage> subscriber(allocator_, *channel_);
    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok()) << builder.status().ToString();
    (*builder)->id = 0x701;
    AllocationRequest child_request;
    child_request.object_size = 16;
    child_request.type_id = TypeId{44};
    child_request.schema = {.short_id = 3, .layout_version = 1};
    child_request.alignment = 8;
    auto child_build = builder->AllocateChild(child_request);
    ASSERT_TRUE(child_build.ok()) << child_build.status().ToString();
    SetGraphChild(**builder, child_build->handle);
    const ShmHandle root = builder->handle();
    const ShmHandle child = child_build->handle;

    const auto before = allocator_.local_cache_stats();
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());
    auto borrowed = subscriber.TryPoll();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    ASSERT_TRUE(std::move(*borrowed).Ack().ok());

    const auto after = allocator_.local_cache_stats();
    EXPECT_EQ(after.published_graph_reclaims,
              before.published_graph_reclaims + 1);
    EXPECT_EQ(after.append_gap_reclaim_scans,
              before.append_gap_reclaim_scans);
    EXPECT_EQ(allocator_.Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, GeneratedGraphPublishRejectsJournalManifestMismatch) {
    const size_t journal_size = AllocationJournal::RequiredSize(1, 3);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 3, allocator_);
    ASSERT_TRUE(journal.ok());
    Publisher<RuntimeGraphMessage> publisher(allocator_, *channel_, *journal);

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 0x702;
    AllocationRequest child_request;
    child_request.object_size = 16;
    child_request.type_id = TypeId{44};
    child_request.schema = {.short_id = 3, .layout_version = 1};
    auto reachable = builder->AllocateChild(child_request);
    auto unreachable = builder->AllocateChild(child_request);
    ASSERT_TRUE(reachable.ok() && unreachable.ok());
    SetGraphChild(**builder, reachable->handle);
    const ShmHandle root = builder->handle();
    const ShmHandle reachable_handle = reachable->handle;
    const ShmHandle unreachable_handle = unreachable->handle;

    const Status publish = publisher.PublishLocal(std::move(*builder));
    EXPECT_EQ(publish.code(), StatusCode::kCorruption);
    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(reachable_handle).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(unreachable_handle).status().code(),
              StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest,
       GraphCollectorFailureAcksThenUsesRootRecoveryFallbackAndReportsCorruption) {
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, allocator_);
    ASSERT_TRUE(journal.ok());
    Publisher<RuntimeGraphMessage> publisher(allocator_, *channel_, *journal);
    Subscriber<RuntimeGraphMessage> subscriber(allocator_, *channel_);

    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 0x703;
    AllocationRequest child_request;
    child_request.object_size = 16;
    child_request.type_id = TypeId{44};
    child_request.schema = {.short_id = 3, .layout_version = 1};
    auto child_build = builder->AllocateChild(child_request);
    ASSERT_TRUE(child_build.ok());
    SetGraphChild(**builder, child_build->handle);
    const ShmHandle root = builder->handle();
    const ShmHandle child = child_build->handle;
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

    auto root_view = allocator_.Inspect(root);
    ASSERT_TRUE(root_view.ok());
    auto* mutable_root =
        const_cast<RuntimeGraphMessage*>(
            static_cast<const RuntimeGraphMessage*>(root_view->data));
    SetGraphChild(*mutable_root, root);

    const uint64_t append_gap_before =
        allocator_.local_cache_stats().append_gap_reclaim_scans;
    auto borrowed = subscriber.TryPoll();
    ASSERT_TRUE(borrowed.ok());
    const Status ack = std::move(*borrowed).Ack();
    EXPECT_EQ(ack.code(), StatusCode::kCorruption);
    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ(allocator_.local_cache_stats().append_gap_reclaim_scans,
              append_gap_before + 1);
    EXPECT_EQ(allocator_.Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest,
       GraphAllocatorValidationFailureNeverReclaimsForeignAllocation) {
    AllocationRequest foreign_request;
    foreign_request.object_size = 16;
    foreign_request.type_id = TypeId{99};
    foreign_request.schema = {.short_id = 99, .layout_version = 1};
    auto foreign = allocator_.Allocate(foreign_request);
    ASSERT_TRUE(foreign.ok());
    ASSERT_TRUE(allocator_.BeginBuild(*foreign).ok());
    ASSERT_TRUE(allocator_.Publish(*foreign).ok());

    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, allocator_);
    ASSERT_TRUE(journal.ok());
    Publisher<RuntimeGraphMessage> publisher(allocator_, *channel_, *journal);
    Subscriber<RuntimeGraphMessage> subscriber(allocator_, *channel_);
    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 0x704;
    AllocationRequest child_request = foreign_request;
    auto owned_child = builder->AllocateChild(child_request);
    ASSERT_TRUE(owned_child.ok());
    SetGraphChild(**builder, owned_child->handle);
    const ShmHandle root = builder->handle();
    const ShmHandle owned = owned_child->handle;
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

    auto root_view = allocator_.Inspect(root);
    ASSERT_TRUE(root_view.ok());
    auto* mutable_root =
        const_cast<RuntimeGraphMessage*>(
            static_cast<const RuntimeGraphMessage*>(root_view->data));
    SetGraphChild(*mutable_root, *foreign);

    auto borrowed = subscriber.TryPoll();
    ASSERT_TRUE(borrowed.ok());
    EXPECT_EQ(std::move(*borrowed).Ack().code(), StatusCode::kCorruption);
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(owned).status().code(), StatusCode::kNotFound);
    auto foreign_view = allocator_.Inspect(*foreign);
    ASSERT_TRUE(foreign_view.ok())
        << "fail-closed graph validation must not reclaim a foreign allocation";
    EXPECT_EQ(foreign_view->state, ObjectState::kPublished);
    ASSERT_TRUE(allocator_.Retire(*foreign).ok());
    ASSERT_TRUE(allocator_.Reclaim(*foreign).ok());
}

TEST(RuntimeGeneratedGraphLifecycleTest,
     SpscTransferKeepsRootAndChildUntilFinalPinRelease) {
    constexpr uint64_t kAllocatorBytes = 1u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto allocator = CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes,
        GeneratedGraphAllocatorConfig());
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    auto channel_memory = AllocateAligned(SpscChannel::RequiredSize(2));
    auto channel = SpscChannel::Init(channel_memory.get(), 2);
    ASSERT_TRUE(channel.ok()) << channel.status().ToString();
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, *allocator);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  *allocator);
    ASSERT_TRUE(pins.ok()) << pins.status().ToString();

    Publisher<golden::Telemetry> publisher(*allocator, *channel, *journal);
    Subscriber<golden::Telemetry> subscriber(
        *allocator, *channel, &*pins, ProcessIdentity::Current());
    auto graph = AllocateGeneratedGraph(publisher, 0x801);
    ASSERT_TRUE(graph.ok()) << graph.status().ToString();
    const ShmHandle root = graph->root.handle();
    const ShmHandle child = graph->child;
    const auto before = allocator->local_cache_stats();
    ASSERT_TRUE(publisher.PublishLocal(std::move(graph->root)).ok());

    auto borrowed = subscriber.TryPoll();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    auto transferred = std::move(*borrowed).Transfer();
    ASSERT_TRUE(transferred.ok()) << transferred.status().ToString();
    EXPECT_TRUE(channel->IsEmpty());
    auto held_root = allocator->Inspect(root);
    auto held_child = allocator->Inspect(child);
    ASSERT_TRUE(held_root.ok() && held_child.ok());
    EXPECT_EQ(held_root->state, ObjectState::kRetired);
    EXPECT_EQ(held_child->state, ObjectState::kPublished);
    EXPECT_EQ(allocator->local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims)
        << "Transfer must bypass typed synchronous graph reclaim";
    EXPECT_EQ(allocator->local_cache_stats().append_gap_reclaim_scans,
              before.append_gap_reclaim_scans)
        << "a live transferred Pin must block fallback before any scan";

    ASSERT_TRUE(transferred->Release().ok());
    EXPECT_EQ(allocator->Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator->Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator->local_cache_stats().append_gap_reclaim_scans,
              before.append_gap_reclaim_scans + 1);
}

TEST(RuntimeGeneratedGraphLifecycleTest,
     BroadcastFinalAckKeepsWholeGraphUntilLastSubscriber) {
    constexpr uint64_t kAllocatorBytes = 1u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto allocator = CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes,
        GeneratedGraphAllocatorConfig());
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    auto channel_memory =
        AllocateAligned(BroadcastChannel::RequiredSize(2));
    auto channel = BroadcastChannel::Init(channel_memory.get(), 2);
    ASSERT_TRUE(channel.ok()) << channel.status().ToString();
    auto first_handle = channel->RegisterSubscriber(SubscriberId{0});
    auto second_handle = channel->RegisterSubscriber(SubscriberId{1});
    ASSERT_TRUE(first_handle.ok() && second_handle.ok());
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, *allocator);
    ASSERT_TRUE(journal.ok());
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  *allocator);
    ASSERT_TRUE(pins.ok());

    Publisher<golden::Telemetry> publisher(
        *allocator, *channel, *pins, *journal);
    Subscriber<golden::Telemetry> first(
        *allocator, *channel, *first_handle, *pins);
    Subscriber<golden::Telemetry> second(
        *allocator, *channel, *second_handle, *pins);
    auto graph = AllocateGeneratedGraph(publisher, 0x802);
    ASSERT_TRUE(graph.ok()) << graph.status().ToString();
    const ShmHandle root = graph->root.handle();
    const ShmHandle child = graph->child;
    const auto before = allocator->local_cache_stats();
    ASSERT_TRUE(publisher.PublishLocal(std::move(graph->root)).ok());

    auto first_borrow = first.TryPoll();
    auto second_borrow = second.TryPoll();
    ASSERT_TRUE(first_borrow.ok() && second_borrow.ok());
    ASSERT_TRUE(std::move(*first_borrow).Ack().ok());
    auto intermediate_root = allocator->Inspect(root);
    auto intermediate_child = allocator->Inspect(child);
    ASSERT_TRUE(intermediate_root.ok() && intermediate_child.ok());
    EXPECT_EQ(intermediate_root->state, ObjectState::kPublished);
    EXPECT_EQ(intermediate_child->state, ObjectState::kPublished);
    EXPECT_EQ(allocator->local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims);
    EXPECT_EQ(allocator->local_cache_stats().append_gap_reclaim_scans,
              before.append_gap_reclaim_scans);

    ASSERT_TRUE(std::move(*second_borrow).Ack().ok());
    EXPECT_EQ(allocator->Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator->Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator->local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims)
        << "Broadcast must remain on channel/Pin fallback";
    EXPECT_EQ(allocator->local_cache_stats().append_gap_reclaim_scans,
              before.append_gap_reclaim_scans + 1);
}

TEST(RuntimeGeneratedGraphLifecycleTest,
     BroadcastTransferDefersWholeGraphPastFinalChannelAck) {
    constexpr uint64_t kAllocatorBytes = 1u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto allocator = CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes,
        GeneratedGraphAllocatorConfig());
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    auto channel_memory =
        AllocateAligned(BroadcastChannel::RequiredSize(2));
    auto channel = BroadcastChannel::Init(channel_memory.get(), 2);
    ASSERT_TRUE(channel.ok());
    auto transferring_handle = channel->RegisterSubscriber(SubscriberId{0});
    auto final_handle = channel->RegisterSubscriber(SubscriberId{1});
    ASSERT_TRUE(transferring_handle.ok() && final_handle.ok());
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, *allocator);
    ASSERT_TRUE(journal.ok());
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  *allocator);
    ASSERT_TRUE(pins.ok());

    Publisher<golden::Telemetry> publisher(
        *allocator, *channel, *pins, *journal);
    Subscriber<golden::Telemetry> transferring(
        *allocator, *channel, *transferring_handle, *pins);
    Subscriber<golden::Telemetry> final_subscriber(
        *allocator, *channel, *final_handle, *pins);
    auto graph = AllocateGeneratedGraph(publisher, 0x803);
    ASSERT_TRUE(graph.ok()) << graph.status().ToString();
    const ShmHandle root = graph->root.handle();
    const ShmHandle child = graph->child;
    const auto before = allocator->local_cache_stats();
    ASSERT_TRUE(publisher.PublishLocal(std::move(graph->root)).ok());

    auto transferring_borrow = transferring.TryPoll();
    ASSERT_TRUE(transferring_borrow.ok());
    auto transferred = std::move(*transferring_borrow).Transfer();
    ASSERT_TRUE(transferred.ok()) << transferred.status().ToString();
    auto final_borrow = final_subscriber.TryPoll();
    ASSERT_TRUE(final_borrow.ok());
    ASSERT_TRUE(std::move(*final_borrow).Ack().ok());

    auto held_root = allocator->Inspect(root);
    auto held_child = allocator->Inspect(child);
    ASSERT_TRUE(held_root.ok() && held_child.ok());
    EXPECT_EQ(held_root->state, ObjectState::kRetired);
    EXPECT_EQ(held_child->state, ObjectState::kPublished);
    EXPECT_EQ(allocator->local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims);
    EXPECT_EQ(allocator->local_cache_stats().append_gap_reclaim_scans,
              before.append_gap_reclaim_scans)
        << "final channel ACK must not scan while a transferred Pin lives";

    ASSERT_TRUE(transferred->Release().ok());
    EXPECT_EQ(allocator->Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator->Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator->local_cache_stats().append_gap_reclaim_scans,
              before.append_gap_reclaim_scans + 1);
}

TEST(RuntimeGeneratedGraphLifecycleTest,
     GeneratedLeafPublishesWithoutAllocationJournal) {
    constexpr uint64_t kAllocatorBytes = 1u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto allocator = CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes,
        GeneratedGraphAllocatorConfig());
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
    auto channel_memory = AllocateAligned(SpscChannel::RequiredSize(2));
    auto channel = SpscChannel::Init(channel_memory.get(), 2);
    ASSERT_TRUE(channel.ok());

    Publisher<golden::Telemetry> publisher(*allocator, *channel);
    auto leaf = publisher.Allocate();
    ASSERT_TRUE(leaf.ok()) << leaf.status().ToString();
    InitializeGeneratedLeaf(**leaf, 0x804);
    const ShmHandle root = leaf->handle();
    ASSERT_TRUE(publisher.PublishLocal(std::move(*leaf)).ok());
    EXPECT_EQ(publisher.published_count(), 1u);

    auto raw_borrow = channel->Poll();
    ASSERT_TRUE(raw_borrow.ok());
    EXPECT_EQ(raw_borrow->slot()->payload, root);
    ASSERT_TRUE(std::move(*raw_borrow).Ack().ok());
    ASSERT_TRUE(allocator->Retire(root).ok());
    ASSERT_TRUE(allocator->Reclaim(root).ok());
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
    std::atomic<uint32_t> first_failure{0};
    std::atomic<uint16_t> first_status{
        static_cast<uint16_t>(StatusCode::kOk)};
    std::string first_status_detail;
    std::atomic<uint64_t> gap_signals{0};
    auto record_failure = [&](uint32_t category, const Status& status) {
        uint32_t expected = 0;
        if (first_failure.compare_exchange_strong(
                expected, category, std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            first_status.store(static_cast<uint16_t>(status.code()),
                               std::memory_order_relaxed);
            first_status_detail = status.ToString();
        }
        failures.fetch_add(1, std::memory_order_relaxed);
    };

    std::thread publishing([&]() {
        for (uint64_t id = 1; id <= kMessages;) {
            auto builder = publisher.Allocate();
            if (!builder.ok()) {
                record_failure(1, builder.status());
                break;
            }
            (*builder)->id = id;
            const Status status = publisher.PublishLocal(std::move(*builder));
            if (status.ok()) {
                ++id;
                continue;
            }
            if (status.code() != StatusCode::kWouldBlock) {
                record_failure(2, status);
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
                    record_failure(
                        3, Status::Error(StatusCode::kCorruption,
                                         "non-monotonic message ID"));
                }
                last_id = (*message)->id;
                const Status ack = std::move(*message).Ack();
                if (!ack.ok()) {
                    record_failure(4, ack);
                }
                continue;
            }
            if (message.status().code() == StatusCode::kDegraded) {
                gap_signals.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            if (message.status().code() != StatusCode::kWouldBlock) {
                record_failure(5, message.status());
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
    ASSERT_EQ(failures.load(std::memory_order_relaxed), 0u)
        << "first failure category="
        << first_failure.load(std::memory_order_relaxed)
        << " status=" << first_status.load(std::memory_order_relaxed)
        << " detail=" << first_status_detail;
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

TEST_F(RuntimeSpscTest, SpscBatchPublishPollIndividualAndDestructorAck) {
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);

    auto builders = AllocateBatch(publisher, 1, 3);
    ASSERT_TRUE(builders.ok()) << builders.status().ToString();
    std::vector<ShmHandle> handles;
    for (const auto& builder : *builders) {
        handles.push_back(builder.handle());
    }
    const BatchPublishResult published = publisher.PublishBatch(*builders);
    ASSERT_TRUE(published.ok()) << published.first_error.ToString();
    EXPECT_EQ(published.requested_count, 3u);
    EXPECT_EQ(published.committed_count, 3u);
    EXPECT_EQ(publisher.batch_publish_calls(), 1u);
    EXPECT_EQ(publisher.batch_committed_items(), 3u);

    auto batch = subscriber.TryPollBatch(3);
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    ASSERT_EQ(batch->size(), 3u);
    EXPECT_TRUE(batch->poll_status().ok());
    EXPECT_EQ((*batch)[0]->id, 1u);
    EXPECT_EQ((*batch)[1]->id, 2u);
    EXPECT_EQ((*batch)[2]->id, 3u);
    EXPECT_EQ(subscriber.batch_poll_calls(), 1u);
    EXPECT_EQ(subscriber.batch_polled_messages(), 3u);

    EXPECT_TRUE(batch->Ack(2).ok()) << "out-of-order ACK must be deferred";
    EXPECT_FALSE(channel_->IsEmpty());
    EXPECT_TRUE(batch->Ack(0).ok());
    EXPECT_FALSE(channel_->IsEmpty());
    const BatchAckResult acked = batch->AckAll();
    EXPECT_TRUE(acked.ok()) << acked.first_error.ToString();
    EXPECT_EQ(acked.acked_count, 2u);
    EXPECT_TRUE(channel_->IsEmpty());
    for (ShmHandle handle : handles) {
        EXPECT_EQ(allocator_.Inspect(handle).status().code(),
                  StatusCode::kNotFound);
    }

    auto destructor_builders = AllocateBatch(publisher, 10, 2);
    ASSERT_TRUE(destructor_builders.ok());
    EXPECT_TRUE(publisher.PublishBatch(*destructor_builders).ok());
    {
        auto destructor_batch = subscriber.TryPollBatch(2);
        ASSERT_TRUE(destructor_batch.ok());
        EXPECT_EQ(destructor_batch->size(), 2u);
    }
    EXPECT_TRUE(channel_->IsEmpty())
        << "batch destruction must ACK the remaining prefix safely";
}

TEST_F(RuntimeSpscTest,
       SpscBatchReportsPartialFullDropAndDeadlineWithoutLeaks) {
    Publisher<RuntimeTestMessage> fill(allocator_, *channel_);
    auto initial = AllocateBatch(fill, 1, kChannelCapacity - 1);
    ASSERT_TRUE(initial.ok());
    ASSERT_TRUE(fill.PublishBatch(*initial).ok());

    Publisher<RuntimeTestMessage> failing(allocator_, *channel_);
    auto partial = AllocateBatch(failing, 4, 3);
    ASSERT_TRUE(partial.ok());
    const ShmHandle failed = (*partial)[1].handle();
    const ShmHandle unattempted = (*partial)[2].handle();
    const BatchPublishResult partial_result = failing.PublishBatch(*partial);
    ASSERT_FALSE(partial_result.ok());
    EXPECT_EQ(partial_result.committed_count, 1u);
    EXPECT_EQ(partial_result.first_failed_index, 1u);
    EXPECT_EQ(partial_result.first_error.code(),
              StatusCode::kResourceExhausted);
    EXPECT_TRUE(partial_result.cleanup_error.ok());
    EXPECT_EQ(allocator_.Inspect(failed).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(unattempted).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(failing.batch_failed_items(), 2u);

    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);
    auto full_batch = subscriber.TryPollBatch(kChannelCapacity);
    ASSERT_TRUE(full_batch.ok());
    ASSERT_EQ(full_batch->size(), kChannelCapacity);
    EXPECT_EQ((*full_batch)[3]->id, 4u)
        << "the committed prefix must remain visible after partial failure";
    EXPECT_TRUE(full_batch->AckAll().ok());

    auto refill = AllocateBatch(fill, 20, kChannelCapacity);
    ASSERT_TRUE(refill.ok());
    ASSERT_TRUE(fill.PublishBatch(*refill).ok());

    PublisherOptions drop_options;
    drop_options.queue_full_policy = QueueFullPolicy::kDropNewest;
    Publisher<RuntimeTestMessage> dropping(allocator_, *channel_, drop_options);
    auto dropped = AllocateBatch(dropping, 30, 2);
    ASSERT_TRUE(dropped.ok());
    const ShmHandle dropped_handle = (*dropped)[0].handle();
    const ShmHandle after_drop = (*dropped)[1].handle();
    const BatchPublishResult drop_result = dropping.PublishBatch(*dropped);
    ASSERT_FALSE(drop_result.ok());
    EXPECT_EQ(drop_result.committed_count, 0u);
    EXPECT_EQ(drop_result.dropped_input_count, 1u);
    EXPECT_EQ(drop_result.first_failed_index, 0u);
    EXPECT_EQ(drop_result.first_error.code(), StatusCode::kDegraded);
    EXPECT_EQ(allocator_.Inspect(dropped_handle).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(after_drop).status().code(),
              StatusCode::kNotFound);

    PublisherOptions block_options;
    block_options.queue_full_policy = QueueFullPolicy::kBlock;
    Publisher<RuntimeTestMessage> blocking(allocator_, *channel_, block_options);
    auto timed = AllocateBatch(blocking, 40, 2);
    ASSERT_TRUE(timed.ok());
    const BatchPublishResult timeout = blocking.PublishBatch(
        *timed, Deadline::FromNow(std::chrono::milliseconds(1)));
    ASSERT_FALSE(timeout.ok());
    EXPECT_EQ(timeout.committed_count, 0u);
    EXPECT_EQ(timeout.first_failed_index, 0u);
    EXPECT_EQ(timeout.first_error.code(), StatusCode::kTimeout);
}

TEST_F(RuntimeSpscTest, BatchValidationFailureRollsBackUncommittedBuilders) {
    const size_t journal_size = AllocationJournal::RequiredSize(3, 1);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(journal_memory.get(), journal_size,
                                           3, 1, allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();
    Publisher<RuntimeTestMessage> publisher(allocator_, *channel_, *journal);
    Subscriber<RuntimeTestMessage> subscriber(allocator_, *channel_);

    auto builders = AllocateBatch(publisher, 50, 3);
    ASSERT_TRUE(builders.ok());
    (*builders)[1]->id = 0;
    const ShmHandle invalid = (*builders)[1].handle();
    const ShmHandle unattempted = (*builders)[2].handle();
    const BatchPublishResult result = publisher.PublishBatch(*builders);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.committed_count, 1u);
    EXPECT_EQ(result.first_failed_index, 1u);
    EXPECT_EQ(result.first_error.code(), StatusCode::kInvalidArgument);
    EXPECT_TRUE(result.cleanup_error.ok());
    EXPECT_EQ(allocator_.Inspect(invalid).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(unattempted).status().code(),
              StatusCode::kNotFound);

    auto committed = subscriber.TryPoll();
    ASSERT_TRUE(committed.ok());
    EXPECT_EQ((*committed)->id, 50u);
    EXPECT_TRUE(std::move(*committed).Ack().ok());
    auto reusable = AllocateBatch(publisher, 60, 3);
    ASSERT_TRUE(reusable.ok())
        << "all Journal transactions must be finalized or rolled back";
    for (auto& builder : *reusable) {
        EXPECT_TRUE(publisher.Abort(std::move(builder)).ok());
    }
}

TEST_F(RuntimeSpscTest, MpscBatchNormalPartialFullAndConservation) {
    constexpr uint64_t kCapacity = 64;
    constexpr uint64_t kAllocatorBytes = 2u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto large_allocator = CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes, AllocatorConfig(128));
    ASSERT_TRUE(large_allocator.ok()) << large_allocator.status().ToString();
    auto channel_memory =
        AllocateAligned(MpscChannel::RequiredSize(kCapacity));
    auto mpsc = MpscChannel::Init(channel_memory.get(), kCapacity);
    ASSERT_TRUE(mpsc.ok()) << mpsc.status().ToString();
    MpscChannel::ProducerIdentity identity{
        .owner = ProcessIdentity::Current(),
        .publisher_id = 0xD603,
    };
    Publisher<RuntimeTestMessage> publisher(*large_allocator, *mpsc, identity);
    Subscriber<RuntimeTestMessage> subscriber(*large_allocator, *mpsc);

    auto initial = AllocateBatch(publisher, 1, kCapacity - 2);
    ASSERT_TRUE(initial.ok());
    const BatchPublishResult normal = publisher.PublishBatch(*initial);
    ASSERT_TRUE(normal.ok()) << normal.first_error.ToString();
    EXPECT_EQ(normal.committed_count, kCapacity - 2);

    auto partial = AllocateBatch(publisher, kCapacity - 1, 4);
    ASSERT_TRUE(partial.ok());
    const ShmHandle failed = (*partial)[2].handle();
    const ShmHandle unattempted = (*partial)[3].handle();
    const BatchPublishResult partial_result = publisher.PublishBatch(*partial);
    ASSERT_FALSE(partial_result.ok());
    EXPECT_EQ(partial_result.committed_count, 2u);
    EXPECT_EQ(partial_result.first_failed_index, 2u);
    EXPECT_EQ(partial_result.first_error.code(),
              StatusCode::kResourceExhausted);
    EXPECT_EQ(large_allocator->Inspect(failed).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(large_allocator->Inspect(unattempted).status().code(),
              StatusCode::kNotFound);

    auto consumed = subscriber.TryPollBatch(kCapacity);
    ASSERT_TRUE(consumed.ok()) << consumed.status().ToString();
    ASSERT_EQ(consumed->size(), kCapacity);
    for (size_t i = 0; i < consumed->size(); ++i) {
        EXPECT_EQ((*consumed)[i]->id, i + 1);
    }
    const BatchAckResult acked = consumed->AckAll();
    EXPECT_TRUE(acked.ok()) << acked.first_error.ToString();
    EXPECT_EQ(acked.acked_count, kCapacity);
    EXPECT_TRUE(mpsc->IsEmpty());
}

TEST_F(RuntimeSpscTest, BroadcastBatchPinsAcksAndReportsPartialFull) {
    constexpr uint64_t kCapacity = 4;
    auto broadcast_memory =
        AllocateAligned(BroadcastChannel::RequiredSize(kCapacity));
    auto broadcast = BroadcastChannel::Init(broadcast_memory.get(), kCapacity);
    ASSERT_TRUE(broadcast.ok()) << broadcast.status().ToString();
    auto subscriber_handle = broadcast->RegisterSubscriber(SubscriberId{0});
    ASSERT_TRUE(subscriber_handle.ok());
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  allocator_);
    ASSERT_TRUE(pins.ok()) << pins.status().ToString();
    Publisher<RuntimeTestMessage> publisher(allocator_, *broadcast, *pins);
    Subscriber<RuntimeTestMessage> subscriber(
        allocator_, *broadcast, *subscriber_handle, *pins);

    auto builders = AllocateBatch(publisher, 1, 3);
    ASSERT_TRUE(builders.ok());
    std::vector<ShmHandle> handles;
    for (const auto& builder : *builders) {
        handles.push_back(builder.handle());
    }
    ASSERT_TRUE(publisher.PublishBatch(*builders).ok());
    auto batch = subscriber.TryPollBatch(3);
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    ASSERT_EQ(batch->size(), 3u);
    for (size_t i = 0; i < batch->size(); ++i) {
        EXPECT_EQ((*batch)[i]->id, i + 1);
        EXPECT_EQ(pins->PinCount(handles[i]), 1u);
    }

    EXPECT_TRUE(batch->Ack(2).ok());
    EXPECT_EQ(pins->PinCount(handles[2]), 1u)
        << "deferred out-of-order ACK must retain its Broadcast pin";
    EXPECT_TRUE(batch->Ack(0).ok());
    EXPECT_EQ(pins->PinCount(handles[0]), 0u);
    EXPECT_EQ(pins->PinCount(handles[1]), 1u);
    EXPECT_EQ(pins->PinCount(handles[2]), 1u);
    EXPECT_TRUE(batch->AckAll().ok());
    for (ShmHandle handle : handles) {
        EXPECT_EQ(pins->PinCount(handle), 0u);
        EXPECT_EQ(allocator_.Inspect(handle).status().code(),
                  StatusCode::kNotFound);
    }

    auto fill = AllocateBatch(publisher, 10, kCapacity - 1);
    ASSERT_TRUE(fill.ok());
    ASSERT_TRUE(publisher.PublishBatch(*fill).ok());
    auto partial = AllocateBatch(publisher, 20, 3);
    ASSERT_TRUE(partial.ok());
    const ShmHandle failed = (*partial)[1].handle();
    const ShmHandle unattempted = (*partial)[2].handle();
    const BatchPublishResult partial_result = publisher.PublishBatch(*partial);
    ASSERT_FALSE(partial_result.ok());
    EXPECT_EQ(partial_result.committed_count, 1u);
    EXPECT_EQ(partial_result.first_failed_index, 1u);
    EXPECT_EQ(partial_result.first_error.code(),
              StatusCode::kResourceExhausted);
    EXPECT_EQ(allocator_.Inspect(failed).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(unattempted).status().code(),
              StatusCode::kNotFound);

    {
        auto remaining = subscriber.TryPollBatch(kCapacity);
        ASSERT_TRUE(remaining.ok());
        ASSERT_EQ(remaining->size(), kCapacity);
        EXPECT_EQ((*remaining)[3]->id, 20u);
    }
    EXPECT_EQ(subscriber.TryPoll().status().code(), StatusCode::kWouldBlock);
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


TEST_F(RuntimeSpscTest, ExclusiveSpscHopTransfersLeafWithoutCopyAndReclaimsOnce) {
    auto hop_memory = AllocateAligned(SpscChannel::RequiredSize(kChannelCapacity));
    auto hop = SpscChannel::Init(hop_memory.get(), kChannelCapacity);
    ASSERT_TRUE(hop.ok()) << hop.status().ToString();

    Publisher<RuntimeTestMessage> producer(allocator_, *channel_);
    Subscriber<RuntimeTestMessage> hop_in(allocator_, *channel_);
    Publisher<RuntimeTestMessage> hop_out(allocator_, *hop);
    Subscriber<RuntimeTestMessage> sink(allocator_, *hop);

    auto builder = producer.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 11;
    (*builder)->value = 22;
    const ShmHandle root = builder->handle();
    ASSERT_TRUE(producer.PublishLocal(std::move(*builder)).ok());

    auto borrowed = hop_in.TryPoll();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    auto exclusive = std::move(*borrowed).TakeExclusive();
    ASSERT_TRUE(exclusive.ok()) << exclusive.status().ToString();
    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ(exclusive->handle(), root);
    (*exclusive)->id = 33;
    ASSERT_TRUE(hop_out.PublishLocal(std::move(*exclusive)).ok());

    auto received = sink.TryPoll();
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(received->metadata().payload, root);
    EXPECT_EQ((*received)->id, 33u);
    EXPECT_EQ((*received)->value, 22u);
    ASSERT_TRUE(std::move(*received).Ack().ok());
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
}

TEST_F(RuntimeSpscTest, ExclusiveReleaseWithoutPublishReclaimsGraph) {
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();

    Publisher<RuntimeGraphMessage> publisher(allocator_, *channel_, *journal);
    Subscriber<RuntimeGraphMessage> subscriber(allocator_, *channel_);
    auto builder = publisher.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 0x901;
    AllocationRequest child_request;
    child_request.object_size = 16;
    child_request.type_id = TypeId{44};
    child_request.schema = {.short_id = 3, .layout_version = 1};
    child_request.alignment = 8;
    auto child_build = builder->AllocateChild(child_request);
    ASSERT_TRUE(child_build.ok()) << child_build.status().ToString();
    std::memset(child_build->data, 0x5A, child_build->object_size);
    SetGraphChild(**builder, child_build->handle);
    const ShmHandle root = builder->handle();
    const ShmHandle child = child_build->handle;
    const auto before = allocator_.local_cache_stats();
    ASSERT_TRUE(publisher.PublishLocal(std::move(*builder)).ok());

    auto borrowed = subscriber.TryPoll();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    auto exclusive = std::move(*borrowed).TakeExclusive();
    ASSERT_TRUE(exclusive.ok()) << exclusive.status().ToString();
    EXPECT_TRUE(channel_->IsEmpty());
    EXPECT_EQ(allocator_.Inspect(root)->state, ObjectState::kPublished);
    EXPECT_EQ(allocator_.Inspect(child)->state, ObjectState::kPublished);
    EXPECT_EQ(allocator_.local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims);

    ASSERT_TRUE(std::move(*exclusive).Release().ok());
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims + 1);
}

TEST_F(RuntimeSpscTest, ExclusiveHopTransfersChildHandleWithoutPayloadCopy) {
    auto hop_memory = AllocateAligned(SpscChannel::RequiredSize(kChannelCapacity));
    auto hop = SpscChannel::Init(hop_memory.get(), kChannelCapacity);
    ASSERT_TRUE(hop.ok()) << hop.status().ToString();
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();

    Publisher<RuntimeGraphMessage> producer(allocator_, *channel_, *journal);
    Subscriber<RuntimeGraphMessage> hop_in(allocator_, *channel_);
    Publisher<RuntimeGraphMessage> hop_out(allocator_, *hop, *journal);
    Subscriber<RuntimeGraphMessage> sink(allocator_, *hop);

    auto builder = producer.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 0x902;
    AllocationRequest child_request;
    child_request.object_size = 16;
    child_request.type_id = TypeId{44};
    child_request.schema = {.short_id = 3, .layout_version = 1};
    child_request.alignment = 8;
    auto child_build = builder->AllocateChild(child_request);
    ASSERT_TRUE(child_build.ok()) << child_build.status().ToString();
    std::memset(child_build->data, 0x5A, child_build->object_size);
    SetGraphChild(**builder, child_build->handle);
    const ShmHandle root = builder->handle();
    const ShmHandle child = child_build->handle;
    const auto before = allocator_.local_cache_stats();
    ASSERT_TRUE(producer.PublishLocal(std::move(*builder)).ok());

    auto borrowed = hop_in.TryPoll();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    auto exclusive = std::move(*borrowed).TakeExclusive();
    ASSERT_TRUE(exclusive.ok()) << exclusive.status().ToString();
    (*exclusive)->id = 0x903;
    ASSERT_TRUE(hop_out.PublishLocal(std::move(*exclusive)).ok());
    EXPECT_EQ(allocator_.local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims);

    auto received = sink.TryPoll();
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(received->metadata().payload, root);
    EXPECT_EQ((*received)->id, 0x903u);
    EXPECT_EQ((*received)->child_offset, child.offset);
    EXPECT_EQ((*received)->child_generation, child.generation);
    EXPECT_EQ((*received)->child_region_id, child.region_id);
    auto child_view = allocator_.Inspect(child);
    ASSERT_TRUE(child_view.ok());
    EXPECT_EQ(child_view->state, ObjectState::kPublished);
    const auto* bytes = static_cast<const uint8_t*>(child_view->data);
    for (size_t i = 0; i < 16; ++i) {
        EXPECT_EQ(bytes[i], 0x5A) << i;
    }
    ASSERT_TRUE(std::move(*received).Ack().ok());
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims + 1);

    auto reused = producer.Allocate();
    ASSERT_TRUE(reused.ok()) << reused.status().ToString();
    EXPECT_TRUE(producer.Abort(std::move(*reused)).ok());
}

TEST_F(RuntimeSpscTest, ExclusiveHopTwoHopsThenFinalAck) {
    auto mid_memory = AllocateAligned(SpscChannel::RequiredSize(kChannelCapacity));
    auto mid = SpscChannel::Init(mid_memory.get(), kChannelCapacity);
    ASSERT_TRUE(mid.ok()) << mid.status().ToString();
    auto out_memory = AllocateAligned(SpscChannel::RequiredSize(kChannelCapacity));
    auto out = SpscChannel::Init(out_memory.get(), kChannelCapacity);
    ASSERT_TRUE(out.ok()) << out.status().ToString();
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, allocator_);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();

    Publisher<RuntimeGraphMessage> producer(allocator_, *channel_, *journal);
    Subscriber<RuntimeGraphMessage> first_in(allocator_, *channel_);
    Publisher<RuntimeGraphMessage> first_out(allocator_, *mid, *journal);
    Subscriber<RuntimeGraphMessage> second_in(allocator_, *mid);
    Publisher<RuntimeGraphMessage> second_out(allocator_, *out, *journal);
    Subscriber<RuntimeGraphMessage> sink(allocator_, *out);

    auto builder = producer.Allocate();
    ASSERT_TRUE(builder.ok());
    (*builder)->id = 0xA01;
    AllocationRequest child_request;
    child_request.object_size = 16;
    child_request.type_id = TypeId{44};
    child_request.schema = {.short_id = 3, .layout_version = 1};
    auto child_build = builder->AllocateChild(child_request);
    ASSERT_TRUE(child_build.ok());
    SetGraphChild(**builder, child_build->handle);
    const ShmHandle root = builder->handle();
    const ShmHandle child = child_build->handle;
    const auto before = allocator_.local_cache_stats();
    ASSERT_TRUE(producer.PublishLocal(std::move(*builder)).ok());

    Publisher<RuntimeGraphMessage>* hops[] = {&first_out, &second_out};
    Subscriber<RuntimeGraphMessage>* inputs[] = {&first_in, &second_in};
    for (size_t i = 0; i < 2; ++i) {
        auto borrowed = inputs[i]->TryPoll();
        ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
        auto exclusive = std::move(*borrowed).TakeExclusive();
        ASSERT_TRUE(exclusive.ok()) << exclusive.status().ToString();
        (*exclusive)->id = 0xA02 + i;
        ASSERT_TRUE(hops[i]->PublishLocal(std::move(*exclusive)).ok());
        EXPECT_EQ(allocator_.local_cache_stats().published_graph_reclaims,
                  before.published_graph_reclaims);
    }

    auto received = sink.TryPoll();
    ASSERT_TRUE(received.ok());
    EXPECT_EQ((*received)->id, 0xA03u);
    EXPECT_EQ(received->metadata().payload, root);
    EXPECT_EQ((*received)->child_offset, child.offset);
    ASSERT_TRUE(std::move(*received).Ack().ok());
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims + 1);
}

TEST(RuntimeGeneratedGraphLifecycleTest,
     ExclusiveSpscHopKeepsChildUntilFinalAck) {
    constexpr uint64_t kAllocatorBytes = 1u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto allocator = CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes,
        GeneratedGraphAllocatorConfig());
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    auto in_memory = AllocateAligned(SpscChannel::RequiredSize(2));
    auto in_channel = SpscChannel::Init(in_memory.get(), 2);
    ASSERT_TRUE(in_channel.ok());
    auto out_memory = AllocateAligned(SpscChannel::RequiredSize(2));
    auto out_channel = SpscChannel::Init(out_memory.get(), 2);
    ASSERT_TRUE(out_channel.ok());
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, *allocator);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();

    Publisher<golden::Telemetry> producer(*allocator, *in_channel, *journal);
    Subscriber<golden::Telemetry> hop_in(*allocator, *in_channel);
    Publisher<golden::Telemetry> hop_out(*allocator, *out_channel, *journal);
    Subscriber<golden::Telemetry> sink(*allocator, *out_channel);

    auto graph = AllocateGeneratedGraph(producer, 0xB01);
    ASSERT_TRUE(graph.ok()) << graph.status().ToString();
    const ShmHandle root = graph->root.handle();
    const ShmHandle child = graph->child;
    const auto before = allocator->local_cache_stats();
    ASSERT_TRUE(producer.PublishLocal(std::move(graph->root)).ok());

    auto borrowed = hop_in.TryPoll();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    auto exclusive = std::move(*borrowed).TakeExclusive();
    ASSERT_TRUE(exclusive.ok()) << exclusive.status().ToString();
    golden::TelemetryAccessor accessor(*exclusive->get());
    EXPECT_EQ(accessor.payload().offset, child.offset);
    EXPECT_EQ(accessor.payload().generation, child.generation);
    EXPECT_EQ(accessor.payload().region_id, child.region_id);
    auto child_view = allocator->Inspect(child);
    ASSERT_TRUE(child_view.ok());
    const auto* bytes = static_cast<const uint8_t*>(child_view->data);
    EXPECT_EQ(bytes[0], 0x5A);
    ASSERT_TRUE(hop_out.PublishLocal(std::move(*exclusive)).ok());
    EXPECT_EQ(allocator->local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims);

    auto received = sink.TryPoll();
    ASSERT_TRUE(received.ok());
    EXPECT_EQ(received->metadata().payload, root);
    golden::TelemetryAccessor received_accessor(**received);
    EXPECT_EQ(received_accessor.payload().offset, child.offset);
    ASSERT_TRUE(std::move(*received).Ack().ok());
    EXPECT_EQ(allocator->Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator->Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator->local_cache_stats().published_graph_reclaims,
              before.published_graph_reclaims + 1);
}

TEST(RuntimeGeneratedGraphLifecycleTest,
     TakeExclusiveRejectsPinTableAndBroadcast) {
    constexpr uint64_t kAllocatorBytes = 1u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto allocator = CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes,
        GeneratedGraphAllocatorConfig());
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    auto spsc_memory = AllocateAligned(SpscChannel::RequiredSize(2));
    auto spsc = SpscChannel::Init(spsc_memory.get(), 2);
    ASSERT_TRUE(spsc.ok());
    auto broadcast_memory = AllocateAligned(BroadcastChannel::RequiredSize(2));
    auto broadcast = BroadcastChannel::Init(broadcast_memory.get(), 2);
    ASSERT_TRUE(broadcast.ok());
    auto subscriber_handle = broadcast->RegisterSubscriber(SubscriberId{0});
    ASSERT_TRUE(subscriber_handle.ok());
    const size_t journal_size = AllocationJournal::RequiredSize(1, 2);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = AllocationJournal::Init(
        journal_memory.get(), journal_size, 1, 2, *allocator);
    ASSERT_TRUE(journal.ok());
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = ShmPinTable::Init(pin_memory.get(), ShmPinTable::RequiredSize(),
                                  *allocator);
    ASSERT_TRUE(pins.ok());

    Publisher<golden::Telemetry> spsc_publisher(
        *allocator, *spsc, *journal);
    Subscriber<golden::Telemetry> pinned(
        *allocator, *spsc, &*pins, ProcessIdentity::Current());
    auto graph = AllocateGeneratedGraph(spsc_publisher, 0xC01);
    ASSERT_TRUE(graph.ok()) << graph.status().ToString();
    ASSERT_TRUE(spsc_publisher.PublishLocal(std::move(graph->root)).ok());
    auto pinned_borrow = pinned.TryPoll();
    ASSERT_TRUE(pinned_borrow.ok()) << pinned_borrow.status().ToString();
    EXPECT_EQ(std::move(*pinned_borrow).TakeExclusive().status().code(),
              StatusCode::kUnsupported);

    Publisher<golden::Telemetry> broadcast_publisher(
        *allocator, *broadcast, *pins, *journal);
    Subscriber<golden::Telemetry> broadcast_subscriber(
        *allocator, *broadcast, *subscriber_handle, *pins);
    auto broadcast_graph = AllocateGeneratedGraph(broadcast_publisher, 0xC02);
    ASSERT_TRUE(broadcast_graph.ok()) << broadcast_graph.status().ToString();
    ASSERT_TRUE(broadcast_publisher.PublishLocal(
                    std::move(broadcast_graph->root)).ok());
    auto borrowed = broadcast_subscriber.TryPoll();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    EXPECT_EQ(std::move(*borrowed).TakeExclusive().status().code(),
              StatusCode::kUnsupported);
}

}  // namespace
}  // namespace mino
