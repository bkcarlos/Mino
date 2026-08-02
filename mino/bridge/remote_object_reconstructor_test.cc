// Copyright 2026 The Mino Authors

#include "mino/bridge/remote_object_reconstructor.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "mino/bridge/schema_negotiator.h"
#include "mino/schema/canonical.h"
#include "mino/schema/compiler.h"
#include "mino/schema/dynamic_value.h"

namespace mino::bridge {
namespace {

struct AlignedDeleter {
    void operator()(std::byte* pointer) const noexcept {
        ::operator delete[](pointer, std::align_val_t(64));
    }
};
using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(size_t bytes) {
    AlignedBytes memory(new (std::align_val_t(64)) std::byte[bytes]);
    std::memset(memory.get(), 0, bytes);
    return memory;
}

ClassTableConfig AllocatorConfig() {
    ClassTableConfig config;
    config.classes = {
        {.slot_size = 64, .slot_count = 16},
        {.slot_size = 128, .slot_count = 16},
        {.slot_size = 256, .slot_count = 16},
        {.slot_size = 512, .slot_count = 8},
    };
    return config;
}

class FixedBindingResolver final : public RemoteObjectBindingResolver {
public:
    Result<RemoteObjectBinding> Resolve(
        TopicId, const schema::SchemaIdentity& identity) override {
        ++calls;
        resolved_identity = identity;
        return binding;
    }

    RemoteObjectBinding binding;
    std::optional<schema::SchemaIdentity> resolved_identity;
    size_t calls = 0;
};

class CapturingTarget final : public DynamicPublicationTarget {
public:
    explicit CapturingTarget(SpscDynamicPublicationTarget& target) noexcept
        : target_(&target) {}

    Status Publish(schema::PreparedDynamicObject&& object,
                   const DynamicPublicationMetadata& metadata) override {
        ++calls;
        last_root = object.root_handle();
        return target_->Publish(std::move(object), metadata);
    }

    SpscDynamicPublicationTarget* target_;
    ShmHandle last_root;
    size_t calls = 0;
};

void CompleteFinalizeFromPersistenceHook(
    AllocationJournal::PersistencePoint point, uint64_t,
    void* context) noexcept {
    if (point != AllocationJournal::PersistencePoint::kFinalizingTagged) return;
    auto* journal = static_cast<AllocationJournal*>(context);
    journal->SetPersistenceHook(nullptr, nullptr);
    (void)journal->RecoverOrphans();
}

class RemoteObjectReconstructorTest : public ::testing::Test {
protected:
    static constexpr uint32_t kSchemaRef = 1;
    static constexpr uint32_t kTopicId = 7;
    static constexpr size_t kAllocatorBytes = 1u << 20;
    static constexpr uint32_t kJournalTransactions = 8;
    static constexpr uint32_t kJournalHandles = 16;
    static constexpr uint64_t kChannelCapacity = 2;

    void SetUp() override {
        auto compiled = schema::SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package bridge_reconstruction;
message Payload {
  uint32 value = 1;
}
)idl");
        ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
        auto registered = registry_.RegisterCompiled(*compiled);
        ASSERT_TRUE(registered.ok()) << registered.status().ToString();
        ASSERT_EQ(registered->size(), 1u);
        descriptors_ = std::move(*registered);
        descriptor_ = descriptors_.front();

        auto layout = schema::LayoutPlanner::Plan(*descriptor_, descriptors_);
        ASSERT_TRUE(layout.ok()) << layout.status().ToString();
        layout_.emplace(std::move(*layout));

        WireFrame announcement;
        announcement.header.frame_type = FrameType::kSchemaAnnounce;
        announcement.header.flags = FlagValue(FrameFlag::kControlFrame);
        auto announcement_payload = SchemaControlCodec::EncodeAnnouncement(
            SchemaAnnouncement(kSchemaRef, descriptor_->identity()));
        ASSERT_TRUE(announcement_payload.ok())
            << announcement_payload.status().ToString();
        announcement.payload = std::move(*announcement_payload);
        auto negotiated = negotiator_.HandleControlFrame(
            std::move(announcement), 1);
        ASSERT_TRUE(negotiated.ok()) << negotiated.status().ToString();

        allocator_memory_ = AllocateAligned(kAllocatorBytes);
        auto allocator = CentralSlabAllocator::Create(
            allocator_memory_.get(), kAllocatorBytes, AllocatorConfig());
        ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();
        allocator_ = *allocator;

        journal_size_ = AllocationJournal::RequiredSize(
            kJournalTransactions, kJournalHandles);
        journal_memory_ = AllocateAligned(journal_size_);
        auto journal = AllocationJournal::Init(
            journal_memory_.get(), journal_size_, kJournalTransactions,
            kJournalHandles, allocator_);
        ASSERT_TRUE(journal.ok()) << journal.status().ToString();
        journal_.emplace(*journal);

        channel_memory_ = AllocateAligned(
            SpscChannel::RequiredSize(kChannelCapacity));
        auto channel = SpscChannel::Init(channel_memory_.get(),
                                         kChannelCapacity);
        ASSERT_TRUE(channel.ok()) << channel.status().ToString();
        channel_.emplace(*channel);
        spsc_target_ = std::make_unique<SpscDynamicPublicationTarget>(
            *channel_, 99);
        capturing_target_ =
            std::make_unique<CapturingTarget>(*spsc_target_);

        topic_.topic_id = TopicId{kTopicId};
        topic_.name = "bridge/reconstruction";
        topic_.schema = descriptor_->identity();
        topic_.state = registry::TopicState::kActive;
        topic_.channel_kind = registry::ChannelKind::kSpsc;
        topic_.capacity = static_cast<uint32_t>(kChannelCapacity);

        resolver_.binding = RemoteObjectBinding{
            .topic = &topic_,
            .schema_handle = descriptor_,
            .layout_plan = &*layout_,
            .descriptor_closure = descriptors_,
            .allocator = &allocator_,
            .allocation_journal = &*journal_,
            .type_id = TypeId{
                static_cast<uint32_t>(descriptor_->identity().short_id())},
            .publication_target = capturing_target_.get(),
        };
    }

    WireFrame ValidFrame(uint32_t value = 42) {
        schema::DynamicMessage message;
        EXPECT_TRUE(message.SetField(
            1, schema::DynamicValue::Unsigned(value)).ok());
        auto encoded = schema::CanonicalWireCodec::Encode(
            *descriptor_, message, descriptors_);
        EXPECT_TRUE(encoded.ok()) << encoded.status().ToString();

        WireFrame frame;
        frame.header.frame_type = FrameType::kData;
        frame.header.topic_id = kTopicId;
        frame.header.msg_type =
            static_cast<uint32_t>(descriptor_->identity().short_id());
        frame.header.connection_schema_ref = kSchemaRef;
        frame.header.schema_version =
            descriptor_->identity().schema_version();
        frame.header.layout_version =
            descriptor_->identity().layout_version();
        frame.header.timestamp_ns = 123456;
        if (encoded.ok()) frame.payload = std::move(*encoded);
        return frame;
    }

    RemoteObjectReconstructor Reconstructor() {
        return RemoteObjectReconstructor(negotiator_, resolver_);
    }

    schema::SchemaRegistry registry_;
    SchemaNegotiator negotiator_{&registry_, nullptr, nullptr};
    std::vector<schema::SchemaHandle> descriptors_;
    schema::SchemaHandle descriptor_;
    std::optional<schema::LayoutPlan> layout_;
    registry::TopicMetadata topic_;

    AlignedBytes allocator_memory_;
    CentralSlabAllocator allocator_;
    size_t journal_size_ = 0;
    AlignedBytes journal_memory_;
    std::optional<AllocationJournal> journal_;
    AlignedBytes channel_memory_;
    std::optional<SpscChannel> channel_;
    std::unique_ptr<SpscDynamicPublicationTarget> spsc_target_;
    std::unique_ptr<CapturingTarget> capturing_target_;
    FixedBindingResolver resolver_;
};

TEST_F(RemoteObjectReconstructorTest, ReconstructsIntoRealSpscAndCanPoll) {
    auto reconstructor = Reconstructor();
    const Status status = reconstructor.DecodeValidatePublish(ValidFrame());
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);

    auto borrow = channel_->Poll();
    ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
    EXPECT_EQ(borrow->slot()->msg_type,
              static_cast<uint32_t>(descriptor_->identity().short_id()));
    EXPECT_EQ(borrow->slot()->schema_short_id,
              descriptor_->identity().short_id());
    EXPECT_EQ(borrow->slot()->schema_version,
              descriptor_->identity().schema_version());
    EXPECT_EQ(borrow->slot()->schema_layout_version,
              descriptor_->identity().layout_version());
    EXPECT_EQ(borrow->slot()->timestamp_ns, 123456u);
    EXPECT_EQ(borrow->slot()->payload, capturing_target_->last_root);
    EXPECT_EQ(borrow->slot()->payload_len, layout_->object_size());

    auto slab = allocator_.Inspect(borrow->slot()->payload);
    ASSERT_TRUE(slab.ok()) << slab.status().ToString();
    EXPECT_EQ(slab->state, ObjectState::kPublished);
    const ShmHandle root = borrow->slot()->payload;
    ASSERT_TRUE(std::move(*borrow).Ack().ok());
    ASSERT_TRUE(allocator_.Retire(root).ok());
    ASSERT_TRUE(allocator_.Reclaim(root).ok());
}

TEST_F(RemoteObjectReconstructorTest,
       FinalizeFailureAfterChannelCommitStillReportsPublishedSuccess) {
    journal_->SetPersistenceHook(&CompleteFinalizeFromPersistenceHook,
                                 &*journal_);
    auto reconstructor = Reconstructor();
    const Status status = reconstructor.DecodeValidatePublish(ValidFrame());
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(spsc_target_->journal_cleanup_debt_count(), 1u);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);

    auto borrow = channel_->Poll();
    ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
    const ShmHandle root = borrow->slot()->payload;
    EXPECT_EQ(root, capturing_target_->last_root);
    ASSERT_TRUE(std::move(*borrow).Ack().ok());
    ASSERT_TRUE(allocator_.Retire(root).ok());
    ASSERT_TRUE(allocator_.Reclaim(root).ok());
}

TEST_F(RemoteObjectReconstructorTest,
       ReconstructsExplicitlyAcceptedPreviousSchemaVersion) {
    auto previous_compiled = schema::SchemaCompiler::Compile(R"idl(
option schema_version = "0.9";
package bridge_reconstruction;
message Payload {
  uint32 value = 1;
}
)idl");
    ASSERT_TRUE(previous_compiled.ok())
        << previous_compiled.status().ToString();
    auto previous_registered = registry_.RegisterCompiled(*previous_compiled);
    ASSERT_TRUE(previous_registered.ok())
        << previous_registered.status().ToString();
    ASSERT_EQ(previous_registered->size(), 1u);
    std::vector<schema::SchemaHandle> previous_descriptors =
        std::move(*previous_registered);
    const schema::SchemaHandle previous = previous_descriptors.front();
    auto previous_layout =
        schema::LayoutPlanner::Plan(*previous, previous_descriptors);
    ASSERT_TRUE(previous_layout.ok()) << previous_layout.status().ToString();

    constexpr uint32_t kPreviousRef = 2;
    WireFrame announcement;
    announcement.header.frame_type = FrameType::kSchemaAnnounce;
    announcement.header.flags = FlagValue(FrameFlag::kControlFrame);
    auto announcement_payload = SchemaControlCodec::EncodeAnnouncement(
        SchemaAnnouncement(kPreviousRef, previous->identity()));
    ASSERT_TRUE(announcement_payload.ok());
    announcement.payload = std::move(*announcement_payload);
    ASSERT_TRUE(negotiator_.HandleControlFrame(std::move(announcement), 2).ok());

    topic_.accepted_schemas.push_back(previous->identity());
    resolver_.binding.schema_handle = previous;
    resolver_.binding.layout_plan = &*previous_layout;
    resolver_.binding.descriptor_closure = previous_descriptors;
    resolver_.binding.type_id = TypeId{
        static_cast<uint32_t>(previous->identity().short_id())};

    schema::DynamicMessage message;
    ASSERT_TRUE(message.SetField(
        1, schema::DynamicValue::Unsigned(84)).ok());
    auto payload = schema::CanonicalWireCodec::Encode(
        *previous, message, previous_descriptors);
    ASSERT_TRUE(payload.ok()) << payload.status().ToString();
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.topic_id = kTopicId;
    frame.header.msg_type =
        static_cast<uint32_t>(previous->identity().short_id());
    frame.header.connection_schema_ref = kPreviousRef;
    frame.header.schema_version = previous->identity().schema_version();
    frame.header.layout_version = previous->identity().layout_version();
    frame.header.timestamp_ns = 654321;
    frame.payload = std::move(*payload);

    auto reconstructor = Reconstructor();
    const Status status = reconstructor.DecodeValidatePublish(frame);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(resolver_.resolved_identity.has_value());
    EXPECT_EQ(resolver_.resolved_identity->canonical_digest(),
              previous->identity().canonical_digest());
    EXPECT_EQ(resolver_.resolved_identity->schema_version(),
              previous->identity().schema_version());
    EXPECT_EQ(resolver_.resolved_identity->layout_version(),
              previous->identity().layout_version());
    auto borrow = channel_->Poll();
    ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
    EXPECT_EQ(borrow->slot()->schema_short_id,
              previous->identity().short_id());
    EXPECT_EQ(borrow->slot()->schema_version,
              previous->identity().schema_version());
    const ShmHandle root = borrow->slot()->payload;
    ASSERT_TRUE(std::move(*borrow).Ack().ok());
    ASSERT_TRUE(allocator_.Retire(root).ok());
    ASSERT_TRUE(allocator_.Reclaim(root).ok());
}

TEST_F(RemoteObjectReconstructorTest,
       MalformedPayloadIsRejectedBeforeSlabAllocation) {
    WireFrame frame = ValidFrame();
    frame.payload = {std::byte{0x08}, std::byte{0x81}, std::byte{0x00}};

    auto reconstructor = Reconstructor();
    const Status status = reconstructor.DecodeValidatePublish(frame);
    EXPECT_EQ(status.code(), StatusCode::kCorruption);
    EXPECT_EQ(capturing_target_->calls, 0u);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
    for (uint32_t i = 0; i < allocator_.total_slot_count(); ++i) {
        EXPECT_EQ(allocator_.AuthoritativeGenerationForRecovery(i), 0u);
    }
    EXPECT_TRUE(channel_->IsEmpty());
}

TEST_F(RemoteObjectReconstructorTest,
       RejectsInactiveOrMismatchedTopicBeforeDecode) {
    auto reconstructor = Reconstructor();
    topic_.state = registry::TopicState::kDraining;
    EXPECT_EQ(reconstructor.DecodeValidatePublish(ValidFrame()).code(),
              StatusCode::kUnavailable);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);

    topic_.state = registry::TopicState::kActive;
    topic_.topic_id = TopicId{kTopicId + 1};
    EXPECT_EQ(reconstructor.DecodeValidatePublish(ValidFrame()).code(),
              StatusCode::kSchemaMismatch);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
}

TEST_F(RemoteObjectReconstructorTest,
       RejectsTopicSchemaOrFrameVersionMismatchBeforeAllocation) {
    auto reconstructor = Reconstructor();
    schema::CanonicalDigest other_digest =
        descriptor_->identity().canonical_digest();
    other_digest[0] ^= std::byte{1};
    topic_.schema = schema::SchemaIdentity(
        schema::DigestShortId(other_digest), other_digest,
        descriptor_->identity().schema_version(),
        descriptor_->identity().layout_version());
    EXPECT_EQ(reconstructor.DecodeValidatePublish(ValidFrame()).code(),
              StatusCode::kSchemaMismatch);

    topic_.schema = descriptor_->identity();
    WireFrame wrong_version = ValidFrame();
    ++wrong_version.header.schema_version;
    EXPECT_EQ(reconstructor.DecodeValidatePublish(wrong_version).code(),
              StatusCode::kSchemaMismatch);
    EXPECT_EQ(capturing_target_->calls, 0u);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
}

TEST_F(RemoteObjectReconstructorTest, QueueFullRollsBackPreparedGraph) {
    for (uint32_t i = 0; i < kChannelCapacity; ++i) {
        auto reservation = channel_->TryReserve();
        ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
        reservation->slot()->msg_type = i + 1;
        ASSERT_TRUE(std::move(*reservation).Commit().ok());
    }
    ASSERT_TRUE(channel_->IsFull());

    auto reconstructor = Reconstructor();
    const Status status = reconstructor.DecodeValidatePublish(ValidFrame());
    EXPECT_EQ(status.code(), StatusCode::kResourceExhausted);
    ASSERT_FALSE(capturing_target_->last_root.IsNull());
    EXPECT_EQ(allocator_.Inspect(capturing_target_->last_root).status().code(),
              StatusCode::kNotFound);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
    EXPECT_EQ(channel_->Size(), kChannelCapacity);

    for (uint32_t i = 0; i < kChannelCapacity; ++i) {
        auto borrow = channel_->Poll();
        ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
        ASSERT_TRUE(std::move(*borrow).Ack().ok());
    }
}

}  // namespace
}  // namespace mino::bridge
