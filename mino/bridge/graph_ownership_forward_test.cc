// Copyright 2026 The Mino Authors

#include "mino/bridge/graph_ownership_forward.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "mino/schema/canonical.h"
#include "mino/schema/compiler.h"
#include "mino/schema/dynamic_value.h"
#include "mino/schema/registry.h"

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
        {.slot_size = 2048, .slot_count = 4},
    };
    return config;
}

class GraphOwnershipForwardTest : public ::testing::Test {
protected:
    static constexpr size_t kAllocatorBytes = 4u << 20;
    static constexpr uint32_t kJournalTransactions = 8;
    static constexpr uint32_t kJournalHandles = 16;

    void SetUp() override {
        auto compiled = schema::SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package ownership_forward;
message Sample {
  uint64 sample_id = 1;
  bytes payload = 2 [max_bytes = 4096];
}
)idl");
        ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
        auto registered = registry_.RegisterCompiled(*compiled);
        ASSERT_TRUE(registered.ok()) << registered.status().ToString();
        ASSERT_EQ(registered->size(), 1u);
        descriptors_ = std::move(*registered);
        descriptor_ = descriptors_.front();

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

        pin_size_ = ShmPinTable::RequiredSize();
        pin_memory_ = AllocateAligned(pin_size_);
        auto pins =
            ShmPinTable::Init(pin_memory_.get(), pin_size_, allocator_);
        ASSERT_TRUE(pins.ok()) << pins.status().ToString();
        pins_.emplace(*pins);

        auto forwarder =
            GraphOwnershipForwarder::Create(descriptor_, descriptors_);
        ASSERT_TRUE(forwarder.ok()) << forwarder.status().ToString();
        forwarder_.emplace(std::move(*forwarder));
    }

    schema::SchemaRegistry registry_;
    std::vector<schema::DynamicSchemaHandle> descriptors_;
    schema::DynamicSchemaHandle descriptor_;
    AlignedBytes allocator_memory_;
    CentralSlabAllocator allocator_{};
    size_t journal_size_ = 0;
    AlignedBytes journal_memory_;
    std::optional<AllocationJournal> journal_;
    size_t pin_size_ = 0;
    AlignedBytes pin_memory_;
    std::optional<ShmPinTable> pins_;
    std::optional<GraphOwnershipForwarder> forwarder_;
};

TEST_F(GraphOwnershipForwardTest,
       EncodeMessageUsesBytesViewWithoutSemanticAssign) {
    const std::string blob(1024, 'Z');
    schema::DynamicMessage message;
    ASSERT_TRUE(message.SetField(1, schema::DynamicValue::Unsigned(7)).ok());
    ASSERT_TRUE(message
                    .SetField(2, schema::DynamicValue::BytesView(std::as_bytes(
                                     std::span(blob.data(), blob.size()))))
                    .ok());

    std::vector<std::byte> wire;
    ASSERT_TRUE(forwarder_->EncodeMessage(message, wire).ok());
    ASSERT_FALSE(wire.empty());
    EXPECT_EQ(forwarder_->census().encode_calls, 1u);
    EXPECT_EQ(forwarder_->census().encode_payload_bytes_copied, blob.size());
    EXPECT_EQ(forwarder_->census().semantic_payload_assigns_avoided, 1u);

    schema::DynamicMessage owned;
    ASSERT_TRUE(owned.SetField(1, schema::DynamicValue::Unsigned(7)).ok());
    auto owned_bytes = schema::DynamicValue::Bytes(
        std::as_bytes(std::span(blob.data(), blob.size())));
    ASSERT_TRUE(owned_bytes.ok());
    ASSERT_TRUE(owned.SetField(2, std::move(*owned_bytes)).ok());
    auto expected =
        schema::CanonicalWireCodec::Encode(*descriptor_, owned, descriptors_);
    ASSERT_TRUE(expected.ok()) << expected.status().ToString();
    EXPECT_EQ(wire, *expected);
}

TEST_F(GraphOwnershipForwardTest, ReconstructRoundTripPreservesPayload) {
    const std::string blob(768, 'B');
    schema::DynamicMessage message;
    ASSERT_TRUE(message.SetField(1, schema::DynamicValue::Unsigned(3)).ok());
    auto owned_bytes = schema::DynamicValue::Bytes(
        std::as_bytes(std::span(blob.data(), blob.size())));
    ASSERT_TRUE(owned_bytes.ok());
    ASSERT_TRUE(message.SetField(2, std::move(*owned_bytes)).ok());
    auto encoded =
        schema::CanonicalWireCodec::Encode(*descriptor_, message, descriptors_);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto layout = schema::LayoutPlanner::Plan(*descriptor_, descriptors_);
    ASSERT_TRUE(layout.ok());
    auto object = forwarder_->ReconstructToObject(
        *encoded, allocator_, *journal_, *pins_,
        TypeId{static_cast<uint32_t>(descriptor_->identity().short_id())});
    ASSERT_TRUE(object.ok()) << object.status().ToString();

    EXPECT_EQ(forwarder_->census().reconstruct_calls, 1u);
    EXPECT_EQ(forwarder_->census().reconstruct_payload_bytes_copied,
              blob.size());
    EXPECT_EQ(forwarder_->census().semantic_payload_assigns_avoided, 1u);

    auto pin = object->Pin();
    ASSERT_TRUE(pin.ok()) << pin.status().ToString();
    schema::LayoutPlan layout_copy = *layout;
    auto view = schema::DynamicView::Create(
        descriptor_, std::move(layout_copy), object->root_handle(), allocator_,
        std::move(*pin), descriptors_);
    ASSERT_TRUE(view.ok()) << view.status().ToString();
    auto borrowed = view->ToDynamicMessage(/*borrow_bytes=*/true);
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    const schema::DynamicValue* payload = borrowed->FindField(2);
    ASSERT_NE(payload, nullptr);
    ASSERT_NE(payload->bytes_view(), nullptr);
    EXPECT_EQ(payload->bytes_view()->value.size(), blob.size());
    EXPECT_EQ(std::memcmp(payload->bytes_view()->value.data(), blob.data(),
                          blob.size()),
              0);

    const schema::DynamicValue* id = borrowed->FindField(1);
    ASSERT_NE(id, nullptr);
    ASSERT_NE(id->unsigned_integer(), nullptr);
    EXPECT_EQ(id->unsigned_integer()->value, 3u);
}

TEST_F(GraphOwnershipForwardTest, BorrowDecodeThenPreparedPublish) {
    const std::string blob(512, 'C');
    schema::DynamicMessage message;
    ASSERT_TRUE(message.SetField(1, schema::DynamicValue::Unsigned(11)).ok());
    auto owned_bytes = schema::DynamicValue::Bytes(
        std::as_bytes(std::span(blob.data(), blob.size())));
    ASSERT_TRUE(owned_bytes.ok());
    ASSERT_TRUE(message.SetField(2, std::move(*owned_bytes)).ok());
    auto encoded =
        schema::CanonicalWireCodec::Encode(*descriptor_, message, descriptors_);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    auto prepared = forwarder_->ReconstructToPrepared(
        *encoded, allocator_, *journal_,
        TypeId{static_cast<uint32_t>(descriptor_->identity().short_id())});
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    EXPECT_FALSE(prepared->root_handle().IsNull());
    ASSERT_TRUE(prepared->Rollback().ok());
}

TEST_F(GraphOwnershipForwardTest, RegistrationHookNoopsWithoutProvider) {
    auto registered =
        forwarder_->TryRegisterPayload(allocator_memory_.get(), 64);
    ASSERT_TRUE(registered.ok()) << registered.status().ToString();
    EXPECT_FALSE(registered->has_value());
    EXPECT_EQ(forwarder_->census().registration_attempts, 1u);
    EXPECT_EQ(forwarder_->census().registration_successes, 0u);
}

}  // namespace
}  // namespace mino::bridge
