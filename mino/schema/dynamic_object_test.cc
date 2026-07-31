// Copyright 2026 The Mino Authors

#include "mino/schema/dynamic_object.h"

#include <gtest/gtest.h>

#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "mino/schema/compiler.h"
#include "mino/schema/wire.h"

namespace mino::schema {
namespace {

struct AlignedDeleter {
    void operator()(std::byte* pointer) const {
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
        {.slot_size = 64, .slot_count = 32},
        {.slot_size = 128, .slot_count = 32},
        {.slot_size = 256, .slot_count = 32},
        {.slot_size = 512, .slot_count = 32},
        {.slot_size = 1024, .slot_count = 16},
        {.slot_size = 2048, .slot_count = 16},
    };
    return config;
}

class DynamicObjectTest : public ::testing::Test {
protected:
    static constexpr size_t kAllocatorBytes = 4u << 20;
    static constexpr uint32_t kJournalTransactions = 8;
    static constexpr uint32_t kJournalHandles = 128;

    void SetUp() override {
        CompileOptions options;
        options.allow_implicit_schema_version = true;
        auto compiled = SchemaCompiler::Compile(R"idl(
package p;
message Child {
  required int64 count = 1;
  optional string note = 2 [max_bytes = 32];
}
message Root {
  required int32 delta = 1;
  optional uint64 sequence = 2;
  optional fixed32 code = 3;
  optional fixed64 wide = 4;
  optional float sample = 5;
  optional double precise = 6;
  optional bool enabled = 7;
  optional string label = 8 [max_bytes = 32];
  optional bytes payload = 9 [max_bytes = 32];
  optional vector<uint32> values = 10 [max_capacity = 8];
  optional Child child = 11;
}
)idl",
                                                options);
        ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
        compiled_.emplace(std::move(*compiled));
        for (const auto& descriptor : compiled_->types()) {
            if (descriptor->aggregate().full_name() == "p.Root") {
                root_schema_ = descriptor;
            } else if (descriptor->aggregate().full_name() == "p.Child") {
                child_schema_ = descriptor;
            }
        }
        root_descriptor_ = root_schema_.get();
        child_descriptor_ = child_schema_.get();
        ASSERT_NE(root_descriptor_, nullptr);
        ASSERT_NE(child_descriptor_, nullptr);
        auto layout = LayoutPlanner::Plan(*root_descriptor_, compiled_->types());
        ASSERT_TRUE(layout.ok()) << layout.status().ToString();
        root_layout_.emplace(std::move(*layout));

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

        pin_memory_ = AllocateAligned(ShmPinTable::RequiredSize());
        auto pins = ShmPinTable::Init(pin_memory_.get(),
                                      ShmPinTable::RequiredSize(), allocator_);
        ASSERT_TRUE(pins.ok()) << pins.status().ToString();
        pins_.emplace(*pins);
    }

    Result<DynamicView> View(DynamicObject& object) {
        auto pin = object.Pin();
        if (!pin.ok()) return pin.status();
        return DynamicView::Create(root_schema_, *root_layout_,
                                   object.root_handle(), allocator_,
                                   std::move(*pin), compiled_->types());
    }

    std::optional<CompiledSchema> compiled_;
    DynamicSchemaHandle root_schema_;
    DynamicSchemaHandle child_schema_;
    const SchemaDescriptor* root_descriptor_ = nullptr;
    const SchemaDescriptor* child_descriptor_ = nullptr;
    std::optional<LayoutPlan> root_layout_;
    AlignedBytes allocator_memory_;
    AlignedBytes journal_memory_;
    AlignedBytes pin_memory_;
    size_t journal_size_ = 0;
    CentralSlabAllocator allocator_;
    std::optional<AllocationJournal> journal_;
    std::optional<ShmPinTable> pins_;
};

TEST_F(DynamicObjectTest, StableHeaderFieldHandlesAndTypedView) {
    auto builder = DynamicBuilder::Create(
        root_schema_, *root_layout_, allocator_, *journal_, TypeId{42},
        compiled_->types());
    ASSERT_TRUE(builder.ok()) << builder.status().ToString();

    auto delta = FieldHandle::ByName(*root_descriptor_, *root_layout_, "delta");
    auto label = FieldHandle::ByName(*root_descriptor_, *root_layout_, "label");
    auto enabled = FieldHandle::ById(*root_descriptor_, *root_layout_, 7);
    ASSERT_TRUE(delta.ok());
    ASSERT_TRUE(label.ok());
    ASSERT_TRUE(enabled.ok());
    EXPECT_EQ(delta->field_index(), 0u);
    ASSERT_TRUE(builder->SetSigned(*delta, -9).ok());
    ASSERT_TRUE(builder->SetString(*label, "héllo").ok());
    ASSERT_TRUE(builder->SetBool(*enabled, true).ok());
    ASSERT_EQ(builder->SetString(*label, std::string_view("\xc0\x80", 2)).code(),
              StatusCode::kSchemaMismatch);

    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok()) << object.status().ToString();
    EXPECT_EQ(journal_->ActiveTransactionCount(), 1u);
    auto slab = allocator_.Inspect(object->root_handle());
    ASSERT_TRUE(slab.ok());
    const auto* bytes = static_cast<const std::byte*>(slab->data);
    uint32_t header_layout = 0;
    std::memcpy(&header_layout,
                bytes + ObjectHeaderLayout::kLayoutVersionOffset,
                sizeof(header_layout));
    EXPECT_EQ(header_layout, root_layout_->layout_version());
    uint64_t fixed_field_bytes = 0;
    std::memcpy(&fixed_field_bytes,
                bytes + ObjectHeaderLayout::kObjectSizeOffset,
                sizeof(fixed_field_bytes));
    EXPECT_EQ(fixed_field_bytes, root_layout_->fixed_area_size());
    EXPECT_NE(fixed_field_bytes, slab->object_size);

    {
        auto view = View(*object);
        ASSERT_TRUE(view.ok()) << view.status().ToString();
        EXPECT_EQ(*view->GetSigned(*delta), -9);
        EXPECT_EQ(*view->GetString(*label), "héllo");
        EXPECT_TRUE(*view->GetBool(*enabled));
        EXPECT_FALSE(*view->HasById(2));
        EXPECT_EQ(view->GetUnsigned(*enabled).status().code(),
                  StatusCode::kSchemaMismatch);
        EXPECT_EQ(object->Reclaim().code(), StatusCode::kUnavailable);
        EXPECT_EQ(journal_->ActiveTransactionCount(), 1u);
        EXPECT_EQ(*view->GetString(*label), "héllo")
            << "an established Pin remains readable after Retire";
    }
    ASSERT_TRUE(object->Reclaim().ok());
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
}

TEST_F(DynamicObjectTest, FullDigestCapabilityRejectsInjectedShortIdCollision) {
    CanonicalDigest collision_digest =
        root_descriptor_->identity().canonical_digest();
    collision_digest[31] ^= std::byte{1};
    std::vector<DependencyDescriptor> dependencies(
        root_descriptor_->dependencies().begin(),
        root_descriptor_->dependencies().end());
    DynamicSchemaHandle collision = std::make_shared<SchemaDescriptor>(
        root_descriptor_->aggregate(),
        SchemaIdentity(root_descriptor_->identity().short_id(),
                               collision_digest,
                               root_descriptor_->identity().schema_version(),
                               root_descriptor_->identity().layout_version()),
        std::string(root_descriptor_->canonical_schema()),
        std::move(dependencies));

    auto rejected = DynamicBuilder::Create(
        collision, *root_layout_, allocator_, *journal_, {}, compiled_->types());
    EXPECT_EQ(rejected.status().code(), StatusCode::kSchemaMismatch);

    auto builder = DynamicBuilder::Create(
        root_schema_, *root_layout_, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto actual = FieldHandle::ById(*root_descriptor_, *root_layout_, 1);
    auto forged = FieldHandle::ById(*collision, *root_layout_, 1);
    ASSERT_TRUE(actual.ok());
    ASSERT_TRUE(forged.ok());
    EXPECT_EQ(builder->SetSigned(*forged, 1).code(),
              StatusCode::kInvalidArgument);
    ASSERT_TRUE(builder->SetSigned(*actual, 1).ok());
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok());

    std::vector<DynamicSchemaHandle> colliding_closure = {collision};
    EXPECT_EQ(ObjectGraphWalker::Validate(
                  *root_descriptor_, *root_layout_, object->root_handle(),
                  allocator_, colliding_closure).code(),
              StatusCode::kSchemaMismatch);
    auto pin = object->Pin();
    ASSERT_TRUE(pin.ok());
    EXPECT_EQ(DynamicView::Create(
                  root_schema_, *root_layout_, object->root_handle(), allocator_,
                  std::move(*pin), colliding_closure).status().code(),
              StatusCode::kSchemaMismatch);
    ASSERT_TRUE(object->Reclaim().ok());
}

TEST_F(DynamicObjectTest, RejectsMismatchedShortPlanBeforeAnyAllocation) {
    const std::vector<DynamicSchemaHandle> child_closure = {child_schema_};
    auto child_layout =
        LayoutPlanner::Plan(*child_descriptor_, child_closure);
    ASSERT_TRUE(child_layout.ok()) << child_layout.status().ToString();
    ASSERT_LT(child_layout->object_size(), root_layout_->object_size());

    auto rejected = DynamicBuilder::Create(
        root_schema_, *child_layout, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
}

TEST_F(DynamicObjectTest, PreparedPublicationFinalizesOnlyAfterVisibility) {
    auto delta = FieldHandle::ById(*root_descriptor_, *root_layout_, 1);
    ASSERT_TRUE(delta.ok());

    auto failed_builder = DynamicBuilder::Create(
        root_schema_, *root_layout_, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(failed_builder.ok());
    ASSERT_TRUE(failed_builder->SetSigned(*delta, 1).ok());
    auto failed = failed_builder->Prepare();
    ASSERT_TRUE(failed.ok());
    ASSERT_EQ(*journal_->State(failed->transaction()),
              AllocationJournalState::kBuilding);
    ASSERT_TRUE(failed->CommitPublication(
        PublicationBinding{.channel_kind = PublicationChannelKind::kMpsc,
                           .channel_id = 9,
                           .sequence = 17,
                           .payload = failed->root_handle()}).ok());
    ASSERT_EQ(*journal_->State(failed->transaction()),
              AllocationJournalState::kCommitted);
    const ShmHandle failed_root = failed->root_handle();
    ASSERT_TRUE(failed->Rollback().ok());
    EXPECT_EQ(allocator_.Inspect(failed_root).status().code(),
              StatusCode::kNotFound);

    auto visible_builder = DynamicBuilder::Create(
        root_schema_, *root_layout_, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(visible_builder.ok());
    ASSERT_TRUE(visible_builder->SetSigned(*delta, 2).ok());
    auto visible = visible_builder->Prepare();
    ASSERT_TRUE(visible.ok());
    const ShmHandle visible_root = visible->root_handle();
    ASSERT_TRUE(visible->CommitPublication(
        PublicationBinding{.channel_kind = PublicationChannelKind::kSpsc,
                           .channel_id = 10,
                           .sequence = 18,
                           .payload = visible_root}).ok());
    EXPECT_EQ(journal_->ActiveTransactionCount(), 1u);
    ASSERT_TRUE(visible->FinalizeVisible().ok());
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
    EXPECT_EQ(allocator_.Inspect(visible_root)->state, ObjectState::kPublished);
    ASSERT_TRUE(allocator_.Retire(visible_root).ok());
    ASSERT_TRUE(allocator_.Reclaim(visible_root).ok());
}

TEST_F(DynamicObjectTest, DynamicMessageRoundTripIsWireByteEquivalent) {
    DynamicMessage source;
    ASSERT_TRUE(source.SetField(1, DynamicValue::Signed(-123)).ok());
    ASSERT_TRUE(source.SetField(2, DynamicValue::Unsigned(300)).ok());
    ASSERT_TRUE(source.SetField(3, DynamicValue::Unsigned(0x78563412u)).ok());
    ASSERT_TRUE(source.SetField(4, DynamicValue::Unsigned(0x0123456789abcdefULL)).ok());
    ASSERT_TRUE(source.SetField(5, DynamicValue::Float32Bits(0x7fc01234u)).ok());
    ASSERT_TRUE(source.SetField(6, DynamicValue::Float64Bits(
                                       0x7ff8000012345678ULL)).ok());
    ASSERT_TRUE(source.SetField(7, DynamicValue::Boolean(true)).ok());
    auto label = DynamicValue::String("é");
    ASSERT_TRUE(label.ok());
    ASSERT_TRUE(source.SetField(8, std::move(*label)).ok());
    const std::vector<std::byte> payload = {std::byte{0}, std::byte{0xff}};
    auto payload_value = DynamicValue::Bytes(payload);
    ASSERT_TRUE(payload_value.ok());
    ASSERT_TRUE(source.SetField(9, std::move(*payload_value)).ok());

    auto values = std::make_shared<DynamicVector>();
    ASSERT_TRUE(values->Add(DynamicValue::Unsigned(1)).ok());
    ASSERT_TRUE(values->Add(DynamicValue::Unsigned(127)).ok());
    ASSERT_TRUE(values->Add(DynamicValue::Unsigned(128)).ok());
    auto vector_value = DynamicValue::Vector(values);
    ASSERT_TRUE(vector_value.ok());
    ASSERT_TRUE(source.SetField(10, std::move(*vector_value)).ok());

    auto child = std::make_shared<DynamicMessage>();
    ASSERT_TRUE(child->SetField(1, DynamicValue::Signed(99)).ok());
    auto note = DynamicValue::String("child");
    ASSERT_TRUE(note.ok());
    ASSERT_TRUE(child->SetField(2, std::move(*note)).ok());
    auto child_value = DynamicValue::Message(child);
    ASSERT_TRUE(child_value.ok());
    ASSERT_TRUE(source.SetField(11, std::move(*child_value)).ok());
    const std::vector<std::byte> unknown = {std::byte{0x78}, std::byte{0x07}};
    ASSERT_TRUE(source.mutable_unknown_fields().Add(15, unknown).ok());

    auto wire_before = CanonicalWireCodec::Encode(
        *root_descriptor_, source, compiled_->types());
    ASSERT_TRUE(wire_before.ok()) << wire_before.status().ToString();
    auto builder = DynamicBuilder::FromDynamicMessage(
        root_schema_, *root_layout_, source, allocator_, *journal_,
        TypeId{42}, compiled_->types());
    ASSERT_TRUE(builder.ok()) << builder.status().ToString();
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok()) << object.status().ToString();

    {
        auto view = View(*object);
        ASSERT_TRUE(view.ok()) << view.status().ToString();
        auto converted = view->ToDynamicMessage();
        ASSERT_TRUE(converted.ok()) << converted.status().ToString();
        auto wire_after = CanonicalWireCodec::Encode(
            *root_descriptor_, *converted, compiled_->types());
        ASSERT_TRUE(wire_after.ok()) << wire_after.status().ToString();
        EXPECT_EQ(*wire_after, *wire_before);

        auto vector_handle =
            FieldHandle::ById(*root_descriptor_, *root_layout_, 10);
        ASSERT_TRUE(vector_handle.ok());
        auto vector_view = view->GetVector(*vector_handle);
        ASSERT_TRUE(vector_view.ok());
        ASSERT_EQ(vector_view->size(), 3u);
        EXPECT_EQ(*vector_view->GetUnsigned(2), 128u);
        EXPECT_EQ(object->Reclaim().code(), StatusCode::kUnavailable);
    }
    ASSERT_TRUE(object->Reclaim().ok());
}

TEST_F(DynamicObjectTest, ReplacementRegistersNewThenEagerlyReclaimsOldChild) {
    auto builder = DynamicBuilder::Create(
        root_schema_, *root_layout_, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto delta = FieldHandle::ById(*root_descriptor_, *root_layout_, 1);
    auto label = FieldHandle::ById(*root_descriptor_, *root_layout_, 8);
    ASSERT_TRUE(delta.ok());
    ASSERT_TRUE(label.ok());
    ASSERT_TRUE(builder->SetSigned(*delta, 1).ok());
    ASSERT_TRUE(builder->SetString(*label, "first").ok());

    auto root_slab = allocator_.Inspect(builder->root_handle());
    ASSERT_TRUE(root_slab.ok());
    const FieldLayout* label_layout = root_layout_->FindField(8);
    ASSERT_NE(label_layout, nullptr);
    ShmHandle old_child;
    std::memcpy(&old_child,
                static_cast<const std::byte*>(root_slab->data) +
                    label_layout->offset(),
                sizeof(old_child));
    ASSERT_FALSE(old_child.IsNull());

    for (size_t i = 0; i < kJournalHandles * 8; ++i) {
        ASSERT_TRUE(builder->SetString(
            *label, (i & 1u) == 0 ? "replacement-a" : "replacement-b").ok());
    }
    EXPECT_EQ(allocator_.Inspect(old_child).status().code(), StatusCode::kNotFound);
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok());
    auto graph = ObjectGraphWalker::Collect(
        *root_descriptor_, *root_layout_, object->root_handle(), allocator_,
        compiled_->types());
    ASSERT_TRUE(graph.ok());
    EXPECT_EQ(graph->size(), 2u);
    ASSERT_TRUE(object->Reclaim().ok());
}

TEST_F(DynamicObjectTest, WalkerBindsChildrenToRootTransactionAndRole) {
    auto builder = DynamicBuilder::Create(
        root_schema_, *root_layout_, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto delta = FieldHandle::ById(*root_descriptor_, *root_layout_, 1);
    auto label = FieldHandle::ById(*root_descriptor_, *root_layout_, 8);
    ASSERT_TRUE(builder->SetSigned(*delta, 1).ok());
    ASSERT_TRUE(builder->SetString(*label, "owned").ok());
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok());

    auto graph = ObjectGraphWalker::Collect(
        *root_descriptor_, *root_layout_, object->root_handle(), allocator_,
        compiled_->types());
    ASSERT_TRUE(graph.ok());
    ASSERT_EQ(graph->size(), 2u);
    auto child = allocator_.Inspect((*graph)[1]);
    ASSERT_TRUE(child.ok());
    auto* header = reinterpret_cast<SlabHeader*>(
        static_cast<std::byte*>(const_cast<void*>(child->data)) -
        sizeof(SlabHeader));

    const uint64_t owner_epoch = header->owner_epoch;
    ++header->owner_epoch;
    EXPECT_EQ(ObjectGraphWalker::Validate(
                  *root_descriptor_, *root_layout_, object->root_handle(),
                  allocator_, compiled_->types()).code(),
              StatusCode::kCorruption);
    header->owner_epoch = owner_epoch;

    const uint32_t role =
        header->allocation_role.load(std::memory_order_relaxed);
    header->allocation_role.store(kAllocationFlagTransactionRoot,
                                  std::memory_order_relaxed);
    EXPECT_EQ(ObjectGraphWalker::Validate(
                  *root_descriptor_, *root_layout_, object->root_handle(),
                  allocator_, compiled_->types()).code(),
              StatusCode::kCorruption);
    header->allocation_role.store(role, std::memory_order_relaxed);
    ASSERT_TRUE(object->Reclaim().ok());
}

TEST_F(DynamicObjectTest, PublicGraphReclaimCannotBypassRootPin) {
    auto builder = DynamicBuilder::Create(
        root_schema_, *root_layout_, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto delta = FieldHandle::ById(*root_descriptor_, *root_layout_, 1);
    ASSERT_TRUE(builder->SetSigned(*delta, 1).ok());
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok());
    auto pin = object->Pin();
    ASSERT_TRUE(pin.ok());

    EXPECT_EQ(ObjectGraphWalker::Reclaim(
                  *root_descriptor_, *root_layout_, object->root_handle(),
                  allocator_, compiled_->types()).code(),
              StatusCode::kPermissionDenied);
    ASSERT_TRUE(allocator_.Inspect(object->root_handle()).ok());
    EXPECT_EQ(object->Reclaim().code(), StatusCode::kUnavailable);
    ASSERT_TRUE(pin->Release().ok());
    ASSERT_TRUE(object->Reclaim().ok());
}

TEST_F(DynamicObjectTest, ChildRegistrationFailureNeverWritesParentMetadata) {
    const size_t small_size = AllocationJournal::RequiredSize(1, 1);
    auto small_memory = AllocateAligned(small_size);
    auto small_journal = AllocationJournal::Init(
        small_memory.get(), small_size, 1, 1, allocator_);
    ASSERT_TRUE(small_journal.ok());
    auto builder = DynamicBuilder::Create(
        root_schema_, *root_layout_, allocator_, *small_journal, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto delta = FieldHandle::ById(*root_descriptor_, *root_layout_, 1);
    auto label = FieldHandle::ById(*root_descriptor_, *root_layout_, 8);
    ASSERT_TRUE(builder->SetSigned(*delta, 1).ok());
    EXPECT_EQ(builder->SetString(*label, "cannot register").code(),
              StatusCode::kResourceExhausted);

    auto slab = allocator_.Inspect(builder->root_handle());
    ASSERT_TRUE(slab.ok());
    const FieldLayout* field = root_layout_->FindField(8);
    ASSERT_NE(field, nullptr);
    const auto storage = std::span(
        static_cast<const std::byte*>(slab->data) + field->offset(),
        field->size());
    for (std::byte byte : storage) EXPECT_EQ(byte, std::byte{0});
    ASSERT_TRUE(builder->Abort().ok());
}

TEST_F(DynamicObjectTest, AbortAndDestructorReclaimRootAndChildren) {
    ShmHandle root;
    ShmHandle child;
    {
        auto builder = DynamicBuilder::Create(
            root_schema_, *root_layout_, allocator_, *journal_, {},
            compiled_->types());
        ASSERT_TRUE(builder.ok());
        root = builder->root_handle();
        auto delta = FieldHandle::ById(*root_descriptor_, *root_layout_, 1);
        auto label = FieldHandle::ById(*root_descriptor_, *root_layout_, 8);
        ASSERT_TRUE(builder->SetSigned(*delta, 1).ok());
        ASSERT_TRUE(builder->SetString(*label, "temporary").ok());
        auto slab = allocator_.Inspect(root);
        ASSERT_TRUE(slab.ok());
        const FieldLayout* field = root_layout_->FindField(8);
        std::memcpy(&child,
                    static_cast<const std::byte*>(slab->data) + field->offset(),
                    sizeof(child));
    }
    EXPECT_EQ(allocator_.Inspect(child).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(allocator_.Inspect(root).status().code(), StatusCode::kNotFound);
    EXPECT_EQ(journal_->ActiveTransactionCount(), 0u);
}

TEST_F(DynamicObjectTest, WalkerRejectsSharedOwnershipAndCyclesSafely) {
    DynamicMessage source;
    ASSERT_TRUE(source.SetField(1, DynamicValue::Signed(1)).ok());
    auto label = DynamicValue::String("root");
    ASSERT_TRUE(label.ok());
    ASSERT_TRUE(source.SetField(8, std::move(*label)).ok());
    const std::vector<std::byte> payload = {std::byte{1}, std::byte{2}};
    auto payload_value = DynamicValue::Bytes(payload);
    ASSERT_TRUE(payload_value.ok());
    ASSERT_TRUE(source.SetField(9, std::move(*payload_value)).ok());
    auto child = std::make_shared<DynamicMessage>();
    ASSERT_TRUE(child->SetField(1, DynamicValue::Signed(2)).ok());
    auto note = DynamicValue::String("nested");
    ASSERT_TRUE(note.ok());
    ASSERT_TRUE(child->SetField(2, std::move(*note)).ok());
    auto child_value = DynamicValue::Message(child);
    ASSERT_TRUE(child_value.ok());
    ASSERT_TRUE(source.SetField(11, std::move(*child_value)).ok());

    auto builder = DynamicBuilder::FromDynamicMessage(
        root_schema_, *root_layout_, source, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok());
    const ShmHandle root = object->root_handle();
    auto original = ObjectGraphWalker::Collect(
        *root_descriptor_, *root_layout_, root, allocator_, compiled_->types());
    ASSERT_TRUE(original.ok());

    auto slab = allocator_.Inspect(root);
    ASSERT_TRUE(slab.ok());
    auto* data = static_cast<std::byte*>(const_cast<void*>(slab->data));
    const FieldLayout* child_field = root_layout_->FindField(11);
    const FieldLayout* label_field = root_layout_->FindField(8);
    const FieldLayout* payload_field = root_layout_->FindField(9);
    ASSERT_NE(child_field, nullptr);
    ASSERT_NE(label_field, nullptr);
    ASSERT_NE(payload_field, nullptr);
    std::byte saved_child_metadata[VariableMetadataLayout::kSize];
    std::memcpy(saved_child_metadata, data + child_field->offset(),
                sizeof(saved_child_metadata));
    // Point the nested message back at root. The walker detects the repeated
    // handle before dereferencing it as a child, so malformed cycles are safe.
    std::memcpy(data + child_field->offset(), &root, sizeof(root));
    const Status cycle = ObjectGraphWalker::Validate(
        *root_descriptor_, *root_layout_, root, allocator_, compiled_->types());
    EXPECT_EQ(cycle.code(), StatusCode::kCorruption);

    std::memcpy(data + child_field->offset(), saved_child_metadata,
                sizeof(saved_child_metadata));
    // Give payload the same child handle as label while retaining its own
    // length/capacity metadata. Ownership sharing is rejected deterministically.
    std::memcpy(data + payload_field->offset(), data + label_field->offset(),
                sizeof(ShmHandle));
    const Status shared = ObjectGraphWalker::Validate(
        *root_descriptor_, *root_layout_, root, allocator_, compiled_->types());
    EXPECT_EQ(shared.code(), StatusCode::kCorruption);

    ASSERT_TRUE(object->Reclaim().ok());
}

TEST_F(DynamicObjectTest, VectorViewRevalidatesMetadataAndHandleGeneration) {
    DynamicMessage source;
    ASSERT_TRUE(source.SetField(1, DynamicValue::Signed(1)).ok());
    auto values = std::make_shared<DynamicVector>();
    ASSERT_TRUE(values->Add(DynamicValue::Unsigned(7)).ok());
    auto value = DynamicValue::Vector(values);
    ASSERT_TRUE(value.ok());
    ASSERT_TRUE(source.SetField(10, std::move(*value)).ok());
    auto builder = DynamicBuilder::FromDynamicMessage(
        root_schema_, *root_layout_, source, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok());
    auto view = View(*object);
    ASSERT_TRUE(view.ok());
    auto field = FieldHandle::ById(*root_descriptor_, *root_layout_, 10);
    ASSERT_TRUE(field.ok());
    auto vector = view->GetVector(*field);
    ASSERT_TRUE(vector.ok());
    EXPECT_EQ(*vector->GetUnsigned(0), 7u);

    auto root = allocator_.Inspect(object->root_handle());
    ASSERT_TRUE(root.ok());
    const FieldLayout* vector_layout = root_layout_->FindField(10);
    ASSERT_NE(vector_layout, nullptr);
    auto* metadata = static_cast<std::byte*>(const_cast<void*>(root->data)) +
                     vector_layout->offset();
    ShmHandle child;
    std::memcpy(&child, metadata, sizeof(child));
    const ShmHandle stale{.offset = child.offset,
                          .generation = child.generation + 1,
                          .region_id = child.region_id};
    std::memcpy(metadata, &stale, sizeof(stale));
    EXPECT_EQ(vector->GetUnsigned(0).status().code(), StatusCode::kNotFound);
    std::memcpy(metadata, &child, sizeof(child));

    uint64_t element_size = 0;
    std::memcpy(&element_size,
                metadata + VariableMetadataLayout::kElementSizeOffset,
                sizeof(element_size));
    const uint64_t oversized = std::numeric_limits<uint64_t>::max();
    std::memcpy(metadata + VariableMetadataLayout::kElementSizeOffset,
                &oversized, sizeof(oversized));
    EXPECT_EQ(vector->GetUnsigned(0).status().code(), StatusCode::kCorruption);
    std::memcpy(metadata + VariableMetadataLayout::kElementSizeOffset,
                &element_size, sizeof(element_size));

    EXPECT_EQ(object->Reclaim().code(), StatusCode::kUnavailable);
}

TEST_F(DynamicObjectTest, UnknownFieldViewUsesConfiguredMaxBytes) {
    DynamicMessage source;
    ASSERT_TRUE(source.SetField(1, DynamicValue::Signed(1)).ok());
    const std::vector<std::byte> unknown = {
        std::byte{0x7a}, std::byte{0x02}, std::byte{0x01}, std::byte{0x02}};
    ASSERT_TRUE(source.mutable_unknown_fields().Add(15, unknown).ok());
    auto builder = DynamicBuilder::FromDynamicMessage(
        root_schema_, *root_layout_, source, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok());

    DynamicObjectOptions options;
    options.unknown_fields.max_bytes = 3;
    auto pin = object->Pin();
    ASSERT_TRUE(pin.ok());
    auto rejected = DynamicView::Create(
        root_schema_, *root_layout_, object->root_handle(), allocator_,
        std::move(*pin), compiled_->types(), options);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kCorruption);
    ASSERT_TRUE(object->Reclaim().ok());
}

TEST_F(DynamicObjectTest, ViewRejectsMalformedHeaderPresenceMetadataAndGeneration) {
    DynamicMessage source;
    ASSERT_TRUE(source.SetField(1, DynamicValue::Signed(1)).ok());
    auto label = DynamicValue::String("checked");
    ASSERT_TRUE(label.ok());
    ASSERT_TRUE(source.SetField(8, std::move(*label)).ok());
    auto builder = DynamicBuilder::FromDynamicMessage(
        root_schema_, *root_layout_, source, allocator_, *journal_, {},
        compiled_->types());
    ASSERT_TRUE(builder.ok());
    auto object = builder->Commit(*pins_);
    ASSERT_TRUE(object.ok());
    const ShmHandle root = object->root_handle();
    auto graph = ObjectGraphWalker::Collect(
        *root_descriptor_, *root_layout_, root, allocator_, compiled_->types());
    ASSERT_TRUE(graph.ok());

    auto slab = allocator_.Inspect(root);
    ASSERT_TRUE(slab.ok());
    auto* data = static_cast<std::byte*>(const_cast<void*>(slab->data));
    const FieldLayout* label_field = root_layout_->FindField(8);
    ASSERT_NE(label_field, nullptr);

    uint64_t saved_object_size = 0;
    std::memcpy(&saved_object_size,
                data + ObjectHeaderLayout::kObjectSizeOffset,
                sizeof(saved_object_size));
    const uint64_t bad_size = saved_object_size + 1;
    std::memcpy(data + ObjectHeaderLayout::kObjectSizeOffset, &bad_size,
                sizeof(bad_size));
    EXPECT_EQ(View(*object).status().code(), StatusCode::kCorruption);
    std::memcpy(data + ObjectHeaderLayout::kObjectSizeOffset,
                &saved_object_size, sizeof(saved_object_size));

    ASSERT_TRUE(label_field->presence_bit().has_value());
    std::byte* presence = data + root_layout_->presence_bitmap_offset() +
                          (*label_field->presence_bit() / 64) * 8;
    uint64_t saved_word = 0;
    std::memcpy(&saved_word, presence, sizeof(saved_word));
    const uint64_t cleared = saved_word &
        ~(uint64_t{1} << (*label_field->presence_bit() % 64));
    std::memcpy(presence, &cleared, sizeof(cleared));
    EXPECT_EQ(ObjectGraphWalker::Validate(
                  *root_descriptor_, *root_layout_, root, allocator_,
                  compiled_->types()).code(),
              StatusCode::kCorruption);
    std::memcpy(presence, &saved_word, sizeof(saved_word));

    ShmHandle child;
    std::memcpy(&child, data + label_field->offset(), sizeof(child));
    const ShmHandle stale{.offset = child.offset,
                          .generation = child.generation + 1,
                          .region_id = child.region_id};
    std::memcpy(data + label_field->offset(), &stale, sizeof(stale));
    EXPECT_EQ(ObjectGraphWalker::Validate(
                  *root_descriptor_, *root_layout_, root, allocator_,
                  compiled_->types()).code(),
              StatusCode::kNotFound);
    std::memcpy(data + label_field->offset(), &child, sizeof(child));

    ASSERT_TRUE(object->Reclaim().ok());
}

class SharedMapping {
public:
    explicit SharedMapping(size_t size) : size_(size) {
        data_ = ::mmap(nullptr, size_, PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        if (data_ != MAP_FAILED) std::memset(data_, 0, size_);
    }
    ~SharedMapping() {
        if (data_ != MAP_FAILED) (void)::munmap(data_, size_);
    }
    bool ok() const { return data_ != MAP_FAILED; }
    void* data() const { return data_; }
private:
    void* data_ = MAP_FAILED;
    size_t size_ = 0;
};

bool AlwaysDead(const ProcessIdentity&, void*) noexcept { return false; }

struct AllocationCrashHook {
    uint32_t allocation_publications = 0;
};
void CrashBeforeSecondManifestAppend(
    AllocationJournal::PersistencePoint point, uint64_t,
    void* opaque) noexcept {
    if (point != AllocationJournal::PersistencePoint::kAllocationPublished) {
        return;
    }
    auto* hook = static_cast<AllocationCrashHook*>(opaque);
    if (++hook->allocation_publications == 2) {
        _exit(0);
    }
}

TEST(DynamicObjectRecoveryTest,
     ForkCrashBetweenAllocatorPublicationAndManifestAppendIsRecovered) {
    CompileOptions compile_options;
    compile_options.allow_implicit_schema_version = true;
    auto compiled = SchemaCompiler::Compile(
        "package p; message M { required int32 id = 1; "
        "optional string text = 2 [max_bytes = 32]; }",
        compile_options);
    ASSERT_TRUE(compiled.ok());
    DynamicSchemaHandle descriptor = compiled->types()[0];
    ASSERT_NE(descriptor, nullptr);
    auto layout = LayoutPlanner::Plan(*descriptor, compiled->types());
    ASSERT_TRUE(layout.ok());

    constexpr size_t kAllocatorBytes = 2u << 20;
    const size_t journal_size = AllocationJournal::RequiredSize(2, 16);
    SharedMapping allocator_mapping(kAllocatorBytes);
    SharedMapping journal_mapping(journal_size);
    ASSERT_TRUE(allocator_mapping.ok());
    ASSERT_TRUE(journal_mapping.ok());
    auto allocator = CentralSlabAllocator::Create(
        allocator_mapping.data(), kAllocatorBytes, AllocatorConfig());
    ASSERT_TRUE(allocator.ok());
    auto journal = AllocationJournal::Init(journal_mapping.data(), journal_size,
                                           2, 16, *allocator);
    ASSERT_TRUE(journal.ok());

    const pid_t child_pid = ::fork();
    ASSERT_GE(child_pid, 0);
    if (child_pid == 0) {
        auto attached_allocator =
            CentralSlabAllocator::Attach(allocator_mapping.data());
        if (!attached_allocator.ok()) _exit(10);
        auto attached_journal = AllocationJournal::Attach(
            journal_mapping.data(), journal_size, *attached_allocator);
        if (!attached_journal.ok()) _exit(11);
        AllocationCrashHook crash_hook;
        attached_journal->SetPersistenceHook(
            &CrashBeforeSecondManifestAppend, &crash_hook);
        ProcessIdentity owner = ProcessIdentity::Current();
        owner.process_epoch ^= 0x55aa55aaULL;
        auto builder = DynamicBuilder::Create(
            descriptor, *layout, *attached_allocator, *attached_journal, {},
            compiled->types(), owner);
        if (!builder.ok()) _exit(12);
        auto id = FieldHandle::ById(*descriptor, *layout, 1);
        auto text = FieldHandle::ById(*descriptor, *layout, 2);
        if (!id.ok() || !text.ok() ||
            !builder->SetSigned(*id, 7).ok() ||
            !builder->SetString(*text, "registered child").ok()) {
            _exit(13);
        }
        // The second allocation publication (the string child) must have exited
        // before AppendHandle. Reaching here means the crash hook did not fire.
        _exit(14);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child_pid, &status, 0), child_pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    EXPECT_EQ(journal->ActiveTransactionCount(), 1u);
    EXPECT_EQ(journal->RecoverOrphans(&AlwaysDead), 1u);
    EXPECT_EQ(journal->ActiveTransactionCount(), 0u);

    for (uint32_t i = 0; i < allocator->total_slot_count(); ++i) {
        SlabHeader header;
        ASSERT_TRUE(allocator->ReadSlotByIndex(i, &header, nullptr));
        EXPECT_EQ(static_cast<ObjectState>(
                      header.object_state.load(std::memory_order_acquire)),
                  ObjectState::kFree);
    }
}

}  // namespace
}  // namespace mino::schema
