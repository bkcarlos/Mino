// Copyright 2026 The Mino Authors

#include "tools/minoc/tests/generated/canonical_wire.generated.h"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string_view>
#include <type_traits>
#include <vector>

#include "gtest/gtest.h"
#include "mino/schema/compiler.h"
#include "mino/schema/dynamic_value.h"
#include "mino/schema/layout.h"
#include "mino/schema/object_graph_walker.h"
#include "mino/schema/wire.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/publisher.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/channel/spsc_channel.h"

namespace {

static_assert(std::is_trivially_default_constructible_v<
              minoc_wire_test::Scalars>);
static_assert(mino::StaticMessageTraits<
                  minoc_wire_test::Scalars>::index_flags == 0u);

struct AlignedDeleter {
    void operator()(std::byte* pointer) const {
        ::operator delete[](pointer, std::align_val_t(64));
    }
};

using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(std::size_t bytes) {
    AlignedBytes memory(
        new (std::align_val_t(64)) std::byte[bytes]);
    std::memset(memory.get(), 0, bytes);
    return memory;
}

void ExpectScalarMessagesEqual(
    const mino::schema::DynamicMessage& actual,
    const mino::schema::DynamicMessage& expected) {
    ASSERT_EQ(actual.fields().size(), expected.fields().size());
    ASSERT_TRUE(actual.unknown_fields().fields().empty());
    ASSERT_TRUE(expected.unknown_fields().fields().empty());
    for (const mino::schema::DynamicField& expected_field : expected.fields()) {
        const mino::schema::DynamicValue* actual_value =
            actual.FindField(expected_field.id());
        ASSERT_NE(actual_value, nullptr) << expected_field.id();
        const mino::schema::DynamicValue& expected_value = expected_field.value();
        ASSERT_EQ(actual_value->kind(), expected_value.kind())
            << expected_field.id();
        switch (expected_value.kind()) {
            case mino::schema::DynamicValue::Kind::kSignedInteger:
                ASSERT_NE(actual_value->signed_integer(), nullptr);
                EXPECT_EQ(actual_value->signed_integer()->value,
                          expected_value.signed_integer()->value);
                break;
            case mino::schema::DynamicValue::Kind::kUnsignedInteger:
                ASSERT_NE(actual_value->unsigned_integer(), nullptr);
                EXPECT_EQ(actual_value->unsigned_integer()->value,
                          expected_value.unsigned_integer()->value);
                break;
            case mino::schema::DynamicValue::Kind::kFloat32:
                ASSERT_NE(actual_value->float32(), nullptr);
                EXPECT_EQ(actual_value->float32()->bits,
                          expected_value.float32()->bits);
                break;
            case mino::schema::DynamicValue::Kind::kFloat64:
                ASSERT_NE(actual_value->float64(), nullptr);
                EXPECT_EQ(actual_value->float64()->bits,
                          expected_value.float64()->bits);
                break;
            case mino::schema::DynamicValue::Kind::kBoolean:
                ASSERT_NE(actual_value->boolean(), nullptr);
                EXPECT_EQ(actual_value->boolean()->value,
                          expected_value.boolean()->value);
                break;
            case mino::schema::DynamicValue::Kind::kString:
            case mino::schema::DynamicValue::Kind::kBytes:
            case mino::schema::DynamicValue::Kind::kBytesView:
            case mino::schema::DynamicValue::Kind::kMessage:
            case mino::schema::DynamicValue::Kind::kVector:
                FAIL() << "fixed-scalar test received a non-scalar value";
        }
    }
}

constexpr std::string_view kSchema = R"idl(
syntax = "v1";
package minoc_wire_test;
option schema_version = "1.0";

struct Scalars {
  required int32 delta32 = 1;
  required int64 delta64 = 2;
  required uint32 count32 = 3;
  required uint64 count64 = 4;
  required fixed32 code32 = 5;
  required fixed64 code64 = 6;
  required float sample32 = 7;
  required double sample64 = 8;
  required bool active = 9;
  optional uint32 sequence = 10;
}

message Child {
  required int32 id = 1;
  optional string note = 2 [max_bytes = 32];
}

struct InlinePoint {
  required fixed32 x = 1;
  required fixed32 y = 2;
}

message GraphValues {
  required string text = 1 [max_bytes = 64];
  required bytes payload = 2 [max_bytes = 64];
  required vector<uint32> values = 3 [max_capacity = 8];
  required Child child = 4;
  optional vector<Child> children = 5 [max_capacity = 4];
  optional InlinePoint point = 6;
}
)idl";

TEST(CanonicalWireGeneratedTest, StaticBytesEqualDynamicMessageAndDecodeRoundTrips) {
    constexpr std::uint32_t kFloatBits = 0x7fc01234u;
    constexpr std::uint64_t kDoubleBits = 0x7ff8000012345678ULL;

    minoc_wire_test::Scalars object;
    minoc_wire_test::ScalarsBuilder builder(object);
    builder.set_delta32(-123);
    builder.set_delta64(std::numeric_limits<std::int64_t>::min());
    builder.set_count32(300u);
    builder.set_count64(0xfedcba9876543210ULL);
    builder.set_code32(0x78563412u);
    builder.set_code64(0x0123456789abcdefULL);
    builder.set_sample32(std::bit_cast<float>(kFloatBits));
    builder.set_sample64(std::bit_cast<double>(kDoubleBits));
    builder.set_active(true);
    builder.set_sequence(42u);

    mino::schema::CompileOptions options;
    auto compiled = mino::schema::SchemaCompiler::Compile(kSchema, options);
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    const mino::schema::SchemaDescriptor* scalars =
        compiled->FindType("minoc_wire_test.Scalars");
    ASSERT_NE(scalars, nullptr);
    std::vector<std::shared_ptr<const mino::schema::SchemaDescriptor>>
        scalar_descriptors;
    for (const auto& descriptor : compiled->types()) {
        if (descriptor.get() == scalars) scalar_descriptors.push_back(descriptor);
    }
    ASSERT_EQ(scalar_descriptors.size(), 1u);

    mino::schema::DynamicMessage dynamic;
    ASSERT_TRUE(dynamic.SetField(
        1, mino::schema::DynamicValue::Signed(-123)).ok());
    ASSERT_TRUE(dynamic.SetField(
        2, mino::schema::DynamicValue::Signed(
               std::numeric_limits<std::int64_t>::min())).ok());
    ASSERT_TRUE(dynamic.SetField(
        3, mino::schema::DynamicValue::Unsigned(300u)).ok());
    ASSERT_TRUE(dynamic.SetField(
        4, mino::schema::DynamicValue::Unsigned(
               0xfedcba9876543210ULL)).ok());
    ASSERT_TRUE(dynamic.SetField(
        5, mino::schema::DynamicValue::Unsigned(0x78563412u)).ok());
    ASSERT_TRUE(dynamic.SetField(
        6, mino::schema::DynamicValue::Unsigned(
               0x0123456789abcdefULL)).ok());
    ASSERT_TRUE(dynamic.SetField(
        7, mino::schema::DynamicValue::Float32Bits(kFloatBits)).ok());
    ASSERT_TRUE(dynamic.SetField(
        8, mino::schema::DynamicValue::Float64Bits(kDoubleBits)).ok());
    ASSERT_TRUE(dynamic.SetField(
        9, mino::schema::DynamicValue::Boolean(true)).ok());
    ASSERT_TRUE(dynamic.SetField(
        10, mino::schema::DynamicValue::Unsigned(42u)).ok());

    auto static_dynamic =
        minoc_wire_test::ScalarsWireAdapter::ToDynamicMessage(object);
    ASSERT_TRUE(static_dynamic.ok()) << static_dynamic.status().ToString();
    ExpectScalarMessagesEqual(*static_dynamic, dynamic);

    auto static_bytes = minoc_wire_test::ScalarsWireAdapter::Encode(object);
    ASSERT_TRUE(static_bytes.ok()) << static_bytes.status().ToString();
    auto static_bytes_again = minoc_wire_test::ScalarsWireAdapter::Encode(object);
    ASSERT_TRUE(static_bytes_again.ok())
        << static_bytes_again.status().ToString();
    EXPECT_EQ(*static_bytes_again, *static_bytes);

    auto dynamic_bytes = mino::schema::CanonicalWireCodec::Encode(
        *scalars, dynamic, scalar_descriptors);
    ASSERT_TRUE(dynamic_bytes.ok()) << dynamic_bytes.status().ToString();
    EXPECT_EQ(*static_bytes, *dynamic_bytes);

    auto decoded = minoc_wire_test::ScalarsWireAdapter::Decode(*dynamic_bytes);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    const minoc_wire_test::ScalarsAccessor accessor(*decoded);
    ASSERT_TRUE(accessor.valid());
    EXPECT_EQ(accessor.delta32(), -123);
    EXPECT_EQ(accessor.delta64(), std::numeric_limits<std::int64_t>::min());
    EXPECT_EQ(accessor.count32(), 300u);
    EXPECT_EQ(accessor.count64(), 0xfedcba9876543210ULL);
    EXPECT_EQ(accessor.code32(), 0x78563412u);
    EXPECT_EQ(accessor.code64(), 0x0123456789abcdefULL);
    EXPECT_EQ(std::bit_cast<std::uint32_t>(accessor.sample32()), kFloatBits);
    EXPECT_EQ(std::bit_cast<std::uint64_t>(accessor.sample64()), kDoubleBits);
    EXPECT_TRUE(accessor.active());
    EXPECT_TRUE(accessor.has_sequence());
    EXPECT_EQ(accessor.sequence(), 42u);
}

TEST(CanonicalWireGeneratedTest, GeneratedTypePublishesThroughRealPublisher) {
    constexpr std::size_t kAllocatorBytes = 1u << 20;
    constexpr std::uint64_t kChannelCapacity = 2;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    mino::ClassTableConfig config;
    config.classes = {{.slot_size = 256, .slot_count = 4}};
    auto allocator = mino::CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes, config);
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    auto channel_memory = AllocateAligned(
        mino::SpscChannel::RequiredSize(kChannelCapacity));
    auto channel = mino::SpscChannel::Init(
        channel_memory.get(), kChannelCapacity);
    ASSERT_TRUE(channel.ok()) << channel.status().ToString();

    mino::Publisher<minoc_wire_test::Scalars> publisher(*allocator, *channel);
    auto allocated = publisher.Allocate();
    ASSERT_TRUE(allocated.ok()) << allocated.status().ToString();
    minoc_wire_test::ScalarsBuilder builder(**allocated);
    builder.set_delta32(0);
    builder.set_delta64(0);
    builder.set_count32(0);
    builder.set_count64(0);
    builder.set_code32(0);
    builder.set_code64(0);
    builder.set_sample32(0.0f);
    builder.set_sample64(0.0);
    builder.set_active(false);
    ASSERT_TRUE(publisher.PublishLocal(std::move(*allocated)).ok());
    EXPECT_EQ(publisher.published_count(), 1u);
    auto published = channel->Poll();
    ASSERT_TRUE(published.ok()) << published.status().ToString();
    EXPECT_EQ(published->slot()->flags,
              mino::StaticMessageTraits<
                  minoc_wire_test::Scalars>::index_flags);
    EXPECT_EQ(published->slot()->schema_short_id,
              minoc_wire_test::Scalars::kSchemaShortId);
    EXPECT_TRUE(std::move(*published).Ack().ok());
}

TEST(CanonicalWireGeneratedTest, DecodePreservesOptionalAbsence) {
    mino::schema::CompileOptions options;
    auto compiled = mino::schema::SchemaCompiler::Compile(kSchema, options);
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();

    const mino::schema::SchemaDescriptor* scalars =
        compiled->FindType("minoc_wire_test.Scalars");
    ASSERT_NE(scalars, nullptr);
    std::vector<std::shared_ptr<const mino::schema::SchemaDescriptor>>
        scalar_descriptors;
    for (const auto& descriptor : compiled->types()) {
        if (descriptor.get() == scalars) scalar_descriptors.push_back(descriptor);
    }
    ASSERT_EQ(scalar_descriptors.size(), 1u);

    mino::schema::DynamicMessage dynamic;
    ASSERT_TRUE(dynamic.SetField(1, mino::schema::DynamicValue::Signed(0)).ok());
    ASSERT_TRUE(dynamic.SetField(2, mino::schema::DynamicValue::Signed(0)).ok());
    ASSERT_TRUE(dynamic.SetField(3, mino::schema::DynamicValue::Unsigned(0)).ok());
    ASSERT_TRUE(dynamic.SetField(4, mino::schema::DynamicValue::Unsigned(0)).ok());
    ASSERT_TRUE(dynamic.SetField(5, mino::schema::DynamicValue::Unsigned(0)).ok());
    ASSERT_TRUE(dynamic.SetField(6, mino::schema::DynamicValue::Unsigned(0)).ok());
    ASSERT_TRUE(dynamic.SetField(7, mino::schema::DynamicValue::Float32Bits(0)).ok());
    ASSERT_TRUE(dynamic.SetField(8, mino::schema::DynamicValue::Float64Bits(0)).ok());
    ASSERT_TRUE(dynamic.SetField(9, mino::schema::DynamicValue::Boolean(false)).ok());
    auto bytes = mino::schema::CanonicalWireCodec::Encode(
        *scalars, dynamic, scalar_descriptors);
    ASSERT_TRUE(bytes.ok()) << bytes.status().ToString();

    auto decoded = minoc_wire_test::ScalarsWireAdapter::Decode(*bytes);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_FALSE(minoc_wire_test::ScalarsAccessor(*decoded).has_sequence());
}

TEST(CanonicalWireGeneratedTest,
     GraphAwareRoundTripMatchesDynamicAndReclaimsWholeGraph) {
    mino::schema::CompileOptions compile_options;
    auto compiled = mino::schema::SchemaCompiler::Compile(kSchema, compile_options);
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    const mino::schema::SchemaDescriptor* root =
        compiled->FindType("minoc_wire_test.GraphValues");
    ASSERT_NE(root, nullptr);

    std::vector<std::shared_ptr<const mino::schema::SchemaDescriptor>> closure;
    for (const auto& descriptor : compiled->types()) {
        const std::string_view name = descriptor->aggregate().full_name();
        if (name == "minoc_wire_test.GraphValues" ||
            name == "minoc_wire_test.Child" ||
            name == "minoc_wire_test.InlinePoint") {
            closure.push_back(descriptor);
        }
    }
    ASSERT_EQ(closure.size(), 3u);

    mino::schema::DynamicMessage source;
    auto text = mino::schema::DynamicValue::String("héllo");
    ASSERT_TRUE(text.ok());
    ASSERT_TRUE(source.SetField(1, std::move(*text)).ok());
    const std::vector<std::byte> payload = {
        std::byte{0x00}, std::byte{0xff}, std::byte{0x07}};
    auto payload_value = mino::schema::DynamicValue::Bytes(payload);
    ASSERT_TRUE(payload_value.ok());
    ASSERT_TRUE(source.SetField(2, std::move(*payload_value)).ok());

    auto values = std::make_shared<mino::schema::DynamicVector>();
    ASSERT_TRUE(values->Add(mino::schema::DynamicValue::Unsigned(1)).ok());
    ASSERT_TRUE(values->Add(mino::schema::DynamicValue::Unsigned(127)).ok());
    ASSERT_TRUE(values->Add(mino::schema::DynamicValue::Unsigned(128)).ok());
    auto values_value = mino::schema::DynamicValue::Vector(values);
    ASSERT_TRUE(values_value.ok());
    ASSERT_TRUE(source.SetField(3, std::move(*values_value)).ok());

    auto child = std::make_shared<mino::schema::DynamicMessage>();
    ASSERT_TRUE(child->SetField(1, mino::schema::DynamicValue::Signed(-7)).ok());
    auto child_note = mino::schema::DynamicValue::String("nested");
    ASSERT_TRUE(child_note.ok());
    ASSERT_TRUE(child->SetField(2, std::move(*child_note)).ok());
    auto child_value = mino::schema::DynamicValue::Message(child);
    ASSERT_TRUE(child_value.ok());
    ASSERT_TRUE(source.SetField(4, std::move(*child_value)).ok());

    auto children = std::make_shared<mino::schema::DynamicVector>();
    auto vector_child = std::make_shared<mino::schema::DynamicMessage>();
    ASSERT_TRUE(vector_child->SetField(
        1, mino::schema::DynamicValue::Signed(9)).ok());
    auto vector_note = mino::schema::DynamicValue::String("vector child");
    ASSERT_TRUE(vector_note.ok());
    ASSERT_TRUE(vector_child->SetField(2, std::move(*vector_note)).ok());
    auto vector_child_value =
        mino::schema::DynamicValue::Message(vector_child);
    ASSERT_TRUE(vector_child_value.ok());
    ASSERT_TRUE(children->Add(std::move(*vector_child_value)).ok());
    auto children_value = mino::schema::DynamicValue::Vector(children);
    ASSERT_TRUE(children_value.ok());
    ASSERT_TRUE(source.SetField(5, std::move(*children_value)).ok());

    auto point = std::make_shared<mino::schema::DynamicMessage>();
    ASSERT_TRUE(point->SetField(
        1, mino::schema::DynamicValue::Unsigned(0x12345678u)).ok());
    ASSERT_TRUE(point->SetField(
        2, mino::schema::DynamicValue::Unsigned(0x90abcdefu)).ok());
    auto point_value = mino::schema::DynamicValue::Message(point);
    ASSERT_TRUE(point_value.ok());
    ASSERT_TRUE(source.SetField(6, std::move(*point_value)).ok());

    const std::vector<std::byte> unknown = {
        std::byte{0x78}, std::byte{0x07}};
    ASSERT_TRUE(source.mutable_unknown_fields().Add(15, unknown).ok());

    auto dynamic_bytes = mino::schema::CanonicalWireCodec::Encode(
        *root, source, closure);
    ASSERT_TRUE(dynamic_bytes.ok()) << dynamic_bytes.status().ToString();

    constexpr std::size_t kAllocatorBytes = 4u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    mino::ClassTableConfig config;
    config.classes = {
        {.slot_size = 64, .slot_count = 32},
        {.slot_size = 128, .slot_count = 32},
        {.slot_size = 256, .slot_count = 32},
        {.slot_size = 512, .slot_count = 32},
        {.slot_size = 1024, .slot_count = 16},
    };
    auto allocator = mino::CentralSlabAllocator::Create(
        allocator_memory.get(), kAllocatorBytes, config);
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    constexpr std::uint32_t kTransactions = 8;
    constexpr std::uint32_t kHandles = 128;
    const std::size_t journal_size =
        mino::AllocationJournal::RequiredSize(kTransactions, kHandles);
    auto journal_memory = AllocateAligned(journal_size);
    auto journal = mino::AllocationJournal::Init(
        journal_memory.get(), journal_size, kTransactions, kHandles, *allocator);
    ASSERT_TRUE(journal.ok()) << journal.status().ToString();
    auto pin_memory = AllocateAligned(mino::ShmPinTable::RequiredSize());
    auto pins = mino::ShmPinTable::Init(
        pin_memory.get(), mino::ShmPinTable::RequiredSize(), *allocator);
    ASSERT_TRUE(pins.ok()) << pins.status().ToString();

    auto object = minoc_wire_test::GraphValuesWireAdapter::Decode(
        *dynamic_bytes, *allocator, *journal, *pins);
    ASSERT_TRUE(object.ok()) << object.status().ToString();
    const mino::ShmHandle root_handle = object->root_handle();
    auto layout = mino::schema::LayoutPlanner::Plan(*root, closure);
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    auto graph = mino::schema::ObjectGraphWalker::Collect(
        *root, *layout, root_handle, *allocator, closure);
    ASSERT_TRUE(graph.ok()) << graph.status().ToString();
    ASSERT_GT(graph->size(), 1u);

    auto dynamic_pin = object->Pin();
    ASSERT_TRUE(dynamic_pin.ok()) << dynamic_pin.status().ToString();
    auto converted = minoc_wire_test::GraphValuesWireAdapter::ToDynamicMessage(
        root_handle, *allocator, std::move(*dynamic_pin));
    ASSERT_TRUE(converted.ok()) << converted.status().ToString();
    EXPECT_EQ(pins->PinCount(root_handle), 0u);

    ASSERT_NE(converted->FindField(1), nullptr);
    ASSERT_NE(converted->FindField(1)->string(), nullptr);
    EXPECT_EQ(converted->FindField(1)->string()->value, "héllo");
    ASSERT_NE(converted->FindField(2)->bytes(), nullptr);
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(),
                           converted->FindField(2)->bytes()->value.begin(),
                           converted->FindField(2)->bytes()->value.end()));
    ASSERT_NE(converted->FindField(3)->vector(), nullptr);
    ASSERT_EQ(converted->FindField(3)->vector()->value->values().size(), 3u);
    EXPECT_EQ(converted->FindField(3)->vector()->value->values()[2]
                  .unsigned_integer()->value,
              128u);
    ASSERT_NE(converted->FindField(4)->message(), nullptr);
    EXPECT_EQ(converted->FindField(4)->message()->value->FindField(1)
                  ->signed_integer()->value,
              -7);
    ASSERT_NE(converted->FindField(5)->vector(), nullptr);
    ASSERT_EQ(converted->FindField(5)->vector()->value->values().size(), 1u);
    EXPECT_EQ(converted->FindField(5)->vector()->value->values()[0]
                  .message()->value->FindField(2)->string()->value,
              "vector child");
    ASSERT_NE(converted->FindField(6)->message(), nullptr);
    EXPECT_EQ(converted->FindField(6)->message()->value->FindField(2)
                  ->unsigned_integer()->value,
              0x90abcdefu);
    ASSERT_EQ(converted->unknown_fields().fields().size(), 1u);
    EXPECT_TRUE(std::equal(
        unknown.begin(), unknown.end(),
        converted->unknown_fields().fields()[0].canonical_bytes().begin(),
        converted->unknown_fields().fields()[0].canonical_bytes().end()));

    auto encode_pin = object->Pin();
    ASSERT_TRUE(encode_pin.ok()) << encode_pin.status().ToString();
    auto static_bytes = minoc_wire_test::GraphValuesWireAdapter::Encode(
        root_handle, *allocator, std::move(*encode_pin));
    ASSERT_TRUE(static_bytes.ok()) << static_bytes.status().ToString();
    EXPECT_EQ(*static_bytes, *dynamic_bytes);
    EXPECT_EQ(pins->PinCount(root_handle), 0u);

    ASSERT_TRUE(object->Reclaim().ok());
    for (mino::ShmHandle handle : *graph) {
        EXPECT_EQ(allocator->Inspect(handle).status().code(),
                  mino::StatusCode::kNotFound);
    }
}

}  // namespace
