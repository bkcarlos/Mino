// Copyright 2026 The Mino Authors

#include "mino/schema/wire.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/compiler.h"

namespace mino::schema {
namespace {

std::vector<std::byte> Bytes(std::initializer_list<uint8_t> values) {
    std::vector<std::byte> result;
    for (uint8_t value : values) result.push_back(static_cast<std::byte>(value));
    return result;
}

std::string Hex(std::span<const std::byte> bytes) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    for (std::byte byte : bytes) {
        const uint8_t value = static_cast<uint8_t>(byte);
        result.push_back(kHex[value >> 4]);
        result.push_back(kHex[value & 0xf]);
    }
    return result;
}

std::shared_ptr<const SchemaDescriptor> CompileOne(std::string_view idl) {
    CompileOptions options;
    options.allow_implicit_schema_version = true;
    auto compiled = SchemaCompiler::Compile(idl, options);
    EXPECT_TRUE(compiled.ok()) << compiled.status().ToString();
    if (!compiled.ok() || compiled->types().size() != 1) return nullptr;
    return compiled->types()[0];
}

TEST(CanonicalWireTest, Leb128AndZigZagAreMinimal) {
    std::vector<std::byte> encoded;
    ASSERT_TRUE(EncodeLeb128(300, encoded).ok());
    EXPECT_EQ(Hex(encoded), "ac02");
    size_t offset = 0;
    auto decoded = DecodeLeb128(encoded, offset);
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(*decoded, 300u);
    EXPECT_EQ(offset, encoded.size());

    const std::array<int64_t, 5> signed_values = {
        std::numeric_limits<int64_t>::min(), -1, 0, 1,
        std::numeric_limits<int64_t>::max()};
    for (int64_t value : signed_values) {
        EXPECT_EQ(ZigZagDecode(ZigZagEncode(value)), value);
    }

    auto overlong = Bytes({0x81, 0x00});
    offset = 0;
    auto rejected = DecodeLeb128(overlong, offset);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kCorruption);

    auto too_wide = Bytes({0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                           0xff, 0xff, 0xff, 0x02});
    offset = 0;
    rejected = DecodeLeb128(too_wide, offset);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kCorruption);
}

TEST(CanonicalWireTest, GoldenVectorIsDeterministicAndBitPreserving) {
    auto descriptor = CompileOne(R"idl(
package p;
message Payload {
  int32 delta = 1;
  uint64 sequence = 2;
  fixed32 code = 3;
  float sample = 4;
  optional string label = 5 [max_bytes = 32];
  vector<uint32> values = 6 [max_capacity = 4];
}
)idl");
    ASSERT_NE(descriptor, nullptr);

    DynamicMessage message;
    ASSERT_TRUE(message.SetField(4, DynamicValue::Float32Bits(0x7fc01234u)).ok());
    ASSERT_TRUE(message.SetField(2, DynamicValue::Unsigned(300)).ok());
    ASSERT_TRUE(message.SetField(1, DynamicValue::Signed(-1)).ok());
    ASSERT_TRUE(message.SetField(3, DynamicValue::Unsigned(0x78563412u)).ok());
    auto label = DynamicValue::String("é");
    ASSERT_TRUE(label.ok());
    ASSERT_TRUE(message.SetField(5, std::move(*label)).ok());
    auto vector = std::make_shared<DynamicVector>();
    ASSERT_TRUE(vector->Add(DynamicValue::Unsigned(1)).ok());
    ASSERT_TRUE(vector->Add(DynamicValue::Unsigned(127)).ok());
    ASSERT_TRUE(vector->Add(DynamicValue::Unsigned(128)).ok());
    auto values = DynamicValue::Vector(vector);
    ASSERT_TRUE(values.ok());
    ASSERT_TRUE(message.SetField(6, std::move(*values)).ok());

    auto encoded = CanonicalWireCodec::Encode(*descriptor, message);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    EXPECT_EQ(Hex(*encoded),
              "080110ac021d12345678253412c07f2a02c3a9320503017f8001");

    auto decoded = CanonicalWireCodec::Decode(*descriptor, *encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_NE(decoded->FindField(4), nullptr);
    ASSERT_NE(decoded->FindField(4)->float32(), nullptr);
    EXPECT_EQ(decoded->FindField(4)->float32()->bits, 0x7fc01234u);
    auto encoded_again = CanonicalWireCodec::Encode(*descriptor, *decoded);
    ASSERT_TRUE(encoded_again.ok());
    EXPECT_EQ(*encoded_again, *encoded);

    auto prepared = PreparedCanonicalWireCodec::Create(descriptor);
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    descriptor.reset();
    const PreparedCanonicalWireCodec& codec = *prepared;
    for (size_t iteration = 0; iteration < 2; ++iteration) {
        auto prepared_encoded = codec.Encode(message);
        ASSERT_TRUE(prepared_encoded.ok())
            << prepared_encoded.status().ToString();
        EXPECT_EQ(*prepared_encoded, *encoded);

        auto prepared_decoded = codec.Decode(*prepared_encoded);
        ASSERT_TRUE(prepared_decoded.ok())
            << prepared_decoded.status().ToString();
        auto prepared_roundtrip = codec.Encode(*prepared_decoded);
        ASSERT_TRUE(prepared_roundtrip.ok())
            << prepared_roundtrip.status().ToString();
        EXPECT_EQ(*prepared_roundtrip, *encoded);
    }
}

TEST(CanonicalWireTest, RejectsMalformedCanonicalInputs) {
    auto descriptor = CompileOne(
        "package p; message M { optional uint32 id = 1; "
        "optional string text = 2 [max_bytes = 8]; }");
    ASSERT_NE(descriptor, nullptr);

    auto overlong = Bytes({0x08, 0x81, 0x00});
    auto result = CanonicalWireCodec::Decode(*descriptor, overlong);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kCorruption);

    auto mismatch = Bytes({0x0d, 0x01, 0x00, 0x00, 0x00});
    result = CanonicalWireCodec::Decode(*descriptor, mismatch);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kSchemaMismatch);

    auto invalid_utf8 = Bytes({0x12, 0x02, 0xc0, 0x80});
    result = CanonicalWireCodec::Decode(*descriptor, invalid_utf8);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kCorruption);

    auto unsorted = Bytes({0x12, 0x01, 0x61, 0x08, 0x01});
    result = CanonicalWireCodec::Decode(*descriptor, unsorted);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kCorruption);

    auto truncated = Bytes({0x12, 0x03, 0x61});
    result = CanonicalWireCodec::Decode(*descriptor, truncated);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kCorruption);
}

TEST(CanonicalWireTest, PreservesUnknownRawBytesAndBoundsThem) {
    auto descriptor = CompileOne(
        "package p; message M { optional uint32 known = 1; }");
    ASSERT_NE(descriptor, nullptr);
    const auto wire = Bytes({0x08, 0x07, 0x12, 0x02, 0x6f, 0x6b,
                             0x18, 0x01});
    auto decoded = CanonicalWireCodec::Decode(*descriptor, wire);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_EQ(decoded->unknown_fields().fields().size(), 2u);
    EXPECT_EQ(decoded->unknown_fields().byte_size(), 6u);
    EXPECT_EQ(Hex(decoded->unknown_fields().fields()[0].canonical_bytes()),
              "12026f6b");
    auto encoded = CanonicalWireCodec::Encode(*descriptor, *decoded);
    ASSERT_TRUE(encoded.ok());
    EXPECT_EQ(*encoded, wire);

    WireLimits limits;
    limits.unknown_fields.max_fields = 1;
    auto bounded = CanonicalWireCodec::Decode(*descriptor, wire, {}, limits);
    ASSERT_FALSE(bounded.ok());
    EXPECT_EQ(bounded.status().code(), StatusCode::kResourceExhausted);

    DynamicMessage reordered;
    ASSERT_TRUE(reordered.SetField(1, DynamicValue::Unsigned(7)).ok());
    const auto field3 = Bytes({0x18, 0x01});
    const auto field2 = Bytes({0x12, 0x02, 0x6f, 0x6b});
    ASSERT_TRUE(reordered.mutable_unknown_fields().Add(3, field3).ok());
    ASSERT_TRUE(reordered.mutable_unknown_fields().Add(2, field2).ok());
    auto sorted = CanonicalWireCodec::Encode(*descriptor, reordered);
    ASSERT_TRUE(sorted.ok());
    EXPECT_EQ(*sorted, wire);
}

TEST(CanonicalWireTest, RequiresExactAuthenticatedDescriptorClosure) {
    CompileOptions options;
    options.allow_implicit_schema_version = true;
    auto compiled = SchemaCompiler::Compile(
        "package p; message Child {} message Root { Child child = 1; }",
        options);
    auto extra_compiled = SchemaCompiler::Compile(
        "package q; message Extra {}", options);
    ASSERT_TRUE(compiled.ok() && extra_compiled.ok());
    const SchemaDescriptor* root = compiled->FindType("p.Root");
    ASSERT_NE(root, nullptr);

    DynamicMessage empty;
    auto missing = CanonicalWireCodec::Encode(*root, empty, {});
    ASSERT_FALSE(missing.ok());
    EXPECT_EQ(missing.status().code(), StatusCode::kSchemaMismatch);

    std::vector<std::shared_ptr<const SchemaDescriptor>> extra(
        compiled->types().begin(), compiled->types().end());
    extra.push_back(extra_compiled->types()[0]);
    auto rejected_extra = CanonicalWireCodec::Decode(*root, {}, extra);
    ASSERT_FALSE(rejected_extra.ok());
    EXPECT_EQ(rejected_extra.status().code(), StatusCode::kSchemaMismatch);

    std::vector<std::shared_ptr<const SchemaDescriptor>> duplicate(
        compiled->types().begin(), compiled->types().end());
    duplicate.push_back(compiled->types()[0]);
    auto rejected_duplicate = CanonicalWireCodec::Decode(*root, {}, duplicate);
    ASSERT_FALSE(rejected_duplicate.ok());
    EXPECT_EQ(rejected_duplicate.status().code(), StatusCode::kSchemaMismatch);
}

TEST(PreparedCanonicalWireCodecTest, RejectsInvalidCreationInputs) {
    static_assert(
        !std::is_default_constructible_v<PreparedCanonicalWireCodec>);

    auto null_root = PreparedCanonicalWireCodec::Create(nullptr);
    ASSERT_FALSE(null_root.ok());
    EXPECT_EQ(null_root.status().code(), StatusCode::kInvalidArgument);

    auto descriptor =
        CompileOne("package p; message M { optional uint32 id = 1; }");
    ASSERT_NE(descriptor, nullptr);

    WireLimits limits;
    limits.max_frame_bytes = 0;
    auto invalid_frame =
        PreparedCanonicalWireCodec::Create(descriptor, {}, limits);
    ASSERT_FALSE(invalid_frame.ok());
    EXPECT_EQ(invalid_frame.status().code(), StatusCode::kInvalidArgument);

    limits = WireLimits{};
    limits.max_depth = 0;
    auto invalid_depth =
        PreparedCanonicalWireCodec::Create(descriptor, {}, limits);
    ASSERT_FALSE(invalid_depth.ok());
    EXPECT_EQ(invalid_depth.status().code(), StatusCode::kInvalidArgument);

    CompileOptions options;
    options.allow_implicit_schema_version = true;
    auto compiled = SchemaCompiler::Compile(
        "package p; message Child {} message Root { Child child = 1; }",
        options);
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    std::shared_ptr<const SchemaDescriptor> root;
    for (const auto& type : compiled->types()) {
        if (type->aggregate().full_name() == "p.Root") {
            root = type;
            break;
        }
    }
    ASSERT_NE(root, nullptr);

    auto invalid_closure = PreparedCanonicalWireCodec::Create(root, {});
    ASSERT_FALSE(invalid_closure.ok());
    EXPECT_EQ(invalid_closure.status().code(), StatusCode::kSchemaMismatch);
}

TEST(DynamicMessageTest, ReserveAndClearRetainReusableMessageSemantics) {
    DynamicMessage message;
    ASSERT_TRUE(message.ReserveFields(8).ok());
    ASSERT_TRUE(message.SetField(2, DynamicValue::Unsigned(9)).ok());
    const auto unknown = Bytes({0x08, 0x01});
    ASSERT_TRUE(message.mutable_unknown_fields().Add(1, unknown).ok());

    message.Clear();
    EXPECT_TRUE(message.fields().empty());
    EXPECT_TRUE(message.unknown_fields().fields().empty());
    EXPECT_EQ(message.unknown_fields().byte_size(), 0u);

    ASSERT_TRUE(message.SetField(1, DynamicValue::Signed(-7)).ok());
    ASSERT_NE(message.FindField(1), nullptr);
    EXPECT_EQ(message.FindField(1)->signed_integer()->value, -7);
}

TEST(CanonicalWireTest, RejectsRepeatedKnownAndPreservesRepeatedUnknown) {
    auto descriptor = CompileOne(
        "package p; message M { optional uint32 known = 2; }");
    ASSERT_NE(descriptor, nullptr);

    const auto repeated_unknown =
        Bytes({0x08, 0x01, 0x08, 0x02, 0x10, 0x03});
    auto decoded = CanonicalWireCodec::Decode(*descriptor, repeated_unknown);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_EQ(decoded->unknown_fields().fields().size(), 2u);
    EXPECT_EQ(Hex(decoded->unknown_fields().fields()[0].canonical_bytes()),
              "0801");
    EXPECT_EQ(Hex(decoded->unknown_fields().fields()[1].canonical_bytes()),
              "0802");
    auto roundtrip = CanonicalWireCodec::Encode(*descriptor, *decoded);
    ASSERT_TRUE(roundtrip.ok()) << roundtrip.status().ToString();
    EXPECT_EQ(*roundtrip, repeated_unknown);

    const auto repeated_known = Bytes({0x10, 0x01, 0x10, 0x02});
    auto rejected = CanonicalWireCodec::Decode(*descriptor, repeated_known);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kCorruption);
}

TEST(PreparedCanonicalWireCodecTest,
     ReuseApiDirectlyMergesKnownAndStableUnknownFields) {
    auto descriptor = CompileOne(R"idl(
package p;
message M {
  optional uint32 known = 2;
  optional string text = 4 [max_bytes = 8];
  optional bytes blob = 5 [max_bytes = 8];
}
)idl");
    ASSERT_NE(descriptor, nullptr);
    auto prepared = PreparedCanonicalWireCodec::Create(descriptor);
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();

    DynamicMessage message;
    ASSERT_TRUE(message.ReserveFields(3).ok());
    ASSERT_TRUE(message.SetField(2, DynamicValue::Unsigned(9)).ok());
    auto text = DynamicValue::String("xy");
    ASSERT_TRUE(text.ok());
    ASSERT_TRUE(message.SetField(4, std::move(*text)).ok());
    const auto blob_bytes = Bytes({0x00, 0xff});
    auto blob = DynamicValue::Bytes(blob_bytes);
    ASSERT_TRUE(blob.ok());
    ASSERT_TRUE(message.SetField(5, std::move(*blob)).ok());
    const auto unknown3_first = Bytes({0x18, 0x01});
    const auto unknown1 = Bytes({0x08, 0x07});
    const auto unknown3_second = Bytes({0x18, 0x02});
    ASSERT_TRUE(
        message.mutable_unknown_fields().Add(3, unknown3_first).ok());
    ASSERT_TRUE(message.mutable_unknown_fields().Add(1, unknown1).ok());
    ASSERT_TRUE(
        message.mutable_unknown_fields().Add(3, unknown3_second).ok());

    CanonicalWireScratch scratch;
    std::vector<std::byte> output;
    output.reserve(128);
    const size_t reserved_capacity = output.capacity();
    ASSERT_TRUE(prepared->EncodeInto(message, scratch, output).ok());
    EXPECT_EQ(Hex(output), "0807100918011802220278792a0200ff");
    EXPECT_EQ(output.capacity(), reserved_capacity);

    CanonicalWireScratch generic_scratch;
    std::vector<std::byte> generic_output;
    ASSERT_TRUE(CanonicalWireCodec::EncodeInto(
                    *descriptor, message, generic_scratch, generic_output)
                    .ok());
    EXPECT_EQ(generic_output, output);
    DynamicMessage generic_decoded;
    ASSERT_TRUE(CanonicalWireCodec::DecodeInto(
                    *descriptor, generic_output, generic_scratch,
                    generic_decoded)
                    .ok());
    EXPECT_EQ(generic_decoded.unknown_fields().fields().size(), 3u);

    DynamicMessage decoded;
    ASSERT_TRUE(decoded.SetField(2, DynamicValue::Unsigned(999)).ok());
    const auto stale_unknown = Bytes({0x28, 0x01});
    ASSERT_TRUE(decoded.mutable_unknown_fields().Add(5, stale_unknown).ok());
    ASSERT_TRUE(prepared->DecodeInto(output, scratch, decoded).ok());
    ASSERT_NE(decoded.FindField(2), nullptr);
    EXPECT_EQ(decoded.FindField(2)->unsigned_integer()->value, 9u);
    ASSERT_EQ(decoded.unknown_fields().fields().size(), 3u);
    EXPECT_EQ(decoded.unknown_fields().fields()[0].field_id(), 1u);
    EXPECT_EQ(decoded.unknown_fields().fields()[1].field_id(), 3u);
    EXPECT_EQ(decoded.unknown_fields().fields()[2].field_id(), 3u);

    ASSERT_TRUE(prepared->EncodeInto(decoded, scratch, output).ok());
    EXPECT_EQ(Hex(output), "0807100918011802220278792a0200ff");
    const auto malformed = Bytes({0x10, 0x81, 0x00});
    Status status = prepared->DecodeInto(malformed, scratch, decoded);
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kCorruption);
    EXPECT_TRUE(decoded.fields().empty());
    EXPECT_TRUE(decoded.unknown_fields().fields().empty());

    DynamicMessage wrong_limits(UnknownFieldLimits{1, 1});
    status = prepared->DecodeInto(output, scratch, wrong_limits);
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
}

TEST(PreparedCanonicalWireCodecTest, BytesViewMatchesOwnedBytesCanonicalOutput) {
    auto descriptor = CompileOne(R"idl(
package p;
message M {
  optional uint32 known = 1;
  bytes blob = 2 [max_bytes = 64];
}
)idl");
    ASSERT_NE(descriptor, nullptr);
    auto prepared = PreparedCanonicalWireCodec::Create(descriptor);
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();

    const auto blob_bytes = Bytes({0x00, 0xff, 0x7f, 0x80});
    DynamicMessage owned_message;
    ASSERT_TRUE(owned_message.SetField(1, DynamicValue::Unsigned(9)).ok());
    auto owned_blob = DynamicValue::Bytes(blob_bytes);
    ASSERT_TRUE(owned_blob.ok());
    ASSERT_TRUE(owned_message.SetField(2, std::move(*owned_blob)).ok());

    DynamicMessage view_message;
    ASSERT_TRUE(view_message.SetField(1, DynamicValue::Unsigned(9)).ok());
    ASSERT_TRUE(view_message.SetField(2, DynamicValue::BytesView(blob_bytes)).ok());

    CanonicalWireScratch scratch;
    std::vector<std::byte> owned_output;
    std::vector<std::byte> view_output;
    ASSERT_TRUE(prepared->EncodeInto(owned_message, scratch, owned_output).ok());
    ASSERT_TRUE(prepared->EncodeInto(view_message, scratch, view_output).ok());
    EXPECT_EQ(view_output, owned_output);

    std::vector<std::byte> generic_output;
    ASSERT_TRUE(CanonicalWireCodec::EncodeInto(
                    *descriptor, view_message, scratch, generic_output)
                    .ok());
    EXPECT_EQ(generic_output, owned_output);
}

TEST(CanonicalWireScratchTest,
     FailedPartialEncodeDoesNotLeakUnknownOrderIntoNestedReuse) {
    CompileOptions options;
    options.allow_implicit_schema_version = true;
    auto compiled = SchemaCompiler::Compile(R"idl(
package p;
message Leaf { uint32 value = 2; }
message Child { Leaf leaf = 2; }
message Root {
  uint32 known = 2;
  Child child = 4;
}
)idl",
                                            options);
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    std::shared_ptr<const SchemaDescriptor> root;
    for (const auto& type : compiled->types()) {
        if (type->aggregate().full_name() == "p.Root") root = type;
    }
    ASSERT_NE(root, nullptr);
    auto prepared = PreparedCanonicalWireCodec::Create(root, compiled->types());
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();

    DynamicMessage failing;
    ASSERT_TRUE(failing.SetField(2, DynamicValue::Signed(7)).ok());
    const auto failing_unknown3 = Bytes({0x18, 0x03});
    const auto failing_unknown1 = Bytes({0x08, 0x01});
    ASSERT_TRUE(
        failing.mutable_unknown_fields().Add(3, failing_unknown3).ok());
    ASSERT_TRUE(
        failing.mutable_unknown_fields().Add(1, failing_unknown1).ok());

    auto leaf = std::make_shared<DynamicMessage>();
    ASSERT_TRUE(leaf->SetField(2, DynamicValue::Unsigned(42)).ok());
    const auto leaf_unknown4 = Bytes({0x20, 0x1c});
    const auto leaf_unknown1 = Bytes({0x08, 0x15});
    ASSERT_TRUE(
        leaf->mutable_unknown_fields().Add(4, leaf_unknown4).ok());
    ASSERT_TRUE(
        leaf->mutable_unknown_fields().Add(1, leaf_unknown1).ok());
    auto leaf_value = DynamicValue::Message(leaf);
    ASSERT_TRUE(leaf_value.ok());

    auto child = std::make_shared<DynamicMessage>();
    ASSERT_TRUE(child->SetField(2, std::move(*leaf_value)).ok());
    const auto child_unknown3 = Bytes({0x18, 0x0d});
    const auto child_unknown1 = Bytes({0x08, 0x0b});
    ASSERT_TRUE(
        child->mutable_unknown_fields().Add(3, child_unknown3).ok());
    ASSERT_TRUE(
        child->mutable_unknown_fields().Add(1, child_unknown1).ok());
    auto child_value = DynamicValue::Message(child);
    ASSERT_TRUE(child_value.ok());

    DynamicMessage succeeding;
    ASSERT_TRUE(succeeding.SetField(2, DynamicValue::Unsigned(7)).ok());
    ASSERT_TRUE(succeeding.SetField(4, std::move(*child_value)).ok());
    const auto success_unknown6 = Bytes({0x30, 0x06});
    const auto success_unknown1 = Bytes({0x08, 0x09});
    const auto success_unknown3 = Bytes({0x18, 0x03});
    ASSERT_TRUE(succeeding.mutable_unknown_fields()
                    .Add(6, success_unknown6)
                    .ok());
    ASSERT_TRUE(succeeding.mutable_unknown_fields()
                    .Add(1, success_unknown1)
                    .ok());
    ASSERT_TRUE(succeeding.mutable_unknown_fields()
                    .Add(3, success_unknown3)
                    .ok());

    constexpr std::string_view kExpected =
        "080910071803220c080b12060815102a201c180d3006";
    const auto exercise = [&](auto&& encode_into) {
        CanonicalWireScratch scratch;
        std::vector<std::byte> output = Bytes({0xff, 0xff});
        output.reserve(128);

        // ID 1 is emitted before known field 2's tag; encoding then fails on
        // the uint32/Signed dynamic-type mismatch after output is non-empty.
        Status status = encode_into(failing, scratch, output);
        ASSERT_FALSE(status.ok());
        EXPECT_EQ(status.code(), StatusCode::kSchemaMismatch);
        EXPECT_TRUE(output.empty());

        status = encode_into(succeeding, scratch, output);
        ASSERT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(Hex(output), kExpected);
    };

    exercise([&](const DynamicMessage& message, CanonicalWireScratch& scratch,
                 std::vector<std::byte>& output) {
        return CanonicalWireCodec::EncodeInto(
            *root, message, scratch, output, compiled->types());
    });
    exercise([&](const DynamicMessage& message, CanonicalWireScratch& scratch,
                 std::vector<std::byte>& output) {
        return prepared->EncodeInto(message, scratch, output);
    });
}

TEST(PreparedCanonicalWireCodecTest, PlansCoverNestedDescriptorClosure) {
    CompileOptions options;
    options.allow_implicit_schema_version = true;
    auto compiled = SchemaCompiler::Compile(R"idl(
package p;
message Child { string value = 1 [max_bytes = 8]; }
message Root { Child child = 1; }
)idl",
                                            options);
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    std::shared_ptr<const SchemaDescriptor> root;
    for (const auto& type : compiled->types()) {
        if (type->aggregate().full_name() == "p.Root") root = type;
    }
    ASSERT_NE(root, nullptr);
    auto prepared = PreparedCanonicalWireCodec::Create(root, compiled->types());
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();

    auto child = std::make_shared<DynamicMessage>();
    auto text = DynamicValue::String("nested");
    ASSERT_TRUE(text.ok());
    ASSERT_TRUE(child->SetField(1, std::move(*text)).ok());
    auto child_value = DynamicValue::Message(child);
    ASSERT_TRUE(child_value.ok());
    DynamicMessage message;
    ASSERT_TRUE(message.SetField(1, std::move(*child_value)).ok());

    CanonicalWireScratch scratch;
    std::vector<std::byte> output;
    ASSERT_TRUE(prepared->EncodeInto(message, scratch, output).ok());
    EXPECT_EQ(Hex(output), "0a080a066e6573746564");
    DynamicMessage decoded;
    ASSERT_TRUE(prepared->DecodeInto(output, scratch, decoded).ok());
    ASSERT_NE(decoded.FindField(1), nullptr);
    ASSERT_NE(decoded.FindField(1)->message(), nullptr);
    const DynamicValue* nested =
        decoded.FindField(1)->message()->value->FindField(1);
    ASSERT_NE(nested, nullptr);
    ASSERT_NE(nested->string(), nullptr);
    EXPECT_EQ(nested->string()->value, "nested");
}

TEST(PreparedCanonicalWireCodecTest,
     CopiedHandlesAreConcurrentWithCallerOwnedScratch) {
    auto descriptor = CompileOne(R"idl(
package p;
message M {
  uint32 sequence = 1;
  string label = 2 [max_bytes = 16];
}
)idl");
    ASSERT_NE(descriptor, nullptr);
    auto prepared = PreparedCanonicalWireCodec::Create(descriptor);
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    const PreparedCanonicalWireCodec codec = *prepared;

    DynamicMessage sample;
    ASSERT_TRUE(sample.SetField(1, DynamicValue::Unsigned(300)).ok());
    auto sample_label = DynamicValue::String("worker");
    ASSERT_TRUE(sample_label.ok());
    ASSERT_TRUE(sample.SetField(2, std::move(*sample_label)).ok());
    auto expected = codec.Encode(sample);
    ASSERT_TRUE(expected.ok()) << expected.status().ToString();

    constexpr size_t kThreadCount = 6;
    constexpr size_t kIterations = 100;
    std::atomic<bool> all_ok = true;
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (size_t thread_index = 0; thread_index < kThreadCount; ++thread_index) {
        PreparedCanonicalWireCodec copied = codec;
        threads.emplace_back(
            [copied, expected_bytes = *expected, &all_ok]() mutable {
                DynamicMessage message;
                if (!message.SetField(1, DynamicValue::Unsigned(300)).ok()) {
                    all_ok.store(false, std::memory_order_relaxed);
                    return;
                }
                auto label = DynamicValue::String("worker");
                if (!label.ok() ||
                    !message.SetField(2, std::move(*label)).ok()) {
                    all_ok.store(false, std::memory_order_relaxed);
                    return;
                }
                CanonicalWireScratch scratch;
                std::vector<std::byte> output;
                output.reserve(expected_bytes.size());
                DynamicMessage decoded;
                for (size_t iteration = 0; iteration < kIterations;
                     ++iteration) {
                    if (!copied.EncodeInto(message, scratch, output).ok() ||
                        output != expected_bytes ||
                        !copied.DecodeInto(output, scratch, decoded).ok()) {
                        all_ok.store(false, std::memory_order_relaxed);
                        return;
                    }
                    const DynamicValue* sequence = decoded.FindField(1);
                    const DynamicValue* decoded_label = decoded.FindField(2);
                    if (sequence == nullptr ||
                        sequence->unsigned_integer() == nullptr ||
                        sequence->unsigned_integer()->value != 300 ||
                        decoded_label == nullptr ||
                        decoded_label->string() == nullptr ||
                        decoded_label->string()->value != "worker") {
                        all_ok.store(false, std::memory_order_relaxed);
                        return;
                    }
                }
            });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_TRUE(all_ok.load(std::memory_order_relaxed));
}

TEST(CanonicalWireTest, EnforcesContainerAndDepthLimitsBeforeAllocation) {
    auto vector_descriptor = CompileOne(
        "package p; message M { optional vector<uint32> values = 1 "
        "[max_capacity = 2]; }");
    ASSERT_NE(vector_descriptor, nullptr);
    auto too_many = Bytes({0x0a, 0x01, 0x03});
    auto vector_result =
        CanonicalWireCodec::Decode(*vector_descriptor, too_many);
    ASSERT_FALSE(vector_result.ok());
    EXPECT_EQ(vector_result.status().code(), StatusCode::kResourceExhausted);

    CompileOptions compile_options;
    compile_options.allow_implicit_schema_version = true;
    auto compiled = SchemaCompiler::Compile(R"idl(
package p;
message A { B b = 1; }
message B { C c = 1; }
message C { optional uint32 value = 1; }
)idl",
                                            compile_options);
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    auto c = std::make_shared<DynamicMessage>();
    ASSERT_TRUE(c->SetField(1, DynamicValue::Unsigned(1)).ok());
    auto c_value = DynamicValue::Message(c);
    ASSERT_TRUE(c_value.ok());
    auto b = std::make_shared<DynamicMessage>();
    ASSERT_TRUE(b->SetField(1, std::move(*c_value)).ok());
    auto b_value = DynamicValue::Message(b);
    ASSERT_TRUE(b_value.ok());
    DynamicMessage a;
    ASSERT_TRUE(a.SetField(1, std::move(*b_value)).ok());

    WireLimits limits;
    limits.max_depth = 1;
    const SchemaDescriptor* a_descriptor = compiled->FindType("p.A");
    ASSERT_NE(a_descriptor, nullptr);
    auto depth = CanonicalWireCodec::Encode(*a_descriptor, a,
                                            compiled->types(), limits);
    ASSERT_FALSE(depth.ok());
    EXPECT_EQ(depth.status().code(), StatusCode::kResourceExhausted);
}

}  // namespace
}  // namespace mino::schema
