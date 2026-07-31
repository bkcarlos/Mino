// Copyright 2026 The Mino Authors

#include "mino/schema/wire.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
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
