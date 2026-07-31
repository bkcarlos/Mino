// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/parser.h"

#include <gtest/gtest.h>

#include <string_view>

#include "mino/common/status.h"

namespace mino::schema {
namespace {

static_assert(noexcept(Parser::Parse(std::string_view{})));

TEST(ParserTest, ParsesCompleteV1ExampleIntoAst) {
    constexpr std::string_view kIdl = R"idl(
syntax = "v1";
package autonomous.sensing;
import "common/point.mino";
option schema_version = "2.1";

struct Point3D {
    float x = 1;
    float y = 2;
    float z = 3;
}

message SensorFrame {
    required uint32 frame_id = 1;
    optional string device_name = 2
        [max_bytes = 64, default = "unknown\n", snapshot_key];
    vector<autonomous.Point3D> points = 3 [max_capacity = 100];
    bytes payload = 5 [max_bytes = 4096];
    reserved 4, 10 to 15;
}
)idl";

    auto result = Parser::Parse(kIdl);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    const SchemaFile& file = *result;
    ASSERT_TRUE(file.syntax.has_value());
    EXPECT_EQ(file.syntax->version, "v1");
    ASSERT_TRUE(file.package.has_value());
    EXPECT_EQ(file.package->name, "autonomous.sensing");
    ASSERT_EQ(file.imports.size(), 1u);
    EXPECT_EQ(file.imports[0].path, "common/point.mino");
    ASSERT_EQ(file.options.size(), 1u);
    EXPECT_EQ(file.options[0].name, "schema_version");
    EXPECT_EQ(file.options[0].value.kind, LiteralKind::kString);
    EXPECT_EQ(file.options[0].value.value, "2.1");

    ASSERT_EQ(file.aggregates.size(), 2u);
    EXPECT_EQ(file.aggregates[0].kind, AggregateKind::kStruct);
    EXPECT_EQ(file.aggregates[0].name, "Point3D");
    ASSERT_EQ(file.aggregates[0].fields.size(), 3u);

    const AggregateDeclaration& message = file.aggregates[1];
    EXPECT_EQ(message.kind, AggregateKind::kMessage);
    EXPECT_EQ(message.name, "SensorFrame");
    ASSERT_EQ(message.fields.size(), 4u);
    EXPECT_EQ(message.fields[0].cardinality, FieldCardinality::kRequired);
    EXPECT_EQ(message.fields[0].type.kind, TypeKind::kScalar);
    EXPECT_EQ(message.fields[0].type.scalar, ScalarType::kUint32);
    EXPECT_EQ(message.fields[0].id, 1u);

    EXPECT_EQ(message.fields[1].cardinality, FieldCardinality::kOptional);
    ASSERT_EQ(message.fields[1].annotations.size(), 3u);
    EXPECT_EQ(message.fields[1].annotations[0].name, "max_bytes");
    ASSERT_TRUE(message.fields[1].annotations[0].value.has_value());
    EXPECT_EQ(message.fields[1].annotations[0].value->value, "64");
    EXPECT_EQ(message.fields[1].annotations[1].name, "default");
    ASSERT_TRUE(message.fields[1].annotations[1].value.has_value());
    EXPECT_EQ(message.fields[1].annotations[1].value->value, "unknown\n");
    EXPECT_EQ(message.fields[1].annotations[2].name, "snapshot_key");
    EXPECT_FALSE(message.fields[1].annotations[2].value.has_value());

    EXPECT_EQ(message.fields[2].type.kind, TypeKind::kVector);
    ASSERT_NE(message.fields[2].type.element_type, nullptr);
    EXPECT_EQ(message.fields[2].type.element_type->kind,
              TypeKind::kUserDefined);
    EXPECT_EQ(message.fields[2].type.element_type->name,
              "autonomous.Point3D");

    ASSERT_EQ(message.reserved.size(), 1u);
    ASSERT_EQ(message.reserved[0].ranges.size(), 2u);
    EXPECT_EQ(message.reserved[0].ranges[0].first, 4u);
    EXPECT_EQ(message.reserved[0].ranges[0].last, 4u);
    EXPECT_EQ(message.reserved[0].ranges[1].first, 10u);
    EXPECT_EQ(message.reserved[0].ranges[1].last, 15u);
    EXPECT_EQ(message.source.begin.line, 13u);
}

TEST(ParserTest, RequiresExplicitFieldId) {
    auto result = Parser::Parse(R"idl(message Broken {
  uint32 missing;
})idl");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(result.status().message().find("expected '='"),
              std::string_view::npos);
    EXPECT_NE(result.status().message().find("line 2"),
              std::string_view::npos);
}

TEST(ParserTest, RejectsMalformedReservedRanges) {
    auto missing_end =
        Parser::Parse("message Broken { reserved 4, 10 to; }");
    ASSERT_FALSE(missing_end.ok());
    EXPECT_EQ(missing_end.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(missing_end.status().message().find("expected integer literal"),
              std::string_view::npos);

    auto trailing_comma = Parser::Parse("message Broken { reserved 4,; }");
    ASSERT_FALSE(trailing_comma.ok());
    EXPECT_EQ(trailing_comma.status().code(), StatusCode::kInvalidArgument);
}

TEST(ParserTest, LeavesSemanticRulesForValidator) {
    auto result = Parser::Parse(R"idl(
message DeferredChecks {
    uint32 first = 1 [unknown_annotation = 7];
    uint32 duplicate = 1;
    reserved 1, 9 to 3;
}
)idl");
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    ASSERT_EQ(result->aggregates.size(), 1u);
    EXPECT_EQ(result->aggregates[0].fields.size(), 2u);
}

TEST(ParserTest, EnforcesParserResourceLimits) {
    ParserOptions options;
    options.max_fields = 1;
    auto fields = Parser::Parse(
        "message M { uint32 a = 1; uint32 b = 2; }", options);
    ASSERT_FALSE(fields.ok());
    EXPECT_EQ(fields.status().code(), StatusCode::kResourceExhausted);

    options = ParserOptions{};
    options.max_nesting_depth = 1;
    auto depth = Parser::Parse(
        "message M { vector<uint32> values = 1 [max_capacity = 1]; }",
        options);
    ASSERT_FALSE(depth.ok());
    EXPECT_EQ(depth.status().code(), StatusCode::kResourceExhausted);

    options = ParserOptions{};
    options.max_reserved_ranges = 1;
    auto ranges = Parser::Parse("message M { reserved 1, 2; }", options);
    ASSERT_FALSE(ranges.ok());
    EXPECT_EQ(ranges.status().code(), StatusCode::kResourceExhausted);

    options = ParserOptions{};
    options.lexer.max_input_bytes = 4;
    auto input = Parser::Parse("message M {}", options);
    ASSERT_FALSE(input.ok());
    EXPECT_EQ(input.status().code(), StatusCode::kResourceExhausted);
}

TEST(ParserTest, RejectsUnexpectedTopLevelTokenAndUnclosedMessage) {
    auto bad_token = Parser::Parse("@");
    ASSERT_FALSE(bad_token.ok());
    EXPECT_EQ(bad_token.status().code(), StatusCode::kInvalidArgument);

    auto unclosed = Parser::Parse("message M { uint32 id = 1;");
    ASSERT_FALSE(unclosed.ok());
    EXPECT_EQ(unclosed.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(unclosed.status().message().find("expected '}'"),
              std::string_view::npos);
}

}  // namespace
}  // namespace mino::schema
