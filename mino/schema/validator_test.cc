// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/validator.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mino/common/status.h"
#include "mino/schema/parser.h"

namespace mino::schema {
namespace {

ValidatorOptions LegacyFixtureOptions() {
    ValidatorOptions options;
    options.allow_implicit_schema_version = true;
    return options;
}

Result<ValidatedSchema> Validate(
    std::string_view idl,
    const ValidatorOptions& options = LegacyFixtureOptions()) {
    auto ast = Parser::Parse(idl);
    if (!ast.ok()) return ast.status();
    return SemanticValidator::Validate(*ast, {}, options);
}

TEST(SemanticValidatorTest, BuildsResolvedStronglyTypedModel) {
    auto result = Validate(R"idl(
syntax = "v1";
package demo.geometry;
option schema_version = "2.7";
struct Point { float x = 1 [default = 0.1]; }
message Frame {
  required uint32 id = 1 [snapshot_key];
  optional string label = 2 [default = "ok", max_bytes = 8];
  vector<Point> points = 3 [max_capacity = 4];
  reserved 4, 9 to 12;
}
)idl");
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_EQ(result->schema_version(), (2u << 16) | 7u);
    ASSERT_EQ(result->aggregates().size(), 2u);
    const AggregateDescriptor& frame = result->aggregates()[1];
    ASSERT_EQ(frame.fields().size(), 3u);
    EXPECT_EQ(frame.fields()[1].constraints().max_bytes(), 8u);
    ASSERT_TRUE(frame.fields()[1].default_value().has_value());
    EXPECT_EQ(frame.fields()[1].default_value()->canonical_value(), "ok");
    EXPECT_EQ(frame.fields()[2].type().element_type()->name(),
              "demo.geometry.Point");
    EXPECT_TRUE(frame.IsReserved(10));
}

TEST(SemanticValidatorTest, EnforcesFieldAndReservedIdentityRules) {
    for (std::string_view idl : {
             "message M { uint32 a = 0; }",
             "message M { uint32 a = 536870912; }",
             "message M { uint32 a = 1; uint32 b = 1; }",
             "message M { uint32 a = 1; uint32 a = 2; }",
             "message M { uint32 a = 1; reserved 1; }",
             "message M { reserved 3 to 2; }",
             "message M { reserved 1 to 3, 3 to 4; }",
         }) {
        auto result = Validate(idl);
        EXPECT_FALSE(result.ok()) << idl;
        if (!result.ok()) {
            EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
        }
    }

    auto maximum = Validate("message M { uint32 a = 536870911; }");
    EXPECT_TRUE(maximum.ok()) << maximum.status().ToString();
}

TEST(SemanticValidatorTest, ResolvesNamesAndRejectsTypeCycles) {
    auto resolved = Validate(R"idl(
package p;
struct A { B b = 1; }
struct B { uint32 value = 1; }
)idl");
    ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
    EXPECT_EQ(resolved->aggregates()[0].fields()[0].type().name(), "p.B");

    for (std::string_view idl : {
             "message M { Missing value = 1; }",
             "message M { M self = 1; }",
             "message A { B b = 1; } message B { A a = 1; }",
             "message A {} message A {}",
             "message vector {}",
         }) {
        auto result = Validate(idl);
        EXPECT_FALSE(result.ok()) << idl;
    }
}

TEST(SemanticValidatorTest, StructRejectsDirectAndTransitiveDynamicFields) {
    for (std::string_view idl : {
             "struct S { string text = 1 [max_bytes = 8]; }",
             "struct S { vector<uint32> values = 1 [max_capacity = 8]; }",
             "message M { uint32 id = 1; } struct S { M child = 1; }",
             R"idl(
struct Inner { bytes data = 1 [max_bytes = 8]; }
struct Outer { Inner inner = 1; }
)idl",
         }) {
        auto result = Validate(idl);
        EXPECT_FALSE(result.ok()) << idl;
        if (!result.ok()) {
            EXPECT_NE(result.status().message().find("dynamic"),
                      std::string_view::npos);
        }
    }
}

TEST(SemanticValidatorTest, ValidatesAnnotationsDefaultsAndBounds) {
    for (std::string_view idl : {
             "message M { string s = 1; }",
             "message M { bytes b = 1; }",
             "message M { vector<uint32> v = 1; }",
             "message M { uint32 n = 1 [max_bytes = 4]; }",
             "message M { uint32 n = 1 [max_capacity = 4]; }",
             "message M { uint32 n = 1 [unknown = 4]; }",
             "message M { uint32 n = 1 [snapshot_key = true]; }",
             "message M { uint32 n = 1 [default = -1]; }",
             "message M { bool b = 1 [default = 1]; }",
             "message M { string s = 1 [max_bytes = 1, default = \"xx\"]; }",
             "message M { string s = 1 [max_bytes = 4, max_bytes = 5]; }",
         }) {
        auto result = Validate(idl);
        EXPECT_FALSE(result.ok()) << idl;
    }

    auto normalized = Validate(R"idl(
message M {
  int32 a = 1 [default = +0007];
  float b = 2 [default = 1e-1];
  string c = 3 [max_bytes = 4, default = "x"];
}
)idl");
    ASSERT_TRUE(normalized.ok()) << normalized.status().ToString();
    EXPECT_EQ(normalized->aggregates()[0].fields()[0]
                  .default_value()
                  ->canonical_value(),
              "7");
    EXPECT_EQ(normalized->aggregates()[0].fields()[1]
                  .default_value()
                  ->canonical_value(),
              "0x3dcccccd");
}

TEST(SemanticValidatorTest, ParsesSchemaVersionAndEnforcesResources) {
    auto version = Validate("option schema_version = \"65535.65535\"; message M {}");
    ASSERT_TRUE(version.ok()) << version.status().ToString();
    EXPECT_EQ(version->schema_version(), 0xffffffffu);

    for (std::string_view idl : {
             "option schema_version = 1.2; message M {}",
             "option schema_version = \"1\"; message M {}",
             "option schema_version = \"65536.0\"; message M {}",
             "option schema_version = \"1.0\"; option schema_version = \"1.1\"; message M {}",
             "option other = 1; message M {}",
         }) {
        auto result = Validate(idl);
        EXPECT_FALSE(result.ok()) << idl;
    }

    ValidatorOptions options = LegacyFixtureOptions();
    options.max_total_capacity = 7;
    auto capacity = Validate(
        "message M { string s = 1 [max_bytes = 8]; }", options);
    ASSERT_FALSE(capacity.ok());
    EXPECT_EQ(capacity.status().code(), StatusCode::kResourceExhausted);
}

TEST(SemanticValidatorTest, RequiresExplicitSchemaVersionByDefault) {
    auto ast = Parser::Parse("message M {}");
    ASSERT_TRUE(ast.ok());
    auto strict = SemanticValidator::Validate(*ast);
    ASSERT_FALSE(strict.ok());
    EXPECT_EQ(strict.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(strict.status().message().find("schema_version"),
              std::string_view::npos);
}

TEST(SemanticValidatorTest, SeparatesUtf8StringAndArbitraryBytesDefaults) {
    auto bytes = Validate(R"idl(
message M { bytes value = 1 [max_bytes = 1, default = "\xff"]; }
)idl");
    ASSERT_TRUE(bytes.ok()) << bytes.status().ToString();
    ASSERT_TRUE(bytes->aggregates()[0].fields()[0].default_value().has_value());
    EXPECT_EQ(bytes->aggregates()[0].fields()[0].default_value()->kind(),
              DefaultValue::Kind::kBytes);

    auto string = Validate(R"idl(
message M { string value = 1 [max_bytes = 1, default = "\xff"]; }
)idl");
    ASSERT_FALSE(string.ok());
    EXPECT_NE(string.status().message().find("UTF-8"),
              std::string_view::npos);
}

TEST(SemanticValidatorTest, ChecksRecursiveCapacityMultiplication) {
    constexpr std::string_view kIdl = R"idl(
message M { vector<vector<uint64>> values = 1 [max_capacity = 4]; }
)idl";
    ValidatorOptions options = LegacyFixtureOptions();
    options.unknown_fields.max_bytes = 8;
    options.unknown_fields.max_fields = 2;
    options.max_total_capacity = 311;
    auto rejected = Validate(kIdl, options);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kResourceExhausted);

    options.max_total_capacity = 312;
    auto accepted = Validate(kIdl, options);
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
}

}  // namespace
}  // namespace mino::schema
