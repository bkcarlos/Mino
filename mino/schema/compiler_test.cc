// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/compiler.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <string_view>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/lexer.h"

namespace mino::schema {
namespace {

static_assert(noexcept(SchemaCompiler::Compile(std::string_view{})));

const SchemaDescriptor& OnlyType(const CompiledSchema& schema) {
    EXPECT_EQ(schema.types().size(), 1u);
    return *schema.types()[0];
}

TEST(CanonicalTest, Sha256MatchesPublishedVector) {
    EXPECT_EQ(DigestHex(Sha256("abc")),
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(SchemaCompilerTest, ProducesCanonicalGoldenVectorAndIdentity) {
    auto result = SchemaCompiler::Compile(R"idl(
syntax = "v1";
package demo;
option schema_version = "2.1";
message Frame {
  optional string renamed_any_time = 2
      [snapshot_key, default = "x\n", max_bytes = 8];
  required uint32 id = 1;
  reserved 3, 4 to 5;
}
)idl");
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    const SchemaDescriptor& descriptor = OnlyType(*result);

    std::string expected("mino-canonical-v1\0", 18);
    expected += "type:message:10:demo.Frame\n";
    expected += "1:uint32:cardinality=required:-\n";
    expected += "2:string:cardinality=optional,max_bytes=8,snapshot_key:string(2):x\n\n";
    expected += "reserved:3-5\n";
    EXPECT_EQ(descriptor.canonical_schema(), expected);
    EXPECT_EQ(DigestHex(descriptor.identity().canonical_digest()),
              "fcebba86f36fd67e357bd93095ee98069b3c70b251e07070b7b93db2bfa6ff5e");
    EXPECT_EQ(descriptor.identity().short_id(), 9139615585523133436ull);
    EXPECT_EQ(descriptor.identity().schema_version(), (2u << 16) | 1u);
    EXPECT_EQ(descriptor.identity().layout_version(), 1u);
}

TEST(SchemaCompilerTest, ExcludesNamesVersionsImportsAndSourceOrder) {
    auto first = SchemaCompiler::Compile(R"idl(
package p;
import "one/path.mino";
option schema_version = "1.0";
message M {
  optional string old_name = 2 [max_bytes = 8, snapshot_key];
  uint32 first = 1;
}
)idl");
    auto second = SchemaCompiler::Compile(R"idl(
package p;
import "different/path.mino";
option schema_version = "9.7";
message M {
  uint32 renamed_first = 1;
  optional string new_name = 2 [snapshot_key, max_bytes = 8];
}
)idl");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(OnlyType(*first).identity().canonical_digest(),
              OnlyType(*second).identity().canonical_digest());
    EXPECT_NE(OnlyType(*first).identity().schema_version(),
              OnlyType(*second).identity().schema_version());
}

TEST(SchemaCompilerTest, CanonicalizesDeterministicDependencyClosure) {
    CompileOptions options;
    options.allow_implicit_schema_version = true;
    auto result = SchemaCompiler::Compile(R"idl(
package p;
message Root { Mid mid = 2; Leaf leaf = 1; }
struct Mid { Leaf leaf = 1; }
struct Leaf { uint32 value = 1; }
)idl",
                                          options);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    ASSERT_EQ(result->types().size(), 3u);
    const SchemaDescriptor* root = result->FindType("p.Root");
    const SchemaDescriptor* mid = result->FindType("p.Mid");
    const SchemaDescriptor* leaf = result->FindType("p.Leaf");
    ASSERT_NE(root, nullptr);
    ASSERT_NE(mid, nullptr);
    ASSERT_NE(leaf, nullptr);
    ASSERT_EQ(mid->dependencies().size(), 1u);
    EXPECT_EQ(mid->dependencies()[0].full_name(), "p.Leaf");
    ASSERT_EQ(root->dependencies().size(), 2u);
    EXPECT_EQ(root->dependencies()[0].full_name(), "p.Leaf");
    EXPECT_EQ(root->dependencies()[1].full_name(), "p.Mid");
    EXPECT_EQ(root->dependencies()[0].digest(),
              leaf->identity().canonical_digest());
    EXPECT_EQ(root->dependencies()[1].digest(),
              mid->identity().canonical_digest());
}

TEST(SchemaCompilerTest, ResolvesProvidedExternalDescriptors) {
    CompileOptions dependency_options;
    dependency_options.allow_implicit_schema_version = true;
    auto dependency = SchemaCompiler::Compile(
        "package common; struct Point { fixed32 x = 1; }",
        dependency_options);
    ASSERT_TRUE(dependency.ok()) << dependency.status().ToString();

    CompileOptions options;
    options.allow_implicit_schema_version = true;
    options.dependencies.push_back(dependency->types()[0]);
    auto result = SchemaCompiler::Compile(R"idl(
package app;
import "ignored/by-identity.mino";
message Sample { common.Point point = 1; }
)idl",
                                          options);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    const SchemaDescriptor& descriptor = OnlyType(*result);
    ASSERT_EQ(descriptor.dependencies().size(), 1u);
    EXPECT_EQ(descriptor.dependencies()[0].full_name(), "common.Point");
    EXPECT_EQ(descriptor.dependencies()[0].digest(),
              dependency->types()[0]->identity().canonical_digest());
}

TEST(SchemaCompilerTest, EnforcesCompilerBoundaryResources) {
    CompileOptions options;
    options.max_input_bytes = 4;
    auto input = SchemaCompiler::Compile("message M {}", options);
    ASSERT_FALSE(input.ok());
    EXPECT_EQ(input.status().code(), StatusCode::kResourceExhausted);

    options = CompileOptions{};
    options.allow_implicit_schema_version = true;
    options.max_canonical_bytes = 8;
    auto canonical = SchemaCompiler::Compile("message M {}", options);
    ASSERT_FALSE(canonical.ok());
    EXPECT_EQ(canonical.status().code(), StatusCode::kResourceExhausted);
}

TEST(SchemaCompilerTest, RequiresExplicitVersionUnlessTestOptionAllowsIt) {
    auto strict = SchemaCompiler::Compile("message M {}");
    ASSERT_FALSE(strict.ok());
    EXPECT_EQ(strict.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(strict.status().message().find("schema_version"),
              std::string_view::npos);

    CompileOptions options;
    options.allow_implicit_schema_version = true;
    auto legacy_fixture = SchemaCompiler::Compile("message M {}", options);
    ASSERT_TRUE(legacy_fixture.ok()) << legacy_fixture.status().ToString();
    EXPECT_EQ(OnlyType(*legacy_fixture).identity().schema_version(), 0u);
}

TEST(SchemaCompilerTest, BytesDefaultsUseUtf8SafeHexCanonicalText) {
    auto result = SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
message M { bytes value = 1 [max_bytes = 2, default = "\xff\x00"]; }
)idl");
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_NE(OnlyType(*result).canonical_schema().find("bytes(2):ff00"),
              std::string_view::npos);
    EXPECT_TRUE(IsValidUtf8(OnlyType(*result).canonical_schema()));

    auto invalid_string = SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
message M { string value = 1 [max_bytes = 1, default = "\xff"]; }
)idl");
    ASSERT_FALSE(invalid_string.ok());
    EXPECT_NE(invalid_string.status().message().find("UTF-8"),
              std::string_view::npos);
}

TEST(SchemaCompilerTest, EnforcesRecursiveVectorCapacityFootprint) {
    constexpr std::string_view kIdl = R"idl(
option schema_version = "1.0";
message M {
  vector<vector<uint64>> values = 1 [max_capacity = 4];
}
)idl";
    // Configured 24 unknown-field bytes + 4 * (40-byte inner vector metadata +
    // 4 * 8-byte uint64 elements).
    CompileOptions options;
    options.unknown_fields.max_bytes = 8;
    options.unknown_fields.max_fields = 2;
    options.max_total_capacity = 311;
    auto too_large = SchemaCompiler::Compile(kIdl, options);
    ASSERT_FALSE(too_large.ok());
    EXPECT_EQ(too_large.status().code(), StatusCode::kResourceExhausted);

    options.max_total_capacity = 312;
    auto exact = SchemaCompiler::Compile(kIdl, options);
    ASSERT_TRUE(exact.ok()) << exact.status().ToString();
}

}  // namespace
}  // namespace mino::schema
