// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/compatibility.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>
#include <vector>

#include "mino/schema/compiler.h"

namespace mino::schema {
namespace {

std::shared_ptr<const SchemaDescriptor> CompileOne(std::string_view fields,
                                                   std::string_view version = "1.0") {
    const std::string idl =
        "option schema_version = \"" + std::string(version) +
        "\"; package p; message M { " + std::string(fields) + " }";
    auto result = SchemaCompiler::Compile(idl);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    if (!result.ok()) return nullptr;
    EXPECT_EQ(result->types().size(), 1u);
    return result->types()[0];
}

Compatibility Check(std::string_view from_fields, std::string_view to_fields) {
    auto from = CompileOne(from_fields);
    auto to = CompileOne(to_fields);
    if (from == nullptr || to == nullptr) return Compatibility::kIncompatible;
    auto result = CompatibilityChecker::Check(*from, *to);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    return result.ok() ? *result : Compatibility::kIncompatible;
}

TEST(CompatibilityTest, IdenticalDigestWinsAcrossVersionLabelsAndRename) {
    auto from = CompileOne("optional uint32 old_name = 1;", "1.0");
    auto version_only = CompileOne("optional uint32 old_name = 1;", "9.9");
    auto renamed = CompileOne("optional uint32 new_name = 1;", "1.1");
    ASSERT_NE(from, nullptr);
    ASSERT_NE(version_only, nullptr);
    ASSERT_NE(renamed, nullptr);

    auto same = CompatibilityChecker::Check(*from, *version_only);
    auto rename = CompatibilityChecker::Check(*from, *renamed);
    ASSERT_TRUE(same.ok());
    ASSERT_TRUE(rename.ok());
    EXPECT_EQ(*same, Compatibility::kIdentical);
    // Field names are excluded from Canonicalization v1, so a pure rename has
    // the stronger kIdentical result and is therefore wire compatible.
    EXPECT_EQ(*rename, Compatibility::kIdentical);
}

TEST(CompatibilityTest, OptionalAdditionAndReservedDeletionAreWireCompatible) {
    EXPECT_EQ(Check("uint32 id = 1;",
                    "uint32 id = 1; optional uint32 extra = 2;"),
              Compatibility::kWireCompatible);
    EXPECT_EQ(Check("uint32 id = 1; optional uint32 extra = 2;",
                    "uint32 id = 1; reserved 2;"),
              Compatibility::kWireCompatible);
}

TEST(CompatibilityTest, RequiredChangesAndTypeChangesAreIncompatible) {
    EXPECT_EQ(Check("uint32 id = 1;", "uint32 id = 1; uint32 extra = 2;"),
              Compatibility::kIncompatible);
    EXPECT_EQ(Check("uint32 id = 1; optional uint32 extra = 2;",
                    "uint32 id = 1;"),
              Compatibility::kIncompatible);
    EXPECT_EQ(Check("uint32 id = 1;", "uint64 id = 1;"),
              Compatibility::kIncompatible);
    EXPECT_EQ(Check("optional uint32 id = 1;", "required uint32 id = 1;"),
              Compatibility::kIncompatible);
    EXPECT_EQ(Check("uint32 id = 1 [default = 1];",
                    "uint32 id = 1 [default = 2];"),
              Compatibility::kIncompatible);
}

TEST(CompatibilityTest, ConstraintDirectionMatchesMatrix) {
    EXPECT_EQ(Check("string value = 1 [max_bytes = 64];",
                    "string value = 1 [max_bytes = 32];"),
              Compatibility::kWriteCompatible);
    EXPECT_EQ(Check("string value = 1 [max_bytes = 32];",
                    "string value = 1 [max_bytes = 64];"),
              Compatibility::kReadCompatible);
    EXPECT_EQ(Check("vector<uint32> value = 1 [max_capacity = 64];",
                    "vector<uint32> value = 1 [max_capacity = 32];"),
              Compatibility::kWriteCompatible);
    EXPECT_EQ(Check("vector<uint32> value = 1 [max_capacity = 32];",
                    "vector<uint32> value = 1 [max_capacity = 64];"),
              Compatibility::kReadCompatible);
}

TEST(CompatibilityTest, CombinationUsesStrictestDirection) {
    EXPECT_EQ(Check(
                  "string a = 1 [max_bytes = 64]; string b = 2 [max_bytes = 32];",
                  "string a = 1 [max_bytes = 32]; string b = 2 [max_bytes = 64];"),
              Compatibility::kIncompatible);
    EXPECT_EQ(Check(
                  "string a = 1 [max_bytes = 64]; optional uint32 b = 2;",
                  "string a = 1 [max_bytes = 32]; optional uint32 c = 2; optional uint32 d = 3;"),
              Compatibility::kWriteCompatible);
}

TEST(CompatibilityTest, AggregateTypeRenameIsIncompatible) {
    auto from = SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package p; "
        "message A { uint32 id = 1; }");
    auto to = SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package p; "
        "message B { uint32 id = 1; }");
    ASSERT_TRUE(from.ok());
    ASSERT_TRUE(to.ok());
    auto result = CompatibilityChecker::Check(*from->types()[0], *to->types()[0]);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(*result, Compatibility::kIncompatible);
}

TEST(CompatibilityTest, RecursesThroughChangedDependencyDigestsAndMergesDirection) {
    auto from = SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package p;
message Child { string value = 1 [max_bytes = 8]; }
message Root { Child first = 1; Child second = 2; }
)idl");
    auto to = SchemaCompiler::Compile(R"idl(
option schema_version = "1.1";
package p;
message Child { string value = 1 [max_bytes = 16]; }
message Root { Child first = 1; Child second = 2; }
)idl");
    ASSERT_TRUE(from.ok()) << from.status().ToString();
    ASSERT_TRUE(to.ok()) << to.status().ToString();

    std::vector<std::shared_ptr<const SchemaDescriptor>> closure;
    closure.insert(closure.end(), from->types().begin(), from->types().end());
    closure.insert(closure.end(), to->types().begin(), to->types().end());
    auto result = CompatibilityChecker::Check(
        *from->FindType("p.Root"), *to->FindType("p.Root"), closure);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_EQ(*result, Compatibility::kReadCompatible);
}

}  // namespace
}  // namespace mino::schema
