// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/codegen/code_generator.h"

#include "mino/schema/codegen/artifact_codec.h"

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"

namespace mino::schema::codegen {
namespace {

std::filesystem::path Runfile(std::string_view path) {
    const char* test_srcdir = std::getenv("TEST_SRCDIR");
    const char* test_workspace = std::getenv("TEST_WORKSPACE");
    EXPECT_NE(test_srcdir, nullptr);
    EXPECT_NE(test_workspace, nullptr);
    return std::filesystem::path(test_srcdir == nullptr ? "" : test_srcdir) /
           (test_workspace == nullptr ? "mino" : test_workspace) / path;
}

std::string Read(std::string_view path) {
    std::ifstream input(Runfile(path), std::ios::binary);
    EXPECT_TRUE(input) << path;
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

Result<GeneratedArtifacts> Generate(std::string_view idl,
                                    std::string header_include) {
    CompileOptions compile_options;
    compile_options.allow_implicit_schema_version = true;
    auto schema = SchemaCompiler::Compile(idl, compile_options);
    if (!schema.ok()) return schema.status();
    std::vector<LayoutPlan> layouts;
    layouts.reserve(schema->types().size());
    for (const auto& descriptor : schema->types()) {
        std::vector<std::shared_ptr<const SchemaDescriptor>> exact;
        for (const auto& candidate : schema->types()) {
            bool required = candidate->aggregate().full_name() ==
                            descriptor->aggregate().full_name();
            for (const auto& dependency : descriptor->dependencies()) {
                required = required ||
                           candidate->aggregate().full_name() ==
                               dependency.full_name();
            }
            if (required) exact.push_back(candidate);
        }
        auto layout = LayoutPlanner::Plan(*descriptor, exact);
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    CodeGeneratorOptions options;
    options.header_include = std::move(header_include);
    return CodeGenerator::Generate(*schema, layouts, options);
}

TEST(CodeGeneratorTest, MatchesGoldenHeaderSourceAndDescriptor) {
    const std::string idl =
        Read("mino/schema/codegen/testdata/golden.mino");
    auto generated = Generate(
        idl, "mino/schema/codegen/testdata/golden.generated.h");
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    EXPECT_EQ(generated->header,
              Read("mino/schema/codegen/testdata/golden.generated.h"));
    EXPECT_EQ(generated->source,
              Read("mino/schema/codegen/testdata/golden.generated.cc"));
    EXPECT_EQ(generated->descriptor,
              Read("mino/schema/codegen/testdata/golden.descriptor"));
}

TEST(CodeGeneratorTest, SameInputIsByteForByteDeterministic) {
    const std::string idl =
        Read("mino/schema/codegen/testdata/golden.mino");
    auto first = Generate(idl, "golden.generated.h");
    auto second = Generate(idl, "golden.generated.h");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(first->header, second->header);
    EXPECT_EQ(first->source, second->source);
    EXPECT_EQ(first->descriptor, second->descriptor);
}

TEST(CodeGeneratorTest, DescriptorCodecRoundTripsSemanticsAndRejectsTampering) {
    const std::string idl =
        Read("mino/schema/codegen/testdata/golden.mino");
    auto generated = Generate(idl, "golden.generated.h");
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    auto decoded = DecodeAndValidate(generated->descriptor);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_EQ(decoded->version, kDescriptorArtifactVersion);
    ASSERT_EQ(decoded->types.size(), 1u);
    const SchemaDescriptor& descriptor = *decoded->types[0].descriptor;
    ASSERT_EQ(descriptor.aggregate().reserved_ranges().size(), 1u);
    EXPECT_EQ(descriptor.aggregate().reserved_ranges()[0].first(), 6u);
    const FieldDescriptor* label = descriptor.aggregate().FindField(2);
    ASSERT_NE(label, nullptr);
    EXPECT_TRUE(label->constraints().snapshot_key());
    ASSERT_TRUE(label->default_value().has_value());
    EXPECT_EQ(label->default_value()->canonical_value(), "ready");

    std::string tampered = generated->descriptor;
    ASSERT_FALSE(tampered.empty());
    tampered.back() ^= 1;
    EXPECT_FALSE(DecodeAndValidate(tampered).ok());
}

TEST(CodeGeneratorTest, ImportedClosureDeterminesVectorElementSizeAndIsRequired) {
    auto dependency = SchemaCompiler::Compile(R"idl(
syntax = "v1";
package ext;
option schema_version = "1.0";
struct Point3D { float x = 1; float y = 2; float z = 3; }
)idl");
    ASSERT_TRUE(dependency.ok()) << dependency.status().ToString();
    CompileOptions compile_options;
    compile_options.dependencies.assign(dependency->types().begin(),
                                        dependency->types().end());
    auto root = SchemaCompiler::Compile(R"idl(
syntax = "v1";
package app;
import "ext.mino";
option schema_version = "1.0";
message Root { vector<ext.Point3D> points = 1 [max_capacity = 4]; }
)idl", compile_options);
    ASSERT_TRUE(root.ok()) << root.status().ToString();
    std::vector<std::shared_ptr<const SchemaDescriptor>> closure(
        dependency->types().begin(), dependency->types().end());
    closure.insert(closure.end(), root->types().begin(), root->types().end());
    auto layout = LayoutPlanner::Plan(*root->types()[0], closure);
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    const std::array layouts = {*layout};

    CodeGeneratorOptions options;
    options.header_include = "root.generated.h";
    auto missing = CodeGenerator::Generate(*root, layouts, options);
    EXPECT_FALSE(missing.ok());

    options.descriptor_closure = closure;
    auto generated = CodeGenerator::Generate(*root, layouts, options);
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    EXPECT_NE(generated->header.find("value.element_size != 48u"),
              std::string::npos);
    auto decoded = DecodeAndValidate(generated->descriptor);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    ASSERT_EQ(decoded->types[0].descriptor->dependencies().size(), 1u);
    EXPECT_EQ(decoded->types[0].descriptor->dependencies()[0].full_name(),
              "ext.Point3D");
}

TEST(CodeGeneratorTest, RejectsNonAuthoritativeLayout) {
    auto first = SchemaCompiler::Compile(R"idl(
syntax = "v1"; package layout_test; option schema_version = "1.0";
message Value { uint32 value = 1; }
)idl");
    auto second = SchemaCompiler::Compile(R"idl(
syntax = "v1"; package layout_test; option schema_version = "1.0";
message Value { uint64 value = 1; }
)idl");
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(second.ok());
    auto wrong_layout = LayoutPlanner::Plan(*second->types()[0], second->types());
    ASSERT_TRUE(wrong_layout.ok());
    const std::array layouts = {*wrong_layout};
    CodeGeneratorOptions options;
    options.header_include = "value.generated.h";
    EXPECT_FALSE(CodeGenerator::Generate(*first, layouts, options).ok());
}

TEST(CodeGeneratorTest, ManglesKeywordsReservedIdentifiersAndHelperCollisions) {
    auto generated = Generate(R"idl(
syntax = "v1";
package names;
option schema_version = "1.0";
message Thing {
  optional uint32 x = 1;
  uint32 has_x = 2;
  uint32 valid = 3;
  uint32 class = 4;
  uint32 foo__bar = 5;
}
message ThingBuilder { uint32 value = 1; }
)idl", "names.generated.h");
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    EXPECT_NE(generated->header.find("has_x_field_2"), std::string::npos);
    EXPECT_NE(generated->header.find("valid_field_3"), std::string::npos);
    EXPECT_NE(generated->header.find("class_mino"), std::string::npos);
    EXPECT_NE(generated->header.find("foo_u_bar"), std::string::npos);
    EXPECT_NE(generated->header.find("ThingBuilder_type_"), std::string::npos);
    EXPECT_EQ(generated->header.find("foo__bar"), std::string::npos);
}

TEST(CodeGeneratorTest, HeaderGuardIncludesSchemaDigest) {
    auto first = Generate(R"idl(
syntax = "v1"; package guard_a; option schema_version = "1.0";
message Value { uint32 value = 1; }
)idl", "same.generated.h");
    auto second = Generate(R"idl(
syntax = "v1"; package guard_b; option schema_version = "1.0";
message Value { uint32 value = 1; }
)idl", "same.generated.h");
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(second.ok());
    const size_t first_guard_end = first->header.find('\n', first->header.find("#ifndef"));
    const size_t second_guard_end = second->header.find('\n', second->header.find("#ifndef"));
    ASSERT_NE(first_guard_end, std::string::npos);
    ASSERT_NE(second_guard_end, std::string::npos);
    EXPECT_NE(first->header.substr(first->header.find("#ifndef"),
                                   first_guard_end - first->header.find("#ifndef")),
              second->header.substr(second->header.find("#ifndef"),
                                    second_guard_end - second->header.find("#ifndef")));
}

TEST(CodeGeneratorTest, RenameKeepsDigestButChangesAccessor) {
    CompileOptions compile_options;
    compile_options.allow_implicit_schema_version = true;
    auto old_name = SchemaCompiler::Compile(R"idl(
package rename_test;
message Value { optional uint64 old_name = 1; }
)idl",
                                            compile_options);
    auto new_name = SchemaCompiler::Compile(R"idl(
package rename_test;
message Value { optional uint64 new_name = 1; }
)idl",
                                            compile_options);
    ASSERT_TRUE(old_name.ok()) << old_name.status().ToString();
    ASSERT_TRUE(new_name.ok()) << new_name.status().ToString();
    ASSERT_EQ(old_name->types().size(), 1u);
    ASSERT_EQ(new_name->types().size(), 1u);
    EXPECT_EQ(old_name->types()[0]->identity().canonical_digest(),
              new_name->types()[0]->identity().canonical_digest());

    auto old_generated = Generate(R"idl(
package rename_test;
message Value { optional uint64 old_name = 1; }
)idl",
                                  "rename.generated.h");
    auto new_generated = Generate(R"idl(
package rename_test;
message Value { optional uint64 new_name = 1; }
)idl",
                                  "rename.generated.h");
    ASSERT_TRUE(old_generated.ok()) << old_generated.status().ToString();
    ASSERT_TRUE(new_generated.ok()) << new_generated.status().ToString();
    EXPECT_NE(old_generated->header, new_generated->header);
    EXPECT_NE(old_generated->header.find("set_old_name"), std::string::npos);
    EXPECT_NE(new_generated->header.find("set_new_name"), std::string::npos);
    EXPECT_EQ(old_generated->header.find("set_new_name"), std::string::npos);
}

}  // namespace
}  // namespace mino::schema::codegen
