// Copyright 2026 The Mino Authors

#include "mino/schema/layout.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>
#include <type_traits>

#include "mino/common/status.h"
#include "mino/schema/compiler.h"

namespace mino::schema {
namespace {

static_assert(!std::is_default_constructible_v<LayoutPlan>);
static_assert(!std::is_aggregate_v<LayoutPlan>);

Result<CompiledSchema> Compile(std::string_view idl) {
    CompileOptions options;
    options.allow_implicit_schema_version = true;
    return SchemaCompiler::Compile(idl, options);
}

TEST(LayoutPlannerTest, GoldenOffsetsPresenceAndStableVariableMetadata) {
    auto compiled = Compile(R"idl(
package p;
message Sample {
  bool enabled = 1;
  optional int64 timestamp = 2;
  optional string name = 3 [max_bytes = 16];
  fixed32 code = 4;
}
)idl");
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    const SchemaDescriptor* descriptor = compiled->FindType("p.Sample");
    ASSERT_NE(descriptor, nullptr);

    auto plan = LayoutPlanner::Plan(*descriptor, compiled->types());
    ASSERT_TRUE(plan.ok()) << plan.status().ToString();
    EXPECT_EQ(plan->layout_version(), 1u);
    EXPECT_EQ(plan->header_size(), 32u);
    EXPECT_EQ(ObjectHeaderLayout::kLayoutVersionOffset, 0u);
    EXPECT_EQ(ObjectHeaderLayout::kSchemaShortIdOffset, 8u);
    EXPECT_EQ(ObjectHeaderLayout::kObjectSizeOffset, 16u);
    EXPECT_EQ(ObjectHeaderLayout::kPresenceBitmapWordsOffset, 28u);
    EXPECT_EQ(plan->presence_bitmap_offset(), 32u);
    EXPECT_EQ(plan->presence_bitmap_words(), 1u);
    EXPECT_EQ(plan->fixed_area_offset(), 40u);
    EXPECT_EQ(plan->fixed_area_size(), 64u);
    ASSERT_TRUE(plan->unknown_fields_offset().has_value());
    EXPECT_EQ(*plan->unknown_fields_offset(), 104u);
    EXPECT_EQ(plan->object_size(), 144u);
    EXPECT_EQ(plan->max_child_bytes(),
              16u + kDynamicUnknownFieldMaxBytes);
    EXPECT_EQ(plan->max_dynamic_children(), 2u);

    ASSERT_EQ(plan->fields().size(), 4u);
    EXPECT_EQ(plan->fields()[0].field_id(), 1u);
    EXPECT_EQ(plan->fields()[0].offset(), 40u);
    EXPECT_EQ(plan->fields()[0].size(), 1u);
    EXPECT_FALSE(plan->fields()[0].presence_bit().has_value());
    EXPECT_EQ(plan->fields()[1].offset(), 48u);
    EXPECT_EQ(plan->fields()[1].presence_bit(), 0u);
    EXPECT_EQ(plan->fields()[2].offset(), 56u);
    EXPECT_EQ(plan->fields()[2].size(), VariableMetadataLayout::kSize);
    EXPECT_EQ(plan->fields()[2].presence_bit(), 1u);
    EXPECT_EQ(plan->fields()[3].offset(), 96u);

    EXPECT_EQ(VariableMetadataLayout::kHandleSize, 16u);
    EXPECT_EQ(VariableMetadataLayout::kLengthOffset, 16u);
    EXPECT_EQ(VariableMetadataLayout::kCapacityOffset, 24u);
    EXPECT_EQ(VariableMetadataLayout::kElementSizeOffset, 32u);
    EXPECT_EQ(VariableMetadataLayout::kSize, 40u);
}

TEST(LayoutPlannerTest, ComputesDynamicChildUpperBoundAndRejectsLimit) {
    auto compiled = Compile(R"idl(
package p;
message M {
  vector<string> names = 1 [max_capacity = 3, max_bytes = 5];
}
)idl");
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    const SchemaDescriptor* descriptor = compiled->FindType("p.M");
    ASSERT_NE(descriptor, nullptr);

    auto plan = LayoutPlanner::Plan(*descriptor, compiled->types());
    ASSERT_TRUE(plan.ok()) << plan.status().ToString();
    // One vector slab with three 40-byte StringMeta elements, plus three
    // bounded string child slabs.
    EXPECT_EQ(plan->max_child_bytes(),
              135u + kDynamicUnknownFieldMaxBytes);
    EXPECT_EQ(plan->max_dynamic_children(), 5u);

    LayoutOptions limits;
    limits.max_total_child_bytes = 134 + kDynamicUnknownFieldMaxBytes;
    auto rejected = LayoutPlanner::Plan(*descriptor, compiled->types(), limits);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kResourceExhausted);
}

TEST(LayoutPlannerTest, InlinesOnlyFixedStructLayouts) {
    auto compiled = Compile(R"idl(
package p;
struct Point { fixed32 x = 1; bool visible = 2; }
message M { Point point = 1; }
)idl");
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    const SchemaDescriptor* descriptor = compiled->FindType("p.M");
    ASSERT_NE(descriptor, nullptr);

    auto plan = LayoutPlanner::Plan(*descriptor, compiled->types());
    ASSERT_TRUE(plan.ok()) << plan.status().ToString();
    ASSERT_EQ(plan->fields().size(), 1u);
    EXPECT_EQ(plan->fields()[0].storage_kind(),
              FieldStorageKind::kInlineStruct);
    EXPECT_EQ(plan->fields()[0].size(), 40u);
    ASSERT_TRUE(plan->unknown_fields_offset().has_value());
    EXPECT_EQ(plan->max_dynamic_children(), 1u);
}

}  // namespace
}  // namespace mino::schema
