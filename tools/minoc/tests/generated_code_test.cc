// Copyright 2026 The Mino Authors

#include "tools/minoc/tests/generated/sample.generated.h"

#include <type_traits>

#include "gtest/gtest.h"
#include "mino/runtime/message_traits.h"

namespace {

TEST(GeneratedCodeTest, BuilderAccessorAndTraitsUsePlannedLayout) {
    static_assert(std::is_standard_layout_v<minoc_test::Sample>);
    static_assert(std::is_trivially_copyable_v<minoc_test::Sample>);
    static_assert(std::is_trivially_default_constructible_v<minoc_test::Sample>);
    static_assert(mino::kHasStaticMessageTraits<minoc_test::Sample>);
    static_assert(
        mino::StaticMessageTraits<minoc_test::Sample>::index_flags ==
        mino::kIndexSlotFlagHasChildSlabs);

    minoc_test::Sample object;
    minoc_test::SampleBuilder builder(object);
    builder.set_sequence(42);
    EXPECT_TRUE(builder.set_label({.offset = 128,
                                   .generation = 3,
                                   .region_id = 9,
                                   .length = 5,
                                   .capacity = 16,
                                   .element_size = 1}));
    EXPECT_TRUE(builder.set_payload({.element_size = 1}));
    EXPECT_TRUE(builder.set_samples({.element_size = 8}));
    EXPECT_FALSE(builder.set_child({.offset = 256,
                                    .length = 40,
                                    .capacity = 40,
                                    .element_size = 1}));
    EXPECT_TRUE(builder.set_child({.offset = 256,
                                   .length = minoc_test::Child::kObjectSize,
                                   .capacity = minoc_test::Child::kObjectSize,
                                   .element_size = 1}));
    builder.set_active(true);

    const minoc_test::SampleAccessor accessor(object);
    EXPECT_TRUE(accessor.valid());
    EXPECT_EQ(accessor.sequence(), 42u);
    EXPECT_EQ(accessor.header().schema_short_id,
              minoc_test::Sample::kSchemaShortId);
    EXPECT_EQ(accessor.header().object_size,
              minoc_test::Sample::kFixedAreaSize);
    EXPECT_TRUE(accessor.has_label());
    EXPECT_EQ(accessor.label().offset, 128u);
    EXPECT_TRUE(accessor.has_active());
    EXPECT_TRUE(accessor.active());
    EXPECT_TRUE(mino::StaticMessageTraits<minoc_test::Sample>::Validate(object).ok());

    builder.clear_label();
    EXPECT_FALSE(minoc_test::SampleAccessor(object).has_label());
    EXPECT_FALSE(builder.set_payload({.length = 33,
                                      .capacity = 33,
                                      .element_size = 1}));
}

TEST(GeneratedCodeTest, WireAdapterRejectsUnresolvedNonEmptyShmStorage) {
    minoc_test::Sample object;
    minoc_test::SampleBuilder builder(object);
    builder.set_sequence(7);
    ASSERT_TRUE(builder.set_label({.offset = 128,
                                   .generation = 1,
                                   .region_id = 2,
                                   .length = 1,
                                   .capacity = 1,
                                   .element_size = 1}));
    ASSERT_TRUE(builder.set_payload({.element_size = 1}));
    ASSERT_TRUE(builder.set_samples({.element_size = 8}));
    ASSERT_TRUE(minoc_test::SampleAccessor(object).valid());

    auto encoded = minoc_test::SampleWireAdapter::Encode(object);
    ASSERT_FALSE(encoded.ok());
    EXPECT_EQ(encoded.status().code(), mino::StatusCode::kUnsupported);
}

TEST(GeneratedCodeTest, AccessorBoundsAndWalkerStructuralChecksAreSafe) {
    minoc_test::Sample object;
    minoc_test::SampleBuilder builder(object);
    builder.set_sequence(7);
    ASSERT_TRUE(builder.set_payload({.element_size = 1}));
    ASSERT_TRUE(builder.set_samples({.element_size = 8}));

    const minoc_test::SampleAccessor truncated(object.storage.data(), 8);
    EXPECT_FALSE(truncated.valid());
    EXPECT_EQ(truncated.sequence(), 0u);
    EXPECT_FALSE(truncated.has_label());

    object.storage[168] = std::byte{2};
    EXPECT_FALSE(minoc_test::SampleAccessor(object).valid());
    EXPECT_FALSE(minoc_test::SampleAccessor(object).active());
    builder.set_active(true);

    object.storage[39] = std::byte{0x80};
    EXPECT_FALSE(minoc_test::SampleAccessor(object).valid());
    object.storage[39] = std::byte{0};

    builder.clear_label();
    object.storage[48] = std::byte{1};
    EXPECT_FALSE(minoc_test::SampleAccessor(object).valid());
    object.storage[48] = std::byte{0};

    object.storage[248] = std::byte{1};
    EXPECT_FALSE(minoc_test::SampleAccessor(object).valid());
}

}  // namespace
