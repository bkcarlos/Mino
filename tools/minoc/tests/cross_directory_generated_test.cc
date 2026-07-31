// Copyright 2026 The Mino Authors

#include "tools/minoc/tests/generated/sensor_frame.generated.h"

#include "gtest/gtest.h"

namespace {

TEST(CrossDirectoryGeneratedTest, RuleCompilesDeclaredWorkspaceImport) {
    mino::examples::SensorFrame object;
    mino::examples::SensorFrameBuilder builder(object);
    builder.set_frame_id(7);
    ASSERT_TRUE(builder.set_device_name({.offset = 64,
                                         .length = 4,
                                         .capacity = 64,
                                         .element_size = 1}));
    EXPECT_FALSE(builder.set_points({.element_size = 40}));
    ASSERT_TRUE(builder.set_points({.element_size = 48}));
    ASSERT_TRUE(builder.set_payload({.element_size = 1}));
    EXPECT_TRUE(mino::examples::SensorFrameAccessor(object).valid());
}

}  // namespace
