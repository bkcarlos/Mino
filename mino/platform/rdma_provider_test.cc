// Copyright 2026 The Mino Authors

#include "mino/platform/rdma_provider.h"

#include <gtest/gtest.h>

namespace mino::platform {
namespace {

TEST(DynamicRdmaProviderTest, RequiresExplicitAbsoluteExistingPlugin) {
    auto relative = CreateDynamicRdmaDeviceProvider(
        {.plugin_path = "libmino-rdma.so", .device_name = "mlx5_0"});
    ASSERT_FALSE(relative.ok());
    EXPECT_EQ(relative.status().code(), StatusCode::kInvalidArgument);

    auto missing = CreateDynamicRdmaDeviceProvider(
        {.plugin_path = "/definitely/not/a/mino/rdma/provider.so",
         .device_name = "mlx5_0"});
    ASSERT_FALSE(missing.ok());
    EXPECT_EQ(missing.status().code(), StatusCode::kUnsupported);
}

}  // namespace
}  // namespace mino::platform
