// Copyright 2026 The Mino Authors

#include "mino/platform/fabric_provider.h"

#include <gtest/gtest.h>

namespace mino::platform {
namespace {

TEST(FabricProviderTest, RejectsUnavailableOrUnprovisionedDevice) {
    FabricProviderCapabilities capabilities;
    EXPECT_EQ(ValidateFabricProviderCapabilities(capabilities).code(),
              StatusCode::kPermissionDenied);
    capabilities.provider_class = FabricProviderClass::kDevice;
    capabilities.kind = FabricKind::kNtb;
    EXPECT_EQ(ValidateFabricProviderCapabilities(capabilities).code(),
              StatusCode::kUnavailable);
}

TEST(FabricProviderTest, DynamicLoaderIsAbsoluteAndFailClosed) {
    EXPECT_EQ(CreateDynamicFabricDeviceProvider({.plugin_path = "relative.so",
                                                 .device_name = "ntb0",
                                                 .expected_kind =
                                                     FabricKind::kNtb})
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
    EXPECT_FALSE(CreateDynamicFabricDeviceProvider(
                     {.plugin_path = "/definitely/not/a/mino/provider.so",
                      .device_name = "ntb0",
                      .expected_kind = FabricKind::kNtb})
                     .ok());
}

TEST(FabricProviderTest, ValidatesAlignmentAndProductionClass) {
    FabricProviderCapabilities capabilities{
        .provider_class = FabricProviderClass::kMock,
        .kind = FabricKind::kCxl,
        .device_present = true,
        .link_active = true,
        .cache_coherent = true,
        .cache_line_bytes = 64,
        .required_alignment = 64,
        .max_connections = 4,
        .max_listeners = 1,
        .max_windows_per_connection = 8,
        .max_window_bytes = 4096,
    };
    EXPECT_TRUE(ValidateFabricProviderCapabilities(capabilities).ok());
    EXPECT_EQ(ValidateFabricProviderCapabilities(capabilities, true).code(),
              StatusCode::kPermissionDenied);
    capabilities.required_alignment = 48;
    EXPECT_EQ(ValidateFabricProviderCapabilities(capabilities).code(),
              StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::platform
