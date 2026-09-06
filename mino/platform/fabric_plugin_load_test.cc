// Copyright 2026 The Mino Authors

#include "mino/platform/fabric_provider.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace mino::platform {
namespace {

std::filesystem::path Runfile(std::string_view relative) {
    const char* srcdir = std::getenv("TEST_SRCDIR");
    const char* workspace = std::getenv("TEST_WORKSPACE");
    std::vector<std::string> workspaces;
    if (workspace != nullptr && *workspace != '\0') {
        workspaces.emplace_back(workspace);
    }
    workspaces.push_back("_main");
    workspaces.push_back("mino");
    workspaces.push_back("Mino");
    const std::filesystem::path base(srcdir == nullptr ? "" : srcdir);
    for (const auto& ws : workspaces) {
        auto candidate = base / ws / relative;
        if (std::filesystem::exists(candidate)) return candidate;
    }
    return base / "_main" / relative;
}

TEST(FabricPluginLoadTest, SoftwareLoopbackLoadsForConfiguredKind) {
    const auto path =
        Runfile("mino/platform/libmino_fabric_software_loopback.so");
    ASSERT_TRUE(std::filesystem::exists(path)) << path;
    ASSERT_TRUE(path.is_absolute());

    auto provider = CreateDynamicFabricDeviceProvider(
        {.plugin_path = path.string(),
         .device_name = "ntb0",
         .expected_kind = FabricKind::kNtb});
    ASSERT_TRUE(provider.ok()) << provider.status().ToString();
    const auto caps = (*provider)->capabilities();
    EXPECT_EQ(caps.provider_class, FabricProviderClass::kDevice);
    EXPECT_EQ(caps.kind, FabricKind::kNtb);
    EXPECT_TRUE(caps.device_present);
    EXPECT_TRUE(caps.link_active);
    EXPECT_FALSE((*provider)->device_id().empty());
    EXPECT_NE((*provider)->provenance().find("NOT-QUALIFICATION-ELIGIBLE"),
              std::string::npos);

    ASSERT_TRUE((*provider)
                    ->Start({.max_connections = 2,
                             .max_listeners = 1,
                             .max_windows_per_connection = 1,
                             .max_events_per_poll = 8,
                             .max_window_bytes = 64 * 1024})
                    .ok());
    EXPECT_TRUE((*provider)->Shutdown().ok());
}

TEST(FabricPluginLoadTest, KindMismatchFailsClosed) {
    const auto path =
        Runfile("mino/platform/libmino_fabric_software_loopback.so");
    ASSERT_TRUE(std::filesystem::exists(path)) << path;
    auto provider = CreateDynamicFabricDeviceProvider(
        {.plugin_path = path.string(),
         .device_name = "ntb0",
         .expected_kind = FabricKind::kCxl});
    ASSERT_FALSE(provider.ok());
    EXPECT_EQ(provider.status().code(), StatusCode::kPermissionDenied);
}

}  // namespace
}  // namespace mino::platform
