// Copyright 2026 The Mino Authors

#include "mino/platform/rdma_provider.h"

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
    return base / (workspaces.empty() ? "_main" : workspaces.front()) /
           relative;
}

TEST(RdmaPluginLoadTest, SoftwareLoopbackLoadsAndRegisters) {
    const auto path =
        Runfile("mino/platform/libmino_rdma_software_loopback.so");
    ASSERT_TRUE(std::filesystem::exists(path)) << path;
    ASSERT_TRUE(path.is_absolute());

    auto provider = CreateDynamicRdmaDeviceProvider(
        {.plugin_path = path.string(), .device_name = "loopback0"});
    ASSERT_TRUE(provider.ok()) << provider.status().ToString();
    EXPECT_EQ((*provider)->provider_class(),
              MemoryRegistrationProviderClass::kDevice);
    EXPECT_TRUE((*provider)->Supports(MemoryRegistrationKind::kRdma));
    EXPECT_NE((*provider)->provenance().find("NOT-QUALIFICATION-ELIGIBLE"),
              std::string::npos);

    ASSERT_TRUE(
        (*provider)
            ->Start({.max_connections = 4,
                     .max_listeners = 2,
                     .send_queue_depth = 8,
                     .receive_queue_depth = 8,
                     .completion_queue_depth = 16,
                     .max_message_bytes = 4096})
            .ok());

    std::vector<std::byte> buffer(64);
    MemoryRegistrationRequest request{
        .address = buffer.data(),
        .bytes = buffer.size(),
        .alignment = 1,
        .scope_id = 1,
        .kind = MemoryRegistrationKind::kRdma,
        .owner = {.process_id = 1, .process_epoch = 1, .lease_id = 1},
    };
    auto registered = (*provider)->Register(request);
    ASSERT_TRUE(registered.ok()) << registered.status().ToString();
    EXPECT_GT(registered->registration_id, 0u);
    EXPECT_TRUE((*provider)->Deregister(*registered).ok());
    EXPECT_TRUE((*provider)->Shutdown().ok());
}

TEST(RdmaPluginLoadTest, VerbsPluginLoadsAbiEvenWithoutDevice) {
    const auto path = Runfile("mino/platform/libmino_rdma_verbs.so");
    ASSERT_TRUE(std::filesystem::exists(path)) << path;
    auto provider = CreateDynamicRdmaDeviceProvider(
        {.plugin_path = path.string(), .device_name = "mlx5_0"});
    // Without libibverbs / device, create returns nullptr → kUnavailable.
    // With a real device the plugin may succeed; either outcome is OK for ABI.
    if (provider.ok()) {
        EXPECT_EQ((*provider)->provider_class(),
                  MemoryRegistrationProviderClass::kDevice);
        EXPECT_NE((*provider)->provenance().find("verbs"), std::string::npos);
    } else {
        EXPECT_TRUE(provider.status().code() == StatusCode::kUnavailable ||
                    provider.status().code() == StatusCode::kUnsupported)
            << provider.status().ToString();
    }
}

}  // namespace
}  // namespace mino::platform
