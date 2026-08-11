// Copyright 2026 The Mino Authors

#include "mino/config/deployment_config.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>

#include "gtest/gtest.h"
#include "mino/common/status.h"

namespace mino::config {
namespace {

std::filesystem::path GoldenPath() {
    const char* source_dir = std::getenv("TEST_SRCDIR");
    const char* workspace = std::getenv("TEST_WORKSPACE");
    EXPECT_NE(source_dir, nullptr);
    EXPECT_NE(workspace, nullptr);
    return std::filesystem::path(source_dir == nullptr ? "" : source_dir) /
           (workspace == nullptr ? "mino" : workspace) / "configs" /
           "node.production-edge.toml";
}

std::filesystem::path IsolationPolicyPath() {
    return GoldenPath().parent_path() / "security-domains.toml";
}

std::string Golden() {
    std::ifstream input(GoldenPath());
    EXPECT_TRUE(input.good()) << GoldenPath();
    std::ostringstream contents;
    contents << input.rdbuf();
    return contents.str();
}

std::string ReplaceOnce(std::string text, std::string_view from,
                        std::string_view to) {
    const size_t position = text.find(from);
    EXPECT_NE(position, std::string::npos) << from;
    if (position != std::string::npos) {
        text.replace(position, from.size(), to);
    }
    return text;
}

TEST(NodeDeploymentConfigTest, ParsesCheckedInGolden) {
    auto parsed = ParseNodeDeploymentConfigToml(Golden());
    ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
    EXPECT_EQ(parsed->schema_version, kNodeDeploymentSchemaVersion);
    EXPECT_EQ(parsed->node.id, 1001u);
    EXPECT_EQ(parsed->node.environment, "production");
    EXPECT_EQ(parsed->node.role, "edge");
    EXPECT_EQ(parsed->security_domain.id, 7u);
    EXPECT_FALSE(parsed->security_domain.trusted);
    EXPECT_EQ(parsed->isolation.uid, 65532u);
    EXPECT_EQ(parsed->isolation.gid, 65532u);
    EXPECT_EQ(parsed->isolation.namespace_name, "mino-domain-7");
    EXPECT_EQ(parsed->region.id, 17u);
    EXPECT_EQ(parsed->region.bytes, 536'870'912u);
    EXPECT_EQ(parsed->resources.bridge_connections, 64u);
    EXPECT_TRUE(parsed->bridge.enabled);
    EXPECT_EQ(parsed->bridge.tls.private_key_file,
              "/run/secrets/mino/tls.key");
    EXPECT_EQ(parsed->monitoring.port, 9464u);
    EXPECT_EQ(parsed->supervisor.mode, "local");
    EXPECT_FALSE(parsed->supervisor.recorder_enabled);
    EXPECT_EQ(parsed->storage.data_dir, "/var/lib/mino/data");
}

TEST(NodeDeploymentConfigTest, LoadsCheckedInGoldenFile) {
    auto loaded = LoadNodeDeploymentConfigFromTomlFile(GoldenPath().string());
    ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
    EXPECT_EQ(loaded->node.name, "mino-production-edge-1001");
}

TEST(NodeDeploymentConfigTest, RejectsUnknownRootAndNestedKeys) {
    auto root = ParseNodeDeploymentConfigToml(Golden() + "\nbackdoor = true\n");
    ASSERT_FALSE(root.ok());
    EXPECT_EQ(root.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(root.status().message().find("unknown key"), std::string::npos);

    auto nested = ParseNodeDeploymentConfigToml(ReplaceOnce(
        Golden(), "shutdown_grace_ms = 30000",
        "shutdown_grace_ms = 30000\nprivate_key = \"not-a-reference\""));
    ASSERT_FALSE(nested.ok());
    EXPECT_NE(nested.status().message().find("unknown key"), std::string::npos);
}

TEST(NodeDeploymentConfigTest, RejectsBoundaryAndCrossFieldErrors) {
    auto unaligned = ParseNodeDeploymentConfigToml(
        ReplaceOnce(Golden(), "bytes = 536870912", "bytes = 536870913"));
    ASSERT_FALSE(unaligned.ok());
    EXPECT_NE(unaligned.status().message().find("page aligned"),
              std::string::npos);

    auto undersized = ParseNodeDeploymentConfigToml(ReplaceOnce(
        Golden(), "memory_bytes = 1073741824", "memory_bytes = 268435456"));
    ASSERT_FALSE(undersized.ok());
    EXPECT_NE(undersized.status().message().find("cover shared memory"),
              std::string::npos);

    auto bridge_budget = ParseNodeDeploymentConfigToml(ReplaceOnce(
        Golden(), "bridge_connections = 64", "bridge_connections = 1"));
    ASSERT_FALSE(bridge_budget.ok());
    EXPECT_NE(bridge_budget.status().message().find("bridge resource budget"),
              std::string::npos);
}

TEST(NodeDeploymentConfigTest, RejectsInlineCredentialMaterialAndTraversal) {
    auto inline_material = ParseNodeDeploymentConfigToml(
        Golden() + "\n# -----BEGIN PRIVATE KEY-----\n");
    ASSERT_FALSE(inline_material.ok());
    EXPECT_NE(inline_material.status().message().find("inline certificate"),
              std::string::npos);

    auto relative = ParseNodeDeploymentConfigToml(ReplaceOnce(
        Golden(), "/run/secrets/mino/tls.key", "../secrets/tls.key"));
    ASSERT_FALSE(relative.ok());
    EXPECT_NE(relative.status().message().find("must be absolute"),
              std::string::npos);
}

TEST(NodeDeploymentConfigTest, EnforcesOneUntrustedDomainPerUidGidAndNamespace) {
    auto policy = LoadSecurityDomainIsolationPolicyFromTomlFile(
        IsolationPolicyPath().string());
    ASSERT_TRUE(policy.ok()) << policy.status().ToString();
    ASSERT_EQ(policy->domains.size(), 1u);
    EXPECT_EQ(policy->domains.front().security_domain_id, 7u);
    EXPECT_EQ(policy->domains.front().uid, 65532u);

    const std::string first =
        "schema_version = 1\n"
        "[[domains]]\nsecurity_domain_id = 7\ntrusted = false\n"
        "uid = 70001\ngid = 70001\nnamespace = \"domain-7\"\n";
    auto same_uid = ParseSecurityDomainIsolationPolicyToml(
        first +
        "[[domains]]\nsecurity_domain_id = 8\ntrusted = false\n"
        "uid = 70001\ngid = 70002\nnamespace = \"domain-8\"\n");
    ASSERT_FALSE(same_uid.ok());
    EXPECT_NE(same_uid.status().message().find("share a UID"),
              std::string::npos);

    auto same_gid = ParseSecurityDomainIsolationPolicyToml(
        first +
        "[[domains]]\nsecurity_domain_id = 8\ntrusted = false\n"
        "uid = 70002\ngid = 70001\nnamespace = \"domain-8\"\n");
    ASSERT_FALSE(same_gid.ok());
    EXPECT_NE(same_gid.status().message().find("share a GID"),
              std::string::npos);

    auto same_namespace = ParseSecurityDomainIsolationPolicyToml(
        first +
        "[[domains]]\nsecurity_domain_id = 8\ntrusted = false\n"
        "uid = 70002\ngid = 70002\nnamespace = \"domain-7\"\n");
    ASSERT_FALSE(same_namespace.ok());
    EXPECT_NE(same_namespace.status().message().find("share a namespace"),
              std::string::npos);
}

TEST(NodeDeploymentConfigTest, RejectsDisabledBridgeWithReservedConnections) {
    auto disabled = ParseNodeDeploymentConfigToml(
        ReplaceOnce(Golden(), "enabled = true", "enabled = false"));
    ASSERT_FALSE(disabled.ok());
    EXPECT_NE(disabled.status().message().find("disabled bridge"),
              std::string::npos);
}

}  // namespace
}  // namespace mino::config
