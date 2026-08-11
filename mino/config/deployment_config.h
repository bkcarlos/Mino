// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_CONFIG_DEPLOYMENT_CONFIG_H_
#define MINO_CONFIG_DEPLOYMENT_CONFIG_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/result.h"

namespace mino::config {

inline constexpr uint32_t kNodeDeploymentSchemaVersion = 1;
inline constexpr size_t kMaximumNodeDeploymentConfigBytes = 1024u * 1024u;

struct DeploymentNodeConfig {
    uint64_t id = 0;
    std::string name;
    std::string environment;
    std::string role;
    uint32_t shutdown_grace_ms = 30'000;
};

struct DeploymentSecurityDomainConfig {
    uint64_t id = 0;
    std::string name;
    bool trusted = false;
};

struct DeploymentIsolationConfig {
    uint32_t uid = 0;
    uint32_t gid = 0;
    std::string namespace_name;
    std::filesystem::path policy_file;
    std::filesystem::path namespace_attestation_file;
};

struct SecurityDomainIsolationBinding {
    uint64_t security_domain_id = 0;
    bool trusted = false;
    uint32_t uid = 0;
    uint32_t gid = 0;
    std::string namespace_name;
};

struct SecurityDomainIsolationPolicy {
    uint32_t schema_version = 0;
    std::vector<SecurityDomainIsolationBinding> domains;
};

struct DeploymentRegionConfig {
    uint32_t id = 0;
    std::string name;
    uint64_t bytes = 0;
};

struct DeploymentResourceConfig {
    uint64_t memory_bytes = 0;
    uint64_t shm_bytes = 0;
    uint64_t file_descriptors = 0;
    uint64_t threads = 0;
    uint64_t bridge_connections = 0;
};

struct DeploymentTlsCredentialReferences {
    std::filesystem::path trust_anchors_file;
    std::filesystem::path certificate_chain_file;
    std::filesystem::path private_key_file;
};

struct DeploymentBridgeConfig {
    bool enabled = false;
    std::string listen_address;
    uint16_t port = 0;
    uint64_t expected_peer_security_domain = 0;
    uint32_t max_connections = 0;
    DeploymentTlsCredentialReferences tls;
};

struct DeploymentMonitoringConfig {
    bool enabled = true;
    bool otlp_enabled = false;
    std::string bind_address;
    uint16_t port = 0;
    uint32_t aggregate_interval_ms = 1000;
    size_t request_bytes_limit = 4096;
    size_t header_count_limit = 32;
    size_t response_bytes_limit = 256u * 1024u;
    size_t connection_limit = 16;
    size_t worker_threads = 2;
    uint32_t read_timeout_ms = 1000;
    uint32_t write_timeout_ms = 2000;
    uint32_t accept_poll_ms = 100;
};

struct DeploymentSupervisorConfig {
    std::string mode;
    std::string control_topic;
    uint32_t channel_capacity = 16;
    uint32_t max_subscribers = 4;
    size_t max_payload_bytes = 4096;
    bool recorder_enabled = false;
    uint64_t recording_id = 0;
};

struct DeploymentStorageConfig {
    std::filesystem::path data_dir;
    std::filesystem::path runtime_dir;
    std::filesystem::path schema_dir;
    uint64_t min_free_bytes = 0;
};

struct NodeDeploymentConfig {
    uint32_t schema_version = 0;
    DeploymentNodeConfig node;
    DeploymentSecurityDomainConfig security_domain;
    DeploymentIsolationConfig isolation;
    DeploymentRegionConfig region;
    DeploymentResourceConfig resources;
    DeploymentBridgeConfig bridge;
    DeploymentMonitoringConfig monitoring;
    DeploymentSupervisorConfig supervisor;
    DeploymentStorageConfig storage;
};

Result<NodeDeploymentConfig> ParseNodeDeploymentConfigToml(
    std::string_view text);
Result<NodeDeploymentConfig> LoadNodeDeploymentConfigFromTomlFile(
    std::string_view path);
Result<SecurityDomainIsolationPolicy> ParseSecurityDomainIsolationPolicyToml(
    std::string_view text);
Result<SecurityDomainIsolationPolicy> LoadSecurityDomainIsolationPolicyFromTomlFile(
    std::string_view path);

}  // namespace mino::config

#endif  // MINO_CONFIG_DEPLOYMENT_CONFIG_H_
