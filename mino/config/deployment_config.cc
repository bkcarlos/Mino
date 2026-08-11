// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/config/deployment_config.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <new>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

#include "toml.hpp"

#include "mino/common/status.h"

namespace mino::config {
namespace {

constexpr uint64_t kMebibyte = 1024ull * 1024ull;
constexpr uint64_t kMaximumRegionBytes = 1ull << 30;
constexpr uint64_t kMaximumMemoryBytes = 1ull << 40;
constexpr size_t kMaximumRequestBytes = 16u * 1024u;
constexpr size_t kMaximumResponseBytes = 512u * 1024u;
constexpr size_t kMaximumConnections = 64;
constexpr size_t kMaximumWorkerThreads = 16;
constexpr size_t kMaximumIsolationDomains = 1024;

Status Invalid(std::string message) {
    return Status::Error(StatusCode::kInvalidArgument, std::move(message));
}

bool IsAllowed(std::string_view key,
               std::initializer_list<std::string_view> allowed) {
    return std::find(allowed.begin(), allowed.end(), key) != allowed.end();
}

Status ValidateKeys(const toml::table& table, std::string_view table_name,
                    std::initializer_list<std::string_view> allowed) {
    for (const auto& [key, value] : table) {
        static_cast<void>(value);
        if (!IsAllowed(key.str(), allowed)) {
            return Invalid("unknown key '" + std::string(table_name) + "." +
                           std::string(key.str()) + "'");
        }
    }
    return Status::Ok();
}

Result<const toml::table*> RequiredTable(const toml::table& parent,
                                         std::string_view key,
                                         std::string_view full_name) {
    const toml::node* node = parent.get(key);
    if (node == nullptr) {
        return Invalid("missing required table '" + std::string(full_name) +
                       "'");
    }
    const toml::table* table = node->as_table();
    if (table == nullptr) {
        return Invalid("'" + std::string(full_name) + "' must be a table");
    }
    return table;
}

template <typename T>
Result<T> RequiredInteger(const toml::table& table, std::string_view key,
                          std::string_view full_name, uint64_t minimum,
                          uint64_t maximum) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        return Invalid("missing required key '" + std::string(full_name) + "'");
    }
    const std::optional<int64_t> value = node->value<int64_t>();
    if (!value.has_value() || *value < 0 ||
        static_cast<uint64_t>(*value) < minimum ||
        static_cast<uint64_t>(*value) > maximum ||
        static_cast<uint64_t>(*value) >
            static_cast<uint64_t>(std::numeric_limits<T>::max())) {
        return Invalid("'" + std::string(full_name) +
                       "' is outside its allowed integer range");
    }
    return static_cast<T>(*value);
}

Result<bool> RequiredBool(const toml::table& table, std::string_view key,
                          std::string_view full_name) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        return Invalid("missing required key '" + std::string(full_name) + "'");
    }
    const std::optional<bool> value = node->value<bool>();
    if (!value.has_value()) {
        return Invalid("'" + std::string(full_name) + "' must be a boolean");
    }
    return *value;
}

Result<std::string> RequiredString(const toml::table& table,
                                   std::string_view key,
                                   std::string_view full_name) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        return Invalid("missing required key '" + std::string(full_name) + "'");
    }
    std::optional<std::string> value = node->value<std::string>();
    if (!value.has_value() || value->empty() || value->size() > 4096 ||
        value->find('\0') != std::string::npos) {
        return Invalid("'" + std::string(full_name) +
                       "' must be a bounded non-empty string");
    }
    return std::move(*value);
}

bool ValidToken(std::string_view value) {
    if (value.empty() || value.size() > 64) return false;
    return std::all_of(value.begin(), value.end(), [](unsigned char character) {
        return std::isalnum(character) != 0 || character == '-' ||
               character == '_' || character == '.';
    });
}

bool ValidTopicName(std::string_view value) {
    if (value.empty() || value.size() > 255 || value.front() == '/' ||
        value.back() == '/') {
        return false;
    }
    return std::all_of(value.begin(), value.end(), [](unsigned char character) {
        return std::isalnum(character) != 0 || character == '-' ||
               character == '_' || character == '.' || character == '/';
    });
}

bool ValidAddress(std::string_view value) {
    if (value.empty() || value.size() > 255) return false;
    return std::all_of(value.begin(), value.end(), [](unsigned char character) {
        return std::isalnum(character) != 0 || character == '-' ||
               character == '_' || character == '.' || character == ':';
    });
}

Result<std::filesystem::path> RequiredAbsolutePath(
    const toml::table& table, std::string_view key, std::string_view full_name) {
    auto value = RequiredString(table, key, full_name);
    if (!value.ok()) return value.status();
    std::filesystem::path path(*value);
    if (!path.is_absolute()) {
        return Invalid("'" + std::string(full_name) + "' must be absolute");
    }
    for (const auto& component : path) {
        if (component == "..") {
            return Invalid("'" + std::string(full_name) +
                           "' must not contain parent traversal");
        }
    }
    return path.lexically_normal();
}

bool ContainsInlineCredentialMaterial(std::string_view text) {
    std::string lower(text);
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char character) {
                       return static_cast<char>(std::tolower(character));
                   });
    return lower.find("-----begin certificate-----") != std::string::npos ||
           lower.find("-----begin private key-----") != std::string::npos ||
           lower.find("-----begin encrypted private key-----") !=
               std::string::npos ||
           lower.find("-----begin rsa private key-----") != std::string::npos ||
           lower.find("-----begin ec private key-----") != std::string::npos;
}

Status ValidateCrossFieldConstraints(const NodeDeploymentConfig& config) {
    if (!ValidToken(config.node.name) ||
        !ValidToken(config.node.environment) || !ValidToken(config.node.role) ||
        !ValidToken(config.security_domain.name) ||
        !ValidToken(config.isolation.namespace_name) ||
        !ValidToken(config.region.name)) {
        return Invalid("node, environment, role, security-domain, namespace, and "
                       "region names must be 1-64 token characters");
    }
    if (config.isolation.uid == 0 || config.isolation.gid == 0) {
        return Invalid("security-domain isolation requires non-root UID and GID");
    }
    if (config.region.bytes % 4096 != 0) {
        return Invalid("'region.bytes' must be page aligned");
    }
    if (config.resources.shm_bytes < config.region.bytes) {
        return Invalid("'resources.shm_bytes' must cover 'region.bytes'");
    }
    if (config.resources.memory_bytes < config.resources.shm_bytes) {
        return Invalid("'resources.memory_bytes' must cover shared memory");
    }
    if (!ValidAddress(config.bridge.listen_address) ||
        !ValidAddress(config.monitoring.bind_address)) {
        return Invalid("bridge and monitoring addresses contain invalid characters");
    }
    if (config.bridge.enabled) {
        if (config.bridge.max_connections == 0 ||
            config.bridge.expected_peer_security_domain == 0) {
            return Invalid("enabled bridge requires bounded connections and a peer "
                           "security domain");
        }
        if (config.resources.bridge_connections <
            config.bridge.max_connections) {
            return Invalid("bridge resource budget is below max_connections");
        }
    } else if (config.bridge.max_connections != 0 ||
               config.resources.bridge_connections != 0) {
        return Invalid("disabled bridge must reserve zero bridge connections");
    }
    if (!config.monitoring.enabled && !config.monitoring.otlp_enabled) {
        return Invalid("at least one monitoring exporter must be enabled");
    }
    if (config.supervisor.mode != "local" ||
        !ValidTopicName(config.supervisor.control_topic) ||
        config.supervisor.channel_capacity < 2 ||
        (config.supervisor.channel_capacity &
         (config.supervisor.channel_capacity - 1)) != 0) {
        return Invalid("local supervisor control topic/capacity is invalid");
    }
    if (config.supervisor.recorder_enabled !=
        (config.node.role == "recorder")) {
        return Invalid("recorder role and supervisor.recorder_enabled must agree");
    }
    if (config.supervisor.recorder_enabled !=
        (config.supervisor.recording_id != 0)) {
        return Invalid("enabled recorder requires a positive recording_id and "
                       "disabled recorder requires zero");
    }
    if (config.storage.data_dir == config.storage.runtime_dir ||
        config.storage.data_dir == config.storage.schema_dir ||
        config.storage.runtime_dir == config.storage.schema_dir) {
        return Invalid("storage directories must be distinct");
    }
    return Status::Ok();
}

Result<NodeDeploymentConfig> Parse(const toml::table& root) {
    MINO_RETURN_IF_ERROR(ValidateKeys(
        root, "root", {"schema_version", "node", "security_domain", "isolation",
                       "region", "resources", "bridge", "monitoring", "supervisor",
                       "storage"}));

    NodeDeploymentConfig config;
    auto schema_version = RequiredInteger<uint32_t>(
        root, "schema_version", "schema_version",
        kNodeDeploymentSchemaVersion, kNodeDeploymentSchemaVersion);
    if (!schema_version.ok()) return schema_version.status();
    config.schema_version = *schema_version;

    auto node = RequiredTable(root, "node", "node");
    if (!node.ok()) return node.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(
        **node, "node", {"id", "name", "environment", "role",
                         "shutdown_grace_ms"}));
    auto node_id = RequiredInteger<uint64_t>(**node, "id", "node.id", 1,
                                             std::numeric_limits<int64_t>::max());
    if (!node_id.ok()) return node_id.status();
    config.node.id = *node_id;
    auto node_name = RequiredString(**node, "name", "node.name");
    if (!node_name.ok()) return node_name.status();
    config.node.name = std::move(*node_name);
    auto environment =
        RequiredString(**node, "environment", "node.environment");
    if (!environment.ok()) return environment.status();
    config.node.environment = std::move(*environment);
    auto role = RequiredString(**node, "role", "node.role");
    if (!role.ok()) return role.status();
    config.node.role = std::move(*role);
    auto grace = RequiredInteger<uint32_t>(**node, "shutdown_grace_ms",
                                           "node.shutdown_grace_ms", 100,
                                           120'000);
    if (!grace.ok()) return grace.status();
    config.node.shutdown_grace_ms = *grace;

    auto security_domain =
        RequiredTable(root, "security_domain", "security_domain");
    if (!security_domain.ok()) return security_domain.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(**security_domain, "security_domain",
                                      {"id", "name", "trusted"}));
    auto security_id = RequiredInteger<uint64_t>(
        **security_domain, "id", "security_domain.id", 1,
        std::numeric_limits<int64_t>::max());
    if (!security_id.ok()) return security_id.status();
    config.security_domain.id = *security_id;
    auto security_name =
        RequiredString(**security_domain, "name", "security_domain.name");
    if (!security_name.ok()) return security_name.status();
    config.security_domain.name = std::move(*security_name);
    auto trusted = RequiredBool(**security_domain, "trusted",
                                "security_domain.trusted");
    if (!trusted.ok()) return trusted.status();
    config.security_domain.trusted = *trusted;

    auto isolation = RequiredTable(root, "isolation", "isolation");
    if (!isolation.ok()) return isolation.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(
        **isolation, "isolation",
        {"uid", "gid", "namespace", "policy_file",
         "namespace_attestation_file"}));
    auto isolation_uid = RequiredInteger<uint32_t>(
        **isolation, "uid", "isolation.uid", 1, UINT32_MAX);
    if (!isolation_uid.ok()) return isolation_uid.status();
    config.isolation.uid = *isolation_uid;
    auto isolation_gid = RequiredInteger<uint32_t>(
        **isolation, "gid", "isolation.gid", 1, UINT32_MAX);
    if (!isolation_gid.ok()) return isolation_gid.status();
    config.isolation.gid = *isolation_gid;
    auto namespace_name =
        RequiredString(**isolation, "namespace", "isolation.namespace");
    if (!namespace_name.ok()) return namespace_name.status();
    config.isolation.namespace_name = std::move(*namespace_name);
    auto policy_file = RequiredAbsolutePath(**isolation, "policy_file",
                                            "isolation.policy_file");
    if (!policy_file.ok()) return policy_file.status();
    config.isolation.policy_file = std::move(*policy_file);
    auto attestation_file = RequiredAbsolutePath(
        **isolation, "namespace_attestation_file",
        "isolation.namespace_attestation_file");
    if (!attestation_file.ok()) return attestation_file.status();
    config.isolation.namespace_attestation_file = std::move(*attestation_file);

    auto region = RequiredTable(root, "region", "region");
    if (!region.ok()) return region.status();
    MINO_RETURN_IF_ERROR(
        ValidateKeys(**region, "region", {"id", "name", "bytes"}));
    auto region_id = RequiredInteger<uint32_t>(**region, "id", "region.id", 1,
                                               UINT32_MAX);
    if (!region_id.ok()) return region_id.status();
    config.region.id = *region_id;
    auto region_name = RequiredString(**region, "name", "region.name");
    if (!region_name.ok()) return region_name.status();
    config.region.name = std::move(*region_name);
    auto region_bytes = RequiredInteger<uint64_t>(
        **region, "bytes", "region.bytes", kMebibyte, kMaximumRegionBytes);
    if (!region_bytes.ok()) return region_bytes.status();
    config.region.bytes = *region_bytes;

    auto resources = RequiredTable(root, "resources", "resources");
    if (!resources.ok()) return resources.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(
        **resources, "resources",
        {"memory_bytes", "shm_bytes", "file_descriptors", "threads",
         "bridge_connections"}));
    auto memory = RequiredInteger<uint64_t>(**resources, "memory_bytes",
                                            "resources.memory_bytes",
                                            kMebibyte, kMaximumMemoryBytes);
    if (!memory.ok()) return memory.status();
    config.resources.memory_bytes = *memory;
    auto shm = RequiredInteger<uint64_t>(**resources, "shm_bytes",
                                         "resources.shm_bytes", kMebibyte,
                                         kMaximumRegionBytes);
    if (!shm.ok()) return shm.status();
    config.resources.shm_bytes = *shm;
    auto descriptors = RequiredInteger<uint64_t>(
        **resources, "file_descriptors", "resources.file_descriptors", 64,
        1'048'576);
    if (!descriptors.ok()) return descriptors.status();
    config.resources.file_descriptors = *descriptors;
    auto threads = RequiredInteger<uint64_t>(**resources, "threads",
                                             "resources.threads", 1, 65'536);
    if (!threads.ok()) return threads.status();
    config.resources.threads = *threads;
    auto bridge_connections = RequiredInteger<uint64_t>(
        **resources, "bridge_connections", "resources.bridge_connections", 0,
        65'535);
    if (!bridge_connections.ok()) return bridge_connections.status();
    config.resources.bridge_connections = *bridge_connections;

    auto bridge = RequiredTable(root, "bridge", "bridge");
    if (!bridge.ok()) return bridge.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(
        **bridge, "bridge",
        {"enabled", "listen_address", "port",
         "expected_peer_security_domain", "max_connections", "tls"}));
    auto bridge_enabled = RequiredBool(**bridge, "enabled", "bridge.enabled");
    if (!bridge_enabled.ok()) return bridge_enabled.status();
    config.bridge.enabled = *bridge_enabled;
    auto listen =
        RequiredString(**bridge, "listen_address", "bridge.listen_address");
    if (!listen.ok()) return listen.status();
    config.bridge.listen_address = std::move(*listen);
    auto bridge_port = RequiredInteger<uint16_t>(**bridge, "port", "bridge.port",
                                                 1, UINT16_MAX);
    if (!bridge_port.ok()) return bridge_port.status();
    config.bridge.port = *bridge_port;
    auto peer_domain = RequiredInteger<uint64_t>(
        **bridge, "expected_peer_security_domain",
        "bridge.expected_peer_security_domain", 1,
        std::numeric_limits<int64_t>::max());
    if (!peer_domain.ok()) return peer_domain.status();
    config.bridge.expected_peer_security_domain = *peer_domain;
    auto max_connections = RequiredInteger<uint32_t>(
        **bridge, "max_connections", "bridge.max_connections", 0, 65'535);
    if (!max_connections.ok()) return max_connections.status();
    config.bridge.max_connections = *max_connections;
    auto tls = RequiredTable(**bridge, "tls", "bridge.tls");
    if (!tls.ok()) return tls.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(
        **tls, "bridge.tls",
        {"trust_anchors_file", "certificate_chain_file", "private_key_file"}));
    auto trust = RequiredAbsolutePath(**tls, "trust_anchors_file",
                                      "bridge.tls.trust_anchors_file");
    if (!trust.ok()) return trust.status();
    config.bridge.tls.trust_anchors_file = std::move(*trust);
    auto certificate = RequiredAbsolutePath(
        **tls, "certificate_chain_file", "bridge.tls.certificate_chain_file");
    if (!certificate.ok()) return certificate.status();
    config.bridge.tls.certificate_chain_file = std::move(*certificate);
    auto key = RequiredAbsolutePath(**tls, "private_key_file",
                                    "bridge.tls.private_key_file");
    if (!key.ok()) return key.status();
    config.bridge.tls.private_key_file = std::move(*key);

    auto monitoring = RequiredTable(root, "monitoring", "monitoring");
    if (!monitoring.ok()) return monitoring.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(
        **monitoring, "monitoring",
        {"enabled", "otlp_enabled", "bind_address", "port",
         "aggregate_interval_ms", "request_bytes_limit", "header_count_limit",
         "response_bytes_limit", "connection_limit", "worker_threads",
         "read_timeout_ms", "write_timeout_ms", "accept_poll_ms"}));
    auto monitoring_enabled =
        RequiredBool(**monitoring, "enabled", "monitoring.enabled");
    if (!monitoring_enabled.ok()) return monitoring_enabled.status();
    config.monitoring.enabled = *monitoring_enabled;
    auto otlp_enabled =
        RequiredBool(**monitoring, "otlp_enabled", "monitoring.otlp_enabled");
    if (!otlp_enabled.ok()) return otlp_enabled.status();
    config.monitoring.otlp_enabled = *otlp_enabled;
    auto bind = RequiredString(**monitoring, "bind_address",
                               "monitoring.bind_address");
    if (!bind.ok()) return bind.status();
    config.monitoring.bind_address = std::move(*bind);
    auto monitoring_port = RequiredInteger<uint16_t>(
        **monitoring, "port", "monitoring.port", 1, UINT16_MAX);
    if (!monitoring_port.ok()) return monitoring_port.status();
    config.monitoring.port = *monitoring_port;
    auto aggregate = RequiredInteger<uint32_t>(
        **monitoring, "aggregate_interval_ms",
        "monitoring.aggregate_interval_ms", 100, 60'000);
    if (!aggregate.ok()) return aggregate.status();
    config.monitoring.aggregate_interval_ms = *aggregate;
    auto request_limit = RequiredInteger<size_t>(
        **monitoring, "request_bytes_limit", "monitoring.request_bytes_limit", 1,
        kMaximumRequestBytes);
    if (!request_limit.ok()) return request_limit.status();
    config.monitoring.request_bytes_limit = *request_limit;
    auto header_limit = RequiredInteger<size_t>(
        **monitoring, "header_count_limit", "monitoring.header_count_limit", 1,
        256);
    if (!header_limit.ok()) return header_limit.status();
    config.monitoring.header_count_limit = *header_limit;
    auto response_limit = RequiredInteger<size_t>(
        **monitoring, "response_bytes_limit", "monitoring.response_bytes_limit",
        1024, kMaximumResponseBytes);
    if (!response_limit.ok()) return response_limit.status();
    config.monitoring.response_bytes_limit = *response_limit;
    auto connection_limit = RequiredInteger<size_t>(
        **monitoring, "connection_limit", "monitoring.connection_limit", 1,
        kMaximumConnections);
    if (!connection_limit.ok()) return connection_limit.status();
    config.monitoring.connection_limit = *connection_limit;
    auto workers = RequiredInteger<size_t>(
        **monitoring, "worker_threads", "monitoring.worker_threads", 1,
        kMaximumWorkerThreads);
    if (!workers.ok()) return workers.status();
    config.monitoring.worker_threads = *workers;
    auto read_timeout = RequiredInteger<uint32_t>(
        **monitoring, "read_timeout_ms", "monitoring.read_timeout_ms", 1,
        60'000);
    if (!read_timeout.ok()) return read_timeout.status();
    config.monitoring.read_timeout_ms = *read_timeout;
    auto write_timeout = RequiredInteger<uint32_t>(
        **monitoring, "write_timeout_ms", "monitoring.write_timeout_ms", 1,
        60'000);
    if (!write_timeout.ok()) return write_timeout.status();
    config.monitoring.write_timeout_ms = *write_timeout;
    auto accept_poll = RequiredInteger<uint32_t>(
        **monitoring, "accept_poll_ms", "monitoring.accept_poll_ms", 1, 5000);
    if (!accept_poll.ok()) return accept_poll.status();
    config.monitoring.accept_poll_ms = *accept_poll;

    auto supervisor = RequiredTable(root, "supervisor", "supervisor");
    if (!supervisor.ok()) return supervisor.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(
        **supervisor, "supervisor",
        {"mode", "control_topic", "channel_capacity", "max_subscribers",
         "max_payload_bytes", "recorder_enabled", "recording_id"}));
    auto supervisor_mode =
        RequiredString(**supervisor, "mode", "supervisor.mode");
    if (!supervisor_mode.ok()) return supervisor_mode.status();
    config.supervisor.mode = std::move(*supervisor_mode);
    auto control_topic = RequiredString(**supervisor, "control_topic",
                                        "supervisor.control_topic");
    if (!control_topic.ok()) return control_topic.status();
    config.supervisor.control_topic = std::move(*control_topic);
    auto channel_capacity = RequiredInteger<uint32_t>(
        **supervisor, "channel_capacity", "supervisor.channel_capacity", 2,
        65'536);
    if (!channel_capacity.ok()) return channel_capacity.status();
    config.supervisor.channel_capacity = *channel_capacity;
    auto max_subscribers = RequiredInteger<uint32_t>(
        **supervisor, "max_subscribers", "supervisor.max_subscribers", 1, 64);
    if (!max_subscribers.ok()) return max_subscribers.status();
    config.supervisor.max_subscribers = *max_subscribers;
    auto max_payload = RequiredInteger<size_t>(
        **supervisor, "max_payload_bytes", "supervisor.max_payload_bytes", 1,
        16u * 1024u * 1024u);
    if (!max_payload.ok()) return max_payload.status();
    config.supervisor.max_payload_bytes = *max_payload;
    auto recorder_enabled = RequiredBool(**supervisor, "recorder_enabled",
                                         "supervisor.recorder_enabled");
    if (!recorder_enabled.ok()) return recorder_enabled.status();
    config.supervisor.recorder_enabled = *recorder_enabled;
    auto recording_id = RequiredInteger<uint64_t>(
        **supervisor, "recording_id", "supervisor.recording_id", 0,
        std::numeric_limits<int64_t>::max());
    if (!recording_id.ok()) return recording_id.status();
    config.supervisor.recording_id = *recording_id;

    auto storage = RequiredTable(root, "storage", "storage");
    if (!storage.ok()) return storage.status();
    MINO_RETURN_IF_ERROR(ValidateKeys(
        **storage, "storage",
        {"data_dir", "runtime_dir", "schema_dir", "min_free_bytes"}));
    auto data_dir =
        RequiredAbsolutePath(**storage, "data_dir", "storage.data_dir");
    if (!data_dir.ok()) return data_dir.status();
    config.storage.data_dir = std::move(*data_dir);
    auto runtime_dir =
        RequiredAbsolutePath(**storage, "runtime_dir", "storage.runtime_dir");
    if (!runtime_dir.ok()) return runtime_dir.status();
    config.storage.runtime_dir = std::move(*runtime_dir);
    auto schema_dir =
        RequiredAbsolutePath(**storage, "schema_dir", "storage.schema_dir");
    if (!schema_dir.ok()) return schema_dir.status();
    config.storage.schema_dir = std::move(*schema_dir);
    auto min_free = RequiredInteger<uint64_t>(
        **storage, "min_free_bytes", "storage.min_free_bytes", kMebibyte,
        kMaximumMemoryBytes);
    if (!min_free.ok()) return min_free.status();
    config.storage.min_free_bytes = *min_free;

    MINO_RETURN_IF_ERROR(ValidateCrossFieldConstraints(config));
    return config;
}

Result<SecurityDomainIsolationPolicy> ParsePolicy(const toml::table& root) {
    MINO_RETURN_IF_ERROR(
        ValidateKeys(root, "root", {"schema_version", "domains"}));
    SecurityDomainIsolationPolicy policy;
    auto schema_version = RequiredInteger<uint32_t>(
        root, "schema_version", "schema_version", 1, 1);
    if (!schema_version.ok()) return schema_version.status();
    policy.schema_version = *schema_version;

    const toml::node* domains_node = root.get("domains");
    const toml::array* domains =
        domains_node == nullptr ? nullptr : domains_node->as_array();
    if (domains == nullptr || domains->empty() ||
        domains->size() > kMaximumIsolationDomains) {
        return Invalid("'domains' must be a non-empty bounded array of tables");
    }
    policy.domains.reserve(domains->size());
    for (size_t index = 0; index < domains->size(); ++index) {
        const toml::table* domain = (*domains)[index].as_table();
        if (domain == nullptr) {
            return Invalid("every isolation domains entry must be a table");
        }
        const std::string prefix = "domains[" + std::to_string(index) + "]";
        MINO_RETURN_IF_ERROR(ValidateKeys(
            *domain, prefix,
            {"security_domain_id", "trusted", "uid", "gid", "namespace"}));
        SecurityDomainIsolationBinding binding;
        auto id = RequiredInteger<uint64_t>(
            *domain, "security_domain_id", prefix + ".security_domain_id", 1,
            std::numeric_limits<int64_t>::max());
        if (!id.ok()) return id.status();
        binding.security_domain_id = *id;
        auto trusted = RequiredBool(*domain, "trusted", prefix + ".trusted");
        if (!trusted.ok()) return trusted.status();
        binding.trusted = *trusted;
        auto uid = RequiredInteger<uint32_t>(*domain, "uid", prefix + ".uid", 1,
                                             UINT32_MAX);
        if (!uid.ok()) return uid.status();
        binding.uid = *uid;
        auto gid = RequiredInteger<uint32_t>(*domain, "gid", prefix + ".gid", 1,
                                             UINT32_MAX);
        if (!gid.ok()) return gid.status();
        binding.gid = *gid;
        auto namespace_name =
            RequiredString(*domain, "namespace", prefix + ".namespace");
        if (!namespace_name.ok()) return namespace_name.status();
        if (!ValidToken(*namespace_name)) {
            return Invalid("isolation namespace must be a bounded token");
        }
        binding.namespace_name = std::move(*namespace_name);
        policy.domains.push_back(std::move(binding));
    }

    for (size_t left = 0; left < policy.domains.size(); ++left) {
        for (size_t right = left + 1; right < policy.domains.size(); ++right) {
            const auto& first = policy.domains[left];
            const auto& second = policy.domains[right];
            if (first.security_domain_id == second.security_domain_id) {
                return Invalid("isolation policy declares a SecurityDomainId more "
                               "than once");
            }
            if (!first.trusted || !second.trusted) {
                if (first.uid == second.uid) {
                    return Invalid("untrusted security domains must not share a UID");
                }
                if (first.gid == second.gid) {
                    return Invalid("untrusted security domains must not share a GID");
                }
                if (first.namespace_name == second.namespace_name) {
                    return Invalid("untrusted security domains must not share a "
                                   "namespace");
                }
            }
        }
    }
    return policy;
}

}  // namespace

Result<NodeDeploymentConfig> ParseNodeDeploymentConfigToml(
    std::string_view text) {
    if (text.empty() || text.size() > kMaximumNodeDeploymentConfigBytes) {
        return Invalid("node deployment configuration is empty or too large");
    }
    try {
        if (ContainsInlineCredentialMaterial(text)) {
            return Invalid("inline certificate or private-key material is forbidden; "
                           "configure runtime-mounted file references only");
        }
        return Parse(toml::parse(text));
    } catch (const toml::parse_error& error) {
        std::ostringstream message;
        message << "invalid TOML: " << error.description();
        return Invalid(message.str());
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate node deployment configuration");
    }
}

Result<SecurityDomainIsolationPolicy> ParseSecurityDomainIsolationPolicyToml(
    std::string_view text) {
    if (text.empty() || text.size() > kMaximumNodeDeploymentConfigBytes) {
        return Invalid("security-domain isolation policy is empty or too large");
    }
    try {
        if (ContainsInlineCredentialMaterial(text)) {
            return Invalid("inline credential material is forbidden in isolation "
                           "policy");
        }
        return ParsePolicy(toml::parse(text));
    } catch (const toml::parse_error& error) {
        std::ostringstream message;
        message << "invalid isolation policy TOML: " << error.description();
        return Invalid(message.str());
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate security-domain isolation policy");
    }
}

Result<NodeDeploymentConfig> LoadNodeDeploymentConfigFromTomlFile(
    std::string_view path) {
    std::ifstream input(std::string(path), std::ios::binary | std::ios::ate);
    if (!input) {
        return Status::Error(StatusCode::kNotFound,
                             "cannot open node deployment configuration '" +
                                 std::string(path) + "'");
    }
    const std::streamsize size = input.tellg();
    if (size <= 0 ||
        static_cast<uint64_t>(size) > kMaximumNodeDeploymentConfigBytes) {
        return Invalid("node deployment configuration is empty or too large");
    }
    std::string contents(static_cast<size_t>(size), '\0');
    input.seekg(0);
    if (!input.read(contents.data(), size)) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot read node deployment configuration '" +
                                 std::string(path) + "'");
    }
    return ParseNodeDeploymentConfigToml(contents);
}

Result<SecurityDomainIsolationPolicy>
LoadSecurityDomainIsolationPolicyFromTomlFile(std::string_view path) {
    std::ifstream input(std::string(path), std::ios::binary | std::ios::ate);
    if (!input) {
        return Status::Error(StatusCode::kNotFound,
                             "cannot open security-domain isolation policy '" +
                                 std::string(path) + "'");
    }
    const std::streamsize size = input.tellg();
    if (size <= 0 ||
        static_cast<uint64_t>(size) > kMaximumNodeDeploymentConfigBytes) {
        return Invalid("security-domain isolation policy is empty or too large");
    }
    std::string contents(static_cast<size_t>(size), '\0');
    input.seekg(0);
    if (!input.read(contents.data(), size)) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot read security-domain isolation policy '" +
                                 std::string(path) + "'");
    }
    return ParseSecurityDomainIsolationPolicyToml(contents);
}

}  // namespace mino::config
