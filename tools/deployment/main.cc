// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/config/deployment_config.h"

namespace {

using mino::Status;
using mino::StatusCode;
using mino::config::NodeDeploymentConfig;

constexpr int kExitSuccess = 0;
constexpr int kExitUsage = 2;
constexpr int kExitConfig = 3;
constexpr int kExitPreflight = 4;
constexpr int kExitLaunch = 5;
constexpr int kExitProbe = 6;

volatile sig_atomic_t g_requested_signal = 0;
volatile sig_atomic_t g_child_pid = -1;

void ForwardSignal(int signal_number) {
    if (g_requested_signal != 0) {
        const pid_t child = static_cast<pid_t>(g_child_pid);
        if (child > 0) (void)::kill(child, SIGKILL);
        return;
    }
    g_requested_signal = signal_number;
    const pid_t child = static_cast<pid_t>(g_child_pid);
    if (child > 0) (void)::kill(child, signal_number);
}

Status Invalid(std::string message) {
    return Status::Error(StatusCode::kInvalidArgument, std::move(message));
}

Status Denied(std::string message) {
    return Status::Error(StatusCode::kPermissionDenied, std::move(message));
}

void PrintUsage(std::ostream& out) {
    out << R"(mino-deploy: deterministic Mino node deployment tool

USAGE:
  mino-deploy generate --environment <development|staging|production>
                       --role <core|edge|recorder> --node-id <id>
                       --security-domain-id <id> --region-id <id>
                       --service-uid <uid> --service-gid <gid>
                       --namespace <deployment-namespace> [--output <new-file>]
  mino-deploy validate --config <file>
  mino-deploy preflight --config <file>
  mino-deploy start --config <file> [--dry-run] -- <absolute-node-binary> [args...]
  mino-deploy probe --config <file> --kind <health|readiness>

EXIT CODES:
  0 success; 2 usage; 3 invalid config; 4 preflight failure;
  5 launcher failure; 6 probe failure. A started child exit status is preserved;
  a forwarded signal returns 128 + signal number.
)";
}

template <typename T>
bool ParseUnsigned(std::string_view text, T* output) {
    if (text.empty()) return false;
    uint64_t value = 0;
    const auto [end, error] =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc() || end != text.data() + text.size() || value == 0 ||
        value > static_cast<uint64_t>(std::numeric_limits<T>::max())) {
        return false;
    }
    *output = static_cast<T>(value);
    return true;
}

struct TemplateOptions {
    std::string environment;
    std::string role;
    uint64_t node_id = 0;
    uint64_t security_domain_id = 0;
    uint32_t region_id = 0;
    uint32_t service_uid = 0;
    uint32_t service_gid = 0;
    std::string namespace_name;
    std::string output_path;
};

struct TemplateLimits {
    uint64_t region_bytes = 0;
    uint64_t memory_bytes = 0;
    uint64_t file_descriptors = 0;
    uint64_t threads = 0;
    uint32_t bridge_connections = 0;
};

mino::Result<TemplateLimits> SelectTemplate(const TemplateOptions& options) {
    TemplateLimits limits;
    if (options.environment == "development") {
        limits = {.region_bytes = 67'108'864,
                  .memory_bytes = 268'435'456,
                  .file_descriptors = 1024,
                  .threads = 64};
    } else if (options.environment == "staging") {
        limits = {.region_bytes = 268'435'456,
                  .memory_bytes = 536'870'912,
                  .file_descriptors = 4096,
                  .threads = 128};
    } else if (options.environment == "production") {
        limits = {.region_bytes = 536'870'912,
                  .memory_bytes = 1'073'741'824,
                  .file_descriptors = 8192,
                  .threads = 256};
    } else {
        return Invalid("unsupported environment template '" +
                       options.environment + "'");
    }

    if (options.role == "edge") {
        limits.bridge_connections = options.environment == "production"   ? 64
                                    : options.environment == "staging" ? 32
                                                                        : 8;
    } else if (options.role == "core") {
        limits.bridge_connections = options.environment == "production"   ? 32
                                    : options.environment == "staging" ? 16
                                                                        : 4;
    } else if (options.role != "recorder") {
        return Invalid("unsupported role template '" + options.role + "'");
    }
    return limits;
}

mino::Result<std::string> RenderTemplate(const TemplateOptions& options) {
    auto limits = SelectTemplate(options);
    if (!limits.ok()) return limits.status();
    const bool bridge_enabled = limits->bridge_connections != 0;
    std::ostringstream output;
    output << "# Generated by mino-deploy. Secrets are runtime-mounted file "
              "references only.\n"
           << "schema_version = 1\n\n"
           << "[node]\n"
           << "id = " << options.node_id << "\n"
           << "name = \"mino-" << options.environment << '-' << options.role
           << '-' << options.node_id << "\"\n"
           << "environment = \"" << options.environment << "\"\n"
           << "role = \"" << options.role << "\"\n"
           << "shutdown_grace_ms = 30000\n\n"
           << "[security_domain]\n"
           << "id = " << options.security_domain_id << "\n"
           << "name = \"domain-" << options.security_domain_id << "\"\n"
           << "trusted = false\n\n"
           << "[isolation]\n"
           << "uid = " << options.service_uid << "\n"
           << "gid = " << options.service_gid << "\n"
           << "namespace = \"" << options.namespace_name << "\"\n"
           << "policy_file = \"/etc/mino/security-domains.toml\"\n"
           << "namespace_attestation_file = "
              "\"/run/secrets/mino/security-domain.namespace\"\n\n"
           << "[region]\n"
           << "id = " << options.region_id << "\n"
           << "name = \"region-" << options.region_id << "\"\n"
           << "bytes = " << limits->region_bytes << "\n\n"
           << "[resources]\n"
           << "memory_bytes = " << limits->memory_bytes << "\n"
           << "shm_bytes = " << limits->region_bytes << "\n"
           << "file_descriptors = " << limits->file_descriptors << "\n"
           << "threads = " << limits->threads << "\n"
           << "bridge_connections = " << limits->bridge_connections << "\n\n"
           << "[bridge]\n"
           << "enabled = " << (bridge_enabled ? "true" : "false") << "\n"
           << "listen_address = \""
           << (bridge_enabled ? "0.0.0.0" : "127.0.0.1") << "\"\n"
           << "port = 7443\n"
           << "expected_peer_security_domain = "
           << options.security_domain_id << "\n"
           << "max_connections = " << limits->bridge_connections << "\n\n"
           << "[bridge.tls]\n"
           << "trust_anchors_file = \"/run/secrets/mino/ca.pem\"\n"
           << "certificate_chain_file = \"/run/secrets/mino/tls.crt\"\n"
           << "private_key_file = \"/run/secrets/mino/tls.key\"\n\n"
           << "[monitoring]\n"
           << "enabled = true\n"
           << "otlp_enabled = false\n"
           << "bind_address = \"127.0.0.1\"\n"
           << "port = 9464\n"
           << "aggregate_interval_ms = 1000\n"
           << "request_bytes_limit = 4096\n"
           << "header_count_limit = 32\n"
           << "response_bytes_limit = 262144\n"
           << "connection_limit = 16\n"
           << "worker_threads = 2\n"
           << "read_timeout_ms = 1000\n"
           << "write_timeout_ms = 2000\n"
           << "accept_poll_ms = 100\n\n"
           << "[supervisor]\n"
           << "mode = \"local\"\n"
           << "control_topic = \"mino/control\"\n"
           << "channel_capacity = 16\n"
           << "max_subscribers = 4\n"
           << "max_payload_bytes = 4096\n"
           << "recorder_enabled = "
           << (options.role == "recorder" ? "true" : "false") << "\n"
           << "recording_id = "
           << (options.role == "recorder" ? options.node_id : 0) << "\n\n"
           << "[storage]\n"
           << "data_dir = \"/var/lib/mino/data\"\n"
           << "runtime_dir = \"/run/mino\"\n"
           << "schema_dir = \"/var/lib/mino/schemas\"\n"
           << "min_free_bytes = " << limits->region_bytes << "\n";
    std::string rendered = output.str();
    auto parsed = mino::config::ParseNodeDeploymentConfigToml(rendered);
    if (!parsed.ok()) return parsed.status();
    return rendered;
}

Status WriteNewFile(std::string_view path, std::string_view contents) {
    const int fd = ::open(std::string(path).c_str(),
                          O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                          0644);
    if (fd < 0) {
        return Denied("cannot create output file '" + std::string(path) +
                      "': " + std::strerror(errno));
    }
    size_t offset = 0;
    while (offset < contents.size()) {
        const ssize_t written =
            ::write(fd, contents.data() + offset, contents.size() - offset);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) {
            const int saved_errno = errno;
            ::close(fd);
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot write generated configuration: " +
                                     std::string(std::strerror(saved_errno)));
        }
        offset += static_cast<size_t>(written);
    }
    if (::fsync(fd) != 0 || ::close(fd) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot persist generated configuration");
    }
    return Status::Ok();
}

Status CheckConfigFile(const std::filesystem::path& path) {
    struct stat state {};
    if (::lstat(path.c_str(), &state) != 0) {
        return Invalid("cannot stat configuration file '" + path.string() +
                       "': " + std::strerror(errno));
    }
    if (!S_ISREG(state.st_mode) || state.st_nlink != 1 ||
        state.st_size <= 0 ||
        static_cast<uint64_t>(state.st_size) >
            mino::config::kMaximumNodeDeploymentConfigBytes) {
        return Denied("configuration must be a bounded single-link regular file");
    }
    if ((state.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        return Denied("configuration must not be group/world writable");
    }
    return Status::Ok();
}

Status CheckDirectory(const std::filesystem::path& path,
                      std::string_view purpose) {
    struct stat state {};
    if (::lstat(path.c_str(), &state) != 0) {
        return Denied(std::string(purpose) + " directory '" + path.string() +
                      "' is unavailable: " + std::strerror(errno));
    }
    if (!S_ISDIR(state.st_mode) || state.st_uid != ::geteuid()) {
        return Denied(std::string(purpose) +
                      " directory must be owned by the service uid and not be a "
                      "symlink");
    }
    constexpr mode_t kOwnerAccess = S_IRUSR | S_IWUSR | S_IXUSR;
    if ((state.st_mode & kOwnerAccess) != kOwnerAccess ||
        (state.st_mode & S_IWOTH) != 0) {
        return Denied(std::string(purpose) +
                      " directory requires owner rwx and no world write");
    }
    return Status::Ok();
}

Status CheckCredentialFile(const std::filesystem::path& path,
                           bool private_key) {
    struct stat state {};
    if (::lstat(path.c_str(), &state) != 0) {
        return Denied("TLS credential '" + path.string() +
                      "' is unavailable: " + std::strerror(errno));
    }
    if (!S_ISREG(state.st_mode) || state.st_nlink != 1 ||
        state.st_uid != ::geteuid() || state.st_size <= 0 ||
        state.st_size > 1024 * 1024) {
        return Denied("TLS credentials must be bounded, single-link regular files "
                      "owned by the service uid");
    }
    if ((state.st_mode & S_IRUSR) == 0) {
        return Denied("TLS credential owner must have read permission");
    }
    if (private_key) {
        if ((state.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
            return Denied("TLS private key permissions must be 0600 or stricter");
        }
    } else if ((state.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        return Denied("TLS certificate/trust files must not be group/world writable");
    }
    return Status::Ok();
}

Status CheckNamespaceAttestation(const NodeDeploymentConfig& config) {
    const std::filesystem::path& path =
        config.isolation.namespace_attestation_file;
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return Denied("namespace attestation '" + path.string() +
                      "' is unavailable: " + std::strerror(errno));
    }
    struct stat state {};
    if (::fstat(fd, &state) != 0 || !S_ISREG(state.st_mode) ||
        state.st_nlink != 1 || state.st_size <= 0 || state.st_size > 256 ||
        (state.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        ::close(fd);
        return Denied("namespace attestation must be a bounded, single-link, "
                      "non-group/world-writable regular file");
    }
    std::string contents(static_cast<size_t>(state.st_size), '\0');
    size_t offset = 0;
    while (offset < contents.size()) {
        const ssize_t count =
            ::read(fd, contents.data() + offset, contents.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            ::close(fd);
            return Denied("namespace attestation changed while reading");
        }
        offset += static_cast<size_t>(count);
    }
    ::close(fd);
    if (!contents.empty() && contents.back() == '\n') contents.pop_back();
    if (contents != config.isolation.namespace_name) {
        return Denied("runtime namespace attestation does not match the declared "
                      "security-domain namespace");
    }
    return Status::Ok();
}

Status CheckIsolationPolicy(const NodeDeploymentConfig& config) {
    if (::geteuid() != static_cast<uid_t>(config.isolation.uid) ||
        ::getegid() != static_cast<gid_t>(config.isolation.gid)) {
        return Denied("process EUID/EGID do not match the security-domain "
                      "isolation binding");
    }
    MINO_RETURN_IF_ERROR(CheckConfigFile(config.isolation.policy_file));
    auto policy = mino::config::LoadSecurityDomainIsolationPolicyFromTomlFile(
        config.isolation.policy_file.string());
    if (!policy.ok()) return policy.status();

    const mino::config::SecurityDomainIsolationBinding* matched = nullptr;
    for (const auto& binding : policy->domains) {
        if (binding.security_domain_id == config.security_domain.id) {
            matched = &binding;
            break;
        }
    }
    if (matched == nullptr || matched->trusted != config.security_domain.trusted ||
        matched->uid != config.isolation.uid ||
        matched->gid != config.isolation.gid ||
        matched->namespace_name != config.isolation.namespace_name) {
        return Denied("security domain is absent from or disagrees with the "
                      "UID/GID/namespace isolation policy");
    }
    return CheckNamespaceAttestation(config);
}

Status CheckResources(const NodeDeploymentConfig& config) {
    struct rlimit limit {};
    if (::getrlimit(RLIMIT_NOFILE, &limit) != 0 ||
        (limit.rlim_cur != RLIM_INFINITY &&
         config.resources.file_descriptors > limit.rlim_cur)) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "RLIMIT_NOFILE is below resources.file_descriptors");
    }
#ifdef RLIMIT_NPROC
    if (::getrlimit(RLIMIT_NPROC, &limit) == 0 &&
        limit.rlim_cur != RLIM_INFINITY && config.resources.threads > limit.rlim_cur) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "RLIMIT_NPROC is below resources.threads");
    }
#endif
#ifdef RLIMIT_AS
    if (::getrlimit(RLIMIT_AS, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY &&
        config.resources.memory_bytes > limit.rlim_cur) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "RLIMIT_AS is below resources.memory_bytes");
    }
#endif
    return Status::Ok();
}

Status CheckFreeSpace(const NodeDeploymentConfig& config) {
    struct statvfs state {};
    if (::statvfs(config.storage.data_dir.c_str(), &state) != 0) {
        return Denied("cannot inspect storage free space: " +
                      std::string(std::strerror(errno)));
    }
    const uint64_t free_bytes =
        state.f_frsize == 0 || state.f_bavail >
                                   std::numeric_limits<uint64_t>::max() /
                                       state.f_frsize
            ? std::numeric_limits<uint64_t>::max()
            : static_cast<uint64_t>(state.f_bavail) * state.f_frsize;
    if (free_bytes < config.storage.min_free_bytes) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "storage free space is below storage.min_free_bytes");
    }
    return Status::Ok();
}

Status Preflight(const NodeDeploymentConfig& config) {
    // SecurityDomainId is only an anti-misattachment identity. OS identity and
    // the externally attested deployment namespace are the actual isolation.
    MINO_RETURN_IF_ERROR(CheckIsolationPolicy(config));
    MINO_RETURN_IF_ERROR(CheckDirectory(config.storage.data_dir, "data"));
    MINO_RETURN_IF_ERROR(CheckDirectory(config.storage.runtime_dir, "runtime"));
    MINO_RETURN_IF_ERROR(CheckDirectory(config.storage.schema_dir, "schema"));
    if (config.bridge.enabled) {
        MINO_RETURN_IF_ERROR(CheckCredentialFile(
            config.bridge.tls.trust_anchors_file, false));
        MINO_RETURN_IF_ERROR(CheckCredentialFile(
            config.bridge.tls.certificate_chain_file, false));
        MINO_RETURN_IF_ERROR(
            CheckCredentialFile(config.bridge.tls.private_key_file, true));
    }
    MINO_RETURN_IF_ERROR(CheckResources(config));
    return CheckFreeSpace(config);
}

Status CheckExecutable(const std::filesystem::path& path) {
    if (!path.is_absolute()) {
        return Invalid("node executable path must be absolute");
    }
    struct stat state {};
    if (::lstat(path.c_str(), &state) != 0 || !S_ISREG(state.st_mode) ||
        (state.st_mode & (S_IWGRP | S_IWOTH)) != 0 ||
        ::access(path.c_str(), X_OK) != 0) {
        return Denied("node executable must be a non-writable executable regular "
                      "file");
    }
    return Status::Ok();
}

mino::Result<NodeDeploymentConfig> LoadChecked(std::string_view config_path) {
    const std::filesystem::path path(config_path);
    const Status checked = CheckConfigFile(path);
    if (!checked.ok()) return checked;
    return mino::config::LoadNodeDeploymentConfigFromTomlFile(config_path);
}

Status ProbeMonitoring(const NodeDeploymentConfig& config) {
    std::string host = config.monitoring.bind_address;
    if (host == "0.0.0.0") host = "127.0.0.1";
    if (host == "::") host = "::1";
    const std::string port = std::to_string(config.monitoring.port);
    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* addresses = nullptr;
    const int lookup = ::getaddrinfo(host.c_str(), port.c_str(), &hints,
                                     &addresses);
    if (lookup != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot resolve monitoring endpoint");
    }

    int socket_fd = -1;
    for (const struct addrinfo* address = addresses; address != nullptr;
         address = address->ai_next) {
        socket_fd = ::socket(address->ai_family, address->ai_socktype,
                             address->ai_protocol);
        if (socket_fd < 0) continue;
        const timeval timeout{.tv_sec = 2, .tv_usec = 0};
        (void)::setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                           sizeof(timeout));
        (void)::setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                           sizeof(timeout));
        if (::connect(socket_fd, address->ai_addr, address->ai_addrlen) == 0) {
            break;
        }
        ::close(socket_fd);
        socket_fd = -1;
    }
    ::freeaddrinfo(addresses);
    if (socket_fd < 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "monitoring endpoint is not reachable");
    }

    constexpr std::string_view request =
        "GET /-/healthy HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    size_t offset = 0;
    while (offset < request.size()) {
        const ssize_t count = ::send(socket_fd, request.data() + offset,
                                     request.size() - offset, 0);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            ::close(socket_fd);
            return Status::Error(StatusCode::kUnavailable,
                                 "monitoring health request failed");
        }
        offset += static_cast<size_t>(count);
    }
    char response[64] = {};
    const ssize_t count = ::recv(socket_fd, response, sizeof(response) - 1, 0);
    ::close(socket_fd);
    if (count <= 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "monitoring health response is empty");
    }
    const std::string_view status(response, static_cast<size_t>(count));
    if (status.substr(0, 12) != "HTTP/1.0 200" &&
        status.substr(0, 12) != "HTTP/1.1 200") {
        return Status::Error(StatusCode::kUnavailable,
                             "monitoring health endpoint is not healthy");
    }
    return Status::Ok();
}

int CmdGenerate(int argc, char** argv) {
    TemplateOptions options;
    for (int index = 0; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        auto next = [&](std::string_view flag) -> const char* {
            if (index + 1 >= argc) {
                std::cerr << "mino-deploy: " << flag << " requires a value\n";
                return nullptr;
            }
            return argv[++index];
        };
        if (argument == "--environment") {
            const char* value = next(argument);
            if (value == nullptr) return kExitUsage;
            options.environment = value;
        } else if (argument == "--role") {
            const char* value = next(argument);
            if (value == nullptr) return kExitUsage;
            options.role = value;
        } else if (argument == "--node-id") {
            const char* value = next(argument);
            if (value == nullptr || !ParseUnsigned(value, &options.node_id)) {
                std::cerr << "mino-deploy: invalid --node-id\n";
                return kExitUsage;
            }
        } else if (argument == "--security-domain-id") {
            const char* value = next(argument);
            if (value == nullptr ||
                !ParseUnsigned(value, &options.security_domain_id)) {
                std::cerr << "mino-deploy: invalid --security-domain-id\n";
                return kExitUsage;
            }
        } else if (argument == "--region-id") {
            const char* value = next(argument);
            if (value == nullptr || !ParseUnsigned(value, &options.region_id)) {
                std::cerr << "mino-deploy: invalid --region-id\n";
                return kExitUsage;
            }
        } else if (argument == "--service-uid") {
            const char* value = next(argument);
            if (value == nullptr || !ParseUnsigned(value, &options.service_uid)) {
                std::cerr << "mino-deploy: invalid --service-uid\n";
                return kExitUsage;
            }
        } else if (argument == "--service-gid") {
            const char* value = next(argument);
            if (value == nullptr || !ParseUnsigned(value, &options.service_gid)) {
                std::cerr << "mino-deploy: invalid --service-gid\n";
                return kExitUsage;
            }
        } else if (argument == "--namespace") {
            const char* value = next(argument);
            if (value == nullptr) return kExitUsage;
            options.namespace_name = value;
        } else if (argument == "--output") {
            const char* value = next(argument);
            if (value == nullptr) return kExitUsage;
            options.output_path = value;
        } else {
            std::cerr << "mino-deploy: unknown generate option " << argument
                      << "\n";
            return kExitUsage;
        }
    }
    if (options.environment.empty() || options.role.empty() ||
        options.node_id == 0 || options.security_domain_id == 0 ||
        options.region_id == 0 || options.service_uid == 0 ||
        options.service_gid == 0 || options.namespace_name.empty()) {
        std::cerr << "mino-deploy: generate requires environment, role, and all "
                     "node/domain/UID/GID/namespace identities\n";
        return kExitUsage;
    }
    auto rendered = RenderTemplate(options);
    if (!rendered.ok()) {
        std::cerr << "mino-deploy: " << rendered.status().ToString() << "\n";
        return kExitConfig;
    }
    if (options.output_path.empty()) {
        std::cout << *rendered;
        return kExitSuccess;
    }
    const Status written = WriteNewFile(options.output_path, *rendered);
    if (!written.ok()) {
        std::cerr << "mino-deploy: " << written.ToString() << "\n";
        return kExitConfig;
    }
    return kExitSuccess;
}

struct CommandOptions {
    std::string config_path;
    std::string probe_kind;
    bool dry_run = false;
    std::vector<std::string> child;
};

mino::Result<CommandOptions> ParseCommandOptions(int argc, char** argv,
                                                 bool allow_child,
                                                 bool allow_probe_kind) {
    CommandOptions options;
    for (int index = 0; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (allow_child && argument == "--") {
            for (++index; index < argc; ++index) options.child.emplace_back(argv[index]);
            break;
        }
        if (argument == "--config") {
            if (++index >= argc) return Invalid("--config requires a value");
            options.config_path = argv[index];
        } else if (allow_child && argument == "--dry-run") {
            options.dry_run = true;
        } else if (allow_probe_kind && argument == "--kind") {
            if (++index >= argc) return Invalid("--kind requires a value");
            options.probe_kind = argv[index];
        } else {
            return Invalid("unknown option '" + std::string(argument) + "'");
        }
    }
    if (options.config_path.empty()) return Invalid("--config is required");
    if (allow_child && options.child.empty()) {
        return Invalid("start requires '-- <absolute-node-binary> [args...]'");
    }
    if (allow_probe_kind && options.probe_kind != "health" &&
        options.probe_kind != "readiness") {
        return Invalid("--kind must be health or readiness");
    }
    return options;
}

int CmdValidateOrPreflight(int argc, char** argv, bool run_preflight) {
    auto options = ParseCommandOptions(argc, argv, false, false);
    if (!options.ok()) {
        std::cerr << "mino-deploy: " << options.status().ToString() << "\n";
        return kExitUsage;
    }
    auto config = LoadChecked(options->config_path);
    if (!config.ok()) {
        std::cerr << "mino-deploy: " << config.status().ToString() << "\n";
        return kExitConfig;
    }
    if (run_preflight) {
        const Status status = Preflight(*config);
        if (!status.ok()) {
            std::cerr << "mino-deploy: " << status.ToString() << "\n";
            return kExitPreflight;
        }
        std::cout << "preflight ok: " << config->node.name << "\n";
    } else {
        std::cout << "configuration valid: " << config->node.name << "\n";
    }
    return kExitSuccess;
}

int RunChild(const NodeDeploymentConfig& config,
             const std::vector<std::string>& command) {
    struct sigaction action {};
    action.sa_handler = ForwardSignal;
    ::sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    if (::sigaction(SIGTERM, &action, nullptr) != 0 ||
        ::sigaction(SIGINT, &action, nullptr) != 0) {
        std::cerr << "mino-deploy: cannot install signal handlers\n";
        return kExitLaunch;
    }

    const pid_t child = ::fork();
    if (child < 0) {
        std::cerr << "mino-deploy: cannot fork node process: "
                  << std::strerror(errno) << "\n";
        return kExitLaunch;
    }
    if (child == 0) {
        struct sigaction reset {};
        reset.sa_handler = SIG_DFL;
        ::sigemptyset(&reset.sa_mask);
        (void)::sigaction(SIGTERM, &reset, nullptr);
        (void)::sigaction(SIGINT, &reset, nullptr);
        std::vector<char*> arguments;
        arguments.reserve(command.size() + 1);
        for (const std::string& value : command) {
            arguments.push_back(const_cast<char*>(value.c_str()));
        }
        arguments.push_back(nullptr);
        ::execv(arguments.front(), arguments.data());
        std::cerr << "mino-deploy: cannot exec node process: "
                  << std::strerror(errno) << "\n";
        _exit(127);
    }

    g_child_pid = static_cast<sig_atomic_t>(child);
    if (g_requested_signal != 0) {
        (void)::kill(child, g_requested_signal);
    }
    bool deadline_set = false;
    std::chrono::steady_clock::time_point deadline;
    int status = 0;
    for (;;) {
        const pid_t waited = ::waitpid(child, &status, WNOHANG);
        if (waited == child) break;
        if (waited < 0 && errno != EINTR) {
            (void)::kill(child, SIGKILL);
            (void)::waitpid(child, nullptr, 0);
            g_child_pid = -1;
            std::cerr << "mino-deploy: waitpid failed: " << std::strerror(errno)
                      << "\n";
            return kExitLaunch;
        }
        if (g_requested_signal != 0 && !deadline_set) {
            deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(config.node.shutdown_grace_ms);
            deadline_set = true;
        }
        if (deadline_set && std::chrono::steady_clock::now() >= deadline) {
            (void)::kill(child, SIGKILL);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    g_child_pid = -1;
    if (g_requested_signal != 0) return 128 + g_requested_signal;
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
    return kExitLaunch;
}

int CmdStart(int argc, char** argv) {
    auto options = ParseCommandOptions(argc, argv, true, false);
    if (!options.ok()) {
        std::cerr << "mino-deploy: " << options.status().ToString() << "\n";
        return kExitUsage;
    }
    auto config = LoadChecked(options->config_path);
    if (!config.ok()) {
        std::cerr << "mino-deploy: " << config.status().ToString() << "\n";
        return kExitConfig;
    }
    const Status preflight = Preflight(*config);
    if (!preflight.ok()) {
        std::cerr << "mino-deploy: " << preflight.ToString() << "\n";
        return kExitPreflight;
    }
    const Status executable = CheckExecutable(options->child.front());
    if (!executable.ok()) {
        std::cerr << "mino-deploy: " << executable.ToString() << "\n";
        return kExitPreflight;
    }
    if (options->dry_run) {
        std::cout << "dry-run ok: " << config->node.name << " -> "
                  << options->child.front() << "\n";
        return kExitSuccess;
    }
    return RunChild(*config, options->child);
}

int CmdProbe(int argc, char** argv) {
    auto options = ParseCommandOptions(argc, argv, false, true);
    if (!options.ok()) {
        std::cerr << "mino-deploy: " << options.status().ToString() << "\n";
        return kExitUsage;
    }
    auto config = LoadChecked(options->config_path);
    if (!config.ok()) {
        std::cerr << "mino-deploy: " << config.status().ToString() << "\n";
        return kExitConfig;
    }
    if (options->probe_kind == "readiness") {
        const Status preflight = Preflight(*config);
        if (!preflight.ok()) {
            std::cerr << "mino-deploy: " << preflight.ToString() << "\n";
            return kExitProbe;
        }
    }
    const Status probed = ProbeMonitoring(*config);
    if (!probed.ok()) {
        std::cerr << "mino-deploy: " << probed.ToString() << "\n";
        return kExitProbe;
    }
    std::cout << options->probe_kind << " ok\n";
    return kExitSuccess;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        PrintUsage(std::cerr);
        return kExitUsage;
    }
    const std::string_view command(argv[1]);
    if (command == "--help" || command == "-h" || command == "help") {
        PrintUsage(std::cout);
        return kExitSuccess;
    }
    if (command == "generate") return CmdGenerate(argc - 2, argv + 2);
    if (command == "validate") {
        return CmdValidateOrPreflight(argc - 2, argv + 2, false);
    }
    if (command == "preflight") {
        return CmdValidateOrPreflight(argc - 2, argv + 2, true);
    }
    if (command == "start") return CmdStart(argc - 2, argv + 2);
    if (command == "probe") return CmdProbe(argc - 2, argv + 2);
    std::cerr << "mino-deploy: unknown command '" << command << "'\n";
    PrintUsage(std::cerr);
    return kExitUsage;
}
