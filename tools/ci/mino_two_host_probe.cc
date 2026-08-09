// Copyright 2026 The Mino Authors
//
// Physical two-host CI probe. The application payload and its remote ACK travel
// exclusively through TcpDriver, BridgeConnectionManager, SessionDiscovery,
// and BridgePipeline.

#include <netdb.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "mino/bridge/bridge_runtime/connection_manager.h"
#include "mino/bridge/schema_negotiator.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/schema/registry.h"
#include "mino/storage/schema_store.h"
#include "mino/transport/tcp_driver.h"
#include "tools/ci/two_host_connection_group.h"

namespace {

using mino::BridgeDispatchRequest;
using mino::LocalPublication;
using mino::NodeId;
using mino::ProcessIdentity;
using mino::Result;
using mino::Status;
using mino::StatusCode;
using mino::TopicId;
using mino::bridge::BridgeConnectionManager;
using mino::bridge::BridgeConnectionManagerOptions;
using mino::bridge::BridgeConnectionMode;
using mino::bridge::BridgeConnectionState;
using mino::bridge::BridgeDescriptorProvider;
using mino::bridge::BridgeIngressPort;
using mino::bridge::BridgeNodeIdentityFence;
using mino::bridge::BridgePumpBudget;
using mino::bridge::BridgeRouteContract;
using mino::bridge::BridgeRuntimeDispatcher;
using mino::bridge::DescriptorAuth;
using mino::bridge::DescriptorPersistence;
using mino::bridge::SchemaNegotiator;
using mino::bridge::WireFrame;
using mino::registry::Reliability;
using mino::transport::EndpointDescriptor;
using mino::transport::TcpDriver;
using mino::transport::TcpDriverOptions;
using mino::tools::ci::TwoHostConnectionGroup;

constexpr std::string_view kProtocol = "mino-two-host-mino-v2";
constexpr std::string_view kEnvelopeMagic = "MINO_TWO_HOST_MINO_V2";
constexpr size_t kCommitLength = 40;
constexpr size_t kDigestLength = 64;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr size_t kMaxProbeFrameBytes = 256u * 1024u;
constexpr uint64_t kInitialSequence = 1;
constexpr uint64_t kReconnectSequence = 2;
constexpr uint32_t kLegacyTopicId = 7001;
constexpr uint32_t kApplicationBarrierTopicId = 8001;
constexpr uint32_t kApplicationReleaseTopicId = 8002;
constexpr uint32_t kApplicationConfirmTopicId = 8003;
constexpr size_t kApplicationPayloadBytes = 256;
constexpr size_t kApplicationBarrierPayloadBytes = 128;
constexpr uint32_t kCorrectnessMessagesPerTopic = 64;
constexpr uint32_t kFixedTotalMessages = 384;
constexpr uint32_t kFixedPerTopicMessages = 32;
constexpr size_t kDispatchBatch = 8;
constexpr size_t kLatencyDispatchBatch = 32;
constexpr uint32_t kLatencyMessagesPerTopic = 64;
constexpr size_t kLatencyPayloadBytes = 256;
constexpr uint32_t kLatencyRequestKind = 1;
constexpr uint32_t kLatencyResponseKind = 2;
constexpr std::array<uint32_t, 6> kScalingTopicCounts = {1, 2, 4, 8,
                                                        16, 32};
constexpr std::array<std::byte, 8> kApplicationMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'A'}, std::byte{'P'}, std::byte{'P'}, std::byte{'5'},
};
constexpr std::array<std::byte, 8> kApplicationBarrierMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'B'}, std::byte{'A'}, std::byte{'R'}, std::byte{'1'},
};
constexpr std::array<std::byte, 8> kLatencyMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'R'}, std::byte{'T'}, std::byte{'T'}, std::byte{'1'},
};

std::atomic<bool> g_stop_requested{false};

void HandleSignal(int) { g_stop_requested.store(true, std::memory_order_relaxed); }

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status TimedOut(std::string_view message) {
    return Status::Error(StatusCode::kTimeout, message);
}

uint64_t NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

bool IsHex(std::string_view value, size_t expected_size) {
    return value.size() == expected_size &&
           std::all_of(value.begin(), value.end(), [](char character) {
               return (character >= '0' && character <= '9') ||
                      (character >= 'a' && character <= 'f');
           });
}

std::string JsonEscape(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char character : value) {
        switch (character) {
            case '\\':
                escaped += "\\\\";
                break;
            case '"':
                escaped += "\\\"";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(character) < 0x20u) {
                    escaped += "?";
                } else {
                    escaped += character;
                }
        }
    }
    return escaped;
}

struct Options {
    std::string role;
    std::string address;
    std::string advertise_address;
    uint16_t port = 0;
    uint32_t deadline_seconds = 0;
    uint16_t tcp_lane_count = 1;
    std::string commit;
    std::string machine_identity;
    std::string run_proof;
    std::filesystem::path output;
};

struct Envelope {
    std::string role;
    std::string commit;
    std::string machine_identity;
    std::string run_proof;
    std::string advertised_address;
};

struct TopicRouteRecord {
    uint64_t topic_id = 0;
    std::string direction;
    uint64_t messages = 0;
    bool ordered = false;
    bool payload_verified = false;
    bool cross_topic_leakage = true;
};

struct NetworkScalingRecord {
    std::string mode;
    uint32_t topic_count = 0;
    uint32_t messages_per_topic = 0;
    uint64_t messages_sent = 0;
    uint64_t messages_received = 0;
    uint64_t application_payload_bytes = 0;
    uint64_t mino_frame_body_bytes = 0;
    uint64_t pipeline_inbound_frames = 0;
    uint64_t pipeline_outbound_frames = 0;
    uint64_t tcp_prefix_bytes = 0;
    uint64_t tcp_framed_bytes = 0;
    uint64_t elapsed_ms = 0;
    uint64_t accepted_acks = 0;
    uint64_t retransmissions = 0;
    uint64_t duplicate_suppressed = 0;
    bool cross_topic_leakage = true;
};

struct LatencySampleRecord {
    uint32_t topic_count = 0;
    uint32_t messages_per_topic = 0;
    uint64_t sample_count = 0;
    uint64_t single_message_rtt_us = 0;
    uint64_t p50_rtt_us = 0;
    uint64_t p95_rtt_us = 0;
    uint64_t p99_rtt_us = 0;
    uint64_t max_rtt_us = 0;
};

struct LaneConnectionRecord {
    uint16_t lane_index = 0;
    bool active = false;
    uint64_t local_session_epoch = 0;
    uint64_t remote_session_epoch = 0;
    uint64_t connection_attempts = 0;
    uint64_t accepted_connections = 0;
    uint64_t completed_handshakes = 0;
    uint64_t reconnects = 0;
    uint64_t disconnects = 0;
};

struct ProbeResult {
    std::string role;
    std::string outcome = "failed";
    std::string commit;
    std::string machine_identity;
    std::string peer_commit;
    std::string peer_machine_identity;
    std::string local_address;
    std::string peer_address;
    std::string local_schema_digest;
    std::string peer_schema_digest;
    std::string persisted_schema_digest;
    std::string error;
    bool session_discovery = false;
    bool bridge_active = false;
    bool reliable_sent = false;
    bool reliable_received = false;
    bool remote_acknowledged = false;
    bool schema_identity_nonempty = false;
    bool descriptor_artifact_nonempty = false;
    bool schema_announcement = false;
    bool schema_request = false;
    bool schema_persisted = false;
    bool persisted_schema_identity_verified = false;
    bool persisted_schema_bytes_verified = false;
    bool forced_disconnect = false;
    bool automatic_reconnect = false;
    bool session_epoch_changed = false;
    bool pending_reliable_before_disconnect = false;
    bool pending_reliable_recovered = false;
    bool pending_reliable_retransmitted = false;
    bool reliable_replay_sent = false;
    bool reliable_replay_pending_observed = false;
    bool dedup_state_preserved = false;
    bool duplicate_suppressed = false;
    bool bidirectional_ack = false;
    uint64_t initial_local_session_epoch = 0;
    uint64_t initial_remote_session_epoch = 0;
    uint64_t local_session_epoch = 0;
    uint64_t remote_session_epoch = 0;
    uint64_t connection_attempts = 0;
    uint64_t accepted_connections = 0;
    uint64_t completed_handshakes = 0;
    uint64_t reconnects = 0;
    uint64_t disconnects = 0;
    uint64_t accepted_acks = 0;
    uint64_t duplicate_checks = 0;
    uint64_t descriptor_authentications = 0;
    uint64_t descriptor_persistences = 0;
    uint64_t descriptor_artifact_bytes = 0;
    uint16_t tcp_lane_count = 1;
    uint16_t active_lane_connections = 0;
    uint16_t exercised_lane_count = 0;
    uint64_t elapsed_ms = 0;
    std::vector<LaneConnectionRecord> lane_connections;
    std::vector<TopicRouteRecord> topic_routes;
    std::vector<NetworkScalingRecord> network_scaling;
    std::vector<LatencySampleRecord> latency_samples;
};

std::optional<std::string_view> FlagValueOf(int argc, char** argv,
                                            std::string_view name) {
    const std::string prefix = "--" + std::string(name) + "=";
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument.starts_with(prefix)) return argument.substr(prefix.size());
    }
    return std::nullopt;
}

template <typename Integer>
Result<Integer> ParseInteger(std::string_view raw, std::string_view name) {
    Integer value = 0;
    const auto parsed =
        std::from_chars(raw.data(), raw.data() + raw.size(), value, 10);
    if (parsed.ec != std::errc{} || parsed.ptr != raw.data() + raw.size()) {
        return Invalid(std::string(name) + " must be a decimal integer");
    }
    return value;
}

Result<Options> ParseOptions(int argc, char** argv) {
    Options options;
    const auto role = FlagValueOf(argc, argv, "role");
    const auto address = FlagValueOf(argc, argv, "address");
    const auto advertise = FlagValueOf(argc, argv, "advertise-address");
    const auto port = FlagValueOf(argc, argv, "port");
    const auto deadline = FlagValueOf(argc, argv, "deadline-seconds");
    const auto tcp_lanes = FlagValueOf(argc, argv, "tcp-lane-count");
    const auto commit = FlagValueOf(argc, argv, "commit");
    const auto identity = FlagValueOf(argc, argv, "machine-identity");
    const auto proof = FlagValueOf(argc, argv, "run-proof");
    const auto output = FlagValueOf(argc, argv, "output");
    if (!role || !address || !advertise || !port || !deadline || !commit ||
        !identity || !proof || !output) {
        return Invalid("required flags: role, address, advertise-address, port, "
                       "deadline-seconds, commit, machine-identity, run-proof, output");
    }
    options.role = *role;
    options.address = *address;
    options.advertise_address = *advertise;
    options.commit = *commit;
    options.machine_identity = *identity;
    options.run_proof = *proof;
    options.output = *output;
    MINO_ASSIGN_OR_RETURN(options.port, ParseInteger<uint16_t>(*port, "port"));
    MINO_ASSIGN_OR_RETURN(options.deadline_seconds,
                          ParseInteger<uint32_t>(*deadline, "deadline-seconds"));
    if (tcp_lanes.has_value()) {
        MINO_ASSIGN_OR_RETURN(
            options.tcp_lane_count,
            ParseInteger<uint16_t>(*tcp_lanes, "tcp-lane-count"));
    }
    if (options.role != "server" && options.role != "client") {
        return Invalid("role must be server or client");
    }
    if (options.port < 1024 || options.deadline_seconds == 0 ||
        options.deadline_seconds > 3600 || options.tcp_lane_count == 0 ||
        options.tcp_lane_count > mino::bridge::kMaxBridgeLaneCount) {
        return Invalid(
            "port, deadline, or TCP lane count is outside the supported range");
    }
    if (!IsHex(options.commit, kCommitLength) ||
        !IsHex(options.machine_identity, kDigestLength) ||
        !IsHex(options.run_proof, kDigestLength)) {
        return Invalid("commit, machine identity, or run proof is not lowercase hex");
    }
    if (options.advertise_address.empty() ||
        options.advertise_address.find('|') != std::string::npos ||
        options.advertise_address.size() > 253) {
        return Invalid("advertise address is invalid");
    }
    return options;
}

Result<EndpointDescriptor> ResolveEndpoint(std::string_view host,
                                           uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    addrinfo* addresses = nullptr;
    const std::string host_string(host);
    const int resolved =
        ::getaddrinfo(host_string.c_str(), nullptr, &hints, &addresses);
    if (resolved != 0) {
        return Invalid(std::string("cannot resolve endpoint: ") +
                       ::gai_strerror(resolved));
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> owned(addresses,
                                                               ::freeaddrinfo);
    for (const addrinfo* current = addresses; current != nullptr;
         current = current->ai_next) {
        if (current->ai_family == AF_INET) {
            const auto* address =
                reinterpret_cast<const sockaddr_in*>(current->ai_addr);
            const auto* bytes = reinterpret_cast<const std::byte*>(
                &address->sin_addr.s_addr);
            return EndpointDescriptor::Ipv4Tcp(
                std::span<const std::byte>(bytes, 4), port);
        }
        if (current->ai_family == AF_INET6) {
            const auto* address =
                reinterpret_cast<const sockaddr_in6*>(current->ai_addr);
            const auto* bytes = reinterpret_cast<const std::byte*>(
                address->sin6_addr.s6_addr);
            return EndpointDescriptor::Ipv6Tcp(
                std::span<const std::byte>(bytes, 16), port);
        }
    }
    return Invalid("endpoint has no IPv4 or IPv6 address");
}

ProcessIdentity Identity(NodeId node, uint64_t incarnation) {
    return ProcessIdentity{
        .node_id = node.value,
        .process_id = 10'000 + incarnation,
        .process_epoch = 20'000 + incarnation,
        .start_time_ns = 30'000 + incarnation,
    };
}

BridgeNodeIdentityFence Fence(NodeId node, uint64_t incarnation) {
    return BridgeNodeIdentityFence{
        .node_id = node,
        .process_identity = Identity(node, incarnation),
        .lease_epoch = 40'000 + incarnation,
        .node_config_version = 50'000 + incarnation,
    };
}

BridgeNodeIdentityFence LocalFence(std::string_view role) {
    return role == "client" ? Fence(NodeId{101}, 1) : Fence(NodeId{202}, 2);
}

BridgeNodeIdentityFence PeerFence(std::string_view role) {
    return role == "client" ? Fence(NodeId{202}, 2) : Fence(NodeId{101}, 1);
}

TcpDriverOptions DriverOptions() {
    TcpDriverOptions options;
    options.max_frame_body_bytes = kMaxProbeFrameBytes;
    options.max_total_send_buffer_bytes = 2 * kMaxProbeFrameBytes;
    options.max_connection_send_buffer_bytes =
        kMaxProbeFrameBytes + sizeof(uint32_t);
    options.max_control_send_buffer_bytes =
        kMaxProbeFrameBytes + sizeof(uint32_t);
    options.max_ready_receive_bytes = 2 * kMaxProbeFrameBytes;
    options.max_ready_receive_messages = 128;
    options.max_pending_accepts = 8;
    options.heartbeat_interval_ms = 1000;
    options.idle_timeout_ms = 10'000;
    options.partial_frame_timeout_ms = 5000;
    options.io_poll_max_ms = 10;
    return options;
}

BridgeConnectionManagerOptions ManagerOptions(
    const Options& options, const EndpointDescriptor& endpoint) {
    BridgeConnectionManagerOptions manager;
    manager.mode = options.role == "server" ? BridgeConnectionMode::kListen
                                             : BridgeConnectionMode::kConnect;
    if (manager.mode == BridgeConnectionMode::kListen) {
        manager.local_endpoint = endpoint;
        manager.peer_route_endpoint = endpoint;
    } else {
        manager.remote_endpoint = endpoint;
    }
    manager.local_identity = LocalFence(options.role);
    manager.expected_peer = PeerFence(options.role);
    manager.route_driver_id = 71;
    manager.route_driver_generation = 3;
    manager.manage_driver_lifecycle = true;
    manager.driver_config = mino::transport::DriverConfig{
        .max_connections = 8,
        .max_listeners = 2,
        .max_queued_sends = 128,
    };
    manager.listen_backlog = 4;
    manager.connect_timeout_ms = 500;
    manager.handshake_timeout_ns = 10 * kNanosecondsPerSecond;
    manager.initial_reconnect_backoff_ns = 100'000'000ull;
    manager.max_reconnect_backoff_ns = 5 * kNanosecondsPerSecond;
    manager.health_probe_interval_ns = kNanosecondsPerSecond;
    manager.max_egress_frames = 32;
    manager.max_egress_bytes = kMaxProbeFrameBytes;
    manager.pipeline.max_control_bytes = kMaxProbeFrameBytes;
    manager.pipeline.max_pending_inbound_bytes = 2 * kMaxProbeFrameBytes;
    manager.pipeline.wire_limits.max_payload_length = kMaxProbeFrameBytes;
    manager.pipeline.wire_limits.max_buffered_bytes = kMaxProbeFrameBytes;
    manager.pipeline.retransmit.max_age_ns = 60 * kNanosecondsPerSecond;
    manager.pipeline.retransmit.max_entries = 32;
    manager.pipeline.retransmit.max_bytes = kMaxProbeFrameBytes;
    return manager;
}

std::string FormatEndpoint(std::string_view host, uint16_t port) {
    if (host.find(':') != std::string_view::npos) {
        return "[" + std::string(host) + "]:" + std::to_string(port);
    }
    return std::string(host) + ":" + std::to_string(port);
}

std::string EncodeEnvelope(const Options& options) {
    return std::string(kEnvelopeMagic) + "|" + options.role + "|" +
           options.commit + "|" + options.machine_identity + "|" +
           options.run_proof + "|" +
           FormatEndpoint(options.advertise_address, options.port);
}

Result<Envelope> DecodeEnvelope(std::span<const std::byte> payload) {
    const std::string text(reinterpret_cast<const char*>(payload.data()),
                           payload.size());
    std::array<std::string_view, 6> fields{};
    size_t begin = 0;
    for (size_t index = 0; index < fields.size(); ++index) {
        const size_t delimiter = text.find('|', begin);
        if (index + 1 == fields.size()) {
            if (delimiter != std::string::npos) {
                return Corruption("Mino probe envelope has extra fields");
            }
            fields[index] = std::string_view(text).substr(begin);
        } else {
            if (delimiter == std::string::npos) {
                return Corruption("Mino probe envelope is truncated");
            }
            fields[index] =
                std::string_view(text).substr(begin, delimiter - begin);
            begin = delimiter + 1;
        }
    }
    if (fields[0] != kEnvelopeMagic ||
        (fields[1] != "server" && fields[1] != "client") ||
        !IsHex(fields[2], kCommitLength) ||
        !IsHex(fields[3], kDigestLength) ||
        !IsHex(fields[4], kDigestLength) || fields[5].empty()) {
        return Corruption("Mino probe envelope fields are invalid");
    }
    return Envelope{
        .role = std::string(fields[1]),
        .commit = std::string(fields[2]),
        .machine_identity = std::string(fields[3]),
        .run_proof = std::string(fields[4]),
        .advertised_address = std::string(fields[5]),
    };
}

struct ProbeSchema {
    mino::schema::SchemaHandle handle;
    std::vector<std::byte> artifact;
};

std::span<const std::byte> Bytes(std::string_view value) {
    return std::as_bytes(std::span<const char>(value.data(), value.size()));
}

std::string DigestHex(const mino::schema::CanonicalDigest& digest) {
    constexpr char kHex[] = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(digest.size() * 2);
    for (std::byte byte : digest) {
        const uint8_t value = static_cast<uint8_t>(byte);
        encoded.push_back(kHex[value >> 4]);
        encoded.push_back(kHex[value & 0x0fu]);
    }
    return encoded;
}

bool CompleteSchemaIdentity(const mino::schema::SchemaIdentity& identity) {
    const bool digest_nonzero = std::any_of(
        identity.canonical_digest().begin(), identity.canonical_digest().end(),
        [](std::byte byte) { return byte != std::byte{0}; });
    return identity.short_id() != 0 && digest_nonzero &&
           identity.schema_version() != 0 && identity.layout_version() != 0;
}

Result<ProbeSchema> BuildProbeSchema(std::string_view role) {
    const std::string idl =
        "option schema_version = \"1.0\"; package mino_two_host_" +
        std::string(role) +
        "; message ProbeEnvelope { string payload = 1 [max_bytes = 2048]; }";
    MINO_ASSIGN_OR_RETURN(auto compiled,
                          mino::schema::SchemaCompiler::Compile(idl));
    if (compiled.types().size() != 1) {
        return Corruption("two-host probe schema did not compile to one type");
    }
    MINO_ASSIGN_OR_RETURN(
        auto layout,
        mino::schema::LayoutPlanner::Plan(*compiled.types().front()));
    const std::array<mino::schema::LayoutPlan, 1> layouts = {
        std::move(layout)};
    MINO_ASSIGN_OR_RETURN(
        auto artifact,
        mino::schema::codegen::EncodeDescriptorArtifact(compiled, layouts));
    if (artifact.empty()) return Corruption("compiled descriptor artifact is empty");
    const auto bytes = Bytes(artifact);
    return ProbeSchema{
        .handle = compiled.types().front(),
        .artifact = std::vector<std::byte>(bytes.begin(), bytes.end()),
    };
}

bool SameIdentity(const mino::schema::SchemaIdentity& lhs,
                  const mino::schema::SchemaIdentity& rhs) {
    return lhs.short_id() == rhs.short_id() &&
           lhs.canonical_digest() == rhs.canonical_digest() &&
           lhs.schema_version() == rhs.schema_version() &&
           lhs.layout_version() == rhs.layout_version();
}

class ExactDescriptorAuth final : public DescriptorAuth {
public:
    explicit ExactDescriptorAuth(const ProbeSchema* expected)
        : expected_(expected) {}

    Status Authenticate(
        const mino::schema::SchemaIdentity& identity,
        std::span<const std::byte> descriptor_artifact) override {
        ++authentications;
        if (expected_ == nullptr || expected_->handle == nullptr ||
            !SameIdentity(identity, expected_->handle->identity()) ||
            !std::equal(descriptor_artifact.begin(), descriptor_artifact.end(),
                        expected_->artifact.begin(), expected_->artifact.end())) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "peer descriptor artifact authentication failed");
        }
        return Status::Ok();
    }

    uint64_t authentications = 0;

private:
    const ProbeSchema* expected_;
};

class StorePersistence final : public DescriptorPersistence {
public:
    explicit StorePersistence(mino::storage::SchemaStore* store)
        : store_(store) {}

    Status Persist(
        const mino::schema::SchemaIdentity& identity,
        std::span<const std::byte> descriptor_artifact) override {
        ++attempts;
        if (store_ == nullptr) {
            return Status::Error(StatusCode::kUnavailable,
                                 "probe schema store is unavailable");
        }
        auto persisted = store_->Persist(identity, descriptor_artifact);
        if (!persisted.ok()) return persisted.status();
        persisted_ref = *persisted;
        return Status::Ok();
    }

    uint64_t attempts = 0;
    mino::storage::SchemaRef persisted_ref = mino::storage::kInvalidSchemaRef;

private:
    mino::storage::SchemaStore* store_;
};

class ExactDescriptorProvider final : public BridgeDescriptorProvider {
public:
    explicit ExactDescriptorProvider(const ProbeSchema* schema)
        : schema_(schema) {}

    Result<std::vector<std::byte>> GetDescriptorArtifact(
        const mino::schema::SchemaIdentity& identity) override {
        if (schema_ == nullptr || schema_->handle == nullptr ||
            !SameIdentity(identity, schema_->handle->identity())) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "dispatcher requested an unknown descriptor");
        }
        return schema_->artifact;
    }

private:
    const ProbeSchema* schema_;
};

mino::bridge::SourceIdentity LegacySource(std::string_view role,
                                           uint16_t lane_count) {
    const uint64_t node_id = LocalFence(role).node_id.value;
    const uint64_t publisher_epoch = role == "client" ? 8101 : 8201;
    const uint64_t first_publisher_id = role == "client" ? 7101 : 7201;
    for (uint64_t publisher_id = first_publisher_id;
         publisher_id != first_publisher_id + 100'000; ++publisher_id) {
        const mino::bridge::SourceIdentity source{
            .node_id = node_id,
            .publisher_id = publisher_id,
            .publisher_epoch = publisher_epoch,
        };
        if (mino::bridge::BridgeLaneFor(source, lane_count) == 0) return source;
    }
    return {};
}

uint32_t RoleCode(std::string_view role) { return role == "client" ? 1u : 2u; }

struct TopicWorkload {
    uint32_t topic_id = 0;
    uint64_t publisher_id = 0;
    uint64_t publisher_epoch = 0;
    uint32_t messages = 0;
};

struct PhasePlan {
    uint32_t phase_id = 0;
    std::string mode;
    uint32_t topic_count = 0;
    uint32_t messages_per_topic = 0;
    std::vector<TopicWorkload> local_topics;
    std::vector<TopicWorkload> peer_topics;

    bool scaling() const noexcept { return !mode.empty(); }
    uint64_t message_count() const noexcept {
        return static_cast<uint64_t>(topic_count) * messages_per_topic;
    }
};

TopicWorkload Workload(std::string_view sender_role, uint32_t topic_id,
                       uint32_t messages) {
    const uint64_t role_base = sender_role == "client" ? 100'000 : 200'000;
    const uint64_t epoch_base = sender_role == "client" ? 300'000 : 400'000;
    return TopicWorkload{
        .topic_id = topic_id,
        .publisher_id = role_base + topic_id,
        .publisher_epoch = epoch_base + topic_id,
        .messages = messages,
    };
}

uint64_t LatencyPublisherId(std::string_view role, uint32_t topic_id) {
    return (role == "client" ? 1'000'000ull : 2'000'000ull) + topic_id;
}

uint64_t LatencyPublisherEpoch(std::string_view role, uint32_t topic_id) {
    return (role == "client" ? 3'000'000ull : 4'000'000ull) + topic_id;
}

struct LatencyTopicPlan {
    uint32_t topic_id = 0;
};

struct LatencyPhasePlan {
    uint32_t phase_id = 0;
    std::string initiator_role;
    uint32_t topic_count = 0;
    std::vector<LatencyTopicPlan> topics;

    uint64_t sample_count() const noexcept {
        return static_cast<uint64_t>(topic_count) *
               kLatencyMessagesPerTopic;
    }
};

std::vector<PhasePlan> BuildPhasePlans(std::string_view role) {
    const std::string_view peer_role = role == "client" ? "server" : "client";
    PhasePlan correctness;
    correctness.phase_id = 1;
    correctness.topic_count = 2;
    correctness.messages_per_topic = kCorrectnessMessagesPerTopic;
    const std::array<uint32_t, 2> server_topics = {1001, 1002};
    const std::array<uint32_t, 2> client_topics = {2001, 2002};
    const auto& local_correctness =
        role == "client" ? client_topics : server_topics;
    const auto& peer_correctness =
        role == "client" ? server_topics : client_topics;
    for (uint32_t topic_id : local_correctness) {
        correctness.local_topics.push_back(
            Workload(role, topic_id, kCorrectnessMessagesPerTopic));
    }
    for (uint32_t topic_id : peer_correctness) {
        correctness.peer_topics.push_back(
            Workload(peer_role, topic_id, kCorrectnessMessagesPerTopic));
    }

    std::vector<PhasePlan> phases;
    phases.reserve(13);
    phases.push_back(std::move(correctness));
    uint32_t sample_index = 0;
    for (std::string_view mode : {std::string_view("fixed_total"),
                                  std::string_view("fixed_per_topic")}) {
        for (uint32_t topic_count : kScalingTopicCounts) {
            const uint32_t messages_per_topic =
                mode == "fixed_total" ? kFixedTotalMessages / topic_count
                                      : kFixedPerTopicMessages;
            PhasePlan sample;
            sample.phase_id = 100 + sample_index;
            sample.mode = mode;
            sample.topic_count = topic_count;
            sample.messages_per_topic = messages_per_topic;
            sample.local_topics.reserve(topic_count);
            sample.peer_topics.reserve(topic_count);
            const uint32_t topic_base = 10'000 + sample_index * 100;
            for (uint32_t topic = 1; topic <= topic_count; ++topic) {
                const uint32_t topic_id = topic_base + topic;
                sample.local_topics.push_back(
                    Workload(role, topic_id, messages_per_topic));
                sample.peer_topics.push_back(
                    Workload(peer_role, topic_id, messages_per_topic));
            }
            phases.push_back(std::move(sample));
            ++sample_index;
        }
    }
    return phases;
}

std::vector<LatencyPhasePlan> BuildLatencyPhasePlans() {
    std::vector<LatencyPhasePlan> phases;
    phases.reserve(kScalingTopicCounts.size() * 2);
    for (size_t count_index = 0; count_index < kScalingTopicCounts.size();
         ++count_index) {
        const uint32_t topic_count = kScalingTopicCounts[count_index];
        for (std::string_view initiator_role :
             {std::string_view("client"), std::string_view("server")}) {
            LatencyPhasePlan phase;
            phase.phase_id = 1'000 + static_cast<uint32_t>(count_index) * 2 +
                             (initiator_role == "client" ? 0u : 1u);
            phase.initiator_role = initiator_role;
            phase.topic_count = topic_count;
            phase.topics.reserve(topic_count);
            const uint32_t topic_base =
                (initiator_role == "client" ? 50'000u : 60'000u) +
                static_cast<uint32_t>(count_index) * 100u;
            for (uint32_t topic = 1; topic <= topic_count; ++topic) {
                phase.topics.push_back(
                    LatencyTopicPlan{.topic_id = topic_base + topic});
            }
            phases.push_back(std::move(phase));
        }
    }
    return phases;
}

size_t PlannedLaneCoverage(
    const Options& options, const std::vector<PhasePlan>& application_phases,
    const std::vector<LatencyPhasePlan>& latency_phases) {
    std::array<bool, mino::bridge::kMaxBridgeLaneCount> covered{};
    const auto mark = [&covered, &options](
                          const mino::bridge::SourceIdentity& source) {
        covered[mino::bridge::BridgeLaneFor(source, options.tcp_lane_count)] =
            true;
    };
    mark(LegacySource(options.role, options.tcp_lane_count));
    for (const PhasePlan& phase : application_phases) {
        for (const TopicWorkload& topic : phase.local_topics) {
            mark(mino::bridge::SourceIdentity{
                .node_id = LocalFence(options.role).node_id.value,
                .publisher_id = topic.publisher_id,
                .publisher_epoch = topic.publisher_epoch,
            });
        }
    }
    for (const LatencyPhasePlan& phase : latency_phases) {
        for (const LatencyTopicPlan& topic : phase.topics) {
            mark(mino::bridge::SourceIdentity{
                .node_id = LocalFence(options.role).node_id.value,
                .publisher_id =
                    LatencyPublisherId(phase.initiator_role, topic.topic_id),
                .publisher_epoch =
                    LatencyPublisherEpoch(phase.initiator_role, topic.topic_id),
            });
        }
    }
    return static_cast<size_t>(std::count(
        covered.begin(),
        covered.begin() + static_cast<std::ptrdiff_t>(options.tcp_lane_count),
        true));
}

void PutU32(std::vector<std::byte>* payload, size_t offset, uint32_t value) {
    for (size_t index = 0; index < 4; ++index) {
        (*payload)[offset + index] = static_cast<std::byte>(
            (value >> (8 * (3 - index))) & uint32_t{0xff});
    }
}

void PutU64(std::vector<std::byte>* payload, size_t offset, uint64_t value) {
    for (size_t index = 0; index < 8; ++index) {
        (*payload)[offset + index] = static_cast<std::byte>(
            (value >> (8 * (7 - index))) & uint64_t{0xff});
    }
}

uint32_t GetU32(std::span<const std::byte> payload, size_t offset) {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
        value = (value << 8) |
                static_cast<uint32_t>(payload[offset + index]);
    }
    return value;
}

uint64_t GetU64(std::span<const std::byte> payload, size_t offset) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value = (value << 8) |
                static_cast<uint64_t>(payload[offset + index]);
    }
    return value;
}

std::vector<std::byte> ApplicationPayload(uint32_t phase_id,
                                          const TopicWorkload& topic,
                                          uint64_t sequence,
                                          std::string_view sender_role) {
    std::vector<std::byte> payload(kApplicationPayloadBytes);
    std::copy(kApplicationMagic.begin(), kApplicationMagic.end(),
              payload.begin());
    PutU32(&payload, 8, phase_id);
    PutU32(&payload, 12, RoleCode(sender_role));
    PutU64(&payload, 16, topic.topic_id);
    PutU64(&payload, 24, sequence);
    PutU64(&payload, 32, LocalFence(sender_role).node_id.value);
    PutU64(&payload, 40, topic.publisher_id);
    PutU64(&payload, 48, topic.publisher_epoch);
    PutU32(&payload, 56, static_cast<uint32_t>(payload.size()));
    PutU32(&payload, 60, 0x4d350000u | RoleCode(sender_role));
    const uint64_t seed = static_cast<uint64_t>(phase_id) * 17 +
                          topic.topic_id * 13 + sequence * 7 +
                          LocalFence(sender_role).node_id.value * 5 +
                          topic.publisher_id * 3 + topic.publisher_epoch;
    for (size_t offset = 64; offset < payload.size(); ++offset) {
        payload[offset] =
            static_cast<std::byte>((seed + offset * 29) & uint64_t{0xff});
    }
    return payload;
}

mino::bridge::SourceIdentity ApplicationBarrierSource(
    std::string_view role) {
    return mino::bridge::SourceIdentity{
        .node_id = LocalFence(role).node_id.value,
        .publisher_id = role == "client" ? 8'100'001ull : 8'200'001ull,
        .publisher_epoch = role == "client" ? 8'300'001ull : 8'400'001ull,
    };
}

mino::bridge::SourceIdentity ApplicationReleaseSource() {
    return mino::bridge::SourceIdentity{
        .node_id = LocalFence("server").node_id.value,
        .publisher_id = 8'200'002ull,
        .publisher_epoch = 8'400'002ull,
    };
}

mino::bridge::SourceIdentity ApplicationConfirmSource() {
    return mino::bridge::SourceIdentity{
        .node_id = LocalFence("client").node_id.value,
        .publisher_id = 8'100'003ull,
        .publisher_epoch = 8'300'003ull,
    };
}

std::vector<std::byte> ApplicationBarrierPayloadFor(
    uint32_t topic_id, const mino::bridge::SourceIdentity& source,
    uint32_t phase_id, uint64_t sequence, std::string_view sender_role,
    uint32_t marker) {
    std::vector<std::byte> payload(kApplicationBarrierPayloadBytes);
    std::copy(kApplicationBarrierMagic.begin(),
              kApplicationBarrierMagic.end(), payload.begin());
    PutU32(&payload, 8, phase_id);
    PutU32(&payload, 12, RoleCode(sender_role));
    PutU64(&payload, 16, topic_id);
    PutU64(&payload, 24, sequence);
    PutU64(&payload, 32, source.node_id);
    PutU64(&payload, 40, source.publisher_id);
    PutU64(&payload, 48, source.publisher_epoch);
    PutU32(&payload, 56, static_cast<uint32_t>(payload.size()));
    PutU32(&payload, 60, marker | RoleCode(sender_role));
    const uint64_t seed = static_cast<uint64_t>(phase_id) * 43 +
                          sequence * 31 + topic_id * 29 +
                          source.node_id * 23 + source.publisher_id * 19 +
                          source.publisher_epoch * 17 + marker;
    for (size_t offset = 64; offset < payload.size(); ++offset) {
        payload[offset] =
            static_cast<std::byte>((seed + offset * 41) & uint64_t{0xff});
    }
    return payload;
}

std::vector<std::byte> ApplicationBarrierPayload(
    uint32_t phase_id, uint64_t sequence, std::string_view sender_role) {
    const mino::bridge::SourceIdentity source =
        ApplicationBarrierSource(sender_role);
    return ApplicationBarrierPayloadFor(
        kApplicationBarrierTopicId, source, phase_id, sequence, sender_role,
        0x42415200u);
}

std::vector<std::byte> ApplicationReleasePayload(uint32_t phase_id,
                                                 uint64_t sequence) {
    return ApplicationBarrierPayloadFor(
        kApplicationReleaseTopicId, ApplicationReleaseSource(), phase_id,
        sequence, "server", 0x52454c00u);
}

std::vector<std::byte> ApplicationConfirmPayload(uint32_t phase_id,
                                                 uint64_t sequence) {
    return ApplicationBarrierPayloadFor(
        kApplicationConfirmTopicId, ApplicationConfirmSource(), phase_id,
        sequence, "client", 0x434f4e00u);
}


std::vector<std::byte> LatencyPayload(uint32_t phase_id, uint32_t kind,
                                     uint32_t topic_id, uint64_t sequence,
                                     uint64_t origin_sent_ns,
                                     std::string_view initiator_role) {
    std::vector<std::byte> payload(kLatencyPayloadBytes);
    std::copy(kLatencyMagic.begin(), kLatencyMagic.end(), payload.begin());
    PutU32(&payload, 8, phase_id);
    PutU32(&payload, 12, kind);
    PutU64(&payload, 16, topic_id);
    PutU64(&payload, 24, sequence);
    PutU64(&payload, 32, origin_sent_ns);
    PutU32(&payload, 40, RoleCode(initiator_role));
    PutU32(&payload, 44, static_cast<uint32_t>(payload.size()));
    PutU64(&payload, 48, LocalFence(initiator_role).node_id.value);
    PutU64(&payload, 56, LatencyPublisherId(initiator_role, topic_id));
    PutU64(&payload, 64, LatencyPublisherEpoch(initiator_role, topic_id));
    PutU32(&payload, 72, 0x52545400u | kind);
    PutU32(&payload, 76, 0x4d4e0000u | RoleCode(initiator_role));
    const uint64_t seed = static_cast<uint64_t>(phase_id) * 31 +
                          static_cast<uint64_t>(kind) * 29 +
                          static_cast<uint64_t>(topic_id) * 23 + sequence * 19 +
                          LocalFence(initiator_role).node_id.value * 17 +
                          LatencyPublisherId(initiator_role, topic_id) * 13 +
                          LatencyPublisherEpoch(initiator_role, topic_id) * 11;
    for (size_t offset = 80; offset < payload.size(); ++offset) {
        payload[offset] =
            static_cast<std::byte>((seed + offset * 37) & uint64_t{0xff});
    }
    return payload;
}

bool FrameMatchesSchema(const WireFrame& frame,
                        const mino::schema::SchemaIdentity& identity) {
    return frame.header.msg_type ==
               static_cast<uint32_t>(identity.short_id() &
                                     uint64_t{0xffff'ffff}) &&
           frame.header.schema_version == identity.schema_version() &&
           frame.header.layout_version == identity.layout_version();
}

struct ApplicationIngressState {
    uint32_t phase_id = 0;
    TopicWorkload topic;
    uint64_t next_sequence = 1;
    uint64_t received = 0;
    bool ordered = true;
    bool payload_verified = true;
    bool cross_topic_leakage = false;
};

struct LatencyIngressState {
    uint32_t phase_id = 0;
    std::string initiator_role;
    uint32_t topic_id = 0;
    uint64_t next_sequence = 1;
    uint64_t received = 0;
    uint64_t requests_dispatched = 0;
    std::array<uint64_t, kLatencyMessagesPerTopic + 1> sent_origins{};
};

struct PendingLatencyEcho {
    uint32_t phase_id = 0;
    uint32_t topic_id = 0;
    uint64_t sequence = 0;
    uint64_t origin_sent_ns = 0;
    std::string initiator_role;
};

struct LatencyCompletion {
    uint32_t phase_id = 0;
    uint64_t rtt_ns = 0;
};

class ProbeIngress final : public BridgeIngressPort {
public:
    ProbeIngress(std::shared_ptr<TcpDriver> driver, std::string local_role,
                 std::string peer_role, const ProbeSchema* peer_schema,
                 mino::bridge::SourceIdentity peer_legacy_source)
        : driver_(std::move(driver)),
          local_role_(std::move(local_role)),
          peer_role_(std::move(peer_role)),
          peer_schema_(peer_schema),
          peer_legacy_source_(peer_legacy_source) {}

    void Attach(BridgeConnectionManager* manager) { manager_ = manager; }

    void ConfigureApplication(const std::vector<PhasePlan>& phases) {
        size_t topic_count = 0;
        for (const PhasePlan& phase : phases) {
            topic_count += phase.peer_topics.size();
        }
        application_.reserve(topic_count);
        application_barrier_phases_.reserve(phases.size() + 1);
        // Sequence one is a pre-workload readiness barrier. It prevents the
        // faster endpoint from dispatching correctness traffic while its peer
        // is still completing reconnect qualification.
        application_barrier_phases_.push_back(0);
        for (const PhasePlan& phase : phases) {
            application_barrier_phases_.push_back(phase.phase_id);
            for (const TopicWorkload& topic : phase.peer_topics) {
                application_.push_back(ApplicationIngressState{
                    .phase_id = phase.phase_id,
                    .topic = topic,
                });
            }
        }
    }

    void ConfigureLatency(const std::vector<LatencyPhasePlan>& phases) {
        size_t topic_count = 0;
        for (const LatencyPhasePlan& phase : phases) {
            topic_count += phase.topics.size();
        }
        latency_.reserve(topic_count);
        for (const LatencyPhasePlan& phase : phases) {
            for (const LatencyTopicPlan& topic : phase.topics) {
                latency_.push_back(LatencyIngressState{
                    .phase_id = phase.phase_id,
                    .initiator_role = phase.initiator_role,
                    .topic_id = topic.topic_id,
                });
            }
        }
    }

    Status BeginLatencyPhase(uint32_t phase_id) {
        if (!pending_echoes_.empty()) {
            return Corruption("latency phase began with pending echoes");
        }
        const bool known = std::any_of(
            latency_.begin(), latency_.end(),
            [phase_id](const LatencyIngressState& state) {
                return state.phase_id == phase_id;
            });
        if (!known) return Corruption("latency phase is unknown");
        active_latency_phase_ = phase_id;
        return Status::Ok();
    }

    Status DecodeValidatePublish(const WireFrame& frame) override {
        if (frame.header.topic_id == kApplicationConfirmTopicId) {
            return DecodeApplicationConfirm(frame);
        }
        if (frame.header.topic_id == kApplicationReleaseTopicId) {
            return DecodeApplicationRelease(frame);
        }
        if (frame.header.topic_id == kApplicationBarrierTopicId) {
            return DecodeApplicationBarrier(frame);
        }
        if (frame.header.topic_id == kLegacyTopicId) {
            return DecodeLegacy(frame);
        }
        auto latency_state = std::find_if(
            latency_.begin(), latency_.end(),
            [&frame](const LatencyIngressState& candidate) {
                return candidate.topic_id == frame.header.topic_id;
            });
        if (latency_state != latency_.end()) {
            return DecodeLatency(frame, &*latency_state);
        }
        auto state = std::find_if(
            application_.begin(), application_.end(),
            [&frame](const ApplicationIngressState& candidate) {
                return candidate.topic.topic_id == frame.header.topic_id;
            });
        if (state == application_.end()) {
            unknown_topic_leakage_ = true;
            return Corruption("application frame used an unknown topic");
        }
        if (peer_schema_ == nullptr || peer_schema_->handle == nullptr ||
            !FrameMatchesSchema(frame, peer_schema_->handle->identity()) ||
            frame.header.source_node_id != LocalFence(peer_role_).node_id.value ||
            frame.header.source_publisher_id != state->topic.publisher_id ||
            frame.header.source_publisher_epoch != state->topic.publisher_epoch) {
            state->payload_verified = false;
            return Corruption("application frame source or schema mismatch");
        }
        if (frame.header.sequence_num != state->next_sequence ||
            frame.header.sequence_num > state->topic.messages) {
            state->ordered = false;
            return Corruption("application frame is gapped or out of order");
        }
        if (frame.payload.size() != kApplicationPayloadBytes ||
            !std::equal(kApplicationMagic.begin(), kApplicationMagic.end(),
                        frame.payload.begin()) ||
            GetU32(frame.payload, 8) != state->phase_id ||
            GetU32(frame.payload, 12) != RoleCode(peer_role_) ||
            GetU64(frame.payload, 16) != frame.header.topic_id ||
            GetU64(frame.payload, 24) != frame.header.sequence_num ||
            GetU64(frame.payload, 32) != frame.header.source_node_id ||
            GetU64(frame.payload, 40) != frame.header.source_publisher_id ||
            GetU64(frame.payload, 48) != frame.header.source_publisher_epoch ||
            GetU32(frame.payload, 56) != kApplicationPayloadBytes ||
            GetU32(frame.payload, 60) !=
                (0x4d350000u | RoleCode(peer_role_))) {
            state->payload_verified = false;
            state->cross_topic_leakage =
                frame.payload.size() == kApplicationPayloadBytes &&
                GetU64(frame.payload, 16) != frame.header.topic_id;
            return Corruption("application frame header/payload mismatch");
        }
        const std::vector<std::byte> expected = ApplicationPayload(
            state->phase_id, state->topic, frame.header.sequence_num,
            peer_role_);
        if (frame.payload != expected) {
            state->payload_verified = false;
            return Corruption("application frame deterministic payload mismatch");
        }
        ++state->received;
        ++state->next_sequence;
        return Status::Ok();
    }

    bool ApplicationBarrierReceived(uint32_t phase_id,
                                    uint64_t sequence) const noexcept {
        return sequence != 0 && sequence <= application_barrier_phases_.size() &&
               application_barriers_received_ >= sequence &&
               application_barrier_phases_[sequence - 1] == phase_id;
    }

    uint64_t ApplicationBarriersReceived() const noexcept {
        return application_barriers_received_;
    }

    bool ApplicationReleaseReceived(uint32_t phase_id,
                                    uint64_t sequence) const noexcept {
        return local_role_ == "client" && sequence != 0 &&
               sequence <= application_barrier_phases_.size() &&
               application_releases_received_ >= sequence &&
               application_barrier_phases_[sequence - 1] == phase_id;
    }

    uint64_t ApplicationReleasesReceived() const noexcept {
        return application_releases_received_;
    }

    bool ApplicationConfirmReceived(uint32_t phase_id,
                                    uint64_t sequence) const noexcept {
        return local_role_ == "server" && sequence != 0 &&
               sequence <= application_barrier_phases_.size() &&
               application_confirms_received_ >= sequence &&
               application_barrier_phases_[sequence - 1] == phase_id;
    }

    uint64_t ApplicationConfirmsReceived() const noexcept {
        return application_confirms_received_;
    }

    bool PhaseComplete(uint32_t phase_id) const {
        bool found = false;
        for (const ApplicationIngressState& state : application_) {
            if (state.phase_id != phase_id) continue;
            found = true;
            if (state.received != state.topic.messages) return false;
        }
        return found;
    }

    uint64_t PhaseReceived(uint32_t phase_id) const {
        uint64_t received = 0;
        for (const ApplicationIngressState& state : application_) {
            if (state.phase_id == phase_id) received += state.received;
        }
        return received;
    }

    bool PhaseLeakage(uint32_t phase_id) const {
        if (unknown_topic_leakage_) return true;
        return std::any_of(
            application_.begin(), application_.end(),
            [phase_id](const ApplicationIngressState& state) {
                return state.phase_id == phase_id &&
                       state.cross_topic_leakage;
            });
    }

    const ApplicationIngressState* TopicState(uint32_t topic_id) const {
        const auto state = std::find_if(
            application_.begin(), application_.end(),
            [topic_id](const ApplicationIngressState& candidate) {
                return candidate.topic.topic_id == topic_id;
            });
        return state == application_.end() ? nullptr : &*state;
    }

    bool LatencyPhaseComplete(uint32_t phase_id) const {
        bool found = false;
        for (const LatencyIngressState& state : latency_) {
            if (state.phase_id != phase_id) continue;
            found = true;
            if (state.received != kLatencyMessagesPerTopic) return false;
        }
        return found;
    }

    uint64_t LatencyPhaseReceived(uint32_t phase_id) const {
        uint64_t received = 0;
        for (const LatencyIngressState& state : latency_) {
            if (state.phase_id == phase_id) received += state.received;
        }
        return received;
    }

    uint64_t LatencyTopicReceived(uint32_t phase_id, uint32_t topic_id) const {
        const auto state = FindLatencyState(phase_id, topic_id);
        return state == latency_.end() ? 0 : state->received;
    }

    Status RecordLatencyRequest(uint32_t phase_id, uint32_t topic_id,
                                uint64_t sequence, uint64_t origin_sent_ns) {
        auto state = FindLatencyState(phase_id, topic_id);
        if (state == latency_.end() || active_latency_phase_ != phase_id ||
            state->initiator_role != local_role_ || origin_sent_ns == 0 ||
            sequence != state->requests_dispatched + 1 ||
            state->requests_dispatched != state->received ||
            sequence > kLatencyMessagesPerTopic) {
            return Corruption("latency request dispatch correlation mismatch");
        }
        state->sent_origins[sequence] = origin_sent_ns;
        ++state->requests_dispatched;
        return Status::Ok();
    }

    const PendingLatencyEcho* PendingEcho() const {
        return pending_echoes_.empty() ? nullptr : &pending_echoes_.front();
    }

    Status PopPendingEcho() {
        if (pending_echoes_.empty()) {
            return Corruption("latency echo queue underflow");
        }
        pending_echoes_.pop_front();
        return Status::Ok();
    }

    size_t PendingEchoCount() const noexcept { return pending_echoes_.size(); }

    std::vector<uint64_t> LatencyRtts(uint32_t phase_id) const {
        std::vector<uint64_t> rtts;
        for (const LatencyCompletion& completion : latency_completions_) {
            if (completion.phase_id == phase_id) {
                rtts.push_back(completion.rtt_ns);
            }
        }
        return rtts;
    }

    std::vector<uint64_t> sequences;
    std::vector<Envelope> envelopes;
    bool forced_disconnect = false;

private:
    using LatencyIterator = std::vector<LatencyIngressState>::iterator;
    using ConstLatencyIterator = std::vector<LatencyIngressState>::const_iterator;

    LatencyIterator FindLatencyState(uint32_t phase_id, uint32_t topic_id) {
        return std::find_if(
            latency_.begin(), latency_.end(),
            [phase_id, topic_id](const LatencyIngressState& state) {
                return state.phase_id == phase_id && state.topic_id == topic_id;
            });
    }

    ConstLatencyIterator FindLatencyState(uint32_t phase_id,
                                          uint32_t topic_id) const {
        return std::find_if(
            latency_.begin(), latency_.end(),
            [phase_id, topic_id](const LatencyIngressState& state) {
                return state.phase_id == phase_id && state.topic_id == topic_id;
            });
    }

    Status DecodeLatency(const WireFrame& frame, LatencyIngressState* state) {
        if (state == nullptr || active_latency_phase_ != state->phase_id) {
            return Corruption("latency frame arrived outside its active phase");
        }
        if (state->initiator_role != local_role_ &&
            state->initiator_role != peer_role_) {
            return Corruption("latency phase has an unknown initiator role");
        }
        const uint32_t expected_kind =
            state->initiator_role == peer_role_ ? kLatencyRequestKind
                                                : kLatencyResponseKind;
        if (peer_schema_ == nullptr || peer_schema_->handle == nullptr ||
            !FrameMatchesSchema(frame, peer_schema_->handle->identity()) ||
            frame.header.source_node_id != LocalFence(peer_role_).node_id.value ||
            frame.header.source_publisher_id !=
                LatencyPublisherId(peer_role_, state->topic_id) ||
            frame.header.source_publisher_epoch !=
                LatencyPublisherEpoch(peer_role_, state->topic_id)) {
            return Corruption("latency frame source or schema mismatch");
        }
        if (frame.header.sequence_num != state->next_sequence ||
            frame.header.sequence_num > kLatencyMessagesPerTopic) {
            return Corruption("latency frame is gapped or out of order");
        }
        if (frame.payload.size() != kLatencyPayloadBytes ||
            !std::equal(kLatencyMagic.begin(), kLatencyMagic.end(),
                        frame.payload.begin()) ||
            GetU32(frame.payload, 8) != state->phase_id ||
            GetU32(frame.payload, 12) != expected_kind ||
            GetU64(frame.payload, 16) != frame.header.topic_id ||
            GetU64(frame.payload, 24) != frame.header.sequence_num ||
            GetU32(frame.payload, 40) != RoleCode(state->initiator_role) ||
            GetU32(frame.payload, 44) != kLatencyPayloadBytes ||
            GetU64(frame.payload, 48) !=
                LocalFence(state->initiator_role).node_id.value ||
            GetU64(frame.payload, 56) !=
                LatencyPublisherId(state->initiator_role, state->topic_id) ||
            GetU64(frame.payload, 64) !=
                LatencyPublisherEpoch(state->initiator_role, state->topic_id) ||
            GetU32(frame.payload, 72) != (0x52545400u | expected_kind) ||
            GetU32(frame.payload, 76) !=
                (0x4d4e0000u | RoleCode(state->initiator_role))) {
            return Corruption("latency frame header or payload mismatch");
        }
        const uint64_t origin_sent_ns = GetU64(frame.payload, 32);
        if (origin_sent_ns == 0) {
            return Corruption("latency frame has an empty origin timestamp");
        }
        const std::vector<std::byte> expected = LatencyPayload(
            state->phase_id, expected_kind, state->topic_id,
            frame.header.sequence_num, origin_sent_ns, state->initiator_role);
        if (frame.payload != expected) {
            return Corruption("latency frame deterministic payload mismatch");
        }
        if (expected_kind == kLatencyRequestKind) {
            pending_echoes_.push_back(PendingLatencyEcho{
                .phase_id = state->phase_id,
                .topic_id = state->topic_id,
                .sequence = frame.header.sequence_num,
                .origin_sent_ns = origin_sent_ns,
                .initiator_role = state->initiator_role,
            });
        } else {
            const uint64_t sequence = frame.header.sequence_num;
            if (state->requests_dispatched != sequence ||
                state->sent_origins[sequence] != origin_sent_ns) {
                return Corruption("latency response correlation mismatch");
            }
            const uint64_t completed_ns = NowNs();
            if (completed_ns <= origin_sent_ns) {
                return Corruption("latency response RTT is not positive");
            }
            latency_completions_.push_back(LatencyCompletion{
                .phase_id = state->phase_id,
                .rtt_ns = completed_ns - origin_sent_ns,
            });
        }
        ++state->received;
        ++state->next_sequence;
        return Status::Ok();
    }

    Status DecodeApplicationConfirm(const WireFrame& frame) {
        if (local_role_ != "server" || peer_role_ != "client") {
            return Corruption("application confirm arrived at client");
        }
        const mino::bridge::SourceIdentity expected_source =
            ApplicationConfirmSource();
        const uint64_t sequence = frame.header.sequence_num;
        if (peer_schema_ == nullptr || peer_schema_->handle == nullptr ||
            !FrameMatchesSchema(frame, peer_schema_->handle->identity()) ||
            frame.header.topic_id != kApplicationConfirmTopicId ||
            frame.header.source_node_id != expected_source.node_id ||
            frame.header.source_publisher_id != expected_source.publisher_id ||
            frame.header.source_publisher_epoch !=
                expected_source.publisher_epoch) {
            return Corruption("application confirm source or schema mismatch");
        }
        if (sequence != next_application_confirm_sequence_ || sequence == 0 ||
            sequence > application_barrier_phases_.size()) {
            return Corruption("application confirm is gapped or out of order");
        }
        const uint32_t phase_id =
            application_barrier_phases_[sequence - 1];
        if (frame.payload.size() != kApplicationBarrierPayloadBytes ||
            !std::equal(kApplicationBarrierMagic.begin(),
                        kApplicationBarrierMagic.end(), frame.payload.begin()) ||
            GetU32(frame.payload, 8) != phase_id ||
            GetU32(frame.payload, 12) != RoleCode("client") ||
            GetU64(frame.payload, 16) != kApplicationConfirmTopicId ||
            GetU64(frame.payload, 24) != sequence ||
            GetU64(frame.payload, 32) != expected_source.node_id ||
            GetU64(frame.payload, 40) != expected_source.publisher_id ||
            GetU64(frame.payload, 48) != expected_source.publisher_epoch ||
            GetU32(frame.payload, 56) != kApplicationBarrierPayloadBytes ||
            GetU32(frame.payload, 60) !=
                (0x434f4e00u | RoleCode("client"))) {
            return Corruption("application confirm header or payload mismatch");
        }
        if (frame.payload != ApplicationConfirmPayload(phase_id, sequence)) {
            return Corruption(
                "application confirm deterministic payload mismatch");
        }
        ++application_confirms_received_;
        ++next_application_confirm_sequence_;
        return Status::Ok();
    }

    Status DecodeApplicationRelease(const WireFrame& frame) {
        if (local_role_ != "client" || peer_role_ != "server") {
            return Corruption("application release arrived at server");
        }
        const mino::bridge::SourceIdentity expected_source =
            ApplicationReleaseSource();
        const uint64_t sequence = frame.header.sequence_num;
        if (peer_schema_ == nullptr || peer_schema_->handle == nullptr ||
            !FrameMatchesSchema(frame, peer_schema_->handle->identity()) ||
            frame.header.topic_id != kApplicationReleaseTopicId ||
            frame.header.source_node_id != expected_source.node_id ||
            frame.header.source_publisher_id != expected_source.publisher_id ||
            frame.header.source_publisher_epoch !=
                expected_source.publisher_epoch) {
            return Corruption("application release source or schema mismatch");
        }
        if (sequence != next_application_release_sequence_ || sequence == 0 ||
            sequence > application_barrier_phases_.size()) {
            return Corruption("application release is gapped or out of order");
        }
        const uint32_t phase_id =
            application_barrier_phases_[sequence - 1];
        if (frame.payload.size() != kApplicationBarrierPayloadBytes ||
            !std::equal(kApplicationBarrierMagic.begin(),
                        kApplicationBarrierMagic.end(), frame.payload.begin()) ||
            GetU32(frame.payload, 8) != phase_id ||
            GetU32(frame.payload, 12) != RoleCode("server") ||
            GetU64(frame.payload, 16) != kApplicationReleaseTopicId ||
            GetU64(frame.payload, 24) != sequence ||
            GetU64(frame.payload, 32) != expected_source.node_id ||
            GetU64(frame.payload, 40) != expected_source.publisher_id ||
            GetU64(frame.payload, 48) != expected_source.publisher_epoch ||
            GetU32(frame.payload, 56) != kApplicationBarrierPayloadBytes ||
            GetU32(frame.payload, 60) !=
                (0x52454c00u | RoleCode("server"))) {
            return Corruption("application release header or payload mismatch");
        }
        if (frame.payload != ApplicationReleasePayload(phase_id, sequence)) {
            return Corruption(
                "application release deterministic payload mismatch");
        }
        ++application_releases_received_;
        ++next_application_release_sequence_;
        return Status::Ok();
    }

    Status DecodeApplicationBarrier(const WireFrame& frame) {
        const mino::bridge::SourceIdentity expected_source =
            ApplicationBarrierSource(peer_role_);
        const uint64_t sequence = frame.header.sequence_num;
        if (peer_schema_ == nullptr || peer_schema_->handle == nullptr ||
            !FrameMatchesSchema(frame, peer_schema_->handle->identity()) ||
            frame.header.topic_id != kApplicationBarrierTopicId ||
            frame.header.source_node_id != expected_source.node_id ||
            frame.header.source_publisher_id != expected_source.publisher_id ||
            frame.header.source_publisher_epoch !=
                expected_source.publisher_epoch) {
            return Corruption("application barrier source or schema mismatch");
        }
        if (sequence != next_application_barrier_sequence_ || sequence == 0 ||
            sequence > application_barrier_phases_.size()) {
            return Corruption("application barrier is gapped or out of order");
        }
        const uint32_t phase_id =
            application_barrier_phases_[sequence - 1];
        if (frame.payload.size() != kApplicationBarrierPayloadBytes ||
            !std::equal(kApplicationBarrierMagic.begin(),
                        kApplicationBarrierMagic.end(), frame.payload.begin()) ||
            GetU32(frame.payload, 8) != phase_id ||
            GetU32(frame.payload, 12) != RoleCode(peer_role_) ||
            GetU64(frame.payload, 16) != kApplicationBarrierTopicId ||
            GetU64(frame.payload, 24) != sequence ||
            GetU64(frame.payload, 32) != expected_source.node_id ||
            GetU64(frame.payload, 40) != expected_source.publisher_id ||
            GetU64(frame.payload, 48) != expected_source.publisher_epoch ||
            GetU32(frame.payload, 56) != kApplicationBarrierPayloadBytes ||
            GetU32(frame.payload, 60) !=
                (0x42415200u | RoleCode(peer_role_))) {
            return Corruption("application barrier header or payload mismatch");
        }
        const std::vector<std::byte> expected =
            ApplicationBarrierPayload(phase_id, sequence, peer_role_);
        if (frame.payload != expected) {
            return Corruption(
                "application barrier deterministic payload mismatch");
        }
        ++application_barriers_received_;
        ++next_application_barrier_sequence_;
        return Status::Ok();
    }

    Status DecodeLegacy(const WireFrame& frame) {
        if (peer_schema_ == nullptr || peer_schema_->handle == nullptr ||
            !FrameMatchesSchema(frame, peer_schema_->handle->identity()) ||
            frame.header.source_node_id != peer_legacy_source_.node_id ||
            frame.header.source_publisher_id !=
                peer_legacy_source_.publisher_id ||
            frame.header.source_publisher_epoch !=
                peer_legacy_source_.publisher_epoch) {
            return Corruption("legacy frame source or schema mismatch");
        }
        auto decoded = DecodeEnvelope(frame.payload);
        if (!decoded.ok()) return decoded.status();
        if (!forced_disconnect && frame.header.topic_id == kLegacyTopicId &&
            frame.header.sequence_num == kReconnectSequence &&
            manager_ != nullptr && driver_ != nullptr) {
            forced_disconnect = true;
            const Status closed = driver_->Close(manager_->connection_id());
            if (!closed.ok()) return closed;
            // Publication has not committed, so dedup HWM remains at the first
            // legacy message and the sender must retransmit this pending frame.
            return Status::Error(StatusCode::kNotFound,
                                 "two-host probe injected disconnect");
        }
        sequences.push_back(frame.header.sequence_num);
        envelopes.push_back(std::move(*decoded));
        return Status::Ok();
    }

    std::shared_ptr<TcpDriver> driver_;
    std::string local_role_;
    std::string peer_role_;
    const ProbeSchema* peer_schema_;
    mino::bridge::SourceIdentity peer_legacy_source_;
    BridgeConnectionManager* manager_ = nullptr;
    std::vector<ApplicationIngressState> application_;
    std::vector<uint32_t> application_barrier_phases_;
    uint64_t next_application_barrier_sequence_ = 1;
    uint64_t application_barriers_received_ = 0;
    uint64_t next_application_release_sequence_ = 1;
    uint64_t application_releases_received_ = 0;
    uint64_t next_application_confirm_sequence_ = 1;
    uint64_t application_confirms_received_ = 0;
    std::vector<LatencyIngressState> latency_;
    std::deque<PendingLatencyEcho> pending_echoes_;
    std::vector<LatencyCompletion> latency_completions_;
    uint32_t active_latency_phase_ = 0;
    bool unknown_topic_leakage_ = false;
};

Status ValidatePeerEnvelope(const Options& options, const Envelope& peer) {
    const std::string_view expected_role =
        options.role == "server" ? "client" : "server";
    if (peer.role != expected_role || peer.commit != options.commit ||
        peer.run_proof != options.run_proof) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "peer Mino probe identity or run proof mismatch");
    }
    if (peer.machine_identity == options.machine_identity) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "server and client machine identities must be different");
    }
    return Status::Ok();
}

BridgeRouteContract RouteContract(uint32_t topic_id, size_t payload_size) {
    return BridgeRouteContract{
        .stamp = mino::transport::RouteStamp{
            .topic_id = TopicId{topic_id},
            .policy = mino::registry::RoutePolicy::kDiscovery,
            .topic_config_version = topic_id + 1,
            .route_version = topic_id + 2,
            .node_registry_version = 3,
            .driver_registry_version = 4,
            .acl_validator_version = 5,
            .schema_validator_version = 6,
            .local_provider_version = 7,
        },
        .delivery = mino::registry::DeliveryPolicy{
            .reliability = Reliability::kReliableOrdered,
            .allow_drop = false,
        },
        .payload_size = static_cast<uint32_t>(payload_size),
        .priority = 0,
    };
}

Status DispatchReliable(
    BridgeRuntimeDispatcher* dispatcher,
    const mino::transport::TargetRoute& target, const Options& options,
    const ProbeSchema& schema, uint32_t topic_id, uint64_t publisher_id,
    uint64_t publisher_epoch, uint64_t sequence,
    std::span<const std::byte> payload) {
    const BridgeDispatchRequest request{
        .topic_id = TopicId{topic_id},
        .schema = schema.handle->identity(),
        .publication = LocalPublication{
            .source = mino::bridge::SourceIdentity{
                .node_id = LocalFence(options.role).node_id.value,
                .publisher_id = publisher_id,
                .publisher_epoch = publisher_epoch,
            },
            .sequence_num = sequence,
            .timestamp_ns = NowNs(),
            .message_type = static_cast<uint32_t>(
                schema.handle->identity().short_id() &
                uint64_t{0xffff'ffff}),
        },
        .priority = 0,
        .canonical_payload = payload,
        .route = {},
    };
    const BridgeRouteContract contract = RouteContract(topic_id, payload.size());
    return dispatcher->DispatchTargets(
        request, std::span<const mino::transport::TargetRoute>(&target, 1),
        contract);
}

Status DispatchLegacy(BridgeRuntimeDispatcher* dispatcher,
                      const mino::transport::TargetRoute& target,
                      const Options& options, const ProbeSchema& schema,
                      uint64_t sequence) {
    const std::string envelope = EncodeEnvelope(options);
    const mino::bridge::SourceIdentity source =
        LegacySource(options.role, options.tcp_lane_count);
    if (source.publisher_id == 0) {
        return Corruption("legacy qualification source cannot select lane zero");
    }
    return DispatchReliable(dispatcher, target, options, schema, kLegacyTopicId,
                            source.publisher_id, source.publisher_epoch,
                            sequence, Bytes(envelope));
}

Status DispatchApplicationBarrier(
    BridgeRuntimeDispatcher* dispatcher,
    const mino::transport::TargetRoute& target, const Options& options,
    const ProbeSchema& schema, uint32_t phase_id, uint64_t sequence) {
    const mino::bridge::SourceIdentity source =
        ApplicationBarrierSource(options.role);
    const std::vector<std::byte> payload =
        ApplicationBarrierPayload(phase_id, sequence, options.role);
    return DispatchReliable(dispatcher, target, options, schema,
                            kApplicationBarrierTopicId, source.publisher_id,
                            source.publisher_epoch, sequence, payload);
}

Status DispatchApplicationRelease(
    BridgeRuntimeDispatcher* dispatcher,
    const mino::transport::TargetRoute& target, const Options& options,
    const ProbeSchema& schema, uint32_t phase_id, uint64_t sequence) {
    if (options.role != "server") {
        return Corruption("client attempted to dispatch application release");
    }
    const mino::bridge::SourceIdentity source = ApplicationReleaseSource();
    const std::vector<std::byte> payload =
        ApplicationReleasePayload(phase_id, sequence);
    return DispatchReliable(dispatcher, target, options, schema,
                            kApplicationReleaseTopicId, source.publisher_id,
                            source.publisher_epoch, sequence, payload);
}

Status DispatchApplicationConfirm(
    BridgeRuntimeDispatcher* dispatcher,
    const mino::transport::TargetRoute& target, const Options& options,
    const ProbeSchema& schema, uint32_t phase_id, uint64_t sequence) {
    if (options.role != "client") {
        return Corruption("server attempted to dispatch application confirm");
    }
    const mino::bridge::SourceIdentity source = ApplicationConfirmSource();
    const std::vector<std::byte> payload =
        ApplicationConfirmPayload(phase_id, sequence);
    return DispatchReliable(dispatcher, target, options, schema,
                            kApplicationConfirmTopicId, source.publisher_id,
                            source.publisher_epoch, sequence, payload);
}

Status DispatchLatency(BridgeRuntimeDispatcher* dispatcher,
                       const mino::transport::TargetRoute& target,
                       const Options& options, const ProbeSchema& schema,
                       uint32_t phase_id, uint32_t kind, uint32_t topic_id,
                       uint64_t sequence, uint64_t origin_sent_ns,
                       std::string_view initiator_role,
                       uint64_t* dispatched_origin_sent_ns = nullptr) {
    if ((kind == kLatencyRequestKind &&
         (origin_sent_ns != 0 || dispatched_origin_sent_ns == nullptr)) ||
        (kind == kLatencyResponseKind &&
         (origin_sent_ns == 0 || dispatched_origin_sent_ns != nullptr)) ||
        (kind != kLatencyRequestKind && kind != kLatencyResponseKind)) {
        return Invalid("latency dispatch kind or origin is inconsistent");
    }
    if (kind == kLatencyRequestKind) {
        origin_sent_ns = NowNs();
        *dispatched_origin_sent_ns = origin_sent_ns;
    }
    const std::vector<std::byte> payload = LatencyPayload(
        phase_id, kind, topic_id, sequence, origin_sent_ns, initiator_role);
    const BridgeRouteContract contract =
        RouteContract(topic_id, payload.size());
    const BridgeDispatchRequest request{
        .topic_id = TopicId{topic_id},
        .schema = schema.handle->identity(),
        .publication = LocalPublication{
            .source = mino::bridge::SourceIdentity{
                .node_id = LocalFence(options.role).node_id.value,
                .publisher_id = LatencyPublisherId(options.role, topic_id),
                .publisher_epoch =
                    LatencyPublisherEpoch(options.role, topic_id),
            },
            .sequence_num = sequence,
            .timestamp_ns =
                kind == kLatencyRequestKind ? origin_sent_ns : NowNs(),
            .message_type = static_cast<uint32_t>(
                schema.handle->identity().short_id() & uint64_t{0xffff'ffff}),
        },
        .priority = 0,
        .canonical_payload = payload,
        .route = {},
    };
    return dispatcher->DispatchTargets(
        request, std::span<const mino::transport::TargetRoute>(&target, 1),
        contract);
}

void CaptureConnectionEvidence(const TwoHostConnectionGroup& connections,
                               const BridgeConnectionManager& legacy_manager,
                               ProbeResult* result) {
    result->local_session_epoch = legacy_manager.local_session_epoch();
    result->remote_session_epoch = legacy_manager.remote_session_epoch();
    const auto stats = connections.stats();
    result->connection_attempts = stats.connection_attempts;
    result->accepted_connections = stats.accepted_connections;
    result->completed_handshakes = stats.completed_handshakes;
    result->reconnects = stats.reconnects;
    result->disconnects = stats.disconnects;
    result->accepted_acks = stats.accepted_acks;
    result->duplicate_checks = stats.duplicate_checks;
    result->tcp_lane_count = connections.lane_count();
    result->active_lane_connections = static_cast<uint16_t>(
        connections.active_lane_connections());
    result->lane_connections.clear();
    result->lane_connections.reserve(connections.lane_count());
    for (uint16_t lane = 0; lane < connections.lane_count(); ++lane) {
        const BridgeConnectionManager& lane_manager = connections.manager(lane);
        result->lane_connections.push_back(LaneConnectionRecord{
            .lane_index = lane,
            .active = lane_manager.state() == BridgeConnectionState::kActive,
            .local_session_epoch = lane_manager.local_session_epoch(),
            .remote_session_epoch = lane_manager.remote_session_epoch(),
            .connection_attempts = lane_manager.stats().connection_attempts,
            .accepted_connections = lane_manager.stats().accepted_connections,
            .completed_handshakes = lane_manager.stats().completed_handshakes,
            .reconnects = lane_manager.stats().reconnects,
            .disconnects = lane_manager.stats().disconnects,
        });
    }
}

struct PhasePipelineMetrics {
    uint64_t frame_body_bytes = 0;
    uint64_t inbound_frames = 0;
    uint64_t outbound_frames = 0;
    uint64_t retransmitted_frames = 0;

    void Add(const mino::bridge::BridgePumpResult& pumped) {
        frame_body_bytes += pumped.bytes;
        inbound_frames += pumped.inbound_frames;
        outbound_frames += pumped.outbound_frames;
        retransmitted_frames += pumped.retransmitted_frames;
    }
};

Status RunApplicationPhaseBarrier(
    const Options& options, uint32_t phase_id, uint64_t sequence,
    const ProbeSchema& local_schema, TwoHostConnectionGroup* connections,
    BridgeRuntimeDispatcher* dispatcher,
    const mino::transport::TargetRoute& target, ProbeIngress* ingress,
    uint64_t deadline_ns) {
    if (sequence == 0 || connections == nullptr || dispatcher == nullptr ||
        ingress == nullptr || !connections->all_pipelines_ready() ||
        connections->retransmit_entries() != 0) {
        return Corruption(
            "application barrier began before workload ACK drain");
    }
    const mino::bridge::SourceIdentity source =
        ApplicationBarrierSource(options.role);
    BridgeConnectionManager& manager = connections->manager_for(source);
    if (manager.pipeline() == nullptr) {
        return Corruption("application barrier lane has no pipeline");
    }
    const uint64_t accepted_start =
        manager.pipeline()->retransmit_stats().accepted_acks;
    bool dispatched = false;
    bool complete = false;
    while (!g_stop_requested.load(std::memory_order_relaxed) &&
           NowNs() < deadline_ns) {
        bool dispatch_progress = false;
        if (!dispatched &&
            connections->state() == BridgeConnectionState::kActive) {
            const Status status = DispatchApplicationBarrier(
                dispatcher, target, options, local_schema, phase_id, sequence);
            if (status.ok()) {
                dispatched = true;
                dispatch_progress = true;
            } else if (status.code() != StatusCode::kWouldBlock &&
                       status.code() != StatusCode::kResourceExhausted) {
                return status;
            }
        }

        BridgePumpBudget budget;
        budget.now_ns = NowNs();
        budget.max_inbound_frames = 1;
        auto pumped = connections->Pump(budget);
        if (!pumped.ok()) return pumped.status();
        const bool local_acknowledged =
            dispatched && manager.pipeline() != nullptr &&
            manager.pipeline()->retransmit_entries() == 0 &&
            manager.pipeline()->retransmit_stats().accepted_acks >
                accepted_start;
        const bool peer_arrived =
            ingress->ApplicationBarrierReceived(phase_id, sequence);
        complete = local_acknowledged && peer_arrived &&
                   connections->all_pipelines_ready() &&
                   connections->retransmit_entries() == 0;
        if (complete) break;
        if (!dispatch_progress && !pumped->pipeline.made_progress) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    if (!complete) {
        return g_stop_requested.load(std::memory_order_relaxed)
                   ? Status::Error(StatusCode::kUnavailable,
                                   "Mino probe interrupted during application barrier")
                   : TimedOut("Mino application phase barrier deadline expired");
    }
    if (ingress->ApplicationBarriersReceived() != sequence) {
        return Corruption("application barrier receive cardinality drifted");
    }

    const bool coordinator = options.role == "server";
    BridgeConnectionManager* release_manager = nullptr;
    uint64_t release_accepted_start = 0;
    if (coordinator) {
        release_manager =
            &connections->manager_for(ApplicationReleaseSource());
        if (release_manager->pipeline() == nullptr) {
            return Corruption("application release lane has no pipeline");
        }
        release_accepted_start =
            release_manager->pipeline()->retransmit_stats().accepted_acks;
    }
    bool release_dispatched = false;
    bool release_complete = false;
    while (!g_stop_requested.load(std::memory_order_relaxed) &&
           NowNs() < deadline_ns) {
        bool dispatch_progress = false;
        if (coordinator && !release_dispatched &&
            connections->state() == BridgeConnectionState::kActive) {
            const Status status = DispatchApplicationRelease(
                dispatcher, target, options, local_schema, phase_id, sequence);
            if (status.ok()) {
                release_dispatched = true;
                dispatch_progress = true;
            } else if (status.code() != StatusCode::kWouldBlock &&
                       status.code() != StatusCode::kResourceExhausted) {
                return status;
            }
        }

        BridgePumpBudget budget;
        budget.now_ns = NowNs();
        budget.max_inbound_frames = 1;
        auto pumped = connections->Pump(budget);
        if (!pumped.ok()) return pumped.status();
        if (coordinator) {
            release_complete =
                release_dispatched &&
                release_manager->pipeline() != nullptr &&
                release_manager->pipeline()->retransmit_entries() == 0 &&
                release_manager->pipeline()
                        ->retransmit_stats()
                        .accepted_acks > release_accepted_start;
        } else {
            release_complete =
                ingress->ApplicationReleaseReceived(phase_id, sequence);
        }
        release_complete = release_complete &&
                           connections->all_pipelines_ready() &&
                           connections->retransmit_entries() == 0;
        if (release_complete) break;
        if (!dispatch_progress && !pumped->pipeline.made_progress) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    if (!release_complete) {
        return g_stop_requested.load(std::memory_order_relaxed)
                   ? Status::Error(StatusCode::kUnavailable,
                                   "Mino probe interrupted during application release")
                   : TimedOut("Mino application phase release deadline expired");
    }
    const uint64_t expected_releases = coordinator ? 0 : sequence;
    if (ingress->ApplicationReleasesReceived() != expected_releases) {
        return Corruption("application release receive cardinality drifted");
    }

    const bool confirmer = options.role == "client";
    BridgeConnectionManager* confirm_manager = nullptr;
    uint64_t confirm_accepted_start = 0;
    if (confirmer) {
        confirm_manager =
            &connections->manager_for(ApplicationConfirmSource());
        if (confirm_manager->pipeline() == nullptr) {
            return Corruption("application confirm lane has no pipeline");
        }
        confirm_accepted_start =
            confirm_manager->pipeline()->retransmit_stats().accepted_acks;
    }
    bool confirm_dispatched = false;
    bool confirm_complete = false;
    while (!g_stop_requested.load(std::memory_order_relaxed) &&
           NowNs() < deadline_ns) {
        bool dispatch_progress = false;
        if (confirmer && !confirm_dispatched &&
            connections->state() == BridgeConnectionState::kActive) {
            const Status status = DispatchApplicationConfirm(
                dispatcher, target, options, local_schema, phase_id, sequence);
            if (status.ok()) {
                confirm_dispatched = true;
                dispatch_progress = true;
            } else if (status.code() != StatusCode::kWouldBlock &&
                       status.code() != StatusCode::kResourceExhausted) {
                return status;
            }
        }

        BridgePumpBudget budget;
        budget.now_ns = NowNs();
        budget.max_inbound_frames = 1;
        auto pumped = connections->Pump(budget);
        if (!pumped.ok()) return pumped.status();
        if (confirmer) {
            confirm_complete =
                confirm_dispatched && confirm_manager->pipeline() != nullptr &&
                confirm_manager->pipeline()->retransmit_entries() == 0 &&
                confirm_manager->pipeline()
                        ->retransmit_stats()
                        .accepted_acks > confirm_accepted_start;
        } else {
            confirm_complete =
                ingress->ApplicationConfirmReceived(phase_id, sequence);
        }
        confirm_complete = confirm_complete &&
                           connections->all_pipelines_ready() &&
                           connections->retransmit_entries() == 0;
        if (confirm_complete) break;
        if (!dispatch_progress && !pumped->pipeline.made_progress) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    if (!confirm_complete) {
        return g_stop_requested.load(std::memory_order_relaxed)
                   ? Status::Error(
                         StatusCode::kUnavailable,
                         "Mino probe interrupted during application confirm")
                   : TimedOut("Mino application phase confirm deadline expired");
    }
    const uint64_t expected_confirms = coordinator ? sequence : 0;
    if (ingress->ApplicationConfirmsReceived() != expected_confirms) {
        return Corruption("application confirm receive cardinality drifted");
    }
    return Status::Ok();
}

Status RunApplicationWorkloads(
    const Options& options, const std::vector<PhasePlan>& phases,
    const ProbeSchema& local_schema, TwoHostConnectionGroup* connections,
    BridgeRuntimeDispatcher* dispatcher,
    const mino::transport::TargetRoute& target, ProbeIngress* ingress,
    uint64_t deadline_ns, ProbeResult* result) {
    result->topic_routes.clear();
    result->network_scaling.clear();
    result->topic_routes.reserve(4);
    result->network_scaling.reserve(12);
    uint64_t barrier_sequence = 1;
    MINO_RETURN_IF_ERROR(RunApplicationPhaseBarrier(
        options, 0, barrier_sequence, local_schema, connections, dispatcher,
        target, ingress, deadline_ns));
    ++barrier_sequence;
    for (const PhasePlan& phase : phases) {
        if (!connections->all_pipelines_ready()) {
            return Corruption("application phase has an inactive lane pipeline");
        }
        if (connections->retransmit_entries() != 0 ||
            ingress->PhaseReceived(phase.phase_id) != 0) {
            return Corruption(
                "application phase began with cross-phase traffic");
        }
        const uint64_t phase_started_ns = NowNs();
        const uint64_t accepted_start = connections->accepted_acks();
        const uint64_t duplicate_start = connections->duplicate_checks();
        PhasePipelineMetrics metrics;
        std::vector<uint64_t> next_sequences(phase.local_topics.size(), 1);
        size_t topic_index = 0;
        size_t completed_topics = 0;
        uint64_t messages_sent = 0;
        bool peer_ack_flushed = false;
        bool phase_complete = false;
        while (!g_stop_requested.load(std::memory_order_relaxed) &&
               NowNs() < deadline_ns) {
            BridgePumpBudget budget;
            budget.now_ns = NowNs();
            budget.max_inbound_frames = 1;
            auto pumped = connections->Pump(budget);
            if (!pumped.ok()) return pumped.status();
            metrics.Add(pumped->pipeline);
            if (ingress->PhaseComplete(phase.phase_id) &&
                pumped->pipeline.outbound_frames != 0) {
                peer_ack_flushed = true;
            }

            const bool may_send =
                options.role == "client" || ingress->PhaseComplete(phase.phase_id);
            if (may_send &&
                connections->state() == BridgeConnectionState::kActive) {
                size_t dispatched = 0;
                while (completed_topics < phase.local_topics.size() &&
                       dispatched < kDispatchBatch) {
                    const TopicWorkload& topic = phase.local_topics[topic_index];
                    const uint64_t sequence = next_sequences[topic_index];
                    const std::vector<std::byte> payload = ApplicationPayload(
                        phase.phase_id, topic, sequence, options.role);
                    const Status status = DispatchReliable(
                        dispatcher, target, options, local_schema,
                        topic.topic_id, topic.publisher_id,
                        topic.publisher_epoch, sequence, payload);
                    if (!status.ok()) {
                        if (status.code() == StatusCode::kWouldBlock ||
                            status.code() == StatusCode::kResourceExhausted) {
                            break;
                        }
                        return status;
                    }
                    ++messages_sent;
                    ++next_sequences[topic_index];
                    if (next_sequences[topic_index] > topic.messages) {
                        ++completed_topics;
                    }
                    topic_index = (topic_index + 1) % phase.local_topics.size();
                    while (completed_topics < phase.local_topics.size() &&
                           next_sequences[topic_index] >
                               phase.local_topics[topic_index].messages) {
                        topic_index =
                            (topic_index + 1) % phase.local_topics.size();
                    }
                    ++dispatched;
                }
            }

            const uint64_t accepted_now = connections->accepted_acks();
            phase_complete =
                completed_topics == phase.local_topics.size() &&
                messages_sent == phase.message_count() &&
                ingress->PhaseComplete(phase.phase_id) &&
                ingress->PhaseReceived(phase.phase_id) == phase.message_count() &&
                peer_ack_flushed && connections->all_pipelines_ready() &&
                connections->retransmit_entries() == 0 &&
                accepted_now >= accepted_start + messages_sent;
            if (phase_complete) break;
            if (!pumped->pipeline.made_progress) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        if (!phase_complete) {
            return g_stop_requested.load(std::memory_order_relaxed)
                       ? Status::Error(StatusCode::kUnavailable,
                                       "Mino probe interrupted by signal")
                       : TimedOut("Mino application phase deadline expired");
        }
        if (!connections->all_pipelines_ready()) {
            return Corruption("application phase lost a lane pipeline");
        }
        const uint64_t accepted_delta =
            connections->accepted_acks() - accepted_start;
        const uint64_t retransmission_delta = metrics.retransmitted_frames;
        const uint64_t duplicate_delta =
            connections->duplicate_checks() - duplicate_start;
        const uint64_t elapsed_ms = std::max<uint64_t>(
            1, (NowNs() - phase_started_ns) / 1'000'000ull);
        const bool cross_topic_leakage = ingress->PhaseLeakage(phase.phase_id);
        if (!phase.scaling()) {
            for (const TopicWorkload& topic : phase.local_topics) {
                result->topic_routes.push_back(TopicRouteRecord{
                    .topic_id = topic.topic_id,
                    .direction = "sent",
                    .messages = topic.messages,
                    .ordered = true,
                    .payload_verified = true,
                    .cross_topic_leakage = cross_topic_leakage,
                });
            }
            for (const TopicWorkload& topic : phase.peer_topics) {
                const ApplicationIngressState* state =
                    ingress->TopicState(topic.topic_id);
                if (state == nullptr || state->received != topic.messages) {
                    return Corruption(
                        "correctness topic ingress evidence is incomplete");
                }
                result->topic_routes.push_back(TopicRouteRecord{
                    .topic_id = topic.topic_id,
                    .direction = "received",
                    .messages = state->received,
                    .ordered = state->ordered,
                    .payload_verified = state->payload_verified,
                    .cross_topic_leakage =
                        state->cross_topic_leakage || cross_topic_leakage,
                });
            }
        } else {
            const uint64_t messages_received =
                ingress->PhaseReceived(phase.phase_id);
            if (duplicate_delta < messages_received) {
                return Corruption(
                    "dedup checks are below committed scaling message count");
            }
            const uint64_t duplicate_suppressed =
                duplicate_delta - messages_received;
            const uint64_t prefix_bytes =
                4 * (metrics.inbound_frames + metrics.outbound_frames);
            result->network_scaling.push_back(NetworkScalingRecord{
                .mode = phase.mode,
                .topic_count = phase.topic_count,
                .messages_per_topic = phase.messages_per_topic,
                .messages_sent = messages_sent,
                .messages_received = messages_received,
                .application_payload_bytes =
                    messages_sent * kApplicationPayloadBytes,
                .mino_frame_body_bytes = metrics.frame_body_bytes,
                .pipeline_inbound_frames = metrics.inbound_frames,
                .pipeline_outbound_frames = metrics.outbound_frames,
                .tcp_prefix_bytes = prefix_bytes,
                .tcp_framed_bytes = metrics.frame_body_bytes + prefix_bytes,
                .elapsed_ms = elapsed_ms,
                .accepted_acks = accepted_delta,
                .retransmissions = retransmission_delta,
                .duplicate_suppressed = duplicate_suppressed,
                .cross_topic_leakage = cross_topic_leakage,
            });
        }
        MINO_RETURN_IF_ERROR(RunApplicationPhaseBarrier(
            options, phase.phase_id, barrier_sequence, local_schema,
            connections, dispatcher, target, ingress, deadline_ns));
        ++barrier_sequence;
    }
    if (ingress->ApplicationBarriersReceived() != phases.size() + 1) {
        return Corruption("application barrier cardinality is incomplete");
    }
    if (result->topic_routes.size() != 4 ||
        result->network_scaling.size() != 12) {
        return Corruption("application evidence cardinality is inconsistent");
    }
    return Status::Ok();
}

uint64_t PositiveNanosecondsToMicroseconds(uint64_t nanoseconds) {
    if (nanoseconds == 0) return 0;
    return std::max<uint64_t>(
        1, nanoseconds / 1'000ull + (nanoseconds % 1'000ull != 0 ? 1 : 0));
}

uint64_t NearestRank(const std::vector<uint64_t>& sorted_samples,
                     uint32_t percentile) {
    const size_t rank =
        (sorted_samples.size() * percentile + 99u) / 100u;
    return sorted_samples[rank - 1];
}

Status RunLatencyWorkloads(
    const Options& options, const std::vector<LatencyPhasePlan>& phases,
    const ProbeSchema& local_schema, TwoHostConnectionGroup* connections,
    BridgeRuntimeDispatcher* dispatcher,
    const mino::transport::TargetRoute& target, ProbeIngress* ingress,
    uint64_t deadline_ns, ProbeResult* result) {
    result->latency_samples.clear();
    result->latency_samples.reserve(kScalingTopicCounts.size());
    for (const LatencyPhasePlan& phase : phases) {
        if (!connections->all_pipelines_ready() ||
            connections->retransmit_entries() != 0) {
            return Corruption("latency phase has pending prior publications");
        }
        MINO_RETURN_IF_ERROR(ingress->BeginLatencyPhase(phase.phase_id));
        const uint64_t accepted_start = connections->accepted_acks();
        const bool local_initiator = phase.initiator_role == options.role;
        std::vector<uint64_t> requests_dispatched(phase.topics.size(), 0);
        size_t topic_index = 0;
        uint64_t local_sends = 0;
        bool final_inbound_ack_flushed = false;
        bool phase_complete = false;
        while (!g_stop_requested.load(std::memory_order_relaxed) &&
               NowNs() < deadline_ns) {
            BridgePumpBudget budget;
            budget.now_ns = NowNs();
            auto pumped = connections->Pump(budget);
            if (!pumped.ok()) return pumped.status();
            if (ingress->LatencyPhaseComplete(phase.phase_id) &&
                pumped->pipeline.outbound_frames != 0) {
                final_inbound_ack_flushed = true;
            }

            bool dispatch_progress = false;
            if (connections->state() == BridgeConnectionState::kActive) {
                if (local_initiator) {
                    size_t examined = 0;
                    size_t dispatched = 0;
                    while (examined < phase.topics.size() &&
                           dispatched < kLatencyDispatchBatch) {
                        const size_t current = topic_index;
                        topic_index = (topic_index + 1) % phase.topics.size();
                        ++examined;
                        const LatencyTopicPlan& topic = phase.topics[current];
                        const uint64_t received = ingress->LatencyTopicReceived(
                            phase.phase_id, topic.topic_id);
                        if (requests_dispatched[current] >=
                                kLatencyMessagesPerTopic ||
                            requests_dispatched[current] != received) {
                            continue;
                        }
                        const uint64_t sequence =
                            requests_dispatched[current] + 1;
                        uint64_t origin_sent_ns = 0;
                        const Status status = DispatchLatency(
                            dispatcher, target, options, local_schema,
                            phase.phase_id, kLatencyRequestKind, topic.topic_id,
                            sequence, 0, phase.initiator_role,
                            &origin_sent_ns);
                        if (!status.ok()) {
                            if (status.code() == StatusCode::kWouldBlock ||
                                status.code() ==
                                    StatusCode::kResourceExhausted) {
                                break;
                            }
                            return status;
                        }
                        MINO_RETURN_IF_ERROR(ingress->RecordLatencyRequest(
                            phase.phase_id, topic.topic_id, sequence,
                            origin_sent_ns));
                        ++requests_dispatched[current];
                        ++local_sends;
                        ++dispatched;
                        dispatch_progress = true;
                    }
                } else {
                    size_t dispatched = 0;
                    while (dispatched < kLatencyDispatchBatch) {
                        const PendingLatencyEcho* pending = ingress->PendingEcho();
                        if (pending == nullptr) break;
                        if (pending->phase_id != phase.phase_id ||
                            pending->initiator_role != phase.initiator_role) {
                            return Corruption("latency echo queue phase mismatch");
                        }
                        const Status status = DispatchLatency(
                            dispatcher, target, options, local_schema,
                            pending->phase_id, kLatencyResponseKind,
                            pending->topic_id, pending->sequence,
                            pending->origin_sent_ns, pending->initiator_role);
                        if (!status.ok()) {
                            if (status.code() == StatusCode::kWouldBlock ||
                                status.code() ==
                                    StatusCode::kResourceExhausted) {
                                break;
                            }
                            return status;
                        }
                        MINO_RETURN_IF_ERROR(ingress->PopPendingEcho());
                        ++local_sends;
                        ++dispatched;
                        dispatch_progress = true;
                    }
                }
            }

            const uint64_t accepted_now = connections->accepted_acks();
            phase_complete =
                local_sends == phase.sample_count() &&
                ingress->LatencyPhaseComplete(phase.phase_id) &&
                ingress->LatencyPhaseReceived(phase.phase_id) ==
                    phase.sample_count() &&
                ingress->PendingEchoCount() == 0 &&
                final_inbound_ack_flushed &&
                connections->all_pipelines_ready() &&
                connections->retransmit_entries() == 0 &&
                accepted_now >= accepted_start + local_sends;
            if (phase_complete) break;
            if (!dispatch_progress && !pumped->pipeline.made_progress) {
                std::this_thread::yield();
            }
        }
        if (!phase_complete) {
            return g_stop_requested.load(std::memory_order_relaxed)
                       ? Status::Error(StatusCode::kUnavailable,
                                       "Mino probe interrupted by signal")
                       : TimedOut("Mino latency phase deadline expired");
        }
        if (!local_initiator) continue;

        std::vector<uint64_t> samples_ns =
            ingress->LatencyRtts(phase.phase_id);
        if (samples_ns.size() != phase.sample_count() || samples_ns.empty()) {
            return Corruption("latency RTT sample cardinality is inconsistent");
        }
        const uint64_t single_message_rtt_us =
            PositiveNanosecondsToMicroseconds(samples_ns.front());
        std::vector<uint64_t> samples_us;
        samples_us.reserve(samples_ns.size());
        for (uint64_t sample_ns : samples_ns) {
            const uint64_t sample_us =
                PositiveNanosecondsToMicroseconds(sample_ns);
            if (sample_us == 0) {
                return Corruption("latency RTT sample is not positive");
            }
            samples_us.push_back(sample_us);
        }
        std::sort(samples_us.begin(), samples_us.end());
        const uint64_t p50_rtt_us = NearestRank(samples_us, 50);
        const uint64_t p95_rtt_us = NearestRank(samples_us, 95);
        const uint64_t p99_rtt_us = NearestRank(samples_us, 99);
        const uint64_t max_rtt_us = samples_us.back();
        if (single_message_rtt_us == 0 ||
            single_message_rtt_us > max_rtt_us ||
            p50_rtt_us > p95_rtt_us || p95_rtt_us > p99_rtt_us ||
            p99_rtt_us > max_rtt_us) {
            return Corruption("latency RTT statistics are inconsistent");
        }
        result->latency_samples.push_back(LatencySampleRecord{
            .topic_count = phase.topic_count,
            .messages_per_topic = kLatencyMessagesPerTopic,
            .sample_count = phase.sample_count(),
            .single_message_rtt_us = single_message_rtt_us,
            .p50_rtt_us = p50_rtt_us,
            .p95_rtt_us = p95_rtt_us,
            .p99_rtt_us = p99_rtt_us,
            .max_rtt_us = max_rtt_us,
        });
    }
    if (result->latency_samples.size() != kScalingTopicCounts.size()) {
        return Corruption("latency evidence cardinality is inconsistent");
    }
    for (size_t index = 0; index < kScalingTopicCounts.size(); ++index) {
        if (result->latency_samples[index].topic_count !=
                kScalingTopicCounts[index] ||
            result->latency_samples[index].messages_per_topic !=
                kLatencyMessagesPerTopic ||
            result->latency_samples[index].sample_count !=
                static_cast<uint64_t>(kScalingTopicCounts[index]) *
                    kLatencyMessagesPerTopic) {
            return Corruption("latency evidence shape is inconsistent");
        }
    }
    return Status::Ok();
}

Status RunProbe(const Options& options, ProbeResult* result) {
    MINO_ASSIGN_OR_RETURN(auto client_schema, BuildProbeSchema("client"));
    MINO_ASSIGN_OR_RETURN(auto server_schema, BuildProbeSchema("server"));
    const ProbeSchema& local_schema =
        options.role == "client" ? client_schema : server_schema;
    const ProbeSchema& peer_schema =
        options.role == "client" ? server_schema : client_schema;
    result->local_schema_digest =
        DigestHex(local_schema.handle->identity().canonical_digest());
    result->peer_schema_digest =
        DigestHex(peer_schema.handle->identity().canonical_digest());
    result->schema_identity_nonempty =
        CompleteSchemaIdentity(local_schema.handle->identity()) &&
        CompleteSchemaIdentity(peer_schema.handle->identity());
    result->descriptor_artifact_nonempty = !local_schema.artifact.empty() &&
                                           !peer_schema.artifact.empty();
    result->descriptor_artifact_bytes = local_schema.artifact.size();
    if (!result->schema_identity_nonempty ||
        !result->descriptor_artifact_nonempty) {
        return Corruption("probe schema identity or artifact is empty");
    }

    mino::schema::SchemaRegistry registry;
    MINO_ASSIGN_OR_RETURN(
        auto local_registered,
        registry.RegisterDescriptor(local_schema.handle));
    static_cast<void>(local_registered);
    std::filesystem::path store_root = options.output.parent_path();
    if (store_root.empty()) store_root = ".";
    store_root /= "schema-store";
    std::error_code remove_error;
    std::filesystem::remove_all(store_root, remove_error);
    if (remove_error) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot reset probe schema store");
    }
    MINO_ASSIGN_OR_RETURN(
        auto store, mino::storage::SchemaStore::Open(store_root, &registry));
    ExactDescriptorAuth descriptor_auth(&peer_schema);
    StorePersistence descriptor_persistence(store.get());

    MINO_ASSIGN_OR_RETURN(auto endpoint,
                          ResolveEndpoint(options.address, options.port));
    MINO_ASSIGN_OR_RETURN(auto created, TcpDriver::Create(DriverOptions()));
    auto driver = std::shared_ptr<TcpDriver>(std::move(created));
    const std::vector<PhasePlan> phases = BuildPhasePlans(options.role);
    const std::vector<LatencyPhasePlan> latency_phases =
        BuildLatencyPhasePlans();
    result->exercised_lane_count = static_cast<uint16_t>(
        PlannedLaneCoverage(options, phases, latency_phases));
    if (result->exercised_lane_count != options.tcp_lane_count) {
        return Corruption(
            "planned two-host workload does not exercise every TCP lane");
    }
    const std::string peer_role =
        options.role == "client" ? "server" : "client";
    const mino::bridge::SourceIdentity local_legacy_source =
        LegacySource(options.role, options.tcp_lane_count);
    const mino::bridge::SourceIdentity peer_legacy_source =
        LegacySource(peer_role, options.tcp_lane_count);
    if (local_legacy_source.publisher_id == 0 ||
        peer_legacy_source.publisher_id == 0) {
        return Corruption("legacy qualification source selection failed");
    }
    ProbeIngress ingress(driver, options.role, peer_role, &peer_schema,
                         peer_legacy_source);
    ingress.ConfigureApplication(phases);
    ingress.ConfigureLatency(latency_phases);
    const BridgeConnectionManagerOptions manager_options =
        ManagerOptions(options, endpoint);
    MINO_ASSIGN_OR_RETURN(
        auto connections,
        TwoHostConnectionGroup::Create(
            manager_options, options.tcp_lane_count, driver, &ingress,
            &registry, &descriptor_auth, &descriptor_persistence));
    BridgeConnectionManager* manager =
        &connections->manager_for(local_legacy_source);
    ingress.Attach(&connections->manager_for(peer_legacy_source));

    auto descriptor_provider =
        std::make_shared<ExactDescriptorProvider>(&local_schema);
    MINO_ASSIGN_OR_RETURN(
        auto dispatcher,
        BridgeRuntimeDispatcher::Create(1, descriptor_provider, 512));
    const BridgeNodeIdentityFence peer_fence = PeerFence(options.role);
    MINO_RETURN_IF_ERROR(
        dispatcher->RegisterPeer(peer_fence.node_id, connections->pool()));
    const mino::transport::RemoteTargetRoute remote_route{
        .endpoint = endpoint,
        .node_config_version = peer_fence.node_config_version,
        .process_identity = peer_fence.process_identity,
        .lease_epoch = peer_fence.lease_epoch,
        .driver_id = manager_options.route_driver_id,
        .driver_generation = manager_options.route_driver_generation,
        .capabilities = driver->capabilities(),
        .driver = driver,
    };
    const mino::transport::TargetRoute target{
        .target_node = peer_fence.node_id,
        .transport = remote_route,
    };
    MINO_RETURN_IF_ERROR(connections->Start(NowNs()));

    const uint64_t started_ns = NowNs();
    const uint64_t deadline_ns =
        started_ns + static_cast<uint64_t>(options.deadline_seconds) *
                         kNanosecondsPerSecond;
    size_t validated_inbound = 0;
    bool initial_enqueued = false;
    bool reconnect_enqueued = false;
    bool replay_enqueued = false;
    bool initial_acknowledged = false;
    bool pending_observed = false;
    bool replay_pending_observed = false;
    uint64_t observed_retransmitted_frames = 0;
    while (!g_stop_requested.load(std::memory_order_relaxed) &&
           NowNs() < deadline_ns) {
        BridgePumpBudget budget;
        budget.now_ns = NowNs();
        budget.max_inbound_frames = 1;
        auto pumped = connections->Pump(budget);
        if (!pumped.ok()) {
            CaptureConnectionEvidence(*connections, *manager, result);
            (void)connections->Shutdown();
            return pumped.status();
        }
        observed_retransmitted_frames +=
            pumped->pipeline.retransmitted_frames;
        if (connections->state() == BridgeConnectionState::kActive) {
            result->bridge_active = true;
            result->session_discovery = manager->local_session_epoch() != 0 &&
                                        manager->remote_session_epoch() != 0;
            if (result->initial_local_session_epoch == 0) {
                result->initial_local_session_epoch =
                    manager->local_session_epoch();
                result->initial_remote_session_epoch =
                    manager->remote_session_epoch();
            }
            if (options.role == "client" && !initial_enqueued) {
                MINO_RETURN_IF_ERROR(DispatchLegacy(
                    dispatcher.get(), target, options, local_schema,
                    kInitialSequence));
                initial_enqueued = true;
                result->reliable_sent = true;
            }
        }

        result->schema_announcement =
            result->schema_announcement ||
            connections->schema_announcement_observed();
        result->schema_request = result->schema_request ||
                                 connections->schema_request_pending();

        while (validated_inbound < ingress.envelopes.size()) {
            MINO_RETURN_IF_ERROR(ValidatePeerEnvelope(
                options, ingress.envelopes[validated_inbound]));
            const uint64_t sequence = ingress.sequences[validated_inbound];
            if (sequence != kInitialSequence &&
                sequence != kReconnectSequence) {
                return Corruption("peer reliable sequence is unexpected");
            }
            const Envelope& peer = ingress.envelopes[validated_inbound];
            result->peer_commit = peer.commit;
            result->peer_machine_identity = peer.machine_identity;
            result->peer_address = peer.advertised_address;
            ++validated_inbound;
        }

        const bool initial_received = std::find(
            ingress.sequences.begin(), ingress.sequences.end(),
            kInitialSequence) != ingress.sequences.end();
        const bool reconnect_received = std::find(
            ingress.sequences.begin(), ingress.sequences.end(),
            kReconnectSequence) != ingress.sequences.end();
        if (options.role == "server" && initial_received &&
            !initial_enqueued &&
            connections->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(DispatchLegacy(
                dispatcher.get(), target, options, local_schema,
                kInitialSequence));
            initial_enqueued = true;
            result->reliable_sent = true;
        }

        if (initial_enqueued && initial_received && manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() == 0 &&
            manager->pipeline()->retransmit_stats().accepted_acks >= 1) {
            initial_acknowledged = true;
        }
        if (options.role == "client" && initial_acknowledged &&
            !reconnect_enqueued &&
            connections->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(DispatchLegacy(
                dispatcher.get(), target, options, local_schema,
                kReconnectSequence));
            reconnect_enqueued = true;
        }
        if (reconnect_enqueued && manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() != 0 &&
            manager->stats().reconnects == 0) {
            pending_observed = true;
            result->pending_reliable_before_disconnect = true;
        }

        const bool first_reconnect_recovered =
            manager->stats().reconnects >= 1 && manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() == 0;
        if (options.role == "client" && reconnect_enqueued &&
            first_reconnect_recovered && !replay_enqueued &&
            connections->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(DispatchLegacy(
                dispatcher.get(), target, options, local_schema,
                kReconnectSequence));
            replay_enqueued = true;
            result->reliable_replay_sent = true;
        }
        if (options.role == "server" && reconnect_received &&
            manager->pipeline() != nullptr &&
            manager->pipeline()->dedup_stats().duplicate_checks >= 1 &&
            !reconnect_enqueued &&
            connections->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(DispatchLegacy(
                dispatcher.get(), target, options, local_schema,
                kReconnectSequence));
            reconnect_enqueued = true;
        }
        if (options.role == "server" && reconnect_enqueued &&
            manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() != 0 &&
            manager->stats().reconnects < 2) {
            pending_observed = true;
            result->pending_reliable_before_disconnect = true;
        }
        const bool second_reconnect_recovered =
            manager->stats().reconnects >= 2 && manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() == 0;
        if (options.role == "server" && reconnect_enqueued &&
            second_reconnect_recovered && !replay_enqueued &&
            connections->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(DispatchLegacy(
                dispatcher.get(), target, options, local_schema,
                kReconnectSequence));
            replay_enqueued = true;
            result->reliable_replay_sent = true;
        }

        if (replay_enqueued && manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() != 0) {
            replay_pending_observed = true;
            result->reliable_replay_pending_observed = true;
        }

        CaptureConnectionEvidence(*connections, *manager, result);
        result->forced_disconnect = ingress.forced_disconnect;
        result->automatic_reconnect = result->reconnects >= 2;
        result->session_epoch_changed =
            result->initial_local_session_epoch != 0 &&
            result->initial_remote_session_epoch != 0 &&
            result->local_session_epoch != 0 &&
            result->remote_session_epoch != 0 &&
            result->initial_local_session_epoch != result->local_session_epoch &&
            result->initial_remote_session_epoch != result->remote_session_epoch;
        result->pending_reliable_recovered =
            pending_observed && result->reconnects >= 1 &&
            manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() == 0;
        result->pending_reliable_retransmitted =
            result->pending_reliable_recovered &&
            observed_retransmitted_frames >= 1;
        result->dedup_state_preserved =
            manager->pipeline() != nullptr &&
            !manager->pipeline()->reliability_degraded();
        result->duplicate_suppressed =
            result->duplicate_checks >= 1 && ingress.envelopes.size() == 2;
        result->remote_acknowledged =
            replay_enqueued && replay_pending_observed &&
            manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() == 0 &&
            result->accepted_acks >= 3;
        result->bidirectional_ack = result->remote_acknowledged &&
                                    reconnect_received &&
                                    result->duplicate_suppressed;
        result->reliable_received = initial_received && reconnect_received;
        result->schema_persisted = descriptor_persistence.attempts >= 1;
        result->descriptor_authentications = descriptor_auth.authentications;
        result->descriptor_persistences = descriptor_persistence.attempts;

        const bool complete =
            result->session_discovery && result->bridge_active &&
            result->tcp_lane_count == options.tcp_lane_count &&
            result->active_lane_connections == options.tcp_lane_count &&
            result->exercised_lane_count == options.tcp_lane_count &&
            result->lane_connections.size() == options.tcp_lane_count &&
            result->reliable_sent && result->reliable_received &&
            result->remote_acknowledged && result->schema_announcement &&
            result->schema_request && result->schema_persisted &&
            result->forced_disconnect && result->automatic_reconnect &&
            result->session_epoch_changed &&
            result->pending_reliable_before_disconnect &&
            result->pending_reliable_recovered &&
            result->pending_reliable_retransmitted &&
            result->reliable_replay_sent &&
            result->reliable_replay_pending_observed &&
            result->dedup_state_preserved &&
            result->duplicate_suppressed && result->bidirectional_ack;
        if (complete) {
            const Status workloads = RunApplicationWorkloads(
                options, phases, local_schema, connections.get(),
                dispatcher.get(), target, &ingress, deadline_ns, result);
            if (!workloads.ok()) {
                CaptureConnectionEvidence(*connections, *manager, result);
                (void)connections->Shutdown();
                return workloads;
            }
            const Status latency = RunLatencyWorkloads(
                options, latency_phases, local_schema, connections.get(),
                dispatcher.get(), target, &ingress, deadline_ns, result);
            if (!latency.ok()) {
                CaptureConnectionEvidence(*connections, *manager, result);
                (void)connections->Shutdown();
                return latency;
            }
            auto persisted = store->FindRef(peer_schema.handle->identity());
            if (!persisted.ok() ||
                *persisted == mino::storage::kInvalidSchemaRef ||
                *persisted != descriptor_persistence.persisted_ref) {
                CaptureConnectionEvidence(*connections, *manager, result);
                (void)connections->Shutdown();
                return Corruption("peer descriptor persistence cannot be resolved");
            }
            MINO_ASSIGN_OR_RETURN(auto entry, store->Resolve(*persisted));
            const std::string expected_digest =
                DigestHex(peer_schema.handle->identity().canonical_digest());
            const std::string expected_filename = expected_digest + ".schema";
            if (!SameIdentity(entry.identity, peer_schema.handle->identity()) ||
                entry.descriptor_path.filename() != expected_filename) {
                CaptureConnectionEvidence(*connections, *manager, result);
                (void)connections->Shutdown();
                return Corruption(
                    "persisted peer descriptor identity or digest is inconsistent");
            }
            result->persisted_schema_identity_verified = true;
            result->persisted_schema_digest = expected_digest;

            std::error_code file_error;
            const uintmax_t persisted_size =
                std::filesystem::file_size(entry.descriptor_path, file_error);
            if (file_error || persisted_size != peer_schema.artifact.size()) {
                CaptureConnectionEvidence(*connections, *manager, result);
                (void)connections->Shutdown();
                return Corruption("persisted peer descriptor artifact is missing");
            }
            std::ifstream persisted_input(entry.descriptor_path,
                                          std::ios::in | std::ios::binary);
            std::vector<std::byte> persisted_bytes(peer_schema.artifact.size());
            if (!persisted_input ||
                !persisted_input.read(
                    reinterpret_cast<char*>(persisted_bytes.data()),
                    static_cast<std::streamsize>(persisted_bytes.size())) ||
                persisted_input.peek() != std::char_traits<char>::eof() ||
                persisted_bytes != peer_schema.artifact) {
                CaptureConnectionEvidence(*connections, *manager, result);
                (void)connections->Shutdown();
                return Corruption(
                    "persisted peer descriptor artifact bytes differ");
            }
            result->persisted_schema_bytes_verified = true;
            for (size_t grace = 0; grace < 100; ++grace) {
                budget.now_ns = NowNs();
                auto grace_pump = connections->Pump(budget);
                if (!grace_pump.ok()) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            CaptureConnectionEvidence(*connections, *manager, result);
            result->outcome = "passed";
            const Status shutdown = connections->Shutdown();
            return shutdown.ok() ? Status::Ok() : shutdown;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    CaptureConnectionEvidence(*connections, *manager, result);
    result->descriptor_authentications = descriptor_auth.authentications;
    result->descriptor_persistences = descriptor_persistence.attempts;
    (void)connections->Shutdown();
    return g_stop_requested.load(std::memory_order_relaxed)
               ? Status::Error(StatusCode::kUnavailable,
                               "Mino probe interrupted by signal")
               : TimedOut("Mino probe deadline expired");
}

Status WriteResult(const Options& options, const ProbeResult& result) {
    std::error_code directory_error;
    if (!options.output.parent_path().empty()) {
        std::filesystem::create_directories(options.output.parent_path(),
                                            directory_error);
        if (directory_error) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot create result directory");
        }
    }
    std::ofstream output(options.output, std::ios::out | std::ios::trunc);
    if (!output) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot open result JSON");
    }
    output << "{\n"
           << "  \"schema_version\": 6,\n"
           << "  \"protocol\": \"" << kProtocol << "\",\n"
           << "  \"role\": \"" << JsonEscape(result.role) << "\",\n"
           << "  \"outcome\": \"" << JsonEscape(result.outcome) << "\",\n"
           << "  \"commit\": \"" << result.commit << "\",\n"
           << "  \"machine_identity\": \"" << result.machine_identity
           << "\",\n"
           << "  \"peer_commit\": \"" << result.peer_commit << "\",\n"
           << "  \"peer_machine_identity\": \""
           << result.peer_machine_identity << "\",\n"
           << "  \"local_address\": \"" << JsonEscape(result.local_address)
           << "\",\n"
           << "  \"peer_address\": \"" << JsonEscape(result.peer_address)
           << "\",\n"
           << "  \"session_discovery\": "
           << (result.session_discovery ? "true" : "false") << ",\n"
           << "  \"bridge_active\": "
           << (result.bridge_active ? "true" : "false") << ",\n"
           << "  \"reliable_sent\": "
           << (result.reliable_sent ? "true" : "false") << ",\n"
           << "  \"reliable_received\": "
           << (result.reliable_received ? "true" : "false") << ",\n"
           << "  \"remote_acknowledged\": "
           << (result.remote_acknowledged ? "true" : "false") << ",\n"
           << "  \"schema_identity_nonempty\": "
           << (result.schema_identity_nonempty ? "true" : "false") << ",\n"
           << "  \"descriptor_artifact_nonempty\": "
           << (result.descriptor_artifact_nonempty ? "true" : "false")
           << ",\n"
           << "  \"schema_announcement\": "
           << (result.schema_announcement ? "true" : "false") << ",\n"
           << "  \"schema_request\": "
           << (result.schema_request ? "true" : "false") << ",\n"
           << "  \"schema_persisted\": "
           << (result.schema_persisted ? "true" : "false") << ",\n"
           << "  \"persisted_schema_identity_verified\": "
           << (result.persisted_schema_identity_verified ? "true" : "false")
           << ",\n"
           << "  \"persisted_schema_bytes_verified\": "
           << (result.persisted_schema_bytes_verified ? "true" : "false")
           << ",\n"
           << "  \"persisted_schema_digest\": \""
           << result.persisted_schema_digest << "\",\n"
           << "  \"forced_disconnect\": "
           << (result.forced_disconnect ? "true" : "false") << ",\n"
           << "  \"automatic_reconnect\": "
           << (result.automatic_reconnect ? "true" : "false") << ",\n"
           << "  \"session_epoch_changed\": "
           << (result.session_epoch_changed ? "true" : "false") << ",\n"
           << "  \"pending_reliable_before_disconnect\": "
           << (result.pending_reliable_before_disconnect ? "true" : "false")
           << ",\n"
           << "  \"pending_reliable_recovered\": "
           << (result.pending_reliable_recovered ? "true" : "false")
           << ",\n"
           << "  \"pending_reliable_retransmitted\": "
           << (result.pending_reliable_retransmitted ? "true" : "false")
           << ",\n"
           << "  \"reliable_replay_sent\": "
           << (result.reliable_replay_sent ? "true" : "false") << ",\n"
           << "  \"reliable_replay_pending_observed\": "
           << (result.reliable_replay_pending_observed ? "true" : "false")
           << ",\n"
           << "  \"dedup_state_preserved\": "
           << (result.dedup_state_preserved ? "true" : "false") << ",\n"
           << "  \"duplicate_suppressed\": "
           << (result.duplicate_suppressed ? "true" : "false") << ",\n"
           << "  \"bidirectional_ack\": "
           << (result.bidirectional_ack ? "true" : "false") << ",\n"
           << "  \"tcp_lane_count\": " << result.tcp_lane_count << ",\n"
           << "  \"active_lane_connections\": "
           << result.active_lane_connections << ",\n"
           << "  \"exercised_lane_count\": "
           << result.exercised_lane_count << ",\n"
           << "  \"lane_connections\": [\n";
    for (size_t index = 0; index < result.lane_connections.size(); ++index) {
        const LaneConnectionRecord& lane = result.lane_connections[index];
        output << "    {\"lane_index\": " << lane.lane_index
               << ", \"active\": " << (lane.active ? "true" : "false")
               << ", \"local_session_epoch\": " << lane.local_session_epoch
               << ", \"remote_session_epoch\": " << lane.remote_session_epoch
               << ", \"connection_attempts\": " << lane.connection_attempts
               << ", \"accepted_connections\": " << lane.accepted_connections
               << ", \"completed_handshakes\": " << lane.completed_handshakes
               << ", \"reconnects\": " << lane.reconnects
               << ", \"disconnects\": " << lane.disconnects << "}"
               << (index + 1 == result.lane_connections.size() ? "\n" : ",\n");
    }
    output << "  ],\n"
           << "  \"topic_routes\": [\n";
    for (size_t index = 0; index < result.topic_routes.size(); ++index) {
        const TopicRouteRecord& route = result.topic_routes[index];
        output << "    {\"topic_id\": " << route.topic_id
               << ", \"direction\": \"" << route.direction
               << "\", \"messages\": " << route.messages
               << ", \"ordered\": "
               << (route.ordered ? "true" : "false")
               << ", \"payload_verified\": "
               << (route.payload_verified ? "true" : "false")
               << ", \"cross_topic_leakage\": "
               << (route.cross_topic_leakage ? "true" : "false") << "}"
               << (index + 1 == result.topic_routes.size() ? "\n" : ",\n");
    }
    output << "  ],\n"
           << "  \"network_scaling\": [\n";
    for (size_t index = 0; index < result.network_scaling.size(); ++index) {
        const NetworkScalingRecord& sample = result.network_scaling[index];
        output << "    {\"mode\": \"" << sample.mode
               << "\", \"topic_count\": " << sample.topic_count
               << ", \"messages_per_topic\": "
               << sample.messages_per_topic
               << ", \"messages_sent\": " << sample.messages_sent
               << ", \"messages_received\": " << sample.messages_received
               << ", \"application_payload_bytes\": "
               << sample.application_payload_bytes
               << ", \"mino_frame_body_bytes\": "
               << sample.mino_frame_body_bytes
               << ", \"pipeline_inbound_frames\": "
               << sample.pipeline_inbound_frames
               << ", \"pipeline_outbound_frames\": "
               << sample.pipeline_outbound_frames
               << ", \"tcp_prefix_bytes\": " << sample.tcp_prefix_bytes
               << ", \"tcp_framed_bytes\": " << sample.tcp_framed_bytes
               << ", \"elapsed_ms\": " << sample.elapsed_ms
               << ", \"accepted_acks\": " << sample.accepted_acks
               << ", \"retransmissions\": " << sample.retransmissions
               << ", \"duplicate_suppressed\": "
               << sample.duplicate_suppressed
               << ", \"cross_topic_leakage\": "
               << (sample.cross_topic_leakage ? "true" : "false") << "}"
               << (index + 1 == result.network_scaling.size() ? "\n"
                                                               : ",\n");
    }
    output << "  ],\n"
           << "  \"latency_samples\": [\n";
    for (size_t index = 0; index < result.latency_samples.size(); ++index) {
        const LatencySampleRecord& sample = result.latency_samples[index];
        output << "    {\"topic_count\": " << sample.topic_count
               << ", \"messages_per_topic\": "
               << sample.messages_per_topic
               << ", \"sample_count\": " << sample.sample_count
               << ", \"single_message_rtt_us\": "
               << sample.single_message_rtt_us
               << ", \"p50_rtt_us\": " << sample.p50_rtt_us
               << ", \"p95_rtt_us\": " << sample.p95_rtt_us
               << ", \"p99_rtt_us\": " << sample.p99_rtt_us
               << ", \"max_rtt_us\": " << sample.max_rtt_us << "}"
               << (index + 1 == result.latency_samples.size() ? "\n"
                                                               : ",\n");
    }
    output << "  ],\n"
           << "  \"local_schema_digest\": \""
           << result.local_schema_digest << "\",\n"
           << "  \"peer_schema_digest\": \""
           << result.peer_schema_digest << "\",\n"
           << "  \"initial_local_session_epoch\": "
           << result.initial_local_session_epoch << ",\n"
           << "  \"initial_remote_session_epoch\": "
           << result.initial_remote_session_epoch << ",\n"
           << "  \"local_session_epoch\": " << result.local_session_epoch
           << ",\n"
           << "  \"remote_session_epoch\": " << result.remote_session_epoch
           << ",\n"
           << "  \"connection_attempts\": " << result.connection_attempts
           << ",\n"
           << "  \"accepted_connections\": " << result.accepted_connections
           << ",\n"
           << "  \"completed_handshakes\": " << result.completed_handshakes
           << ",\n"
           << "  \"reconnects\": " << result.reconnects << ",\n"
           << "  \"disconnects\": " << result.disconnects << ",\n"
           << "  \"accepted_acks\": " << result.accepted_acks << ",\n"
           << "  \"duplicate_checks\": " << result.duplicate_checks << ",\n"
           << "  \"descriptor_authentications\": "
           << result.descriptor_authentications << ",\n"
           << "  \"descriptor_persistences\": "
           << result.descriptor_persistences << ",\n"
           << "  \"descriptor_artifact_bytes\": "
           << result.descriptor_artifact_bytes << ",\n"
           << "  \"elapsed_ms\": " << result.elapsed_ms << ",\n"
           << "  \"error\": \"" << JsonEscape(result.error) << "\"\n"
           << "}\n";
    output.flush();
    return output ? Status::Ok()
                  : Status::Error(StatusCode::kUnavailable,
                                  "cannot write result JSON");
}

}  // namespace

int main(int argc, char** argv) {
    const auto parsed = ParseOptions(argc, argv);
    if (!parsed.ok()) {
        std::cerr << parsed.status().ToString() << '\n';
        return 2;
    }
    const Options& options = *parsed;
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
    ProbeResult result;
    result.role = options.role;
    result.commit = options.commit;
    result.machine_identity = options.machine_identity;
    result.local_address =
        FormatEndpoint(options.advertise_address, options.port);
    result.peer_address = options.role == "client" ? options.address : "";
    const uint64_t started_ns = NowNs();
    const Status status = RunProbe(options, &result);
    result.elapsed_ms = (NowNs() - started_ns) / 1'000'000ull;
    if (!status.ok()) {
        result.outcome = "failed";
        result.error = status.ToString();
    }
    const Status written = WriteResult(options, result);
    if (!written.ok()) {
        std::cerr << written.ToString() << '\n';
        return 3;
    }
    std::cout << "mino_result=" << options.output << '\n';
    return status.ok() ? 0 : 1;
}
