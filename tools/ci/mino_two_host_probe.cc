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

namespace {

using mino::NodeId;
using mino::ProcessIdentity;
using mino::Result;
using mino::Status;
using mino::StatusCode;
using mino::bridge::BridgeConnectionManager;
using mino::bridge::BridgeConnectionManagerOptions;
using mino::bridge::BridgeConnectionMode;
using mino::bridge::BridgeConnectionState;
using mino::bridge::BridgeIngressPort;
using mino::bridge::BridgeNodeIdentityFence;
using mino::bridge::BridgePumpBudget;
using mino::bridge::DescriptorAuth;
using mino::bridge::DescriptorPersistence;
using mino::bridge::EncodedOutboundFrame;
using mino::bridge::FrameFlag;
using mino::bridge::FrameType;
using mino::bridge::FlagValue;
using mino::bridge::SchemaNegotiator;
using mino::bridge::WireFrame;
using mino::registry::Reliability;
using mino::transport::EndpointDescriptor;
using mino::transport::TcpDriver;
using mino::transport::TcpDriverOptions;

constexpr std::string_view kProtocol = "mino-two-host-mino-v2";
constexpr std::string_view kEnvelopeMagic = "MINO_TWO_HOST_MINO_V2";
constexpr size_t kCommitLength = 40;
constexpr size_t kDigestLength = 64;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr size_t kMaxProbeFrameBytes = 256u * 1024u;
constexpr uint64_t kInitialSequence = 1;
constexpr uint64_t kReconnectSequence = 2;

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
    uint64_t elapsed_ms = 0;
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
    if (options.role != "server" && options.role != "client") {
        return Invalid("role must be server or client");
    }
    if (options.port < 1024 || options.deadline_seconds == 0 ||
        options.deadline_seconds > 3600) {
        return Invalid("port or deadline is outside the supported range");
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
    options.max_connection_send_buffer_bytes = kMaxProbeFrameBytes;
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

class ProbeIngress final : public BridgeIngressPort {
public:
    explicit ProbeIngress(std::shared_ptr<TcpDriver> driver)
        : driver_(std::move(driver)) {}

    void Attach(BridgeConnectionManager* manager) { manager_ = manager; }

    Status DecodeValidatePublish(const WireFrame& frame) override {
        auto decoded = DecodeEnvelope(frame.payload);
        if (!decoded.ok()) return decoded.status();
        if (!forced_disconnect && frame.header.sequence_num == kReconnectSequence &&
            manager_ != nullptr && driver_ != nullptr) {
            forced_disconnect = true;
            const Status closed = driver_->Close(manager_->connection_id());
            if (!closed.ok()) return closed;
            // Publication has not committed, so dedup HWM remains at the first
            // message and the sender must retransmit this pending reliable frame
            // after BridgeConnectionManager reconnects.
            return Status::Error(StatusCode::kNotFound,
                                 "two-host probe injected disconnect");
        }
        sequences.push_back(frame.header.sequence_num);
        envelopes.push_back(std::move(*decoded));
        return Status::Ok();
    }

    std::vector<uint64_t> sequences;
    std::vector<Envelope> envelopes;
    bool forced_disconnect = false;

private:
    std::shared_ptr<TcpDriver> driver_;
    BridgeConnectionManager* manager_ = nullptr;
};

WireFrame DataFrame(const Options& options,
                    const mino::schema::SchemaIdentity& identity,
                    uint64_t sequence) {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 7001;
    frame.header.msg_type = static_cast<uint32_t>(identity.short_id());
    frame.header.schema_version = identity.schema_version();
    frame.header.layout_version = identity.layout_version();
    frame.header.source_node_id = LocalFence(options.role).node_id.value;
    frame.header.source_publisher_id =
        options.role == "client" ? 7101 : 7201;
    frame.header.source_publisher_epoch =
        options.role == "client" ? 8101 : 8201;
    frame.header.sequence_num = sequence;
    const std::string envelope = EncodeEnvelope(options);
    frame.payload.resize(envelope.size());
    std::transform(envelope.begin(), envelope.end(), frame.payload.begin(),
                   [](char character) {
                       return static_cast<std::byte>(
                           static_cast<unsigned char>(character));
                   });
    return frame;
}

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

Status EnqueueReliable(BridgeConnectionManager* manager,
                       const Options& options, const ProbeSchema& schema,
                       uint64_t sequence) {
    return manager->Enqueue(EncodedOutboundFrame{
        .frame = DataFrame(options, schema.handle->identity(), sequence),
        .reliability = Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = schema.handle->identity(),
        .descriptor_artifact = schema.artifact,
    });
}

void CaptureManagerEvidence(const BridgeConnectionManager& manager,
                            ProbeResult* result) {
    result->local_session_epoch = manager.local_session_epoch();
    result->remote_session_epoch = manager.remote_session_epoch();
    result->connection_attempts = manager.stats().connection_attempts;
    result->accepted_connections = manager.stats().accepted_connections;
    result->completed_handshakes = manager.stats().completed_handshakes;
    result->reconnects = manager.stats().reconnects;
    result->disconnects = manager.stats().disconnects;
    if (manager.pipeline() != nullptr) {
        result->accepted_acks =
            manager.pipeline()->retransmit_stats().accepted_acks;
        result->duplicate_checks =
            manager.pipeline()->dedup_stats().duplicate_checks;
    }
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
    SchemaNegotiator schema_negotiator(&registry, &descriptor_auth,
                                       &descriptor_persistence);

    MINO_ASSIGN_OR_RETURN(auto endpoint,
                          ResolveEndpoint(options.address, options.port));
    MINO_ASSIGN_OR_RETURN(auto created, TcpDriver::Create(DriverOptions()));
    auto driver = std::shared_ptr<TcpDriver>(std::move(created));
    ProbeIngress ingress(driver);
    MINO_ASSIGN_OR_RETURN(
        auto manager,
        BridgeConnectionManager::Create(ManagerOptions(options, endpoint),
                                        driver, &ingress, &schema_negotiator));
    ingress.Attach(manager.get());
    MINO_RETURN_IF_ERROR(manager->Start(NowNs()));

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
    while (!g_stop_requested.load(std::memory_order_relaxed) &&
           NowNs() < deadline_ns) {
        BridgePumpBudget budget;
        budget.now_ns = NowNs();
        auto pumped = manager->Pump(budget);
        if (!pumped.ok()) {
            CaptureManagerEvidence(*manager, result);
            (void)manager->Shutdown();
            return pumped.status();
        }
        if (manager->state() == BridgeConnectionState::kActive) {
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
                MINO_RETURN_IF_ERROR(EnqueueReliable(
                    manager.get(), options, local_schema, kInitialSequence));
                initial_enqueued = true;
                result->reliable_sent = true;
            }
        }

        result->schema_announcement =
            result->schema_announcement ||
            schema_negotiator.local_ref_high_watermark() != 0 ||
            schema_negotiator.remote_ref_high_watermark() != 0;
        result->schema_request = result->schema_request ||
                                 schema_negotiator.pending_request_count() != 0;

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
            !initial_enqueued && manager->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(EnqueueReliable(
                manager.get(), options, local_schema, kInitialSequence));
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
            manager->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(EnqueueReliable(
                manager.get(), options, local_schema, kReconnectSequence));
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
            manager->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(EnqueueReliable(
                manager.get(), options, local_schema, kReconnectSequence));
            replay_enqueued = true;
            result->reliable_replay_sent = true;
        }
        if (options.role == "server" && reconnect_received &&
            manager->pipeline() != nullptr &&
            manager->pipeline()->dedup_stats().duplicate_checks >= 1 &&
            !reconnect_enqueued &&
            manager->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(EnqueueReliable(
                manager.get(), options, local_schema, kReconnectSequence));
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
            manager->state() == BridgeConnectionState::kActive) {
            MINO_RETURN_IF_ERROR(EnqueueReliable(
                manager.get(), options, local_schema, kReconnectSequence));
            replay_enqueued = true;
            result->reliable_replay_sent = true;
        }

        if (replay_enqueued && manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() != 0) {
            replay_pending_observed = true;
            result->reliable_replay_pending_observed = true;
        }

        CaptureManagerEvidence(*manager, result);
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
            result->pending_reliable_recovered;
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
            auto persisted = store->FindRef(peer_schema.handle->identity());
            if (!persisted.ok() ||
                *persisted == mino::storage::kInvalidSchemaRef ||
                *persisted != descriptor_persistence.persisted_ref) {
                CaptureManagerEvidence(*manager, result);
                (void)manager->Shutdown();
                return Corruption("peer descriptor persistence cannot be resolved");
            }
            MINO_ASSIGN_OR_RETURN(auto entry, store->Resolve(*persisted));
            const std::string expected_digest =
                DigestHex(peer_schema.handle->identity().canonical_digest());
            const std::string expected_filename = expected_digest + ".schema";
            if (!SameIdentity(entry.identity, peer_schema.handle->identity()) ||
                entry.descriptor_path.filename() != expected_filename) {
                CaptureManagerEvidence(*manager, result);
                (void)manager->Shutdown();
                return Corruption(
                    "persisted peer descriptor identity or digest is inconsistent");
            }
            result->persisted_schema_identity_verified = true;
            result->persisted_schema_digest = expected_digest;

            std::error_code file_error;
            const uintmax_t persisted_size =
                std::filesystem::file_size(entry.descriptor_path, file_error);
            if (file_error || persisted_size != peer_schema.artifact.size()) {
                CaptureManagerEvidence(*manager, result);
                (void)manager->Shutdown();
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
                CaptureManagerEvidence(*manager, result);
                (void)manager->Shutdown();
                return Corruption(
                    "persisted peer descriptor artifact bytes differ");
            }
            result->persisted_schema_bytes_verified = true;
            for (size_t grace = 0; grace < 100; ++grace) {
                budget.now_ns = NowNs();
                auto grace_pump = manager->Pump(budget);
                if (!grace_pump.ok()) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            CaptureManagerEvidence(*manager, result);
            result->outcome = "passed";
            const Status shutdown = manager->Shutdown();
            return shutdown.ok() ? Status::Ok() : shutdown;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    CaptureManagerEvidence(*manager, result);
    result->descriptor_authentications = descriptor_auth.authentications;
    result->descriptor_persistences = descriptor_persistence.attempts;
    (void)manager->Shutdown();
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
           << "  \"schema_version\": 4,\n"
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
