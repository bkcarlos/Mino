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
#include "mino/common/result.h"
#include "mino/common/status.h"
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
using mino::bridge::EncodedOutboundFrame;
using mino::bridge::WireFrame;
using mino::bridge::FrameFlag;
using mino::bridge::FrameType;
using mino::bridge::FlagValue;
using mino::registry::Reliability;
using mino::transport::EndpointDescriptor;
using mino::transport::TcpDriver;
using mino::transport::TcpDriverOptions;

constexpr std::string_view kProtocol = "mino-two-host-mino-v1";
constexpr std::string_view kEnvelopeMagic = "MINO_TWO_HOST_MINO_V1";
constexpr size_t kCommitLength = 40;
constexpr size_t kDigestLength = 64;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;

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
    std::string error;
    bool session_discovery = false;
    bool bridge_active = false;
    bool reliable_sent = false;
    bool reliable_received = false;
    bool remote_acknowledged = false;
    uint64_t local_session_epoch = 0;
    uint64_t remote_session_epoch = 0;
    uint64_t connection_attempts = 0;
    uint64_t accepted_connections = 0;
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
    options.max_frame_body_bytes = 4096;
    options.max_total_send_buffer_bytes = 64 * 1024;
    options.max_connection_send_buffer_bytes = 32 * 1024;
    options.max_ready_receive_bytes = 64 * 1024;
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
    manager.max_egress_bytes = 64 * 1024;
    manager.pipeline.wire_limits.max_payload_length = 4096;
    manager.pipeline.wire_limits.max_buffered_bytes = 8192;
    manager.pipeline.retransmit.max_age_ns = 60 * kNanosecondsPerSecond;
    manager.pipeline.retransmit.max_entries = 32;
    manager.pipeline.retransmit.max_bytes = 64 * 1024;
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

class ProbeIngress final : public BridgeIngressPort {
public:
    Status DecodeValidatePublish(const WireFrame& frame) override {
        auto decoded = DecodeEnvelope(frame.payload);
        if (!decoded.ok()) return decoded.status();
        envelopes.push_back(std::move(*decoded));
        return Status::Ok();
    }

    std::vector<Envelope> envelopes;
};

WireFrame DataFrame(const Options& options) {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 7001;
    frame.header.msg_type = 1;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = LocalFence(options.role).node_id.value;
    frame.header.source_publisher_id =
        options.role == "client" ? 7101 : 7201;
    frame.header.source_publisher_epoch =
        options.role == "client" ? 8101 : 8201;
    frame.header.sequence_num = 1;
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
                       const Options& options) {
    return manager->Enqueue(EncodedOutboundFrame{
        .frame = DataFrame(options),
        .reliability = Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
}

Status RunProbe(const Options& options, ProbeResult* result) {
    MINO_ASSIGN_OR_RETURN(auto endpoint,
                          ResolveEndpoint(options.address, options.port));
    MINO_ASSIGN_OR_RETURN(auto created, TcpDriver::Create(DriverOptions()));
    auto driver = std::shared_ptr<TcpDriver>(std::move(created));
    ProbeIngress ingress;
    MINO_ASSIGN_OR_RETURN(
        auto manager,
        BridgeConnectionManager::Create(ManagerOptions(options, endpoint),
                                        driver, &ingress));
    MINO_RETURN_IF_ERROR(manager->Start(NowNs()));

    const uint64_t started_ns = NowNs();
    const uint64_t deadline_ns =
        started_ns + static_cast<uint64_t>(options.deadline_seconds) *
                         kNanosecondsPerSecond;
    bool message_enqueued = false;
    bool frame_left_queue = false;
    bool peer_validated = false;
    while (!g_stop_requested.load(std::memory_order_relaxed) &&
           NowNs() < deadline_ns) {
        BridgePumpBudget budget;
        budget.now_ns = NowNs();
        auto pumped = manager->Pump(budget);
        if (!pumped.ok()) {
            (void)manager->Shutdown();
            return pumped.status();
        }
        if (manager->state() == BridgeConnectionState::kActive) {
            result->bridge_active = true;
            result->session_discovery = manager->local_session_epoch() != 0 &&
                                        manager->remote_session_epoch() != 0;
            if (options.role == "client" && !message_enqueued) {
                MINO_RETURN_IF_ERROR(EnqueueReliable(manager.get(), options));
                message_enqueued = true;
                result->reliable_sent = true;
            }
        }
        if (message_enqueued && manager->queued_egress_frames() == 0) {
            frame_left_queue = true;
        }
        if (!ingress.envelopes.empty() && !peer_validated) {
            MINO_RETURN_IF_ERROR(
                ValidatePeerEnvelope(options, ingress.envelopes.front()));
            const Envelope& peer = ingress.envelopes.front();
            result->peer_commit = peer.commit;
            result->peer_machine_identity = peer.machine_identity;
            result->peer_address = peer.advertised_address;
            result->reliable_received = true;
            peer_validated = true;
            if (options.role == "server") {
                MINO_RETURN_IF_ERROR(EnqueueReliable(manager.get(), options));
                message_enqueued = true;
                result->reliable_sent = true;
            }
        }
        const bool acknowledged =
            message_enqueued && frame_left_queue && manager->pipeline() != nullptr &&
            manager->pipeline()->retransmit_entries() == 0;
        if (acknowledged) result->remote_acknowledged = true;
        if (peer_validated && acknowledged) {
            // The receiver's ACK is flushed in the same BridgePipeline::Pump that
            // commits ingress. A short grace period keeps the connection alive
            // while the peer consumes that ACK and writes its own result.
            for (size_t grace = 0; grace < 100; ++grace) {
                budget.now_ns = NowNs();
                auto grace_pump = manager->Pump(budget);
                if (!grace_pump.ok()) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            result->local_session_epoch = manager->local_session_epoch();
            result->remote_session_epoch = manager->remote_session_epoch();
            result->connection_attempts = manager->stats().connection_attempts;
            result->accepted_connections = manager->stats().accepted_connections;
            result->outcome = "passed";
            const Status shutdown = manager->Shutdown();
            return shutdown.ok() ? Status::Ok() : shutdown;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    result->local_session_epoch = manager->local_session_epoch();
    result->remote_session_epoch = manager->remote_session_epoch();
    result->connection_attempts = manager->stats().connection_attempts;
    result->accepted_connections = manager->stats().accepted_connections;
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
           << "  \"schema_version\": 1,\n"
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
           << "  \"local_session_epoch\": " << result.local_session_epoch
           << ",\n"
           << "  \"remote_session_epoch\": " << result.remote_session_epoch
           << ",\n"
           << "  \"connection_attempts\": " << result.connection_attempts
           << ",\n"
           << "  \"accepted_connections\": " << result.accepted_connections
           << ",\n"
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
