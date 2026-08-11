// Copyright 2026 The Mino Authors
//
// Two-process TCP/UDP/RDMA benchmark over the same Canonical Wire payload matrix.
// RDMA zero-copy mode uses the real device-provider boundary directly so the
// posted buffer remains registered and pinned until terminal CQ completion.

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/platform/fabric_provider.h"
#include "mino/platform/rdma_provider.h"
#include "mino/transport/fabric_driver.h"
#include "mino/transport/rdma_driver.h"
#include "mino/transport/tcp_driver.h"
#include "mino/transport/udp_driver.h"

namespace {

using Clock = std::chrono::steady_clock;
using mino::StatusCode;
using mino::bridge::FlagValue;
using mino::bridge::FrameFlag;
using mino::bridge::WireFrame;
using mino::bridge::WireFrameCodec;
using mino::platform::FabricDeviceProvider;
using mino::platform::RdmaDeviceProvider;
using mino::transport::ConnectionId;
using mino::transport::EndpointAddressFamily;
using mino::transport::EndpointDescriptor;
using mino::transport::NetworkProtocol;
using mino::transport::TransportDriver;
using mino::transport::TransportKind;

struct Options {
    std::string transport;
    std::string role;
    std::string address = "127.0.0.1";
    uint16_t port = 19106;
    std::string udp_peer_address;
    uint16_t udp_peer_port = 0;
    std::string udp_local_address;
    uint16_t udp_local_port = 0;
    uint32_t iterations = 1000;
    std::vector<size_t> payloads = {128, 1024, 65536, 1048576};
    std::string rdma_provider;
    std::string rdma_device;
    std::string fabric_provider;
    std::string fabric_device;
    mino::platform::FabricKind fabric_kind = mino::platform::FabricKind::kIpcf;
    uint32_t fabric_domain = 0;
    uint32_t fabric_channel = 0;
    uint64_t local_node = 0;
    uint64_t local_security_domain = 0;
    uint64_t peer_node = 0;
    uint64_t peer_security_domain = 0;
    std::string output = "transport-matrix.jsonl";
};

std::optional<std::string_view> Flag(int argc, char** argv,
                                     std::string_view name) {
    const std::string prefix = "--" + std::string(name) + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        if (argument.starts_with(prefix)) return argument.substr(prefix.size());
    }
    return std::nullopt;
}

template <typename Integer>
bool ParseInteger(std::string_view text, Integer* value) {
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(),
                                        *value, 10);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

bool ParsePayloads(std::string_view text, std::vector<size_t>* payloads) {
    payloads->clear();
    while (!text.empty()) {
        const size_t comma = text.find(',');
        const std::string_view item = text.substr(0, comma);
        size_t value = 0;
        if (!ParseInteger(item, &value) || value == 0 ||
            value > 16u * 1024u * 1024u) {
            return false;
        }
        payloads->push_back(value);
        if (comma == std::string_view::npos) break;
        text.remove_prefix(comma + 1);
    }
    return !payloads->empty();
}

bool ParseOptions(int argc, char** argv, Options* options) {
    if (const auto value = Flag(argc, argv, "transport")) {
        options->transport = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "role")) {
        options->role = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "address")) {
        options->address = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "udp-peer-address")) {
        options->udp_peer_address = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "udp-local-address")) {
        options->udp_local_address = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "rdma-provider")) {
        options->rdma_provider = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "rdma-device")) {
        options->rdma_device = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "fabric-provider")) {
        options->fabric_provider = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "fabric-device")) {
        options->fabric_device = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "fabric-kind")) {
        if (*value == "ipcf") {
            options->fabric_kind = mino::platform::FabricKind::kIpcf;
        } else if (*value == "ntb") {
            options->fabric_kind = mino::platform::FabricKind::kNtb;
        } else if (*value == "cxl") {
            options->fabric_kind = mino::platform::FabricKind::kCxl;
        } else {
            return false;
        }
    }
    if (const auto value = Flag(argc, argv, "output")) {
        options->output = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "port");
        value && !ParseInteger(*value, &options->port)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "udp-peer-port");
        value && !ParseInteger(*value, &options->udp_peer_port)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "udp-local-port");
        value && !ParseInteger(*value, &options->udp_local_port)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "iterations");
        value && !ParseInteger(*value, &options->iterations)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "fabric-domain");
        value && !ParseInteger(*value, &options->fabric_domain)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "fabric-channel");
        value && !ParseInteger(*value, &options->fabric_channel)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "local-node");
        value && !ParseInteger(*value, &options->local_node)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "local-security-domain");
        value && !ParseInteger(*value, &options->local_security_domain)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "peer-node");
        value && !ParseInteger(*value, &options->peer_node)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "peer-security-domain");
        value && !ParseInteger(*value, &options->peer_security_domain)) {
        return false;
    }
    if (const auto value = Flag(argc, argv, "payloads");
        value && !ParsePayloads(*value, &options->payloads)) {
        return false;
    }
    in_addr address{};
    in_addr udp_peer{};
    in_addr udp_local{};
    const bool known_transport =
        options->transport == "tcp" || options->transport == "udp" ||
        options->transport == "rdma" ||
        options->transport == "rdma-zero-copy" ||
        options->transport == "fabric";
    return known_transport &&
           (options->role == "server" || options->role == "client") &&
           inet_pton(AF_INET, options->address.c_str(), &address) == 1 &&
           options->port != 0 && options->iterations != 0 &&
           (options->transport != "udp" ||
            (options->udp_peer_port != 0 && options->udp_local_port != 0 &&
             inet_pton(AF_INET, options->udp_peer_address.c_str(),
                       &udp_peer) == 1 &&
             inet_pton(AF_INET, options->udp_local_address.c_str(),
                       &udp_local) == 1)) &&
           options->iterations <= 10'000'000 && !options->output.empty() &&
           ((!options->transport.starts_with("rdma")) ||
            !options->rdma_provider.empty()) &&
           (options->transport != "fabric" ||
            (!options->fabric_provider.empty() &&
             !options->fabric_device.empty() &&
             options->fabric_domain != 0 && options->fabric_channel != 0 &&
             options->local_node != 0 && options->local_security_domain != 0 &&
             options->peer_node != 0 && options->peer_security_domain != 0 &&
             options->local_security_domain !=
                 options->peer_security_domain));
}

EndpointDescriptor EndpointAt(std::string_view text, uint16_t port,
                              std::string_view transport) {
    in_addr address{};
    const std::string owned(text);
    static_cast<void>(inet_pton(AF_INET, owned.c_str(), &address));
    std::array<std::byte, 4> bytes{};
    std::memcpy(bytes.data(), &address.s_addr, bytes.size());
    if (transport.starts_with("rdma")) {
        auto endpoint = EndpointDescriptor::Ip(
            TransportKind::kRdma, EndpointAddressFamily::kIpv4,
            NetworkProtocol::kRdmaCompatible, bytes, port);
        return endpoint.ok() ? *endpoint : EndpointDescriptor{};
    }
    auto endpoint = transport == "udp"
                        ? EndpointDescriptor::Ipv4Udp(bytes, port)
                        : EndpointDescriptor::Ipv4Tcp(bytes, port);
    return endpoint.ok() ? *endpoint : EndpointDescriptor{};
}

EndpointDescriptor EndpointFor(const Options& options) {
    if (options.transport == "fabric") {
        auto endpoint = EndpointDescriptor::SharedFabric(options.fabric_domain,
                                                         options.fabric_channel);
        return endpoint.ok() ? *endpoint : EndpointDescriptor{};
    }
    return EndpointAt(options.address, options.port, options.transport);
}

std::vector<std::byte> Canonical(size_t payload_bytes, uint64_t sequence) {
    WireFrame frame;
    frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 1;
    frame.header.msg_type = 0xd606;
    frame.header.connection_schema_ref = 1;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 1;
    frame.header.source_publisher_id = 1;
    frame.header.source_publisher_epoch = 1;
    frame.header.sequence_num = sequence + 1;
    frame.header.timestamp_ns = sequence;
    frame.payload.resize(payload_bytes);
    for (size_t i = 0; i < payload_bytes; ++i) {
        frame.payload[i] = static_cast<std::byte>((sequence + i * 31) & 0xff);
    }
    auto encoded = WireFrameCodec::Encode(frame);
    return encoded.ok() ? std::move(*encoded) : std::vector<std::byte>{};
}

uint64_t Nanoseconds(Clock::duration duration) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
}

uint64_t Percentile(std::vector<uint64_t> values, double percentile) {
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(
        percentile * static_cast<double>(values.size() - 1));
    return values[index];
}

mino::Result<std::shared_ptr<RdmaDeviceProvider>> Provider(
    const Options& options) {
    return mino::platform::CreateDynamicRdmaDeviceProvider({
        .plugin_path = options.rdma_provider,
        .device_name = options.rdma_device,
    });
}

mino::Result<std::shared_ptr<FabricDeviceProvider>> FabricProvider(
    const Options& options) {
    return mino::platform::CreateDynamicFabricDeviceProvider({
        .plugin_path = options.fabric_provider,
        .device_name = options.fabric_device,
        .expected_kind = options.fabric_kind,
    });
}

class QualificationFabricAttestor final
    : public mino::transport::FabricAttestationVerifier {
public:
    explicit QualificationFabricAttestor(const Options& options) noexcept
        : local_node_(options.local_node),
          local_domain_(options.local_security_domain),
          peer_node_(options.peer_node),
          peer_domain_(options.peer_security_domain),
          kind_(options.fabric_kind) {}

    mino::Result<mino::security::AuthenticatedPeer> Verify(
        const mino::transport::FabricAttestation& attestation) const noexcept override {
        if (attestation.local_node_id.value != local_node_ ||
            attestation.local_security_domain.value != local_domain_ ||
            attestation.peer_node_id.value != peer_node_ ||
            attestation.peer_security_domain.value != peer_domain_ ||
            attestation.kind != kind_ || attestation.provider_provenance.empty() ||
            attestation.local_device_id.empty() ||
            attestation.peer_device_id.empty() ||
            attestation.window_set_id == 0 ||
            attestation.window_generation == 0 ||
            attestation.session_epoch == 0 || attestation.evidence.empty()) {
            return mino::Status::Error(StatusCode::kPermissionDenied,
                                       "fabric qualification attestation mismatch");
        }
        return mino::security::AuthenticatedPeer{
            .node_id = attestation.peer_node_id,
            .security_domain = attestation.peer_security_domain,
            .credential_generation = attestation.window_generation,
        };
    }

private:
    uint64_t local_node_;
    uint64_t local_domain_;
    uint64_t peer_node_;
    uint64_t peer_domain_;
    mino::platform::FabricKind kind_;
};

mino::Result<std::shared_ptr<TransportDriver>> Driver(
    const Options& options,
    const std::shared_ptr<RdmaDeviceProvider>& provider,
    const std::shared_ptr<FabricDeviceProvider>& fabric_provider) {
    if (options.transport == "tcp") {
        mino::transport::TcpDriverOptions tcp;
        tcp.max_connection_send_buffer_bytes =
            static_cast<size_t>(tcp.max_frame_body_bytes) +
            mino::bridge::kLengthPrefixSize;
        MINO_ASSIGN_OR_RETURN(auto driver,
                              mino::transport::TcpDriver::Create(tcp));
        return std::shared_ptr<TransportDriver>(std::move(driver));
    }
    if (options.transport == "udp") {
        mino::transport::UdpDriverOptions udp;
        udp.max_datagram_bytes = 1200;
        udp.max_message_bytes = 16u * 1024u * 1024u;
        MINO_ASSIGN_OR_RETURN(auto driver,
                              mino::transport::UdpDriver::Create(udp));
        return std::shared_ptr<TransportDriver>(std::move(driver));
    }
    if (options.transport == "fabric") {
        const auto capabilities = fabric_provider->capabilities();
        if (capabilities.max_window_bytes <=
            mino::transport::kFabricWindowHeaderBytes) {
            return mino::Status::Error(StatusCode::kResourceExhausted,
                                       "fabric provider window is too small");
        }
        const size_t max_message = std::min(
            mino::transport::kMaxPayloadBytes,
            capabilities.max_window_bytes -
                mino::transport::kFabricWindowHeaderBytes);
        mino::transport::FabricDriverOptions fabric{
            .provider = fabric_provider,
            .attestation_verifier =
                std::make_shared<QualificationFabricAttestor>(options),
            .local_node_id = mino::NodeId{options.local_node},
            .local_security_domain =
                mino::SecurityDomainId{options.local_security_domain},
            .max_windows_per_connection =
                std::min<uint32_t>(64, capabilities.max_windows_per_connection),
            .event_queue_depth = 256,
            .receive_queue_depth = 256,
            .completion_queue_depth = 512,
            .max_message_bytes = max_message,
            .max_queued_receive_bytes = max_message * 4,
            .allow_mock_provider_for_testing = false,
        };
        MINO_ASSIGN_OR_RETURN(
            auto driver,
            mino::transport::FabricWindowDriver::Create(std::move(fabric)));
        return std::shared_ptr<TransportDriver>(std::move(driver));
    }
    mino::transport::RdmaDriverOptions rdma{
        .provider = provider,
        .send_queue_depth = 256,
        .receive_queue_depth = 256,
        .completion_queue_depth = 512,
        .max_message_bytes = 16u * 1024u * 1024u + 256,
        .max_queued_send_bytes = 256u * 1024u * 1024u,
        .max_queued_receive_bytes = 256u * 1024u * 1024u,
        .registration_quota_bytes = 256u * 1024u * 1024u,
        .registration_scope_id = 0xd606,
        .registration_owner = {.process_id = 1,
                               .process_epoch = 1,
                               .lease_id = 1},
        .authentication_mode =
            mino::transport::RdmaAuthenticationMode::kVerifiedPeer,
        .controlled_fabric_verifier = nullptr,
        .allow_mock_provider_for_testing = false,
    };
    MINO_ASSIGN_OR_RETURN(auto driver,
                          mino::transport::RdmaDriver::Create(std::move(rdma)));
    return std::shared_ptr<TransportDriver>(std::move(driver));
}

mino::Status FlushUntracked(TransportDriver& driver) {
    const auto deadline = Clock::now() + std::chrono::seconds(30);
    for (;;) {
        if (auto* tcp = dynamic_cast<mino::transport::TcpDriver*>(&driver);
            tcp != nullptr) {
            if (tcp->stats().queued_send_bytes == 0) return mino::Status::Ok();
        } else if (auto* rdma =
                       dynamic_cast<mino::transport::RdmaDriver*>(&driver);
                   rdma != nullptr) {
            auto progress = rdma->PollCompletions({.max_completions = 1,
                                                   .timeout_ms = 0,
                                                   .connection_id = 0});
            if (!progress.ok() &&
                progress.status().code() != StatusCode::kWouldBlock) {
                return progress.status();
            }
            if (rdma->stats().outstanding_work_requests == 0) {
                return mino::Status::Ok();
            }
        } else if (auto* fabric =
                       dynamic_cast<mino::transport::FabricWindowDriver*>(&driver);
                   fabric != nullptr) {
            auto progress = fabric->PollCompletions({.max_completions = 1,
                                                     .timeout_ms = 0,
                                                     .connection_id = 0});
            if (!progress.ok() &&
                progress.status().code() != StatusCode::kWouldBlock) {
                return progress.status();
            }
            if (fabric->stats().outstanding_windows == 0) {
                return mino::Status::Ok();
            }
        } else {
            return mino::Status::Ok();  // UDP sendmsg is synchronous.
        }
        if (Clock::now() >= deadline) {
            return mino::Status::Error(StatusCode::kTimeout,
                                       "benchmark local send drain timed out");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

mino::Result<mino::transport::ReceivedMessage> Receive(
    TransportDriver& driver, ConnectionId filter = 0) {
    MINO_ASSIGN_OR_RETURN(auto received,
                          driver.Poll({.max_messages = 1,
                                       .max_bytes = 32u * 1024u * 1024u,
                                       .timeout_ms = 60'000,
                                       .connection_id = filter}));
    return std::move(received.messages.front());
}

mino::Status RunDriverServer(const Options& options,
                             std::shared_ptr<TransportDriver> driver) {
    MINO_RETURN_IF_ERROR(driver->Start({.max_connections = 8,
                                        .max_listeners = 2,
                                        .max_queued_sends = 512}));
    ConnectionId receive_connection = 0;
    ConnectionId send_connection = 0;
    if (options.transport == "udp") {
        MINO_ASSIGN_OR_RETURN(
            auto connection,
            driver->Connect({
                .remote_endpoint = EndpointAt(options.udp_peer_address,
                                              options.udp_peer_port, "udp"),
                .local_bind = EndpointAt(options.udp_local_address,
                                         options.udp_local_port, "udp"),
                .timeout_ms = 60'000,
            }));
        receive_connection = connection.id;
        send_connection = connection.id;
    } else {
        MINO_ASSIGN_OR_RETURN(
            auto listener,
            driver->Listen({.local_endpoint = EndpointFor(options),
                            .backlog = 8}));
        MINO_ASSIGN_OR_RETURN(auto accepted,
                              driver->Accept({.listener_id = listener.id,
                                              .timeout_ms = 60'000}));
        receive_connection = accepted.id;
        send_connection = accepted.id;
    }
    const uint64_t total = static_cast<uint64_t>(options.iterations) *
                           options.payloads.size();
    for (uint64_t i = 0; i < total; ++i) {
        MINO_ASSIGN_OR_RETURN(auto message,
                              Receive(*driver, receive_connection));

        MINO_ASSIGN_OR_RETURN(
            size_t sent,
            driver->SendUntracked({
                .connection_id = send_connection,
                .payload = message.payload,
                .traffic_class =
                    mino::transport::UntrackedTrafficClass::kData,
            }));
        (void)sent;
        // SendUntracked is admission only. Drain local transport work before the
        // next phase or Shutdown without claiming remote acceptance.
        MINO_RETURN_IF_ERROR(FlushUntracked(*driver));
    }
    return driver->Shutdown();
}

mino::Status RunDriverClient(const Options& options,
                             std::shared_ptr<TransportDriver> driver,
                             std::string_view provenance) {
    MINO_RETURN_IF_ERROR(driver->Start({.max_connections = 8,
                                        .max_listeners = 2,
                                        .max_queued_sends = 512}));
    const std::optional<EndpointDescriptor> local_bind =
        options.transport == "udp"
            ? std::optional(EndpointAt(options.udp_local_address,
                                       options.udp_local_port, "udp"))
            : std::nullopt;
    MINO_ASSIGN_OR_RETURN(
        auto connection,
        driver->Connect({.remote_endpoint = EndpointFor(options),
                         .local_bind = local_bind,
                         .timeout_ms = 60'000}));
    std::ofstream output(options.output, std::ios::trunc);
    if (!output) {
        return mino::Status::Error(StatusCode::kUnavailable,
                                   "cannot open benchmark output");
    }
    uint64_t sequence = 0;
    for (size_t payload_bytes : options.payloads) {
        std::vector<uint64_t> latencies;
        latencies.reserve(options.iterations);
        const std::clock_t cpu_start = std::clock();
        const auto wall_start = Clock::now();
        size_t wire_bytes = 0;
        for (uint32_t i = 0; i < options.iterations; ++i) {
            auto canonical = Canonical(payload_bytes, sequence++);
            wire_bytes = canonical.size();
            const auto start = Clock::now();
            MINO_ASSIGN_OR_RETURN(
                size_t sent,
                driver->SendUntracked({
                    .connection_id = connection.id,
                    .payload = canonical,
                    .traffic_class =
                        mino::transport::UntrackedTrafficClass::kData,
                }));
            (void)sent;
            MINO_ASSIGN_OR_RETURN(auto echo, Receive(*driver, connection.id));
            if (echo.payload != canonical) {
                return mino::Status::Error(StatusCode::kCorruption,
                                           "benchmark echo mismatch");
            }
            latencies.push_back(Nanoseconds(Clock::now() - start));
        }
        const uint64_t elapsed_ns = Nanoseconds(Clock::now() - wall_start);
        const uint64_t cpu_ns = static_cast<uint64_t>(
            (static_cast<long double>(std::clock() - cpu_start) * 1.0e9L) /
            CLOCKS_PER_SEC);
        const long double transferred =
            static_cast<long double>(payload_bytes) * options.iterations * 2;
        const long double throughput =
            transferred * 1.0e9L / static_cast<long double>(elapsed_ns);
        output << "{\"transport\":\"" << options.transport
               << "\",\"copy_mode\":\"driver-staging\",\"payload_bytes\":"
               << payload_bytes << ",\"wire_bytes\":" << wire_bytes
               << ",\"iterations\":" << options.iterations
               << ",\"p50_rtt_ns\":" << Percentile(latencies, 0.50)
               << ",\"p99_rtt_ns\":" << Percentile(latencies, 0.99)
               << ",\"elapsed_ns\":" << elapsed_ns
               << ",\"process_cpu_ns\":" << cpu_ns
               << ",\"payload_bytes_per_second\":"
               << static_cast<uint64_t>(throughput)
               << ",\"provider_provenance\":\"" << provenance << "\"}\n";
    }
    return driver->Shutdown();
}

mino::Result<mino::platform::RdmaProviderPollResult> ProviderPoll(
    RdmaDeviceProvider& provider, uint32_t completions, uint32_t receives) {
    return provider.Poll({.max_completions = completions,
                          .max_receives = receives,
                          .max_receive_bytes = 32u * 1024u * 1024u,
                          .timeout_ms = 60'000});
}

mino::Status RunZeroCopy(const Options& options,
                         const std::shared_ptr<RdmaDeviceProvider>& provider) {
    MINO_RETURN_IF_ERROR(provider->Start({.max_connections = 8,
                                          .max_listeners = 2,
                                          .send_queue_depth = 8,
                                          .receive_queue_depth = 8,
                                          .completion_queue_depth = 16,
                                          .max_message_bytes =
                                              16u * 1024u * 1024u + 256}));
    MINO_ASSIGN_OR_RETURN(
        auto recovered,
        provider->RecoverStale({.scope_id = 0xd606,
                                .current_process_id = 1,
                                .current_process_epoch = 1}));
    (void)recovered;
    mino::platform::RdmaProviderConnection connection;
    if (options.role == "server") {
        MINO_ASSIGN_OR_RETURN(auto listener,
                              provider->Listen({.local_endpoint = EndpointAt(
                                                   options.address, options.port,
                                                   options.transport),
                                                .backlog = 8}));
        MINO_ASSIGN_OR_RETURN(connection,
                              provider->Accept(listener.id, 60'000));
    } else {
        MINO_ASSIGN_OR_RETURN(connection,
                              provider->Connect({.remote_endpoint = EndpointAt(
                                                     options.address, options.port,
                                                     options.transport),
                                                 .local_bind = std::nullopt,
                                                 .timeout_ms = 60'000}));
    }
    if (!connection.verified_peer.has_value() ||
        !connection.verified_peer->complete()) {
        return mino::Status::Error(StatusCode::kPermissionDenied,
                                   "RDMA qualification peer is not verified");
    }

    std::ofstream output;
    if (options.role == "client") output.open(options.output, std::ios::trunc);
    uint64_t wr = 1;
    uint64_t sequence = 0;
    for (size_t payload_bytes : options.payloads) {
        std::vector<uint64_t> latencies;
        const std::clock_t cpu_start = std::clock();
        const auto wall_start = Clock::now();
        for (uint32_t i = 0; i < options.iterations; ++i) {
            std::vector<std::byte> buffer;
            if (options.role == "client") {
                buffer = Canonical(payload_bytes, sequence++);
            } else {
                for (;;) {
                    MINO_ASSIGN_OR_RETURN(auto polled,
                                          ProviderPoll(*provider, 0, 1));
                    if (!polled.receives.empty()) {
                        buffer = std::move(polled.receives.front().canonical_wire);
                        break;
                    }
                }
            }
            const mino::MemoryRegistrationOwner owner{
                .process_id = 1, .process_epoch = 1, .lease_id = wr};
            MINO_ASSIGN_OR_RETURN(auto registration,
                                  provider->Register({
                                      .address = buffer.data(),
                                      .bytes = buffer.size(),
                                      .alignment = alignof(std::max_align_t),
                                      .scope_id = 0xd606,
                                      .kind = mino::MemoryRegistrationKind::kRdma,
                                      .owner = owner,
                                      .require_physical_contiguous = false,
                                  }));
            const auto started = Clock::now();
            MINO_RETURN_IF_ERROR(provider->PostSend({
                .work_request_id = wr,
                .connection_id = connection.id,
                .canonical_wire = buffer,
                .registration = registration,
            }));
            bool terminal = false;
            bool echo = options.role == "server";
            while (!terminal || !echo) {
                MINO_ASSIGN_OR_RETURN(auto polled,
                                      ProviderPoll(*provider, 1, echo ? 0 : 1));
                for (const auto& completion : polled.completions) {
                    if (completion.work_request_id == wr && completion.terminal) {
                        if (!completion.status.ok()) return completion.status;
                        terminal = true;
                    }
                }
                if (!polled.receives.empty()) {
                    if (polled.receives.front().canonical_wire != buffer) {
                        return mino::Status::Error(StatusCode::kCorruption,
                                                   "zero-copy echo mismatch");
                    }
                    echo = true;
                }
            }
            MINO_RETURN_IF_ERROR(provider->Deregister(registration));
            if (options.role == "client") {
                latencies.push_back(Nanoseconds(Clock::now() - started));
            }
            ++wr;
        }
        if (options.role == "client") {
            const uint64_t elapsed_ns = Nanoseconds(Clock::now() - wall_start);
            const uint64_t cpu_ns = static_cast<uint64_t>(
                (static_cast<long double>(std::clock() - cpu_start) * 1.0e9L) /
                CLOCKS_PER_SEC);
            const long double throughput =
                static_cast<long double>(payload_bytes) * options.iterations * 2 *
                1.0e9L / static_cast<long double>(elapsed_ns);
            output << "{\"transport\":\"rdma\",\"copy_mode\":\"registered-zero-copy\""
                   << ",\"payload_bytes\":" << payload_bytes
                   << ",\"iterations\":" << options.iterations
                   << ",\"p50_rtt_ns\":" << Percentile(latencies, 0.50)
                   << ",\"p99_rtt_ns\":" << Percentile(latencies, 0.99)
                   << ",\"elapsed_ns\":" << elapsed_ns
                   << ",\"process_cpu_ns\":" << cpu_ns
                   << ",\"payload_bytes_per_second\":"
                   << static_cast<uint64_t>(throughput)
                   << ",\"provider_provenance\":\""
                   << provider->provenance() << "\"}\n";
        }
    }
    return provider->Shutdown();
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseOptions(argc, argv, &options)) {
        std::cerr << "usage: --transport=tcp|udp|rdma|rdma-zero-copy|fabric "
                     "--role=server|client [--payloads=128,1024,...] "
                     "[--udp-local-address=IP --udp-local-port=N "
                     "--udp-peer-address=IP --udp-peer-port=N] "
                     "[--rdma-provider=/absolute/provider.so] "
                     "[--fabric-provider=/absolute/provider.so "
                     "--fabric-device=NAME --fabric-kind=ipcf|ntb|cxl "
                     "--fabric-domain=N --fabric-channel=N "
                     "--local-node=N --local-security-domain=N "
                     "--peer-node=N --peer-security-domain=N]\n";
        return 2;
    }
    std::shared_ptr<RdmaDeviceProvider> provider;
    std::shared_ptr<FabricDeviceProvider> fabric_provider;
    if (options.transport.starts_with("rdma")) {
        auto loaded = Provider(options);
        if (!loaded.ok()) {
            std::cerr << loaded.status().ToString() << '\n';
            return 1;
        }
        provider = std::move(*loaded);
    }
    if (options.transport == "fabric") {
        auto loaded = FabricProvider(options);
        if (!loaded.ok()) {
            std::cerr << loaded.status().ToString() << '\n';
            return 1;
        }
        fabric_provider = std::move(*loaded);
    }
    mino::Status status;
    if (options.transport == "rdma-zero-copy") {
        status = RunZeroCopy(options, provider);
    } else {
        auto driver = Driver(options, provider, fabric_provider);
        if (!driver.ok()) {
            std::cerr << driver.status().ToString() << '\n';
            return 1;
        }
        status = options.role == "server"
                     ? RunDriverServer(options, std::move(*driver))
                     : RunDriverClient(
                           options, std::move(*driver),
                           provider != nullptr
                               ? provider->provenance()
                               : (fabric_provider != nullptr
                                      ? fabric_provider->provenance()
                                      : "builtin-posix"));
    }
    if (!status.ok()) {
        std::cerr << status.ToString() << '\n';
        return 1;
    }
    return 0;
}
