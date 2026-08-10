// Copyright 2026 The Mino Authors
//
// Fair, process-separated Mino TcpDriver versus native ZeroMQ transport
// comparison. Production code is consumed without benchmark-only hooks.

#include <arpa/inet.h>
#include <zmq.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/common/status.h"
#include "mino/transport/tcp_driver.h"

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

constexpr std::array<std::byte, 8> kMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'B'}, std::byte{'E'}, std::byte{'N'}, std::byte{'2'},
};
constexpr size_t kBenchmarkHeaderBytes = 40;
constexpr size_t kMinimumTcpBodyBytes = mino::bridge::kWireBaseHeaderLength;
constexpr std::array<uint32_t, 6> kDefaultTopicCounts = {1, 2, 4, 8, 16, 32};
constexpr uint32_t kMaxTopicCount = 4096;
constexpr uint32_t kWireMessageType = 0x42454e32u;
constexpr uint32_t kWireSchemaRef = 0x10203040u;
constexpr uint64_t kWireNode = 0x1112131415161718ull;
constexpr uint64_t kWirePublisher = 0x2122232425262728ull;
constexpr uint64_t kWireEpoch = 0x3132333435363738ull;

constexpr uint32_t kHello = 1;
constexpr uint32_t kPhaseStart = 2;
constexpr uint32_t kData = 3;
constexpr uint32_t kWarmupDone = 4;
constexpr uint32_t kPhaseDone = 5;
constexpr uint32_t kStop = 6;
constexpr uint32_t kStopDone = 7;
constexpr uint32_t kFinalAck = 8;

struct Options {
    std::string backend;
    std::string layer;
    std::string role;
    std::string mode = "all";
    std::string address = "127.0.0.1";
    uint16_t port = 19090;
    uint16_t lane_count = 2;
    uint32_t messages_per_topic = 64;
    uint32_t warmup_messages_per_topic = 8;
    uint32_t topic_count = 0;
    uint32_t outstanding = 1;
    size_t payload_bytes = 256;
    uint32_t deadline_seconds = 120;
    std::string output = "transport_benchmark.json";
};

struct Message {
    uint32_t kind = 0;
    uint32_t phase = 0;
    uint32_t topic = 0;
    uint32_t sequence = 0;
    uint64_t origin_ns = 0;
};

struct Envelope {
    uint16_t lane = 0;
    std::vector<std::byte> body;
    std::vector<std::byte> route;
};

struct Record {
    std::string mode;
    uint32_t topic_count = 0;
    uint64_t sample_count = 0;
    uint64_t p50_rtt_us = 0;
    uint64_t p95_rtt_us = 0;
    uint64_t p99_rtt_us = 0;
    uint64_t max_rtt_us = 0;
    uint64_t elapsed_us = 0;
    double messages_per_second = 0.0;
};

uint64_t NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch())
            .count());
}

uint64_t DeadlineNs(const Options& options) {
    return NowNs() + static_cast<uint64_t>(options.deadline_seconds) *
                         1'000'000'000ull;
}

uint32_t RemainingMs(uint64_t deadline_ns, uint32_t maximum = 100) {
    const uint64_t now = NowNs();
    if (now >= deadline_ns) return 0;
    const uint64_t remaining_ns = deadline_ns - now;
    const uint64_t rounded_ms = (remaining_ns + 999'999) / 1'000'000;
    return static_cast<uint32_t>(std::min<uint64_t>(maximum, rounded_ms));
}

uint64_t ToMicroseconds(uint64_t nanoseconds) {
    return nanoseconds / 1000 + (nanoseconds % 1000 == 0 ? 0 : 1);
}

std::optional<std::string_view> Flag(int argc, char** argv,
                                     std::string_view name) {
    const std::string prefix = "--" + std::string(name) + "=";
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument.starts_with(prefix)) return argument.substr(prefix.size());
    }
    return std::nullopt;
}

template <typename Integer>
bool ParseInteger(std::string_view text, Integer* value) {
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), *value, 10);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

bool ParseOptions(int argc, char** argv, Options* options, std::string* error) {
    if (const auto value = Flag(argc, argv, "backend")) {
        options->backend = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "layer")) {
        options->layer = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "role")) {
        options->role = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "mode")) {
        options->mode = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "address")) {
        options->address = std::string(*value);
    }
    if (const auto value = Flag(argc, argv, "output")) {
        options->output = std::string(*value);
    }
    const auto parse = [&](std::string_view name, auto* destination) {
        const auto value = Flag(argc, argv, name);
        return !value || ParseInteger(*value, destination);
    };
    if (!parse("port", &options->port) ||
        !parse("lane-count", &options->lane_count) ||
        !parse("messages-per-topic", &options->messages_per_topic) ||
        !parse("warmup-messages-per-topic",
               &options->warmup_messages_per_topic) ||
        !parse("topic-count", &options->topic_count) ||
        !parse("outstanding", &options->outstanding) ||
        !parse("payload-bytes", &options->payload_bytes) ||
        !parse("deadline-seconds", &options->deadline_seconds)) {
        *error = "a numeric benchmark option is invalid";
        return false;
    }
    in_addr parsed_address{};
    if ((options->backend != "mino" && options->backend != "zmq") ||
        (options->layer != "l1" && options->layer != "l2") ||
        (options->role != "server" && options->role != "client") ||
        (options->mode != "all" && options->mode != "serial" &&
         options->mode != "per_topic_concurrent" &&
         options->mode != "one_way")) {
        *error = "required values: --backend=mino|zmq --layer=l1|l2 "
                 "--role=server|client";
        return false;
    }
    if (inet_pton(AF_INET, options->address.c_str(), &parsed_address) != 1 ||
        options->port < 1024 || options->lane_count == 0 ||
        options->lane_count > 64 ||
        options->port > std::numeric_limits<uint16_t>::max() -
                            options->lane_count + 1 ||
        options->messages_per_topic == 0 ||
        options->warmup_messages_per_topic > options->messages_per_topic ||
        options->topic_count > kMaxTopicCount ||
        options->outstanding == 0 || options->outstanding > 1024 ||
        options->payload_bytes < kBenchmarkHeaderBytes ||
        options->payload_bytes > 16u * 1024u * 1024u ||
        options->deadline_seconds == 0 || options->deadline_seconds > 3600 ||
        options->output.empty()) {
        *error = "benchmark option is outside the supported range";
        return false;
    }
    if (options->mode == "one_way" && options->topic_count == 0) {
        *error = "one_way mode requires an explicit nonzero topic-count";
        return false;
    }
    if (options->layer == "l1" &&
        options->payload_bytes < kMinimumTcpBodyBytes) {
        *error = "L1 payload-bytes must be at least 80 for TcpDriver";
        return false;
    }
    return true;
}

void PutU32(std::span<std::byte> bytes, size_t offset, uint32_t value) {
    for (size_t index = 0; index < 4; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (8 * (3 - index))) & uint32_t{0xff});
    }
}

void PutU64(std::span<std::byte> bytes, size_t offset, uint64_t value) {
    for (size_t index = 0; index < 8; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (8 * (7 - index))) & uint64_t{0xff});
    }
}

uint32_t GetU32(std::span<const std::byte> bytes, size_t offset) {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
        value = (value << 8) | static_cast<uint32_t>(bytes[offset + index]);
    }
    return value;
}

uint64_t GetU64(std::span<const std::byte> bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value = (value << 8) | static_cast<uint64_t>(bytes[offset + index]);
    }
    return value;
}

uint64_t PayloadSeed(const Message& message) {
    return static_cast<uint64_t>(message.kind) * 97 +
           static_cast<uint64_t>(message.phase) * 71 +
           static_cast<uint64_t>(message.topic) * 53 +
           static_cast<uint64_t>(message.sequence) * 31;
}

std::vector<std::byte> EncodeBenchmarkPayload(const Message& message,
                                              size_t payload_bytes) {
    std::vector<std::byte> bytes(payload_bytes);
    std::copy(kMagic.begin(), kMagic.end(), bytes.begin());
    PutU32(bytes, 8, message.kind);
    PutU32(bytes, 12, message.phase);
    PutU32(bytes, 16, message.topic);
    PutU32(bytes, 20, message.sequence);
    PutU64(bytes, 24, message.origin_ns);
    PutU64(bytes, 32, payload_bytes);
    const uint64_t seed = PayloadSeed(message);
    for (size_t index = kBenchmarkHeaderBytes; index < bytes.size(); ++index) {
        bytes[index] =
            static_cast<std::byte>((seed + index * 37) & uint64_t{0xff});
    }
    return bytes;
}

bool DecodeBenchmarkPayload(std::span<const std::byte> bytes,
                            Message* message, std::string* error) {
    if (bytes.size() < kBenchmarkHeaderBytes ||
        !std::equal(kMagic.begin(), kMagic.end(), bytes.begin()) ||
        GetU64(bytes, 32) != bytes.size()) {
        *error = "benchmark payload header is invalid";
        return false;
    }
    message->kind = GetU32(bytes, 8);
    message->phase = GetU32(bytes, 12);
    message->topic = GetU32(bytes, 16);
    message->sequence = GetU32(bytes, 20);
    message->origin_ns = GetU64(bytes, 24);
    const uint64_t seed = PayloadSeed(*message);
    for (size_t index = kBenchmarkHeaderBytes; index < bytes.size(); ++index) {
        const std::byte expected =
            static_cast<std::byte>((seed + index * 37) & uint64_t{0xff});
        if (bytes[index] != expected) {
            *error = "deterministic benchmark payload verification failed";
            return false;
        }
    }
    return true;
}

bool EncodeBody(const Options& options, const Message& message,
                std::vector<std::byte>* body, std::string* error) {
    if (options.layer == "l1") {
        *body = EncodeBenchmarkPayload(message, options.payload_bytes);
        return true;
    }
    mino::bridge::WireFrame frame;
    frame.header.frame_type = mino::bridge::FrameType::kData;
    frame.header.flags =
        mino::bridge::FlagValue(mino::bridge::FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = message.topic;
    frame.header.msg_type = kWireMessageType;
    frame.header.connection_schema_ref = kWireSchemaRef;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = kWireNode;
    frame.header.source_publisher_id = kWirePublisher;
    frame.header.source_publisher_epoch = kWireEpoch;
    frame.header.sequence_num = message.sequence;
    frame.header.timestamp_ns = message.origin_ns;
    frame.payload = EncodeBenchmarkPayload(message, options.payload_bytes);
    auto encoded = mino::bridge::WireFrameCodec::Encode(frame);
    if (!encoded.ok()) {
        *error = "WireFrameCodec::Encode failed: " + encoded.status().ToString();
        return false;
    }
    *body = std::move(*encoded);
    return true;
}

bool DecodeBody(const Options& options, std::span<const std::byte> body,
                Message* message, std::string* error) {
    if (options.layer == "l1") {
        if (body.size() != options.payload_bytes) {
            *error = "L1 body size differs from payload-bytes";
            return false;
        }
        return DecodeBenchmarkPayload(body, message, error);
    }
    auto decoded = mino::bridge::WireFrameCodec::Decode(body);
    if (!decoded.ok()) {
        *error = "WireFrameCodec::Decode failed: " + decoded.status().ToString();
        return false;
    }
    if (decoded->header.frame_type != mino::bridge::FrameType::kData ||
        decoded->header.flags != mino::bridge::FlagValue(
                                     mino::bridge::FrameFlag::kPayloadCrcPresent) ||
        decoded->header.msg_type != kWireMessageType ||
        decoded->header.connection_schema_ref != kWireSchemaRef ||
        decoded->header.schema_version != 1 ||
        decoded->header.layout_version != 1 ||
        decoded->header.source_node_id != kWireNode ||
        decoded->header.source_publisher_id != kWirePublisher ||
        decoded->header.source_publisher_epoch != kWireEpoch ||
        decoded->payload.size() != options.payload_bytes) {
        *error = "L2 deterministic WireFrame header is invalid";
        return false;
    }
    if (!DecodeBenchmarkPayload(decoded->payload, message, error)) return false;
    if (decoded->header.topic_id != message->topic ||
        decoded->header.sequence_num != message->sequence ||
        decoded->header.timestamp_ns != message->origin_ns) {
        *error = "L2 WireFrame and benchmark payload correlation differs";
        return false;
    }
    return true;
}

uint16_t LaneFor(uint32_t topic, uint16_t lane_count) {
    return static_cast<uint16_t>(topic % lane_count);
}

std::string ZmqEndpoint(const Options& options, uint16_t lane) {
    return "tcp://" + options.address + ":" +
           std::to_string(static_cast<uint32_t>(options.port) + lane);
}

bool MakeMinoEndpoint(const Options& options, uint16_t lane,
                      mino::transport::EndpointDescriptor* endpoint,
                      std::string* error) {
    in_addr address{};
    if (inet_pton(AF_INET, options.address.c_str(), &address) != 1) {
        *error = "invalid IPv4 address";
        return false;
    }
    const auto bytes = std::as_bytes(std::span(&address, size_t{1}));
    auto created = mino::transport::EndpointDescriptor::Ipv4Tcp(
        bytes, static_cast<uint16_t>(options.port + lane));
    if (!created.ok()) {
        *error = "cannot create Mino endpoint: " + created.status().ToString();
        return false;
    }
    *endpoint = *created;
    return true;
}

class Transport {
public:
    virtual ~Transport() = default;
    virtual bool Initialize(uint64_t deadline_ns, std::string* error) = 0;
    virtual bool Send(uint16_t lane, std::span<const std::byte> body,
                      uint64_t deadline_ns, std::string* error) = 0;
    virtual bool Receive(uint64_t deadline_ns, Envelope* envelope,
                         std::string* error) = 0;
    virtual bool Echo(const Envelope& envelope, uint64_t deadline_ns,
                      std::string* error) = 0;
};

class MinoTransport final : public Transport {
public:
    explicit MinoTransport(const Options& options) : options_(options) {}

    ~MinoTransport() override {
        if (driver_ != nullptr) static_cast<void>(driver_->Shutdown());
    }

    bool Initialize(uint64_t deadline_ns, std::string* error) override {
        std::vector<std::byte> probe;
        if (!EncodeBody(options_, Message{}, &probe, error)) return false;
        const size_t body_bound = std::max(probe.size(), options_.payload_bytes);
        mino::transport::TcpDriverOptions tcp_options;
        tcp_options.max_frame_body_bytes = static_cast<uint32_t>(body_bound);
        tcp_options.max_total_send_buffer_bytes = 64u * 1024u * 1024u;
        tcp_options.max_connection_send_buffer_bytes = 16u * 1024u * 1024u;
        tcp_options.max_ready_receive_bytes = 64u * 1024u * 1024u;
        tcp_options.max_ready_receive_messages = 65'536;
        tcp_options.max_pending_accepts = 128;
        tcp_options.heartbeat_interval_ms = 1000;
        tcp_options.idle_timeout_ms = 60'000;
        tcp_options.partial_frame_timeout_ms = 10'000;
        tcp_options.io_poll_max_ms = 1;
        tcp_options.max_control_send_buffer_bytes = 16u * 1024u * 1024u;
        tcp_options.max_control_send_messages = 65'536;
        auto created = mino::transport::TcpDriver::Create(tcp_options);
        if (!created.ok()) {
            *error = "TcpDriver::Create failed: " + created.status().ToString();
            return false;
        }
        driver_ = std::move(*created);
        const mino::transport::DriverConfig config{
            .max_connections = static_cast<uint32_t>(options_.lane_count * 2),
            .max_listeners = options_.lane_count,
            .max_queued_sends = 65'536,
        };
        const mino::Status started = driver_->Start(config);
        if (!started.ok()) {
            *error = "TcpDriver::Start failed: " + started.ToString();
            return false;
        }
        if (options_.role == "server") {
            listeners_.reserve(options_.lane_count);
            for (uint16_t lane = 0; lane < options_.lane_count; ++lane) {
                mino::transport::EndpointDescriptor endpoint;
                if (!MakeMinoEndpoint(options_, lane, &endpoint, error)) {
                    return false;
                }
                auto listener = driver_->Listen(
                    {.local_endpoint = endpoint, .backlog = 4});
                if (!listener.ok()) {
                    *error = "TcpDriver::Listen failed: " +
                             listener.status().ToString();
                    return false;
                }
                listeners_.push_back(listener->id);
            }
            connections_.reserve(options_.lane_count);
            for (uint16_t lane = 0; lane < options_.lane_count; ++lane) {
                const uint32_t timeout = RemainingMs(deadline_ns, 60'000);
                if (timeout == 0) {
                    *error = "deadline expired while accepting Mino lanes";
                    return false;
                }
                auto accepted = driver_->Accept({
                    .listener_id = listeners_[lane], .timeout_ms = timeout});
                if (!accepted.ok()) {
                    *error = "TcpDriver::Accept failed: " +
                             accepted.status().ToString();
                    return false;
                }
                connection_lanes_.emplace(accepted->id, lane);
                connections_.push_back(accepted->id);
            }
        } else {
            connections_.reserve(options_.lane_count);
            for (uint16_t lane = 0; lane < options_.lane_count; ++lane) {
                mino::transport::EndpointDescriptor endpoint;
                if (!MakeMinoEndpoint(options_, lane, &endpoint, error)) {
                    return false;
                }
                const uint32_t timeout = RemainingMs(deadline_ns, 60'000);
                if (timeout == 0) {
                    *error = "deadline expired while connecting Mino lanes";
                    return false;
                }
                auto connected = driver_->Connect({
                    .remote_endpoint = endpoint,
                    .local_bind = std::nullopt,
                    .timeout_ms = timeout,
                });
                if (!connected.ok()) {
                    *error = "TcpDriver::Connect failed: " +
                             connected.status().ToString();
                    return false;
                }
                connection_lanes_.emplace(connected->id, lane);
                connections_.push_back(connected->id);
            }
        }
        return true;
    }

    bool Send(uint16_t lane, std::span<const std::byte> body,
              uint64_t deadline_ns, std::string* error) override {
        if (lane >= connections_.size()) {
            *error = "Mino send lane is out of range";
            return false;
        }
        while (NowNs() < deadline_ns) {
            auto sent = driver_->SendUntracked({
                .connection_id = connections_[lane],
                .payload = body,
                .traffic_class = mino::transport::UntrackedTrafficClass::kData,
            });
            if (sent.ok()) return true;
            if (sent.status().code() != mino::StatusCode::kWouldBlock) {
                *error = "TcpDriver::SendUntracked failed: " +
                         sent.status().ToString();
                return false;
            }
            std::this_thread::sleep_for(50us);
        }
        *error = "deadline expired while sending through TcpDriver";
        return false;
    }

    bool Receive(uint64_t deadline_ns, Envelope* envelope,
                 std::string* error) override {
        while (NowNs() < deadline_ns) {
            const uint32_t timeout = RemainingMs(deadline_ns);
            auto received = driver_->Poll({
                .max_messages = 1,
                .max_bytes = 32u * 1024u * 1024u,
                .timeout_ms = timeout,
                .connection_id = mino::transport::kInvalidConnectionId,
            });
            if (!received.ok()) {
                if (received.status().code() == mino::StatusCode::kTimeout ||
                    received.status().code() == mino::StatusCode::kWouldBlock) {
                    continue;
                }
                *error = "TcpDriver::Poll failed: " +
                         received.status().ToString();
                return false;
            }
            const auto lane =
                connection_lanes_.find(received->messages.front().connection_id);
            if (lane == connection_lanes_.end()) {
                *error = "TcpDriver returned an unknown connection";
                return false;
            }
            envelope->lane = lane->second;
            envelope->body = std::move(received->messages.front().payload);
            envelope->route.clear();
            return true;
        }
        *error = "deadline expired while polling TcpDriver";
        return false;
    }

    bool Echo(const Envelope& envelope, uint64_t deadline_ns,
              std::string* error) override {
        return Send(envelope.lane, envelope.body, deadline_ns, error);
    }

private:
    Options options_;
    std::unique_ptr<mino::transport::TcpDriver> driver_;
    std::vector<mino::transport::ConnectionId> listeners_;
    std::vector<mino::transport::ConnectionId> connections_;
    std::unordered_map<mino::transport::ConnectionId, uint16_t>
        connection_lanes_;
};

class ZmqTransport final : public Transport {
public:
    explicit ZmqTransport(const Options& options) : options_(options) {}

    ~ZmqTransport() override {
        for (void* socket : sockets_) {
            if (socket != nullptr) static_cast<void>(zmq_close(socket));
        }
        if (context_ != nullptr) static_cast<void>(zmq_ctx_term(context_));
    }

    bool Initialize(uint64_t /*deadline_ns*/, std::string* error) override {
        context_ = zmq_ctx_new();
        if (context_ == nullptr || zmq_ctx_set(context_, ZMQ_IO_THREADS, 1) != 0) {
            *error = ZmqError("cannot initialize ZeroMQ context");
            return false;
        }
        sockets_.reserve(options_.lane_count);
        for (uint16_t lane = 0; lane < options_.lane_count; ++lane) {
            const int type = options_.role == "server" ? ZMQ_ROUTER : ZMQ_DEALER;
            void* socket = zmq_socket(context_, type);
            if (socket == nullptr || !SetOptions(socket, error)) {
                if (socket != nullptr) static_cast<void>(zmq_close(socket));
                return false;
            }
            if (options_.role == "client") {
                const std::string identity =
                    "layer-comparison-" + std::to_string(lane);
                if (zmq_setsockopt(socket, ZMQ_ROUTING_ID, identity.data(),
                                   identity.size()) != 0) {
                    *error = ZmqError("cannot set ZeroMQ routing id");
                    static_cast<void>(zmq_close(socket));
                    return false;
                }
            }
            const std::string endpoint = ZmqEndpoint(options_, lane);
            const int result = options_.role == "server"
                                   ? zmq_bind(socket, endpoint.c_str())
                                   : zmq_connect(socket, endpoint.c_str());
            if (result != 0) {
                *error = ZmqError(options_.role == "server"
                                      ? "ZeroMQ bind failed"
                                      : "ZeroMQ connect failed");
                static_cast<void>(zmq_close(socket));
                return false;
            }
            sockets_.push_back(socket);
        }
        return true;
    }

    bool Send(uint16_t lane, std::span<const std::byte> body,
              uint64_t deadline_ns, std::string* error) override {
        if (lane >= sockets_.size()) {
            *error = "ZeroMQ send lane is out of range";
            return false;
        }
        return SendFrame(sockets_[lane], body, 0, deadline_ns, error);
    }

    bool Receive(uint64_t deadline_ns, Envelope* envelope,
                 std::string* error) override {
        size_t ready_lane = 0;
        if (!PollOne(deadline_ns, &ready_lane, error)) return false;
        envelope->lane = static_cast<uint16_t>(ready_lane);
        envelope->route.clear();
        if (options_.role == "server") {
            if (!ReceiveFrame(sockets_[ready_lane], &envelope->route, error)) {
                return false;
            }
            int more = 0;
            size_t more_size = sizeof(more);
            if (envelope->route.empty() ||
                zmq_getsockopt(sockets_[ready_lane], ZMQ_RCVMORE, &more,
                               &more_size) != 0 ||
                more == 0) {
                *error = "ZeroMQ ROUTER identity envelope is invalid";
                return false;
            }
        }
        if (!ReceiveFrame(sockets_[ready_lane], &envelope->body, error)) {
            return false;
        }
        int more = 0;
        size_t more_size = sizeof(more);
        if (zmq_getsockopt(sockets_[ready_lane], ZMQ_RCVMORE, &more,
                           &more_size) != 0 ||
            more != 0) {
            *error = "ZeroMQ body is not a single native message frame";
            return false;
        }
        return true;
    }

    bool Echo(const Envelope& envelope, uint64_t deadline_ns,
              std::string* error) override {
        if (envelope.lane >= sockets_.size() || envelope.route.empty()) {
            *error = "ZeroMQ echo envelope is invalid";
            return false;
        }
        if (!SendFrame(sockets_[envelope.lane], envelope.route, ZMQ_SNDMORE,
                       deadline_ns, error)) {
            return false;
        }
        return SendFrame(sockets_[envelope.lane], envelope.body, 0,
                         deadline_ns, error);
    }

private:
    static std::string ZmqError(std::string_view prefix) {
        return std::string(prefix) + ": " + zmq_strerror(errno);
    }

    static bool SetOptions(void* socket, std::string* error) {
        const int linger_ms = 1000;
        const int immediate = 1;
        const int hwm = 65'536;
        const int keepalive = 1;
        const int heartbeat_interval = 1000;
        const int heartbeat_timeout = 10'000;
        const int timeout_ms = 100;
        for (const auto& [option, value] : {
                 std::pair{ZMQ_LINGER, linger_ms},
                 std::pair{ZMQ_IMMEDIATE, immediate},
                 std::pair{ZMQ_SNDHWM, hwm},
                 std::pair{ZMQ_RCVHWM, hwm},
                 std::pair{ZMQ_TCP_KEEPALIVE, keepalive},
                 std::pair{ZMQ_HEARTBEAT_IVL, heartbeat_interval},
                 std::pair{ZMQ_HEARTBEAT_TIMEOUT, heartbeat_timeout},
                 std::pair{ZMQ_SNDTIMEO, timeout_ms},
                 std::pair{ZMQ_RCVTIMEO, timeout_ms},
             }) {
            if (zmq_setsockopt(socket, option, &value, sizeof(value)) != 0) {
                *error = ZmqError("zmq_setsockopt failed");
                return false;
            }
        }
        return true;
    }

    static bool SendFrame(void* socket, std::span<const std::byte> bytes,
                          int flags, uint64_t deadline_ns, std::string* error) {
        while (NowNs() < deadline_ns) {
            const int sent = zmq_send(socket, bytes.data(), bytes.size(), flags);
            if (sent == static_cast<int>(bytes.size())) return true;
            if (sent >= 0) {
                *error = "ZeroMQ performed a partial message send";
                return false;
            }
            if (errno != EAGAIN && errno != EINTR) {
                *error = ZmqError("zmq_send failed");
                return false;
            }
        }
        *error = "deadline expired while sending through ZeroMQ";
        return false;
    }

    static bool ReceiveFrame(void* socket, std::vector<std::byte>* bytes,
                             std::string* error) {
        zmq_msg_t message;
        if (zmq_msg_init(&message) != 0) {
            *error = ZmqError("zmq_msg_init failed");
            return false;
        }
        const int received = zmq_msg_recv(&message, socket, 0);
        if (received < 0) {
            *error = ZmqError("zmq_msg_recv failed");
            static_cast<void>(zmq_msg_close(&message));
            return false;
        }
        const size_t size = zmq_msg_size(&message);
        const auto* data = static_cast<const std::byte*>(zmq_msg_data(&message));
        bytes->assign(data, data + size);
        if (zmq_msg_close(&message) != 0) {
            *error = ZmqError("zmq_msg_close failed");
            return false;
        }
        return true;
    }

    bool PollOne(uint64_t deadline_ns, size_t* ready_lane,
                 std::string* error) {
        std::vector<zmq_pollitem_t> items(sockets_.size());
        for (size_t index = 0; index < sockets_.size(); ++index) {
            items[index] = zmq_pollitem_t{
                .socket = sockets_[index],
                .fd = 0,
                .events = ZMQ_POLLIN,
                .revents = 0,
            };
        }
        while (NowNs() < deadline_ns) {
            const long timeout = RemainingMs(deadline_ns);
            const int ready =
                zmq_poll(items.data(), static_cast<int>(items.size()), timeout);
            if (ready < 0) {
                if (errno == EINTR) continue;
                *error = ZmqError("zmq_poll failed");
                return false;
            }
            if (ready == 0) continue;
            for (size_t index = 0; index < items.size(); ++index) {
                if ((items[index].revents & ZMQ_POLLIN) != 0) {
                    *ready_lane = index;
                    return true;
                }
            }
        }
        *error = "deadline expired while polling ZeroMQ";
        return false;
    }

    Options options_;
    void* context_ = nullptr;
    std::vector<void*> sockets_;
};

std::unique_ptr<Transport> MakeTransport(const Options& options) {
    if (options.backend == "mino") {
        return std::make_unique<MinoTransport>(options);
    }
    return std::make_unique<ZmqTransport>(options);
}

bool SendMessage(const Options& options, Transport* transport, uint16_t lane,
                 Message message, uint64_t deadline_ns, uint64_t* origin_ns,
                 std::string* error) {
    // This is the common timing boundary: origin is written before either the
    // L1 body construction or the L2 WireFrame construction and Encode call.
    message.origin_ns = NowNs();
    if (origin_ns != nullptr) *origin_ns = message.origin_ns;
    std::vector<std::byte> body;
    if (!EncodeBody(options, message, &body, error)) return false;
    return transport->Send(lane, body, deadline_ns, error);
}

bool ReceiveMessage(const Options& options, Transport* transport,
                    uint64_t deadline_ns, Envelope* envelope, Message* message,
                    std::string* error) {
    if (!transport->Receive(deadline_ns, envelope, error)) return false;
    if (!DecodeBody(options, envelope->body, message, error)) return false;
    if (LaneFor(message->topic, options.lane_count) != envelope->lane) {
        *error = "message arrived on a lane other than topic % lane_count";
        return false;
    }
    return true;
}

bool SyncLanes(const Options& options, Transport* transport, uint32_t kind,
               uint32_t phase, uint64_t deadline_ns, std::string* error) {
    for (uint16_t lane = 0; lane < options.lane_count; ++lane) {
        if (!SendMessage(options, transport, lane,
                         Message{.kind = kind,
                                 .phase = phase,
                                 .topic = lane,
                                 .sequence = lane},
                         deadline_ns, nullptr, error)) {
            return false;
        }
    }
    std::vector<bool> seen(options.lane_count, false);
    for (uint16_t count = 0; count < options.lane_count; ++count) {
        Envelope envelope;
        Message message;
        if (!ReceiveMessage(options, transport, deadline_ns, &envelope, &message,
                            error)) {
            return false;
        }
        if (message.kind != kind || message.phase != phase ||
            message.topic >= options.lane_count ||
            message.sequence != message.topic || seen[message.topic]) {
            *error = "phase synchronization echo is invalid";
            return false;
        }
        seen[message.topic] = true;
    }
    return true;
}

bool SendFinalAcks(const Options& options, Transport* transport,
                   uint64_t deadline_ns, std::string* error) {
    for (uint16_t lane = 0; lane < options.lane_count; ++lane) {
        if (!SendMessage(options, transport, lane,
                         Message{.kind = kFinalAck,
                                 .phase = 0,
                                 .topic = lane,
                                 .sequence = lane},
                         deadline_ns, nullptr, error)) {
            return false;
        }
    }
    // TcpDriver admission and zmq_send are local. A short grace period plus
    // ZeroMQ's bounded linger lets the final one-way acknowledgement drain.
    std::this_thread::sleep_for(100ms);
    return true;
}

bool RunTraffic(const Options& options, Transport* transport,
                std::string_view mode, uint32_t phase, uint32_t topic_count,
                uint32_t messages_per_topic, bool record_samples,
                uint64_t deadline_ns, Record* record, std::string* error) {
    const uint64_t total =
        static_cast<uint64_t>(topic_count) * messages_per_topic;
    if (total == 0) return true;
    std::vector<uint32_t> next_sequence(topic_count, 1);
    std::vector<uint32_t> in_flight(topic_count, 0);
    std::vector<std::vector<bool>> seen(
        topic_count, std::vector<bool>(messages_per_topic + 1, false));
    std::vector<uint64_t> samples;
    if (record_samples) samples.reserve(total);
    uint64_t sent_count = 0;
    uint64_t completed_count = 0;
    uint32_t next_serial_topic = 0;

    const auto send_one = [&](uint32_t topic) {
        const uint32_t sequence = next_sequence[topic]++;
        if (!SendMessage(options, transport, LaneFor(topic, options.lane_count),
                         Message{.kind = kData,
                                 .phase = phase,
                                 .topic = topic,
                                 .sequence = sequence},
                         deadline_ns, nullptr, error)) {
            return false;
        }
        ++in_flight[topic];
        ++sent_count;
        return true;
    };
    const auto fill_topic = [&](uint32_t topic) {
        while (next_sequence[topic] <= messages_per_topic &&
               in_flight[topic] < options.outstanding) {
            if (!send_one(topic)) return false;
        }
        return true;
    };
    const auto send_next_serial = [&]() {
        for (uint32_t checked = 0; checked < topic_count; ++checked) {
            const uint32_t topic = (next_serial_topic + checked) % topic_count;
            if (next_sequence[topic] <= messages_per_topic) {
                next_serial_topic = (topic + 1) % topic_count;
                return send_one(topic);
            }
        }
        return true;
    };

    const uint64_t started_ns = NowNs();
    if (mode == "serial") {
        if (!send_next_serial()) return false;
    } else {
        for (uint32_t topic = 0; topic < topic_count; ++topic) {
            if (!fill_topic(topic)) return false;
        }
    }
    while (completed_count < total) {
        Envelope envelope;
        Message message;
        if (!ReceiveMessage(options, transport, deadline_ns, &envelope, &message,
                            error)) {
            return false;
        }
        const uint64_t completed_ns = NowNs();
        if (message.kind != kData || message.phase != phase ||
            message.topic >= topic_count || message.sequence == 0 ||
            message.sequence > messages_per_topic ||
            seen[message.topic][message.sequence] ||
            in_flight[message.topic] == 0 || message.origin_ns == 0 ||
            completed_ns < message.origin_ns) {
            *error = "data echo correlation is invalid";
            return false;
        }
        seen[message.topic][message.sequence] = true;
        --in_flight[message.topic];
        ++completed_count;
        if (record_samples) samples.push_back(completed_ns - message.origin_ns);
        if (mode == "serial") {
            if (sent_count < total && !send_next_serial()) return false;
        } else if (!fill_topic(message.topic)) {
            return false;
        }
    }
    const uint64_t elapsed_ns = NowNs() - started_ns;
    if (!record_samples) return true;
    for (uint64_t& sample : samples) sample = ToMicroseconds(sample);
    std::sort(samples.begin(), samples.end());
    const auto percentile = [&](uint32_t value) {
        const size_t rank = (samples.size() * value + 99) / 100;
        return samples[rank - 1];
    };
    *record = Record{
        .mode = std::string(mode),
        .topic_count = topic_count,
        .sample_count = total,
        .p50_rtt_us = percentile(50),
        .p95_rtt_us = percentile(95),
        .p99_rtt_us = percentile(99),
        .max_rtt_us = samples.back(),
        .elapsed_us = ToMicroseconds(elapsed_ns),
        .messages_per_second =
            elapsed_ns == 0
                ? 0.0
                : static_cast<double>(total) * 1'000'000'000.0 /
                      static_cast<double>(elapsed_ns),
    };
    return true;
}

bool RunOneWayTraffic(const Options& options, Transport* transport,
                      uint32_t phase, uint32_t topic_count,
                      uint32_t messages_per_topic, uint32_t completion_kind,
                      bool record_result, uint64_t deadline_ns, Record* record,
                      std::string* error) {
    const uint64_t total =
        static_cast<uint64_t>(topic_count) * messages_per_topic;
    const uint64_t started_ns = NowNs();
    for (uint32_t sequence = 1; sequence <= messages_per_topic; ++sequence) {
        for (uint32_t topic = 0; topic < topic_count; ++topic) {
            if (!SendMessage(options, transport,
                             LaneFor(topic, options.lane_count),
                             Message{.kind = kData,
                                     .phase = phase,
                                     .topic = topic,
                                     .sequence = sequence},
                             deadline_ns, nullptr, error)) {
                return false;
            }
        }
    }
    if (!SyncLanes(options, transport, completion_kind, phase, deadline_ns,
                   error)) {
        return false;
    }
    if (!record_result) return true;
    const uint64_t elapsed_ns = NowNs() - started_ns;
    *record = Record{
        .mode = "one_way",
        .topic_count = topic_count,
        .sample_count = total,
        .elapsed_us = ToMicroseconds(elapsed_ns),
        .messages_per_second =
            elapsed_ns == 0
                ? 0.0
                : static_cast<double>(total) * 1'000'000'000.0 /
                      static_cast<double>(elapsed_ns),
    };
    return true;
}

enum class OneWayStage : uint8_t { kIdle, kWarmup, kMeasured, kComplete };

struct OneWayServerState {
    uint32_t phase = 0;
    OneWayStage stage = OneWayStage::kIdle;
    std::vector<uint32_t> next_sequence;
    std::vector<bool> phase_started;
    std::vector<bool> warmup_done;
    std::vector<bool> phase_done;
};

bool ValidateOneWayMessage(const Options& options, const Envelope& envelope,
                           const Message& message, OneWayServerState* state,
                           std::string* error) {
    const auto valid_barrier = [&] {
        return message.topic == envelope.lane &&
               message.sequence == message.topic;
    };
    const auto reset_phase = [&] {
        state->phase = message.phase;
        state->stage = OneWayStage::kWarmup;
        state->next_sequence.assign(options.topic_count, 1);
        state->phase_started.assign(options.lane_count, false);
        state->warmup_done.assign(options.lane_count, false);
        state->phase_done.assign(options.lane_count, false);
    };
    const auto all_lanes = [](const std::vector<bool>& lanes) {
        return std::all_of(lanes.begin(), lanes.end(), [](bool seen) {
            return seen;
        });
    };
    const auto validate_lane_complete = [&](uint32_t expected_next) {
        for (uint32_t topic = envelope.lane; topic < options.topic_count;
             topic += options.lane_count) {
            if (state->next_sequence[topic] != expected_next) return false;
        }
        return true;
    };

    if (message.kind == kPhaseStart) {
        if (!valid_barrier()) {
            *error = "one-way phase-start barrier is invalid";
            return false;
        }
        if (message.phase != state->phase) {
            if (state->stage != OneWayStage::kIdle &&
                state->stage != OneWayStage::kComplete) {
                *error = "one-way phase started before the prior phase completed";
                return false;
            }
            reset_phase();
        }
        if (state->stage != OneWayStage::kWarmup ||
            state->phase_started[envelope.lane]) {
            *error = "one-way phase-start barrier is duplicated or out of order";
            return false;
        }
        state->phase_started[envelope.lane] = true;
        return true;
    }

    if (message.kind == kData) {
        if ((state->stage != OneWayStage::kWarmup &&
             state->stage != OneWayStage::kMeasured) ||
            message.phase != state->phase ||
            !all_lanes(state->phase_started) ||
            message.topic >= options.topic_count || message.sequence == 0 ||
            message.sequence != state->next_sequence[message.topic]) {
            *error = "one-way DATA sequence or phase is invalid";
            return false;
        }
        const uint32_t limit =
            state->stage == OneWayStage::kWarmup
                ? options.warmup_messages_per_topic
                : options.messages_per_topic;
        if (message.sequence > limit) {
            *error = "one-way DATA exceeds the configured phase count";
            return false;
        }
        ++state->next_sequence[message.topic];
        return true;
    }

    if (message.kind == kWarmupDone) {
        if (!valid_barrier() || message.phase != state->phase ||
            state->stage != OneWayStage::kWarmup ||
            state->warmup_done[envelope.lane] ||
            !validate_lane_complete(
                options.warmup_messages_per_topic + 1)) {
            *error = "one-way warmup completion is invalid or premature";
            return false;
        }
        state->warmup_done[envelope.lane] = true;
        if (all_lanes(state->warmup_done)) {
            state->next_sequence.assign(options.topic_count, 1);
            state->stage = OneWayStage::kMeasured;
        }
        return true;
    }

    if (message.kind == kPhaseDone) {
        if (!valid_barrier() || message.phase != state->phase ||
            state->stage != OneWayStage::kMeasured ||
            state->phase_done[envelope.lane] ||
            !validate_lane_complete(options.messages_per_topic + 1)) {
            *error = "one-way measured completion is invalid or premature";
            return false;
        }
        state->phase_done[envelope.lane] = true;
        if (all_lanes(state->phase_done)) {
            state->stage = OneWayStage::kComplete;
        }
        return true;
    }

    return true;
}

std::string JsonEscape(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const unsigned char character : value) {
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
                if (character < 0x20) {
                    constexpr char kHex[] = "0123456789abcdef";
                    escaped += "\\u00";
                    escaped += kHex[(character >> 4) & 0x0f];
                    escaped += kHex[character & 0x0f];
                } else {
                    escaped += static_cast<char>(character);
                }
        }
    }
    return escaped;
}

std::string Scope(const Options& options) {
    const std::string_view direction =
        options.mode == "one_way" ? "one-way delivery" : "echo";
    if (options.layer == "l1") {
        return options.backend == "mino"
                   ? "production TcpDriver raw length-framed body " +
                         std::string(direction)
                   : "native ZeroMQ DEALER/ROUTER single-body " +
                         std::string(direction);
    }
    return options.backend == "mino"
               ? "production WireFrameCodec body over production TcpDriver " +
                     std::string(direction)
               : "production WireFrameCodec body over native ZeroMQ DEALER/ROUTER " +
                     std::string(direction);
}

size_t EncodedBodyBytes(const Options& options) {
    std::vector<std::byte> body;
    std::string ignored;
    if (!EncodeBody(options, Message{}, &body, &ignored)) return 0;
    return body.size();
}

bool WriteResult(const Options& options, std::string_view outcome,
                 std::string_view error, const std::vector<Record>& records,
                 uint64_t messages_received, uint64_t messages_echoed = 0) {
    std::ofstream output(options.output, std::ios::trunc);
    if (!output) return false;
    output << std::fixed << std::setprecision(3)
           << "{\n"
           << "  \"schema_version\": 1,\n"
           << "  \"benchmark\": \"mino.transport.layer_comparison\",\n"
           << "  \"backend\": \"" << JsonEscape(options.backend) << "\",\n"
           << "  \"layer\": \"" << JsonEscape(options.layer) << "\",\n"
           << "  \"scope\": \"" << JsonEscape(Scope(options)) << "\",\n"
           << "  \"role\": \"" << JsonEscape(options.role) << "\",\n"
           << "  \"outcome\": \"" << JsonEscape(outcome) << "\",\n"
           << "  \"payload_bytes\": " << options.payload_bytes << ",\n"
           << "  \"encoded_body_bytes\": " << EncodedBodyBytes(options)
           << ",\n"
           << "  \"lane_count\": " << options.lane_count << ",\n"
           << "  \"io_threads\": 1,\n"
           << "  \"messages_per_topic\": " << options.messages_per_topic
           << ",\n"
           << "  \"warmup\": " << options.warmup_messages_per_topic << ",\n"
           << "  \"warmup_messages_per_topic\": "
           << options.warmup_messages_per_topic << ",\n"
           << "  \"outstanding\": " << options.outstanding << ",\n"
           << "  \"messages_received\": " << messages_received << ",\n"
           << "  \"messages_echoed\": " << messages_echoed << ",\n"
           << "  \"records\": [\n";
    for (size_t index = 0; index < records.size(); ++index) {
        const Record& record = records[index];
        output << "    {\"mode\": \"" << JsonEscape(record.mode)
               << "\", \"topic_count\": " << record.topic_count
               << ", \"sample_count\": " << record.sample_count
               << ", \"p50_rtt_us\": " << record.p50_rtt_us
               << ", \"p95_rtt_us\": " << record.p95_rtt_us
               << ", \"p99_rtt_us\": " << record.p99_rtt_us
               << ", \"max_rtt_us\": " << record.max_rtt_us
               << ", \"elapsed_us\": " << record.elapsed_us
               << ", \"messages_per_second\": "
               << record.messages_per_second << "}"
               << (index + 1 == records.size() ? "\n" : ",\n");
    }
    output << "  ],\n"
           << "  \"error\": \"" << JsonEscape(error) << "\"\n"
           << "}\n";
    return static_cast<bool>(output);
}

int RunClient(const Options& options) {
    std::string error;
    std::vector<Record> records;
    const uint64_t deadline_ns = DeadlineNs(options);
    std::unique_ptr<Transport> transport = MakeTransport(options);
    if (!transport->Initialize(deadline_ns, &error)) {
        WriteResult(options, "failed", error, records, 0);
        std::cerr << error << '\n';
        return 1;
    }
    if (!SyncLanes(options, transport.get(), kHello, 0, deadline_ns, &error)) {
        WriteResult(options, "failed", error, records, 0);
        std::cerr << error << '\n';
        return 1;
    }
    records.reserve(kDefaultTopicCounts.size() * 2);
    uint32_t phase = 1;
    if (options.mode == "one_way") {
        if (!SyncLanes(options, transport.get(), kPhaseStart, phase,
                       deadline_ns, &error)) {
            WriteResult(options, "failed", error, records, 0);
            std::cerr << error << '\n';
            return 1;
        }
        Record ignored;
        if (!RunOneWayTraffic(
                options, transport.get(), phase, options.topic_count,
                options.warmup_messages_per_topic, kWarmupDone, false,
                deadline_ns, &ignored, &error)) {
            WriteResult(options, "failed", error, records, 0);
            std::cerr << error << '\n';
            return 1;
        }
        Record record;
        if (!RunOneWayTraffic(options, transport.get(), phase,
                              options.topic_count,
                              options.messages_per_topic, kPhaseDone, true,
                              deadline_ns, &record, &error)) {
            WriteResult(options, "failed", error, records, 0);
            std::cerr << error << '\n';
            return 1;
        }
        records.push_back(std::move(record));
    } else {
        const auto run_echo_phase = [&](std::string_view mode,
                                        uint32_t topic_count) {
            if (!SyncLanes(options, transport.get(), kPhaseStart, phase,
                           deadline_ns, &error)) {
                return false;
            }
            Record ignored;
            if (!RunTraffic(options, transport.get(), mode, phase, topic_count,
                            options.warmup_messages_per_topic, false,
                            deadline_ns, &ignored, &error) ||
                !SyncLanes(options, transport.get(), kWarmupDone, phase,
                           deadline_ns, &error)) {
                return false;
            }
            Record record;
            if (!RunTraffic(options, transport.get(), mode, phase, topic_count,
                            options.messages_per_topic, true, deadline_ns,
                            &record, &error) ||
                !SyncLanes(options, transport.get(), kPhaseDone, phase,
                           deadline_ns, &error)) {
                return false;
            }
            records.push_back(std::move(record));
            ++phase;
            return true;
        };
        for (std::string_view mode : {
                 std::string_view("serial"),
                 std::string_view("per_topic_concurrent")}) {
            if (options.mode != "all" && options.mode != mode) continue;
            if (options.topic_count != 0) {
                if (!run_echo_phase(mode, options.topic_count)) {
                    WriteResult(options, "failed", error, records, 0);
                    std::cerr << error << '\n';
                    return 1;
                }
                continue;
            }
            for (uint32_t topic_count : kDefaultTopicCounts) {
                if (!run_echo_phase(mode, topic_count)) {
                    WriteResult(options, "failed", error, records, 0);
                    std::cerr << error << '\n';
                    return 1;
                }
            }
        }
    }
    if (!SyncLanes(options, transport.get(), kStop, 0, deadline_ns, &error) ||
        !SyncLanes(options, transport.get(), kStopDone, 0, deadline_ns, &error) ||
        !SendFinalAcks(options, transport.get(), deadline_ns, &error)) {
        WriteResult(options, "failed", error, records, 0);
        std::cerr << error << '\n';
        return 1;
    }
    if (!WriteResult(options, "passed", "", records, 0)) {
        std::cerr << "cannot write benchmark result to " << options.output
                  << '\n';
        return 1;
    }
    return 0;
}

int RunServer(const Options& options) {
    std::string error;
    uint64_t messages_received = 0;
    uint64_t messages_echoed = 0;
    OneWayServerState one_way_state;
    const uint64_t deadline_ns = DeadlineNs(options);
    std::unique_ptr<Transport> transport = MakeTransport(options);
    if (!transport->Initialize(deadline_ns, &error)) {
        WriteResult(options, "failed", error, {}, messages_received,
                    messages_echoed);
        std::cerr << error << '\n';
        return 1;
    }
    std::vector<bool> final_acks(options.lane_count, false);
    uint16_t final_ack_count = 0;
    while (final_ack_count < options.lane_count) {
        Envelope envelope;
        Message message;
        if (!ReceiveMessage(options, transport.get(), deadline_ns, &envelope,
                            &message, &error)) {
            break;
        }
        if (message.kind < kHello || message.kind > kFinalAck) {
            error = "server received an unknown benchmark message kind";
            break;
        }
        if (message.kind == kFinalAck) {
            if ((options.mode == "one_way" &&
                 one_way_state.stage != OneWayStage::kComplete) ||
                message.topic >= options.lane_count ||
                message.sequence != message.topic ||
                final_acks[message.topic]) {
                error = "server received an invalid final acknowledgement";
                break;
            }
            final_acks[message.topic] = true;
            ++final_ack_count;
            continue;
        }
        bool should_echo = true;
        if (options.mode == "one_way") {
            if (!ValidateOneWayMessage(options, envelope, message,
                                       &one_way_state, &error)) {
                break;
            }
            should_echo = message.kind != kData;
        }
        if (message.kind == kData) ++messages_received;
        // DecodeBody above performs full deterministic validation (and L2 CRC)
        // before echo or one-way sequence validation.
        if (should_echo) {
            if (!transport->Echo(envelope, deadline_ns, &error)) break;
            if (message.kind == kData) ++messages_echoed;
        }
    }
    const bool passed = error.empty() && final_ack_count == options.lane_count;
    if (!WriteResult(options, passed ? "passed" : "failed", error, {},
                     messages_received, messages_echoed)) {
        std::cerr << "cannot write benchmark result to " << options.output
                  << '\n';
        return 1;
    }
    if (!passed) std::cerr << error << '\n';
    return passed ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        static_cast<void>(WriteResult(options, "failed", error, {}, 0));
        std::cerr << error << '\n';
        return 2;
    }
    return options.role == "server" ? RunServer(options) : RunClient(options);
}
