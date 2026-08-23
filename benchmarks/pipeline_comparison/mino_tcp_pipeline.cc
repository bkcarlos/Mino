// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/dynamic_value.h"
#include "mino/schema/wire.h"
#include "mino/transport/tcp_driver.h"
#include "mino/transport/transport_driver.h"

namespace mino::benchmarks::pipeline {
namespace {

using bridge::FlagValue;
using bridge::FrameFlag;
using bridge::FrameType;
using bridge::WireFrame;
using bridge::WireFrameCodec;
using bridge::WireFrameLimits;
using schema::DynamicMessage;
using schema::DynamicValue;
using schema::PreparedCanonicalWireCodec;
using schema::SchemaDescriptor;
using transport::ConnectionId;
using transport::EndpointDescriptor;
using transport::TcpDriver;
using transport::TcpDriverOptions;

constexpr std::string_view kBackend = "mino-tcp-canonical";
constexpr uint16_t kDefaultPortBase = 24'000;
constexpr uint16_t kMaximumPortBase = 65'531;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr uint64_t kMaximumInitialLatencyReserve = 1'000'000;
constexpr uint32_t kMaximumFrameBodyBytes = 2u * 1024u * 1024u;
constexpr size_t kMaximumArtifactBytes = 16u * 1024u * 1024u;
constexpr std::string_view kSchemaName =
    "mino.benchmarks.pipeline.AutonomyPipelineFrame";

struct BackendOptions {
    std::string listen_address = "127.0.0.1";
    std::string peer_address = "127.0.0.1";
    uint16_t port_base = kDefaultPortBase;
    std::filesystem::path descriptor;
    bool independent_host_clocks = false;
};

struct RunStatistics {
    uint64_t measured_completed = 0;
    uint64_t encoded_bytes_total = 0;
    uint64_t duplicate = 0;
    uint64_t out_of_order = 0;
    uint64_t corrupt = 0;
    uint64_t first_measured_origin_ns = 0;
    uint64_t first_measured_completion_ns = 0;
    uint64_t last_measured_completion_ns = 0;
    std::vector<uint64_t> latencies_ns;
};

uint64_t AbsoluteDeadline(const CommonOptions& options) {
    const uint64_t now = NowNs();
    const uint64_t duration = options.deadline_seconds * kNanosecondsPerSecond;
    if (duration > std::numeric_limits<uint64_t>::max() - now) {
        return std::numeric_limits<uint64_t>::max();
    }
    return now + duration;
}

uint32_t RemainingMs(uint64_t deadline_ns, uint32_t maximum = 60'000) {
    const uint64_t now = NowNs();
    if (now >= deadline_ns) return 0;
    const uint64_t remaining_ns = deadline_ns - now;
    const uint64_t rounded = (remaining_ns + 999'999u) / 1'000'000u;
    return static_cast<uint32_t>(
        std::min<uint64_t>(std::max<uint64_t>(rounded, 1), maximum));
}

std::optional<size_t> InputEdge(Role role) {
    switch (role) {
        case Role::kPerception: return std::nullopt;
        case Role::kPrediction: return 0;
        case Role::kPlanning: return 1;
        case Role::kControl: return 2;
        case Role::kGuardian: return 3;
        case Role::kCanbus: return 4;
    }
    throw std::invalid_argument("invalid pipeline role");
}

std::optional<size_t> OutputEdge(Role role) {
    switch (role) {
        case Role::kPerception: return 0;
        case Role::kPrediction: return 1;
        case Role::kPlanning: return 2;
        case Role::kControl: return 3;
        case Role::kGuardian: return 4;
        case Role::kCanbus: return std::nullopt;
    }
    throw std::invalid_argument("invalid pipeline role");
}

uint64_t StableRunHash(std::string_view value) {
    constexpr uint64_t kOffset = 14'695'981'039'346'656'037ull;
    constexpr uint64_t kPrime = 1'099'511'628'211ull;
    uint64_t hash = kOffset;
    for (char byte : value) {
        hash ^= static_cast<uint8_t>(byte);
        hash *= kPrime;
    }
    return hash == 0 ? 1 : hash;
}

void ThrowStatus(std::string_view operation, const Status& status) {
    throw std::runtime_error(std::string(operation) + ": " + status.ToString());
}

template <typename T>
T TakeOrThrow(std::string_view operation, Result<T>&& result) {
    if (!result.ok()) ThrowStatus(operation, result.status());
    return std::move(result).value();
}

uint64_t ParseUnsigned(std::string_view value, std::string_view option) {
    uint64_t parsed = 0;
    const auto conversion =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (value.empty() || conversion.ec != std::errc{} ||
        conversion.ptr != value.data() + value.size()) {
        throw std::runtime_error(std::string(option) +
                                 " requires an unsigned integer");
    }
    return parsed;
}

std::optional<std::string_view> OptionValue(int* index, int argc, char** argv,
                                            std::string_view option) {
    const std::string_view argument(argv[*index]);
    if (argument == option) {
        if (*index + 1 >= argc || argv[*index + 1] == nullptr) {
            throw std::runtime_error(std::string(option) + " requires a value");
        }
        return std::string_view(argv[++(*index)]);
    }
    if (argument.size() > option.size() && argument.starts_with(option) &&
        argument[option.size()] == '=') {
        const std::string_view value = argument.substr(option.size() + 1);
        if (value.empty()) {
            throw std::runtime_error(std::string(option) +
                                     " requires a non-empty value");
        }
        return value;
    }
    return std::nullopt;
}

void ValidateIpv4(std::string_view value, std::string_view option) {
    in_addr address{};
    const std::string text(value);
    if (inet_pton(AF_INET, text.c_str(), &address) != 1) {
        throw std::runtime_error(std::string(option) +
                                 " requires a numeric IPv4 address");
    }
}

BackendOptions ParseBackendOptions(int argc, char** argv) {
    BackendOptions options;
    bool listen_seen = false;
    bool peer_seen = false;
    bool port_seen = false;
    bool descriptor_seen = false;
    bool clock_seen = false;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            throw std::invalid_argument("argv contains a null argument");
        }
        if (const auto value =
                OptionValue(&index, argc, argv, "--listen-address")) {
            if (listen_seen) {
                throw std::runtime_error(
                    "--listen-address may be specified only once");
            }
            listen_seen = true;
            ValidateIpv4(*value, "--listen-address");
            options.listen_address = std::string(*value);
            continue;
        }
        if (const auto value =
                OptionValue(&index, argc, argv, "--peer-address")) {
            if (peer_seen) {
                throw std::runtime_error(
                    "--peer-address may be specified only once");
            }
            peer_seen = true;
            ValidateIpv4(*value, "--peer-address");
            options.peer_address = std::string(*value);
            continue;
        }
        if (const auto value = OptionValue(&index, argc, argv, "--port-base")) {
            if (port_seen) {
                throw std::runtime_error("--port-base may be specified only once");
            }
            port_seen = true;
            const uint64_t parsed = ParseUnsigned(*value, "--port-base");
            if (parsed == 0 || parsed > kMaximumPortBase) {
                throw std::runtime_error("--port-base must be in [1, 65531]");
            }
            options.port_base = static_cast<uint16_t>(parsed);
            continue;
        }
        if (const auto value = OptionValue(
                &index, argc, argv, "--schema-descriptor")) {
            if (descriptor_seen) {
                throw std::runtime_error(
                    "--schema-descriptor may be specified only once");
            }
            descriptor_seen = true;
            options.descriptor = std::filesystem::path(*value);
            continue;
        }
        if (const auto value = OptionValue(&index, argc, argv, "--clock-mode")) {
            if (clock_seen) {
                throw std::runtime_error("--clock-mode may be specified only once");
            }
            clock_seen = true;
            if (*value == "same-host") {
                options.independent_host_clocks = false;
            } else if (*value == "independent-hosts") {
                options.independent_host_clocks = true;
            } else {
                throw std::runtime_error(
                    "--clock-mode must be same-host or independent-hosts");
            }
        }
    }
    if (!descriptor_seen) {
        throw std::runtime_error("--schema-descriptor is required");
    }
    return options;
}

EndpointDescriptor MakeEndpoint(std::string_view address, uint16_t port) {
    in_addr parsed{};
    const std::string text(address);
    if (inet_pton(AF_INET, text.c_str(), &parsed) != 1) {
        throw std::runtime_error("invalid numeric IPv4 endpoint");
    }
    const auto bytes = std::as_bytes(std::span(&parsed, size_t{1}));
    return TakeOrThrow("EndpointDescriptor::Ipv4Tcp",
                       EndpointDescriptor::Ipv4Tcp(bytes, port));
}

std::string ReadArtifact(const std::filesystem::path& path) {
    std::ifstream stream(path, std::ios::binary);
    if (!stream.good()) {
        throw std::runtime_error("cannot open schema descriptor: " +
                                 path.string());
    }
    stream.seekg(0, std::ios::end);
    const std::streamoff length = stream.tellg();
    if (length <= 0 ||
        static_cast<uint64_t>(length) > kMaximumArtifactBytes) {
        throw std::runtime_error("schema descriptor size is invalid");
    }
    stream.seekg(0, std::ios::beg);
    std::string bytes(static_cast<size_t>(length), '\0');
    stream.read(bytes.data(), length);
    if (!stream || stream.gcount() != length) {
        throw std::runtime_error("cannot read complete schema descriptor");
    }
    return bytes;
}

class PipelineSchema final {
  public:
    explicit PipelineSchema(const std::filesystem::path& artifact_path) {
        const std::string bytes = ReadArtifact(artifact_path);
        auto artifact = schema::codegen::DecodeAndValidate(bytes);
        if (!artifact.ok()) {
            ThrowStatus("decode schema descriptor", artifact.status());
        }
        if (artifact->types.size() != 1 ||
            artifact->types.front().descriptor == nullptr ||
            artifact->types.front().descriptor->aggregate().full_name() !=
                kSchemaName) {
            throw std::runtime_error(
                "schema descriptor does not contain the autonomy pipeline type");
        }
        descriptor_ = artifact->types.front().descriptor;
        auto prepared = PreparedCanonicalWireCodec::Create(descriptor_);
        if (!prepared.ok()) {
            ThrowStatus("prepare canonical wire codec", prepared.status());
        }
        prepared_codec_.emplace(std::move(*prepared));
    }

    const SchemaDescriptor& descriptor() const noexcept { return *descriptor_; }

    std::vector<std::byte> Encode(const SemanticFrame& frame) const {
        DynamicMessage message;
        Set(message, 1, DynamicValue::Unsigned(frame.sample_id));
        Set(message, 2, DynamicValue::Unsigned(frame.origin_timestamp_ns));
        Set(message, 3, DynamicValue::Unsigned(frame.perception_timestamp_ns));
        Set(message, 4, DynamicValue::Unsigned(frame.prediction_timestamp_ns));
        Set(message, 5, DynamicValue::Unsigned(frame.planning_timestamp_ns));
        Set(message, 6, DynamicValue::Unsigned(frame.control_timestamp_ns));
        Set(message, 7, DynamicValue::Unsigned(frame.guardian_timestamp_ns));
        Set(message, 8, DynamicValue::Unsigned(frame.completed_stage_mask));
        Set(message, 9, DynamicValue::Unsigned(frame.profile));
        Set(message, 10, DynamicValue::Unsigned(frame.object_count));
        Set(message, 11,
            DynamicValue::Unsigned(frame.trajectory_point_count));
        Set(message, 12, DynamicValue::Float64Bits(
                             std::bit_cast<uint64_t>(frame.ego_speed_mps)));
        Set(message, 13, DynamicValue::Float64Bits(
                             std::bit_cast<uint64_t>(frame.steering_angle_rad)));
        Set(message, 14, DynamicValue::Float64Bits(
                             std::bit_cast<uint64_t>(frame.acceleration_mps2)));
        Set(message, 15, DynamicValue::Float64Bits(
                             std::bit_cast<uint64_t>(frame.brake_percentage)));
        Set(message, 16, DynamicValue::Boolean(frame.emergency_stop));
        Set(message, 17, DynamicValue::Unsigned(frame.payload_checksum));
        const auto payload = std::as_bytes(
            std::span(frame.payload.data(), frame.payload.size()));
        auto bytes = DynamicValue::Bytes(payload);
        if (!bytes.ok()) ThrowStatus("create dynamic payload", bytes.status());
        Set(message, 18, std::move(*bytes));
        return TakeOrThrow("CanonicalWireCodec::Encode",
                           prepared_codec_->Encode(message));
    }

    void Decode(std::span<const std::byte> bytes, SemanticFrame* frame) const {
        if (frame == nullptr) {
            throw std::invalid_argument("semantic decode destination is null");
        }
        DynamicMessage message = TakeOrThrow("CanonicalWireCodec::Decode",
                                             prepared_codec_->Decode(bytes));
        if (!message.unknown_fields().fields().empty()) {
            throw std::runtime_error(
                "canonical pipeline message contains unknown fields");
        }
        frame->sample_id = Unsigned(message, 1);
        frame->origin_timestamp_ns = Unsigned(message, 2);
        frame->perception_timestamp_ns = Unsigned(message, 3);
        frame->prediction_timestamp_ns = Unsigned(message, 4);
        frame->planning_timestamp_ns = Unsigned(message, 5);
        frame->control_timestamp_ns = Unsigned(message, 6);
        frame->guardian_timestamp_ns = Unsigned(message, 7);
        frame->completed_stage_mask = NarrowU32(Unsigned(message, 8), 8);
        frame->profile = NarrowU32(Unsigned(message, 9), 9);
        frame->object_count = NarrowU32(Unsigned(message, 10), 10);
        frame->trajectory_point_count =
            NarrowU32(Unsigned(message, 11), 11);
        frame->ego_speed_mps = Float64(message, 12);
        frame->steering_angle_rad = Float64(message, 13);
        frame->acceleration_mps2 = Float64(message, 14);
        frame->brake_percentage = Float64(message, 15);
        frame->emergency_stop = Boolean(message, 16);
        frame->payload_checksum = Unsigned(message, 17);
        const DynamicValue& payload = Field(message, 18);
        if (payload.bytes() == nullptr ||
            payload.bytes()->value.size() > kLargePayloadBytes) {
            throw std::runtime_error(
                "canonical payload has the wrong dynamic type or size");
        }
        const auto& payload_bytes = payload.bytes()->value;
        frame->payload.resize(payload_bytes.size());
        if (!payload_bytes.empty()) {
            std::memcpy(frame->payload.data(), payload_bytes.data(),
                        payload_bytes.size());
        }
    }

  private:
    static void Set(DynamicMessage& message, uint32_t id,
                    DynamicValue value) {
        const Status status = message.SetField(id, std::move(value));
        if (!status.ok()) ThrowStatus("set dynamic field", status);
    }

    static const DynamicValue& Field(const DynamicMessage& message,
                                     uint32_t id) {
        const DynamicValue* value = message.FindField(id);
        if (value == nullptr) {
            throw std::runtime_error("canonical message is missing field " +
                                     std::to_string(id));
        }
        return *value;
    }

    static uint64_t Unsigned(const DynamicMessage& message, uint32_t id) {
        const auto* value = Field(message, id).unsigned_integer();
        if (value == nullptr) {
            throw std::runtime_error("canonical unsigned field has wrong type");
        }
        return value->value;
    }

    static uint32_t NarrowU32(uint64_t value, uint32_t id) {
        if (value > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("canonical uint32 field overflows: " +
                                     std::to_string(id));
        }
        return static_cast<uint32_t>(value);
    }

    static double Float64(const DynamicMessage& message, uint32_t id) {
        const auto* value = Field(message, id).float64();
        if (value == nullptr) {
            throw std::runtime_error("canonical double field has wrong type");
        }
        return std::bit_cast<double>(value->bits);
    }

    static bool Boolean(const DynamicMessage& message, uint32_t id) {
        const auto* value = Field(message, id).boolean();
        if (value == nullptr) {
            throw std::runtime_error("canonical bool field has wrong type");
        }
        return value->value;
    }

    std::shared_ptr<const SchemaDescriptor> descriptor_;
    std::optional<PreparedCanonicalWireCodec> prepared_codec_;
};

std::vector<std::byte> EncodeU64Pair(uint64_t first, uint64_t second) {
    std::vector<std::byte> result(16);
    for (size_t index = 0; index < 8; ++index) {
        result[index] = static_cast<std::byte>(first >> ((7 - index) * 8));
        result[8 + index] =
            static_cast<std::byte>(second >> ((7 - index) * 8));
    }
    return result;
}

uint64_t DecodeU64(std::span<const std::byte> bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value = (value << 8) |
                static_cast<uint8_t>(bytes[offset + index]);
    }
    return value;
}

class MinoTcpPipeline final {
  public:
    MinoTcpPipeline(const CommonOptions& common, const BackendOptions& backend)
        : common_(common), backend_(backend), run_hash_(StableRunHash(common.run_id)) {
        try {
            Initialize();
        } catch (...) {
            CloseBestEffort();
            throw;
        }
    }

    MinoTcpPipeline(const MinoTcpPipeline&) = delete;
    MinoTcpPipeline& operator=(const MinoTcpPipeline&) = delete;
    ~MinoTcpPipeline() { CloseBestEffort(); }

    void SendData(size_t edge, uint64_t sample_id,
                  const SemanticFrame& semantic, const PipelineSchema& schema,
                  uint64_t deadline_ns, size_t* encoded_size) {
        if (!output_connection_.has_value()) {
            throw std::logic_error("pipeline role has no TCP output connection");
        }
        WireFrame frame;
        frame.header.frame_type = FrameType::kData;
        frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
        frame.header.topic_id = static_cast<uint32_t>(edge + 1);
        frame.header.msg_type = static_cast<uint32_t>(
            schema.descriptor().identity().short_id());
        frame.header.connection_schema_ref = 1;
        frame.header.schema_version =
            schema.descriptor().identity().schema_version();
        frame.header.layout_version =
            schema.descriptor().identity().layout_version();
        frame.header.source_node_id = run_hash_;
        frame.header.source_publisher_id = 1;
        frame.header.source_publisher_epoch = run_hash_;
        frame.header.sequence_num = sample_id + 1;
        frame.header.timestamp_ns = semantic.origin_timestamp_ns;
        frame.payload = schema.Encode(semantic);
        std::vector<std::byte> body = TakeOrThrow(
            "WireFrameCodec::Encode", WireFrameCodec::Encode(frame, limits_));
        if (encoded_size != nullptr) *encoded_size = body.size();
        Send(*output_connection_, body, deadline_ns,
             transport::UntrackedTrafficClass::kData);
    }

    void ReceiveData(size_t edge, uint64_t expected_id,
                     const PipelineSchema& schema, uint64_t deadline_ns,
                     size_t* encoded_size, SemanticFrame* semantic) {
        if (!input_connection_.has_value()) {
            throw std::logic_error("pipeline role has no TCP input connection");
        }
        std::vector<std::byte> body = Receive(*input_connection_, deadline_ns);
        if (encoded_size != nullptr) *encoded_size = body.size();
        WireFrame frame = TakeOrThrow(
            "WireFrameCodec::Decode", WireFrameCodec::Decode(body, limits_));
        const auto& identity = schema.descriptor().identity();
        if (frame.header.frame_type != FrameType::kData ||
            frame.header.topic_id != edge + 1 ||
            frame.header.msg_type != static_cast<uint32_t>(identity.short_id()) ||
            frame.header.connection_schema_ref != 1 ||
            frame.header.schema_version != identity.schema_version() ||
            frame.header.layout_version != identity.layout_version() ||
            frame.header.source_node_id != run_hash_ ||
            frame.header.source_publisher_id != 1 ||
            frame.header.source_publisher_epoch != run_hash_ ||
            frame.header.sequence_num != expected_id + 1) {
            throw std::runtime_error(
                "Mino TCP WireFrame header does not match this pipeline run");
        }
        schema.Decode(frame.payload, semantic);
    }

    void Complete(uint64_t total_frames, uint64_t deadline_ns) {
        if (common_.role == Role::kCanbus) {
            SendCompletion(*input_connection_, total_frames, deadline_ns);
            return;
        }
        ValidateCompletion(Receive(*output_connection_, deadline_ns),
                           total_frames);
        if (common_.role != Role::kPerception) {
            SendCompletion(*input_connection_, total_frames, deadline_ns);
        }
    }

    void CloseOrThrow() {
        if (driver_ == nullptr) return;
        const Status status = driver_->Shutdown();
        driver_.reset();
        if (!status.ok()) ThrowStatus("TcpDriver::Shutdown", status);
    }

    std::string EndpointsJson() const {
        std::string result = "{\"listen\":";
        if (const auto edge = InputEdge(common_.role); edge.has_value()) {
            result += "\"" + JsonEscape(backend_.listen_address + ":" +
                                         std::to_string(backend_.port_base +
                                                        *edge)) +
                      "\"";
        } else {
            result += "null";
        }
        result += ",\"peer\":";
        if (const auto edge = OutputEdge(common_.role); edge.has_value()) {
            result += "\"" + JsonEscape(backend_.peer_address + ":" +
                                         std::to_string(backend_.port_base +
                                                        *edge)) +
                      "\"";
        } else {
            result += "null";
        }
        result += "}";
        return result;
    }

  private:
    void Initialize() {
        limits_.max_payload_length = kMaximumFrameBodyBytes;
        limits_.max_buffered_bytes =
            bridge::kLengthPrefixSize + bridge::kWireMaximumHeaderLength +
            kMaximumFrameBodyBytes;

        TcpDriverOptions options;
        options.max_frame_body_bytes = kMaximumFrameBodyBytes;
        options.max_total_send_buffer_bytes = 32u * 1024u * 1024u;
        options.max_connection_send_buffer_bytes = 16u * 1024u * 1024u;
        options.max_ready_receive_bytes = 32u * 1024u * 1024u;
        options.max_ready_receive_messages = 65'536;
        options.max_pending_accepts = 16;
        options.heartbeat_interval_ms = 1000;
        options.idle_timeout_ms = 60'000;
        options.partial_frame_timeout_ms = 10'000;
        options.io_poll_max_ms = 1;
        options.max_control_send_buffer_bytes = 4u * 1024u * 1024u;
        options.max_control_send_messages = 1024;
        driver_ = TakeOrThrow("TcpDriver::Create", TcpDriver::Create(options));
        const Status started = driver_->Start({
            .max_connections = 4,
            .max_listeners = 1,
            .max_queued_sends = 65'536,
        });
        if (!started.ok()) ThrowStatus("TcpDriver::Start", started);

        const uint64_t deadline_ns = AbsoluteDeadline(common_);
        if (const auto edge = InputEdge(common_.role); edge.has_value()) {
            const EndpointDescriptor endpoint = MakeEndpoint(
                backend_.listen_address,
                static_cast<uint16_t>(backend_.port_base + *edge));
            const auto listener = driver_->Listen({
                .local_endpoint = endpoint,
                .backlog = 4,
            });
            if (!listener.ok()) ThrowStatus("TcpDriver::Listen", listener.status());
            listener_ = listener->id;
        }
        if (const auto edge = OutputEdge(common_.role); edge.has_value()) {
            const EndpointDescriptor endpoint = MakeEndpoint(
                backend_.peer_address,
                static_cast<uint16_t>(backend_.port_base + *edge));
            for (;;) {
                const uint32_t timeout = RemainingMs(deadline_ns, 1000);
                if (timeout == 0) {
                    throw std::runtime_error(
                        "deadline expired connecting Mino TCP output");
                }
                auto connected = driver_->Connect({
                    .remote_endpoint = endpoint,
                    .local_bind = std::nullopt,
                    .timeout_ms = timeout,
                });
                if (connected.ok()) {
                    output_connection_ = connected->id;
                    break;
                }
                if (connected.status().code() == StatusCode::kInvalidArgument ||
                    connected.status().code() == StatusCode::kPermissionDenied ||
                    connected.status().code() == StatusCode::kUnsupported) {
                    ThrowStatus("TcpDriver::Connect", connected.status());
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        if (listener_.has_value()) {
            const uint32_t timeout = RemainingMs(deadline_ns);
            if (timeout == 0) {
                throw std::runtime_error(
                    "deadline expired accepting Mino TCP input");
            }
            auto accepted = driver_->Accept({
                .listener_id = *listener_,
                .timeout_ms = timeout,
            });
            if (!accepted.ok()) ThrowStatus("TcpDriver::Accept", accepted.status());
            input_connection_ = accepted->id;
        }
    }

    void Send(ConnectionId connection, std::span<const std::byte> body,
              uint64_t deadline_ns,
              transport::UntrackedTrafficClass traffic_class) {
        while (NowNs() < deadline_ns) {
            auto sent = driver_->SendUntracked({
                .connection_id = connection,
                .payload = body,
                .traffic_class = traffic_class,
            });
            if (sent.ok()) return;
            if (sent.status().code() != StatusCode::kWouldBlock &&
                sent.status().code() != StatusCode::kResourceExhausted) {
                ThrowStatus("TcpDriver::SendUntracked", sent.status());
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        throw std::runtime_error("deadline expired sending through Mino TCP");
    }

    std::vector<std::byte> Receive(ConnectionId connection,
                                   uint64_t deadline_ns) {
        while (NowNs() < deadline_ns) {
            const uint32_t timeout = RemainingMs(deadline_ns);
            if (timeout == 0) break;
            auto received = driver_->Poll({
                .max_messages = 1,
                .max_bytes = kMaximumFrameBodyBytes,
                .timeout_ms = timeout,
                .connection_id = connection,
            });
            if (!received.ok()) {
                if (received.status().code() == StatusCode::kTimeout ||
                    received.status().code() == StatusCode::kWouldBlock) {
                    continue;
                }
                ThrowStatus("TcpDriver::Poll", received.status());
            }
            if (received->messages.size() != 1 ||
                received->messages.front().connection_id != connection) {
                throw std::runtime_error(
                    "TcpDriver returned an invalid receive batch");
            }
            return std::move(received->messages.front().payload);
        }
        throw std::runtime_error("deadline expired receiving through Mino TCP");
    }

    void SendCompletion(ConnectionId connection, uint64_t total_frames,
                        uint64_t deadline_ns) {
        WireFrame completion;
        completion.header.frame_type = FrameType::kAck;
        completion.header.flags = FlagValue(FrameFlag::kControlFrame) |
                                  FlagValue(FrameFlag::kPayloadCrcPresent);
        completion.payload = EncodeU64Pair(run_hash_, total_frames);
        std::vector<std::byte> body = TakeOrThrow(
            "encode pipeline completion",
            WireFrameCodec::Encode(completion, limits_));
        Send(connection, body, deadline_ns,
             transport::UntrackedTrafficClass::kProtocolControl);
        // SendUntracked success is local queue admission only. Do not tear down
        // the process immediately after admitting the reverse completion ACK;
        // first require the production driver's userspace queue to drain into
        // the TCP socket so the next upstream role can observe it.
        while (driver_->stats().queued_send_bytes != 0) {
            if (NowNs() >= deadline_ns) {
                throw std::runtime_error(
                    "deadline expired flushing Mino TCP completion ACK");
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }

    void ValidateCompletion(std::span<const std::byte> body,
                            uint64_t total_frames) {
        WireFrame completion = TakeOrThrow(
            "decode pipeline completion",
            WireFrameCodec::Decode(body, limits_));
        if (completion.header.frame_type != FrameType::kAck ||
            completion.payload.size() != 16 ||
            DecodeU64(completion.payload, 0) != run_hash_ ||
            DecodeU64(completion.payload, 8) != total_frames) {
            throw std::runtime_error(
                "Mino TCP completion acknowledgment is invalid");
        }
    }

    void CloseBestEffort() noexcept {
        if (driver_ != nullptr) {
            const Status status = driver_->Shutdown();
            if (!status.ok()) {
                std::cerr << "TcpDriver::Shutdown failed: " << status.ToString()
                          << '\n';
            }
            driver_.reset();
        }
    }

    const CommonOptions& common_;
    BackendOptions backend_;
    uint64_t run_hash_ = 0;
    WireFrameLimits limits_;
    std::unique_ptr<TcpDriver> driver_;
    std::optional<ConnectionId> listener_;
    std::optional<ConnectionId> input_connection_;
    std::optional<ConnectionId> output_connection_;
};

void ValidateSequenceAndPhase(const CommonOptions& options,
                              const SemanticFrame& frame,
                              uint64_t expected_id,
                              RunStatistics* statistics) {
    if (frame.sample_id != expected_id) {
        if (frame.sample_id < expected_id) {
            ++statistics->duplicate;
            throw std::runtime_error("duplicate sample_id");
        }
        ++statistics->out_of_order;
        throw std::runtime_error("out-of-order sample_id");
    }
    const bool measured = expected_id >= options.warmup_messages;
    if ((measured && frame.origin_timestamp_ns == 0) ||
        (!measured && frame.origin_timestamp_ns != 0)) {
        ++statistics->corrupt;
        throw std::runtime_error(
            measured ? "measured frame has a zero origin timestamp"
                     : "warmup frame has a non-zero origin timestamp");
    }
    if (frame.profile != static_cast<uint32_t>(options.profile)) {
        ++statistics->corrupt;
        throw std::runtime_error("frame profile does not match --profile");
    }
}

std::array<uint64_t, 5> StageTimestamps(const SemanticFrame& frame) {
    return {frame.perception_timestamp_ns, frame.prediction_timestamp_ns,
            frame.planning_timestamp_ns, frame.control_timestamp_ns,
            frame.guardian_timestamp_ns};
}

bool ApplyIndependentHostStage(Role role, SemanticFrame* frame,
                               std::string* error) {
    if (frame == nullptr) {
        if (error != nullptr) *error = "frame must not be null";
        return false;
    }
    if (!ValidateSemanticFrame(*frame, error)) return false;
    if (frame->completed_stage_mask != ExpectedMask(role)) {
        if (error != nullptr) *error = "stage mask mismatch";
        return false;
    }
    const size_t completed = static_cast<size_t>(role);
    const auto timestamps = StageTimestamps(*frame);
    for (size_t index = 0; index < timestamps.size(); ++index) {
        if ((index < completed && timestamps[index] == 0) ||
            (index >= completed && timestamps[index] != 0)) {
            if (error != nullptr) *error = "stage timestamp presence mismatch";
            return false;
        }
    }
    if (role == Role::kCanbus) return true;
    const uint64_t timestamp = NowNs();
    switch (role) {
        case Role::kPerception: frame->perception_timestamp_ns = timestamp; break;
        case Role::kPrediction: frame->prediction_timestamp_ns = timestamp; break;
        case Role::kPlanning: frame->planning_timestamp_ns = timestamp; break;
        case Role::kControl: frame->control_timestamp_ns = timestamp; break;
        case Role::kGuardian: frame->guardian_timestamp_ns = timestamp; break;
        case Role::kCanbus: break;
    }
    frame->completed_stage_mask |= RoleBit(role);
    return true;
}

bool ApplyConfiguredStage(const BackendOptions& backend, Role role,
                          SemanticFrame* frame, std::string* error) {
    return backend.independent_host_clocks
               ? ApplyIndependentHostStage(role, frame, error)
               : ApplyStage(role, frame, error);
}

uint64_t TotalFrames(const CommonOptions& options) {
    if (options.messages >
        std::numeric_limits<uint64_t>::max() - options.warmup_messages) {
        throw std::runtime_error("total frame count overflows");
    }
    return options.messages + options.warmup_messages;
}

void RunSource(const CommonOptions& options, const BackendOptions& backend,
               MinoTcpPipeline* transport, const PipelineSchema& schema,
               uint64_t deadline_ns, RunStatistics* statistics) {
    const size_t output_edge = *OutputEdge(options.role);
    const uint64_t total = TotalFrames(options);
    const uint64_t schedule_start_ns = NowNs();
    for (uint64_t sample_id = 0; sample_id < total; ++sample_id) {
        PaceSource(schedule_start_ns, sample_id, options.publish_interval_us,
                   deadline_ns);
        const bool measured = sample_id >= options.warmup_messages;
        SemanticFrame frame =
            InitializeSourceFrame(sample_id, options.profile, measured);
        std::string error;
        if (!ApplyConfiguredStage(backend, Role::kPerception, &frame, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error(
                "perception stage rejected source frame: " + error);
        }
        size_t encoded_size = 0;
        transport->SendData(output_edge, sample_id, frame, schema, deadline_ns,
                            &encoded_size);
        if (measured) {
            statistics->encoded_bytes_total += encoded_size;
            ++statistics->measured_completed;
        }
    }
}

void RunForwarder(const CommonOptions& options, const BackendOptions& backend,
                  MinoTcpPipeline* transport, const PipelineSchema& schema,
                  uint64_t deadline_ns, RunStatistics* statistics) {
    const size_t input_edge = *InputEdge(options.role);
    const size_t output_edge = *OutputEdge(options.role);
    const uint64_t total = TotalFrames(options);
    SemanticFrame frame;
    frame.payload.reserve(ProfilePayloadBytes(options.profile));
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        transport->ReceiveData(input_edge, expected_id, schema, deadline_ns,
                               nullptr, &frame);
        ValidateSequenceAndPhase(options, frame, expected_id, statistics);
        std::string error;
        if (!ApplyConfiguredStage(backend, options.role, &frame, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error(std::string(RoleName(options.role)) +
                                     " stage rejected frame: " + error);
        }
        size_t encoded_size = 0;
        transport->SendData(output_edge, expected_id, frame, schema,
                            deadline_ns, &encoded_size);
        if (expected_id >= options.warmup_messages) {
            statistics->encoded_bytes_total += encoded_size;
            ++statistics->measured_completed;
        }
    }
}

void RunSink(const CommonOptions& options, const BackendOptions& backend,
             MinoTcpPipeline* transport, const PipelineSchema& schema,
             uint64_t deadline_ns, RunStatistics* statistics) {
    const size_t input_edge = *InputEdge(options.role);
    statistics->latencies_ns.reserve(static_cast<size_t>(std::min(
        options.messages, kMaximumInitialLatencyReserve)));
    const uint64_t total = TotalFrames(options);
    SemanticFrame frame;
    frame.payload.reserve(ProfilePayloadBytes(options.profile));
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        size_t encoded_size = 0;
        transport->ReceiveData(input_edge, expected_id, schema, deadline_ns,
                               &encoded_size, &frame);
        ValidateSequenceAndPhase(options, frame, expected_id, statistics);
        std::string error;
        if (!ApplyConfiguredStage(backend, Role::kCanbus, &frame, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error("canbus stage rejected frame: " + error);
        }
        if (expected_id < options.warmup_messages) continue;

        const uint64_t completion_ns = NowNs();
        if (statistics->measured_completed == 0) {
            statistics->first_measured_origin_ns = frame.origin_timestamp_ns;
            statistics->first_measured_completion_ns = completion_ns;
        }
        statistics->last_measured_completion_ns = completion_ns;
        statistics->encoded_bytes_total += encoded_size;
        if (!backend.independent_host_clocks) {
            if (completion_ns < frame.origin_timestamp_ns) {
                ++statistics->corrupt;
                throw std::runtime_error(
                    "sink completion timestamp precedes frame origin");
            }
            statistics->latencies_ns.push_back(
                completion_ns - frame.origin_timestamp_ns);
        }
        ++statistics->measured_completed;
    }
}

std::string BackendDetails(const BackendOptions& backend,
                           const PipelineSchema& schema,
                           const MinoTcpPipeline& transport) {
    const auto& identity = schema.descriptor().identity();
    return "{\"transport\":\"production TcpDriver plaintext benchmark mode\","
           "\"wire_frame\":\"WireFrameCodec v1 with payload CRC\","
           "\"schema_codec\":\"CanonicalWireCodec\","
           "\"canonical_descriptor_closure\":\"startup-prepared\","
           "\"schema_source\":\"minoc descriptor artifact\","
           "\"compilation_mode\":\"" + std::string(CompilationMode()) +
           "\",\"schema_short_id\":" + std::to_string(identity.short_id()) +
           ",\"schema_version\":" +
           std::to_string(identity.schema_version()) +
           ",\"layout_version\":" +
           std::to_string(identity.layout_version()) +
           ",\"clock_mode\":\"" +
           std::string(backend.independent_host_clocks ? "independent-hosts"
                                                       : "same-host") +
           "\",\"one_way_latency_valid\":" +
           (backend.independent_host_clocks ? "false" : "true") +
           ",\"completion_barrier\":\"reverse hop-by-hop ACK\","
           "\"endpoints\":" + transport.EndpointsJson() + "}";
}

void PopulateResult(const CommonOptions& options,
                    const BackendOptions& backend,
                    const RunStatistics& statistics, bool success,
                    SinkResult* result) {
    result->counts.offered = options.messages;
    result->counts.received = statistics.measured_completed;
    result->counts.duplicate = statistics.duplicate;
    result->counts.out_of_order = statistics.out_of_order;
    result->counts.corrupt = statistics.corrupt;
    result->counts.lost =
        statistics.measured_completed < options.messages
            ? options.messages - statistics.measured_completed
            : 0;
    if (success) {
        result->counts.received = options.messages;
        result->counts.lost = 0;
    }
    if (statistics.measured_completed != 0) {
        result->encoded_bytes =
            (statistics.encoded_bytes_total +
             statistics.measured_completed / 2) /
            statistics.measured_completed;
    }
    if (options.role != Role::kCanbus) return;

    result->latency_ns = Summarize(statistics.latencies_ns);
    if (backend.independent_host_clocks) {
        if (statistics.measured_completed > 1 &&
            statistics.first_measured_completion_ns != 0 &&
            statistics.last_measured_completion_ns >
                statistics.first_measured_completion_ns) {
            result->elapsed_ns = statistics.last_measured_completion_ns -
                                 statistics.first_measured_completion_ns;
            result->throughput_messages_per_second =
                static_cast<double>(statistics.measured_completed - 1) *
                static_cast<double>(kNanosecondsPerSecond) /
                static_cast<double>(result->elapsed_ns);
        }
        return;
    }
    if (statistics.first_measured_origin_ns != 0 &&
        statistics.last_measured_completion_ns >=
            statistics.first_measured_origin_ns) {
        result->elapsed_ns = std::max<uint64_t>(
            1, statistics.last_measured_completion_ns -
                   statistics.first_measured_origin_ns);
        result->throughput_messages_per_second =
            static_cast<double>(statistics.measured_completed) *
            static_cast<double>(kNanosecondsPerSecond) /
            static_cast<double>(result->elapsed_ns);
    }
}

void WriteResultBestEffort(const SinkResult& result) noexcept {
    try {
        WriteSinkResult(result);
    } catch (const std::exception& exception) {
        std::cerr << "failed to write result artifact " << result.options.output
                  << ": " << exception.what() << '\n';
    } catch (...) {
        std::cerr << "failed to write result artifact " << result.options.output
                  << ": unknown exception\n";
    }
}

int PipelineMain(int argc, char** argv) {
    std::optional<CommonOptions> parsed_common;
    std::optional<BackendOptions> parsed_backend;
    RunStatistics statistics;
    SinkResult result;
    try {
        parsed_common = ParseCommonOptions(argc, argv);
        parsed_backend = ParseBackendOptions(argc, argv);
        const CommonOptions& common = *parsed_common;
        const BackendOptions& backend = *parsed_backend;
        result.backend = std::string(kBackend);
        result.options = common;
        result.payload_bytes = ProfilePayloadBytes(common.profile);

        PipelineSchema schema(backend.descriptor);
        const uint64_t deadline_ns = AbsoluteDeadline(common);
        MinoTcpPipeline transport(common, backend);
        result.backend_details = BackendDetails(backend, schema, transport);
        WriteReadyFile(common.runtime_dir, kBackend, common.role, common.run_id);
        if (!WaitForStartFile(common.runtime_dir, common.run_id, deadline_ns)) {
            throw std::runtime_error("deadline expired waiting for start file");
        }

        switch (common.role) {
            case Role::kPerception:
                RunSource(common, backend, &transport, schema, deadline_ns,
                          &statistics);
                break;
            case Role::kPrediction:
            case Role::kPlanning:
            case Role::kControl:
            case Role::kGuardian:
                RunForwarder(common, backend, &transport, schema, deadline_ns,
                             &statistics);
                break;
            case Role::kCanbus:
                RunSink(common, backend, &transport, schema, deadline_ns,
                        &statistics);
                break;
        }
        if (statistics.measured_completed != common.messages) {
            throw std::runtime_error(
                "completed measured-frame count mismatch");
        }
        transport.Complete(TotalFrames(common), deadline_ns);
        transport.CloseOrThrow();
        PopulateResult(common, backend, statistics, true, &result);
        WriteSinkResult(result);
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << kBackend << " pipeline failed: " << exception.what()
                  << '\n';
        if (parsed_common.has_value()) {
            result.backend = std::string(kBackend);
            result.options = *parsed_common;
            result.payload_bytes = ProfilePayloadBytes(result.options.profile);
            const BackendOptions fallback =
                parsed_backend.value_or(BackendOptions{});
            PopulateResult(result.options, fallback, statistics, false, &result);
            result.outcome = "failure";
            result.error = exception.what();
            WriteResultBestEffort(result);
        }
        return 1;
    } catch (...) {
        std::cerr << kBackend << " pipeline failed: unknown exception\n";
        if (parsed_common.has_value()) {
            result.backend = std::string(kBackend);
            result.options = *parsed_common;
            result.payload_bytes = ProfilePayloadBytes(result.options.profile);
            const BackendOptions fallback =
                parsed_backend.value_or(BackendOptions{});
            PopulateResult(result.options, fallback, statistics, false, &result);
            result.outcome = "failure";
            result.error = "unknown exception";
            WriteResultBestEffort(result);
        }
        return 1;
    }
}

}  // namespace
}  // namespace mino::benchmarks::pipeline

int main(int argc, char** argv) {
    return mino::benchmarks::pipeline::PipelineMain(argc, argv);
}
