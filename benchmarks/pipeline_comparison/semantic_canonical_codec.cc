// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/semantic_canonical_codec.h"

#include <bit>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/descriptor.h"

namespace mino::benchmarks::pipeline {
namespace {

using schema::DynamicMessage;
using schema::DynamicValue;
using schema::PreparedCanonicalWireCodec;
using schema::SchemaDescriptor;

constexpr size_t kMaximumArtifactBytes = 16u * 1024u * 1024u;
constexpr std::string_view kSchemaName =
    "mino.benchmarks.pipeline.AutonomyPipelineFrame";

void ThrowStatus(std::string_view operation, const Status& status) {
    throw std::runtime_error(std::string(operation) + ": " + status.ToString());
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

void Set(DynamicMessage& message, uint32_t id, DynamicValue value) {
    const Status status = message.SetField(id, std::move(value));
    if (!status.ok()) ThrowStatus("set dynamic field", status);
}

const DynamicValue& Field(const DynamicMessage& message, uint32_t id) {
    const DynamicValue* value = message.FindField(id);
    if (value == nullptr) {
        throw std::runtime_error("canonical message is missing field " +
                                 std::to_string(id));
    }
    return *value;
}

uint64_t Unsigned(const DynamicMessage& message, uint32_t id) {
    const auto* value = Field(message, id).unsigned_integer();
    if (value == nullptr) {
        throw std::runtime_error("canonical unsigned field has wrong type");
    }
    return value->value;
}

uint32_t NarrowU32(uint64_t value, uint32_t id) {
    if (value > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("canonical uint32 field overflows: " +
                                 std::to_string(id));
    }
    return static_cast<uint32_t>(value);
}

double Float64(const DynamicMessage& message, uint32_t id) {
    const auto* value = Field(message, id).float64();
    if (value == nullptr) {
        throw std::runtime_error("canonical double field has wrong type");
    }
    return std::bit_cast<double>(value->bits);
}

bool Boolean(const DynamicMessage& message, uint32_t id) {
    const auto* value = Field(message, id).boolean();
    if (value == nullptr) {
        throw std::runtime_error("canonical bool field has wrong type");
    }
    return value->value;
}

}  // namespace

SemanticCanonicalCodec SemanticCanonicalCodec::FromDescriptorFile(
    const std::filesystem::path& artifact_path) {
    SemanticCanonicalCodec codec;
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
    codec.descriptor_ = artifact->types.front().descriptor;
    auto prepared = PreparedCanonicalWireCodec::Create(codec.descriptor_);
    if (!prepared.ok()) {
        ThrowStatus("prepare canonical wire codec", prepared.status());
    }
    codec.prepared_codec_.emplace(std::move(*prepared));
    const Status encode_reserved = codec.encode_message_.ReserveFields(18);
    if (!encode_reserved.ok()) {
        ThrowStatus("reserve canonical encode fields", encode_reserved);
    }
    const Status decode_reserved = codec.decode_message_.ReserveFields(18);
    if (!decode_reserved.ok()) {
        ThrowStatus("reserve canonical decode fields", decode_reserved);
    }
    return codec;
}

void SemanticCanonicalCodec::Encode(const SemanticFrame& frame,
                                    std::vector<std::byte>* output) {
    if (output == nullptr) {
        throw std::invalid_argument("canonical encode destination is null");
    }
    encode_message_.Clear();
    DynamicMessage& message = encode_message_;
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
    Set(message, 11, DynamicValue::Unsigned(frame.trajectory_point_count));
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
    Set(message, 18, DynamicValue::BytesView(payload));
    const Status encoded =
        prepared_codec_->EncodeInto(message, encode_scratch_, *output);
    if (!encoded.ok()) ThrowStatus("CanonicalWireCodec::EncodeInto", encoded);
}

void SemanticCanonicalCodec::Decode(std::span<const std::byte> bytes,
                                    SemanticFrame* frame) {
    if (frame == nullptr) {
        throw std::invalid_argument("semantic decode destination is null");
    }
    const Status decoded = prepared_codec_->DecodeInto(
        bytes, decode_scratch_, decode_message_);
    if (!decoded.ok()) ThrowStatus("CanonicalWireCodec::DecodeInto", decoded);
    const DynamicMessage& message = decode_message_;
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
    frame->trajectory_point_count = NarrowU32(Unsigned(message, 11), 11);
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

}  // namespace mino::benchmarks::pipeline
