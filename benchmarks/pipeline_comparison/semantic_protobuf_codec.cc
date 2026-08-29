// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/semantic_protobuf_codec.h"

#include <climits>
#include <stdexcept>

namespace mino::benchmarks::pipeline {
namespace {

constexpr size_t kMaximumEncodedBytes = kLargePayloadBytes + 1024;

}  // namespace

void SemanticToProtobuf(const SemanticFrame& source,
                        AutonomyPipelineFrame* destination) {
    if (destination == nullptr) {
        throw std::invalid_argument("protobuf destination is null");
    }
    destination->Clear();
    destination->set_sample_id(source.sample_id);
    destination->set_origin_timestamp_ns(source.origin_timestamp_ns);
    destination->set_perception_timestamp_ns(source.perception_timestamp_ns);
    destination->set_prediction_timestamp_ns(source.prediction_timestamp_ns);
    destination->set_planning_timestamp_ns(source.planning_timestamp_ns);
    destination->set_control_timestamp_ns(source.control_timestamp_ns);
    destination->set_guardian_timestamp_ns(source.guardian_timestamp_ns);
    destination->set_completed_stage_mask(source.completed_stage_mask);
    destination->set_profile(source.profile);
    destination->set_object_count(source.object_count);
    destination->set_trajectory_point_count(source.trajectory_point_count);
    destination->set_ego_speed_mps(source.ego_speed_mps);
    destination->set_steering_angle_rad(source.steering_angle_rad);
    destination->set_acceleration_mps2(source.acceleration_mps2);
    destination->set_brake_percentage(source.brake_percentage);
    destination->set_emergency_stop(source.emergency_stop);
    destination->set_payload_checksum(source.payload_checksum);
    destination->set_payload(
        reinterpret_cast<const char*>(source.payload.data()),
        source.payload.size());
}

bool ProtobufToSemantic(const AutonomyPipelineFrame& source,
                        SemanticFrame* destination, std::string* error) {
    if (destination == nullptr) {
        if (error != nullptr) *error = "semantic destination is null";
        return false;
    }
    if (source.payload().size() > kLargePayloadBytes) {
        if (error != nullptr) {
            *error = "protobuf payload exceeds the strict 1 MiB limit";
        }
        return false;
    }
    destination->sample_id = source.sample_id();
    destination->origin_timestamp_ns = source.origin_timestamp_ns();
    destination->perception_timestamp_ns = source.perception_timestamp_ns();
    destination->prediction_timestamp_ns = source.prediction_timestamp_ns();
    destination->planning_timestamp_ns = source.planning_timestamp_ns();
    destination->control_timestamp_ns = source.control_timestamp_ns();
    destination->guardian_timestamp_ns = source.guardian_timestamp_ns();
    destination->completed_stage_mask = source.completed_stage_mask();
    destination->profile = source.profile();
    destination->object_count = source.object_count();
    destination->trajectory_point_count = source.trajectory_point_count();
    destination->ego_speed_mps = source.ego_speed_mps();
    destination->steering_angle_rad = source.steering_angle_rad();
    destination->acceleration_mps2 = source.acceleration_mps2();
    destination->brake_percentage = source.brake_percentage();
    destination->emergency_stop = source.emergency_stop();
    destination->payload_checksum = source.payload_checksum();
    destination->payload.assign(source.payload().begin(), source.payload().end());
    return true;
}

std::string SerializeFrame(const SemanticFrame& frame) {
    AutonomyPipelineFrame protobuf_frame;
    SemanticToProtobuf(frame, &protobuf_frame);
    if (protobuf_frame.ByteSizeLong() > kMaximumEncodedBytes) {
        throw std::runtime_error(
            "serialized protobuf frame exceeds encoded-size limit");
    }
    std::string bytes;
    if (!protobuf_frame.SerializeToString(&bytes)) {
        throw std::runtime_error("failed to serialize protobuf frame");
    }
    if (bytes.size() > kMaximumEncodedBytes) {
        throw std::runtime_error(
            "serialized protobuf frame exceeds encoded-size limit");
    }
    return bytes;
}

bool ParseFrame(std::span<const uint8_t> bytes, SemanticFrame* frame,
                std::string* error) {
    if (bytes.size() > kMaximumEncodedBytes ||
        bytes.size() > static_cast<size_t>(INT_MAX)) {
        if (error != nullptr) {
            *error = "encoded protobuf frame exceeds receive limit";
        }
        return false;
    }
    AutonomyPipelineFrame protobuf_frame;
    if (!protobuf_frame.ParseFromArray(bytes.data(),
                                       static_cast<int>(bytes.size()))) {
        if (error != nullptr) *error = "failed to parse protobuf frame";
        return false;
    }
    return ProtobufToSemantic(protobuf_frame, frame, error);
}

bool ParseFrame(std::span<const std::byte> bytes, SemanticFrame* frame,
                std::string* error) {
    return ParseFrame(
        std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()),
        frame, error);
}

}  // namespace mino::benchmarks::pipeline
