// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/generated/AutonomyPipelineFrame.hpp"
#include "benchmarks/pipeline_comparison/generated/AutonomyPipelineFramePubSubTypes.hpp"
#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <fastdds/dds/core/ReturnCode.hpp>
#include <fastdds/dds/core/Time_t.hpp>
#include <fastdds/dds/core/policy/QosPolicies.hpp>
#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/qos/DomainParticipantQos.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/dds/topic/qos/TopicQos.hpp>
#include <fastdds/rtps/attributes/BuiltinTransports.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace mino::benchmarks::pipeline {
namespace {

namespace dds = eprosima::fastdds::dds;
namespace rtps = eprosima::fastdds::rtps;

constexpr std::string_view kBackend = "fastdds-idl";
constexpr uint32_t kDefaultDomainId = 73;
constexpr int32_t kDefaultHistoryDepth = 64;
constexpr uint32_t kMaximumDomainId = 232;
constexpr int32_t kMinimumHistoryDepth = 2;
constexpr int32_t kMaximumHistoryDepth = 4096;
constexpr size_t kMaximumPayloadBytes = kLargePayloadBytes;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr uint64_t kMatchingPollNanoseconds = 10'000'000ull;

constexpr uint64_t kMaximumInitialLatencyReserve = 1'000'000;

struct BackendOptions {
    uint32_t domain_id = kDefaultDomainId;
    int32_t history_depth = kDefaultHistoryDepth;
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

uint64_t RemainingNanoseconds(uint64_t absolute_deadline_ns) {
    const uint64_t now = NowNs();
    return now < absolute_deadline_ns ? absolute_deadline_ns - now : 0;
}

dds::Duration_t RemainingDuration(uint64_t absolute_deadline_ns) {
    const uint64_t remaining_ns = RemainingNanoseconds(absolute_deadline_ns);
    return dds::Duration_t(
        static_cast<int32_t>(remaining_ns / kNanosecondsPerSecond),
        static_cast<uint32_t>(remaining_ns % kNanosecondsPerSecond));
}

void EnsureBeforeDeadline(uint64_t absolute_deadline_ns,
                          std::string_view operation) {
    if (RemainingNanoseconds(absolute_deadline_ns) == 0) {
        throw std::runtime_error("deadline expired before " +
                                 std::string(operation));
    }
}

std::string ReturnCodeName(dds::ReturnCode_t code) {
    switch (code) {
        case dds::RETCODE_OK: return "RETCODE_OK";
        case dds::RETCODE_ERROR: return "RETCODE_ERROR";
        case dds::RETCODE_UNSUPPORTED: return "RETCODE_UNSUPPORTED";
        case dds::RETCODE_BAD_PARAMETER: return "RETCODE_BAD_PARAMETER";
        case dds::RETCODE_PRECONDITION_NOT_MET:
            return "RETCODE_PRECONDITION_NOT_MET";
        case dds::RETCODE_OUT_OF_RESOURCES:
            return "RETCODE_OUT_OF_RESOURCES";
        case dds::RETCODE_NOT_ENABLED: return "RETCODE_NOT_ENABLED";
        case dds::RETCODE_IMMUTABLE_POLICY:
            return "RETCODE_IMMUTABLE_POLICY";
        case dds::RETCODE_INCONSISTENT_POLICY:
            return "RETCODE_INCONSISTENT_POLICY";
        case dds::RETCODE_ALREADY_DELETED:
            return "RETCODE_ALREADY_DELETED";
        case dds::RETCODE_TIMEOUT: return "RETCODE_TIMEOUT";
        case dds::RETCODE_NO_DATA: return "RETCODE_NO_DATA";
        case dds::RETCODE_ILLEGAL_OPERATION:
            return "RETCODE_ILLEGAL_OPERATION";
        default:
            return "UNKNOWN_RETURN_CODE(" + std::to_string(code) + ")";
    }
}

void RequireOk(dds::ReturnCode_t code, std::string_view operation) {
    if (code != dds::RETCODE_OK) {
        throw std::runtime_error(std::string(operation) + " failed: " +
                                 ReturnCodeName(code));
    }
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

uint64_t HashRunId(std::string_view run_id, uint64_t seed) {
    constexpr uint64_t kFnvPrime = 1'099'511'628'211ull;
    uint64_t hash = seed;
    for (char character : run_id) {
        hash ^= static_cast<uint8_t>(character);
        hash *= kFnvPrime;
    }
    return hash;
}

std::string Hex64(uint64_t value) {
    constexpr char kHexDigits[] = "0123456789abcdef";
    std::string result(16, '0');
    for (size_t index = 0; index < result.size(); ++index) {
        const size_t shift = (result.size() - index - 1) * 4;
        result[index] = kHexDigits[(value >> shift) & 0x0f];
    }
    return result;
}

bool IsDdsTokenCharacter(char character) {
    return (character >= 'a' && character <= 'z') ||
           (character >= 'A' && character <= 'Z') ||
           (character >= '0' && character <= '9') || character == '_';
}

std::string SafeRunToken(std::string_view run_id) {
    constexpr size_t kMaximumReadableCharacters = 48;
    std::string readable;
    readable.reserve(std::min(run_id.size(), kMaximumReadableCharacters));
    bool last_was_separator = false;
    for (char character : run_id) {
        if (readable.size() >= kMaximumReadableCharacters) break;
        if (IsDdsTokenCharacter(character)) {
            readable.push_back(character);
            last_was_separator = false;
        } else if (!readable.empty() && !last_was_separator) {
            readable.push_back('_');
            last_was_separator = true;
        }
    }
    while (!readable.empty() && readable.back() == '_') readable.pop_back();
    if (readable.empty()) readable = "run";

    constexpr uint64_t kFnvOffset = 14'695'981'039'346'656'037ull;
    constexpr uint64_t kSecondSeed = 7'804'984'730'203'442'981ull;
    return readable + "_" + Hex64(HashRunId(run_id, kFnvOffset)) +
           Hex64(HashRunId(run_id, kSecondSeed));
}

std::array<std::string, 5> TopicNames(std::string_view run_id) {
    const std::string prefix = "mino_pipeline_" + SafeRunToken(run_id) +
                               "_edge_";
    std::array<std::string, 5> topics;
    for (size_t edge = 0; edge < topics.size(); ++edge) {
        topics[edge] = prefix + std::to_string(edge);
    }
    return topics;
}

uint64_t ParseUnsignedBackendValue(std::string_view value,
                                   std::string_view option) {
    uint64_t parsed = 0;
    const auto conversion = std::from_chars(
        value.data(), value.data() + value.size(), parsed);
    if (value.empty() || conversion.ec != std::errc{} ||
        conversion.ptr != value.data() + value.size()) {
        throw std::runtime_error(std::string(option) +
                                 " requires an unsigned decimal integer");
    }
    return parsed;
}

std::optional<std::string_view> BackendOptionValue(
    int* index, int argc, char** argv, std::string_view option) {
    const std::string_view argument(argv[*index]);
    if (argument == option) {
        if (*index + 1 >= argc || argv[*index + 1] == nullptr) {
            throw std::runtime_error(std::string(option) + " requires a value");
        }
        return std::string_view(argv[++(*index)]);
    }
    if (argument.size() > option.size() && argument.starts_with(option) &&
        argument[option.size()] == '=') {
        return argument.substr(option.size() + 1);
    }
    return std::nullopt;
}

BackendOptions ParseBackendOptions(int argc, char** argv) {
    BackendOptions options;
    bool seen_domain_id = false;
    bool seen_history_depth = false;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            throw std::invalid_argument("argv contains a null argument");
        }
        if (const auto value = BackendOptionValue(
                &index, argc, argv, "--domain-id")) {
            if (seen_domain_id) {
                throw std::runtime_error(
                    "--domain-id may be specified only once");
            }
            seen_domain_id = true;
            const uint64_t parsed =
                ParseUnsignedBackendValue(*value, "--domain-id");
            if (parsed > kMaximumDomainId) {
                throw std::runtime_error("--domain-id must be in [0, 232]");
            }
            options.domain_id = static_cast<uint32_t>(parsed);
            continue;
        }
        if (const auto value = BackendOptionValue(
                &index, argc, argv, "--history-depth")) {
            if (seen_history_depth) {
                throw std::runtime_error(
                    "--history-depth may be specified only once");
            }
            seen_history_depth = true;
            const uint64_t parsed =
                ParseUnsignedBackendValue(*value, "--history-depth");
            if (parsed < static_cast<uint64_t>(kMinimumHistoryDepth) ||
                parsed > static_cast<uint64_t>(kMaximumHistoryDepth)) {
                throw std::runtime_error(
                    "--history-depth must be in [2, 4096]");
            }
            options.history_depth = static_cast<int32_t>(parsed);
        }
    }
    return options;
}

template <typename Qos>
void ConfigureDeliveryQos(Qos* qos, int32_t history_depth,
                          uint64_t deadline_seconds) {
    qos->reliability().kind = dds::RELIABLE_RELIABILITY_QOS;
    qos->reliability().max_blocking_time = dds::Duration_t(
        static_cast<int32_t>(deadline_seconds), 0u);
    qos->durability().kind = dds::VOLATILE_DURABILITY_QOS;
    // KEEP_LAST may overwrite reliable-but-unacknowledged samples when the
    // application reader is slower than the writer. KEEP_ALL with bounded
    // resource limits applies backpressure instead, preserving conservation.
    qos->history().kind = dds::KEEP_ALL_HISTORY_QOS;
    qos->history().depth = history_depth;
    qos->resource_limits().max_samples = history_depth;
    qos->resource_limits().max_instances = 1;
    qos->resource_limits().max_samples_per_instance = history_depth;
}

class FastDdsPipeline {
  public:
    FastDdsPipeline(const CommonOptions& common,
                    const BackendOptions& backend,
                    const std::array<std::string, 5>& topics)
        : role_(common.role),
          deadline_seconds_(common.deadline_seconds),
          backend_(backend),
          topics_(topics),
          type_(new AutonomyPipelineFramePubSubType()) {
        try {
            Initialize();
        } catch (...) {
            CloseBestEffort();
            throw;
        }
    }

    FastDdsPipeline(const FastDdsPipeline&) = delete;
    FastDdsPipeline& operator=(const FastDdsPipeline&) = delete;

    ~FastDdsPipeline() { CloseBestEffort(); }

    void WaitForExpectedMatches(uint64_t absolute_deadline_ns) {
        while (true) {
            bool input_matched = reader_ == nullptr;
            bool output_matched = writer_ == nullptr;
            if (reader_ != nullptr) {
                dds::SubscriptionMatchedStatus status;
                RequireOk(reader_->get_subscription_matched_status(status),
                          "Fast DDS get_subscription_matched_status");
                input_matched = status.current_count >= 1;
            }
            if (writer_ != nullptr) {
                dds::PublicationMatchedStatus status;
                RequireOk(writer_->get_publication_matched_status(status),
                          "Fast DDS get_publication_matched_status");
                output_matched = status.current_count >= 1;
            }
            if (input_matched && output_matched) return;

            const uint64_t remaining_ns =
                RemainingNanoseconds(absolute_deadline_ns);
            if (remaining_ns == 0) {
                std::string missing;
                if (!input_matched) missing += " input-reader/upstream-writer";
                if (!output_matched) {
                    missing += " output-writer/downstream-reader";
                }
                throw std::runtime_error(
                    "deadline expired waiting for Fast DDS matching:" + missing);
            }
            std::this_thread::sleep_for(std::chrono::nanoseconds(
                std::min(remaining_ns, kMatchingPollNanoseconds)));
        }
    }

    void Write(const AutonomyPipelineFrame& sample,
               uint64_t absolute_deadline_ns) {
        if (writer_ == nullptr) {
            throw std::logic_error("pipeline role has no Fast DDS DataWriter");
        }
        EnsureBeforeDeadline(absolute_deadline_ns, "Fast DDS write");
        const dds::ReturnCode_t code = writer_->write(&sample);
        if (code != dds::RETCODE_OK) {
            throw std::runtime_error("Fast DDS DataWriter::write failed: " +
                                     ReturnCodeName(code));
        }
        EnsureBeforeDeadline(absolute_deadline_ns,
                             "completion of Fast DDS write");
    }

    AutonomyPipelineFrame Take(uint64_t absolute_deadline_ns) {
        if (reader_ == nullptr) {
            throw std::logic_error("pipeline role has no Fast DDS DataReader");
        }
        AutonomyPipelineFrame sample;
        while (true) {
            const dds::Duration_t remaining =
                RemainingDuration(absolute_deadline_ns);
            if (remaining.seconds == 0 && remaining.nanosec == 0) {
                throw std::runtime_error(
                    "deadline expired before Fast DDS take_next_sample");
            }
            if (!reader_->wait_for_unread_message(remaining)) {
                throw std::runtime_error(
                    "deadline expired waiting for a Fast DDS sample");
            }

            dds::SampleInfo info;
            const dds::ReturnCode_t code =
                reader_->take_next_sample(&sample, &info);
            if (code == dds::RETCODE_NO_DATA) continue;
            if (code != dds::RETCODE_OK) {
                throw std::runtime_error(
                    "Fast DDS DataReader::take_next_sample failed: " +
                    ReturnCodeName(code));
            }
            if (!info.valid_data) continue;
            EnsureBeforeDeadline(absolute_deadline_ns,
                                 "completion of Fast DDS take_next_sample");
            return sample;
        }
    }

    uint32_t SerializedSize(const AutonomyPipelineFrame& sample) {
        const uint32_t size = type_.calculate_serialized_size(
            &sample,
            dds::DataRepresentationId_t::XCDR_DATA_REPRESENTATION);
        if (size == 0) {
            throw std::runtime_error(
                "Fast DDS TopicDataType serialized-size provider failed");
        }
        return size;
    }

    void WaitForAcknowledgments(uint64_t absolute_deadline_ns) {
        if (writer_ == nullptr) return;
        const dds::Duration_t remaining =
            RemainingDuration(absolute_deadline_ns);
        if (remaining.seconds == 0 && remaining.nanosec == 0) {
            throw std::runtime_error(
                "deadline expired before Fast DDS acknowledgment wait");
        }
        RequireOk(writer_->wait_for_acknowledgments(remaining),
                  "Fast DDS wait_for_acknowledgments");
        EnsureBeforeDeadline(absolute_deadline_ns,
                             "Fast DDS acknowledgment completion");
    }

    void CloseOrThrow() {
        if (participant_ == nullptr) return;
        const dds::ReturnCode_t contained =
            participant_->delete_contained_entities();
        const dds::ReturnCode_t participant =
            dds::DomainParticipantFactory::get_instance()->delete_participant(
                participant_);
        if (participant == dds::RETCODE_OK) ResetEntityPointers();
        if (contained != dds::RETCODE_OK || participant != dds::RETCODE_OK) {
            std::string error = "Fast DDS entity cleanup failed";
            if (contained != dds::RETCODE_OK) {
                error += ": delete_contained_entities=" +
                         ReturnCodeName(contained);
            }
            if (participant != dds::RETCODE_OK) {
                error += ": delete_participant=" +
                         ReturnCodeName(participant);
            }
            throw std::runtime_error(error);
        }
    }

  private:
    void Initialize() {
        dds::DomainParticipantQos participant_qos =
            dds::PARTICIPANT_QOS_DEFAULT;
        participant_qos.setup_transports(rtps::BuiltinTransports::DEFAULT);
        const std::string participant_name =
            "mino_fastdds_" + SafeRunToken(topics_[0]) + "_" +
            std::string(RoleName(role_));
        participant_qos.name(participant_name.c_str());
        participant_ =
            dds::DomainParticipantFactory::get_instance()->create_participant(
                backend_.domain_id, participant_qos, nullptr,
                dds::StatusMask::none());
        if (participant_ == nullptr) {
            throw std::runtime_error(
                "Fast DDS create_participant returned nullptr");
        }
        RequireOk(type_.register_type(participant_),
                  "Fast DDS register AutonomyPipelineFramePubSubType");

        const std::optional<size_t> input_edge = InputEdge(role_);
        if (input_edge.has_value()) CreateInput(*input_edge);
        const std::optional<size_t> output_edge = OutputEdge(role_);
        if (output_edge.has_value()) CreateOutput(*output_edge);
    }

    dds::Topic* CreateTopic(size_t edge) {
        dds::TopicQos topic_qos = dds::TOPIC_QOS_DEFAULT;
        ConfigureDeliveryQos(&topic_qos, backend_.history_depth,
                             deadline_seconds_);
        dds::Topic* topic = participant_->create_topic(
            topics_[edge], type_.get_type_name(), topic_qos, nullptr,
            dds::StatusMask::none());
        if (topic == nullptr) {
            throw std::runtime_error("Fast DDS create_topic returned nullptr for " +
                                     topics_[edge]);
        }
        return topic;
    }

    void CreateInput(size_t edge) {
        subscriber_ = participant_->create_subscriber(
            dds::SUBSCRIBER_QOS_DEFAULT, nullptr, dds::StatusMask::none());
        if (subscriber_ == nullptr) {
            throw std::runtime_error(
                "Fast DDS create_subscriber returned nullptr");
        }
        input_topic_ = CreateTopic(edge);
        dds::DataReaderQos reader_qos = dds::DATAREADER_QOS_DEFAULT;
        ConfigureDeliveryQos(&reader_qos, backend_.history_depth,
                             deadline_seconds_);
        reader_qos.data_sharing().automatic();
        reader_ = subscriber_->create_datareader(
            input_topic_, reader_qos, nullptr, dds::StatusMask::none());
        if (reader_ == nullptr) {
            throw std::runtime_error(
                "Fast DDS create_datareader returned nullptr");
        }
    }

    void CreateOutput(size_t edge) {
        publisher_ = participant_->create_publisher(
            dds::PUBLISHER_QOS_DEFAULT, nullptr, dds::StatusMask::none());
        if (publisher_ == nullptr) {
            throw std::runtime_error(
                "Fast DDS create_publisher returned nullptr");
        }
        output_topic_ = CreateTopic(edge);
        dds::DataWriterQos writer_qos = dds::DATAWRITER_QOS_DEFAULT;
        ConfigureDeliveryQos(&writer_qos, backend_.history_depth,
                             deadline_seconds_);
        writer_qos.publish_mode().kind = dds::SYNCHRONOUS_PUBLISH_MODE;
        writer_qos.data_sharing().automatic();
        writer_ = publisher_->create_datawriter(
            output_topic_, writer_qos, nullptr, dds::StatusMask::none());
        if (writer_ == nullptr) {
            throw std::runtime_error(
                "Fast DDS create_datawriter returned nullptr");
        }
    }

    void ResetEntityPointers() noexcept {
        participant_ = nullptr;
        publisher_ = nullptr;
        subscriber_ = nullptr;
        input_topic_ = nullptr;
        output_topic_ = nullptr;
        writer_ = nullptr;
        reader_ = nullptr;
    }

    void CloseBestEffort() noexcept {
        if (participant_ == nullptr) return;
        const dds::ReturnCode_t contained =
            participant_->delete_contained_entities();
        if (contained != dds::RETCODE_OK) {
            std::cerr << "Fast DDS delete_contained_entities failed: "
                      << ReturnCodeName(contained) << '\n';
        }
        const dds::ReturnCode_t participant =
            dds::DomainParticipantFactory::get_instance()->delete_participant(
                participant_);
        if (participant != dds::RETCODE_OK) {
            std::cerr << "Fast DDS delete_participant failed: "
                      << ReturnCodeName(participant) << '\n';
        } else {
            ResetEntityPointers();
        }
    }

    Role role_;
    uint64_t deadline_seconds_;
    BackendOptions backend_;
    std::array<std::string, 5> topics_;
    dds::TypeSupport type_;
    dds::DomainParticipant* participant_ = nullptr;
    dds::Publisher* publisher_ = nullptr;
    dds::Subscriber* subscriber_ = nullptr;
    dds::Topic* input_topic_ = nullptr;
    dds::Topic* output_topic_ = nullptr;
    dds::DataWriter* writer_ = nullptr;
    dds::DataReader* reader_ = nullptr;
};

void SemanticToTyped(const SemanticFrame& source,
                     AutonomyPipelineFrame* destination) {
    destination->sample_id(source.sample_id);
    destination->origin_timestamp_ns(source.origin_timestamp_ns);
    destination->perception_timestamp_ns(source.perception_timestamp_ns);
    destination->prediction_timestamp_ns(source.prediction_timestamp_ns);
    destination->planning_timestamp_ns(source.planning_timestamp_ns);
    destination->control_timestamp_ns(source.control_timestamp_ns);
    destination->guardian_timestamp_ns(source.guardian_timestamp_ns);
    destination->completed_stage_mask(source.completed_stage_mask);
    destination->profile(source.profile);
    destination->object_count(source.object_count);
    destination->trajectory_point_count(source.trajectory_point_count);
    destination->ego_speed_mps(source.ego_speed_mps);
    destination->steering_angle_rad(source.steering_angle_rad);
    destination->acceleration_mps2(source.acceleration_mps2);
    destination->brake_percentage(source.brake_percentage);
    destination->emergency_stop(source.emergency_stop);
    destination->payload_checksum(source.payload_checksum);
    destination->payload(source.payload);
}

bool TypedToSemantic(const AutonomyPipelineFrame& source,
                     SemanticFrame* destination, std::string* error) {
    if (source.payload().size() > kMaximumPayloadBytes) {
        *error = "Fast DDS typed payload exceeds the strict 1 MiB limit";
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
    destination->trajectory_point_count =
        source.trajectory_point_count();
    destination->ego_speed_mps = source.ego_speed_mps();
    destination->steering_angle_rad = source.steering_angle_rad();
    destination->acceleration_mps2 = source.acceleration_mps2();
    destination->brake_percentage = source.brake_percentage();
    destination->emergency_stop = source.emergency_stop();
    destination->payload_checksum = source.payload_checksum();
    destination->payload.assign(source.payload().begin(),
                                source.payload().end());
    return true;
}

void ValidateSequenceAndPhase(const CommonOptions& options,
                              const SemanticFrame& frame,
                              uint64_t expected_id,
                              RunStatistics* statistics) {
    if (frame.sample_id != expected_id) {
        if (frame.sample_id < expected_id) {
            ++statistics->duplicate;
            throw std::runtime_error(
                "duplicate sample_id: expected " +
                std::to_string(expected_id) + ", got " +
                std::to_string(frame.sample_id));
        }
        ++statistics->out_of_order;
        throw std::runtime_error(
            "out-of-order sample_id: expected " +
            std::to_string(expected_id) + ", got " +
            std::to_string(frame.sample_id));
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

SemanticFrame DecodeTyped(const AutonomyPipelineFrame& typed,
                          RunStatistics* statistics) {
    SemanticFrame frame;
    std::string error;
    if (!TypedToSemantic(typed, &frame, &error)) {
        ++statistics->corrupt;
        throw std::runtime_error(error);
    }
    return frame;
}

uint64_t TotalFrames(const CommonOptions& options) {
    return options.warmup_messages + options.messages;
}

void RecordEncodedSize(FastDdsPipeline* pipeline,
                       const AutonomyPipelineFrame& typed,
                       RunStatistics* statistics) {
    statistics->encoded_bytes_total += pipeline->SerializedSize(typed);
}

void RunSource(const CommonOptions& options, FastDdsPipeline* pipeline,
               uint64_t absolute_deadline_ns,
               RunStatistics* statistics) {
    const uint64_t total = TotalFrames(options);
    const uint64_t schedule_start_ns = NowNs();
    for (uint64_t sample_id = 0; sample_id < total; ++sample_id) {
        PaceSource(schedule_start_ns, sample_id, options.publish_interval_us,
                   absolute_deadline_ns);
        EnsureBeforeDeadline(absolute_deadline_ns,
                             "source frame initialization");
        const bool measured = sample_id >= options.warmup_messages;
        SemanticFrame frame =
            InitializeSourceFrame(sample_id, options.profile, measured);
        std::string error;
        if (!ApplyStageForClockMode(Role::kPerception, &frame,
                                    options.clock_mode, &error)) {
            throw std::runtime_error(
                "perception stage rejected source frame: " + error);
        }
        AutonomyPipelineFrame typed;
        SemanticToTyped(frame, &typed);
        pipeline->Write(typed, absolute_deadline_ns);
        if (measured) {
            RecordEncodedSize(pipeline, typed, statistics);
            ++statistics->measured_completed;
        }
    }
}

void RunForwarder(const CommonOptions& options, FastDdsPipeline* pipeline,
                  uint64_t absolute_deadline_ns,
                  RunStatistics* statistics) {
    const uint64_t total = TotalFrames(options);
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        const AutonomyPipelineFrame received =
            pipeline->Take(absolute_deadline_ns);
        SemanticFrame frame = DecodeTyped(received, statistics);
        ValidateSequenceAndPhase(options, frame, expected_id, statistics);
        std::string error;
        if (!ApplyStageForClockMode(options.role, &frame,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error(std::string(RoleName(options.role)) +
                                     " stage rejected frame: " + error);
        }
        AutonomyPipelineFrame outgoing;
        SemanticToTyped(frame, &outgoing);
        pipeline->Write(outgoing, absolute_deadline_ns);
        if (expected_id >= options.warmup_messages) {
            RecordEncodedSize(pipeline, outgoing, statistics);
            ++statistics->measured_completed;
        }
    }
}

void RunSink(const CommonOptions& options, FastDdsPipeline* pipeline,
             uint64_t absolute_deadline_ns,
             RunStatistics* statistics) {
    statistics->latencies_ns.reserve(static_cast<size_t>(std::min(
        options.messages, kMaximumInitialLatencyReserve)));
    const uint64_t total = TotalFrames(options);
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        const AutonomyPipelineFrame received =
            pipeline->Take(absolute_deadline_ns);
        SemanticFrame frame = DecodeTyped(received, statistics);
        ValidateSequenceAndPhase(options, frame, expected_id, statistics);
        std::string error;
        if (!ApplyStageForClockMode(Role::kCanbus, &frame,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error("canbus stage rejected frame: " + error);
        }
        if (expected_id < options.warmup_messages) continue;

        const uint64_t completion_ns = NowNs();
        if (completion_ns >= absolute_deadline_ns) {
            throw std::runtime_error(
                "deadline expired during sink frame processing");
        }
        if (statistics->measured_completed == 0) {
            statistics->first_measured_origin_ns = frame.origin_timestamp_ns;
            statistics->first_measured_completion_ns = completion_ns;
        }
        statistics->last_measured_completion_ns = completion_ns;
        if (options.clock_mode == ClockMode::kSameHost) {
            if (completion_ns < frame.origin_timestamp_ns) {
                ++statistics->corrupt;
                throw std::runtime_error(
                    "sink completion timestamp precedes frame origin");
            }
            statistics->latencies_ns.push_back(completion_ns -
                                               frame.origin_timestamp_ns);
        }
        // Serialized-size accounting is reporting-only and must remain outside
        // the end-to-end latency boundary.
        statistics->encoded_bytes_total += pipeline->SerializedSize(received);
        ++statistics->measured_completed;
    }
}

std::string BackendDetails(
    const BackendOptions& backend, uint64_t deadline_seconds,
    const std::array<std::string, 5>& topics) {
    std::string details =
        "{\"fastdds_pinned_version\":\"3.4.2.bcr.1\","
        "\"fastcdr_pinned_version\":\"2.3.5.bcr.0\","
        "\"idl_support\":\"FastDDSGen-compatible IDL support\","
        "\"generated_support\":"
        "\"template-reproduced-pending-regen-diff\","
        "\"domain_id\":" + std::to_string(backend.domain_id) +
        ",\"topics\":[";
    for (size_t edge = 0; edge < topics.size(); ++edge) {
        if (edge != 0) details += ',';
        details += "\"" + JsonEscape(topics[edge]) + "\"";
    }
    details +=
        "],\"qos\":{\"reliability\":\"reliable\","
        "\"durability\":\"volatile\","
        "\"history\":\"keep_all_bounded\","
        "\"publish_mode\":\"synchronous\","
        "\"data_sharing\":\"automatic\","
        "\"history_depth\":" + std::to_string(backend.history_depth) +
        ",\"resource_limits\":{\"max_samples\":" +
        std::to_string(backend.history_depth) +
        ",\"max_instances\":1,\"max_samples_per_instance\":" +
        std::to_string(backend.history_depth) +
        "},\"max_blocking_time_ms\":" +
        std::to_string(deadline_seconds * 1000u) + "},"
        "\"discovery\":\"default builtin UDP discovery\","
        "\"transport\":"
        "\"Fast DDS DEFAULT builtin UDPv4 plus intrahost SHM; not SHM-only\","
        "\"shm_only\":false,"
        "\"encoded_bytes_metric\":"
        "\"average generated TopicDataType XCDR1 serialized size\"}";
    return details;
}

void PopulateResult(const CommonOptions& options,
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
    if (options.clock_mode == ClockMode::kIndependentHosts) {
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
        std::cerr << "failed to write result artifact "
                  << result.options.output << ": " << exception.what() << '\n';
    } catch (...) {
        std::cerr << "failed to write result artifact "
                  << result.options.output << ": unknown exception\n";
    }
}

int PipelineMain(int argc, char** argv) {
    std::optional<CommonOptions> parsed_options;
    BackendOptions backend_options;
    RunStatistics statistics;
    SinkResult result;
    std::array<std::string, 5> topics;
    try {
        parsed_options = ParseCommonOptions(argc, argv);
        const CommonOptions& options = *parsed_options;
        result.backend = std::string(kBackend);
        result.options = options;
        result.payload_bytes = ProfilePayloadBytes(options.profile);

        backend_options = ParseBackendOptions(argc, argv);
        topics = TopicNames(options.run_id);
        result.backend_details = BackendDetails(
            backend_options, options.deadline_seconds, topics);
        const uint64_t absolute_deadline_ns = AbsoluteDeadline(options);
        FastDdsPipeline pipeline(options, backend_options, topics);
        pipeline.WaitForExpectedMatches(absolute_deadline_ns);

        WriteReadyFile(options.runtime_dir, kBackend, options.role,
                       options.run_id);
        if (!WaitForStartFile(options.runtime_dir, options.run_id,
                              absolute_deadline_ns)) {
            throw std::runtime_error(
                "deadline expired waiting for start file");
        }

        switch (options.role) {
            case Role::kPerception:
                RunSource(options, &pipeline, absolute_deadline_ns,
                          &statistics);
                break;
            case Role::kPrediction:
            case Role::kPlanning:
            case Role::kControl:
            case Role::kGuardian:
                RunForwarder(options, &pipeline, absolute_deadline_ns,
                             &statistics);
                break;
            case Role::kCanbus:
                RunSink(options, &pipeline, absolute_deadline_ns,
                        &statistics);
                break;
        }

        if (statistics.measured_completed != options.messages) {
            throw std::runtime_error(
                "completed measured-frame count mismatch");
        }
        pipeline.WaitForAcknowledgments(absolute_deadline_ns);
        pipeline.CloseOrThrow();
        PopulateResult(options, statistics, true, &result);
        WriteSinkResult(result);
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << kBackend << " pipeline failed: " << exception.what()
                  << '\n';
        if (parsed_options.has_value()) {
            if (result.backend.empty()) result.backend = std::string(kBackend);
            result.options = *parsed_options;
            result.payload_bytes = ProfilePayloadBytes(result.options.profile);
            if (topics[0].empty()) topics = TopicNames(result.options.run_id);
            if (result.backend_details.empty()) {
                result.backend_details = BackendDetails(
                    backend_options, result.options.deadline_seconds, topics);
            }
            PopulateResult(result.options, statistics, false, &result);
            result.outcome = "failure";
            result.error = exception.what();
            WriteResultBestEffort(result);
        }
        return 1;
    } catch (...) {
        std::cerr << kBackend << " pipeline failed: unknown exception\n";
        if (parsed_options.has_value()) {
            if (result.backend.empty()) result.backend = std::string(kBackend);
            result.options = *parsed_options;
            result.payload_bytes = ProfilePayloadBytes(result.options.profile);
            if (topics[0].empty()) topics = TopicNames(result.options.run_id);
            if (result.backend_details.empty()) {
                result.backend_details = BackendDetails(
                    backend_options, result.options.deadline_seconds, topics);
            }
            PopulateResult(result.options, statistics, false, &result);
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
