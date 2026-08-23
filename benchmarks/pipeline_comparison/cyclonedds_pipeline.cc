// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/cyclonedds_generated/autonomy_pipeline.h"
#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <dds/cdr/dds_cdrstream.h>
#include <dds/dds.h>

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
#include <thread>
#include <utility>
#include <vector>

namespace mino::benchmarks::pipeline {
namespace {

using DdsFrame = mino_benchmarks_pipeline_AutonomyPipelineFrame;

constexpr std::string_view kBackend = "cyclonedds-idl";
constexpr uint32_t kDefaultDomainId = 73;
constexpr int32_t kDefaultHistoryDepth = 64;
constexpr uint32_t kMaximumDomainId = 232;
constexpr int32_t kMinimumHistoryDepth = 2;
constexpr int32_t kMaximumHistoryDepth = 4096;
constexpr size_t kMaximumPayloadBytes = kLargePayloadBytes;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr uint64_t kMatchingPollNanoseconds = 10'000'000ull;
constexpr uint64_t kWriteBlockingNanoseconds = 1'000'000ull;
constexpr uint64_t kMaximumInitialLatencyReserve = 1'000'000;

struct BackendOptions {
    uint32_t domain_id = kDefaultDomainId;
    int32_t history_depth = kDefaultHistoryDepth;
};

struct RunStatistics {
    uint64_t measured_completed = 0;
    uint64_t encoded_bytes_per_message = 0;
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

dds_duration_t RemainingDuration(uint64_t absolute_deadline_ns) {
    const uint64_t remaining = RemainingNanoseconds(absolute_deadline_ns);
    return remaining > static_cast<uint64_t>(std::numeric_limits<dds_duration_t>::max())
               ? std::numeric_limits<dds_duration_t>::max()
               : static_cast<dds_duration_t>(remaining);
}

std::string DdsError(dds_return_t code) {
    if (code >= 0) return "DDS_RETCODE_OK";
    const char* description = dds_strretcode(-code);
    return description == nullptr ? "unknown Cyclone DDS error"
                                  : std::string(description);
}

void RequireEntity(dds_entity_t entity, std::string_view operation) {
    if (entity < 0) {
        throw std::runtime_error(std::string(operation) + " failed: " +
                                 DdsError(entity));
    }
}

void RequireOk(dds_return_t code, std::string_view operation) {
    if (code != DDS_RETCODE_OK) {
        throw std::runtime_error(std::string(operation) + " failed: " +
                                 DdsError(code));
    }
}

void EnsureBeforeDeadline(uint64_t deadline, std::string_view operation) {
    if (RemainingNanoseconds(deadline) == 0) {
        throw std::runtime_error("deadline expired before " +
                                 std::string(operation));
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
    const std::string prefix =
        "mino_pipeline_" + SafeRunToken(run_id) + "_edge_";
    std::array<std::string, 5> topics;
    for (size_t edge = 0; edge < topics.size(); ++edge) {
        topics[edge] = prefix + std::to_string(edge);
    }
    return topics;
}

uint64_t ParseUnsignedBackendValue(std::string_view value,
                                   std::string_view option) {
    uint64_t parsed = 0;
    const auto conversion =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
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
        if (const auto value =
                BackendOptionValue(&index, argc, argv, "--domain-id")) {
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

class Qos final {
  public:
    Qos(int32_t history_depth, bool writer) : value_(dds_create_qos()) {
        if (value_ == nullptr) throw std::bad_alloc();
        dds_qset_reliability(
            value_, DDS_RELIABILITY_RELIABLE,
            static_cast<dds_duration_t>(kWriteBlockingNanoseconds));
        dds_qset_durability(value_, DDS_DURABILITY_VOLATILE);
        dds_qset_history(value_, DDS_HISTORY_KEEP_ALL, 0);
        dds_qset_resource_limits(value_, history_depth, 1, history_depth);
        const dds_data_representation_id_t representation =
            DDS_DATA_REPRESENTATION_XCDR1;
        dds_qset_data_representation(value_, 1, &representation);
        if (writer) dds_qset_writer_batching(value_, false);
    }

    Qos(const Qos&) = delete;
    Qos& operator=(const Qos&) = delete;
    ~Qos() { dds_delete_qos(value_); }

    const dds_qos_t* get() const noexcept { return value_; }

  private:
    dds_qos_t* value_ = nullptr;
};

class SampleLoan final {
  public:
    SampleLoan(dds_entity_t reader, void** samples, int32_t count) noexcept
        : reader_(reader), samples_(samples), count_(count) {}
    SampleLoan(const SampleLoan&) = delete;
    SampleLoan& operator=(const SampleLoan&) = delete;
    ~SampleLoan() {
        if (count_ > 0) {
            const dds_return_t code = dds_return_loan(reader_, samples_, count_);
            if (code != DDS_RETCODE_OK) {
                std::cerr << "Cyclone DDS dds_return_loan failed: "
                          << DdsError(code) << '\n';
            }
        }
    }

    void ReturnOrThrow() {
        if (count_ <= 0) return;
        const dds_return_t code = dds_return_loan(reader_, samples_, count_);
        count_ = 0;
        RequireOk(code, "Cyclone DDS dds_return_loan");
    }

  private:
    dds_entity_t reader_ = DDS_ENTITY_NIL;
    void** samples_ = nullptr;
    int32_t count_ = 0;
};

class CycloneDdsPipeline final {
  public:
    CycloneDdsPipeline(const CommonOptions& common,
                       const BackendOptions& backend,
                       const std::array<std::string, 5>& topics)
        : role_(common.role),
          backend_(backend),
          topics_(topics) {
        try {
            Initialize();
        } catch (...) {
            CloseBestEffort();
            throw;
        }
    }

    CycloneDdsPipeline(const CycloneDdsPipeline&) = delete;
    CycloneDdsPipeline& operator=(const CycloneDdsPipeline&) = delete;
    ~CycloneDdsPipeline() { CloseBestEffort(); }

    void WaitForExpectedMatches(uint64_t absolute_deadline_ns) {
        while (true) {
            bool input_matched = reader_ == DDS_ENTITY_NIL;
            bool output_matched = writer_ == DDS_ENTITY_NIL;
            if (reader_ != DDS_ENTITY_NIL) {
                const dds_return_t count =
                    dds_get_matched_publications(reader_, nullptr, 0);
                if (count < 0) {
                    throw std::runtime_error(
                        "Cyclone DDS get matched publications failed: " +
                        DdsError(count));
                }
                input_matched = count >= 1;
            }
            if (writer_ != DDS_ENTITY_NIL) {
                const dds_return_t count =
                    dds_get_matched_subscriptions(writer_, nullptr, 0);
                if (count < 0) {
                    throw std::runtime_error(
                        "Cyclone DDS get matched subscriptions failed: " +
                        DdsError(count));
                }
                output_matched = count >= 1;
            }
            if (input_matched && output_matched) return;
            const uint64_t remaining =
                RemainingNanoseconds(absolute_deadline_ns);
            if (remaining == 0) {
                throw std::runtime_error(
                    "deadline expired waiting for Cyclone DDS endpoint matches");
            }
            std::this_thread::sleep_for(std::chrono::nanoseconds(
                std::min(remaining, kMatchingPollNanoseconds)));
        }
    }

    void Write(const DdsFrame& sample, uint64_t absolute_deadline_ns) {
        if (writer_ == DDS_ENTITY_NIL) {
            throw std::logic_error("pipeline role has no Cyclone DDS writer");
        }
        while (true) {
            EnsureBeforeDeadline(absolute_deadline_ns, "Cyclone DDS write");
            const dds_return_t code = dds_write(writer_, &sample);
            if (code == DDS_RETCODE_OK) {
                EnsureBeforeDeadline(absolute_deadline_ns,
                                     "completion of Cyclone DDS write");
                return;
            }
            if (code != DDS_RETCODE_TIMEOUT) {
                throw std::runtime_error("Cyclone DDS dds_write failed: " +
                                         DdsError(code));
            }
            EnsureBeforeDeadline(absolute_deadline_ns,
                                 "retry of timed-out Cyclone DDS write");
        }
    }

    DdsFrame Take(uint64_t absolute_deadline_ns) {
        if (reader_ == DDS_ENTITY_NIL || waitset_ == DDS_ENTITY_NIL) {
            throw std::logic_error("pipeline role has no Cyclone DDS reader");
        }
        while (true) {
            const dds_duration_t remaining =
                RemainingDuration(absolute_deadline_ns);
            if (remaining <= 0) {
                throw std::runtime_error(
                    "deadline expired waiting for a Cyclone DDS sample");
            }
            dds_attach_t triggered = 0;
            const dds_return_t waited =
                dds_waitset_wait(waitset_, &triggered, 1, remaining);
            if (waited < 0) {
                throw std::runtime_error("Cyclone DDS waitset failed: " +
                                         DdsError(waited));
            }
            if (waited == 0) {
                throw std::runtime_error(
                    "deadline expired waiting for a Cyclone DDS sample");
            }

            void* samples[1] = {nullptr};
            dds_sample_info_t infos[1]{};
            const dds_return_t count =
                dds_take(reader_, samples, infos, 1, 1);
            if (count < 0) {
                throw std::runtime_error("Cyclone DDS dds_take failed: " +
                                         DdsError(count));
            }
            if (count == 0) continue;
            SampleLoan loan(reader_, samples, count);
            if (!infos[0].valid_data) {
                loan.ReturnOrThrow();
                continue;
            }
            const auto* received = static_cast<const DdsFrame*>(samples[0]);
            DdsFrame result{};
            result.sample_id = received->sample_id;
            result.origin_timestamp_ns = received->origin_timestamp_ns;
            result.perception_timestamp_ns = received->perception_timestamp_ns;
            result.prediction_timestamp_ns = received->prediction_timestamp_ns;
            result.planning_timestamp_ns = received->planning_timestamp_ns;
            result.control_timestamp_ns = received->control_timestamp_ns;
            result.guardian_timestamp_ns = received->guardian_timestamp_ns;
            result.completed_stage_mask = received->completed_stage_mask;
            result.profile = received->profile;
            result.object_count = received->object_count;
            result.trajectory_point_count = received->trajectory_point_count;
            result.ego_speed_mps = received->ego_speed_mps;
            result.steering_angle_rad = received->steering_angle_rad;
            result.acceleration_mps2 = received->acceleration_mps2;
            result.brake_percentage = received->brake_percentage;
            result.emergency_stop = received->emergency_stop;
            result.payload_checksum = received->payload_checksum;
            if (received->payload._length > kMaximumPayloadBytes ||
                (received->payload._length != 0 &&
                 received->payload._buffer == nullptr)) {
                throw std::runtime_error(
                    "Cyclone DDS typed payload is invalid or exceeds 1 MiB");
            }
            owned_payload_.assign(
                received->payload._buffer,
                received->payload._buffer + received->payload._length);
            result.payload._maximum =
                static_cast<uint32_t>(owned_payload_.size());
            result.payload._length =
                static_cast<uint32_t>(owned_payload_.size());
            result.payload._buffer = owned_payload_.data();
            result.payload._release = false;
            loan.ReturnOrThrow();
            EnsureBeforeDeadline(absolute_deadline_ns,
                                 "completion of Cyclone DDS take");
            return result;
        }
    }

    uint32_t SerializedSize(const DdsFrame& sample) const {
        const size_t body_size = dds_stream_getsize_sample(
            reinterpret_cast<const char*>(&sample),
            &mino_benchmarks_pipeline_AutonomyPipelineFrame_cdrstream_desc,
            DDSI_RTPS_CDR_ENC_VERSION_1);
        if (body_size == SIZE_MAX ||
            body_size > std::numeric_limits<uint32_t>::max() - 4u) {
            throw std::runtime_error(
                "Cyclone DDS XCDR1 serialized-size calculation failed");
        }
        return static_cast<uint32_t>(body_size + 4u);
    }

    void WaitForAcknowledgments(uint64_t absolute_deadline_ns) {
        if (writer_ == DDS_ENTITY_NIL) return;
        const dds_duration_t remaining =
            RemainingDuration(absolute_deadline_ns);
        if (remaining <= 0) {
            throw std::runtime_error(
                "deadline expired before Cyclone DDS acknowledgment wait");
        }
        RequireOk(dds_wait_for_acks(writer_, remaining),
                  "Cyclone DDS dds_wait_for_acks");
    }

    void CloseOrThrow() {
        if (participant_ == DDS_ENTITY_NIL) return;
        const dds_return_t code = dds_delete(participant_);
        participant_ = DDS_ENTITY_NIL;
        reader_ = DDS_ENTITY_NIL;
        writer_ = DDS_ENTITY_NIL;
        waitset_ = DDS_ENTITY_NIL;
        RequireOk(code, "Cyclone DDS participant cleanup");
    }

  private:
    void Initialize() {
        participant_ = dds_create_participant(backend_.domain_id, nullptr, nullptr);
        RequireEntity(participant_, "Cyclone DDS dds_create_participant");
        if (const std::optional<size_t> input = InputEdge(role_); input.has_value()) {
            CreateInput(*input);
        }
        if (const std::optional<size_t> output = OutputEdge(role_);
            output.has_value()) {
            CreateOutput(*output);
        }
    }

    dds_entity_t CreateTopic(size_t edge) {
        Qos qos(backend_.history_depth, false);
        const dds_entity_t topic = dds_create_topic(
            participant_,
            &mino_benchmarks_pipeline_AutonomyPipelineFrame_desc,
            topics_[edge].c_str(), qos.get(), nullptr);
        RequireEntity(topic, "Cyclone DDS dds_create_topic");
        return topic;
    }

    void CreateInput(size_t edge) {
        const dds_entity_t topic = CreateTopic(edge);
        Qos qos(backend_.history_depth, false);
        reader_ = dds_create_reader(participant_, topic, qos.get(), nullptr);
        RequireEntity(reader_, "Cyclone DDS dds_create_reader");
        read_condition_ = dds_create_readcondition(reader_, DDS_ANY_STATE);
        RequireEntity(read_condition_,
                      "Cyclone DDS dds_create_readcondition");
        waitset_ = dds_create_waitset(participant_);
        RequireEntity(waitset_, "Cyclone DDS dds_create_waitset");
        RequireOk(dds_waitset_attach(waitset_, read_condition_, 1),
                  "Cyclone DDS dds_waitset_attach");
    }

    void CreateOutput(size_t edge) {
        const dds_entity_t topic = CreateTopic(edge);
        Qos qos(backend_.history_depth, true);
        writer_ = dds_create_writer(participant_, topic, qos.get(), nullptr);
        RequireEntity(writer_, "Cyclone DDS dds_create_writer");
    }

    void CloseBestEffort() noexcept {
        if (participant_ == DDS_ENTITY_NIL) return;
        const dds_return_t code = dds_delete(participant_);
        if (code != DDS_RETCODE_OK) {
            std::cerr << "Cyclone DDS participant cleanup failed: "
                      << DdsError(code) << '\n';
        }
        participant_ = DDS_ENTITY_NIL;
        reader_ = DDS_ENTITY_NIL;
        writer_ = DDS_ENTITY_NIL;
        read_condition_ = DDS_ENTITY_NIL;
        waitset_ = DDS_ENTITY_NIL;
    }

    Role role_;
    BackendOptions backend_;
    std::array<std::string, 5> topics_;
    dds_entity_t participant_ = DDS_ENTITY_NIL;
    dds_entity_t reader_ = DDS_ENTITY_NIL;
    dds_entity_t writer_ = DDS_ENTITY_NIL;
    dds_entity_t read_condition_ = DDS_ENTITY_NIL;
    dds_entity_t waitset_ = DDS_ENTITY_NIL;
    std::vector<uint8_t> owned_payload_;
};

void SemanticToTyped(const SemanticFrame& source, DdsFrame* destination) {
    if (source.payload.size() > kMaximumPayloadBytes ||
        source.payload.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("semantic payload exceeds Cyclone DDS IDL bound");
    }
    *destination = DdsFrame{};
    destination->sample_id = source.sample_id;
    destination->origin_timestamp_ns = source.origin_timestamp_ns;
    destination->perception_timestamp_ns = source.perception_timestamp_ns;
    destination->prediction_timestamp_ns = source.prediction_timestamp_ns;
    destination->planning_timestamp_ns = source.planning_timestamp_ns;
    destination->control_timestamp_ns = source.control_timestamp_ns;
    destination->guardian_timestamp_ns = source.guardian_timestamp_ns;
    destination->completed_stage_mask = source.completed_stage_mask;
    destination->profile = source.profile;
    destination->object_count = source.object_count;
    destination->trajectory_point_count = source.trajectory_point_count;
    destination->ego_speed_mps = source.ego_speed_mps;
    destination->steering_angle_rad = source.steering_angle_rad;
    destination->acceleration_mps2 = source.acceleration_mps2;
    destination->brake_percentage = source.brake_percentage;
    destination->emergency_stop = source.emergency_stop;
    destination->payload_checksum = source.payload_checksum;
    destination->payload._maximum =
        static_cast<uint32_t>(source.payload.size());
    destination->payload._length =
        static_cast<uint32_t>(source.payload.size());
    destination->payload._buffer =
        const_cast<uint8_t*>(source.payload.data());
    destination->payload._release = false;
}

SemanticFrame TypedToSemantic(const DdsFrame& source) {
    if (source.payload._length > kMaximumPayloadBytes ||
        (source.payload._length != 0 && source.payload._buffer == nullptr)) {
        throw std::runtime_error(
            "Cyclone DDS typed payload is invalid or exceeds 1 MiB");
    }
    SemanticFrame destination;
    destination.sample_id = source.sample_id;
    destination.origin_timestamp_ns = source.origin_timestamp_ns;
    destination.perception_timestamp_ns = source.perception_timestamp_ns;
    destination.prediction_timestamp_ns = source.prediction_timestamp_ns;
    destination.planning_timestamp_ns = source.planning_timestamp_ns;
    destination.control_timestamp_ns = source.control_timestamp_ns;
    destination.guardian_timestamp_ns = source.guardian_timestamp_ns;
    destination.completed_stage_mask = source.completed_stage_mask;
    destination.profile = source.profile;
    destination.object_count = source.object_count;
    destination.trajectory_point_count = source.trajectory_point_count;
    destination.ego_speed_mps = source.ego_speed_mps;
    destination.steering_angle_rad = source.steering_angle_rad;
    destination.acceleration_mps2 = source.acceleration_mps2;
    destination.brake_percentage = source.brake_percentage;
    destination.emergency_stop = source.emergency_stop;
    destination.payload_checksum = source.payload_checksum;
    destination.payload.assign(source.payload._buffer,
                               source.payload._buffer + source.payload._length);
    return destination;
}

void ValidateSequenceAndPhase(const CommonOptions& options,
                              const SemanticFrame& frame,
                              uint64_t expected_id,
                              RunStatistics* statistics) {
    if (frame.sample_id != expected_id) {
        if (frame.sample_id < expected_id) {
            ++statistics->duplicate;
            throw std::runtime_error(
                "duplicate sample_id: expected " + std::to_string(expected_id) +
                ", got " + std::to_string(frame.sample_id));
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

uint64_t TotalFrames(const CommonOptions& options) {
    return options.warmup_messages + options.messages;
}

void RecordEncodedSizeOnce(CycloneDdsPipeline* pipeline,
                           const DdsFrame& sample,
                           RunStatistics* statistics) {
    if (statistics->encoded_bytes_per_message == 0) {
        statistics->encoded_bytes_per_message =
            pipeline->SerializedSize(sample);
    }
}

void RunSource(const CommonOptions& options, CycloneDdsPipeline* pipeline,
               uint64_t deadline, RunStatistics* statistics) {
    const uint64_t total = TotalFrames(options);
    const uint64_t schedule_start_ns = NowNs();
    for (uint64_t sample_id = 0; sample_id < total; ++sample_id) {
        PaceSource(schedule_start_ns, sample_id, options.publish_interval_us,
                   deadline);
        const bool measured = sample_id >= options.warmup_messages;
        SemanticFrame frame =
            InitializeSourceFrame(sample_id, options.profile, measured);
        std::string error;
        if (!ApplyStageForClockMode(Role::kPerception, &frame,
                                    options.clock_mode, &error)) {
            throw std::runtime_error(
                "perception stage rejected source frame: " + error);
        }
        DdsFrame typed{};
        SemanticToTyped(frame, &typed);
        pipeline->Write(typed, deadline);
        if (measured) {
            RecordEncodedSizeOnce(pipeline, typed, statistics);
            ++statistics->measured_completed;
        }
    }
}

void RunForwarder(const CommonOptions& options,
                  CycloneDdsPipeline* pipeline, uint64_t deadline,
                  RunStatistics* statistics) {
    const uint64_t total = TotalFrames(options);
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        const DdsFrame received = pipeline->Take(deadline);
        SemanticFrame frame = TypedToSemantic(received);
        ValidateSequenceAndPhase(options, frame, expected_id, statistics);
        std::string error;
        if (!ApplyStageForClockMode(options.role, &frame,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error(std::string(RoleName(options.role)) +
                                     " stage rejected frame: " + error);
        }
        DdsFrame outgoing{};
        SemanticToTyped(frame, &outgoing);
        pipeline->Write(outgoing, deadline);
        if (expected_id >= options.warmup_messages) {
            RecordEncodedSizeOnce(pipeline, outgoing, statistics);
            ++statistics->measured_completed;
        }
    }
}

void RunSink(const CommonOptions& options, CycloneDdsPipeline* pipeline,
             uint64_t deadline, RunStatistics* statistics) {
    statistics->latencies_ns.reserve(static_cast<size_t>(std::min(
        options.messages, kMaximumInitialLatencyReserve)));
    const uint64_t total = TotalFrames(options);
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        const DdsFrame received = pipeline->Take(deadline);
        SemanticFrame frame = TypedToSemantic(received);
        ValidateSequenceAndPhase(options, frame, expected_id, statistics);
        std::string error;
        if (!ApplyStageForClockMode(Role::kCanbus, &frame,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error("canbus stage rejected frame: " + error);
        }
        if (expected_id < options.warmup_messages) continue;

        const uint64_t completion_ns = NowNs();
        if (completion_ns >= deadline) {
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
        // The profile fixes the payload length, so every XCDR1 sample has the
        // same encoded size; calculate this reporting metric only once.
        RecordEncodedSizeOnce(pipeline, received, statistics);
        ++statistics->measured_completed;
    }
}

std::string BackendDetails(
    const BackendOptions& backend,
    const std::array<std::string, 5>& topics) {
    std::string details =
        "{\"cyclonedds_pinned_version\":\"11.0.1\","
        "\"idl_support\":\"Cyclone DDS idlc generated C type\","
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
        "\"writer_batching\":false,"
        "\"data_representation\":\"xcdr1\","
        "\"history_depth\":" + std::to_string(backend.history_depth) +
        ",\"resource_limits\":{\"max_samples\":" +
        std::to_string(backend.history_depth) +
        ",\"max_instances\":1,\"max_samples_per_instance\":" +
        std::to_string(backend.history_depth) +
        "},\"max_blocking_time_ms\":" +
        std::to_string(kWriteBlockingNanoseconds / 1'000'000u) + "},"
        "\"discovery\":\"Cyclone DDS default SPDP or CYCLONEDDS_URI\","
        "\"transport\":\"Cyclone DDS default UDP; no PSMX target in BCR overlay\","
        "\"shm_only\":false,"
        "\"encoded_bytes_metric\":\"generated XCDR1 CDR stream size including 4-byte encapsulation\"}";
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
        result->encoded_bytes = statistics.encoded_bytes_per_message;
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
        std::cerr << "failed to write result artifact " << result.options.output
                  << ": " << exception.what() << '\n';
    } catch (...) {
        std::cerr << "failed to write result artifact " << result.options.output
                  << ": unknown exception\n";
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
        result.backend_details = BackendDetails(backend_options, topics);
        const uint64_t deadline = AbsoluteDeadline(options);
        CycloneDdsPipeline pipeline(options, backend_options, topics);
        pipeline.WaitForExpectedMatches(deadline);

        WriteReadyFile(options.runtime_dir, kBackend, options.role,
                       options.run_id);
        if (!WaitForStartFile(options.runtime_dir, options.run_id, deadline)) {
            throw std::runtime_error(
                "deadline expired waiting for start file");
        }

        switch (options.role) {
            case Role::kPerception:
                RunSource(options, &pipeline, deadline, &statistics);
                break;
            case Role::kPrediction:
            case Role::kPlanning:
            case Role::kControl:
            case Role::kGuardian:
                RunForwarder(options, &pipeline, deadline, &statistics);
                break;
            case Role::kCanbus:
                RunSink(options, &pipeline, deadline, &statistics);
                break;
        }

        if (statistics.measured_completed != options.messages) {
            throw std::runtime_error(
                "completed measured-frame count mismatch");
        }
        pipeline.WaitForAcknowledgments(deadline);
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
                result.backend_details = BackendDetails(backend_options, topics);
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
                result.backend_details = BackendDetails(backend_options, topics);
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
