// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_PIPELINE_COMPARISON_PIPELINE_COMMON_H_
#define BENCHMARKS_PIPELINE_COMPARISON_PIPELINE_COMMON_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace mino::benchmarks::pipeline {

enum class Role : uint8_t {
    kPerception = 0,
    kPrediction = 1,
    kPlanning = 2,
    kControl = 3,
    kGuardian = 4,
    kCanbus = 5,
};

constexpr uint32_t kPerceptionStageBit = 1u << 0;
constexpr uint32_t kPredictionStageBit = 1u << 1;
constexpr uint32_t kPlanningStageBit = 1u << 2;
constexpr uint32_t kControlStageBit = 1u << 3;
constexpr uint32_t kGuardianStageBit = 1u << 4;
constexpr uint32_t kFinalStageMask = kPerceptionStageBit |
                                     kPredictionStageBit |
                                     kPlanningStageBit | kControlStageBit |
                                     kGuardianStageBit;

std::optional<Role> ParseRole(std::string_view name);
std::string_view RoleName(Role role);
std::optional<Role> NextRole(Role role);
// CANBus is the validating sink, not a forwarding stage, and therefore has no
// bit in the frame transported by the backends.
uint32_t RoleBit(Role role);
// The exact completed_stage_mask required on entry to role.
uint32_t ExpectedMask(Role role);

enum class Profile : uint32_t {
    kSmall = 0,
    kMedium = 1,
    kLarge = 2,
};

enum class ClockMode : uint8_t {
    kSameHost = 0,
    kIndependentHosts = 1,
};

std::string_view ClockModeName(ClockMode mode);
std::string_view CompilationMode();

constexpr size_t kSmallPayloadBytes = 256;
constexpr size_t kMediumPayloadBytes = 65'536;
constexpr size_t kLargePayloadBytes = 1'048'576;

std::optional<Profile> ParseProfile(std::string_view name);
std::string_view ProfileName(Profile profile);
size_t ProfilePayloadBytes(Profile profile);

// One-to-one C++ representation of every field in autonomy_pipeline.proto.
struct SemanticFrame {
    uint64_t sample_id = 0;
    uint64_t origin_timestamp_ns = 0;
    uint64_t perception_timestamp_ns = 0;
    uint64_t prediction_timestamp_ns = 0;
    uint64_t planning_timestamp_ns = 0;
    uint64_t control_timestamp_ns = 0;
    uint64_t guardian_timestamp_ns = 0;
    uint32_t completed_stage_mask = 0;
    uint32_t profile = 0;
    uint32_t object_count = 0;
    uint32_t trajectory_point_count = 0;
    double ego_speed_mps = 0.0;
    double steering_angle_rad = 0.0;
    double acceleration_mps2 = 0.0;
    double brake_percentage = 0.0;
    bool emergency_stop = false;
    uint64_t payload_checksum = 0;
    std::vector<uint8_t> payload;
};

void FillDeterministicPayload(uint64_t sample_id, Profile profile,
                              std::vector<uint8_t>* payload);
std::vector<uint8_t> MakeDeterministicPayload(uint64_t sample_id,
                                              Profile profile);
uint64_t StablePayloadChecksum(std::span<const uint8_t> payload);
bool ValidateDeterministicPayload(uint64_t sample_id, Profile profile,
                                  std::span<const uint8_t> payload,
                                  std::string* error);

// measured=false creates a warmup frame whose origin timestamp is zero.
// For a measured frame, the origin timestamp is taken before semantic fields
// and payload are constructed, so construction and publication are inside the
// end-to-end timing boundary.
SemanticFrame InitializeSourceFrame(uint64_t sample_id, Profile profile,
                                    bool measured);
// Deterministic/test-friendly form. An origin of zero denotes warmup.
SemanticFrame InitializeSourceFrameAt(uint64_t sample_id, Profile profile,
                                      uint64_t origin_timestamp_ns);

bool ValidateSemanticFrame(const SemanticFrame& frame, std::string* error);
bool ValidateSemanticFrame(const SemanticFrame& frame,
                           std::span<const uint8_t> payload,
                           std::string* error);
// Bridge-only validation for canonical transit. This deliberately validates
// transport structure, sequencing, phase, profile, exact payload size, stage
// mask, and timestamp shape without re-running deterministic business-field,
// checksum, or payload-pattern checks performed by the six business stages.
bool ValidateBridgeTransitFrame(const SemanticFrame& frame,
                                uint64_t expected_sequence,
                                uint64_t warmup_messages,
                                Profile expected_profile,
                                Role destination_role, ClockMode clock_mode,
                                std::string* error);
bool ValidateFrameForStage(Role role, const SemanticFrame& frame,
                           std::string* error);
bool ValidateFrameForStage(Role role, const SemanticFrame& frame,
                           std::span<const uint8_t> payload,
                           std::string* error);
// Explicit timestamp form is useful when the backend timestamps immediately
// after receive/deserialize. CANBus validates only and leaves frame unchanged.
bool ApplyStage(Role role, SemanticFrame* frame, uint64_t timestamp_ns,
                std::string* error);
bool ApplyStage(Role role, SemanticFrame* frame, std::string* error);
bool ApplyStage(Role role, SemanticFrame* frame,
                std::span<const uint8_t> payload, uint64_t timestamp_ns,
                std::string* error);
bool ApplyStage(Role role, SemanticFrame* frame,
                std::span<const uint8_t> payload, std::string* error);
// Independent-host mode validates timestamp presence but never orders or
// subtracts CLOCK_MONOTONIC values from different boot clock domains.
bool ApplyStageForClockMode(Role role, SemanticFrame* frame, ClockMode mode,
                            std::string* error);
bool ApplyStageForClockMode(Role role, SemanticFrame* frame,
                            std::span<const uint8_t> payload, ClockMode mode,
                            std::string* error);

struct CommonOptions {
    Role role = Role::kPerception;
    Profile profile = Profile::kSmall;
    uint64_t messages = 10'000;
    uint64_t warmup_messages = 1'000;
    // Zero means saturation mode. A positive interval paces source emission and
    // keeps serialization/transport latency separate from queue buildup.
    uint64_t publish_interval_us = 0;
    uint64_t deadline_seconds = 30;
    ClockMode clock_mode = ClockMode::kSameHost;
    std::string run_id;
    std::filesystem::path runtime_dir;
    std::filesystem::path output;
};

constexpr uint64_t kMaxMessages = 1'000'000'000;
constexpr uint64_t kMaxWarmupMessages = 1'000'000'000;
constexpr uint64_t kMaxPublishIntervalUs = 60'000'000;
constexpr uint64_t kMaxDeadlineSeconds = 86'400;

// Unknown arguments are deliberately ignored so each backend can parse its own
// options from the same argv. Recognized common options are always strict.
CommonOptions ParseCommonOptions(int argc, char** argv);
void ValidateCommonOptions(const CommonOptions& options);

// Uses CLOCK_MONOTONIC_RAW where the platform exposes it. Otherwise all three
// functions consistently use CLOCK_MONOTONIC and ClockName makes that fallback
// explicit.
uint64_t NowNs();
// Waits until schedule_start_ns + sample_id * publish_interval_us. A zero
// interval is a no-op. Throws if the schedule overflows or crosses the absolute
// process deadline.
void PaceSource(uint64_t schedule_start_ns, uint64_t sample_id,
                uint64_t publish_interval_us,
                uint64_t absolute_deadline_ns);
std::string_view ClockName();
uint64_t ClockResolutionNs();
std::string ReadBootId();

struct Distribution {
    size_t samples = 0;
    uint64_t p50 = 0;
    uint64_t p95 = 0;
    uint64_t p99 = 0;
    uint64_t p99_9 = 0;
    uint64_t maximum = 0;
};

Distribution Summarize(std::vector<uint64_t> samples);

struct ResultCounts {
    uint64_t offered = 0;
    uint64_t received = 0;
    uint64_t duplicate = 0;
    uint64_t out_of_order = 0;
    uint64_t corrupt = 0;
    uint64_t lost = 0;
};

struct SinkResult {
    std::string backend;
    CommonOptions options;
    ResultCounts counts;
    Distribution latency_ns;
    uint64_t elapsed_ns = 0;
    double throughput_messages_per_second = 0.0;
    uint64_t payload_bytes = 0;
    uint64_t encoded_bytes = 0;
    std::string outcome = "success";
    std::string error;
    // Emitted as a raw JSON object after syntax validation, never as a string.
    std::string backend_details = "{}";
};

std::string JsonEscape(std::string_view input);
// Best-effort callers can invoke this after backend option parsing fails. It
// recognizes exactly one non-empty --output PATH or --output=PATH occurrence
// and atomically writes only parse-failure schema/outcome/error fields. It
// returns false when no unambiguous safe output path was present; no run/profile
// or other unvalidated metadata is synthesized.
bool WriteBridgeParseFailureArtifactFromArgs(int argc, char** argv,
                                             std::string_view parse_error);

// Pure JSON builder shared by the Mino TCP worker and regression tests.
std::string BuildMinoTcpBackendDetails(
    uint64_t schema_short_id, uint32_t schema_version,
    uint32_t layout_version, ClockMode clock_mode,
    uint32_t receive_batch_size, std::string_view endpoints_json);

// Writes via a temporary sibling and atomic rename. Count inconsistencies and
// malformed backend_details force a failure artifact rather than suppressing it.
void WriteSinkResult(const SinkResult& result);
void WriteSinkResult(const std::filesystem::path& output,
                     const SinkResult& result);

// backend must be a single safe token. role is serialized with RoleName().
// The ready file is runtime_dir/<backend>-<role>.ready and is atomically renamed
// into place. Its content is run_id followed by a newline.
void WriteReadyFile(const std::filesystem::path& runtime_dir,
                    std::string_view backend, Role role,
                    std::string_view run_id);
// Waits for runtime_dir/start until the absolute NowNs() deadline and rejects
// stale barriers whose content is not exactly run_id followed by a newline.
bool WaitForStartFile(const std::filesystem::path& runtime_dir,
                      std::string_view run_id,
                      uint64_t absolute_deadline_ns);

}  // namespace mino::benchmarks::pipeline

#endif  // BENCHMARKS_PIPELINE_COMPARISON_PIPELINE_COMMON_H_
