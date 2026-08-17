// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iterator>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <utility>

namespace mino::benchmarks::pipeline {
namespace {

struct ClockSelection {
    clockid_t id;
    bool uses_raw;
};

const ClockSelection& BenchmarkClock() {
    static const ClockSelection selection = [] {
#if defined(CLOCK_MONOTONIC_RAW)
        timespec probe{};
        if (clock_gettime(CLOCK_MONOTONIC_RAW, &probe) == 0) {
            return ClockSelection{CLOCK_MONOTONIC_RAW, true};
        }
#endif
        return ClockSelection{CLOCK_MONOTONIC, false};
    }();
    return selection;
}

constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr uint64_t kFnvOffsetBasis = 14'695'981'039'346'656'037ull;
constexpr uint64_t kFnvPrime = 1'099'511'628'211ull;

bool Fail(std::string message, std::string* error) {
    if (error != nullptr) *error = std::move(message);
    return false;
}

std::optional<Profile> ProfileFromWire(uint32_t value) {
    switch (value) {
        case static_cast<uint32_t>(Profile::kSmall): return Profile::kSmall;
        case static_cast<uint32_t>(Profile::kMedium): return Profile::kMedium;
        case static_cast<uint32_t>(Profile::kLarge): return Profile::kLarge;
    }
    return std::nullopt;
}

uint8_t DeterministicPayloadByte(uint64_t sample_id, Profile profile,
                                 size_t index) {
    uint64_t value = sample_id ^
                     (static_cast<uint64_t>(profile) << 61) ^
                     (static_cast<uint64_t>(index) *
                      0x9e3779b97f4a7c15ull);
    value += 0x9e3779b97f4a7c15ull;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ull;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebull;
    value ^= value >> 31;
    return static_cast<uint8_t>(value >> 56);
}

void PopulateDeterministicSemanticFields(SemanticFrame* frame) {
    const uint64_t id = frame->sample_id;
    frame->object_count = static_cast<uint32_t>(8 + (id % 97));
    frame->trajectory_point_count = static_cast<uint32_t>(16 + (id % 241));
    frame->ego_speed_mps = static_cast<double>(id % 701) * 0.05;
    frame->steering_angle_rad =
        static_cast<double>(static_cast<int64_t>(id % 401) - 200) * 0.001;
    frame->acceleration_mps2 =
        static_cast<double>(static_cast<int64_t>(id % 121) - 60) * 0.05;
    frame->brake_percentage = static_cast<double>(id % 101);
    frame->emergency_stop = (id % 257) == 0;
}

std::array<uint64_t, 5> StageTimestamps(const SemanticFrame& frame) {
    return {frame.perception_timestamp_ns, frame.prediction_timestamp_ns,
            frame.planning_timestamp_ns, frame.control_timestamp_ns,
            frame.guardian_timestamp_ns};
}

size_t ForwardingStageCountBefore(Role role) {
    switch (role) {
        case Role::kPerception: return 0;
        case Role::kPrediction: return 1;
        case Role::kPlanning: return 2;
        case Role::kControl: return 3;
        case Role::kGuardian: return 4;
        case Role::kCanbus: return 5;
    }
    throw std::invalid_argument("invalid pipeline role");
}

uint64_t ParseUnsigned(std::string_view text, std::string_view option) {
    uint64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        throw std::runtime_error(std::string(option) +
                                 " requires an unsigned integer");
    }
    return value;
}

std::optional<std::string_view> InlineOption(std::string_view argument,
                                             std::string_view option) {
    if (argument == option) return std::string_view{};
    if (argument.size() > option.size() && argument.starts_with(option) &&
        argument[option.size()] == '=') {
        return argument.substr(option.size() + 1);
    }
    return std::nullopt;
}

std::string_view OptionValue(int* index, int argc, char** argv,
                             std::string_view option,
                             std::string_view inline_value,
                             bool is_inline) {
    if (is_inline) {
        if (inline_value.empty()) {
            throw std::runtime_error(std::string(option) +
                                     " requires a non-empty value");
        }
        return inline_value;
    }
    if (*index + 1 >= argc) {
        throw std::runtime_error(std::string(option) + " requires a value");
    }
    ++(*index);
    const std::string_view value(argv[*index]);
    if (value.empty()) {
        throw std::runtime_error(std::string(option) +
                                 " requires a non-empty value");
    }
    return value;
}

bool ContainsNul(std::string_view value) {
    return value.find('\0') != std::string_view::npos;
}

std::string Trim(std::string_view text) {
    size_t begin = 0;
    while (begin < text.size() &&
           std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
        ++begin;
    }
    size_t end = text.size();
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(text[end - 1])) != 0) {
        --end;
    }
    return std::string(text.substr(begin, end - begin));
}

class JsonParser {
  public:
    explicit JsonParser(std::string_view input) : input_(input) {}

    bool ParseObjectDocument() {
        SkipWhitespace();
        if (!ParseObject(0)) return false;
        SkipWhitespace();
        return position_ == input_.size();
    }

  private:
    static constexpr size_t kMaxDepth = 64;

    void SkipWhitespace() {
        while (position_ < input_.size()) {
            const char value = input_[position_];
            if (value != ' ' && value != '\t' && value != '\n' &&
                value != '\r') {
                break;
            }
            ++position_;
        }
    }

    bool Consume(char expected) {
        SkipWhitespace();
        if (position_ >= input_.size() || input_[position_] != expected) {
            return false;
        }
        ++position_;
        return true;
    }

    bool ParseValue(size_t depth) {
        if (depth > kMaxDepth) return false;
        SkipWhitespace();
        if (position_ >= input_.size()) return false;
        switch (input_[position_]) {
            case '{': return ParseObject(depth);
            case '[': return ParseArray(depth);
            case '"': return ParseString();
            case 't': return ParseLiteral("true");
            case 'f': return ParseLiteral("false");
            case 'n': return ParseLiteral("null");
            default: return ParseNumber();
        }
    }

    bool ParseObject(size_t depth) {
        if (depth > kMaxDepth || !Consume('{')) return false;
        SkipWhitespace();
        if (position_ < input_.size() && input_[position_] == '}') {
            ++position_;
            return true;
        }
        while (true) {
            if (!ParseString() || !Consume(':') || !ParseValue(depth + 1)) {
                return false;
            }
            SkipWhitespace();
            if (position_ >= input_.size()) return false;
            if (input_[position_] == '}') {
                ++position_;
                return true;
            }
            if (input_[position_] != ',') return false;
            ++position_;
        }
    }

    bool ParseArray(size_t depth) {
        if (depth > kMaxDepth || !Consume('[')) return false;
        SkipWhitespace();
        if (position_ < input_.size() && input_[position_] == ']') {
            ++position_;
            return true;
        }
        while (true) {
            if (!ParseValue(depth + 1)) return false;
            SkipWhitespace();
            if (position_ >= input_.size()) return false;
            if (input_[position_] == ']') {
                ++position_;
                return true;
            }
            if (input_[position_] != ',') return false;
            ++position_;
        }
    }

    bool ParseString() {
        SkipWhitespace();
        if (position_ >= input_.size() || input_[position_] != '"') {
            return false;
        }
        ++position_;
        while (position_ < input_.size()) {
            const unsigned char value =
                static_cast<unsigned char>(input_[position_++]);
            if (value == '"') return true;
            if (value < 0x20u) return false;
            if (value != '\\') continue;
            if (position_ >= input_.size()) return false;
            const char escape = input_[position_++];
            if (escape == '"' || escape == '\\' || escape == '/' ||
                escape == 'b' || escape == 'f' || escape == 'n' ||
                escape == 'r' || escape == 't') {
                continue;
            }
            if (escape != 'u' || position_ + 4 > input_.size()) return false;
            for (size_t index = 0; index < 4; ++index) {
                if (std::isxdigit(static_cast<unsigned char>(
                        input_[position_ + index])) == 0) {
                    return false;
                }
            }
            position_ += 4;
        }
        return false;
    }

    bool ParseLiteral(std::string_view literal) {
        if (input_.substr(position_, literal.size()) != literal) return false;
        position_ += literal.size();
        return true;
    }

    bool ParseNumber() {
        const size_t begin = position_;
        if (position_ < input_.size() && input_[position_] == '-') ++position_;
        if (position_ >= input_.size()) return false;
        if (input_[position_] == '0') {
            ++position_;
            if (position_ < input_.size() &&
                std::isdigit(static_cast<unsigned char>(input_[position_])) !=
                    0) {
                return false;
            }
        } else {
            if (std::isdigit(static_cast<unsigned char>(input_[position_])) ==
                    0 ||
                input_[position_] == '0') {
                return false;
            }
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])) !=
                       0) {
                ++position_;
            }
        }
        if (position_ < input_.size() && input_[position_] == '.') {
            ++position_;
            const size_t digits = position_;
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])) !=
                       0) {
                ++position_;
            }
            if (digits == position_) return false;
        }
        if (position_ < input_.size() &&
            (input_[position_] == 'e' || input_[position_] == 'E')) {
            ++position_;
            if (position_ < input_.size() &&
                (input_[position_] == '+' || input_[position_] == '-')) {
                ++position_;
            }
            const size_t digits = position_;
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])) !=
                       0) {
                ++position_;
            }
            if (digits == position_) return false;
        }
        return position_ > begin;
    }

    std::string_view input_;
    size_t position_ = 0;
};

bool IsSafeToken(std::string_view token) {
    if (token.empty() || token.size() > 128 || token == "." || token == "..") {
        return false;
    }
    return std::all_of(token.begin(), token.end(), [](unsigned char value) {
        return std::isalnum(value) != 0 || value == '_' || value == '-' ||
               value == '.';
    });
}

void WriteAll(int descriptor, std::string_view content) {
    size_t written = 0;
    while (written < content.size()) {
        const ssize_t result =
            write(descriptor, content.data() + written, content.size() - written);
        if (result < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "write");
        }
        if (result == 0) throw std::runtime_error("write returned zero bytes");
        written += static_cast<size_t>(result);
    }
}

void AtomicWrite(const std::filesystem::path& destination,
                 std::string_view content) {
    static std::atomic<uint64_t> sequence{0};
    const std::filesystem::path temporary =
        destination.string() + ".tmp." + std::to_string(getpid()) + "." +
        std::to_string(sequence.fetch_add(1, std::memory_order_relaxed));
    const int descriptor =
        open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (descriptor < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "open temporary artifact");
    }
    try {
        WriteAll(descriptor, content);
        if (fsync(descriptor) != 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "fsync temporary artifact");
        }
        if (close(descriptor) != 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "close temporary artifact");
        }
    } catch (...) {
        const int saved_errno = errno;
        close(descriptor);
        unlink(temporary.c_str());
        errno = saved_errno;
        throw;
    }
    if (rename(temporary.c_str(), destination.c_str()) != 0) {
        const int saved_errno = errno;
        unlink(temporary.c_str());
        throw std::system_error(saved_errno, std::generic_category(),
                                "rename artifact");
    }
}

std::string CountFailure(const ResultCounts& counts) {
    if (counts.duplicate != 0) return "duplicate messages detected";
    if (counts.out_of_order != 0) return "out-of-order messages detected";
    if (counts.corrupt != 0) return "corrupt messages detected";
    if (counts.lost != 0) return "lost messages detected";
    if (counts.received != counts.offered) {
        return "offered and received counts differ";
    }
    return {};
}

}  // namespace

std::optional<Role> ParseRole(std::string_view name) {
    if (name == "perception") return Role::kPerception;
    if (name == "prediction") return Role::kPrediction;
    if (name == "planning") return Role::kPlanning;
    if (name == "control") return Role::kControl;
    if (name == "guardian") return Role::kGuardian;
    if (name == "canbus") return Role::kCanbus;
    return std::nullopt;
}

std::string_view RoleName(Role role) {
    switch (role) {
        case Role::kPerception: return "perception";
        case Role::kPrediction: return "prediction";
        case Role::kPlanning: return "planning";
        case Role::kControl: return "control";
        case Role::kGuardian: return "guardian";
        case Role::kCanbus: return "canbus";
    }
    throw std::invalid_argument("invalid pipeline role");
}

std::optional<Role> NextRole(Role role) {
    switch (role) {
        case Role::kPerception: return Role::kPrediction;
        case Role::kPrediction: return Role::kPlanning;
        case Role::kPlanning: return Role::kControl;
        case Role::kControl: return Role::kGuardian;
        case Role::kGuardian: return Role::kCanbus;
        case Role::kCanbus: return std::nullopt;
    }
    throw std::invalid_argument("invalid pipeline role");
}

uint32_t RoleBit(Role role) {
    switch (role) {
        case Role::kPerception: return kPerceptionStageBit;
        case Role::kPrediction: return kPredictionStageBit;
        case Role::kPlanning: return kPlanningStageBit;
        case Role::kControl: return kControlStageBit;
        case Role::kGuardian: return kGuardianStageBit;
        case Role::kCanbus: return 0;
    }
    throw std::invalid_argument("invalid pipeline role");
}

uint32_t ExpectedMask(Role role) {
    switch (role) {
        case Role::kPerception: return 0;
        case Role::kPrediction: return kPerceptionStageBit;
        case Role::kPlanning:
            return kPerceptionStageBit | kPredictionStageBit;
        case Role::kControl:
            return kPerceptionStageBit | kPredictionStageBit |
                   kPlanningStageBit;
        case Role::kGuardian:
            return kPerceptionStageBit | kPredictionStageBit |
                   kPlanningStageBit | kControlStageBit;
        case Role::kCanbus: return kFinalStageMask;
    }
    throw std::invalid_argument("invalid pipeline role");
}

std::optional<Profile> ParseProfile(std::string_view name) {
    if (name == "small") return Profile::kSmall;
    if (name == "medium") return Profile::kMedium;
    if (name == "large") return Profile::kLarge;
    return std::nullopt;
}

std::string_view ProfileName(Profile profile) {
    switch (profile) {
        case Profile::kSmall: return "small";
        case Profile::kMedium: return "medium";
        case Profile::kLarge: return "large";
    }
    throw std::invalid_argument("invalid payload profile");
}

std::string_view CompilationMode() {
#ifdef MINO_PIPELINE_COMPILATION_MODE
    return MINO_PIPELINE_COMPILATION_MODE;
#else
    return "unknown";
#endif
}

std::string_view ClockModeName(ClockMode mode) {
    switch (mode) {
        case ClockMode::kSameHost: return "same-host";
        case ClockMode::kIndependentHosts: return "independent-hosts";
    }
    throw std::invalid_argument("invalid clock mode");
}

size_t ProfilePayloadBytes(Profile profile) {
    switch (profile) {
        case Profile::kSmall: return kSmallPayloadBytes;
        case Profile::kMedium: return kMediumPayloadBytes;
        case Profile::kLarge: return kLargePayloadBytes;
    }
    throw std::invalid_argument("invalid payload profile");
}

void FillDeterministicPayload(uint64_t sample_id, Profile profile,
                              std::vector<uint8_t>* payload) {
    if (payload == nullptr) {
        throw std::invalid_argument("payload destination must not be null");
    }
    payload->resize(ProfilePayloadBytes(profile));
    for (size_t index = 0; index < payload->size(); ++index) {
        (*payload)[index] = DeterministicPayloadByte(sample_id, profile, index);
    }
}

std::vector<uint8_t> MakeDeterministicPayload(uint64_t sample_id,
                                              Profile profile) {
    std::vector<uint8_t> payload;
    FillDeterministicPayload(sample_id, profile, &payload);
    return payload;
}

uint64_t StablePayloadChecksum(std::span<const uint8_t> payload) {
    uint64_t checksum = kFnvOffsetBasis;
    for (const uint8_t value : payload) {
        checksum ^= value;
        checksum *= kFnvPrime;
    }
    return checksum;
}

bool ValidateDeterministicPayload(uint64_t sample_id, Profile profile,
                                  std::span<const uint8_t> payload,
                                  std::string* error) {
    const size_t expected_size = ProfilePayloadBytes(profile);
    if (payload.size() != expected_size) {
        return Fail("payload size mismatch: expected " +
                        std::to_string(expected_size) + ", got " +
                        std::to_string(payload.size()),
                    error);
    }
    for (size_t index = 0; index < payload.size(); ++index) {
        if (payload[index] !=
            DeterministicPayloadByte(sample_id, profile, index)) {
            return Fail("payload byte mismatch at index " +
                            std::to_string(index),
                        error);
        }
    }
    return true;
}

SemanticFrame InitializeSourceFrame(uint64_t sample_id, Profile profile,
                                    bool measured) {
    const uint64_t origin_timestamp_ns = measured ? NowNs() : 0;
    return InitializeSourceFrameAt(sample_id, profile, origin_timestamp_ns);
}

SemanticFrame InitializeSourceFrameAt(uint64_t sample_id, Profile profile,
                                      uint64_t origin_timestamp_ns) {
    SemanticFrame frame;
    frame.sample_id = sample_id;
    frame.origin_timestamp_ns = origin_timestamp_ns;
    frame.profile = static_cast<uint32_t>(profile);
    PopulateDeterministicSemanticFields(&frame);
    FillDeterministicPayload(sample_id, profile, &frame.payload);
    frame.payload_checksum = StablePayloadChecksum(frame.payload);
    return frame;
}

bool ValidateSemanticFrame(const SemanticFrame& frame, std::string* error) {
    const std::optional<Profile> profile = ProfileFromWire(frame.profile);
    if (!profile.has_value()) return Fail("invalid frame profile", error);
    if ((frame.completed_stage_mask & ~kFinalStageMask) != 0) {
        return Fail("completed stage mask contains unknown bits", error);
    }

    SemanticFrame expected;
    expected.sample_id = frame.sample_id;
    PopulateDeterministicSemanticFields(&expected);
    if (frame.object_count != expected.object_count) {
        return Fail("object_count mismatch", error);
    }
    if (frame.trajectory_point_count != expected.trajectory_point_count) {
        return Fail("trajectory_point_count mismatch", error);
    }
    if (frame.ego_speed_mps != expected.ego_speed_mps) {
        return Fail("ego_speed_mps mismatch", error);
    }
    if (frame.steering_angle_rad != expected.steering_angle_rad) {
        return Fail("steering_angle_rad mismatch", error);
    }
    if (frame.acceleration_mps2 != expected.acceleration_mps2) {
        return Fail("acceleration_mps2 mismatch", error);
    }
    if (frame.brake_percentage != expected.brake_percentage) {
        return Fail("brake_percentage mismatch", error);
    }
    if (frame.emergency_stop != expected.emergency_stop) {
        return Fail("emergency_stop mismatch", error);
    }
    const uint64_t checksum = StablePayloadChecksum(frame.payload);
    if (frame.payload_checksum != checksum) {
        return Fail("payload checksum mismatch", error);
    }
    return ValidateDeterministicPayload(frame.sample_id, *profile, frame.payload,
                                        error);
}

bool ValidateFrameForStage(Role role, const SemanticFrame& frame,
                           std::string* error) {
    if (!ValidateSemanticFrame(frame, error)) return false;
    const uint32_t expected_mask = ExpectedMask(role);
    if (frame.completed_stage_mask != expected_mask) {
        return Fail("stage mask mismatch for " + std::string(RoleName(role)) +
                        ": expected " + std::to_string(expected_mask) +
                        ", got " +
                        std::to_string(frame.completed_stage_mask),
                    error);
    }

    const size_t completed = ForwardingStageCountBefore(role);
    const std::array<uint64_t, 5> timestamps = StageTimestamps(frame);
    uint64_t previous = frame.origin_timestamp_ns;
    for (size_t index = 0; index < timestamps.size(); ++index) {
        if (index < completed) {
            if (timestamps[index] == 0) {
                return Fail("completed stage has a zero timestamp", error);
            }
            if (previous != 0 && timestamps[index] < previous) {
                return Fail("stage timestamps are not monotonic", error);
            }
            previous = timestamps[index];
        } else if (timestamps[index] != 0) {
            return Fail("incomplete stage has a non-zero timestamp", error);
        }
    }
    return true;
}

namespace {

bool StampValidatedStage(Role role, SemanticFrame* frame,
                         uint64_t timestamp_ns, std::string* error) {
    if (role == Role::kCanbus) return true;
    if (timestamp_ns == 0) return Fail("stage timestamp must be non-zero", error);

    const std::array<uint64_t, 5> timestamps = StageTimestamps(*frame);
    const size_t completed = ForwardingStageCountBefore(role);
    const uint64_t previous =
        completed == 0 ? frame->origin_timestamp_ns : timestamps[completed - 1];
    if (previous != 0 && timestamp_ns < previous) {
        return Fail("new stage timestamp is not monotonic", error);
    }

    switch (role) {
        case Role::kPerception:
            frame->perception_timestamp_ns = timestamp_ns;
            break;
        case Role::kPrediction:
            frame->prediction_timestamp_ns = timestamp_ns;
            break;
        case Role::kPlanning:
            frame->planning_timestamp_ns = timestamp_ns;
            break;
        case Role::kControl:
            frame->control_timestamp_ns = timestamp_ns;
            break;
        case Role::kGuardian:
            frame->guardian_timestamp_ns = timestamp_ns;
            break;
        case Role::kCanbus: break;
    }
    frame->completed_stage_mask |= RoleBit(role);
    return true;
}

}  // namespace

bool ApplyStage(Role role, SemanticFrame* frame, uint64_t timestamp_ns,
                std::string* error) {
    if (frame == nullptr) return Fail("frame must not be null", error);
    if (!ValidateFrameForStage(role, *frame, error)) return false;
    return StampValidatedStage(role, frame, timestamp_ns, error);
}

bool ApplyStage(Role role, SemanticFrame* frame, std::string* error) {
    if (frame == nullptr) return Fail("frame must not be null", error);
    if (!ValidateFrameForStage(role, *frame, error)) return false;
    // The production overload timestamps only after complete semantic and
    // payload validation. Tests may still inject an explicit timestamp above.
    return StampValidatedStage(role, frame, NowNs(), error);
}

bool ApplyStageForClockMode(Role role, SemanticFrame* frame, ClockMode mode,
                            std::string* error) {
    if (mode == ClockMode::kSameHost) {
        return ApplyStage(role, frame, error);
    }
    if (mode != ClockMode::kIndependentHosts) {
        return Fail("invalid clock mode", error);
    }
    if (frame == nullptr) return Fail("frame must not be null", error);
    if (!ValidateSemanticFrame(*frame, error)) return false;
    if (frame->completed_stage_mask != ExpectedMask(role)) {
        return Fail("stage mask mismatch for " + std::string(RoleName(role)),
                    error);
    }
    const size_t completed = ForwardingStageCountBefore(role);
    const std::array<uint64_t, 5> timestamps = StageTimestamps(*frame);
    for (size_t index = 0; index < timestamps.size(); ++index) {
        if (index < completed && timestamps[index] == 0) {
            return Fail("completed stage has a zero timestamp", error);
        }
        if (index >= completed && timestamps[index] != 0) {
            return Fail("incomplete stage has a non-zero timestamp", error);
        }
    }
    if (role == Role::kCanbus) return true;
    const uint64_t timestamp_ns = NowNs();
    if (timestamp_ns == 0) return Fail("stage timestamp must be non-zero", error);
    switch (role) {
        case Role::kPerception:
            frame->perception_timestamp_ns = timestamp_ns;
            break;
        case Role::kPrediction:
            frame->prediction_timestamp_ns = timestamp_ns;
            break;
        case Role::kPlanning:
            frame->planning_timestamp_ns = timestamp_ns;
            break;
        case Role::kControl:
            frame->control_timestamp_ns = timestamp_ns;
            break;
        case Role::kGuardian:
            frame->guardian_timestamp_ns = timestamp_ns;
            break;
        case Role::kCanbus: break;
    }
    frame->completed_stage_mask |= RoleBit(role);
    return true;
}

CommonOptions ParseCommonOptions(int argc, char** argv) {
    if (argc < 0 || (argc > 0 && argv == nullptr)) {
        throw std::invalid_argument("invalid argc/argv");
    }
    CommonOptions options;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            throw std::invalid_argument("argv contains a null argument");
        }
        const std::string_view argument(argv[index]);
        const auto parse_value = [&](std::string_view option)
            -> std::optional<std::string_view> {
            const std::optional<std::string_view> inline_value =
                InlineOption(argument, option);
            if (!inline_value.has_value()) return std::nullopt;
            return OptionValue(&index, argc, argv, option, *inline_value,
                               argument != option);
        };

        if (const auto value = parse_value("--role")) {
            const std::optional<Role> parsed = ParseRole(*value);
            if (!parsed.has_value()) {
                throw std::runtime_error(
                    "--role must be perception, prediction, planning, control, "
                    "guardian, or canbus");
            }
            options.role = *parsed;
            continue;
        }
        if (const auto value = parse_value("--profile")) {
            const std::optional<Profile> parsed = ParseProfile(*value);
            if (!parsed.has_value()) {
                throw std::runtime_error(
                    "--profile must be small, medium, or large");
            }
            options.profile = *parsed;
            continue;
        }
        if (const auto value = parse_value("--messages")) {
            options.messages = ParseUnsigned(*value, "--messages");
            continue;
        }
        if (const auto value = parse_value("--warmup-messages")) {
            options.warmup_messages =
                ParseUnsigned(*value, "--warmup-messages");
            continue;
        }
        if (const auto value = parse_value("--publish-interval-us")) {
            options.publish_interval_us =
                ParseUnsigned(*value, "--publish-interval-us");
            continue;
        }
        if (const auto value = parse_value("--deadline-seconds")) {
            options.deadline_seconds =
                ParseUnsigned(*value, "--deadline-seconds");
            continue;
        }
        if (const auto value = parse_value("--clock-mode")) {
            if (*value == "same-host") {
                options.clock_mode = ClockMode::kSameHost;
            } else if (*value == "independent-hosts") {
                options.clock_mode = ClockMode::kIndependentHosts;
            } else {
                throw std::runtime_error(
                    "--clock-mode must be same-host or independent-hosts");
            }
            continue;
        }
        if (const auto value = parse_value("--run-id")) {
            options.run_id = std::string(*value);
            continue;
        }
        if (const auto value = parse_value("--runtime-dir")) {
            options.runtime_dir = std::filesystem::path(*value);
            continue;
        }
        if (const auto value = parse_value("--output")) {
            options.output = std::filesystem::path(*value);
            continue;
        }
        // Unknown options and positional arguments belong to a backend parser.
    }
    ValidateCommonOptions(options);
    return options;
}

void ValidateCommonOptions(const CommonOptions& options) {
    (void)RoleName(options.role);
    (void)ProfileName(options.profile);
    (void)ClockModeName(options.clock_mode);
    if (options.messages == 0 || options.messages > kMaxMessages) {
        throw std::runtime_error("--messages must be in [1, 1000000000]");
    }
    if (options.warmup_messages > kMaxWarmupMessages) {
        throw std::runtime_error(
            "--warmup-messages must be in [0, 1000000000]");
    }
    if (options.publish_interval_us > kMaxPublishIntervalUs) {
        throw std::runtime_error(
            "--publish-interval-us must be in [0, 60000000]");
    }
    if (options.deadline_seconds == 0 ||
        options.deadline_seconds > kMaxDeadlineSeconds) {
        throw std::runtime_error("--deadline-seconds must be in [1, 86400]");
    }
    if (options.run_id.empty() || ContainsNul(options.run_id)) {
        throw std::runtime_error("--run-id must be non-empty");
    }
    const std::string runtime_dir = options.runtime_dir.string();
    if (runtime_dir.empty() || ContainsNul(runtime_dir)) {
        throw std::runtime_error("--runtime-dir must be non-empty");
    }
    const std::string output = options.output.string();
    if (output.empty() || ContainsNul(output)) {
        throw std::runtime_error("--output must be non-empty");
    }
}

uint64_t NowNs() {
    timespec value{};
    if (clock_gettime(BenchmarkClock().id, &value) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "clock_gettime");
    }
    return static_cast<uint64_t>(value.tv_sec) * kNanosecondsPerSecond +
           static_cast<uint64_t>(value.tv_nsec);
}

void PaceSource(uint64_t schedule_start_ns, uint64_t sample_id,
                uint64_t publish_interval_us,
                uint64_t absolute_deadline_ns) {
    if (publish_interval_us == 0) return;
    constexpr uint64_t kNanosecondsPerMicrosecond = 1'000;
    if (publish_interval_us >
        std::numeric_limits<uint64_t>::max() / kNanosecondsPerMicrosecond) {
        throw std::overflow_error("publish interval nanoseconds overflow");
    }
    const uint64_t interval_ns =
        publish_interval_us * kNanosecondsPerMicrosecond;
    if (sample_id != 0 &&
        interval_ns > std::numeric_limits<uint64_t>::max() / sample_id) {
        throw std::overflow_error("source publish schedule overflow");
    }
    const uint64_t offset_ns = interval_ns * sample_id;
    if (offset_ns > std::numeric_limits<uint64_t>::max() - schedule_start_ns) {
        throw std::overflow_error("source publish target overflow");
    }
    const uint64_t target_ns = schedule_start_ns + offset_ns;
    if (target_ns >= absolute_deadline_ns) {
        throw std::runtime_error("source publish schedule exceeds deadline");
    }
    for (;;) {
        const uint64_t now = NowNs();
        if (now >= target_ns) {
            // Do not silently relabel a catch-up burst as paced latency. Allow
            // sub-period scheduler jitter, but fail once the source is more
            // than one complete publish interval behind its absolute schedule.
            if (sample_id != 0 && now - target_ns > interval_ns) {
                throw std::runtime_error(
                    "paced source fell more than one interval behind schedule");
            }
            return;
        }
        if (now >= absolute_deadline_ns) {
            throw std::runtime_error("deadline expired while pacing source");
        }
        const uint64_t remaining_ns = target_ns - now;
        std::this_thread::sleep_for(
            std::chrono::nanoseconds(std::min<uint64_t>(remaining_ns, 1'000'000)));
    }
}

std::string_view ClockName() {
    return BenchmarkClock().uses_raw
               ? "CLOCK_MONOTONIC_RAW"
               : "CLOCK_MONOTONIC (fallback: CLOCK_MONOTONIC_RAW unavailable)";
}

uint64_t ClockResolutionNs() {
    timespec value{};
    if (clock_getres(BenchmarkClock().id, &value) != 0) {
        throw std::system_error(errno, std::generic_category(), "clock_getres");
    }
    return static_cast<uint64_t>(value.tv_sec) * kNanosecondsPerSecond +
           static_cast<uint64_t>(value.tv_nsec);
}

std::string ReadBootId() {
    std::ifstream input("/proc/sys/kernel/random/boot_id");
    std::string boot_id;
    if (!input || !std::getline(input, boot_id)) return "unavailable";
    while (!boot_id.empty() &&
           (boot_id.back() == '\r' || boot_id.back() == '\n' ||
            boot_id.back() == ' ' || boot_id.back() == '\t')) {
        boot_id.pop_back();
    }
    return boot_id.empty() ? "unavailable" : boot_id;
}

Distribution Summarize(std::vector<uint64_t> samples) {
    Distribution result;
    result.samples = samples.size();
    if (samples.empty()) return result;
    std::sort(samples.begin(), samples.end());
    const auto nearest_rank = [&](uint64_t numerator, uint64_t denominator) {
        const uint64_t count = static_cast<uint64_t>(samples.size());
        const uint64_t rank = (count * numerator + denominator - 1) / denominator;
        return samples[static_cast<size_t>(rank - 1)];
    };
    result.p50 = nearest_rank(50, 100);
    result.p95 = nearest_rank(95, 100);
    result.p99 = nearest_rank(99, 100);
    result.p99_9 = nearest_rank(999, 1000);
    result.maximum = samples.back();
    return result;
}

std::string JsonEscape(std::string_view input) {
    std::ostringstream output;
    for (const unsigned char character : input) {
        switch (character) {
            case '"': output << "\\\""; break;
            case '\\': output << "\\\\"; break;
            case '\b': output << "\\b"; break;
            case '\f': output << "\\f"; break;
            case '\n': output << "\\n"; break;
            case '\r': output << "\\r"; break;
            case '\t': output << "\\t"; break;
            default:
                if (character < 0x20u) {
                    output << "\\u" << std::hex << std::setw(4)
                           << std::setfill('0')
                           << static_cast<unsigned int>(character) << std::dec
                           << std::setfill(' ');
                } else {
                    output << static_cast<char>(character);
                }
        }
    }
    return output.str();
}

void WriteSinkResult(const SinkResult& result) {
    WriteSinkResult(result.options.output, result);
}

void WriteSinkResult(const std::filesystem::path& output,
                     const SinkResult& result) {
    ValidateCommonOptions(result.options);
    if (output.empty()) throw std::runtime_error("result output is empty");

    const std::string details = Trim(result.backend_details);
    const bool details_valid = JsonParser(details).ParseObjectDocument();
    std::string effective_error = result.error;
    std::string effective_outcome = result.outcome;
    const std::string count_failure = CountFailure(result.counts);
    if (!count_failure.empty()) {
        effective_outcome = "failure";
        if (effective_error.empty()) effective_error = count_failure;
    }
    if (!details_valid) {
        effective_outcome = "failure";
        if (!effective_error.empty()) effective_error += "; ";
        effective_error += "invalid backend_details JSON object";
    }
    if (effective_outcome != "success" || !effective_error.empty() ||
        result.backend.empty()) {
        effective_outcome = "failure";
        if (result.backend.empty() && effective_error.empty()) {
            effective_error = "backend must be non-empty";
        }
    }

    double throughput = result.throughput_messages_per_second;
    if (!std::isfinite(throughput) || throughput < 0.0) {
        throughput = 0.0;
        effective_outcome = "failure";
        if (!effective_error.empty()) effective_error += "; ";
        effective_error += "invalid throughput";
    } else if (throughput == 0.0 && result.elapsed_ns != 0) {
        throughput = static_cast<double>(result.counts.received) *
                     static_cast<double>(kNanosecondsPerSecond) /
                     static_cast<double>(result.elapsed_ns);
    }

    std::ostringstream json;
    json << std::setprecision(17)
         << "{\n"
         << "  \"schema\": \"mino.pipeline_e2e_benchmark.worker.v1\",\n"
         << "  \"backend\": \"" << JsonEscape(result.backend) << "\",\n"
         << "  \"role\": \"" << RoleName(result.options.role) << "\",\n"
         << "  \"profile\": \"" << ProfileName(result.options.profile)
         << "\",\n"
         << "  \"configuration\": {\n"
         << "    \"messages\": " << result.options.messages << ",\n"
         << "    \"warmup_messages\": " << result.options.warmup_messages
         << ",\n"
         << "    \"publish_interval_us\": "
         << result.options.publish_interval_us << ",\n"
         << "    \"deadline_seconds\": "
         << result.options.deadline_seconds << ",\n"
         << "    \"clock_mode\": \""
         << ClockModeName(result.options.clock_mode) << "\",\n"
         << "    \"run_id\": \"" << JsonEscape(result.options.run_id)
         << "\",\n"
         << "    \"runtime_dir\": \""
         << JsonEscape(result.options.runtime_dir.string()) << "\",\n"
         << "    \"output\": \""
         << JsonEscape(result.options.output.string()) << "\"\n"
         << "  },\n"
         << "  \"clock\": {\"name\": \"" << JsonEscape(ClockName())
         << "\", \"resolution_ns\": " << ClockResolutionNs()
         << ", \"boot_id\": \"" << JsonEscape(ReadBootId()) << "\"},\n"
         << "  \"counts\": {\"offered\": " << result.counts.offered
         << ", \"received\": " << result.counts.received
         << ", \"duplicate\": " << result.counts.duplicate
         << ", \"out_of_order\": " << result.counts.out_of_order
         << ", \"corrupt\": " << result.counts.corrupt
         << ", \"lost\": " << result.counts.lost << "},\n"
         << "  \"latency_ns\": {\"samples\": "
         << result.latency_ns.samples << ", \"p50\": "
         << result.latency_ns.p50 << ", \"p95\": "
         << result.latency_ns.p95 << ", \"p99\": "
         << result.latency_ns.p99 << ", \"p99_9\": "
         << result.latency_ns.p99_9 << ", \"max\": "
         << result.latency_ns.maximum << "},\n"
         << "  \"elapsed_ns\": " << result.elapsed_ns << ",\n"
         << "  \"throughput_messages_per_second\": " << throughput
         << ",\n"
         << "  \"payload_bytes\": " << result.payload_bytes << ",\n"
         << "  \"encoded_bytes\": " << result.encoded_bytes << ",\n"
         << "  \"outcome\": \"" << JsonEscape(effective_outcome)
         << "\",\n"
         << "  \"error\": \"" << JsonEscape(effective_error) << "\",\n"
         << "  \"backend_details\": "
         << (details_valid ? details : "{}") << "\n"
         << "}\n";
    AtomicWrite(output, json.str());
}

void WriteReadyFile(const std::filesystem::path& runtime_dir,
                    std::string_view backend, Role role,
                    std::string_view run_id) {
    if (runtime_dir.empty()) {
        throw std::invalid_argument("runtime directory must be non-empty");
    }
    if (!IsSafeToken(backend)) {
        throw std::invalid_argument("backend is not a safe path token");
    }
    const std::string_view role_name = RoleName(role);
    if (!IsSafeToken(role_name)) {
        throw std::invalid_argument("role is not a safe path token");
    }
    const std::filesystem::path destination =
        runtime_dir / (std::string(backend) + "-" + std::string(role_name) +
                       ".ready");
    AtomicWrite(destination, std::string(run_id) + "\n");
}

bool WaitForStartFile(const std::filesystem::path& runtime_dir,
                      std::string_view run_id,
                      uint64_t absolute_deadline_ns) {
    if (runtime_dir.empty()) {
        throw std::invalid_argument("runtime directory must be non-empty");
    }
    if (run_id.empty() || ContainsNul(run_id)) {
        throw std::invalid_argument("run-id must be non-empty and contain no NUL");
    }
    const std::filesystem::path start = runtime_dir / "start";
    while (true) {
        std::error_code error;
        if (std::filesystem::exists(start, error)) {
            std::ifstream stream(start, std::ios::binary);
            if (!stream.good()) {
                throw std::runtime_error("cannot open start barrier");
            }
            std::string content((std::istreambuf_iterator<char>(stream)),
                                std::istreambuf_iterator<char>());
            if (content != std::string(run_id) + "\n") {
                throw std::runtime_error("start barrier run-id mismatch");
            }
            return true;
        }
        if (error && error != std::errc::no_such_file_or_directory) {
            throw std::system_error(error, "check start file");
        }
        if (NowNs() >= absolute_deadline_ns) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

}  // namespace mino::benchmarks::pipeline
