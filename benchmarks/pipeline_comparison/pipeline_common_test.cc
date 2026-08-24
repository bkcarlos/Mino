// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sys/syscall.h>
#endif
#include <unistd.h>

#if defined(__linux__)
namespace {

thread_local bool g_inject_close_failure = false;
thread_local int g_close_target = -1;
thread_local int g_close_calls = 0;
thread_local bool g_track_fsync = false;
thread_local int g_fsync_calls = 0;

}  // namespace

extern "C" int close(int descriptor) {
    if (g_inject_close_failure && descriptor == g_close_target) {
        ++g_close_calls;
        const int result =
            static_cast<int>(syscall(SYS_close, descriptor));
        if (g_close_calls == 1) {
            errno = EIO;
            return -1;
        }
        return result;
    }
    return static_cast<int>(syscall(SYS_close, descriptor));
}

extern "C" int fsync(int descriptor) {
    if (g_inject_close_failure && g_close_target < 0) {
        g_close_target = descriptor;
    }
    if (g_track_fsync) ++g_fsync_calls;
    return static_cast<int>(syscall(SYS_fsync, descriptor));
}
#endif

namespace mino::benchmarks::pipeline {
namespace {

class Arguments {
  public:
    Arguments(std::initializer_list<std::string_view> values) {
        storage_.reserve(values.size());
        for (const std::string_view value : values) storage_.emplace_back(value);
        argv_.reserve(storage_.size());
        for (std::string& value : storage_) argv_.push_back(value.data());
    }

    CommonOptions Parse() {
        return ParseCommonOptions(static_cast<int>(argv_.size()), argv_.data());
    }

    int argc() const { return static_cast<int>(argv_.size()); }
    char** argv() { return argv_.data(); }

  private:
    std::vector<std::string> storage_;
    std::vector<char*> argv_;
};

Arguments ValidArguments() {
    return Arguments({"worker", "--role", "planning", "--profile=medium",
                      "--messages", "100", "--warmup-messages", "10",
                      "--deadline-seconds", "5", "--run-id", "round-1",
                      "--runtime-dir", "/tmp/pipeline-runtime", "--output",
                      "/tmp/pipeline-result.json"});
}

std::filesystem::path CreateTestDirectory(std::string_view name) {
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() /
        (std::string(name) + "-" + std::to_string(getpid()));
    std::filesystem::remove_all(directory);
    if (!std::filesystem::create_directory(directory)) {
        throw std::runtime_error("failed to create test directory");
    }
    return directory;
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

double ReadThroughput(const std::filesystem::path& path) {
    const std::string json = ReadFile(path);
    constexpr std::string_view prefix =
        "\"throughput_messages_per_second\": ";
    const size_t begin = json.find(prefix);
    if (begin == std::string::npos) {
        throw std::runtime_error("throughput field not found");
    }
    const size_t value_begin = begin + prefix.size();
    const size_t value_end = json.find(',', value_begin);
    return std::stod(json.substr(value_begin, value_end - value_begin));
}

SinkResult FallbackThroughputResult(const std::filesystem::path& output,
                                    ClockMode clock_mode, uint64_t received) {
    SinkResult result;
    result.backend = "test-backend";
    result.options = ValidArguments().Parse();
    result.options.role = Role::kCanbus;
    result.options.clock_mode = clock_mode;
    result.options.output = output;
    result.counts.offered = received;
    result.counts.received = received;
    result.elapsed_ns = 1'000'000'000;
    return result;
}

#if defined(__linux__)
class ScopedCloseFailure final {
  public:
    ScopedCloseFailure() {
        g_close_target = -1;
        g_close_calls = 0;
        g_inject_close_failure = true;
    }
    ~ScopedCloseFailure() {
        g_inject_close_failure = false;
        g_close_target = -1;
    }

    int calls() const { return g_close_calls; }
};

class ScopedFsyncTracking final {
  public:
    ScopedFsyncTracking() {
        g_fsync_calls = 0;
        g_track_fsync = true;
    }
    ~ScopedFsyncTracking() { g_track_fsync = false; }

    int calls() const { return g_fsync_calls; }
};
#endif

TEST(PipelineCommonTest, ParsesEveryRoleAndDefinesOrderAndBits) {
    const std::vector<Role> roles = {
        Role::kPerception, Role::kPrediction, Role::kPlanning,
        Role::kControl,    Role::kGuardian,   Role::kCanbus,
    };
    uint32_t preceding_mask = 0;
    for (size_t index = 0; index < roles.size(); ++index) {
        const Role role = roles[index];
        ASSERT_EQ(ParseRole(RoleName(role)), role);
        EXPECT_EQ(ExpectedMask(role), preceding_mask);
        if (index + 1 < roles.size()) {
            ASSERT_TRUE(NextRole(role).has_value());
            EXPECT_EQ(*NextRole(role), roles[index + 1]);
            EXPECT_NE(RoleBit(role), 0u);
            preceding_mask |= RoleBit(role);
        } else {
            EXPECT_FALSE(NextRole(role).has_value());
            EXPECT_EQ(RoleBit(role), 0u);
        }
    }
    EXPECT_EQ(preceding_mask, kFinalStageMask);
    EXPECT_FALSE(ParseRole("CANBus").has_value());
    EXPECT_FALSE(ParseRole("unknown").has_value());
}

TEST(PipelineCommonTest, RecordsBazelCompilationMode) {
    EXPECT_TRUE(CompilationMode() == "fastbuild" || CompilationMode() == "opt" ||
                CompilationMode() == "dbg");
}

TEST(PipelineCommonTest, ParsesProfilesWithExactPayloadSizes) {
    ASSERT_EQ(ParseProfile("small"), Profile::kSmall);
    ASSERT_EQ(ParseProfile("medium"), Profile::kMedium);
    ASSERT_EQ(ParseProfile("large"), Profile::kLarge);
    EXPECT_EQ(ProfilePayloadBytes(Profile::kSmall), 256u);
    EXPECT_EQ(ProfilePayloadBytes(Profile::kMedium), 65'536u);
    EXPECT_EQ(ProfilePayloadBytes(Profile::kLarge), 1'048'576u);
    EXPECT_EQ(ProfileName(Profile::kMedium), "medium");
    EXPECT_FALSE(ParseProfile("64k").has_value());
}

TEST(PipelineCommonTest, PayloadIsDeterministicAndCorruptionIsDetected) {
    const std::vector<uint8_t> first =
        MakeDeterministicPayload(42, Profile::kMedium);
    const std::vector<uint8_t> second =
        MakeDeterministicPayload(42, Profile::kMedium);
    const std::vector<uint8_t> other_sample =
        MakeDeterministicPayload(43, Profile::kMedium);
    EXPECT_EQ(first, second);
    EXPECT_NE(first, other_sample);
    EXPECT_EQ(StablePayloadChecksum(first), StablePayloadChecksum(second));

    std::string error;
    EXPECT_TRUE(ValidateDeterministicPayload(42, Profile::kMedium, first,
                                             &error));
    std::vector<uint8_t> corrupted = first;
    corrupted[corrupted.size() / 2] ^= 0x80u;
    EXPECT_FALSE(ValidateDeterministicPayload(42, Profile::kMedium, corrupted,
                                              &error));
    EXPECT_NE(error.find("payload byte mismatch"), std::string::npos);
}

TEST(PipelineCommonTest, ApplyStageRequiresExactMaskAndTimestampOrder) {
    SemanticFrame out_of_order =
        InitializeSourceFrameAt(7, Profile::kSmall, 100);
    std::string error;
    EXPECT_FALSE(ApplyStage(Role::kPrediction, &out_of_order, 102, &error));
    EXPECT_EQ(out_of_order.completed_stage_mask, 0u);

    SemanticFrame frame = InitializeSourceFrameAt(7, Profile::kSmall, 100);
    EXPECT_TRUE(ApplyStage(Role::kPerception, &frame, 101, &error));
    EXPECT_EQ(frame.completed_stage_mask, kPerceptionStageBit);
    EXPECT_TRUE(ApplyStage(Role::kPrediction, &frame, 102, &error));
    EXPECT_TRUE(ApplyStage(Role::kPlanning, &frame, 103, &error));
    EXPECT_TRUE(ApplyStage(Role::kControl, &frame, 104, &error));
    EXPECT_TRUE(ApplyStage(Role::kGuardian, &frame, 105, &error));
    EXPECT_EQ(frame.completed_stage_mask, kFinalStageMask);
    EXPECT_TRUE(ValidateFrameForStage(Role::kCanbus, frame, &error));

    const SemanticFrame before_sink = frame;
    EXPECT_TRUE(ApplyStage(Role::kCanbus, &frame, 106, &error));
    EXPECT_EQ(frame.completed_stage_mask, before_sink.completed_stage_mask);
    EXPECT_EQ(frame.guardian_timestamp_ns,
              before_sink.guardian_timestamp_ns);

    SemanticFrame backwards = InitializeSourceFrameAt(8, Profile::kSmall, 200);
    EXPECT_FALSE(ApplyStage(Role::kPerception, &backwards, 199, &error));
}

TEST(PipelineCommonTest, SemanticAndChecksumCorruptionAreDetected) {
    SemanticFrame frame = InitializeSourceFrameAt(11, Profile::kSmall, 0);
    std::string error;
    frame.object_count += 1;
    EXPECT_FALSE(ValidateSemanticFrame(frame, &error));
    EXPECT_EQ(error, "object_count mismatch");

    frame = InitializeSourceFrameAt(11, Profile::kSmall, 0);
    frame.payload.back() ^= 1u;
    EXPECT_FALSE(ValidateSemanticFrame(frame, &error));
    EXPECT_EQ(error, "payload checksum mismatch");
}

TEST(PipelineCommonTest,
     BridgeStructuralValidationAcceptsBusinessCorruptionThatStageRejects) {
    enum class Corruption { kScalar, kChecksum, kPayloadPattern };
    const std::vector<std::pair<std::string_view, Corruption>> cases = {
        {"deterministic scalar", Corruption::kScalar},
        {"payload checksum", Corruption::kChecksum},
        {"payload pattern", Corruption::kPayloadPattern},
    };
    for (const auto& [name, corruption] : cases) {
        SCOPED_TRACE(name);
        SemanticFrame frame =
            InitializeSourceFrameAt(20, Profile::kSmall, 100);
        std::string error;
        ASSERT_TRUE(ApplyStage(Role::kPerception, &frame, 101, &error));
        ASSERT_TRUE(ApplyStage(Role::kPrediction, &frame, 102, &error));
        switch (corruption) {
            case Corruption::kScalar: ++frame.object_count; break;
            case Corruption::kChecksum: ++frame.payload_checksum; break;
            case Corruption::kPayloadPattern: frame.payload[7] ^= 1u; break;
        }
        EXPECT_TRUE(ValidateBridgeTransitFrame(
            frame, 20, 10, Profile::kSmall, Role::kPlanning,
            ClockMode::kSameHost, &error))
            << error;
        EXPECT_FALSE(ValidateFrameForStage(Role::kPlanning, frame, &error));
    }
}

TEST(PipelineCommonTest, BridgeStructuralValidationIsClockModeAware) {
    SemanticFrame frame = InitializeSourceFrameAt(20, Profile::kSmall, 100);
    std::string error;
    ASSERT_TRUE(ApplyStage(Role::kPerception, &frame, 101, &error));
    ASSERT_TRUE(ApplyStage(Role::kPrediction, &frame, 102, &error));
    EXPECT_TRUE(ValidateBridgeTransitFrame(
        frame, 20, 10, Profile::kSmall, Role::kPlanning,
        ClockMode::kSameHost, &error));

    frame.prediction_timestamp_ns = 99;
    EXPECT_FALSE(ValidateBridgeTransitFrame(
        frame, 20, 10, Profile::kSmall, Role::kPlanning,
        ClockMode::kSameHost, &error));
    EXPECT_TRUE(ValidateBridgeTransitFrame(
        frame, 20, 10, Profile::kSmall, Role::kPlanning,
        ClockMode::kIndependentHosts, &error));

    frame.payload.pop_back();
    EXPECT_FALSE(ValidateBridgeTransitFrame(
        frame, 20, 10, Profile::kSmall, Role::kPlanning,
        ClockMode::kIndependentHosts, &error));
}

TEST(PipelineCommonTest, WarmupAndMeasuredOriginsAreDistinct) {
    const SemanticFrame warmup =
        InitializeSourceFrame(1, Profile::kSmall, false);
    const SemanticFrame measured =
        InitializeSourceFrame(1, Profile::kSmall, true);
    EXPECT_EQ(warmup.origin_timestamp_ns, 0u);
    EXPECT_NE(measured.origin_timestamp_ns, 0u);
    EXPECT_EQ(warmup.payload, measured.payload);
}

TEST(PipelineCommonTest, UsesNearestRankIncludingP99Point9) {
    std::vector<uint64_t> samples;
    for (uint64_t value = 1; value <= 1'000; ++value) {
        samples.push_back(value);
    }
    const Distribution result = Summarize(samples);
    EXPECT_EQ(result.samples, 1'000u);
    EXPECT_EQ(result.p50, 500u);
    EXPECT_EQ(result.p95, 950u);
    EXPECT_EQ(result.p99, 990u);
    EXPECT_EQ(result.p99_9, 999u);
    EXPECT_EQ(result.maximum, 1'000u);

    const Distribution empty = Summarize({});
    EXPECT_EQ(empty.samples, 0u);
    EXPECT_EQ(empty.p99_9, 0u);
}

TEST(PipelineCommonTest, ParsesStrictCommonOptionsAndIgnoresBackendOptions) {
    Arguments arguments(
        {"worker", "--role", "planning", "--profile=medium", "--messages",
         "100", "--warmup-messages", "10", "--publish-interval-us", "250",
         "--deadline-seconds", "5", "--run-id", "round-1", "--runtime-dir", "/tmp/runtime", "--output",
         "/tmp/result.json", "--zmq-socket-type", "push", "backend-value"});
    const CommonOptions options = arguments.Parse();
    EXPECT_EQ(options.role, Role::kPlanning);
    EXPECT_EQ(options.profile, Profile::kMedium);
    EXPECT_EQ(options.messages, 100u);
    EXPECT_EQ(options.warmup_messages, 10u);
    EXPECT_EQ(options.publish_interval_us, 250u);
    EXPECT_EQ(options.deadline_seconds, 5u);
    EXPECT_EQ(options.run_id, "round-1");
}

TEST(PipelineCommonTest, RejectsOptionValuesOutsideStrictRanges) {
    {
        Arguments arguments(
            {"worker", "--run-id", "run", "--runtime-dir", "/tmp/r",
             "--output", "/tmp/o", "--messages", "0"});
        EXPECT_THROW(arguments.Parse(), std::runtime_error);
    }
    {
        Arguments arguments(
            {"worker", "--run-id", "run", "--runtime-dir", "/tmp/r",
             "--output", "/tmp/o", "--messages", "1000000001"});
        EXPECT_THROW(arguments.Parse(), std::runtime_error);
    }
    {
        Arguments arguments(
            {"worker", "--run-id", "run", "--runtime-dir", "/tmp/r",
             "--output", "/tmp/o", "--warmup-messages", "-1"});
        EXPECT_THROW(arguments.Parse(), std::runtime_error);
    }
    {
        Arguments arguments(
            {"worker", "--run-id", "run", "--runtime-dir", "/tmp/r",
             "--output", "/tmp/o", "--publish-interval-us", "60000001"});
        EXPECT_THROW(arguments.Parse(), std::runtime_error);
    }
    {
        Arguments arguments(
            {"worker", "--run-id", "run", "--runtime-dir", "/tmp/r",
             "--output", "/tmp/o", "--deadline-seconds", "86401"});
        EXPECT_THROW(arguments.Parse(), std::runtime_error);
    }
    {
        Arguments arguments({"worker", "--run-id=", "--runtime-dir", "/tmp/r",
                             "--output", "/tmp/o"});
        EXPECT_THROW(arguments.Parse(), std::runtime_error);
    }
    {
        Arguments arguments({"worker", "--run-id", "run", "--runtime-dir",
                             "/tmp/r", "--output="});
        EXPECT_THROW(arguments.Parse(), std::runtime_error);
    }
}

TEST(PipelineCommonTest, RejectsDuplicateCommonOptions) {
    Arguments arguments(
        {"worker", "--run-id", "run", "--runtime-dir", "/tmp/r",
         "--output", "/tmp/o", "--messages", "10", "--messages=20"});
    EXPECT_THROW(arguments.Parse(), std::runtime_error);
}

TEST(PipelineCommonTest, RejectsNullSeparatedOptionValue) {
    char worker[] = "worker";
    char option[] = "--messages";
    char* argv[] = {worker, option, nullptr};
    EXPECT_THROW(ParseCommonOptions(3, argv), std::runtime_error);
}

TEST(PipelineCommonTest, ThroughputFallbackUsesClockModeSampleSemantics) {
    const std::filesystem::path directory =
        CreateTestDirectory("pipeline-throughput-mode-test");
    const std::filesystem::path output = directory / "result.json";

    WriteSinkResult(output,
                    FallbackThroughputResult(output, ClockMode::kSameHost, 5));
    EXPECT_DOUBLE_EQ(ReadThroughput(output), 5.0);

    WriteSinkResult(output, FallbackThroughputResult(
                                output, ClockMode::kIndependentHosts, 5));
    EXPECT_DOUBLE_EQ(ReadThroughput(output), 4.0);

    std::filesystem::remove_all(directory);
}

TEST(PipelineCommonTest,
     IndependentHostThroughputFallbackHandlesFewerThanTwoReceives) {
    const std::filesystem::path directory =
        CreateTestDirectory("pipeline-throughput-small-count-test");
    const std::filesystem::path output = directory / "result.json";

    WriteSinkResult(output, FallbackThroughputResult(
                                output, ClockMode::kIndependentHosts, 0));
    EXPECT_DOUBLE_EQ(ReadThroughput(output), 0.0);

    WriteSinkResult(output, FallbackThroughputResult(
                                output, ClockMode::kIndependentHosts, 1));
    EXPECT_DOUBLE_EQ(ReadThroughput(output), 0.0);

    std::filesystem::remove_all(directory);
}

#if defined(__linux__)
TEST(PipelineCommonTest, AtomicWriteDoesNotRetryAfterCloseFailure) {
    const std::filesystem::path directory =
        CreateTestDirectory("pipeline-close-failure-test");
    int close_calls = 0;
    {
        ScopedCloseFailure failure;
        EXPECT_THROW(WriteReadyFile(directory, "backend", Role::kPlanning,
                                    "run-id"),
                     std::system_error);
        close_calls = failure.calls();
    }
    EXPECT_EQ(close_calls, 1);
    EXPECT_TRUE(std::filesystem::is_empty(directory));
    std::filesystem::remove_all(directory);
}

TEST(PipelineCommonTest, AtomicWriteFsyncsParentDirectoryAfterRename) {
    const std::filesystem::path directory =
        CreateTestDirectory("pipeline-directory-fsync-test");
    int fsync_calls = 0;
    {
        ScopedFsyncTracking tracking;
        ASSERT_NO_THROW(WriteReadyFile(directory, "backend", Role::kPlanning,
                                       "run-id"));
        fsync_calls = tracking.calls();
    }
    EXPECT_EQ(fsync_calls, 2);
    EXPECT_EQ(ReadFile(directory / "backend-planning.ready"), "run-id\n");
    std::filesystem::remove_all(directory);
}
#endif

TEST(PipelineCommonTest,
     BridgeParseFailureArtifactPreservesErrorWithoutInventingMetadata) {
    const std::filesystem::path directory =
        CreateTestDirectory("pipeline-bridge-parse-failure-test");
    const std::filesystem::path output = directory / "bridge-failure.json";
    Arguments malformed(
        {"bridge", "--mode=invalid", "--profile=small", "--output",
         output.string()});
    const std::string parse_error =
        "--mode must be source or sink\nusage: original bridge usage";

    ASSERT_TRUE(WriteBridgeParseFailureArtifactFromArgs(
        malformed.argc(), malformed.argv(), parse_error));
    const std::string json = ReadFile(output);
    EXPECT_NE(json.find(
                  "\"schema\": "
                  "\"mino.pipeline_bridge_benchmark.parse_failure.v1\""),
              std::string::npos);
    EXPECT_NE(json.find("\"outcome\": \"failure\""), std::string::npos);
    EXPECT_NE(json.find(
                  "\"error\": \"--mode must be source or sink\\nusage: "
                  "original bridge usage\""),
              std::string::npos);
    EXPECT_EQ(json.find("\"run_id\""), std::string::npos);
    EXPECT_EQ(json.find("\"profile\""), std::string::npos);
    EXPECT_EQ(json.find("\"clock\""), std::string::npos);
    EXPECT_EQ(json.find("\"counters\""), std::string::npos);

    Arguments ambiguous(
        {"bridge", "--output", (directory / "first.json").string(),
         "--output=" + (directory / "second.json").string()});
    EXPECT_FALSE(WriteBridgeParseFailureArtifactFromArgs(
        ambiguous.argc(), ambiguous.argv(), parse_error));
    EXPECT_FALSE(std::filesystem::exists(directory / "first.json"));
    EXPECT_FALSE(std::filesystem::exists(directory / "second.json"));
    std::filesystem::remove_all(directory);
}

TEST(PipelineCommonTest, MinoTcpBackendDetailsRemainsValidJson) {
    const std::filesystem::path directory =
        CreateTestDirectory("pipeline-mino-tcp-details-test");
    const std::filesystem::path output = directory / "result.json";
    const std::string details = BuildMinoTcpBackendDetails(
        123, 4, 5, ClockMode::kSameHost, 8,
        "{\"listen\":\"127.0.0.1:24000\",\"peer\":null}");
    EXPECT_NE(details.find(
                  "\"completion_barrier\":\"reverse hop-by-hop ACK\","
                  "\"receive_batch_size\":8"),
              std::string::npos);

    SinkResult result;
    result.backend = "mino-tcp-canonical";
    result.options = ValidArguments().Parse();
    result.options.role = Role::kCanbus;
    result.options.output = output;
    result.counts.offered = result.options.messages;
    result.counts.received = result.options.messages;
    result.payload_bytes = kSmallPayloadBytes;
    result.encoded_bytes = 320;
    result.backend_details = details;
    ASSERT_NO_THROW(WriteSinkResult(result));

    const std::string artifact = ReadFile(output);
    EXPECT_NE(artifact.find("\"outcome\": \"success\""),
              std::string::npos);
    EXPECT_EQ(artifact.find("invalid backend_details JSON object"),
              std::string::npos);
    EXPECT_NE(artifact.find(
                  "\"completion_barrier\":\"reverse hop-by-hop ACK\","
                  "\"receive_batch_size\":8"),
              std::string::npos);
    std::filesystem::remove_all(directory);
}

TEST(PipelineCommonTest, FailureJsonEscapesErrorAndSanitizesInvalidDetails) {
    const std::filesystem::path output =
        std::filesystem::temp_directory_path() /
        ("pipeline-common-test-" + std::to_string(getpid()) + ".json");
    std::filesystem::remove(output);

    SinkResult result;
    result.backend = "protobuf-zmq";
    result.options = ValidArguments().Parse();
    result.options.role = Role::kCanbus;
    result.options.profile = Profile::kSmall;
    result.options.output = output;
    result.counts.offered = 10;
    result.counts.received = 9;
    result.counts.lost = 1;
    result.latency_ns = Summarize({10, 20, 30});
    result.elapsed_ns = 1'000;
    result.payload_bytes = 256;
    result.encoded_bytes = 300;
    result.outcome = "failure";
    result.error = "receive failed: \"closed\"\nretry stopped";
    result.backend_details = "{\"socket\":}";

    ASSERT_NO_THROW(WriteSinkResult(result));
    std::ifstream input(output);
    ASSERT_TRUE(input.good());
    const std::string json((std::istreambuf_iterator<char>(input)),
                           std::istreambuf_iterator<char>());
    EXPECT_NE(json.find(
                  "\"schema\": \"mino.pipeline_e2e_benchmark.worker.v1\""),
              std::string::npos);
    EXPECT_NE(json.find("\"outcome\": \"failure\""), std::string::npos);
    EXPECT_NE(json.find("\\\"closed\\\"\\nretry stopped"),
              std::string::npos);
    EXPECT_NE(json.find("invalid backend_details JSON object"),
              std::string::npos);
    EXPECT_NE(json.find("\"backend_details\": {}"), std::string::npos);
    EXPECT_EQ(json.find("{\"socket\":}"), std::string::npos);

    std::filesystem::remove(output);
}

TEST(PipelineCommonTest, StartBarrierRequiresMatchingRunId) {
    const std::filesystem::path runtime =
        std::filesystem::temp_directory_path() /
        ("pipeline-start-test-" + std::to_string(getpid()));
    std::filesystem::remove_all(runtime);
    ASSERT_TRUE(std::filesystem::create_directory(runtime));

    {
        std::ofstream start(runtime / "start", std::ios::binary);
        start << "stale-run\n";
    }
    EXPECT_THROW(WaitForStartFile(runtime, "current-run", NowNs() + 1'000'000),
                 std::runtime_error);

    {
        std::ofstream start(runtime / "start", std::ios::binary | std::ios::trunc);
        start << "current-run\n";
    }
    EXPECT_TRUE(WaitForStartFile(runtime, "current-run", NowNs() + 1'000'000));
    std::filesystem::remove_all(runtime);
}

TEST(PipelineCommonTest, JsonEscapeCoversControlCharacters) {
    EXPECT_EQ(JsonEscape("a\"b\\c\n\t"), "a\\\"b\\\\c\\n\\t");
    const std::string escaped = JsonEscape(std::string("x\x01y", 3));
    EXPECT_EQ(escaped, "x\\u0001y");
}

}  // namespace
}  // namespace mino::benchmarks::pipeline
