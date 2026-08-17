// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include <unistd.h>

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
