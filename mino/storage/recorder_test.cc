// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"

namespace mino::storage {
namespace {

struct TestArtifact {
    schema::SchemaIdentity identity;
    std::vector<std::byte> bytes;
};

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_recorder_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    return path;
}

Result<TestArtifact> CompileArtifact(std::string_view type_name) {
    const std::string idl =
        "option schema_version = \"1.0\"; package recorder; message " +
        std::string(type_name) + " { uint64 value = 1; }";
    auto compiled = schema::SchemaCompiler::Compile(idl);
    if (!compiled.ok()) return compiled.status();
    std::vector<schema::LayoutPlan> layouts;
    layouts.reserve(compiled->types().size());
    for (const auto& descriptor : compiled->types()) {
        auto layout = schema::LayoutPlanner::Plan(*descriptor, {});
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    auto encoded =
        schema::codegen::EncodeDescriptorArtifact(*compiled, layouts);
    if (!encoded.ok()) return encoded.status();
    const auto bytes = std::as_bytes(
        std::span<const char>(encoded->data(), encoded->size()));
    return TestArtifact{
        .identity = compiled->types()[0]->identity(),
        .bytes = std::vector<std::byte>(bytes.begin(), bytes.end()),
    };
}

uint32_t Crc32c(std::span<const std::byte> bytes) {
    uint32_t state = 0xffffffffu;
    for (std::byte byte : bytes) {
        state ^= static_cast<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                    ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

std::vector<std::byte> Payload(uint8_t value) {
    return {static_cast<std::byte>(value),
            static_cast<std::byte>(value + 1)};
}

RecorderRecordMetadata Metadata(TopicId topic_id,
                                const schema::SchemaIdentity& identity,
                                uint64_t source_sequence,
                                std::span<const std::byte> payload,
                                uint64_t publisher_id = 1) {
    return RecorderRecordMetadata{
        .schema = RecorderSchemaMetadata{
            .short_id = identity.short_id(),
            .canonical_digest = identity.canonical_digest(),
            .schema_version = identity.schema_version(),
            .layout_version = identity.layout_version(),
        },
        .topic_id = topic_id,
        .source = MessageSource{
            .node_id = 7,
            .publisher_id = publisher_id,
            .publisher_epoch = 3,
            .source_sequence = source_sequence,
            .observed_timestamp_ns = 100 + source_sequence,
        },
        .ingestion_timestamp_ns = 1000 + source_sequence,
        .payload_size = static_cast<uint32_t>(payload.size()),
        .payload_crc = Crc32c(payload),
    };
}

RecordingSessionMetadata SessionMetadata() {
    return RecordingSessionMetadata{
        .recording_id = 100,
        .created_at_ns = 10,
        .owner_id = 20,
        .owner_epoch = 30,
        .config_version = 1,
    };
}

RecorderTopicConfig TopicConfig(TopicId topic_id, std::string name,
                                const TestArtifact& artifact,
                                uint32_t partitions = 1) {
    RecorderTopicConfig config;
    config.topic_id = topic_id;
    config.topic_name = std::move(name);
    config.config_version = 1;
    config.partition_count = partitions;
    config.policy.mode = RecordingMode::kBestEffort;
    config.policy.backpressure_topology =
        RecordBackpressureTopology::kIsolated;
    config.schemas.push_back(RecorderTopicSchema{
        .identity = artifact.identity,
        .descriptor_artifact = artifact.bytes,
    });
    return config;
}

void EnqueueOk(Recorder& recorder, uint32_t partition_id, TopicId topic_id,
               const schema::SchemaIdentity& identity, uint64_t sequence,
               uint8_t payload_value, uint64_t publisher_id = 1) {
    const std::vector<std::byte> payload = Payload(payload_value);
    auto enqueued = recorder.Enqueue(
        partition_id,
        Metadata(topic_id, identity, sequence, payload, publisher_id), payload);
    ASSERT_TRUE(enqueued.ok()) << enqueued.status().ToString();
    ASSERT_EQ(enqueued->disposition, RecorderEnqueueDisposition::kBuffered);
}

struct CrashFixture {
    std::filesystem::path root;
    std::filesystem::path partition_root;
    TestArtifact artifact;
    RecorderTopicConfig config;
    PartitionMetadata partition;
    SchemaRef schema_ref = 0;
};

Result<CrashFixture> PrepareCrashFixture(std::string_view name) {
    auto artifact = CompileArtifact("CrashRecord");
    if (!artifact.ok()) return artifact.status();
    const std::filesystem::path root = TestDirectory(name);
    RecorderTopicConfig config =
        TopicConfig(TopicId{40}, "crash", *artifact);
    auto recorder = Recorder::Create(root, SessionMetadata());
    if (!recorder.ok()) return recorder.status();
    Status added = (*recorder)->AddTopic(config);
    if (!added.ok()) return added;
    const SchemaRef schema_ref =
        (*recorder)->manifest_snapshot().topics[0].schema_snapshot[0].schema_ref;
    recorder->reset();

    const std::filesystem::path partition_root =
        root / "topics" / "40" / "partitions" / "0000";
    auto manifest = PartitionManifest::Open(partition_root);
    if (!manifest.ok()) return manifest.status();
    const PartitionMetadata partition = (*manifest)->snapshot().partition;
    manifest->reset();
    return CrashFixture{
        .root = root,
        .partition_root = partition_root,
        .artifact = std::move(*artifact),
        .config = std::move(config),
        .partition = partition,
        .schema_ref = schema_ref,
    };
}

SegmentHeader CrashHeader(const CrashFixture& fixture) {
    SegmentHeader header;
    header.recording_id = fixture.partition.recording_id;
    header.topic_id = fixture.partition.topic_id;
    header.partition_id = fixture.partition.partition_id;
    header.writer_id = fixture.partition.writer_id;
    header.first_ingestion_sequence = 1;
    header.created_at_ns = 100;
    return header;
}

Record CrashRecord(const CrashFixture& fixture, uint64_t sequence) {
    Record record;
    record.header.schema_ref = fixture.schema_ref;
    record.header.schema_version = fixture.artifact.identity.schema_version();
    record.header.layout_version = fixture.artifact.identity.layout_version();
    record.header.topic_id = fixture.partition.topic_id;
    record.header.partition_id = fixture.partition.partition_id;
    record.header.ingestion_sequence = sequence;
    record.header.ingestion_timestamp_ns = 1000 + sequence;
    record.header.node_id = 1;
    record.header.publisher_id = 2;
    record.header.publisher_epoch = 3;
    record.header.source_sequence = sequence;
    record.header.observed_timestamp_ns = 2000 + sequence;
    record.payload = Payload(static_cast<uint8_t>(sequence));
    return record;
}

Result<std::vector<std::byte>> CrashSegmentBytes(
    const CrashFixture& fixture, std::span<const Record> records) {
    auto header = EncodeSegmentHeader(CrashHeader(fixture));
    if (!header.ok()) return header.status();
    std::vector<std::byte> bytes = std::move(*header);
    for (const Record& record : records) {
        auto encoded = EncodeRecord(record);
        if (!encoded.ok()) return encoded.status();
        bytes.insert(bytes.end(), encoded->begin(), encoded->end());
    }
    return bytes;
}

Status InstallActiveSegment(
    const CrashFixture& fixture, SegmentPersistentState state,
    const std::optional<std::vector<std::byte>>& file_bytes) {
    auto manifest = PartitionManifest::Open(fixture.partition_root);
    if (!manifest.ok()) return manifest.status();
    SegmentManifestEntry entry{
        .segment_id = 1,
        .state = SegmentPersistentState::kCreating,
        .first_ingestion_sequence = 1,
        .last_ingestion_sequence = 1,
        .created_at_ns = 100,
        .sealed_at_ns = 0,
        .size_bytes = kEncodedSegmentHeaderSize,
        .relative_path = std::filesystem::path("segments") / "00000001.mino",
    };
    Status status = (*manifest)->AddSegment(entry);
    if (!status.ok()) return status;
    if (state == SegmentPersistentState::kOpen) {
        entry.state = state;
        status = (*manifest)->UpdateSegment(entry);
        if (!status.ok()) return status;
    }
    manifest->reset();
    if (!file_bytes.has_value()) return Status::Ok();

    std::error_code error;
    std::filesystem::create_directories(fixture.partition_root / "segments",
                                        error);
    if (error) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot create crash segment directory");
    }
    std::ofstream output(fixture.partition_root / "segments" /
                             "00000001.mino",
                         std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot create crash segment file");
    }
    output.write(reinterpret_cast<const char*>(file_bytes->data()),
                 static_cast<std::streamsize>(file_bytes->size()));
    if (!output.good()) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot write crash segment file");
    }
    return Status::Ok();
}

std::vector<std::byte> ReadTestFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    const std::string data{std::istreambuf_iterator<char>(input),
                           std::istreambuf_iterator<char>()};
    const auto bytes =
        std::as_bytes(std::span<const char>(data.data(), data.size()));
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

void ExpectRecovered(const CrashFixture& fixture, bool expect_recovery_gap,
                     uint64_t expected_last_sequence) {
    auto reopened = Recorder::Open(fixture.root);
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    RecorderTopicConfig config = fixture.config;
    config.schemas[0].descriptor_artifact.clear();
    ASSERT_TRUE((*reopened)->AddTopic(config).ok());
    auto runtime =
        (*reopened)->GetPartitionStatus(TopicId{40}, 0, UINT64_MAX);
    ASSERT_TRUE(runtime.ok()) << runtime.status().ToString();
    EXPECT_EQ(runtime->next_ingestion_sequence,
              expected_last_sequence + 1);
    reopened->reset();

    auto manifest = PartitionManifest::Open(fixture.partition_root);
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
    const PartitionManifestSnapshot& snapshot = (*manifest)->snapshot();
    ASSERT_EQ(snapshot.segments.size(), 1u);
    const SegmentManifestEntry& segment = snapshot.segments[0];
    EXPECT_EQ(segment.state, SegmentPersistentState::kSealed);
    EXPECT_EQ(segment.last_ingestion_sequence, expected_last_sequence);
    ASSERT_TRUE(snapshot.checkpoint.has_value());
    EXPECT_EQ(snapshot.checkpoint->segment_id, segment.segment_id);
    EXPECT_EQ(snapshot.checkpoint->durable_offset, segment.size_bytes);
    EXPECT_EQ(snapshot.checkpoint->durable_sequence,
              expected_last_sequence);

    const std::vector<std::byte> bytes = ReadTestFile(
        fixture.partition_root / segment.relative_path);
    ASSERT_EQ(bytes.size(), segment.size_bytes);
    ASSERT_GT(bytes.size(), kEncodedSegmentHeaderSize);
    auto header = DecodeSegmentHeader(
        std::span<const std::byte>(bytes).first(kEncodedSegmentHeaderSize));
    ASSERT_TRUE(header.ok()) << header.status().ToString();
    auto decoded = DecodeRecord(
        std::span<const std::byte>(bytes).subspan(kEncodedSegmentHeaderSize));
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded->header.ingestion_sequence,
              expected_last_sequence);
    if (expect_recovery_gap) {
        EXPECT_EQ(decoded->header.flags, kRecordFlagGap);
        EXPECT_EQ(decoded->header.schema_ref, 0u);
        auto gap = DecodeGapPayload(decoded->payload);
        ASSERT_TRUE(gap.ok()) << gap.status().ToString();
        EXPECT_EQ(gap->reason, GapReason::kRecorderRestartRecovery);
    } else {
        EXPECT_EQ(decoded->header.flags, 0u);
        EXPECT_EQ(decoded->header.schema_ref, fixture.schema_ref);
    }
}

TEST(RecorderTest, DrivesTwoTopicsAndMultiplePartitionsIndependently) {
    auto alpha = CompileArtifact("Alpha");
    auto beta = CompileArtifact("Beta");
    ASSERT_TRUE(alpha.ok()) << alpha.status().ToString();
    ASSERT_TRUE(beta.ok()) << beta.status().ToString();
    auto recorder = Recorder::Create(TestDirectory("multi"), SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    ASSERT_TRUE((*recorder)
                    ->AddTopic(TopicConfig(TopicId{10}, "alpha", *alpha, 2))
                    .ok());
    ASSERT_TRUE((*recorder)
                    ->AddTopic(TopicConfig(TopicId{20}, "beta", *beta))
                    .ok());
    ASSERT_EQ((*recorder)->manifest_snapshot().topics.size(), 2u);
    ASSERT_TRUE((*recorder)->Start(1000).ok());

    EnqueueOk(**recorder, 0, TopicId{10}, alpha->identity, 1, 1);
    EnqueueOk(**recorder, 1, TopicId{10}, alpha->identity, 1, 2, 2);
    EnqueueOk(**recorder, 0, TopicId{20}, beta->identity, 1, 3);
    auto pumped = (*recorder)->Pump(1002);
    ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
    EXPECT_EQ(pumped->partitions_visited, 3u);
    EXPECT_EQ(pumped->data_records, 3u);
    EXPECT_EQ((*recorder)->GetPartitionStatus(TopicId{10}, 0, 1002)
                  ->next_ingestion_sequence,
              2u);
    EXPECT_EQ((*recorder)->GetPartitionStatus(TopicId{10}, 1, 1002)
                  ->next_ingestion_sequence,
              2u);
    EXPECT_EQ((*recorder)->GetPartitionStatus(TopicId{20}, 0, 1002)
                  ->next_ingestion_sequence,
              2u);
}

TEST(RecorderTest, HotKeyIsolatedByConservedPartitionBudgets) {
    auto artifact = CompileArtifact("HotKey");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    auto recorder = Recorder::Create(TestDirectory("hot_key"), SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    RecorderTopicConfig config =
        TopicConfig(TopicId{19}, "hot-key", *artifact, 2);
    config.partition_strategy = TopicPartitionStrategy::kKey;
    config.total_buffer_byte_limit = 2u * kRecorderSmallBufferClassBytes;
    config.buffer_pool_options.global_byte_limit =
        config.total_buffer_byte_limit;
    config.buffer_pool_options.default_topic_byte_limit =
        config.total_buffer_byte_limit;
    config.buffer_pool_options.queue_capacity = 1;
    ASSERT_TRUE((*recorder)->AddTopic(config).ok());
    ASSERT_TRUE((*recorder)->Start(1).ok());

    std::array<std::byte, 1> hot_key{std::byte{0}};
    std::array<std::byte, 1> cool_key{std::byte{1}};
    const TopicPartitionMap map{
        .map_version = config.partition_map_version,
        .generation = config.partition_generation,
        .partition_count = config.partition_count,
        .strategy = config.partition_strategy,
        .state = TopicPartitionMapState::kActive,
        .hash_algorithm_version = config.partition_hash_version,
        .hash_seed = config.partition_hash_seed,
    };
    const uint32_t hot_partition = SelectTopicPartition(
        map, TopicPartitionRouteInput{
                 .key = hot_key,
                 .hash = std::nullopt,
                 .manual_partition_id = std::nullopt,
                 .source = {}})
                                       .value();
    for (uint16_t candidate = 1;
         SelectTopicPartition(
             map, TopicPartitionRouteInput{
                      .key = cool_key,
                      .hash = std::nullopt,
                      .manual_partition_id = std::nullopt,
                      .source = {}})
                 .value() == hot_partition;
         ++candidate) {
        ASSERT_LT(candidate, 256u);
        cool_key[0] = static_cast<std::byte>(candidate);
    }

    const std::vector<std::byte> first_payload = Payload(1);
    RecorderEnqueueRequest first{
        .partition_id = 0,
        .partition_key = hot_key,
        .partition_hash = std::nullopt,
        .metadata = Metadata(TopicId{19}, artifact->identity, 1,
                             first_payload),
        .payload = first_payload,
        .user_tag = 0,
        .available_cursor = std::nullopt,
        .timeout = std::chrono::nanoseconds::zero(),
    };
    auto first_result = (*recorder)->Enqueue(first);
    ASSERT_TRUE(first_result.ok()) << first_result.status().ToString();
    EXPECT_EQ(first_result->disposition, RecorderEnqueueDisposition::kBuffered);
    EXPECT_EQ(first_result->partition_id, hot_partition);

    const std::vector<std::byte> second_payload = Payload(2);
    RecorderEnqueueRequest second = first;
    second.metadata = Metadata(TopicId{19}, artifact->identity, 2,
                               second_payload);
    second.payload = second_payload;
    auto hot_result = (*recorder)->Enqueue(second);
    ASSERT_TRUE(hot_result.ok()) << hot_result.status().ToString();
    EXPECT_EQ(hot_result->disposition, RecorderEnqueueDisposition::kDropped);

    RecorderEnqueueRequest cool = second;
    cool.partition_key = cool_key;
    auto cool_result = (*recorder)->Enqueue(cool);
    ASSERT_TRUE(cool_result.ok()) << cool_result.status().ToString();
    EXPECT_EQ(cool_result->disposition, RecorderEnqueueDisposition::kBuffered);
    EXPECT_NE(cool_result->partition_id, hot_partition);

    const auto hot_status =
        (*recorder)->GetPartitionStatus(TopicId{19}, hot_partition, 2000);
    const auto cool_status = (*recorder)->GetPartitionStatus(
        TopicId{19}, cool_result->partition_id, 2000);
    ASSERT_TRUE(hot_status.ok());
    ASSERT_TRUE(cool_status.ok());
    EXPECT_LE(hot_status->buffer_pool.bytes_in_use,
              kRecorderSmallBufferClassBytes);
    EXPECT_LE(cool_status->buffer_pool.bytes_in_use,
              kRecorderSmallBufferClassBytes);
    EXPECT_LE(hot_status->buffer_pool.bytes_in_use +
                  cool_status->buffer_pool.bytes_in_use,
              config.total_buffer_byte_limit);
    ASSERT_TRUE((*recorder)->Stop(2000).ok());
}

TEST(RecorderTest, PersistsSchemaBeforeAdmissionAndRejectsIllegalPolicy) {
    auto artifact = CompileArtifact("Ordered");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestDirectory("schema_order");
    auto recorder = Recorder::Create(root, SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    RecorderTopicConfig config =
        TopicConfig(TopicId{11}, "ordered", *artifact);
    ASSERT_TRUE((*recorder)->AddTopic(config).ok());
    const TopicTableEntry& topic =
        (*recorder)->manifest_snapshot().topics.front();
    ASSERT_EQ(topic.schema_snapshot.size(), 1u);
    EXPECT_TRUE(std::filesystem::is_regular_file(
        root / topic.schema_snapshot.front().descriptor_path));

    RecorderTopicConfig invalid =
        TopicConfig(TopicId{12}, "invalid", *artifact);
    invalid.policy.mode = RecordingMode::kDurable;
    invalid.policy.backpressure_topology =
        RecordBackpressureTopology::kBestEffort;
    EXPECT_EQ((*recorder)->AddTopic(invalid).code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ((*recorder)->manifest_snapshot().topics.size(), 1u);
}

TEST(RecorderTest, ConvertsBufferDropIntoTopologyDebtAndWriterGap) {
    auto artifact = CompileArtifact("Drop");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    auto recorder = Recorder::Create(TestDirectory("drop"), SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    RecorderTopicConfig config =
        TopicConfig(TopicId{13}, "drop", *artifact);
    config.buffer_pool_options.global_byte_limit =
        kRecorderSmallBufferClassBytes;
    config.buffer_pool_options.default_topic_byte_limit =
        kRecorderSmallBufferClassBytes;
    config.buffer_pool_options.queue_capacity = 1;
    ASSERT_TRUE((*recorder)->AddTopic(config).ok());
    ASSERT_TRUE((*recorder)->Start(1000).ok());

    EnqueueOk(**recorder, 0, TopicId{13}, artifact->identity, 1, 1);
    const std::vector<std::byte> dropped_payload = Payload(2);
    auto dropped = (*recorder)->Enqueue(
        0, Metadata(TopicId{13}, artifact->identity, 2, dropped_payload),
        dropped_payload);
    ASSERT_TRUE(dropped.ok()) << dropped.status().ToString();
    EXPECT_EQ(dropped->disposition, RecorderEnqueueDisposition::kDropped);
    ASSERT_EQ(dropped->gap_debts.size(), 1u);
    EXPECT_EQ(dropped->gap_debts[0].record_count(), 1u);

    ASSERT_TRUE((*recorder)->Pump(1002).ok());
    EnqueueOk(**recorder, 0, TopicId{13}, artifact->identity, 3, 3);
    auto pumped = (*recorder)->Pump(1003);
    ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
    EXPECT_EQ(pumped->data_records, 1u);
    EXPECT_EQ(pumped->gap_records, 1u);
    EXPECT_EQ((*recorder)->metrics().dropped_records, 1u);
    EXPECT_EQ((*recorder)->metrics().gap_debts, 1u);
}

TEST(RecorderTest, DurableFlushStopAndOpenContinueTheSession) {
    auto artifact = CompileArtifact("Durable");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestDirectory("reopen");
    RecorderTopicConfig config =
        TopicConfig(TopicId{14}, "durable", *artifact);
    config.policy.mode = RecordingMode::kDurable;
    config.policy.backpressure_topology =
        RecordBackpressureTopology::kIsolated;
    config.policy.full_policy = BufferFullPolicy::kBlock;
    config.policy.ack_level = RecordAckLevel::kDurable;
    config.policy.sync_policy = SegmentSyncPolicy::kPerBatch;

    auto recorder = Recorder::Create(root, SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    ASSERT_TRUE((*recorder)->AddTopic(config).ok());
    ASSERT_TRUE((*recorder)->Start(1000).ok());
    EnqueueOk(**recorder, 0, TopicId{14}, artifact->identity, 1, 1);
    ASSERT_TRUE((*recorder)->Flush(RecordAckLevel::kDurable, 1002).ok());
    auto before_stop =
        (*recorder)->GetPartitionStatus(TopicId{14}, 0, 1002);
    ASSERT_TRUE(before_stop.ok()) << before_stop.status().ToString();
    EXPECT_EQ(before_stop->next_ingestion_sequence, 2u);
    EXPECT_GE((*recorder)->metrics().durable_records, 1u);
    ASSERT_TRUE((*recorder)->Stop(1003).ok());
    recorder->reset();

    auto reopened = Recorder::Open(root);
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    config.schemas[0].descriptor_artifact.clear();
    ASSERT_TRUE((*reopened)->AddTopic(config).ok());
    ASSERT_TRUE((*reopened)->Start(1004).ok());
    EnqueueOk(**reopened, 0, TopicId{14}, artifact->identity, 2, 2);
    ASSERT_TRUE((*reopened)->Stop(1005).ok());
    auto status = (*reopened)->GetPartitionStatus(TopicId{14}, 0, 1005);
    ASSERT_TRUE(status.ok()) << status.status().ToString();
    EXPECT_EQ(status->next_ingestion_sequence, 3u);
}

TEST(RecorderRecoveryTest,
     RecoversMissingHeaderOnlyAndMidRecordReservationsWithGap) {
    enum class CrashPoint { kBeforeFile, kHeaderOnly, kMidRecord };
    const std::vector<CrashPoint> points = {
        CrashPoint::kBeforeFile,
        CrashPoint::kHeaderOnly,
        CrashPoint::kMidRecord,
    };
    size_t index = 0;
    for (CrashPoint point : points) {
        SCOPED_TRACE(index);
        auto prepared = PrepareCrashFixture(
            "active_tail_" + std::to_string(index++));
        ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
        CrashFixture fixture = std::move(*prepared);
        std::optional<std::vector<std::byte>> bytes;
        SegmentPersistentState state = SegmentPersistentState::kCreating;
        if (point != CrashPoint::kBeforeFile) {
            auto encoded = CrashSegmentBytes(fixture, {});
            ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
            bytes = std::move(*encoded);
            state = SegmentPersistentState::kOpen;
        }
        if (point == CrashPoint::kMidRecord) {
            const Record record = CrashRecord(fixture, 1);
            auto encoded = CrashSegmentBytes(
                fixture, std::span<const Record>(&record, 1));
            ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
            ASSERT_GT(encoded->size(), kEncodedSegmentHeaderSize + 16u);
            encoded->resize(encoded->size() - 10);
            bytes = std::move(*encoded);
        }
        ASSERT_TRUE(InstallActiveSegment(fixture, state, bytes).ok());
        ExpectRecovered(fixture, true, 1);
    }
}

TEST(RecorderRecoveryTest,
     SealsCompleteRecordWrittenBeforeManifestProgressUpdate) {
    auto prepared = PrepareCrashFixture("complete_before_manifest");
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    CrashFixture fixture = std::move(*prepared);
    const Record record = CrashRecord(fixture, 1);
    auto bytes = CrashSegmentBytes(
        fixture, std::span<const Record>(&record, 1));
    ASSERT_TRUE(bytes.ok()) << bytes.status().ToString();
    ASSERT_TRUE(InstallActiveSegment(fixture, SegmentPersistentState::kOpen,
                                     *bytes)
                    .ok());
    ExpectRecovered(fixture, false, 1);
}

TEST(RecorderRecoveryTest, ReconcilesSealedSegmentMissingCheckpoint) {
    auto prepared = PrepareCrashFixture("sealed_before_checkpoint");
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    CrashFixture fixture = std::move(*prepared);
    const Record record = CrashRecord(fixture, 1);
    auto bytes = CrashSegmentBytes(
        fixture, std::span<const Record>(&record, 1));
    ASSERT_TRUE(bytes.ok()) << bytes.status().ToString();
    ASSERT_TRUE(InstallActiveSegment(fixture, SegmentPersistentState::kOpen,
                                     *bytes)
                    .ok());
    auto manifest = PartitionManifest::Open(fixture.partition_root);
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
    SegmentManifestEntry segment = (*manifest)->snapshot().segments.back();
    segment.size_bytes = bytes->size();
    ASSERT_TRUE((*manifest)->UpdateSegment(segment).ok());
    segment.state = SegmentPersistentState::kSealed;
    segment.sealed_at_ns = 101;
    ASSERT_TRUE((*manifest)->UpdateSegment(segment).ok());
    manifest->reset();

    ExpectRecovered(fixture, false, 1);
}

TEST(RecorderRecoveryTest, CommittedInteriorCorruptionFailsClosed) {
    auto prepared = PrepareCrashFixture("interior_corruption");
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    CrashFixture fixture = std::move(*prepared);
    const std::vector<Record> records = {CrashRecord(fixture, 1),
                                         CrashRecord(fixture, 2)};
    auto bytes = CrashSegmentBytes(fixture, records);
    ASSERT_TRUE(bytes.ok()) << bytes.status().ToString();
    ASSERT_GT(bytes->size(), kEncodedSegmentHeaderSize + 112u);
    (*bytes)[kEncodedSegmentHeaderSize + 112] ^= std::byte{1};
    const uint64_t original_size = bytes->size();
    ASSERT_TRUE(InstallActiveSegment(fixture, SegmentPersistentState::kOpen,
                                     *bytes)
                    .ok());

    auto reopened = Recorder::Open(fixture.root);
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    fixture.config.schemas[0].descriptor_artifact.clear();
    const Status failed = (*reopened)->AddTopic(fixture.config);
    EXPECT_EQ(failed.code(), StatusCode::kCorruption);
    EXPECT_EQ(std::filesystem::file_size(
                  fixture.partition_root / "segments" / "00000001.mino"),
              original_size);
}

TEST(RecorderRecoveryTest, RejectsActiveSegmentSymlink) {
    auto prepared = PrepareCrashFixture("active_symlink");
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    CrashFixture fixture = std::move(*prepared);
    ASSERT_TRUE(InstallActiveSegment(fixture,
                                     SegmentPersistentState::kCreating,
                                     std::nullopt)
                    .ok());
    std::error_code error;
    std::filesystem::create_directories(fixture.partition_root / "segments",
                                        error);
    ASSERT_FALSE(error);
    const std::filesystem::path outside = fixture.root / "outside.mino";
    auto header = CrashSegmentBytes(fixture, {});
    ASSERT_TRUE(header.ok()) << header.status().ToString();
    std::ofstream output(outside, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(header->data()),
                 static_cast<std::streamsize>(header->size()));
    output.close();
    std::filesystem::create_symlink(
        outside,
        fixture.partition_root / "segments" / "00000001.mino", error);
    ASSERT_FALSE(error);

    auto reopened = Recorder::Open(fixture.root);
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    fixture.config.schemas[0].descriptor_artifact.clear();
    EXPECT_EQ((*reopened)->AddTopic(fixture.config).code(),
              StatusCode::kCorruption);
}

TEST(RecorderTest, FailRecordingPolicyFailsEveryPartitionInTheTopic) {
    auto artifact = CompileArtifact("FailRecording");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    auto recorder = Recorder::Create(TestDirectory("fail_recording"),
                                     SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    RecorderTopicConfig config =
        TopicConfig(TopicId{17}, "fail", *artifact, 2);
    config.policy.full_policy = BufferFullPolicy::kFailRecording;
    config.buffer_pool_options.global_byte_limit =
        kRecorderSmallBufferClassBytes;
    config.buffer_pool_options.default_topic_byte_limit =
        kRecorderSmallBufferClassBytes;
    config.buffer_pool_options.queue_capacity = 1;
    ASSERT_TRUE((*recorder)->AddTopic(config).ok());
    ASSERT_TRUE((*recorder)->Start(1000).ok());
    EnqueueOk(**recorder, 0, TopicId{17}, artifact->identity, 1, 1);

    const std::vector<std::byte> payload = Payload(2);
    auto failed = (*recorder)->Enqueue(
        0, Metadata(TopicId{17}, artifact->identity, 2, payload), payload);
    ASSERT_TRUE(failed.ok()) << failed.status().ToString();
    EXPECT_EQ(failed->disposition, RecorderEnqueueDisposition::kFailed);
    EXPECT_EQ(failed->status.code(), StatusCode::kResourceExhausted);
    for (uint32_t partition = 0; partition < 2; ++partition) {
        auto status = (*recorder)->GetPartitionStatus(TopicId{17}, partition,
                                                      1002);
        ASSERT_TRUE(status.ok()) << status.status().ToString();
        EXPECT_EQ(status->topology_state, RecordingTopologyState::kFailed);
        EXPECT_EQ(status->error_status.code(),
                  StatusCode::kResourceExhausted);
        EXPECT_TRUE(status->buffer_pool.closed);
    }
}

struct FailingWrite {
    size_t calls = 0;
};

std::ptrdiff_t FailEveryWrite(int, const std::byte*, size_t,
                              void* context) noexcept {
    auto* failing = static_cast<FailingWrite*>(context);
    ++failing->calls;
    errno = EIO;
    return -1;
}

int FailEverySync(int, void* context) noexcept {
    auto* failing = static_cast<FailingWrite*>(context);
    ++failing->calls;
    errno = EIO;
    return -1;
}

TEST(RecorderTest,
     SnapshotOverwritesWithoutSegmentsContinuesAndValidatesManifestIdentity) {
    auto artifact = CompileArtifact("SnapshotState");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestDirectory("snapshot");
    RecorderTopicConfig config =
        TopicConfig(TopicId{18}, "snapshot", *artifact);
    config.policy.mode = RecordingMode::kSnapshot;
    config.policy.is_state_topic = true;
    // Snapshot mode must not instantiate or validate an ordinary buffer pool.
    config.buffer_pool_options.global_byte_limit = 0;
    config.buffer_pool_options.default_topic_byte_limit = 0;
    config.buffer_pool_options.queue_capacity = 0;

    auto recorder = Recorder::Create(root, SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    ASSERT_TRUE((*recorder)->AddTopic(config).ok());
    ASSERT_TRUE((*recorder)->Start(1000).ok());
    for (uint64_t sequence = 1; sequence <= 3; ++sequence) {
        const std::vector<std::byte> payload =
            Payload(static_cast<uint8_t>(sequence));
        auto enqueued = (*recorder)->Enqueue(
            0, Metadata(TopicId{18}, artifact->identity, sequence, payload),
            payload);
        ASSERT_TRUE(enqueued.ok()) << enqueued.status().ToString();
        EXPECT_EQ(enqueued->disposition,
                  RecorderEnqueueDisposition::kBuffered);
        EXPECT_FALSE(enqueued->acknowledged.has_value());
    }
    auto pumped = (*recorder)->Pump(1004);
    ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
    EXPECT_EQ(pumped->partitions_visited, 1u);
    EXPECT_EQ(pumped->dequeued_records, 0u);
    EXPECT_EQ(pumped->data_records, 0u);
    ASSERT_TRUE((*recorder)->Flush(RecordAckLevel::kDurable, 1004).ok());
    ASSERT_TRUE((*recorder)->Flush(RecordAckLevel::kDurable, 1004).ok());

    auto status = (*recorder)->GetPartitionStatus(TopicId{18}, 0, 1004);
    ASSERT_TRUE(status.ok()) << status.status().ToString();
    EXPECT_EQ(status->recording_mode, RecordingMode::kSnapshot);
    EXPECT_TRUE(status->has_snapshot);
    EXPECT_EQ(status->writer_state, TopicWriterState::kRunning);
    EXPECT_EQ(status->next_ingestion_sequence, 4u);
    EXPECT_EQ(status->buffer_pool.bytes_in_use, 0u);
    EXPECT_EQ((*recorder)->metrics().buffered_records, 0u);
    EXPECT_EQ((*recorder)->metrics().written_records, 3u);
    EXPECT_EQ((*recorder)->metrics().durable_records, 3u);
    ASSERT_TRUE((*recorder)->Stop(1005).ok());
    ASSERT_TRUE((*recorder)->Stop(1005).ok());
    ASSERT_TRUE((*recorder)
                    ->Flush(RecordAckLevel::kDurable, 1005)
                    .ok());
    recorder->reset();

    const std::filesystem::path partition_root =
        root / "topics" / "18" / "partitions" / "0000";
    EXPECT_TRUE(std::filesystem::is_regular_file(partition_root /
                                                "snapshot.mino"));
    EXPECT_FALSE(std::filesystem::exists(partition_root / "segments"));
    EXPECT_TRUE(std::filesystem::is_regular_file(partition_root / "manifest"));
    auto partition_manifest = PartitionManifest::Open(partition_root);
    ASSERT_TRUE(partition_manifest.ok())
        << partition_manifest.status().ToString();
    const PartitionManifestSnapshot& partition_snapshot =
        (*partition_manifest)->snapshot();
    EXPECT_EQ(partition_snapshot.partition.recording_id, 100u);
    EXPECT_EQ(partition_snapshot.partition.topic_id, 18u);
    EXPECT_EQ(partition_snapshot.partition.partition_id, 0u);
    EXPECT_EQ(partition_snapshot.partition.owner_epoch, 30u);
    EXPECT_EQ(partition_snapshot.partition.config_version, 1u);
    EXPECT_NE(partition_snapshot.partition.writer_id, 0u);
    EXPECT_TRUE(partition_snapshot.segments.empty());
    EXPECT_FALSE(partition_snapshot.checkpoint.has_value());
    const PartitionMetadata persisted_partition = partition_snapshot.partition;
    partition_manifest->reset();

    const std::vector<std::byte> bytes =
        ReadTestFile(partition_root / "snapshot.mino");
    ASSERT_GT(bytes.size(), kEncodedSegmentHeaderSize);
    auto snapshot_header = DecodeSegmentHeader(
        std::span<const std::byte>(bytes).first(kEncodedSegmentHeaderSize));
    ASSERT_TRUE(snapshot_header.ok()) << snapshot_header.status().ToString();
    EXPECT_EQ(snapshot_header->recording_id,
              persisted_partition.recording_id);
    EXPECT_EQ(snapshot_header->topic_id, persisted_partition.topic_id);
    EXPECT_EQ(snapshot_header->partition_id,
              persisted_partition.partition_id);
    EXPECT_EQ(snapshot_header->writer_id, persisted_partition.writer_id);
    auto latest = DecodeRecord(
        std::span<const std::byte>(bytes).subspan(kEncodedSegmentHeaderSize));
    ASSERT_TRUE(latest.ok()) << latest.status().ToString();
    EXPECT_EQ(latest->header.ingestion_sequence, 3u);
    EXPECT_EQ(latest->header.source_sequence, 3u);
    EXPECT_EQ(latest->payload, Payload(3));

    auto reopened = Recorder::Open(root);
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    config.schemas[0].descriptor_artifact.clear();
    ASSERT_TRUE((*reopened)->AddTopic(config).ok());
    auto restored =
        (*reopened)->GetPartitionStatus(TopicId{18}, 0, 1006);
    ASSERT_TRUE(restored.ok()) << restored.status().ToString();
    EXPECT_TRUE(restored->has_snapshot);
    EXPECT_EQ(restored->next_ingestion_sequence, 4u);
    ASSERT_TRUE((*reopened)->Start(1006).ok());
    const std::vector<std::byte> payload = Payload(9);
    auto enqueued = (*reopened)->Enqueue(
        0, Metadata(TopicId{18}, artifact->identity, 99, payload, 77), payload);
    ASSERT_TRUE(enqueued.ok()) << enqueued.status().ToString();
    EXPECT_FALSE(enqueued->acknowledged.has_value());
    EXPECT_EQ((*reopened)
                  ->GetPartitionStatus(TopicId{18}, 0, 1100)
                  ->next_ingestion_sequence,
              5u);
    ASSERT_TRUE((*reopened)->Stop(1100).ok());
    reopened->reset();

    const std::vector<std::byte> reopened_bytes =
        ReadTestFile(partition_root / "snapshot.mino");
    auto reopened_latest = DecodeRecord(
        std::span<const std::byte>(reopened_bytes)
            .subspan(kEncodedSegmentHeaderSize));
    ASSERT_TRUE(reopened_latest.ok())
        << reopened_latest.status().ToString();
    EXPECT_EQ(reopened_latest->header.ingestion_sequence, 4u);
    EXPECT_EQ(reopened_latest->header.node_id, 7u);
    EXPECT_EQ(reopened_latest->header.publisher_id, 77u);
    EXPECT_EQ(reopened_latest->header.publisher_epoch, 3u);
    EXPECT_EQ(reopened_latest->header.source_sequence, 99u);
    EXPECT_EQ(reopened_latest->header.observed_timestamp_ns, 199u);
    EXPECT_EQ(reopened_latest->payload, payload);

    std::vector<std::byte> mismatched_bytes = reopened_bytes;
    auto mismatched_header = DecodeSegmentHeader(
        std::span<const std::byte>(mismatched_bytes)
            .first(kEncodedSegmentHeaderSize));
    ASSERT_TRUE(mismatched_header.ok())
        << mismatched_header.status().ToString();
    ++mismatched_header->writer_id;
    auto encoded_mismatched_header = EncodeSegmentHeader(*mismatched_header);
    ASSERT_TRUE(encoded_mismatched_header.ok())
        << encoded_mismatched_header.status().ToString();
    std::copy(encoded_mismatched_header->begin(),
              encoded_mismatched_header->end(), mismatched_bytes.begin());
    std::ofstream output(partition_root / "snapshot.mino",
                         std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output.write(reinterpret_cast<const char*>(mismatched_bytes.data()),
                 static_cast<std::streamsize>(mismatched_bytes.size()));
    output.close();
    ASSERT_TRUE(output.good());

    auto mismatched = Recorder::Open(root);
    ASSERT_TRUE(mismatched.ok()) << mismatched.status().ToString();
    const Status identity_error = (*mismatched)->AddTopic(config);
    EXPECT_EQ(identity_error.code(), StatusCode::kCorruption);
}

TEST(RecorderTest, CountsRealStorageSyncFailureSeparately) {
    auto artifact = CompileArtifact("SyncFailure");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    auto recorder = Recorder::Create(TestDirectory("sync_failure"),
                                     SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    FailingWrite failing;
    RecorderTopicConfig config =
        TopicConfig(TopicId{19}, "sync-failure", *artifact);
    config.policy.mode = RecordingMode::kDurable;
    config.policy.ack_level = RecordAckLevel::kDurable;
    config.policy.sync_policy = SegmentSyncPolicy::kPerBatch;
    config.segment_options.data_sync_hook = FailEverySync;
    config.segment_options.io_hook_context = &failing;
    ASSERT_TRUE((*recorder)->AddTopic(config).ok());
    ASSERT_TRUE((*recorder)->Start(1000).ok());
    const std::vector<std::byte> payload = Payload(1);
    auto enqueued = (*recorder)->Enqueue(
        0, Metadata(TopicId{19}, artifact->identity, 1, payload), payload);
    ASSERT_TRUE(enqueued.ok()) << enqueued.status().ToString();
    EXPECT_EQ(enqueued->disposition, RecorderEnqueueDisposition::kFailed);
    EXPECT_EQ(enqueued->status.code(), StatusCode::kUnavailable);
    const RecorderMetrics metrics = (*recorder)->metrics();
    EXPECT_EQ(metrics.sync_failures, 1u);
    EXPECT_EQ(metrics.write_failures, 0u);
    EXPECT_EQ(metrics.writer_failures, 1u);
    EXPECT_GT(failing.calls, 0u);
}

TEST(RecorderTest, IsolatesWriterFailureFromHealthyTopic) {
    auto artifact = CompileArtifact("Isolated");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    auto recorder = Recorder::Create(TestDirectory("isolation"),
                                     SessionMetadata());
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    FailingWrite failing;
    RecorderTopicConfig bad =
        TopicConfig(TopicId{15}, "bad", *artifact);
    bad.segment_options.write_hook = FailEveryWrite;
    bad.segment_options.io_hook_context = &failing;
    ASSERT_TRUE((*recorder)->AddTopic(bad).ok());
    ASSERT_TRUE((*recorder)
                    ->AddTopic(TopicConfig(TopicId{16}, "good", *artifact))
                    .ok());
    ASSERT_TRUE((*recorder)->Start(1000).ok());
    EnqueueOk(**recorder, 0, TopicId{15}, artifact->identity, 1, 1);
    EnqueueOk(**recorder, 0, TopicId{16}, artifact->identity, 1, 2);
    EXPECT_GT((*recorder)->metrics().pending_bytes, 0u);

    auto pumped = (*recorder)->Pump(1002);
    ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
    ASSERT_EQ(pumped->failures.size(), 1u);
    EXPECT_EQ(pumped->failures[0].topic_id, TopicId{15});
    EXPECT_EQ(pumped->data_records, 1u);
    EXPECT_EQ((*recorder)->state(), RecorderState::kDegraded);
    EXPECT_EQ((*recorder)->error_status().code(), StatusCode::kDegraded);
    const RecorderMetrics operational = (*recorder)->metrics();
    EXPECT_EQ(operational.write_failures, 1u);
    EXPECT_EQ(operational.sync_failures, 0u);
    EXPECT_EQ(operational.writer_failures, 1u);
    EXPECT_EQ(operational.written_records, 1u);
    EXPECT_EQ(operational.pending_bytes, 0u);
    EXPECT_EQ((*recorder)->GetPartitionStatus(TopicId{15}, 0, 1002)
                  ->writer_state,
              TopicWriterState::kError);
    EXPECT_EQ((*recorder)->GetPartitionStatus(TopicId{16}, 0, 1002)
                  ->writer_state,
              TopicWriterState::kRunning);
}

TEST(RecorderCapacityTest, AddTopicCommitsExactEstimateAndDestructionReleasesIt) {
    auto artifact = CompileArtifact("CapacityRecord");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    RecorderTopicConfig first =
        TopicConfig(TopicId{90}, "capacity-first", *artifact, 2);
    auto estimate = EstimateRecorderTopicResources(first);
    ASSERT_TRUE(estimate.ok()) << estimate.status().ToString();

    capacity::NodeBudget budget;
    budget.limit = *estimate;
    auto controller_result = capacity::CapacityController::Create(budget);
    ASSERT_TRUE(controller_result.ok())
        << controller_result.status().ToString();
    auto controller = std::move(*controller_result);
    auto recorder = Recorder::Create(TestDirectory("capacity"), SessionMetadata(),
                                     {}, controller);
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    ASSERT_TRUE((*recorder)->AddTopic(first).ok());
    EXPECT_EQ(controller->Snapshot().committed, *estimate);

    RecorderTopicConfig second =
        TopicConfig(TopicId{91}, "capacity-second", *artifact, 2);
    const Status denied = (*recorder)->AddTopic(second);
    EXPECT_EQ(denied.code(), StatusCode::kResourceExhausted)
        << denied.ToString();
    EXPECT_TRUE(controller->Snapshot().pending.empty());
    EXPECT_EQ(controller->Snapshot().committed, *estimate);

    recorder->reset();
    EXPECT_TRUE(controller->Snapshot().committed.empty());
}

TEST(RecorderCapacityTest, EstimateRejectsPartitionMultiplicationOverflow) {
    auto artifact = CompileArtifact("CapacityOverflow");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    RecorderTopicConfig config =
        TopicConfig(TopicId{92}, "capacity-overflow", *artifact, 2);
    config.buffer_pool_options.global_byte_limit =
        std::numeric_limits<size_t>::max();
    EXPECT_EQ(EstimateRecorderTopicResources(config).status().code(),
              StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::storage
