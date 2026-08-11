// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/replay_engine.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/segment_format.h"

namespace mino::storage {
namespace {

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path directory =
        base / ("mino_replay_engine_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(directory, ignored);
    std::filesystem::create_directories(directory);
    return directory;
}

SchemaRefSnapshot Schema(uint32_t ref) {
    SchemaRefSnapshot schema;
    schema.schema_ref = ref;
    schema.schema_version = 0x00010000u;
    schema.layout_version = 1;
    schema.canonical_digest[0] = static_cast<std::byte>(ref);
    schema.descriptor_path =
        std::filesystem::path("schemas") /
        ("schema_" + std::to_string(ref) + ".schema");
    return schema;
}

RecordingManifestSnapshot Manifest() {
    RecordingManifestSnapshot manifest;
    manifest.generation = 1;
    manifest.session = RecordingSessionMetadata{
        .recording_id = 7,
        .created_at_ns = 100,
        .owner_id = 2,
        .owner_epoch = 3,
        .config_version = 4,
    };
    manifest.topics = {
        TopicTableEntry{.topic_id = 10,
                        .topic_name = "alpha",
                        .config_version = 4,
                        .partition_maps = {},
                        .schema_snapshot = {Schema(1)}},
        TopicTableEntry{.topic_id = 20,
                        .topic_name = "beta",
                        .config_version = 4,
                        .partition_maps = {},
                        .schema_snapshot = {Schema(2)}},
    };
    return manifest;
}

Record SampleRecord(uint32_t topic_id, uint32_t partition_id,
                    uint64_t ingestion_sequence,
                    uint64_t ingestion_timestamp_ns, uint64_t node_id,
                    uint64_t source_sequence,
                    uint64_t observed_timestamp_ns, uint32_t schema_ref) {
    Record record;
    record.header.schema_ref = schema_ref;
    record.header.schema_version = 0x00010000u;
    record.header.layout_version = 1;
    record.header.topic_id = topic_id;
    record.header.partition_id = partition_id;
    record.header.ingestion_sequence = ingestion_sequence;
    record.header.ingestion_timestamp_ns = ingestion_timestamp_ns;
    record.header.node_id = node_id;
    record.header.publisher_id = 100 + node_id;
    record.header.publisher_epoch = 3;
    record.header.source_sequence = source_sequence;
    record.header.observed_timestamp_ns = observed_timestamp_ns;
    record.payload = {static_cast<std::byte>(topic_id),
                      static_cast<std::byte>(ingestion_sequence)};
    return record;
}

void Append(std::vector<std::byte>* output,
            std::span<const std::byte> input) {
    output->insert(output->end(), input.begin(), input.end());
}

std::vector<std::byte> SegmentBytes(uint32_t topic_id, uint32_t partition_id,
                                    uint64_t first_sequence,
                                    uint64_t created_at_ns,
                                    std::span<const Record> records) {
    const SegmentHeader segment_header{
        .flags = 0,
        .recording_id = 7,
        .topic_id = topic_id,
        .partition_id = partition_id,
        .writer_id = 9,
        .first_ingestion_sequence = first_sequence,
        .created_at_ns = created_at_ns,
    };
    auto encoded_header = EncodeSegmentHeader(segment_header);
    EXPECT_TRUE(encoded_header.ok());
    if (!encoded_header.ok()) return {};
    std::vector<std::byte> bytes = *encoded_header;
    for (const Record& record : records) {
        auto encoded_record = EncodeRecord(record);
        EXPECT_TRUE(encoded_record.ok());
        if (!encoded_record.ok()) return {};
        Append(&bytes, *encoded_record);
    }
    return bytes;
}

std::filesystem::path WriteSegment(const std::filesystem::path& directory,
                                   std::string_view name, uint32_t topic_id,
                                   uint32_t partition_id,
                                   uint64_t first_sequence,
                                   uint64_t created_at_ns,
                                   std::span<const Record> records) {
    const std::filesystem::path path = directory / std::string(name);
    const std::vector<std::byte> bytes =
        SegmentBytes(topic_id, partition_id, first_sequence, created_at_ns,
                     records);
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    EXPECT_TRUE(output.is_open());
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
    EXPECT_TRUE(output.good());
    return path;
}

void WriteBytes(const std::filesystem::path& path,
                std::span<const std::byte> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(output.good());
}

struct ReplayPathReplacement {
    std::filesystem::path backup;
    bool replaced = false;
};

Status ReplaceReplayPathAfterScan(const std::filesystem::path& path,
                                  void* context) {
    auto* replacement = static_cast<ReplayPathReplacement*>(context);
    std::error_code error;
    std::filesystem::rename(path, replacement->backup, error);
    if (error) {
        return Status::Error(StatusCode::kUnavailable,
                             "test could not rename scanned replay segment");
    }
    std::filesystem::copy_file(replacement->backup, path, error);
    if (error) {
        return Status::Error(StatusCode::kUnavailable,
                             "test could not install replacement replay segment");
    }
    replacement->replaced = true;
    return Status::Ok();
}

struct PublishedMessage {
    std::string target_namespace;
    std::string topic_name;
    uint32_t topic_id = 0;
    uint32_t partition_id = 0;
    SchemaRefSnapshot schema;
    std::vector<std::byte> payload;
    ReplayMessageMetadata metadata;
};

class CapturingPublisher final : public ReplayPublisherAdapter {
public:
    Status Publish(const ReplayPublishRequest& request) noexcept override {
        try {
            messages.push_back(PublishedMessage{
                .target_namespace = std::string(request.target_namespace),
                .topic_name = std::string(request.topic_name),
                .topic_id = request.topic_id,
                .partition_id = request.partition_id,
                .schema = request.schema,
                .payload = std::vector<std::byte>(
                    request.canonical_payload.begin(),
                    request.canonical_payload.end()),
                .metadata = request.metadata,
            });
            return Status::Ok();
        } catch (...) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
    }

    std::vector<PublishedMessage> messages;
};

class FakeClock final : public ReplayClock {
public:
    uint64_t NowNs() noexcept override { return now_ns; }
    uint64_t now_ns = 0;
};

class FakeSleeper final : public ReplaySleeper {
public:
    explicit FakeSleeper(FakeClock* clock) : clock_(clock) {}

    Status SleepFor(uint64_t duration_ns) noexcept override {
        sleeps.push_back(duration_ns);
        clock_->now_ns += duration_ns;
        return Status::Ok();
    }

    FakeClock* clock_;
    std::vector<uint64_t> sleeps;
};

TEST(ReplayEngineTest, AppliesAllFiltersAndPublishesReplayMetadata) {
    const std::filesystem::path directory = TestDirectory("filters");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 100, 7, 40, 1000, 1),
        SampleRecord(10, 0, 2, 200, 8, 50, 1100, 1),
        SampleRecord(10, 0, 3, 300, 8, 60, 1200, 1),
    };
    const std::filesystem::path path =
        WriteSegment(directory, "alpha.mino", 10, 0, 1, 10, records);

    ReplayOptions options;
    options.replay_session_id = 99;
    options.timestamp_mode = ReplayTimestampMode::kPreserveOriginal;
    options.filter.topic_names = {"alpha"};
    options.filter.node_ids = {8};
    options.filter.ingestion_timestamp_ns = {.minimum = 150, .maximum = 250};
    options.filter.observed_timestamp_ns = {.minimum = 1050, .maximum = 1150};
    options.filter.source_sequence = {.minimum = 50, .maximum = 50};
    options.filter.ingestion_sequence = {.minimum = 2, .maximum = 2};
    CapturingPublisher publisher;
    FakeClock clock;
    clock.now_ns = 5000;
    FakeSleeper sleeper(&clock);

    auto engine = ReplayEngine::Create({path}, Manifest(), &publisher, options,
                                       nullptr, &clock, &sleeper);
    ASSERT_TRUE(engine.ok()) << engine.status().ToString();
    auto count = (*engine)->Run();
    ASSERT_TRUE(count.ok()) << count.status().ToString();
    EXPECT_EQ(*count, 1u);
    ASSERT_EQ(publisher.messages.size(), 1u);
    const PublishedMessage& message = publisher.messages[0];
    EXPECT_EQ(message.target_namespace, "replay");
    EXPECT_EQ(message.topic_name, "alpha");
    EXPECT_EQ(message.schema.schema_ref, 1u);
    EXPECT_EQ(message.metadata.message_origin, ReplayMessageOrigin::kReplay);
    EXPECT_EQ(message.metadata.replay_session_id, 99u);
    EXPECT_EQ(message.metadata.original_ingestion_timestamp_ns, 200u);
    EXPECT_EQ(message.metadata.original_observed_timestamp_ns, 1100u);
    EXPECT_EQ(message.metadata.replay_timestamp_ns, 5000u);
    EXPECT_EQ(message.metadata.publish_timestamp_ns, 200u);
    EXPECT_EQ(message.metadata.original_source_sequence, 50u);
    EXPECT_EQ(message.metadata.original_ingestion_sequence, 2u);
    EXPECT_TRUE(sleeper.sleeps.empty());
}

TEST(ReplayEngineTest, OrdersSegmentsByPartitionSequenceAndMergesTopicHeads) {
    const std::filesystem::path directory = TestDirectory("ordering");
    const std::vector<Record> alpha_first = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 1)};
    const std::vector<Record> alpha_second = {
        SampleRecord(10, 0, 2, 300, 1, 2, 300, 1)};
    const std::vector<Record> beta = {
        SampleRecord(20, 0, 1, 200, 2, 1, 200, 2),
        SampleRecord(20, 0, 2, 300, 2, 2, 300, 2),
    };
    const std::filesystem::path second = WriteSegment(
        directory, "alpha_2.mino", 10, 0, 2, 20, alpha_second);
    const std::filesystem::path beta_path =
        WriteSegment(directory, "beta.mino", 20, 0, 1, 10, beta);
    const std::filesystem::path first = WriteSegment(
        directory, "alpha_1.mino", 10, 0, 1, 10, alpha_first);
    CapturingPublisher publisher;
    FakeClock clock;
    FakeSleeper sleeper(&clock);

    auto engine = ReplayEngine::Create({second, beta_path, first}, Manifest(),
                                       &publisher, {}, nullptr, &clock,
                                       &sleeper);
    ASSERT_TRUE(engine.ok()) << engine.status().ToString();
    auto count = (*engine)->Run();
    ASSERT_TRUE(count.ok()) << count.status().ToString();
    EXPECT_EQ(*count, 4u);
    ASSERT_EQ(publisher.messages.size(), 4u);
    EXPECT_EQ(publisher.messages[0].topic_id, 10u);
    EXPECT_EQ(publisher.messages[0].metadata.original_ingestion_sequence, 1u);
    EXPECT_EQ(publisher.messages[1].topic_id, 20u);
    EXPECT_EQ(publisher.messages[1].metadata.original_ingestion_sequence, 1u);
    // Equal receive time is broken deterministically by topic_id.
    EXPECT_EQ(publisher.messages[2].topic_id, 10u);
    EXPECT_EQ(publisher.messages[2].metadata.original_ingestion_sequence, 2u);
    EXPECT_EQ(publisher.messages[3].topic_id, 20u);
    EXPECT_EQ(publisher.messages[3].metadata.original_ingestion_sequence, 2u);
}

TEST(ReplayEngineTest, RepartitionGenerationsMergeDeterministicallyAndFilter) {
    const std::filesystem::path root = TestDirectory("repartition_merge");
    const std::filesystem::path generation_one =
        root / "topics/10/partitions/0000/segments";
    const std::filesystem::path generation_two =
        root / "topics/10/generations/00000000000000000002/partitions/0000/segments";
    std::filesystem::create_directories(generation_one);
    std::filesystem::create_directories(generation_two);
    const std::vector<Record> old_records = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 1)};
    const std::vector<Record> new_records = {
        SampleRecord(10, 0, 1, 100, 2, 1, 100, 1)};
    const std::filesystem::path old_path = WriteSegment(
        generation_one, "old.mino", 10, 0, 1, 10, old_records);
    const std::filesystem::path new_path = WriteSegment(
        generation_two, "new.mino", 10, 0, 1, 20, new_records);

    RecordingManifestSnapshot manifest = Manifest();
    manifest.topics[0].partition_maps = {
        TopicPartitionMap{
            .map_version = 1,
            .generation = 1,
            .partition_count = 1,
            .strategy = TopicPartitionStrategy::kSource,
            .state = TopicPartitionMapState::kRetired,
        },
        TopicPartitionMap{
            .map_version = 2,
            .generation = 2,
            .partition_count = 1,
            .strategy = TopicPartitionStrategy::kSource,
            .state = TopicPartitionMapState::kActive,
        },
    };
    CapturingPublisher merged;
    auto engine = ReplayEngine::Create({new_path, old_path}, manifest, &merged,
                                       ReplayOptions{});
    ASSERT_TRUE(engine.ok()) << engine.status().ToString();
    ASSERT_TRUE((*engine)->Run().ok());
    ASSERT_EQ(merged.messages.size(), 2u);
    EXPECT_EQ(merged.messages[0].metadata.original_partition_generation, 1u);
    EXPECT_EQ(merged.messages[1].metadata.original_partition_generation, 2u);

    ReplayOptions filtered_options;
    filtered_options.filter.partition_ids = {0};
    filtered_options.filter.partition_generations = {2};
    filtered_options.filter.ingestion_sequence.minimum = 1;
    CapturingPublisher filtered;
    auto filtered_engine = ReplayEngine::Create(
        {old_path, new_path}, manifest, &filtered, filtered_options);
    ASSERT_TRUE(filtered_engine.ok())
        << filtered_engine.status().ToString();
    ASSERT_TRUE((*filtered_engine)->Run().ok());
    ASSERT_EQ(filtered.messages.size(), 1u);
    EXPECT_EQ(filtered.messages[0].metadata.original_partition_generation, 2u);
}

TEST(ReplayEngineTest, SchedulerSupportsOriginalFastSlowAndStepPlayback) {
    const std::filesystem::path directory = TestDirectory("timing");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 1000, 1, 1, 1000, 1),
        SampleRecord(10, 0, 2, 2000, 1, 2, 2000, 1),
        SampleRecord(10, 0, 3, 4000, 1, 3, 4000, 1),
    };
    const std::filesystem::path path =
        WriteSegment(directory, "timing.mino", 10, 0, 1, 10, records);

    struct TimingCase {
        double speed;
        std::vector<uint64_t> sleeps;
    };
    const std::array<TimingCase, 3> cases = {
        TimingCase{1.0, {1000, 2000}},
        TimingCase{2.0, {500, 1000}},
        TimingCase{0.5, {2000, 4000}},
    };
    for (const TimingCase& timing : cases) {
        SCOPED_TRACE(timing.speed);
        ReplayOptions options;
        options.playback.speed = timing.speed;
        CapturingPublisher publisher;
        FakeClock clock;
        clock.now_ns = 100;
        FakeSleeper sleeper(&clock);
        auto engine = ReplayEngine::Create({path}, Manifest(), &publisher,
                                           options, nullptr, &clock, &sleeper);
        ASSERT_TRUE(engine.ok()) << engine.status().ToString();
        auto count = (*engine)->Run();
        ASSERT_TRUE(count.ok()) << count.status().ToString();
        EXPECT_EQ(*count, 3u);
        EXPECT_EQ(sleeper.sleeps, timing.sleeps);
    }

    ReplayOptions step_options;
    step_options.playback.mode = ReplayPlaybackMode::kStep;
    CapturingPublisher step_publisher;
    FakeClock step_clock;
    FakeSleeper step_sleeper(&step_clock);
    auto step_engine = ReplayEngine::Create({path}, Manifest(), &step_publisher,
                                            step_options, nullptr, &step_clock,
                                            &step_sleeper);
    ASSERT_TRUE(step_engine.ok()) << step_engine.status().ToString();
    for (size_t index = 1; index <= records.size(); ++index) {
        auto stepped = (*step_engine)->Step();
        ASSERT_TRUE(stepped.ok()) << stepped.status().ToString();
        EXPECT_TRUE(*stepped);
        EXPECT_EQ(step_publisher.messages.size(), index);
    }
    auto eof = (*step_engine)->Step();
    ASSERT_TRUE(eof.ok());
    EXPECT_FALSE(*eof);
    EXPECT_TRUE(step_sleeper.sleeps.empty());
}

TEST(ReplayEngineTest, DefaultsToReplayNamespaceAndGuardsLiveInjection) {
    const std::filesystem::path directory = TestDirectory("namespace");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 1)};
    const std::filesystem::path path =
        WriteSegment(directory, "one.mino", 10, 0, 1, 10, records);
    CapturingPublisher publisher;

    ReplayOptions denied_options;
    denied_options.publish_target = ReplayPublishTarget::kLive;
    auto denied = ReplayEngine::Create({path}, Manifest(), &publisher,
                                       denied_options);
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kPermissionDenied);
    EXPECT_TRUE(publisher.messages.empty());

    ReplayOptions allowed_options;
    allowed_options.publish_target = ReplayPublishTarget::kLive;
    allowed_options.live_injection_authorized = true;
    allowed_options.live_namespace = "production";
    auto allowed = ReplayEngine::Create({path}, Manifest(), &publisher,
                                        allowed_options);
    ASSERT_TRUE(allowed.ok()) << allowed.status().ToString();
    ASSERT_TRUE((*allowed)->Run().ok());
    ASSERT_EQ(publisher.messages.size(), 1u);
    EXPECT_EQ(publisher.messages[0].target_namespace, "production");
}

TEST(ReplayEngineTest, SkipsSchemaLessGapsAndPreservesTombstoneFlag) {
    const std::filesystem::path directory = TestDirectory("control_records");
    Record gap = SampleRecord(10, 0, 1, 100, 1, 1, 100, 1);
    gap.header.flags = kRecordFlagGap;
    gap.header.schema_ref = 0;
    gap.header.schema_version = 0;
    gap.header.layout_version = 0;
    gap.payload.clear();
    Record tombstone = SampleRecord(10, 0, 2, 200, 1, 2, 200, 1);
    tombstone.header.flags = kRecordFlagTombstone;
    const std::vector<Record> records = {gap, tombstone};
    const std::filesystem::path path = WriteSegment(
        directory, "control.mino", 10, 0, 1, 10, records);
    CapturingPublisher publisher;

    auto engine = ReplayEngine::Create({path}, Manifest(), &publisher);
    ASSERT_TRUE(engine.ok()) << engine.status().ToString();
    auto count = (*engine)->Run();
    ASSERT_TRUE(count.ok()) << count.status().ToString();
    EXPECT_EQ(*count, 1u);
    ASSERT_EQ(publisher.messages.size(), 1u);
    EXPECT_EQ(publisher.messages[0].metadata.record_flags,
              kRecordFlagTombstone);
    EXPECT_EQ(publisher.messages[0].metadata.original_ingestion_sequence, 2u);
}

TEST(ReplayEngineTest, UnknownSchemaFailsClosedBeforeAnyPublish) {
    const std::filesystem::path directory = TestDirectory("schema");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 9)};
    const std::filesystem::path path =
        WriteSegment(directory, "unknown.mino", 10, 0, 1, 10, records);
    CapturingPublisher publisher;

    auto engine = ReplayEngine::Create({path}, Manifest(), &publisher);
    ASSERT_FALSE(engine.ok());
    EXPECT_EQ(engine.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_TRUE(publisher.messages.empty());
}

TEST(ReplayEngineTest, CorruptSegmentFailsClosedBeforeAnyPublish) {
    const std::filesystem::path directory = TestDirectory("corruption");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 1)};
    std::vector<std::byte> bytes = SegmentBytes(10, 0, 1, 10, records);
    ASSERT_GT(bytes.size(), kEncodedSegmentHeaderSize + 112u);
    bytes[kEncodedSegmentHeaderSize + 112] ^= std::byte{1};
    const std::filesystem::path path = directory / "corrupt.mino";
    WriteBytes(path, bytes);
    CapturingPublisher publisher;

    auto engine = ReplayEngine::Create({path}, Manifest(), &publisher);
    ASSERT_FALSE(engine.ok());
    EXPECT_EQ(engine.status().code(), StatusCode::kCorruption);
    EXPECT_TRUE(publisher.messages.empty());
}

TEST(ReplayEngineTest, InstallsNormalPublisherAfterValidationBeforePlayback) {
    const std::filesystem::path directory = TestDirectory("adapter_install");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 1)};
    const std::filesystem::path path =
        WriteSegment(directory, "one.mino", 10, 0, 1, 10, records);
    ReplayOptions options;
    options.playback.mode = ReplayPlaybackMode::kStep;

    auto engine = ReplayEngine::Create({path}, Manifest(), options);
    ASSERT_TRUE(engine.ok()) << engine.status().ToString();
    EXPECT_FALSE((*engine)->has_publisher_adapter());
    auto missing = (*engine)->Step();
    ASSERT_FALSE(missing.ok());
    EXPECT_EQ(missing.status().code(), StatusCode::kUnsupported);

    CapturingPublisher publisher;
    ASSERT_TRUE((*engine)->InstallPublisherAdapter(&publisher).ok());
    EXPECT_TRUE((*engine)->has_publisher_adapter());
    auto advanced = (*engine)->Step();
    ASSERT_TRUE(advanced.ok()) << advanced.status().ToString();
    EXPECT_TRUE(*advanced);
    ASSERT_EQ(publisher.messages.size(), 1u);
    EXPECT_FALSE((*engine)->InstallPublisherAdapter(&publisher).ok());
}

TEST(ReplayEngineTest, CountsPublishedRecordBeforeSurfacingPrefetchError) {
    const std::filesystem::path directory = TestDirectory("prefetch_error");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 1),
        SampleRecord(10, 0, 2, 200, 1, 2, 200, 1),
    };
    std::vector<std::byte> bytes = SegmentBytes(10, 0, 1, 10, records);
    const std::filesystem::path path = directory / "two.mino";
    WriteBytes(path, bytes);
    ReplayOptions options;
    options.playback.mode = ReplayPlaybackMode::kStep;
    CapturingPublisher publisher;
    auto engine = ReplayEngine::Create({path}, Manifest(), &publisher, options);
    ASSERT_TRUE(engine.ok()) << engine.status().ToString();

    auto scanned = ScanSegment(path);
    ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
    ASSERT_EQ(scanned->records.size(), 2u);
    const size_t damage = static_cast<size_t>(
        scanned->records[1].record_offset + 112u);
    ASSERT_LT(damage, bytes.size());
    bytes[damage] ^= std::byte{1};
    WriteBytes(path, bytes);

    auto first = (*engine)->Step();
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_TRUE(*first);
    EXPECT_EQ((*engine)->published_records(), 1u);
    ASSERT_EQ(publisher.messages.size(), 1u);

    auto terminal = (*engine)->Step();
    ASSERT_FALSE(terminal.ok());
    EXPECT_EQ(terminal.status().code(), StatusCode::kCorruption);
    EXPECT_EQ((*engine)->published_records(), 1u);
}

TEST(SegmentReplayReaderTest, RejectsReopenedDescriptorIdentityMismatch) {
    const std::filesystem::path directory = TestDirectory("reader_identity");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 1)};
    const std::filesystem::path path =
        WriteSegment(directory, "replaceable.mino", 10, 0, 1, 10, records);
    ReplayPathReplacement replacement{
        .backup = directory / "scanned.mino",
    };

    auto reader = SegmentReplayReader::OpenForTesting(
        path, {}, ReplaceReplayPathAfterScan, &replacement);
    EXPECT_TRUE(replacement.replaced);
    ASSERT_FALSE(reader.ok());
    EXPECT_EQ(reader.status().code(), StatusCode::kUnavailable);
    EXPECT_NE(reader.status().message().find("device/inode"),
              std::string_view::npos);
}

TEST(SegmentReplayReaderTest, DecodesFullEnvelopeAndDetectsPostScanDamage) {
    const std::filesystem::path directory = TestDirectory("reader_damage");
    const std::vector<Record> records = {
        SampleRecord(10, 0, 1, 100, 1, 1, 100, 1)};
    std::vector<std::byte> bytes = SegmentBytes(10, 0, 1, 10, records);
    const std::filesystem::path path = directory / "mutable.mino";
    WriteBytes(path, bytes);

    auto reader = SegmentReplayReader::Open(path);
    ASSERT_TRUE(reader.ok()) << reader.status().ToString();
    bytes[kEncodedSegmentHeaderSize + 112] ^= std::byte{1};
    WriteBytes(path, bytes);
    auto record = (*reader)->Next();
    ASSERT_FALSE(record.ok());
    EXPECT_EQ(record.status().code(), StatusCode::kCorruption);
}

}  // namespace
}  // namespace mino::storage
