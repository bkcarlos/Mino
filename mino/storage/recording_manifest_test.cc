// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recording_manifest.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/status.h"

namespace mino::storage {
namespace {

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_recording_manifest_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path);
    return path;
}

void WriteByte(const std::filesystem::path& path) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.put('x');
    ASSERT_TRUE(output.good());
}

std::array<std::byte, kSchemaDigestSize> Digest(uint8_t seed) {
    std::array<std::byte, kSchemaDigestSize> digest{};
    for (size_t index = 0; index < digest.size(); ++index) {
        digest[index] = static_cast<std::byte>(seed + index);
    }
    return digest;
}

std::string DigestHex(
    const std::array<std::byte, kSchemaDigestSize>& digest) {
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result;
    for (std::byte byte : digest) {
        const uint8_t value = static_cast<uint8_t>(byte);
        result.push_back(digits[value >> 4]);
        result.push_back(digits[value & 0x0fu]);
    }
    return result;
}

SchemaRefSnapshot Schema(uint32_t ref, uint8_t seed) {
    SchemaRefSnapshot schema;
    schema.schema_ref = ref;
    schema.schema_version = 0x00010002u;
    schema.layout_version = 3;
    schema.canonical_digest = Digest(seed);
    schema.descriptor_path =
        std::filesystem::path("schemas") /
        (DigestHex(schema.canonical_digest) + ".schema");
    return schema;
}

RecordingManifestSnapshot RecordingSnapshot() {
    RecordingManifestSnapshot snapshot;
    snapshot.generation = 7;
    snapshot.session = RecordingSessionMetadata{
        .recording_id = 101,
        .created_at_ns = 1000,
        .owner_id = 22,
        .owner_epoch = 3,
        .config_version = 9,
    };
    snapshot.topics = {
        TopicTableEntry{.topic_id = 10,
                        .topic_name = "camera.front",
                        .config_version = 8,
                        .partition_maps = {TopicPartitionMap{
                            .map_version = 8,
                            .generation = 1,
                            .partition_count = 4,
                            .strategy = TopicPartitionStrategy::kSource,
                            .state = TopicPartitionMapState::kActive,
                            .hash_algorithm_version =
                                kStablePartitionHashVersion,
                            .hash_seed = 99,
                        }},
                        .schema_snapshot = {Schema(1, 1), Schema(2, 33)}},
        TopicTableEntry{.topic_id = 20,
                        .topic_name = "imu",
                        .config_version = 9,
                        .partition_maps = {},
                        .schema_snapshot = {Schema(3, 65)}},
    };
    return snapshot;
}

SegmentManifestEntry Segment(uint64_t id, SegmentPersistentState state,
                             uint64_t first, uint64_t last) {
    return SegmentManifestEntry{
        .segment_id = id,
        .state = state,
        .first_ingestion_sequence = first,
        .last_ingestion_sequence = last,
        .created_at_ns = 100 + id,
        .sealed_at_ns =
            static_cast<uint8_t>(state) >=
                    static_cast<uint8_t>(SegmentPersistentState::kSealed)
                ? 200 + id
                : 0,
        .size_bytes = 4096,
        .relative_path =
            std::filesystem::path("segments") /
            ("0000000" + std::to_string(id) + ".mino"),
    };
}

PartitionManifestSnapshot PartitionSnapshot() {
    PartitionManifestSnapshot snapshot;
    snapshot.generation = 11;
    snapshot.partition = PartitionMetadata{
        .recording_id = 101,
        .topic_id = 10,
        .partition_id = 0,
        .writer_id = 44,
        .owner_epoch = 3,
        .config_version = 9,
    };
    snapshot.segments = {
        Segment(1, SegmentPersistentState::kIndexed, 1, 10),
        Segment(2, SegmentPersistentState::kOpen, 11, 20),
    };
    snapshot.checkpoint = DurableCheckpoint{
        .segment_id = 2,
        .durable_offset = 2048,
        .durable_sequence = 18,
    };
    return snapshot;
}

uint32_t Crc32c(std::span<const std::byte> bytes, size_t crc_offset) {
    uint32_t state = 0xffffffffu;
    for (size_t index = 0; index < bytes.size(); ++index) {
        uint8_t value = index >= crc_offset && index < crc_offset + 4
                            ? 0
                            : static_cast<uint8_t>(bytes[index]);
        state ^= value;
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                    ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

void WriteLe32(std::vector<std::byte>* bytes, size_t offset, uint32_t value) {
    ASSERT_LE(offset + 4, bytes->size());
    for (size_t index = 0; index < 4; ++index) {
        (*bytes)[offset + index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

TEST(RecordingManifestCodecTest, RoundTripsExplicitLittleEndianFormats) {
    const RecordingManifestSnapshot recording = RecordingSnapshot();
    auto encoded_recording = EncodeRecordingManifest(recording);
    ASSERT_TRUE(encoded_recording.ok())
        << encoded_recording.status().ToString();
    ASSERT_GE(encoded_recording->size(), 76u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded_recording)[8]), 2u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded_recording)[9]), 0u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded_recording)[10]), 76u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded_recording)[11]), 0u);
    auto decoded_recording = DecodeRecordingManifest(*encoded_recording);
    ASSERT_TRUE(decoded_recording.ok())
        << decoded_recording.status().ToString();
    EXPECT_EQ(*decoded_recording, recording);

    const PartitionManifestSnapshot partition = PartitionSnapshot();
    auto encoded_partition = EncodePartitionManifest(partition);
    ASSERT_TRUE(encoded_partition.ok())
        << encoded_partition.status().ToString();
    ASSERT_GE(encoded_partition->size(), 104u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded_partition)[8]), 2u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded_partition)[10]), 136u);
    auto decoded_partition = DecodePartitionManifest(*encoded_partition);
    ASSERT_TRUE(decoded_partition.ok())
        << decoded_partition.status().ToString();
    EXPECT_EQ(*decoded_partition, partition);
}

TEST(RecordingManifestCodecTest, RejectsCorruptionBoundsDuplicatesAndTraversal) {
    auto encoded = EncodeRecordingManifest(RecordingSnapshot());
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    (*encoded)[encoded->size() - 1] ^= std::byte{1};
    EXPECT_EQ(DecodeRecordingManifest(*encoded).status().code(),
              StatusCode::kCorruption);
    encoded->pop_back();
    EXPECT_EQ(DecodeRecordingManifest(*encoded).status().code(),
              StatusCode::kCorruption);

    ManifestLimits small;
    small.max_topics = 1;
    EXPECT_EQ(EncodeRecordingManifest(RecordingSnapshot(), small).status().code(),
              StatusCode::kResourceExhausted);

    RecordingManifestSnapshot duplicate = RecordingSnapshot();
    duplicate.topics[1].topic_name = duplicate.topics[0].topic_name;
    EXPECT_EQ(EncodeRecordingManifest(duplicate).status().code(),
              StatusCode::kCorruption);
    duplicate = RecordingSnapshot();
    duplicate.topics[0].schema_snapshot[0].descriptor_path =
        "schemas/../escape.schema";
    EXPECT_FALSE(EncodeRecordingManifest(duplicate).ok());

    auto partition = EncodePartitionManifest(PartitionSnapshot());
    ASSERT_TRUE(partition.ok()) << partition.status().ToString();
    (*partition)[136 + 8] = std::byte{99};
    WriteLe32(&*partition, 132, Crc32c(*partition, 132));
    EXPECT_EQ(DecodePartitionManifest(*partition).status().code(),
              StatusCode::kCorruption);
}

TEST(RecordingManifestTest, EnforcesOwnerTopicMappingAndRecoveryWatermarks) {
    const std::filesystem::path root = TestDirectory("topics");
    auto manifest = RecordingManifest::Create(
        root, RecordingSnapshot().session);
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
    TopicTableEntry topic{.topic_id = 10,
                          .topic_name = "camera.front",
                          .config_version = 8,
                          .partition_maps = {},
                          .schema_snapshot = {Schema(1, 1)}};
    ASSERT_TRUE((*manifest)->AddTopic(topic).ok());
    EXPECT_EQ((*manifest)->FindTopic(10)->topic_name, "camera.front");
    EXPECT_EQ((*manifest)->FindTopic("camera.front")->topic_id, 10u);

    TopicTableEntry duplicate_id = topic;
    duplicate_id.topic_name = "other";
    EXPECT_EQ((*manifest)->AddTopic(duplicate_id).code(),
              StatusCode::kAlreadyExists);
    TopicTableEntry duplicate_name = topic;
    duplicate_name.topic_id = 11;
    EXPECT_EQ((*manifest)->AddTopic(duplicate_name).code(),
              StatusCode::kAlreadyExists);
    topic.topic_name = "renamed";
    topic.config_version = 10;
    EXPECT_EQ((*manifest)->UpdateTopic(topic).code(),
              StatusCode::kInvalidArgument);
    EXPECT_FALSE((*manifest)->UpdateSessionConfigVersion(8).ok());
    EXPECT_EQ(RecordingManifest::Open(root).status().code(),
              StatusCode::kUnavailable);

    const uint64_t generation = (*manifest)->snapshot().generation;
    manifest->reset();
    ManifestOptions rollback;
    rollback.minimum_generation = generation + 1;
    EXPECT_EQ(RecordingManifest::Open(root, rollback).status().code(),
              StatusCode::kCorruption);
    rollback = {};
    rollback.minimum_config_version = 10;
    EXPECT_EQ(RecordingManifest::Open(root, rollback).status().code(),
              StatusCode::kCorruption);
}

TEST(RecordingManifestTest, RepartitionNeedsNewGenerationDrainAndCutoverProof) {
    const std::filesystem::path root = TestDirectory("repartition");
    auto manifest = RecordingManifest::Create(root, RecordingSnapshot().session);
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
    TopicPartitionMap active{
        .map_version = 1,
        .generation = 1,
        .partition_count = 1,
        .strategy = TopicPartitionStrategy::kSource,
        .state = TopicPartitionMapState::kActive,
    };
    ASSERT_TRUE((*manifest)
                    ->AddTopic(TopicTableEntry{
                        .topic_id = 30,
                        .topic_name = "partitioned",
                        .config_version = 1,
                        .partition_maps = {active},
                        .schema_snapshot = {},
                    })
                    .ok());
    TopicPartitionMap prepared = active;
    prepared.map_version = 2;
    prepared.generation = 2;
    prepared.partition_count = 4;
    prepared.state = TopicPartitionMapState::kPrepared;
    ASSERT_TRUE((*manifest)->PrepareTopicPartitionMap(30, prepared).ok());

    TopicTableEntry illegal = *(*manifest)->FindTopic(30);
    illegal.partition_maps.front().partition_count = 2;
    EXPECT_EQ((*manifest)->UpdateTopic(illegal).code(),
              StatusCode::kInvalidArgument);
    ASSERT_TRUE((*manifest)->BeginTopicPartitionDrain(30, 2).ok());
    EXPECT_EQ((*manifest)
                  ->CutoverTopicPartitionMap(30, 2, PartitionDrainProof{})
                  .code(),
              StatusCode::kWouldBlock);
    const PartitionDrainProof proof{
        .old_routes_fenced = true,
        .queued_records = 0,
        .reserved_records = 0,
        .active_writers = 0,
        .last_admitted_sequence = 77,
        .last_persisted_sequence = 77,
    };
    ASSERT_TRUE((*manifest)->CutoverTopicPartitionMap(30, 2, proof).ok());
    const TopicTableEntry cutover = *(*manifest)->FindTopic(30);
    ASSERT_EQ(cutover.partition_maps.size(), 2u);
    EXPECT_EQ(cutover.partition_maps[0].state,
              TopicPartitionMapState::kRetired);
    EXPECT_EQ(cutover.partition_maps[1].state,
              TopicPartitionMapState::kActive);
    EXPECT_EQ(cutover.partition_maps[1].partition_count, 4u);
}

TEST(PartitionManifestTest, EnforcesStateMachineCheckpointAndOrphanApis) {
    const std::filesystem::path root = TestDirectory("partition");
    std::filesystem::create_directory(root / "segments");
    auto manifest = PartitionManifest::Create(root, PartitionSnapshot().partition);
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();

    SegmentManifestEntry segment =
        Segment(1, SegmentPersistentState::kCreating, 10, 10);
    segment.size_bytes = 52;
    ASSERT_TRUE((*manifest)->AddSegment(segment).ok());
    segment.state = SegmentPersistentState::kOpen;
    segment.last_ingestion_sequence = 12;
    segment.size_bytes = 300;
    ASSERT_TRUE((*manifest)->UpdateSegment(segment).ok());
    ASSERT_TRUE((*manifest)
                    ->UpdateCheckpoint(DurableCheckpoint{1, 200, 11})
                    .ok());
    EXPECT_FALSE((*manifest)
                     ->UpdateCheckpoint(DurableCheckpoint{1, 199, 10})
                     .ok());

    segment.state = SegmentPersistentState::kRetained;
    EXPECT_FALSE((*manifest)->UpdateSegment(segment).ok());
    segment.state = SegmentPersistentState::kSealed;
    segment.sealed_at_ns = 500;
    ASSERT_TRUE((*manifest)->UpdateSegment(segment).ok());
    segment.state = SegmentPersistentState::kIndexed;
    ASSERT_TRUE((*manifest)->UpdateSegment(segment).ok());
    segment.state = SegmentPersistentState::kRetained;
    ASSERT_TRUE((*manifest)->UpdateSegment(segment).ok());
    segment.state = SegmentPersistentState::kDeleted;
    ASSERT_TRUE((*manifest)->UpdateSegment(segment).ok());

    SegmentManifestEntry traversal =
        Segment(2, SegmentPersistentState::kSealed, 20, 25);
    traversal.relative_path = "segments/../escape.mino";
    EXPECT_FALSE((*manifest)->AddSegment(traversal).ok());

    SegmentManifestEntry orphan =
        Segment(2, SegmentPersistentState::kSealed, 13, 18);
    WriteByte(root / orphan.relative_path);
    ASSERT_TRUE((*manifest)->AdoptSealedOrphan(orphan).ok());

    const std::filesystem::path candidate = "segments/00000003.mino";
    WriteByte(root / candidate);
    auto quarantined = (*manifest)->QuarantineOrphan(candidate);
    ASSERT_TRUE(quarantined.ok()) << quarantined.status().ToString();
    EXPECT_TRUE(std::filesystem::is_regular_file(root / *quarantined));
    EXPECT_FALSE(std::filesystem::exists(root / candidate));
}

TEST(PartitionManifestTest, RecoveryRejectsTrackedSymlink) {
    const std::filesystem::path root = TestDirectory("symlink");
    std::filesystem::create_directory(root / "segments");
    WriteByte(root / "real.mino");
    ASSERT_EQ(::symlink((root / "real.mino").c_str(),
                        (root / "segments/00000001.mino").c_str()),
              0);
    auto manifest = PartitionManifest::Create(root, PartitionSnapshot().partition);
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
    SegmentManifestEntry entry =
        Segment(1, SegmentPersistentState::kCreating, 1, 1);
    entry.size_bytes = 52;
    ASSERT_TRUE((*manifest)->AddSegment(entry).ok());
    manifest->reset();
    EXPECT_EQ(PartitionManifest::Open(root).status().code(),
              StatusCode::kCorruption);
}

struct FaultState {
    std::optional<ManifestFaultPoint> fail_at;
    std::vector<ManifestFaultPoint> observed;
};

Status FaultHook(ManifestFaultPoint point, void* context) noexcept {
    auto* state = static_cast<FaultState*>(context);
    state->observed.push_back(point);
    if (state->fail_at == point) {
        return Status::Error(StatusCode::kUnavailable, "injected failure");
    }
    return Status::Ok();
}

TEST(RecordingManifestTest, AtomicCommitHooksPreserveOldOrNewSnapshot) {
    constexpr std::array<ManifestFaultPoint, 4> points = {
        ManifestFaultPoint::kAfterTempWrite,
        ManifestFaultPoint::kAfterTempDataSync,
        ManifestFaultPoint::kAfterRename,
        ManifestFaultPoint::kAfterParentDirectorySync,
    };
    for (ManifestFaultPoint point : points) {
        const std::filesystem::path root = TestDirectory("atomic");
        auto created = RecordingManifest::Create(
            root, RecordingSnapshot().session);
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        created->reset();

        FaultState state{.fail_at = point, .observed = {}};
        ManifestOptions options;
        options.fault_hook = FaultHook;
        options.fault_hook_context = &state;
        auto manifest = RecordingManifest::Open(root, options);
        ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
        EXPECT_FALSE((*manifest)->UpdateSessionConfigVersion(10).ok());
        EXPECT_TRUE((*manifest)->poisoned());
        manifest->reset();

        auto recovered = RecordingManifest::Open(root);
        ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
        const bool rename_completed =
            point == ManifestFaultPoint::kAfterRename ||
            point == ManifestFaultPoint::kAfterParentDirectorySync;
        EXPECT_EQ((*recovered)->snapshot().session.config_version,
                  rename_completed ? 10u : 9u);
        EXPECT_FALSE(state.observed.empty());
    }
}

TEST(PartitionManifestTest, OrphanHooksCoverRenameAndDirectorySync) {
    constexpr std::array<ManifestFaultPoint, 2> points = {
        ManifestFaultPoint::kAfterOrphanRename,
        ManifestFaultPoint::kAfterOrphanDirectorySync,
    };
    for (ManifestFaultPoint point : points) {
        const std::filesystem::path root = TestDirectory("orphan_atomic");
        std::filesystem::create_directory(root / "segments");
        auto created =
            PartitionManifest::Create(root, PartitionSnapshot().partition);
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        created->reset();
        const std::filesystem::path candidate = "segments/orphan.mino";
        WriteByte(root / candidate);

        FaultState state{.fail_at = point, .observed = {}};
        ManifestOptions options;
        options.fault_hook = FaultHook;
        options.fault_hook_context = &state;
        auto manifest = PartitionManifest::Open(root, options);
        ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
        EXPECT_FALSE((*manifest)->QuarantineOrphan(candidate).ok());
        EXPECT_TRUE((*manifest)->poisoned());
        EXPECT_TRUE(std::filesystem::exists(
            root / "segments/orphan.mino.orphan"));
    }
}

}  // namespace
}  // namespace mino::storage
