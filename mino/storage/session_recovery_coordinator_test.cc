// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/session_recovery_coordinator.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <cerrno>
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

#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/schema/registry.h"
#include "mino/storage/segment_format.h"

namespace mino::storage {
namespace {

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_session_recovery_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path);
    return path;
}

struct TestArtifact {
    schema::SchemaIdentity identity;
    std::vector<std::byte> bytes;
};

Result<TestArtifact> CompileArtifact() {
    auto compiled = schema::SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package recovery; "
        "message Event { uint64 value = 1; }");
    if (!compiled.ok()) return compiled.status();
    std::vector<schema::LayoutPlan> layouts;
    for (const auto& descriptor : compiled->types()) {
        auto layout = schema::LayoutPlanner::Plan(*descriptor, {});
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    auto encoded = schema::codegen::EncodeDescriptorArtifact(*compiled, layouts);
    if (!encoded.ok()) return encoded.status();
    const auto bytes = std::as_bytes(
        std::span<const char>(encoded->data(), encoded->size()));
    return TestArtifact{
        .identity = compiled->types()[0]->identity(),
        .bytes = std::vector<std::byte>(bytes.begin(), bytes.end()),
    };
}

void WriteBytes(const std::filesystem::path& path,
                std::span<const std::byte> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(output.good());
}

std::vector<std::byte> SegmentBytes(uint64_t segment_first,
                                    uint64_t record_sequence,
                                    uint64_t created_at_ns) {
    SegmentHeader header{
        .recording_id = 50,
        .topic_id = 10,
        .partition_id = 0,
        .writer_id = 60,
        .first_ingestion_sequence = segment_first,
        .created_at_ns = created_at_ns,
    };
    Record record;
    record.header.flags = kRecordFlagGap;
    record.header.topic_id = 10;
    record.header.partition_id = 0;
    record.header.ingestion_sequence = record_sequence;
    record.header.ingestion_timestamp_ns = created_at_ns + 1;
    record.header.node_id = 1;
    record.header.publisher_id = 2;
    record.header.publisher_epoch = 1;
    record.header.source_sequence = record_sequence;
    record.header.observed_timestamp_ns = created_at_ns;
    record.payload = {std::byte{1}};
    auto encoded_header = EncodeSegmentHeader(header);
    auto encoded_record = EncodeRecord(record);
    EXPECT_TRUE(encoded_header.ok()) << encoded_header.status().ToString();
    EXPECT_TRUE(encoded_record.ok()) << encoded_record.status().ToString();
    if (!encoded_header.ok() || !encoded_record.ok()) return {};
    std::vector<std::byte> result = *encoded_header;
    result.insert(result.end(), encoded_record->begin(), encoded_record->end());
    return result;
}

std::vector<std::byte> OrdinarySegmentBytes(
    uint64_t sequence, uint64_t created_at_ns,
    const SchemaRefSnapshot& schema, uint32_t schema_version) {
    SegmentHeader header{
        .recording_id = 50,
        .topic_id = 10,
        .partition_id = 0,
        .writer_id = 60,
        .first_ingestion_sequence = sequence,
        .created_at_ns = created_at_ns,
    };
    Record record;
    record.header.schema_ref = schema.schema_ref;
    record.header.schema_version = schema_version;
    record.header.layout_version = schema.layout_version;
    record.header.topic_id = 10;
    record.header.partition_id = 0;
    record.header.ingestion_sequence = sequence;
    record.header.ingestion_timestamp_ns = created_at_ns + 1;
    record.header.node_id = 1;
    record.header.publisher_id = 2;
    record.header.publisher_epoch = 1;
    record.header.source_sequence = sequence;
    record.header.observed_timestamp_ns = created_at_ns;
    record.payload = {std::byte{9}};
    auto encoded_header = EncodeSegmentHeader(header);
    auto encoded_record = EncodeRecord(record);
    EXPECT_TRUE(encoded_header.ok());
    EXPECT_TRUE(encoded_record.ok());
    if (!encoded_header.ok() || !encoded_record.ok()) return {};
    std::vector<std::byte> result = *encoded_header;
    result.insert(result.end(), encoded_record->begin(), encoded_record->end());
    return result;
}

void CreateActiveFixture(const std::filesystem::path& root,
                         std::string_view partition_directory,
                         std::span<const std::byte> segment_bytes,
                         SegmentPersistentState state) {
    const std::filesystem::path partition_root =
        root / "topics/10/partitions" / std::string(partition_directory);
    std::filesystem::create_directories(partition_root / "segments");
    auto recording = RecordingManifest::Create(
        root, RecordingSessionMetadata{.recording_id = 50,
                                       .created_at_ns = 1,
                                       .owner_id = 2,
                                       .owner_epoch = 3,
                                       .config_version = 1});
    ASSERT_TRUE(recording.ok()) << recording.status().ToString();
    ASSERT_TRUE((*recording)
                    ->AddTopic(TopicTableEntry{.topic_id = 10,
                                               .topic_name = "events",
                                               .config_version = 1,
                                               .schema_snapshot = {}})
                    .ok());
    recording->reset();
    WriteBytes(partition_root / "segments/00000001.mino", segment_bytes);
    auto partition = PartitionManifest::Create(
        partition_root,
        PartitionMetadata{.recording_id = 50,
                          .topic_id = 10,
                          .partition_id = 0,
                          .writer_id = 60,
                          .owner_epoch = 3,
                          .config_version = 1});
    ASSERT_TRUE(partition.ok()) << partition.status().ToString();
    ASSERT_TRUE((*partition)
                    ->AddSegment(SegmentManifestEntry{
                        .segment_id = 1,
                        .state = state,
                        .first_ingestion_sequence = 1,
                        .last_ingestion_sequence = 1,
                        .created_at_ns = 100,
                        .sealed_at_ns = 0,
                        .size_bytes = segment_bytes.size(),
                        .relative_path = "segments/00000001.mino",
                    })
                    .ok());
}

int FailSync(int, void*) noexcept {
    errno = EIO;
    return -1;
}

TEST(SessionRecoveryCoordinatorTest,
     RepairsActiveAdoptsSealedOrphanAndQuarantinesInvalidCandidate) {
    const std::filesystem::path root = TestDirectory("full");
    const std::filesystem::path partition_root =
        root / "topics/10/partitions/0000";
    std::filesystem::create_directories(partition_root / "segments");

    auto artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    schema::SchemaRegistry registry;
    auto schema_store = SchemaStore::Open(root, &registry);
    ASSERT_TRUE(schema_store.ok()) << schema_store.status().ToString();
    auto schema_ref = (*schema_store)->Persist(artifact->identity, artifact->bytes);
    ASSERT_TRUE(schema_ref.ok()) << schema_ref.status().ToString();
    auto stored_schema = (*schema_store)->Resolve(*schema_ref);
    ASSERT_TRUE(stored_schema.ok()) << stored_schema.status().ToString();
    SchemaRefSnapshot schema_snapshot{
        .schema_ref = *schema_ref,
        .schema_version = artifact->identity.schema_version(),
        .layout_version = artifact->identity.layout_version(),
        .canonical_digest = artifact->identity.canonical_digest(),
        .descriptor_path = std::filesystem::relative(
            stored_schema->descriptor_path, root),
    };
    schema_store->reset();

    auto recording = RecordingManifest::Create(
        root, RecordingSessionMetadata{.recording_id = 50,
                                       .created_at_ns = 1,
                                       .owner_id = 2,
                                       .owner_epoch = 3,
                                       .config_version = 1});
    ASSERT_TRUE(recording.ok()) << recording.status().ToString();
    ASSERT_TRUE((*recording)
                    ->AddTopic(TopicTableEntry{.topic_id = 10,
                                               .topic_name = "events",
                                               .config_version = 1,
                                               .schema_snapshot = {schema_snapshot}})
                    .ok());
    recording->reset();

    const std::vector<std::byte> active_complete = SegmentBytes(1, 1, 100);
    std::vector<std::byte> active_torn = active_complete;
    active_torn.push_back(std::byte{0x80});
    WriteBytes(partition_root / "segments/00000001.mino", active_torn);
    auto partition = PartitionManifest::Create(
        partition_root,
        PartitionMetadata{.recording_id = 50,
                          .topic_id = 10,
                          .partition_id = 0,
                          .writer_id = 60,
                          .owner_epoch = 3,
                          .config_version = 1});
    ASSERT_TRUE(partition.ok()) << partition.status().ToString();
    ASSERT_TRUE((*partition)
                    ->AddSegment(SegmentManifestEntry{
                        .segment_id = 1,
                        .state = SegmentPersistentState::kOpen,
                        .first_ingestion_sequence = 1,
                        .last_ingestion_sequence = 1,
                        .created_at_ns = 100,
                        .sealed_at_ns = 0,
                        .size_bytes = active_complete.size(),
                        .relative_path = "segments/00000001.mino",
                    })
                    .ok());
    partition->reset();

    const std::vector<std::byte> orphan = SegmentBytes(2, 2, 200);
    std::vector<std::byte> repairable_orphan = orphan;
    repairable_orphan.push_back(std::byte{0x80});
    WriteBytes(partition_root / "segments/00000002.mino", repairable_orphan);
    const std::vector<std::byte> invalid = OrdinarySegmentBytes(
        3, 300, schema_snapshot, schema_snapshot.schema_version + 1);
    WriteBytes(partition_root / "segments/00000003.mino", invalid);

    auto coordinator = SessionRecoveryCoordinator::Open(
        root, SessionRecoveryOptions{.recovery_timestamp_ns = 500});
    ASSERT_TRUE(coordinator.ok()) << coordinator.status().ToString();
    auto report = (*coordinator)->Recover();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    ASSERT_EQ(report->partitions_recovered, 1u);
    EXPECT_EQ(report->schema_refs_validated, 1u);
    EXPECT_EQ(report->segments_scanned, 3u);
    EXPECT_EQ(report->repaired_segments, 2u);
    EXPECT_EQ(report->adopted_sealed_orphans, 1u);
    EXPECT_EQ(report->quarantined_orphans, 1u);
    ASSERT_EQ(report->durable_boundaries.size(), 1u);
    EXPECT_EQ(report->durable_boundaries[0].durable_segment_id, 2u);
    EXPECT_EQ(report->durable_boundaries[0].durable_sequence, 2u);
    EXPECT_EQ(report->durable_boundaries[0].durable_offset, orphan.size());
    coordinator->reset();

    EXPECT_EQ(std::filesystem::file_size(
                  partition_root / "segments/00000001.mino"),
              active_complete.size());
    EXPECT_TRUE(std::filesystem::is_regular_file(
        partition_root / "segments/00000003.mino.orphan"));
    EXPECT_FALSE(std::filesystem::exists(
        partition_root / "segments/00000003.mino"));

    auto reopened = SessionRecoveryCoordinator::Open(root);
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    auto idempotent = (*reopened)->Recover();
    ASSERT_TRUE(idempotent.ok()) << idempotent.status().ToString();
    EXPECT_EQ(idempotent->schema_refs_validated, 1u);
    EXPECT_EQ(idempotent->repaired_segments, 0u);
    EXPECT_EQ(idempotent->adopted_sealed_orphans, 0u);
    EXPECT_EQ(idempotent->durable_boundaries[0].durable_segment_id, 2u);
    reopened->reset();

    auto recovered_manifest = PartitionManifest::Open(partition_root);
    ASSERT_TRUE(recovered_manifest.ok())
        << recovered_manifest.status().ToString();
    const PartitionManifestSnapshot& snapshot =
        (*recovered_manifest)->snapshot();
    ASSERT_EQ(snapshot.segments.size(), 2u);
    EXPECT_EQ(snapshot.segments[0].state, SegmentPersistentState::kSealed);
    EXPECT_EQ(snapshot.segments[0].size_bytes, active_complete.size());
    EXPECT_EQ(snapshot.segments[1].segment_id, 2u);
    EXPECT_EQ(snapshot.segments[1].state, SegmentPersistentState::kSealed);
    ASSERT_TRUE(snapshot.checkpoint.has_value());
    EXPECT_EQ(snapshot.checkpoint->segment_id, 2u);
    EXPECT_EQ(snapshot.checkpoint->durable_sequence, 2u);
}

TEST(SessionRecoveryCoordinatorTest,
     NonContiguousRepairableOrphanIsQuarantinedWithoutModification) {
    const std::filesystem::path root = TestDirectory("orphan_gap");
    const std::vector<std::byte> active = SegmentBytes(1, 1, 100);
    CreateActiveFixture(root, "0000", active, SegmentPersistentState::kOpen);
    const std::filesystem::path partition_root =
        root / "topics/10/partitions/0000";
    std::vector<std::byte> orphan = SegmentBytes(3, 3, 300);
    orphan.push_back(std::byte{0x80});
    const std::filesystem::path orphan_path =
        partition_root / "segments/00000002.mino";
    WriteBytes(orphan_path, orphan);

    auto coordinator = SessionRecoveryCoordinator::Open(root);
    ASSERT_TRUE(coordinator.ok()) << coordinator.status().ToString();
    auto recovered = (*coordinator)->Recover();
    ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
    EXPECT_EQ(recovered->repaired_segments, 0u);
    EXPECT_EQ(recovered->adopted_sealed_orphans, 0u);
    EXPECT_EQ(recovered->quarantined_orphans, 1u);
    EXPECT_FALSE(std::filesystem::exists(orphan_path));
    const std::filesystem::path quarantined =
        partition_root / "segments/00000002.mino.orphan";
    ASSERT_TRUE(std::filesystem::is_regular_file(quarantined));
    EXPECT_EQ(std::filesystem::file_size(quarantined), orphan.size());
}

TEST(SessionRecoveryCoordinatorTest, HeaderOnlyActiveGetsDurableRecoveryGap) {
    const std::filesystem::path root = TestDirectory("header_only");
    const SegmentHeader header{
        .recording_id = 50,
        .topic_id = 10,
        .partition_id = 0,
        .writer_id = 60,
        .first_ingestion_sequence = 1,
        .created_at_ns = 100,
    };
    auto encoded_header = EncodeSegmentHeader(header);
    ASSERT_TRUE(encoded_header.ok()) << encoded_header.status().ToString();
    CreateActiveFixture(root, "0000", *encoded_header,
                        SegmentPersistentState::kCreating);

    auto coordinator = SessionRecoveryCoordinator::Open(
        root, SessionRecoveryOptions{.recovery_timestamp_ns = 500});
    ASSERT_TRUE(coordinator.ok()) << coordinator.status().ToString();
    auto report = (*coordinator)->Recover();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    ASSERT_EQ(report->durable_boundaries.size(), 1u);
    EXPECT_EQ(report->durable_boundaries[0].durable_sequence, 1u);
    coordinator->reset();

    const std::filesystem::path partition_root =
        root / "topics/10/partitions/0000";
    auto scanned = ScanSegment(partition_root / "segments/00000001.mino");
    ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
    EXPECT_TRUE(scanned->clean());
    ASSERT_EQ(scanned->records.size(), 1u);
    EXPECT_NE(scanned->records[0].flags & kRecordFlagGap, 0u);
    auto manifest = PartitionManifest::Open(partition_root);
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
    EXPECT_EQ((*manifest)->snapshot().segments[0].state,
              SegmentPersistentState::kSealed);
    ASSERT_TRUE((*manifest)->snapshot().checkpoint.has_value());
}

TEST(SessionRecoveryCoordinatorTest,
     SyncFailuresDoNotPublishSealedOrCheckpointBoundary) {
    for (int failure = 0; failure < 2; ++failure) {
        const std::filesystem::path root =
            TestDirectory(failure == 0 ? "segment_sync_fail" : "dir_sync_fail");
        const std::vector<std::byte> bytes = SegmentBytes(1, 1, 100);
        CreateActiveFixture(root, "0000", bytes, SegmentPersistentState::kOpen);
        SessionRecoveryOptions options;
        if (failure == 0) {
            options.segment_repair_options.sync_hook = FailSync;
        } else {
            options.directory_sync_hook = FailSync;
        }
        auto coordinator = SessionRecoveryCoordinator::Open(root, options);
        ASSERT_TRUE(coordinator.ok()) << coordinator.status().ToString();
        auto recovered = (*coordinator)->Recover();
        ASSERT_FALSE(recovered.ok());
        coordinator->reset();

        auto manifest = PartitionManifest::Open(
            root / "topics/10/partitions/0000");
        ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
        EXPECT_EQ((*manifest)->snapshot().segments[0].state,
                  SegmentPersistentState::kOpen);
        EXPECT_FALSE((*manifest)->snapshot().checkpoint.has_value());
    }
}

TEST(SessionRecoveryCoordinatorTest, RejectsPartitionPathIdentityMismatch) {
    const std::filesystem::path root = TestDirectory("partition_path_mismatch");
    const std::vector<std::byte> bytes = SegmentBytes(1, 1, 100);
    CreateActiveFixture(root, "0001", bytes, SegmentPersistentState::kOpen);
    auto coordinator = SessionRecoveryCoordinator::Open(root);
    ASSERT_TRUE(coordinator.ok()) << coordinator.status().ToString();
    auto recovered = (*coordinator)->Recover();
    ASSERT_FALSE(recovered.ok());
    EXPECT_EQ(recovered.status().code(), StatusCode::kCorruption);
}

TEST(SessionRecoveryCoordinatorTest, RejectsAncestorSymlink) {
    const std::filesystem::path base = TestDirectory("ancestor_symlink");
    const std::filesystem::path real_parent = base / "real";
    const std::filesystem::path root = real_parent / "session";
    std::filesystem::create_directories(real_parent);
    const std::vector<std::byte> bytes = SegmentBytes(1, 1, 100);
    CreateActiveFixture(root, "0000", bytes, SegmentPersistentState::kOpen);
    const std::filesystem::path linked_parent = base / "linked";
    ASSERT_EQ(::symlink(real_parent.c_str(), linked_parent.c_str()), 0);

    auto coordinator = SessionRecoveryCoordinator::Open(linked_parent / "session");
    ASSERT_FALSE(coordinator.ok());
    EXPECT_EQ(coordinator.status().code(), StatusCode::kPermissionDenied);
}

}  // namespace
}  // namespace mino::storage
