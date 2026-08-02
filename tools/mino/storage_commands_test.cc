// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "tools/mino/storage_commands.h"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/segment_format.h"

namespace mino::tools {
namespace {

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_storage_commands_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path);
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

void Append(std::vector<std::byte>* output,
            std::span<const std::byte> bytes) {
    output->insert(output->end(), bytes.begin(), bytes.end());
}

struct TestArtifact {
    schema::SchemaIdentity identity;
    std::vector<std::byte> bytes;
};

Result<TestArtifact> CompileArtifact() {
    Result<schema::CompiledSchema> compiled = schema::SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package cli; "
        "message Event { uint64 value = 1; }");
    if (!compiled.ok()) return compiled.status();
    std::vector<schema::LayoutPlan> layouts;
    for (const auto& descriptor : compiled->types()) {
        Result<schema::LayoutPlan> layout =
            schema::LayoutPlanner::Plan(*descriptor, {});
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    Result<std::string> encoded =
        schema::codegen::EncodeDescriptorArtifact(*compiled, layouts);
    if (!encoded.ok()) return encoded.status();
    const std::span<const std::byte> bytes = std::as_bytes(
        std::span<const char>(encoded->data(), encoded->size()));
    return TestArtifact{.identity = compiled->types()[0]->identity(),
                        .bytes = std::vector<std::byte>(bytes.begin(), bytes.end())};
}

storage::Record SampleRecord(const schema::SchemaIdentity& identity) {
    storage::Record record;
    record.header.schema_ref = 1;
    record.header.schema_version = identity.schema_version();
    record.header.layout_version = identity.layout_version();
    record.header.topic_id = 10;
    record.header.partition_id = 0;
    record.header.ingestion_sequence = 1;
    record.header.ingestion_timestamp_ns = 100;
    record.header.node_id = 7;
    record.header.publisher_id = 8;
    record.header.publisher_epoch = 1;
    record.header.source_sequence = 9;
    record.header.observed_timestamp_ns = 90;
    record.payload = {std::byte{1}, std::byte{2}, std::byte{3}};
    return record;
}

std::vector<std::byte> SegmentBytes(
    const schema::SchemaIdentity& identity) {
    const storage::SegmentHeader header{
        .flags = 0,
        .recording_id = 7,
        .topic_id = 10,
        .partition_id = 0,
        .writer_id = 3,
        .first_ingestion_sequence = 1,
        .created_at_ns = 50,
    };
    auto encoded_header = storage::EncodeSegmentHeader(header);
    EXPECT_TRUE(encoded_header.ok()) << encoded_header.status().ToString();
    auto encoded_record = storage::EncodeRecord(SampleRecord(identity));
    EXPECT_TRUE(encoded_record.ok()) << encoded_record.status().ToString();
    if (!encoded_header.ok() || !encoded_record.ok()) return {};
    std::vector<std::byte> bytes = *encoded_header;
    Append(&bytes, *encoded_record);
    return bytes;
}

std::string DigestHex(
    const std::array<std::byte, storage::kSchemaDigestSize>& digest) {
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result;
    for (std::byte byte : digest) {
        const uint8_t value = static_cast<uint8_t>(byte);
        result.push_back(digits[value >> 4]);
        result.push_back(digits[value & 0x0fu]);
    }
    return result;
}

struct SessionFixture {
    std::filesystem::path root;
    std::filesystem::path segment;
    std::filesystem::path descriptor;
    std::optional<schema::SchemaIdentity> identity;
};

SessionFixture CreateSession(std::string_view name) {
    SessionFixture fixture;
    Result<TestArtifact> artifact = CompileArtifact();
    EXPECT_TRUE(artifact.ok()) << artifact.status().ToString();
    if (!artifact.ok()) return fixture;
    fixture.identity = artifact->identity;
    fixture.root = TestDirectory(name);
    const std::filesystem::path partition =
        fixture.root / "topics/10/partitions/0000";
    std::filesystem::create_directories(partition / "segments");
    std::filesystem::create_directories(fixture.root / "schemas");
    fixture.segment = partition / "segments/00000001.mino";
    const std::vector<std::byte> bytes = SegmentBytes(artifact->identity);
    WriteBytes(fixture.segment, bytes);

    storage::SchemaRefSnapshot schema;
    schema.schema_ref = 1;
    schema.schema_version = artifact->identity.schema_version();
    schema.layout_version = artifact->identity.layout_version();
    schema.canonical_digest = artifact->identity.canonical_digest();
    schema.descriptor_path =
        std::filesystem::path("schemas") /
        (DigestHex(schema.canonical_digest) + ".schema");
    fixture.descriptor = fixture.root / schema.descriptor_path;
    WriteBytes(fixture.descriptor, artifact->bytes);

    auto recording = storage::RecordingManifest::Create(
        fixture.root,
        storage::RecordingSessionMetadata{
            .recording_id = 7,
            .created_at_ns = 1,
            .owner_id = 2,
            .owner_epoch = 1,
            .config_version = 1,
        });
    EXPECT_TRUE(recording.ok()) << recording.status().ToString();
    if (recording.ok()) {
        EXPECT_TRUE((*recording)
                        ->AddTopic(storage::TopicTableEntry{
                            .topic_id = 10,
                            .topic_name = "camera.front",
                            .config_version = 1,
                            .schema_snapshot = {schema},
                        })
                        .ok());
    }
    recording = Status::Error(StatusCode::kInternal);

    auto partition_manifest = storage::PartitionManifest::Create(
        partition,
        storage::PartitionMetadata{
            .recording_id = 7,
            .topic_id = 10,
            .partition_id = 0,
            .writer_id = 3,
            .owner_epoch = 1,
            .config_version = 1,
        });
    EXPECT_TRUE(partition_manifest.ok())
        << partition_manifest.status().ToString();
    if (partition_manifest.ok()) {
        EXPECT_TRUE((*partition_manifest)
                        ->AddSegment(storage::SegmentManifestEntry{
                            .segment_id = 1,
                            .state = storage::SegmentPersistentState::kSealed,
                            .first_ingestion_sequence = 1,
                            .last_ingestion_sequence = 1,
                            .created_at_ns = 50,
                            .sealed_at_ns = 60,
                            .size_bytes = bytes.size(),
                            .relative_path = "segments/00000001.mino",
                        })
                        .ok());
    }
    return fixture;
}

class CapturingAdapter final : public storage::ReplayPublisherAdapter {
public:
    Status Publish(const storage::ReplayPublishRequest& request) noexcept override {
        ++published;
        topic_name = request.topic_name;
        return Status::Ok();
    }

    size_t published = 0;
    std::string topic_name;
};

class CapturingRecorderLauncher final : public RecorderServiceLauncher {
public:
    Status Run(const std::filesystem::path& session_root) noexcept override {
        ++calls;
        root = session_root;
        return result;
    }

    size_t calls = 0;
    std::filesystem::path root;
    Status result = Status::Ok();
};

TEST(StorageCommandsTest, ParserHasStableUsageExitCode) {
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand({}, out, err), kStorageExitUsage);
    EXPECT_EQ(RunStorageCommand({"unknown"}, out, err), kStorageExitUsage);
    EXPECT_EQ(RunStorageCommand({"repair", "a", "b"}, out, err),
              kStorageExitUsage);
}

TEST(StorageCommandsTest, RecordCreatesConfigAndInspectShowsTopicNameAndId) {
    const std::filesystem::path root = TestDirectory("record");
    std::filesystem::remove_all(root);
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand(
                  {"record", "create", root.string(), "--recording-id", "11",
                   "--owner-id", "12", "--owner-epoch", "1",
                   "--config-version", "3", "--created-at-ns", "99",
                   "--topic", "10:camera.front", "--partitions", "2"},
                  out, err),
              kStorageExitSuccess)
        << err.str();

    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand({"inspect", root.string()}, out, err),
              kStorageExitSuccess)
        << err.str();
    EXPECT_NE(out.str().find("topic: camera.front (id=10)"), std::string::npos);
    EXPECT_NE(out.str().find("partitions: 2"), std::string::npos);

    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand({"record", "create", root.string(),
                                 "--validate-only", "--recording-id", "11",
                                 "--topic", "10:camera.front"},
                                out, err),
              kStorageExitSuccess)
        << err.str();
}

TEST(StorageCommandsTest, VerifyChecksManifestSchemaAndCorruption) {
    const SessionFixture fixture = CreateSession("verify");
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand({"verify", fixture.segment.string()}, out, err),
              kStorageExitSuccess)
        << err.str();
    EXPECT_NE(out.str().find("clean"), std::string::npos);

    std::filesystem::remove(fixture.descriptor);
    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand({"verify", fixture.segment.string()}, out, err),
              kStorageExitFailure);

    Result<TestArtifact> artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    std::vector<std::byte> damaged = SegmentBytes(artifact->identity);
    ASSERT_GT(damaged.size(), storage::kEncodedSegmentHeaderSize + 112);
    damaged[storage::kEncodedSegmentHeaderSize + 112] ^= std::byte{1};
    const std::filesystem::path standalone =
        TestDirectory("corrupt") / "corrupt.mino";
    WriteBytes(standalone, damaged);
    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand({"verify", standalone.string()}, out, err),
              kStorageExitInvalidData);
}

TEST(StorageCommandsTest, VerifyRejectsMalformedDescriptorArtifact) {
    const SessionFixture fixture = CreateSession("bad_descriptor");
    const std::array<std::byte, 8> invalid = {
        std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
        std::byte{'D'}, std::byte{'S'}, std::byte{'C'}, std::byte{'2'},
    };
    WriteBytes(fixture.descriptor, invalid);
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand({"verify", fixture.segment.string()}, out, err),
              kStorageExitInvalidData);
    EXPECT_NE(err.str().find("descriptor artifact"), std::string::npos);
}

TEST(StorageCommandsTest, RepairDefaultsDryRunAndRequiresApplyToModify) {
    Result<TestArtifact> artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    std::vector<std::byte> incomplete = SegmentBytes(artifact->identity);
    ASSERT_GT(incomplete.size(), storage::kEncodedSegmentHeaderSize + 1);
    incomplete.resize(incomplete.size() - 1);
    const std::filesystem::path path =
        TestDirectory("repair") / "incomplete.mino";
    WriteBytes(path, incomplete);
    const uint64_t before = std::filesystem::file_size(path);

    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand({"repair", path.string()}, out, err),
              kStorageExitInvalidData)
        << err.str();
    EXPECT_EQ(std::filesystem::file_size(path), before);
    EXPECT_NE(out.str().find("mode=dry-run"), std::string::npos);
    EXPECT_NE(out.str().find("modified=false"), std::string::npos);

    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand({"repair", path.string(), "--apply"}, out, err),
              kStorageExitPermissionDenied);
    EXPECT_EQ(std::filesystem::file_size(path), before);

    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand({"repair", path.string(), "--apply",
                                 "--standalone"},
                                out, err),
              kStorageExitSuccess)
        << err.str();
    EXPECT_EQ(std::filesystem::file_size(path),
              storage::kEncodedSegmentHeaderSize);
    EXPECT_NE(out.str().find("repaired=true"), std::string::npos);
    EXPECT_NE(out.str().find("mode=apply-standalone"), std::string::npos);
    EXPECT_NE(out.str().find("modified=true"), std::string::npos);
}

TEST(StorageCommandsTest, RepairApplyRequiresExclusiveOwnershipLock) {
    Result<TestArtifact> artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    std::vector<std::byte> incomplete = SegmentBytes(artifact->identity);
    incomplete.resize(incomplete.size() - 1);
    const std::filesystem::path path =
        TestDirectory("repair_lock") / "incomplete.mino";
    WriteBytes(path, incomplete);
    const uint64_t before = std::filesystem::file_size(path);
    const std::filesystem::path lock_path =
        std::filesystem::path(path.string() + ".repair.owner.lock");
    const int lock_fd = ::open(lock_path.c_str(), O_RDWR | O_CREAT, 0600);
    ASSERT_GE(lock_fd, 0);
    ASSERT_EQ(::flock(lock_fd, LOCK_EX | LOCK_NB), 0);

    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand(
                  {"repair", path.string(), "--apply", "--standalone"},
                  out, err),
              kStorageExitFailure);
    EXPECT_EQ(std::filesystem::file_size(path), before);
    static_cast<void>(::close(lock_fd));
}

TEST(StorageCommandsTest, RepairApplyRejectsHardLinkedStandaloneSegment) {
    Result<TestArtifact> artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    std::vector<std::byte> incomplete = SegmentBytes(artifact->identity);
    incomplete.resize(incomplete.size() - 1);
    const std::filesystem::path directory = TestDirectory("repair_hard_link");
    const std::filesystem::path path = directory / "incomplete.mino";
    const std::filesystem::path alias = directory / "alias.mino";
    WriteBytes(path, incomplete);
    std::error_code link_error;
    std::filesystem::create_hard_link(path, alias, link_error);
    ASSERT_FALSE(link_error) << link_error.message();
    const uint64_t before = std::filesystem::file_size(path);

    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand(
                  {"repair", path.string(), "--apply", "--standalone"},
                  out, err),
              kStorageExitPermissionDenied);
    EXPECT_EQ(std::filesystem::file_size(path), before);
    EXPECT_EQ(std::filesystem::file_size(alias), before);
    EXPECT_NE(err.str().find("hard-linked"), std::string::npos);
}

TEST(StorageCommandsTest, RepairApplyRejectsSessionTrackedSegment) {
    const SessionFixture fixture = CreateSession("tracked_repair");
    ASSERT_TRUE(fixture.identity.has_value());
    std::vector<std::byte> incomplete = SegmentBytes(*fixture.identity);
    incomplete.resize(incomplete.size() - 1);
    WriteBytes(fixture.segment, incomplete);
    const uint64_t before = std::filesystem::file_size(fixture.segment);
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand({"repair", fixture.segment.string(), "--apply",
                                 "--standalone"},
                                out, err),
              kStorageExitPermissionDenied);
    EXPECT_EQ(std::filesystem::file_size(fixture.segment), before);
    EXPECT_NE(err.str().find("SessionRecoveryCoordinator"), std::string::npos);
}

TEST(StorageCommandsTest, ReplayUsesAdapterFiltersAndRequiresLiveAuthorization) {
    const SessionFixture fixture = CreateSession("replay");
    CapturingAdapter adapter;
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand(
                  {"replay", fixture.root.string(), "--step", "--topic",
                   "camera.front", "--segment", fixture.segment.string()},
                  out, err,
                  StorageCommandServices{.replay_adapter = &adapter}),
              kStorageExitSuccess)
        << err.str();
    EXPECT_EQ(adapter.published, 1u);
    EXPECT_EQ(adapter.topic_name, "camera.front");

    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand({"replay", fixture.root.string(), "--live",
                                 "--validate-only"},
                                out, err, &adapter),
              kStorageExitPermissionDenied);

    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand({"replay", fixture.root.string(), "--live",
                                 "--authorize-live", "--validate-only"},
                                out, err, &adapter),
              kStorageExitSuccess)
        << err.str();
}

TEST(StorageCommandsTest, ReplayStandaloneSegmentRequiresUnsafeAndForbidsLive) {
    const SessionFixture fixture = CreateSession("standalone_replay");
    const std::filesystem::path standalone =
        TestDirectory("standalone_segment") / "outside.mino";
    ASSERT_TRUE(fixture.identity.has_value());
    WriteBytes(standalone, SegmentBytes(*fixture.identity));
    std::ostringstream out;
    std::ostringstream err;

    EXPECT_EQ(RunStorageCommand({"replay", fixture.root.string(), "--segment",
                                 standalone.string(), "--validate-only"},
                                out, err),
              kStorageExitPermissionDenied);

    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand(
                  {"replay", fixture.root.string(), "--segment",
                   standalone.string(), "--unsafe-standalone-segment",
                   "--validate-only"},
                  out, err),
              kStorageExitSuccess)
        << err.str();

    CapturingAdapter adapter;
    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand(
                  {"replay", fixture.root.string(), "--segment",
                   standalone.string(), "--unsafe-standalone-segment", "--live",
                   "--authorize-live"},
                  out, err,
                  StorageCommandServices{.replay_adapter = &adapter}),
              kStorageExitPermissionDenied);
    EXPECT_EQ(adapter.published, 0u);
}

TEST(StorageCommandsTest, RecordRunRequiresAndUsesInstalledServiceLauncher) {
    const std::filesystem::path root = TestDirectory("record_run");
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand({"record", "run", root.string()}, out, err),
              kStorageExitFailure);
    EXPECT_NE(err.str().find("no RecorderService launcher is installed"),
              std::string::npos);

    CapturingRecorderLauncher launcher;
    out.str("");
    err.str("");
    EXPECT_EQ(RunStorageCommand(
                  {"record", "run", root.string()}, out, err,
                  StorageCommandServices{
                      .recorder_service_launcher = &launcher}),
              kStorageExitSuccess)
        << err.str();
    EXPECT_EQ(launcher.calls, 1u);
    EXPECT_EQ(launcher.root, root);
    EXPECT_NE(out.str().find("recording service exited"), std::string::npos);
}

TEST(StorageCommandsTest, VerifyRejectsSymlinkedSchema) {
    const SessionFixture fixture = CreateSession("symlink");
    std::filesystem::remove(fixture.descriptor);
    const std::filesystem::path target = fixture.root / "descriptor-target";
    const std::array<std::byte, 1> descriptor = {std::byte{1}};
    WriteBytes(target, descriptor);
    ASSERT_EQ(::symlink(target.c_str(), fixture.descriptor.c_str()), 0);

    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(RunStorageCommand({"verify", fixture.segment.string()}, out, err),
              kStorageExitPermissionDenied);
}

}  // namespace
}  // namespace mino::tools
