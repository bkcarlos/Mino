// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "tools/mino/runtime_services.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/storage/recorder.h"
#include "mino/storage/recorder_subscriber.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/segment_recovery.h"
#include "tools/mino/storage_commands.h"

namespace mino::tools {
namespace {

using namespace std::chrono_literals;

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_runtime_services_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path);
    return path;
}

struct TestArtifact {
    schema::SchemaIdentity identity{0, {}, 0, 0};
    std::vector<std::byte> bytes;
};

Result<TestArtifact> CompileArtifact() {
    Result<schema::CompiledSchema> compiled = schema::SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package runtime_cli; "
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
    return TestArtifact{
        .identity = compiled->types().front()->identity(),
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

std::filesystem::path WriteRuntimeConfig(
    const std::filesystem::path& root, std::string_view topic_name,
    const TestArtifact& artifact, uint64_t stop_after_records) {
    WriteBytes(root / "event.schema", artifact.bytes);
    const std::filesystem::path config_path = root / "runtime.toml";
    std::ofstream config(config_path, std::ios::trunc);
    EXPECT_TRUE(config.is_open());
    config << "[runtime]\n"
              "node_id = 4242\n"
              "lease_epoch = 7\n"
              "lease_duration_ms = 60000\n"
              "region_id = 17\n"
              "region_bytes = 8388608\n"
              "topic_id_state = \"topic_ids.state\"\n\n"
              "[record]\n"
           << "stop_after_records = " << stop_after_records << "\n"
              "max_runtime_ms = 5000\n"
              "poll_interval_ms = 1\n"
              "buffer_bytes = 1048576\n"
              "queue_capacity = 64\n"
              "max_segment_bytes = 1048576\n\n"
              "[[topics]]\n"
           << "name = \"" << topic_name << "\"\n"
              "schema_artifact = \"event.schema\"\n"
              "schema_type = \"runtime_cli.Event\"\n"
              "channel_capacity = 8\n"
              "max_subscribers = 4\n"
              "max_payload_bytes = 4096\n"
              "record_partitions = 1\n";
    EXPECT_TRUE(config.good());
    return config_path;
}

class RecorderCommandThreadGuard final {
public:
    RecorderCommandThreadGuard(BusRecorderServiceLauncher& launcher,
                               std::thread& thread) noexcept
        : launcher_(&launcher), thread_(&thread) {}
    ~RecorderCommandThreadGuard() {
        if (thread_->joinable()) {
            launcher_->RequestStop();
            thread_->join();
        }
    }

private:
    BusRecorderServiceLauncher* launcher_;
    std::thread* thread_;
};

storage::RecorderRecordMetadata RecordMetadata(
    TopicId topic_id, const schema::SchemaIdentity& identity,
    std::span<const std::byte> payload) {
    return storage::RecorderRecordMetadata{
        .schema = storage::RecorderSchemaMetadata{
            .short_id = identity.short_id(),
            .canonical_digest = identity.canonical_digest(),
            .schema_version = identity.schema_version(),
            .layout_version = identity.layout_version(),
        },
        .topic_id = topic_id,
        .source = storage::MessageSource{
            .node_id = 11,
            .publisher_id = 12,
            .publisher_epoch = 13,
            .source_sequence = 1,
            .observed_timestamp_ns = 100,
        },
        .ingestion_timestamp_ns = 200,
        .payload_size = static_cast<uint32_t>(payload.size()),
        .payload_crc = storage::RecorderPayloadCrc32c(payload),
    };
}

TEST(RuntimeServicesCliTest, RecordRunReceivesBusMessageAndWritesSegment) {
    Result<TestArtifact> artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestDirectory("record");
    const std::filesystem::path session = root / "session";

    std::ostringstream create_out;
    std::ostringstream create_err;
    ASSERT_EQ(RunStorageCommand(
                  {"record", "create", session.string(), "--recording-id", "101",
                   "--owner-id", "2", "--owner-epoch", "3",
                   "--config-version", "1", "--topic", "19:events"},
                  create_out, create_err),
              kStorageExitSuccess)
        << create_err.str();

    const std::filesystem::path config_path =
        WriteRuntimeConfig(root, "events", *artifact, 1);
    Result<std::unique_ptr<RuntimeCommandServices>> runtime =
        RuntimeCommandServices::Create(config_path);
    ASSERT_TRUE(runtime.ok()) << runtime.status().ToString();

    std::atomic<int> command_result{-1};
    std::ostringstream run_out;
    std::ostringstream run_err;
    std::thread command([&] {
        command_result.store(
            RunStorageCommand({"record", "run", session.string()}, run_out,
                              run_err, (*runtime)->services()),
            std::memory_order_release);
    });
    RecorderCommandThreadGuard command_guard(
        (*runtime)->recorder_launcher(), command);
    for (size_t attempt = 0;
         attempt < 5000 && !(*runtime)->recorder_launcher().running(); ++attempt) {
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE((*runtime)->recorder_launcher().running()) << run_err.str();

    Result<BusPublisher> publisher =
        (*runtime)->bus().CreatePublisher("events", artifact->identity);
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    const std::vector<std::byte> payload = {
        std::byte{0x08}, std::byte{0x96}, std::byte{0x01}};
    Result<BusPublishResult> published = publisher->Publish(payload);
    ASSERT_TRUE(published.ok()) << published.status().ToString();
    ASSERT_TRUE(published->bridge_status.ok())
        << published->bridge_status.ToString();

    command.join();
    ASSERT_EQ(command_result.load(std::memory_order_acquire), kStorageExitSuccess)
        << run_err.str();

    Result<std::unique_ptr<storage::PartitionManifest>> manifest =
        storage::PartitionManifest::Open(
            session / "topics/19/partitions/0000");
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
    ASSERT_FALSE((*manifest)->snapshot().segments.empty());
    const storage::SegmentManifestEntry& segment =
        (*manifest)->snapshot().segments.front();
    Result<storage::SegmentRecoveryReport> scanned = storage::ScanSegment(
        session / "topics/19/partitions/0000" / segment.relative_path);
    ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
    EXPECT_TRUE(scanned->clean());
    ASSERT_EQ(scanned->records.size(), 1u);
}

TEST(RuntimeServicesCliTest, ReplayPublishesCanonicalPayloadToBusSubscriber) {
    Result<TestArtifact> artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestDirectory("replay");
    const std::filesystem::path session = root / "session";
    const std::vector<std::byte> payload = {
        std::byte{0x08}, std::byte{0x2a}};

    Result<std::unique_ptr<storage::Recorder>> recorder = storage::Recorder::Create(
        session, storage::RecordingSessionMetadata{
                     .recording_id = 202,
                     .created_at_ns = 1,
                     .owner_id = 2,
                     .owner_epoch = 3,
                     .config_version = 1,
                 });
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    storage::RecorderTopicConfig topic;
    topic.topic_id = TopicId{10};
    topic.topic_name = "events";
    topic.config_version = 1;
    topic.schemas.push_back(storage::RecorderTopicSchema{
        .identity = artifact->identity,
        .descriptor_artifact = artifact->bytes,
    });
    ASSERT_TRUE((*recorder)->AddTopic(topic).ok());
    ASSERT_TRUE((*recorder)->Start(100).ok());
    Result<storage::RecorderEnqueueResult> enqueued = (*recorder)->Enqueue(
        0, RecordMetadata(TopicId{10}, artifact->identity, payload), payload);
    ASSERT_TRUE(enqueued.ok()) << enqueued.status().ToString();
    ASSERT_TRUE((*recorder)->Pump(201).ok());
    ASSERT_TRUE((*recorder)->Stop(202).ok());
    recorder->reset();

    const std::filesystem::path config_path =
        WriteRuntimeConfig(root, "replay/events", *artifact, 0);
    Result<std::unique_ptr<RuntimeCommandServices>> runtime =
        RuntimeCommandServices::Create(config_path);
    ASSERT_TRUE(runtime.ok()) << runtime.status().ToString();
    Result<BusSubscriber> subscriber = (*runtime)->bus().CreateSubscriber(
        "replay/events", artifact->identity);
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();

    std::ostringstream replay_out;
    std::ostringstream replay_err;
    ASSERT_EQ(RunStorageCommand({"replay", session.string(), "--step"},
                                replay_out, replay_err,
                                (*runtime)->services()),
              kStorageExitSuccess)
        << replay_err.str();
    Result<CanonicalMessage> received = subscriber->TryPoll();
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(received->payload, payload);
    EXPECT_TRUE(registry::SchemaIdentityEqual(received->schema,
                                             artifact->identity));
}

}  // namespace
}  // namespace mino::tools
