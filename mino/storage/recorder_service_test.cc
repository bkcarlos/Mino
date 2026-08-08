// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder_service.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/storage/segment_recovery.h"

namespace mino {

struct RecorderServiceTestMessage {
    uint64_t value = 0;
};

template <>
struct StaticMessageTraits<RecorderServiceTestMessage> {
    static constexpr bool kIsSpecialized = true;
    static constexpr TypeId type_id{91};
    static constexpr uint32_t message_type = 91;
    static constexpr uint32_t schema_version = 1;
    static constexpr uint64_t schema_short_id = 1;
    static constexpr uint32_t layout_version = 1;
    static constexpr uint32_t index_flags = 0;
    static Status Validate(const RecorderServiceTestMessage&) noexcept {
        return Status::Ok();
    }
};

}  // namespace mino

namespace mino::storage {
namespace {

using namespace std::chrono_literals;
using TestMessage = RecorderServiceTestMessage;

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
        base / ("mino_recorder_service_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    return path;
}

Result<TestArtifact> CompileArtifact() {
    auto compiled = schema::SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package service; "
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

class AtomicClock final : public RecorderClock {
public:
    uint64_t NowNs() noexcept override {
        return now_.fetch_add(1, std::memory_order_relaxed);
    }

private:
    std::atomic<uint64_t> now_{1000};
};

class OneBorrow final : public RecorderBorrow<TestMessage> {
public:
    OneBorrow(TestMessage value, MessageMetadata metadata,
              std::atomic<bool>* acked) noexcept
        : value_(value), metadata_(metadata), acked_(acked) {}

    const TestMessage& value() const noexcept override { return value_; }
    const MessageMetadata& metadata() const noexcept override { return metadata_; }
    bool active() const noexcept override { return active_; }
    Status Ack() noexcept override {
        if (!active_) return Status::Error(StatusCode::kInvalidArgument);
        active_ = false;
        acked_->store(true, std::memory_order_release);
        return Status::Ok();
    }

private:
    TestMessage value_;
    MessageMetadata metadata_;
    std::atomic<bool>* acked_ = nullptr;
    bool active_ = true;
};

class OneBorrowSource final : public RecorderBorrowSource<TestMessage> {
public:
    explicit OneBorrowSource(MessageMetadata metadata) noexcept
        : metadata_(metadata) {}

    Result<std::unique_ptr<RecorderBorrow<TestMessage>>> TryBorrow()
        noexcept override {
        if (emitted_.exchange(true, std::memory_order_acq_rel)) {
            return Status::Error(StatusCode::kWouldBlock);
        }
        try {
            return std::unique_ptr<RecorderBorrow<TestMessage>>(
                std::make_unique<OneBorrow>(TestMessage{42}, metadata_, &acked_));
        } catch (const std::bad_alloc&) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
    }

    bool acked() const noexcept {
        return acked_.load(std::memory_order_acquire);
    }

private:
    MessageMetadata metadata_;
    std::atomic<bool> emitted_{false};
    std::atomic<bool> acked_{false};
};

class Encoder final : public RecorderCanonicalEncoder<TestMessage> {
public:
    Status Validate(const TestMessage&, const MessageMetadata&) noexcept override {
        return Status::Ok();
    }
    Result<std::vector<std::byte>> Encode(
        const TestMessage&, const MessageMetadata&) noexcept override {
        return std::vector<std::byte>{std::byte{0x08}, std::byte{0x2a}};
    }
};

class BlockingServiceSource final : public RecorderServiceSource {
public:
    BlockingServiceSource(RecorderEnqueueSink* sink,
                          RecorderRecordMetadata metadata,
                          std::vector<std::byte> payload) noexcept
        : sink_(sink), metadata_(metadata), payload_(std::move(payload)) {}

    Result<RecorderRecordResult> PollOne() noexcept override {
        if (emitted_.exchange(true, std::memory_order_acq_rel)) {
            return Status::Error(StatusCode::kWouldBlock);
        }
        {
            std::unique_lock lock(mutex_);
            entered_ = true;
            condition_.notify_all();
            condition_.wait(lock, [this] { return released_; });
        }
        Result<RecorderCopyResult> copied = sink_->ReserveCopyCommit(
            RecorderCopyRequest{.metadata = &metadata_,
                                .payload = payload_,
                                .full_policy = BufferFullPolicy::kBlock,
                                .timeout = 0ns,
                                .user_tag = metadata_.source.source_sequence});
        if (!copied.ok()) return copied.status();
        RecorderRecordResult result;
        result.disposition =
            copied->admission == BufferAdmission::kAccepted
                ? RecorderRecordDisposition::kBuffered
                : RecorderRecordDisposition::kDropped;
        result.status = Status::Ok();
        return result;
    }
    bool has_pending() const noexcept override { return false; }
    size_t pending_bytes() const noexcept override { return 0; }
    bool pending_persistence_configured() const noexcept override { return true; }
    Status PersistPending() noexcept override { return Status::Ok(); }

    bool WaitUntilEntered(std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex_);
        return condition_.wait_for(lock, timeout, [this] { return entered_; });
    }
    void Release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        condition_.notify_all();
    }

private:
    RecorderEnqueueSink* sink_ = nullptr;
    RecorderRecordMetadata metadata_;
    std::vector<std::byte> payload_;
    std::atomic<bool> emitted_{false};
    std::mutex mutex_;
    std::condition_variable condition_;
    bool entered_ = false;
    bool released_ = false;
};

class FatalServiceSource final : public RecorderServiceSource {
public:
    Result<RecorderRecordResult> PollOne() noexcept override {
        return Status::Error(StatusCode::kInternal, "injected source failure");
    }
    bool has_pending() const noexcept override { return false; }
    size_t pending_bytes() const noexcept override { return 0; }
    bool pending_persistence_configured() const noexcept override { return true; }
    Status PersistPending() noexcept override { return Status::Ok(); }
};

TEST(RecorderServiceTest, PollsSubscriberPumpsAndStopsDurably) {
    auto artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestDirectory("pipeline");

    auto recorder = Recorder::Create(
        root, RecordingSessionMetadata{.recording_id = 101,
                                       .created_at_ns = 1,
                                       .owner_id = 2,
                                       .owner_epoch = 3,
                                       .config_version = 1});
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    RecorderTopicConfig topic;
    topic.topic_id = TopicId{19};
    topic.topic_name = "events";
    topic.config_version = 1;
    topic.policy.mode = RecordingMode::kBestEffort;
    topic.policy.backpressure_topology = RecordBackpressureTopology::kIsolated;
    topic.schemas.push_back(RecorderTopicSchema{
        .identity = artifact->identity,
        .descriptor_artifact = artifact->bytes,
    });
    ASSERT_TRUE((*recorder)->AddTopic(topic).ok());
    const SchemaRef schema_ref =
        (*recorder)->manifest_snapshot().topics[0].schema_snapshot[0].schema_ref;

    AtomicClock clock;
    MessageMetadata runtime_metadata{
        .message_type = StaticMessageTraits<TestMessage>::message_type,
        .schema_version = artifact->identity.schema_version(),
        .schema_short_id = artifact->identity.short_id(),
        .schema_layout_version = artifact->identity.layout_version(),
        .sequence_num = 1,
        .timestamp_ns = 900,
        .payload = ShmHandle{.offset = 64, .generation = 1, .region_id = 1},
        .payload_len = sizeof(TestMessage),
        .flags = 0,
    };
    OneBorrowSource borrow_source(runtime_metadata);
    Encoder encoder;
    FixedRecorderSourceResolver resolver(7, 8, 1);
    RecorderEnqueueSink sink(**recorder, 0);
    auto pending_store = FileRecorderPendingStore::Open(
        root / "pending/events-0.pending");
    ASSERT_TRUE(pending_store.ok()) << pending_store.status().ToString();
    RecorderSubscriberOptions subscriber_options{
        .topic_id = TopicId{19},
        .schema = RecorderSchemaMetadata{
            .short_id = artifact->identity.short_id(),
            .canonical_digest = artifact->identity.canonical_digest(),
            .schema_version = artifact->identity.schema_version(),
            .layout_version = artifact->identity.layout_version(),
        },
        .schema_ref = schema_ref,
        .source_identity = MessageSource{7, 8, 1, 0, 0},
        .full_policy = BufferFullPolicy::kBlock,
        .max_canonical_payload_bytes = 1024,
        .pending_retry_timeout = 1ms,
        .pending_store = pending_store->get(),
    };
    auto subscriber = RecorderSubscriber<TestMessage>::Create(
        subscriber_options, &borrow_source, &encoder, &resolver, &sink, &clock);
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();

    auto service = RecorderService::Create(
        std::move(*recorder),
        RecorderServiceOptions{.idle_poll_interval = 100us,
                               .max_records_per_partition_per_cycle = 16,
                               .pending_flush_attempts = 10,
                               .stop_flush_level = RecordAckLevel::kDurable},
        &clock);
    ASSERT_TRUE(service.ok()) << service.status().ToString();
    ASSERT_TRUE((*service)
                    ->AddSource(std::make_unique<
                                RecorderSubscriberSourceAdapter<TestMessage>>(
                        std::move(*subscriber)))
                    .ok());
    ASSERT_TRUE((*service)->Start().ok());

    for (size_t attempt = 0; attempt < 1000; ++attempt) {
        if ((*service)->status().metrics.source_buffered_records == 1) break;
        std::this_thread::sleep_for(100us);
    }
    ASSERT_TRUE(borrow_source.acked());
    ASSERT_EQ((*service)->status().metrics.source_buffered_records, 1u);
    ASSERT_TRUE((*service)->Flush().ok());
    ASSERT_TRUE((*service)->Stop().ok());

    const RecorderServiceStatus stopped = (*service)->status();
    EXPECT_EQ(stopped.state, RecorderServiceState::kStopped);
    EXPECT_EQ(stopped.recorder_state, RecorderState::kStopped);
    EXPECT_EQ(stopped.recorder_metrics.durable_records, 1u);
    EXPECT_FALSE(std::filesystem::exists(root / "pending/events-0.pending"));
    service->reset();

    auto recovered_service = RecorderService::OpenRecovered(root);
    ASSERT_TRUE(recovered_service.ok())
        << recovered_service.status().ToString();
    const RecorderServiceStatus recovered_status =
        (*recovered_service)->status();
    ASSERT_TRUE(recovered_status.recovery_report.has_value());
    EXPECT_EQ(recovered_status.recovery_report->partitions_recovered, 1u);
    EXPECT_EQ(recovered_status.state, RecorderServiceState::kCreated);
    recovered_service->reset();

    const std::filesystem::path partition_root =
        root / "topics/19/partitions/0000";
    auto manifest = PartitionManifest::Open(partition_root);
    ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
    ASSERT_TRUE((*manifest)->snapshot().checkpoint.has_value());
    EXPECT_EQ((*manifest)->snapshot().checkpoint->durable_sequence, 1u);
    ASSERT_EQ((*manifest)->snapshot().segments.size(), 1u);
    auto scanned = ScanSegment(
        partition_root / (*manifest)->snapshot().segments[0].relative_path);
    ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
    EXPECT_TRUE(scanned->clean());
    EXPECT_EQ(scanned->records_scanned, 1u);
}

TEST(RecorderServiceTest, FlushWaitsForConcurrentIntakeAndDurableBarrier) {
    auto artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestDirectory("flush_barrier");
    auto recorder = Recorder::Create(
        root, RecordingSessionMetadata{.recording_id = 303,
                                       .created_at_ns = 1,
                                       .owner_id = 2,
                                       .owner_epoch = 3,
                                       .config_version = 1});
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    RecorderTopicConfig topic;
    topic.topic_id = TopicId{19};
    topic.topic_name = "events";
    topic.config_version = 1;
    topic.policy.mode = RecordingMode::kBestEffort;
    topic.policy.backpressure_topology = RecordBackpressureTopology::kIsolated;
    topic.schemas.push_back(RecorderTopicSchema{
        .identity = artifact->identity,
        .descriptor_artifact = artifact->bytes,
    });
    ASSERT_TRUE((*recorder)->AddTopic(topic).ok());

    AtomicClock clock;
    RecorderEnqueueSink sink(**recorder, 0);
    const std::vector<std::byte> payload{std::byte{0x08}, std::byte{0x2a}};
    RecorderRecordMetadata metadata{
        .schema = RecorderSchemaMetadata{
            .short_id = artifact->identity.short_id(),
            .canonical_digest = artifact->identity.canonical_digest(),
            .schema_version = artifact->identity.schema_version(),
            .layout_version = artifact->identity.layout_version(),
        },
        .topic_id = TopicId{19},
        .source = MessageSource{7, 8, 1, 1, 900},
        .ingestion_timestamp_ns = 1001,
        .payload_size = static_cast<uint32_t>(payload.size()),
        .payload_crc = RecorderPayloadCrc32c(payload),
    };
    auto blocking = std::make_unique<BlockingServiceSource>(
        &sink, metadata, payload);
    BlockingServiceSource* blocking_pointer = blocking.get();
    auto service = RecorderService::Create(
        std::move(*recorder),
        RecorderServiceOptions{.idle_poll_interval = 100us,
                               .max_records_per_partition_per_cycle = 16,
                               .pending_flush_attempts = 10,
                               .stop_flush_level = RecordAckLevel::kDurable},
        &clock);
    ASSERT_TRUE(service.ok()) << service.status().ToString();
    ASSERT_TRUE((*service)->AddSource(std::move(blocking)).ok());
    ASSERT_TRUE((*service)->Start().ok());
    ASSERT_TRUE(blocking_pointer->WaitUntilEntered(1s));

    std::atomic<bool> flush_done{false};
    Status flush_status = Status::Error(StatusCode::kInternal);
    std::thread flusher([&] {
        flush_status = (*service)->Flush(RecordAckLevel::kDurable);
        flush_done.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(10ms);
    EXPECT_FALSE(flush_done.load(std::memory_order_acquire));
    blocking_pointer->Release();
    flusher.join();

    ASSERT_TRUE(flush_status.ok()) << flush_status.ToString();
    EXPECT_EQ((*service)->status().recorder_metrics.durable_records, 1u);
    ASSERT_TRUE((*service)->Stop().ok());
}

TEST(RecorderServiceTest, SourceErrorDrainsAckedPendingBeforeStop) {
    auto artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::filesystem::path root = TestDirectory("error_pending");
    auto recorder = Recorder::Create(
        root, RecordingSessionMetadata{.recording_id = 202,
                                       .created_at_ns = 1,
                                       .owner_id = 2,
                                       .owner_epoch = 3,
                                       .config_version = 1});
    ASSERT_TRUE(recorder.ok()) << recorder.status().ToString();
    RecorderTopicConfig topic;
    topic.topic_id = TopicId{19};
    topic.topic_name = "events";
    topic.config_version = 1;
    topic.policy.mode = RecordingMode::kMemoryBuffered;
    topic.policy.full_policy = BufferFullPolicy::kBlock;
    topic.policy.backpressure_topology =
        RecordBackpressureTopology::kStrongConsistent;
    topic.policy.require_complete_recording = true;
    topic.buffer_pool_options.global_byte_limit =
        kRecorderSmallBufferClassBytes;
    topic.buffer_pool_options.default_topic_byte_limit =
        kRecorderSmallBufferClassBytes;
    topic.buffer_pool_options.queue_capacity = 1;
    topic.schemas.push_back(RecorderTopicSchema{
        .identity = artifact->identity,
        .descriptor_artifact = artifact->bytes,
    });
    ASSERT_TRUE((*recorder)->AddTopic(topic).ok());
    const SchemaRef schema_ref =
        (*recorder)->manifest_snapshot().topics[0].schema_snapshot[0].schema_ref;

    AtomicClock clock;
    const MessageMetadata runtime_metadata{
        .message_type = StaticMessageTraits<TestMessage>::message_type,
        .schema_version = artifact->identity.schema_version(),
        .schema_short_id = artifact->identity.short_id(),
        .schema_layout_version = artifact->identity.layout_version(),
        .sequence_num = 1,
        .timestamp_ns = 900,
        .payload = ShmHandle{.offset = 64, .generation = 1, .region_id = 1},
        .payload_len = sizeof(TestMessage),
        .flags = 0,
    };
    OneBorrowSource first_source(runtime_metadata);
    OneBorrowSource second_source(runtime_metadata);
    Encoder encoder;
    FixedRecorderSourceResolver first_resolver(7, 8, 1);
    FixedRecorderSourceResolver second_resolver(7, 9, 1);
    RecorderEnqueueSink first_sink(**recorder, 0);
    RecorderEnqueueSink second_sink(**recorder, 0);
    auto first_store = FileRecorderPendingStore::Open(
        root / "pending/first.pending");
    auto second_store = FileRecorderPendingStore::Open(
        root / "pending/second.pending");
    ASSERT_TRUE(first_store.ok()) << first_store.status().ToString();
    ASSERT_TRUE(second_store.ok()) << second_store.status().ToString();
    auto make_options = [&](RecorderPendingStore* store,
                            uint64_t publisher_id) {
        return RecorderSubscriberOptions{
            .topic_id = TopicId{19},
            .schema = RecorderSchemaMetadata{
                .short_id = artifact->identity.short_id(),
                .canonical_digest = artifact->identity.canonical_digest(),
                .schema_version = artifact->identity.schema_version(),
                .layout_version = artifact->identity.layout_version(),
            },
            .schema_ref = schema_ref,
            .source_identity = MessageSource{7, publisher_id, 1, 0, 0},
            .full_policy = BufferFullPolicy::kBlock,
            .max_canonical_payload_bytes = 1024,
            .pending_retry_timeout = 1ms,
            .pending_store = store,
        };
    };
    auto first_subscriber = RecorderSubscriber<TestMessage>::Create(
        make_options(first_store->get(), 8), &first_source, &encoder,
        &first_resolver, &first_sink, &clock);
    auto second_subscriber = RecorderSubscriber<TestMessage>::Create(
        make_options(second_store->get(), 9), &second_source, &encoder,
        &second_resolver, &second_sink, &clock);
    ASSERT_TRUE(first_subscriber.ok()) << first_subscriber.status().ToString();
    ASSERT_TRUE(second_subscriber.ok()) << second_subscriber.status().ToString();

    auto service = RecorderService::Create(
        std::move(*recorder),
        RecorderServiceOptions{.idle_poll_interval = 100us,
                               .max_records_per_partition_per_cycle = 16,
                               .pending_flush_attempts = 20,
                               .stop_flush_level = RecordAckLevel::kDurable},
        &clock);
    ASSERT_TRUE(service.ok()) << service.status().ToString();
    ASSERT_TRUE((*service)
                    ->AddSource(std::make_unique<
                                RecorderSubscriberSourceAdapter<TestMessage>>(
                        std::move(*first_subscriber)))
                    .ok());
    ASSERT_TRUE((*service)
                    ->AddSource(std::make_unique<
                                RecorderSubscriberSourceAdapter<TestMessage>>(
                        std::move(*second_subscriber)))
                    .ok());
    ASSERT_TRUE(
        (*service)->AddSource(std::make_unique<FatalServiceSource>()).ok());
    ASSERT_TRUE((*service)->Start().ok());

    for (size_t attempt = 0; attempt < 1000; ++attempt) {
        if ((*service)->status().state == RecorderServiceState::kError) break;
        std::this_thread::sleep_for(100us);
    }
    EXPECT_EQ((*service)->status().state, RecorderServiceState::kError);
    EXPECT_TRUE(first_source.acked());
    EXPECT_TRUE(second_source.acked());
    EXPECT_FALSE(std::filesystem::exists(root / "pending/first.pending"));
    EXPECT_FALSE(std::filesystem::exists(root / "pending/second.pending"));
    EXPECT_EQ((*service)->status().recorder_metrics.durable_records, 2u);
    ASSERT_TRUE((*service)->Stop().ok());
}

TEST(RecorderServiceCapacityTest, WorkerThreadChargeIsExclusiveAndReleased) {
    capacity::NodeBudget budget;
    budget.limit.threads = 1;
    auto controller_result = capacity::CapacityController::Create(budget);
    ASSERT_TRUE(controller_result.ok())
        << controller_result.status().ToString();
    auto controller = std::move(*controller_result);
    const RecordingSessionMetadata metadata{
        .recording_id = 901,
        .created_at_ns = 1,
        .owner_id = 2,
        .owner_epoch = 3,
        .config_version = 1,
    };

    auto first_recorder =
        Recorder::Create(TestDirectory("capacity-first"), metadata);
    ASSERT_TRUE(first_recorder.ok()) << first_recorder.status().ToString();
    auto first_service = RecorderService::Create(
        std::move(*first_recorder), {}, nullptr, controller);
    ASSERT_TRUE(first_service.ok()) << first_service.status().ToString();
    EXPECT_EQ(controller->Snapshot().committed.threads, 1u);

    auto second_recorder =
        Recorder::Create(TestDirectory("capacity-second"), metadata);
    ASSERT_TRUE(second_recorder.ok()) << second_recorder.status().ToString();
    auto denied = RecorderService::Create(
        std::move(*second_recorder), {}, nullptr, controller);
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(denied.status().code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(controller->Snapshot().pending.empty());

    first_service->reset();
    EXPECT_EQ(controller->Snapshot().committed.threads, 0u);
}

}  // namespace
}  // namespace mino::storage
