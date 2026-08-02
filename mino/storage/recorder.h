// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RECORDER_H_
#define MINO_STORAGE_RECORDER_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/descriptor.h"
#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/recording_policy.h"
#include "mino/storage/recording_topology.h"
#include "mino/storage/schema_store.h"
#include "mino/storage/snapshot_store.h"
#include "mino/storage/topic_writer.h"

namespace mino::storage {

// Session-level lifecycle. kDegraded means at least one Topic/Partition failed,
// while healthy partitions remain usable. Only session metadata/schema failures
// put the whole Recorder in kError.
enum class RecorderState : uint8_t {
    kCreated = 0,
    kRunning = 1,
    kDegraded = 2,
    kStopping = 3,
    kStopped = 4,
    kError = 5,
};

struct RecorderSessionOptions {
    ManifestOptions manifest_options{};
    SchemaStoreOptions schema_store_options{};
};

// AddTopic persists every descriptor before publishing the corresponding
// TopicTable schema snapshot. On a reopened session descriptor_artifact may be
// empty if the exact identity is already present in SchemaStore.
struct RecorderTopicSchema {
    schema::SchemaIdentity identity;
    std::vector<std::byte> descriptor_artifact;
};

struct RecorderTopicConfig {
    TopicId topic_id{};
    std::string topic_name;
    uint64_t config_version = 0;
    uint32_t partition_count = 1;
    RecordingPolicy policy{};
    RecorderBufferPoolOptions buffer_pool_options{};
    SegmentWriterOptions segment_options{};
    SnapshotStoreOptions snapshot_options{};
    // Zero derives a stable, non-zero writer ID from the session/topic/partition.
    // Otherwise partition N uses writer_id_base + N.
    uint64_t writer_id_base = 0;
    std::vector<RecorderTopicSchema> schemas;
};

enum class RecorderEnqueueDisposition : uint8_t {
    kBuffered = 0,
    kDropped = 1,
    kBlocked = 2,
    kFailed = 3,
};

struct RecorderEnqueueRequest {
    uint32_t partition_id = 0;
    RecorderRecordMetadata metadata{};
    std::span<const std::byte> payload;
    uint64_t user_tag = 0;
    // The adapter's source-channel cursor. If absent, Recorder uses the next
    // contiguous partition cursor. It is intentionally independent of a
    // publisher's source_sequence because one partition may have many sources.
    std::optional<uint64_t> available_cursor;
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::zero();
};

struct RecorderEnqueueResult {
    RecorderEnqueueDisposition disposition =
        RecorderEnqueueDisposition::kFailed;
    Status status = Status::Ok();
    // The configured acknowledgement reached before Enqueue returned. Snapshot
    // mode has no RecordAckLevel and leaves this absent.
    std::optional<RecordAckLevel> acknowledged;
    std::vector<DiscardedBuffer> discarded;
    std::vector<RecordingGapDebt> gap_debts;
};

struct RecorderOperationFailure {
    TopicId topic_id{};
    uint32_t partition_id = 0;
    Status status = Status::Ok();
};

struct RecorderPumpResult {
    size_t partitions_visited = 0;
    size_t dequeued_records = 0;
    size_t data_records = 0;
    size_t tombstone_records = 0;
    size_t gap_records = 0;
    size_t duplicate_records = 0;
    size_t rotations = 0;
    std::vector<RecorderOperationFailure> failures;
};

struct RecorderMetrics {
    uint64_t enqueue_calls = 0;
    uint64_t accepted_records = 0;
    uint64_t buffered_records = 0;
    uint64_t written_records = 0;
    uint64_t durable_records = 0;
    uint64_t dropped_records = 0;
    uint64_t blocked_records = 0;
    uint64_t gap_debts = 0;
    uint64_t duplicate_records = 0;
    uint64_t writer_failures = 0;
    uint64_t pump_calls = 0;
    uint64_t flush_calls = 0;
};

struct RecorderPartitionStatus {
    TopicId topic_id{};
    uint32_t partition_id = 0;
    RecordingMode recording_mode = RecordingMode::kBestEffort;
    bool has_snapshot = false;
    TopicWriterState writer_state = TopicWriterState::kCreated;
    RecordingTopologyState topology_state = RecordingTopologyState::kActive;
    Status error_status = Status::Ok();
    RecorderBufferPoolStats buffer_pool{};
    RecordingTopologyMetrics topology_metrics{};
    uint64_t next_ingestion_sequence = 1;
};

// Product-level synchronous orchestration core. It owns no Bus or worker
// threads; callers may drive it from an executor, a polling loop, or tests.
// Public operations are serialized, and no TopicWriter is executed concurrently.
class Recorder final {
public:
    static Result<std::unique_ptr<Recorder>> Create(
        const std::filesystem::path& session_root,
        const RecordingSessionMetadata& metadata,
        const RecorderSessionOptions& options = {}) noexcept;
    static Result<std::unique_ptr<Recorder>> Open(
        const std::filesystem::path& session_root,
        const RecorderSessionOptions& options = {}) noexcept;

    // Explicit aliases make the ownership boundary clear at call sites.
    static Result<std::unique_ptr<Recorder>> CreateSession(
        const std::filesystem::path& session_root,
        const RecordingSessionMetadata& metadata,
        const RecorderSessionOptions& options = {}) noexcept {
        return Create(session_root, metadata, options);
    }
    static Result<std::unique_ptr<Recorder>> OpenSession(
        const std::filesystem::path& session_root,
        const RecorderSessionOptions& options = {}) noexcept {
        return Open(session_root, options);
    }

    ~Recorder();

    Recorder(const Recorder&) = delete;
    Recorder& operator=(const Recorder&) = delete;
    Recorder(Recorder&&) = delete;
    Recorder& operator=(Recorder&&) = delete;

    // Adds a new manifest topic or attaches the same configuration after Open().
    // An already attached (session, topic, partition) tuple is rejected.
    Status AddTopic(const RecorderTopicConfig& config) noexcept;

    Status Start(uint64_t now_ns) noexcept;
    Result<RecorderEnqueueResult> Enqueue(
        const RecorderEnqueueRequest& request) noexcept;
    Result<RecorderEnqueueResult> Enqueue(
        uint32_t partition_id, const RecorderRecordMetadata& metadata,
        std::span<const std::byte> payload, uint64_t user_tag = 0,
        std::optional<uint64_t> available_cursor = std::nullopt,
        std::chrono::nanoseconds timeout =
            std::chrono::nanoseconds::zero()) noexcept {
        return Enqueue(RecorderEnqueueRequest{
            .partition_id = partition_id,
            .metadata = metadata,
            .payload = payload,
            .user_tag = user_tag,
            .available_cursor = available_cursor,
            .timeout = timeout,
        });
    }
    Result<RecorderPumpResult> Pump(
        uint64_t now_ns,
        size_t max_records_per_partition =
            std::numeric_limits<size_t>::max()) noexcept;
    Status Flush(RecordAckLevel level, uint64_t now_ns) noexcept;
    Status Stop(uint64_t now_ns) noexcept;

    RecorderState state() const noexcept;
    Status error_status() const noexcept;
    RecorderMetrics metrics() const noexcept;
    Result<RecorderPartitionStatus> GetPartitionStatus(
        TopicId topic_id, uint32_t partition_id,
        uint64_t now_ns) const noexcept;
    const RecordingManifestSnapshot& manifest_snapshot() const noexcept;

private:
    class Impl;
    static Result<std::unique_ptr<Recorder>> FinishCreate(
        const std::filesystem::path& session_root,
        RecorderSessionOptions options,
        std::unique_ptr<RecordingManifest> manifest);
    explicit Recorder(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_RECORDER_H_
