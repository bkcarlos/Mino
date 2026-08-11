// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_TOPIC_WRITER_H_
#define MINO_STORAGE_TOPIC_WRITER_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/recording_policy.h"
#include "mino/storage/schema_store.h"
#include "mino/storage/segment_writer.h"

namespace mino::storage {

inline constexpr uint32_t kGapPayloadMagic = 0x5041474du;  // "MGAP" on disk.
inline constexpr uint16_t kGapPayloadVersion = 1;
inline constexpr size_t kEncodedGapPayloadSize = 24;

// The high bit is reserved by TopicWriter's queue contract. Producers set it
// when a committed reservation represents a tombstone rather than data. The
// remaining bits stay available as the producer's opaque correlation tag.
inline constexpr uint64_t kTopicWriterTombstoneUserTagMask = 1ull << 63;
inline constexpr uint64_t kTopicWriterUserTagValueMask =
    ~kTopicWriterTombstoneUserTagMask;

enum class GapReason : uint16_t {
    kSourceSequenceJump = 1,
    kDropNewest = 2,
    kDropOldest = 3,
    kDropNewestNoDroppableOldest = 4,
    kAllocationFailure = 5,
    kRecordingFailed = 6,
    kReservationCancelled = 7,
    // A crash left a manifest-declared ingestion reservation with no complete
    // record. Recorder recovery consumes that reservation with a schema-less Gap.
    kRecorderRestartRecovery = 8,
};

struct GapPayload {
    GapReason reason = GapReason::kSourceSequenceJump;
    uint64_t first_missing_source_sequence = 0;
    uint64_t last_missing_source_sequence = 0;

    bool operator==(const GapPayload&) const = default;
};

Result<std::vector<std::byte>> EncodeGapPayload(
    const GapPayload& payload) noexcept;
Result<GapPayload> DecodeGapPayload(std::span<const std::byte> encoded) noexcept;

enum class TopicWriterState : uint8_t {
    kCreated,
    kRunning,
    kStopping,
    kStopped,
    kError,
};



struct TopicWriterAck {
    RecordAckLevel level = RecordAckLevel::kWritten;
    uint64_t ingestion_sequence = 0;
    uint64_t segment_id = 0;
    uint16_t record_flags = 0;
    uint64_t user_tag = 0;
    MessageSource source;
};

using TopicWriterAckCallback =
    void (*)(const TopicWriterAck& ack, void* context) noexcept;
using TopicWriterSchemaResolver =
    Result<SchemaRef> (*)(const RecorderSchemaMetadata& schema,
                         void* context) noexcept;
using TopicWriterDirectorySyncHook =
    int (*)(int directory_fd, void* context) noexcept;

struct TopicWriterOptions {
    std::filesystem::path partition_root;
    uint64_t recording_id = 0;
    TopicId topic_id{};
    uint32_t partition_id = 0;
    uint64_t writer_id = 0;

    // Zero derives the first sequence from the manifest (or one for a new
    // partition). A non-zero value must be strictly after all tracked segments.
    uint64_t initial_ingestion_sequence = 0;
    SegmentWriterOptions segment_options{};
    // Test/platform seam for the mandatory segments-directory fsync after a new
    // Segment entry is created and before any durable checkpoint/ACK is exposed.
    TopicWriterDirectorySyncHook directory_sync_hook = nullptr;
    void* directory_sync_context = nullptr;

    // Stop normally owns queue shutdown and drains all committed records. Set
    // false only when a higher-level owner shares the pool lifetime.
    bool close_buffer_pool_on_stop = true;

    // If set, this resolver is used instead of SchemaStore. It must validate the
    // complete RecorderSchemaMetadata identity and return its session-local ref.
    TopicWriterSchemaResolver schema_resolver = nullptr;
    void* schema_resolver_context = nullptr;

    TopicWriterAckCallback ack_callback = nullptr;
    void* ack_context = nullptr;
};

struct TopicWriterPumpResult {
    size_t dequeued_records = 0;
    size_t data_records = 0;
    size_t tombstone_records = 0;
    size_t gap_records = 0;
    size_t duplicate_records = 0;
    size_t rotations = 0;
};

// Synchronous, single-logical-owner writer for one Topic/Partition. Public
// calls are serialized defensively. ACK callbacks are always dispatched after
// releasing the internal mutex.
class TopicWriter final {
public:
    static Result<std::unique_ptr<TopicWriter>> Create(
        TopicWriterOptions options, RecorderBufferPool* buffer_pool,
        PartitionManifest* partition_manifest,
        const SchemaStore* schema_store = nullptr) noexcept;

    ~TopicWriter();

    TopicWriter(const TopicWriter&) = delete;
    TopicWriter& operator=(const TopicWriter&) = delete;
    TopicWriter(TopicWriter&&) = delete;
    TopicWriter& operator=(TopicWriter&&) = delete;

    Status Start(uint64_t now_ns);
    Result<TopicWriterPumpResult> Pump(
        uint64_t now_ns,
        size_t max_records = std::numeric_limits<size_t>::max());
    Status Flush(RecordAckLevel level, uint64_t now_ns);
    Status Stop(uint64_t now_ns);

    Status ReportDiscard(const DiscardedBuffer& discarded);
    Status ReportDiscards(std::span<const DiscardedBuffer> discarded);

    TopicWriterState state() const;
    Status error_status() const;
    uint64_t next_ingestion_sequence() const;
    uint64_t duplicate_count() const;
    SegmentWriterFailureKind failure_kind() const;

private:
    struct SourceKey {
        uint64_t node_id = 0;
        uint64_t publisher_id = 0;
        uint64_t publisher_epoch = 0;

        bool operator==(const SourceKey&) const = default;
    };

    struct SourceKeyHash {
        size_t operator()(const SourceKey& key) const noexcept;
    };

    struct SourceProgress {
        bool has_last_sequence = false;
        uint64_t last_sequence = 0;
        std::map<uint64_t, GapReason> reported_discards;
    };

    struct PendingAck {
        uint64_t ingestion_sequence = 0;
        uint64_t segment_id = 0;
        uint16_t record_flags = 0;
        uint64_t user_tag = 0;
        MessageSource source;
    };

    TopicWriter(TopicWriterOptions options, RecorderBufferPool* buffer_pool,
                PartitionManifest* partition_manifest,
                const SchemaStore* schema_store,
                uint64_t first_sequence, uint64_t next_segment_id) noexcept;

    Status ValidateManifestIdentity() const;
    Result<SchemaRef> ResolveSchema(
        const RecorderSchemaMetadata& schema) const;
    Result<uint64_t> AllocateSequenceLocked();
    Status ProcessHandleLocked(RecorderBufferHandle handle, uint64_t now_ns,
                               TopicWriterPumpResult* result,
                               std::vector<TopicWriterAck>* callbacks);
    Status AppendRecordLocked(Record record, PendingAck ack, uint64_t now_ns,
                              TopicWriterPumpResult* result,
                              std::vector<TopicWriterAck>* callbacks);
    Status EnsureSegmentLocked(uint64_t first_sequence, uint64_t now_ns);
    Status SyncSegmentsDirectoryLocked();
    Status FlushCurrentLocked(uint64_t now_ns,
                              std::vector<TopicWriterAck>* callbacks);
    Status SealCurrentLocked(uint64_t now_ns,
                             TopicWriterPumpResult* result,
                             std::vector<TopicWriterAck>* callbacks);
    Status UpdateOpenManifestLocked();
    Status PublishDurabilityLocked(std::vector<TopicWriterAck>* callbacks);
    Status ReportDiscardLocked(const DiscardedBuffer& discarded);
    Status PoisonLocked(Status status);

    static SourceKey KeyOf(const MessageSource& source) noexcept;
    static GapReason GapReasonFor(BufferDiscardReason reason) noexcept;
    static GapReason SelectGapReason(const SourceProgress& progress,
                                     uint64_t first_missing,
                                     uint64_t last_missing) noexcept;
    static void DispatchCallbacks(TopicWriterAckCallback callback, void* context,
                                  std::span<const TopicWriterAck> callbacks)
        noexcept;

    mutable std::mutex mutex_;
    TopicWriterOptions options_;
    RecorderBufferPool* buffer_pool_ = nullptr;
    PartitionManifest* partition_manifest_ = nullptr;
    const SchemaStore* schema_store_ = nullptr;

    TopicWriterState state_ = TopicWriterState::kCreated;
    Status error_status_ = Status::Ok();
    uint64_t next_ingestion_sequence_ = 1;
    bool sequence_exhausted_ = false;
    uint64_t next_segment_id_ = 1;
    uint64_t duplicate_count_ = 0;
    uint64_t last_now_ns_ = 0;

    std::unordered_map<SourceKey, SourceProgress, SourceKeyHash> sources_;
    std::unique_ptr<SegmentWriter> segment_writer_;
    std::optional<SegmentManifestEntry> active_segment_;
    std::vector<PendingAck> active_acks_;
    size_t written_ack_count_ = 0;
    size_t durable_ack_count_ = 0;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_TOPIC_WRITER_H_
