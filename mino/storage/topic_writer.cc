// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/topic_writer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <iomanip>
#include <limits>
#include <new>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

void WriteLe16(std::span<std::byte> output, size_t offset,
               uint16_t value) noexcept {
    for (size_t index = 0; index < 2; ++index) {
        output[offset + index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

void WriteLe32(std::span<std::byte> output, size_t offset,
               uint32_t value) noexcept {
    for (size_t index = 0; index < 4; ++index) {
        output[offset + index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

void WriteLe64(std::span<std::byte> output, size_t offset,
               uint64_t value) noexcept {
    for (size_t index = 0; index < 8; ++index) {
        output[offset + index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

uint16_t ReadLe16(std::span<const std::byte> input, size_t offset) noexcept {
    uint16_t value = 0;
    for (size_t index = 0; index < 2; ++index) {
        value |= static_cast<uint16_t>(static_cast<uint8_t>(input[offset + index]))
                 << (index * 8);
    }
    return value;
}

uint32_t ReadLe32(std::span<const std::byte> input, size_t offset) noexcept {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
        value |= static_cast<uint32_t>(static_cast<uint8_t>(input[offset + index]))
                 << (index * 8);
    }
    return value;
}

uint64_t ReadLe64(std::span<const std::byte> input, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(input[offset + index]))
                 << (index * 8);
    }
    return value;
}

bool ValidGapReason(GapReason reason) noexcept {
    const uint16_t value = static_cast<uint16_t>(reason);
    return value >= static_cast<uint16_t>(GapReason::kSourceSequenceJump) &&
           value <=
               static_cast<uint16_t>(GapReason::kRecorderRestartRecovery);
}

uint32_t PayloadCrc32c(std::span<const std::byte> payload) noexcept {
    uint32_t state = 0xffffffffu;
    for (std::byte byte : payload) {
        state ^= static_cast<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                    ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

std::filesystem::path SegmentRelativePath(uint64_t segment_id) {
    std::ostringstream name;
    name << std::setfill('0') << std::setw(8) << segment_id << ".mino";
    return std::filesystem::path("segments") / name.str();
}

Status AllocationFailure() {
    return Exhausted("cannot allocate TopicWriter state");
}

}  // namespace

Result<std::vector<std::byte>> EncodeGapPayload(
    const GapPayload& payload) noexcept {
    try {
        if (!ValidGapReason(payload.reason)) {
            return Invalid("gap payload reason is unknown");
        }
        if (payload.last_missing_source_sequence <
            payload.first_missing_source_sequence) {
            return Invalid("gap payload sequence range is reversed");
        }
        std::vector<std::byte> encoded(kEncodedGapPayloadSize);
        WriteLe32(encoded, 0, kGapPayloadMagic);
        WriteLe16(encoded, 4, kGapPayloadVersion);
        WriteLe16(encoded, 6, static_cast<uint16_t>(payload.reason));
        WriteLe64(encoded, 8, payload.first_missing_source_sequence);
        WriteLe64(encoded, 16, payload.last_missing_source_sequence);
        return encoded;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::length_error&) {
        return AllocationFailure();
    }
}

Result<GapPayload> DecodeGapPayload(
    std::span<const std::byte> encoded) noexcept {
    if (encoded.size() != kEncodedGapPayloadSize) {
        return Corruption("gap payload size is invalid");
    }
    if (ReadLe32(encoded, 0) != kGapPayloadMagic) {
        return Corruption("gap payload magic is invalid");
    }
    if (ReadLe16(encoded, 4) != kGapPayloadVersion) {
        return Corruption("gap payload version is unsupported");
    }
    const GapReason reason = static_cast<GapReason>(ReadLe16(encoded, 6));
    if (!ValidGapReason(reason)) {
        return Corruption("gap payload reason is unknown");
    }
    GapPayload payload{
        .reason = reason,
        .first_missing_source_sequence = ReadLe64(encoded, 8),
        .last_missing_source_sequence = ReadLe64(encoded, 16),
    };
    if (payload.last_missing_source_sequence <
        payload.first_missing_source_sequence) {
        return Corruption("gap payload sequence range is reversed");
    }
    return payload;
}

Result<std::unique_ptr<TopicWriter>> TopicWriter::Create(
    TopicWriterOptions options, RecorderBufferPool* buffer_pool,
    PartitionManifest* partition_manifest,
    const SchemaStore* schema_store) noexcept {
    try {
        if (buffer_pool == nullptr || partition_manifest == nullptr) {
            return Invalid("TopicWriter dependencies must not be null");
        }
        if (options.partition_root.empty() || options.recording_id == 0 ||
            options.topic_id.value == 0 || options.writer_id == 0) {
            return Invalid("TopicWriter identity is incomplete");
        }
        if (options.schema_resolver == nullptr && schema_store == nullptr) {
            return Invalid("TopicWriter requires a schema resolver");
        }

        const PartitionManifestSnapshot& snapshot =
            partition_manifest->snapshot();
        if (snapshot.partition.recording_id != options.recording_id ||
            snapshot.partition.topic_id != options.topic_id.value ||
            snapshot.partition.partition_id != options.partition_id ||
            snapshot.partition.writer_id != options.writer_id) {
            return Invalid("TopicWriter identity does not match manifest");
        }
        if (!snapshot.segments.empty()) {
            const SegmentPersistentState state = snapshot.segments.back().state;
            if (state == SegmentPersistentState::kCreating ||
                state == SegmentPersistentState::kOpen) {
                return Unavailable("partition already has an active segment");
            }
        }

        uint64_t first_sequence = 1;
        uint64_t next_segment_id = 1;
        if (!snapshot.segments.empty()) {
            const SegmentManifestEntry& last = snapshot.segments.back();
            if (last.last_ingestion_sequence ==
                    std::numeric_limits<uint64_t>::max() ||
                last.segment_id == std::numeric_limits<uint64_t>::max()) {
                return Exhausted("partition sequence or segment ID is exhausted");
            }
            first_sequence = last.last_ingestion_sequence + 1;
            next_segment_id = last.segment_id + 1;
        }
        if (options.initial_ingestion_sequence != 0) {
            if (options.initial_ingestion_sequence < first_sequence) {
                return Invalid("initial ingestion sequence overlaps manifest");
            }
            first_sequence = options.initial_ingestion_sequence;
        }

        std::unique_ptr<TopicWriter> writer(new TopicWriter(
            std::move(options), buffer_pool, partition_manifest, schema_store,
            first_sequence, next_segment_id));
        return writer;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::length_error&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

TopicWriter::TopicWriter(TopicWriterOptions options,
                         RecorderBufferPool* buffer_pool,
                         PartitionManifest* partition_manifest,
                         const SchemaStore* schema_store,
                         uint64_t first_sequence,
                         uint64_t next_segment_id) noexcept
    : options_(std::move(options)),
      buffer_pool_(buffer_pool),
      partition_manifest_(partition_manifest),
      schema_store_(schema_store),
      next_ingestion_sequence_(first_sequence),
      next_segment_id_(next_segment_id) {}

TopicWriter::~TopicWriter() = default;

Status TopicWriter::Start(uint64_t now_ns) {
    std::lock_guard lock(mutex_);
    if (state_ == TopicWriterState::kError) return error_status_;
    if (state_ != TopicWriterState::kCreated) {
        return Invalid("TopicWriter can only start once");
    }
    const Status identity = ValidateManifestIdentity();
    if (!identity.ok()) return PoisonLocked(identity);
    last_now_ns_ = now_ns;
    state_ = TopicWriterState::kRunning;
    return Status::Ok();
}

Result<TopicWriterPumpResult> TopicWriter::Pump(uint64_t now_ns,
                                                size_t max_records) {
    std::vector<TopicWriterAck> callbacks;
    TopicWriterPumpResult result;
    Status status = Status::Ok();
    {
        std::unique_lock lock(mutex_);
        if (state_ == TopicWriterState::kError) return error_status_;
        if (state_ != TopicWriterState::kRunning) {
            return Invalid("TopicWriter is not running");
        }
        if (now_ns < last_now_ns_) {
            return Invalid("TopicWriter time must be nondecreasing");
        }
        last_now_ns_ = now_ns;
        try {
            while (result.dequeued_records < max_records) {
                Result<RecorderBufferHandle> dequeued =
                    buffer_pool_->TryDequeue();
                if (!dequeued.ok()) {
                    if (dequeued.status().code() == StatusCode::kWouldBlock) {
                        break;
                    }
                    if (dequeued.status().code() == StatusCode::kUnavailable &&
                        !buffer_pool_->stats().recording_failed) {
                        break;
                    }
                    status = PoisonLocked(dequeued.status());
                    break;
                }
                ++result.dequeued_records;
                status = ProcessHandleLocked(std::move(*dequeued), now_ns,
                                             &result, &callbacks);
                if (!status.ok()) break;
            }
            if (status.ok()) {
                status = FlushCurrentLocked(now_ns, &callbacks);
                if (!status.ok()) status = PoisonLocked(status);
            }
        } catch (const std::bad_alloc&) {
            status = PoisonLocked(AllocationFailure());
        } catch (const std::length_error&) {
            status = PoisonLocked(AllocationFailure());
        } catch (const std::exception& error) {
            status = PoisonLocked(Invalid(error.what()));
        }
        lock.unlock();
    }
    DispatchCallbacks(options_.ack_callback, options_.ack_context, callbacks);
    if (!status.ok()) return status;
    return result;
}

Status TopicWriter::Flush(RecordAckLevel level, uint64_t now_ns) {
    std::vector<TopicWriterAck> callbacks;
    Status status = Status::Ok();
    {
        std::unique_lock lock(mutex_);
        if (state_ == TopicWriterState::kError) return error_status_;
        if (state_ != TopicWriterState::kRunning) {
            return Invalid("TopicWriter is not running");
        }
        if (now_ns < last_now_ns_) {
            return Invalid("TopicWriter time must be nondecreasing");
        }
        last_now_ns_ = now_ns;
        try {
            if (level == RecordAckLevel::kWritten) {
                status = FlushCurrentLocked(now_ns, &callbacks);
            } else if (level == RecordAckLevel::kDurable) {
                status = SealCurrentLocked(now_ns, nullptr, &callbacks);
            } else {
                status = Invalid("TopicWriter ACK level is unknown");
            }
            if (!status.ok() && status.code() != StatusCode::kInvalidArgument) {
                status = PoisonLocked(status);
            }
        } catch (const std::bad_alloc&) {
            status = PoisonLocked(AllocationFailure());
        } catch (const std::length_error&) {
            status = PoisonLocked(AllocationFailure());
        } catch (const std::exception& error) {
            status = PoisonLocked(Invalid(error.what()));
        }
        lock.unlock();
    }
    DispatchCallbacks(options_.ack_callback, options_.ack_context, callbacks);
    return status;
}

Status TopicWriter::Stop(uint64_t now_ns) {
    std::vector<TopicWriterAck> callbacks;
    TopicWriterPumpResult ignored;
    Status status = Status::Ok();
    {
        std::unique_lock lock(mutex_);
        if (state_ == TopicWriterState::kError) return error_status_;
        if (state_ == TopicWriterState::kStopped) return Status::Ok();
        if (state_ == TopicWriterState::kCreated) {
            if (options_.close_buffer_pool_on_stop) buffer_pool_->Close();
            state_ = TopicWriterState::kStopped;
            return Status::Ok();
        }
        if (state_ != TopicWriterState::kRunning) {
            return Invalid("TopicWriter stop is already in progress");
        }
        if (now_ns < last_now_ns_) {
            return Invalid("TopicWriter time must be nondecreasing");
        }
        last_now_ns_ = now_ns;
        state_ = TopicWriterState::kStopping;
        if (options_.close_buffer_pool_on_stop) buffer_pool_->Close();
        try {
            while (true) {
                Result<RecorderBufferHandle> dequeued =
                    buffer_pool_->TryDequeue();
                if (!dequeued.ok()) {
                    if ((dequeued.status().code() == StatusCode::kWouldBlock) ||
                        (dequeued.status().code() == StatusCode::kUnavailable &&
                         !buffer_pool_->stats().recording_failed)) {
                        break;
                    }
                    status = PoisonLocked(dequeued.status());
                    break;
                }
                ++ignored.dequeued_records;
                status = ProcessHandleLocked(std::move(*dequeued), now_ns,
                                             &ignored, &callbacks);
                if (!status.ok()) break;
            }
            if (status.ok()) {
                status = SealCurrentLocked(now_ns, nullptr, &callbacks);
                if (!status.ok()) status = PoisonLocked(status);
            }
            if (status.ok()) state_ = TopicWriterState::kStopped;
        } catch (const std::bad_alloc&) {
            status = PoisonLocked(AllocationFailure());
        } catch (const std::length_error&) {
            status = PoisonLocked(AllocationFailure());
        } catch (const std::exception& error) {
            status = PoisonLocked(Invalid(error.what()));
        }
        lock.unlock();
    }
    DispatchCallbacks(options_.ack_callback, options_.ack_context, callbacks);
    return status;
}

Status TopicWriter::ReportDiscard(const DiscardedBuffer& discarded) {
    std::lock_guard lock(mutex_);
    return ReportDiscardLocked(discarded);
}

Status TopicWriter::ReportDiscards(
    std::span<const DiscardedBuffer> discarded) {
    std::lock_guard lock(mutex_);
    for (const DiscardedBuffer& item : discarded) {
        const Status status = ReportDiscardLocked(item);
        if (!status.ok()) return status;
    }
    return Status::Ok();
}

TopicWriterState TopicWriter::state() const {
    std::lock_guard lock(mutex_);
    return state_;
}

Status TopicWriter::error_status() const {
    std::lock_guard lock(mutex_);
    return error_status_;
}

uint64_t TopicWriter::next_ingestion_sequence() const {
    std::lock_guard lock(mutex_);
    return next_ingestion_sequence_;
}

uint64_t TopicWriter::duplicate_count() const {
    std::lock_guard lock(mutex_);
    return duplicate_count_;
}

SegmentWriterFailureKind TopicWriter::failure_kind() const {
    std::lock_guard lock(mutex_);
    return segment_writer_ == nullptr ? SegmentWriterFailureKind::kNone
                                      : segment_writer_->failure_kind();
}

size_t TopicWriter::SourceKeyHash::operator()(const SourceKey& key) const
    noexcept {
    size_t hash = static_cast<size_t>(key.node_id ^ (key.node_id >> 32));
    hash ^= static_cast<size_t>(key.publisher_id ^ (key.publisher_id >> 32)) +
            0x9e3779b9u + (hash << 6) + (hash >> 2);
    hash ^= static_cast<size_t>(key.publisher_epoch ^
                                (key.publisher_epoch >> 32)) +
            0x9e3779b9u + (hash << 6) + (hash >> 2);
    return hash;
}

Status TopicWriter::ValidateManifestIdentity() const {
    const PartitionMetadata& metadata = partition_manifest_->snapshot().partition;
    if (metadata.recording_id != options_.recording_id ||
        metadata.topic_id != options_.topic_id.value ||
        metadata.partition_id != options_.partition_id ||
        metadata.writer_id != options_.writer_id) {
        return Invalid("TopicWriter identity no longer matches manifest");
    }
    return Status::Ok();
}

Result<SchemaRef> TopicWriter::ResolveSchema(
    const RecorderSchemaMetadata& schema) const {
    if (options_.schema_resolver != nullptr) {
        Result<SchemaRef> resolved = options_.schema_resolver(
            schema, options_.schema_resolver_context);
        if (!resolved.ok()) return resolved.status();
        if (*resolved == kInvalidSchemaRef) {
            return Status::Error(StatusCode::kNotFound,
                                 "schema resolver returned invalid ref");
        }
        return *resolved;
    }

    Result<SchemaRef> ref = schema_store_->FindRef(schema.canonical_digest);
    if (!ref.ok()) return ref.status();
    Result<SchemaStoreEntry> entry = schema_store_->Resolve(*ref);
    if (!entry.ok()) return entry.status();
    const schema::SchemaIdentity& identity = entry->identity;
    if (identity.short_id() != schema.short_id ||
        identity.canonical_digest() != schema.canonical_digest ||
        identity.schema_version() != schema.schema_version ||
        identity.layout_version() != schema.layout_version) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "recorder schema metadata does not match store");
    }
    return *ref;
}

Result<uint64_t> TopicWriter::AllocateSequenceLocked() {
    if (sequence_exhausted_) {
        return Exhausted("ingestion sequence is exhausted");
    }
    const uint64_t allocated = next_ingestion_sequence_;
    if (allocated == std::numeric_limits<uint64_t>::max()) {
        sequence_exhausted_ = true;
    } else {
        ++next_ingestion_sequence_;
    }
    return allocated;
}

Status TopicWriter::ProcessHandleLocked(
    RecorderBufferHandle handle, uint64_t now_ns,
    TopicWriterPumpResult* result, std::vector<TopicWriterAck>* callbacks) {
    if (!handle.metadata().has_value()) {
        return PoisonLocked(Corruption("dequeued recorder buffer has no metadata"));
    }
    const RecorderRecordMetadata metadata = *handle.metadata();
    if (handle.topic_id() != options_.topic_id ||
        metadata.topic_id != options_.topic_id) {
        return PoisonLocked(Corruption("dequeued recorder topic is incorrect"));
    }
    if (metadata.payload_size != handle.size()) {
        return PoisonLocked(Corruption("dequeued recorder payload size changed"));
    }
    if (PayloadCrc32c(handle.bytes()) != metadata.payload_crc) {
        return PoisonLocked(Corruption("dequeued recorder payload CRC32C mismatch"));
    }

    Result<SchemaRef> schema_ref = ResolveSchema(metadata.schema);
    if (!schema_ref.ok()) return PoisonLocked(schema_ref.status());

    const SourceKey source_key = KeyOf(metadata.source);
    SourceProgress& progress = sources_[source_key];
    if (progress.has_last_sequence &&
        metadata.source.source_sequence <= progress.last_sequence) {
        ++duplicate_count_;
        ++result->duplicate_records;
        return Status::Ok();
    }

    const bool needs_gap =
        progress.has_last_sequence &&
        metadata.source.source_sequence > progress.last_sequence + 1;
    if (needs_gap && !sequence_exhausted_ &&
        next_ingestion_sequence_ == std::numeric_limits<uint64_t>::max()) {
        return PoisonLocked(
            Exhausted("insufficient ingestion sequences for gap and data"));
    }

    if (needs_gap) {
        const uint64_t first_missing = progress.last_sequence + 1;
        const uint64_t last_missing = metadata.source.source_sequence - 1;
        const GapPayload gap_payload{
            .reason = SelectGapReason(progress, first_missing, last_missing),
            .first_missing_source_sequence = first_missing,
            .last_missing_source_sequence = last_missing,
        };
        Result<std::vector<std::byte>> encoded_gap =
            EncodeGapPayload(gap_payload);
        if (!encoded_gap.ok()) return PoisonLocked(encoded_gap.status());
        Result<uint64_t> gap_sequence = AllocateSequenceLocked();
        if (!gap_sequence.ok()) return PoisonLocked(gap_sequence.status());

        Record gap;
        gap.header.flags = kRecordFlagGap;
        gap.header.topic_id = options_.topic_id.value;
        gap.header.partition_id = options_.partition_id;
        gap.header.ingestion_sequence = *gap_sequence;
        gap.header.ingestion_timestamp_ns = metadata.ingestion_timestamp_ns;
        gap.header.node_id = metadata.source.node_id;
        gap.header.publisher_id = metadata.source.publisher_id;
        gap.header.publisher_epoch = metadata.source.publisher_epoch;
        gap.header.source_sequence = first_missing;
        gap.header.observed_timestamp_ns =
            metadata.source.observed_timestamp_ns;
        gap.payload = std::move(*encoded_gap);
        PendingAck gap_ack{
            .ingestion_sequence = *gap_sequence,
            .segment_id = 0,
            .record_flags = kRecordFlagGap,
            .user_tag = 0,
            .source = MessageSource{
                .node_id = metadata.source.node_id,
                .publisher_id = metadata.source.publisher_id,
                .publisher_epoch = metadata.source.publisher_epoch,
                .source_sequence = first_missing,
                .observed_timestamp_ns =
                    metadata.source.observed_timestamp_ns,
            },
        };
        Status status = AppendRecordLocked(std::move(gap), std::move(gap_ack),
                                           now_ns, result, callbacks);
        if (!status.ok()) return PoisonLocked(status);
        ++result->gap_records;
    }

    Result<uint64_t> ingestion_sequence = AllocateSequenceLocked();
    if (!ingestion_sequence.ok()) return PoisonLocked(ingestion_sequence.status());
    Record record;
    record.header.flags =
        (handle.user_tag() & kTopicWriterTombstoneUserTagMask) != 0
            ? kRecordFlagTombstone
            : 0;
    record.header.schema_ref = *schema_ref;
    record.header.schema_version = metadata.schema.schema_version;
    record.header.layout_version = metadata.schema.layout_version;
    record.header.topic_id = options_.topic_id.value;
    record.header.partition_id = options_.partition_id;
    record.header.ingestion_sequence = *ingestion_sequence;
    record.header.ingestion_timestamp_ns = metadata.ingestion_timestamp_ns;
    record.header.node_id = metadata.source.node_id;
    record.header.publisher_id = metadata.source.publisher_id;
    record.header.publisher_epoch = metadata.source.publisher_epoch;
    record.header.source_sequence = metadata.source.source_sequence;
    record.header.observed_timestamp_ns = metadata.source.observed_timestamp_ns;
    record.payload.assign(handle.bytes().begin(), handle.bytes().end());

    PendingAck ack{
        .ingestion_sequence = *ingestion_sequence,
        .segment_id = 0,
        .record_flags = record.header.flags,
        .user_tag = handle.user_tag() & kTopicWriterUserTagValueMask,
        .source = metadata.source,
    };
    const bool tombstone = record.header.flags == kRecordFlagTombstone;
    Status status = AppendRecordLocked(std::move(record), std::move(ack), now_ns,
                                       result, callbacks);
    if (!status.ok()) return PoisonLocked(status);

    progress.has_last_sequence = true;
    progress.last_sequence = metadata.source.source_sequence;
    progress.reported_discards.erase(
        progress.reported_discards.begin(),
        progress.reported_discards.upper_bound(progress.last_sequence));
    if (tombstone) {
        ++result->tombstone_records;
    } else {
        ++result->data_records;
    }
    return Status::Ok();
}

Status TopicWriter::AppendRecordLocked(
    Record record, PendingAck ack, uint64_t now_ns,
    TopicWriterPumpResult* result, std::vector<TopicWriterAck>* callbacks) {
    while (true) {
        Status status = EnsureSegmentLocked(record.header.ingestion_sequence,
                                            now_ns);
        if (!status.ok()) return status;
        Result<SegmentAppendResult> appended =
            segment_writer_->Append(record, now_ns);
        if (!appended.ok()) return appended.status();
        if (appended->records_accepted == 1) {
            ack.segment_id = active_segment_->segment_id;
            active_acks_.push_back(std::move(ack));
            if (appended->rotate_needed) {
                return SealCurrentLocked(now_ns, result, callbacks);
            }
            return Status::Ok();
        }
        if (!appended->rotate_needed) {
            return Status::Error(StatusCode::kInternal,
                                 "segment accepted no record without rotation");
        }
        status = SealCurrentLocked(now_ns, result, callbacks);
        if (!status.ok()) return status;
    }
}

Status TopicWriter::EnsureSegmentLocked(uint64_t first_sequence,
                                        uint64_t now_ns) {
    if (segment_writer_ != nullptr) return Status::Ok();
    if (next_segment_id_ == 0) {
        return Exhausted("segment ID is exhausted");
    }

    std::error_code filesystem_error;
    std::filesystem::create_directories(options_.partition_root / "segments",
                                        filesystem_error);
    if (filesystem_error) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot create partition segments directory");
    }

    const uint64_t segment_id = next_segment_id_;
    const std::filesystem::path relative_path =
        SegmentRelativePath(segment_id);
    SegmentManifestEntry entry{
        .segment_id = segment_id,
        .state = SegmentPersistentState::kCreating,
        .first_ingestion_sequence = first_sequence,
        .last_ingestion_sequence = first_sequence,
        .created_at_ns = now_ns,
        .sealed_at_ns = 0,
        .size_bytes = kEncodedSegmentHeaderSize,
        .relative_path = relative_path,
    };
    Status status = partition_manifest_->AddSegment(entry);
    if (!status.ok()) return status;

    SegmentHeader header;
    header.recording_id = options_.recording_id;
    header.topic_id = options_.topic_id.value;
    header.partition_id = options_.partition_id;
    header.writer_id = options_.writer_id;
    header.first_ingestion_sequence = first_sequence;
    header.created_at_ns = now_ns;
    Result<std::unique_ptr<SegmentWriter>> created = SegmentWriter::Create(
        options_.partition_root / relative_path, header, now_ns,
        options_.segment_options);
    if (!created.ok()) return created.status();
    // fdatasync on the Segment makes file contents durable but does not persist
    // the new directory entry. No durable checkpoint or ACK may cross this
    // boundary until the segments directory itself is synced.
    status = SyncSegmentsDirectoryLocked();
    if (!status.ok()) return status;

    entry.state = SegmentPersistentState::kOpen;
    status = partition_manifest_->UpdateSegment(entry);
    if (!status.ok()) return status;

    segment_writer_ = std::move(*created);
    active_segment_ = entry;
    active_acks_.clear();
    written_ack_count_ = 0;
    durable_ack_count_ = 0;
    next_segment_id_ =
        segment_id == std::numeric_limits<uint64_t>::max() ? 0
                                                           : segment_id + 1;
    return Status::Ok();
}

Status TopicWriter::SyncSegmentsDirectoryLocked() {
    const std::filesystem::path directory =
        options_.partition_root / "segments";
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const int fd = ::open(directory.c_str(), flags);
    if (fd < 0) {
        return Status::Error(
            StatusCode::kUnavailable,
            "cannot open TopicWriter segments directory: " +
                std::string(std::strerror(errno)));
    }
    struct stat attributes {};
    if (::fstat(fd, &attributes) != 0 || !S_ISDIR(attributes.st_mode)) {
        const int saved_errno = errno;
        static_cast<void>(::close(fd));
        errno = saved_errno;
        return Status::Error(StatusCode::kUnavailable,
                             "TopicWriter segments path changed type");
    }
    int result = 0;
    do {
        result = options_.directory_sync_hook == nullptr
                     ? ::fsync(fd)
                     : options_.directory_sync_hook(
                           fd, options_.directory_sync_context);
    } while (result != 0 && errno == EINTR);
    const int saved_errno = errno;
    static_cast<void>(::close(fd));
    errno = saved_errno;
    if (result != 0) {
        return Status::Error(
            StatusCode::kUnavailable,
            "cannot fsync TopicWriter segments directory: " +
                std::string(std::strerror(errno)));
    }
    return Status::Ok();
}

Status TopicWriter::FlushCurrentLocked(
    uint64_t now_ns, std::vector<TopicWriterAck>* callbacks) {
    if (segment_writer_ == nullptr) return Status::Ok();
    Status status = segment_writer_->Flush(now_ns);
    if (!status.ok()) return status;

    while (written_ack_count_ < active_acks_.size()) {
        const PendingAck& ack = active_acks_[written_ack_count_++];
        callbacks->push_back(TopicWriterAck{
            .level = RecordAckLevel::kWritten,
            .ingestion_sequence = ack.ingestion_sequence,
            .segment_id = ack.segment_id,
            .record_flags = ack.record_flags,
            .user_tag = ack.user_tag,
            .source = ack.source,
        });
    }
    return PublishDurabilityLocked(callbacks);
}

Status TopicWriter::SealCurrentLocked(
    uint64_t now_ns, TopicWriterPumpResult* result,
    std::vector<TopicWriterAck>* callbacks) {
    if (segment_writer_ == nullptr) return Status::Ok();
    Status status = segment_writer_->Seal(now_ns);
    if (!status.ok()) return status;
    if (!active_segment_.has_value() || active_acks_.empty()) {
        return Status::Error(StatusCode::kInternal,
                             "active segment has no tracked record");
    }

    active_segment_->state = SegmentPersistentState::kSealed;
    active_segment_->last_ingestion_sequence =
        active_acks_.back().ingestion_sequence;
    active_segment_->sealed_at_ns = now_ns;
    active_segment_->size_bytes = segment_writer_->size_bytes();
    status = partition_manifest_->UpdateSegment(*active_segment_);
    if (!status.ok()) return status;

    while (written_ack_count_ < active_acks_.size()) {
        const PendingAck& ack = active_acks_[written_ack_count_++];
        callbacks->push_back(TopicWriterAck{
            .level = RecordAckLevel::kWritten,
            .ingestion_sequence = ack.ingestion_sequence,
            .segment_id = ack.segment_id,
            .record_flags = ack.record_flags,
            .user_tag = ack.user_tag,
            .source = ack.source,
        });
    }
    status = PublishDurabilityLocked(callbacks);
    if (!status.ok()) return status;

    segment_writer_.reset();
    active_segment_.reset();
    active_acks_.clear();
    written_ack_count_ = 0;
    durable_ack_count_ = 0;
    if (result != nullptr) ++result->rotations;
    return Status::Ok();
}

Status TopicWriter::UpdateOpenManifestLocked() {
    if (segment_writer_ == nullptr || !active_segment_.has_value() ||
        active_acks_.empty()) {
        return Status::Ok();
    }
    active_segment_->last_ingestion_sequence =
        active_acks_.back().ingestion_sequence;
    active_segment_->size_bytes = segment_writer_->size_bytes();
    return partition_manifest_->UpdateSegment(*active_segment_);
}

Status TopicWriter::PublishDurabilityLocked(
    std::vector<TopicWriterAck>* callbacks) {
    if (segment_writer_ == nullptr || !active_segment_.has_value()) {
        return Status::Ok();
    }
    const uint64_t durable_records = segment_writer_->durable_records();
    if (durable_records > active_acks_.size()) {
        return Status::Error(StatusCode::kInternal,
                             "segment durable count exceeds ACK ledger");
    }
    const size_t durable_count = static_cast<size_t>(durable_records);
    if (durable_count <= durable_ack_count_) return Status::Ok();

    Status status = UpdateOpenManifestLocked();
    if (!status.ok()) return status;
    const PendingAck& last_durable = active_acks_[durable_count - 1];
    status = partition_manifest_->UpdateCheckpoint(DurableCheckpoint{
        .segment_id = active_segment_->segment_id,
        .durable_offset = segment_writer_->durable_bytes(),
        .durable_sequence = last_durable.ingestion_sequence,
    });
    if (!status.ok()) return status;

    while (durable_ack_count_ < durable_count) {
        const PendingAck& ack = active_acks_[durable_ack_count_++];
        callbacks->push_back(TopicWriterAck{
            .level = RecordAckLevel::kDurable,
            .ingestion_sequence = ack.ingestion_sequence,
            .segment_id = ack.segment_id,
            .record_flags = ack.record_flags,
            .user_tag = ack.user_tag,
            .source = ack.source,
        });
    }
    return Status::Ok();
}

Status TopicWriter::ReportDiscardLocked(const DiscardedBuffer& discarded) {
    if (state_ == TopicWriterState::kError) return error_status_;
    if (state_ == TopicWriterState::kStopped) {
        return Invalid("cannot report a discard to a stopped TopicWriter");
    }
    if (discarded.topic_id != options_.topic_id) {
        return Invalid("discarded buffer belongs to another topic");
    }
    if (!discarded.metadata.has_value()) {
        return Invalid("discarded buffer has no recorder metadata");
    }
    if (discarded.metadata->topic_id != options_.topic_id) {
        return Invalid("discarded metadata belongs to another topic");
    }
    try {
        SourceProgress& progress = sources_[KeyOf(discarded.metadata->source)];
        const uint64_t source_sequence =
            discarded.metadata->source.source_sequence;
        if (!progress.has_last_sequence ||
            source_sequence > progress.last_sequence) {
            progress.reported_discards.insert_or_assign(
                source_sequence, GapReasonFor(discarded.reason));
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::length_error&) {
        return AllocationFailure();
    }
}

Status TopicWriter::PoisonLocked(Status status) {
    if (state_ != TopicWriterState::kError) {
        error_status_ = std::move(status);
        state_ = TopicWriterState::kError;
    }
    return error_status_;
}

TopicWriter::SourceKey TopicWriter::KeyOf(
    const MessageSource& source) noexcept {
    return SourceKey{source.node_id, source.publisher_id,
                     source.publisher_epoch};
}

GapReason TopicWriter::GapReasonFor(BufferDiscardReason reason) noexcept {
    switch (reason) {
        case BufferDiscardReason::kDropNewest:
            return GapReason::kDropNewest;
        case BufferDiscardReason::kDropOldest:
            return GapReason::kDropOldest;
        case BufferDiscardReason::kDropNewestNoDroppableOldest:
            return GapReason::kDropNewestNoDroppableOldest;
        case BufferDiscardReason::kAllocationFailure:
            return GapReason::kAllocationFailure;
        case BufferDiscardReason::kFailRecording:
            return GapReason::kRecordingFailed;
        case BufferDiscardReason::kReservationCancelled:
            return GapReason::kReservationCancelled;
    }
    return GapReason::kSourceSequenceJump;
}

GapReason TopicWriter::SelectGapReason(const SourceProgress& progress,
                                       uint64_t first_missing,
                                       uint64_t last_missing) noexcept {
    auto found = progress.reported_discards.lower_bound(first_missing);
    if (found == progress.reported_discards.end() ||
        found->first != first_missing) {
        return GapReason::kSourceSequenceJump;
    }
    const GapReason reason = found->second;
    uint64_t expected = first_missing;
    while (found != progress.reported_discards.end() &&
           found->first <= last_missing) {
        if (found->first != expected || found->second != reason) {
            return GapReason::kSourceSequenceJump;
        }
        if (expected == last_missing) return reason;
        ++expected;
        ++found;
    }
    return GapReason::kSourceSequenceJump;
}

void TopicWriter::DispatchCallbacks(
    TopicWriterAckCallback callback, void* context,
    std::span<const TopicWriterAck> callbacks) noexcept {
    if (callback == nullptr) return;
    for (const TopicWriterAck& ack : callbacks) callback(ack, context);
}

}  // namespace mino::storage
