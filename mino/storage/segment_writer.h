// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_SEGMENT_WRITER_H_
#define MINO_STORAGE_SEGMENT_WRITER_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <vector>

#include "mino/common/result.h"
#include "mino/storage/segment_format.h"

namespace mino::storage {

enum class SegmentWriterState : uint8_t {
    kOpen,
    kSealed,
    kError,
};

enum class SegmentSyncPolicy : uint8_t {
    kNone,
    kInterval,
    kPerBatch,
    kPerRecord,
};

// Hooks use POSIX write/fdatasync return conventions and errno. A null hook
// selects the real system call. They are intentionally narrow so short writes,
// EINTR, ENOSPC, EIO, and sync ordering can be tested deterministically.
using SegmentWriteHook = std::ptrdiff_t (*)(int fd, const std::byte* data,
                                            size_t size,
                                            void* context) noexcept;
using SegmentDataSyncHook = int (*)(int fd, void* context) noexcept;

struct SegmentWriterOptions {
    // Zero disables the corresponding batching threshold. Explicit Flush(), a
    // rotation boundary, and Seal() always drain a pending batch.
    uint64_t batch_bytes = 1u << 20;
    uint64_t batch_records = 128;
    uint64_t flush_interval_ns = 10'000'000;

    SegmentSyncPolicy sync_policy = SegmentSyncPolicy::kNone;
    // For kInterval, a sync is due when either non-zero threshold is reached.
    uint64_t sync_interval_ns = 1'000'000'000;
    uint64_t sync_interval_bytes = 0;

    // Zero disables the corresponding rotation threshold. max_segment_bytes
    // includes the encoded SegmentHeader. A record that would cross a limit is
    // not accepted into this segment.
    uint64_t max_segment_bytes = 0;
    uint64_t max_segment_records = 0;
    uint64_t max_segment_duration_ns = 0;

    SegmentFormatLimits format_limits{};
    SegmentWriteHook write_hook = nullptr;
    SegmentDataSyncHook data_sync_hook = nullptr;
    void* io_hook_context = nullptr;
};

struct SegmentAppendResult {
    // AppendBatch may stop safely at a rotation boundary. The caller resumes at
    // this index with a newly created SegmentWriter.
    size_t records_accepted = 0;
    bool rotate_needed = false;
};

// Creates and exclusively owns one new append-only segment file. This class is
// deliberately single-owner and not thread-safe. Creation writes exactly one
// canonical SegmentHeader; successful append operations write only bytes from
// EncodeRecord(). I/O or synchronization failures permanently poison the
// writer into kError. Recovery requires constructing a new writer explicitly.
class SegmentWriter final {
public:
    static Result<std::unique_ptr<SegmentWriter>> Create(
        const std::filesystem::path& path, const SegmentHeader& header,
        uint64_t opened_at_ns,
        const SegmentWriterOptions& options = {});

    ~SegmentWriter();

    SegmentWriter(const SegmentWriter&) = delete;
    SegmentWriter& operator=(const SegmentWriter&) = delete;
    SegmentWriter(SegmentWriter&&) = delete;
    SegmentWriter& operator=(SegmentWriter&&) = delete;

    Result<SegmentAppendResult> Append(const Record& record, uint64_t now_ns);
    Result<SegmentAppendResult> AppendBatch(std::span<const Record> records,
                                            uint64_t now_ns);

    // Drains the current batch and applies the configured synchronization
    // policy. For kNone this does not sync; Seal() always does.
    Status Flush(uint64_t now_ns);

    // Drains, fdatasyncs, and transitions OPEN -> SEALED. Repeated calls after
    // a successful seal are idempotent and do not issue another sync.
    Status Seal(uint64_t now_ns);

    SegmentWriterState state() const noexcept { return state_; }
    const Status& error_status() const noexcept { return error_status_; }
    const std::filesystem::path& path() const noexcept { return path_; }
    uint64_t size_bytes() const noexcept { return logical_bytes_; }
    uint64_t record_count() const noexcept { return logical_records_; }
    uint64_t durable_bytes() const noexcept { return durable_bytes_; }
    uint64_t durable_records() const noexcept { return durable_records_; }
    bool rotation_needed(uint64_t now_ns) const noexcept;

private:
    SegmentWriter(std::filesystem::path path, int fd, uint64_t opened_at_ns,
                  uint32_t topic_id, uint32_t partition_id,
                  uint64_t first_ingestion_sequence,
                  SegmentWriterOptions options) noexcept;

    Status ValidateOpenAndTime(uint64_t now_ns);
    Status FlushPending(uint64_t now_ns);
    Status WriteAll(std::span<const std::byte> bytes);
    Status DataSync(uint64_t now_ns);
    Status Poison(Status status);
    bool WouldRotate(size_t encoded_record_size, uint64_t now_ns) const noexcept;
    bool BatchDue(uint64_t now_ns) const noexcept;
    bool IntervalSyncDue(uint64_t now_ns) const noexcept;

    std::filesystem::path path_;
    int fd_ = -1;
    SegmentWriterOptions options_;
    SegmentWriterState state_ = SegmentWriterState::kOpen;
    Status error_status_ = Status::Ok();

    uint64_t opened_at_ns_ = 0;
    uint32_t topic_id_ = 0;
    uint32_t partition_id_ = 0;
    uint64_t first_ingestion_sequence_ = 0;
    uint64_t last_observed_at_ns_ = 0;
    uint64_t last_flush_at_ns_ = 0;
    uint64_t last_sync_at_ns_ = 0;

    uint64_t logical_bytes_ = kEncodedSegmentHeaderSize;
    uint64_t logical_records_ = 0;
    uint64_t written_bytes_ = kEncodedSegmentHeaderSize;
    uint64_t written_records_ = 0;
    uint64_t durable_bytes_ = 0;
    uint64_t durable_records_ = 0;
    uint64_t unsynced_bytes_ = kEncodedSegmentHeaderSize;

    bool has_last_sequence_ = false;
    uint64_t last_sequence_ = 0;
    uint64_t pending_bytes_ = 0;
    std::vector<std::vector<std::byte>> pending_records_;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_SEGMENT_WRITER_H_
