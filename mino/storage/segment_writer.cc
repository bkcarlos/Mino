// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/segment_writer.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <string>
#include <string_view>
#include <utility>

#include "mino/common/status.h"

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status IoError(std::string_view operation,
               const std::filesystem::path& path) {
    const int error = errno;
    StatusCode code = StatusCode::kUnavailable;
    if (error == EACCES || error == EPERM || error == EROFS) {
        code = StatusCode::kPermissionDenied;
    } else if (error == ENOSPC || error == EDQUOT) {
        code = StatusCode::kResourceExhausted;
    } else if (error == EEXIST) {
        code = StatusCode::kAlreadyExists;
    }
    return Status::Error(code, std::string(operation) + " '" + path.string() +
                                   "': " + std::strerror(error));
}

Status ValidateOptions(const SegmentWriterOptions& options) {
    if (options.sync_policy == SegmentSyncPolicy::kInterval &&
        options.sync_interval_ns == 0 && options.sync_interval_bytes == 0) {
        return Invalid("interval sync policy requires a time or byte threshold");
    }
    if (options.max_segment_bytes != 0 &&
        options.max_segment_bytes <
            kEncodedSegmentHeaderSize + kMinimumEncodedRecordSize) {
        return Invalid("max_segment_bytes cannot hold one minimum record");
    }
    const Result<size_t> minimum_size =
        EncodedRecordSize(0, options.format_limits);
    if (!minimum_size.ok()) return minimum_size.status();
    return Status::Ok();
}

int OpenFlags() noexcept {
    int flags = O_WRONLY | O_CREAT | O_EXCL | O_APPEND;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    return flags;
}

Status SetCloseOnExec(int fd) {
#ifdef O_CLOEXEC
    static_cast<void>(fd);
    return Status::Ok();
#else
    const int flags = ::fcntl(fd, F_GETFD);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot set segment close-on-exec flag");
    }
    return Status::Ok();
#endif
}

bool ReachedElapsed(uint64_t now_ns, uint64_t start_ns,
                    uint64_t interval_ns) noexcept {
    return interval_ns != 0 && now_ns >= start_ns &&
           now_ns - start_ns >= interval_ns;
}

}  // namespace

Result<std::unique_ptr<SegmentWriter>> SegmentWriter::Create(
    const std::filesystem::path& path, const SegmentHeader& header,
    uint64_t opened_at_ns, const SegmentWriterOptions& options) {
    try {
        if (path.empty()) return Invalid("segment path is empty");
        const Status options_status = ValidateOptions(options);
        if (!options_status.ok()) return options_status;

        Result<std::vector<std::byte>> encoded_header =
            EncodeSegmentHeader(header);
        if (!encoded_header.ok()) return encoded_header.status();

        const int fd = ::open(path.c_str(), OpenFlags(), 0644);
        if (fd < 0) return IoError("cannot create segment", path);

        const Status close_on_exec = SetCloseOnExec(fd);
        if (!close_on_exec.ok()) {
            static_cast<void>(::close(fd));
            static_cast<void>(::unlink(path.c_str()));
            return close_on_exec;
        }

        std::unique_ptr<SegmentWriter> writer;
        try {
            writer.reset(new SegmentWriter(
                path, fd, opened_at_ns, header.topic_id, header.partition_id,
                header.first_ingestion_sequence, options));
        } catch (...) {
            static_cast<void>(::close(fd));
            static_cast<void>(::unlink(path.c_str()));
            throw;
        }

        try {
            const Status header_status = writer->WriteAll(*encoded_header);
            if (!header_status.ok()) {
                static_cast<void>(::unlink(path.c_str()));
                return header_status;
            }
        } catch (...) {
            static_cast<void>(::unlink(path.c_str()));
            throw;
        }
        return writer;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate segment writer state");
    } catch (const std::length_error&) {
        return Exhausted("segment writer allocation is too large");
    }
}

SegmentWriter::SegmentWriter(std::filesystem::path path, int fd,
                             uint64_t opened_at_ns, uint32_t topic_id,
                             uint32_t partition_id,
                             uint64_t first_ingestion_sequence,
                             SegmentWriterOptions options) noexcept
    : path_(std::move(path)),
      fd_(fd),
      options_(options),
      opened_at_ns_(opened_at_ns),
      topic_id_(topic_id),
      partition_id_(partition_id),
      first_ingestion_sequence_(first_ingestion_sequence),
      last_observed_at_ns_(opened_at_ns),
      last_flush_at_ns_(opened_at_ns),
      last_sync_at_ns_(opened_at_ns) {}

SegmentWriter::~SegmentWriter() {
    if (fd_ >= 0) static_cast<void>(::close(fd_));
}

Result<SegmentAppendResult> SegmentWriter::Append(const Record& record,
                                                  uint64_t now_ns) {
    return AppendBatch(std::span<const Record>(&record, 1), now_ns);
}

Result<SegmentAppendResult> SegmentWriter::AppendBatch(
    std::span<const Record> records, uint64_t now_ns) {
    const Status state_status = ValidateOpenAndTime(now_ns);
    if (!state_status.ok()) return state_status;
    if (records.empty()) return SegmentAppendResult{};

    try {
        std::vector<std::vector<std::byte>> encoded_records;
        encoded_records.reserve(records.size());
        uint64_t sequence = last_sequence_;
        bool has_sequence = has_last_sequence_;
        for (const Record& record : records) {
            if (record.header.topic_id != topic_id_ ||
                record.header.partition_id != partition_id_) {
                return Invalid("record topic or partition does not match segment");
            }
            if ((!has_sequence &&
                 record.header.ingestion_sequence != first_ingestion_sequence_) ||
                (has_sequence && record.header.ingestion_sequence <= sequence)) {
                return Invalid(
                    has_sequence
                        ? "record ingestion_sequence must be strictly increasing"
                        : "first record sequence does not match segment header");
            }
            Result<std::vector<std::byte>> encoded =
                EncodeRecord(record, options_.format_limits);
            if (!encoded.ok()) return encoded.status();
            encoded_records.push_back(std::move(*encoded));
            sequence = record.header.ingestion_sequence;
            has_sequence = true;
        }

        SegmentAppendResult result;
        for (size_t index = 0; index < encoded_records.size(); ++index) {
            std::vector<std::byte>& encoded = encoded_records[index];
            if (WouldRotate(encoded.size(), now_ns)) {
                const Status flush_status = FlushPending(now_ns);
                if (!flush_status.ok()) return flush_status;
                result.rotate_needed = true;
                return result;
            }

            const uint64_t encoded_size = static_cast<uint64_t>(encoded.size());
            pending_bytes_ += encoded_size;
            logical_bytes_ += encoded_size;
            ++logical_records_;
            pending_records_.push_back(std::move(encoded));
            has_last_sequence_ = true;
            last_sequence_ = records[index].header.ingestion_sequence;
            ++result.records_accepted;

            if (rotation_needed(now_ns)) {
                const Status flush_status = FlushPending(now_ns);
                if (!flush_status.ok()) return flush_status;
                result.rotate_needed = true;
                return result;
            }
            if (BatchDue(now_ns)) {
                const Status flush_status = FlushPending(now_ns);
                if (!flush_status.ok()) return flush_status;
            }
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot buffer encoded segment records");
    } catch (const std::length_error&) {
        return Exhausted("segment record batch is too large");
    }
}

Status SegmentWriter::Flush(uint64_t now_ns) {
    const Status state_status = ValidateOpenAndTime(now_ns);
    if (!state_status.ok()) {
        if (state_ == SegmentWriterState::kSealed) return Status::Ok();
        return state_status;
    }
    return FlushPending(now_ns);
}

Status SegmentWriter::Seal(uint64_t now_ns) {
    if (state_ == SegmentWriterState::kSealed) return Status::Ok();
    const Status state_status = ValidateOpenAndTime(now_ns);
    if (!state_status.ok()) return state_status;

    const Status flush_status = FlushPending(now_ns);
    if (!flush_status.ok()) return flush_status;
    const Status sync_status = DataSync(now_ns);
    if (!sync_status.ok()) return sync_status;
    state_ = SegmentWriterState::kSealed;
    return Status::Ok();
}

bool SegmentWriter::rotation_needed(uint64_t now_ns) const noexcept {
    if (state_ != SegmentWriterState::kOpen) return false;
    if (options_.max_segment_bytes != 0 &&
        logical_bytes_ >= options_.max_segment_bytes) {
        return true;
    }
    if (options_.max_segment_records != 0 &&
        logical_records_ >= options_.max_segment_records) {
        return true;
    }
    return ReachedElapsed(now_ns, opened_at_ns_,
                          options_.max_segment_duration_ns);
}

Status SegmentWriter::ValidateOpenAndTime(uint64_t now_ns) {
    if (state_ == SegmentWriterState::kError) return error_status_;
    if (state_ != SegmentWriterState::kOpen) {
        return Invalid("segment writer is sealed");
    }
    if (now_ns < last_observed_at_ns_) {
        return Invalid("segment writer time must be nondecreasing");
    }
    last_observed_at_ns_ = now_ns;
    return Status::Ok();
}

Status SegmentWriter::FlushPending(uint64_t now_ns) {
    const bool had_records = !pending_records_.empty();
    for (const std::vector<std::byte>& encoded : pending_records_) {
        const Status write_status = WriteAll(encoded);
        if (!write_status.ok()) return write_status;
        written_bytes_ += static_cast<uint64_t>(encoded.size());
        ++written_records_;
        unsynced_bytes_ += static_cast<uint64_t>(encoded.size());

        if (options_.sync_policy == SegmentSyncPolicy::kPerRecord) {
            const Status sync_status = DataSync(now_ns);
            if (!sync_status.ok()) return sync_status;
        }
    }
    pending_records_.clear();
    pending_bytes_ = 0;
    if (had_records) last_flush_at_ns_ = now_ns;

    if (options_.sync_policy == SegmentSyncPolicy::kPerBatch && had_records) {
        return DataSync(now_ns);
    }
    if (options_.sync_policy == SegmentSyncPolicy::kInterval &&
        IntervalSyncDue(now_ns)) {
        return DataSync(now_ns);
    }
    return Status::Ok();
}

Status SegmentWriter::WriteAll(std::span<const std::byte> bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const size_t remaining = bytes.size() - offset;
        const size_t max_request =
            static_cast<size_t>(std::numeric_limits<ssize_t>::max());
        const size_t request = remaining < max_request ? remaining : max_request;
        std::ptrdiff_t written = 0;
        if (options_.write_hook == nullptr) {
            const ssize_t result = ::write(fd_, bytes.data() + offset, request);
            written = static_cast<std::ptrdiff_t>(result);
        } else {
            written = options_.write_hook(fd_, bytes.data() + offset, request,
                                          options_.io_hook_context);
        }
        if (written < 0) {
            if (errno == EINTR) continue;
            return Poison(IoError("cannot append segment", path_));
        }
        if (written == 0) {
            return Poison(Status::Error(StatusCode::kUnavailable,
                                        "zero-byte segment write"));
        }
        const size_t count = static_cast<size_t>(written);
        if (count > request) {
            return Poison(Status::Error(StatusCode::kInternal,
                                        "segment write hook over-reported bytes"));
        }
        offset += count;
    }
    return Status::Ok();
}

Status SegmentWriter::DataSync(uint64_t now_ns) {
    while (true) {
        const int result = options_.data_sync_hook == nullptr
                               ? ::fdatasync(fd_)
                               : options_.data_sync_hook(
                                     fd_, options_.io_hook_context);
        if (result == 0) break;
        if (errno == EINTR) continue;
        return Poison(IoError("cannot fdatasync segment", path_));
    }
    durable_bytes_ = written_bytes_;
    durable_records_ = written_records_;
    unsynced_bytes_ = 0;
    last_sync_at_ns_ = now_ns;
    return Status::Ok();
}

Status SegmentWriter::Poison(Status status) {
    if (state_ != SegmentWriterState::kError) {
        error_status_ = std::move(status);
        state_ = SegmentWriterState::kError;
    }
    return error_status_;
}

bool SegmentWriter::WouldRotate(size_t encoded_record_size,
                                uint64_t now_ns) const noexcept {
    if (rotation_needed(now_ns) ||
        logical_records_ == std::numeric_limits<uint64_t>::max()) {
        return true;
    }
    const uint64_t size = static_cast<uint64_t>(encoded_record_size);
    if (logical_bytes_ > std::numeric_limits<uint64_t>::max() - size) {
        return true;
    }
    if (options_.max_segment_bytes != 0 &&
        logical_bytes_ + size > options_.max_segment_bytes) {
        return true;
    }
    return options_.max_segment_records != 0 &&
           logical_records_ >= options_.max_segment_records;
}

bool SegmentWriter::BatchDue(uint64_t now_ns) const noexcept {
    if (options_.batch_bytes != 0 &&
        pending_bytes_ >= options_.batch_bytes) {
        return true;
    }
    if (options_.batch_records != 0 &&
        pending_records_.size() >= options_.batch_records) {
        return true;
    }
    return ReachedElapsed(now_ns, last_flush_at_ns_,
                          options_.flush_interval_ns);
}

bool SegmentWriter::IntervalSyncDue(uint64_t now_ns) const noexcept {
    if (unsynced_bytes_ == 0) return false;
    if (options_.sync_interval_bytes != 0 &&
        unsynced_bytes_ >= options_.sync_interval_bytes) {
        return true;
    }
    return ReachedElapsed(now_ns, last_sync_at_ns_,
                          options_.sync_interval_ns);
}

}  // namespace mino::storage
