// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/segment_recovery.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "mino/bridge/crc32c.h"
#include "mino/common/status.h"

namespace mino::storage {
namespace {

constexpr size_t kIoBufferSize = 64u * 1024u;

constexpr size_t kRecordHeaderOffset = 12;
constexpr size_t kRecordMagicOffset = 0;
constexpr size_t kRecordVersionOffset = 4;
constexpr size_t kRecordFlagsOffset = 6;
constexpr size_t kRecordSchemaRefOffset = 8;
constexpr size_t kRecordSchemaVersionOffset = 12;
constexpr size_t kRecordLayoutVersionOffset = 16;
constexpr size_t kRecordTopicIdOffset = 20;
constexpr size_t kRecordPartitionIdOffset = 24;
constexpr size_t kRecordIngestionSequenceOffset = 28;
constexpr size_t kRecordIngestionTimestampOffset = 36;
constexpr size_t kRecordSourceSequenceOffset = 68;
constexpr size_t kRecordObservedTimestampOffset = 76;
constexpr size_t kRecordPayloadSizeOffset = 84;
constexpr size_t kRecordPayloadCrcOffset = 88;
constexpr size_t kRecordHeaderCrcOffset = 92;
constexpr size_t kRecordReservedOffset = 96;
constexpr size_t kTrailerLengthOffset = 0;
constexpr size_t kTrailerSequenceOffset = 8;
constexpr size_t kTrailerRecordCrcOffset = 16;
constexpr size_t kTrailerCommitMarkerOffset = 20;

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
    } else if (error == ENOENT) {
        code = StatusCode::kNotFound;
    }
    return Status::Error(code, std::string(operation) + " '" + path.string() +
                                   "': " + std::strerror(error));
}

uint16_t ReadLe16(std::span<const std::byte> input, size_t offset) noexcept {
    uint16_t value = 0;
    for (size_t i = 0; i < 2; ++i) {
        value |= static_cast<uint16_t>(
            static_cast<uint16_t>(static_cast<uint8_t>(input[offset + i]))
            << (8 * i));
    }
    return value;
}

uint32_t ReadLe32(std::span<const std::byte> input, size_t offset) noexcept {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value |= static_cast<uint32_t>(
            static_cast<uint32_t>(static_cast<uint8_t>(input[offset + i]))
            << (8 * i));
    }
    return value;
}

uint64_t ReadLe64(std::span<const std::byte> input, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value |= static_cast<uint64_t>(
            static_cast<uint64_t>(static_cast<uint8_t>(input[offset + i]))
            << (8 * i));
    }
    return value;
}

size_t PaddingSize(uint32_t payload_size) noexcept {
    const uint64_t unaligned =
        kRecordLengthFieldSize + kRecordHeaderLengthFieldSize +
        kEncodedRecordHeaderSize + static_cast<uint64_t>(payload_size) +
        kRecordTrailerSize;
    return static_cast<size_t>(
        (kRecordAlignment - (unaligned % kRecordAlignment)) % kRecordAlignment);
}

class ScopedFd final {
public:
    explicit ScopedFd(int fd) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) static_cast<void>(::close(fd_));
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    int get() const noexcept { return fd_; }

private:
    int fd_;
};

int OpenFlags(bool writable) noexcept {
    int flags = writable ? O_RDWR : O_RDONLY;
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

Result<uint64_t> FileSize(int fd, const std::filesystem::path& path) {
    struct stat attributes {};
    if (::fstat(fd, &attributes) != 0) {
        return IoError("cannot stat segment", path);
    }
    if (attributes.st_size < 0) {
        return Status::Error(StatusCode::kCorruption,
                             "segment has a negative file size");
    }
    return static_cast<uint64_t>(attributes.st_size);
}

class SegmentReader final {
public:
    SegmentReader(int fd, const std::filesystem::path& path,
                  const SegmentRecoveryOptions& options) noexcept
        : fd_(fd), path_(path), options_(options) {}

    // Returns false only for EOF before the requested range was filled.
    Result<bool> ReadExact(uint64_t offset, std::span<std::byte> output) const {
        size_t completed = 0;
        while (completed < output.size()) {
            const size_t remaining = output.size() - completed;
            const size_t max_request =
                static_cast<size_t>(std::numeric_limits<ssize_t>::max());
            const size_t request = std::min(remaining, max_request);
            if (offset > std::numeric_limits<uint64_t>::max() - completed) {
                return Status::Error(StatusCode::kCorruption,
                                     "segment read offset overflows uint64");
            }
            const uint64_t current_offset = offset + completed;

            std::ptrdiff_t count = 0;
            if (options_.pread_hook != nullptr) {
                count = options_.pread_hook(fd_, output.data() + completed,
                                            request, current_offset,
                                            options_.io_hook_context);
            } else {
                if (current_offset >
                    static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
                    return Status::Error(StatusCode::kCorruption,
                                         "segment offset does not fit off_t");
                }
                const ssize_t result =
                    ::pread(fd_, output.data() + completed, request,
                            static_cast<off_t>(current_offset));
                count = static_cast<std::ptrdiff_t>(result);
            }

            if (count < 0) {
                if (errno == EINTR) continue;
                return IoError("cannot read segment", path_);
            }
            if (count == 0) return false;
            const size_t read_size = static_cast<size_t>(count);
            if (read_size > request) {
                return Status::Error(
                    StatusCode::kInternal,
                    "segment pread hook over-reported returned bytes");
            }
            completed += read_size;
        }
        return true;
    }

private:
    int fd_;
    const std::filesystem::path& path_;
    const SegmentRecoveryOptions& options_;
};

struct RecordCheck {
    bool valid = false;
    bool has_complete_commit_marker = false;
    SegmentRecoveryReason reason = SegmentRecoveryReason::kNone;
    std::string detail;
    SegmentRecordOffset metadata;
};

RecordCheck Failure(RecordCheck check, SegmentRecoveryReason reason,
                    std::string_view detail) {
    check.reason = reason;
    check.detail = detail;
    return check;
}

bool SchemaRefKnown(uint32_t schema_ref,
                    const SegmentRecoveryOptions& options) noexcept {
    if (options.known_schema_refs == nullptr &&
        options.schema_ref_validator == nullptr) {
        return true;
    }
    if (options.known_schema_refs != nullptr &&
        options.known_schema_refs->contains(schema_ref)) {
        return true;
    }
    return options.schema_ref_validator != nullptr &&
           options.schema_ref_validator(schema_ref,
                                        options.schema_ref_context);
}

Result<RecordCheck> ValidateRecord(
    const SegmentReader& reader, uint64_t file_size, uint64_t record_offset,
    const SegmentHeader& segment_header,
    const SegmentRecoveryOptions& options, bool validate_context,
    std::optional<uint64_t> previous_sequence, bool require_first_sequence) {
    RecordCheck check;
    check.metadata.record_offset = record_offset;

    if (record_offset > file_size ||
        file_size - record_offset < kRecordLengthFieldSize) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kIncompleteLeadingLength,
                       "record leading length is incomplete");
    }

    std::array<std::byte, kRecordLengthFieldSize> leading{};
    Result<bool> leading_read = reader.ReadExact(record_offset, leading);
    if (!leading_read.ok()) return leading_read.status();
    if (!*leading_read) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kIncompleteLeadingLength,
                       "record leading length ended at EOF");
    }

    const uint64_t record_length = ReadLe64(leading, 0);
    if (record_length >
        std::numeric_limits<uint64_t>::max() - kRecordLengthFieldSize) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kRecordLengthOverflow,
                       "record_length overflows uint64");
    }
    const uint64_t encoded_size = record_length + kRecordLengthFieldSize;
    check.metadata.encoded_size = encoded_size;
    if (encoded_size > file_size - record_offset) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kIncompleteRecord,
                       "record envelope extends past EOF");
    }
    check.metadata.record_end_offset = record_offset + encoded_size;
    if (encoded_size < kMinimumEncodedRecordSize ||
        encoded_size > options.format_limits.max_encoded_record_size) {
        if (encoded_size >= sizeof(uint64_t)) {
            std::array<std::byte, sizeof(uint64_t)> marker{};
            Result<bool> marker_read = reader.ReadExact(
                check.metadata.record_end_offset - marker.size(), marker);
            if (!marker_read.ok()) return marker_read.status();
            check.has_complete_commit_marker =
                *marker_read && ReadLe64(marker, 0) == kRecordCommitMarker;
        }
        if (encoded_size < kMinimumEncodedRecordSize) {
            return Failure(std::move(check),
                           SegmentRecoveryReason::kRecordTooSmall,
                           "record envelope is smaller than the v1 minimum");
        }
        return Failure(std::move(check), SegmentRecoveryReason::kRecordTooLarge,
                       "record envelope exceeds max_encoded_record_size");
    }

    std::array<std::byte, kRecordHeaderLengthFieldSize +
                              kEncodedRecordHeaderSize>
        header_area{};
    Result<bool> header_read = reader.ReadExact(
        record_offset + kRecordLengthFieldSize, header_area);
    if (!header_read.ok()) return header_read.status();
    if (!*header_read) {
        return Failure(std::move(check), SegmentRecoveryReason::kIncompleteRecord,
                       "record header ended at EOF");
    }
    const std::span<const std::byte> header =
        std::span<const std::byte>(header_area)
            .subspan(kRecordHeaderLengthFieldSize);

    const uint64_t trailer_offset =
        record_offset + encoded_size - kRecordTrailerSize;
    std::array<std::byte, kRecordTrailerSize> trailer{};
    Result<bool> trailer_read = reader.ReadExact(trailer_offset, trailer);
    if (!trailer_read.ok()) return trailer_read.status();
    if (!*trailer_read) {
        return Failure(std::move(check), SegmentRecoveryReason::kIncompleteRecord,
                       "record trailer ended at EOF");
    }
    check.has_complete_commit_marker =
        ReadLe64(trailer, kTrailerCommitMarkerOffset) == kRecordCommitMarker;

    if (ReadLe32(header_area, 0) != kEncodedRecordHeaderSize) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kHeaderLengthMismatch,
                       "noncanonical record header_length");
    }
    if (ReadLe32(header, kRecordMagicOffset) != kRecordHeaderMagic ||
        ReadLe16(header, kRecordVersionOffset) != kRecordFormatVersion ||
        (ReadLe16(header, kRecordFlagsOffset) & ~kKnownRecordFlags) != 0 ||
        ReadLe32(header, kRecordReservedOffset) != 0) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kRecordHeaderCorruption,
                       "record header magic, version, flags, or reserved field is invalid");
    }

    bridge::Crc32cAccumulator header_crc;
    header_crc.Update(header.first(kRecordHeaderCrcOffset));
    header_crc.Update(header.subspan(kRecordHeaderCrcOffset + 4));
    if (ReadLe32(header, kRecordHeaderCrcOffset) != header_crc.Finish()) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kHeaderCrcMismatch,
                       "record header CRC32C mismatch");
    }

    const uint32_t payload_size =
        ReadLe32(header, kRecordPayloadSizeOffset);
    check.metadata.payload_size = payload_size;
    if (payload_size > options.format_limits.max_payload_size) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kPayloadSizeMismatch,
                       "payload_size exceeds max_payload_size");
    }
    const uint64_t canonical_size =
        kRecordLengthFieldSize + kRecordHeaderLengthFieldSize +
        kEncodedRecordHeaderSize + static_cast<uint64_t>(payload_size) +
        PaddingSize(payload_size) + kRecordTrailerSize;
    if (canonical_size != encoded_size) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kPayloadSizeMismatch,
                       "payload_size does not match record_length");
    }

    if (ReadLe64(trailer, kTrailerLengthOffset) != record_length) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kTrailingLengthMismatch,
                       "leading and trailing record_length mismatch");
    }
    const uint64_t ingestion_sequence =
        ReadLe64(header, kRecordIngestionSequenceOffset);
    if (ReadLe64(trailer, kTrailerSequenceOffset) != ingestion_sequence) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kTrailerSequenceMismatch,
                       "trailer ingestion_sequence mismatch");
    }
    if (!check.has_complete_commit_marker) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kCommitMarkerMismatch,
                       "record commit marker mismatch");
    }

    const uint64_t payload_relative_offset =
        kRecordHeaderOffset + kEncodedRecordHeaderSize;
    const uint64_t payload_end =
        payload_relative_offset + static_cast<uint64_t>(payload_size);
    const uint64_t record_crc_size = encoded_size - kRecordTrailerSize;
    check.metadata.payload_offset = record_offset + payload_relative_offset;

    bridge::Crc32cAccumulator record_crc;
    bridge::Crc32cAccumulator payload_crc;
    bool padding_is_zero = true;
    std::array<std::byte, kIoBufferSize> buffer{};
    uint64_t relative_offset = 0;
    while (relative_offset < record_crc_size) {
        const uint64_t remaining = record_crc_size - relative_offset;
        const size_t chunk_size = static_cast<size_t>(
            std::min<uint64_t>(remaining, buffer.size()));
        std::span<std::byte> chunk(buffer.data(), chunk_size);
        Result<bool> chunk_read =
            reader.ReadExact(record_offset + relative_offset, chunk);
        if (!chunk_read.ok()) return chunk_read.status();
        if (!*chunk_read) {
            return Failure(std::move(check),
                           SegmentRecoveryReason::kIncompleteRecord,
                           "record body ended at EOF");
        }
        record_crc.Update(chunk);

        const uint64_t chunk_end = relative_offset + chunk_size;
        const uint64_t payload_part_begin =
            std::max(relative_offset, payload_relative_offset);
        const uint64_t payload_part_end = std::min(chunk_end, payload_end);
        if (payload_part_begin < payload_part_end) {
            payload_crc.Update(chunk.subspan(
                static_cast<size_t>(payload_part_begin - relative_offset),
                static_cast<size_t>(payload_part_end - payload_part_begin)));
        }

        const uint64_t padding_begin = std::max(relative_offset, payload_end);
        const uint64_t padding_end = std::min(chunk_end, record_crc_size);
        if (padding_begin < padding_end) {
            const std::span<const std::byte> padding = chunk.subspan(
                static_cast<size_t>(padding_begin - relative_offset),
                static_cast<size_t>(padding_end - padding_begin));
            padding_is_zero =
                padding_is_zero &&
                std::all_of(padding.begin(), padding.end(), [](std::byte byte) {
                    return byte == std::byte{0};
                });
        }
        relative_offset = chunk_end;
    }

    if (!padding_is_zero) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kPaddingCorruption,
                       "record alignment padding is non-zero");
    }
    if (ReadLe32(header, kRecordPayloadCrcOffset) != payload_crc.Finish()) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kPayloadCrcMismatch,
                       "record payload CRC32C mismatch");
    }
    if (ReadLe32(trailer, kTrailerRecordCrcOffset) != record_crc.Finish()) {
        return Failure(std::move(check),
                       SegmentRecoveryReason::kRecordCrcMismatch,
                       "record envelope CRC32C mismatch");
    }

    check.metadata.flags = ReadLe16(header, kRecordFlagsOffset);
    check.metadata.schema_ref = ReadLe32(header, kRecordSchemaRefOffset);
    check.metadata.schema_version =
        ReadLe32(header, kRecordSchemaVersionOffset);
    check.metadata.layout_version =
        ReadLe32(header, kRecordLayoutVersionOffset);
    check.metadata.ingestion_sequence = ingestion_sequence;
    check.metadata.ingestion_timestamp_ns =
        ReadLe64(header, kRecordIngestionTimestampOffset);
    check.metadata.source_sequence =
        ReadLe64(header, kRecordSourceSequenceOffset);
    check.metadata.observed_timestamp_ns =
        ReadLe64(header, kRecordObservedTimestampOffset);

    if (validate_context) {
        if (ReadLe32(header, kRecordTopicIdOffset) != segment_header.topic_id ||
            ReadLe32(header, kRecordPartitionIdOffset) !=
                segment_header.partition_id) {
            return Failure(std::move(check),
                           SegmentRecoveryReason::kTopicPartitionMismatch,
                           "record topic or partition does not match segment");
        }
        const bool sequence_does_not_follow =
            previous_sequence.has_value() &&
            (*previous_sequence == std::numeric_limits<uint64_t>::max() ||
             ingestion_sequence != *previous_sequence + 1);
        if ((require_first_sequence &&
             ingestion_sequence !=
                 segment_header.first_ingestion_sequence) ||
            sequence_does_not_follow) {
            return Failure(std::move(check),
                           SegmentRecoveryReason::kIngestionSequenceMismatch,
                           "record ingestion_sequence is not contiguous");
        }
        const bool schema_less_gap =
            (check.metadata.flags & kRecordFlagGap) != 0 &&
            check.metadata.schema_ref == 0;
        if (!schema_less_gap &&
            !SchemaRefKnown(check.metadata.schema_ref, options)) {
            return Failure(std::move(check),
                           SegmentRecoveryReason::kUnknownSchemaRef,
                           "record references an unknown session schema_ref");
        }
    }

    check.valid = true;
    return check;
}

Result<bool> HasLaterCommittedRecord(
    const SegmentReader& reader, uint64_t file_size, uint64_t damage_offset,
    const SegmentHeader& segment_header,
    const SegmentRecoveryOptions& options) {
    if (file_size < kMinimumEncodedRecordSize ||
        damage_offset > std::numeric_limits<uint64_t>::max() -
                            kRecordAlignment) {
        return false;
    }
    uint64_t candidate = damage_offset + kRecordAlignment;
    const uint64_t last_candidate = file_size - kMinimumEncodedRecordSize;
    while (candidate <= last_candidate) {
        Result<RecordCheck> checked = ValidateRecord(
            reader, file_size, candidate, segment_header, options, false,
            std::nullopt, false);
        if (!checked.ok()) return checked.status();
        if (checked->valid) return true;
        if (candidate > std::numeric_limits<uint64_t>::max() -
                            kRecordAlignment) {
            break;
        }
        candidate += kRecordAlignment;
    }
    return false;
}

void SetFinding(SegmentRecoveryReport* report, const RecordCheck& check,
                bool committed_or_interior) {
    report->reason = check.reason;
    report->reason_detail = check.detail;
    report->damage_offset = check.metadata.record_offset;
    if (committed_or_interior) {
        report->disposition = SegmentRecoveryDisposition::kCorruption;
        report->truncated_bytes = 0;
    } else {
        report->disposition = SegmentRecoveryDisposition::kIncompleteTail;
        report->truncated_bytes =
            report->file_size - report->last_complete_offset;
    }
}

Result<bool> ApplyCheckpoint(const SegmentReader& reader,
                             const SegmentRecoveryOptions& options,
                             SegmentRecoveryReport* report) {
    if (!options.checkpoint.has_value()) return false;
    const SegmentRecoveryCheckpoint& checkpoint = *options.checkpoint;
    if (checkpoint.offset == kEncodedSegmentHeaderSize) {
        if (checkpoint.ingestion_sequence.has_value()) return false;
        report->checkpoint_used = true;
        report->metadata_is_complete = true;
        return true;
    }
    if (!checkpoint.ingestion_sequence.has_value() ||
        checkpoint.offset > report->file_size ||
        checkpoint.offset < kEncodedSegmentHeaderSize +
                                kMinimumEncodedRecordSize) {
        return false;
    }

    const uint64_t trailer_offset = checkpoint.offset - kRecordTrailerSize;
    std::array<std::byte, kRecordLengthFieldSize> trailing_length{};
    Result<bool> length_read =
        reader.ReadExact(trailer_offset, trailing_length);
    if (!length_read.ok()) return length_read.status();
    if (!*length_read) return false;
    const uint64_t record_length = ReadLe64(trailing_length, 0);
    if (record_length >
        std::numeric_limits<uint64_t>::max() - kRecordLengthFieldSize) {
        return false;
    }
    const uint64_t encoded_size = record_length + kRecordLengthFieldSize;
    if (encoded_size > checkpoint.offset - kEncodedSegmentHeaderSize) {
        return false;
    }
    const uint64_t record_offset = checkpoint.offset - encoded_size;
    if ((record_offset - kEncodedSegmentHeaderSize) % kRecordAlignment != 0) {
        return false;
    }

    Result<RecordCheck> checked = ValidateRecord(
        reader, report->file_size, record_offset, report->segment_header,
        options, true, std::nullopt, false);
    if (!checked.ok()) return checked.status();
    if (!checked->valid ||
        checked->metadata.record_end_offset != checkpoint.offset ||
        checked->metadata.ingestion_sequence !=
            *checkpoint.ingestion_sequence) {
        return false;
    }

    report->checkpoint_used = true;
    report->metadata_is_complete = false;
    report->scan_start_offset = checkpoint.offset;
    report->last_complete_offset = checkpoint.offset;
    report->last_complete_record_offset = record_offset;
    report->has_last_complete_sequence = true;
    report->last_complete_sequence = *checkpoint.ingestion_sequence;
    return true;
}

Result<SegmentRecoveryReport> ScanFd(
    int fd, const std::filesystem::path& path,
    const SegmentRecoveryOptions& options) {
    const Result<size_t> minimum_record =
        EncodedRecordSize(0, options.format_limits);
    if (!minimum_record.ok()) return minimum_record.status();

    Result<uint64_t> size = FileSize(fd, path);
    if (!size.ok()) return size.status();

    SegmentRecoveryReport report;
    report.file_size = *size;
    report.scan_start_offset = kEncodedSegmentHeaderSize;
    report.last_complete_offset = kEncodedSegmentHeaderSize;

    if (report.file_size < kEncodedSegmentHeaderSize) {
        report.last_complete_offset = 0;
        report.disposition = SegmentRecoveryDisposition::kCorruption;
        report.reason = SegmentRecoveryReason::kIncompleteSegmentHeader;
        report.damage_offset = 0;
        report.reason_detail = "segment header is truncated";
        return report;
    }

    const SegmentReader reader(fd, path, options);
    std::array<std::byte, kEncodedSegmentHeaderSize> encoded_header{};
    Result<bool> header_read = reader.ReadExact(0, encoded_header);
    if (!header_read.ok()) return header_read.status();
    if (!*header_read) {
        report.last_complete_offset = 0;
        report.disposition = SegmentRecoveryDisposition::kCorruption;
        report.reason = SegmentRecoveryReason::kIncompleteSegmentHeader;
        report.damage_offset = 0;
        report.reason_detail = "segment header ended at EOF";
        return report;
    }
    Result<SegmentHeader> decoded_header = DecodeSegmentHeader(encoded_header);
    if (!decoded_header.ok()) {
        report.last_complete_offset = 0;
        report.disposition = SegmentRecoveryDisposition::kCorruption;
        report.reason = SegmentRecoveryReason::kSegmentHeaderCorruption;
        report.damage_offset = 0;
        report.reason_detail = decoded_header.status().ToString();
        return report;
    }
    report.segment_header = *decoded_header;

    Result<bool> checkpoint_applied = ApplyCheckpoint(reader, options, &report);
    if (!checkpoint_applied.ok()) return checkpoint_applied.status();
    if (options.checkpoint.has_value() && !*checkpoint_applied) {
        report.checkpoint_fell_back = true;
        report.checkpoint_used = false;
        report.metadata_is_complete = true;
        report.scan_start_offset = kEncodedSegmentHeaderSize;
        report.last_complete_offset = kEncodedSegmentHeaderSize;
        report.last_complete_record_offset = 0;
        report.has_last_complete_sequence = false;
        report.last_complete_sequence = 0;
    }

    uint64_t offset = report.scan_start_offset;
    std::optional<uint64_t> previous_sequence;
    if (report.has_last_complete_sequence) {
        previous_sequence = report.last_complete_sequence;
    }
    while (offset < report.file_size) {
        const bool require_first_sequence =
            !previous_sequence.has_value() &&
            offset == kEncodedSegmentHeaderSize;
        Result<RecordCheck> checked = ValidateRecord(
            reader, report.file_size, offset, report.segment_header, options,
            true, previous_sequence, require_first_sequence);
        if (!checked.ok()) return checked.status();
        if (!checked->valid) {
            bool committed_or_interior = checked->has_complete_commit_marker;
            if (!committed_or_interior) {
                Result<bool> later = HasLaterCommittedRecord(
                    reader, report.file_size, offset, report.segment_header,
                    options);
                if (!later.ok()) return later.status();
                committed_or_interior = *later;
                if (committed_or_interior) {
                    checked->detail +=
                        "; a later fully committed record makes this interior corruption";
                }
            }
            SetFinding(&report, *checked, committed_or_interior);
            return report;
        }

        report.records.push_back(checked->metadata);
        ++report.records_scanned;
        report.last_complete_record_offset = offset;
        report.last_complete_offset = checked->metadata.record_end_offset;
        report.has_last_complete_sequence = true;
        report.last_complete_sequence =
            checked->metadata.ingestion_sequence;
        previous_sequence = checked->metadata.ingestion_sequence;
        offset = checked->metadata.record_end_offset;
    }

    report.disposition = SegmentRecoveryDisposition::kClean;
    report.reason = SegmentRecoveryReason::kNone;
    report.damage_offset = report.file_size;
    report.truncated_bytes = 0;
    return report;
}

Status TruncateAndSync(int fd, const std::filesystem::path& path, uint64_t size,
                       const SegmentRepairOptions& options) {
    if (size > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
        return Invalid("truncate size does not fit off_t");
    }
    while (true) {
        const int result =
            options.truncate_hook == nullptr
                ? ::ftruncate(fd, static_cast<off_t>(size))
                : options.truncate_hook(fd, size, options.io_hook_context);
        if (result == 0) break;
        if (errno == EINTR) continue;
        return IoError("cannot truncate segment", path);
    }

    while (true) {
        int result = 0;
        if (options.sync_hook != nullptr) {
            result = options.sync_hook(fd, options.io_hook_context);
        } else if (options.sync_mode == SegmentRecoverySyncMode::kFull) {
            result = ::fsync(fd);
        } else {
            result = ::fdatasync(fd);
        }
        if (result == 0) break;
        if (errno == EINTR) continue;
        return IoError(options.sync_mode == SegmentRecoverySyncMode::kFull
                           ? "cannot fsync repaired segment"
                           : "cannot fdatasync repaired segment",
                       path);
    }
    return Status::Ok();
}

Result<int> OpenSegment(const std::filesystem::path& path, bool writable) {
    if (path.empty()) return Invalid("segment path is empty");
    const int fd = ::open(path.c_str(), OpenFlags(writable));
    if (fd < 0) return IoError("cannot open segment", path);
    const Status close_on_exec = SetCloseOnExec(fd);
    if (!close_on_exec.ok()) {
        static_cast<void>(::close(fd));
        return close_on_exec;
    }
    return fd;
}

}  // namespace

Result<SegmentRecoveryReport> ScanSegment(
    const std::filesystem::path& path,
    const SegmentRecoveryOptions& options) {
    try {
        Result<int> opened = OpenSegment(path, false);
        if (!opened.ok()) return opened.status();
        const ScopedFd fd(*opened);
        return ScanFd(fd.get(), path, options);
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate segment recovery report");
    } catch (const std::length_error&) {
        return Exhausted("segment recovery metadata is too large");
    }
}

Status TruncateSegment(const std::filesystem::path& path, uint64_t size,
                       const SegmentRepairOptions& options) {
    try {
        if (size < kEncodedSegmentHeaderSize) {
            return Invalid("truncate size would remove the Segment Header");
        }
        Result<int> opened = OpenSegment(path, true);
        if (!opened.ok()) return opened.status();
        const ScopedFd fd(*opened);
        Result<uint64_t> current_size = FileSize(fd.get(), path);
        if (!current_size.ok()) return current_size.status();
        if (size > *current_size) {
            return Invalid("truncate size cannot extend a segment");
        }
        return TruncateAndSync(fd.get(), path, size, options);
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate segment truncate error state");
    } catch (const std::length_error&) {
        return Exhausted("segment truncate path is too large");
    }
}

Result<SegmentRecoveryReport> RepairSegment(
    const std::filesystem::path& path,
    const SegmentRecoveryOptions& recovery_options,
    const SegmentRepairOptions& repair_options) {
    try {
        Result<int> opened = OpenSegment(path, true);
        if (!opened.ok()) return opened.status();
        const ScopedFd fd(*opened);
        Result<SegmentRecoveryReport> scanned =
            ScanFd(fd.get(), path, recovery_options);
        if (!scanned.ok()) return scanned.status();
        if (scanned->disposition == SegmentRecoveryDisposition::kCorruption) {
            return Status::Error(StatusCode::kCorruption,
                                 scanned->reason_detail);
        }
        if (scanned->disposition == SegmentRecoveryDisposition::kClean) {
            return std::move(*scanned);
        }

        Result<uint64_t> current_size = FileSize(fd.get(), path);
        if (!current_size.ok()) return current_size.status();
        if (*current_size != scanned->file_size) {
            return Status::Error(
                StatusCode::kUnavailable,
                "segment size changed between recovery scan and repair");
        }
        const Status repaired = TruncateAndSync(
            fd.get(), path, scanned->last_complete_offset, repair_options);
        if (!repaired.ok()) return repaired;
        scanned->repaired = true;
        return std::move(*scanned);
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate segment repair report");
    } catch (const std::length_error&) {
        return Exhausted("segment repair metadata is too large");
    }
}

}  // namespace mino::storage
