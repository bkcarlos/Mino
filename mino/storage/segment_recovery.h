// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_SEGMENT_RECOVERY_H_
#define MINO_STORAGE_SEGMENT_RECOVERY_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "mino/common/result.h"
#include "mino/storage/segment_format.h"

namespace mino::storage {

// Hooks follow POSIX pread/ftruncate/fdatasync conventions and set errno on
// failure. Null hooks select the real system calls. The uint64_t offsets are
// range-checked before real POSIX calls are made.
using SegmentPreadHook = std::ptrdiff_t (*)(int fd, std::byte* data, size_t size,
                                            uint64_t offset,
                                            void* context) noexcept;
using SegmentTruncateHook = int (*)(int fd, uint64_t size,
                                    void* context) noexcept;
using SegmentRecoverySyncHook = int (*)(int fd, void* context) noexcept;
using SegmentSchemaRefValidator = bool (*)(uint32_t schema_ref,
                                           void* context) noexcept;

struct SegmentRecoveryCheckpoint {
    // End offset immediately after the checkpoint record's commit marker. The
    // Segment Header end is also a valid checkpoint when sequence is absent.
    uint64_t offset = kEncodedSegmentHeaderSize;
    std::optional<uint64_t> ingestion_sequence;
};

struct SegmentRecordOffset {
    uint64_t record_offset = 0;
    uint64_t record_end_offset = 0;
    uint64_t encoded_size = 0;
    uint64_t payload_offset = 0;
    uint32_t payload_size = 0;

    uint16_t flags = 0;
    uint32_t schema_ref = 0;
    uint32_t schema_version = 0;
    uint32_t layout_version = 0;
    uint64_t ingestion_sequence = 0;
    uint64_t ingestion_timestamp_ns = 0;
    uint64_t source_sequence = 0;
    uint64_t observed_timestamp_ns = 0;
};

enum class SegmentRecoveryDisposition : uint8_t {
    kClean,
    // No complete commit evidence exists at or after damage_offset. Removing
    // [last_complete_offset, file_size) is safe.
    kIncompleteTail,
    // A committed record is corrupt, or a later committed record proves that
    // the damage is interior. Destructive recovery must fail closed.
    kCorruption,
};

enum class SegmentRecoveryReason : uint8_t {
    kNone,
    kIncompleteSegmentHeader,
    kSegmentHeaderCorruption,
    kIncompleteLeadingLength,
    kRecordLengthOverflow,
    kRecordTooSmall,
    kRecordTooLarge,
    kIncompleteRecord,
    kHeaderLengthMismatch,
    kRecordHeaderCorruption,
    kHeaderCrcMismatch,
    kPayloadSizeMismatch,
    kPayloadCrcMismatch,
    kPaddingCorruption,
    kTrailingLengthMismatch,
    kTrailerSequenceMismatch,
    kRecordCrcMismatch,
    kCommitMarkerMismatch,
    kTopicPartitionMismatch,
    kIngestionSequenceMismatch,
    kUnknownSchemaRef,
};

struct SegmentRecoveryReport {
    SegmentHeader segment_header{};
    uint64_t file_size = 0;

    // scan_start_offset is the effective start after checkpoint validation.
    // records contains metadata from this offset onward. metadata_is_complete
    // is false when a checkpoint skipped already indexed records.
    uint64_t scan_start_offset = kEncodedSegmentHeaderSize;
    bool checkpoint_used = false;
    bool checkpoint_fell_back = false;
    bool metadata_is_complete = true;

    uint64_t records_scanned = 0;
    uint64_t last_complete_offset = 0;  // End offset, safe truncation boundary.
    uint64_t last_complete_record_offset = 0;
    bool has_last_complete_sequence = false;
    uint64_t last_complete_sequence = 0;

    SegmentRecoveryDisposition disposition =
        SegmentRecoveryDisposition::kClean;
    SegmentRecoveryReason reason = SegmentRecoveryReason::kNone;
    uint64_t damage_offset = 0;
    uint64_t truncated_bytes = 0;
    std::string reason_detail;

    bool repaired = false;
    std::vector<SegmentRecordOffset> records;

    bool clean() const noexcept {
        return disposition == SegmentRecoveryDisposition::kClean;
    }
    bool repairable() const noexcept {
        return disposition == SegmentRecoveryDisposition::kIncompleteTail;
    }
};

struct SegmentRecoveryOptions {
    SegmentFormatLimits format_limits{};
    std::optional<SegmentRecoveryCheckpoint> checkpoint;

    // If either source is configured, a ref accepted by either source is known.
    // The pointed-to set and callback context need only outlive Scan/Repair.
    const std::unordered_set<uint32_t>* known_schema_refs = nullptr;
    SegmentSchemaRefValidator schema_ref_validator = nullptr;
    void* schema_ref_context = nullptr;

    SegmentPreadHook pread_hook = nullptr;
    void* io_hook_context = nullptr;
};

enum class SegmentRecoverySyncMode : uint8_t {
    kDataOnly,  // fdatasync
    kFull,      // fsync
};

struct SegmentRepairOptions {
    SegmentRecoverySyncMode sync_mode = SegmentRecoverySyncMode::kDataOnly;
    SegmentTruncateHook truncate_hook = nullptr;
    SegmentRecoverySyncHook sync_hook = nullptr;
    void* io_hook_context = nullptr;
};

// Scans one immutable snapshot of the current file size. Format findings are
// returned in the report; Result errors are reserved for invalid API options,
// allocation failure, and I/O failures. No bytes are modified.
Result<SegmentRecoveryReport> ScanSegment(
    const std::filesystem::path& path,
    const SegmentRecoveryOptions& options = {});

// Explicit low-level truncation. The requested size must preserve the complete
// Segment Header and cannot extend the file. A successful truncate is followed
// by fdatasync/fsync according to options before success is returned.
Status TruncateSegment(const std::filesystem::path& path, uint64_t size,
                       const SegmentRepairOptions& options = {});

// Rescans and truncates only kIncompleteTail. kClean is a no-op;
// kCorruption returns kCorruption without modifying the file. The returned
// report describes the pre-repair damage and sets repaired on truncation.
Result<SegmentRecoveryReport> RepairSegment(
    const std::filesystem::path& path,
    const SegmentRecoveryOptions& recovery_options = {},
    const SegmentRepairOptions& repair_options = {});

}  // namespace mino::storage

#endif  // MINO_STORAGE_SEGMENT_RECOVERY_H_
