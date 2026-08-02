// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_SEGMENT_FORMAT_H_
#define MINO_STORAGE_SEGMENT_FORMAT_H_

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "mino/common/result.h"

namespace mino::storage {

// Every integer is encoded explicitly in little-endian order. These logical
// types are never copied to or from disk as C++ object representations.
inline constexpr uint32_t kSegmentHeaderMagic = 0x4745534du;  // "MSEG" on disk.
inline constexpr uint32_t kRecordHeaderMagic = 0x4345524du;   // "MREC" on disk.
inline constexpr uint16_t kSegmentFormatVersion = 1;
inline constexpr uint16_t kRecordFormatVersion = 1;
inline constexpr uint64_t kRecordCommitMarker =
    0x4d494e4f434d4d54ull;  // Design section 17.10.

inline constexpr size_t kEncodedSegmentHeaderSize = 52;
inline constexpr size_t kEncodedRecordHeaderSize = 100;
inline constexpr size_t kRecordLengthFieldSize = 8;
inline constexpr size_t kRecordHeaderLengthFieldSize = 4;
inline constexpr size_t kRecordTrailerSize = 28;
inline constexpr size_t kRecordAlignment = 8;
inline constexpr size_t kMinimumEncodedRecordSize = 144;

// Segment lifecycle is persisted by the manifest; v1 Segment Header bits are
// therefore all reserved. Record bits distinguish normal payloads from the
// explicit discontinuity records emitted by TopicWriter. All unknown bits are
// rejected by both encoders and decoders.
inline constexpr uint16_t kKnownSegmentFlags = 0;
inline constexpr uint16_t kRecordFlagGap = 1u << 0;
inline constexpr uint16_t kRecordFlagTombstone = 1u << 1;
inline constexpr uint16_t kKnownRecordFlags =
    kRecordFlagGap | kRecordFlagTombstone;

struct SegmentHeader {
    uint16_t flags = 0;
    uint64_t recording_id = 0;
    uint32_t topic_id = 0;
    uint32_t partition_id = 0;
    uint64_t writer_id = 0;
    uint64_t first_ingestion_sequence = 0;
    uint64_t created_at_ns = 0;

    bool operator==(const SegmentHeader& other) const noexcept;
};

struct RecordHeader {
    uint16_t flags = 0;
    uint32_t schema_ref = 0;
    uint32_t schema_version = 0;
    uint32_t layout_version = 0;
    uint32_t topic_id = 0;
    uint32_t partition_id = 0;
    uint64_t ingestion_sequence = 0;
    uint64_t ingestion_timestamp_ns = 0;
    uint64_t node_id = 0;
    uint64_t publisher_id = 0;
    uint64_t publisher_epoch = 0;
    uint64_t source_sequence = 0;
    uint64_t observed_timestamp_ns = 0;

    bool operator==(const RecordHeader& other) const noexcept;
};

struct Record {
    RecordHeader header;
    std::vector<std::byte> payload;

    bool operator==(const Record& other) const;
};

struct SegmentFormatLimits {
    uint64_t max_payload_size = 64ull * 1024ull * 1024ull;
    uint64_t max_encoded_record_size =
        64ull * 1024ull * 1024ull + 4096ull;
};

// Computes the complete envelope size, including the leading length, padding,
// trailer, and commit marker. Invalid local sizes or limits return
// kInvalidArgument without allocating.
Result<size_t> EncodedRecordSize(
    uint64_t payload_size, const SegmentFormatLimits& limits = {}) noexcept;

Result<std::vector<std::byte>> EncodeSegmentHeader(
    const SegmentHeader& header) noexcept;
Result<SegmentHeader> DecodeSegmentHeader(
    std::span<const std::byte> encoded) noexcept;

Result<std::vector<std::byte>> EncodeRecord(
    const Record& record, const SegmentFormatLimits& limits = {}) noexcept;
Result<Record> DecodeRecord(
    std::span<const std::byte> encoded,
    const SegmentFormatLimits& limits = {}) noexcept;

}  // namespace mino::storage

#endif  // MINO_STORAGE_SEGMENT_FORMAT_H_
