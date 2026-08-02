// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/segment_format.h"

#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>
#include <string_view>

#include "mino/bridge/crc32c.h"
#include "mino/common/status.h"

namespace mino::storage {
namespace {

constexpr size_t kSegmentMagicOffset = 0;
constexpr size_t kSegmentVersionOffset = 4;
constexpr size_t kSegmentFlagsOffset = 6;
constexpr size_t kSegmentRecordingIdOffset = 8;
constexpr size_t kSegmentTopicIdOffset = 16;
constexpr size_t kSegmentPartitionIdOffset = 20;
constexpr size_t kSegmentWriterIdOffset = 24;
constexpr size_t kSegmentFirstSequenceOffset = 32;
constexpr size_t kSegmentCreatedAtOffset = 40;
constexpr size_t kSegmentHeaderCrcOffset = 48;

constexpr size_t kRecordHeaderLengthOffset = 8;
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
constexpr size_t kRecordNodeIdOffset = 44;
constexpr size_t kRecordPublisherIdOffset = 52;
constexpr size_t kRecordPublisherEpochOffset = 60;
constexpr size_t kRecordSourceSequenceOffset = 68;
constexpr size_t kRecordObservedTimestampOffset = 76;
constexpr size_t kRecordPayloadSizeOffset = 84;
constexpr size_t kRecordPayloadCrcOffset = 88;
constexpr size_t kRecordHeaderCrcOffset = 92;
constexpr size_t kRecordReservedOffset = 96;

constexpr size_t kTrailerRecordLengthOffset = 0;
constexpr size_t kTrailerIngestionSequenceOffset = 8;
constexpr size_t kTrailerRecordCrcOffset = 16;
constexpr size_t kTrailerCommitMarkerOffset = 20;

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status ResourceExhausted() {
    return Status::Error(StatusCode::kResourceExhausted);
}

void WriteLe16(std::span<std::byte> output, size_t offset,
               uint16_t value) noexcept {
    for (size_t i = 0; i < 2; ++i) {
        output[offset + i] = static_cast<std::byte>(value & 0xffu);
        value = static_cast<uint16_t>(value >> 8);
    }
}

void WriteLe32(std::span<std::byte> output, size_t offset,
               uint32_t value) noexcept {
    for (size_t i = 0; i < 4; ++i) {
        output[offset + i] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

void WriteLe64(std::span<std::byte> output, size_t offset,
               uint64_t value) noexcept {
    for (size_t i = 0; i < 8; ++i) {
        output[offset + i] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
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

Status ValidateLimits(const SegmentFormatLimits& limits) {
    if (limits.max_encoded_record_size < kMinimumEncodedRecordSize) {
        return Invalid("max_encoded_record_size is smaller than one envelope");
    }
    return Status::Ok();
}

size_t PaddingSize(uint64_t payload_size) noexcept {
    const uint64_t size_without_padding =
        kRecordLengthFieldSize + kRecordHeaderLengthFieldSize +
        kEncodedRecordHeaderSize + payload_size + kRecordTrailerSize;
    return static_cast<size_t>(
        (kRecordAlignment - (size_without_padding % kRecordAlignment)) %
        kRecordAlignment);
}

uint32_t SegmentHeaderCrc(std::span<const std::byte> encoded) noexcept {
    return bridge::Crc32c(encoded.first(kSegmentHeaderCrcOffset));
}

uint32_t RecordHeaderCrc(std::span<const std::byte> header) noexcept {
    bridge::Crc32cAccumulator accumulator;
    accumulator.Update(header.first(kRecordHeaderCrcOffset));
    accumulator.Update(header.subspan(kRecordHeaderCrcOffset + 4));
    return accumulator.Finish();
}

void EncodeRecordHeader(const Record& record, std::span<std::byte> header) {
    WriteLe32(header, kRecordMagicOffset, kRecordHeaderMagic);
    WriteLe16(header, kRecordVersionOffset, kRecordFormatVersion);
    WriteLe16(header, kRecordFlagsOffset, record.header.flags);
    WriteLe32(header, kRecordSchemaRefOffset, record.header.schema_ref);
    WriteLe32(header, kRecordSchemaVersionOffset,
              record.header.schema_version);
    WriteLe32(header, kRecordLayoutVersionOffset,
              record.header.layout_version);
    WriteLe32(header, kRecordTopicIdOffset, record.header.topic_id);
    WriteLe32(header, kRecordPartitionIdOffset, record.header.partition_id);
    WriteLe64(header, kRecordIngestionSequenceOffset,
              record.header.ingestion_sequence);
    WriteLe64(header, kRecordIngestionTimestampOffset,
              record.header.ingestion_timestamp_ns);
    WriteLe64(header, kRecordNodeIdOffset, record.header.node_id);
    WriteLe64(header, kRecordPublisherIdOffset, record.header.publisher_id);
    WriteLe64(header, kRecordPublisherEpochOffset,
              record.header.publisher_epoch);
    WriteLe64(header, kRecordSourceSequenceOffset,
              record.header.source_sequence);
    WriteLe64(header, kRecordObservedTimestampOffset,
              record.header.observed_timestamp_ns);
    WriteLe32(header, kRecordPayloadSizeOffset,
              static_cast<uint32_t>(record.payload.size()));
    WriteLe32(header, kRecordPayloadCrcOffset,
              bridge::Crc32c(record.payload));
    WriteLe32(header, kRecordHeaderCrcOffset, 0);
    WriteLe32(header, kRecordReservedOffset, 0);
    WriteLe32(header, kRecordHeaderCrcOffset, RecordHeaderCrc(header));
}

}  // namespace

bool SegmentHeader::operator==(const SegmentHeader& other) const noexcept {
    return flags == other.flags && recording_id == other.recording_id &&
           topic_id == other.topic_id &&
           partition_id == other.partition_id && writer_id == other.writer_id &&
           first_ingestion_sequence == other.first_ingestion_sequence &&
           created_at_ns == other.created_at_ns;
}

bool RecordHeader::operator==(const RecordHeader& other) const noexcept {
    return flags == other.flags && schema_ref == other.schema_ref &&
           schema_version == other.schema_version &&
           layout_version == other.layout_version &&
           topic_id == other.topic_id &&
           partition_id == other.partition_id &&
           ingestion_sequence == other.ingestion_sequence &&
           ingestion_timestamp_ns == other.ingestion_timestamp_ns &&
           node_id == other.node_id && publisher_id == other.publisher_id &&
           publisher_epoch == other.publisher_epoch &&
           source_sequence == other.source_sequence &&
           observed_timestamp_ns == other.observed_timestamp_ns;
}

bool Record::operator==(const Record& other) const {
    return header == other.header && payload == other.payload;
}

Result<size_t> EncodedRecordSize(
    uint64_t payload_size, const SegmentFormatLimits& limits) noexcept {
    const Status limit_status = ValidateLimits(limits);
    if (!limit_status.ok()) return limit_status;
    if (payload_size > std::numeric_limits<uint32_t>::max()) {
        return Invalid("payload_size does not fit the v1 uint32 field");
    }
    if (payload_size > limits.max_payload_size) {
        return Invalid("payload_size exceeds max_payload_size");
    }

    const uint64_t encoded_size =
        kRecordLengthFieldSize + kRecordHeaderLengthFieldSize +
        kEncodedRecordHeaderSize + payload_size + PaddingSize(payload_size) +
        kRecordTrailerSize;
    if (encoded_size > limits.max_encoded_record_size) {
        return Invalid("record envelope exceeds max_encoded_record_size");
    }
    if (encoded_size > std::numeric_limits<size_t>::max()) {
        return Invalid("record envelope does not fit size_t");
    }
    return static_cast<size_t>(encoded_size);
}

Result<std::vector<std::byte>> EncodeSegmentHeader(
    const SegmentHeader& header) noexcept {
    try {
        if ((header.flags & ~kKnownSegmentFlags) != 0) {
            return Invalid("segment header contains reserved flag bits");
        }

        std::vector<std::byte> output(kEncodedSegmentHeaderSize);
        std::span<std::byte> bytes(output);
        WriteLe32(bytes, kSegmentMagicOffset, kSegmentHeaderMagic);
        WriteLe16(bytes, kSegmentVersionOffset, kSegmentFormatVersion);
        WriteLe16(bytes, kSegmentFlagsOffset, header.flags);
        WriteLe64(bytes, kSegmentRecordingIdOffset, header.recording_id);
        WriteLe32(bytes, kSegmentTopicIdOffset, header.topic_id);
        WriteLe32(bytes, kSegmentPartitionIdOffset, header.partition_id);
        WriteLe64(bytes, kSegmentWriterIdOffset, header.writer_id);
        WriteLe64(bytes, kSegmentFirstSequenceOffset,
                  header.first_ingestion_sequence);
        WriteLe64(bytes, kSegmentCreatedAtOffset, header.created_at_ns);
        WriteLe32(bytes, kSegmentHeaderCrcOffset, SegmentHeaderCrc(bytes));
        return output;
    } catch (const std::bad_alloc&) {
        return ResourceExhausted();
    } catch (const std::length_error&) {
        return ResourceExhausted();
    }
}

Result<SegmentHeader> DecodeSegmentHeader(
    std::span<const std::byte> encoded) noexcept {
    if (encoded.size() != kEncodedSegmentHeaderSize) {
        return Corruption(encoded.size() < kEncodedSegmentHeaderSize
                              ? "truncated segment header"
                              : "trailing bytes after segment header");
    }
    if (ReadLe32(encoded, kSegmentMagicOffset) != kSegmentHeaderMagic) {
        return Corruption("segment header magic mismatch");
    }
    if (ReadLe16(encoded, kSegmentVersionOffset) != kSegmentFormatVersion) {
        return Corruption("unsupported segment format version");
    }
    const uint16_t flags = ReadLe16(encoded, kSegmentFlagsOffset);
    if ((flags & ~kKnownSegmentFlags) != 0) {
        return Corruption("segment header contains reserved flag bits");
    }
    if (ReadLe32(encoded, kSegmentHeaderCrcOffset) !=
        SegmentHeaderCrc(encoded)) {
        return Corruption("segment header CRC32C mismatch");
    }

    SegmentHeader header;
    header.flags = flags;
    header.recording_id = ReadLe64(encoded, kSegmentRecordingIdOffset);
    header.topic_id = ReadLe32(encoded, kSegmentTopicIdOffset);
    header.partition_id = ReadLe32(encoded, kSegmentPartitionIdOffset);
    header.writer_id = ReadLe64(encoded, kSegmentWriterIdOffset);
    header.first_ingestion_sequence =
        ReadLe64(encoded, kSegmentFirstSequenceOffset);
    header.created_at_ns = ReadLe64(encoded, kSegmentCreatedAtOffset);
    return header;
}

Result<std::vector<std::byte>> EncodeRecord(
    const Record& record, const SegmentFormatLimits& limits) noexcept {
    try {
        if ((record.header.flags & ~kKnownRecordFlags) != 0) {
            return Invalid("record header contains reserved flag bits");
        }
        const Result<size_t> size = EncodedRecordSize(record.payload.size(), limits);
        if (!size.ok()) return size.status();

        std::vector<std::byte> output(*size);
        std::span<std::byte> bytes(output);
        const uint64_t record_length = output.size() - kRecordLengthFieldSize;
        WriteLe64(bytes, 0, record_length);
        WriteLe32(bytes, kRecordHeaderLengthOffset,
                  static_cast<uint32_t>(kEncodedRecordHeaderSize));

        std::span<std::byte> encoded_header =
            bytes.subspan(kRecordHeaderOffset, kEncodedRecordHeaderSize);
        EncodeRecordHeader(record, encoded_header);

        const size_t payload_offset =
            kRecordHeaderOffset + kEncodedRecordHeaderSize;
        std::copy(record.payload.begin(), record.payload.end(),
                  output.begin() + payload_offset);

        const size_t trailer_offset = output.size() - kRecordTrailerSize;
        WriteLe64(bytes, trailer_offset + kTrailerRecordLengthOffset,
                  record_length);
        WriteLe64(bytes, trailer_offset + kTrailerIngestionSequenceOffset,
                  record.header.ingestion_sequence);
        WriteLe32(bytes, trailer_offset + kTrailerRecordCrcOffset,
                  bridge::Crc32c(
                      std::span<const std::byte>(bytes).first(trailer_offset)));
        WriteLe64(bytes, trailer_offset + kTrailerCommitMarkerOffset,
                  kRecordCommitMarker);
        return output;
    } catch (const std::bad_alloc&) {
        return ResourceExhausted();
    } catch (const std::length_error&) {
        return ResourceExhausted();
    }
}

Result<Record> DecodeRecord(std::span<const std::byte> encoded,
                            const SegmentFormatLimits& limits) noexcept {
    try {
        const Status limit_status = ValidateLimits(limits);
        if (!limit_status.ok()) return limit_status;
        if (encoded.size() < kMinimumEncodedRecordSize) {
            return Corruption("truncated record envelope");
        }
        if (encoded.size() > limits.max_encoded_record_size) {
            return Corruption("record exceeds max_encoded_record_size");
        }

        const uint64_t record_length = ReadLe64(encoded, 0);
        if (record_length >
            std::numeric_limits<uint64_t>::max() - kRecordLengthFieldSize) {
            return Corruption("record_length overflows uint64");
        }
        const uint64_t declared_size =
            record_length + kRecordLengthFieldSize;
        if (declared_size != encoded.size()) {
            return Corruption(declared_size < encoded.size()
                                  ? "trailing bytes after record envelope"
                                  : "truncated record envelope");
        }
        if (ReadLe32(encoded, kRecordHeaderLengthOffset) !=
            kEncodedRecordHeaderSize) {
            return Corruption("noncanonical record header_length");
        }

        const size_t trailer_offset = encoded.size() - kRecordTrailerSize;
        const std::span<const std::byte> header =
            encoded.subspan(kRecordHeaderOffset, kEncodedRecordHeaderSize);
        if (ReadLe32(header, kRecordMagicOffset) != kRecordHeaderMagic) {
            return Corruption("record header magic mismatch");
        }
        if (ReadLe16(header, kRecordVersionOffset) !=
            kRecordFormatVersion) {
            return Corruption("unsupported record format version");
        }
        const uint16_t flags = ReadLe16(header, kRecordFlagsOffset);
        if ((flags & ~kKnownRecordFlags) != 0) {
            return Corruption("record header contains reserved flag bits");
        }
        if (ReadLe32(header, kRecordReservedOffset) != 0) {
            return Corruption("record header reserved field is non-zero");
        }

        const uint32_t payload_size =
            ReadLe32(header, kRecordPayloadSizeOffset);
        if (payload_size > limits.max_payload_size) {
            return Corruption("payload_size exceeds max_payload_size");
        }
        const uint64_t canonical_size =
            kRecordLengthFieldSize + kRecordHeaderLengthFieldSize +
            kEncodedRecordHeaderSize + static_cast<uint64_t>(payload_size) +
            PaddingSize(payload_size) + kRecordTrailerSize;
        if (canonical_size != encoded.size()) {
            return Corruption("payload_size does not match record_length");
        }

        if (ReadLe64(encoded,
                     trailer_offset + kTrailerRecordLengthOffset) !=
            record_length) {
            return Corruption("leading and trailing record_length mismatch");
        }
        const uint64_t ingestion_sequence =
            ReadLe64(header, kRecordIngestionSequenceOffset);
        if (ReadLe64(encoded,
                     trailer_offset + kTrailerIngestionSequenceOffset) !=
            ingestion_sequence) {
            return Corruption("trailer ingestion_sequence mismatch");
        }
        if (ReadLe64(encoded,
                     trailer_offset + kTrailerCommitMarkerOffset) !=
            kRecordCommitMarker) {
            return Corruption("record commit marker mismatch");
        }

        const size_t payload_offset =
            kRecordHeaderOffset + kEncodedRecordHeaderSize;
        const size_t padding_offset = payload_offset + payload_size;
        if (!std::all_of(encoded.begin() + padding_offset,
                         encoded.begin() + trailer_offset,
                         [](std::byte byte) { return byte == std::byte{0}; })) {
            return Corruption("record alignment padding is non-zero");
        }
        if (ReadLe32(header, kRecordHeaderCrcOffset) !=
            RecordHeaderCrc(header)) {
            return Corruption("record header CRC32C mismatch");
        }

        const std::span<const std::byte> payload =
            encoded.subspan(payload_offset, payload_size);
        if (ReadLe32(header, kRecordPayloadCrcOffset) !=
            bridge::Crc32c(payload)) {
            return Corruption("record payload CRC32C mismatch");
        }
        if (ReadLe32(encoded, trailer_offset + kTrailerRecordCrcOffset) !=
            bridge::Crc32c(encoded.first(trailer_offset))) {
            return Corruption("record envelope CRC32C mismatch");
        }

        Record record;
        record.header.flags = flags;
        record.header.schema_ref = ReadLe32(header, kRecordSchemaRefOffset);
        record.header.schema_version =
            ReadLe32(header, kRecordSchemaVersionOffset);
        record.header.layout_version =
            ReadLe32(header, kRecordLayoutVersionOffset);
        record.header.topic_id = ReadLe32(header, kRecordTopicIdOffset);
        record.header.partition_id =
            ReadLe32(header, kRecordPartitionIdOffset);
        record.header.ingestion_sequence = ingestion_sequence;
        record.header.ingestion_timestamp_ns =
            ReadLe64(header, kRecordIngestionTimestampOffset);
        record.header.node_id = ReadLe64(header, kRecordNodeIdOffset);
        record.header.publisher_id = ReadLe64(header, kRecordPublisherIdOffset);
        record.header.publisher_epoch =
            ReadLe64(header, kRecordPublisherEpochOffset);
        record.header.source_sequence =
            ReadLe64(header, kRecordSourceSequenceOffset);
        record.header.observed_timestamp_ns =
            ReadLe64(header, kRecordObservedTimestampOffset);
        record.payload.assign(payload.begin(), payload.end());
        return record;
    } catch (const std::bad_alloc&) {
        return ResourceExhausted();
    } catch (const std::length_error&) {
        return ResourceExhausted();
    }
}

}  // namespace mino::storage
