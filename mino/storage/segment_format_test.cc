// Copyright 2026 The Mino Authors

#include "mino/storage/segment_format.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <span>
#include <string_view>
#include <vector>

#include "mino/common/status.h"

namespace mino::storage {
namespace {

std::vector<std::byte> Bytes(std::initializer_list<uint8_t> values) {
    std::vector<std::byte> output;
    output.reserve(values.size());
    for (uint8_t value : values) {
        output.push_back(static_cast<std::byte>(value));
    }
    return output;
}

void ExpectBytesAt(std::span<const std::byte> actual, size_t offset,
                   std::initializer_list<uint8_t> expected,
                   std::string_view field) {
    SCOPED_TRACE(field);
    ASSERT_LE(offset + expected.size(), actual.size());
    size_t index = 0;
    for (uint8_t value : expected) {
        EXPECT_EQ(static_cast<uint8_t>(actual[offset + index]), value)
            << "byte " << index << " at absolute offset " << offset + index;
        ++index;
    }
}

void WriteLe32(std::vector<std::byte>* bytes, size_t offset, uint32_t value) {
    ASSERT_LE(offset + 4, bytes->size());
    for (size_t i = 0; i < 4; ++i) {
        (*bytes)[offset + i] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

void WriteLe64(std::vector<std::byte>* bytes, size_t offset, uint64_t value) {
    ASSERT_LE(offset + 8, bytes->size());
    for (size_t i = 0; i < 8; ++i) {
        (*bytes)[offset + i] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

SegmentHeader SampleSegmentHeader() {
    SegmentHeader header;
    header.recording_id = 0x0102030405060708ull;
    header.topic_id = 0x11121314u;
    header.partition_id = 0x21222324u;
    header.writer_id = 0x3132333435363738ull;
    header.first_ingestion_sequence = 0x4142434445464748ull;
    header.created_at_ns = 0x5152535455565758ull;
    return header;
}

Record SampleRecord() {
    Record record;
    record.header.schema_ref = 0x01020304u;
    record.header.schema_version = 0x00020003u;
    record.header.layout_version = 0x11121314u;
    record.header.topic_id = 0x21222324u;
    record.header.partition_id = 0x31323334u;
    record.header.ingestion_sequence = 0x0102030405060708ull;
    record.header.ingestion_timestamp_ns = 0x1112131415161718ull;
    record.header.node_id = 0x2122232425262728ull;
    record.header.publisher_id = 0x3132333435363738ull;
    record.header.publisher_epoch = 0x4142434445464748ull;
    record.header.source_sequence = 0x5152535455565758ull;
    record.header.observed_timestamp_ns = 0x6162636465666768ull;
    record.payload = Bytes({0xde, 0xad, 0xbe});
    return record;
}

void ExpectRecordCorruption(std::span<const std::byte> encoded) {
    const Result<Record> decoded = DecodeRecord(encoded);
    ASSERT_FALSE(decoded.ok());
    EXPECT_EQ(decoded.status().code(), StatusCode::kCorruption)
        << decoded.status().ToString();
}

TEST(SegmentFormatTest, SegmentHeaderRoundTripsAndUsesGoldenLittleEndian) {
    const SegmentHeader expected = SampleSegmentHeader();
    const Result<std::vector<std::byte>> encoded =
        EncodeSegmentHeader(expected);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    ASSERT_EQ(encoded->size(), kEncodedSegmentHeaderSize);

    ExpectBytesAt(*encoded, 0, {0x4d, 0x53, 0x45, 0x47}, "magic MSEG");
    ExpectBytesAt(*encoded, 4, {0x01, 0x00}, "format_version");
    ExpectBytesAt(*encoded, 6, {0x00, 0x00}, "flags");
    ExpectBytesAt(*encoded, 8,
                  {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
                  "recording_id");
    ExpectBytesAt(*encoded, 16, {0x14, 0x13, 0x12, 0x11}, "topic_id");
    ExpectBytesAt(*encoded, 20, {0x24, 0x23, 0x22, 0x21},
                  "partition_id");
    ExpectBytesAt(*encoded, 24,
                  {0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31},
                  "writer_id");
    ExpectBytesAt(*encoded, 32,
                  {0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41},
                  "first_ingestion_sequence");
    ExpectBytesAt(*encoded, 40,
                  {0x58, 0x57, 0x56, 0x55, 0x54, 0x53, 0x52, 0x51},
                  "created_at_ns");
    ExpectBytesAt(*encoded, 48, {0x40, 0x77, 0xcd, 0x5f},
                  "little-endian segment header CRC32C");

    const Result<SegmentHeader> decoded = DecodeSegmentHeader(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, expected);
}

TEST(SegmentFormatTest, SegmentHeaderRejectsTruncationTrailingAndCrcDamage) {
    const Result<std::vector<std::byte>> encoded =
        EncodeSegmentHeader(SampleSegmentHeader());
    ASSERT_TRUE(encoded.ok());

    for (size_t size = 0; size < encoded->size(); ++size) {
        const Result<SegmentHeader> decoded =
            DecodeSegmentHeader(std::span<const std::byte>(*encoded).first(size));
        ASSERT_FALSE(decoded.ok()) << "accepted size " << size;
        EXPECT_EQ(decoded.status().code(), StatusCode::kCorruption);
    }

    std::vector<std::byte> trailing = *encoded;
    trailing.push_back(std::byte{0});
    EXPECT_EQ(DecodeSegmentHeader(trailing).status().code(),
              StatusCode::kCorruption);

    std::vector<std::byte> damaged = *encoded;
    damaged[8] ^= std::byte{0x01};
    EXPECT_EQ(DecodeSegmentHeader(damaged).status().code(),
              StatusCode::kCorruption);

    std::vector<std::byte> magic = *encoded;
    magic[0] ^= std::byte{0x01};
    EXPECT_EQ(DecodeSegmentHeader(magic).status().code(),
              StatusCode::kCorruption);

    std::vector<std::byte> version = *encoded;
    version[4] = std::byte{0x02};
    EXPECT_EQ(DecodeSegmentHeader(version).status().code(),
              StatusCode::kCorruption);

    std::vector<std::byte> reserved_flags = *encoded;
    reserved_flags[6] = std::byte{0x01};
    EXPECT_EQ(DecodeSegmentHeader(reserved_flags).status().code(),
              StatusCode::kCorruption);

    SegmentHeader invalid = SampleSegmentHeader();
    invalid.flags = 1;
    const Result<std::vector<std::byte>> rejected =
        EncodeSegmentHeader(invalid);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kInvalidArgument);
}

TEST(SegmentFormatTest, RecordRoundTripsAndUsesGoldenLittleEndianEnvelope) {
    const Record expected = SampleRecord();
    const Result<std::vector<std::byte>> encoded = EncodeRecord(expected);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    ASSERT_EQ(encoded->size(), kMinimumEncodedRecordSize);

    ExpectBytesAt(*encoded, 0,
                  {0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                  "leading record_length");
    ExpectBytesAt(*encoded, 8, {0x64, 0x00, 0x00, 0x00},
                  "header_length");
    ExpectBytesAt(*encoded, 12, {0x4d, 0x52, 0x45, 0x43}, "magic MREC");
    ExpectBytesAt(*encoded, 16, {0x01, 0x00}, "format_version");
    ExpectBytesAt(*encoded, 20, {0x04, 0x03, 0x02, 0x01}, "schema_ref");
    ExpectBytesAt(*encoded, 24, {0x03, 0x00, 0x02, 0x00},
                  "schema_version");
    ExpectBytesAt(*encoded, 40,
                  {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
                  "ingestion_sequence");
    ExpectBytesAt(*encoded, 96, {0x03, 0x00, 0x00, 0x00}, "payload_size");
    ExpectBytesAt(*encoded, 100, {0xbe, 0x0f, 0x7d, 0xef},
                  "little-endian payload CRC32C");
    ExpectBytesAt(*encoded, 104, {0x33, 0xb3, 0x2d, 0xa9},
                  "little-endian record header CRC32C");
    ExpectBytesAt(*encoded, 108, {0x00, 0x00, 0x00, 0x00}, "reserved");
    ExpectBytesAt(*encoded, 112, {0xde, 0xad, 0xbe}, "payload");
    ExpectBytesAt(*encoded, 115, {0x00}, "alignment padding");
    ExpectBytesAt(*encoded, 116,
                  {0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                  "trailing record_length");
    ExpectBytesAt(*encoded, 124,
                  {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
                  "trailer ingestion_sequence");
    ExpectBytesAt(*encoded, 132, {0x63, 0xc7, 0x90, 0x23},
                  "little-endian record CRC32C");
    ExpectBytesAt(*encoded, 136,
                  {0x54, 0x4d, 0x4d, 0x43, 0x4f, 0x4e, 0x49, 0x4d},
                  "little-endian commit marker");

    const Result<Record> decoded = DecodeRecord(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, expected);
}

TEST(SegmentFormatTest, EncodedSizeIncludesCanonicalEightBytePadding) {
    for (uint64_t payload_size = 0; payload_size < 24; ++payload_size) {
        const Result<size_t> size = EncodedRecordSize(payload_size);
        ASSERT_TRUE(size.ok()) << size.status().ToString();
        EXPECT_EQ(*size % kRecordAlignment, 0u);
        EXPECT_GE(*size, kRecordLengthFieldSize +
                             kRecordHeaderLengthFieldSize +
                             kEncodedRecordHeaderSize + payload_size +
                             kRecordTrailerSize);
        EXPECT_LT(*size, kRecordLengthFieldSize +
                            kRecordHeaderLengthFieldSize +
                            kEncodedRecordHeaderSize + payload_size +
                            kRecordTrailerSize + kRecordAlignment);
    }
}

TEST(SegmentFormatTest, RecordRejectsEveryTruncationAndTrailingBytes) {
    const Result<std::vector<std::byte>> encoded = EncodeRecord(SampleRecord());
    ASSERT_TRUE(encoded.ok());
    for (size_t size = 0; size < encoded->size(); ++size) {
        SCOPED_TRACE(size);
        ExpectRecordCorruption(
            std::span<const std::byte>(*encoded).first(size));
    }

    std::vector<std::byte> trailing = *encoded;
    trailing.push_back(std::byte{0});
    ExpectRecordCorruption(trailing);
}

TEST(SegmentFormatTest, RecordRejectsLengthAndSequenceMismatches) {
    const Result<std::vector<std::byte>> encoded = EncodeRecord(SampleRecord());
    ASSERT_TRUE(encoded.ok());

    std::vector<std::byte> leading_length = *encoded;
    WriteLe64(&leading_length, 0, 137);
    ExpectRecordCorruption(leading_length);

    std::vector<std::byte> header_length = *encoded;
    WriteLe32(&header_length, 8, 99);
    ExpectRecordCorruption(header_length);

    std::vector<std::byte> payload_length = *encoded;
    WriteLe32(&payload_length, 96, 2);
    ExpectRecordCorruption(payload_length);

    std::vector<std::byte> trailing_length = *encoded;
    WriteLe64(&trailing_length, trailing_length.size() - kRecordTrailerSize,
              135);
    ExpectRecordCorruption(trailing_length);

    std::vector<std::byte> trailer_sequence = *encoded;
    WriteLe64(&trailer_sequence,
              trailer_sequence.size() - kRecordTrailerSize + 8, 9);
    ExpectRecordCorruption(trailer_sequence);
}

TEST(SegmentFormatTest, RecordRejectsHeaderPayloadAndEnvelopeCrcDamage) {
    const Result<std::vector<std::byte>> encoded = EncodeRecord(SampleRecord());
    ASSERT_TRUE(encoded.ok());

    std::vector<std::byte> header_damage = *encoded;
    header_damage[20] ^= std::byte{0x01};
    ExpectRecordCorruption(header_damage);

    std::vector<std::byte> stored_header_crc = *encoded;
    stored_header_crc[104] ^= std::byte{0x01};
    ExpectRecordCorruption(stored_header_crc);

    std::vector<std::byte> payload_damage = *encoded;
    payload_damage[112] ^= std::byte{0x01};
    ExpectRecordCorruption(payload_damage);

    std::vector<std::byte> stored_record_crc = *encoded;
    stored_record_crc[stored_record_crc.size() - 12] ^= std::byte{0x01};
    ExpectRecordCorruption(stored_record_crc);
}

TEST(SegmentFormatTest, RecordRejectsCommitMarkerReservedAndPaddingDamage) {
    const Result<std::vector<std::byte>> encoded = EncodeRecord(SampleRecord());
    ASSERT_TRUE(encoded.ok());

    std::vector<std::byte> marker = *encoded;
    marker.back() ^= std::byte{0x01};
    ExpectRecordCorruption(marker);

    std::vector<std::byte> reserved = *encoded;
    reserved[108] = std::byte{1};
    ExpectRecordCorruption(reserved);

    std::vector<std::byte> padding = *encoded;
    padding[115] = std::byte{1};
    ExpectRecordCorruption(padding);

    std::vector<std::byte> magic = *encoded;
    magic[12] ^= std::byte{1};
    ExpectRecordCorruption(magic);

    std::vector<std::byte> version = *encoded;
    version[16] = std::byte{2};
    ExpectRecordCorruption(version);

    std::vector<std::byte> flags = *encoded;
    flags[19] = std::byte{0x80};
    ExpectRecordCorruption(flags);
}

TEST(SegmentFormatTest, InvalidLocalInputsDifferFromCorruptDiskBytes) {
    const Result<size_t> overflow =
        EncodedRecordSize(std::numeric_limits<uint64_t>::max());
    ASSERT_FALSE(overflow.ok());
    EXPECT_EQ(overflow.status().code(), StatusCode::kInvalidArgument);

    SegmentFormatLimits invalid_limits;
    invalid_limits.max_encoded_record_size = kMinimumEncodedRecordSize - 1;
    const Result<size_t> invalid_size = EncodedRecordSize(0, invalid_limits);
    ASSERT_FALSE(invalid_size.ok());
    EXPECT_EQ(invalid_size.status().code(), StatusCode::kInvalidArgument);

    Record invalid_record = SampleRecord();
    invalid_record.header.flags = 0x8000;
    const Result<std::vector<std::byte>> invalid_flags =
        EncodeRecord(invalid_record);
    ASSERT_FALSE(invalid_flags.ok());
    EXPECT_EQ(invalid_flags.status().code(), StatusCode::kInvalidArgument);

    const Result<std::vector<std::byte>> encoded = EncodeRecord(SampleRecord());
    ASSERT_TRUE(encoded.ok());
    SegmentFormatLimits decode_limits;
    decode_limits.max_payload_size = 2;
    const Result<Record> corrupt = DecodeRecord(*encoded, decode_limits);
    ASSERT_FALSE(corrupt.ok());
    EXPECT_EQ(corrupt.status().code(), StatusCode::kCorruption);

    const Result<Record> bad_local_limits =
        DecodeRecord(*encoded, invalid_limits);
    ASSERT_FALSE(bad_local_limits.ok());
    EXPECT_EQ(bad_local_limits.status().code(),
              StatusCode::kInvalidArgument);
}

TEST(SegmentFormatTest, GapAndTombstoneFlagsRoundTrip) {
    for (uint16_t flags : {kRecordFlagGap, kRecordFlagTombstone,
                           static_cast<uint16_t>(kRecordFlagGap |
                                                 kRecordFlagTombstone)}) {
        Record expected = SampleRecord();
        expected.header.flags = flags;
        const Result<std::vector<std::byte>> encoded = EncodeRecord(expected);
        ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
        const Result<Record> decoded = DecodeRecord(*encoded);
        ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
        EXPECT_EQ(decoded->header.flags, flags);
    }
}

TEST(SegmentFormatTest, BoundsAreEnforcedBeforeAllocationOrLengthArithmetic) {
    Record record = SampleRecord();
    record.payload = Bytes({1, 2, 3, 4, 5, 6, 7, 8});
    const Result<std::vector<std::byte>> encoded = EncodeRecord(record);
    ASSERT_TRUE(encoded.ok());
    ASSERT_GT(encoded->size(), kMinimumEncodedRecordSize);

    SegmentFormatLimits limits;
    limits.max_payload_size = 7;
    const Result<std::vector<std::byte>> rejected_payload =
        EncodeRecord(record, limits);
    ASSERT_FALSE(rejected_payload.ok());
    EXPECT_EQ(rejected_payload.status().code(), StatusCode::kInvalidArgument);

    limits.max_payload_size = 1024;
    limits.max_encoded_record_size = kMinimumEncodedRecordSize;
    const Result<std::vector<std::byte>> rejected_envelope =
        EncodeRecord(record, limits);
    ASSERT_FALSE(rejected_envelope.ok());
    EXPECT_EQ(rejected_envelope.status().code(),
              StatusCode::kInvalidArgument);

    const Result<Record> oversized_disk = DecodeRecord(*encoded, limits);
    ASSERT_FALSE(oversized_disk.ok());
    EXPECT_EQ(oversized_disk.status().code(), StatusCode::kCorruption);

    std::vector<std::byte> overflowing_length = *encoded;
    WriteLe64(&overflowing_length, 0, std::numeric_limits<uint64_t>::max());
    ExpectRecordCorruption(overflowing_length);
}

}  // namespace
}  // namespace mino::storage
