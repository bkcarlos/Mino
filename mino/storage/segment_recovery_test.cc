// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/segment_recovery.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "mino/common/status.h"
#include "mino/storage/segment_format.h"

namespace mino::storage {
namespace {

std::filesystem::path TestPath(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path directory =
        base / ("mino_segment_recovery_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(directory, ignored);
    std::filesystem::create_directories(directory);
    return directory / "00000001.mino";
}

SegmentHeader SampleHeader() {
    SegmentHeader header;
    header.recording_id = 7;
    header.topic_id = 11;
    header.partition_id = 3;
    header.writer_id = 29;
    header.first_ingestion_sequence = 1;
    header.created_at_ns = 100;
    return header;
}

Record SampleRecord(uint64_t sequence, uint32_t schema_ref = 1,
                    size_t payload_size = 9) {
    Record record;
    record.header.schema_ref = schema_ref;
    record.header.schema_version = 0x00010000u;
    record.header.layout_version = 1;
    record.header.topic_id = 11;
    record.header.partition_id = 3;
    record.header.ingestion_sequence = sequence;
    record.header.ingestion_timestamp_ns = 1000 + sequence;
    record.header.node_id = 41;
    record.header.publisher_id = 43;
    record.header.publisher_epoch = 47;
    record.header.source_sequence = 100 + sequence;
    record.header.observed_timestamp_ns = 2000 + sequence;
    record.payload.resize(payload_size);
    for (size_t index = 0; index < payload_size; ++index) {
        record.payload[index] = static_cast<std::byte>(index & 0xffu);
    }
    return record;
}

void Append(std::vector<std::byte>* output,
            std::span<const std::byte> bytes) {
    output->insert(output->end(), bytes.begin(), bytes.end());
}

std::vector<std::byte> SegmentBytes(std::span<const Record> records) {
    auto header = EncodeSegmentHeader(SampleHeader());
    EXPECT_TRUE(header.ok());
    if (!header.ok()) return {};
    std::vector<std::byte> bytes = *header;
    for (const Record& record : records) {
        auto encoded = EncodeRecord(record);
        EXPECT_TRUE(encoded.ok());
        if (!encoded.ok()) return {};
        Append(&bytes, *encoded);
    }
    return bytes;
}

void WriteFile(const std::filesystem::path& path,
               std::span<const std::byte> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(output.good());
}

void WriteLe64(std::vector<std::byte>* bytes, size_t offset, uint64_t value) {
    ASSERT_LE(offset + sizeof(uint64_t), bytes->size());
    for (size_t index = 0; index < sizeof(uint64_t); ++index) {
        (*bytes)[offset + index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

TEST(SegmentRecoveryTest, KillLikeEveryTruncationFindsExactSafeBoundary) {
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2, 1, 37)};
    const std::vector<std::byte> complete = SegmentBytes(records);
    const size_t first_size = *EncodedRecordSize(records[0].payload.size());
    const uint64_t first_end = kEncodedSegmentHeaderSize + first_size;
    const std::filesystem::path path = TestPath("every_truncation");

    for (size_t size = 0; size <= complete.size(); ++size) {
        SCOPED_TRACE(size);
        WriteFile(path, std::span<const std::byte>(complete).first(size));
        auto report = ScanSegment(path);
        ASSERT_TRUE(report.ok()) << report.status().ToString();
        if (size < kEncodedSegmentHeaderSize) {
            EXPECT_EQ(report->disposition,
                      SegmentRecoveryDisposition::kCorruption);
            EXPECT_EQ(report->reason,
                      SegmentRecoveryReason::kIncompleteSegmentHeader);
            continue;
        }

        if (size == kEncodedSegmentHeaderSize || size == first_end ||
            size == complete.size()) {
            EXPECT_TRUE(report->clean()) << report->reason_detail;
            EXPECT_EQ(report->last_complete_offset, size);
            continue;
        }

        ASSERT_TRUE(report->repairable()) << report->reason_detail;
        const uint64_t expected_end = size < first_end
                                          ? kEncodedSegmentHeaderSize
                                          : first_end;
        EXPECT_EQ(report->last_complete_offset, expected_end);
        EXPECT_EQ(report->truncated_bytes,
                  static_cast<uint64_t>(size) - expected_end);
    }
}

TEST(SegmentRecoveryTest, CrcAndMarkerDamageFailClosedWhenCommittedOrInterior) {
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2)};
    const std::vector<std::byte> original = SegmentBytes(records);
    const size_t record_size = *EncodedRecordSize(records[0].payload.size());
    const size_t first_offset = kEncodedSegmentHeaderSize;
    const size_t second_offset = first_offset + record_size;

    {
        std::vector<std::byte> damaged = original;
        damaged[first_offset + 112] ^= std::byte{1};
        const std::filesystem::path path = TestPath("payload_crc");
        WriteFile(path, damaged);
        auto report = ScanSegment(path);
        ASSERT_TRUE(report.ok());
        EXPECT_EQ(report->disposition,
                  SegmentRecoveryDisposition::kCorruption);
        EXPECT_EQ(report->reason, SegmentRecoveryReason::kPayloadCrcMismatch);
        EXPECT_EQ(report->last_complete_offset, kEncodedSegmentHeaderSize);
    }
    {
        std::vector<std::byte> damaged = original;
        damaged[first_offset + record_size - 1] ^= std::byte{1};
        const std::filesystem::path path = TestPath("interior_marker");
        WriteFile(path, damaged);
        auto report = ScanSegment(path);
        ASSERT_TRUE(report.ok());
        EXPECT_EQ(report->disposition,
                  SegmentRecoveryDisposition::kCorruption);
        EXPECT_EQ(report->reason,
                  SegmentRecoveryReason::kCommitMarkerMismatch);
    }
    {
        std::vector<std::byte> damaged = original;
        damaged[second_offset + record_size - 12] ^= std::byte{1};
        const std::filesystem::path path = TestPath("record_crc");
        WriteFile(path, damaged);
        auto report = ScanSegment(path);
        ASSERT_TRUE(report.ok());
        EXPECT_EQ(report->disposition,
                  SegmentRecoveryDisposition::kCorruption);
        EXPECT_EQ(report->reason, SegmentRecoveryReason::kRecordCrcMismatch);
        EXPECT_EQ(report->last_complete_offset, second_offset);
    }
}

TEST(SegmentRecoveryTest, NoncontiguousCommittedSequenceFailsClosed) {
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(3)};
    const std::vector<std::byte> bytes = SegmentBytes(records);
    const std::filesystem::path path = TestPath("sequence_gap");
    WriteFile(path, bytes);

    auto report = ScanSegment(path);
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->disposition,
              SegmentRecoveryDisposition::kCorruption);
    EXPECT_EQ(report->reason,
              SegmentRecoveryReason::kIngestionSequenceMismatch);
    EXPECT_EQ(report->last_complete_sequence, 1u);
}

TEST(SegmentRecoveryTest, DamagedUncommittedLastMarkerIsRepairableTail) {
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2)};
    std::vector<std::byte> bytes = SegmentBytes(records);
    const size_t record_size = *EncodedRecordSize(records[0].payload.size());
    const uint64_t first_end = kEncodedSegmentHeaderSize + record_size;
    bytes.back() ^= std::byte{1};
    const std::filesystem::path path = TestPath("tail_marker");
    WriteFile(path, bytes);

    auto report = ScanSegment(path);
    ASSERT_TRUE(report.ok());
    EXPECT_TRUE(report->repairable());
    EXPECT_EQ(report->reason, SegmentRecoveryReason::kCommitMarkerMismatch);
    EXPECT_EQ(report->last_complete_offset, first_end);
    EXPECT_EQ(report->last_complete_sequence, 1u);
    EXPECT_EQ(report->truncated_bytes, record_size);
}

TEST(SegmentRecoveryTest, ValidCheckpointSkipsPrefixAndInvalidCheckpointFallsBack) {
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2),
                                         SampleRecord(3)};
    const std::vector<std::byte> bytes = SegmentBytes(records);
    const uint64_t record_size = *EncodedRecordSize(records[0].payload.size());
    const uint64_t first_end = kEncodedSegmentHeaderSize + record_size;
    const std::filesystem::path path = TestPath("checkpoint");
    WriteFile(path, bytes);

    SegmentRecoveryOptions valid_options;
    valid_options.checkpoint = SegmentRecoveryCheckpoint{first_end, 1};
    auto from_checkpoint = ScanSegment(path, valid_options);
    ASSERT_TRUE(from_checkpoint.ok()) << from_checkpoint.status().ToString();
    EXPECT_TRUE(from_checkpoint->clean());
    EXPECT_TRUE(from_checkpoint->checkpoint_used);
    EXPECT_FALSE(from_checkpoint->checkpoint_fell_back);
    EXPECT_FALSE(from_checkpoint->metadata_is_complete);
    EXPECT_EQ(from_checkpoint->scan_start_offset, first_end);
    ASSERT_EQ(from_checkpoint->records.size(), 2u);
    EXPECT_EQ(from_checkpoint->records[0].record_offset, first_end);
    EXPECT_EQ(from_checkpoint->last_complete_sequence, 3u);

    SegmentRecoveryOptions invalid_options;
    invalid_options.checkpoint =
        SegmentRecoveryCheckpoint{first_end - kRecordAlignment, 1};
    auto fallback = ScanSegment(path, invalid_options);
    ASSERT_TRUE(fallback.ok()) << fallback.status().ToString();
    EXPECT_TRUE(fallback->clean());
    EXPECT_FALSE(fallback->checkpoint_used);
    EXPECT_TRUE(fallback->checkpoint_fell_back);
    EXPECT_TRUE(fallback->metadata_is_complete);
    EXPECT_EQ(fallback->scan_start_offset, kEncodedSegmentHeaderSize);
    EXPECT_EQ(fallback->records.size(), 3u);
}

struct RepairIoState {
    size_t truncate_calls = 0;
    size_t sync_calls = 0;
    bool interrupt_truncate_once = false;
    bool interrupt_sync_once = false;
};

int ControlledTruncate(int fd, uint64_t size, void* context) noexcept {
    auto* state = static_cast<RepairIoState*>(context);
    ++state->truncate_calls;
    if (state->interrupt_truncate_once) {
        state->interrupt_truncate_once = false;
        errno = EINTR;
        return -1;
    }
    return ::ftruncate(fd, static_cast<off_t>(size));
}

int ControlledSync(int fd, void* context) noexcept {
    auto* state = static_cast<RepairIoState*>(context);
    ++state->sync_calls;
    if (state->interrupt_sync_once) {
        state->interrupt_sync_once = false;
        errno = EINTR;
        return -1;
    }
    static_cast<void>(fd);
    return 0;
}

TEST(SegmentRecoveryTest, RepairTruncatesAndSyncsOnlyIncompleteTail) {
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2)};
    std::vector<std::byte> bytes = SegmentBytes(records);
    const uint64_t record_size = *EncodedRecordSize(records[0].payload.size());
    const uint64_t first_end = kEncodedSegmentHeaderSize + record_size;
    bytes.back() ^= std::byte{1};
    const std::filesystem::path path = TestPath("repair");
    WriteFile(path, bytes);

    RepairIoState io;
    io.interrupt_truncate_once = true;
    io.interrupt_sync_once = true;
    SegmentRepairOptions options;
    options.truncate_hook = ControlledTruncate;
    options.sync_hook = ControlledSync;
    options.io_hook_context = &io;
    auto repaired = RepairSegment(path, {}, options);
    ASSERT_TRUE(repaired.ok()) << repaired.status().ToString();
    EXPECT_TRUE(repaired->repaired);
    EXPECT_EQ(repaired->truncated_bytes, record_size);
    EXPECT_EQ(std::filesystem::file_size(path), first_end);
    EXPECT_EQ(io.truncate_calls, 2u);
    EXPECT_EQ(io.sync_calls, 2u);

    auto clean = ScanSegment(path);
    ASSERT_TRUE(clean.ok());
    EXPECT_TRUE(clean->clean());

    std::vector<std::byte> committed_damage = SegmentBytes(records);
    committed_damage[kEncodedSegmentHeaderSize + 112] ^= std::byte{1};
    const std::filesystem::path corrupt_path = TestPath("repair_fail_closed");
    WriteFile(corrupt_path, committed_damage);
    const uint64_t size_before = std::filesystem::file_size(corrupt_path);
    auto rejected = RepairSegment(corrupt_path);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kCorruption);
    EXPECT_EQ(std::filesystem::file_size(corrupt_path), size_before);
}

TEST(SegmentRecoveryTest, ExplicitTruncateRejectsExtensionAndSyncsSuccess) {
    const std::vector<Record> records = {SampleRecord(1)};
    const std::vector<std::byte> bytes = SegmentBytes(records);
    const std::filesystem::path path = TestPath("explicit_truncate");
    WriteFile(path, bytes);

    RepairIoState io;
    SegmentRepairOptions options;
    options.truncate_hook = ControlledTruncate;
    options.sync_hook = ControlledSync;
    options.io_hook_context = &io;
    ASSERT_TRUE(TruncateSegment(path, kEncodedSegmentHeaderSize, options).ok());
    EXPECT_EQ(std::filesystem::file_size(path), kEncodedSegmentHeaderSize);
    EXPECT_EQ(io.truncate_calls, 1u);
    EXPECT_EQ(io.sync_calls, 1u);

    EXPECT_EQ(TruncateSegment(path, kEncodedSegmentHeaderSize + 1).code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(TruncateSegment(path, kEncodedSegmentHeaderSize - 1).code(),
              StatusCode::kInvalidArgument);
}

struct ReadIoState {
    size_t calls = 0;
    size_t max_read = std::numeric_limits<size_t>::max();
    size_t interrupt_call = std::numeric_limits<size_t>::max();
    size_t fail_call = std::numeric_limits<size_t>::max();
};

std::ptrdiff_t ControlledPread(int fd, std::byte* data, size_t size,
                               uint64_t offset, void* context) noexcept {
    auto* state = static_cast<ReadIoState*>(context);
    ++state->calls;
    if (state->calls == state->interrupt_call) {
        errno = EINTR;
        return -1;
    }
    if (state->calls == state->fail_call) {
        errno = EIO;
        return -1;
    }
    const size_t count = std::min(size, state->max_read);
    return static_cast<std::ptrdiff_t>(
        ::pread(fd, data, count, static_cast<off_t>(offset)));
}

TEST(SegmentRecoveryTest, RetriesEintrAndShortReadsAndPropagatesIoError) {
    const std::vector<Record> records = {SampleRecord(1, 1, 200000)};
    const std::vector<std::byte> bytes = SegmentBytes(records);
    const std::filesystem::path path = TestPath("read_hooks");
    WriteFile(path, bytes);

    ReadIoState short_reads;
    short_reads.max_read = 7;
    short_reads.interrupt_call = 1;
    SegmentRecoveryOptions options;
    options.pread_hook = ControlledPread;
    options.io_hook_context = &short_reads;
    auto recovered = ScanSegment(path, options);
    ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
    EXPECT_TRUE(recovered->clean());
    EXPECT_GT(short_reads.calls, 100u);

    ReadIoState failed;
    failed.fail_call = 3;
    options.io_hook_context = &failed;
    auto error = ScanSegment(path, options);
    ASSERT_FALSE(error.ok());
    EXPECT_EQ(error.status().code(), StatusCode::kUnavailable);
}

TEST(SegmentRecoveryTest, OverflowLengthNeverAllocatesAndDoesNotHideInteriorData) {
    const Record record = SampleRecord(1);
    std::vector<std::byte> tail = SegmentBytes(std::span<const Record>(&record, 1));
    const uint64_t valid_end = tail.size();
    tail.resize(tail.size() + sizeof(uint64_t));
    WriteLe64(&tail, static_cast<size_t>(valid_end),
              std::numeric_limits<uint64_t>::max());
    const std::filesystem::path tail_path = TestPath("overflow_tail");
    WriteFile(tail_path, tail);

    auto report = ScanSegment(tail_path);
    ASSERT_TRUE(report.ok());
    EXPECT_TRUE(report->repairable());
    EXPECT_EQ(report->reason, SegmentRecoveryReason::kRecordLengthOverflow);
    EXPECT_EQ(report->last_complete_offset, valid_end);
    EXPECT_EQ(report->truncated_bytes, sizeof(uint64_t));

    const Record second = SampleRecord(2);
    auto encoded_second = EncodeRecord(second);
    ASSERT_TRUE(encoded_second.ok());
    Append(&tail, *encoded_second);
    const std::filesystem::path interior_path = TestPath("overflow_interior");
    WriteFile(interior_path, tail);
    auto interior = ScanSegment(interior_path);
    ASSERT_TRUE(interior.ok());
    EXPECT_EQ(interior->disposition,
              SegmentRecoveryDisposition::kCorruption);
    EXPECT_EQ(interior->reason,
              SegmentRecoveryReason::kRecordLengthOverflow);
}

bool ValidateSchemaTwo(uint32_t schema_ref, void* context) noexcept {
    const auto* enabled = static_cast<const bool*>(context);
    return *enabled && schema_ref == 2;
}

TEST(SegmentRecoveryTest, UnknownSchemaRefIsCommittedCorruption) {
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2, 2)};
    const std::vector<std::byte> bytes = SegmentBytes(records);
    const std::filesystem::path path = TestPath("schema_ref");
    WriteFile(path, bytes);

    const std::unordered_set<uint32_t> known = {1};
    SegmentRecoveryOptions options;
    options.known_schema_refs = &known;
    auto unknown = ScanSegment(path, options);
    ASSERT_TRUE(unknown.ok());
    EXPECT_EQ(unknown->disposition,
              SegmentRecoveryDisposition::kCorruption);
    EXPECT_EQ(unknown->reason, SegmentRecoveryReason::kUnknownSchemaRef);
    EXPECT_EQ(unknown->last_complete_sequence, 1u);

    const bool callback_enabled = true;
    options.schema_ref_validator = ValidateSchemaTwo;
    options.schema_ref_context = const_cast<bool*>(&callback_enabled);
    auto accepted = ScanSegment(path, options);
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
    EXPECT_TRUE(accepted->clean());
}

TEST(SegmentRecoveryTest, ReturnsIndexRebuildMetadataWithoutPayloadAllocation) {
    const std::vector<Record> records = {SampleRecord(1, 7, 3),
                                         SampleRecord(2, 8, 70000)};
    const std::vector<std::byte> bytes = SegmentBytes(records);
    const std::filesystem::path path = TestPath("metadata");
    WriteFile(path, bytes);

    auto report = ScanSegment(path);
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    ASSERT_TRUE(report->clean());
    ASSERT_EQ(report->records.size(), 2u);
    EXPECT_TRUE(report->metadata_is_complete);
    EXPECT_EQ(report->records[0].record_offset, kEncodedSegmentHeaderSize);
    EXPECT_EQ(report->records[0].payload_offset,
              kEncodedSegmentHeaderSize + 112);
    EXPECT_EQ(report->records[0].payload_size, 3u);
    EXPECT_EQ(report->records[0].schema_ref, 7u);
    EXPECT_EQ(report->records[1].ingestion_sequence, 2u);
    EXPECT_EQ(report->records[1].source_sequence, 102u);
    EXPECT_EQ(report->records[1].record_end_offset, bytes.size());
    EXPECT_EQ(report->last_complete_offset, bytes.size());
    EXPECT_EQ(report->last_complete_record_offset,
              report->records[1].record_offset);
}

}  // namespace
}  // namespace mino::storage
