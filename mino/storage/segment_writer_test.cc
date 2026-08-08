// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/segment_writer.h"

#include <gtest/gtest.h>

#include <sys/uio.h>
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
#include <span>
#include <string>
#include <string_view>
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
        base / ("mino_segment_writer_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(directory, ignored);
    std::filesystem::create_directories(directory);
    return directory / "00000001.mino";
}

std::vector<std::byte> ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    const std::string characters{std::istreambuf_iterator<char>(input),
                                 std::istreambuf_iterator<char>()};
    std::vector<std::byte> bytes;
    bytes.reserve(characters.size());
    for (char character : characters) {
        bytes.push_back(
            static_cast<std::byte>(static_cast<uint8_t>(character)));
    }
    return bytes;
}

SegmentHeader SampleHeader(uint64_t created_at_ns = 100) {
    SegmentHeader header;
    header.recording_id = 7;
    header.topic_id = 11;
    header.partition_id = 3;
    header.writer_id = 29;
    header.first_ingestion_sequence = 1;
    header.created_at_ns = created_at_ns;
    return header;
}

Record SampleRecord(uint64_t sequence, size_t payload_size = 3) {
    Record record;
    record.header.schema_ref = 1;
    record.header.schema_version = 0x00010000u;
    record.header.layout_version = 1;
    record.header.topic_id = 11;
    record.header.partition_id = 3;
    record.header.ingestion_sequence = sequence;
    record.header.ingestion_timestamp_ns = 1000 + sequence;
    record.header.node_id = 41;
    record.header.publisher_id = 43;
    record.header.publisher_epoch = 47;
    record.header.source_sequence = sequence;
    record.header.observed_timestamp_ns = 2000 + sequence;
    record.payload.resize(payload_size);
    for (size_t index = 0; index < payload_size; ++index) {
        record.payload[index] = static_cast<std::byte>(index & 0xffu);
    }
    return record;
}

SegmentWriterOptions BufferedOptions() {
    SegmentWriterOptions options;
    options.batch_bytes = 0;
    options.batch_records = 0;
    options.flush_interval_ns = 0;
    return options;
}

void AppendBytes(std::vector<std::byte>* output,
                 std::span<const std::byte> bytes) {
    output->insert(output->end(), bytes.begin(), bytes.end());
}

struct IoState {
    size_t max_write_size = std::numeric_limits<size_t>::max();
    size_t write_calls = 0;
    size_t sync_calls = 0;
    size_t fail_write_call = std::numeric_limits<size_t>::max();
    size_t fail_sync_call = std::numeric_limits<size_t>::max();
};

std::ptrdiff_t ControlledWrite(int fd, const std::byte* data, size_t size,
                               void* context) noexcept {
    auto* state = static_cast<IoState*>(context);
    ++state->write_calls;
    if (state->write_calls == state->fail_write_call) {
        errno = EIO;
        return -1;
    }
    const size_t count = std::min(size, state->max_write_size);
    return static_cast<std::ptrdiff_t>(::write(fd, data, count));
}

struct VectorIoState {
    bool armed = false;
    size_t calls = 0;
    size_t interrupts = 0;
    size_t max_write_size = std::numeric_limits<size_t>::max();
    size_t bytes_before_error = std::numeric_limits<size_t>::max();
    int write_error = 0;
    bool crossed_buffer_boundary = false;
};

std::ptrdiff_t ControlledWritev(
    int fd, std::span<const SegmentWriteBuffer> buffers,
    void* context) noexcept {
    auto* state = static_cast<VectorIoState*>(context);
    ++state->calls;
    if (state->armed && state->interrupts != 0) {
        --state->interrupts;
        errno = EINTR;
        return -1;
    }
    if (state->armed && state->bytes_before_error == 0 &&
        state->write_error != 0) {
        errno = state->write_error;
        return -1;
    }

    size_t limit = state->armed ? state->max_write_size
                                : std::numeric_limits<size_t>::max();
    if (state->armed &&
        state->bytes_before_error != std::numeric_limits<size_t>::max()) {
        limit = std::min(limit, state->bytes_before_error);
    }
    std::array<iovec, 64> vectors{};
    size_t vector_count = 0;
    size_t request_bytes = 0;
    for (const SegmentWriteBuffer& buffer : buffers) {
        if (vector_count == vectors.size() || request_bytes == limit) break;
        const size_t amount = std::min(buffer.size, limit - request_bytes);
        vectors[vector_count++] = iovec{
            .iov_base = const_cast<std::byte*>(buffer.data),
            .iov_len = amount,
        };
        request_bytes += amount;
    }
    if (request_bytes == 0) {
        errno = EIO;
        return -1;
    }
    const ssize_t written =
        ::writev(fd, vectors.data(), static_cast<int>(vector_count));
    if (written > 0 && buffers.size() > 1 &&
        static_cast<size_t>(written) > buffers[0].size) {
        state->crossed_buffer_boundary = true;
    }
    if (written > 0 && state->armed &&
        state->bytes_before_error != std::numeric_limits<size_t>::max()) {
        state->bytes_before_error -= static_cast<size_t>(written);
    }
    return static_cast<std::ptrdiff_t>(written);
}

int ControlledSync(int fd, void* context) noexcept {
    auto* state = static_cast<IoState*>(context);
    ++state->sync_calls;
    if (state->sync_calls == state->fail_sync_call) {
        errno = EIO;
        return -1;
    }
    static_cast<void>(fd);
    return 0;
}

TEST(SegmentWriterTest, WritesHeaderThenOnlyCanonicalRecordsAndFlushesBatch) {
    const std::filesystem::path path = TestPath("canonical");
    SegmentWriterOptions options = BufferedOptions();
    auto writer = SegmentWriter::Create(path, SampleHeader(), 100, options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kOpen);
    EXPECT_EQ((*writer)->size_bytes(), kEncodedSegmentHeaderSize);
    EXPECT_EQ(ReadFile(path).size(), kEncodedSegmentHeaderSize);

    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2, 9)};
    auto appended = (*writer)->AppendBatch(records, 101);
    ASSERT_TRUE(appended.ok()) << appended.status().ToString();
    EXPECT_EQ(appended->records_accepted, 2u);
    EXPECT_FALSE(appended->rotate_needed);
    EXPECT_EQ((*writer)->record_count(), 2u);
    EXPECT_EQ(ReadFile(path).size(), kEncodedSegmentHeaderSize);

    ASSERT_TRUE((*writer)->Flush(102).ok());
    auto expected_header = EncodeSegmentHeader(SampleHeader());
    auto expected_first = EncodeRecord(records[0]);
    auto expected_second = EncodeRecord(records[1]);
    ASSERT_TRUE(expected_header.ok());
    ASSERT_TRUE(expected_first.ok());
    ASSERT_TRUE(expected_second.ok());
    std::vector<std::byte> expected = *expected_header;
    AppendBytes(&expected, *expected_first);
    AppendBytes(&expected, *expected_second);
    EXPECT_EQ(ReadFile(path), expected);
    EXPECT_EQ((*writer)->durable_records(), 0u);
}

TEST(SegmentWriterTest, CreateIsExclusiveSingleOwner) {
    const std::filesystem::path path = TestPath("owner");
    auto owner = SegmentWriter::Create(path, SampleHeader(), 100);
    ASSERT_TRUE(owner.ok()) << owner.status().ToString();
    auto second = SegmentWriter::Create(path, SampleHeader(), 100);
    ASSERT_FALSE(second.ok());
    EXPECT_EQ(second.status().code(), StatusCode::kAlreadyExists);
}

TEST(SegmentWriterTest, NoneAndPerBatchPoliciesInvokeExpectedSyncs) {
    {
        IoState io;
        SegmentWriterOptions options = BufferedOptions();
        options.write_hook = ControlledWrite;
        options.data_sync_hook = ControlledSync;
        options.io_hook_context = &io;
        auto writer =
            SegmentWriter::Create(TestPath("sync_none"), SampleHeader(), 100,
                                  options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
        ASSERT_TRUE((*writer)->Flush(102).ok());
        EXPECT_EQ(io.sync_calls, 0u);
        ASSERT_TRUE((*writer)->Seal(103).ok());
        EXPECT_EQ(io.sync_calls, 1u);
    }
    {
        IoState io;
        SegmentWriterOptions options = BufferedOptions();
        options.batch_records = 2;
        options.sync_policy = SegmentSyncPolicy::kPerBatch;
        options.write_hook = ControlledWrite;
        options.data_sync_hook = ControlledSync;
        options.io_hook_context = &io;
        auto writer = SegmentWriter::Create(TestPath("sync_batch"),
                                             SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
        EXPECT_EQ(io.sync_calls, 0u);
        ASSERT_TRUE((*writer)->Append(SampleRecord(2), 102).ok());
        EXPECT_EQ(io.sync_calls, 1u);
        EXPECT_EQ((*writer)->durable_records(), 2u);
        ASSERT_TRUE((*writer)->Flush(103).ok());
        EXPECT_EQ(io.sync_calls, 1u);
        ASSERT_TRUE((*writer)->Seal(104).ok());
        EXPECT_EQ(io.sync_calls, 2u);
    }
}

TEST(SegmentWriterTest, PerBatchAndIntervalPoliciesGatherBeforeSync) {
    for (const SegmentSyncPolicy policy : {SegmentSyncPolicy::kPerBatch,
                                           SegmentSyncPolicy::kInterval}) {
        SCOPED_TRACE(static_cast<int>(policy));
        IoState io;
        SegmentWriterOptions options = BufferedOptions();
        options.sync_policy = policy;
        if (policy == SegmentSyncPolicy::kInterval) {
            options.sync_interval_ns = 0;
            options.sync_interval_bytes = 1;
        }
        options.data_sync_hook = ControlledSync;
        options.io_hook_context = &io;
        auto writer = SegmentWriter::Create(
            TestPath("gather_before_sync_" +
                     std::to_string(static_cast<int>(policy))),
            SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        const std::vector<Record> records = {
            SampleRecord(1, 13), SampleRecord(2, 17), SampleRecord(3, 19)};
        ASSERT_TRUE((*writer)->AppendBatch(records, 101).ok());
        EXPECT_EQ((*writer)->durable_records(), 0u);
        ASSERT_TRUE((*writer)->Flush(102).ok());
        EXPECT_EQ((*writer)->stats().writev_syscalls, 1u);
        EXPECT_EQ((*writer)->stats().writev_buffers, records.size());
        EXPECT_EQ(io.sync_calls, 1u);
        EXPECT_EQ((*writer)->durable_records(), records.size());
        EXPECT_EQ((*writer)->durable_bytes(), (*writer)->size_bytes());
    }
}

TEST(SegmentWriterTest, IntervalPolicySupportsDeterministicTimeAndBytes) {
    {
        IoState io;
        SegmentWriterOptions options = BufferedOptions();
        options.batch_records = 1;
        options.sync_policy = SegmentSyncPolicy::kInterval;
        options.sync_interval_ns = 100;
        options.write_hook = ControlledWrite;
        options.data_sync_hook = ControlledSync;
        options.io_hook_context = &io;
        auto writer = SegmentWriter::Create(TestPath("sync_interval_time"),
                                             SampleHeader(), 1000, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_TRUE((*writer)->Append(SampleRecord(1), 1050).ok());
        EXPECT_EQ(io.sync_calls, 0u);
        ASSERT_TRUE((*writer)->Append(SampleRecord(2), 1100).ok());
        EXPECT_EQ(io.sync_calls, 1u);
        EXPECT_EQ((*writer)->durable_records(), 2u);
    }
    {
        IoState io;
        const size_t record_size = *EncodedRecordSize(3);
        SegmentWriterOptions options = BufferedOptions();
        options.batch_records = 1;
        options.sync_policy = SegmentSyncPolicy::kInterval;
        options.sync_interval_ns = 0;
        options.sync_interval_bytes =
            kEncodedSegmentHeaderSize + static_cast<uint64_t>(record_size);
        options.write_hook = ControlledWrite;
        options.data_sync_hook = ControlledSync;
        options.io_hook_context = &io;
        auto writer = SegmentWriter::Create(TestPath("sync_interval_bytes"),
                                             SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
        EXPECT_EQ(io.sync_calls, 1u);
        EXPECT_EQ((*writer)->durable_records(), 1u);
    }
}

TEST(SegmentWriterTest, PerRecordSyncsEachRecordEvenWithinOneBatch) {
    IoState io;
    SegmentWriterOptions options = BufferedOptions();
    options.sync_policy = SegmentSyncPolicy::kPerRecord;
    options.write_hook = ControlledWrite;
    options.data_sync_hook = ControlledSync;
    options.io_hook_context = &io;
    auto writer = SegmentWriter::Create(TestPath("sync_record"), SampleHeader(),
                                        100, options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2)};
    ASSERT_TRUE((*writer)->AppendBatch(records, 101).ok());
    ASSERT_TRUE((*writer)->Flush(102).ok());
    EXPECT_EQ(io.write_calls, 3u);  // Header plus one call per record.
    EXPECT_EQ((*writer)->stats().writev_syscalls, 0u);
    EXPECT_EQ(io.sync_calls, 2u);
    EXPECT_EQ((*writer)->durable_records(), 2u);
    ASSERT_TRUE((*writer)->Seal(103).ok());
    EXPECT_EQ(io.sync_calls, 3u);
}

TEST(SegmentWriterTest, BatchesRecordsWithFewerBoundedWritevSyscalls) {
    const std::filesystem::path path = TestPath("writev_batch");
    SegmentWriterOptions options = BufferedOptions();
    std::vector<Record> records;
    constexpr size_t kRecordCount = 70;
    records.reserve(kRecordCount);
    for (size_t index = 0; index < kRecordCount; ++index) {
        records.push_back(SampleRecord(index + 1, 32));
    }

    auto writer = SegmentWriter::Create(path, SampleHeader(), 100, options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();
    ASSERT_TRUE((*writer)->AppendBatch(records, 101).ok());
    ASSERT_TRUE((*writer)->Flush(102).ok());

    const SegmentWriterStats stats = (*writer)->stats();
    EXPECT_GT(stats.writev_syscalls, 0u);
    EXPECT_LT(stats.writev_syscalls, records.size());
    EXPECT_GE(stats.writev_buffers, records.size());
    EXPECT_EQ(stats.write_syscalls, stats.writev_syscalls + 1);  // Header.
    EXPECT_EQ(stats.io_bytes_written, (*writer)->size_bytes());
    EXPECT_EQ((*writer)->written_bytes(), (*writer)->size_bytes());
    EXPECT_EQ((*writer)->written_records(), records.size());
    EXPECT_EQ((*writer)->durable_records(), 0u);
}

TEST(SegmentWriterTest, WritevRetriesEintrAndShortWriteAcrossRecordBoundary) {
    const std::filesystem::path path = TestPath("writev_partial");
    VectorIoState io;
    SegmentWriterOptions options = BufferedOptions();
    options.writev_hook = ControlledWritev;
    options.io_hook_context = &io;
    auto writer = SegmentWriter::Create(path, SampleHeader(), 100, options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();

    const std::vector<Record> records = {
        SampleRecord(1, 17), SampleRecord(2, 31), SampleRecord(3, 47)};
    auto first = EncodeRecord(records[0]);
    ASSERT_TRUE(first.ok());
    io.armed = true;
    io.interrupts = 1;
    io.max_write_size = first->size() + 7;
    ASSERT_TRUE((*writer)->AppendBatch(records, 101).ok());
    ASSERT_TRUE((*writer)->Flush(102).ok());
    EXPECT_TRUE(io.crossed_buffer_boundary);
    EXPECT_GT((*writer)->stats().writev_syscalls, 2u);
    EXPECT_EQ((*writer)->written_records(), records.size());
    EXPECT_EQ((*writer)->written_bytes(), (*writer)->size_bytes());

    auto expected_header = EncodeSegmentHeader(SampleHeader());
    ASSERT_TRUE(expected_header.ok());
    std::vector<std::byte> expected = *expected_header;
    for (const Record& record : records) {
        auto encoded = EncodeRecord(record);
        ASSERT_TRUE(encoded.ok());
        AppendBytes(&expected, *encoded);
    }
    EXPECT_EQ(ReadFile(path), expected);
    EXPECT_EQ((*writer)->stats().io_bytes_written, expected.size());
}

TEST(SegmentWriterTest, WritevPartialErrorKeepsCompletedRecordCountersSticky) {
    const std::filesystem::path path = TestPath("writev_partial_error");
    VectorIoState io;
    SegmentWriterOptions options = BufferedOptions();
    options.writev_hook = ControlledWritev;
    options.io_hook_context = &io;
    auto writer = SegmentWriter::Create(path, SampleHeader(), 100, options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();

    const std::vector<Record> records = {
        SampleRecord(1, 19), SampleRecord(2, 23), SampleRecord(3, 29)};
    auto first = EncodeRecord(records[0]);
    ASSERT_TRUE(first.ok());
    io.armed = true;
    io.bytes_before_error = first->size() + 7;
    io.write_error = EIO;
    ASSERT_TRUE((*writer)->AppendBatch(records, 101).ok());
    const Status failed = (*writer)->Flush(102);
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.code(), StatusCode::kUnavailable);
    EXPECT_TRUE(io.crossed_buffer_boundary);
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kError);
    EXPECT_EQ((*writer)->written_records(), 1u);
    EXPECT_EQ((*writer)->written_bytes(),
              kEncodedSegmentHeaderSize + first->size());
    EXPECT_EQ((*writer)->durable_records(), 0u);
    EXPECT_EQ((*writer)->stats().io_bytes_written,
              kEncodedSegmentHeaderSize + first->size() + 7);
    const size_t calls_after_error = io.calls;
    EXPECT_EQ((*writer)->Flush(103), failed);
    EXPECT_EQ((*writer)->Seal(103), failed);
    EXPECT_EQ(io.calls, calls_after_error);
}

TEST(SegmentWriterTest, ReportsRecordByteAndDurationRotationSafely) {
    const uint64_t record_size = static_cast<uint64_t>(*EncodedRecordSize(3));
    {
        SegmentWriterOptions options = BufferedOptions();
        options.max_segment_records = 2;
        auto writer = SegmentWriter::Create(TestPath("rotate_records"),
                                             SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        auto first = (*writer)->Append(SampleRecord(1), 101);
        ASSERT_TRUE(first.ok());
        EXPECT_FALSE(first->rotate_needed);
        auto second = (*writer)->Append(SampleRecord(2), 102);
        ASSERT_TRUE(second.ok());
        EXPECT_EQ(second->records_accepted, 1u);
        EXPECT_TRUE(second->rotate_needed);
        EXPECT_TRUE((*writer)->rotation_needed(102));
        auto rejected = (*writer)->Append(SampleRecord(3), 103);
        ASSERT_TRUE(rejected.ok());
        EXPECT_EQ(rejected->records_accepted, 0u);
        EXPECT_TRUE(rejected->rotate_needed);
        EXPECT_EQ((*writer)->record_count(), 2u);
    }
    {
        SegmentWriterOptions options = BufferedOptions();
        options.max_segment_bytes =
            kEncodedSegmentHeaderSize + record_size * 2 - 1;
        auto writer = SegmentWriter::Create(TestPath("rotate_bytes"),
                                             SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_EQ((*writer)->Append(SampleRecord(1), 101)->records_accepted, 1u);
        auto crossing = (*writer)->Append(SampleRecord(2), 102);
        ASSERT_TRUE(crossing.ok());
        EXPECT_EQ(crossing->records_accepted, 0u);
        EXPECT_TRUE(crossing->rotate_needed);
        EXPECT_EQ((*writer)->size_bytes(),
                  kEncodedSegmentHeaderSize + record_size);
    }
    {
        SegmentWriterOptions options = BufferedOptions();
        options.max_segment_duration_ns = 50;
        auto writer = SegmentWriter::Create(TestPath("rotate_duration"),
                                             SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        auto due = (*writer)->Append(SampleRecord(1), 150);
        ASSERT_TRUE(due.ok());
        EXPECT_EQ(due->records_accepted, 0u);
        EXPECT_TRUE(due->rotate_needed);
        EXPECT_EQ((*writer)->record_count(), 0u);
    }
}

TEST(SegmentWriterTest, AppendBatchStopsAtRotationBoundary) {
    SegmentWriterOptions options = BufferedOptions();
    options.max_segment_records = 2;
    auto writer = SegmentWriter::Create(TestPath("batch_rotation"),
                                        SampleHeader(), 100, options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();
    const std::vector<Record> records = {SampleRecord(1), SampleRecord(2),
                                         SampleRecord(3)};
    auto result = (*writer)->AppendBatch(records, 101);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_EQ(result->records_accepted, 2u);
    EXPECT_TRUE(result->rotate_needed);
    EXPECT_EQ((*writer)->record_count(), 2u);
    EXPECT_GT(ReadFile((*writer)->path()).size(), kEncodedSegmentHeaderSize);
}

TEST(SegmentWriterTest, RetriesShortWritesAndPoisonsOnWriteError) {
    {
        IoState io;
        io.max_write_size = 7;
        SegmentWriterOptions options = BufferedOptions();
        options.write_hook = ControlledWrite;
        options.io_hook_context = &io;
        const std::filesystem::path path = TestPath("short_write");
        auto writer =
            SegmentWriter::Create(path, SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
        ASSERT_TRUE((*writer)->Flush(102).ok());
        auto header = EncodeSegmentHeader(SampleHeader());
        auto record = EncodeRecord(SampleRecord(1));
        ASSERT_TRUE(header.ok());
        ASSERT_TRUE(record.ok());
        std::vector<std::byte> expected = *header;
        AppendBytes(&expected, *record);
        EXPECT_EQ(ReadFile(path), expected);
        EXPECT_GT(io.write_calls, 2u);
    }
    {
        IoState io;
        SegmentWriterOptions options = BufferedOptions();
        options.write_hook = ControlledWrite;
        options.io_hook_context = &io;
        auto writer = SegmentWriter::Create(TestPath("write_error"),
                                             SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
        io.fail_write_call = io.write_calls + 1;
        const Status failed = (*writer)->Flush(102);
        ASSERT_FALSE(failed.ok());
        EXPECT_EQ(failed.code(), StatusCode::kUnavailable);
        EXPECT_EQ((*writer)->state(), SegmentWriterState::kError);
        const size_t calls_after_failure = io.write_calls;
        EXPECT_EQ((*writer)->Seal(103), failed);
        EXPECT_EQ((*writer)->Flush(103), failed);
        EXPECT_FALSE((*writer)->Append(SampleRecord(2), 103).ok());
        EXPECT_EQ(io.write_calls, calls_after_failure);
    }
    {
        IoState io;
        io.fail_write_call = 1;
        SegmentWriterOptions options = BufferedOptions();
        options.write_hook = ControlledWrite;
        options.io_hook_context = &io;
        const std::filesystem::path path = TestPath("header_write_error");
        auto writer = SegmentWriter::Create(path, SampleHeader(), 100, options);
        ASSERT_FALSE(writer.ok());
        EXPECT_EQ(writer.status().code(), StatusCode::kUnavailable);
        EXPECT_FALSE(std::filesystem::exists(path));
    }
}

TEST(SegmentWriterTest, SyncErrorPoisonsAndNeverAutomaticallyRecovers) {
    IoState io;
    io.fail_sync_call = 1;
    SegmentWriterOptions options = BufferedOptions();
    options.sync_policy = SegmentSyncPolicy::kPerBatch;
    options.write_hook = ControlledWrite;
    options.data_sync_hook = ControlledSync;
    options.io_hook_context = &io;
    auto writer = SegmentWriter::Create(TestPath("sync_error"), SampleHeader(),
                                        100, options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();
    ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
    const Status failed = (*writer)->Flush(102);
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kError);
    EXPECT_EQ((*writer)->error_status(), failed);
    EXPECT_EQ((*writer)->Flush(103), failed);
    EXPECT_EQ(io.sync_calls, 1u);
}

TEST(SegmentWriterTest, SealIsDurableIdempotentAndRejectsFurtherAppends) {
    IoState io;
    SegmentWriterOptions options = BufferedOptions();
    options.data_sync_hook = ControlledSync;
    options.io_hook_context = &io;
    auto writer = SegmentWriter::Create(TestPath("seal"), SampleHeader(), 100,
                                        options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();
    ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
    ASSERT_TRUE((*writer)->Seal(102).ok());
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kSealed);
    EXPECT_EQ((*writer)->durable_bytes(), (*writer)->size_bytes());
    EXPECT_EQ((*writer)->durable_records(), 1u);
    EXPECT_EQ(io.sync_calls, 1u);
    ASSERT_TRUE((*writer)->Seal(1).ok());
    EXPECT_EQ(io.sync_calls, 1u);
    EXPECT_TRUE((*writer)->Flush(103).ok());
    auto append = (*writer)->Append(SampleRecord(2), 103);
    ASSERT_FALSE(append.ok());
    EXPECT_EQ(append.status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kSealed);
}

TEST(SegmentWriterTest, CallerErrorsDoNotPoisonOpenWriter) {
    SegmentWriterOptions invalid_io_options;
    invalid_io_options.max_writev_bytes = 0;
    EXPECT_EQ(SegmentWriter::Create(TestPath("invalid_writev_quota"),
                                    SampleHeader(), 100, invalid_io_options)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
    invalid_io_options = {};
    invalid_io_options.write_hook = ControlledWrite;
    invalid_io_options.writev_hook = ControlledWritev;
    EXPECT_EQ(SegmentWriter::Create(TestPath("conflicting_io_hooks"),
                                    SampleHeader(), 100, invalid_io_options)
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);

    SegmentWriterOptions invalid_options;
    invalid_options.sync_policy = SegmentSyncPolicy::kInterval;
    invalid_options.sync_interval_ns = 0;
    invalid_options.sync_interval_bytes = 0;
    auto invalid = SegmentWriter::Create(TestPath("invalid_options"),
                                         SampleHeader(), 100, invalid_options);
    ASSERT_FALSE(invalid.ok());
    EXPECT_EQ(invalid.status().code(), StatusCode::kInvalidArgument);

    auto writer = SegmentWriter::Create(TestPath("caller_errors"),
                                        SampleHeader(), 100, BufferedOptions());
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();
    Record bad_flags = SampleRecord(1);
    bad_flags.header.flags = 0x8000;
    auto rejected = (*writer)->Append(bad_flags, 101);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kOpen);
    EXPECT_EQ((*writer)->Flush(99).code(), StatusCode::kInvalidArgument);
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kOpen);

    Record wrong_topic = SampleRecord(1);
    wrong_topic.header.topic_id = 12;
    auto topic_rejected = (*writer)->Append(wrong_topic, 102);
    ASSERT_FALSE(topic_rejected.ok());
    EXPECT_EQ(topic_rejected.status().code(), StatusCode::kInvalidArgument);

    auto wrong_first_sequence = (*writer)->Append(SampleRecord(2), 103);
    ASSERT_FALSE(wrong_first_sequence.ok());
    EXPECT_EQ(wrong_first_sequence.status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kOpen);

    ASSERT_TRUE((*writer)->Append(SampleRecord(1), 104).ok());
    auto duplicate_sequence = (*writer)->Append(SampleRecord(1), 105);
    ASSERT_FALSE(duplicate_sequence.ok());
    EXPECT_EQ(duplicate_sequence.status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ((*writer)->state(), SegmentWriterState::kOpen);
}

}  // namespace
}  // namespace mino::storage
