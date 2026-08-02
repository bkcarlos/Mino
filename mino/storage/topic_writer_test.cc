// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/topic_writer.h"

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
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/status.h"
#include "mino/storage/segment_format.h"

namespace mino::storage {
namespace {

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_topic_writer_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path);
    return path;
}

uint32_t Crc32c(std::span<const std::byte> bytes) {
    uint32_t state = 0xffffffffu;
    for (std::byte byte : bytes) {
        state ^= static_cast<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                    ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

uint64_t ReadLe64(std::span<const std::byte> bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value |= static_cast<uint64_t>(
                     static_cast<uint8_t>(bytes[offset + index]))
                 << (index * 8);
    }
    return value;
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

std::vector<Record> ReadRecords(const std::filesystem::path& path) {
    const std::vector<std::byte> bytes = ReadFile(path);
    EXPECT_GE(bytes.size(), kEncodedSegmentHeaderSize);
    std::vector<Record> records;
    size_t offset = kEncodedSegmentHeaderSize;
    while (offset < bytes.size()) {
        if (offset + kRecordLengthFieldSize > bytes.size()) {
            ADD_FAILURE() << "truncated record length";
            return records;
        }
        const uint64_t body_size = ReadLe64(bytes, offset);
        if (body_size > bytes.size() - offset - kRecordLengthFieldSize) {
            ADD_FAILURE() << "record length exceeds file";
            return records;
        }
        const size_t record_size =
            kRecordLengthFieldSize + static_cast<size_t>(body_size);
        auto decoded = DecodeRecord(
            std::span<const std::byte>(bytes).subspan(offset, record_size));
        EXPECT_TRUE(decoded.ok()) << decoded.status().ToString();
        if (!decoded.ok()) break;
        records.push_back(std::move(*decoded));
        offset += record_size;
    }
    EXPECT_EQ(offset, bytes.size());
    return records;
}

RecorderSchemaMetadata Schema(uint64_t short_id = 1) {
    RecorderSchemaMetadata schema;
    schema.short_id = short_id;
    for (size_t index = 0; index < schema.canonical_digest.size(); ++index) {
        schema.canonical_digest[index] =
            static_cast<std::byte>(short_id + index);
    }
    schema.schema_version = 0x00010002u;
    schema.layout_version = 3;
    return schema;
}

RecorderRecordMetadata Metadata(uint64_t publisher_id, uint64_t source_sequence,
                                std::span<const std::byte> payload,
                                uint64_t short_id = 1) {
    return RecorderRecordMetadata{
        .schema = Schema(short_id),
        .topic_id = TopicId{10},
        .source = MessageSource{
            .node_id = 7,
            .publisher_id = publisher_id,
            .publisher_epoch = 3,
            .source_sequence = source_sequence,
            .observed_timestamp_ns = 1000 + source_sequence,
        },
        .ingestion_timestamp_ns = 2000 + source_sequence,
        .payload_size = static_cast<uint32_t>(payload.size()),
        .payload_crc = Crc32c(payload),
    };
}

std::vector<std::byte> Payload(uint8_t value) {
    return {static_cast<std::byte>(value),
            static_cast<std::byte>(value + 1)};
}

struct ResolverState {
    uint64_t accepted_short_id = 1;
    size_t calls = 0;
};

Result<SchemaRef> ResolveSchema(const RecorderSchemaMetadata& schema,
                                void* context) noexcept {
    auto* state = static_cast<ResolverState*>(context);
    ++state->calls;
    if (schema.short_id != state->accepted_short_id ||
        schema != Schema(state->accepted_short_id)) {
        return Status::Error(StatusCode::kNotFound, "unknown test schema");
    }
    return SchemaRef{9};
}

struct AckState {
    TopicWriter* writer = nullptr;
    std::vector<TopicWriterAck> acks;
    bool callback_observed_unlocked_writer = false;
};

void CaptureAck(const TopicWriterAck& ack, void* context) noexcept {
    auto* state = static_cast<AckState*>(context);
    state->acks.push_back(ack);
    if (state->writer != nullptr) {
        static_cast<void>(state->writer->state());
        state->callback_observed_unlocked_writer = true;
    }
}

struct WriterFixture {
    std::filesystem::path root;
    std::unique_ptr<RecorderBufferPool> pool;
    std::unique_ptr<PartitionManifest> manifest;
    ResolverState resolver;
    AckState ack;
    std::unique_ptr<TopicWriter> writer;

    explicit WriterFixture(std::string_view name,
                           SegmentWriterOptions segment_options = {})
        : root(TestDirectory(name)) {
        RecorderBufferPoolOptions pool_options;
        pool_options.global_byte_limit = 64u * 1024u;
        pool_options.default_topic_byte_limit = pool_options.global_byte_limit;
        pool_options.queue_capacity = 64;
        auto created_pool = RecorderBufferPool::Create(pool_options);
        EXPECT_TRUE(created_pool.ok()) << created_pool.status().ToString();
        if (created_pool.ok()) pool = std::move(*created_pool);

        PartitionMetadata partition{
            .recording_id = 100,
            .topic_id = 10,
            .partition_id = 0,
            .writer_id = 44,
            .owner_epoch = 2,
            .config_version = 1,
        };
        auto created_manifest = PartitionManifest::Create(root, partition);
        EXPECT_TRUE(created_manifest.ok())
            << created_manifest.status().ToString();
        if (created_manifest.ok()) manifest = std::move(*created_manifest);

        TopicWriterOptions options;
        options.partition_root = root;
        options.recording_id = 100;
        options.topic_id = TopicId{10};
        options.partition_id = 0;
        options.writer_id = 44;
        options.segment_options = segment_options;
        options.schema_resolver = ResolveSchema;
        options.schema_resolver_context = &resolver;
        options.ack_callback = CaptureAck;
        options.ack_context = &ack;
        auto created_writer = TopicWriter::Create(
            std::move(options), pool.get(), manifest.get());
        EXPECT_TRUE(created_writer.ok()) << created_writer.status().ToString();
        if (created_writer.ok()) {
            writer = std::move(*created_writer);
            ack.writer = writer.get();
        }
    }

    void Enqueue(uint64_t publisher_id, uint64_t source_sequence,
                 const std::vector<std::byte>& payload, uint64_t user_tag = 0,
                 uint64_t short_id = 1, bool corrupt_crc = false) {
        RecorderRecordMetadata metadata =
            Metadata(publisher_id, source_sequence, payload, short_id);
        if (corrupt_crc) metadata.payload_crc ^= 1u;
        BufferReservationRequest request;
        request.topic_id = TopicId{10};
        request.payload_size = payload.size();
        request.user_tag = user_tag;
        request.metadata = metadata;
        auto reserved = pool->Reserve(request);
        ASSERT_TRUE(reserved.ok()) << reserved.status().ToString();
        ASSERT_TRUE(reserved->accepted());
        std::copy(payload.begin(), payload.end(),
                  reserved->reservation.bytes().begin());
        ASSERT_TRUE(std::move(reserved->reservation).Commit().ok());
    }
};

TEST(TopicWriterGapCodecTest, UsesCanonicalLittleEndianEncodingAndBounds) {
    const GapPayload payload{
        .reason = GapReason::kDropOldest,
        .first_missing_source_sequence = 0x0102030405060708ull,
        .last_missing_source_sequence = 0x1112131415161718ull,
    };
    auto encoded = EncodeGapPayload(payload);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    ASSERT_EQ(encoded->size(), kEncodedGapPayloadSize);
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[0]), 0x4du);
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[1]), 0x47u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[2]), 0x41u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[3]), 0x50u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[4]), 1u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[6]),
              static_cast<uint8_t>(GapReason::kDropOldest));
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[8]), 0x08u);
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[15]), 0x01u);
    auto decoded = DecodeGapPayload(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, payload);

    const GapPayload recovery_gap{
        .reason = GapReason::kRecorderRestartRecovery,
        .first_missing_source_sequence = 19,
        .last_missing_source_sequence = 19,
    };
    auto recovery_encoded = EncodeGapPayload(recovery_gap);
    ASSERT_TRUE(recovery_encoded.ok())
        << recovery_encoded.status().ToString();
    EXPECT_EQ(*DecodeGapPayload(*recovery_encoded), recovery_gap);

    encoded->pop_back();
    EXPECT_EQ(DecodeGapPayload(*encoded).status().code(),
              StatusCode::kCorruption);
    EXPECT_EQ(EncodeGapPayload(
                  GapPayload{GapReason::kSourceSequenceJump, 9, 8})
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);
}

TEST(TopicWriterTest,
     HandlesMultiplePublishersDuplicateReportedGapTombstoneAndAcks) {
    SegmentWriterOptions segment_options;
    segment_options.batch_bytes = 0;
    segment_options.batch_records = 0;
    segment_options.flush_interval_ns = 0;
    WriterFixture fixture("ordering", segment_options);
    ASSERT_NE(fixture.writer, nullptr);
    ASSERT_TRUE(fixture.writer->Start(100).ok());

    const std::vector<std::byte> a1 = Payload(1);
    const std::vector<std::byte> b10 = Payload(10);
    const std::vector<std::byte> duplicate = Payload(1);
    const std::vector<std::byte> a3 = Payload(3);
    fixture.Enqueue(101, 1, a1, 11);
    fixture.Enqueue(202, 10, b10, 12);
    fixture.Enqueue(101, 1, duplicate, 13);

    const RecorderRecordMetadata dropped = Metadata(101, 2, Payload(2));
    ASSERT_TRUE(fixture.writer
                    ->ReportDiscard(DiscardedBuffer{
                        .reason = BufferDiscardReason::kReservationCancelled,
                        .topic_id = TopicId{10},
                        .user_tag = 99,
                        .payload_size = 2,
                        .charged_bytes = 4096,
                        .metadata = dropped,
                    })
                    .ok());
    fixture.Enqueue(101, 3, a3,
                    kTopicWriterTombstoneUserTagMask | 14u);

    auto pumped = fixture.writer->Pump(101);
    ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
    EXPECT_EQ(pumped->dequeued_records, 4u);
    EXPECT_EQ(pumped->data_records, 2u);
    EXPECT_EQ(pumped->tombstone_records, 1u);
    EXPECT_EQ(pumped->gap_records, 1u);
    EXPECT_EQ(pumped->duplicate_records, 1u);
    EXPECT_EQ(fixture.writer->duplicate_count(), 1u);
    EXPECT_EQ(fixture.writer->next_ingestion_sequence(), 5u);
    ASSERT_TRUE(fixture.ack.callback_observed_unlocked_writer);
    ASSERT_EQ(fixture.ack.acks.size(), 4u);
    for (size_t index = 0; index < fixture.ack.acks.size(); ++index) {
        EXPECT_EQ(fixture.ack.acks[index].level, RecordAckLevel::kWritten);
        EXPECT_EQ(fixture.ack.acks[index].ingestion_sequence, index + 1);
    }
    EXPECT_EQ(fixture.ack.acks[3].record_flags, kRecordFlagTombstone);
    EXPECT_EQ(fixture.ack.acks[3].user_tag, 14u);

    const auto& segments = fixture.manifest->snapshot().segments;
    ASSERT_EQ(segments.size(), 1u);
    EXPECT_EQ(segments[0].state, SegmentPersistentState::kOpen);
    EXPECT_EQ(segments[0].first_ingestion_sequence, 1u);
    EXPECT_EQ(segments[0].last_ingestion_sequence, 4u);
    const std::vector<Record> records =
        ReadRecords(fixture.root / segments[0].relative_path);
    ASSERT_EQ(records.size(), 4u);
    EXPECT_EQ(records[0].header.publisher_id, 101u);
    EXPECT_EQ(records[0].header.ingestion_sequence, 1u);
    EXPECT_EQ(records[1].header.publisher_id, 202u);
    EXPECT_EQ(records[1].header.ingestion_sequence, 2u);
    EXPECT_EQ(records[2].header.flags, kRecordFlagGap);
    EXPECT_EQ(records[2].header.ingestion_sequence, 3u);
    auto gap = DecodeGapPayload(records[2].payload);
    ASSERT_TRUE(gap.ok()) << gap.status().ToString();
    EXPECT_EQ(*gap, (GapPayload{GapReason::kReservationCancelled, 2, 2}));
    EXPECT_EQ(records[3].header.flags, kRecordFlagTombstone);
    EXPECT_EQ(records[3].header.ingestion_sequence, 4u);
    EXPECT_EQ(records[3].header.schema_ref, 9u);

    ASSERT_TRUE(
        fixture.writer->Flush(RecordAckLevel::kDurable, 102).ok());
    ASSERT_EQ(fixture.ack.acks.size(), 8u);
    for (size_t index = 4; index < fixture.ack.acks.size(); ++index) {
        EXPECT_EQ(fixture.ack.acks[index].level, RecordAckLevel::kDurable);
        EXPECT_EQ(fixture.ack.acks[index].ingestion_sequence, index - 3);
    }
    ASSERT_EQ(fixture.manifest->snapshot().segments.size(), 1u);
    EXPECT_EQ(fixture.manifest->snapshot().segments[0].state,
              SegmentPersistentState::kSealed);
    ASSERT_TRUE(fixture.manifest->snapshot().checkpoint.has_value());
    EXPECT_EQ(fixture.manifest->snapshot().checkpoint->durable_sequence, 4u);
    EXPECT_EQ(fixture.manifest->snapshot().checkpoint->segment_id, 1u);

    fixture.Enqueue(202, 11, Payload(11), 15);
    ASSERT_TRUE(fixture.writer->Stop(103).ok());
    EXPECT_EQ(fixture.writer->state(), TopicWriterState::kStopped);
    ASSERT_EQ(fixture.manifest->snapshot().segments.size(), 2u);
    EXPECT_EQ(fixture.manifest->snapshot().segments[1].state,
              SegmentPersistentState::kSealed);
    EXPECT_EQ(fixture.manifest->snapshot().segments[1].first_ingestion_sequence,
              5u);
    EXPECT_EQ(fixture.manifest->snapshot().checkpoint->durable_sequence, 5u);
}

TEST(TopicWriterTest, RotatesSegmentsAndPersistsManifestProgress) {
    SegmentWriterOptions segment_options;
    segment_options.batch_bytes = 0;
    segment_options.batch_records = 0;
    segment_options.flush_interval_ns = 0;
    segment_options.max_segment_records = 2;
    WriterFixture fixture("rotation", segment_options);
    ASSERT_NE(fixture.writer, nullptr);
    ASSERT_TRUE(fixture.writer->Start(100).ok());
    for (uint64_t sequence = 1; sequence <= 5; ++sequence) {
        fixture.Enqueue(300, sequence,
                        Payload(static_cast<uint8_t>(sequence)), sequence);
    }

    auto pumped = fixture.writer->Pump(101);
    ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
    EXPECT_EQ(pumped->data_records, 5u);
    EXPECT_EQ(pumped->rotations, 2u);
    const auto& open_snapshot = fixture.manifest->snapshot();
    ASSERT_EQ(open_snapshot.segments.size(), 3u);
    EXPECT_EQ(open_snapshot.segments[0].state,
              SegmentPersistentState::kSealed);
    EXPECT_EQ(open_snapshot.segments[1].state,
              SegmentPersistentState::kSealed);
    EXPECT_EQ(open_snapshot.segments[2].state, SegmentPersistentState::kOpen);
    EXPECT_EQ(open_snapshot.segments[0].first_ingestion_sequence, 1u);
    EXPECT_EQ(open_snapshot.segments[0].last_ingestion_sequence, 2u);
    EXPECT_EQ(open_snapshot.segments[1].first_ingestion_sequence, 3u);
    EXPECT_EQ(open_snapshot.segments[1].last_ingestion_sequence, 4u);
    EXPECT_EQ(open_snapshot.segments[2].first_ingestion_sequence, 5u);
    EXPECT_EQ(open_snapshot.segments[2].last_ingestion_sequence, 5u);
    ASSERT_TRUE(open_snapshot.checkpoint.has_value());
    EXPECT_EQ(open_snapshot.checkpoint->durable_sequence, 4u);

    ASSERT_TRUE(fixture.writer->Stop(102).ok());
    const auto& sealed_snapshot = fixture.manifest->snapshot();
    ASSERT_EQ(sealed_snapshot.segments.size(), 3u);
    EXPECT_TRUE(std::all_of(
        sealed_snapshot.segments.begin(), sealed_snapshot.segments.end(),
        [](const SegmentManifestEntry& segment) {
            return segment.state == SegmentPersistentState::kSealed;
        }));
    ASSERT_TRUE(sealed_snapshot.checkpoint.has_value());
    EXPECT_EQ(sealed_snapshot.checkpoint->durable_sequence, 5u);
    EXPECT_EQ(sealed_snapshot.checkpoint->segment_id, 3u);
}

TEST(TopicWriterTest, UnknownSchemaAndPayloadCrcFailureAreStickyWithoutSequence) {
    {
        WriterFixture fixture("unknown_schema");
        ASSERT_NE(fixture.writer, nullptr);
        ASSERT_TRUE(fixture.writer->Start(100).ok());
        fixture.Enqueue(1, 1, Payload(1), 0, 999);
        auto failed = fixture.writer->Pump(101);
        ASSERT_FALSE(failed.ok());
        EXPECT_EQ(failed.status().code(), StatusCode::kNotFound);
        EXPECT_EQ(fixture.writer->state(), TopicWriterState::kError);
        EXPECT_EQ(fixture.writer->next_ingestion_sequence(), 1u);
        EXPECT_EQ(fixture.writer->Flush(RecordAckLevel::kWritten, 102),
                  failed.status());
        EXPECT_EQ(fixture.writer->Stop(102), failed.status());
        EXPECT_TRUE(fixture.manifest->snapshot().segments.empty());
    }
    {
        WriterFixture fixture("crc_error");
        ASSERT_NE(fixture.writer, nullptr);
        ASSERT_TRUE(fixture.writer->Start(100).ok());
        fixture.Enqueue(1, 1, Payload(1), 0, 1, true);
        auto failed = fixture.writer->Pump(101);
        ASSERT_FALSE(failed.ok());
        EXPECT_EQ(failed.status().code(), StatusCode::kCorruption);
        EXPECT_EQ(fixture.writer->state(), TopicWriterState::kError);
        EXPECT_EQ(fixture.writer->next_ingestion_sequence(), 1u);
        EXPECT_TRUE(fixture.manifest->snapshot().segments.empty());
    }
}

struct FailingIo {
    size_t writes = 0;
    size_t fail_write = 2;
};

std::ptrdiff_t FailSecondWrite(int fd, const std::byte* data, size_t size,
                               void* context) noexcept {
    auto* io = static_cast<FailingIo*>(context);
    ++io->writes;
    if (io->writes == io->fail_write) {
        errno = EIO;
        return -1;
    }
    return static_cast<std::ptrdiff_t>(::write(fd, data, size));
}

TEST(TopicWriterTest, SegmentWriteFailurePoisonsWriterPermanently) {
    FailingIo io;
    SegmentWriterOptions segment_options;
    segment_options.batch_bytes = 0;
    segment_options.batch_records = 0;
    segment_options.flush_interval_ns = 0;
    segment_options.write_hook = FailSecondWrite;
    segment_options.io_hook_context = &io;
    WriterFixture fixture("writer_error", segment_options);
    ASSERT_NE(fixture.writer, nullptr);
    ASSERT_TRUE(fixture.writer->Start(100).ok());
    fixture.Enqueue(1, 1, Payload(1));

    auto failed = fixture.writer->Pump(101);
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.status().code(), StatusCode::kUnavailable);
    EXPECT_EQ(fixture.writer->state(), TopicWriterState::kError);
    EXPECT_EQ(fixture.writer->error_status(), failed.status());
    const size_t writes_after_failure = io.writes;
    EXPECT_EQ(fixture.writer->Pump(102).status(), failed.status());
    EXPECT_EQ(fixture.writer->Flush(RecordAckLevel::kDurable, 102),
              failed.status());
    EXPECT_EQ(fixture.writer->Stop(102), failed.status());
    EXPECT_EQ(io.writes, writes_after_failure);
    ASSERT_EQ(fixture.manifest->snapshot().segments.size(), 1u);
    EXPECT_EQ(fixture.manifest->snapshot().segments[0].state,
              SegmentPersistentState::kOpen);
}

}  // namespace
}  // namespace mino::storage
