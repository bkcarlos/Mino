// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder_subscriber.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace mino {

struct RecorderSubscriberTestMessage {
    uint32_t value = 0;
};

template <>
struct StaticMessageTraits<RecorderSubscriberTestMessage> {
    static constexpr bool kIsSpecialized = true;
    static constexpr TypeId type_id{51};
    static constexpr uint32_t message_type = 51;
    static constexpr uint32_t schema_version = 7;
    static constexpr uint64_t schema_short_id = 0x0807060504030201ULL;
    static constexpr uint32_t layout_version = 3;
    static constexpr uint32_t index_flags = 0;
    static Status Validate(const RecorderSubscriberTestMessage&) noexcept {
        return Status::Ok();
    }
};

}  // namespace mino

namespace mino::storage {
namespace {

using namespace std::chrono_literals;
using TestMessage = RecorderSubscriberTestMessage;

constexpr uint64_t kSchemaShortId = 0x0807060504030201ULL;
constexpr uint32_t kSchemaVersion = 7;
constexpr uint32_t kLayoutVersion = 3;
constexpr uint32_t kMessageType = 51;

schema::CanonicalDigest TestDigest() {
    schema::CanonicalDigest digest{};
    for (size_t i = 0; i < digest.size(); ++i) {
        digest[i] = static_cast<std::byte>(i + 1);
    }
    return digest;
}

RecorderSubscriberOptions TestOptions(
    BufferFullPolicy policy = BufferFullPolicy::kBlock) {
    return RecorderSubscriberOptions{
        .topic_id = TopicId{19},
        .schema = RecorderSchemaMetadata{
            .short_id = kSchemaShortId,
            .canonical_digest = TestDigest(),
            .schema_version = kSchemaVersion,
            .layout_version = kLayoutVersion,
        },
        .full_policy = policy,
        .max_canonical_payload_bytes = 1024,
        .pending_retry_timeout = 5ms,
    };
}

RecorderRecordMetadata TestStoredMetadata(TopicId topic_id,
                                          MessageSource source,
                                          size_t payload_size) {
    return RecorderRecordMetadata{
        .schema = TestOptions().schema,
        .topic_id = topic_id,
        .source = source,
        .ingestion_timestamp_ns = 999,
        .payload_size = static_cast<uint32_t>(payload_size),
        .payload_crc = 123,
    };
}

MessageMetadata TestRuntimeMetadata() {
    return MessageMetadata{
        .message_type = kMessageType,
        .schema_version = kSchemaVersion,
        .schema_short_id = kSchemaShortId,
        .schema_layout_version = kLayoutVersion,
        .sequence_num = 1234,
        .timestamp_ns = 5678,
        .payload = ShmHandle{.offset = 64, .generation = 2, .region_id = 1},
        .payload_len = sizeof(TestMessage),
        .flags = 0,
    };
}

class FakeBorrow final : public RecorderBorrow<TestMessage> {
public:
    FakeBorrow(std::vector<std::string>* events, TestMessage value,
               MessageMetadata metadata, Status ack_status)
        : events_(events),
          value_(value),
          metadata_(metadata),
          ack_status_(std::move(ack_status)) {}

    ~FakeBorrow() override {
        if (active_) events_->push_back("implicit_ack");
    }

    const TestMessage& value() const noexcept override { return value_; }
    const MessageMetadata& metadata() const noexcept override {
        return metadata_;
    }
    bool active() const noexcept override { return active_; }
    Status Ack() noexcept override {
        events_->push_back("ack");
        if (!active_) return Status::Error(StatusCode::kInvalidArgument);
        active_ = false;
        return ack_status_;
    }

private:
    std::vector<std::string>* events_;
    TestMessage value_;
    MessageMetadata metadata_;
    Status ack_status_;
    bool active_ = true;
};

class FakeBorrowSource final : public RecorderBorrowSource<TestMessage> {
public:
    explicit FakeBorrowSource(std::vector<std::string>* events)
        : events_(events) {}

    Result<std::unique_ptr<RecorderBorrow<TestMessage>>> TryBorrow()
        noexcept override {
        events_->push_back("borrow");
        ++calls;
        try {
            auto borrow = std::make_unique<FakeBorrow>(
                events_, message, metadata, ack_status);
            return std::unique_ptr<RecorderBorrow<TestMessage>>(
                std::move(borrow));
        } catch (const std::bad_alloc&) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
    }

    TestMessage message{42};
    MessageMetadata metadata = TestRuntimeMetadata();
    Status ack_status = Status::Ok();
    size_t calls = 0;

private:
    std::vector<std::string>* events_;
};

class FakeEncoder final : public RecorderCanonicalEncoder<TestMessage> {
public:
    explicit FakeEncoder(std::vector<std::string>* events) : events_(events) {}

    Status Validate(const TestMessage& value,
                    const MessageMetadata& metadata) noexcept override {
        events_->push_back("validate");
        observed_value = value.value;
        observed_metadata = metadata;
        return validation_status;
    }

    Result<std::vector<std::byte>> Encode(
        const TestMessage& value,
        const MessageMetadata& metadata) noexcept override {
        events_->push_back("encode");
        observed_value = value.value;
        observed_metadata = metadata;
        if (!encode_status.ok()) return encode_status;
        return payload;
    }

    Status validation_status = Status::Ok();
    Status encode_status = Status::Ok();
    std::vector<std::byte> payload{
        std::byte{0x08}, std::byte{0x2a}, std::byte{0x10}, std::byte{0x01}};
    uint32_t observed_value = 0;
    MessageMetadata observed_metadata;

private:
    std::vector<std::string>* events_;
};

class FakeSourceResolver final : public RecorderSourceResolver {
public:
    explicit FakeSourceResolver(std::vector<std::string>* events)
        : events_(events) {}

    Result<MessageSource> Resolve(
        const MessageMetadata& metadata) noexcept override {
        events_->push_back("source");
        observed_metadata = metadata;
        if (!status.ok()) return status;
        return source;
    }

    Status status = Status::Ok();
    MessageSource source{
        .node_id = 11,
        .publisher_id = 22,
        .publisher_epoch = 33,
        .source_sequence = 44,
        .observed_timestamp_ns = 55,
    };
    MessageMetadata observed_metadata;

private:
    std::vector<std::string>* events_;
};

class FakeClock final : public RecorderClock {
public:
    uint64_t NowNs() noexcept override { return now_ns; }
    uint64_t now_ns = 999;
};

class FakeSink final : public RecorderBufferSink {
public:
    enum class ActionKind {
        kAccepted,
        kDropped,
        kRecordingFailed,
        kTimeout,
        kCopyFailure,
    };

    struct Action {
        ActionKind kind = ActionKind::kAccepted;
        std::vector<DiscardedBuffer> discarded;
    };

    explicit FakeSink(std::vector<std::string>* events) : events_(events) {}

    Result<RecorderCopyResult> ReserveCopyCommit(
        const RecorderCopyRequest& request) noexcept override {
        events_->push_back("reserve");
        ++calls;
        observed_timeout = request.timeout;
        observed_policy = request.full_policy;
        observed_user_tag = request.user_tag;
        if (request.metadata != nullptr) observed_metadata = *request.metadata;
        observed_payload.assign(request.payload.begin(), request.payload.end());

        Action action;
        if (!actions.empty()) {
            action = std::move(actions.front());
            actions.pop_front();
        }
        switch (action.kind) {
            case ActionKind::kAccepted:
                events_->push_back("copy");
                events_->push_back("commit");
                return RecorderCopyResult{
                    .admission = BufferAdmission::kAccepted,
                    .discarded = std::move(action.discarded),
                };
            case ActionKind::kDropped:
                return RecorderCopyResult{
                    .admission = BufferAdmission::kDroppedNewest,
                    .discarded = std::move(action.discarded),
                };
            case ActionKind::kRecordingFailed:
                return RecorderCopyResult{
                    .admission = BufferAdmission::kRecordingFailed,
                    .discarded = std::move(action.discarded),
                };
            case ActionKind::kTimeout:
                return Status::Error(StatusCode::kTimeout,
                                     "fake buffer full");
            case ActionKind::kCopyFailure:
                events_->push_back("copy");
                return Status::Error(StatusCode::kInternal,
                                     "fake copy failure");
        }
        return Status::Error(StatusCode::kInternal);
    }

    std::deque<Action> actions;
    RecorderRecordMetadata observed_metadata;
    std::vector<std::byte> observed_payload;
    std::chrono::nanoseconds observed_timeout{};
    BufferFullPolicy observed_policy = BufferFullPolicy::kBlock;
    uint64_t observed_user_tag = 0;
    size_t calls = 0;

private:
    std::vector<std::string>* events_;
};

class RecorderSubscriberTest : public ::testing::Test {
protected:
    RecorderSubscriberTest()
        : source(&events), encoder(&events), resolver(&events), sink(&events) {}

    std::unique_ptr<RecorderSubscriber<TestMessage>> Create(
        BufferFullPolicy policy = BufferFullPolicy::kBlock) {
        auto created = RecorderSubscriber<TestMessage>::Create(
            TestOptions(policy), &source, &encoder, &resolver, &sink, &clock);
        EXPECT_TRUE(created.ok()) << created.status().ToString();
        return created.ok() ? std::move(*created) : nullptr;
    }

    std::vector<std::string> events;
    FakeBorrowSource source;
    FakeEncoder encoder;
    FakeSourceResolver resolver;
    FakeSink sink;
    FakeClock clock;
};

TEST_F(RecorderSubscriberTest,
       ValidatesEncodesCopiesCommitsThenAcksAndPreservesMetadata) {
    auto subscriber = Create();
    ASSERT_NE(subscriber, nullptr);

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kBuffered);
    EXPECT_TRUE(result->status.ok());
    EXPECT_TRUE(result->source_acked());
    EXPECT_FALSE(result->pending);
    EXPECT_EQ(events, (std::vector<std::string>{
                          "borrow", "validate", "source", "encode",
                          "reserve", "copy", "commit", "ack"}));

    ASSERT_TRUE(result->metadata.has_value());
    EXPECT_EQ(result->metadata->topic_id, TopicId{19});
    EXPECT_EQ(result->metadata->schema, TestOptions().schema);
    EXPECT_EQ(result->metadata->source, resolver.source);
    EXPECT_EQ(result->metadata->ingestion_timestamp_ns, clock.now_ns);
    EXPECT_EQ(result->metadata->payload_size, encoder.payload.size());
    EXPECT_EQ(result->metadata->payload_crc,
              RecorderPayloadCrc32c(encoder.payload));
    EXPECT_EQ(sink.observed_metadata, *result->metadata);
    EXPECT_EQ(sink.observed_payload, encoder.payload);
    EXPECT_EQ(sink.observed_user_tag, resolver.source.source_sequence);
    EXPECT_EQ(resolver.observed_metadata.sequence_num,
              source.metadata.sequence_num);
}

TEST_F(RecorderSubscriberTest, ValidationFailureIsExplicitAndDoesNotEncode) {
    encoder.validation_status =
        Status::Error(StatusCode::kSchemaMismatch, "bad object");
    auto subscriber = Create();

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kFailed);
    EXPECT_EQ(result->status.code(), StatusCode::kSchemaMismatch);
    EXPECT_TRUE(result->source_acked());
    EXPECT_EQ(sink.calls, 0u);
    EXPECT_EQ(events,
              (std::vector<std::string>{"borrow", "validate", "ack"}));
}

TEST_F(RecorderSubscriberTest, EncodeFailureIsExplicitAndNeverReserves) {
    encoder.encode_status =
        Status::Error(StatusCode::kResourceExhausted, "encode bound");
    auto subscriber = Create();

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kFailed);
    EXPECT_EQ(result->status.code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(result->source_acked());
    EXPECT_EQ(sink.calls, 0u);
    EXPECT_EQ(events, (std::vector<std::string>{
                          "borrow", "validate", "source", "encode", "ack"}));
}

TEST_F(RecorderSubscriberTest, OversizedCanonicalPayloadNeverReserves) {
    encoder.payload.resize(TestOptions().max_canonical_payload_bytes + 1,
                           std::byte{0x7f});
    auto subscriber = Create();

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kFailed);
    EXPECT_EQ(result->status.code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(result->source_acked());
    EXPECT_EQ(sink.calls, 0u);
}

TEST_F(RecorderSubscriberTest, CopyFailureIsExplicitAndOccursBeforeAck) {
    sink.actions.push_back({FakeSink::ActionKind::kCopyFailure, {}});
    auto subscriber = Create(BufferFullPolicy::kDropNewest);

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kFailed);
    EXPECT_EQ(result->status.code(), StatusCode::kInternal);
    EXPECT_TRUE(result->source_acked());
    EXPECT_EQ(events, (std::vector<std::string>{
                          "borrow", "validate", "source", "encode",
                          "reserve", "copy", "ack"}));
}

TEST_F(RecorderSubscriberTest, DropNewestReportsDiscardAndAcks) {
    const DiscardedBuffer discard{
        .reason = BufferDiscardReason::kDropNewest,
        .topic_id = TopicId{19},
        .user_tag = resolver.source.source_sequence,
        .payload_size = encoder.payload.size(),
        .charged_bytes = 4096,
        .metadata = TestStoredMetadata(TopicId{19}, resolver.source,
                                       encoder.payload.size()),
    };
    sink.actions.push_back(
        {FakeSink::ActionKind::kDropped, {discard}});
    auto subscriber = Create(BufferFullPolicy::kDropNewest);

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kDropped);
    EXPECT_EQ(result->status.code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(result->source_acked());
    ASSERT_EQ(result->discarded.size(), 1u);
    EXPECT_EQ(result->discarded[0].reason,
              BufferDiscardReason::kDropNewest);
    ASSERT_TRUE(result->discarded[0].metadata.has_value());
    EXPECT_EQ(result->discarded[0].metadata->source, resolver.source);
    EXPECT_EQ(events.back(), "ack");
}

TEST_F(RecorderSubscriberTest, DropOldestReportsVictimsAndBuffersIncoming) {
    const MessageSource victim_source{
        .node_id = 101,
        .publisher_id = 102,
        .publisher_epoch = 103,
        .source_sequence = 88,
        .observed_timestamp_ns = 104,
    };
    const DiscardedBuffer victim{
        .reason = BufferDiscardReason::kDropOldest,
        .topic_id = TopicId{7},
        .user_tag = 88,
        .payload_size = 10,
        .charged_bytes = 4096,
        .metadata = TestStoredMetadata(TopicId{7}, victim_source, 10),
    };
    sink.actions.push_back(
        {FakeSink::ActionKind::kAccepted, {victim}});
    auto subscriber = Create(BufferFullPolicy::kDropOldest);

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kBuffered);
    EXPECT_TRUE(result->source_acked());
    ASSERT_EQ(result->discarded.size(), 1u);
    EXPECT_EQ(result->discarded[0].reason,
              BufferDiscardReason::kDropOldest);
    ASSERT_TRUE(result->discarded[0].metadata.has_value());
    EXPECT_EQ(result->discarded[0].metadata->source, victim_source);
}

TEST_F(RecorderSubscriberTest, FailRecordingReportsFailureAndDiscard) {
    const DiscardedBuffer discard{
        .reason = BufferDiscardReason::kFailRecording,
        .topic_id = TopicId{19},
        .user_tag = resolver.source.source_sequence,
        .payload_size = encoder.payload.size(),
        .charged_bytes = 4096,
        .metadata = TestStoredMetadata(TopicId{19}, resolver.source,
                                       encoder.payload.size()),
    };
    sink.actions.push_back(
        {FakeSink::ActionKind::kRecordingFailed, {discard}});
    auto subscriber = Create(BufferFullPolicy::kFailRecording);

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kFailed);
    EXPECT_EQ(result->status.code(), StatusCode::kUnavailable);
    EXPECT_TRUE(result->source_acked());
    ASSERT_EQ(result->discarded.size(), 1u);
    EXPECT_EQ(result->discarded[0].reason,
              BufferDiscardReason::kFailRecording);
    ASSERT_TRUE(result->discarded[0].metadata.has_value());
    EXPECT_EQ(result->discarded[0].metadata->source, resolver.source);
}

TEST_F(RecorderSubscriberTest,
       BlockStagesBoundedBytesAcksThenFlushesWithoutAnotherBorrow) {
    sink.actions.push_back({FakeSink::ActionKind::kTimeout, {}});
    sink.actions.push_back({FakeSink::ActionKind::kAccepted, {}});
    auto subscriber = Create(BufferFullPolicy::kBlock);

    auto blocked = subscriber->TryRecord();
    ASSERT_TRUE(blocked.ok());
    EXPECT_EQ(blocked->disposition, RecorderRecordDisposition::kBlocked);
    EXPECT_EQ(blocked->status.code(), StatusCode::kTimeout);
    EXPECT_TRUE(blocked->source_acked());
    EXPECT_TRUE(blocked->pending);
    EXPECT_TRUE(subscriber->has_pending());
    EXPECT_EQ(subscriber->pending_bytes(), encoder.payload.size());
    EXPECT_EQ(sink.observed_timeout, 0ns);
    EXPECT_EQ(events, (std::vector<std::string>{
                          "borrow", "validate", "source", "encode",
                          "reserve", "ack"}));

    events.clear();
    auto flushed = subscriber->TryRecord();
    ASSERT_TRUE(flushed.ok());
    EXPECT_EQ(flushed->disposition, RecorderRecordDisposition::kBuffered);
    EXPECT_TRUE(flushed->source_acked());
    EXPECT_FALSE(flushed->pending);
    EXPECT_FALSE(subscriber->has_pending());
    EXPECT_EQ(source.calls, 1u);
    EXPECT_EQ(sink.observed_timeout, 5ms);
    EXPECT_EQ(events,
              (std::vector<std::string>{"reserve", "copy", "commit"}));
}

TEST_F(RecorderSubscriberTest,
       BlockedPendingRecordPreservesPriorAckFailureAcrossFlush) {
    source.ack_status =
        Status::Error(StatusCode::kUnavailable, "ack failed");
    sink.actions.push_back({FakeSink::ActionKind::kTimeout, {}});
    sink.actions.push_back({FakeSink::ActionKind::kAccepted, {}});
    auto subscriber = Create(BufferFullPolicy::kBlock);

    auto blocked = subscriber->TryRecord();
    ASSERT_TRUE(blocked.ok());
    EXPECT_EQ(blocked->disposition, RecorderRecordDisposition::kFailed);
    EXPECT_TRUE(blocked->pending);
    EXPECT_EQ(blocked->ack_status.code(), StatusCode::kUnavailable);

    auto flushed = subscriber->TryRecord();
    ASSERT_TRUE(flushed.ok());
    EXPECT_EQ(flushed->disposition, RecorderRecordDisposition::kFailed);
    EXPECT_FALSE(flushed->pending);
    EXPECT_EQ(flushed->ack_status.code(), StatusCode::kUnavailable);
    EXPECT_FALSE(flushed->source_acked());
    EXPECT_EQ(source.calls, 1u);
}

TEST_F(RecorderSubscriberTest, AckFailureIsReportedAfterSuccessfulCommit) {
    source.ack_status =
        Status::Error(StatusCode::kUnavailable, "ack failed");
    auto subscriber = Create();

    auto result = subscriber->TryRecord();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->disposition, RecorderRecordDisposition::kFailed);
    EXPECT_TRUE(result->status.ok());
    EXPECT_EQ(result->ack_status.code(), StatusCode::kUnavailable);
    EXPECT_FALSE(result->source_acked());
    EXPECT_EQ(events, (std::vector<std::string>{
                          "borrow", "validate", "source", "encode",
                          "reserve", "copy", "commit", "ack"}));
}

TEST(RecorderSubscriberOptionsTest, RejectsUnboundedOrIncompleteConfiguration) {
    auto options = TestOptions();
    options.topic_id = TopicId{};
    EXPECT_EQ(ValidateRecorderSubscriberOptions(options).code(),
              StatusCode::kInvalidArgument);

    options = TestOptions();
    options.max_canonical_payload_bytes = 0;
    EXPECT_EQ(ValidateRecorderSubscriberOptions(options).code(),
              StatusCode::kInvalidArgument);

    options = TestOptions();
    options.pending_retry_timeout = std::chrono::minutes(2);
    EXPECT_EQ(ValidateRecorderSubscriberOptions(options).code(),
              StatusCode::kInvalidArgument);
}

TEST(RecorderBufferPoolSinkTest,
     CopiesCanonicalBytesAndPreservesMetadataAndDropIdentity) {
    RecorderBufferPoolOptions pool_options;
    pool_options.global_byte_limit = 4096;
    pool_options.default_topic_byte_limit = 4096;
    pool_options.queue_capacity = 1;
    auto created = RecorderBufferPool::Create(pool_options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    RecorderBufferPoolSink sink(**created);

    const std::vector<std::byte> payload{
        std::byte{0x08}, std::byte{0x96}, std::byte{0x01}};
    const RecorderRecordMetadata metadata{
        .schema = TestOptions().schema,
        .topic_id = TopicId{19},
        .source = MessageSource{1, 2, 3, 77, 88},
        .ingestion_timestamp_ns = 99,
        .payload_size = static_cast<uint32_t>(payload.size()),
        .payload_crc = RecorderPayloadCrc32c(payload),
    };
    auto copied = sink.ReserveCopyCommit(RecorderCopyRequest{
        .metadata = &metadata,
        .payload = payload,
        .full_policy = BufferFullPolicy::kBlock,
        .timeout = 0ns,
        .user_tag = 77,
    });
    ASSERT_TRUE(copied.ok()) << copied.status().ToString();
    EXPECT_EQ(copied->admission, BufferAdmission::kAccepted);

    RecorderRecordMetadata dropped_metadata = metadata;
    dropped_metadata.source.source_sequence = 78;
    dropped_metadata.source.observed_timestamp_ns = 89;
    dropped_metadata.ingestion_timestamp_ns = 100;
    auto dropped = sink.ReserveCopyCommit(RecorderCopyRequest{
        .metadata = &dropped_metadata,
        .payload = payload,
        .full_policy = BufferFullPolicy::kDropNewest,
        .timeout = 0ns,
        .user_tag = 78,
    });
    ASSERT_TRUE(dropped.ok()) << dropped.status().ToString();
    EXPECT_EQ(dropped->admission, BufferAdmission::kDroppedNewest);
    ASSERT_EQ(dropped->discarded.size(), 1u);
    ASSERT_TRUE(dropped->discarded[0].metadata.has_value());
    EXPECT_EQ(*dropped->discarded[0].metadata, dropped_metadata);
    EXPECT_EQ(dropped->discarded[0].metadata->source.node_id, 1u);
    EXPECT_EQ(dropped->discarded[0].metadata->source.publisher_id, 2u);
    EXPECT_EQ(dropped->discarded[0].metadata->source.publisher_epoch, 3u);
    EXPECT_EQ(dropped->discarded[0].metadata->source.source_sequence, 78u);

    auto dequeued = (*created)->TryDequeue();
    ASSERT_TRUE(dequeued.ok()) << dequeued.status().ToString();
    EXPECT_EQ(dequeued->topic_id(), TopicId{19});
    EXPECT_EQ(dequeued->user_tag(), 77u);
    ASSERT_TRUE(dequeued->metadata().has_value());
    EXPECT_EQ(*dequeued->metadata(), metadata);
    EXPECT_EQ(std::vector<std::byte>(dequeued->bytes().begin(),
                                     dequeued->bytes().end()),
              payload);
}

TEST(RecorderPayloadCrc32cTest, MatchesStandardCheckVector) {
    const std::array<std::byte, 9> input = {
        std::byte{'1'}, std::byte{'2'}, std::byte{'3'},
        std::byte{'4'}, std::byte{'5'}, std::byte{'6'},
        std::byte{'7'}, std::byte{'8'}, std::byte{'9'},
    };
    EXPECT_EQ(RecorderPayloadCrc32c(input), 0xe3069283u);
}

}  // namespace
}  // namespace mino::storage
