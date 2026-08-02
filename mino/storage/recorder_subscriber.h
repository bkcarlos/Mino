// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RECORDER_SUBSCRIBER_H_
#define MINO_STORAGE_RECORDER_SUBSCRIBER_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/runtime/message.h"
#include "mino/runtime/message_traits.h"
#include "mino/runtime/subscriber.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/dynamic_object.h"
#include "mino/schema/wire.h"
#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/recording_types.h"

namespace mino::storage {

enum class RecorderRecordDisposition : uint8_t {
    kBuffered,
    kDropped,
    kBlocked,
    kFailed,
};

struct RecorderRecordResult {
    RecorderRecordDisposition disposition = RecorderRecordDisposition::kFailed;
    // The pipeline or policy result. ACK has its own status so an ACK failure
    // never hides an encode/copy/drop decision.
    Status status = Status::Ok();
    Status ack_status = Status::Ok();
    bool ack_attempted = false;
    bool pending = false;
    std::optional<RecorderRecordMetadata> metadata;
    std::vector<DiscardedBuffer> discarded;

    bool source_acked() const noexcept {
        return ack_attempted && ack_status.ok();
    }
};

struct RecorderSubscriberOptions {
    TopicId topic_id{};
    RecorderSchemaMetadata schema;
    BufferFullPolicy full_policy = BufferFullPolicy::kBlock;
    size_t max_canonical_payload_bytes = 16u * 1024u * 1024u;

    // Used only while flushing recorder-owned pending bytes. Admission while a
    // SHM Borrow is active is always attempted with a zero timeout.
    std::chrono::nanoseconds pending_retry_timeout =
        std::chrono::milliseconds(100);
};

Status ValidateRecorderSubscriberOptions(
    const RecorderSubscriberOptions& options) noexcept;
uint32_t RecorderPayloadCrc32c(std::span<const std::byte> payload) noexcept;

class RecorderClock {
public:
    virtual ~RecorderClock() = default;
    virtual uint64_t NowNs() noexcept = 0;
};

class SystemRecorderClock final : public RecorderClock {
public:
    uint64_t NowNs() noexcept override;
};

class RecorderSourceResolver {
public:
    virtual ~RecorderSourceResolver() = default;
    virtual Result<MessageSource> Resolve(
        const MessageMetadata& metadata) noexcept = 0;
};

// Convenience resolver for a Subscriber known to carry exactly one publisher
// identity. Multi-publisher topics must inject a resolver that obtains the true
// identity from their transport/runtime side metadata; identities are never
// guessed by RecorderSubscriber.
class FixedRecorderSourceResolver final : public RecorderSourceResolver {
public:
    FixedRecorderSourceResolver(uint64_t node_id, uint64_t publisher_id,
                                uint64_t publisher_epoch) noexcept
        : node_id_(node_id),
          publisher_id_(publisher_id),
          publisher_epoch_(publisher_epoch) {}

    Result<MessageSource> Resolve(
        const MessageMetadata& metadata) noexcept override;

private:
    uint64_t node_id_ = 0;
    uint64_t publisher_id_ = 0;
    uint64_t publisher_epoch_ = 0;
};

struct RecorderCopyRequest {
    const RecorderRecordMetadata* metadata = nullptr;
    std::span<const std::byte> payload;
    BufferFullPolicy full_policy = BufferFullPolicy::kBlock;
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::zero();
    uint64_t user_tag = 0;
};

struct RecorderCopyResult {
    BufferAdmission admission = BufferAdmission::kAccepted;
    std::vector<DiscardedBuffer> discarded;
};

// Test seam around the indivisible recorder-memory operation. Implementations
// must reserve, copy every byte, and commit in that order. They must return an
// error if copying or commit fails; an accepted result means ownership of all
// canonical bytes has transferred to the sink.
class RecorderBufferSink {
public:
    virtual ~RecorderBufferSink() = default;
    virtual Result<RecorderCopyResult> ReserveCopyCommit(
        const RecorderCopyRequest& request) noexcept = 0;
};

class RecorderBufferPoolSink final : public RecorderBufferSink {
public:
    explicit RecorderBufferPoolSink(RecorderBufferPool& pool) noexcept
        : pool_(&pool) {}

    Result<RecorderCopyResult> ReserveCopyCommit(
        const RecorderCopyRequest& request) noexcept override;

private:
    RecorderBufferPool* pool_ = nullptr;
};

template <typename T>
class RecorderCanonicalEncoder {
public:
    virtual ~RecorderCanonicalEncoder() = default;
    virtual Status Validate(const T& value,
                            const MessageMetadata& metadata) noexcept = 0;
    virtual Result<std::vector<std::byte>> Encode(
        const T& value, const MessageMetadata& metadata) noexcept = 0;
};

// Adapter for the Canonical Wire adapter emitted by minoc.
template <typename T, typename WireAdapter>
class GeneratedRecorderCanonicalEncoder final
    : public RecorderCanonicalEncoder<T> {
public:
    explicit GeneratedRecorderCanonicalEncoder(
        schema::WireLimits limits = {}) noexcept
        : limits_(limits) {}

    Status Validate(const T& value,
                    const MessageMetadata& metadata) noexcept override {
        static_cast<void>(metadata);
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        return StaticMessageTraits<T>::Validate(value);
    }

    Result<std::vector<std::byte>> Encode(
        const T& value,
        const MessageMetadata& metadata) noexcept override {
        static_cast<void>(metadata);
        return WireAdapter::Encode(value, limits_);
    }

private:
    schema::WireLimits limits_;
};

// Adapter for generated objects whose canonical form traverses child Slabs.
// It takes a temporary root Pin while the Borrow is valid, and the generated
// graph encoder consumes/releases that Pin before this method returns.
template <typename T, typename WireAdapter>
class GeneratedGraphRecorderCanonicalEncoder final
    : public RecorderCanonicalEncoder<T> {
public:
    GeneratedGraphRecorderCanonicalEncoder(
        CentralSlabAllocator& allocator, ShmPinTable& pins,
        schema::WireLimits wire_limits = {},
        schema::DynamicObjectOptions object_options = {},
        ProcessIdentity owner = ProcessIdentity::Current()) noexcept
        : allocator_(&allocator),
          pins_(&pins),
          wire_limits_(wire_limits),
          object_options_(object_options),
          owner_(owner) {}

    Status Validate(const T& value,
                    const MessageMetadata& metadata) noexcept override {
        static_cast<void>(metadata);
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        return StaticMessageTraits<T>::Validate(value);
    }

    Result<std::vector<std::byte>> Encode(
        const T& value,
        const MessageMetadata& metadata) noexcept override {
        static_cast<void>(value);
        const ShmPinContract contract{
            .type_id = StaticMessageTraits<T>::type_id,
            .schema_short_id = StaticMessageTraits<T>::schema_short_id,
            .layout_version = StaticMessageTraits<T>::layout_version,
            .object_size = sizeof(T),
        };
        Result<ShmPinToken> pin =
            pins_->Pin(metadata.payload, contract, owner_);
        if (!pin.ok()) return pin.status();
        return WireAdapter::Encode(metadata.payload, *allocator_,
                                   std::move(*pin), wire_limits_,
                                   object_options_);
    }

private:
    CentralSlabAllocator* allocator_ = nullptr;
    ShmPinTable* pins_ = nullptr;
    schema::WireLimits wire_limits_;
    schema::DynamicObjectOptions object_options_;
    ProcessIdentity owner_;
};

template <typename T>
class RecorderBorrow {
public:
    virtual ~RecorderBorrow() = default;
    virtual const T& value() const noexcept = 0;
    virtual const MessageMetadata& metadata() const noexcept = 0;
    virtual bool active() const noexcept = 0;
    virtual Status Ack() noexcept = 0;
};

template <typename T>
class RecorderBorrowSource {
public:
    virtual ~RecorderBorrowSource() = default;
    virtual Result<std::unique_ptr<RecorderBorrow<T>>> TryBorrow() noexcept = 0;
};

// Allocation happens before Subscriber::TryPoll(), so an adapter allocation
// failure can never acquire and then implicitly ACK a runtime message.
template <typename T>
class RuntimeRecorderBorrowSource final : public RecorderBorrowSource<T> {
private:
    class RuntimeBorrow final : public RecorderBorrow<T> {
    public:
        void Set(BorrowedMessage<T> borrow) noexcept {
            borrow_.emplace(std::move(borrow));
        }

        const T& value() const noexcept override { return **borrow_; }
        const MessageMetadata& metadata() const noexcept override {
            return borrow_->metadata();
        }
        bool active() const noexcept override {
            return borrow_.has_value() && borrow_->active();
        }
        Status Ack() noexcept override {
            if (!active()) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "recorder borrow is not active");
            }
            Status status = std::move(*borrow_).Ack();
            borrow_.reset();
            return status;
        }

    private:
        std::optional<BorrowedMessage<T>> borrow_;
    };

public:
    explicit RuntimeRecorderBorrowSource(Subscriber<T>& subscriber) noexcept
        : subscriber_(&subscriber) {}

    Result<std::unique_ptr<RecorderBorrow<T>>> TryBorrow() noexcept override {
        std::unique_ptr<RuntimeBorrow> adapted;
        try {
            adapted = std::make_unique<RuntimeBorrow>();
        } catch (const std::bad_alloc&) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "cannot allocate recorder borrow adapter");
        }

        Result<BorrowedMessage<T>> borrowed = subscriber_->TryPoll();
        if (!borrowed.ok()) return borrowed.status();
        adapted->Set(std::move(*borrowed));
        return std::unique_ptr<RecorderBorrow<T>>(std::move(adapted));
    }

private:
    Subscriber<T>* subscriber_ = nullptr;
};

template <typename T>
class RecorderSubscriber final {
public:
    static Result<std::unique_ptr<RecorderSubscriber>> Create(
        RecorderSubscriberOptions options, RecorderBorrowSource<T>* source,
        RecorderCanonicalEncoder<T>* encoder,
        RecorderSourceResolver* source_resolver, RecorderBufferSink* sink,
        RecorderClock* clock) noexcept {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        const Status options_status =
            ValidateRecorderSubscriberOptions(options);
        if (!options_status.ok()) return options_status;
        if (source == nullptr || encoder == nullptr ||
            source_resolver == nullptr || sink == nullptr || clock == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "recorder subscriber dependency is null");
        }
        try {
            return std::unique_ptr<RecorderSubscriber>(new RecorderSubscriber(
                std::move(options), source, encoder, source_resolver, sink,
                clock));
        } catch (const std::bad_alloc&) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "cannot allocate recorder subscriber");
        }
    }

    RecorderSubscriber(const RecorderSubscriber&) = delete;
    RecorderSubscriber& operator=(const RecorderSubscriber&) = delete;
    RecorderSubscriber(RecorderSubscriber&&) = delete;
    RecorderSubscriber& operator=(RecorderSubscriber&&) = delete;

    // If a previous kBlock admission is pending, this flushes recorder-owned
    // canonical bytes before polling another SHM message. No Borrow is held
    // during that potentially waiting operation.
    Result<RecorderRecordResult> TryRecord() noexcept {
        std::unique_ptr<RecorderBorrow<T>> borrow;
        try {
            if (pending_.has_value()) return FlushPending();

            Result<std::unique_ptr<RecorderBorrow<T>>> acquired =
                source_->TryBorrow();
            if (!acquired.ok()) return acquired.status();
            borrow = std::move(*acquired);
            if (borrow == nullptr || !borrow->active()) {
                return Status::Error(StatusCode::kInternal,
                                     "borrow source returned no active borrow");
            }
            return RecordBorrow(*borrow);
        } catch (const std::bad_alloc&) {
            if (borrow != nullptr && borrow->active()) {
                RecorderRecordResult result;
                result.status = Status::Error(StatusCode::kResourceExhausted);
                return AckAndReturn(std::move(result), *borrow);
            }
            return Status::Error(StatusCode::kResourceExhausted);
        } catch (...) {
            if (borrow != nullptr && borrow->active()) {
                RecorderRecordResult result;
                result.status = Status::Error(StatusCode::kInternal);
                return AckAndReturn(std::move(result), *borrow);
            }
            return Status::Error(StatusCode::kInternal);
        }
    }

    bool has_pending() const noexcept { return pending_.has_value(); }
    size_t pending_bytes() const noexcept {
        return pending_.has_value() ? pending_->payload.size() : 0;
    }

private:
    struct PendingRecord {
        RecorderRecordMetadata metadata;
        std::vector<std::byte> payload;
        uint64_t user_tag = 0;
        bool ack_attempted = false;
        StatusCode ack_code = StatusCode::kOk;
    };

    RecorderSubscriber(RecorderSubscriberOptions options,
                       RecorderBorrowSource<T>* source,
                       RecorderCanonicalEncoder<T>* encoder,
                       RecorderSourceResolver* source_resolver,
                       RecorderBufferSink* sink, RecorderClock* clock) noexcept
        : options_(std::move(options)),
          source_(source),
          encoder_(encoder),
          source_resolver_(source_resolver),
          sink_(sink),
          clock_(clock) {}

    static bool ValidSource(const MessageSource& source) noexcept {
        return source.node_id != 0 && source.publisher_id != 0 &&
               source.publisher_epoch != 0;
    }

    Status ValidateMetadata(const MessageMetadata& metadata) const noexcept {
        if (metadata.message_type != StaticMessageTraits<T>::message_type ||
            metadata.schema_short_id != options_.schema.short_id ||
            metadata.schema_version != options_.schema.schema_version ||
            metadata.schema_layout_version != options_.schema.layout_version ||
            metadata.payload_len != sizeof(T) || metadata.payload.IsNull()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "recorder message metadata does not match topic schema");
        }
        return Status::Ok();
    }

    RecorderRecordResult AckAndReturn(RecorderRecordResult result,
                                      RecorderBorrow<T>& borrow) noexcept {
        result.ack_attempted = true;
        result.ack_status = borrow.Ack();
        if (!result.ack_status.ok()) {
            result.disposition = RecorderRecordDisposition::kFailed;
        }
        return result;
    }

    Result<RecorderRecordResult> RecordBorrow(
        RecorderBorrow<T>& borrow) {
        RecorderRecordResult result;

        const Status metadata_status = ValidateMetadata(borrow.metadata());
        if (!metadata_status.ok()) {
            result.status = metadata_status;
            return AckAndReturn(std::move(result), borrow);
        }

        const Status value_status =
            encoder_->Validate(borrow.value(), borrow.metadata());
        if (!value_status.ok()) {
            result.status = value_status;
            return AckAndReturn(std::move(result), borrow);
        }

        Result<MessageSource> source =
            source_resolver_->Resolve(borrow.metadata());
        if (!source.ok()) {
            result.status = source.status();
            return AckAndReturn(std::move(result), borrow);
        }
        if (!ValidSource(*source)) {
            result.status = Status::Error(
                StatusCode::kInvalidArgument,
                "recorder source identity is incomplete");
            return AckAndReturn(std::move(result), borrow);
        }

        Result<std::vector<std::byte>> encoded =
            encoder_->Encode(borrow.value(), borrow.metadata());
        if (!encoded.ok()) {
            result.status = encoded.status();
            return AckAndReturn(std::move(result), borrow);
        }
        if (encoded->size() > options_.max_canonical_payload_bytes ||
            encoded->size() >
                static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
            result.status = Status::Error(
                StatusCode::kResourceExhausted,
                "canonical recorder payload exceeds configured bound");
            return AckAndReturn(std::move(result), borrow);
        }

        RecorderRecordMetadata record{
            .schema = options_.schema,
            .topic_id = options_.topic_id,
            .source = *source,
            .ingestion_timestamp_ns = clock_->NowNs(),
            .payload_size = static_cast<uint32_t>(encoded->size()),
            .payload_crc = RecorderPayloadCrc32c(*encoded),
        };
        result.metadata = record;
        const uint64_t user_tag = source->source_sequence;
        const RecorderCopyRequest request{
            .metadata = &record,
            .payload = *encoded,
            .full_policy = options_.full_policy,
            .timeout = std::chrono::nanoseconds::zero(),
            .user_tag = user_tag,
        };
        Result<RecorderCopyResult> copied =
            sink_->ReserveCopyCommit(request);
        if (!copied.ok()) {
            result.status = copied.status();
            const bool admission_blocked =
                copied.status().code() == StatusCode::kWouldBlock ||
                copied.status().code() == StatusCode::kTimeout;
            if (options_.full_policy == BufferFullPolicy::kBlock &&
                admission_blocked) {
                pending_.emplace(PendingRecord{
                    .metadata = record,
                    .payload = std::move(*encoded),
                    .user_tag = user_tag,
                });
                result.disposition = RecorderRecordDisposition::kBlocked;
                result.pending = true;
            } else if ((options_.full_policy ==
                            BufferFullPolicy::kDropNewest ||
                        options_.full_policy ==
                            BufferFullPolicy::kDropOldest) &&
                       admission_blocked) {
                result.disposition = RecorderRecordDisposition::kDropped;
            }
            RecorderRecordResult finalized =
                AckAndReturn(std::move(result), borrow);
            if (finalized.pending && pending_.has_value()) {
                pending_->ack_attempted = finalized.ack_attempted;
                pending_->ack_code = finalized.ack_status.code();
            }
            return finalized;
        }

        result.discarded = std::move(copied->discarded);
        switch (copied->admission) {
            case BufferAdmission::kAccepted:
                result.disposition = RecorderRecordDisposition::kBuffered;
                result.status = Status::Ok();
                break;
            case BufferAdmission::kDroppedNewest:
                result.disposition = RecorderRecordDisposition::kDropped;
                result.status = Status::Error(
                    StatusCode::kResourceExhausted,
                    "recorder buffer policy dropped the message");
                break;
            case BufferAdmission::kRecordingFailed:
                result.disposition = RecorderRecordDisposition::kFailed;
                result.status = Status::Error(
                    StatusCode::kUnavailable,
                    "recorder buffer policy stopped recording");
                break;
        }
        return AckAndReturn(std::move(result), borrow);
    }

    RecorderRecordResult FlushPending() {
        PendingRecord& pending = *pending_;
        RecorderRecordResult result;
        result.metadata = pending.metadata;
        result.ack_attempted = pending.ack_attempted;
        result.ack_status = pending.ack_code == StatusCode::kOk
                                ? Status::Ok()
                                : Status::Error(pending.ack_code);

        const RecorderCopyRequest request{
            .metadata = &pending.metadata,
            .payload = pending.payload,
            .full_policy = BufferFullPolicy::kBlock,
            .timeout = options_.pending_retry_timeout,
            .user_tag = pending.user_tag,
        };
        Result<RecorderCopyResult> copied =
            sink_->ReserveCopyCommit(request);
        if (!copied.ok()) {
            result.status = copied.status();
            if (copied.status().code() == StatusCode::kWouldBlock ||
                copied.status().code() == StatusCode::kTimeout) {
                result.disposition = RecorderRecordDisposition::kBlocked;
                result.pending = true;
            } else {
                result.disposition = RecorderRecordDisposition::kFailed;
                pending_.reset();
            }
            if (!result.ack_status.ok()) {
                result.disposition = RecorderRecordDisposition::kFailed;
            }
            return result;
        }

        result.discarded = std::move(copied->discarded);
        switch (copied->admission) {
            case BufferAdmission::kAccepted:
                result.disposition = RecorderRecordDisposition::kBuffered;
                result.status = Status::Ok();
                break;
            case BufferAdmission::kDroppedNewest:
                result.disposition = RecorderRecordDisposition::kDropped;
                result.status = Status::Error(
                    StatusCode::kResourceExhausted,
                    "pending recorder message was dropped");
                break;
            case BufferAdmission::kRecordingFailed:
                result.disposition = RecorderRecordDisposition::kFailed;
                result.status = Status::Error(
                    StatusCode::kUnavailable,
                    "recording failed while flushing pending message");
                break;
        }
        pending_.reset();
        if (!result.ack_status.ok()) {
            result.disposition = RecorderRecordDisposition::kFailed;
        }
        return result;
    }

    RecorderSubscriberOptions options_;
    RecorderBorrowSource<T>* source_ = nullptr;
    RecorderCanonicalEncoder<T>* encoder_ = nullptr;
    RecorderSourceResolver* source_resolver_ = nullptr;
    RecorderBufferSink* sink_ = nullptr;
    RecorderClock* clock_ = nullptr;
    std::optional<PendingRecord> pending_;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_RECORDER_SUBSCRIBER_H_
