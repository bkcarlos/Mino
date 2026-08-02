// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder_subscriber.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <new>
#include <string_view>
#include <utility>

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

bool IsValidPolicy(BufferFullPolicy policy) noexcept {
    switch (policy) {
        case BufferFullPolicy::kBlock:
        case BufferFullPolicy::kDropNewest:
        case BufferFullPolicy::kDropOldest:
        case BufferFullPolicy::kFailRecording:
            return true;
    }
    return false;
}

bool HasNonzeroDigest(const schema::CanonicalDigest& digest) noexcept {
    return std::any_of(digest.begin(), digest.end(),
                       [](std::byte value) { return value != std::byte{0}; });
}

uint64_t DigestShortId(const schema::CanonicalDigest& digest) noexcept {
    uint64_t short_id = 0;
    for (size_t i = 0; i < sizeof(short_id); ++i) {
        short_id |= static_cast<uint64_t>(static_cast<uint8_t>(digest[i]))
                    << (i * 8u);
    }
    return short_id;
}

const std::array<uint32_t, 256>& Crc32cTable() noexcept {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> values{};
        for (uint32_t i = 0; i < values.size(); ++i) {
            uint32_t value = i;
            for (int bit = 0; bit < 8; ++bit) {
                value = (value >> 1) ^
                        ((value & 1u) != 0 ? 0x82f63b78u : 0u);
            }
            values[i] = value;
        }
        return values;
    }();
    return table;
}

}  // namespace



Status ValidateRecorderSubscriberOptions(
    const RecorderSubscriberOptions& options) noexcept {
    constexpr auto kMaximumPendingWait = std::chrono::minutes(1);
    if (options.topic_id.value == 0) {
        return Invalid("recorder topic ID must be non-zero");
    }
    if (options.schema.short_id == 0 ||
        options.schema.schema_version == 0 ||
        options.schema.layout_version == 0 ||
        !HasNonzeroDigest(options.schema.canonical_digest)) {
        return Invalid("recorder schema identity is incomplete");
    }
    if (options.schema.short_id !=
        DigestShortId(options.schema.canonical_digest)) {
        return Invalid("recorder schema short ID does not match digest");
    }
    if (!IsValidPolicy(options.full_policy)) {
        return Invalid("recorder buffer full policy is invalid");
    }
    if (options.max_canonical_payload_bytes == 0 ||
        options.max_canonical_payload_bytes >
            static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return Invalid("recorder canonical payload bound is invalid");
    }
    if (options.pending_retry_timeout < std::chrono::nanoseconds::zero() ||
        options.pending_retry_timeout > kMaximumPendingWait) {
        return Invalid("recorder pending retry timeout is out of bounds");
    }
    return Status::Ok();
}

uint32_t RecorderPayloadCrc32c(
    std::span<const std::byte> payload) noexcept {
    uint32_t state = 0xffffffffu;
    const auto& table = Crc32cTable();
    for (std::byte byte : payload) {
        const uint8_t value = static_cast<uint8_t>(byte);
        state = table[(state ^ value) & 0xffu] ^ (state >> 8);
    }
    return state ^ 0xffffffffu;
}

uint64_t SystemRecorderClock::NowNs() noexcept {
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    if (elapsed.count() <= 0) return 0;
    return static_cast<uint64_t>(elapsed.count());
}

Result<MessageSource> FixedRecorderSourceResolver::Resolve(
    const MessageMetadata& metadata) noexcept {
    if (node_id_ == 0 || publisher_id_ == 0 || publisher_epoch_ == 0) {
        return Invalid("fixed recorder source identity is incomplete");
    }
    return MessageSource{
        .node_id = node_id_,
        .publisher_id = publisher_id_,
        .publisher_epoch = publisher_epoch_,
        .source_sequence = metadata.sequence_num,
        .observed_timestamp_ns = metadata.timestamp_ns,
    };
}

Result<RecorderCopyResult> RecorderBufferPoolSink::ReserveCopyCommit(
    const RecorderCopyRequest& request) noexcept {
    try {
        if (pool_ == nullptr || request.metadata == nullptr ||
            request.metadata->topic_id.value == 0 ||
            request.metadata->payload_size != request.payload.size() ||
            request.metadata->payload_crc !=
                RecorderPayloadCrc32c(request.payload)) {
            return Invalid("recorder copy request metadata is inconsistent");
        }

        Result<BufferReserveResult> reserved = pool_->Reserve(
            BufferReservationRequest{
                .topic_id = request.metadata->topic_id,
                .payload_size = request.payload.size(),
                .user_tag = request.user_tag,
                .full_policy = request.full_policy,
                .timeout = request.timeout,
                .metadata = *request.metadata,
            });
        if (!reserved.ok()) return reserved.status();

        RecorderCopyResult result{
            .admission = reserved->admission,
            .discarded = std::move(reserved->discarded),
        };
        if (!reserved->accepted()) return result;
        if (!reserved->reservation.active() ||
            reserved->reservation.bytes().size() != request.payload.size()) {
            reserved->reservation.Cancel();
            return Status::Error(StatusCode::kInternal,
                                 "recorder pool returned an invalid reservation");
        }

        std::copy(request.payload.begin(), request.payload.end(),
                  reserved->reservation.bytes().begin());
        const Status committed = std::move(reserved->reservation).Commit();
        if (!committed.ok()) return committed;
        return result;
    } catch (const std::bad_alloc&) {
        return Exhausted("recorder copy allocation failed");
    } catch (const std::length_error&) {
        return Exhausted("recorder copy exceeded a container bound");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "recorder copy failed unexpectedly");
    }
}

}  // namespace mino::storage
