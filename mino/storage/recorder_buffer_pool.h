// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RECORDER_BUFFER_POOL_H_
#define MINO_STORAGE_RECORDER_BUFFER_POOL_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/storage/recording_types.h"

namespace mino::storage {

inline constexpr size_t kRecorderSmallBufferClassBytes = 4u * 1024u;
inline constexpr size_t kRecorderMediumBufferClassBytes = 64u * 1024u;
inline constexpr size_t kRecorderLargeBufferClassBytes = 1024u * 1024u;

enum class BufferFullPolicy : uint8_t {
    kBlock,
    kDropNewest,
    kDropOldest,
    kFailRecording,
};

enum class BufferPressureState : uint8_t {
    kLow,
    kNormal,
    kHigh,
};

enum class BufferAdmission : uint8_t {
    kAccepted,
    kDroppedNewest,
    kRecordingFailed,
};

enum class BufferDiscardReason : uint8_t {
    kDropNewest,
    kDropOldest,
    // kDropOldest could not evict a sufficient committed prefix because the
    // limiting bytes or slots were held by reservations or the consumer.
    kDropNewestNoDroppableOldest,
    // Older records were already evicted, but allocating/accounting the
    // replacement failed. Both the victims and incoming record are reported.
    kAllocationFailure,
    kFailRecording,
    kReservationCancelled,
};

struct DiscardedBuffer {
    BufferDiscardReason reason = BufferDiscardReason::kDropNewest;
    TopicId topic_id{};
    uint64_t user_tag = 0;
    size_t payload_size = 0;
    size_t charged_bytes = 0;
    // Present when the producer supplied recorder identity. Drop reports retain
    // it so TopicWriter can derive source-sequence Gaps without side channels.
    std::optional<RecorderRecordMetadata> metadata;
};

struct RecorderBufferPoolOptions {
    size_t global_byte_limit = 64u * 1024u * 1024u;
    size_t default_topic_byte_limit = 16u * 1024u * 1024u;
    size_t queue_capacity = 4096;
    size_t max_topics = 1024;

    // Requests through 1 MiB use fixed classes. Larger requests are rounded up
    // to 4 KiB and are bounded by this value; large-object buffers are not
    // retained in the free cache.
    size_t max_large_object_bytes = 16u * 1024u * 1024u;

    // Zero selects defaults derived from global_byte_limit (80% and 50%).
    size_t high_watermark_bytes = 0;
    size_t low_watermark_bytes = 0;

    // A topic absent from this map uses default_topic_byte_limit.
    std::unordered_map<TopicId, size_t> topic_byte_limits;
};

struct RecorderBufferPoolStats {
    size_t bytes_in_use = 0;
    size_t allocated_bytes = 0;
    size_t queued_records = 0;
    size_t reserved_records = 0;
    size_t active_topics = 0;

    uint64_t accepted_records = 0;
    uint64_t dequeued_records = 0;
    uint64_t dropped_newest_records = 0;
    uint64_t dropped_oldest_records = 0;
    uint64_t cancelled_reservations = 0;
    uint64_t block_timeouts = 0;
    uint64_t recording_failures = 0;

    BufferPressureState pressure = BufferPressureState::kLow;
    bool closed = false;
    bool recording_failed = false;
};

struct BufferReservationRequest {
    TopicId topic_id{};
    size_t payload_size = 0;
    uint64_t user_tag = 0;
    BufferFullPolicy full_policy = BufferFullPolicy::kBlock;
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max();
    // Optional preserves the generic D5-04 byte-buffer API. RecorderSubscriber
    // always supplies this value.
    std::optional<RecorderRecordMetadata> metadata;
};

namespace detail {
struct RecorderBufferPoolState;
struct RecorderBufferBlock;
}  // namespace detail

// Move-only ownership of one recorder buffer. Its charged bytes remain in the
// global and per-topic budgets until the handle is destroyed or moved away.
class RecorderBufferHandle final {
public:
    RecorderBufferHandle() noexcept = default;
    ~RecorderBufferHandle();

    RecorderBufferHandle(const RecorderBufferHandle&) = delete;
    RecorderBufferHandle& operator=(const RecorderBufferHandle&) = delete;
    RecorderBufferHandle(RecorderBufferHandle&& other) noexcept;
    RecorderBufferHandle& operator=(RecorderBufferHandle&& other) noexcept;

    bool valid() const noexcept { return block_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }

    std::byte* data() noexcept;
    const std::byte* data() const noexcept;
    std::span<std::byte> bytes() noexcept;
    std::span<const std::byte> bytes() const noexcept;

    size_t size() const noexcept { return payload_size_; }
    size_t capacity() const noexcept { return charged_bytes_; }
    TopicId topic_id() const noexcept { return topic_id_; }
    uint64_t user_tag() const noexcept { return user_tag_; }
    const std::optional<RecorderRecordMetadata>& metadata() const noexcept {
        return metadata_;
    }

    void Reset() noexcept;

private:
    friend class RecorderBufferPool;
    friend struct detail::RecorderBufferPoolState;

    RecorderBufferHandle(
        std::shared_ptr<detail::RecorderBufferPoolState> state,
        detail::RecorderBufferBlock* block, TopicId topic_id,
        size_t payload_size, size_t charged_bytes, uint64_t user_tag,
        std::optional<RecorderRecordMetadata> metadata) noexcept;

    std::shared_ptr<detail::RecorderBufferPoolState> state_;
    detail::RecorderBufferBlock* block_ = nullptr;
    TopicId topic_id_{};
    size_t payload_size_ = 0;
    size_t charged_bytes_ = 0;
    uint64_t user_tag_ = 0;
    std::optional<RecorderRecordMetadata> metadata_;
};

// Producer-side two-phase queue reservation. Destruction safely cancels an
// uncommitted reservation and releases both its slot and byte budgets.
class RecorderBufferReservation final {
public:
    RecorderBufferReservation() noexcept = default;
    ~RecorderBufferReservation();

    RecorderBufferReservation(const RecorderBufferReservation&) = delete;
    RecorderBufferReservation& operator=(const RecorderBufferReservation&) =
        delete;
    RecorderBufferReservation(RecorderBufferReservation&& other) noexcept;
    RecorderBufferReservation& operator=(
        RecorderBufferReservation&& other) noexcept;

    bool active() const noexcept { return block_ != nullptr; }
    explicit operator bool() const noexcept { return active(); }

    std::byte* data() noexcept;
    const std::byte* data() const noexcept;
    std::span<std::byte> bytes() noexcept;
    std::span<const std::byte> bytes() const noexcept;

    size_t size() const noexcept { return payload_size_; }
    size_t capacity() const noexcept { return charged_bytes_; }
    TopicId topic_id() const noexcept { return topic_id_; }
    uint64_t user_tag() const noexcept { return user_tag_; }
    const std::optional<RecorderRecordMetadata>& metadata() const noexcept {
        return metadata_;
    }

    // Commits in producer completion order. On close/failure the reservation is
    // released and a non-OK Status explicitly reports that it was not queued.
    Status Commit() && noexcept;

    // Explicit cancellation returns the identity and charge of the discarded
    // reservation. The destructor performs the same release and increments the
    // cancellation counter if Cancel() was not called.
    std::optional<DiscardedBuffer> Cancel() noexcept;

private:
    friend class RecorderBufferPool;
    friend struct detail::RecorderBufferPoolState;

    RecorderBufferReservation(
        std::shared_ptr<detail::RecorderBufferPoolState> state,
        detail::RecorderBufferBlock* block, TopicId topic_id,
        size_t payload_size, size_t charged_bytes, uint64_t user_tag,
        std::optional<RecorderRecordMetadata> metadata) noexcept;

    std::shared_ptr<detail::RecorderBufferPoolState> state_;
    detail::RecorderBufferBlock* block_ = nullptr;
    TopicId topic_id_{};
    size_t payload_size_ = 0;
    size_t charged_bytes_ = 0;
    uint64_t user_tag_ = 0;
    std::optional<RecorderRecordMetadata> metadata_;
};

struct BufferReserveResult {
    BufferAdmission admission = BufferAdmission::kAccepted;
    RecorderBufferReservation reservation;
    // Contains every policy-driven discard caused by this call. Accepted
    // kDropOldest calls report all evicted records; rejected calls report the
    // incoming record itself.
    std::vector<DiscardedBuffer> discarded;

    bool accepted() const noexcept {
        return admission == BufferAdmission::kAccepted;
    }
};

// Process-local, bounded MPSC queue and recorder-owned chunk pool. Any number
// of producers may Reserve/Commit concurrently. Exactly one logical consumer
// should call Dequeue; calls are nevertheless serialized defensively.
class RecorderBufferPool final {
public:
    static Result<std::unique_ptr<RecorderBufferPool>> Create(
        const RecorderBufferPoolOptions& options = {});

    ~RecorderBufferPool();

    RecorderBufferPool(const RecorderBufferPool&) = delete;
    RecorderBufferPool& operator=(const RecorderBufferPool&) = delete;
    RecorderBufferPool(RecorderBufferPool&&) = delete;
    RecorderBufferPool& operator=(RecorderBufferPool&&) = delete;

    Result<BufferReserveResult> Reserve(
        const BufferReservationRequest& request);

    // Waits for a committed record. A finite timeout returns kTimeout; after
    // Close(), queued records remain drainable and an empty queue returns
    // kUnavailable. The default waits indefinitely.
    Result<RecorderBufferHandle> Dequeue(
        std::chrono::nanoseconds timeout =
            std::chrono::nanoseconds::max());
    Result<RecorderBufferHandle> TryDequeue();

    // Idempotently rejects future reservations and wakes all blocked producers
    // and the consumer. Existing committed records can still be drained.
    void Close() noexcept;

    RecorderBufferPoolStats stats() const noexcept;
    size_t TopicBytesInUse(TopicId topic_id) const noexcept;
    size_t TopicByteLimit(TopicId topic_id) const noexcept;

private:
    explicit RecorderBufferPool(
        std::shared_ptr<detail::RecorderBufferPoolState> state) noexcept;

    std::shared_ptr<detail::RecorderBufferPoolState> state_;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_RECORDER_BUFFER_POOL_H_
