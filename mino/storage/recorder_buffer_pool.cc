// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder_buffer_pool.h"

#include <array>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <new>
#include <utility>

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

constexpr std::array<size_t, 3> kFixedClassBytes = {
    kRecorderSmallBufferClassBytes,
    kRecorderMediumBufferClassBytes,
    kRecorderLargeBufferClassBytes,
};

Result<RecorderBufferPoolOptions> NormalizeOptions(
    const RecorderBufferPoolOptions& requested) {
    RecorderBufferPoolOptions options = requested;
    if (options.global_byte_limit == 0) {
        return Invalid("recorder global byte limit must be non-zero");
    }
    if (options.default_topic_byte_limit == 0) {
        return Invalid("recorder default topic byte limit must be non-zero");
    }
    if (options.queue_capacity == 0) {
        return Invalid("recorder queue capacity must be non-zero");
    }
    if (options.max_topics == 0) {
        return Invalid("recorder max_topics must be non-zero");
    }
    if (options.max_large_object_bytes < kRecorderLargeBufferClassBytes) {
        return Invalid("recorder large-object limit must be at least 1 MiB");
    }
    for (const auto& [topic_id, limit] : options.topic_byte_limits) {
        static_cast<void>(topic_id);
        if (limit == 0) {
            return Invalid("recorder topic byte limits must be non-zero");
        }
    }

    if (options.high_watermark_bytes == 0) {
        options.high_watermark_bytes =
            options.global_byte_limit - options.global_byte_limit / 5;
        if (options.high_watermark_bytes == 0) {
            options.high_watermark_bytes = 1;
        }
    }
    if (options.low_watermark_bytes == 0) {
        options.low_watermark_bytes = options.global_byte_limit / 2;
    }
    if (options.high_watermark_bytes > options.global_byte_limit) {
        return Invalid("recorder high watermark exceeds global byte limit");
    }
    if (options.low_watermark_bytes >= options.high_watermark_bytes) {
        return Invalid("recorder low watermark must be below high watermark");
    }
    return options;
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

}  // namespace

namespace detail {

struct RecorderBufferBlock {
    explicit RecorderBufferBlock(size_t buffer_capacity)
        : data(buffer_capacity == 0
                   ? nullptr
                   : std::make_unique<std::byte[]>(buffer_capacity)),
          capacity(buffer_capacity) {}

    std::unique_ptr<std::byte[]> data;
    size_t capacity = 0;
    int fixed_class = -1;
    RecorderBufferBlock* free_next = nullptr;
    RecorderBufferBlock* all_previous = nullptr;
    RecorderBufferBlock* all_next = nullptr;
};

struct QueueEntry {
    RecorderBufferBlock* block = nullptr;
    TopicId topic_id{};
    size_t payload_size = 0;
    size_t charged_bytes = 0;
    uint64_t user_tag = 0;
};

struct TopicUsage {
    size_t bytes = 0;
    size_t leases = 0;
};

struct RecorderBufferPoolState final
    : std::enable_shared_from_this<RecorderBufferPoolState> {
    explicit RecorderBufferPoolState(RecorderBufferPoolOptions normalized)
        : options(std::move(normalized)), queue(options.queue_capacity) {}

    ~RecorderBufferPoolState() {
        RecorderBufferBlock* block = all_blocks;
        while (block != nullptr) {
            RecorderBufferBlock* next = block->all_next;
            delete block;
            block = next;
        }
    }

    RecorderBufferPoolState(const RecorderBufferPoolState&) = delete;
    RecorderBufferPoolState& operator=(const RecorderBufferPoolState&) = delete;

    size_t TopicLimitLocked(TopicId topic_id) const noexcept {
        const auto configured = options.topic_byte_limits.find(topic_id);
        return configured == options.topic_byte_limits.end()
                   ? options.default_topic_byte_limit
                   : configured->second;
    }

    size_t TopicBytesLocked(TopicId topic_id) const noexcept {
        const auto found = topic_usage.find(topic_id);
        return found == topic_usage.end() ? 0 : found->second.bytes;
    }

    static int FixedClassForCapacity(size_t capacity) noexcept {
        for (size_t index = 0; index < kFixedClassBytes.size(); ++index) {
            if (capacity == kFixedClassBytes[index]) {
                return static_cast<int>(index);
            }
        }
        return -1;
    }

    Result<size_t> ChargedBytes(size_t payload_size) const {
        if (payload_size == 0) return size_t{0};
        for (size_t class_bytes : kFixedClassBytes) {
            if (payload_size <= class_bytes) return class_bytes;
        }
        if (payload_size > options.max_large_object_bytes) {
            return Exhausted("recorder payload exceeds large-object limit");
        }
        constexpr size_t kAlignment = kRecorderSmallBufferClassBytes;
        const size_t remainder = payload_size % kAlignment;
        if (remainder == 0) return payload_size;
        const size_t increment = kAlignment - remainder;
        if (payload_size > std::numeric_limits<size_t>::max() - increment) {
            return Exhausted("recorder payload size overflows class rounding");
        }
        const size_t rounded = payload_size + increment;
        return rounded <= options.max_large_object_bytes
                   ? rounded
                   : options.max_large_object_bytes;
    }

    bool CanAdmitLocked(TopicId topic_id, size_t charged_bytes) const noexcept {
        if (reserved_records + queued_records >= options.queue_capacity) {
            return false;
        }
        if (charged_bytes > options.global_byte_limit - bytes_in_use) {
            return false;
        }
        const size_t topic_bytes = TopicBytesLocked(topic_id);
        const size_t topic_limit = TopicLimitLocked(topic_id);
        return charged_bytes <= topic_limit - topic_bytes;
    }

    void UpdatePressureLocked() noexcept {
        if (bytes_in_use >= options.high_watermark_bytes) {
            pressure = BufferPressureState::kHigh;
        } else if (bytes_in_use <= options.low_watermark_bytes) {
            pressure = BufferPressureState::kLow;
        } else {
            pressure = BufferPressureState::kNormal;
        }
    }

    void LinkBlockLocked(RecorderBufferBlock* block) noexcept {
        block->all_next = all_blocks;
        block->all_previous = nullptr;
        if (all_blocks != nullptr) all_blocks->all_previous = block;
        all_blocks = block;
        allocated_bytes += block->capacity;
    }

    void DeleteBlockLocked(RecorderBufferBlock* block) noexcept {
        if (block->all_previous == nullptr) {
            all_blocks = block->all_next;
        } else {
            block->all_previous->all_next = block->all_next;
        }
        if (block->all_next != nullptr) {
            block->all_next->all_previous = block->all_previous;
        }
        allocated_bytes -= block->capacity;
        delete block;
    }

    bool EvictOneFreeBlockLocked() noexcept {
        for (RecorderBufferBlock*& head : free_blocks) {
            if (head == nullptr) continue;
            RecorderBufferBlock* victim = head;
            head = victim->free_next;
            victim->free_next = nullptr;
            DeleteBlockLocked(victim);
            return true;
        }
        return false;
    }

    RecorderBufferBlock* AcquireBlockLocked(size_t charged_bytes) {
        const int fixed_class = FixedClassForCapacity(charged_bytes);
        if (fixed_class >= 0) {
            RecorderBufferBlock*& head =
                free_blocks[static_cast<size_t>(fixed_class)];
            if (head != nullptr) {
                RecorderBufferBlock* block = head;
                head = block->free_next;
                block->free_next = nullptr;
                return block;
            }
        }

        while (charged_bytes > options.global_byte_limit - allocated_bytes) {
            if (!EvictOneFreeBlockLocked()) return nullptr;
        }
        auto* block = new RecorderBufferBlock(charged_bytes);
        block->fixed_class = fixed_class;
        LinkBlockLocked(block);
        return block;
    }

    void ReleaseBlockLocked(RecorderBufferBlock* block, TopicId topic_id,
                            size_t charged_bytes) noexcept {
        const auto found = topic_usage.find(topic_id);
        if (found != topic_usage.end()) {
            if (charged_bytes <= found->second.bytes) {
                found->second.bytes -= charged_bytes;
            } else {
                found->second.bytes = 0;
            }
            if (found->second.leases != 0) --found->second.leases;
            if (found->second.leases == 0) topic_usage.erase(found);
        }
        if (charged_bytes <= bytes_in_use) {
            bytes_in_use -= charged_bytes;
        } else {
            bytes_in_use = 0;
        }

        if (!closed && block->fixed_class >= 0) {
            RecorderBufferBlock*& head =
                free_blocks[static_cast<size_t>(block->fixed_class)];
            block->free_next = head;
            head = block;
        } else {
            DeleteBlockLocked(block);
        }
        UpdatePressureLocked();
    }

    void ReleaseHandle(RecorderBufferBlock* block, TopicId topic_id,
                       size_t charged_bytes) noexcept {
        {
            std::lock_guard lock(mutex);
            ReleaseBlockLocked(block, topic_id, charged_bytes);
        }
        capacity_changed.notify_all();
    }

    std::optional<DiscardedBuffer> CancelReservation(
        RecorderBufferBlock* block, TopicId topic_id, size_t payload_size,
        size_t charged_bytes, uint64_t user_tag) noexcept {
        if (block == nullptr) return std::nullopt;
        {
            std::lock_guard lock(mutex);
            if (reserved_records != 0) --reserved_records;
            ++cancelled_reservations;
            ReleaseBlockLocked(block, topic_id, charged_bytes);
        }
        capacity_changed.notify_all();
        return DiscardedBuffer{BufferDiscardReason::kReservationCancelled,
                               topic_id, user_tag, payload_size, charged_bytes};
    }

    Status CommitReservation(RecorderBufferBlock* block, TopicId topic_id,
                             size_t payload_size, size_t charged_bytes,
                             uint64_t user_tag) {
        {
            std::lock_guard lock(mutex);
            if (closed || recording_failed) {
                // Construct the potentially allocating diagnostic before
                // relinquishing ownership. If construction throws, Commit()'s
                // catch path still owns and can safely cancel the reservation.
                Status rejection =
                    Unavailable(closed ? "recorder buffer pool is closed"
                                       : "recorder recording has failed");
                if (reserved_records != 0) --reserved_records;
                ReleaseBlockLocked(block, topic_id, charged_bytes);
                capacity_changed.notify_all();
                return rejection;
            }

            QueueEntry& entry = queue[queue_tail];
            entry = QueueEntry{block, topic_id, payload_size, charged_bytes,
                               user_tag};
            queue_tail = (queue_tail + 1) % options.queue_capacity;
            ++queued_records;
            if (reserved_records != 0) --reserved_records;
            ++accepted_records;
        }
        record_available.notify_one();
        return Status::Ok();
    }

    QueueEntry QueueAtLocked(size_t offset) const noexcept {
        return queue[(queue_head + offset) % options.queue_capacity];
    }

    void DropFrontLocked() noexcept {
        QueueEntry& entry = queue[queue_head];
        RecorderBufferBlock* block = entry.block;
        const TopicId topic_id = entry.topic_id;
        const size_t charged_bytes = entry.charged_bytes;
        entry = QueueEntry{};
        queue_head = (queue_head + 1) % options.queue_capacity;
        --queued_records;
        ++dropped_oldest_records;
        ReleaseBlockLocked(block, topic_id, charged_bytes);
    }

    RecorderBufferPoolOptions options;
    mutable std::mutex mutex;
    std::condition_variable capacity_changed;
    std::condition_variable record_available;

    std::vector<QueueEntry> queue;
    size_t queue_head = 0;
    size_t queue_tail = 0;
    size_t queued_records = 0;
    size_t reserved_records = 0;

    std::unordered_map<TopicId, TopicUsage> topic_usage;
    size_t bytes_in_use = 0;
    size_t allocated_bytes = 0;
    BufferPressureState pressure = BufferPressureState::kLow;
    bool closed = false;
    bool recording_failed = false;

    uint64_t accepted_records = 0;
    uint64_t dequeued_records = 0;
    uint64_t dropped_newest_records = 0;
    uint64_t dropped_oldest_records = 0;
    uint64_t cancelled_reservations = 0;
    uint64_t block_timeouts = 0;
    uint64_t recording_failures = 0;

    RecorderBufferBlock* all_blocks = nullptr;
    std::array<RecorderBufferBlock*, 3> free_blocks{};
};

}  // namespace detail

RecorderBufferHandle::RecorderBufferHandle(
    std::shared_ptr<detail::RecorderBufferPoolState> state,
    detail::RecorderBufferBlock* block, TopicId topic_id, size_t payload_size,
    size_t charged_bytes, uint64_t user_tag) noexcept
    : state_(std::move(state)),
      block_(block),
      topic_id_(topic_id),
      payload_size_(payload_size),
      charged_bytes_(charged_bytes),
      user_tag_(user_tag) {}

RecorderBufferHandle::~RecorderBufferHandle() { Reset(); }

RecorderBufferHandle::RecorderBufferHandle(
    RecorderBufferHandle&& other) noexcept
    : state_(std::move(other.state_)),
      block_(std::exchange(other.block_, nullptr)),
      topic_id_(other.topic_id_),
      payload_size_(std::exchange(other.payload_size_, 0)),
      charged_bytes_(std::exchange(other.charged_bytes_, 0)),
      user_tag_(std::exchange(other.user_tag_, 0)) {}

RecorderBufferHandle& RecorderBufferHandle::operator=(
    RecorderBufferHandle&& other) noexcept {
    if (this == &other) return *this;
    Reset();
    state_ = std::move(other.state_);
    block_ = std::exchange(other.block_, nullptr);
    topic_id_ = other.topic_id_;
    payload_size_ = std::exchange(other.payload_size_, 0);
    charged_bytes_ = std::exchange(other.charged_bytes_, 0);
    user_tag_ = std::exchange(other.user_tag_, 0);
    return *this;
}

std::byte* RecorderBufferHandle::data() noexcept {
    return block_ == nullptr ? nullptr : block_->data.get();
}

const std::byte* RecorderBufferHandle::data() const noexcept {
    return block_ == nullptr ? nullptr : block_->data.get();
}

std::span<std::byte> RecorderBufferHandle::bytes() noexcept {
    return std::span<std::byte>(data(), payload_size_);
}

std::span<const std::byte> RecorderBufferHandle::bytes() const noexcept {
    return std::span<const std::byte>(data(), payload_size_);
}

void RecorderBufferHandle::Reset() noexcept {
    if (block_ == nullptr) return;
    detail::RecorderBufferBlock* block = std::exchange(block_, nullptr);
    const TopicId topic_id = topic_id_;
    const size_t charged_bytes = std::exchange(charged_bytes_, 0);
    payload_size_ = 0;
    user_tag_ = 0;
    state_->ReleaseHandle(block, topic_id, charged_bytes);
    state_.reset();
}

RecorderBufferReservation::RecorderBufferReservation(
    std::shared_ptr<detail::RecorderBufferPoolState> state,
    detail::RecorderBufferBlock* block, TopicId topic_id, size_t payload_size,
    size_t charged_bytes, uint64_t user_tag) noexcept
    : state_(std::move(state)),
      block_(block),
      topic_id_(topic_id),
      payload_size_(payload_size),
      charged_bytes_(charged_bytes),
      user_tag_(user_tag) {}

RecorderBufferReservation::~RecorderBufferReservation() {
    static_cast<void>(Cancel());
}

RecorderBufferReservation::RecorderBufferReservation(
    RecorderBufferReservation&& other) noexcept
    : state_(std::move(other.state_)),
      block_(std::exchange(other.block_, nullptr)),
      topic_id_(other.topic_id_),
      payload_size_(std::exchange(other.payload_size_, 0)),
      charged_bytes_(std::exchange(other.charged_bytes_, 0)),
      user_tag_(std::exchange(other.user_tag_, 0)) {}

RecorderBufferReservation& RecorderBufferReservation::operator=(
    RecorderBufferReservation&& other) noexcept {
    if (this == &other) return *this;
    static_cast<void>(Cancel());
    state_ = std::move(other.state_);
    block_ = std::exchange(other.block_, nullptr);
    topic_id_ = other.topic_id_;
    payload_size_ = std::exchange(other.payload_size_, 0);
    charged_bytes_ = std::exchange(other.charged_bytes_, 0);
    user_tag_ = std::exchange(other.user_tag_, 0);
    return *this;
}

std::byte* RecorderBufferReservation::data() noexcept {
    return block_ == nullptr ? nullptr : block_->data.get();
}

const std::byte* RecorderBufferReservation::data() const noexcept {
    return block_ == nullptr ? nullptr : block_->data.get();
}

std::span<std::byte> RecorderBufferReservation::bytes() noexcept {
    return std::span<std::byte>(data(), payload_size_);
}

std::span<const std::byte> RecorderBufferReservation::bytes() const noexcept {
    return std::span<const std::byte>(data(), payload_size_);
}

Status RecorderBufferReservation::Commit() && noexcept {
    if (block_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    detail::RecorderBufferBlock* block = std::exchange(block_, nullptr);
    const TopicId topic_id = topic_id_;
    const size_t payload_size = std::exchange(payload_size_, 0);
    const size_t charged_bytes = std::exchange(charged_bytes_, 0);
    const uint64_t user_tag = std::exchange(user_tag_, 0);
    try {
        Status status = state_->CommitReservation(
            block, topic_id, payload_size, charged_bytes, user_tag);
        state_.reset();
        return status;
    } catch (...) {
        state_->CancelReservation(block, topic_id, payload_size, charged_bytes,
                                  user_tag);
        state_.reset();
        return Status::Error(StatusCode::kInternal);
    }
}

std::optional<DiscardedBuffer> RecorderBufferReservation::Cancel() noexcept {
    if (block_ == nullptr) return std::nullopt;
    detail::RecorderBufferBlock* block = std::exchange(block_, nullptr);
    const TopicId topic_id = topic_id_;
    const size_t payload_size = std::exchange(payload_size_, 0);
    const size_t charged_bytes = std::exchange(charged_bytes_, 0);
    const uint64_t user_tag = std::exchange(user_tag_, 0);
    std::optional<DiscardedBuffer> discarded = state_->CancelReservation(
        block, topic_id, payload_size, charged_bytes, user_tag);
    state_.reset();
    return discarded;
}

RecorderBufferPool::RecorderBufferPool(
    std::shared_ptr<detail::RecorderBufferPoolState> state) noexcept
    : state_(std::move(state)) {}

Result<std::unique_ptr<RecorderBufferPool>> RecorderBufferPool::Create(
    const RecorderBufferPoolOptions& requested_options) {
    auto normalized = NormalizeOptions(requested_options);
    if (!normalized.ok()) return normalized.status();
    try {
        auto state = std::make_shared<detail::RecorderBufferPoolState>(
            std::move(*normalized));
        return std::unique_ptr<RecorderBufferPool>(
            new RecorderBufferPool(std::move(state)));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate recorder buffer pool metadata");
    }
}

RecorderBufferPool::~RecorderBufferPool() { Close(); }

Result<BufferReserveResult> RecorderBufferPool::Reserve(
    const BufferReservationRequest& request) {
    if (!IsValidPolicy(request.full_policy)) {
        return Invalid("invalid recorder buffer full policy");
    }
    if (request.timeout < std::chrono::nanoseconds::zero()) {
        return Invalid("recorder reserve timeout must not be negative");
    }

    auto charged_result = state_->ChargedBytes(request.payload_size);
    if (!charged_result.ok()) return charged_result.status();
    const size_t charged_bytes = *charged_result;

    std::unique_lock lock(state_->mutex);
    if (state_->closed) return Unavailable("recorder buffer pool is closed");
    if (state_->recording_failed) {
        return Unavailable("recorder recording has failed");
    }

    const size_t topic_limit = state_->TopicLimitLocked(request.topic_id);
    const bool request_can_ever_fit =
        charged_bytes <= state_->options.global_byte_limit &&
        charged_bytes <= topic_limit;

    auto make_incoming_discard = [&](BufferDiscardReason reason) {
        return DiscardedBuffer{reason, request.topic_id, request.user_tag,
                               request.payload_size, charged_bytes};
    };

    std::vector<DiscardedBuffer> policy_discards;
    if (!state_->CanAdmitLocked(request.topic_id, charged_bytes)) {
        switch (request.full_policy) {
            case BufferFullPolicy::kBlock: {
                if (!request_can_ever_fit) {
                    return Exhausted(
                        "recorder request exceeds global or topic byte limit");
                }
                const auto ready = [&] {
                    return state_->closed || state_->recording_failed ||
                           state_->CanAdmitLocked(request.topic_id,
                                                  charged_bytes);
                };
                bool awakened = true;
                if (request.timeout == std::chrono::nanoseconds::max()) {
                    state_->capacity_changed.wait(lock, ready);
                } else {
                    awakened = state_->capacity_changed.wait_for(
                        lock, request.timeout, ready);
                }
                if (!awakened) {
                    ++state_->block_timeouts;
                    return Status::Error(StatusCode::kTimeout,
                                         "recorder reserve timed out");
                }
                if (state_->closed) {
                    return Unavailable("recorder buffer pool is closed");
                }
                if (state_->recording_failed) {
                    return Unavailable("recorder recording has failed");
                }
                break;
            }
            case BufferFullPolicy::kDropNewest: {
                BufferReserveResult result;
                result.admission = BufferAdmission::kDroppedNewest;
                result.discarded.push_back(
                    make_incoming_discard(BufferDiscardReason::kDropNewest));
                ++state_->dropped_newest_records;
                return Result<BufferReserveResult>(std::move(result));
            }
            case BufferFullPolicy::kFailRecording: {
                BufferReserveResult result;
                result.admission = BufferAdmission::kRecordingFailed;
                result.discarded.push_back(
                    make_incoming_discard(BufferDiscardReason::kFailRecording));
                state_->recording_failed = true;
                ++state_->recording_failures;
                lock.unlock();
                state_->capacity_changed.notify_all();
                state_->record_available.notify_all();
                return Result<BufferReserveResult>(std::move(result));
            }
            case BufferFullPolicy::kDropOldest: {
                size_t prefix = 0;
                size_t freed_global = 0;
                size_t freed_topic = 0;
                size_t freed_slots = 0;
                bool found_prefix = false;
                const size_t current_topic_bytes =
                    state_->TopicBytesLocked(request.topic_id);
                for (; prefix < state_->queued_records; ++prefix) {
                    const detail::QueueEntry entry =
                        state_->QueueAtLocked(prefix);
                    freed_global += entry.charged_bytes;
                    if (entry.topic_id == request.topic_id) {
                        freed_topic += entry.charged_bytes;
                    }
                    ++freed_slots;
                    const bool slot_fits =
                        state_->reserved_records + state_->queued_records -
                                freed_slots <
                            state_->options.queue_capacity;
                    const bool global_fits =
                        charged_bytes <=
                        state_->options.global_byte_limit -
                            (state_->bytes_in_use - freed_global);
                    const bool topic_fits =
                        charged_bytes <=
                        topic_limit - (current_topic_bytes - freed_topic);
                    if (slot_fits && global_fits && topic_fits) {
                        ++prefix;
                        found_prefix = true;
                        break;
                    }
                }

                if (!request_can_ever_fit || !found_prefix || prefix == 0 ||
                    prefix > state_->queued_records) {
                    BufferReserveResult result;
                    result.admission = BufferAdmission::kDroppedNewest;
                    result.discarded.push_back(make_incoming_discard(
                        BufferDiscardReason::kDropNewestNoDroppableOldest));
                    ++state_->dropped_newest_records;
                    return Result<BufferReserveResult>(std::move(result));
                }

                // Keep one spare element so a later accounting/allocation
                // failure can report the incoming record without allocating
                // after the oldest prefix has already been discarded.
                policy_discards.reserve(prefix + 1);
                for (size_t offset = 0; offset < prefix; ++offset) {
                    const detail::QueueEntry entry =
                        state_->QueueAtLocked(offset);
                    policy_discards.push_back(DiscardedBuffer{
                        BufferDiscardReason::kDropOldest, entry.topic_id,
                        entry.user_tag, entry.payload_size,
                        entry.charged_bytes});
                }
                for (size_t count = 0; count < prefix; ++count) {
                    state_->DropFrontLocked();
                }
                break;
            }
        }
    }

    auto usage = state_->topic_usage.find(request.topic_id);
    if (usage == state_->topic_usage.end()) {
        if (state_->topic_usage.size() >= state_->options.max_topics) {
            if (!policy_discards.empty()) {
                BufferReserveResult result;
                result.admission = BufferAdmission::kDroppedNewest;
                result.discarded = std::move(policy_discards);
                result.discarded.push_back(make_incoming_discard(
                    BufferDiscardReason::kDropNewestNoDroppableOldest));
                ++state_->dropped_newest_records;
                lock.unlock();
                state_->capacity_changed.notify_all();
                return Result<BufferReserveResult>(std::move(result));
            }
            return Exhausted("recorder active topic limit reached");
        }
        try {
            usage = state_->topic_usage.emplace(request.topic_id,
                                                detail::TopicUsage{})
                        .first;
        } catch (const std::bad_alloc&) {
            if (!policy_discards.empty()) {
                BufferReserveResult result;
                result.admission = BufferAdmission::kDroppedNewest;
                result.discarded = std::move(policy_discards);
                result.discarded.push_back(make_incoming_discard(
                    BufferDiscardReason::kAllocationFailure));
                ++state_->dropped_newest_records;
                lock.unlock();
                state_->capacity_changed.notify_all();
                return Result<BufferReserveResult>(std::move(result));
            }
            return Exhausted("cannot allocate recorder topic accounting");
        }
    }

    detail::RecorderBufferBlock* block = nullptr;
    try {
        block = state_->AcquireBlockLocked(charged_bytes);
    } catch (const std::bad_alloc&) {
        block = nullptr;
    }
    if (block == nullptr) {
        if (usage->second.leases == 0) state_->topic_usage.erase(usage);
        if (!policy_discards.empty()) {
            BufferReserveResult result;
            result.admission = BufferAdmission::kDroppedNewest;
            result.discarded = std::move(policy_discards);
            result.discarded.push_back(make_incoming_discard(
                BufferDiscardReason::kAllocationFailure));
            ++state_->dropped_newest_records;
            lock.unlock();
            state_->capacity_changed.notify_all();
            return Result<BufferReserveResult>(std::move(result));
        }
        return Exhausted("cannot allocate recorder buffer class");
    }

    ++state_->reserved_records;
    ++usage->second.leases;
    usage->second.bytes += charged_bytes;
    state_->bytes_in_use += charged_bytes;
    state_->UpdatePressureLocked();

    BufferReserveResult result;
    result.admission = BufferAdmission::kAccepted;
    result.discarded = std::move(policy_discards);
    result.reservation = RecorderBufferReservation(
        state_, block, request.topic_id, request.payload_size, charged_bytes,
        request.user_tag);
    lock.unlock();
    if (!result.discarded.empty()) state_->capacity_changed.notify_all();
    return Result<BufferReserveResult>(std::move(result));
}

Result<RecorderBufferHandle> RecorderBufferPool::Dequeue(
    std::chrono::nanoseconds timeout) {
    if (timeout < std::chrono::nanoseconds::zero()) {
        return Invalid("recorder dequeue timeout must not be negative");
    }

    std::unique_lock lock(state_->mutex);
    const auto ready = [&] {
        return state_->queued_records != 0 || state_->closed ||
               state_->recording_failed;
    };
    bool awakened = true;
    if (state_->queued_records == 0) {
        if (timeout == std::chrono::nanoseconds::max()) {
            state_->record_available.wait(lock, ready);
        } else {
            awakened = state_->record_available.wait_for(lock, timeout, ready);
        }
    }
    if (!awakened) {
        return Status::Error(StatusCode::kTimeout,
                             "recorder dequeue timed out");
    }
    if (state_->queued_records == 0) {
        return Unavailable(state_->closed ? "recorder buffer pool is closed"
                                          : "recorder recording has failed");
    }

    detail::QueueEntry& entry = state_->queue[state_->queue_head];
    RecorderBufferHandle handle(state_, entry.block, entry.topic_id,
                                entry.payload_size, entry.charged_bytes,
                                entry.user_tag);
    entry = detail::QueueEntry{};
    state_->queue_head =
        (state_->queue_head + 1) % state_->options.queue_capacity;
    --state_->queued_records;
    ++state_->dequeued_records;
    lock.unlock();
    state_->capacity_changed.notify_all();
    return handle;
}

Result<RecorderBufferHandle> RecorderBufferPool::TryDequeue() {
    std::unique_lock lock(state_->mutex);
    if (state_->queued_records == 0) {
        if (state_->closed || state_->recording_failed) {
            return Unavailable(state_->closed ? "recorder buffer pool is closed"
                                              : "recorder recording has failed");
        }
        return Status::Error(StatusCode::kWouldBlock,
                             "recorder queue is empty");
    }

    detail::QueueEntry& entry = state_->queue[state_->queue_head];
    RecorderBufferHandle handle(state_, entry.block, entry.topic_id,
                                entry.payload_size, entry.charged_bytes,
                                entry.user_tag);
    entry = detail::QueueEntry{};
    state_->queue_head =
        (state_->queue_head + 1) % state_->options.queue_capacity;
    --state_->queued_records;
    ++state_->dequeued_records;
    lock.unlock();
    state_->capacity_changed.notify_all();
    return handle;
}

void RecorderBufferPool::Close() noexcept {
    if (state_ == nullptr) return;
    {
        std::lock_guard lock(state_->mutex);
        state_->closed = true;
    }
    state_->capacity_changed.notify_all();
    state_->record_available.notify_all();
}

RecorderBufferPoolStats RecorderBufferPool::stats() const noexcept {
    std::lock_guard lock(state_->mutex);
    return RecorderBufferPoolStats{
        state_->bytes_in_use,
        state_->allocated_bytes,
        state_->queued_records,
        state_->reserved_records,
        state_->topic_usage.size(),
        state_->accepted_records,
        state_->dequeued_records,
        state_->dropped_newest_records,
        state_->dropped_oldest_records,
        state_->cancelled_reservations,
        state_->block_timeouts,
        state_->recording_failures,
        state_->pressure,
        state_->closed,
        state_->recording_failed,
    };
}

size_t RecorderBufferPool::TopicBytesInUse(TopicId topic_id) const noexcept {
    std::lock_guard lock(state_->mutex);
    return state_->TopicBytesLocked(topic_id);
}

size_t RecorderBufferPool::TopicByteLimit(TopicId topic_id) const noexcept {
    std::lock_guard lock(state_->mutex);
    return state_->TopicLimitLocked(topic_id);
}

}  // namespace mino::storage
