// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_SUBSCRIBER_H_
#define MINO_RUNTIME_SUBSCRIBER_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/runtime/deadline.h"
#include "mino/runtime/message.h"
#include "mino/runtime/message_traits.h"
#include "mino/runtime/publisher.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/shm/channel/mpsc_channel.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino {

template <typename T>
class Subscriber;

template <typename T>
class BorrowedMessageBatch;

struct BatchAckResult {
    static constexpr size_t kNoFailure = std::numeric_limits<size_t>::max();

    size_t requested_count = 0;
    size_t acked_count = 0;
    size_t first_failed_index = kNoFailure;
    Status first_error = Status::Ok();

    bool ok() const noexcept { return first_error.ok(); }
};

// Runtime borrow for a resolved fixed-layout payload. Unlike the low-level
// Channel Borrow, destruction performs a best-effort ACK and payload cleanup.
// Callers that need to observe ACK failures should use explicit Ack().
template <typename T>
class BorrowedMessage {
public:
    BorrowedMessage() noexcept = default;
    BorrowedMessage(const BorrowedMessage&) = delete;
    BorrowedMessage& operator=(const BorrowedMessage&) = delete;

    BorrowedMessage(BorrowedMessage&& other) noexcept
        : allocator_(other.allocator_),
          pin_table_(other.pin_table_),
          pin_owner_(other.pin_owner_),
          borrow_(std::move(other.borrow_)),
          borrow_pin_(std::move(other.borrow_pin_)),
          value_(other.value_),
          metadata_(other.metadata_),
          payload_cleanup_by_channel_(other.payload_cleanup_by_channel_),
          active_(other.active_) {
        other.allocator_ = nullptr;
        other.pin_table_ = nullptr;
        other.pin_owner_ = {};
        other.value_ = nullptr;
        other.active_ = false;
    }

    BorrowedMessage& operator=(BorrowedMessage&& other) noexcept {
        if (this != &other) {
            AckIfActive();
            allocator_ = other.allocator_;
            pin_table_ = other.pin_table_;
            pin_owner_ = other.pin_owner_;
            borrow_ = std::move(other.borrow_);
            borrow_pin_ = std::move(other.borrow_pin_);
            value_ = other.value_;
            metadata_ = other.metadata_;
            payload_cleanup_by_channel_ = other.payload_cleanup_by_channel_;
            active_ = other.active_;
            other.allocator_ = nullptr;
            other.pin_table_ = nullptr;
            other.pin_owner_ = {};
            other.value_ = nullptr;
            other.active_ = false;
        }
        return *this;
    }

    ~BorrowedMessage() { AckIfActive(); }

    const T* get() const noexcept { return value_; }
    const T* operator->() const noexcept { return value_; }
    const T& operator*() const noexcept { return *value_; }
    const MessageMetadata& metadata() const noexcept { return metadata_; }
    bool active() const noexcept { return active_; }

    Result<ShmSharedPtr<T>> Transfer() && noexcept {
        if (!active_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "borrowed message is not active");
        }
        if (pin_table_ == nullptr) {
            return Status::Error(StatusCode::kUnsupported,
                                 "subscriber has no Pin table");
        }
        ShmSharedPtr<T> pinned;
        if (borrow_pin_.active()) {
            pinned = std::move(borrow_pin_);
        } else {
            Result<ShmSharedPtr<T>> acquired = ShmSharedPtr<T>::Pin(
                *pin_table_, metadata_.payload, pin_owner_);
            if (!acquired.ok()) {
                return acquired.status();
            }
            pinned = std::move(*acquired);
        }
        const Status ack = std::move(*this).Ack();
        if (!ack.ok()) {
            pinned.Release().ok();
            return ack;
        }
        return pinned;
    }


    // Detaches this SPSC borrow from the channel without reclaiming the
    // published graph. The returned ExclusiveMessage owns the same root and
    // child handles so a hop can mutate root scalars and republish without a
    // payload memcpy. Pin-table and Broadcast/MPSC borrows are rejected.
    Result<ExclusiveMessage<T>> TakeExclusive() && noexcept {
        if (!active_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "borrowed message is not active");
        }
        if (!std::holds_alternative<SpscChannel::Borrow>(borrow_)) {
            return Status::Error(
                StatusCode::kUnsupported,
                "exclusive transfer requires an SPSC subscriber");
        }
        if (payload_cleanup_by_channel_ || pin_table_ != nullptr ||
            borrow_pin_.active()) {
            return Status::Error(
                StatusCode::kUnsupported,
                "exclusive transfer does not use Pin-table lifetime");
        }
        if (allocator_ == nullptr || value_ == nullptr ||
            metadata_.payload.IsNull()) {
            return Status::Error(StatusCode::kCorruption,
                                 "borrowed message payload is incomplete");
        }

        ExclusiveMessage<T> exclusive(
            allocator_, const_cast<T*>(value_), metadata_);
        active_ = false;
        value_ = nullptr;
        const Status channel_ack = std::visit(
            [](auto& borrow) { return std::move(borrow).Ack(); }, borrow_);
        allocator_ = nullptr;
        pin_table_ = nullptr;
        pin_owner_ = {};
        if (!channel_ack.ok()) {
            exclusive.Disarm();
            return channel_ack;
        }
        return exclusive;
    }

    Status Ack() && noexcept {
        if (!active_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "borrowed message is not active");
        }

        const bool typed_reclaim_eligible =
            !payload_cleanup_by_channel_ && pin_table_ == nullptr;
        std::array<ShmHandle, OwnedGraphCapacity()> manifest{};
        size_t manifest_count = 0;
        Status graph_collection = Status::Ok();
        if constexpr (SupportsOwnedGraphCollection()) {
            if (typed_reclaim_eligible) {
                graph_collection = CollectOwnedGraph(
                    metadata_.payload, *value_, manifest, manifest_count);
                if (graph_collection.ok() &&
                    (manifest_count == 0 || manifest_count > manifest.size())) {
                    graph_collection = Status::Error(
                        StatusCode::kCorruption,
                        "owned graph collector returned an invalid size");
                }
            }
        }

        active_ = false;
        value_ = nullptr;
        const Status channel_ack = std::visit(
            [](auto& borrow) { return std::move(borrow).Ack(); }, borrow_);

        Status typed_reclaim = Status::Ok();
        Status fallback_cleanup = Status::Ok();
        if (channel_ack.ok() && !payload_cleanup_by_channel_) {
            bool reclaimed_by_manifest = false;
            if constexpr (SupportsOwnedGraphCollection()) {
                if (typed_reclaim_eligible && graph_collection.ok()) {
                    typed_reclaim = allocator_->ReclaimPublishedGraph(
                        metadata_.payload,
                        std::span<const ShmHandle>(manifest.data(),
                                                   manifest_count));
                    reclaimed_by_manifest = typed_reclaim.ok();
                }
            }
            if (!reclaimed_by_manifest) {
                fallback_cleanup = FallbackRootCleanup();
            }
        }

        Status release_pin = Status::Ok();
        if (borrow_pin_.active()) {
            release_pin = borrow_pin_.Release();
        }
        allocator_ = nullptr;
        pin_table_ = nullptr;
        pin_owner_ = {};

        if (!channel_ack.ok()) return channel_ack;
        if (!graph_collection.ok()) return graph_collection;
        if (!typed_reclaim.ok()) return typed_reclaim;
        if (!fallback_cleanup.ok()) return fallback_cleanup;
        return release_pin;
    }

private:
    friend class Subscriber<T>;

    static constexpr bool SupportsOwnedGraphCollection() noexcept {
        if constexpr (requires {
                          StaticMessageTraits<T>::kOwnedGraphCollectionSupported;
                          StaticMessageTraits<T>::kMaxOwnedGraphHandles;
                      }) {
            return StaticMessageTraits<T>::kOwnedGraphCollectionSupported;
        }
        return false;
    }

    static constexpr size_t OwnedGraphCapacity() noexcept {
        if constexpr (SupportsOwnedGraphCollection()) {
            static_assert(StaticMessageTraits<T>::kMaxOwnedGraphHandles > 0,
                          "owned graph collector must have root capacity");
            return StaticMessageTraits<T>::kMaxOwnedGraphHandles;
        }
        return 1;
    }

    Status FallbackRootCleanup() noexcept {
        const Status retire = allocator_->Retire(metadata_.payload);
        if (!retire.ok()) return retire;
        const Status reclaim = allocator_->Reclaim(metadata_.payload);
        if (reclaim.code() == StatusCode::kWouldBlock && pin_table_ != nullptr) {
            return Status::Ok();
        }
        return reclaim;
    }

    template <typename ChannelBorrow>
    BorrowedMessage(CentralSlabAllocator* allocator, ShmPinTable* pin_table,
                    const ProcessIdentity& pin_owner, ChannelBorrow&& borrow,
                    ShmSharedPtr<T>&& borrow_pin, const T* value,
                    MessageMetadata metadata,
                    bool payload_cleanup_by_channel) noexcept
        : allocator_(allocator),
          pin_table_(pin_table),
          pin_owner_(pin_owner),
          borrow_(std::forward<ChannelBorrow>(borrow)),
          borrow_pin_(std::move(borrow_pin)),
          value_(value),
          metadata_(metadata),
          payload_cleanup_by_channel_(payload_cleanup_by_channel),
          active_(true) {}

    void AckIfActive() noexcept {
        if (active_) {
            std::move(*this).Ack().ok();
        }
    }

    CentralSlabAllocator* allocator_ = nullptr;
    ShmPinTable* pin_table_ = nullptr;
    ProcessIdentity pin_owner_;
    std::variant<SpscChannel::Borrow, MpscChannel::Borrow,
                 BroadcastChannel::Borrow> borrow_;
    // Runtime Broadcast Borrows hold a Pin before exposing the payload pointer.
    // DropOldest may retire the payload, but reclamation waits for this Pin.
    ShmSharedPtr<T> borrow_pin_;
    const T* value_ = nullptr;
    MessageMetadata metadata_;
    bool payload_cleanup_by_channel_ = false;
    bool active_ = false;
};

// Move-only, capacity-bounded collection returned by Subscriber::PollBatch.
// Reading is const-only so ACK ownership cannot escape the collection. Ack(i)
// accepts out-of-order requests but physically drains only the contiguous
// prefix, preserving every channel's ordered cursor contract. Destruction
// requests and drains every remaining ACK in order.
template <typename T>
class BorrowedMessageBatch {
public:
    BorrowedMessageBatch() noexcept = default;
    BorrowedMessageBatch(const BorrowedMessageBatch&) = delete;
    BorrowedMessageBatch& operator=(const BorrowedMessageBatch&) = delete;

    BorrowedMessageBatch(BorrowedMessageBatch&& other) noexcept
        : messages_(std::move(other.messages_)),
          ack_requested_(std::move(other.ack_requested_)),
          next_ack_(other.next_ack_),
          poll_status_(std::move(other.poll_status_)) {
        other.next_ack_ = 0;
    }

    BorrowedMessageBatch& operator=(BorrowedMessageBatch&& other) noexcept {
        if (this != &other) {
            (void)AckAll();
            messages_ = std::move(other.messages_);
            ack_requested_ = std::move(other.ack_requested_);
            next_ack_ = other.next_ack_;
            poll_status_ = std::move(other.poll_status_);
            other.next_ack_ = 0;
        }
        return *this;
    }

    ~BorrowedMessageBatch() { (void)AckAll(); }

    size_t size() const noexcept { return messages_.size(); }
    bool empty() const noexcept { return messages_.empty(); }
    const BorrowedMessage<T>& operator[](size_t index) const noexcept {
        return messages_[index];
    }
    typename std::vector<BorrowedMessage<T>>::const_iterator begin() const
        noexcept {
        return messages_.begin();
    }
    typename std::vector<BorrowedMessage<T>>::const_iterator end() const
        noexcept {
        return messages_.end();
    }

    // Non-OK only reports a physical ACK failure while draining the newly
    // contiguous prefix. An out-of-order request is retained and returns OK.
    Status Ack(size_t index) noexcept {
        if (index >= messages_.size() || index < next_ack_ ||
            ack_requested_[index] != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "batch ACK index is invalid or already ACKed");
        }
        ack_requested_[index] = 1;
        Status first_error = Status::Ok();
        while (next_ack_ < messages_.size() &&
               ack_requested_[next_ack_] != 0) {
            const Status ack = std::move(messages_[next_ack_]).Ack();
            if (first_error.ok() && !ack.ok()) {
                first_error = ack;
            }
            ++next_ack_;
        }
        return first_error;
    }

    BatchAckResult AckAll() noexcept {
        BatchAckResult outcome;
        outcome.requested_count = messages_.size() - next_ack_;
        for (size_t i = next_ack_; i < ack_requested_.size(); ++i) {
            ack_requested_[i] = 1;
        }
        while (next_ack_ < messages_.size()) {
            const size_t index = next_ack_;
            const Status ack = std::move(messages_[index]).Ack();
            if (ack.ok()) {
                ++outcome.acked_count;
            } else if (outcome.first_error.ok()) {
                outcome.first_failed_index = index;
                outcome.first_error = ack;
            }
            ++next_ack_;
        }
        return outcome;
    }

    // A partial batch can carry the first non-tail error encountered after at
    // least one message was borrowed. kWouldBlock at the current tail is a
    // normal short batch and leaves this status OK.
    const Status& poll_status() const noexcept { return poll_status_; }

private:
    friend class Subscriber<T>;

    BorrowedMessageBatch(std::vector<BorrowedMessage<T>>&& messages,
                         Status poll_status) noexcept
        : messages_(std::move(messages)),
          ack_requested_(messages_.size(), 0),
          poll_status_(std::move(poll_status)) {}

    std::vector<BorrowedMessage<T>> messages_;
    std::vector<uint8_t> ack_requested_;
    size_t next_ack_ = 0;
    Status poll_status_ = Status::Ok();
};

// Fixed-layout Subscriber facade over SPSC, MPSC, or Broadcast.
template <typename T>
class Subscriber {
public:
    Subscriber(CentralSlabAllocator& allocator, SpscChannel& channel,
               ShmPinTable* pin_table = nullptr,
               const ProcessIdentity& pin_owner =
                   ProcessIdentity::Current()) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          pin_table_(pin_table),
          pin_owner_(pin_owner) {
        ValidateStaticContract();
    }

    Subscriber(CentralSlabAllocator& allocator, MpscChannel& channel,
               ShmPinTable* pin_table = nullptr,
               const ProcessIdentity& pin_owner =
                   ProcessIdentity::Current()) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          pin_table_(pin_table),
          pin_owner_(pin_owner) {
        ValidateStaticContract();
    }

    Subscriber(CentralSlabAllocator& allocator, BroadcastChannel& channel,
               BroadcastChannel::SubscriberHandle subscriber,
               ShmPinTable& pin_table,
               const ProcessIdentity& pin_owner =
                   ProcessIdentity::Current()) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          pin_table_(&pin_table),
          pin_owner_(pin_owner),
          broadcast_subscriber_(subscriber) {
        channel.SetPayloadRetireObserver(&ShmPinTable::RetirePayloadCallback,
                                         &pin_table);
        ValidateStaticContract();
    }

    Result<BorrowedMessage<T>> TryPoll() noexcept {
        if (std::holds_alternative<SpscChannel*>(channel_)) {
            Result<SpscChannel::Borrow> borrow =
                std::get<SpscChannel*>(channel_)->Poll();
            if (!borrow.ok()) {
                return borrow.status();
            }
            return ResolveBorrow(std::move(*borrow));
        }
        if (std::holds_alternative<MpscChannel*>(channel_)) {
            Result<MpscChannel::Borrow> borrow =
                std::get<MpscChannel*>(channel_)->Poll();
            if (!borrow.ok()) {
                return borrow.status();
            }
            return ResolveBorrow(std::move(*borrow));
        }
        Result<BroadcastChannel::Borrow> borrow =
            std::get<BroadcastChannel*>(channel_)->Poll(
                broadcast_subscriber_, pin_owner_);
        if (!borrow.ok()) {
            return borrow.status();
        }
        return ResolveBorrow(std::move(*borrow));
    }

    Result<BroadcastChannel::Gap> LastBroadcastGap() const noexcept {
        if (!std::holds_alternative<BroadcastChannel*>(channel_)) {
            return Status::Error(StatusCode::kUnsupported,
                                 "subscriber is not bound to Broadcast");
        }
        return std::get<BroadcastChannel*>(channel_)->LastGap(
            broadcast_subscriber_);
    }

    Result<BroadcastChannel::SubscriberStats> BroadcastStats() const noexcept {
        if (!std::holds_alternative<BroadcastChannel*>(channel_)) {
            return Status::Error(StatusCode::kUnsupported,
                                 "subscriber is not bound to Broadcast");
        }
        return std::get<BroadcastChannel*>(channel_)->GetSubscriberStats(
            broadcast_subscriber_);
    }

    // The requested vector capacity is explicit and bounded. A successful
    // result always contains at least one BorrowedMessage; reaching the current
    // queue tail returns a shorter batch with poll_status()==OK.
    static constexpr size_t kMaxBatchMessages = 1024;

    Result<BorrowedMessageBatch<T>> TryPollBatch(size_t capacity) noexcept {
        batch_poll_calls_.fetch_add(1, std::memory_order_relaxed);
        if (capacity == 0 || capacity > kMaxBatchMessages) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "poll batch capacity must be in [1, kMaxBatchMessages]");
        }

        Result<BorrowedMessage<T>> first = TryPoll();
        if (!first.ok()) {
            return first.status();
        }
        std::vector<BorrowedMessage<T>> messages;
        messages.reserve(capacity);
        messages.push_back(std::move(*first));
        const uint64_t head_sequence =
            messages.front().metadata().sequence_num;
        Status terminal_status = Status::Ok();
        for (size_t offset = 1; offset < capacity; ++offset) {
            Result<BorrowedMessage<T>> next =
                TryPollAtOffset(head_sequence, offset);
            if (!next.ok()) {
                if (next.status().code() != StatusCode::kWouldBlock) {
                    terminal_status = next.status();
                }
                break;
            }
            messages.push_back(std::move(*next));
        }
        batch_polled_messages_.fetch_add(messages.size(),
                                         std::memory_order_relaxed);
        return BorrowedMessageBatch<T>(std::move(messages), terminal_status);
    }

    Result<BorrowedMessageBatch<T>> PollBatch(
        size_t capacity,
        Deadline deadline = Deadline::Infinite()) noexcept {
        for (;;) {
            Result<BorrowedMessageBatch<T>> batch = TryPollBatch(capacity);
            if (batch.ok()) {
                return batch;
            }
            if (batch.status().code() != StatusCode::kWouldBlock) {
                return batch.status();
            }
            if (deadline.expired()) {
                return Status::Error(StatusCode::kTimeout,
                                     "subscriber batch poll deadline expired");
            }
            std::this_thread::yield();
        }
    }

    uint64_t batch_poll_calls() const noexcept {
        return batch_poll_calls_.load(std::memory_order_relaxed);
    }

    uint64_t batch_polled_messages() const noexcept {
        return batch_polled_messages_.load(std::memory_order_relaxed);
    }

    Result<BorrowedMessage<T>> Poll(
        Deadline deadline = Deadline::Infinite()) noexcept {
        for (;;) {
            Result<BorrowedMessage<T>> message = TryPoll();
            if (message.ok()) {
                return message;
            }
            if (message.status().code() != StatusCode::kWouldBlock) {
                return message.status();
            }
            if (deadline.expired()) {
                return Status::Error(StatusCode::kTimeout,
                                     "subscriber poll deadline expired");
            }
            std::this_thread::yield();
        }
    }

    template <typename Callback>
    Status Poll(Callback&& callback, Deadline deadline) noexcept {
        Result<BorrowedMessage<T>> result = Poll(deadline);
        if (!result.ok()) {
            return result.status();
        }

        Status callback_status = Status::Ok();
#if defined(__cpp_exceptions)
        try {
#endif
            using Return = std::invoke_result_t<Callback, const BorrowedMessage<T>&>;
            if constexpr (std::is_same_v<Return, Status>) {
                callback_status = std::invoke(std::forward<Callback>(callback),
                                              std::as_const(*result));
            } else {
                static_assert(std::is_same_v<Return, void>,
                              "subscriber callback must return void or Status");
                std::invoke(std::forward<Callback>(callback),
                            std::as_const(*result));
            }
#if defined(__cpp_exceptions)
        } catch (...) {
            callback_status = Status::Error(
                StatusCode::kInternal, "subscriber callback threw an exception");
        }
#endif

        const Status ack = std::move(*result).Ack();
        return callback_status.ok() ? ack : callback_status;
    }

private:
    Result<BorrowedMessage<T>> TryPollAtOffset(
        uint64_t head_sequence, size_t offset) noexcept {
        if (std::holds_alternative<SpscChannel*>(channel_)) {
            Result<SpscChannel::Borrow> borrow =
                std::get<SpscChannel*>(channel_)->PollAtOffset(head_sequence,
                                                               offset);
            if (!borrow.ok()) {
                return borrow.status();
            }
            return ResolveBorrow(std::move(*borrow));
        }
        if (std::holds_alternative<MpscChannel*>(channel_)) {
            Result<MpscChannel::Borrow> borrow =
                std::get<MpscChannel*>(channel_)->PollAtOffset(head_sequence,
                                                               offset);
            if (!borrow.ok()) {
                return borrow.status();
            }
            return ResolveBorrow(std::move(*borrow));
        }
        Result<BroadcastChannel::Borrow> borrow =
            std::get<BroadcastChannel*>(channel_)
                ->PollAtOffsetWhileHeadBorrowed(broadcast_subscriber_, offset);
        if (!borrow.ok()) {
            return borrow.status();
        }
        return ResolveBorrow(std::move(*borrow));
    }

    static constexpr void ValidateStaticContract() noexcept {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        static_assert(std::is_standard_layout_v<T> &&
                          std::is_trivially_copyable_v<T>,
                      "fixed-layout messages must be SHM-safe POD types");
    }

    template <typename ChannelBorrow>
    Result<BorrowedMessage<T>> ResolveBorrow(ChannelBorrow&& borrow) noexcept {
        const IndexSlotSnapshot& slot = *borrow.slot();
        const Status metadata_status = ValidateMetadata(slot);
        if (!metadata_status.ok()) {
            std::move(borrow).Ack().ok();
            return metadata_status;
        }

        constexpr bool kChannelManagedCleanup =
            std::is_same_v<std::remove_cvref_t<ChannelBorrow>,
                           BroadcastChannel::Borrow>;
        ShmSharedPtr<T> borrow_pin;
        const T* value = nullptr;
        if constexpr (kChannelManagedCleanup) {
            if (pin_table_ == nullptr) {
                std::move(borrow).Ack().ok();
                return Status::Error(StatusCode::kUnsupported,
                                     "Broadcast subscriber requires a Pin table");
            }
            Result<ShmSharedPtr<T>> pinned = ShmSharedPtr<T>::Pin(
                *pin_table_, slot.payload, pin_owner_);
            if (!pinned.ok()) {
                const Status ack = std::move(borrow).Ack();
                if (ack.code() == StatusCode::kNotFound) {
                    auto gap = std::get<BroadcastChannel*>(channel_)->LastGap(
                        broadcast_subscriber_);
                    if (gap.ok()) {
                        return Status::Error(
                            StatusCode::kDegraded,
                            "broadcast payload was dropped before Borrow Pin");
                    }
                }
                return pinned.status();
            }
            value = pinned->get();
            borrow_pin = std::move(*pinned);
        } else {
            Result<SlabView> slab = allocator_->Inspect(slot.payload);
            if (!slab.ok()) {
                std::move(borrow).Ack().ok();
                return slab.status();
            }
            if (slab->state != ObjectState::kPublished ||
                slab->object_size != sizeof(T) || slab->capacity < sizeof(T) ||
                slab->type_id != StaticMessageTraits<T>::type_id ||
                slab->schema_short_id != StaticMessageTraits<T>::schema_short_id ||
                slab->layout_version != StaticMessageTraits<T>::layout_version ||
                slab->data == nullptr) {
                std::move(borrow).Ack().ok();
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "payload slab does not match static message traits");
            }
            value = static_cast<const T*>(slab->data);
        }

        return BorrowedMessage<T>(
            allocator_, pin_table_, pin_owner_,
            std::forward<ChannelBorrow>(borrow), std::move(borrow_pin), value,
            MetadataFromSlot(slot), kChannelManagedCleanup);
    }

    static Status ValidateMetadata(const IndexSlotSnapshot& slot) noexcept {
        if (slot.msg_type != StaticMessageTraits<T>::message_type ||
            slot.schema_version != StaticMessageTraits<T>::schema_version ||
            slot.schema_short_id != StaticMessageTraits<T>::schema_short_id ||
            slot.schema_layout_version !=
                StaticMessageTraits<T>::layout_version ||
            slot.payload_len != sizeof(T) || slot.payload.IsNull()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "index slot does not match static message traits");
        }
        if ((slot.flags & kIndexSlotFlagReservedMask) != 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "index slot contains reserved flag bits");
        }
        return Status::Ok();
    }

    CentralSlabAllocator* allocator_;
    std::variant<SpscChannel*, MpscChannel*, BroadcastChannel*> channel_;
    ShmPinTable* pin_table_ = nullptr;
    ProcessIdentity pin_owner_;
    BroadcastChannel::SubscriberHandle broadcast_subscriber_;
    std::atomic<uint64_t> batch_poll_calls_{0};
    std::atomic<uint64_t> batch_polled_messages_{0};
};

}  // namespace mino

#endif  // MINO_RUNTIME_SUBSCRIBER_H_
