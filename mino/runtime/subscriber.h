// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_SUBSCRIBER_H_
#define MINO_RUNTIME_SUBSCRIBER_H_

#include <functional>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/runtime/deadline.h"
#include "mino/runtime/message.h"
#include "mino/runtime/message_traits.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/shm/channel/mpsc_channel.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino {

template <typename T>
class Subscriber;

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

    Status Ack() && noexcept {
        if (!active_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "borrowed message is not active");
        }
        active_ = false;
        value_ = nullptr;

        const Status channel_ack = std::visit(
            [](auto& borrow) { return std::move(borrow).Ack(); }, borrow_);
        Status retire = Status::Ok();
        Status reclaim = Status::Ok();
        Status release_pin = Status::Ok();
        if (!payload_cleanup_by_channel_) {
            retire = allocator_->Retire(metadata_.payload);
            if (retire.ok() &&
                (pin_table_ == nullptr ||
                 pin_table_->PinCount(metadata_.payload) == 0)) {
                reclaim = allocator_->Reclaim(metadata_.payload);
            }
        }
        if (borrow_pin_.active()) {
            release_pin = borrow_pin_.Release();
        }
        allocator_ = nullptr;
        pin_table_ = nullptr;
        pin_owner_ = {};

        if (!channel_ack.ok()) {
            return channel_ack;
        }
        if (!retire.ok()) {
            return retire;
        }
        if (!reclaim.ok()) {
            return reclaim;
        }
        return release_pin;
    }

private:
    friend class Subscriber<T>;

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

// D2-10 fixed-layout Subscriber facade over an SPSC Channel.
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
    static constexpr void ValidateStaticContract() noexcept {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        static_assert(std::is_standard_layout_v<T> &&
                          std::is_trivially_copyable_v<T>,
                      "D2 fixed-layout messages must be SHM-safe POD types");
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
};

}  // namespace mino

#endif  // MINO_RUNTIME_SUBSCRIBER_H_
