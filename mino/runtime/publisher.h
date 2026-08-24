// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_PUBLISHER_H_
#define MINO_RUNTIME_PUBLISHER_H_

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/deadline.h"
#include "mino/runtime/delivery_receipt.h"
#include "mino/runtime/journal_channel_recovery.h"
#include "mino/runtime/message_traits.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/shm/channel/mpsc_channel.h"
#include "mino/shm/channel/queue_full_policy.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino {

template <typename T>
class Publisher;

// Exclusive construction window for one fixed-layout SHM object. An active
// builder owns an unpublished allocator slot; destruction follows the RAII
// abort path so validation/reservation failures cannot leak allocations.
template <typename T>
class MessageBuilder {
public:
    MessageBuilder() noexcept = default;
    MessageBuilder(const MessageBuilder&) = delete;
    MessageBuilder& operator=(const MessageBuilder&) = delete;

    MessageBuilder(MessageBuilder&& other) noexcept { MoveFrom(other); }

    MessageBuilder& operator=(MessageBuilder&& other) noexcept {
        if (this != &other) {
            AbortIfActive();
            MoveFrom(other);
        }
        return *this;
    }

    ~MessageBuilder() { AbortIfActive(); }

    T* get() noexcept { return value_; }
    const T* get() const noexcept { return value_; }
    T* operator->() noexcept { return value_; }
    const T* operator->() const noexcept { return value_; }
    T& operator*() noexcept { return *value_; }
    const T& operator*() const noexcept { return *value_; }

    bool active() const noexcept { return active_; }
    ShmHandle handle() const noexcept { return handle_; }

    Result<MutableBuildView> AllocateChild(
        const AllocationRequest& request) noexcept {
        if (!active_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "message builder is not active");
        }
        if (journal_ == nullptr) {
            return Status::Error(StatusCode::kUnsupported,
                                 "message builder has no allocation journal");
        }
        MINO_ASSIGN_OR_RETURN(ShmHandle child,
                              journal_->AllocateChild(transaction_, request));
        Result<MutableBuildView> build = allocator_->BeginBuild(child);
        if (!build.ok()) {
            (void)journal_->Abort(transaction_);
            active_ = false;
            return build.status();
        }
        return build;
    }

    Status RegisterChild(ShmHandle child) noexcept {
        if (!active_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "message builder is not active");
        }
        if (journal_ == nullptr) {
            return Status::Error(StatusCode::kUnsupported,
                                 "message builder has no allocation journal");
        }
        return journal_->RegisterChild(transaction_, child);
    }

private:
    friend class Publisher<T>;

    MessageBuilder(CentralSlabAllocator* allocator, ShmHandle handle,
                   T* value, AllocationJournal* journal = nullptr,
                   AllocationTransaction transaction = {}) noexcept
        : allocator_(allocator),
          handle_(handle),
          value_(value),
          journal_(journal),
          transaction_(transaction),
          active_(true) {}

    Status Abort() noexcept {
        if (!active_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "message builder is not active");
        }
        active_ = false;
        std::destroy_at(value_);
        value_ = nullptr;
        CentralSlabAllocator* allocator = allocator_;
        AllocationJournal* journal = journal_;
        const AllocationTransaction transaction = transaction_;
        const bool journal_committed = journal_committed_;
        allocator_ = nullptr;
        journal_ = nullptr;
        transaction_ = {};
        journal_committed_ = false;
        if (journal == nullptr) {
            return allocator->Abort(handle_);
        }
        return journal_committed ? journal->RollbackCommitted(transaction)
                                 : journal->Abort(transaction);
    }

    Status CommitJournal(const PublicationBinding& binding) noexcept {
        if (journal_ == nullptr) {
            return Status::Ok();
        }
        const Status status = journal_->Commit(transaction_, binding);
        if (status.ok()) {
            journal_committed_ = true;
        }
        return status;
    }

    Status FinalizeJournal() noexcept {
        if (journal_ == nullptr) {
            return Status::Ok();
        }
        if (!journal_committed_) {
            return Status::Error(StatusCode::kInternal,
                                 "allocation journal is not committed");
        }
        const Status status = journal_->FinalizeCommit(transaction_);
        if (status.ok()) {
            journal_ = nullptr;
            transaction_ = {};
            journal_committed_ = false;
        }
        return status;
    }

    Status RollbackJournal() noexcept {
        std::destroy_at(value_);
        value_ = nullptr;
        if (journal_ == nullptr) {
            MINO_RETURN_IF_ERROR(allocator_->Retire(handle_));
            return allocator_->Reclaim(handle_);
        }
        if (!journal_committed_) {
            return Status::Error(StatusCode::kInternal,
                                 "allocation journal is not committed");
        }
        const Status status = journal_->RollbackCommitted(transaction_);
        if (status.ok()) {
            journal_ = nullptr;
            transaction_ = {};
            journal_committed_ = false;
        }
        return status;
    }

    void Disarm() noexcept {
        active_ = false;
        allocator_ = nullptr;
        value_ = nullptr;
        journal_ = nullptr;
        transaction_ = {};
        journal_committed_ = false;
    }

    void AbortIfActive() noexcept {
        if (active_) {
            Abort().ok();
        }
    }

    void MoveFrom(MessageBuilder& other) noexcept {
        allocator_ = other.allocator_;
        handle_ = other.handle_;
        value_ = other.value_;
        journal_ = other.journal_;
        transaction_ = other.transaction_;
        journal_committed_ = other.journal_committed_;
        active_ = other.active_;
        other.allocator_ = nullptr;
        other.value_ = nullptr;
        other.journal_ = nullptr;
        other.transaction_ = {};
        other.journal_committed_ = false;
        other.active_ = false;
    }

    CentralSlabAllocator* allocator_ = nullptr;
    ShmHandle handle_;
    T* value_ = nullptr;
    AllocationJournal* journal_ = nullptr;
    AllocationTransaction transaction_;
    bool journal_committed_ = false;
    bool active_ = false;
};

struct PublisherOptions {
    QueueFullPolicy queue_full_policy = QueueFullPolicy::kFail;
    uint32_t sample_rate = 1;
};

// Outcome of an ordered batch publish. Items before first_failed_index reached
// their individual channel Commit point; the failed item and every later item
// are aborted when they belong to this Publisher. Policy-driven DropNewest and
// Sample rejection is explicit as kDegraded instead of the single-item API's
// compatibility success.
struct BatchPublishResult {
    static constexpr size_t kNoFailure = std::numeric_limits<size_t>::max();

    size_t requested_count = 0;
    size_t committed_count = 0;
    size_t dropped_input_count = 0;
    size_t first_failed_index = kNoFailure;
    Status first_error = Status::Ok();
    Status cleanup_error = Status::Ok();

    bool ok() const noexcept { return first_error.ok(); }
};

// Fixed-layout Publisher facade over an SPSC Channel. The public type is
// intentionally independent of Schema internals: StaticMessageTraits<T> is the
// seam CodeGen specializes.
template <typename T>
class Publisher {
public:
    // Compatibility constructors model one process-local channel and bind it
    // to this stable, non-zero ID. Multi-channel runtimes should use an
    // explicit channel_id overload and register every Publisher with the same
    // JournalChannelRecoveryCoordinator.
    static constexpr uint64_t kDefaultSingleChannelId = 1;

    Publisher(CentralSlabAllocator& allocator, SpscChannel& channel,
              PublisherOptions options = {}) noexcept
        : Publisher(allocator, channel, kDefaultSingleChannelId, options) {}

    Publisher(CentralSlabAllocator& allocator, SpscChannel& channel,
              uint64_t channel_id, PublisherOptions options = {}) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          channel_id_(NormalizeChannelId(channel_id)),
          options_(options) {
        ValidateStaticContract();
    }

    Publisher(CentralSlabAllocator& allocator, SpscChannel& channel,
              AllocationJournal& journal,
              const ProcessIdentity& owner = ProcessIdentity::Current(),
              PublisherOptions options = {}) noexcept
        : Publisher(allocator, channel, kDefaultSingleChannelId, journal,
                    owner, options) {}

    Publisher(CentralSlabAllocator& allocator, SpscChannel& channel,
              uint64_t channel_id, AllocationJournal& journal,
              const ProcessIdentity& owner = ProcessIdentity::Current(),
              PublisherOptions options = {}) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          channel_id_(NormalizeChannelId(channel_id)),
          allocation_journal_(&journal),
          allocation_owner_(owner),
          options_(options) {
        ValidateStaticContract();
    }

    Publisher(CentralSlabAllocator& allocator, MpscChannel& channel,
              MpscChannel::ProducerIdentity producer_identity,
              PublisherOptions options = {}) noexcept
        : Publisher(allocator, channel, kDefaultSingleChannelId,
                    producer_identity, options) {}

    Publisher(CentralSlabAllocator& allocator, MpscChannel& channel,
              uint64_t channel_id,
              MpscChannel::ProducerIdentity producer_identity,
              PublisherOptions options = {}) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          channel_id_(NormalizeChannelId(channel_id)),
          mpsc_identity_(producer_identity),
          options_(options) {
        ValidateStaticContract();
    }

    Publisher(CentralSlabAllocator& allocator, MpscChannel& channel,
              MpscChannel::ProducerIdentity producer_identity,
              AllocationJournal& journal,
              PublisherOptions options = {}) noexcept
        : Publisher(allocator, channel, kDefaultSingleChannelId,
                    producer_identity, journal, options) {}

    Publisher(CentralSlabAllocator& allocator, MpscChannel& channel,
              uint64_t channel_id,
              MpscChannel::ProducerIdentity producer_identity,
              AllocationJournal& journal,
              PublisherOptions options = {}) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          channel_id_(NormalizeChannelId(channel_id)),
          mpsc_identity_(producer_identity),
          allocation_journal_(&journal),
          allocation_owner_(producer_identity.owner),
          options_(options) {
        ValidateStaticContract();
    }

    Publisher(CentralSlabAllocator& allocator, BroadcastChannel& channel,
              ShmPinTable& pins, PublisherOptions options = {}) noexcept
        : Publisher(allocator, channel, kDefaultSingleChannelId, pins,
                    options) {}

    Publisher(CentralSlabAllocator& allocator, BroadcastChannel& channel,
              uint64_t channel_id, ShmPinTable& pins,
              PublisherOptions options = {}) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          channel_id_(NormalizeChannelId(channel_id)),
          options_(options) {
        channel.SetPayloadRetireObserver(&ShmPinTable::RetirePayloadCallback,
                                         &pins);
        ValidateStaticContract();
    }

    Publisher(CentralSlabAllocator& allocator, BroadcastChannel& channel,
              ShmPinTable& pins, AllocationJournal& journal,
              const ProcessIdentity& owner = ProcessIdentity::Current(),
              PublisherOptions options = {}) noexcept
        : Publisher(allocator, channel, kDefaultSingleChannelId, pins, journal,
                    owner, options) {}

    Publisher(CentralSlabAllocator& allocator, BroadcastChannel& channel,
              uint64_t channel_id, ShmPinTable& pins,
              AllocationJournal& journal,
              const ProcessIdentity& owner = ProcessIdentity::Current(),
              PublisherOptions options = {}) noexcept
        : allocator_(&allocator),
          channel_(&channel),
          channel_id_(NormalizeChannelId(channel_id)),
          allocation_journal_(&journal),
          allocation_owner_(owner),
          options_(options) {
        channel.SetPayloadRetireObserver(&ShmPinTable::RetirePayloadCallback,
                                         &pins);
        ValidateStaticContract();
    }

    Result<MessageBuilder<T>> Allocate(
        Deadline deadline = Deadline::Infinite()) noexcept {
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "publisher allocation deadline expired");
        }
        AllocationRequest request;
        request.object_size = sizeof(T);
        request.type_id = StaticMessageTraits<T>::type_id;
        request.schema = SchemaIdentity{
            .short_id = StaticMessageTraits<T>::schema_short_id,
            .layout_version = StaticMessageTraits<T>::layout_version,
        };
        request.alignment = alignof(T);

        if (allocation_journal_ == nullptr) {
            MINO_ASSIGN_OR_RETURN(ShmHandle handle, allocator_->Allocate(request));
            Result<MutableBuildView> build = allocator_->BeginBuild(handle);
            if (!build.ok()) {
                allocator_->Abort(handle).ok();
                return build.status();
            }
            if (build->capacity < sizeof(T) || build->object_size != sizeof(T) ||
                build->data == nullptr) {
                allocator_->Abort(handle).ok();
                return Status::Error(StatusCode::kCorruption,
                                     "allocator returned an invalid build view");
            }
            T* value = std::construct_at(static_cast<T*>(build->data));
            return MessageBuilder<T>(allocator_, handle, value);
        }

        MINO_ASSIGN_OR_RETURN(
            AllocationTransaction transaction,
            allocation_journal_->Begin(allocation_owner_));
        Result<ShmHandle> allocated =
            allocation_journal_->AllocateRoot(transaction, request);
        if (!allocated.ok()) {
            (void)allocation_journal_->Abort(transaction);
            return allocated.status();
        }
        const ShmHandle handle = *allocated;
        Result<MutableBuildView> build = allocator_->BeginBuild(handle);
        if (!build.ok()) {
            (void)allocation_journal_->Abort(transaction);
            return build.status();
        }
        if (build->capacity < sizeof(T) || build->object_size != sizeof(T) ||
            build->data == nullptr) {
            (void)allocation_journal_->Abort(transaction);
            return Status::Error(StatusCode::kCorruption,
                                 "allocator returned an invalid build view");
        }
        T* value = std::construct_at(static_cast<T*>(build->data));
        return MessageBuilder<T>(allocator_, handle, value,
                                 allocation_journal_, transaction);
    }

    // Publishes locally. Success means kLocalPublished only. A policy-driven
    // DropNewest/Sample outcome is normalized to success and counted, matching
    // the Runtime error contract rather than exposing Channel kDegraded.
    Status PublishLocal(MessageBuilder<T>&& builder,
                        Deadline deadline = Deadline::Infinite()) noexcept {
        return PublishLocalImpl(builder, deadline, nullptr, nullptr);
    }

    // Bounded ordered publish. The span size is the requested capacity and may
    // not exceed kMaxBatchMessages. Every item independently executes the same
    // Validate -> Journal publish/bind -> Reserve -> Commit pipeline as
    // PublishLocal(), using one shared absolute Deadline. Processing stops at
    // the first error; no later builder is silently retained or dropped.
    static constexpr size_t kMaxBatchMessages = 1024;

    BatchPublishResult PublishBatch(
        std::span<MessageBuilder<T>> builders,
        Deadline deadline = Deadline::Infinite()) noexcept {
        batch_publish_calls_.fetch_add(1, std::memory_order_relaxed);
        BatchPublishResult outcome;
        outcome.requested_count = builders.size();
        if (builders.size() > kMaxBatchMessages) {
            outcome.first_failed_index = 0;
            outcome.first_error = Status::Error(
                StatusCode::kInvalidArgument,
                "publish batch exceeds Publisher::kMaxBatchMessages");
            batch_failed_items_.fetch_add(builders.size(),
                                          std::memory_order_relaxed);
            return outcome;
        }

        for (size_t i = 0; i < builders.size(); ++i) {
            bool locally_published = false;
            const Status status = PublishLocalImpl(
                builders[i], deadline, nullptr, &locally_published);
            if (!status.ok() || !locally_published) {
                outcome.first_failed_index = i;
                if (!status.ok()) {
                    outcome.first_error = status;
                } else {
                    outcome.dropped_input_count = 1;
                    outcome.first_error = Status::Error(
                        StatusCode::kDegraded,
                        "publish batch item was rejected by QueueFullPolicy");
                }
                outcome.cleanup_error = AbortBatchRemainder(builders, i + 1);
                batch_failed_items_.fetch_add(builders.size() - i,
                                              std::memory_order_relaxed);
                return outcome;
            }
            ++outcome.committed_count;
            batch_committed_items_.fetch_add(1, std::memory_order_relaxed);
        }
        return outcome;
    }

    BatchPublishResult PublishBatch(
        std::vector<MessageBuilder<T>>& builders,
        Deadline deadline = Deadline::Infinite()) noexcept {
        return PublishBatch(
            std::span<MessageBuilder<T>>(builders.data(), builders.size()),
            deadline);
    }

    Result<DeliveryReceipt> Publish(
        MessageBuilder<T>&& builder, OutstandingReceiptTable& receipts,
        const PublisherReceiptIdentity& publisher_identity,
        std::span<const DeliveryTarget> target_snapshot,
        const DeliveryRequirement& requirement,
        Deadline deadline = Deadline::Infinite()) noexcept {
        if (!builder.active() || builder.allocator_ != allocator_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "builder does not belong to this publisher");
        }
        if (deadline.expired()) {
            return AbortWith(builder, Status::Error(
                StatusCode::kTimeout, "publish deadline expired"));
        }
        const Status validation = StaticMessageTraits<T>::Validate(*builder);
        if (!validation.ok()) {
            return AbortWith(builder, validation);
        }
        const Status manifest = ValidateOwnedGraphManifest(builder);
        if (!manifest.ok()) {
            return AbortWith(builder, manifest);
        }

        Result<OutstandingReceiptTable::Reservation> receipt_reservation =
            receipts.Reserve(publisher_identity, target_snapshot, requirement);
        if (!receipt_reservation.ok()) {
            return AbortWith(builder, receipt_reservation.status());
        }

        uint64_t source_sequence = 0;
        bool locally_published = false;
        const Status local = PublishLocalImpl(
            builder, deadline, &source_sequence, &locally_published);
        if (!local.ok()) {
            return local;
        }
        if (!locally_published) {
            receipt_reservation->Cancel();
            return Status::Error(
                StatusCode::kDegraded,
                "local publish was dropped; delivery receipt canceled");
        }
        return std::move(*receipt_reservation).Commit(source_sequence);
    }

    Status Abort(MessageBuilder<T>&& builder) noexcept {
        if (!builder.active() || builder.allocator_ != allocator_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "builder does not belong to this publisher");
        }
        return builder.Abort();
    }

    uint64_t published_count() const noexcept {
        return published_count_.load(std::memory_order_relaxed);
    }

    uint64_t dropped_count() const noexcept {
        return dropped_count_.load(std::memory_order_relaxed);
    }

    uint64_t journal_cleanup_debt_count() const noexcept {
        return journal_cleanup_debt_count_.load(std::memory_order_relaxed);
    }

    uint64_t batch_publish_calls() const noexcept {
        return batch_publish_calls_.load(std::memory_order_relaxed);
    }

    uint64_t batch_committed_items() const noexcept {
        return batch_committed_items_.load(std::memory_order_relaxed);
    }

    uint64_t batch_failed_items() const noexcept {
        return batch_failed_items_.load(std::memory_order_relaxed);
    }

    uint64_t channel_id() const noexcept { return channel_id_; }

    // Registers this Publisher's channel and stable ID in one operation. A
    // Broadcast Publisher additionally supplies the read-only publication view
    // because BroadcastChannel intentionally exposes no shared-memory base.
    Status RegisterRecoveryChannel(
        JournalChannelRecoveryCoordinator& coordinator,
        const BroadcastPublicationView* broadcast_view = nullptr) const {
        if (std::holds_alternative<SpscChannel*>(channel_)) {
            return coordinator.RegisterChannel(
                channel_id_, *std::get<SpscChannel*>(channel_));
        }
        if (std::holds_alternative<MpscChannel*>(channel_)) {
            return coordinator.RegisterChannel(
                channel_id_, *std::get<MpscChannel*>(channel_));
        }
        if (broadcast_view == nullptr) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "broadcast recovery registration requires a publication view");
        }
        return coordinator.RegisterChannel(channel_id_, *broadcast_view);
    }

private:
    Status AbortBatchRemainder(std::span<MessageBuilder<T>> builders,
                               size_t first) noexcept {
        Status first_error = Status::Ok();
        for (size_t i = first; i < builders.size(); ++i) {
            MessageBuilder<T>& builder = builders[i];
            if (!builder.active() || builder.allocator_ != allocator_) {
                continue;
            }
            const Status aborted = builder.Abort();
            if (first_error.ok() && !aborted.ok()) {
                first_error = aborted;
            }
        }
        return first_error;
    }

    Status PublishLocalImpl(MessageBuilder<T>& builder, Deadline deadline,
                            uint64_t* source_sequence,
                            bool* locally_published) noexcept {
        if (locally_published != nullptr) {
            *locally_published = false;
        }
        if (!builder.active() || builder.allocator_ != allocator_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "builder does not belong to this publisher");
        }
        if (deadline.expired()) {
            return AbortWith(builder, Status::Error(
                StatusCode::kTimeout, "publish deadline expired"));
        }

        const Status validation = StaticMessageTraits<T>::Validate(*builder);
        if (!validation.ok()) {
            return AbortWith(builder, validation);
        }
        const Status manifest = ValidateOwnedGraphManifest(builder);
        if (!manifest.ok()) {
            return AbortWith(builder, manifest);
        }

        if (std::holds_alternative<SpscChannel*>(channel_)) {
            return FinishPublish(builder, ReserveSpsc(deadline),
                                 source_sequence, locally_published);
        }
        if (std::holds_alternative<MpscChannel*>(channel_)) {
            return FinishPublish(builder, ReserveMpsc(deadline), source_sequence,
                                 locally_published);
        }
        return FinishPublish(builder, ReserveBroadcast(deadline),
                             source_sequence, locally_published);
    }

    static constexpr bool SupportsOwnedGraphCollection() noexcept {
        if constexpr (requires {
                          StaticMessageTraits<T>::kOwnedGraphCollectionSupported;
                          StaticMessageTraits<T>::kMaxOwnedGraphHandles;
                      }) {
            return StaticMessageTraits<T>::kOwnedGraphCollectionSupported;
        }
        return false;
    }

    static Status ValidateOwnedGraphManifest(
        MessageBuilder<T>& builder) noexcept {
        if constexpr (!SupportsOwnedGraphCollection()) {
            return Status::Ok();
        } else {
            static_assert(StaticMessageTraits<T>::kMaxOwnedGraphHandles > 0,
                          "owned graph collector must have root capacity");
            std::array<ShmHandle,
                       StaticMessageTraits<T>::kMaxOwnedGraphHandles>
                reachable{};
            size_t handle_count = 0;
            MINO_RETURN_IF_ERROR(CollectOwnedGraph(
                builder.handle_, *builder.value_, reachable, handle_count));
            if (handle_count == 0 || handle_count > reachable.size()) {
                return Status::Error(StatusCode::kCorruption,
                                     "owned graph collector returned an invalid size");
            }
            if (handle_count == 1) return Status::Ok();
            if (builder.journal_ == nullptr) {
                return Status::Error(
                    StatusCode::kUnsupported,
                    "owned graph publication requires an allocation journal");
            }
            return builder.journal_->ValidateManifest(
                builder.transaction_,
                std::span<const ShmHandle>(reachable.data(), handle_count));
        }
    }

    static constexpr void ValidateStaticContract() noexcept {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        static_assert(std::is_standard_layout_v<T>,
                      "SHM messages must have standard layout");
        static_assert(std::is_trivially_copyable_v<T>,
                      "fixed-layout messages must be trivially copyable");
        static_assert(std::is_trivially_default_constructible_v<T>,
                      "fixed-layout messages must be trivially default constructible");
        static_assert(std::is_trivially_destructible_v<T>,
                      "fixed-layout messages must be trivially destructible");
    }

    template <typename Reservation>
    Status FinishPublish(MessageBuilder<T>& builder,
                         Result<Reservation> reservation,
                         uint64_t* source_sequence,
                         bool* locally_published) noexcept {
        if (!reservation.ok()) {
            if (reservation.status().code() == StatusCode::kDegraded) {
                const Status abort = builder.Abort();
                if (!abort.ok()) {
                    return abort;
                }
                dropped_count_.fetch_add(1, std::memory_order_relaxed);
                return Status::Ok();
            }
            return AbortWith(builder, reservation.status());
        }

        uint64_t dropped_messages = 0;
        if constexpr (std::is_same_v<Reservation,
                                     BroadcastChannel::Reservation>) {
            dropped_messages = reservation->dropped_messages();
        }

        const Status published =
            builder.journal_ == nullptr
                ? allocator_->Publish(builder.handle_)
                : builder.journal_->PublishGraph(builder.transaction_);
        if (!published.ok()) {
            return AbortWith(builder, published);
        }

        IndexSlot* slot = reservation->slot();
        slot->msg_type = StaticMessageTraits<T>::message_type;
        slot->schema_version = StaticMessageTraits<T>::schema_version;
        slot->schema_short_id = StaticMessageTraits<T>::schema_short_id;
        slot->schema_layout_version = StaticMessageTraits<T>::layout_version;
        slot->reserved0 = 0;
        slot->timestamp_ns = MonotonicNowNs();
        slot->payload = builder.handle_;
        slot->payload_len = sizeof(T);
        slot->flags = StaticMessageTraits<T>::index_flags;

        const uint64_t committed_sequence =
            slot->sequence_num.load(std::memory_order_relaxed);
        PublicationBinding binding{
            .channel_kind = PublicationKind(),
            .channel_id = channel_id_,
            .sequence = committed_sequence,
            .payload = builder.handle_,
        };
        const Status journal_committed = builder.CommitJournal(binding);
        if (!journal_committed.ok()) {
            return AbortWith(builder, journal_committed);
        }
        const Status committed = std::move(*reservation).Commit();
        if (!committed.ok()) {
            const Status rollback = builder.RollbackJournal();
            builder.Disarm();
            return rollback.ok() ? committed : rollback;
        }
        const Status finalized = builder.FinalizeJournal();
        builder.Disarm();
        if (!finalized.ok()) {
            // Channel Commit is the publication linearization point. Reporting a
            // normal publish failure here would invite a duplicate retry even
            // though consumers can already observe the message. Persisted
            // binding + kCommitted/kFinalizing state lets recovery discharge
            // this cleanup debt later.
            journal_cleanup_debt_count_.fetch_add(1,
                                                  std::memory_order_relaxed);
        }
        if constexpr (std::is_same_v<Reservation,
                                     BroadcastChannel::Reservation>) {
            std::get<BroadcastChannel*>(channel_)->CollectGarbage();
        }
        if (source_sequence != nullptr) {
            *source_sequence = committed_sequence;
        }
        if (locally_published != nullptr) {
            *locally_published = true;
        }
        if (dropped_messages != 0) {
            dropped_count_.fetch_add(dropped_messages,
                                     std::memory_order_relaxed);
        }
        published_count_.fetch_add(1, std::memory_order_relaxed);
        return Status::Ok();
    }

    Result<SpscChannel::Reservation> ReserveSpsc(Deadline deadline) noexcept {
        SpscChannel* channel = std::get<SpscChannel*>(channel_);
        if (options_.queue_full_policy != QueueFullPolicy::kBlock) {
            return channel->Reserve(options_.queue_full_policy,
                                    options_.sample_rate);
        }
        for (;;) {
            Result<SpscChannel::Reservation> reservation = channel->TryReserve();
            if (reservation.ok()) {
                return reservation;
            }
            if (reservation.status().code() != StatusCode::kWouldBlock) {
                return reservation.status();
            }
            if (deadline.expired()) {
                return Status::Error(StatusCode::kTimeout,
                                     "publish blocked until deadline");
            }
            std::this_thread::yield();
        }
    }

    Result<MpscChannel::Reservation> ReserveMpsc(Deadline deadline) noexcept {
        MpscChannel* channel = std::get<MpscChannel*>(channel_);
        if (options_.queue_full_policy != QueueFullPolicy::kBlock) {
            return channel->Reserve(mpsc_identity_, options_.queue_full_policy,
                                    options_.sample_rate);
        }
        for (;;) {
            Result<MpscChannel::Reservation> reservation =
                channel->TryReserve(mpsc_identity_);
            if (reservation.ok()) {
                return reservation;
            }
            const StatusCode code = reservation.status().code();
            if (code != StatusCode::kWouldBlock &&
                code != StatusCode::kResourceExhausted) {
                return reservation.status();
            }
            if (deadline.expired()) {
                return Status::Error(StatusCode::kTimeout,
                                     "publish blocked until deadline");
            }
            std::this_thread::yield();
        }
    }

    Result<BroadcastChannel::Reservation> ReserveBroadcast(
        Deadline deadline) noexcept {
        BroadcastChannel* channel = std::get<BroadcastChannel*>(channel_);
        if (options_.queue_full_policy != QueueFullPolicy::kBlock) {
            return channel->Reserve(options_.queue_full_policy,
                                    options_.sample_rate);
        }
        for (;;) {
            Result<BroadcastChannel::Reservation> reservation =
                channel->TryReserve();
            if (reservation.ok()) {
                return reservation;
            }
            if (reservation.status().code() != StatusCode::kWouldBlock) {
                return reservation.status();
            }
            if (deadline.expired()) {
                return Status::Error(StatusCode::kTimeout,
                                     "publish blocked until deadline");
            }
            std::this_thread::yield();
        }
    }

    static Status AbortWith(MessageBuilder<T>& builder,
                            const Status& original) noexcept {
        const Status abort = builder.Abort();
        return abort.ok() ? original : abort;
    }

    PublicationChannelKind PublicationKind() const noexcept {
        if (std::holds_alternative<SpscChannel*>(channel_)) {
            return PublicationChannelKind::kSpsc;
        }
        if (std::holds_alternative<MpscChannel*>(channel_)) {
            return PublicationChannelKind::kMpsc;
        }
        return PublicationChannelKind::kBroadcast;
    }

    static constexpr uint64_t NormalizeChannelId(
        uint64_t channel_id) noexcept {
        // Zero was the historical hard-coded binding. Treat it as the explicit
        // single-channel compatibility choice, never as a persisted channel ID.
        return channel_id == 0 ? kDefaultSingleChannelId : channel_id;
    }

    static uint64_t MonotonicNowNs() noexcept {
        const auto now = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }

    CentralSlabAllocator* allocator_;
    std::variant<SpscChannel*, MpscChannel*, BroadcastChannel*> channel_;
    uint64_t channel_id_ = kDefaultSingleChannelId;
    MpscChannel::ProducerIdentity mpsc_identity_{};
    AllocationJournal* allocation_journal_ = nullptr;
    ProcessIdentity allocation_owner_;
    PublisherOptions options_;
    std::atomic<uint64_t> published_count_{0};
    std::atomic<uint64_t> dropped_count_{0};
    std::atomic<uint64_t> journal_cleanup_debt_count_{0};
    std::atomic<uint64_t> batch_publish_calls_{0};
    std::atomic<uint64_t> batch_committed_items_{0};
    std::atomic<uint64_t> batch_failed_items_{0};
};

}  // namespace mino

#endif  // MINO_RUNTIME_PUBLISHER_H_
