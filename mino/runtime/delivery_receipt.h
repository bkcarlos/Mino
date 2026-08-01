// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_DELIVERY_RECEIPT_H_
#define MINO_RUNTIME_DELIVERY_RECEIPT_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/runtime/deadline.h"

namespace mino {

struct ReceiptId {
    uint64_t value = 0;
    friend constexpr bool operator==(ReceiptId, ReceiptId) = default;
};

enum class DeliveryStage : uint32_t {
    kLocalPublished = 0,
    kRemoteAccepted = 1,
    kRecorderBuffered = 2,
    kStorageWritten = 3,
    kStorageDurable = 4,
};

enum class CompletionPolicy : uint32_t {
    kAll = 0,
    kAny = 1,
    kQuorum = 2,
};

struct DeliveryRequirement {
    DeliveryStage stage = DeliveryStage::kLocalPublished;
    CompletionPolicy completion = CompletionPolicy::kAll;
    uint32_t quorum = 0;
    Deadline deadline = Deadline::Infinite();
};

enum class DeliveryTargetKind : uint32_t {
    kNode = 0,
    kRecorder = 1,
};

struct DeliveryTarget {
    DeliveryTargetKind kind = DeliveryTargetKind::kNode;
    uint64_t id = 0;
    friend constexpr bool operator==(DeliveryTarget, DeliveryTarget) = default;
};

struct TargetDeliveryStatus {
    DeliveryTarget target;
    DeliveryStage reached_stage = DeliveryStage::kLocalPublished;
    Status status = Status::Error(StatusCode::kWouldBlock,
                                  "delivery target is still pending");
};

struct PublisherReceiptIdentity {
    ProcessIdentity process;
    PublisherId publisher_id;

    friend bool operator==(const PublisherReceiptIdentity&,
                           const PublisherReceiptIdentity&) = default;
};

class OutstandingReceiptTable;
struct OutstandingReceiptTableTestAccess;

class DeliveryReceipt {
public:
    struct State;

    DeliveryReceipt() = default;

    ReceiptId id() const noexcept;
    bool valid() const noexcept { return state_ != nullptr; }

    Result<std::vector<TargetDeliveryStatus>> Wait(Deadline deadline) const;
    void CancelWait() noexcept;

private:
    friend class OutstandingReceiptTable;

    explicit DeliveryReceipt(std::shared_ptr<State> state) noexcept
        : state_(std::move(state)) {}

    std::shared_ptr<State> state_;
};

class OutstandingReceiptTable {
public:
    struct Limits {
        uint32_t max_outstanding = 1024;
        uint32_t max_per_publisher = 256;
    };

    // Move-only admission capability. Until Commit(), the table slot is
    // reserved but not visible to Acknowledge() or outstanding(). Destruction
    // and Cancel() release the reservation.
    class Reservation {
    public:
        Reservation() = default;
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;
        Reservation(Reservation&& other) noexcept;
        Reservation& operator=(Reservation&& other) noexcept;
        ~Reservation();

        bool valid() const noexcept { return table_ != nullptr; }
        DeliveryReceipt Commit(uint64_t source_sequence) && noexcept;
        void Cancel() noexcept;

    private:
        friend class OutstandingReceiptTable;

        Reservation(OutstandingReceiptTable* table, ReceiptId id) noexcept
            : table_(table), id_(id) {}

        OutstandingReceiptTable* table_ = nullptr;
        ReceiptId id_;
    };

    OutstandingReceiptTable();
    explicit OutstandingReceiptTable(Limits limits);
    ~OutstandingReceiptTable();

    OutstandingReceiptTable(const OutstandingReceiptTable&) = delete;
    OutstandingReceiptTable& operator=(const OutstandingReceiptTable&) = delete;

    // Performs validation and reserves global/per-publisher admission before a
    // local message is published. Commit binds the eventual local sequence.
    Result<Reservation> Reserve(
        const PublisherReceiptIdentity& publisher,
        std::span<const DeliveryTarget> targets,
        const DeliveryRequirement& requirement);

    // Convenience for callers that already have a committed local sequence.
    Result<DeliveryReceipt> Create(
        const PublisherReceiptIdentity& publisher, uint64_t source_sequence,
        std::span<const DeliveryTarget> targets,
        const DeliveryRequirement& requirement);

    Status Acknowledge(ReceiptId id, DeliveryTarget target,
                       DeliveryStage reached_stage,
                       Status status = Status::Ok());

    // Removes all outstanding table entries owned by one process incarnation.
    // Existing DeliveryReceipt handles are woken with kUnavailable.
    uint32_t CleanupPublisher(const PublisherReceiptIdentity& publisher) noexcept;

    uint32_t outstanding() const noexcept;
    uint32_t outstanding_for(
        const PublisherReceiptIdentity& publisher) const noexcept;

private:
    friend struct OutstandingReceiptTableTestAccess;

    enum class ReserveFailurePointForTesting : uint32_t {
        kNone = 0,
        kState,
        kTargets,
        kUpdated,
        kPublisherEntry,
        kReceiptEntry,
    };

    DeliveryReceipt CommitReservation(ReceiptId id,
                                      uint64_t source_sequence) noexcept;
    void CancelReservation(ReceiptId id) noexcept;
    void SetReserveFailurePointForTesting(
        ReserveFailurePointForTesting point) noexcept;
    void SetNextReceiptIdForTesting(uint64_t next_id);

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Explicit branch-aware stage test. DeliveryStage is intentionally not a
// linear ordering: Remote and Recorder/Storage acknowledgements are separate.
bool DeliveryStageSatisfies(DeliveryTargetKind target_kind,
                            DeliveryStage reached,
                            DeliveryStage required) noexcept;

}  // namespace mino

#endif  // MINO_RUNTIME_DELIVERY_RECEIPT_H_
