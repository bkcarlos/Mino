// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/allocation_journal.h"

#include <cerrno>
#include <csignal>
#include <new>
#include <set>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace mino {

struct alignas(64) AllocationJournal::SharedControl {
    std::atomic<uint64_t> magic{0};
    std::atomic<uint32_t> layout_version{0};
    uint32_t transaction_capacity = 0;
    uint32_t handles_per_transaction = 0;
    uint32_t reserved0 = 0;
    unsigned char reserved[40] = {};
};

static_assert(sizeof(AllocationJournal::SharedControl) == 64);

struct alignas(64) AllocationJournal::SharedRecord {
    std::atomic<uint64_t> control{0};
    std::atomic<uint32_t> handle_count{0};
    std::atomic<uint32_t> reclaim_cursor{0};

    std::atomic<uint64_t> owner_node_id{0};
    std::atomic<uint64_t> owner_process_id{0};
    std::atomic<uint64_t> owner_process_epoch{0};
    std::atomic<uint64_t> owner_start_time_ns{0};

    std::atomic<uint32_t> publication_kind{0};
    uint32_t reserved0 = 0;
    std::atomic<uint64_t> publication_channel_id{0};
    std::atomic<uint64_t> publication_sequence{0};
    std::atomic<uint64_t> publication_payload_offset{0};
    std::atomic<uint64_t> publication_payload_identity{0};
    unsigned char reserved[32] = {};
};

static_assert(sizeof(AllocationJournal::SharedRecord) == 128);
static_assert(alignof(AllocationJournal::SharedRecord) == 64);

struct alignas(8) AllocationJournal::SharedHandle {
    std::atomic<uint64_t> offset{0};
    std::atomic<uint64_t> identity{0};
};

static_assert(sizeof(AllocationJournal::SharedHandle) == 16);
static_assert(alignof(AllocationJournal::SharedHandle) == 8);

namespace {

struct SharedLayout {
    AllocationJournal::SharedControl* control;
    AllocationJournal::SharedRecord* records;
    AllocationJournal::SharedHandle* handles;
};

SharedLayout LayoutOf(void* shm_base, uint32_t transaction_capacity) noexcept {
    auto* bytes = static_cast<std::byte*>(shm_base);
    auto* control =
        reinterpret_cast<AllocationJournal::SharedControl*>(bytes);
    auto* records = reinterpret_cast<AllocationJournal::SharedRecord*>(
        bytes + sizeof(AllocationJournal::SharedControl));
    auto* handles = reinterpret_cast<AllocationJournal::SharedHandle*>(
        bytes + sizeof(AllocationJournal::SharedControl) +
        sizeof(AllocationJournal::SharedRecord) * transaction_capacity);
    return SharedLayout{control, records, handles};
}

uint64_t PackHandleIdentity(ShmHandle handle) noexcept {
    return (static_cast<uint64_t>(handle.region_id) << 32) |
           handle.generation;
}

ShmHandle UnpackHandle(uint64_t offset, uint64_t identity) noexcept {
    return ShmHandle{
        .offset = offset,
        .generation = static_cast<uint32_t>(identity),
        .region_id = static_cast<uint32_t>(identity >> 32),
    };
}

struct LegacyProbeContext {
    AllocationJournal::LegacyIdentityProbe probe = nullptr;
    void* context = nullptr;
};

ProcessLiveness InvokeLegacyProbe(const ProcessIdentity& owner,
                                  void* opaque) noexcept {
    auto* bridge = static_cast<LegacyProbeContext*>(opaque);
    return bridge->probe(owner, bridge->context) ? ProcessLiveness::kAlive
                                                  : ProcessLiveness::kDead;
}

}  // namespace

size_t AllocationJournal::RequiredSize(
    uint32_t transaction_capacity,
    uint32_t handles_per_transaction) noexcept {
    if (transaction_capacity == 0 || handles_per_transaction == 0) {
        return 0;
    }
    constexpr size_t kFixed = sizeof(SharedControl);
    constexpr size_t kRecord = sizeof(SharedRecord);
    constexpr size_t kHandle = sizeof(SharedHandle);
    const size_t transactions = transaction_capacity;
    const size_t handles_per = handles_per_transaction;
    if (transactions >
        (std::numeric_limits<size_t>::max() - kFixed) / kRecord) {
        return 0;
    }
    const size_t records_size = transactions * kRecord;
    const size_t remaining =
        std::numeric_limits<size_t>::max() - kFixed - records_size;
    if (handles_per > std::numeric_limits<size_t>::max() / transactions) {
        return 0;
    }
    const size_t handle_count = transactions * handles_per;
    if (handle_count > remaining / kHandle) {
        return 0;
    }
    return kFixed + records_size + handle_count * kHandle;
}

Result<AllocationJournal> AllocationJournal::Init(
    void* shm_base, size_t shm_size, uint32_t transaction_capacity,
    uint32_t handles_per_transaction,
    CentralSlabAllocator& allocator) noexcept {
    const size_t required =
        RequiredSize(transaction_capacity, handles_per_transaction);
    if (shm_base == nullptr ||
        reinterpret_cast<uintptr_t>(shm_base) % alignof(SharedControl) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation journal base must be cache-line aligned");
    }
    if (required == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation journal capacities are invalid");
    }
    if (shm_size < required) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "shared memory is too small for allocation journal");
    }

    const SharedLayout layout = LayoutOf(shm_base, transaction_capacity);
    new (layout.control) SharedControl{};
    layout.control->layout_version.store(kLayoutVersion,
                                         std::memory_order_relaxed);
    layout.control->transaction_capacity = transaction_capacity;
    layout.control->handles_per_transaction = handles_per_transaction;
    for (uint32_t i = 0; i < transaction_capacity; ++i) {
        new (&layout.records[i]) SharedRecord{};
        layout.records[i].control.store(
            MakeTag(0, AllocationJournalState::kFree),
            std::memory_order_relaxed);
    }
    const size_t handle_count =
        static_cast<size_t>(transaction_capacity) * handles_per_transaction;
    for (size_t i = 0; i < handle_count; ++i) {
        new (&layout.handles[i]) SharedHandle{};
    }
    layout.control->magic.store(kMagic, std::memory_order_release);
    return AllocationJournal(layout.control, layout.records, layout.handles,
                             transaction_capacity, handles_per_transaction,
                             &allocator);
}

Result<AllocationJournal> AllocationJournal::Attach(
    void* shm_base, size_t shm_size,
    CentralSlabAllocator& allocator) noexcept {
    if (shm_base == nullptr ||
        reinterpret_cast<uintptr_t>(shm_base) % alignof(SharedControl) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation journal base must be cache-line aligned");
    }
    if (shm_size < sizeof(SharedControl)) {
        return Status::Error(StatusCode::kCorruption,
                             "allocation journal is smaller than its control block");
    }
    auto* control = static_cast<SharedControl*>(shm_base);
    if (control->magic.load(std::memory_order_acquire) != kMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "allocation journal magic mismatch");
    }
    if (control->layout_version.load(std::memory_order_acquire) !=
        kLayoutVersion) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "allocation journal layout version mismatch");
    }
    const uint32_t transaction_capacity = control->transaction_capacity;
    const uint32_t handles_per_transaction =
        control->handles_per_transaction;
    const size_t required =
        RequiredSize(transaction_capacity, handles_per_transaction);
    if (required == 0 || shm_size < required) {
        return Status::Error(StatusCode::kCorruption,
                             "allocation journal layout is invalid");
    }
    const SharedLayout layout = LayoutOf(shm_base, transaction_capacity);
    return AllocationJournal(layout.control, layout.records, layout.handles,
                             transaction_capacity, handles_per_transaction,
                             &allocator);
}

Result<AllocationTransaction> AllocationJournal::Begin(
    const ProcessIdentity& owner) noexcept {
    if (owner.IsZero()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction requires an owner");
    }
    MINO_ASSIGN_OR_RETURN(uint64_t epoch,
                          allocator_->NextAllocationTransactionId());
    if (epoch > kMaxTransactionEpoch) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "allocation transaction tag space exhausted");
    }

    for (uint32_t i = 0; i < transaction_capacity_; ++i) {
        SharedRecord& record = records_[i];
        uint64_t observed = record.control.load(std::memory_order_acquire);
        if (TagState(observed) != AllocationJournalState::kFree) {
            continue;
        }
        const uint64_t initializing =
            MakeTag(epoch, AllocationJournalState::kInitializing);
        if (!record.control.compare_exchange_strong(
                observed, initializing, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            continue;
        }
        InvokePersistenceHook(PersistencePoint::kInitializingTagged, epoch);

        record.handle_count.store(0, std::memory_order_relaxed);
        record.reclaim_cursor.store(0, std::memory_order_relaxed);
        StoreOwner(record, owner);
        StoreBinding(record, {});
        const uint64_t building =
            MakeTag(epoch, AllocationJournalState::kBuilding);
        uint64_t expected = initializing;
        if (!record.control.compare_exchange_strong(
                expected, building, std::memory_order_release,
                std::memory_order_acquire)) {
            return Status::Error(StatusCode::kUnavailable,
                                 "transaction initialization was recovered");
        }
        InvokePersistenceHook(PersistencePoint::kBuildingPublished, epoch);
        return AllocationTransaction{
            .journal_index = i,
            .transaction_epoch = epoch,
            .owner = owner,
        };
    }
    return Status::Error(StatusCode::kResourceExhausted,
                         "allocation journal transaction capacity exhausted");
}

Result<AllocationTransaction> AllocationJournal::Begin(
    const ProcessIdentity& owner, ShmHandle legacy_root) noexcept {
    (void)owner;
    (void)legacy_root;
    return Status::Error(
        StatusCode::kUnsupported,
        "already-allocated roots are not crash-safe; use Begin then AllocateRoot");
}

Result<ShmHandle> AllocationJournal::AllocateRoot(
    const AllocationTransaction& transaction,
    const AllocationRequest& request) noexcept {
    return AllocateTracked(transaction, request, true);
}

Result<ShmHandle> AllocationJournal::AllocateChild(
    const AllocationTransaction& transaction,
    const AllocationRequest& request) noexcept {
    return AllocateTracked(transaction, request, false);
}

Result<ShmHandle> AllocationJournal::AllocateTracked(
    const AllocationTransaction& transaction, const AllocationRequest& request,
    bool root) noexcept {
    if (!ExactTag(transaction, AllocationJournalState::kBuilding)) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction is stale or inactive");
    }
    AllocationRequest stamped = request;
    stamped.owner_epoch = transaction.owner.process_epoch;
    stamped.allocation_transaction_id = transaction.transaction_epoch;
    stamped.allocation_flags = root ? kAllocationFlagTransactionRoot
                                    : kAllocationFlagTransactionChild;
    MINO_ASSIGN_OR_RETURN(ShmHandle handle, allocator_->Allocate(stamped));
    InvokePersistenceHook(PersistencePoint::kAllocationPublished,
                          transaction.transaction_epoch);
    const Status appended = AppendHandle(transaction, handle, root);
    if (!appended.ok()) {
        // The allocator stamp makes this recoverable if local cleanup is
        // interrupted. Reclaim only this failed allocation on the normal path;
        // reclaiming the whole transaction would invalidate an already-built root.
        (void)ReclaimHandle(handle);
        return appended;
    }
    return handle;
}

Status AllocationJournal::AppendHandle(
    const AllocationTransaction& transaction, ShmHandle handle,
    bool root) noexcept {
    if (transaction.journal_index >= transaction_capacity_ || handle.IsNull() ||
        !ExactTag(transaction, AllocationJournalState::kBuilding)) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction is stale or inactive");
    }
    SharedRecord& record = records_[transaction.journal_index];
    const uint32_t count =
        record.handle_count.load(std::memory_order_acquire);
    if ((root && count != 0) || (!root && count == 0)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "transaction root/child order is invalid");
    }
    SharedHandle* handles = HandlesAt(transaction.journal_index);
    uint32_t destination = count;
    for (uint32_t i = 0; i < count; ++i) {
        const ShmHandle existing = LoadHandle(handles[i]);
        if (existing == handle) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "allocation handle is already registered");
        }
        if (!root && i != 0 && existing.IsNull() && destination == count) {
            destination = i;
        }
    }
    if (destination == count && count >= handles_per_transaction_) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "allocation transaction handle capacity exhausted");
    }
    StoreHandle(handles[destination], handle);
    if (destination == count) {
        record.handle_count.store(count + 1, std::memory_order_release);
    }
    InvokePersistenceHook(PersistencePoint::kHandleAppended,
                          transaction.transaction_epoch);
    if (!ExactTag(transaction, AllocationJournalState::kBuilding)) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction ended during append");
    }
    return Status::Ok();
}

Status AllocationJournal::RegisterChild(
    const AllocationTransaction& transaction, ShmHandle child) noexcept {
    if (child.IsNull()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "child handle must not be null");
    }
    Result<SlabView> slab = allocator_->Inspect(child);
    if (!slab.ok()) {
        return slab.status();
    }
    if (slab->owner_epoch != transaction.owner.process_epoch ||
        slab->allocation_transaction_id != transaction.transaction_epoch ||
        (slab->allocation_flags & kAllocationFlagTransactionChild) == 0 ||
        (slab->state != ObjectState::kAllocated &&
         slab->state != ObjectState::kBuilding)) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "child is not an unpublished allocation owned by this transaction");
    }
    return AppendHandle(transaction, child, false);
}

Status AllocationJournal::ReleaseChild(
    const AllocationTransaction& transaction, ShmHandle child) noexcept {
    if (transaction.journal_index >= transaction_capacity_ || child.IsNull() ||
        !ExactTag(transaction, AllocationJournalState::kBuilding)) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction is stale or inactive");
    }
    SharedRecord& record = records_[transaction.journal_index];
    const uint32_t count = record.handle_count.load(std::memory_order_acquire);
    SharedHandle* handles = HandlesAt(transaction.journal_index);
    uint32_t found = count;
    for (uint32_t i = 1; i < count; ++i) {
        if (LoadHandle(handles[i]) == child) {
            found = i;
            break;
        }
    }
    if (found == count) {
        return Status::Error(StatusCode::kNotFound,
                             "child is not present in transaction manifest");
    }

    // Never clear the durable manifest reference before the allocation is gone.
    MINO_RETURN_IF_ERROR(ReclaimHandle(child));
    if (!ExactTag(transaction, AllocationJournalState::kBuilding)) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction ended during child release");
    }
    StoreHandle(handles[found], {});
    InvokePersistenceHook(PersistencePoint::kHandleReleased,
                          transaction.transaction_epoch);
    return Status::Ok();
}

Status AllocationJournal::ValidateManifest(
    const AllocationTransaction& transaction,
    std::span<const ShmHandle> reachable) const noexcept {
    try {
        if (!ExactTag(transaction, AllocationJournalState::kBuilding)) {
            return Status::Error(StatusCode::kNotFound,
                                 "allocation transaction is stale or inactive");
        }
        const SharedRecord& record = records_[transaction.journal_index];
        const uint32_t count =
            record.handle_count.load(std::memory_order_acquire);
        if (count == 0 || count > handles_per_transaction_ ||
            reachable.empty()) {
            return Status::Error(StatusCode::kCorruption,
                                 "allocation manifest has no valid root");
        }
        const SharedHandle* handles = HandlesAt(transaction.journal_index);
        if (LoadHandle(handles[0]) != reachable.front()) {
            return Status::Error(StatusCode::kCorruption,
                                 "allocation manifest root is not graph root");
        }
        using HandleKey = std::tuple<uint32_t, uint64_t, uint32_t>;
        const auto key = [](ShmHandle handle) {
            return HandleKey(handle.region_id, handle.offset, handle.generation);
        };
        std::set<HandleKey> manifest;
        for (uint32_t i = 0; i < count; ++i) {
            const ShmHandle handle = LoadHandle(handles[i]);
            if (handle.IsNull()) continue;
            if (!manifest.emplace(key(handle)).second) {
                return Status::Error(StatusCode::kCorruption,
                                     "allocation manifest contains a duplicate");
            }
        }
        std::set<HandleKey> graph;
        for (ShmHandle handle : reachable) {
            if (handle.IsNull() || !graph.emplace(key(handle)).second) {
                return Status::Error(StatusCode::kCorruption,
                                     "reachable graph contains an invalid handle");
            }
        }
        if (manifest != graph) {
            return Status::Error(
                StatusCode::kCorruption,
                "allocation manifest does not equal the reachable graph");
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status AllocationJournal::PublishGraph(
    const AllocationTransaction& transaction) noexcept {
    if (!ExactTag(transaction, AllocationJournalState::kBuilding)) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction is stale or inactive");
    }
    SharedRecord& record = records_[transaction.journal_index];
    if (record.handle_count.load(std::memory_order_acquire) == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction has no root");
    }
    try {
        const uint32_t count =
            record.handle_count.load(std::memory_order_acquire);
        std::vector<ShmHandle> manifest;
        manifest.reserve(count);
        SharedHandle* handles = HandlesAt(transaction.journal_index);
        for (uint32_t i = 0; i < count; ++i) {
            const ShmHandle handle = LoadHandle(handles[i]);
            if (!handle.IsNull()) manifest.push_back(handle);
        }
        if (manifest.empty()) {
            return Status::Error(StatusCode::kCorruption,
                                 "allocation transaction root is missing");
        }
        return allocator_->PublishTransaction(
            transaction.owner.process_epoch, transaction.transaction_epoch,
            manifest, manifest.front());
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

bool AllocationJournal::ExactTag(
    const AllocationTransaction& transaction,
    AllocationJournalState state) const noexcept {
    return transaction.valid() &&
           transaction.journal_index < transaction_capacity_ &&
           records_[transaction.journal_index].control.load(
               std::memory_order_acquire) ==
               MakeTag(transaction.transaction_epoch, state);
}

Status AllocationJournal::Commit(
    const AllocationTransaction& transaction,
    const PublicationBinding& binding) noexcept {
    if (transaction.journal_index >= transaction_capacity_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction index is invalid");
    }
    SharedRecord& record = records_[transaction.journal_index];
    const uint64_t building = MakeTag(transaction.transaction_epoch,
                                      AllocationJournalState::kBuilding);
    if (record.control.load(std::memory_order_acquire) != building) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction is stale or inactive");
    }
    if (record.handle_count.load(std::memory_order_acquire) == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction has no root");
    }
    PublicationBinding stored = binding;
    if (stored.payload.IsNull()) {
        stored.payload = LoadHandle(HandlesAt(transaction.journal_index)[0]);
    }
    StoreBinding(record, stored);
    uint64_t expected = building;
    if (!record.control.compare_exchange_strong(
            expected,
            MakeTag(transaction.transaction_epoch,
                    AllocationJournalState::kCommitted),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction lost commit ownership");
    }
    return Status::Ok();
}

Status AllocationJournal::FinalizeCommit(
    const AllocationTransaction& transaction) noexcept {
    if (transaction.journal_index >= transaction_capacity_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction index is invalid");
    }
    SharedRecord& record = records_[transaction.journal_index];
    const uint64_t committed = MakeTag(transaction.transaction_epoch,
                                       AllocationJournalState::kCommitted);
    uint64_t expected = committed;
    const uint64_t finalizing = MakeTag(transaction.transaction_epoch,
                                        AllocationJournalState::kFinalizing);
    if (!record.control.compare_exchange_strong(
            expected, finalizing, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        if (expected != finalizing) {
            return Status::Error(StatusCode::kNotFound,
                                 "committed transaction is stale");
        }
    }
    return ContinueFinalize(transaction.journal_index, finalizing);
}

Status AllocationJournal::StartReclaim(
    const AllocationTransaction& transaction,
    AllocationJournalState from) noexcept {
    SharedRecord& record = records_[transaction.journal_index];
    const uint64_t source = MakeTag(transaction.transaction_epoch, from);
    const uint64_t reclaiming = MakeTag(transaction.transaction_epoch,
                                        AllocationJournalState::kReclaiming);
    uint64_t expected = source;
    if (!record.control.compare_exchange_strong(
            expected, reclaiming, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        if (expected != reclaiming) {
            return Status::Error(StatusCode::kNotFound,
                                 "transaction lost reclaim ownership");
        }
    } else {
        record.reclaim_cursor.store(
            record.handle_count.load(std::memory_order_acquire),
            std::memory_order_release);
        InvokePersistenceHook(PersistencePoint::kReclaimTagged,
                              transaction.transaction_epoch);
    }
    return ContinueReclaim(transaction.journal_index, reclaiming);
}

Status AllocationJournal::RollbackCommitted(
    const AllocationTransaction& transaction) noexcept {
    if (transaction.journal_index >= transaction_capacity_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction index is invalid");
    }
    return StartReclaim(transaction, AllocationJournalState::kCommitted);
}

Status AllocationJournal::Abort(
    const AllocationTransaction& transaction) noexcept {
    if (transaction.journal_index >= transaction_capacity_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction index is invalid");
    }
    return StartReclaim(transaction, AllocationJournalState::kBuilding);
}

Status AllocationJournal::ContinueReclaim(uint32_t journal_index,
                                          uint64_t tag) noexcept {
    SharedRecord& record = records_[journal_index];
    if (record.control.load(std::memory_order_acquire) != tag ||
        TagState(tag) != AllocationJournalState::kReclaiming) {
        return Status::Error(StatusCode::kNotFound,
                             "reclaim transaction tag changed");
    }
    uint32_t cursor = record.reclaim_cursor.load(std::memory_order_acquire);
    const uint32_t count = record.handle_count.load(std::memory_order_acquire);
    if (cursor > count || count > handles_per_transaction_) {
        cursor = count;
        record.reclaim_cursor.store(cursor, std::memory_order_release);
    }
    SharedHandle* handles = HandlesAt(journal_index);
    while (cursor > 0) {
        const ShmHandle handle = LoadHandle(handles[cursor - 1]);
        const Status status = handle.IsNull() ? Status::Ok()
                                              : ReclaimHandle(handle);
        if (!status.ok()) {
            return status;
        }
        --cursor;
        record.reclaim_cursor.store(cursor, std::memory_order_release);
        InvokePersistenceHook(PersistencePoint::kReclaimProgress,
                              TagEpoch(tag));
    }

    const ProcessIdentity owner = LoadOwner(record);
    const Status scanned = allocator_->ReclaimTransactionAppendGap(
        owner.process_epoch, TagEpoch(tag));
    if (!scanned.ok()) {
        return scanned;
    }
    const uint64_t finalizing =
        MakeTag(TagEpoch(tag), AllocationJournalState::kFinalizing);
    uint64_t expected = tag;
    if (!record.control.compare_exchange_strong(
            expected, finalizing, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        if (expected != finalizing) {
            return Status::Error(StatusCode::kUnavailable,
                                 "reclaim completion tag changed");
        }
    }
    return ContinueFinalize(journal_index, finalizing);
}

Status AllocationJournal::ContinueFinalize(uint32_t journal_index,
                                           uint64_t tag) noexcept {
    SharedRecord& record = records_[journal_index];
    if (TagState(tag) != AllocationJournalState::kFinalizing ||
        record.control.load(std::memory_order_acquire) != tag) {
        return Status::Error(StatusCode::kNotFound,
                             "finalize transaction tag changed");
    }
    record.handle_count.store(0, std::memory_order_relaxed);
    record.reclaim_cursor.store(0, std::memory_order_relaxed);
    StoreBinding(record, {});
    StoreOwner(record, {});
    uint64_t expected = tag;
    InvokePersistenceHook(PersistencePoint::kFinalizingTagged,
                          TagEpoch(tag));
    if (!record.control.compare_exchange_strong(
            expected, MakeTag(TagEpoch(tag), AllocationJournalState::kFree),
            std::memory_order_release, std::memory_order_acquire)) {
        return Status::Error(StatusCode::kUnavailable,
                             "finalize transaction tag changed");
    }
    return Status::Ok();
}

uint32_t AllocationJournal::RecoverOrphans(
    IdentityProbe identity_probe, void* identity_probe_context,
    CommittedOrphanResolver committed_resolver,
    void* committed_resolver_context) noexcept {
    if (identity_probe == nullptr) {
        identity_probe = &DefaultIdentityProbe;
    }
    uint32_t recovered = 0;
    for (uint32_t i = 0; i < transaction_capacity_; ++i) {
        SharedRecord& record = records_[i];
        uint64_t tag = record.control.load(std::memory_order_acquire);
        const uint64_t epoch = TagEpoch(tag);
        const AllocationJournalState state = TagState(tag);
        if (state == AllocationJournalState::kFree) {
            continue;
        }
        if (state == AllocationJournalState::kFinalizing) {
            if (ContinueFinalize(i, tag).ok()) {
                ++recovered;
            }
            continue;
        }
        if (state == AllocationJournalState::kReclaiming) {
            if (ContinueReclaim(i, tag).ok()) {
                ++recovered;
            }
            continue;
        }

        const ProcessIdentity owner = LoadOwner(record);
        ProcessLiveness liveness = ProcessLiveness::kDead;
        if (!owner.IsZero()) {
            liveness = identity_probe(owner, identity_probe_context);
        }
        if (liveness != ProcessLiveness::kDead) {
            continue;
        }

        if (state == AllocationJournalState::kInitializing) {
            uint64_t expected = tag;
            const uint64_t finalizing =
                MakeTag(epoch, AllocationJournalState::kFinalizing);
            if (record.control.compare_exchange_strong(
                    expected, finalizing, std::memory_order_acq_rel,
                    std::memory_order_acquire) &&
                ContinueFinalize(i, finalizing).ok()) {
                ++recovered;
            }
            continue;
        }

        AllocationTransaction transaction{
            .journal_index = i,
            .transaction_epoch = epoch,
            .owner = owner,
        };
        if (state == AllocationJournalState::kBuilding) {
            if (StartReclaim(transaction, state).ok()) {
                ++recovered;
            }
            continue;
        }
        if (state != AllocationJournalState::kCommitted ||
            committed_resolver == nullptr) {
            continue;
        }
        const CommittedOrphanAction action = committed_resolver(
            transaction, LoadBinding(record), committed_resolver_context);
        if (action == CommittedOrphanAction::kRollback) {
            if (StartReclaim(transaction, state).ok()) {
                ++recovered;
            }
        } else if (action == CommittedOrphanAction::kFinalize) {
            uint64_t expected = tag;
            const uint64_t finalizing =
                MakeTag(epoch, AllocationJournalState::kFinalizing);
            if (record.control.compare_exchange_strong(
                    expected, finalizing, std::memory_order_acq_rel,
                    std::memory_order_acquire) &&
                ContinueFinalize(i, finalizing).ok()) {
                ++recovered;
            }
        }
    }
    return recovered;
}

uint32_t AllocationJournal::RecoverOrphans(
    LegacyIdentityProbe identity_probe,
    void* identity_probe_context) noexcept {
    if (identity_probe == nullptr) {
        return RecoverOrphans();
    }
    LegacyProbeContext bridge{identity_probe, identity_probe_context};
    return RecoverOrphans(&InvokeLegacyProbe, &bridge, nullptr, nullptr);
}

uint32_t AllocationJournal::ActiveTransactionCount() const noexcept {
    uint32_t active = 0;
    for (uint32_t i = 0; i < transaction_capacity_; ++i) {
        if (TagState(records_[i].control.load(std::memory_order_acquire)) !=
            AllocationJournalState::kFree) {
            ++active;
        }
    }
    return active;
}

Result<AllocationJournalState> AllocationJournal::State(
    const AllocationTransaction& transaction) const noexcept {
    if (!transaction.valid() ||
        transaction.journal_index >= transaction_capacity_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction is invalid");
    }
    const uint64_t tag = records_[transaction.journal_index].control.load(
        std::memory_order_acquire);
    if (TagEpoch(tag) != transaction.transaction_epoch) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction was recycled");
    }
    return TagState(tag);
}

Result<PublicationBinding> AllocationJournal::Binding(
    const AllocationTransaction& transaction) const noexcept {
    if (!transaction.valid() ||
        transaction.journal_index >= transaction_capacity_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocation transaction is invalid");
    }
    const SharedRecord& record = records_[transaction.journal_index];
    const uint64_t tag = record.control.load(std::memory_order_acquire);
    if (TagEpoch(tag) != transaction.transaction_epoch ||
        (TagState(tag) != AllocationJournalState::kCommitted &&
         TagState(tag) != AllocationJournalState::kFinalizing)) {
        return Status::Error(StatusCode::kNotFound,
                             "allocation transaction has no publication binding");
    }
    return LoadBinding(record);
}

ProcessLiveness AllocationJournal::DefaultIdentityProbe(
    const ProcessIdentity& owner, void*) noexcept {
    if (owner.IsZero() || owner.process_id == 0) {
        return ProcessLiveness::kDead;
    }
    if (IsProcessIdentityAlive(owner)) {
        return ProcessLiveness::kAlive;
    }
    errno = 0;
    const int rc = ::kill(static_cast<pid_t>(owner.process_id), 0);
    if (rc == 0 || errno == EPERM) {
        // The PID exists but this protocol cannot prove whether it is a reused
        // incarnation or an inaccessible /proc entry. Never convert Unknown to
        // Dead: safety takes precedence over eager reclamation.
        return ProcessLiveness::kUnknown;
    }
    return errno == ESRCH ? ProcessLiveness::kDead
                          : ProcessLiveness::kUnknown;
}

ProcessIdentity AllocationJournal::LoadOwner(
    const SharedRecord& record) noexcept {
    return ProcessIdentity{
        .node_id = record.owner_node_id.load(std::memory_order_acquire),
        .process_id = record.owner_process_id.load(std::memory_order_acquire),
        .process_epoch =
            record.owner_process_epoch.load(std::memory_order_acquire),
        .start_time_ns =
            record.owner_start_time_ns.load(std::memory_order_acquire),
    };
}

void AllocationJournal::StoreOwner(SharedRecord& record,
                                   const ProcessIdentity& owner) noexcept {
    record.owner_node_id.store(owner.node_id, std::memory_order_relaxed);
    record.owner_process_id.store(owner.process_id, std::memory_order_relaxed);
    record.owner_process_epoch.store(owner.process_epoch,
                                     std::memory_order_relaxed);
    record.owner_start_time_ns.store(owner.start_time_ns,
                                     std::memory_order_release);
}

PublicationBinding AllocationJournal::LoadBinding(
    const SharedRecord& record) const noexcept {
    return PublicationBinding{
        .channel_kind = static_cast<PublicationChannelKind>(
            record.publication_kind.load(std::memory_order_acquire)),
        .channel_id = record.publication_channel_id.load(
            std::memory_order_acquire),
        .sequence = record.publication_sequence.load(std::memory_order_acquire),
        .payload = UnpackHandle(
            record.publication_payload_offset.load(std::memory_order_acquire),
            record.publication_payload_identity.load(std::memory_order_acquire)),
    };
}

void AllocationJournal::StoreBinding(
    SharedRecord& record, const PublicationBinding& binding) noexcept {
    record.publication_channel_id.store(binding.channel_id,
                                        std::memory_order_relaxed);
    record.publication_sequence.store(binding.sequence,
                                      std::memory_order_relaxed);
    record.publication_payload_offset.store(binding.payload.offset,
                                            std::memory_order_relaxed);
    record.publication_payload_identity.store(
        PackHandleIdentity(binding.payload), std::memory_order_relaxed);
    record.publication_kind.store(static_cast<uint32_t>(binding.channel_kind),
                                  std::memory_order_release);
}

void AllocationJournal::InvokePersistenceHook(PersistencePoint point,
                                              uint64_t epoch) noexcept {
    if (persistence_hook_ != nullptr) {
        persistence_hook_(point, epoch, persistence_hook_context_);
    }
}

AllocationJournal::SharedHandle* AllocationJournal::HandlesAt(
    uint32_t journal_index) const noexcept {
    return handles_ + static_cast<size_t>(journal_index) *
                          handles_per_transaction_;
}

ShmHandle AllocationJournal::LoadHandle(
    const SharedHandle& handle) noexcept {
    const uint64_t offset = handle.offset.load(std::memory_order_acquire);
    return UnpackHandle(offset,
                        handle.identity.load(std::memory_order_relaxed));
}

void AllocationJournal::StoreHandle(SharedHandle& destination,
                                    ShmHandle handle) noexcept {
    destination.identity.store(PackHandleIdentity(handle),
                               std::memory_order_relaxed);
    destination.offset.store(handle.offset, std::memory_order_release);
}

Status AllocationJournal::ReclaimHandle(ShmHandle handle) noexcept {
    Result<SlabView> slab = allocator_->Inspect(handle);
    if (!slab.ok()) {
        return slab.status().code() == StatusCode::kNotFound
                   ? Status::Ok()
                   : slab.status();
    }
    switch (slab->state) {
        case ObjectState::kAllocated:
        case ObjectState::kBuilding:
            return allocator_->Abort(handle);
        case ObjectState::kPublished:
            MINO_RETURN_IF_ERROR(allocator_->Retire(handle));
            return allocator_->Reclaim(handle);
        case ObjectState::kRetired:
        case ObjectState::kAborting:
        case ObjectState::kReclaiming:
        case ObjectState::kAllocating:
            return allocator_->Reclaim(handle);
        case ObjectState::kFree:
            return Status::Ok();
    }
    return Status::Error(StatusCode::kCorruption,
                         "allocation journal found unknown slab state");
}

}  // namespace mino
