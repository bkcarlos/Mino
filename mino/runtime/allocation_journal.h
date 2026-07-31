// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_ALLOCATION_JOURNAL_H_
#define MINO_RUNTIME_ALLOCATION_JOURNAL_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>

#include "mino/abi/shm_handle.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/allocator/central_slab.h"

namespace mino {

enum class AllocationJournalState : uint8_t {
    kFree = 0,
    kInitializing = 1,
    kBuilding = 2,
    kCommitted = 3,
    kReclaiming = 4,
    kFinalizing = 5,
};

enum class ProcessLiveness : uint32_t {
    kAlive = 0,
    kDead = 1,
    kUnknown = 2,
};

enum class PublicationChannelKind : uint32_t {
    kNone = 0,
    kSpsc = 1,
    kMpsc = 2,
    kBroadcast = 3,
};

struct PublicationBinding {
    PublicationChannelKind channel_kind = PublicationChannelKind::kNone;
    uint64_t channel_id = 0;
    uint64_t sequence = 0;
    ShmHandle payload;
};

// Process-local capability for one shared allocation transaction. The
// transaction id is also the generation embedded in the record's tagged
// control word, so stale capabilities cannot CAS a recycled record.
struct AllocationTransaction {
    uint32_t journal_index = std::numeric_limits<uint32_t>::max();
    uint64_t transaction_epoch = 0;
    ProcessIdentity owner;

    bool valid() const noexcept {
        return journal_index != std::numeric_limits<uint32_t>::max() &&
               transaction_epoch != 0 && !owner.IsZero();
    }
};

class AllocationJournal {
public:
    static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
                  "AllocationJournal requires lock-free 64-bit atomics");

    using IdentityProbe = ProcessLiveness (*)(const ProcessIdentity&,
                                               void*) noexcept;
    using LegacyIdentityProbe = bool (*)(const ProcessIdentity&,
                                         void*) noexcept;

    enum class PersistencePoint : uint32_t {
        kInitializingTagged = 1,
        kBuildingPublished = 2,
        kAllocationPublished = 3,
        kHandleAppended = 4,
        kHandleReleased = 8,
        kReclaimTagged = 5,
        kReclaimProgress = 6,
        kFinalizingTagged = 7,
    };
    using PersistenceHook = void (*)(PersistencePoint, uint64_t,
                                     void*) noexcept;

    void SetPersistenceHook(PersistenceHook hook, void* context) noexcept {
        persistence_hook_ = hook;
        persistence_hook_context_ = context;
    }

    enum class CommittedOrphanAction : uint32_t {
        kDefer = 0,
        kRollback = 1,
        kFinalize = 2,
    };
    using CommittedOrphanResolver = CommittedOrphanAction (*)(
        const AllocationTransaction&, const PublicationBinding&,
        void*) noexcept;

    static constexpr uint64_t kMagic = 0x4D49'4E4F'414A'4E4CULL;
    static constexpr uint32_t kLayoutVersion = 2;
    static constexpr uint64_t kMaxTransactionEpoch = (uint64_t{1} << 56) - 1;

    struct SharedControl;
    struct SharedRecord;
    struct SharedHandle;

    static size_t RequiredSize(uint32_t transaction_capacity,
                               uint32_t handles_per_transaction) noexcept;

    static Result<AllocationJournal> Init(
        void* shm_base, size_t shm_size, uint32_t transaction_capacity,
        uint32_t handles_per_transaction,
        CentralSlabAllocator& allocator) noexcept;

    static Result<AllocationJournal> Attach(
        void* shm_base, size_t shm_size,
        CentralSlabAllocator& allocator) noexcept;

    // Journal-first transaction creation. No slab allocation occurs before the
    // returned exact tagged capability exists in shared memory.
    Result<AllocationTransaction> Begin(
        const ProcessIdentity& owner) noexcept;

    // Source-compatibility trap for pre-D2 callers. It always returns
    // kUnsupported because accepting an already-allocated root would reopen the
    // untracked Allocate -> Begin crash window. Migrate to Begin() followed by
    // AllocateRoot().
    Result<AllocationTransaction> Begin(
        const ProcessIdentity& owner, ShmHandle legacy_root) noexcept;

    Result<ShmHandle> AllocateRoot(
        const AllocationTransaction& transaction,
        const AllocationRequest& request) noexcept;
    Result<ShmHandle> AllocateChild(
        const AllocationTransaction& transaction,
        const AllocationRequest& request) noexcept;

    // Compatibility/adoption path. Only a transaction-stamped unpublished
    // child allocated through AllocateChild() is accepted.
    Status RegisterChild(const AllocationTransaction& transaction,
                         ShmHandle child) noexcept;

    // Reclaims a BUILDING child and then tombstones its manifest slot. The
    // reclaim-before-tombstone order is crash safe: recovery may see a stale
    // handle, but never loses the only durable reference to a live allocation.
    // Tombstoned slots are reused by subsequent AllocateChild calls.
    Status ReleaseChild(const AllocationTransaction& transaction,
                        ShmHandle child) noexcept;

    // Verifies that the durable manifest contains exactly the reachable graph:
    // root first, no duplicates, no missing or unreachable children.
    Status ValidateManifest(
        const AllocationTransaction& transaction,
        std::span<const ShmHandle> reachable) const noexcept;

    // Publishes every transaction allocation, children first and root last.
    Status PublishGraph(const AllocationTransaction& transaction) noexcept;

    Status Commit(const AllocationTransaction& transaction,
                  const PublicationBinding& binding = {}) noexcept;
    Status FinalizeCommit(const AllocationTransaction& transaction) noexcept;
    Status RollbackCommitted(
        const AllocationTransaction& transaction) noexcept;
    Status Abort(const AllocationTransaction& transaction) noexcept;

    uint32_t RecoverOrphans(
        IdentityProbe identity_probe = nullptr,
        void* identity_probe_context = nullptr,
        CommittedOrphanResolver committed_resolver = nullptr,
        void* committed_resolver_context = nullptr) noexcept;

    // Source compatibility for older explicit test/application probes. New
    // recovery code should use the tri-state overload so Unknown is preserved.
    uint32_t RecoverOrphans(LegacyIdentityProbe identity_probe,
                            void* identity_probe_context = nullptr) noexcept;

    uint32_t ActiveTransactionCount() const noexcept;
    Result<AllocationJournalState> State(
        const AllocationTransaction& transaction) const noexcept;
    Result<PublicationBinding> Binding(
        const AllocationTransaction& transaction) const noexcept;

    uint32_t transaction_capacity() const noexcept {
        return transaction_capacity_;
    }
    uint32_t handles_per_transaction() const noexcept {
        return handles_per_transaction_;
    }

private:
    AllocationJournal(SharedControl* control, SharedRecord* records,
                      SharedHandle* handles, uint32_t transaction_capacity,
                      uint32_t handles_per_transaction,
                      CentralSlabAllocator* allocator) noexcept
        : records_(records),
          handles_(handles),
          transaction_capacity_(transaction_capacity),
          handles_per_transaction_(handles_per_transaction),
          allocator_(allocator) {
        (void)control;
    }

    static uint64_t MakeTag(uint64_t epoch,
                            AllocationJournalState state) noexcept {
        return (epoch << 8) | static_cast<uint8_t>(state);
    }
    static uint64_t TagEpoch(uint64_t tag) noexcept { return tag >> 8; }
    static AllocationJournalState TagState(uint64_t tag) noexcept {
        return static_cast<AllocationJournalState>(tag & 0xFFu);
    }

    static ProcessLiveness DefaultIdentityProbe(
        const ProcessIdentity& owner, void*) noexcept;
    static ProcessIdentity LoadOwner(const SharedRecord& record) noexcept;
    static void StoreOwner(SharedRecord& record,
                           const ProcessIdentity& owner) noexcept;

    bool ExactTag(const AllocationTransaction& transaction,
                  AllocationJournalState state) const noexcept;
    Status AppendHandle(const AllocationTransaction& transaction,
                        ShmHandle handle, bool root) noexcept;
    Result<ShmHandle> AllocateTracked(
        const AllocationTransaction& transaction,
        const AllocationRequest& request, bool root) noexcept;
    PublicationBinding LoadBinding(const SharedRecord& record) const noexcept;
    void StoreBinding(SharedRecord& record,
                      const PublicationBinding& binding) noexcept;
    Status StartReclaim(const AllocationTransaction& transaction,
                        AllocationJournalState from) noexcept;
    Status ContinueReclaim(uint32_t journal_index, uint64_t tag) noexcept;
    Status ContinueFinalize(uint32_t journal_index, uint64_t tag) noexcept;
    SharedHandle* HandlesAt(uint32_t journal_index) const noexcept;
    static ShmHandle LoadHandle(const SharedHandle& handle) noexcept;
    static void StoreHandle(SharedHandle& destination,
                            ShmHandle handle) noexcept;
    Status ReclaimHandle(ShmHandle handle) noexcept;
    void InvokePersistenceHook(PersistencePoint point,
                               uint64_t epoch) noexcept;

    SharedRecord* records_ = nullptr;
    SharedHandle* handles_ = nullptr;
    uint32_t transaction_capacity_ = 0;
    uint32_t handles_per_transaction_ = 0;
    CentralSlabAllocator* allocator_ = nullptr;
    PersistenceHook persistence_hook_ = nullptr;
    void* persistence_hook_context_ = nullptr;
};

}  // namespace mino

#endif  // MINO_RUNTIME_ALLOCATION_JOURNAL_H_
