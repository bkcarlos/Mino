// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_SHM_SHARED_PTR_H_
#define MINO_RUNTIME_SHM_SHARED_PTR_H_

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "mino/abi/shm_handle.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/runtime/message_traits.h"
#include "mino/shm/allocator/central_slab.h"

namespace mino {

// Immutable allocator metadata required when establishing a new Pin. The
// contract is checked against CentralSlabAllocator::Inspect() before a Pin is
// made visible in shared memory.
struct ShmPinContract {
    TypeId type_id;
    uint64_t schema_short_id = 0;
    uint32_t layout_version = 0;
    uint32_t object_size = 0;
};

class ShmPinTable;

// Process-local capability for one shared Pin record. It is move-only and may
// be released explicitly. Destruction is a best-effort normal-path release;
// owner lease cleanup remains the crash-safe backstop.
class ShmPinToken {
public:
    ShmPinToken() noexcept = default;
    ShmPinToken(const ShmPinToken&) = delete;
    ShmPinToken& operator=(const ShmPinToken&) = delete;

    ShmPinToken(ShmPinToken&& other) noexcept;
    ShmPinToken& operator=(ShmPinToken&& other) noexcept;
    ~ShmPinToken();

    Status Release() noexcept;
    Result<ShmPinToken> Clone() const noexcept;

    bool active() const noexcept { return table_ != nullptr; }
    ShmHandle handle() const noexcept { return handle_; }
    const ProcessIdentity& owner() const noexcept { return owner_; }
    const void* data() const noexcept { return data_; }
    uint32_t record_index_for_testing() const noexcept { return record_index_; }
    uint64_t record_reservation_for_testing() const noexcept {
        return record_reservation_;
    }

private:
    friend class ShmPinTable;

    ShmPinToken(ShmPinTable* table, uint32_t record_index,
                uint64_t record_state, uint64_t record_reservation,
                ShmHandle handle,
                const ProcessIdentity& owner, const void* data) noexcept;

    void Disarm() noexcept;

    ShmPinTable* table_ = nullptr;
    uint32_t record_index_ = 0;
    uint64_t record_state_ = 0;
    uint64_t record_reservation_ = 0;
    ShmHandle handle_;
    ProcessIdentity owner_;
    const void* data_ = nullptr;
};

// Fixed shared-memory Pin table with a process-local allocator facade. The
// shared block contains no process-local pointers and can be attached at a
// different virtual address in every process.
class ShmPinTable {
public:
    static constexpr uint32_t kMaxPinsPerObject = 64;
    static constexpr uint32_t kMaxPinsPerProcess = 4096;
    static constexpr uint32_t kPinCapacity = 16384;

    // Version 5 adds per-bucket recoverable cleanup descriptors and a
    // recoverable global mutator-registration lock on top of generation-checked
    // token recovery. Attach deliberately rejects all older layouts.
    static constexpr uint32_t kLayoutVersion = 5;
    static constexpr uint32_t kMutatorCapacity = 256;

    enum class PinFaultPointForTesting : uint32_t {
        kMutatorBeforeRegistrationLock,
        kMutatorSlotWriting,
        kMutatorIdentityReady,
        kMutatorActivePublished,
        kOwnerCleanupOdd,
        kRecordClaimed,
        kRecordMetadataReady,
        kRecordPrepared,
        kObjectQuotaLocked,
        kObjectQuotaCommitted,
        kObjectInspected,
        kOwnerQuotaLocked,
        kOwnerQuotaCommitted,
        kRecordActive,
        kRecordReleasing,
        kOwnerQuotaReleaseLocked,
        kObjectQuotaReleaseLocked,
        kRecordStateCleared,
    };
    using PinFaultInjectorForTesting = void (*)(PinFaultPointForTesting,
                                                void*) noexcept;

    // Process-local hook. Tests may terminate the process from the callback to
    // exercise shared-memory recovery at deterministic state boundaries.
    static void SetFaultInjectorForTesting(
        PinFaultInjectorForTesting injector, void* context = nullptr) noexcept;

    // Forward-declared shared layout nodes. Their fields remain defined only in
    // the implementation file; the names are public so layout helper functions
    // can be kept outside the process-local facade.
    struct SharedControl;
    struct SharedMutator;
    struct SharedCleanup;
    struct SharedRecord;

    static size_t RequiredSize() noexcept;

    static Result<ShmPinTable> Init(void* shm_base, size_t shm_size,
                                    CentralSlabAllocator& allocator);
    static Result<ShmPinTable> Attach(void* shm_base, size_t shm_size,
                                      CentralSlabAllocator& allocator);

    // `owner` is the logical lease owner and may differ from the process
    // executing Pin(). Crash recovery separately records and probes the actual
    // mutator ProcessIdentity::Current().
    Result<ShmPinToken> Pin(
        ShmHandle handle, const ShmPinContract& contract,
        const ProcessIdentity& owner = ProcessIdentity::Current()) noexcept;

    // Counts exact records, not quota hash buckets. Transitional records are
    // included so reclamation cannot pass an in-flight Pin publication.
    uint32_t PinCount(ShmHandle handle) const noexcept;
    uint32_t OwnerPinCount(const ProcessIdentity& owner) const noexcept;

    // Releases every record bound to the exact logical process incarnation.
    // Cleanup linearizes while its owner fence is odd: every Pin ordered before
    // the odd->even publication is either removed or fails before Active, and
    // Pins that observe the odd interval are rejected. A Pin that observes the
    // newly published even epoch is ordered after Cleanup, even if its invocation
    // overlaps the final return instructions.
    // Returns the number of records removed. Retired objects whose final Pin is
    // removed are reclaimed before this method returns (best effort).
    uint32_t CleanupOwner(const ProcessIdentity& owner) noexcept;

    // SubscriberLeaseCoordinator::PinCleanup-compatible adapter. Pass the
    // ShmPinTable facade as pin_cleanup_context.
    static void CleanupOwnerCallback(const ProcessIdentity& owner,
                                     void* context) noexcept;

    // BroadcastChannel::PayloadRetireObserver-compatible adapter. Retire marks
    // the payload unavailable to new Pins and reclaims immediately only when
    // no live Pin remains.
    Status RetirePayload(ShmHandle handle) noexcept;
    static void RetirePayloadCallback(ShmHandle handle,
                                      void* context) noexcept;

private:
    friend class ShmPinToken;

    ShmPinTable(SharedControl* control, SharedMutator* mutators,
                SharedCleanup* cleanups, SharedRecord* records,
                CentralSlabAllocator* allocator) noexcept
        : control_(control),
          mutators_(mutators),
          cleanups_(cleanups),
          records_(records),
          allocator_(allocator) {}

    Result<ShmPinToken> CloneToken(const ShmPinToken& source) noexcept;
    Result<ShmPinToken> AcquireRecord(ShmHandle handle,
                                      const ProcessIdentity& owner,
                                      const ShmPinContract* contract,
                                      bool allow_retired) noexcept;
    static bool ReclaimGuardCallback(ShmHandle handle,
                                     void* context) noexcept;
    Status ReleaseRecord(uint32_t record_index, uint64_t* record_state,
                         uint64_t* record_reservation, ShmHandle handle,
                         const ProcessIdentity& owner) noexcept;
    Status MaybeReclaim(ShmHandle handle) noexcept;
    bool HasPinOrPublicationGuard(ShmHandle handle) const noexcept;
    Status RecoverRecord(uint32_t record_index, uint64_t state,
                         uint64_t reservation) noexcept;
    uint32_t FinishOwnerCleanup(const ProcessIdentity& owner,
                                uint32_t owner_bucket,
                                uint64_t cleanup_token) noexcept;
    Result<uint32_t> EnsureCurrentMutator() noexcept;
    void RecoverDeadMutatorsOnAttach() noexcept;
    void RecoverDeadCleanupsOnAttach() noexcept;

    SharedControl* control_ = nullptr;
    SharedMutator* mutators_ = nullptr;
    SharedCleanup* cleanups_ = nullptr;
    SharedRecord* records_ = nullptr;
    CentralSlabAllocator* allocator_ = nullptr;
};

// Read-only, process-local RAII facade over one Pin. Copying is deliberately
// disabled: Clone() makes quota failure explicit and also works after Retire as
// long as this instance still owns a valid Pin.
template <typename T>
class ShmSharedPtr {
public:
    ShmSharedPtr() noexcept = default;
    ShmSharedPtr(const ShmSharedPtr&) = delete;
    ShmSharedPtr& operator=(const ShmSharedPtr&) = delete;

    ShmSharedPtr(ShmSharedPtr&& other) noexcept
        : token_(std::move(other.token_)), value_(other.value_) {
        other.value_ = nullptr;
    }

    ShmSharedPtr& operator=(ShmSharedPtr&& other) noexcept {
        if (this != &other) {
            token_ = std::move(other.token_);
            value_ = other.value_;
            other.value_ = nullptr;
        }
        return *this;
    }

    ~ShmSharedPtr() = default;

    static Result<ShmSharedPtr<T>> Pin(
        ShmPinTable& table, ShmHandle handle,
        const ProcessIdentity& owner = ProcessIdentity::Current()) noexcept {
        ValidateStaticContract();
        const ShmPinContract contract{
            .type_id = StaticMessageTraits<T>::type_id,
            .schema_short_id = StaticMessageTraits<T>::schema_short_id,
            .layout_version = StaticMessageTraits<T>::layout_version,
            .object_size = sizeof(T),
        };
        Result<ShmPinToken> token = table.Pin(handle, contract, owner);
        if (!token.ok()) {
            return token.status();
        }
        const T* value = static_cast<const T*>(token->data());
        return ShmSharedPtr<T>(std::move(*token), value);
    }

    Result<ShmSharedPtr<T>> Clone() const noexcept {
        if (!active()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "cannot clone an inactive ShmSharedPtr");
        }
        Result<ShmPinToken> token = token_.Clone();
        if (!token.ok()) {
            return token.status();
        }
        const T* value = static_cast<const T*>(token->data());
        return ShmSharedPtr<T>(std::move(*token), value);
    }

    Status Release() noexcept {
        value_ = nullptr;
        return token_.Release();
    }

    const T* get() const noexcept { return value_; }
    const T* operator->() const noexcept { return value_; }
    const T& operator*() const noexcept { return *value_; }
    bool active() const noexcept { return token_.active(); }
    ShmHandle handle() const noexcept { return token_.handle(); }
    const ProcessIdentity& owner() const noexcept { return token_.owner(); }

private:
    static constexpr void ValidateStaticContract() noexcept {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        static_assert(std::is_standard_layout_v<T> &&
                          std::is_trivially_copyable_v<T>,
                      "ShmSharedPtr<T> requires a SHM-safe POD type");
    }

    ShmSharedPtr(ShmPinToken&& token, const T* value) noexcept
        : token_(std::move(token)), value_(value) {}

    ShmPinToken token_;
    const T* value_ = nullptr;
};

// Short compatibility name for callers that refer to the shared Pin registry
// by its design-document name.
using PinTable = ShmPinTable;

}  // namespace mino

#endif  // MINO_RUNTIME_SHM_SHARED_PTR_H_
