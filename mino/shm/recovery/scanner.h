// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SHM_RECOVERY_SCANNER_H_
#define MINO_SHM_RECOVERY_SCANNER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino::shm::recovery {

// Recovery uses the allocator's authoritative lifecycle ABI. No recovery-local
// ObjectState or slab-header prefix is permitted.
using ObjectState = ::mino::ObjectState;

constexpr bool IsProtocolReclaimableState(uint32_t value) {
    return value == static_cast<uint32_t>(ObjectState::kFree) ||
           value == static_cast<uint32_t>(ObjectState::kAllocating) ||
           value == static_cast<uint32_t>(ObjectState::kReclaiming);
}

constexpr bool IsValidPublishedState(uint32_t value) {
    switch (static_cast<ObjectState>(value)) {
        case ObjectState::kAllocated:
        case ObjectState::kBuilding:
        case ObjectState::kPublished:
        case ObjectState::kRetired:
        case ObjectState::kAborting:
            return true;
        case ObjectState::kFree:
        case ObjectState::kReclaiming:
        case ObjectState::kAllocating:
            return false;
    }
    return false;
}

std::string_view ObjectStateName(uint32_t value);

// Legacy image-scanner owner block. Real SharedMemoryRegion recovery does not
// use this block: it uses mino::RecoveryOwner over SuperBlock and passes a
// RecoveryOwnership callback below. The type remains for source compatibility
// with offline image tooling and its cross-process tests.
struct alignas(64) RecoveryOwnerState {
    static constexpr uint32_t kMagic = 0x524F5731;

    uint32_t magic;
    uint32_t reserved0;
    std::atomic<uint64_t> owner_pid;
    std::atomic<uint64_t> epoch;
    std::atomic<uint64_t> lease_deadline_ns;
    uint64_t reserved1[4];
    std::atomic<uint64_t> heartbeat_ns;
    uint64_t reserved2[7];
};
static_assert(sizeof(RecoveryOwnerState) == 128);
static_assert(alignof(RecoveryOwnerState) == 64);
static_assert(std::is_standard_layout_v<RecoveryOwnerState>);

class RecoveryOwner {
public:
    static constexpr uint64_t kLeaseDurationNs = 2'000'000'000;

    static uint64_t NowNs();
    RecoveryOwner(RecoveryOwnerState* state, uint64_t pid) noexcept;

    static void Initialize(RecoveryOwnerState* state) noexcept;
    Status TryAcquire();
    Status RenewLease();
    void Heartbeat() noexcept;
    void Release() noexcept;
    bool IsOwner() const noexcept;
    bool IsIdle() const noexcept { return CurrentOwner() == 0; }
    uint64_t CurrentOwner() const noexcept;
    uint64_t Epoch() const noexcept;
    uint64_t LeaseDeadlineNs() const noexcept;
    Status WaitForIdle(uint64_t timeout_ns) const;

private:
    friend class RecoveryScanner;
    RecoveryOwner(RecoveryOwnerState* state, uint64_t pid,
                  std::atomic<uint64_t>* external_token) noexcept;
    uint64_t LeaseToken() const noexcept;
    void StoreLeaseToken(uint64_t token) noexcept;

    RecoveryOwnerState* state_ = nullptr;
    uint64_t pid_ = 0;
    uint64_t lease_token_ = 0;
    std::atomic<uint64_t>* external_token_ = nullptr;
};

struct RecoveryScannerOptions {
    bool repair = true;

    // A retired slot may be reclaimed only while the caller's recovery owner
    // excludes normal Region service. The real allocator's reclaim protocol is
    // used; recovery does not invent borrow/pin fields in SlabHeader.
    bool reclaim_retired = true;
};

struct RecoveryReport {
    uint64_t slots_scanned = 0;
    uint64_t orphan_slab_count = 0;
    uint64_t reclaimed_slab_count = 0;
    uint64_t stale_ack_count = 0;
    uint64_t bitmap_inconsistency_count = 0;
    uint64_t corrupted_slab_count = 0;
    std::string details;

    void AddDetail(std::string line) {
        details += line;
        details += '\n';
    }
};

// Minimal ownership boundary between Region and scanner. Region supplies
// callbacks backed by its authoritative SuperBlock RecoveryOwner. This avoids a
// dependency from recovery back into Region and prevents a second owner from
// participating in the same dirty-attach chain.
struct RecoveryOwnership {
    void* context = nullptr;
    bool (*is_owner)(const void*) noexcept = nullptr;
    void (*heartbeat)(void*) noexcept = nullptr;

    bool IsOwner() const noexcept {
        return is_owner != nullptr && is_owner(context);
    }
    void Heartbeat() const noexcept {
        if (heartbeat != nullptr) {
            heartbeat(context);
        }
    }
};

class RecoveryScanner {
public:
    // Source-compatibility names for offline tooling. They are not used to
    // interpret CentralSlabAllocator metadata. SlabHeaderPrefix and BitmapWord
    // are aliases of the real allocator ABI rather than parallel layouts.
    using BitmapWord = std::atomic<uint64_t>;
    using SlabHeaderPrefix = ::mino::SlabHeader;
    static constexpr uint32_t kSlabMagic = ::mino::kSlabHeaderMagic;

    struct ClassDescriptor {
        uint32_t class_id;
        uint32_t slot_count;
        uint64_t bitmap_offset;
        uint64_t slots_offset;
        uint32_t slot_stride;
        uint32_t reserved;
    };

    struct Layout {
        uint64_t recovery_state_offset = 0;
        uint64_t class_table_offset = 0;
        uint32_t class_count = 0;
        uint32_t reserved = 0;
    };

    struct AckScanInput {
        uint64_t live_subscriber_mask = 0;
        uint64_t* bitmaps = nullptr;
        uint32_t bitmap_count = 0;
    };

    // Legacy offline-image entry point. The sidecar descriptors provide only
    // offsets/counts; slots and bitmap words are interpreted with the real
    // allocator SlabHeader and std::atomic<uint64_t> ABI aliases above.
    static Result<RecoveryScanner> Create(std::byte* base, uint64_t size,
                                          Layout layout,
                                          RecoveryScannerOptions options = {});

    // Real allocator metadata entry points used by Region and focused tests.
    static Result<RecoveryScanner> Create(
        void* allocator_base, uint64_t available_size,
        RecoveryOwnership ownership,
        RecoveryScannerOptions options = {});
    static Result<RecoveryScanner> Create(
        CentralSlabAllocator allocator, RecoveryOwnership ownership,
        RecoveryScannerOptions options = {});

    Result<RecoveryReport> Scan();
    Status ReclaimOrphanSlabs();
    // Requires recovery ownership. Each aligned bitmap is updated with an
    // atomic fetch_and so concurrent observers never see a torn ACK word.
    Status CleanupStaleAcks(const AckScanInput& input,
                            uint64_t* cleared = nullptr);

    // Pure read-only verification. Returns kCorruption for any bitmap/header
    // inconsistency, orphan, or corrupted slab and never applies repairs.
    Status VerifyBitmapConsistency();

    // Legacy owner accessor for offline image recovery only. Region recovery
    // never calls this method.
    RecoveryOwner Owner() noexcept;
    RecoveryOwner Owner() const noexcept;

    const Layout& layout() const noexcept { return legacy_layout_; }
    const RecoveryScannerOptions& options() const noexcept { return options_; }

private:
    RecoveryScanner(CentralSlabAllocator allocator,
                    RecoveryOwnership ownership,
                    RecoveryScannerOptions options,
                    RecoveryOwnerState* legacy_owner_state = nullptr)
        : allocator_(std::move(allocator)),
          ownership_(ownership),
          options_(options),
          legacy_owner_state_(legacy_owner_state) {}

    struct LegacyOwnershipContext {
        RecoveryOwnerState* state = nullptr;
        uint64_t pid = 0;
        std::atomic<uint64_t> lease_token{0};
    };

    static uint64_t SelfPid();
    static bool LegacyIsOwner(const void* context) noexcept;
    static void LegacyHeartbeat(void* context) noexcept;
    Status ScanSlots(RecoveryReport& report, bool repair);
    Status ScanLegacyImage(RecoveryReport& report, bool repair);

    CentralSlabAllocator allocator_;
    RecoveryOwnership ownership_;
    RecoveryScannerOptions options_;
    RecoveryOwnerState* legacy_owner_state_ = nullptr;
    std::shared_ptr<LegacyOwnershipContext> legacy_context_;
    Layout legacy_layout_{};
    std::byte* legacy_base_ = nullptr;
    uint64_t legacy_size_ = 0;
};

}  // namespace mino::shm::recovery

#endif  // MINO_SHM_RECOVERY_SCANNER_H_
