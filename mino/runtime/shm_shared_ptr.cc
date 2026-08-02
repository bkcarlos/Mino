// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/shm_shared_ptr.h"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <new>
#include <thread>

#if defined(__unix__) || defined(__APPLE__)
#include <signal.h>
#include <unistd.h>
#endif

namespace mino {

static_assert(std::atomic<uint32_t>::is_always_lock_free,
              "shared Pin counters require lock-free uint32 atomics");
static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "shared Pin records require lock-free uint64 atomics");

namespace {

constexpr uint64_t kPinTableMagic = 0x4D494E4F50494E31ULL;  // "MINOPIN1"
constexpr uint32_t kObjectQuotaBucketCount = 8192;
constexpr uint32_t kOwnerQuotaBucketCount = 256;
constexpr uint64_t kStatePhaseMask = 0x7;
constexpr uint64_t kStateMutatorShift = 3;
constexpr uint64_t kStateMutatorMask = ShmPinTable::kMutatorCapacity - 1;
constexpr uint64_t kStateEpochShift = 11;
constexpr uint64_t kStateClaiming = 1;
constexpr uint64_t kStatePrepared = 2;
constexpr uint64_t kStateObjectCharging = 3;
constexpr uint64_t kStateObjectCharged = 4;
constexpr uint64_t kStateOwnerCharging = 5;
constexpr uint64_t kStateActive = 6;
constexpr uint64_t kStateReleasing = 7;
constexpr uint64_t kMutatorPhaseMask = 0x3;
constexpr uint64_t kMutatorWriting = 1;
constexpr uint64_t kMutatorActive = 2;
constexpr uint64_t kWritingPidMask = 0xFFFFFFFFULL;
constexpr uint64_t kWritingFingerprintMask = (1ULL << 30) - 1;
constexpr uint64_t kWritingFingerprintShift = 34;
static_assert((ShmPinTable::kMutatorCapacity &
               (ShmPinTable::kMutatorCapacity - 1)) == 0);
constexpr uint32_t kInvalidRecordIndex =
    std::numeric_limits<uint32_t>::max();

constexpr uint32_t kObjectCharge = 1u << 0;
constexpr uint32_t kOwnerCharge = 1u << 1;
constexpr uint32_t kObjectAcquireOp = 1u << 0;
constexpr uint32_t kOwnerAcquireOp = 1u << 1;
constexpr uint32_t kObjectReleaseOp = 1u << 2;
constexpr uint32_t kOwnerReleaseOp = 1u << 3;

// A quota word is normally just a count. While a record changes that count,
// bit 31 is set and the remaining bits carry both the old count and the exact
// record index that owns the recoverable micro-transaction.
constexpr uint32_t kQuotaLocked = 1u << 31;
constexpr uint32_t kQuotaCountBits = 17;
constexpr uint32_t kQuotaCountMask = (1u << kQuotaCountBits) - 1;
constexpr uint32_t kQuotaRecordShift = kQuotaCountBits;
constexpr uint32_t kQuotaRecordMask = (1u << 14) - 1;
static_assert(ShmPinTable::kPinCapacity <= (1u << 14));
static_assert(ShmPinTable::kMaxPinsPerProcess < kQuotaCountMask);

std::atomic<ShmPinTable::PinFaultInjectorForTesting> g_fault_injector{nullptr};
std::atomic<void*> g_fault_context{nullptr};

constexpr size_t AlignUp(size_t value, size_t alignment) noexcept {
    return (value + alignment - 1) & ~(alignment - 1);
}

uint64_t Mix64(uint64_t value) noexcept {
    value ^= value >> 30;
    value *= 0xBF58476D1CE4E5B9ULL;
    value ^= value >> 27;
    value *= 0x94D049BB133111EBULL;
    return value ^ (value >> 31);
}

uint64_t HandleHash(ShmHandle handle) noexcept {
    uint64_t hash = Mix64(handle.offset);
    hash ^= Mix64((static_cast<uint64_t>(handle.region_id) << 32) |
                  handle.generation);
    return Mix64(hash);
}

uint64_t OwnerHash(const ProcessIdentity& owner) noexcept {
    uint64_t hash = Mix64(owner.node_id);
    hash ^= Mix64(owner.process_id);
    hash ^= Mix64(owner.process_epoch);
    hash ^= Mix64(owner.start_time_ns);
    return Mix64(hash);
}

uint64_t EncodeState(uint64_t epoch, uint32_t mutator_index,
                     uint64_t phase) noexcept {
    return (epoch << kStateEpochShift) |
           (static_cast<uint64_t>(mutator_index) << kStateMutatorShift) |
           phase;
}

uint64_t StatePhase(uint64_t state) noexcept {
    return state & kStatePhaseMask;
}

uint32_t StateMutator(uint64_t state) noexcept {
    return static_cast<uint32_t>((state >> kStateMutatorShift) &
                                 kStateMutatorMask);
}

uint64_t OperationIdentity(uint64_t state) noexcept {
    return state & ~kStatePhaseMask;
}

bool SameOperation(uint64_t lhs, uint64_t rhs) noexcept {
    return lhs != 0 && rhs != 0 &&
           OperationIdentity(lhs) == OperationIdentity(rhs);
}

uint64_t EncodeMutatorState(uint64_t epoch, uint64_t phase) noexcept {
    return (epoch << 2) | phase;
}

uint64_t EncodeWritingMutator(const ProcessIdentity& identity) noexcept {
    const uint64_t fingerprint =
        Mix64(identity.node_id ^ Mix64(identity.process_epoch) ^
              Mix64(identity.start_time_ns)) &
        kWritingFingerprintMask;
    return (fingerprint << kWritingFingerprintShift) |
           ((identity.process_id & kWritingPidMask) << 2) | kMutatorWriting;
}

uint64_t WritingMutatorPid(uint64_t state) noexcept {
    return (state >> 2) & kWritingPidMask;
}

bool WritingMutatorDefinitelyDead(uint64_t state) noexcept {
    if ((state & kMutatorPhaseMask) != kMutatorWriting) return false;
    const uint64_t pid = WritingMutatorPid(state);
    if (pid == 0) return true;
    const ProcessIdentity& current = ProcessIdentity::Current();
    if (pid == current.process_id) {
        // The OS cannot host two live incarnations with the same PID. A
        // different atomic writing key therefore belongs to a recycled PID.
        return state != EncodeWritingMutator(current);
    }
#if defined(__linux__)
    char path[64];
    std::snprintf(path, sizeof(path), "/proc/%llu/stat",
                  static_cast<unsigned long long>(pid));
    errno = 0;
    FILE* stat = std::fopen(path, "r");
    if (stat == nullptr) return errno == ENOENT || errno == ESRCH;
    char line[1024];
    const bool read = std::fgets(line, sizeof(line), stat) != nullptr;
    std::fclose(stat);
    if (!read) return false;
    const char* close = std::strrchr(line, ')');
    if (close == nullptr || close[1] != ' ') return false;
    const char process_state = close[2];
    return process_state == 'Z' || process_state == 'X';
#elif defined(__unix__) || defined(__APPLE__)
    if (::kill(static_cast<pid_t>(pid), 0) == 0 || errno == EPERM) return false;
    return errno == ESRCH;
#else
    return false;
#endif
}

uint64_t MutatorPhase(uint64_t state) noexcept {
    return state & kMutatorPhaseMask;
}

bool IsBoundPhase(uint64_t phase) noexcept {
    return phase >= kStatePrepared && phase <= kStateReleasing;
}

bool AdvanceState(ShmPinTable::SharedRecord& record, uint64_t expected,
                  uint64_t desired) noexcept;

bool QuotaIsLocked(uint32_t word) noexcept {
    return (word & kQuotaLocked) != 0;
}

uint32_t QuotaCount(uint32_t word) noexcept {
    return word & kQuotaCountMask;
}

uint32_t QuotaRecord(uint32_t word) noexcept {
    return (word >> kQuotaRecordShift) & kQuotaRecordMask;
}

uint32_t LockedQuotaWord(uint32_t record_index, uint32_t count) noexcept {
    return kQuotaLocked | (record_index << kQuotaRecordShift) | count;
}

void InjectFault(ShmPinTable::PinFaultPointForTesting point) noexcept {
    const auto injector = g_fault_injector.load(std::memory_order_acquire);
    if (injector != nullptr) {
        injector(point, g_fault_context.load(std::memory_order_acquire));
    }
}

}  // namespace

struct alignas(64) ShmPinTable::SharedControl {
    std::atomic<uint64_t> magic{0};
    std::atomic<uint32_t> layout_version{0};
    uint32_t pin_capacity = 0;
    uint32_t object_quota_bucket_count = 0;
    uint32_t owner_quota_bucket_count = 0;
    std::atomic<uint64_t> next_epoch{1};
    uint32_t mutator_capacity = 0;
    std::atomic<uint64_t> registration_lock{0};
    unsigned char reserved[16] = {};
};

static_assert(sizeof(ShmPinTable::SharedControl) == 64);
static_assert(alignof(ShmPinTable::SharedControl) == 64);

struct alignas(64) ShmPinTable::SharedMutator {
    std::atomic<uint64_t> state{0};
    std::atomic<uint64_t> node_id{0};
    std::atomic<uint64_t> process_id{0};
    std::atomic<uint64_t> process_epoch{0};
    std::atomic<uint64_t> start_time_ns{0};
    unsigned char reserved[24] = {};
};

static_assert(sizeof(ShmPinTable::SharedMutator) == 64);
static_assert(alignof(ShmPinTable::SharedMutator) == 64);

struct alignas(64) ShmPinTable::SharedCleanup {
    std::atomic<uint64_t> token{0};
    std::atomic<uint64_t> owner_node_id{0};
    std::atomic<uint64_t> owner_process_id{0};
    std::atomic<uint64_t> owner_process_epoch{0};
    std::atomic<uint64_t> owner_start_time_ns{0};
    std::atomic<uint32_t> metadata_ready{0};
    unsigned char reserved[20] = {};
};

static_assert(sizeof(ShmPinTable::SharedCleanup) == 64);
static_assert(alignof(ShmPinTable::SharedCleanup) == 64);

struct alignas(64) ShmPinTable::SharedRecord {
    std::atomic<uint64_t> state{0};
    std::atomic<uint64_t> handle_offset{0};
    std::atomic<uint64_t> owner_node_id{0};
    std::atomic<uint64_t> owner_process_id{0};
    std::atomic<uint64_t> owner_process_epoch{0};
    std::atomic<uint64_t> owner_start_time_ns{0};
    std::atomic<uint32_t> handle_generation{0};
    std::atomic<uint32_t> handle_region_id{0};
    std::atomic<uint32_t> object_quota_bucket{0};
    std::atomic<uint32_t> owner_quota_bucket{0};

    // `reservation` is the first word claimed and the last word cleared. It
    // carries the mutator slot and operation epoch even before wider metadata is
    // ready, and prevents reuse throughout release-tail recovery.
    std::atomic<uint64_t> reservation{0};
    std::atomic<uint32_t> charges{0};
    std::atomic<uint32_t> quota_ops{0};
    std::atomic<uint32_t> metadata_ready{0};
    std::atomic<uint64_t> recovery_guard{0};
    unsigned char reserved[20] = {};
};

static_assert(sizeof(ShmPinTable::SharedRecord) == 128);
static_assert(alignof(ShmPinTable::SharedRecord) == 64);

namespace {

struct SharedLayout {
    ShmPinTable::SharedControl* control;
    ShmPinTable::SharedMutator* mutators;
    ShmPinTable::SharedCleanup* cleanups;
    std::atomic<uint32_t>* object_quotas;
    std::atomic<uint32_t>* owner_quotas;
    std::atomic<uint64_t>* owner_fences;
    ShmPinTable::SharedRecord* records;
};

SharedLayout LayoutOf(void* shm_base) noexcept {
    auto* bytes = static_cast<std::byte*>(shm_base);
    size_t offset = 0;
    auto* control = reinterpret_cast<ShmPinTable::SharedControl*>(bytes + offset);
    offset += sizeof(ShmPinTable::SharedControl);
    offset = AlignUp(offset, alignof(ShmPinTable::SharedMutator));
    auto* mutators =
        reinterpret_cast<ShmPinTable::SharedMutator*>(bytes + offset);
    offset += sizeof(ShmPinTable::SharedMutator) *
              ShmPinTable::kMutatorCapacity;
    offset = AlignUp(offset, alignof(ShmPinTable::SharedCleanup));
    auto* cleanups =
        reinterpret_cast<ShmPinTable::SharedCleanup*>(bytes + offset);
    offset += sizeof(ShmPinTable::SharedCleanup) * kOwnerQuotaBucketCount;
    offset = AlignUp(offset, alignof(std::atomic<uint32_t>));
    auto* object_quotas =
        reinterpret_cast<std::atomic<uint32_t>*>(bytes + offset);
    offset += sizeof(std::atomic<uint32_t>) * kObjectQuotaBucketCount;
    offset = AlignUp(offset, alignof(std::atomic<uint32_t>));
    auto* owner_quotas =
        reinterpret_cast<std::atomic<uint32_t>*>(bytes + offset);
    offset += sizeof(std::atomic<uint32_t>) * kOwnerQuotaBucketCount;
    offset = AlignUp(offset, alignof(std::atomic<uint64_t>));
    auto* owner_fences =
        reinterpret_cast<std::atomic<uint64_t>*>(bytes + offset);
    offset += sizeof(std::atomic<uint64_t>) * kOwnerQuotaBucketCount;
    offset = AlignUp(offset, alignof(ShmPinTable::SharedRecord));
    auto* records = reinterpret_cast<ShmPinTable::SharedRecord*>(bytes + offset);
    return SharedLayout{control, mutators, cleanups, object_quotas,
                        owner_quotas, owner_fences, records};
}

bool AdvanceState(ShmPinTable::SharedRecord& record, uint64_t expected,
                  uint64_t desired) noexcept {
    return record.state.compare_exchange_strong(
        expected, desired, std::memory_order_acq_rel,
        std::memory_order_acquire);
}

ProcessIdentity LoadMutator(
    const ShmPinTable::SharedMutator& mutator) noexcept {
    const uint64_t expected_state =
        mutator.state.load(std::memory_order_acquire);
    if (MutatorPhase(expected_state) != kMutatorActive) return {};
    ProcessIdentity identity{
        .node_id = mutator.node_id.load(std::memory_order_relaxed),
        .process_id = mutator.process_id.load(std::memory_order_relaxed),
        .process_epoch = mutator.process_epoch.load(std::memory_order_relaxed),
        .start_time_ns = mutator.start_time_ns.load(std::memory_order_relaxed),
    };
    if (mutator.state.load(std::memory_order_acquire) != expected_state) {
        return {};
    }
    return identity;
}

ProcessIdentityLiveness ProbeMutator(
    const ShmPinTable::SharedMutator& mutator) noexcept {
    const ProcessIdentity identity = LoadMutator(mutator);
    if (identity.IsZero()) return ProcessIdentityLiveness::kUnknown;
    const ProcessIdentity& current = ProcessIdentity::Current();
    if (identity.node_id != 0 && current.node_id != 0 &&
        identity.node_id != current.node_id) {
        return ProcessIdentityLiveness::kUnknown;
    }
    return ProbeProcessIdentity(identity);
}

ShmHandle LoadHandle(const ShmPinTable::SharedRecord& record) noexcept {
    return ShmHandle{
        .offset = record.handle_offset.load(std::memory_order_relaxed),
        .generation =
            record.handle_generation.load(std::memory_order_relaxed),
        .region_id = record.handle_region_id.load(std::memory_order_relaxed),
    };
}

ProcessIdentity LoadCleanupOwner(
    const ShmPinTable::SharedCleanup& cleanup, uint64_t expected_token) noexcept {
    if (expected_token == 0 ||
        cleanup.token.load(std::memory_order_acquire) != expected_token ||
        cleanup.metadata_ready.load(std::memory_order_acquire) == 0) {
        return {};
    }
    ProcessIdentity owner{
        .node_id = cleanup.owner_node_id.load(std::memory_order_relaxed),
        .process_id = cleanup.owner_process_id.load(std::memory_order_relaxed),
        .process_epoch =
            cleanup.owner_process_epoch.load(std::memory_order_relaxed),
        .start_time_ns =
            cleanup.owner_start_time_ns.load(std::memory_order_relaxed),
    };
    if (cleanup.token.load(std::memory_order_acquire) != expected_token) {
        return {};
    }
    return owner;
}

ProcessIdentity LoadOwner(const ShmPinTable::SharedRecord& record) noexcept {
    return ProcessIdentity{
        .node_id = record.owner_node_id.load(std::memory_order_relaxed),
        .process_id = record.owner_process_id.load(std::memory_order_relaxed),
        .process_epoch =
            record.owner_process_epoch.load(std::memory_order_relaxed),
        .start_time_ns =
            record.owner_start_time_ns.load(std::memory_order_relaxed),
    };
}

bool StableRecordMatch(const ShmPinTable::SharedRecord& record,
                       uint64_t expected_state, ShmHandle handle,
                       const ProcessIdentity* owner) noexcept {
    if (record.state.load(std::memory_order_acquire) != expected_state ||
        record.metadata_ready.load(std::memory_order_acquire) == 0) {
        return false;
    }
    const ShmHandle observed_handle = LoadHandle(record);
    const ProcessIdentity observed_owner = LoadOwner(record);
    if (record.state.load(std::memory_order_acquire) != expected_state) {
        return false;
    }
    return observed_handle == handle &&
           (owner == nullptr || observed_owner == *owner);
}

Result<uint32_t> LockQuotaForIncrement(std::atomic<uint32_t>& quota,
                                       uint32_t record_index,
                                       uint32_t limit) noexcept {
    uint32_t observed = quota.load(std::memory_order_acquire);
    for (uint32_t attempt = 0; attempt < 64; ++attempt) {
        if (QuotaIsLocked(observed)) {
            observed = quota.load(std::memory_order_acquire);
            continue;
        }
        const uint32_t count = QuotaCount(observed);
        if (count >= limit) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        if (quota.compare_exchange_weak(
                observed, LockedQuotaWord(record_index, count),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            return count;
        }
    }
    return Status::Error(StatusCode::kWouldBlock,
                         "quota bucket remained contended");
}

bool ResolveInterruptedAcquire(ShmPinTable::SharedRecord& record,
                               std::atomic<uint32_t>& quota,
                               uint32_t record_index, uint32_t acquire_op,
                               uint32_t charge, uint64_t release_state,
                               bool wait_for_live_mutator) noexcept {
    if (record.state.load(std::memory_order_acquire) != release_state) {
        return false;
    }
    if ((record.quota_ops.load(std::memory_order_acquire) & acquire_op) == 0) {
        return true;
    }
    uint32_t word = quota.load(std::memory_order_acquire);
    while (QuotaIsLocked(word)) {
        if (QuotaRecord(word) != record_index) return false;
        if (!wait_for_live_mutator) {
            // A dead mutator cannot finish the increment. Restore the exact old
            // count carried by its recoverable lock word.
            record.charges.fetch_and(~charge, std::memory_order_acq_rel);
            quota.store(QuotaCount(word), std::memory_order_release);
            break;
        }
        std::this_thread::yield();
        word = quota.load(std::memory_order_acquire);
    }
    // If the bucket is unlocked and the charge bit is present, the increment
    // committed before the crash and normal release below will decrement it.
    record.quota_ops.fetch_and(~acquire_op, std::memory_order_acq_rel);
    return true;
}

bool ReleaseQuota(ShmPinTable::SharedRecord& record,
                  std::atomic<uint32_t>& quota, uint32_t record_index,
                  uint32_t charge, uint32_t release_op,
                  ShmPinTable::PinFaultPointForTesting fault_point,
                  uint64_t release_state) noexcept {
    const uint32_t initial_charges =
        record.charges.load(std::memory_order_acquire);
    const uint32_t initial_ops = record.quota_ops.load(std::memory_order_acquire);
    if ((initial_charges & charge) == 0 &&
        (initial_ops & release_op) == 0) {
        return true;
    }

    record.quota_ops.fetch_or(release_op, std::memory_order_acq_rel);
    for (;;) {
        if (record.state.load(std::memory_order_acquire) != release_state) {
            return false;
        }
        uint32_t word = quota.load(std::memory_order_acquire);
        if (QuotaIsLocked(word)) {
            if (QuotaRecord(word) != record_index) {
                std::this_thread::yield();
                continue;
            }
            const uint32_t count = QuotaCount(word);
            if (count == 0) return false;
            InjectFault(fault_point);
            record.charges.fetch_and(~charge, std::memory_order_acq_rel);
            quota.store(count - 1, std::memory_order_release);
            record.quota_ops.fetch_and(~release_op, std::memory_order_acq_rel);
            return true;
        }

        if ((record.charges.load(std::memory_order_acquire) & charge) == 0) {
            record.quota_ops.fetch_and(~release_op, std::memory_order_acq_rel);
            return true;
        }
        const uint32_t count = QuotaCount(word);
        if (count == 0) return false;
        if (!quota.compare_exchange_weak(
                word, LockedQuotaWord(record_index, count),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            continue;
        }
        InjectFault(fault_point);
        record.charges.fetch_and(~charge, std::memory_order_acq_rel);
        quota.store(count - 1, std::memory_order_release);
        record.quota_ops.fetch_and(~release_op, std::memory_order_acq_rel);
        return true;
    }
    return false;
}

}  // namespace

void ShmPinTable::SetFaultInjectorForTesting(
    PinFaultInjectorForTesting injector, void* context) noexcept {
    g_fault_context.store(context, std::memory_order_release);
    g_fault_injector.store(injector, std::memory_order_release);
}

size_t ShmPinTable::RequiredSize() noexcept {
    size_t size = sizeof(SharedControl);
    size = AlignUp(size, alignof(SharedMutator));
    size += sizeof(SharedMutator) * kMutatorCapacity;
    size = AlignUp(size, alignof(SharedCleanup));
    size += sizeof(SharedCleanup) * kOwnerQuotaBucketCount;
    size = AlignUp(size, alignof(std::atomic<uint32_t>));
    size += sizeof(std::atomic<uint32_t>) * kObjectQuotaBucketCount;
    size = AlignUp(size, alignof(std::atomic<uint32_t>));
    size += sizeof(std::atomic<uint32_t>) * kOwnerQuotaBucketCount;
    size = AlignUp(size, alignof(std::atomic<uint64_t>));
    size += sizeof(std::atomic<uint64_t>) * kOwnerQuotaBucketCount;
    size = AlignUp(size, alignof(SharedRecord));
    size += sizeof(SharedRecord) * kPinCapacity;
    return size;
}

Result<ShmPinTable> ShmPinTable::Init(void* shm_base, size_t shm_size,
                                      CentralSlabAllocator& allocator) {
    if (shm_base == nullptr ||
        reinterpret_cast<uintptr_t>(shm_base) % alignof(SharedControl) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Pin table base must be 64-byte aligned");
    }
    if (shm_size < RequiredSize()) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "shared memory is too small for the fixed Pin table");
    }

    const SharedLayout layout = LayoutOf(shm_base);
    new (layout.control) SharedControl{};
    for (uint32_t i = 0; i < kMutatorCapacity; ++i) {
        new (&layout.mutators[i]) SharedMutator{};
    }
    for (uint32_t i = 0; i < kOwnerQuotaBucketCount; ++i) {
        new (&layout.cleanups[i]) SharedCleanup{};
    }
    for (uint32_t i = 0; i < kObjectQuotaBucketCount; ++i) {
        new (&layout.object_quotas[i]) std::atomic<uint32_t>{0};
    }
    for (uint32_t i = 0; i < kOwnerQuotaBucketCount; ++i) {
        new (&layout.owner_quotas[i]) std::atomic<uint32_t>{0};
    }
    for (uint32_t i = 0; i < kOwnerQuotaBucketCount; ++i) {
        new (&layout.owner_fences[i]) std::atomic<uint64_t>{0};
    }
    for (uint32_t i = 0; i < kPinCapacity; ++i) {
        new (&layout.records[i]) SharedRecord{};
    }

    layout.control->pin_capacity = kPinCapacity;
    layout.control->object_quota_bucket_count = kObjectQuotaBucketCount;
    layout.control->owner_quota_bucket_count = kOwnerQuotaBucketCount;
    layout.control->mutator_capacity = kMutatorCapacity;
    layout.control->layout_version.store(kLayoutVersion,
                                         std::memory_order_relaxed);
    layout.control->magic.store(kPinTableMagic, std::memory_order_release);
    allocator.SetReclaimGuard(&ShmPinTable::ReclaimGuardCallback,
                              layout.control);
    return ShmPinTable(layout.control, layout.mutators, layout.cleanups,
                       layout.records, &allocator);
}

Result<ShmPinTable> ShmPinTable::Attach(void* shm_base, size_t shm_size,
                                        CentralSlabAllocator& allocator) {
    if (shm_base == nullptr ||
        reinterpret_cast<uintptr_t>(shm_base) % alignof(SharedControl) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Pin table base must be 64-byte aligned");
    }
    if (shm_size < RequiredSize()) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "shared memory is too small for the fixed Pin table");
    }

    const SharedLayout layout = LayoutOf(shm_base);
    if (layout.control->magic.load(std::memory_order_acquire) !=
        kPinTableMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "Pin table magic mismatch");
    }
    if (layout.control->layout_version.load(std::memory_order_acquire) !=
            kLayoutVersion ||
        layout.control->pin_capacity != kPinCapacity ||
        layout.control->object_quota_bucket_count !=
            kObjectQuotaBucketCount ||
        layout.control->owner_quota_bucket_count != kOwnerQuotaBucketCount ||
        layout.control->mutator_capacity != kMutatorCapacity) {
        return Status::Error(
            StatusCode::kCorruption,
            "Pin table layout mismatch (version 5 recovery fencing required)");
    }

    ShmPinTable table(layout.control, layout.mutators, layout.cleanups,
                      layout.records, &allocator);
    allocator.SetReclaimGuard(&ShmPinTable::ReclaimGuardCallback,
                              layout.control);
    uint64_t registration =
        layout.control->registration_lock.load(std::memory_order_acquire);
    if (MutatorPhase(registration) == kMutatorWriting &&
        WritingMutatorDefinitelyDead(registration)) {
        (void)layout.control->registration_lock.compare_exchange_strong(
            registration, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
    }
    table.RecoverDeadCleanupsOnAttach();
    table.RecoverDeadMutatorsOnAttach();
    return table;
}

Result<uint32_t> ShmPinTable::EnsureCurrentMutator() noexcept {
    const ProcessIdentity& current = ProcessIdentity::Current();
    const uint64_t writing_state = EncodeWritingMutator(current);
    const uint32_t start = static_cast<uint32_t>(
        OwnerHash(current) % kMutatorCapacity);

    for (uint32_t i = 0; i < kMutatorCapacity; ++i) {
        if (LoadMutator(mutators_[i]) == current) return i;
    }
    InjectFault(PinFaultPointForTesting::kMutatorBeforeRegistrationLock);

    for (;;) {
        uint64_t lock = 0;
        if (control_->registration_lock.compare_exchange_weak(
                lock, writing_state, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            break;
        }
        if (MutatorPhase(lock) == kMutatorWriting &&
            WritingMutatorDefinitelyDead(lock)) {
            (void)control_->registration_lock.compare_exchange_strong(
                lock, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        } else {
            std::this_thread::yield();
        }
    }
    auto unlock_registration = [&]() noexcept {
        uint64_t expected = writing_state;
        (void)control_->registration_lock.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
    };

    uint32_t claimed = kMutatorCapacity;
    for (uint32_t i = 0; i < kMutatorCapacity; ++i) {
        if (LoadMutator(mutators_[i]) == current) {
            claimed = i;
            break;
        }
    }
    if (claimed == kMutatorCapacity) {
        for (uint32_t distance = 0; distance < kMutatorCapacity; ++distance) {
            const uint32_t index = (start + distance) % kMutatorCapacity;
            uint64_t observed =
                mutators_[index].state.load(std::memory_order_acquire);
            if (MutatorPhase(observed) == kMutatorWriting &&
                WritingMutatorDefinitelyDead(observed)) {
                (void)mutators_[index].state.compare_exchange_strong(
                    observed, 0, std::memory_order_acq_rel,
                    std::memory_order_acquire);
                observed = 0;
            }
            if (observed == 0 &&
                mutators_[index].state.compare_exchange_strong(
                    observed, writing_state, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                claimed = index;
                break;
            }
        }
        if (claimed == kMutatorCapacity) {
            unlock_registration();
            return Status::Error(StatusCode::kResourceExhausted,
                                 "Pin table mutator registry is full");
        }
        InjectFault(PinFaultPointForTesting::kMutatorSlotWriting);

        SharedMutator& slot = mutators_[claimed];
        slot.node_id.store(current.node_id, std::memory_order_relaxed);
        slot.process_id.store(current.process_id, std::memory_order_relaxed);
        slot.process_epoch.store(current.process_epoch,
                                 std::memory_order_relaxed);
        slot.start_time_ns.store(current.start_time_ns,
                                 std::memory_order_relaxed);
        InjectFault(PinFaultPointForTesting::kMutatorIdentityReady);

        const uint64_t slot_epoch =
            control_->next_epoch.fetch_add(1, std::memory_order_relaxed);
        if (slot_epoch == 0 ||
            slot_epoch > (std::numeric_limits<uint64_t>::max() >> 2)) {
            uint64_t expected = writing_state;
            (void)slot.state.compare_exchange_strong(
                expected, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
            unlock_registration();
            return Status::Error(StatusCode::kResourceExhausted);
        }
        const uint64_t active_state =
            EncodeMutatorState(slot_epoch, kMutatorActive);
        uint64_t expected = writing_state;
        if (!slot.state.compare_exchange_strong(
                expected, active_state, std::memory_order_release,
                std::memory_order_acquire)) {
            unlock_registration();
            return Status::Error(StatusCode::kWouldBlock,
                                 "mutator registration lost its lock");
        }
        InjectFault(PinFaultPointForTesting::kMutatorActivePublished);
    }

    // Final canonical election occurs after Active publication while the
    // recoverable global registration lock excludes every other registrar.
    for (;;) {
        uint32_t canonical = kMutatorCapacity;
        uint32_t active_count = 0;
        for (uint32_t i = 0; i < kMutatorCapacity; ++i) {
            if (LoadMutator(mutators_[i]) == current) {
                canonical = canonical == kMutatorCapacity
                                ? i
                                : (i < canonical ? i : canonical);
                ++active_count;
            }
        }
        if (canonical == kMutatorCapacity) {
            unlock_registration();
            return Status::Error(StatusCode::kCorruption,
                                 "mutator Active publication disappeared");
        }
        for (uint32_t i = 0; i < kMutatorCapacity; ++i) {
            if (i == canonical) continue;
            const uint64_t state =
                mutators_[i].state.load(std::memory_order_acquire);
            if (MutatorPhase(state) == kMutatorActive &&
                LoadMutator(mutators_[i]) == current) {
                uint64_t duplicate = state;
                (void)mutators_[i].state.compare_exchange_strong(
                    duplicate, 0, std::memory_order_acq_rel,
                    std::memory_order_acquire);
            }
        }
        uint32_t confirmed = 0;
        for (uint32_t i = 0; i < kMutatorCapacity; ++i) {
            if (LoadMutator(mutators_[i]) == current) ++confirmed;
        }
        if (confirmed == 1) {
            unlock_registration();
            return canonical;
        }
        if (active_count <= 1) std::this_thread::yield();
    }
}

Result<ShmPinToken> ShmPinTable::Pin(
    ShmHandle handle, const ShmPinContract& contract,
    const ProcessIdentity& owner) noexcept {
    if (handle.IsNull() || owner.IsZero() || owner.process_epoch == 0 ||
        contract.object_size == 0) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    return AcquireRecord(handle, owner, &contract, false);
}

Result<ShmPinToken> ShmPinTable::AcquireRecord(
    ShmHandle handle, const ProcessIdentity& owner,
    const ShmPinContract* contract, bool allow_retired) noexcept {
    const SharedLayout layout = LayoutOf(control_);
    const uint32_t object_bucket = static_cast<uint32_t>(
        HandleHash(handle) % kObjectQuotaBucketCount);
    const uint32_t owner_bucket = static_cast<uint32_t>(
        OwnerHash(owner) % kOwnerQuotaBucketCount);
    const uint64_t owner_fence =
        layout.owner_fences[owner_bucket].load(std::memory_order_acquire);
    if ((owner_fence & 1u) != 0) {
        return Status::Error(StatusCode::kWouldBlock,
                             "logical owner cleanup is in progress");
    }
    Result<uint32_t> mutator = EnsureCurrentMutator();
    if (!mutator.ok()) return mutator.status();

    const uint64_t epoch =
        control_->next_epoch.fetch_add(1, std::memory_order_relaxed);
    if (epoch == 0 ||
        epoch > (std::numeric_limits<uint64_t>::max() >> kStateEpochShift)) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
    const uint64_t claiming_state =
        EncodeState(epoch, *mutator, kStateClaiming);
    const uint32_t start = static_cast<uint32_t>(
        Mix64(HandleHash(handle) ^ OwnerHash(owner) ^ epoch) % kPinCapacity);

    uint32_t record_index = kInvalidRecordIndex;
    for (uint32_t distance = 0; distance < kPinCapacity; ++distance) {
        const uint32_t index = (start + distance) % kPinCapacity;
        uint64_t expected_reservation = 0;
        if (!records_[index].reservation.compare_exchange_strong(
                expected_reservation, claiming_state,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            continue;
        }
        uint64_t expected_state = 0;
        if (records_[index].state.compare_exchange_strong(
                expected_state, claiming_state, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            record_index = index;
            break;
        }
        uint64_t failed_claim = claiming_state;
        (void)records_[index].reservation.compare_exchange_strong(
            failed_claim, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
    }
    if (record_index == kInvalidRecordIndex) {
        return Status::Error(StatusCode::kResourceExhausted);
    }

    SharedRecord& record = records_[record_index];
    InjectFault(PinFaultPointForTesting::kRecordClaimed);
    record.charges.store(0, std::memory_order_relaxed);
    record.quota_ops.store(0, std::memory_order_relaxed);
    record.metadata_ready.store(0, std::memory_order_relaxed);
    record.handle_offset.store(handle.offset, std::memory_order_relaxed);
    record.handle_generation.store(handle.generation,
                                   std::memory_order_relaxed);
    record.handle_region_id.store(handle.region_id, std::memory_order_relaxed);
    record.owner_node_id.store(owner.node_id, std::memory_order_relaxed);
    record.owner_process_id.store(owner.process_id, std::memory_order_relaxed);
    record.owner_process_epoch.store(owner.process_epoch,
                                     std::memory_order_relaxed);
    record.owner_start_time_ns.store(owner.start_time_ns,
                                     std::memory_order_relaxed);
    record.object_quota_bucket.store(object_bucket, std::memory_order_relaxed);
    record.owner_quota_bucket.store(owner_bucket, std::memory_order_relaxed);
    record.metadata_ready.store(1, std::memory_order_release);
    InjectFault(PinFaultPointForTesting::kRecordMetadataReady);
    const uint64_t prepared_state =
        EncodeState(epoch, *mutator, kStatePrepared);
    if (!AdvanceState(record, claiming_state, prepared_state)) {
        const Status rollback = RecoverRecord(
            record_index, record.state.load(std::memory_order_acquire),
            claiming_state);
        return rollback.ok()
                   ? Status::Error(StatusCode::kNotFound,
                                   "Pin publication was fenced by cleanup")
                   : rollback;
    }
    InjectFault(PinFaultPointForTesting::kRecordPrepared);

    if (layout.owner_fences[owner_bucket].load(std::memory_order_acquire) !=
        owner_fence) {
        const Status rollback =
            RecoverRecord(record_index, prepared_state, claiming_state);
        return rollback.ok()
                   ? Status::Error(StatusCode::kNotFound,
                                   "owner cleanup fenced Pin publication")
                   : rollback;
    }

    const uint64_t object_charging_state =
        EncodeState(epoch, *mutator, kStateObjectCharging);
    if (!AdvanceState(record, prepared_state, object_charging_state)) {
        const Status rollback = RecoverRecord(
            record_index, record.state.load(std::memory_order_acquire),
            claiming_state);
        return rollback.ok() ? Status::Error(StatusCode::kNotFound) : rollback;
    }
    auto rollback_failure = [&](Status failure) -> Result<ShmPinToken> {
        const Status rollback = RecoverRecord(
            record_index, record.state.load(std::memory_order_acquire),
            claiming_state);
        if (!rollback.ok()) return rollback;
        MaybeReclaim(handle).ok();
        return failure;
    };

    record.quota_ops.fetch_or(kObjectAcquireOp, std::memory_order_acq_rel);
    Result<uint32_t> object_count = LockQuotaForIncrement(
        layout.object_quotas[object_bucket], record_index,
        kMaxPinsPerObject);
    if (!object_count.ok()) {
        record.quota_ops.fetch_and(~kObjectAcquireOp,
                                   std::memory_order_acq_rel);
        return rollback_failure(object_count.status());
    }
    InjectFault(PinFaultPointForTesting::kObjectQuotaLocked);
    if (record.state.load(std::memory_order_acquire) !=
        object_charging_state) {
        layout.object_quotas[object_bucket].store(*object_count,
                                                  std::memory_order_release);
        record.quota_ops.fetch_and(~kObjectAcquireOp,
                                   std::memory_order_acq_rel);
        return rollback_failure(Status::Error(
            StatusCode::kNotFound, "object quota lock lost operation fence"));
    }
    record.charges.fetch_or(kObjectCharge, std::memory_order_acq_rel);
    layout.object_quotas[object_bucket].store(*object_count + 1,
                                               std::memory_order_release);
    record.quota_ops.fetch_and(~kObjectAcquireOp, std::memory_order_acq_rel);
    const uint64_t object_charged_state =
        EncodeState(epoch, *mutator, kStateObjectCharged);
    if (!AdvanceState(record, object_charging_state, object_charged_state)) {
        return rollback_failure(Status::Error(
            StatusCode::kNotFound, "object charge was fenced by cleanup"));
    }
    InjectFault(PinFaultPointForTesting::kObjectQuotaCommitted);

    Result<SlabView> slab = allocator_->Inspect(handle);
    if (!slab.ok()) return rollback_failure(slab.status());
    if ((!allow_retired && slab->state != ObjectState::kPublished) ||
        (allow_retired && slab->state != ObjectState::kPublished &&
         slab->state != ObjectState::kRetired)) {
        return rollback_failure(Status::Error(
            StatusCode::kInvalidArgument,
            allow_retired
                ? "clone source is neither Published nor Retired"
                : "only a Published object may establish a new Pin"));
    }
    if (slab->data == nullptr) {
        return rollback_failure(Status::Error(StatusCode::kCorruption));
    }
    if (contract != nullptr &&
        (slab->type_id != contract->type_id ||
         slab->schema_short_id != contract->schema_short_id ||
         slab->layout_version != contract->layout_version ||
         slab->object_size != contract->object_size ||
         slab->capacity < contract->object_size)) {
        return rollback_failure(Status::Error(
            StatusCode::kSchemaMismatch,
            "object does not match the requested Pin contract"));
    }
    InjectFault(PinFaultPointForTesting::kObjectInspected);

    const uint64_t owner_charging_state =
        EncodeState(epoch, *mutator, kStateOwnerCharging);
    if (!AdvanceState(record, object_charged_state, owner_charging_state)) {
        return rollback_failure(Status::Error(
            StatusCode::kNotFound, "owner charge was fenced by cleanup"));
    }
    record.quota_ops.fetch_or(kOwnerAcquireOp, std::memory_order_acq_rel);
    Result<uint32_t> owner_count = LockQuotaForIncrement(
        layout.owner_quotas[owner_bucket], record_index,
        kMaxPinsPerProcess);
    if (!owner_count.ok()) {
        record.quota_ops.fetch_and(~kOwnerAcquireOp,
                                   std::memory_order_acq_rel);
        return rollback_failure(owner_count.status());
    }
    InjectFault(PinFaultPointForTesting::kOwnerQuotaLocked);
    if (record.state.load(std::memory_order_acquire) != owner_charging_state) {
        layout.owner_quotas[owner_bucket].store(*owner_count,
                                                std::memory_order_release);
        record.quota_ops.fetch_and(~kOwnerAcquireOp,
                                   std::memory_order_acq_rel);
        return rollback_failure(Status::Error(
            StatusCode::kNotFound, "owner quota lock lost operation fence"));
    }
    record.charges.fetch_or(kOwnerCharge, std::memory_order_acq_rel);
    layout.owner_quotas[owner_bucket].store(*owner_count + 1,
                                            std::memory_order_release);
    record.quota_ops.fetch_and(~kOwnerAcquireOp, std::memory_order_acq_rel);
    InjectFault(PinFaultPointForTesting::kOwnerQuotaCommitted);

    const uint64_t active_state =
        EncodeState(epoch, *mutator, kStateActive);
    if (layout.owner_fences[owner_bucket].load(std::memory_order_acquire) !=
        owner_fence) {
        return rollback_failure(Status::Error(
            StatusCode::kNotFound,
            "owner cleanup fenced Pin immediately before activation"));
    }
    if (!AdvanceState(record, owner_charging_state, active_state)) {
        return rollback_failure(Status::Error(
            StatusCode::kNotFound, "Pin activation was fenced by cleanup"));
    }
    InjectFault(PinFaultPointForTesting::kRecordActive);
    return ShmPinToken(this, record_index, active_state, claiming_state,
                       handle, owner, slab->data);
}

Result<ShmPinToken> ShmPinTable::CloneToken(
    const ShmPinToken& source) noexcept {
    if (source.table_ != this || source.record_index_ >= kPinCapacity ||
        StatePhase(source.record_state_) != kStateActive ||
        records_[source.record_index_].reservation.load(
            std::memory_order_acquire) != source.record_reservation_ ||
        !SameOperation(source.record_state_, source.record_reservation_) ||
        !StableRecordMatch(records_[source.record_index_],
                           source.record_state_, source.handle_,
                           &source.owner_)) {
        return Status::Error(StatusCode::kNotFound);
    }
    return AcquireRecord(source.handle_, source.owner_, nullptr, true);
}

Status ShmPinTable::RecoverRecord(uint32_t record_index, uint64_t state,
                                  uint64_t reservation) noexcept {
    if (record_index >= kPinCapacity) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    SharedRecord& record = records_[record_index];
    if (reservation == 0 ||
        record.reservation.load(std::memory_order_acquire) != reservation) {
        return Status::Error(StatusCode::kNotFound,
                             "record reservation generation changed");
    }
    if (state == 0) {
        if (record.state.load(std::memory_order_acquire) != 0) {
            return Status::Error(StatusCode::kNotFound);
        }
        uint64_t guard =
            record.recovery_guard.load(std::memory_order_acquire);
        if (guard != 0) {
            if (!SameOperation(guard, reservation)) {
                return Status::Error(StatusCode::kNotFound,
                                     "release tail generation changed");
            }
            const uint32_t guard_mutator = StateMutator(guard);
            if (guard_mutator >= kMutatorCapacity) {
                return Status::Error(StatusCode::kCorruption);
            }
            if (ProbeMutator(mutators_[guard_mutator]) !=
                ProcessIdentityLiveness::kDead) {
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "release tail mutator cannot be proven dead");
            }
        }
        Result<uint32_t> current_mutator = EnsureCurrentMutator();
        if (!current_mutator.ok()) return current_mutator.status();
        const uint64_t recovery_epoch =
            control_->next_epoch.fetch_add(1, std::memory_order_relaxed);
        if (recovery_epoch == 0 ||
            recovery_epoch >
                (std::numeric_limits<uint64_t>::max() >> kStateEpochShift)) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        const uint64_t takeover =
            EncodeState(recovery_epoch, *current_mutator, kStateReleasing);
        if (!record.recovery_guard.compare_exchange_strong(
                guard, takeover, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return Status::Error(StatusCode::kWouldBlock);
        }
        uint64_t expected_reservation = reservation;
        if (!record.reservation.compare_exchange_strong(
                expected_reservation, takeover, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            uint64_t expected_guard = takeover;
            (void)record.recovery_guard.compare_exchange_strong(
                expected_guard, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
            return Status::Error(StatusCode::kNotFound);
        }
        guard = takeover;
        reservation = takeover;
        if (record.state.load(std::memory_order_acquire) != 0) {
            return Status::Error(StatusCode::kNotFound);
        }
        record.metadata_ready.store(0, std::memory_order_release);
        if (guard != 0) {
            uint64_t expected_guard = guard;
            (void)record.recovery_guard.compare_exchange_strong(
                expected_guard, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
        uint64_t final_reservation = reservation;
        (void)record.reservation.compare_exchange_strong(
            final_reservation, 0, std::memory_order_acq_rel,
            std::memory_order_acquire);
        return Status::Ok();
    }
    if (!IsBoundPhase(StatePhase(state)) ||
        record.metadata_ready.load(std::memory_order_acquire) == 0) {
        return Status::Error(StatusCode::kWouldBlock,
                             "record metadata is not recoverably published");
    }

    Result<uint32_t> current_mutator = EnsureCurrentMutator();
    if (!current_mutator.ok()) return current_mutator.status();
    const ProcessIdentity& current = ProcessIdentity::Current();
    uint64_t releasing_state = 0;
    bool wait_for_live_acquire = false;
    for (uint32_t attempt = 0; attempt < 1024; ++attempt) {
        state = record.state.load(std::memory_order_acquire);
        if (state == 0) return RecoverRecord(record_index, 0, reservation);
        if (record.reservation.load(std::memory_order_acquire) != reservation) {
            return Status::Error(StatusCode::kNotFound,
                                 "record reservation generation changed");
        }
        if (!IsBoundPhase(StatePhase(state))) {
            return Status::Error(StatusCode::kCorruption,
                                 "record has an invalid recovery phase");
        }
        if (!SameOperation(state, reservation)) {
            if (StatePhase(state) != kStateReleasing) {
                return Status::Error(StatusCode::kNotFound,
                                     "record state generation changed");
            }
            const uint32_t state_mutator = StateMutator(state);
            if (state_mutator >= kMutatorCapacity) {
                return Status::Error(StatusCode::kCorruption);
            }
            const bool owned_by_current =
                state_mutator == *current_mutator &&
                LoadMutator(mutators_[state_mutator]) == current;
            if (!owned_by_current &&
                ProbeMutator(mutators_[state_mutator]) !=
                    ProcessIdentityLiveness::kDead) {
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "new release generation is still owned by a live mutator");
            }
            uint64_t expected_reservation = reservation;
            if (!record.reservation.compare_exchange_strong(
                    expected_reservation, state, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return Status::Error(StatusCode::kNotFound);
            }
            reservation = state;
        }
        if (StatePhase(state) == kStateReleasing) {
            const uint32_t owner_index = StateMutator(state);
            if (owner_index >= kMutatorCapacity) {
                return Status::Error(StatusCode::kCorruption);
            }
            if (owner_index == *current_mutator &&
                LoadMutator(mutators_[owner_index]) == current) {
                releasing_state = state;
                break;
            }
            const ProcessIdentityLiveness liveness =
                ProbeMutator(mutators_[owner_index]);
            if (liveness == ProcessIdentityLiveness::kAlive) {
                std::this_thread::yield();
                continue;
            }
            if (liveness == ProcessIdentityLiveness::kUnknown) {
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "release owner identity cannot be proven dead");
            }
            wait_for_live_acquire = false;
        } else {
            const uint32_t operation_mutator = StateMutator(state);
            if (operation_mutator >= kMutatorCapacity) {
                return Status::Error(StatusCode::kCorruption);
            }
            if (operation_mutator == *current_mutator &&
                LoadMutator(mutators_[operation_mutator]) == current) {
                wait_for_live_acquire = true;
            } else {
                const ProcessIdentityLiveness liveness =
                    ProbeMutator(mutators_[operation_mutator]);
                if (liveness == ProcessIdentityLiveness::kUnknown) {
                    return Status::Error(
                        StatusCode::kWouldBlock,
                        "record mutator identity cannot be proven dead");
                }
                wait_for_live_acquire =
                    liveness == ProcessIdentityLiveness::kAlive;
            }
        }

        const uint64_t recovery_epoch =
            control_->next_epoch.fetch_add(1, std::memory_order_relaxed);
        if (recovery_epoch == 0 ||
            recovery_epoch >
                (std::numeric_limits<uint64_t>::max() >> kStateEpochShift)) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        const uint64_t desired =
            EncodeState(recovery_epoch, *current_mutator, kStateReleasing);
        if (AdvanceState(record, state, desired)) {
            uint64_t expected_reservation = reservation;
            if (!record.reservation.compare_exchange_strong(
                    expected_reservation, desired, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return Status::Error(StatusCode::kNotFound,
                                     "release reservation generation changed");
            }
            reservation = desired;
            releasing_state = desired;
            break;
        }
    }
    if (releasing_state == 0) {
        return Status::Error(StatusCode::kWouldBlock,
                             "another live mutator is releasing the Pin");
    }
    if (reservation != releasing_state) {
        uint64_t expected_reservation = reservation;
        if (!record.reservation.compare_exchange_strong(
                expected_reservation, releasing_state,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            return Status::Error(StatusCode::kNotFound,
                                 "release reservation generation changed");
        }
        reservation = releasing_state;
    }

    uint64_t guard = 0;
    if (!record.recovery_guard.compare_exchange_strong(
            guard, releasing_state, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        if (guard == releasing_state) {
            for (uint32_t attempt = 0; attempt < 1024; ++attempt) {
                if (record.recovery_guard.load(std::memory_order_acquire) == 0) {
                    return Status::Ok();
                }
                std::this_thread::yield();
            }
            return Status::Error(StatusCode::kWouldBlock,
                                 "release operation is active in another thread");
        }
        const uint32_t guard_mutator = StateMutator(guard);
        if (guard_mutator >= kMutatorCapacity ||
            ProbeMutator(mutators_[guard_mutator]) !=
                ProcessIdentityLiveness::kDead ||
            !record.recovery_guard.compare_exchange_strong(
                guard, releasing_state, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "recovery guard owner is not recoverable");
        }
    }

    if (record.reservation.load(std::memory_order_acquire) !=
            releasing_state ||
        record.state.load(std::memory_order_acquire) != releasing_state) {
        return Status::Error(StatusCode::kWouldBlock,
                             "release operation lost its reservation fence");
    }

    const SharedLayout layout = LayoutOf(control_);
    const uint32_t object_bucket =
        record.object_quota_bucket.load(std::memory_order_relaxed);
    const uint32_t owner_bucket =
        record.owner_quota_bucket.load(std::memory_order_relaxed);
    if (object_bucket >= kObjectQuotaBucketCount ||
        owner_bucket >= kOwnerQuotaBucketCount) {
        return Status::Error(StatusCode::kCorruption,
                             "record quota bucket is out of range");
    }

    if (!ResolveInterruptedAcquire(record, layout.owner_quotas[owner_bucket],
                                   record_index, kOwnerAcquireOp, kOwnerCharge,
                                   releasing_state, wait_for_live_acquire) ||
        !ResolveInterruptedAcquire(record, layout.object_quotas[object_bucket],
                                   record_index, kObjectAcquireOp, kObjectCharge,
                                   releasing_state, wait_for_live_acquire)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "interrupted quota acquire lost operation fence");
    }
    if (!ReleaseQuota(record, layout.owner_quotas[owner_bucket], record_index,
                      kOwnerCharge, kOwnerReleaseOp,
                      PinFaultPointForTesting::kOwnerQuotaReleaseLocked,
                      releasing_state) ||
        !ReleaseQuota(record, layout.object_quotas[object_bucket], record_index,
                      kObjectCharge, kObjectReleaseOp,
                      PinFaultPointForTesting::kObjectQuotaReleaseLocked,
                      releasing_state)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "quota release lost operation fence");
    }

    if (!AdvanceState(record, releasing_state, 0)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "release completion lost operation fence");
    }
    InjectFault(PinFaultPointForTesting::kRecordStateCleared);
    record.metadata_ready.store(0, std::memory_order_release);
    uint64_t expected_guard = releasing_state;
    (void)record.recovery_guard.compare_exchange_strong(
        expected_guard, 0, std::memory_order_acq_rel,
        std::memory_order_acquire);
    uint64_t expected_reservation = releasing_state;
    (void)record.reservation.compare_exchange_strong(
        expected_reservation, 0, std::memory_order_acq_rel,
        std::memory_order_acquire);
    return Status::Ok();
}

Status ShmPinTable::ReleaseRecord(uint32_t record_index,
                                  uint64_t* record_state,
                                  uint64_t* record_reservation,
                                  ShmHandle handle,
                                  const ProcessIdentity& owner) noexcept {
    if (record_index >= kPinCapacity || record_state == nullptr ||
        record_reservation == nullptr || *record_reservation == 0) {
        return Status::Error(StatusCode::kNotFound);
    }

    SharedRecord& record = records_[record_index];
    uint64_t observed_state = record.state.load(std::memory_order_acquire);
    uint64_t observed_reservation =
        record.reservation.load(std::memory_order_acquire);
    if (observed_state == 0) {
        if (observed_reservation == 0) return Status::Ok();
        if (observed_reservation != *record_reservation) {
            return Status::Error(StatusCode::kNotFound,
                                 "token reservation generation is stale");
        }
        return RecoverRecord(record_index, 0, observed_reservation);
    }
    if (!SameOperation(observed_reservation, *record_reservation)) {
        return StatePhase(observed_state) == kStateReleasing
                   ? Status::Error(StatusCode::kWouldBlock,
                                   "a newer release generation owns the record")
                   : Status::Error(StatusCode::kNotFound,
                                   "token reservation generation is stale");
    }

    if (observed_state == *record_state &&
        StatePhase(observed_state) == kStateActive &&
        StableRecordMatch(record, observed_state, handle, &owner)) {
        const uint64_t releasing_state =
            (observed_state & ~kStatePhaseMask) | kStateReleasing;
        uint64_t expected_state = observed_state;
        if (!record.state.compare_exchange_strong(
                expected_state, releasing_state, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            observed_state = expected_state;
        } else {
            uint64_t expected_reservation = observed_reservation;
            if (!record.reservation.compare_exchange_strong(
                    expected_reservation, releasing_state,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                return Status::Error(StatusCode::kWouldBlock,
                                     "release reservation handoff raced");
            }
            *record_state = releasing_state;
            *record_reservation = releasing_state;
            InjectFault(PinFaultPointForTesting::kRecordReleasing);
            const Status recovered = RecoverRecord(
                record_index, releasing_state, releasing_state);
            if (!recovered.ok()) return recovered;
            return MaybeReclaim(handle);
        }
    }

    observed_reservation =
        record.reservation.load(std::memory_order_acquire);
    if (StatePhase(observed_state) == kStateReleasing &&
        SameOperation(observed_state, observed_reservation) &&
        SameOperation(observed_reservation, *record_reservation)) {
        *record_state = observed_state;
        *record_reservation = observed_reservation;
        const Status recovered = RecoverRecord(
            record_index, observed_state, observed_reservation);
        if (!recovered.ok()) return recovered;
        return MaybeReclaim(handle);
    }
    if (record.state.load(std::memory_order_acquire) == 0 &&
        record.reservation.load(std::memory_order_acquire) == 0) {
        return Status::Ok();
    }
    return Status::Error(StatusCode::kNotFound,
                         "token no longer owns the record generation");
}

uint32_t ShmPinTable::PinCount(ShmHandle handle) const noexcept {
    uint32_t count = 0;
    for (uint32_t i = 0; i < kPinCapacity; ++i) {
        const uint64_t state = records_[i].state.load(std::memory_order_acquire);
        if (!IsBoundPhase(StatePhase(state))) continue;
        if (StableRecordMatch(records_[i], state, handle, nullptr)) ++count;
    }
    return count;
}

uint32_t ShmPinTable::OwnerPinCount(
    const ProcessIdentity& owner) const noexcept {
    uint32_t count = 0;
    for (uint32_t i = 0; i < kPinCapacity; ++i) {
        const uint64_t state = records_[i].state.load(std::memory_order_acquire);
        if (!IsBoundPhase(StatePhase(state))) continue;
        const ShmHandle handle = LoadHandle(records_[i]);
        if (StableRecordMatch(records_[i], state, handle, &owner)) ++count;
    }
    return count;
}

uint32_t ShmPinTable::FinishOwnerCleanup(const ProcessIdentity& owner,
                                         uint32_t owner_bucket,
                                         uint64_t cleanup_token) noexcept {
    const SharedLayout layout = LayoutOf(control_);
    SharedCleanup& cleanup = cleanups_[owner_bucket];
    std::atomic<uint64_t>& fence = layout.owner_fences[owner_bucket];
    uint32_t cleaned = 0;
    uint32_t stable_passes = 0;
    while (stable_passes < 2) {
        if (cleanup.token.load(std::memory_order_acquire) != cleanup_token ||
            (fence.load(std::memory_order_acquire) & 1u) == 0) {
            return cleaned;
        }
        bool saw_matching_record = false;
        for (uint32_t i = 0; i < kPinCapacity; ++i) {
            SharedRecord& record = records_[i];
            const uint64_t reservation =
                record.reservation.load(std::memory_order_acquire);
            const uint64_t state = record.state.load(std::memory_order_acquire);
            if (state == 0) {
                if (reservation != 0 &&
                    record.metadata_ready.load(std::memory_order_acquire) != 0 &&
                    LoadOwner(record) == owner) {
                    saw_matching_record = true;
                    if (RecoverRecord(i, 0, reservation).ok()) ++cleaned;
                }
                continue;
            }
            if (StatePhase(state) == kStateClaiming) {
                if (record.metadata_ready.load(std::memory_order_acquire) != 0 &&
                    LoadOwner(record) == owner &&
                    record.state.load(std::memory_order_acquire) == state) {
                    saw_matching_record = true;
                    uint64_t expected_state = state;
                    if (record.state.compare_exchange_strong(
                            expected_state, 0, std::memory_order_acq_rel,
                            std::memory_order_acquire)) {
                        record.metadata_ready.store(0,
                                                    std::memory_order_release);
                        uint64_t expected_reservation = reservation;
                        (void)record.reservation.compare_exchange_strong(
                            expected_reservation, 0,
                            std::memory_order_acq_rel,
                            std::memory_order_acquire);
                        ++cleaned;
                    }
                }
                continue;
            }
            if (!IsBoundPhase(StatePhase(state))) continue;
            const ShmHandle handle = LoadHandle(record);
            if (!StableRecordMatch(record, state, handle, &owner)) continue;
            saw_matching_record = true;
            if (RecoverRecord(i, state, reservation).ok()) {
                ++cleaned;
                if (!handle.IsNull()) MaybeReclaim(handle).ok();
            }
        }
        if (saw_matching_record) {
            stable_passes = 0;
            std::this_thread::yield();
        } else {
            ++stable_passes;
        }
    }

    uint64_t odd_fence = fence.load(std::memory_order_acquire);
    if ((odd_fence & 1u) == 0 ||
        cleanup.token.load(std::memory_order_acquire) != cleanup_token ||
        !fence.compare_exchange_strong(
            odd_fence, odd_fence + 1, std::memory_order_release,
            std::memory_order_acquire)) {
        return cleaned;
    }
    cleanup.metadata_ready.store(0, std::memory_order_release);
    uint64_t expected_token = cleanup_token;
    (void)cleanup.token.compare_exchange_strong(
        expected_token, 0, std::memory_order_acq_rel,
        std::memory_order_acquire);
    return cleaned;
}

uint32_t ShmPinTable::CleanupOwner(const ProcessIdentity& owner) noexcept {
    if (owner.IsZero() || owner.process_epoch == 0) return 0;
    Result<uint32_t> mutator = EnsureCurrentMutator();
    if (!mutator.ok()) return 0;
    const uint32_t owner_bucket = static_cast<uint32_t>(
        OwnerHash(owner) % kOwnerQuotaBucketCount);
    SharedCleanup& cleanup = cleanups_[owner_bucket];

    const uint64_t cleanup_epoch =
        control_->next_epoch.fetch_add(1, std::memory_order_relaxed);
    if (cleanup_epoch == 0 ||
        cleanup_epoch >
            (std::numeric_limits<uint64_t>::max() >> kStateEpochShift)) {
        return 0;
    }
    const uint64_t cleanup_token =
        EncodeState(cleanup_epoch, *mutator, kStateReleasing);
    for (;;) {
        uint64_t observed = cleanup.token.load(std::memory_order_acquire);
        if (observed == 0) {
            if (cleanup.token.compare_exchange_weak(
                    observed, cleanup_token, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                break;
            }
            continue;
        }
        const uint32_t cleaner_mutator = StateMutator(observed);
        if (cleaner_mutator >= kMutatorCapacity ||
            ProbeMutator(mutators_[cleaner_mutator]) !=
                ProcessIdentityLiveness::kDead) {
            return 0;
        }
        RecoverDeadCleanupsOnAttach();
    }

    cleanup.owner_node_id.store(owner.node_id, std::memory_order_relaxed);
    cleanup.owner_process_id.store(owner.process_id,
                                   std::memory_order_relaxed);
    cleanup.owner_process_epoch.store(owner.process_epoch,
                                      std::memory_order_relaxed);
    cleanup.owner_start_time_ns.store(owner.start_time_ns,
                                      std::memory_order_relaxed);
    cleanup.metadata_ready.store(1, std::memory_order_release);

    const SharedLayout layout = LayoutOf(control_);
    std::atomic<uint64_t>& fence = layout.owner_fences[owner_bucket];
    uint64_t even_fence = fence.load(std::memory_order_acquire);
    if ((even_fence & 1u) != 0 ||
        !fence.compare_exchange_strong(
            even_fence, even_fence + 1, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return 0;
    }
    InjectFault(PinFaultPointForTesting::kOwnerCleanupOdd);
    return FinishOwnerCleanup(owner, owner_bucket, cleanup_token);
}

void ShmPinTable::RecoverDeadCleanupsOnAttach() noexcept {
    const SharedLayout layout = LayoutOf(control_);
    for (uint32_t bucket = 0; bucket < kOwnerQuotaBucketCount; ++bucket) {
        SharedCleanup& cleanup = cleanups_[bucket];
        uint64_t token = cleanup.token.load(std::memory_order_acquire);
        if (token == 0) continue;
        const uint32_t cleaner_mutator = StateMutator(token);
        if (cleaner_mutator >= kMutatorCapacity ||
            ProbeMutator(mutators_[cleaner_mutator]) !=
                ProcessIdentityLiveness::kDead) {
            continue;
        }
        Result<uint32_t> current_mutator = EnsureCurrentMutator();
        if (!current_mutator.ok()) continue;
        const uint64_t recovery_epoch =
            control_->next_epoch.fetch_add(1, std::memory_order_relaxed);
        if (recovery_epoch == 0 ||
            recovery_epoch >
                (std::numeric_limits<uint64_t>::max() >> kStateEpochShift)) {
            continue;
        }
        const uint64_t takeover =
            EncodeState(recovery_epoch, *current_mutator, kStateReleasing);
        if (!cleanup.token.compare_exchange_strong(
                token, takeover, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            continue;
        }
        const ProcessIdentity owner = LoadCleanupOwner(cleanup, takeover);
        const uint64_t fence =
            layout.owner_fences[bucket].load(std::memory_order_acquire);
        if ((fence & 1u) != 0 && !owner.IsZero()) {
            (void)FinishOwnerCleanup(owner, bucket, takeover);
        } else if ((fence & 1u) == 0) {
            cleanup.metadata_ready.store(0, std::memory_order_release);
            uint64_t expected = takeover;
            (void)cleanup.token.compare_exchange_strong(
                expected, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
    }
}

void ShmPinTable::RecoverDeadMutatorsOnAttach() noexcept {
    for (uint32_t i = 0; i < kPinCapacity; ++i) {
        SharedRecord& record = records_[i];
        const uint64_t reservation =
            record.reservation.load(std::memory_order_acquire);
        if (reservation == 0) continue;
        const uint64_t state = record.state.load(std::memory_order_acquire);
        const uint64_t owner_state = state == 0 ? reservation : state;
        const uint32_t mutator_index = StateMutator(owner_state);
        if (mutator_index >= kMutatorCapacity ||
            ProbeMutator(mutators_[mutator_index]) !=
                ProcessIdentityLiveness::kDead) {
            continue;
        }

        if (state != 0 && StatePhase(state) == kStateClaiming) {
            uint64_t expected_state = state;
            if (!record.state.compare_exchange_strong(
                    expected_state, 0, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                continue;
            }
            record.metadata_ready.store(0, std::memory_order_release);
            uint64_t expected_reservation = reservation;
            (void)record.reservation.compare_exchange_strong(
                expected_reservation, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
            continue;
        }
        const ShmHandle handle =
            record.metadata_ready.load(std::memory_order_acquire) != 0
                ? LoadHandle(record)
                : ShmHandle{};
        if (RecoverRecord(i, state, reservation).ok() && !handle.IsNull()) {
            MaybeReclaim(handle).ok();
        }
    }

    for (uint32_t mutator_index = 0; mutator_index < kMutatorCapacity;
         ++mutator_index) {
        const uint64_t mutator_state =
            mutators_[mutator_index].state.load(std::memory_order_acquire);
        if (MutatorPhase(mutator_state) == kMutatorWriting) {
            if (WritingMutatorDefinitelyDead(mutator_state)) {
                uint64_t writing = mutator_state;
                (void)mutators_[mutator_index].state.compare_exchange_strong(
                    writing, 0, std::memory_order_acq_rel,
                    std::memory_order_acquire);
            }
            continue;
        }
        if (MutatorPhase(mutator_state) != kMutatorActive ||
            ProbeMutator(mutators_[mutator_index]) !=
                ProcessIdentityLiveness::kDead) {
            continue;
        }
        bool referenced = false;
        for (uint32_t record_index = 0; record_index < kPinCapacity;
             ++record_index) {
            const uint64_t reservation = records_[record_index].reservation.load(
                std::memory_order_acquire);
            if (reservation != 0 &&
                StateMutator(reservation) == mutator_index) {
                referenced = true;
                break;
            }
        }
        if (!referenced) {
            for (uint32_t bucket = 0; bucket < kOwnerQuotaBucketCount;
                 ++bucket) {
                const uint64_t cleanup_token =
                    cleanups_[bucket].token.load(std::memory_order_acquire);
                if (cleanup_token != 0 &&
                    StateMutator(cleanup_token) == mutator_index) {
                    referenced = true;
                    break;
                }
            }
        }
        if (!referenced) {
            uint64_t active = mutator_state;
            (void)mutators_[mutator_index].state.compare_exchange_strong(
                active, 0, std::memory_order_acq_rel,
                std::memory_order_acquire);
        }
    }
}

void ShmPinTable::CleanupOwnerCallback(const ProcessIdentity& owner,
                                       void* context) noexcept {
    if (context != nullptr) {
        static_cast<ShmPinTable*>(context)->CleanupOwner(owner);
    }
}

Status ShmPinTable::RetirePayload(ShmHandle handle) noexcept {
    const Status retired = allocator_->Retire(handle);
    if (!retired.ok() && retired.code() != StatusCode::kNotFound) {
        return retired;
    }
    if (retired.code() == StatusCode::kNotFound) {
        return Status::Ok();
    }
    return MaybeReclaim(handle);
}

Status ShmPinTable::RetirePayloadCallback(ShmHandle handle,
                                          void* context) noexcept {
    if (context == nullptr) {
        return Status::Error(StatusCode::kUnavailable,
                             "payload retire context is null");
    }
    return static_cast<ShmPinTable*>(context)->RetirePayload(handle);
}

bool ShmPinTable::HasPinOrPublicationGuard(ShmHandle handle) const noexcept {
    if (handle.IsNull() || control_ == nullptr) return false;
    const SharedLayout layout = LayoutOf(control_);
    const uint32_t object_bucket = static_cast<uint32_t>(
        HandleHash(handle) % kObjectQuotaBucketCount);
    return layout.object_quotas[object_bucket].load(std::memory_order_acquire) !=
           0;
}

bool ShmPinTable::ReclaimGuardCallback(ShmHandle handle,
                                       void* context) noexcept {
    if (handle.IsNull() || context == nullptr) return false;
    const SharedLayout layout = LayoutOf(context);
    const uint32_t object_bucket = static_cast<uint32_t>(
        HandleHash(handle) % kObjectQuotaBucketCount);
    return layout.object_quotas[object_bucket].load(std::memory_order_acquire) ==
           0;
}

Status ShmPinTable::MaybeReclaim(ShmHandle handle) noexcept {
    Result<SlabView> slab = allocator_->Inspect(handle);
    if (!slab.ok()) {
        return slab.status().code() == StatusCode::kNotFound
                   ? Status::Ok()
                   : slab.status();
    }
    if (slab->state != ObjectState::kRetired || PinCount(handle) != 0) {
        return Status::Ok();
    }
    const Status reclaimed = allocator_->Reclaim(handle);
    return reclaimed.code() == StatusCode::kNotFound ? Status::Ok() : reclaimed;
}

ShmPinToken::ShmPinToken(ShmPinTable* table, uint32_t record_index,
                         uint64_t record_state, uint64_t record_reservation,
                         ShmHandle handle, const ProcessIdentity& owner,
                         const void* data) noexcept
    : table_(table),
      record_index_(record_index),
      record_state_(record_state),
      record_reservation_(record_reservation),
      handle_(handle),
      owner_(owner),
      data_(data) {}

ShmPinToken::ShmPinToken(ShmPinToken&& other) noexcept
    : table_(other.table_),
      record_index_(other.record_index_),
      record_state_(other.record_state_),
      record_reservation_(other.record_reservation_),
      handle_(other.handle_),
      owner_(other.owner_),
      data_(other.data_) {
    other.Disarm();
}

ShmPinToken& ShmPinToken::operator=(ShmPinToken&& other) noexcept {
    if (this != &other) {
        if (active() && !Release().ok()) return *this;
        table_ = other.table_;
        record_index_ = other.record_index_;
        record_state_ = other.record_state_;
        record_reservation_ = other.record_reservation_;
        handle_ = other.handle_;
        owner_ = other.owner_;
        data_ = other.data_;
        other.Disarm();
    }
    return *this;
}

ShmPinToken::~ShmPinToken() {
    if (active()) Release().ok();
}

Status ShmPinToken::Release() noexcept {
    if (!active()) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    const Status released = table_->ReleaseRecord(
        record_index_, &record_state_, &record_reservation_, handle_, owner_);
    if (released.ok()) Disarm();
    return released;
}

Result<ShmPinToken> ShmPinToken::Clone() const noexcept {
    if (!active()) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    return table_->CloneToken(*this);
}

void ShmPinToken::Disarm() noexcept {
    table_ = nullptr;
    record_index_ = 0;
    record_state_ = 0;
    record_reservation_ = 0;
    handle_ = {};
    owner_ = {};
    data_ = nullptr;
}

}  // namespace mino