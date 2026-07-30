// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/shm_shared_ptr.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>

namespace mino {

static_assert(std::atomic<uint32_t>::is_always_lock_free,
              "shared Pin counters require lock-free uint32 atomics");
static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "shared Pin records require lock-free uint64 atomics");

namespace {

constexpr uint64_t kPinTableMagic = 0x4D494E4F50494E31ULL;  // "MINOPIN1"
constexpr uint32_t kPinTableLayoutVersion = 1;
constexpr uint32_t kObjectQuotaBucketCount = 8192;
constexpr uint32_t kOwnerQuotaBucketCount = 256;
constexpr uint64_t kStatePhaseMask = 0x7;
constexpr uint64_t kStateWriting = 1;
constexpr uint64_t kStateObjectCharged = 2;
constexpr uint64_t kStateActive = 3;
constexpr uint64_t kStateReleasing = 4;
constexpr uint32_t kInvalidRecordIndex =
    std::numeric_limits<uint32_t>::max();

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

uint64_t EncodeState(uint64_t epoch, uint64_t phase) noexcept {
    return (epoch << 3) | phase;
}

uint64_t StatePhase(uint64_t state) noexcept {
    return state & kStatePhaseMask;
}

bool IsBoundPhase(uint64_t phase) noexcept {
    return phase == kStateWriting || phase == kStateObjectCharged ||
           phase == kStateActive || phase == kStateReleasing;
}

bool IncrementQuota(std::atomic<uint32_t>& count, uint32_t limit) noexcept {
    uint32_t observed = count.load(std::memory_order_relaxed);
    while (observed < limit) {
        if (count.compare_exchange_weak(observed, observed + 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void DecrementQuota(std::atomic<uint32_t>& count) noexcept {
    uint32_t observed = count.load(std::memory_order_relaxed);
    while (observed != 0) {
        if (count.compare_exchange_weak(observed, observed - 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
            return;
        }
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
    unsigned char reserved[32] = {};
};

static_assert(sizeof(ShmPinTable::SharedControl) == 64);
static_assert(alignof(ShmPinTable::SharedControl) == 64);

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
    unsigned char reserved[48] = {};
};

static_assert(sizeof(ShmPinTable::SharedRecord) == 128);
static_assert(alignof(ShmPinTable::SharedRecord) == 64);

namespace {

struct SharedLayout {
    ShmPinTable::SharedControl* control;
    std::atomic<uint32_t>* object_quotas;
    std::atomic<uint32_t>* owner_quotas;
    ShmPinTable::SharedRecord* records;
};

SharedLayout LayoutOf(void* shm_base) noexcept {
    auto* bytes = static_cast<std::byte*>(shm_base);
    size_t offset = 0;
    auto* control = reinterpret_cast<ShmPinTable::SharedControl*>(bytes + offset);
    offset += sizeof(ShmPinTable::SharedControl);
    offset = AlignUp(offset, alignof(std::atomic<uint32_t>));
    auto* object_quotas =
        reinterpret_cast<std::atomic<uint32_t>*>(bytes + offset);
    offset += sizeof(std::atomic<uint32_t>) * kObjectQuotaBucketCount;
    offset = AlignUp(offset, alignof(std::atomic<uint32_t>));
    auto* owner_quotas =
        reinterpret_cast<std::atomic<uint32_t>*>(bytes + offset);
    offset += sizeof(std::atomic<uint32_t>) * kOwnerQuotaBucketCount;
    offset = AlignUp(offset, alignof(ShmPinTable::SharedRecord));
    auto* records = reinterpret_cast<ShmPinTable::SharedRecord*>(bytes + offset);
    return SharedLayout{control, object_quotas, owner_quotas, records};
}

ShmHandle LoadHandle(const ShmPinTable::SharedRecord& record) noexcept {
    return ShmHandle{
        .offset = record.handle_offset.load(std::memory_order_relaxed),
        .generation =
            record.handle_generation.load(std::memory_order_relaxed),
        .region_id = record.handle_region_id.load(std::memory_order_relaxed),
    };
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
    if (record.state.load(std::memory_order_acquire) != expected_state) {
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

}  // namespace

size_t ShmPinTable::RequiredSize() noexcept {
    size_t size = sizeof(SharedControl);
    size = AlignUp(size, alignof(std::atomic<uint32_t>));
    size += sizeof(std::atomic<uint32_t>) * kObjectQuotaBucketCount;
    size = AlignUp(size, alignof(std::atomic<uint32_t>));
    size += sizeof(std::atomic<uint32_t>) * kOwnerQuotaBucketCount;
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
    for (uint32_t i = 0; i < kObjectQuotaBucketCount; ++i) {
        new (&layout.object_quotas[i]) std::atomic<uint32_t>{0};
    }
    for (uint32_t i = 0; i < kOwnerQuotaBucketCount; ++i) {
        new (&layout.owner_quotas[i]) std::atomic<uint32_t>{0};
    }
    for (uint32_t i = 0; i < kPinCapacity; ++i) {
        new (&layout.records[i]) SharedRecord{};
    }

    layout.control->pin_capacity = kPinCapacity;
    layout.control->object_quota_bucket_count = kObjectQuotaBucketCount;
    layout.control->owner_quota_bucket_count = kOwnerQuotaBucketCount;
    layout.control->layout_version.store(kPinTableLayoutVersion,
                                         std::memory_order_relaxed);
    layout.control->magic.store(kPinTableMagic, std::memory_order_release);
    return ShmPinTable(layout.control, layout.records, &allocator);
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
            kPinTableLayoutVersion ||
        layout.control->pin_capacity != kPinCapacity ||
        layout.control->object_quota_bucket_count !=
            kObjectQuotaBucketCount ||
        layout.control->owner_quota_bucket_count != kOwnerQuotaBucketCount) {
        return Status::Error(StatusCode::kCorruption,
                             "Pin table layout mismatch");
    }
    return ShmPinTable(layout.control, layout.records, &allocator);
}

Result<ShmPinToken> ShmPinTable::Pin(ShmHandle handle,
                                      const ShmPinContract& contract,
                                      const ProcessIdentity& owner) noexcept {
    if (handle.IsNull() || owner.IsZero() || contract.object_size == 0) {
        return Status::Error(StatusCode::kInvalidArgument);
    }

    Result<SlabView> slab = allocator_->Inspect(handle);
    if (!slab.ok()) {
        return slab.status();
    }
    if (slab->state != ObjectState::kPublished) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "only a Published object may establish a new Pin");
    }
    if (slab->type_id != contract.type_id ||
        slab->schema_short_id != contract.schema_short_id ||
        slab->layout_version != contract.layout_version ||
        slab->object_size != contract.object_size ||
        slab->capacity < contract.object_size || slab->data == nullptr) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "object does not match the requested Pin contract");
    }
    return AcquireRecord(handle, owner, slab->data);
}

Result<ShmPinToken> ShmPinTable::AcquireRecord(
    ShmHandle handle, const ProcessIdentity& owner, const void* data) noexcept {
    const SharedLayout layout = LayoutOf(control_);
    const uint32_t object_bucket = static_cast<uint32_t>(
        HandleHash(handle) % kObjectQuotaBucketCount);
    const uint32_t owner_bucket = static_cast<uint32_t>(
        OwnerHash(owner) % kOwnerQuotaBucketCount);

    const uint64_t epoch =
        control_->next_epoch.fetch_add(1, std::memory_order_relaxed);
    if (epoch == 0 || epoch > (std::numeric_limits<uint64_t>::max() >> 3)) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
    const uint64_t writing_state = EncodeState(epoch, kStateWriting);
    const uint32_t start = static_cast<uint32_t>(
        Mix64(HandleHash(handle) ^ OwnerHash(owner) ^ epoch) % kPinCapacity);

    uint32_t record_index = kInvalidRecordIndex;
    for (uint32_t distance = 0; distance < kPinCapacity; ++distance) {
        const uint32_t index = (start + distance) % kPinCapacity;
        uint64_t expected = 0;
        if (records_[index].state.compare_exchange_strong(
                expected, writing_state, std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            record_index = index;
            break;
        }
    }
    if (record_index == kInvalidRecordIndex) {
        return Status::Error(StatusCode::kResourceExhausted);
    }

    SharedRecord& record = records_[record_index];
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

    if (!IncrementQuota(layout.object_quotas[object_bucket],
                        kMaxPinsPerObject)) {
        record.state.store(0, std::memory_order_release);
        return Status::Error(StatusCode::kResourceExhausted);
    }
    record.state.store(EncodeState(epoch, kStateObjectCharged),
                       std::memory_order_release);

    if (!IncrementQuota(layout.owner_quotas[owner_bucket],
                        kMaxPinsPerProcess)) {
        DecrementQuota(layout.object_quotas[object_bucket]);
        record.state.store(0, std::memory_order_release);
        return Status::Error(StatusCode::kResourceExhausted);
    }

    const uint64_t active_state = EncodeState(epoch, kStateActive);
    record.state.store(active_state, std::memory_order_release);
    return ShmPinToken(this, record_index, active_state, handle, owner, data);
}

Result<ShmPinToken> ShmPinTable::CloneToken(
    const ShmPinToken& source) noexcept {
    if (source.table_ != this || source.record_index_ >= kPinCapacity ||
        !StableRecordMatch(records_[source.record_index_],
                           source.record_state_, source.handle_,
                           &source.owner_)) {
        return Status::Error(StatusCode::kNotFound);
    }

    Result<SlabView> slab = allocator_->Inspect(source.handle_);
    if (!slab.ok()) {
        return slab.status();
    }
    if (slab->state != ObjectState::kPublished &&
        slab->state != ObjectState::kRetired) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    if (slab->data == nullptr) {
        return Status::Error(StatusCode::kCorruption);
    }
    return AcquireRecord(source.handle_, source.owner_, slab->data);
}

Status ShmPinTable::ReleaseRecord(uint32_t record_index,
                                  uint64_t record_state, ShmHandle handle,
                                  const ProcessIdentity& owner) noexcept {
    if (record_index >= kPinCapacity ||
        StatePhase(record_state) != kStateActive ||
        !StableRecordMatch(records_[record_index], record_state, handle,
                           &owner)) {
        return Status::Error(StatusCode::kNotFound);
    }

    SharedRecord& record = records_[record_index];
    const uint64_t releasing_state =
        (record_state & ~kStatePhaseMask) | kStateReleasing;
    uint64_t expected = record_state;
    if (!record.state.compare_exchange_strong(
            expected, releasing_state, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
        return Status::Error(StatusCode::kNotFound);
    }

    const SharedLayout layout = LayoutOf(control_);
    const uint32_t object_bucket =
        record.object_quota_bucket.load(std::memory_order_relaxed);
    const uint32_t owner_bucket =
        record.owner_quota_bucket.load(std::memory_order_relaxed);
    DecrementQuota(layout.owner_quotas[owner_bucket]);
    DecrementQuota(layout.object_quotas[object_bucket]);
    record.state.store(0, std::memory_order_release);
    return MaybeReclaim(handle);
}

uint32_t ShmPinTable::PinCount(ShmHandle handle) const noexcept {
    uint32_t count = 0;
    for (uint32_t i = 0; i < kPinCapacity; ++i) {
        const uint64_t state = records_[i].state.load(std::memory_order_acquire);
        if (!IsBoundPhase(StatePhase(state))) {
            continue;
        }
        if (StableRecordMatch(records_[i], state, handle, nullptr)) {
            ++count;
        }
    }
    return count;
}

uint32_t ShmPinTable::OwnerPinCount(
    const ProcessIdentity& owner) const noexcept {
    uint32_t count = 0;
    for (uint32_t i = 0; i < kPinCapacity; ++i) {
        const uint64_t state = records_[i].state.load(std::memory_order_acquire);
        if (!IsBoundPhase(StatePhase(state))) {
            continue;
        }
        const ShmHandle handle = LoadHandle(records_[i]);
        if (StableRecordMatch(records_[i], state, handle, &owner)) {
            ++count;
        }
    }
    return count;
}

uint32_t ShmPinTable::CleanupOwner(const ProcessIdentity& owner) noexcept {
    if (owner.IsZero()) {
        return 0;
    }

    const SharedLayout layout = LayoutOf(control_);
    uint32_t cleaned = 0;
    for (uint32_t i = 0; i < kPinCapacity; ++i) {
        SharedRecord& record = records_[i];
        const uint64_t state = record.state.load(std::memory_order_acquire);
        const uint64_t phase = StatePhase(state);
        if (phase != kStateWriting && phase != kStateObjectCharged &&
            phase != kStateActive) {
            continue;
        }
        const ShmHandle handle = LoadHandle(record);
        if (!StableRecordMatch(record, state, handle, &owner)) {
            continue;
        }

        const uint64_t releasing_state =
            (state & ~kStatePhaseMask) | kStateReleasing;
        uint64_t expected = state;
        if (!record.state.compare_exchange_strong(
                expected, releasing_state, std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            continue;
        }

        if (phase == kStateActive) {
            const uint32_t owner_bucket =
                record.owner_quota_bucket.load(std::memory_order_relaxed);
            DecrementQuota(layout.owner_quotas[owner_bucket]);
        }
        if (phase == kStateActive || phase == kStateObjectCharged) {
            const uint32_t object_bucket =
                record.object_quota_bucket.load(std::memory_order_relaxed);
            DecrementQuota(layout.object_quotas[object_bucket]);
        }
        record.state.store(0, std::memory_order_release);
        ++cleaned;
        if (!handle.IsNull()) {
            MaybeReclaim(handle).ok();
        }
    }
    return cleaned;
}

void ShmPinTable::CleanupOwnerCallback(const ProcessIdentity& owner,
                                       void* context) noexcept {
    if (context != nullptr) {
        static_cast<ShmPinTable*>(context)->CleanupOwner(owner);
    }
}

Status ShmPinTable::RetirePayload(ShmHandle handle) noexcept {
    const Status retired = allocator_->Retire(handle);
    if (!retired.ok()) {
        return retired;
    }
    return MaybeReclaim(handle);
}

void ShmPinTable::RetirePayloadCallback(ShmHandle handle,
                                        void* context) noexcept {
    if (context != nullptr) {
        static_cast<ShmPinTable*>(context)->RetirePayload(handle).ok();
    }
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
                         uint64_t record_state, ShmHandle handle,
                         const ProcessIdentity& owner,
                         const void* data) noexcept
    : table_(table),
      record_index_(record_index),
      record_state_(record_state),
      handle_(handle),
      owner_(owner),
      data_(data) {}

ShmPinToken::ShmPinToken(ShmPinToken&& other) noexcept
    : table_(other.table_),
      record_index_(other.record_index_),
      record_state_(other.record_state_),
      handle_(other.handle_),
      owner_(other.owner_),
      data_(other.data_) {
    other.Disarm();
}

ShmPinToken& ShmPinToken::operator=(ShmPinToken&& other) noexcept {
    if (this != &other) {
        if (active()) {
            Release().ok();
        }
        table_ = other.table_;
        record_index_ = other.record_index_;
        record_state_ = other.record_state_;
        handle_ = other.handle_;
        owner_ = other.owner_;
        data_ = other.data_;
        other.Disarm();
    }
    return *this;
}

ShmPinToken::~ShmPinToken() {
    if (active()) {
        Release().ok();
    }
}

Status ShmPinToken::Release() noexcept {
    if (!active()) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    ShmPinTable* table = table_;
    const uint32_t record_index = record_index_;
    const uint64_t record_state = record_state_;
    const ShmHandle handle = handle_;
    const ProcessIdentity owner = owner_;
    Disarm();
    return table->ReleaseRecord(record_index, record_state, handle, owner);
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
    handle_ = {};
    owner_ = {};
    data_ = nullptr;
}

}  // namespace mino
