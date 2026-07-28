// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.gnu.org/licenses/lgpl-3.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include "mino/shm/recovery/scanner.h"

#include <chrono>
#include <cstring>
#include <string>
#include <thread>

#include <unistd.h>

namespace mino::shm::recovery {

namespace {

// Formats "class/slot" finding locations without pulling in <sstream> on
// every call; details are diagnostics, not hot path (detailed design 5.1:
// 详细诊断写入进程本地日志或诊断缓冲).
std::string Finding(const char* kind, uint32_t class_id, uint32_t slot,
                    const std::string& what) {
    std::string out;
    out.reserve(96);
    out += kind;
    out += " class=";
    out += std::to_string(class_id);
    out += " slot=";
    out += std::to_string(slot);
    out += ": ";
    out += what;
    return out;
}

}  // namespace

std::string_view ObjectStateName(uint32_t value) {
    switch (static_cast<ObjectState>(value)) {
        case ObjectState::kAllocated:
            return "ALLOCATED";
        case ObjectState::kBuilding:
            return "BUILDING";
        case ObjectState::kPublished:
            return "PUBLISHED";
        case ObjectState::kRetired:
            return "RETIRED";
        case ObjectState::kAborting:
            return "ABORTING";
    }
    if (value == 0) {
        return "FREE";
    }
    return "INVALID";
}

// ---------------------------------------------------------------------------
// RecoveryOwner
// ---------------------------------------------------------------------------

uint64_t RecoveryOwner::NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

void RecoveryOwner::Initialize(RecoveryOwnerState* state) noexcept {
    state->magic = RecoveryOwnerState::kMagic;
    state->reserved0 = 0;
    state->owner_pid.store(0, std::memory_order_relaxed);
    state->epoch.store(0, std::memory_order_relaxed);
    state->lease_deadline_ns.store(0, std::memory_order_relaxed);
    state->heartbeat_ns.store(0, std::memory_order_relaxed);
    for (auto& word : state->reserved1) {
        word = 0;
    }
}

Status RecoveryOwner::TryAcquire() {
    if (state_->magic != RecoveryOwnerState::kMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "recovery owner block has bad magic");
    }
    const uint64_t now = NowNs();

    // Fast path: already the owner with a live lease -> idempotent re-acquire.
    if (IsOwner()) {
        return RenewLease();
    }

    const uint64_t observed_owner =
        state_->owner_pid.load(std::memory_order_acquire);
    if (observed_owner != 0) {
        const uint64_t deadline =
            state_->lease_deadline_ns.load(std::memory_order_acquire);
        if (deadline > now) {
            // Live owner: refuse. Detailed design 6.5 step 4: contenders wait,
            // time out, or run read-only diagnostics.
            return Status::Error(
                StatusCode::kAlreadyExists,
                "recovery owner held by pid=" +
                    std::to_string(observed_owner) + " epoch=" +
                    std::to_string(Epoch()) + " lease_deadline_ns=" +
                    std::to_string(deadline));
        }
        // Expired owner: single CAS performs takeover. The epoch increment
        // below happens only after this CAS succeeds, so two contenders cannot
        // both bump the epoch (detailed design 6.5 step 5).
        uint64_t expected = observed_owner;
        if (!state_->owner_pid.compare_exchange_strong(
                expected, pid_, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "recovery ownership raced; retry");
        }
        state_->epoch.fetch_add(1, std::memory_order_acq_rel);
    } else {
        // Free ownership: plain CAS 0 -> self, then publish a new epoch.
        uint64_t expected = 0;
        if (!state_->owner_pid.compare_exchange_strong(
                expected, pid_, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "recovery ownership raced; retry");
        }
        state_->epoch.fetch_add(1, std::memory_order_acq_rel);
    }

    // Publication order: owner_pid -> epoch -> lease. A crashed owner between
    // epoch and lease leaves an expired lease, so the next contender can take
    // over and redoes the (idempotent) recovery from scratch (6.5 step 6).
    state_->lease_deadline_ns.store(now + kLeaseDurationNs,
                                    std::memory_order_release);
    state_->heartbeat_ns.store(now, std::memory_order_release);
    return Status::Ok();
}

Status RecoveryOwner::RenewLease() {
    if (!IsOwner()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "cannot renew: not the recovery owner");
    }
    state_->lease_deadline_ns.store(NowNs() + kLeaseDurationNs,
                                    std::memory_order_release);
    return Status::Ok();
}

void RecoveryOwner::Heartbeat() noexcept {
    if (IsOwner()) {
        state_->heartbeat_ns.store(NowNs(), std::memory_order_relaxed);
    }
}

void RecoveryOwner::Release() noexcept {
    uint64_t expected = pid_;
    state_->owner_pid.compare_exchange_strong(expected, 0,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire);
}

bool RecoveryOwner::IsOwner() const noexcept {
    const uint64_t owner = state_->owner_pid.load(std::memory_order_acquire);
    if (owner != pid_) {
        return false;
    }
    return state_->lease_deadline_ns.load(std::memory_order_acquire) > NowNs();
}

uint64_t RecoveryOwner::CurrentOwner() const noexcept {
    const uint64_t owner = state_->owner_pid.load(std::memory_order_acquire);
    if (owner == 0) {
        return 0;
    }
    const uint64_t deadline =
        state_->lease_deadline_ns.load(std::memory_order_acquire);
    return deadline > NowNs() ? owner : 0;
}

Status RecoveryOwner::WaitForIdle(uint64_t timeout_ns) const {
    const uint64_t deadline = NowNs() + timeout_ns;
    while (NowNs() < deadline) {
        if (IsIdle()) {
            return Status::Ok();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return IsIdle() ? Status::Ok()
                    : Status::Error(StatusCode::kTimeout,
                                    "recovery owner still active");
}

// ---------------------------------------------------------------------------
// RecoveryScanner
// ---------------------------------------------------------------------------

Result<RecoveryScanner> RecoveryScanner::Create(std::byte* base,
                                                uint64_t size, Layout layout,
                                                RecoveryScannerOptions options) {
    if (base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument, "region base is null");
    }
    if (size == 0) {
        return Status::Error(StatusCode::kInvalidArgument, "region size is zero");
    }
    RecoveryScanner scanner(base, size, layout, options);

    // Validate the layout contract up front so scans never touch out-of-bounds
    // memory. All additions are overflow-checked.
    if (scanner.At(layout.recovery_state_offset, sizeof(RecoveryOwnerState)) ==
        nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery owner block out of bounds");
    }
    if (scanner.At(layout.class_table_offset,
                   static_cast<uint64_t>(layout.class_count) *
                       sizeof(ClassDescriptor)) == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "class descriptor table out of bounds");
    }
    const auto* recovery_state =
        static_cast<const RecoveryOwnerState*>(scanner.At(
            layout.recovery_state_offset, sizeof(RecoveryOwnerState)));
    if (recovery_state->magic != RecoveryOwnerState::kMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "recovery owner block has bad magic");
    }
    for (uint32_t i = 0; i < layout.class_count; ++i) {
        const ClassDescriptor* cls = scanner.ClassAt(i);
        const uint64_t word_count = (cls->slot_count + 63) / 64;
        if (scanner.At(cls->bitmap_offset, word_count * sizeof(BitmapWord)) ==
            nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "bitmap of class " + std::to_string(i) +
                                     " out of bounds");
        }
        if (cls->slot_count > 0 &&
            (cls->slot_stride < sizeof(SlabHeaderPrefix) ||
             scanner.At(cls->slots_offset,
                        static_cast<uint64_t>(cls->slot_count - 1) *
                                cls->slot_stride +
                            sizeof(SlabHeaderPrefix)) == nullptr)) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "slot array of class " + std::to_string(i) +
                    " out of bounds or stride too small");
        }
    }
    return scanner;
}

Result<RecoveryReport> RecoveryScanner::Scan() {
    RecoveryReport report;
    MINO_RETURN_IF_ERROR(ScanClasses(report, options_.repair));
    return report;
}

Status RecoveryScanner::ReclaimOrphanSlabs() {
    if (!Owner().IsOwner()) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "reclaim requires recovery ownership (detailed design 6.5)");
    }
    RecoveryReport report;
    MINO_RETURN_IF_ERROR(ScanClasses(report, /*repair=*/true));
    return Status::Ok();
}

Status RecoveryScanner::CleanupStaleAcks(const AckScanInput& input,
                                         uint64_t* cleared) {
    if (input.bitmap_count > 0 && input.bitmaps == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "bitmap_count > 0 but bitmaps is null");
    }
    const uint64_t dead_mask = ~input.live_subscriber_mask;
    uint64_t count = 0;
    for (uint32_t i = 0; i < input.bitmap_count; ++i) {
        const uint64_t before = input.bitmaps[i];
        const uint64_t after = before & input.live_subscriber_mask;
        input.bitmaps[i] = after;
        count += static_cast<uint64_t>(
            __builtin_popcountll(before & dead_mask));
    }
    if (cleared != nullptr) {
        *cleared = count;
    }
    return Status::Ok();
}

Status RecoveryScanner::VerifyBitmapConsistency() {
    RecoveryReport report;
    MINO_RETURN_IF_ERROR(ScanClasses(report, /*repair=*/Owner().IsOwner()));
    if (report.corrupted_slab_count > 0) {
        return Status::Error(
            StatusCode::kCorruption,
            "found " + std::to_string(report.corrupted_slab_count) +
                " slab(s) with bad magic; quarantine required");
    }
    return Status::Ok();
}

Status RecoveryScanner::ScanClasses(RecoveryReport& report, bool repair) {
    const bool owner = Owner().IsOwner();
    if (repair && !owner) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "repairing scan requires recovery ownership (detailed design 6.5)");
    }

    for (uint32_t c = 0; c < layout_.class_count; ++c) {
        const ClassDescriptor* cls = ClassAt(c);
        if (cls == nullptr) {
            return Status::Error(StatusCode::kInternal,
                                 "class descriptor table moved during scan");
        }
        const BitmapWord* bitmap = BitmapOf(*cls);
        BitmapWord* bitmap_mut = repair ? BitmapOfMut(*cls) : nullptr;
        if (bitmap == nullptr || (repair && bitmap_mut == nullptr)) {
            return Status::Error(StatusCode::kInternal,
                                 "bitmap moved during scan");
        }
        if (repair) {
            Owner().Heartbeat();
        }

        for (uint32_t s = 0; s < cls->slot_count; ++s) {
            ++report.slots_scanned;
            const bool occupied = IsBitSet(bitmap, s);
            const SlabHeaderPrefix* header = SlotAt(*cls, s);
            if (header == nullptr) {
                return Status::Error(StatusCode::kInternal,
                                     "slot array moved during scan");
            }
            // Mutable alias used only on repair paths; kept nullptr in
            // read-only mode so an accidental write is impossible.
            SlabHeaderPrefix* header_mut =
                repair ? SlotAtMut(*cls, s) : nullptr;
            const uint32_t state =
                header->object_state.load(std::memory_order_acquire);

            if (!occupied) {
                // Bitmap free: header must carry no live state. A non-FREE
                // state here is a bitmap/header inconsistency (architecture
                // 12.1: 验证位图与 Slab Header 状态一致).
                if (state != 0) {
                    ++report.bitmap_inconsistency_count;
                    report.AddDetail(Finding(
                        "bitmap_inconsistency", cls->class_id, s,
                        "bitmap free but object_state=" +
                            std::string(ObjectStateName(state)) +
                            (repair ? " (state cleared to FREE)"
                                    : " (not repaired)")));
                    if (repair) {
                        // The slot is not owned by anyone (bitmap says free),
                        // so clearing the stale state cannot race a legitimate
                        // publisher: allocation only sets state after it has
                        // CASed the bitmap bit. Re-running sees state == FREE.
                        header_mut->object_state.store(
                            0, std::memory_order_release);
                    }
                }
                continue;
            }

            // Bitmap occupied: the header must be well-formed.
            if (header->magic != kSlabMagic) {
                ++report.corrupted_slab_count;
                report.AddDetail(Finding(
                    "corruption", cls->class_id, s,
                    "bitmap occupied but header magic=0x" +
                        std::to_string(header->magic) +
                        " (NOT auto-repaired; quarantine required)"));
                continue;
            }

            if (!IsValidPublishedState(state)) {
                // Orphan: generation never published (detailed design 8.3), so
                // clearing the bitmap bit carries no ABA risk.
                ++report.orphan_slab_count;
                const std::string state_name(ObjectStateName(state));
                if (repair) {
                    header_mut->object_state.store(0,
                                                   std::memory_order_release);
                    ClearBit(bitmap_mut, s);
                    ++report.reclaimed_slab_count;
                    report.AddDetail(Finding(
                        "orphan_slab", cls->class_id, s,
                        "object_state=" + state_name + " reclaimed"));
                } else {
                    report.AddDetail(Finding("orphan_slab", cls->class_id, s,
                                             "object_state=" + state_name +
                                                 " (not reclaimed)"));
                }
                continue;
            }

            // Optional CRC verification of the immutable header prefix. A
            // zero CRC means "not computed" (e.g. pre-publish states keep
            // owner/transaction fields in flux); only published objects have
            // a frozen immutable prefix.
            if (state == static_cast<uint32_t>(ObjectState::kPublished) ||
                state == static_cast<uint32_t>(ObjectState::kRetired)) {
                const uint32_t crc = ComputeImmutableCrc(*header);
                if (header->immutable_header_crc != 0 &&
                    header->immutable_header_crc != crc) {
                    ++report.corrupted_slab_count;
                    report.AddDetail(Finding(
                        "corruption", cls->class_id, s,
                        "immutable header CRC mismatch (NOT auto-repaired)"));
                    continue;
                }
            }

            if (state == static_cast<uint32_t>(ObjectState::kRetired) &&
                options_.reclaim_retired) {
                // Detailed design 8.4: Reclaim requires no valid Borrow AND
                // no live Pin, both conditions at once.
                const uint32_t borrows =
                    header->borrow_refcount.load(std::memory_order_acquire);
                const uint32_t pins =
                    header->pin_refcount.load(std::memory_order_acquire);
                if (borrows == 0 && pins == 0) {
                    if (repair) {
                        header_mut->object_state.store(
                            0, std::memory_order_release);
                        ClearBit(bitmap_mut, s);
                        ++report.reclaimed_slab_count;
                        report.AddDetail(Finding(
                            "retired_slab", cls->class_id, s,
                            "RETIRED with no borrow/pin reclaimed"));
                    }
                } else {
                    report.AddDetail(Finding(
                        "retired_slab", cls->class_id, s,
                        "RETIRED but borrow_refcount=" +
                            std::to_string(borrows) + " pin_refcount=" +
                            std::to_string(pins) + "; kept"));
                }
            }
        }
    }
    return Status::Ok();
}

// ---------------------------------------------------------------------------
// Layout helpers
// ---------------------------------------------------------------------------

const void* RecoveryScanner::At(uint64_t offset, uint64_t bytes) const
    noexcept {
    // Checked arithmetic per detailed design 6.3: offset+bytes must not
    // overflow and must stay within the region.
    if (offset > size_ || bytes > size_ - offset) {
        return nullptr;
    }
    return base_ + offset;
}

void* RecoveryScanner::AtMut(uint64_t offset, uint64_t bytes) noexcept {
    return const_cast<void*>(static_cast<const RecoveryScanner*>(this)->At(
        offset, bytes));
}

RecoveryOwnerState* RecoveryScanner::RecoveryState() noexcept {
    return static_cast<RecoveryOwnerState*>(
        AtMut(layout_.recovery_state_offset, sizeof(RecoveryOwnerState)));
}

const RecoveryOwnerState* RecoveryScanner::RecoveryState() const noexcept {
    return static_cast<const RecoveryOwnerState*>(
        At(layout_.recovery_state_offset, sizeof(RecoveryOwnerState)));
}

uint64_t RecoveryScanner::SelfPid() {
    return static_cast<uint64_t>(::getpid());
}

const RecoveryScanner::ClassDescriptor* RecoveryScanner::ClassAt(
    uint32_t index) const noexcept {
    if (index >= layout_.class_count) {
        return nullptr;
    }
    return static_cast<const ClassDescriptor*>(
        At(layout_.class_table_offset +
               static_cast<uint64_t>(index) * sizeof(ClassDescriptor),
           sizeof(ClassDescriptor)));
}

const RecoveryScanner::BitmapWord* RecoveryScanner::BitmapOf(
    const ClassDescriptor& cls) const noexcept {
    return static_cast<const BitmapWord*>(
        At(cls.bitmap_offset, sizeof(BitmapWord)));
}

RecoveryScanner::BitmapWord* RecoveryScanner::BitmapOfMut(
    const ClassDescriptor& cls) noexcept {
    return static_cast<BitmapWord*>(AtMut(cls.bitmap_offset,
                                          sizeof(BitmapWord)));
}

const RecoveryScanner::SlabHeaderPrefix* RecoveryScanner::SlotAt(
    const ClassDescriptor& cls, uint32_t slot) const noexcept {
    if (slot >= cls.slot_count) {
        return nullptr;
    }
    return static_cast<const SlabHeaderPrefix*>(
        At(cls.slots_offset + static_cast<uint64_t>(slot) * cls.slot_stride,
           sizeof(SlabHeaderPrefix)));
}

RecoveryScanner::SlabHeaderPrefix* RecoveryScanner::SlotAtMut(
    const ClassDescriptor& cls, uint32_t slot) noexcept {
    return const_cast<SlabHeaderPrefix*>(
        static_cast<const RecoveryScanner*>(this)->SlotAt(cls, slot));
}

bool RecoveryScanner::IsBitSet(const BitmapWord* words,
                               uint32_t index) noexcept {
    const uint64_t bits =
        words[index / 64].bits.load(std::memory_order_acquire);
    return (bits >> (index % 64) & 1ULL) != 0;
}

void RecoveryScanner::ClearBit(BitmapWord* words, uint32_t index) noexcept {
    // fetch_and with acq_rel so the reclaim of the slot (header state store
    // above) is ordered before the bit becomes visible as free.
    words[index / 64].bits.fetch_and(~(1ULL << (index % 64)),
                                     std::memory_order_acq_rel);
}

// CRC32C (Castagnoli, polynomial 0x1EDC6F41, reflected). Table-driven to
// keep this dependency-free; the region/allocator agents can swap in a
// hardware implementation behind the same function when they land.
uint32_t RecoveryScanner::ComputeImmutableCrc(
    const SlabHeaderPrefix& h) noexcept {
    static const uint32_t kTable[256] = {
#define MINO_CRC32C_ENTRY(n)                                               \
    [] {                                                                   \
        uint32_t c = n;                                                    \
        for (int k = 0; k < 8; ++k) {                                      \
            c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);             \
        }                                                                  \
        return c;                                                          \
    }()
#define MINO_CRC32C_4(n) \
    MINO_CRC32C_ENTRY(n), MINO_CRC32C_ENTRY(n + 1), MINO_CRC32C_ENTRY(n + 2), MINO_CRC32C_ENTRY(n + 3)
#define MINO_CRC32C_16(n) \
    MINO_CRC32C_4(n), MINO_CRC32C_4(n + 4), MINO_CRC32C_4(n + 8), MINO_CRC32C_4(n + 12)
#define MINO_CRC32C_64(n) \
    MINO_CRC32C_16(n), MINO_CRC32C_16(n + 16), MINO_CRC32C_16(n + 32), MINO_CRC32C_16(n + 48)
        MINO_CRC32C_64(0), MINO_CRC32C_64(64), MINO_CRC32C_64(128), MINO_CRC32C_64(192)
#undef MINO_CRC32C_64
#undef MINO_CRC32C_16
#undef MINO_CRC32C_4
#undef MINO_CRC32C_ENTRY
    };

    // Coverage per detailed design 8.1: Magic, Version, Class, Generation,
    // Capacity, Object Size, Type, Layout, Schema Short ID. We additionally
    // cover owner_epoch and allocation_transaction_id is explicitly excluded
    // (recovery-mutable); object_state and the CRC field itself are excluded.
    struct __attribute__((packed)) ImmutableView {
        uint32_t magic;
        uint16_t header_version;
        uint16_t class_id;
        uint32_t generation;
        uint32_t capacity;
        uint32_t object_size;
        uint32_t type_id;
        uint32_t layout_version;
        uint64_t schema_short_id;
    };
    ImmutableView view;
    view.magic = h.magic;
    view.header_version = h.header_version;
    view.class_id = h.class_id;
    view.generation = h.generation;
    view.capacity = h.capacity;
    view.object_size = h.object_size;
    view.type_id = h.type_id;
    view.layout_version = h.layout_version;
    view.schema_short_id = h.schema_short_id;

    const auto* bytes = reinterpret_cast<const unsigned char*>(&view);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < sizeof(view); ++i) {
        crc = kTable[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

}  // namespace mino::shm::recovery
