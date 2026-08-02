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

#include "tools/mino/inspector.h"

#include <array>
#include <atomic>
#include <cstring>
#include <limits>
#include <memory>
#include <ostream>
#include <utility>

#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/region/region.h"

namespace mino::tools {

namespace {

// MpmcRing requires a trivially-copyable element type to validate an existing
// backing. Inspector only needs an opaque element with the exact IndexSlot ABI;
// the actual storage is interpreted through the authoritative IndexSlot type
// after MpmcRing::Attach has validated size, alignment, magic, and version.
struct alignas(alignof(Inspector::IndexSlotView)) InspectableRingElement {
    std::byte bytes[sizeof(Inspector::IndexSlotView)];
};
using InspectableRing = MpmcRing<InspectableRingElement>;

static_assert(sizeof(InspectableRingElement) ==
              sizeof(Inspector::IndexSlotView));
static_assert(alignof(InspectableRingElement) ==
              alignof(Inspector::IndexSlotView));
static_assert(InspectableRing::RequiredSize(
                  1, sizeof(InspectableRingElement),
                  alignof(InspectableRingElement)) ==
              sizeof(Inspector::RingControlView) +
                  sizeof(Inspector::RingSlotView));

Result<InspectableRing> AttachInspectableRing(const void* control) {
    // MpmcRing::Attach is logically read-only but its non-owning view supports
    // later mutation, so its API accepts void*. Inspector never calls a
    // mutating operation on the returned view.
    return InspectableRing::Attach(const_cast<void*>(control));
}

// MpmcRing payload bytes are published and observed through lock-free atomic
// byte accesses. Reading the complete IndexSlot representation this way avoids
// racing a producer's CommitEnqueue. The outer slot sequence is sampled before
// and after this function by the caller; only an unchanged era may be decoded.
IndexSlotSnapshot AtomicSnapshotIndexSlot(const unsigned char* storage,
                                          uint32_t* state) {
    static_assert(ATOMIC_CHAR_LOCK_FREE == 2);
    std::array<unsigned char, sizeof(Inspector::IndexSlotView)> bytes{};
    for (size_t i = 0; i < bytes.size(); ++i) {
        auto& source = const_cast<unsigned char&>(storage[i]);
        bytes[i] = std::atomic_ref<unsigned char>(source).load(
            std::memory_order_relaxed);
    }

    auto copy = [&bytes](size_t offset, void* destination, size_t size) {
        std::memcpy(destination, bytes.data() + offset, size);
    };
    IndexSlotSnapshot snapshot{};
    copy(offsetof(IndexSlot, msg_type), &snapshot.msg_type,
         sizeof(snapshot.msg_type));
    copy(offsetof(IndexSlot, schema_version), &snapshot.schema_version,
         sizeof(snapshot.schema_version));
    copy(offsetof(IndexSlot, schema_short_id), &snapshot.schema_short_id,
         sizeof(snapshot.schema_short_id));
    copy(offsetof(IndexSlot, schema_layout_version),
         &snapshot.schema_layout_version,
         sizeof(snapshot.schema_layout_version));
    copy(offsetof(IndexSlot, reserved0), &snapshot.reserved0,
         sizeof(snapshot.reserved0));
    copy(offsetof(IndexSlot, sequence_num), &snapshot.sequence_num,
         sizeof(snapshot.sequence_num));
    copy(offsetof(IndexSlot, timestamp_ns), &snapshot.timestamp_ns,
         sizeof(snapshot.timestamp_ns));
    copy(offsetof(IndexSlot, payload), &snapshot.payload,
         sizeof(snapshot.payload));
    copy(offsetof(IndexSlot, payload_len), &snapshot.payload_len,
         sizeof(snapshot.payload_len));
    copy(offsetof(IndexSlot, immutable_metadata_crc),
         &snapshot.immutable_metadata_crc,
         sizeof(snapshot.immutable_metadata_crc));
    copy(offsetof(IndexSlot, flags), &snapshot.flags,
         sizeof(snapshot.flags));
    copy(offsetof(IndexSlot, state), state, sizeof(*state));
    return snapshot;
}

constexpr std::string_view ObjectStateName(uint32_t value) {
    // Keep in sync with recovery::ObjectStateName (merged when the region
    // and allocator headers land).
    switch (value) {
        case 0:
            return "FREE";
        case 1:
            return "ALLOCATED";
        case 2:
            return "BUILDING";
        case 3:
            return "PUBLISHED";
        case 4:
            return "RETIRED";
        case 5:
            return "ABORTING";
        case 6:
            return "RECLAIMING";
        case 7:
            return "ALLOCATING";
        default:
            return "INVALID";
    }
}

constexpr bool IsValidPublishedState(uint32_t value) {
    return value >= static_cast<uint32_t>(ObjectState::kAllocated) &&
           value <= static_cast<uint32_t>(ObjectState::kAborting);
}

constexpr bool IsProtocolReclaimableState(uint32_t value) {
    return value == static_cast<uint32_t>(ObjectState::kFree) ||
           value == static_cast<uint32_t>(ObjectState::kReclaiming) ||
           value == static_cast<uint32_t>(ObjectState::kAllocating);
}

}  // namespace

Status UpgradeRegionV4ToV5Offline(
    const RegionV4UpgradeOptions& options) {
    return SharedMemoryRegion::UpgradeV4ToV5Offline(options);
}

Status CopyUpgradeRegionV4ToV5Offline(
    const RegionV4CopyUpgradeOptions& options) {
    auto destination =
        SharedMemoryRegion::CopyUpgradeV4ToV5Offline(options);
    if (!destination.ok()) {
        return destination.status();
    }
    return destination->Detach();
}

std::string SlotStateName(uint32_t state) {
    // Ring index-slot states from the authoritative SlotState ABI.
    switch (state) {
        case 0:
            return "FREE";
        case 1:
            return "RESERVED";
        case 2:
            return "WRITING";
        case 3:
            return "READY";
        case 4:
            return "RETIRED";
        case 5:
            return "ABORTED";
        case 6:
            return "RETIRING";
        default:
            return "INVALID(" + std::to_string(state) + ")";
    }
}

// ---------------------------------------------------------------------------
// Attach
// ---------------------------------------------------------------------------

Result<Inspector> Inspector::Attach(const std::string& region_name) {
    if (region_name.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "region name must not be empty");
    }

    RegionAttachOptions options;
    options.name = region_name;
    options.read_only = true;
    options.allow_quarantined_read_only = true;
    MINO_ASSIGN_OR_RETURN(SharedMemoryRegion attached,
                          SharedMemoryRegion::Attach(options));
    auto region = std::make_shared<SharedMemoryRegion>(std::move(attached));

    const SuperBlock& sb = *region->superblock();
    const uint64_t allocator_available = region->size() - sb.allocator_offset;
    const void* allocator_base = region->base() + sb.allocator_offset;
    MINO_ASSIGN_OR_RETURN(
        const bool metadata_present,
        CentralSlabAllocator::HasAllocatorMetadata(allocator_base,
                                                   allocator_available));
    if (!metadata_present) {
        return Status::Error(StatusCode::kNotFound,
                             "Region allocator metadata is not initialized");
    }

    RegionAllocatorStorage storage{
        .region_base = region->base(),
        .region_size = region->size(),
        .allocator_offset = sb.allocator_offset,
        .allocator_size = sb.data_offset - sb.allocator_offset,
        .data_offset = sb.data_offset,
        .data_size = sb.data_size,
        .region_id = sb.region_id,
    };
    MINO_ASSIGN_OR_RETURN(CentralSlabAllocator attached_allocator,
                          CentralSlabAllocator::AttachInRegion(storage));
    auto allocator = std::make_shared<CentralSlabAllocator>(
        std::move(attached_allocator));

    Layout layout;
    layout.classes.resize(allocator->class_count());
    for (uint32_t class_id = 0; class_id < allocator->class_count();
         ++class_id) {
        layout.classes[class_id] = ClassView{
            .class_id = class_id,
            .slot_count = 0,
            .bitmap_offset = 0,
            .slots_offset = 0,
            .slot_stride = 0,
            .reserved = 0,
        };
    }
    // The allocator's persisted class table is authoritative. Padding slots
    // between 64-bit bitmap shards have class_count() as their sentinel class
    // id and are intentionally excluded from the Inspector's logical layout.
    for (uint32_t slot = 0; slot < allocator->total_slot_count(); ++slot) {
        const uint16_t class_id = allocator->ClassIdForRecovery(slot);
        if (class_id < layout.classes.size()) {
            ++layout.classes[class_id].slot_count;
        }
    }
    auto channels = region->channel_directory();
    if (!channels.ok()) {
        // v2-v4 Regions remain readable but predate persistent ring discovery.
        if (channels.status().code() != StatusCode::kUnsupported) {
            return channels.status();
        }
    } else {
        for (uint32_t i = 0; i < channels->entry_count; ++i) {
            const ChannelRingDescriptor& entry = channels->entries[i];
            if (entry.state !=
                static_cast<uint32_t>(ChannelRingState::kActive)) {
                continue;
            }
            layout.rings.push_back(Layout::RingRef{
                .channel_id = entry.channel_id,
                .channel_type = entry.channel_type,
                .control_offset = entry.control_offset,
                .extent_size = entry.extent_size,
                .capacity = entry.capacity,
                .generation = entry.generation,
                .state = entry.state,
                .reserved = 0,
            });
        }
    }

    Inspector inspector(region->base(), region->size(), std::move(layout),
                        region_name);
    inspector.region_ = std::move(region);
    inspector.allocator_ = std::move(allocator);
    return inspector;
}

Result<Inspector> Inspector::AttachMemory(const std::byte* base, uint64_t size,
                                          Layout layout,
                                          std::string region_name) {
    if (base == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument, "region base is null");
    }
    Inspector inspector(base, size, std::move(layout), std::move(region_name));

    for (uint32_t i = 0; i < inspector.layout_.classes.size(); ++i) {
        const ClassView& cls = inspector.layout_.classes[i];
        const uint64_t word_count =
            (static_cast<uint64_t>(cls.slot_count) + 63) / 64;
        const uint64_t bitmap_extent =
            word_count == 0 ? 0 : (word_count - 1) * 64 +
                                      sizeof(std::atomic<uint64_t>);
        if (inspector.At(cls.bitmap_offset, bitmap_extent) == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "bitmap of class " + std::to_string(i) +
                                     " out of bounds");
        }
        if (cls.slot_count > 0) {
            const uint64_t slots_before_last =
                static_cast<uint64_t>(cls.slot_count - 1) * cls.slot_stride;
            if (cls.slot_stride < sizeof(SlabHeaderView) ||
                slots_before_last >
                    std::numeric_limits<uint64_t>::max() -
                        sizeof(SlabHeaderView) ||
                inspector.At(cls.slots_offset,
                             slots_before_last + sizeof(SlabHeaderView)) ==
                    nullptr) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "slot array of class " +
                                         std::to_string(i) + " out of bounds");
            }
        }
    }
    for (size_t ring_index = 0; ring_index < inspector.layout_.rings.size();
         ++ring_index) {
        const auto& ring = inspector.layout_.rings[ring_index];
        for (size_t prior = 0; prior < ring_index; ++prior) {
            if (inspector.layout_.rings[prior].channel_id == ring.channel_id) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "duplicate ring channel id in layout");
            }
        }
        const void* control_address =
            inspector.At(ring.control_offset, sizeof(RingControlView));
        if (control_address == nullptr) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "ring control block of channel " +
                    std::to_string(ring.channel_id) + " out of bounds");
        }
        auto attached = AttachInspectableRing(control_address);
        if (!attached.ok()) {
            // Preserve offline diagnostics for corrupt/unsupported control
            // blocks; DumpRingBuffer reports the authoritative Attach error.
            // Misalignment, however, would make any typed control access UB.
            if (attached.status().code() == StatusCode::kInvalidArgument) {
                return attached.status();
            }
            continue;
        }

        const auto* control = static_cast<const RingControlView*>(control_address);
        const bool persisted_metadata = ring.channel_type != 0 ||
                                        ring.extent_size != 0 ||
                                        ring.capacity != 0 ||
                                        ring.generation != 0 || ring.state != 0;
        if (persisted_metadata &&
            (ring.channel_type !=
                 static_cast<uint32_t>(ChannelRingType::kMpmcRing) ||
             ring.state != static_cast<uint32_t>(ChannelRingState::kActive) ||
             ring.generation == 0 || ring.capacity != control->capacity ||
             ring.generation !=
                 control->generation.load(std::memory_order_acquire) ||
             control->active_state.load(std::memory_order_acquire) !=
                 static_cast<uint32_t>(MpmcRingState::kActive) ||
             ring.extent_size != MpmcRingRequiredSize(
                                     control->capacity, control->elem_size,
                                     control->elem_align))) {
            return Status::Error(StatusCode::kCorruption,
                                 "persisted ring descriptor does not match control ABI");
        }
        const uint64_t slots_offset =
            ring.control_offset + sizeof(RingControlView);
        if (control->capacity >
                std::numeric_limits<uint64_t>::max() / kRingSlotStride ||
            inspector.At(slots_offset,
                         control->capacity * kRingSlotStride) == nullptr) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "ring slots of channel " + std::to_string(ring.channel_id) +
                    " out of bounds");
        }
    }
    return inspector;
}

// ---------------------------------------------------------------------------
// ScanSlabs
// ---------------------------------------------------------------------------

Result<Inspector::SlabConsistencyReport> Inspector::ScanSlabs() const {
    SlabConsistencyReport report;

    auto classify = [&](uint32_t class_id, uint32_t slot_index,
                        bool occupied, const SlabHeaderView& header,
                        uint32_t authoritative_generation,
                        bool check_generation) {
        ++report.total_slots;
        const uint32_t state =
            header.object_state.load(std::memory_order_acquire);
        const uint32_t generation =
            header.generation.load(std::memory_order_acquire);

        if (!occupied) {
            if (state == static_cast<uint32_t>(ObjectState::kFree)) {
                ++report.free_count;
            } else {
                ++report.inconsistent_count;
                report.findings.push_back(SlabFinding{
                    class_id, slot_index,
                    SlabFinding::Kind::kInconsistent, state, generation,
                    "bitmap free but object_state=" +
                        std::string(ObjectStateName(state))});
            }
            return;
        }

        if (header.magic != kSlabMagic) {
            ++report.corrupt_count;
            report.findings.push_back(SlabFinding{
                class_id, slot_index, SlabFinding::Kind::kCorrupt, state,
                generation, "bad header magic"});
            return;
        }

        if (!IsValidPublishedState(state)) {
            if (!IsProtocolReclaimableState(state)) {
                ++report.corrupt_count;
                report.findings.push_back(SlabFinding{
                    class_id, slot_index, SlabFinding::Kind::kCorrupt, state,
                    generation, "unknown object_state"});
            } else {
                ++report.orphan_count;
                report.findings.push_back(SlabFinding{
                    class_id, slot_index, SlabFinding::Kind::kOrphan, state,
                    generation,
                    "bitmap occupied but object_state=" +
                        std::string(ObjectStateName(state)) +
                        " (allocation was not durably published, "
                        "recoverable)"});
            }
            return;
        }

        const bool valid =
            header.header_version == kSlabHeaderVersion &&
            header.class_id == class_id &&
            header.object_size <= header.capacity &&
            header.immutable_header_crc == ComputeImmutableCrc(header) &&
            (!check_generation || generation == authoritative_generation);
        if (!valid) {
            ++report.corrupt_count;
            report.findings.push_back(SlabFinding{
                class_id, slot_index, SlabFinding::Kind::kCorrupt, state,
                generation,
                "allocator SlabHeader/generation invariant failed"});
            return;
        }

        ++report.ok_count;
    };

    if (allocator_ != nullptr) {
        std::vector<uint32_t> next_local_slot(allocator_->class_count(), 0);
        for (uint32_t global_slot = 0;
             global_slot < allocator_->total_slot_count(); ++global_slot) {
            const uint16_t class_id =
                allocator_->ClassIdForRecovery(global_slot);
            if (class_id >= allocator_->class_count()) {
                continue;  // Bitmap shard padding, not a configured slot.
            }
            SlabHeaderView header{};
            if (!allocator_->ReadSlotByIndex(global_slot, &header, nullptr)) {
                return Status::Error(StatusCode::kInternal,
                                     "allocator slot disappeared during scan");
            }
            classify(
                class_id, next_local_slot[class_id]++,
                allocator_->IsSlotOccupiedForRecovery(global_slot), header,
                allocator_->AuthoritativeGenerationForRecovery(global_slot),
                /*check_generation=*/true);
        }
        return report;
    }

    for (const ClassView& cls : layout_.classes) {
        for (uint32_t slot = 0; slot < cls.slot_count; ++slot) {
            const SlabHeaderView* header = SlotAt(cls, slot);
            if (header == nullptr) {
                return Status::Error(StatusCode::kInternal,
                                     "slot array moved during scan");
            }
            classify(cls.class_id, slot,
                     IsBitSet(base_, cls.bitmap_offset, slot), *header,
                     /*authoritative_generation=*/0,
                     /*check_generation=*/false);
        }
    }
    return report;
}

// ---------------------------------------------------------------------------
// DumpRingBuffer
// ---------------------------------------------------------------------------

Result<Inspector::RingBufferDump> Inspector::DumpRingBuffer(
    uint32_t channel_id) const {
    const Layout::RingRef* ref = nullptr;
    for (const auto& ring : layout_.rings) {
        if (ring.channel_id == channel_id) {
            ref = &ring;
            break;
        }
    }
    if (ref == nullptr) {
        return Status::Error(
            StatusCode::kNotFound,
            "no active ring registered for channel " +
                std::to_string(channel_id));
    }

    const void* control_address =
        At(ref->control_offset, sizeof(RingControlView));
    if (control_address == nullptr) {
        return Status::Error(StatusCode::kCorruption,
                             "ring control block out of bounds");
    }
    auto attached = AttachInspectableRing(control_address);
    if (!attached.ok()) {
        return attached.status();
    }

    const auto* control = static_cast<const RingControlView*>(control_address);
    const uint64_t capacity = control->capacity;
    if (ref->generation != 0 &&
        control->generation.load(std::memory_order_acquire) !=
            ref->generation) {
        return Status::Error(StatusCode::kUnavailable,
                             "ring generation changed after Inspector attach");
    }
    const bool persisted_metadata = ref->channel_type != 0 ||
                                    ref->extent_size != 0 ||
                                    ref->capacity != 0 ||
                                    ref->generation != 0 || ref->state != 0;
    if (persisted_metadata &&
        (ref->channel_type !=
             static_cast<uint32_t>(ChannelRingType::kMpmcRing) ||
         ref->state != static_cast<uint32_t>(ChannelRingState::kActive) ||
         ref->generation == 0 || ref->capacity != capacity ||
         control->active_state.load(std::memory_order_acquire) !=
             static_cast<uint32_t>(MpmcRingState::kActive) ||
         ref->extent_size != MpmcRingRequiredSize(
                                 capacity, control->elem_size,
                                 control->elem_align))) {
        return Status::Error(StatusCode::kCorruption,
                             "ring descriptor no longer matches control ABI");
    }
    const uint64_t slots_offset =
        ref->control_offset + sizeof(RingControlView);
    if (capacity >
            std::numeric_limits<uint64_t>::max() / kRingSlotStride ||
        At(slots_offset, capacity * kRingSlotStride) == nullptr) {
        return Status::Error(StatusCode::kCorruption,
                             "ring slot array is out of bounds");
    }

    RingBufferDump dump;
    dump.channel_id = channel_id;
    dump.channel_type = ref->channel_type;
    dump.generation = ref->generation;
    dump.capacity = capacity;
    dump.enqueue_pos = control->enqueue_pos.load(std::memory_order_acquire);
    dump.dequeue_pos = control->dequeue_pos.load(std::memory_order_acquire);
    dump.pending = dump.enqueue_pos - dump.dequeue_pos;
    dump.elem_size = control->elem_size;
    dump.elem_align = control->elem_align;
    dump.layout_version =
        control->layout_version.load(std::memory_order_relaxed);

    const std::byte* slots_base =
        static_cast<const std::byte*>(At(slots_offset, 0));
    dump.slots.reserve(dump.capacity);
    for (uint64_t i = 0; i < dump.capacity; ++i) {
        const auto* ring_slot = reinterpret_cast<const RingSlotView*>(
            slots_base + i * kRingSlotStride);
        const uint64_t before =
            ring_slot->sequence.load(std::memory_order_acquire);
        uint32_t state = 0;
        const IndexSlotSnapshot snapshot =
            AtomicSnapshotIndexSlot(ring_slot->storage, &state);
        const uint64_t after =
            ring_slot->sequence.load(std::memory_order_acquire);

        RingSlotSummary summary;
        summary.ring_sequence = after;
        if (before != after) {
            summary.unstable = true;
            dump.slots.push_back(summary);
            continue;
        }
        summary.sequence = snapshot.sequence_num;
        summary.state = state;
        summary.msg_type = snapshot.msg_type;
        summary.timestamp_ns = snapshot.timestamp_ns;
        summary.payload_offset = snapshot.payload.offset;
        summary.payload_len = snapshot.payload_len;
        summary.payload_generation = snapshot.payload.generation;
        summary.crc_checked =
            state == static_cast<uint32_t>(SlotState::kReady);
        summary.crc_valid =
            summary.crc_checked && VerifySnapshotCrc(snapshot);
        dump.slots.push_back(summary);
    }
    MINO_RETURN_IF_ERROR(attached->ValidateFence());
    return dump;
}

// ---------------------------------------------------------------------------
// PrintReport
// ---------------------------------------------------------------------------

Status Inspector::PrintReport(std::ostream& out) const {
    out << "=== Mino Inspector Report ===\n";
    out << "region: " << region_name_ << "  mapped_size: " << size_
        << " bytes\n\n";

    MINO_ASSIGN_OR_RETURN(const auto slabs, ScanSlabs());
    out << "--- Slab Consistency ---\n";
    out << "total_slots:        " << slabs.total_slots << "\n";
    out << "ok:                 " << slabs.ok_count << "\n";
    out << "free:               " << slabs.free_count << "\n";
    out << "orphan:             " << slabs.orphan_count << "\n";
    out << "inconsistent:       " << slabs.inconsistent_count << "\n";
    out << "corrupt:            " << slabs.corrupt_count << "\n";
    if (!slabs.findings.empty()) {
        out << "\nfindings:\n";
        for (const auto& f : slabs.findings) {
            const char* kind = f.kind == SlabFinding::Kind::kOrphan
                                   ? "ORPHAN"
                                   : f.kind == SlabFinding::Kind::kInconsistent
                                         ? "INCONSISTENT"
                                         : "CORRUPT";
            out << "  [" << kind << "] class=" << f.class_id
                << " slot=" << f.slot_index
                << " state=" << ObjectStateName(f.object_state)
                << " gen=" << f.generation << "  " << f.note << "\n";
        }
    }

    for (const auto& ring : layout_.rings) {
        auto dump = DumpRingBuffer(ring.channel_id);
        out << "\n--- RingBuffer channel " << ring.channel_id << " ---\n";
        if (!dump.ok()) {
            out << "dump failed: " << dump.status().ToString() << "\n";
            continue;
        }
        out << "type/generation:    " << dump->channel_type << " / "
            << dump->generation << "\n";
        out << "capacity:           " << dump->capacity << "\n";
        out << "enqueue_pos:        " << dump->enqueue_pos << "\n";
        out << "dequeue_pos:        " << dump->dequeue_pos << "\n";
        out << "pending:            " << dump->pending << "\n";
        out << "elem_size/align:    " << dump->elem_size << " / "
            << dump->elem_align << "\n";
        out << "layout_version:     " << dump->layout_version << "\n";
        out << "slots (physical order):\n";
        for (uint64_t i = 0; i < dump->slots.size(); ++i) {
            const auto& s = dump->slots[i];
            out << "  [" << i << "] ring_seq=" << s.ring_sequence;
            if (s.unstable) {
                out << " UNSTABLE\n";
                continue;
            }
            out << " seq=" << s.sequence
                << " state=" << SlotStateName(s.state)
                << " msg_type=" << s.msg_type << " payload_off=0x" << std::hex
                << s.payload_offset << std::dec << " len=" << s.payload_len
                << " gen=" << s.payload_generation;
            if (s.crc_checked) {
                out << " crc=" << (s.crc_valid ? "OK" : "BAD");
            }
            out << "\n";
        }
    }
    return Status::Ok();
}

// ---------------------------------------------------------------------------
// Layout helpers
// ---------------------------------------------------------------------------

const void* Inspector::At(uint64_t offset, uint64_t bytes) const noexcept {
    if (offset > size_ || bytes > size_ - offset) {
        return nullptr;
    }
    return base_ + offset;
}

const Inspector::SlabHeaderView* Inspector::SlotAt(
    const ClassView& cls, uint32_t slot) const noexcept {
    if (slot >= cls.slot_count) {
        return nullptr;
    }
    return static_cast<const SlabHeaderView*>(
        At(cls.slots_offset + static_cast<uint64_t>(slot) * cls.slot_stride,
           sizeof(SlabHeaderView)));
}

bool Inspector::IsBitSet(const std::byte* base, uint64_t bitmap_offset,
                         uint32_t index) noexcept {
    // Legacy offline sidecars place one bitmap word on each cache line. Live
    // Region attachments use the allocator facade above and do not enter here.
    const auto* word = reinterpret_cast<const std::atomic<uint64_t>*>(
        base + bitmap_offset + static_cast<uint64_t>(index / 64) * 64);
    return (word->load(std::memory_order_acquire) >> (index % 64) & 1ULL) !=
           0;
}

uint32_t Inspector::ComputeImmutableCrc(const SlabHeaderView& h) noexcept {
    return ComputeImmutableHeaderCrc(h);
}

}  // namespace mino::tools
