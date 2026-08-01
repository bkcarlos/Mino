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

#include <atomic>
#include <limits>
#include <memory>
#include <ostream>
#include <utility>

#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/region/region.h"

namespace mino::tools {

namespace {

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

std::string SlotStateName(uint32_t state) {
    // Ring index-slot states (detailed design 9.5): FREE, RESERVED, WRITING,
    // READY, ABORTED, RETIRED.
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
            return "ABORTED";
        case 5:
            return "RETIRED";
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
    // The current Region Directory reserves bytes but does not persist channel
    // ring registrations. Do not guess offsets by scanning for magic: callers
    // that need ring dumps must continue to provide explicit RingRef metadata.

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
    for (const auto& ring : inspector.layout_.rings) {
        if (inspector.At(ring.control_offset, sizeof(RingControlView)) ==
            nullptr) {
            return Status::Error(
                StatusCode::kInvalidArgument,
                "ring control block of channel " +
                    std::to_string(ring.channel_id) + " out of bounds");
        }
        const auto* control = static_cast<const RingControlView*>(
            inspector.At(ring.control_offset, sizeof(RingControlView)));
        if (control->magic == RingControlView::kMagic) {
            if (control->capacity >
                    std::numeric_limits<uint64_t>::max() / kIndexSlotSize ||
                inspector.At(ring.slots_offset,
                             control->capacity * kIndexSlotSize) == nullptr) {
                return Status::Error(
                    StatusCode::kInvalidArgument,
                    "ring slots of channel " +
                        std::to_string(ring.channel_id) + " out of bounds");
            }
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
            "no ring buffer registered for channel " +
                std::to_string(channel_id) +
                "; Region metadata does not currently persist ring locations, "
                "so provide an explicit RingRef/layout sidecar");
    }

    const auto* control = static_cast<const RingControlView*>(
        At(ref->control_offset, sizeof(RingControlView)));
    if (control == nullptr) {
        return Status::Error(StatusCode::kCorruption,
                             "ring control block out of bounds");
    }
    if (control->magic != RingControlView::kMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "ring control block has bad magic (channel " +
                                 std::to_string(channel_id) + ")");
    }
    const uint64_t capacity = control->capacity;
    if (capacity > std::numeric_limits<uint64_t>::max() / kIndexSlotSize ||
        At(ref->slots_offset, capacity * kIndexSlotSize) == nullptr) {
        return Status::Error(StatusCode::kCorruption,
                             "ring slot array is out of bounds");
    }

    RingBufferDump dump;
    dump.channel_id = channel_id;
    dump.capacity = capacity;
    dump.enqueue_pos = control->enqueue_pos.load(std::memory_order_acquire);
    dump.dequeue_pos = control->dequeue_pos.load(std::memory_order_acquire);
    dump.pending = dump.enqueue_pos - dump.dequeue_pos;
    dump.elem_size = control->elem_size;
    dump.elem_align = control->elem_align;
    dump.layout_version = control->layout_version;

    const std::byte* slots_base =
        static_cast<const std::byte*>(At(ref->slots_offset, 0));
    dump.slots.reserve(dump.capacity);
    for (uint64_t i = 0; i < dump.capacity; ++i) {
        const auto* slot = reinterpret_cast<const IndexSlotView*>(
            slots_base + i * kIndexSlotSize);
        dump.slots.push_back(RingSlotSummary{
            slot->sequence_num,
            slot->state.load(std::memory_order_acquire),
            slot->msg_type,
            slot->timestamp_ns,
            slot->payload_offset,
            slot->payload_len,
            slot->payload_generation,
        });
    }
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
            out << "  [" << i << "] seq=" << s.sequence
                << " state=" << SlotStateName(s.state)
                << " msg_type=" << s.msg_type << " payload_off=0x" << std::hex
                << s.payload_offset << std::dec << " len=" << s.payload_len
                << " gen=" << s.payload_generation << "\n";
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
