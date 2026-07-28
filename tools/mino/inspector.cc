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
#include <ostream>
#include <utility>

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
        default:
            return "INVALID";
    }
}

constexpr bool IsValidPublishedState(uint32_t value) {
    return value >= 1 && value <= 5;
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
    // //mino/shm/region:region (D1-03) is developed in parallel. When it
    // lands this becomes SharedMemoryRegion::Attach + layout derivation from
    // the SuperBlock. The CLI falls back to --image in the meantime.
    return Status::Error(
        StatusCode::kUnsupported,
        "attaching by region name requires //mino/shm/region:region "
        "(in development); use `mino inspect --image <file>` with a region "
        "image instead");
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
        const uint64_t word_count = (cls.slot_count + 63) / 64;
        if (inspector.At(cls.bitmap_offset, word_count * 64) == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "bitmap of class " + std::to_string(i) +
                                     " out of bounds");
        }
        if (cls.slot_count > 0 &&
            (cls.slot_stride < sizeof(SlabHeaderView) ||
             inspector.At(cls.slots_offset,
                          static_cast<uint64_t>(cls.slot_count - 1) *
                                  cls.slot_stride +
                              sizeof(SlabHeaderView)) == nullptr)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "slot array of class " + std::to_string(i) +
                                     " out of bounds");
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
            if (inspector.At(ring.slots_offset,
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

    for (uint32_t c = 0; c < layout_.classes.size(); ++c) {
        const ClassView& cls = layout_.classes[c];
        for (uint32_t s = 0; s < cls.slot_count; ++s) {
            ++report.total_slots;
            const bool occupied = IsBitSet(base_, cls.bitmap_offset, s);
            const SlabHeaderView* header = SlotAt(cls, s);
            if (header == nullptr) {
                return Status::Error(StatusCode::kInternal,
                                     "slot array moved during scan");
            }
            const uint32_t state =
                header->object_state.load(std::memory_order_acquire);

            if (!occupied) {
                if (state == 0) {
                    ++report.free_count;
                } else {
                    ++report.inconsistent_count;
                    report.findings.push_back(SlabFinding{
                        cls.class_id, s, SlabFinding::Kind::kInconsistent,
                        state, 0,
                        "bitmap free but object_state=" +
                            std::string(ObjectStateName(state))});
                }
                continue;
            }

            if (header->magic != kSlabMagic) {
                ++report.corrupt_count;
                report.findings.push_back(SlabFinding{
                    cls.class_id, s, SlabFinding::Kind::kCorrupt, state, 0,
                    "bad header magic"});
                continue;
            }

            if (!IsValidPublishedState(state)) {
                ++report.orphan_count;
                report.findings.push_back(SlabFinding{
                    cls.class_id, s, SlabFinding::Kind::kOrphan, state,
                    header->generation,
                    "bitmap occupied but object_state=" +
                        std::string(ObjectStateName(state)) +
                        " (generation never published, recoverable)"});
                continue;
            }

            if (state == 3 /*PUBLISHED*/ || state == 4 /*RETIRED*/) {
                const uint32_t crc = ComputeImmutableCrc(*header);
                if (header->immutable_header_crc != 0 &&
                    header->immutable_header_crc != crc) {
                    ++report.corrupt_count;
                    report.findings.push_back(SlabFinding{
                        cls.class_id, s, SlabFinding::Kind::kCorrupt, state,
                        header->generation, "immutable header CRC mismatch"});
                    continue;
                }
            }

            ++report.ok_count;
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
        return Status::Error(StatusCode::kNotFound,
                             "no ring buffer registered for channel " +
                                 std::to_string(channel_id));
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

    RingBufferDump dump;
    dump.channel_id = channel_id;
    dump.capacity = control->capacity;
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
    // Bitmap words are 64-byte aligned cache-line words; only the first 8
    // bytes carry bits.
    const auto* word = reinterpret_cast<const std::atomic<uint64_t>*>(
        base + bitmap_offset + static_cast<uint64_t>(index / 64) * 64);
    return (word->load(std::memory_order_acquire) >> (index % 64) & 1ULL) !=
           0;
}

uint32_t Inspector::ComputeImmutableCrc(const SlabHeaderView& h) noexcept {
    // CRC32C (Castagnoli, reflected) with the same coverage as the recovery
    // scanner: magic, version, class, generation, capacity, object_size,
    // type_id, layout_version, schema_short_id.
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
    ImmutableView view{h.magic,      h.header_version, h.class_id,
                       h.generation, h.capacity,       h.object_size,
                       h.type_id,    h.layout_version, h.schema_short_id};

    uint32_t table[256];
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
        }
        table[i] = c;
    }
    const auto* bytes = reinterpret_cast<const unsigned char*>(&view);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < sizeof(view); ++i) {
        crc = table[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

}  // namespace mino::tools
