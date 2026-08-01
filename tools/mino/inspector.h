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

#ifndef TOOLS_MINO_INSPECTOR_H_
#define TOOLS_MINO_INSPECTOR_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino {
class CentralSlabAllocator;
class SharedMemoryRegion;
}

namespace mino::tools {

// ---------------------------------------------------------------------------
// Inspector (D1-11): offline diagnostic tool for Mino shared-memory regions.
//
// Implements architecture doc 15.6:
//   - Slab 一致性扫描 (SlabConsistencyReport)
//   - RingBuffer Dump (RingBufferDump)
//   - 诊断报告输出 (PrintReport)
//
// The Inspector is strictly read-only with respect to allocator/channel
// payload state. Attach(name) owns a read-only SharedMemoryRegion mapping and
// derives slab offsets from the validated, persisted allocator metadata.
// AttachMemory(...) remains available for offline images and explicit ring
// layouts. Ring registration is not yet present in the Region Directory, so a
// name-only attach intentionally leaves Layout::rings empty.
// ---------------------------------------------------------------------------
class Inspector {
public:
    // The diagnostic scanner uses the allocator's authoritative shared-memory
    // ABI rather than maintaining a parallel tools-only header definition.
    using SlabHeaderView = ::mino::SlabHeader;
    static constexpr uint32_t kSlabMagic = ::mino::kSlabHeaderMagic;

    struct ClassView {
        uint32_t class_id;
        uint32_t slot_count;
        uint64_t bitmap_offset;
        uint64_t slots_offset;
        uint32_t slot_stride;
        uint32_t reserved;
    };

    // MPMC skeleton control block (detailed design 9.9). Producer and
    // consumer cursors live on separate cache lines.
    struct alignas(64) RingControlView {
        static constexpr uint32_t kMagic = 0x52494E47;  // "RING".

        uint32_t magic;
        uint32_t layout_version;
        uint32_t elem_size;
        uint32_t elem_align;
        uint64_t capacity;  // Power of two.
        uint64_t reserved0[3];

        alignas(64) std::atomic<uint64_t> enqueue_pos;
        uint64_t reserved1[7];

        alignas(64) std::atomic<uint64_t> dequeue_pos;
        uint64_t reserved2[7];
    };
    static_assert(sizeof(RingControlView) == 192,
                  "RingControlView must be three cache lines");

    // Index slot view (detailed design 9.2). Only the fields the Inspector
    // reports on are interpreted; the slot is 128 bytes on the wire.
    struct IndexSlotView {
        uint32_t msg_type;
        uint32_t schema_version;
        uint64_t schema_short_id;
        uint32_t schema_layout_version;
        uint32_t reserved;
        uint64_t sequence_num;
        uint64_t timestamp_ns;
        uint64_t payload_offset;
        uint32_t payload_generation;
        uint32_t payload_region_id;
        uint32_t payload_len;
        uint32_t immutable_metadata_crc;
        std::atomic<uint32_t> state;
        uint32_t flags;
    };
    static constexpr uint32_t kIndexSlotSize = 128;

    struct Layout {
        // Class descriptors are host-side snapshots parsed from a sidecar or
        // derived from validated SuperBlock/allocator metadata. For live name
        // attachments, class_id/slot_count are populated while physical access
        // remains delegated to the validated allocator facade; offset fields
        // are therefore zero and must not be reused as an offline sidecar.
        std::vector<ClassView> classes;
        // Ring buffers: channel_id -> (control block, slot array) offsets.
        struct RingRef {
            uint32_t channel_id;
            uint32_t reserved;
            uint64_t control_offset;
            uint64_t slots_offset;  // IndexSlotView[kIndexSlotSize] array.
        };
        std::vector<RingRef> rings;
    };

    // One inconsistent/orphan/corrupt slot finding.
    struct SlabFinding {
        uint32_t class_id = 0;
        uint32_t slot_index = 0;
        enum class Kind : uint32_t { kOk, kOrphan, kInconsistent, kCorrupt };
        Kind kind = Kind::kOk;
        uint32_t object_state = 0;
        uint32_t generation = 0;
        std::string note;
    };

    struct SlabConsistencyReport {
        uint64_t total_slots = 0;
        uint64_t ok_count = 0;         // Bitmap and header state agree.
        uint64_t free_count = 0;       // Bitmap free, no stale state.
        uint64_t orphan_count = 0;     // Bitmap occupied, state not published.
        uint64_t inconsistent_count = 0;  // Bitmap free but state != FREE.
        uint64_t corrupt_count = 0;    // Bad magic / CRC mismatch.
        std::vector<SlabFinding> findings;  // Non-ok entries only.
    };

    struct RingSlotSummary {
        uint64_t sequence = 0;
        uint32_t state = 0;
        uint32_t msg_type = 0;
        uint64_t timestamp_ns = 0;
        uint64_t payload_offset = 0;
        uint32_t payload_len = 0;
        uint32_t payload_generation = 0;
    };

    struct RingBufferDump {
        uint32_t channel_id = 0;
        uint64_t capacity = 0;
        uint64_t enqueue_pos = 0;
        uint64_t dequeue_pos = 0;
        uint64_t pending = 0;          // enqueue_pos - dequeue_pos.
        uint32_t elem_size = 0;
        uint32_t elem_align = 0;
        uint32_t layout_version = 0;
        std::vector<RingSlotSummary> slots;  // capacity entries, physical order.
    };

    // Opens an existing Region by name in read-only mode, validates its
    // SuperBlock and allocator metadata, and derives the slab layout. Ring
    // buffers are not auto-discovered until the Region Directory persists ring
    // registrations; use AttachMemory with explicit RingRef entries for those.
    static Result<Inspector> Attach(const std::string& region_name);

    // Attaches to a caller-provided region image. The Inspector never writes
    // through this mapping (all views are const).
    static Result<Inspector> AttachMemory(const std::byte* base, uint64_t size,
                                          Layout layout,
                                          std::string region_name = "<memory>");

    // Scans every class: validates header magic, immutable CRC (for frozen
    // states), bitmap/state consistency, and classifies each slot. Read-only.
    Result<SlabConsistencyReport> ScanSlabs() const;

    // Dumps one ring buffer: control block plus per-slot summaries in
    // physical order. Returns kNotFound for unknown channel ids and
    // kCorruption for a bad control block.
    Result<RingBufferDump> DumpRingBuffer(uint32_t channel_id) const;

    // Writes a human-readable diagnostic report. Runs ScanSlabs() and dumps
    // every ring listed in the layout.
    Status PrintReport(std::ostream& out) const;

    const std::string& region_name() const noexcept { return region_name_; }
    const Layout& layout() const noexcept { return layout_; }

private:
    Inspector(const std::byte* base, uint64_t size, Layout layout,
              std::string region_name)
        : base_(base),
          size_(size),
          layout_(std::move(layout)),
          region_name_(std::move(region_name)) {}

    const void* At(uint64_t offset, uint64_t bytes) const noexcept;
    const SlabHeaderView* SlotAt(const ClassView& cls,
                                 uint32_t slot) const noexcept;
    static bool IsBitSet(const std::byte* base, uint64_t bitmap_offset,
                         uint32_t index) noexcept;
    static uint32_t ComputeImmutableCrc(const SlabHeaderView& h) noexcept;

    const std::byte* base_;
    uint64_t size_;
    Layout layout_;
    std::string region_name_;
    // Non-null only for Attach(name); keeps the read-only mapping alive for
    // every pointer-based view held by this Inspector.
    std::shared_ptr<::mino::SharedMemoryRegion> region_;
    // Validated allocator facade used only through its recovery-facing const
    // observation methods for live Region attachments.
    std::shared_ptr<const ::mino::CentralSlabAllocator> allocator_;
};

// Free helpers shared by PrintReport and the CLI.
std::string SlotStateName(uint32_t state);

}  // namespace mino::tools

#endif  // TOOLS_MINO_INSPECTOR_H_
