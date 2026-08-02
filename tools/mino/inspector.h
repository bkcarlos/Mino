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
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/mpmc_ring.h"

namespace mino {
class CentralSlabAllocator;
class SharedMemoryRegion;
struct RegionV4CopyUpgradeOptions;
struct RegionV4UpgradeOptions;
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
// layouts. Region layout v5 persists a CRC-validated Channel Directory, so a
// name-only attach discovers active rings without a sidecar.
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

    // Ring diagnostics consume the authoritative channel ABI directly. A real
    // MpmcRing slot contains its own Vyukov sequence before the aligned element
    // storage; it is not an array of bare IndexSlot records.
    using RingControlView = ::mino::MpmcRingControlBlock;
    using IndexSlotView = ::mino::IndexSlot;
    using RingSlotView =
        ::mino::MpmcRingSlot<sizeof(IndexSlotView), alignof(IndexSlotView)>;
    static constexpr uint32_t kRingSlotStride = sizeof(RingSlotView);

    struct Layout {
        // Class descriptors are host-side snapshots parsed from a sidecar or
        // derived from validated SuperBlock/allocator metadata. For live name
        // attachments, class_id/slot_count are populated while physical access
        // remains delegated to the validated allocator facade; offset fields
        // are therefore zero and must not be reused as an offline sidecar.
        std::vector<ClassView> classes;
        // Ring buffers: channel_id -> MpmcRing backing offset. The slot array
        // immediately follows the control block, as required by MpmcRing ABI.
        struct RingRef {
            uint32_t channel_id = 0;
            uint32_t channel_type = 0;
            uint64_t control_offset = 0;
            uint64_t extent_size = 0;
            uint64_t capacity = 0;
            uint64_t generation = 0;
            uint32_t state = 0;
            uint32_t reserved = 0;
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
        uint64_t ring_sequence = 0;  // MpmcRing slot ownership sequence.
        bool unstable = false;       // Outer sequence changed during snapshot.
        bool crc_checked = false;    // READY immutable metadata was checked.
        bool crc_valid = false;
        uint8_t reserved0 = 0;
        uint32_t reserved1 = 0;
        uint64_t sequence = 0;       // IndexSlot message sequence.
        uint32_t state = 0;
        uint32_t msg_type = 0;
        uint64_t timestamp_ns = 0;
        uint64_t payload_offset = 0;
        uint32_t payload_len = 0;
        uint32_t payload_generation = 0;
    };

    struct RingBufferDump {
        uint32_t channel_id = 0;
        uint32_t channel_type = 0;
        uint64_t generation = 0;
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
    // SuperBlock, allocator metadata, and v5 Channel Directory, then derives
    // the slab layout and active ring registrations automatically.
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
    // physical order. Returns kNotFound for unknown channel ids; malformed or
    // incompatible backing returns the authoritative MpmcRing validation status.
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

// Explicit offline migration tool APIs. They intentionally are not invoked by
// Attach and are not rolling-compatible operations.
Status UpgradeRegionV4ToV5Offline(const RegionV4UpgradeOptions& options);
Status CopyUpgradeRegionV4ToV5Offline(
    const RegionV4CopyUpgradeOptions& options);

// Free helpers shared by PrintReport and the CLI.
std::string SlotStateName(uint32_t state);

}  // namespace mino::tools

#endif  // TOOLS_MINO_INSPECTOR_H_
