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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "mino/platform/shared_memory.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/region/region.h"

namespace mino::tools {
namespace {

// In-memory region image used to exercise the Inspector without real SHM.
class InspectorFixture {
public:
    InspectorFixture() {
        uint64_t offset = 0;

        // One class, 4 slots of 128 bytes each.
        class_.class_id = 0;
        class_.slot_count = 4;
        class_.slot_stride = 128;

        offset = Align64(offset);
        class_.bitmap_offset = offset;
        offset += 64;  // One cache-line bitmap word.

        offset = Align64(offset);
        class_.slots_offset = offset;
        offset += 4 * 128;

        offset = Align64(offset);
        ring_control_offset_ = offset;
        offset += sizeof(Inspector::RingControlView);

        offset = Align64(offset);
        ring_slots_offset_ = offset;
        offset += 8 * Inspector::kIndexSlotSize;

        size_ = offset;
        // 64-byte-aligned allocation: the views placed inside carry
        // cache-line-aligned atomics (UBSAN rejects placement-new on a
        // merely 16-aligned heap pointer).
        memory_.reset(new (std::align_val_t(64)) std::byte[size_]);
        std::memset(memory_.get(), 0, size_);

        // Initialize the ring control block (8 slots, 128-byte elements).
        auto* control = RingControl();
        new (control) Inspector::RingControlView();
        control->magic = Inspector::RingControlView::kMagic;
        control->layout_version = 1;
        control->elem_size = Inspector::kIndexSlotSize;
        control->elem_align = 64;
        control->capacity = 8;
    }

    Inspector::Layout MakeLayout() const {
        Inspector::Layout layout;
        layout.classes.push_back(class_);
        Inspector::Layout::RingRef ring;
        ring.channel_id = 7;
        ring.reserved = 0;
        ring.control_offset = ring_control_offset_;
        ring.slots_offset = ring_slots_offset_;
        layout.rings.push_back(ring);
        return layout;
    }

    std::byte* base() { return memory_.get(); }
    uint64_t size() const { return size_; }

    Inspector::RingControlView* RingControl() {
        return reinterpret_cast<Inspector::RingControlView*>(
            memory_.get() + ring_control_offset_);
    }

    Inspector::IndexSlotView* RingSlot(uint32_t index) {
        return reinterpret_cast<Inspector::IndexSlotView*>(
            memory_.get() + ring_slots_offset_ +
            index * Inspector::kIndexSlotSize);
    }

    Inspector::SlabHeaderView* Slab(uint32_t slot) {
        return reinterpret_cast<Inspector::SlabHeaderView*>(
            memory_.get() + class_.slots_offset +
            static_cast<uint64_t>(slot) * class_.slot_stride);
    }

    void SetBitmap(uint32_t slot) {
        auto* word = reinterpret_cast<std::atomic<uint64_t>*>(
            memory_.get() + class_.bitmap_offset);
        word->fetch_or(1ULL << slot, std::memory_order_acq_rel);
    }

    // Publishes slot `slot` with a valid header + CRC.
    void MakePublished(uint32_t slot) {
        auto* h = Slab(slot);
        // Value-initialization initializes every member (scalar and atomic);
        // a prior memset would trip GCC's -Wclass-memaccess on the
        // non-trivially-copyable type.
        new (h) Inspector::SlabHeaderView();
        h->magic = Inspector::kSlabMagic;
        h->header_version = kSlabHeaderVersion;
        h->class_id = 0;
        h->generation.store(11, std::memory_order_relaxed);
        h->capacity = 128;
        h->object_size = 100;
        h->type_id = 5;
        h->layout_version = 1;
        h->schema_short_id = 0x1234;
        h->object_state.store(3 /*PUBLISHED*/, std::memory_order_release);
        h->immutable_header_crc = ComputeCrc(*h);
        SetBitmap(slot);
    }

    // Bitmap occupied but object_state never published.
    void MakeOrphan(uint32_t slot) {
        auto* h = Slab(slot);
        // See MakePublished: value-initialization replaces the memset.
        new (h) Inspector::SlabHeaderView();
        h->magic = Inspector::kSlabMagic;
        h->generation.store(4, std::memory_order_relaxed);
        h->object_state.store(0, std::memory_order_release);
        SetBitmap(slot);
    }

    // Bitmap free but a stale header state lingers.
    void MakeInconsistent(uint32_t slot) {
        MakePublished(slot);
        auto* word = reinterpret_cast<std::atomic<uint64_t>*>(
            memory_.get() + class_.bitmap_offset);
        word->fetch_and(~(1ULL << slot), std::memory_order_acq_rel);
    }

    void MakeCorrupt(uint32_t slot) {
        MakePublished(slot);
        Slab(slot)->magic = 0xDEADBEEF;
    }

    static uint32_t ComputeCrc(const Inspector::SlabHeaderView& h) {
        return ComputeImmutableHeaderCrc(h);
    }

private:
    static uint64_t Align64(uint64_t v) { return (v + 63) & ~uint64_t{63}; }

    struct AlignedDeleter {
        void operator()(std::byte* p) const {
            ::operator delete[](p, std::align_val_t(64));
        }
    };

    std::unique_ptr<std::byte[], AlignedDeleter> memory_;
    uint64_t size_ = 0;
    uint64_t ring_control_offset_ = 0;
    uint64_t ring_slots_offset_ = 0;
    Inspector::ClassView class_{};
};

class InspectorTest : public ::testing::Test {
protected:
    std::string RegionName(const char* tag) {
        static uint32_t sequence = 0;
        std::string name = "/mi_" + std::to_string(::getpid()) + "_" +
                           std::to_string(++sequence) + tag;
        names_.push_back(name);
        return name;
    }

    void TearDown() override {
        for (const std::string& name : names_) {
            (void)SharedMemorySegment::Unlink(name);
        }
    }

    InspectorFixture fixture_;
    std::vector<std::string> names_;
};

TEST_F(InspectorTest, AttachByNameValidatesInputAndMissingRegion) {
    auto empty = Inspector::Attach("");
    ASSERT_FALSE(empty.ok());
    EXPECT_EQ(empty.status().code(), StatusCode::kInvalidArgument);

    auto missing = Inspector::Attach(RegionName("_missing"));
    ASSERT_FALSE(missing.ok());
    EXPECT_EQ(missing.status().code(), StatusCode::kNotFound);
}

TEST_F(InspectorTest, AttachByNameDerivesRealAllocatorLayoutReadOnly) {
    const std::string name = RegionName("_real");
    RegionCreateOptions create_options;
    create_options.name = name;
    create_options.size_bytes = 1024 * 1024;
    auto region = SharedMemoryRegion::Create(create_options);
    ASSERT_TRUE(region.ok()) << region.status().ToString();

    const SuperBlock& sb = *region->superblock();
    RegionAllocatorStorage storage{
        .region_base = region->base(),
        .region_size = region->size(),
        .allocator_offset = sb.allocator_offset,
        .allocator_size = sb.data_offset - sb.allocator_offset,
        .data_offset = sb.data_offset,
        .data_size = sb.data_size,
        .region_id = sb.region_id,
    };
    ClassTableConfig config;
    config.classes = {
        {.slot_size = 64, .slot_count = 4},
        {.slot_size = 256, .slot_count = 2},
    };
    auto allocator = CentralSlabAllocator::CreateInRegion(storage, config);
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    AllocationRequest request;
    request.object_size = 32;
    request.type_id = TypeId{7};
    request.schema = SchemaIdentity{.short_id = 0x1234, .layout_version = 1};
    auto handle = allocator->Allocate(request);
    ASSERT_TRUE(handle.ok()) << handle.status().ToString();
    ASSERT_TRUE(allocator->BeginBuild(*handle).ok());
    ASSERT_TRUE(allocator->Publish(*handle).ok());

    const RegionState state_before = LoadRegionState(*region->superblock());
    const bool clean_before = LoadCleanShutdown(*region->superblock());
    const uint64_t epoch_before = LoadRegionEpoch(*region->superblock());
    {
        auto inspector = Inspector::Attach(name);
        ASSERT_TRUE(inspector.ok()) << inspector.status().ToString();
        ASSERT_EQ(inspector->layout().classes.size(), 2u);
        EXPECT_TRUE(inspector->layout().rings.empty());
        EXPECT_EQ(inspector->layout().classes[0].slot_count, 4u);
        EXPECT_EQ(inspector->layout().classes[1].slot_count, 2u);

        auto report = inspector->ScanSlabs();
        ASSERT_TRUE(report.ok()) << report.status().ToString();
        EXPECT_EQ(report->total_slots, 6u);
        EXPECT_EQ(report->ok_count, 1u);
        EXPECT_EQ(report->free_count, 5u);
        EXPECT_EQ(report->orphan_count, 0u);
        EXPECT_EQ(report->inconsistent_count, 0u);
        EXPECT_EQ(report->corrupt_count, 0u);

        auto ring = inspector->DumpRingBuffer(7);
        ASSERT_FALSE(ring.ok());
        EXPECT_EQ(ring.status().code(), StatusCode::kNotFound);
        EXPECT_NE(ring.status().message().find("explicit RingRef"),
                  std::string_view::npos);
    }

    // Destroying the Inspector only unmaps its read-only attachment; it must
    // not run recovery or publish CLOSED/clean lifecycle state.
    EXPECT_EQ(LoadRegionState(*region->superblock()), state_before);
    EXPECT_EQ(LoadCleanShutdown(*region->superblock()), clean_before);
    EXPECT_EQ(LoadRegionEpoch(*region->superblock()), epoch_before);
}

TEST_F(InspectorTest, AttachByNameDiagnosesQuarantinedRegionReadOnly) {
    const std::string name = RegionName("_quarantined");
    RegionCreateOptions create_options;
    create_options.name = name;
    create_options.size_bytes = 1024 * 1024;
    auto region = SharedMemoryRegion::Create(create_options);
    ASSERT_TRUE(region.ok()) << region.status().ToString();

    SuperBlock* sb = region->superblock();
    RegionAllocatorStorage storage{
        .region_base = region->base(),
        .region_size = region->size(),
        .allocator_offset = sb->allocator_offset,
        .allocator_size = sb->data_offset - sb->allocator_offset,
        .data_offset = sb->data_offset,
        .data_size = sb->data_size,
        .region_id = sb->region_id,
    };
    ClassTableConfig config;
    config.classes = {{.slot_size = 64, .slot_count = 4}};
    auto allocator = CentralSlabAllocator::CreateInRegion(storage, config);
    ASSERT_TRUE(allocator.ok()) << allocator.status().ToString();

    StoreState(*sb, RegionState::kQuarantined);
    StoreRecoveryFence(
        *sb, EncodeRecoveryFence(LoadRecoveryEpoch(*sb),
                                 RecoveryFencePhase::kQuarantined));
    const RegionState state_before = LoadRegionState(*sb);
    const uint64_t epoch_before = LoadRegionEpoch(*sb);
    const bool clean_before = LoadCleanShutdown(*sb);
    const uint64_t service_fence_before = LoadServiceFence(*sb);
    const uint64_t recovery_fence_before = LoadRecoveryFence(*sb);

    {
        auto inspector = Inspector::Attach(name);
        ASSERT_TRUE(inspector.ok()) << inspector.status().ToString();
        auto report = inspector->ScanSlabs();
        ASSERT_TRUE(report.ok()) << report.status().ToString();
        EXPECT_EQ(report->total_slots, 4u);
        EXPECT_EQ(report->free_count, 4u);
        EXPECT_EQ(report->corrupt_count, 0u);
    }

    EXPECT_EQ(LoadRegionState(*sb), state_before);
    EXPECT_EQ(LoadRegionEpoch(*sb), epoch_before);
    EXPECT_EQ(LoadCleanShutdown(*sb), clean_before);
    EXPECT_EQ(LoadServiceFence(*sb), service_fence_before);
    EXPECT_EQ(LoadRecoveryFence(*sb), recovery_fence_before);
}

TEST_F(InspectorTest, AttachByNameRejectsRegionWithoutAllocatorMetadata) {
    const std::string name = RegionName("_empty");
    RegionCreateOptions options;
    options.name = name;
    options.size_bytes = 1024 * 1024;
    auto region = SharedMemoryRegion::Create(options);
    ASSERT_TRUE(region.ok()) << region.status().ToString();

    auto inspector = Inspector::Attach(name);
    ASSERT_FALSE(inspector.ok());
    EXPECT_EQ(inspector.status().code(), StatusCode::kNotFound);
    EXPECT_NE(inspector.status().message().find("allocator metadata"),
              std::string_view::npos);
    EXPECT_EQ(LoadRegionState(*region->superblock()), RegionState::kActive);
    EXPECT_FALSE(LoadCleanShutdown(*region->superblock()));
}

TEST_F(InspectorTest, AttachByNameRejectsCorruptAllocatorMetadataReadOnly) {
    const std::string name = RegionName("_corrupt");
    RegionCreateOptions options;
    options.name = name;
    options.size_bytes = 1024 * 1024;
    auto region = SharedMemoryRegion::Create(options);
    ASSERT_TRUE(region.ok()) << region.status().ToString();
    const uint64_t allocator_offset = region->superblock()->allocator_offset;
    *reinterpret_cast<uint32_t*>(region->base() + allocator_offset) =
        0xDEADBEEFu;

    auto inspector = Inspector::Attach(name);
    ASSERT_FALSE(inspector.ok());
    EXPECT_EQ(inspector.status().code(), StatusCode::kCorruption);
    EXPECT_EQ(LoadRegionState(*region->superblock()), RegionState::kActive);
    EXPECT_FALSE(LoadCleanShutdown(*region->superblock()));
}

TEST_F(InspectorTest, AttachMemoryValidatesBounds) {
    Inspector::Layout bad = fixture_.MakeLayout();
    bad.classes[0].slots_offset = fixture_.size() - 4;  // Out of bounds.
    auto result = Inspector::AttachMemory(fixture_.base(), fixture_.size(),
                                          bad, "test");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(InspectorTest, ScanSlabsCleanRegion) {
    fixture_.MakePublished(0);
    fixture_.MakePublished(2);
    auto inspector = Inspector::AttachMemory(
        fixture_.base(), fixture_.size(), fixture_.MakeLayout(), "test");
    ASSERT_TRUE(inspector.ok()) << inspector.status().ToString();

    auto report = inspector->ScanSlabs();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->total_slots, 4u);
    EXPECT_EQ(report->ok_count, 2u);
    EXPECT_EQ(report->free_count, 2u);
    EXPECT_EQ(report->orphan_count, 0u);
    EXPECT_EQ(report->inconsistent_count, 0u);
    EXPECT_EQ(report->corrupt_count, 0u);
    EXPECT_TRUE(report->findings.empty());
}

TEST_F(InspectorTest, ScanSlabsClassifiesAllFindingKinds) {
    fixture_.MakePublished(0);     // ok
    fixture_.MakeOrphan(1);        // orphan
    fixture_.MakeInconsistent(2);  // inconsistent
    fixture_.MakeCorrupt(3);       // corrupt

    auto inspector = Inspector::AttachMemory(
        fixture_.base(), fixture_.size(), fixture_.MakeLayout(), "test");
    ASSERT_TRUE(inspector.ok()) << inspector.status().ToString();
    auto report = inspector->ScanSlabs();
    ASSERT_TRUE(report.ok()) << report.status().ToString();

    EXPECT_EQ(report->total_slots, 4u);
    EXPECT_EQ(report->ok_count, 1u);
    EXPECT_EQ(report->orphan_count, 1u);
    EXPECT_EQ(report->inconsistent_count, 1u);
    EXPECT_EQ(report->corrupt_count, 1u);
    ASSERT_EQ(report->findings.size(), 3u);

    // Findings carry kind + location.
    EXPECT_EQ(report->findings[0].kind, Inspector::SlabFinding::Kind::kOrphan);
    EXPECT_EQ(report->findings[0].slot_index, 1u);
    EXPECT_EQ(report->findings[1].kind,
              Inspector::SlabFinding::Kind::kInconsistent);
    EXPECT_EQ(report->findings[1].slot_index, 2u);
    EXPECT_EQ(report->findings[2].kind,
              Inspector::SlabFinding::Kind::kCorrupt);
    EXPECT_EQ(report->findings[2].slot_index, 3u);
}

TEST_F(InspectorTest, DumpRingBufferReportsCursorsAndSlots) {
    auto* control = fixture_.RingControl();
    control->enqueue_pos.store(5, std::memory_order_release);
    control->dequeue_pos.store(2, std::memory_order_release);

    auto* slot = fixture_.RingSlot(3);
    slot->sequence_num = 41;
    slot->msg_type = 7;
    slot->timestamp_ns = 123456789;
    slot->payload_offset = 0x2000;
    slot->payload_generation = 9;
    slot->payload_len = 256;
    slot->state.store(3 /*READY*/, std::memory_order_release);

    auto inspector = Inspector::AttachMemory(
        fixture_.base(), fixture_.size(), fixture_.MakeLayout(), "test");
    ASSERT_TRUE(inspector.ok()) << inspector.status().ToString();
    auto dump = inspector->DumpRingBuffer(7);
    ASSERT_TRUE(dump.ok()) << dump.status().ToString();

    EXPECT_EQ(dump->channel_id, 7u);
    EXPECT_EQ(dump->capacity, 8u);
    EXPECT_EQ(dump->enqueue_pos, 5u);
    EXPECT_EQ(dump->dequeue_pos, 2u);
    EXPECT_EQ(dump->pending, 3u);
    ASSERT_EQ(dump->slots.size(), 8u);
    EXPECT_EQ(dump->slots[3].sequence, 41u);
    EXPECT_EQ(dump->slots[3].state, 3u);
    EXPECT_EQ(dump->slots[3].msg_type, 7u);
    EXPECT_EQ(dump->slots[3].payload_offset, 0x2000u);
    EXPECT_EQ(dump->slots[3].payload_generation, 9u);
    EXPECT_EQ(dump->slots[3].payload_len, 256u);
    // Untouched slots remain FREE.
    EXPECT_EQ(dump->slots[0].state, 0u);
}

TEST_F(InspectorTest, DumpRingBufferUnknownChannel) {
    auto inspector = Inspector::AttachMemory(
        fixture_.base(), fixture_.size(), fixture_.MakeLayout(), "test");
    ASSERT_TRUE(inspector.ok()) << inspector.status().ToString();
    auto dump = inspector->DumpRingBuffer(99);
    ASSERT_FALSE(dump.ok());
    EXPECT_EQ(dump.status().code(), StatusCode::kNotFound);
}

TEST_F(InspectorTest, DumpRingBufferBadMagicIsCorruption) {
    fixture_.RingControl()->magic = 0xBAD;
    auto inspector = Inspector::AttachMemory(
        fixture_.base(), fixture_.size(), fixture_.MakeLayout(), "test");
    ASSERT_TRUE(inspector.ok()) << inspector.status().ToString();
    auto dump = inspector->DumpRingBuffer(7);
    ASSERT_FALSE(dump.ok());
    EXPECT_EQ(dump.status().code(), StatusCode::kCorruption);
}

TEST_F(InspectorTest, PrintReportIncludesSlabsAndRings) {
    fixture_.MakePublished(0);
    fixture_.MakeOrphan(1);
    fixture_.RingControl()->enqueue_pos.store(1, std::memory_order_release);

    auto inspector = Inspector::AttachMemory(
        fixture_.base(), fixture_.size(), fixture_.MakeLayout(), "test");
    ASSERT_TRUE(inspector.ok()) << inspector.status().ToString();

    std::ostringstream out;
    Status st = inspector->PrintReport(out);
    ASSERT_TRUE(st.ok()) << st.ToString();
    const std::string text = out.str();
    EXPECT_NE(text.find("Slab Consistency"), std::string::npos);
    EXPECT_NE(text.find("orphan:             1"), std::string::npos);
    EXPECT_NE(text.find("RingBuffer channel 7"), std::string::npos);
    EXPECT_NE(text.find("enqueue_pos:        1"), std::string::npos);
}

}  // namespace
}  // namespace mino::tools
