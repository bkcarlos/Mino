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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <vector>

#include <gtest/gtest.h>

namespace mino::shm::recovery {
namespace {

// ---------------------------------------------------------------------------
// TestRegion: an anonymous in-memory region satisfying the scanner's layout
// contract. It deliberately allocates raw memory (not posix shm) so the
// same fixture works in single-process unit tests; the double-owner test
// places the control block in MAP_SHARED memory instead.
// ---------------------------------------------------------------------------
struct TestLayoutSpec {
    uint32_t class_count = 2;
    uint32_t slots_per_class = 128;   // Exercises the >64 bitmap word path.
    uint32_t slot_stride = 64;        // sizeof(SlabHeaderPrefix)-ish stride.
};

class TestRegion {
public:
    explicit TestRegion(TestLayoutSpec spec = {}) {
        const uint32_t slot_stride =
            std::max<uint32_t>(spec.slot_stride,
                               sizeof(RecoveryScanner::SlabHeaderPrefix));
        slot_stride_ = slot_stride;

        uint64_t offset = 0;
        recovery_offset_ = offset;
        offset += sizeof(RecoveryOwnerState);

        // Keep every section 64-byte aligned to mimic the real region layout.
        offset = Align64(offset);
        class_table_offset_ = offset;
        offset += sizeof(RecoveryScanner::ClassDescriptor) * spec.class_count;

        classes_.resize(spec.class_count);
        for (uint32_t c = 0; c < spec.class_count; ++c) {
            auto& cls = classes_[c];
            cls.class_id = c;
            cls.slot_count = spec.slots_per_class;
            cls.slot_stride = slot_stride;

            offset = Align64(offset);
            cls.bitmap_offset = offset;
            offset += ((spec.slots_per_class + 63) / 64) *
                      sizeof(RecoveryScanner::BitmapWord);

            offset = Align64(offset);
            cls.slots_offset = offset;
            offset += static_cast<uint64_t>(spec.slots_per_class) * slot_stride;
        }

        size_ = offset;
        // 64-byte-aligned allocation: sections are aligned via Align64 on top
        // of this base, so the base itself must be 64-aligned. RecoveryOwner-
        // State and the SlabHeaderPrefix views carry cache-line-aligned
        // atomics (UBSAN rejects placement-new on a merely 16-aligned heap
        // pointer, which is what glibc malloc returns).
        memory_.reset(new (std::align_val_t(64)) std::byte[size_]);
        std::memset(memory_.get(), 0, size_);

        RecoveryOwner::Initialize(RecoveryState());
        std::memcpy(ClassTable(), classes_.data(),
                    classes_.size() * sizeof(RecoveryScanner::ClassDescriptor));
    }

    RecoveryScanner::Layout layout() const {
        RecoveryScanner::Layout layout;
        layout.recovery_state_offset = recovery_offset_;
        layout.class_table_offset = class_table_offset_;
        layout.class_count = static_cast<uint32_t>(classes_.size());
        return layout;
    }

    std::byte* base() { return memory_.get(); }
    uint64_t size() const { return size_; }

    RecoveryOwnerState* RecoveryState() {
        return reinterpret_cast<RecoveryOwnerState*>(memory_.get() +
                                                     recovery_offset_);
    }

    RecoveryScanner::ClassDescriptor* ClassTable() {
        return reinterpret_cast<RecoveryScanner::ClassDescriptor*>(
            memory_.get() + class_table_offset_);
    }

    // --- Slot manipulation helpers (act as the "allocator under test") ---

    RecoveryScanner::SlabHeaderPrefix* Header(uint32_t cls, uint32_t slot) {
        const auto& c = classes_[cls];
        return reinterpret_cast<RecoveryScanner::SlabHeaderPrefix*>(
            memory_.get() + c.slots_offset +
            static_cast<uint64_t>(slot) * c.slot_stride);
    }

    void SetBit(uint32_t cls, uint32_t slot) {
        auto* words = reinterpret_cast<RecoveryScanner::BitmapWord*>(
            memory_.get() + classes_[cls].bitmap_offset);
        words[slot / 64].bits.fetch_or(1ULL << (slot % 64),
                                       std::memory_order_acq_rel);
    }

    bool IsBitSet(uint32_t cls, uint32_t slot) {
        auto* words = reinterpret_cast<RecoveryScanner::BitmapWord*>(
            memory_.get() + classes_[cls].bitmap_offset);
        return (words[slot / 64].bits.load(std::memory_order_acquire) >>
                    (slot % 64) &
                1ULL) != 0;
    }

    // Simulates a fully published object (valid magic + CRC).
    void MakePublished(uint32_t cls, uint32_t slot) {
        auto* h = Header(cls, slot);
        // Value-initialization both zero-initializes the scalar members and
        // default-constructs the atomic members; a prior memset would trip
        // GCC's -Wclass-memaccess on the non-trivially-copyable type.
        new (h) RecoveryScanner::SlabHeaderPrefix();
        h->magic = RecoveryScanner::kSlabMagic;
        h->header_version = 1;
        h->class_id = static_cast<uint16_t>(cls);
        h->generation = 7;
        h->capacity = 256;
        h->object_size = 200;
        h->type_id = 42;
        h->layout_version = 1;
        h->schema_short_id = 0xABCD;
        h->owner_epoch = 99;
        h->object_state.store(
            static_cast<uint32_t>(ObjectState::kPublished),
            std::memory_order_release);
        h->immutable_header_crc = ComputeCrcForTest(*h);
        SetBit(cls, slot);
    }

    // Simulates a crashed allocator: bitmap bit set, no valid state.
    void MakeOrphan(uint32_t cls, uint32_t slot, uint32_t raw_state = 0) {
        auto* h = Header(cls, slot);
        // See MakePublished: value-initialization replaces the memset.
        new (h) RecoveryScanner::SlabHeaderPrefix();
        h->magic = RecoveryScanner::kSlabMagic;
        h->generation = 3;
        h->object_state.store(raw_state, std::memory_order_release);
        SetBit(cls, slot);
    }

    // Simulates a retired object whose borrows/pins are configurable.
    void MakeRetired(uint32_t cls, uint32_t slot, uint32_t borrows = 0,
                     uint32_t pins = 0) {
        MakePublished(cls, slot);
        auto* h = Header(cls, slot);
        h->object_state.store(static_cast<uint32_t>(ObjectState::kRetired),
                              std::memory_order_release);
        h->borrow_refcount.store(borrows, std::memory_order_release);
        h->pin_refcount.store(pins, std::memory_order_release);
    }

    // Simulates a header left behind after the bitmap bit was cleared.
    void MakeInconsistent(uint32_t cls, uint32_t slot) {
        MakePublished(cls, slot);
        auto* words = reinterpret_cast<RecoveryScanner::BitmapWord*>(
            memory_.get() + classes_[cls].bitmap_offset);
        words[slot / 64].bits.fetch_and(~(1ULL << (slot % 64)),
                                        std::memory_order_acq_rel);
    }

    void MakeCorrupted(uint32_t cls, uint32_t slot) {
        MakePublished(cls, slot);
        Header(cls, slot)->magic = 0xDEADBEEF;
    }

    // CRC with the same coverage as the scanner's ComputeImmutableCrc. The
    // scanner's own CRC function is exercised end-to-end by the corruption
    // tests; this helper builds matching headers via the public verifier.
    static uint32_t ComputeCrcForTest(
        const RecoveryScanner::SlabHeaderPrefix& h) {
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
        } view{h.magic,     h.header_version, h.class_id,
               h.generation, h.capacity,      h.object_size,
               h.type_id,   h.layout_version, h.schema_short_id};
        // CRC32C table (reflected, poly 0x82F63B78).
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

private:
    static uint64_t Align64(uint64_t v) { return (v + 63) & ~uint64_t{63}; }

    struct AlignedDeleter {
        void operator()(std::byte* p) const {
            ::operator delete[](p, std::align_val_t(64));
        }
    };

    std::unique_ptr<std::byte[], AlignedDeleter> memory_;
    uint64_t size_ = 0;
    uint64_t recovery_offset_ = 0;
    uint64_t class_table_offset_ = 0;
    uint32_t slot_stride_ = 0;
    std::vector<RecoveryScanner::ClassDescriptor> classes_;
};

class RecoveryScannerTest : public ::testing::Test {
protected:
    // Creates a scanner that owns the region (acquires the lease).
    Result<RecoveryScanner> MakeOwnerScanner(
        RecoveryScannerOptions options = {}) {
        auto result = RecoveryScanner::Create(region_.base(), region_.size(),
                                              region_.layout(), options);
        if (!result.ok()) {
            return result.status();
        }
        Status st = result->Owner().TryAcquire();
        if (!st.ok()) {
            return st;
        }
        return result;
    }

    TestRegion region_;
};

TEST_F(RecoveryScannerTest, CreateRejectsInvalidLayout) {
    RecoveryScanner::Layout bad = region_.layout();
    bad.recovery_state_offset = region_.size() - 8;  // Out of bounds.
    auto result =
        RecoveryScanner::Create(region_.base(), region_.size(), bad);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(RecoveryScannerTest, CreateRejectsBadRecoveryMagic) {
    region_.RecoveryState()->magic = 0xBAD;
    auto result = RecoveryScanner::Create(region_.base(), region_.size(),
                                          region_.layout());
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kCorruption);
}

TEST_F(RecoveryScannerTest, ScanCleanRegionFindsNothing) {
    region_.MakePublished(0, 0);
    region_.MakePublished(1, 65);  // Cross into the second bitmap word.
    auto scanner = MakeOwnerScanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();

    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->slots_scanned, 256u);
    EXPECT_EQ(report->orphan_slab_count, 0u);
    EXPECT_EQ(report->reclaimed_slab_count, 0u);
    EXPECT_EQ(report->bitmap_inconsistency_count, 0u);
    EXPECT_EQ(report->corrupted_slab_count, 0u);
    // Published slots must survive a repairing scan untouched.
    EXPECT_TRUE(region_.IsBitSet(0, 0));
    EXPECT_TRUE(region_.IsBitSet(1, 65));
}

TEST_F(RecoveryScannerTest, DetectsAndReclaimsOrphanSlabs) {
    region_.MakePublished(0, 1);
    region_.MakeOrphan(0, 2, /*raw_state=*/0);        // FREE-like garbage.
    region_.MakeOrphan(0, 3, /*raw_state=*/0xCAFE);   // Invalid enum value.
    region_.MakeOrphan(1, 100, /*raw_state=*/0);      // Second class.

    auto scanner = MakeOwnerScanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();

    EXPECT_EQ(report->orphan_slab_count, 3u);
    EXPECT_EQ(report->reclaimed_slab_count, 3u);
    EXPECT_TRUE(region_.IsBitSet(0, 1));    // Published survivor.
    EXPECT_FALSE(region_.IsBitSet(0, 2));   // Orphans reclaimed.
    EXPECT_FALSE(region_.IsBitSet(0, 3));
    EXPECT_FALSE(region_.IsBitSet(1, 100));
    // State reset to FREE so a future allocation starts clean.
    EXPECT_EQ(region_.Header(0, 2)->object_state.load(), 0u);
    EXPECT_NE(report->details.find("orphan_slab"), std::string::npos);
}

TEST_F(RecoveryScannerTest, ScanIsIdempotent) {
    region_.MakeOrphan(0, 5);
    region_.MakeRetired(0, 6);
    region_.MakeInconsistent(0, 7);

    auto scanner = MakeOwnerScanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();

    auto first = scanner->Scan();
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_EQ(first->orphan_slab_count, 1u);
    EXPECT_EQ(first->reclaimed_slab_count, 2u);  // Orphan + retired.
    EXPECT_EQ(first->bitmap_inconsistency_count, 1u);

    auto second = scanner->Scan();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(second->orphan_slab_count, 0u);
    EXPECT_EQ(second->reclaimed_slab_count, 0u);
    EXPECT_EQ(second->bitmap_inconsistency_count, 0u);
    EXPECT_EQ(second->corrupted_slab_count, 0u);
}

TEST_F(RecoveryScannerTest, ReadOnlyScanDoesNotRepair) {
    region_.MakeOrphan(0, 9);
    RecoveryScannerOptions options;
    options.repair = false;
    auto scanner = MakeOwnerScanner(options);
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();

    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->orphan_slab_count, 1u);
    EXPECT_EQ(report->reclaimed_slab_count, 0u);
    EXPECT_TRUE(region_.IsBitSet(0, 9));  // Untouched.
}

TEST_F(RecoveryScannerTest, RepairRequiresOwnership) {
    region_.MakeOrphan(0, 10);
    auto scanner = RecoveryScanner::Create(region_.base(), region_.size(),
                                           region_.layout());
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();

    // No TryAcquire: repairing entry points must refuse.
    Status st = scanner->ReclaimOrphanSlabs();
    EXPECT_EQ(st.code(), StatusCode::kPermissionDenied);
    st = scanner->Scan().status();
    EXPECT_EQ(st.code(), StatusCode::kPermissionDenied);
    // Bit untouched.
    EXPECT_TRUE(region_.IsBitSet(0, 10));
}

TEST_F(RecoveryScannerTest, RetiredReclaimedOnlyWithoutBorrowOrPin) {
    region_.MakeRetired(0, 20, /*borrows=*/0, /*pins=*/0);  // Reclaimable.
    region_.MakeRetired(0, 21, /*borrows=*/1, /*pins=*/0);  // Borrow alive.
    region_.MakeRetired(0, 22, /*borrows=*/0, /*pins=*/2);  // Pins alive.

    auto scanner = MakeOwnerScanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();

    EXPECT_EQ(report->reclaimed_slab_count, 1u);
    EXPECT_FALSE(region_.IsBitSet(0, 20));
    EXPECT_TRUE(region_.IsBitSet(0, 21));
    EXPECT_TRUE(region_.IsBitSet(0, 22));
    EXPECT_NE(report->details.find("pin_refcount=2"), std::string::npos);
}

TEST_F(RecoveryScannerTest, DetectsAndRepairsBitmapInconsistency) {
    region_.MakeInconsistent(0, 30);
    region_.MakeInconsistent(1, 70);

    auto scanner = MakeOwnerScanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    Status st = scanner->VerifyBitmapConsistency();
    EXPECT_TRUE(st.ok()) << st.ToString();

    // Header state cleared to FREE; no bits were set (repair direction).
    EXPECT_EQ(region_.Header(0, 30)->object_state.load(), 0u);
    EXPECT_EQ(region_.Header(1, 70)->object_state.load(), 0u);
    EXPECT_FALSE(region_.IsBitSet(0, 30));
}

TEST_F(RecoveryScannerTest, CorruptionIsReportedNotRepaired) {
    region_.MakeCorrupted(0, 40);
    region_.MakeOrphan(0, 41);

    auto scanner = MakeOwnerScanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();

    EXPECT_EQ(report->corrupted_slab_count, 1u);
    EXPECT_EQ(report->orphan_slab_count, 1u);          // Orphan still reclaimed.
    EXPECT_TRUE(region_.IsBitSet(0, 40));              // Corruption untouched.
    EXPECT_NE(report->details.find("quarantine"), std::string::npos);

    Status st = scanner->VerifyBitmapConsistency();
    EXPECT_EQ(st.code(), StatusCode::kCorruption);
}

TEST_F(RecoveryScannerTest, DetectsCrcMismatch) {
    region_.MakePublished(0, 50);
    // Tamper with an immutable field after CRC was computed.
    region_.Header(0, 50)->object_size = 12345;

    auto scanner = MakeOwnerScanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->corrupted_slab_count, 1u);
    EXPECT_TRUE(region_.IsBitSet(0, 50));  // Never auto-repaired.
}

TEST_F(RecoveryScannerTest, CleanupStaleAcksClearsDeadSubscriberBits) {
    // Three broadcast slots; subscriber set {0,1,2,3}, subscribers 1 and 3
    // died (lease expiry confirmed by the caller/coordinator).
    uint64_t bitmaps[3] = {
        0b1111,  // All four owe ACKs.
        0b0110,  // Subscribers 1 and 2 owe ACKs.
        0b0000,  // Fully ACKed already.
    };
    RecoveryScanner::AckScanInput input;
    input.live_subscriber_mask = 0b0101;  // Subscribers 0 and 2 alive.
    input.bitmaps = bitmaps;
    input.bitmap_count = 3;

    auto scanner = RecoveryScanner::Create(region_.base(), region_.size(),
                                           region_.layout());
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();

    uint64_t cleared = 0;
    Status st = scanner->CleanupStaleAcks(input, &cleared);
    ASSERT_TRUE(st.ok()) << st.ToString();
    EXPECT_EQ(cleared, 3u);          // Bits 1,3 in slot 0; bit 1 in slot 1.
    EXPECT_EQ(bitmaps[0], 0b0101u);  // Live bits preserved.
    EXPECT_EQ(bitmaps[1], 0b0100u);
    EXPECT_EQ(bitmaps[2], 0b0000u);

    // Idempotent: a second pass clears nothing.
    st = scanner->CleanupStaleAcks(input, &cleared);
    ASSERT_TRUE(st.ok()) << st.ToString();
    EXPECT_EQ(cleared, 0u);
}

TEST_F(RecoveryScannerTest, CleanupStaleAcksRejectsNullBitmaps) {
    RecoveryScanner::AckScanInput input;
    input.bitmaps = nullptr;
    input.bitmap_count = 1;
    auto scanner = RecoveryScanner::Create(region_.base(), region_.size(),
                                           region_.layout());
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    EXPECT_EQ(scanner->CleanupStaleAcks(input).code(),
              StatusCode::kInvalidArgument);
}

TEST_F(RecoveryScannerTest, MultiWordBitmapBoundary) {
    // Slots around the 64-bit word boundary and at the tail word.
    region_.MakeOrphan(0, 63);
    region_.MakeOrphan(0, 64);
    region_.MakeOrphan(0, 127);

    auto scanner = MakeOwnerScanner();
    ASSERT_TRUE(scanner.ok()) << scanner.status().ToString();
    auto report = scanner->Scan();
    ASSERT_TRUE(report.ok()) << report.status().ToString();
    EXPECT_EQ(report->reclaimed_slab_count, 3u);
    EXPECT_FALSE(region_.IsBitSet(0, 63));
    EXPECT_FALSE(region_.IsBitSet(0, 64));
    EXPECT_FALSE(region_.IsBitSet(0, 127));
}

}  // namespace
}  // namespace mino::shm::recovery
