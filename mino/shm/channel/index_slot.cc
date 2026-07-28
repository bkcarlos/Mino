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

#include "mino/shm/channel/index_slot.h"

#include <cstdint>

namespace mino {
namespace {

// CRC-32C (Castagnoli, iSCSI polynomial 0x1EDC6F41, reflected) lookup table.
// Matches the Slab Header CRC (design doc 8.1) so corruption-detection
// semantics are uniform across the SHM data plane. Table-driven to stay
// dependency-free; a hardware CRC32C path can replace this behind the same
// interface without changing the wire semantics.
const uint32_t* Crc32cTable() {
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
    MINO_CRC32C_ENTRY(n), MINO_CRC32C_ENTRY(n + 1), \
        MINO_CRC32C_ENTRY(n + 2), MINO_CRC32C_ENTRY(n + 3)
#define MINO_CRC32C_16(n) \
    MINO_CRC32C_4(n), MINO_CRC32C_4(n + 4), MINO_CRC32C_4(n + 8), \
        MINO_CRC32C_4(n + 12)
#define MINO_CRC32C_64(n) \
    MINO_CRC32C_16(n), MINO_CRC32C_16(n + 16), MINO_CRC32C_16(n + 32), \
        MINO_CRC32C_16(n + 48)
        MINO_CRC32C_64(0), MINO_CRC32C_64(64), MINO_CRC32C_64(128),
        MINO_CRC32C_64(192)
#undef MINO_CRC32C_64
#undef MINO_CRC32C_16
#undef MINO_CRC32C_4
#undef MINO_CRC32C_ENTRY
    };
    return kTable;
}

// Field-wise CRC32C over the immutable metadata. Fields are fed in
// declaration order in their wire (little-endian) encoding — on the
// supported little-endian targets this is the identity. Feeding field by
// field (instead of raw bytes) keeps the CRC independent of any padding the
// compiler might place between the immutable fields.
//
// Coverage per design doc 9.2: msg_type, schema_version, schema_short_id,
// schema_layout_version, reserved0, sequence_num, timestamp_ns, payload
// (offset, generation, region_id), payload_len. NOT covered: state, flags,
// the CRC field itself, and all sidecar metadata. The snapshot overload is
// the single implementation; the slot overload delegates so the coverage can
// never drift between the two views.
uint32_t ComputeCrc(const IndexSlotSnapshot& s) noexcept {
    const uint32_t* table = Crc32cTable();
    uint32_t crc = 0xFFFFFFFFu;

    auto add_byte = [&crc, table](uint8_t byte) noexcept {
        crc = table[(crc ^ byte) & 0xFFu] ^ (crc >> 8);
    };
    auto add_u32 = [&add_byte](uint32_t v) noexcept {
        for (int i = 0; i < 4; ++i) {
            add_byte(static_cast<uint8_t>(v & 0xFFu));
            v >>= 8;
        }
    };
    auto add_u64 = [&add_byte](uint64_t v) noexcept {
        for (int i = 0; i < 8; ++i) {
            add_byte(static_cast<uint8_t>(v & 0xFFu));
            v >>= 8;
        }
    };

    add_u32(s.msg_type);
    add_u32(s.schema_version);
    add_u64(s.schema_short_id);
    add_u32(s.schema_layout_version);
    add_u32(s.reserved0);
    add_u64(s.sequence_num);
    add_u64(s.timestamp_ns);
    add_u64(s.payload.offset);
    add_u32(s.payload.generation);
    add_u32(s.payload.region_id);
    add_u32(s.payload_len);

    return crc ^ 0xFFFFFFFFu;
}

}  // namespace

IndexSlotSnapshot SnapshotIndexSlot(const IndexSlot& slot) noexcept {
    IndexSlotSnapshot s;
    s.msg_type = slot.msg_type;
    s.schema_version = slot.schema_version;
    s.schema_short_id = slot.schema_short_id;
    s.schema_layout_version = slot.schema_layout_version;
    s.reserved0 = slot.reserved0;
    s.sequence_num = slot.sequence_num;
    s.timestamp_ns = slot.timestamp_ns;
    s.payload = slot.payload;
    s.payload_len = slot.payload_len;
    s.immutable_metadata_crc = slot.immutable_metadata_crc;
    s.flags = slot.flags;
    return s;
}

uint32_t ComputeSnapshotCrc(const IndexSlotSnapshot& snapshot) noexcept {
    return ComputeCrc(snapshot);
}

bool VerifySnapshotCrc(const IndexSlotSnapshot& snapshot) noexcept {
    return snapshot.immutable_metadata_crc == ComputeCrc(snapshot);
}

uint32_t ComputeIndexSlotImmutableCrc(const IndexSlot& slot) noexcept {
    return ComputeCrc(SnapshotIndexSlot(slot));
}

void SealIndexSlotImmutableCrc(IndexSlot& slot) noexcept {
    IndexSlotSnapshot s = SnapshotIndexSlot(slot);
    s.immutable_metadata_crc = 0;
    slot.immutable_metadata_crc = ComputeCrc(s);
}

bool VerifyIndexSlotImmutableCrc(const IndexSlot& slot) noexcept {
    return VerifySnapshotCrc(SnapshotIndexSlot(slot));
}

}  // namespace mino
