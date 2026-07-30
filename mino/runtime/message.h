// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_MESSAGE_H_
#define MINO_RUNTIME_MESSAGE_H_

#include <cstdint>

#include "mino/abi/shm_handle.h"
#include "mino/shm/channel/index_slot.h"

namespace mino {

// Stable Runtime-facing metadata copied from an acquired IndexSlot snapshot.
// It contains no process-local pointers and remains valid for the lifetime of a
// BorrowedMessage even if the ring slot is subsequently reused.
struct MessageMetadata {
    uint32_t message_type = 0;
    uint32_t schema_version = 0;
    uint64_t schema_short_id = 0;
    uint32_t schema_layout_version = 0;
    uint64_t sequence_num = 0;
    uint64_t timestamp_ns = 0;
    ShmHandle payload;
    uint32_t payload_len = 0;
    uint32_t flags = 0;
};

inline MessageMetadata MetadataFromSlot(
    const IndexSlotSnapshot& slot) noexcept {
    return MessageMetadata{
        .message_type = slot.msg_type,
        .schema_version = slot.schema_version,
        .schema_short_id = slot.schema_short_id,
        .schema_layout_version = slot.schema_layout_version,
        .sequence_num = slot.sequence_num,
        .timestamp_ns = slot.timestamp_ns,
        .payload = slot.payload,
        .payload_len = slot.payload_len,
        .flags = slot.flags,
    };
}

}  // namespace mino

#endif  // MINO_RUNTIME_MESSAGE_H_
