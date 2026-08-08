// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RECORDING_TYPES_H_
#define MINO_STORAGE_RECORDING_TYPES_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "mino/common/ids.h"

namespace mino::storage {

// Fixed-width value form shared by Recorder Subscriber, Buffer Pool and the
// future TopicWriter. It is not a serialized C++ layout.
struct RecorderSchemaMetadata {
    uint64_t short_id = 0;
    std::array<std::byte, 32> canonical_digest{};
    uint32_t schema_version = 0;
    uint32_t layout_version = 0;

    friend bool operator==(const RecorderSchemaMetadata&,
                           const RecorderSchemaMetadata&) = default;
};

struct MessageSource {
    uint64_t node_id = 0;
    uint64_t publisher_id = 0;
    uint64_t publisher_epoch = 0;
    uint64_t source_sequence = 0;
    uint64_t observed_timestamp_ns = 0;

    friend bool operator==(const MessageSource&, const MessageSource&) = default;
};

// Metadata accompanying canonical payload bytes from buffer admission through writing.
// ingestion_sequence is intentionally absent: TopicWriter assigns it only
// after queue admission.
struct RecorderRecordMetadata {
    RecorderSchemaMetadata schema;
    TopicId topic_id{};
    MessageSource source;
    uint64_t ingestion_timestamp_ns = 0;
    uint32_t payload_size = 0;
    uint32_t payload_crc = 0;

    friend bool operator==(const RecorderRecordMetadata&,
                           const RecorderRecordMetadata&) = default;
};

static_assert(std::is_nothrow_copy_constructible_v<RecorderRecordMetadata>);
static_assert(std::is_nothrow_copy_assignable_v<RecorderRecordMetadata>);
static_assert(std::is_nothrow_move_constructible_v<RecorderRecordMetadata>);
static_assert(std::is_nothrow_move_assignable_v<RecorderRecordMetadata>);

}  // namespace mino::storage

#endif  // MINO_STORAGE_RECORDING_TYPES_H_
