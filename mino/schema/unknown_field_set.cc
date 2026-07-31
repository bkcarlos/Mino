// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/unknown_field_set.h"

#include <algorithm>
#include <limits>
#include <new>

namespace mino::schema {

Status UnknownFieldSet::Add(
    uint32_t field_id,
    std::span<const std::byte> canonical_field_bytes) noexcept {
    try {
        if (field_id == 0 || canonical_field_bytes.empty()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "unknown field must have an ID and bytes");
        }
        if (fields_.size() >= limits_.max_fields) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "unknown field count exceeds limit");
        }
        if (canonical_field_bytes.size() > limits_.max_bytes -
                                                 std::min(byte_size_, limits_.max_bytes)) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "unknown field bytes exceed limit");
        }
        std::vector<std::byte> bytes(canonical_field_bytes.begin(),
                                     canonical_field_bytes.end());
        fields_.emplace_back(field_id, std::move(bytes));
        byte_size_ += canonical_field_bytes.size();
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

void UnknownFieldSet::Clear() noexcept {
    fields_.clear();
    byte_size_ = 0;
}

}  // namespace mino::schema
