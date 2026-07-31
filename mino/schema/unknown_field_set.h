// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_UNKNOWN_FIELD_SET_H_
#define MINO_SCHEMA_UNKNOWN_FIELD_SET_H_

#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "mino/common/status.h"

namespace mino::schema {

struct UnknownFieldLimits {
    size_t max_bytes = 64u << 10;
    size_t max_fields = 64;
};

class UnknownField {
public:
    UnknownField(uint32_t field_id, std::vector<std::byte> canonical_bytes)
        : field_id_(field_id), canonical_bytes_(std::move(canonical_bytes)) {}

    uint32_t field_id() const noexcept { return field_id_; }
    std::span<const std::byte> canonical_bytes() const noexcept {
        return canonical_bytes_;
    }

private:
    uint32_t field_id_ = 0;
    std::vector<std::byte> canonical_bytes_;
};

class UnknownFieldSet {
public:
    explicit UnknownFieldSet(UnknownFieldLimits limits = {}) noexcept
        : limits_(limits) {}

    // canonical_field_bytes must contain one complete canonical tag+payload.
    Status Add(uint32_t field_id,
               std::span<const std::byte> canonical_field_bytes) noexcept;
    void Clear() noexcept;

    std::span<const UnknownField> fields() const noexcept { return fields_; }
    size_t byte_size() const noexcept { return byte_size_; }
    const UnknownFieldLimits& limits() const noexcept { return limits_; }

private:
    UnknownFieldLimits limits_;
    size_t byte_size_ = 0;
    std::vector<UnknownField> fields_;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_UNKNOWN_FIELD_SET_H_
