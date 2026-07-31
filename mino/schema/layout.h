// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_LAYOUT_H_
#define MINO_SCHEMA_LAYOUT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "mino/common/result.h"
#include "mino/schema/descriptor.h"

namespace mino::schema {

inline constexpr uint32_t kDynamicLayoutVersion = 1;
inline constexpr uint64_t kDynamicUnknownFieldMaxCount = 64;
// Canonical bytes plus one {field_id:u32, byte_length:u32} frame per field.
inline constexpr uint64_t kDynamicUnknownFieldMaxBytes =
    (64u << 10) + kDynamicUnknownFieldMaxCount * 8;

// Stable serialized v1 object header. These are byte offsets, independent of
// compiler ABI and host struct padding.
struct ObjectHeaderLayout {
    static constexpr size_t kLayoutVersionOffset = 0;
    static constexpr size_t kHeaderSizeOffset = 4;
    static constexpr size_t kSchemaShortIdOffset = 8;
    // Encodes LayoutPlan::fixed_area_size(), not the total slab allocation.
    static constexpr size_t kObjectSizeOffset = 16;
    static constexpr size_t kFieldCountOffset = 24;
    static constexpr size_t kPresenceBitmapWordsOffset = 28;
    static constexpr size_t kSize = 32;
    static constexpr size_t kAlignment = 8;
};

// Stable v1 metadata layout. A handle is encoded as offset:u64,
// generation:u32, region_id:u32 and is never represented by a C++ pointer.
struct VariableMetadataLayout {
    static constexpr size_t kHandleOffset = 0;
    static constexpr size_t kHandleSize = 16;
    static constexpr size_t kLengthOffset = 16;
    static constexpr size_t kCapacityOffset = 24;
    static constexpr size_t kElementSizeOffset = 32;
    static constexpr size_t kSize = 40;
    static constexpr size_t kAlignment = 8;
};

enum class FieldStorageKind {
    kScalar,
    kInlineStruct,
    kVariable,
};

class FieldLayout {
public:
    FieldLayout(uint32_t field_id, size_t offset, size_t size,
                size_t alignment, FieldStorageKind storage_kind,
                std::optional<size_t> presence_bit,
                uint64_t max_child_bytes,
                uint64_t max_dynamic_children) noexcept;

    uint32_t field_id() const noexcept { return field_id_; }
    size_t offset() const noexcept { return offset_; }
    size_t size() const noexcept { return size_; }
    size_t alignment() const noexcept { return alignment_; }
    FieldStorageKind storage_kind() const noexcept { return storage_kind_; }
    std::optional<size_t> presence_bit() const noexcept { return presence_bit_; }
    uint64_t max_child_bytes() const noexcept { return max_child_bytes_; }
    uint64_t max_dynamic_children() const noexcept {
        return max_dynamic_children_;
    }

private:
    uint32_t field_id_ = 0;
    size_t offset_ = 0;
    size_t size_ = 0;
    size_t alignment_ = 1;
    FieldStorageKind storage_kind_ = FieldStorageKind::kScalar;
    std::optional<size_t> presence_bit_;
    uint64_t max_child_bytes_ = 0;
    uint64_t max_dynamic_children_ = 0;
};

class PlannerImpl;

class LayoutPlan {
public:

    uint32_t layout_version() const noexcept { return layout_version_; }
    size_t header_size() const noexcept { return header_size_; }
    size_t presence_bitmap_offset() const noexcept {
        return presence_bitmap_offset_;
    }
    size_t presence_bitmap_words() const noexcept {
        return presence_bitmap_words_;
    }
    size_t fixed_area_offset() const noexcept { return fixed_area_offset_; }
    size_t fixed_area_size() const noexcept { return fixed_area_size_; }
    std::optional<size_t> unknown_fields_offset() const noexcept {
        return unknown_fields_offset_;
    }
    size_t object_size() const noexcept { return object_size_; }
    size_t object_alignment() const noexcept { return object_alignment_; }
    uint64_t max_child_bytes() const noexcept { return max_child_bytes_; }
    uint64_t max_dynamic_children() const noexcept {
        return max_dynamic_children_;
    }
    std::span<const FieldLayout> fields() const noexcept { return fields_; }
    const FieldLayout* FindField(uint32_t field_id) const noexcept;

private:
    friend class LayoutPlanner;
    friend class PlannerImpl;
    LayoutPlan(uint32_t layout_version, size_t header_size,
               size_t presence_bitmap_offset,
               size_t presence_bitmap_words, size_t fixed_area_offset,
               size_t fixed_area_size,
               std::optional<size_t> unknown_fields_offset,
               size_t object_size, size_t object_alignment,
               uint64_t max_child_bytes,
               uint64_t max_dynamic_children,
               std::vector<FieldLayout> fields) noexcept;

    uint32_t layout_version_ = kDynamicLayoutVersion;
    size_t header_size_ = 0;
    size_t presence_bitmap_offset_ = 0;
    size_t presence_bitmap_words_ = 0;
    size_t fixed_area_offset_ = 0;
    size_t fixed_area_size_ = 0;
    std::optional<size_t> unknown_fields_offset_;
    size_t object_size_ = 0;
    size_t object_alignment_ = 1;
    uint64_t max_child_bytes_ = 0;
    uint64_t max_dynamic_children_ = 0;
    std::vector<FieldLayout> fields_;
};

struct LayoutOptions {
    size_t max_fields = 4096;
    size_t max_depth = 32;
    uint64_t max_object_size = 16u << 20;
    uint64_t max_total_child_bytes = 64u << 20;
    uint64_t max_dynamic_children = 1u << 20;
};

class LayoutPlanner {
public:
    // descriptors supplies the closure needed to resolve user-defined fields.
    // The returned plan owns no descriptor pointers and remains immutable.
    static Result<LayoutPlan> Plan(
        const SchemaDescriptor& descriptor,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors = {},
        const LayoutOptions& options = {}) noexcept;

    // Recomputes the canonical plan and compares every structural component.
    static Status Validate(
        const SchemaDescriptor& descriptor, const LayoutPlan& plan,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors = {})
        noexcept;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_LAYOUT_H_
