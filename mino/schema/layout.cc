// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/layout.h"

#include <algorithm>
#include <limits>
#include <map>
#include <new>
#include <string>
#include <utility>

#include "mino/common/status.h"
#include "mino/schema/descriptor_closure.h"

namespace mino::schema {
namespace {

constexpr size_t kObjectHeaderSize = ObjectHeaderLayout::kSize;
constexpr size_t kObjectAlignment = ObjectHeaderLayout::kAlignment;

bool CheckedAdd(uint64_t lhs, uint64_t rhs, uint64_t& result) noexcept {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) return false;
    result = lhs + rhs;
    return true;
}

bool CheckedMultiply(uint64_t lhs, uint64_t rhs, uint64_t& result) noexcept {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return false;
    }
    result = lhs * rhs;
    return true;
}

bool AlignUp(uint64_t value, uint64_t alignment, uint64_t& result) noexcept {
    if (alignment == 0) return false;
    const uint64_t remainder = value % alignment;
    if (remainder == 0) {
        result = value;
        return true;
    }
    return CheckedAdd(value, alignment - remainder, result);
}

struct TypeShape {
    uint64_t size = 0;
    uint64_t alignment = 1;
    FieldStorageKind storage_kind = FieldStorageKind::kScalar;
    uint64_t max_child_bytes = 0;
    uint64_t max_dynamic_children = 0;
};

Result<TypeShape> ScalarShape(ScalarType scalar,
                              const ConstraintSet& constraints) {
    switch (scalar) {
        case ScalarType::kBool:
            return TypeShape{1, 1, FieldStorageKind::kScalar, 0, 0};
        case ScalarType::kInt32:
        case ScalarType::kUint32:
        case ScalarType::kFixed32:
        case ScalarType::kFloat:
            return TypeShape{4, 4, FieldStorageKind::kScalar, 0, 0};
        case ScalarType::kInt64:
        case ScalarType::kUint64:
        case ScalarType::kFixed64:
        case ScalarType::kDouble:
            return TypeShape{8, 8, FieldStorageKind::kScalar, 0, 0};
        case ScalarType::kString:
        case ScalarType::kBytes:
            if (!constraints.max_bytes().has_value()) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "dynamic scalar is missing max_bytes");
            }
            return TypeShape{VariableMetadataLayout::kSize,
                             VariableMetadataLayout::kAlignment,
                             FieldStorageKind::kVariable,
                             *constraints.max_bytes(), 1};
    }
    return Status::Error(StatusCode::kInvalidArgument,
                         "unsupported scalar type");
}

}  // namespace

class PlannerImpl {
public:
    PlannerImpl(
        const SchemaDescriptor& root,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors,
        const LayoutOptions& options)
        : options_(options) {
        descriptors_.emplace(std::string(root.aggregate().full_name()), &root);
        for (const auto& descriptor : descriptors) {
            if (descriptor != nullptr) {
                descriptors_.insert_or_assign(
                    std::string(descriptor->aggregate().full_name()),
                    descriptor.get());
            }
        }
    }

    Result<LayoutPlan> Run(const SchemaDescriptor& descriptor) {
        if (descriptor.identity().layout_version() != kDynamicLayoutVersion) {
            return Status::Error(StatusCode::kUnsupported,
                                 "only dynamic layout_version=1 is supported");
        }
        return Build(descriptor, 0);
    }

private:
    Result<TypeShape> Shape(const TypeDescriptor& type,
                            const ConstraintSet& constraints, size_t depth) {
        if (depth > options_.max_depth) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "layout nesting exceeds max_depth");
        }
        if (type.kind() == TypeDescriptor::Kind::kScalar) {
            if (!type.scalar().has_value()) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "scalar descriptor has no scalar kind");
            }
            return ScalarShape(*type.scalar(), constraints);
        }
        if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
            const auto it = descriptors_.find(type.name());
            if (it == descriptors_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "layout dependency is unavailable");
            }
            auto nested = Build(*it->second, depth + 1);
            if (!nested.ok()) return nested.status();
            if (it->second->aggregate().kind() == AggregateKind::kStruct) {
                if (nested->max_dynamic_children() != 0) {
                    return Status::Error(
                        StatusCode::kInvalidArgument,
                        "struct layout must not contain dynamic children");
                }
                return TypeShape{
                    static_cast<uint64_t>(nested->object_size()),
                    static_cast<uint64_t>(nested->object_alignment()),
                    FieldStorageKind::kInlineStruct, 0, 0};
            }
            uint64_t child_bytes = 0;
            if (!CheckedAdd(static_cast<uint64_t>(nested->object_size()),
                            nested->max_child_bytes(), child_bytes)) {
                return Overflow();
            }
            uint64_t child_count = 0;
            if (!CheckedAdd(1, nested->max_dynamic_children(), child_count)) {
                return Overflow();
            }
            return TypeShape{VariableMetadataLayout::kSize,
                             VariableMetadataLayout::kAlignment,
                             FieldStorageKind::kVariable, child_bytes,
                             child_count};
        }

        if (type.element_type() == nullptr ||
            !constraints.max_capacity().has_value()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "vector layout requires element type and max_capacity");
        }
        auto element = Shape(*type.element_type(), constraints, depth + 1);
        if (!element.ok()) return element.status();
        const uint64_t capacity = *constraints.max_capacity();
        uint64_t immediate_bytes = 0;
        uint64_t descendant_bytes = 0;
        if (!CheckedMultiply(capacity, element->size, immediate_bytes) ||
            !CheckedMultiply(capacity, element->max_child_bytes,
                             descendant_bytes)) {
            return Overflow();
        }
        uint64_t child_bytes = 0;
        if (!CheckedAdd(immediate_bytes, descendant_bytes, child_bytes)) {
            return Overflow();
        }
        uint64_t descendants = 0;
        uint64_t child_count = 0;
        if (!CheckedMultiply(capacity, element->max_dynamic_children,
                             descendants) ||
            !CheckedAdd(1, descendants, child_count)) {
            return Overflow();
        }
        return TypeShape{VariableMetadataLayout::kSize,
                         VariableMetadataLayout::kAlignment,
                         FieldStorageKind::kVariable, child_bytes,
                         child_count};
    }

    Result<LayoutPlan> Build(const SchemaDescriptor& descriptor, size_t depth) {
        if (descriptor.identity().layout_version() != kDynamicLayoutVersion) {
            return Status::Error(StatusCode::kUnsupported,
                                 "layout dependency is not version 1");
        }
        if (depth > options_.max_depth) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "layout nesting exceeds max_depth");
        }
        const std::string name(descriptor.aggregate().full_name());
        const auto cached = cache_.find(name);
        if (cached != cache_.end()) return cached->second;
        if (building_.contains(name)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "recursive layout dependency");
        }
        building_.emplace(name, true);

        const auto fields = descriptor.aggregate().fields();
        if (fields.size() > options_.max_fields) {
            building_.erase(name);
            return Status::Error(StatusCode::kResourceExhausted,
                                 "layout field count exceeds max_fields");
        }
        size_t optional_count = 0;
        for (const FieldDescriptor& field : fields) {
            if (field.cardinality() == FieldCardinality::kOptional) {
                ++optional_count;
            }
        }
        if (optional_count > std::numeric_limits<size_t>::max() - 63) {
            building_.erase(name);
            return Overflow();
        }
        const size_t bitmap_words = (optional_count + 63) / 64;
        uint64_t bitmap_bytes = 0;
        if (!CheckedMultiply(static_cast<uint64_t>(bitmap_words), 8,
                             bitmap_bytes)) {
            building_.erase(name);
            return Overflow();
        }
        uint64_t fixed_offset = 0;
        if (!CheckedAdd(kObjectHeaderSize, bitmap_bytes, fixed_offset)) {
            building_.erase(name);
            return Overflow();
        }
        if (!AlignUp(fixed_offset, kObjectAlignment, fixed_offset)) {
            building_.erase(name);
            return Overflow();
        }

        std::vector<FieldLayout> layouts;
        layouts.reserve(fields.size());
        uint64_t cursor = fixed_offset;
        uint64_t max_alignment = kObjectAlignment;
        uint64_t total_child_bytes = 0;
        uint64_t total_children = 0;
        size_t optional_index = 0;
        for (const FieldDescriptor& field : fields) {
            auto shape = Shape(field.type(), field.constraints(), depth + 1);
            if (!shape.ok()) {
                building_.erase(name);
                return shape.status();
            }
            if (!AlignUp(cursor, shape->alignment, cursor)) {
                building_.erase(name);
                return Overflow();
            }
            const uint64_t field_offset = cursor;
            if (!CheckedAdd(cursor, shape->size, cursor) ||
                !CheckedAdd(total_child_bytes, shape->max_child_bytes,
                            total_child_bytes) ||
                !CheckedAdd(total_children, shape->max_dynamic_children,
                            total_children)) {
                building_.erase(name);
                return Overflow();
            }
            max_alignment = std::max(max_alignment, shape->alignment);
            std::optional<size_t> presence_bit;
            if (field.cardinality() == FieldCardinality::kOptional) {
                presence_bit = optional_index++;
            }
            if (field_offset > std::numeric_limits<size_t>::max() ||
                shape->size > std::numeric_limits<size_t>::max() ||
                shape->alignment > std::numeric_limits<size_t>::max()) {
                building_.erase(name);
                return Overflow();
            }
            layouts.emplace_back(field.id(), static_cast<size_t>(field_offset),
                                 static_cast<size_t>(shape->size),
                                 static_cast<size_t>(shape->alignment),
                                 shape->storage_kind, presence_bit,
                                 shape->max_child_bytes,
                                 shape->max_dynamic_children);
        }
        if (!AlignUp(cursor, max_alignment, cursor)) {
            building_.erase(name);
            return Overflow();
        }
        const uint64_t fields_end = cursor;
        std::optional<size_t> unknown_fields_offset;
        if (descriptor.aggregate().kind() == AggregateKind::kMessage) {
            if (cursor > std::numeric_limits<size_t>::max()) {
                building_.erase(name);
                return Overflow();
            }
            unknown_fields_offset = static_cast<size_t>(cursor);
            if (!CheckedAdd(cursor, VariableMetadataLayout::kSize, cursor) ||
                !CheckedAdd(total_child_bytes,
                            kDynamicUnknownFieldMaxBytes, total_child_bytes) ||
                !CheckedAdd(total_children, 1, total_children) ||
                !AlignUp(cursor, max_alignment, cursor)) {
                building_.erase(name);
                return Overflow();
            }
        }
        if (descriptor.aggregate().kind() == AggregateKind::kStruct &&
            total_children != 0) {
            building_.erase(name);
            return Status::Error(
                StatusCode::kInvalidArgument,
                "struct layout must be fixed and contain no dynamic fields");
        }
        if (cursor > options_.max_object_size ||
            total_child_bytes > options_.max_total_child_bytes ||
            total_children > options_.max_dynamic_children) {
            building_.erase(name);
            return Status::Error(StatusCode::kResourceExhausted,
                                 "layout exceeds configured resource limits");
        }
        if (cursor > std::numeric_limits<size_t>::max() ||
            fixed_offset > std::numeric_limits<size_t>::max() ||
            max_alignment > std::numeric_limits<size_t>::max()) {
            building_.erase(name);
            return Overflow();
        }
        const size_t object_size = static_cast<size_t>(cursor);
        const size_t fixed_area_offset = static_cast<size_t>(fixed_offset);
        const size_t fixed_area_size =
            static_cast<size_t>(fields_end - fixed_offset);
        LayoutPlan plan(
            kDynamicLayoutVersion, kObjectHeaderSize, kObjectHeaderSize,
            bitmap_words, fixed_area_offset, fixed_area_size,
            unknown_fields_offset, object_size,
            static_cast<size_t>(max_alignment), total_child_bytes,
            total_children, std::move(layouts));
        building_.erase(name);
        cache_.emplace(name, plan);
        return plan;
    }

    static Status Overflow() {
        return Status::Error(StatusCode::kResourceExhausted,
                             "layout size arithmetic overflow");
    }

    const LayoutOptions& options_;
    std::map<std::string, const SchemaDescriptor*, std::less<>> descriptors_;
    std::map<std::string, LayoutPlan, std::less<>> cache_;
    std::map<std::string, bool, std::less<>> building_;
};

FieldLayout::FieldLayout(uint32_t field_id, size_t offset, size_t size,
                         size_t alignment, FieldStorageKind storage_kind,
                         std::optional<size_t> presence_bit,
                         uint64_t max_child_bytes,
                         uint64_t max_dynamic_children) noexcept
    : field_id_(field_id),
      offset_(offset),
      size_(size),
      alignment_(alignment),
      storage_kind_(storage_kind),
      presence_bit_(presence_bit),
      max_child_bytes_(max_child_bytes),
      max_dynamic_children_(max_dynamic_children) {}

LayoutPlan::LayoutPlan(uint32_t layout_version, size_t header_size,
                       size_t presence_bitmap_offset,
                       size_t presence_bitmap_words, size_t fixed_area_offset,
                       size_t fixed_area_size,
                       std::optional<size_t> unknown_fields_offset,
                       size_t object_size, size_t object_alignment,
                       uint64_t max_child_bytes,
                       uint64_t max_dynamic_children,
                       std::vector<FieldLayout> fields) noexcept
    : layout_version_(layout_version),
      header_size_(header_size),
      presence_bitmap_offset_(presence_bitmap_offset),
      presence_bitmap_words_(presence_bitmap_words),
      fixed_area_offset_(fixed_area_offset),
      fixed_area_size_(fixed_area_size),
      unknown_fields_offset_(unknown_fields_offset),
      object_size_(object_size),
      object_alignment_(object_alignment),
      max_child_bytes_(max_child_bytes),
      max_dynamic_children_(max_dynamic_children),
      fields_(std::move(fields)) {}

const FieldLayout* LayoutPlan::FindField(uint32_t field_id) const noexcept {
    const auto it = std::lower_bound(
        fields_.begin(), fields_.end(), field_id,
        [](const FieldLayout& field, uint32_t id) {
            return field.field_id() < id;
        });
    return it != fields_.end() && it->field_id() == field_id ? &*it : nullptr;
}

Result<LayoutPlan> LayoutPlanner::Plan(
    const SchemaDescriptor& descriptor,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors,
    const LayoutOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(ValidateDescriptorClosure(descriptor, descriptors));
        return PlannerImpl(descriptor, descriptors, options).Run(descriptor);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status LayoutPlanner::Validate(
    const SchemaDescriptor& descriptor, const LayoutPlan& plan,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors)
    noexcept {
    try {
        LayoutOptions options;
        options.max_fields = std::max(options.max_fields, plan.fields().size());
        options.max_object_size = std::max<uint64_t>(
            options.max_object_size, plan.object_size());
        options.max_total_child_bytes = std::max(
            options.max_total_child_bytes, plan.max_child_bytes());
        options.max_dynamic_children = std::max(
            options.max_dynamic_children, plan.max_dynamic_children());
        auto expected = Plan(descriptor, descriptors, options);
        if (!expected.ok()) return expected.status();
        const bool top_level_equal =
            plan.layout_version() == expected->layout_version() &&
            plan.header_size() == expected->header_size() &&
            plan.presence_bitmap_offset() ==
                expected->presence_bitmap_offset() &&
            plan.presence_bitmap_words() ==
                expected->presence_bitmap_words() &&
            plan.fixed_area_offset() == expected->fixed_area_offset() &&
            plan.fixed_area_size() == expected->fixed_area_size() &&
            plan.unknown_fields_offset() ==
                expected->unknown_fields_offset() &&
            plan.object_size() == expected->object_size() &&
            plan.object_alignment() == expected->object_alignment() &&
            plan.max_child_bytes() == expected->max_child_bytes() &&
            plan.max_dynamic_children() ==
                expected->max_dynamic_children() &&
            plan.fields().size() == expected->fields().size();
        if (!top_level_equal) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "LayoutPlan is not the canonical descriptor plan");
        }
        for (size_t i = 0; i < plan.fields().size(); ++i) {
            const FieldLayout& actual = plan.fields()[i];
            const FieldLayout& wanted = expected->fields()[i];
            if (actual.field_id() != wanted.field_id() ||
                actual.offset() != wanted.offset() ||
                actual.size() != wanted.size() ||
                actual.alignment() != wanted.alignment() ||
                actual.storage_kind() != wanted.storage_kind() ||
                actual.presence_bit() != wanted.presence_bit() ||
                actual.max_child_bytes() != wanted.max_child_bytes() ||
                actual.max_dynamic_children() !=
                    wanted.max_dynamic_children()) {
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "LayoutPlan field structure is not canonical");
            }
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema
