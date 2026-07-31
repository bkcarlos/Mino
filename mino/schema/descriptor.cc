// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/descriptor.h"

#include <algorithm>
#include <utility>

namespace mino::schema {

SchemaIdentity::SchemaIdentity(uint64_t short_id,
                               CanonicalDigest canonical_digest,
                               uint32_t schema_version,
                               uint32_t layout_version) noexcept
    : canonical_digest_(canonical_digest),
      short_id_(short_id),
      schema_version_(schema_version),
      layout_version_(layout_version) {}

TypeDescriptor::TypeDescriptor(
    Kind kind, std::optional<ScalarType> scalar, std::string name,
    std::shared_ptr<const TypeDescriptor> element_type)
    : kind_(kind),
      scalar_(scalar),
      name_(std::move(name)),
      element_type_(std::move(element_type)) {}

TypeDescriptor TypeDescriptor::Scalar(ScalarType scalar, std::string name) {
    return TypeDescriptor(Kind::kScalar, scalar, std::move(name), nullptr);
}

TypeDescriptor TypeDescriptor::UserDefined(std::string full_name) {
    return TypeDescriptor(Kind::kUserDefined, std::nullopt,
                          std::move(full_name), nullptr);
}

TypeDescriptor TypeDescriptor::Vector(TypeDescriptor element_type) {
    return TypeDescriptor(
        Kind::kVector, std::nullopt, "vector",
        std::make_shared<const TypeDescriptor>(std::move(element_type)));
}

ConstraintSet::ConstraintSet(std::optional<uint64_t> max_bytes,
                             std::optional<uint64_t> max_capacity,
                             bool snapshot_key) noexcept
    : max_bytes_(max_bytes),
      max_capacity_(max_capacity),
      snapshot_key_(snapshot_key) {}

DefaultValue::DefaultValue(Kind kind, std::string canonical_value)
    : kind_(kind), canonical_value_(std::move(canonical_value)) {}

FieldDescriptor::FieldDescriptor(
    uint32_t id, std::string name, FieldCardinality cardinality,
    TypeDescriptor type, ConstraintSet constraints,
    std::optional<DefaultValue> default_value)
    : id_(id),
      name_(std::move(name)),
      cardinality_(cardinality),
      type_(std::move(type)),
      constraints_(std::move(constraints)),
      default_value_(std::move(default_value)) {}

AggregateDescriptor::AggregateDescriptor(
    AggregateKind kind, std::string full_name,
    std::vector<FieldDescriptor> fields,
    std::vector<ReservedRangeDescriptor> reserved_ranges)
    : kind_(kind),
      full_name_(std::move(full_name)),
      fields_(std::move(fields)),
      reserved_ranges_(std::move(reserved_ranges)) {
    std::sort(fields_.begin(), fields_.end(),
              [](const FieldDescriptor& lhs, const FieldDescriptor& rhs) {
                  return lhs.id() < rhs.id();
              });
    std::sort(reserved_ranges_.begin(), reserved_ranges_.end(),
              [](const ReservedRangeDescriptor& lhs,
                 const ReservedRangeDescriptor& rhs) {
                  return lhs.first() < rhs.first();
              });
}

const FieldDescriptor* AggregateDescriptor::FindField(uint32_t id) const
    noexcept {
    const auto it = std::lower_bound(
        fields_.begin(), fields_.end(), id,
        [](const FieldDescriptor& field, uint32_t field_id) {
            return field.id() < field_id;
        });
    return it != fields_.end() && it->id() == id ? &*it : nullptr;
}

bool AggregateDescriptor::IsReserved(uint32_t id) const noexcept {
    const auto it = std::upper_bound(
        reserved_ranges_.begin(), reserved_ranges_.end(), id,
        [](uint32_t field_id, const ReservedRangeDescriptor& range) {
            return field_id < range.first();
        });
    if (it == reserved_ranges_.begin()) {
        return false;
    }
    return id <= std::prev(it)->last();
}

DependencyDescriptor::DependencyDescriptor(std::string full_name,
                                           CanonicalDigest digest)
    : full_name_(std::move(full_name)), digest_(digest) {}

SchemaDescriptor::SchemaDescriptor(
    AggregateDescriptor aggregate, SchemaIdentity identity,
    std::string canonical_schema,
    std::vector<DependencyDescriptor> dependencies)
    : aggregate_(std::move(aggregate)),
      identity_(std::move(identity)),
      canonical_schema_(std::move(canonical_schema)),
      dependencies_(std::move(dependencies)) {}

CompiledSchema::CompiledSchema(
    std::vector<std::shared_ptr<const SchemaDescriptor>> types)
    : types_(std::move(types)) {
    std::sort(types_.begin(), types_.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs->aggregate().full_name() <
                         rhs->aggregate().full_name();
              });
}

const SchemaDescriptor* CompiledSchema::FindType(
    std::string_view full_name) const noexcept {
    const auto it = std::lower_bound(
        types_.begin(), types_.end(), full_name,
        [](const auto& descriptor, std::string_view name) {
            return descriptor->aggregate().full_name() < name;
        });
    return it != types_.end() && (*it)->aggregate().full_name() == full_name
               ? it->get()
               : nullptr;
}

}  // namespace mino::schema
