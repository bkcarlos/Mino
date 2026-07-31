// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SCHEMA_DESCRIPTOR_H_
#define MINO_SCHEMA_DESCRIPTOR_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/schema/ast.h"

namespace mino::schema {

using CanonicalDigest = std::array<std::byte, 32>;

// SchemaIdentity is a value model, not a serialized C++ object. Network,
// storage, and shared-memory formats must encode each fixed-width member
// explicitly and must never persist sizeof(SchemaIdentity) bytes.
class SchemaIdentity {
public:
    SchemaIdentity(uint64_t short_id, CanonicalDigest canonical_digest,
                   uint32_t schema_version, uint32_t layout_version) noexcept;

    const CanonicalDigest& canonical_digest() const noexcept {
        return canonical_digest_;
    }
    uint64_t short_id() const noexcept { return short_id_; }
    uint32_t schema_version() const noexcept { return schema_version_; }
    uint32_t layout_version() const noexcept { return layout_version_; }

private:
    CanonicalDigest canonical_digest_{};
    uint64_t short_id_ = 0;
    uint32_t schema_version_ = 0;
    uint32_t layout_version_ = 0;
};

class TypeDescriptor {
public:
    enum class Kind { kScalar, kUserDefined, kVector };

    static TypeDescriptor Scalar(ScalarType scalar, std::string name);
    static TypeDescriptor UserDefined(std::string full_name);
    static TypeDescriptor Vector(TypeDescriptor element_type);

    Kind kind() const noexcept { return kind_; }
    std::optional<ScalarType> scalar() const noexcept { return scalar_; }
    std::string_view name() const noexcept { return name_; }
    const TypeDescriptor* element_type() const noexcept {
        return element_type_.get();
    }

private:
    TypeDescriptor(Kind kind, std::optional<ScalarType> scalar,
                   std::string name,
                   std::shared_ptr<const TypeDescriptor> element_type);

    Kind kind_ = Kind::kUserDefined;
    std::optional<ScalarType> scalar_;
    std::string name_;
    std::shared_ptr<const TypeDescriptor> element_type_;
};

class ConstraintSet {
public:
    ConstraintSet(std::optional<uint64_t> max_bytes,
                  std::optional<uint64_t> max_capacity,
                  bool snapshot_key) noexcept;

    std::optional<uint64_t> max_bytes() const noexcept { return max_bytes_; }
    std::optional<uint64_t> max_capacity() const noexcept {
        return max_capacity_;
    }
    bool snapshot_key() const noexcept { return snapshot_key_; }

private:
    std::optional<uint64_t> max_bytes_;
    std::optional<uint64_t> max_capacity_;
    bool snapshot_key_ = false;
};

class DefaultValue {
public:
    enum class Kind {
        kInteger,
        kFloat32,
        kFloat64,
        kBoolean,
        kString,
        kBytes,
    };

    DefaultValue(Kind kind, std::string canonical_value);

    Kind kind() const noexcept { return kind_; }
    // Integer and boolean values use normalized text. Floating-point values use
    // an IEEE-754 bit-pattern hex string. String and bytes values contain their
    // exact decoded bytes; canonicalization hex-encodes bytes values.
    std::string_view canonical_value() const noexcept {
        return canonical_value_;
    }

private:
    Kind kind_;
    std::string canonical_value_;
};

class FieldDescriptor {
public:
    FieldDescriptor(uint32_t id, std::string name,
                    FieldCardinality cardinality, TypeDescriptor type,
                    ConstraintSet constraints,
                    std::optional<DefaultValue> default_value);

    uint32_t id() const noexcept { return id_; }
    std::string_view name() const noexcept { return name_; }
    FieldCardinality cardinality() const noexcept { return cardinality_; }
    const TypeDescriptor& type() const noexcept { return type_; }
    const ConstraintSet& constraints() const noexcept { return constraints_; }
    const std::optional<DefaultValue>& default_value() const noexcept {
        return default_value_;
    }

private:
    uint32_t id_;
    std::string name_;
    FieldCardinality cardinality_;
    TypeDescriptor type_;
    ConstraintSet constraints_;
    std::optional<DefaultValue> default_value_;
};

class ReservedRangeDescriptor {
public:
    ReservedRangeDescriptor(uint32_t first, uint32_t last) noexcept
        : first_(first), last_(last) {}

    uint32_t first() const noexcept { return first_; }
    uint32_t last() const noexcept { return last_; }

private:
    uint32_t first_;
    uint32_t last_;
};

class AggregateDescriptor {
public:
    AggregateDescriptor(AggregateKind kind, std::string full_name,
                        std::vector<FieldDescriptor> fields,
                        std::vector<ReservedRangeDescriptor> reserved_ranges);

    AggregateKind kind() const noexcept { return kind_; }
    std::string_view full_name() const noexcept { return full_name_; }
    std::span<const FieldDescriptor> fields() const noexcept { return fields_; }
    std::span<const ReservedRangeDescriptor> reserved_ranges() const noexcept {
        return reserved_ranges_;
    }
    const FieldDescriptor* FindField(uint32_t id) const noexcept;
    bool IsReserved(uint32_t id) const noexcept;

private:
    AggregateKind kind_;
    std::string full_name_;
    // Both collections are normalized and sorted by ID/range start.
    std::vector<FieldDescriptor> fields_;
    std::vector<ReservedRangeDescriptor> reserved_ranges_;
};

class DependencyDescriptor {
public:
    DependencyDescriptor(std::string full_name, CanonicalDigest digest);

    std::string_view full_name() const noexcept { return full_name_; }
    const CanonicalDigest& digest() const noexcept { return digest_; }

private:
    std::string full_name_;
    CanonicalDigest digest_{};
};

// Immutable, ABI-independent logical descriptor. It deliberately exposes no
// mutable containers and makes no promise that its in-memory C++ layout is a
// wire/storage representation.
class SchemaDescriptor {
public:
    SchemaDescriptor(AggregateDescriptor aggregate, SchemaIdentity identity,
                     std::string canonical_schema,
                     std::vector<DependencyDescriptor> dependencies);

    const AggregateDescriptor& aggregate() const noexcept { return aggregate_; }
    const SchemaIdentity& identity() const noexcept { return identity_; }
    std::string_view canonical_schema() const noexcept {
        return canonical_schema_;
    }
    std::span<const DependencyDescriptor> dependencies() const noexcept {
        return dependencies_;
    }

private:
    AggregateDescriptor aggregate_;
    SchemaIdentity identity_;
    std::string canonical_schema_;
    std::vector<DependencyDescriptor> dependencies_;
};

class CompiledSchema {
public:
    explicit CompiledSchema(
        std::vector<std::shared_ptr<const SchemaDescriptor>> types);

    std::span<const std::shared_ptr<const SchemaDescriptor>> types() const
        noexcept {
        return types_;
    }
    const SchemaDescriptor* FindType(std::string_view full_name) const noexcept;

private:
    std::vector<std::shared_ptr<const SchemaDescriptor>> types_;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_DESCRIPTOR_H_
