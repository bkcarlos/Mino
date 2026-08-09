// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/validator.h"

#include <algorithm>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <locale>
#include <map>
#include <new>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/layout.h"
#include "mino/schema/lexer.h"

namespace mino::schema {
namespace {

constexpr uint64_t kMaxFieldId = 536870911;

Status Error(const SourceRange& source, std::string message) {
    return Status::Error(
        StatusCode::kInvalidArgument,
        "line " + std::to_string(source.begin.line) + ", column " +
            std::to_string(source.begin.column) + ": " + std::move(message));
}

Status ResourceError(const SourceRange& source, std::string message) {
    return Status::Error(
        StatusCode::kResourceExhausted,
        "line " + std::to_string(source.begin.line) + ", column " +
            std::to_string(source.begin.column) + ": " + std::move(message));
}

bool IsScalarTypeName(std::string_view name) {
    return name == "int32" || name == "int64" || name == "uint32" ||
           name == "uint64" || name == "fixed32" || name == "fixed64" ||
           name == "float" || name == "double" || name == "bool" ||
           name == "string" || name == "bytes" || name == "vector";
}

bool IsStringLike(const TypeDescriptor& type) {
    if (type.kind() == TypeDescriptor::Kind::kScalar) {
        return type.scalar() == ScalarType::kString ||
               type.scalar() == ScalarType::kBytes;
    }
    return type.kind() == TypeDescriptor::Kind::kVector &&
           IsStringLike(*type.element_type());
}

bool ContainsVector(const TypeDescriptor& type) {
    return type.kind() == TypeDescriptor::Kind::kVector;
}

Result<uint64_t> ParsePositiveLimit(const Annotation& annotation) {
    if (!annotation.value.has_value() ||
        annotation.value->kind != LiteralKind::kInteger) {
        return Error(annotation.source, "annotation '" + annotation.name +
                                            "' requires an integer value");
    }
    std::string_view text = annotation.value->value;
    if (!text.empty() && text.front() == '+') {
        text.remove_prefix(1);
    }
    uint64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parsed.ec != std::errc() ||
        parsed.ptr != text.data() + text.size() || value == 0) {
        return Error(annotation.source, "annotation '" + annotation.name +
                                            "' must be a positive uint64");
    }
    return value;
}

std::string HexBits(uint64_t bits, size_t digits) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result(digits, '0');
    for (size_t i = 0; i < digits; ++i) {
        result[digits - 1 - i] = kHex[bits & 0xfu];
        bits >>= 4;
    }
    return result;
}

template <typename T>
std::optional<T> ParseFiniteFloating(std::string_view text) {
    if (text.empty()) return std::nullopt;
    std::istringstream input{std::string(text)};
    input.imbue(std::locale::classic());
    T value = 0;
    input >> std::noskipws >> value;
    if (input.fail() || !input.eof() || !std::isfinite(value)) {
        return std::nullopt;
    }
    return value;
}

template <typename T>
Result<T> ParseSignedInteger(const Literal& literal, T minimum, T maximum) {
    if (literal.kind != LiteralKind::kInteger) {
        return Error(literal.source, "default must be an integer literal");
    }
    std::string_view text = literal.value;
    if (!text.empty() && text.front() == '+') {
        text.remove_prefix(1);
    }
    int64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parsed.ec != std::errc() ||
        parsed.ptr != text.data() + text.size() || value < minimum ||
        value > maximum) {
        return Error(literal.source, "integer default is outside field range");
    }
    return static_cast<T>(value);
}

template <typename T>
Result<T> ParseUnsignedInteger(const Literal& literal, T maximum) {
    if (literal.kind != LiteralKind::kInteger) {
        return Error(literal.source, "default must be an integer literal");
    }
    std::string_view text = literal.value;
    if (!text.empty() && text.front() == '+') {
        text.remove_prefix(1);
    }
    uint64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parsed.ec != std::errc() ||
        parsed.ptr != text.data() + text.size() || value > maximum) {
        return Error(literal.source, "integer default is outside field range");
    }
    return static_cast<T>(value);
}

Result<DefaultValue> ValidateDefault(const Literal& literal,
                                     const TypeDescriptor& type) {
    if (type.kind() != TypeDescriptor::Kind::kScalar) {
        return Error(literal.source,
                     "default is supported only for scalar fields");
    }
    switch (*type.scalar()) {
        case ScalarType::kInt32: {
            auto value = ParseSignedInteger<int32_t>(
                literal, std::numeric_limits<int32_t>::min(),
                std::numeric_limits<int32_t>::max());
            if (!value.ok()) return value.status();
            return DefaultValue(DefaultValue::Kind::kInteger,
                                std::to_string(*value));
        }
        case ScalarType::kInt64: {
            auto value = ParseSignedInteger<int64_t>(
                literal, std::numeric_limits<int64_t>::min(),
                std::numeric_limits<int64_t>::max());
            if (!value.ok()) return value.status();
            return DefaultValue(DefaultValue::Kind::kInteger,
                                std::to_string(*value));
        }
        case ScalarType::kUint32:
        case ScalarType::kFixed32: {
            auto value = ParseUnsignedInteger<uint32_t>(
                literal, std::numeric_limits<uint32_t>::max());
            if (!value.ok()) return value.status();
            return DefaultValue(DefaultValue::Kind::kInteger,
                                std::to_string(*value));
        }
        case ScalarType::kUint64:
        case ScalarType::kFixed64: {
            auto value = ParseUnsignedInteger<uint64_t>(
                literal, std::numeric_limits<uint64_t>::max());
            if (!value.ok()) return value.status();
            return DefaultValue(DefaultValue::Kind::kInteger,
                                std::to_string(*value));
        }
        case ScalarType::kFloat: {
            if (literal.kind != LiteralKind::kInteger &&
                literal.kind != LiteralKind::kFloatingPoint) {
                return Error(literal.source,
                             "float default must be a numeric literal");
            }
            std::string_view text = literal.value;
            if (!text.empty() && text.front() == '+') text.remove_prefix(1);
            const auto value = ParseFiniteFloating<float>(text);
            if (!value.has_value()) {
                return Error(literal.source, "invalid finite float default");
            }
            return DefaultValue(
                DefaultValue::Kind::kFloat32,
                "0x" + HexBits(std::bit_cast<uint32_t>(*value), 8));
        }
        case ScalarType::kDouble: {
            if (literal.kind != LiteralKind::kInteger &&
                literal.kind != LiteralKind::kFloatingPoint) {
                return Error(literal.source,
                             "double default must be a numeric literal");
            }
            std::string_view text = literal.value;
            if (!text.empty() && text.front() == '+') text.remove_prefix(1);
            const auto value = ParseFiniteFloating<double>(text);
            if (!value.has_value()) {
                return Error(literal.source, "invalid finite double default");
            }
            return DefaultValue(
                DefaultValue::Kind::kFloat64,
                "0x" + HexBits(std::bit_cast<uint64_t>(*value), 16));
        }
        case ScalarType::kBool:
            if (literal.kind != LiteralKind::kBoolean) {
                return Error(literal.source,
                             "bool default must be true or false");
            }
            return DefaultValue(DefaultValue::Kind::kBoolean, literal.value);
        case ScalarType::kString:
            if (literal.kind != LiteralKind::kString) {
                return Error(literal.source,
                             "string default must be a string literal");
            }
            if (!IsValidUtf8(literal.value)) {
                return Error(literal.source,
                             "string default must be valid UTF-8");
            }
            return DefaultValue(DefaultValue::Kind::kString, literal.value);
        case ScalarType::kBytes:
            if (literal.kind != LiteralKind::kString) {
                return Error(literal.source,
                             "bytes default must be a string literal");
            }
            return DefaultValue(DefaultValue::Kind::kBytes, literal.value);
    }
    return Error(literal.source, "unsupported default value");
}

Result<uint32_t> ParseSchemaVersion(const OptionDeclaration& option) {
    if (option.value.kind != LiteralKind::kString) {
        return Error(option.source,
                     "schema_version must be a string in major.minor form");
    }
    const std::string_view text = option.value.value;
    const size_t dot = text.find('.');
    if (dot == std::string_view::npos || dot == 0 || dot + 1 == text.size() ||
        text.find('.', dot + 1) != std::string_view::npos) {
        return Error(option.source,
                     "schema_version must use major.minor form");
    }
    uint32_t major = 0;
    uint32_t minor = 0;
    const auto major_result =
        std::from_chars(text.data(), text.data() + dot, major);
    const auto minor_result =
        std::from_chars(text.data() + dot + 1, text.data() + text.size(), minor);
    if (major_result.ec != std::errc() ||
        major_result.ptr != text.data() + dot ||
        minor_result.ec != std::errc() ||
        minor_result.ptr != text.data() + text.size() || major > 0xffffu ||
        minor > 0xffffu) {
        return Error(option.source,
                     "schema_version components must be decimal uint16 values");
    }
    return (major << 16) | minor;
}

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

bool CheckedAlignUp(uint64_t value, uint64_t alignment,
                    uint64_t& result) noexcept {
    if (alignment == 0) return false;
    const uint64_t remainder = value % alignment;
    return remainder == 0
               ? (result = value, true)
               : CheckedAdd(value, alignment - remainder, result);
}

struct CapacityShape {
    uint64_t size = 0;
    uint64_t alignment = 1;
    uint64_t child_bytes = 0;
};

class CapacityFootprintValidator {
public:
    CapacityFootprintValidator(
        std::span<const AggregateDescriptor> local,
        std::span<const std::shared_ptr<const SchemaDescriptor>> imported,
        const ValidatorOptions& options)
        : local_(local), options_(options) {
        for (const auto& descriptor : imported) {
            if (descriptor != nullptr) {
                descriptors_.emplace(
                    std::string(descriptor->aggregate().full_name()),
                    &descriptor->aggregate());
            }
        }
        for (const AggregateDescriptor& aggregate : local_) {
            descriptors_.insert_or_assign(std::string(aggregate.full_name()),
                                          &aggregate);
        }
    }

    Result<void> Run() {
        for (const AggregateDescriptor& aggregate : local_) {
            auto shape = Build(aggregate, 0);
            if (!shape.ok()) return shape.status();
            if (shape->child_bytes > options_.max_total_capacity) {
                return Status::Error(
                    StatusCode::kResourceExhausted,
                    "declared capacity footprint exceeds max_total_capacity");
            }
        }
        return Result<void>();
    }

private:
    Result<CapacityShape> Shape(const TypeDescriptor& type,
                                const ConstraintSet& constraints,
                                size_t depth) {
        if (depth > options_.max_type_nesting_depth) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "capacity footprint exceeds nesting limit");
        }
        if (type.kind() == TypeDescriptor::Kind::kScalar) {
            if (!type.scalar().has_value()) return InvalidType();
            switch (*type.scalar()) {
                case ScalarType::kBool:
                    return CapacityShape{1, 1, 0};
                case ScalarType::kInt32:
                case ScalarType::kUint32:
                case ScalarType::kFixed32:
                case ScalarType::kFloat:
                    return CapacityShape{4, 4, 0};
                case ScalarType::kInt64:
                case ScalarType::kUint64:
                case ScalarType::kFixed64:
                case ScalarType::kDouble:
                    return CapacityShape{8, 8, 0};
                case ScalarType::kString:
                case ScalarType::kBytes:
                    if (!constraints.max_bytes().has_value()) {
                        return InvalidType();
                    }
                    return CapacityShape{
                        VariableMetadataLayout::kSize,
                        VariableMetadataLayout::kAlignment,
                        *constraints.max_bytes()};
            }
            return InvalidType();
        }
        if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
            const auto descriptor = descriptors_.find(type.name());
            if (descriptor == descriptors_.end()) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "capacity dependency is unavailable");
            }
            auto nested = Build(*descriptor->second, depth + 1);
            if (!nested.ok()) return nested.status();
            if (descriptor->second->kind() == AggregateKind::kStruct) {
                if (nested->child_bytes != 0) return InvalidType();
                return *nested;
            }
            uint64_t child_bytes = 0;
            if (!CheckedAdd(nested->size, nested->child_bytes, child_bytes)) {
                return Overflow();
            }
            return CapacityShape{VariableMetadataLayout::kSize,
                                 VariableMetadataLayout::kAlignment,
                                 child_bytes};
        }
        if (type.element_type() == nullptr ||
            !constraints.max_capacity().has_value()) {
            return InvalidType();
        }
        auto element = Shape(*type.element_type(), constraints, depth + 1);
        if (!element.ok()) return element.status();
        uint64_t immediate_bytes = 0;
        uint64_t descendant_bytes = 0;
        uint64_t child_bytes = 0;
        if (!CheckedMultiply(*constraints.max_capacity(), element->size,
                             immediate_bytes) ||
            !CheckedMultiply(*constraints.max_capacity(), element->child_bytes,
                             descendant_bytes) ||
            !CheckedAdd(immediate_bytes, descendant_bytes, child_bytes)) {
            return Overflow();
        }
        return CapacityShape{VariableMetadataLayout::kSize,
                             VariableMetadataLayout::kAlignment,
                             child_bytes};
    }

    Result<CapacityShape> Build(const AggregateDescriptor& aggregate,
                                size_t depth) {
        if (depth > options_.max_type_nesting_depth) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "capacity footprint exceeds nesting limit");
        }
        const std::string name(aggregate.full_name());
        const auto cached = cache_.find(name);
        if (cached != cache_.end()) return cached->second;
        if (!building_.insert(name).second) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "recursive capacity dependency");
        }

        size_t optional_count = 0;
        for (const FieldDescriptor& field : aggregate.fields()) {
            if (field.cardinality() == FieldCardinality::kOptional) {
                ++optional_count;
            }
        }
        const uint64_t bitmap_words =
            static_cast<uint64_t>((optional_count + 63) / 64);
        uint64_t bitmap_bytes = 0;
        uint64_t cursor = 0;
        if (!CheckedMultiply(bitmap_words, 8, bitmap_bytes) ||
            !CheckedAdd(ObjectHeaderLayout::kSize, bitmap_bytes, cursor) ||
            !CheckedAlignUp(cursor, ObjectHeaderLayout::kAlignment, cursor)) {
            return OverflowFor(name);
        }

        uint64_t max_alignment = ObjectHeaderLayout::kAlignment;
        uint64_t child_bytes = 0;
        for (const FieldDescriptor& field : aggregate.fields()) {
            auto shape = Shape(field.type(), field.constraints(), depth + 1);
            if (!shape.ok()) {
                building_.erase(name);
                return shape.status();
            }
            if (!CheckedAlignUp(cursor, shape->alignment, cursor) ||
                !CheckedAdd(cursor, shape->size, cursor) ||
                !CheckedAdd(child_bytes, shape->child_bytes, child_bytes)) {
                return OverflowFor(name);
            }
            max_alignment = std::max(max_alignment, shape->alignment);
        }
        if (!CheckedAlignUp(cursor, max_alignment, cursor)) {
            return OverflowFor(name);
        }
        if (aggregate.kind() == AggregateKind::kMessage) {
            uint64_t unknown_framing_bytes = 0;
            uint64_t unknown_child_bytes = 0;
            if (!CheckedMultiply(options_.unknown_fields.max_fields, 8,
                                 unknown_framing_bytes) ||
                !CheckedAdd(options_.unknown_fields.max_bytes,
                            unknown_framing_bytes, unknown_child_bytes) ||
                !CheckedAdd(cursor, VariableMetadataLayout::kSize, cursor) ||
                !CheckedAdd(child_bytes, unknown_child_bytes, child_bytes) ||
                !CheckedAlignUp(cursor, max_alignment, cursor)) {
                return OverflowFor(name);
            }
        }
        CapacityShape result{cursor, max_alignment, child_bytes};
        building_.erase(name);
        cache_.emplace(name, result);
        return result;
    }

    Result<CapacityShape> OverflowFor(const std::string& name) {
        building_.erase(name);
        return Overflow();
    }

    static Status Overflow() {
        return Status::Error(StatusCode::kResourceExhausted,
                             "declared capacity footprint overflows uint64");
    }

    static Status InvalidType() {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid type in capacity footprint");
    }

    std::span<const AggregateDescriptor> local_;
    const ValidatorOptions& options_;
    std::map<std::string, const AggregateDescriptor*, std::less<>> descriptors_;
    std::map<std::string, CapacityShape, std::less<>> cache_;
    std::set<std::string, std::less<>> building_;
};

Result<void> ValidateCapacityFootprints(
    std::span<const AggregateDescriptor> aggregates,
    std::span<const std::shared_ptr<const SchemaDescriptor>> imported,
    const ValidatorOptions& options) {
    return CapacityFootprintValidator(aggregates, imported, options).Run();
}

class ValidatorImpl {
public:
    ValidatorImpl(
        const SchemaFile& file,
        std::span<const std::shared_ptr<const SchemaDescriptor>> imported_types,
        const ValidatorOptions& options)
        : file_(file), imported_types_(imported_types), options_(options) {}

    Result<ValidatedSchema> Run() {
        if (file_.syntax.has_value() && file_.syntax->version != "v1") {
            return Error(file_.syntax->source, "only syntax \"v1\" is supported");
        }
        if (file_.aggregates.size() > options_.max_types) {
            return ResourceError(file_.source,
                                 "type count exceeds max_types");
        }
        package_ = file_.package.has_value() ? file_.package->name : "";
        if (package_.size() > options_.max_name_bytes) {
            return ResourceError(file_.package->source,
                                 "package name exceeds max_name_bytes");
        }

        bool saw_schema_version = false;
        for (const OptionDeclaration& option : file_.options) {
            if (option.name != "schema_version") {
                return Error(option.source, "unknown option '" + option.name + "'");
            }
            if (saw_schema_version) {
                return Error(option.source, "duplicate schema_version option");
            }
            saw_schema_version = true;
            auto version = ParseSchemaVersion(option);
            if (!version.ok()) return version.status();
            schema_version_ = *version;
        }

        if (!saw_schema_version && !options_.allow_implicit_schema_version) {
            return Error(file_.source,
                         "schema requires explicit option schema_version = "
                         "\"major.minor\"");
        }

        for (const auto& imported : imported_types_) {
            if (imported == nullptr) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "imported descriptor is null");
            }
            const std::string name(imported->aggregate().full_name());
            if (!external_types_.emplace(name, imported.get()).second) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "duplicate imported type '" + name + "'");
            }
        }

        for (const AggregateDeclaration& aggregate : file_.aggregates) {
            if (aggregate.name.size() > options_.max_name_bytes) {
                return ResourceError(aggregate.source,
                                     "aggregate name exceeds max_name_bytes");
            }
            if (IsScalarTypeName(aggregate.name)) {
                return Error(aggregate.source, "aggregate name '" +
                                                   aggregate.name +
                                                   "' conflicts with a type name");
            }
            const std::string full_name = FullName(aggregate.name);
            if (local_types_.contains(full_name) ||
                external_types_.contains(full_name)) {
                return Error(aggregate.source, "duplicate aggregate/type name '" +
                                                   full_name + "'");
            }
            local_types_.emplace(full_name, &aggregate);
        }

        std::vector<AggregateDescriptor> aggregates;
        aggregates.reserve(file_.aggregates.size());
        for (const AggregateDeclaration& aggregate : file_.aggregates) {
            auto descriptor = ValidateAggregate(aggregate);
            if (!descriptor.ok()) return descriptor.status();
            aggregates.push_back(std::move(*descriptor));
        }

        auto cycle = CheckRecursion();
        if (!cycle.ok()) return cycle.status();

        std::map<std::string, const AggregateDescriptor*, std::less<>> built;
        for (const AggregateDescriptor& aggregate : aggregates) {
            built.emplace(std::string(aggregate.full_name()), &aggregate);
        }
        for (const AggregateDeclaration& declaration : file_.aggregates) {
            if (declaration.kind != AggregateKind::kStruct) continue;
            const AggregateDescriptor& aggregate =
                *built.at(FullName(declaration.name));
            std::set<std::string, std::less<>> visiting;
            for (const FieldDescriptor& field : aggregate.fields()) {
                if (IsDynamic(field.type(), built, visiting)) {
                    return Error(declaration.source,
                                 "struct '" + std::string(aggregate.full_name()) +
                                     "' cannot contain dynamic field '" +
                                     std::string(field.name()) + "'");
                }
            }
        }

        auto capacity =
            ValidateCapacityFootprints(aggregates, imported_types_, options_);
        if (!capacity.ok()) return capacity.status();

        return ValidatedSchema(package_, schema_version_, std::move(aggregates));
    }

private:
    std::string FullName(std::string_view name) const {
        if (package_.empty()) return std::string(name);
        return package_ + "." + std::string(name);
    }

    Result<std::string> ResolveUserType(const TypeReference& type) const {
        const std::string package_relative = FullName(type.name);
        if (local_types_.contains(package_relative) ||
            external_types_.contains(package_relative)) {
            return package_relative;
        }
        if (local_types_.contains(type.name) || external_types_.contains(type.name)) {
            return type.name;
        }
        return Error(type.source, "unknown user type '" + type.name + "'");
    }

    Result<TypeDescriptor> ResolveType(const TypeReference& type, size_t depth,
                                       std::set<std::string, std::less<>>& edges) {
        if (depth > options_.max_type_nesting_depth) {
            return ResourceError(type.source,
                                 "type nesting exceeds max_type_nesting_depth");
        }
        if (type.kind == TypeKind::kScalar) {
            return TypeDescriptor::Scalar(*type.scalar, type.name);
        }
        if (type.kind == TypeKind::kUserDefined) {
            auto name = ResolveUserType(type);
            if (!name.ok()) return name.status();
            if (local_types_.contains(*name)) edges.insert(*name);
            return TypeDescriptor::UserDefined(std::move(*name));
        }
        if (type.element_type == nullptr) {
            return Error(type.source, "vector is missing its element type");
        }
        auto element = ResolveType(*type.element_type, depth + 1, edges);
        if (!element.ok()) return element.status();
        return TypeDescriptor::Vector(std::move(*element));
    }

    Result<AggregateDescriptor> ValidateAggregate(
        const AggregateDeclaration& aggregate) {
        if (total_fields_ + aggregate.fields.size() > options_.max_fields) {
            return ResourceError(aggregate.source,
                                 "field count exceeds max_fields");
        }
        total_fields_ += aggregate.fields.size();

        std::set<uint64_t> field_ids;
        std::set<std::string, std::less<>> field_names;
        std::set<std::string, std::less<>> edges;
        std::vector<FieldDescriptor> fields;
        fields.reserve(aggregate.fields.size());
        for (const FieldDeclaration& field : aggregate.fields) {
            if (field.id < 1 || field.id > kMaxFieldId) {
                return Error(field.source,
                             "field id must be in 1..536870911");
            }
            if (!field_ids.insert(field.id).second) {
                return Error(field.source,
                             "duplicate field id " + std::to_string(field.id));
            }
            if (!field_names.insert(field.name).second) {
                return Error(field.source,
                             "duplicate field name '" + field.name + "'");
            }
            if (field.name.size() > options_.max_name_bytes) {
                return ResourceError(field.source,
                                     "field name exceeds max_name_bytes");
            }
            auto type = ResolveType(field.type, 1, edges);
            if (!type.ok()) return type.status();
            auto validated = ValidateField(field, std::move(*type));
            if (!validated.ok()) return validated.status();
            fields.push_back(std::move(*validated));
        }
        graph_.emplace(FullName(aggregate.name), std::move(edges));

        std::vector<ReservedRangeDescriptor> reserved;
        for (const ReservedDeclaration& declaration : aggregate.reserved) {
            if (total_reserved_ + declaration.ranges.size() >
                options_.max_reserved_ranges) {
                return ResourceError(declaration.source,
                                     "reserved range count exceeds limit");
            }
            total_reserved_ += declaration.ranges.size();
            for (const ReservedRange& range : declaration.ranges) {
                if (range.first < 1 || range.last > kMaxFieldId ||
                    range.first > range.last) {
                    return Error(range.source,
                                 "reserved range must be ordered within "
                                 "1..536870911");
                }
                reserved.emplace_back(static_cast<uint32_t>(range.first),
                                      static_cast<uint32_t>(range.last));
            }
        }
        std::sort(reserved.begin(), reserved.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return lhs.first() < rhs.first();
                  });
        for (size_t i = 1; i < reserved.size(); ++i) {
            if (reserved[i].first() <= reserved[i - 1].last()) {
                return Error(aggregate.source, "reserved ranges overlap");
            }
        }
        for (const FieldDescriptor& field : fields) {
            for (const ReservedRangeDescriptor& range : reserved) {
                if (field.id() >= range.first() && field.id() <= range.last()) {
                    return Error(aggregate.source,
                                 "field id " + std::to_string(field.id()) +
                                     " conflicts with reserved range");
                }
            }
        }
        return AggregateDescriptor(aggregate.kind, FullName(aggregate.name),
                                   std::move(fields), std::move(reserved));
    }

    Result<FieldDescriptor> ValidateField(const FieldDeclaration& field,
                                          TypeDescriptor type) {
        std::set<std::string, std::less<>> names;
        std::optional<uint64_t> max_bytes;
        std::optional<uint64_t> max_capacity;
        bool snapshot_key = false;
        const Literal* default_literal = nullptr;
        for (const Annotation& annotation : field.annotations) {
            if (!names.insert(annotation.name).second) {
                return Error(annotation.source,
                             "duplicate annotation '" + annotation.name + "'");
            }
            if (annotation.name == "max_bytes") {
                if (!IsStringLike(type)) {
                    return Error(annotation.source,
                                 "max_bytes applies only to string/bytes fields");
                }
                auto value = ParsePositiveLimit(annotation);
                if (!value.ok()) return value.status();
                max_bytes = *value;
            } else if (annotation.name == "max_capacity") {
                if (!ContainsVector(type)) {
                    return Error(annotation.source,
                                 "max_capacity applies only to vector fields");
                }
                auto value = ParsePositiveLimit(annotation);
                if (!value.ok()) return value.status();
                max_capacity = *value;
            } else if (annotation.name == "default") {
                if (!annotation.value.has_value()) {
                    return Error(annotation.source,
                                 "default annotation requires a value");
                }
                default_literal = &*annotation.value;
            } else if (annotation.name == "snapshot_key") {
                if (annotation.value.has_value()) {
                    return Error(annotation.source,
                                 "snapshot_key does not take a value");
                }
                snapshot_key = true;
            } else {
                return Error(annotation.source,
                             "unknown annotation '" + annotation.name + "'");
            }
        }
        if (IsStringLike(type) && !max_bytes.has_value()) {
            return Error(field.source,
                         "string/bytes field requires max_bytes");
        }
        if (ContainsVector(type) && !max_capacity.has_value()) {
            return Error(field.source,
                         "vector field requires max_capacity");
        }

        std::optional<DefaultValue> default_value;
        if (default_literal != nullptr) {
            auto parsed = ValidateDefault(*default_literal, type);
            if (!parsed.ok()) return parsed.status();
            if ((parsed->kind() == DefaultValue::Kind::kString ||
                 parsed->kind() == DefaultValue::Kind::kBytes) &&
                max_bytes.has_value() &&
                parsed->canonical_value().size() > *max_bytes) {
                return Error(default_literal->source,
                             "default exceeds max_bytes");
            }
            default_value = std::move(*parsed);
        }
        return FieldDescriptor(
            static_cast<uint32_t>(field.id), field.name, field.cardinality,
            std::move(type),
            ConstraintSet(max_bytes, max_capacity, snapshot_key),
            std::move(default_value));
    }

    Result<void> CheckRecursion() {
        enum class Color { kWhite, kGray, kBlack };
        std::map<std::string, Color, std::less<>> colors;
        for (const auto& [name, unused] : graph_) {
            static_cast<void>(unused);
            colors.emplace(name, Color::kWhite);
        }
        for (const auto& [name, unused] : graph_) {
            static_cast<void>(unused);
            if (colors[name] == Color::kWhite) {
                auto status = Visit(name, colors);
                if (!status.ok()) return status;
            }
        }
        return Result<void>();
    }

    template <typename ColorMap>
    Result<void> Visit(const std::string& name, ColorMap& colors) {
        using Color = typename ColorMap::mapped_type;
        colors[name] = Color::kGray;
        for (const std::string& dependency : graph_.at(name)) {
            if (colors[dependency] == Color::kGray) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "recursive type dependency involving '" +
                                         dependency + "'");
            }
            if (colors[dependency] == Color::kWhite) {
                auto status = Visit(dependency, colors);
                if (!status.ok()) return status;
            }
        }
        colors[name] = Color::kBlack;
        return Result<void>();
    }

    bool IsDynamic(
        const TypeDescriptor& type,
        const std::map<std::string, const AggregateDescriptor*, std::less<>>& built,
        std::set<std::string, std::less<>>& visiting) const {
        if (type.kind() == TypeDescriptor::Kind::kVector) return true;
        if (type.kind() == TypeDescriptor::Kind::kScalar) {
            return type.scalar() == ScalarType::kString ||
                   type.scalar() == ScalarType::kBytes;
        }
        const auto local = built.find(type.name());
        if (local != built.end()) {
            if (local->second->kind() == AggregateKind::kMessage) return true;
            const std::string name(type.name());
            if (!visiting.insert(name).second) return true;
            for (const FieldDescriptor& field : local->second->fields()) {
                if (IsDynamic(field.type(), built, visiting)) return true;
            }
            visiting.erase(name);
            return false;
        }
        const auto external = external_types_.find(type.name());
        if (external == external_types_.end()) return true;
        if (external->second->aggregate().kind() == AggregateKind::kMessage) {
            return true;
        }
        for (const FieldDescriptor& field :
             external->second->aggregate().fields()) {
            if (IsDynamic(field.type(), built, visiting)) return true;
        }
        return false;
    }

    const SchemaFile& file_;
    std::span<const std::shared_ptr<const SchemaDescriptor>> imported_types_;
    const ValidatorOptions& options_;
    std::string package_;
    uint32_t schema_version_ = 0;
    size_t total_fields_ = 0;
    size_t total_reserved_ = 0;
    std::map<std::string, const AggregateDeclaration*, std::less<>> local_types_;
    std::map<std::string, const SchemaDescriptor*, std::less<>> external_types_;
    std::map<std::string, std::set<std::string, std::less<>>, std::less<>> graph_;
};

}  // namespace

ValidatedSchema::ValidatedSchema(
    std::string package_name, uint32_t schema_version,
    std::vector<AggregateDescriptor> aggregates)
    : package_name_(std::move(package_name)),
      schema_version_(schema_version),
      aggregates_(std::move(aggregates)) {}

Result<ValidatedSchema> SemanticValidator::Validate(
    const SchemaFile& file,
    std::span<const std::shared_ptr<const SchemaDescriptor>> imported_types,
    const ValidatorOptions& options) noexcept {
    try {
        return ValidatorImpl(file, imported_types, options).Run();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema
